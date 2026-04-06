package kafka

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/quantara/object-framework/internal/domain"
	"github.com/quantara/object-framework/internal/processor"
	"github.com/quantara/object-framework/internal/repository"
)

// Consumer handles Kafka message consumption with parallel processing
type Consumer struct {
	reader      *kafka.Reader
	processor   *processor.FxSpotProcessor
	rawMsgRepo  *repository.RawMessageRepository
	idFunc      func(ctx context.Context) (int64, error)
	workerCount int

	// Metrics
	processedCount uint64
	errorCount     uint64
	inFlightCount  int64
}

// NewConsumer creates a new Kafka consumer with worker pool support
func NewConsumer(
	brokers []string,
	topic string,
	groupID string,
	proc *processor.FxSpotProcessor,
	rawMsgRepo *repository.RawMessageRepository,
	idFunc func(ctx context.Context) (int64, error),
	workerCount int,
) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10e6, // 10MB
		MaxWait:        500 * time.Millisecond,
		CommitInterval: 0, // Manual commit
		StartOffset:    kafka.LastOffset,
		// Fetch more messages for batching
		QueueCapacity: workerCount * 2,
	})

	if workerCount <= 0 {
		workerCount = 100 // Default
	}

	return &Consumer{
		reader:      reader,
		processor:   proc,
		rawMsgRepo:  rawMsgRepo,
		idFunc:      idFunc,
		workerCount: workerCount,
	}
}

// messageTask represents a message to be processed
type messageTask struct {
	msg    kafka.Message
	rawMsg *domain.RawMessageDto
}

// Start begins consuming messages with parallel workers
func (c *Consumer) Start(ctx context.Context) error {
	log.Printf("Starting Kafka consumer for topic: %s with %d workers", c.reader.Config().Topic, c.workerCount)

	// Create worker pool
	taskChan := make(chan messageTask, c.workerCount*4)
	commitChan := make(chan kafka.Message, c.workerCount*4)

	var wg sync.WaitGroup

	// Start worker goroutines
	for i := 0; i < c.workerCount; i++ {
		wg.Add(1)
		go c.worker(ctx, &wg, taskChan, commitChan)
	}

	// Start commit goroutine (batches commits for efficiency)
	commitDone := make(chan struct{})
	go c.commitWorker(ctx, commitChan, commitDone)

	// Start metrics reporter
	go c.metricsReporter(ctx)

	// Number of parallel fetchers (10 is good for 10 partitions)
	fetcherCount := 10
	if fetcherCount > c.workerCount/10 {
		fetcherCount = c.workerCount / 10
	}
	if fetcherCount < 1 {
		fetcherCount = 1
	}

	log.Printf("Starting %d parallel fetchers", fetcherCount)

	// Raw message channel for fetcher → preparer pipeline
	rawMsgChan := make(chan kafka.Message, c.workerCount*2)

	// Start parallel preparers (do ID generation and DB operations)
	var preparerWg sync.WaitGroup
	preparerCount := fetcherCount * 2 // More preparers than fetchers
	for i := 0; i < preparerCount; i++ {
		preparerWg.Add(1)
		go c.preparer(ctx, &preparerWg, rawMsgChan, taskChan)
	}

	// Start parallel fetchers
	var fetcherWg sync.WaitGroup
	for i := 0; i < fetcherCount; i++ {
		fetcherWg.Add(1)
		go c.fetcher(ctx, &fetcherWg, rawMsgChan)
	}

	// Wait for context cancellation
	<-ctx.Done()
	log.Println("Kafka consumer stopping...")

	// Close fetchers first
	fetcherWg.Wait()
	close(rawMsgChan)

	// Then preparers
	preparerWg.Wait()
	close(taskChan)

	// Then workers
	wg.Wait()
	close(commitChan)
	<-commitDone

	return c.reader.Close()
}

// fetcher continuously fetches messages from Kafka
func (c *Consumer) fetcher(ctx context.Context, wg *sync.WaitGroup, rawMsgChan chan<- kafka.Message) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				continue
			}
			select {
			case rawMsgChan <- msg:
			case <-ctx.Done():
				return
			}
		}
	}
}

// preparer prepares messages (ID generation, DB operations) in parallel
func (c *Consumer) preparer(ctx context.Context, wg *sync.WaitGroup, rawMsgChan <-chan kafka.Message, taskChan chan<- messageTask) {
	defer wg.Done()

	for msg := range rawMsgChan {
		select {
		case <-ctx.Done():
			return
		default:
		}

		task, err := c.prepareMessage(ctx, msg)
		if err != nil {
			log.Printf("Error preparing message: %v", err)
			continue
		}
		if task != nil {
			atomic.AddInt64(&c.inFlightCount, 1)
			select {
			case taskChan <- *task:
			case <-ctx.Done():
				return
			}
		}
	}
}

// prepareMessage prepares a fetched message for processing
func (c *Consumer) prepareMessage(ctx context.Context, msg kafka.Message) (*messageTask, error) {
	// Build messageID
	messageID := fmt.Sprintf("%s:%d:%d:%d",
		msg.Topic,
		msg.Partition,
		msg.Offset,
		msg.Time.UnixMilli(),
	)

	// Check for duplicates
	exists, err := c.rawMsgRepo.Exists(ctx, messageID)
	if err != nil {
		log.Printf("Warning: failed to check message existence: %v", err)
	}
	if exists {
		log.Printf("Skipping duplicate message: %s", messageID)
		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("Warning: failed to commit duplicate message: %v", err)
		}
		return nil, nil
	}

	// Generate ID for raw message
	rawMsgID, err := c.idFunc(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to generate raw message ID: %w", err)
	}

	// Extract headers
	metadata := extractHeaders(msg.Headers)
	if len(msg.Key) > 0 {
		metadata[domain.KafkaHeaderKey] = string(msg.Key)
	}

	// Create raw message DTO
	rawMsg := &domain.RawMessageDto{
		ID:        rawMsgID,
		Metadata:  metadata,
		MessageID: messageID,
		Value:     msg.Value,
		Source:    domain.SourceMOEX,
		Status:    domain.RawMessageStatusProcessing,
	}

	// Save raw message with PROCESSING status
	if err := c.rawMsgRepo.Create(ctx, rawMsg); err != nil {
		return nil, fmt.Errorf("failed to save raw message: %w", err)
	}

	return &messageTask{
		msg:    msg,
		rawMsg: rawMsg,
	}, nil
}

// worker processes messages from the task channel
func (c *Consumer) worker(ctx context.Context, wg *sync.WaitGroup, taskChan <-chan messageTask, commitChan chan<- kafka.Message) {
	defer wg.Done()

	for task := range taskChan {
		select {
		case <-ctx.Done():
			return
		default:
			c.processTask(ctx, task, commitChan)
		}
	}
}

// processTask processes a single message task
func (c *Consumer) processTask(ctx context.Context, task messageTask, commitChan chan<- kafka.Message) {
	defer atomic.AddInt64(&c.inFlightCount, -1)

	result := c.processor.Process(ctx, task.rawMsg)

	if result.Success {
		atomic.AddUint64(&c.processedCount, 1)
	} else {
		atomic.AddUint64(&c.errorCount, 1)
		log.Printf("Failed to process message %s: %v", task.rawMsg.MessageID, result.Error)
	}

	// Send to commit channel
	select {
	case commitChan <- task.msg:
	case <-ctx.Done():
	}
}

// commitWorker batches and commits messages
func (c *Consumer) commitWorker(ctx context.Context, commitChan <-chan kafka.Message, done chan<- struct{}) {
	defer close(done)

	var batch []kafka.Message
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case msg, ok := <-commitChan:
			if !ok {
				// Channel closed, commit remaining
				if len(batch) > 0 {
					c.commitBatch(ctx, batch)
				}
				return
			}
			batch = append(batch, msg)
			// Commit when batch is large enough
			if len(batch) >= 100 {
				c.commitBatch(ctx, batch)
				batch = batch[:0]
			}

		case <-ticker.C:
			// Periodic commit
			if len(batch) > 0 {
				c.commitBatch(ctx, batch)
				batch = batch[:0]
			}

		case <-ctx.Done():
			if len(batch) > 0 {
				c.commitBatch(context.Background(), batch)
			}
			return
		}
	}
}

// commitBatch commits a batch of messages
func (c *Consumer) commitBatch(ctx context.Context, batch []kafka.Message) {
	if len(batch) == 0 {
		return
	}
	if err := c.reader.CommitMessages(ctx, batch...); err != nil {
		log.Printf("Warning: failed to commit %d messages: %v", len(batch), err)
	}
}

// metricsReporter periodically logs metrics
func (c *Consumer) metricsReporter(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	var lastProcessed uint64

	for {
		select {
		case <-ticker.C:
			processed := atomic.LoadUint64(&c.processedCount)
			errors := atomic.LoadUint64(&c.errorCount)
			inFlight := atomic.LoadInt64(&c.inFlightCount)
			rate := float64(processed-lastProcessed) / 10.0
			lastProcessed = processed

			log.Printf("Consumer metrics: processed=%d, errors=%d, in_flight=%d, rate=%.1f msg/sec",
				processed, errors, inFlight, rate)

		case <-ctx.Done():
			return
		}
	}
}

// extractHeaders extracts Kafka headers into a map
func extractHeaders(headers []kafka.Header) map[string]string {
	result := make(map[string]string, len(headers))
	for _, h := range headers {
		result[h.Key] = string(h.Value)
	}
	return result
}

// Stats returns consumer statistics
func (c *Consumer) Stats() kafka.ReaderStats {
	return c.reader.Stats()
}

// Reader returns the underlying Kafka reader (for health checks)
func (c *Consumer) Reader() *kafka.Reader {
	return c.reader
}

// Close closes the consumer
func (c *Consumer) Close() error {
	return c.reader.Close()
}

// Metrics returns current metrics
func (c *Consumer) Metrics() (processed, errors uint64, inFlight int64) {
	return atomic.LoadUint64(&c.processedCount),
		atomic.LoadUint64(&c.errorCount),
		atomic.LoadInt64(&c.inFlightCount)
}

// buildMessageID creates a unique message ID from Kafka message coordinates
func buildMessageID(topic string, partition int, offset int64, timestamp time.Time) string {
	return topic + ":" + strconv.Itoa(partition) + ":" + strconv.FormatInt(offset, 10) + ":" + strconv.FormatInt(timestamp.UnixMilli(), 10)
}
