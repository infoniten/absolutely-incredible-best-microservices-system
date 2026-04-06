package kafka

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/quantara/object-framework/internal/domain"
	"github.com/quantara/object-framework/internal/processor"
	"github.com/quantara/object-framework/internal/repository"
)

// Consumer handles Kafka message consumption
type Consumer struct {
	reader       *kafka.Reader
	processor    *processor.FxSpotProcessor
	rawMsgRepo   *repository.RawMessageRepository
	idFunc       func(ctx context.Context) (int64, error)
}

// NewConsumer creates a new Kafka consumer
func NewConsumer(
	brokers []string,
	topic string,
	groupID string,
	proc *processor.FxSpotProcessor,
	rawMsgRepo *repository.RawMessageRepository,
	idFunc func(ctx context.Context) (int64, error),
) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10e6, // 10MB
		MaxWait:        1 * time.Second,
		CommitInterval: 1 * time.Second,
		StartOffset:    kafka.LastOffset,
	})

	return &Consumer{
		reader:     reader,
		processor:  proc,
		rawMsgRepo: rawMsgRepo,
		idFunc:     idFunc,
	}
}

// Start begins consuming messages
func (c *Consumer) Start(ctx context.Context) error {
	log.Printf("Starting Kafka consumer for topic: %s", c.reader.Config().Topic)

	for {
		select {
		case <-ctx.Done():
			log.Println("Kafka consumer stopping...")
			return c.reader.Close()
		default:
			if err := c.consumeMessage(ctx); err != nil {
				log.Printf("Error consuming message: %v", err)
				// Continue processing other messages
			}
		}
	}
}

// consumeMessage reads and processes a single message
func (c *Consumer) consumeMessage(ctx context.Context) error {
	// Read message with context
	msg, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch message: %w", err)
	}

	// Build messageID: topic:partition:offset:timestamp
	messageID := fmt.Sprintf("%s:%d:%d:%d",
		msg.Topic,
		msg.Partition,
		msg.Offset,
		msg.Time.UnixMilli(),
	)

	log.Printf("Received message: %s", messageID)

	// Check for duplicates
	exists, err := c.rawMsgRepo.Exists(ctx, messageID)
	if err != nil {
		log.Printf("Warning: failed to check message existence: %v", err)
	}
	if exists {
		log.Printf("Skipping duplicate message: %s", messageID)
		// Commit the message to avoid reprocessing
		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("Warning: failed to commit duplicate message: %v", err)
		}
		return nil
	}

	// Generate ID for raw message
	rawMsgID, err := c.idFunc(ctx)
	if err != nil {
		return fmt.Errorf("failed to generate raw message ID: %w", err)
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
		return fmt.Errorf("failed to save raw message: %w", err)
	}

	// Process the message
	result := c.processor.Process(ctx, rawMsg)

	if result.Success {
		log.Printf("Successfully processed message %s, TradeNo=%s, GlobalID=%d",
			messageID, result.TradeNo, result.GlobalID)
	} else {
		log.Printf("Failed to process message %s: %v", messageID, result.Error)
	}

	// Commit the message
	if err := c.reader.CommitMessages(ctx, msg); err != nil {
		log.Printf("Warning: failed to commit message: %v", err)
	}

	return nil
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

// buildMessageID creates a unique message ID from Kafka message coordinates
func buildMessageID(topic string, partition int, offset int64, timestamp time.Time) string {
	return topic + ":" + strconv.Itoa(partition) + ":" + strconv.FormatInt(offset, 10) + ":" + strconv.FormatInt(timestamp.UnixMilli(), 10)
}
