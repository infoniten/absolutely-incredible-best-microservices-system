package kafka

import (
	"context"
	"fmt"
	"log"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// Producer publishes committed trade events to Kafka.
type Producer struct {
	writer *kafkago.Writer
	topic  string
}

// NewProducer creates a Kafka producer. Returns nil if brokers is empty (disabled).
func NewProducer(brokers []string, topic string) *Producer {
	if len(brokers) == 0 || topic == "" {
		return nil
	}

	writer := &kafkago.Writer{
		Addr:         kafkago.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafkago.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
		Async:        true, // non-blocking writes for best-effort publishing
	}

	return &Producer{writer: writer, topic: topic}
}

// PublishCommittedObjects sends committed objects to Kafka.
// Each object is published as a separate message with objectClass as key.
// This is best-effort — errors are logged but do not fail the commit.
func (p *Producer) PublishCommittedObjects(ctx context.Context, txID string, objects []TradeEvent) {
	if p == nil || len(objects) == 0 {
		return
	}

	messages := make([]kafkago.Message, 0, len(objects))
	for _, obj := range objects {
		messages = append(messages, kafkago.Message{
			Key:   []byte(obj.Key),
			Value: []byte(obj.Value),
			Headers: []kafkago.Header{
				{Key: "transactionId", Value: []byte(txID)},
				{Key: "objectClass", Value: []byte(obj.ObjectClass)},
			},
		})
	}

	if err := p.writer.WriteMessages(ctx, messages...); err != nil {
		log.Printf("Warning: failed to publish %d trade events to %s: %v", len(messages), p.topic, err)
	}
}

// Close closes the Kafka writer.
func (p *Producer) Close() error {
	if p == nil {
		return nil
	}
	return p.writer.Close()
}

// TradeEvent represents a committed object to publish.
type TradeEvent struct {
	Key         string // message key (e.g., globalId or objectClass:globalId)
	Value       string // JSON payload
	ObjectClass string
}

// EnsureTopicExists creates the topic if it doesn't exist.
func EnsureTopicExists(brokers []string, topic string, numPartitions, replicationFactor int) error {
	if len(brokers) == 0 || topic == "" {
		return nil
	}

	conn, err := kafkago.Dial("tcp", brokers[0])
	if err != nil {
		return err
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return err
	}

	controllerConn, err := kafkago.Dial("tcp", controller.Host+":"+itoa(controller.Port))
	if err != nil {
		return err
	}
	defer controllerConn.Close()

	err = controllerConn.CreateTopics(kafkago.TopicConfig{
		Topic:             topic,
		NumPartitions:     numPartitions,
		ReplicationFactor: replicationFactor,
	})
	if err != nil {
		// Topic may already exist — not an error
		log.Printf("Kafka topic create %s: %v (may already exist)", topic, err)
	}
	return nil
}

func itoa(n int) string {
	return fmt.Sprintf("%d", n)
}
