package domain

import "time"

type RawMessageStatus string

const (
	RawMessageStatusProcessing RawMessageStatus = "PROCESSING"
	RawMessageStatusSaved      RawMessageStatus = "SAVED"
	RawMessageStatusFailed     RawMessageStatus = "FAILED"
)

// RawMessageDto represents a raw message from Kafka before processing
type RawMessageDto struct {
	ID        int64                  `json:"id"`
	Metadata  map[string]string      `json:"metadata"`
	MessageID string                 `json:"messageId"` // topic:partition:offset:timestamp
	Value     []byte                 `json:"value"`
	Source    string                 `json:"source"` // MOEX
	Status    RawMessageStatus       `json:"status"`
	Error     string                 `json:"error,omitempty"`
	CreatedAt time.Time              `json:"createdAt"`
	UpdatedAt time.Time              `json:"updatedAt"`
}

// KafkaHeaders contains standard Kafka header keys
const (
	KafkaHeaderKey = "kafka.key"
)
