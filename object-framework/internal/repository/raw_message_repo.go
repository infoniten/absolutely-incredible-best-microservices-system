package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/quantara/object-framework/internal/domain"
)

// RawMessageRepository handles persistence of raw Kafka messages
type RawMessageRepository struct {
	db *sql.DB
}

// NewRawMessageRepository creates a new RawMessageRepository
func NewRawMessageRepository(db *sql.DB) *RawMessageRepository {
	return &RawMessageRepository{db: db}
}

// Create inserts a new raw message record
func (r *RawMessageRepository) Create(ctx context.Context, msg *domain.RawMessageDto) error {
	metadataJSON, err := json.Marshal(msg.Metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	query := `
		INSERT INTO raw_messages (id, metadata, message_id, value, source, status, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`

	now := time.Now()
	_, err = r.db.ExecContext(ctx, query,
		msg.ID,
		metadataJSON,
		msg.MessageID,
		msg.Value,
		msg.Source,
		msg.Status,
		now,
		now,
	)
	if err != nil {
		return fmt.Errorf("failed to insert raw message: %w", err)
	}

	msg.CreatedAt = now
	msg.UpdatedAt = now
	return nil
}

// UpdateStatus updates the status of a raw message
func (r *RawMessageRepository) UpdateStatus(ctx context.Context, id int64, status domain.RawMessageStatus, errorMsg string) error {
	query := `
		UPDATE raw_messages
		SET status = $1, error = $2, updated_at = $3
		WHERE id = $4
	`

	_, err := r.db.ExecContext(ctx, query, status, errorMsg, time.Now(), id)
	if err != nil {
		return fmt.Errorf("failed to update raw message status: %w", err)
	}
	return nil
}

// GetByMessageID retrieves a raw message by its Kafka message ID
func (r *RawMessageRepository) GetByMessageID(ctx context.Context, messageID string) (*domain.RawMessageDto, error) {
	query := `
		SELECT id, metadata, message_id, value, source, status, error, created_at, updated_at
		FROM raw_messages
		WHERE message_id = $1
	`

	var msg domain.RawMessageDto
	var metadataJSON []byte
	var errorMsg sql.NullString

	err := r.db.QueryRowContext(ctx, query, messageID).Scan(
		&msg.ID,
		&metadataJSON,
		&msg.MessageID,
		&msg.Value,
		&msg.Source,
		&msg.Status,
		&errorMsg,
		&msg.CreatedAt,
		&msg.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get raw message: %w", err)
	}

	if err := json.Unmarshal(metadataJSON, &msg.Metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	if errorMsg.Valid {
		msg.Error = errorMsg.String
	}

	return &msg, nil
}

// Exists checks if a message with the given messageID already exists
func (r *RawMessageRepository) Exists(ctx context.Context, messageID string) (bool, error) {
	query := `SELECT EXISTS(SELECT 1 FROM raw_messages WHERE message_id = $1)`

	var exists bool
	err := r.db.QueryRowContext(ctx, query, messageID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check message existence: %w", err)
	}
	return exists, nil
}

// CreateTableIfNotExists creates the raw_messages table if it doesn't exist
func (r *RawMessageRepository) CreateTableIfNotExists(ctx context.Context) error {
	query := `
		CREATE TABLE IF NOT EXISTS raw_messages (
			id BIGINT PRIMARY KEY,
			metadata JSONB NOT NULL,
			message_id VARCHAR(512) NOT NULL UNIQUE,
			value BYTEA NOT NULL,
			source VARCHAR(64) NOT NULL,
			status VARCHAR(32) NOT NULL,
			error TEXT,
			created_at TIMESTAMP NOT NULL,
			updated_at TIMESTAMP NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_raw_messages_message_id ON raw_messages(message_id);
		CREATE INDEX IF NOT EXISTS idx_raw_messages_status ON raw_messages(status);
	`

	_, err := r.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create raw_messages table: %w", err)
	}
	return nil
}
