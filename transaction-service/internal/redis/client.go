package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// TxStatus represents the status of a transaction
type TxStatus string

const (
	TxStatusActive     TxStatus = "ACTIVE"
	TxStatusCommitting TxStatus = "COMMITTING"
	TxStatusCommitted  TxStatus = "COMMITTED"
	TxStatusRolledBack TxStatus = "ROLLED_BACK"
)

// TxMeta holds transaction metadata
type TxMeta struct {
	Status    TxStatus  `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

// StoredObject represents an object stored in Redis
type StoredObject struct {
	Headers json.RawMessage `json:"headers"`
	Payload json.RawMessage `json:"payload"`
}

// Client wraps Redis operations for transaction storage
type Client struct {
	rdb redis.UniversalClient
	ttl time.Duration
}

// NewClient creates a new Redis client
func NewClient(redisURL string, clusterNodes []string, ttlSeconds int) (*Client, error) {
	var rdb redis.UniversalClient

	if len(clusterNodes) > 0 {
		// Cluster mode
		rdb = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs: clusterNodes,
		})
	} else {
		// Standalone mode
		opt, err := redis.ParseURL(redisURL)
		if err != nil {
			return nil, fmt.Errorf("failed to parse redis URL: %w", err)
		}
		rdb = redis.NewClient(opt)
	}

	return &Client{
		rdb: rdb,
		ttl: time.Duration(ttlSeconds) * time.Second,
	}, nil
}

// Ping checks Redis connectivity
func (c *Client) Ping(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err()
}

// Close closes the Redis connection
func (c *Client) Close() error {
	return c.rdb.Close()
}

// ============================================================================
// Transaction Management
// ============================================================================

// metaKey returns the metadata key for a transaction
// Using hash tag {txID} to ensure all keys are on the same shard in cluster mode
func metaKey(txID string) string {
	return fmt.Sprintf("{%s}:meta", txID)
}

// objectKey returns the key for storing objects by algorithm and class
func objectKey(txID string, algorithmID uint32, className string) string {
	return fmt.Sprintf("{%s}:alg:%d:class:%s", txID, algorithmID, className)
}

// CreateTransaction creates a new transaction in Redis
func (c *Client) CreateTransaction(ctx context.Context, txID string) error {
	meta := TxMeta{
		Status:    TxStatusActive,
		CreatedAt: time.Now(),
	}

	data, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("failed to marshal tx meta: %w", err)
	}

	key := metaKey(txID)
	if err := c.rdb.Set(ctx, key, data, c.ttl).Err(); err != nil {
		return fmt.Errorf("failed to create transaction: %w", err)
	}

	return nil
}

// GetTransactionStatus returns the status of a transaction
func (c *Client) GetTransactionStatus(ctx context.Context, txID string) (TxStatus, error) {
	key := metaKey(txID)
	data, err := c.rdb.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return "", fmt.Errorf("transaction not found: %s", txID)
	}
	if err != nil {
		return "", fmt.Errorf("failed to get transaction: %w", err)
	}

	var meta TxMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return "", fmt.Errorf("failed to unmarshal tx meta: %w", err)
	}

	return meta.Status, nil
}

// SetTransactionStatus updates the status of a transaction
func (c *Client) SetTransactionStatus(ctx context.Context, txID string, status TxStatus) error {
	key := metaKey(txID)
	data, err := c.rdb.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return fmt.Errorf("transaction not found: %s", txID)
	}
	if err != nil {
		return fmt.Errorf("failed to get transaction: %w", err)
	}

	var meta TxMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return fmt.Errorf("failed to unmarshal tx meta: %w", err)
	}

	meta.Status = status
	newData, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("failed to marshal tx meta: %w", err)
	}

	// Keep remaining TTL
	ttl, _ := c.rdb.TTL(ctx, key).Result()
	if ttl <= 0 {
		ttl = c.ttl
	}

	if err := c.rdb.Set(ctx, key, newData, ttl).Err(); err != nil {
		return fmt.Errorf("failed to update transaction status: %w", err)
	}

	return nil
}

// ============================================================================
// Object Storage
// ============================================================================

// AddObject adds an object to the transaction
func (c *Client) AddObject(ctx context.Context, txID string, algorithmID uint32, className string, obj *StoredObject) error {
	data, err := json.Marshal(obj)
	if err != nil {
		return fmt.Errorf("failed to marshal object: %w", err)
	}

	key := objectKey(txID, algorithmID, className)

	pipe := c.rdb.Pipeline()
	pipe.RPush(ctx, key, data)
	pipe.Expire(ctx, key, c.ttl)

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to add object: %w", err)
	}

	return nil
}

// GetObjectKeys returns all object list keys for a transaction
func (c *Client) GetObjectKeys(ctx context.Context, txID string) ([]string, error) {
	pattern := fmt.Sprintf("{%s}:alg:*", txID)
	var keys []string

	iter := c.rdb.Scan(ctx, 0, pattern, 0).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		// Skip meta key
		if key != metaKey(txID) {
			keys = append(keys, key)
		}
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("failed to scan keys: %w", err)
	}

	return keys, nil
}

// GetObjects returns objects from a specific key with pagination
func (c *Client) GetObjects(ctx context.Context, key string, offset, limit int64) ([]*StoredObject, error) {
	data, err := c.rdb.LRange(ctx, key, offset, offset+limit-1).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get objects: %w", err)
	}

	objects := make([]*StoredObject, 0, len(data))
	for _, d := range data {
		var obj StoredObject
		if err := json.Unmarshal([]byte(d), &obj); err != nil {
			return nil, fmt.Errorf("failed to unmarshal object: %w", err)
		}
		objects = append(objects, &obj)
	}

	return objects, nil
}

// GetObjectCount returns the number of objects in a key
func (c *Client) GetObjectCount(ctx context.Context, key string) (int64, error) {
	return c.rdb.LLen(ctx, key).Result()
}

// DeleteTransaction removes all keys associated with a transaction
func (c *Client) DeleteTransaction(ctx context.Context, txID string) error {
	// Get all keys for this transaction
	pattern := fmt.Sprintf("{%s}:*", txID)

	var keys []string
	iter := c.rdb.Scan(ctx, 0, pattern, 0).Iterator()
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if err := iter.Err(); err != nil {
		return fmt.Errorf("failed to scan keys: %w", err)
	}

	if len(keys) > 0 {
		if err := c.rdb.Del(ctx, keys...).Err(); err != nil {
			return fmt.Errorf("failed to delete keys: %w", err)
		}
	}

	return nil
}
