package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
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
func NewClient(redisURL string, clusterNodes []string, username, password string, ttlSeconds int) (*Client, error) {
	var rdb redis.UniversalClient

	if len(clusterNodes) > 0 {
		// Cluster mode
		rdb = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:    clusterNodes,
			Username: username,
			Password: password,
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

// keysSetKey returns the key for the set that indexes all object keys of a transaction.
// Uses the same {txID} hash tag so it co-locates on the same cluster shard.
func keysSetKey(txID string) string {
	return fmt.Sprintf("{%s}:keys", txID)
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

	setKey := keysSetKey(txID)

	pipe := c.rdb.Pipeline()
	pipe.RPush(ctx, key, data)
	pipe.Expire(ctx, key, c.ttl)
	pipe.SAdd(ctx, setKey, key)
	pipe.Expire(ctx, setKey, c.ttl)

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to add object: %w", err)
	}

	return nil
}

// GetObjectKeys returns all object list keys for a transaction.
// Uses an indexed Set ({txID}:keys) instead of SCAN so it works correctly
// in Redis Cluster mode (SCAN on a ClusterClient only walks one random node).
func (c *Client) GetObjectKeys(ctx context.Context, txID string) ([]string, error) {
	keys, err := c.rdb.SMembers(ctx, keysSetKey(txID)).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read transaction keys set: %w", err)
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

// DeleteTransaction removes all keys associated with a transaction.
// Uses the {txID}:keys index Set instead of SCAN so it works correctly
// in Redis Cluster mode. All keys share the {txID} hash tag, so a single
// DEL of multiple keys is safe (no cross-slot error).
func (c *Client) DeleteTransaction(ctx context.Context, txID string) error {
	setKey := keysSetKey(txID)

	objectKeys, err := c.rdb.SMembers(ctx, setKey).Result()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("failed to read transaction keys set: %w", err)
	}

	toDelete := make([]string, 0, len(objectKeys)+2)
	toDelete = append(toDelete, objectKeys...)
	toDelete = append(toDelete, metaKey(txID), setKey)

	if err := c.rdb.Del(ctx, toDelete...).Err(); err != nil {
		return fmt.Errorf("failed to delete keys: %w", err)
	}

	return nil
}

// DeleteKeys deletes keys one-by-one.
// Single-key DEL avoids cross-slot issues in Redis Cluster mode.
func (c *Client) DeleteKeys(ctx context.Context, keys ...string) error {
	for _, key := range keys {
		if strings.TrimSpace(key) == "" {
			continue
		}
		if err := c.rdb.Del(ctx, key).Err(); err != nil {
			return fmt.Errorf("failed to delete key %s: %w", key, err)
		}
	}
	return nil
}

// TryLock attempts to acquire a distributed lock using SETNX with TTL.
// Returns true if the lock was acquired, false if another instance holds it.
func (c *Client) TryLock(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	ok, err := c.rdb.SetNX(ctx, key, "1", ttl).Result()
	if err != nil {
		return false, fmt.Errorf("failed to acquire lock %s: %w", key, err)
	}
	return ok, nil
}

// PutCommittedGlobal stores one committed object body by global key.
// Value layout matches search-service cache contract.
func (c *Client) PutCommittedGlobal(ctx context.Context, key, objectClass, body string) error {
	if strings.TrimSpace(key) == "" || strings.TrimSpace(objectClass) == "" || strings.TrimSpace(body) == "" {
		return nil
	}

	values := map[string]any{
		"objectClass": objectClass,
		"body":        body,
	}
	if err := c.rdb.HSet(ctx, key, values).Err(); err != nil {
		return fmt.Errorf("failed to set committed global key %s: %w", key, err)
	}
	return nil
}

// PutEnrichFilter stores a filter-based enrichment cache entry.
// Key format: enrich:filter:{class}:{field}:{value} → globalId
func (c *Client) PutEnrichFilter(ctx context.Context, key string, globalID int64, ttl time.Duration) error {
	if strings.TrimSpace(key) == "" {
		return nil
	}
	return c.rdb.Set(ctx, key, strconv.FormatInt(globalID, 10), ttl).Err()
}

// AppendCommittedParent appends committed embedded objects to parent list key.
func (c *Client) AppendCommittedParent(ctx context.Context, key string, items []string) error {
	if strings.TrimSpace(key) == "" || len(items) == 0 {
		return nil
	}

	args := make([]any, 0, len(items))
	for _, item := range items {
		if strings.TrimSpace(item) == "" {
			continue
		}
		args = append(args, item)
	}
	if len(args) == 0 {
		return nil
	}

	if err := c.rdb.RPush(ctx, key, args...).Err(); err != nil {
		return fmt.Errorf("failed to append committed parent key %s: %w", key, err)
	}
	return nil
}
