package cache

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// Key format: lock:obj:<globalID>
	lockKeyPrefix = "lock:obj:"
)

// LockCache provides Redis caching for locks
type LockCache struct {
	client *redis.Client
}

// CachedLock represents a cached lock entry
type CachedLock struct {
	Token    string
	ExpireAt time.Time
}

// NewLockCache creates a new Redis-based lock cache
func NewLockCache(addr, password string, db int) (*LockCache, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return &LockCache{client: client}, nil
}

// buildKey creates Redis key for a globalID
func buildKey(globalID int64) string {
	return lockKeyPrefix + strconv.FormatInt(globalID, 10)
}

// Set caches a lock with TTL based on expiration time
func (c *LockCache) Set(ctx context.Context, globalID int64, token string, expireAt time.Time) error {
	ttl := time.Until(expireAt)
	if ttl <= 0 {
		// Already expired, don't cache
		return nil
	}

	key := buildKey(globalID)
	// Store token and expireAt as hash
	pipe := c.client.Pipeline()
	pipe.HSet(ctx, key, "token", token, "expire", expireAt.UnixMilli())
	pipe.PExpire(ctx, key, ttl)
	_, err := pipe.Exec(ctx)
	return err
}

// Get retrieves a cached lock
func (c *LockCache) Get(ctx context.Context, globalID int64) (*CachedLock, error) {
	key := buildKey(globalID)

	result, err := c.client.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, err
	}

	if len(result) == 0 {
		return nil, nil // Not found
	}

	token, ok := result["token"]
	if !ok {
		return nil, nil
	}

	expireStr, ok := result["expire"]
	if !ok {
		return nil, nil
	}

	expireMs, err := strconv.ParseInt(expireStr, 10, 64)
	if err != nil {
		return nil, nil
	}

	expireAt := time.UnixMilli(expireMs)

	// Check if expired (shouldn't happen due to TTL, but be safe)
	if expireAt.Before(time.Now()) {
		c.Delete(ctx, globalID)
		return nil, nil
	}

	return &CachedLock{
		Token:    token,
		ExpireAt: expireAt,
	}, nil
}

// Delete removes a lock from cache
func (c *LockCache) Delete(ctx context.Context, globalID int64) error {
	key := buildKey(globalID)
	return c.client.Del(ctx, key).Err()
}

// UpdateTTL updates the expiration time of a cached lock
func (c *LockCache) UpdateTTL(ctx context.Context, globalID int64, token string, newExpireAt time.Time) error {
	ttl := time.Until(newExpireAt)
	if ttl <= 0 {
		return c.Delete(ctx, globalID)
	}

	key := buildKey(globalID)

	// Update expire field and TTL atomically using transaction
	pipe := c.client.Pipeline()
	pipe.HSet(ctx, key, "expire", newExpireAt.UnixMilli())
	pipe.PExpire(ctx, key, ttl)
	_, err := pipe.Exec(ctx)
	return err
}

// Close closes the Redis connection
func (c *LockCache) Close() error {
	return c.client.Close()
}
