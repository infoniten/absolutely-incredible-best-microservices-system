package cache

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// Key format: lock:obj:<globalID>
	lockKeyPrefix = "lock:obj:"
)

// LockCache provides Redis caching for locks
type LockCache struct {
	client redis.Cmdable
	closer func() error
}

// CachedLock represents a cached lock entry
type CachedLock struct {
	Token    string
	ExpireAt time.Time
}

// NewLockCache creates a new Redis-based lock cache (standalone mode)
func NewLockCache(addr, password string, db int) (*LockCache, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return &LockCache{
		client: client,
		closer: client.Close,
	}, nil
}

// NewLockCacheCluster creates a new Redis cluster-based lock cache
func NewLockCacheCluster(nodes []string, username, password string) (*LockCache, error) {
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    nodes,
		Username: username,
		Password: password,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis cluster: %w", err)
	}

	return &LockCache{
		client: client,
		closer: client.Close,
	}, nil
}

// buildKey creates Redis key for a globalID
func buildKey(globalID int64) string {
	return lockKeyPrefix + strconv.FormatInt(globalID, 10)
}

// Set caches a lock with TTL based on expiration time
func (c *LockCache) Set(ctx context.Context, globalID int64, token string, expireAt time.Time) error {
	ttl := time.Until(expireAt)
	if ttl <= 0 {
		return nil
	}

	key := buildKey(globalID)
	// Store as simple string "token:expireMs" to work with cluster (no pipeline across slots)
	value := token + ":" + strconv.FormatInt(expireAt.UnixMilli(), 10)
	return c.client.Set(ctx, key, value, ttl).Err()
}

// Get retrieves a cached lock
func (c *LockCache) Get(ctx context.Context, globalID int64) (*CachedLock, error) {
	key := buildKey(globalID)

	result, err := c.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return nil, nil // Not found
	}
	if err != nil {
		return nil, err
	}

	// Parse "token:expireMs"
	parts := strings.SplitN(result, ":", 2)
	if len(parts) != 2 {
		return nil, nil
	}

	token := parts[0]
	expireMs, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return nil, nil
	}

	expireAt := time.UnixMilli(expireMs)
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
	value := token + ":" + strconv.FormatInt(newExpireAt.UnixMilli(), 10)
	return c.client.Set(ctx, key, value, ttl).Err()
}

// Close closes the Redis connection
func (c *LockCache) Close() error {
	if c.closer != nil {
		return c.closer()
	}
	return nil
}
