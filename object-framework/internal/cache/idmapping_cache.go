package cache

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// IdMappingCache caches external_id -> globalID lookups in Redis (cluster or standalone).
//
// Multiple versions of the same trade (same TRADENO) arrive sequentially; after the
// first message creates the mapping in DB, subsequent versions can resolve the
// GlobalID from this cache and skip the DB roundtrip entirely.
type IdMappingCache struct {
	rdb redis.UniversalClient
	ttl time.Duration
}

// NewRedisClient creates a shared redis.UniversalClient. If clusterNodes is non-empty,
// a cluster client is used; otherwise the standalone redisURL is parsed.
func NewRedisClient(redisURL string, clusterNodes []string, username, password string) (redis.UniversalClient, error) {
	if len(clusterNodes) > 0 {
		return redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:    clusterNodes,
			Username: username,
			Password: password,
		}), nil
	}

	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse redis URL: %w", err)
	}
	if username != "" {
		opt.Username = username
	}
	if password != "" {
		opt.Password = password
	}
	return redis.NewClient(opt), nil
}

// NewIdMappingCache constructs a cache with its own Redis connection.
func NewIdMappingCache(redisURL string, clusterNodes []string, username, password string, ttlSeconds int) (*IdMappingCache, error) {
	rdb, err := NewRedisClient(redisURL, clusterNodes, username, password)
	if err != nil {
		return nil, err
	}
	return &IdMappingCache{
		rdb: rdb,
		ttl: time.Duration(ttlSeconds) * time.Second,
	}, nil
}

// NewIdMappingCacheFromClient constructs a cache that reuses an existing Redis client.
func NewIdMappingCacheFromClient(rdb redis.UniversalClient, ttlSeconds int) *IdMappingCache {
	return &IdMappingCache{
		rdb: rdb,
		ttl: time.Duration(ttlSeconds) * time.Second,
	}
}

func (c *IdMappingCache) Ping(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err()
}

func (c *IdMappingCache) Close() error {
	return c.rdb.Close()
}

// key layout: idmap:{source}:{sourceObjectType}:{externalID}
func cacheKey(externalID, source, sourceObjectType string) string {
	return fmt.Sprintf("idmap:%s:%s:%s", source, sourceObjectType, externalID)
}

// Get returns (globalID, true) on hit, (0, false) on miss.
func (c *IdMappingCache) Get(ctx context.Context, externalID, source, sourceObjectType string) (int64, bool, error) {
	val, err := c.rdb.Get(ctx, cacheKey(externalID, source, sourceObjectType)).Result()
	if errors.Is(err, redis.Nil) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	gid, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("invalid cached globalID %q: %w", val, err)
	}
	return gid, true, nil
}

// Set stores the mapping with the configured TTL.
func (c *IdMappingCache) Set(ctx context.Context, externalID, source, sourceObjectType string, globalID int64) error {
	return c.rdb.Set(
		ctx,
		cacheKey(externalID, source, sourceObjectType),
		strconv.FormatInt(globalID, 10),
		c.ttl,
	).Err()
}
