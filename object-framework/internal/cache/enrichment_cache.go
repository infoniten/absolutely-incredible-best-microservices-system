package cache

import (
	"context"
	"errors"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// EnrichmentCache reads enrichment data from Redis to avoid gRPC calls to search-service.
//
// Two key patterns are used:
//   - AltId keys written by search-service: "alt:{objectClass}:{sourceAlias}:{altID}" → globalId
//   - Filter keys written by transaction-service: "enrich:filter:{objectClass}:{field}:{value}" → globalId
type EnrichmentCache struct {
	rdb redis.UniversalClient
}

// NewEnrichmentCache creates a cache that shares the provided Redis client.
func NewEnrichmentCache(rdb redis.UniversalClient) *EnrichmentCache {
	return &EnrichmentCache{rdb: rdb}
}

// GetGlobalIDByAltID reads the alt-id key written by search-service.
// Key format: alt:{objectClass}:{sourceAlias}:{altID}
func (c *EnrichmentCache) GetGlobalIDByAltID(ctx context.Context, objectClass, altID, sourceAlias string) (int64, bool, error) {
	key := "alt:" + objectClass + ":" + sourceAlias + ":" + altID
	return c.getInt64(ctx, key)
}

// GetGlobalIDByFilter reads the filter key written by transaction-service.
// Key format: enrich:filter:{objectClass}:{field}:{value}
func (c *EnrichmentCache) GetGlobalIDByFilter(ctx context.Context, objectClass, field, value string) (int64, bool, error) {
	key := "enrich:filter:" + objectClass + ":" + field + ":" + value
	return c.getInt64(ctx, key)
}

func (c *EnrichmentCache) getInt64(ctx context.Context, key string) (int64, bool, error) {
	val, err := c.rdb.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	gid, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, false, nil // treat unparseable value as miss
	}
	return gid, true, nil
}
