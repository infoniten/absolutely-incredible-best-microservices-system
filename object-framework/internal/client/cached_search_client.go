package client

import (
	"context"
	"log"

	"github.com/quantara/object-framework/internal/cache"
)

// CachedSearchClient wraps SearchClient with a Redis cache layer for enrichment lookups.
// On cache miss it falls back to the gRPC SearchClient transparently.
type CachedSearchClient struct {
	search *SearchClient
	cache  *cache.EnrichmentCache
}

// NewCachedSearchClient creates a CachedSearchClient. If enrichCache is nil, all calls
// go directly to gRPC (equivalent to using SearchClient directly).
func NewCachedSearchClient(search *SearchClient, enrichCache *cache.EnrichmentCache) *CachedSearchClient {
	return &CachedSearchClient{search: search, cache: enrichCache}
}

// GetObjectByGlobalID delegates directly to gRPC (not cached — version/revision change on each update).
func (c *CachedSearchClient) GetObjectByGlobalID(ctx context.Context, objectClass string, globalID int64, draftStatus, actualDate string) (map[string]interface{}, error) {
	return c.search.GetObjectByGlobalID(ctx, objectClass, globalID, draftStatus, actualDate)
}

// GetGlobalIDByAltIDOrNull checks Redis first (alt:{class}:{source}:{altID}), falls back to gRPC.
func (c *CachedSearchClient) GetGlobalIDByAltIDOrNull(ctx context.Context, objectClass, altID, sourceAlias string) (*int64, error) {
	if c.cache != nil {
		gid, found, err := c.cache.GetGlobalIDByAltID(ctx, objectClass, altID, sourceAlias)
		if err != nil {
			log.Printf("enrichment cache altId error (falling back to gRPC): %v", err)
		} else if found {
			return &gid, nil
		}
	}
	return c.search.GetGlobalIDByAltIDOrNull(ctx, objectClass, altID, sourceAlias)
}

// FindGlobalIDByFilter checks Redis first (enrich:filter:{class}:{field}:{value}), falls back to gRPC.
func (c *CachedSearchClient) FindGlobalIDByFilter(ctx context.Context, objectClass, field, value string) (*int64, error) {
	if c.cache != nil {
		gid, found, err := c.cache.GetGlobalIDByFilter(ctx, objectClass, field, value)
		if err != nil {
			log.Printf("enrichment cache filter error (falling back to gRPC): %v", err)
		} else if found {
			return &gid, nil
		}
	}
	return c.search.FindGlobalIDByFilter(ctx, objectClass, field, value)
}

// Close closes the underlying gRPC connection.
func (c *CachedSearchClient) Close() error {
	return c.search.Close()
}
