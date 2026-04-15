package client

import "context"

// EnrichmentSearchClient is the interface used by the processor for enrichment and object lookups.
// Both SearchClient (gRPC-only) and CachedSearchClient (Redis + gRPC fallback) implement it.
type EnrichmentSearchClient interface {
	GetObjectByGlobalID(ctx context.Context, objectClass string, globalID int64, draftStatus, actualDate string) (map[string]interface{}, error)
	GetGlobalIDByAltIDOrNull(ctx context.Context, objectClass, altID, sourceAlias string) (*int64, error)
	FindGlobalIDByFilter(ctx context.Context, objectClass, field, value string) (*int64, error)
}
