//go:build integration

package integration

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"

	"github.com/quantara/object-framework/internal/cache"
	"github.com/quantara/object-framework/internal/domain"
	"github.com/quantara/object-framework/internal/repository"
	tc "github.com/quantara/object-framework/test/testcontainers"
)

// Verifies the IdMapping Redis cache: first FindByExternalID hits Postgres and
// primes Redis; the second hit returns from Redis (proven by stopping Postgres
// access via wrong DSN — instead, we read the key directly to confirm contents).
func TestIdMappingCache_HitMissAndPrime(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	containers, err := tc.StartContainers(ctx)
	if err != nil {
		t.Fatalf("start containers: %v", err)
	}
	defer containers.Cleanup(context.Background())

	db, err := sql.Open("postgres", containers.PostgresURL)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	repo := repository.NewGlobalIDMappingRepository(db)
	if err := repo.CreateTableIfNotExists(ctx); err != nil {
		t.Fatalf("create table: %v", err)
	}

	idCache, err := cache.NewIdMappingCache(containers.RedisURL, nil, "", "", 600)
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}
	defer idCache.Close()
	if err := idCache.Ping(ctx); err != nil {
		t.Fatalf("ping redis: %v", err)
	}
	repo.SetCache(idCache)

	const (
		extID      = "TRADE-12345"
		source     = domain.SourceMOEX
		objectType = domain.SourceObjectTypeFXSPOTTrade
		mappingID  = int64(111)
		globalID   = int64(999)
	)

	// 1) Miss: nothing in cache, nothing in DB.
	got, err := repo.FindByExternalID(ctx, extID, source, objectType)
	if err != nil {
		t.Fatalf("find (miss): %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil on miss, got %+v", got)
	}

	// 2) Create -> should prime cache.
	if _, _, err := repo.GetOrCreate(ctx, extID, source, objectType, mappingID, globalID); err != nil {
		t.Fatalf("get or create: %v", err)
	}

	// Verify Redis directly.
	rdb := redis.NewClient(mustParseURL(t, containers.RedisURL))
	defer rdb.Close()
	val, err := rdb.Get(ctx, "idmap:"+source+":"+objectType+":"+extID).Result()
	if err != nil {
		t.Fatalf("redis get after create: %v", err)
	}
	if val != "999" {
		t.Fatalf("expected cached value 999, got %q", val)
	}

	// 3) Subsequent FindByExternalID resolves from cache. Drop the DB table to
	//    prove Postgres is not consulted: a cache hit must still return globalID.
	if _, err := db.ExecContext(ctx, "DROP TABLE globalid_mappings_go"); err != nil {
		t.Fatalf("drop table: %v", err)
	}

	got, err = repo.FindByExternalID(ctx, extID, source, objectType)
	if err != nil {
		t.Fatalf("find (cache hit): %v", err)
	}
	if got == nil || got.GlobalID != globalID {
		t.Fatalf("expected cached globalID=%d, got %+v", globalID, got)
	}
}

func mustParseURL(t *testing.T, url string) *redis.Options {
	t.Helper()
	opt, err := redis.ParseURL(url)
	if err != nil {
		t.Fatalf("parse redis url: %v", err)
	}
	return opt
}
