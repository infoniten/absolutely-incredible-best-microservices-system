package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	redisclient "github.com/quantara/transaction-service/internal/redis"
)

const (
	enrichWarmupLockKey = "enrich:warmup:lock"
	enrichWarmupLockTTL = 60 * time.Second
)

// WarmupEnrichFilterCache pre-populates enrich:filter: Redis keys from the database.
// Uses a distributed lock so that only one replica in a multi-instance deployment
// performs the warmup. Other replicas skip if the lock is held.
func WarmupEnrichFilterCache(
	ctx context.Context,
	redis *redisclient.Client,
	db *sql.DB,
	dbSchema string,
	enrichFields map[string]map[string]string,
	ttl time.Duration,
	classToMainTable map[string]string,
	classToDataTable map[string]string,
) {
	if redis == nil || len(enrichFields) == 0 {
		return
	}

	// Try to acquire distributed lock (SETNX). If another replica already holds it, skip.
	acquired, err := redis.TryLock(ctx, enrichWarmupLockKey, enrichWarmupLockTTL)
	if err != nil {
		log.Printf("Enrichment warmup: failed to acquire lock: %v", err)
		return
	}
	if !acquired {
		log.Println("Enrichment warmup: skipped (another instance is warming up)")
		return
	}

	log.Println("Enrichment warmup: starting...")
	total := 0

	for objectClass, fieldMap := range enrichFields {
		mainTable := classToMainTable[objectClass]
		dataTable := classToDataTable[objectClass]
		if mainTable == "" || dataTable == "" {
			log.Printf("Enrichment warmup: skipping %s (no table mapping)", objectClass)
			continue
		}

		if dbSchema != "" {
			mainTable = dbSchema + "." + mainTable
			dataTable = dbSchema + "." + dataTable
		}

		count, err := warmupClass(ctx, redis, db, objectClass, mainTable, dataTable, fieldMap, ttl)
		if err != nil {
			log.Printf("Enrichment warmup: %s failed: %v", objectClass, err)
			continue
		}
		total += count
		log.Printf("Enrichment warmup: %s — %d entries", objectClass, count)
	}

	log.Printf("Enrichment warmup complete: %d cache entries written", total)
}

func warmupClass(
	ctx context.Context,
	redis *redisclient.Client,
	db *sql.DB,
	objectClass, mainTable, dataTable string,
	fieldMap map[string]string,
	ttl time.Duration,
) (int, error) {
	query := fmt.Sprintf(
		`SELECT m.global_id, d.content
		 FROM %s m
		 JOIN %s d ON d.id = m.id
		 WHERE lower(m.object_class) = lower($1)
		   AND now() >= m.saved_at AND now() < m.closed_at`,
		mainTable, dataTable,
	)

	rows, err := db.QueryContext(ctx, query, objectClass)
	if err != nil {
		return 0, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	count := 0
	rowCount := 0
	for rows.Next() {
		rowCount++
		var globalID int64
		var contentRaw string
		if err := rows.Scan(&globalID, &contentRaw); err != nil {
			log.Printf("Enrichment warmup: %s scan error: %v", objectClass, err)
			continue
		}
		if globalID == 0 {
			log.Printf("Enrichment warmup: %s skipping row with globalID=0", objectClass)
			continue
		}

		var content map[string]json.RawMessage
		if err := json.Unmarshal([]byte(contentRaw), &content); err != nil {
			log.Printf("Enrichment warmup: %s JSON parse error for globalID=%d: %v", objectClass, globalID, err)
			continue
		}

		for filterField, jsonKey := range fieldMap {
			raw, ok := content[jsonKey]
			if !ok {
				log.Printf("Enrichment warmup: %s globalID=%d missing JSON key %q", objectClass, globalID, jsonKey)
				continue
			}
			var value string
			if err := json.Unmarshal(raw, &value); err != nil {
				log.Printf("Enrichment warmup: %s globalID=%d key %q unmarshal error: %v", objectClass, globalID, jsonKey, err)
				continue
			}
			if value == "" {
				continue
			}

			key := "enrich:filter:" + objectClass + ":" + filterField + ":" + value
			if err := redis.PutEnrichFilter(ctx, key, globalID, ttl); err != nil {
				log.Printf("Enrichment warmup: %s failed to write key %s: %v", objectClass, key, err)
				continue
			}
			count++
		}
	}
	log.Printf("Enrichment warmup: %s queried %d rows from DB", objectClass, rowCount)

	return count, rows.Err()
}

// BuildTableMappings extracts mainTable and dataTable for each enrichment class from metamodel.
func BuildTableMappings(
	enrichFields map[string]map[string]string,
	getMainTable func(string) (string, error),
	getDataTable func(string) (string, error),
) (mainTables, dataTables map[string]string) {
	mainTables = make(map[string]string, len(enrichFields))
	dataTables = make(map[string]string, len(enrichFields))

	for className := range enrichFields {
		mt, err := getMainTable(className)
		if err != nil {
			log.Printf("Enrichment warmup: no main table for %s: %v", className, err)
			continue
		}
		dt, err := getDataTable(className)
		if err != nil {
			log.Printf("Enrichment warmup: no data table for %s: %v", className, err)
			continue
		}
		mainTables[className] = mt
		dataTables[className] = dt
		log.Printf("Enrichment warmup: mapped %s -> main=%s, data=%s", className, mt, dt)
	}
	return
}

