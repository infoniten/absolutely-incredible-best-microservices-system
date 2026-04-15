package config

import (
	"encoding/json"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	GRPCPort          string
	HTTPPort          string
	DatabaseURL       string
	RedisURL          string
	RedisClusterNodes []string
	RedisUsername     string
	RedisPassword     string
	DataDictionaryURL string
	JaegerEndpoint    string
	ServiceName       string
	TxTTLSeconds      int
	CommitChunkSize   int
	MaxDBConnections  int

	// Enrichment filter cache: transaction-service writes enrich:filter: keys on commit
	EnrichFilterCacheTTLSecs int
	EnrichFilterFields       map[string]map[string]string // objectClass → {filterField → jsonKey}
}

func Load() *Config {
	return &Config{
		GRPCPort:          getEnv("GRPC_PORT", "50054"),
		HTTPPort:          getEnv("HTTP_PORT", "8087"),
		DatabaseURL:       getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/quantara?sslmode=disable"),
		RedisURL:          getEnv("REDIS_URL", "redis://localhost:6379/0"),
		RedisClusterNodes: getEnvSlice("REDIS_CLUSTER_NODES", nil),
		RedisUsername:     getEnv("REDIS_USERNAME", ""),
		RedisPassword:     getEnv("REDIS_PASSWORD", ""),
		DataDictionaryURL: getEnv("DATADICTIONARY_URL", "http://localhost:8083"),
		JaegerEndpoint:    getEnv("JAEGER_ENDPOINT", "localhost:4318"),
		ServiceName:       getEnv("SERVICE_NAME", "transaction-service"),
		TxTTLSeconds:      getEnvInt("TX_TTL_SECONDS", 600),
		CommitChunkSize:   getEnvInt("COMMIT_CHUNK_SIZE", 5000),
		MaxDBConnections:  getEnvInt("MAX_DB_CONNECTIONS", 50),

		EnrichFilterCacheTTLSecs: getEnvInt("ENRICH_FILTER_CACHE_TTL_SECONDS", 86400),
		EnrichFilterFields:       loadEnrichFilterFields(),
	}
}

// loadEnrichFilterFields returns the mapping of objectClass → {filterField → jsonKey}.
// Can be overridden via ENRICH_FILTER_FIELDS env var (JSON format).
func loadEnrichFilterFields() map[string]map[string]string {
	defaults := map[string]map[string]string{
		"System":       {"System.code": "code"},
		"Venue":        {"Venue.code": "code"},
		"Currency":     {"Asset.code": "code"},
		"Counterparty": {"Counterparty.code": "code"},
		"FxPair":       {"Instrument.code": "code"},
	}

	raw := os.Getenv("ENRICH_FILTER_FIELDS")
	if raw == "" {
		return defaults
	}

	var parsed map[string]map[string]string
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return defaults
	}
	return parsed
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if i, err := strconv.Atoi(value); err == nil {
			return i
		}
	}
	return defaultValue
}

func getEnvSlice(key string, defaultValue []string) []string {
	if value := os.Getenv(key); value != "" {
		return strings.Split(value, ",")
	}
	return defaultValue
}
