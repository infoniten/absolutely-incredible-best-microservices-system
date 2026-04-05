package config

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	GRPCPort           string
	DatabaseURL        string
	RedisURL           string
	RedisClusterNodes  []string
	DataDictionaryURL  string
	JaegerEndpoint     string
	ServiceName        string
	TxTTLSeconds       int
	CommitChunkSize    int
}

func Load() *Config {
	return &Config{
		GRPCPort:           getEnv("GRPC_PORT", "50054"),
		DatabaseURL:        getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/quantara?sslmode=disable"),
		RedisURL:           getEnv("REDIS_URL", "redis://localhost:6379/0"),
		RedisClusterNodes:  getEnvSlice("REDIS_CLUSTER_NODES", nil),
		DataDictionaryURL:  getEnv("DATADICTIONARY_URL", "http://localhost:8083"),
		JaegerEndpoint:     getEnv("JAEGER_ENDPOINT", "localhost:4318"),
		ServiceName:        getEnv("SERVICE_NAME", "transaction-service"),
		TxTTLSeconds:       getEnvInt("TX_TTL_SECONDS", 600),
		CommitChunkSize:    getEnvInt("COMMIT_CHUNK_SIZE", 5000),
	}
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
