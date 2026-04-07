package config

import (
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
