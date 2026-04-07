package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	// HTTP
	HTTPPort string

	// Kafka
	KafkaBrokers       []string
	KafkaTopic         string
	KafkaConsumerGroup string

	// PostgreSQL
	DatabaseURL string

	// gRPC Services
	IDServiceAddr          string
	GlobalIDServiceAddr    string
	LockServiceAddr        string
	SearchServiceAddr      string
	TransactionServiceAddr string

	// Telemetry
	ServiceName    string
	JaegerEndpoint string

	// Processing
	LockTTLMs   int64
	WorkerCount int // Number of parallel workers for message processing

	// ID Pool settings
	IDPoolBatchSize       int32 // Number of IDs to prefetch in one batch
	IDPoolRefillThreshold int   // Refill pool when size drops below this

	// Lock retry settings
	LockRetryIntervalMs int64 // Fixed interval between lock retry attempts (infinite retry)
}

func Load() *Config {
	return &Config{
		// HTTP
		HTTPPort: getEnv("HTTP_PORT", "8088"),

		// Kafka
		KafkaBrokers:       getEnvAsSlice("KAFKA_BROKERS", []string{"kafka:9092"}),
		KafkaTopic:         getEnv("KAFKA_TOPIC", "moex.trades"),
		KafkaConsumerGroup: getEnv("KAFKA_CONSUMER_GROUP", "object-framework-group"),

		// PostgreSQL
		DatabaseURL: getEnv("DATABASE_URL", "postgres://postgres:postgres@postgres:5432/quantara?sslmode=disable"),

		// gRPC Services
		IDServiceAddr:          getEnv("ID_SERVICE_ADDR", "id-service:50051"),
		GlobalIDServiceAddr:    getEnv("GLOBALID_SERVICE_ADDR", "globalid-service:50052"),
		LockServiceAddr:        getEnv("LOCK_SERVICE_ADDR", "lock-service:50053"),
		SearchServiceAddr:      getEnv("SEARCH_SERVICE_ADDR", "search-service:50055"),
		TransactionServiceAddr: getEnv("TRANSACTION_SERVICE_ADDR", "transaction-service:50054"),

		// Telemetry
		ServiceName:    getEnv("SERVICE_NAME", "object-framework"),
		JaegerEndpoint: getEnv("JAEGER_ENDPOINT", "jaeger:4318"),

		// Processing
		LockTTLMs:   getEnvAsInt64("LOCK_TTL_MS", 30000),
		WorkerCount: int(getEnvAsInt64("WORKER_COUNT", 100)),

		// ID Pool settings
		IDPoolBatchSize:       int32(getEnvAsInt64("ID_POOL_BATCH_SIZE", 2000)),
		IDPoolRefillThreshold: int(getEnvAsInt64("ID_POOL_REFILL_THRESHOLD", 500)),

		// Lock retry settings
		LockRetryIntervalMs: getEnvAsInt64("LOCK_RETRY_INTERVAL_MS", 100), // 100ms default
	}
}

func (c *Config) KafkaDialTimeout() time.Duration {
	return 10 * time.Second
}

func (c *Config) GRPCDialTimeout() time.Duration {
	return 5 * time.Second
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

func getEnvAsSlice(key string, defaultValue []string) []string {
	if value, exists := os.LookupEnv(key); exists {
		return strings.Split(value, ",")
	}
	return defaultValue
}

func getEnvAsInt64(key string, defaultValue int64) int64 {
	if value, exists := os.LookupEnv(key); exists {
		if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
			return intVal
		}
	}
	return defaultValue
}
