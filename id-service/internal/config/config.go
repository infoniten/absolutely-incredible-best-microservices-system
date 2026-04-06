package config

import (
	"os"
	"strconv"
)

type Config struct {
	GRPCPort         string
	DatabaseURL      string
	PoolSize         int
	JaegerEndpoint   string
	ServiceName      string
	MaxDBConnections int
}

func Load() *Config {
	return &Config{
		GRPCPort:         getEnv("GRPC_PORT", "50051"),
		DatabaseURL:      getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/quantara?sslmode=disable"),
		PoolSize:         getEnvInt("ID_POOL_SIZE", 1000),
		JaegerEndpoint:   getEnv("JAEGER_ENDPOINT", "http://localhost:4318"),
		ServiceName:      getEnv("SERVICE_NAME", "id-service"),
		MaxDBConnections: getEnvInt("MAX_DB_CONNECTIONS", 50),
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
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}
