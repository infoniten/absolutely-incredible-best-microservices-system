package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	GRPCPort          string
	DatabaseURL       string
	JaegerEndpoint    string
	ServiceName       string
	DefaultTTL        time.Duration
	CleanupInterval   time.Duration
	MaxDBConnections  int
	DBConnMaxLifetime time.Duration

	// Redis cache
	RedisAddr     string
	RedisPassword string
	RedisDB       int
	CacheEnabled  bool
}

func Load() *Config {
	return &Config{
		GRPCPort:          getEnv("GRPC_PORT", "50053"),
		DatabaseURL:       getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/quantara?sslmode=disable"),
		JaegerEndpoint:    getEnv("JAEGER_ENDPOINT", "http://localhost:4318"),
		ServiceName:       getEnv("SERVICE_NAME", "lock-service"),
		DefaultTTL:        getEnvDuration("DEFAULT_TTL_MS", 10000*time.Millisecond),
		CleanupInterval:   getEnvDuration("CLEANUP_INTERVAL", 1*time.Hour),
		MaxDBConnections:  getEnvInt("MAX_DB_CONNECTIONS", 50),
		DBConnMaxLifetime: getEnvDuration("DB_CONN_MAX_LIFETIME", 5*time.Minute),

		// Redis cache
		RedisAddr:     getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword: getEnv("REDIS_PASSWORD", ""),
		RedisDB:       getEnvInt("REDIS_DB", 0),
		CacheEnabled:  getEnvBool("CACHE_ENABLED", true),
	}
}

func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		return value == "true" || value == "1"
	}
	return defaultValue
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

func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.ParseInt(value, 10, 64); err == nil {
			return time.Duration(intValue) * time.Millisecond
		}
	}
	return defaultValue
}
