package config

import (
	"os"
	"strconv"
)

type Config struct {
	GRPCPort               string
	DatabaseURL            string
	DatabaseSchema         string
	RedisURL               string
	DataDictionaryURL      string
	DomainConfigURL        string
	DomainConfigLocalFile  string
	FilterJSONFieldEnabled bool
	JaegerEndpoint         string
	ServiceName            string
	MaxDBConnections       int
}

func Load() *Config {
	return &Config{
		GRPCPort:               getEnv("GRPC_PORT", "50055"),
		DatabaseURL:            getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/quantara?sslmode=disable"),
		DatabaseSchema:         getEnv("DB_SCHEMA", "murex"),
		RedisURL:               getEnv("REDIS_URL", "redis://localhost:6379/0"),
		DataDictionaryURL:      getEnv("DATADICTIONARY_URL", ""),
		DomainConfigURL:        resolveDomainConfigURL(),
		DomainConfigLocalFile:  getEnv("DOMAIN_CONFIG_LOCAL_FILE", "domain-config.json"),
		FilterJSONFieldEnabled: getEnvBool("SEARCH_FILTER_JSON_FIELD_ENABLED", false),
		JaegerEndpoint:         getEnv("JAEGER_ENDPOINT", "localhost:4318"),
		ServiceName:            getEnv("SERVICE_NAME", "search-service"),
		MaxDBConnections:       getEnvInt("MAX_DB_CONNECTIONS", 50),
	}
}

func resolveDomainConfigURL() string {
	if value := getEnv("DOMAIN_CONFIG_URL", ""); value != "" {
		return value
	}

	if base := getEnv("DATADICTIONARY_URL", ""); base != "" {
		return trimSlash(base) + "/api/search-service/metadata"
	}

	// Default for local development with compose port mapping.
	return "http://localhost:8083/api/search-service/metadata"
}

func trimSlash(value string) string {
	for len(value) > 0 && value[len(value)-1] == '/' {
		value = value[:len(value)-1]
	}
	return value
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return defaultValue
	}
	return parsed
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}
