package testcontainers

import (
	"context"
	"fmt"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
)

// TestContainers holds all test container instances
type TestContainers struct {
	PostgresContainer *postgres.PostgresContainer
	RedisContainer    *redis.RedisContainer
	KafkaContainer    *kafka.KafkaContainer

	PostgresURL string
	RedisURL    string
	KafkaBroker string
}

// StartContainers starts all required containers for integration tests
func StartContainers(ctx context.Context) (*TestContainers, error) {
	tc := &TestContainers{}

	// Start PostgreSQL
	pgContainer, err := postgres.Run(ctx,
		"postgres:15-alpine",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to start postgres: %w", err)
	}
	tc.PostgresContainer = pgContainer

	pgHost, err := pgContainer.Host(ctx)
	if err != nil {
		return nil, err
	}
	pgPort, err := pgContainer.MappedPort(ctx, "5432")
	if err != nil {
		return nil, err
	}
	tc.PostgresURL = fmt.Sprintf("postgres://test:test@%s:%s/testdb?sslmode=disable", pgHost, pgPort.Port())

	// Start Redis
	redisContainer, err := redis.Run(ctx,
		"redis:7-alpine",
		testcontainers.WithWaitStrategy(
			wait.ForLog("Ready to accept connections").
				WithStartupTimeout(30*time.Second)),
	)
	if err != nil {
		tc.Cleanup(ctx)
		return nil, fmt.Errorf("failed to start redis: %w", err)
	}
	tc.RedisContainer = redisContainer

	redisHost, err := redisContainer.Host(ctx)
	if err != nil {
		tc.Cleanup(ctx)
		return nil, err
	}
	redisPort, err := redisContainer.MappedPort(ctx, "6379")
	if err != nil {
		tc.Cleanup(ctx)
		return nil, err
	}
	tc.RedisURL = fmt.Sprintf("redis://%s:%s", redisHost, redisPort.Port())

	// Start Kafka
	kafkaContainer, err := kafka.Run(ctx,
		"confluentinc/cp-kafka:7.5.0",
		kafka.WithClusterID("test-cluster"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("Kafka Server started").
				WithStartupTimeout(60*time.Second)),
	)
	if err != nil {
		tc.Cleanup(ctx)
		return nil, fmt.Errorf("failed to start kafka: %w", err)
	}
	tc.KafkaContainer = kafkaContainer

	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil {
		tc.Cleanup(ctx)
		return nil, err
	}
	if len(brokers) > 0 {
		tc.KafkaBroker = brokers[0]
	}

	return tc, nil
}

// Cleanup terminates all containers
func (tc *TestContainers) Cleanup(ctx context.Context) {
	if tc.KafkaContainer != nil {
		tc.KafkaContainer.Terminate(ctx)
	}
	if tc.RedisContainer != nil {
		tc.RedisContainer.Terminate(ctx)
	}
	if tc.PostgresContainer != nil {
		tc.PostgresContainer.Terminate(ctx)
	}
}
