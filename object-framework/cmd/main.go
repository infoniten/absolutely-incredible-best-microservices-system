package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"

	"github.com/quantara/object-framework/internal/cache"
	"github.com/quantara/object-framework/internal/client"
	"github.com/quantara/object-framework/internal/config"
	"github.com/quantara/object-framework/internal/kafka"
	"github.com/quantara/object-framework/internal/processor"
	"github.com/quantara/object-framework/internal/repository"
	"github.com/quantara/object-framework/internal/server"
	"github.com/quantara/object-framework/internal/telemetry"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Println("Starting Object Framework service...")

	// Load configuration
	cfg := config.Load()
	log.Printf("Configuration loaded: topic=%s, group=%s, http_port=%s",
		cfg.KafkaTopic, cfg.KafkaConsumerGroup, cfg.HTTPPort)

	// Setup context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize telemetry
	shutdownTracer, err := telemetry.InitTracer(ctx, cfg.ServiceName, cfg.JaegerEndpoint)
	if err != nil {
		log.Printf("Warning: failed to initialize telemetry: %v", err)
	} else {
		defer func() {
			if err := shutdownTracer(ctx); err != nil {
				log.Printf("Error shutting down tracer: %v", err)
			}
		}()
	}

	// Connect to PostgreSQL
	db, err := sql.Open("postgres", cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Configure connection pool
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}
	log.Println("Connected to PostgreSQL")

	// Initialize repositories
	rawMsgRepo := repository.NewRawMessageRepository(db)
	globalIDRepo := repository.NewGlobalIDMappingRepository(db)

	// Create tables if not exist
	if err := rawMsgRepo.CreateTableIfNotExists(ctx); err != nil {
		log.Fatalf("Failed to create raw_messages table: %v", err)
	}
	if err := globalIDRepo.CreateTableIfNotExists(ctx); err != nil {
		log.Fatalf("Failed to create globalid_mappings table: %v", err)
	}
	log.Println("Database tables initialized")

	// Initialize Redis cache for IdMapping lookups (cluster mode if REDIS_CLUSTER_NODES is set)
	idMapCache, err := cache.NewIdMappingCache(
		cfg.RedisURL,
		cfg.RedisClusterNodes,
		cfg.RedisUsername,
		cfg.RedisPassword,
		cfg.IdMappingCacheTTLSecs,
	)
	if err != nil {
		log.Fatalf("Failed to create IdMapping cache: %v", err)
	}
	defer idMapCache.Close()
	if err := idMapCache.Ping(ctx); err != nil {
		log.Fatalf("Failed to ping Redis: %v", err)
	}
	globalIDRepo.SetCache(idMapCache)
	if len(cfg.RedisClusterNodes) > 0 {
		log.Printf("IdMapping Redis cache enabled (cluster mode, nodes=%v, ttl=%ds)", cfg.RedisClusterNodes, cfg.IdMappingCacheTTLSecs)
	} else {
		log.Printf("IdMapping Redis cache enabled (standalone, url=%s, ttl=%ds)", cfg.RedisURL, cfg.IdMappingCacheTTLSecs)
	}

	// Initialize gRPC clients
	idClient, err := client.NewIDClient(cfg.IDServiceAddr)
	if err != nil {
		log.Fatalf("Failed to create ID client: %v", err)
	}
	defer idClient.Close()
	log.Printf("Connected to ID Service at %s", cfg.IDServiceAddr)

	globalIDClient, err := client.NewGlobalIDClient(cfg.GlobalIDServiceAddr)
	if err != nil {
		log.Fatalf("Failed to create GlobalID client: %v", err)
	}
	defer globalIDClient.Close()
	log.Printf("Connected to GlobalID Service at %s", cfg.GlobalIDServiceAddr)

	// Initialize ID pools for high-throughput
	idPool := client.NewIDPool(idClient, cfg.IDPoolBatchSize, cfg.IDPoolRefillThreshold)
	if err := idPool.Prefill(ctx); err != nil {
		log.Fatalf("Failed to prefill ID pool: %v", err)
	}
	log.Printf("ID pool initialized: size=%d, batch=%d, refill_threshold=%d",
		idPool.Size(), cfg.IDPoolBatchSize, cfg.IDPoolRefillThreshold)

	globalIDPool := client.NewGlobalIDPool(globalIDClient, cfg.IDPoolBatchSize, cfg.IDPoolRefillThreshold)
	if err := globalIDPool.Prefill(ctx); err != nil {
		log.Fatalf("Failed to prefill GlobalID pool: %v", err)
	}
	log.Printf("GlobalID pool initialized: size=%d, batch=%d, refill_threshold=%d",
		globalIDPool.Size(), cfg.IDPoolBatchSize, cfg.IDPoolRefillThreshold)

	lockClient, err := client.NewLockClient(cfg.LockServiceAddr)
	if err != nil {
		log.Fatalf("Failed to create Lock client: %v", err)
	}
	defer lockClient.Close()
	log.Printf("Connected to Lock Service at %s", cfg.LockServiceAddr)

	searchClient, err := client.NewSearchClient(cfg.SearchServiceAddr)
	if err != nil {
		log.Fatalf("Failed to create Search client: %v", err)
	}
	defer searchClient.Close()
	log.Printf("Connected to Search Service at %s", cfg.SearchServiceAddr)

	txClient, err := client.NewTransactionClient(cfg.TransactionServiceAddr)
	if err != nil {
		log.Fatalf("Failed to create Transaction client: %v", err)
	}
	defer txClient.Close()
	log.Printf("Connected to Transaction Service at %s", cfg.TransactionServiceAddr)

	// Initialize processor with ID pools for high throughput
	proc := processor.NewFxSpotProcessor(
		rawMsgRepo,
		globalIDRepo,
		idPool,
		globalIDPool,
		lockClient,
		searchClient,
		txClient,
		cfg.LockTTLMs,
		cfg.LockRetryIntervalMs,
	)
	log.Printf("FxSpot processor initialized: ID pools, lock TTL=%dms, lock retry interval=%dms",
		cfg.LockTTLMs, cfg.LockRetryIntervalMs)

	// Initialize Kafka consumer with worker pool (using ID pool for raw message IDs)
	consumer := kafka.NewConsumer(
		cfg.KafkaBrokers,
		cfg.KafkaTopic,
		cfg.KafkaConsumerGroup,
		proc,
		rawMsgRepo,
		func(ctx context.Context) (int64, error) {
			return idPool.GetID(ctx)
		},
		cfg.WorkerCount,
	)
	log.Printf("Kafka consumer initialized for brokers: %v, workers: %d", cfg.KafkaBrokers, cfg.WorkerCount)

	// Initialize health server
	healthServer := server.NewHealthServer(cfg.HTTPPort, db, consumer.Reader())

	// Setup graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start health server
	go func() {
		if err := healthServer.Start(); err != nil && err != http.ErrServerClosed {
			log.Printf("Health server error: %v", err)
		}
	}()
	log.Printf("Health server started on port %s", cfg.HTTPPort)

	// Start consumer in goroutine
	go func() {
		if err := consumer.Start(ctx); err != nil {
			log.Printf("Kafka consumer error: %v", err)
		}
	}()

	log.Println("Object Framework service started successfully")
	log.Printf("Health endpoints: http://localhost:%s/health, /health/live, /health/ready, /metrics", cfg.HTTPPort)

	// Wait for shutdown signal
	sig := <-sigChan
	log.Printf("Received signal %v, shutting down...", sig)

	// Cancel context to stop consumer
	cancel()

	// Graceful shutdown with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// Shutdown health server
	if err := healthServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("Error shutting down health server: %v", err)
	}

	// Close consumer
	if err := consumer.Close(); err != nil {
		log.Printf("Error closing consumer: %v", err)
	}

	log.Println("Object Framework service stopped")
}
