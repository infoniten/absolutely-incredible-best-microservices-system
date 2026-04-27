package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/quantara/transaction-service/internal/config"
	txkafka "github.com/quantara/transaction-service/internal/kafka"
	"github.com/quantara/transaction-service/internal/metamodel"
	"github.com/quantara/transaction-service/internal/redis"
	"github.com/quantara/transaction-service/internal/server"
	"github.com/quantara/transaction-service/internal/telemetry"
	pb "github.com/quantara/transaction-service/proto"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Load configuration
	cfg := config.Load()
	log.Printf("Starting transaction-service (grpc=%s, http=%s)", cfg.GRPCPort, cfg.HTTPPort)

	// Initialize telemetry
	tel, err := telemetry.New(ctx, cfg.ServiceName, cfg.JaegerEndpoint)
	if err != nil {
		log.Printf("Warning: failed to initialize telemetry: %v", err)
	} else {
		defer tel.Shutdown(ctx)
	}

	// Connect to PostgreSQL
	db, err := sql.Open("postgres", cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Configure connection pool
	db.SetMaxOpenConns(cfg.MaxDBConnections)
	db.SetMaxIdleConns(cfg.MaxDBConnections / 2)
	db.SetConnMaxLifetime(5 * time.Minute)
	log.Printf("Database pool configured: max=%d, idle=%d", cfg.MaxDBConnections, cfg.MaxDBConnections/2)

	// Verify database connection
	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}
	log.Println("Connected to PostgreSQL")

	// Connect to Redis
	redisClient, err := redis.NewClient(cfg.RedisURL, cfg.RedisClusterNodes, cfg.RedisUsername, cfg.RedisPassword, cfg.TxTTLSeconds)
	if err != nil {
		log.Fatalf("Failed to create Redis client: %v", err)
	}
	defer redisClient.Close()

	// Verify Redis connection
	if err := redisClient.Ping(ctx); err != nil {
		log.Fatalf("Failed to ping Redis: %v", err)
	}
	log.Println("Connected to Redis")

	// Load metamodel from DataDictionary
	metamodelCache := metamodel.NewCache(cfg.DataDictionaryURL)
	loadCtx, loadCancel := context.WithTimeout(ctx, 30*time.Second)
	defer loadCancel()

	if err := metamodelCache.Load(loadCtx); err != nil {
		log.Printf("Warning: failed to load metamodel: %v", err)
		log.Println("Service will start but metamodel lookups may fail")
	} else {
		log.Printf("Loaded metamodel: %d types", metamodelCache.TypeCount())
	}

	// Warmup enrichment filter cache (with distributed lock for multi-replica safety)
	if len(cfg.EnrichFilterFields) > 0 {
		mainTables, dataTables := server.BuildTableMappings(
			cfg.EnrichFilterFields,
			metamodelCache.GetMainTable,
			metamodelCache.GetDataTable,
		)
		filterTTL := time.Duration(cfg.EnrichFilterCacheTTLSecs) * time.Second
		// DB schema is "murex" — same as search-service uses
		dbSchema := "murex"
		server.WarmupEnrichFilterCache(ctx, redisClient, db, dbSchema, cfg.EnrichFilterFields, filterTTL, mainTables, dataTables)
	}

	// Create gRPC server
	var opts []grpc.ServerOption
	if tel != nil {
		opts = append(opts,
			grpc.StatsHandler(otelgrpc.NewServerHandler()),
		)
	}

	grpcServer := grpc.NewServer(opts...)

	// Register transaction service
	var tracer trace.Tracer
	if tel != nil {
		tracer = tel.Tracer
	}
	// Kafka producer for trade events (nil if KAFKA_BROKERS not configured)
	var kafkaProducer *txkafka.Producer
	if len(cfg.KafkaBrokers) > 0 {
		if err := txkafka.EnsureTopicExists(cfg.KafkaBrokers, cfg.KafkaTradesTopic, 10, 1); err != nil {
			log.Printf("Warning: failed to ensure Kafka topic %s: %v", cfg.KafkaTradesTopic, err)
		}
		kafkaProducer = txkafka.NewProducer(cfg.KafkaBrokers, cfg.KafkaTradesTopic)
		if kafkaProducer != nil {
			defer kafkaProducer.Close()
			log.Printf("Kafka producer initialized: brokers=%v, topic=%s", cfg.KafkaBrokers, cfg.KafkaTradesTopic)
		}
	}

	txServer := server.NewServer(cfg, redisClient, metamodelCache, db, tracer, kafkaProducer)
	pb.RegisterTransactionServiceServer(grpcServer, txServer)

	// Register health service
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	// Enable reflection for debugging
	reflection.Register(grpcServer)

	// Start listening
	lis, err := net.Listen("tcp", ":"+cfg.GRPCPort)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	opsMux := http.NewServeMux()
	opsMux.Handle("/metrics", promhttp.Handler())

	opsServer := &http.Server{
		Addr:              ":" + cfg.HTTPPort,
		Handler:           opsMux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Graceful shutdown
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh

		log.Println("Shutting down gracefully...")
		healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
		grpcServer.GracefulStop()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := opsServer.Shutdown(shutdownCtx); err != nil {
			log.Printf("Warning: failed to shutdown HTTP ops server: %v", err)
		}
		cancel()
	}()

	go func() {
		log.Printf("Transaction HTTP ops listening on :%s", cfg.HTTPPort)
		if err := opsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Failed to serve HTTP ops endpoints: %v", err)
		}
	}()

	// Start serving
	log.Printf("Transaction service listening on :%s", cfg.GRPCPort)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
