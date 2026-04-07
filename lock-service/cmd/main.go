package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"github.com/quantara/lock-service/internal/cache"
	"github.com/quantara/lock-service/internal/config"
	"github.com/quantara/lock-service/internal/cron"
	"github.com/quantara/lock-service/internal/repository"
	"github.com/quantara/lock-service/internal/server"
	"github.com/quantara/lock-service/internal/telemetry"
	pb "github.com/quantara/lock-service/proto"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := config.Load()

	// Initialize telemetry
	tel, err := telemetry.New(ctx, cfg.ServiceName, cfg.JaegerEndpoint)
	if err != nil {
		log.Fatalf("failed to initialize telemetry: %v", err)
	}
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := tel.Shutdown(shutdownCtx); err != nil {
			log.Printf("failed to shutdown telemetry: %v", err)
		}
	}()

	// Connect to database
	db, err := sql.Open("postgres", cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer db.Close()

	// Configure connection pool for high load
	db.SetMaxOpenConns(cfg.MaxDBConnections)
	db.SetMaxIdleConns(cfg.MaxDBConnections / 2)
	db.SetConnMaxLifetime(cfg.DBConnMaxLifetime)

	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("failed to ping database: %v", err)
	}
	log.Println("connected to database")

	// Initialize Redis cache (optional)
	var lockCache *cache.LockCache
	if cfg.CacheEnabled {
		var err error
		lockCache, err = cache.NewLockCache(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
		if err != nil {
			log.Printf("WARNING: failed to connect to Redis cache: %v (continuing without cache)", err)
		} else {
			log.Printf("connected to Redis cache at %s", cfg.RedisAddr)
			defer lockCache.Close()
		}
	}

	// Initialize repository
	repo := repository.NewLockRepository(db, lockCache, tel.Tracer)

	// Start cleanup cron job
	cleanupJob := cron.NewCleanupJob(repo, tel.Tracer, cfg.CleanupInterval)
	cleanupJob.Start()
	defer cleanupJob.Stop()

	// Create gRPC server
	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)

	// Register services
	srv := server.New(repo, tel.Tracer, cfg.DefaultTTL)
	pb.RegisterLockServiceServer(grpcServer, srv)

	// Health check
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	// Reflection for debugging
	reflection.Register(grpcServer)

	// Start server
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", cfg.GRPCPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	go func() {
		log.Printf("starting gRPC server on port %s", cfg.GRPCPort)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("shutting down...")
	grpcServer.GracefulStop()
}
