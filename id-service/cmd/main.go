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

	"github.com/quantara/id-service/internal/config"
	"github.com/quantara/id-service/internal/idpool"
	"github.com/quantara/id-service/internal/logging"
	"github.com/quantara/id-service/internal/server"
	"github.com/quantara/id-service/internal/telemetry"
	pb "github.com/quantara/id-service/proto"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := config.Load()
	logCleanup, err := logging.Setup(cfg.ServiceName, cfg.GRPCPort)
	if err != nil {
		log.Fatalf("failed to initialize logger: %v", err)
	}
	defer logCleanup()

	// Initialize telemetry
	tel, err := telemetry.New(ctx, cfg.ServiceName, cfg.JaegerEndpoint)
	if err != nil {
		log.Fatalf("failed to initialize telemetry: %v", err)
	}
	defer func() {
		if err := tel.Shutdown(ctx); err != nil {
			log.Printf("failed to shutdown telemetry: %v", err)
		}
	}()

	// Connect to database
	db, err := sql.Open("postgres", cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer db.Close()

	// Configure connection pool
	db.SetMaxOpenConns(cfg.MaxDBConnections)
	db.SetMaxIdleConns(cfg.MaxDBConnections / 2)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("failed to ping database: %v", err)
	}
	log.Printf("connected to database (pool: max=%d, idle=%d)", cfg.MaxDBConnections, cfg.MaxDBConnections/2)

	// Initialize ID pool
	pool := idpool.New(db, tel.Tracer, cfg.PoolSize)

	// Create gRPC server
	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)

	// Register services
	srv := server.New(pool, tel.Tracer)
	pb.RegisterIDServiceServer(grpcServer, srv)

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
