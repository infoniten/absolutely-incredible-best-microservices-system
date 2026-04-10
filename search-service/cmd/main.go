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
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"github.com/quantara/search-service/internal/cache"
	"github.com/quantara/search-service/internal/config"
	"github.com/quantara/search-service/internal/domainconfig"
	"github.com/quantara/search-service/internal/filter"
	"github.com/quantara/search-service/internal/logging"
	"github.com/quantara/search-service/internal/registry"
	"github.com/quantara/search-service/internal/repository"
	"github.com/quantara/search-service/internal/server"
	"github.com/quantara/search-service/internal/service"
	"github.com/quantara/search-service/internal/telemetry"
	pb "github.com/quantara/search-service/proto"
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

	loader := domainconfig.New(cfg.DomainConfigURL, cfg.DomainConfigLocalFile)
	domainCfg, err := loader.Load(ctx)
	if err != nil {
		log.Fatalf("failed to load domain config: %v", err)
	}
	if info, err := loader.Info(); err == nil {
		log.Printf("domain config loaded: source=%s location=%s hash=%s", info.Source, info.Location, info.Hash)
	}

	objectClassRegistry, err := registry.NewObjectClassRegistry(domainCfg)
	if err != nil {
		log.Fatalf("failed to initialize object class registry: %v", err)
	}
	hierarchyRegistry := registry.NewObjectClassHierarchyRegistry(domainCfg, objectClassRegistry)
	jsonFieldRegistry := registry.NewJsonFieldRegistry(domainCfg, objectClassRegistry)

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

	var redisClient redis.UniversalClient
	if len(cfg.RedisClusterNodes) > 0 {
		// Cluster mode
		client := redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:    cfg.RedisClusterNodes,
			Username: cfg.RedisUsername,
			Password: cfg.RedisPassword,
		})
		if err := client.Ping(ctx).Err(); err != nil {
			log.Printf("redis cluster is unavailable, caching disabled: %v", err)
			_ = client.Close()
		} else {
			log.Printf("connected to redis cluster (%d nodes)", len(cfg.RedisClusterNodes))
			redisClient = client
			defer redisClient.Close()
		}
	} else {
		// Standalone mode
		redisOptions, err := redis.ParseURL(cfg.RedisURL)
		if err != nil {
			log.Fatalf("failed to parse REDIS_URL: %v", err)
		}
		client := redis.NewClient(redisOptions)
		if err := client.Ping(ctx).Err(); err != nil {
			log.Printf("redis is unavailable, caching disabled: %v", err)
			_ = client.Close()
		} else {
			log.Println("connected to redis")
			redisClient = client
			defer redisClient.Close()
		}
	}

	searchRepository := repository.NewSearchRepository(db, cfg.DatabaseSchema)
	cacheService := cache.NewSearchCacheService(redisClient, objectClassRegistry, hierarchyRegistry)

	filterBuilder := filter.NewTypedFilterSQLBuilder()
	indexSourceFactory := filter.NewIndexSourceFactory(domainCfg, objectClassRegistry)
	joinPlanner := filter.NewJoinPlanner(cfg.DatabaseSchema)
	fieldRegistryFactory := filter.NewFilterFieldRegistryFactory(
		cfg.FilterJSONFieldEnabled,
		objectClassRegistry,
		hierarchyRegistry,
		jsonFieldRegistry,
	)
	typedFilterService := filter.NewTypedFilterService(
		filterBuilder,
		indexSourceFactory,
		joinPlanner,
		fieldRegistryFactory,
		cfg.DatabaseSchema,
	)

	searchService := service.NewSearchService(
		searchRepository,
		cacheService,
		typedFilterService,
		objectClassRegistry,
		hierarchyRegistry,
	)

	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)

	searchServer := server.New(searchService, loader, tel.Tracer)
	pb.RegisterSearchServiceServer(grpcServer, searchServer)

	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	reflection.Register(grpcServer)

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

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("shutting down...")
	grpcServer.GracefulStop()
}
