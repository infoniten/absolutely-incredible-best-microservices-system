package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// HealthServer provides HTTP endpoints for health checks and metrics
type HealthServer struct {
	db           *sql.DB
	kafkaReader  *kafka.Reader
	httpServer   *http.Server

	// Status tracking
	mu              sync.RWMutex
	messagesProcessed int64
	lastProcessedAt   time.Time
	lastError         string
}

// NewHealthServer creates a new health server
func NewHealthServer(port string, db *sql.DB, kafkaReader *kafka.Reader) *HealthServer {
	hs := &HealthServer{
		db:          db,
		kafkaReader: kafkaReader,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", hs.handleHealth)
	mux.HandleFunc("/health/live", hs.handleLiveness)
	mux.HandleFunc("/health/ready", hs.handleReadiness)
	mux.HandleFunc("/metrics", hs.handleMetrics)

	hs.httpServer = &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	return hs
}

// Start starts the HTTP server
func (hs *HealthServer) Start() error {
	log.Printf("Starting health server on %s", hs.httpServer.Addr)
	return hs.httpServer.ListenAndServe()
}

// Shutdown gracefully shuts down the server
func (hs *HealthServer) Shutdown(ctx context.Context) error {
	return hs.httpServer.Shutdown(ctx)
}

// RecordProcessed records a successfully processed message
func (hs *HealthServer) RecordProcessed() {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	hs.messagesProcessed++
	hs.lastProcessedAt = time.Now()
	hs.lastError = ""
}

// RecordError records a processing error
func (hs *HealthServer) RecordError(err string) {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	hs.lastError = err
}

// HealthResponse represents the health check response
type HealthResponse struct {
	Status    string            `json:"status"`
	Timestamp string            `json:"timestamp"`
	Checks    map[string]Check  `json:"checks"`
}

// Check represents a single health check
type Check struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

// handleHealth returns overall health status
func (hs *HealthServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	checks := make(map[string]Check)
	overallStatus := "UP"

	// Check database
	dbCheck := hs.checkDatabase(ctx)
	checks["database"] = dbCheck
	if dbCheck.Status != "UP" {
		overallStatus = "DOWN"
	}

	// Check Kafka
	kafkaCheck := hs.checkKafka()
	checks["kafka"] = kafkaCheck
	if kafkaCheck.Status != "UP" {
		overallStatus = "DOWN"
	}

	response := HealthResponse{
		Status:    overallStatus,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Checks:    checks,
	}

	w.Header().Set("Content-Type", "application/json")
	if overallStatus != "UP" {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	json.NewEncoder(w).Encode(response)
}

// handleLiveness returns liveness status (is the process alive)
func (hs *HealthServer) handleLiveness(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":    "UP",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})
}

// handleReadiness returns readiness status (can accept traffic)
func (hs *HealthServer) handleReadiness(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	// Check database connection
	if err := hs.db.PingContext(ctx); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{
			"status":  "DOWN",
			"message": "database not ready",
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":    "UP",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})
}

// MetricsResponse represents metrics response
type MetricsResponse struct {
	MessagesProcessed int64  `json:"messages_processed"`
	LastProcessedAt   string `json:"last_processed_at,omitempty"`
	LastError         string `json:"last_error,omitempty"`
	KafkaStats        *KafkaStats `json:"kafka_stats,omitempty"`
}

// KafkaStats contains Kafka consumer statistics
type KafkaStats struct {
	Topic         string `json:"topic"`
	Partition     string `json:"partition"`
	Messages      int64  `json:"messages"`
	Bytes         int64  `json:"bytes"`
	Rebalances    int64  `json:"rebalances"`
	Timeouts      int64  `json:"timeouts"`
	Errors        int64  `json:"errors"`
	DialTime      string `json:"dial_time_avg"`
	ReadTime      string `json:"read_time_avg"`
	WaitTime      string `json:"wait_time_avg"`
	FetchSize     int64  `json:"fetch_size_avg"`
	FetchBytes    int64  `json:"fetch_bytes_avg"`
	Offset        int64  `json:"offset"`
	Lag           int64  `json:"lag"`
}

// handleMetrics returns service metrics
func (hs *HealthServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	hs.mu.RLock()
	defer hs.mu.RUnlock()

	response := MetricsResponse{
		MessagesProcessed: hs.messagesProcessed,
		LastError:         hs.lastError,
	}

	if !hs.lastProcessedAt.IsZero() {
		response.LastProcessedAt = hs.lastProcessedAt.UTC().Format(time.RFC3339)
	}

	if hs.kafkaReader != nil {
		stats := hs.kafkaReader.Stats()
		response.KafkaStats = &KafkaStats{
			Topic:      stats.Topic,
			Partition:  stats.Partition,
			Messages:   stats.Messages,
			Bytes:      stats.Bytes,
			Rebalances: stats.Rebalances,
			Timeouts:   stats.Timeouts,
			Errors:     stats.Errors,
			DialTime:   stats.DialTime.Avg.String(),
			ReadTime:   stats.ReadTime.Avg.String(),
			WaitTime:   stats.WaitTime.Avg.String(),
			FetchSize:  stats.FetchSize.Avg,
			FetchBytes: stats.FetchBytes.Avg,
			Offset:     stats.Offset,
			Lag:        stats.Lag,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (hs *HealthServer) checkDatabase(ctx context.Context) Check {
	if err := hs.db.PingContext(ctx); err != nil {
		return Check{Status: "DOWN", Message: err.Error()}
	}
	return Check{Status: "UP"}
}

func (hs *HealthServer) checkKafka() Check {
	if hs.kafkaReader == nil {
		return Check{Status: "DOWN", Message: "kafka reader not initialized"}
	}

	stats := hs.kafkaReader.Stats()
	if stats.Errors > 0 {
		return Check{Status: "DEGRADED", Message: "kafka has errors"}
	}

	return Check{Status: "UP"}
}
