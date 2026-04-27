package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	"github.com/quantara/transaction-service/internal/config"
	"github.com/quantara/transaction-service/internal/metamodel"
	redisclient "github.com/quantara/transaction-service/internal/redis"
	pb "github.com/quantara/transaction-service/proto"
)

// TestIntegration_FullTransactionFlow tests the complete transaction lifecycle
func TestIntegration_FullTransactionFlow(t *testing.T) {
	// Setup mock DataDictionary server
	ddServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := metamodel.TransactionInstructionsResponse{
			TransactionInstructions: struct {
				Headers []string                    `json:"headers"`
				Type    []metamodel.TypeInstruction `json:"type"`
			}{
				Headers: []string{},
				Type: []metamodel.TypeInstruction{
					{
						Name:      "TestEntity",
						Loader:    "EmbeddedEntityRepository",
						MainTable: "test_entity_main",
						DataTable: "test_entity_data",
						Indexes:   []metamodel.IndexTable{},
					},
				},
			},
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer ddServer.Close()

	// Setup miniredis
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer mr.Close()

	// Setup mock database
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock db: %v", err)
	}
	defer db.Close()

	// Create Redis client
	redisClient, err := redisclient.NewClient("redis://"+mr.Addr(), nil, "", "", 3600)
	if err != nil {
		t.Fatalf("failed to create redis client: %v", err)
	}
	defer redisClient.Close()

	// Create metamodel cache
	metamodelCache := metamodel.NewCache(ddServer.URL)
	if err := metamodelCache.Load(context.Background()); err != nil {
		t.Fatalf("failed to load metamodel: %v", err)
	}

	// Create config
	cfg := &config.Config{
		GRPCPort:        "50054",
		CommitChunkSize: 100,
	}

	// Create server
	server := NewServer(cfg, redisClient, metamodelCache, db, nil, nil)

	ctx := context.Background()

	// Step 1: Begin Transaction
	t.Run("BeginTransaction", func(t *testing.T) {
		resp, err := server.BeginTransaction(ctx, &pb.BeginTxRequest{})
		if err != nil {
			t.Fatalf("BeginTransaction failed: %v", err)
		}

		if resp.TransactionId == "" {
			t.Error("transaction ID should not be empty")
		}

		// Verify transaction was created in Redis
		status, err := redisClient.GetTransactionStatus(ctx, resp.TransactionId)
		if err != nil {
			t.Fatalf("failed to get transaction status: %v", err)
		}

		if status != redisclient.TxStatusActive {
			t.Errorf("expected status ACTIVE, got %s", status)
		}
	})

	// Step 2: Save Objects
	t.Run("SaveObject", func(t *testing.T) {
		// First begin a transaction
		beginResp, _ := server.BeginTransaction(ctx, &pb.BeginTxRequest{})
		txID := beginResp.TransactionId

		// Save an object
		saveResp, err := server.Save(ctx, &pb.SaveRequest{
			TransactionId: txID,
			Headers: &pb.ObjectHeaders{
				Id:         12345,
				GlobalId:   67890,
				ObjectType: "TestEntity",
				LockId:     "lock-123",
				Revision:   1,
			},
			Payload: []byte(`{"name": "test"}`),
		})

		if err != nil {
			t.Fatalf("Save failed: %v", err)
		}

		if !saveResp.Success {
			t.Errorf("Save should succeed, error: %s", saveResp.Error)
		}

		// Verify object was stored in Redis
		keys, _ := redisClient.GetObjectKeys(ctx, txID)
		if len(keys) != 1 {
			t.Errorf("expected 1 object key, got %d", len(keys))
		}
	})

	// Step 3: Save with explicit algorithm ID
	t.Run("SaveWithAlgorithmID", func(t *testing.T) {
		beginResp, _ := server.BeginTransaction(ctx, &pb.BeginTxRequest{})
		txID := beginResp.TransactionId

		algID := uint32(1) // Embedded
		saveResp, err := server.Save(ctx, &pb.SaveRequest{
			TransactionId: txID,
			AlgorithmId:   &algID,
			Headers: &pb.ObjectHeaders{
				Id:         111,
				GlobalId:   222,
				ObjectType: "TestEntity",
				LockId:     "",
				Revision:   0,
			},
			Payload: []byte(`{}`),
		})

		if err != nil {
			t.Fatalf("Save failed: %v", err)
		}

		if !saveResp.Success {
			t.Errorf("Save should succeed")
		}
	})

	// Step 4: Rollback
	t.Run("Rollback", func(t *testing.T) {
		beginResp, _ := server.BeginTransaction(ctx, &pb.BeginTxRequest{})
		txID := beginResp.TransactionId

		// Add some objects
		server.Save(ctx, &pb.SaveRequest{
			TransactionId: txID,
			Headers: &pb.ObjectHeaders{
				Id:         1,
				GlobalId:   1,
				ObjectType: "TestEntity",
				Revision:   0,
			},
			Payload: []byte(`{}`),
		})

		// Rollback
		rollbackResp, err := server.Rollback(ctx, &pb.RollbackRequest{
			TransactionId: txID,
		})

		if err != nil {
			t.Fatalf("Rollback failed: %v", err)
		}

		if !rollbackResp.Success {
			t.Error("Rollback should succeed")
		}

		// Verify transaction is deleted
		_, err = redisClient.GetTransactionStatus(ctx, txID)
		if err == nil {
			t.Error("transaction should be deleted after rollback")
		}
	})

	// Step 5: Commit empty transaction
	t.Run("CommitEmptyTransaction", func(t *testing.T) {
		beginResp, _ := server.BeginTransaction(ctx, &pb.BeginTxRequest{})
		txID := beginResp.TransactionId

		commitResp, err := server.Commit(ctx, &pb.CommitRequest{
			TransactionId: txID,
		})

		if err != nil {
			t.Fatalf("Commit failed: %v", err)
		}

		if !commitResp.Success {
			t.Errorf("Commit should succeed, error: %s", commitResp.Error)
		}

		if commitResp.ObjectsSaved != 0 {
			t.Errorf("expected 0 objects saved, got %d", commitResp.ObjectsSaved)
		}
	})

	// Step 6: Commit with objects (mock DB expectations)
	t.Run("CommitWithObjects", func(t *testing.T) {
		beginResp, _ := server.BeginTransaction(ctx, &pb.BeginTxRequest{})
		txID := beginResp.TransactionId

		// Add objects
		server.Save(ctx, &pb.SaveRequest{
			TransactionId: txID,
			Headers: &pb.ObjectHeaders{
				Id:         1001,
				GlobalId:   2001,
				ObjectType: "TestEntity",
				Revision:   0,
			},
			Payload: []byte(`{"name": "obj1"}`),
		})

		// Setup mock expectations for commit
		mock.ExpectBegin()
		mock.ExpectExec("INSERT INTO murex.test_entity_main").
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("INSERT INTO murex.test_entity_data").
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()

		commitResp, err := server.Commit(ctx, &pb.CommitRequest{
			TransactionId: txID,
		})

		if err != nil {
			t.Fatalf("Commit failed: %v", err)
		}

		if !commitResp.Success {
			t.Errorf("Commit should succeed, error: %s", commitResp.Error)
		}

		// Verify SQL expectations were met
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("SQL expectations not met: %v", err)
		}
	})
}

// TestIntegration_InvalidTransactionOperations tests error cases
func TestIntegration_InvalidTransactionOperations(t *testing.T) {
	// Setup
	mr, _ := miniredis.Run()
	defer mr.Close()

	redisClient, _ := redisclient.NewClient("redis://"+mr.Addr(), nil, "", "", 3600)
	defer redisClient.Close()

	cfg := &config.Config{CommitChunkSize: 100}
	server := NewServer(cfg, redisClient, nil, nil, nil, nil)
	ctx := context.Background()

	t.Run("SaveToNonExistentTransaction", func(t *testing.T) {
		resp, _ := server.Save(ctx, &pb.SaveRequest{
			TransactionId: "non-existent-tx",
			Headers: &pb.ObjectHeaders{
				Id:         1,
				GlobalId:   1,
				ObjectType: "Test",
			},
		})

		if resp.Success {
			t.Error("Save to non-existent transaction should fail")
		}
	})

	t.Run("CommitNonExistentTransaction", func(t *testing.T) {
		resp, _ := server.Commit(ctx, &pb.CommitRequest{
			TransactionId: "non-existent-tx",
		})

		if resp.Success {
			t.Error("Commit non-existent transaction should fail")
		}
	})
}

// TestIntegration_MultipleObjectTypes tests saving different object types
func TestIntegration_MultipleObjectTypes(t *testing.T) {
	// Setup mock DataDictionary
	ddServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := metamodel.TransactionInstructionsResponse{
			TransactionInstructions: struct {
				Headers []string                    `json:"headers"`
				Type    []metamodel.TypeInstruction `json:"type"`
			}{
				Type: []metamodel.TypeInstruction{
					{
						Name:      "EntityA",
						Loader:    "EmbeddedEntityRepository",
						MainTable: "entity_a_main",
						DataTable: "entity_a_data",
					},
					{
						Name:      "EntityB",
						Loader:    "RevisionedEntityRepository",
						MainTable: "entity_b_main",
						DataTable: "entity_b_data",
					},
				},
			},
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer ddServer.Close()

	mr, _ := miniredis.Run()
	defer mr.Close()

	redisClient, _ := redisclient.NewClient("redis://"+mr.Addr(), nil, "", "", 3600)
	defer redisClient.Close()

	metamodelCache := metamodel.NewCache(ddServer.URL)
	metamodelCache.Load(context.Background())

	cfg := &config.Config{CommitChunkSize: 100}
	server := NewServer(cfg, redisClient, metamodelCache, nil, nil, nil)
	ctx := context.Background()

	// Begin transaction
	beginResp, _ := server.BeginTransaction(ctx, &pb.BeginTxRequest{})
	txID := beginResp.TransactionId

	// Save EntityA
	server.Save(ctx, &pb.SaveRequest{
		TransactionId: txID,
		Headers:       &pb.ObjectHeaders{Id: 1, GlobalId: 1, ObjectType: "EntityA"},
		Payload:       []byte(`{}`),
	})

	// Save EntityB
	server.Save(ctx, &pb.SaveRequest{
		TransactionId: txID,
		Headers:       &pb.ObjectHeaders{Id: 2, GlobalId: 2, ObjectType: "EntityB"},
		Payload:       []byte(`{}`),
	})

	// Verify both object types are stored
	keys, _ := redisClient.GetObjectKeys(ctx, txID)
	if len(keys) != 2 {
		t.Errorf("expected 2 object keys (different types), got %d", len(keys))
	}
}

// TestIntegration_LargeTransaction tests handling many objects
func TestIntegration_LargeTransaction(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large transaction test in short mode")
	}

	ddServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := metamodel.TransactionInstructionsResponse{
			TransactionInstructions: struct {
				Headers []string                    `json:"headers"`
				Type    []metamodel.TypeInstruction `json:"type"`
			}{
				Type: []metamodel.TypeInstruction{
					{
						Name:      "BulkEntity",
						Loader:    "EmbeddedEntityRepository",
						MainTable: "bulk_entity_main",
						DataTable: "bulk_entity_data",
					},
				},
			},
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer ddServer.Close()

	mr, _ := miniredis.Run()
	defer mr.Close()

	redisClient, _ := redisclient.NewClient("redis://"+mr.Addr(), nil, "", "", 3600)
	defer redisClient.Close()

	metamodelCache := metamodel.NewCache(ddServer.URL)
	metamodelCache.Load(context.Background())

	cfg := &config.Config{CommitChunkSize: 100}
	server := NewServer(cfg, redisClient, metamodelCache, nil, nil, nil)
	ctx := context.Background()

	beginResp, _ := server.BeginTransaction(ctx, &pb.BeginTxRequest{})
	txID := beginResp.TransactionId

	// Add 1000 objects
	objectCount := 1000
	for i := 0; i < objectCount; i++ {
		server.Save(ctx, &pb.SaveRequest{
			TransactionId: txID,
			Headers: &pb.ObjectHeaders{
				Id:         int64(i),
				GlobalId:   int64(i),
				ObjectType: "BulkEntity",
			},
			Payload: []byte(`{"index":` + string(rune('0'+i%10)) + `}`),
		})
	}

	// Verify count
	key := "{" + txID + "}:alg:1:class:BulkEntity"
	count, _ := redisClient.GetObjectCount(ctx, key)
	if count != int64(objectCount) {
		t.Errorf("expected %d objects, got %d", objectCount, count)
	}
}

// Helper to create full test server setup
func setupTestServer(t *testing.T) (*Server, *sql.DB, sqlmock.Sqlmock, func()) {
	t.Helper()

	ddServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := metamodel.TransactionInstructionsResponse{
			TransactionInstructions: struct {
				Headers []string                    `json:"headers"`
				Type    []metamodel.TypeInstruction `json:"type"`
			}{
				Type: []metamodel.TypeInstruction{
					{
						Name:      "TestEntity",
						Loader:    "EmbeddedEntityRepository",
						MainTable: "test_main",
						DataTable: "test_data",
					},
				},
			},
		}
		json.NewEncoder(w).Encode(response)
	}))

	mr, _ := miniredis.Run()
	redisClient, _ := redisclient.NewClient("redis://"+mr.Addr(), nil, "", "", 3600)
	metamodelCache := metamodel.NewCache(ddServer.URL)
	metamodelCache.Load(context.Background())

	db, mock, _ := sqlmock.New()

	cfg := &config.Config{CommitChunkSize: 100}
	server := NewServer(cfg, redisClient, metamodelCache, db, nil, nil)

	cleanup := func() {
		db.Close()
		redisClient.Close()
		mr.Close()
		ddServer.Close()
	}

	return server, db, mock, cleanup
}
