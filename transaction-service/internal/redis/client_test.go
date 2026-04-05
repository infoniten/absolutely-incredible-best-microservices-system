package redis

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func setupTestRedis(t *testing.T) (*Client, *miniredis.Miniredis) {
	t.Helper()

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}

	client, err := NewClient("redis://"+mr.Addr(), nil, 3600)
	if err != nil {
		mr.Close()
		t.Fatalf("failed to create client: %v", err)
	}

	t.Cleanup(func() {
		client.Close()
		mr.Close()
	})

	return client, mr
}

func TestNewClient_Standalone(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer mr.Close()

	client, err := NewClient("redis://"+mr.Addr(), nil, 3600)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	defer client.Close()

	if client.ttl != 3600*time.Second {
		t.Errorf("expected TTL 3600s, got %v", client.ttl)
	}
}

func TestNewClient_InvalidURL(t *testing.T) {
	_, err := NewClient("invalid-url", nil, 3600)
	if err == nil {
		t.Error("expected error for invalid URL")
	}
}

func TestClient_Ping(t *testing.T) {
	client, _ := setupTestRedis(t)

	ctx := context.Background()
	if err := client.Ping(ctx); err != nil {
		t.Errorf("Ping failed: %v", err)
	}
}

func TestClient_CreateTransaction(t *testing.T) {
	client, mr := setupTestRedis(t)
	ctx := context.Background()

	txID := "test-tx-123"
	err := client.CreateTransaction(ctx, txID)
	if err != nil {
		t.Fatalf("CreateTransaction failed: %v", err)
	}

	// Verify the key exists
	key := "{" + txID + "}:meta"
	if !mr.Exists(key) {
		t.Error("transaction meta key should exist")
	}

	// Verify the content
	data, err := mr.Get(key)
	if err != nil {
		t.Fatalf("failed to get key: %v", err)
	}

	var meta TxMeta
	if err := json.Unmarshal([]byte(data), &meta); err != nil {
		t.Fatalf("failed to unmarshal meta: %v", err)
	}

	if meta.Status != TxStatusActive {
		t.Errorf("expected status ACTIVE, got %s", meta.Status)
	}
}

func TestClient_GetTransactionStatus(t *testing.T) {
	client, _ := setupTestRedis(t)
	ctx := context.Background()

	txID := "test-tx-456"

	// Test non-existent transaction
	_, err := client.GetTransactionStatus(ctx, txID)
	if err == nil {
		t.Error("expected error for non-existent transaction")
	}

	// Create transaction and test
	err = client.CreateTransaction(ctx, txID)
	if err != nil {
		t.Fatalf("CreateTransaction failed: %v", err)
	}

	status, err := client.GetTransactionStatus(ctx, txID)
	if err != nil {
		t.Fatalf("GetTransactionStatus failed: %v", err)
	}

	if status != TxStatusActive {
		t.Errorf("expected status ACTIVE, got %s", status)
	}
}

func TestClient_SetTransactionStatus(t *testing.T) {
	client, _ := setupTestRedis(t)
	ctx := context.Background()

	txID := "test-tx-789"

	// Test non-existent transaction
	err := client.SetTransactionStatus(ctx, txID, TxStatusCommitting)
	if err == nil {
		t.Error("expected error for non-existent transaction")
	}

	// Create and update
	err = client.CreateTransaction(ctx, txID)
	if err != nil {
		t.Fatalf("CreateTransaction failed: %v", err)
	}

	err = client.SetTransactionStatus(ctx, txID, TxStatusCommitting)
	if err != nil {
		t.Fatalf("SetTransactionStatus failed: %v", err)
	}

	status, _ := client.GetTransactionStatus(ctx, txID)
	if status != TxStatusCommitting {
		t.Errorf("expected status COMMITTING, got %s", status)
	}

	// Test all status transitions
	statuses := []TxStatus{TxStatusCommitted, TxStatusRolledBack}
	for _, s := range statuses {
		err = client.SetTransactionStatus(ctx, txID, s)
		if err != nil {
			t.Fatalf("SetTransactionStatus to %s failed: %v", s, err)
		}

		status, _ := client.GetTransactionStatus(ctx, txID)
		if status != s {
			t.Errorf("expected status %s, got %s", s, status)
		}
	}
}

func TestClient_AddObject(t *testing.T) {
	client, mr := setupTestRedis(t)
	ctx := context.Background()

	txID := "test-tx-add-obj"
	err := client.CreateTransaction(ctx, txID)
	if err != nil {
		t.Fatalf("CreateTransaction failed: %v", err)
	}

	obj := &StoredObject{
		Headers: json.RawMessage(`{"id":123,"object_type":"TestEntity"}`),
		Payload: json.RawMessage(`{"name":"test"}`),
	}

	err = client.AddObject(ctx, txID, 2, "TestEntity", obj)
	if err != nil {
		t.Fatalf("AddObject failed: %v", err)
	}

	// Verify the object was stored
	key := "{" + txID + "}:alg:2:class:TestEntity"
	if !mr.Exists(key) {
		t.Error("object key should exist")
	}

	// Verify list length
	length, err := mr.List(key)
	if err != nil {
		t.Fatalf("failed to get list: %v", err)
	}
	if len(length) != 1 {
		t.Errorf("expected 1 object, got %d", len(length))
	}
}

func TestClient_AddObject_MultipleObjects(t *testing.T) {
	client, _ := setupTestRedis(t)
	ctx := context.Background()

	txID := "test-tx-multi"
	err := client.CreateTransaction(ctx, txID)
	if err != nil {
		t.Fatalf("CreateTransaction failed: %v", err)
	}

	// Add multiple objects
	for i := 0; i < 5; i++ {
		obj := &StoredObject{
			Headers: json.RawMessage(`{"id":` + string(rune('0'+i)) + `}`),
			Payload: json.RawMessage(`{}`),
		}
		err = client.AddObject(ctx, txID, 1, "TestEntity", obj)
		if err != nil {
			t.Fatalf("AddObject %d failed: %v", i, err)
		}
	}

	key := "{" + txID + "}:alg:1:class:TestEntity"
	count, err := client.GetObjectCount(ctx, key)
	if err != nil {
		t.Fatalf("GetObjectCount failed: %v", err)
	}
	if count != 5 {
		t.Errorf("expected 5 objects, got %d", count)
	}
}

func TestClient_GetObjectKeys(t *testing.T) {
	client, _ := setupTestRedis(t)
	ctx := context.Background()

	txID := "test-tx-keys"
	err := client.CreateTransaction(ctx, txID)
	if err != nil {
		t.Fatalf("CreateTransaction failed: %v", err)
	}

	// Add objects for different algorithms and classes
	testCases := []struct {
		algID     uint32
		className string
	}{
		{1, "EntityA"},
		{1, "EntityB"},
		{2, "EntityA"},
		{3, "EntityC"},
	}

	for _, tc := range testCases {
		obj := &StoredObject{
			Headers: json.RawMessage(`{}`),
			Payload: json.RawMessage(`{}`),
		}
		err = client.AddObject(ctx, txID, tc.algID, tc.className, obj)
		if err != nil {
			t.Fatalf("AddObject failed: %v", err)
		}
	}

	keys, err := client.GetObjectKeys(ctx, txID)
	if err != nil {
		t.Fatalf("GetObjectKeys failed: %v", err)
	}

	if len(keys) != 4 {
		t.Errorf("expected 4 keys, got %d", len(keys))
	}

	// Verify meta key is not included
	for _, k := range keys {
		if k == "{"+txID+"}:meta" {
			t.Error("meta key should not be included in object keys")
		}
	}
}

func TestClient_GetObjects(t *testing.T) {
	client, _ := setupTestRedis(t)
	ctx := context.Background()

	txID := "test-tx-get"
	err := client.CreateTransaction(ctx, txID)
	if err != nil {
		t.Fatalf("CreateTransaction failed: %v", err)
	}

	// Add 10 objects
	for i := 0; i < 10; i++ {
		obj := &StoredObject{
			Headers: json.RawMessage(`{"id":` + string(rune('0'+i)) + `}`),
			Payload: json.RawMessage(`{"index":` + string(rune('0'+i)) + `}`),
		}
		err = client.AddObject(ctx, txID, 1, "TestEntity", obj)
		if err != nil {
			t.Fatalf("AddObject failed: %v", err)
		}
	}

	key := "{" + txID + "}:alg:1:class:TestEntity"

	// Test pagination
	objects, err := client.GetObjects(ctx, key, 0, 5)
	if err != nil {
		t.Fatalf("GetObjects failed: %v", err)
	}
	if len(objects) != 5 {
		t.Errorf("expected 5 objects, got %d", len(objects))
	}

	objects, err = client.GetObjects(ctx, key, 5, 5)
	if err != nil {
		t.Fatalf("GetObjects failed: %v", err)
	}
	if len(objects) != 5 {
		t.Errorf("expected 5 objects, got %d", len(objects))
	}

	// Test beyond range
	objects, err = client.GetObjects(ctx, key, 10, 5)
	if err != nil {
		t.Fatalf("GetObjects failed: %v", err)
	}
	if len(objects) != 0 {
		t.Errorf("expected 0 objects, got %d", len(objects))
	}
}

func TestClient_GetObjectCount(t *testing.T) {
	client, _ := setupTestRedis(t)
	ctx := context.Background()

	txID := "test-tx-count"
	err := client.CreateTransaction(ctx, txID)
	if err != nil {
		t.Fatalf("CreateTransaction failed: %v", err)
	}

	key := "{" + txID + "}:alg:1:class:TestEntity"

	// Empty list
	count, err := client.GetObjectCount(ctx, key)
	if err != nil {
		t.Fatalf("GetObjectCount failed: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0, got %d", count)
	}

	// Add objects
	for i := 0; i < 3; i++ {
		obj := &StoredObject{Headers: json.RawMessage(`{}`), Payload: json.RawMessage(`{}`)}
		client.AddObject(ctx, txID, 1, "TestEntity", obj)
	}

	count, err = client.GetObjectCount(ctx, key)
	if err != nil {
		t.Fatalf("GetObjectCount failed: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3, got %d", count)
	}
}

func TestClient_DeleteTransaction(t *testing.T) {
	client, mr := setupTestRedis(t)
	ctx := context.Background()

	txID := "test-tx-delete"
	err := client.CreateTransaction(ctx, txID)
	if err != nil {
		t.Fatalf("CreateTransaction failed: %v", err)
	}

	// Add some objects
	obj := &StoredObject{Headers: json.RawMessage(`{}`), Payload: json.RawMessage(`{}`)}
	client.AddObject(ctx, txID, 1, "EntityA", obj)
	client.AddObject(ctx, txID, 2, "EntityB", obj)

	// Verify keys exist
	metaKey := "{" + txID + "}:meta"
	objKey1 := "{" + txID + "}:alg:1:class:EntityA"
	objKey2 := "{" + txID + "}:alg:2:class:EntityB"

	if !mr.Exists(metaKey) || !mr.Exists(objKey1) || !mr.Exists(objKey2) {
		t.Error("keys should exist before delete")
	}

	// Delete transaction
	err = client.DeleteTransaction(ctx, txID)
	if err != nil {
		t.Fatalf("DeleteTransaction failed: %v", err)
	}

	// Verify all keys are deleted
	if mr.Exists(metaKey) || mr.Exists(objKey1) || mr.Exists(objKey2) {
		t.Error("all keys should be deleted")
	}
}

func TestClient_DeleteTransaction_Empty(t *testing.T) {
	client, _ := setupTestRedis(t)
	ctx := context.Background()

	// Delete non-existent transaction should not error
	err := client.DeleteTransaction(ctx, "non-existent")
	if err != nil {
		t.Errorf("DeleteTransaction should not error for non-existent tx: %v", err)
	}
}

func TestMetaKey(t *testing.T) {
	key := metaKey("tx-123")
	expected := "{tx-123}:meta"
	if key != expected {
		t.Errorf("expected %s, got %s", expected, key)
	}
}

func TestObjectKey(t *testing.T) {
	key := objectKey("tx-123", 2, "TestEntity")
	expected := "{tx-123}:alg:2:class:TestEntity"
	if key != expected {
		t.Errorf("expected %s, got %s", expected, key)
	}
}

func TestTxStatus_Constants(t *testing.T) {
	if TxStatusActive != "ACTIVE" {
		t.Errorf("expected ACTIVE, got %s", TxStatusActive)
	}
	if TxStatusCommitting != "COMMITTING" {
		t.Errorf("expected COMMITTING, got %s", TxStatusCommitting)
	}
	if TxStatusCommitted != "COMMITTED" {
		t.Errorf("expected COMMITTED, got %s", TxStatusCommitted)
	}
	if TxStatusRolledBack != "ROLLED_BACK" {
		t.Errorf("expected ROLLED_BACK, got %s", TxStatusRolledBack)
	}
}

// TestNewClient_Cluster tests cluster mode initialization
func TestNewClient_Cluster(t *testing.T) {
	// This test just verifies the cluster client is created
	// We can't easily test actual cluster functionality without a real cluster
	client, err := NewClient("", []string{"localhost:7000", "localhost:7001"}, 3600)
	if err != nil {
		t.Fatalf("NewClient cluster mode failed: %v", err)
	}
	defer client.Close()

	if client.ttl != 3600*time.Second {
		t.Errorf("expected TTL 3600s, got %v", client.ttl)
	}
}

// Benchmark tests
func BenchmarkClient_AddObject(b *testing.B) {
	mr, _ := miniredis.Run()
	defer mr.Close()

	client, _ := NewClient("redis://"+mr.Addr(), nil, 3600)
	defer client.Close()

	ctx := context.Background()
	txID := "bench-tx"
	client.CreateTransaction(ctx, txID)

	obj := &StoredObject{
		Headers: json.RawMessage(`{"id":1,"object_type":"TestEntity"}`),
		Payload: json.RawMessage(`{"data":"test"}`),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		client.AddObject(ctx, txID, 1, "TestEntity", obj)
	}
}

func BenchmarkClient_GetObjects(b *testing.B) {
	mr, _ := miniredis.Run()
	defer mr.Close()

	client, _ := NewClient("redis://"+mr.Addr(), nil, 3600)
	defer client.Close()

	ctx := context.Background()
	txID := "bench-tx-get"
	client.CreateTransaction(ctx, txID)

	// Pre-populate with objects
	obj := &StoredObject{
		Headers: json.RawMessage(`{"id":1}`),
		Payload: json.RawMessage(`{"data":"test"}`),
	}
	for i := 0; i < 1000; i++ {
		client.AddObject(ctx, txID, 1, "TestEntity", obj)
	}

	key := "{" + txID + "}:alg:1:class:TestEntity"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		client.GetObjects(ctx, key, 0, 100)
	}
}

// Mock redis client for error testing
type mockRedisClient struct {
	redis.UniversalClient
}
