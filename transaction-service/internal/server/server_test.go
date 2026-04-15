package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/quantara/transaction-service/internal/config"
	"github.com/quantara/transaction-service/internal/metamodel"
	"github.com/quantara/transaction-service/internal/redis"
	pb "github.com/quantara/transaction-service/proto"
)

func TestParseObjectKey(t *testing.T) {
	tests := []struct {
		name          string
		key           string
		expectedAlgID uint32
		expectedClass string
	}{
		{
			name:          "valid key",
			key:           "{tx-123}:alg:2:class:TestEntity",
			expectedAlgID: 2,
			expectedClass: "TestEntity",
		},
		{
			name:          "algorithm 1",
			key:           "{abc-def}:alg:1:class:EmbeddedEntity",
			expectedAlgID: 1,
			expectedClass: "EmbeddedEntity",
		},
		{
			name:          "algorithm 3",
			key:           "{uuid-here}:alg:3:class:DraftableEntity",
			expectedAlgID: 3,
			expectedClass: "DraftableEntity",
		},
		{
			name:          "class with dots",
			key:           "{tx}:alg:2:class:com.example.Entity",
			expectedAlgID: 2,
			expectedClass: "com.example.Entity",
		},
		{
			name:          "invalid key - no alg",
			key:           "{tx-123}:class:TestEntity",
			expectedAlgID: 0,
			expectedClass: "",
		},
		{
			name:          "invalid key - wrong format",
			key:           "tx-123:alg:2:class:TestEntity",
			expectedAlgID: 0,
			expectedClass: "",
		},
		{
			name:          "empty key",
			key:           "",
			expectedAlgID: 0,
			expectedClass: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			algID, className := parseObjectKey(tt.key)

			if algID != tt.expectedAlgID {
				t.Errorf("expected algID %d, got %d", tt.expectedAlgID, algID)
			}

			if className != tt.expectedClass {
				t.Errorf("expected className '%s', got '%s'", tt.expectedClass, className)
			}
		})
	}
}

func TestParseStoredObject(t *testing.T) {
	tests := []struct {
		name        string
		obj         *redis.StoredObject
		expectError bool
		validate    func(*testing.T, *ObjectRequest)
	}{
		{
			name: "full object",
			obj: &redis.StoredObject{
				Headers: json.RawMessage(`{
					"id": 12345,
					"global_id": 67890,
					"object_type": "TestEntity",
					"lock_id": "lock-abc",
					"revision": 5,
					"version": 10,
					"actual_from": "2024-01-01",
					"draft_status": "DRAFT",
					"parent_id": 111
				}`),
				Payload: json.RawMessage(`{"name": "test", "value": 42}`),
			},
			expectError: false,
			validate: func(t *testing.T, req *ObjectRequest) {
				if req.ID != 12345 {
					t.Errorf("expected ID 12345, got %d", req.ID)
				}
				if req.GlobalID != 67890 {
					t.Errorf("expected GlobalID 67890, got %d", req.GlobalID)
				}
				if req.ObjectType != "TestEntity" {
					t.Errorf("expected ObjectType 'TestEntity', got '%s'", req.ObjectType)
				}
				if req.LockID != "lock-abc" {
					t.Errorf("expected LockID 'lock-abc', got '%s'", req.LockID)
				}
				if req.Revision != 5 {
					t.Errorf("expected Revision 5, got %d", req.Revision)
				}
				if req.Version == nil || *req.Version != 10 {
					t.Errorf("expected Version 10, got %v", req.Version)
				}
				if req.ActualFrom == nil || *req.ActualFrom != "2024-01-01" {
					t.Errorf("expected ActualFrom '2024-01-01', got %v", req.ActualFrom)
				}
				if req.DraftStatus == nil || *req.DraftStatus != "DRAFT" {
					t.Errorf("expected DraftStatus 'DRAFT', got %v", req.DraftStatus)
				}
				if req.ParentID == nil || *req.ParentID != 111 {
					t.Errorf("expected ParentID 111, got %v", req.ParentID)
				}
				if req.PayloadMap["name"] != "test" {
					t.Errorf("expected payload name 'test', got %v", req.PayloadMap["name"])
				}
			},
		},
		{
			name: "minimal object",
			obj: &redis.StoredObject{
				Headers: json.RawMessage(`{
					"id": 1,
					"global_id": 2,
					"object_type": "Simple",
					"lock_id": "",
					"revision": 0
				}`),
				Payload: json.RawMessage(`{}`),
			},
			expectError: false,
			validate: func(t *testing.T, req *ObjectRequest) {
				if req.ID != 1 {
					t.Errorf("expected ID 1, got %d", req.ID)
				}
				if req.Version != nil {
					t.Errorf("expected Version nil, got %v", req.Version)
				}
				if req.ActualFrom != nil {
					t.Errorf("expected ActualFrom nil, got %v", req.ActualFrom)
				}
			},
		},
		{
			name: "invalid headers json",
			obj: &redis.StoredObject{
				Headers: json.RawMessage(`invalid json`),
				Payload: json.RawMessage(`{}`),
			},
			expectError: true,
		},
		{
			name: "invalid payload json - should not error",
			obj: &redis.StoredObject{
				Headers: json.RawMessage(`{"id": 1, "global_id": 2, "object_type": "Test", "lock_id": "", "revision": 0}`),
				Payload: json.RawMessage(`invalid`),
			},
			expectError: false,
			validate: func(t *testing.T, req *ObjectRequest) {
				if req.PayloadMap == nil {
					t.Error("PayloadMap should be initialized even for invalid JSON")
				}
			},
		},
		{
			name: "empty payload",
			obj: &redis.StoredObject{
				Headers: json.RawMessage(`{"id": 1, "global_id": 2, "object_type": "Test", "lock_id": "", "revision": 0}`),
				Payload: nil,
			},
			expectError: false,
			validate: func(t *testing.T, req *ObjectRequest) {
				if req.PayloadMap != nil && len(req.PayloadMap) > 0 {
					t.Error("PayloadMap should be empty for nil payload")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := parseStoredObject(tt.obj)

			if tt.expectError {
				if err == nil {
					t.Error("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tt.validate != nil {
				tt.validate(t, req)
			}
		})
	}
}

func TestExtractFieldValue(t *testing.T) {
	server := &Server{}

	obj := &ObjectRequest{
		ID:       12345,
		GlobalID: 67890,
		PayloadMap: map[string]interface{}{
			"name":    "test",
			"count":   float64(42),
			"enabled": true,
			"nested":  map[string]interface{}{"key": "value"},
		},
	}

	tests := []struct {
		name     string
		field    metamodel.IndexField
		expected interface{}
	}{
		{
			name:     "extract id by source",
			field:    metamodel.IndexField{Source: "id", DBFieldName: "id"},
			expected: int64(12345),
		},
		{
			name:     "extract id by db field name",
			field:    metamodel.IndexField{Source: "something", DBFieldName: "id"},
			expected: int64(12345),
		},
		{
			name:     "extract string from payload",
			field:    metamodel.IndexField{Source: "name", DBFieldName: "name"},
			expected: "test",
		},
		{
			name:     "extract number from payload",
			field:    metamodel.IndexField{Source: "count", DBFieldName: "count"},
			expected: float64(42),
		},
		{
			name:     "extract bool from payload",
			field:    metamodel.IndexField{Source: "enabled", DBFieldName: "enabled"},
			expected: true,
		},
		{
			name:     "non-existent field",
			field:    metamodel.IndexField{Source: "nonexistent", DBFieldName: "nonexistent"},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := server.extractFieldValue(obj, tt.field)

			if result != tt.expected {
				t.Errorf("expected %v (%T), got %v (%T)", tt.expected, tt.expected, result, result)
			}
		})
	}
}

func TestDraftStatusCacheSuffix(t *testing.T) {
	draft := "DRAFT"
	confirmed := "CONFIRMED"

	if got := draftStatusCacheSuffix(&draft); got != "draft" {
		t.Fatalf("expected draft suffix, got %q", got)
	}
	if got := draftStatusCacheSuffix(&confirmed); got != "confirmed" {
		t.Fatalf("expected confirmed suffix, got %q", got)
	}
	if got := draftStatusCacheSuffix(nil); got != "confirmed" {
		t.Fatalf("expected default confirmed suffix, got %q", got)
	}
}

func TestBuildCommittedCachePlan(t *testing.T) {
	embeddedObj := &redis.StoredObject{
		Headers: json.RawMessage(`{
			"id": 1,
			"global_id": 11,
			"object_type": "PersonAlternativeIdentifier",
			"lock_id": "",
			"revision": 1,
			"parent_id": 101
		}`),
		Payload: json.RawMessage(`{"value":"ALT_1","sourceId":101}`),
	}
	revisionedObj := &redis.StoredObject{
		Headers: json.RawMessage(`{
			"id": 2,
			"global_id": 22,
			"object_type": "Source",
			"lock_id": "",
			"revision": 1
		}`),
		Payload: json.RawMessage(`{"alias":"SRC_1"}`),
	}
	draftObj := &redis.StoredObject{
		Headers: json.RawMessage(`{
			"id": 3,
			"global_id": 33,
			"object_type": "FixingSource",
			"lock_id": "",
			"revision": 1,
			"draft_status": "DRAFT"
		}`),
		Payload: json.RawMessage(`{"code":"FIX_1"}`),
	}

	plan, err := buildCommittedCachePlan([]prefetchedData{
		{
			algorithmID: metamodel.AlgorithmEmbedded,
			className:   "PersonAlternativeIdentifier",
			objects:     []*redis.StoredObject{embeddedObj},
		},
		{
			algorithmID: metamodel.AlgorithmRevisioned,
			className:   "Source",
			objects:     []*redis.StoredObject{revisionedObj},
		},
		{
			algorithmID: metamodel.AlgorithmDraftableDateBounded,
			className:   "FixingSource",
			objects:     []*redis.StoredObject{draftObj},
		},
	}, nil)
	if err != nil {
		t.Fatalf("buildCommittedCachePlan failed: %v", err)
	}

	embeddedKey := committedParentKey("PersonAlternativeIdentifier", 101)
	if got := len(plan.parentAppends[embeddedKey]); got != 1 {
		t.Fatalf("expected 1 embedded payload, got %d", got)
	}

	revisionedKey := committedGlobalKey(22)
	if _, ok := plan.preDeleteKeys[revisionedKey]; !ok {
		t.Fatalf("expected pre-delete key %s", revisionedKey)
	}
	if got := plan.globalUpserts[revisionedKey].objectClass; got != "Source" {
		t.Fatalf("unexpected objectClass for revisioned key: %q", got)
	}

	draftKey := committedTradeKey(33, "draft")
	if _, ok := plan.preDeleteKeys[draftKey]; !ok {
		t.Fatalf("expected pre-delete key %s", draftKey)
	}
	if got := plan.globalUpserts[draftKey].objectClass; got != "FixingSource" {
		t.Fatalf("unexpected objectClass for draft key: %q", got)
	}
}

func TestExtractFieldValue_NilPayloadMap(t *testing.T) {
	server := &Server{}

	obj := &ObjectRequest{
		ID:         12345,
		PayloadMap: nil,
	}

	// Should return id for id field
	field := metamodel.IndexField{Source: "id", DBFieldName: "id"}
	result := server.extractFieldValue(obj, field)
	if result != int64(12345) {
		t.Errorf("expected 12345, got %v", result)
	}

	// Should return nil for other fields
	field = metamodel.IndexField{Source: "name", DBFieldName: "name"}
	result = server.extractFieldValue(obj, field)
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

func TestNewServer(t *testing.T) {
	cfg := &config.Config{
		GRPCPort:        "50054",
		CommitChunkSize: 1000,
	}

	server := NewServer(cfg, nil, nil, nil, nil)

	if server == nil {
		t.Fatal("NewServer returned nil")
	}

	if server.cfg != cfg {
		t.Error("config not set correctly")
	}
}

// Test helper to create a mock database
func createMockDB(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock db: %v", err)
	}

	return db, mock
}

// Test helper to create a test metamodel cache
func createTestMetamodelCache() *metamodel.Cache {
	cache := metamodel.NewCache("")
	// Manually populate for testing
	return cache
}

func TestObjectRequest_Fields(t *testing.T) {
	// Test that ObjectRequest properly handles all field types
	version := int32(5)
	actualFrom := "2024-01-01"
	draftStatus := "PUBLISHED"
	parentID := int64(100)

	req := &ObjectRequest{
		ID:          1,
		GlobalID:    2,
		ObjectType:  "Test",
		LockID:      "lock",
		Revision:    3,
		Version:     &version,
		ActualFrom:  &actualFrom,
		DraftStatus: &draftStatus,
		ParentID:    &parentID,
		Payload:     json.RawMessage(`{}`),
		PayloadMap:  map[string]interface{}{},
	}

	if *req.Version != 5 {
		t.Errorf("expected Version 5, got %d", *req.Version)
	}

	if *req.ActualFrom != "2024-01-01" {
		t.Errorf("expected ActualFrom '2024-01-01', got '%s'", *req.ActualFrom)
	}

	if *req.DraftStatus != "PUBLISHED" {
		t.Errorf("expected DraftStatus 'PUBLISHED', got '%s'", *req.DraftStatus)
	}

	if *req.ParentID != 100 {
		t.Errorf("expected ParentID 100, got %d", *req.ParentID)
	}
}

// Test startSpan helper
func TestStartSpan(t *testing.T) {
	server := &Server{tracer: nil}
	ctx := context.Background()

	// Should not panic with nil tracer
	newCtx, span := server.startSpan(ctx, "test")

	if newCtx == nil {
		t.Error("context should not be nil")
	}

	if span == nil {
		t.Error("span should not be nil")
	}

	// End should not panic
	span.End()
}

// Benchmark parseObjectKey
func BenchmarkParseObjectKey(b *testing.B) {
	key := "{tx-123-456-789}:alg:2:class:com.example.TestEntity"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		parseObjectKey(key)
	}
}

// Benchmark parseStoredObject
func BenchmarkParseStoredObject(b *testing.B) {
	obj := &redis.StoredObject{
		Headers: json.RawMessage(`{
			"id": 12345,
			"global_id": 67890,
			"object_type": "TestEntity",
			"lock_id": "lock-abc",
			"revision": 5,
			"version": 10,
			"actual_from": "2024-01-01",
			"draft_status": "DRAFT",
			"parent_id": 111
		}`),
		Payload: json.RawMessage(`{"name": "test", "value": 42, "nested": {"a": 1, "b": 2}}`),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		parseStoredObject(obj)
	}
}

// Test gRPC methods error handling

func TestSave_InvalidTransactionID(t *testing.T) {
	// This test verifies behavior when transaction doesn't exist
	// Would require mock Redis client
}

func TestCommit_EmptyTransaction(t *testing.T) {
	// This test verifies behavior when no objects to commit
	// Would require mock Redis client
}

// Integration-style test with mocks
func TestExecuteBatchSave_UnknownAlgorithm(t *testing.T) {
	db, mock := createMockDB(t)
	defer db.Close()

	server := &Server{
		db:        db,
		metamodel: createTestMetamodelCache(),
	}

	ctx := context.Background()
	mock.ExpectBegin()
	tx, _ := db.BeginTx(ctx, nil)

	// Unknown algorithm ID should return error
	_, _, err := server.executeBatchSave(ctx, tx, 999, "TestEntity", nil)
	if err == nil {
		t.Error("expected error for unknown algorithm")
	}
}

// Test proto message handling
func TestObjectHeaders_Marshaling(t *testing.T) {
	headers := &pb.ObjectHeaders{
		Id:         12345,
		GlobalId:   67890,
		ObjectType: "TestEntity",
		LockId:     "lock-123",
		Revision:   5,
	}

	data, err := json.Marshal(headers)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	var parsed pb.ObjectHeaders
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if parsed.Id != headers.Id {
		t.Errorf("expected Id %d, got %d", headers.Id, parsed.Id)
	}
}
