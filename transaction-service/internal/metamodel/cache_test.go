package metamodel

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNewCache(t *testing.T) {
	cache := NewCache("http://localhost:8080")

	if cache == nil {
		t.Fatal("NewCache returned nil")
	}

	if cache.dataDictionaryURL != "http://localhost:8080" {
		t.Errorf("expected dataDictionaryURL to be 'http://localhost:8080', got '%s'", cache.dataDictionaryURL)
	}

	if cache.types == nil {
		t.Error("types map should be initialized")
	}

	if cache.TypeCount() != 0 {
		t.Errorf("expected TypeCount to be 0, got %d", cache.TypeCount())
	}
}

func TestCache_Load(t *testing.T) {
	response := TransactionInstructionsResponse{
		TransactionInstructions: struct {
			Headers []string          `json:"headers"`
			Type    []TypeInstruction `json:"type"`
		}{
			Headers: []string{"Content-Type", "Authorization"},
			Type: []TypeInstruction{
				{
					Name:      "TestEntity",
					Loader:    "RevisionedEntityRepository",
					MainTable: "test_entity_main",
					DataTable: "test_entity_data",
					Indexes: []IndexTable{
						{
							TableName: "test_entity_index",
							Fields: []IndexField{
								{Source: "id", DBFieldName: "id", DBFieldType: "bigint", Required: true},
								{Source: "name", DBFieldName: "name", DBFieldType: "varchar", Required: false},
							},
						},
					},
				},
				{
					Name:      "EmbeddedEntity",
					Loader:    "EmbeddedEntityRepository",
					MainTable: "embedded_entity_main",
					DataTable: "embedded_entity_data",
				},
				{
					Name:      "DraftableEntity",
					Loader:    "DraftableDateBoundedEntityRepository",
					MainTable: "draftable_entity_main",
					DataTable: "draftable_entity_data",
				},
			},
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/transaction-service/instructions" {
			t.Errorf("unexpected path: %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}

		if r.Header.Get("Accept") != "application/json" {
			t.Errorf("expected Accept header to be 'application/json', got '%s'", r.Header.Get("Accept"))
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	cache := NewCache(server.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := cache.Load(ctx)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cache.TypeCount() != 3 {
		t.Errorf("expected TypeCount to be 3, got %d", cache.TypeCount())
	}

	headers := cache.GetHeaders()
	if len(headers) != 2 {
		t.Errorf("expected 2 headers, got %d", len(headers))
	}
}

func TestCache_Load_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal server error"))
	}))
	defer server.Close()

	cache := NewCache(server.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := cache.Load(ctx)
	if err == nil {
		t.Fatal("expected error on server error response")
	}
}

func TestCache_Load_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("invalid json"))
	}))
	defer server.Close()

	cache := NewCache(server.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := cache.Load(ctx)
	if err == nil {
		t.Fatal("expected error on invalid JSON response")
	}
}

func TestCache_GetTypeInstruction(t *testing.T) {
	cache := createTestCache(t)

	// Test existing type
	ti, err := cache.GetTypeInstruction("TestEntity")
	if err != nil {
		t.Fatalf("GetTypeInstruction failed: %v", err)
	}

	if ti.Name != "TestEntity" {
		t.Errorf("expected Name to be 'TestEntity', got '%s'", ti.Name)
	}

	if ti.MainTable != "test_entity_main" {
		t.Errorf("expected MainTable to be 'test_entity_main', got '%s'", ti.MainTable)
	}

	// Test non-existing type
	_, err = cache.GetTypeInstruction("NonExistent")
	if err == nil {
		t.Error("expected error for non-existent type")
	}
}

func TestCache_GetAlgorithmID(t *testing.T) {
	cache := createTestCache(t)

	tests := []struct {
		className   string
		expectedID  uint32
		expectError bool
	}{
		{"TestEntity", AlgorithmRevisioned, false},
		{"EmbeddedEntity", AlgorithmEmbedded, false},
		{"DraftableEntity", AlgorithmDraftableDateBounded, false},
		{"NonExistent", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.className, func(t *testing.T) {
			algID, err := cache.GetAlgorithmID(tt.className)

			if tt.expectError {
				if err == nil {
					t.Error("expected error")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if algID != tt.expectedID {
				t.Errorf("expected algorithm ID %d, got %d", tt.expectedID, algID)
			}
		})
	}
}

func TestCache_GetMainTable(t *testing.T) {
	cache := createTestCache(t)

	table, err := cache.GetMainTable("TestEntity")
	if err != nil {
		t.Fatalf("GetMainTable failed: %v", err)
	}

	if table != "test_entity_main" {
		t.Errorf("expected 'test_entity_main', got '%s'", table)
	}

	// Test non-existing type
	_, err = cache.GetMainTable("NonExistent")
	if err == nil {
		t.Error("expected error for non-existent type")
	}
}

func TestCache_GetDataTable(t *testing.T) {
	cache := createTestCache(t)

	table, err := cache.GetDataTable("TestEntity")
	if err != nil {
		t.Fatalf("GetDataTable failed: %v", err)
	}

	if table != "test_entity_data" {
		t.Errorf("expected 'test_entity_data', got '%s'", table)
	}

	// Test non-existing type
	_, err = cache.GetDataTable("NonExistent")
	if err == nil {
		t.Error("expected error for non-existent type")
	}
}

func TestCache_GetIndexTables(t *testing.T) {
	cache := createTestCache(t)

	indexes, err := cache.GetIndexTables("TestEntity")
	if err != nil {
		t.Fatalf("GetIndexTables failed: %v", err)
	}

	if len(indexes) != 1 {
		t.Fatalf("expected 1 index table, got %d", len(indexes))
	}

	if indexes[0].TableName != "test_entity_index" {
		t.Errorf("expected table name 'test_entity_index', got '%s'", indexes[0].TableName)
	}

	if len(indexes[0].Fields) != 2 {
		t.Errorf("expected 2 fields, got %d", len(indexes[0].Fields))
	}

	// Test type with no indexes
	indexes, err = cache.GetIndexTables("EmbeddedEntity")
	if err != nil {
		t.Fatalf("GetIndexTables failed: %v", err)
	}

	if len(indexes) != 0 {
		t.Errorf("expected 0 index tables, got %d", len(indexes))
	}
}

func TestCache_GetTypeInstruction_EmptyMainTable(t *testing.T) {
	cache := NewCache("")
	cache.types["NoMainTable"] = &TypeInstruction{
		Name:      "NoMainTable",
		Loader:    "RevisionedEntityRepository",
		MainTable: "",
		DataTable: "some_data",
	}

	_, err := cache.GetMainTable("NoMainTable")
	if err == nil {
		t.Error("expected error for empty main table")
	}
}

func TestCache_GetTypeInstruction_EmptyDataTable(t *testing.T) {
	cache := NewCache("")
	cache.types["NoDataTable"] = &TypeInstruction{
		Name:      "NoDataTable",
		Loader:    "RevisionedEntityRepository",
		MainTable: "some_main",
		DataTable: "",
	}

	_, err := cache.GetDataTable("NoDataTable")
	if err == nil {
		t.Error("expected error for empty data table")
	}
}

func TestCache_GetAlgorithmID_NoAlgorithm(t *testing.T) {
	cache := NewCache("")
	cache.types["NoAlgorithm"] = &TypeInstruction{
		Name:        "NoAlgorithm",
		Loader:      "UnknownRepository",
		AlgorithmID: 0,
	}

	_, err := cache.GetAlgorithmID("NoAlgorithm")
	if err == nil {
		t.Error("expected error for unknown algorithm")
	}
}

func TestAlgorithmNameToID(t *testing.T) {
	tests := []struct {
		name     string
		expected uint32
		exists   bool
	}{
		{"EmbeddedEntityRepository", AlgorithmEmbedded, true},
		{"RevisionedEntityRepository", AlgorithmRevisioned, true},
		{"DraftableDateBoundedEntityRepository", AlgorithmDraftableDateBounded, true},
		{"UnknownRepository", 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id, ok := AlgorithmNameToID[tt.name]
			if ok != tt.exists {
				t.Errorf("expected exists=%v, got %v", tt.exists, ok)
			}
			if ok && id != tt.expected {
				t.Errorf("expected ID %d, got %d", tt.expected, id)
			}
		})
	}
}

// Helper function to create a populated cache for testing
func createTestCache(t *testing.T) *Cache {
	t.Helper()

	response := TransactionInstructionsResponse{
		TransactionInstructions: struct {
			Headers []string          `json:"headers"`
			Type    []TypeInstruction `json:"type"`
		}{
			Headers: []string{"Content-Type"},
			Type: []TypeInstruction{
				{
					Name:      "TestEntity",
					Loader:    "RevisionedEntityRepository",
					MainTable: "test_entity_main",
					DataTable: "test_entity_data",
					Indexes: []IndexTable{
						{
							TableName: "test_entity_index",
							Fields: []IndexField{
								{Source: "id", DBFieldName: "id", DBFieldType: "bigint", Required: true},
								{Source: "name", DBFieldName: "name", DBFieldType: "varchar", Required: false},
							},
						},
					},
				},
				{
					Name:      "EmbeddedEntity",
					Loader:    "EmbeddedEntityRepository",
					MainTable: "embedded_entity_main",
					DataTable: "embedded_entity_data",
				},
				{
					Name:      "DraftableEntity",
					Loader:    "DraftableDateBoundedEntityRepository",
					MainTable: "draftable_entity_main",
					DataTable: "draftable_entity_data",
				},
			},
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	t.Cleanup(server.Close)

	cache := NewCache(server.URL)
	ctx := context.Background()
	if err := cache.Load(ctx); err != nil {
		t.Fatalf("failed to load cache: %v", err)
	}

	return cache
}
