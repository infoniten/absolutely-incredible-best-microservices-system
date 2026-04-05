package metamodel

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
)

// Algorithm IDs
const (
	AlgorithmEmbedded             uint32 = 1
	AlgorithmRevisioned           uint32 = 2
	AlgorithmDraftableDateBounded uint32 = 3
)

// AlgorithmNameToID maps algorithm name to ID
var AlgorithmNameToID = map[string]uint32{
	"EmbeddedEntityRepository":             AlgorithmEmbedded,
	"RevisionedEntityRepository":           AlgorithmRevisioned,
	"DraftableDateBoundedEntityRepository": AlgorithmDraftableDateBounded,
}

// IndexField describes a field in an index table
type IndexField struct {
	Source        string `json:"source"`        // Source JSON field name
	JSONFieldType string `json:"jsonFieldType"` // Java/JSON type
	DBFieldName   string `json:"dbFieldName"`   // Database column name
	DBFieldType   string `json:"dbFieldType"`   // Database field type
	Required      bool   `json:"required"`
	Transform     string `json:"transform,omitempty"`
}

// IndexTable describes an index table
type IndexTable struct {
	TableName string       `json:"tableName"`
	Fields    []IndexField `json:"fields"`
}

// TypeInstruction holds metadata for a concrete class
type TypeInstruction struct {
	Name        string       `json:"name"`      // Class name
	Loader      string       `json:"loader"`    // Algorithm name
	MainTable   string       `json:"mainTable"` // Main table name
	DataTable   string       `json:"dataTable"` // Data table name
	Indexes     []IndexTable `json:"indexes"`   // Index tables
	AlgorithmID uint32       // Computed from Loader
}

// TransactionInstructionsResponse represents the API response
type TransactionInstructionsResponse struct {
	TransactionInstructions struct {
		Headers []string          `json:"headers"`
		Type    []TypeInstruction `json:"type"`
	} `json:"transactionInstructions"`
}

// Cache holds the metamodel in memory
type Cache struct {
	mu                sync.RWMutex
	types             map[string]*TypeInstruction // className -> instruction
	headers           []string
	dataDictionaryURL string
}

// NewCache creates a new metamodel cache
func NewCache(dataDictionaryURL string) *Cache {
	return &Cache{
		types:             make(map[string]*TypeInstruction),
		dataDictionaryURL: dataDictionaryURL,
	}
}

// Load fetches metamodel from DataDictionary
func (c *Cache) Load(ctx context.Context) error {
	url := fmt.Sprintf("%s/api/transaction-service/instructions", c.dataDictionaryURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to fetch instructions: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to fetch instructions: status %d, body: %s", resp.StatusCode, string(body))
	}

	var response TransactionInstructionsResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return fmt.Errorf("failed to decode instructions: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Clear existing data
	c.types = make(map[string]*TypeInstruction)
	c.headers = response.TransactionInstructions.Headers

	// Process types
	for i := range response.TransactionInstructions.Type {
		t := &response.TransactionInstructions.Type[i]

		// Compute algorithm ID from loader name
		if algID, ok := AlgorithmNameToID[t.Loader]; ok {
			t.AlgorithmID = algID
		}

		c.types[t.Name] = t
	}

	return nil
}

// GetTypeInstruction returns the full instruction for a class
func (c *Cache) GetTypeInstruction(className string) (*TypeInstruction, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if t, ok := c.types[className]; ok {
		return t, nil
	}

	return nil, fmt.Errorf("type not found: %s", className)
}

// GetAlgorithmID returns the algorithm ID for a class
func (c *Cache) GetAlgorithmID(className string) (uint32, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if t, ok := c.types[className]; ok {
		if t.AlgorithmID == 0 {
			return 0, fmt.Errorf("no algorithm defined for class: %s", className)
		}
		return t.AlgorithmID, nil
	}

	return 0, fmt.Errorf("type not found: %s", className)
}

// GetMainTable returns the main table name for a class
func (c *Cache) GetMainTable(className string) (string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if t, ok := c.types[className]; ok {
		if t.MainTable == "" {
			return "", fmt.Errorf("no main table defined for class: %s", className)
		}
		return t.MainTable, nil
	}

	return "", fmt.Errorf("type not found: %s", className)
}

// GetDataTable returns the data table name for a class
func (c *Cache) GetDataTable(className string) (string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if t, ok := c.types[className]; ok {
		if t.DataTable == "" {
			return "", fmt.Errorf("no data table defined for class: %s", className)
		}
		return t.DataTable, nil
	}

	return "", fmt.Errorf("type not found: %s", className)
}

// GetIndexTables returns the index tables for a class
func (c *Cache) GetIndexTables(className string) ([]IndexTable, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if t, ok := c.types[className]; ok {
		return t.Indexes, nil
	}

	return nil, fmt.Errorf("type not found: %s", className)
}

// GetHeaders returns the list of header names
func (c *Cache) GetHeaders() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.headers
}

// TypeCount returns the number of loaded types
func (c *Cache) TypeCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.types)
}
