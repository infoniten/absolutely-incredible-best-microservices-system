//go:build integration

package integration

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"github.com/quantara/object-framework/internal/domain"
	"github.com/quantara/object-framework/internal/parser"
	"github.com/quantara/object-framework/internal/repository"
)

// These tests connect to externally running containers
// Set POSTGRES_URL environment variable to run

func getPostgresURL() string {
	url := os.Getenv("POSTGRES_URL")
	if url == "" {
		url = "postgres://test:test@localhost:5432/testdb?sslmode=disable"
	}
	return url
}

func skipIfNoPostgres(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("postgres", getPostgresURL())
	if err != nil {
		t.Skipf("Skipping test, cannot connect to postgres: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Skipf("Skipping test, postgres not available: %v", err)
	}
	return db
}

func TestExternal_RawMessageRepository(t *testing.T) {
	db := skipIfNoPostgres(t)
	defer db.Close()

	ctx := context.Background()
	repo := repository.NewRawMessageRepository(db)

	// Create table
	if err := repo.CreateTableIfNotExists(ctx); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Generate unique message ID for this test run
	msgID := "test:" + time.Now().Format("20060102150405.000000000")

	// Create message
	msg := &domain.RawMessageDto{
		ID:        time.Now().UnixNano(),
		Metadata:  map[string]string{"test": "value"},
		MessageID: msgID,
		Value:     []byte("<xml>test</xml>"),
		Source:    domain.SourceMOEX,
		Status:    domain.RawMessageStatusProcessing,
	}

	if err := repo.Create(ctx, msg); err != nil {
		t.Fatalf("failed to create message: %v", err)
	}
	t.Logf("Created message with ID: %d, MessageID: %s", msg.ID, msg.MessageID)

	// Check exists
	exists, err := repo.Exists(ctx, msg.MessageID)
	if err != nil {
		t.Fatalf("failed to check existence: %v", err)
	}
	if !exists {
		t.Error("message should exist")
	}

	// Get by message ID
	found, err := repo.GetByMessageID(ctx, msg.MessageID)
	if err != nil {
		t.Fatalf("failed to get message: %v", err)
	}
	if found == nil {
		t.Fatal("message not found")
	}
	if found.Source != domain.SourceMOEX {
		t.Errorf("expected source MOEX, got %s", found.Source)
	}
	if string(found.Value) != "<xml>test</xml>" {
		t.Errorf("expected value '<xml>test</xml>', got %s", string(found.Value))
	}

	// Update status
	if err := repo.UpdateStatus(ctx, msg.ID, domain.RawMessageStatusSaved, ""); err != nil {
		t.Fatalf("failed to update status: %v", err)
	}

	// Verify update
	found, _ = repo.GetByMessageID(ctx, msg.MessageID)
	if found.Status != domain.RawMessageStatusSaved {
		t.Errorf("expected status SAVED, got %s", found.Status)
	}

	t.Log("RawMessageRepository test passed")
}

func TestExternal_GlobalIDMappingRepository(t *testing.T) {
	db := skipIfNoPostgres(t)
	defer db.Close()

	ctx := context.Background()
	repo := repository.NewGlobalIDMappingRepository(db)

	// Create table
	if err := repo.CreateTableIfNotExists(ctx); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Use unique external ID for this test run
	externalID := "TEST" + time.Now().Format("20060102150405")
	source := domain.SourceMOEX
	sourceObjectType := domain.SourceObjectTypeFXSPOT
	mappingID := time.Now().UnixNano()
	globalID := time.Now().UnixNano() + 1000

	// First call should create
	mapping, created, err := repo.GetOrCreate(ctx, externalID, source, sourceObjectType, mappingID, globalID)
	if err != nil {
		t.Fatalf("failed to get or create: %v", err)
	}
	if !created {
		t.Error("expected mapping to be created")
	}
	if mapping.GlobalID != globalID {
		t.Errorf("expected GlobalID %d, got %d", globalID, mapping.GlobalID)
	}
	t.Logf("Created mapping: externalID=%s, globalID=%d", externalID, mapping.GlobalID)

	// Second call should find existing
	mapping2, created2, err := repo.GetOrCreate(ctx, externalID, source, sourceObjectType, mappingID+1, globalID+1)
	if err != nil {
		t.Fatalf("failed to get or create second time: %v", err)
	}
	if created2 {
		t.Error("expected mapping to be found, not created")
	}
	if mapping2.GlobalID != globalID {
		t.Errorf("expected GlobalID %d (original), got %d", globalID, mapping2.GlobalID)
	}

	t.Log("GlobalIDMappingRepository test passed")
}

func TestExternal_MoexParser(t *testing.T) {
	xmlData := loadFixtureExternal(t, "moex_fxspot.xml")

	record, err := parser.ParseMoexMessage(xmlData)
	if err != nil {
		t.Fatalf("failed to parse: %v", err)
	}

	// Verify parsing
	if record.TradeNo != "8889069094" {
		t.Errorf("expected TRADENO 8889069094, got %s", record.TradeNo)
	}
	if record.BuySell != "B" {
		t.Errorf("expected BUYSELL B, got %s", record.BuySell)
	}
	if record.Price != "11.0576" {
		t.Errorf("expected PRICE 11.0576, got %s", record.Price)
	}

	// Convert to trade
	trade, err := record.ToFxSpotForwardTrade(100, 1, 2, 1, 1)
	if err != nil {
		t.Fatalf("failed to convert: %v", err)
	}

	if trade.Status != "live" {
		t.Errorf("expected status 'live', got %s", trade.Status)
	}
	if trade.DraftStatus != "CONFIRMED" {
		t.Errorf("expected draft status 'CONFIRMED', got %s", trade.DraftStatus)
	}
	if trade.Side != "BUY" {
		t.Errorf("expected side 'BUY', got %s", trade.Side)
	}
	if trade.FxRate != 11.0576 {
		t.Errorf("expected fxRate 11.0576, got %f", trade.FxRate)
	}
	if trade.BaseAmount != 250000000 {
		t.Errorf("expected baseAmount 250000000, got %f", trade.BaseAmount)
	}

	t.Log("MoexParser test passed")
}

func TestExternal_CashflowCreation(t *testing.T) {
	trade := &domain.FxSpotForwardTrade{
		ID:             12345,
		GlobalID:       67890,
		BaseValueDate:  time.Date(2026, 2, 5, 0, 0, 0, 0, time.UTC),
		FxRate:         11.0576,
		NotionalAmount: 2764400000.00,
	}

	cashflows := parser.CreateCashflows(trade, 100, 101)

	if len(cashflows) != 2 {
		t.Fatalf("expected 2 cashflows, got %d", len(cashflows))
	}

	// Verify RECEIVE cashflow
	receive := cashflows[0]
	if receive.ID != 100 {
		t.Errorf("expected receive ID 100, got %d", receive.ID)
	}
	if receive.ParentID != trade.ID {
		t.Errorf("expected receive ParentID %d, got %d", trade.ID, receive.ParentID)
	}
	if receive.Direction != domain.PaymentDirectionReceive {
		t.Errorf("expected RECEIVE direction, got %s", receive.Direction)
	}
	if receive.Type != domain.CashflowTypeReceivable {
		t.Errorf("expected RECEIVABLE type, got %s", receive.Type)
	}
	if receive.ConversionRate != trade.FxRate {
		t.Errorf("expected conversion rate %f, got %f", trade.FxRate, receive.ConversionRate)
	}

	// Verify PAY cashflow
	pay := cashflows[1]
	if pay.ID != 101 {
		t.Errorf("expected pay ID 101, got %d", pay.ID)
	}
	if pay.Direction != domain.PaymentDirectionPay {
		t.Errorf("expected PAY direction, got %s", pay.Direction)
	}
	if pay.Type != domain.CashflowTypePayable {
		t.Errorf("expected PAYABLE type, got %s", pay.Type)
	}

	t.Log("Cashflow creation test passed")
}

func loadFixtureExternal(t *testing.T, name string) []byte {
	t.Helper()
	// Try multiple paths
	paths := []string{
		"../fixtures/" + name,
		"test/fixtures/" + name,
		"fixtures/" + name,
	}
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err == nil {
			return data
		}
	}
	t.Fatalf("failed to load fixture %s from any path", name)
	return nil
}
