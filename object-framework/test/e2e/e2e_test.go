//go:build e2e

package e2e

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
)

// E2E test that verifies the full flow:
// 1. Seed reference dictionaries into the database
// 2. Send message to Kafka
// 3. Object-framework processes it
// 4. Data appears in PostgreSQL tables with enriched reference fields

const (
	defaultKafkaBroker  = "localhost:29092"
	defaultPostgresURL  = "postgres://postgres:postgres@localhost:5432/quantara?sslmode=disable"
	defaultKafkaTopic   = "moex.trades"
)

func getEnv(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}

// Seed IDs (high range to avoid conflicts with generated IDs)
const (
	seedSourceMOEXID       = 9000001
	seedSourceMOEXGlobalID = 9100001
	seedSourceSUDIRID      = 9000002
	seedSourceSUDIRGlobalID = 9100002

	seedLegalEntityID       = 9000010
	seedLegalEntityGlobalID = 9100010
	seedLegalEntityAltID    = 9000011

	seedCounterpartyID       = 9000020
	seedCounterpartyGlobalID = 9100020
	seedCounterpartyAltID    = 9000021

	seedDefaultCounterpartyID       = 9000025
	seedDefaultCounterpartyGlobalID = 9100025

	seedSystemID       = 9000030
	seedSystemGlobalID = 9100030

	seedPersonID       = 9000040
	seedPersonGlobalID = 9100040
	seedPersonAltID    = 9000041

	seedPortfolioID       = 9000050
	seedPortfolioGlobalID = 9100050
	seedPortfolioAltID    = 9000051

	seedVenueID       = 9000060
	seedVenueGlobalID = 9100060

	seedBaseCurrencyID       = 9000070
	seedBaseCurrencyGlobalID = 9100070

	seedNotionalCurrencyID       = 9000071
	seedNotionalCurrencyGlobalID = 9100071

	seedFxPairID       = 9000080
	seedFxPairGlobalID = 9100080
)

const closedAtInfinity = "3000-01-01 00:00:00"
const savedAtPast = "2020-01-01 00:00:00"

func TestE2E_FullProcessingFlow(t *testing.T) {
	ctx := context.Background()

	kafkaBroker := getEnv("KAFKA_BROKER", defaultKafkaBroker)
	postgresURL := getEnv("POSTGRES_URL", defaultPostgresURL)
	kafkaTopic := getEnv("KAFKA_TOPIC", defaultKafkaTopic)

	// Connect to PostgreSQL
	db, err := sql.Open("postgres", postgresURL)
	if err != nil {
		t.Fatalf("Failed to connect to postgres: %v", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("Failed to ping postgres: %v", err)
	}
	t.Log("Connected to PostgreSQL")

	// Seed reference data
	if err := seedReferenceData(ctx, db); err != nil {
		t.Fatalf("Failed to seed reference data: %v", err)
	}
	t.Log("Reference data seeded")

	// Generate unique TRADENO for this test
	tradeNo := fmt.Sprintf("E2E%d", time.Now().UnixNano())

	// Create test XML message (with all required attributes for enrichment)
	testMessage := createTestMessage(tradeNo)
	t.Logf("Test TRADENO: %s", tradeNo)

	// Send message to Kafka
	err = sendToKafka(ctx, kafkaBroker, kafkaTopic, tradeNo, testMessage)
	if err != nil {
		t.Fatalf("Failed to send message to Kafka: %v", err)
	}
	t.Log("Message sent to Kafka")

	// Wait for processing and verify
	t.Log("Waiting for object-framework to process...")

	// 1. Check raw_messages table
	err = waitForCondition(ctx, 30*time.Second, func() (bool, error) {
		return checkRawMessageExists(ctx, db, tradeNo)
	})
	if err != nil {
		t.Fatalf("Raw message not found: %v", err)
	}
	t.Log("Raw message found in raw_messages table")

	// 2. Check raw_messages status is SAVED
	err = waitForCondition(ctx, 60*time.Second, func() (bool, error) {
		return checkRawMessageStatus(ctx, db, tradeNo, "SAVED")
	})
	if err != nil {
		// Check if FAILED
		failed, _ := checkRawMessageStatus(ctx, db, tradeNo, "FAILED")
		if failed {
			errMsg := getRawMessageError(ctx, db, tradeNo)
			t.Fatalf("Message processing FAILED: %s", errMsg)
		}
		t.Fatalf("Raw message status not SAVED: %v", err)
	}
	t.Log("Raw message status is SAVED")

	// 3. Check globalid_mappings table
	globalID, err := getGlobalIDMapping(ctx, db, tradeNo)
	if err != nil {
		t.Fatalf("GlobalID mapping not found: %v", err)
	}
	t.Logf("GlobalID mapping found: %d", globalID)

	// 4. Check trade_main table (murex schema)
	err = waitForCondition(ctx, 10*time.Second, func() (bool, error) {
		return checkTradeExists(ctx, db, tradeNo)
	})
	if err != nil {
		t.Fatalf("Trade not found in trade_main: %v", err)
	}
	t.Log("Trade found in murex.trade_main table")

	// 5. Verify trade data (basic fields)
	trade, err := getTrade(ctx, db, tradeNo)
	if err != nil {
		t.Fatalf("Failed to get trade: %v", err)
	}

	if trade.ExternalID != tradeNo {
		t.Errorf("Expected external_id %s, got %s", tradeNo, trade.ExternalID)
	}
	if trade.Side != "BUY" {
		t.Errorf("Expected side BUY, got %s", trade.Side)
	}
	if trade.Status != "live" {
		t.Errorf("Expected status live, got %s", trade.Status)
	}
	if trade.DraftStatus != "CONFIRMED" {
		t.Errorf("Expected draft_status CONFIRMED, got %s", trade.DraftStatus)
	}
	t.Logf("Trade data verified: side=%s, status=%s, draft_status=%s",
		trade.Side, trade.Status, trade.DraftStatus)

	// 6. Verify enriched reference fields
	refs, err := getTradeReferenceFields(ctx, db, tradeNo)
	if err != nil {
		t.Fatalf("Failed to get trade reference fields: %v", err)
	}

	assertRefField(t, "legalEntityId", refs.LegalEntityID, seedLegalEntityGlobalID)
	assertRefField(t, "counterpartyId", refs.CounterpartyID, seedCounterpartyGlobalID)
	assertRefField(t, "sourceSystemId", refs.SourceSystemID, seedSystemGlobalID)
	assertRefField(t, "portfolioId", refs.PortfolioID, seedPortfolioGlobalID)
	assertRefField(t, "traderId", refs.TraderID, seedPersonGlobalID)
	assertRefField(t, "authorizedById", refs.AuthorizedByID, seedPersonGlobalID)
	assertRefField(t, "venueId", refs.VenueID, seedVenueGlobalID)
	assertRefField(t, "baseCurrencyId", refs.BaseCurrencyID, seedBaseCurrencyGlobalID)
	assertRefField(t, "notionalCurrencyId", refs.NotionalCurrencyID, seedNotionalCurrencyGlobalID)
	assertRefField(t, "instrumentId", refs.InstrumentID, seedFxPairGlobalID)
	t.Log("Trade reference fields verified")

	// 7. Check cashflows in trade_cashflow_data
	cashflowCount, err := countCashflows(ctx, db, trade.ID)
	if err != nil {
		t.Logf("Warning: Failed to count cashflows: %v", err)
	} else {
		if cashflowCount != 2 {
			t.Errorf("Expected 2 cashflows, got %d", cashflowCount)
		} else {
			t.Logf("Cashflows in trade_cashflow_data: %d", cashflowCount)
		}
	}

	// 8. Check index tables
	indexCounts, err := checkIndexTables(ctx, db, trade.ID)
	if err != nil {
		t.Logf("Warning: Failed to check index tables: %v", err)
	} else {
		if indexCounts.TradeIndex < 1 {
			t.Errorf("Expected trade_index entry, got %d", indexCounts.TradeIndex)
		}
		if indexCounts.FxSpotTradeIndex < 1 {
			t.Errorf("Expected fx_spot_forward_trade_index entry, got %d", indexCounts.FxSpotTradeIndex)
		}
		if indexCounts.CashflowMain < 2 {
			t.Errorf("Expected 2 cashflow_main entries, got %d", indexCounts.CashflowMain)
		}
		if indexCounts.CashflowIndex < 2 {
			t.Errorf("Expected 2 cashflow_index entries, got %d", indexCounts.CashflowIndex)
		}
		t.Logf("Index tables verified: trade_index=%d, fx_spot_forward_trade_index=%d, cashflow_main=%d, cashflow_index=%d",
			indexCounts.TradeIndex, indexCounts.FxSpotTradeIndex, indexCounts.CashflowMain, indexCounts.CashflowIndex)
	}

	// 9. Check product tables
	productCounts, err := checkProductTables(ctx, db)
	if err != nil {
		t.Logf("Warning: Failed to check product tables: %v", err)
	} else {
		if productCounts.ProductMain < 1 {
			t.Errorf("Expected product_main entry, got %d", productCounts.ProductMain)
		}
		if productCounts.ProductIndex < 1 {
			t.Errorf("Expected product_index entry, got %d", productCounts.ProductIndex)
		}
		if productCounts.FxSpotProductData < 1 {
			t.Errorf("Expected fx_spot_exchange_product_data entry, got %d", productCounts.FxSpotProductData)
		}
		t.Logf("Product tables verified: product_main=%d, product_index=%d, fx_spot_exchange_product_data=%d",
			productCounts.ProductMain, productCounts.ProductIndex, productCounts.FxSpotProductData)
	}

	// Cleanup seed data
	cleanupReferenceData(ctx, db)

	t.Log("E2E test completed successfully!")
}

func createTestMessage(tradeNo string) []byte {
	return []byte(fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<record timestamp="2026-02-02T00:00:00.000" exchange="MOEX" market="CUR" system="TRD" entity="SBRF" interface="ASTS" table="TRADES"
ACCOUNT="MB0002500722" BANKACCID="00722" BANKID="NCC" BUYSELL="B"
CLEARINGBANKACCID="00722" CLEARINGCENTERCOMM="26434.58" CLEARINGFIRMID="MB0002500000"
COMMISSION="62199.01" CPFIRMID="MB0046000000" CURRENCYID="RUB"
PRICE="11.0576" QUANTITY="250000000" SECCODE="CNYRUB_TOM"
SETTLEDATE="2026-02-05" TRADEDATE="2026-02-02" TRADENO="%s"
TRADETIME="17:55:40" VALUE="2764400000.00"
SECURITIES-FACEUNIT="CNY" SECURITIES-FACEVALUE="10"
USERID="TESTUSER01" TRADER_SIGMA_LOGIN="test.trader"
BRD-BOOKNAME="TEST-BOOK">
<MxFlowID>test</MxFlowID>
</record>`, tradeNo))
}

// seedReferenceData inserts dictionary entries into the murex schema
// so that the enrichment step in the processor can resolve reference IDs.
func seedReferenceData(ctx context.Context, db *sql.DB) error {
	// Source: MOEX
	if err := seedRevisionedEntity(ctx, db, "source_main", seedSourceMOEXID, seedSourceMOEXGlobalID, "Source"); err != nil {
		return fmt.Errorf("seed source MOEX: %w", err)
	}
	if err := seedIndexRow(ctx, db, "source_index", "id, alias", seedSourceMOEXID, "MOEX"); err != nil {
		return fmt.Errorf("seed source_index MOEX: %w", err)
	}
	if err := seedDataTable(ctx, db, "source_data", seedSourceMOEXID,
		fmt.Sprintf(`{"id":%d,"globalId":%d,"alias":"MOEX"}`, seedSourceMOEXID, seedSourceMOEXGlobalID)); err != nil {
		return fmt.Errorf("seed source_data MOEX: %w", err)
	}

	// Source: SUDIR
	if err := seedRevisionedEntity(ctx, db, "source_main", seedSourceSUDIRID, seedSourceSUDIRGlobalID, "Source"); err != nil {
		return fmt.Errorf("seed source SUDIR: %w", err)
	}
	if err := seedIndexRow(ctx, db, "source_index", "id, alias", seedSourceSUDIRID, "SUDIR"); err != nil {
		return fmt.Errorf("seed source_index SUDIR: %w", err)
	}
	if err := seedDataTable(ctx, db, "source_data", seedSourceSUDIRID,
		fmt.Sprintf(`{"id":%d,"globalId":%d,"alias":"SUDIR"}`, seedSourceSUDIRID, seedSourceSUDIRGlobalID)); err != nil {
		return fmt.Errorf("seed source_data SUDIR: %w", err)
	}

	// LegalEntity (with alternative identifier for entity="SBRF")
	if err := seedRevisionedEntity(ctx, db, "legal_entity_main", seedLegalEntityID, seedLegalEntityGlobalID, "LegalEntity"); err != nil {
		return fmt.Errorf("seed legal_entity: %w", err)
	}
	if err := seedIndexRow(ctx, db, "legal_entity_index", "id, code", seedLegalEntityID, "SBRF"); err != nil {
		return fmt.Errorf("seed legal_entity_index: %w", err)
	}
	if err := seedDataTable(ctx, db, "legal_entity_data", seedLegalEntityID,
		fmt.Sprintf(`{"id":%d,"globalId":%d,"code":"SBRF"}`, seedLegalEntityID, seedLegalEntityGlobalID)); err != nil {
		return fmt.Errorf("seed legal_entity_data: %w", err)
	}
	if err := seedAlternativeIdentifier(ctx, db, seedLegalEntityAltID, seedLegalEntityID, seedSourceMOEXGlobalID, "SBRF", "LegalEntityAlternativeIdentifier"); err != nil {
		return fmt.Errorf("seed legal_entity alt id: %w", err)
	}

	// Counterparty (with alternative identifier for BANKID="NCC")
	if err := seedRevisionedEntity(ctx, db, "counterparty_main", seedCounterpartyID, seedCounterpartyGlobalID, "Counterparty"); err != nil {
		return fmt.Errorf("seed counterparty: %w", err)
	}
	if err := seedIndexRow(ctx, db, "counterparty_index", "id, code", seedCounterpartyID, "NCC"); err != nil {
		return fmt.Errorf("seed counterparty_index: %w", err)
	}
	if err := seedDataTable(ctx, db, "counterparty_data", seedCounterpartyID,
		fmt.Sprintf(`{"id":%d,"globalId":%d,"code":"NCC"}`, seedCounterpartyID, seedCounterpartyGlobalID)); err != nil {
		return fmt.Errorf("seed counterparty_data: %w", err)
	}
	if err := seedAlternativeIdentifier(ctx, db, seedCounterpartyAltID, seedCounterpartyID, seedSourceMOEXGlobalID, "NCC", "CounterpartyAlternativeIdentifier"); err != nil {
		return fmt.Errorf("seed counterparty alt id: %w", err)
	}

	// Default Counterparty (NKCBN fallback)
	if err := seedRevisionedEntity(ctx, db, "counterparty_main", seedDefaultCounterpartyID, seedDefaultCounterpartyGlobalID, "Counterparty"); err != nil {
		return fmt.Errorf("seed default counterparty: %w", err)
	}
	if err := seedIndexRow(ctx, db, "counterparty_index", "id, code", seedDefaultCounterpartyID, "NKCBN"); err != nil {
		return fmt.Errorf("seed default counterparty_index: %w", err)
	}
	if err := seedDataTable(ctx, db, "counterparty_data", seedDefaultCounterpartyID,
		fmt.Sprintf(`{"id":%d,"globalId":%d,"code":"NKCBN"}`, seedDefaultCounterpartyID, seedDefaultCounterpartyGlobalID)); err != nil {
		return fmt.Errorf("seed default counterparty_data: %w", err)
	}

	// System (code="MC")
	if err := seedRevisionedEntity(ctx, db, "system_main", seedSystemID, seedSystemGlobalID, "System"); err != nil {
		return fmt.Errorf("seed system: %w", err)
	}
	if err := seedIndexRow(ctx, db, "system_index", "id, code", seedSystemID, "MC"); err != nil {
		return fmt.Errorf("seed system_index: %w", err)
	}
	if err := seedDataTable(ctx, db, "system_data", seedSystemID,
		fmt.Sprintf(`{"id":%d,"globalId":%d,"code":"MC"}`, seedSystemID, seedSystemGlobalID)); err != nil {
		return fmt.Errorf("seed system_data: %w", err)
	}

	// Person (trader, with alternative identifier for TraderSigmaLogin)
	if err := seedRevisionedEntity(ctx, db, "person_main", seedPersonID, seedPersonGlobalID, "Person"); err != nil {
		return fmt.Errorf("seed person: %w", err)
	}
	if err := seedIndexRow(ctx, db, "person_index", "id, personal_number", seedPersonID, "PN001"); err != nil {
		return fmt.Errorf("seed person_index: %w", err)
	}
	if err := seedDataTable(ctx, db, "person_data", seedPersonID,
		fmt.Sprintf(`{"id":%d,"globalId":%d,"personalNumber":"PN001"}`, seedPersonID, seedPersonGlobalID)); err != nil {
		return fmt.Errorf("seed person_data: %w", err)
	}
	if err := seedAlternativeIdentifier(ctx, db, seedPersonAltID, seedPersonID, seedSourceSUDIRGlobalID, "test.trader", "PersonAlternativeIdentifier"); err != nil {
		return fmt.Errorf("seed person alt id: %w", err)
	}

	// Portfolio (with alternative identifier for BRD-BOOKNAME="TEST-BOOK")
	if err := seedRevisionedEntity(ctx, db, "portfolio_main", seedPortfolioID, seedPortfolioGlobalID, "Portfolio"); err != nil {
		return fmt.Errorf("seed portfolio: %w", err)
	}
	if err := seedIndexRow(ctx, db, "portfolio_index", "id, code", seedPortfolioID, "TEST-BOOK"); err != nil {
		return fmt.Errorf("seed portfolio_index: %w", err)
	}
	if err := seedDataTable(ctx, db, "portfolio_data", seedPortfolioID,
		fmt.Sprintf(`{"id":%d,"globalId":%d,"code":"TEST-BOOK"}`, seedPortfolioID, seedPortfolioGlobalID)); err != nil {
		return fmt.Errorf("seed portfolio_data: %w", err)
	}
	if err := seedAlternativeIdentifier(ctx, db, seedPortfolioAltID, seedPortfolioID, seedSourceMOEXGlobalID, "TEST-BOOK", "PortfolioAlternativeIdentifier"); err != nil {
		return fmt.Errorf("seed portfolio alt id: %w", err)
	}

	// Venue (code="MOEX")
	if err := seedRevisionedEntity(ctx, db, "venue_main", seedVenueID, seedVenueGlobalID, "Venue"); err != nil {
		return fmt.Errorf("seed venue: %w", err)
	}
	if err := seedIndexRow(ctx, db, "venue_index", "id, code", seedVenueID, "MOEX"); err != nil {
		return fmt.Errorf("seed venue_index: %w", err)
	}
	if err := seedDataTable(ctx, db, "venue_data", seedVenueID,
		fmt.Sprintf(`{"id":%d,"globalId":%d,"code":"MOEX"}`, seedVenueID, seedVenueGlobalID)); err != nil {
		return fmt.Errorf("seed venue_data: %w", err)
	}

	// Currency: CNY (base currency, Asset.code = SECURITIES-FACEUNIT)
	if err := seedRevisionedEntity(ctx, db, "asset_main", seedBaseCurrencyID, seedBaseCurrencyGlobalID, "Currency"); err != nil {
		return fmt.Errorf("seed base currency: %w", err)
	}
	if err := seedIndexRow(ctx, db, "asset_index", "id, code", seedBaseCurrencyID, "CNY"); err != nil {
		return fmt.Errorf("seed asset_index CNY: %w", err)
	}
	if err := seedIndexRow(ctx, db, "currency_index", "id", seedBaseCurrencyID); err != nil {
		return fmt.Errorf("seed currency_index CNY: %w", err)
	}
	if err := seedDataTable(ctx, db, "currency_data", seedBaseCurrencyID,
		fmt.Sprintf(`{"id":%d,"globalId":%d,"code":"CNY"}`, seedBaseCurrencyID, seedBaseCurrencyGlobalID)); err != nil {
		return fmt.Errorf("seed currency_data CNY: %w", err)
	}

	// Currency: RUB (notional currency, Asset.code = CURRENCYID)
	if err := seedRevisionedEntity(ctx, db, "asset_main", seedNotionalCurrencyID, seedNotionalCurrencyGlobalID, "Currency"); err != nil {
		return fmt.Errorf("seed notional currency: %w", err)
	}
	if err := seedIndexRow(ctx, db, "asset_index", "id, code", seedNotionalCurrencyID, "RUB"); err != nil {
		return fmt.Errorf("seed asset_index RUB: %w", err)
	}
	if err := seedIndexRow(ctx, db, "currency_index", "id", seedNotionalCurrencyID); err != nil {
		return fmt.Errorf("seed currency_index RUB: %w", err)
	}
	if err := seedDataTable(ctx, db, "currency_data", seedNotionalCurrencyID,
		fmt.Sprintf(`{"id":%d,"globalId":%d,"code":"RUB"}`, seedNotionalCurrencyID, seedNotionalCurrencyGlobalID)); err != nil {
		return fmt.Errorf("seed currency_data RUB: %w", err)
	}

	// FxPair (Instrument.code = SecuritiesFaceValue/CurrencyID = "10/RUB")
	if err := seedRevisionedEntity(ctx, db, "draftable_revisioned_entity_main", seedFxPairID, seedFxPairGlobalID, "FxPair"); err != nil {
		return fmt.Errorf("seed fx_pair: %w", err)
	}
	if err := seedIndexRow(ctx, db, "instrument_index", "id, code", seedFxPairID, "10/RUB"); err != nil {
		return fmt.Errorf("seed instrument_index: %w", err)
	}
	if err := seedIndexRow(ctx, db, "fx_pair_index", "id", seedFxPairID); err != nil {
		return fmt.Errorf("seed fx_pair_index: %w", err)
	}
	if err := seedDataTable(ctx, db, "fx_pair_data", seedFxPairID,
		fmt.Sprintf(`{"id":%d,"globalId":%d,"code":"10/RUB"}`, seedFxPairID, seedFxPairGlobalID)); err != nil {
		return fmt.Errorf("seed fx_pair_data: %w", err)
	}

	return nil
}

func seedRevisionedEntity(ctx context.Context, db *sql.DB, table string, id, globalID int64, objectClass string) error {
	return execSQL(ctx, db,
		fmt.Sprintf(`INSERT INTO murex.%s (id, global_id, object_class, revision, version, saved_at, closed_at)
		VALUES ($1, $2, $3, 1, 1, $4, $5) ON CONFLICT DO NOTHING`, table),
		id, globalID, objectClass, savedAtPast, closedAtInfinity)
}

func seedIndexRow(ctx context.Context, db *sql.DB, table string, columns string, args ...interface{}) error {
	placeholders := ""
	for i := range args {
		if i > 0 {
			placeholders += ", "
		}
		placeholders += fmt.Sprintf("$%d", i+1)
	}
	// Append created_at as last column
	nextIdx := len(args) + 1
	columns += ", created_at"
	placeholders += fmt.Sprintf(", $%d", nextIdx)
	args = append(args, savedAtPast)

	return execSQL(ctx, db,
		fmt.Sprintf(`INSERT INTO murex.%s (%s) VALUES (%s) ON CONFLICT DO NOTHING`, table, columns, placeholders),
		args...)
}

func seedAlternativeIdentifier(ctx context.Context, db *sql.DB, altID, parentID, sourceGlobalID int64, value, objectClass string) error {
	if err := execSQL(ctx, db,
		`INSERT INTO murex.alternative_identifier_main (id, object_class, parent_id, created_at)
		 VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING`,
		altID, objectClass, parentID, savedAtPast); err != nil {
		return err
	}
	return seedIndexRow(ctx, db, "alternative_identifier_index", "id, value, source_id", altID, value, sourceGlobalID)
}

func seedDataTable(ctx context.Context, db *sql.DB, table string, id int64, content string) error {
	return execSQL(ctx, db,
		fmt.Sprintf(`INSERT INTO murex.%s (id, content, created_at) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`, table),
		id, content, savedAtPast)
}

func execSQL(ctx context.Context, db *sql.DB, query string, args ...interface{}) error {
	_, err := db.ExecContext(ctx, query, args...)
	return err
}

func cleanupReferenceData(ctx context.Context, db *sql.DB) {
	seedIDs := []int64{
		seedSourceMOEXID, seedSourceSUDIRID,
		seedLegalEntityID, seedLegalEntityAltID,
		seedCounterpartyID, seedCounterpartyAltID, seedDefaultCounterpartyID,
		seedSystemID,
		seedPersonID, seedPersonAltID,
		seedPortfolioID, seedPortfolioAltID,
		seedVenueID,
		seedBaseCurrencyID, seedNotionalCurrencyID,
		seedFxPairID,
	}

	tables := []string{
		// Data tables first
		"source_data", "legal_entity_data", "counterparty_data", "system_data",
		"person_data", "portfolio_data", "venue_data", "currency_data", "fx_pair_data",
		// Index tables
		"source_index", "legal_entity_index", "counterparty_index", "system_index",
		"person_index", "portfolio_index", "venue_index", "asset_index", "currency_index",
		"instrument_index", "fx_pair_index", "alternative_identifier_index",
		// Main tables
		"alternative_identifier_main",
		"source_main", "legal_entity_main", "counterparty_main", "system_main",
		"person_main", "portfolio_main", "venue_main", "asset_main",
		"draftable_revisioned_entity_main",
	}

	for _, table := range tables {
		for _, id := range seedIDs {
			db.ExecContext(ctx, fmt.Sprintf("DELETE FROM murex.%s WHERE id = $1", table), id)
		}
	}
}

// TradeReferenceFields holds the enriched reference IDs from the trade JSONB content
type TradeReferenceFields struct {
	LegalEntityID      *int64
	CounterpartyID     *int64
	SourceSystemID     *int64
	PortfolioID        *int64
	TraderID           *int64
	AuthorizedByID     *int64
	VenueID            *int64
	BaseCurrencyID     *int64
	NotionalCurrencyID *int64
	InstrumentID       *int64
}

func getTradeReferenceFields(ctx context.Context, db *sql.DB, tradeNo string) (*TradeReferenceFields, error) {
	refs := &TradeReferenceFields{}
	err := db.QueryRowContext(ctx, `
		SELECT
			(td.content->>'legalEntityId')::bigint,
			(td.content->>'counterpartyId')::bigint,
			(td.content->>'sourceSystemId')::bigint,
			(td.content->>'portfolioId')::bigint,
			(td.content->>'traderId')::bigint,
			(td.content->>'authorizedById')::bigint,
			(td.content->>'venueId')::bigint,
			(td.content->>'baseCurrencyId')::bigint,
			(td.content->>'notionalCurrencyId')::bigint,
			(td.content->>'instrumentId')::bigint
		FROM murex.trade_main tm
		JOIN murex.fx_spot_forward_trade_data td ON tm.id = td.id
		WHERE td.content->>'externalId' = $1
		ORDER BY tm.id DESC
		LIMIT 1
	`, tradeNo).Scan(
		&refs.LegalEntityID,
		&refs.CounterpartyID,
		&refs.SourceSystemID,
		&refs.PortfolioID,
		&refs.TraderID,
		&refs.AuthorizedByID,
		&refs.VenueID,
		&refs.BaseCurrencyID,
		&refs.NotionalCurrencyID,
		&refs.InstrumentID,
	)
	return refs, err
}

func assertRefField(t *testing.T, name string, actual *int64, expected int64) {
	t.Helper()
	if actual == nil {
		t.Errorf("Expected %s=%d, got nil", name, expected)
		return
	}
	if *actual != expected {
		t.Errorf("Expected %s=%d, got %d", name, expected, *actual)
	}
}

func sendToKafka(ctx context.Context, broker, topic, key string, message []byte) error {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(broker),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		WriteTimeout: 10 * time.Second,
	}
	defer writer.Close()

	return writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: message,
	})
}

func waitForCondition(ctx context.Context, timeout time.Duration, check func() (bool, error)) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok, err := check()
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("condition not met within %v", timeout)
}

func checkRawMessageExists(ctx context.Context, db *sql.DB, tradeNo string) (bool, error) {
	var count int
	err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM raw_messages WHERE convert_from(value, 'UTF8') LIKE $1",
		"%TRADENO=\""+tradeNo+"\"%",
	).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func checkRawMessageStatus(ctx context.Context, db *sql.DB, tradeNo string, status string) (bool, error) {
	var count int
	err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM raw_messages WHERE convert_from(value, 'UTF8') LIKE $1 AND status = $2",
		"%TRADENO=\""+tradeNo+"\"%", status,
	).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func getRawMessageError(ctx context.Context, db *sql.DB, tradeNo string) string {
	var errMsg sql.NullString
	db.QueryRowContext(ctx,
		"SELECT error FROM raw_messages WHERE convert_from(value, 'UTF8') LIKE $1",
		"%TRADENO=\""+tradeNo+"\"%",
	).Scan(&errMsg)
	if errMsg.Valid {
		return errMsg.String
	}
	return "unknown error"
}

func getGlobalIDMapping(ctx context.Context, db *sql.DB, tradeNo string) (int64, error) {
	var globalID int64
	err := db.QueryRowContext(ctx,
		"SELECT global_id FROM globalid_mappings WHERE external_id = $1 AND source = 'MOEX'",
		tradeNo,
	).Scan(&globalID)
	if err != nil {
		return 0, err
	}
	return globalID, nil
}

func checkTradeExists(ctx context.Context, db *sql.DB, tradeNo string) (bool, error) {
	var count int
	err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM murex.trade_main tm
		 JOIN murex.fx_spot_forward_trade_data td ON tm.id = td.id
		 WHERE td.content->>'externalId' = $1`,
		tradeNo,
	).Scan(&count)
	if err != nil {
		return false, nil // Table might not exist yet
	}
	return count > 0, nil
}

type Trade struct {
	ID          int64
	GlobalID    int64
	ExternalID  string
	Side        string
	Status      string
	DraftStatus string
}

func getTrade(ctx context.Context, db *sql.DB, tradeNo string) (*Trade, error) {
	var trade Trade

	err := db.QueryRowContext(ctx, `
		SELECT
			tm.id,
			tm.global_id,
			td.content->>'externalId' as external_id,
			td.content->>'side' as side,
			td.content->>'status' as status,
			COALESCE(td.content->>'draftStatus', 'CONFIRMED') as draft_status
		FROM murex.trade_main tm
		JOIN murex.fx_spot_forward_trade_data td ON tm.id = td.id
		WHERE td.content->>'externalId' = $1
		ORDER BY tm.id DESC
		LIMIT 1
	`, tradeNo).Scan(&trade.ID, &trade.GlobalID, &trade.ExternalID, &trade.Side, &trade.Status, &trade.DraftStatus)

	return &trade, err
}

func countCashflows(ctx context.Context, db *sql.DB, parentID int64) (int, error) {
	var count int

	err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM murex.trade_cashflow_data
		 WHERE (content->>'parentId')::bigint = $1`,
		parentID,
	).Scan(&count)

	if err != nil {
		return 0, err
	}

	return count, err
}

// IndexCounts holds counts from various index tables
type IndexCounts struct {
	TradeIndex        int
	FxSpotTradeIndex  int
	CashflowMain      int
	CashflowIndex     int
}

func checkIndexTables(ctx context.Context, db *sql.DB, tradeID int64) (*IndexCounts, error) {
	counts := &IndexCounts{}

	err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM murex.trade_index WHERE id = $1`,
		tradeID,
	).Scan(&counts.TradeIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to check trade_index: %w", err)
	}

	err = db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM murex.fx_spot_forward_trade_index WHERE id = $1`,
		tradeID,
	).Scan(&counts.FxSpotTradeIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to check fx_spot_forward_trade_index: %w", err)
	}

	err = db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM murex.cashflow_main cm
		 JOIN murex.trade_cashflow_data tcd ON cm.id = tcd.id
		 WHERE (tcd.content->>'parentId')::bigint = $1`,
		tradeID,
	).Scan(&counts.CashflowMain)
	if err != nil {
		return nil, fmt.Errorf("failed to check cashflow_main: %w", err)
	}

	err = db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM murex.cashflow_index ci
		 JOIN murex.trade_cashflow_data tcd ON ci.id = tcd.id
		 WHERE (tcd.content->>'parentId')::bigint = $1`,
		tradeID,
	).Scan(&counts.CashflowIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to check cashflow_index: %w", err)
	}

	return counts, nil
}

// ProductCounts holds counts from product tables
type ProductCounts struct {
	ProductMain       int
	ProductIndex      int
	FxSpotProductData int
}

func checkProductTables(ctx context.Context, db *sql.DB) (*ProductCounts, error) {
	counts := &ProductCounts{}

	err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM murex.product_main`,
	).Scan(&counts.ProductMain)
	if err != nil {
		return nil, fmt.Errorf("failed to check product_main: %w", err)
	}

	err = db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM murex.product_index`,
	).Scan(&counts.ProductIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to check product_index: %w", err)
	}

	err = db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM murex.fx_spot_exchange_product_data`,
	).Scan(&counts.FxSpotProductData)
	if err != nil {
		return nil, fmt.Errorf("failed to check fx_spot_exchange_product_data: %w", err)
	}

	return counts, nil
}
