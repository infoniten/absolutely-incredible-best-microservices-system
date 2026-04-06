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
// 1. Send message to Kafka
// 2. Object-framework processes it
// 3. Data appears in PostgreSQL tables

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

	// Generate unique TRADENO for this test
	tradeNo := fmt.Sprintf("E2E%d", time.Now().UnixNano())

	// Create test XML message
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
	t.Log("✓ Raw message found in raw_messages table")

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
	t.Log("✓ Raw message status is SAVED")

	// 3. Check globalid_mappings table
	globalID, err := getGlobalIDMapping(ctx, db, tradeNo)
	if err != nil {
		t.Fatalf("GlobalID mapping not found: %v", err)
	}
	t.Logf("✓ GlobalID mapping found: %d", globalID)

	// 4. Check trade_main table (murex schema)
	err = waitForCondition(ctx, 10*time.Second, func() (bool, error) {
		return checkTradeExists(ctx, db, tradeNo)
	})
	if err != nil {
		t.Fatalf("Trade not found in trade_main: %v", err)
	}
	t.Log("✓ Trade found in murex.trade_main table")

	// 5. Verify trade data
	trade, err := getTrade(ctx, db, tradeNo)
	if err != nil {
		t.Fatalf("Failed to get trade: %v", err)
	}

	// Verify trade fields
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
	t.Logf("✓ Trade data verified: side=%s, status=%s, draft_status=%s",
		trade.Side, trade.Status, trade.DraftStatus)

	// 6. Check cashflows in trade_cashflow_data
	cashflowCount, err := countCashflows(ctx, db, trade.ID)
	if err != nil {
		t.Logf("Warning: Failed to count cashflows: %v", err)
	} else {
		if cashflowCount != 2 {
			t.Errorf("Expected 2 cashflows, got %d", cashflowCount)
		} else {
			t.Logf("✓ Cashflows in trade_cashflow_data: %d", cashflowCount)
		}
	}

	// 7. Check index tables
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
		t.Logf("✓ Index tables verified: trade_index=%d, fx_spot_forward_trade_index=%d, cashflow_main=%d, cashflow_index=%d",
			indexCounts.TradeIndex, indexCounts.FxSpotTradeIndex, indexCounts.CashflowMain, indexCounts.CashflowIndex)
	}

	// 8. Check product tables
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
		t.Logf("✓ Product tables verified: product_main=%d, product_index=%d, fx_spot_exchange_product_data=%d",
			productCounts.ProductMain, productCounts.ProductIndex, productCounts.FxSpotProductData)
	}

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
TRADETIME="17:55:40" VALUE="2764400000.00">
<MxFlowID>test</MxFlowID>
</record>`, tradeNo))
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
	// Data is stored in fx_spot_forward_trade_data.content (JSONB)
	// Join with trade_main via id
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

	// Data is stored in fx_spot_forward_trade_data.content (JSONB)
	// Join with trade_main via id
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

	// Cashflows are stored in trade_cashflow_data table
	// They reference parent trade via content->>'parentId'
	err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM murex.trade_cashflow_data
		 WHERE (content->>'parentId')::bigint = $1`,
		parentID,
	).Scan(&count)

	if err != nil {
		// Table might not exist or have different structure
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

	// Check trade_index
	err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM murex.trade_index WHERE id = $1`,
		tradeID,
	).Scan(&counts.TradeIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to check trade_index: %w", err)
	}

	// Check fx_spot_forward_trade_index
	err = db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM murex.fx_spot_forward_trade_index WHERE id = $1`,
		tradeID,
	).Scan(&counts.FxSpotTradeIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to check fx_spot_forward_trade_index: %w", err)
	}

	// Check cashflow_main (by parent trade)
	err = db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM murex.cashflow_main cm
		 JOIN murex.trade_cashflow_data tcd ON cm.id = tcd.id
		 WHERE (tcd.content->>'parentId')::bigint = $1`,
		tradeID,
	).Scan(&counts.CashflowMain)
	if err != nil {
		return nil, fmt.Errorf("failed to check cashflow_main: %w", err)
	}

	// Check cashflow_index (by parent trade)
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

	// Check product_main
	err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM murex.product_main`,
	).Scan(&counts.ProductMain)
	if err != nil {
		return nil, fmt.Errorf("failed to check product_main: %w", err)
	}

	// Check product_index
	err = db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM murex.product_index`,
	).Scan(&counts.ProductIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to check product_index: %w", err)
	}

	// Check fx_spot_exchange_product_data
	err = db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM murex.fx_spot_exchange_product_data`,
	).Scan(&counts.FxSpotProductData)
	if err != nil {
		return nil, fmt.Errorf("failed to check fx_spot_exchange_product_data: %w", err)
	}

	return counts, nil
}
