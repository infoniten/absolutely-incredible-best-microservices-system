package parser

import (
	"testing"
	"time"

	"github.com/quantara/object-framework/internal/domain"
)

func TestParseMoexMessage(t *testing.T) {
	xmlData := []byte(`<?xml version="1.0" encoding="UTF-8"?>
<record timestamp="2026-02-02T00:00:00.000" exchange="MOEX" market="CUR" system="TRD" entity="SBRF" interface="ASTS" version="" table="TRADES" ACCOUNT="MB0002500722" BANKACCID="00722" BANKID="NCC" BUYSELL="B" CLEARINGBANKACCID="00722" CLEARINGCENTERCOMM="26434.58" CLEARINGFIRMID="MB0002500000" CLEARINGTRDACCID="MB0002500722" COMMISSION="62199.01" CPFIRMID="MB0046000000" CPORDERNO="39376853588" CURRENCYID="RUB" EXCHANGECOMM="35764.43" FIRMID="MB0002500000" HIDDENQTYORDER="N" MAKERFLAG="M" MICROSECONDS="504322" ORDERNO="39376842992" PERIOD="N" PRICE="11.0576" QUANTITY="250000000" SECBOARD="CNGD" SECCODE="CNYRUB_TOM" SETTLECODE="T1" SETTLEDATE="2026-02-05" SETTLELIABILITYFLAG="N" STATUS="M" TRADEDATE="2026-02-02" TRADENO="8889069094" PelicanTrdID="8889069094" TRADETIME="17:55:40" TRADETYPE="N" TRADINGSYSTEMCOMM="0.00" USERID="MD0002501028" VALUE="2764400000.00" SECURITIES-SHORTNAME="CNYRUB_TOM" SECURITIES-FACEVALUE="1" SECURITIES-FACEUNIT="CNY" SECURITIES-LOTSIZE="1" SECURITIES-CURRENCYID="RUB" INSTRS-INSTRTYPE="C" INSTRS-INSTRFWD="N" TRADER_SOURCE="MOEXTerminal" TRADER_SIGMA_LOGIN="19808785" PAPA-ALGOID="NOT_FOUND" BRD-BOOKNAME="NOT_FOUND" BRD-PLBASECCY="NOT_FOUND" BRD-FOLDERCCY="NOT_FOUND"><MxFlowID>kfmxf</MxFlowID></record>`)

	record, err := ParseMoexMessage(xmlData)
	if err != nil {
		t.Fatalf("failed to parse: %v", err)
	}

	// Test metadata
	if record.Exchange != "MOEX" {
		t.Errorf("expected exchange MOEX, got %s", record.Exchange)
	}
	if record.Market != "CUR" {
		t.Errorf("expected market CUR, got %s", record.Market)
	}

	// Test trade fields
	if record.TradeNo != "8889069094" {
		t.Errorf("expected TRADENO 8889069094, got %s", record.TradeNo)
	}
	if record.BuySell != "B" {
		t.Errorf("expected BUYSELL B, got %s", record.BuySell)
	}
	if record.Price != "11.0576" {
		t.Errorf("expected PRICE 11.0576, got %s", record.Price)
	}
	if record.Quantity != "250000000" {
		t.Errorf("expected QUANTITY 250000000, got %s", record.Quantity)
	}
	if record.Value != "2764400000.00" {
		t.Errorf("expected VALUE 2764400000.00, got %s", record.Value)
	}
	if record.TradeDate != "2026-02-02" {
		t.Errorf("expected TRADEDATE 2026-02-02, got %s", record.TradeDate)
	}
	if record.TradeTime != "17:55:40" {
		t.Errorf("expected TRADETIME 17:55:40, got %s", record.TradeTime)
	}
	if record.SettleDate != "2026-02-05" {
		t.Errorf("expected SETTLEDATE 2026-02-05, got %s", record.SettleDate)
	}

	// Test nested element
	if record.MxFlowID != "kfmxf" {
		t.Errorf("expected MxFlowID kfmxf, got %s", record.MxFlowID)
	}
}

func TestToFxSpotForwardTrade(t *testing.T) {
	xmlData := []byte(`<?xml version="1.0" encoding="UTF-8"?>
<record BUYSELL="B" PRICE="11.0576" QUANTITY="250000000" VALUE="2764400000.00" TRADEDATE="2026-02-02" TRADETIME="17:55:40" TRADENO="8889069094" SETTLEDATE="2026-02-05"></record>`)

	record, err := ParseMoexMessage(xmlData)
	if err != nil {
		t.Fatalf("failed to parse: %v", err)
	}

	trade, err := record.ToFxSpotForwardTrade(100, 1, 2, 1, 1)
	if err != nil {
		t.Fatalf("failed to convert: %v", err)
	}

	// Verify trade fields
	if trade.ID != 1 {
		t.Errorf("expected ID 1, got %d", trade.ID)
	}
	if trade.GlobalID != 2 {
		t.Errorf("expected GlobalID 2, got %d", trade.GlobalID)
	}
	if trade.ProductID != 100 {
		t.Errorf("expected ProductID 100, got %d", trade.ProductID)
	}
	if trade.Version != 1 {
		t.Errorf("expected Version 1, got %d", trade.Version)
	}
	if trade.Revision != 1 {
		t.Errorf("expected Revision 1, got %d", trade.Revision)
	}
	if trade.Status != "live" {
		t.Errorf("expected Status live, got %s", trade.Status)
	}
	if trade.DraftStatus != "CONFIRMED" {
		t.Errorf("expected DraftStatus CONFIRMED, got %s", trade.DraftStatus)
	}
	if trade.Side != "BUY" {
		t.Errorf("expected Side BUY, got %s", trade.Side)
	}
	if trade.ExternalID != "8889069094" {
		t.Errorf("expected ExternalID 8889069094, got %s", trade.ExternalID)
	}
	if trade.BaseAmount != 250000000 {
		t.Errorf("expected BaseAmount 250000000, got %f", trade.BaseAmount)
	}
	if trade.NotionalAmount != 2764400000.00 {
		t.Errorf("expected NotionalAmount 2764400000.00, got %f", trade.NotionalAmount)
	}
	if trade.FxRate != 11.0576 {
		t.Errorf("expected FxRate 11.0576, got %f", trade.FxRate)
	}

	expectedTradeDate := time.Date(2026, 2, 2, 0, 0, 0, 0, time.UTC)
	if !trade.TradeDate.Equal(expectedTradeDate) {
		t.Errorf("expected TradeDate %v, got %v", expectedTradeDate, trade.TradeDate)
	}

	expectedSettleDate := time.Date(2026, 2, 5, 0, 0, 0, 0, time.UTC)
	if !trade.BaseValueDate.Equal(expectedSettleDate) {
		t.Errorf("expected BaseValueDate %v, got %v", expectedSettleDate, trade.BaseValueDate)
	}
}

func TestCreateCashflows(t *testing.T) {
	trade := &domain.FxSpotForwardTrade{
		ID:             1,
		BaseValueDate:  time.Date(2026, 2, 5, 0, 0, 0, 0, time.UTC),
		FxRate:         11.0576,
		NotionalAmount: 2764400000.00,
	}

	cashflows := CreateCashflows(trade, 10, 11)

	if len(cashflows) != 2 {
		t.Fatalf("expected 2 cashflows, got %d", len(cashflows))
	}

	receive := cashflows[0]
	pay := cashflows[1]

	// Check receive cashflow
	if receive.ID != 10 {
		t.Errorf("expected receive ID 10, got %d", receive.ID)
	}
	if receive.ParentID != 1 {
		t.Errorf("expected receive ParentID 1, got %d", receive.ParentID)
	}
	if receive.Direction != domain.PaymentDirectionReceive {
		t.Errorf("expected receive Direction RECEIVE, got %s", receive.Direction)
	}
	if receive.Type != domain.CashflowTypeReceivable {
		t.Errorf("expected receive Type RECEIVABLE, got %s", receive.Type)
	}
	if receive.IsSettled != false {
		t.Errorf("expected receive IsSettled false, got %v", receive.IsSettled)
	}

	// Check pay cashflow
	if pay.ID != 11 {
		t.Errorf("expected pay ID 11, got %d", pay.ID)
	}
	if pay.ParentID != 1 {
		t.Errorf("expected pay ParentID 1, got %d", pay.ParentID)
	}
	if pay.Direction != domain.PaymentDirectionPay {
		t.Errorf("expected pay Direction PAY, got %s", pay.Direction)
	}
	if pay.Type != domain.CashflowTypePayable {
		t.Errorf("expected pay Type PAYABLE, got %s", pay.Type)
	}
}
