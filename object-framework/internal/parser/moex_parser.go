package parser

import (
	"encoding/xml"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/quantara/object-framework/internal/domain"
)

// MoexRecord represents the parsed MOEX XML record
type MoexRecord struct {
	XMLName xml.Name `xml:"record"`

	// Metadata
	Timestamp string `xml:"timestamp,attr"`
	Exchange  string `xml:"exchange,attr"`
	Market    string `xml:"market,attr"`
	System    string `xml:"system,attr"`
	Entity    string `xml:"entity,attr"`
	Interface string `xml:"interface,attr"`
	Table     string `xml:"table,attr"`

	// Trade fields
	Account              string `xml:"ACCOUNT,attr"`
	BankAccID            string `xml:"BANKACCID,attr"`
	BankID               string `xml:"BANKID,attr"`
	BuySell              string `xml:"BUYSELL,attr"`
	ClearingBankAccID    string `xml:"CLEARINGBANKACCID,attr"`
	ClearingCenterComm   string `xml:"CLEARINGCENTERCOMM,attr"`
	ClearingFirmID       string `xml:"CLEARINGFIRMID,attr"`
	ClearingTrdAccID     string `xml:"CLEARINGTRDACCID,attr"`
	Commission           string `xml:"COMMISSION,attr"`
	CPFirmID             string `xml:"CPFIRMID,attr"`
	CPOrderNo            string `xml:"CPORDERNO,attr"`
	CurrencyID           string `xml:"CURRENCYID,attr"`
	ExchangeComm         string `xml:"EXCHANGECOMM,attr"`
	FirmID               string `xml:"FIRMID,attr"`
	HiddenQtyOrder       string `xml:"HIDDENQTYORDER,attr"`
	MakerFlag            string `xml:"MAKERFLAG,attr"`
	Microseconds         string `xml:"MICROSECONDS,attr"`
	OrderNo              string `xml:"ORDERNO,attr"`
	Period               string `xml:"PERIOD,attr"`
	Price                string `xml:"PRICE,attr"`
	Quantity             string `xml:"QUANTITY,attr"`
	SecBoard             string `xml:"SECBOARD,attr"`
	SecCode              string `xml:"SECCODE,attr"`
	SettleCode           string `xml:"SETTLECODE,attr"`
	SettleDate           string `xml:"SETTLEDATE,attr"`
	SettleLiabilityFlag  string `xml:"SETTLELIABILITYFLAG,attr"`
	Status               string `xml:"STATUS,attr"`
	TradeDate            string `xml:"TRADEDATE,attr"`
	TradeNo              string `xml:"TRADENO,attr"`
	PelicanTrdID         string `xml:"PelicanTrdID,attr"`
	TradeTime            string `xml:"TRADETIME,attr"`
	TradeType            string `xml:"TRADETYPE,attr"`
	TradingSystemComm    string `xml:"TRADINGSYSTEMCOMM,attr"`
	UserID               string `xml:"USERID,attr"`
	Value                string `xml:"VALUE,attr"`

	// Securities info
	SecuritiesShortName  string `xml:"SECURITIES-SHORTNAME,attr"`
	SecuritiesFaceValue  string `xml:"SECURITIES-FACEVALUE,attr"`
	SecuritiesFaceUnit   string `xml:"SECURITIES-FACEUNIT,attr"`
	SecuritiesLotSize    string `xml:"SECURITIES-LOTSIZE,attr"`
	SecuritiesCurrencyID string `xml:"SECURITIES-CURRENCYID,attr"`

	// Instrument info
	InstrsInstrType string `xml:"INSTRS-INSTRTYPE,attr"`
	InstrsInstrFwd  string `xml:"INSTRS-INSTRFWD,attr"`

	// Trader info
	TraderSource     string `xml:"TRADER_SOURCE,attr"`
	TraderSigmaLogin string `xml:"TRADER_SIGMA_LOGIN,attr"`
	PapaAlgoID       string `xml:"PAPA-ALGOID,attr"`
	BrdBookName      string `xml:"BRD-BOOKNAME,attr"`
	BrdPlBaseCcy     string `xml:"BRD-PLBASECCY,attr"`
	BrdFolderCcy     string `xml:"BRD-FOLDERCCY,attr"`

	// Nested elements
	MxFlowID string `xml:"MxFlowID"`
}

// ParseMoexMessage parses MOEX XML message and returns MoexRecord
func ParseMoexMessage(data []byte) (*MoexRecord, error) {
	var record MoexRecord
	if err := xml.Unmarshal(data, &record); err != nil {
		return nil, fmt.Errorf("failed to parse MOEX XML: %w", err)
	}
	return &record, nil
}

// GetTradeNo returns the trade number (TRADENO)
func (r *MoexRecord) GetTradeNo() string {
	return r.TradeNo
}

// ToFxSpotForwardTrade converts MoexRecord to FxSpotForwardTrade domain object
// productGlobalID is the GlobalID of the parent FxSpotExchangeProduct
// tradeID and tradeGlobalID are generated IDs for this trade
func (r *MoexRecord) ToFxSpotForwardTrade(productGlobalID, tradeID, tradeGlobalID int64, version, revision int32) (*domain.FxSpotForwardTrade, error) {
	tradeDate, err := r.parseDate(r.TradeDate)
	if err != nil {
		return nil, fmt.Errorf("failed to parse TRADEDATE: %w", err)
	}

	tradeDateTime, err := r.parseDateTime(r.TradeDate, r.TradeTime)
	if err != nil {
		return nil, fmt.Errorf("failed to parse TRADEDATE+TRADETIME: %w", err)
	}

	settleDate, err := r.parseDate(r.SettleDate)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SETTLEDATE: %w", err)
	}

	quantity, err := r.parseFloat(r.Quantity)
	if err != nil {
		return nil, fmt.Errorf("failed to parse QUANTITY: %w", err)
	}

	value, err := r.parseFloat(r.Value)
	if err != nil {
		return nil, fmt.Errorf("failed to parse VALUE: %w", err)
	}

	price, err := r.parseFloat(r.Price)
	if err != nil {
		return nil, fmt.Errorf("failed to parse PRICE: %w", err)
	}

	side := r.normalizeSide(r.BuySell)

	return &domain.FxSpotForwardTrade{
		ID:                tradeID,
		GlobalID:          tradeGlobalID,
		ProductID:         productGlobalID,
		Version:           version,
		Revision:          revision,
		Status:            "live",
		DraftStatus:       "CONFIRMED",
		ActualFrom:        tradeDate,
		TradeDateTime:     tradeDateTime,
		TradeDate:         tradeDate,
		Side:              side,
		ExternalID:        r.TradeNo,
		BaseAmount:        quantity,
		NotionalAmount:    value,
		FxRate:            price,
		BaseValueDate:     settleDate,
		NotionalValueDate: settleDate,
		// Reference fields are null for now
		BaseCurrencyID:     nil,
		NotionalCurrencyID: nil,
		CounterpartyID:     nil,
	}, nil
}

// parseDate parses date string in format "2026-02-02"
func (r *MoexRecord) parseDate(dateStr string) (time.Time, error) {
	if dateStr == "" {
		return time.Time{}, fmt.Errorf("empty date string")
	}
	return time.Parse("2006-01-02", dateStr)
}

// parseDateTime parses date and time strings
func (r *MoexRecord) parseDateTime(dateStr, timeStr string) (time.Time, error) {
	if dateStr == "" || timeStr == "" {
		return time.Time{}, fmt.Errorf("empty date or time string")
	}
	combined := dateStr + " " + timeStr
	return time.Parse("2006-01-02 15:04:05", combined)
}

// parseFloat parses string to float64
func (r *MoexRecord) parseFloat(s string) (float64, error) {
	if s == "" {
		return 0, nil
	}
	// Remove any spaces and replace comma with dot
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, ",", ".")
	return strconv.ParseFloat(s, 64)
}

// normalizeSide converts BUYSELL value to standard side format
func (r *MoexRecord) normalizeSide(buySell string) string {
	switch strings.ToUpper(buySell) {
	case "B", "BUY":
		return "BUY"
	case "S", "SELL":
		return "SELL"
	default:
		return buySell
	}
}

// CreateCashflows creates cashflow pair for a trade
func CreateCashflows(trade *domain.FxSpotForwardTrade, receiveID, payID int64) []domain.TradeCashflow {
	cashflowReceive := domain.TradeCashflow{
		ID:                 receiveID,
		ParentID:           trade.ID,
		ConversionDate:     trade.BaseValueDate,
		ConversionRate:     trade.FxRate,
		ConversionSourceID: nil, // null for FXSPOT
		CounterpartyID:     trade.CounterpartyID,
		Direction:          domain.PaymentDirectionReceive,
		IsSettled:          false,
		OriginalAmount:     trade.NotionalAmount,
		OriginalCurrencyID: trade.BaseCurrencyID,
		PaymentAmount:      trade.NotionalAmount,
		PaymentCurrencyID:  trade.BaseCurrencyID,
		PaymentDate:        trade.BaseValueDate,
		Type:               domain.CashflowTypeReceivable,
	}

	cashflowPay := domain.TradeCashflow{
		ID:                 payID,
		ParentID:           trade.ID,
		ConversionDate:     trade.BaseValueDate,
		ConversionRate:     trade.FxRate,
		ConversionSourceID: nil, // null for FXSPOT
		CounterpartyID:     trade.CounterpartyID,
		Direction:          domain.PaymentDirectionPay,
		IsSettled:          false,
		OriginalAmount:     trade.NotionalAmount,
		OriginalCurrencyID: trade.BaseCurrencyID,
		PaymentAmount:      trade.NotionalAmount,
		PaymentCurrencyID:  trade.BaseCurrencyID,
		PaymentDate:        trade.BaseValueDate,
		Type:               domain.CashflowTypePayable,
	}

	return []domain.TradeCashflow{cashflowReceive, cashflowPay}
}
