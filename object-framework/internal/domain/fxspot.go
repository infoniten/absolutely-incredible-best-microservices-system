package domain

import "time"

// FxSpotExchangeProduct represents the exchange product entity
type FxSpotExchangeProduct struct {
	ID       int64 `json:"id"`
	GlobalID int64 `json:"globalId"`
	Version  int32 `json:"version"`
	Revision int32 `json:"revision"`
}

// FxSpotForwardTrade represents the trade entity
type FxSpotForwardTrade struct {
	ID                 int64     `json:"id"`
	GlobalID           int64     `json:"globalId"`
	ProductID          int64     `json:"productId"` // Reference to FxSpotExchangeProduct.GlobalID
	Version            int32     `json:"version"`
	Revision           int32     `json:"revision"`
	Status             string    `json:"status"`      // "live"
	DraftStatus        string    `json:"draftStatus"` // "CONFIRMED"
	ActualFrom         time.Time `json:"actualFrom"`  // TRADEDATE
	TradeDateTime      time.Time `json:"tradeDateTime"`
	TradeDate          time.Time `json:"tradeDate"`
	Side               string    `json:"side"`       // BUYSELL: B/S
	ExternalID         string    `json:"externalId"` // TRADENO
	BaseAmount         float64   `json:"baseAmount"` // QUANTITY
	NotionalAmount     float64   `json:"notionalAmount"` // VALUE
	FxRate             float64   `json:"fxRate"`         // PRICE
	BaseValueDate      time.Time `json:"baseValueDate"`      // SETTLEDATE
	NotionalValueDate  time.Time `json:"notionalValueDate"`  // SETTLEDATE
	LegalEntityID      *int64    `json:"legalEntityId,omitempty"`
	CounterpartyID     *int64    `json:"counterpartyId,omitempty"`
	SourceSystemID     *int64    `json:"sourceSystemId,omitempty"`
	PortfolioID        *int64    `json:"portfolioId,omitempty"`
	TraderID           *int64    `json:"traderId,omitempty"`
	AuthorizedByID     *int64    `json:"authorizedById,omitempty"`
	VenueID            *int64    `json:"venueId,omitempty"`
	BaseCurrencyID     *int64    `json:"baseCurrencyId,omitempty"`
	NotionalCurrencyID *int64    `json:"notionalCurrencyId,omitempty"`
	InstrumentID       *int64    `json:"instrumentId,omitempty"`
	Cashflows          []TradeCashflow `json:"cashflows,omitempty"`
}

// TradeCashflow represents a cashflow associated with a trade
type TradeCashflow struct {
	ID                 int64     `json:"id"`
	ParentID           int64     `json:"parentId"` // Reference to FxSpotForwardTrade.ID
	ConversionDate     time.Time `json:"conversionDate"`
	ConversionRate     float64   `json:"conversionRate"`
	ConversionSourceID *int64    `json:"conversionSourceId,omitempty"` // null for FXSPOT
	CounterpartyID     *int64    `json:"counterpartyId,omitempty"`
	Direction          PaymentDirection `json:"direction"`
	IsSettled          bool      `json:"isSettled"`
	OriginalAmount     float64   `json:"originalAmount"`
	OriginalCurrencyID *int64    `json:"originalCurrencyId,omitempty"`
	PaymentAmount      float64   `json:"paymentAmount"`
	PaymentCurrencyID  *int64    `json:"paymentCurrencyId,omitempty"`
	PaymentDate        time.Time `json:"paymentDate"`
	Type               CashflowType `json:"type"`
}

type PaymentDirection string

const (
	PaymentDirectionReceive PaymentDirection = "RECEIVE"
	PaymentDirectionPay     PaymentDirection = "PAY"
)

type CashflowType string

const (
	CashflowTypeReceivable CashflowType = "RECEIVABLE"
	CashflowTypePayable    CashflowType = "PAYABLE"
)

// ObjectTypes for transaction service
const (
	ObjectTypeFxSpotExchangeProduct = "FxSpotExchangeProduct"
	ObjectTypeFxSpotForwardTrade    = "FxSpotForwardTrade"
	ObjectTypeTradeCashflow         = "TradeCashflow"
)
