package processor

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/quantara/object-framework/internal/client"
	"github.com/quantara/object-framework/internal/domain"
	"github.com/quantara/object-framework/internal/parser"
	"github.com/quantara/object-framework/internal/repository"
)

// FxSpotProcessor handles the business logic for FX Spot trade processing
type FxSpotProcessor struct {
	rawMsgRepo      *repository.RawMessageRepository
	globalIDRepo    *repository.GlobalIDMappingRepository
	idClient        *client.IDClient
	globalIDClient  *client.GlobalIDClient
	lockClient      *client.LockClient
	searchClient    *client.SearchClient
	txClient        *client.TransactionClient
	lockTTLMs       int64
}

// NewFxSpotProcessor creates a new FxSpotProcessor
func NewFxSpotProcessor(
	rawMsgRepo *repository.RawMessageRepository,
	globalIDRepo *repository.GlobalIDMappingRepository,
	idClient *client.IDClient,
	globalIDClient *client.GlobalIDClient,
	lockClient *client.LockClient,
	searchClient *client.SearchClient,
	txClient *client.TransactionClient,
	lockTTLMs int64,
) *FxSpotProcessor {
	return &FxSpotProcessor{
		rawMsgRepo:     rawMsgRepo,
		globalIDRepo:   globalIDRepo,
		idClient:       idClient,
		globalIDClient: globalIDClient,
		lockClient:     lockClient,
		searchClient:   searchClient,
		txClient:       txClient,
		lockTTLMs:      lockTTLMs,
	}
}

// ProcessResult contains the result of message processing
type ProcessResult struct {
	Success   bool
	MessageID string
	GlobalID  int64
	TradeNo   string
	Error     error
}

// Process processes a single MOEX message
func (p *FxSpotProcessor) Process(ctx context.Context, rawMsg *domain.RawMessageDto) *ProcessResult {
	result := &ProcessResult{
		MessageID: rawMsg.MessageID,
	}

	// Step 1: Parse XML
	moexRecord, err := parser.ParseMoexMessage(rawMsg.Value)
	if err != nil {
		result.Error = fmt.Errorf("failed to parse MOEX message: %w", err)
		p.updateRawMessageStatus(ctx, rawMsg.ID, domain.RawMessageStatusFailed, result.Error.Error())
		return result
	}

	tradeNo := moexRecord.GetTradeNo()
	result.TradeNo = tradeNo
	log.Printf("Processing trade TRADENO=%s", tradeNo)

	// Step 2: Resolve GlobalID for this trade
	productGlobalID, isNewProduct, err := p.resolveGlobalID(ctx, tradeNo)
	if err != nil {
		result.Error = fmt.Errorf("failed to resolve GlobalID: %w", err)
		p.updateRawMessageStatus(ctx, rawMsg.ID, domain.RawMessageStatusFailed, result.Error.Error())
		return result
	}
	result.GlobalID = productGlobalID
	log.Printf("Resolved GlobalID=%d, isNew=%v", productGlobalID, isNewProduct)

	// Step 3: Acquire lock
	lockResult, err := p.lockClient.LockWithTTL(ctx, productGlobalID, p.lockTTLMs)
	if err != nil {
		result.Error = fmt.Errorf("failed to acquire lock: %w", err)
		p.updateRawMessageStatus(ctx, rawMsg.ID, domain.RawMessageStatusFailed, result.Error.Error())
		return result
	}
	if !lockResult.Success {
		result.Error = fmt.Errorf("lock not acquired: %s", lockResult.Error)
		p.updateRawMessageStatus(ctx, rawMsg.ID, domain.RawMessageStatusFailed, result.Error.Error())
		return result
	}
	lockToken := lockResult.Token
	log.Printf("Lock acquired, token=%s", lockToken)

	// Ensure lock is released
	defer func() {
		if err := p.lockClient.Unlock(ctx, productGlobalID, lockToken); err != nil {
			log.Printf("Warning: failed to release lock: %v", err)
		}
	}()

	// Step 4: Process business logic
	err = p.processBusinessLogic(ctx, moexRecord, productGlobalID, isNewProduct, lockToken)
	if err != nil {
		result.Error = fmt.Errorf("business logic failed: %w", err)
		p.updateRawMessageStatus(ctx, rawMsg.ID, domain.RawMessageStatusFailed, result.Error.Error())
		return result
	}

	// Step 5: Update raw message status to SAVED
	if err := p.updateRawMessageStatus(ctx, rawMsg.ID, domain.RawMessageStatusSaved, ""); err != nil {
		log.Printf("Warning: failed to update raw message status: %v", err)
	}

	result.Success = true
	log.Printf("Successfully processed trade TRADENO=%s, GlobalID=%d", tradeNo, productGlobalID)
	return result
}

// resolveGlobalID resolves or creates the GlobalID for a trade
func (p *FxSpotProcessor) resolveGlobalID(ctx context.Context, tradeNo string) (int64, bool, error) {
	// Try to find existing mapping first
	existing, err := p.globalIDRepo.FindByExternalID(
		ctx,
		tradeNo,
		domain.SourceMOEX,
		domain.SourceObjectTypeFXSPOT,
	)
	if err != nil {
		return 0, false, fmt.Errorf("failed to find existing mapping: %w", err)
	}
	if existing != nil {
		return existing.GlobalID, false, nil
	}

	// No existing mapping - need to create one with real IDs
	mappingID, err := p.idClient.GetID(ctx)
	if err != nil {
		return 0, false, fmt.Errorf("failed to get ID for mapping: %w", err)
	}

	globalID, err := p.globalIDClient.GetGlobalID(ctx)
	if err != nil {
		return 0, false, fmt.Errorf("failed to get GlobalID: %w", err)
	}

	// Create the mapping (handles race condition internally)
	mapping, created, err := p.globalIDRepo.GetOrCreate(
		ctx,
		tradeNo,
		domain.SourceMOEX,
		domain.SourceObjectTypeFXSPOT,
		mappingID,
		globalID,
	)
	if err != nil {
		return 0, false, err
	}

	return mapping.GlobalID, created, nil
}

// processBusinessLogic handles the main business logic
func (p *FxSpotProcessor) processBusinessLogic(ctx context.Context, moexRecord *parser.MoexRecord, productGlobalID int64, isNewProduct bool, lockToken string) error {
	// Get current date for actual_date
	actualDate := time.Now().Format("2006-01-02")

	// Step 1: Get or create Product
	var product *domain.FxSpotExchangeProduct
	var existingTrade map[string]interface{}

	if !isNewProduct {
		// Try to find existing product
		productData, err := p.searchClient.GetObjectByGlobalID(
			ctx,
			domain.ObjectTypeFxSpotExchangeProduct,
			productGlobalID,
			"CONFIRMED",
			actualDate,
		)
		if err != nil {
			return fmt.Errorf("failed to search for product: %w", err)
		}

		if productData != nil {
			// Product exists - increment version/revision
			product = p.mapToProduct(productData)
			product.Version++
			product.Revision++

			// Generate new ID for updated product
			newID, err := p.idClient.GetID(ctx)
			if err != nil {
				return fmt.Errorf("failed to get new ID for product: %w", err)
			}
			product.ID = newID

			// Try to find existing trade
			existingTrade, _ = p.findExistingTrade(ctx, productGlobalID, actualDate)
		}
	}

	// If product doesn't exist, create new one
	if product == nil {
		productID, err := p.idClient.GetID(ctx)
		if err != nil {
			return fmt.Errorf("failed to get ID for new product: %w", err)
		}

		product = &domain.FxSpotExchangeProduct{
			ID:       productID,
			GlobalID: productGlobalID,
			Version:  1,
			Revision: 1,
		}
	}

	// Step 2: Create or update Trade
	var trade *domain.FxSpotForwardTrade
	var tradeVersion, tradeRevision int32 = 1, 1

	if existingTrade != nil {
		// Update existing trade
		tradeVersion = int32(getIntFromMap(existingTrade, "version")) + 1
		tradeRevision = int32(getIntFromMap(existingTrade, "revision")) + 1
	}

	// Generate trade IDs
	tradeID, err := p.idClient.GetID(ctx)
	if err != nil {
		return fmt.Errorf("failed to get ID for trade: %w", err)
	}

	tradeGlobalID, err := p.globalIDClient.GetGlobalID(ctx)
	if err != nil {
		return fmt.Errorf("failed to get GlobalID for trade: %w", err)
	}

	// Convert MOEX record to trade
	trade, err = moexRecord.ToFxSpotForwardTrade(productGlobalID, tradeID, tradeGlobalID, tradeVersion, tradeRevision)
	if err != nil {
		return fmt.Errorf("failed to convert MOEX record to trade: %w", err)
	}

	// Step 3: Create Cashflows
	cashflowIDs, err := p.idClient.GetBatchID(ctx, 2)
	if err != nil {
		return fmt.Errorf("failed to get IDs for cashflows: %w", err)
	}

	cashflows := parser.CreateCashflows(trade, cashflowIDs[0], cashflowIDs[1])
	trade.Cashflows = cashflows

	// Step 4: Save everything in a transaction
	return p.saveInTransaction(ctx, product, trade, lockToken)
}

// findExistingTrade searches for an existing trade by product GlobalID
func (p *FxSpotProcessor) findExistingTrade(ctx context.Context, productGlobalID int64, actualDate string) (map[string]interface{}, error) {
	// Search for trade with this productID
	tradeData, err := p.searchClient.GetObjectByGlobalID(
		ctx,
		domain.ObjectTypeFxSpotForwardTrade,
		productGlobalID, // Using productGlobalID as reference
		"CONFIRMED",
		actualDate,
	)
	if err != nil {
		return nil, err
	}
	return tradeData, nil
}

// saveInTransaction saves product, trade and cashflows in a single transaction
func (p *FxSpotProcessor) saveInTransaction(ctx context.Context, product *domain.FxSpotExchangeProduct, trade *domain.FxSpotForwardTrade, lockToken string) error {
	// Begin transaction
	txID, err := p.txClient.BeginTransaction(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Rollback on error
	committed := false
	defer func() {
		if !committed {
			if rbErr := p.txClient.Rollback(ctx, txID); rbErr != nil {
				log.Printf("Warning: failed to rollback transaction: %v", rbErr)
			}
		}
	}()

	// Save Product
	actualFrom := trade.ActualFrom.Format("2006-01-02")
	draftStatus := "CONFIRMED"
	version := product.Version

	err = p.txClient.SaveObject(ctx, txID, &client.ObjectHeaders{
		ID:          product.ID,
		GlobalID:    product.GlobalID,
		ObjectType:  domain.ObjectTypeFxSpotExchangeProduct,
		LockID:      lockToken,
		Revision:    product.Revision,
		Version:     &version,
		ActualFrom:  &actualFrom,
		DraftStatus: &draftStatus,
	}, product)
	if err != nil {
		return fmt.Errorf("failed to save product: %w", err)
	}

	// Save Trade
	tradeVersion := trade.Version
	err = p.txClient.SaveObject(ctx, txID, &client.ObjectHeaders{
		ID:          trade.ID,
		GlobalID:    trade.GlobalID,
		ObjectType:  domain.ObjectTypeFxSpotForwardTrade,
		LockID:      lockToken,
		Revision:    trade.Revision,
		Version:     &tradeVersion,
		ActualFrom:  &actualFrom,
		DraftStatus: &draftStatus,
	}, trade)
	if err != nil {
		return fmt.Errorf("failed to save trade: %w", err)
	}

	// Save Cashflows (embedded in trade, using parent_id)
	for _, cf := range trade.Cashflows {
		parentID := trade.ID
		err = p.txClient.SaveObject(ctx, txID, &client.ObjectHeaders{
			ID:          cf.ID,
			GlobalID:    0, // Embedded objects don't have GlobalID
			ObjectType:  domain.ObjectTypeTradeCashflow,
			LockID:      lockToken,
			Revision:    1,
			ParentID:    &parentID,
			DraftStatus: &draftStatus,
		}, cf)
		if err != nil {
			return fmt.Errorf("failed to save cashflow: %w", err)
		}
	}

	// Commit
	result, err := p.txClient.Commit(ctx, txID)
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	if !result.Success {
		return fmt.Errorf("commit failed: %s", result.Error)
	}

	committed = true
	log.Printf("Transaction committed, saved %d objects", result.ObjectsSaved)
	return nil
}

func (p *FxSpotProcessor) updateRawMessageStatus(ctx context.Context, id int64, status domain.RawMessageStatus, errorMsg string) error {
	return p.rawMsgRepo.UpdateStatus(ctx, id, status, errorMsg)
}

// mapToProduct converts search result to FxSpotExchangeProduct
func (p *FxSpotProcessor) mapToProduct(data map[string]interface{}) *domain.FxSpotExchangeProduct {
	return &domain.FxSpotExchangeProduct{
		ID:       int64(getIntFromMap(data, "id")),
		GlobalID: int64(getIntFromMap(data, "globalId")),
		Version:  int32(getIntFromMap(data, "version")),
		Revision: int32(getIntFromMap(data, "revision")),
	}
}

// getIntFromMap safely extracts an int from a map
func getIntFromMap(m map[string]interface{}, key string) int {
	if v, ok := m[key]; ok {
		switch val := v.(type) {
		case float64:
			return int(val)
		case int:
			return val
		case int64:
			return int(val)
		}
	}
	return 0
}
