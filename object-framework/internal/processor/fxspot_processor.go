package processor

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/quantara/object-framework/internal/client"
	"github.com/quantara/object-framework/internal/domain"
	"github.com/quantara/object-framework/internal/parser"
	"github.com/quantara/object-framework/internal/repository"
)

// FxSpotProcessor handles the business logic for FX Spot trade processing
type FxSpotProcessor struct {
	rawMsgRepo     *repository.RawMessageRepository
	globalIDRepo   *repository.GlobalIDMappingRepository
	idClient       *client.IDClient
	globalIDClient *client.GlobalIDClient
	lockClient     *client.LockClient
	searchClient   *client.SearchClient
	txClient       *client.TransactionClient
	lockTTLMs      int64
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

	// Step 2: Resolve GlobalIDs for product and trade IN PARALLEL
	var productGlobalID, tradeGlobalID int64
	var isNewProduct, isNewTrade bool
	var resolveErr error

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		var err error
		productGlobalID, isNewProduct, err = p.resolveGlobalID(ctx, tradeNo, domain.SourceObjectTypeFXSPOT)
		if err != nil {
			resolveErr = fmt.Errorf("failed to resolve product GlobalID: %w", err)
		}
	}()

	go func() {
		defer wg.Done()
		var err error
		tradeGlobalID, isNewTrade, err = p.resolveGlobalID(ctx, tradeNo, domain.SourceObjectTypeFXSPOTTrade)
		if err != nil {
			resolveErr = fmt.Errorf("failed to resolve trade GlobalID: %w", err)
		}
	}()

	wg.Wait()

	if resolveErr != nil {
		result.Error = resolveErr
		p.updateRawMessageStatus(ctx, rawMsg.ID, domain.RawMessageStatusFailed, result.Error.Error())
		return result
	}

	result.GlobalID = productGlobalID

	// Step 3: Acquire lock with retry
	var lockToken string
	maxRetries := 5
	baseBackoff := 10 * time.Millisecond

	for attempt := 0; attempt < maxRetries; attempt++ {
		lockResult, err := p.lockClient.LockWithTTL(ctx, productGlobalID, p.lockTTLMs)
		if err != nil {
			// Check if it's a transient error (contention)
			if attempt < maxRetries-1 {
				backoff := baseBackoff * time.Duration(1<<attempt) // exponential backoff: 10ms, 20ms, 40ms, 80ms, 160ms
				time.Sleep(backoff)
				continue
			}
			result.Error = fmt.Errorf("failed to acquire lock after %d attempts: %w", maxRetries, err)
			p.updateRawMessageStatus(ctx, rawMsg.ID, domain.RawMessageStatusFailed, result.Error.Error())
			return result
		}
		if !lockResult.Success {
			// Lock held by another process - retry with backoff
			if attempt < maxRetries-1 {
				backoff := baseBackoff * time.Duration(1<<attempt)
				time.Sleep(backoff)
				continue
			}
			result.Error = fmt.Errorf("lock not acquired after %d attempts: %s", maxRetries, lockResult.Error)
			p.updateRawMessageStatus(ctx, rawMsg.ID, domain.RawMessageStatusFailed, result.Error.Error())
			return result
		}
		lockToken = lockResult.Token
		break
	}

	// Ensure lock is released
	defer func() {
		if err := p.lockClient.Unlock(ctx, productGlobalID, lockToken); err != nil {
			log.Printf("Warning: failed to release lock: %v", err)
		}
	}()

	// Step 4: Process business logic with optimizations
	err = p.processBusinessLogicOptimized(ctx, moexRecord, productGlobalID, tradeGlobalID, isNewProduct, isNewTrade, lockToken)
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
	return result
}

// resolveGlobalID resolves or creates the GlobalID for an object type
func (p *FxSpotProcessor) resolveGlobalID(ctx context.Context, tradeNo string, objectType string) (int64, bool, error) {
	// Try to find existing mapping first
	existing, err := p.globalIDRepo.FindByExternalID(ctx, tradeNo, domain.SourceMOEX, objectType)
	if err != nil {
		return 0, false, fmt.Errorf("failed to find existing mapping: %w", err)
	}
	if existing != nil {
		return existing.GlobalID, false, nil
	}

	// No existing mapping - need to create one with real IDs
	// Get both IDs in parallel
	var mappingID, globalID int64
	var idErr, globalIDErr error

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		mappingID, idErr = p.idClient.GetID(ctx)
	}()

	go func() {
		defer wg.Done()
		globalID, globalIDErr = p.globalIDClient.GetGlobalID(ctx)
	}()

	wg.Wait()

	if idErr != nil {
		return 0, false, fmt.Errorf("failed to get ID for mapping: %w", idErr)
	}
	if globalIDErr != nil {
		return 0, false, fmt.Errorf("failed to get GlobalID: %w", globalIDErr)
	}

	// Create the mapping (handles race condition internally)
	mapping, created, err := p.globalIDRepo.GetOrCreate(ctx, tradeNo, domain.SourceMOEX, objectType, mappingID, globalID)
	if err != nil {
		return 0, false, err
	}

	return mapping.GlobalID, created, nil
}

// processBusinessLogicOptimized handles the main business logic with all optimizations
func (p *FxSpotProcessor) processBusinessLogicOptimized(ctx context.Context, moexRecord *parser.MoexRecord, productGlobalID, tradeGlobalID int64, isNewProduct, isNewTrade bool, lockToken string) error {
	actualDate := time.Now().Format("2006-01-02")

	// Step 1: Batch ID generation - get all IDs we need in one call
	// We need: productID, tradeID, 2 cashflowIDs = 4 IDs max
	allIDs, err := p.idClient.GetBatchID(ctx, 4)
	if err != nil {
		return fmt.Errorf("failed to get batch IDs: %w", err)
	}

	// Step 2: Search for existing product and trade IN PARALLEL (if needed)
	var existingProduct, existingTrade map[string]interface{}
	var productSearchErr, tradeSearchErr error

	if !isNewProduct || !isNewTrade {
		var wg sync.WaitGroup

		if !isNewProduct {
			wg.Add(1)
			go func() {
				defer wg.Done()
				var err error
				existingProduct, err = p.searchClient.GetObjectByGlobalID(
					ctx,
					domain.ObjectTypeFxSpotExchangeProduct,
					productGlobalID,
					"CONFIRMED",
					actualDate,
				)
				if err != nil && !client.IsNotFoundError(err) {
					// Only set error if it's NOT a NotFound error
					// NotFound is expected during initial load (race condition)
					productSearchErr = fmt.Errorf("failed to search for product: %w", err)
				}
				// If NotFound - existingProduct stays nil, treat as new
			}()
		}

		if !isNewTrade {
			wg.Add(1)
			go func() {
				defer wg.Done()
				var err error
				existingTrade, err = p.searchClient.GetObjectByGlobalID(
					ctx,
					domain.ObjectTypeFxSpotForwardTrade,
					tradeGlobalID,
					"CONFIRMED",
					actualDate,
				)
				if err != nil && !client.IsNotFoundError(err) {
					// Only set error if it's NOT a NotFound error
					// NotFound is expected during initial load (race condition)
					tradeSearchErr = fmt.Errorf("failed to search for trade: %w", err)
				}
				// If NotFound - existingTrade stays nil, treat as new
			}()
		}

		wg.Wait()

		if productSearchErr != nil {
			return productSearchErr
		}
		if tradeSearchErr != nil {
			return tradeSearchErr
		}
	}

	// Step 3: Build Product
	var product *domain.FxSpotExchangeProduct
	if existingProduct != nil {
		product = p.mapToProduct(existingProduct)
		product.Version++
		product.Revision++
		product.ID = allIDs[0]
	} else {
		product = &domain.FxSpotExchangeProduct{
			ID:       allIDs[0],
			GlobalID: productGlobalID,
			Version:  1,
			Revision: 1,
		}
	}

	// Step 4: Build Trade
	var tradeVersion, tradeRevision int32 = 1, 1
	if existingTrade != nil {
		tradeVersion = int32(getIntFromMap(existingTrade, "version")) + 1
		tradeRevision = int32(getIntFromMap(existingTrade, "revision")) + 1
	}

	trade, err := moexRecord.ToFxSpotForwardTrade(productGlobalID, allIDs[1], tradeGlobalID, tradeVersion, tradeRevision)
	if err != nil {
		return fmt.Errorf("failed to convert MOEX record to trade: %w", err)
	}

	// Step 5: Build Cashflows
	cashflows := parser.CreateCashflows(trade, allIDs[2], allIDs[3])
	trade.Cashflows = cashflows

	// Step 6: Save everything using streaming transaction
	return p.saveWithStreaming(ctx, product, trade, lockToken)
}

// saveWithStreaming saves all objects using gRPC streaming for better performance
func (p *FxSpotProcessor) saveWithStreaming(ctx context.Context, product *domain.FxSpotExchangeProduct, trade *domain.FxSpotForwardTrade, lockToken string) error {
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

	// Prepare common values
	actualFrom := trade.ActualFrom.Format("2006-01-02")
	draftStatus := "CONFIRMED"
	version := product.Version

	// Build all save requests
	saveRequests := make([]*client.SaveRequest, 0, 4)

	// Product
	saveRequests = append(saveRequests, &client.SaveRequest{
		TxID: txID,
		Headers: &client.ObjectHeaders{
			ID:          product.ID,
			GlobalID:    product.GlobalID,
			ObjectType:  domain.ObjectTypeFxSpotExchangeProduct,
			LockID:      lockToken,
			Revision:    product.Revision,
			Version:     &version,
			ActualFrom:  &actualFrom,
			DraftStatus: &draftStatus,
		},
		Payload: product,
	})

	// Trade
	tradeVersion := trade.Version
	saveRequests = append(saveRequests, &client.SaveRequest{
		TxID: txID,
		Headers: &client.ObjectHeaders{
			ID:          trade.ID,
			GlobalID:    trade.GlobalID,
			ObjectType:  domain.ObjectTypeFxSpotForwardTrade,
			LockID:      lockToken,
			Revision:    trade.Revision,
			Version:     &tradeVersion,
			ActualFrom:  &actualFrom,
			DraftStatus: &draftStatus,
		},
		Payload: trade,
	})

	// Cashflows
	for _, cf := range trade.Cashflows {
		parentID := trade.ID
		saveRequests = append(saveRequests, &client.SaveRequest{
			TxID: txID,
			Headers: &client.ObjectHeaders{
				ID:          cf.ID,
				GlobalID:    0,
				ObjectType:  domain.ObjectTypeTradeCashflow,
				LockID:      lockToken,
				Revision:    1,
				ParentID:    &parentID,
				DraftStatus: &draftStatus,
			},
			Payload: cf,
		})
	}

	// Save all objects using streaming
	if err := p.txClient.SaveObjectsStream(ctx, saveRequests); err != nil {
		return fmt.Errorf("failed to save objects: %w", err)
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
