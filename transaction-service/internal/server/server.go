package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/quantara/transaction-service/internal/config"
	"github.com/quantara/transaction-service/internal/metamodel"
	"github.com/quantara/transaction-service/internal/redis"
	pb "github.com/quantara/transaction-service/proto"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// noopTracer is used when telemetry is not configured
var noopTracer = noop.NewTracerProvider().Tracer("")

// startSpan safely starts a span if tracer is available
func (s *Server) startSpan(ctx context.Context, name string) (context.Context, trace.Span) {
	if s.tracer != nil {
		return s.tracer.Start(ctx, name)
	}
	return noopTracer.Start(ctx, name)
}

// Server implements the TransactionService gRPC server
type Server struct {
	pb.UnimplementedTransactionServiceServer
	cfg       *config.Config
	redis     *redis.Client
	metamodel *metamodel.Cache
	db        *sql.DB
	tracer    trace.Tracer
	metrics   *transactionMetrics
}

// NewServer creates a new transaction service server
func NewServer(
	cfg *config.Config,
	redisClient *redis.Client,
	metamodelCache *metamodel.Cache,
	db *sql.DB,
	tracer trace.Tracer,
) *Server {
	return &Server{
		cfg:       cfg,
		redis:     redisClient,
		metamodel: metamodelCache,
		db:        db,
		tracer:    tracer,
		metrics:   newTransactionMetrics(),
	}
}

// BeginTransaction creates a new transaction
func (s *Server) BeginTransaction(ctx context.Context, req *pb.BeginTxRequest) (*pb.BeginTxResponse, error) {
	ctx, span := s.startSpan(ctx, "BeginTransaction")
	defer span.End()

	txID := uuid.New().String()

	if err := s.redis.CreateTransaction(ctx, txID); err != nil {
		return nil, fmt.Errorf("failed to create transaction: %w", err)
	}
	s.metrics.incBegin(ctx)

	log.Printf("Transaction created: %s", txID)

	return &pb.BeginTxResponse{
		TransactionId: txID,
	}, nil
}

// Save adds an object to the transaction
func (s *Server) Save(ctx context.Context, req *pb.SaveRequest) (*pb.SaveResponse, error) {
	ctx, span := s.startSpan(ctx, "Save")
	defer span.End()

	fail := func(message string) (*pb.SaveResponse, error) {
		s.metrics.incSaveError(ctx)
		return &pb.SaveResponse{Success: false, Error: message}, nil
	}

	// Validate transaction
	status, err := s.redis.GetTransactionStatus(ctx, req.TransactionId)
	if err != nil {
		return fail(err.Error())
	}
	if status != redis.TxStatusActive {
		return fail(fmt.Sprintf("transaction is not active: %s", status))
	}

	// Determine algorithm ID
	algorithmID := req.AlgorithmId
	if algorithmID == nil || *algorithmID == 0 {
		algID, err := s.metamodel.GetAlgorithmID(req.Headers.ObjectType)
		if err != nil {
			return fail(err.Error())
		}
		algorithmID = &algID
	}

	// Convert headers to JSON
	headersJSON, err := json.Marshal(req.Headers)
	if err != nil {
		return fail(fmt.Sprintf("failed to marshal headers: %v", err))
	}

	// Store in Redis
	obj := &redis.StoredObject{
		Headers: headersJSON,
		Payload: req.Payload,
	}

	if err := s.redis.AddObject(ctx, req.TransactionId, *algorithmID, req.Headers.ObjectType, obj); err != nil {
		return fail(err.Error())
	}
	s.metrics.incSave(ctx)

	return &pb.SaveResponse{Success: true}, nil
}

// SaveStream handles streaming saves
func (s *Server) SaveStream(stream pb.TransactionService_SaveStreamServer) error {
	ctx := stream.Context()
	_, span := s.startSpan(ctx, "SaveStream")
	defer span.End()

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		resp, err := s.Save(ctx, req)
		if err != nil {
			return err
		}

		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

// Commit commits all objects in the transaction to the database
func (s *Server) Commit(ctx context.Context, req *pb.CommitRequest) (*pb.CommitResponse, error) {
	ctx, span := s.startSpan(ctx, "Commit")
	defer span.End()
	startedAt := time.Now()
	defer s.metrics.observeCommitDuration(ctx, time.Since(startedAt))

	fail := func(message string) (*pb.CommitResponse, error) {
		s.metrics.incCommitError(ctx)
		return &pb.CommitResponse{Success: false, Error: message}, nil
	}

	// Validate transaction
	status, err := s.redis.GetTransactionStatus(ctx, req.TransactionId)
	if err != nil {
		return fail(err.Error())
	}
	if status != redis.TxStatusActive {
		return fail(fmt.Sprintf("transaction is not active: %s", status))
	}

	// Set status to COMMITTING
	if err := s.redis.SetTransactionStatus(ctx, req.TransactionId, redis.TxStatusCommitting); err != nil {
		return fail(err.Error())
	}

	// Get all object keys
	getTypesStart := time.Now()
	keys, err := s.redis.GetObjectKeys(ctx, req.TransactionId)
	s.metrics.observeCommitDurationByType(ctx, "getTypesRedis", time.Since(getTypesStart))
	if err != nil {
		s.redis.SetTransactionStatus(ctx, req.TransactionId, redis.TxStatusActive)
		return fail(err.Error())
	}

	if len(keys) == 0 {
		// No objects to commit
		s.redis.DeleteTransaction(ctx, req.TransactionId)
		s.metrics.incCommit(ctx)
		return &pb.CommitResponse{Success: true, ObjectsSaved: 0}, nil
	}

	// Pre-fetch all objects from Redis in PARALLEL (before SQL transaction)
	prefetchStart := time.Now()
	type prefetchedData struct {
		key         string
		algorithmID uint32
		className   string
		objects     []*redis.StoredObject
		err         error
	}

	prefetchChan := make(chan prefetchedData, len(keys))
	var wg sync.WaitGroup

	for _, key := range keys {
		wg.Add(1)
		go func(k string) {
			defer wg.Done()
			algorithmID, className := parseObjectKey(k)
			if algorithmID == 0 {
				return
			}

			// Fetch all objects for this key
			var allObjects []*redis.StoredObject
			var offset int64 = 0
			chunkSize := int64(s.cfg.CommitChunkSize)

			for {
				objects, err := s.redis.GetObjects(ctx, k, offset, chunkSize)
				if err != nil {
					prefetchChan <- prefetchedData{key: k, err: err}
					return
				}
				if len(objects) == 0 {
					break
				}
				allObjects = append(allObjects, objects...)
				if int64(len(objects)) < chunkSize {
					break
				}
				offset += chunkSize
			}

			prefetchChan <- prefetchedData{
				key:         k,
				algorithmID: algorithmID,
				className:   className,
				objects:     allObjects,
			}
		}(key)
	}

	// Wait for all prefetch goroutines
	wg.Wait()
	close(prefetchChan)

	// Collect prefetched data
	var prefetchedItems []prefetchedData
	for item := range prefetchChan {
		if item.err != nil {
			s.redis.SetTransactionStatus(ctx, req.TransactionId, redis.TxStatusActive)
			return fail(fmt.Sprintf("failed to prefetch objects: %v", item.err))
		}
		if len(item.objects) > 0 {
			prefetchedItems = append(prefetchedItems, item)
		}
	}
	s.metrics.observeCommitDurationByType(ctx, "prefetchRedis", time.Since(prefetchStart))

	// Begin SQL transaction
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		s.redis.SetTransactionStatus(ctx, req.TransactionId, redis.TxStatusActive)
		return fail(fmt.Sprintf("failed to begin SQL transaction: %v", err))
	}

	var totalSaved int32
	var results []*pb.ObjectResult

	dbAllStart := time.Now()
	// Process prefetched data (SQL writes must be sequential within transaction)
	for _, item := range prefetchedItems {
		typeStart := time.Now()

		// Execute batch save based on algorithm
		saved, objResults, err := s.executeBatchSave(ctx, tx, item.algorithmID, item.className, item.objects)
		if err != nil {
			tx.Rollback()
			s.redis.SetTransactionStatus(ctx, req.TransactionId, redis.TxStatusActive)
			return fail(err.Error())
		}

		totalSaved += saved
		results = append(results, objResults...)
		s.metrics.observeCommitDurationByType(ctx, item.className, time.Since(typeStart))
	}
	s.metrics.observeCommitDurationByType(ctx, "DBALL", time.Since(dbAllStart))

	// Commit SQL transaction
	if err := tx.Commit(); err != nil {
		s.redis.SetTransactionStatus(ctx, req.TransactionId, redis.TxStatusActive)
		return fail(fmt.Sprintf("failed to commit SQL transaction: %v", err))
	}

	// Clean up Redis
	s.redis.DeleteTransaction(ctx, req.TransactionId)
	s.metrics.incCommit(ctx)

	log.Printf("Transaction committed: %s, objects saved: %d", req.TransactionId, totalSaved)

	return &pb.CommitResponse{
		Success:      true,
		ObjectsSaved: totalSaved,
		Results:      results,
	}, nil
}

// Rollback cancels the transaction
func (s *Server) Rollback(ctx context.Context, req *pb.RollbackRequest) (*pb.RollbackResponse, error) {
	ctx, span := s.startSpan(ctx, "Rollback")
	defer span.End()

	fail := func() (*pb.RollbackResponse, error) {
		s.metrics.incRollbackError(ctx)
		return &pb.RollbackResponse{Success: false}, nil
	}

	// Set status to ROLLED_BACK
	if err := s.redis.SetTransactionStatus(ctx, req.TransactionId, redis.TxStatusRolledBack); err != nil {
		log.Printf("Warning: failed to set rollback status: %v", err)
	}

	// Delete all transaction data
	if err := s.redis.DeleteTransaction(ctx, req.TransactionId); err != nil {
		return fail()
	}

	log.Printf("Transaction rolled back: %s", req.TransactionId)
	s.metrics.incRollback(ctx)

	return &pb.RollbackResponse{Success: true}, nil
}

// ============================================================================
// Helper functions
// ============================================================================

// parseObjectKey extracts algorithm ID and class name from a Redis key
func parseObjectKey(key string) (uint32, string) {
	re := regexp.MustCompile(`\{[^}]+\}:alg:(\d+):class:(.+)`)
	matches := re.FindStringSubmatch(key)
	if len(matches) != 3 {
		return 0, ""
	}

	algID, err := strconv.ParseUint(matches[1], 10, 32)
	if err != nil {
		return 0, ""
	}

	return uint32(algID), matches[2]
}

// executeBatchSave executes batch save based on algorithm
func (s *Server) executeBatchSave(ctx context.Context, tx *sql.Tx, algorithmID uint32, className string, objects []*redis.StoredObject) (int32, []*pb.ObjectResult, error) {
	switch algorithmID {
	case metamodel.AlgorithmEmbedded:
		return s.saveEmbedded(ctx, tx, className, objects)
	case metamodel.AlgorithmRevisioned:
		return s.saveRevisioned(ctx, tx, className, objects)
	case metamodel.AlgorithmDraftableDateBounded:
		return s.saveDraftableDateBounded(ctx, tx, className, objects)
	default:
		return 0, nil, fmt.Errorf("unknown algorithm ID: %d", algorithmID)
	}
}

// ObjectRequest represents a parsed object for SQL operations
type ObjectRequest struct {
	ID          int64
	GlobalID    int64
	ObjectType  string
	LockID      string
	Revision    int32
	Version     *int32
	ActualFrom  *string
	DraftStatus *string
	ParentID    *int64
	Payload     json.RawMessage
	PayloadMap  map[string]interface{} // Parsed JSON for index field extraction
}

// parseStoredObject converts a StoredObject to ObjectRequest
func parseStoredObject(obj *redis.StoredObject) (*ObjectRequest, error) {
	var headers pb.ObjectHeaders
	if err := json.Unmarshal(obj.Headers, &headers); err != nil {
		return nil, fmt.Errorf("failed to unmarshal headers: %w", err)
	}

	// Parse payload for field extraction
	var payloadMap map[string]interface{}
	if len(obj.Payload) > 0 {
		if err := json.Unmarshal(obj.Payload, &payloadMap); err != nil {
			// Not a fatal error - payload might be empty or non-object
			payloadMap = make(map[string]interface{})
		}
	}

	req := &ObjectRequest{
		ID:         headers.Id,
		GlobalID:   headers.GlobalId,
		ObjectType: headers.ObjectType,
		LockID:     headers.LockId,
		Revision:   headers.Revision,
		Payload:    obj.Payload,
		PayloadMap: payloadMap,
	}

	if headers.Version != nil {
		v := *headers.Version
		req.Version = &v
	}
	if headers.ActualFrom != nil {
		req.ActualFrom = headers.ActualFrom
	}
	if headers.DraftStatus != nil {
		req.DraftStatus = headers.DraftStatus
	}
	if headers.ParentId != nil {
		req.ParentID = headers.ParentId
	}

	return req, nil
}

// ============================================================================
// Data and Index table helpers
// ============================================================================

// insertDataTable inserts objects into the data table
func (s *Server) insertDataTable(ctx context.Context, tx *sql.Tx, className string, objects []*ObjectRequest) error {
	dataTable, err := s.metamodel.GetDataTable(className)
	if err != nil {
		return err
	}

	var values []string
	var args []interface{}
	argIdx := 1

	for _, obj := range objects {
		values = append(values, fmt.Sprintf("($%d::bigint, $%d::jsonb, now())", argIdx, argIdx+1))
		args = append(args, obj.ID, string(obj.Payload))
		argIdx += 2
	}

	query := fmt.Sprintf("INSERT INTO murex.%s (id, content, created_at) VALUES %s", dataTable, strings.Join(values, ","))

	_, err = tx.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to insert into %s: %w", dataTable, err)
	}

	return nil
}

// insertIndexTables inserts objects into all index tables
func (s *Server) insertIndexTables(ctx context.Context, tx *sql.Tx, className string, objects []*ObjectRequest) error {
	indexTables, err := s.metamodel.GetIndexTables(className)
	if err != nil {
		return err
	}

	for _, indexTable := range indexTables {
		if err := s.insertSingleIndexTable(ctx, tx, indexTable, objects); err != nil {
			return err
		}
	}

	return nil
}

// insertSingleIndexTable inserts objects into one index table
func (s *Server) insertSingleIndexTable(ctx context.Context, tx *sql.Tx, indexTable metamodel.IndexTable, objects []*ObjectRequest) error {
	if len(indexTable.Fields) == 0 {
		return nil
	}

	// Build column list (excluding created_at which we'll add)
	var columns []string
	for _, field := range indexTable.Fields {
		columns = append(columns, field.DBFieldName)
	}

	var values []string
	var args []interface{}
	argIdx := 1

	for _, obj := range objects {
		var placeholders []string

		for _, field := range indexTable.Fields {
			// Handle special fields
			if field.DBFieldName == "created_at" {
				placeholders = append(placeholders, "now()")
				continue
			}

			// Get value from payload or headers
			value := s.extractFieldValue(obj, field)

			placeholders = append(placeholders, fmt.Sprintf("$%d::%s", argIdx, field.DBFieldType))
			args = append(args, value)
			argIdx++
		}

		values = append(values, "("+strings.Join(placeholders, ", ")+")")
	}

	query := fmt.Sprintf("INSERT INTO murex.%s (%s) VALUES %s",
		indexTable.TableName,
		strings.Join(columns, ", "),
		strings.Join(values, ", "))

	_, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to insert into %s: %w", indexTable.TableName, err)
	}

	return nil
}

// extractFieldValue extracts a field value from ObjectRequest
func (s *Server) extractFieldValue(obj *ObjectRequest, field metamodel.IndexField) interface{} {
	// Handle id field specially
	if field.Source == "id" || field.DBFieldName == "id" {
		return obj.ID
	}

	// Try to get from payload map
	if obj.PayloadMap != nil {
		if val, ok := obj.PayloadMap[field.Source]; ok {
			return val
		}
	}

	return nil
}

// ============================================================================
// Algorithm implementations
// ============================================================================

func (s *Server) saveEmbedded(ctx context.Context, tx *sql.Tx, className string, objects []*redis.StoredObject) (int32, []*pb.ObjectResult, error) {
	mainTable, err := s.metamodel.GetMainTable(className)
	if err != nil {
		return 0, nil, err
	}

	// Parse all objects
	parsedObjects := make([]*ObjectRequest, 0, len(objects))
	for _, obj := range objects {
		req, err := parseStoredObject(obj)
		if err != nil {
			return 0, nil, err
		}
		parsedObjects = append(parsedObjects, req)
	}

	// Build batch INSERT for main table
	var values []string
	var args []interface{}
	argIdx := 1

	for _, req := range parsedObjects {
		var parentID int64 = 0
		if req.ParentID != nil {
			parentID = *req.ParentID
		}

		values = append(values, fmt.Sprintf("($%d::bigint, $%d::varchar, $%d::bigint, now())",
			argIdx, argIdx+1, argIdx+2))
		args = append(args, req.ID, req.ObjectType, parentID)
		argIdx += 3
	}

	query := fmt.Sprintf("INSERT INTO murex.%s (id, object_class, parent_id, created_at) VALUES %s", mainTable, strings.Join(values, ","))

	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to insert into %s: %w", mainTable, err)
	}

	affected, _ := result.RowsAffected()

	// Insert into data table
	if err := s.insertDataTable(ctx, tx, className, parsedObjects); err != nil {
		return 0, nil, err
	}

	// Insert into index tables
	if err := s.insertIndexTables(ctx, tx, className, parsedObjects); err != nil {
		return 0, nil, err
	}

	return int32(affected), nil, nil
}

func (s *Server) saveRevisioned(ctx context.Context, tx *sql.Tx, className string, objects []*redis.StoredObject) (int32, []*pb.ObjectResult, error) {
	mainTable, err := s.metamodel.GetMainTable(className)
	if err != nil {
		return 0, nil, err
	}

	// Parse all objects
	parsedObjects := make([]*ObjectRequest, 0, len(objects))
	for _, obj := range objects {
		req, err := parseStoredObject(obj)
		if err != nil {
			return 0, nil, err
		}
		parsedObjects = append(parsedObjects, req)
	}

	// Build VALUES for CTE
	var values []string
	var args []interface{}
	argIdx := 1

	for _, req := range parsedObjects {
		version := 0
		if req.Version != nil {
			version = int(*req.Version)
		}

		values = append(values, fmt.Sprintf("($%d::bigint, $%d::varchar, $%d::bigint, $%d::int, $%d::int, $%d::varchar)",
			argIdx, argIdx+1, argIdx+2, argIdx+3, argIdx+4, argIdx+5))
		args = append(args, req.ID, req.ObjectType, req.GlobalID, req.Revision, version, req.LockID)
		argIdx += 6
	}

	query := fmt.Sprintf(`
		WITH src_from_app as (
			SELECT *
			FROM (VALUES %s) as appTable(id, object_class, global_id, revision, version, token)
		),
		records_lock_check as (
			SELECT CASE
				WHEN NOT exists (
					SELECT sfa.*, mol.* FROM src_from_app as sfa
					LEFT JOIN murex.object_lock mol
					ON mol.token = sfa.token
					AND expire >= NOW() and sfa.global_id = mol.global_id
					WHERE mol.token is null
				)
				THEN 'YES'
				ELSE 'NO'
			END as all_records_have_lock
		),
		src_from_table as (
			SELECT mcm.id, mcm.object_class, mcm.global_id, mcm.revision, mcm.version, mcm.saved_at, mcm.closed_at, sfa.token
			FROM murex.%s AS mcm
			JOIN src_from_app AS sfa ON mcm.global_id = sfa.global_id
			WHERE closed_at = TIMESTAMP '3000-01-01 00:00:00'
		),
		total_data AS (
			SELECT total_data.* FROM (
				SELECT * FROM src_from_table
				UNION ALL
				SELECT id, object_class, global_id, revision, version, null, null, token FROM src_from_app
			) as total_data
		),
		data_for_merge AS (
			SELECT * FROM total_data td
			CROSS JOIN records_lock_check rlc
			WHERE rlc.all_records_have_lock != 'NO'
		)
		MERGE INTO murex.%s AS mcm
		USING data_for_merge AS dfm
		ON mcm.id = dfm.id
		WHEN MATCHED THEN
			UPDATE SET closed_at = NOW()
		WHEN NOT MATCHED THEN
			INSERT (id, object_class, global_id, revision, version, saved_at, closed_at)
			VALUES (dfm.id, dfm.object_class, dfm.global_id, dfm.revision, dfm.version, NOW(), TIMESTAMP '3000-01-01 00:00:00');
	`, strings.Join(values, ","), mainTable, mainTable)

	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to merge into %s: %w", mainTable, err)
	}

	affected, _ := result.RowsAffected()

	// Insert into data table
	if err := s.insertDataTable(ctx, tx, className, parsedObjects); err != nil {
		return 0, nil, err
	}

	// Insert into index tables
	if err := s.insertIndexTables(ctx, tx, className, parsedObjects); err != nil {
		return 0, nil, err
	}

	return int32(affected), nil, nil
}

func (s *Server) saveDraftableDateBounded(ctx context.Context, tx *sql.Tx, className string, objects []*redis.StoredObject) (int32, []*pb.ObjectResult, error) {
	mainTable, err := s.metamodel.GetMainTable(className)
	if err != nil {
		return 0, nil, err
	}

	// Parse all objects
	parsedObjects := make([]*ObjectRequest, 0, len(objects))
	for _, obj := range objects {
		req, err := parseStoredObject(obj)
		if err != nil {
			return 0, nil, err
		}
		parsedObjects = append(parsedObjects, req)
	}

	// Build VALUES for CTE
	var values []string
	var args []interface{}
	argIdx := 1

	for _, req := range parsedObjects {
		draftStatus := false
		if req.DraftStatus != nil && *req.DraftStatus == "DRAFT" {
			draftStatus = true
		}

		var version interface{} = nil
		if req.Version != nil {
			version = *req.Version
		}

		actualFrom := "2000-01-01"
		if req.ActualFrom != nil {
			actualFrom = *req.ActualFrom
		}

		values = append(values, fmt.Sprintf("($%d::bigint, $%d::varchar, $%d::bigint, $%d::date, $%d::bool, $%d::int, $%d::int, $%d::varchar)",
			argIdx, argIdx+1, argIdx+2, argIdx+3, argIdx+4, argIdx+5, argIdx+6, argIdx+7))

		args = append(args, req.ID, req.ObjectType, req.GlobalID, actualFrom, draftStatus, req.Revision, version, req.LockID)
		argIdx += 8
	}

	query := fmt.Sprintf(`
		WITH src_from_app as (
			SELECT *
			FROM (VALUES %s) AS app_table(id, object_class, global_id, actual_from, draft_status, revision, version, token)
		),
		const AS (
			SELECT
				NOW()::TIMESTAMP AS ts_now,
				DATE '3000-01-01' AS biz_inf,
				TIMESTAMP '3000-01-01 00:00:00' AS tech_inf
		),
		lock_check AS (
			SELECT NOT EXISTS (
				SELECT 1
				FROM src_from_app s
				LEFT JOIN murex.object_lock l
					ON l.token = s.token
					AND l.expire >= NOW()
				WHERE l.token IS NULL
			) AS all_records_have_lock
		),
		active_seg AS (
			SELECT
				s.*,
				seg.actual_from AS seg_from,
				seg.actual_to AS seg_to
			FROM src_from_app s
			LEFT JOIN LATERAL (
				SELECT tm.actual_from, tm.actual_to
				FROM murex.%s tm
				CROSS JOIN const c
				WHERE tm.object_class = s.object_class
					AND tm.global_id = s.global_id
					AND tm.closed_at = c.tech_inf
					AND s.actual_from >= tm.actual_from
					AND s.actual_from < tm.actual_to
				ORDER BY tm.actual_from DESC
				LIMIT 1
				FOR UPDATE OF tm
			) seg ON TRUE
		),
		src_norm AS (
			SELECT
				a.id, a.object_class, a.global_id,
				a.actual_from,
				c.biz_inf AS actual_to,
				a.draft_status,
				a.revision,
				CASE WHEN a.draft_status = false THEN a.version ELSE NULL END AS version,
				a.seg_from, a.seg_to,
				a.token
			FROM active_seg a
			CROSS JOIN const c
		),
		cut_business_period AS (
			UPDATE murex.%s tm
			SET actual_to = s.actual_from
			FROM src_norm s
			CROSS JOIN const c
			JOIN lock_check lc ON lc.all_records_have_lock
			WHERE s.seg_from IS NOT NULL
				AND s.actual_from > s.seg_from
				AND tm.object_class = s.object_class
				AND tm.global_id = s.global_id
				AND tm.closed_at = c.tech_inf
				AND tm.actual_from = s.seg_from
				AND tm.actual_to = s.seg_to
				AND s.draft_status = false
				AND tm.draft_status = false
		),
		close_technical AS (
			UPDATE murex.%s tm
			SET closed_at = c.ts_now
			FROM src_norm s
			CROSS JOIN const c
			JOIN lock_check lc ON lc.all_records_have_lock
			WHERE tm.object_class = s.object_class
				AND tm.global_id = s.global_id
				AND tm.closed_at = c.tech_inf
				AND (
					(s.draft_status = true AND tm.draft_status = true)
					OR
					(s.draft_status = false AND (
						tm.draft_status = true
						OR (
							tm.draft_status = false
							AND s.seg_from IS NOT NULL
							AND tm.actual_from = s.seg_from
							AND tm.actual_to = CASE
								WHEN s.actual_from > s.seg_from THEN s.actual_from
								ELSE s.seg_to
							END
						)
					))
				)
		),
		ins AS (
			INSERT INTO murex.%s (
				id, object_class, global_id,
				actual_from, actual_to,
				draft_status,
				revision, version,
				saved_at, closed_at
			)
			SELECT
				s.id, s.object_class, s.global_id,
				s.actual_from, s.actual_to,
				s.draft_status,
				s.revision, s.version,
				c.ts_now, c.tech_inf
			FROM src_norm s
			CROSS JOIN const c
			JOIN lock_check lc ON lc.all_records_have_lock
			RETURNING id
		)
		SELECT COUNT(*) FROM ins;
	`, strings.Join(values, ","), mainTable, mainTable, mainTable, mainTable)

	var count int32
	err = tx.QueryRowContext(ctx, query, args...).Scan(&count)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to execute DraftableDateBounded save for %s: %w", mainTable, err)
	}

	if count > 0 {
		// Insert into data table
		if err := s.insertDataTable(ctx, tx, className, parsedObjects); err != nil {
			return 0, nil, err
		}

		// Insert into index tables
		if err := s.insertIndexTables(ctx, tx, className, parsedObjects); err != nil {
			return 0, nil, err
		}
	}

	return count, nil, nil
}
