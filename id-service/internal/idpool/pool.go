package idpool

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type Pool struct {
	db           *sql.DB
	tracer       trace.Tracer
	poolSize     int
	ids          []int64
	mu           sync.Mutex
	refillThreshold int
}

func New(db *sql.DB, tracer trace.Tracer, poolSize int) *Pool {
	return &Pool{
		db:              db,
		tracer:          tracer,
		poolSize:        poolSize,
		ids:             make([]int64, 0, poolSize),
		refillThreshold: poolSize / 10, // refill when 10% remaining
	}
}

func (p *Pool) GetID(ctx context.Context) (int64, error) {
	ctx, span := p.tracer.Start(ctx, "Pool.GetID")
	defer span.End()

	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.ids) == 0 {
		if err := p.refillLocked(ctx); err != nil {
			return 0, err
		}
	}

	id := p.ids[0]
	p.ids = p.ids[1:]

	span.SetAttributes(attribute.Int64("id", id))

	// Async refill if below threshold
	if len(p.ids) < p.refillThreshold {
		go func() {
			p.mu.Lock()
			defer p.mu.Unlock()
			_ = p.refillLocked(context.Background())
		}()
	}

	return id, nil
}

func (p *Pool) GetBatchID(ctx context.Context, batchSize int) ([]int64, error) {
	ctx, span := p.tracer.Start(ctx, "Pool.GetBatchID")
	defer span.End()

	span.SetAttributes(attribute.Int("batch_size", batchSize))

	p.mu.Lock()
	defer p.mu.Unlock()

	// If requested batch is larger than pool, fetch directly from DB
	if batchSize > p.poolSize {
		return p.fetchFromDB(ctx, batchSize)
	}

	// Ensure we have enough IDs
	for len(p.ids) < batchSize {
		if err := p.refillLocked(ctx); err != nil {
			return nil, err
		}
	}

	ids := make([]int64, batchSize)
	copy(ids, p.ids[:batchSize])
	p.ids = p.ids[batchSize:]

	span.SetAttributes(attribute.Int("ids_returned", len(ids)))

	return ids, nil
}

func (p *Pool) refillLocked(ctx context.Context) error {
	ctx, span := p.tracer.Start(ctx, "Pool.refill")
	defer span.End()

	ids, err := p.fetchFromDB(ctx, p.poolSize)
	if err != nil {
		return err
	}

	p.ids = append(p.ids, ids...)
	span.SetAttributes(attribute.Int("pool_size_after_refill", len(p.ids)))

	return nil
}

func (p *Pool) fetchFromDB(ctx context.Context, count int) ([]int64, error) {
	ctx, span := p.tracer.Start(ctx, "Pool.fetchFromDB")
	defer span.End()

	span.SetAttributes(attribute.Int("count", count))

	var startID int64
	err := p.db.QueryRowContext(ctx, "SELECT murex.get_next_batch_id($1)", count).Scan(&startID)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to fetch IDs from database: %w", err)
	}

	ids := make([]int64, count)
	for i := 0; i < count; i++ {
		ids[i] = startID + int64(i)
	}

	return ids, nil
}
