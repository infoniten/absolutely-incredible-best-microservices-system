package client

import (
	"context"
	"fmt"
	"sync"
)

const (
	// DefaultGlobalIDBatchSize is the default number of GlobalIDs to fetch in one batch
	DefaultGlobalIDBatchSize = 2000
	// DefaultGlobalIDPoolRefillThreshold - refill when pool drops below this
	DefaultGlobalIDPoolRefillThreshold = 500
)

// GlobalIDPool provides a local pool of pre-fetched GlobalIDs for high-throughput scenarios
type GlobalIDPool struct {
	client           *GlobalIDClient
	batchSize        int32
	refillThreshold  int

	mu              sync.Mutex
	ids             []int64
	refilling       bool
	refillCond      *sync.Cond
}

// NewGlobalIDPool creates a new GlobalID pool with pre-fetching
func NewGlobalIDPool(client *GlobalIDClient, batchSize int32, refillThreshold int) *GlobalIDPool {
	if batchSize <= 0 {
		batchSize = DefaultGlobalIDBatchSize
	}
	if refillThreshold <= 0 {
		refillThreshold = DefaultGlobalIDPoolRefillThreshold
	}

	p := &GlobalIDPool{
		client:          client,
		batchSize:       batchSize,
		refillThreshold: refillThreshold,
		ids:             make([]int64, 0, batchSize),
	}
	p.refillCond = sync.NewCond(&p.mu)
	return p
}

// Prefill fills the pool initially (call once at startup)
func (p *GlobalIDPool) Prefill(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	ids, err := p.client.GetBatchGlobalID(ctx, p.batchSize)
	if err != nil {
		return fmt.Errorf("failed to prefill GlobalID pool: %w", err)
	}

	p.ids = append(p.ids, ids...)
	return nil
}

// GetGlobalID returns a single GlobalID from the pool
func (p *GlobalIDPool) GetGlobalID(ctx context.Context) (int64, error) {
	p.mu.Lock()

	// Wait if pool is empty and refilling
	for len(p.ids) == 0 {
		if !p.refilling {
			// Start refill
			p.refilling = true
			p.mu.Unlock()

			ids, err := p.client.GetBatchGlobalID(ctx, p.batchSize)

			p.mu.Lock()
			p.refilling = false

			if err != nil {
				p.refillCond.Broadcast()
				p.mu.Unlock()
				return 0, fmt.Errorf("failed to refill GlobalID pool: %w", err)
			}

			p.ids = append(p.ids, ids...)
			p.refillCond.Broadcast()
		} else {
			// Wait for refill to complete
			p.refillCond.Wait()
		}
	}

	// Get GlobalID from pool
	id := p.ids[0]
	p.ids = p.ids[1:]
	poolSize := len(p.ids)

	// Trigger background refill if below threshold
	if poolSize < p.refillThreshold && !p.refilling {
		go p.backgroundRefill(context.Background())
	}

	p.mu.Unlock()
	return id, nil
}

// GetBatchGlobalID returns multiple GlobalIDs from the pool
func (p *GlobalIDPool) GetBatchGlobalID(ctx context.Context, count int) ([]int64, error) {
	result := make([]int64, 0, count)

	for i := 0; i < count; i++ {
		id, err := p.GetGlobalID(ctx)
		if err != nil {
			return nil, err
		}
		result = append(result, id)
	}

	return result, nil
}

// backgroundRefill refills the pool in background
func (p *GlobalIDPool) backgroundRefill(ctx context.Context) {
	p.mu.Lock()
	if p.refilling {
		p.mu.Unlock()
		return
	}
	p.refilling = true
	p.mu.Unlock()

	ids, err := p.client.GetBatchGlobalID(ctx, p.batchSize)

	p.mu.Lock()
	p.refilling = false
	if err == nil {
		p.ids = append(p.ids, ids...)
	}
	p.refillCond.Broadcast()
	p.mu.Unlock()
}

// Size returns current pool size
func (p *GlobalIDPool) Size() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.ids)
}
