package client

import (
	"context"
	"fmt"
	"sync"
)

const (
	// DefaultIDBatchSize is the default number of IDs to fetch in one batch
	DefaultIDBatchSize = 2000
	// DefaultIDPoolRefillThreshold - refill when pool drops below this
	DefaultIDPoolRefillThreshold = 500
)

// IDPool provides a local pool of pre-fetched IDs for high-throughput scenarios
type IDPool struct {
	client           *IDClient
	batchSize        int32
	refillThreshold  int

	mu              sync.Mutex
	ids             []int64
	refilling       bool
	refillCond      *sync.Cond
}

// NewIDPool creates a new ID pool with pre-fetching
func NewIDPool(client *IDClient, batchSize int32, refillThreshold int) *IDPool {
	if batchSize <= 0 {
		batchSize = DefaultIDBatchSize
	}
	if refillThreshold <= 0 {
		refillThreshold = DefaultIDPoolRefillThreshold
	}

	p := &IDPool{
		client:          client,
		batchSize:       batchSize,
		refillThreshold: refillThreshold,
		ids:             make([]int64, 0, batchSize),
	}
	p.refillCond = sync.NewCond(&p.mu)
	return p
}

// Prefill fills the pool initially (call once at startup)
func (p *IDPool) Prefill(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	ids, err := p.client.GetBatchID(ctx, p.batchSize)
	if err != nil {
		return fmt.Errorf("failed to prefill ID pool: %w", err)
	}

	p.ids = append(p.ids, ids...)
	return nil
}

// GetID returns a single ID from the pool
func (p *IDPool) GetID(ctx context.Context) (int64, error) {
	p.mu.Lock()

	// Wait if pool is empty and refilling
	for len(p.ids) == 0 {
		if !p.refilling {
			// Start refill
			p.refilling = true
			p.mu.Unlock()

			ids, err := p.client.GetBatchID(ctx, p.batchSize)

			p.mu.Lock()
			p.refilling = false

			if err != nil {
				p.refillCond.Broadcast()
				p.mu.Unlock()
				return 0, fmt.Errorf("failed to refill ID pool: %w", err)
			}

			p.ids = append(p.ids, ids...)
			p.refillCond.Broadcast()
		} else {
			// Wait for refill to complete
			p.refillCond.Wait()
		}
	}

	// Get ID from pool
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

// GetBatchID returns multiple IDs from the pool
func (p *IDPool) GetBatchID(ctx context.Context, count int) ([]int64, error) {
	result := make([]int64, 0, count)

	for i := 0; i < count; i++ {
		id, err := p.GetID(ctx)
		if err != nil {
			return nil, err
		}
		result = append(result, id)
	}

	return result, nil
}

// backgroundRefill refills the pool in background
func (p *IDPool) backgroundRefill(ctx context.Context) {
	p.mu.Lock()
	if p.refilling {
		p.mu.Unlock()
		return
	}
	p.refilling = true
	p.mu.Unlock()

	ids, err := p.client.GetBatchID(ctx, p.batchSize)

	p.mu.Lock()
	p.refilling = false
	if err == nil {
		p.ids = append(p.ids, ids...)
	}
	p.refillCond.Broadcast()
	p.mu.Unlock()
}

// Size returns current pool size
func (p *IDPool) Size() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.ids)
}
