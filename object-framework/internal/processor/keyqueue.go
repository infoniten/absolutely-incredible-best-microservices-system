package processor

import "sync"

// keyQueue provides per-key FIFO ordering. For each key, goroutines wait in
// line — only the first in the queue proceeds, the rest block on a channel.
// When the holder calls Release, the next waiter is unblocked.
//
// This ensures that messages with the same key (e.g., productGlobalID) are
// processed strictly in arrival order, even when multiple goroutines compete.
//
// Typical usage:
//
//	release := q.Acquire(key)
//	defer release()
//	// ... do work under exclusive access for this key ...
type keyQueue struct {
	mu      sync.Mutex
	waiters map[int64][]chan struct{}
}

func newKeyQueue() *keyQueue {
	return &keyQueue{
		waiters: make(map[int64][]chan struct{}),
	}
}

// Acquire waits in FIFO order for exclusive access to the given key.
// Returns a release function that MUST be called when done.
func (q *keyQueue) Acquire(key int64) (release func()) {
	q.mu.Lock()

	queue := q.waiters[key]
	ch := make(chan struct{})

	if len(queue) == 0 {
		// No one holds this key — we're first, proceed immediately.
		close(ch)
	}
	q.waiters[key] = append(queue, ch)
	q.mu.Unlock()

	// Block until it's our turn (immediate if we're first).
	<-ch

	return func() {
		q.mu.Lock()
		defer q.mu.Unlock()

		queue := q.waiters[key]
		// Remove ourselves (always first in the slice).
		queue = queue[1:]

		if len(queue) == 0 {
			delete(q.waiters, key)
		} else {
			q.waiters[key] = queue
			// Wake the next waiter.
			close(queue[0])
		}
	}
}

// WaitCount returns the total number of goroutines waiting across all keys
// (not including the currently active holder for each key).
func (q *keyQueue) WaitCount() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()

	var count int64
	for _, queue := range q.waiters {
		if len(queue) > 1 {
			count += int64(len(queue) - 1) // -1: holder is not "waiting"
		}
	}
	return count
}
