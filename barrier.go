package sync

import (
	"context"
	"sync"
)

// barrier represents a single barrier with multiple waiters.
type barrier struct {
	sync.Mutex
	count int
	zcs   []*zeroCounter
}

// wait waits for the barrier to reach a certain target.
func (b *barrier) wait(ctx context.Context, target int) error {
	b.Lock()

	// If we're already over the target, return immediately.
	if target <= b.count {
		b.Unlock()
		return nil
	}

	// Create a zero counter to wait for target - count elements to signal entry.
	// It also returns if the context fires.
	zc := newZeroCounter(ctx, target-b.count)

	// TODO: Or do a simpler way where we just pool every 100ms to see if count has
	// the correct value or ctx fired. Less code and less complexity.
	b.zcs = append(b.zcs, zc)
	b.Unlock()
	return zc.wait()
}

// inc increments the barrier by one unit. To do so, we increment
// the counter and tell all the channels we received a new entry.
func (b *barrier) inc() int {
	b.Lock()
	defer b.Unlock()

	b.count += 1
	count := b.count

	for _, zc := range b.zcs {
		zc.dec()
	}

	return count
}

// isDone returns true if all the counters for this barrier have reached zero.
func (b *barrier) isDone() bool {
	b.Lock()
	defer b.Unlock()

	for _, zc := range b.zcs {
		if !zc.done() {
			return false
		}
	}

	return true
}

type zeroCounter struct {
	sync.Mutex
	ctx    context.Context
	ch     chan struct{}
	closed bool
	count  int
}

func newZeroCounter(ctx context.Context, target int) *zeroCounter {
	return &zeroCounter{
		count: target,
		ctx:   ctx,
		ch:    make(chan struct{}),
	}
}

func (w *zeroCounter) dec() {
	w.Lock()
	defer w.Unlock()

	if w.closed {
		return
	}

	w.count -= 1
	if w.count <= 0 {
		w.closed = true
		close(w.ch)
	}
}

func (w *zeroCounter) wait() error {
	select {
	case <-w.ctx.Done():
		return w.ctx.Err()
	case <-w.ch:
		return nil
	}
}

func (w *zeroCounter) done() bool {
	return w.closed
}
