package sync

import (
	"context"
	"sync"
	"time"
)

type subscription struct {
	ctx    context.Context
	closed bool
	last   int
	outCh  chan string
	doneCh chan error
}

func (s *subscription) closeWithContext() {
	if s.closed {
		return
	}

	s.doneCh <- s.ctx.Err()
	s.closed = true
	close(s.doneCh)
	close(s.outCh)
}

type pubsub struct {
	ctx     context.Context
	cancel  context.CancelFunc
	lastmod time.Time
	wg      sync.WaitGroup

	msgsMu sync.RWMutex
	msgs   []string

	subsMu sync.RWMutex
	subs   []*subscription
}

func (ps *pubsub) subscribe(ctx context.Context) *subscription {
	ps.subsMu.Lock()
	defer ps.subsMu.Unlock()

	sub := &subscription{
		ctx:    ctx,
		closed: false,
		outCh:  make(chan string),
		doneCh: make(chan error, 1),
		last:   0,
	}

	ps.lastmod = time.Now()
	ps.subs = append(ps.subs, sub)
	return sub
}

func (ps *pubsub) publish(msg string) int {
	ps.msgsMu.Lock()
	defer ps.msgsMu.Unlock()

	ps.lastmod = time.Now()
	ps.msgs = append(ps.msgs, msg)
	return len(ps.msgs)
}

func (ps *pubsub) worker() {
	ticker := time.NewTicker(100 * time.Millisecond)
	ps.wg.Add(1)
	defer ps.wg.Done()

	for {
		select {
		case <-ticker.C:
			ps.subsMu.RLock()
			for _, sub := range ps.subs {
				ps.msgsMu.RLock()
				for _, msg := range ps.msgs[sub.last:] {
					if !sub.closed && sub.ctx.Err() == nil {
						sub.outCh <- msg
					} else if !sub.closed {
						sub.closeWithContext()
						break
					}
				}
				sub.last = len(ps.msgs)
				ps.msgsMu.RUnlock()
			}
			ps.subsMu.RUnlock()
		case <-ps.ctx.Done():
			ps.subsMu.Lock()
			defer ps.subsMu.Unlock()

			for _, sub := range ps.subs {
				sub.closeWithContext()
			}

			return
		}
	}
}

func (ps *pubsub) isDone() bool {
	ps.subsMu.RLock()
	defer ps.subsMu.RUnlock()

	for _, sub := range ps.subs {
		if sub.ctx.Err() == nil {
			return false
		} else if !sub.closed {
			sub.closeWithContext()
		}
	}

	return true
}

func (ps *pubsub) close() {
	ps.cancel()
	ps.wg.Wait()
}
