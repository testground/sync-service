package sync

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type subscription struct {
	sync.Mutex
	ctx    context.Context
	outCh  chan string
	doneCh chan error
	last   int
	closed bool
}

func (s *subscription) write(d time.Duration, msg string) error {
	s.Lock()
	defer s.Unlock()

	if s.closed {
		return fmt.Errorf("subscription already closed")
	}

	ctx, cancel := context.WithTimeout(s.ctx, d)
	defer cancel()

	select {
	case s.outCh <- msg:
		// Increment last for the next ID.
		s.last++
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *subscription) close() {
	s.Lock()
	defer s.Unlock()

	if s.closed {
		return
	}

	s.closed = true
	s.doneCh <- s.ctx.Err()
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
			ps.writeMessages()
		case <-ps.ctx.Done():
			ps.cancel()
			return
		}
	}
}

func (ps *pubsub) writeMessages() {
	ps.subsMu.RLock()
	defer ps.subsMu.RUnlock()

	for _, sub := range ps.subs {
		func() {
			ps.msgsMu.RLock()
			defer ps.msgsMu.RUnlock()

			for _, msg := range ps.msgs[sub.last:] {
				err := sub.write(time.Second*5, msg)
				if err != nil {
					log.Warnf("cannot send message to subscriber: %w", err)
					break
				}
			}
		}()
	}
}

func (ps *pubsub) isDone() bool {
	ps.subsMu.RLock()
	defer ps.subsMu.RUnlock()

	for _, sub := range ps.subs {
		if sub.ctx.Err() == nil {
			return false
		} else if !sub.closed {
			sub.close()
		}
	}

	return true
}

func (ps *pubsub) close() {
	ps.cancel()

	ps.subsMu.Lock()
	defer ps.subsMu.Unlock()

	for _, sub := range ps.subs {
		sub.close()
	}

	ps.wg.Wait()
}
