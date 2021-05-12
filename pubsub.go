package sync

import (
	"context"
	"fmt"
	"sync"
	"time"
)

var (
	ErrorWriteTimeout       = fmt.Errorf("writing to subscription timeout")
	ErrorSubscriptionClosed = fmt.Errorf("subscription already closed")
)

type subscription struct {
	ctx    context.Context
	closed bool
	last   int
	outCh  chan string
	doneCh chan error
}

func (s *subscription) write(d time.Duration, msg string) error {
	if s.closed {
		return ErrorSubscriptionClosed
	}

	select {
	case s.outCh <- msg:
		return nil
	case <-time.After(d):
		return ErrorWriteTimeout
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
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
			ps.propagateMessages()
		case <-ps.ctx.Done():
			ps.cancel()
			return
		}
	}
}

func (ps *pubsub) propagateMessages() {
	ps.subsMu.RLock()
	defer ps.subsMu.RUnlock()

	for _, sub := range ps.subs {
		func() {
			ps.msgsMu.RLock()
			defer ps.msgsMu.RUnlock()

			for i, msg := range ps.msgs[sub.last:] {
				err := sub.write(time.Second*5, msg)

				if err == ErrorWriteTimeout {
					// TODO: log
					break
				} else if err == ErrorSubscriptionClosed {

					break
				} else if err != nil {
					// TODO: log
					sub.closeWithContext()
					break
				}

				sub.last = i + 1
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
			sub.closeWithContext()
		}
	}

	return true
}

func (ps *pubsub) close() {
	ps.cancel()

	ps.subsMu.Lock()
	defer ps.subsMu.Unlock()

	for _, sub := range ps.subs {
		sub.closeWithContext()
	}

	ps.wg.Wait()
}
