package sync

import (
	"context"
	"encoding/json"
	"sync"
	"time"
)

// InMemService is an in memory service for testing purposes.
type InMemService struct {
	barriersMu sync.RWMutex
	barriers   map[string]int64
	pubsubMu   sync.RWMutex
	pubsub     map[string][]string
}

func NewInMemService() *InMemService {
	return &InMemService{
		barriers: map[string]int64{},
		pubsub:   map[string][]string{},
	}
}

func (s *InMemService) Publish(ctx context.Context, topic string, payload interface{}) (seq int64, err error) {
	s.pubsubMu.Lock()
	defer s.pubsubMu.Unlock()

	if _, ok := s.pubsub[topic]; !ok {
		s.pubsub[topic] = []string{}
	}

	v, err := json.Marshal(payload)
	if err != nil {
		return -1, err
	}

	s.pubsub[topic] = append(s.pubsub[topic], string(v))
	return int64(len(s.pubsub[topic])), nil
}

func (s *InMemService) Subscribe(ctx context.Context, topic string) (*Subscription, error) {
	s.pubsubMu.Lock()
	if _, ok := s.pubsub[topic]; !ok {
		s.pubsub[topic] = []string{}
	}
	s.pubsubMu.Unlock()

	outCh := make(chan string)
	doneCh := make(chan error, 1)

	sub := &Subscription{
		outCh:  outCh,
		doneCh: doneCh,
	}

	go func() {
		counter := 0
		for {
			select {
			case <-ctx.Done():
				sub.doneCh <- ctx.Err()
				return

			case <-time.After(time.Millisecond * 100):
				s.pubsubMu.RLock()
				v := s.pubsub[topic]
				s.pubsubMu.RUnlock()
				if len(v) != counter {
					for ; counter < len(v); counter++ {
						sub.outCh <- v[counter]
					}
				}
			}
		}
	}()

	return sub, nil
}

func (s *InMemService) Barrier(ctx context.Context, state string, target int64) error {
	s.barriersMu.Lock()
	if _, ok := s.barriers[state]; !ok {
		s.barriers[state] = 0
	}
	s.barriersMu.Unlock()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-time.After(time.Millisecond * 100):
			s.barriersMu.RLock()
			v := s.barriers[state]
			s.barriersMu.RUnlock()
			if v >= target {
				return nil
			}
		}
	}
}

func (s *InMemService) SignalEntry(ctx context.Context, state string) (after int64, err error) {
	s.barriersMu.Lock()
	defer s.barriersMu.Unlock()

	if _, ok := s.barriers[state]; !ok {
		s.barriers[state] = 0
	}

	s.barriers[state]++
	return s.barriers[state], nil
}

func (s *InMemService) Close() error {
	return nil
}
