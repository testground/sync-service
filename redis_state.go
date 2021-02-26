package sync

import (
	"context"
)

func (s *RedisService) Barrier(ctx context.Context, state string, target int64) (err error) {
	if target == 0 {
		s.log.Warnw("requested a barrier with target zero; satisfying immediately", "state", state)
		return nil
	}

	b := &barrier{
		key:      state,
		target:   target,
		ctx:      ctx,
		doneCh:   make(chan error, 1),
		resultCh: make(chan error),
	}

	s.barrierCh <- b
	err = <-b.resultCh
	if err != nil {
		return err
	}

	err = <-b.doneCh
	return err
}

func (s *RedisService) SignalEntry(ctx context.Context, state string) (seq int64, err error) {
	s.log.Debugw("signalling entry to state", "key", state)

	// Increment a counter on the state key.
	seq, err = s.rclient.Incr(state).Result()
	if err != nil {
		return 0, err
	}

	s.log.Debugw("new value of state", "key", state, "value", seq)
	return seq, err
}
