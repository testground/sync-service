package sync

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-redis/redis/v7"
	"github.com/google/uuid"
)

// Publish publishes an item on the supplied topic. The payload type must match
// the payload type on the Topic; otherwise Publish will error.
//
// This method returns synchronously, once the item has been published
// successfully, returning the sequence number of the new item in the ordered
// topic, or an error if one occurred, starting with 1 (for the first item).
//
// If error is non-nil, the sequence number must be disregarded.
func (s *RedisService) Publish(ctx context.Context, topic string, payload interface{}) (seq int64, err error) {
	log := s.log.With("topic", topic)
	log.Debugw("publishing item on topic", "payload", payload)

	// Serialize the payload.
	bytes, err := json.Marshal(payload)
	if err != nil {
		return 0, fmt.Errorf("failed while serializing payload: %w", err)
	}

	log.Debugw("serialized json payload", "json", string(bytes))

	// Perform a Redis transaction, adding the item to the stream and fetching
	// the XLEN of the stream.
	args := &redis.XAddArgs{
		Stream: topic,
		ID:     "*",
		Values: map[string]interface{}{RedisPayloadKey: bytes},
	}

	pipe := s.rclient.TxPipeline()
	_ = pipe.XAdd(args)
	xlen := pipe.XLen(topic)

	_, err = pipe.ExecContext(ctx)
	if err != nil {
		log.Debugw("failed to publish item", "error", err)
		return 0, err
	}

	seq = xlen.Val()
	s.log.Debugw("successfully published item; sequence number obtained", "seq", seq)
	return seq, nil
}

func (s *RedisService) Subscribe(ctx context.Context, topic string) (*Subscription, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if err := s.ctx.Err(); err != nil {
		return nil, err
	}

	sub := &subscription{
		id:       uuid.New().String()[24:],
		ctx:      ctx,
		outCh:    make(chan string),
		doneCh:   make(chan error, 1),
		resultCh: make(chan error),
		topic:    topic,
	}
	s.subCh <- sub
	err := <-sub.resultCh
	return &Subscription{
		outCh:  sub.outCh,
		doneCh: sub.doneCh,
	}, err
}
