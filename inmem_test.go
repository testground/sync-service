package sync

import (
	"context"
	"testing"
)

func getInMemService(ctx context.Context) (Service, error) {
	return NewInMemService(), nil
}

func TestInMemBarrier(t *testing.T) {
	testBarrier(t, getInMemService)
}

func TestInMemBarrierBeyondTarget(t *testing.T) {
	testBarrierBeyondTarget(t, getInMemService)
}

func TestInMemBarrierZero(t *testing.T) {
	testBarrierZero(t, getInMemService)
}

func TestInMemBarrierCancel(t *testing.T) {
	testBarrierCancel(t, getInMemService)
}

func TestInMemBarrierDeadline(t *testing.T) {
	testBarrierDeadline(t, getInMemService)
}

func TestInMemSubscribeAfterAllPublished(t *testing.T) {
	testSubscribeAfterAllPublished(t, getInMemService)
}

func TestInMemSubscribeFirstConcurrentWrites(t *testing.T) {
	testSubscribeFirstConcurrentWrites(t, getInMemService)
}

func TestInMemSubscriptionConcurrentPublishersSubscribers(t *testing.T) {
	testSubscriptionConcurrentPublishersSubscribers(t, getInMemService)
}

func TestInMemSequenceOnWrite(t *testing.T) {
	testSequenceOnWrite(t, getInMemService)
}
