// +build stress

package sync

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestPerfBarrier(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	service, err := getRedisService(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()

	state := "yoda:" + uuid.New().String()

	workers := 5
	iterations := 100000

	for i := 1; i <= workers; i++ {
		go func(t *testing.T) {
			for i := 1; i <= iterations; i++ {
				if _, err := service.SignalEntry(ctx, state); err != nil {
					t.Error(err)
				}
			}
		}(t)
	}

	err = service.Barrier(ctx, state, int64(workers*iterations))
	if err != nil {
		t.Fatal(err)
	}
}
