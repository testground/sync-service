package sync

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/testground/testground/pkg/logging"
	"go.uber.org/zap"
)

func TestMain(m *testing.M) {
	// Avoid collisions in Redis keys over test runs.
	rand.Seed(time.Now().UnixNano())

	// _ = os.Setenv("LOG_LEVEL", "debug")

	// Set fail-fast options for creating the client, capturing the default
	// state to restore it.
	prev := DefaultRedisOpts
	DefaultRedisOpts.PoolTimeout = 500 * time.Millisecond
	DefaultRedisOpts.MaxRetries = 0

	closeFn, err := ensureRedis()
	DefaultRedisOpts = prev
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	v := m.Run()

	_ = closeFn()
	os.Exit(v)
}

// Check if there's a running instance of redis, or start it otherwise. If we
// start an ad-hoc instance, the close function will terminate it.
func ensureRedis() (func() error, error) {
	// Try to obtain a client; if this fails, we'll attempt to start a redis
	// instance.
	client, err := redisClient(context.Background(), zap.S(), &RedisConfiguration{
		Host: "localhost",
	})
	if err == nil {
		_ = client.Close()
		return func() error { return nil }, err
	}

	cmd := exec.Command("redis-server", "-")
	if err := cmd.Start(); err != nil {
		return func() error { return nil }, fmt.Errorf("failed to start redis: %w", err)
	}

	time.Sleep(1 * time.Second)

	// Try to obtain a client again.
	if client, err = redisClient(context.Background(), zap.S(), &RedisConfiguration{
		Host: "localhost",
	}); err != nil {
		return func() error { return nil }, fmt.Errorf("failed to obtain redis client despite starting instance: %v", err)
	}
	defer client.Close()

	return func() error {
		if err := cmd.Process.Kill(); err != nil {
			return fmt.Errorf("failed while stopping test-scoped redis: %s", err)
		}
		return nil
	}, nil
}

func getRedisService(ctx context.Context) (Service, error) {
	return NewRedisService(ctx, logging.S(), &RedisConfiguration{
		Port: 6379,
		Host: "localhost",
	})
}

func TestRedisBarrier(t *testing.T) {
	testBarrier(t, getRedisService)
}

func TestRedisBarrierBeyondTarget(t *testing.T) {
	testBarrierBeyondTarget(t, getRedisService)
}

func TestRedisBarrierZero(t *testing.T) {
	testBarrierZero(t, getRedisService)
}

func TestRedisBarrierCancel(t *testing.T) {
	testBarrierCancel(t, getRedisService)
}

func TestRedisBarrierDeadline(t *testing.T) {
	testBarrierDeadline(t, getRedisService)
}

func TestRedisSubscribeAfterAllPublished(t *testing.T) {
	testSubscribeAfterAllPublished(t, getRedisService)
}

func TestRedisSubscribeFirstConcurrentWrites(t *testing.T) {
	testSubscribeFirstConcurrentWrites(t, getRedisService)
}

func TestRedisSubscriptionConcurrentPublishersSubscribers(t *testing.T) {
	testSubscriptionConcurrentPublishersSubscribers(t, getRedisService)
}

func TestRedisSequenceOnWrite(t *testing.T) {
	testSequenceOnWrite(t, getRedisService)
}

func TestGC(t *testing.T) {
	GCLastAccessThreshold = 1 * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	v, err := getRedisService(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	service := v.(*RedisService)

	b := make([]byte, 16)
	_, _ = rand.Read(b)
	prefix := hex.EncodeToString(b)

	// Create 200 keys.
	for i := 1; i <= 200; i++ {
		state := fmt.Sprintf("%s-%d", prefix, i)
		if _, err := service.SignalEntry(ctx, state); err != nil {
			t.Fatal(err)
		}
	}

	pattern := "*" + prefix + "*"
	keys, _ := service.rclient.Keys(pattern).Result()
	if l := len(keys); l != 200 {
		t.Fatalf("expected 200 keys matching %s; got: %d", pattern, l)
	}

	time.Sleep(2 * time.Second)

	ch := make(chan error, 1)
	service.EnableBackgroundGC(ch)

	<-ch

	keys, _ = service.rclient.Keys(pattern).Result()
	if l := len(keys); l != 0 {
		t.Fatalf("expected 0 keys matching %s; got: %d", pattern, l)
	}
}

func TestConnUnblock(t *testing.T) {
	client := redis.NewClient(&redis.Options{})
	c := client.Conn()
	id, _ := c.ClientID().Result()

	ch := make(chan struct{})
	go func() {
		timer := time.AfterFunc(1*time.Second, func() { close(ch) })
		_, _ = c.XRead(&redis.XReadArgs{Streams: []string{"aaaa", "0"}, Block: 0}).Result()
		timer.Stop()
	}()

	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("XREAD unexpectedly returned early")
	}

	unblocked, err := client.ClientUnblock(id).Result()
	if err != nil {
		t.Fatal(err)
	}
	if unblocked != 1 {
		t.Fatal("expected CLIENT UNBLOCK to return 1")
	}
	for i := 0; i < 10; i++ {
		id2, err := c.ClientID().Result()
		if err != nil {
			t.Fatal(err)
		}
		if id != id2 {
			t.Errorf("expected client id to be: %d, was: %d", id, id2)
		}
	}
}
