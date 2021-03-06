package sync

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/go-redis/redis/v7"
	"go.uber.org/zap"
)

const (
	RedisPayloadKey = "p"

	// EnvRedisHost is the environment variable that should contain the redis host.
	EnvRedisHost = "REDIS_HOST"

	// DefaultRedisHost is the default redis host.
	DefaultRedisHost = "testground-redis"
)

var DefaultRedisOpts = redis.Options{
	MinIdleConns:       2,               // allow the pool to downsize to 0 conns.
	PoolSize:           5,               // one for subscriptions, one for nonblocking operations.
	PoolTimeout:        3 * time.Minute, // amount of time a waiter will wait for a conn to become available.
	MaxRetries:         30,
	MinRetryBackoff:    1 * time.Second,
	MaxRetryBackoff:    3 * time.Second,
	DialTimeout:        10 * time.Second,
	ReadTimeout:        10 * time.Second,
	WriteTimeout:       10 * time.Second,
	IdleCheckFrequency: 30 * time.Second,
	MaxConnAge:         2 * time.Minute,
}

type RedisConfiguration struct {
	Port int
	Host string
}

type RedisService struct {
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	rclient *redis.Client
	log     *zap.SugaredLogger

	barrierCh chan *barrier
	subCh     chan *subscription
}

func NewRedisService(ctx context.Context, log *zap.SugaredLogger, cfg *RedisConfiguration) (*RedisService, error) {
	rclient, err := redisClient(ctx, log, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create redis client: %w", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	s := &RedisService{
		ctx:       ctx,
		cancel:    cancel,
		log:       log,
		rclient:   rclient,
		barrierCh: make(chan *barrier),
		subCh:     make(chan *subscription),
	}

	s.wg.Add(2)
	go s.barrierWorker()
	go s.subscriptionWorker()

	return s, nil
}

// Close closes this service, cancels ongoing operations, and releases resources.
func (s *RedisService) Close() error {
	s.cancel()
	s.wg.Wait()

	return s.rclient.Close()
}

// barrier represents a barrier over a State. A Barrier is a synchronisation
// checkpoint that will fire once the `target` number of entries on that state
// have been registered.
type barrier struct {
	ctx      context.Context
	key      string
	target   int64
	doneCh   chan error
	resultCh chan error
}

// subscription represents a receive channel for data being published in a
// Topic.
type subscription struct {
	id       string
	ctx      context.Context
	outCh    chan string
	doneCh   chan error
	resultCh chan error
	topic    string
}

// sendFn is a closure that sends an element into the supplied ch and
// it will block if the receiver is not consuming from the channel.
// If the context is closed, the send will be aborted, and the closure will
// return a false value.
func (sub *subscription) sendFn(v string) (sent bool) {
	cases := []reflect.SelectCase{
		{Dir: reflect.SelectSend, Chan: reflect.ValueOf(sub.outCh), Send: reflect.ValueOf(v)},
		{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(sub.ctx.Done())},
	}
	_, _, ctxFired := reflect.Select(cases)
	return !ctxFired
}

// redisClient returns a Redis client constructed from this process' environment
// variables.
func redisClient(ctx context.Context, log *zap.SugaredLogger, cfg *RedisConfiguration) (client *redis.Client, err error) {
	if cfg.Port == 0 {
		cfg.Port = 6379
	}

	opts := DefaultRedisOpts
	opts.Addr = fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	client = redis.NewClient(&opts).WithContext(ctx)

	for {
		if err := ctx.Err(); err != nil {
			_ = client.Close()
			return nil, err
		}

		ok := func() bool {
			log.Infow("trying redis host", "host", cfg.Host, "port", cfg.Port)
			ctx, cancel := context.WithTimeout(ctx, time.Second*5)
			defer cancel()

			if err := client.WithContext(ctx).Ping().Err(); err != nil {
				log.Errorw("failed to ping redis host", "host", cfg.Host, "port", cfg.Port, "error", err)
				return false
			}

			log.Info("redis OK")
			return true
		}()

		if ok {
			return client, nil
		}
	}
}
