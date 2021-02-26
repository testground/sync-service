package main

import (
	"context"
	"log"
	"net/http"
	"os"

	"github.com/testground/sync-service"
	"github.com/testground/testground/pkg/cmd"
	"github.com/testground/testground/pkg/logging"
)

const envRedisHost = "REDIS_HOST"
const defaultRedisHost = "testground-redis"

func main() {
	err := run()
	if err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, cancel := context.WithCancel(cmd.ProcessContext())
	defer cancel()

	redisHost := os.Getenv(envRedisHost)
	if redisHost == "" {
		redisHost = defaultRedisHost
	}

	service, err := sync.NewRedisService(ctx, logging.S(), &sync.RedisConfiguration{
		Port: 6379,
		Host: redisHost,
	})
	if err != nil {
		return err
	}
	service.EnableBackgroundGC(nil)

	srv, err := sync.NewServer(service, 5050)
	if err != nil {
		return err
	}

	exiting := make(chan struct{})
	defer close(exiting)

	go func() {
		select {
		case <-ctx.Done():
		case <-exiting:
			// no need to shutdown in this case.
			return
		}

		logging.S().Infow("shutting down sync service")

		_ = service.Close()
		_ = srv.Shutdown(ctx)
	}()

	logging.S().Infow("sync service listening", "addr", srv.Addr())
	err = srv.Serve()
	if err == http.ErrServerClosed {
		err = nil
	}
	return err
}
