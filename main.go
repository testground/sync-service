package main

import (
	"context"
	"log"
	"net/http"
	"os"

	"github.com/testground/testground/pkg/cmd"
	"github.com/testground/testground/pkg/logging"
)

func main() {
	err := run()
	if err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, cancel := context.WithCancel(cmd.ProcessContext())
	defer cancel()

	redisHost := os.Getenv(EnvRedisHost)
	if redisHost == "" {
		redisHost = DefaultRedisHost
	}

	service, err := NewRedisService(ctx, logging.S(), &RedisConfiguration{
		Port: 6379,
		Host: redisHost,
	})
	if err != nil {
		return err
	}
	service.EnableBackgroundGC(nil)

	srv, err := NewServer(service, 5050)
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
