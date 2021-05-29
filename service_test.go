package sync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/testground/testground/pkg/logging"
	"golang.org/x/sync/errgroup"
)

func getDefaultService(ctx context.Context) (Service, error) {
	return NewDefaultService(ctx, logging.S())
}

func TestBarrier(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	service, err := getDefaultService(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()

	state := "yoda:" + uuid.New().String()
	for i := 1; i <= 10; i++ {
		if curr, err := service.SignalEntry(ctx, state); err != nil {
			t.Fatal(err)
		} else if curr != i {
			t.Fatalf("expected current count to be: %d; was: %d", i, curr)
		}
	}

	err = service.Barrier(ctx, state, 10)
	if err != nil {
		t.Fatal(err)
	}
}

func TestBarrierBeyondTarget(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	service, err := getDefaultService(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()

	state := "yoda" + uuid.New().String()
	for i := 1; i <= 20; i++ {
		if curr, err := service.SignalEntry(ctx, state); err != nil {
			t.Fatal(err)
		} else if curr != i {
			t.Fatalf("expected current count to be: %d; was: %d", i, curr)
		}
	}

	err = service.Barrier(ctx, state, 10)
	if err != nil {
		t.Fatal(err)
	}
}

func TestBarrierZero(t *testing.T) {
	timeout := time.After(3 * time.Second)
	done := make(chan bool)
	var errs error

	go func() {
		defer func() {
			done <- true
		}()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		service, err := getDefaultService(ctx)
		if err != nil {
			errs = err
			return
		}
		defer service.Close()

		err = service.Barrier(ctx, "apollo", 0)
		if err != nil {
			errs = err
		}
	}()

	select {
	case <-timeout:
		t.Fatal("test didn't finish in time")
	case <-done:
		if errs != nil {
			t.Fatal(errs)
		}
	}
}

func TestBarrierCancel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	service, err := getDefaultService(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()

	go func() {
		time.Sleep(time.Second)
		cancel()
	}()

	state := "yoda" + uuid.New().String()
	err = service.Barrier(ctx, state, 10)

	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context cancelled error; instead got: %s", err)
	}
}

func TestBarrierDeadline(t *testing.T) {
	timeout := time.After(3 * time.Second)
	done := make(chan bool)
	var errs error

	go func() {
		defer func() {
			done <- true
		}()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		service, err := getDefaultService(ctx)
		if err != nil {
			errs = err
			return
		}
		defer service.Close()

		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		state := "yoda" + uuid.New().String()
		err = service.Barrier(ctx, state, 10)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("expected context cancelled error; instead got: %s", err)
		}
	}()

	select {
	case <-timeout:
		t.Fatal("test didn't finish in time")
	case <-done:
		if errs != nil {
			t.Fatal(errs)
		}
	}
}

type TestPayload struct {
	FieldA string
	FieldB struct {
		FieldB1 string
		FieldB2 int
	}
}

func TestSubscribeAfterAllPublished(t *testing.T) {
	iterations := 1000
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	service, err := getDefaultService(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()

	values := make([]TestPayload, 0, iterations)
	for i := 0; i < iterations; i++ {
		s := fmt.Sprintf("item-%d", i)
		values = append(values, TestPayload{
			FieldA: s,
			FieldB: struct {
				FieldB1 string
				FieldB2 int
			}{FieldB1: s, FieldB2: i},
		})
	}

	topic := "pandemic:2021:" + uuid.New().String()

	for i, s := range values {
		if seq, err := service.Publish(context.Background(), topic, s); err != nil {
			t.Fatalf("failed while writing key to subtree: %s", err)
		} else if seq != i+1 {
			t.Fatalf("expected seq == i+1; seq: %d; i: %d", seq, i)
		}
	}

	sub, err := service.Subscribe(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}

	for i, expected := range values {
		chosen, recv, _ := reflect.Select([]reflect.SelectCase{
			{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(sub.outCh)},
			{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())},
		})

		expect, err := json.Marshal(expected)
		if err != nil {
			t.Fatal(err)
		}

		switch chosen {
		case 0:
			if recv.Kind() == reflect.Ptr {
				recv = recv.Elem()
			}

			if (recv.Interface()) != string(expect) {
				t.Fatalf("expected value %v, got %v in position %d", string(expect), recv.Interface(), i)
			}
		case 1:
			t.Fatal("failed to receive all expected items within the deadline")
		}
	}
}

func TestSubscribeFirstConcurrentWrites(t *testing.T) {
	iterations := 1000
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	service, err := getDefaultService(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()

	topic := "virus:" + uuid.New().String()

	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	sub, err := service.Subscribe(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}

	grp, ctx := errgroup.WithContext(context.Background())
	v := make([]bool, iterations)
	for i := 0; i < iterations; i++ {
		grp.Go(func() error {
			seq, err := service.Publish(ctx, topic, "foo")
			if err != nil {
				return err
			}
			v[seq-1] = true
			return nil
		})
	}

	if err := grp.Wait(); err != nil {
		t.Fatal(err)
	}

	for i, b := range v {
		if !b {
			t.Fatalf("sequence number absent: %d", i+1)
		}
	}

	// receive all items.
	for i := 0; i < iterations; i++ {
		<-sub.outCh
	}

	// no more items queued
	if l := len(sub.outCh); l > 0 {
		t.Fatalf("expected no more items queued; got: %d", l)
	}
}

func TestSubscriptionConcurrentPublishersSubscribers(t *testing.T) {
	var (
		topics     = 100
		iterations = 100
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	service, err := getDefaultService(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()

	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	pgrp, ctx := errgroup.WithContext(context.Background())
	sgrp, ctx := errgroup.WithContext(context.Background())
	for i := 0; i < topics; i++ {
		topic := fmt.Sprintf("antigen-%d", i)

		// launch producer.
		pgrp.Go(func() error {
			for i := 0; i < iterations; i++ {
				_, err := service.Publish(ctx, topic, "foo")
				if err != nil {
					return err
				}
			}
			return nil
		})

		// launch subscriber.
		sgrp.Go(func() error {
			sub, err := service.Subscribe(ctx, topic)
			if err != nil {
				return err
			}
			for i := 0; i < iterations; i++ {
				<-sub.outCh
			}
			return nil
		})
	}

	if err := pgrp.Wait(); err != nil {
		t.Fatalf("producers failed: %s", err)
	}

	if err := sgrp.Wait(); err != nil {
		t.Fatalf("subscribers failed: %s", err)
	}
}

func TestSequenceOnWrite(t *testing.T) {
	var (
		iterations = 1000
		topic      = "pandemic:" + uuid.New().String()
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	service, err := getDefaultService(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()

	s := "a"
	for i := 1; i <= iterations; i++ {
		seq, err := service.Publish(ctx, topic, s)
		if err != nil {
			t.Fatal(err)
		}

		if seq != i {
			t.Fatalf("expected seq %d, got %d", i, seq)
		}
	}
}
