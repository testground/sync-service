package sync

import (
	"context"
	"testing"
)

func getConnWithInMemService(ctx context.Context) *connection {
	service, _ := getInMemService(ctx)

	conn := &connection{
		service:     service,
		ctx:         ctx,
		responses:   make(chan *Response),
		cancelFuncs: map[string]context.CancelFunc{},
	}

	return conn
}

func TestConnectionBarrier(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn := getConnWithInMemService(ctx)

	go conn.barrierHandler("pandemic", &BarrierRequest{
		State:  "coronavirus",
		Target: -1,
	})

	res := <-conn.responses

	if res.ID != "pandemic" {
		t.Error("expected res.ID to be pandemic")
	}

	if res.Error != "" {
		t.Error(res.Error)
	}
}

func TestConnectionSignalEntry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn := getConnWithInMemService(ctx)

	go conn.signalEntryHandler("pandemic", &SignalEntryRequest{
		State: "coronavirus",
	})

	res := <-conn.responses

	if res.ID != "pandemic" {
		t.Error("expected res.ID to be pandemic")
	}

	if res.Error != "" {
		t.Error(res.Error)
	}

	if res.SignalEntryResponse == nil {
		t.Error("expecting signal entry response")
	}

	if res.SignalEntryResponse.Seq != 1 {
		t.Error("expecting seq to be 1")
	}
}

func TestConnectionPublish(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn := getConnWithInMemService(ctx)

	go conn.publishHandler("pandemic", &PublishRequest{
		Topic:   "science",
		Payload: "just a test",
	})

	res := <-conn.responses

	if res.ID != "pandemic" {
		t.Error("expected res.ID to be pandemic")
	}

	if res.Error != "" {
		t.Error(res.Error)
	}

	if res.PublishResponse == nil {
		t.Error("expecting publish response")
	}

	if res.PublishResponse.Seq != 1 {
		t.Error("expecting seq to be 1")
	}
}

func TestConnectionSubscribe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn := getConnWithInMemService(ctx)

	go conn.subscribeHandler("pandemic1", &SubscribeRequest{
		Topic: "data",
	})

	go conn.publishHandler("pandemic2", &PublishRequest{
		Topic:   "data",
		Payload: "just a test",
	})

	<-conn.responses // discard publish
	res := <-conn.responses

	if res.ID != "pandemic1" {
		t.Error("expected res.ID to be pandemic")
	}

	if res.Error != "" {
		t.Error(res.Error)
	}
}
