package main

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Marz32onE/instrumentation-go/otel-nats/oteljetstream"
	"github.com/nats-io/nats.go/jetstream"
)

// stubIter blocks in Next until Stop is called, then reports the iterator as
// closed — the shape of a live consumer with no traffic during shutdown.
type stubIter struct {
	stopped chan struct{}
}

func (s *stubIter) Next(_ ...jetstream.NextOpt) (context.Context, jetstream.Msg, error) {
	<-s.stopped
	return nil, nil, jetstream.ErrMsgIteratorClosed
}

func (s *stubIter) Stop()  { close(s.stopped) }
func (s *stubIter) Drain() {}

// TestDrainPool_WaitCoversPumpGoroutine pins the shutdown handshake: the pump
// goroutine itself is counted in the WaitGroup, so wg.Wait() cannot return
// while the pump is still running — a message received between iter.Next()
// and the per-message wg.Add(1) can therefore never slip past shutdown's wait.
func TestDrainPool_WaitCoversPumpGoroutine(t *testing.T) {
	iter := &stubIter{stopped: make(chan struct{})}
	sem := make(chan struct{}, 1)
	var wg sync.WaitGroup
	drainPool(context.Background(), iter, sem, &wg, func(oteljetstream.Msg) {})

	waitDone := make(chan struct{})
	go func() { wg.Wait(); close(waitDone) }()

	select {
	case <-waitDone:
		t.Fatal("wg.Wait returned while the pump goroutine was still running")
	case <-time.After(50 * time.Millisecond):
	}

	iter.Stop()
	select {
	case <-waitDone:
	case <-time.After(2 * time.Second):
		t.Fatal("wg.Wait did not return after the iterator was stopped")
	}
}
