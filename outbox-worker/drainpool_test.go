package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

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
	drainPool(context.Background(), iter, sem, &wg, func(context.Context, jetstream.Msg) {}, func(error) {})

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

// errPumpDied stands in for a terminal iterator error — a deleted durable, say.
var errPumpDied = errors.New("consumer deleted")

// dyingIter fails its first Next with a terminal error, the shape of a durable
// that vanished under a live consumer.
type dyingIter struct{}

func (dyingIter) Next(_ ...jetstream.NextOpt) (context.Context, jetstream.Msg, error) {
	return nil, nil, errPumpDied
}
func (dyingIter) Stop()  {}
func (dyingIter) Drain() {}

// TestDrainPool_ReportsTerminalErrorToStopped pins that the pump hands its
// terminal error to the stopped hook: nothing else observes the pump exiting,
// and the guard behind that hook is what fails readiness and restarts the pod.
func TestDrainPool_ReportsTerminalErrorToStopped(t *testing.T) {
	got := make(chan error, 1)
	sem := make(chan struct{}, 1)
	var wg sync.WaitGroup
	drainPool(context.Background(), dyingIter{}, sem, &wg, func(context.Context, jetstream.Msg) {},
		func(err error) { got <- err })
	wg.Wait()

	select {
	case err := <-got:
		if !errors.Is(err, errPumpDied) {
			t.Fatalf("stopped hook received %v, want %v", err, errPumpDied)
		}
	default:
		t.Fatal("drainPool must report its terminal error to the stopped hook")
	}
}
