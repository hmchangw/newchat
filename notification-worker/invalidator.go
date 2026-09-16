package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/bytedance/sonic"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/model"
)

// defaultInvalidationQueueSize bounds the buffer between the NATS reader and the
// Valkey worker. Drops above it are safe: roomsubcache's TTL reconciles staleness.
const defaultInvalidationQueueSize = 256

// memberEventIterator is the narrow o11ynats.MessagesContext surface the
// invalidator consumes, injectable by tests.
type memberEventIterator interface {
	Next(opts ...jetstream.NextOpt) (context.Context, jetstream.Msg, error)
	Stop()
}

// roomInvalidator drains canonical member events into a bounded queue and
// invalidates the member cache off the NATS dispatch path, so a slow Valkey
// never blocks consumption.
//
// Channel ownership is what makes shutdown safe: the reader goroutine is the
// ONLY sender on queue, so the reader — never the shutdown path — closes it.
// Closing from outside raced a reader parked between Next() returning and its
// send, which panics the process with "send on closed channel".
type roomInvalidator struct {
	iter       memberEventIterator
	queue      chan string
	invalidate func(ctx context.Context, roomID string)

	readerDone chan struct{}
	workerDone chan struct{}
	cancel     context.CancelFunc
	stopOnce   sync.Once
}

// newRoomInvalidator starts the reader and drain goroutines. Both are tracked by
// completion channels, so Stop can prove neither outlives it.
func newRoomInvalidator(ctx context.Context, iter memberEventIterator, invalidate func(context.Context, string), queueSize int) *roomInvalidator {
	if queueSize <= 0 {
		queueSize = defaultInvalidationQueueSize
	}
	workerCtx, cancel := context.WithCancel(ctx)
	r := &roomInvalidator{
		iter:       iter,
		queue:      make(chan string, queueSize),
		invalidate: invalidate,
		readerDone: make(chan struct{}),
		workerDone: make(chan struct{}),
		cancel:     cancel,
	}
	go r.readLoop()
	go r.workLoop(workerCtx)
	return r
}

// readLoop is the sole sender on queue and closes it on exit — the handshake
// that lets workLoop's range terminate without anyone closing the channel from
// another goroutine.
func (r *roomInvalidator) readLoop() {
	defer close(r.readerDone)
	defer close(r.queue)

	for {
		_, msg, err := r.iter.Next()
		if err != nil {
			// ErrMsgIteratorClosed is the normal stop (iter.Stop() on shutdown);
			// anything else means consumption died unexpectedly — surface it.
			if !errors.Is(err, jetstream.ErrMsgIteratorClosed) {
				slog.Error("canonical member event iterator stopped", "error", err)
			}
			return
		}

		var evt model.CanonicalMemberEvent
		if err := sonic.Unmarshal(msg.Data(), &evt); err != nil {
			// Poison: it will never decode on redelivery, so Ack-drop it.
			slog.Warn("canonical member event decode failed", "error", err)
			r.ack(msg)
			continue
		}
		if evt.RoomID != "" {
			select {
			case r.queue <- evt.RoomID:
			default:
				slog.Warn("invalidation queue full, dropping (TTL will reconcile)", "roomId", evt.RoomID)
			}
		}
		r.ack(msg)
	}
}

func (r *roomInvalidator) workLoop(ctx context.Context) {
	defer close(r.workerDone)
	for roomID := range r.queue {
		r.invalidate(ctx, roomID)
	}
}

// ack records rather than discards the Ack error: a failed Ack means the event
// redelivers and the room is invalidated twice, which is harmless but worth seeing.
func (r *roomInvalidator) ack(msg jetstream.Msg) {
	if err := msg.Ack(); err != nil {
		slog.Warn("ack canonical member event failed", "error", err)
	}
}

// Stop halts consumption and waits for both goroutines, in that order: the
// reader closes the queue on its way out, which is what allows the drain worker
// to finish the work already accepted. If stepCtx expires while the worker is
// wedged in an in-flight Valkey call, its context is cancelled to free it and
// Stop still waits, so no goroutine outlives the call. Safe to call twice.
func (r *roomInvalidator) Stop(stepCtx context.Context) error {
	r.stopOnce.Do(func() { r.iter.Stop() })
	defer r.cancel()

	select {
	case <-r.readerDone:
	case <-stepCtx.Done():
		// The reader still owes us the queue close, so the worker cannot finish.
		// Report rather than block shutdown; the process is exiting regardless.
		return fmt.Errorf("invalidation reader did not stop: %w", stepCtx.Err())
	}

	select {
	case <-r.workerDone:
	case <-stepCtx.Done():
		r.cancel()     // unblock an in-flight Valkey DEL so the worker can exit
		<-r.workerDone // bounded: the queue is closed, so the range terminates
	}
	return nil
}
