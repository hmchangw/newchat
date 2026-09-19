package natsutil

import (
	"context"
	"fmt"
	"sync"

	"github.com/nats-io/nats.go/jetstream"
)

// MsgIterator is the pull-iterator surface a lane drives — the slice of
// o11y/nats MessagesContext that RunPool needs, narrowed to an interface so the
// loop is testable without a NATS server.
type MsgIterator interface {
	Next(...jetstream.NextOpt) (context.Context, jetstream.Msg, error)
	Stop()
}

// RunPool drains iter across a bounded worker pool in a background goroutine,
// returning immediately.
//
// sem bounds in-flight handlers and wg tracks them for shutdown; both are the
// caller's so a home lane and a failover lane can share one pool — a
// failover-lane message is still this site's message and deserves the same
// concurrency budget, not a second one.
//
// The loop exits when Next reports an error, which is what iter.Stop causes.
// Acking, retry, and panic recovery belong to handle, because they differ per
// service.
func RunPool(iter MsgIterator, sem chan struct{}, wg *sync.WaitGroup,
	handle func(context.Context, jetstream.Msg),
) {
	// The loop goroutine is itself counted, so shutdown — which stops the
	// iterator and then waits on wg — cannot pass through while a message Next
	// already returned is still on its way to a worker.
	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			msgCtx, msg, err := iter.Next()
			if err != nil {
				return
			}
			sem <- struct{}{}
			wg.Add(1)
			go func(msgCtx context.Context, msg jetstream.Msg) {
				defer func() {
					<-sem
					wg.Done()
				}()
				handle(msgCtx, msg)
			}(msgCtx, msg)
		}
	}()
}

// WaitPool blocks until every worker tracked by wg has finished, or ctx
// expires. A timeout means a wedged handler — surfaced as an error rather than
// a shutdown that overruns the Kubernetes grace period.
//
// Shared because every worker service needs exactly this hook, and a
// hand-rolled copy that forgets the ctx arm hangs shutdown instead of failing
// it.
func WaitPool(ctx context.Context, wg *sync.WaitGroup) error {
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("worker drain timed out: %w", ctx.Err())
	}
}

// Lane is a running pull consumer that a service can stop without a nil check.
// A nil *Lane is one that never bound — an unreachable buddy, say — so a
// service holds the same value either way and shutdown reads the same.
//
// The handlers are tracked by the caller's WaitGroup, not here, because a home
// lane and a buddy lane share one pool: waiting is WaitPool's job, once, on
// that shared group.
type Lane struct{ iter MsgIterator }

// NewLane wraps an already-running iterator, for a caller that drove RunPool
// (or its own loop) itself.
func NewLane(iter MsgIterator) *Lane { return &Lane{iter: iter} }

// Bound reports whether this lane was bound at all. A nil Lane is one that
// never was, so a readiness check can ask without a nil guard of its own.
func (l *Lane) Bound() bool { return l != nil }

// Stop halts the pull iterator so no new work is picked up. Call it on every
// lane before waiting on any of them, or a stopped lane's peers keep feeding
// the pool it shares.
func (l *Lane) Stop() {
	if l != nil {
		l.iter.Stop()
	}
}
