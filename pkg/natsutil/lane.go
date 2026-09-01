package natsutil

import (
	"context"
	"fmt"
	"sync"

	o11ynats "github.com/flywindy/o11y/nats"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/natsmetrics"
)

// laneOpts carries the optional instrumentation a lane reports through.
type laneOpts struct {
	metrics    *natsmetrics.Consumer
	maxDeliver int
}

// LaneOption configures a lane's pull loop.
type LaneOption func(*laneOpts)

// WithLaneMetrics reports this lane's loop state and per-message outcomes to c.
//
// Metrics are per-lane, not per-service: a Consumer's attributes name the stream
// and durable it was built for, so a buddy lane must be given its own rather
// than share the home lane's — otherwise failover traffic would be attributed to
// the stream it did not come from.
func WithLaneMetrics(c *natsmetrics.Consumer, maxDeliver int) LaneOption {
	return func(o *laneOpts) {
		o.metrics = c
		o.maxDeliver = maxDeliver
	}
}

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
	handle func(context.Context, jetstream.Msg), opts ...LaneOption,
) {
	var o laneOpts
	for _, apply := range opts {
		apply(&o)
	}

	// The loop goroutine is itself counted, so shutdown — which stops the
	// iterator and then waits on wg — cannot pass through while a message Next
	// already returned is still on its way to a worker.
	wg.Add(1)
	go func() {
		defer wg.Done()

		// An instrumented lane runs natsmetrics' loop rather than a second copy
		// of it, so per-message tracking, redelivery counting and terminal
		// classification stay in one place repo-wide.
		if o.metrics != nil {
			natsmetrics.ConsumeInPool(context.Background(), iter, o.metrics, sem, o.maxDeliver, wg,
				func(msg jetstream.Msg) natsmetrics.EventType {
					return natsmetrics.EventTypeFromSubject(msg.Subject())
				},
				func(msgCtx context.Context, msg *natsmetrics.Message) { handle(msgCtx, msg) })
			return
		}

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

// Lane is a running pull consumer plus the WaitGroup tracking its in-flight
// handlers — the two things shutdown needs, paired so a service holds one value
// per lane instead of an iterator and a WaitGroup it has to keep in sync.
//
// A nil *Lane is a bound lane that never came up (an unreachable buddy, say);
// Stop and Wait are no-ops on it, so shutdown needs no nil checks.
type Lane struct {
	iter MsgIterator
	wg   *sync.WaitGroup
}

// NewLane pairs an already-running iterator with the WaitGroup its handlers
// report to. Use it when the service drives its own consume loop — StartLane is
// the shorthand for the ordinary bounded-pool case.
func NewLane(iter MsgIterator, wg *sync.WaitGroup) *Lane { return &Lane{iter: iter, wg: wg} }

// StartLane binds cons and drains it across a pool of maxWorkers that the lane
// owns. Use it for a service's only lane; a service running a home lane and a
// buddy lane wants StartLaneInPool so the two share one budget.
func StartLane(ctx context.Context, cons o11ynats.Consumer, maxWorkers int,
	handle func(context.Context, jetstream.Msg), opts ...LaneOption,
) (*Lane, error) {
	var wg sync.WaitGroup
	return StartLaneInPool(ctx, cons, maxWorkers, make(chan struct{}, maxWorkers), &wg, handle, opts...)
}

// StartLaneInPool binds cons and drains it into a pool the CALLER owns, so
// several lanes share one concurrency budget and one WaitGroup.
//
// This is the right call for a buddy lane. A lane that allocates its own
// semaphore silently doubles a service's in-flight handlers the moment the
// failover lane binds — MAX_WORKERS becomes 2×MAX_WORKERS against the same
// Mongo, Cassandra and Valkey — even though the two lanes carry the same site's
// work and were sized as one budget.
//
// maxWorkers still sizes the pull batch, which is per-consumer.
func StartLaneInPool(ctx context.Context, cons o11ynats.Consumer, maxWorkers int,
	sem chan struct{}, wg *sync.WaitGroup, handle func(context.Context, jetstream.Msg),
	opts ...LaneOption,
) (*Lane, error) {
	iter, err := cons.Messages(ctx, jetstream.PullMaxMessages(2*maxWorkers))
	if err != nil {
		return nil, fmt.Errorf("bind consumer messages: %w", err)
	}
	RunPool(iter, sem, wg, handle, opts...)
	return NewLane(iter, wg), nil
}

// Stop halts the pull iterator so no new work is picked up. Call it on every
// lane before waiting on any of them, or a stopped lane's peers keep feeding
// the pool it shares.
func (l *Lane) Stop() {
	if l != nil {
		l.iter.Stop()
	}
}

// Wait blocks until every in-flight handler has finished, or ctx expires. A
// timeout means a wedged handler — surfaced as an error rather than a shutdown
// that overruns the Kubernetes grace period.
func (l *Lane) Wait(ctx context.Context) error {
	if l == nil {
		return nil
	}
	return WaitPool(ctx, l.wg)
}
