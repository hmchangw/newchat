package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	o11ynats "github.com/flywindy/o11y/nats"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/health"
	"github.com/hmchangw/chat/pkg/jsiter"
)

// msgFetcher is the subset of a JetStream pull consumer that runConsumer needs,
// normalized so one loop drives both the o11y-wrapped local consumers and the
// raw domain-scoped HR consumer. Both adapters yield a context/msg pair, which
// is exactly what Handler.AddWithContext consumes.
type msgFetcher interface {
	Fetch(context.Context, int, ...jetstream.FetchOpt) (msgBatch, error)
}

// msgBatch yields already-unwrapped messages for one Fetch.
//
// Error reports what went wrong during the batch, and must be read only after
// Messages is drained. It is not redundant with the error Fetch returns: a
// deleted consumer, a leadership change and a missed heartbeat all arrive
// here and never through Fetch, so a loop that skips it cannot tell a dead
// consumer from a quiet stream.
type msgBatch interface {
	Messages() <-chan o11ynats.FetchedMessage
	Error() error
}

// fetchSource is what runConsumer pulls from: a fetcher plus the decision about
// a fetch that failed.
type fetchSource interface {
	msgFetcher
	// Recover reacts to one fetch's outcome — backing off, and rebuilding the
	// consumer when the failure means the old one will never produce again. A
	// nil error reports a clean batch, which clears the failure run. It reports
	// false when consumption is over and the loop must stop.
	Recover(context.Context, error) bool
}

// Compile-time checks that both adapters satisfy msgFetcher, and that the
// recovering wrapper runConsumer actually holds satisfies fetchSource.
var (
	_ msgFetcher  = rawConsumerAdapter{}
	_ msgFetcher  = o11yConsumerAdapter{}
	_ fetchSource = (*recoveringFetcher)(nil)
)

// rawConsumerAdapter wraps a raw (domain-scoped) jetstream.Consumer. Raw NATS
// lacks the per-message consumer span that the o11y facade provides, so each
// delivered message carries the Fetch caller context.
type rawConsumerAdapter struct{ c jetstream.Consumer }

func (a rawConsumerAdapter) Fetch(ctx context.Context, n int, opts ...jetstream.FetchOpt) (msgBatch, error) {
	b, err := a.c.Fetch(n, opts...)
	if err != nil {
		return nil, err
	}
	return rawBatch{ctx: ctx, b: b}, nil
}

type rawBatch struct {
	ctx context.Context
	b   jetstream.MessageBatch
}

func (r rawBatch) Error() error { return r.b.Error() }

func (r rawBatch) Messages() <-chan o11ynats.FetchedMessage {
	out := make(chan o11ynats.FetchedMessage)
	go func() {
		defer close(out)
		for m := range r.b.Messages() {
			out <- o11ynats.FetchedMessage{Ctx: r.ctx, Msg: m}
		}
	}()
	return out
}

// o11yConsumerAdapter wraps an o11y Consumer. Its batch already yields
// FetchedMessage values with the receive-span context, so it passes through.
type o11yConsumerAdapter struct{ c o11ynats.Consumer }

func (a o11yConsumerAdapter) Fetch(ctx context.Context, n int, opts ...jetstream.FetchOpt) (msgBatch, error) {
	return a.c.Fetch(ctx, n, opts...)
}

// buildFetcher opens a fetcher over a freshly resolved consumer. It runs again
// on every rebuild, so it must re-resolve rather than close over a handle the
// server may already have dropped.
type buildFetcher func(context.Context) (msgFetcher, error)

// transientEscalation caps how many recoverable failures may arrive back to
// back before the consumer is rebuilt anyway. A consumer the server has stopped
// feeding looks exactly like one that keeps missing heartbeats, and retrying
// forever against it is the silent stall in a new shape.
const transientEscalation = 3

// recoveringFetcher keeps a pull consumer fetching across the failures a bare
// Fetch loop swallows.
//
// The HR collection fetches across a JetStream domain on another site, so its
// pull requests, expiries and heartbeats all cross a gateway. When that link
// flaps the server may drop the consumer; every later Fetch then returns an
// empty batch and a nil error, and a loop that only inspects the Fetch error
// indexes nothing for the rest of the pod's life while looking perfectly busy.
type recoveringFetcher struct {
	name  string
	build buildFetcher
	stop  <-chan struct{}

	// sleepFn parks for one backoff step, reporting false when shutdown or
	// context cancellation cut the wait short. It is a field so tests can drive
	// recovery without real delays; the default sleeps for the step it is given.
	sleepFn func(context.Context, time.Duration) bool

	mu         sync.Mutex
	cur        msgFetcher
	transients int

	up atomic.Bool
}

// newRecoveringFetcher opens the first fetcher. stop may be nil; when supplied
// it aborts an in-flight backoff so shutdown is not held up by a peer that is
// still down.
func newRecoveringFetcher(ctx context.Context, name string, build buildFetcher, stop <-chan struct{}) (*recoveringFetcher, error) {
	r := &recoveringFetcher{name: name, build: build, stop: stop}
	r.sleepFn = r.sleep

	f, err := build(ctx)
	if err != nil {
		return nil, fmt.Errorf("build %s consumer fetcher: %w", name, err)
	}
	r.cur = f
	r.up.Store(true)
	return r, nil
}

// Fetch delegates to the current fetcher. A fetch that returns without error
// clears the transient run — the consumer is demonstrably still answering.
func (r *recoveringFetcher) Fetch(ctx context.Context, n int, opts ...jetstream.FetchOpt) (msgBatch, error) {
	r.mu.Lock()
	cur := r.cur
	r.mu.Unlock()

	// A fetch that merely returned is not progress: Fetch hands back a batch and
	// a nil error even when the server-side failure is waiting in batch.Error().
	// Only Recover, which sees that outcome, clears the transient run.
	return cur.Fetch(ctx, n, opts...)
}

// Recover applies the outcome of one fetch, reporting whether the loop should
// keep going. A nil err means the batch completed cleanly — the consumer is
// demonstrably still answering, so it clears the recoverable-failure run.
func (r *recoveringFetcher) Recover(ctx context.Context, err error) bool {
	if err == nil {
		r.mu.Lock()
		r.transients = 0
		r.mu.Unlock()
		return true
	}

	disposition := jsiter.Classify(err)

	if disposition == jsiter.Transient {
		r.mu.Lock()
		r.transients++
		attempts := r.transients
		r.mu.Unlock()

		if attempts < transientEscalation {
			slog.WarnContext(ctx, "search consumer hit a recoverable fetch error, retrying",
				"consumer", r.name, "attempt", attempts, "error", err)
			return r.sleepFn(ctx, jsiter.RebuildBackoff[0])
		}
		slog.WarnContext(ctx, "search consumer kept failing without fetching, rebuilding",
			"consumer", r.name, "attempts", attempts, "error", err)
		disposition = jsiter.Fatal
	}

	if disposition == jsiter.Stopped {
		r.up.Store(false)
		slog.ErrorContext(ctx, "search consumer ended consumption",
			"consumer", r.name, "error", err)
		return false
	}

	slog.ErrorContext(ctx, "search consumer stopped, rebuilding", "consumer", r.name, "error", err)
	return r.rebuild(ctx)
}

// IsUp reports whether the wrapper currently holds a usable fetcher. It goes
// false while a rebuild is in flight and stays false once consumption ended.
//
// Deliberately not "time since the last document": a collection with nothing to
// index is healthy, and treating quiet as stalled would restart idle workers.
func (r *recoveringFetcher) IsUp() bool { return r.up.Load() }

// HealthCheck reports this consumer's state as a dependency probe. Wire it next
// to the NATS connection check: the connection being up says nothing about
// whether this consumer is still fetching, which is the failure that hides.
func (r *recoveringFetcher) HealthCheck() health.Check {
	return health.Check{
		Name: "jetstream-consumer:" + r.name,
		Probe: func(context.Context) error {
			if !r.IsUp() {
				return fmt.Errorf("%s consumer fetcher is down", r.name)
			}
			return nil
		},
	}
}

// rebuild replaces a consumer the server stopped, retrying on backoff until it
// succeeds. Giving up would leave the collection silently indexing nothing,
// which is the bug; a peer site that is down comes back.
func (r *recoveringFetcher) rebuild(ctx context.Context) bool {
	r.up.Store(false)

	for attempt := 0; ; attempt++ {
		if !r.sleepFn(ctx, backoffStep(attempt)) {
			return false
		}

		f, err := r.build(ctx)
		if err != nil {
			slog.ErrorContext(ctx, "search consumer rebuild failed, retrying",
				"consumer", r.name, "attempt", attempt+1, "error", err)
			continue
		}

		r.mu.Lock()
		r.cur = f
		r.transients = 0
		r.mu.Unlock()
		r.up.Store(true)
		slog.InfoContext(ctx, "search consumer rebuilt", "consumer", r.name, "attempts", attempt+1)
		return true
	}
}

// backoffStep returns the rebuild schedule entry for attempt, reusing the last
// one once attempts run past the schedule.
func backoffStep(attempt int) time.Duration {
	if attempt >= len(jsiter.RebuildBackoff) {
		attempt = len(jsiter.RebuildBackoff) - 1
	}
	return jsiter.RebuildBackoff[attempt]
}

// sleep parks for a jittered d, reporting false when shutdown or context
// cancellation cut the wait short. Jittered because a gateway outage fails every
// worker and collection at once, and an unjittered schedule would then aim their
// CreateOrUpdateConsumer calls at the recovering control plane in lockstep.
func (r *recoveringFetcher) sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(jsiter.Jitter(d))
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-r.stop:
		return false
	case <-ctx.Done():
		return false
	}
}
