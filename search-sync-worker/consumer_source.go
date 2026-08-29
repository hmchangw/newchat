package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	o11ynats "github.com/flywindy/o11y/nats"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/health"
	"github.com/hmchangw/chat/pkg/jsiter"
)

// msgFetcher is the pull-consumer surface runConsumer needs, normalized so one
// loop drives both the o11y-wrapped local consumers and the raw HR one.
type msgFetcher interface {
	Fetch(context.Context, int, ...jetstream.FetchOpt) (msgBatch, error)
}

// msgBatch yields one Fetch's messages. Error is readable only after Messages
// is drained, and is the only place a deleted consumer, a leadership change or
// a missed heartbeat shows up — Fetch itself reports none of them.
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

// openFetcher opens a fetcher over a freshly resolved consumer. It runs again
// on every rebuild, so it must re-resolve rather than close over a handle the
// server may already have dropped.
type openFetcher func(context.Context) (msgFetcher, error)

// recoveringFetcher keeps a pull consumer fetching across the failures a bare
// Fetch loop swallows. The HR collection pulls across a gateway, so a flapping
// link can drop its consumer — after which Fetch returns empty batches and a
// nil error forever, and the pod looks busy while indexing nothing.
type recoveringFetcher struct {
	name string
	open openFetcher
	stop <-chan struct{}

	// sleepFn parks for one backoff step, reporting false when shutdown or
	// context cancellation cut the wait short. sleepFn and nowFn are fields so
	// tests drive backoff and the escalation window without real time.
	sleepFn func(context.Context, time.Duration) bool
	nowFn   func() time.Time

	// Everything below belongs to the one runConsumer goroutine that calls
	// Fetch and Recover, so it needs no lock. Only up is shared, with the
	// health server.
	cur        msgFetcher
	transients int
	// attempt indexes the backoff across consecutive rebuilds, so a consumer
	// that rebuilds cleanly and fails again at once keeps backing off instead of
	// hammering the first step against a degraded control plane. Windowed by
	// lastAttempt: an empty poll is not proof the replacement works, so only a
	// quiet stretch clears the run.
	attempt     int
	lastAttempt time.Time

	up atomic.Bool
}

// newRecoveringFetcher opens the first fetcher. stop may be nil; when supplied
// it aborts an in-flight backoff so shutdown is not held up by a peer that is
// still down.
func newRecoveringFetcher(ctx context.Context, name string, open openFetcher, stop <-chan struct{}) (*recoveringFetcher, error) {
	r := &recoveringFetcher{name: name, open: open, stop: stop, nowFn: time.Now}
	r.sleepFn = func(ctx context.Context, d time.Duration) bool {
		return jsiter.SleepUntil(ctx, r.stop, d)
	}

	f, err := open(ctx)
	if err != nil {
		return nil, fmt.Errorf("open %s consumer fetcher: %w", name, err)
	}
	r.cur = f
	r.up.Store(true)
	return r, nil
}

// Fetch delegates to the current fetcher, and deliberately clears nothing: a
// fetch that merely returned is not progress, because Fetch hands back a batch
// and a nil error even when the failure is waiting in batch.Error(). Only
// Recover sees that outcome, so only Recover clears the failure run.
func (r *recoveringFetcher) Fetch(ctx context.Context, n int, opts ...jetstream.FetchOpt) (msgBatch, error) {
	return r.cur.Fetch(ctx, n, opts...)
}

// Recover applies the outcome of one fetch, reporting whether the loop should
// keep going. A nil err means the batch completed cleanly, so the consumer
// answered and the recoverable-failure run is cleared. It does NOT clear the
// rebuild schedule: an empty poll is not proof that a replacement works, and a
// flapping leader that answers every other pull would otherwise hold every
// rebuild at the minimum delay forever. That run is cleared by time instead.
func (r *recoveringFetcher) Recover(ctx context.Context, err error) bool {
	if err == nil {
		r.transients = 0
		return true
	}

	// Stopped is the only way out; everything else rebuilds, including a run of
	// recoverable errors that never fetches.
	switch jsiter.Classify(err) {
	case jsiter.Stopped:
		r.up.Store(false)
		slog.ErrorContext(ctx, "search consumer ended consumption",
			"consumer", r.name, "error", err)
		return false

	case jsiter.Transient:
		r.transients++
		if r.transients < jsiter.TransientEscalation {
			slog.WarnContext(ctx, "search consumer hit a recoverable fetch error, retrying",
				"consumer", r.name, "attempt", r.transients, "error", err)
			return r.sleepFn(ctx, jsiter.RebuildBackoff[0])
		}
		slog.WarnContext(ctx, "search consumer kept failing without fetching, rebuilding",
			"consumer", r.name, "attempts", r.transients, "error", err)

	case jsiter.Fatal:
		// Nothing to weigh: the rebuild below is the whole response.
	}

	slog.ErrorContext(ctx, "search consumer stopped, rebuilding", "consumer", r.name, "error", err)
	return r.rebuild(ctx)
}

// IsUp reports whether a usable fetcher is held: false while rebuilding and
// after consumption ends. Deliberately not "time since the last document" — a
// collection with nothing to index is healthy, not stalled.
func (r *recoveringFetcher) IsUp() bool { return r.up.Load() }

// HealthCheck reports this consumer's state as a dependency probe. Wire it next
// to the NATS connection check: the connection being up says nothing about
// whether this consumer is still fetching, which is the failure that hides.
func (r *recoveringFetcher) HealthCheck() health.Check { return jsiter.Check(r.name, r.IsUp) }

// rebuild replaces a consumer the server stopped, retrying on backoff until it
// succeeds. Giving up would leave the collection silently indexing nothing,
// which is the bug; a peer site that is down comes back.
func (r *recoveringFetcher) rebuild(ctx context.Context) bool {
	r.up.Store(false)

	now := r.nowFn()
	attempt := jsiter.SeedAttempt(r.attempt, r.lastAttempt, now)
	r.lastAttempt = now

	for ; ; attempt++ {
		if !r.sleepFn(ctx, jsiter.BackoffStep(attempt)) {
			return false
		}

		f, err := r.open(ctx)
		if err != nil {
			slog.ErrorContext(ctx, "search consumer rebuild failed, retrying",
				"consumer", r.name, "attempt", attempt+1, "error", err)
			continue
		}

		r.cur = f
		r.transients = 0
		// attempt+1, not 0: this fetcher has merely been built, not proven.
		r.attempt = attempt + 1
		r.up.Store(true)
		slog.InfoContext(ctx, "search consumer rebuilt", "consumer", r.name, "attempts", attempt+1)
		return true
	}
}
