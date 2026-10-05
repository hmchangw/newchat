package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	o11ynats "github.com/flywindy/o11y/nats"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/auditarchive"
	"github.com/hmchangw/chat/pkg/jobguard"
	"github.com/hmchangw/chat/pkg/loopguard"
	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/natsutil"
)

// maxFetchWait bounds one Fetch so the loop re-checks stop, the fill interval
// and the context at least this often.
const maxFetchWait = time.Second

// defaultFetchRetry is the pause after a non-terminal Fetch error, so a
// connection that fails instantly does not turn the loop into a hot spin.
const defaultFetchRetry = time.Second

// msgFetcher is the subset of a JetStream pull consumer the lane needs,
// normalized so a raw jetstream.Consumer (tests) and the o11y consumer
// (production) both fit.
type msgFetcher interface {
	Fetch(ctx context.Context, n int, opts ...jetstream.FetchOpt) (msgBatch, error)
}

// msgBatch yields already-unwrapped messages for one Fetch.
type msgBatch interface {
	Messages() <-chan o11ynats.FetchedMessage
	// Error is the batch's terminal error, valid once Messages has closed.
	// nats.go reports a deleted consumer, a bad request, no responders and a
	// missing heartbeat here rather than from Fetch.
	Error() error
}

// rawConsumerAdapter wraps a raw jetstream.Consumer; each delivered message
// carries the Fetch caller's context.
type rawConsumerAdapter struct{ c jetstream.Consumer }

func (a rawConsumerAdapter) Fetch(ctx context.Context, n int, opts ...jetstream.FetchOpt) (msgBatch, error) {
	b, err := a.c.Fetch(n, opts...)
	if err != nil {
		return nil, fmt.Errorf("fetch from consumer: %w", err)
	}
	return rawBatch{b: b, ctx: ctx}, nil
}

type rawBatch struct {
	b   jetstream.MessageBatch
	ctx context.Context
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

// o11yConsumerAdapter wraps an o11y Consumer, whose batch already carries the
// per-message receive-span context.
type o11yConsumerAdapter struct{ c o11ynats.Consumer }

func (a o11yConsumerAdapter) Fetch(ctx context.Context, n int, opts ...jetstream.FetchOpt) (msgBatch, error) {
	b, err := a.c.Fetch(ctx, n, opts...)
	if err != nil {
		return nil, fmt.Errorf("fetch from o11y consumer: %w", err)
	}
	return b, nil
}

type builder func(ctx context.Context, site string, msg jetstream.Msg, data []byte, c *auditarchive.Cipher, now time.Time) (item, error)

type laneConfig struct {
	site         string
	name         string
	fetchBatch   int
	fillInterval time.Duration
	now          func() time.Time
	metrics      *metrics // optional: nil records nothing
}

type lane struct {
	cfg        laneConfig
	fetcher    msgFetcher
	build      builder
	cipher     *auditarchive.Cipher
	batcher    *batcher
	flusher    *flusher
	guard      *loopguard.Guard
	fetchRetry time.Duration
	newTimer   func(time.Duration) *time.Timer // swapped in tests to observe the pause
	inFlight   sync.WaitGroup
	slot       chan struct{} // one background flush at a time
}

func newLane(cfg laneConfig, fetcher msgFetcher, build builder, cipher *auditarchive.Cipher, b *batcher, f *flusher, guard *loopguard.Guard) *lane {
	return &lane{
		cfg: cfg, fetcher: fetcher, build: build, cipher: cipher, batcher: b, flusher: f, guard: guard,
		fetchRetry: defaultFetchRetry, newTimer: time.NewTimer, slot: make(chan struct{}, 1),
	}
}

// terminalFetchErr reports errors no further Fetch can recover from; anything
// else (timeouts, a missing heartbeat, a dropped connection) is retried.
func terminalFetchErr(err error) bool {
	return errors.Is(err, jetstream.ErrConsumerNotFound) || errors.Is(err, jetstream.ErrConsumerDeleted) ||
		errors.Is(err, jetstream.ErrStreamNotFound) || errors.Is(err, jetstream.ErrBadRequest)
}

// benignBatchErr reports batch errors that just mean "nothing arrived this
// round"; the next Fetch resumes on an intact consumer.
func benignBatchErr(err error) bool {
	return err == nil || errors.Is(err, nats.ErrTimeout) || errors.Is(err, jetstream.ErrNoMessages) || natsmetrics.Recoverable(err)
}

// run is the consume loop: fetch up to fetchBatch, build each message (poison
// is Termed, skip is Acked), add it to the batcher, and seal+flush when a
// bound trips or the fill interval is due. One flush runs in the background
// while the next batch fills. On stop or context cancel it drains; a terminal
// fetch error (consumer deleted/not found) drains, reports guard.Stopped(err)
// and returns.
func (l *lane) run(ctx context.Context, stopCh <-chan struct{}, doneCh chan<- struct{}) {
	defer close(doneCh)
	// Flushes outlive a cancelled ctx: the drain must still write and settle
	// what was fetched, and flusher applies its own per-call timeouts.
	flushCtx := context.WithoutCancel(ctx)
	drain := func() {
		l.flushTaken(flushCtx)
		l.inFlight.Wait()
	}
	fetchWait := maxFetchWait
	if l.cfg.fillInterval > 0 {
		fetchWait = min(fetchWait, l.cfg.fillInterval)
	}

	for {
		select {
		case <-stopCh:
			drain()
			return
		case <-ctx.Done():
			drain()
			return
		default:
		}
		batch, err := l.fetcher.Fetch(ctx, l.cfg.fetchBatch, jetstream.FetchMaxWait(fetchWait))
		if err == nil {
			for fm := range batch.Messages() {
				l.handle(fm.Ctx, flushCtx, fm.Msg)
			}
			// Only the batch's own error can be benign: a Fetch call that
			// itself fails always takes the pause below.
			if err = batch.Error(); benignBatchErr(err) {
				l.flushIfDue(flushCtx)
				continue
			}
		}
		select {
		case <-stopCh:
			drain()
			return
		case <-ctx.Done():
			drain()
			return
		default:
		}
		if terminalFetchErr(err) {
			drain()
			l.guard.Stopped(err)
			return
		}
		slog.WarnContext(ctx, "fetch failed, retrying", "lane", l.cfg.name, "error", err)
		l.flushIfDue(flushCtx)
		l.pause(ctx, stopCh)
	}
}

// pause waits fetchRetry, or less if the lane is told to stop, so a consumer
// that fails instantly cannot turn the loop into a hot spin.
func (l *lane) pause(ctx context.Context, stopCh <-chan struct{}) {
	t := l.newTimer(l.fetchRetry)
	defer t.Stop()
	select {
	case <-t.C:
	case <-stopCh:
	case <-ctx.Done():
	}
}

func (l *lane) flushIfDue(flushCtx context.Context) {
	if l.batcher.due(l.cfg.now()) {
		l.flushTaken(flushCtx)
	}
}

// handle builds one message and settles it immediately when it is poison,
// skipped or unbuildable; otherwise it joins the batch, flushing when a count
// or byte bound trips.
func (l *lane) handle(ctx, flushCtx context.Context, msg jetstream.Msg) {
	jobguard.Guard("archive build "+l.cfg.name, func() {
		noteRedelivery(l.cfg.metrics, msg)
		data, err := natsutil.DecodePayload(msg)
		if err != nil {
			slog.ErrorContext(ctx, "undecodable payload, terminating", "lane", l.cfg.name, "subject", msg.Subject(), "error", err)
			l.term(ctx, msg)
			return
		}
		it, err := l.build(ctx, l.cfg.site, msg, data, l.cipher, l.cfg.now())
		switch {
		case errors.Is(err, errSkip):
			natsutil.Ack(msg, "not archived: "+err.Error())
		case errors.Is(err, errPoison):
			slog.ErrorContext(ctx, "poison event, terminating", "lane", l.cfg.name, "subject", msg.Subject(), "error", err)
			l.term(ctx, msg)
		case err != nil:
			slog.ErrorContext(ctx, "build failed, will redeliver", "lane", l.cfg.name, "subject", msg.Subject(), "error", err)
			l.flusher.nakAll(ctx, []item{{ctx: ctx, msg: msg}}, "build failed")
		default:
			if l.batcher.add(it, l.cfg.now()) {
				l.flushTaken(flushCtx)
			}
		}
	})
}

func (l *lane) term(ctx context.Context, msg jetstream.Msg) {
	if err := msg.Term(); err != nil {
		slog.ErrorContext(ctx, "term failed", "lane", l.cfg.name, "error", err)
	}
}

// flushTaken seals whatever the batcher holds and flushes it on a background
// goroutine. The slot admits one flush at a time, so a slow archive applies
// backpressure to the fetch loop instead of piling up unbounded batches.
func (l *lane) flushTaken(ctx context.Context) {
	items := l.batcher.take()
	if items == nil {
		return
	}
	s, err := seal(l.cfg.site, l.cfg.name, items, l.cfg.now())
	if err != nil {
		slog.ErrorContext(ctx, "seal failed, releasing batch", "lane", l.cfg.name, "error", err)
		l.flusher.nakAll(ctx, items, "seal failed")
		return
	}
	l.slot <- struct{}{}
	l.inFlight.Add(1)
	go func() {
		defer l.inFlight.Done()
		defer func() { <-l.slot }()
		jobguard.Guard("archive flush "+l.cfg.name, func() { l.flusher.flush(ctx, s) })
	}()
}
