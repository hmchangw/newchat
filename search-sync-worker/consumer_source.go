package main

import (
	"context"
	"errors"

	o11ynats "github.com/flywindy/o11y/nats"
	"github.com/nats-io/nats.go/jetstream"
)

// msgFetcher is the subset of a JetStream pull consumer that runConsumer needs,
// normalized so one loop drives both the o11y-wrapped local consumers and the
// raw domain-scoped HR consumer. Both adapters yield a context/msg pair, which
// is exactly what Handler.AddWithContext consumes.
type msgFetcher interface {
	Fetch(context.Context, int, ...jetstream.FetchOpt) (msgBatch, error)
}

// msgBatch yields already-unwrapped messages for one Fetch. Error is readable
// only after Messages has been drained, and is the only place a deleted
// consumer, a leadership change or a failed pull shows up — Fetch itself
// reports none of them, and keeps handing back empty batches and a nil error.
type msgBatch interface {
	Messages() <-chan o11ynats.FetchedMessage
	Error() error
}

// batchErrorIsTerminal reports whether a batch error means this consumer will
// never produce again, so the loop must stop rather than keep pulling at it.
//
// Unrecognised errors count as terminal: an error that reaches here at all is
// already narrow, since nats.go keeps an idle poll clean — it withholds
// ErrTimeout, ErrNoMessages and ErrMaxBytesExceeded from the batch error
// (pull.go:944-948) — and a loop that mistook a real death for noise would be
// the silent stall this check exists to end.
func batchErrorIsTerminal(err error) bool {
	switch {
	case err == nil:
		return false
	// The consumer is still live: nats.go raises this from a timer, and the next
	// Fetch opens its own pull request and subscription. Unreachable at the
	// one-second FetchMaxWait this loop uses, since nats.go disables the
	// heartbeat below a ten-second expiry (pull.go:840-845), but it is the one
	// recoverable value the field can carry if that wait ever grows.
	case errors.Is(err, jetstream.ErrNoHeartbeat):
		return false
	// Shutdown cancelling the fetch context, not a consumer that died.
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return false
	default:
		return true
	}
}

// Compile-time checks that both adapters satisfy msgFetcher. runConsumer
// (Task 2) will hold a msgFetcher rather than either concrete adapter type.
var (
	_ msgFetcher = rawConsumerAdapter{}
	_ msgFetcher = o11yConsumerAdapter{}
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
