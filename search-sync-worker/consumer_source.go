package main

import (
	"context"

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

// msgBatch yields already-unwrapped messages for one Fetch.
type msgBatch interface {
	Messages() <-chan o11ynats.FetchedMessage
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
