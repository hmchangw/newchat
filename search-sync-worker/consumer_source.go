package main

import (
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Marz32onE/instrumentation-go/otel-nats/oteljetstream"
)

// msgFetcher is the subset of a JetStream pull consumer that runConsumer needs,
// normalized so one loop drives both the otel-wrapped local consumers and the
// raw domain-scoped HR consumer. Both adapters yield raw jetstream.Msg, which is
// exactly what handler.Add consumes.
type msgFetcher interface {
	Fetch(n int, opts ...jetstream.FetchOpt) (msgBatch, error)
}

// msgBatch yields already-unwrapped raw jetstream.Msg values for one Fetch.
type msgBatch interface {
	Messages() <-chan jetstream.Msg
}

// Compile-time checks that both adapters satisfy msgFetcher. runConsumer
// (Task 2) will hold a msgFetcher rather than either concrete adapter type.
var (
	_ msgFetcher = rawConsumerAdapter{}
	_ msgFetcher = otelConsumerAdapter{}
)

// rawConsumerAdapter wraps a raw (domain-scoped) jetstream.Consumer. A
// jetstream.MessageBatch already yields raw jetstream.Msg, so it satisfies
// msgBatch directly and the batch passes through unchanged.
type rawConsumerAdapter struct{ c jetstream.Consumer }

func (a rawConsumerAdapter) Fetch(n int, opts ...jetstream.FetchOpt) (msgBatch, error) {
	b, err := a.c.Fetch(n, opts...)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// otelConsumerAdapter wraps an oteljetstream.Consumer. Its batch yields
// oteljetstream.Msg (which embeds jetstream.Msg plus a trace context); otelBatch
// unwraps each to the embedded raw interface.
type otelConsumerAdapter struct{ c oteljetstream.Consumer }

func (a otelConsumerAdapter) Fetch(n int, opts ...jetstream.FetchOpt) (msgBatch, error) {
	b, err := a.c.Fetch(n, opts...)
	if err != nil {
		return nil, err
	}
	return otelBatch{b}, nil
}

// otelBatch re-channels an oteljetstream.MessageBatch as raw jetstream.Msg. The
// goroutine is leak-safe: runConsumer always drains Messages() to completion, so
// when the source channel closes the goroutine closes out and exits.
type otelBatch struct{ b oteljetstream.MessageBatch }

func (o otelBatch) Messages() <-chan jetstream.Msg {
	out := make(chan jetstream.Msg)
	go func() {
		defer close(out)
		for m := range o.b.Messages() {
			out <- m.Msg
		}
	}()
	return out
}
