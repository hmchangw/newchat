package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type submitter interface {
	Submit(ev CDCEvent)
}

type watcher struct {
	js      jetstream.JetStream
	stream  string
	startAt time.Time
	sub     submitter
	live    atomic.Bool
}

//nolint:unused // wired into main.go's dependency graph by a later task
func newWatcher(js jetstream.JetStream, streamName string, startAt time.Time, sub submitter) *watcher {
	return &watcher{js: js, stream: streamName, startAt: startAt, sub: sub}
}

// orderedConfig maps the optional replay start onto the ordered-consumer
// deliver policy: zero time = live tail (new messages only).
func orderedConfig(startAt time.Time) jetstream.OrderedConsumerConfig {
	if startAt.IsZero() {
		return jetstream.OrderedConsumerConfig{DeliverPolicy: jetstream.DeliverNewPolicy}
	}
	return jetstream.OrderedConsumerConfig{
		DeliverPolicy: jetstream.DeliverByStartTimePolicy,
		OptStartTime:  &startAt,
	}
}

func (w *watcher) Run(ctx context.Context) error {
	cons, err := w.js.OrderedConsumer(ctx, w.stream, orderedConfig(w.startAt))
	if err != nil {
		return fmt.Errorf("create ordered consumer on %s: %w", w.stream, err)
	}
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		w.handleMsg(msg.Data(), msg.Subject())
	})
	if err != nil {
		return fmt.Errorf("consume from %s: %w", w.stream, err)
	}
	w.live.Store(true)
	defer func() {
		w.live.Store(false)
		cc.Stop()
	}()
	<-ctx.Done()
	return nil
}

func (w *watcher) handleMsg(data []byte, subject string) {
	ev, err := decodeCDCEvent(data)
	if err != nil {
		// Subject only — the payload may hold user content and must not be logged.
		slog.Warn("skip undecodable oplog event", "subject", subject, "error", err)
		return
	}
	w.sub.Submit(ev)
}

func (w *watcher) Live() bool { return w.live.Load() }
