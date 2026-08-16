package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type submitter interface {
	Submit(ev CDCEvent)
	SubmitUndecodable(collection, op string)
}

type watcher struct {
	js      jetstream.JetStream
	stream  string
	filter  string // subject filter, chat.migration.oplog.{siteID}.>
	startAt time.Time
	sub     submitter
	live    atomic.Bool
}

func newWatcher(js jetstream.JetStream, streamName, filter string, startAt time.Time, sub submitter) *watcher {
	return &watcher{js: js, stream: streamName, filter: filter, startAt: startAt, sub: sub}
}

// orderedConfig maps the optional replay start onto the deliver policy: zero time = live tail.
func orderedConfig(startAt time.Time, filter string) jetstream.OrderedConsumerConfig {
	cfg := jetstream.OrderedConsumerConfig{
		DeliverPolicy:  jetstream.DeliverNewPolicy,
		FilterSubjects: []string{filter},
	}
	if !startAt.IsZero() {
		cfg.DeliverPolicy = jetstream.DeliverByStartTimePolicy
		cfg.OptStartTime = &startAt
	}
	return cfg
}

func (w *watcher) Run(ctx context.Context) error {
	cons, err := w.js.OrderedConsumer(ctx, w.stream, orderedConfig(w.startAt, w.filter))
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
	select {
	case <-ctx.Done():
		return nil
	case <-cc.Closed():
		// The consume loop died underneath us (connection lost past reconnect,
		// consumer deleted) — a silent wait here would leave a lying dashboard.
		return fmt.Errorf("consume loop on %s closed unexpectedly", w.stream)
	}
}

// parseOplogSubject splits chat.migration.oplog.{siteID}.{collection}.{op};
// the subject is the authoritative routing key, not the payload.
func parseOplogSubject(subj string) (collection, op string, ok bool) {
	parts := strings.Split(subj, ".")
	if len(parts) != 6 || parts[0] != "chat" || parts[1] != "migration" || parts[2] != "oplog" {
		return "", "", false
	}
	return parts[4], parts[5], true
}

func (w *watcher) handleMsg(data []byte, subj string) {
	collection, op, ok := parseOplogSubject(subj)
	if !ok {
		// Subject only — the payload may hold user content and must not be logged.
		slog.Warn("skip event on unrecognized subject", "subject", subj)
		return
	}
	ev, err := decodeCDCEvent(data)
	if err != nil {
		slog.Warn("undecodable oplog event", "subject", subj, "error", err)
		w.sub.SubmitUndecodable(collection, op)
		return
	}
	if ev.Collection != collection || ev.Op != op {
		slog.Warn("oplog payload disagrees with subject; subject wins",
			"subject", subj, "payload_collection", ev.Collection, "payload_op", ev.Op)
	}
	ev.Collection, ev.Op = collection, op
	w.sub.Submit(ev)
}

func (w *watcher) Live() bool { return w.live.Load() }
