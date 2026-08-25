package main

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/natsutil"
)

// jetStreamPublisher is the slice of jetstream.JetStream the outbox publish
// needs. It exists so the wiring below is testable without a live server.
type jetStreamPublisher interface {
	PublishMsg(ctx context.Context, m *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error)
}

// newOutboxPublisher builds the callback pkg/outbox.Publish drives.
//
// JetStream, and blocking on the PubAck, is the whole point. This subject is
// the durable OUTBOX that outbox-worker drains, and the handler deletes the
// local subscription before it publishes — so a publish reported as successful
// but never persisted strands the remote membership with nothing left to
// retry, which is precisely what the OUTBOX exists to prevent. A core-NATS
// publish returns nil once the bytes reach the local socket buffer and would
// report exactly that false success. The two sibling producers (room-worker,
// room-service) already publish their outbox events this way.
//
// The dedup id rides as jetstream.WithMsgID so the server applies Nats-Msg-Id
// de-duplication: a redelivered removal collapses onto the existing entry
// rather than appending a second one.
func newOutboxPublisher(js jetStreamPublisher) outboxPublisher {
	return func(ctx context.Context, subj string, data []byte, msgID string) error {
		if _, err := js.PublishMsg(ctx, natsutil.NewMsg(ctx, subj, data), jetstream.WithMsgID(msgID)); err != nil {
			return fmt.Errorf("publish to %q: %w", subj, err)
		}
		return nil
	}
}
