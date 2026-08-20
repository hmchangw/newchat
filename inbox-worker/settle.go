package main

import (
	"context"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/jobguard"
	"github.com/hmchangw/chat/pkg/jsretry"
	"github.com/hmchangw/chat/pkg/natsutil"
)

// inboxMsg is the subset of jetstream.Msg the settle path needs. It exposes
// NakWithDelay but deliberately NOT Nak: a delay-less Nak redelivers instantly,
// so a brief Mongo/NATS blip burns the consumer's delivery budget in
// milliseconds and the federation event is dropped (see pkg/jsretry's doc).
type inboxMsg interface {
	jsretry.Msg
	Data() []byte
	Headers() nats.Header
	Subject() string
}

// eventHandler is the handler surface processEvent drives; *Handler satisfies it.
type eventHandler interface {
	HandleEvent(ctx context.Context, data []byte) error
}

// processEvent runs one INBOX event through the handler and settles it: Ack on
// success, Ack-drop on a permanent (poison) failure, NakWithDelay on a transient
// one. jobguard contains handler panics (Ack-dropping the message) — both the
// membership lane and the fan-out goroutines run outside natsrouter's Recovery
// middleware, so an unrecovered panic would crash the worker and crash-loop on
// JetStream redelivery.
func processEvent(ctx context.Context, h eventHandler, msg inboxMsg) {
	jobguard.Run(msg, func() {
		handlerCtx, _ := natsutil.StampRequestID(ctx, msg.Headers(), msg.Subject())
		jsretry.Settle(handlerCtx, msg, jsretry.DefaultBackoff, h.HandleEvent(handlerCtx, msg.Data()))
	})
}
