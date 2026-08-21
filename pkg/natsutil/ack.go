package natsutil

import "log/slog"

// Acker is the minimal JetStream message interface the Ack helper needs.
// Both `jetstream.Msg` (nats.go) and otel-wrapped variants
// (e.g. `oteljetstream.Msg`) satisfy it.
type Acker interface {
	Ack() error
}

// Ack acks `msg` and logs any failure under a consistent structured-log
// shape (`reason` + `error`). `reason` is a short label describing WHY
// the message is being acked — e.g. "handler succeeded", "filtered",
// "malformed payload" — so operators can query logs by cause without
// parsing free-text phrases.
//
// Use this from every JetStream consumer in the repo rather than hand-rolling
// an `if err := msg.Ack(); err != nil { slog.Error(...) }` block. Consolidating
// the pattern gives us one place to add tracing spans, metrics counters, or
// delivery-context fields later, and keeps log keys consistent across services.
//
// There is deliberately no Nak counterpart here: a bare msg.Nak() is an instant
// redelivery that ignores the consumer's BackOff schedule
// (nats-server/server/consumer.go:3308-3311), so a brief downstream blip burns
// MaxDeliver in milliseconds. Use jsretry.Nak instead.
func Ack(msg Acker, reason string) {
	if err := msg.Ack(); err != nil {
		slog.Error("ack failed", "reason", reason, "error", err)
	}
}
