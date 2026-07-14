// Package migration holds the shared consume/disposition machinery for the
// data-migration transformer services (messages and collections). Each service
// keeps its own event mapping and metrics; this package owns the policy that
// turns a handler result into a JetStream disposition.
package migration

import "errors"

// ErrPoison marks an event that can never succeed (unmappable doc). The consume
// loop Terms these instead of redelivering, so one bad event never wedges the stream.
var ErrPoison = errors.New("poison event")

// ErrSkipped marks an event the handler deliberately dropped. The consume loop Acks
// these but does NOT count them as processed — the skip is already metered by the handler.
var ErrSkipped = errors.New("event skipped")

// Action is the JetStream disposition a consume loop should apply to a message.
type Action int

const (
	// ActionAck: handler succeeded — Ack and count as processed.
	ActionAck Action = iota
	// ActionTerm: poison — Term, never redeliver.
	ActionTerm
	// ActionAckSkip: deliberate skip — Ack, do NOT count as processed.
	ActionAckSkip
	// ActionNak: transient failure — Nak for redelivery.
	ActionNak
	// ActionTermExhausted: transient failure that has hit the delivery cap — Term
	// explicitly (with an exhaustion metric) instead of letting JetStream silently drop it.
	ActionTermExhausted
)

// Classify maps a handler result to a disposition Action. isFinal reports whether this
// is the last delivery (a further Nak would be silently dropped). Poison and skip take
// precedence over isFinal — a poison/skip is terminal regardless of delivery count.
func Classify(err error, isFinal bool) Action {
	switch {
	case err == nil:
		return ActionAck
	case errors.Is(err, ErrPoison):
		return ActionTerm
	case errors.Is(err, ErrSkipped):
		return ActionAckSkip
	case isFinal:
		return ActionTermExhausted
	default:
		return ActionNak
	}
}

// IsFinalDelivery reports whether numDelivered has reached maxDeliver, so a further Nak
// would be a silent drop. maxDeliver <= 0 means unlimited (never final).
func IsFinalDelivery(numDelivered uint64, maxDeliver int) bool {
	if maxDeliver <= 0 {
		return false
	}
	return numDelivered >= uint64(maxDeliver)
}
