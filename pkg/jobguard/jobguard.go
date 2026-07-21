// Package jobguard contains panic recovery for JetStream message-processing
// goroutines in the worker services.
//
// The async consumer goroutines run OUTSIDE natsrouter's Recovery middleware,
// so an unrecovered panic in a handler (e.g. an errcode WithCause/WithMetadata
// misuse, a nil deref on a malformed event) would crash the whole worker
// process. Worse, the in-flight message is left un-acked, so JetStream
// redelivers it after the pod restarts — turning a deterministic panic into a
// crash loop driven by a single poison message. jobguard contains the panic so
// the process survives.
//
// Two dispositions are offered. Per-message workers use Run, which Acks the
// message on panic (poison-pill DROP). The batch worker (search-sync-worker)
// uses Guard directly around its buffer/flush calls so a panic recovers and
// the loop CONTINUES — the affected messages stay un-acked and JetStream
// redelivers them after AckWait, avoiding a batch-wide drop.
package jobguard

import (
	"log/slog"
	"runtime/debug"
)

// Message is the minimal JetStream message surface Run needs to drop a poison
// message. Both jetstream.Msg and the o11y/nats facade's message satisfy it.
type Message interface {
	Subject() string
	Ack() error
}

// Guard runs fn and recovers from any panic, logging it with label (typically
// the message subject) for correlation. It reports whether fn panicked so
// callers can decide how to dispose of the in-flight work. Guard never
// re-panics: the calling goroutine survives regardless of what fn does.
func Guard(label string, fn func()) (panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("panic in jetstream handler — recovered",
				"panic", r, "subject", label, "stack", string(debug.Stack()))
			panicked = true
		}
	}()
	fn()
	return false
}

// Run executes process for a single self-disposing JetStream message — process
// performs its own Ack/Nak on the normal path — and contains any panic. On
// panic it Acks msg, a poison-pill drop, because a deterministic panic would
// otherwise crash-loop the worker via JetStream redelivery. This mirrors
// natsrouter.Recovery, which Acks-on-panic with an Internal reply.
func Run(msg Message, process func()) {
	if Guard(msg.Subject(), process) {
		if err := msg.Ack(); err != nil {
			slog.Error("failed to ack after panic", "error", err, "subject", msg.Subject())
			return
		}
		// Distinct from Guard's recover log so an operator can tell a poison
		// DROP (this path, Run) apart from a recover-and-REDELIVER (Guard used
		// directly, e.g. search-sync-worker's batch loop) — both share Guard's
		// recover line, but only Run disposes of the message.
		slog.Warn("dropped poison message after panic (Ack)", "subject", msg.Subject())
	}
}
