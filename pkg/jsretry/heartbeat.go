package jsretry

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// minHeartbeatInterval stops a short AckWait making the heartbeat its own load.
const minHeartbeatInterval = time.Second

// DefaultHeartbeatMax bounds extension when a caller sets no Max. Mirrors the
// HEARTBEAT_MAX struct tag on stream.ConsumerSettings, which is the operator
// knob; the two are pinned together by a test in pkg/stream.
const DefaultHeartbeatMax = 10 * time.Minute

// InProgressMsg is the subset of the JetStream message API the heartbeat needs.
type InProgressMsg interface {
	InProgress() error
}

// HeartbeatInterval derives a heartbeat period from AckWait: a third of the
// budget, so two ticks can be lost before the message counts as un-acked.
func HeartbeatInterval(ackWait time.Duration) time.Duration {
	if ackWait <= 0 {
		return 0
	}
	return max(ackWait/3, minHeartbeatInterval)
}

// HeartbeatBudget bounds one message's ack extension. Every paces the ticker
// (non-positive disables the heartbeat); Max caps the total extension.
//
// Max exists because an unbounded heartbeat converts a wedged handler from a
// bounded failure into an unbounded one: the message holds its MaxAckPending
// slot forever, is never redelivered, and never reaches MaxDeliver, so the
// server-side lever that covers a hang never fires and the stall is invisible.
// Spending the budget restores that escape hatch — the deadline lapses, the
// message redelivers, and a genuinely poisonous one is dropped and logged.
type HeartbeatBudget struct {
	Every time.Duration
	Max   time.Duration
}

// resolvedMax is the ceiling actually applied. A caller that sets no Max gets
// DefaultHeartbeatMax rather than forever: unbounded is deliberately not
// expressible, because omission is the case a caller gets wrong.
func (b HeartbeatBudget) resolvedMax() time.Duration {
	if b.Max <= 0 {
		return DefaultHeartbeatMax
	}
	return b.Max
}

// Heartbeat extends msg's ack deadline until stop (defer it), ctx is done, or
// the budget is spent, so a slow handler is not redelivered into a second worker
// running the same job — while a wedged one still eventually lets go.
func Heartbeat(ctx context.Context, msg InProgressMsg, b HeartbeatBudget) (stop func()) {
	if b.Every <= 0 {
		return func() {}
	}
	done := make(chan struct{})
	exited := make(chan struct{})
	var once sync.Once
	// stop waits for the goroutine: callers settle the message immediately
	// after, and an InProgress landing past the Ack would race it.
	stop = func() {
		once.Do(func() { close(done) })
		<-exited
	}

	go func() {
		defer close(exited)
		ticker := time.NewTicker(b.Every)
		defer ticker.Stop()
		// A handler that outlives this has stopped making progress in any way
		// we can see, so stop defending its claim on the message.
		budget := time.NewTimer(b.resolvedMax())
		defer budget.Stop()
		var reported bool
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-budget.C:
				slog.WarnContext(ctx, "ack heartbeat budget spent; letting the message redeliver",
					"budget", b.resolvedMax())
				return
			case <-ticker.C:
				// InProgress cannot distinguish a settled message from a
				// transport blip, so keep extending and log only the first
				// failure rather than giving the deadline up silently.
				if err := msg.InProgress(); err != nil && !reported {
					reported = true
					slog.DebugContext(ctx, "ack heartbeat failed; still retrying", "error", err)
				}
			}
		}
	}()
	return stop
}
