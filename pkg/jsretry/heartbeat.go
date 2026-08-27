package jsretry

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// minHeartbeatInterval floors the derived interval so a short AckWait cannot
// produce a heartbeat so frequent it becomes its own load.
const minHeartbeatInterval = time.Second

// InProgressMsg is the subset of the JetStream message API the heartbeat needs.
// jetstream.Msg satisfies it, as do the wrappers that embed it.
type InProgressMsg interface {
	InProgress() error
}

// HeartbeatInterval derives a heartbeat period from a consumer's AckWait: a
// third of the budget, so two consecutive heartbeats can be lost before the
// server considers the message un-acked. A non-positive AckWait returns 0,
// which disables the heartbeat.
func HeartbeatInterval(ackWait time.Duration) time.Duration {
	if ackWait <= 0 {
		return 0
	}
	return max(ackWait/3, minHeartbeatInterval)
}

// Heartbeat extends msg's ack deadline every interval until the returned stop
// is called or ctx is done, and returns stop. Callers must defer stop so the
// goroutine always terminates, including on the panic path.
//
// Without this, a handler that runs longer than the consumer's AckWait is
// redelivered while it is still working, and a second worker begins executing
// the same job concurrently — for room mutations that means duplicate key
// rotations and duplicate fan-out. A non-positive interval disables it.
//
// A failing InProgress ends the loop: it means the message is no longer live
// (already settled, or the consumer is gone), so retrying every interval would
// only spam the log.
func Heartbeat(ctx context.Context, msg InProgressMsg, every time.Duration) (stop func()) {
	if every <= 0 {
		return func() {}
	}
	done := make(chan struct{})
	var once sync.Once
	stop = func() { once.Do(func() { close(done) }) }

	go func() {
		ticker := time.NewTicker(every)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := msg.InProgress(); err != nil {
					slog.DebugContext(ctx, "ack heartbeat stopped; message no longer in flight", "error", err)
					return
				}
			}
		}
	}()
	return stop
}
