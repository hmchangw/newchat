package jsretry

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// minHeartbeatInterval stops a short AckWait making the heartbeat its own load.
const minHeartbeatInterval = time.Second

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

// Heartbeat extends msg's ack deadline until stop (defer it) or ctx is done, so
// a slow handler is not redelivered into a second worker running the same job.
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
