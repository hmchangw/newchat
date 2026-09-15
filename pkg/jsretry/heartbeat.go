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
		ticker := time.NewTicker(every)
		defer ticker.Stop()
		var reported bool
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
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
