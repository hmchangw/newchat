package jsiter

import (
	"context"
	"errors"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// ErrStopped is returned by Next once Stop has been called.
var ErrStopped = errors.New("consumer pump stopped")

// Disposition is what a caller should do about an error from a consumer.
type Disposition int

const (
	// Stopped means consumption is over; nothing can be built against it.
	Stopped Disposition = iota
	// Transient means the consumer is still live — try again.
	Transient
	// Fatal means the consumer was stopped and a new one must be built.
	Fatal
)

// Classify maps a consumer error onto the action it calls for. Unrecognized
// errors are Fatal: a needless rebuild costs one API call, while mistaking a
// dead consumer for a live one costs every message until the pod restarts.
func Classify(err error) Disposition {
	switch {
	case err == nil:
		return Transient
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return Stopped
	case errors.Is(err, jetstream.ErrConnectionClosed), errors.Is(err, nats.ErrConnectionClosed):
		return Stopped
	case errors.Is(err, jetstream.ErrNoHeartbeat), errors.Is(err, nats.ErrTimeout):
		return Transient
	// ErrMsgIteratorClosed lands here: Pump checks its own stop signal first, so
	// an unexplained closure means the subscription died underneath.
	default:
		return Fatal
	}
}
