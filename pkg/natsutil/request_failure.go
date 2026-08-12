package natsutil

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/errcode"
)

// RequestFailure classifies a NATS request/reply transport error.
//
// Two failures are availability problems rather than defects and are reported
// as such: nothing is subscribed to the subject (ErrNoResponders), and nobody
// answered in time (ErrTimeout / context.DeadlineExceeded). Both mean an
// upstream service is down, not yet started, or unreachable — not that this
// service is broken — so they classify as unavailable (HTTP 503) and tell the
// caller a retry is worthwhile.
//
// Every other error is wrapped raw and collapses to internal at the boundary,
// which is the correct treatment for a genuine fault.
//
// op describes what the caller was doing ("rooms-info rpc"), matching the
// fmt.Errorf convention it replaces. It reaches the client in the message, so
// it must not contain a token, account, or message body.
//
// The underlying error is attached with WithCause: server-side only, logged
// once by Classify, never serialised into the wire envelope. It deliberately
// does NOT go into Metadata, which is client-visible.
//
// context.Canceled is not handled here. Only admin-service maps it today, with
// a rationale specific to that idempotent call; see the spec's "Sites
// deliberately excluded".
//
// A nil err returns nil. Every caller today already guards with if err != nil,
// so that arm never fires in production — it is here because the alternative is
// silent and nasty: without it, an unguarded call would reach
// fmt.Errorf("%s: %w", op, nil) and hand back a NON-nil error reading
// "op: %!w(<nil>)", turning a success into a failure that reads like correct
// code at the call site.
func RequestFailure(op string, err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, nats.ErrNoResponders):
		return errcode.Unavailable(op+": no service responding",
			errcode.WithReason(errcode.NatsNoResponders),
			errcode.WithCause(err))
	case errors.Is(err, nats.ErrTimeout), errors.Is(err, context.DeadlineExceeded):
		return errcode.Unavailable(op+": upstream did not respond in time",
			errcode.WithReason(errcode.NatsRequestTimeout),
			errcode.WithCause(err))
	default:
		return fmt.Errorf("%s: %w", op, err)
	}
}
