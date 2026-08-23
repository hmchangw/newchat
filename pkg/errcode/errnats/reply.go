// Package errnats adapts errcode.Error to NATS request/reply responses.
package errnats

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/errcode"
)

const fallback = `{"code":"internal","error":"internal error"}`

// oversizeEnvelope is built once at init: it is the reply of last resort after
// a response already overflowed max_payload, so it must be small enough to
// always fit (pinned under NATS' 1024-byte floor by a test).
var oversizeEnvelope = MarshalQuiet(errcode.Internal(
	"response payload exceeds maximum size",
	errcode.WithReason(errcode.ResponseTooLarge),
))

// IsOversize reports whether err is the broker refusing a payload larger than
// max_payload. nats.go rejects the publish synchronously, before anything
// reaches the wire, so the reply subject is still live and a smaller envelope
// can still be delivered.
func IsOversize(err error) bool {
	return errors.Is(err, nats.ErrMaxPayload)
}

// OversizeEnvelope returns the compact envelope to send in place of a response
// that exceeded max_payload: code internal, reason response_too_large (see
// docs/client-api.md §6). The copy is deliberate — Respond may retain the
// slice, and a shared backing array would let one caller corrupt every later
// fallback in the process.
func OversizeEnvelope() []byte {
	return bytes.Clone(oversizeEnvelope)
}

// respondOversize sends the fallback envelope after send failed with an
// oversize rejection. A no-op for any other failure: those mean the reply
// never left for a reason a second, smaller publish would hit as well.
func respondOversize(sendErr error, send func([]byte) error, subject string) {
	if !IsOversize(sendErr) {
		return
	}
	if err := send(OversizeEnvelope()); err != nil {
		slog.Error("oversize reply fallback failed", "error", err, "subject", subject)
	}
}

// Marshal classifies err (logging it once) and returns the JSON envelope.
func Marshal(ctx context.Context, err error) []byte {
	data, mErr := json.Marshal(errcode.Classify(ctx, err))
	if mErr != nil {
		return []byte(fallback)
	}
	return data
}

// MarshalQuiet returns the envelope WITHOUT logging. Use only on paths that
// already logged the failure (panic backstop, admission/replyBusy).
func MarshalQuiet(err error) []byte {
	var e *errcode.Error
	if !errors.As(err, &e) {
		e = errcode.Internal("internal error")
	}
	data, mErr := json.Marshal(e)
	if mErr != nil {
		return []byte(fallback)
	}
	return data
}

// Reply classifies err (logging once) and sends the envelope on msg's reply subject.
func Reply(ctx context.Context, msg *nats.Msg, err error) {
	if rErr := msg.Respond(Marshal(ctx, err)); rErr != nil {
		slog.ErrorContext(ctx, "error reply failed", "error", rErr, "subject", msg.Subject)
		respondOversize(rErr, msg.Respond, msg.Subject)
	}
}

// ReplyQuiet sends the envelope WITHOUT logging the failure (see MarshalQuiet).
func ReplyQuiet(msg *nats.Msg, err error) {
	if rErr := msg.Respond(MarshalQuiet(err)); rErr != nil {
		slog.Error("error reply failed", "error", rErr, "subject", msg.Subject)
		respondOversize(rErr, msg.Respond, msg.Subject)
	}
}
