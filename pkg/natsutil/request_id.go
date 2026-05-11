// request_id.go: helpers to propagate X-Request-ID between context.Context and nats.Header. Missing IDs degrade to a log gap, not a correctness failure.
package natsutil

import (
	"context"
	"log/slog"

	"github.com/nats-io/nats.go"
)

// RequestIDHeader is the canonical NATS/HTTP header for the request correlation ID.
const RequestIDHeader = "X-Request-ID"

type ctxKey int

const requestIDKey ctxKey = 0

// WithRequestID returns ctx with the request ID stored; empty id is a no-op.
func WithRequestID(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	return context.WithValue(ctx, requestIDKey, id)
}

// RequestIDFromContext returns the request ID stored in ctx, or "" if none.
func RequestIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey).(string)
	return id
}

// ContextWithRequestIDFromHeaders returns ctx augmented with X-Request-ID from headers, or ctx unchanged if absent.
func ContextWithRequestIDFromHeaders(ctx context.Context, headers nats.Header) context.Context {
	if headers == nil {
		return ctx
	}
	id := headers.Get(RequestIDHeader)
	if id == "" {
		return ctx
	}
	return WithRequestID(ctx, id)
}

// HeaderForContext returns a nats.Header carrying X-Request-ID from ctx, or nil if ctx has no request ID.
func HeaderForContext(ctx context.Context) nats.Header {
	id := RequestIDFromContext(ctx)
	if id == "" {
		return nil
	}
	return nats.Header{RequestIDHeader: []string{id}}
}

// NewMsg builds a *nats.Msg with subj, data, and X-Request-ID drawn from ctx (nil header if no ID).
func NewMsg(ctx context.Context, subj string, data []byte) *nats.Msg {
	return &nats.Msg{
		Subject: subj,
		Data:    data,
		Header:  HeaderForContext(ctx),
	}
}

// OutboxDedupID composes a JetStream Nats-Msg-Id as base+":"+destSiteID. base
// is the X-Request-ID from ctx; falls back to payloadSeed when ctx carries no
// request ID, with a warn log so partial-deployment cases are observable.
func OutboxDedupID(ctx context.Context, destSiteID, payloadSeed string) string {
	base := RequestIDFromContext(ctx)
	if base == "" {
		slog.Warn("missing X-Request-ID; falling back to payload-derived outbox dedup base", "destSiteID", destSiteID)
		base = payloadSeed
	}
	return base + ":" + destSiteID
}
