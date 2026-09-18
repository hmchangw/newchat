// Package reqid owns the context key the X-Request-ID correlation value travels
// under, and nothing else.
//
// It exists as a leaf so packages on both sides of the NATS helpers can read the
// id without depending on each other: pkg/natsutil owns the header plumbing and
// re-exports these two as WithRequestID/RequestIDFromContext, while pkg/jsretry
// only needs to name the id in a log line and must stay below natsutil.
package reqid

import "context"

// ctxKey is unexported so no other package can write or read this value by
// constructing an equal key.
type ctxKey int

const requestIDKey ctxKey = 0

// With returns ctx carrying id. An empty id is a no-op, so a caller that mints
// on missing can call it unconditionally without erasing an id already there.
func With(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	return context.WithValue(ctx, requestIDKey, id)
}

// From returns the request id on ctx, or "" when there is none.
func From(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey).(string)
	return id
}
