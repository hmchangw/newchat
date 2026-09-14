// request_id.go: helpers to propagate X-Request-ID between context.Context and nats.Header.
// Two entry-point helpers per docs/error-handling.md §3a:
//   - StampRequestID — mint-on-missing (default; safe for paths where the ID is logging-only).
//   - RequireRequestID — reject-on-missing (for paths that derive JetStream Nats-Msg-Id
//     or document IDs from the request ID, where server-side minting would break client-retry dedup).
package natsutil

import (
	"context"
	"log/slog"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/idgen"
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

// HeaderForContext returns a nats.Header carrying the propagated correlation
// values from ctx — X-Request-ID and, when a verbose rung was requested,
// X-Debug. Returns nil when ctx carries neither, so callers still get a nil
// (not empty) header on the common path.
func HeaderForContext(ctx context.Context) nats.Header {
	id := RequestIDFromContext(ctx)
	debug := DebugLevelFromContext(ctx)
	payload := PayloadCaptureFromContext(ctx)
	migration := migrationLiveFromContext(ctx)
	if id == "" && debug == DebugOff && !payload && !migration {
		return nil
	}
	h := nats.Header{}
	if id != "" {
		h[RequestIDHeader] = []string{id}
	}
	if debug != DebugOff {
		h[DebugHeader] = []string{debug.String()}
	}
	if payload {
		h[DebugPayloadHeader] = []string{"1"}
	}
	if migration {
		h[HeaderMigration] = []string{MigrationLive}
	}
	return h
}

// NewMsg builds a *nats.Msg with subj, data, and X-Request-ID drawn from ctx (nil header if no ID).
func NewMsg(ctx context.Context, subj string, data []byte) *nats.Msg {
	return &nats.Msg{
		Subject: subj,
		Data:    data,
		Header:  HeaderForContext(ctx),
	}
}

// NewMsgEncoded is NewMsg plus a Nats-Encoding header when encoding is
// non-empty. NewMsg returns a nil Header when ctx carries no request-id, so
// this owns that guard — callers don't need to know the quirk.
func NewMsgEncoded(ctx context.Context, subj string, data []byte, encoding string) *nats.Msg {
	msg := NewMsg(ctx, subj, data)
	if encoding != "" {
		if msg.Header == nil {
			msg.Header = nats.Header{}
		}
		msg.Header.Set(HeaderNatsEncoding, encoding)
	}
	return msg
}

// StampRequestID is the request-id half of the NATS boundary. It:
//  1. Resolves the inbound X-Request-ID via idgen.ResolveRequestID (mint when
//     missing/malformed per the repo-wide policy in docs/error-handling.md),
//  2. Stamps the resolved id onto ctx via WithRequestID,
//  3. Emits a single Warn line when a malformed inbound value was replaced
//     (silent on missing — that's the benign common case),
//  4. Returns the new ctx and the id so the caller can also enrich its slog
//     values (c.WithLogValues for natsrouter, errcode.WithLogValues for raw
//     QueueSubscribe handlers).
//
// subject is logged alongside the warn for trace context; pass "" if not
// applicable.
//
// Stamping the id is only one of the three things a boundary owes a message —
// the X-Debug rung and the payload capture ride the same headers, and a site
// that does one of the three and not the others is how the fleet drifted
// before. Do not call this directly from a stream consumer: logctx.ConsumeContext
// does all three and is what a JetStream consume loop should use. natsrouter's
// RequestID middleware is the equivalent for request/reply.
func StampRequestID(ctx context.Context, headers nats.Header, subject string) (context.Context, string) {
	var inbound string
	if headers != nil {
		inbound = headers.Get(RequestIDHeader)
	}
	id, replaced := idgen.ResolveRequestID(inbound)
	ctx = WithRequestID(ctx, id)
	if replaced {
		slog.WarnContext(ctx, "minted request_id (inbound invalid)", "inbound", inbound, "subject", subject)
	}
	return ctx, id
}

// RequireRequestID is the strict variant of StampRequestID. Use it on entry
// points whose downstream pipeline derives JetStream Nats-Msg-Id components
// or deterministic document IDs from the request ID (room-service handlers,
// room-worker.natsServerCreateDM) — silently minting a fresh UUID server-side
// would break client-retry deduplication on those paths. Missing or malformed
// inbound headers return an errcode.BadRequest; the ctx is returned unchanged
// so the caller can still use it for logging the failure.
//
// See docs/error-handling.md §3a for the rationale and the list of paths that
// must use this instead of StampRequestID.
func RequireRequestID(ctx context.Context, headers nats.Header, subject string) (context.Context, string, error) {
	var inbound string
	if headers != nil {
		inbound = headers.Get(RequestIDHeader)
	}
	if !idgen.IsValidUUID(inbound) {
		return ctx, "", errcode.BadRequest(
			"X-Request-ID header is required (must be a valid hyphenated UUID per docs/error-handling.md §3a)",
			errcode.WithReason(errcode.RequestIDRequired),
		)
	}
	return WithRequestID(ctx, inbound), inbound, nil
}

// InboxDedupID composes a JetStream Nats-Msg-Id as base+":"+destSiteID. base
// is the X-Request-ID from ctx; falls back to payloadSeed when ctx carries no
// request ID, with a warn log so partial-deployment cases are observable.
func InboxDedupID(ctx context.Context, destSiteID, payloadSeed string) string {
	base := RequestIDFromContext(ctx)
	if base == "" {
		slog.Warn("missing X-Request-ID; falling back to payload-derived inbox dedup base", "destSiteID", destSiteID)
		base = payloadSeed
	}
	return base + ":" + destSiteID
}
