package natsrouter

import (
	"context"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errnats"
	"github.com/hmchangw/chat/pkg/logctx"
	"github.com/hmchangw/chat/pkg/natsutil"
)

// Middleware is a handler that participates in the middleware chain.
// Call c.Next() to continue, or return/call c.Abort() to short-circuit.
type Middleware = HandlerFunc

// requestIDKey is the context key used to store the request ID.
const requestIDKey = "requestID"

// RequestID extracts X-Request-ID (or mints via idgen), stores it on the
// natsrouter keys map AND the underlying ctx, AND enriches the ctx logger so
// every Classify line on this request automatically carries request_id —
// handlers don't need to re-pass it. It also admits the inbound X-Debug rung
// (logctx.Admit) so verbose-logging intent both propagates downstream and,
// within this instance's rate budget, gates the handler's flow/debug/trace
// edges. This is the single global boundary every router installs, so every
// handler gets debug admission with no per-handler work.
func RequestID() HandlerFunc {
	return func(c *Context) {
		var (
			headers nats.Header
			subj    string
		)
		if c.Msg != nil {
			headers = c.Msg.Header
			subj = c.Msg.Subject
		}
		ctx, reqID := natsutil.StampRequestID(c.ctx, headers, subj)
		ctx = logctx.Admit(ctx, headers)
		c.Set(requestIDKey, reqID)
		c.SetContext(ctx)
		c.WithLogValues("request_id", reqID)
		// dev-only request-payload capture (no-op unless DEBUG_LOG_PAYLOADS +
		// X-Debug-Payload); central here so every RPC is covered once, after
		// admission has stamped the intent onto ctx.
		if c.Msg != nil {
			logctx.CapturePayload(ctx, "request", subj, c.Msg.Data)
		}
		c.Next()
	}
}

// RequireRequestID is the strict variant of RequestID: a missing/non-UUID
// X-Request-ID is rejected (BadRequest, reason RequestIDRequired) and aborts; never mints.
func RequireRequestID() HandlerFunc {
	return func(c *Context) {
		var (
			headers nats.Header
			subj    string
		)
		if c.Msg != nil {
			headers = c.Msg.Header
			subj = c.Msg.Subject
		}
		ctx, id, err := natsutil.RequireRequestID(c.ctx, headers, subj)
		if err != nil {
			// c.Msg is always set in production (acquireContext); guard so a
			// nil-Msg test context aborts cleanly instead of panicking in Respond.
			if c.Msg != nil {
				errnats.Reply(c, c.Msg, err)
			}
			c.Abort()
			return
		}
		c.Set(requestIDKey, id)
		c.SetContext(ctx)
		c.WithLogValues("request_id", id)
		c.Next()
	}
}

// requestAttrs returns common log attributes including the request ID if present.
func requestAttrs(c *Context) []any {
	var attrs []any
	if c.Msg != nil {
		attrs = append(attrs, "subject", c.Msg.Subject)
	}
	if id, ok := c.Get(requestIDKey); ok {
		attrs = append(attrs, "request_id", id)
	}
	return attrs
}

// Recovery returns middleware that catches panics, logs them, and replies with "internal error".
func Recovery() HandlerFunc {
	return func(c *Context) {
		defer func() {
			if r := recover(); r != nil {
				attrs := append(requestAttrs(c), "panic", r)
				slog.Error("panic recovered", attrs...)
				// Already logged above; ReplyQuiet avoids a redundant Classify line.
				errnats.ReplyQuiet(c.Msg, errcode.Internal("internal error"))
				c.Abort()
			}
		}()
		c.Next()
	}
}

// Logging returns middleware that records a per-request breadcrumb (subject,
// duration, request ID) at the on-demand FLOW level — NOT always-on INFO.
// Steady-state per-RPC visibility comes from metrics and OTel traces; the
// per-request line surfaces only when the client flags the request via X-Debug
// (see pkg/logctx), so it costs nothing for unflagged traffic. Errors are
// unaffected — they are logged once by errcode.Classify at the reply boundary.
func Logging() HandlerFunc {
	return func(c *Context) {
		start := time.Now()
		c.Next()
		attrs := append(requestAttrs(c), "duration", time.Since(start))
		slog.Log(c, logctx.LevelFlow, "nats request", attrs...)
	}
}

// HandlerTimeout returns middleware that wraps the handler context with a
// deadline of d. Downstream calls that respect context (Cassandra/Mongo
// drivers, otelnats.Conn.Publish, etc.) will abort if the chain runs longer
// than d. The deadline is released when the chain returns.
//
// Caveat — the timeout does NOT actively interrupt a running handler. A
// handler doing pure CPU work or calling a non-context-aware library will
// run to completion past the deadline, holding its admission slot the
// whole time. Make sure handlers either propagate ctx into every blocking
// call or are short by construction.
//
// Reply mapping — when a context-aware downstream call returns
// context.DeadlineExceeded and the handler returns
// `fmt.Errorf("...: %w", err)`, the router's replyErr path collapses
// to `{"code":"internal","error":"internal error"}` (no typed errcode
// match). Recommended pattern: in the handler, map the deadline-expired
// sentinel explicitly, e.g.
//
//	if errors.Is(err, context.DeadlineExceeded) {
//	    return nil, errcode.Unavailable("request timed out")
//	}
//
// so the caller sees a structured "unavailable" code instead of a
// generic internal error.
//
// Place AFTER RequestID so the request ID is set before any
// timeout-related downstream work runs. Position relative to Logging
// does not affect what Logging records: Logging measures via
// `time.Since(start)` in its post-`c.Next()` phase, which captures the
// full chain duration regardless of where HandlerTimeout sits.
func HandlerTimeout(d time.Duration) HandlerFunc {
	return func(c *Context) {
		ctx, cancel := context.WithTimeout(c.ctx, d)
		defer cancel()
		c.SetContext(ctx)
		c.Next()
	}
}
