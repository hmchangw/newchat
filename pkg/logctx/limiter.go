package logctx

import (
	"context"
	"log/slog"

	"github.com/nats-io/nats.go"
	"golang.org/x/time/rate"

	"github.com/hmchangw/chat/pkg/natsutil"
)

// allower is the minimal limiter surface Admit needs; satisfied by
// *rate.Limiter and by deterministic test doubles.
type allower interface{ Allow() bool }

// denyAll honors nothing. It is the default so a service that never calls
// Configure emits no verbose output even for flagged traffic.
type denyAll struct{}

func (denyAll) Allow() bool { return false }

var limiter allower = denyAll{}

// capturePayloads gates full-payload logging (see CapturePayload). Default false
// so payload capture is inert in production regardless of the X-Debug-Payload
// header; an operator opts in per environment via DEBUG_LOG_PAYLOADS.
var capturePayloads bool

// Config tunes the per-instance honored-debug rate cap (env prefix DEBUG_LOG_).
type Config struct {
	Rate     float64 `env:"RATE"     envDefault:"50"` // honored debug messages/sec
	Burst    int     `env:"BURST"    envDefault:"50"`
	Payloads bool    `env:"PAYLOADS" envDefault:"false"` // DEBUG_LOG_PAYLOADS: dev-only full-payload logging
}

// Configure installs the package rate limiter and payload-capture gate. Call
// once at startup, before any message is served — it is not safe to call
// concurrently with Admit.
func Configure(c Config) {
	limiter = rate.NewLimiter(rate.Limit(c.Rate), c.Burst)
	capturePayloads = c.Payloads
}

// Admit decides — once — the verbose-logging threshold honored for this message
// on THIS instance:
//  1. parse X-Debug into a rung; if off, return ctx untouched (no token spent);
//  2. store the rung on ctx so the intent propagates downstream, regardless of
//     the cap;
//  3. if the rate limiter allows, store the honored slog threshold so the
//     context-aware Handler will emit this message's verbose lines.
//
// The honor decision is made once here, so a message's verbose lines are
// all-or-nothing — never a half-emitted trace.
func Admit(ctx context.Context, headers nats.Header) context.Context {
	// Payload-capture intent is independent of the rung and is stamped first so
	// it propagates even when no X-Debug rung is set. Emission is gated by
	// CapturePayload (config flag), not here.
	if natsutil.PayloadCaptureFromHeader(headers) {
		ctx = natsutil.WithPayloadCapture(ctx)
	}
	rung := natsutil.ParseDebugLevel(headers.Get(natsutil.DebugHeader))
	if rung == natsutil.DebugOff {
		return ctx
	}
	ctx = natsutil.WithDebugLevel(ctx, rung)
	if limiter.Allow() {
		ctx = withHonoredThreshold(ctx, threshold(rung))
	}
	return ctx
}

// ShouldCapture reports whether a payload would be logged for ctx — i.e. the
// service has payload logging enabled (DEBUG_LOG_PAYLOADS) AND this request
// asked for it. Callers use it to skip marshaling work when capture is off.
func ShouldCapture(ctx context.Context) bool {
	return capturePayloads && natsutil.PayloadCaptureFromContext(ctx)
}

// CapturePayload logs a full request/reply/consumed payload — CONTENT, not
// metadata. It is a no-op unless (1) the service enabled payload logging via
// DEBUG_LOG_PAYLOADS and (2) the request carried X-Debug-Payload. direction is
// "request"/"reply"/"consumed". Logged at INFO so it is independent of the
// metadata-admission gate; the body is the `payload` field.
func CapturePayload(ctx context.Context, direction, subject string, data []byte) {
	if !ShouldCapture(ctx) {
		return
	}
	slog.InfoContext(ctx, "debug payload",
		"direction", direction, "subject", subject,
		"request_id", natsutil.RequestIDFromContext(ctx),
		"bytes", len(data), "payload", string(data))
}
