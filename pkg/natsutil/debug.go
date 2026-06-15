// debug.go: opt-in per-request verbose-logging intent, propagated between
// context.Context and nats.Header exactly like X-Request-ID (see request_id.go).
//
// The wire token is the only stable contract. The internal DebugLevel type is a
// private detail: new rungs, or a future comma-list of facets, are additive and
// non-breaking because clients only ever send a token string.
package natsutil

import (
	"context"
	"strings"

	"github.com/nats-io/nats.go"
)

// DebugHeader carries the requested verbose-logging rung for this request.
const DebugHeader = "X-Debug"

// DebugPayloadHeader requests full request/reply payload logging for this
// request. It is a SEPARATE capability from the X-Debug metadata ladder: a
// service only honors it when its own config enables payload logging
// (DEBUG_LOG_PAYLOADS), so it is inert in production regardless of the header.
const DebugPayloadHeader = "X-Debug-Payload"

type payloadKey int

const payloadCaptureKey payloadKey = 0

// WithPayloadCapture marks ctx as payload-capture-requested; propagated onto
// outbound messages so the intent flows cross-service like the rung.
func WithPayloadCapture(ctx context.Context) context.Context {
	return context.WithValue(ctx, payloadCaptureKey, true)
}

// PayloadCaptureFromContext reports whether ctx requested payload capture.
func PayloadCaptureFromContext(ctx context.Context) bool {
	v, _ := ctx.Value(payloadCaptureKey).(bool)
	return v
}

// PayloadCaptureFromHeader reports whether headers carry a truthy
// X-Debug-Payload (1/true/on, case-insensitive).
func PayloadCaptureFromHeader(headers nats.Header) bool {
	return parseTruthy(headers.Get(DebugPayloadHeader))
}

// DebugLevel is the requested verbosity rung. Off is the zero value; the ladder
// is cumulative (each rung includes the ones below it):
//
//	off < flow < debug < trace
//
// flow  — cross-service path + timing breadcrumbs
// debug — + in-service decision branches
// trace — + per-item / per-recipient lines
type DebugLevel int

const (
	DebugOff DebugLevel = iota
	DebugFlow
	DebugBasic
	DebugTrace
)

// parseTruthy reports whether v is an affirmative flag value (1/true/on),
// trimmed and case-insensitive. Single source for the truthy vocabulary shared
// by the X-Debug "debug" alias and the X-Debug-Payload trigger.
func parseTruthy(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "on":
		return true
	default:
		return false
	}
}

// ParseDebugLevel maps an inbound header value to a rung (trimmed, case-insensitive).
// It is strict by design: any unrecognized value is DebugOff, so a stray
// "X-Debug: 0" (or typo) can never silently enable verbose logging.
//
//	"" / "0" / "false" / "off" / "no" / unknown → DebugOff
//	"flow"                                       → DebugFlow
//	"1" / "true" / "on" / "debug"                → DebugBasic
//	"trace"                                      → DebugTrace
func ParseDebugLevel(v string) DebugLevel {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "flow":
		return DebugFlow
	case "debug":
		return DebugBasic
	case "trace":
		return DebugTrace
	default:
		if parseTruthy(v) { // 1/true/on alias debug
			return DebugBasic
		}
		return DebugOff
	}
}

// String renders a rung to its canonical header token. DebugOff (and any
// out-of-range value) renders as "" — it is never emitted on the wire.
func (l DebugLevel) String() string {
	switch l {
	case DebugFlow:
		return "flow"
	case DebugBasic:
		return "debug"
	case DebugTrace:
		return "trace"
	default:
		return ""
	}
}

// debugCtxKey is a distinct key type so the debug value never collides with the
// request-ID value, even though both use the zero key.
type debugCtxKey int

const debugLevelKey debugCtxKey = 0

// WithDebugLevel returns ctx carrying the requested rung. DebugOff is a no-op
// (returns the parent unchanged), mirroring WithRequestID's empty-is-no-op.
func WithDebugLevel(ctx context.Context, l DebugLevel) context.Context {
	if l == DebugOff {
		return ctx
	}
	return context.WithValue(ctx, debugLevelKey, l)
}

// DebugLevelFromContext returns the requested rung stored in ctx, or DebugOff.
func DebugLevelFromContext(ctx context.Context) DebugLevel {
	l, _ := ctx.Value(debugLevelKey).(DebugLevel)
	return l
}
