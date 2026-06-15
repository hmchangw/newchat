package natsutil

import (
	"context"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
)

func TestDebugHeader_Constant(t *testing.T) {
	assert.Equal(t, "X-Debug", DebugHeader)
}

// The ladder is cumulative: off < flow < debug < trace. Downstream emission
// relies on this ordering, so lock it as an invariant.
func TestDebugLevel_LadderOrdering(t *testing.T) {
	assert.Less(t, DebugOff, DebugFlow)
	assert.Less(t, DebugFlow, DebugBasic)
	assert.Less(t, DebugBasic, DebugTrace)
}

func TestParseDebugLevel(t *testing.T) {
	cases := []struct {
		in   string
		want DebugLevel
	}{
		// Off: empty, falsey, explicit off, and—strictly—anything unrecognized.
		{"", DebugOff},
		{"0", DebugOff},
		{"false", DebugOff},
		{"off", DebugOff},
		{"no", DebugOff},
		{"garbage", DebugOff},
		{"2", DebugOff}, // no numeric ladder: 2 is NOT trace
		{"DEBUGGING", DebugOff},
		// flow
		{"flow", DebugFlow},
		// debug + its truthy/bool aliases
		{"debug", DebugBasic},
		{"1", DebugBasic},
		{"true", DebugBasic},
		{"on", DebugBasic},
		// trace (no bool alias — must be spelled out)
		{"trace", DebugTrace},
		// case-insensitive + whitespace-trimmed
		{"Trace", DebugTrace},
		{"  FLOW  ", DebugFlow},
		{"\tDebug\n", DebugBasic},
		{"TRUE", DebugBasic},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			assert.Equal(t, tc.want, ParseDebugLevel(tc.in))
		})
	}
}

func TestDebugLevel_String(t *testing.T) {
	assert.Equal(t, "", DebugOff.String())
	assert.Equal(t, "flow", DebugFlow.String())
	assert.Equal(t, "debug", DebugBasic.String())
	assert.Equal(t, "trace", DebugTrace.String())
}

// String→Parse round-trips for every non-off rung (canonical tokens only).
func TestDebugLevel_StringParseRoundTrip(t *testing.T) {
	for _, l := range []DebugLevel{DebugFlow, DebugBasic, DebugTrace} {
		assert.Equal(t, l, ParseDebugLevel(l.String()))
	}
}

func TestWithDebugLevel_RoundTrip(t *testing.T) {
	ctx := WithDebugLevel(context.Background(), DebugTrace)
	assert.Equal(t, DebugTrace, DebugLevelFromContext(ctx))
}

func TestWithDebugLevel_OffIsNoOp(t *testing.T) {
	parent := context.Background()
	ctx := WithDebugLevel(parent, DebugOff)
	assert.True(t, ctx == parent, "DebugOff must return the parent ctx unchanged")
	assert.Equal(t, DebugOff, DebugLevelFromContext(ctx))
}

func TestWithDebugLevel_Overwrites(t *testing.T) {
	ctx := WithDebugLevel(context.Background(), DebugFlow)
	ctx = WithDebugLevel(ctx, DebugTrace)
	assert.Equal(t, DebugTrace, DebugLevelFromContext(ctx))
}

func TestDebugLevelFromContext_MissingReturnsOff(t *testing.T) {
	assert.Equal(t, DebugOff, DebugLevelFromContext(context.Background()))
}

// The debug ctx key must not collide with the request-ID ctx key.
func TestDebugLevel_IndependentOfRequestID(t *testing.T) {
	ctx := WithRequestID(context.Background(), "req-1")
	ctx = WithDebugLevel(ctx, DebugBasic)
	assert.Equal(t, "req-1", RequestIDFromContext(ctx))
	assert.Equal(t, DebugBasic, DebugLevelFromContext(ctx))
}

func TestHeaderForContext_EmitsDebugAlongsideRequestID(t *testing.T) {
	ctx := WithRequestID(context.Background(), "req-xyz")
	ctx = WithDebugLevel(ctx, DebugTrace)
	h := HeaderForContext(ctx)
	assert.Equal(t, "req-xyz", h.Get(RequestIDHeader))
	assert.Equal(t, "trace", h.Get(DebugHeader))
}

// Debug intent must propagate even on the (rare) path with no request ID.
func TestHeaderForContext_EmitsDebugWithoutRequestID(t *testing.T) {
	ctx := WithDebugLevel(context.Background(), DebugFlow)
	h := HeaderForContext(ctx)
	assert.NotNil(t, h)
	assert.Equal(t, "flow", h.Get(DebugHeader))
	assert.Equal(t, "", h.Get(RequestIDHeader))
}

func TestHeaderForContext_NoDebugHeaderWhenOff(t *testing.T) {
	ctx := WithRequestID(context.Background(), "req-xyz")
	h := HeaderForContext(ctx)
	_, present := h[DebugHeader]
	assert.False(t, present, "X-Debug must be absent when no rung requested")
}

func TestNewMsg_AttachesDebugFromContext(t *testing.T) {
	ctx := WithRequestID(context.Background(), "req-1")
	ctx = WithDebugLevel(ctx, DebugBasic)
	msg := NewMsg(ctx, "chat.foo.bar", []byte("p"))
	assert.Equal(t, "debug", msg.Header.Get(DebugHeader))
}

// The header X-Debug round-trips back to the same rung via ParseDebugLevel.
func TestHeaderForContext_DebugRoundTrip(t *testing.T) {
	for _, l := range []DebugLevel{DebugFlow, DebugBasic, DebugTrace} {
		ctx := WithDebugLevel(context.Background(), l)
		h := HeaderForContext(ctx)
		assert.Equal(t, l, ParseDebugLevel(h.Get(DebugHeader)))
	}
}

func TestDebugPayloadHeader_Constant(t *testing.T) {
	assert.Equal(t, "X-Debug-Payload", DebugPayloadHeader)
}

func TestPayloadCaptureFromHeader(t *testing.T) {
	truthy := []string{"1", "true", "on", "TRUE", "  On  "}
	for _, v := range truthy {
		assert.True(t, PayloadCaptureFromHeader(nats.Header{DebugPayloadHeader: []string{v}}), v)
	}
	falsey := []string{"", "0", "false", "off", "garbage"}
	for _, v := range falsey {
		assert.False(t, PayloadCaptureFromHeader(nats.Header{DebugPayloadHeader: []string{v}}), v)
	}
	assert.False(t, PayloadCaptureFromHeader(nil), "nil header")
}

func TestWithPayloadCapture_RoundTrip(t *testing.T) {
	assert.False(t, PayloadCaptureFromContext(context.Background()))
	ctx := WithPayloadCapture(context.Background())
	assert.True(t, PayloadCaptureFromContext(ctx))
}

func TestHeaderForContext_EmitsPayloadCapture(t *testing.T) {
	ctx := WithPayloadCapture(WithRequestID(context.Background(), "req-1"))
	h := HeaderForContext(ctx)
	assert.Equal(t, "1", h.Get(DebugPayloadHeader))
	assert.Equal(t, "req-1", h.Get(RequestIDHeader))

	// absent → not emitted
	plain := HeaderForContext(WithRequestID(context.Background(), "req-1"))
	_, present := plain[DebugPayloadHeader]
	assert.False(t, present)
}
