package logctx

import (
	"context"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/natsutil"
)

const validRequestID = "01970a4f-8c2d-7c9a-abcd-e0123456789f"

func TestConsumeContext_HonoursAnInboundRequestID(t *testing.T) {
	headers := nats.Header{natsutil.RequestIDHeader: []string{validRequestID}}

	ctx, id := ConsumeContext(context.Background(), headers, "chat.msg.canonical.site-a.created", nil)

	assert.Equal(t, validRequestID, id)
	assert.Equal(t, validRequestID, natsutil.RequestIDFromContext(ctx))
}

func TestConsumeContext_MintsWhenAbsent(t *testing.T) {
	ctx, id := ConsumeContext(context.Background(), nats.Header{}, "chat.msg.canonical.site-a.created", nil)

	require.NotEmpty(t, id)
	assert.True(t, idgen.IsValidUUID(id), "a minted id must be a hyphenated UUID")
	assert.Equal(t, id, natsutil.RequestIDFromContext(ctx))
}

func TestConsumeContext_NilHeadersDoNotPanic(t *testing.T) {
	ctx, id := ConsumeContext(context.Background(), nil, "chat.msg.canonical.site-a.created", nil)

	assert.NotEmpty(t, id)
	assert.Equal(t, id, natsutil.RequestIDFromContext(ctx))
}

// The gap this helper closes: seven consumers stamped the request id but never
// admitted the X-Debug rung, so verbose-logging intent died at the stream
// boundary. Admission is what carries it into the handler.
func TestConsumeContext_AdmitsTheDebugRung(t *testing.T) {
	tests := []struct {
		name   string
		header string
		want   natsutil.DebugLevel
	}{
		{"no header", "", natsutil.DebugOff},
		{"flow", "flow", natsutil.DebugFlow},
		{"debug", "debug", natsutil.DebugBasic},
		{"trace", "trace", natsutil.DebugTrace},
		{"truthy aliases debug", "true", natsutil.DebugBasic},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			headers := nats.Header{}
			if tt.header != "" {
				headers.Set(natsutil.DebugHeader, tt.header)
			}

			ctx, _ := ConsumeContext(context.Background(), headers, "chat.msg.canonical.site-a.created", nil)

			assert.Equal(t, tt.want, natsutil.DebugLevelFromContext(ctx))
		})
	}
}

// Payload-capture intent rides independently of the rung, so it must propagate
// even when no X-Debug rung is set.
func TestConsumeContext_AdmitsPayloadCaptureIntent(t *testing.T) {
	headers := nats.Header{}
	headers.Set(natsutil.DebugPayloadHeader, "true")

	ctx, _ := ConsumeContext(context.Background(), headers, "chat.msg.canonical.site-a.created", nil)

	assert.True(t, natsutil.PayloadCaptureFromContext(ctx))
}

func TestConsumeContext_NoPayloadIntentWithoutTheHeader(t *testing.T) {
	ctx, _ := ConsumeContext(context.Background(), nats.Header{}, "chat.msg.canonical.site-a.created", nil)

	assert.False(t, natsutil.PayloadCaptureFromContext(ctx))
}

// The returned context is what the handler runs under, so the request id has to
// be on it before anything else reads it — the capture below already does.
func TestConsumeContext_CapturesThePayloadUnderTheStampedContext(t *testing.T) {
	headers := nats.Header{natsutil.RequestIDHeader: []string{validRequestID}}
	headers.Set(natsutil.DebugPayloadHeader, "true")

	ctx, id := ConsumeContext(context.Background(), headers, "chat.msg.canonical.site-a.created", []byte(`{"a":1}`))

	assert.Equal(t, validRequestID, id)
	assert.Equal(t, validRequestID, natsutil.RequestIDFromContext(ctx))
	assert.True(t, natsutil.PayloadCaptureFromContext(ctx))
}
