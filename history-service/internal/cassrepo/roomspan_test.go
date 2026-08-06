package cassrepo

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/hmchangw/chat/pkg/msgbucket"
)

// recordingRepo returns a Repository whose tracer feeds an in-memory recorder,
// so span assertions run without a real tracer provider or global state.
func recordingRepo(t *testing.T) (*Repository, *tracetest.SpanRecorder) {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(recorder),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	return &Repository{tracer: tp.Tracer("test")}, recorder
}

func attrValue(span sdktrace.ReadOnlySpan, key string) (string, bool) {
	for _, a := range span.Attributes() {
		if string(a.Key) == key {
			return a.Value.AsString(), true
		}
	}
	return "", false
}

func TestNewRepository_SetsTracer(t *testing.T) {
	r := NewRepository(nil, msgbucket.Sizer{}, 0, nil)
	assert.NotNil(t, r.tracer, "NewRepository must default the tracer so production spans record")
}

func TestRepository_startSpan_TagsRoomAndMessageID(t *testing.T) {
	r, recorder := recordingRepo(t)

	ctx, span := r.startSpan(context.Background(), "cassrepo.PinMessage", "room-42", "msg-1")
	span.End()

	// The returned context carries the span so nested query spans parent to it.
	assert.Equal(t,
		span.SpanContext().SpanID(),
		oteltrace.SpanFromContext(ctx).SpanContext().SpanID(),
		"returned ctx must carry the started span",
	)

	ended := recorder.Ended()
	require.Len(t, ended, 1)
	assert.Equal(t, "cassrepo.PinMessage", ended[0].Name())
	roomID, ok := attrValue(ended[0], "room_id")
	require.True(t, ok, "span must carry a room_id attribute")
	assert.Equal(t, "room-42", roomID)
	messageID, ok := attrValue(ended[0], "message_id")
	require.True(t, ok, "span must carry a message_id attribute")
	assert.Equal(t, "msg-1", messageID)
}

func TestRepository_startSpan_OmitsEmptyMessageID(t *testing.T) {
	r, recorder := recordingRepo(t)

	// Room-range reads (bucket walks) serve no single message, so message_id
	// is empty and must be omitted rather than written blank.
	_, span := r.startSpan(context.Background(), "cassrepo.GetMessagesBefore", "room-42", "")
	span.End()

	ended := recorder.Ended()
	require.Len(t, ended, 1)
	roomID, ok := attrValue(ended[0], "room_id")
	require.True(t, ok)
	assert.Equal(t, "room-42", roomID)
	_, ok = attrValue(ended[0], "message_id")
	assert.False(t, ok, "an empty message id must be omitted, not written as an empty attribute")
}

func TestRepository_startSpan_NestsUnderParent(t *testing.T) {
	r, recorder := recordingRepo(t)

	parentCtx, parent := r.tracer.Start(context.Background(), "handler")
	_, child := r.startSpan(parentCtx, "cassrepo.PinMessage", "room-7", "msg-2")
	child.End()
	parent.End()

	var childSpan sdktrace.ReadOnlySpan
	for _, s := range recorder.Ended() {
		if s.Name() == "cassrepo.PinMessage" {
			childSpan = s
		}
	}
	require.NotNil(t, childSpan)
	assert.Equal(t,
		parent.SpanContext().SpanID(),
		childSpan.Parent().SpanID(),
		"room span must nest under the caller's span so query spans inherit room context",
	)
}
