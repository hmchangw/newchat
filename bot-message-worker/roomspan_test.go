package main

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

// recordingStore returns a CassandraStore whose tracer feeds an in-memory
// recorder, so span assertions run without a global provider.
func recordingStore(t *testing.T) (*CassandraStore, *tracetest.SpanRecorder) {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(recorder),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	return &CassandraStore{tracer: tp.Tracer("test")}, recorder
}

func spanAttr(span sdktrace.ReadOnlySpan, key string) (string, bool) {
	for _, a := range span.Attributes() {
		if string(a.Key) == key {
			return a.Value.AsString(), true
		}
	}
	return "", false
}

func TestNewCassandraStore_SetsTracer(t *testing.T) {
	s := NewCassandraStore(nil, msgbucket.Sizer{}, nil)
	assert.NotNil(t, s.tracer, "NewCassandraStore must default the tracer so production spans record")
}

func TestCassandraStore_startSpan_TagsRoomAndMessageID(t *testing.T) {
	s, recorder := recordingStore(t)

	ctx, span := s.startSpan(context.Background(), "cassandra.SaveMessage", "room-9", "msg-1")
	span.End()

	assert.Equal(t,
		span.SpanContext().SpanID(),
		oteltrace.SpanFromContext(ctx).SpanContext().SpanID(),
		"returned ctx must carry the started span",
	)

	ended := recorder.Ended()
	require.Len(t, ended, 1)
	assert.Equal(t, "cassandra.SaveMessage", ended[0].Name())
	roomID, ok := spanAttr(ended[0], "room_id")
	require.True(t, ok, "span must carry a room_id attribute")
	assert.Equal(t, "room-9", roomID)
	messageID, ok := spanAttr(ended[0], "message_id")
	require.True(t, ok, "span must carry a message_id attribute")
	assert.Equal(t, "msg-1", messageID)
}

func TestCassandraStore_startSpan_NestsUnderParent(t *testing.T) {
	s, recorder := recordingStore(t)

	parentCtx, parent := s.tracer.Start(context.Background(), "consume")
	_, child := s.startSpan(parentCtx, "cassandra.SaveThreadMessage", "room-3", "msg-2")
	child.End()
	parent.End()

	var childSpan sdktrace.ReadOnlySpan
	for _, sp := range recorder.Ended() {
		if sp.Name() == "cassandra.SaveThreadMessage" {
			childSpan = sp
		}
	}
	require.NotNil(t, childSpan)
	assert.Equal(t,
		parent.SpanContext().SpanID(),
		childSpan.Parent().SpanID(),
		"room span must nest under the caller's span so query spans inherit room context",
	)
}
