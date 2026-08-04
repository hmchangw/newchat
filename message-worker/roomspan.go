package main

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// tracerName identifies this service's spans in the global tracer provider.
const tracerName = "github.com/hmchangw/chat/message-worker"

// startSpan opens a child span named op tagged with the room and/or message id
// so the gocql query spans the o11y observer emits under this context nest
// beneath it and carry the ids in the trace — otherwise they are only query
// arguments the query spans don't expose. Either id may be empty to omit it
// (reads keyed only by message_id pass an empty room). Callers defer span.End().
func (s *CassandraStore) startSpan(ctx context.Context, op, roomID, messageID string) (context.Context, trace.Span) {
	ctx, span := s.tracer.Start(ctx, op)
	attrs := make([]attribute.KeyValue, 0, 2)
	if roomID != "" {
		attrs = append(attrs, attribute.String("room_id", roomID))
	}
	if messageID != "" {
		attrs = append(attrs, attribute.String("message_id", messageID))
	}
	span.SetAttributes(attrs...)
	return ctx, span
}
