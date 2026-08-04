package main

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// tracerName identifies this service's spans in the global tracer provider.
const tracerName = "github.com/hmchangw/chat/message-worker"

// startRoomSpan opens a child span named op tagged with the room id and returns
// the span's context. The gocql query spans the o11y observer emits for writes
// run under this context nest beneath it, so a Cassandra trace carries the room
// it served — room_id is otherwise only a query argument the query spans don't
// expose. Callers defer span.End().
func (s *CassandraStore) startRoomSpan(ctx context.Context, op, roomID string) (context.Context, trace.Span) {
	ctx, span := s.tracer.Start(ctx, op)
	span.SetAttributes(attribute.String("room_id", roomID))
	return ctx, span
}
