package cassrepo

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// startRoomSpan opens a child span named op tagged with the room id and returns
// the span's context. The gocql query spans the o11y observer emits for queries
// run under this context nest beneath it, so a Cassandra trace carries the room
// it served — the missing key for per-room debugging, since room_id is a query
// argument the query spans don't expose on their own. Callers defer span.End().
func (r *Repository) startRoomSpan(ctx context.Context, op, roomID string) (context.Context, trace.Span) {
	ctx, span := r.tracer.Start(ctx, op)
	span.SetAttributes(attribute.String("room_id", roomID))
	return ctx, span
}
