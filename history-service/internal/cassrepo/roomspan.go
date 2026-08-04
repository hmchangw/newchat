package cassrepo

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// startSpan opens a child span named op tagged with the room and/or message id
// and returns the span's context. The gocql query spans the o11y observer emits
// for queries run under this context nest beneath it, so a Cassandra trace
// carries the room and message it served — the missing keys for per-room and
// per-message debugging, since both are query arguments the query spans don't
// expose. Either id may be empty to omit it (room-range reads pass an empty
// message id; message-id point reads pass an empty room). Callers defer
// span.End().
func (r *Repository) startSpan(ctx context.Context, op, roomID, messageID string) (context.Context, trace.Span) {
	ctx, span := r.tracer.Start(ctx, op)
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
