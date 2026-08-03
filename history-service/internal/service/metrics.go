package service

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// roomsGetBatchSize records the number of distinct rooms per rooms.get request.
// Paired with the history_room_preview cache hit/miss counters, it sizes the
// history-service read load that the Phase-2 denormalization would remove.
var roomsGetBatchSize metric.Int64Histogram

func init() {
	h, err := otel.Meter("history-service").Int64Histogram(
		"history_rooms_get_batch_size",
		metric.WithDescription("Distinct rooms per rooms.get request."),
	)
	if err != nil {
		// Fall back to a no-op instrument so recording is always safe even if the
		// global meter provider rejects instrument creation at init time.
		h, _ = noop.NewMeterProvider().Meter("history-service").Int64Histogram("history_rooms_get_batch_size")
	}
	roomsGetBatchSize = h
}
