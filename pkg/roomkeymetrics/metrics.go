// Package roomkeymetrics exposes OTel metric instruments for the room-key
// fan-out and inter-site RPC code paths shared by room-worker and inbox-worker.
package roomkeymetrics

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

var (
	// FanoutErrors counts the number of failed RoomKeyEvent sends to a single account.
	FanoutErrors metric.Int64Counter
	// StoreErrors counts room-key store (MongoDB) operation failures, tagged by operation name.
	StoreErrors metric.Int64Counter
	// KeyAbsentErrors fires when the store is healthy but no current key exists for a room.
	// Distinct from StoreErrors which counts I/O failures.
	KeyAbsentErrors metric.Int64Counter
)

func init() {
	m := otel.Meter("room-key")

	var err error
	FanoutErrors, err = m.Int64Counter(
		"room_key_fanout_errors_total",
		metric.WithDescription("Number of failed RoomKeyEvent sends to a single account"),
	)
	if err != nil {
		// Fall back to a no-op counter so the program continues to run even if
		// the global meter provider is not yet initialised at package init time.
		FanoutErrors, _ = noop.NewMeterProvider().Meter("room-key").Int64Counter("room_key_fanout_errors_total")
	}

	StoreErrors, err = m.Int64Counter(
		"room_key_store_errors_total",
		metric.WithDescription("Number of room-key store operation failures"),
	)
	if err != nil {
		StoreErrors, _ = noop.NewMeterProvider().Meter("room-key").Int64Counter("room_key_store_errors_total")
	}

	KeyAbsentErrors, err = m.Int64Counter(
		"room_key_absent_errors_total",
		metric.WithDescription("Number of times the store returned (nil, nil) for a room key — key absent, not a transient error"),
	)
	if err != nil {
		KeyAbsentErrors, _ = noop.NewMeterProvider().Meter("room-key").Int64Counter("room_key_absent_errors_total")
	}
}

// storeOpAttrs pre-builds the bounded op attribute sets so the recording path
// does not construct one per call. The set is closed: these are the only
// operations the room-key store exposes.
var storeOpAttrs = func() map[string]metric.MeasurementOption {
	ops := []string{"Get", "Set", "SetWithVersion", "Rotate", "SetIfAbsent", "ArchiveRetired", "RepairIndexes"}
	built := make(map[string]metric.MeasurementOption, len(ops))
	for _, op := range ops {
		built[op] = metric.WithAttributes(attribute.String("op", op))
	}
	return built
}()

// keyAbsentOpAttrs bounds KeyAbsentErrors' op label the same way. Only
// room-service names an operation — it separates "retention was too short" from
// room-worker's unrelated use of the same counter, which passes nothing.
var keyAbsentOpAttrs = func() map[string]metric.MeasurementOption {
	ops := []string{"GetByVersion"}
	built := make(map[string]metric.MeasurementOption, len(ops))
	for _, op := range ops {
		built[op] = metric.WithAttributes(attribute.String("op", op))
	}
	return built
}()

// RecordFanoutErrors counts failed RoomKeyEvent sends.
//
// It deliberately takes no label. The counter was previously called with the
// room id as an attribute from inside the per-account fan-out loop, which is
// one series per room — unbounded — and rebuilt the attribute set on every
// iteration. The room a fan-out failed for is already on the log line and the
// span next to each call, where high cardinality is free; a counter is not the
// place for it.
func RecordFanoutErrors(ctx context.Context, count int64) {
	if count <= 0 {
		return
	}
	FanoutErrors.Add(ctx, count)
}

// RecordStoreError counts a room-key store failure against its bounded
// operation name. An unrecognised op is recorded without the label rather than
// minting a new series.
func RecordStoreError(ctx context.Context, op string) {
	if opt, ok := storeOpAttrs[op]; ok {
		StoreErrors.Add(ctx, 1, opt)
		return
	}
	StoreErrors.Add(ctx, 1)
}

// RecordKeyAbsent counts a room key the store reported as absent, against its
// bounded operation name. An empty op — room-worker has no operation to name —
// or an unrecognised one records without the label rather than minting a series.
func RecordKeyAbsent(ctx context.Context, op string) {
	if opt, ok := keyAbsentOpAttrs[op]; ok {
		KeyAbsentErrors.Add(ctx, 1, opt)
		return
	}
	KeyAbsentErrors.Add(ctx, 1)
}
