package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/searchengine"
)

func newTestSyncMetrics(t *testing.T) (*syncMetrics, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	return newSyncMetrics(mp.Meter("test")), reader
}

func collectMetrics(t *testing.T, reader sdkmetric.Reader) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	return rm
}

func sumPoints(t *testing.T, rm metricdata.ResourceMetrics, name string) []metricdata.DataPoint[int64] {
	t.Helper()
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			data, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok, "metric %s has unexpected type %T", name, m.Data)
			return data.DataPoints
		}
	}
	return nil
}

func histPoints[T int64 | float64](t *testing.T, rm metricdata.ResourceMetrics, name string) []metricdata.HistogramDataPoint[T] {
	t.Helper()
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			data, ok := m.Data.(metricdata.Histogram[T])
			require.True(t, ok, "metric %s has unexpected type %T", name, m.Data)
			return data.DataPoints
		}
	}
	return nil
}

func attrMap(set attribute.Set) map[string]string {
	out := map[string]string{}
	for _, kv := range set.ToSlice() {
		out[string(kv.Key)] = kv.Value.AsString()
	}
	return out
}

// sumValue returns the summed value of every datapoint matching want exactly.
func sumValue(points []metricdata.DataPoint[int64], want map[string]string) int64 {
	var total int64
	for _, p := range points {
		got := attrMap(p.Attributes)
		match := len(got) == len(want)
		for k, v := range want {
			if got[k] != v {
				match = false
				break
			}
		}
		if match {
			total += p.Value
		}
	}
	return total
}

func metricsTestHandler(t *testing.T, store Store) (*Handler, *sdkmetric.ManualReader) {
	t.Helper()
	sm, reader := newTestSyncMetrics(t)
	h := NewHandler(store, newMessageCollection("msgs-v1", "site-a", time.Time{}, false), 500)
	h.metrics = sm.forCollection("message-sync")
	return h, reader
}

func metricsTestEvent() *model.MessageEvent {
	return &model.MessageEvent{
		Event: model.EventCreated,
		Message: model.Message{
			ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
			Content: "hello", CreatedAt: time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC),
		},
		SiteID: "site-a", Timestamp: 100,
	}
}

func TestSyncMetrics_FlushSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().Bulk(gomock.Any(), gomock.Len(1)).
		Return([]searchengine.BulkResult{{Status: 201}}, nil)

	h, reader := metricsTestHandler(t, store)
	h.Add(makeStubMsg(t, metricsTestEvent()))
	h.Flush(context.Background())

	rm := collectMetrics(t, reader)

	durations := histPoints[float64](t, rm, "search_sync_worker_bulk_flush_duration")
	require.Len(t, durations, 1)
	assert.Equal(t, uint64(1), durations[0].Count)
	assert.Equal(t, map[string]string{
		"collection": "message-sync",
		"outcome":    "success",
	}, attrMap(durations[0].Attributes))

	actions := histPoints[int64](t, rm, "search_sync_worker_bulk_flush_actions")
	require.Len(t, actions, 1)
	assert.Equal(t, int64(1), actions[0].Sum)
	assert.Equal(t, map[string]string{"collection": "message-sync"}, attrMap(actions[0].Attributes))

	messages := sumPoints(t, rm, "search_sync_worker_messages")
	assert.Equal(t, int64(1), sumValue(messages, map[string]string{
		"collection": "message-sync", "outcome": "acked", "reason": "success",
	}))
}

func TestSyncMetrics_FlushRequestError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().Bulk(gomock.Any(), gomock.Any()).
		Return(nil, errors.New("es down"))

	h, reader := metricsTestHandler(t, store)
	h.Add(makeStubMsg(t, metricsTestEvent()))
	h.Flush(context.Background())

	rm := collectMetrics(t, reader)

	durations := histPoints[float64](t, rm, "search_sync_worker_bulk_flush_duration")
	require.Len(t, durations, 1)
	assert.Equal(t, map[string]string{
		"collection": "message-sync",
		"outcome":    "request_failed",
	}, attrMap(durations[0].Attributes))

	messages := sumPoints(t, rm, "search_sync_worker_messages")
	assert.Equal(t, int64(1), sumValue(messages, map[string]string{
		"collection": "message-sync", "outcome": "nakked", "reason": "request_failed",
	}))
}

func TestSyncMetrics_FlushItemFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().Bulk(gomock.Any(), gomock.Len(1)).
		Return([]searchengine.BulkResult{{Status: 429, Error: "es_rejected_execution_exception"}}, nil)

	h, reader := metricsTestHandler(t, store)
	h.Add(makeStubMsg(t, metricsTestEvent()))
	h.Flush(context.Background())

	rm := collectMetrics(t, reader)

	failures := sumPoints(t, rm, "search_sync_worker_bulk_item_failures")
	assert.Equal(t, int64(1), sumValue(failures, map[string]string{
		"collection": "message-sync", "action": "index", "status": "429",
	}))

	messages := sumPoints(t, rm, "search_sync_worker_messages")
	assert.Equal(t, int64(1), sumValue(messages, map[string]string{
		"collection": "message-sync", "outcome": "nakked", "reason": "item_failed",
	}))

	durations := histPoints[float64](t, rm, "search_sync_worker_bulk_flush_duration")
	require.Len(t, durations, 1)
	assert.Equal(t, "success", attrMap(durations[0].Attributes)["outcome"],
		"per-item failures nak their own messages but the flush round-trip itself succeeded")
}

func TestSyncMetrics_FlushResultMismatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().Bulk(gomock.Any(), gomock.Len(1)).
		Return([]searchengine.BulkResult{}, nil)

	h, reader := metricsTestHandler(t, store)
	h.Add(makeStubMsg(t, metricsTestEvent()))
	h.Flush(context.Background())

	rm := collectMetrics(t, reader)

	durations := histPoints[float64](t, rm, "search_sync_worker_bulk_flush_duration")
	require.Len(t, durations, 1)
	assert.Equal(t, "result_mismatch", attrMap(durations[0].Attributes)["outcome"])

	messages := sumPoints(t, rm, "search_sync_worker_messages")
	assert.Equal(t, int64(1), sumValue(messages, map[string]string{
		"collection": "message-sync", "outcome": "nakked", "reason": "result_mismatch",
	}))
}

func TestSyncMetrics_AddPoisonAcked(t *testing.T) {
	ctrl := gomock.NewController(t)
	h, reader := metricsTestHandler(t, NewMockStore(ctrl))

	h.Add(&stubMsg{data: []byte("{invalid")})

	rm := collectMetrics(t, reader)
	messages := sumPoints(t, rm, "search_sync_worker_messages")
	assert.Equal(t, int64(1), sumValue(messages, map[string]string{
		"collection": "message-sync", "outcome": "acked", "reason": "poison",
	}))
}

func TestSyncMetrics_AddFilteredAcked(t *testing.T) {
	ctrl := gomock.NewController(t)
	sm, reader := newTestSyncMetrics(t)
	// syncFrom after the event's CreatedAt filters it out — zero actions, acked.
	cutoff := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	h := NewHandler(NewMockStore(ctrl), newMessageCollection("msgs-v1", "site-a", cutoff, false), 500)
	h.metrics = sm.forCollection("message-sync")

	h.Add(makeStubMsg(t, metricsTestEvent()))

	rm := collectMetrics(t, reader)
	messages := sumPoints(t, rm, "search_sync_worker_messages")
	assert.Equal(t, int64(1), sumValue(messages, map[string]string{
		"collection": "message-sync", "outcome": "acked", "reason": "filtered",
	}))
	assert.Equal(t, 0, h.MessageCount())
}

func TestSyncMetrics_NoMetricsConfigured(t *testing.T) {
	// A handler without metrics wiring must keep working — nothing to assert
	// beyond "does not panic" on both the Add and Flush paths.
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().Bulk(gomock.Any(), gomock.Any()).
		Return([]searchengine.BulkResult{{Status: 201}}, nil)

	h := NewHandler(store, newMessageCollection("msgs-v1", "site-a", time.Time{}, false), 500)
	h.Add(makeStubMsg(t, metricsTestEvent()))
	h.Flush(context.Background())
	assert.Equal(t, 0, h.MessageCount())
}

func TestBulkStatusLabel(t *testing.T) {
	tests := []struct {
		name   string
		status int
		want   string
	}{
		{"bad request kept exact", 400, "400"},
		{"not found kept exact", 404, "404"},
		{"conflict kept exact", 409, "409"},
		{"payload too large kept exact", 413, "413"},
		{"too many requests kept exact", 429, "429"},
		{"internal kept exact", 500, "500"},
		{"unavailable kept exact", 503, "503"},
		{"other 4xx bucketed", 418, "4xx"},
		{"other 5xx bucketed", 502, "5xx"},
		{"non-error status bucketed as other", 302, "other"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, bulkStatusLabel(tt.status))
		})
	}
}

func TestESParentResolver_RecordsResolveDuration(t *testing.T) {
	sm, reader := newTestSyncMetrics(t)

	resolved := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	r := &esParentResolver{
		esRead: func(ctx context.Context, messageID string) (time.Time, bool) {
			if messageID == "hit" {
				return resolved, true
			}
			return time.Time{}, false
		},
		timeout: time.Second,
		metrics: sm.forCollection("message-sync"),
	}

	_, ok := r.ResolveParentCreatedAt(context.Background(), "hit")
	require.True(t, ok)
	_, ok = r.ResolveParentCreatedAt(context.Background(), "miss")
	require.False(t, ok)

	rm := collectMetrics(t, reader)
	points := histPoints[float64](t, rm, "search_sync_worker_parent_resolve_duration")
	require.Len(t, points, 2)
	byOutcome := map[string]uint64{}
	for _, p := range points {
		attrs := attrMap(p.Attributes)
		assert.Equal(t, "message-sync", attrs["collection"],
			"the two message resolvers share the instruments, so the series must split by collection")
		byOutcome[attrs["outcome"]] = p.Count
	}
	assert.Equal(t, uint64(1), byOutcome["resolved"])
	assert.Equal(t, uint64(1), byOutcome["unresolved"])
}

// recordItemFailure runs once per failed bulk action, and the loop that calls
// it is bounded by the actions in one message — a Teams migration batch
// contributes one action per migrated message, so a single message can drive
// the whole inner loop. The path is only "cold" while Elasticsearch is
// healthy; under the 429 backpressure these metrics exist to diagnose it runs
// once per rejected action, up to BULK_BATCH_SIZE (500) per flush. Building an
// attribute set per call there adds allocation pressure to a system already
// under strain, so the labels are precomputed like every other recorder in
// this file.
func TestRecordItemFailure_DoesNotAllocate(t *testing.T) {
	m := newSyncMetrics(noop.NewMeterProvider().Meter("test"))
	c := m.forCollection("messages")
	ctx := context.Background()

	// One allocation is the floor for any OTel recording call: Add(ctx, n, opt)
	// is variadic, so the option slice is built per call. The contract is parity
	// with a peer that already precomputes its labels, not zero.
	peer := testing.AllocsPerRun(200, func() {
		c.recordMessages(ctx, dispAckedSuccess, 1)
	})
	allocs := testing.AllocsPerRun(200, func() {
		c.recordItemFailure(ctx, "index", 429)
	})

	assert.Equal(t, peer, allocs,
		"recording a bulk item failure must cost the same as a precomputed peer")
}

// Every action/status pair the classifier can produce must resolve to a
// precomputed set, including statuses that bucket into a class.
func TestRecordItemFailure_CoversEveryBoundedLabel(t *testing.T) {
	m := newSyncMetrics(noop.NewMeterProvider().Meter("test"))
	c := m.forCollection("messages")
	ctx := context.Background()

	for _, action := range []string{"index", "delete", "update"} {
		for _, status := range []int{400, 404, 409, 413, 429, 500, 503, 418, 599, 0} {
			require.NotPanics(t, func() { c.recordItemFailure(ctx, action, status) })
		}
	}
	// An action outside the closed set still records, without minting a label.
	require.NotPanics(t, func() { c.recordItemFailure(ctx, "not-an-action", 429) })
}

// An action outside allBulkActions must keep the status. bulkStatusLabel is
// total, so the status is bounded whatever the action was, and it is the half
// that carries the diagnosis — the fallback used to drop it, blinding the 429
// signal for any action added to searchengine but not to allBulkActions.
func TestRecordItemFailure_UnknownActionKeepsStatus(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	c := newSyncMetrics(mp.Meter("search-sync-test")).forCollection("messages")

	c.recordItemFailure(context.Background(), "an-action-nobody-registered", 429)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	var got map[string]string
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "search_sync_worker_bulk_item_failures" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			require.Len(t, sum.DataPoints, 1)
			got = map[string]string{}
			for _, kv := range sum.DataPoints[0].Attributes.ToSlice() {
				got[string(kv.Key)] = kv.Value.String()
			}
		}
	}
	require.NotNil(t, got, "the failure must still be recorded")
	assert.Equal(t, "429", got["status"], "the bounded status must survive an unknown action")
	assert.Equal(t, "messages", got["collection"])
	assert.NotContains(t, got, "action", "the unvetted action must not become a label")
}

// The fallback is precomputed like every other recorder here, so it costs the
// same as the hit path.
func TestRecordItemFailure_UnknownActionDoesNotAllocateMore(t *testing.T) {
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(sdkmetric.NewManualReader()))
	c := newSyncMetrics(mp.Meter("search-sync-test")).forCollection("messages")
	ctx := context.Background()

	known := testing.AllocsPerRun(50, func() { c.recordItemFailure(ctx, "index", 429) })
	unknown := testing.AllocsPerRun(50, func() { c.recordItemFailure(ctx, "unregistered", 429) })

	assert.Equal(t, known, unknown, "the fallback must not cost more than the precomputed hit")
}
