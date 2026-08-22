package roomkeymetrics

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// The instruments in this package are created at init() against the global
// meter, so a test that wants to read them has to install a provider on that
// same global. OTel's global meter delegates already-created instruments to the
// real provider once one arrives, which means the counters read here are the
// ones production records to — not a parallel set built for the test.
//
// Installing a global exactly once keeps the reader shared, so every assertion
// below measures a delta around its own call rather than an absolute total.
// That is what keeps the tests independent of each other and of their order.
//
// The delta is only sound while these tests run sequentially: two tests
// recording concurrently would each read the other's increments between their
// own before and after. No test in this package may call t.Parallel(). The
// shared reader is not a choice — the instruments are created at init() against
// the global meter, so there is one set of counters per process to read.
var (
	installOnce sync.Once
	reader      sdkmetric.Reader
)

func installGlobalReader(t *testing.T) {
	t.Helper()
	installOnce.Do(func() {
		reader = sdkmetric.NewManualReader()
		otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	})
}

// counterTotal sums the points of the named counter whose attribute set matches
// want exactly. An absent instrument or point is zero, which is precisely what
// "nothing was recorded under these labels" means.
func counterTotal(t *testing.T, name string, want attribute.Set) int64 {
	t.Helper()
	installGlobalReader(t)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok, "%s must be a monotonic sum", name)
			for _, dp := range sum.DataPoints {
				if dp.Attributes.Equals(&want) {
					total += dp.Value
				}
			}
		}
	}
	return total
}

func opSet(op string) attribute.Set { return attribute.NewSet(attribute.String("op", op)) }

// RecordFanoutErrors takes no label argument on purpose. The counter used to be
// called with attribute.String("roomId", roomID) straight from the per-account
// fan-out loop, which is one series per room — unbounded — and rebuilt the
// attribute set on every call. Exposing a helper with no label parameter makes
// the mistake unrepresentable rather than relying on review to catch it again,
// so the assertion is that the recorded point carries an empty label set.
func TestRecordFanoutErrors_RecordsUnlabelledPoints(t *testing.T) {
	const name = "room_key_fanout_errors_total"
	unlabelled := *attribute.EmptySet()
	before := counterTotal(t, name, unlabelled)

	RecordFanoutErrors(context.Background(), 1)
	RecordFanoutErrors(context.Background(), 42)

	assert.Equal(t, int64(43), counterTotal(t, name, unlabelled)-before)
}

// A zero or negative count is a caller bug; recording it would break the
// counter's monotonicity, so it is dropped before it reaches Add.
func TestRecordFanoutErrors_IgnoresNonPositiveCounts(t *testing.T) {
	const name = "room_key_fanout_errors_total"
	unlabelled := *attribute.EmptySet()
	before := counterTotal(t, name, unlabelled)

	RecordFanoutErrors(context.Background(), 0)
	RecordFanoutErrors(context.Background(), -1)

	assert.Zero(t, counterTotal(t, name, unlabelled)-before, "a non-positive count must record nothing")
}

// The store counter keeps its op label because Get, Set, SetWithVersion and
// Rotate are a closed set, unlike a room id. Anything outside that set is
// recorded without the label rather than minting a series for it — an op string
// that reached here unrecognised is by definition one the enum does not bound.
func TestRecordStoreError_LabelsOnlyBoundedOperations(t *testing.T) {
	const name = "room_key_store_errors_total"

	tests := []struct {
		name string
		op   string
		want attribute.Set
	}{
		{name: "get", op: "Get", want: opSet("Get")},
		{name: "set", op: "Set", want: opSet("Set")},
		{name: "set with version", op: "SetWithVersion", want: opSet("SetWithVersion")},
		{name: "rotate", op: "Rotate", want: opSet("Rotate")},
		{name: "unknown op drops the label", op: "DropDatabase", want: *attribute.EmptySet()},
		{name: "empty op drops the label", op: "", want: *attribute.EmptySet()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := counterTotal(t, name, tt.want)
			unbounded := counterTotal(t, name, opSet(tt.op))

			RecordStoreError(context.Background(), tt.op)

			assert.Equal(t, int64(1), counterTotal(t, name, tt.want)-before)
			if tt.want.Len() == 0 {
				assert.Zero(t, counterTotal(t, name, opSet(tt.op))-unbounded,
					"an unrecognised op must not open a series of its own")
			}
		})
	}
}
