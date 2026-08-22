package natsmetrics

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// A key that never occurs must never be built. The publisher's label space is
// closed but wide — destination x operation x outcome is 1,848 combinations,
// and PublishLabelsFromSubject plus the fixed call sites can only ever produce
// about seventeen destination/operation pairs. Precomputing the full cross
// product built 8,094 allocations of attribute sets per Publisher that nothing
// would ever look up.
func TestOptTable_BuildsOnlyWhatIsAskedFor(t *testing.T) {
	var built []string
	table := newOptTable(func(key string) metric.MeasurementOption {
		built = append(built, key)
		return metric.WithAttributes(attribute.String("key", key))
	})

	table.get("a")
	table.get("b")
	table.get("a")

	assert.Equal(t, []string{"a", "b"}, built, "each key must be built exactly once")
}

// The same key must return the identical option, not an equal one: the hot path
// hands it straight to Add/Record, and rebuilding per call is the cost this
// table exists to avoid.
func TestOptTable_ReturnsTheSameOptionForTheSameKey(t *testing.T) {
	table := newOptTable(func(key publishKey) metric.MeasurementOption {
		return metric.WithAttributes(attribute.String("operation", string(key.operation)))
	})
	key := publishKey{DestinationCanonical, OperationCanonicalPublish, PublishTimeout}

	assert.Equal(t, table.get(key), table.get(key))
}

// After the first call for a key, lookup must be allocation-free. Failure
// recording runs once per recipient in fan-out and once per rejected publish
// during an outage — the moment the label space stops being cheap is exactly
// the moment the metric matters.
func TestOptTable_LookupIsAllocationFreeOnceWarm(t *testing.T) {
	table := newOptTable(func(key publishKey) metric.MeasurementOption {
		return metric.WithAttributes(attribute.String("outcome", string(key.outcome)))
	})
	key := publishKey{DestinationCanonical, OperationCanonicalPublish, PublishTimeout}
	table.get(key)

	allocs := testing.AllocsPerRun(100, func() { _ = table.get(key) })

	assert.Zero(t, allocs, "a warm lookup must not allocate")
}

// Recorders are called from every worker goroutine at once. Concurrent misses
// on distinct keys must all survive and all agree, which is what the -race
// build the Makefile runs is there to prove.
func TestOptTable_ConcurrentMissesAllResolve(t *testing.T) {
	table := newOptTable(func(key int) metric.MeasurementOption {
		return metric.WithAttributes(attribute.Int("n", key))
	})

	var wg sync.WaitGroup
	for i := range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 32 {
				// assert, not require: require calls FailNow, and the testing
				// package requires FailNow to run on the test's own goroutine —
				// from here it would kill this worker without failing the test.
				assert.NotNil(t, table.get(i%16))
			}
		}()
	}
	wg.Wait()

	for i := range 16 {
		assert.Equal(t, table.get(i), table.get(i), "key %d must resolve to one stable option", i)
	}
}

// Building a recorder must not cost thousands of allocations. Every service
// builds a Publisher at startup and a Consumer per worker loop; the cross
// product made that 8,094 and 732 allocations respectively, for label
// combinations the process would never record.
func TestRecorderConstructionDoesNotPrecomputeTheCrossProduct(t *testing.T) {
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(sdkmetric.NewManualReader()))
	m := NewFromProvider(mp)

	publisher := testing.AllocsPerRun(20, func() { _ = m.Publisher("site-a") })
	consumer := testing.AllocsPerRun(20, func() {
		_ = m.Consumer(ConsumerConfig{Site: "site-a", Stream: "ROOMS-site-a", Consumer: "room-worker"})
	})

	assert.Less(t, publisher, float64(100), "Publisher construction must not precompute 1,848 unused attribute sets")
	assert.Less(t, consumer, float64(100), "Consumer construction must not precompute 180 unused attribute sets")
}
