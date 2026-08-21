package natsmetrics

import (
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/otel/metric"
)

// optTable caches one metric.MeasurementOption per label combination, building
// each combination the first time it is recorded rather than all of them up
// front.
//
// The label space is closed — every component is a normalized enum — but it is
// far wider than what a process actually records. destination x operation x
// outcome is 1,680 combinations, while PublishLabelsFromSubject and the fixed
// call sites between them can only produce about seventeen destination/operation
// pairs; precomputing the cross product cost 8,094 allocations per Publisher and
// held every one of them for the life of the process.
//
// Lazy must not mean "build it per call": the hot paths here record once per
// recipient in fan-out and once per rejected publish during an outage, and
// attribute.NewSet sorts and allocates every time (264 ns/192 B against 9.6 ns
// for a map lookup). So a read is an atomic pointer load plus a plain map
// lookup — no allocation, no lock, no contention between recorders — and only a
// miss takes the mutex, copies the map and publishes a new one. Copy-on-write is
// affordable precisely because the label space is closed: the number of writes
// over a process lifetime is bounded by its size, and in practice by the handful
// of combinations that occur.
type optTable[K comparable] struct {
	build func(K) metric.MeasurementOption
	mu    sync.Mutex
	warm  atomic.Pointer[map[K]metric.MeasurementOption]
}

func newOptTable[K comparable](build func(K) metric.MeasurementOption) *optTable[K] {
	return &optTable[K]{build: build}
}

// get returns the option for key, building it on first use.
func (t *optTable[K]) get(key K) metric.MeasurementOption {
	if warm := t.warm.Load(); warm != nil {
		if opt, ok := (*warm)[key]; ok {
			return opt
		}
	}
	return t.miss(key)
}

func (t *optTable[K]) miss(key K) metric.MeasurementOption {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Another recorder may have built this key between the failed read and the
	// lock. Returning its option keeps one option per key, so callers that
	// compare or reuse the value see a stable result.
	current := t.warm.Load()
	size := 0
	if current != nil {
		if opt, ok := (*current)[key]; ok {
			return opt
		}
		size = len(*current)
	}

	// Copy rather than mutate: readers hold the old map without a lock.
	next := make(map[K]metric.MeasurementOption, size+1)
	if current != nil {
		for k, v := range *current {
			next[k] = v
		}
	}
	opt := t.build(key)
	next[key] = opt
	t.warm.Store(&next)
	return opt
}
