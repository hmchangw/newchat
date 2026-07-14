// Package cachemetrics exposes shared OpenTelemetry counters for cache
// hit/miss/error outcomes, tagged by cache name and tier ("l1" or "l2").
//
// A single instrument set across every cache lets one Grafana panel compute
// per-cache hit rate from cache="…",tier="…" label selectors, e.g.:
//
//	sum by (cache) (rate(cache_hits_total[5m]))
//	  / sum by (cache) (rate(cache_hits_total[5m]) + rate(cache_misses_total[5m]))
//
// Counters are cumulative (never reset); the dashboard computes rates. When
// no MeterProvider is installed the instruments are no-ops, so callers can
// record unconditionally on the hot path.
package cachemetrics

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// Metrics holds the cache outcome counters created against one meter.
type Metrics struct {
	hits   metric.Int64Counter
	misses metric.Int64Counter
	errs   metric.Int64Counter
}

// New creates the cache outcome counters against m.
func New(m metric.Meter) (*Metrics, error) {
	hits, err := m.Int64Counter("cache_hits_total",
		metric.WithDescription("Cache lookups served from cache, by cache and tier."))
	if err != nil {
		return nil, fmt.Errorf("create cache_hits_total counter: %w", err)
	}
	misses, err := m.Int64Counter("cache_misses_total",
		metric.WithDescription("Cache lookups that missed and fell through to the backing store, by cache and tier."))
	if err != nil {
		return nil, fmt.Errorf("create cache_misses_total counter: %w", err)
	}
	errs, err := m.Int64Counter("cache_errors_total",
		metric.WithDescription("Cache lookups that failed at the cache layer (transport/decode), by cache and tier."))
	if err != nil {
		return nil, fmt.Errorf("create cache_errors_total counter: %w", err)
	}
	return &Metrics{hits: hits, misses: misses, errs: errs}, nil
}

// Recorder records cache outcomes for one (cache, tier) pair. The zero value
// is unusable; obtain one via Metrics.For or the package-level For.
type Recorder struct {
	m     *Metrics
	attrs metric.MeasurementOption
}

// For returns a Recorder that tags every outcome with the given cache name
// and tier ("l1" or "l2").
func (m *Metrics) For(cache, tier string) Recorder {
	return Recorder{
		m: m,
		attrs: metric.WithAttributes(
			attribute.String("cache", cache),
			attribute.String("tier", tier),
		),
	}
}

// Hit records a cache lookup served from cache.
func (r Recorder) Hit(ctx context.Context) { r.m.hits.Add(ctx, 1, r.attrs) }

// Miss records a cache lookup that fell through to the backing store.
func (r Recorder) Miss(ctx context.Context) { r.m.misses.Add(ctx, 1, r.attrs) }

// Error records a cache-layer failure (transport error, oversize blob, or
// decode failure) — distinct from a miss, which is a clean absence.
func (r Recorder) Error(ctx context.Context) { r.m.errs.Add(ctx, 1, r.attrs) }

// def is the package-default Metrics backed by the global meter, so callers
// that do not own a Metrics can record via the package-level For.
var def *Metrics

func init() {
	m, err := New(otel.Meter("cache"))
	if err != nil {
		// Fall back to no-op instruments so recording is always safe even if
		// the global meter provider rejects instrument creation at init time.
		m, _ = New(noop.NewMeterProvider().Meter("cache"))
	}
	def = m
}

// For returns a Recorder from the package-default Metrics, tagged with the
// given cache name and tier.
func For(cache, tier string) Recorder { return def.For(cache, tier) }
