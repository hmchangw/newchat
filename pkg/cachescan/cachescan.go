// Package cachescan measures how much of a Valkey keyspace each application
// cache occupies, and publishes the result as OpenTelemetry gauges.
//
// Valkey has no per-prefix memory accounting: INFO memory reports one total
// per node and nothing decomposes it. This package walks the keyspace with
// SCAN, classifies each key through pkg/cachekeys, measures a sample of them
// with MEMORY USAGE, and extrapolates a per-cache byte estimate from the
// sample. Keys matching no registered keyspace land in cachekeys.Unclassified
// — the bucket that makes a cache added without a registry entry visible
// rather than silently absent.
//
// A scan is O(keyspace) and is meant to run on a timer (minutes), never on a
// request path. SCAN is cursor-based and non-blocking, so it does not stall
// the server, but it is not a point-in-time snapshot: keys created or expired
// mid-walk may be counted once, twice, or not at all. That is acceptable for a
// share-of-memory metric and unacceptable for anything needing exactness.
//
// Reported bytes are attributed bytes, and deliberately do not sum to the
// node's used_memory. MEMORY USAGE accounts for a key's own allocations but
// not the per-key share of the main dict's bucket array or the cluster slot
// index, and a node also carries a fixed baseline belonging to no cache.
// Measured on Valkey 7.2 with ~75-byte string values, MEMORY USAGE reported
// 160 B/key against a true used_memory delta of 196 B/key — roughly 82%. Read
// the gauges as each cache's share of attributed bytes and as a trend over
// time; read used_memory for what the node is actually holding.
package cachescan

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/hmchangw/chat/pkg/cachekeys"
)

// Node is one Valkey primary's keyspace, narrowed to what a scan needs.
type Node interface {
	// Scan returns one page of keys and the cursor to resume from; a zero
	// next cursor ends the walk.
	Scan(ctx context.Context, cursor uint64, count int64) (keys []string, next uint64, err error)
	// MemoryUsage reports a key's total memory footprint. ok is false when the
	// key no longer exists, which is expected: a TTL can fire between the SCAN
	// that returned it and this call.
	MemoryUsage(ctx context.Context, key string) (bytes int64, ok bool, err error)
}

// Cluster fans a scan out over every primary. Implementations may invoke fn
// concurrently.
type Cluster interface {
	ForEachPrimary(ctx context.Context, fn func(context.Context, Node) error) error
}

// Options tunes the cost/accuracy trade-off of a scan.
type Options struct {
	// ScanCount is the COUNT hint per SCAN call — the page size.
	ScanCount int64
	// SampleRate measures one key in N per cache. Higher is cheaper and less
	// accurate; 1 measures every key.
	SampleRate int
	// MinSamples is the number of keys measured unconditionally per cache
	// before SampleRate applies, so a cache with few keys is never left
	// unmeasured and reporting zero bytes.
	MinSamples int
}

// DefaultOptions is a starting point for a multi-million-key keyspace: pages of
// 1000, one key in 100 measured, and at least 50 keys per cache.
func DefaultOptions() Options {
	return Options{ScanCount: 1000, SampleRate: 100, MinSamples: 50}
}

// normalize replaces non-positive fields with defaults, so the zero Options is
// usable and a misconfigured SampleRate cannot divide by zero.
func (o Options) normalize() Options {
	d := DefaultOptions()
	if o.ScanCount <= 0 {
		o.ScanCount = d.ScanCount
	}
	if o.SampleRate <= 0 {
		o.SampleRate = d.SampleRate
	}
	if o.MinSamples < 0 {
		o.MinSamples = 0
	}
	return o
}

// CacheUsage is one cache's slice of the keyspace.
type CacheUsage struct {
	Cache string
	// Keys is the exact count — every key is classified, not sampled.
	Keys int64
	// SampledKeys and SampledBytes are the measured subset EstimatedBytes
	// extrapolates from; they are reported so a consumer can judge confidence.
	SampledKeys  int64
	SampledBytes int64
	// EstimatedBytes is Keys × mean sampled size, or 0 when nothing was
	// sampled. Zero here means "unmeasured", not "empty" — check Keys.
	EstimatedBytes int64
}

// Report is the outcome of one scan, sorted by EstimatedBytes descending.
type Report struct {
	Caches     []CacheUsage
	TotalKeys  int64
	TotalBytes int64
}

// Share returns cache's fraction of estimated total bytes, or 0 when the
// report is empty or the cache is absent.
func (r Report) Share(cache string) float64 {
	if r.TotalBytes == 0 {
		return 0
	}
	for _, u := range r.Caches {
		if u.Cache == cache {
			return float64(u.EstimatedBytes) / float64(r.TotalBytes)
		}
	}
	return 0
}

// accumulator collects usage across concurrent per-node walks.
type accumulator struct {
	mu   sync.Mutex
	byID map[string]*CacheUsage
	opts Options
}

// newAccumulator seeds one entry per registered cache plus the unclassified
// bucket, so a cache that has dropped to zero keys reports zero instead of
// disappearing and leaving a stale gauge behind.
func newAccumulator(opts Options) *accumulator {
	a := &accumulator{byID: make(map[string]*CacheUsage), opts: opts}
	for _, ks := range cachekeys.Keyspaces() {
		a.entry(ks.Name)
	}
	a.entry(cachekeys.Unclassified)
	return a
}

// entry returns the usage for name, creating it if absent. Callers other than
// newAccumulator must hold a.mu.
func (a *accumulator) entry(name string) *CacheUsage {
	u, ok := a.byID[name]
	if !ok {
		u = &CacheUsage{Cache: name}
		a.byID[name] = u
	}
	return u
}

// count records a key against its cache and reports whether it should be
// measured. Sampling decisions are made under the lock so the 1-in-N cadence
// holds across concurrent node walks.
func (a *accumulator) count(key string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	u := a.entry(cachekeys.Classify(key))
	u.Keys++
	return u.SampledKeys < int64(a.opts.MinSamples) || u.Keys%int64(a.opts.SampleRate) == 0
}

// measured records a successful MEMORY USAGE result for key's cache.
func (a *accumulator) measured(key string, bytes int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	u := a.entry(cachekeys.Classify(key))
	u.SampledKeys++
	u.SampledBytes += bytes
}

// report extrapolates each cache's mean sampled size over its key count.
func (a *accumulator) report() Report {
	a.mu.Lock()
	defer a.mu.Unlock()

	var out Report
	for _, u := range a.byID {
		usage := *u
		if usage.SampledKeys > 0 {
			usage.EstimatedBytes = usage.Keys * usage.SampledBytes / usage.SampledKeys
		}
		out.Caches = append(out.Caches, usage)
		out.TotalKeys += usage.Keys
		out.TotalBytes += usage.EstimatedBytes
	}
	sort.Slice(out.Caches, func(i, j int) bool {
		if out.Caches[i].EstimatedBytes != out.Caches[j].EstimatedBytes {
			return out.Caches[i].EstimatedBytes > out.Caches[j].EstimatedBytes
		}
		return out.Caches[i].Cache < out.Caches[j].Cache
	})
	return out
}

// Scan walks every primary in c and returns per-cache usage. It fails on the
// first node error rather than reporting a partial keyspace as if it were
// whole — a silently short report would understate a cache's share.
func Scan(ctx context.Context, c Cluster, opts Options) (Report, error) {
	opts = opts.normalize()
	acc := newAccumulator(opts)

	if err := c.ForEachPrimary(ctx, func(ctx context.Context, n Node) error {
		return scanNode(ctx, n, acc, opts)
	}); err != nil {
		return Report{}, fmt.Errorf("scan valkey keyspace: %w", err)
	}
	return acc.report(), nil
}

// scanNode walks one primary's keyspace to cursor exhaustion.
func scanNode(ctx context.Context, n Node, acc *accumulator, opts Options) error {
	var cursor uint64
	for {
		keys, next, err := n.Scan(ctx, cursor, opts.ScanCount)
		if err != nil {
			return fmt.Errorf("scan at cursor %d: %w", cursor, err)
		}
		for _, key := range keys {
			if !acc.count(key) {
				continue
			}
			bytes, ok, err := n.MemoryUsage(ctx, key)
			if err != nil {
				return fmt.Errorf("memory usage: %w", err)
			}
			if !ok {
				continue
			}
			acc.measured(key, bytes)
		}
		if next == 0 {
			return nil
		}
		cursor = next
	}
}

// Metrics holds the gauges a Report is published through.
type Metrics struct {
	keys       metric.Int64Gauge
	bytes      metric.Int64Gauge
	totalKeys  metric.Int64Gauge
	totalBytes metric.Int64Gauge

	// opts is the per-cache attribute set, built once. The label space is
	// closed by construction — a cache label is a registered Keyspace name or
	// Unclassified, nothing else — so the whole table is known at startup and
	// the recording path is a map lookup rather than an attribute.NewSet per
	// cache per tick. A scan records every cache on every interval for the
	// life of the process, so the per-call cost would be paid forever.
	opts map[string]metric.MeasurementOption
}

// newCacheOpts precomputes one attribute set per possible cache label.
func newCacheOpts() map[string]metric.MeasurementOption {
	spaces := cachekeys.Keyspaces()
	opts := make(map[string]metric.MeasurementOption, len(spaces)+1)
	for _, ks := range spaces {
		opts[ks.Name] = metric.WithAttributes(attribute.String("cache", ks.Name))
	}
	opts[cachekeys.Unclassified] = metric.WithAttributes(
		attribute.String("cache", cachekeys.Unclassified))
	return opts
}

// NewMetrics creates the keyspace gauges against m.
func NewMetrics(m metric.Meter) (*Metrics, error) {
	if m == nil {
		return nil, errors.New("cachescan: meter must not be nil")
	}
	keys, err := m.Int64Gauge("valkey_cache_keys",
		metric.WithDescription("Keys in the Valkey keyspace attributed to each cache."))
	if err != nil {
		return nil, fmt.Errorf("create valkey_cache_keys gauge: %w", err)
	}
	bytes, err := m.Int64Gauge("valkey_cache_bytes",
		metric.WithDescription("Estimated bytes in the Valkey keyspace attributed to each cache."),
		metric.WithUnit("By"))
	if err != nil {
		return nil, fmt.Errorf("create valkey_cache_bytes gauge: %w", err)
	}
	totalKeys, err := m.Int64Gauge("valkey_cache_keys_total",
		metric.WithDescription("Keys seen across the whole scanned keyspace."))
	if err != nil {
		return nil, fmt.Errorf("create valkey_cache_keys_total gauge: %w", err)
	}
	totalBytes, err := m.Int64Gauge("valkey_cache_bytes_total",
		metric.WithDescription("Estimated bytes across the whole scanned keyspace."),
		metric.WithUnit("By"))
	if err != nil {
		return nil, fmt.Errorf("create valkey_cache_bytes_total gauge: %w", err)
	}
	return &Metrics{
		keys: keys, bytes: bytes, totalKeys: totalKeys, totalBytes: totalBytes,
		opts: newCacheOpts(),
	}, nil
}

// Record publishes r. Percentages are left to the dashboard: exporting a share
// would bake in a denominator that a Grafana expression can compute per panel.
func (m *Metrics) Record(ctx context.Context, r Report) {
	for _, u := range r.Caches {
		// A label outside the table cannot arise from Classify, but falling back
		// to the unclassified set keeps this total rather than dropping the
		// sample — and matches what such a key would have classified as anyway.
		attrs, ok := m.opts[u.Cache]
		if !ok {
			attrs = m.opts[cachekeys.Unclassified]
		}
		m.keys.Record(ctx, u.Keys, attrs)
		m.bytes.Record(ctx, u.EstimatedBytes, attrs)
	}
	m.totalKeys.Record(ctx, r.TotalKeys)
	m.totalBytes.Record(ctx, r.TotalBytes)
}
