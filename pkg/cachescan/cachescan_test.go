package cachescan

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/hmchangw/chat/pkg/cachekeys"
)

// fakeNode serves a fixed key list in pages and a per-key size.
type fakeNode struct {
	keys      []string
	sizes     map[string]int64
	missing   map[string]bool // key vanished between SCAN and MEMORY USAGE
	scanErr   error
	memErr    error
	mu        sync.Mutex
	memCalls  int
	scanCalls int
}

func (n *fakeNode) Scan(_ context.Context, cursor uint64, count int64) ([]string, uint64, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.scanCalls++
	if n.scanErr != nil {
		return nil, 0, n.scanErr
	}
	// Page in uint64 throughout: converting the cursor to int would be an
	// unchecked narrowing conversion (gosec G115).
	total := uint64(len(n.keys))
	if cursor >= total {
		return nil, 0, nil
	}
	page := uint64(1)
	if count > 0 {
		page = uint64(count)
	}
	end := min(cursor+page, total)
	next := end
	if end == total {
		next = 0
	}
	return n.keys[cursor:end], next, nil
}

func (n *fakeNode) MemoryUsage(_ context.Context, key string) (int64, bool, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.memCalls++
	if n.memErr != nil {
		return 0, false, n.memErr
	}
	if n.missing[key] {
		return 0, false, nil
	}
	if sz, ok := n.sizes[key]; ok {
		return sz, true, nil
	}
	return 100, true, nil
}

// fakeCluster fans out over its nodes concurrently, like go-redis ForEachMaster.
type fakeCluster struct {
	nodes []*fakeNode
	err   error
}

func (c *fakeCluster) ForEachPrimary(ctx context.Context, fn func(context.Context, Node) error) error {
	if c.err != nil {
		return c.err
	}
	var wg sync.WaitGroup
	errs := make([]error, len(c.nodes))
	for i, n := range c.nodes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = fn(ctx, n)
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}

func singleNode(keys []string, sizes map[string]int64) *fakeCluster {
	return &fakeCluster{nodes: []*fakeNode{{keys: keys, sizes: sizes}}}
}

// usage returns the CacheUsage for cache, or the zero value.
func usage(t *testing.T, r Report, cache string) CacheUsage {
	t.Helper()
	for _, u := range r.Caches {
		if u.Cache == cache {
			return u
		}
	}
	t.Fatalf("cache %q absent from report", cache)
	return CacheUsage{}
}

// measureAll samples every key, making estimation exact.
func measureAll() Options { return Options{ScanCount: 10, SampleRate: 1} }

func TestScan_ClassifiesAndCounts(t *testing.T) {
	keys := []string{
		cachekeys.RoomMeta("r1"),
		cachekeys.RoomMeta("r2"),
		cachekeys.RoomSubs("r1"),
		cachekeys.BotIdempotency("op1"),
		"totally:unknown:key",
	}
	sizes := map[string]int64{
		cachekeys.RoomMeta("r1"):        300,
		cachekeys.RoomMeta("r2"):        500,
		cachekeys.RoomSubs("r1"):        1000,
		cachekeys.BotIdempotency("op1"): 80,
		"totally:unknown:key":           40,
	}

	got, err := Scan(context.Background(), singleNode(keys, sizes), measureAll())
	require.NoError(t, err)

	assert.Equal(t, int64(5), got.TotalKeys)
	assert.Equal(t, int64(1920), got.TotalBytes)

	meta := usage(t, got, "roommeta")
	assert.Equal(t, int64(2), meta.Keys)
	assert.Equal(t, int64(800), meta.EstimatedBytes)

	assert.Equal(t, int64(1000), usage(t, got, "roomsubs").EstimatedBytes)
	assert.Equal(t, int64(80), usage(t, got, "botidempotency").EstimatedBytes)
	assert.Equal(t, int64(40), usage(t, got, cachekeys.Unclassified).EstimatedBytes)
}

// TestScan_ReportsEveryRegisteredCache keeps dashboards stable: a cache that
// drops to zero keys must report zero rather than vanish and leave a stale gauge.
func TestScan_ReportsEveryRegisteredCache(t *testing.T) {
	got, err := Scan(context.Background(), singleNode([]string{cachekeys.RoomMeta("r1")}, nil), measureAll())
	require.NoError(t, err)

	for _, ks := range cachekeys.Keyspaces() {
		u := usage(t, got, ks.Name)
		if ks.Name != "roommeta" {
			assert.Zero(t, u.Keys, "cache %q should report zero keys", ks.Name)
			assert.Zero(t, u.EstimatedBytes, "cache %q should report zero bytes", ks.Name)
		}
	}
	assert.NotPanics(t, func() { usage(t, got, cachekeys.Unclassified) })
}

func TestScan_SortsByEstimatedBytesDescending(t *testing.T) {
	keys := []string{cachekeys.RoomMeta("r1"), cachekeys.RoomSubs("r1"), cachekeys.BotIdempotency("op1")}
	sizes := map[string]int64{
		cachekeys.RoomMeta("r1"):        200,
		cachekeys.RoomSubs("r1"):        900,
		cachekeys.BotIdempotency("op1"): 500,
	}

	got, err := Scan(context.Background(), singleNode(keys, sizes), measureAll())
	require.NoError(t, err)

	require.GreaterOrEqual(t, len(got.Caches), 3)
	assert.Equal(t, "roomsubs", got.Caches[0].Cache)
	assert.Equal(t, "botidempotency", got.Caches[1].Cache)
	assert.Equal(t, "roommeta", got.Caches[2].Cache)
}

// TestScan_EstimatesFromSample is the core extrapolation: with 100 same-sized
// keys and 1-in-10 sampling, the estimate must still be exact.
func TestScan_EstimatesFromSample(t *testing.T) {
	var keys []string
	sizes := map[string]int64{}
	for i := range 100 {
		k := cachekeys.RoomMeta(string(rune('a'+i/26)) + string(rune('a'+i%26)))
		keys = append(keys, k)
		sizes[k] = 400
	}

	got, err := Scan(context.Background(), singleNode(keys, sizes),
		Options{ScanCount: 25, SampleRate: 10, MinSamples: 0})
	require.NoError(t, err)

	meta := usage(t, got, "roommeta")
	assert.Equal(t, int64(100), meta.Keys)
	assert.Equal(t, int64(10), meta.SampledKeys, "1 in 10 keys should be measured")
	assert.Equal(t, int64(40000), meta.EstimatedBytes)
}

// TestScan_MinSamplesCoversSmallCaches guards the failure where a rare cache is
// never sampled and reports zero bytes despite holding keys.
func TestScan_MinSamplesCoversSmallCaches(t *testing.T) {
	var keys []string
	for i := range 500 {
		keys = append(keys, cachekeys.RoomMeta(string(rune('a'+i/26))+string(rune('a'+i%26))))
	}
	keys = append(keys, cachekeys.BotIdempotency("lonely"))
	sizes := map[string]int64{cachekeys.BotIdempotency("lonely"): 777}

	got, err := Scan(context.Background(), singleNode(keys, sizes),
		Options{ScanCount: 100, SampleRate: 1000, MinSamples: 1})
	require.NoError(t, err)

	idem := usage(t, got, "botidempotency")
	assert.Equal(t, int64(1), idem.Keys)
	assert.Equal(t, int64(1), idem.SampledKeys)
	assert.Equal(t, int64(777), idem.EstimatedBytes)
}

func TestScan_UnsampledCacheReportsZeroBytesNotGarbage(t *testing.T) {
	keys := []string{cachekeys.RoomMeta("r1"), cachekeys.RoomMeta("r2")}

	got, err := Scan(context.Background(), singleNode(keys, nil),
		Options{ScanCount: 10, SampleRate: 1000, MinSamples: 0})
	require.NoError(t, err)

	meta := usage(t, got, "roommeta")
	assert.Equal(t, int64(2), meta.Keys)
	assert.Zero(t, meta.SampledKeys)
	assert.Zero(t, meta.EstimatedBytes)
}

// TestScan_KeyVanishedBetweenScanAndMeasure covers the inherent race: a TTL can
// expire a key after SCAN returned it. It counts toward Keys but not the sample.
func TestScan_KeyVanishedBetweenScanAndMeasure(t *testing.T) {
	k1, k2 := cachekeys.RoomMeta("r1"), cachekeys.RoomMeta("r2")
	node := &fakeNode{
		keys:    []string{k1, k2},
		sizes:   map[string]int64{k2: 600},
		missing: map[string]bool{k1: true},
	}

	got, err := Scan(context.Background(), &fakeCluster{nodes: []*fakeNode{node}}, measureAll())
	require.NoError(t, err)

	meta := usage(t, got, "roommeta")
	assert.Equal(t, int64(2), meta.Keys)
	assert.Equal(t, int64(1), meta.SampledKeys)
	assert.Equal(t, int64(1200), meta.EstimatedBytes, "estimate extrapolates the surviving sample")
}

func TestScan_AggregatesAcrossNodesConcurrently(t *testing.T) {
	nodes := []*fakeNode{
		{keys: []string{cachekeys.RoomMeta("a")}, sizes: map[string]int64{cachekeys.RoomMeta("a"): 100}},
		{keys: []string{cachekeys.RoomMeta("b")}, sizes: map[string]int64{cachekeys.RoomMeta("b"): 300}},
		{keys: []string{cachekeys.RoomSubs("c")}, sizes: map[string]int64{cachekeys.RoomSubs("c"): 700}},
	}

	got, err := Scan(context.Background(), &fakeCluster{nodes: nodes}, measureAll())
	require.NoError(t, err)

	assert.Equal(t, int64(3), got.TotalKeys)
	assert.Equal(t, int64(1100), got.TotalBytes)
	assert.Equal(t, int64(400), usage(t, got, "roommeta").EstimatedBytes)
}

func TestScan_PagesThroughCursor(t *testing.T) {
	var keys []string
	for i := range 30 {
		keys = append(keys, cachekeys.RoomMeta(string(rune('a'+i))))
	}
	node := &fakeNode{keys: keys}

	got, err := Scan(context.Background(), &fakeCluster{nodes: []*fakeNode{node}}, Options{ScanCount: 7, SampleRate: 1})
	require.NoError(t, err)

	assert.Equal(t, int64(30), got.TotalKeys)
	assert.Greater(t, node.scanCalls, 1, "a 30-key node with COUNT 7 must page")
}

func TestScan_Errors(t *testing.T) {
	sentinel := errors.New("boom")

	tests := []struct {
		name    string
		cluster *fakeCluster
	}{
		{"fan-out fails", &fakeCluster{err: sentinel}},
		{"scan fails", &fakeCluster{nodes: []*fakeNode{{scanErr: sentinel}}}},
		{"memory usage fails", &fakeCluster{nodes: []*fakeNode{{
			keys: []string{cachekeys.RoomMeta("r1")}, memErr: sentinel,
		}}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Scan(context.Background(), tc.cluster, measureAll())
			require.Error(t, err)
			assert.ErrorIs(t, err, sentinel)
		})
	}
}

func TestScan_EmptyKeyspace(t *testing.T) {
	got, err := Scan(context.Background(), singleNode(nil, nil), measureAll())
	require.NoError(t, err)

	assert.Zero(t, got.TotalKeys)
	assert.Zero(t, got.TotalBytes)
	assert.NotEmpty(t, got.Caches, "registered caches still report zero")
}

func TestOptions_Normalize(t *testing.T) {
	tests := []struct {
		name string
		in   Options
		want Options
	}{
		// MinSamples is left alone at 0: unlike ScanCount and SampleRate, zero is
		// a meaningful setting ("no floor") rather than an unusable one, so
		// normalize cannot distinguish it from unset and must not overwrite it.
		// Callers wanting the floor take DefaultOptions or set it explicitly.
		{"zero value gets defaults for the unusable fields", Options{},
			Options{ScanCount: DefaultOptions().ScanCount, SampleRate: DefaultOptions().SampleRate, MinSamples: 0}},
		{"negative values are corrected", Options{ScanCount: -1, SampleRate: -5, MinSamples: -2},
			Options{ScanCount: DefaultOptions().ScanCount, SampleRate: DefaultOptions().SampleRate, MinSamples: 0}},
		{"explicit values preserved", Options{ScanCount: 5, SampleRate: 3, MinSamples: 7},
			Options{ScanCount: 5, SampleRate: 3, MinSamples: 7}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.in.normalize())
		})
	}
}

func TestReport_Share(t *testing.T) {
	r := Report{
		TotalBytes: 1000,
		Caches: []CacheUsage{
			{Cache: "roomsubs", EstimatedBytes: 750},
			{Cache: "roommeta", EstimatedBytes: 250},
		},
	}

	assert.InDelta(t, 0.75, r.Share("roomsubs"), 1e-9)
	assert.InDelta(t, 0.25, r.Share("roommeta"), 1e-9)
	assert.Zero(t, r.Share("absent"))
	assert.Zero(t, Report{}.Share("roomsubs"), "empty report must not divide by zero")
}

func gaugeValue(t *testing.T, rm metricdata.ResourceMetrics, name string, attrs ...attribute.KeyValue) int64 {
	t.Helper()
	want := attribute.NewSet(attrs...)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			g, ok := m.Data.(metricdata.Gauge[int64])
			require.True(t, ok, "metric %s is not Gauge[int64]", name)
			for _, dp := range g.DataPoints {
				if dp.Attributes.Equals(&want) {
					return dp.Value
				}
			}
		}
	}
	return -1
}

func TestMetrics_Record(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m, err := NewMetrics(mp.Meter("test"))
	require.NoError(t, err)

	m.Record(context.Background(), Report{
		TotalKeys:  3,
		TotalBytes: 900,
		Caches: []CacheUsage{
			{Cache: "roomsubs", Keys: 2, EstimatedBytes: 800},
			{Cache: "roommeta", Keys: 1, EstimatedBytes: 100},
		},
	})

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	assert.Equal(t, int64(2), gaugeValue(t, rm, "valkey_cache_keys", attribute.String("cache", "roomsubs")))
	assert.Equal(t, int64(800), gaugeValue(t, rm, "valkey_cache_bytes", attribute.String("cache", "roomsubs")))
	assert.Equal(t, int64(100), gaugeValue(t, rm, "valkey_cache_bytes", attribute.String("cache", "roommeta")))
	assert.Equal(t, int64(900), gaugeValue(t, rm, "valkey_cache_bytes_total"))
	assert.Equal(t, int64(3), gaugeValue(t, rm, "valkey_cache_keys_total"))
}

func TestNewMetrics_NilMeterIsRejected(t *testing.T) {
	_, err := NewMetrics(nil)
	require.Error(t, err)
}
