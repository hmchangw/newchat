package main

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/hmchangw/chat/pkg/cachekeys"
	"github.com/hmchangw/chat/pkg/cachescan"
)

type fakeNode struct {
	keys    []string
	scanErr error
}

func (n *fakeNode) Scan(_ context.Context, cursor uint64, _ int64) ([]string, uint64, error) {
	if n.scanErr != nil {
		return nil, 0, n.scanErr
	}
	if cursor != 0 {
		return nil, 0, nil
	}
	return n.keys, 0, nil
}

func (n *fakeNode) MemoryUsage(_ context.Context, _ string) (int64, bool, error) {
	return 250, true, nil
}

type fakeCluster struct {
	node  *fakeNode
	calls atomic.Int64
}

func (c *fakeCluster) ForEachPrimary(ctx context.Context, fn func(context.Context, cachescan.Node) error) error {
	c.calls.Add(1)
	return fn(ctx, c.node)
}

func newTestRunner(t *testing.T, c cachescan.Cluster) (*runner, sdkmetric.Reader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m, err := cachescan.NewMetrics(mp.Meter("test"))
	require.NoError(t, err)
	return &runner{
		cluster: c,
		metrics: m,
		opts:    cachescan.Options{ScanCount: 10, SampleRate: 1},
		timeout: time.Minute,
	}, reader
}

func gaugeValue(t *testing.T, reader sdkmetric.Reader, name string, attrs ...attribute.KeyValue) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
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

func TestRunner_ScanOnce_RecordsMetrics(t *testing.T) {
	cluster := &fakeCluster{node: &fakeNode{keys: []string{
		cachekeys.RoomMeta("r1"), cachekeys.RoomMeta("r2"), cachekeys.RoomSubs("r1"),
	}}}
	r, reader := newTestRunner(t, cluster)

	require.NoError(t, r.scanOnce(context.Background()))

	assert.Equal(t, int64(2), gaugeValue(t, reader, "valkey_cache_keys", attribute.String("cache", "roommeta")))
	assert.Equal(t, int64(500), gaugeValue(t, reader, "valkey_cache_bytes", attribute.String("cache", "roommeta")))
	assert.Equal(t, int64(3), gaugeValue(t, reader, "valkey_cache_keys_total"))
}

func TestRunner_ScanOnce_PropagatesScanError(t *testing.T) {
	sentinel := errors.New("scan exploded")
	r, _ := newTestRunner(t, &fakeCluster{node: &fakeNode{scanErr: sentinel}})

	err := r.scanOnce(context.Background())

	require.Error(t, err)
	assert.ErrorIs(t, err, sentinel)
}

// TestRunner_ScanOnce_AppliesTimeout proves the per-scan deadline is what
// bounds a hung scan, not the caller's context.
func TestRunner_ScanOnce_AppliesTimeout(t *testing.T) {
	blocked := make(chan struct{})
	t.Cleanup(func() { close(blocked) })

	r, _ := newTestRunner(t, blockingCluster{until: blocked})
	r.timeout = 20 * time.Millisecond

	err := r.scanOnce(context.Background())

	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

type blockingCluster struct{ until chan struct{} }

func (b blockingCluster) ForEachPrimary(ctx context.Context, _ func(context.Context, cachescan.Node) error) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.until:
		return nil
	}
}

// TestRunner_Run_ScansImmediately guards against the first report being delayed
// by a full interval after startup.
func TestRunner_Run_ScansImmediately(t *testing.T) {
	cluster := &fakeCluster{node: &fakeNode{keys: []string{cachekeys.RoomMeta("r1")}}}
	r, _ := newTestRunner(t, cluster)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); r.run(ctx, time.Hour) }()

	require.Eventually(t, func() bool { return cluster.calls.Load() >= 1 },
		2*time.Second, 5*time.Millisecond, "run must scan before the first tick")

	cancel()
	wg.Wait()
}

func TestRunner_Run_StopsOnContextCancel(t *testing.T) {
	cluster := &fakeCluster{node: &fakeNode{keys: []string{cachekeys.RoomMeta("r1")}}}
	r, _ := newTestRunner(t, cluster)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.run(ctx, 10*time.Millisecond); close(done) }()

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("run did not return after context cancel")
	}
}

// TestRunner_Run_SurvivesScanErrors keeps a transient Valkey failure from
// ending the loop and silently freezing every gauge.
func TestRunner_Run_SurvivesScanErrors(t *testing.T) {
	cluster := &fakeCluster{node: &fakeNode{scanErr: errors.New("transient")}}
	r, _ := newTestRunner(t, cluster)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); r.run(ctx, time.Millisecond) }()

	require.Eventually(t, func() bool { return cluster.calls.Load() >= 3 },
		2*time.Second, 5*time.Millisecond, "loop must keep scanning after an error")

	cancel()
	wg.Wait()
}

func TestScanOptions(t *testing.T) {
	cfg := config{ScanCount: 500, ScanSampleRate: 25, ScanMinSamples: 10}

	assert.Equal(t, cachescan.Options{ScanCount: 500, SampleRate: 25, MinSamples: 10}, cfg.scanOptions())
}

func TestConfig_Validate(t *testing.T) {
	valid := config{
		ValkeyAddrs:  []string{"valkey:6379"},
		ScanInterval: 5 * time.Minute,
		ScanTimeout:  2 * time.Minute,
	}

	tests := []struct {
		name    string
		mutate  func(*config)
		wantErr string
	}{
		{"valid", func(*config) {}, ""},
		{"zero interval", func(c *config) { c.ScanInterval = 0 }, "CACHE_SCAN_INTERVAL"},
		{"negative interval", func(c *config) { c.ScanInterval = -time.Second }, "CACHE_SCAN_INTERVAL"},
		{"zero timeout", func(c *config) { c.ScanTimeout = 0 }, "CACHE_SCAN_TIMEOUT"},
		{"no addresses", func(c *config) { c.ValkeyAddrs = nil }, "VALKEY_ADDRS"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := valid
			tc.mutate(&cfg)

			err := cfg.validate()

			if tc.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}
