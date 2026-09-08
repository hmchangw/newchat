// Command cache-metrics-worker publishes per-cache Valkey memory attribution.
//
// Valkey reports one used_memory per node and nothing decomposes it by
// application cache. This worker periodically walks the keyspace, classifies
// each key through pkg/cachekeys, and exports valkey_cache_keys and
// valkey_cache_bytes per cache — including an "unclassified" series that makes
// a cache added without a cachekeys entry visible instead of invisible.
//
// It stands alone rather than riding in an existing service because the scan is
// O(keyspace) and must never share a process with a request path.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/hmchangw/chat/pkg/cachescan"
	"github.com/hmchangw/chat/pkg/health"
	"github.com/hmchangw/chat/pkg/obs"
	"github.com/hmchangw/chat/pkg/shutdown"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

type config struct {
	ValkeyAddrs    []string `env:"VALKEY_ADDRS,required" envSeparator:","`
	ValkeyPassword string   `env:"VALKEY_PASSWORD"       envDefault:""`
	SiteID         string   `env:"SITE_ID"               envDefault:"site-local"`

	// ScanInterval is how often the keyspace is walked. Memory shares move
	// slowly; a short interval buys nothing and costs a full walk each time.
	ScanInterval time.Duration `env:"CACHE_SCAN_INTERVAL" envDefault:"5m"`
	ScanTimeout  time.Duration `env:"CACHE_SCAN_TIMEOUT"  envDefault:"2m"`
	// ScanCount is the SCAN COUNT hint (page size).
	ScanCount int64 `env:"CACHE_SCAN_COUNT" envDefault:"1000"`
	// ScanSampleRate measures one key in N per cache; ScanMinSamples is the
	// per-cache floor that keeps a small cache from reporting zero bytes.
	ScanSampleRate int `env:"CACHE_SCAN_SAMPLE_RATE" envDefault:"100"`
	ScanMinSamples int `env:"CACHE_SCAN_MIN_SAMPLES" envDefault:"50"`

	HealthAddr   string `env:"HEALTH_ADDR"   envDefault:":8081"`
	PProfEnabled bool   `env:"PPROF_ENABLED" envDefault:"false"`
}

// maxScanCount caps the SCAN COUNT hint. COUNT is only a hint, but Valkey
// still assembles a page of roughly that size and scanNode holds the whole
// []string while it issues MEMORY USAGE for the sampled keys — so an
// accidental extra digit costs memory on both the server and this worker at
// once. 100k is far above any useful page size and far below a damaging one.
const maxScanCount = 100_000

// validate rejects config that would make the loop misbehave rather than fail
// loudly: a non-positive interval panics time.NewTicker, and a non-positive
// timeout cancels every scan before it starts.
//
// The three scan knobs are checked here because cachescan.Options.normalize
// treats them permissively by design — it substitutes a default for a
// non-positive count or rate and clamps a negative minimum to zero. That is
// the right behaviour for a library with an optional Options, and the wrong
// behaviour for an operator typo: CACHE_SCAN_MIN_SAMPLES=-1 would start
// cleanly and silently disable the floor that keeps a cache holding fewer
// keys than the sample rate from reporting zero bytes.
func (c *config) validate() error {
	if c.ScanInterval <= 0 {
		return fmt.Errorf("CACHE_SCAN_INTERVAL must be positive, got %v", c.ScanInterval)
	}
	if c.ScanTimeout <= 0 {
		return fmt.Errorf("CACHE_SCAN_TIMEOUT must be positive, got %v", c.ScanTimeout)
	}
	if len(c.ValkeyAddrs) == 0 {
		return fmt.Errorf("VALKEY_ADDRS must list at least one address")
	}
	if c.ScanCount <= 0 || c.ScanCount > maxScanCount {
		return fmt.Errorf("CACHE_SCAN_COUNT must be in 1..%d, got %d", maxScanCount, c.ScanCount)
	}
	if c.ScanSampleRate <= 0 {
		return fmt.Errorf("CACHE_SCAN_SAMPLE_RATE must be positive, got %d", c.ScanSampleRate)
	}
	if c.ScanMinSamples < 0 {
		return fmt.Errorf("CACHE_SCAN_MIN_SAMPLES must not be negative, got %d", c.ScanMinSamples)
	}
	return nil
}

// scanOptions projects the scan tuning knobs onto cachescan.Options.
func (c *config) scanOptions() cachescan.Options {
	return cachescan.Options{
		ScanCount:  c.ScanCount,
		SampleRate: c.ScanSampleRate,
		MinSamples: c.ScanMinSamples,
	}
}

func main() {
	cfg, err := env.ParseAs[config]()
	if err != nil {
		slog.Error("parse config", "error", err)
		os.Exit(1)
	}
	if err := cfg.validate(); err != nil {
		slog.Error("invalid config", "error", err)
		os.Exit(1)
	}

	ctx := context.Background()

	sdk, obsShutdown, err := obs.Init(ctx)
	if err != nil {
		slog.Error("init observability failed", "error", err)
		os.Exit(1)
	}

	client, err := valkeyutil.ConnectCluster(ctx, cfg.ValkeyAddrs, cfg.ValkeyPassword,
		valkeyutil.WithObservability(sdk),
		valkeyutil.WithRequireParentSpan(true),
	)
	if err != nil {
		slog.Error("valkey connect failed", "error", err)
		os.Exit(1)
	}

	// The scan needs SCAN and MEMORY USAGE, which are outside valkeyutil.Client
	// by design; Cluster is the narrow escape hatch back to the raw client.
	cluster := valkeyutil.Cluster(client)
	if cluster == nil {
		slog.Error("valkey client does not expose a cluster connection")
		os.Exit(1)
	}

	metrics, err := cachescan.NewMetrics(sdk.MeterProvider().Meter("cachescan"))
	if err != nil {
		slog.Error("init cache metrics failed", "error", err)
		os.Exit(1)
	}

	r := &runner{
		cluster: cachescan.NewCluster(cluster),
		metrics: metrics,
		opts:    cfg.scanOptions(),
		timeout: cfg.ScanTimeout,
	}

	runCtx, stopRun := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		r.run(runCtx, cfg.ScanInterval)
	}()

	healthStop, err := health.ServeWithPprof(cfg.HealthAddr, 5*time.Second, cfg.PProfEnabled)
	if err != nil {
		slog.Error("health server failed to start", "error", err)
		os.Exit(1)
	}

	slog.Info("cache-metrics-worker started",
		"site", cfg.SiteID,
		"interval", cfg.ScanInterval,
		"sample_rate", cfg.ScanSampleRate,
		"min_samples", cfg.ScanMinSamples)

	shutdown.Wait(ctx, 25*time.Second,
		func(_ context.Context) error { stopRun(); return nil },
		func(ctx context.Context) error {
			done := make(chan struct{})
			go func() { wg.Wait(); close(done) }()
			select {
			case <-done:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
		func(ctx context.Context) error { return healthStop(ctx) },
		func(_ context.Context) error { valkeyutil.Disconnect(client); return nil },
		// Flush observability LAST so all prior teardown telemetry is exported.
		func(ctx context.Context) error { return obsShutdown(ctx) },
	)
}
