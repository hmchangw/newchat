package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/hmchangw/chat/pkg/natsutil"
)

// latenciesOf extracts the latency tape from a sample slice.
func latenciesOf(samples []HistorySample) []time.Duration {
	out := make([]time.Duration, len(samples))
	for i := range samples {
		out[i] = samples[i].Latency
	}
	return out
}

// buildHistoryInputs assembles normalized step inputs from a (hold-only) history
// collector. Per-endpoint latency series gate independently; no consumer queue
// exists for synchronous reads so Pending is empty.
func buildHistoryInputs(targetRPS int, hold time.Duration, c *HistoryCollector) rpsStepInputs {
	hist := c.HistorySamples()
	thread := c.ThreadSamples()
	failed := c.TimeoutErrors() + c.ReplyErrors() + c.BadReplyCount()
	attempted := len(hist) + len(thread) + failed
	return rpsStepInputs{
		TargetRPS:    targetRPS,
		Hold:         hold,
		AttemptedOps: attempted,
		FailedOps:    failed,
		Saturation:   c.SaturationCount(),
		Latencies: []seriesSamples{
			{Name: "history", Samples: latenciesOf(hist)},
			{Name: "thread", Samples: latenciesOf(thread)},
		},
	}
}

// historyWorkload drives history-service read requests at a given RPS.
// As with messagesWorkload, the natsutil connection (*otelnats.Conn) and metrics
// server are captured by the cleanup closure, not stored on the struct.
type historyWorkload struct {
	cfg             *config
	preset          *HistoryPreset
	fixtures        HistoryFixtures
	seed            int64
	mix             EndpointMix
	beforeMode      BeforeMode
	scrollbackPages int
	pageLimit       int
	requestTimeout  time.Duration
	metrics         *Metrics
	requester       HistoryRequester
}

func (w *historyWorkload) Label() string { return "history" }

// historyWorkloadParams bundles the history-specific tunables.
type historyWorkloadParams struct {
	Mix             EndpointMix
	BeforeMode      BeforeMode
	ScrollbackPages int
	PageLimit       int
	RequestTimeout  time.Duration
}

func newHistoryWorkload(ctx context.Context, cfg *config, preset *HistoryPreset, seed int64, p historyWorkloadParams) (*historyWorkload, func(), error) {
	if cfg.CassandraHosts == "" {
		return nil, nil, fmt.Errorf("history workload requires CASSANDRA_HOSTS")
	}
	nc, err := natsutil.Connect(cfg.NatsURL, cfg.NatsCredsFile)
	if err != nil {
		return nil, nil, fmt.Errorf("nats connect: %w", err)
	}
	metrics := NewMetrics()
	srv := &http.Server{Addr: cfg.MetricsAddr, Handler: metrics.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Warn("metrics server stopped", "error", err)
		}
	}()
	w := &historyWorkload{
		cfg: cfg, preset: preset, fixtures: BuildHistoryFixtures(preset, seed, cfg.SiteID, time.Now().UTC()),
		seed: seed, mix: p.Mix, beforeMode: p.BeforeMode, scrollbackPages: p.ScrollbackPages,
		pageLimit: p.PageLimit, requestTimeout: p.RequestTimeout,
		metrics: metrics, requester: newNATSHistoryRequester(nc.NatsConn()),
	}
	cleanup := func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = srv.Shutdown(shutCtx)
		cancel()
		_ = nc.Drain()
	}
	return w, cleanup, nil
}

func (w *historyWorkload) newGenerator(collector *HistoryCollector, targetRPS int) *HistoryGenerator {
	return NewHistoryGenerator(&HistoryGeneratorConfig{
		Preset: w.preset, Fixtures: &w.fixtures, SiteID: w.cfg.SiteID, Rate: targetRPS,
		Mix: w.mix, BeforeMode: w.beforeMode, ScrollbackPages: w.scrollbackPages,
		PageLimit: w.pageLimit, RequestTimeout: w.requestTimeout,
		Requester: w.requester, Collector: collector, MaxInFlight: w.cfg.MaxInFlight,
	}, w.seed)
}

// runFor runs gen.Run in a goroutine for d (or until ctx cancels), then stops it.
func runFor(ctx context.Context, gen *HistoryGenerator, d time.Duration) error {
	genCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = gen.Run(genCtx)
	}()
	err := waitOrCancel(ctx, d)
	cancel()
	wg.Wait()
	return err
}

// RunStep runs warmup (discarded) then hold (measured) as two sequential
// generator runs so the hold collector contains only hold-window data.
func (w *historyWorkload) RunStep(ctx context.Context, targetRPS int, warmup, hold time.Duration) (rpsStepInputs, error) {
	if warmup > 0 {
		warmCollector := NewHistoryCollector()
		if err := runFor(ctx, w.newGenerator(warmCollector, targetRPS), warmup); err != nil {
			return rpsStepInputs{}, err
		}
	}
	collector := NewHistoryCollector()
	if err := runFor(ctx, w.newGenerator(collector, targetRPS), hold); err != nil {
		return rpsStepInputs{}, err
	}
	time.Sleep(2 * time.Second) // drain trailing in-flight replies into the collector
	return buildHistoryInputs(targetRPS, hold, collector), nil
}
