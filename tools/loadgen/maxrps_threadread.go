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

// threadReadLatencies extracts the latency tape from a sample slice.
func threadReadLatencies(samples []threadReadSample) []time.Duration {
	out := make([]time.Duration, len(samples))
	for i := range samples {
		out[i] = samples[i].Latency
	}
	return out
}

// buildThreadReadInputs assembles normalized step inputs from a (hold-only)
// collector. GetThreadMessages is synchronous request/reply, so there is no
// consumer queue and Pending stays empty; the single "thread-read" series gates.
func buildThreadReadInputs(targetRPS int, hold time.Duration, c *threadReadCollector) rpsStepInputs {
	samples := c.Samples()
	failed := c.TimeoutErrors() + c.ReplyErrors() + c.BadReplyCount()
	return rpsStepInputs{
		TargetRPS:    targetRPS,
		Hold:         hold,
		AttemptedOps: len(samples) + failed,
		FailedOps:    failed,
		Saturation:   c.SaturationCount(),
		EmitUnderrun: c.UnderrunCount(),
		Latencies: []seriesSamples{
			{Name: "thread-read", Samples: threadReadLatencies(samples)},
		},
	}
}

// threadReadWorkload drives GetThreadMessages requests at a given RPS. As with
// the other read workloads the natsutil connection and metrics server are
// captured by the cleanup closure, not stored on the struct.
type threadReadWorkload struct {
	cfg            *config
	preset         *HistoryPreset
	fixtures       HistoryFixtures
	seed           int64
	pageLimit      int
	requestTimeout time.Duration
	metrics        *Metrics
	requester      HistoryRequester
}

func (w *threadReadWorkload) Label() string { return "thread-read" }

// newThreadReadWorkload connects NATS, starts the metrics server, and builds the
// history fixtures (which already carry ThreadParents). Requires CASSANDRA_HOSTS
// for parity with the history workload whose seed it depends on.
func newThreadReadWorkload(ctx context.Context, cfg *config, preset *HistoryPreset, seed int64, pageLimit int, requestTimeout time.Duration) (*threadReadWorkload, func(), error) {
	if cfg.CassandraHosts == "" {
		return nil, nil, fmt.Errorf("thread-read workload requires CASSANDRA_HOSTS")
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
	w := &threadReadWorkload{
		cfg:            cfg,
		preset:         preset,
		fixtures:       BuildHistoryFixtures(preset, seed, cfg.SiteID, time.Now().UTC()),
		seed:           seed,
		pageLimit:      pageLimit,
		requestTimeout: requestTimeout,
		metrics:        metrics,
		requester:      newNATSHistoryRequester(nc.NatsConn()),
	}
	cleanup := func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = srv.Shutdown(shutCtx)
		cancel()
		_ = nc.Drain()
	}
	return w, cleanup, nil
}

func (w *threadReadWorkload) newGenerator(collector *threadReadCollector, targetRPS int) *threadReadGenerator {
	return newThreadReadGenerator(&threadReadGeneratorConfig{
		Fixtures:       &w.fixtures,
		SiteID:         w.cfg.SiteID,
		Rate:           targetRPS,
		PageLimit:      w.pageLimit,
		RequestTimeout: w.requestTimeout,
		Requester:      w.requester,
		Collector:      collector,
		MaxInFlight:    w.cfg.MaxInFlight,
	}, w.seed)
}

// runThreadReadFor runs gen.Run for d (or until ctx cancels), then stops it and
// waits for in-flight requests to drain. Mirrors runRoomReadFor for the
// thread-read generator type.
func runThreadReadFor(ctx context.Context, gen *threadReadGenerator, d time.Duration) error {
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
func (w *threadReadWorkload) RunStep(ctx context.Context, targetRPS int, warmup, hold time.Duration) (rpsStepInputs, error) {
	if warmup > 0 {
		warmCollector := newThreadReadCollector()
		if err := runThreadReadFor(ctx, w.newGenerator(warmCollector, targetRPS), warmup); err != nil {
			return rpsStepInputs{}, err
		}
	}
	collector := newThreadReadCollector()
	if err := runThreadReadFor(ctx, w.newGenerator(collector, targetRPS), hold); err != nil {
		return rpsStepInputs{}, err
	}
	return buildThreadReadInputs(targetRPS, hold, collector), nil
}
