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

// roomReadLatencies extracts the latency tape from a sample slice.
func roomReadLatencies(samples []RoomReadSample) []time.Duration {
	out := make([]time.Duration, len(samples))
	for i := range samples {
		out[i] = samples[i].Latency
	}
	return out
}

// buildRoomReadInputs assembles normalized step inputs from a (hold-only)
// collector. message.read is synchronous request/reply, so there is no consumer
// queue and Pending stays empty; the single "room-read" latency series gates.
func buildRoomReadInputs(targetRPS int, hold time.Duration, c *RoomReadCollector) rpsStepInputs {
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
			{Name: "room-read", Samples: roomReadLatencies(samples)},
		},
	}
}

// roomReadWorkload drives message.read requests at a given RPS. As with the
// other workloads the natsutil connection and metrics server are captured by the
// cleanup closure, not stored on the struct.
type roomReadWorkload struct {
	cfg            *config
	preset         *Preset
	fixtures       Fixtures
	seed           int64
	requestTimeout time.Duration
	metrics        *Metrics
	requester      RoomReadRequester
}

func (w *roomReadWorkload) Label() string { return "room-read" }

// newRoomReadWorkload connects NATS, starts the metrics server, and builds the
// fixtures used for target selection. Only room IDs and subscriber accounts are
// read from the fixtures (both deterministic on seed), so selection stays
// consistent with whatever `loadgen seed --workload=room-read` wrote earlier.
func newRoomReadWorkload(ctx context.Context, cfg *config, preset *Preset, seed int64, requestTimeout time.Duration) (*roomReadWorkload, func(), error) {
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
	w := &roomReadWorkload{
		cfg:            cfg,
		preset:         preset,
		fixtures:       BuildRoomReadFixtures(preset, seed, cfg.SiteID, time.Now().UTC()),
		seed:           seed,
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

func (w *roomReadWorkload) newGenerator(collector *RoomReadCollector, targetRPS int) *roomReadGenerator {
	return newRoomReadGenerator(&roomReadGeneratorConfig{
		Fixtures:       &w.fixtures,
		SiteID:         w.cfg.SiteID,
		Rate:           targetRPS,
		RequestTimeout: w.requestTimeout,
		Requester:      w.requester,
		Collector:      collector,
		MaxInFlight:    w.cfg.MaxInFlight,
	}, w.seed)
}

// runRoomReadFor runs gen.Run for d (or until ctx cancels), then stops it and
// waits for all in-flight requests to drain. Mirrors maxrps_history.go's runFor
// for the room-read generator type (runFor is typed to *HistoryGenerator and
// cannot accept *roomReadGenerator).
func runRoomReadFor(ctx context.Context, gen *roomReadGenerator, d time.Duration) error {
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
// generator runs so the hold collector contains only hold-window data. No
// post-hold sleep is needed: runRoomReadFor's wg.Wait already drains every
// in-flight synchronous request into the collector before RunStep returns.
func (w *roomReadWorkload) RunStep(ctx context.Context, targetRPS int, warmup, hold time.Duration) (rpsStepInputs, error) {
	if warmup > 0 {
		warmCollector := NewRoomReadCollector()
		if err := runRoomReadFor(ctx, w.newGenerator(warmCollector, targetRPS), warmup); err != nil {
			return rpsStepInputs{}, err
		}
	}
	collector := NewRoomReadCollector()
	if err := runRoomReadFor(ctx, w.newGenerator(collector, targetRPS), hold); err != nil {
		return rpsStepInputs{}, err
	}
	return buildRoomReadInputs(targetRPS, hold, collector), nil
}
