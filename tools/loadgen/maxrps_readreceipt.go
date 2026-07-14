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

// buildReadReceiptInputs maps a hold-window collector to the normalized step
// inputs. A single latency series ("read-receipt") gates the verdict; Pending
// is empty because read receipts are synchronous reads with no JetStream
// consumer (same as the history workload).
func buildReadReceiptInputs(targetRPS int, hold time.Duration, c *ReadReceiptCollector) rpsStepInputs {
	samples := c.Samples()
	failed := c.Failed()
	return rpsStepInputs{
		TargetRPS:    targetRPS,
		Hold:         hold,
		AttemptedOps: len(samples) + failed,
		FailedOps:    failed,
		Saturation:   c.Saturation(),
		EmitUnderrun: c.UnderrunCount(),
		Latencies: []seriesSamples{
			{Name: "read-receipt", Samples: samples},
		},
	}
}

// readReceiptWorkload drives the room-service read-receipt RPC at a given RPS.
// As with historyWorkload, the natsutil connection (*otelnats.Conn) and metrics
// server are captured by the cleanup closure, not stored on the struct.
type readReceiptWorkload struct {
	cfg            *config
	siteID         string
	targets        []readReceiptTarget
	seed           int64
	requestTimeout time.Duration
	metrics        *Metrics
	requester      ReadReceiptRequester
}

func (w *readReceiptWorkload) Label() string { return "read-receipt" }

// newReadReceiptWorkload wires NATS, the metrics server, the requester, and
// derives top-level read-receipt targets from the history fixtures. The
// returned cleanup shuts the metrics server and drains NATS.
func newReadReceiptWorkload(ctx context.Context, cfg *config, preset *HistoryPreset, seed int64, requestTimeout time.Duration) (*readReceiptWorkload, func(), error) {
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

	res := BuildHistoryFixtures(preset, seed, cfg.SiteID, time.Now().UTC())
	plan := res.FullPlan()
	w := &readReceiptWorkload{
		cfg: cfg, siteID: cfg.SiteID, targets: deriveReadReceiptTargets(&plan),
		seed: seed, requestTimeout: requestTimeout, metrics: metrics,
		requester: newNATSReadReceiptRequester(nc.NatsConn()),
	}
	cleanup := func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = srv.Shutdown(shutCtx)
		cancel()
		_ = nc.Drain()
	}
	return w, cleanup, nil
}

func (w *readReceiptWorkload) newGenerator(collector *ReadReceiptCollector, targetRPS int) *ReadReceiptGenerator {
	return NewReadReceiptGenerator(&ReadReceiptGeneratorConfig{
		Targets: w.targets, SiteID: w.siteID, Rate: targetRPS,
		RequestTimeout: w.requestTimeout, Requester: w.requester,
		Collector: collector, MaxInFlight: w.cfg.MaxInFlight,
	}, w.seed)
}

// RunStep runs warmup (discarded) then hold (measured) as two sequential
// generator runs so the hold collector contains only hold-window data.
// Mirrors historyWorkload.RunStep.
func (w *readReceiptWorkload) RunStep(ctx context.Context, targetRPS int, warmup, hold time.Duration) (rpsStepInputs, error) {
	if warmup > 0 {
		warmCollector := NewReadReceiptCollector()
		if err := runReadReceiptFor(ctx, w.newGenerator(warmCollector, targetRPS), warmup); err != nil {
			return rpsStepInputs{}, err
		}
	}
	collector := NewReadReceiptCollector()
	if err := runReadReceiptFor(ctx, w.newGenerator(collector, targetRPS), hold); err != nil {
		return rpsStepInputs{}, err
	}
	time.Sleep(2 * time.Second) // drain trailing in-flight replies into the collector
	return buildReadReceiptInputs(targetRPS, hold, collector), nil
}

// runReadReceiptFor runs gen.Run in a goroutine for d (or until ctx cancels),
// then stops it. Mirrors history's runFor.
func runReadReceiptFor(ctx context.Context, gen *ReadReceiptGenerator, d time.Duration) error {
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
