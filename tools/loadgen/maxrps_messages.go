package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
)

// msgCounters is a point-in-time snapshot of the loadgen publish counters.
type msgCounters struct {
	published float64
	err       map[string]float64 // keyed by reason
}

var msgErrorReasons = []string{"publish", "marshal", "gatekeeper", "bad_reply", "saturated"}

// diffCounters returns end-start for published and each tracked reason.
func diffCounters(start, end msgCounters) msgCounters {
	d := msgCounters{published: end.published - start.published, err: map[string]float64{}}
	for _, r := range msgErrorReasons {
		d.err[r] = end.err[r] - start.err[r]
	}
	return d
}

// buildMessagesInputs assembles the normalized step inputs from a counter delta,
// the hold-window latency tapes, and the pending snapshots.
//
// Error accounting (see spec §5): FailedOps counts hard publish/gatekeeper errors
// only; missing replies/broadcasts are NOT counted (late stragglers would create
// false trips) — slow/dropped delivery is caught by latency and pending-growth.
func buildMessagesInputs(
	targetRPS int, hold time.Duration, delta msgCounters,
	e1, e2 []time.Duration,
	startPending, endPending map[string]uint64,
	durables []string, pendingOK bool,
) rpsStepInputs {
	attempted := int(delta.published + delta.err["publish"] + delta.err["marshal"])
	failed := int(delta.err["publish"] + delta.err["marshal"] + delta.err["gatekeeper"] + delta.err["bad_reply"])
	in := rpsStepInputs{
		TargetRPS:    targetRPS,
		Hold:         hold,
		AttemptedOps: attempted,
		FailedOps:    failed,
		Saturation:   int(delta.err["saturated"]),
		Latencies: []seriesSamples{
			{Name: "E1", Samples: e1},
			{Name: "E2", Samples: e2},
		},
	}
	if !pendingOK {
		in.Inconclusive = true
		in.InconclusiveReason = "consumer pending snapshot failed — backlog signal unavailable"
		return in
	}
	for _, d := range durables {
		in.Pending = append(in.Pending, consumerPendingDelta{Durable: d, Start: startPending[d], End: endPending[d]})
	}
	return in
}

// messagesWorkload drives the messaging pipeline at a given RPS.
// The natsutil connection and metrics server are not stored on the struct
// (natsutil.Connect returns *otelnats.Conn); they are captured by the cleanup
// closure instead, so the adapter only keeps what RunStep needs.
type messagesWorkload struct {
	cfg       *config
	preset    *Preset
	fixtures  Fixtures
	inject    InjectMode
	seed      int64
	js        jetstream.JetStream
	metrics   *Metrics
	collector *Collector
	publisher Publisher
	canonical string
	durables  []string
}

func (w *messagesWorkload) Label() string { return "messages" }

// newMessagesWorkload wires NATS, the metrics server, the E1/E2 subscriptions,
// and the publisher. The returned cleanup unsubscribes, shuts the metrics server
// and drains NATS.
func newMessagesWorkload(ctx context.Context, cfg *config, preset *Preset, inject InjectMode, seed int64) (*messagesWorkload, func(), error) {
	nc, err := natsutil.Connect(cfg.NatsURL, cfg.NatsCredsFile)
	if err != nil {
		return nil, nil, fmt.Errorf("nats connect: %w", err)
	}
	js, err := jetstream.New(nc.NatsConn())
	if err != nil {
		_ = nc.Drain()
		return nil, nil, fmt.Errorf("jetstream init: %w", err)
	}
	metrics := NewMetrics()
	srv := &http.Server{Addr: cfg.MetricsAddr, Handler: metrics.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Warn("metrics server stopped", "error", err)
		}
	}()
	shutdownSrv := func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}

	collector := NewCollector(metrics, preset.Name)

	e1Sub, err := nc.NatsConn().Subscribe(subject.UserResponseWildcard(), func(msg *nats.Msg) {
		reqID := lastToken(msg.Subject)
		var payload struct {
			Error string `json:"error"`
		}
		if err := json.Unmarshal(msg.Data, &payload); err != nil {
			metrics.PublishErrors.WithLabelValues(preset.Name, "bad_reply").Inc()
			return
		}
		if payload.Error != "" {
			metrics.PublishErrors.WithLabelValues(preset.Name, "gatekeeper").Inc()
		}
		collector.RecordReply(reqID, time.Now())
	})
	if err != nil {
		shutdownSrv()
		_ = nc.Drain()
		return nil, nil, fmt.Errorf("subscribe e1: %w", err)
	}
	e2Handler := newE2Handler(collector)
	e2Sub, err := nc.NatsConn().Subscribe(subject.RoomEventWildcard(), e2Handler)
	if err != nil {
		shutdownSrv()
		_ = e1Sub.Unsubscribe()
		_ = nc.Drain()
		return nil, nil, fmt.Errorf("subscribe e2: %w", err)
	}
	e2DMSub, err := nc.NatsConn().Subscribe(subject.UserRoomEventWildcard(), e2Handler)
	if err != nil {
		shutdownSrv()
		_ = e1Sub.Unsubscribe()
		_ = e2Sub.Unsubscribe()
		_ = nc.Drain()
		return nil, nil, fmt.Errorf("subscribe e2 dm: %w", err)
	}

	w := &messagesWorkload{
		cfg: cfg, preset: preset, fixtures: BuildFixtures(preset, seed, cfg.SiteID),
		inject: inject, seed: seed, js: js, metrics: metrics, collector: collector,
		publisher: newNatsCorePublisher(nc.NatsConn(), inject, js),
		canonical: stream.MessagesCanonical(cfg.SiteID).Name,
		durables:  []string{"message-worker", "broadcast-worker"},
	}
	cleanup := func() {
		_ = e1Sub.Unsubscribe()
		_ = e2Sub.Unsubscribe()
		_ = e2DMSub.Unsubscribe()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = srv.Shutdown(shutCtx)
		cancel()
		_ = nc.Drain()
	}
	return w, cleanup, nil
}

func (w *messagesWorkload) snapshotCounters() msgCounters {
	mfs, err := w.metrics.Registry.Gather()
	if err != nil {
		slog.Warn("metrics gather", "error", err)
	}
	c := msgCounters{
		published: gatheredCounterValue(mfs, "loadgen_published_total", "", ""),
		err:       map[string]float64{},
	}
	for _, reason := range msgErrorReasons {
		c.err[reason] = gatheredCounterValue(mfs, "loadgen_publish_errors_total", "reason", reason)
	}
	return c
}

func (w *messagesWorkload) snapshotPending(ctx context.Context) (map[string]uint64, error) {
	out := map[string]uint64{}
	for _, d := range w.durables {
		cons, err := w.js.Consumer(ctx, w.canonical, d)
		if err != nil {
			return nil, fmt.Errorf("consumer %s: %w", d, err)
		}
		info, err := cons.Info(ctx)
		if err != nil {
			return nil, fmt.Errorf("consumer info %s: %w", d, err)
		}
		out[d] = info.NumPending
	}
	return out, nil
}

// RunStep runs a fresh generator at targetRPS for warmup+hold, resetting the
// collector at the hold boundary so only the hold window is measured.
func (w *messagesWorkload) RunStep(ctx context.Context, targetRPS int, warmup, hold time.Duration) (rpsStepInputs, error) {
	gen := NewGenerator(&GeneratorConfig{
		Preset: w.preset, Fixtures: w.fixtures, SiteID: w.cfg.SiteID,
		Rate: targetRPS, Inject: w.inject, Publisher: w.publisher,
		Metrics: w.metrics, Collector: w.collector,
		WarmupDeadline: time.Now().Add(warmup), MaxInFlight: w.cfg.MaxInFlight,
	}, w.seed)

	genCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = gen.Run(genCtx)
	}()

	if err := waitOrCancel(ctx, warmup); err != nil {
		cancel()
		wg.Wait()
		return rpsStepInputs{}, err
	}

	holdStart := time.Now()
	w.collector.Reset()
	startCounts := w.snapshotCounters()
	startPending, perr1 := w.snapshotPending(ctx)

	holdErr := waitOrCancel(ctx, hold)

	// Counters are snapshotted at hold-end, before the drain: gatekeeper/bad_reply
	// errors whose reply lands during the drain are deliberately excluded (see the
	// straggler-exclusion rationale on buildMessagesInputs). The drain only lets
	// trailing E1/E2 latency samples settle for the percentile signals.
	endCounts := w.snapshotCounters()
	endPending, perr2 := w.snapshotPending(ctx)
	cancel()
	wg.Wait()
	time.Sleep(2 * time.Second) // drain trailing replies/broadcasts
	w.collector.DiscardBefore(holdStart)

	if holdErr != nil {
		return rpsStepInputs{}, holdErr
	}

	delta := diffCounters(startCounts, endCounts)
	pendingOK := perr1 == nil && perr2 == nil
	if !pendingOK {
		slog.Warn("pending snapshot failed", "start_err", perr1, "end_err", perr2)
	}
	return buildMessagesInputs(targetRPS, hold, delta,
		w.collector.E1Samples(), w.collector.E2Samples(),
		startPending, endPending, w.durables, pendingOK), nil
}
