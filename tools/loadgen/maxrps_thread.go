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

// threadWorkload drives the thread-reply send path at a given RPS. It is the
// messagesWorkload shape with ThreadFixtures and a forced frontdoor inject with
// ParentsByRoom wired into the per-step Generator. E1/E2 correlation, counters,
// and pending model are reused unchanged.
type threadWorkload struct {
	cfg       *config
	preset    *Preset
	fixtures  *ThreadFixtures
	seed      int64
	js        jetstream.JetStream
	metrics   *Metrics
	collector *Collector
	publisher Publisher
	canonical string
	durables  []string
}

func (w *threadWorkload) Label() string { return "thread" }

// newThreadWorkload wires NATS, the metrics server, the E1/E2 subscriptions, and
// the publisher. The returned cleanup unsubscribes, shuts the metrics server,
// and drains NATS. fixtures must already be seeded (rooms/subs/keys in Mongo,
// parents in Cassandra).
func newThreadWorkload(ctx context.Context, cfg *config, preset *Preset, fixtures *ThreadFixtures, seed int64) (*threadWorkload, func(), error) {
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

	w := &threadWorkload{
		cfg: cfg, preset: preset, fixtures: fixtures, seed: seed,
		js: js, metrics: metrics, collector: collector,
		publisher: newNatsCorePublisher(nc.NatsConn(), InjectFrontdoor, js),
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

func (w *threadWorkload) snapshotCounters() msgCounters {
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

func (w *threadWorkload) snapshotPending(ctx context.Context) (map[string]uint64, error) {
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

// RunStep runs a fresh thread-reply generator at targetRPS for warmup+hold,
// resetting the collector at the hold boundary so only the hold window is
// measured. Identical to messagesWorkload.RunStep except the Generator is wired
// with InjectFrontdoor + ParentsByRoom.
func (w *threadWorkload) RunStep(ctx context.Context, targetRPS int, warmup, hold time.Duration) (rpsStepInputs, error) {
	gen := NewGenerator(&GeneratorConfig{
		Preset: w.preset, Fixtures: w.fixtures.Fixtures, SiteID: w.cfg.SiteID,
		Rate: targetRPS, Inject: InjectFrontdoor, Publisher: w.publisher,
		Metrics: w.metrics, Collector: w.collector,
		ParentsByRoom:  w.fixtures.ParentsByRoom,
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
