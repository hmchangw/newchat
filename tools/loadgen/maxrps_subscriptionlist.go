package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// subscriptionListLatencies extracts the latency tape from a sample slice.
func subscriptionListLatencies(samples []SubscriptionListSample) []time.Duration {
	out := make([]time.Duration, len(samples))
	for i := range samples {
		out[i] = samples[i].Latency
	}
	return out
}

// buildSubscriptionListInputs assembles normalized step inputs from a
// (hold-only) collector. subscription.list is synchronous request/reply, so
// there is no consumer queue and Pending stays empty; the single
// "subscription-list" latency series gates.
//
// Empty pages join the failure count. Every seeded account owns subscriptions,
// so a zero-row reply means the fixtures or the list type are wrong — and since
// an empty page is also the fastest possible reply, scoring it as a success
// would let a misconfigured run report a record-breaking ramp.
func buildSubscriptionListInputs(targetRPS int, hold time.Duration, c *SubscriptionListCollector) rpsStepInputs {
	samples := c.Samples()
	failed := c.TimeoutErrors() + c.ReplyErrors() + c.BadReplyCount() + c.EmptyPageCount()
	return rpsStepInputs{
		TargetRPS:    targetRPS,
		Hold:         hold,
		AttemptedOps: len(samples) + failed,
		FailedOps:    failed,
		Saturation:   c.SaturationCount(),
		EmitUnderrun: c.UnderrunCount(),
		Latencies: []seriesSamples{
			{Name: "subscription-list", Samples: subscriptionListLatencies(samples)},
		},
	}
}

// subscriptionListWorkload drives subscription.list requests at a given RPS. As
// with the other workloads the natsutil connection and metrics server are
// captured by the cleanup closure, not stored on the struct.
type subscriptionListWorkload struct {
	cfg                *config
	fixtures           Fixtures
	seed               int64
	requestTimeout     time.Duration
	requester          SubscriptionListRequester
	listType           string
	limit              int
	includeLastMessage *bool
}

func (w *subscriptionListWorkload) Label() string { return "subscription-list" }

// subscriptionListParams are the request-shape knobs runMaxRPS passes through.
type subscriptionListParams struct {
	ListType           string
	Limit              int
	IncludeLastMessage *bool
	RequestTimeout     time.Duration
}

// newSubscriptionListWorkload connects NATS, starts the metrics server, and
// builds the fixtures used for account selection. Only subscription accounts
// are read from the fixtures (deterministic on seed), so selection stays
// consistent with whatever `loadgen seed --workload=subscription-list` wrote.
func newSubscriptionListWorkload(ctx context.Context, cfg *config, preset *Preset, seed int64, p subscriptionListParams) (*subscriptionListWorkload, func(), error) {
	nc, err := dialNATS(cfg.NatsURL, cfg.NatsCredsFile)
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
	fixtures := BuildSubscriptionListFixtures(preset, seed, cfg.SiteID, time.Now().UTC())
	// Warned, not rejected: measuring a one-row page is a legitimate thing to
	// want, as long as it is on purpose. Silently doing it is the failure mode —
	// the ramp would report the latency of a reply that is not a sidebar.
	if degeneratePageFixtures(&fixtures) {
		slog.Warn("preset yields about one subscription per account; this ramp measures a one-row page, not a sidebar cold-open — use --preset=realistic for a representative page",
			"preset", preset.Name,
			"subs_per_account", subscriptionsPerAccount(&fixtures))
	}
	w := &subscriptionListWorkload{
		cfg:                cfg,
		fixtures:           fixtures,
		seed:               seed,
		requestTimeout:     p.RequestTimeout,
		requester:          newNATSHistoryRequester(nc.NatsConn()),
		listType:           p.ListType,
		limit:              p.Limit,
		includeLastMessage: p.IncludeLastMessage,
	}
	cleanup := func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = srv.Shutdown(shutCtx)
		cancel()
		_ = nc.Drain()
	}
	return w, cleanup, nil
}

func (w *subscriptionListWorkload) newGenerator(runCtx context.Context, collector *SubscriptionListCollector, targetRPS int) *subscriptionListGenerator {
	return newSubscriptionListGenerator(&subscriptionListGeneratorConfig{
		RunCtx:             runCtx,
		Fixtures:           &w.fixtures,
		SiteID:             w.cfg.SiteID,
		Rate:               targetRPS,
		RequestTimeout:     w.requestTimeout,
		Requester:          w.requester,
		Collector:          collector,
		MaxInFlight:        w.cfg.MaxInFlight,
		ListType:           w.listType,
		Limit:              w.limit,
		IncludeLastMessage: w.includeLastMessage,
	}, w.seed)
}

// runSubscriptionListFor runs gen.Run for d (or until ctx cancels), then stops
// it and waits for all in-flight requests to drain. Mirrors
// maxrps_roomread.go's runRoomReadFor for this generator type.
func runSubscriptionListFor(ctx context.Context, gen *subscriptionListGenerator, d time.Duration) error {
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
// post-hold sleep is needed: runSubscriptionListFor's wg.Wait already drains
// every in-flight synchronous request into the collector before RunStep returns.
func (w *subscriptionListWorkload) RunStep(ctx context.Context, targetRPS int, warmup, hold time.Duration) (rpsStepInputs, error) {
	if warmup > 0 {
		warmCollector := NewSubscriptionListCollector()
		if err := runSubscriptionListFor(ctx, w.newGenerator(ctx, warmCollector, targetRPS), warmup); err != nil {
			return rpsStepInputs{}, err
		}
	}
	collector := NewSubscriptionListCollector()
	if err := runSubscriptionListFor(ctx, w.newGenerator(ctx, collector, targetRPS), hold); err != nil {
		return rpsStepInputs{}, err
	}
	// rpsStepInputs carries no row counts, so without this the page size actually
	// measured is invisible in the report — and a ramp over one-row pages looks
	// identical to a ramp over sidebar-sized ones.
	logStepPageShape(targetRPS, collector)
	return buildSubscriptionListInputs(targetRPS, hold, collector), nil
}

// logStepPageShape reports what the step actually measured: the page size, so a
// run answers "was this a real sidebar?" without re-deriving it from the preset,
// and the failure classes split out. rpsStepInputs carries only a summed
// FailedOps, so without this a broker timeout, a protocol incompatibility and
// bad fixtures all render as the same number — three problems needing three
// different responses.
func logStepPageShape(targetRPS int, c *SubscriptionListCollector) {
	samples := c.Samples()
	timeouts, replies := c.TimeoutErrors(), c.ReplyErrors()
	bad, empty := c.BadReplyCount(), c.EmptyPageCount()
	if len(samples) == 0 && timeouts+replies+bad+empty == 0 {
		return
	}
	slog.Info("subscription-list step shape",
		"rps", targetRPS,
		"pages", len(samples),
		"mean_rows", c.MeanRows(),
		"has_more_pages", c.HasMoreCount(),
		"timeout_errors", timeouts,
		"service_errors", replies,
		"bad_replies", bad,
		"empty_pages", empty)
}
