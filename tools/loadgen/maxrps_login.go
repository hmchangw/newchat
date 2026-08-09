package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nkeys"
)

// The login workload drives auth-service's POST /api/v1/auth so SLO-3
// ("successful login within the bound / eligible login attempts",
// docs/specs/o11y/o11y-slo.md §3) can be measured under load. Every other
// loadgen workload reaches NATS with a pre-provisioned creds file, which skips
// the HTTP leg entirely — auth was the one already-measurable SLO that no
// workload could exercise.

// loginCollector accumulates one step's outcomes. Only successful logins have
// a latency, while all eligible successes and failures stay in SLO-3's valid
// denominator. The verdict counts successes within the p99 bound directly;
// percentiles are diagnostic only.
type loginCollector struct {
	mu         sync.Mutex
	samples    []time.Duration
	good       int
	failed     int
	excluded   int
	saturation int
	underrun   int
}

func newLoginCollector() *loginCollector { return &loginCollector{} }

func (c *loginCollector) Record(o sloOutcome, d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch o {
	case outcomeGood:
		c.good++
		c.samples = append(c.samples, d)
	case outcomeFailed:
		c.failed++
	case outcomeExcluded:
		c.excluded++
	}
}

func (c *loginCollector) Samples() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Duration(nil), c.samples...)
}

// RecordSaturation notes an attempt the pacer had to skip because the
// in-flight pool was full. That is a load-box limit, not a service failure, so
// it feeds Saturation (which drives INCONCLUSIVE) and never FailedOps.
func (c *loginCollector) RecordSaturation() {
	c.mu.Lock()
	c.saturation++
	c.mu.Unlock()
}

// RecordUnderrun notes events the pacer could not release on schedule. Also a
// load-box limit, and distinct from saturation: underrun means the dispatch
// loop fell behind, saturation means it kept up but the pool was full.
func (c *loginCollector) RecordUnderrun(n int) {
	if n <= 0 {
		return
	}
	c.mu.Lock()
	c.underrun += n
	c.mu.Unlock()
}

func (c *loginCollector) Good() int       { return c.count(func() int { return c.good }) }
func (c *loginCollector) Failed() int     { return c.count(func() int { return c.failed }) }
func (c *loginCollector) Excluded() int   { return c.count(func() int { return c.excluded }) }
func (c *loginCollector) Saturation() int { return c.count(func() int { return c.saturation }) }
func (c *loginCollector) Underrun() int   { return c.count(func() int { return c.underrun }) }

func (c *loginCollector) count(read func() int) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return read()
}

// buildLoginInputs assembles step inputs from a hold-window collector.
// AttemptedOps is good+failed, not the raw request count: excluded attempts
// leave the denominator per o11y-slo.md §0.1, so a run against bad fixtures
// reports a small sample rather than a false failure rate.
func buildLoginInputs(targetRPS int, hold time.Duration, c *loginCollector) rpsStepInputs {
	good, failed := c.Good(), c.Failed()
	samples := c.Samples()
	return rpsStepInputs{
		TargetRPS:               targetRPS,
		Hold:                    hold,
		AttemptedOps:            good + failed,
		FailedOps:               failed,
		Saturation:              c.Saturation(),
		EmitUnderrun:            c.Underrun(),
		ErrorRateDiagnosticOnly: true,
		Latencies: []seriesSamples{{
			Name: "login", Samples: samples, DiagnosticOnly: true,
		}},
		EventRatios: []eventRatioInput{{
			Name: "SLO-3", Valid: good + failed, SuccessfulLatencies: samples,
			Target: 0.99, Bound: latencyBoundP99,
		}},
	}
}

// loginKeyPool hands out pre-generated NATS user public keys. Minting an
// ed25519 keypair per request would put the load box's CPU, not auth-service,
// on the critical path and surface as an INCONCLUSIVE ramp.
type loginKeyPool struct {
	keys []string
}

func newLoginKeyPool(size int) (*loginKeyPool, error) {
	if size <= 0 {
		return nil, fmt.Errorf("login key pool size must be > 0, got %d", size)
	}
	keys := make([]string, size)
	for i := range keys {
		kp, err := nkeys.CreateUser()
		if err != nil {
			return nil, fmt.Errorf("create nkey user %d: %w", i, err)
		}
		pub, err := kp.PublicKey()
		if err != nil {
			return nil, fmt.Errorf("public key %d: %w", i, err)
		}
		keys[i] = pub
	}
	return &loginKeyPool{keys: keys}, nil
}

// next returns the key for iteration i, cycling through the pool.
func (p *loginKeyPool) next(i int) string { return p.keys[i%len(p.keys)] }

// loginRequester issues one dev-mode auth request. net/http rather than Resty
// (which the services use): a load driver must measure the bare request, and
// Resty's retry layer would both distort latency and hide failures. Matches
// the existing outbound-call style in daily_verdict.go.
type loginRequester struct {
	url    string
	client *http.Client
}

func newLoginRequester(baseURL string, timeout time.Duration) *loginRequester {
	return &loginRequester{
		url:    baseURL + "/api/v1/auth",
		client: &http.Client{Timeout: timeout},
	}
}

// login posts the tokenless (dev-mode) auth body and returns the outcome with
// the wall-clock duration. The tokenless branch is deliberate: it exercises the
// real handler, nkey validation and JWT minting without standing up an OIDC
// provider, which is what SLO-3's auth leg measures.
func (r *loginRequester) login(ctx context.Context, account, natsPublicKey string) (sloOutcome, time.Duration) {
	body, err := json.Marshal(map[string]string{"account": account, "natsPublicKey": natsPublicKey})
	if err != nil {
		return outcomeFailed, 0
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.url, bytes.NewReader(body))
	if err != nil {
		return outcomeFailed, 0
	}
	req.Header.Set("Content-Type", "application/json")

	start := time.Now()
	resp, err := r.client.Do(req)
	elapsed := time.Since(start)
	if err != nil {
		// The step deadline cancels the context handed to every in-flight
		// request, so a window-edge request fails through no fault of the
		// service. Those are excluded rather than counted: at MaxInFlight=200
		// they would otherwise add 200 failures per step and trip the
		// error-rate SLO on a perfectly healthy service. A timeout of the
		// request itself (r.client.Timeout) leaves ctx.Err() nil and stays a
		// failure — that one is the service missing its budget.
		if ctx.Err() != nil {
			return outcomeExcluded, elapsed
		}
		return classifyHTTPStatus(0, true), elapsed
	}
	defer func() { _ = resp.Body.Close() }()
	// Drain so the connection returns to the keep-alive pool; a fresh TCP+TLS
	// handshake per request would measure the load box, not auth-service.
	_, _ = io.Copy(io.Discard, resp.Body)
	return classifyHTTPStatus(resp.StatusCode, false), elapsed
}

// loginWorkload drives auth-service logins at a given RPS.
type loginWorkload struct {
	fixtures  Fixtures
	requester *loginRequester
	keys      *loginKeyPool
	maxInFlt  int
}

func (w *loginWorkload) Label() string { return "login" }

// newLoginWorkload builds the workload from the messages fixtures, so logins
// use the same account set the rest of the load exercises.
func newLoginWorkload(cfg *config, preset *Preset, seed int64, authURL string, timeout time.Duration, poolSize int) (*loginWorkload, func(), error) {
	if authURL == "" {
		return nil, nil, fmt.Errorf("login workload requires --auth-url (or AUTH_URL)")
	}
	keys, err := newLoginKeyPool(poolSize)
	if err != nil {
		return nil, nil, fmt.Errorf("build login key pool: %w", err)
	}
	f := BuildFixtures(preset, seed, cfg.SiteID)
	if len(f.Subscriptions) == 0 {
		return nil, nil, fmt.Errorf("preset %s produced no accounts to log in as", preset.Name)
	}
	w := &loginWorkload{
		fixtures:  f,
		requester: newLoginRequester(authURL, timeout),
		keys:      keys,
		maxInFlt:  max(1, cfg.MaxInFlight),
	}
	return w, func() { w.requester.client.CloseIdleConnections() }, nil
}

// RunStep paces logins at targetRPS for warmup then hold, measuring only the
// hold window. Requests are issued from a bounded worker pool so a slow
// auth-service cannot grow unbounded goroutines on the load box.
func (w *loginWorkload) RunStep(ctx context.Context, targetRPS int, warmup, hold time.Duration) (rpsStepInputs, error) {
	if warmup > 0 {
		if err := w.drive(ctx, targetRPS, warmup, newLoginCollector()); err != nil {
			return rpsStepInputs{}, err
		}
	}
	c := newLoginCollector()
	if err := w.drive(ctx, targetRPS, hold, c); err != nil {
		return rpsStepInputs{}, err
	}
	return buildLoginInputs(targetRPS, hold, c), nil
}

// drive emits at targetRPS for d, recording every outcome into c.
//
// Uses the shared batched pacer rather than a ticker per request: at rates
// above ~1k the per-request interval falls below what the Go runtime can
// schedule, and it silently coalesces ticks — the load box would cap the
// achieved rate long before auth-service did.
func (w *loginWorkload) drive(ctx context.Context, targetRPS int, d time.Duration, c *loginCollector) error {
	runCtx, cancel := context.WithTimeout(ctx, d)
	defer cancel()

	var seq atomic.Int64
	pacedDispatch(runCtx, targetRPS, w.maxInFlt,
		c.RecordUnderrun,
		c.RecordSaturation,
		func(reqCtx context.Context) {
			i := int(seq.Add(1) - 1)
			sub := w.fixtures.Subscriptions[i%len(w.fixtures.Subscriptions)]
			outcome, elapsed := w.requester.login(reqCtx, sub.User.Account, w.keys.next(i))
			c.Record(outcome, elapsed)
		})

	// The parent being cancelled is a real abort; this step's own deadline
	// expiring is the normal end of the window.
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}
