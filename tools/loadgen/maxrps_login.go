package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/nats-io/nkeys"
)

// The login workload drives auth-service's POST /api/v1/auth so SLO-3
// ("successful login within the bound / eligible login attempts",
// docs/specs/o11y/o11y-slo.md §3) can be measured under load. Every other
// loadgen workload reaches NATS with a pre-provisioned creds file, which skips
// the HTTP leg entirely — auth was the one already-measurable SLO that no
// workload could exercise.

// loginCollector accumulates one step's outcomes. Only successful logins
// contribute latency samples: SLO-3 gates on "succeeded *and* within the
// bound", so timing a failure would let it move the percentile in whichever
// direction it happened to land.
type loginCollector struct {
	mu         sync.Mutex
	samples    []time.Duration
	good       int
	failed     int
	excluded   int
	saturation int
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

func (c *loginCollector) Good() int       { return c.count(func() int { return c.good }) }
func (c *loginCollector) Failed() int     { return c.count(func() int { return c.failed }) }
func (c *loginCollector) Excluded() int   { return c.count(func() int { return c.excluded }) }
func (c *loginCollector) Saturation() int { return c.count(func() int { return c.saturation }) }

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
	return rpsStepInputs{
		TargetRPS:    targetRPS,
		Hold:         hold,
		AttemptedOps: good + failed,
		FailedOps:    failed,
		Saturation:   c.Saturation(),
		Latencies:    []seriesSamples{{Name: "login", Samples: c.Samples()}},
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
func (w *loginWorkload) drive(ctx context.Context, targetRPS int, d time.Duration, c *loginCollector) error {
	runCtx, cancel := context.WithTimeout(ctx, d)
	defer cancel()

	sem := make(chan struct{}, w.maxInFlt)
	var wg sync.WaitGroup
	interval := time.Second / time.Duration(max(1, targetRPS))
	tick := time.NewTicker(max(interval, time.Microsecond))
	defer tick.Stop()

	i := 0
	for {
		select {
		case <-runCtx.Done():
			wg.Wait()
			// The parent being cancelled is a real abort; the step's own
			// deadline expiring is the normal end of the window.
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return nil
		case <-tick.C:
			sub := w.fixtures.Subscriptions[i%len(w.fixtures.Subscriptions)]
			key := w.keys.next(i)
			i++
			select {
			case sem <- struct{}{}:
			default:
				// In-flight pool full: record saturation rather than blocking
				// the pacer, which would silently lower the achieved rate.
				c.RecordSaturation()
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				outcome, elapsed := w.requester.login(runCtx, sub.User.Account, key)
				c.Record(outcome, elapsed)
			}()
		}
	}
}
