# loadgen max-rps SLO finder — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `loadgen max-rps --workload=messages|history` subcommand that ramps target RPS through an explicit step list, holds at each step under an SLO, and reports the largest RPS that held.

**Architecture:** A generic, workload-agnostic engine (`ramp.go` + `verdict.go` + `maxrps_report.go`) drives a per-workload adapter (`maxrps_messages.go` / `maxrps_history.go`) through the `rpsWorkload` interface. Adapters reuse the existing `Generator` / `HistoryGenerator`, `Collector` / `HistoryCollector`, presets, subscriptions, and `ComputePercentiles`. All identifiers are `rps`-prefixed to avoid symbol collisions with PR #234's `daily_*` code in the same `package main`.

**Tech Stack:** Go 1.25, NATS + JetStream (`nats.go`), Prometheus client, testify, testcontainers (integration). Build/test via `make` targets only.

**Spec:** `docs/superpowers/specs/2026-05-28-max-rps-slo-loadgen-design.md`

---

## File structure

All new files live in `tools/loadgen/` (`package main`), consistent with the existing flat layout.

- **Create `tools/loadgen/verdict.go`** — `rpsThresholds`, `seriesSamples`, `consumerPendingDelta`, `rpsStepInputs`, `verdictKind`, `seriesPercentile`, `rpsStepResult`, `evaluateRPSStep`. Pure logic, no I/O.
- **Create `tools/loadgen/ramp.go`** — `parseRPSSteps`, `waitOrCancel`, `rpsWorkload` interface, `rampConfig`, `runRamp`, `maxRPSExitCode`. Engine only; no NATS.
- **Create `tools/loadgen/maxrps_report.go`** — `renderRPSReport`, `writeRPSCSV`, `lastPassRPS`, `firstTrip`.
- **Create `tools/loadgen/maxrps_messages.go`** — `messagesWorkload` adapter + `newMessagesWorkload` constructor + counter/pending snapshot helpers.
- **Create `tools/loadgen/maxrps_history.go`** — `historyWorkload` adapter + `newHistoryWorkload` constructor.
- **Create `tools/loadgen/maxrps.go`** — `runMaxRPS` (flag parsing, wiring, ramp, report).
- **Modify `tools/loadgen/main.go`** — add the `max-rps` case to `dispatch` (`main.go:82-100`).
- **Modify `tools/loadgen/collector.go`** — add `Collector.Reset()`.
- **Create tests:** `verdict_test.go`, `ramp_test.go`, `maxrps_report_test.go`, `maxrps_messages_test.go`, `maxrps_history_test.go`, `maxrps_test.go`; extend `integration_test.go`.
- **Modify `tools/loadgen/README.md`** and **`tools/loadgen/deploy/Makefile`**.

---

## Task 1: Verdict types and `evaluateRPSStep`

**Files:**
- Create: `tools/loadgen/verdict.go`
- Test: `tools/loadgen/verdict_test.go`

- [ ] **Step 1: Write the failing tests**

Create `tools/loadgen/verdict_test.go`:

```go
package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func ms(n int) time.Duration { return time.Duration(n) * time.Millisecond }

// nLatencies returns a slice of n identical-latency samples.
func nLatencies(n int, d time.Duration) []time.Duration {
	out := make([]time.Duration, n)
	for i := range out {
		out[i] = d
	}
	return out
}

func defaultRPSThresholds() rpsThresholds {
	return rpsThresholds{
		P95:           ms(100),
		P99:           ms(250),
		ErrorRate:     0.001,
		PendingGrowth: 1000,
		RateTolerance: 0.05,
	}
}

func TestEvaluateRPSStep(t *testing.T) {
	th := defaultRPSThresholds()
	tests := []struct {
		name     string
		in       rpsStepInputs
		wantKind verdictKind
	}{
		{
			name: "all healthy passes",
			in: rpsStepInputs{
				TargetRPS: 1000, Hold: time.Second, AttemptedOps: 1000, FailedOps: 0,
				Latencies: []seriesSamples{{Name: "E1", Samples: nLatencies(100, ms(20))}},
				Pending:   []consumerPendingDelta{{Durable: "message-worker", Start: 0, End: 10}},
			},
			wantKind: verdictPass,
		},
		{
			name: "p95 over threshold trips",
			in: rpsStepInputs{
				TargetRPS: 1000, Hold: time.Second, AttemptedOps: 1000,
				Latencies: []seriesSamples{{Name: "E1", Samples: nLatencies(100, ms(150))}},
			},
			wantKind: verdictTrip,
		},
		{
			name: "p99 over threshold trips",
			in: rpsStepInputs{
				TargetRPS: 1000, Hold: time.Second, AttemptedOps: 1000,
				// 99 samples at 20ms, 1 at 300ms -> p95=20ms (ok), p99=300ms (>250ms).
				Latencies: []seriesSamples{{Name: "E1", Samples: append(nLatencies(99, ms(20)), ms(300))}},
			},
			wantKind: verdictTrip,
		},
		{
			name: "error rate over threshold trips",
			in: rpsStepInputs{
				TargetRPS: 1000, Hold: time.Second, AttemptedOps: 1000, FailedOps: 5, // 0.5% > 0.1%
				Latencies: []seriesSamples{{Name: "E1", Samples: nLatencies(100, ms(20))}},
			},
			wantKind: verdictTrip,
		},
		{
			name: "pending growth over threshold trips",
			in: rpsStepInputs{
				TargetRPS: 1000, Hold: time.Second, AttemptedOps: 1000,
				Latencies: []seriesSamples{{Name: "E1", Samples: nLatencies(100, ms(20))}},
				Pending:   []consumerPendingDelta{{Durable: "broadcast-worker", Start: 0, End: 1500}},
			},
			wantKind: verdictTrip,
		},
		{
			name: "per-endpoint: slow thread trips even if history fast",
			in: rpsStepInputs{
				TargetRPS: 1000, Hold: time.Second, AttemptedOps: 1000,
				Latencies: []seriesSamples{
					{Name: "history", Samples: nLatencies(100, ms(20))},
					{Name: "thread", Samples: nLatencies(100, ms(180))},
				},
			},
			wantKind: verdictTrip,
		},
		{
			name: "healthy but rate shortfall is inconclusive",
			in: rpsStepInputs{
				TargetRPS: 1000, Hold: time.Second, AttemptedOps: 800, // 80% < 95%
				Latencies: []seriesSamples{{Name: "E1", Samples: nLatencies(80, ms(20))}},
			},
			wantKind: verdictInconclusive,
		},
		{
			name: "trip beats shortfall: high latency AND low achieved is a TRIP",
			in: rpsStepInputs{
				TargetRPS: 1000, Hold: time.Second, AttemptedOps: 800, // shortfall...
				Latencies: []seriesSamples{{Name: "E1", Samples: nLatencies(80, ms(400))}}, // ...but slow
			},
			wantKind: verdictTrip,
		},
		{
			name: "explicit inconclusive flag short-circuits",
			in: rpsStepInputs{
				TargetRPS: 1000, Hold: time.Second, AttemptedOps: 1000,
				Inconclusive: true, InconclusiveReason: "pending snapshot failed",
				Latencies: []seriesSamples{{Name: "E1", Samples: nLatencies(100, ms(20))}},
			},
			wantKind: verdictInconclusive,
		},
		{
			name: "p95 exactly at threshold passes (boundary)",
			in: rpsStepInputs{
				TargetRPS: 1000, Hold: time.Second, AttemptedOps: 1000,
				Latencies: []seriesSamples{{Name: "E1", Samples: nLatencies(100, ms(100))}},
			},
			wantKind: verdictPass,
		},
		{
			name: "empty samples does not panic and passes on other signals",
			in: rpsStepInputs{
				TargetRPS: 1000, Hold: time.Second, AttemptedOps: 1000,
				Latencies: []seriesSamples{{Name: "E1", Samples: nil}},
			},
			wantKind: verdictPass,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := evaluateRPSStep(tt.in, th)
			assert.Equal(t, tt.wantKind, got.Kind, "reasons=%v", got.Reasons)
		})
	}
}

func TestEvaluateRPSStep_AchievedAndErrorRate(t *testing.T) {
	th := defaultRPSThresholds()
	in := rpsStepInputs{
		TargetRPS: 1000, Hold: 2 * time.Second, AttemptedOps: 1000, FailedOps: 100,
		Latencies: []seriesSamples{{Name: "E1", Samples: nLatencies(10, ms(20))}},
	}
	got := evaluateRPSStep(in, th)
	assert.InDelta(t, 500.0, got.AchievedRPS, 0.01) // 1000 ops / 2s
	assert.InDelta(t, 0.1, got.ErrorRate, 0.0001)   // 100/1000
}

func TestEvaluateRPSStep_WorstPendingReported(t *testing.T) {
	th := defaultRPSThresholds()
	in := rpsStepInputs{
		TargetRPS: 1000, Hold: time.Second, AttemptedOps: 1000,
		Latencies: []seriesSamples{{Name: "E1", Samples: nLatencies(10, ms(20))}},
		Pending: []consumerPendingDelta{
			{Durable: "message-worker", Start: 0, End: 50},
			{Durable: "broadcast-worker", Start: 100, End: 700}, // delta 600, the worst
		},
	}
	got := evaluateRPSStep(in, th)
	assert.Equal(t, "broadcast-worker", got.WorstDurable)
	assert.Equal(t, int64(600), got.WorstDelta)
	assert.Equal(t, verdictPass, got.Kind) // 600 < 1000
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd tools/loadgen && go test -run 'TestEvaluateRPSStep' . 2>&1 | head -20`
Expected: FAIL — `undefined: rpsThresholds`, `undefined: evaluateRPSStep`, etc.

- [ ] **Step 3: Write the implementation**

Create `tools/loadgen/verdict.go`:

```go
package main

import (
	"fmt"
	"time"
)

// rpsThresholds holds the SLO limits a step is judged against. Every gated
// latency series shares the same P95/P99 limits.
type rpsThresholds struct {
	P95, P99      time.Duration
	ErrorRate     float64
	PendingGrowth uint64 // messages only; per-durable end-start NumPending delta
	RateTolerance float64
}

// seriesSamples is one named latency tape (e.g. "E1","E2" or "history","thread").
type seriesSamples struct {
	Name    string
	Samples []time.Duration
}

// consumerPendingDelta is one durable's NumPending at the hold boundaries.
type consumerPendingDelta struct {
	Durable    string
	Start, End uint64
}

// Delta returns End-Start as a signed value (it can be negative if the backlog drained).
func (d consumerPendingDelta) Delta() int64 { return int64(d.End) - int64(d.Start) }

// rpsStepInputs is the normalized, workload-agnostic measurement of one step.
type rpsStepInputs struct {
	TargetRPS    int
	Hold         time.Duration
	AttemptedOps int
	FailedOps    int
	Saturation   int // open-loop self-saturation tally (corroborates shortfall)
	Latencies    []seriesSamples
	Pending      []consumerPendingDelta // empty for history
	// Inconclusive is set by the adapter when measurement itself failed (e.g. a
	// pending snapshot errored), independent of the system under test.
	Inconclusive       bool
	InconclusiveReason string
}

type verdictKind int

const (
	verdictPass verdictKind = iota
	verdictTrip
	verdictInconclusive
)

func (k verdictKind) String() string {
	switch k {
	case verdictTrip:
		return "TRIP"
	case verdictInconclusive:
		return "INCONCLUSIVE"
	default:
		return "PASS"
	}
}

// seriesPercentile is a named series' computed percentiles, for reporting.
type seriesPercentile struct {
	Name string
	Pct  Percentiles
}

// rpsStepResult is the verdict for one step.
type rpsStepResult struct {
	TargetRPS    int
	AchievedRPS  float64
	AttemptedOps int
	FailedOps    int
	Saturation   int
	ErrorRate    float64
	Latencies    []seriesPercentile
	WorstDurable string
	WorstDelta   int64
	Kind         verdictKind
	Reasons      []string
}

// evaluateRPSStep classifies a step PASS / TRIP / INCONCLUSIVE.
//
// Precedence (deliberately differs from PR #234):
//  1. explicit measurement-failure inconclusive (harness/measurement issue),
//  2. TRIP if any SLO signal is over threshold — server-induced backpressure
//     must NOT be misread as a harness limit,
//  3. INCONCLUSIVE if the harness could not push the target rate while the
//     system looked healthy (the load box is the limit, not the service),
//  4. PASS.
func evaluateRPSStep(in rpsStepInputs, th rpsThresholds) rpsStepResult {
	res := rpsStepResult{
		TargetRPS:    in.TargetRPS,
		AttemptedOps: in.AttemptedOps,
		FailedOps:    in.FailedOps,
		Saturation:   in.Saturation,
	}
	if in.Hold > 0 {
		res.AchievedRPS = float64(in.AttemptedOps) / in.Hold.Seconds()
	}
	if in.AttemptedOps > 0 {
		res.ErrorRate = float64(in.FailedOps) / float64(in.AttemptedOps)
	}

	// Compute percentiles for every series (always, so the report has data).
	for _, s := range in.Latencies {
		res.Latencies = append(res.Latencies, seriesPercentile{Name: s.Name, Pct: ComputePercentiles(s.Samples)})
	}
	// Worst pending delta (always, for the report column).
	res.WorstDelta = 0
	for _, p := range in.Pending {
		if d := p.Delta(); d > res.WorstDelta || res.WorstDurable == "" {
			res.WorstDelta = d
			res.WorstDurable = p.Durable
		}
	}

	// (1) Measurement failure short-circuits.
	if in.Inconclusive {
		res.Kind = verdictInconclusive
		res.Reasons = []string{in.InconclusiveReason}
		return res
	}

	// (2) TRIP conditions (accumulate human-readable reasons).
	var reasons []string
	for _, sp := range res.Latencies {
		if sp.Pct.P95 > th.P95 {
			reasons = append(reasons, fmt.Sprintf("%s p95=%s > %s", sp.Name, sp.Pct.P95, th.P95))
		}
		if sp.Pct.P99 > th.P99 {
			reasons = append(reasons, fmt.Sprintf("%s p99=%s > %s", sp.Name, sp.Pct.P99, th.P99))
		}
	}
	if res.ErrorRate > th.ErrorRate {
		reasons = append(reasons, fmt.Sprintf("error rate %.3f%% > %.3f%%", res.ErrorRate*100, th.ErrorRate*100))
	}
	for _, p := range in.Pending {
		if d := p.Delta(); d > int64(th.PendingGrowth) {
			reasons = append(reasons, fmt.Sprintf("%s pending +%d > +%d", p.Durable, d, th.PendingGrowth))
		}
	}
	if len(reasons) > 0 {
		res.Kind = verdictTrip
		res.Reasons = reasons
		return res
	}

	// (3) Healthy-but-cannot-push -> INCONCLUSIVE.
	if th.RateTolerance > 0 && res.AchievedRPS < float64(in.TargetRPS)*(1-th.RateTolerance) {
		res.Kind = verdictInconclusive
		res.Reasons = []string{fmt.Sprintf(
			"achieved %.0f rps < %.0f%% of target %d rps (saturation=%d) — load box limited",
			res.AchievedRPS, (1-th.RateTolerance)*100, in.TargetRPS, in.Saturation)}
		return res
	}

	// (4) PASS.
	res.Kind = verdictPass
	return res
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd tools/loadgen && go test -run 'TestEvaluateRPSStep' . 2>&1 | tail -5`
Expected: PASS (`ok  github.com/hmchangw/chat/tools/loadgen`).

- [ ] **Step 5: Commit**

```bash
git add tools/loadgen/verdict.go tools/loadgen/verdict_test.go
git commit -m "feat(loadgen): add rps step verdict types and evaluateRPSStep"
```

---

## Task 2: Ramp engine — `parseRPSSteps`, `waitOrCancel`, `runRamp`

**Files:**
- Create: `tools/loadgen/ramp.go`
- Test: `tools/loadgen/ramp_test.go`

- [ ] **Step 1: Write the failing tests**

Create `tools/loadgen/ramp_test.go`:

```go
package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseRPSSteps(t *testing.T) {
	tests := []struct {
		in      string
		want    []int
		wantErr bool
	}{
		{in: "500,1000,2000", want: []int{500, 1000, 2000}},
		{in: "1k,2k,5k", want: []int{1000, 2000, 5000}},
		{in: " 500 , 1k ", want: []int{500, 1000}},
		{in: "1000", want: []int{1000}},
		{in: "", wantErr: true},
		{in: "abc", wantErr: true},
		{in: "1000,500", wantErr: true},  // not ascending
		{in: "0,1000", wantErr: true},    // not positive
		{in: "1000,1000", wantErr: true}, // not strictly ascending
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := parseRPSSteps(tt.in)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// fakeWorkload returns canned inputs, one per step, in order.
type fakeWorkload struct {
	inputs []rpsStepInputs
	calls  int
}

func (f *fakeWorkload) Label() string { return "fake" }
func (f *fakeWorkload) RunStep(_ context.Context, _ int, _, _ time.Duration) (rpsStepInputs, error) {
	in := f.inputs[f.calls]
	f.calls++
	return in, nil
}

func passInputs(target int) rpsStepInputs {
	return rpsStepInputs{TargetRPS: target, Hold: time.Second, AttemptedOps: target,
		Latencies: []seriesSamples{{Name: "E1", Samples: nLatencies(10, ms(20))}}}
}

func tripInputs(target int) rpsStepInputs {
	return rpsStepInputs{TargetRPS: target, Hold: time.Second, AttemptedOps: target,
		Latencies: []seriesSamples{{Name: "E1", Samples: nLatencies(10, ms(400))}}}
}

func inconclusiveInputs(target int) rpsStepInputs {
	return rpsStepInputs{TargetRPS: target, Hold: time.Second, AttemptedOps: target / 2,
		Latencies: []seriesSamples{{Name: "E1", Samples: nLatencies(10, ms(20))}}}
}

func TestRunRamp_StopsOnTrip(t *testing.T) {
	w := &fakeWorkload{inputs: []rpsStepInputs{passInputs(500), tripInputs(1000), passInputs(2000)}}
	cfg := rampConfig{Steps: []int{500, 1000, 2000}, Hold: time.Second,
		Thresholds: defaultRPSThresholds(), StopOnTrip: true}
	results := runRamp(context.Background(), w, cfg)
	require.Len(t, results, 2) // stopped after the trip at 1000
	assert.Equal(t, verdictPass, results[0].Kind)
	assert.Equal(t, verdictTrip, results[1].Kind)
	assert.Equal(t, 2, w.calls)
}

func TestRunRamp_DoesNotStopOnInconclusive(t *testing.T) {
	w := &fakeWorkload{inputs: []rpsStepInputs{passInputs(500), inconclusiveInputs(1000), passInputs(2000)}}
	cfg := rampConfig{Steps: []int{500, 1000, 2000}, Hold: time.Second,
		Thresholds: defaultRPSThresholds(), StopOnTrip: true}
	results := runRamp(context.Background(), w, cfg)
	require.Len(t, results, 3)
	assert.Equal(t, verdictInconclusive, results[1].Kind)
	assert.Equal(t, verdictPass, results[2].Kind)
}

func TestRunRamp_NoStopOnTripRunsAll(t *testing.T) {
	w := &fakeWorkload{inputs: []rpsStepInputs{passInputs(500), tripInputs(1000), tripInputs(2000)}}
	cfg := rampConfig{Steps: []int{500, 1000, 2000}, Hold: time.Second,
		Thresholds: defaultRPSThresholds(), StopOnTrip: false}
	results := runRamp(context.Background(), w, cfg)
	require.Len(t, results, 3)
}

func TestMaxRPSExitCode(t *testing.T) {
	pass := []rpsStepResult{{Kind: verdictPass}, {Kind: verdictTrip}}
	none := []rpsStepResult{{Kind: verdictInconclusive}, {Kind: verdictTrip}}
	assert.Equal(t, 0, maxRPSExitCode(pass))
	assert.Equal(t, 1, maxRPSExitCode(none))
	assert.Equal(t, 1, maxRPSExitCode(nil))
}

func TestWaitOrCancel(t *testing.T) {
	require.NoError(t, waitOrCancel(context.Background(), time.Millisecond))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.Error(t, waitOrCancel(ctx, time.Hour))
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd tools/loadgen && go test -run 'TestParseRPSSteps|TestRunRamp|TestMaxRPSExitCode|TestWaitOrCancel' . 2>&1 | head -20`
Expected: FAIL — `undefined: parseRPSSteps`, `undefined: runRamp`, etc.

- [ ] **Step 3: Write the implementation**

Create `tools/loadgen/ramp.go`:

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"
)

// rpsWorkload is the engine<->adapter seam. RunStep drives open-loop load at
// targetRPS, owning its own warmup/hold measurement boundaries, and returns the
// normalized inputs for the hold window. The engine owns cooldown, stop-on-trip
// and last-pass tracking.
type rpsWorkload interface {
	RunStep(ctx context.Context, targetRPS int, warmup, hold time.Duration) (rpsStepInputs, error)
	Label() string
}

// rampConfig parameterizes a ramp.
type rampConfig struct {
	Steps                    []int
	Warmup, Hold, Cooldown   time.Duration
	Thresholds               rpsThresholds
	StopOnTrip               bool
}

// parseRPSSteps parses a comma-separated, strictly-ascending list of positive
// RPS values. A trailing "k" multiplies by 1000 (e.g. "5k" -> 5000).
func parseRPSSteps(s string) ([]int, error) {
	parts := strings.Split(s, ",")
	out := make([]int, 0, len(parts))
	prev := 0
	for _, raw := range parts {
		tok := strings.TrimSpace(raw)
		if tok == "" {
			return nil, fmt.Errorf("empty step in %q", s)
		}
		mult := 1
		if strings.HasSuffix(tok, "k") || strings.HasSuffix(tok, "K") {
			mult = 1000
			tok = tok[:len(tok)-1]
		}
		n, err := strconv.Atoi(strings.TrimSpace(tok))
		if err != nil {
			return nil, fmt.Errorf("bad step %q: %w", raw, err)
		}
		n *= mult
		if n <= 0 {
			return nil, fmt.Errorf("step must be > 0, got %d", n)
		}
		if n <= prev {
			return nil, fmt.Errorf("steps must be strictly ascending, got %d after %d", n, prev)
		}
		prev = n
		out = append(out, n)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no steps parsed from %q", s)
	}
	return out, nil
}

// waitOrCancel sleeps for d or returns early with ctx.Err() if ctx is cancelled.
func waitOrCancel(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// runRamp executes each step in order. It stops early on the first TRIP when
// StopOnTrip is set (an INCONCLUSIVE step never stops the ramp), and on ctx
// cancellation, returning whatever results were gathered.
func runRamp(ctx context.Context, w rpsWorkload, cfg rampConfig) []rpsStepResult {
	var results []rpsStepResult
	for i, n := range cfg.Steps {
		if ctx.Err() != nil {
			break
		}
		in, err := w.RunStep(ctx, n, cfg.Warmup, cfg.Hold)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				break
			}
			slog.Warn("step run failed", "rps", n, "error", err)
			break
		}
		res := evaluateRPSStep(in, cfg.Thresholds)
		results = append(results, res)
		slog.Info("step complete", "rps", n, "verdict", res.Kind.String(),
			"achieved", res.AchievedRPS, "reasons", res.Reasons)
		if cfg.StopOnTrip && res.Kind == verdictTrip {
			break
		}
		if i < len(cfg.Steps)-1 {
			if err := waitOrCancel(ctx, cfg.Cooldown); err != nil {
				break
			}
		}
	}
	return results
}

// maxRPSExitCode returns 0 if any step PASSed, else 1.
func maxRPSExitCode(results []rpsStepResult) int {
	for i := range results {
		if results[i].Kind == verdictPass {
			return 0
		}
	}
	return 1
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd tools/loadgen && go test -run 'TestParseRPSSteps|TestRunRamp|TestMaxRPSExitCode|TestWaitOrCancel' . 2>&1 | tail -5`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tools/loadgen/ramp.go tools/loadgen/ramp_test.go
git commit -m "feat(loadgen): add rps ramp engine (parseRPSSteps, runRamp)"
```

---

## Task 3: Report — `renderRPSReport`, `writeRPSCSV`

**Files:**
- Create: `tools/loadgen/maxrps_report.go`
- Test: `tools/loadgen/maxrps_report_test.go`

- [ ] **Step 1: Write the failing tests**

Create `tools/loadgen/maxrps_report_test.go`:

```go
package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func sampleResults() []rpsStepResult {
	return []rpsStepResult{
		{TargetRPS: 500, AchievedRPS: 499, ErrorRate: 0, Kind: verdictPass,
			Latencies: []seriesPercentile{{Name: "E1", Pct: Percentiles{P95: ms(20), P99: ms(40)}}}},
		{TargetRPS: 1000, AchievedRPS: 998, ErrorRate: 0, Kind: verdictPass,
			Latencies: []seriesPercentile{{Name: "E1", Pct: Percentiles{P95: ms(60), P99: ms(90)}}}},
		{TargetRPS: 2000, AchievedRPS: 1900, ErrorRate: 0.02, Kind: verdictTrip,
			WorstDurable: "broadcast-worker", WorstDelta: 1500,
			Latencies: []seriesPercentile{{Name: "E1", Pct: Percentiles{P95: ms(160), P99: ms(300)}}},
			Reasons:   []string{"E1 p95=160ms > 100ms", "broadcast-worker pending +1500 > +1000"}},
	}
}

func TestRenderRPSReport_ReportsLastPass(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, renderRPSReport(&buf, sampleResults(), "messages", "medium"))
	out := buf.String()
	assert.Contains(t, out, "ANSWER: max RPS = 1000")
	assert.Contains(t, out, "workload=messages")
	assert.Contains(t, out, "preset=medium")
	assert.Contains(t, out, "Next limit:")
	assert.Contains(t, out, "broadcast-worker pending +1500 > +1000")
	assert.Contains(t, out, "E1 p95") // dynamic series column header
}

func TestRenderRPSReport_NoStepPassed(t *testing.T) {
	results := []rpsStepResult{{TargetRPS: 500, Kind: verdictTrip, Reasons: []string{"E1 p95=400ms > 100ms"}}}
	var buf bytes.Buffer
	require.NoError(t, renderRPSReport(&buf, results, "history", "history-medium"))
	assert.Contains(t, buf.String(), "ANSWER: no step passed")
}

func TestLastPassRPS(t *testing.T) {
	assert.Equal(t, 1000, lastPassRPS(sampleResults()))
	assert.Equal(t, 0, lastPassRPS([]rpsStepResult{{Kind: verdictTrip}}))
}

func TestWriteRPSCSV(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, writeRPSCSV(&buf, sampleResults()))
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	require.Len(t, lines, 4) // header + 3 rows
	assert.Contains(t, lines[0], "target_rps")
	assert.Contains(t, lines[0], "achieved_rps")
	assert.Contains(t, lines[0], "E1_p95_ms")
	assert.Contains(t, lines[0], "verdict")
	assert.Contains(t, lines[3], "2000")
	assert.Contains(t, lines[3], "TRIP")
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd tools/loadgen && go test -run 'TestRenderRPSReport|TestLastPassRPS|TestWriteRPSCSV' . 2>&1 | head -20`
Expected: FAIL — `undefined: renderRPSReport`, etc.

- [ ] **Step 3: Write the implementation**

Create `tools/loadgen/maxrps_report.go`:

```go
package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"
	"text/tabwriter"
)

// lastPassRPS returns the largest TargetRPS whose step PASSed, or 0 if none.
// Assumes results are in ascending step order.
func lastPassRPS(results []rpsStepResult) int {
	last := 0
	for i := range results {
		if results[i].Kind == verdictPass {
			last = results[i].TargetRPS
		}
	}
	return last
}

// firstTrip returns the first tripped step, or nil if none tripped.
func firstTrip(results []rpsStepResult) *rpsStepResult {
	for i := range results {
		if results[i].Kind == verdictTrip {
			return &results[i]
		}
	}
	return nil
}

// seriesNames returns the ordered union of latency-series names across results.
func seriesNames(results []rpsStepResult) []string {
	var names []string
	seen := map[string]bool{}
	for i := range results {
		for _, sp := range results[i].Latencies {
			if !seen[sp.Name] {
				seen[sp.Name] = true
				names = append(names, sp.Name)
			}
		}
	}
	return names
}

// pctFor returns the percentiles for a named series in a result (zero if absent).
func pctFor(r *rpsStepResult, name string) Percentiles {
	for _, sp := range r.Latencies {
		if sp.Name == name {
			return sp.Pct
		}
	}
	return Percentiles{}
}

// renderRPSReport writes the per-step table and the ANSWER line.
func renderRPSReport(w io.Writer, results []rpsStepResult, workload, preset string) error {
	fmt.Fprintf(w, "=== loadgen max-rps complete (workload=%s, preset=%s) ===\n\n", workload, preset)
	names := seriesNames(results)

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	header := []string{"target_rps", "achieved_rps"}
	for _, n := range names {
		header = append(header, n+" p95", n+" p99")
	}
	header = append(header, "err%", "worst_pending", "verdict")
	fmt.Fprintln(tw, strings.Join(header, "\t"))

	for i := range results {
		r := &results[i]
		row := []string{strconv.Itoa(r.TargetRPS), fmt.Sprintf("%.0f", r.AchievedRPS)}
		for _, n := range names {
			p := pctFor(r, n)
			row = append(row, p.P95.String(), p.P99.String())
		}
		pending := "-"
		if r.WorstDurable != "" {
			pending = fmt.Sprintf("%s +%d", r.WorstDurable, r.WorstDelta)
		}
		row = append(row, fmt.Sprintf("%.3f", r.ErrorRate*100), pending, r.Kind.String())
		fmt.Fprintln(tw, strings.Join(row, "\t"))
	}
	if err := tw.Flush(); err != nil {
		return fmt.Errorf("flush table: %w", err)
	}

	fmt.Fprintln(w)
	pass := lastPassRPS(results)
	if pass == 0 {
		fmt.Fprintf(w, "ANSWER: no step passed (workload=%s, preset=%s)\n", workload, preset)
		return nil
	}
	fmt.Fprintf(w, "ANSWER: max RPS = %d (workload=%s, preset=%s)\n", pass, workload, preset)
	if trip := firstTrip(results); trip != nil {
		fmt.Fprintf(w, "        Next limit: %s\n", strings.Join(trip.Reasons, "; "))
	}
	return nil
}

// writeRPSCSV writes one row per step. Series percentile columns are emitted in
// the union order of series names across all steps.
func writeRPSCSV(w io.Writer, results []rpsStepResult) error {
	cw := csv.NewWriter(w)
	names := seriesNames(results)

	header := []string{"target_rps", "achieved_rps"}
	for _, n := range names {
		header = append(header, n+"_p95_ms", n+"_p99_ms")
	}
	header = append(header, "error_rate", "attempted", "failed", "saturation", "worst_durable", "worst_pending_delta", "verdict", "reasons")
	if err := cw.Write(header); err != nil {
		return fmt.Errorf("write csv header: %w", err)
	}

	for i := range results {
		r := &results[i]
		row := []string{strconv.Itoa(r.TargetRPS), fmt.Sprintf("%.1f", r.AchievedRPS)}
		for _, n := range names {
			p := pctFor(r, n)
			row = append(row,
				strconv.FormatInt(p.P95.Milliseconds(), 10),
				strconv.FormatInt(p.P99.Milliseconds(), 10))
		}
		row = append(row,
			strconv.FormatFloat(r.ErrorRate, 'f', 6, 64),
			strconv.Itoa(r.AttemptedOps), strconv.Itoa(r.FailedOps), strconv.Itoa(r.Saturation),
			r.WorstDurable, strconv.FormatInt(r.WorstDelta, 10),
			r.Kind.String(), strings.Join(r.Reasons, "; "))
		if err := cw.Write(row); err != nil {
			return fmt.Errorf("write csv row: %w", err)
		}
	}
	cw.Flush()
	if err := cw.Error(); err != nil {
		return fmt.Errorf("flush csv: %w", err)
	}
	return nil
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd tools/loadgen && go test -run 'TestRenderRPSReport|TestLastPassRPS|TestWriteRPSCSV' . 2>&1 | tail -5`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tools/loadgen/maxrps_report.go tools/loadgen/maxrps_report_test.go
git commit -m "feat(loadgen): add max-rps report renderer and CSV writer"
```

---

## Task 4: Add `Collector.Reset()`

**Files:**
- Modify: `tools/loadgen/collector.go`
- Test: `tools/loadgen/collector_test.go` (add one test)

- [ ] **Step 1: Write the failing test**

Add to `tools/loadgen/collector_test.go`:

```go
func TestCollector_Reset(t *testing.T) {
	c := NewCollector(NewMetrics(), "test")
	now := time.Now()
	c.RecordPublish("req-1", "msg-1", now)
	c.RecordReply("req-1", now.Add(10*time.Millisecond))
	c.RecordBroadcast("msg-1", now.Add(20*time.Millisecond))
	require.Equal(t, 1, c.E1Count())
	require.Equal(t, 1, c.E2Count())

	c.Reset()

	assert.Equal(t, 0, c.E1Count())
	assert.Equal(t, 0, c.E2Count())
	mr, mb := c.Finalize()
	assert.Equal(t, 0, mr)
	assert.Equal(t, 0, mb)
	// After reset, a fresh publish+reply correlates normally.
	c.RecordPublish("req-2", "msg-2", now)
	c.RecordReply("req-2", now.Add(5*time.Millisecond))
	assert.Equal(t, 1, c.E1Count())
}
```

Ensure `collector_test.go` imports `time`, `testing`, and testify `assert`/`require` (add any missing imports).

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd tools/loadgen && go test -run 'TestCollector_Reset' . 2>&1 | head -20`
Expected: FAIL — `c.Reset undefined`.

- [ ] **Step 3: Write the implementation**

Add to `tools/loadgen/collector.go` (after `NewCollector`):

```go
// Reset clears all correlation state and accumulated samples. Used by the
// max-rps ramp to start each step's hold window from a clean slate while the
// E1/E2 subscriptions (which hold this *Collector pointer) stay alive.
func (c *Collector) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.byReqID = make(map[string]publishEntry)
	c.byMsgID = make(map[string]publishEntry)
	c.e1 = nil
	c.e2 = nil
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd tools/loadgen && go test -run 'TestCollector_Reset' . 2>&1 | tail -5`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tools/loadgen/collector.go tools/loadgen/collector_test.go
git commit -m "feat(loadgen): add Collector.Reset for per-step ramp windows"
```

---

## Task 5: Messages workload adapter

**Files:**
- Create: `tools/loadgen/maxrps_messages.go`
- Test: `tools/loadgen/maxrps_messages_test.go`

This adapter reuses `Generator`, `Collector`, `newE2Handler`, `newNatsCorePublisher`, `gatheredCounterValue`, `stream.MessagesCanonical`, and `subject.*Wildcard` (all already in `package main`). The pure-logic helpers (`buildMessagesInputs`, `diffCounters`) are unit-tested; the NATS-touching constructor and `RunStep` are covered by the integration test in Task 8.

> **Note:** `natsutil.Connect` returns `*otelnats.Conn`. The adapter does NOT store it on the struct — the constructor captures the connection (and the metrics `*http.Server`) in the cleanup closure, so no `otelnats` import is needed in the adapter.

- [ ] **Step 1: Write the failing tests**

Create `tools/loadgen/maxrps_messages_test.go`:

```go
package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDiffCounters(t *testing.T) {
	start := msgCounters{published: 100, err: map[string]float64{"publish": 1, "saturated": 5}}
	end := msgCounters{published: 1100, err: map[string]float64{"publish": 3, "saturated": 9}}
	d := diffCounters(start, end)
	assert.Equal(t, float64(1000), d.published)
	assert.Equal(t, float64(2), d.err["publish"])
	assert.Equal(t, float64(4), d.err["saturated"])
}

func TestBuildMessagesInputs(t *testing.T) {
	delta := msgCounters{
		published: 980,
		err:       map[string]float64{"publish": 10, "marshal": 0, "gatekeeper": 5, "bad_reply": 0, "saturated": 7},
	}
	e1 := nLatencies(50, ms(15))
	e2 := nLatencies(50, ms(30))
	pending := map[string]uint64{"message-worker": 12, "broadcast-worker": 40}
	startPending := map[string]uint64{"message-worker": 2, "broadcast-worker": 5}
	durables := []string{"message-worker", "broadcast-worker"}

	in := buildMessagesInputs(1000, 10*time.Second, delta, e1, e2, startPending, pending, durables, true)

	// attempted = published(980) + publish(10) + marshal(0)
	assert.Equal(t, 990, in.AttemptedOps)
	// failed = publish(10) + marshal(0) + gatekeeper(5) + bad_reply(0)
	assert.Equal(t, 15, in.FailedOps)
	assert.Equal(t, 7, in.Saturation)
	assert.Len(t, in.Latencies, 2)
	assert.Equal(t, "E1", in.Latencies[0].Name)
	assert.Equal(t, "E2", in.Latencies[1].Name)
	assert.Len(t, in.Pending, 2)
	assert.Equal(t, uint64(2), in.Pending[0].Start)
	assert.Equal(t, uint64(12), in.Pending[0].End)
	assert.False(t, in.Inconclusive)
}

func TestBuildMessagesInputs_PendingUnavailableIsInconclusive(t *testing.T) {
	delta := msgCounters{published: 1000, err: map[string]float64{}}
	in := buildMessagesInputs(1000, time.Second, delta, nil, nil, nil, nil, []string{"message-worker"}, false)
	assert.True(t, in.Inconclusive)
	assert.Contains(t, in.InconclusiveReason, "pending")
	assert.Empty(t, in.Pending)
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd tools/loadgen && go test -run 'TestDiffCounters|TestBuildMessagesInputs' . 2>&1 | head -20`
Expected: FAIL — `undefined: diffCounters`, `undefined: buildMessagesInputs`, `undefined: msgCounters`.

- [ ] **Step 3: Write the implementation**

Create `tools/loadgen/maxrps_messages.go`:

```go
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
		_ = nc.Drain()
		return nil, nil, fmt.Errorf("subscribe e1: %w", err)
	}
	e2Handler := newE2Handler(collector)
	e2Sub, err := nc.NatsConn().Subscribe(subject.RoomEventWildcard(), e2Handler)
	if err != nil {
		_ = e1Sub.Unsubscribe()
		_ = nc.Drain()
		return nil, nil, fmt.Errorf("subscribe e2: %w", err)
	}
	e2DMSub, err := nc.NatsConn().Subscribe(subject.UserRoomEventWildcard(), e2Handler)
	if err != nil {
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
	mfs, _ := w.metrics.Registry.Gather()
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
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd tools/loadgen && go test -run 'TestDiffCounters|TestBuildMessagesInputs' . 2>&1 | tail -5`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tools/loadgen/maxrps_messages.go tools/loadgen/maxrps_messages_test.go
git commit -m "feat(loadgen): add messages workload adapter for max-rps"
```

---

## Task 6: History workload adapter

**Files:**
- Create: `tools/loadgen/maxrps_history.go`
- Test: `tools/loadgen/maxrps_history_test.go`

The history adapter runs warmup and hold as two sequential generator runs, each with its own fresh `HistoryCollector`, so the hold collector holds only hold-window samples and error tallies (no time filtering needed). It reuses `NewHistoryGenerator`, `newNATSHistoryRequester`, and `BuildHistoryFixtures`.

- [ ] **Step 1: Write the failing tests**

Create `tools/loadgen/maxrps_history_test.go`:

```go
package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBuildHistoryInputs(t *testing.T) {
	c := NewHistoryCollector()
	now := time.Now()
	for i := 0; i < 40; i++ {
		c.RecordSample(HistorySample{Endpoint: HistoryEndpointHistory, Latency: ms(15), At: now})
	}
	for i := 0; i < 10; i++ {
		c.RecordSample(HistorySample{Endpoint: HistoryEndpointThread, Latency: ms(25), At: now})
	}
	c.RecordError(HistoryEndpointHistory, errClassTimeout, 0)
	c.RecordError(HistoryEndpointThread, errClassReply, 0)
	c.RecordSaturation()
	c.RecordSaturation()

	in := buildHistoryInputs(2000, 30*time.Second, c)

	// attempted = 40 + 10 history/thread samples + 2 errors (timeout+reply)
	assert.Equal(t, 52, in.AttemptedOps)
	assert.Equal(t, 2, in.FailedOps)
	assert.Equal(t, 2, in.Saturation)
	assert.Len(t, in.Latencies, 2)
	assert.Equal(t, "history", in.Latencies[0].Name)
	assert.Equal(t, "thread", in.Latencies[1].Name)
	assert.Len(t, in.Latencies[0].Samples, 40)
	assert.Len(t, in.Latencies[1].Samples, 10)
	assert.Empty(t, in.Pending) // history has no consumer queue
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd tools/loadgen && go test -run 'TestBuildHistoryInputs' . 2>&1 | head -20`
Expected: FAIL — `undefined: buildHistoryInputs`.

- [ ] **Step 3: Write the implementation**

Create `tools/loadgen/maxrps_history.go`:

```go
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
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd tools/loadgen && go test -run 'TestBuildHistoryInputs' . 2>&1 | tail -5`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tools/loadgen/maxrps_history.go tools/loadgen/maxrps_history_test.go
git commit -m "feat(loadgen): add history workload adapter for max-rps"
```

---

## Task 7: CLI wiring — `runMaxRPS` and `dispatch`

**Files:**
- Create: `tools/loadgen/maxrps.go`
- Modify: `tools/loadgen/main.go` (`dispatch`, `main.go:82-100`)
- Test: `tools/loadgen/maxrps_test.go`

- [ ] **Step 1: Write the failing tests**

Create `tools/loadgen/maxrps_test.go`:

```go
package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultSteps(t *testing.T) {
	msgs, err := parseRPSSteps(defaultSteps("messages"))
	require.NoError(t, err)
	assert.Equal(t, []int{500, 1000, 2000, 5000, 10000}, msgs)

	hist, err := parseRPSSteps(defaultSteps("history"))
	require.NoError(t, err)
	assert.Equal(t, []int{200, 500, 1000, 2000, 5000}, hist)
}

func TestBuildThresholds(t *testing.T) {
	th := buildThresholds(100*time.Millisecond, 250*time.Millisecond, 0.001, 1000, 0.05)
	assert.Equal(t, 100*time.Millisecond, th.P95)
	assert.Equal(t, 250*time.Millisecond, th.P99)
	assert.Equal(t, 0.001, th.ErrorRate)
	assert.Equal(t, uint64(1000), th.PendingGrowth)
	assert.Equal(t, 0.05, th.RateTolerance)
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd tools/loadgen && go test -run 'TestDefaultSteps|TestBuildThresholds' . 2>&1 | head -20`
Expected: FAIL — `undefined: defaultSteps`, `undefined: buildThresholds`.

- [ ] **Step 3: Write the implementation**

Create `tools/loadgen/maxrps.go`:

```go
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"
)

func defaultSteps(workload string) string {
	if workload == "history" {
		return "200,500,1000,2000,5000"
	}
	return "500,1000,2000,5000,10000"
}

func buildThresholds(p95, p99 time.Duration, errRate float64, pendingGrowth uint64, rateTol float64) rpsThresholds {
	return rpsThresholds{P95: p95, P99: p99, ErrorRate: errRate, PendingGrowth: pendingGrowth, RateTolerance: rateTol}
}

// runMaxRPS parses flags, builds the workload adapter, runs the ramp and prints
// the report. Returns the process exit code.
func runMaxRPS(ctx context.Context, cfg *config, args []string) int {
	fs := flag.NewFlagSet("max-rps", flag.ExitOnError)
	workload := fs.String("workload", "messages", "messages|history")
	preset := fs.String("preset", "", "preset name")
	seed := fs.Int64("seed", 42, "RNG seed")
	stepsFlag := fs.String("steps", "", "ascending RPS list, e.g. 500,1k,2k,5k,10k (default depends on workload)")
	warmup := fs.Duration("warmup", 10*time.Second, "per-step warmup (samples discarded)")
	hold := fs.Duration("hold", 30*time.Second, "per-step measurement window")
	cooldown := fs.Duration("cooldown", 5*time.Second, "per-step settle gap")
	sloP95 := fs.Duration("slo-p95", 100*time.Millisecond, "p95 latency SLO (all gated series)")
	sloP99 := fs.Duration("slo-p99", 250*time.Millisecond, "p99 latency SLO (all gated series)")
	sloErr := fs.Float64("slo-error-rate", 0.001, "max error rate (failed/attempted)")
	sloPending := fs.Uint64("slo-pending-growth", 1000, "max per-durable pending growth (messages only)")
	rateTol := fs.Float64("rate-tolerance", 0.05, "achieved-vs-target shortfall band for INCONCLUSIVE")
	stopOnTrip := fs.Bool("stop-on-trip", true, "stop the ramp at the first TRIP")
	inject := fs.String("inject", "frontdoor", "messages only: frontdoor|canonical")
	// history-only tunables (ignored for messages):
	mixFlag := fs.String("mix", "history:80,thread:20", "history only: endpoint mix")
	beforeModeFlag := fs.String("before-mode", "open:70,scrollback:30", "history only: before-cursor mix")
	scrollbackPages := fs.Int("scrollback-pages", 5, "history only: pages per scrollback chain")
	pageLimit := fs.Int("page-limit", 20, "history only: page limit")
	requestTimeout := fs.Duration("request-timeout", 5*time.Second, "history only: per-request timeout")
	csvPath := fs.String("csv", "", "optional CSV output path")
	_ = fs.Parse(args)

	if *preset == "" {
		fmt.Fprintln(os.Stderr, "--preset required")
		return 2
	}
	stepsStr := *stepsFlag
	if stepsStr == "" {
		stepsStr = defaultSteps(*workload)
	}
	steps, err := parseRPSSteps(stepsStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bad --steps: %v\n", err)
		return 2
	}
	thresholds := buildThresholds(*sloP95, *sloP99, *sloErr, *sloPending, *rateTol)

	var (
		w        rpsWorkload
		cleanup  func()
		presetID string
	)
	switch *workload {
	case "messages":
		p, ok := BuiltinPreset(*preset)
		if !ok {
			fmt.Fprintf(os.Stderr, "unknown preset: %s\n", *preset)
			return 2
		}
		injectMode, err := ParseInjectMode(*inject)
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			return 2
		}
		mw, clean, err := newMessagesWorkload(ctx, cfg, &p, injectMode, *seed)
		if err != nil {
			slog.Error("init messages workload", "error", err)
			return 1
		}
		w, cleanup, presetID = mw, clean, p.Name
	case "history":
		p, ok := BuiltinHistoryPreset(*preset)
		if !ok {
			fmt.Fprintf(os.Stderr, "unknown history preset: %s\n", *preset)
			return 2
		}
		mix, err := ParseEndpointMix(*mixFlag)
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			return 2
		}
		beforeMode, err := ParseBeforeMode(*beforeModeFlag)
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			return 2
		}
		if *scrollbackPages <= 0 {
			fmt.Fprintln(os.Stderr, "--scrollback-pages must be > 0")
			return 2
		}
		hw, clean, err := newHistoryWorkload(ctx, cfg, &p, *seed, historyWorkloadParams{
			Mix: mix, BeforeMode: beforeMode, ScrollbackPages: *scrollbackPages,
			PageLimit: *pageLimit, RequestTimeout: *requestTimeout,
		})
		if err != nil {
			slog.Error("init history workload", "error", err)
			return 1
		}
		w, cleanup, presetID = hw, clean, p.Name
	default:
		fmt.Fprintf(os.Stderr, "unknown workload: %s\n", *workload)
		return 2
	}
	defer cleanup()

	results := runRamp(ctx, w, rampConfig{
		Steps: steps, Warmup: *warmup, Hold: *hold, Cooldown: *cooldown,
		Thresholds: thresholds, StopOnTrip: *stopOnTrip,
	})

	if err := renderRPSReport(os.Stdout, results, w.Label(), presetID); err != nil {
		slog.Warn("render report", "error", err)
	}
	if *csvPath != "" {
		f, err := os.Create(*csvPath)
		if err != nil {
			slog.Error("create csv", "error", err)
		} else {
			if err := writeRPSCSV(f, results); err != nil {
				slog.Error("write csv", "error", err)
			}
			_ = f.Close()
		}
	}
	return maxRPSExitCode(results)
}
```

- [ ] **Step 4: Wire into `dispatch`**

In `tools/loadgen/main.go`, add a case to the `dispatch` switch (after the `history-sustained` case, before `default`):

```go
	case "max-rps":
		return runMaxRPS(ctx, cfg, os.Args[2:])
```

Also update the usage line in `main` (`main.go:59`) to:

```go
		fmt.Fprintln(os.Stderr, "usage: loadgen <seed|run|teardown|members-sustained|members-capacity|history-sustained|max-rps> [flags]")
```

- [ ] **Step 5: Run the tests + build to verify**

Run: `cd tools/loadgen && go test -run 'TestDefaultSteps|TestBuildThresholds' . 2>&1 | tail -5 && go build ./... 2>&1 | tail -5`
Expected: tests PASS; build clean (no output).

- [ ] **Step 6: Commit**

```bash
git add tools/loadgen/maxrps.go tools/loadgen/maxrps_test.go tools/loadgen/main.go
git commit -m "feat(loadgen): wire max-rps subcommand into dispatch"
```

---

## Task 8: Integration test — end-to-end 2-step ramp

**Files:**
- Modify: `tools/loadgen/integration_test.go`

Follow the existing `TestLoadgenSmallPreset_EndToEnd` in `integration_test.go` (build tag `//go:build integration`, `TestMain` via `testutil.RunTests` already present). That test creates the canonical stream, two ack-only durables (`message-worker`, `broadcast-worker`), a fake gatekeeper that forwards frontdoor sends to the canonical subject, and a fake broadcast-worker. The new test reuses the same scaffolding but drives the load through `newMessagesWorkload` + `runRamp` instead of a manual `Generator`.

Key facts (verified against the existing test):
- Connection in-test is `nats.Connect(testutil.NATS(t))` (the adapter opens its own connection internally via `natsutil.Connect`).
- The canonical stream + the two durables MUST exist before the ramp runs, because `messagesWorkload.snapshotPending` calls `js.Consumer(canonical, "message-worker"/"broadcast-worker").Info`.
- No Mongo seeding is needed: the fake gatekeeper does not validate against Mongo, and the generator picks subjects from the adapter's in-memory fixtures.
- The fake gatekeeper does NOT reply, so E1 stays empty; `AttemptedOps` comes from the `loadgen_published_total` counter delta, which is the assertion target.

- [ ] **Step 1: Re-read the existing test to confirm the scaffolding is unchanged**

Run: `cd tools/loadgen && sed -n '20,120p' integration_test.go`
Confirm the stream/durable/gatekeeper setup matches what is reused below.

- [ ] **Step 2: Write the integration test**

Add this function to `tools/loadgen/integration_test.go` (uses only `require`, which the file already imports — no new imports needed):

```go
func TestMaxRPS_Messages_TwoStepRamp(t *testing.T) {
	ctx := context.Background()
	siteID := "site-maxrps"

	nc, err := nats.Connect(testutil.NATS(t))
	require.NoError(t, err)
	defer nc.Drain()
	js, err := jetstream.New(nc)
	require.NoError(t, err)

	canonical := stream.MessagesCanonical(siteID)
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     canonical.Name,
		Subjects: canonical.Subjects,
	})
	require.NoError(t, err)

	// Ack-only durables so the canonical stream drains to zero (pending stays low).
	for _, durable := range []string{"message-worker", "broadcast-worker"} {
		cons, err := js.CreateOrUpdateConsumer(ctx, canonical.Name, jetstream.ConsumerConfig{
			Durable:   durable,
			AckPolicy: jetstream.AckExplicitPolicy,
		})
		require.NoError(t, err)
		cc, err := cons.Consume(func(msg jetstream.Msg) { _ = msg.Ack() })
		require.NoError(t, err)
		defer cc.Stop()
	}

	// Fake gatekeeper: frontdoor send -> canonical event.
	gkSub, err := nc.Subscribe(subject.MsgSendWildcard(siteID), func(m *nats.Msg) {
		var req model.SendMessageRequest
		if err := json.Unmarshal(m.Data, &req); err != nil {
			return
		}
		evt := model.MessageEvent{
			Message:   model.Message{ID: req.ID, Content: req.Content, CreatedAt: time.Now().UTC()},
			SiteID:    siteID,
			Timestamp: time.Now().UnixMilli(),
		}
		data, _ := json.Marshal(evt)
		_, _ = js.Publish(ctx, subject.MsgCanonicalCreated(siteID), data)
	})
	require.NoError(t, err)
	defer gkSub.Unsubscribe()

	cfg := &config{NatsURL: testutil.NATS(t), SiteID: siteID, MetricsAddr: ":0", MaxInFlight: 100}
	preset, _ := BuiltinPreset("small")

	w, cleanup, err := newMessagesWorkload(ctx, cfg, &preset, InjectFrontdoor, 42)
	require.NoError(t, err)
	defer cleanup()

	results := runRamp(ctx, w, rampConfig{
		Steps: []int{50, 100}, Warmup: time.Second, Hold: 2 * time.Second, Cooldown: 0,
		Thresholds: rpsThresholds{
			P95: time.Second, P99: 2 * time.Second, ErrorRate: 0.9,
			PendingGrowth: 1_000_000, RateTolerance: 0.9,
		},
		StopOnTrip: true,
	})

	require.Len(t, results, 2)
	for _, r := range results {
		require.NotEqual(t, verdictTrip, r.Kind, "reasons=%v", r.Reasons)
		require.Greater(t, r.AttemptedOps, 0)
		require.Greater(t, r.AchievedRPS, 0.0)
	}
}
```

- [ ] **Step 3: Run the integration test**

Run: `make test-integration SERVICE=tools/loadgen 2>&1 | tail -30`
Expected: PASS (Docker required). The whole-package integration build must compile; if the existing file already declares `siteID`/`canonical` at function scope elsewhere there is no conflict (each test function has its own scope).

- [ ] **Step 4: Commit**

```bash
git add tools/loadgen/integration_test.go
git commit -m "test(loadgen): integration coverage for max-rps messages ramp"
```

---

## Task 9: README and deploy Makefile target

**Files:**
- Modify: `tools/loadgen/README.md`
- Modify: `tools/loadgen/deploy/Makefile`

- [ ] **Step 1: Read both files**

Run: `cd tools/loadgen && sed -n '1,40p' README.md && echo '--- MAKEFILE ---' && sed -n '1,60p' deploy/Makefile`
Note the existing section style and how `run` / `run-history` (or `history-sustained`) targets pass env (`PRESET`, `RATE`, etc.).

- [ ] **Step 2: Add a README section**

Append a `## max-rps — auto-find Max RPS under SLO` section to `tools/loadgen/README.md` documenting:
- the subcommand and `--workload=messages|history`,
- the flag table (copy from the spec §2),
- how to read the `ANSWER: max RPS = N` line and INCONCLUSIVE rows,
- two quick-start command examples (one per workload), e.g.:

```bash
# messages: ramp 500..10k rps, stop at first SLO breach
loadgen max-rps --workload=messages --preset=medium --steps=500,1k,2k,5k,10k

# history: per-endpoint SLO, custom p95
loadgen max-rps --workload=history --preset=history-medium --steps=200,500,1k,2k --slo-p95=80ms
```

- [ ] **Step 3: Add a Makefile target**

Add to `tools/loadgen/deploy/Makefile` (matching the existing target style, with `WORKLOAD`/`PRESET`/`STEPS` overridable):

```makefile
WORKLOAD ?= messages
STEPS ?=

.PHONY: run-max-rps
run-max-rps: ## Ramp RPS to find the max under SLO (WORKLOAD=messages|history PRESET=.. STEPS=..)
	$(COMPOSE) run --rm loadgen max-rps --workload=$(WORKLOAD) --preset=$(PRESET) $(if $(STEPS),--steps=$(STEPS),)
```

> Match `$(COMPOSE)`, the service name (`loadgen`), and the `##` help convention to whatever the existing `run` target uses; adjust if the file differs.

- [ ] **Step 4: Verify the Makefile parses**

Run: `make -C tools/loadgen/deploy -n run-max-rps PRESET=medium 2>&1 | tail -5`
Expected: prints the docker compose command without executing it (no make syntax error).

- [ ] **Step 5: Commit**

```bash
git add tools/loadgen/README.md tools/loadgen/deploy/Makefile
git commit -m "docs(loadgen): document max-rps subcommand and add run-max-rps target"
```

---

## Task 10: Final verification

**Files:** none (verification only)

- [ ] **Step 1: Format**

Run: `make fmt 2>&1 | tail -5`
Expected: no diff complaints; reformats if needed.

- [ ] **Step 2: Lint**

Run: `make lint 2>&1 | tail -20`
Expected: 0 issues. Fix any reported (common: unused var, `gofmt`, error-wrap style per CLAUDE.md). If `unparam`/`revive` flags the `ctx context.Context` parameter on `newMessagesWorkload` / `newHistoryWorkload` as unused, drop the parameter and update both call sites in `maxrps.go` and the integration test, or `_ = ctx`.

- [ ] **Step 3: Unit tests with race detector**

Run: `make test SERVICE=tools/loadgen 2>&1 | tail -20`
Expected: PASS under `-race`.

- [ ] **Step 4: SAST**

Run: `make sast 2>&1 | tail -20`
Expected: no medium+ findings. (No `InsecureSkipVerify` or unsafe conversions introduced; the `int64(uint64)` in `consumerPendingDelta.Delta` is on small bounded values — if gosec flags G115, add `// #nosec G115 -- NumPending is a small bounded backlog count` directly above the conversion.)

- [ ] **Step 5: Integration tests**

Run: `make test-integration SERVICE=tools/loadgen 2>&1 | tail -20`
Expected: PASS (Docker required).

- [ ] **Step 6: Commit any fixes, then push**

```bash
git add -A
git commit -m "chore(loadgen): lint/sast fixes for max-rps" # only if there were fixes
git push -u origin claude/max-rps-slo-loadgen-ajKRi
```

---

## Notes for the implementer

- **No `docs/client-api.md` change** — this is tooling, not a client-facing handler.
- **Determinism:** the messages adapter reconstructs `Generator` per step with the same seed; this replays the same RNG sequence each step, which is fine (the workload shape per step is deterministic). The history adapter uses fresh collectors per warmup/hold run.
- **Error-rate definition (messages):** `FailedOps` counts hard publish/gatekeeper/marshal/bad_reply errors only. Missing replies/broadcasts are deliberately NOT counted as failures (late stragglers would create false trips); slow or dropped delivery is caught by the E2 latency and pending-growth signals instead. This is intentional — do not "fix" it by adding missing-reply counts to `FailedOps`.
- **INCONCLUSIVE precedence:** TRIP is checked before the rate-shortfall guard. Do not reorder — a server saturating backpressures the open-loop generator (achieved < target), and that must read as TRIP, not INCONCLUSIVE. See spec §5.
- **Convergence with PR #234:** all identifiers are `rps`-prefixed (`rpsThresholds`, `evaluateRPSStep`, `parseRPSSteps`, `renderRPSReport`, …) so there is no `package main` symbol collision with #234's `daily_*` code regardless of merge order.
```
