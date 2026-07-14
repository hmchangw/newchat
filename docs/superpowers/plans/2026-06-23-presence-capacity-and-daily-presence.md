# Presence Capacity Mode + Daily Presence Load Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `presence-capacity` loadgen subcommand that finds the maximum number of simultaneously-online users, and an opt-in `daily --presence` flag that adds observational presence load to the existing daily scenario.

**Architecture:** Both features reuse the existing presence primitives in `tools/loadgen` (`presence_user.go`, `presence_pool.go`, `presence_collector.go`). Part 1 is a standalone subcommand modelled on `presence-storm` (`presence_storm.go`): cumulative population ramp, connect-edge latency measured during activation, false-offline count measured during a steady-state hold. Part 2 hooks presence emission into the daily user lifecycle (`daily.go`), reporting presence latency/errors as observational stats that never affect the daily PASS/TRIP verdict.

**Tech Stack:** Go 1.25, `nats.go`, `go.uber.org/mock`+`testify` (unit tests), standard `testing`. Build/test via `make` targets (`make test SERVICE=loadgen` runs `tools/loadgen` unit tests; the loadgen module lives at `tools/loadgen`).

**Spec:** `docs/superpowers/specs/2026-06-23-presence-capacity-and-daily-presence-design.md`

---

## Conventions for every task

- Tests live in `package main` in `tools/loadgen/*_test.go` (same package — access unexported types).
- Run unit tests for a single test from the repo root with:
  `cd tools/loadgen && go test -race -run '<RegExp>' ./...`
  (The repo Makefile's `make test SERVICE=loadgen` runs the whole loadgen suite with `-race`; use the direct `go test -run` form for single-test TDD loops, then `make test SERVICE=loadgen` before each commit.)
- Commit only after `cd tools/loadgen && go build ./... && go test -race ./...` passes. A pre-commit hook runs lint + tests.
- Never edit `mock_store_test.go` (not relevant here — no store interfaces change).

---

# PHASE 1 — `presence-capacity` subcommand

Phase 1 is fully standalone (no dependency on Phase 2).

## File structure (Phase 1)

- Modify: `tools/loadgen/presence_collector.go` — add false-offline watcher.
- Modify: `tools/loadgen/presence_collector_test.go` — watcher tests.
- Create: `tools/loadgen/presence_capacity_verdict.go` — `capacityThresholds`, `capacityStepInputs`, `capacityStepResult`, `evaluateCapacityStep` (pure).
- Create: `tools/loadgen/presence_capacity_verdict_test.go`.
- Create: `tools/loadgen/presence_capacity.go` — config, env, runner, factory, prod entrypoint.
- Create: `tools/loadgen/presence_capacity_test.go`.
- Create: `tools/loadgen/presence_capacity_report.go` — console + CSV renderers.
- Create: `tools/loadgen/presence_capacity_report_test.go`.
- Modify: `tools/loadgen/main.go` — dispatch + usage string.

---

## Task 1: False-offline watcher on the collector

**Files:**
- Modify: `tools/loadgen/presence_collector.go`
- Test: `tools/loadgen/presence_collector_test.go`

- [ ] **Step 1: Write the failing test**

Append to `tools/loadgen/presence_collector_test.go`:

```go
func TestPresenceCollector_FalseOfflineWatcher(t *testing.T) {
	c := newPresenceCollector()
	now := time.Now()

	// Not watching yet: an offline is ignored.
	c.Observe("u-1", model.StatusOffline, now)
	assert.Equal(t, 0, c.FalseOfflines())

	// Arm a cohort of three accounts.
	c.WatchOnline([]string{"u-1", "u-2", "u-3"})

	// Offline for a watched account counts.
	c.Observe("u-1", model.StatusOffline, now)
	assert.Equal(t, 1, c.FalseOfflines())

	// Same account offline again is deduped.
	c.Observe("u-1", model.StatusOffline, now)
	assert.Equal(t, 1, c.FalseOfflines())

	// Offline for an account NOT in the cohort is ignored.
	c.Observe("u-99", model.StatusOffline, now)
	assert.Equal(t, 1, c.FalseOfflines())

	// A non-offline status for a watched account does not count.
	c.Observe("u-2", model.StatusOnline, now)
	assert.Equal(t, 1, c.FalseOfflines())

	// A second watched account going offline counts.
	c.Observe("u-3", model.StatusOffline, now)
	assert.Equal(t, 2, c.FalseOfflines())

	// StopWatchOnline freezes the count but preserves it for reading.
	c.StopWatchOnline()
	c.Observe("u-2", model.StatusOffline, now)
	assert.Equal(t, 2, c.FalseOfflines())
}

func TestPresenceCollector_ResetClearsWatcher(t *testing.T) {
	c := newPresenceCollector()
	c.WatchOnline([]string{"u-1"})
	c.Observe("u-1", model.StatusOffline, time.Now())
	assert.Equal(t, 1, c.FalseOfflines())
	c.Reset()
	assert.Equal(t, 0, c.FalseOfflines())
	// After reset, not watching: offline ignored again.
	c.Observe("u-1", model.StatusOffline, time.Now())
	assert.Equal(t, 0, c.FalseOfflines())
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd tools/loadgen && go test -race -run 'TestPresenceCollector_FalseOffline|TestPresenceCollector_ResetClearsWatcher' ./...`
Expected: FAIL — `c.WatchOnline undefined`, `c.FalseOfflines undefined`, `c.StopWatchOnline undefined`.

- [ ] **Step 3: Add the watcher fields and methods**

In `tools/loadgen/presence_collector.go`, extend the `presenceCollector` struct (add after the recovery-tracker fields, before the closing brace):

```go
	// false-offline watcher (capacity mode). When watching, an observed
	// offline for an account in watchCohort is recorded once in falseOfflines.
	watching      bool
	watchCohort   map[string]struct{}
	falseOfflines map[string]struct{}
```

In `Observe`, after the recovery-tracker block (inside the same locked method), add:

```go
	if c.watching && status == model.StatusOffline {
		if _, want := c.watchCohort[account]; want {
			if c.falseOfflines == nil {
				c.falseOfflines = make(map[string]struct{})
			}
			c.falseOfflines[account] = struct{}{}
		}
	}
```

In `Reset`, add these three lines inside the lock (alongside the existing field clears):

```go
	c.watching = false
	c.watchCohort = nil
	c.falseOfflines = nil
```

Add the three new methods at the end of the file:

```go
// WatchOnline arms a cohort of accounts expected to stay online. While armed,
// each distinct watched account observed going offline is one false offline.
func (c *presenceCollector) WatchOnline(accounts []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.watching = true
	c.watchCohort = make(map[string]struct{}, len(accounts))
	for _, a := range accounts {
		c.watchCohort[a] = struct{}{}
	}
	c.falseOfflines = make(map[string]struct{})
}

// StopWatchOnline disarms the watcher but preserves the count for reading.
func (c *presenceCollector) StopWatchOnline() {
	c.mu.Lock()
	c.watching = false
	c.mu.Unlock()
}

// FalseOfflines is the number of distinct watched accounts seen going offline.
func (c *presenceCollector) FalseOfflines() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.falseOfflines)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd tools/loadgen && go test -race -run 'TestPresenceCollector_FalseOffline|TestPresenceCollector_ResetClearsWatcher' ./...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd tools/loadgen && go build ./... && go test -race ./...
git add tools/loadgen/presence_collector.go tools/loadgen/presence_collector_test.go
git commit -m "feat(loadgen): add false-offline watcher to presence collector"
```

---

## Task 2: Capacity verdict (pure function)

**Files:**
- Create: `tools/loadgen/presence_capacity_verdict.go`
- Test: `tools/loadgen/presence_capacity_verdict_test.go`

- [ ] **Step 1: Write the failing test**

Create `tools/loadgen/presence_capacity_verdict_test.go`:

```go
package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func capThresholds() capacityThresholds {
	return capacityThresholds{
		ConnectP95Ms: 500, ConnectP99Ms: 1000,
		FalseOfflineRate: 0.001, ErrorRate: 0.01,
		PingTolerance: 0.10, GCPauseInconclusive: 50,
	}
}

func baseCapInputs() capacityStepInputs {
	return capacityStepInputs{
		N: 1000, EffectiveN: 1000,
		StartedAt: time.Now(), HoldDuration: time.Minute,
		ConnectLatencyMs: []float64{10, 20, 30, 40, 50},
		ConnectAttempted: 1000, ConnectFailed: 0,
		FalseOfflines: 0,
		PingsSent:     2000, PingsRequired: 2000,
		Self: SelfMetrics{GCPauseP99Ms: 5},
	}
}

func TestEvaluateCapacityStep_Pass(t *testing.T) {
	r := evaluateCapacityStep(baseCapInputs(), capThresholds())
	assert.Equal(t, verdictPass, r.Kind)
	assert.Empty(t, r.Reasons)
}

func TestEvaluateCapacityStep_GCInconclusive(t *testing.T) {
	in := baseCapInputs()
	in.Self.GCPauseP99Ms = 75
	r := evaluateCapacityStep(in, capThresholds())
	assert.Equal(t, verdictInconclusive, r.Kind)
	assert.Contains(t, r.Reasons[0], "gc pause")
}

func TestEvaluateCapacityStep_ZeroAttemptsInconclusive(t *testing.T) {
	in := baseCapInputs()
	in.ConnectAttempted = 0
	r := evaluateCapacityStep(in, capThresholds())
	assert.Equal(t, verdictInconclusive, r.Kind)
	assert.Contains(t, r.Reasons[0], "zero hellos")
}

func TestEvaluateCapacityStep_ActivationShortfallInconclusive(t *testing.T) {
	in := baseCapInputs()
	in.EffectiveN = 800 // 80% < 95%
	r := evaluateCapacityStep(in, capThresholds())
	assert.Equal(t, verdictInconclusive, r.Kind)
	assert.Contains(t, r.Reasons[0], "activated")
}

func TestEvaluateCapacityStep_PingShortfallInconclusive(t *testing.T) {
	in := baseCapInputs()
	in.PingsSent = 1500 // 75% < 90%
	r := evaluateCapacityStep(in, capThresholds())
	assert.Equal(t, verdictInconclusive, r.Kind)
	assert.Contains(t, r.Reasons[0], "ping sustainability")
}

func TestEvaluateCapacityStep_PingShortfallBeatsFalseOffline(t *testing.T) {
	// A load-box-induced ping shortfall must read INCONCLUSIVE, never TRIP,
	// even when false offlines also occurred.
	in := baseCapInputs()
	in.PingsSent = 1500     // shortfall
	in.FalseOfflines = 500  // would otherwise TRIP
	r := evaluateCapacityStep(in, capThresholds())
	assert.Equal(t, verdictInconclusive, r.Kind)
	assert.Contains(t, r.Reasons[0], "ping sustainability")
}

func TestEvaluateCapacityStep_FalseOfflineTrip(t *testing.T) {
	in := baseCapInputs()
	in.FalseOfflines = 5 // 5/1000 = 0.005 > 0.001
	r := evaluateCapacityStep(in, capThresholds())
	assert.Equal(t, verdictTrip, r.Kind)
	assert.Contains(t, r.Reasons[0], "false_offline_rate")
}

func TestEvaluateCapacityStep_ConnectLatencyTrip(t *testing.T) {
	in := baseCapInputs()
	in.ConnectLatencyMs = []float64{600, 700, 800, 900, 1200}
	r := evaluateCapacityStep(in, capThresholds())
	assert.Equal(t, verdictTrip, r.Kind)
	// Both p95 and p99 caps exceeded.
	assert.Len(t, r.Reasons, 2)
}

func TestEvaluateCapacityStep_ConnectErrorTrip(t *testing.T) {
	in := baseCapInputs()
	in.ConnectFailed = 50 // 50/1000 = 0.05 > 0.01
	r := evaluateCapacityStep(in, capThresholds())
	assert.Equal(t, verdictTrip, r.Kind)
	assert.Contains(t, r.Reasons[0], "connect error_rate")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd tools/loadgen && go test -race -run 'TestEvaluateCapacityStep' ./...`
Expected: FAIL — `capacityThresholds`, `capacityStepInputs`, `evaluateCapacityStep` undefined.

- [ ] **Step 3: Write the verdict implementation**

Create `tools/loadgen/presence_capacity_verdict.go`:

```go
package main

import (
	"fmt"
	"time"
)

// capacityThresholds are the presence-capacity SLO cutoffs.
type capacityThresholds struct {
	ConnectP95Ms        float64
	ConnectP99Ms        float64
	FalseOfflineRate    float64 // fraction of N swept offline (TRIP)
	ErrorRate           float64 // connect (hello) error-rate cap
	PingTolerance       float64 // ping-sustainability shortfall band (INCONCLUSIVE)
	GCPauseInconclusive float64 // loadgen self-saturation guard (ms)
}

func defaultCapacityThresholds() capacityThresholds {
	return capacityThresholds{
		ConnectP95Ms: 500, ConnectP99Ms: 1000,
		FalseOfflineRate: 0.001, ErrorRate: 0.01,
		PingTolerance: 0.10, GCPauseInconclusive: 50,
	}
}

// capacityStepInputs is everything evaluateCapacityStep needs for one N step.
type capacityStepInputs struct {
	N                int
	EffectiveN       int
	StartedAt        time.Time
	HoldDuration     time.Duration
	ConnectLatencyMs []float64 // hello -> online round-trips, captured at activation
	ConnectAttempted int64
	ConnectFailed    int64
	FalseOfflines    int
	PingsSent        int64
	PingsRequired    int64
	Self             SelfMetrics
}

// capacityStepResult is the graded verdict for one N step.
type capacityStepResult struct {
	N                int
	EffectiveN       int
	StartedAt        time.Time
	HoldDuration     time.Duration
	ConnectP50Ms     float64
	ConnectP95Ms     float64
	ConnectP99Ms     float64
	ConnectErrorRate float64
	FalseOfflines    int
	FalseOfflineRate float64
	PingSustain      float64 // PingsSent / PingsRequired
	Self             SelfMetrics
	Kind             verdictKind
	Reasons          []string
}

//nolint:gocritic // hugeParam: pure-function copy cost is negligible per step.
func evaluateCapacityStep(in capacityStepInputs, th capacityThresholds) capacityStepResult {
	r := capacityStepResult{
		N: in.N, EffectiveN: in.EffectiveN,
		StartedAt: in.StartedAt, HoldDuration: in.HoldDuration,
		FalseOfflines: in.FalseOfflines,
		ConnectP50Ms:  percentile(in.ConnectLatencyMs, 0.50),
		ConnectP95Ms:  percentile(in.ConnectLatencyMs, 0.95),
		ConnectP99Ms:  percentile(in.ConnectLatencyMs, 0.99),
	}
	if in.ConnectAttempted > 0 {
		r.ConnectErrorRate = float64(in.ConnectFailed) / float64(in.ConnectAttempted)
	}
	if in.N > 0 {
		r.FalseOfflineRate = float64(in.FalseOfflines) / float64(in.N)
	}
	if in.PingsRequired > 0 {
		r.PingSustain = float64(in.PingsSent) / float64(in.PingsRequired)
	}

	// INCONCLUSIVE overrides PASS/TRIP. Order matters: a loadgen-induced ping
	// shortfall must mask any false-offline TRIP (the sweep was our fault).
	if in.Self.GCPauseP99Ms > th.GCPauseInconclusive {
		r.Kind = verdictInconclusive
		r.Reasons = []string{fmt.Sprintf("inconclusive: loadgen gc pause p99=%.1fms > %.0f", in.Self.GCPauseP99Ms, th.GCPauseInconclusive)}
		return r
	}
	if in.ConnectAttempted == 0 {
		r.Kind = verdictInconclusive
		r.Reasons = []string{"inconclusive: zero hellos attempted (publisher down or emitters not wired)"}
		return r
	}
	if in.N > 0 && in.EffectiveN > 0 && float64(in.EffectiveN)/float64(in.N) < 0.95 {
		r.Kind = verdictInconclusive
		r.Reasons = []string{fmt.Sprintf("inconclusive: only %d/%d users activated", in.EffectiveN, in.N)}
		return r
	}
	if in.PingsRequired > 0 && r.PingSustain < 1.0-th.PingTolerance {
		r.Kind = verdictInconclusive
		r.Reasons = []string{fmt.Sprintf("inconclusive: ping sustainability %.1f%% < %.0f%% (loadgen could not sustain heartbeats)",
			r.PingSustain*100, (1.0-th.PingTolerance)*100)}
		return r
	}

	var reasons []string
	if r.FalseOfflineRate > th.FalseOfflineRate {
		reasons = append(reasons, fmt.Sprintf("false_offline_rate=%.4f > %.4f (%d users swept offline)",
			r.FalseOfflineRate, th.FalseOfflineRate, in.FalseOfflines))
	}
	if r.ConnectP95Ms > th.ConnectP95Ms {
		reasons = append(reasons, fmt.Sprintf("connect p95=%.0fms > %.0f", r.ConnectP95Ms, th.ConnectP95Ms))
	}
	if r.ConnectP99Ms > th.ConnectP99Ms {
		reasons = append(reasons, fmt.Sprintf("connect p99=%.0fms > %.0f", r.ConnectP99Ms, th.ConnectP99Ms))
	}
	if r.ConnectErrorRate > th.ErrorRate {
		reasons = append(reasons, fmt.Sprintf("connect error_rate=%.4f > %.4f", r.ConnectErrorRate, th.ErrorRate))
	}
	if len(reasons) > 0 {
		r.Kind = verdictTrip
		r.Reasons = reasons
		return r
	}
	r.Kind = verdictPass
	return r
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd tools/loadgen && go test -race -run 'TestEvaluateCapacityStep' ./...`
Expected: PASS (all 9 subtests).

- [ ] **Step 5: Commit**

```bash
cd tools/loadgen && go build ./... && go test -race ./...
git add tools/loadgen/presence_capacity_verdict.go tools/loadgen/presence_capacity_verdict_test.go
git commit -m "feat(loadgen): add presence-capacity verdict"
```

---

## Task 3: Capacity config parser

**Files:**
- Create: `tools/loadgen/presence_capacity.go` (config portion only this task)
- Test: `tools/loadgen/presence_capacity_test.go`

- [ ] **Step 1: Write the failing test**

Create `tools/loadgen/presence_capacity_test.go`:

```go
package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseCapacityConfig_Defaults(t *testing.T) {
	cfg, err := parseCapacityConfig(nil)
	require.NoError(t, err)
	assert.Equal(t, []int{10000, 20000, 50000, 100000, 200000}, cfg.Steps)
	assert.Equal(t, 30*time.Second, cfg.Warmup)
	assert.Equal(t, 120*time.Second, cfg.Hold)
	assert.Equal(t, 30*time.Second, cfg.Heartbeat)
	assert.InDelta(t, 0.001, cfg.FalseOfflineRate, 1e-9)
	assert.InDelta(t, 0.10, cfg.PingTolerance, 1e-9)
	assert.True(t, cfg.StopOnTrip)
}

func TestParseCapacityConfig_StepsShorthandAndOverrides(t *testing.T) {
	cfg, err := parseCapacityConfig([]string{"--steps=1k,2k", "--hold=10s", "--false-offline-rate=0.05"})
	require.NoError(t, err)
	assert.Equal(t, []int{1000, 2000}, cfg.Steps)
	assert.Equal(t, 10*time.Second, cfg.Hold)
	assert.InDelta(t, 0.05, cfg.FalseOfflineRate, 1e-9)
}

func TestParseCapacityConfig_BadSteps(t *testing.T) {
	_, err := parseCapacityConfig([]string{"--steps=abc"})
	require.Error(t, err)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd tools/loadgen && go test -race -run 'TestParseCapacityConfig' ./...`
Expected: FAIL — `parseCapacityConfig undefined`, `capacityConfig` undefined.

- [ ] **Step 3: Write the config + parser**

Create `tools/loadgen/presence_capacity.go` with the config and parser (the env/runner come in Task 4):

```go
package main

import (
	"flag"
	"fmt"
	"time"
)

type capacityConfig struct {
	Steps            []int
	Warmup           time.Duration
	Hold             time.Duration
	Cooldown         time.Duration
	Heartbeat        time.Duration
	ConnectP95Ms     float64
	ConnectP99Ms     float64
	FalseOfflineRate float64
	ErrorRate        float64
	PingTolerance    float64
	StopOnTrip       bool
	PublisherConns   int
	ObserverConns    int
	CSVPath          string
}

func parseCapacityConfig(args []string) (capacityConfig, error) {
	fs := flag.NewFlagSet("presence-capacity", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), `loadgen presence-capacity — find the max concurrent online population N

Cumulatively ramps a synthetic population through --steps. Each step activates
the delta of new users (each sends hello, measuring connect-edge latency),
holds with every user online and heartbeating, and counts false offlines
(users the service wrongly swept offline) plus ping sustainability. Reports the
largest N held without tripping.

Flags:
`)
		fs.PrintDefaults()
	}
	steps := fs.String("steps", "10000,20000,50000,100000,200000", "comma-separated cumulative N per step; `k` suffix x1000")
	warmup := fs.Duration("warmup", 30*time.Second, "post-activation settle before snapshot")
	hold := fs.Duration("hold", 120*time.Second, "steady-state false-offline window")
	cooldown := fs.Duration("cooldown", 15*time.Second, "per-step cooldown before next step")
	heartbeat := fs.Duration("heartbeat", 30*time.Second, "per-user ping interval (matches PRESENCE_HEARTBEAT_INTERVAL)")
	connectP95 := fs.Float64("connect-p95-ms", 500, "connect-edge p95 latency cap (ms)")
	connectP99 := fs.Float64("connect-p99-ms", 1000, "connect-edge p99 latency cap (ms)")
	falseOff := fs.Float64("false-offline-rate", 0.001, "false-offline fraction cap (TRIP)")
	errRate := fs.Float64("error-rate", 0.01, "connect error-rate cap (fraction)")
	pingTol := fs.Float64("ping-tolerance", 0.10, "ping-sustainability shortfall band (INCONCLUSIVE)")
	stop := fs.Bool("stop-on-trip", true, "stop the ramp on the first TRIP")
	pub := fs.Int("publisher-conns", 16, "shared publisher connection count")
	obs := fs.Int("observer-conns", 4, "observer connection count (subscribe presence.state.*)")
	csv := fs.String("csv", "", "optional CSV output path")
	if err := fs.Parse(args); err != nil {
		return capacityConfig{}, err
	}
	parsedSteps, err := parseStepList(*steps)
	if err != nil {
		return capacityConfig{}, err
	}
	return capacityConfig{
		Steps: parsedSteps, Warmup: *warmup, Hold: *hold, Cooldown: *cooldown,
		Heartbeat: *heartbeat, ConnectP95Ms: *connectP95, ConnectP99Ms: *connectP99,
		FalseOfflineRate: *falseOff, ErrorRate: *errRate, PingTolerance: *pingTol,
		StopOnTrip: *stop, PublisherConns: *pub, ObserverConns: *obs, CSVPath: *csv,
	}, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd tools/loadgen && go test -race -run 'TestParseCapacityConfig' ./...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd tools/loadgen && go build ./... && go test -race ./...
git add tools/loadgen/presence_capacity.go tools/loadgen/presence_capacity_test.go
git commit -m "feat(loadgen): add presence-capacity config parser"
```

---

## Task 4: Capacity env, step runner, and factory

**Files:**
- Modify: `tools/loadgen/presence_capacity.go`
- Test: `tools/loadgen/presence_capacity_test.go`

This task adds the env type, the step runner (`runStepCapacity`), the
activation/emitter helpers, the `capacityFactory` interface, the test-driver
loop (`runPresenceCapacityForTest`), and the prod factory. The unit test drives
`runStepCapacity` through the `onActivated`/`afterReset` seams without a broker.

- [ ] **Step 1: Write the failing test**

Append to `tools/loadgen/presence_capacity_test.go`:

```go
import (
	"context"
	// add to the existing import block: "context", "time" already present
)

// fakeCapacityEnv builds a capacityEnv with no real pool. onActivated marks
// users active (incrementing the collector's connect samples as if hellos
// resolved); afterReset injects the hold-window behaviour the test wants.
func TestRunStepCapacity_PassPath(t *testing.T) {
	c := newPresenceCollector()
	users := make([]*presenceUser, 100)
	for i := range users {
		users[i] = newPresenceUser(i, "site-test")
	}
	env := &capacityEnv{
		collector: c, users: users,
		thresholds: defaultCapacityThresholds(),
		warmup:     0, hold: 0, cooldown: 0, heartbeat: 30 * time.Second,
	}
	// Activation seam: simulate each hello resolving to online quickly.
	env.onActivated = func(e *capacityEnv, idx int) {
		sentAt := time.Now()
		e.collector.Expect(users[idx].account, "online", sentAt)
		e.collector.Observe(users[idx].account, "online", sentAt.Add(10*time.Millisecond))
	}
	// Hold seam: simulate the full ping quota being sent (no false offlines).
	env.afterReset = func(e *capacityEnv) {
		// hold==0 so PingsRequired==0; sustainability check is skipped.
	}

	r := runStepCapacity(context.Background(), env, 100, 0)
	assert.Equal(t, verdictPass, r.Kind)
	assert.Equal(t, 100, r.EffectiveN)
	assert.InDelta(t, 10, r.ConnectP50Ms, 5)
}

func TestRunStepCapacity_FalseOfflineTrips(t *testing.T) {
	c := newPresenceCollector()
	users := make([]*presenceUser, 100)
	for i := range users {
		users[i] = newPresenceUser(i, "site-test")
	}
	env := &capacityEnv{
		collector: c, users: users,
		thresholds: defaultCapacityThresholds(),
		warmup:     0, hold: 0, cooldown: 0, heartbeat: 30 * time.Second,
	}
	env.onActivated = func(e *capacityEnv, idx int) {
		sentAt := time.Now()
		e.collector.Expect(users[idx].account, "online", sentAt)
		e.collector.Observe(users[idx].account, "online", sentAt.Add(5*time.Millisecond))
	}
	// During the hold, the service falsely sweeps 10 users offline.
	env.afterReset = func(e *capacityEnv) {
		for i := 0; i < 10; i++ {
			e.collector.Observe(users[i].account, "offline", time.Now())
		}
	}
	r := runStepCapacity(context.Background(), env, 100, 0)
	assert.Equal(t, verdictTrip, r.Kind)
	assert.Equal(t, 10, r.FalseOfflines)
}
```

Note: the `import` block at the top of the test file must include `"context"`.
Merge it into the existing import group rather than adding a second block.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd tools/loadgen && go test -race -run 'TestRunStepCapacity' ./...`
Expected: FAIL — `capacityEnv`, `runStepCapacity` undefined.

- [ ] **Step 3: Add env, runner, factory to `presence_capacity.go`**

Append to `tools/loadgen/presence_capacity.go` (add the new imports
`"context"`, `"errors"`, `"log/slog"`, `"os"`, `"sync/atomic"` to the import
block):

```go
// capacityEnv bundles a capacity run's deps. onActivated / afterReset are test
// seams (nil in prod -> real wiring used).
type capacityEnv struct {
	pool       *presencePool
	collector  *presenceCollector
	users      []*presenceUser
	thresholds capacityThresholds
	siteID     string
	warmup     time.Duration
	hold       time.Duration
	cooldown   time.Duration
	heartbeat  time.Duration

	onActivated func(env *capacityEnv, idx int)
	afterReset  func(env *capacityEnv)

	holdDurationNanos atomic.Int64
	activated         atomic.Int64
	pingsSent         atomic.Int64
}

func (env *capacityEnv) holding() bool { return env.holdDurationNanos.Load() > 0 }

type capacityFactory interface {
	Build(cfg capacityConfig) *capacityEnv
}

// runStepCapacity activates the delta of new users (measuring connect-edge
// latency), warms up, snapshots connect stats, then holds while watching for
// false offlines, and grades the step.
func runStepCapacity(ctx context.Context, env *capacityEnv, n, prevN int) capacityStepResult {
	activateCapacityUsers(ctx, env, prevN, n)
	_ = waitOrCancel(ctx, env.warmup)

	// Connect-edge stats: the hello->online round-trips captured during
	// activation+warmup. ReapMissing counts hellos that never went online.
	connectLat := env.collector.LatenciesMs()
	env.collector.ReapMissing()
	connectAttempted := env.collector.Attempted()
	connectFailed := env.collector.Failed()

	startedAt := time.Now()
	env.collector.Reset()
	cohort := make([]string, 0, n)
	for i := 0; i < n && i < len(env.users); i++ {
		cohort = append(cohort, env.users[i].account)
	}
	env.collector.WatchOnline(cohort)
	env.pingsSent.Store(0)
	env.holdDurationNanos.Store(env.hold.Nanoseconds())
	if env.afterReset != nil {
		env.afterReset(env)
	}
	_ = waitOrCancel(ctx, env.hold)
	env.collector.StopWatchOnline()
	env.holdDurationNanos.Store(0)

	var pingsRequired int64
	if env.heartbeat > 0 {
		pingsRequired = int64(n) * int64(env.hold/env.heartbeat)
	}

	in := capacityStepInputs{
		N: n, EffectiveN: int(env.activated.Load()),
		StartedAt: startedAt, HoldDuration: env.hold,
		ConnectLatencyMs: connectLat,
		ConnectAttempted: connectAttempted,
		ConnectFailed:    connectFailed,
		FalseOfflines:    env.collector.FalseOfflines(),
		PingsSent:        env.pingsSent.Load(),
		PingsRequired:    pingsRequired,
		Self:             snapshotSelfMetrics(),
	}
	r := evaluateCapacityStep(in, env.thresholds)
	_ = waitOrCancel(ctx, env.cooldown)
	return r
}

// activateCapacityUsers brings users [from,to) online (hello) at a bounded rate
// and starts their steady-ping goroutine.
func activateCapacityUsers(ctx context.Context, env *capacityEnv, from, to int) {
	if to > len(env.users) {
		to = len(env.users)
	}
	tokens := time.NewTicker(time.Second / 1000) // 1000 activations/sec
	defer tokens.Stop()
	for i := from; i < to; i++ {
		select {
		case <-ctx.Done():
			return
		case <-tokens.C:
		}
		if env.onActivated != nil {
			env.onActivated(env, i) // test seam
		} else {
			startCapacityEmitter(ctx, env, env.users[i])
		}
		env.activated.Add(1)
	}
}

// startCapacityEmitter sends one user's hello (measured connect edge) and runs
// its steady-ping loop. No churn: no activity flips, no bye.
func startCapacityEmitter(ctx context.Context, env *capacityEnv, u *presenceUser) {
	emitCapacityHello(env, u)
	go func() {
		tick := time.NewTicker(env.heartbeat)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
			tr := u.ping(nowMillis())
			if env.pool != nil {
				_ = env.pool.Publish(tr.subject, tr.payload)
			}
			if env.holding() {
				env.pingsSent.Add(1)
			}
		}
	}()
}

// emitCapacityHello publishes a hello and registers its online expectation.
func emitCapacityHello(env *capacityEnv, u *presenceUser) {
	tr := u.hello(nowMillis())
	if env.pool == nil {
		return
	}
	sentAt := time.Now()
	if err := env.pool.Publish(tr.subject, tr.payload); err != nil {
		env.collector.RecordEmit()
		env.collector.RecordEmitFailure()
		return
	}
	env.collector.Expect(u.account, tr.expect, sentAt)
}

//nolint:gocritic // cfg passed by value to satisfy capacityFactory interface
func runPresenceCapacityForTest(ctx context.Context, cfg capacityConfig, f capacityFactory) ([]capacityStepResult, error) {
	if len(cfg.Steps) == 0 {
		return nil, fmt.Errorf("cfg.Steps cannot be empty")
	}
	env := f.Build(cfg)
	if env.pool != nil {
		defer env.pool.Close()
	}
	prevN := 0
	var results []capacityStepResult
	for _, n := range cfg.Steps {
		if err := ctx.Err(); err != nil {
			slog.Info("presence-capacity interrupted", "completed_steps", len(results))
			break
		}
		r := runStepCapacity(ctx, env, n, prevN)
		results = append(results, r)
		if cfg.StopOnTrip && r.Kind == verdictTrip {
			break
		}
		prevN = n
	}
	return results, nil
}

// prodCapacityFactory wires the real pool.
type prodCapacityFactory struct{ baseCfg *config }

//nolint:gocritic // cfg by value to match interface
func (f *prodCapacityFactory) Build(cfg capacityConfig) *capacityEnv {
	c := newPresenceCollector()
	siteID := f.baseCfg.SiteID
	if siteID == "" {
		siteID = "site-local"
	}
	pool, err := newPresencePool(f.baseCfg.NatsURL, f.baseCfg.NatsCredsFile, cfg.PublisherConns, cfg.ObserverConns, c)
	if err != nil {
		slog.Error("presence pool init failed; emitters will no-op", "err", err)
	}
	users := make([]*presenceUser, slicesMaxInt(cfg.Steps))
	for i := range users {
		users[i] = newPresenceUser(i, siteID)
	}
	return &capacityEnv{
		pool: pool, collector: c, users: users,
		thresholds: capacityThresholds{
			ConnectP95Ms: cfg.ConnectP95Ms, ConnectP99Ms: cfg.ConnectP99Ms,
			FalseOfflineRate: cfg.FalseOfflineRate, ErrorRate: cfg.ErrorRate,
			PingTolerance: cfg.PingTolerance, GCPauseInconclusive: 50,
		},
		siteID: siteID,
		warmup: cfg.Warmup, hold: cfg.Hold, cooldown: cfg.Cooldown, heartbeat: cfg.Heartbeat,
	}
}

// runPresenceCapacity is the production entrypoint invoked by main.go.
func runPresenceCapacity(ctx context.Context, baseCfg *config, args []string) int {
	cfg, err := parseCapacityConfig(args)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		slog.Error("parse presence-capacity config", "error", err)
		return 2
	}
	results, err := runPresenceCapacityForTest(ctx, cfg, &prodCapacityFactory{baseCfg: baseCfg})
	if err != nil {
		slog.Error("presence-capacity run", "error", err)
		return 1
	}
	renderCapacityConsole(os.Stdout, results)
	if cfg.CSVPath != "" {
		if err := writeCapacityCSV(cfg.CSVPath, results); err != nil {
			slog.Error("write capacity csv", "error", err)
			return 1
		}
	}
	return presenceCapacityExitCode(results)
}

// presenceCapacityExitCode returns 0 if any step passed, else 1.
func presenceCapacityExitCode(results []capacityStepResult) int {
	for i := range results {
		if results[i].Kind == verdictPass {
			return 0
		}
	}
	return 1
}
```

Note: `renderCapacityConsole` and `writeCapacityCSV` are defined in Task 5;
until then the package won't compile. That's expected — Step 4 of THIS task
only runs the targeted test after Task 5 lands. To keep this task
self-compiling, temporarily declare stubs at the bottom of `presence_capacity.go`:

```go
// Temporary stubs — replaced by real implementations in Task 5.
func renderCapacityConsole(_ io.Writer, _ []capacityStepResult) {}
func writeCapacityCSV(_ string, _ []capacityStepResult) error   { return nil }
```

Add `"io"` to the imports for the stub. (Task 5 Step 3 deletes these stubs and
the `"io"` import moves to the report file.)

- [ ] **Step 4: Run test to verify it passes**

Run: `cd tools/loadgen && go test -race -run 'TestRunStepCapacity' ./...`
Expected: PASS (both subtests).

- [ ] **Step 5: Commit**

```bash
cd tools/loadgen && go build ./... && go test -race ./...
git add tools/loadgen/presence_capacity.go tools/loadgen/presence_capacity_test.go
git commit -m "feat(loadgen): add presence-capacity env, step runner, and factory"
```

---

## Task 5: Capacity console + CSV renderers

**Files:**
- Create: `tools/loadgen/presence_capacity_report.go`
- Modify: `tools/loadgen/presence_capacity.go` (remove the temporary stubs + `"io"` import)
- Test: `tools/loadgen/presence_capacity_report_test.go`

- [ ] **Step 1: Write the failing test**

Create `tools/loadgen/presence_capacity_report_test.go`:

```go
package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRenderCapacityConsole_AnswerLine(t *testing.T) {
	results := []capacityStepResult{
		{N: 10000, EffectiveN: 10000, ConnectP50Ms: 8, ConnectP95Ms: 40, ConnectP99Ms: 80, PingSustain: 1.0, Kind: verdictPass},
		{N: 20000, EffectiveN: 20000, ConnectP50Ms: 9, ConnectP95Ms: 60, ConnectP99Ms: 120, PingSustain: 1.0, Kind: verdictPass},
		{N: 50000, EffectiveN: 50000, FalseOfflines: 900, FalseOfflineRate: 0.018, PingSustain: 1.0,
			Kind: verdictTrip, Reasons: []string{"false_offline_rate=0.0180 > 0.0010 (900 users swept offline)"}},
	}
	var buf bytes.Buffer
	renderCapacityConsole(&buf, results)
	out := buf.String()
	assert.Contains(t, out, "MAX CONCURRENT ONLINE: 20000")
	assert.Contains(t, out, "Next limit:")
	assert.Contains(t, out, "false_offline_rate")
}

func TestRenderCapacityConsole_NoPass(t *testing.T) {
	results := []capacityStepResult{
		{N: 10000, EffectiveN: 5000, Kind: verdictInconclusive, Reasons: []string{"inconclusive: only 5000/10000 users activated"}},
	}
	var buf bytes.Buffer
	renderCapacityConsole(&buf, results)
	assert.Contains(t, buf.String(), "MAX CONCURRENT ONLINE: none")
}

func TestWriteCapacityCSV_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cap.csv")
	results := []capacityStepResult{
		{N: 20000, EffectiveN: 20000, StartedAt: time.Now(), ConnectP95Ms: 60, FalseOfflines: 0, PingSustain: 1.0, Kind: verdictPass},
		{N: 10000, EffectiveN: 10000, StartedAt: time.Now(), ConnectP95Ms: 40, FalseOfflines: 0, PingSustain: 1.0, Kind: verdictPass},
	}
	require.NoError(t, writeCapacityCSV(path, results))
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	out := string(b)
	assert.Contains(t, out, "n,effective_n,")
	// Rows sorted ascending by N: 10000 before 20000.
	idx10 := bytes.Index(b, []byte("10000,10000"))
	idx20 := bytes.Index(b, []byte("20000,20000"))
	assert.True(t, idx10 >= 0 && idx20 >= 0 && idx10 < idx20)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd tools/loadgen && go test -race -run 'TestRenderCapacityConsole|TestWriteCapacityCSV' ./...`
Expected: FAIL — duplicate `renderCapacityConsole`/`writeCapacityCSV` declarations (the stubs from Task 4) OR assertion failures. Resolve by replacing the stubs (Step 3).

- [ ] **Step 3: Remove the stubs and write the real renderers**

In `tools/loadgen/presence_capacity.go`, delete the two temporary stub
functions and remove `"io"` from its import block.

Create `tools/loadgen/presence_capacity_report.go`:

```go
package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
)

func renderCapacityConsole(w io.Writer, results []capacityStepResult) {
	fmt.Fprintln(w, "N        cP50   cP95   cP99   false_off  ping%   verdict")
	var lastPass int
	for i := range results {
		r := &results[i]
		nLabel := strconv.Itoa(r.N)
		if r.EffectiveN > 0 && r.EffectiveN != r.N {
			nLabel = fmt.Sprintf("%d(%d)", r.N, r.EffectiveN)
		}
		if r.Kind == verdictPass {
			lastPass = r.N
		}
		fmt.Fprintf(w, "%-8s %-6.0f %-6.0f %-6.0f %-10d %-7.1f %s\n",
			nLabel, r.ConnectP50Ms, r.ConnectP95Ms, r.ConnectP99Ms,
			r.FalseOfflines, r.PingSustain*100, r.Kind)
		if r.Kind != verdictPass && len(r.Reasons) > 0 {
			fmt.Fprintf(w, "    reasons: %s\n", joinReasons(r.Reasons))
		}
	}
	fmt.Fprintln(w)
	if lastPass > 0 {
		fmt.Fprintf(w, "MAX CONCURRENT ONLINE: %d (last passing step)\n", lastPass)
		for i := range results {
			if results[i].Kind == verdictTrip {
				fmt.Fprintf(w, "        Next limit: %s\n", joinReasons(results[i].Reasons))
				break
			}
		}
	} else {
		fmt.Fprintln(w, "MAX CONCURRENT ONLINE: none (no step passed)")
	}
}

func writeCapacityCSV(path string, results []capacityStepResult) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create csv: %w", err)
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	header := []string{
		"n", "effective_n", "started_at",
		"connect_p50_ms", "connect_p95_ms", "connect_p99_ms", "connect_error_rate",
		"false_offlines", "false_offline_rate", "ping_sustain", "verdict", "reasons",
	}
	if err := w.Write(header); err != nil {
		return fmt.Errorf("write csv header: %w", err)
	}
	rs := make([]capacityStepResult, len(results))
	copy(rs, results)
	sort.Slice(rs, func(i, j int) bool { return rs[i].N < rs[j].N })
	for i := range rs {
		r := &rs[i]
		row := []string{
			strconv.Itoa(r.N), strconv.Itoa(r.EffectiveN),
			r.StartedAt.UTC().Format("2006-01-02T15:04:05Z"),
			fmt.Sprintf("%.0f", r.ConnectP50Ms), fmt.Sprintf("%.0f", r.ConnectP95Ms),
			fmt.Sprintf("%.0f", r.ConnectP99Ms), fmt.Sprintf("%.6f", r.ConnectErrorRate),
			strconv.Itoa(r.FalseOfflines), fmt.Sprintf("%.6f", r.FalseOfflineRate),
			fmt.Sprintf("%.4f", r.PingSustain), r.Kind.String(), joinReasons(r.Reasons),
		}
		if err := w.Write(row); err != nil {
			return fmt.Errorf("write csv row: %w", err)
		}
	}
	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd tools/loadgen && go test -race -run 'TestRenderCapacityConsole|TestWriteCapacityCSV' ./...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd tools/loadgen && go build ./... && go test -race ./...
git add tools/loadgen/presence_capacity_report.go tools/loadgen/presence_capacity_report_test.go tools/loadgen/presence_capacity.go
git commit -m "feat(loadgen): add presence-capacity console and CSV renderers"
```

---

## Task 6: Wire `presence-capacity` into main dispatch

**Files:**
- Modify: `tools/loadgen/main.go:76` (usage string) and `tools/loadgen/main.go:99-127` (dispatch switch)
- Test: `tools/loadgen/main_test.go`

- [ ] **Step 1: Write the failing test**

Append to `tools/loadgen/main_test.go`:

```go
func TestParseCapacityConfig_HelpReturnsErrHelp(t *testing.T) {
	_, err := parseCapacityConfig([]string{"-h"})
	require.ErrorIs(t, err, flag.ErrHelp)
}
```

(Confirms the subcommand parser is reachable and follows the `-h` convention.
The dispatch wiring itself is exercised by `go build` + manual run; this test
locks the help contract. Ensure `"flag"` and `require` are imported in
`main_test.go` — add them to the existing import block if missing.)

- [ ] **Step 2: Run test to verify it fails (or builds)**

Run: `cd tools/loadgen && go test -race -run 'TestParseCapacityConfig_HelpReturnsErrHelp' ./...`
Expected: PASS already if Task 3 landed (parser exists). If `flag` import is
missing in the test file it FAILS to compile — add the import.

- [ ] **Step 3: Add dispatch case + usage**

In `tools/loadgen/main.go`, update the usage string at line ~76 to include
`presence-capacity`:

```go
		fmt.Fprintln(os.Stderr, "usage: loadgen <seed|run|teardown|members-sustained|members-capacity|history-sustained|max-rps|daily|max-room-size|presence-sustained|presence-storm|presence-capacity> [flags]")
```

In the `dispatch` switch, add a case after the `presence-storm` case:

```go
	case "presence-capacity":
		return runPresenceCapacity(ctx, cfg, os.Args[2:])
```

- [ ] **Step 4: Verify build + dispatch**

Run: `cd tools/loadgen && go build ./... && go vet ./...`
Expected: clean build.
Run: `cd tools/loadgen && go test -race ./...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tools/loadgen/main.go tools/loadgen/main_test.go
git commit -m "feat(loadgen): wire presence-capacity subcommand into dispatch"
```

---

## Task 7: Phase 1 docs — README mention

**Files:**
- Modify: `tools/loadgen/README.md`

- [ ] **Step 1: Find the presence subcommand section**

Run: `cd tools/loadgen && grep -n "presence-storm\|presence-sustained" README.md`
Expected: locate the presence subcommand documentation block.

- [ ] **Step 2: Add a `presence-capacity` subsection**

Insert after the `presence-storm` documentation (match the surrounding heading
style — mirror how `presence-storm` is documented). Content:

```markdown
### `presence-capacity` — max concurrent online users

Cumulatively ramps a synthetic population through `--steps`. Each step activates
the delta of new users (each `hello`, measuring connect-edge latency), then
holds with every user online and heartbeating, counting **false offlines**
(users the service wrongly swept offline) and **ping sustainability**. Reports
the largest N held without tripping.

- Connect-edge latency (`hello`→`online`) is measured during activation; the
  steady-state hold has no transitions to time.
- False offlines are the ceiling signal; a loadgen-induced ping shortfall reads
  INCONCLUSIVE, never TRIP.

```
loadgen presence-capacity --steps=10k,20k,50k,100k,200k --hold=120s --csv=cap.csv
```
```

- [ ] **Step 3: Commit**

```bash
git add tools/loadgen/README.md
git commit -m "docs(loadgen): document presence-capacity subcommand"
```

---

# PHASE 2 — `daily --presence`

Phase 2 depends only on the pre-existing presence primitives, not on Phase 1.

## File structure (Phase 2)

- Modify: `tools/loadgen/presence_user.go` — add `newPresenceUserForAccount`; refactor `newPresenceUser` to delegate.
- Modify: `tools/loadgen/presence_user_test.go` — constructor test.
- Modify: `tools/loadgen/daily_user.go` — add `presence *presenceUser` field to `userState`.
- Modify: `tools/loadgen/daily.go` — config flags, `stepEnv` fields, `emitPresence` helper, activation hello, emitter ping+flip hooks, `prodEnvFactory` wiring, presence reset in `runStep`, `closePools` drain.
- Modify: `tools/loadgen/daily_verdict.go` — `PresenceObsStats` type + `Presence` field on `StepResult`; `runStep` populates it.
- Modify: `tools/loadgen/daily_report.go` — presence console line + CSV columns.
- Test: `tools/loadgen/daily_test.go`, `tools/loadgen/daily_verdict_test.go`, `tools/loadgen/daily_report_test.go`.

---

## Task 8: `newPresenceUserForAccount` constructor

**Files:**
- Modify: `tools/loadgen/presence_user.go:37-46`
- Test: `tools/loadgen/presence_user_test.go`

- [ ] **Step 1: Write the failing test**

Append to `tools/loadgen/presence_user_test.go`:

```go
func TestNewPresenceUserForAccount(t *testing.T) {
	u := newPresenceUserForAccount("user-42", "site-x")
	assert.Equal(t, "user-42", u.account)
	assert.Equal(t, "c-user-42", u.connID)
	assert.Equal(t, "site-x", u.siteID)
	assert.Equal(t, model.StatusOffline, u.status)
	assert.Equal(t, -1, u.idx)
}

func TestNewPresenceUser_DelegatesAndKeepsIndex(t *testing.T) {
	u := newPresenceUser(7, "site-y")
	assert.Equal(t, presenceAccount(7), u.account)
	assert.Equal(t, "c-"+presenceAccount(7), u.connID)
	assert.Equal(t, 7, u.idx)
	assert.Equal(t, model.StatusOffline, u.status)
}
```

(Ensure `model` and `assert` are imported in `presence_user_test.go`.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cd tools/loadgen && go test -race -run 'TestNewPresenceUserForAccount|TestNewPresenceUser_Delegates' ./...`
Expected: FAIL — `newPresenceUserForAccount undefined`.

- [ ] **Step 3: Add the constructor and refactor `newPresenceUser`**

In `tools/loadgen/presence_user.go`, replace the existing `newPresenceUser`
(lines 37-46) with:

```go
// newPresenceUserForAccount builds a presenceUser bound to an explicit account
// (connID = "c-"+account). idx is -1 because there is no synthetic index; this
// is used by daily, whose users carry real fixture accounts. The daily emitter
// never reads idx, so the sentinel is safe.
func newPresenceUserForAccount(account, siteID string) *presenceUser {
	return &presenceUser{
		idx:     -1,
		account: account,
		connID:  "c-" + account,
		siteID:  siteID,
		status:  model.StatusOffline,
	}
}

func newPresenceUser(idx int, siteID string) *presenceUser {
	u := newPresenceUserForAccount(presenceAccount(idx), siteID)
	u.idx = idx
	return u
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd tools/loadgen && go test -race -run 'TestNewPresenceUserForAccount|TestNewPresenceUser' ./...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd tools/loadgen && go build ./... && go test -race ./...
git add tools/loadgen/presence_user.go tools/loadgen/presence_user_test.go
git commit -m "feat(loadgen): add account-bound presence user constructor"
```

---

## Task 9: Daily config flags for presence

**Files:**
- Modify: `tools/loadgen/daily.go:28-44` (struct), `:98-146` (parse)
- Test: `tools/loadgen/daily_test.go`

- [ ] **Step 1: Write the failing test**

Append to `tools/loadgen/daily_test.go`:

```go
func TestParseDailyConfig_PresenceDefaultsOff(t *testing.T) {
	cfg, err := parseDailyConfig([]string{"--preset=daily-heavy"})
	require.NoError(t, err)
	assert.False(t, cfg.Presence)
	assert.Equal(t, 30*time.Second, cfg.PresenceHeartbeat)
	assert.Equal(t, 8, cfg.PresencePublisherConns)
	assert.Equal(t, 2, cfg.PresenceObserverConns)
}

func TestParseDailyConfig_PresenceEnabled(t *testing.T) {
	cfg, err := parseDailyConfig([]string{"--preset=daily-heavy", "--presence", "--presence-heartbeat=15s"})
	require.NoError(t, err)
	assert.True(t, cfg.Presence)
	assert.Equal(t, 15*time.Second, cfg.PresenceHeartbeat)
}
```

(Ensure `require` and `time` are imported in `daily_test.go` — they are used by
existing tests, so likely already present.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cd tools/loadgen && go test -race -run 'TestParseDailyConfig_Presence' ./...`
Expected: FAIL — `cfg.Presence undefined`.

- [ ] **Step 3: Add fields and flags**

In `tools/loadgen/daily.go`, add to the `dailyConfig` struct (after `ActionP99Ms`):

```go
	// Presence load (opt-in). When Presence is false the daily run is
	// unchanged — no presence pool is built and no presence is emitted.
	Presence               bool
	PresenceHeartbeat      time.Duration
	PresencePublisherConns int
	PresenceObserverConns  int
```

In `parseDailyConfig`, after the `actionP99` flag declaration (line ~110), add:

```go
	presence := fs.Bool("presence", false, "emit presence load (hello/ping/activity) per daily user; observational stats only, never affects the verdict")
	presenceHeartbeat := fs.Duration("presence-heartbeat", 30*time.Second, "per-user presence ping interval (only with --presence)")
	presencePub := fs.Int("presence-publisher-conns", 8, "presence publisher connection count (only with --presence)")
	presenceObs := fs.Int("presence-observer-conns", 2, "presence observer connection count (only with --presence)")
```

In the returned `dailyConfig{...}` literal, add the four fields:

```go
		Presence:               *presence,
		PresenceHeartbeat:      *presenceHeartbeat,
		PresencePublisherConns: *presencePub,
		PresenceObserverConns:  *presenceObs,
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd tools/loadgen && go test -race -run 'TestParseDailyConfig' ./...`
Expected: PASS (new + existing daily-config tests).

- [ ] **Step 5: Commit**

```bash
cd tools/loadgen && go build ./... && go test -race ./...
git add tools/loadgen/daily.go tools/loadgen/daily_test.go
git commit -m "feat(loadgen): add daily --presence config flags"
```

---

## Task 10: `PresenceObsStats` type + `StepResult.Presence` field

**Files:**
- Modify: `tools/loadgen/daily_verdict.go:126-145` (StepResult) and add the new type
- Test: `tools/loadgen/daily_verdict_test.go`

- [ ] **Step 1: Write the failing test**

Append to `tools/loadgen/daily_verdict_test.go`:

```go
func TestEvaluateStep_IgnoresPresence(t *testing.T) {
	// evaluateStep must never read Presence; a healthy step stays PASS
	// regardless of presence stats, which are populated outside the verdict.
	in := stepInputs{
		N: 100, EffectiveN: 100,
		HoldDuration:   time.Minute,
		LatencySamples: []float64{10, 20, 30},
		AttemptedOps:   1000, FailedOps: 0,
		Self: SelfMetrics{GCPauseP99Ms: 5},
	}
	r := evaluateStep(in, defaultThresholds())
	assert.False(t, r.Tripped)
	assert.False(t, r.Inconclusive)
	assert.Nil(t, r.Presence) // evaluateStep does not set it
}

func TestPresenceObsStats_Shape(t *testing.T) {
	s := PresenceObsStats{P50Ms: 5, P95Ms: 40, P99Ms: 90, Attempted: 100, Failed: 2}
	if s.Attempted > 0 {
		s.ErrorRate = float64(s.Failed) / float64(s.Attempted)
	}
	assert.InDelta(t, 0.02, s.ErrorRate, 1e-9)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd tools/loadgen && go test -race -run 'TestEvaluateStep_IgnoresPresence|TestPresenceObsStats_Shape' ./...`
Expected: FAIL — `PresenceObsStats undefined`, `r.Presence undefined`.

- [ ] **Step 3: Add the type and field**

In `tools/loadgen/daily_verdict.go`, add the type near `ActionLatencyStats`:

```go
// PresenceObsStats is the observational presence summary for one daily step.
// It NEVER affects the verdict — evaluateStep does not read it. Latency is a
// single combined figure over connect (hello->online) and activity
// (setAway->away/online) transitions; pings are no-ops and contribute only to
// attempted/error accounting.
type PresenceObsStats struct {
	P50Ms     float64
	P95Ms     float64
	P99Ms     float64
	Attempted int64
	Failed    int64
	ErrorRate float64
}
```

Add the field to `StepResult` (after `ActionLatencies`):

```go
	// Presence is non-nil only when daily ran with --presence. Observational.
	Presence *PresenceObsStats
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd tools/loadgen && go test -race -run 'TestEvaluateStep_IgnoresPresence|TestPresenceObsStats_Shape' ./...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd tools/loadgen && go build ./... && go test -race ./...
git add tools/loadgen/daily_verdict.go tools/loadgen/daily_verdict_test.go
git commit -m "feat(loadgen): add observational presence stats to daily StepResult"
```

---

## Task 11: `userState.presence` field + `emitPresence` helper + flip emission

**Files:**
- Modify: `tools/loadgen/daily_user.go:111-134` (struct) — add field
- Modify: `tools/loadgen/daily.go` — add `stepEnv` presence fields + `emitPresence`
- Test: `tools/loadgen/daily_test.go`

- [ ] **Step 1: Write the failing test**

Append to `tools/loadgen/daily_test.go`:

```go
func TestEmitPresence_NoPoolIsNoop(t *testing.T) {
	// With no presence pool, emitPresence must be a safe no-op.
	env := &stepEnv{} // presencePool nil
	u := newPresenceUserForAccount("user-1", "site-test")
	emitPresence(env, u, u.hello(nowMillis())) // must not panic
}

func TestEmitPresence_RecordsExpectation(t *testing.T) {
	c := newPresenceCollector()
	env := &stepEnv{presenceCollector: c}
	// A non-nil pool is required for emitPresence to record; use a pool with
	// zero publisher conns so Publish returns an error (counts as failure).
	env.presencePool = &presencePool{collector: c}
	u := newPresenceUserForAccount("user-1", "site-test")
	emitPresence(env, u, u.hello(nowMillis()))
	// Publish failed (no conns) -> attempted + failed both incremented.
	assert.Equal(t, int64(1), c.Attempted())
	assert.Equal(t, int64(1), c.Failed())
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd tools/loadgen && go test -race -run 'TestEmitPresence' ./...`
Expected: FAIL — `stepEnv` has no `presencePool`/`presenceCollector`,
`emitPresence undefined`.

- [ ] **Step 3: Add the field, env fields, and helper**

In `tools/loadgen/daily_user.go`, add to the `userState` struct (after
`idleProb`):

```go
	// presence is non-nil only when daily runs with --presence. It carries the
	// per-user presence state machine (hello/ping/activity).
	presence *presenceUser
```

In `tools/loadgen/daily.go`, add to the `stepEnv` struct (after `mintJWT`):

```go
	// Presence load (nil when --presence is off). presencePool owns its own
	// publisher + observer conns, independent of the message pools.
	presencePool      *presencePool
	presenceCollector *presenceCollector
	presenceHeartbeat time.Duration
```

Add the helper near `doAction` in `daily.go`:

```go
// emitPresence publishes one presence transition for u and records its
// attempt/expectation/failure on the presence collector. No-op when presence
// is disabled (nil pool) or u has no presence state.
func emitPresence(env *stepEnv, u *presenceUser, tr presenceTransition) {
	if env.presencePool == nil || u == nil {
		return
	}
	sentAt := time.Now()
	err := env.presencePool.Publish(tr.subject, tr.payload)
	if tr.expect == "" { // no-op transition (steady ping) — don't measure
		return
	}
	if err != nil {
		env.presenceCollector.RecordEmit()
		env.presenceCollector.RecordEmitFailure()
		return
	}
	env.presenceCollector.Expect(u.account, tr.expect, sentAt)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd tools/loadgen && go test -race -run 'TestEmitPresence' ./...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd tools/loadgen && go build ./... && go test -race ./...
git add tools/loadgen/daily_user.go tools/loadgen/daily.go tools/loadgen/daily_test.go
git commit -m "feat(loadgen): add presence state to daily user + emitPresence helper"
```

---

## Task 12: Emit presence in activation + emitter (hello, ping, flips)

**Files:**
- Modify: `tools/loadgen/daily.go` — `activateUsers` (hello), `startEmitter` (ping ticker + flip activity)
- Test: `tools/loadgen/daily_test.go`

This task wires the actual emission points. Because `startEmitter` launches a
goroutine that's awkward to assert on directly, the test drives the
flip-emission logic through a small extracted helper `presenceFlip`, and asserts
`activateUsers` emits a hello via a stub env with a recording collector.

- [ ] **Step 1: Write the failing test**

Append to `tools/loadgen/daily_test.go`:

```go
func TestPresenceFlip_EmitsActivityOnChange(t *testing.T) {
	c := newPresenceCollector()
	env := &stepEnv{presenceCollector: c, presencePool: &presencePool{collector: c}}
	u := &userState{Account: "user-1", presence: newPresenceUserForAccount("user-1", "site-test")}
	// Bring presence online first (hello), so activity transitions can measure.
	emitPresence(env, u.presence, u.presence.hello(nowMillis()))
	c.Reset()

	// active=true now; flipping to idle (active=false) emits setAway(true)->away.
	u.active = true
	presenceFlip(env, u, /*wasActive=*/ true) // active unchanged -> no emit
	assert.Equal(t, int64(0), c.Attempted())

	u.active = false
	presenceFlip(env, u, /*wasActive=*/ true) // changed true->false -> setAway(true)
	assert.Equal(t, int64(1), c.Attempted())
}

func TestActivateUsers_EmitsPresenceHello(t *testing.T) {
	c := newPresenceCollector()
	users := []*userState{
		{ID: "u0", Account: "user-0", presence: newPresenceUserForAccount("user-0", "site-test")},
	}
	env := &stepEnv{
		users:             users,
		presenceCollector: c,
		presencePool:      &presencePool{collector: c}, // 0 conns -> publish fails, still records
		// publish nil -> message emitter no-op; we only assert presence here.
	}
	activateUsers(context.Background(), env, 0, 1)
	// Hello recorded an attempt (publish failed: attempted+failed).
	assert.Equal(t, int64(1), c.Attempted())
}
```

(Ensure `"context"` is imported in `daily_test.go`.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cd tools/loadgen && go test -race -run 'TestPresenceFlip|TestActivateUsers_EmitsPresenceHello' ./...`
Expected: FAIL — `presenceFlip undefined`; `activateUsers` doesn't emit presence.

- [ ] **Step 3: Add `presenceFlip`, hook activation and emitter**

In `tools/loadgen/daily.go`, add the flip helper near `emitPresence`:

```go
// presenceFlip emits an activity transition when the user's active state
// changed this tick. active->idle => away; idle->active => not away. No-op when
// presence is disabled or the state didn't change.
func presenceFlip(env *stepEnv, u *userState, wasActive bool) {
	if env.presencePool == nil || u.presence == nil || u.active == wasActive {
		return
	}
	emitPresence(env, u.presence, u.presence.setAway(!u.active, nowMillis()))
}
```

In `activateUsers`, inside the loop after the user is successfully pool-added
and the message emitter is started (after the `if poolAdded && env.publish != nil { startEmitter(...) }`
block, before `env.activatedCount.Add(1)`), add the presence hello:

```go
		if poolAdded && env.presencePool != nil && u.presence != nil {
			emitPresence(env, u.presence, u.presence.hello(nowMillis()))
		}
```

In `startEmitter`, add a presence ping ticker and flip emission. Replace the
goroutine body's ticker setup and loop head so it reads:

```go
	go func() {
		seed := int64(uint64(env.runSeed)*0x9E3779B97F4A7C15) + int64(userIdx)
		r := rand.New(rand.NewSource(seed))
		weights := defaultActionWeights()
		baseRate := actionRatePerSecond(weights.totalPerDay(), 8*time.Hour)

		tick := time.NewTicker(1 * time.Second)
		defer tick.Stop()

		// Optional presence ping ticker (own interval, independent of the 1s
		// Markov tick). Only armed when --presence is on.
		var presenceC <-chan time.Time
		if env.presencePool != nil && u.presence != nil && env.presenceHeartbeat > 0 {
			pt := time.NewTicker(env.presenceHeartbeat)
			defer pt.Stop()
			presenceC = pt.C
		}

		for {
			select {
			case <-ctx.Done():
				return
			case <-presenceC:
				emitPresence(env, u.presence, u.presence.ping(nowMillis()))
				continue
			case <-tick.C:
			}
			wasActive := u.active
			u.step(r)
			presenceFlip(env, u, wasActive)
			if !u.active {
				continue
			}
			holdStart, holdDuration := env.currentHold()
			if holdDuration <= 0 {
				continue // env not yet initialised; wait for runStep to set
			}
			compress := (8 * time.Hour).Seconds() / holdDuration.Seconds()
			elapsed := time.Since(holdStart)
			rate := baseRate * compress * rateMultiplier(elapsed, holdDuration)
			if r.Float64() < rate {
				doAction(ctx, env, u, r, weights)
			}
		}
	}()
```

(This replaces the existing goroutine body — keep the surrounding `startEmitter`
function signature and doc comment unchanged.)

- [ ] **Step 4: Run test to verify it passes**

Run: `cd tools/loadgen && go test -race -run 'TestPresenceFlip|TestActivateUsers_EmitsPresenceHello' ./...`
Expected: PASS.
Run: `cd tools/loadgen && go test -race -run 'TestStartEmitter|TestActivateUsers|TestRunStep' ./...`
Expected: PASS (existing daily emitter/activation tests still green — presence
paths are nil-guarded when disabled).

- [ ] **Step 5: Commit**

```bash
cd tools/loadgen && go build ./... && go test -race ./...
git add tools/loadgen/daily.go tools/loadgen/daily_test.go
git commit -m "feat(loadgen): emit presence hello/ping/activity in daily emitter"
```

---

## Task 13: Reset + snapshot presence stats in `runStep`; drain pool in `closePools`

**Files:**
- Modify: `tools/loadgen/daily.go` — `runStep` (reset at hold start, snapshot at end), `closePools` (drain)
- Test: `tools/loadgen/daily_test.go`

- [ ] **Step 1: Write the failing test**

Append to `tools/loadgen/daily_test.go`:

```go
func TestSnapshotPresenceStats_NilWhenDisabled(t *testing.T) {
	env := &stepEnv{} // no presence collector
	r := StepResult{}
	snapshotPresenceStats(env, &r)
	assert.Nil(t, r.Presence)
}

func TestSnapshotPresenceStats_PopulatesFromCollector(t *testing.T) {
	c := newPresenceCollector()
	now := time.Now()
	// Two resolved transitions -> two latency samples.
	c.Expect("user-1", "online", now)
	c.Observe("user-1", "online", now.Add(10*time.Millisecond))
	c.Expect("user-2", "away", now)
	c.Observe("user-2", "away", now.Add(20*time.Millisecond))
	env := &stepEnv{presenceCollector: c}
	r := StepResult{}
	snapshotPresenceStats(env, &r)
	require.NotNil(t, r.Presence)
	assert.Equal(t, int64(2), r.Presence.Attempted)
	assert.Equal(t, int64(0), r.Presence.Failed)
	assert.InDelta(t, 20, r.Presence.P99Ms, 5)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd tools/loadgen && go test -race -run 'TestSnapshotPresenceStats' ./...`
Expected: FAIL — `snapshotPresenceStats undefined`.

- [ ] **Step 3: Add `snapshotPresenceStats`, wire into `runStep`, drain in `closePools`**

In `tools/loadgen/daily.go`, add the helper near `emitPresence`:

```go
// snapshotPresenceStats fills r.Presence from the presence collector (after
// counting unresolved expectations as failures). No-op when presence is off.
func snapshotPresenceStats(env *stepEnv, r *StepResult) {
	if env.presenceCollector == nil {
		return
	}
	env.presenceCollector.ReapMissing()
	attempted := env.presenceCollector.Attempted()
	failed := env.presenceCollector.Failed()
	lat := env.presenceCollector.LatenciesMs()
	s := &PresenceObsStats{
		P50Ms: percentile(lat, 0.50),
		P95Ms: percentile(lat, 0.95),
		P99Ms: percentile(lat, 0.99),
		Attempted: attempted, Failed: failed,
	}
	if attempted > 0 {
		s.ErrorRate = float64(failed) / float64(attempted)
	}
	r.Presence = s
}
```

In `runStep`, find the existing `env.collector.Reset()` call (daily.go:321) and
add a presence reset immediately after it so presence stats cover the hold
window:

```go
	env.collector.Reset()
	if env.presenceCollector != nil {
		env.presenceCollector.Reset()
	}
```

In `runStep`, after `r := evaluateStep(in, env.thresholds)` (daily.go:363) and
before the cooldown wait, add:

```go
	snapshotPresenceStats(env, &r)
```

In `closePools` (daily.go:639), add a presence-pool drain:

```go
func closePools(env *stepEnv) {
	if env.direct != nil {
		env.direct.Close()
	}
	if env.multiplex != nil {
		env.multiplex.Close()
	}
	if env.presencePool != nil {
		env.presencePool.Close()
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd tools/loadgen && go test -race -run 'TestSnapshotPresenceStats' ./...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd tools/loadgen && go build ./... && go test -race ./...
git add tools/loadgen/daily.go tools/loadgen/daily_test.go
git commit -m "feat(loadgen): snapshot daily presence stats per step and drain pool"
```

---

## Task 14: Build presence pool in `prodEnvFactory.Build`

**Files:**
- Modify: `tools/loadgen/daily.go:661-757` (`prodEnvFactory.Build`)
- Test: `tools/loadgen/daily_test.go` (compile + nil-safety via existing factory paths)

`prodEnvFactory.Build` requires a live NATS URL to dial, so unit tests can't
fully exercise the dialing path. The test asserts the wiring is gated on
`cfg.Presence` by checking that the disabled path leaves `userState.presence`
nil and `env.presencePool` nil (no dial attempted).

- [ ] **Step 1: Write the failing test**

Append to `tools/loadgen/daily_test.go`:

```go
func TestProdEnvFactory_PresenceDisabledLeavesNil(t *testing.T) {
	f := &prodEnvFactory{baseCfg: &config{NatsURL: "nats://127.0.0.1:14222", SiteID: "site-test"}}
	users := []*userState{{ID: "u0", Account: "user-0"}}
	cfg := dailyConfig{Preset: "daily-heavy", MultiplexPoolSize: 0} // Presence false
	env := f.Build(cfg, users)
	assert.Nil(t, env.presencePool)
	assert.Nil(t, env.presenceCollector)
	assert.Nil(t, users[0].presence)
}
```

(This relies on `Build` not dialing presence when `cfg.Presence` is false. The
message-pool dials may log warnings on connect failure — that's fine; the test
only asserts the presence fields stay nil. Existing daily tests already build
the prod factory with an unreachable URL, so this pattern is established.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cd tools/loadgen && go test -race -run 'TestProdEnvFactory_PresenceDisabledLeavesNil' ./...`
Expected: FAIL — `env.presencePool`/`env.presenceCollector` fields exist but are
never set; the assertion on `users[0].presence` may pass, but the test compiles
only once Step 3 references are consistent. (If it already passes because the
fields default to nil, proceed — Step 3 adds the *enabled* path.)

- [ ] **Step 3: Add the presence wiring to `Build`**

In `tools/loadgen/daily.go`, inside `prodEnvFactory.Build`, after `siteID` is
finalised and before the `return &stepEnv{...}` literal, add:

```go
	var presencePool *presencePool
	var presenceCollector *presenceCollector
	if cfg.Presence {
		presenceCollector = newPresenceCollector()
		pp, err := newPresencePool(f.baseCfg.NatsURL, f.baseCfg.NatsCredsFile,
			cfg.PresencePublisherConns, cfg.PresenceObserverConns, presenceCollector)
		if err != nil {
			slog.Error("presence pool init failed; presence emission disabled", "err", err)
			presencePool = nil
		} else {
			presencePool = pp
		}
		for _, u := range users {
			u.presence = newPresenceUserForAccount(u.Account, siteID)
		}
	}
```

Add the three fields to the returned `&stepEnv{...}` literal (after `cooldown: cfg.Cooldown,`):

```go
		presencePool:      presencePool,
		presenceCollector: presenceCollector,
		presenceHeartbeat: cfg.PresenceHeartbeat,
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd tools/loadgen && go test -race -run 'TestProdEnvFactory' ./...`
Expected: PASS.
Run: `cd tools/loadgen && go test -race ./...`
Expected: PASS (whole suite).

- [ ] **Step 5: Commit**

```bash
cd tools/loadgen && go build ./... && go test -race ./...
git add tools/loadgen/daily.go tools/loadgen/daily_test.go
git commit -m "feat(loadgen): build presence pool in daily prod factory when --presence set"
```

---

## Task 15: Presence reporting — console line + CSV columns

**Files:**
- Modify: `tools/loadgen/daily_report.go` — `renderConsole` (presence line), `writeDailyCSV` (columns)
- Test: `tools/loadgen/daily_report_test.go`

- [ ] **Step 1: Write the failing test**

Append to `tools/loadgen/daily_report_test.go`:

```go
func TestRenderConsole_PresenceLine(t *testing.T) {
	results := []StepResult{
		{
			N: 1000, EffectiveN: 1000,
			P50LatencyMs: 10, P95LatencyMs: 100, P99LatencyMs: 200,
			AttemptedOps: 5000, FailedOps: 0,
			Presence: &PresenceObsStats{P50Ms: 5, P95Ms: 40, P99Ms: 90, Attempted: 800, Failed: 4, ErrorRate: 0.005},
		},
	}
	var buf bytes.Buffer
	renderConsole(&buf, results)
	out := buf.String()
	assert.Contains(t, out, "presence:")
	assert.Contains(t, out, "p99=90")
}

func TestRenderConsole_NoPresenceLineWhenDisabled(t *testing.T) {
	results := []StepResult{{N: 1000, EffectiveN: 1000, AttemptedOps: 100}}
	var buf bytes.Buffer
	renderConsole(&buf, results)
	assert.NotContains(t, buf.String(), "presence:")
}

func TestWriteDailyCSV_PresenceColumns(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "daily.csv")
	results := []StepResult{
		{N: 1000, EffectiveN: 1000, StartedAt: time.Now(),
			Presence: &PresenceObsStats{P50Ms: 5, P95Ms: 40, P99Ms: 90, Attempted: 800, Failed: 4, ErrorRate: 0.005}},
	}
	require.NoError(t, writeDailyCSV(path, results))
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	out := string(b)
	assert.Contains(t, out, "presence_p99_ms")
	assert.Contains(t, out, "presence_attempted")
}

func TestWriteDailyCSV_PresenceColumnsZeroWhenDisabled(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "daily.csv")
	results := []StepResult{{N: 1000, EffectiveN: 1000, StartedAt: time.Now()}}
	require.NoError(t, writeDailyCSV(path, results))
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	// Header still has presence columns; row has zero-valued presence cells.
	assert.Contains(t, string(b), "presence_p50_ms")
}
```

(Ensure `bytes`, `os`, `path/filepath`, `time` are imported in
`daily_report_test.go` — add any missing to the import block.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cd tools/loadgen && go test -race -run 'TestRenderConsole_Presence|TestRenderConsole_NoPresence|TestWriteDailyCSV_Presence' ./...`
Expected: FAIL — presence line/columns not emitted.

- [ ] **Step 3: Add the console line and CSV columns**

In `tools/loadgen/daily_report.go`, in `renderConsole`, after the
`formatActionLatencies` block (inside the per-result loop, after the
`if len(r.ActionLatencies) > 0 {...}` block), add:

```go
		if r.Presence != nil {
			p := r.Presence
			fmt.Fprintf(w, "    presence: p50=%.0f p95=%.0f p99=%.0f err%%=%.3f (attempted=%d failed=%d)\n",
				p.P50Ms, p.P95Ms, p.P99Ms, p.ErrorRate*100, p.Attempted, p.Failed)
		}
```

In `writeDailyCSV`, extend the header slice — after the per-action columns are
appended (after the `for _, k := range allActionKinds {...}` header loop, before
`w.Write(header)`), add:

```go
	header = append(header,
		"presence_p50_ms", "presence_p95_ms", "presence_p99_ms",
		"presence_attempted", "presence_failed", "presence_error_rate")
```

In the row-building loop, after the per-action columns are appended to `row`
(after the `for _, k := range allActionKinds {...}` row loop, before
`w.Write(row)`), add:

```go
		var pP50, pP95, pP99, pErr float64
		var pAtt, pFail int64
		if r.Presence != nil {
			pP50, pP95, pP99 = r.Presence.P50Ms, r.Presence.P95Ms, r.Presence.P99Ms
			pAtt, pFail, pErr = r.Presence.Attempted, r.Presence.Failed, r.Presence.ErrorRate
		}
		row = append(row,
			fmt.Sprintf("%.0f", pP50), fmt.Sprintf("%.0f", pP95), fmt.Sprintf("%.0f", pP99),
			strconv.FormatInt(pAtt, 10), strconv.FormatInt(pFail, 10), fmt.Sprintf("%.6f", pErr))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd tools/loadgen && go test -race -run 'TestRenderConsole|TestWriteDailyCSV' ./...`
Expected: PASS (new + existing report tests — the existing CSV round-trip tests
must still pass with the added columns).

- [ ] **Step 5: Commit**

```bash
cd tools/loadgen && go build ./... && go test -race ./...
git add tools/loadgen/daily_report.go tools/loadgen/daily_report_test.go
git commit -m "feat(loadgen): report observational presence stats in daily output"
```

---

## Task 16: Phase 2 docs — README mention

**Files:**
- Modify: `tools/loadgen/README.md`

- [ ] **Step 1: Find the daily subcommand section**

Run: `cd tools/loadgen && grep -n "daily" README.md | head`
Expected: locate the `daily` documentation block.

- [ ] **Step 2: Add a `--presence` note**

Insert into the `daily` flags/usage documentation (match surrounding style):

```markdown
**Optional presence load:** `--presence` makes each daily user also maintain
presence (hello on activation, ping every `--presence-heartbeat`, activity flip
on each active↔idle transition). Presence latency/errors are reported
**observationally** (a `presence:` line per step and `presence_*` CSV columns)
and never affect the daily PASS/TRIP verdict. Off by default.

```
loadgen daily --preset=daily-heavy --presence --presence-heartbeat=30s --csv=daily.csv
```
```

- [ ] **Step 3: Commit**

```bash
git add tools/loadgen/README.md
git commit -m "docs(loadgen): document daily --presence flag"
```

---

## Task 17: Full-suite verification + lint + push

**Files:** none (verification only)

- [ ] **Step 1: Run the whole loadgen unit suite with race**

Run: `make test SERVICE=loadgen`
Expected: PASS, no data races.

- [ ] **Step 2: Lint**

Run: `make lint`
Expected: clean. Fix any `gofmt`/`goimports`/`staticcheck` findings inline
(common: import ordering in the new files, unused imports). Re-run until clean.

- [ ] **Step 3: SAST (matches CI gate)**

Run: `make sast`
Expected: no medium+ findings. The new code does no crypto, no `exec`, no file
perms beyond `os.Create` for CSV (mirrors existing `writePresenceCSV`), so no
suppressions should be needed.

- [ ] **Step 4: Manual smoke (optional, requires local stack)**

If a local NATS + presence service is running (`docker-compose` per
`tools/loadgen/deploy`):

Run: `cd tools/loadgen && go run . presence-capacity --steps=100,200 --warmup=2s --hold=5s --cooldown=1s`
Expected: a table with two rows and a `MAX CONCURRENT ONLINE:` line.

Run: `cd tools/loadgen && go run . daily --preset=daily-light --steps=200 --warmup=5s --hold=15s --presence --presence-heartbeat=5s`
Expected: a daily table with a `presence:` line under the step.

- [ ] **Step 5: Push the branch**

```bash
git push -u origin claude/user-presence-loadgen-nk1ni7
```

(Retry up to 4 times with exponential backoff on network errors. Do NOT open a
PR unless explicitly asked.)

---

## Self-review checklist (completed during plan authoring)

- **Spec coverage:**
  - §4 presence-capacity → Tasks 1–7 (watcher, verdict, config, env/runner, renderers, dispatch, docs). ✓
  - §4.5 collector watcher → Task 1. ✓
  - §4.4 verdict precedence incl. ping-before-false-offline → Task 2 (`TestEvaluateCapacityStep_PingShortfallBeatsFalseOffline`). ✓
  - §4.3 connect latency at activation + false offline at hold → Task 4 runner (snapshot before Reset, WatchOnline during hold). ✓
  - §5 daily --presence → Tasks 8–16. ✓
  - §5.3 `newPresenceUserForAccount` → Task 8. ✓
  - §5.4 stepEnv wiring + activation hello + emitter ping/flip → Tasks 11–12, 14. ✓
  - §5.5 observational stats (combined latency), verdict unchanged → Tasks 10, 13, 15. ✓
  - §5.6 config flags → Task 9. ✓
  - §5.7 shutdown drain → Task 13. ✓
- **Placeholder scan:** no TBD/TODO; every code step shows full code. ✓
- **Type consistency:** `capacityEnv`, `capacityStepInputs/Result`, `capacityThresholds`, `evaluateCapacityStep`, `renderCapacityConsole`, `writeCapacityCSV`, `newPresenceUserForAccount`, `emitPresence`, `presenceFlip`, `snapshotPresenceStats`, `PresenceObsStats` used consistently across tasks. `verdictKind`/`verdictPass`/`verdictTrip`/`verdictInconclusive`, `percentile`, `snapshotSelfMetrics`, `waitOrCancel`, `parseStepList`, `slicesMaxInt`, `joinReasons` reused from existing files. ✓
</content>
</invoke>
