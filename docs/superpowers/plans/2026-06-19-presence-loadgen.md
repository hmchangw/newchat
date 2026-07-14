# Presence Load Generator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add two new `tools/loadgen` subcommands — `presence-sustained` (ramp a simulated user population N to find the max sustainable N) and `presence-storm` (ramp a reconnect-storm fraction at fixed N to find the largest storm the service recovers from) — that drive `user-presence-service` over NATS and grade it against latency / error / recovery SLOs.

**Architecture:** Service-centric, NATS-only. The loadgen synthesizes accounts (`u-000000`-style, no Mongo/Valkey seeding — the presence service trusts the JWT self-token and never looks accounts up on hello/ping/activity/bye). A small shared **publisher pool** sends presence transitions on `chat.user.{account}.event.presence.{site}.{hello|ping|activity|bye}`; a small **observer pool** subscribes to the `chat.user.presence.state.*` wildcard and time-stamps each `model.PresenceState` publish. Each state-changing transition registers an **expectation** (account → expected status); the matching observed publish yields an end-to-end latency sample. Per-user transitions are serialized so each maps 1:1 to the next publish for that account (the wire payload carries no request ID — only account + status). Verdict reuses the existing in-package `verdictKind`, `percentile`, and `snapshotSelfMetrics`/`SelfMetrics` helpers.

**Tech Stack:** Go 1.25, `github.com/nats-io/nats.go`, `pkg/subject`, `pkg/model`, `pkg/idgen`, `pkg/natsutil`, `stretchr/testify`, `pkg/testutil` (integration). All commands via `make` targets.

**Phasing:** Phase 1 (Tasks 1–9) delivers a working, shippable `presence-sustained` mode. Phase 2 (Tasks 10–14) adds `presence-storm` on the same foundation. Phase 3 (Task 15) is docs. Each phase compiles, tests, and is independently useful.

**Conventions for every task:**
- All files live in `/home/user/chat/tools/loadgen/`, `package main`. Test files are `package main` too (access unexported types).
- Run unit tests with `make test SERVICE=loadgen` from repo root (the Makefile maps `SERVICE` to the package dir and adds `-race`). To run a single test during red/green: `cd /home/user/chat && go test -race ./tools/loadgen/ -run <TestName> -v`.
- Run `make lint` and `make fmt` before each commit; the pre-commit hook enforces them.
- Commit messages end with the two trailers configured for this repo (Co-Authored-By + Claude-Session). Do NOT include the model identifier anywhere.

---

## File Structure

| File | Responsibility |
|------|----------------|
| `presence_subjects.go` | Concrete presence write-subject builders (substitute `{account}` into the `pkg/subject` patterns). |
| `presence_user.go` | `presenceUser` per-identity state machine; builds the next transition (subject + JSON payload + expected resulting status). |
| `presence_collector.go` | Expectation registry, latency samples, attempted/failed counters, recovery tracker; observer callback target. |
| `presence_verdict.go` | `presenceThresholds` / `presenceStepInputs` / `presenceStepResult` + `evaluatePresenceStep` (sustained); `stormThresholds` / `stormStepInputs` / `stormStepResult` + `evaluateStormStep` (storm). |
| `presence_pool.go` | Shared publisher pool + observer pool (wildcard subscribe). |
| `presence.go` | `presence-sustained`: config parse, `stepEnv`, per-user emitter, ramp loop, env factory, prod entrypoint. |
| `presence_storm.go` | `presence-storm`: config parse, warm/drop/herd/recovery orchestration, ramp loop, prod entrypoint. |
| `presence_report.go` | Console + CSV rendering for both modes. |
| `presence_integration_test.go` | `//go:build integration` — drives real NATS + Valkey + presence-service. |
| `main.go` (modify) | Add `presence-sustained` / `presence-storm` dispatch cases + usage string. |
| `README.md` (modify) | Document the two modes. |

---

## Phase 1 — Foundation + `presence-sustained`

### Task 1: Concrete presence subject builders

**Files:**
- Create: `tools/loadgen/presence_subjects.go`
- Test: `tools/loadgen/presence_subjects_test.go`

The `pkg/subject` package exposes presence write subjects only as *patterns* with a literal `{account}` placeholder (e.g. `PresenceHelloPattern(siteID)` → `chat.user.{account}.event.presence.<site>.hello`). The loadgen must publish on concrete subjects, so substitute the real account. Keeping this in the loadgen avoids touching the shared client-facing subject contract.

- [ ] **Step 1: Write the failing test**

```go
package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPresenceConcreteSubjects(t *testing.T) {
	const account = "u-000007"
	const site = "site-local"

	assert.Equal(t, "chat.user.u-000007.event.presence.site-local.hello",
		presenceHelloSubject(account, site))
	assert.Equal(t, "chat.user.u-000007.event.presence.site-local.ping",
		presencePingSubject(account, site))
	assert.Equal(t, "chat.user.u-000007.event.presence.site-local.activity",
		presenceActivitySubject(account, site))
	assert.Equal(t, "chat.user.u-000007.event.presence.site-local.bye",
		presenceByeSubject(account, site))
}

func TestPresenceConcreteSubjects_NoPlaceholderLeftover(t *testing.T) {
	got := presenceHelloSubject("alice", "s1")
	assert.NotContains(t, got, "{account}")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/user/chat && go test ./tools/loadgen/ -run TestPresenceConcreteSubjects -v`
Expected: FAIL — `undefined: presenceHelloSubject` (etc.)

- [ ] **Step 3: Write minimal implementation**

```go
package main

import (
	"strings"

	"github.com/hmchangw/chat/pkg/subject"
)

// Presence write subjects are exposed by pkg/subject only as natsrouter
// patterns carrying a literal "{account}" token. The loadgen publishes on
// concrete subjects, so it substitutes the synthetic account into the pattern.
// Building on top of the pattern builders keeps the structure single-sourced.

func concretePresenceSubject(pattern, account string) string {
	return strings.Replace(pattern, "{account}", account, 1)
}

func presenceHelloSubject(account, siteID string) string {
	return concretePresenceSubject(subject.PresenceHelloPattern(siteID), account)
}

func presencePingSubject(account, siteID string) string {
	return concretePresenceSubject(subject.PresencePingPattern(siteID), account)
}

func presenceActivitySubject(account, siteID string) string {
	return concretePresenceSubject(subject.PresenceActivityPattern(siteID), account)
}

func presenceByeSubject(account, siteID string) string {
	return concretePresenceSubject(subject.PresenceByePattern(siteID), account)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /home/user/chat && go test ./tools/loadgen/ -run TestPresenceConcreteSubjects -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /home/user/chat
git add tools/loadgen/presence_subjects.go tools/loadgen/presence_subjects_test.go
git commit -m "feat(loadgen): concrete presence write-subject builders"
```

---

### Task 2: `presenceUser` state machine

**Files:**
- Create: `tools/loadgen/presence_user.go`
- Test: `tools/loadgen/presence_user_test.go`

A `presenceUser` is one synthetic identity. It mirrors the effective status the service *should* compute, so each transition can declare its expected resulting published status. With one connection per user, the aggregate is trivial: `away=true` → `away`, `away=false` → `online`, `bye` → `offline`. A transition whose `expect` is `StatusNone` produces no publish (steady-state ping, or an activity call that doesn't change the aggregate) and must NOT be measured.

- [ ] **Step 1: Write the failing test**

```go
package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

func TestPresenceUser_Account(t *testing.T) {
	assert.Equal(t, "u-000000", presenceAccount(0))
	assert.Equal(t, "u-000042", presenceAccount(42))
}

func TestPresenceUser_HelloOnline(t *testing.T) {
	u := newPresenceUser(1, "site-local")
	tr := u.hello(1000)
	assert.Equal(t, "chat.user.u-000001.event.presence.site-local.hello", tr.subject)
	assert.Equal(t, model.StatusOnline, tr.expect)

	var h model.Hello
	require.NoError(t, json.Unmarshal(tr.payload, &h))
	assert.Equal(t, u.connID, h.ConnID)
	assert.Equal(t, int64(1000), h.Timestamp)
	assert.Equal(t, model.StatusOnline, u.status)
}

func TestPresenceUser_PingIsNoOp(t *testing.T) {
	u := newPresenceUser(1, "site-local")
	u.hello(1000)
	tr := u.ping(2000)
	assert.Equal(t, "chat.user.u-000001.event.presence.site-local.ping", tr.subject)
	assert.Equal(t, model.StatusNone, tr.expect, "steady-state ping must expect no publish")
}

func TestPresenceUser_ActivityFlip(t *testing.T) {
	u := newPresenceUser(1, "site-local")
	u.hello(1000)

	away := u.setAway(true, 2000)
	assert.Equal(t, model.StatusAway, away.expect)
	var a model.Activity
	require.NoError(t, json.Unmarshal(away.payload, &a))
	assert.True(t, a.Away)
	assert.Equal(t, model.StatusAway, u.status)

	back := u.setAway(false, 3000)
	assert.Equal(t, model.StatusOnline, back.expect)
}

func TestPresenceUser_ActivityNoChangeNoPublish(t *testing.T) {
	u := newPresenceUser(1, "site-local")
	u.hello(1000)
	tr := u.setAway(false, 2000) // already online/active
	assert.Equal(t, model.StatusNone, tr.expect)
}

func TestPresenceUser_ByeOffline(t *testing.T) {
	u := newPresenceUser(1, "site-local")
	u.hello(1000)
	tr := u.bye(4000)
	assert.Equal(t, "chat.user.u-000001.event.presence.site-local.bye", tr.subject)
	assert.Equal(t, model.StatusOffline, tr.expect)
	assert.Equal(t, model.StatusOffline, u.status)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/user/chat && go test ./tools/loadgen/ -run TestPresenceUser -v`
Expected: FAIL — `undefined: newPresenceUser` (etc.)

- [ ] **Step 3: Write minimal implementation**

```go
package main

import (
	"encoding/json"
	"fmt"

	"github.com/hmchangw/chat/pkg/model"
)

// presenceTransition is one outbound presence event plus the effective status
// the service is expected to publish as a result. expect == StatusNone means
// the transition is a no-op at the service (steady-state ping or an activity
// call that doesn't move the aggregate) and must not be measured.
type presenceTransition struct {
	subject string
	payload []byte
	expect  model.PresenceStatus
}

// presenceUser is one synthetic identity with a single logical connection. It
// mirrors the effective status the service should compute so each transition
// can declare its expected resulting publish.
type presenceUser struct {
	idx     int
	account string
	connID  string
	siteID  string
	status  model.PresenceStatus // mirror of effective status
	away    bool                 // current activity (true = inactive)
}

// presenceAccount is the deterministic synthetic account for index i. The
// presence service never validates these against Mongo on hello/ping/activity/
// bye, so no seeding is required.
func presenceAccount(i int) string { return fmt.Sprintf("u-%06d", i) }

func newPresenceUser(idx int, siteID string) *presenceUser {
	account := presenceAccount(idx)
	return &presenceUser{
		idx:     idx,
		account: account,
		connID:  "c-" + account,
		siteID:  siteID,
		status:  model.StatusOffline,
	}
}

func mustMarshal(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		// Payloads are fixed-shape structs from pkg/model; marshal cannot fail.
		panic(fmt.Sprintf("marshal presence payload: %v", err))
	}
	return b
}

func (u *presenceUser) hello(tsMillis int64) presenceTransition {
	u.status = model.StatusOnline
	u.away = false
	return presenceTransition{
		subject: presenceHelloSubject(u.account, u.siteID),
		payload: mustMarshal(model.Hello{ConnID: u.connID, Timestamp: tsMillis}),
		expect:  model.StatusOnline,
	}
}

func (u *presenceUser) ping(tsMillis int64) presenceTransition {
	// A ping for a known connection is a no-op at the service.
	return presenceTransition{
		subject: presencePingSubject(u.account, u.siteID),
		payload: mustMarshal(model.Ping{ConnID: u.connID, Timestamp: tsMillis}),
		expect:  model.StatusNone,
	}
}

func (u *presenceUser) setAway(away bool, tsMillis int64) presenceTransition {
	tr := presenceTransition{
		subject: presenceActivitySubject(u.account, u.siteID),
		payload: mustMarshal(model.Activity{ConnID: u.connID, Away: away, Timestamp: tsMillis}),
		expect:  model.StatusNone,
	}
	// Only an actual change while online moves the published aggregate.
	if u.status != model.StatusOffline && away != u.away {
		if away {
			u.status = model.StatusAway
			tr.expect = model.StatusAway
		} else {
			u.status = model.StatusOnline
			tr.expect = model.StatusOnline
		}
	}
	u.away = away
	return tr
}

func (u *presenceUser) bye(tsMillis int64) presenceTransition {
	u.status = model.StatusOffline
	return presenceTransition{
		subject: presenceByeSubject(u.account, u.siteID),
		payload: mustMarshal(model.ByeRequest{ConnID: u.connID, Timestamp: tsMillis}),
		expect:  model.StatusOffline,
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /home/user/chat && go test ./tools/loadgen/ -run TestPresenceUser -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /home/user/chat
git add tools/loadgen/presence_user.go tools/loadgen/presence_user_test.go
git commit -m "feat(loadgen): presenceUser transition state machine"
```

---

### Task 3: `presenceCollector` — expectations, latency, errors, recovery

**Files:**
- Create: `tools/loadgen/presence_collector.go`
- Test: `tools/loadgen/presence_collector_test.go`

The collector is the observer-callback target. Per-user transitions are serialized, so at most one expectation is open per account. `Reset()` is called at hold start (clears samples, counters, and stale warmup expectations). `ReapMissing()` at hold end counts still-open expectations as failures. The recovery tracker (used by storm) is included here so the single observer callback feeds both.

- [ ] **Step 1: Write the failing test**

```go
package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/model"
)

func TestPresenceCollector_LatencySample(t *testing.T) {
	c := newPresenceCollector()
	t0 := time.Unix(0, 0)
	c.Expect("u-1", model.StatusOnline, t0)
	c.Observe("u-1", model.StatusOnline, t0.Add(40*time.Millisecond))

	assert.Equal(t, int64(1), c.Attempted())
	assert.Equal(t, int64(0), c.Failed())
	lat := c.LatenciesMs()
	assert.Len(t, lat, 1)
	assert.InDelta(t, 40.0, lat[0], 0.001)
}

func TestPresenceCollector_WrongStatusIgnored(t *testing.T) {
	c := newPresenceCollector()
	t0 := time.Unix(0, 0)
	c.Expect("u-1", model.StatusOnline, t0)
	c.Observe("u-1", model.StatusAway, t0.Add(10*time.Millisecond)) // not what we awaited
	assert.Len(t, c.LatenciesMs(), 0)
	c.ReapMissing()
	assert.Equal(t, int64(1), c.Failed(), "unresolved expectation reaps as missing")
}

func TestPresenceCollector_OrphanObserveIgnored(t *testing.T) {
	c := newPresenceCollector()
	// Sweeper-driven offline for an account we never awaited: orphan, ignored.
	c.Observe("u-99", model.StatusOffline, time.Now())
	assert.Equal(t, int64(0), c.Attempted())
	assert.Len(t, c.LatenciesMs(), 0)
}

func TestPresenceCollector_EmitFailure(t *testing.T) {
	c := newPresenceCollector()
	c.RecordEmit()
	c.RecordEmitFailure()
	assert.Equal(t, int64(1), c.Attempted())
	assert.Equal(t, int64(1), c.Failed())
}

func TestPresenceCollector_Reset(t *testing.T) {
	c := newPresenceCollector()
	t0 := time.Unix(0, 0)
	c.Expect("u-1", model.StatusOnline, t0)
	c.Observe("u-1", model.StatusOnline, t0.Add(time.Millisecond))
	c.Reset()
	assert.Equal(t, int64(0), c.Attempted())
	assert.Equal(t, int64(0), c.Failed())
	assert.Len(t, c.LatenciesMs(), 0)
	c.ReapMissing()
	assert.Equal(t, int64(0), c.Failed(), "reset must drop stale expectations")
}

func TestPresenceCollector_Recovery(t *testing.T) {
	c := newPresenceCollector()
	start := time.Unix(100, 0)
	c.BeginRecovery([]string{"u-1", "u-2"}, start)
	assert.False(t, c.RecoveryComplete())

	c.Observe("u-1", model.StatusOnline, start.Add(20*time.Millisecond))
	assert.False(t, c.RecoveryComplete())
	c.Observe("u-2", model.StatusOnline, start.Add(70*time.Millisecond))
	assert.True(t, c.RecoveryComplete())
	assert.InDelta(t, 70.0, float64(c.RecoveryElapsed().Milliseconds()), 0.001)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/user/chat && go test ./tools/loadgen/ -run TestPresenceCollector -v`
Expected: FAIL — `undefined: newPresenceCollector`

- [ ] **Step 3: Write minimal implementation**

```go
package main

import (
	"sync"
	"time"

	"github.com/hmchangw/chat/pkg/model"
)

// presenceExpectation is one in-flight state-changing transition awaiting its
// resulting PresenceState publish.
type presenceExpectation struct {
	status model.PresenceStatus
	sentAt time.Time
}

// presenceCollector correlates emitted transitions with observed state
// publishes. It is the single observer-callback target and is safe for
// concurrent use (many emitters + observer goroutines).
type presenceCollector struct {
	mu        sync.Mutex
	pending   map[string]presenceExpectation // account -> open expectation
	latencies []float64                      // ms, current hold window
	attempted int64
	failed    int64

	// recovery tracker (storm mode)
	recovering   bool
	recStart     time.Time
	recRemaining map[string]struct{}
	recElapsed   time.Duration
	recDone      bool
}

func newPresenceCollector() *presenceCollector {
	return &presenceCollector{pending: make(map[string]presenceExpectation)}
}

// RecordEmit counts one attempted state-changing transition.
func (c *presenceCollector) RecordEmit() {
	c.mu.Lock()
	c.attempted++
	c.mu.Unlock()
}

// RecordEmitFailure counts a publish that errored at send time.
func (c *presenceCollector) RecordEmitFailure() {
	c.mu.Lock()
	c.failed++
	c.mu.Unlock()
}

// Expect registers (and counts) one awaited transition. It both increments
// attempted and opens an expectation, so emitters call Expect instead of
// RecordEmit for transitions that should publish.
func (c *presenceCollector) Expect(account string, status model.PresenceStatus, sentAt time.Time) {
	c.mu.Lock()
	c.attempted++
	c.pending[account] = presenceExpectation{status: status, sentAt: sentAt}
	c.mu.Unlock()
}

// Observe is called for every PresenceState publish seen on the wildcard.
func (c *presenceCollector) Observe(account string, status model.PresenceStatus, at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.pending[account]; ok && e.status == status {
		c.latencies = append(c.latencies, float64(at.Sub(e.sentAt).Microseconds())/1000.0)
		delete(c.pending, account)
	}
	if c.recovering && !c.recDone && status == model.StatusOnline {
		if _, want := c.recRemaining[account]; want {
			delete(c.recRemaining, account)
			if len(c.recRemaining) == 0 {
				c.recDone = true
				c.recElapsed = at.Sub(c.recStart)
			}
		}
	}
}

// ReapMissing counts every still-open expectation as a missing observation
// (a transition that never produced its publish), then clears them.
func (c *presenceCollector) ReapMissing() {
	c.mu.Lock()
	c.failed += int64(len(c.pending))
	c.pending = make(map[string]presenceExpectation)
	c.mu.Unlock()
}

// Reset clears samples, counters, and stale expectations at hold start.
func (c *presenceCollector) Reset() {
	c.mu.Lock()
	c.pending = make(map[string]presenceExpectation)
	c.latencies = nil
	c.attempted = 0
	c.failed = 0
	c.mu.Unlock()
}

func (c *presenceCollector) Attempted() int64 { c.mu.Lock(); defer c.mu.Unlock(); return c.attempted }
func (c *presenceCollector) Failed() int64    { c.mu.Lock(); defer c.mu.Unlock(); return c.failed }

func (c *presenceCollector) LatenciesMs() []float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]float64, len(c.latencies))
	copy(out, c.latencies)
	return out
}

// BeginRecovery arms the recovery tracker for a set of accounts expected to
// come back online, anchored at start.
func (c *presenceCollector) BeginRecovery(accounts []string, start time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.recovering = true
	c.recDone = false
	c.recStart = start
	c.recElapsed = 0
	c.recRemaining = make(map[string]struct{}, len(accounts))
	for _, a := range accounts {
		c.recRemaining[a] = struct{}{}
	}
}

func (c *presenceCollector) RecoveryComplete() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.recDone
}

func (c *presenceCollector) RecoveryElapsed() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.recElapsed
}

// RecoveryRemaining is the count of accounts not yet observed back online.
func (c *presenceCollector) RecoveryRemaining() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.recRemaining)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /home/user/chat && go test ./tools/loadgen/ -run TestPresenceCollector -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /home/user/chat
git add tools/loadgen/presence_collector.go tools/loadgen/presence_collector_test.go
git commit -m "feat(loadgen): presence collector with expectation correlation"
```

---

### Task 4: Sustained verdict

**Files:**
- Create: `tools/loadgen/presence_verdict.go`
- Test: `tools/loadgen/presence_verdict_test.go`

Reuse the in-package `verdictKind`, `percentile`, and `SelfMetrics`. Signals (per the approved design): state-publish latency p95/p99, error rate, and the GC self-saturation INCONCLUSIVE guard. INCONCLUSIVE also covers zero-attempts and >5% activation shortfall.

- [ ] **Step 1: Write the failing test**

```go
package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func presenceTestThresholds() presenceThresholds {
	return presenceThresholds{P95Ms: 200, P99Ms: 500, ErrorRate: 0.01, GCPauseInconclusive: 50}
}

func TestEvaluatePresenceStep_Pass(t *testing.T) {
	in := presenceStepInputs{
		N: 1000, EffectiveN: 1000,
		LatencySamples: []float64{10, 20, 30, 40, 50},
		Attempted:      500, Failed: 0,
	}
	r := evaluatePresenceStep(in, presenceTestThresholds())
	assert.Equal(t, verdictPass, r.Kind)
}

func TestEvaluatePresenceStep_TripLatency(t *testing.T) {
	samples := make([]float64, 100)
	for i := range samples {
		samples[i] = 600 // all over p99 cap
	}
	in := presenceStepInputs{N: 1000, EffectiveN: 1000, LatencySamples: samples, Attempted: 100}
	r := evaluatePresenceStep(in, presenceTestThresholds())
	assert.Equal(t, verdictTrip, r.Kind)
	assert.NotEmpty(t, r.Reasons)
}

func TestEvaluatePresenceStep_TripErrorRate(t *testing.T) {
	in := presenceStepInputs{
		N: 1000, EffectiveN: 1000,
		LatencySamples: []float64{10}, Attempted: 100, Failed: 5, // 5% > 1%
	}
	r := evaluatePresenceStep(in, presenceTestThresholds())
	assert.Equal(t, verdictTrip, r.Kind)
}

func TestEvaluatePresenceStep_InconclusiveGC(t *testing.T) {
	in := presenceStepInputs{
		N: 1000, EffectiveN: 1000, LatencySamples: []float64{10}, Attempted: 100,
		Self: SelfMetrics{GCPauseP99Ms: 80},
	}
	r := evaluatePresenceStep(in, presenceTestThresholds())
	assert.Equal(t, verdictInconclusive, r.Kind)
}

func TestEvaluatePresenceStep_InconclusiveZeroAttempts(t *testing.T) {
	in := presenceStepInputs{N: 1000, EffectiveN: 1000, Attempted: 0}
	r := evaluatePresenceStep(in, presenceTestThresholds())
	assert.Equal(t, verdictInconclusive, r.Kind)
}

func TestEvaluatePresenceStep_InconclusiveActivationShortfall(t *testing.T) {
	in := presenceStepInputs{
		N: 1000, EffectiveN: 800, // 80% < 95%
		LatencySamples: []float64{10}, Attempted: 100,
	}
	r := evaluatePresenceStep(in, presenceTestThresholds())
	assert.Equal(t, verdictInconclusive, r.Kind)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/user/chat && go test ./tools/loadgen/ -run TestEvaluatePresenceStep -v`
Expected: FAIL — `undefined: presenceThresholds`

- [ ] **Step 3: Write minimal implementation**

```go
package main

import (
	"fmt"
	"time"
)

// presenceThresholds are the sustained-mode SLO cutoffs.
type presenceThresholds struct {
	P95Ms               float64
	P99Ms               float64
	ErrorRate           float64 // fraction (0.01 = 1%)
	GCPauseInconclusive float64 // loadgen self-saturation guard (ms)
}

func defaultPresenceThresholds() presenceThresholds {
	return presenceThresholds{P95Ms: 200, P99Ms: 500, ErrorRate: 0.01, GCPauseInconclusive: 50}
}

// presenceStepInputs is everything evaluatePresenceStep needs for one N step.
type presenceStepInputs struct {
	N              int
	EffectiveN     int
	StartedAt      time.Time
	HoldDuration   time.Duration
	LatencySamples []float64 // ms, transition -> observed publish
	Attempted      int64
	Failed         int64
	Self           SelfMetrics
}

// presenceStepResult is the sustained verdict for one N step.
type presenceStepResult struct {
	N            int
	EffectiveN   int
	StartedAt    time.Time
	HoldDuration time.Duration
	P50Ms        float64
	P95Ms        float64
	P99Ms        float64
	ErrorRate    float64
	Attempted    int64
	Failed       int64
	Self         SelfMetrics
	Kind         verdictKind
	Reasons      []string
}

//nolint:gocritic // hugeParam: pure-function copy cost is negligible per step.
func evaluatePresenceStep(in presenceStepInputs, th presenceThresholds) presenceStepResult {
	r := presenceStepResult{
		N: in.N, EffectiveN: in.EffectiveN,
		StartedAt: in.StartedAt, HoldDuration: in.HoldDuration,
		Attempted: in.Attempted, Failed: in.Failed, Self: in.Self,
		P50Ms: percentile(in.LatencySamples, 0.50),
		P95Ms: percentile(in.LatencySamples, 0.95),
		P99Ms: percentile(in.LatencySamples, 0.99),
	}
	if in.Attempted > 0 {
		r.ErrorRate = float64(in.Failed) / float64(in.Attempted)
	}

	// INCONCLUSIVE overrides PASS/TRIP.
	if in.Self.GCPauseP99Ms > th.GCPauseInconclusive {
		r.Kind = verdictInconclusive
		r.Reasons = []string{fmt.Sprintf("inconclusive: loadgen gc pause p99=%.1fms > %.0f", in.Self.GCPauseP99Ms, th.GCPauseInconclusive)}
		return r
	}
	if in.Attempted == 0 {
		r.Kind = verdictInconclusive
		r.Reasons = []string{"inconclusive: zero transitions attempted (publisher down or emitters not wired)"}
		return r
	}
	if in.N > 0 && in.EffectiveN > 0 && float64(in.EffectiveN)/float64(in.N) < 0.95 {
		r.Kind = verdictInconclusive
		r.Reasons = []string{fmt.Sprintf("inconclusive: only %d/%d users activated", in.EffectiveN, in.N)}
		return r
	}

	var reasons []string
	if r.P95Ms > th.P95Ms {
		reasons = append(reasons, fmt.Sprintf("p95=%.0fms > %.0f", r.P95Ms, th.P95Ms))
	}
	if r.P99Ms > th.P99Ms {
		reasons = append(reasons, fmt.Sprintf("p99=%.0fms > %.0f", r.P99Ms, th.P99Ms))
	}
	if r.ErrorRate > th.ErrorRate {
		reasons = append(reasons, fmt.Sprintf("error_rate=%.4f > %.4f", r.ErrorRate, th.ErrorRate))
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

Run: `cd /home/user/chat && go test ./tools/loadgen/ -run TestEvaluatePresenceStep -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /home/user/chat
git add tools/loadgen/presence_verdict.go tools/loadgen/presence_verdict_test.go
git commit -m "feat(loadgen): presence-sustained verdict evaluation"
```

---

### Task 5: Publisher + observer pools

**Files:**
- Create: `tools/loadgen/presence_pool.go`
- Test: `tools/loadgen/presence_pool_test.go`

A `presencePool` owns a round-robin pool of publisher connections and a set of observer connections subscribed to `chat.user.presence.state.*`. Reuses `connectWithCreds`. The round-robin index logic is unit-tested without a real broker via a small seam; live connect/subscribe is covered by the integration test (Task 9).

- [ ] **Step 1: Write the failing test**

```go
package main

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRoundRobinIndex(t *testing.T) {
	var rr roundRobin
	got := []int{rr.next(3), rr.next(3), rr.next(3), rr.next(3)}
	assert.Equal(t, []int{0, 1, 2, 0}, got)
}

func TestRoundRobinIndex_Concurrent(t *testing.T) {
	var rr roundRobin
	const n = 1000
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() { defer wg.Done(); _ = rr.next(4) }()
	}
	wg.Wait()
	// After n increments the counter has advanced by n; next index is deterministic.
	assert.Equal(t, int((uint64(n))%4), rr.next(4))
}

func TestDecodePresenceState(t *testing.T) {
	data := []byte(`{"account":"u-1","siteId":"s1","status":"online","timestamp":123}`)
	acc, status, ok := decodePresenceState(data)
	assert.True(t, ok)
	assert.Equal(t, "u-1", acc)
	assert.Equal(t, "online", string(status))

	_, _, ok = decodePresenceState([]byte(`not json`))
	assert.False(t, ok)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/user/chat && go test ./tools/loadgen/ -run 'TestRoundRobinIndex|TestDecodePresenceState' -v`
Expected: FAIL — `undefined: roundRobin`

- [ ] **Step 3: Write minimal implementation**

```go
package main

import (
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
)

// roundRobin is a lock-free monotonic counter used to spread publishes across
// the publisher connection pool.
type roundRobin struct{ n atomic.Uint64 }

func (r *roundRobin) next(mod int) int {
	if mod <= 0 {
		return 0
	}
	return int((r.n.Add(1) - 1) % uint64(mod))
}

// decodePresenceState extracts the account and status from a PresenceState
// publish. ok is false on malformed input.
func decodePresenceState(data []byte) (string, model.PresenceStatus, bool) {
	var st model.PresenceState
	if err := json.Unmarshal(data, &st); err != nil || st.Account == "" {
		return "", "", false
	}
	return st.Account, st.Status, true
}

// presencePool owns publisher conns (round-robin) and observer conns
// (subscribed to the per-user state wildcard). Each observed publish is
// timestamped and fed to the collector.
type presencePool struct {
	pubConns  []*nats.Conn
	obsConns  []*nats.Conn
	rr        roundRobin
	collector *presenceCollector
}

// newPresencePool dials pubN publisher conns and obsN observer conns, and
// subscribes every observer conn to chat.user.presence.state.*.
func newPresencePool(url, credsFile string, pubN, obsN int, c *presenceCollector) (*presencePool, error) {
	p := &presencePool{collector: c}
	for i := 0; i < pubN; i++ {
		nc, err := connectWithCreds(url, fmt.Sprintf("loadgen-presence-pub-%d", i), credsFile)
		if err != nil {
			p.Close()
			return nil, fmt.Errorf("presence publisher conn %d: %w", i, err)
		}
		p.pubConns = append(p.pubConns, nc)
	}
	wildcard := subject.PresenceState("*")
	for i := 0; i < obsN; i++ {
		nc, err := connectWithCreds(url, fmt.Sprintf("loadgen-presence-obs-%d", i), credsFile)
		if err != nil {
			p.Close()
			return nil, fmt.Errorf("presence observer conn %d: %w", i, err)
		}
		if _, err := nc.Subscribe(wildcard, p.onState); err != nil {
			_ = nc.Drain()
			p.Close()
			return nil, fmt.Errorf("presence observer subscribe: %w", err)
		}
		if err := nc.Flush(); err != nil {
			p.Close()
			return nil, fmt.Errorf("presence observer flush: %w", err)
		}
		p.obsConns = append(p.obsConns, nc)
	}
	return p, nil
}

func (p *presencePool) onState(m *nats.Msg) {
	acc, status, ok := decodePresenceState(m.Data)
	if !ok {
		return
	}
	p.collector.Observe(acc, status, time.Now())
}

// Publish sends one transition on a round-robin publisher conn with a fresh
// X-Request-ID header (matches the daily emitter convention).
func (p *presencePool) Publish(subj string, data []byte) error {
	if len(p.pubConns) == 0 {
		return fmt.Errorf("no publisher conn")
	}
	nc := p.pubConns[p.rr.next(len(p.pubConns))]
	return nc.PublishMsg(&nats.Msg{
		Subject: subj,
		Data:    data,
		Header:  nats.Header{natsutil.RequestIDHeader: []string{idgen.GenerateRequestID()}},
	})
}

// Close drains all connections.
func (p *presencePool) Close() {
	for _, nc := range p.pubConns {
		_ = nc.Drain()
	}
	for _, nc := range p.obsConns {
		_ = nc.Drain()
	}
	p.pubConns = nil
	p.obsConns = nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /home/user/chat && go test ./tools/loadgen/ -run 'TestRoundRobinIndex|TestDecodePresenceState' -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /home/user/chat
git add tools/loadgen/presence_pool.go tools/loadgen/presence_pool_test.go
git commit -m "feat(loadgen): presence publisher/observer connection pools"
```

---

### Task 6: `presence-sustained` run loop, config, env factory

**Files:**
- Create: `tools/loadgen/presence.go`
- Test: `tools/loadgen/presence_test.go`

Mirrors the `daily` shape: a `presenceConfig`, a `presenceEnv` bundling deps (stub-able), a `runStep`, an `envFactory` interface, `runPresenceSustainedForTest`, and the prod entrypoint `runPresenceSustained`. Per-user emitters run a heartbeat ping ticker plus probabilistic churn (activity flips + reconnects). The hold-window envelope is anchored exactly like daily so emitters started in an earlier step re-read it.

- [ ] **Step 1: Write the failing test (config + ramp with a stub env)**

```go
package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParsePresenceConfig_Defaults(t *testing.T) {
	cfg, err := parsePresenceConfig(nil)
	require.NoError(t, err)
	assert.Equal(t, []int{1000, 2000, 5000, 10000, 20000, 50000, 100000}, cfg.Steps)
	assert.Equal(t, 30*time.Second, cfg.Heartbeat)
	assert.True(t, cfg.StopOnTrip)
}

func TestParsePresenceConfig_Steps(t *testing.T) {
	cfg, err := parsePresenceConfig([]string{"--steps=1k,2k,5k", "--hold=10s"})
	require.NoError(t, err)
	assert.Equal(t, []int{1000, 2000, 5000}, cfg.Steps)
	assert.Equal(t, 10*time.Second, cfg.Hold)
}

// stubPresenceEnv records activations and lets the test inject latency/error.
type stubPresenceFactory struct {
	built *presenceEnv
}

func (f *stubPresenceFactory) Build(cfg presenceConfig) *presenceEnv {
	c := newPresenceCollector()
	env := &presenceEnv{
		collector:  c,
		thresholds: defaultPresenceThresholds(),
		siteID:     "site-local",
		warmup:     0, hold: 0, cooldown: 0,
		users: make([]*presenceUser, slicesMaxInt(cfg.Steps)),
		// emit is a no-op stub: simulate a healthy step by seeding the
		// collector directly when runStep resets+holds.
		emit: func(env *presenceEnv, u *presenceUser) {},
		// activate just counts; pretend everyone comes online.
		onActivated: func(env *presenceEnv, n int) {
			for i := 0; i < 50; i++ {
				c.Expect("u", "online", time.Unix(0, 0))
				c.Observe("u", "online", time.Unix(0, int64(10*time.Millisecond)))
			}
		},
	}
	for i := range env.users {
		env.users[i] = newPresenceUser(i, env.siteID)
	}
	f.built = env
	return env
}

func TestRunPresenceSustained_StubRamp(t *testing.T) {
	cfg := presenceConfig{Steps: []int{10, 20}, StopOnTrip: false}
	results, err := runPresenceSustainedForTest(context.Background(), cfg, &stubPresenceFactory{})
	require.NoError(t, err)
	require.Len(t, results, 2)
	assert.Equal(t, 10, results[0].N)
	assert.Equal(t, verdictPass, results[0].Kind)
}
```

> Note: `slicesMaxInt` is a tiny helper added in Step 3; if you prefer, inline `slices.Max(cfg.Steps)`.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/user/chat && go test ./tools/loadgen/ -run 'TestParsePresenceConfig|TestRunPresenceSustained' -v`
Expected: FAIL — `undefined: parsePresenceConfig`

- [ ] **Step 3: Write minimal implementation**

```go
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

type presenceConfig struct {
	Steps           []int
	Warmup          time.Duration
	Hold            time.Duration
	Cooldown        time.Duration
	Heartbeat       time.Duration // ping interval per user
	ActivityFlipRate float64      // activity flips per user per minute
	ReconnectRate   float64       // reconnects per user per minute
	StopOnTrip      bool
	PublisherConns  int
	ObserverConns   int
	CSVPath         string
	P95Ms           float64
	P99Ms           float64
	ErrorRate       float64
}

func slicesMaxInt(xs []int) int {
	if len(xs) == 0 {
		return 0
	}
	return slices.Max(xs)
}

func parsePresenceConfig(args []string) (presenceConfig, error) {
	fs := flag.NewFlagSet("presence-sustained", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), `loadgen presence-sustained — find the sustainable presence population N

Ramps a synthetic user population through --steps. For each N: activates the
delta of new users (each sends hello), warms up, holds while users heartbeat
(ping) and churn (activity flips + reconnects), then grades state-publish
latency, error rate, and loadgen self-saturation. Reports the largest passing N.

Flags:
`)
		fs.PrintDefaults()
	}
	steps := fs.String("steps", "1000,2000,5000,10000,20000,50000,100000", "comma-separated N per step; `k` suffix x1000")
	warmup := fs.Duration("warmup", 30*time.Second, "per-step warm-up before measurement")
	hold := fs.Duration("hold", 120*time.Second, "per-step steady-state measurement window")
	cooldown := fs.Duration("cooldown", 15*time.Second, "per-step cooldown before next step")
	heartbeat := fs.Duration("heartbeat", 30*time.Second, "per-user ping interval (matches PRESENCE_HEARTBEAT_INTERVAL)")
	flip := fs.Float64("activity-flip-rate", 4, "activity flips per user per minute (churn that publishes)")
	reconnect := fs.Float64("reconnect-rate", 1, "bye+hello reconnects per user per minute")
	stop := fs.Bool("stop-on-trip", true, "stop the ramp on the first TRIP")
	pub := fs.Int("publisher-conns", 16, "shared publisher connection count")
	obs := fs.Int("observer-conns", 4, "observer connection count (subscribe presence.state.*)")
	csv := fs.String("csv", "", "optional CSV output path")
	p95 := fs.Float64("p95-ms", 200, "state-publish p95 latency cap (ms)")
	p99 := fs.Float64("p99-ms", 500, "state-publish p99 latency cap (ms)")
	errRate := fs.Float64("error-rate", 0.01, "error-rate cap (fraction)")
	if err := fs.Parse(args); err != nil {
		return presenceConfig{}, err
	}
	parsedSteps, err := parseStepList(*steps)
	if err != nil {
		return presenceConfig{}, err
	}
	return presenceConfig{
		Steps: parsedSteps, Warmup: *warmup, Hold: *hold, Cooldown: *cooldown,
		Heartbeat: *heartbeat, ActivityFlipRate: *flip, ReconnectRate: *reconnect,
		StopOnTrip: *stop, PublisherConns: *pub, ObserverConns: *obs, CSVPath: *csv,
		P95Ms: *p95, P99Ms: *p99, ErrorRate: *errRate,
	}, nil
}

// presenceEnv bundles a sustained run's deps. Stub-able for unit tests via the
// emit / onActivated function fields (nil in prod → real wiring used).
type presenceEnv struct {
	pool        *presencePool
	collector   *presenceCollector
	users       []*presenceUser
	thresholds  presenceThresholds
	siteID      string
	warmup      time.Duration
	hold        time.Duration
	cooldown    time.Duration
	heartbeat   time.Duration
	flipRate    float64
	reconnRate  float64

	// Seams for unit tests; prod leaves them nil and uses the real loops.
	emit        func(env *presenceEnv, u *presenceUser)
	onActivated func(env *presenceEnv, n int)

	holdStartNanos    atomic.Int64
	holdDurationNanos atomic.Int64
	activated         atomic.Int64
}

func (env *presenceEnv) setHold(start time.Time, d time.Duration) {
	env.holdStartNanos.Store(start.UnixNano())
	env.holdDurationNanos.Store(d.Nanoseconds())
}

func (env *presenceEnv) holding() bool { return env.holdDurationNanos.Load() > 0 }

type presenceFactory interface {
	Build(cfg presenceConfig) *presenceEnv
}

// runStepPresence activates the delta of new users, warms up, holds while
// measuring, then cools down and returns the graded step.
func runStepPresence(ctx context.Context, env *presenceEnv, n, prevN int) presenceStepResult {
	activatePresenceUsers(ctx, env, prevN, n)
	waitOrCancel(ctx, env.warmup)

	startedAt := time.Now()
	env.setHold(startedAt, env.hold)
	env.collector.Reset()
	waitOrCancel(ctx, env.hold)
	env.collector.ReapMissing()
	env.holdDurationNanos.Store(0) // pause churn during cooldown

	in := presenceStepInputs{
		N: n, EffectiveN: int(env.activated.Load()),
		StartedAt: startedAt, HoldDuration: env.hold,
		LatencySamples: env.collector.LatenciesMs(),
		Attempted:      env.collector.Attempted(),
		Failed:         env.collector.Failed(),
		Self:           snapshotSelfMetrics(),
	}
	r := evaluatePresenceStep(in, env.thresholds)
	waitOrCancel(ctx, env.cooldown)
	return r
}

// activatePresenceUsers brings users [from,to) online (hello) at a bounded
// rate and starts their per-user emitter goroutine.
func activatePresenceUsers(ctx context.Context, env *presenceEnv, from, to int) {
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
		u := env.users[i]
		if env.onActivated != nil {
			env.onActivated(env, i) // test seam
		} else {
			startPresenceEmitter(ctx, env, u)
		}
		env.activated.Add(1)
	}
}

// startPresenceEmitter runs one user's hello + heartbeat + churn loop until
// ctx cancels.
func startPresenceEmitter(ctx context.Context, env *presenceEnv, u *presenceUser) {
	// Initial hello (counted but pre-hold; cleared by Reset at hold start).
	emitTransition(env, u.hello(nowMillis()))
	go func() {
		r := rand.New(rand.NewSource(int64(u.idx)*0x9E3779B97F4A7C15 + 1))
		tick := time.NewTicker(env.heartbeat)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
			// Heartbeat ping (no-op publish).
			emitTransition(env, u.ping(nowMillis()))
			if !env.holding() {
				continue
			}
			// Per-tick churn probability = rate-per-min * heartbeat-fraction-of-min.
			frac := env.heartbeat.Minutes()
			if r.Float64() < env.flipRate*frac {
				emitTransition(env, u.setAway(!u.away, nowMillis()))
			}
			if r.Float64() < env.reconnRate*frac {
				emitTransition(env, u.bye(nowMillis()))
				emitTransition(env, u.hello(nowMillis()))
			}
		}
	}()
}

// emitTransition publishes one transition, recording attempt/expectation/
// failure against the collector. No-op transitions (expect == StatusNone) are
// sent but not counted as measurable attempts.
func emitTransition(env *presenceEnv, tr presenceTransition) {
	if env.emit != nil { // test seam
		return
	}
	sentAt := time.Now()
	err := env.pool.Publish(tr.subject, tr.payload)
	if tr.expect == "" { // StatusNone: no publish expected, don't measure
		return
	}
	if err != nil {
		env.collector.RecordEmit()
		env.collector.RecordEmitFailure()
		return
	}
	// Expect counts the attempt and opens the expectation.
	acc := accountFromSubject(tr.subject)
	env.collector.Expect(acc, tr.expect, sentAt)
}

func nowMillis() int64 { return time.Now().UTC().UnixMilli() }

// accountFromSubject extracts {account} from chat.user.{account}.event... .
func accountFromSubject(subj string) string {
	const prefix = "chat.user."
	if len(subj) <= len(prefix) {
		return ""
	}
	rest := subj[len(prefix):]
	for i := 0; i < len(rest); i++ {
		if rest[i] == '.' {
			return rest[:i]
		}
	}
	return rest
}

func waitOrCancel(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

//nolint:gocritic // cfg passed by value to satisfy presenceFactory interface
func runPresenceSustainedForTest(ctx context.Context, cfg presenceConfig, f presenceFactory) ([]presenceStepResult, error) {
	if len(cfg.Steps) == 0 {
		return nil, fmt.Errorf("cfg.Steps cannot be empty")
	}
	env := f.Build(cfg)
	if env.pool != nil {
		defer env.pool.Close()
	}
	prevN := 0
	var results []presenceStepResult
	for _, n := range cfg.Steps {
		if err := ctx.Err(); err != nil {
			slog.Info("presence-sustained interrupted", "completed_steps", len(results))
			break
		}
		r := runStepPresence(ctx, env, n, prevN)
		results = append(results, r)
		if cfg.StopOnTrip && r.Kind == verdictTrip {
			break
		}
		prevN = n
	}
	return results, nil
}

// prodPresenceFactory wires the real pool.
type prodPresenceFactory struct{ baseCfg *config }

//nolint:gocritic // cfg by value to match interface
func (f *prodPresenceFactory) Build(cfg presenceConfig) *presenceEnv {
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
	return &presenceEnv{
		pool: pool, collector: c, users: users,
		thresholds: presenceThresholds{P95Ms: cfg.P95Ms, P99Ms: cfg.P99Ms, ErrorRate: cfg.ErrorRate, GCPauseInconclusive: 50},
		siteID:    siteID,
		warmup:    cfg.Warmup, hold: cfg.Hold, cooldown: cfg.Cooldown,
		heartbeat: cfg.Heartbeat, flipRate: cfg.ActivityFlipRate, reconnRate: cfg.ReconnectRate,
	}
}

// runPresenceSustained is the production entrypoint invoked by main.go.
func runPresenceSustained(ctx context.Context, baseCfg *config, args []string) int {
	cfg, err := parsePresenceConfig(args)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		slog.Error("parse presence-sustained config", "error", err)
		return 2
	}
	results, err := runPresenceSustainedForTest(ctx, cfg, &prodPresenceFactory{baseCfg: baseCfg})
	if err != nil {
		slog.Error("presence-sustained run", "error", err)
		return 1
	}
	renderPresenceConsole(os.Stdout, results)
	if cfg.CSVPath != "" {
		if err := writePresenceCSV(cfg.CSVPath, results); err != nil {
			slog.Error("write presence csv", "error", err)
			return 1
		}
	}
	return presenceExitCode(results)
}

// presenceExitCode returns 0 if any step passed, else 1.
func presenceExitCode(results []presenceStepResult) int {
	for i := range results {
		if results[i].Kind == verdictPass {
			return 0
		}
	}
	return 1
}

var _ = sync.Mutex{} // keep sync imported if unused after edits
```

> The `emit`/`onActivated` seams are wired so the stub factory test exercises the ramp loop without a broker. Prod paths use the real `pool`. After implementing, delete the `var _ = sync.Mutex{}` line if `sync` ends up used elsewhere; the linter will tell you.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /home/user/chat && go test ./tools/loadgen/ -run 'TestParsePresenceConfig|TestRunPresenceSustained' -v`
Expected: PASS

- [ ] **Step 5: Run the full package + lint**

Run: `cd /home/user/chat && go test -race ./tools/loadgen/ && make lint`
Expected: PASS (renderPresenceConsole / writePresenceCSV are defined in Task 7; if running before Task 7, stub them or implement Task 7 first — see note).

> Ordering note: `presence.go` references `renderPresenceConsole` and `writePresenceCSV` from Task 7. Implement Task 7 in the same working session before running the full-package build, or temporarily add no-op stubs. The two tasks are committed separately but must both be present to compile.

- [ ] **Step 6: Commit**

```bash
cd /home/user/chat
git add tools/loadgen/presence.go tools/loadgen/presence_test.go
git commit -m "feat(loadgen): presence-sustained ramp loop and config"
```

---

### Task 7: Sustained reporting (console + CSV)

**Files:**
- Create: `tools/loadgen/presence_report.go`
- Test: `tools/loadgen/presence_report_test.go`

- [ ] **Step 1: Write the failing test**

```go
package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRenderPresenceConsole_Answer(t *testing.T) {
	results := []presenceStepResult{
		{N: 1000, P95Ms: 40, P99Ms: 90, Kind: verdictPass},
		{N: 2000, P95Ms: 250, P99Ms: 600, Kind: verdictTrip, Reasons: []string{"p99=600ms > 500"}},
	}
	var buf bytes.Buffer
	renderPresenceConsole(&buf, results)
	out := buf.String()
	assert.Contains(t, out, "ANSWER: N = 1000")
	assert.Contains(t, out, "Next limit: p99=600ms > 500")
}

func TestRenderPresenceConsole_NoPass(t *testing.T) {
	var buf bytes.Buffer
	renderPresenceConsole(&buf, []presenceStepResult{{N: 1000, Kind: verdictTrip, Reasons: []string{"x"}}})
	assert.Contains(t, buf.String(), "no step passed")
}

func TestWritePresenceCSV(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.csv")
	require.NoError(t, writePresenceCSV(path, []presenceStepResult{
		{N: 1000, EffectiveN: 1000, P50Ms: 10, P95Ms: 40, P99Ms: 90, ErrorRate: 0, Attempted: 500, Kind: verdictPass},
	}))
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	assert.Equal(t, "n,effective_n,p50_ms,p95_ms,p99_ms,error_rate,attempted,failed,verdict,reasons", lines[0])
	assert.Contains(t, lines[1], "1000")
	assert.Contains(t, lines[1], "PASS")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/user/chat && go test ./tools/loadgen/ -run 'TestRenderPresenceConsole|TestWritePresenceCSV' -v`
Expected: FAIL — `undefined: renderPresenceConsole`

- [ ] **Step 3: Write minimal implementation**

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

func renderPresenceConsole(w io.Writer, results []presenceStepResult) {
	fmt.Fprintln(w, "N        p50    p95    p99    err%    verdict")
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
		fmt.Fprintf(w, "%-8s %-6.0f %-6.0f %-6.0f %-7.3f%% %s\n",
			nLabel, r.P50Ms, r.P95Ms, r.P99Ms, r.ErrorRate*100, r.Kind)
		if r.Kind != verdictPass && len(r.Reasons) > 0 {
			fmt.Fprintf(w, "    reasons: %s\n", joinReasons(r.Reasons))
		}
	}
	fmt.Fprintln(w)
	if lastPass > 0 {
		fmt.Fprintf(w, "ANSWER: N = %d (last passing step)\n", lastPass)
		for i := range results {
			if results[i].Kind == verdictTrip {
				fmt.Fprintf(w, "        Next limit: %s\n", joinReasons(results[i].Reasons))
				break
			}
		}
	} else {
		fmt.Fprintln(w, "ANSWER: no step passed")
	}
}

func writePresenceCSV(path string, results []presenceStepResult) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create csv: %w", err)
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	if err := w.Write([]string{"n", "effective_n", "p50_ms", "p95_ms", "p99_ms", "error_rate", "attempted", "failed", "verdict", "reasons"}); err != nil {
		return fmt.Errorf("write csv header: %w", err)
	}
	rs := make([]presenceStepResult, len(results))
	copy(rs, results)
	sort.Slice(rs, func(i, j int) bool { return rs[i].N < rs[j].N })
	for i := range rs {
		r := &rs[i]
		row := []string{
			strconv.Itoa(r.N), strconv.Itoa(r.EffectiveN),
			fmt.Sprintf("%.0f", r.P50Ms), fmt.Sprintf("%.0f", r.P95Ms), fmt.Sprintf("%.0f", r.P99Ms),
			fmt.Sprintf("%.6f", r.ErrorRate),
			strconv.FormatInt(r.Attempted, 10), strconv.FormatInt(r.Failed, 10),
			r.Kind.String(), joinReasons(r.Reasons),
		}
		if err := w.Write(row); err != nil {
			return fmt.Errorf("write csv row: %w", err)
		}
	}
	return nil
}
```

> `joinReasons` already exists in `daily_report.go` (same package) — reuse it, do not redefine.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /home/user/chat && go test ./tools/loadgen/ -run 'TestRenderPresenceConsole|TestWritePresenceCSV' -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /home/user/chat
git add tools/loadgen/presence_report.go tools/loadgen/presence_report_test.go
git commit -m "feat(loadgen): presence-sustained console + CSV reporting"
```

---

### Task 8: Wire `presence-sustained` into dispatch

**Files:**
- Modify: `tools/loadgen/main.go:76` (usage string) and `tools/loadgen/main.go:99-119` (dispatch switch)

- [ ] **Step 1: Add the dispatch case**

In `dispatch`, add before the `default:` case (after the `case "max-room-size":` block):

```go
	case "presence-sustained":
		return runPresenceSustained(ctx, cfg, os.Args[2:])
```

- [ ] **Step 2: Update the usage string**

Change the usage line at `main.go:76` to include the new subcommand:

```go
		fmt.Fprintln(os.Stderr, "usage: loadgen <seed|run|teardown|members-sustained|members-capacity|history-sustained|max-rps|daily|max-room-size|presence-sustained> [flags]")
```

- [ ] **Step 3: Build and verify the subcommand is reachable**

Run: `cd /home/user/chat && make build SERVICE=loadgen && go test -race ./tools/loadgen/`
Expected: build succeeds; tests PASS.

Manually confirm help works (no NATS needed — parse happens before connect; note that prod `Build` will log a pool-init error if NATS is down, which is fine for `-h`):

Run: `cd /home/user/chat && go run ./tools/loadgen presence-sustained -h 2>&1 | head -5`
Expected: prints the presence-sustained usage block.

- [ ] **Step 4: Commit**

```bash
cd /home/user/chat
git add tools/loadgen/main.go
git commit -m "feat(loadgen): wire presence-sustained subcommand"
```

---

### Task 9: Integration test (sustained)

**Files:**
- Create: `tools/loadgen/presence_integration_test.go`

Drives the real presence service against shared `testutil.NATS` + `testutil.SharedValkeyCluster`. The presence service runs in-process (import its handler/store) OR — preferred and simpler — assert the loadgen primitives against a real NATS+Valkey-backed presence handler started in the test. Inspect `user-presence-service/integration_test.go` for how the service's store + handler + sweeper are constructed from `testutil` so this test can stand one up and publish through it.

- [ ] **Step 1: Read the presence service's integration harness**

Run: `cd /home/user/chat && sed -n '1,60p' user-presence-service/integration_test.go`
Note how it builds the valkey store and (if present) the natsrouter registration. Reuse that shape.

- [ ] **Step 2: Write the integration test**

```go
//go:build integration

package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

// TestPresenceSustained_EndToEnd stands up a real NATS + presence service and
// verifies the loadgen observes online/away/offline state publishes with
// non-empty latency samples and zero missing observations on a tiny ramp.
func TestPresenceSustained_EndToEnd(t *testing.T) {
	natsURL := testutil.NATS(t)
	startPresenceServiceForTest(t, natsURL) // helper: see Step 3

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg := presenceConfig{
		Steps: []int{20}, Warmup: time.Second, Hold: 4 * time.Second, Cooldown: 0,
		Heartbeat: time.Second, ActivityFlipRate: 60, ReconnectRate: 10,
		StopOnTrip: false, PublisherConns: 2, ObserverConns: 1,
		P95Ms: 2000, P99Ms: 5000, ErrorRate: 0.5,
	}
	baseCfg := &config{NatsURL: natsURL, SiteID: "site-local"}
	results, err := runPresenceSustainedForTest(ctx, cfg, &prodPresenceFactory{baseCfg: baseCfg})
	require.NoError(t, err)
	require.Len(t, results, 1)
	r := results[0]
	assert.Equal(t, verdictPass, r.Kind, "reasons: %v", r.Reasons)
	assert.Greater(t, r.Attempted, int64(0))
	assert.Greater(t, r.P95Ms, 0.0, "expected at least one latency sample")
}
```

- [ ] **Step 3: Implement `startPresenceServiceForTest`**

Add a `_test.go` helper (same file) that constructs the presence valkey store from `testutil.SharedValkeyCluster(t)` (register `t.Cleanup(func(){ testutil.FlushValkey(t) })`), wires the handler with a publish function bound to a `testutil.NATS` connection, registers the four presence patterns for `site-local` via `natsrouter`, and starts the sweeper. Mirror `user-presence-service/main.go` wiring exactly (config defaults: stale 45s, sweep 5s, conns TTL 5m). Keep the connection drained on `t.Cleanup`.

> If standing up the full service in-process proves heavy, the acceptable fallback is to run the service binary via its existing compose/integration harness; but prefer in-process for speed and isolation, consistent with the daily integration test.

- [ ] **Step 4: Run the integration test**

Run: `cd /home/user/chat && make test-integration SERVICE=loadgen` (or `go test -race -tags integration ./tools/loadgen/ -run TestPresenceSustained_EndToEnd -v`)
Expected: PASS. Requires Docker.

- [ ] **Step 5: Commit**

```bash
cd /home/user/chat
git add tools/loadgen/presence_integration_test.go
git commit -m "test(loadgen): presence-sustained integration test"
```

**Phase 1 complete:** `presence-sustained` is a working, graded mode.

---

## Phase 2 — `presence-storm`

### Task 10: Storm verdict

**Files:**
- Modify: `tools/loadgen/presence_verdict.go` (append storm types)
- Test: `tools/loadgen/presence_verdict_test.go` (append)

Per the approved design, storm output is the **max survivable storm fraction**: each fraction step PASSES if recovery completed within the SLO AND spike error rate ≤ cap AND spike p99 ≤ cap; else TRIP. Incomplete recovery (timed out) is a TRIP, not INCONCLUSIVE. GC saturation is INCONCLUSIVE.

- [ ] **Step 1: Write the failing test (append to presence_verdict_test.go)**

```go
func stormTestThresholds() stormThresholds {
	return stormThresholds{RecoverySLO: 10 * time.Second, P99Ms: 1000, ErrorRate: 0.05, GCPauseInconclusive: 50}
}

func TestEvaluateStormStep_Pass(t *testing.T) {
	in := stormStepInputs{
		Fraction: 0.5, StormUsers: 500,
		RecoveryComplete: true, RecoveryElapsed: 3 * time.Second,
		SpikeLatencyMs: []float64{100, 200, 300}, Attempted: 500, Failed: 0,
	}
	r := evaluateStormStep(in, stormTestThresholds())
	assert.Equal(t, verdictPass, r.Kind)
}

func TestEvaluateStormStep_TripSlowRecovery(t *testing.T) {
	in := stormStepInputs{
		Fraction: 1.0, StormUsers: 1000,
		RecoveryComplete: true, RecoveryElapsed: 20 * time.Second,
		SpikeLatencyMs: []float64{100}, Attempted: 1000,
	}
	r := evaluateStormStep(in, stormTestThresholds())
	assert.Equal(t, verdictTrip, r.Kind)
}

func TestEvaluateStormStep_TripIncompleteRecovery(t *testing.T) {
	in := stormStepInputs{
		Fraction: 1.0, StormUsers: 1000, RecoveryComplete: false,
		RecoveryRemaining: 12, SpikeLatencyMs: []float64{100}, Attempted: 1000,
	}
	r := evaluateStormStep(in, stormTestThresholds())
	assert.Equal(t, verdictTrip, r.Kind)
}

func TestEvaluateStormStep_InconclusiveGC(t *testing.T) {
	in := stormStepInputs{
		Fraction: 0.5, StormUsers: 500, RecoveryComplete: true, RecoveryElapsed: time.Second,
		SpikeLatencyMs: []float64{10}, Attempted: 500, Self: SelfMetrics{GCPauseP99Ms: 80},
	}
	r := evaluateStormStep(in, stormTestThresholds())
	assert.Equal(t, verdictInconclusive, r.Kind)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/user/chat && go test ./tools/loadgen/ -run TestEvaluateStormStep -v`
Expected: FAIL — `undefined: stormThresholds`

- [ ] **Step 3: Write minimal implementation (append to presence_verdict.go)**

```go
// stormThresholds are the per-storm-step SLO cutoffs.
type stormThresholds struct {
	RecoverySLO         time.Duration
	P99Ms               float64
	ErrorRate           float64
	GCPauseInconclusive float64
}

func defaultStormThresholds() stormThresholds {
	return stormThresholds{RecoverySLO: 10 * time.Second, P99Ms: 1000, ErrorRate: 0.05, GCPauseInconclusive: 50}
}

type stormStepInputs struct {
	Fraction          float64
	StormUsers        int
	RecoveryComplete  bool
	RecoveryElapsed   time.Duration
	RecoveryRemaining int
	SpikeLatencyMs    []float64
	Attempted         int64
	Failed            int64
	Self              SelfMetrics
}

type stormStepResult struct {
	Fraction         float64
	StormUsers       int
	RecoveryComplete bool
	RecoveryMs       float64
	P99Ms            float64
	ErrorRate        float64
	Kind             verdictKind
	Reasons          []string
}

//nolint:gocritic // hugeParam: pure-function copy cost negligible per step.
func evaluateStormStep(in stormStepInputs, th stormThresholds) stormStepResult {
	r := stormStepResult{
		Fraction: in.Fraction, StormUsers: in.StormUsers,
		RecoveryComplete: in.RecoveryComplete,
		RecoveryMs:       float64(in.RecoveryElapsed.Milliseconds()),
		P99Ms:            percentile(in.SpikeLatencyMs, 0.99),
	}
	if in.Attempted > 0 {
		r.ErrorRate = float64(in.Failed) / float64(in.Attempted)
	}
	if in.Self.GCPauseP99Ms > th.GCPauseInconclusive {
		r.Kind = verdictInconclusive
		r.Reasons = []string{fmt.Sprintf("inconclusive: loadgen gc pause p99=%.1fms > %.0f", in.Self.GCPauseP99Ms, th.GCPauseInconclusive)}
		return r
	}
	var reasons []string
	if !in.RecoveryComplete {
		reasons = append(reasons, fmt.Sprintf("recovery incomplete: %d users never observed back online within SLO", in.RecoveryRemaining))
	} else if in.RecoveryElapsed > th.RecoverySLO {
		reasons = append(reasons, fmt.Sprintf("recovery=%s > %s", in.RecoveryElapsed.Round(time.Millisecond), th.RecoverySLO))
	}
	if r.P99Ms > th.P99Ms {
		reasons = append(reasons, fmt.Sprintf("spike p99=%.0fms > %.0f", r.P99Ms, th.P99Ms))
	}
	if r.ErrorRate > th.ErrorRate {
		reasons = append(reasons, fmt.Sprintf("error_rate=%.4f > %.4f", r.ErrorRate, th.ErrorRate))
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

Run: `cd /home/user/chat && go test ./tools/loadgen/ -run TestEvaluateStormStep -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /home/user/chat
git add tools/loadgen/presence_verdict.go tools/loadgen/presence_verdict_test.go
git commit -m "feat(loadgen): presence-storm verdict evaluation"
```

---

### Task 11: Storm orchestration + config

**Files:**
- Create: `tools/loadgen/presence_storm.go`
- Test: `tools/loadgen/presence_storm_test.go`

Orchestration: warm N users (hello + steady ping, no churn). For each fraction in `--storm-steps`: pick `floor(f*N)` users; drop them (`graceful` → bye; `silent` → stop pinging and wait > stale threshold so the sweeper marks them offline); arm recovery for those accounts; thundering-herd `hello` them (optionally paced by `--reconnect-rate`); wait until recovery completes or SLO deadline; grade; cooldown to re-settle.

The per-user steady-ping goroutines must be individually pausable (silent drop). Model each warmed user with a `stormConn` holding a `paused atomic.Bool`.

- [ ] **Step 1: Write the failing test (config + fraction math + grading via stub)**

```go
package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseStormConfig_Defaults(t *testing.T) {
	cfg, err := parseStormConfig(nil)
	require.NoError(t, err)
	assert.Equal(t, 10000, cfg.Users)
	assert.Equal(t, []float64{0.10, 0.25, 0.50, 1.0}, cfg.StormSteps)
	assert.Equal(t, "graceful", cfg.Mode)
}

func TestParseStormConfig_Parsed(t *testing.T) {
	cfg, err := parseStormConfig([]string{"--users=500", "--storm-steps=0.5,1.0", "--storm-mode=silent"})
	require.NoError(t, err)
	assert.Equal(t, 500, cfg.Users)
	assert.Equal(t, []float64{0.5, 1.0}, cfg.StormSteps)
	assert.Equal(t, "silent", cfg.Mode)
}

func TestParseStormConfig_RejectsBadFraction(t *testing.T) {
	_, err := parseStormConfig([]string{"--storm-steps=1.5"})
	assert.Error(t, err)
}

func TestParseStormConfig_RejectsBadMode(t *testing.T) {
	_, err := parseStormConfig([]string{"--storm-mode=nope"})
	assert.Error(t, err)
}

func TestStormUserCount(t *testing.T) {
	assert.Equal(t, 250, stormUserCount(0.25, 1000))
	assert.Equal(t, 1000, stormUserCount(1.0, 1000))
	assert.Equal(t, 1, stormUserCount(0.0001, 1000)) // at least 1 when fraction>0
}

func TestParseFractionList(t *testing.T) {
	got, err := parseFractionList("0.1,0.25,1.0")
	require.NoError(t, err)
	assert.Equal(t, []float64{0.1, 0.25, 1.0}, got)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/user/chat && go test ./tools/loadgen/ -run 'TestParseStormConfig|TestStormUserCount|TestParseFractionList' -v`
Expected: FAIL — `undefined: parseStormConfig`

- [ ] **Step 3: Write minimal implementation**

```go
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type stormConfig struct {
	Users          int
	StormSteps     []float64
	Mode           string // "graceful" | "silent"
	Warmup         time.Duration
	Settle         time.Duration // cooldown between storm steps
	RecoverySLO    time.Duration
	Heartbeat      time.Duration
	SilentWait     time.Duration // wait for sweeper offline in silent mode
	ReconnectRate  int           // hellos/sec during the herd; 0 = unbounded burst
	StopOnTrip     bool
	PublisherConns int
	ObserverConns  int
	CSVPath        string
	P99Ms          float64
	ErrorRate      float64
}

func parseFractionList(s string) ([]float64, error) {
	if strings.TrimSpace(s) == "" {
		return nil, fmt.Errorf("--storm-steps cannot be empty")
	}
	var out []float64
	for _, p := range strings.Split(s, ",") {
		f, err := strconv.ParseFloat(strings.TrimSpace(p), 64)
		if err != nil {
			return nil, fmt.Errorf("invalid fraction %q: %w", p, err)
		}
		if f <= 0 || f > 1 {
			return nil, fmt.Errorf("fraction %v out of range (0,1]", f)
		}
		out = append(out, f)
	}
	return out, nil
}

func stormUserCount(fraction float64, n int) int {
	c := int(math.Floor(fraction * float64(n)))
	if c < 1 && fraction > 0 {
		c = 1
	}
	if c > n {
		c = n
	}
	return c
}

func parseStormConfig(args []string) (stormConfig, error) {
	fs := flag.NewFlagSet("presence-storm", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), `loadgen presence-storm — find the largest survivable reconnect storm

Warms --users online, then for each fraction in --storm-steps drops that
fraction and thundering-herd reconnects them, measuring recovery time, spike
latency, and error rate. Reports the largest fraction that recovered within
--recovery-slo.

Flags:
`)
		fs.PrintDefaults()
	}
	users := fs.Int("users", 10000, "fixed warmed population N")
	steps := fs.String("storm-steps", "0.10,0.25,0.50,1.0", "comma-separated storm fractions in (0,1]")
	mode := fs.String("storm-mode", "graceful", "graceful (bye+hello) | silent (stop ping, sweeper offline, hello)")
	warmup := fs.Duration("warmup", 30*time.Second, "warm-up before the first storm")
	settle := fs.Duration("settle", 15*time.Second, "cooldown between storm steps")
	slo := fs.Duration("recovery-slo", 10*time.Second, "max recovery time to PASS a storm")
	heartbeat := fs.Duration("heartbeat", 30*time.Second, "warmed-user ping interval")
	silentWait := fs.Duration("silent-wait", 50*time.Second, "silent mode: wait for sweeper offline (> stale threshold)")
	reconnect := fs.Int("reconnect-rate", 0, "hellos/sec during the herd (0 = unbounded burst)")
	stop := fs.Bool("stop-on-trip", true, "stop ramping fractions on the first TRIP")
	pub := fs.Int("publisher-conns", 16, "shared publisher connection count")
	obs := fs.Int("observer-conns", 4, "observer connection count")
	csv := fs.String("csv", "", "optional CSV output path")
	p99 := fs.Float64("p99-ms", 1000, "spike p99 latency cap (ms)")
	errRate := fs.Float64("error-rate", 0.05, "spike error-rate cap (fraction)")
	if err := fs.Parse(args); err != nil {
		return stormConfig{}, err
	}
	fractions, err := parseFractionList(*steps)
	if err != nil {
		return stormConfig{}, err
	}
	if *mode != "graceful" && *mode != "silent" {
		return stormConfig{}, fmt.Errorf("invalid --storm-mode %q (want graceful|silent)", *mode)
	}
	return stormConfig{
		Users: *users, StormSteps: fractions, Mode: *mode,
		Warmup: *warmup, Settle: *settle, RecoverySLO: *slo, Heartbeat: *heartbeat,
		SilentWait: *silentWait, ReconnectRate: *reconnect, StopOnTrip: *stop,
		PublisherConns: *pub, ObserverConns: *obs, CSVPath: *csv,
		P99Ms: *p99, ErrorRate: *errRate,
	}, nil
}

// stormConn is one warmed user whose steady-ping goroutine can be paused
// (silent drop) and resumed.
type stormConn struct {
	user   *presenceUser
	paused atomic.Bool
}

// stormEnv bundles a storm run's deps. The runStorm seam lets unit tests drive
// the ramp loop without a broker.
type stormEnv struct {
	pool       *presencePool
	collector  *presenceCollector
	conns      []*stormConn
	thresholds stormThresholds
	cfg        stormConfig
	siteID     string

	// runStorm executes one fraction step and returns its inputs. Prod uses
	// the real implementation; tests inject a stub.
	runStorm func(ctx context.Context, env *stormEnv, fraction float64) stormStepInputs
}

type stormFactory interface {
	Build(cfg stormConfig) *stormEnv
}

//nolint:gocritic // cfg by value to satisfy interface
func runPresenceStormForTest(ctx context.Context, cfg stormConfig, f stormFactory) ([]stormStepResult, error) {
	if len(cfg.StormSteps) == 0 {
		return nil, fmt.Errorf("cfg.StormSteps cannot be empty")
	}
	env := f.Build(cfg)
	if env.pool != nil {
		defer env.pool.Close()
	}
	var results []stormStepResult
	for _, frac := range cfg.StormSteps {
		if err := ctx.Err(); err != nil {
			break
		}
		in := env.runStorm(ctx, env, frac)
		in.Self = snapshotSelfMetrics()
		r := evaluateStormStep(in, env.thresholds)
		results = append(results, r)
		if cfg.StopOnTrip && r.Kind == verdictTrip {
			break
		}
		waitOrCancel(ctx, cfg.Settle)
	}
	return results, nil
}

// executeStorm is the real one-fraction storm: drop, herd, measure recovery.
func executeStorm(ctx context.Context, env *stormEnv, fraction float64) stormStepInputs {
	count := stormUserCount(fraction, len(env.conns))
	victims := env.conns[:count]
	env.collector.Reset()

	// Drop.
	switch env.cfg.Mode {
	case "silent":
		for _, sc := range victims {
			sc.paused.Store(true)
		}
		waitOrCancel(ctx, env.cfg.SilentWait) // sweeper marks them offline
	default: // graceful
		for _, sc := range victims {
			emitTransitionRaw(env.pool, env.collector, sc.user.bye(nowMillis()))
		}
	}

	// Arm recovery and thundering-herd hello.
	accounts := make([]string, count)
	for i, sc := range victims {
		accounts[i] = sc.user.account
	}
	start := time.Now()
	env.collector.BeginRecovery(accounts, start)
	herdHello(ctx, env, victims)

	// Wait for recovery or SLO deadline.
	deadline := time.NewTimer(env.cfg.RecoverySLO)
	defer deadline.Stop()
	poll := time.NewTicker(50 * time.Millisecond)
	defer poll.Stop()
	for !env.collector.RecoveryComplete() {
		select {
		case <-ctx.Done():
			goto done
		case <-deadline.C:
			goto done
		case <-poll.C:
		}
	}
done:
	for _, sc := range victims {
		sc.paused.Store(false) // resume steady ping
	}
	return stormStepInputs{
		Fraction: fraction, StormUsers: count,
		RecoveryComplete: env.collector.RecoveryComplete(),
		RecoveryElapsed:  env.collector.RecoveryElapsed(),
		RecoveryRemaining: env.collector.RecoveryRemaining(),
		SpikeLatencyMs:   env.collector.LatenciesMs(),
		Attempted:        env.collector.Attempted(),
		Failed:           env.collector.Failed(),
	}
}

// herdHello re-hellos every victim, optionally paced at cfg.ReconnectRate/sec.
func herdHello(ctx context.Context, env *stormEnv, victims []*stormConn) {
	if env.cfg.ReconnectRate <= 0 {
		var wg sync.WaitGroup
		for _, sc := range victims {
			wg.Add(1)
			go func(sc *stormConn) {
				defer wg.Done()
				emitTransitionRaw(env.pool, env.collector, sc.user.hello(nowMillis()))
			}(sc)
		}
		wg.Wait()
		return
	}
	tick := time.NewTicker(time.Second / time.Duration(env.cfg.ReconnectRate))
	defer tick.Stop()
	for _, sc := range victims {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		emitTransitionRaw(env.pool, env.collector, sc.user.hello(nowMillis()))
	}
}

// emitTransitionRaw is the pool-backed emit used by storm (no test seam).
func emitTransitionRaw(pool *presencePool, c *presenceCollector, tr presenceTransition) {
	sentAt := time.Now()
	err := pool.Publish(tr.subject, tr.payload)
	if tr.expect == "" {
		return
	}
	if err != nil {
		c.RecordEmit()
		c.RecordEmitFailure()
		return
	}
	c.Expect(accountFromSubject(tr.subject), tr.expect, sentAt)
}

// prodStormFactory wires the real pool and warms the population.
type prodStormFactory struct{ baseCfg *config }

//nolint:gocritic // cfg by value to satisfy interface
func (f *prodStormFactory) Build(cfg stormConfig) *stormEnv {
	c := newPresenceCollector()
	siteID := f.baseCfg.SiteID
	if siteID == "" {
		siteID = "site-local"
	}
	pool, err := newPresencePool(f.baseCfg.NatsURL, f.baseCfg.NatsCredsFile, cfg.PublisherConns, cfg.ObserverConns, c)
	if err != nil {
		slog.Error("presence pool init failed", "err", err)
	}
	conns := make([]*stormConn, cfg.Users)
	for i := range conns {
		conns[i] = &stormConn{user: newPresenceUser(i, siteID)}
	}
	return &stormEnv{
		pool: pool, collector: c, conns: conns,
		thresholds: stormThresholds{RecoverySLO: cfg.RecoverySLO, P99Ms: cfg.P99Ms, ErrorRate: cfg.ErrorRate, GCPauseInconclusive: 50},
		cfg: cfg, siteID: siteID, runStorm: executeStorm,
	}
}

// runPresenceStorm is the production entrypoint.
func runPresenceStorm(ctx context.Context, baseCfg *config, args []string) int {
	cfg, err := parseStormConfig(args)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		slog.Error("parse presence-storm config", "error", err)
		return 2
	}
	factory := &prodStormFactory{baseCfg: baseCfg}
	env := factory.Build(cfg)
	// Warm the population (hello + steady ping) before the first storm.
	warmStormPopulation(ctx, env)
	waitOrCancel(ctx, cfg.Warmup)

	results, err := runStormSteps(ctx, cfg, env)
	if env.pool != nil {
		env.pool.Close()
	}
	if err != nil {
		slog.Error("presence-storm run", "error", err)
		return 1
	}
	renderStormConsole(os.Stdout, results)
	if cfg.CSVPath != "" {
		if err := writeStormCSV(cfg.CSVPath, results); err != nil {
			slog.Error("write storm csv", "error", err)
			return 1
		}
	}
	return presenceStormExitCode(results)
}

// runStormSteps drives the fraction ramp against an already-warmed env.
func runStormSteps(ctx context.Context, cfg stormConfig, env *stormEnv) ([]stormStepResult, error) {
	var results []stormStepResult
	for _, frac := range cfg.StormSteps {
		if err := ctx.Err(); err != nil {
			break
		}
		in := env.runStorm(ctx, env, frac)
		in.Self = snapshotSelfMetrics()
		r := evaluateStormStep(in, env.thresholds)
		results = append(results, r)
		if cfg.StopOnTrip && r.Kind == verdictTrip {
			break
		}
		waitOrCancel(ctx, cfg.Settle)
	}
	return results, nil
}

// warmStormPopulation hello's every conn and starts its pausable steady-ping
// goroutine.
func warmStormPopulation(ctx context.Context, env *stormEnv) {
	if env.pool == nil {
		return
	}
	for _, sc := range env.conns {
		emitTransitionRaw(env.pool, env.collector, sc.user.hello(nowMillis()))
		go func(sc *stormConn) {
			tick := time.NewTicker(env.cfg.Heartbeat)
			defer tick.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
				}
				if sc.paused.Load() {
					continue
				}
				_ = env.pool.Publish(presencePingSubject(sc.user.account, sc.user.siteID),
					mustMarshal(pingPayload(sc.user.connID)))
			}
		}(sc)
	}
}

func presenceStormExitCode(results []stormStepResult) int {
	for i := range results {
		if results[i].Kind == verdictPass {
			return 0
		}
	}
	return 1
}

var _ = atomic.Bool{}
```

> Add a tiny helper `pingPayload` to `presence_user.go` to avoid importing `model` here, OR inline `model.Ping{...}` and import `model`. Simplest: in `presence_user.go` add:
> ```go
> func pingPayload(connID string) model.Ping { return model.Ping{ConnID: connID, Timestamp: nowMillis()} }
> ```
> Remove the trailing `var _ = atomic.Bool{}` once `atomic` is otherwise referenced (it is, via `stormConn.paused`).

- [ ] **Step 4: Add a stub-factory test for the ramp loop**

Append to `presence_storm_test.go`:

```go
import "context"

type stubStormFactory struct{}

func (stubStormFactory) Build(cfg stormConfig) *stormEnv {
	return &stormEnv{
		collector:  newPresenceCollector(),
		thresholds: defaultStormThresholds(),
		cfg:        cfg,
		runStorm: func(ctx context.Context, env *stormEnv, frac float64) stormStepInputs {
			// Healthy below 1.0, slow recovery at 1.0.
			rec := 2 * time.Second
			if frac >= 1.0 {
				rec = 30 * time.Second
			}
			return stormStepInputs{
				Fraction: frac, StormUsers: int(frac * 1000),
				RecoveryComplete: true, RecoveryElapsed: rec,
				SpikeLatencyMs: []float64{100}, Attempted: 1000, Failed: 0,
			}
		},
	}
}

func TestRunPresenceStorm_StubRamp(t *testing.T) {
	cfg := stormConfig{StormSteps: []float64{0.5, 1.0}, StopOnTrip: true, Settle: 0}
	results, err := runPresenceStormForTest(context.Background(), cfg, stubStormFactory{})
	require.NoError(t, err)
	require.Len(t, results, 2)
	assert.Equal(t, verdictPass, results[0].Kind)
	assert.Equal(t, verdictTrip, results[1].Kind)
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd /home/user/chat && go test ./tools/loadgen/ -run 'Storm' -v`
Expected: PASS (after Task 12 provides `renderStormConsole`/`writeStormCSV`, or stub them first — same ordering note as Task 6/7).

- [ ] **Step 6: Commit**

```bash
cd /home/user/chat
git add tools/loadgen/presence_storm.go tools/loadgen/presence_storm_test.go tools/loadgen/presence_user.go
git commit -m "feat(loadgen): presence-storm orchestration and config"
```

---

### Task 12: Storm reporting

**Files:**
- Modify: `tools/loadgen/presence_report.go` (append)
- Test: `tools/loadgen/presence_report_test.go` (append)

- [ ] **Step 1: Write the failing test (append)**

```go
func TestRenderStormConsole_Answer(t *testing.T) {
	results := []stormStepResult{
		{Fraction: 0.5, StormUsers: 500, RecoveryComplete: true, RecoveryMs: 3000, P99Ms: 200, Kind: verdictPass},
		{Fraction: 1.0, StormUsers: 1000, RecoveryComplete: true, RecoveryMs: 20000, P99Ms: 300, Kind: verdictTrip, Reasons: []string{"recovery=20s > 10s"}},
	}
	var buf bytes.Buffer
	renderStormConsole(&buf, results)
	out := buf.String()
	assert.Contains(t, out, "ANSWER: max survivable storm = 0.50")
	assert.Contains(t, out, "Next limit: recovery=20s > 10s")
}

func TestWriteStormCSV(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "storm.csv")
	require.NoError(t, writeStormCSV(path, []stormStepResult{
		{Fraction: 0.5, StormUsers: 500, RecoveryComplete: true, RecoveryMs: 3000, P99Ms: 200, ErrorRate: 0, Kind: verdictPass},
	}))
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	assert.Equal(t, "fraction,storm_users,recovery_complete,recovery_ms,p99_ms,error_rate,verdict,reasons", lines[0])
	assert.Contains(t, lines[1], "0.50")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/user/chat && go test ./tools/loadgen/ -run 'TestRenderStormConsole|TestWriteStormCSV' -v`
Expected: FAIL — `undefined: renderStormConsole`

- [ ] **Step 3: Write minimal implementation (append to presence_report.go)**

```go
func renderStormConsole(w io.Writer, results []stormStepResult) {
	fmt.Fprintln(w, "fraction users  recovery   p99    err%    verdict")
	var lastPass float64
	for i := range results {
		r := &results[i]
		if r.Kind == verdictPass {
			lastPass = r.Fraction
		}
		rec := fmt.Sprintf("%.0fms", r.RecoveryMs)
		if !r.RecoveryComplete {
			rec = "INCOMPLETE"
		}
		fmt.Fprintf(w, "%-8.2f %-6d %-10s %-6.0f %-7.3f%% %s\n",
			r.Fraction, r.StormUsers, rec, r.P99Ms, r.ErrorRate*100, r.Kind)
		if r.Kind != verdictPass && len(r.Reasons) > 0 {
			fmt.Fprintf(w, "    reasons: %s\n", joinReasons(r.Reasons))
		}
	}
	fmt.Fprintln(w)
	if lastPass > 0 {
		fmt.Fprintf(w, "ANSWER: max survivable storm = %.2f (largest passing fraction)\n", lastPass)
		for i := range results {
			if results[i].Kind == verdictTrip {
				fmt.Fprintf(w, "        Next limit: %s\n", joinReasons(results[i].Reasons))
				break
			}
		}
	} else {
		fmt.Fprintln(w, "ANSWER: no storm fraction survived")
	}
}

func writeStormCSV(path string, results []stormStepResult) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create csv: %w", err)
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	if err := w.Write([]string{"fraction", "storm_users", "recovery_complete", "recovery_ms", "p99_ms", "error_rate", "verdict", "reasons"}); err != nil {
		return fmt.Errorf("write csv header: %w", err)
	}
	for i := range results {
		r := &results[i]
		row := []string{
			fmt.Sprintf("%.2f", r.Fraction), strconv.Itoa(r.StormUsers),
			strconv.FormatBool(r.RecoveryComplete), fmt.Sprintf("%.0f", r.RecoveryMs),
			fmt.Sprintf("%.0f", r.P99Ms), fmt.Sprintf("%.6f", r.ErrorRate),
			r.Kind.String(), joinReasons(r.Reasons),
		}
		if err := w.Write(row); err != nil {
			return fmt.Errorf("write csv row: %w", err)
		}
	}
	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /home/user/chat && go test ./tools/loadgen/ -run 'TestRenderStormConsole|TestWriteStormCSV' -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /home/user/chat
git add tools/loadgen/presence_report.go tools/loadgen/presence_report_test.go
git commit -m "feat(loadgen): presence-storm console + CSV reporting"
```

---

### Task 13: Wire `presence-storm` into dispatch

**Files:**
- Modify: `tools/loadgen/main.go`

- [ ] **Step 1: Add the dispatch case** (after the `presence-sustained` case)

```go
	case "presence-storm":
		return runPresenceStorm(ctx, cfg, os.Args[2:])
```

- [ ] **Step 2: Update the usage string** to append `|presence-storm`.

- [ ] **Step 3: Build + full unit test + lint**

Run: `cd /home/user/chat && make build SERVICE=loadgen && go test -race ./tools/loadgen/ && make lint`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
cd /home/user/chat
git add tools/loadgen/main.go
git commit -m "feat(loadgen): wire presence-storm subcommand"
```

---

### Task 14: Integration test (storm)

**Files:**
- Modify: `tools/loadgen/presence_integration_test.go` (append)

- [ ] **Step 1: Write the integration test (append)**

```go
// TestPresenceStorm_EndToEnd warms a small population, drops+reconnects all of
// them, and asserts recovery completes within a generous SLO.
func TestPresenceStorm_EndToEnd(t *testing.T) {
	natsURL := testutil.NATS(t)
	startPresenceServiceForTest(t, natsURL)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cfg := stormConfig{
		Users: 50, StormSteps: []float64{0.5, 1.0}, Mode: "graceful",
		Warmup: 2 * time.Second, Settle: time.Second, RecoverySLO: 10 * time.Second,
		Heartbeat: time.Second, ReconnectRate: 0, StopOnTrip: false,
		PublisherConns: 2, ObserverConns: 1, P99Ms: 5000, ErrorRate: 0.5,
	}
	baseCfg := &config{NatsURL: natsURL, SiteID: "site-local"}
	env := (&prodStormFactory{baseCfg: baseCfg}).Build(cfg)
	warmStormPopulation(ctx, env)
	time.Sleep(2 * time.Second)
	results, err := runStormSteps(ctx, cfg, env)
	env.pool.Close()
	require.NoError(t, err)
	require.Len(t, results, 2)
	for _, r := range results {
		assert.True(t, r.RecoveryComplete, "fraction %.2f should recover; reasons %v", r.Fraction, r.Reasons)
	}
}
```

> `time.Sleep` here is test-only orchestration (waiting for warm-up to settle before storming), not production goroutine synchronization — acceptable in an integration test. If the linter objects, replace with a poll on `env.collector` observing N online states.

- [ ] **Step 2: Run the integration test**

Run: `cd /home/user/chat && go test -race -tags integration ./tools/loadgen/ -run 'TestPresenceStorm_EndToEnd' -v`
Expected: PASS. Requires Docker.

- [ ] **Step 3: Commit**

```bash
cd /home/user/chat
git add tools/loadgen/presence_integration_test.go
git commit -m "test(loadgen): presence-storm integration test"
```

---

## Phase 3 — Docs

### Task 15: README

**Files:**
- Modify: `tools/loadgen/README.md`

No `docs/client-api.md` change is required: the loadgen adds no client-facing handler and changes no presence wire contract.

- [ ] **Step 1: Add a "Presence" section to the README**

Document both subcommands with their purpose, a one-line invocation example each, the SLO signals (sustained: state-publish p95/p99, error rate, GC self-saturation INCONCLUSIVE; storm: recovery time vs SLO, spike p99, spike error rate), the two storm modes (`graceful` vs `silent`), and the no-seeding note (synthetic accounts; presence trusts the JWT self-token).

Example block to include:

```markdown
## Presence

Drives `user-presence-service` over NATS. No seeding — synthetic accounts
(`u-NNNNNN`) work because the service trusts the JWT self-token on
hello/ping/activity/bye.

### presence-sustained — max sustainable population N

    loadgen presence-sustained --steps=1k,2k,5k,10k --hold=120s --csv=presence.csv

Ramps N; per step grades state-publish latency (p95/p99), error rate (missing
observations + publish failures), with a loadgen self-saturation INCONCLUSIVE
guard. Reports the largest passing N.

### presence-storm — largest survivable reconnect storm

    loadgen presence-storm --users=20000 --storm-steps=0.1,0.25,0.5,1.0 --storm-mode=graceful

At fixed N, ramps the dropped-and-reconnected fraction. graceful = bye+hello;
silent = stop pinging until the sweeper marks offline, then hello (gateway-blip
realistic). Grades recovery time vs --recovery-slo, spike p99, and error rate;
reports the largest fraction that recovered in SLO.
```

- [ ] **Step 2: Commit**

```bash
cd /home/user/chat
git add tools/loadgen/README.md
git commit -m "docs(loadgen): document presence-sustained and presence-storm"
```

---

## Final verification (before pushing)

- [ ] `cd /home/user/chat && make fmt && make lint` — clean.
- [ ] `cd /home/user/chat && go test -race ./tools/loadgen/` — all unit tests pass.
- [ ] `cd /home/user/chat && go test -race -tags integration ./tools/loadgen/ -run Presence` — integration tests pass (Docker required).
- [ ] `cd /home/user/chat && make sast` — no new medium+ findings.
- [ ] Push: `git push -u origin claude/user-presence-loadgen-nk1ni7` (retry with backoff on network error). Do NOT open a PR unless asked.

---

## Self-Review Notes

- **Spec coverage:** sustained-mode signals (state-publish latency p95/p99, error rate, GC self-saturation guard) → Task 4; storm = max survivable fraction → Task 10; serialized-per-user 1:1 correlation → Tasks 2–3 (`Expect`/`Observe`); graceful vs silent → Task 11; no-seeding synthetic accounts → Task 2 (`presenceAccount`). All approved decisions are covered.
- **Type consistency:** `presenceCollector`, `presenceUser`, `presenceTransition`, `presenceEnv`, `stormEnv`, `presenceStepResult`, `stormStepResult` are used consistently across tasks. `joinReasons` and `parseStepList` are reused from existing daily files (do not redefine). `percentile`, `verdictKind`, `SelfMetrics`, `snapshotSelfMetrics`, `connectWithCreds`, `config` are reused in-package.
- **Known cross-task compile coupling:** `presence.go` (Task 6) and `presence_storm.go` (Task 11) reference report funcs from Tasks 7 and 12. Implement each phase's report task alongside its run-loop task before running the full-package build; commits stay separate but the working tree must contain both to compile (flagged in those tasks).
- **No outstanding placeholders or naming inconsistencies** after the self-review pass.
