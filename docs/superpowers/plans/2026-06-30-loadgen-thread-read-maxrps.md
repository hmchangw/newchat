# Thread-Read Max-RPS Loadgen Workload Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a focused `thread-read` workload to `tools/loadgen` that finds the max sustainable RPS for loading thread messages (`history-service.GetThreadMessages`).

**Architecture:** A new `rpsWorkload` adapter (collector + generator + adapter) mirroring the existing `room-read` single-latency-series read workload, reusing the history presets, fixtures, seed, and NATS requester. First-page-only reads; gated on one `"thread-read"` latency series + error rate; no consumer-pending signal.

**Tech Stack:** Go 1.25, NATS request/reply (`pkg/subject.MsgThread`), `math/rand` Zipf room selection, the shared loadgen pacer/ramp/verdict harness, `testify` for assertions, `go.uber.org/mock` not needed (hand-written fake requester).

## Global Constraints

- All work lives in `tools/loadgen/` (flat `package main`). No new third-party deps.
- Use `make` targets, never raw `go` (e.g. `make test SERVICE=loadgen` is not valid — loadgen is a tool; use `make -C` is also not it). The loadgen package is tested with `go test ./tools/loadgen/...` under the repo's Makefile `test` target. For a single package during development run `go test ./tools/loadgen/ -run <name>` (the Makefile wraps `-race`; keep `-race` in mind). Integration tests use the `integration` build tag.
- Reuse the package-shared `errClass` consts (`errClassTimeout`, `errClassReply`, `errClassBadReply`) — do not invent new error classes.
- Reuse the existing `HistoryRequester` interface and `newNATSHistoryRequester` — do not add a third identical requester interface.
- Reuse the existing `getThreadMessagesRequest` struct (defined in `history_generator.go`) for the request body — do not redefine it.
- New types are unexported (`threadReadCollector`, `threadReadGenerator`, `threadReadWorkload`, `threadReadSample`) per CLAUDE.md "export only what other packages consume" — everything here is package-internal.
- Zipf params match the sibling workloads: `rand.NewZipf(r, 1.1, 1.0, n)`.
- Commit after every task with the trailers:
  ```
  Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01R44pRsRkU6XDV7dne1Eudv
  ```
- Commits must be authored by `Claude <noreply@anthropic.com>` (already configured via `git config user.email/user.name`).

---

## File Structure

- Create `tools/loadgen/threadread_collector.go` — `threadReadCollector` + `threadReadSample`.
- Create `tools/loadgen/threadread_collector_test.go` — collector unit tests.
- Create `tools/loadgen/threadread_generator.go` — `threadReadGenerator` + `threadReadReply` + config.
- Create `tools/loadgen/threadread_generator_test.go` — generator unit tests (hand-written fake requester).
- Create `tools/loadgen/maxrps_threadread.go` — `threadReadWorkload` adapter + `buildThreadReadInputs` + `runThreadReadFor`.
- Create `tools/loadgen/maxrps_threadread_test.go` — adapter/inputs/Label/defaultSteps tests.
- Create `tools/loadgen/threadread_integration_test.go` — `//go:build integration` end-to-end against a NATS stub.
- Modify `tools/loadgen/maxrps.go` — `defaultSteps` read branch + `runMaxRPS` `thread-read` case + `--workload` usage string.
- Modify `tools/loadgen/main.go` — `seed`/`teardown` `thread-read` cases + usage strings.
- Modify `tools/loadgen/README.md` — new "Thread-read workload" section + `max-rps` references.

---

### Task 1: Thread-read collector

**Files:**
- Create: `tools/loadgen/threadread_collector.go`
- Test: `tools/loadgen/threadread_collector_test.go`

**Interfaces:**
- Consumes: package-shared `errClass` consts from `history_collector.go`.
- Produces:
  - `type threadReadSample struct { Latency time.Duration; At time.Time }`
  - `func newThreadReadCollector() *threadReadCollector`
  - Methods: `RecordSample(threadReadSample)`, `RecordError(errClass, time.Duration)`, `RecordBadReply(time.Duration)`, `RecordSaturation()`, `RecordUnderrun(int)`, `RecordNoParents()`, `Samples() []threadReadSample`, `TimeoutErrors() int`, `ReplyErrors() int`, `BadReplyCount() int`, `SaturationCount() int`, `UnderrunCount() int`, `NoParentsCount() int`.

- [ ] **Step 1: Write the failing test**

Create `tools/loadgen/threadread_collector_test.go`:

```go
package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestThreadReadCollector_RecordsSamplesAndErrors(t *testing.T) {
	c := newThreadReadCollector()
	c.RecordSample(threadReadSample{Latency: 3 * time.Millisecond, At: time.Now()})
	c.RecordSample(threadReadSample{Latency: 7 * time.Millisecond, At: time.Now()})
	c.RecordError(errClassTimeout, time.Millisecond)
	c.RecordError(errClassReply, time.Millisecond)
	c.RecordBadReply(time.Millisecond)
	c.RecordSaturation()
	c.RecordUnderrun(4)
	c.RecordUnderrun(0) // no-op
	c.RecordNoParents()

	assert.Len(t, c.Samples(), 2)
	assert.Equal(t, 1, c.TimeoutErrors())
	assert.Equal(t, 1, c.ReplyErrors())
	assert.Equal(t, 1, c.BadReplyCount())
	assert.Equal(t, 1, c.SaturationCount())
	assert.Equal(t, 4, c.UnderrunCount())
	assert.Equal(t, 1, c.NoParentsCount())
}

func TestThreadReadCollector_SamplesReturnsCopy(t *testing.T) {
	c := newThreadReadCollector()
	c.RecordSample(threadReadSample{Latency: time.Millisecond, At: time.Now()})
	got := c.Samples()
	got[0].Latency = 999 * time.Second // mutate the copy
	assert.Equal(t, time.Millisecond, c.Samples()[0].Latency, "Samples must return a defensive copy")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./tools/loadgen/ -run TestThreadReadCollector`
Expected: FAIL — `undefined: newThreadReadCollector` / `threadReadSample`.

- [ ] **Step 3: Write minimal implementation**

Create `tools/loadgen/threadread_collector.go`:

```go
package main

import (
	"sync"
	"time"
)

// threadReadSample captures one completed GetThreadMessages round-trip.
type threadReadSample struct {
	Latency time.Duration
	At      time.Time
}

// threadReadCollector aggregates samples and errors across a workload run.
// All methods are safe for concurrent use. Reuses the package-shared errClass
// consts (errClassTimeout / errClassReply / errClassBadReply).
type threadReadCollector struct {
	mu         sync.Mutex
	samples    []threadReadSample
	errors     map[errClass]int
	saturation int
	underrun   int
	noParents  int
}

// newThreadReadCollector returns an empty collector.
func newThreadReadCollector() *threadReadCollector {
	return &threadReadCollector{errors: map[errClass]int{}}
}

// RecordSample stores one completed-call sample.
func (c *threadReadCollector) RecordSample(s threadReadSample) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.samples = append(c.samples, s)
}

// RecordError tallies a per-class transport/reply error.
func (c *threadReadCollector) RecordError(class errClass, _ time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errors[class]++
}

// RecordBadReply tallies a reply that was undecodable or missing parentMessage.
func (c *threadReadCollector) RecordBadReply(_ time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errors[errClassBadReply]++
}

// RecordSaturation tallies a tick that fired while the in-flight pool was full.
func (c *threadReadCollector) RecordSaturation() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.saturation++
}

// RecordUnderrun adds n events the pacer could not release on schedule. n<=0 is a no-op.
func (c *threadReadCollector) RecordUnderrun(n int) {
	if n <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.underrun += n
}

// RecordNoParents tallies a request that landed on a room with no seeded thread
// parents and was skipped. Informational — not counted as a failure.
func (c *threadReadCollector) RecordNoParents() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.noParents++
}

// Samples returns a defensive copy of the sample tape.
func (c *threadReadCollector) Samples() []threadReadSample {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]threadReadSample, len(c.samples))
	copy(out, c.samples)
	return out
}

// TimeoutErrors returns the timeout-class error count.
func (c *threadReadCollector) TimeoutErrors() int { return c.errCount(errClassTimeout) }

// ReplyErrors returns the reply-class error count.
func (c *threadReadCollector) ReplyErrors() int { return c.errCount(errClassReply) }

// BadReplyCount returns the count of undecodable / missing-parent replies.
func (c *threadReadCollector) BadReplyCount() int { return c.errCount(errClassBadReply) }

func (c *threadReadCollector) errCount(class errClass) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.errors[class]
}

// SaturationCount returns the count of saturation events.
func (c *threadReadCollector) SaturationCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.saturation
}

// UnderrunCount returns the total emit-underrun events.
func (c *threadReadCollector) UnderrunCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.underrun
}

// NoParentsCount returns the count of no-parent skips.
func (c *threadReadCollector) NoParentsCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.noParents
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./tools/loadgen/ -run TestThreadReadCollector -v`
Expected: PASS (both tests).

- [ ] **Step 5: Commit**

```bash
git add tools/loadgen/threadread_collector.go tools/loadgen/threadread_collector_test.go
git commit -m "loadgen: add thread-read collector

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01R44pRsRkU6XDV7dne1Eudv"
```

---

### Task 2: Thread-read generator

**Files:**
- Create: `tools/loadgen/threadread_generator.go`
- Test: `tools/loadgen/threadread_generator_test.go`

**Interfaces:**
- Consumes: `threadReadCollector` (Task 1); `HistoryRequester`, `getThreadMessagesRequest`, `classifyRequesterError` (from `history_generator.go`); `HistoryFixtures`, `ThreadParentRef` (from `history.go`); `serialDispatch`/`pacedDispatch` (from `pacer.go`); `subject.MsgThread`, `idgen.GenerateRequestID`, `natsutil.WithRequestID`.
- Produces:
  - `type threadReadGeneratorConfig struct { Fixtures *HistoryFixtures; SiteID string; Rate int; PageLimit int; RequestTimeout time.Duration; Requester HistoryRequester; Collector *threadReadCollector; MaxInFlight int }`
  - `func newThreadReadGenerator(cfg *threadReadGeneratorConfig, seed int64) *threadReadGenerator`
  - `func (g *threadReadGenerator) Run(ctx context.Context) error`

- [ ] **Step 1: Write the failing test**

Create `tools/loadgen/threadread_generator_test.go`:

```go
package main

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/natsutil"
)

// fakeThreadReadRequester records every (subject, body, request-ID) it is asked
// to request and returns a configurable reply/error.
type fakeThreadReadRequester struct {
	mu       sync.Mutex
	subjects []string
	bodies   [][]byte
	reqIDs   []string
	reply    []byte
	err      error
}

func (f *fakeThreadReadRequester) Request(ctx context.Context, subj string, data []byte, _ time.Duration) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.subjects = append(f.subjects, subj)
	f.bodies = append(f.bodies, data)
	f.reqIDs = append(f.reqIDs, natsutil.RequestIDFromContext(ctx))
	if f.err != nil {
		return nil, f.err
	}
	return f.reply, nil
}

func (f *fakeThreadReadRequester) snapshot() (subs []string, bodies [][]byte, ids []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	subs = append(subs, f.subjects...)
	bodies = append(bodies, f.bodies...)
	ids = append(ids, f.reqIDs...)
	return
}

// threadTestPreset is a tiny deterministic history preset that guarantees thread
// parents in every room without the cost of the medium/large presets.
func threadTestPreset() HistoryPreset {
	return HistoryPreset{
		Name: "thread-test", Users: 12, Rooms: 3, BaselineSize: 6,
		MessagesPerRoom: 40, MessageSpanDays: 1, ThreadRate: 0.25,
		RepliesPerThread: 3, ContentBytes: 50,
	}
}

func newThreadReadTestGen(t *testing.T, req HistoryRequester, c *threadReadCollector) (*threadReadGenerator, HistoryFixtures) {
	t.Helper()
	p := threadTestPreset()
	f := BuildHistoryFixtures(&p, 42, "site-test", time.Now().UTC())
	gen := newThreadReadGenerator(&threadReadGeneratorConfig{
		Fixtures:       &f,
		SiteID:         "site-test",
		Rate:           200,
		PageLimit:      20,
		RequestTimeout: time.Second,
		Requester:      req,
		Collector:      c,
		MaxInFlight:    8,
	}, 42)
	return gen, f
}

func TestThreadReadGenerator_EmitsRealThreadRequests(t *testing.T) {
	req := &fakeThreadReadRequester{reply: []byte(`{"messages":[],"parentMessage":{"messageId":"x"},"hasNext":false}`)}
	c := newThreadReadCollector()
	gen, f := newThreadReadTestGen(t, req, c)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	require.NoError(t, gen.Run(ctx))

	subs, bodies, _ := req.snapshot()
	require.NotEmpty(t, subs, "generator issued no requests")
	for i, s := range subs {
		assert.True(t, strings.HasPrefix(s, "chat.user."), "unexpected subject %q", s)
		assert.True(t, strings.HasSuffix(s, ".msg.thread"), "unexpected subject %q", s)

		// Subject tokens: chat.user.{account}.request.room.{roomID}.{siteID}.msg.thread
		toks := strings.Split(s, ".")
		require.GreaterOrEqual(t, len(toks), 9, "subject %q has too few tokens", s)
		account, roomID := toks[2], toks[5]

		var body getThreadMessagesRequest
		require.NoError(t, json.Unmarshal(bodies[i], &body))
		assert.Equal(t, 20, body.Limit)

		// threadMessageId must be a real seeded parent of that room.
		parentIDs := map[string]bool{}
		for _, p := range f.ThreadParents[roomID] {
			parentIDs[p.MessageID] = true
		}
		assert.True(t, parentIDs[body.ThreadMessageID],
			"threadMessageId %q is not a seeded parent of room %q", body.ThreadMessageID, roomID)

		// account must be a subscriber of that room.
		isMember := false
		for j := range f.Fixtures.Subscriptions {
			sub := f.Fixtures.Subscriptions[j]
			if sub.RoomID == roomID && sub.User.Account == account {
				isMember = true
				break
			}
		}
		assert.True(t, isMember, "account %q is not a subscriber of room %q", account, roomID)
	}
	assert.NotEmpty(t, c.Samples(), "healthy replies should be recorded as samples")
}

func TestThreadReadGenerator_ErrorEnvelopeRecordedAsReplyError(t *testing.T) {
	req := &fakeThreadReadRequester{reply: []byte(`{"error":"room not found"}`)}
	c := newThreadReadCollector()
	gen, _ := newThreadReadTestGen(t, req, c)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	require.NoError(t, gen.Run(ctx))

	assert.Empty(t, c.Samples(), "error envelopes must not count as samples")
	assert.Greater(t, c.ReplyErrors(), 0, "error envelope must count as a reply error")
}

func TestThreadReadGenerator_MissingParentMessageIsBadReply(t *testing.T) {
	req := &fakeThreadReadRequester{reply: []byte(`{"messages":[]}`)}
	c := newThreadReadCollector()
	gen, _ := newThreadReadTestGen(t, req, c)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	require.NoError(t, gen.Run(ctx))

	assert.Empty(t, c.Samples())
	assert.Greater(t, c.BadReplyCount(), 0, "reply without parentMessage must count as bad reply")
}

func TestThreadReadGenerator_TimeoutRecorded(t *testing.T) {
	req := &fakeThreadReadRequester{err: context.DeadlineExceeded}
	c := newThreadReadCollector()
	gen, _ := newThreadReadTestGen(t, req, c)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	require.NoError(t, gen.Run(ctx))

	assert.Greater(t, c.TimeoutErrors(), 0, "DeadlineExceeded must count as a timeout")
}

func TestThreadReadGenerator_NoParentsSkippedAndCounted(t *testing.T) {
	// history-small has ThreadRate 0 -> no parents in any room.
	p, ok := BuiltinHistoryPreset("history-small")
	require.True(t, ok)
	f := BuildHistoryFixtures(&p, 42, "site-test", time.Now().UTC())
	req := &fakeThreadReadRequester{reply: []byte(`{"parentMessage":{"messageId":"x"}}`)}
	c := newThreadReadCollector()
	gen := newThreadReadGenerator(&threadReadGeneratorConfig{
		Fixtures: &f, SiteID: "site-test", Rate: 200, PageLimit: 20,
		RequestTimeout: time.Second, Requester: req, Collector: c, MaxInFlight: 8,
	}, 42)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	require.NoError(t, gen.Run(ctx))

	subs, _, _ := req.snapshot()
	assert.Empty(t, subs, "no parents means no requests should be issued")
	assert.Empty(t, c.Samples())
	assert.Greater(t, c.NoParentsCount(), 0, "no-parent rooms must be counted")
}

func TestThreadReadGenerator_RequiresPositiveRate(t *testing.T) {
	req := &fakeThreadReadRequester{reply: []byte(`{"parentMessage":{"messageId":"x"}}`)}
	c := newThreadReadCollector()
	gen, _ := newThreadReadTestGen(t, req, c)
	gen.cfg.Rate = 0
	assert.Error(t, gen.Run(context.Background()))
}

func TestThreadReadGenerator_CarriesFreshRequestID(t *testing.T) {
	req := &fakeThreadReadRequester{reply: []byte(`{"parentMessage":{"messageId":"x"}}`)}
	c := newThreadReadCollector()
	gen, _ := newThreadReadTestGen(t, req, c)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	require.NoError(t, gen.Run(ctx))

	_, _, ids := req.snapshot()
	require.NotEmpty(t, ids)
	seen := map[string]bool{}
	for _, id := range ids {
		assert.True(t, idgen.IsValidUUID(id), "request ID %q must be a valid UUID", id)
		assert.False(t, seen[id], "each request must mint a fresh X-Request-ID, got duplicate %q", id)
		seen[id] = true
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./tools/loadgen/ -run TestThreadReadGenerator`
Expected: FAIL — `undefined: newThreadReadGenerator` / `threadReadGeneratorConfig`.

- [ ] **Step 3: Write minimal implementation**

Create `tools/loadgen/threadread_generator.go`:

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
)

// threadReadReply is the minimal projection of the GetThreadMessages reply the
// generator needs to validate it: a non-empty top-level "error" is the errcode
// envelope (failure); a present "parentMessage" marks a healthy success. This is
// stricter than the history workload, which records any non-transport reply as a
// success sample.
type threadReadReply struct {
	Error         string          `json:"error"`
	ParentMessage json.RawMessage `json:"parentMessage"`
}

// threadReadCallerSet bundles a room's subscribers and its seeded thread parents.
type threadReadCallerSet struct {
	subscribers []model.Subscription
	parents     []ThreadParentRef
}

// threadReadGeneratorConfig bundles every dependency the generator needs.
type threadReadGeneratorConfig struct {
	Fixtures       *HistoryFixtures
	SiteID         string
	Rate           int
	PageLimit      int
	RequestTimeout time.Duration
	Requester      HistoryRequester
	Collector      *threadReadCollector
	MaxInFlight    int
}

// threadReadGenerator drives the open-loop GetThreadMessages request/reply loop.
// Rooms are picked with a Zipf skew (hot rooms read more often); a random
// subscriber of the chosen room is the caller, reading a random seeded thread.
type threadReadGenerator struct {
	cfg threadReadGeneratorConfig

	rngMu sync.Mutex
	rng   *rand.Rand
	zipf  *rand.Zipf

	roomLookup map[string]*threadReadCallerSet
}

// newThreadReadGenerator constructs a generator seeded from `seed`. Zipf params
// (s=1.1, v=1.0) match the history/room-read workloads for consistent skew.
func newThreadReadGenerator(cfg *threadReadGeneratorConfig, seed int64) *threadReadGenerator {
	r := rand.New(rand.NewSource(seed))
	rooms := cfg.Fixtures.Fixtures.Rooms
	roomCount := len(rooms)
	if roomCount < 1 {
		roomCount = 1
	}
	zipfN := uint64(roomCount - 1)
	if zipfN < 1 {
		zipfN = 1
	}
	z := rand.NewZipf(r, 1.1, 1.0, zipfN)

	lookup := make(map[string]*threadReadCallerSet, roomCount)
	for i := range rooms {
		lookup[rooms[i].ID] = &threadReadCallerSet{}
	}
	for i := range cfg.Fixtures.Fixtures.Subscriptions {
		s := &cfg.Fixtures.Fixtures.Subscriptions[i]
		if set, ok := lookup[s.RoomID]; ok {
			set.subscribers = append(set.subscribers, *s)
		}
	}
	for roomID, refs := range cfg.Fixtures.ThreadParents {
		if set, ok := lookup[roomID]; ok {
			set.parents = refs
		}
	}

	return &threadReadGenerator{cfg: *cfg, rng: r, zipf: z, roomLookup: lookup}
}

// Run drives the open-loop requester until ctx cancels. MaxInFlight>0 uses the
// batched pacer; MaxInFlight<=0 selects the legacy serial path (bisection only).
func (g *threadReadGenerator) Run(ctx context.Context) error {
	if g.cfg.Rate <= 0 {
		return fmt.Errorf("rate must be > 0")
	}
	if g.cfg.MaxInFlight <= 0 {
		serialDispatch(ctx, g.cfg.Rate, g.requestOne)
		return nil
	}
	pacedDispatch(ctx, g.cfg.Rate, g.cfg.MaxInFlight,
		g.cfg.Collector.RecordUnderrun, g.cfg.Collector.RecordSaturation, g.requestOne)
	return nil
}

func (g *threadReadGenerator) requestOne(ctx context.Context) {
	roomID := g.pickRoom()
	if roomID == "" {
		return
	}
	set := g.roomLookup[roomID]
	if set == nil || len(set.subscribers) == 0 {
		return
	}
	if len(set.parents) == 0 {
		g.cfg.Collector.RecordNoParents()
		return
	}
	caller := set.subscribers[g.intn(len(set.subscribers))]
	parent := set.parents[g.intn(len(set.parents))]
	g.doThreadRead(ctx, roomID, caller.User.Account, parent.MessageID)
}

func (g *threadReadGenerator) doThreadRead(ctx context.Context, roomID, account, parentID string) {
	body := getThreadMessagesRequest{ThreadMessageID: parentID, Limit: g.cfg.PageLimit}
	data, err := json.Marshal(body)
	if err != nil {
		g.cfg.Collector.RecordBadReply(0)
		return
	}
	subj := subject.MsgThread(account, roomID, g.cfg.SiteID)
	// Mint a fresh X-Request-ID per request so server-side logs/traces for
	// benchmark traffic are correlatable.
	ctx = natsutil.WithRequestID(ctx, idgen.GenerateRequestID())
	start := time.Now()
	reply, err := g.cfg.Requester.Request(ctx, subj, data, g.cfg.RequestTimeout)
	latency := time.Since(start)

	if err != nil {
		// Run-level cancellation isn't a real failure — the run is draining.
		if ctx.Err() != nil {
			return
		}
		g.cfg.Collector.RecordError(classifyRequesterError(err), latency)
		return
	}

	var parsed threadReadReply
	if jerr := json.Unmarshal(reply, &parsed); jerr != nil {
		g.cfg.Collector.RecordBadReply(latency)
		return
	}
	if parsed.Error != "" {
		g.cfg.Collector.RecordError(errClassReply, latency)
		return
	}
	if len(parsed.ParentMessage) == 0 || string(parsed.ParentMessage) == "null" {
		g.cfg.Collector.RecordBadReply(latency)
		return
	}
	g.cfg.Collector.RecordSample(threadReadSample{Latency: latency, At: time.Now()})
}

func (g *threadReadGenerator) pickRoom() string {
	rooms := g.cfg.Fixtures.Fixtures.Rooms
	if len(rooms) == 0 {
		return ""
	}
	g.rngMu.Lock()
	idx := g.zipf.Uint64()
	g.rngMu.Unlock()
	if int(idx) >= len(rooms) {
		idx = uint64(len(rooms) - 1)
	}
	return rooms[idx].ID
}

func (g *threadReadGenerator) intn(n int) int {
	if n <= 0 {
		return 0
	}
	g.rngMu.Lock()
	defer g.rngMu.Unlock()
	return g.rng.Intn(n)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./tools/loadgen/ -run TestThreadReadGenerator -v`
Expected: PASS (all seven subtests).

- [ ] **Step 5: Commit**

```bash
git add tools/loadgen/threadread_generator.go tools/loadgen/threadread_generator_test.go
git commit -m "loadgen: add thread-read generator

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01R44pRsRkU6XDV7dne1Eudv"
```

---

### Task 3: Thread-read workload adapter

**Files:**
- Create: `tools/loadgen/maxrps_threadread.go`
- Test: `tools/loadgen/maxrps_threadread_test.go`

**Interfaces:**
- Consumes: `threadReadCollector`, `threadReadGenerator`, `threadReadGeneratorConfig` (Tasks 1-2); `rpsWorkload`, `rpsStepInputs`, `seriesSamples`, `waitOrCancel` (from `ramp.go`/`verdict.go`); `config`, `Metrics`, `NewMetrics` (from `main.go`/`metrics.go`); `HistoryPreset`, `BuildHistoryFixtures` (from `history.go`); `HistoryRequester`, `newNATSHistoryRequester` (from `history_generator.go`/`history_main.go`); `natsutil.Connect`.
- Produces:
  - `func buildThreadReadInputs(targetRPS int, hold time.Duration, c *threadReadCollector) rpsStepInputs`
  - `type threadReadWorkload struct { ... }` implementing `rpsWorkload`
  - `func newThreadReadWorkload(ctx context.Context, cfg *config, preset *HistoryPreset, seed int64, pageLimit int, requestTimeout time.Duration) (*threadReadWorkload, func(), error)`

- [ ] **Step 1: Write the failing test**

Create `tools/loadgen/maxrps_threadread_test.go`:

```go
package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Compile-time check: threadReadWorkload satisfies rpsWorkload.
var _ rpsWorkload = (*threadReadWorkload)(nil)

func TestBuildThreadReadInputs_MapsCollector(t *testing.T) {
	c := newThreadReadCollector()
	c.RecordSample(threadReadSample{Latency: 4 * time.Millisecond, At: time.Now()})
	c.RecordSample(threadReadSample{Latency: 6 * time.Millisecond, At: time.Now()})
	c.RecordError(errClassTimeout, time.Millisecond)
	c.RecordError(errClassReply, time.Millisecond)
	c.RecordBadReply(time.Millisecond)
	c.RecordSaturation()

	in := buildThreadReadInputs(1000, 30*time.Second, c)

	assert.Equal(t, 1000, in.TargetRPS)
	assert.Equal(t, 30*time.Second, in.Hold)
	assert.Equal(t, 3, in.FailedOps)    // 1 timeout + 1 reply + 1 bad reply
	assert.Equal(t, 5, in.AttemptedOps) // 2 samples + 3 failed
	assert.Equal(t, 1, in.Saturation)
	assert.Empty(t, in.Pending, "synchronous RPC has no pending durables")
	require.Len(t, in.Latencies, 1)
	assert.Equal(t, "thread-read", in.Latencies[0].Name)
	assert.Len(t, in.Latencies[0].Samples, 2)
}

func TestBuildThreadReadInputs_PopulatesEmitUnderrun(t *testing.T) {
	c := newThreadReadCollector()
	c.RecordUnderrun(7)
	c.RecordUnderrun(3)
	in := buildThreadReadInputs(2000, 30*time.Second, c)
	assert.Equal(t, 10, in.EmitUnderrun)
}

func TestThreadReadWorkload_Label(t *testing.T) {
	w := &threadReadWorkload{}
	assert.Equal(t, "thread-read", w.Label())
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./tools/loadgen/ -run 'TestBuildThreadReadInputs|TestThreadReadWorkload'`
Expected: FAIL — `undefined: buildThreadReadInputs` / `threadReadWorkload`.

- [ ] **Step 3: Write minimal implementation**

Create `tools/loadgen/maxrps_threadread.go`:

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

// threadReadLatencies extracts the latency tape from a sample slice.
func threadReadLatencies(samples []threadReadSample) []time.Duration {
	out := make([]time.Duration, len(samples))
	for i := range samples {
		out[i] = samples[i].Latency
	}
	return out
}

// buildThreadReadInputs assembles normalized step inputs from a (hold-only)
// collector. GetThreadMessages is synchronous request/reply, so there is no
// consumer queue and Pending stays empty; the single "thread-read" series gates.
func buildThreadReadInputs(targetRPS int, hold time.Duration, c *threadReadCollector) rpsStepInputs {
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
			{Name: "thread-read", Samples: threadReadLatencies(samples)},
		},
	}
}

// threadReadWorkload drives GetThreadMessages requests at a given RPS. As with
// the other read workloads the natsutil connection and metrics server are
// captured by the cleanup closure, not stored on the struct.
type threadReadWorkload struct {
	cfg            *config
	preset         *HistoryPreset
	fixtures       HistoryFixtures
	seed           int64
	pageLimit      int
	requestTimeout time.Duration
	metrics        *Metrics
	requester      HistoryRequester
}

func (w *threadReadWorkload) Label() string { return "thread-read" }

// newThreadReadWorkload connects NATS, starts the metrics server, and builds the
// history fixtures (which already carry ThreadParents). Requires CASSANDRA_HOSTS
// for parity with the history workload whose seed it depends on.
func newThreadReadWorkload(ctx context.Context, cfg *config, preset *HistoryPreset, seed int64, pageLimit int, requestTimeout time.Duration) (*threadReadWorkload, func(), error) {
	if cfg.CassandraHosts == "" {
		return nil, nil, fmt.Errorf("thread-read workload requires CASSANDRA_HOSTS")
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
	w := &threadReadWorkload{
		cfg:            cfg,
		preset:         preset,
		fixtures:       BuildHistoryFixtures(preset, seed, cfg.SiteID, time.Now().UTC()),
		seed:           seed,
		pageLimit:      pageLimit,
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

func (w *threadReadWorkload) newGenerator(collector *threadReadCollector, targetRPS int) *threadReadGenerator {
	return newThreadReadGenerator(&threadReadGeneratorConfig{
		Fixtures:       &w.fixtures,
		SiteID:         w.cfg.SiteID,
		Rate:           targetRPS,
		PageLimit:      w.pageLimit,
		RequestTimeout: w.requestTimeout,
		Requester:      w.requester,
		Collector:      collector,
		MaxInFlight:    w.cfg.MaxInFlight,
	}, w.seed)
}

// runThreadReadFor runs gen.Run for d (or until ctx cancels), then stops it and
// waits for in-flight requests to drain. Mirrors runRoomReadFor for the
// thread-read generator type.
func runThreadReadFor(ctx context.Context, gen *threadReadGenerator, d time.Duration) error {
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
func (w *threadReadWorkload) RunStep(ctx context.Context, targetRPS int, warmup, hold time.Duration) (rpsStepInputs, error) {
	if warmup > 0 {
		warmCollector := newThreadReadCollector()
		if err := runThreadReadFor(ctx, w.newGenerator(warmCollector, targetRPS), warmup); err != nil {
			return rpsStepInputs{}, err
		}
	}
	collector := newThreadReadCollector()
	if err := runThreadReadFor(ctx, w.newGenerator(collector, targetRPS), hold); err != nil {
		return rpsStepInputs{}, err
	}
	return buildThreadReadInputs(targetRPS, hold, collector), nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./tools/loadgen/ -run 'TestBuildThreadReadInputs|TestThreadReadWorkload' -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tools/loadgen/maxrps_threadread.go tools/loadgen/maxrps_threadread_test.go
git commit -m "loadgen: add thread-read workload adapter

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01R44pRsRkU6XDV7dne1Eudv"
```

---

### Task 4: CLI wiring (max-rps, seed, teardown)

**Files:**
- Modify: `tools/loadgen/maxrps.go` (`defaultSteps`, `runMaxRPS` switch, `--workload` usage)
- Modify: `tools/loadgen/main.go` (`runSeed` switch + usage, `runTeardown` switch + usage)
- Test: `tools/loadgen/maxrps_threadread_test.go` (append `defaultSteps` test)

**Interfaces:**
- Consumes: `newThreadReadWorkload` (Task 3); `BuiltinHistoryPreset`, `runSeedHistory`, `runTeardownHistory` (existing).
- Produces: `thread-read` routing in `runMaxRPS`, `runSeed`, `runTeardown`; `defaultSteps("thread-read") == "200,500,1000,2000,5000"`.

- [ ] **Step 1: Write the failing test**

Append to `tools/loadgen/maxrps_threadread_test.go`:

```go
func TestDefaultSteps_ThreadRead(t *testing.T) {
	assert.Equal(t, "200,500,1000,2000,5000", defaultSteps("thread-read"))
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./tools/loadgen/ -run TestDefaultSteps_ThreadRead`
Expected: FAIL — got `"500,1000,2000,5000,10000"` (the default branch) instead of the read-path steps.

- [ ] **Step 3: Write minimal implementation**

In `tools/loadgen/maxrps.go`, add `"thread-read"` to the read-path branch of `defaultSteps`:

```go
func defaultSteps(workload string) string {
	switch workload {
	case "history", "read-receipt", "room-read", "thread-read":
		return "200,500,1000,2000,5000"
	default:
		return "500,1000,2000,5000,10000"
	}
}
```

In `tools/loadgen/maxrps.go`, update the `--workload` flag description to list `thread-read`:

```go
	workload := fs.String("workload", "messages", "messages|thread|history|read-receipt|room-read|thread-read")
```

In `tools/loadgen/maxrps.go`, add a `case "thread-read"` to the `switch *workload` in `runMaxRPS`, immediately after the existing `case "room-read"` block:

```go
	case "thread-read":
		p, ok := BuiltinHistoryPreset(*preset)
		if !ok {
			fmt.Fprintf(os.Stderr, "unknown history preset: %s\n", *preset)
			return 2
		}
		if *requestTimeout <= 0 {
			fmt.Fprintln(os.Stderr, "--request-timeout must be > 0")
			return 2
		}
		if *pageLimit <= 0 {
			fmt.Fprintln(os.Stderr, "--page-limit must be > 0")
			return 2
		}
		trw, clean, err := newThreadReadWorkload(ctx, cfg, &p, *seed, *pageLimit, *requestTimeout)
		if err != nil {
			slog.Error("init thread-read workload", "error", err)
			return 1
		}
		w, cleanup, presetID = trw, clean, p.Name
```

In `tools/loadgen/main.go`, update the `runSeed` `--workload` flag description and add a `case "thread-read"`:

```go
	workload := fs.String("workload", "messages", "messages|thread|members|history|read-receipt|room-read|thread-read|botroom")
```

```go
	case "thread-read":
		return runSeedHistory(ctx, cfg, *preset, *seed)
```

In `tools/loadgen/main.go`, update the `runTeardown` `--workload` flag description and add a `case "thread-read"`:

```go
	workload := fs.String("workload", "messages", "messages|thread|members|history|room-read|thread-read|botroom")
```

```go
	case "thread-read":
		return runTeardownHistory(ctx, cfg, *preset, *seed)
```

- [ ] **Step 4: Run the tests and verify build**

Run: `go test ./tools/loadgen/ -run TestDefaultSteps_ThreadRead -v`
Expected: PASS.

Run: `go build ./tools/loadgen/`
Expected: builds with no errors.

- [ ] **Step 5: Commit**

```bash
git add tools/loadgen/maxrps.go tools/loadgen/main.go tools/loadgen/maxrps_threadread_test.go
git commit -m "loadgen: wire thread-read into max-rps/seed/teardown CLI

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01R44pRsRkU6XDV7dne1Eudv"
```

---

### Task 5: Integration test (NATS round-trip against a stub responder)

**Files:**
- Create: `tools/loadgen/threadread_integration_test.go`

**Interfaces:**
- Consumes: `BuildHistoryFixtures`, `Seed`, `newThreadReadGenerator`, `threadReadGeneratorConfig`, `newThreadReadCollector`, `newNATSHistoryRequester`; `subject.MsgThreadWildcard`; `testutil.MongoDB`, `testutil.NATS`.
- Produces: nothing consumed downstream — coverage only.

This mirrors `roomread_integration_test.go`: it drives the generator against a stub NATS responder (the real history-service is exercised by that service's own integration tests). The generator reads `ThreadParents` from the in-memory fixtures, so no Cassandra is needed here — the Cassandra seed path is covered by the existing history seed integration tests. The package's existing `TestMain` (`testutil.RunTests`) drives container cleanup; do not add another.

- [ ] **Step 1: Write the test**

Create `tools/loadgen/threadread_integration_test.go`:

```go
//go:build integration

package main

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
)

// TestThreadReadWorkload_EndToEnd seeds history fixtures (with thread parents)
// into a real Mongo, then drives the generator briefly against a stub
// GetThreadMessages responder and asserts it records samples with no errors.
func TestThreadReadWorkload_EndToEnd(t *testing.T) {
	ctx := context.Background()

	db := testutil.MongoDB(t, "loadgen_threadread")

	siteID := "site-test"
	p := HistoryPreset{
		Name: "thread-it", Users: 12, Rooms: 3, BaselineSize: 6,
		MessagesPerRoom: 60, MessageSpanDays: 1, ThreadRate: 0.25,
		RepliesPerThread: 3, ContentBytes: 50,
	}
	fixtures := BuildHistoryFixtures(&p, 42, siteID, time.Now().UTC())
	require.NoError(t, Seed(ctx, db, &fixtures.Fixtures))
	require.NotEmpty(t, fixtures.ThreadParents, "preset must seed thread parents")

	// Stub GetThreadMessages responder.
	nc, err := nats.Connect(testutil.NATS(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = nc.Drain() })

	sub, err := nc.Subscribe(subject.MsgThreadWildcard(siteID), func(m *nats.Msg) {
		_ = m.Respond([]byte(`{"messages":[],"parentMessage":{"messageId":"x"},"hasNext":false}`))
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	collector := newThreadReadCollector()
	gen := newThreadReadGenerator(&threadReadGeneratorConfig{
		Fixtures:       &fixtures,
		SiteID:         siteID,
		Rate:           50,
		PageLimit:      20,
		RequestTimeout: 2 * time.Second,
		Requester:      newNATSHistoryRequester(nc),
		Collector:      collector,
		MaxInFlight:    16,
	}, 42)

	runCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	require.NoError(t, gen.Run(runCtx))

	assert.NotEmpty(t, collector.Samples(), "generator produced zero samples")
	assert.Equal(t, 0, collector.TimeoutErrors(), "no requests should time out against the stub")
	assert.Equal(t, 0, collector.ReplyErrors(), "stub never returns an error")
	assert.Equal(t, 0, collector.BadReplyCount(), "stub always returns a parentMessage")
	assert.Equal(t, 0, collector.NoParentsCount(), "every seeded room has parents")
}
```

- [ ] **Step 2: Run the integration test**

Run: `go test -tags integration ./tools/loadgen/ -run TestThreadReadWorkload_EndToEnd -v`
Expected: PASS (requires Docker for the Mongo + NATS testutil containers).

- [ ] **Step 3: Commit**

```bash
git add tools/loadgen/threadread_integration_test.go
git commit -m "loadgen: add thread-read end-to-end integration test

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01R44pRsRkU6XDV7dne1Eudv"
```

---

### Task 6: Documentation

**Files:**
- Modify: `tools/loadgen/README.md`

**Interfaces:** none (docs only).

- [ ] **Step 1: Update the `max-rps` workload references**

In `tools/loadgen/README.md`, in the "max-rps — auto-find Max RPS under SLO" section, update the usage line and the `--workload` flag row to include `thread-read`:

- Change the code-fence usage line from
  `loadgen max-rps --workload=messages|history|read-receipt --preset=<name> [flags]`
  to
  `loadgen max-rps --workload=messages|history|read-receipt|room-read|thread-read --preset=<name> [flags]`
- In the Flags table, change the `--workload` row's Notes cell to:
  `` `messages`, `history`, `read-receipt`, `room-read`, or `thread-read` ``

- [ ] **Step 2: Add the Thread-read workload section**

In `tools/loadgen/README.md`, add this section immediately after the "History workload (LoadHistory / GetThreadMessages benchmark)" section and before the "Thread-reply workload (thread-send benchmark)" section:

```markdown
## Thread-read workload (GetThreadMessages benchmark)

Finds the maximum sustainable RPS for **loading thread messages** —
`history-service.GetThreadMessages`, the single-partition slice read on the
Cassandra `thread_messages_by_thread` table. This isolates the thread-read
ceiling that the `history` workload only measures blended with `LoadHistory`
(via its `--mix`); read the focused number here and compare it against the
blended `history` run on the same box.

**First-page opens only.** Each request opens a thread cold — pick a seeded
parent and fetch the first page of replies (no cursor). Models the dominant
real case (a user clicking into a thread).

**Reuses the history fixtures and seed.** Like `read-receipt`, this workload
reads the history presets' rooms/subscriptions and the seeded thread parents +
replies; there is no dedicated seed. Requires `CASSANDRA_HOSTS` and the same
`MESSAGE_BUCKET_HOURS` as the running services.

### Quick start

```bash
make -C tools/loadgen/deploy up

# Seed rooms/subs/keys (Mongo) + parents/replies/thread_rooms (Cassandra+Mongo).
# Use a preset that seeds threads: history-medium or history-large
# (history-small has ThreadRate 0 and seeds no threads).
loadgen seed --workload=thread-read --preset=history-medium

# Ramp the thread-read path.
loadgen max-rps --workload=thread-read --preset=history-medium

# Clean up.
loadgen teardown --workload=thread-read --preset=history-medium
```

Via the deploy Makefile:

```bash
make -C tools/loadgen/deploy run-max-rps WORKLOAD=thread-read PRESET=history-medium
```

### Presets

Reuses the **history** presets. `history-medium` / `history-large` seed thread
parents in every room; `history-small` seeds none, so a thread-read ramp on it
issues no real reads (every request is counted as `no-thread-parents` and the
step reports no samples).

### Subcommands

- `loadgen seed --workload=thread-read --preset=<name> [--seed=42]` — delegates
  to the history seed (Mongo users/rooms/subscriptions/thread\_rooms + room keys;
  Cassandra `messages_by_room` / `messages_by_id` / `thread_messages_by_thread`).
- `loadgen max-rps --workload=thread-read --preset=<name> [flags]` — ramp the
  GetThreadMessages read path. Honors `--page-limit` (default 20),
  `--request-timeout` (default 5s), and the shared ramp flags (`--steps`
  defaults to `200,500,1000,2000,5000`, `--warmup`, `--hold`, `--cooldown`,
  `--slo-*`, `--csv`).
- `loadgen teardown --workload=thread-read --preset=<name>` — delegates to the
  history teardown.

### Reading the summary

Synchronous request/reply: gated on the single `thread-read` latency series'
p95/p99 and the error rate only (no consumer-pending signal, so
`--slo-pending-growth` is ignored). A non-zero error rate at low RPS usually
means a seeding/config problem — a `MESSAGE_BUCKET_HOURS` mismatch making the
seeded parents unreadable, or pointing the run at `history-small`. The verdict,
INCONCLUSIVE load-box guard, and CSV output behave exactly as for the other
read workloads.
```

- [ ] **Step 2: Commit**

```bash
git add tools/loadgen/README.md
git commit -m "loadgen: document the thread-read workload

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01R44pRsRkU6XDV7dne1Eudv"
```

---

## Final verification

- [ ] Run the full loadgen unit suite: `go test ./tools/loadgen/ -race`
  Expected: PASS (all new + existing tests).
- [ ] Lint: `make lint` (golangci-lint — goimports, staticcheck, errcheck, etc.).
  Expected: no new findings in the touched files.
- [ ] SAST: `make sast` is a blocking CI gate; run it before pushing.
  Expected: no new medium+ findings (this change adds no `InsecureSkipVerify`,
  unsafe conversions, or new attack surface).

---

## Self-Review notes (against the spec)

- **Spec coverage:** collector (Task 1), generator with first-page reads + strict reply validation + no-parents skip (Task 2), adapter + single `thread-read` series + no pending (Task 3), CLI `max-rps`/`seed`/`teardown` + default read-path steps (Task 4), integration (Task 5), README section + flag references (Task 6). All spec sections map to a task.
- **Reuse:** history fixtures/seed/teardown, `HistoryRequester`, `getThreadMessagesRequest`, the ramp/verdict/report/CSV harness — no duplication.
- **Type consistency:** `threadReadCollector`/`threadReadSample`/`threadReadGenerator`/`threadReadGeneratorConfig`/`threadReadWorkload`/`threadReadReply`/`buildThreadReadInputs`/`runThreadReadFor`/`newThreadReadWorkload`/`newThreadReadGenerator`/`newThreadReadCollector` used identically across tasks. `Label()` returns `"thread-read"`; the latency series name is `"thread-read"`; `defaultSteps("thread-read")` matches the other reads.
- **Deviation from spec:** the spec mentioned the generator could "error on an empty parent set"; the plan instead mirrors the history workload — Zipf over all rooms, with no-parent rooms skipped and counted via `RecordNoParents` (a `history-small` run then reports zero samples rather than erroring). This is the more consistent, lower-surprise behavior and keeps the `noParents` counter meaningful.
