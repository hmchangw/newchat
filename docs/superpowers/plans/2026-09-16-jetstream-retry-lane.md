# JetStream Retry Lane Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the long tail of JetStream retries off the hot consumer lane onto a dedicated RETRY stream, so a sustained transient-failure rate no longer exhausts a consumer's ack-pending budget and stalls healthy traffic.

**Architecture:** A message that fails keeps its ack-pending slot for the whole Nak backoff. `jsretry.DefaultBackoff` spends 756s per failing message, so at 5 failures/s the 1000-slot budget is gone in 200s. A new `pkg/retrylane` wraps `pkg/jsretry`: the fast rungs (`1s/5s/30s` = 36s) stay in place, and at the threshold the message is republished to `RETRY-{siteID}` and Acked, where a second per-service consumer runs the same handler on the slow rungs. The total retry budget is unchanged (12.6 min) — only the ack-pending occupancy moves, 756s → 36s.

**Tech Stack:** Go 1.25, NATS JetStream (`nats-io/nats.go/jetstream`), `caarlos0/env`, `go.uber.org/mock`, `stretchr/testify`, `testcontainers-go` via `pkg/testutil`.

**Spec:** `docs/superpowers/specs/2026-09-10-jetstream-retry-lane-design.md`

## Global Constraints

- **Scope is phases 0–3 only.** Phase 4 (the `DLQ-{siteID}` stream, `dlq-worker`, the Mongo triage record, `tools/dlqreplay`, admin-service endpoints) is a **separate PR** and is NOT in this plan. Terminal failures on the retry lane keep today's behaviour — `MaxDeliver` exhaustion with the existing `max_deliver` metric — until phase 4 lands.
- **TDD is mandatory** (CLAUDE.md §4): Red → Green → Refactor → Commit. Never write implementation before its test. Never skip the Red phase.
- **Always use `make` targets**, never raw `go` commands. `make test SERVICE=<name>`, `make test-integration SERVICE=<name>`, `make lint`, `make fmt`.
- **Coverage floor 80%**, target 90%+ for `pkg/retrylane` (a shared `pkg/` package).
- **Never call bare `Nak()` or `NakWithDelay(0)`** — `.semgrep/jsnak.yml` rule `jsretry-no-bare-nak` and `jsretry-no-zero-nak-delay` are blocking CI gates. All nak paths go through `pkg/jsretry`.
- **Never hardcode `cc.BackOff`** in a service — `.semgrep/jsnak.yml` rule `jsretry-no-hardcoded-consumer-backoff`. Derive via `stream.DurableConsumerDefaults`.
- **`make sast` is a blocking CI gate** (fails on medium+). Run locally before pushing.
- **Subjects come from `pkg/subject` builders**, never raw `fmt.Sprintf` at a call site.
- **Stream bootstrap is opt-in**: `BOOTSTRAP_STREAMS` env, default `false`; bootstrap helpers set only `Name + Subjects`.
- **Feature flag default is `false`**: `RETRY_LANE_ENABLED=false` everywhere until deliberately enabled per service.
- **Errors wrap with context**: `fmt.Errorf("short description: %w", err)`. Never bare `err`.
- **Logging is `log/slog` JSON only**, structured key-value fields, `request_id` propagated. Never log message bodies or tokens.

---

### Task 1: The gate — prove a Nak-with-delay holds its ack-pending slot

**This task is a gate.** The entire design rests on the premise that a message Nak'd with a delay continues to occupy its consumer's `MaxAckPending` budget for the whole backoff. That premise is currently sourced from this repo's own prose (`docs/design/2026-07-05-membership-federation-durability.md:38`, `tools/observability/METRICS.md:56`), not from nats-server source or a live consumer. **If this test does not behave as predicted, stop and report back — do not proceed to Task 2.**

**Files:**
- Create: `pkg/stream/ackpending_integration_test.go`

**Interfaces:**
- Consumes: `testutil.NATS(t) string`, `testutil.RunTests(m)` from `pkg/testutil`.
- Produces: nothing consumed by later tasks — this is a proof, not a component.

- [ ] **Step 1: Check whether `pkg/stream` already has an integration TestMain**

Run: `grep -rn "func TestMain" pkg/stream/`

If a `TestMain` already exists in an `//go:build integration` file in `pkg/stream`, do NOT add another — Go allows only one `TestMain` per package. Note which file it is and skip the `TestMain` in Step 2.

- [ ] **Step 2: Write the failing test**

Create `pkg/stream/ackpending_integration_test.go`:

```go
//go:build integration

package stream_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

// TestNakWithDelayHoldsAckPendingSlot is the load-bearing premise of the retry
// lane design: a message Nak'd with a delay keeps occupying the consumer's
// MaxAckPending budget for the whole backoff, so parked retries starve healthy
// traffic. With MaxAckPending=1, a single parked message must block delivery of
// the next one until its nak delay elapses.
func TestNakWithDelayHoldsAckPendingSlot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	nc, err := nats.Connect(testutil.NATS(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = nc.Drain() })

	js, err := jetstream.New(nc)
	require.NoError(t, err)

	const streamName = "ACKPENDING-PREMISE"
	const subj = "ackpending.premise.test"
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{subj},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = js.DeleteStream(context.Background(), streamName) })

	for _, body := range []string{"first", "second"} {
		_, err = js.Publish(ctx, subj, []byte(body))
		require.NoError(t, err)
	}

	cons, err := js.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{
		Durable:       "premise",
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckWait:       30 * time.Second,
		MaxDeliver:    6,
		MaxAckPending: 1,
	})
	require.NoError(t, err)

	first, err := cons.Next(jetstream.FetchMaxWait(5 * time.Second))
	require.NoError(t, err)
	require.Equal(t, "first", string(first.Data()))

	// Park it for longer than the probe window below.
	require.NoError(t, first.NakWithDelay(10*time.Second))

	// THE ASSERTION: with the parked message still holding the only ack-pending
	// slot, the second message must not be delivered.
	_, err = cons.Next(jetstream.FetchMaxWait(3 * time.Second))
	require.Error(t, err, "a parked nak must hold its ack-pending slot and block the next delivery")
	require.True(t, errors.Is(err, jetstream.ErrNoMessages) || errors.Is(err, nats.ErrTimeout),
		"expected no-messages/timeout, got %v", err)
}
```

- [ ] **Step 3: Run the test**

Run: `make test-integration SERVICE=stream`

If the Makefile's `SERVICE=` does not resolve `pkg/` packages, check how other `pkg/` integration tests are invoked first:

Run: `grep -n "test-integration" Makefile`

Then use the form that matches. Expected: **PASS** — the parked nak blocks the second delivery.

- [ ] **Step 4: Evaluate the gate**

- **PASS** → the premise holds. Record the result in the commit message and continue to Task 2.
- **FAIL** (second message delivered while first is parked) → **STOP.** The ack-pending occupancy argument is wrong, the 756s→36s improvement does not exist, and the design needs revisiting before any further work. Report the observed behaviour and halt.

- [ ] **Step 5: Commit**

```bash
git add pkg/stream/ackpending_integration_test.go
git commit -m "test(stream): pin the nak-with-delay ack-pending premise

The retry lane design rests on a Nak'd-with-delay message continuing to
occupy its consumer's MaxAckPending slot for the whole backoff. That was
documented in prose but never tested. With MaxAckPending=1 a parked nak
must block the next delivery; this test fails loudly if a future
nats-server release changes that."
```

---

### Task 2: `pkg/subject` — retry subject builders

**Files:**
- Modify: `pkg/subject/subject.go`
- Test: `pkg/subject/subject_test.go`

**Interfaces:**
- Produces:
  - `subject.Retry(siteID, consumer, tier string) string` → `chat.retry.{siteID}.{consumer}.{tier}`
  - `subject.RetryConsumerWildcard(siteID, consumer string) string` → `chat.retry.{siteID}.{consumer}.>`
  - `subject.RetryWildcard(siteID string) string` → `chat.retry.{siteID}.>`

- [ ] **Step 1: Write the failing test**

Append to `pkg/subject/subject_test.go` (match the file's existing package clause — check whether it is `package subject` or `package subject_test` and use the same):

```go
func TestRetrySubjects(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{"slow tier", subject.Retry("site1", "message-worker", subject.RetryTierSlow), "chat.retry.site1.message-worker.slow"},
		{"replay tier", subject.Retry("site1", "message-worker", subject.RetryTierReplay), "chat.retry.site1.message-worker.replay"},
		{"consumer wildcard", subject.RetryConsumerWildcard("site1", "broadcast-worker"), "chat.retry.site1.broadcast-worker.>"},
		{"stream wildcard", subject.RetryWildcard("site1"), "chat.retry.site1.>"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.got)
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=subject` (or the `pkg/` equivalent per the Makefile)
Expected: FAIL — `undefined: subject.Retry`

- [ ] **Step 3: Write minimal implementation**

Append to `pkg/subject/subject.go`:

```go
// Retry tier tokens. A tier distinguishes how an entry arrived on the RETRY
// stream: slow is an organic escalation from a hot lane, replay is an
// operator-triggered re-run. Both are drained by the same per-service consumer
// (RetryConsumerWildcard); the tier exists so a replay is distinguishable from
// a real failure in metrics and logs.
const (
	RetryTierSlow   = "slow"
	RetryTierReplay = "replay"
)

// Retry is the RETRY-{siteID} subject for one consumer's escalated message:
// chat.retry.{siteID}.{consumer}.{tier}. The consumer token routes the message
// back to exactly one consumer — JetStream filters by subject, not by header,
// so republishing to the origin subject would re-run every consumer of that
// stream when only one failed.
func Retry(siteID, consumer, tier string) string {
	return fmt.Sprintf("chat.retry.%s.%s.%s", siteID, consumer, tier)
}

// RetryConsumerWildcard is the FilterSubject for one service's retry consumer:
// chat.retry.{siteID}.{consumer}.>, covering every tier.
func RetryConsumerWildcard(siteID, consumer string) string {
	return fmt.Sprintf("chat.retry.%s.%s.>", siteID, consumer)
}

// RetryWildcard matches every retry subject on a site: chat.retry.{siteID}.>.
// Use as the RETRY-{siteID} stream's subject pattern.
func RetryWildcard(siteID string) string {
	return fmt.Sprintf("chat.retry.%s.>", siteID)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=subject`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/subject/subject.go pkg/subject/subject_test.go
git commit -m "feat(subject): add RETRY stream subject builders

chat.retry.{siteID}.{consumer}.{tier}. The consumer token is load-bearing:
JetStream filters by subject not header, so a retry must carry its target
consumer in the subject or a replay re-runs every consumer of the origin
stream."
```

---

### Task 3: `pkg/stream` — the RETRY stream config

**Files:**
- Modify: `pkg/stream/stream.go`
- Test: `pkg/stream/stream_test.go`

**Interfaces:**
- Consumes: `subject.RetryWildcard` (Task 2).
- Produces: `stream.Retry(siteID string) Config` — `Name: "RETRY-{siteID}"`, `Subjects: ["chat.retry.{siteID}.>"]`.

- [ ] **Step 1: Write the failing test**

Append to `pkg/stream/stream_test.go` (match the existing package clause):

```go
func TestRetry(t *testing.T) {
	got := stream.Retry("site1")
	assert.Equal(t, "RETRY-site1", got.Name)
	assert.Equal(t, []string{"chat.retry.site1.>"}, got.Subjects)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=stream`
Expected: FAIL — `undefined: stream.Retry`

- [ ] **Step 3: Write minimal implementation**

Append to `pkg/stream/stream.go`:

```go
// Retry returns RETRY-{siteID}: the tiered-redelivery lane where a message
// goes when its in-place fast-rung budget is spent, so the long waits stop
// occupying the hot consumer's ack-pending budget. Each participating service
// binds its own consumer filtered to chat.retry.{siteID}.{its name}.>.
//
// Duplicates (the dedup window) and retention are ops/IaC-owned, as for every
// stream here; pkg/retrylane's deterministic Nats-Msg-Id only deduplicates a
// crash-retried escalation while that window holds.
func Retry(siteID string) Config {
	return Config{
		Name:     fmt.Sprintf("RETRY-%s", siteID),
		Subjects: []string{subject.RetryWildcard(siteID)},
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=stream`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/stream/stream.go pkg/stream/stream_test.go
git commit -m "feat(stream): add the RETRY-{siteID} stream config"
```

---

### Task 4: `pkg/natsmetrics` — the `escalated` outcome

Without this, the hot lane's Ack-after-republish is indistinguishable from a successful process, making escalation invisible in exactly the dashboards built to catch it.

**Files:**
- Modify: `pkg/natsmetrics/metrics.go:25-41` (the `Outcome` const block and `allOutcomes`)
- Modify: `pkg/natsmetrics/metrics.go` (add an `Escalated` method on `*Message`)
- Test: `pkg/natsmetrics/metrics_test.go`

**Interfaces:**
- Produces:
  - `natsmetrics.OutcomeEscalated Outcome = "escalated"`
  - `func (m *Message) Escalated()` — records the escalation disposition for this delivery. Call it **instead of** letting the Ack record `ack`.

> `TerminalDeadLettered` from spec §5 is deliberately NOT added here — it labels the DLQ hop, which is phase 4.

- [ ] **Step 1: Write the failing test**

Append to `pkg/natsmetrics/metrics_test.go`:

```go
func TestOutcomeEscalatedIsRegistered(t *testing.T) {
	assert.Contains(t, natsmetrics.AllOutcomesForTest(), natsmetrics.OutcomeEscalated,
		"escalated must be pre-registered or its series never initialises")
	assert.Equal(t, natsmetrics.Outcome("escalated"), natsmetrics.OutcomeEscalated)
}
```

If `allOutcomes` is unexported and the test file is `package natsmetrics` (internal), assert on `allOutcomes` directly and drop the `AllOutcomesForTest` accessor. Check first:

Run: `head -5 pkg/natsmetrics/metrics_test.go`

Use the internal form if the test package is `natsmetrics`; only add an exported test accessor if the tests are external.

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=natsmetrics`
Expected: FAIL — `undefined: natsmetrics.OutcomeEscalated`

- [ ] **Step 3: Write minimal implementation**

In `pkg/natsmetrics/metrics.go`, add to the `Outcome` const block:

```go
	// OutcomeEscalated is an Ack that handed the message to the RETRY lane
	// rather than completing it. It must not be counted as `ack`: the work is
	// not done, it moved. See pkg/retrylane.
	OutcomeEscalated Outcome = "escalated"
```

And add it to `allOutcomes`:

```go
var allOutcomes = []Outcome{
	OutcomeAck, OutcomeNak, OutcomeTerm, OutcomeLeftPending,
	OutcomeHandlerCancelled, OutcomeEscalated,
}
```

Add the recorder near the other disposition methods (beside `Term`):

```go
// Escalated records that this delivery was Acked because pkg/retrylane
// republished it onto the RETRY stream. The caller Acks the message itself;
// this only fixes the label, so the escalation does not read as a success.
func (m *Message) Escalated() { m.finishWithOutcome(m.ctx, OutcomeEscalated) }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=natsmetrics`
Expected: PASS

- [ ] **Step 5: Verify no double-counting regression**

Run: `make test SERVICE=natsmetrics`
Expected: PASS — all pre-existing tests still green. `finishWithOutcome` runs under `disposeOnce`, so a later `Ack()` on the same tracked message cannot also record `ack`. Confirm by reading `finishWithOutcome` and `disposeOnce` in `pkg/natsmetrics/metrics.go:474-505`.

- [ ] **Step 6: Commit**

```bash
git add pkg/natsmetrics/
git commit -m "feat(natsmetrics): add the escalated consumer outcome

An Ack that handed the message to the RETRY lane is not a success — the
work moved, it did not complete. Without a distinct outcome the escalation
is invisible in the dashboards built to catch it."
```

---

### Task 5: `pkg/retrylane` — the escalation envelope

Pure functions only: headers and dedup ID. No publishing, no NATS.

**Files:**
- Create: `pkg/retrylane/envelope.go`
- Create: `pkg/retrylane/envelope_test.go`

**Interfaces:**
- Produces:
  - Header constants `HeaderOriginStream`, `HeaderOriginSeq`, `HeaderOriginSubject`, `HeaderConsumer`, `HeaderAttempt`, `HeaderFirstFailedAt`, `HeaderReason`
  - `func DedupID(originStream string, originSeq uint64, consumer string) string`
  - `func BuildHeaders(in nats.Header, meta *jetstream.MsgMetadata, originSubject, consumer, reason string, now time.Time) nats.Header`
  - `func ReasonFor(err error) string`

- [ ] **Step 1: Write the failing test**

Create `pkg/retrylane/envelope_test.go`:

```go
package retrylane_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/retrylane"
)

func TestDedupID(t *testing.T) {
	assert.Equal(t, "MESSAGES-CANONICAL-site1:42:message-worker",
		retrylane.DedupID("MESSAGES-CANONICAL-site1", 42, "message-worker"))
}

func TestDedupIDIsDeterministic(t *testing.T) {
	a := retrylane.DedupID("S", 7, "c")
	b := retrylane.DedupID("S", 7, "c")
	assert.Equal(t, a, b, "escalation is publish-then-ack; a crash-retried escalation must dedup")
}

func TestReasonFor(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"not found", errcode.NotFound("room not found"), "not_found"},
		{"wrapped errcode", fmt.Errorf("outer: %w", errcode.Conflict("dup")), "conflict"},
		{"plain infra error", errors.New("dial tcp: connection refused"), "internal"},
		{"nil", nil, "internal"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, retrylane.ReasonFor(tt.err))
		})
	}
}

func TestReasonForNeverLeaksTheErrorText(t *testing.T) {
	secret := "user-said-something-private"
	got := retrylane.ReasonFor(errors.New(secret))
	assert.NotContains(t, got, secret, "the reason is a bounded category, never the error string")
}

func meta(stream string, seq uint64, delivered uint64) *jetstream.MsgMetadata {
	return &jetstream.MsgMetadata{
		Stream:       stream,
		NumDelivered: delivered,
		Sequence:     jetstream.SequencePair{Stream: seq},
	}
}

func TestBuildHeadersPopulatesProvenance(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000).UTC()
	got := retrylane.BuildHeaders(nil, meta("MESSAGES-CANONICAL-site1", 42, 4),
		"chat.msg.canonical.site1.created", "message-worker", "internal", now)

	assert.Equal(t, "MESSAGES-CANONICAL-site1", got.Get(retrylane.HeaderOriginStream))
	assert.Equal(t, "42", got.Get(retrylane.HeaderOriginSeq))
	assert.Equal(t, "chat.msg.canonical.site1.created", got.Get(retrylane.HeaderOriginSubject))
	assert.Equal(t, "message-worker", got.Get(retrylane.HeaderConsumer))
	assert.Equal(t, "internal", got.Get(retrylane.HeaderReason))
	assert.Equal(t, "4", got.Get(retrylane.HeaderAttempt))
	assert.Equal(t, "1700000000000", got.Get(retrylane.HeaderFirstFailedAt))
}

func TestBuildHeadersCarriesCorrelationThrough(t *testing.T) {
	in := nats.Header{}
	in.Set(natsutil.RequestIDHeader, "01970a4f-8c2d-7c9a-abcd-e0123456789f")
	in.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")

	got := retrylane.BuildHeaders(in, meta("S", 1, 4), "subj", "c", "internal", time.Now())

	assert.Equal(t, "01970a4f-8c2d-7c9a-abcd-e0123456789f", got.Get(natsutil.RequestIDHeader),
		"a retry twelve minutes later must land in the same trace lineage")
	assert.Equal(t, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", got.Get("traceparent"))
}

func TestBuildHeadersAccumulatesAttemptsAcrossLanes(t *testing.T) {
	in := nats.Header{}
	in.Set(retrylane.HeaderAttempt, "4")

	got := retrylane.BuildHeaders(in, meta("S", 1, 2), "subj", "c", "internal", time.Now())

	assert.Equal(t, "6", got.Get(retrylane.HeaderAttempt),
		"attempts are cumulative: 4 on the hot lane plus 2 on the retry lane")
}

func TestBuildHeadersPreservesFirstFailedAt(t *testing.T) {
	in := nats.Header{}
	in.Set(retrylane.HeaderFirstFailedAt, "1600000000000")

	got := retrylane.BuildHeaders(in, meta("S", 1, 2), "subj", "c", "internal", time.Now())

	assert.Equal(t, "1600000000000", got.Get(retrylane.HeaderFirstFailedAt),
		"first failure time must survive every hop or time-to-dead-letter is wrong")
}

func TestBuildHeadersDoesNotMutateInput(t *testing.T) {
	in := nats.Header{}
	in.Set(retrylane.HeaderAttempt, "4")

	_ = retrylane.BuildHeaders(in, meta("S", 1, 2), "subj", "c", "internal", time.Now())

	assert.Equal(t, "4", in.Get(retrylane.HeaderAttempt), "input headers belong to the live message")
}

func TestBuildHeadersHandlesNilMetadata(t *testing.T) {
	got := retrylane.BuildHeaders(nil, nil, "subj", "c", "internal", time.Now())
	require.NotNil(t, got)
	assert.Equal(t, "c", got.Get(retrylane.HeaderConsumer))
	assert.Equal(t, "", got.Get(retrylane.HeaderOriginStream))
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=retrylane`
Expected: FAIL — package `retrylane` does not exist.

- [ ] **Step 3: Write minimal implementation**

Create `pkg/retrylane/envelope.go`:

```go
// Package retrylane moves the long tail of JetStream retries off a hot
// consumer lane and onto the RETRY stream.
//
// A message Nak'd with a delay keeps its ack-pending slot for the whole
// backoff, so jsretry.DefaultBackoff spends 756s of a shared 1000-slot budget
// per failing message. At a sustained 5 failures/s the budget is exhausted in
// ~200s and the consumer stops delivering anything, healthy messages included
// — the stall arrives long before the MaxDeliver drop does.
//
// This package keeps the fast rungs in place (~36s) and republishes the
// message onto RETRY-{siteID} when they are spent, where a second per-service
// consumer runs the same handler on the slow rungs. The total retry budget is
// unchanged; only the ack-pending occupancy moves.
package retrylane

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/natsutil"
)

// Escalation headers. Metadata rides in headers so the body stays
// byte-identical: message-worker writes those bytes to Cassandra, and the
// hot-path workers marshal via sonic whose output is not byte-identical to
// stdlib, so a re-marshal could change bytes that dedup keys pin.
const (
	HeaderOriginStream  = "X-Retry-Origin-Stream"
	HeaderOriginSeq     = "X-Retry-Origin-Seq"
	HeaderOriginSubject = "X-Retry-Origin-Subject"
	HeaderConsumer      = "X-Retry-Consumer"
	HeaderAttempt       = "X-Retry-Attempt"
	HeaderFirstFailedAt = "X-Retry-First-Failed-At"
	HeaderReason        = "X-Retry-Reason"
)

// traceparentHeader is the W3C trace context header carried through every hop
// so a retry lands in the same trace lineage as the original send.
const traceparentHeader = "traceparent"

// DedupID is the escalation's Nats-Msg-Id. Escalation is publish-then-Ack and
// therefore at-least-once: a crash between the two re-runs the handler and
// re-escalates, and this deterministic id makes the second publish a dedup
// no-op — while the stream's duplicate window holds, which is ops-owned.
func DedupID(originStream string, originSeq uint64, consumer string) string {
	return fmt.Sprintf("%s:%d:%s", originStream, originSeq, consumer)
}

// ReasonFor maps an error to a bounded category label. It returns the errcode
// Code when one is in the chain and "internal" otherwise. It never returns the
// error text: the reason reaches a header and a metric label, and a raw cause
// can carry a message body or token.
func ReasonFor(err error) string {
	var ec *errcode.Error
	if errors.As(err, &ec) && ec.Code.Valid() {
		return string(ec.Code)
	}
	return string(errcode.CodeInternal)
}

// BuildHeaders returns the headers for an escalated message. in is the live
// message's headers and is never mutated. Attempts accumulate across lanes and
// the first-failure timestamp survives every hop, so time-to-dead-letter stays
// truthful however many times a message is re-escalated.
func BuildHeaders(in nats.Header, meta *jetstream.MsgMetadata,
	originSubject, consumer, reason string, now time.Time,
) nats.Header {
	out := nats.Header{}

	// Correlation first: a retry must stay in the original trace lineage.
	if v := in.Get(natsutil.RequestIDHeader); v != "" {
		out.Set(natsutil.RequestIDHeader, v)
	}
	if v := in.Get(traceparentHeader); v != "" {
		out.Set(traceparentHeader, v)
	}

	out.Set(HeaderOriginSubject, originSubject)
	out.Set(HeaderConsumer, consumer)
	out.Set(HeaderReason, reason)

	var delivered uint64
	if meta != nil {
		out.Set(HeaderOriginStream, meta.Stream)
		out.Set(HeaderOriginSeq, strconv.FormatUint(meta.Sequence.Stream, 10))
		delivered = meta.NumDelivered
	}

	prior, _ := strconv.ParseUint(in.Get(HeaderAttempt), 10, 64)
	out.Set(HeaderAttempt, strconv.FormatUint(prior+delivered, 10))

	firstFailed := in.Get(HeaderFirstFailedAt)
	if firstFailed == "" {
		firstFailed = strconv.FormatInt(now.UTC().UnixMilli(), 10)
	}
	out.Set(HeaderFirstFailedAt, firstFailed)

	return out
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=retrylane`
Expected: PASS

If `TestReasonFor` fails on the `errcode.NotFound(...)` constructor signature, check the real one:

Run: `grep -n "func NotFound\|func Conflict" pkg/errcode/*.go`

and adjust the test's construction (not the implementation) to match.

- [ ] **Step 5: Commit**

```bash
git add pkg/retrylane/
git commit -m "feat(retrylane): escalation envelope — headers, dedup id, reason

Metadata rides in headers so the body stays byte-identical; the reason is a
bounded errcode category, never the error string, because it reaches a
header and a metric label. Attempts accumulate across lanes and the
first-failure timestamp survives every hop."
```

---

### Task 6: `pkg/retrylane` — `Lane.Settle`

**Files:**
- Create: `pkg/retrylane/retrylane.go`
- Create: `pkg/retrylane/retrylane_test.go`

**Interfaces:**
- Consumes: `BuildHeaders`, `DedupID`, `ReasonFor` (Task 5); `subject.Retry`, `subject.RetryTierSlow` (Task 2); `jsretry.Settle`, `jsretry.Nak`.
- Produces:
  - `type PublishFunc func(ctx context.Context, subj string, data []byte, hdr nats.Header, msgID string) error`
  - `type Msg interface { jsretry.Msg; Data() []byte; Subject() string; Headers() nats.Header }`
  - `type Lane struct { Consumer, SiteID string; Publish PublishFunc; Enabled bool; FastSteps int; OnEscalate func() }`
  - `func (l *Lane) Settle(ctx context.Context, msg Msg, backoff []time.Duration, err error)`
  - `func (l *Lane) WithEscalationHook(fn func()) *Lane`

> `PublishFunc` takes headers, unlike `pkg/outbox.Publish`'s 4-arg form. The divergence is deliberate — the escalation's whole payload of metadata is in headers.

- [ ] **Step 1: Write the failing test**

Create `pkg/retrylane/retrylane_test.go`:

```go
package retrylane_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/retrylane"
)

// fakeMsg is a retrylane.Msg double recording ack/nak behaviour.
type fakeMsg struct {
	data         []byte
	subject      string
	headers      nats.Header
	stream       string
	seq          uint64
	numDelivered uint64
	metaErr      error

	acked    bool
	naked    bool
	nakDelay time.Duration
}

func (m *fakeMsg) Metadata() (*jetstream.MsgMetadata, error) {
	if m.metaErr != nil {
		return nil, m.metaErr
	}
	return &jetstream.MsgMetadata{
		Stream:       m.stream,
		NumDelivered: m.numDelivered,
		Sequence:     jetstream.SequencePair{Stream: m.seq},
	}, nil
}
func (m *fakeMsg) Ack() error                    { m.acked = true; return nil }
func (m *fakeMsg) NakWithDelay(d time.Duration) error {
	m.naked = true
	m.nakDelay = d
	return nil
}
func (m *fakeMsg) Data() []byte         { return m.data }
func (m *fakeMsg) Subject() string      { return m.subject }
func (m *fakeMsg) Headers() nats.Header { return m.headers }

type capturedPublish struct {
	called  bool
	subject string
	data    []byte
	hdr     nats.Header
	msgID   string
	err     error
}

func (c *capturedPublish) fn() retrylane.PublishFunc {
	return func(_ context.Context, subj string, data []byte, hdr nats.Header, msgID string) error {
		c.called = true
		c.subject = subj
		c.data = data
		c.hdr = hdr
		c.msgID = msgID
		return c.err
	}
}

var testBackoff = []time.Duration{time.Second, 5 * time.Second, 30 * time.Second}

func newMsg(delivered uint64) *fakeMsg {
	return &fakeMsg{
		data:         []byte(`{"msg":"hello"}`),
		subject:      "chat.msg.canonical.site1.created",
		headers:      nats.Header{},
		stream:       "MESSAGES-CANONICAL-site1",
		seq:          42,
		numDelivered: delivered,
	}
}

func newLane(pub retrylane.PublishFunc) *retrylane.Lane {
	return &retrylane.Lane{
		Consumer:  "message-worker",
		SiteID:    "site1",
		Publish:   pub,
		Enabled:   true,
		FastSteps: 3,
	}
}

func TestSettleAcksOnSuccess(t *testing.T) {
	pub := &capturedPublish{}
	msg := newMsg(1)
	newLane(pub.fn()).Settle(context.Background(), msg, testBackoff, nil)

	assert.True(t, msg.acked)
	assert.False(t, msg.naked)
	assert.False(t, pub.called, "success never escalates")
}

func TestSettleNaksBelowThreshold(t *testing.T) {
	for delivered := uint64(1); delivered <= 3; delivered++ {
		pub := &capturedPublish{}
		msg := newMsg(delivered)
		newLane(pub.fn()).Settle(context.Background(), msg, testBackoff, errors.New("mongo down"))

		assert.True(t, msg.naked, "delivery %d is within the fast budget", delivered)
		assert.False(t, msg.acked)
		assert.False(t, pub.called)
		assert.Positive(t, msg.nakDelay, "a zero delay would be a bare nak")
	}
}

func TestSettleEscalatesAboveThreshold(t *testing.T) {
	pub := &capturedPublish{}
	msg := newMsg(4)
	newLane(pub.fn()).Settle(context.Background(), msg, testBackoff, errors.New("mongo down"))

	require.True(t, pub.called, "the fast budget is spent at delivery 4")
	assert.Equal(t, "chat.retry.site1.message-worker.slow", pub.subject)
	assert.Equal(t, []byte(`{"msg":"hello"}`), pub.data, "the body must be byte-identical")
	assert.Equal(t, "MESSAGES-CANONICAL-site1:42:message-worker", pub.msgID)
	assert.Equal(t, "message-worker", pub.hdr.Get(retrylane.HeaderConsumer))
	assert.True(t, msg.acked, "the hot lane releases the slot after a successful republish")
	assert.False(t, msg.naked)
}

func TestSettleNeverEscalatesPermanent(t *testing.T) {
	pub := &capturedPublish{}
	msg := newMsg(9)
	err := errcode.Permanent(errcode.BadRequest("malformed payload"))
	newLane(pub.fn()).Settle(context.Background(), msg, testBackoff, err)

	assert.False(t, pub.called, "a permanent error can never succeed; escalating it wastes the lane")
	assert.True(t, msg.acked, "permanent stays an Ack-drop")
	assert.False(t, msg.naked)
}

func TestSettleNaksWhenPublishFails(t *testing.T) {
	pub := &capturedPublish{err: errors.New("stream unavailable")}
	msg := newMsg(4)
	newLane(pub.fn()).Settle(context.Background(), msg, testBackoff, errors.New("mongo down"))

	require.True(t, pub.called)
	assert.False(t, msg.acked, "acking after a failed republish would silently lose the message")
	assert.True(t, msg.naked, "fall back to in-place redelivery")
}

func TestSettleDisabledIsPureJsretry(t *testing.T) {
	pub := &capturedPublish{}
	lane := newLane(pub.fn())
	lane.Enabled = false
	msg := newMsg(9)
	lane.Settle(context.Background(), msg, testBackoff, errors.New("mongo down"))

	assert.False(t, pub.called, "the flag is the rollback; disabled must behave exactly as before")
	assert.True(t, msg.naked)
	assert.False(t, msg.acked)
}

func TestSettleFallsBackWhenMetadataUnavailable(t *testing.T) {
	pub := &capturedPublish{}
	msg := newMsg(4)
	msg.metaErr = errors.New("no metadata")
	newLane(pub.fn()).Settle(context.Background(), msg, testBackoff, errors.New("mongo down"))

	assert.False(t, pub.called, "without NumDelivered we cannot know the budget is spent")
	assert.True(t, msg.naked)
}

func TestSettleWithoutPublishFuncDegradesToJsretry(t *testing.T) {
	lane := &retrylane.Lane{Consumer: "c", SiteID: "s", Enabled: true, FastSteps: 3}
	msg := newMsg(9)
	lane.Settle(context.Background(), msg, testBackoff, errors.New("mongo down"))

	assert.True(t, msg.naked, "a misconfigured lane must degrade, never panic")
	assert.False(t, msg.acked)
}

func TestSettleCallsOnEscalateBeforeAck(t *testing.T) {
	pub := &capturedPublish{}
	msg := newMsg(4)
	lane := newLane(pub.fn())

	var calledBeforeAck bool
	lane.OnEscalate = func() { calledBeforeAck = !msg.acked }

	lane.Settle(context.Background(), msg, testBackoff, errors.New("mongo down"))

	assert.True(t, calledBeforeAck,
		"the delivery must be labelled escalated before the Ack records it as a success")
}

func TestSettleDoesNotCallOnEscalateWhenPublishFails(t *testing.T) {
	pub := &capturedPublish{err: errors.New("stream unavailable")}
	msg := newMsg(4)
	lane := newLane(pub.fn())

	var called bool
	lane.OnEscalate = func() { called = true }

	lane.Settle(context.Background(), msg, testBackoff, errors.New("mongo down"))

	assert.False(t, called, "nothing escalated, so there is nothing to label")
}

func TestWithEscalationHookDoesNotMutateTheOriginal(t *testing.T) {
	base := newLane((&capturedPublish{}).fn())
	derived := base.WithEscalationHook(func() {})

	assert.Nil(t, base.OnEscalate, "the shared lane is read by every message goroutine")
	assert.NotNil(t, derived.OnEscalate)
	assert.Equal(t, base.Consumer, derived.Consumer)
	assert.Equal(t, base.FastSteps, derived.FastSteps)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=retrylane`
Expected: FAIL — `undefined: retrylane.Lane`

- [ ] **Step 3: Write minimal implementation**

Create `pkg/retrylane/retrylane.go`:

```go
package retrylane

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/jsretry"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
)

// PublishFunc publishes an escalated message. Injected rather than holding a
// JetStream handle, mirroring pkg/outbox.Publish, so tests capture escalations
// without a real NATS connection. It carries headers, unlike outbox's form —
// the escalation's metadata is entirely in headers.
type PublishFunc func(ctx context.Context, subj string, data []byte, hdr nats.Header, msgID string) error

// Msg widens jsretry.Msg with the accessors escalation needs. jetstream.Msg and
// oteljetstream.Msg both satisfy it.
type Msg interface {
	jsretry.Msg
	Data() []byte
	Subject() string
	Headers() nats.Header
}

// Lane settles messages for one consumer, escalating to RETRY-{SiteID} when the
// in-place fast-rung budget is spent. The zero value is disabled.
type Lane struct {
	// Consumer names this service's durable; it becomes the subject token that
	// routes an escalation back to exactly this consumer.
	Consumer string
	SiteID   string
	Publish  PublishFunc

	// Enabled is the rollback switch. When false, Settle is exactly jsretry.Settle.
	Enabled bool

	// FastSteps is how many deliveries stay in place before escalating; it
	// indexes into the backoff schedule rather than redefining it, so the total
	// retry budget is unchanged and only the ack-pending occupancy moves.
	FastSteps int

	// OnEscalate runs after a successful republish and before the Ack, so the
	// delivery is labelled `escalated` rather than `ack` — an escalation is not
	// a completion, the work moved. Optional; set it per message via
	// WithEscalationHook, never on a shared Lane.
	OnEscalate func()
}

// WithEscalationHook returns a shallow copy of l whose OnEscalate is fn. Call it
// per message: the hook closes over one delivery's metrics recorder, so
// mutating a shared Lane instead would be a data race across the worker's
// message goroutines.
func (l *Lane) WithEscalationHook(fn func()) *Lane {
	copied := *l
	copied.OnEscalate = fn
	return &copied
}

// Settle resolves a processed message:
//   - err == nil            → Ack
//   - permanent (errcode)   → Ack-drop (never escalates: it can never succeed)
//   - within FastSteps      → NakWithDelay, in place
//   - budget spent          → republish to RETRY, then Ack
//
// A failed republish falls back to a Nak: acking a message that was never
// handed on would lose it silently.
func (l *Lane) Settle(ctx context.Context, msg Msg, backoff []time.Duration, err error) {
	if !l.shouldEscalate(msg, err) {
		jsretry.Settle(ctx, msg, backoff, err)
		return
	}
	if escErr := l.escalate(ctx, msg, err); escErr != nil {
		slog.ErrorContext(ctx, "retry-lane escalation failed — falling back to in-place redelivery",
			"consumer", l.Consumer, "error", escErr,
			"request_id", natsutil.RequestIDFromContext(ctx))
		jsretry.Nak(ctx, msg, backoff, "escalation publish failed")
		return
	}
	if l.OnEscalate != nil {
		l.OnEscalate()
	}
	if ackErr := msg.Ack(); ackErr != nil {
		slog.ErrorContext(ctx, "failed to ack escalated message", "error", ackErr,
			"request_id", natsutil.RequestIDFromContext(ctx))
	}
}

// shouldEscalate reports whether this delivery has spent its in-place budget.
// Every negative answer routes to jsretry, which keeps the existing semantics
// as the single fallback path.
func (l *Lane) shouldEscalate(msg Msg, err error) bool {
	if err == nil || !l.Enabled || l.Publish == nil || l.FastSteps <= 0 {
		return false
	}
	if _, isPermanent := errcode.IsPermanent(err); isPermanent {
		return false
	}
	meta, metaErr := msg.Metadata()
	if metaErr != nil || meta == nil {
		return false
	}
	return meta.NumDelivered > uint64(l.FastSteps)
}

// escalate republishes the message onto the RETRY stream. The body is passed
// through untouched — re-marshalling could change bytes that dedup keys and
// wire-compat tests pin.
func (l *Lane) escalate(ctx context.Context, msg Msg, err error) error {
	meta, metaErr := msg.Metadata()
	if metaErr != nil {
		return fmt.Errorf("read message metadata: %w", metaErr)
	}
	hdr := BuildHeaders(msg.Headers(), meta, msg.Subject(), l.Consumer, ReasonFor(err), time.Now())
	subj := subject.Retry(l.SiteID, l.Consumer, subject.RetryTierSlow)
	msgID := DedupID(meta.Stream, meta.Sequence.Stream, l.Consumer)

	if pubErr := l.Publish(ctx, subj, msg.Data(), hdr, msgID); pubErr != nil {
		return fmt.Errorf("publish to retry lane %s: %w", subj, pubErr)
	}
	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=retrylane`
Expected: PASS — all thirteen tests.

- [ ] **Step 5: Verify coverage meets the `pkg/` bar**

Run: `go test -coverprofile=/tmp/claude-0/-home-user-newchat/364b9a92-e390-5afd-bbcb-3977f1fbf86e/scratchpad/rl.out ./pkg/retrylane/ && go tool cover -func=/tmp/claude-0/-home-user-newchat/364b9a92-e390-5afd-bbcb-3977f1fbf86e/scratchpad/rl.out`
Expected: total ≥ 90%. If below, add cases for the uncovered branches before committing.

- [ ] **Step 6: Lint and SAST**

Run: `make fmt && make lint && make sast`
Expected: clean. In particular the `jsretry-no-bare-nak` semgrep rule must not fire — every nak here goes through `jsretry.Nak`.

- [ ] **Step 7: Commit**

```bash
git add pkg/retrylane/
git commit -m "feat(retrylane): escalate to the RETRY lane when the fast budget is spent

Settle keeps jsretry's three outcomes and adds a fourth: republish to
RETRY-{siteID} and Ack, once NumDelivered passes FastSteps. Permanent
errors never escalate — they can never succeed. A failed republish falls
back to a Nak rather than acking, because acking a message that was never
handed on loses it silently. Disabled is exactly jsretry.Settle, so the
flag is a real rollback."
```

---

### Task 7: `pkg/retrylane` — consumer settings and binding helper

Every participating service binds a second consumer. This keeps that boilerplate in one place.

**Files:**
- Create: `pkg/retrylane/consumer.go`
- Create: `pkg/retrylane/consumer_test.go`

**Interfaces:**
- Consumes: `stream.ConsumerSettings`, `stream.DurableConsumerDefaults`, `subject.RetryConsumerWildcard`.
- Produces:
  - `type Settings struct { Enabled bool; FastSteps int; Consumer stream.ConsumerSettings }` with env prefix `RETRY_`
  - `func ConsumerConfig(siteID, consumer string, s Settings) jetstream.ConsumerConfig`
  - `func SlowBackoff(fastSteps int, full []time.Duration) []time.Duration`

- [ ] **Step 1: Write the failing test**

Create `pkg/retrylane/consumer_test.go`:

```go
package retrylane_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/jsretry"
	"github.com/hmchangw/chat/pkg/retrylane"
	"github.com/hmchangw/chat/pkg/stream"
)

func TestSlowBackoffIsTheTailOfTheFullSchedule(t *testing.T) {
	got := retrylane.SlowBackoff(3, jsretry.DefaultBackoff)
	assert.Equal(t, []time.Duration{2 * time.Minute, 10 * time.Minute}, got,
		"the budget is relocated, not redefined: slow is the tail the fast rungs left")
}

func TestSlowBackoffTotalPlusFastEqualsOriginalBudget(t *testing.T) {
	var fast, slow time.Duration
	for _, d := range jsretry.DefaultBackoff[:3] {
		fast += d
	}
	for _, d := range retrylane.SlowBackoff(3, jsretry.DefaultBackoff) {
		slow += d
	}
	var full time.Duration
	for _, d := range jsretry.DefaultBackoff {
		full += d
	}
	assert.Equal(t, full, fast+slow, "total patience per message must be unchanged")
}

func TestSlowBackoffDegradesSafely(t *testing.T) {
	tests := []struct {
		name      string
		fastSteps int
		full      []time.Duration
	}{
		{"fastSteps beyond the schedule", 99, jsretry.DefaultBackoff},
		{"empty schedule", 3, nil},
		{"negative fastSteps", -1, jsretry.DefaultBackoff},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := retrylane.SlowBackoff(tt.fastSteps, tt.full)
			require.NotEmpty(t, got, "an empty schedule would make jsretry fall back to a 1ms nak")
		})
	}
}

func TestConsumerConfigFiltersToItsOwnConsumer(t *testing.T) {
	s := retrylane.Settings{
		Enabled:   true,
		FastSteps: 3,
		Consumer: stream.ConsumerSettings{
			AckWait: 30 * time.Second, MaxDeliver: 3, MaxWaiting: 512,
			MaxAckPending: 4000, BackOffSteps: 3, BackOffFactor: 2, BackOffMax: 8 * time.Minute,
		},
	}
	cfg := retrylane.ConsumerConfig("site1", "message-worker", s)

	assert.Equal(t, "message-worker-retry", cfg.Durable)
	assert.Equal(t, []string{"chat.retry.site1.message-worker.>"}, cfg.FilterSubjects,
		"a retry consumer must never drain another service's escalations")
	assert.Equal(t, 4000, cfg.MaxAckPending,
		"the retry lane holds the long waits, so it needs its own large budget")
}

func TestApplyDefaultsFillsOnlyUnsetConsumerSettings(t *testing.T) {
	filled := retrylane.Settings{Enabled: true, FastSteps: 3}.ApplyDefaults()
	assert.Equal(t, 4000, filled.Consumer.MaxAckPending)
	assert.Equal(t, 3, filled.Consumer.MaxDeliver)

	explicit := retrylane.Settings{
		Enabled:  true,
		Consumer: stream.ConsumerSettings{MaxAckPending: 250, MaxDeliver: 9},
	}.ApplyDefaults()
	assert.Equal(t, 250, explicit.Consumer.MaxAckPending, "an operator override must survive")
	assert.Equal(t, 9, explicit.Consumer.MaxDeliver)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=retrylane`
Expected: FAIL — `undefined: retrylane.SlowBackoff`

- [ ] **Step 3: Write minimal implementation**

Create `pkg/retrylane/consumer.go`:

```go
package retrylane

import (
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
)

// Settings is the retry lane's env surface. Embed in a service Config with
// envPrefix:"RETRY_".
//
// The surface exposes a split *index*, not a schedule. A raw []time.Duration
// env knob has no off-switch under caarlos0/env — an empty value falls back to
// envDefault — and would let an operator silently redefine the retry budget.
type Settings struct {
	// Enabled is the per-service opt-in, default false. Disabling stops new
	// escalations; the retry consumer keeps draining what is already parked.
	Enabled bool `env:"LANE_ENABLED" envDefault:"false"`

	// FastSteps is how many deliveries stay on the hot lane. 3 leaves
	// {1s,5s,30s} = 36s of ack-pending occupancy instead of 756s.
	FastSteps int `env:"LANE_FAST_STEPS" envDefault:"3"`

	// Consumer tunes the retry lane's own durable, envPrefix RETRY_CONSUMER_.
	// MaxAckPending defaults high: this lane deliberately holds the long waits,
	// sized for ~5 escalations/s against ~720s of slow-rung occupancy.
	Consumer stream.ConsumerSettings `envPrefix:"CONSUMER_"`
}

// DurableName is the retry consumer's durable for a service.
func DurableName(consumer string) string { return consumer + "-retry" }

// DefaultConsumerSettings are the retry lane's consumer defaults, which differ
// from a hot lane's. This lane deliberately parks the long waits, so its
// ack-pending budget is sized for ~5 escalations/s against ~720s of slow-rung
// occupancy (≈3,600 in flight), and MaxDeliver counts retry-lane attempts only.
//
// Services apply this when their parsed Settings carry no explicit override —
// struct-tag envDefaults cannot express a different default for an embedded
// stream.ConsumerSettings than the hot lane's.
func DefaultConsumerSettings() stream.ConsumerSettings {
	return stream.ConsumerSettings{
		AckWait:       30 * time.Second,
		MaxDeliver:    3,
		MaxWaiting:    512,
		MaxAckPending: 4000,
		BackOffSteps:  3,
		BackOffFactor: 2,
		BackOffMax:    8 * time.Minute,
	}
}

// ApplyDefaults fills in DefaultConsumerSettings when the operator set no
// retry-consumer env vars, leaving any explicit value untouched. Call it once
// after env parsing.
func (s Settings) ApplyDefaults() Settings {
	if s.Consumer.MaxAckPending == 0 {
		s.Consumer = DefaultConsumerSettings()
	}
	return s
}

// SlowBackoff returns the rungs the fast schedule left behind. The retry budget
// is relocated, not redefined: fast + slow equals the original schedule, so
// total patience per message is unchanged and only the occupancy moves.
//
// It never returns an empty slice — jsretry floors an empty schedule at a 1ms
// nak, which would burn the retry lane's MaxDeliver in milliseconds.
func SlowBackoff(fastSteps int, full []time.Duration) []time.Duration {
	if len(full) == 0 {
		return jsretryFallback()
	}
	if fastSteps < 0 {
		fastSteps = 0
	}
	if fastSteps >= len(full) {
		// Everything is a fast rung; reuse the last entry so the lane still paces.
		return []time.Duration{full[len(full)-1]}
	}
	return full[fastSteps:]
}

// jsretryFallback is the schedule used when a caller supplies none.
func jsretryFallback() []time.Duration {
	return []time.Duration{2 * time.Minute, 10 * time.Minute}
}

// ConsumerConfig is the retry lane's durable consumer for one service, filtered
// to its own escalations. Built through stream.DurableConsumerDefaults so the
// derived BackOff and AckWait cannot disagree (a hardcoded cc.BackOff is a
// blocking semgrep finding).
func ConsumerConfig(siteID, consumer string, s Settings) jetstream.ConsumerConfig {
	cc := stream.DurableConsumerDefaults(s.Consumer)
	cc.Durable = DurableName(consumer)
	cc.FilterSubjects = []string{subject.RetryConsumerWildcard(siteID, consumer)}
	return cc
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=retrylane`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/retrylane/consumer.go pkg/retrylane/consumer_test.go
git commit -m "feat(retrylane): retry-consumer settings and binding helper

The env surface is a split index, not a schedule — a raw []time.Duration
knob has no off-switch under caarlos0/env and would let an operator
silently redefine the retry budget. SlowBackoff is the tail the fast rungs
left, so fast + slow is exactly the original schedule."
```

---

### Task 8: `pkg/retrylane` — end-to-end escalation against real NATS

**Files:**
- Create: `pkg/retrylane/retrylane_integration_test.go`

**Interfaces:**
- Consumes: everything from Tasks 2–7; `testutil.NATS(t)`.
- Produces: nothing — this is the proof that the unit-level pieces compose.

- [ ] **Step 1: Write the failing test**

Create `pkg/retrylane/retrylane_integration_test.go`:

```go
//go:build integration

package retrylane_test

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/retrylane"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

// TestEscalationRoundTrip proves the pieces compose: a message that exhausts
// its fast budget on a hot consumer lands on the RETRY stream, addressed to
// exactly one consumer, with a byte-identical body.
func TestEscalationRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	nc, err := nats.Connect(testutil.NATS(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = nc.Drain() })

	js, err := jetstream.New(nc)
	require.NoError(t, err)

	const siteID = "rlit"
	const consumerName = "test-worker"

	hot := stream.Config{Name: "HOT-" + siteID, Subjects: []string{"hot." + siteID + ".>"}}
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{Name: hot.Name, Subjects: hot.Subjects})
	require.NoError(t, err)
	t.Cleanup(func() { _ = js.DeleteStream(context.Background(), hot.Name) })

	retryCfg := stream.Retry(siteID)
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{Name: retryCfg.Name, Subjects: retryCfg.Subjects})
	require.NoError(t, err)
	t.Cleanup(func() { _ = js.DeleteStream(context.Background(), retryCfg.Name) })

	body := []byte(`{"msg":"hello","n":1}`)
	_, err = js.Publish(ctx, "hot."+siteID+".created", body)
	require.NoError(t, err)

	hotCons, err := js.CreateOrUpdateConsumer(ctx, hot.Name, jetstream.ConsumerConfig{
		Durable:       consumerName,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckWait:       2 * time.Second,
		MaxDeliver:    10,
		MaxAckPending: 100,
	})
	require.NoError(t, err)

	lane := &retrylane.Lane{
		Consumer:  consumerName,
		SiteID:    siteID,
		Enabled:   true,
		FastSteps: 2,
		Publish: func(ctx context.Context, subj string, data []byte, hdr nats.Header, msgID string) error {
			_, pubErr := js.PublishMsg(ctx, &nats.Msg{
				Subject: subj,
				Data:    data,
				Header:  hdr,
			}, jetstream.WithMsgID(msgID))
			return pubErr
		},
	}

	// Fail every delivery on a short backoff; deliveries 1-2 nak in place,
	// delivery 3 escalates.
	fastBackoff := []time.Duration{200 * time.Millisecond, 200 * time.Millisecond}
	handlerErr := assert.AnError

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		msg, fetchErr := hotCons.Next(jetstream.FetchMaxWait(2 * time.Second))
		if fetchErr != nil {
			continue
		}
		lane.Settle(ctx, msg, fastBackoff, handlerErr)

		retryStream, sErr := js.Stream(ctx, retryCfg.Name)
		require.NoError(t, sErr)
		info, iErr := retryStream.Info(ctx)
		require.NoError(t, iErr)
		if info.State.Msgs > 0 {
			break
		}
	}

	retryStream, err := js.Stream(ctx, retryCfg.Name)
	require.NoError(t, err)
	got, err := retryStream.GetMsg(ctx, 1)
	require.NoError(t, err, "the message must have escalated onto the RETRY stream")

	assert.Equal(t, subject.Retry(siteID, consumerName, subject.RetryTierSlow), got.Subject,
		"the escalation must be addressed to exactly one consumer")
	assert.Equal(t, body, got.Data, "the body must survive escalation byte-identically")
	assert.Equal(t, consumerName, got.Header.Get(retrylane.HeaderConsumer))
	assert.Equal(t, hot.Name, got.Header.Get(retrylane.HeaderOriginStream))
	assert.NotEmpty(t, got.Header.Get(retrylane.HeaderFirstFailedAt))
}
```

- [ ] **Step 2: Run the test**

Run: `make test-integration SERVICE=retrylane`
Expected: PASS

If `retryStream.GetMsg` is unavailable on the `o11ynats.Stream` wrapper, use the raw `jetstream.Stream` from `js.Stream(...)` — this test builds `js` directly via `jetstream.New(nc)`, so it already holds the raw interface.

- [ ] **Step 3: Commit**

```bash
git add pkg/retrylane/retrylane_integration_test.go
git commit -m "test(retrylane): end-to-end escalation onto the RETRY stream

Proves the pieces compose against a real consumer: a message that exhausts
its fast budget lands on RETRY addressed to exactly one consumer, with a
byte-identical body and intact provenance headers."
```

---

### Task 9: Wire `notification-worker` (phase 2 canary)

Lowest blast radius of the three adopters: not the persistence path, not user-visible delivery.

**Files:**
- Modify: `notification-worker/config.go` (or wherever its `Config` struct lives — check `main.go` first)
- Modify: `notification-worker/main.go:448` (the `jsretry.Settle` call) and the consumer setup nearby
- Modify: `notification-worker/bootstrap.go`
- Modify: `notification-worker/deploy/docker-compose.yml`
- Test: `notification-worker/main_test.go` or the existing config test file

**Interfaces:**
- Consumes: `retrylane.Lane`, `retrylane.Settings`, `retrylane.ConsumerConfig`, `retrylane.SlowBackoff`, `stream.Retry` (Tasks 3–7).
- Produces: the wiring pattern Tasks 10–11 copy.

- [ ] **Step 1: Locate the config struct and the consume loop**

Run: `grep -n "type Config struct" -A 30 notification-worker/*.go | head -50`
Run: `grep -n "Consumer stream.ConsumerSettings\|CreateOrUpdateConsumer\|jsretry.Settle" notification-worker/main.go`

Note the exact line numbers; the edits below attach to them.

- [ ] **Step 2: Write the failing config test**

Append to the service's config test file (match its existing package and style):

```go
func TestConfigRetryLaneDefaultsOff(t *testing.T) {
	t.Setenv("SITE_ID", "site1")
	// Set every other required env var the existing config tests set — copy the
	// setup from the nearest existing TestConfig* in this file.

	cfg, err := parseConfig()
	require.NoError(t, err)

	assert.False(t, cfg.Retry.Enabled, "the retry lane must be opt-in per service")
	assert.Equal(t, 3, cfg.Retry.FastSteps)
	assert.Equal(t, 4000, cfg.Retry.Consumer.MaxAckPending,
		"the retry lane holds the long waits and needs its own large budget")
}
```

Replace `parseConfig()` with whatever the service actually calls (`env.ParseAs[Config]()` or a named helper) — check with `grep -n "env.Parse" notification-worker/*.go`.

- [ ] **Step 3: Run test to verify it fails**

Run: `make test SERVICE=notification-worker`
Expected: FAIL — `cfg.Retry` undefined

- [ ] **Step 4: Add the config field**

In the service's `Config` struct, beside the existing `Consumer stream.ConsumerSettings`:

```go
	// Retry is the tiered-redelivery lane; disabled by default. See pkg/retrylane.
	Retry retrylane.Settings `envPrefix:"RETRY_"`
```

Then override the retry consumer's ack-pending default for this service in `deploy/docker-compose.yml`:

```yaml
      - RETRY_LANE_ENABLED=${RETRY_LANE_ENABLED:-false}
      - RETRY_CONSUMER_MAX_ACK_PENDING=${RETRY_CONSUMER_MAX_ACK_PENDING:-4000}
      - RETRY_CONSUMER_MAX_DELIVER=${RETRY_CONSUMER_MAX_DELIVER:-3}
```

`stream.ConsumerSettings`'s struct-tag `envDefault` for `MaxAckPending` is 1000 — the hot lane's number — and a struct tag cannot express a different default for the embedded copy. So apply the retry lane's defaults right after env parsing, wherever the service builds its `Config`:

```go
	cfg.Retry = cfg.Retry.ApplyDefaults()
```

`ApplyDefaults` (Task 7) fills in `DefaultConsumerSettings()` only when the operator set nothing, so an explicit `RETRY_CONSUMER_MAX_ACK_PENDING` still wins.

- [ ] **Step 5: Run test to verify it passes**

Run: `make test SERVICE=notification-worker`
Expected: PASS

- [ ] **Step 6: Bootstrap the RETRY stream in dev**

In `notification-worker/bootstrap.go`, add the RETRY stream to the enabled branch (dev/integration only — production streams are ops-owned):

```go
		retryCfg := stream.Retry(siteID)
		if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:     retryCfg.Name,
			Subjects: retryCfg.Subjects,
		}); err != nil {
			return fmt.Errorf("create stream %s: %w", retryCfg.Name, err)
		}
```

Match the existing helper's signature — it takes explicit stream/subject params, so either extend the parameter list or read `siteID` the way the function already does. Check with `sed -n '20,60p' notification-worker/bootstrap.go` before editing.

- [ ] **Step 7: Build the Lane and swap the Settle call**

In `main.go`, after the JetStream handle exists and before the consume loop:

```go
	retryLane := &retrylane.Lane{
		Consumer:  consumerName, // the same string used as the hot consumer's Durable
		SiteID:    cfg.SiteID,
		Enabled:   cfg.Retry.Enabled,
		FastSteps: cfg.Retry.FastSteps,
		Publish: func(ctx context.Context, subj string, data []byte, hdr nats.Header, msgID string) error {
			_, err := js.PublishMsg(ctx, &nats.Msg{Subject: subj, Data: data, Header: hdr},
				jetstream.WithMsgID(msgID))
			return err
		},
	}
```

Then replace `notification-worker/main.go:448`:

```go
					jsretry.Settle(handlerCtx, msg, jsretry.DefaultBackoff, handler.HandleMessage(handlerCtx, msg.Data()))
```

with:

```go
					retryLane.Settle(handlerCtx, msg, fastBackoff, handler.HandleMessage(handlerCtx, msg.Data()))
```

where `fastBackoff` is computed once near the Lane:

```go
	// The fast rungs stay in place; SlowBackoff's tail runs on the retry lane.
	fastBackoff := jsretry.DefaultBackoff
	if cfg.Retry.Enabled {
		fastBackoff = jsretry.DefaultBackoff[:cfg.Retry.FastSteps]
	}
```

> `msg` at that point is the `natsmetrics`-tracked message, which embeds `jetstream.Msg` and therefore already satisfies `retrylane.Msg`. If the compiler disagrees, check what `consumerMetrics.Track` returns and whether it exposes `Data()`, `Subject()` and `Headers()` — it embeds `jetstream.Msg`, so it should.

- [ ] **Step 8: Label the escalation in metrics**

`Lane.OnEscalate` and `Lane.WithEscalationHook` already exist from Task 6. Wire them so an escalation does not read as a success.

Inside the per-message goroutine, where `tracked` (the `*natsmetrics.Message`) is in scope, derive a per-message lane rather than mutating the shared one — the hook closes over one delivery's recorder, so setting it on the shared `Lane` would be a data race across message goroutines:

```go
				perMsgLane := retryLane.WithEscalationHook(tracked.Escalated)
```

and settle through `perMsgLane` in Step 7's call site instead of `retryLane`.

Run: `make test SERVICE=notification-worker` with `-race` (the Makefile already passes it).
Expected: PASS, no race detected.

- [ ] **Step 9: Bind the retry consumer and its consume loop**

In `main.go`, after the hot consumer is created, add the second consumer. Copy the hot loop's shape — same semaphore pattern, same `jobguard.Run`, same tracking — with two differences: `retrylane.ConsumerConfig` for the config, and `retrylane.SlowBackoff(...)` for the schedule, settled with plain `jsretry.Settle` (the retry lane does not escalate again in phases 0–3):

```go
	retryCons, err := js.CreateOrUpdateConsumer(ctx, stream.Retry(cfg.SiteID).Name,
		retrylane.ConsumerConfig(cfg.SiteID, consumerName, cfg.Retry))
	if err != nil {
		slog.Error("failed to create retry consumer", "error", err)
		os.Exit(1)
	}

	slowBackoff := retrylane.SlowBackoff(cfg.Retry.FastSteps, jsretry.DefaultBackoff)
```

Then a consume loop mirroring the hot one, settling with:

```go
					jsretry.Settle(handlerCtx, msg, slowBackoff, handler.HandleMessage(handlerCtx, msg.Data()))
```

**Important — the rollback asymmetry from spec §4:** bind and drain this consumer **regardless of `cfg.Retry.Enabled`**. Disabling stops new escalations; it must not strand messages already parked on RETRY. Guard only the *escalation* on the flag, never the drain.

Its `iter.Stop()` must join the existing shutdown sequence: `iter.Stop()` → `wg.Wait()` → `nc.Drain()` (CLAUDE.md §6).

- [ ] **Step 10: Verify the service builds and its tests pass**

Run: `make build SERVICE=notification-worker`
Run: `make test SERVICE=notification-worker`
Run: `make lint && make sast`
Expected: all clean.

- [ ] **Step 11: Commit**

```bash
git add notification-worker/ pkg/retrylane/
git commit -m "feat(notification-worker): wire the retry lane, disabled by default

Phase 2 canary — lowest blast radius of the three adopters: not the
persistence path, not user-visible delivery. The escalation is gated on
RETRY_LANE_ENABLED, but the retry consumer binds and drains unconditionally
so flipping the flag off cannot strand messages already parked on RETRY."
```

---

### Task 10: Wire `message-worker` (phase 3)

**Files:**
- Modify: `message-worker/main.go`, its config file, `message-worker/handler.go:89`, `message-worker/bootstrap.go`, `message-worker/deploy/docker-compose.yml`

**Interfaces:**
- Consumes: the exact pattern established in Task 9.

Two differences from Task 9, both important:

- The Settle call is at `message-worker/handler.go:89`, **inside the handler** rather than in `main.go`, so the `*retrylane.Lane` is injected into the handler struct via its constructor (accept interfaces, return structs — CLAUDE.md §3).
- `message-worker` also settles at `teamsbatch.go:64,71,74` on the **Teams migration** path. **Leave those on plain `jsretry.Settle`** — they consume MESSAGES-TEAMS, a different stream with batch semantics, and are out of scope.

- [ ] **Step 1: Write the failing handler test**

Add to `message-worker/handler_test.go`. The doubles are re-declared here because Go test helpers cannot cross packages:

```go
// retryFakeMsg is a retrylane.Msg double for the escalation tests.
type retryFakeMsg struct {
	data         []byte
	subject      string
	headers      nats.Header
	stream       string
	seq          uint64
	numDelivered uint64
	acked        bool
	naked        bool
}

func (m *retryFakeMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{
		Stream:       m.stream,
		NumDelivered: m.numDelivered,
		Sequence:     jetstream.SequencePair{Stream: m.seq},
	}, nil
}
func (m *retryFakeMsg) Ack() error                         { m.acked = true; return nil }
func (m *retryFakeMsg) NakWithDelay(_ time.Duration) error { m.naked = true; return nil }
func (m *retryFakeMsg) Data() []byte                       { return m.data }
func (m *retryFakeMsg) Subject() string                    { return m.subject }
func (m *retryFakeMsg) Headers() nats.Header               { return m.headers }

func TestHandlerEscalatesWhenFastBudgetSpent(t *testing.T) {
	var (
		published bool
		gotSubj   string
		gotData   []byte
	)
	lane := &retrylane.Lane{
		Consumer:  "message-worker",
		SiteID:    "site1",
		Enabled:   true,
		FastSteps: 3,
		Publish: func(_ context.Context, subj string, data []byte, _ nats.Header, _ string) error {
			published = true
			gotSubj = subj
			gotData = data
			return nil
		},
	}

	body := []byte(`{"messageId":"abc"}`)
	msg := &retryFakeMsg{
		data:         body,
		subject:      "chat.msg.canonical.site1.created",
		headers:      nats.Header{},
		stream:       "MESSAGES-CANONICAL-site1",
		seq:          7,
		numDelivered: 4, // past FastSteps=3
	}

	lane.Settle(context.Background(), msg, jsretry.DefaultBackoff[:3], errors.New("cassandra unavailable"))

	require.True(t, published, "delivery 4 must escalate rather than park another 12 minutes")
	assert.Equal(t, "chat.retry.site1.message-worker.slow", gotSubj)
	assert.Equal(t, body, gotData, "the body must reach Cassandra byte-identical after a replay")
	assert.True(t, msg.acked)
	assert.False(t, msg.naked)
}

func TestHandlerStillNaksWithinFastBudget(t *testing.T) {
	lane := &retrylane.Lane{
		Consumer: "message-worker", SiteID: "site1", Enabled: true, FastSteps: 3,
		Publish: func(_ context.Context, _ string, _ []byte, _ nats.Header, _ string) error {
			t.Fatal("must not escalate within the fast budget")
			return nil
		},
	}
	msg := &retryFakeMsg{
		data: []byte(`{}`), subject: "s", headers: nats.Header{},
		stream: "MESSAGES-CANONICAL-site1", seq: 1, numDelivered: 2,
	}

	lane.Settle(context.Background(), msg, jsretry.DefaultBackoff[:3], errors.New("cassandra unavailable"))

	assert.True(t, msg.naked)
	assert.False(t, msg.acked)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=message-worker`
Expected: FAIL — `retrylane` not imported / handler has no lane.

- [ ] **Step 3: Apply Task 9's Steps 1, 4, 6, 7, 8, 9 to `message-worker`**

Identical in shape to Task 9 — the config field (`Retry retrylane.Settings` with `envPrefix:"RETRY_"` plus `cfg.Retry = cfg.Retry.ApplyDefaults()`), the `bootstrap.go` RETRY stream in the dev-only branch, the `Lane` construction with the `js.PublishMsg` publish func, the per-message `WithEscalationHook(tracked.Escalated)`, the retry consumer bound via `retrylane.ConsumerConfig` and drained **unconditionally** regardless of the flag, and the `deploy/docker-compose.yml` env block.

The one structural difference: pass the lane into `NewHandler(...)` and store it on the handler struct, then replace `handler.go:89`'s `jsretry.Settle(ctx, msg, jsretry.DefaultBackoff, ...)` with the lane call using the fast prefix.

- [ ] **Step 4: Verify**

Run: `make build SERVICE=message-worker && make test SERVICE=message-worker && make lint`
Expected: clean, and `teamsbatch.go` untouched — confirm with `git diff message-worker/teamsbatch.go` returning empty.

- [ ] **Step 3: Commit**

```bash
git add message-worker/
git commit -m "feat(message-worker): wire the retry lane, disabled by default

The Teams migration path (teamsbatch.go) stays on plain jsretry — a
different stream with batch semantics, out of scope."
```

---

### Task 11: Wire `broadcast-worker` (phase 3)

**Files:**
- Modify: `broadcast-worker/main.go:446`, its config file, `broadcast-worker/bootstrap.go`, `broadcast-worker/deploy/docker-compose.yml`

**Interfaces:**
- Consumes: the pattern from Task 9.

Two differences from Task 9:

- `broadcast-worker/main.go:446` settles with **`jsretry.LowLatencyBackoff`**, not `DefaultBackoff`. Its fast rungs are `LowLatencyBackoff[:FastSteps]` and its slow tail is `retrylane.SlowBackoff(cfg.Retry.FastSteps, jsretry.LowLatencyBackoff)`. Do **not** switch it to `DefaultBackoff` — this is the user-visible fan-out path and its first retry is deliberately sub-second.
- `broadcast-worker/handler.go:490` publishes to OUTBOX via `outbox.Publish`. That is the federation relay, **not** the retry lane. Leave it untouched.

- [ ] **Step 1: Write the failing test**

Add to `broadcast-worker/main_test.go` (or the nearest existing test file in the package), using the `retryFakeMsg` double from Task 10 re-declared here:

```go
func TestRetryLaneUsesLowLatencyScheduleNotDefault(t *testing.T) {
	fast := jsretry.LowLatencyBackoff[:3]
	slow := retrylane.SlowBackoff(3, jsretry.LowLatencyBackoff)

	assert.Equal(t, 200*time.Millisecond, fast[0],
		"the fan-out path's first retry stays sub-second — a user is waiting on it")

	var total time.Duration
	for _, d := range append(append([]time.Duration{}, fast...), slow...) {
		total += d
	}
	var original time.Duration
	for _, d := range jsretry.LowLatencyBackoff {
		original += d
	}
	assert.Equal(t, original, total, "the budget is relocated, not redefined")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=broadcast-worker`
Expected: FAIL — `retrylane` not imported.

- [ ] **Step 3: Apply Task 9's Steps 1, 4, 6, 7, 8, 9 to `broadcast-worker`**

Same shape as Task 9 — config field with `ApplyDefaults`, `bootstrap.go` RETRY stream in the dev-only branch, `Lane` with the `js.PublishMsg` publish func, per-message `WithEscalationHook(tracked.Escalated)`, retry consumer via `retrylane.ConsumerConfig` drained **unconditionally**, and the compose env block — but with `LowLatencyBackoff` as the base schedule everywhere `DefaultBackoff` appeared.

- [ ] **Step 4: Verify**

Run: `make build SERVICE=broadcast-worker && make test SERVICE=broadcast-worker && make lint`
Expected: clean, and `git diff broadcast-worker/handler.go` shows no change around line 490.

- [ ] **Step 3: Commit**

```bash
git add broadcast-worker/
git commit -m "feat(broadcast-worker): wire the retry lane, disabled by default

Keeps LowLatencyBackoff as the base schedule — this is the user-visible
fan-out path and its first retry is deliberately sub-second, so the split
is taken from that schedule, not DefaultBackoff."
```

---

### Task 12: Documentation

Two existing documents become wrong the moment this ships. Leaving them stale would mislead whoever is holding the pager.

**Files:**
- Modify: `tools/observability/METRICS.md:101`
- Modify: `CLAUDE.md` §6 "JetStream Redelivery Backoff"

**Interfaces:**
- Consumes: the shipped behaviour of Tasks 2–11.

- [ ] **Step 1: Fix the METRICS.md runbook row**

`tools/observability/METRICS.md:101` currently reads that a dependency outage past the retry budget means "**Not healed — abandoned** … nothing auto-routes them (no DLQ)." On a participating consumer that is now false. Replace the row's disposition text with:

```markdown
| A dependency outage lasts longer than the retry budget | `num_redelivered` ↑ then falls to 0 **while** `chat_nats_terminal_failures_total{reason="max_deliver"}` ↑ | **On a consumer without the retry lane: not healed — abandoned.** Messages exhausted MaxDeliver; nothing auto-routes them. On a consumer with `RETRY_LANE_ENABLED=true`, look at `outcome="escalated"` instead: the message moved to `RETRY-{siteID}` after its fast rungs and is being retried on the slow schedule there. Escalation rate is the early signal — it moves ~36s into an incident, versus ~12.6 min for a max_deliver drop. |
```

Also add a row for the new outcome in the metrics table near `outcome` values:

```markdown
| `outcome="escalated"` | consumer | an Ack that handed the message to `RETRY-{siteID}`, not a completion | Rising escalations mean a dependency is failing faster than the fast rungs absorb. This is the **incident** alert: it fires minutes before any terminal-failure counter moves. |
```

- [ ] **Step 2: Update CLAUDE.md §6**

The section documents two levers. Add the third, after the `pkg/jsretry` bullet:

```markdown
- **`pkg/retrylane`** (client-side escalation) fires when a handler's transient failures
  exhaust the in-place fast rungs. It republishes the message to `RETRY-{siteID}` and Acks,
  so the long waits stop occupying the hot consumer's ack-pending budget — a Nak'd-with-delay
  message holds its slot for the whole backoff, so at ~5 failures/s the default 1000-slot
  budget is exhausted in ~200s and the consumer stalls for healthy traffic too. The total
  retry budget is unchanged; only the occupancy moves. Opt-in per service via
  `RETRY_LANE_ENABLED`; excluded by design from the FIFO lanes (`outbox-worker` ordered
  consumers, `hr-sync-worker`) and from `search-sync-worker`, which rely on delivery order
  that escalation does not preserve.
```

- [ ] **Step 3: Verify no client-facing doc change is needed**

Run: `git log --oneline <plan-base>..HEAD -- docs/client-api.md docs/client-api/ pkg/model/`

where `<plan-base>` is the commit this plan's own work started from (`0887bf1` for the original run), **not** `main`. Expected: empty. Nothing client-facing moves — no handler registration, no `pkg/model` request/reply or event struct changed. If it returns commits, stop and reconcile: a client-facing change requires updating `docs/client-api.md` and its derived views in the same PR.

> Do **not** diff against `main` here. This is a long-lived integration branch — during the original run it sat 42 commits ahead of `main`, ~40 of them unrelated pre-existing feature work that had already touched `docs/client-api.md`, `events.md` and `request-reply.md`. Diffing against `main` measures that drift rather than this plan's work and produces a false positive that blocks the task.

- [ ] **Step 4: Commit**

```bash
git add tools/observability/METRICS.md CLAUDE.md
git commit -m "docs: record the retry lane as the third redelivery lever

METRICS.md's 'abandoned — no DLQ' runbook row is false on a participating
consumer and would mislead an on-call engineer; it now points at the
escalated outcome, which moves ~36s into an incident rather than ~12.6 min."
```

---

## Verification Before Done

- [ ] `make lint` clean
- [ ] `make test` clean (all services)
- [ ] `make test-integration SERVICE=retrylane` and `SERVICE=stream` pass
- [ ] `make sast` clean — no `jsretry-no-bare-nak` / `jsretry-no-zero-nak-delay` / `jsretry-no-hardcoded-consumer-backoff` findings
- [ ] `pkg/retrylane` coverage ≥ 90%
- [ ] `RETRY_LANE_ENABLED` defaults to `false` in all three services — confirm with `grep -rn "LANE_ENABLED" --include="*.go" --include="*.yml" .`
- [ ] Task 1's gate passed and the result is recorded in its commit message

## Ops Handoff — Required Before Enabling the Flag Anywhere

These are IaC-owned and **cannot be enforced from this repo** (`pkg/stream.Config` carries only `Name` and `Subjects`). Code merging with `RETRY_LANE_ENABLED=false` is safe without them; flipping the flag on in an environment is not.

1. **`RETRY-{siteID}` provisioned** with `R3 + file`, for the same reason OUTBOX is.
2. **`RETRY-{siteID}` `Duplicates` (dedup window) ≥ the escalation window.** `pkg/retrylane.DedupID` is what makes the publish-then-Ack escalation safe against a crash in that gap. Set too short, the exactly-once escalation silently degrades to at-least-once and a crash mid-escalation produces a duplicate — absorbed by handler idempotency, but no longer guaranteed away. Spec §6.1.
3. **`RETRY-{siteID}` retention** long enough to outlast the slow schedule plus a deploy window, so a rolling restart cannot drop parked retries.

Raise these with whoever owns the JetStream IaC before phase 2 goes live in any environment, and record the agreed values in the spec's §6.

## Out of Scope — Phase 4, Separate PR

`DLQ-{siteID}` stream · `dlq-worker` · the Mongo triage record and its content rule · `tools/dlqreplay` · admin-service DLQ endpoints · `TerminalDeadLettered` · the DLQ `MaxAge` governance sign-off (spec §6.2) · the operator runbook's replay path (spec §3.12).

Until that lands, a message that exhausts the retry lane's own `MaxDeliver` is dropped exactly as it is today, with the existing `max_deliver` terminal metric. **That is not a regression** — it is the current behaviour, reached ~12.6 min in as before, with the stall fixed in front of it.
