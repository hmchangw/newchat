# JetStream Exponential Backoff Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give every JetStream consumer in the repo an exponential-backoff redelivery schedule by default, anchored so it cannot collapse `AckWait`, and fix the five consumers that currently hot-loop on failure.

**Architecture:** Two disjoint levers. Lever A is the server-side consumer `BackOff`, derived in `pkg/stream` from `AckWait × Factor^i` so `BackOff[0] == AckWait` by construction. Lever B is the client-side `pkg/jsretry` `NakWithDelay` schedule, which gains AWS equal jitter and a settle-less `Nak` helper. Five services that call bare `Nak()` / `NakWithDelay(0)` migrate to `jsretry.Nak`.

**Tech Stack:** Go 1.25, `nats.go/jetstream` v1.50.0, `caarlos0/env/v11`, `testify`, `testcontainers-go` via `pkg/testutil`.

**Spec:** `docs/superpowers/specs/2026-08-21-jetstream-exponential-backoff-design.md`

## Global Constraints

- All commands go through `make` targets — never raw `go` commands. `make test SERVICE=<name>`, `make lint`, `make fmt`, `make sast`.
- TDD is mandatory: write the failing test, run it, confirm it fails, then implement.
- Minimum 80% coverage per package; `pkg/stream` and `pkg/jsretry` target 90%+ as shared `pkg/` code.
- `log/slog` only, structured key-value fields, never interpolated strings.
- gosec is a blocking CI gate. `//nolint:gosec` does NOT suppress standalone gosec — use `// #nosec <RULE> -- reason` on the line directly above the statement.
- Never edit `mock_store_test.go` by hand.
- Branch: `claude/jetstream-exponential-backoff-shhdsi`. Commit after each task.
- Agreed default schedule: `{30s, 1m, 2m, 4m, 8m}` over `MaxDeliver=6` — 15.5 min budget.
- `jsretry.DefaultBackoff` becomes `{1s, 5s, 30s, 2m, 10m}`; `jsretry.LowLatencyBackoff` stays `{200ms, 1s, 5s, 30s}` unchanged.

**Server-side facts this plan depends on** (verified against `nats-io/nats-server` `server/consumer.go`, main):
- `consumer.go:677-682` — `if len(config.BackOff) > 0 { config.AckWait = config.BackOff[0] }`
- `consumer.go:807` and `:2588` — `len(BackOff) > MaxDeliver` is a hard error unless `MaxDeliver == -1`
- `consumer.go:3308-3311` — a bare `-NAK` goes straight on the redeliver queue; `BackOff` is never consulted
- `nats.go` `jetstream/message.go:374` — the client only serializes a nak delay when it is `> 0`, so `NakWithDelay(0)` sends a bare `-NAK`

---

### Task 1: `pkg/stream` — derive the BackOff schedule from AckWait

**Files:**
- Modify: `pkg/stream/consumer.go`
- Test: `pkg/stream/consumer_test.go`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `stream.ConsumerSettings` gains three fields — `BackOffSteps int`, `BackOffFactor float64`, `BackOffMax time.Duration`. `stream.DurableConsumerDefaults(s ConsumerSettings) jetstream.ConsumerConfig` keeps its exact signature and now populates `cc.BackOff`.

- [ ] **Step 1: Write the failing tests**

Append to `pkg/stream/consumer_test.go`:

```go
func TestDurableConsumerDefaults_BackOff(t *testing.T) {
	tests := []struct {
		name string
		s    stream.ConsumerSettings
		want []time.Duration
	}{
		{
			name: "agreed default schedule",
			s: stream.ConsumerSettings{
				AckWait: 30 * time.Second, MaxDeliver: 6,
				BackOffSteps: 5, BackOffFactor: 2, BackOffMax: 8 * time.Minute,
			},
			want: []time.Duration{30 * time.Second, time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute},
		},
		{
			name: "BackOffMax caps and every later step stays at the cap",
			s: stream.ConsumerSettings{
				AckWait: 30 * time.Second, MaxDeliver: 6,
				BackOffSteps: 5, BackOffFactor: 2, BackOffMax: 2 * time.Minute,
			},
			want: []time.Duration{30 * time.Second, time.Minute, 2 * time.Minute, 2 * time.Minute, 2 * time.Minute},
		},
		{
			name: "steps zero disables BackOff — the operator off-switch",
			s: stream.ConsumerSettings{
				AckWait: 30 * time.Second, MaxDeliver: 6,
				BackOffSteps: 0, BackOffFactor: 2, BackOffMax: 8 * time.Minute,
			},
			want: nil,
		},
		{
			name: "steps beyond MaxDeliver are clamped — the server rejects the excess",
			s: stream.ConsumerSettings{
				AckWait: 30 * time.Second, MaxDeliver: 3,
				BackOffSteps: 5, BackOffFactor: 2, BackOffMax: 8 * time.Minute,
			},
			want: []time.Duration{30 * time.Second, time.Minute, 2 * time.Minute},
		},
		{
			name: "MaxDeliver -1 exempts the clamp",
			s: stream.ConsumerSettings{
				AckWait: 30 * time.Second, MaxDeliver: -1,
				BackOffSteps: 5, BackOffFactor: 2, BackOffMax: 8 * time.Minute,
			},
			want: []time.Duration{30 * time.Second, time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute},
		},
		{
			name: "factor below 1 flattens rather than shrinking",
			s: stream.ConsumerSettings{
				AckWait: 30 * time.Second, MaxDeliver: 6,
				BackOffSteps: 3, BackOffFactor: 0.5, BackOffMax: 8 * time.Minute,
			},
			want: []time.Duration{30 * time.Second, 30 * time.Second, 30 * time.Second},
		},
		{
			name: "zero AckWait yields no schedule",
			s: stream.ConsumerSettings{
				AckWait: 0, MaxDeliver: 6,
				BackOffSteps: 5, BackOffFactor: 2, BackOffMax: 8 * time.Minute,
			},
			want: nil,
		},
		{
			name: "zero BackOffMax leaves the schedule uncapped",
			s: stream.ConsumerSettings{
				AckWait: time.Second, MaxDeliver: 6,
				BackOffSteps: 4, BackOffFactor: 2, BackOffMax: 0,
			},
			want: []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cc := stream.DurableConsumerDefaults(tt.s)
			assert.Equal(t, tt.want, cc.BackOff)
		})
	}
}

// The whole point of deriving the schedule: nats-server overwrites AckWait with
// BackOff[0] (server/consumer.go:677-682), so if they ever disagreed the
// consumer would silently run a shorter AckWait than configured.
func TestDurableConsumerDefaults_BackOffHeadEqualsAckWait(t *testing.T) {
	for _, ackWait := range []time.Duration{time.Second, 30 * time.Second, 2 * time.Minute} {
		cc := stream.DurableConsumerDefaults(stream.ConsumerSettings{
			AckWait: ackWait, MaxDeliver: 6,
			BackOffSteps: 5, BackOffFactor: 2, BackOffMax: 8 * time.Minute,
		})
		require.NotEmpty(t, cc.BackOff)
		assert.Equal(t, cc.AckWait, cc.BackOff[0], "BackOff[0] must equal AckWait for ackWait=%s", ackWait)
	}
}

// The server errors with JSConsumerMaxDeliverBackoffError when the schedule is
// longer than MaxDeliver (server/consumer.go:807, :2588).
func TestDurableConsumerDefaults_BackOffNeverExceedsMaxDeliver(t *testing.T) {
	for steps := 1; steps <= 12; steps++ {
		cc := stream.DurableConsumerDefaults(stream.ConsumerSettings{
			AckWait: 30 * time.Second, MaxDeliver: 6,
			BackOffSteps: steps, BackOffFactor: 2, BackOffMax: 8 * time.Minute,
		})
		assert.LessOrEqual(t, len(cc.BackOff), 6, "steps=%d", steps)
	}
}
```

Replace the body of `TestConsumerSettingsEnvDefaults` with:

```go
	assert.Equal(t, 30*time.Second, h.Consumer.AckWait)
	assert.Equal(t, 6, h.Consumer.MaxDeliver)
	assert.Equal(t, 512, h.Consumer.MaxWaiting)
	assert.Equal(t, 1000, h.Consumer.MaxAckPending)
	assert.Equal(t, 5, h.Consumer.BackOffSteps)
	assert.InDelta(t, 2.0, h.Consumer.BackOffFactor, 0.0001)
	assert.Equal(t, 8*time.Minute, h.Consumer.BackOffMax)
```

Append to `TestConsumerSettingsEnvOverrides`, before `env.Parse`:

```go
	t.Setenv("CONSUMER_BACKOFF_STEPS", "3")
	t.Setenv("CONSUMER_BACKOFF_FACTOR", "3")
	t.Setenv("CONSUMER_BACKOFF_MAX", "1m")
```

and after it:

```go
	assert.Equal(t, 3, h.Consumer.BackOffSteps)
	assert.InDelta(t, 3.0, h.Consumer.BackOffFactor, 0.0001)
	assert.Equal(t, time.Minute, h.Consumer.BackOffMax)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test SERVICE=stream`
Expected: FAIL — `unknown field BackOffSteps in struct literal`, and `TestConsumerSettingsEnvDefaults` failing on `MaxDeliver` 5 vs 6.

- [ ] **Step 3: Implement the derivation**

In `pkg/stream/consumer.go`, add `"log/slog"` to the imports, extend the struct, and add the schedule builder.

Replace the `ConsumerSettings` struct with:

```go
// ConsumerSettings holds the env-driven knobs for durable JetStream
// consumers. Embed in each service's Config with envPrefix:"CONSUMER_".
//
// Defaults are set on the struct tags so caarlos0/env supplies them when
// the env vars are unset. Operators tune per-service values via the
// service's deployment env (e.g. CONSUMER_MAX_ACK_PENDING).
type ConsumerSettings struct {
	AckWait       time.Duration `env:"ACK_WAIT"        envDefault:"30s"`
	MaxDeliver    int           `env:"MAX_DELIVER"     envDefault:"6"`
	MaxWaiting    int           `env:"MAX_WAITING"     envDefault:"512"`
	MaxAckPending int           `env:"MAX_ACK_PENDING" envDefault:"1000"`

	// BackOff schedule shape. Entry i is AckWait * Factor^i capped at Max, so
	// entry 0 is AckWait by construction — nats-server overwrites AckWait with
	// BackOff[0] (server/consumer.go:677-682) and the two must never disagree.
	// Steps=0 disables the schedule entirely (flat AckWait retry).
	BackOffSteps  int           `env:"BACKOFF_STEPS"  envDefault:"5"`
	BackOffFactor float64       `env:"BACKOFF_FACTOR" envDefault:"2"`
	BackOffMax    time.Duration `env:"BACKOFF_MAX"    envDefault:"8m"`
}
```

Add `BackOff: s.backOffSchedule(),` to the `jetstream.ConsumerConfig` literal returned by `DurableConsumerDefaults`, and extend that function's doc comment with:

```go
// BackOff is derived from AckWait via the BackOff* knobs; see backOffSchedule.
// It governs redelivery only for messages that go un-acked past AckWait — a
// handler that Naks is spaced by pkg/jsretry instead, because a bare -NAK
// bypasses BackOff entirely (server/consumer.go:3308-3311).
```

Then append:

```go
// backOffSchedule builds the redelivery schedule: entry i is AckWait*Factor^i,
// capped at BackOffMax. Returns nil when disabled, leaving flat AckWait retry.
//
// Steps are clamped to MaxDeliver because nats-server rejects a schedule longer
// than MaxDeliver outright (server/consumer.go:807), and a clamp with a warning
// beats a consumer that fails to create at startup.
func (s ConsumerSettings) backOffSchedule() []time.Duration {
	steps := s.BackOffSteps
	if steps <= 0 || s.AckWait <= 0 {
		return nil
	}
	if s.MaxDeliver != -1 && steps > s.MaxDeliver {
		slog.Warn("consumer backoff steps exceed MaxDeliver — clamping",
			"backoffSteps", steps, "maxDeliver", s.MaxDeliver)
		steps = s.MaxDeliver
	}
	if steps <= 0 {
		return nil
	}
	factor := s.BackOffFactor
	if factor < 1 {
		factor = 1
	}

	out := make([]time.Duration, steps)
	d := s.AckWait
	for i := range out {
		if s.BackOffMax > 0 && d > s.BackOffMax {
			d = s.BackOffMax
		}
		out[i] = d
		next := time.Duration(float64(d) * factor)
		if next < d {
			next = d // overflow guard
		}
		d = next
	}
	return out
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `make test SERVICE=stream`
Expected: PASS, including the pre-existing `TestDurableConsumerDefaults` "zero settings produce zero values" subtest — `ConsumerSettings{}` has `BackOffSteps == 0`, so `cc.BackOff` stays nil.

- [ ] **Step 5: Confirm the other 9 services still compile and pass**

Run: `make lint && make test`
Expected: PASS. Every per-service `consumer_config_test.go` builds `ConsumerSettings` as a struct literal without the new fields, so `BackOffSteps` is 0 and those consumers get `BackOff: nil` — no assertion changes needed. The one exception is `search-sync-worker`, handled in Task 3.

- [ ] **Step 6: Commit**

```bash
git add pkg/stream/consumer.go pkg/stream/consumer_test.go
git commit -m "feat(stream): derive consumer BackOff from AckWait

Entry i is AckWait*Factor^i capped at BackOffMax, so BackOff[0] equals
AckWait by construction and nats-server's AckWait=BackOff[0] overwrite
can never silently shorten it. Defaults give {30s,1m,2m,4m,8m} over
MaxDeliver=6. CONSUMER_BACKOFF_STEPS=0 is the per-service off-switch."
```

---

### Task 2: `pkg/jsretry` — equal jitter, longer tail, settle-less `Nak`

**Files:**
- Modify: `pkg/jsretry/jsretry.go`
- Test: `pkg/jsretry/jsretry_test.go`

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces: `jsretry.Nak(ctx context.Context, msg Msg, backoff []time.Duration, reason string)`. `jsretry.DefaultBackoff` becomes a 5-entry schedule. `backoffFor` now returns a jittered duration in `[base/2, base]`.

- [ ] **Step 1: Write the failing tests**

In `pkg/jsretry/jsretry_test.go`, replace `TestSettle_BackoffSelectedByAttempt`, `TestBackoffFor` and `TestBackoffFor_MetadataError` with jitter-aware versions, and add coverage for `Nak`:

```go
// assertJittered asserts d is within the equal-jitter band [base/2, base].
func assertJittered(t *testing.T, base, d time.Duration) {
	t.Helper()
	assert.GreaterOrEqual(t, d, base/2, "equal jitter guarantees at least half the base delay")
	assert.LessOrEqual(t, d, base, "jitter never exceeds the base delay")
}

func TestSettle_BackoffSelectedByAttempt(t *testing.T) {
	m := &fakeMsg{numDelivered: 2} // second delivery -> testSchedule[1]
	Settle(context.Background(), m, testSchedule, errors.New("boom"))
	assertJittered(t, testSchedule[1], m.nakDelay)
}

func TestBackoffFor(t *testing.T) {
	tests := []struct {
		name         string
		numDelivered uint64
		base         time.Duration
	}{
		{name: "metadata zero — first", numDelivered: 0, base: testSchedule[0]},
		{name: "first delivery — first", numDelivered: 1, base: testSchedule[0]},
		{name: "second delivery — second", numDelivered: 2, base: testSchedule[1]},
		{name: "third delivery — third", numDelivered: 3, base: testSchedule[2]},
		{name: "beyond schedule — reuses last", numDelivered: 99, base: testSchedule[2]},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertJittered(t, tt.base, backoffFor(&fakeMsg{numDelivered: tt.numDelivered}, testSchedule))
		})
	}
}

func TestBackoffFor_MetadataError(t *testing.T) {
	assertJittered(t, testSchedule[0], backoffFor(&fakeMsg{metaErr: errors.New("no meta")}, testSchedule))
}

// Jitter must actually vary, otherwise a fleet that parked during one outage
// retries in lockstep the moment the dependency recovers.
func TestBackoffFor_JitterVaries(t *testing.T) {
	seen := make(map[time.Duration]struct{})
	for range 200 {
		seen[backoffFor(&fakeMsg{numDelivered: 3}, testSchedule)] = struct{}{}
	}
	assert.Greater(t, len(seen), 1, "equal jitter should produce a spread of delays")
}

func TestJitter_ZeroAndNegative(t *testing.T) {
	assert.Equal(t, time.Duration(0), jitter(0))
	assert.Equal(t, -time.Second, jitter(-time.Second))
}

func TestNak_UsesJitteredBackoffForAttempt(t *testing.T) {
	m := &fakeMsg{numDelivered: 2}
	Nak(context.Background(), m, testSchedule, "downstream unavailable")
	assert.True(t, m.naked)
	assert.False(t, m.acked)
	assertJittered(t, testSchedule[1], m.nakDelay)
}

func TestNak_AlwaysSendsAPositiveDelay(t *testing.T) {
	// NakWithDelay(0) serializes as a bare -NAK, which nats-server redelivers
	// instantly and which ignores the consumer's BackOff entirely. The whole
	// point of this helper is that it can never emit one.
	for _, nd := range []uint64{0, 1, 2, 3, 99} {
		m := &fakeMsg{numDelivered: nd}
		Nak(context.Background(), m, testSchedule, "transient")
		assert.Positive(t, m.nakDelay, "numDelivered=%d", nd)
	}
}

func TestNak_LogsWhenTheNakCallFails(t *testing.T) {
	n := 0
	prev := slog.Default()
	slog.SetDefault(slog.New(countHandler{n: &n}))
	defer slog.SetDefault(prev)

	Nak(context.Background(), &fakeMsg{numDelivered: 1, nakErr: errors.New("conn closed")}, testSchedule, "transient")
	assert.Equal(t, 1, n, "a failed nak network call is logged once")
}

func TestNak_SilentOnSuccess(t *testing.T) {
	n := 0
	prev := slog.Default()
	slog.SetDefault(slog.New(countHandler{n: &n}))
	defer slog.SetDefault(prev)

	Nak(context.Background(), &fakeMsg{numDelivered: 1}, testSchedule, "transient")
	assert.Zero(t, n, "the caller owns the business-error log; Nak must not double-log")
}

func TestDefaultBackoff_ShapeAndBudget(t *testing.T) {
	assert.Equal(t, []time.Duration{
		1 * time.Second, 5 * time.Second, 30 * time.Second, 2 * time.Minute, 10 * time.Minute,
	}, DefaultBackoff)

	var total time.Duration
	for _, d := range DefaultBackoff {
		total += d
	}
	assert.Equal(t, 12*time.Minute+36*time.Second, total, "agreed ~15 min retry budget")
}

func TestLowLatencyBackoff_StaysShort(t *testing.T) {
	// Deliberately not extended: broadcast fan-out is user-visible and a
	// 15-minute-late broadcast is worthless.
	assert.Equal(t, []time.Duration{
		200 * time.Millisecond, 1 * time.Second, 5 * time.Second, 30 * time.Second,
	}, LowLatencyBackoff)
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test SERVICE=jsretry`
Expected: FAIL — `undefined: Nak`, `undefined: jitter`, and `TestDefaultBackoff_ShapeAndBudget` failing on the 4-entry schedule.

- [ ] **Step 3: Implement jitter, the tail step, and `Nak`**

In `pkg/jsretry/jsretry.go`, add `"math/rand/v2"` to the imports.

Extend `DefaultBackoff` with the tail entry:

```go
// DefaultBackoff suits workers whose retries can be spaced generously because
// the work is not latency-sensitive — enough to ride out a brief Cassandra or
// Mongo outage without exhausting the consumer's MaxDeliver. The first four
// entries are Synadia's published reference schedule; the 10m tail extends the
// budget to ~13 minutes across MaxDeliver=6.
var DefaultBackoff = []time.Duration{
	1 * time.Second,
	5 * time.Second,
	30 * time.Second,
	2 * time.Minute,
	10 * time.Minute,
}
```

Add `Nak` after `SettleQuiet`:

```go
// Nak schedules msg for redelivery after this attempt's backoff delay, and logs
// only if the Nak network call itself fails — the caller owns the business-error
// log. Use it wherever a handler settles its own ack/nak rather than returning an
// error to Settle.
//
// Never call msg.Nak() or msg.NakWithDelay(0) directly: both serialize as a bare
// -NAK, which nats-server puts straight on the redeliver queue without consulting
// the consumer's BackOff (server/consumer.go:3308-3311), so a sub-second
// downstream blip burns MaxDeliver in milliseconds and the message is dropped.
//
// reason is a short label describing why the message is being redelivered — e.g.
// "bulk index failure", "transient downstream error".
func Nak(ctx context.Context, msg Msg, backoff []time.Duration, reason string) {
	delay := backoffFor(msg, backoff)
	if err := msg.NakWithDelay(delay); err != nil {
		slog.ErrorContext(ctx, "failed to nak message", "reason", reason,
			"delay", delay.String(), "error", err, "request_id", natsutil.RequestIDFromContext(ctx))
	}
}
```

Replace `backoffFor` and add `jitter`:

```go
// backoffFor selects the delay for the next redelivery, indexed by how many
// times the message has already been delivered; the last entry is reused once
// attempts exceed the schedule, and the result is jittered. Falls back to the
// first entry when metadata is unavailable. The uint64 counter is walked rather
// than converted to int, avoiding any narrowing-overflow concern.
func backoffFor(msg Msg, backoff []time.Duration) time.Duration {
	base := backoff[0]
	// NumDelivered is 1 on the first delivery, so the i'th redelivery uses
	// backoff[i]; once attempts exceed the schedule the last entry is reused.
	if meta, err := msg.Metadata(); err == nil && meta != nil {
		for i := uint64(1); i < meta.NumDelivered && i < uint64(len(backoff)); i++ {
			base = backoff[i]
		}
	}
	return jitter(base)
}

// jitter applies AWS "equal jitter": half the base delay plus a random amount up
// to the other half. It guarantees at least 50% of the base while decorrelating a
// fleet whose messages all parked during the same outage — without it every
// backed-off message retries in lockstep the instant the dependency recovers.
// The server-side consumer BackOff cannot do this; it is a literal duration list.
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	half := d / 2
	// #nosec G404 -- retry jitter, not security-sensitive
	return half + time.Duration(rand.Int64N(int64(d-half)+1))
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `make test SERVICE=jsretry`
Expected: PASS. If `TestSettle`'s existing `assert.Positive(t, m.nakDelay, ...)` subtests fail, that is a real regression — jitter must never produce a zero delay for a positive base.

- [ ] **Step 5: Run lint and the gosec gate**

Run: `make lint && make sast-gosec`
Expected: PASS. If gosec still reports G404, confirm the `// #nosec G404 -- retry jitter, not security-sensitive` comment sits on the line *directly above* the `return` statement with no blank line between.

- [ ] **Step 6: Commit**

```bash
git add pkg/jsretry/jsretry.go pkg/jsretry/jsretry_test.go
git commit -m "feat(jsretry): equal jitter, 10m tail step, and a settle-less Nak

Adds AWS equal jitter (d/2 + rand(0, d/2)) so a fleet that parked during
one outage does not retry in lockstep on recovery — the server-side
BackOff is a literal duration list and cannot jitter. Extends
DefaultBackoff with a 10m tail to reach the agreed ~15 min budget, and
adds Nak for handlers that settle their own ack/nak."
```

---

### Task 3: Fix the three bare-`Nak()` hot-loops and delete `natsutil.Nak`

**Files:**
- Modify: `message-gatekeeper/handler.go:206`
- Modify: `inbox-worker/main.go:809`
- Modify: `search-sync-worker/handler.go:111,221,261`
- Modify: `search-sync-worker/main.go:513-522` (delete the hardcoded `cc.BackOff`)
- Modify: `search-sync-worker/consumer_config_test.go:57,81-82`
- Modify: `pkg/natsutil/ack.go` (delete `Nak` and `Naker`)
- Modify: `pkg/natsutil/ack_test.go:48,54` (delete the `Nak` tests)

**Interfaces:**
- Consumes: `jsretry.Nak(ctx, msg, backoff, reason)` and `jsretry.DefaultBackoff` from Task 2.
- Produces: nothing later tasks depend on.

- [ ] **Step 1: Write the failing test for search-sync-worker's consumer config**

In `search-sync-worker/consumer_config_test.go`, replace the assertion at line 57:

```go
				assert.Equal(t, []time.Duration{1 * time.Second, 5 * time.Second, 30 * time.Second}, cc.BackOff)
```

with:

```go
				// The schedule comes from ConsumerSettings now. A hardcoded
				// BackOff{1s,...} silently set the server-side AckWait to 1s
				// (server/consumer.go:677-682), redelivering ES bulk requests
				// while they were still in flight.
				assert.Nil(t, cc.BackOff, "BackOff must come from ConsumerSettings, not be hardcoded")
```

Replace the assertion at lines 81-82 (the comment "BackOff is hardcoded by buildConsumerConfig, not from settings." and its `assert.Equal`) with:

```go
		assert.Nil(t, cc.BackOff)
```

Add a new test asserting the schedule now flows from settings:

```go
func TestBuildConsumerConfig_BackOffFromSettings(t *testing.T) {
	cc := buildConsumerConfig(stream.ConsumerSettings{
		AckWait: 30 * time.Second, MaxDeliver: 6, MaxWaiting: 512, MaxAckPending: 1000,
		BackOffSteps: 5, BackOffFactor: 2, BackOffMax: 8 * time.Minute,
	}, fakeCollection{name: "message-sync"}, "site-a")

	assert.Equal(t, []time.Duration{
		30 * time.Second, time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute,
	}, cc.BackOff)
	assert.Equal(t, cc.AckWait, cc.BackOff[0])
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `make test SERVICE=search-sync-worker`
Expected: FAIL — `cc.BackOff` is still the hardcoded `{1s, 5s, 30s}`.

- [ ] **Step 3: Delete the hardcoded schedule**

In `search-sync-worker/main.go`, delete line 518 (`cc.BackOff = []time.Duration{...}`) and rewrite the `buildConsumerConfig` doc comment (lines 513-514) as:

```go
// buildConsumerConfig returns the durable consumer config for one collection.
// The BackOff schedule comes from ConsumerSettings — never hardcode it here, a
// literal BackOff[0] silently becomes the server-side AckWait.
```

Remove the now-unused `"time"` import if nothing else in the file needs it.

- [ ] **Step 4: Run it to verify it passes**

Run: `make test SERVICE=search-sync-worker`
Expected: PASS.

- [ ] **Step 5: Migrate the three `natsutil.Nak` call sites**

In `search-sync-worker/handler.go`, replace the import of `pkg/natsutil`'s Nak usage with `jsretry`, then:

- line 111: `natsutil.Nak(msg, "update-by-query failed")` → `jsretry.Nak(ctx, msg, jsretry.DefaultBackoff, "update-by-query failed")`
- line 221: `natsutil.Nak(p.jsMsg, "bulk action failed")` → `jsretry.Nak(bulkCtx, p.jsMsg, jsretry.DefaultBackoff, "bulk action failed")`
- line 261: `natsutil.Nak(p.jsMsg, reason)` → `jsretry.Nak(ctx, p.jsMsg, jsretry.DefaultBackoff, reason)`

Use whichever `context.Context` is already in scope at each site — read the surrounding function before editing. Keep every `h.metrics.recordMessages(...)` call exactly as it is.

Add the import:

```go
	"github.com/hmchangw/chat/pkg/jsretry"
```

- [ ] **Step 6: Migrate message-gatekeeper**

In `message-gatekeeper/handler.go`, replace lines 206-208:

```go
			if err := msg.Nak(); err != nil {
				slog.ErrorContext(ctx, "failed to nack message", "error", err, "request_id", req.RequestID)
			}
```

with:

```go
			jsretry.Nak(ctx, msg, jsretry.DefaultBackoff, "process message failed (infra)")
```

The `slog.ErrorContext(ctx, "process message failed (infra)", ...)` line directly above stays — it carries the cause, and `jsretry.Nak` deliberately does not re-log the business error. Add the `jsretry` import.

- [ ] **Step 7: Migrate inbox-worker**

In `inbox-worker/main.go`, replace lines 809-811:

```go
				if err := msg.Nak(); err != nil {
					slog.Error("failed to nak message", "error", err)
				}
```

with:

```go
				jsretry.Nak(handlerCtx, msg, jsretry.DefaultBackoff, "handle event failed")
```

The `slog.Error("handle event failed", ...)` line above stays. Add the `jsretry` import.

- [ ] **Step 8: Delete `natsutil.Nak`**

In `pkg/natsutil/ack.go`, delete the `Naker` interface and the `Nak` function. Keep `Acker` and `Ack`. In `pkg/natsutil/ack_test.go`, delete the two tests calling `natsutil.Nak` (lines 48 and 54) and any now-unused fixture they alone used.

Add to `Ack`'s doc comment:

```go
// There is deliberately no Nak counterpart here: a bare msg.Nak() is an instant
// redelivery that ignores the consumer's BackOff schedule
// (nats-server/server/consumer.go:3308-3311). Use jsretry.Nak instead.
```

- [ ] **Step 9: Verify nothing still reaches for the removed helper**

Run: `grep -rn "natsutil.Nak\|msg.Nak()\|NakWithDelay(0)" --include=*.go . | grep -v _test.go`
Expected: two remaining hits, both in `bot-message-worker/handler.go` and `push-notification-service/handler.go` — those are Task 4.

- [ ] **Step 10: Run the full suite**

Run: `make lint && make test`
Expected: PASS.

- [ ] **Step 11: Commit**

```bash
git add message-gatekeeper inbox-worker search-sync-worker pkg/natsutil
git commit -m "fix: replace bare Nak() hot-loops with jsretry.Nak

A bare -NAK goes straight on the redeliver queue without consulting the
consumer's BackOff (nats-server/server/consumer.go:3308-3311), so a
sub-second Mongo or ES blip burned all five delivery attempts in
milliseconds and dropped the message. Also drops search-sync-worker's
hardcoded BackOff{1s,5s,30s}, which had silently set its server-side
AckWait to 1s, and removes natsutil.Nak so the hot-loop cannot come back."
```

---

### Task 4: bot-message-worker and push-notification-service adopt `ConsumerSettings`

**Files:**
- Modify: `bot-message-worker/main.go:39-40,123-131`
- Modify: `bot-message-worker/handler.go:43-47`
- Modify: `bot-message-worker/deploy/docker-compose.yml:22`
- Create: `bot-message-worker/consumer_config_test.go`
- Modify: `push-notification-service/main.go:23-25,63-73`
- Modify: `push-notification-service/handler.go:44`
- Create: `push-notification-service/consumer_config_test.go`

**Interfaces:**
- Consumes: `stream.ConsumerSettings` and `stream.DurableConsumerDefaults` from Task 1; `jsretry.Nak` and `jsretry.DefaultBackoff` from Task 2.
- Produces: `buildConsumerConfig(s stream.ConsumerSettings, siteID string) jetstream.ConsumerConfig` in bot-message-worker, and `buildConsumerConfig(s stream.ConsumerSettings, mode stream.Pipeline, wiring stream.Wiring) jetstream.ConsumerConfig` in push-notification-service.

- [ ] **Step 1: Write the failing test for bot-message-worker**

Create `bot-message-worker/consumer_config_test.go`:

```go
package main

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/stream"
)

func TestBuildConsumerConfig(t *testing.T) {
	s := stream.ConsumerSettings{
		AckWait: 30 * time.Second, MaxDeliver: 6, MaxWaiting: 512, MaxAckPending: 1000,
		BackOffSteps: 5, BackOffFactor: 2, BackOffMax: 8 * time.Minute,
	}

	cc := buildConsumerConfig(s, "site-a")

	assert.Equal(t, "bot-message-worker", cc.Durable)
	assert.Equal(t, "chat.bot.canonical.site-a.created", cc.FilterSubject)
	assert.Equal(t, jetstream.AckExplicitPolicy, cc.AckPolicy)
	assert.Equal(t, jetstream.DeliverAllPolicy, cc.DeliverPolicy)
	assert.Equal(t, 30*time.Second, cc.AckWait)
	assert.Equal(t, 6, cc.MaxDeliver)
	assert.Equal(t, 512, cc.MaxWaiting)
	assert.Equal(t, 1000, cc.MaxAckPending)
	assert.Equal(t, []time.Duration{
		30 * time.Second, time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute,
	}, cc.BackOff)
}

// Regression: the previous inline BackOff{1s,...} silently set the server-side
// AckWait to 1s (server/consumer.go:677-682), redelivering Cassandra writes
// while they were still in flight.
func TestBuildConsumerConfig_BackOffHeadEqualsAckWait(t *testing.T) {
	cc := buildConsumerConfig(stream.ConsumerSettings{
		AckWait: 45 * time.Second, MaxDeliver: 6,
		BackOffSteps: 5, BackOffFactor: 2, BackOffMax: 8 * time.Minute,
	}, "site-a")

	assert.Equal(t, cc.AckWait, cc.BackOff[0])
}
```

Verify the expected `FilterSubject` against `subject.BotCanonicalCreated("site-a")` before running — read `pkg/subject/subject.go` and use the literal that builder produces.

- [ ] **Step 2: Run it to verify it fails**

Run: `make test SERVICE=bot-message-worker`
Expected: FAIL — `undefined: buildConsumerConfig`.

- [ ] **Step 3: Implement bot-message-worker's config**

In `bot-message-worker/main.go`, delete the `MaxDeliver int` field (line 40) and add to the config struct:

```go
	Consumer stream.ConsumerSettings `envPrefix:"CONSUMER_"`
```

Replace the inline `jetstream.ConsumerConfig{...}` literal (lines 123-131) with:

```go
	cons, err := js.CreateOrUpdateConsumer(ctx, streamCfg.Name, buildConsumerConfig(cfg.Consumer, cfg.SiteID))
```

and add at the bottom of the file:

```go
// buildConsumerConfig returns the durable consumer config. The BackOff schedule
// comes from ConsumerSettings — never hardcode it, a literal BackOff[0] silently
// becomes the server-side AckWait.
func buildConsumerConfig(s stream.ConsumerSettings, siteID string) jetstream.ConsumerConfig {
	cc := stream.DurableConsumerDefaults(s)
	cc.Durable = "bot-message-worker"
	cc.FilterSubject = subject.BotCanonicalCreated(siteID)
	return cc
}
```

Remove the `"time"` import if it becomes unused.

- [ ] **Step 4: Fix bot-message-worker's nak**

In `bot-message-worker/handler.go`, replace lines 43-47:

```go
		// NakWithDelay(0) defers to the consumer's BackOff schedule.
		if nakErr := msg.NakWithDelay(0); nakErr != nil {
			slog.WarnContext(ctx, "bot-message-worker nak failed", "error", nakErr)
		}
```

with:

```go
		jsretry.Nak(ctx, msg, jsretry.DefaultBackoff, "transient cassandra error")
```

The comment being deleted was wrong: `NakWithDelay(0)` serializes as a bare `-NAK` (nats.go `jetstream/message.go:374`), which ignores `BackOff` entirely. The `slog.WarnContext(ctx, "bot-message-worker transient error — nak", ...)` line above stays. Add the `jsretry` import.

- [ ] **Step 5: Update bot-message-worker's compose env**

In `bot-message-worker/deploy/docker-compose.yml`, change line 22 from `- MAX_DELIVER=5` to `- CONSUMER_MAX_DELIVER=6`.

- [ ] **Step 6: Run it to verify it passes**

Run: `make test SERVICE=bot-message-worker`
Expected: PASS.

- [ ] **Step 7: Write the failing test for push-notification-service**

Create `push-notification-service/consumer_config_test.go` with the same shape. Read `push-notification-service/main.go` first to see how `stream.Resolve(cfg.Mode, cfg.SiteID)` produces `wiring.PushInputWildcard` and how `cfg.Mode.ConsumerName("push-notification-service")` derives the durable — the test must call `stream.Resolve` itself rather than hardcoding those strings:

```go
package main

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/stream"
)

func TestBuildConsumerConfig(t *testing.T) {
	s := stream.ConsumerSettings{
		AckWait: 30 * time.Second, MaxDeliver: 6, MaxWaiting: 512, MaxAckPending: 1000,
		BackOffSteps: 5, BackOffFactor: 2, BackOffMax: 8 * time.Minute,
	}
	mode := stream.Pipeline("user")
	wiring := stream.Resolve(mode, "site-a")

	cc := buildConsumerConfig(s, mode, wiring)

	assert.Equal(t, mode.ConsumerName("push-notification-service"), cc.Durable)
	assert.Equal(t, wiring.PushInputWildcard, cc.FilterSubject)
	assert.Equal(t, jetstream.AckExplicitPolicy, cc.AckPolicy)
	assert.Equal(t, 30*time.Second, cc.AckWait)
	assert.Equal(t, 6, cc.MaxDeliver)
	assert.Equal(t, []time.Duration{
		30 * time.Second, time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute,
	}, cc.BackOff)
	assert.Equal(t, cc.AckWait, cc.BackOff[0])
}
```

If `stream.Pipeline("user")` is not the right way to construct the mode, read `pkg/stream/pipeline.go` and use whatever constructor or constant it exposes.

- [ ] **Step 8: Run it to verify it fails**

Run: `make test SERVICE=push-notification-service`
Expected: FAIL — `undefined: buildConsumerConfig`.

- [ ] **Step 9: Implement push-notification-service's config and nak**

In `push-notification-service/main.go`, delete the `MaxDeliver int` field and add:

```go
	Consumer stream.ConsumerSettings `envPrefix:"CONSUMER_"`
```

Replace the inline consumer literal with:

```go
	cons, err := js.CreateOrUpdateConsumer(ctx, wiring.PushStream.Name, buildConsumerConfig(cfg.Consumer, cfg.Mode, wiring))
```

and add at the bottom of the file:

```go
// buildConsumerConfig returns the durable consumer config. The BackOff schedule
// comes from ConsumerSettings — never hardcode it, a literal BackOff[0] silently
// becomes the server-side AckWait.
func buildConsumerConfig(s stream.ConsumerSettings, mode stream.Pipeline, wiring stream.Wiring) jetstream.ConsumerConfig {
	cc := stream.DurableConsumerDefaults(s)
	cc.Durable = mode.ConsumerName("push-notification-service")
	cc.FilterSubject = wiring.PushInputWildcard
	return cc
}
```

Use whatever type `stream.Resolve` actually returns for `wiring` — read its signature and match it exactly.

In `push-notification-service/handler.go`, replace line 44 `_ = msg.NakWithDelay(0)` with:

```go
		jsretry.Nak(ctx, msg, jsretry.DefaultBackoff, "transient dispatch error")
```

Add the `jsretry` import.

- [ ] **Step 10: Run both services' tests**

Run: `make test SERVICE=push-notification-service && make test SERVICE=bot-message-worker`
Expected: PASS.

- [ ] **Step 11: Confirm no bare naks remain**

Run: `grep -rn "natsutil.Nak\|msg.Nak()\|NakWithDelay(0)" --include=*.go . | grep -v _test.go | grep -v data-migration`
Expected: no output.

- [ ] **Step 12: Commit**

```bash
git add bot-message-worker push-notification-service
git commit -m "refactor(bot-message-worker,push-notification-service): adopt ConsumerSettings

Both hand-rolled a ConsumerConfig with BackOff{1s,2s,5s,10s,30s}, which
set their server-side AckWait to 1s and redelivered Cassandra writes and
FCM dispatches while they were still in flight. Both also called
NakWithDelay(0), which serializes as a bare -NAK and hot-loops. Now they
share the common defaults and settle through jsretry.Nak."
```

---

### Task 5: hr-sync-worker literal, CLAUDE.md, and the spec correction

**Files:**
- Modify: `hr-sync-worker/main.go:112-116`
- Modify: `CLAUDE.md` (§6 "JetStream Consumer Pattern")
- Modify: `docs/superpowers/specs/2026-08-21-jetstream-exponential-backoff-design.md` (§5)

**Interfaces:**
- Consumes: `stream.ConsumerSettings` from Task 1.
- Produces: nothing.

- [ ] **Step 1: Give hr-sync-worker the common schedule**

`hr-sync-worker` builds its `ConsumerSettings` as a struct literal, so it would otherwise get `BackOffSteps: 0` and no schedule. In `hr-sync-worker/main.go`, replace lines 112-116:

```go
	consCfg := stream.DurableConsumerDefaults(stream.ConsumerSettings{
		AckWait:    30 * time.Second,
		MaxDeliver: -1, // never drop a feed batch; jsretry backoff spaces the retries
		MaxWaiting: 512,
	})
```

with:

```go
	consCfg := stream.DurableConsumerDefaults(stream.ConsumerSettings{
		AckWait:    30 * time.Second,
		MaxDeliver: -1, // never drop a feed batch; jsretry backoff spaces the retries
		MaxWaiting: 512,
		// MaxDeliver=-1 exempts this consumer from the len(BackOff)<=MaxDeliver rule.
		BackOffSteps:  5,
		BackOffFactor: 2,
		BackOffMax:    8 * time.Minute,
	})
```

- [ ] **Step 2: Verify it builds and passes**

Run: `make test SERVICE=hr-sync-worker && make build SERVICE=hr-sync-worker`
Expected: PASS.

- [ ] **Step 3: Document the two levers in CLAUDE.md**

In CLAUDE.md §6, immediately after the "JetStream Consumer Pattern" bullets, add:

```markdown
### JetStream Redelivery Backoff

Two levers space redeliveries, and they fire on **disjoint** failure modes. Set both.

- **Consumer `BackOff`** (server-side, `pkg/stream.ConsumerSettings`) fires only when a
  message goes un-acked past `AckWait` — pod crash, OOM, hang, or a handler slower than
  `AckWait`. Derived as `AckWait * BackOffFactor^i` capped at `BackOffMax`; defaults give
  `{30s, 1m, 2m, 4m, 8m}` over `MaxDeliver=6`. `CONSUMER_BACKOFF_STEPS=0` disables it.
- **`pkg/jsretry`** (client-side `NakWithDelay`) fires when a handler catches a transient
  error. `DefaultBackoff` for non-latency-sensitive work, `LowLatencyBackoff` for
  user-visible fan-out. Equal-jittered; the server-side lever cannot jitter.

Three server rules the code must respect (`nats-io/nats-server`, `server/consumer.go`):

- **A bare `Nak()` ignores `BackOff` entirely** — it goes straight on the redeliver queue
  (`:3308-3311`), so a sub-second blip burns `MaxDeliver` in milliseconds. `NakWithDelay(0)`
  is the same thing, because nats.go only serializes a delay when it is `> 0`. **Never call
  either** — use `jsretry.Settle`, `jsretry.SettleQuiet`, or `jsretry.Nak`.
- **`BackOff[0]` overwrites `AckWait`** (`:677-682`). Never hardcode a `cc.BackOff` in a
  service; let `stream.DurableConsumerDefaults` derive it so the two cannot disagree.
- **`len(BackOff) > MaxDeliver` is a hard create/update error** unless `MaxDeliver == -1`
  (`:807`, `:2588`). `DurableConsumerDefaults` clamps and warns.
```

- [ ] **Step 4: Correct §5 of the spec**

The spec's §5 says the eight per-service `consumer_config_test.go` files need updating for
`MaxDeliver=6` and the new `BackOff`. That turned out to be wrong: those tests build
`ConsumerSettings` as struct literals without the new fields, so `BackOffSteps` is 0 and
`cc.BackOff` is nil — they pass unchanged. Replace that paragraph with:

```markdown
**Per-service `consumer_config_test.go`** — no change needed. All eight build
`ConsumerSettings` as struct literals with explicit `AckWait`/`MaxDeliver`, so the new
`BackOff*` fields default to zero and `cc.BackOff` stays nil. Propagation is covered in
`pkg/stream`, where it belongs. The one exception is search-sync-worker, whose assertions
on the hardcoded `{1s, 5s, 30s}` (`consumer_config_test.go:57,82`) are replaced.
```

- [ ] **Step 5: Run the full suite and lint**

Run: `make fmt && make lint && make test`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add hr-sync-worker CLAUDE.md docs/superpowers/specs/2026-08-21-jetstream-exponential-backoff-design.md
git commit -m "docs: document the two JetStream backoff levers; hr-sync-worker schedule

hr-sync-worker builds ConsumerSettings as a struct literal, so it needs
the BackOff* fields set explicitly to pick up the common default."
```

---

### Task 6: Integration test — prove the server keeps `AckWait == BackOff[0]`

**Files:**
- Create: `pkg/stream/consumer_integration_test.go`

**Interfaces:**
- Consumes: `stream.DurableConsumerDefaults` from Task 1, `testutil.NATS(t)` and `testutil.RunTests(m)` from `pkg/testutil`.
- Produces: nothing.

This is the only test that can catch the `AckWait = BackOff[0]` overwrite, because the
overwrite happens inside nats-server. A unit test asserting on the struct we send would
have passed happily while the three buggy services ran a 1-second `AckWait` in production.

- [ ] **Step 1: Check whether `pkg/stream` already has a `TestMain`**

Run: `grep -rn "func TestMain" pkg/stream/`
If one exists, do not add a second — reuse it. If it lives in a non-integration file, the
new file must not declare another.

- [ ] **Step 2: Write the failing test**

Create `pkg/stream/consumer_integration_test.go`:

```go
//go:build integration

package stream_test

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

// The server overwrites AckWait with BackOff[0] (server/consumer.go:677-682).
// Deriving the schedule from AckWait makes the overwrite a no-op — this test
// proves it against a real nats-server rather than against our own struct.
func TestDurableConsumerDefaults_ServerHonoursDerivedSchedule(t *testing.T) {
	url := testutil.NATS(t)
	nc, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(func() { _ = nc.Drain() })

	js, err := jetstream.New(nc)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	streamName := "BACKOFF-TEST"
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{"backoff.test.>"},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = js.DeleteStream(context.Background(), streamName) })

	cc := stream.DurableConsumerDefaults(stream.ConsumerSettings{
		AckWait: 30 * time.Second, MaxDeliver: 6, MaxWaiting: 512, MaxAckPending: 1000,
		BackOffSteps: 5, BackOffFactor: 2, BackOffMax: 8 * time.Minute,
	})
	cc.Durable = "backoff-test-consumer"

	cons, err := js.CreateOrUpdateConsumer(ctx, streamName, cc)
	require.NoError(t, err, "the server must accept the derived schedule")

	got := cons.CachedInfo().Config
	assert.Equal(t, []time.Duration{
		30 * time.Second, time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute,
	}, got.BackOff)
	assert.Equal(t, 30*time.Second, got.AckWait,
		"server-side AckWait must survive the BackOff[0] overwrite")
	assert.Equal(t, 6, got.MaxDeliver)
}

// len(BackOff) > MaxDeliver is JSConsumerMaxDeliverBackoffError at create time
// (server/consumer.go:807) — the clamp in backOffSchedule is what prevents it.
func TestDurableConsumerDefaults_ClampKeepsTheServerHappy(t *testing.T) {
	url := testutil.NATS(t)
	nc, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(func() { _ = nc.Drain() })

	js, err := jetstream.New(nc)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	streamName := "BACKOFF-CLAMP-TEST"
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{"backoff.clamp.>"},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = js.DeleteStream(context.Background(), streamName) })

	// 9 steps against MaxDeliver=3 — without the clamp the server rejects this.
	cc := stream.DurableConsumerDefaults(stream.ConsumerSettings{
		AckWait: time.Second, MaxDeliver: 3, MaxWaiting: 512, MaxAckPending: 100,
		BackOffSteps: 9, BackOffFactor: 2, BackOffMax: time.Minute,
	})
	cc.Durable = "backoff-clamp-consumer"

	cons, err := js.CreateOrUpdateConsumer(ctx, streamName, cc)
	require.NoError(t, err)
	assert.Len(t, cons.CachedInfo().Config.BackOff, 3)
}
```

- [ ] **Step 3: Run it to verify it fails**

Temporarily revert `DurableConsumerDefaults` to not set `BackOff` (comment out the
`BackOff: s.backOffSchedule(),` line), then run: `make test-integration SERVICE=stream`
Expected: FAIL on the `got.BackOff` assertion. Restore the line afterwards.

- [ ] **Step 4: Run it against the real implementation**

Run: `make test-integration SERVICE=stream`
Expected: PASS. If `testutil.NATS` or `testutil.RunTests` are named differently, read
`pkg/testutil/` and match the real API — CLAUDE.md forbids starting your own containers
with `natsmod.Run` or `testcontainers.GenericContainer`.

- [ ] **Step 5: Run everything**

Run: `make fmt && make lint && make test && make sast`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add pkg/stream/consumer_integration_test.go
git commit -m "test(stream): prove the server keeps AckWait after the BackOff overwrite

Only an integration test can catch this: the overwrite happens inside
nats-server, so a unit test asserting on the struct we send would pass
while the consumer ran a 1s AckWait in production."
```

---

## Self-Review

**Spec coverage:**

| Spec section | Task |
|---|---|
| §4.1 `pkg/stream` derivation, off-switch, clamp, factor guard | Task 1 |
| §4.2 `jsretry` tail step, equal jitter, `Nak` | Task 2 |
| §4.3 remove `natsutil.Nak` | Task 3 |
| §4.4 message-gatekeeper, inbox-worker, search-sync-worker | Task 3 |
| §4.4 bot-message-worker, push-notification-service | Task 4 |
| §4.4 hr-sync-worker | Task 5 |
| §4.5 CLAUDE.md | Task 5 |
| §5 unit tests | Tasks 1, 2, 3, 4 |
| §5 integration test for the server-side `AckWait` | Task 6 |

**Correction applied:** §5's claim that eight per-service `consumer_config_test.go` files
need `MaxDeliver=6` updates is wrong — they use struct literals, so they are unaffected.
Task 5 Step 4 fixes the spec.

**Type consistency:** `BackOffSteps int`, `BackOffFactor float64`, `BackOffMax
time.Duration` and the unexported `backOffSchedule()` / `jitter()` are used with the same
names and types in every task that references them. `jsretry.Nak(ctx, msg, backoff,
reason)` has the same four-parameter shape at all six call sites.

**Ordering:** Tasks 3 and 4 both depend on Task 2's `jsretry.Nak`; Tasks 4, 5 and 6 depend
on Task 1's new fields. Execute in order.
