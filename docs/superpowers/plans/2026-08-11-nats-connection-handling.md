# NATS Drain + Slow Consumer Visibility Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `nc.Drain()` actually wait for the drain to finish before the process exits, and make NATS slow consumers visible in logs and metrics.

**Architecture:** Two additions to `pkg/natsutil` — a blocking `Drain` helper that waits on the connection's CLOSED status transition, and a slow-consumer branch in the shared `nats.ErrorHandler`. Then a sweep converting 30 call sites across five distinct shapes to use the new helper.

**Tech Stack:** Go 1.25, `github.com/nats-io/nats.go` v1.50.0, `github.com/flywindy/o11y/nats` v0.9.1, `go.opentelemetry.io/otel` metrics, `testify`, embedded `nats-server/v2` for tests.

**Spec:** `docs/superpowers/specs/2026-08-11-nats-connection-handling-design.md`

## Global Constraints

- All commands go through `make` — never raw `go` commands. Tests: `make test SERVICE=pkg/natsutil`. Lint: `make lint`. Build: `make build SERVICE=<name>`.
- All tests run with `-race` (the Makefile handles this).
- TDD is mandatory: write the failing test, run it, confirm it fails for the right reason, then implement.
- Coverage floor 80% repo-wide; `pkg/` targets 90%.
- Logging is `log/slog` only, structured key-value fields, never interpolated strings.
- Errors wrap with context: `fmt.Errorf("short description: %w", err)`. Never bare `err`.
- Existing `pkg/natsutil` tests live in the **external** test package `package natsutil_test`. Default to that. Task 2 is the one exception — it tests unexported helpers, so its file declares `package natsutil`. Go permits both in one directory; do not convert the existing external tests.
- Pre-commit hook runs lint and tests. Fix failures rather than bypassing.
- No new third-party dependencies. Everything used here is already in `go.mod`.
- Never commit `.env` files. Do not create a PR unless explicitly asked.

## File Structure

| File | Responsibility |
|------|----------------|
| `pkg/natsutil/drain.go` (create) | `Drain(ctx, conn)` — blocking drain helper |
| `pkg/natsutil/drain_test.go` (create) | Drain behavior tests against embedded NATS |
| `pkg/natsutil/slowconsumer.go` (create) | Slow-consumer field extraction, log-level selection, drop counter |
| `pkg/natsutil/slowconsumer_test.go` (create) | Field extraction + level selection tests |
| `pkg/natsutil/connect.go` (modify) | Add `DrainTimeout` default; route `ErrSlowConsumer` in `ErrorHandler` |
| `pkg/natsutil/connect_test.go` (modify) | Assert `DrainTimeout` is applied |
| 30 service `main.go` files (modify) | Call-site conversion, five shapes |

---

### Task 1: Blocking `natsutil.Drain` + drain timeout

**Files:**
- Create: `pkg/natsutil/drain.go`
- Create: `pkg/natsutil/drain_test.go`
- Modify: `pkg/natsutil/connect.go:16` (const block), `pkg/natsutil/connect.go:43-59` (`baseOpts`)
- Modify: `pkg/natsutil/connect_test.go`

**Interfaces:**
- Consumes: `natsutil.Connect(ctx, url, credsFile string, tp trace.TracerProvider, prop propagation.TextMapPropagator, tracingEnabled bool, opts ...nats.Option) (*o11ynats.Conn, error)` — already exists.
- Produces: `natsutil.Drain(ctx context.Context, conn *o11ynats.Conn) error` — every later task calls exactly this.

**Background the implementer needs:**

`nats.Conn.Drain()` (`nats.go@v1.50.0:6055-6075`) does not block. It flips the connection to `DRAINING_SUBS`, spawns `go nc.drainConnection()`, and returns `nil`. The real work — draining subscriptions, `FlushTimeout(5s)`, then `Close()` — happens on that goroutine. A shutdown hook that returns the result of `Drain()` therefore returns instantly and the process exits mid-drain.

`o11ynats.Conn` embeds `otelnats.Conn`, whose `Drain()` (`otelnats/conn.go:222-224`) is a bare delegation to the underlying `*nats.Conn`. Reaching through `conn.NatsConn()` skips no tracing behavior.

We wait on `nc.StatusChanged(nats.CLOSED)` (`nats.go:6379`) rather than a `ClosedHandler`, because `connect.go:53` already occupies the `ClosedHandler` option slot and same-kind `nats.Option`s overwrite rather than compose.

This pattern is not new to the codebase. `pkg/natsrouter.Shutdown` (`router.go:246-275`) already does the same thing one level down, for subscriptions: register `StatusChanged(nats.SubscriptionClosed)` listeners on every subscription *before* calling `Drain()`, then wait on them with the caller's context. Read that function before implementing — this task is the `Conn`-level counterpart, and matching its structure keeps the two consistent.

- [ ] **Step 1: Write the failing tests**

Create `pkg/natsutil/drain_test.go`:

```go
package natsutil_test

import (
	"context"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/hmchangw/chat/pkg/natsutil"
)

// startTestServer starts an embedded NATS server and returns its client URL.
// Separate from startTestNATS (which hands back a raw *nats.Conn) because these
// tests need to build a real *o11ynats.Conn through natsutil.Connect, so the
// production option wiring — including DrainTimeout — is what gets exercised.
func startTestServer(t *testing.T) string {
	t.Helper()
	ns, err := natsserver.NewServer(&natsserver.Options{Port: -1})
	require.NoError(t, err)
	ns.Start()
	require.True(t, ns.ReadyForConnections(5*time.Second))
	t.Cleanup(ns.Shutdown)
	return ns.ClientURL()
}

func testConn(t *testing.T, url string) *o11ynats.Conn {
	t.Helper()
	conn, err := natsutil.Connect(context.Background(), url, "",
		noop.NewTracerProvider(), propagation.TraceContext{}, false)
	require.NoError(t, err)
	return conn
}

func TestDrain_ClosesConnection(t *testing.T) {
	conn := testConn(t, startTestServer(t))

	_, err := conn.NatsConn().Subscribe("drain.test", func(*nats.Msg) {})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, natsutil.Drain(ctx, conn))
	require.True(t, conn.NatsConn().IsClosed(),
		"Drain must not return until the connection has reached CLOSED")
}

// The regression this whole change exists for: Drain must not return while a
// handler is still running. Before the fix nats.Conn.Drain returned instantly
// and this assertion fails with handlerDone still false.
func TestDrain_WaitsForInFlightHandler(t *testing.T) {
	url := startTestServer(t)
	conn := testConn(t, url)

	release := make(chan struct{})
	handlerDone := make(chan struct{})
	_, err := conn.NatsConn().Subscribe("drain.slow", func(*nats.Msg) {
		<-release
		close(handlerDone)
	})
	require.NoError(t, err)
	require.NoError(t, conn.NatsConn().Flush())

	pub := testConn(t, url)
	require.NoError(t, pub.NatsConn().Publish("drain.slow", []byte("x")))
	require.NoError(t, pub.NatsConn().Flush())

	drained := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		drained <- natsutil.Drain(ctx, conn)
	}()

	select {
	case <-drained:
		t.Fatal("Drain returned while the handler was still in flight")
	case <-time.After(200 * time.Millisecond):
	}

	close(release)
	<-handlerDone
	require.NoError(t, <-drained)
}

func TestDrain_CtxExpiredReturnsError(t *testing.T) {
	url := startTestServer(t)
	conn := testConn(t, url)

	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	_, err := conn.NatsConn().Subscribe("drain.wedged", func(*nats.Msg) { <-release })
	require.NoError(t, err)
	require.NoError(t, conn.NatsConn().Flush())

	pub := testConn(t, url)
	require.NoError(t, pub.NatsConn().Publish("drain.wedged", []byte("x")))
	require.NoError(t, pub.NatsConn().Flush())

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err = natsutil.Drain(ctx, conn)
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestDrain_AlreadyClosedIsNotAnError(t *testing.T) {
	conn := testConn(t, startTestServer(t))
	conn.NatsConn().Close()

	require.NoError(t, natsutil.Drain(context.Background(), conn))
}

func TestDrain_Idempotent(t *testing.T) {
	conn := testConn(t, startTestServer(t))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, natsutil.Drain(ctx, conn))
	require.NoError(t, natsutil.Drain(ctx, conn))
}

func TestDrain_NilConnIsNotAnError(t *testing.T) {
	require.NoError(t, natsutil.Drain(context.Background(), nil))
}
```

Add the `o11ynats` import to the file's import block:

```go
o11ynats "github.com/flywindy/o11y/nats"
```

- [ ] **Step 2: Run the tests and confirm they fail**

Run: `make test SERVICE=pkg/natsutil`

Expected: compile failure — `undefined: natsutil.Drain`. That is the correct Red state.

- [ ] **Step 3: Implement `Drain`**

Create `pkg/natsutil/drain.go`:

```go
package natsutil

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	o11ynats "github.com/flywindy/o11y/nats"
	"github.com/nats-io/nats.go"
)

// Drain puts the connection into the drain state and blocks until it reaches
// CLOSED or ctx expires.
//
// nats.Conn.Drain only *starts* the drain — it spawns drainConnection on a
// goroutine and returns nil immediately — so a shutdown hook that returns
// nc.Drain() directly returns before subscriptions have drained and before the
// final publish flush, and the process exits with buffered JetStream acks and
// request/reply responses still in the write buffer.
func Drain(ctx context.Context, conn *o11ynats.Conn) error {
	if conn == nil {
		return nil
	}
	nc := conn.NatsConn()
	if nc == nil {
		return nil
	}

	// Register the listener before calling Drain so a fast close cannot land in
	// the gap between the two calls, and re-check IsClosed after, to cover a
	// close that completed before the listener was in place. Together these
	// close both sides of the race.
	ch := nc.StatusChanged(nats.CLOSED)
	defer nc.RemoveStatusListener(ch)

	if err := nc.Drain(); err != nil {
		switch {
		case errors.Is(err, nats.ErrConnectionClosed):
			return nil
		case errors.Is(err, nats.ErrConnectionReconnecting):
			// Drain hard-closed the connection. Buffered publishes are gone,
			// but there was no reachable server to flush them to — a normal
			// shutdown-during-outage, not a failure worth reporting upward.
			slog.WarnContext(ctx, "nats drain skipped: connection reconnecting")
			return nil
		default:
			return fmt.Errorf("start nats drain: %w", err)
		}
	}
	if nc.IsClosed() {
		return nil
	}

	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("nats drain incomplete: %w", ctx.Err())
	}
}
```

- [ ] **Step 4: Run the tests and confirm they pass**

Run: `make test SERVICE=pkg/natsutil`

Expected: PASS, all seven `TestDrain_*` cases.

If `TestDrain_WaitsForInFlightHandler` fails, the wait logic is wrong — do not "fix" it by loosening the assertion. That test is the entire point of the change.

- [ ] **Step 5: Add the drain timeout default**

In `pkg/natsutil/connect.go`, extend the const block at line 16:

```go
const (
	defaultReconnectWait = 2 * time.Second
	// defaultDrainTimeout bounds the subscription-drain phase. Worst case is
	// this plus drainConnection's internal FlushTimeout(5s) = 20s, which fits
	// inside the 25s shutdown.Wait budget and the 30s Kubernetes grace period.
	// The library default is 30s (nats.DefaultDrainTimeout), which is larger
	// than our budget and so could never be the timeout that fires.
	defaultDrainTimeout = 15 * time.Second
)
```

Add to `baseOpts` (after `nats.ReconnectWait(defaultReconnectWait)` at line 46):

```go
nats.DrainTimeout(defaultDrainTimeout),
```

Caller `opts` are appended after `baseOpts` (`connect.go:60`), so a caller that genuinely needs a different value can still override.

- [ ] **Step 6: Write the failing test for the drain timeout**

Append to `pkg/natsutil/connect_test.go`:

```go
func TestConnect_SetsDrainTimeout(t *testing.T) {
	conn, err := natsutil.Connect(context.Background(), startTestServer(t), "",
		noop.NewTracerProvider(), propagation.TraceContext{}, false)
	require.NoError(t, err)
	t.Cleanup(conn.NatsConn().Close)

	require.Equal(t, 15*time.Second, conn.NatsConn().Opts.DrainTimeout,
		"DrainTimeout must be under the 25s shutdown budget, not the 30s library default")
}
```

Add `"time"` to the import block if it is not already present.

- [ ] **Step 7: Run the tests**

Run: `make test SERVICE=pkg/natsutil`

Expected: PASS. If Step 5 was skipped this fails with `30s != 15s`.

- [ ] **Step 8: Lint and check coverage**

Run: `make lint`

Run: `go test -race -coverprofile=/tmp/claude-0/-home-user-newchat/8d24fbda-5dbb-53c0-8a07-ddeccbb30dc1/scratchpad/cover.out ./pkg/natsutil/... && go tool cover -func=/tmp/claude-0/-home-user-newchat/8d24fbda-5dbb-53c0-8a07-ddeccbb30dc1/scratchpad/cover.out | grep -E "drain.go|total:"`

Expected: `drain.go` functions at 90%+, no lint findings.

- [ ] **Step 9: Commit**

```bash
git add pkg/natsutil/drain.go pkg/natsutil/drain_test.go pkg/natsutil/connect.go pkg/natsutil/connect_test.go
git commit -m "feat(natsutil): add blocking Drain and bounded DrainTimeout

nats.Conn.Drain only starts the drain on a goroutine and returns nil, so
shutdown hooks returned before subscriptions drained and the process
exited with buffered acks and replies still in the write buffer.

Drain now waits on the CLOSED status transition, bounded by the caller's
context, and DrainTimeout is set to 15s so it fits inside the 25s
shutdown budget rather than the 30s library default."
```

---

### Task 2: Slow consumer diagnostics

**Files:**
- Create: `pkg/natsutil/slowconsumer.go`
- Create: `pkg/natsutil/slowconsumer_test.go`
- Modify: `pkg/natsutil/connect.go:56-58` (`ErrorHandler`)

**Interfaces:**
- Consumes: nothing from Task 1. This task is independent and may be done in either order.
- Produces: `slowConsumerFields(sub *nats.Subscription) []any` and `logSlowConsumer(log *slog.Logger, sub *nats.Subscription)`, both unexported. No later task calls them.

**Background the implementer needs:**

`connect.go:56` currently throws away the `*nats.Subscription` argument:

```go
nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
	log.Error("nats async error", "error", err)
}),
```

So a slow consumer logs `nats async error: nats: slow consumer, messages dropped` with no subject, no queue group, and no drop count — nothing to act on or alert on.

`nats.ErrSlowConsumer` (`nats.go:111`) is a plain `errors.New` sentinel, so `errors.Is` matches. `Subscription.Dropped()` (`nats.go:5574`), `Pending()` (`:5462`), and `PendingLimits()` (`:5521`) each return an error when the subscription is already closed — which happens routinely during shutdown, so every one must be handled rather than ignored. `sub` itself is nil for connection-level async errors.

Log level: **ERROR** when `Dropped() > 0`, **WARN** otherwise. Dropped messages on a core NATS subscription are unrecoverable loss and should page; a slow consumer that has not yet dropped anything is a warning.

- [ ] **Step 1: Write the failing tests**

Create `pkg/natsutil/slowconsumer_test.go`:

```go
package natsutil

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

// This file is in package natsutil (not natsutil_test) because it exercises
// unexported helpers. The rest of the package's tests stay external.

func TestSlowConsumerFields_RealSubscription(t *testing.T) {
	nc := newLocalConn(t)
	sub, err := nc.QueueSubscribe("slow.subject", "slow-queue", func(*nats.Msg) {})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	got := fieldMap(t, slowConsumerFields(sub))

	require.Equal(t, "slow.subject", got["subject"])
	require.Equal(t, "slow-queue", got["queue"])
	require.Contains(t, got, "dropped")
	require.Contains(t, got, "pending_msgs")
	require.Contains(t, got, "pending_bytes")
	require.Contains(t, got, "limit_msgs")
	require.Contains(t, got, "limit_bytes")
}

func TestSlowConsumerFields_NilSubscription(t *testing.T) {
	require.NotPanics(t, func() {
		got := fieldMap(t, slowConsumerFields(nil))
		require.Equal(t, "unknown", got["subject"])
	})
}

func TestSlowConsumerFields_ClosedSubscription(t *testing.T) {
	nc := newLocalConn(t)
	sub, err := nc.Subscribe("closed.subject", func(*nats.Msg) {})
	require.NoError(t, err)
	require.NoError(t, sub.Unsubscribe())

	require.NotPanics(t, func() {
		got := fieldMap(t, slowConsumerFields(sub))
		require.Equal(t, "closed.subject", got["subject"])
	})
}

func TestLogSlowConsumer_LevelSelection(t *testing.T) {
	tests := []struct {
		name      string
		dropped   int
		wantLevel string
	}{
		{name: "no drops yet warns", dropped: 0, wantLevel: "WARN"},
		{name: "drops are an error", dropped: 3, wantLevel: "ERROR"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			log := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

			logSlowConsumerAt(log, tt.dropped, []any{"subject", "x"})

			var rec map[string]any
			require.NoError(t, json.Unmarshal(buf.Bytes(), &rec))
			require.Equal(t, tt.wantLevel, rec["level"])
		})
	}
}

func fieldMap(t *testing.T, fields []any) map[string]any {
	t.Helper()
	require.Zero(t, len(fields)%2, "slog fields must be key-value pairs")
	out := make(map[string]any, len(fields)/2)
	for i := 0; i < len(fields); i += 2 {
		key, ok := fields[i].(string)
		require.True(t, ok, "field key at %d is not a string", i)
		out[key] = fields[i+1]
	}
	return out
}
```

Add a `newLocalConn` helper to the same file (the existing `startTestNATS` lives in the external test package and is not reachable from here):

```go
func newLocalConn(t *testing.T) *nats.Conn {
	t.Helper()
	ns, err := natsserver.NewServer(&natsserver.Options{Port: -1})
	require.NoError(t, err)
	ns.Start()
	require.True(t, ns.ReadyForConnections(5*time.Second))
	t.Cleanup(ns.Shutdown)
	nc, err := nats.Connect(ns.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	return nc
}
```

with imports `natsserver "github.com/nats-io/nats-server/v2/server"` and `"time"`.

- [ ] **Step 2: Run the tests and confirm they fail**

Run: `make test SERVICE=pkg/natsutil`

Expected: compile failure — `undefined: slowConsumerFields`, `undefined: logSlowConsumerAt`.

- [ ] **Step 3: Implement the helpers**

Create `pkg/natsutil/slowconsumer.go`:

```go
package natsutil

import (
	"context"
	"log/slog"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// slowConsumerDropped counts messages dropped by a subscription that could not
// keep up, tagged by subject and queue group.
var slowConsumerDropped metric.Int64Counter

func init() {
	var err error
	slowConsumerDropped, err = otel.Meter("nats").Int64Counter(
		"nats_slow_consumer_dropped_total",
		metric.WithDescription("Messages dropped by a NATS subscription that could not keep up, by subject and queue group"),
	)
	if err != nil {
		// Fall back to a no-op counter so recording stays safe even if the
		// global meter provider is not initialised at package init time.
		slowConsumerDropped, _ = noop.NewMeterProvider().Meter("nats").
			Int64Counter("nats_slow_consumer_dropped_total")
	}
}

// slowConsumerFields builds the slog fields describing a slow consumer.
// Every accessor is best-effort: sub is nil for connection-level async errors,
// and Dropped/Pending/PendingLimits all error on an already-closed
// subscription, which happens routinely during shutdown. A diagnostic path
// must never panic or mask the event it is reporting.
func slowConsumerFields(sub *nats.Subscription) []any {
	if sub == nil {
		return []any{"subject", "unknown"}
	}
	fields := []any{"subject", sub.Subject, "queue", sub.Queue}
	if dropped, err := sub.Dropped(); err == nil {
		fields = append(fields, "dropped", dropped)
	}
	// nbytes, not bytes — a local named bytes shadows the stdlib package and
	// trips the linter's shadow check.
	if msgs, nbytes, err := sub.Pending(); err == nil {
		fields = append(fields, "pending_msgs", msgs, "pending_bytes", nbytes)
	}
	if msgs, nbytes, err := sub.PendingLimits(); err == nil {
		fields = append(fields, "limit_msgs", msgs, "limit_bytes", nbytes)
	}
	return fields
}

// subDropped returns the subscription's drop count, or 0 when it cannot be read.
func subDropped(sub *nats.Subscription) int {
	if sub == nil {
		return 0
	}
	dropped, err := sub.Dropped()
	if err != nil {
		return 0
	}
	return dropped
}

// logSlowConsumer reports a slow consumer and records the drop count.
func logSlowConsumer(log *slog.Logger, sub *nats.Subscription) {
	dropped := subDropped(sub)
	logSlowConsumerAt(log, dropped, slowConsumerFields(sub))
	if dropped > 0 && sub != nil {
		slowConsumerDropped.Add(context.Background(), int64(dropped),
			metric.WithAttributes(
				attribute.String("subject", sub.Subject),
				attribute.String("queue", sub.Queue),
			))
	}
}

// logSlowConsumerAt picks the level: dropped messages on a core subscription are
// unrecoverable loss and should page, so they log at ERROR. A slow consumer that
// has not dropped anything yet is a warning.
func logSlowConsumerAt(log *slog.Logger, dropped int, fields []any) {
	if dropped > 0 {
		log.Error("nats slow consumer, messages dropped", fields...)
		return
	}
	log.Warn("nats slow consumer", fields...)
}
```

- [ ] **Step 4: Run the tests and confirm they pass**

Run: `make test SERVICE=pkg/natsutil`

Expected: PASS.

- [ ] **Step 5: Wire the handler**

In `pkg/natsutil/connect.go`, replace the `ErrorHandler` at lines 56-58:

```go
nats.ErrorHandler(func(_ *nats.Conn, sub *nats.Subscription, err error) {
	if errors.Is(err, nats.ErrSlowConsumer) {
		logSlowConsumer(log, sub)
		return
	}
	log.Error("nats async error", "error", err)
}),
```

Add `"errors"` to the import block.

- [ ] **Step 6: Run tests and lint**

Run: `make test SERVICE=pkg/natsutil`

Run: `make lint`

Expected: PASS, no findings.

- [ ] **Step 7: Commit**

```bash
git add pkg/natsutil/slowconsumer.go pkg/natsutil/slowconsumer_test.go pkg/natsutil/connect.go
git commit -m "feat(natsutil): report slow consumers with subject and drop count

The shared ErrorHandler discarded its *nats.Subscription argument, so a
slow consumer logged a bare 'nats async error' with no subject, queue, or
drop count — nothing to act on or alert on.

Slow consumer events now log the subject, queue group, drop count and
pending/limit counters, at ERROR once messages have actually been dropped
and WARN before that, and increment nats_slow_consumer_dropped_total."
```

---

### Task 3: Convert Shape A — hook lambdas (19 sites)

**Files (all `Modify`):**

`bot-message-handler/main.go:92`, `bot-message-worker/main.go:177`, `bot-room-service/main.go:109`, `broadcast-worker/main.go:275`, `history-service/cmd/main.go:242`, `inbox-worker/main.go:776`, `media-service/main.go:107`, `message-gatekeeper/main.go:222`, `message-worker/main.go:271`, `notification-worker/main.go:390`, `outbox-worker/main.go:186`, `push-notification-service/main.go:118`, `room-service/main.go:306`, `room-worker/main.go:312`, `search-service/main.go:265`, `search-sync-worker/main.go:351`, `translation-service/main.go:121`, `user-presence-service/main.go:163`, `user-service/main.go:146`

**Interfaces:**
- Consumes: `natsutil.Drain(ctx context.Context, conn *o11ynats.Conn) error` from Task 1.
- Produces: nothing.

**Background:** These are all inside a `shutdown.Wait(ctx, 25*time.Second, ...)` hook list. `shutdown.Wait` passes each hook a context already bounded to 25s (`pkg/shutdown/shutdown.go:22`), so the drain inherits the shutdown budget with no extra plumbing.

Line numbers will drift as you edit. Locate each site by content, not by line.

- [ ] **Step 1: Convert every site**

Each site is one of two forms. With a named ctx:

```go
-		func(ctx context.Context) error { return nc.Drain() },
+		func(ctx context.Context) error { return natsutil.Drain(ctx, nc) },
```

With a discarded ctx — the parameter must be named:

```go
-		func(_ context.Context) error { return nc.Drain() },
+		func(ctx context.Context) error { return natsutil.Drain(ctx, nc) },
```

The discarded-ctx form appears at `bot-message-worker:177`, `bot-room-service:109`, `media-service:107`, `notification-worker:390`, and `push-notification-service:118`. The rest already name it.

Ensure each file imports `"github.com/hmchangw/chat/pkg/natsutil"` — most already do, since they call `natsutil.Connect`.

- [ ] **Step 2: Verify nothing was missed**

Run: `grep -rn "return nc.Drain()" --include="*.go" . | grep -v data-migration | grep -v tools/`

Expected: no output.

- [ ] **Step 3: Build every touched service**

Run: `make build SERVICE=bot-message-handler && make build SERVICE=bot-message-worker && make build SERVICE=bot-room-service && make build SERVICE=broadcast-worker && make build SERVICE=inbox-worker && make build SERVICE=media-service && make build SERVICE=message-gatekeeper && make build SERVICE=message-worker && make build SERVICE=notification-worker && make build SERVICE=outbox-worker && make build SERVICE=push-notification-service && make build SERVICE=room-service && make build SERVICE=room-worker && make build SERVICE=search-service && make build SERVICE=search-sync-worker && make build SERVICE=translation-service && make build SERVICE=user-presence-service && make build SERVICE=user-service`

Then: `make build SERVICE=history-service`

Expected: all succeed. The Makefile special-cases `history-service` to build from `./history-service/cmd/` (`Makefile:115`), so the plain service name is correct — verified.

- [ ] **Step 4: Full test and lint sweep**

Run: `make test`

Run: `make lint`

Expected: PASS, no findings.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor: block on NATS drain in service shutdown hooks

Nineteen services returned nc.Drain() straight from their shutdown hook,
which returned instantly and let the process exit mid-drain. Route them
through natsutil.Drain so the hook waits, bounded by the 25s shutdown
context each hook already receives."
```

---

### Task 4: Convert Shapes B and C — inline hooks and deferred drains (4 sites)

**Files:**
- Modify: `botplatform-service/main.go:127`, `admin-service/main.go:93-99`
- Modify: `teams-room-creation/main.go:70-74`, `user-presence-service/sync/main.go:105-109`

**Interfaces:**
- Consumes: `natsutil.Drain(ctx context.Context, conn *o11ynats.Conn) error` from Task 1.
- Produces: nothing.

**Background:** Shape B sites sit inside a hook that also shuts the HTTP server down and disconnects Mongo/Valkey, and swallow the drain error into a log line. Keep that swallow — the surrounding hook returns the HTTP server's error, and changing which error wins is a behavior change outside this spec's scope.

Shape C sites are `defer`red inside a `run` function with no `shutdown.Wait` context to borrow. The subtlety that makes or breaks these: on SIGTERM the parent `ctx` is **already cancelled** by the time the deferred function runs, so deriving a timeout from it directly produces an already-expired context and the drain returns instantly — a silent no-op that looks exactly like a working fix. `context.WithoutCancel` is what prevents that.

- [ ] **Step 1: Convert `botplatform-service`**

```go
-				if drainErr := nc.Drain(); drainErr != nil {
+				if drainErr := natsutil.Drain(ctx, nc); drainErr != nil {
 					slog.Warn("nats drain failed", "error", drainErr)
 				}
```

- [ ] **Step 2: Convert `admin-service` and delete the stale comment**

```go
-				// srv.Shutdown has already waited out any in-flight toggle, so
-				// Drain (which returns immediately and finishes in the background)
-				// only closes the idle connection.
-				if drainErr := nc.NatsConn().Drain(); drainErr != nil {
+				if drainErr := natsutil.Drain(ctx, nc); drainErr != nil {
 					slog.Warn("drain nats", "error", drainErr)
 				}
```

The comment described the exact trap this change removes. Leaving it would be actively misleading.

- [ ] **Step 3: Convert `teams-room-creation`**

```go
 	defer func() {
-		if err := nc.Drain(); err != nil {
+		// ctx is already cancelled on SIGTERM by the time this defer runs, so
+		// the drain deadline must not be derived from it.
+		dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
+		defer cancel()
+		if err := natsutil.Drain(dctx, nc); err != nil {
 			slog.Error("nats drain", "error", err)
 		}
 	}()
```

Ensure `"time"` is imported.

- [ ] **Step 4: Convert `user-presence-service/sync`**

```go
 	defer func() {
-		if err := nc.Drain(); err != nil {
+		// ctx is already cancelled on SIGTERM by the time this defer runs, so
+		// the drain deadline must not be derived from it.
+		dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
+		defer cancel()
+		if err := natsutil.Drain(dctx, nc); err != nil {
 			slog.Warn("nats drain", "error", err)
 		}
 	}()
```

Ensure `"time"` is imported.

- [ ] **Step 5: Build, test, lint**

Run: `make build SERVICE=botplatform-service && make build SERVICE=admin-service && make build SERVICE=teams-room-creation && make build SERVICE=user-presence-service`

Run: `make test`

Run: `make lint`

Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "refactor: block on NATS drain in inline and deferred shutdown paths

botplatform-service and admin-service drained inline inside a combined
shutdown hook; teams-room-creation and user-presence-service/sync drained
from a defer in their run function. All four returned instantly.

The deferred sites derive their deadline via context.WithoutCancel — the
parent context is already cancelled on SIGTERM, so deriving from it
directly would expire the drain immediately and silently do nothing."
```

---

### Task 5: Convert Shape D — `nc.Close()` sites (2 sites)

**Files:**
- Modify: `hr-sync-worker/main.go:90-104`
- Modify: `teams-hr-sync/main.go:139`

**Interfaces:**
- Consumes: `natsutil.Drain(ctx context.Context, conn *o11ynats.Conn) error` from Task 1.
- Produces: nothing.

**Background:** `hr-sync-worker` closes the connection inside the same hook that disconnects Mongo, after a separate hook has stopped the consumers. Between `cc.Stop()` and `Close()` there are in-flight JetStream acks in the client write buffer; `Close` drops them and those messages redeliver on the next start. CLAUDE.md's worker cleanup order is `iter.Stop()` → `wg.Wait()` → `nc.Drain()` → disconnect databases, so the drain belongs in its own hook between the consumer stop and the Mongo disconnect.

`teams-hr-sync` is **not** losing data today — its `js.PublishMsg` (`main.go:179`) is synchronous and every `PubAck` is in hand before the deferred `Close` runs. Convert it for consistency and to stop the pattern being copied, not to fix a live bug.

- [ ] **Step 1: Split `hr-sync-worker`'s drain into its own hook**

```go
 	shutdown.Wait(ctx, 25*time.Second,
 		func(_ context.Context) error {
 			for _, cc := range consumeCtxs {
 				cc.Stop()
 			}
 			return nil
 		},
 		func(ctx context.Context) error { return healthStop(ctx) },
+		func(ctx context.Context) error { return natsutil.Drain(ctx, nc) },
 		func(ctx context.Context) error {
 			mongoutil.Disconnect(ctx, mongoClient)
-			nc.Close()
 			return nil
 		},
 		func(ctx context.Context) error { return obsShutdown(ctx) },
 	)
```

- [ ] **Step 2: Convert `teams-hr-sync`**

```go
-	defer nc.Close()
+	defer func() {
+		// ctx is already cancelled on SIGTERM by the time this defer runs, so
+		// the drain deadline must not be derived from it.
+		dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
+		defer cancel()
+		if err := natsutil.Drain(dctx, nc); err != nil {
+			slog.Error("nats drain", "error", err)
+		}
+	}()
```

Ensure `"time"` and `"log/slog"` are imported.

- [ ] **Step 3: Verify no stray closes remain**

Run: `grep -rn "nc\.Close()" --include="*.go" . | grep -v _test | grep -v tools/`

Expected: exactly one line — `pkg/testutil/nats_binary.go:68`. That is test-harness teardown, not a service shutdown path; **leave it alone**. Before this task the same grep also lists `teams-hr-sync/main.go:139` and `hr-sync-worker/main.go:100`, which are the two sites being converted here.

- [ ] **Step 4: Build, test, lint**

Run: `make build SERVICE=hr-sync-worker && make build SERVICE=teams-hr-sync`

Run: `make test`

Run: `make lint`

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor: drain instead of close in hr-sync-worker and teams-hr-sync

hr-sync-worker closed the connection after stopping its consumers, dropping
buffered JetStream acks and forcing redelivery. Its drain now has its own
hook between the consumer stop and the Mongo disconnect, matching the
documented worker cleanup order.

teams-hr-sync was not losing data — its JetStream publishes are synchronous
and acked before the defer ran — but is converted so the pattern is not
copied."
```

---

### Task 6: Convert `data-migration` shutdown paths (5 sites)

**Files:**
- Modify: `data-migration/oplog-connector/main.go:97` and `:234`
- Modify: `data-migration/oplog-transformer/main.go:131`
- Modify: `data-migration/oplog-direct-transfer/main.go:149`
- Modify: `data-migration/oplog-collections-transformer/main.go:183`

**Interfaces:**
- Consumes: `natsutil.Drain(ctx context.Context, conn *o11ynats.Conn) error` from Task 1.
- Produces: nothing.

**Background — read before touching anything here.** These four binaries contain **19** `nc.Drain()` calls. Only the 5 listed above are shutdown paths. The other **14 are startup error paths and must be left exactly as they are**:

```go
js, err := nc.JetStream()
if err != nil {
	slog.Error("jetstream init failed", "error", err)
	_ = nc.Drain()
	mongoutil.Disconnect(ctx, client)
	os.Exit(1)
}
```

Nothing has been published at that point, so there is nothing buffered to flush, and blocking up to 15s before an error exit is strictly worse than the current behavior. Converting them would be a regression, not a completion.

The 14 to leave alone: `oplog-connector:135,140,147`; `oplog-transformer:82,108,118`; `oplog-direct-transfer:96,104,127,138`; `oplog-collections-transformer:111,119,158,169`.

- [ ] **Step 1: Convert the four `shutdown.Wait` hooks**

`oplog-connector:97`:

```go
-		func(context.Context) error { return conn.nc.Drain() },
+		func(ctx context.Context) error { return natsutil.Drain(ctx, conn.nc) },
```

`oplog-transformer:131`, `oplog-direct-transfer:149`, `oplog-collections-transformer:183`:

```go
-		func(context.Context) error { return nc.Drain() },
+		func(ctx context.Context) error { return natsutil.Drain(ctx, nc) },
```

- [ ] **Step 2: Convert `oplog-connector`'s shutdown function**

At `oplog-connector/main.go:234`, inside the function that already does a bounded `awaitWatchers`:

```go
 	c.beginShutdown()
 	wctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
 	defer cancel()
 	if err := c.awaitWatchers(wctx); err != nil {
 		slog.Warn("watcher drain incomplete", "error", err)
 	}
-	_ = c.nc.Drain()
+	dctx, dcancel := context.WithTimeout(context.Background(), 15*time.Second)
+	defer dcancel()
+	if err := natsutil.Drain(dctx, c.nc); err != nil {
+		slog.Warn("nats drain incomplete", "error", err)
+	}
 	mongoutil.Disconnect(context.Background(), c.client)
```

This one already derives from `context.Background()`, so no `WithoutCancel` is needed.

- [ ] **Step 3: Confirm the 14 error paths are untouched**

Run: `grep -rn "_ = nc.Drain()" --include="*.go" data-migration | wc -l`

Expected: `14`. If this prints anything else, an error path was converted by mistake — revert it.

- [ ] **Step 4: Build, test, lint**

Run: `make build SERVICE=data-migration/oplog-connector && make build SERVICE=data-migration/oplog-transformer && make build SERVICE=data-migration/oplog-direct-transfer && make build SERVICE=data-migration/oplog-collections-transformer`

Run: `make test`

Run: `make lint`

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor: block on NATS drain in data-migration shutdown paths

Converts the four shutdown.Wait hooks and oplog-connector's own shutdown
function. The 14 startup error-path drains are deliberately left alone:
they run before anything is published and are followed immediately by
os.Exit(1), so blocking there would only delay an error exit."
```

---

### Task 7: Final verification sweep

**Files:** none modified.

**Interfaces:** none.

- [ ] **Step 1: Confirm no unconverted drain or close sites remain outside `tools/`**

Run: `grep -rn "nc\.Drain()\|nc\.Close()\|NatsConn()\.Drain()" --include="*.go" . --exclude="*_test.go" | grep -v "^./tools/" | grep -v "_ = nc.Drain()" | grep -v "^\./pkg/"`

Expected: exactly one line — the comment at `outbox-worker/main.go:200`, which mentions `nc.Drain()` in prose while describing a WaitGroup race and is not a call site. Any other hit is a site the sweep missed.

The grep deliberately excludes `pkg/`: `pkg/natsrouter/router.go:268` calls `Subscription.Drain()`, which is a different method with its own correct wait already implemented, and `pkg/natsutil/drain.go` is the new helper itself. Before this task's conversions the same grep returns 31 lines (25 root-service sites + 5 in `data-migration` + that one comment).

- [ ] **Step 2: Full test suite with race detector**

Run: `make test`

Expected: PASS across all packages.

- [ ] **Step 3: Lint**

Run: `make lint`

Expected: no findings.

- [ ] **Step 4: SAST**

Run: `make sast`

Expected: no medium-or-higher findings. This is a blocking CI gate, so run it before pushing rather than discovering it in CI.

- [ ] **Step 5: Coverage check on the new code**

Run: `go test -race -coverprofile=/tmp/claude-0/-home-user-newchat/8d24fbda-5dbb-53c0-8a07-ddeccbb30dc1/scratchpad/cover.out ./pkg/natsutil/... && go tool cover -func=/tmp/claude-0/-home-user-newchat/8d24fbda-5dbb-53c0-8a07-ddeccbb30dc1/scratchpad/cover.out | grep -E "drain.go|slowconsumer.go|total:"`

Expected: `drain.go` and `slowconsumer.go` functions at 90%+, package total above the 80% floor.

- [ ] **Step 6: Push**

```bash
git push -u origin claude/nats-graceful-shutdown-b40zva
```

Do not open a PR unless explicitly asked.

---

## Notes for the reviewer

**No wire, schema, or config change.** Nothing here is client-facing, so `docs/client-api.md` and its derived views are untouched.

**Expected behavior change:** services will now take up to ~20s to exit instead of exiting instantly. That is the point — but it will look like a pod-restart regression on first deploy. A service that sits at the full 15s drain timeout has a wedged subscription handler, which is a real finding, not a bug in this change.

**Deliberately out of scope**, so the omissions are not read as oversights:
- `SetPendingLimits` tuning for `broadcast-worker` — diagnose with the new counter first, tune with real numbers second.
- `tools/loadgen` (11 files) — a dev harness where a publish lost at exit changes nothing.
- The 14 `data-migration` startup error paths (Task 6).
- Payload size checks and `NoResponders` error mapping — PR 2, separate spec.
