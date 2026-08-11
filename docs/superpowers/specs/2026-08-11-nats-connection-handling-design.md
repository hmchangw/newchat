# NATS Connection Handling: Real Drain + Slow Consumer Visibility (PR 1 of 2)

Four gaps were raised against our NATS usage: graceful shutdown, slow consumer
warnings, payload size checks, and `NoResponders` in request/reply errors. They
split cleanly along a seam: two are properties of the *connection* and belong in
`pkg/natsutil`; two are call-site sweeps that touch handler code and the client
API docs.

This spec covers **PR 1** — the connection-level pair. PR 2 (payload size checks
and `NoResponders` mapping) gets its own spec.

## Scope

| Change | Where |
|--------|-------|
| `natsutil.Drain` — make graceful shutdown actually wait | new, `pkg/natsutil/drain.go` |
| `nats.DrainTimeout` default | `pkg/natsutil/connect.go` |
| Slow consumer diagnostics + counter | `pkg/natsutil/connect.go`, new `pkg/natsutil/slowconsumer.go` |
| Call-site conversion | 25 sites in root services + 5 in `data-migration` |

### Out of scope

- **`SetPendingLimits`.** Raising the per-subscription buffer (default 512k msgs
  / 64MB) is the actual *remedy* for fanout pressure in `broadcast-worker`. It is
  per-subscription, so it cannot be a `Connect` default, and picking a number
  before we can see drop counts would be guessing. Diagnose first (this spec),
  tune second (follow-up, with real numbers).
- **`tools/loadgen`** (11 files). A dev harness — a publish lost at exit changes
  nothing. Converting it would triple the diff for no operational gain.
- Payload size checks and `NoResponders` mapping — PR 2.

## The bug

`nc.Drain()` does not drain. From `nats.go@v1.50.0:6055-6075`:

```go
func (nc *Conn) Drain() error {
	nc.mu.Lock()
	if nc.isClosed() {
		nc.mu.Unlock()
		return ErrConnectionClosed
	}
	if nc.isConnecting() || nc.isReconnecting() {
		nc.mu.Unlock()
		nc.Close()
		return ErrConnectionReconnecting
	}
	if nc.isDraining() {
		nc.mu.Unlock()
		return nil
	}
	nc.changeConnStatus(DRAINING_SUBS)
	go nc.drainConnection()
	nc.mu.Unlock()

	return nil
}
```

Its own doc comment names the fix: *"Use the ClosedCB option to know when the
connection has moved from draining to closed."* Nothing in this repo does.

It flips status, spawns `drainConnection` on a goroutine, and returns `nil`. All
the real work — draining each subscription, then `FlushTimeout(5s)` to push
buffered bytes to the server, then `Close()` — happens on that goroutine.

Every service does this:

```go
func(ctx context.Context) error { return nc.Drain() },
```

The hook returns `nil` instantly. `shutdown.Wait` proceeds to the remaining
hooks, `main` returns, the process exits, and `drainConnection` dies wherever it
got to. What is in the client write buffer at that moment is lost: JetStream
acks (→ redelivery), request/reply responses (→ caller timeout), OUTBOX forwards.

The trap is documented at exactly one site, `admin-service/main.go:93-95`:

```go
// srv.Shutdown has already waited out any in-flight toggle, so
// Drain (which returns immediately and finishes in the background)
// only closes the idle connection.
```

The reasoning is locally sound — `admin-service` is HTTP-only with an idle NATS
connection — but the behavior it describes was never generalized to the 22 sites
where the connection is *not* idle.

**Secondary:** `nats.DrainTimeout` is never set, so it defaults to 30s
(`DefaultDrainTimeout`, `nats.go:65`). That is larger than the 25s budget and equal to
the Kubernetes `terminationGracePeriodSeconds`, so the internal timeout can never
be the thing that fires — SIGKILL beats it.

## Design

### A. `natsutil.Drain`

New file `pkg/natsutil/drain.go`:

```go
// Drain puts the connection into the drain state and blocks until it reaches
// CLOSED or ctx expires.
//
// nats.Conn.Drain() only *starts* the drain — it spawns drainConnection on a
// goroutine and returns nil immediately — so a shutdown hook that calls it
// directly returns before subscriptions have drained and before the final
// publish flush, and the process exits with buffered acks and replies still in
// the write buffer.
func Drain(ctx context.Context, conn *o11ynats.Conn) error {
	nc := conn.NatsConn()

	// Register before Drain so a fast close cannot fire in the gap between the
	// two calls; re-check IsClosed after, to cover a close that landed before
	// the listener was in place. Together these close both sides of the race.
	ch := nc.StatusChanged(nats.CLOSED)
	defer nc.RemoveStatusListener(ch)

	if err := nc.Drain(); err != nil {
		switch {
		case errors.Is(err, nats.ErrConnectionClosed):
			return nil
		case errors.Is(err, nats.ErrConnectionReconnecting):
			// Drain hard-closed the connection. Buffered publishes are gone,
			// but there was no reachable server to flush them to — this is a
			// normal shutdown-during-outage, not a failure to report.
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

Why `StatusChanged` rather than a `ClosedHandler`: `connect.go:53` already
installs a `ClosedHandler` for logging, and `nats.Option`s of the same kind
overwrite rather than compose. `Conn.StatusChanged` (`nats.go:6379`) is an
independent listener registry, so it does not contend for that slot. The channel
it returns is buffered (cap 10) and `changeConnStatus` (`nats.go:6451`) fires
`sendStatusEvent` on every transition, so the CLOSED signal cannot be missed
once the listener is registered.

The two benign errors matter for noise: today `ErrConnectionClosed` and
`ErrConnectionReconnecting` propagate out of the hook and surface as shutdown
errors on ordinary restarts.

`ctx` is the one `shutdown.Wait` passes, so the wait shares the same 25s budget
as every other hook and cannot outlive it.

### B. Drain timeout

Add to `baseOpts` in `Connect`:

```go
nats.DrainTimeout(defaultDrainTimeout), // 10 * time.Second
```

Worst case: 10s subscription drain + `drainConnection`'s internal
`FlushTimeout(5s)` = 15s, inside the 25s shutdown budget, inside the 30s
Kubernetes grace period.

The 25s is **shared, not per-hook**: `shutdown.Wait` creates one context for the
whole shutdown and runs the hooks sequentially over it
(`pkg/shutdown/shutdown.go:21-32`). A 15s worst-case drain therefore leaves ~10s
for every remaining hook — DB disconnects, HTTP shutdown, o11y flush — which are
sub-second in practice. A drain that genuinely needs more than 10s means a
wedged handler, which is a finding to surface rather than a wait to extend.

A package const rather than an env var — no service should want a different
answer, and `Connect` already appends caller `opts` after `baseOpts`
(`connect.go:60`), so a caller that genuinely needs to override still can.

### C. Slow consumer diagnostics

`connect.go:56` discards the subscription:

```go
nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
	log.Error("nats async error", "error", err)
}),
```

A slow consumer logs `nats async error: nats: slow consumer, messages dropped`
— no subject, no queue group, no count. Nothing to act on, nothing to alert on.
Replace with:

```go
nats.ErrorHandler(func(_ *nats.Conn, sub *nats.Subscription, err error) {
	if errors.Is(err, nats.ErrSlowConsumer) {
		logSlowConsumer(log, sub)
		return
	}
	log.Error("nats async error", "error", err)
}),
```

New file `pkg/natsutil/slowconsumer.go`:

- `subDropped(sub *nats.Subscription) (int, bool)` — one read of `Dropped()`,
  reporting whether it could be read at all. The caller threads the result into
  both the level decision and the field list, so a subscription that closes
  mid-report cannot produce an ERROR line saying "messages dropped" whose field
  set silently omits the count.
- `slowConsumerFields(sub *nats.Subscription, dropped int, ok bool) []any` —
  returns `subject`, `queue`, `dropped`, `pending_msgs`, `pending_bytes`,
  `limit_msgs`, `limit_bytes`. Split out from the handler purely so it is
  testable against a real subscription. It must tolerate `sub == nil`
  (connection-level async errors pass nil) and the errors `Pending()` /
  `PendingLimits()` return on an already-closed subscription, omitting what it
  cannot read rather than reporting a bogus zero — a diagnostic path must never
  panic during shutdown.
- `logSlowConsumer(log *slog.Logger, sub *nats.Subscription)` — logs at **ERROR**
  when the drop count is above zero, **WARN** otherwise. Dropped messages on a
  core NATS subscription are unrecoverable loss and should page; a slow consumer
  that has not yet dropped anything is a warning.
- An otel counter `nats_slow_consumer_events_total{subject,queue}`, following
  the `pkg/roomkeymetrics` shape (package-level var, `init()` from
  `otel.Meter(...)`, no-op fallback) so it records unconditionally without a
  wiring precondition.

  **It counts episodes, one per callback — not messages.** `Subscription.Dropped()`
  is a cumulative total never reset except on unsubscribe (`nats.go:3770`,
  `:5574-5584`), and the async error callback fires only on the
  Active→SlowConsumer transition (`nats.go:3771-3787`), with `sub.sc` cleared
  again on the next successful delivery (`:3742`). Adding `Dropped()` per episode
  would re-add every earlier episode's drops — 3 dropped then 5 dropped would
  report 3 + 8 = 11. The callback is also dispatched through an async queue
  (`:3785`), so a `Dropped()` read inside it is not even the count at episode
  time. Exact per-episode numbers belong in the log fields, where they are read
  once and reported accurately.

### D. Call-site conversion

Four distinct shapes, not one. Each needs its own treatment.

**Shape A — hook lambda (19 sites).** Mechanical:

```go
-func(ctx context.Context) error { return nc.Drain() },
+func(ctx context.Context) error { return natsutil.Drain(ctx, nc) },
```

`bot-message-handler:92`, `bot-message-worker:177`, `bot-room-service:109`,
`broadcast-worker:275`, `history-service/cmd:242`, `inbox-worker:776`,
`media-service:107`, `message-gatekeeper:222`, `message-worker:271`,
`notification-worker:390`, `outbox-worker:186`, `push-notification-service:118`,
`room-service:306`, `room-worker:312`, `search-service:265`,
`search-sync-worker:351`, `translation-service:121`, `user-presence-service:163`,
`user-service:146`.

Note the sites taking `_ context.Context` need the parameter named to pass it on.

**Shape B — inline inside a combined hook, error downgraded to a log (2 sites).**
`botplatform-service:127`, `admin-service:98`. Both sit inside a hook that also
shuts down the HTTP server and disconnects Mongo/Valkey, and both swallow the
drain error into `slog.Warn`. Convert to `natsutil.Drain(ctx, nc)` in place,
keeping the existing swallow — the surrounding hook returns the *HTTP server's*
error, and changing which error wins is a behavior change outside this spec's
scope. Delete the now-false comment at `admin-service:93-95`.

**Shape C — `defer` in a run function (2 sites).** `teams-room-creation:71`,
`user-presence-service/sync:106`. Same bug in a different wrapper: the deferred
`Drain` returns instantly and `run` returns to `main`, which exits. These have no
`shutdown.Wait` ctx to borrow, so give them a bounded one:

```go
defer func() {
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 20*time.Second)
	defer cancel()
	if err := natsutil.Drain(dctx, nc); err != nil {
		slog.Error("nats drain", "error", err)
	}
}()
```

`context.WithoutCancel` is load-bearing: on SIGTERM the parent `ctx` is already
cancelled by the time the defer runs, so deriving from it directly would expire
the drain immediately.

**Shape D — `nc.Close()` (2 sites).**

- `hr-sync-worker:100` — closes after stopping consumers, inside the same hook
  that disconnects Mongo. Between `cc.Stop()` and `Close()` there are in-flight
  JetStream acks in the write buffer; `Close` drops them and the messages
  redeliver. Split into its own drain hook, ordered after the consumer stop and
  before the Mongo disconnect, per the CLAUDE.md worker cleanup order.
- `teams-hr-sync:139` — `defer nc.Close()` in a one-shot CronJob. **Not** losing
  data today: `js.PublishMsg` (`main.go:179`) is synchronous and every `PubAck`
  is in hand before the defer runs. Convert for consistency and to keep the
  pattern from being copied, not to fix a live bug.

**Shape E — startup error path (14 sites, deliberately NOT converted).**
Across the four `data-migration` binaries, most `_ = nc.Drain()` calls look like
this:

```go
js, err := nc.JetStream()
if err != nil {
	slog.Error("jetstream init failed", "error", err)
	_ = nc.Drain()
	mongoutil.Disconnect(ctx, client)
	os.Exit(1)
}
```

This is best-effort cleanup on a startup failure, followed immediately by
`os.Exit(1)`. Nothing has been published yet, so there is nothing buffered to
flush, and blocking up to 15s before an error exit is strictly worse than the
current behavior. Left as-is. Listed here so a reviewer does not read the
omission as an oversight.

**`data-migration` real shutdown sites (5).** `oplog-connector:97` and `:234`,
`oplog-transformer:131`, `oplog-direct-transfer:149`,
`oplog-collections-transformer:183` — the `shutdown.Wait` hooks plus
`oplog-connector`'s own shutdown function, which already does a bounded
`awaitWatchers` before draining. These move real data; converted with the rest.

## Testing

TDD, red first. `pkg/natsutil` already runs an embedded NATS server in
`reply_test.go:40`, so all of this is testable without mocks or a container.

`drain_test.go`:

| Case | Assertion |
|------|-----------|
| Happy path | subscribe + publish, `Drain` returns nil, `nc.IsClosed()` is true on return |
| Actually blocks | a handler held open by a channel; `Drain` must not return before it completes |
| Ctx expiry | wedged handler + already-expired ctx → error wrapping `context.DeadlineExceeded` |
| Already closed | `nc.Close()` then `Drain` → nil |
| Idempotent | second `Drain` → nil (library already guards via `isDraining`; a concurrent second caller still waits for CLOSED) |
| Nil conn | returns nil, no panic |
| `DrainTimeout` applied | option present on the connection |

`slowconsumer_test.go`:

| Case | Assertion |
|------|-----------|
| Real subscription | all seven fields present and correctly typed |
| `nil` subscription | no panic, degraded field set |
| Closed subscription | no panic, errors from `Dropped`/`Pending` handled |
| Level selection | ERROR when `dropped > 0`, WARN when 0 |

Listener cleanup is structural — `defer nc.RemoveStatusListener(ch)` covers every
exit path — so it is reviewed, not separately asserted. There is no public API
to count registered listeners.

Coverage: 80% floor repo-wide, 90% target for `pkg/`.

## Rollout

No wire, schema, or config change. No `docs/client-api.md` impact — nothing here
is client-facing. Behavior change is confined to shutdown, where services will
now take up to ~15s to exit instead of exiting instantly, and slow consumer
events become visible.

The one risk worth watching: a service with a wedged subscription handler that
previously exited instantly will now sit until the 10s drain timeout. That is
the correct behavior — it is the signal that the handler is wedged — but it will
look like a regression in pod restart time on first deploy. Expected, not a bug.
