# JetStream Exponential Backoff — Common Consumer Default

**Date:** 2026-08-21
**Status:** Approved for implementation
**Scope:** the 9 services using `stream.DurableConsumerDefaults`, plus `bot-message-worker` and `push-notification-service`

## 1. Problem

Redelivery timing is inconsistent across the repo's JetStream consumers, and three
distinct bugs follow from it.

**1a. Five consumers hot-loop on failure.** A bare `Nak()` puts the message straight
on the redeliver queue and calls `signalNewMessages()` — the consumer's `BackOff` is
never consulted (`nats-server/server/consumer.go:3308-3311`, nats-io/nats-server#5856).
`NakWithDelay(0)` is identical, because the nats.go client only serializes a delay
when it is `> 0` (`jetstream/message.go:374`). So a 200 ms Mongo blip burns all five
delivery attempts in milliseconds and the message is dropped.

| Service | Site | Call |
|---|---|---|
| message-gatekeeper | `handler.go:206` | `msg.Nak()` |
| inbox-worker | `main.go:809` | `msg.Nak()` |
| search-sync-worker | `handler.go:111,221,261` | `natsutil.Nak` → `msg.Nak()` |
| bot-message-worker | `handler.go:45` | `msg.NakWithDelay(0)` |
| push-notification-service | `handler.go:44` | `msg.NakWithDelay(0)` |

`bot-message-worker/handler.go:44` carries the comment "NakWithDelay(0) defers to the
consumer's BackOff schedule." That is not what happens.

**1b. Three consumers silently run a 1-second `AckWait`.** nats-server overwrites
`AckWait` with `BackOff[0]` (`consumer.go:677-682`):

```go
// If BackOff was specified that will override the AckWait and the MaxDeliver.
if len(config.BackOff) > 0 {
    config.AckWait = config.BackOff[0]
}
```

search-sync-worker (`main.go:518`), bot-message-worker (`main.go:129`) and
push-notification-service (`main.go:71`) all set `BackOff[0] = 1s`. Their declared
`AckWait: 30 * time.Second` is dead code. Any ES bulk request or FCM dispatch over
one second is redelivered while still in flight.

**1c. Ack-timeout redelivery has no backoff anywhere.** The six services that settle
correctly through `jsretry` set no consumer `BackOff` at all, so a message whose pod
crashed retries flat every 30 s, five times, and is dropped 2.5 minutes later — the
case where backoff matters most, since a crash-looping pod re-crashes on the same
message.

## 2. Key mechanism: the two levers are disjoint

| | Lever A — consumer `BackOff` | Lever B — `jsretry` → `NakWithDelay` |
|---|---|---|
| Owner | nats-server | `pkg/jsretry` |
| Fires on | message un-acked past `AckWait` (crash, OOM, hang, slow handler) | handler caught a transient error and Nak'd |
| Jitter | impossible — a literal duration list | possible, client-side |
| Overridden by | `NakWithDelay(d)` | — |

Neither lever covers the other's failure mode, so the design sets both. `MaxDeliver`
is shared and caps whichever path a message takes.

Two server-side constraints bind the design:

- `AckWait` is overwritten by `BackOff[0]` (`consumer.go:677-682`).
- `len(BackOff) > MaxDeliver` is a hard error at consumer create **and** update
  (`consumer.go:807`, `consumer.go:2588`), except when MaxDeliver means unlimited: the
  server normalizes `0` and `< -1` to `-1` before that check (`consumer.go:612-617`).

## 3. Reference values

| Source | Value |
|---|---|
| NATS CLI defaults (`natscli/cli/consumer_command.go:143-146`) | `--backoff linear --backoff-steps 10 --backoff-min 1m --backoff-max 20m`; auto-sets `MaxDeliver = len(BackOff)+1` |
| Synadia reference worker (JetStream DLQ/replay guide) | `BackOff{1s, 5s, 30s, 2m}`, `MaxDeliver: 5` |
| AWS Architecture Blog, *Exponential Backoff and Jitter* | equal jitter `base/2 + rand(0, base/2)` |

NATS' own defaults start at a **minute**, not a second, because Lever A is about ack
timeouts rather than application retries. That is why the Lever A schedule below is
anchored at `AckWait` instead of at `1s`.

Agreed retry budget: **~15 minutes**, after which JetStream drops the message. Drops
are already observable — `pkg/natsmetrics` emits `TerminalMaxDeliver`. There is no DLQ
stream, and stream retention is ops/IaC-owned (`pkg/stream.Config` carries only `Name`
and `Subjects`), so it is not a backstop this repo can reason about.

## 4. Design

### 4.1 `pkg/stream` — derive the schedule, don't accept it

A raw `[]time.Duration` env knob would reproduce bug 1b: an operator setting
`CONSUMER_BACKOFF=1s,...` silently drops `AckWait` to 1 s. It also has no off-switch —
verified against `caarlos0/env` v11.4.0, an empty env value falls back to `envDefault`
rather than yielding an empty slice.

So the env surface expresses the *shape*, and the schedule is derived from `AckWait`:

```go
type ConsumerSettings struct {
	AckWait       time.Duration `env:"ACK_WAIT"        envDefault:"30s"`
	MaxDeliver    int           `env:"MAX_DELIVER"     envDefault:"6"`
	MaxWaiting    int           `env:"MAX_WAITING"     envDefault:"512"`
	MaxAckPending int           `env:"MAX_ACK_PENDING" envDefault:"1000"`

	BackOffSteps  int           `env:"BACKOFF_STEPS"  envDefault:"5"`
	BackOffFactor float64       `env:"BACKOFF_FACTOR" envDefault:"2"`
	BackOffMax    time.Duration `env:"BACKOFF_MAX"    envDefault:"8m"`
}
```

Entry `i` is `AckWait * Factor^i`, capped at `BackOffMax`. Defaults yield
`{30s, 1m, 2m, 4m, 8m}` — **15.5 min over `MaxDeliver=6`**.

`BackOff[0] == AckWait` holds *by construction*, so bug 1b becomes unrepresentable
rather than merely validated. The vocabulary deliberately mirrors natscli's
`--backoff-steps` / `--backoff-min` / `--backoff-max`, with `AckWait` as `min`.

`DurableConsumerDefaults` keeps its signature — no churn across nine call sites — and
enforces:

1. `BackOffSteps <= 0`, or `AckWait <= 0` → `cc.BackOff` stays `nil`, flat `AckWait`
   retry. **`CONSUMER_BACKOFF_STEPS=0` is the off-switch**, reverting any service to
   today's behavior with an env change and no code deploy.
2. `BackOffSteps > MaxDeliver` and `MaxDeliver > 0` → clamp to `MaxDeliver` and
   `slog.Warn`, since the server hard-rejects the excess. `MaxDeliver` of `0` or `< -1`
   means unlimited (the server normalizes both to `-1`) and is not clamped.
   Clamp-and-warn follows the precedent in
   `data-migration/oplog-transformer/config.go:42-45`.
3. `BackOffFactor < 1` → treated as `1` (flat schedule); a shrinking backoff is never
   intended.

Each entry is capped at `BackOffMax` and the multiplication is overflow-guarded: once
a step reaches `BackOffMax`, every later step is `BackOffMax`.

A service needing a bespoke irregular schedule may still assign `cc.BackOff` after the
call — the helper supplies a default, it does not seal the field.

### 4.2 `pkg/jsretry` — longer tail, equal jitter, and a settle-less `Nak`

```go
var DefaultBackoff = []time.Duration{
	1 * time.Second, 5 * time.Second, 30 * time.Second, 2 * time.Minute, 10 * time.Minute,
}

var LowLatencyBackoff = []time.Duration{
	200 * time.Millisecond, 1 * time.Second, 5 * time.Second, 30 * time.Second,
}
```

`DefaultBackoff` keeps Synadia's published four-entry shape verbatim and appends one
`10m` tail step → 12.6 min over five gaps, matching the agreed budget.

`LowLatencyBackoff` is deliberately **not** extended. broadcast-worker is user-visible
fan-out; a 15-minute-late broadcast is worthless, so it keeps its short budget.

Equal jitter is applied inside `backoffFor`, the only place it can be — Lever A cannot
jitter:

```go
// Equal jitter (AWS): d/2 + rand(0, d/2). Guarantees at least half the base
// delay while decorrelating a fleet that all parked during the same outage.
// #nosec G404 -- retry jitter, not security-sensitive
return d/2 + time.Duration(rand.Int64N(int64(d/2)+1))
```

`math/rand/v2` trips gosec **G404**, and per CLAUDE.md a `//nolint:gosec` directive does
not suppress standalone gosec — the native `// #nosec G404 -- reason` comment on the
line directly above is required.

New export, so the five hot-loop sites can be fixed without restructuring five handlers
into error-returning shapes:

```go
// Nak schedules `msg` for redelivery after the backoff delay for this attempt,
// jittered. Use this instead of a bare msg.Nak(): a bare Nak is an *instant*
// redelivery that ignores the consumer's BackOff schedule entirely
// (nats-server/server/consumer.go:3308-3311), so a brief downstream blip burns
// MaxDeliver in milliseconds. `reason` is a short label describing why.
func Nak(ctx context.Context, msg Msg, backoff []time.Duration, reason string)
```

`Settle` / `SettleQuiet` keep their signatures; jitter is internal to both.

### 4.3 `pkg/natsutil` — remove `Nak`

`natsutil.Nak` is a bare-`Nak()` wrapper used only by search-sync-worker's three sites.
Once they migrate it is deleted, so the hot-loop cannot be reintroduced by reaching for
the obvious-looking helper. `natsutil.Ack` stays.

### 4.4 Per-service migration

| Service | Change |
|---|---|
| message-gatekeeper | `handler.go:206` `msg.Nak()` → `jsretry.Nak(ctx, msg, jsretry.DefaultBackoff, …)`. Existing `slog.ErrorContext` above it stays — it logs the cause, `jsretry.Nak` does not re-log the business error. |
| inbox-worker | `main.go:809` → `jsretry.Nak`. |
| search-sync-worker | Delete the hardcoded `cc.BackOff` at `main.go:518` (bug 1b). Three `natsutil.Nak` → `jsretry.Nak`. |
| bot-message-worker | Adopt `stream.ConsumerSettings` + `DurableConsumerDefaults`; delete the inline `BackOff` at `main.go:129` and the standalone `MaxDeliver` env field. `handler.go:45` `NakWithDelay(0)` → `jsretry.Nak`; delete the incorrect comment. |
| push-notification-service | Same: adopt `ConsumerSettings`, delete inline `BackOff` at `main.go:71` and the `MaxDeliver` field; `handler.go:44` → `jsretry.Nak`. |
| hr-sync-worker | Builds `ConsumerSettings` as a struct literal (`main.go:112`); add `BackOffSteps/Factor/Max` so it matches the common default. `MaxDeliver: -1` exempts it from the length rule. |
| message-worker, broadcast-worker, notification-worker, room-worker, outbox-worker | No code change — they inherit the new defaults through `DurableConsumerDefaults`. |

`outbox-worker`'s ordered lanes (`MaxAckPending=1`, `MaxDeliver=-1`) keep `AckWait=30s`
because `BackOff[0] == AckWait`; the FIFO probe cadence for a down peer is unchanged for
the first attempt and then spaces out, which is the desired behavior.

### 4.5 Documentation

CLAUDE.md §6 "JetStream Consumer Pattern" gains the two-lever rule: which lever fires on
which failure, that a bare `Nak()` ignores `BackOff`, that `BackOff[0]` overwrites
`AckWait`, and that `len(BackOff) <= MaxDeliver` is server-enforced.

No `docs/client-api.md` change — no client-facing handler or `pkg/model` wire struct is
touched.

## 5. Testing

TDD per CLAUDE.md §4: tests first, confirmed failing, then implementation.

**`pkg/stream/consumer_test.go`** — table-driven over `(AckWait, Steps, Factor, Max,
MaxDeliver)`:
- defaults → exactly `{30s, 1m, 2m, 4m, 8m}`
- `BackOff[0] == AckWait` for every non-empty case (the bug-1b invariant)
- `Steps=0` → `nil` BackOff, `AckWait` preserved
- `Steps > MaxDeliver` → clamped to `MaxDeliver`
- `MaxDeliver=-1` → no clamp
- `Factor < 1` → flat schedule
- `BackOffMax` caps and all later entries stay at the cap
- zero `ConsumerSettings{}` → all-zero config (existing test must keep passing)
- env round-trip: new `CONSUMER_BACKOFF_*` defaults and overrides

**`pkg/jsretry/jsretry_test.go`** — existing exact-delay assertions become bounded
assertions `[d/2, d]`; index selection by `NumDelivered` and last-entry reuse retained;
new `Nak` covered for delay selection, nak-failure logging, and metadata-error fallback.

**`pkg/stream` integration test** (`//go:build integration`, `testutil.NATS(t)`) — create
a real consumer from `DurableConsumerDefaults` and read back
`cons.CachedInfo().Config`, asserting the server-side `AckWait` equals `BackOff[0]`.
This is the test that would have caught bug 1b, and it must be an integration test
because the overwrite happens server-side.

**Per-service `consumer_config_test.go`** — no change needed. All eight build
`ConsumerSettings` as struct literals with explicit `AckWait`/`MaxDeliver`, so the new
`BackOff*` fields default to zero and `cc.BackOff` stays nil. Propagation is covered in
`pkg/stream`, where it belongs. The one exception is search-sync-worker, whose assertions
on the hardcoded `{1s, 5s, 30s}` (`consumer_config_test.go:57,82`) are replaced.

**New** `consumer_config_test.go` for bot-message-worker and push-notification-service,
which have none today.

Coverage floor of 80% applies; `pkg/stream` and `pkg/jsretry` target 90%+ as shared
`pkg/` code.

## 6. Risks

| Risk | Mitigation |
|---|---|
| `CreateOrUpdateConsumer` must update `BackOff`/`MaxDeliver` on existing durables | Supported on 2.10+; server re-runs only the length check on update (`consumer.go:2588`). natscli refuses to edit `AckWait` on a backoff consumer, but that is a CLI-side guard, not a server one. |
| Longer retention of failing messages consumes `MaxAckPending` budget | `MaxAckPending=1000` default is unchanged; a sustained outage parks at most that many. `CONSUMER_BACKOFF_STEPS=0` is the per-service escape hatch. |
| `MaxDeliver` 5 → 6 shifts `TerminalMaxDeliver` metric timing | Metric reads the configured value at runtime; no code change needed in `pkg/natsmetrics`. |
| Jitter makes retry timing non-deterministic in tests | Assert bounds `[d/2, d]`, not exact values. |
| Existing hot-looping consumers currently drop fast; after the fix they retain messages ~13 min | Intended — that is the requested budget. Drops remain visible via `TerminalMaxDeliver`. |
