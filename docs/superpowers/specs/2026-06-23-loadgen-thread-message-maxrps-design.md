# Design: `thread` max-rps workload for loadgen

**Date:** 2026-06-23
**Status:** Approved (brainstorm) — pending implementation plan

## Goal

Measure the maximum sustainable RPS for **sending a thread reply** through the
single-site messaging pipeline, using the existing `tools/loadgen` `max-rps`
harness. "Sustainable" means the highest RPS step at which p95/p99 latency and
consumer-pending growth stay within SLO — the same verdict model already used by
the `messages` workload.

The point of the benchmark is to isolate the **extra cost a thread reply pays
over a plain send**, so the two numbers are directly comparable on the same box.

## Background

`max-rps` is a subcommand of the `loadgen` binary with pluggable workloads
(`messages`, `history`, `read-receipt`, `room-read`) behind a small
`rpsWorkload` interface (`ramp.go`):

```go
type rpsWorkload interface {
    RunStep(ctx context.Context, targetRPS int, warmup, hold time.Duration) (rpsStepInputs, error)
    Label() string
}
```

Implementing this interface gets the ramp engine, SLO gating, verdict logic,
report, and CSV output for free. The `messages` workload (`maxrps_messages.go`)
is the direct analog: an open-loop JetStream publisher measuring two latency
edges and the canonical-stream consumer backlog.

### What a thread reply is, on the wire

A thread reply is an ordinary `model.SendMessageRequest` published on the same
`subject.MsgSend(account, roomID, siteID)` subject as a plain send — the *only*
wire difference is that `ThreadParentMessageID` is set to the ID of an existing
message in the same room. (`daily_actions.go:threadReply` already does exactly
this inside the mixed `daily` workload; this design lifts that into an isolated
capacity benchmark.)

### Why a thread reply costs more than a plain send

- **E1 (publish → gatekeeper ack).** When `ThreadParentMessageID != ""`,
  `message-gatekeeper` runs `resolveThreadParentCreatedAt` →
  `parentFetcher.FetchQuotedParent`, a **synchronous NATS request to
  history-service's `GetMessageByID`** (`subject.MsgGet`, reading Cassandra
  `messages_by_id`). Plain sends skip this entirely. This added round-trip is
  the headline difference the benchmark exposes at E1.
- **E2 (publish → broadcast).** Downstream, `message-worker` additionally writes
  `thread_messages_by_thread`, upserts the thread room/subscription, and emits
  tcount-badge (`ServerBroadcastThreadTCount`) + thread-metadata fan-out. The
  reply still produces the same `RoomEvent` that E2 already correlates on, so the
  existing E2 edge keeps working; the extra thread writes show up as added E2
  latency / pending growth, not as a missing signal.

### The hard constraint: parents must really exist

Because the gatekeeper *fetches* the parent (it does not trust the client), every
thread reply must reference a parent message ID that resolves through
history-service. A reply to a non-existent parent makes the gatekeeper Nak for
redelivery (treated as infra failure) — it never acks, so it would register as
pure E1 timeout noise. Therefore parents **must be seeded before the run.**

## Decisions (best-judgment, from brainstorm)

1. **Target:** the thread-reply **send (write)** path. Read-side
   (`GetThreadMessages`) is out of scope — `history-sustained` already exercises
   thread reads.
2. **Injection mode: frontdoor only.** The unique thread cost (the gatekeeper
   parent-fetch) lives only on the frontdoor path; canonical injection bypasses
   the gatekeeper and would measure nothing thread-specific at E1. The `thread`
   workload hard-codes `InjectFrontdoor` and does not accept `--inject`.
3. **Parents pre-seeded in Cassandra**, reusing the existing history-seed
   machinery (`writePlannedMessage`, which already writes both `messages_by_room`
   and `messages_by_id`). Live two-phase (send-parent-then-reply) is rejected: it
   folds parent-send latency and a persistence race into every sample.
4. **Multiple parents per room** (default 8), so thread fan-out spreads across
   several threads per room rather than hammering a single hot thread — the
   realistic steady state. Configurable via the preset.
5. **Integration shape:** a new `thread` workload behind the existing
   `rpsWorkload` interface, plus a `thread` case in `seed` and `teardown`. Not a
   standalone subcommand; not folded into `messages`.
6. **Fixtures: reuse the `messages` presets** (`small`/`medium`/`large`/
   `realistic`) for rooms + subscriptions, and layer the thread parents on top —
   no dedicated `thread-*` presets.

## Architecture

Four pieces, each mirroring an existing analog.

### 1. Thread-parent fixtures + seed (`thread_seed.go`)

- `ThreadFixtures` = the existing `messages` `Fixtures` (rooms, subscriptions,
  room keys) **plus** `ParentsByRoom map[string][]string` — for each room, the
  list of seeded parent message IDs.
- `BuildThreadFixtures(preset, seed, siteID)` builds the base fixtures, then
  deterministically mints `ParentsPerRoom` parent message IDs per room
  (`idgen.GenerateMessageID()` off the seeded RNG so runs are reproducible),
  each authored by a random subscriber of that room.
- `SeedThreadParents(ctx, session, sizer, fixtures, siteID)` writes those parents
  to Cassandra via the existing `writePlannedMessage` path (top-level messages:
  `ThreadParentID == ""`), so history-service's `GetMessageByID` resolves them.
  Rooms + subscriptions + room keys are seeded through the existing `Seed` /
  `SeedRoomKeys` calls first (the gatekeeper still does the membership check).
- We deliberately do **not** pre-seed thread rooms / thread subscriptions in
  Mongo: the first reply to each parent creates them naturally, and that
  transient is absorbed by the ramp's per-step warmup window. Steady-state
  replies (the part that's measured) hit the cheaper "thread already exists"
  path, which is the realistic dominant case.
- `MESSAGE_BUCKET_HOURS` must match the services (already a loadgen config
  concern via `msgbucket.Sizer`); the seed reuses that sizer.

### 2. Thread publish path (extend `Generator`)

`Generator.publishOne` already branches on inject mode. Add a thread mode driven
by an optional `ParentsByRoom` on `GeneratorConfig`:

- When `ParentsByRoom != nil`, after picking a subscription, look up a parent for
  `sub.RoomID`, pick one at random, and build the `SendMessageRequest` with
  `ThreadParentMessageID` set — everything else (request ID, content, collector
  `RecordPublish`, metrics) is unchanged, so E1/E2 correlation is reused verbatim.
- A subscription whose room has no seeded parents is skipped (counts as nothing;
  by construction every seeded room has parents).
- The existing plain-send and canonical paths are untouched.

Keeping this in `Generator` (rather than a parallel generator) means the pacer,
worker pool, warmup accounting, and error metrics are shared.

### 3. Thread workload adapter (`maxrps_thread.go`)

`threadWorkload` is a near-copy of `messagesWorkload`:

- Same NATS connect, metrics server, E1 subscription
  (`subject.UserResponseWildcard`), E2 subscriptions
  (`RoomEventWildcard` + `UserRoomEventWildcard`), collector, and canonical
  durables (`message-worker`, `broadcast-worker`).
- Holds `ThreadFixtures`; `RunStep` builds the `Generator` with
  `ParentsByRoom` populated and `Inject: InjectFrontdoor`.
- `Label()` returns `"thread"`.
- Reuses `buildMessagesInputs` / `msgCounters` / `snapshotPending` unchanged
  (same error reasons, same pending model). If `buildMessagesInputs` is
  workload-named in a confusing way, it stays shared — no behavioral fork.

### 4. CLI wiring

- `max-rps`: add `case "thread"` in `runMaxRPS` (`maxrps.go`) → `newThreadWorkload`.
  Reuse the `messages` default RPS steps. `thread` does **not** read `--inject`.
  Bottleneck attribution stays `messages`-only (the existing `diagnoseBottleneck`
  guard already short-circuits non-`messages` workloads).
- `seed`: add `case "thread"` → `runSeedThread` (seeds rooms/subs/keys, then
  parents). Honor the existing `--users` override for parity with `messages`,
  plus a `--parents-per-room` flag (default from preset).
- `teardown`: add `case "thread"` → `runTeardownThread` (clear Mongo fixtures via
  the existing `Teardown`; clear the seeded Cassandra parents via the
  history-seed teardown path scoped to the fixture rooms).
- Usage strings in `main.go` updated to list `thread`.

## Data flow (one measured reply)

```
loadgen publishOne (frontdoor, ThreadParentMessageID set)
  → MsgSend  → message-gatekeeper.HandleJetStreamMsg
                 ├─ membership check (Mongo)
                 ├─ resolveThreadParentCreatedAt → GetMessageByID RPC → history-service → Cassandra   [E1 extra cost]
                 └─ publish MESSAGES_CANONICAL  → gatekeeper reply  ── E1 (publish→ack) ──┐
  → message-worker: persist message + thread_messages_by_thread + thread room/sub upsert  │
  → broadcast-worker: RoomEvent fan-out (+ tcount/metadata)  ── E2 (publish→broadcast) ───┘
```

E1 = `Collector` requestID correlation (publish → gatekeeper reply).
E2 = `Collector` messageID correlation (publish → `RoomEvent.LastMsgID`).

## Error handling / measurement semantics

- Same as `messages`: `FailedOps` counts only hard publish/marshal/gatekeeper
  errors; missing replies/broadcasts are caught by latency + pending growth, not
  counted as failures (avoids false trips on late stragglers).
- A gatekeeper reply carrying an `error` field (e.g. a transient parent-fetch
  failure surfacing as an errcode) increments the `gatekeeper` reason, exactly as
  today.
- Because every parent is pre-seeded and resolvable, a steady `gatekeeper`-error
  rate at low RPS would indicate a seeding/config bug (e.g. bucket-hours mismatch
  making the parent unreadable), surfaced by the verdict as an early trip.

## Testing (TDD)

Unit tests, same-package `package main`, mocked publisher / in-memory collector —
no real NATS/Mongo/Cassandra:

- `BuildThreadFixtures`: determinism (same seed → same parents), `ParentsPerRoom`
  count per room, every parent's author is a subscriber of its room, every seeded
  room has ≥1 parent.
- `Generator.publishOne` thread mode: emitted `SendMessageRequest` has a non-empty
  `ThreadParentMessageID` belonging to the target room; request published on
  `MsgSend`; collector `RecordPublish` invoked; room-with-no-parents is skipped.
- `threadWorkload.Label()` and counter/pending plumbing reuse the existing
  `maxrps_messages_test.go` assertions (shared helpers).
- CLI dispatch: `seed/teardown/max-rps` route `thread` to the right runner and
  reject it where `--inject` would be meaningless.

Integration tests (`//go:build integration`, `testutil` containers) follow the
existing `seed_integration_test.go` shape: seed parents into a real Cassandra
keyspace and assert `GetMessageByID`-shaped reads resolve them.

## Docs

Update `tools/loadgen/README.md`: a "Thread-reply workload" section (quick start
`seed --workload=thread` / `max-rps --workload=thread`, the parent-seeding
requirement, the frontdoor-only note, and "compare the `thread` ceiling against
the `messages` ceiling on the same box"). No `docs/client-api.md` change — no
client-facing handler is added or modified.

## Non-goals

- No `GetThreadMessages` read benchmark (covered by `history-sustained`).
- No canonical-injection thread path.
- No cross-site / federated thread replies.
- No new presets — reuse the `messages` presets.
- Not a CI gate; invoked manually, compared within one machine.
