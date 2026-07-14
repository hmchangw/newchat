# Design: `thread-read` max-rps workload for loadgen

**Date:** 2026-06-30
**Status:** Approved (brainstorm) — pending implementation plan

## Goal

Measure the maximum sustainable RPS for **loading thread messages**
(`history-service.GetThreadMessages` — the single-partition slice read on the
Cassandra `thread_messages_by_thread` table) through the existing
`tools/loadgen` `max-rps` harness, and surface what limits it. "Sustainable"
means the highest RPS step at which p95/p99 latency and error rate stay within
SLO — the same verdict model the other read workloads (`room-read`,
`read-receipt`, `history`) already use.

The point is to **isolate the thread-read ceiling** as its own focused
benchmark, rather than the blended 20% sub-mix it is today inside the `history`
workload.

## Background

`max-rps` is a subcommand of the `loadgen` binary with pluggable workloads
behind a small `rpsWorkload` interface (`ramp.go`):

```go
type rpsWorkload interface {
    RunStep(ctx context.Context, targetRPS int, warmup, hold time.Duration) (rpsStepInputs, error)
    Label() string
}
```

Implementing this interface gets the ramp engine, SLO gating, verdict logic,
report, and CSV output for free. The existing read workloads are the direct
analogs:

- `room-read` (`maxrps_roomread.go` + `roomread_generator.go` +
  `roomread_collector.go`) — the cleanest single-latency-series synchronous
  request/reply read. **This design mirrors it.**
- `history` (`maxrps_history.go` + `history_generator.go`) — already exercises
  `GetThreadMessages`, but only blended with `LoadHistory` via the
  `--mix=history:80,thread:20` endpoint mix, so the thread-read ceiling cannot
  be read off it.
- `read-receipt` reuses the **history** fixtures and the history seed verbatim;
  this design does the same.

### What "load thread messages" is, on the wire

A request/reply RPC on `subject.MsgThread(account, roomID, siteID)` =
`chat.user.{account}.request.room.{roomID}.{siteID}.msg.thread`, carrying
`{ "threadMessageId": <parentID>, "limit": N }`
(`models.GetThreadMessagesRequest`). On success the reply is a
`GetThreadMessagesResponse` (`{ "messages": [...], "parentMessage": {...},
"hasNext": bool, ... }`); on failure it is the `pkg/errcode` envelope
`{ "error": "<message>", ... }`.

The existing `history_generator.go` already builds exactly this request
(`getThreadMessagesRequest{ThreadMessageID, Limit}` on `subject.MsgThread`) for
its thread sub-mix; this design lifts that single endpoint into an isolated,
first-page-only capacity benchmark.

### What the read costs on the server

`GetThreadMessages` (history-service/internal/service/threads.go):

1. `getAccessSince(account, roomID)` — membership/access check (Mongo). The
   caller **must** be a subscriber of the room.
2. `findMessage(roomID, threadMessageID)` — resolves the parent from Cassandra
   `messages_by_id`; rejects replies-as-parent and out-of-window parents.
3. A parallel pair: `msgReader.GetThreadMessages(threadRoomID, …)` —
   single-partition slice on `thread_messages_by_thread` (no bucket walk) — and
   `threadRooms.GetMinThreadUserLastSeenAt(threadRoomID)` (Mongo).

The single-partition slice (step 3) is the headline cost this benchmark
isolates.

### The hard constraint: parents must really exist

Because the server fetches and validates the parent, every request must
reference a real top-level message ID that resolves through history-service and
has a non-empty `thread_room_id`. Therefore thread parents (and their replies)
**must be seeded before the run** — satisfied for free by the history seed,
which writes `messages_by_id`, `messages_by_room`, `thread_messages_by_thread`,
and the `thread_rooms` Mongo docs.

## Decisions (from brainstorm)

1. **Target:** the thread-message **read** path (`GetThreadMessages`). The send
   path is the separate, already-implemented `thread` workload.
2. **Access pattern: first-page opens only.** Each request opens a thread cold —
   pick a seeded parent, fetch the first page of replies (no cursor). Models the
   dominant real case (a user clicking into a thread) and keeps the signal clean.
   Cursor/deep-scroll pagination is a non-goal.
3. **Fixtures: reuse the history presets and the history seed**, exactly as
   `read-receipt` does. `BuildHistoryFixtures` already produces the
   `ThreadParents map[string][]ThreadParentRef` the generator needs. No new seed
   code. Threads are shallow (the history presets seed 3 replies per thread); that
   is accepted. `history-medium` / `history-large` seed threads;
   `history-small` (ThreadRate 0) has none and is unsuitable.
4. **Integration shape:** a new `thread-read` workload behind the existing
   `rpsWorkload` interface, plus thin `thread-read` cases in `seed`/`teardown`
   that delegate to the existing history seed/teardown. Not a standalone
   subcommand; not folded into `history`.
5. **Stricter reply validation than `history`.** The current `history` workload
   records any non-transport reply as a success sample, so an errcode reply is
   miscounted as a healthy read. The `thread-read` generator validates the reply
   (below) and counts errcode envelopes / undecodable bodies as failures.

## Architecture

Four small new pieces plus thin CLI wiring, all in `tools/loadgen/`. Each
mirrors its `room-read` analog.

### 1. Collector (`threadread_collector.go`)

`threadReadCollector` — a near-copy of `roomread_collector.go`:

- One `[]threadReadSample` latency tape (`RecordSample`).
- Per-class error tally over the shared `errClass` consts (`errClassTimeout`,
  `errClassReply`, `errClassBadReply`) via `RecordError` / `RecordBadReply`.
- `saturation` and `underrun` counters (`RecordSaturation` / `RecordUnderrun`)
  for the pacer's load-box signals.
- An informational `noParents` counter (`RecordNoParents`) for requests that
  landed on a room with no seeded parents and were skipped — not a failure,
  mirrors the history workload's no-thread-parents tally.
- Accessors (`Samples`, `TimeoutErrors`, `ReplyErrors`, `BadReplyCount`,
  `SaturationCount`, `UnderrunCount`, `NoParentsCount`). All methods
  mutex-guarded; `Samples` returns a defensive copy.

### 2. Generator (`threadread_generator.go`)

`threadReadGenerator` — mirrors `roomReadGenerator`:

- Built from the **history** fixtures. At construction, builds a lookup of
  rooms that have ≥1 seeded parent (`fixtures.ThreadParents`) joined with that
  room's subscribers (`fixtures.Fixtures.Subscriptions`). Rooms without parents
  are excluded from the Zipf pick set entirely (so we don't waste ticks).
- Zipf room pick (`s=1.1, v=1.0`, matching history/room-read) over the
  parent-bearing rooms; a random subscriber of the chosen room is the caller;
  a random parent of that room is the `threadMessageId`.
- Builds `getThreadMessagesRequest{ThreadMessageID, Limit: PageLimit}`, mints a
  fresh `X-Request-ID` (`natsutil.WithRequestID` + `idgen.GenerateRequestID`,
  like room-read), and issues `Requester.Request(ctx, subject.MsgThread(...),
  data, RequestTimeout)`.
- Reply handling:
  - Transport error: run-level ctx cancel → ignored (draining); otherwise
    `classifyRequesterError` → `errClassTimeout` / `errClassReply`.
  - Decode a minimal `{ "error": string, "parentMessage": json.RawMessage }`:
    non-empty `error` → `RecordError(errClassReply)`; decode failure or nil
    `parentMessage` → `RecordBadReply`; otherwise `RecordSample` with the
    measured latency.
- Reuses `pacedDispatch` (MaxInFlight>0) / `serialDispatch` (==0, bisection),
  with the collector's underrun/saturation callbacks, exactly as room-read.

If the parent-bearing room set is empty (e.g. someone points it at
`history-small`), `Run` returns an error so the step fails loudly rather than
reporting a meaningless zero.

### 3. Workload adapter (`maxrps_threadread.go`)

`threadReadWorkload` implementing `rpsWorkload`, mirroring `roomReadWorkload`:

- `newThreadReadWorkload(ctx, cfg, preset *HistoryPreset, seed, pageLimit,
  requestTimeout)` — requires `CASSANDRA_HOSTS` (fail-fast, like history),
  connects NATS, starts the metrics server, builds `BuildHistoryFixtures`, and
  wires `newNATSHistoryRequester`. Cleanup closure shuts the metrics server and
  drains NATS.
- `Label()` returns `"thread-read"`.
- `RunStep` runs warmup (discarded) then hold (measured) as two sequential
  generator runs via a `runFor`-style helper (room-read's `runRoomReadFor`
  shape; typed to `*threadReadGenerator`).
- `buildThreadReadInputs(targetRPS, hold, collector)` → `rpsStepInputs` with a
  single `"thread-read"` latency series, `AttemptedOps = len(samples)+failed`,
  `FailedOps = timeouts+reply+badReply`, `Saturation`/`EmitUnderrun` from the
  collector, and **no** `Pending` (synchronous read, no consumer queue).

### 4. CLI wiring (`maxrps.go`, `main.go`)

- `maxrps.go`:
  - Add `thread-read` to the `--workload` usage string and the `defaultSteps`
    read-path branch (`"200,500,1000,2000,5000"`).
  - Add `case "thread-read"` in `runMaxRPS`: look up `BuiltinHistoryPreset`,
    validate `--request-timeout` and `--page-limit` (>0), call
    `newThreadReadWorkload`. Reuses the existing `--page-limit` and
    `--request-timeout` flags already defined for the history/read workloads.
  - `diagnoseBottleneck` stays `messages`-only (its existing guard already
    short-circuits other workloads).
- `main.go`:
  - `seed`: add `case "thread-read"` → delegate to `runSeedHistory` (so
    `loadgen seed --workload=thread-read --preset=history-medium` works and
    seeds parents+replies+thread_rooms).
  - `teardown`: add `case "thread-read"` → delegate to `runTeardownHistory`.
  - Update the `seed`/`teardown` `--workload` usage strings and the top-level
    usage line as needed.

## Reuse (no new code)

- **Fixtures + seed/teardown**: `BuildHistoryFixtures`, `runSeedHistory`,
  `runTeardownHistory` (Cassandra `messages_by_id` / `messages_by_room` /
  `thread_messages_by_thread`, Mongo `thread_rooms`, room keys).
  `MESSAGE_BUCKET_HOURS` must match the services, already a loadgen config
  concern via `msgbucket.Sizer`.
- **Ramp engine, verdict, SLO gating, report, CSV** — all via the `rpsWorkload`
  interface and the existing `runMaxRPS` plumbing.

## Data flow (one measured read)

```
loadgen threadReadGenerator.requestOne
  → MsgThread request/reply → history-service.GetThreadMessages
       ├─ getAccessSince(account, roomID)                 (Mongo membership)
       ├─ findMessage(roomID, threadMessageID)            (Cassandra messages_by_id)
       └─ parallel:
            ├─ GetThreadMessages(threadRoomID, …)         (Cassandra thread_messages_by_thread)  [headline cost]
            └─ GetMinThreadUserLastSeenAt(threadRoomID)   (Mongo)
  → reply ── single "thread-read" latency sample ──┘
```

## Error handling / measurement semantics

- `FailedOps` = timeout + reply-error + bad-reply counts. `noParents` is
  informational and excluded from `FailedOps`.
- A non-empty `error` field in the reply (errcode envelope) is a failure
  (`errClassReply`) — at a steady low-RPS rate it indicates a seeding/config bug
  (e.g. `MESSAGE_BUCKET_HOURS` mismatch making parents unreadable, or pointing
  at `history-small`), surfaced as an early trip / high error rate.
- Run-level ctx cancellation during drain is not counted as a failure.

## Testing (TDD)

Unit tests, same-package `package main`, mocked `RoomReadRequester`-shaped
requester / in-memory collector — no real NATS/Mongo/Cassandra:

- Generator: emitted request targets `subject.MsgThread`, its `threadMessageId`
  is a real parent of the chosen room, and the caller is a subscriber of that
  room; a room with no parents is excluded from selection (and an all-empty
  fixture makes `Run` error); error-envelope reply → `errClassReply`; reply with
  no `parentMessage` / undecodable → `errClassBadReply`; healthy reply →
  one latency sample.
- Collector: sample/error/saturation/underrun/no-parents tallies and defensive
  `Samples` copy (mirror `roomread_collector_test.go`).
- Adapter: `Label() == "thread-read"`; `buildThreadReadInputs` yields a single
  `"thread-read"` series, correct attempted/failed counts, empty pending.
- CLI dispatch: `max-rps`/`seed`/`teardown` route `thread-read` to the right
  runners; `max-rps --workload=thread-read` rejects a non-history preset and a
  non-positive `--request-timeout`/`--page-limit`.

Integration test (`//go:build integration`, `testutil` Cassandra + Mongo),
following the existing history integration shape: seed a small set of
parents+replies into a real keyspace, run the generator at a low rate against a
recorded requester or the real history-service-shaped read, and assert replies
resolve to samples (no errors).

## Docs

Add a "Thread-read workload" section to `tools/loadgen/README.md`: quick start
(`seed --workload=thread-read --preset=history-medium`, then
`max-rps --workload=thread-read --preset=history-medium`), the history-seed /
`CASSANDRA_HOSTS` / `MESSAGE_BUCKET_HOURS` requirements, the first-page-only and
single-series-gating notes, the `history-small`-has-no-threads caveat, and "read
the thread-read ceiling here vs the blended number from the `history` workload".
Update the `max-rps` flag/Workload references to list `thread-read`. No
`docs/client-api.md` change — no client-facing handler is added or modified.

## Non-goals

- No cursor / deep-scroll pagination (first-page opens only).
- No new presets — reuse the history presets.
- No canonical injection (this is a read RPC; injection modes don't apply).
- No cross-site / federated thread reads.
- No bottleneck attribution (stays `messages`-only).
- Not a CI gate; invoked manually, compared within one machine.
