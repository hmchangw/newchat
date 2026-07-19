# teams-room-creation — design

**Date:** 2026-07-16
**Branch:** `claude/teams-chat-sync-service-9ispb0` (based on the teams-chat-sync line, which carries the `teams_chat` model with `NeedCreateRoom`)
**Status:** Approved (brainstorming)

## 1. Purpose

A run-to-completion job, triggered by a **k8s CronJob**, that turns Teams chats
flagged for room creation into room-canonical NATS events. It is the consumer
end of the `needCreateRoom` flag: `teams-chat-sync` sets it `true` for oneOnOne
chats on insert (complete on first sight), and `teams-chat-member-sync` sets it
`true` for group chats each time it resolves the roster — on first onboarding
**and** again whenever a membership change re-triggers member sync. So the
published event is a **create-or-sync** signal (create the room if absent, else
reconcile its members), and the downstream room-worker must be idempotent on
chat id — see §9.

Each run:

1. Lists every `teams_chat` where `needCreateRoom=true`.
2. Groups the chats by `siteId`.
3. Batches up to N chats into one NATS event per batch, published on the
   room-canonical subject for that site.
4. Flips `needCreateRoom=false` for exactly the chats whose batch was
   acknowledged by JetStream.

One global instance serves the whole federation (same operating model as
`teams-chat-sync`).

## 2. Structural model

Structurally a twin of `teams-chat-sync`: a `package main` service directory at
the repo root, a `run() error` entrypoint that wires dependencies and performs
one pass, `os.Exit(1)` only on total failure. It differs from the sync jobs in
that it **publishes to NATS/JetStream** (like `outbox-worker`) rather than
calling Microsoft Graph.

Service directory: `teams-room-creation/`

| File | Responsibility |
|------|----------------|
| `main.go` | `slog` setup, `run()`, `os.Exit` on total failure |
| `config.go` | `caarlos0/env` `Config` struct + `validateConfig` |
| `store.go` | `TeamsChatStore` interface + `//go:generate mockgen` |
| `store_mongo.go` | Mongo read/write implementation |
| `publisher.go` | NATS/JetStream connect + batch publish (dedup msgID) |
| `runner.go` | Orchestration: list → group → batch → publish → flip |
| `runner_test.go`, `store_mongo_test.go`, `main_test.go` | unit tests |
| `integration_test.go` | testcontainers (`testutil.MongoDB` + `testutil.NATS`) |
| `mock_store_test.go` | generated mock (never edited by hand) |
| `deploy/` | `Dockerfile`, `docker-compose.yml`, `azure-pipelines.yml` |

## 3. Event model (`pkg/model/teamsroom.go`)

New batch envelope, one per (siteId, batch). Added to
`pkg/model/model_test.go` round-trip coverage.

```go
// TeamsRoomCreateEvent is the batch envelope published to the room-canonical
// subject: one event carries up to N chats that all share a site. The site is
// carried on the subject, not in the payload.
type TeamsRoomCreateEvent struct {
    Chats     []TeamsRoomCreateChat `json:"chats"`
    Timestamp int64                 `json:"timestamp"` // event publish time, UnixMilli UTC
}

// TeamsRoomCreateChat is one chat's worth of room-creation input.
type TeamsRoomCreateChat struct {
    ID              string                  `json:"id"`
    Name            string                  `json:"name"`
    Members         []TeamsRoomCreateMember `json:"members"`
    CreatedDateTime time.Time               `json:"createdDateTime"`
}

// TeamsRoomCreateMember is one member reference in a room-creation event.
// Only account + history-visibility are carried; the Graph member id is dropped.
type TeamsRoomCreateMember struct {
    Account                     string    `json:"account"`
    VisibleHistoryStartDateTime time.Time `json:"visibleHistoryStartDateTime"`
}
```

`Timestamp` satisfies the CLAUDE.md rule that every NATS event struct in
`pkg/model` carries a `Timestamp int64`, stamped at publish time with
`time.Now().UTC().UnixMilli()`.

The event carries exactly the fields the task specifies per chat:
`{id, name, members=[{account, visibleHistoryStartDateTime}], createdDateTime}`.

## 4. Subject & stream

New builder in `pkg/subject`:

```go
// RoomCanonicalTeamsCreate returns the subject for a batch of Teams-derived
// room-creation events for one site.
func RoomCanonicalTeamsCreate(siteID string) string {
    return fmt.Sprintf("chat.room.canonical.%s.teams.create", siteID)
}
```

The subject lands in the existing `ROOMS_{siteID}` stream
(`chat.room.canonical.{siteID}.>`). **No new stream and no bootstrap
ownership** — `ROOMS` is owned by ops/IaC. Cross-site delivery is handled by
the NATS supercluster/gateway routing (an ops concern); the job simply
publishes to `chat.room.canonical.{chat.siteId}.teams.create` for each group.

Published via JetStream `PublishMsg`, blocking on `PubAck`, with a
deterministic dedup id:

```
Nats-Msg-Id = teamroom:{siteID}:{sha256-hex of sorted chat IDs in the batch}
```

so a re-run that republishes an un-flipped batch is deduplicated server-side.
This is best-effort across cron runs: if ops want to rely on it, the `ROOMS`
stream's `Duplicates` window must be ≥ the CronJob interval — otherwise
duplicate suppression rests on downstream room-worker idempotency (already
noted out-of-scope in §9).

## 5. Store

`mongoStore` over two clients, mirroring `teams-chat-sync`:

- **read client** (`mongoutil.ConnectRead`, secondary-preferred):
  `ListChatsNeedingRoom(ctx) ([]model.TeamsChat, error)` — `find {needCreateRoom:true}`
  with an explicit projection `{_id, name, members, createdDateTime, siteId, updatedAt}`
  (`updatedAt` is the compare-and-set token).
- **write client** (`mongoutil.Connect`):
  `MarkRoomsCreated(ctx, refs []RoomCreatedRef) error` — bulk **compare-and-set**:
  for each `{id, updatedAt}` ref, clears `needCreateRoom` only where `updatedAt`
  still matches. A chat re-flagged by member-sync since it was listed (its
  `updatedAt` moved) is left set and re-published next run, so a concurrent
  membership update is never dropped.

Interface (defined in the consumer, `store.go`):

```go
type RoomCreatedRef struct {
    ID        string
    UpdatedAt time.Time
}

type TeamsChatStore interface {
    ListChatsNeedingRoom(ctx context.Context) ([]model.TeamsChat, error)
    MarkRoomsCreated(ctx context.Context, refs []RoomCreatedRef) error
}
```

## 6. Config (environment)

| Env var | Default | Notes |
|---------|---------|-------|
| `MONGO_URI` | — | required |
| `MONGO_DB` | `chat` | |
| `MONGO_USERNAME` / `MONGO_PASSWORD` | `""` | |
| `NATS_URL` | — | required |
| `NATS_CREDS_FILE` | `""` | |
| `ROOM_CREATE_BATCH_SIZE` | `100` | chats per event; must be > 0 |
| `MAX_WORKERS` | `8` | parallel publish across site-group batches |

The run deadline is owned by the Kubernetes CronJob (`activeDeadlineSeconds`),
not an app-level timeout: `run()` uses a `signal.NotifyContext(SIGINT, SIGTERM)`
context so the pod's termination signal aborts the run between operations.

Plain `log/slog` JSON, like the sibling teams-* jobs — no OTel SDK is wired. NATS
still needs a tracer/propagator, so `run()` passes no-ops
(`noop.NewTracerProvider()`, `propagation.TraceContext{}`); `o11y/nats` gates
header work on `O11Y_ENABLED`, so this stays off the hot path. No secrets defaulted.

## 7. Flow & error handling

```
run():
  parse+validate config
  connect read + write Mongo, NATS (+ JetStream, no-op tracer/propagator)
  chats = store.ListChatsNeedingRoom()
  groups = group chats by siteId
  for each (siteId, chats) group:                # bounded by MAX_WORKERS
    for each batch of up to N chats:
      evt = build TeamsRoomCreateEvent(batch, Timestamp=now)   # site is on the subject
      ack, err = publish(subject.RoomCanonicalTeamsCreate(siteId), evt, dedupID)
      if err: log warn, continue                 # chats stay flagged for next run
      store.MarkRoomsCreated(batch refs)          # option C: CAS-flip only on ack
```

- **Partial failure is normal.** A failed batch is logged and skipped; its chats
  keep `needCreateRoom=true` and are retried on the next CronJob schedule. The
  job exits `0`.
- **Total failure** (cannot connect to Mongo/NATS, cannot list) returns a
  non-zero exit so the CronJob surfaces it.
- **At-least-once** semantics: a crash after `PublishMsg` acks but before
  `MarkRoomsCreated` republishes the batch next run; the deterministic dedup id
  makes the republish a server-side no-op, and the downstream room-worker must
  be idempotent on chat id (out of scope for this service).

## 8. Testing (TDD, Red-Green-Refactor)

- **Unit:** inject the publish function as a field so tests capture published
  events without a real NATS connection; mock `TeamsChatStore`. Table-driven
  coverage of: empty list, single site, multi-site grouping, batch chunking
  (exact multiple, remainder, batch size 1), publish error → not flipped,
  publish ack → flipped, member field mapping.
- **Store:** `store_mongo_test.go` integration via `testutil.MongoDB` — list
  projection correctness, `MarkRoomsCreated` compare-and-set, and a stale-ref
  no-op (a moved `updatedAt` must not clear the flag).
- **Integration:** `integration_test.go` with `testutil.MongoDB` + `testutil.NATS`,
  end-to-end: seed flagged chats → run → assert events on the stream and flags
  cleared. `TestMain` drives `testutil.RunTests`.
- **Model:** add the three new types to `pkg/model/model_test.go` round-trip.
- ≥80% coverage floor; target 90%+ on `runner.go` and store.

## 9. Out of scope / non-goals

- Downstream materialization of rooms (room-worker consuming `teams.create`) —
  a separate change; this service only publishes.
- `docs/client-api.md` — unchanged; the subject is internal
  (`chat.room.canonical.…`), not a `chat.user.` client RPC.
- No new third-party dependencies.

## 10. Naming (settled)

- Subject operation token is `teams.create` (plural), consistent with the rest
  of the family (`teams_chat`, `teams-*`).
- The wire field for a member's history-visibility is
  `visibleHistoryStartDateTime`, identical to the `TeamsChatMember` model field
  it is copied from.
