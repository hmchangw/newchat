# Teams Room Creation Verification — Design

Date: 2026-08-01
Status: Approved (brainstorming output)

## Problem

`teams-room-creation` publishes room-creation events per site and clears
`needCreateRoom`, but nothing confirms the destination site actually
materialised the room and its subscriptions. A `room-worker` that skipped a
chat (per-chat reconcile failure is a WARN-and-continue) leaves a silent gap:
the flag is cleared, the room never exists.

This design adds a verification lane: a flag written at creation time, a global
job that audits flagged chats, and a per-site read-only HTTP service that
reports what each site's Mongo actually holds.

## Components

| Component | Shape | Scope |
|---|---|---|
| `teams-room-creation` (change) | existing CronJob | global |
| `teams-room-verify` (new) | CronJob, run-to-completion | global |
| `teams-room-inspector` (new) | Gin HTTP service | one deployment per site |

Data flow: `teams-room-verify` reads `teams_chat` (global Mongo) where
`needVerify=true`, groups by `siteId`, and calls that site's
`teams-room-inspector` over HTTP. The inspector reads its own site-local Mongo
(`rooms`, `subscriptions`) and returns counts. The verifier compares and clears
the flag for chats that converged.

## 1. `needVerify` flag

`pkg/model.TeamsChat` gains:

```go
NeedVerify bool `json:"needVerify" bson:"needVerify"`
```

`teams-room-creation`'s `MarkRoomsCreated` update becomes a single
`$set: {needCreateRoom: false, needVerify: true}`, keeping the existing
compare-and-set on `updatedAt`. No new store method, no extra round trip.

`MarkRoomsCreated` is only called after JetStream acknowledges the batch, so
`needVerify=true` means precisely: *a creation event for this chat was durably
published*. Chats whose publish failed keep `needCreateRoom=true` and are never
flagged for verification.

## 2. `teams-room-verify`

Run-to-completion CronJob, mirroring `teams-room-creation`: parse config, one
pass, exit. Scheduled offset after the creation cron so `room-worker` has time
to converge. Files: `main.go`, `config.go`, `runner.go`, `client.go`,
`store.go`, `store_mongo.go`, plus tests and `deploy/`.

### Config

| Env | Required | Default |
|---|---|---|
| `MONGO_URI` | yes | — |
| `MONGO_DB` | no | `chat` |
| `MONGO_USERNAME` / `MONGO_PASSWORD` | no | `""` |
| `TEAMS_VERIFY_SITE_URLS` | yes | — |
| `VERIFY_BATCH_SIZE` | no | `200` |
| `MAX_WORKERS` | no | `8` |

`TEAMS_VERIFY_SITE_URLS` is a JSON object mapping siteId to inspector base URL,
following `portal-service`'s `PORTAL_SITE_URLS` precedent:

```json
{"site-a":"http://teams-room-inspector.site-a.svc:8080",
 "site-b":"http://teams-room-inspector.site-b.svc:8080"}
```

`validateConfig` rejects a non-positive `VERIFY_BATCH_SIZE` or `MAX_WORKERS`,
and a `TEAMS_VERIFY_SITE_URLS` that fails to parse or is empty.

Reads go through `mongoutil.ConnectRead` (secondary-preferred), the flag clear
through `mongoutil.Connect` (primary) — the same two-lane wiring as
`teams-room-creation`.

### Store

```go
type VerifiedRef struct {
    ID        string
    UpdatedAt time.Time
}

type TeamsChatStore interface {
    ListChatsNeedingVerify(ctx context.Context) ([]model.TeamsChat, error)
    MarkVerified(ctx context.Context, refs []VerifiedRef) error
}
```

`ListChatsNeedingVerify` filters `{"needVerify": true}`, projects
`_id, members, siteId, updatedAt`, sorts by `_id` for deterministic batching.

`MarkVerified` bulk-writes `$set: {needVerify: false}` filtered on
`{_id, updatedAt}` — a chat re-written by `teams-chat-member-sync` since it was
listed keeps its flag and is re-verified next run.

### Pass

1. List flagged chats; empty list logs and returns.
2. Group by `siteId` preserving input order, chunk each group into
   `VERIFY_BATCH_SIZE` batches (same `planBatches` shape as the creation job).
3. Fan out over a `chan struct{}` semaphore sized by `MAX_WORKERS`, tracked by a
   `sync.WaitGroup`.
4. Per batch: POST the chat ids to that site's inspector, compare each result,
   collect matched refs, `MarkVerified` them.

### Comparison rule

For each chat in the response:

- `roomExists == false` → **missing room**.
- `roomExists && subscriptionCount != len(chat.Members)` → **subscription
  mismatch**, in either direction (shortfall or extra).
- otherwise → match; the chat's ref is cleared.

Expected count is the raw `len(chat.Members)`. `room-worker` skips members with
an empty `account` (guests and externals absent from `teams_user`), so a chat
containing one reports as mismatched on every run. To keep that
distinguishable from a genuine gap, every mismatch log carries both numbers.

### Logging

Per mismatch (WARN):
`chat_id`, `site_id`, `room_id`, `expected_members`, `accounts_present`,
`actual_subscriptions`, `room_user_count`, `reason` (`missing_room` |
`subscription_mismatch`).

Per site (INFO):
`site_id`, `chats_checked`, `rooms_missing`, `subs_mismatched`, `chats_ok`.

### Failure handling

- Site with no `TEAMS_VERIFY_SITE_URLS` entry → WARN, skip the site, flags stay.
- Inspector call fails or returns non-2xx → WARN, skip the batch, flags stay.
- `MarkVerified` fails → WARN; the chats re-verify next run (verification is
  read-only, so a repeat is harmless).
- Only the initial list failure aborts the run with an error.

## 3. `teams-room-inspector`

Long-running Gin service, one deployment per site, reading the site-local
Mongo. Files: `main.go`, `routes.go`, `handler.go`, `store.go`,
`store_mongo.go`, plus tests and `deploy/`.

`main.go` wires `obs.Init`, `ginutil.RequestID()`, a Gin server with timeouts,
and `shutdown.Wait` (drain HTTP, then disconnect Mongo).

### Config

| Env | Required | Default |
|---|---|---|
| `MONGO_URI` | yes | — |
| `MONGO_DB` | no | `chat` |
| `MONGO_USERNAME` / `MONGO_PASSWORD` | no | `""` |
| `SITE_ID` | yes | — |
| `PORT` | no | `8080` |

### Routes

- `GET /healthz`
- `POST /internal/teams/rooms/verify`

No authentication: the endpoint is cluster-internal, matching the fact that no
service in this repo currently does service-to-service HTTP auth. Network
policy is the boundary.

### Handler

Binds `{"chatIds": [...]}`, requiring 1–500 ids; anything else is
`errcode.BadRequest` written via `errhttp.Write`. For each chat id it derives
`roomID = idgen.DeterministicID([]byte(chatID))` — the same derivation
`room-worker` used when creating the room — then issues two projected queries,
no `$lookup`:

- `rooms`: `find {_id: {$in: roomIDs}}`, projection `{_id: 1, userCount: 1}`
- `subscriptions`: `aggregate [{$match: {roomId: {$in: roomIDs}}},
  {$group: {_id: "$roomId", count: {$sum: 1}}}]`

A chat id absent from both result sets yields `roomExists: false`,
`subscriptionCount: 0`. The response carries exactly one result per requested
chat id, in request order. If a response is nonetheless missing a requested id,
the verifier logs a WARN and leaves that chat flagged rather than treating the
absence as a missing room.

### Wire contract

Request and response structs live in `pkg/model/teams.go`, shared by both
services:

```go
type TeamsRoomVerifyRequest struct {
    ChatIDs []string `json:"chatIds"`
}

type TeamsRoomVerifyResult struct {
    ChatID            string `json:"chatId"`
    RoomID            string `json:"roomId"`
    RoomExists        bool   `json:"roomExists"`
    SubscriptionCount int    `json:"subscriptionCount"`
    RoomUserCount     int    `json:"roomUserCount"`
}

type TeamsRoomVerifyResponse struct {
    SiteID         string                  `json:"siteId"`
    RequestedCount int                     `json:"requestedCount"`
    FoundCount     int                     `json:"foundCount"`
    Chats          []TeamsRoomVerifyResult `json:"chats"`
}
```

`FoundCount` is the number of requested chats whose room exists.
`RoomUserCount` is the room's denormalized counter, reported so a drift between
it and the actual subscription count is visible.

Example response:

```json
{
  "siteId": "site-a",
  "requestedCount": 3,
  "foundCount": 2,
  "chats": [
    {"chatId": "19:abc@thread.v2", "roomId": "7bQ1kR2mN8xY4pL0v",
     "roomExists": true, "subscriptionCount": 5, "roomUserCount": 5},
    {"chatId": "19:def@thread.v2", "roomId": "3mZ8tK1qW5cH9nR2b",
     "roomExists": true, "subscriptionCount": 3, "roomUserCount": 4},
    {"chatId": "19:ghi@unq.gbl.spaces", "roomId": "9xV4jP7sD2fG6bT1m",
     "roomExists": false, "subscriptionCount": 0, "roomUserCount": 0}
  ]
}
```

These are service-to-service HTTP contracts — not `chat.user.*` NATS handlers
and not `auth-service` HTTP routes — so `docs/client-api.md` needs no update.

## Testing

TDD throughout: tests first, confirmed failing, then the minimum
implementation. Minimum 80% coverage per package.

Unit tests (mocked stores, `httptest` inspector, no real infrastructure):

- `teams-room-creation`: `MarkRoomsCreated` sets both fields under CAS.
- `teams-room-verify` runner, table-driven: exact match; missing room; fewer
  subscriptions than members; more subscriptions than members; chat with a
  member lacking an account; site absent from the registry; inspector returns
  500; inspector returns a malformed body; empty flagged list; batching across
  more chats than `VERIFY_BATCH_SIZE`; multiple sites in one pass.
- `teams-room-verify` config validation: bad batch size, bad worker count,
  unparseable and empty site registry.
- `teams-room-inspector` handler, table-driven: valid ids; empty `chatIds`;
  over-limit `chatIds`; malformed JSON; store error; room present with zero
  subscriptions; unknown chat id.

Integration tests (`//go:build integration`, `testutil.MongoDB`,
`TestMain` → `testutil.RunTests`):

- `teams-room-creation`: flag transition on a real collection, including the
  stale-`updatedAt` no-op.
- `teams-room-verify`: `ListChatsNeedingVerify` projection and `MarkVerified`
  CAS semantics.
- `teams-room-inspector`: counting against real `rooms` and `subscriptions`
  documents, including rooms with no subscriptions and ids with no room.

## Deployment

Each new service gets `deploy/Dockerfile` (multi-stage,
`golang:1.25.12-alpine` → `alpine:3.21`, build context at repo root),
`deploy/docker-compose.yml` (local dev dependencies only), and
`deploy/azure-pipelines.yml`, following the existing `teams-*` services.

`teams-room-verify` is a CronJob; `teams-room-inspector` is a Deployment plus a
Service, one per site.

## Out of scope

- Self-healing. Verification never re-flags `needCreateRoom`; a mismatch is
  reported and the chat stays flagged for the next run.
- A persisted result collection. Reporting is structured logs only.
- Verifying the federated subscription mirrors that `room-worker` pushes to
  members' *home* sites. Verification covers the room's owning site only.
