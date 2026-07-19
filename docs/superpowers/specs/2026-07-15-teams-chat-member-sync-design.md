# teams-chat-member-sync — Design

**Date:** 2026-07-15
**Status:** Approved

## Purpose

A run-to-completion job, triggered by a Kubernetes CronJob, that resolves the
authoritative member list for Teams chats flagged by `teams-chat-sync`. It
reads `teams_chat` documents where `needMemberSync=true`, fetches each chat's
members from the Graph `/chats/{id}/members` endpoint, resolves each member's
account from `teams_user` by userId (batched + cached), writes the member list
back, and hands the chat off to the next stage by setting `needCreateRoom=true`
and `needMemberSync=false`.

Pipeline position: `teams-user-sync` → `teams-chat-sync` (sets
`needMemberSync=true`) → **`teams-chat-member-sync`** (sets
`needCreateRoom=true`) → (future) room-creation job.

## Topology & auth (decided)

- **One global job** for the whole federation: a single CronJob instance reads
  and writes one MongoDB. Not per-site.
- **App-only Graph auth** (client-credentials), reusing `pkg/msgraph`. Requires
  the `Chat.Read.All` (or `ChatMember.Read.All`) **application** permission,
  admin-consented.
- **Two Mongo clients over one replica set** — a secondary-preferred read
  client (`mongoutil.ConnectRead`) for the `teams_chat` scan and `teams_user`
  resolution, and a primary write client (`mongoutil.Connect`) for the
  `teams_chat` updates. Both share one `MONGO_URI`/`MONGO_DB`/credential pair
  (like `room-worker`) — only the read preference differs. Mirrors
  `teams-chat-sync`.

## Service layout

New top-level flat service `teams-chat-member-sync/`, structured identically to
`teams-chat-sync`:

```
teams-chat-member-sync/
├── main.go              # Config parse, dependency wiring, run() error
├── syncer.go            # Orchestrator + per-chat worker + shared account cache
├── store.go             # TeamsChatStore + TeamsUserStore interfaces + go:generate mockgen
├── store_mongo.go       # mongoStore over readDB/writeDB
├── syncer_test.go       # Unit tests (mocked Graph + mocked stores)
├── worker_test.go
├── store_mongo_test.go  # Update-document unit tests
├── mock_store_test.go   # Generated
├── integration_test.go  # testutil.MongoDB, //go:build integration
└── deploy/              # Dockerfile, docker-compose.yml, azure-pipelines.yml
```

No NATS and no `SITE_ID` — the job is global and talks only to MongoDB and
Graph. Mongo traffic is split into a read client and a write client as above.

### pkg/model change

Add one field to the existing `TeamsChat` struct in `pkg/model/teams.go`:

```go
NeedCreateRoom bool `json:"needCreateRoom" bson:"needCreateRoom"`
```

Set `true` by this job on a successful member sync; consumed by the future
room-creation job. `TeamsChatMember{ID, Account, VisibleHistoryStartDateTime}`
is unchanged and reused as the stored member shape.

### pkg/msgraph addition

A new `ChatMembersReader` surface on the existing app-only client (same pattern
as `ChatsReader`):

```go
type ChatMembersReader interface {
    // ListChatMembers returns the chat's members, following @odata.nextLink
    // pagination. Throttled (429/503) responses are retried per Retry-After
    // and arm the shared tenant-wide gate, exactly like ListUserChats.
    ListChatMembers(ctx context.Context, chatID string) ([]ChatMemberDetail, error)
}

type ChatMemberDetail struct {
    UserID                      string    `json:"userId"`
    VisibleHistoryStartDateTime time.Time `json:"visibleHistoryStartDateTime"`
}
```

Request shape — a plain GET, no OData query options:

```
GET /v1.0/chats/{chatId}/members
```

The endpoint uses server-driven paging (`@odata.nextLink`), followed
internally. Implemented on the shared `graphClient` via the existing
`getThrottled` / `waitThrottle` (per-request Retry-After retry + tenant-wide
throttle gate). The account is resolved downstream from `userId` via
`teams_user`, so no UPN/email is read from the member resource.

## Data model (read + written)

`teams_chat` — owned by `teams-chat-sync`; this job **reads** the sync-flagged
subset and **writes** `members`, `needCreateRoom`, `needMemberSync`,
`updatedAt`:

| Field | BSON | Written | Notes |
|---|---|---|---|
| ID | `_id` | no | Graph chat id (read as the work key) |
| Members | `members` | **yes** | `[]TeamsChatMember{id, account, visibleHistoryStartDateTime}` — full replace |
| NeedCreateRoom | `needCreateRoom` | **yes → true** | Hands the chat to the room-creation stage |
| NeedMemberSync | `needMemberSync` | **yes → false** | Cleared on success; stays true on failure |
| UpdatedAt | `updatedAt` | **yes → now** | Stamped at write time |

`teams_user` — read-only here, for account resolution: projected to
`_id, account`.

## Sync flow

1. **Load work list.** `ListChatsToSync` reads `teams_chat` where
   `needMemberSync=true`, projecting `_id` only → `[]chatID`.
2. **Fan-out.** Chat IDs are sent down a channel consumed by `MAX_WORKERS`
   goroutines (default 8), tracked by `sync.WaitGroup`. No `time.Sleep`.
3. **Per chat** (`syncChat`):
   1. `ListChatMembers(chatID)` (paginated).
   2. Resolve every member's `userId` to an account through the **shared
      account cache** (see below) → `map[userId]account`.
   3. Build `[]TeamsChatMember{ID: userId, Account: account,
      VisibleHistoryStartDateTime}`; members absent from `teams_user` keep
      `account: ""`.
   5. `SetMembersSynced(chatID, seenUpdatedAt, members, now)` — an **optimistic
      conditional** `$set` of `{members, needCreateRoom:true,
      needMemberSync:false, updatedAt:now}`, filtered on
      `{_id: chatID, updatedAt: seenUpdatedAt}` where `seenUpdatedAt` is the
      value read in step 1. This closes a lost-update race: `teams-chat-sync`
      re-sets `needMemberSync=true` (and advances `updatedAt`) on every group
      refresh, so if it rewrote the chat concurrently, the filter no longer
      matches — the write no-ops (`MatchedCount==0`) and the chat is reported
      **superseded**, leaving `needMemberSync=true` for a fresh sync next run.
   6. On any error (Graph, resolution, write): log, **skip the `$set`** so the
      chat keeps `needMemberSync=true` and is retried next run; mark the chat
      failed.
4. **Summary + exit.** Log totals (chats total / succeeded / failed /
   superseded, members written). **Superseded is benign** — the chat is
   correctly re-flagged by the concurrent writer and retries next run, so it
   does NOT fail the job. Only a genuine per-chat failure makes the job exit
   non-zero (so the CronJob records it).

### Shared account cache

A process-wide, mutex-guarded `map[userID]account` shared by all workers.
`resolve(ctx, ids)` partitions the request into cached and uncached IDs,
issues a single `AccountsByIDs` `$in` query for the uncached set (read client),
and stores every result **including misses** (`account: ""`) so a userId is
queried at most once per run. This is the "local cache to reduce lookups" —
group chats commonly share members, so most fallback lookups hit the cache.

## Error handling

- No client boundary → no `pkg/errcode`. Raw `fmt.Errorf("…: %w", err)`
  wrapping; `run()` returns an error and `main` exits 1.
- Graph 429/503: per-request Retry-After retries (bounded) plus the shared
  tenant-wide throttle gate already in `pkg/msgraph`. Exhausted throttle
  surfaces as a per-chat failure.
- `log/slog` JSON. Each synced chat logs an info line with its chat id and the
  member count written; per-chat failures log at error level with chat id. Never
  log tokens or the client secret.

## Configuration

| Env var | Default | Purpose |
|---|---|---|
| `MONGO_URI` | required | Replica set URI; reads use a secondary-preferred client, writes a primary client |
| `MONGO_DB` | `chat` | Database name |
| `MAX_WORKERS` | `8` | Worker pool size |
| `GRAPH_TENANT_ID` | required | Azure AD tenant |
| `GRAPH_CLIENT_ID` | required | App registration id |
| `GRAPH_CLIENT_SECRET` | required | App registration secret |
| `GRAPH_TLS_INSECURE_SKIP_VERIFY` | `false` | Dev/on-prem TLS interception only |

Optional `MONGO_USERNAME`/`MONGO_PASSWORD` (shared by both clients) default to
empty, per repo convention.

## Testing (TDD, ≥ 80% business-logic coverage)

- **Unit — `teams-chat-member-sync`** (mocked `ChatMembersReader`, mockgen
  stores):
  - Member mapping: every member resolved via `AccountsByIDs`; userId absent
    from `teams_user` → `account: ""`.
  - Account cache: uncached IDs batched into one `AccountsByIDs` call; hits and
    misses both cached (a repeat resolve issues no query); cross-worker dedup
    under `-race`.
  - Per-chat failure: Graph error and write error each keep `needMemberSync`
    (no `$set` applied) and fail the run; happy path clears it.
  - Superseded write: `SetMembersSynced` returns the superseded sentinel when
    `updatedAt` changed under it → run reports it and returns nil (benign, not
    a failure).
  - `SetMembersSynced` update document: `$set` contains exactly `members`,
    `needCreateRoom:true`, `needMemberSync:false`, `updatedAt`.
- **Unit — `pkg/msgraph`** (httptest): `ListChatMembers` plain-GET request
  shape (no OData query), nextLink pagination, 429 gate/retry, Graph error
  sanitization (no raw body), empty result.
- **Integration** (`testutil.MongoDB`, `//go:build integration`): a
  `needMemberSync=true` doc gets members replaced, `needCreateRoom=true`,
  `needMemberSync=false`, `updatedAt` set when `seenUpdatedAt` matches; a
  **stale `seenUpdatedAt` leaves the doc untouched** (superseded);
  `ListChatsToSync` returns only flagged chats with their `updatedAt`;
  `AccountsByIDs` projection and `$in`.

## Out of scope

- The room-creation job that consumes `needCreateRoom` (future stage).
- Populating `teams_user` / `teams_chat` (upstream jobs).
- Kubernetes CronJob manifests (ops/IaC; `deploy/docker-compose.yml` covers
  local runs). Recommend `concurrencyPolicy: Forbid` — the shared account
  cache is per-process.
