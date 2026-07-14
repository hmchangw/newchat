# teams-chat-sync — Design

**Date:** 2026-07-14
**Status:** Approved

## Purpose

A run-to-completion job, triggered by a Kubernetes CronJob, that mirrors Microsoft
Teams chats into MongoDB. It reads the externally-populated `teams_user`
collection, fetches each user's chats from the Graph API within a per-user
watermark window, resolves each chat's owning site by member-majority vote, and
upserts the result into `teams_chat`. Downstream consumers use `teams_chat`
(notably its `needUserSync` flag) to drive further sync work.

## Topology & auth (decided)

- **One global job** for the whole federation: a single CronJob instance reads
  one MongoDB whose `teams_user` collection holds users of **all** sites and
  writes one `teams_chat` collection. Not per-site.
- **App-only Graph auth** (client-credentials), reusing `pkg/msgraph`'s existing
  flow. Requires the `Chat.Read.All` (or `Chat.ReadBasic.All`) **application**
  permission, admin-consented. No delegated/ROPC service account.
- **All time math in UTC.** `startOfDay(now)` = today `00:00:00Z`. The default
  watermark is a full RFC3339 UTC timestamp from config.

## Service layout

New top-level flat service `teams-chat-sync/` (repo convention), modeled on the
`user-presence-service/sync` run-to-completion job (`run() error`, `RUN_TIMEOUT`
context, tracer init, deferred cleanup, `os.Exit(1)` on failure):

```
teams-chat-sync/
├── main.go              # Config parse, dependency wiring, run() error
├── syncer.go            # Orchestrator: load user cache → worker pool → summary
├── worker.go            # Per-user: fetch chats, dedup, vote siteID, upsert, advance watermark
├── store.go             # TeamsUserStore + TeamsChatStore interfaces + go:generate mockgen
├── store_mongo.go       # Mongo implementation (mongoutil.Connect, BulkWrite)
├── syncer_test.go       # Unit tests (mocked Graph + mocked store)
├── worker_test.go
├── mock_store_test.go   # Generated
├── integration_test.go  # testutil.MongoDB, //go:build integration
└── deploy/              # Dockerfile, docker-compose.yml, azure-pipelines.yml
```

No NATS connection and no `SITE_ID` env — the job is global and talks only to
MongoDB and Graph.

### pkg/msgraph addition

A new `ChatsReader` surface on the existing app-only client (same pattern as
`DirectoryReader`):

```go
type ChatsReader interface {
    // ListUserChats returns the user's chats with lastUpdatedDateTime in
    // (from, to), members expanded. Follows @odata.nextLink pagination
    // internally and honors 429 Retry-After with a small bounded retry.
    ListUserChats(ctx context.Context, userID string, from, to time.Time) ([]Chat, error)
}

type Chat struct {
    ID                  string
    ChatType            string // "oneOnOne" | "group" | "meeting"
    Topic               string // may be empty (Graph null)
    CreatedDateTime     time.Time
    LastUpdatedDateTime time.Time
    Members             []ChatMember
}

type ChatMember struct {
    UserID                      string // aadUserConversationMember userId
    VisibleHistoryStartDateTime time.Time
}
```

Request shape:

```
GET /v1.0/users/{uid}/chats
  ?$filter=lastUpdatedDateTime gt {from RFC3339} and lastUpdatedDateTime lt {to RFC3339}
  &$expand=members
  &$select=id,chatType,topic,createdDateTime,lastUpdatedDateTime
```

## Data models

`teams_user` — externally populated; this job **reads** it and **writes only
`from`**:

| Field | BSON | Type | Notes |
|---|---|---|---|
| ID | `_id` | string | Teams (AAD) user object id |
| SiteID | `siteID` | string | Owning site |
| Account | `account` | string | Chat-system account |
| From | `from` | date (optional) | Watermark; absent until first successful sync |

`teams_chat` — owned by this job:

| Field | BSON | Type | Notes |
|---|---|---|---|
| ID | `_id` | string | Graph chat id |
| Name | `name` | string | Graph `topic`; `""` when Graph returns null |
| ChatType | `chatType` | string | `oneOnOne` / `group` / `meeting` |
| CreatedDateTime | `createdDateTime` | date | Immutable (`$setOnInsert`) |
| LastUpdatedDateTime | `lastUpdatedDateTime` | date | |
| Members | `members` | array | `{id, account, visibleHistoryStartDateTime}` per member |
| SiteID | `siteID` | string | **`$setOnInsert` only — never changes after insert** |
| UpdatedAt | `updatedAt` | date | Set to now on every write |
| NeedUserSync | `needUserSync` | bool | `chatType != "oneOnOne"` |

Model structs carry both `json` and `bson` tags (camelCase). Date-times are
`time.Time` (BSON dates) — they round-trip directly with Graph's ISO-8601.
Members not found in `teams_user` (guests, outsiders) are **kept** with
`account: ""` for full fidelity with the real Teams roster.

## Sync flow

1. **Load cache.** Read all `teams_user` docs (projection: `_id, siteID,
   account, from`) into an in-memory `map[userID]cachedUser`. This map serves
   both as the work list and as the member→siteID/account lookup.
2. **Window.** `to = startOfDay(now, UTC)`. Per user, `from` = their watermark
   or `SYNC_DEFAULT_FROM`. Users with `from >= to` are **skipped** (empty
   window — e.g. a second run the same day); no Graph call, no watermark write.
3. **Fan-out.** Users are sent down a channel consumed by `MAX_WORKERS`
   goroutines (default 8 — Graph throttling makes large pools
   counterproductive), tracked by `sync.WaitGroup`. No `time.Sleep`
   synchronization.
4. **Dedup / write-reduction cache.** A shared mutex-guarded
   `processedChats map[string]struct{}`: a worker atomically check-and-claims a
   chat id before processing; the first worker wins, later workers skip. Since
   many users share the same chats, this is also the write-reduction cache.
5. **siteID vote.** Among the chat's members, those present in the user cache
   vote with their `siteID`; majority wins; ties break to the lexicographically
   smallest siteID (deterministic across runs). The fetching user is always a
   member and always in the cache, so there is always ≥ 1 voter.
6. **Upsert** (one `BulkWrite` of upserts per Graph page, keyed on `_id`):
   - `oneOnOne`: **all** fields under `$setOnInsert` — an existing doc is never
     modified (oneOnOne chats never change after insert).
   - group/meeting: `$setOnInsert: {createdDateTime, siteID}`,
     `$set: {name, chatType, lastUpdatedDateTime, members, needUserSync: true,
     updatedAt: now}`.
   - siteID immutability is thus enforced at the DB layer; the in-memory dedup
     is an optimization, not a correctness mechanism.
7. **Watermark.** When a user's chats are fully fetched (all pages) and
   upserted, set `teams_user.from = to` for that user. On any per-user failure:
   log, keep the old `from` (the window is refetched next run), continue with
   other users. If any user failed, the job exits non-zero so the CronJob
   records the failure.

**Claimed-but-failed subtlety:** if worker A claims a chat and its upsert
fails, other workers skip that chat for the rest of the run. Safe: A's user
keeps the old watermark, so the chat is refetched and written next run.

## Configuration

Parsed with `caarlos0/env` into a typed `Config`; fail fast on missing
required vars.

| Env var | Default | Purpose |
|---|---|---|
| `MONGO_URI` | required | Mongo connection string |
| `MONGO_DB` | `chat` | Database name |
| `MAX_WORKERS` | `8` | Worker pool size |
| `SYNC_DEFAULT_FROM` | `2026-04-01T00:00:00Z` | RFC3339 watermark for users with no `from` |
| `RUN_TIMEOUT` | `30m` | Whole-job context deadline |
| `GRAPH_TENANT_ID` | required | Azure AD tenant |
| `GRAPH_CLIENT_ID` | required | App registration id |
| `GRAPH_CLIENT_SECRET` | required | App registration secret |
| `GRAPH_TLS_INSECURE_SKIP_VERIFY` | `false` | Dev/on-prem TLS interception only |

## Error handling & observability

- No client boundary → no `pkg/errcode`. Raw `fmt.Errorf("…: %w", err)`
  wrapping throughout; `run()` returns the error and `main` exits 1.
- `log/slog` JSON. Per-run summary line: users total / succeeded / failed /
  skipped, chats upserted / deduped. Per-user failures logged at error level
  with user id (never the client secret or token).
- OTel tracer init via `otelutil.InitTracer("teams-chat-sync")`, mirroring the
  presence sync.
- **Graph throttling (429/503) is handled inside `pkg/msgraph` at two levels:**
  1. **Per-request retry:** each request retries up to 4 attempts, honoring the
     server's `Retry-After` header (default 2s when absent, capped).
  2. **Tenant-wide throttle gate:** Graph throttles per app+tenant, so a
     throttle response also arms a client-wide gate (`throttleUntil` on the
     shared `graphClient`, mutex-guarded, monotonically extended). Every
     worker waits out the gate before sending its next request — one
     throttled worker pauses the whole pool instead of the pool hammering an
     already-throttled tenant. The gate wait is capped (5m) so a hostile
     header can't stall the run; waits are timer+ctx based, never `time.Sleep`.
  Persistent throttling (retries exhausted) surfaces as a per-user failure:
  the watermark holds and the user is retried next run, while the armed gate
  still slows the remaining workers.

## Testing (TDD, ≥ 80% coverage)

- **Unit — `teams-chat-sync`** (mocked `ChatsReader`, mockgen store,
  table-driven):
  - siteID vote: clear majority, tie → lexicographic, unknown members excluded.
  - Cross-worker dedup: same chat from two users → one upsert.
  - Update-document construction: oneOnOne all-setOnInsert vs group set/setOnInsert split; `needUserSync` per chatType; `name` empty on null topic.
  - Watermark: advanced on success, held on Graph failure and on upsert failure, skipped when `from >= to`, default-from fallback.
  - Job exit: non-zero when any user fails.
- **Unit — `pkg/msgraph`** (httptest stubs, existing style): query encoding
  (filter/expand/select), nextLink pagination, 429 Retry-After retry, retries
  exhausted, tenant-wide throttle gate (armed by a 429, blocks the next call,
  monotonic extension, ctx-cancel abort, capped wait), token error, Graph
  error, empty result.
- **Integration** (`testutil.MongoDB`, `//go:build integration`, `TestMain` →
  `testutil.RunTests`): siteID unchanged across repeated upserts, oneOnOne doc
  untouched by a second upsert, group fields refreshed, watermark persisted,
  user-cache load projection.

## Out of scope

- Populating `teams_user` (external).
- Consuming `needUserSync` (downstream job).
- Kubernetes CronJob manifests (ops/IaC; `deploy/docker-compose.yml` covers
  local runs).
- Message/history sync — this job syncs chat metadata only.
