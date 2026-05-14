# search-service NATS Migrations — Design

**Status:** Draft
**Date:** 2026-05-13
**Branch:** `claude/migrate-search-nats-eHDjs`

## 1. Goal

Migrate four legacy HTTP search endpoints into `search-service` as NATS
request/reply RPCs, unified under a single conceptual model: *every search is
scoped to what the caller can already see, and `{account}` from the subject is
both the authenticated identity and the access boundary.*

The four endpoints, with their target NATS subjects:

| Legacy HTTP | NATS subject | Status |
|---|---|---|
| `GET /api/v3/apps` | `chat.user.{account}.request.search.apps` | **new** |
| `GET /api/v3/messages` | `chat.user.{account}.request.search.messages` | extend existing |
| `GET /api/v3/users` | `chat.user.{account}.request.search.users` | **new** |
| `GET /api/v3/subscriptions` | `chat.user.{account}.request.search.rooms` | reshape existing endpoint (subject unchanged) |

All four migrations land on the same branch as one unit of work.

## 2. Unifying Principle

> **Every search RPC is scoped to what the caller can already see.** The
> `{account}` token in the subject is both authentication (enforced by the
> NATS auth callout) and the access boundary for the result set.

| Endpoint | Noun returned | Scoped to | Filter |
|---|---|---|---|
| `search.apps` | `model.App` | apps the caller has subscribed to (enforced via pipeline `$lookup subscriptions`) | name + optional `assistant.enabled` |
| `search.messages` | `model.SearchMessage` | rooms the caller is a member of (enforced via Valkey restricted-rooms cache + ES user-room index) | content (+ optional `RoomIDs` scope) |
| `search.users` | `model.SearchUser` | the caller's company (enforced by the third-party HR endpoint) | name/account (third-party-defined) |
| `search.rooms` | `model.SearchRoom` | the caller's room subscriptions (enforced by ES `spotlight` index doc-routing) | name + `RoomType` |

No search RPC ever returns data outside the caller's reach. There is no
"global" search — the access guard is part of the query, not a separate
authorization step the caller can bypass.

## 3. Architecture

`search-service` gains a third storage backend (Mongo) alongside its existing
ES and Valkey backends, and a new outbound HTTP client (Resty) for the users
endpoint. Each backend is exposed via its own interface; the handler holds
all of them and routes each endpoint to the backend(s) it needs.

```
handler {
    store SearchStore         // ES — existing (spotlight + messages indices)
    mongo MongoStore          // NEW — apps aggregation + batch hydration
    cache RestrictedRoomCache // Valkey — existing
    users SearchUsersClient   // NEW — outbound HTTP to third-party HR endpoint
    cfg   handlerConfig
}
```

| Endpoint | Backends used |
|---|---|
| `search.apps` | Mongo (only) |
| `search.messages` | ES + Valkey + Mongo (hydrate users/rooms) |
| `search.users` | Resty HTTP (third-party) |
| `search.rooms` | ES + Mongo (hydrate `SearchRoom`) |

## 4. Decisions

| # | Decision | Rationale |
|---|---|---|
| D1 | All four endpoints live in `search-service`. | Project rule: all search endpoints belong to `search-service` regardless of backend. |
| D2 | Authentication = `{account}` from subject. No `*User` resolution in handlers. | Matches every existing NATS handler in the repo (history-service, room-service, existing search-service). The NATS auth callout enforces the subject `{account}` is the verified identity. |
| D3 | Each backend gets its own interface (`SearchStore`, `MongoStore`, `RestrictedRoomCache`, `SearchUsersClient`). | Mirrors existing `SearchStore` vs `RestrictedRoomCache` split. Avoids a fat interface spanning ES + Mongo + HTTP. |
| D4 | Single Mongo connection in `search-service/main.go`. Only `apps` is bound as a `*mongo.Collection` in Go; `subscriptions`, `rooms`, `users` referenced by name in pipelines / batch fetches. | search-service does not own indexes on subs/rooms/users — those are owned by `room-service`/`room-worker`. The aggregation runtime resolves cross-collection joins by name in the same database. |
| D5 | Response shapes preserved verbatim from legacy HTTP, **not** normalized. | Behavior-preserving migration. Clients porting from HTTP to NATS see the same payload structure. Normalization is a separate concern that can be done later if desired. |
| D6 | No `pkg/appstore` extraction. `room-service` untouched. | YAGNI. Only two readers of the `apps` collection today (`room-service` + new `search-service`), with different query shapes. Extract when a third reader appears. |
| D7 | `model.App` not extended in this branch. | User will extend the model in a follow-up. The pipeline's terminal `$project` mirrors whatever `model.App` exposes at the time. |
| D8 | `search.messages` does ES + Mongo enrichment + final transformation in a single RPC. `MessageSearchHit` becomes internal-only. | Avoids forcing the caller into two round-trips (one for ES, one to enrich users/rooms). Same architectural pattern as the apps pipeline (`$lookup` enrichment inside the search call). |
| D9 | `search.users` is a thin proxy to a third-party HR endpoint. No Mongo read; no `employee` collection involvement in this spec. | The third-party owns the search index and the company-scoping logic. |
| D10 | `search.rooms` reshapes the existing endpoint — subject stays `search.rooms`; the response wire shape changes to `{rooms: []SearchRoom}` (Mongo-hydrated). **Breaking change to the response shape only** (subject unchanged). | Preserves the existing subject name for client continuity; only the per-hit projection (Mongo-hydrated SearchRoom) and envelope key (`rooms`) change. |

## 5. Wire Contracts

All four NATS payloads use JSON. Subjects follow the existing
`chat.user.{account}.request.search.<noun>` pattern. The `{account}` token is
parsed by `natsrouter` via `c.Param("account")`. Handlers wrap their work
with `defer observeRequest(metricKindX, &err)()` for Prometheus.

### 5.1 `search.apps`

**Subject:** `chat.user.{account}.request.search.apps`

**Request (`pkg/model/search.go`):**
```go
type SearchAppsRequest struct {
    Query        string `json:"query"`
    AssistantEnabled *bool  `json:"assistantEnabled,omitempty"`
    Size             int    `json:"size,omitempty"`
    Offset           int    `json:"offset,omitempty"`
}
```

**Response:**
```go
type SearchAppsResponse struct {
    Apps []model.App `json:"apps"`
}
```

**Validation:**
- `strings.TrimSpace(Query)` — if empty, return `natsrouter.ErrBadRequest("query is required")`.
- `normalizePagination(&Size, &Offset)` (existing helper) — clamp; reject negatives.

**`AssistantEnabled` semantics (strict equality):**
- `nil` → no filter
- `true` → match `assistant.enabled == true`
- `false` → match `assistant.enabled == false` (does **not** match docs with `assistant` missing)

### 5.2 `search.messages` v2

**Subject:** unchanged — `chat.user.{account}.request.search.messages`

**Request (`pkg/model/search.go`, extended):**
```go
type SearchMessagesRequest struct {
    Query string   `json:"query"`
    RoomIDs    []string `json:"roomIds,omitempty"`  // NEW — scope to these rooms if set
    Size       int      `json:"size,omitempty"`
    Offset     int      `json:"offset,omitempty"`
}
```

**Response — breaking change from today's shape:**
```go
type SearchMessagesResponse struct {
    Messages []model.SearchMessage `json:"messages"`
    Total    int64                 `json:"total"`
}
```

(Today: `{total, results []MessageSearchHit}`. After: `{messages, total}`.
`MessageSearchHit` is demoted to an unexported internal staging type used only
between the ES query and the Mongo hydration step. The public reply type is
`model.SearchMessage`.)

**Validation:** unchanged from current (`Query` required; pagination
normalized via existing helper).

### 5.3 `search.users`

**Subject:** `chat.user.{account}.request.search.users`

**Request (`pkg/model/search.go`):**
```go
type SearchUsersRequest struct {
    Query string `json:"query"`
}
```

(No pagination — the third-party HR endpoint hardcodes offset 0, limit 25.)

**Response — raw JSON array (no envelope):**
```go
type SearchUsersResponse = []model.SearchUser
```

The handler returns `[]model.SearchUser` directly; `natsrouter` marshals it
into a top-level JSON array.

**Validation:** `strings.TrimSpace(Query)` — if empty, return
`natsrouter.ErrBadRequest("query is required")`.

### 5.4 `search.rooms` (response reshape; subject unchanged)

**Subject:** `chat.user.{account}.request.search.rooms`

**Request (`pkg/model/search.go`, renamed and reshaped):**
```go
type SearchRoomsRequest struct {
    Query    string `json:"query"`
    RoomType string `json:"roomType,omitempty"`  // "all" (default) | "channel" | "dm"
    Size     int    `json:"size,omitempty"`
    Offset   int    `json:"offset,omitempty"`
}
```

**Response:**
```go
type SearchRoomsResponse struct {
    Subscriptions []model.SearchRoom `json:"subscriptions"`
}
```

**Validation:**
- `Query` non-empty after trim.
- `RoomType` ∈ `{"", "all", "channel", "dm"}` (empty/"all" treated identically). Reject `"app"` and anything else.
- `Size` ≤ 100 (reuses `MaxDocCounts` cap from `handlerConfig`).

(Existing `SearchRoomsRequest`/`SearchRoomsResponse` types are deleted in
the same commit as the rename.)

## 6. File-Level Changes

### `pkg/model/`

| File | Action | Notes |
|---|---|---|
| `pkg/model/search.go` | edit | Add `SearchAppsRequest`/`SearchAppsResponse`. Add `SearchUsersRequest`. Rename `SearchRoomsRequest`/`SearchRoomsResponse` → `SearchRoomsRequest`/`SearchRoomsResponse` (with `Query`/`RoomType` field renames). Reshape `SearchMessagesResponse` to `{Messages, Total}`. Add new projection types `SearchMessage`, `SearchUser`, `SearchRoom` (field lists per the user's legacy HTTP shapes — supplied by the user during implementation). Delete `RoomSearchHit` (fully dead after the subscription reshape). Move `MessageSearchHit` into `search-service` as an unexported internal staging type used between the ES query and the Mongo hydration step (no longer a public `pkg/model` type). |
| `pkg/model/model_test.go` | edit | Add round-trip tests for every new and reshaped type. |

### `pkg/subject/`

| File | Action | Notes |
|---|---|---|
| `pkg/subject/subject.go` | edit | Add `SearchApps(account)` + `SearchAppsPattern()`. Add `SearchUsers(account)` + `SearchUsersPattern()`. Rename `SearchRooms` / `SearchRoomsPattern` → `SearchRooms` / `SearchRoomsPattern` (subject token changes from `rooms` to `subscriptions`). |
| `pkg/subject/subject_test.go` | edit | Update existing room-search test name + add tests for the new builders. |

### `search-service/`

| File | Action | Notes |
|---|---|---|
| `search-service/store.go` | edit | Add `MongoStore` interface (methods: `SearchAppsByName`, `FindUsersByIDs`, `FindRoomsByIDs`, `HydrateRooms`). Add `SearchUsersClient` interface (single method `SearchUsers(ctx, query) ([]model.SearchUser, error)`). Keep existing `SearchStore` and `RestrictedRoomCache` interfaces. Update `//go:generate mockgen` directive. |
| `search-service/store_mongo.go` | **new** | `mongoStore` struct + implementations for all four `MongoStore` methods. Binds `apps` collection; aggregations on subs/rooms/users referenced by string name in pipelines. |
| `search-service/query_apps.go` | **new** | `buildSearchAppsPipeline(query, account, assistantEnabled, limit)` — pipeline body **authored by the user**; the spec provides the function signature and a placeholder `[]bson.M{}`. Pipeline shape: `$match name+assistantEnabled` → `$lookup subscriptions` (the access guard) → `$group` → `$lookup rooms` → `$limit` → `$project` matching `model.App`. |
| `search-service/query_apps_test.go` | **new** | Table-driven pipeline-builder tests against expected BSON shape. Verifies regex escaping (`regexp.QuoteMeta` on `query`), optional stages present/absent based on `assistantEnabled` being `nil` / `true` / `false`, and `$limit` reflecting `Size`. |
| `search-service/users_client.go` | **new** | `httpUsersClient` Resty adapter implementing `SearchUsersClient`. Wraps the outbound HTTP call to the third-party HR endpoint. **Third-party request/response wire shape: TBD** — fill in when implementing; the interface boundary keeps the handler tests independent of it. |
| `search-service/handler.go` | edit | Inject `mongo MongoStore` and `users SearchUsersClient` into `handler`. Add `searchApps`, `searchUsers` handler methods. Refactor existing `searchMessages` for ES → Mongo enrichment → `SearchMessage` transformation. Refactor existing `searchRooms` → `searchRooms` (subject rename, request/response reshape, Mongo hydration). Register all four routes on `Router`. |
| `search-service/handler_test.go` | edit | New table-driven test groups for each of the four endpoints, using mocked stores. Cover: happy path; empty input rejection; pagination clamping; backend errors; access-guard behavior (restricted-rooms classification for `/messages` with supplied `RoomIDs`); `assistantEnabled` pass-through for `/apps`. |
| `search-service/integration_test.go` | edit | Add testcontainers Mongo. Seed `apps`/`subscriptions`/`rooms`/`users` docs. Stub the third-party HR HTTP endpoint with `httptest.Server`. Run each handler end-to-end through `natsrouter`. |
| `search-service/main.go` | edit | Add `MongoConfig{URI, DB}` and `UsersAPIConfig{URL, Timeout, Token}`. Wire `mongoutil.Connect`, construct `mongoStore` and `httpUsersClient`, pass all four stores into `newHandler`. Add Mongo `Disconnect` to `shutdown.Wait`. |
| `search-service/metrics.go` | edit | Add `metricKindApps`, `metricKindUsers`, `metricKindRooms` constants. Rename `metricKindRooms` → `metricKindRooms`. |
| `search-service/response.go` | edit | Relocate `MessageSearchHit` from `pkg/model` into this file as an unexported staging type (e.g., `messageSearchHit`). Delete the public-API exposure of `RoomSearchHit`. Add transformation helpers from ES projection → `model.SearchMessage` / `model.SearchRoom`. |
| `search-service/response_test.go` | edit | Update for the transformation helpers; cover edge cases (empty hits, partial enrichment data). |
| `search-service/deploy/docker-compose.yml` | edit | Add `mongo` service; add `USERS_API_URL` env (point at a mock service for local dev). |
| `search-service/mock_store_test.go` | regenerate | Via `make generate SERVICE=search-service`. |

### `docs/`

| File | Action | Notes |
|---|---|---|
| `docs/client-api.md` | edit | Document all four new/changed RPCs (request shape, response shape, error codes, scope rules). Required by CLAUDE.md §5 since these are client-facing handlers. |

## 7. Per-Endpoint Request Flow

### 7.1 `search.apps`

1. NATS message lands on `chat.user.{account}.request.search.apps`.
2. `natsrouter` middleware (`RequestID`, `Recovery`, `Logging`) wraps the call.
3. `searchApps(c, req)`:
   1. `account, _ := c.Params.Require("account")`.
   2. `h.normalizePagination(&req.Size, &req.Offset)`.
   3. `query := strings.TrimSpace(req.Query)`; if empty → `ErrBadRequest("query is required")`.
   4. `ctx, cancel := h.withRequestTimeout(c); defer cancel()`.
   5. `apps, err := h.mongo.SearchAppsByName(ctx, query, account, req.AssistantEnabled, req.Size)`.
      - On error: log with `account` + err; return `ErrInternal("search backend unavailable")`.
   6. Return `&SearchAppsResponse{Apps: apps}`.
4. `defer observeRequest(metricKindApps, &err)()` records duration + outcome.

### 7.2 `search.messages` v2

1. As above, account from subject.
2. `normalizePagination`; reject empty `Query`.
3. `restricted, err := h.loadRestricted(ctx, account)` — existing Valkey-cached restricted-rooms map.
4. `accessibleRoomIDs, err := classifyRoomIDs(req.RoomIDs, restricted)` — see §8.
5. `body, err := buildMessageQuery(req, account, accessibleRoomIDs, ...)` — existing builder, parameterized to take a pre-classified room list when `RoomIDs` is set.
6. `raw, err := h.store.Search(ctx, MessageIndexPattern, body)` — existing ES path.
7. `internalHits, err := parseMessagesResponse(raw)` — produces internal `[]messageSearchHit`.
8. Extract distinct `userIDs`, `roomIDs` from `internalHits`.
9. `users, err := h.mongo.FindUsersByIDs(ctx, userIDs)` and `rooms, err := h.mongo.FindRoomsByIDs(ctx, roomIDs)` — batch lookups.
10. Zip hits + enrichment into `[]model.SearchMessage` (see §6.3 transformation helper).
11. Return `&SearchMessagesResponse{Messages: ..., Total: ...}`.

### 7.3 `search.users`

1. Account from subject (used only for logging/metrics; no scoping applied by `search-service`).
2. Reject empty `Query`.
3. `users, err := h.users.SearchUsers(ctx, req.Query)` — Resty call to third-party.
   - Backend error → `ErrInternal("user search backend unavailable")`.
   - Bad request from third-party (4xx) → log + `ErrInternal` (do not leak third-party messages).
4. Return `users` (raw `[]model.SearchUser`).

### 7.4 `search.rooms`

1. Account from subject.
2. `normalizePagination` (size capped at 100); reject empty `Query`; validate `RoomType`.
3. `body, err := buildRoomQuery(req, account)` — existing room-query builder, reused with the renamed field names.
4. ES query on `spotlight` index → produces internal `[]roomSearchHit` (room IDs + minimal metadata).
5. Extract distinct `roomIDs`.
6. `subs, err := h.mongo.HydrateRooms(ctx, account, roomIDs)` — fetch the caller's `Subscription` docs for those rooms, build `[]model.SearchRoom`.
7. Return `&SearchRoomsResponse{Subscriptions: subs}`.

## 8. `RoomIDs` Classification (for `/messages` v2)

When the client supplies `RoomIDs`, we need to know which of those IDs are
restricted (require an HSS-floor filter) and which are unrestricted. The
existing `loadRestricted` cache already gives us the restricted-rooms map
keyed by roomID.

```go
func classifyRoomIDs(
    requested []string,
    restricted map[string]int64,
) (unrestricted []string, restrictedWithFloor map[string]int64) {
    // Allocate based on input size. Filtering OUT roomIDs the user has
    // no access to is the third bucket — silently dropped so a malicious
    // caller can't probe arbitrary roomIDs.
    ...
}
```

Security rule (M2 locked in): an unknown roomID — one that's neither in the
user's restricted map nor in the user's unrestricted set (queried via ES
`user-room` terms-lookup, unchanged from today's flow) — is silently dropped.
The user gets results only for rooms they actually belong to.

When `RoomIDs` is empty (the original global-search path), the existing
behavior is unchanged: query all rooms the user has access to via the ES
`user-room` index plus the restricted-rooms HSS floor map.

## 9. Configuration

`search-service/main.go` Config grows two sections:

```go
type MongoConfig struct {
    URI string `env:"URI,required"`
    DB  string `env:"DB"  envDefault:"chat"`
}

type UsersAPIConfig struct {
    URL     string        `env:"URL,required"`
    Timeout time.Duration `env:"TIMEOUT" envDefault:"5s"`
    Token   string        `env:"TOKEN"   envDefault:""`  // optional auth header
}

type Config struct {
    SiteID   string         `env:"SITE_ID" envDefault:"site-local"`
    ES       ESConfig       `envPrefix:"SEARCH_"`
    Valkey   ValkeyConfig   `envPrefix:"VALKEY_"`
    NATS     NATSConfig     `envPrefix:"NATS_"`
    Search   SearchConfig   `envPrefix:"SEARCH_"`
    Mongo    MongoConfig    `envPrefix:"MONGO_"`     // NEW
    UsersAPI UsersAPIConfig `envPrefix:"USERS_API_"` // NEW
}
```

`main.go` wires:
- `mongoutil.Connect(ctx, cfg.Mongo.URI, cfg.Mongo.DB)` → `*mongo.Database` → `newMongoStore(db)`.
- `resty.New().SetBaseURL(cfg.UsersAPI.URL).SetTimeout(cfg.UsersAPI.Timeout)` → `newHTTPUsersClient(rc, cfg.UsersAPI.Token)`.
- Both stores passed into `newHandler` alongside the existing ES + Valkey deps.

Shutdown order (extends existing `shutdown.Wait`):

```go
shutdown.Wait(ctx, 25*time.Second,
    func(ctx context.Context) error { return router.Shutdown(ctx) },
    func(ctx context.Context) error { return nc.Drain() },
    func(ctx context.Context) error { return tracerShutdown(ctx) },
    func(_ context.Context) error { valkeyutil.Disconnect(valkey); return nil },
    func(ctx context.Context) error { return mongoClient.Disconnect(ctx) },     // NEW
    func(ctx context.Context) error { return metricsServer.Shutdown(ctx) },
)
```

## 10. Error Handling

| Condition | Response |
|---|---|
| Missing/empty required field after trim | `ErrBadRequest("<field> is required")` |
| Negative `Size` or `Offset` | `ErrBadRequest("size and offset must be non-negative")` (existing helper) |
| Invalid `RoomType` for subscriptions | `ErrBadRequest("invalid roomType")` |
| Mongo `Aggregate` / `Find` error | log w/ `account` + err; `ErrInternal("search backend unavailable")` |
| Cursor decode error | log; `ErrInternal("unexpected search response")` |
| ES backend error | log; `ErrInternal("search backend unavailable")` (existing) |
| Valkey cache error | log warning; fall through to ES (existing) |
| Resty HTTP error to third-party | log; `ErrInternal("user search backend unavailable")` |
| Third-party 4xx | log raw status/body; `ErrInternal` (never leak third-party error text to client) |
| Context deadline | propagates; surfaced as internal error |

No raw internal errors leak to the client (CLAUDE.md §3 Error Handling).

## 11. Observability

`search-service/metrics.go` gets four kind constants:

```go
const (
    metricKindMessages      = "messages"
    metricKindApps          = "apps"           // NEW
    metricKindUsers         = "users"          // NEW
    metricKindRooms = "rooms"
)
```

Existing `metricKindRooms` is renamed to `metricKindRooms` in the
same commit as the subject rename. Operators with dashboards keyed on
`kind="rooms"` need to update.

Each handler defers `observeRequest(metricKindX, &err)()` to record duration
+ outcome. ES timing already tracked via `observeES()`; Mongo and HTTP-client
timing get new equivalents (`observeMongo()`, `observeUsersAPI()`).

## 12. Testing Strategy (TDD per CLAUDE.md §4)

Each phase below follows Red → Green → Refactor → Commit.

**Phase 0 — model + subject** (foundation, no behavior change yet):
1. Add round-trip tests for all new/reshaped types in `pkg/model/model_test.go`. Red.
2. Add new types and rename existing ones. Green.
3. Add subject-builder tests in `pkg/subject/subject_test.go`. Red.
4. Add new subject builders and rename existing. Green.

**Phase 1 — `search.apps`:**
1. Write `query_apps_test.go` table-driven tests for the pipeline builder (escaping, optional stages, limit). Red.
2. Implement `buildSearchAppsPipeline` skeleton (user fills body). Green.
3. Write `handler_test.go` tests for `searchApps` (happy/empty/error/validation). Red.
4. Implement `searchApps` handler + `MongoStore.SearchAppsByName`. Green.
5. Refactor: extract shared validation helpers if duplication appears.
6. Commit.

**Phase 2 — `search.users`:**
1. Define `SearchUsersClient` interface; mock in handler tests. Red.
2. Write handler tests (happy/empty-query/backend-error). Red.
3. Implement `searchUsers` handler + `httpUsersClient` Resty adapter. Green.
4. Stub third-party in integration test with `httptest.Server`. Green.
5. Commit.

**Phase 3 — `search.rooms` (response reshape):**
1. Rename subject + type tests (Phase 0 covers most of this). Verify red→green flips.
2. Rewrite handler tests for the new request fields + response shape. Red.
3. Implement reshaped `searchRooms` handler + `MongoStore.HydrateRooms`. Green.
4. Commit.

**Phase 4 — `search.messages` v2:**
1. Write tests for `classifyRoomIDs`. Red.
2. Implement `classifyRoomIDs`. Green.
3. Extend `handler_test.go` for the new `RoomIDs` field + Mongo enrichment + new response shape. Red.
4. Refactor `searchMessages` to do ES → Mongo enrichment → `SearchMessage` transformation. Add `MongoStore.FindUsersByIDs` / `FindRoomsByIDs`. Green.
5. Update integration tests to seed users/rooms and assert enriched output. Green.
6. Commit.

**Coverage targets** (CLAUDE.md §4 Coverage):
- ≥80% on all new files
- ≥90% on `handler.go`, `store_mongo.go`, `query_apps.go`, `users_client.go`

**Run `make generate SERVICE=search-service`** at the start of every phase that adds methods to `MongoStore` or `SearchUsersClient`.

## 13. Out of Scope

- Extending `model.App` with the richer field set (`avatarUrl`, `version`, `categories`, etc.) — user owns this follow-up.
- Extracting `pkg/appstore` — defer until a third reader of the `apps` collection appears.
- Refactoring `room-service` to share `apps`-collection access — untouched.
- Modifying the `employee` collection schema or its ownership — out of band; not referenced by any endpoint in this spec.
- Migrating the third-party HR endpoint's auth scheme — TBD when wiring `USERS_API_TOKEN`.
- Normalizing the four response envelope shapes into a single convention — explicit design decision D5 to preserve legacy shapes.

## 14. Follow-up Items

Captured here so they don't get lost; **not** part of this spec's implementation:

1. **Extend `model.App`** to expose the full provisioned field set (avatar URL, categories, version, etc.) so `search.apps` returns richer documents without further code changes in `search-service`.
2. **Decide on `model.SearchMessage` / `SearchUser` / `SearchRoom` field lists** — the spec assumes the user supplies these from the legacy HTTP response shapes. They live in `pkg/model/search.go`.
3. **Document `USERS_API_TOKEN` auth scheme** once the third-party endpoint's auth requirements are known.
4. **Operator dashboard update** for the `metricKindRooms` → `metricKindRooms` rename.
5. **Surface `editedAt` / `updatedAt` on `SearchMessage`** — REQUIRED follow-up. Today the Cassandra message model carries `EditedAt` + `UpdatedAt` (`pkg/model/cassandra/message.go:99-100`) but `model.Message` (the NATS event payload) does not, and `MessageSearchIndex` (`search-sync-worker/messages.go`) does not index either field. Plumbing them through requires a cross-service change in three places: (a) add `EditedAt` and `UpdatedAt` to `model.Message` and propagate them through the event flow (message-gatekeeper → message-worker → search-sync-worker); (b) add both fields to `MessageSearchIndex` and the index template; (c) extend `messageSearchHit` + `SearchMessage` to project them. Out of scope for the current branch — track this as a follow-up PR.
