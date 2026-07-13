# teams-user-sync — Design

**Date:** 2026-07-13
**Status:** Approved

## 1. Purpose

A long-running cron batch service that keeps the MongoDB `teams_user`
collection populated with every Teams (Azure AD) user in the tenant, joined
with the HR system's site assignment. On each scheduled run it walks the
Microsoft Graph `/users` directory page by page, finds users not yet in
`teams_user`, resolves their `siteID` from the `hr` collection, and
batch-writes the merged records.

The resulting `teams_user` document is:

```json
{ "_id": "<teams user object id>", "upn": "<userPrincipalName>", "account": "<upn local part>", "siteId": "<hr siteID>" }
```

- `_id` — Teams (Azure AD) user object id, from Graph.
- `upn` — the user's `userPrincipalName`, from Graph.
- `account` — the lowercased UPN local part (text before `@`); the same value
  used for the `hr.accountName` lookup.
- `siteId` — the HR system's site id, from the `hr` collection.

## 2. Decisions (settled during brainstorming)

| Question | Decision |
|---|---|
| Scheduling | Cron expression via **robfig/cron/v3** (new dependency, approved). Skip-if-running via robfig's built-in `cron.SkipIfStillRunning` job wrapper. |
| Sync strategy | **Page-streaming** (Approach A): process each Graph page immediately — memory bounded at one page, partial progress survives a mid-run failure. |
| HR miss | A Teams user with no matching `hr.accountName` is **skipped** (no write) and counted; the per-run summary logs the total. Retried naturally next run. |
| Write scope | **Insert missing only** — users already present in `teams_user` (by `_id`) are left untouched; no UPN-change refresh in v1. The write itself is an idempotent upsert (mechanism, not scope — see §3.3 step 4). |
| Mongo separation | **Two URIs, two clients** (`MONGO_READ_URI` / `MONGO_WRITE_URI`, each with its own credentials and db name). Reads (`teams_user` diff, `hr` lookup) use the read client; the batch write uses the write client. URIs may be identical in dev. |

## 3. Architecture

New flat service directory `teams-user-sync` at the repo root
(`package main`), standard per-service layout. The service uses **no NATS and
no HTTP server** — it is a pure scheduled batch job. Graceful shutdown via
`pkg/shutdown.Wait`.

```
teams-user-sync/
├── main.go              # config parse, wiring, cron start, graceful shutdown
├── config.go            # Config struct (caarlos0/env)
├── handler.go           # Syncer: updateUsers run + per-page flow
├── model.go             # service-local TeamsUser struct
├── store.go             # Store interface + //go:generate mockgen
├── store_mongo.go       # two-client Mongo implementation
├── handler_test.go      # unit tests (mocked Store + fake UserLister)
├── config_test.go       # config parsing tests
├── integration_test.go  # testcontainers Mongo + httptest Graph
├── mock_store_test.go   # generated
└── deploy/
    ├── Dockerfile
    ├── docker-compose.yml
    └── azure-pipelines.yml
```

### 3.1 Scheduling

- `robfig/cron/v3` with the schedule from `SYNC_CRON` (default `0 2 * * *`).
- The job is registered wrapped in `cron.SkipIfStillRunning(logger)` — if a
  fire occurs while the previous run is still executing, it is skipped (and
  robfig logs the skip). This is exactly the required "skip if the previous
  job is not yet finished" semantics with no hand-rolled locking.
- `RUN_ON_START=true` (default `false`) additionally triggers one run
  immediately at startup — dev/compose convenience.
- Shutdown: `cron.Stop()` returns a context that is done when the in-flight
  job (if any) finishes; `main.go` waits on it with a timeout consistent with
  the repo-wide 25s shutdown budget, then disconnects both Mongo clients.

### 3.2 Graph client (`pkg/msgraph` extension)

Extend the existing `pkg/msgraph` package (reusing its client-credentials
token cache) with paginated user listing:

```go
// UserLister walks the tenant's user directory page by page.
type UserLister interface {
    // ListUsers calls fn once per page of up to pageSize users
    // (GET /users?$select=id,userPrincipalName&$top={pageSize}), following
    // @odata.nextLink until exhausted. A non-nil error from fn aborts the walk.
    ListUsers(ctx context.Context, pageSize int, fn func([]GraphUser) error) error
}
```

- Implemented on the existing `graphClient`; constructor
  `NewUserListerClient(cfg, opts...)` mirroring `NewDirectoryClient`.
- Reuses `GraphUser{ID, UserPrincipalName}`.
- Non-200 responses surface as wrapped errors with status + sanitized Graph
  error code only (same convention as `CreateOnlineMeeting`); never the raw
  body.

### 3.3 Per-page flow (`updateUsers`)

For each Graph page (≤ `GRAPH_PAGE_SIZE` users):

1. **Diff:** query `teams_user` via the **read** client:
   `find({_id: {$in: pageIDs}}, {projection: {_id: 1}})` → set of existing
   ids. Users already present are skipped.
2. **Account derivation:** for each missing user, `account` = lowercased
   local part of `userPrincipalName` (text before `@`). Users whose UPN
   domain does not equal `TEAMS_EMAIL_DOMAIN` (case-insensitive) — guests,
   `#EXT#` accounts, service principals — are skipped and counted.
3. **HR lookup:** query `hr` via the **read** client:
   `find({accountName: {$in: accounts}}, {projection: {accountName: 1, siteID: 1}})`
   → `account → siteID` map. Accounts with no match are skipped and counted.
4. **Merge + write:** for each user with an HR match, build
   `TeamsUser{ID, UPN, Account, SiteID}` and bulk-**upsert** via the **write** client
   (`mongoutil.UpsertModel` batch keyed on `_id`). Upsert (not insert) keeps
   reruns and read-replica lag harmless — no duplicate-key failures.

Any Graph or Mongo error aborts the run with a wrapped error logged once at
the run level. The next cron fire retries from scratch; idempotent upserts
make that safe.

### 3.4 Model

Service-local (`model.go`) — not `pkg/model`: it is not a NATS payload and no
other service consumes it.

```go
type TeamsUser struct {
    ID      string `json:"id" bson:"_id"`
    UPN     string `json:"upn" bson:"upn"`
    Account string `json:"account" bson:"account"`
    SiteID  string `json:"siteId" bson:"siteId"`
}
```

`siteId` follows the repo's camelCase bson/json tag convention (matching
`pkg/model/teams.go`), even though the HR source field is `siteID`.

### 3.5 Store

Consumer-defined interface in `store.go`:

```go
type Store interface {
    // ExistingIDs returns which of ids already exist in teams_user (read client).
    ExistingIDs(ctx context.Context, ids []string) (map[string]struct{}, error)
    // HRSiteIDs resolves accounts to siteIDs from the hr collection (read client).
    HRSiteIDs(ctx context.Context, accounts []string) (map[string]string, error)
    // UpsertTeamsUsers bulk-upserts merged records into teams_user (write client).
    UpsertTeamsUsers(ctx context.Context, users []TeamsUser) error
}
```

`store_mongo.go` holds two `*mongo.Database` handles (read, write) built from
two `mongoutil.Connect` clients. Both queries project precisely (per
CLAUDE.md). Collection names are constants: `teams_user`, `hr`.

The `hr` document shape this service depends on (read-only, owned by the
external HR pipeline): `{ accountName: string, siteID: string }`. Matching is
by the lowercased UPN local part; `hr.accountName` is assumed to be stored
lowercase (the same convention `pkg/msgraph.ResolveAccountIDs` already relies
on).

Index: `teams_user` needs no secondary indexes in v1 (`_id` covers the diff
query and the upsert). No index is created on `hr` — this service does not
own that collection; the `accountName` batch lookup relies on the owner's
indexing.

### 3.6 Configuration

| Env var | Required | Default | Purpose |
|---|---|---|---|
| `SYNC_CRON` | no | `0 2 * * *` | Cron expression (5-field) for the sync job |
| `RUN_ON_START` | no | `false` | Fire one sync immediately at startup |
| `TEAMS_TENANT_ID` | yes | — | Azure AD tenant |
| `TEAMS_CLIENT_ID` | yes | — | App registration client id |
| `TEAMS_CLIENT_SECRET` | yes | — | App registration secret |
| `TEAMS_EMAIL_DOMAIN` | yes | — | UPN domain filter + account derivation |
| `GRAPH_PAGE_SIZE` | no | `500` | Graph `$top` per page |
| `MONGO_READ_URI` | yes | — | Read cluster URI (`teams_user` diff, `hr` lookup) |
| `MONGO_READ_USERNAME` / `MONGO_READ_PASSWORD` | no | empty | Read credentials |
| `MONGO_READ_DB` | no | `chat` | Read database name |
| `MONGO_WRITE_URI` | yes | — | Write cluster URI (`teams_user` upserts) |
| `MONGO_WRITE_USERNAME` / `MONGO_WRITE_PASSWORD` | no | empty | Write credentials |
| `MONGO_WRITE_DB` | no | `chat` | Write database name |
| `LOG_LEVEL` | no | `info` | slog level |

Parsed with `caarlos0/env` into a typed `Config`; fail fast on missing
required vars. Secrets are `required` with no defaults.

### 3.7 Observability

- slog JSON. Each run generates a request id via `idgen.GenerateRequestID()`,
  carried in `context.Context` and attached to every log line of the run.
- End-of-run summary log: pages walked, users seen, already present,
  domain-skipped, HR-unmatched, upserted, duration.
- No Prometheus endpoint in v1 (no HTTP server at all).

## 4. Error handling

Not client-facing — no `pkg/errcode` usage. All errors are raw
`fmt.Errorf("…: %w", err)` wrapped with what the function was doing, logged
once at the run boundary. Never log tokens or Graph response bodies.

## 5. Testing (TDD)

- **`handler_test.go`** — table-driven unit tests of the per-page flow with a
  mocked `Store` (mockgen) and a fake `UserLister` (function-backed): happy
  path (multi-page), all-users-existing (no HR call, no write), HR miss
  skipped + counted, wrong-domain UPN skipped, malformed UPN (no `@`)
  skipped, store error aborts run, Graph error aborts run, empty tenant.
- **`config_test.go`** — required-var failure, defaults.
- **`pkg/msgraph` unit tests** — `ListUsers` pagination against `httptest`:
  single page, multi-page via `@odata.nextLink`, `$top`/`$select` query
  assertions, non-200 error, fn-error aborts walk.
- **`integration_test.go`** (`//go:build integration`) — `testutil.MongoDB`
  supplying both read and write handles + `httptest` Graph server: seeds
  `hr` and partial `teams_user`, runs `updateUsers`, asserts exact resulting
  `teams_user` docs; second run is a no-op (idempotency).
- **Cron wiring** — unit test that the registered job skips when the previous
  run is still executing (drive `SkipIfStillRunning` wrapper directly).
- Coverage: ≥ 80% package minimum, ≥ 90% target on `handler.go` and
  `store_mongo.go` per repo policy.

## 6. Deploy

- `deploy/Dockerfile` — standard multi-stage (`golang:1.25.12-alpine` →
  `alpine:3.21`), repo-root build context.
- `deploy/docker-compose.yml` — service + a single MongoDB (read and write
  URIs both pointed at it), `RUN_ON_START=true` for instant feedback. No NATS.
- `deploy/azure-pipelines.yml` — copied from a sibling service.

## 7. Out of scope (v1)

- Deleting/disabling `teams_user` docs for users removed from the tenant.
- Refreshing existing docs on UPN or siteID change.
- Graph delta queries (`/users/delta`) for incremental sync.
- Prometheus metrics / healthz endpoint.
- Multi-tenant support.
