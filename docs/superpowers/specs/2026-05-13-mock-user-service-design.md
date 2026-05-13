# Mock User Service — Design

**Date:** 2026-05-13
**Status:** Draft
**Services:** new `mock-user-service`; shared `pkg/subject`
**Branch:** `claude/mock-user-service-fGyWo`

## Problem

The chat platform exposes a set of client-facing user RPC subjects under
`chat.user.{account}.request.user.{siteID}.…` (status, profile, subscription,
apps). No service currently answers them. Frontend and integration work that
depends on these subjects has nothing to talk to in local development; ad-hoc
stub services have proliferated, each with its own subject strings and response
shapes.

This spec defines a single development-only service — `mock-user-service` —
that owns all 12 user RPC subjects with stateless, hardcoded responses, plus
the `pkg/subject` builders, parsers, and wildcards that pin the subject
strings down so future real services can plug in against the same contract.

## Goals

- One development service answers all 12 user RPC subjects with deterministic
  responses suitable for frontend/integration work.
- Subject strings are owned by `pkg/subject` builders/parsers/wildcards, never
  built with raw `fmt.Sprintf` inside the service.
- The mock has no database, no JetStream, and no shared mutable state — fully
  stateless so it can be killed and restarted at will in dev.
- `docs/client-api.md` documents the 12 subjects with request/response shapes
  and error cases.
- The service follows the project's flat service layout and `natsrouter`
  conventions exactly, so swapping in a real implementation later is a matter
  of replacing handler bodies — not subjects, types, or wiring.

## Non-Goals

- No persistence of any kind. Status, profile, and subscription state are
  fixed at compile time; `status.set` and `subscribe/unsubscribeApp` do not
  alter anything observable.
- No real authentication or authorization. The service trusts whatever
  reaches the NATS bus.
- No production deployment. The Dockerfile and pipeline exist for local
  compose parity and CI builds; nothing in this spec is meant to ship to
  production.
- No new `pkg/model` types. Request and response structs live inside the
  service package.
- No JetStream interaction (no streams to bootstrap, no events published).
- No integration tests (no real dependencies to integrate against).

## Subjects

All 12 subjects use the user-scoped RPC pattern:

```
chat.user.{account}.request.user.{siteID}.{area}.{action}
```

except `room.subscription.get`, which is room-scoped under user:

```
chat.user.{account}.request.user.{siteID}.room.{roomID}.subscription.get
```

| # | Subject (concrete form) | Request type | Response type | Notes |
|---|---|---|---|---|
| 1 | `chat.user.{account}.request.user.{siteID}.status.getByName` | `StatusGetByNameRequest{Name}` | `StatusResponse{Name, StatusText, StatusIsShow}` | Stateless — fixed defaults for any `Name`. |
| 2 | `chat.user.{account}.request.user.{siteID}.status.set` | `StatusSetRequest{StatusText, StatusIsShow}` | `OKResponse{Success: true}` | No-op; response is fixed. |
| 3 | `chat.user.{account}.request.user.{siteID}.profile.getByName` | `ProfileGetByNameRequest{Name}` | `ProfileResponse{Name, DisplayName, Email}` | Stateless. |
| 4 | `chat.user.{account}.request.user.{siteID}.subscription.getCurrent` | `GetSubsRequest{Favorite, MembersContain, AccountNames}` | `SubscriptionListResponse{Subscriptions, Total}` | Filters accepted and ignored. |
| 5 | `chat.user.{account}.request.user.{siteID}.subscription.getRooms` | `GetSubsRequest` | `SubscriptionListResponse` | Same as `getCurrent`. |
| 6 | `chat.user.{account}.request.user.{siteID}.subscription.getChannels` | `GetSubsRequest` | `SubscriptionListResponse` | Same. |
| 7 | `chat.user.{account}.request.user.{siteID}.subscription.getDM` | `GetDMSubRequest{TargetAccount}` | `DMSubscriptionResponse{Subscription}` | Single `model.Subscription`. |
| 8 | `chat.user.{account}.request.user.{siteID}.subscription.getApps` | `GetAppSubsRequest{Favorite}` | `SubscriptionListResponse` | Filter ignored. |
| 9 | `chat.user.{account}.request.user.{siteID}.subscription.subscribeApp` | `AppSubscriptionRequest{AppID}` | `OKResponse{Success: true}` | No-op. |
| 10 | `chat.user.{account}.request.user.{siteID}.subscription.unsubscribeApp` | `AppSubscriptionRequest{AppID}` | `OKResponse{Success: true}` | No-op. |
| 11 | `chat.user.{account}.request.user.{siteID}.room.{roomID}.subscription.get` | _(none)_ | `RoomSubscriptionResponse{Subscription}` | `RegisterNoBody`; `roomID` echoed into `Subscription.RoomID`. |
| 12 | `chat.user.{account}.request.user.{siteID}.apps.list` | _(none)_ | `AppListResponse{Apps, Total}` | `RegisterNoBody`; two mock `model.App`s. |

### Mock data conventions

- `SubscriptionListResponse` (routes 4–6, 8) returns **two** mock
  `model.Subscription` entries with `Total: 2`.
- `DMSubscriptionResponse` (route 7) returns one `model.Subscription` whose
  `User.Account == TargetAccount` from the request.
- `RoomSubscriptionResponse` (route 11) returns one `model.Subscription` whose
  `RoomID` is taken from the subject param.
- `AppListResponse` (route 12) returns **two** mock `model.App` entries with
  `Total: 2`.
- `StatusResponse.Name` and `ProfileResponse.Name` echo the request's `Name`
  field; the rest of the fields are package-level constants
  (`mockStatusText`, `mockStatusIsShow`, `mockDisplayName`, `mockEmail`).
- Mock `Subscription.User.Account` is derived from the `account` subject param
  for all list and room-scoped responses (so the subscription looks "owned by"
  the requester). The DM route is the deliberate exception: its single
  `Subscription.User.Account` is set to the request's `TargetAccount` so the
  returned subscription represents the DM peer, not the caller.
- All mock `Subscription.SiteID` values are set to `h.siteID`.

## `pkg/subject` additions

Twelve specific builders, six parsers, five wildcards. All added to
`pkg/subject/subject.go` alongside the existing builders.

### Specific builders

```go
func UserStatusGetByName(account, siteID string) string
func UserStatusSet(account, siteID string) string
func UserProfileGetByName(account, siteID string) string
func UserSubscriptionGetCurrent(account, siteID string) string
func UserSubscriptionGetRooms(account, siteID string) string
func UserSubscriptionGetChannels(account, siteID string) string
func UserSubscriptionGetDM(account, siteID string) string
func UserSubscriptionGetApps(account, siteID string) string
func UserSubscriptionSubscribeApp(account, siteID string) string
func UserSubscriptionUnsubscribeApp(account, siteID string) string
func UserRoomSubscriptionGet(account, siteID, roomID string) string
func UserAppsList(account, siteID string) string
```

### Parsers

```go
// Parses any 8-token subject of the form
//   chat.user.{account}.request.user.{siteID}.{area}.{action}
// where area is one of "status", "subscription", "profile", "apps".
// Does NOT match the 10-token room-scoped form — use ParseRoomSubject.
func ParseUserSubject(subj string) (account, siteID, area, action string, ok bool)

// Narrow parsers — validate `area` and return account+action only.
// Each returns ok=false if the subject doesn't match its expected area.
func ParseStatusSubject(subj string) (account, action string, ok bool)
func ParseSubscriptionSubject(subj string) (account, action string, ok bool)
func ParseProfileSubject(subj string) (account, action string, ok bool)
func ParseAppsSubject(subj string) (account, action string, ok bool)

// Parses the 10-token room-scoped form
//   chat.user.{account}.request.user.{siteID}.room.{roomID}.{area}.{action}
// Returns the trailing `{action}` token (e.g. "get" for subscription.get).
// Returns ok=false if the subject is not exactly 10 tokens or does not
// start with `chat.user.*.request.user.*.room.*.`.
func ParseRoomSubject(subj string) (account, roomID, action string, ok bool)
```

### Wildcard builders

For services that want to subscribe with a single `>` per area instead of one
subscription per route. The mock itself uses `natsrouter` per-route
registration and does not consume these wildcards, but they are part of the
public `pkg/subject` API so future services (or test fixtures) can.

```go
func UserStatusWildCard(siteID string) string         // chat.user.*.request.user.{siteID}.status.>
func UserSubscriptionWildCard(siteID string) string   // chat.user.*.request.user.{siteID}.subscription.>
func UserProfileWildCard(siteID string) string        // chat.user.*.request.user.{siteID}.profile.>
func UserRoomWildCard(siteID string) string           // chat.user.*.request.user.{siteID}.room.>
func UserAppsWildCard(siteID string) string           // chat.user.*.request.user.{siteID}.apps.>
```

### Tests

`pkg/subject/subject_test.go` gains:

- One table-driven test per builder asserting the exact literal output.
- One round-trip test per parser: feed the corresponding builder's output and
  assert the extracted fields (account/siteID/area/action for the area
  parsers; account/roomID/action for `ParseRoomSubject` against
  `UserRoomSubscriptionGet`'s output).
- A malformed-subject table per parser covering: wrong prefix
  (`chat.room.…`), wrong area, wrong token count, empty string.
  `ParseRoomSubject`'s table additionally rejects subjects missing the
  literal `room` token at position 6 and subjects that aren't exactly
  10 tokens.
- A wildcards table asserting each wildcard's literal output.

## Service layout

Per the project's flat-service convention (no `cmd/`, no `internal/`):

```
mock-user-service/
├── main.go              # config + NATS connect + router wire-up + graceful shutdown
├── handler.go           # Handler struct + 12 RPC methods + Register(router) + local Req/Resp types + mock data constants
├── handler_test.go      # table-driven unit tests, one section per route
└── deploy/
    ├── Dockerfile           # multi-stage golang:1.25.8-alpine → alpine:3.21
    ├── docker-compose.yml   # NATS only — no Mongo, Cassandra, Valkey
    └── azure-pipelines.yml  # adapted from a sibling Gin-less service
```

No `store.go`, no `mock_store_test.go`, no `integration_test.go`, no
`bootstrap.go` — none of them apply.

### `main.go`

Config (env vars, parsed via `caarlos0/env/v11`):

| Var | Default | Required |
|---|---|---|
| `NATS_URL` | — | yes |
| `NATS_CREDS_FILE` | `""` | no |
| `SITE_ID` | `"site-local"` | no |

Flow:

1. `slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))`.
2. `env.ParseAs[config]()`; on failure, log and `os.Exit(1)`.
3. `otelutil.InitTracer(ctx, "mock-user-service")`.
4. `natsutil.Connect(cfg.NatsURL, cfg.NatsCredsFile)`.
5. `router := natsrouter.Default(nc, "mock-user-service")` then
   `router.Use(natsrouter.HandlerTimeout(5 * time.Second))`.
6. `handler := NewHandler(cfg.SiteID)`; `handler.Register(router)`.
7. `slog.Info("mock-user-service running", "site", cfg.SiteID)`.
8. `shutdown.Wait(ctx, 25*time.Second, …)` with cleanup order:
   - `router.Shutdown(ctx)`
   - `nc.Drain()`
   - `tracerShutdown(ctx)`

No database connections, no JetStream usage.

### `handler.go`

```go
type Handler struct {
    siteID string
}

func NewHandler(siteID string) *Handler { return &Handler{siteID: siteID} }

func (h *Handler) Register(r *natsrouter.Router) {
    natsrouter.Register(r, "chat.user.{account}.request.user.{siteID}.status.getByName",      h.statusGetByName)
    natsrouter.Register(r, "chat.user.{account}.request.user.{siteID}.status.set",            h.statusSet)
    natsrouter.Register(r, "chat.user.{account}.request.user.{siteID}.profile.getByName",     h.profileGetByName)
    natsrouter.Register(r, "chat.user.{account}.request.user.{siteID}.subscription.getCurrent",    h.subscriptionGetCurrent)
    natsrouter.Register(r, "chat.user.{account}.request.user.{siteID}.subscription.getRooms",      h.subscriptionGetRooms)
    natsrouter.Register(r, "chat.user.{account}.request.user.{siteID}.subscription.getChannels",   h.subscriptionGetChannels)
    natsrouter.Register(r, "chat.user.{account}.request.user.{siteID}.subscription.getDM",         h.subscriptionGetDM)
    natsrouter.Register(r, "chat.user.{account}.request.user.{siteID}.subscription.getApps",       h.subscriptionGetApps)
    natsrouter.Register(r, "chat.user.{account}.request.user.{siteID}.subscription.subscribeApp",  h.subscriptionSubscribeApp)
    natsrouter.Register(r, "chat.user.{account}.request.user.{siteID}.subscription.unsubscribeApp", h.subscriptionUnsubscribeApp)
    natsrouter.RegisterNoBody(r, "chat.user.{account}.request.user.{siteID}.room.{roomID}.subscription.get", h.roomSubscriptionGet)
    natsrouter.RegisterNoBody(r, "chat.user.{account}.request.user.{siteID}.apps.list",                       h.appsList)
}
```

Each handler first calls a shared `checkSite(c)` helper:

```go
func (h *Handler) checkSite(c *natsrouter.Context) error {
    if c.Param("siteID") != h.siteID {
        return natsrouter.ErrNotFound("unknown site")
    }
    return nil
}
```

Request/response types are unexported and declared at the top of
`handler.go` — they consume `model.Subscription` and `model.App` from
`pkg/model` for the embedded values.

### Mock data constants

```go
const (
    mockStatusText   = "available"
    mockStatusIsShow = true
    mockDisplayName  = "Mock User"
    mockEmail        = "mock@example.test"
)
```

Mock `model.Subscription` and `model.App` fixtures are constructed inside
small package-level helper functions (`buildMockSub(account, siteID)`,
`buildMockApp(id, name)`) so each handler can call them and unit tests can
assert against the same source of truth.

## Tests

`handler_test.go` (build tag: none — pure unit, no external deps):

- One `t.Run`-driven block per handler. Each block covers:
  - **happy path**: valid `account` + `siteID` params; assert response shape,
    `Total` field where applicable, and that echoed input fields appear in
    the response.
  - **siteID mismatch**: `c.Param("siteID")` set to a different value;
    assert handler returns `natsrouter.ErrNotFound` with code `not_found`.
- For `roomSubscriptionGet`: also assert `Subscription.RoomID == roomID`
  from the param.
- For `subscriptionGetDM`: assert `Subscription.User.Account ==
  TargetAccount` from the request.

All tests use `natsrouter.NewContext(map[string]string{…})` — no live NATS
connection, no goroutine spawning.

Coverage target: ≥ 90% per the project's core-business-logic threshold.

## `docs/client-api.md` updates

A new top-level section "User RPC (mock-user-service)" is added. Each of the
12 subjects gets:

- Concrete subject string (with builder reference).
- Request JSON shape (or "no body" for `RegisterNoBody` routes).
- Response JSON shape with field descriptions.
- Error cases: at minimum `unknown site` → `{"error":"unknown site","code":"not_found"}`.
- Note that the service is dev-only and stateless.

## Deployment

### Dockerfile (`mock-user-service/deploy/Dockerfile`)

Multi-stage build per the project convention:

- Stage 1: `golang:1.25.8-alpine` — `COPY go.mod go.sum`, `go mod download`,
  copy source, `go build -o /out/mock-user-service ./mock-user-service`.
- Stage 2: `alpine:3.21` — `COPY --from=builder /out/mock-user-service /app/`,
  set non-root user, `ENTRYPOINT ["/app/mock-user-service"]`.

Build context is the repo root so `pkg/` and `go.mod` are accessible.

### docker-compose.yml (`mock-user-service/deploy/docker-compose.yml`)

Two services only:

- `nats:2.10-alpine` with `--jetstream --http_port 8222` (JetStream enabled
  for parity even though this service does not use it).
- `mock-user-service` built from `../../` with `NATS_URL=nats://nats:4222`
  and `SITE_ID=site-local`.

### azure-pipelines.yml

Adapted from a sibling Gin-less service (e.g. `message-worker`). No
service-specific test selection beyond `make test SERVICE=mock-user-service`.

## Out of scope / explicitly rejected

- Persistence (in-memory or otherwise). The user chose stateless for
  `status.set`; same rationale applies to subscribe/unsubscribeApp.
- A `subscription.count` route — considered, then removed to keep the surface
  at 12 routes.
- New `pkg/model` types. Request/response types stay private to the service.
- Integration tests with testcontainers.

## Risks

- **Schema drift between mock and future real service.** Mitigated by
  pinning subject strings and request/response shapes in `pkg/subject` and
  `docs/client-api.md` rather than re-inventing them inside the mock.
- **Accidental production deployment.** Mitigated by naming
  (`mock-user-service`), the dev-only note in `client-api.md`, and the
  absence of any persistence or auth. No further runtime guard is added —
  ops controls deployment.

## Future work (NOT part of this spec)

- A real user-service implementation: mongo-backed status/profile, real
  subscription queries (likely sourced from the existing subscription store),
  real `subscribeApp`/`unsubscribeApp` writes.
