# User-Service

**Date:** 2026-06-04
**Status:** Draft — pending review

## Overview

Introduce (recreate) the `user-service` microservice: a NATS request/reply
service (Go, **no JetStream**) exposing **9 endpoints** (the 8 agreed in
`docs/user-service-endpoint-consolidation.md`, plus the later-added
`subscription.getByRoomID`). It manages
user **status**, room/app **subscriptions**, and **app** browsing, federated
across sites via room-service RPC.

**Source of truth & references.** `docs/user-service-endpoint-consolidation.md`
(the agreed table) is authoritative for *which* endpoints exist.
`claude_its_a_gift_for_u.txt` is a **dated implementation reference** — used only
for behavioral detail (Mongo aggregations, validation rules) where it does **not**
contradict the table; any conflict is escalated, not silently followed.

**The 9 endpoints**: `status.getByName`, `status.set`,
`subscription.list`, `subscription.getChannels`, `subscription.getDM`,
`subscription.getByRoomID`, `subscription.count`, `subscription.setAppSubscription`,
`apps.list`. (`subscription.getByRoomID` was added after the original 8-endpoint
agreement — a single-room subscription lookup returning a 0-or-1-element
`SubscriptionListResponse`.) The legacy
`subscription.getCurrent`/`getRooms`/`getApps` merge into `subscription.list`;
`subscribeApp`/`unsubscribeApp` merge into `subscription.setAppSubscription`;
`profile.getByName` and the employee endpoint are removed. (There is no
`room.{roomID}.subscription.get` — that was a throwaway-mock artifact, never a
real endpoint.)

**Architecture stance.** Flat `package main` does not scale to 8 endpoints that
will grow. This service is built **on top of `history-service`'s established
patterns** — a `service` package that owns the handlers as methods and registers
them, consumer-defined repository interfaces, a `mongorepo` persistence package
built on `pkg/mongoutil`, and a `publisher` adapter — but **unpacked** (top-level
packages, no `internal/`) with the entrypoint as a root `main.go`.

## Motivation

- **Single subscription read**: three near-identical getters over the same
  `Subscription` response become one `subscription.list` with a `type` parameter.
- **Idempotent app subscription**: two inverse verbs become one declarative
  `subscription.setAppSubscription` (`subscribed` end-state, PUT-like).
- **No hidden room hiding**: the legacy `GET_WITHIN_DAYS` silent filter becomes an
  opt-in `updatedWithinDays` request parameter.
- **No internal data**: `profile.getByName` and the employee endpoint are removed.
- **Future-proof structure**: a layered package design (per `history-service`)
  keeps each concern isolated and testable as the endpoint set grows.

## Architecture

### Package layout

```text
user-service/
├── main.go                 # package main — load config, connect, wire deps, svc.RegisterHandlers, shutdown; compile-time assertions
├── config/
│   ├── config.go           # package config — Config (envPrefix MONGO_/NATS_) + Load()  [mirror history-service/internal/config]
│   └── config_test.go
├── models/                 # package models — service-local request/response DTOs (one file per area), each with its own _test
│   ├── status.go           #   statusGetByName/Set request + StatusView
│   ├── status_test.go
│   ├── subscription.go     #   list/getChannels/getDM/count requests + responses
│   ├── subscription_test.go
│   ├── app.go              #   setAppSubscription request + AppListItem + AppsListResponse
│   └── app_test.go
├── service/                # package service — interfaces + handlers (one file per area, file+file_test)
│   ├── service.go          #   UserService struct, New(), RegisterHandlers; interfaces + mockgen
│   ├── service_test.go     #   shared test helpers (newSvc, ctx, requireCode)
│   ├── status.go           #   status.getByName / status.set handlers
│   ├── status_test.go
│   ├── subscriptions.go    #   subscription.list / getChannels / getDM / count handlers
│   ├── subscriptions_test.go
│   ├── apps.go             #   subscription.setAppSubscription / apps.list handlers
│   ├── apps_test.go
│   └── mocks/              #   generated mocks (mockgen -destination=mocks/mock_repository.go)
├── mongorepo/              # package mongorepo — ONE FILE PER COLLECTION (file+file_test), pipelines inline
│   ├── store.go            #   Store struct, New(), EnsureIndexes, shared helpers
│   ├── users.go            #   users collection: GetUserStatus / SetUserStatus
│   ├── users_test.go
│   ├── subscriptions.go    #   subscriptions collection: Aggregate/FindChannels/GetDM/Count/GetActive/GetAppSub/SetAppSubscribed (+ inline pipelines)
│   ├── subscriptions_test.go
│   ├── apps.go             #   apps collection: GetApp / ListApps (+ inline pipeline)
│   ├── apps_test.go
│   ├── setup_test.go       #   integration harness (newTestStore, seed) — mirrors history-service/internal/mongorepo/setup_test.go
│   └── main_test.go        #   TestMain → testutil.RunTests(m)
├── roomclient/             # package roomclient — Client impl of service.RoomClient (NATS-RPC); errcode.Parse on remote envelopes
│   ├── client.go
│   └── client_integration_test.go
├── publisher/              # package publisher — Publisher impl of service.EventPublisher (core nc.Publish to OUTBOX)
│   ├── publisher.go
│   └── publisher_test.go
└── deploy/{Dockerfile, docker-compose.yml, azure-pipelines.yml}
```

Test organization (mirrors history-service): every source file has its own
`_test.go` — no merged test blobs. Integration packages use a shared
`setup_test.go` + `main_test.go` (`testutil.RunTests`); unit packages use
table-driven tests with `testify` + `go.uber.org/mock`.

Dependency direction: `main → {config, service, mongorepo, roomclient, publisher}`;
`models` is a leaf (imports only `pkg/model`), consumed by `service` and
`mongorepo`; `service` depends only on its own interfaces (+ `models`). No import
cycles. Compile-time assertions live in
`main.go`: `var _ service.UserStore = (*mongorepo.Store)(nil)`,
`var _ service.RoomClient = (*roomclient.Client)(nil)`,
`var _ service.EventPublisher = (*publisher.Publisher)(nil)`.

### Wiring (`main.go`) — mirrors `history-service/cmd/main.go`

```go
func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := config.Load()
	if err != nil { slog.Error("parse config", "error", err); os.Exit(1) }

	ctx := context.Background()
	tracerShutdown, err := otelutil.InitTracer(ctx, "user-service")
	if err != nil { slog.Error("init tracer failed", "error", err); os.Exit(1) }

	nc, err := natsutil.Connect(cfg.NATS.URL, cfg.NATS.CredsFile)
	if err != nil { slog.Error("nats connect failed", "error", err); os.Exit(1) }

	mongoClient, err := mongoutil.Connect(ctx, cfg.Mongo.URI, cfg.Mongo.Username, cfg.Mongo.Password)
	if err != nil { slog.Error("mongo connect failed", "error", err); os.Exit(1) }
	db := mongoClient.Database(cfg.Mongo.DB)

	store := mongorepo.New(db)
	if err := store.EnsureIndexes(ctx); err != nil { slog.Error("ensure indexes failed", "error", err); os.Exit(1) }

	rooms := roomclient.New(nc, cfg.SiteID)
	pub := publisher.New(nc)
	svc := service.New(store, rooms, pub, &cfg)

	router := natsrouter.New(nc, "user-service")
	router.Use(natsrouter.Recovery())
	router.Use(natsrouter.RequestID())
	router.Use(natsrouter.Logging())
	svc.RegisterHandlers(router, cfg.SiteID)

	slog.Info("user-service running", "site", cfg.SiteID)
	shutdown.Wait(ctx, 25*time.Second,
		func(ctx context.Context) error { return router.Shutdown(ctx) },
		func(ctx context.Context) error { return nc.Drain() },
		func(ctx context.Context) error { mongoutil.Disconnect(ctx, mongoClient); return nil },
		func(ctx context.Context) error { return tracerShutdown(ctx) },
	)
}
```

### Request flow

```text
Client → NATS request/reply
  chat.user.{account}.request.user.{siteID}.{area}.{action}
        │
        ▼
service.UserService method  (registered via natsrouter; queue group "user-service")
  ├─ validate (c.Param, req body)
  ├─ mongorepo (pkg/mongoutil): users / subscriptions / rooms / apps
  ├─ roomclient (nc.Request → chat.server.*): RoomsInfoBatch, RoomCreateDMSync
  ├─ publisher (nc.Publish): status.set → OUTBOX
  └─ return (resp, error) → natsrouter → errnats marshals the reply
```

### Implementation conventions (REQUIRED — mirror history-service)

These are not optional; the implementation must follow them exactly:

1. **natsrouter for all endpoints.** `router := natsrouter.New(nc, "user-service")`
   with `Recovery()` + `RequestID()` + `Logging()` middleware. Each endpoint is a
   **method on `*UserService`** with signature
   `func (s *UserService) X(c *natsrouter.Context, req ReqT) (*RespT, error)`
   (or `RegisterNoBody` for `apps.list`), registered in `RegisterHandlers` via
   `natsrouter.Register(r, subject.XPattern(siteID), s.X)`. Read params with
   `c.Param("account"/"siteID"/"roomID")`; attach `c.WithLogValues(...)`.
2. **Errors via errnats.** Handlers **return** typed `pkg/errcode` errors; never
   call `msg.Respond`/marshal manually. `natsrouter`'s `replyErr` calls
   `errnats.Reply(c, c.Msg, err)`, which classifies, logs once, and writes the
   wire envelope. No log-and-return.
3. **Interface wiring like history-service.** Consumer-defined interfaces
   (`UserStore`, `RoomClient`, `EventPublisher`) are declared in the `service`
   package with a single `//go:generate mockgen -destination=mocks/mock_repository.go -package=mocks . UserStore,RoomClient,EventPublisher`
   directive; impls live in `mongorepo`/`roomclient`/`publisher`; compile-time
   `var _ service.X = (*pkg.Y)(nil)` assertions in `main.go`.
4. **Heavy reliance on `pkg/mongoutil`.** All Mongo access goes through
   `mongoutil.Connect`, `mongoutil.NewCollection[T]`, and its exported methods
   (`FindOne`, `FindByID`, `FindMany`, `Aggregate`, `AggregatePaged`,
   `WithProjection`/`WithSort`/`WithLimit`, `.Raw()` only for `UpdateOne`/
   `CountDocuments` where no typed wrapper exists). No hand-rolled driver setup.
   Reuse other `pkg/` exports likewise: `natsutil`, `otelutil`, `shutdown`,
   `errcode`/`errnats`, `subject`, `model`, `idgen`.

## Subject Definitions

Base `chat.user.{account}.request.user.{siteID}.{area}.{action}`; queue group
`user-service`; registered with the `…Pattern` builders.

**Add** (`pkg/subject`, with `subject_test.go` cases): `UserSubscriptionList` /
`…ListPattern`; `UserSubscriptionSetAppSubscription` / `…Pattern`;
`UserSubscriptionGetByRoomID` / `…Pattern`.

**Keep**: `UserStatusGetByName`, `UserStatusSet`, `UserSubscriptionGetChannels`,
`UserSubscriptionGetDM`, `UserSubscriptionCount`, `UserAppsList` (+ `…Pattern`).

**Remove** (+ `…Pattern`/`…WildCard`): `UserSubscriptionGetCurrent`,
`UserSubscriptionGetRooms`, `UserSubscriptionGetApps`,
`UserSubscriptionSubscribeApp`, `UserSubscriptionUnsubscribeApp`,
`UserProfileGetByName`, `UserRoomSubscriptionGet`. **Caller migration:**
`tools/loadgen/daily_actions.go:94` (`refreshRoomList`) → `UserSubscriptionList`
with `{"type":"rooms"}`. The `mock-user-service` is superseded and removed.

## Endpoints & DTOs

Registered in `service.RegisterHandlers`. **Request/response DTOs live in the
`user-service/models` package** (package `models`), one file per area
(`status.go`/`subscription.go`/`app.go`), each with its own `_test.go`
(marshal round-trip per `pkg/model/model_test.go` style). All DTOs are exported
because the `service` and `mongorepo` packages consume them. Handlers reference
them as `models.X`; the wire schema is unchanged.

| # | Subject | Method (`*UserService`) | Request | Response |
|---|---------|-------------------------|---------|----------|
| 1 | `status.getByName` | `StatusGetByName` | `models.StatusGetByNameRequest` | `models.StatusView` |
| 2 | `status.set` | `StatusSet` | `models.StatusSetRequest` | `models.StatusView`; outbox `model.UserStatusUpdated` |
| 3 | `subscription.list` | `ListSubscriptions` | `models.SubscriptionListRequest` | `models.SubscriptionListResponse` |
| 4 | `subscription.getChannels` | `GetChannels` | `models.GetChannelsRequest` | `models.SubscriptionListResponse` |
| 5 | `subscription.getDM` | `GetDM` | `models.GetDMRequest` | `models.DMResponse` (`model.DMSubscription`) |
| 6 | `subscription.getByRoomID` | `GetByRoomID` | `models.GetByRoomIDRequest` | `models.SubscriptionListResponse` (0-or-1) |
| 7 | `subscription.count` | `CountSubscriptions` | `models.CountRequest` | `models.CountResponse` |
| 8 | `subscription.setAppSubscription` | `SetAppSubscription` | `models.SetAppSubscriptionRequest` | `models.OKResponse` |
| 9 | `apps.list` | `AppsList` (`RegisterNoBody`) | *(none)* | `models.AppsListResponse` |

```go
// models/subscription.go
type SubscriptionListRequest struct {
	Type              string `json:"type"`                        // "current" | "rooms" | "apps"
	Favorite          *bool  `json:"favorite,omitempty"`
	UpdatedWithinDays *int   `json:"updatedWithinDays,omitempty"` // rooms only (ignored for current/apps)
}
type SubscriptionListResponse struct {
	Subscriptions []model.Subscription `json:"subscriptions"`
	Total         int                  `json:"total"`
}
// (GetChannelsRequest, GetDMRequest, DMResponse, CountRequest, CountResponse here too)

// models/status.go
type StatusView struct {
	Account      string `json:"account"`
	StatusText   string `json:"statusText"`
	StatusIsShow *bool  `json:"statusIsShow,omitempty"`
	ChineseName  string `json:"chineseName,omitempty"`
	EngName      string `json:"engName,omitempty"`
}
// (StatusGetByNameRequest, StatusSetRequest here too)

// models/app.go — AppListItem is an app + the requesting user's subscription flag.
type AppListItem struct {
	model.App
	IsSubscribed bool `json:"isSubscribed"`
}
// (SetAppSubscriptionRequest, AppsListResponse, OKResponse here too)
```

`type` maps: `current` → all current subs (rooms + apps); `rooms` → `dm`+`channel`;
`apps` → `botDM` with `isSubscribed=true`.

## Service layer (`service` package)

`UserService` holds the three interfaces + site config and owns every handler.

```go
type UserService struct {
	store      UserStore
	rooms      RoomClient
	publisher  EventPublisher
	siteID     string
	allSiteIDs []string
	maxSubs    int
}

func New(store UserStore, rooms RoomClient, pub EventPublisher, cfg *config.Config) *UserService { … }

func (s *UserService) RegisterHandlers(r *natsrouter.Router, siteID string) {
	natsrouter.Register(r, subject.UserStatusGetByNamePattern(siteID), s.StatusGetByName)
	natsrouter.Register(r, subject.UserStatusSetPattern(siteID), s.StatusSet)
	natsrouter.Register(r, subject.UserSubscriptionListPattern(siteID), s.ListSubscriptions)
	natsrouter.Register(r, subject.UserSubscriptionGetChannelsPattern(siteID), s.GetChannels)
	natsrouter.Register(r, subject.UserSubscriptionGetDMPattern(siteID), s.GetDM)
	natsrouter.Register(r, subject.UserSubscriptionGetByRoomIDPattern(siteID), s.GetByRoomID)
	natsrouter.Register(r, subject.UserSubscriptionCountPattern(siteID), s.CountSubscriptions)
	natsrouter.Register(r, subject.UserSubscriptionSetAppSubscriptionPattern(siteID), s.SetAppSubscription)
	natsrouter.RegisterNoBody(r, subject.UserAppsListPattern(siteID), s.AppsList)
}
```

**MANDATORY — `natsrouter` only.** Every handler registers through
`pkg/natsrouter` (`Register[Req,Resp]` / `RegisterNoBody[Resp]`), which itself
calls `nc.QueueSubscribe(subject, queue, …)` with the `"user-service"` queue group
(`pkg/natsrouter/router.go:217`) and supplies the Recovery/RequestID/Logging
middleware, typed JSON (un)marshaling, and `errnats.Reply` error envelopes. The
gift guide's pattern (§2.4/§2.5 — three wildcard `nc.QueueSubscribe(status.>/…)`
calls with hand-written `dispatchStatus`/`dispatchSubscription` fan-out) is
**explicitly rejected**: raw `nc.QueueSubscribe` and manual dispatch MUST NOT
appear anywhere in this service. One `natsrouter` registration per concrete
action subject, never a wildcard dispatcher.

### Interfaces (consumer-defined, `service/service.go`)

Imports: `model "…/pkg/model"` (shared domain + events) and
`models "…/user-service/models"` (local DTOs).

```go
//go:generate mockgen -destination=mocks/mock_repository.go -package=mocks . UserStore,RoomClient,EventPublisher

type UserStore interface {
	GetUserStatus(ctx context.Context, account string) (*model.User, error)
	SetUserStatus(ctx context.Context, account, text string, isShow *bool) error
	AggregateSubscriptions(ctx context.Context, account, listType string, withinDays *int, limit int) ([]model.Subscription, error)
	FindChannelsByMembers(ctx context.Context, account string, members []string) ([]model.Subscription, error)
	GetDMSubscription(ctx context.Context, account, target string) (*model.DMSubscription, error)
	CountActiveSubscriptions(ctx context.Context, account string) (int, error)
	GetActiveSubscriptions(ctx context.Context, account string, limit int) ([]model.Subscription, error)
	GetApp(ctx context.Context, appID string) (*model.App, error)
	ListApps(ctx context.Context, account string) ([]models.AppListItem, error)
	GetAppSubscription(ctx context.Context, account, botName string) (*model.Subscription, error)
	SetAppSubscribed(ctx context.Context, account, botName string, subscribed, muted bool) error
}

type RoomClient interface {
	GetRoomsInfo(ctx context.Context, siteID string, roomIDs []string) ([]model.RoomInfo, error)
	CreateDMRoom(ctx context.Context, account, otherAccount string, roomType model.RoomType) (model.Subscription, error)
}

type EventPublisher interface {
	Publish(ctx context.Context, subject string, data []byte) error
}
```

### Handler-method behavior

Site isolation is **structural**, not a per-handler guard: the `*Pattern` builders bake `cfg.SiteID` as a literal subject token (only `{account}` is captured), so the instance subscribes only to subjects containing its own site. A `c.Param("siteID")` guard is impossible — that param is never captured (always `""`) — so handlers start directly with validation/business logic. `s.siteID`/`s.allSiteIDs` are used only by `publishStatus` for cross-site outbox routing.

- **`StatusGetByName`** — `store.GetUserStatus` (reads the `users` doc, which gains `statusText`/`statusIsShow` — see Model changes); nil → `errcode.NotFound`; map `model.User` → `StatusView` (`account`, `statusText`, `statusIsShow`, `engName`, `chineseName`).
- **`StatusSet`** — validate `len(text) ≤ 512`; `store.SetUserStatus`; publish `UserStatusUpdated` to OUTBOX per remote site (Cross-Site Federation); return the refreshed `StatusView`.
- **`ListSubscriptions`** — validate `type ∈ {current,rooms,apps}` (else `errcode.BadRequest`); `store.AggregateSubscriptions(account, type, withinDays, s.maxSubs)`. `updatedWithinDays == nil` ⇒ **no age filter** (there is no server-side default). When `favorite=true`: **(a) filter to `favorite:true` only AND (b) move the self-DM to the front** — two separate operations (a `dm` sub whose counterpart `name` == `account`). Order favorite-first then `name` — the sort key is the `favorite` **bool** (the dated guide's `favoritedAt` field does not exist in `model.Subscription` — **verified**). **Then enrich the result bodies** via `enrichWithRoomInfo` (see below).
- **`GetChannels`** — exactly one of `membersContain`/`accountNames` (else `BadRequest`): `membersContain` matches channels containing that **one** account; `accountNames` matches channels containing **all** listed accounts. `store.FindChannelsByMembers` (bots excluded, `createdAt` DESC — the sort key comes from the local `rooms` `$lookup`). **Then `enrichWithRoomInfo`.**
- **`GetDM`** — reject `strings.HasPrefix(target,"p_")`/`HasSuffix(target,".bot")` → `BadRequest(errcode.UserInvalidDMTarget)`; `store.GetDMSubscription` (joins the local `users` collection to populate the counterpart's **`account`, `name`, `engName`** — the only 3 `model.SubscriptionHRInfo` fields, `pkg/model/subscription.go:57-61` — into `DMSubscription.HRInfo`). **For a cross-site DM counterpart the local `users` join finds nothing → `HRInfo` is left nil/empty** (no cross-site user RPC; fallback is empty). Nil sub → `NotFound(errcode.UserSubscriptionNotFound)`; **enrich via `enrichWithRoomInfo`**; return `model.DMSubscription`.
- **`CountSubscriptions`** — total via `store.CountActiveSubscriptions`; if `unread`, group `store.GetActiveSubscriptions` by `siteID`, parallel `rooms.GetRoomsInfo` via `errgroup` into a per-site indexed slice (race-free), count where `sub.LastSeenAt.UTC().UnixMilli() < *info.LastMsgAt` (units differ — `LastSeenAt *time.Time` vs `LastMsgAt *int64` millis; nil `LastMsgAt` ⇒ not unread; nil `LastSeenAt` + non-nil `LastMsgAt` ⇒ unread); **any RPC error → return total** (lossy fallback, documented).
- **`SetAppSubscription`** — `store.GetApp`; nil → `NotFound(UserAppNotFound)`; nil/disabled `Assistant` → `BadRequest(UserAppDisabled)`; `subscribed=true`: no botDM sub → `rooms.CreateDMRoom(account, Assistant.Name, model.RoomTypeBotDM)`, else `store.SetAppSubscribed(...,true,false)` (PUT: always clears `muted`); `subscribed=false`: `store.SetAppSubscribed(...,false,true)`.
- **`AppsList`** — `store.ListApps` (per-user `isSubscribed` via `$lookup`).

### Room-info enrichment (`enrichWithRoomInfo`) — shared by all subscription endpoints

`subscription.list`, `subscription.getChannels`, and `subscription.getDM` return
**enriched** subscription bodies, not raw Mongo rows. Two stages:

1. **Mongo `$lookup` to the local `rooms` collection (deleted-filter).** The
   aggregation joins `subscriptions.roomId → rooms._id`. Rooms are **soft-deleted
   by renaming** (no `deleted`/`deletedAt` flag exists on `model.Room`): the filter
   drops any joined room whose `name` starts with `Del-`
   (`name: {$not: {$regex: "^Del-"}}`). For **local-site** subs
   (`sub.siteId == s.siteID`) a deleted/missing join → **drop the sub**.
   **Cross-site** subs have no local `rooms` doc, so the local join is empty by
   design — they are **kept** and resolved by RPC in stage 2.
2. **RPC enrichment (`enrichWithRoomInfo`).** Group the surviving subs by
   `siteId`, fire **parallel** `rooms.GetRoomsInfo(siteID, roomIDs)` per site
   (`errgroup`, race-free per-site slices), and merge `model.RoomInfo`'s fields
   back onto each sub. Per-room handling:
   - `info.Found == false` (e.g. a remotely-deleted cross-site room) → **skip
     enrichment but KEEP the sub** in the list (no room metadata).
   - **If a site's RPC fails**, its subs are returned **without room metadata**
     (graceful degradation — never drop on RPC error).

**Enriched response shape (decided — map to this repo's existing fields).** The
internal repo's `hasUnread`/`hasGroupMention`/`sub.room` names are the *original
system's*; this service maps them onto the fields the frontend already contracts
(`chat-frontend/src/api/types.ts:31-54`), overwriting the sub in place:

| Internal-repo name | This-repo field set during enrichment | Source |
|---|---|---|
| `hasUnread` | `Subscription.alert` ← `roomLastMsgAt > LastSeenAt` | local `Room.LastMsgAt` or RPC `RoomInfo.LastMsgAt` |
| `hasGroupMention` | `Subscription.hasMention` ← `roomLastMentionAllAt > LastSeenAt` | local `Room.LastMentionAllAt` or RPC `RoomInfo.LastMentionAllAt` |
| `sub.room.{…}` (nested) | flat `Subscription.{userCount, lastMsgAt, lastMsgId}` | local `Room.{UserCount, LastMsgAt, LastMsgID}` only |

`Subscription.name` is also refreshed from the room name. Because `model.RoomInfo`
(the RPC reply) carries only `Name`/`LastMsgAt`/`LastMentionAllAt`, **cross-site**
rows get `alert`/`hasMention`/`lastMsgAt`/`name` but **not** `userCount`/`lastMsgId`
(those come only from the local `Room` `$lookup`); the frontend already types them
optional, so missing ⇒ default. **No frontend or `docs/client-api.md` schema change**
— the model just gains the 3 flat fields the wire contract already expects (see
Model changes). Compare timestamps in a single unit (`LastSeenAt` is `*time.Time`,
RPC `LastMsgAt` is `*int64` millis — normalize as in `CountSubscriptions`).

## Persistence (`mongorepo` package)

`mongorepo.Store` implements `service.UserStore`, holding **four**
`*mongoutil.Collection[T]` (users/subscriptions/apps/**rooms**), mirroring
`history-service/internal/mongorepo`. The `rooms` collection is **read-only** —
used only as the `$lookup` target that filters deleted local rooms out of the
subscription aggregations (gift Ch.4 + Critical Detail #2). **One source file per
collection** (each with its own `_test.go` — no test blobs); pipeline builders are
inline in the collection file that uses them (no separate `pipelines.go`).

```text
mongorepo/
├── store.go    # Store struct, New(), EnsureIndexes, shared helpers (db handle)
├── users.go    # GetUserStatus, SetUserStatus
├── subscriptions.go  # AggregateSubscriptions, FindChannelsByMembers, GetDMSubscription,
│                     #   CountActiveSubscriptions, GetActiveSubscriptions, GetAppSubscription, SetAppSubscribed
│                     #   (all $lookup the `rooms` collection to drop deleted local rooms)
└── apps.go     # GetApp, ListApps
```
```go
type Store struct {
	users         *mongoutil.Collection[model.User]
	subscriptions *mongoutil.Collection[model.Subscription]
	apps          *mongoutil.Collection[model.App]
	rooms         *mongoutil.Collection[model.Room] // read-only; $lookup target only
}
func New(db *mongo.Database) *Store { … mongoutil.NewCollection[…](db.Collection("…")) … }
```

- Reads via `FindOne`/`FindByID`/`FindMany`/`Aggregate` (+ `WithProjection`/
  `WithSort`/`WithLimit`); missing → `(nil, nil)` (handled by `mongoutil.FindOne`).
- **`users.go`** — `GetUserStatus` (`FindOne{account, active:true}`),
  `SetUserStatus` (`.Raw().UpdateOne` `$set` statusText/statusIsShow).
- **`subscriptions.go`** — `AggregateSubscriptions`: **`type=current` keeps the
  gift's `$facet` pipeline** — a `rooms` branch (`roomType ∈ [dm,channel]`,
  `$lookup` → `users`) and an `apps` branch (`roomType = botDM`, `$lookup` →
  `apps`), merged via `$concatArrays` — because the two branches need *different*
  per-type joins; a single `$match` cannot join the right metadata per type.
  `type=rooms` is the rooms branch only; `type=apps` the apps branch only. All
  variants then `$lookup` `rooms` to drop `Del-`-prefixed (deleted) local rooms +
  `$sort{favorite:-1,name:1}` + `$limit` (and the optional `_updatedAt` window).
  `FindChannelsByMembers` (`$group`/`$all`
  intersection + `rooms` `$lookup` for the `createdAt` DESC sort),
  `GetDMSubscription` (returns `*model.DMSubscription`; `$lookup`
  to `users` for the 3 hr-info fields), `CountActiveSubscriptions`
  (`.Raw().CountDocuments` with the exact `$or`: `{roomType ∈ [dm,channel],
  muted ≠ true}` **OR** `{roomType = botDM, muted ≠ true, isSubscribed = true}`),
  `GetActiveSubscriptions` (same active set via `FindMany`+`WithLimit`),
  `GetAppSubscription`, `SetAppSubscribed` (`.Raw().UpdateOne` `$set` isSubscribed/muted).
- **`apps.go`** — `GetApp` (`FindByID`), `ListApps` (`$lookup`+`$addFields(isSubscribed)`,
  decoded as `[]models.AppListItem` — `mongorepo` imports `user-service/models`).
- **All user reads filter `active: true`.**
- `EnsureIndexes(ctx)`: `subscriptions{u.account, roomType}`, `{roomId, u.account}`,
  `{name, roomType}`; `users{account}` unique.

## RoomClient (`roomclient` package)

`roomclient.Client` implements `service.RoomClient` via `nc.RequestWithContext`
on **server-scoped** subjects `subject.RoomsInfoBatch(siteID)` /
`subject.RoomCreateDMSync(siteID)`. A non-OK reply is decoded with
`errcode.Parse(data)` and the parsed `*errcode.Error` is returned **as-is** (it
satisfies `error`; never re-wrap through a constructor — `Parse` does not validate
`Code`). The wired `nc` is the `*otelnats.Conn` returned by `natsutil.Connect`
(`github.com/Marz32onE/instrumentation-go/otel-nats/otelnats`), so
`roomclient.New(nc *otelnats.Conn, siteID string)` (matching
`message-gatekeeper/fetcher_history.go`). `RoomsInfoBatch` is server-scoped on
**room-service**; `RoomCreateDMSync` is handled by **room-worker**
(`natsServerCreateDM`). `CreateDMRoom` sends `model.SyncCreateDMRequest{RoomType,
RequesterAccount, OtherAccount}` and returns `reply.Subscription` from
`model.SyncCreateDMReply`. Errors flow via the room-worker's `errcode` envelope
(parsed with `errcode.Parse` and returned as-is); a decoded reply with
`Success == false` is a defensive backstop returning `errcode.Internal` rather
than a zero `Subscription`.
Confirm envelopes against `docs/superpowers/specs/2026-04-14-room-info-batch-rpc-design.md`.

## Publisher (`publisher` package)

`publisher.Publisher` implements `service.EventPublisher` (mirrors
`history-service/internal/publisher`, but **core NATS** since there is no
JetStream): `New(nc *otelnats.Conn)` (the type `natsutil.Connect` returns),
`Publish(ctx, subj, data) → nc.Publish(subj, data)`.

## Config (`config` package)

Mirror `history-service/internal/config`: nested `MongoConfig`/`NATSConfig` with
`envPrefix`, plus a `Load()`:

```go
type Config struct {
	SiteID               string      `env:"SITE_ID" envDefault:"site-local"`
	AllSiteIDs           []string    `env:"ALL_SITE_IDS" envDefault:""`
	MaxSubscriptionLimit int         `env:"MAX_SUBSCRIPTION_LIMIT" envDefault:"1000"`
	Mongo                MongoConfig `envPrefix:"MONGO_"`
	NATS                 NATSConfig  `envPrefix:"NATS_"`
}
// MongoConfig{ URI required, DB=chat, Username, Password }; NATSConfig{ URL required, CredsFile }.
func Load() (Config, error) { return env.ParseAs[Config]() }
```

(The guide's `CURRENT_DOMAIN` is intentionally dropped — it served the removed
employee paths.)

## Model changes (`pkg/model`)

Cross-site / shared types live in `pkg/model` (consumed by the **remote**
`inbox-worker`, so they cannot live in the service-local `models` package). Three
additions, each with `json`+`bson` tags and a `model_test.go` round-trip case:

1. **`model.User` gains status fields** (in `pkg/model/user.go`). The current
   `model.User` has no `statusText`/`statusIsShow` (only account/site/eng+chinese
   name/HR fields), so `GetUserStatus`/`StatusView` cannot work without them. Add
   `StatusText string` and `StatusIsShow *bool` (the status payload the `users`
   doc must carry).
2. **`model.UserStatusUpdated` event** in **`pkg/model/event.go`** (beside the
   existing cross-site events `MessageEvent`, `SubscriptionUpdateEvent`,
   `OutboxEvent`, …): `{account, statusText, statusIsShow *bool, timestamp int64}`
   (`Timestamp` set at publish via `time.Now().UTC().UnixMilli()` per CLAUDE.md).
3. **`model.Subscription` gains the 3 enrichment fields the frontend already
   contracts** (`chat-frontend/src/api/types.ts:51-53`) but Go lacks today:
   `UserCount int json:"userCount,omitempty" bson:"userCount,omitempty"`,
   `LastMsgAt *time.Time json:"lastMsgAt,omitempty" bson:"lastMsgAt,omitempty"`,
   `LastMsgID string json:"lastMsgId,omitempty" bson:"lastMsgId,omitempty"`.
   They are **read-time only** — `user-service` never writes `Subscription` docs, so
   nothing persists them; the `rooms` `$lookup`'s `$addFields` materializes them from
   the joined `Room` and they must therefore be **decodable** (real `bson` tags, not
   `bson:"-"`), populated only for **local** rows (the RPC reply `model.RoomInfo` has
   no `userCount`/`lastMsgId`, so cross-site rows leave them empty → frontend defaults).
   `alert`/`hasMention` already exist and need no change. This closes the gap that
   blocked Q1's "map to existing fields"; no frontend/`client-api.md` schema change
   (the wire shape is unchanged).

(Service-local request/response DTOs do **not** go here — they live in
`user-service/models`; see Endpoints & DTOs.)

## Cross-Site Federation

`status.set` publishes `model.UserStatusUpdated` to
`subject.Outbox(s.siteID, dest, "userStatus_updated")` for each `dest` in
`AllSiteIDs` **excluding `s.siteID`**, via `s.publisher.Publish`. No streams are
created here (OUTBOX/INBOX owned by ops/IaC; remote application owned by
`inbox-worker`); no `bootstrap.go`.

**Intentional exception — do not "fix":** unlike `room-service`/`room-worker`/
`message-worker` (which JetStream-publish per-destination with `OutboxDedupID`),
user-service uses **fire-and-forget core `nc.Publish`** and **broadcasts to all
configured sites** (`ALL_SITE_IDS`). This is mandated by the gift guide §2.5/§3.2/
§5/§8 (the service has no JetStream at all); status is last-write-wins so the lack
of PubAck/dedup is acceptable. (Core publishes still land in the `OUTBOX_{siteID}`
JetStream stream.)

## Error Handling

Typed `pkg/errcode` returned from handler-methods, marshaled by `natsrouter` via
`errnats.Reply` (classify-and-log-once). Reasons in **`pkg/errcode/codes_user.go`**,
`User`-prefixed: `UserAppNotFound`, `UserAppDisabled`, `UserInvalidDMTarget`,
`UserSubscriptionNotFound`. Infra failures wrap with `fmt.Errorf("…: %w", err)` →
collapse to `internal`. `errors.Is/As`; never log raw bodies/tokens.

| Condition | Reply |
|-----------|-------|
| `type` empty/unknown · `getChannels` not exactly one filter · `status.set` >512B | `BadRequest` |
| `getDM` target `p_*`/`*.bot` | `BadRequest(UserInvalidDMTarget)` |
| app not found | `NotFound(UserAppNotFound)` |
| app nil/disabled assistant | `BadRequest(UserAppDisabled)` |
| DM not found | `NotFound(UserSubscriptionNotFound)` |
| Mongo error | wrapped → `internal` |
| room-service RPC error (count unread) | **fall back to total** |
| room-service RPC error (setAppSubscription create) | wrapped → `internal` |

## Testing (TDD)

Follow history-service's test methods. **Every source file has its own `_test.go`
— no merged test blobs.** Split by area, matching the source files.

- **`models/{status,subscription,app}_test.go`** — marshal/unmarshal round-trip
  per area (`pkg/model/model_test.go` style).
- **`service/`** — table-driven unit tests using `service/mocks` (gomock) for
  `UserStore`/`RoomClient`/`EventPublisher`; shared helpers in `service_test.go`
  (`newSvc`/`ctx`/`requireCode`), then `status_test.go` (getByName + set 512B +
  outbox-excludes-self), `subscriptions_test.go` (list types + bad type + favorite
  self-DM; getChannels exactly-one; getDM rejection + not-found; count total +
  unread + **RPC-error→total fallback**, run under `-race`), `apps_test.go`
  (setAppSubscription create/reactivate-clears-muted/unsubscribe/not-found/disabled;
  apps.list). `make generate SERVICE=user-service` regenerates `service/mocks`.
- **`mongorepo/`** (`//go:build integration`) — `main_test.go` (`TestMain →
  testutil.RunTests(m)`) + `setup_test.go` (`newTestStore`/`seed` helpers,
  `testutil.MongoDB(t, "user-service")`), then one `_test.go` per collection
  (`users_test.go`/`subscriptions_test.go`/`apps_test.go`) asserting each query +
  pipeline + indexes against a real Mongo.
- **`roomclient/client_integration_test.go`** — `testutil.NATS(t)` responders for
  `RoomsInfoBatch`/`RoomCreateDMSync`; assert decode + `errcode.Parse` path.
- **`config/config_test.go`**, **`publisher/publisher_test.go`** — unit.
- **≥80% coverage on every package**, `-race` always.

## CLAUDE.md & Client API Updates

- **CLAUDE.md**: add `user-service` (NATS request/reply, no JetStream).
- **`docs/client-api.md`** (CLAUDE.md §5): `subscription.list` &
  `subscription.setAppSubscription` schemas, new error reasons, `UserStatusUpdated`
  event, and the removal of `profile.getByName` **and** the employee endpoint.
