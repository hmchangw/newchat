# User-Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Recreate `user-service` — a NATS request/reply (no JetStream) Go service exposing the 8 agreed endpoints (status, subscriptions, apps), built on history-service's patterns, with room-info-enriched subscription replies and fire-and-forget cross-site status federation.

**Architecture:** Root `main.go` (package main) + unpacked top-level packages: `config`; `models` (service-local request/response DTOs, split by area); `service` (owns the 8 endpoints as methods on `*UserService` + `RegisterHandlers` + consumer-defined `UserStore`/`RoomClient`/`EventPublisher` interfaces + mocks + the shared `enrichWithRoomInfo` step); `mongorepo` (Mongo impl over `pkg/mongoutil`, one file per collection, read-only `rooms` join for deleted-filter + local-row enrichment); `roomclient` (room-service/room-worker RPC); `publisher` (core-NATS outbox). Source of truth: `docs/superpowers/specs/2026-06-04-user-service-design.md`.

**Tech Stack:** Go 1.25, `pkg/natsrouter` + `pkg/natsutil` (→ `*otelnats.Conn`), `pkg/mongoutil`, `pkg/errcode`/`errnats`, `pkg/subject`, `pkg/otelutil`, `pkg/shutdown`, `caarlos0/env`, `go.uber.org/mock`, `testify`, `pkg/testutil` (testcontainers), `golang.org/x/sync/errgroup`.

## FORCED implementation rules (apply to every task)

1. **natsrouter only** — endpoints are methods `func (s *UserService) X(c *natsrouter.Context, req models.T) (*models.Resp, error)` registered in `service.RegisterHandlers` via `natsrouter.Register`/`RegisterNoBody`; router built with `Recovery()`+`RequestID()`+`Logging()`; read params via `c.Param(...)`, log via `c.WithLogValues(...)`. The gift guide's raw `nc.QueueSubscribe`/`dispatchStatus` wildcard fan-out is **forbidden** — `natsrouter` wraps `QueueSubscribe` with the `"user-service"` queue group (`pkg/natsrouter/router.go:217`). One registration per concrete action subject.
2. **errnats** — handlers **return** typed `pkg/errcode` errors (constructors `BadRequest`/`NotFound`/`Internal`, `WithReason(...)`); never marshal/respond manually; `natsrouter` calls `errnats.Reply`. Infra failures return raw `fmt.Errorf("...: %w", err)`. No log-and-return.
3. **Interface wiring like history-service** — interfaces in `service`, single `//go:generate mockgen -destination=mocks/mock_repository.go -package=mocks . UserStore,RoomClient,EventPublisher`; impls in `mongorepo`/`roomclient`/`publisher`; compile-time `var _ service.X = (*pkg.Y)(nil)` in `main.go`.
4. **Heavy `pkg/mongoutil` reliance** — all Mongo via `mongoutil.Connect`/`NewCollection[T]` (`FindOne`/`FindByID`/`FindMany`/`Aggregate`/`WithProjection`/`WithSort`/`WithLimit`/`.Raw()`); never hand-roll. Reuse `natsutil`/`otelutil`/`shutdown`/`subject` likewise.
5. **DTOs live in `user-service/models`** (package `models`), split by area, each with its own `_test.go`. Shared cross-site types (`UserStatusUpdated`) and the 3 `Subscription` enrichment fields live in `pkg/model` (the remote `inbox-worker` consumes the event). The `service` package imports both: `model "…/pkg/model"` and `models "…/user-service/models"`.
6. **Every source file has its own `_test.go`** — no merged test blobs. **≥80% coverage on every package**, `-race` always.

## Confirmed repo APIs (verified before writing — do not re-litigate)

- `natsrouter`: `func NewContext(map[string]string) *Context`; `(*Context).Param(key) string`; `(*Context).WithLogValues(args ...any)`; `Register[Req,Resp]`, `RegisterNoBody[Resp]`. `*Context` satisfies `context.Context`.
- `errcode`: codes `CodeBadRequest`/`CodeNotFound`/`CodeForbidden`/`CodeConflict`/`CodeInternal`; `Error.Code` field; constructors `BadRequest/NotFound/Internal(msg string, opts ...Option) *Error`; `WithReason(Reason) Option`; `Parse([]byte) (*Error, bool)`.
- `mongoutil`: `NewCollection[T](*mongo.Collection)`; `(*Collection[T]).FindOne/FindByID/FindMany(ctx, filter, opts...)`, `.Aggregate(ctx, bson.A) ([]T, error)`, `.Raw() *mongo.Collection`; options `WithProjection/WithSort/WithLimit`. `FindOne` returns `(nil, nil)` on no-doc.
- `pkg/model`: `Room{Name, UserCount int, LastMsgAt *time.Time, LastMsgID string, LastMentionAllAt *time.Time, ...}`; `RoomInfo{RoomID, Found bool, Name, LastMsgAt *int64, LastMentionAllAt *int64, ...}`; `DMSubscription{*Subscription; HRInfo *SubscriptionHRInfo}`; `SubscriptionHRInfo{Account, Name, EngName}`; `SyncCreateDMRequest{RoomType, RequesterAccount, OtherAccount}`; `SyncCreateDMReply{Success, Subscription}`; `AppAssistant{Enabled bool, Name string}`.
- history-service mongorepo test harness: `main_test.go` → `func TestMain(m *testing.M){ testutil.RunTests(m) }`; `setup_test.go` → `func setupMongo(t) *mongo.Database { return testutil.MongoDB(t, "<prefix>") }`.

---

## Chapters

1. Foundations — `pkg/subject` builders (+ loadgen migration), `pkg/errcode/codes_user.go`, `pkg/model` changes (User status fields, `UserStatusUpdated` event, `Subscription` 3 enrichment fields).
2. `config` package.
3. `models` package — request/response DTOs + `AppListItem`, split by area.
4. `service` skeleton — interfaces, `UserService`, `RegisterHandlers`, stub handlers, mocks.
5. Status handlers (`service/status.go`).
6. `subscription.list` (`service/subscriptions.go`) — type validation + favorite filter + self-DM-front.
7. `getChannels` + `getDM`.
8. `enrichWithRoomInfo` — shared room-info enrichment; wired into list/getChannels/getDM.
9. `subscription.count` (total + unread).
10. `setAppSubscription` + `apps.list` (`service/apps.go`).
11. `mongorepo` package — per-collection files, `rooms` join, `$facet`, `Del-` filter, integration tests.
12. `roomclient` package.
13. `publisher` package.
14. `main.go` wiring + build.
15. Deploy, docs, remove `mock-user-service`.
16. Final verification.

Chapters 3–10 (models + service) are mock-driven unit tests and need no Docker. Chapters 11–12 need Docker (testcontainers). Run unit tests `make test SERVICE=user-service`, integration `make test-integration SERVICE=user-service`, mocks `make generate SERVICE=user-service`.

---

## Chapter 1: Foundations

### Task 1.1: Add `UserSubscriptionList` + `UserSubscriptionSetAppSubscription` subject builders

**Files:** Modify `pkg/subject/subject.go`; Test `pkg/subject/subject_test.go`.

- [ ] **Step 1: Add failing test rows** to the concrete, `*Pattern`, and wildcard-panic tables:

```go
{"subscription.list", subject.UserSubscriptionList("alice", "s1"), "chat.user.alice.request.user.s1.subscription.list"},
{"subscription.setAppSubscription", subject.UserSubscriptionSetAppSubscription("alice", "s1"), "chat.user.alice.request.user.s1.subscription.setAppSubscription"},
```
```go
{"subscription.list", subject.UserSubscriptionListPattern("s1"), "chat.user.{account}.request.user.s1.subscription.list"},
{"subscription.setAppSubscription", subject.UserSubscriptionSetAppSubscriptionPattern("s1"), "chat.user.{account}.request.user.s1.subscription.setAppSubscription"},
```
```go
{"UserSubscriptionList", func() { subject.UserSubscriptionList("*", "s1") }},
{"UserSubscriptionSetAppSubscription", func() { subject.UserSubscriptionSetAppSubscription("*", "s1") }},
```

- [ ] **Step 2: Run** `go test ./pkg/subject/...` → FAIL (undefined).
- [ ] **Step 3: Implement** in `pkg/subject/subject.go` beside the other `UserSubscription*` builders (match the existing helper style — confirm the validator name, e.g. `isValidAccountToken`, against the neighbouring builders before copying):

```go
func UserSubscriptionList(account, siteID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.user.%s.subscription.list", account, siteID)
}
func UserSubscriptionListPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.user.%s.subscription.list", siteID)
}
func UserSubscriptionSetAppSubscription(account, siteID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.user.%s.subscription.setAppSubscription", account, siteID)
}
func UserSubscriptionSetAppSubscriptionPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.user.%s.subscription.setAppSubscription", siteID)
}
```

- [ ] **Step 4: Run** `go test ./pkg/subject/...` → PASS.
- [ ] **Step 5: Commit** `git commit -am "feat(subject): add user subscription.list + setAppSubscription builders"`.

### Task 1.2: Migrate the loadgen caller off the removed `getRooms` builder

**Files:** Modify `tools/loadgen/daily_actions.go` (`refreshRoomList`) + `tools/loadgen/daily_actions_test.go` (`TestRefreshRoomList_Requests`).

- [ ] **Step 1: Update the test** to expect the new subject + `{"type":"rooms"}` body:

```go
func TestRefreshRoomList_Requests(t *testing.T) {
	c := &captured{}
	u := &userState{ID: "u-1", Account: "user-1"}
	ctx := actionCtx{Ctx: context.Background(), Publish: c.publish, Request: c.request, SiteID: "site-test"}
	require.NoError(t, refreshRoomList(ctx, u))
	require.Len(t, c.reqs, 1)
	require.Equal(t, subject.UserSubscriptionList("user-1", "site-test"), c.reqs[0].Subj)
	require.JSONEq(t, `{"type":"rooms"}`, string(c.reqs[0].Data))
}
```

- [ ] **Step 2: Run** `go test ./tools/loadgen/... -run TestRefreshRoomList_Requests` → FAIL.
- [ ] **Step 3: Implement** (confirm `actionCtx`/`defaultRequestTimeout`/`Request` signatures against the existing file before editing):

```go
func refreshRoomList(a actionCtx, u *userState) error {
	payload, err := json.Marshal(map[string]string{"type": "rooms"})
	if err != nil {
		return fmt.Errorf("marshal room-list: %w", err)
	}
	_, err = a.Request(a.Ctx, subject.UserSubscriptionList(u.Account, a.SiteID), payload, defaultRequestTimeout)
	if err != nil {
		return fmt.Errorf("request room-list: %w", err)
	}
	return nil
}
```

- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `git commit -am "refactor(loadgen): use subscription.list for room refresh"`.

### Task 1.3: Remove legacy subject builders

**Files:** Modify `pkg/subject/subject.go` + `pkg/subject/subject_test.go`; possibly `git rm -r mock-user-service/`.

> `mock-user-service` is superseded by this service and is deleted in Ch.15.3. If removing the legacy builders breaks its compilation (likely — it registers them), **delete the whole `mock-user-service/` directory in this task** (`git rm -r mock-user-service/`) rather than surgically editing a doomed package; Ch.15.3 then only needs to clean its `docker-local`/compose/pipeline references. This keeps `go build ./...` green at this commit. (Its sole real caller, loadgen, was migrated in Task 1.2.)

- [ ] **Step 1:** Delete `UserSubscriptionGetCurrent`, `UserSubscriptionGetRooms`, `UserSubscriptionGetApps`, `UserSubscriptionSubscribeApp`, `UserSubscriptionUnsubscribeApp`, `UserProfileGetByName`, `UserRoomSubscriptionGet` and their `…Pattern`/`…WildCard` siblings + their test rows. **Keep** `UserSubscriptionGetChannels`/`GetDM`/`Count`, `UserStatus*`, `UserAppsList`, and the `UserSubscriptionWildCard`/`UserStatusWildcard`/`UserAppsWildcard` patterns (still used for nothing in this service — `natsrouter` registers concrete subjects — but other code/tests may reference them; delete a `WildCard` only if nothing references it).
- [ ] **Step 2:** `grep -rn "UserSubscriptionGetCurrent\|UserSubscriptionGetRooms\|UserSubscriptionGetApps\|UserSubscriptionSubscribeApp\|UserSubscriptionUnsubscribeApp\|UserProfileGetByName\|UserRoomSubscriptionGet" --include=*.go .` → only `mock-user-service` (deleted Ch.15) and the test rows you just removed. Delete any remaining non-mock references.
- [ ] **Step 3: Run** `go build ./... && go test ./pkg/subject/... && make lint` → PASS, `0 issues`.
- [ ] **Step 4: Commit** `git commit -am "refactor(subject): remove legacy user builders"`.

### Task 1.4: `pkg/errcode/codes_user.go`

**Files:** Create `pkg/errcode/codes_user.go` + `pkg/errcode/codes_user_test.go`.

- [ ] **Step 1: Failing test** (match `codes_room_test.go` style):

```go
package errcode

import "testing"

func TestUserReasons(t *testing.T) {
	cases := map[Reason]string{
		UserAppNotFound:          "app_not_found",
		UserAppDisabled:          "app_disabled",
		UserInvalidDMTarget:      "invalid_dm_target",
		UserSubscriptionNotFound: "subscription_not_found",
	}
	for r, want := range cases {
		if string(r) != want {
			t.Errorf("reason %q != %q", string(r), want)
		}
	}
}
```

- [ ] **Step 2: Run** `go test ./pkg/errcode/...` → FAIL. **Step 3: Implement** `codes_user.go`:

```go
package errcode

// Constant names carry the User domain prefix; wire string values are
// unprefixed, matching house style (cf. codes_room.go: RoomUserNotFound = "user_not_found").
const (
	UserAppNotFound          Reason = "app_not_found"
	UserAppDisabled          Reason = "app_disabled"
	UserInvalidDMTarget      Reason = "invalid_dm_target"
	UserSubscriptionNotFound Reason = "subscription_not_found"
)
```

- [ ] **Step 4: Run** → PASS. **Step 5: Commit** `git commit -am "feat(errcode): user reason codes"`.

### Task 1.5: `pkg/model` changes — User status fields, `UserStatusUpdated` event, `Subscription` enrichment fields

**Files:** Modify `pkg/model/user.go`, `pkg/model/event.go`, `pkg/model/subscription.go`; Test `pkg/model/model_test.go`.

- [ ] **Step 1: Add failing round-trip cases** in `model_test.go` using the existing generic `roundTrip` helper (confirm its exact name/signature in the file first): (a) a `User` with `StatusText`/`StatusIsShow` set; (b) a `UserStatusUpdated` with all fields; (c) a `Subscription` with `UserCount`/`LastMsgAt`/`LastMsgID` set. Assert marshal/unmarshal equality.
- [ ] **Step 2: Run** `go test ./pkg/model/...` → FAIL (unknown fields/type).
- [ ] **Step 3: Implement.** Add to `model.User` (`pkg/model/user.go`):

```go
StatusText   string `json:"statusText"             bson:"statusText"`
StatusIsShow *bool  `json:"statusIsShow,omitempty" bson:"statusIsShow,omitempty"`
```

Add the event to `pkg/model/event.go` (beside `MessageEvent`/`SubscriptionUpdateEvent`/`OutboxEvent`):

```go
// UserStatusUpdated is the cross-site outbox event user-service publishes on
// status.set; the remote inbox-worker applies it. Timestamp is the event-level
// time set at publish via time.Now().UTC().UnixMilli().
type UserStatusUpdated struct {
	Account      string `json:"account"                bson:"account"`
	StatusText   string `json:"statusText"             bson:"statusText"`
	StatusIsShow *bool  `json:"statusIsShow,omitempty" bson:"statusIsShow,omitempty"`
	Timestamp    int64  `json:"timestamp"              bson:"timestamp"`
}
```

Add the 3 read-time enrichment fields to `model.Subscription` (`pkg/model/subscription.go`) — they mirror what `chat-frontend/src/api/types.ts:51-53` already expects; `user-service` never persists them (it writes only status on `users` and `isSubscribed`/`muted` on `subscriptions` via `$set`), so real `bson` tags are required only so the `rooms` `$lookup`'s `$addFields` output decodes back into the struct:

```go
// Room-level enrichment, populated at read time from the rooms $lookup
// ($addFields) for local rows; cross-site rows leave userCount/lastMsgId empty
// (the RoomsInfoBatch RPC reply has no such fields). Never persisted by user-service.
UserCount int        `json:"userCount,omitempty" bson:"userCount,omitempty"`
LastMsgAt *time.Time `json:"lastMsgAt,omitempty" bson:"lastMsgAt,omitempty"`
LastMsgID string     `json:"lastMsgId,omitempty" bson:"lastMsgId,omitempty"`
```

(`pkg/model/subscription.go` already imports `time`. `alert`/`hasMention` already exist — do not re-add.)

- [ ] **Step 4: Run** `go test ./pkg/model/...` → PASS. **Step 5: Commit** `git commit -am "feat(model): user status fields, UserStatusUpdated event, subscription enrichment fields"`.

---

## Chapter 2: `config` package

### Task 2.1: Config + Load

**Files:** Create `user-service/config/config.go`; Test `user-service/config/config_test.go`.

- [ ] **Step 1: Failing test** — set env, call `config.Load()`, assert fields; assert missing `MONGO_URI`/`NATS_URL` errors:

```go
package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoad(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://x")
	t.Setenv("NATS_URL", "nats://x")
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("ALL_SITE_IDS", "site-a,site-b")
	cfg, err := Load()
	require.NoError(t, err)
	require.Equal(t, "site-a", cfg.SiteID)
	require.Equal(t, []string{"site-a", "site-b"}, cfg.AllSiteIDs)
	require.Equal(t, "chat", cfg.Mongo.DB)
	require.Equal(t, 1000, cfg.MaxSubscriptionLimit)
}

func TestLoad_MissingRequired(t *testing.T) {
	t.Setenv("MONGO_URI", "")
	t.Setenv("NATS_URL", "")
	_, err := Load()
	require.Error(t, err)
}
```

- [ ] **Step 2: Run** `go test ./user-service/config/...` → FAIL.
- [ ] **Step 3: Implement** (mirror `history-service/internal/config`; confirm `env/v11` import path against history-service):

```go
package config

import "github.com/caarlos0/env/v11"

type MongoConfig struct {
	URI      string `env:"URI"      required:"true"`
	DB       string `env:"DB"       envDefault:"chat"`
	Username string `env:"USERNAME" envDefault:""`
	Password string `env:"PASSWORD" envDefault:""`
}

type NATSConfig struct {
	URL       string `env:"URL"        required:"true"`
	CredsFile string `env:"CREDS_FILE" envDefault:""`
}

type Config struct {
	SiteID               string      `env:"SITE_ID"                envDefault:"site-local"`
	AllSiteIDs           []string    `env:"ALL_SITE_IDS"           envDefault:""`
	MaxSubscriptionLimit int         `env:"MAX_SUBSCRIPTION_LIMIT" envDefault:"1000"`
	Mongo                MongoConfig `envPrefix:"MONGO_"`
	NATS                 NATSConfig  `envPrefix:"NATS_"`
}

func Load() (Config, error) { return env.ParseAs[Config]() }
```

(No `CURRENT_DOMAIN` — confirmed unused/dead in the gift guide; intentionally dropped.)

- [ ] **Step 4: Run** → PASS; coverage `go test ./user-service/config/ -cover` ≥ 80%. **Step 5: Commit** `git commit -am "feat(user-service): config package"`.

---

## Chapter 3: `models` package

Goal: the service-local request/response DTOs, one file per area, each exported (the `service` and `mongorepo` packages consume them) and each with a marshal round-trip `_test.go`. Import path `github.com/hmchangw/chat/user-service/models`. The wire schema matches `chat-frontend/src/api/types.ts` — do not change field names.

### Task 3.1: Status DTOs (`models/status.go`)

**Files:** Create `user-service/models/status.go`, `user-service/models/status_test.go`.

- [ ] **Step 1: Write** `models/status.go`:

```go
package models

// StatusGetByNameRequest is the body of status.getByName.
type StatusGetByNameRequest struct {
	Name string `json:"name"`
}

// StatusSetRequest is the body of status.set (Text ≤ 512 bytes).
type StatusSetRequest struct {
	Text   string `json:"text"`
	IsShow *bool  `json:"isShow,omitempty"`
}

// StatusView is the response of status.getByName / status.set.
type StatusView struct {
	Account      string `json:"account"`
	StatusText   string `json:"statusText"`
	StatusIsShow *bool  `json:"statusIsShow,omitempty"`
	ChineseName  string `json:"chineseName,omitempty"`
	EngName      string `json:"engName,omitempty"`
}
```

- [ ] **Step 2: Write the failing round-trip test** `models/status_test.go`:

```go
package models

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStatusView_RoundTrip(t *testing.T) {
	show := true
	in := StatusView{Account: "bob", StatusText: "hi", StatusIsShow: &show, ChineseName: "鮑勃", EngName: "Bob"}
	b, err := json.Marshal(in)
	require.NoError(t, err)
	var out StatusView
	require.NoError(t, json.Unmarshal(b, &out))
	require.Equal(t, in, out)
}

func TestStatusSetRequest_RoundTrip(t *testing.T) {
	in := StatusSetRequest{Text: "busy"}
	b, err := json.Marshal(in)
	require.NoError(t, err)
	var out StatusSetRequest
	require.NoError(t, json.Unmarshal(b, &out))
	require.Equal(t, in, out)
}
```

- [ ] **Step 3: Run** `go test ./user-service/models/...` → PASS (pure structs compile + round-trip). **Step 4: Commit** `git commit -am "feat(user-service): status DTOs"`.

### Task 3.2: Subscription DTOs (`models/subscription.go`)

**Files:** Create `user-service/models/subscription.go`, `user-service/models/subscription_test.go`.

- [ ] **Step 1: Write** `models/subscription.go`:

```go
package models

import "github.com/hmchangw/chat/pkg/model"

// SubscriptionListRequest is the body of subscription.list.
// Type ∈ {current, rooms, apps}. UpdatedWithinDays nil ⇒ no age filter.
type SubscriptionListRequest struct {
	Type              string `json:"type"`
	Favorite          *bool  `json:"favorite,omitempty"`
	UpdatedWithinDays *int   `json:"updatedWithinDays,omitempty"`
}

// SubscriptionListResponse is returned by subscription.list and subscription.getChannels.
type SubscriptionListResponse struct {
	Subscriptions []model.Subscription `json:"subscriptions"`
	Total         int                  `json:"total"`
}

// GetChannelsRequest is the body of subscription.getChannels (exactly one of the two set).
type GetChannelsRequest struct {
	MembersContain string   `json:"membersContain,omitempty"`
	AccountNames   []string `json:"accountNames,omitempty"`
}

// GetDMRequest is the body of subscription.getDM.
type GetDMRequest struct {
	AccountName string `json:"accountName"`
}

// DMResponse wraps the enriched DM subscription returned by subscription.getDM.
type DMResponse struct {
	Subscription model.DMSubscription `json:"subscription"`
}

// CountRequest is the body of subscription.count (Unread nil/false ⇒ total).
type CountRequest struct {
	Unread *bool `json:"unread,omitempty"`
}

// CountResponse is returned by subscription.count.
type CountResponse struct {
	Count int `json:"count"`
}
```

- [ ] **Step 2: Write the failing round-trip test** `models/subscription_test.go` covering `SubscriptionListRequest` (with `Favorite`/`UpdatedWithinDays` set), `SubscriptionListResponse` (one `model.Subscription`), `GetChannelsRequest`, `DMResponse` (a `model.DMSubscription` with `HRInfo`), and `CountResponse`. Use the same `json.Marshal`→`Unmarshal`→`require.Equal` shape as Task 3.1.
- [ ] **Step 3: Run** `go test ./user-service/models/...` → PASS. **Step 4: Commit** `git commit -am "feat(user-service): subscription DTOs"`.

### Task 3.3: App DTOs (`models/app.go`)

**Files:** Create `user-service/models/app.go`, `user-service/models/app_test.go`.

- [ ] **Step 1: Write** `models/app.go`:

```go
package models

import "github.com/hmchangw/chat/pkg/model"

// SetAppSubscriptionRequest is the body of subscription.setAppSubscription (PUT-like; Subscribed is the desired end-state).
type SetAppSubscriptionRequest struct {
	AppID      string `json:"appId"`
	Subscribed bool   `json:"subscribed"`
}

// AppListItem is an app plus the requesting user's subscription flag.
// Embedded App flattens on the wire (one extra top-level isSubscribed field).
// The bson tag is required because mongorepo.ListApps decodes the apps
// aggregation ($addFields isSubscribed) directly into []AppListItem (Ch.11).
type AppListItem struct {
	model.App
	IsSubscribed bool `json:"isSubscribed" bson:"isSubscribed"`
}

// AppsListResponse is returned by apps.list.
type AppsListResponse struct {
	Apps  []AppListItem `json:"apps"`
	Total int           `json:"total"`
}

// OKResponse is the generic success body (subscription.setAppSubscription).
type OKResponse struct {
	Success bool `json:"success"`
}
```

- [ ] **Step 2: Write the failing round-trip test** `models/app_test.go` covering `SetAppSubscriptionRequest`, `AppListItem` (with `App: model.App{ID:"a1", Name:"Helper"}` + `IsSubscribed:true` — assert the flattened `isSubscribed` survives round-trip), and `AppsListResponse`.
- [ ] **Step 3: Run** `go test ./user-service/models/...` → PASS. (`models` is a pure-struct package: `go test -cover` reports `[no statements]` — coverage is N/A here, NOT a floor failure; the round-trips exist for marshal-correctness. See Ch.16.) **Step 4: Commit** `git commit -am "feat(user-service): app DTOs"`.

---

## Chapter 4: `service` skeleton

Goal: interfaces, `UserService`, `RegisterHandlers`, generated mocks, and stub handler methods so the package compiles. Handlers get real bodies in Ch.5–10. (No `checkSite` — see the review-fix note under Task 4.1.)

### Task 4.1: Interfaces + struct + RegisterHandlers (`service/service.go`)

**Files:** Create `user-service/service/service.go`.

- [ ] **Step 1: Write** `service/service.go`:

```go
package service

import (
	"context"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/user-service/config"
	"github.com/hmchangw/chat/user-service/models"
)

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

type UserService struct {
	store      UserStore
	rooms      RoomClient
	pub        EventPublisher
	siteID     string
	allSiteIDs []string
	maxSubs    int
}

func New(store UserStore, rooms RoomClient, pub EventPublisher, cfg *config.Config) *UserService {
	return &UserService{
		store: store, rooms: rooms, pub: pub,
		siteID: cfg.SiteID, allSiteIDs: cfg.AllSiteIDs, maxSubs: cfg.MaxSubscriptionLimit,
	}
}

func (s *UserService) RegisterHandlers(r *natsrouter.Router, siteID string) {
	natsrouter.Register(r, subject.UserStatusGetByNamePattern(siteID), s.StatusGetByName)
	natsrouter.Register(r, subject.UserStatusSetPattern(siteID), s.StatusSet)
	natsrouter.Register(r, subject.UserSubscriptionListPattern(siteID), s.ListSubscriptions)
	natsrouter.Register(r, subject.UserSubscriptionGetChannelsPattern(siteID), s.GetChannels)
	natsrouter.Register(r, subject.UserSubscriptionGetDMPattern(siteID), s.GetDM)
	natsrouter.Register(r, subject.UserSubscriptionCountPattern(siteID), s.CountSubscriptions)
	natsrouter.Register(r, subject.UserSubscriptionSetAppSubscriptionPattern(siteID), s.SetAppSubscription)
	natsrouter.RegisterNoBody(r, subject.UserAppsListPattern(siteID), s.AppsList)
}
```

> **No `checkSite`/site guard (review fix).** The spec's "guard via `c.Param("siteID")`" is unimplementable: the `*Pattern` builders bake `siteID` as a **literal** subject token (`pkg/subject/subject.go:710` → `chat.user.{account}.request.user.<siteID>.status.getByName`), so the only captured param is `{account}` — `c.Param("siteID")` is always `""` and a guard would reject every request. Site isolation is already guaranteed structurally: this instance subscribes only to its own `cfg.SiteID` literal subjects, so cross-site requests never route here. Handlers therefore start directly with their logic; `s.siteID`/`s.allSiteIDs` are still used by `publishStatus` (Ch.5.3). (The spec's Handler-method-behavior "guard the site" line is stale — noted to the author.)
>
> Confirm `subject.UserStatusGetByNamePattern`/`UserStatusSetPattern`/`UserSubscriptionGetChannelsPattern`/`UserSubscriptionGetDMPattern`/`UserSubscriptionCountPattern`/`UserAppsListPattern` exist (kept in Ch.1.3). The `Register` generic infers `Req`/`Resp` from each method's signature defined in Ch.5–10.

- [ ] **Step 2: Do NOT commit yet** — `service.go` references handler methods that don't exist until Task 4.2, so the package won't compile and the pre-commit lint/test hook would reject it. Task 4.2 writes the stubs + generates mocks, then commits `service.go` + stubs + mocks together in one compiling commit.

### Task 4.2: Stub handlers + generate mocks

**Files:** Create `user-service/service/status.go`, `subscriptions.go`, `apps.go` (stubs); generate `user-service/service/mocks/mock_repository.go`.

- [ ] **Step 1: Write stub handler files** so the package compiles. Each method has the exact signature `RegisterHandlers` wires; body returns `nil, errcode.Internal("not implemented")`:
  - `status.go` — `StatusGetByName(c *natsrouter.Context, req models.StatusGetByNameRequest) (*models.StatusView, error)`, `StatusSet(c *natsrouter.Context, req models.StatusSetRequest) (*models.StatusView, error)`
  - `subscriptions.go` — `ListSubscriptions(c, models.SubscriptionListRequest) (*models.SubscriptionListResponse, error)`, `GetChannels(c, models.GetChannelsRequest) (*models.SubscriptionListResponse, error)`, `GetDM(c, models.GetDMRequest) (*models.DMResponse, error)`, `CountSubscriptions(c, models.CountRequest) (*models.CountResponse, error)`
  - `apps.go` — `SetAppSubscription(c, models.SetAppSubscriptionRequest) (*models.OKResponse, error)`, `AppsList(c *natsrouter.Context) (*models.AppsListResponse, error)` (no body — matches `RegisterNoBody`)

  Each file: `package service` + imports `github.com/hmchangw/chat/pkg/errcode`, `github.com/hmchangw/chat/pkg/natsrouter`, `github.com/hmchangw/chat/user-service/models`. Example stub:

```go
func (s *UserService) StatusGetByName(c *natsrouter.Context, req models.StatusGetByNameRequest) (*models.StatusView, error) {
	return nil, errcode.Internal("not implemented")
}
```

- [ ] **Step 2: Generate mocks** `make generate SERVICE=user-service` → creates `user-service/service/mocks/mock_repository.go` (mocks `UserStore`/`RoomClient`/`EventPublisher`; note `ListApps` returns `[]models.AppListItem`).
- [ ] **Step 3: Build** `go build ./user-service/... && make lint` → PASS, `0 issues`.
- [ ] **Step 4: Commit** `git commit -am "feat(user-service): service skeleton + mocks"`.

---

## Chapter 5: Status handlers

### Task 5.1: Shared unit-test helper (`service/service_test.go`)

**Files:** Create `user-service/service/service_test.go`.

- [ ] **Step 1: Write** the shared helpers (used by every `*_test.go` in the package — no behavior, so no failing-test step):

```go
package service

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/user-service/config"
	"github.com/hmchangw/chat/user-service/service/mocks"
)

func newSvc(t *testing.T) (*UserService, *mocks.MockUserStore, *mocks.MockRoomClient, *mocks.MockEventPublisher) {
	t.Helper()
	ctrl := gomock.NewController(t)
	store := mocks.NewMockUserStore(ctrl)
	rooms := mocks.NewMockRoomClient(ctrl)
	pub := mocks.NewMockEventPublisher(ctrl)
	cfg := &config.Config{SiteID: "site-a", AllSiteIDs: []string{"site-a", "site-b"}, MaxSubscriptionLimit: 1000}
	return New(store, rooms, pub, cfg), store, rooms, pub
}

func ctx(account, siteID string) *natsrouter.Context {
	return natsrouter.NewContext(map[string]string{"account": account, "siteID": siteID})
}

func requireCode(t *testing.T, err error, code errcode.Code) {
	t.Helper()
	var ee *errcode.Error
	require.True(t, errors.As(err, &ee), "want *errcode.Error, got %T", err)
	assert.Equal(t, code, ee.Code)
}
```

- [ ] **Step 2: Commit** `git commit -am "test(user-service): shared service test helpers"`.

### Task 5.2: `StatusGetByName` (`service/status.go`)

**Files:** Modify `user-service/service/status.go`; Test `user-service/service/status_test.go`.

- [ ] **Step 1: Failing tests** `status_test.go`:

```go
package service

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/user-service/models"
)

func TestStatusGetByName(t *testing.T) {
	svc, store, _, _ := newSvc(t)
	show := true
	store.EXPECT().GetUserStatus(gomock.Any(), "bob").
		Return(&model.User{Account: "bob", StatusText: "hi", StatusIsShow: &show, EngName: "Bob", ChineseName: "鮑勃"}, nil)
	resp, err := svc.StatusGetByName(ctx("alice", "site-a"), models.StatusGetByNameRequest{Name: "bob"})
	require.NoError(t, err)
	assert.Equal(t, "bob", resp.Account)
	assert.Equal(t, "鮑勃", resp.ChineseName)
}

func TestStatusGetByName_NotFound(t *testing.T) {
	svc, store, _, _ := newSvc(t)
	store.EXPECT().GetUserStatus(gomock.Any(), "ghost").Return(nil, nil)
	_, err := svc.StatusGetByName(ctx("alice", "site-a"), models.StatusGetByNameRequest{Name: "ghost"})
	requireCode(t, err, errcode.CodeNotFound)
}
```

(No site-mismatch test — `checkSite` was removed; see Ch.4.1. The `ctx(account, siteID)` helper keeps its `siteID` arg for readability; handlers don't read it.)

- [ ] **Step 2: Run** `make test SERVICE=user-service` → FAIL (stub returns internal). **Step 3: Implement** `status.go` (replace the stub):

```go
package service

import (
	"fmt"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/user-service/models"
)

func (s *UserService) StatusGetByName(c *natsrouter.Context, req models.StatusGetByNameRequest) (*models.StatusView, error) {
	c.WithLogValues("account", c.Param("account"), "target", req.Name)
	u, err := s.store.GetUserStatus(c, req.Name)
	if err != nil {
		return nil, fmt.Errorf("get status: %w", err)
	}
	if u == nil {
		return nil, errcode.NotFound("user not found")
	}
	return &models.StatusView{
		Account: u.Account, StatusText: u.StatusText, StatusIsShow: u.StatusIsShow,
		ChineseName: u.ChineseName, EngName: u.EngName,
	}, nil
}
```

(`c` is a `context.Context`, so it passes straight to the store.)

- [ ] **Step 4: Run** → PASS. **Step 5: Commit** `git commit -am "feat(user-service): status.getByName"`.

### Task 5.3: `StatusSet` + outbox federation

**Files:** Modify `user-service/service/status.go`; Test `user-service/service/status_test.go`.

- [ ] **Step 1: Failing tests** — 512-byte validation + fire-and-forget outbox to `site-b` only (self `site-a` skipped). gomock fails the test if `Publish` is called for `site-a`, proving self-exclusion:

```go
func TestStatusSet_TooLong(t *testing.T) {
	svc, _, _, _ := newSvc(t)
	_, err := svc.StatusSet(ctx("alice", "site-a"), models.StatusSetRequest{Text: string(make([]byte, 513))})
	requireCode(t, err, errcode.CodeBadRequest)
}

func TestStatusSet_OutboxExcludesSelf(t *testing.T) {
	svc, store, _, pub := newSvc(t)
	store.EXPECT().SetUserStatus(gomock.Any(), "alice", "busy", gomock.Any()).Return(nil)
	store.EXPECT().GetUserStatus(gomock.Any(), "alice").Return(&model.User{Account: "alice", StatusText: "busy"}, nil)
	pub.EXPECT().Publish(gomock.Any(), subject.Outbox("site-a", "site-b", "userStatus_updated"), gomock.Any()).Return(nil)
	_, err := svc.StatusSet(ctx("alice", "site-a"), models.StatusSetRequest{Text: "busy"})
	require.NoError(t, err)
}
```

(Add `"github.com/hmchangw/chat/pkg/subject"` to the test imports.)

- [ ] **Step 2: Run** → FAIL. **Step 3: Implement** in `status.go` (add imports `encoding/json`, `log/slog`, `time`, `github.com/hmchangw/chat/pkg/model`, `github.com/hmchangw/chat/pkg/subject`):

```go
const maxStatusBytes = 512

func (s *UserService) StatusSet(c *natsrouter.Context, req models.StatusSetRequest) (*models.StatusView, error) {
	account := c.Param("account")
	if len(req.Text) > maxStatusBytes {
		return nil, errcode.BadRequest("status text too long")
	}
	if err := s.store.SetUserStatus(c, account, req.Text, req.IsShow); err != nil {
		return nil, fmt.Errorf("set status: %w", err)
	}
	s.publishStatus(c, account, req.Text, req.IsShow)
	return s.StatusGetByName(c, models.StatusGetByNameRequest{Name: account})
}

// publishStatus fire-and-forget broadcasts to every OTHER configured site
// (gift guide §2.5/§3.2: core nc.Publish, no JetStream, all sites). Publish
// errors are logged, never returned — status is last-write-wins.
func (s *UserService) publishStatus(c *natsrouter.Context, account, text string, isShow *bool) {
	evt := model.UserStatusUpdated{
		Account: account, StatusText: text, StatusIsShow: isShow,
		Timestamp: time.Now().UTC().UnixMilli(),
	}
	data, err := json.Marshal(evt)
	if err != nil {
		slog.Error("marshal status outbox", "error", err)
		return
	}
	for _, dest := range s.allSiteIDs {
		if dest == s.siteID {
			continue
		}
		if err := s.pub.Publish(c, subject.Outbox(s.siteID, dest, "userStatus_updated"), data); err != nil {
			slog.Error("publish status outbox", "error", err, "dest", dest)
		}
	}
}
```

> The event-type token `"userStatus_updated"` matches the gift guide §5 (`outbox.{siteID}.to.{destSiteID}.userStatus_updated`). Confirm `subject.Outbox(siteID, destSiteID, eventType)` signature (verified: `pkg/subject/subject.go:128`).

- [ ] **Step 4: Run** `make test SERVICE=user-service` → PASS; `make lint`. **Step 5: Commit** `git commit -am "feat(user-service): status.set + fire-and-forget outbox"`.

---

## Chapter 6: `subscription.list`

**Files:** Modify `user-service/service/subscriptions.go`; Test `user-service/service/subscriptions_test.go`.

> Enrichment is added to the three subscription endpoints in Ch.8. To keep this chapter's tests green without a `RoomClient` mock, Task 6.1 defines a **temporary passthrough** `enrichWithRoomInfo` that returns its input unchanged; Ch.8 replaces its body with the real RPC logic and then adds the `rooms.GetRoomsInfo` expectations to the list/getChannels/getDM tests.

### Task 6.1: type validation + passthrough

- [ ] **Step 1: Failing tests** `subscriptions_test.go`:

```go
package service

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/user-service/models"
)

func TestListSubscriptions_Types(t *testing.T) {
	for _, typ := range []string{"current", "rooms", "apps"} {
		svc, store, _, _ := newSvc(t)
		store.EXPECT().AggregateSubscriptions(gomock.Any(), "alice", typ, gomock.Any(), 1000).
			Return([]model.Subscription{{ID: "s1"}}, nil)
		resp, err := svc.ListSubscriptions(ctx("alice", "site-a"), models.SubscriptionListRequest{Type: typ})
		require.NoError(t, err)
		assert.Equal(t, 1, resp.Total)
	}
}

func TestListSubscriptions_BadType(t *testing.T) {
	svc, _, _, _ := newSvc(t)
	for _, typ := range []string{"", "bogus"} {
		_, err := svc.ListSubscriptions(ctx("alice", "site-a"), models.SubscriptionListRequest{Type: typ})
		requireCode(t, err, errcode.CodeBadRequest)
	}
}
```

- [ ] **Step 2: Run** → FAIL. **Step 3: Implement** in `subscriptions.go` (imports: `fmt`, `pkg/errcode`, `pkg/model`, `pkg/natsrouter`, `user-service/models`):

```go
var validListTypes = map[string]bool{"current": true, "rooms": true, "apps": true}

func (s *UserService) ListSubscriptions(c *natsrouter.Context, req models.SubscriptionListRequest) (*models.SubscriptionListResponse, error) {
	if !validListTypes[req.Type] {
		return nil, errcode.BadRequest("unknown subscription type")
	}
	account := c.Param("account")
	subs, err := s.store.AggregateSubscriptions(c, account, req.Type, req.UpdatedWithinDays, s.maxSubs)
	if err != nil {
		return nil, fmt.Errorf("list subscriptions: %w", err)
	}
	if req.Favorite != nil && *req.Favorite {
		subs = filterFavorites(subs)
		subs = moveSelfDMFront(subs, account)
	}
	if err := s.enrichWithRoomInfo(c, subs); err != nil {
		return nil, err
	}
	return &models.SubscriptionListResponse{Subscriptions: subs, Total: len(subs)}, nil
}

// enrichWithRoomInfo is replaced with the real RPC implementation in Ch.8.
// Passthrough for now so Ch.6–7 tests need no RoomClient mock.
func (s *UserService) enrichWithRoomInfo(c *natsrouter.Context, subs []model.Subscription) error {
	return nil
}

// filterFavorites and moveSelfDMFront are implemented in Task 6.2; temporary
// stubs here so this task compiles:
func filterFavorites(subs []model.Subscription) []model.Subscription { return subs }
func moveSelfDMFront(subs []model.Subscription, _ string) []model.Subscription { return subs }
```

- [ ] **Step 4: Run** → PASS. **Step 5: Commit** `git commit -am "feat(user-service): subscription.list type validation"`.

### Task 6.2: favorite filter — keep favorites AND move self-DM to front

> Per spec: `favorite=true` is **two operations** — (a) keep only `favorite==true` rows, then (b) move the self-DM (a `dm` sub whose counterpart `name == account`) to the front of the filtered slice.

- [ ] **Step 1: Failing tests**:

```go
func TestFilterFavorites(t *testing.T) {
	subs := []model.Subscription{
		{ID: "a", Favorite: true},
		{ID: "b", Favorite: false},
		{ID: "c", Favorite: true},
	}
	got := filterFavorites(subs)
	require.Len(t, got, 2)
	assert.Equal(t, "a", got[0].ID)
	assert.Equal(t, "c", got[1].ID)
}

func TestMoveSelfDMFront(t *testing.T) {
	subs := []model.Subscription{
		{ID: "a", RoomType: model.RoomTypeChannel, Name: "Eng"},
		{ID: "self", RoomType: model.RoomTypeDM, Name: "alice"},
		{ID: "b", RoomType: model.RoomTypeDM, Name: "bob"},
	}
	got := moveSelfDMFront(subs, "alice")
	require.Equal(t, "self", got[0].ID)
	require.Len(t, got, 3)
}

func TestMoveSelfDMFront_NoSelf(t *testing.T) {
	subs := []model.Subscription{{ID: "a", RoomType: model.RoomTypeChannel}}
	got := moveSelfDMFront(subs, "alice")
	require.Equal(t, "a", got[0].ID)
}
```

- [ ] **Step 2: Run** → FAIL. **Step 3: Implement** (replace the two temp stubs):

```go
func filterFavorites(subs []model.Subscription) []model.Subscription {
	out := subs[:0:0]
	for _, sub := range subs {
		if sub.Favorite {
			out = append(out, sub)
		}
	}
	return out
}

func moveSelfDMFront(subs []model.Subscription, account string) []model.Subscription {
	for i, sub := range subs {
		if sub.RoomType == model.RoomTypeDM && sub.Name == account {
			out := make([]model.Subscription, 0, len(subs))
			out = append(out, sub)
			out = append(out, subs[:i]...)
			return append(out, subs[i+1:]...)
		}
	}
	return subs
}
```

- [ ] **Step 4: Run** → PASS. **Step 5: Commit** `git commit -am "feat(user-service): favorite filter + self-DM-to-front"`.

---

## Chapter 7: `getChannels` + `getDM`

**Files:** Modify `user-service/service/subscriptions.go`; Test `user-service/service/subscriptions_test.go`. (Both call the passthrough `enrichWithRoomInfo` from Ch.6; real RPC enrichment is wired in Ch.8.)

### Task 7.1: `GetChannels`

- [ ] **Step 1: Failing tests**:

```go
func TestGetChannels_ExactlyOne(t *testing.T) {
	svc, _, _, _ := newSvc(t)
	_, err := svc.GetChannels(ctx("alice", "site-a"), models.GetChannelsRequest{})
	requireCode(t, err, errcode.CodeBadRequest)
	_, err = svc.GetChannels(ctx("alice", "site-a"), models.GetChannelsRequest{MembersContain: "x", AccountNames: []string{"y"}})
	requireCode(t, err, errcode.CodeBadRequest)
}

func TestGetChannels_ByMembersContain(t *testing.T) {
	svc, store, _, _ := newSvc(t)
	store.EXPECT().FindChannelsByMembers(gomock.Any(), "alice", []string{"carol"}).Return([]model.Subscription{{ID: "c1"}}, nil)
	resp, err := svc.GetChannels(ctx("alice", "site-a"), models.GetChannelsRequest{MembersContain: "carol"})
	require.NoError(t, err)
	assert.Equal(t, 1, resp.Total)
}

func TestGetChannels_ByAccountNames(t *testing.T) {
	svc, store, _, _ := newSvc(t)
	store.EXPECT().FindChannelsByMembers(gomock.Any(), "alice", []string{"carol", "dave"}).Return([]model.Subscription{{ID: "c1"}}, nil)
	resp, err := svc.GetChannels(ctx("alice", "site-a"), models.GetChannelsRequest{AccountNames: []string{"carol", "dave"}})
	require.NoError(t, err)
	assert.Equal(t, 1, resp.Total)
}
```

- [ ] **Step 2: Run** → FAIL. **Step 3: Implement**:

```go
func (s *UserService) GetChannels(c *natsrouter.Context, req models.GetChannelsRequest) (*models.SubscriptionListResponse, error) {
	hasContain, hasNames := req.MembersContain != "", len(req.AccountNames) > 0
	if hasContain == hasNames {
		return nil, errcode.BadRequest("exactly one of membersContain or accountNames is required")
	}
	members := req.AccountNames
	if hasContain {
		members = []string{req.MembersContain}
	}
	subs, err := s.store.FindChannelsByMembers(c, c.Param("account"), members)
	if err != nil {
		return nil, fmt.Errorf("get channels: %w", err)
	}
	if err := s.enrichWithRoomInfo(c, subs); err != nil {
		return nil, err
	}
	return &models.SubscriptionListResponse{Subscriptions: subs, Total: len(subs)}, nil
}
```

- [ ] **Step 4: Run** → PASS. **Step 5: Commit** `git commit -am "feat(user-service): subscription.getChannels"`.

### Task 7.2: `GetDM`

> `GetDMSubscription` (Ch.11) populates the 3 `SubscriptionHRInfo` fields (`account`/`name`/`engName`) via the local `users` `$lookup`; cross-site counterparts leave `HRInfo` nil. The service just returns what the store gives, then enriches the embedded `Subscription` with room info.

- [ ] **Step 1: Failing tests**:

```go
func TestGetDM_InvalidTarget(t *testing.T) {
	svc, _, _, _ := newSvc(t)
	for _, target := range []string{"p_system", "helper.bot"} {
		_, err := svc.GetDM(ctx("alice", "site-a"), models.GetDMRequest{AccountName: target})
		requireCode(t, err, errcode.CodeBadRequest)
	}
}

func TestGetDM_Empty(t *testing.T) {
	svc, _, _, _ := newSvc(t)
	_, err := svc.GetDM(ctx("alice", "site-a"), models.GetDMRequest{AccountName: ""})
	requireCode(t, err, errcode.CodeBadRequest)
}

func TestGetDM_NotFound(t *testing.T) {
	svc, store, _, _ := newSvc(t)
	store.EXPECT().GetDMSubscription(gomock.Any(), "alice", "bob").Return(nil, nil)
	_, err := svc.GetDM(ctx("alice", "site-a"), models.GetDMRequest{AccountName: "bob"})
	requireCode(t, err, errcode.CodeNotFound)
}

func TestGetDM_OK(t *testing.T) {
	svc, store, _, _ := newSvc(t)
	store.EXPECT().GetDMSubscription(gomock.Any(), "alice", "bob").
		Return(&model.DMSubscription{
			Subscription: &model.Subscription{ID: "d1"},
			HRInfo:       &model.SubscriptionHRInfo{Account: "bob", Name: "bob", EngName: "Bob"},
		}, nil)
	resp, err := svc.GetDM(ctx("alice", "site-a"), models.GetDMRequest{AccountName: "bob"})
	require.NoError(t, err)
	assert.Equal(t, "d1", resp.Subscription.ID)
	assert.Equal(t, "Bob", resp.Subscription.HRInfo.EngName)
}
```

> Confirm `model.DMSubscription`'s embed is `*Subscription` (verified) so `Subscription: &model.Subscription{…}` is correct and `resp.Subscription.ID` promotes through the pointer.

- [ ] **Step 2: Run** → FAIL. **Step 3: Implement** (add `"strings"` import):

```go
func (s *UserService) GetDM(c *natsrouter.Context, req models.GetDMRequest) (*models.DMResponse, error) {
	if req.AccountName == "" {
		return nil, errcode.BadRequest("accountName required")
	}
	if strings.HasPrefix(req.AccountName, "p_") || strings.HasSuffix(req.AccountName, ".bot") {
		return nil, errcode.BadRequest("invalid DM target", errcode.WithReason(errcode.UserInvalidDMTarget))
	}
	dm, err := s.store.GetDMSubscription(c, c.Param("account"), req.AccountName)
	if err != nil {
		return nil, fmt.Errorf("get dm: %w", err)
	}
	if dm == nil {
		return nil, errcode.NotFound("dm not found", errcode.WithReason(errcode.UserSubscriptionNotFound))
	}
	// Enrich the embedded subscription with room info (in-place via a 1-elem slice).
	one := []model.Subscription{*dm.Subscription}
	if err := s.enrichWithRoomInfo(c, one); err != nil {
		return nil, err
	}
	out := *dm
	out.Subscription = &one[0]
	return &models.DMResponse{Subscription: out}, nil
}
```

- [ ] **Step 4: Run** → PASS. **Step 5: Commit** `git commit -am "feat(user-service): subscription.getDM"`.

---

## Chapter 8: `enrichWithRoomInfo`

Goal: replace the Ch.6 passthrough with the real two-stage enrichment's **RPC stage** (stage 1, the local `rooms` `$lookup` deleted-filter + local-row `userCount`/`lastMsgId`/`lastMsgAt`, lives in `mongorepo`, Ch.11). This stage groups subs by `siteId`, calls `GetRoomsInfo` per site **in parallel with per-site degradation** (a failed site keeps its subs but unenriched — NOT a whole-request fallback, unlike `count`), and maps `model.RoomInfo` onto each sub: `name`, `lastMsgAt`, and the computed `alert`/`hasMention` (this repo's equivalents of the original `hasUnread`/`hasGroupMention`). `userCount`/`lastMsgId` are left as the `$lookup` set them (the RPC reply has neither).

**Files:** Modify `user-service/service/subscriptions.go`; Create `user-service/service/enrich_test.go`; Modify `user-service/service/subscriptions_test.go`.

### Task 8.1: implement enrichment + unread/applyRoomInfo helpers

- [ ] **Step 1: Failing tests** `enrich_test.go`:

```go
package service

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
)

func TestEnrichWithRoomInfo_LocalAndCrossSite(t *testing.T) {
	svc, _, rooms, _ := newSvc(t)
	seen := time.UnixMilli(100).UTC()
	newer := int64(200)
	subs := []model.Subscription{
		{ID: "a", RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen, UserCount: 5},
		{ID: "b", RoomID: "r2", SiteID: "site-b", LastSeenAt: &seen},
	}
	rooms.EXPECT().GetRoomsInfo(gomock.Any(), "site-a", []string{"r1"}).
		Return([]model.RoomInfo{{RoomID: "r1", Found: true, Name: "Eng", LastMsgAt: &newer}}, nil)
	rooms.EXPECT().GetRoomsInfo(gomock.Any(), "site-b", []string{"r2"}).
		Return([]model.RoomInfo{{RoomID: "r2", Found: true, Name: "Ops", LastMsgAt: &newer}}, nil)
	require.NoError(t, svc.enrichWithRoomInfo(ctx("alice", "site-a"), subs))
	assert.Equal(t, "Eng", subs[0].Name)
	assert.True(t, subs[0].Alert)         // lastMsgAt 200 > lastSeen 100
	assert.Equal(t, 5, subs[0].UserCount) // preserved from $lookup
	assert.Equal(t, "Ops", subs[1].Name)
	assert.True(t, subs[1].Alert)
}

func TestEnrichWithRoomInfo_NotFoundKeepsSub(t *testing.T) {
	svc, _, rooms, _ := newSvc(t)
	subs := []model.Subscription{{ID: "a", RoomID: "r1", SiteID: "site-a"}}
	rooms.EXPECT().GetRoomsInfo(gomock.Any(), "site-a", []string{"r1"}).
		Return([]model.RoomInfo{{RoomID: "r1", Found: false}}, nil)
	require.NoError(t, svc.enrichWithRoomInfo(ctx("alice", "site-a"), subs))
	require.Len(t, subs, 1)
	assert.Empty(t, subs[0].Name)
	assert.False(t, subs[0].Alert)
}

func TestEnrichWithRoomInfo_RPCFailDegradesSiteKeepsOthers(t *testing.T) {
	svc, _, rooms, _ := newSvc(t)
	seen := time.UnixMilli(100).UTC()
	newer := int64(200)
	subs := []model.Subscription{
		{ID: "a", RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen},
		{ID: "b", RoomID: "r2", SiteID: "site-b", LastSeenAt: &seen},
	}
	rooms.EXPECT().GetRoomsInfo(gomock.Any(), "site-a", []string{"r1"}).Return(nil, errors.New("down"))
	rooms.EXPECT().GetRoomsInfo(gomock.Any(), "site-b", []string{"r2"}).
		Return([]model.RoomInfo{{RoomID: "r2", Found: true, Name: "Ops", LastMsgAt: &newer}}, nil)
	require.NoError(t, svc.enrichWithRoomInfo(ctx("alice", "site-a"), subs))
	assert.Empty(t, subs[0].Name) // site-a degraded
	assert.False(t, subs[0].Alert)
	assert.Equal(t, "Ops", subs[1].Name) // site-b still enriched
	assert.True(t, subs[1].Alert)
}
```

- [ ] **Step 2: Run** `make test SERVICE=user-service` → enrich tests FAIL (passthrough does nothing). **Step 3: Implement** — replace the Ch.6 passthrough body in `subscriptions.go` and add the helpers (imports: add `sync`, `time`, `log/slog`):

```go
// enrichWithRoomInfo overwrites each sub's room-derived fields from room-service.
// Subs are grouped by siteId and queried in parallel; a per-site RPC failure
// leaves that site's subs unenriched (kept, not dropped) — this is NOT count's
// all-or-nothing fallback. info.Found==false likewise keeps the sub unenriched.
func (s *UserService) enrichWithRoomInfo(c *natsrouter.Context, subs []model.Subscription) error {
	if len(subs) == 0 {
		return nil
	}
	idxBySite := map[string][]int{}
	for i := range subs {
		idxBySite[subs[i].SiteID] = append(idxBySite[subs[i].SiteID], i)
	}
	sites := make([]string, 0, len(idxBySite))
	for site := range idxBySite {
		sites = append(sites, site)
	}
	infoBySite := make([]map[string]model.RoomInfo, len(sites)) // nil ⇒ site degraded
	var wg sync.WaitGroup
	for i, site := range sites {
		i, site := i, site
		wg.Add(1)
		go func() {
			defer wg.Done()
			roomIDs := make([]string, 0, len(idxBySite[site]))
			for _, j := range idxBySite[site] {
				roomIDs = append(roomIDs, subs[j].RoomID)
			}
			infos, err := s.rooms.GetRoomsInfo(c, site, roomIDs)
			if err != nil {
				slog.Warn("room-info enrichment degraded", "site", site, "error", err)
				return
			}
			m := make(map[string]model.RoomInfo, len(infos))
			for _, in := range infos {
				m[in.RoomID] = in
			}
			infoBySite[i] = m
		}()
	}
	wg.Wait()
	for i, site := range sites {
		m := infoBySite[i]
		if m == nil {
			continue
		}
		for _, j := range idxBySite[site] {
			applyRoomInfo(&subs[j], m[subs[j].RoomID])
		}
	}
	return nil
}

// applyRoomInfo overwrites name/lastMsgAt and computes alert/hasMention.
// info is the zero value (Found=false) for rooms the RPC didn't return — skipped.
func applyRoomInfo(sub *model.Subscription, info model.RoomInfo) {
	if !info.Found {
		return
	}
	if info.Name != "" {
		sub.Name = info.Name
	}
	if info.LastMsgAt != nil {
		t := time.UnixMilli(*info.LastMsgAt).UTC()
		sub.LastMsgAt = &t
	}
	sub.Alert = unread(sub.LastSeenAt, info.LastMsgAt)
	sub.HasMention = unread(sub.LastSeenAt, info.LastMentionAllAt)
}

// unread reports whether a room event at ms (epoch millis) is newer than lastSeen.
// Shared with CountSubscriptions (Ch.9). nil ms ⇒ not unread; nil lastSeen + ms ⇒ unread.
func unread(lastSeen *time.Time, ms *int64) bool {
	if ms == nil {
		return false
	}
	if lastSeen == nil {
		return true
	}
	return lastSeen.UTC().UnixMilli() < *ms
}
```

> Concurrency note: goroutines write only their own `infoBySite[i]` and read `subs` read-only; the apply loop runs after `wg.Wait()` on the main goroutine — race-free under `-race`. `sync.WaitGroup` (not `errgroup`) is deliberate: per-site degradation must not cancel sibling sites.

- [ ] **Step 4: Run** `make test SERVICE=user-service` → enrich tests PASS, but the Ch.6/7 happy-path tests now FAIL (real enrichment calls `GetRoomsInfo`, which has no expectation). Proceed to Step 5.
- [ ] **Step 5: Update Ch.6/7 tests** in `subscriptions_test.go` to register the now-expected RPC. For each happy-path test that returns subs from the store (`TestListSubscriptions_Types`, `TestGetChannels_ByMembersContain`, `TestGetChannels_ByAccountNames`, `TestGetDM_OK`), capture the rooms mock (`svc, store, rooms, _ := newSvc(t)`) and add:

```go
rooms.EXPECT().GetRoomsInfo(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
```

(Returning `(nil, nil)` ⇒ empty info map ⇒ `applyRoomInfo` sees `Found=false` ⇒ subs pass through unchanged, so the `Total`/ID assertions still hold.) The validation/not-found tests reach no store/enrichment call and need no change.

- [ ] **Step 6: Run** `make test SERVICE=user-service` (with `-race`) → PASS, no races. **Step 7: Commit** `git commit -am "feat(user-service): room-info enrichment for subscription replies"`.

---

## Chapter 9: `subscription.count`

**Files:** Modify `user-service/service/subscriptions.go`; Test `user-service/service/subscriptions_test.go`. Reuses the `unread` helper from Ch.8.

> Unlike enrichment's per-site degradation, `count` uses **all-or-nothing fallback to total** on any RPC error (gift guide §3.3 / Critical Detail #4) — `errgroup` (fail-fast) is correct here.

### Task 9.1: total

- [ ] **Step 1: Failing test**:

```go
func TestCount_Total(t *testing.T) {
	svc, store, _, _ := newSvc(t)
	store.EXPECT().CountActiveSubscriptions(gomock.Any(), "alice").Return(7, nil)
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{})
	require.NoError(t, err)
	assert.Equal(t, 7, resp.Count)
}
```

- [ ] **Step 2: Run** → FAIL. **Step 3: Implement** (temp `countUnread` returns total):

```go
func (s *UserService) CountSubscriptions(c *natsrouter.Context, req models.CountRequest) (*models.CountResponse, error) {
	account := c.Param("account")
	total, err := s.store.CountActiveSubscriptions(c, account)
	if err != nil {
		return nil, fmt.Errorf("count subscriptions: %w", err)
	}
	if req.Unread == nil || !*req.Unread {
		return &models.CountResponse{Count: total}, nil
	}
	return s.countUnread(c, account, total)
}

func (s *UserService) countUnread(ctx context.Context, account string, total int) (*models.CountResponse, error) {
	return &models.CountResponse{Count: total}, nil // replaced in Task 9.2
}
```

(Add `"context"` import if not already present.)

- [ ] **Step 4: Run** → PASS. **Step 5: Commit** `git commit -am "feat(user-service): subscription.count total"`.

### Task 9.2: unread (parallel, race-free, lossy fallback)

- [ ] **Step 1: Failing tests**:

```go
func TestCountUnread_Happy(t *testing.T) {
	svc, store, rooms, _ := newSvc(t)
	seen := time.UnixMilli(100).UTC()
	newer := int64(200)
	store.EXPECT().CountActiveSubscriptions(gomock.Any(), "alice").Return(2, nil)
	store.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", 1000).
		Return([]model.Subscription{{RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen}}, nil)
	rooms.EXPECT().GetRoomsInfo(gomock.Any(), "site-a", []string{"r1"}).
		Return([]model.RoomInfo{{RoomID: "r1", Found: true, LastMsgAt: &newer}}, nil)
	yes := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &yes})
	require.NoError(t, err)
	assert.Equal(t, 1, resp.Count)
}

func TestCountUnread_FallbackToTotal(t *testing.T) {
	svc, store, rooms, _ := newSvc(t)
	store.EXPECT().CountActiveSubscriptions(gomock.Any(), "alice").Return(5, nil)
	store.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", 1000).
		Return([]model.Subscription{{RoomID: "r1", SiteID: "site-a"}}, nil)
	rooms.EXPECT().GetRoomsInfo(gomock.Any(), "site-a", gomock.Any()).Return(nil, errors.New("down"))
	yes := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &yes})
	require.NoError(t, err)
	assert.Equal(t, 5, resp.Count) // fell back to total
}
```

(Add `"errors"`, `"time"` to test imports if not present.)

- [ ] **Step 2: Run** → FAIL. **Step 3: Implement** (replace `countUnread`; add `golang.org/x/sync/errgroup` import; reuse `unread` from Ch.8):

```go
func (s *UserService) countUnread(ctx context.Context, account string, total int) (*models.CountResponse, error) {
	subs, err := s.store.GetActiveSubscriptions(ctx, account, s.maxSubs)
	if err != nil {
		return nil, fmt.Errorf("count unread: %w", err)
	}
	bySite := map[string][]model.Subscription{}
	for _, sub := range subs {
		bySite[sub.SiteID] = append(bySite[sub.SiteID], sub)
	}
	sites := make([]string, 0, len(bySite))
	for site := range bySite {
		sites = append(sites, site)
	}
	results := make([]int, len(sites))
	g, gctx := errgroup.WithContext(ctx)
	for i, site := range sites {
		i, site := i, site
		g.Go(func() error {
			roomIDs := make([]string, 0, len(bySite[site]))
			for _, sub := range bySite[site] {
				roomIDs = append(roomIDs, sub.RoomID)
			}
			infos, err := s.rooms.GetRoomsInfo(gctx, site, roomIDs)
			if err != nil {
				return err
			}
			lastMsg := make(map[string]*int64, len(infos))
			for _, in := range infos {
				lastMsg[in.RoomID] = in.LastMsgAt
			}
			n := 0
			for _, sub := range bySite[site] {
				if unread(sub.LastSeenAt, lastMsg[sub.RoomID]) {
					n++
				}
			}
			results[i] = n
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		slog.Warn("unread count fell back to total", "error", err)
		return &models.CountResponse{Count: total}, nil
	}
	unreadTotal := 0
	for _, n := range results {
		unreadTotal += n
	}
	return &models.CountResponse{Count: unreadTotal}, nil
}
```

- [ ] **Step 4: Run** `make test SERVICE=user-service` (with `-race`) → PASS, no races. **Step 5: Commit** `git commit -am "feat(user-service): unread count + lossy fallback"`.

---

## Chapter 10: `setAppSubscription` + `apps.list`

**Files:** Modify `user-service/service/apps.go`; Test `user-service/service/apps_test.go`.

### Task 10.1: `SetAppSubscription` (PUT-like)

> `subscribed=true`: no existing botDM sub ⇒ `CreateDMRoom`; existing ⇒ `SetAppSubscribed(true, false)` (always clears `muted`). `subscribed=false` ⇒ `SetAppSubscribed(false, true)`. `app.Assistant` nil or `Enabled==false` ⇒ disabled.

- [ ] **Step 1: Failing tests** `apps_test.go`:

```go
package service

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/user-service/models"
)

func appWith(enabled bool) *model.App {
	return &model.App{ID: "app1", Name: "Helper", Assistant: &model.AppAssistant{Enabled: enabled, Name: "helper.bot"}}
}

func TestSetApp_NotFound(t *testing.T) {
	svc, store, _, _ := newSvc(t)
	store.EXPECT().GetApp(gomock.Any(), "nope").Return(nil, nil)
	_, err := svc.SetAppSubscription(ctx("alice", "site-a"), models.SetAppSubscriptionRequest{AppID: "nope", Subscribed: true})
	requireCode(t, err, errcode.CodeNotFound)
}

func TestSetApp_Disabled(t *testing.T) {
	svc, store, _, _ := newSvc(t)
	store.EXPECT().GetApp(gomock.Any(), "app1").Return(appWith(false), nil)
	_, err := svc.SetAppSubscription(ctx("alice", "site-a"), models.SetAppSubscriptionRequest{AppID: "app1", Subscribed: true})
	requireCode(t, err, errcode.CodeBadRequest)
}

func TestSetApp_SubscribeNew(t *testing.T) {
	svc, store, rooms, _ := newSvc(t)
	store.EXPECT().GetApp(gomock.Any(), "app1").Return(appWith(true), nil)
	store.EXPECT().GetAppSubscription(gomock.Any(), "alice", "helper.bot").Return(nil, nil)
	rooms.EXPECT().CreateDMRoom(gomock.Any(), "alice", "helper.bot", model.RoomTypeBotDM).Return(model.Subscription{ID: "new"}, nil)
	resp, err := svc.SetAppSubscription(ctx("alice", "site-a"), models.SetAppSubscriptionRequest{AppID: "app1", Subscribed: true})
	require.NoError(t, err)
	assert.True(t, resp.Success)
}

func TestSetApp_Reactivate_ClearsMuted(t *testing.T) {
	svc, store, _, _ := newSvc(t)
	store.EXPECT().GetApp(gomock.Any(), "app1").Return(appWith(true), nil)
	store.EXPECT().GetAppSubscription(gomock.Any(), "alice", "helper.bot").Return(&model.Subscription{ID: "ex", Muted: true}, nil)
	store.EXPECT().SetAppSubscribed(gomock.Any(), "alice", "helper.bot", true, false).Return(nil)
	_, err := svc.SetAppSubscription(ctx("alice", "site-a"), models.SetAppSubscriptionRequest{AppID: "app1", Subscribed: true})
	require.NoError(t, err)
}

func TestSetApp_Unsubscribe(t *testing.T) {
	svc, store, _, _ := newSvc(t)
	store.EXPECT().GetApp(gomock.Any(), "app1").Return(appWith(true), nil)
	store.EXPECT().SetAppSubscribed(gomock.Any(), "alice", "helper.bot", false, true).Return(nil)
	_, err := svc.SetAppSubscription(ctx("alice", "site-a"), models.SetAppSubscriptionRequest{AppID: "app1", Subscribed: false})
	require.NoError(t, err)
}
```

- [ ] **Step 2: Run** → FAIL. **Step 3: Implement** `apps.go` (imports: `fmt`, `pkg/errcode`, `pkg/model`, `pkg/natsrouter`, `user-service/models`):

```go
func (s *UserService) SetAppSubscription(c *natsrouter.Context, req models.SetAppSubscriptionRequest) (*models.OKResponse, error) {
	if req.AppID == "" {
		return nil, errcode.BadRequest("appId required")
	}
	account := c.Param("account")
	app, err := s.store.GetApp(c, req.AppID)
	if err != nil {
		return nil, fmt.Errorf("set app subscription: %w", err)
	}
	if app == nil {
		return nil, errcode.NotFound("app not found", errcode.WithReason(errcode.UserAppNotFound))
	}
	if app.Assistant == nil || !app.Assistant.Enabled {
		return nil, errcode.BadRequest("app has no enabled assistant", errcode.WithReason(errcode.UserAppDisabled))
	}
	botName := app.Assistant.Name

	if !req.Subscribed {
		if err := s.store.SetAppSubscribed(c, account, botName, false, true); err != nil {
			return nil, fmt.Errorf("unsubscribe app: %w", err)
		}
		return &models.OKResponse{Success: true}, nil
	}
	existing, err := s.store.GetAppSubscription(c, account, botName)
	if err != nil {
		return nil, fmt.Errorf("get app subscription: %w", err)
	}
	if existing == nil {
		if _, err := s.rooms.CreateDMRoom(c, account, botName, model.RoomTypeBotDM); err != nil {
			return nil, fmt.Errorf("create botDM room: %w", err)
		}
		return &models.OKResponse{Success: true}, nil
	}
	if err := s.store.SetAppSubscribed(c, account, botName, true, false); err != nil {
		return nil, fmt.Errorf("reactivate app: %w", err)
	}
	return &models.OKResponse{Success: true}, nil
}
```

- [ ] **Step 4: Run** → PASS. **Step 5: Commit** `git commit -am "feat(user-service): subscription.setAppSubscription"`.

### Task 10.2: `AppsList`

- [ ] **Step 1: Failing test**:

```go
func TestAppsList(t *testing.T) {
	svc, store, _, _ := newSvc(t)
	store.EXPECT().ListApps(gomock.Any(), "alice").Return([]models.AppListItem{
		{App: model.App{ID: "a1"}, IsSubscribed: true},
		{App: model.App{ID: "a2"}},
	}, nil)
	resp, err := svc.AppsList(ctx("alice", "site-a"))
	require.NoError(t, err)
	assert.Equal(t, 2, resp.Total)
	assert.True(t, resp.Apps[0].IsSubscribed)
}
```

- [ ] **Step 2: Run** → FAIL. **Step 3: Implement**:

```go
func (s *UserService) AppsList(c *natsrouter.Context) (*models.AppsListResponse, error) {
	apps, err := s.store.ListApps(c, c.Param("account"))
	if err != nil {
		return nil, fmt.Errorf("list apps: %w", err)
	}
	return &models.AppsListResponse{Apps: apps, Total: len(apps)}, nil
}
```

- [ ] **Step 4: Run** `make test SERVICE=user-service && make lint`; check service coverage `go test ./user-service/service/ -coverprofile=/tmp/c.out && go tool cover -func=/tmp/c.out | tail -1` ≥ 80% (target 90%+). **Step 5: Commit** `git commit -am "feat(user-service): apps.list"`.

---

## Chapter 11: `mongorepo` package

Goal: `mongorepo.Store` implementing `service.UserStore` over `pkg/mongoutil`, **one source file per collection** (+ its own `_test.go`), proven by testcontainer integration tests. Holds **four** collections (users/subscriptions/apps + read-only `rooms`). All user reads filter `active: true`. Each integration test seeds Mongo, calls the method, asserts the result — TDD: write test → FAIL → implement → PASS → commit. Needs Docker.

> Pipeline correctness is validated against a real Mongo via the integration tests, which are the gate — the pipeline snippets below are the intended shape; adjust field paths to what the assertions require. Reference the gift guide §3.3 (`getCurrent` facet, lines 194-211) and §3.4 (`apps.list`).

### Task 11.1: Store + New (4 collections) + EnsureIndexes + integration harness

**Files:** Create `user-service/mongorepo/store.go`, `user-service/mongorepo/main_test.go`, `user-service/mongorepo/setup_test.go`.

- [ ] **Step 1: Write the harness** (mirrors `history-service/internal/mongorepo`). `main_test.go`:

```go
//go:build integration

package mongorepo

import (
	"testing"

	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }
```

`setup_test.go`:

```go
//go:build integration

package mongorepo

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/testutil"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	db := testutil.MongoDB(t, "user-service")
	s := New(db, "site-a") // local site for the deleted-filter; seed local rows with siteId "site-a", cross-site with another
	require.NoError(t, s.EnsureIndexes(context.Background()))
	return s
}

// seed inserts raw docs into a collection on the store's database.
func seed(t *testing.T, s *Store, coll string, docs ...any) {
	t.Helper()
	_, err := s.db().Collection(coll).InsertMany(context.Background(), docs)
	require.NoError(t, err)
}
```

- [ ] **Step 2: Write the failing index test** `store_test.go`:

```go
//go:build integration

package mongorepo

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestEnsureIndexes_Integration(t *testing.T) {
	s := newTestStore(t)
	cur, err := s.subscriptions.Raw().Indexes().List(context.Background())
	require.NoError(t, err)
	var idx []bson.M
	require.NoError(t, cur.All(context.Background(), &idx))
	require.GreaterOrEqual(t, len(idx), 4) // _id + 3 declared
}
```

- [ ] **Step 3: Run** `make test-integration SERVICE=user-service` → FAIL (compile: `New`/`Store` missing).
- [ ] **Step 4: Implement** `store.go`:

```go
package mongorepo

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
)

type Store struct {
	users         *mongoutil.Collection[model.User]
	subscriptions *mongoutil.Collection[model.Subscription]
	apps          *mongoutil.Collection[model.App]
	rooms         *mongoutil.Collection[model.Room] // read-only: $lookup target for deleted-filter + local enrichment
	siteID        string                            // this instance's site — distinguishes local vs cross-site rows in the deleted-filter
}

func New(db *mongo.Database, siteID string) *Store {
	return &Store{
		users:         mongoutil.NewCollection[model.User](db.Collection("users")),
		subscriptions: mongoutil.NewCollection[model.Subscription](db.Collection("subscriptions")),
		apps:          mongoutil.NewCollection[model.App](db.Collection("apps")),
		rooms:         mongoutil.NewCollection[model.Room](db.Collection("rooms")),
		siteID:        siteID,
	}
}

func (s *Store) db() *mongo.Database { return s.subscriptions.Raw().Database() }

func (s *Store) EnsureIndexes(ctx context.Context) error {
	if _, err := s.subscriptions.Raw().Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "u.account", Value: 1}, {Key: "roomType", Value: 1}}},
		{Keys: bson.D{{Key: "roomId", Value: 1}, {Key: "u.account", Value: 1}}},
		{Keys: bson.D{{Key: "name", Value: 1}, {Key: "roomType", Value: 1}}},
	}); err != nil {
		return fmt.Errorf("create subscription indexes: %w", err)
	}
	if _, err := s.users.Raw().Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "account", Value: 1}}, Options: options.Index().SetUnique(true),
	}); err != nil {
		return fmt.Errorf("create user index: %w", err)
	}
	return nil // rooms indexes are owned by room-service, not created here
}
```

- [ ] **Step 5: Run** → PASS. **Step 6: Commit** `git commit -am "feat(user-service): mongorepo store + indexes + harness"`.

### Task 11.2: `users.go` — GetUserStatus, SetUserStatus

**Files:** Create `user-service/mongorepo/users.go`, `user-service/mongorepo/users_test.go`.

- [ ] **Step 1: Failing integration tests** (`//go:build integration`): seed a `users` doc `{_id, account:"bob", active:true, statusText:"hi", engName:"Bob"}` and an `active:false` doc; assert `GetUserStatus("bob")` returns it, `GetUserStatus(inactive)` returns `(nil,nil)`, and after `SetUserStatus("bob","busy",&true)` a re-read shows `statusText:"busy"`.
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement** `users.go`:

```go
package mongorepo

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/model"
)

func (s *Store) GetUserStatus(ctx context.Context, account string) (*model.User, error) {
	return s.users.FindOne(ctx, bson.M{"account": account, "active": true})
}

func (s *Store) SetUserStatus(ctx context.Context, account, text string, isShow *bool) error {
	set := bson.M{"statusText": text}
	if isShow != nil {
		set["statusIsShow"] = *isShow
	}
	if _, err := s.users.Raw().UpdateOne(ctx,
		bson.M{"account": account, "active": true},
		bson.M{"$set": set},
	); err != nil {
		return fmt.Errorf("update user status: %w", err)
	}
	return nil
}
```

- [ ] **Step 4: Run** → PASS. **Step 5: Commit** `git commit -am "feat(user-service): mongorepo users queries"`.

### Task 11.3: `subscriptions.go` — list/channels/dm/count/active/app queries

**Files:** Create `user-service/mongorepo/subscriptions.go`, `user-service/mongorepo/subscriptions_test.go`. Pipelines inline.

A shared helper builds the **rooms-join + deleted-filter + local enrichment** tail appended to every subscription pipeline (cross-site rows have no local `rooms` doc and survive; local rooms renamed `Del-*` are dropped):

```go
// roomsEnrichStages: $lookup local rooms, then DROP a row only when it is a
// LOCAL sub (siteId == localSiteID) whose room is missing OR soft-deleted
// (name ^Del-). CROSS-SITE rows (siteId != localSiteID) have no local room doc
// by design and are always kept (RPC enriches them later). Local rows get
// userCount/lastMsgAt/lastMsgId copied from the joined room.
func roomsEnrichStages(localSiteID string) bson.A {
	return bson.A{
		bson.M{"$lookup": bson.M{"from": "rooms", "localField": "roomId", "foreignField": "_id", "as": "room"}},
		bson.M{"$unwind": bson.M{"path": "$room", "preserveNullAndEmptyArrays": true}},
		bson.M{"$match": bson.M{"$or": bson.A{
			bson.M{"siteId": bson.M{"$ne": localSiteID}}, // cross-site: keep regardless
			bson.M{"$and": bson.A{ // local: room must exist AND not be Del-prefixed
				bson.M{"room": bson.M{"$ne": nil}},
				bson.M{"room.name": bson.M{"$not": bson.M{"$regex": "^Del-"}}},
			}},
		}}},
		bson.M{"$addFields": bson.M{
			"userCount": "$room.userCount",
			"lastMsgAt": "$room.lastMsgAt",
			"lastMsgId": "$room.lastMsgId",
		}},
		bson.M{"$project": bson.M{"room": 0}},
	}
}
```

- [ ] **Step 1: Write failing integration tests** (one `t.Run` per method) seeding `subscriptions`/`rooms`/`users`/`apps`, asserting:
  - **`AggregateSubscriptions`**: `current` returns dm+channel+botDM (botDM only when `isSubscribed:true`); `rooms` returns dm+channel; `apps` returns subscribed botDMs. Deleted-filter cases (all with `siteId=="site-a"`, the store's local site): a **local** sub whose room is named `Del-Eng` is **dropped**; a **local** sub whose `roomId` has **no** doc in `rooms` is **dropped** (missing local room ⇒ deleted); a **cross-site** sub (`siteId=="site-b"`, no doc in local `rooms`) is **kept** with empty `userCount`. Local rows get `userCount`/`lastMsgAt`/`lastMsgId` from the joined room. `favorite` sorts before non-favorite, then by `name`. `withinDays` non-nil drops subs whose `_updatedAt` is older than the cutoff (rooms/current only); nil ⇒ no age filter. `limit` caps the result.
  - **`FindChannelsByMembers`**: seeds two channels; `["carol"]` returns channels containing carol; `["carol","dave"]` returns only channels containing BOTH; bots excluded; sorted `createdAt` DESC.
  - **`GetDMSubscription`**: returns the dm sub with `HRInfo{Account,Name,EngName}` populated from the local `users` join; a **cross-site** counterpart (no local user) yields `HRInfo == nil`; miss yields `(nil,nil)`.
  - **`CountActiveSubscriptions`**: counts the exact `$or` (dm/channel muted≠true) OR (botDM muted≠true, isSubscribed:true); muted excluded.
  - **`GetActiveSubscriptions`**: returns that same active set (used by unread count), capped by limit.
  - **`GetAppSubscription`** / **`SetAppSubscribed`**: round-trip a botDM sub's `isSubscribed`/`muted`.
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement** `subscriptions.go`. Key shapes:

```go
// AggregateSubscriptions: rooms/apps are a single matching branch; current
// delegates to aggregateCurrent (the $facet path). Every path runs the shared
// roomsEnrichStages(s.siteID) deleted-filter, then $sort{favorite:-1,name:1}+$limit.
func (s *Store) AggregateSubscriptions(ctx context.Context, account, listType string, withinDays *int, limit int) ([]model.Subscription, error) {
	if listType == "current" {
		return s.aggregateCurrent(ctx, account, withinDays, limit)
	}
	match := bson.M{"u.account": account, "muted": bson.M{"$ne": true}}
	switch listType {
	case "rooms":
		match["roomType"] = bson.M{"$in": bson.A{"dm", "channel"}}
		if withinDays != nil {
			match["_updatedAt"] = bson.M{"$gte": time.Now().UTC().AddDate(0, 0, -*withinDays)}
		}
	case "apps":
		match["roomType"] = "botDM"
		match["isSubscribed"] = true
	}
	pipeline := bson.A{bson.M{"$match": match}}
	pipeline = append(pipeline, roomsEnrichStages(s.siteID)...)
	pipeline = append(pipeline,
		bson.M{"$sort": bson.D{{Key: "favorite", Value: -1}, {Key: "name", Value: 1}}},
		bson.M{"$limit": int64(limit)},
	)
	return s.subscriptions.Aggregate(ctx, pipeline)
}

// aggregateCurrent merges the rooms branch (dm/channel, joined to users) and the
// apps branch (botDM, joined to apps) via $facet + $concatArrays — each branch
// needs a DIFFERENT per-type $lookup, so a single $match cannot serve both (gift
// guide getCurrent, lines 194-211). Shared deleted-filter runs before the facet;
// the per-branch user/app joins are retained for parity with the original (a
// richer client shape consumes them) and projected off the typed []Subscription
// result — the integration test asserts the merged set/ordering, not the joins.
func (s *Store) aggregateCurrent(ctx context.Context, account string, withinDays *int, limit int) ([]model.Subscription, error) {
	match := bson.M{"u.account": account, "$or": bson.A{
		bson.M{"roomType": bson.M{"$in": bson.A{"dm", "channel"}}, "muted": bson.M{"$ne": true}},
		bson.M{"roomType": "botDM", "muted": bson.M{"$ne": true}, "isSubscribed": true},
	}}
	if withinDays != nil {
		match["_updatedAt"] = bson.M{"$gte": time.Now().UTC().AddDate(0, 0, -*withinDays)}
	}
	pipeline := bson.A{bson.M{"$match": match}}
	pipeline = append(pipeline, roomsEnrichStages(s.siteID)...)
	pipeline = append(pipeline,
		bson.M{"$facet": bson.M{
			"rooms": bson.A{
				bson.M{"$match": bson.M{"roomType": bson.M{"$in": bson.A{"dm", "channel"}}}},
				bson.M{"$lookup": bson.M{"from": "users", "localField": "name", "foreignField": "account", "as": "user"}},
			},
			"apps": bson.A{
				bson.M{"$match": bson.M{"roomType": "botDM"}},
				bson.M{"$lookup": bson.M{"from": "apps", "localField": "name", "foreignField": "assistant.name", "as": "app"}},
			},
		}},
		bson.M{"$project": bson.M{"all": bson.M{"$concatArrays": bson.A{"$rooms", "$apps"}}}},
		bson.M{"$unwind": "$all"},
		bson.M{"$replaceRoot": bson.M{"newRoot": "$all"}},
		bson.M{"$project": bson.M{"user": 0, "app": 0}},
		bson.M{"$sort": bson.D{{Key: "favorite", Value: -1}, {Key: "name", Value: 1}}},
		bson.M{"$limit": int64(limit)},
	)
	return s.subscriptions.Aggregate(ctx, pipeline)
}
```

> Remaining methods (all append `roomsEnrichStages(s.siteID)` where they return subscription bodies, so the deleted-filter is consistent): **`FindChannelsByMembers`** does the `$group`/`$all` intersection on channel subs, then `roomsEnrichStages(s.siteID)` + `$sort{createdAt:-1}` (the `createdAt` sort key comes from the joined room — keep the room field until after the sort, or sort on the sub's own `_updatedAt` if `createdAt` isn't on the sub; the integration test pins ordering). **`GetDMSubscription`** matches `{u.account, name:target, roomType:"dm"}`, runs `roomsEnrichStages(s.siteID)`, then a `users` `$lookup` (localField `name` → `users.account`) + `$addFields hrInfo` (`account`/`name`/`engName`; `$$REMOVE` when the join is empty so cross-site counterparts decode to `HRInfo==nil`); decode via `s.subscriptions.Raw().Aggregate` → `cur.All(&[]model.DMSubscription)`, return first or `(nil,nil)`. **Confirm the source of `hrInfo.name`** (login account vs a display field) against the gift/original — the integration test pins it. **`CountActiveSubscriptions`** uses `s.subscriptions.Raw().CountDocuments` with the `$or` (no rooms join — it's a count); **`GetActiveSubscriptions`** uses `FindMany` with the same `$or` + `WithLimit(int64(limit))` (note: `WithLimit` takes `int64`, `pkg/mongoutil/options.go:48`); **`SetAppSubscribed`** uses `Raw().UpdateOne({u.account,name:botName,roomType:botDM},{$set:{isSubscribed,muted}})`; **`GetAppSubscription`** uses `FindOne({u.account,name:botName,roomType:botDM})`. Add `"time"` import.

- [ ] **Step 4: Run** `make test-integration SERVICE=user-service` → PASS (commit per method group as they go green). **Step 5: Final commit** `git commit -am "feat(user-service): mongorepo subscription queries (rooms join, Del- filter, facet, enrichment)"`.

### Task 11.4: `apps.go` — GetApp, ListApps

**Files:** Create `user-service/mongorepo/apps.go`, `user-service/mongorepo/apps_test.go`.

- [ ] **Step 1: Failing integration tests**: seed two `apps` (`assistant.name` = `helper.bot`/`other.bot`) and a subscribed botDM sub for `helper.bot`; assert `GetApp(id)` returns the app / `(nil,nil)` on miss; `ListApps("alice")` returns both apps sorted by `name`, with `isSubscribed:true` for `helper.bot` only.
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement** `apps.go` (`ListApps` decodes the aggregation into `[]models.AppListItem` — `mongorepo` imports `user-service/models`; no cycle since `models` imports only `pkg/model`):

```go
package mongorepo

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/user-service/models"
)

func (s *Store) GetApp(ctx context.Context, appID string) (*model.App, error) {
	return s.apps.FindByID(ctx, appID)
}

func (s *Store) ListApps(ctx context.Context, account string) ([]models.AppListItem, error) {
	pipeline := bson.A{
		bson.M{"$lookup": bson.M{
			"from": "subscriptions",
			"let":  bson.M{"botName": "$assistant.name"},
			"pipeline": bson.A{bson.M{"$match": bson.M{"$expr": bson.M{"$and": bson.A{
				bson.M{"$eq": bson.A{"$u.account", account}},
				bson.M{"$eq": bson.A{"$name", "$$botName"}},
				bson.M{"$eq": bson.A{"$isSubscribed", true}},
			}}}}},
			"as": "sub",
		}},
		bson.M{"$addFields": bson.M{"isSubscribed": bson.M{"$gt": bson.A{bson.M{"$size": "$sub"}, 0}}}},
		bson.M{"$project": bson.M{"sub": 0}},
		bson.M{"$sort": bson.M{"name": 1}},
	}
	cur, err := s.apps.Raw().Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("list apps: %w", err)
	}
	var out []models.AppListItem
	if err := cur.All(ctx, &out); err != nil {
		return nil, fmt.Errorf("decode apps: %w", err)
	}
	return out, nil
}
```

- [ ] **Step 4: Run** `make test-integration SERVICE=user-service && make lint`. **Step 5: Commit** `git commit -am "feat(user-service): mongorepo apps queries"`.

---

## Chapter 12: `roomclient` package

**Files:** Create `user-service/roomclient/client.go`, `user-service/roomclient/client_integration_test.go`.

> `roomclient.Client` implements `service.RoomClient` via `nc.Request(ctx, subj, data, timeout)` on **server-scoped** subjects. `RoomsInfoBatch` is served by **room-service**; `RoomCreateDMSync` by **room-worker**. A non-OK reply is decoded with `errcode.Parse` and returned as-is. Confirm envelopes against `docs/superpowers/specs/2026-04-14-room-info-batch-rpc-design.md`. Needs Docker (NATS testcontainer).

### Task 12.1: `GetRoomsInfo`

- [ ] **Step 1: Failing integration test** — responder on `subject.RoomsInfoBatch("site-a")` returns rooms; assert decode:

```go
//go:build integration

package roomclient

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Marz32onE/instrumentation-go/otel-nats/otelnats"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

// dial returns the *otelnats.Conn type that New (and production) uses.
func dial(t *testing.T) *otelnats.Conn {
	t.Helper()
	nc, err := otelnats.Connect(testutil.NATS(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = nc.Drain() })
	return nc
}

func TestGetRoomsInfo_Integration(t *testing.T) {
	nc := dial(t)
	// otelnats.Subscribe handler is func(m otelnats.Msg); respond via m.Msg.Respond
	// (verified: message-gatekeeper/fetcher_history_test.go:61-63).
	sub, err := nc.Subscribe(subject.RoomsInfoBatch("site-a"), func(m otelnats.Msg) {
		out, _ := json.Marshal(model.RoomsInfoBatchResponse{Rooms: []model.RoomInfo{{RoomID: "r1", Found: true, Name: "Eng"}}})
		_ = m.Msg.Respond(out)
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	rooms, err := New(nc, "site-a").GetRoomsInfo(context.Background(), "site-a", []string{"r1"})
	require.NoError(t, err)
	require.Len(t, rooms, 1)
	require.Equal(t, "Eng", rooms[0].Name)
}
```

> **API (verified):** `otelnats.Connect(url) (*otelnats.Conn, error)`; `Subscribe(subj, func(m otelnats.Msg))` with reply via `m.Msg.Respond(...)` (`message-gatekeeper/fetcher_history_test.go:61-63`); `(*otelnats.Conn).Drain()`. `pkg/natsutil.Connect` also returns `*otelnats.Conn`. The Ch.12.2 (`CreateDMRoom`) and Ch.13 (publisher) integration tests use the same `dial` + `func(m otelnats.Msg)` responder pattern. Confirm `subject.RoomsInfoBatch`/`RoomCreateDMSync` builder names.

- [ ] **Step 2: Run** `make test-integration SERVICE=user-service` → FAIL. **Step 3: Implement** `client.go`:

```go
package roomclient

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Marz32onE/instrumentation-go/otel-nats/otelnats"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

// roomRPCTimeout bounds each room-service request/reply.
const roomRPCTimeout = 5 * time.Second

type Client struct {
	nc     *otelnats.Conn
	siteID string
}

func New(nc *otelnats.Conn, siteID string) *Client { return &Client{nc: nc, siteID: siteID} }

func (c *Client) GetRoomsInfo(ctx context.Context, siteID string, roomIDs []string) ([]model.RoomInfo, error) {
	req, err := json.Marshal(model.RoomsInfoBatchRequest{RoomIDs: roomIDs})
	if err != nil {
		return nil, fmt.Errorf("marshal rooms-info: %w", err)
	}
	msg, err := c.nc.Request(ctx, subject.RoomsInfoBatch(siteID), req, roomRPCTimeout)
	if err != nil {
		return nil, fmt.Errorf("rooms-info rpc: %w", err)
	}
	if e, ok := errcode.Parse(msg.Data); ok {
		return nil, e
	}
	var out model.RoomsInfoBatchResponse
	if err := json.Unmarshal(msg.Data, &out); err != nil {
		return nil, fmt.Errorf("decode rooms-info: %w", err)
	}
	return out.Rooms, nil
}
```

> Confirm `subject.RoomsInfoBatch` builder name + the `RoomsInfoBatchRequest`/`RoomsInfoBatchResponse` types (verified present in `pkg/model/room.go`). **API (verified):** `(*otelnats.Conn).Request(ctx, subject, data, timeout)` — 4 args incl. timeout, NOT `RequestWithContext` (real caller `message-gatekeeper/fetcher_history.go:53`).

- [ ] **Step 4: Run** → PASS. **Step 5: Commit** `git commit -am "feat(user-service): roomclient.GetRoomsInfo"`.

### Task 12.2: `CreateDMRoom`

- [ ] **Step 1: Failing integration test** — responder on `subject.RoomCreateDMSync("site-a")` returns `model.SyncCreateDMReply{Success:true, Subscription:{ID:"new"}}`; assert `CreateDMRoom` returns `.Subscription` with `ID=="new"`.
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement**:

```go
func (c *Client) CreateDMRoom(ctx context.Context, account, otherAccount string, roomType model.RoomType) (model.Subscription, error) {
	body, err := json.Marshal(model.SyncCreateDMRequest{RoomType: roomType, RequesterAccount: account, OtherAccount: otherAccount})
	if err != nil {
		return model.Subscription{}, fmt.Errorf("marshal create-dm: %w", err)
	}
	msg, err := c.nc.Request(ctx, subject.RoomCreateDMSync(c.siteID), body, roomRPCTimeout)
	if err != nil {
		return model.Subscription{}, fmt.Errorf("create-dm rpc: %w", err)
	}
	if e, ok := errcode.Parse(msg.Data); ok {
		return model.Subscription{}, e
	}
	var reply model.SyncCreateDMReply
	if err := json.Unmarshal(msg.Data, &reply); err != nil {
		return model.Subscription{}, fmt.Errorf("decode create-dm: %w", err)
	}
	return reply.Subscription, nil
}
```

> Confirm `subject.RoomCreateDMSync` builder name; `SyncCreateDMRequest`/`SyncCreateDMReply` fields verified in `pkg/model`.

- [ ] **Step 4: Run** `make test-integration SERVICE=user-service` → PASS. **Step 5: Commit** `git commit -am "feat(user-service): roomclient.CreateDMRoom"`.

---

## Chapter 13: `publisher` package

**Files:** Create `user-service/publisher/publisher.go`, `user-service/publisher/publisher_integration_test.go`.

> `publisher.Publisher` implements `service.EventPublisher` with **core NATS** (no JetStream — gift guide §2.5). It's a one-line transport wrapper, so it's proven by a NATS-testcontainer integration test (publish → receive) rather than a unit test that would need to fake the concrete `*otelnats.Conn`.

- [ ] **Step 1: Implement** `publisher.go`:

```go
package publisher

import (
	"context"

	"github.com/Marz32onE/instrumentation-go/otel-nats/otelnats"
)

type Publisher struct{ nc *otelnats.Conn }

func New(nc *otelnats.Conn) *Publisher { return &Publisher{nc: nc} }

func (p *Publisher) Publish(ctx context.Context, subject string, data []byte) error {
	return p.nc.Publish(ctx, subject, data)
}
```

> **API (verified):** `(*otelnats.Conn).Publish(ctx, subject, data) error` — takes the ctx (for trace propagation), `conn.go:284`. Not the bare `nats.Conn.Publish(subj, data)`.

- [ ] **Step 2: Failing integration test** `publisher_integration_test.go` (`//go:build integration`): `TestMain` → `testutil.RunTests(m)`; subscribe to a subject on the test NATS, `New(nc).Publish(ctx, subj, data)`, assert the message is received (use a buffered chan + short `select`/timeout). Mirror the `dial` pattern from Ch.12.
- [ ] **Step 3: Run** `make test-integration SERVICE=user-service` → FAIL then (after Step 1 already implemented) PASS. **Step 4: Build+lint** `go build ./user-service/publisher/ && make lint`. **Step 5: Commit** `git commit -am "feat(user-service): publisher (core NATS) + integration test"`.

---

## Chapter 14: `main.go` wiring + build

**Files:** Create `user-service/main.go`.

- [ ] **Step 1: Write** `main.go` (mirrors `history-service/cmd/main.go`; compile-time interface assertions live here):

```go
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/otelutil"
	"github.com/hmchangw/chat/pkg/shutdown"
	"github.com/hmchangw/chat/user-service/config"
	"github.com/hmchangw/chat/user-service/mongorepo"
	"github.com/hmchangw/chat/user-service/publisher"
	"github.com/hmchangw/chat/user-service/roomclient"
	"github.com/hmchangw/chat/user-service/service"
)

var (
	_ service.UserStore      = (*mongorepo.Store)(nil)
	_ service.RoomClient     = (*roomclient.Client)(nil)
	_ service.EventPublisher = (*publisher.Publisher)(nil)
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := config.Load()
	if err != nil {
		slog.Error("parse config", "error", err)
		os.Exit(1)
	}
	ctx := context.Background()

	tracerShutdown, err := otelutil.InitTracer(ctx, "user-service")
	if err != nil {
		slog.Error("init tracer failed", "error", err)
		os.Exit(1)
	}
	nc, err := natsutil.Connect(cfg.NATS.URL, cfg.NATS.CredsFile)
	if err != nil {
		slog.Error("nats connect failed", "error", err)
		os.Exit(1)
	}
	mongoClient, err := mongoutil.Connect(ctx, cfg.Mongo.URI, cfg.Mongo.Username, cfg.Mongo.Password)
	if err != nil {
		slog.Error("mongo connect failed", "error", err)
		os.Exit(1)
	}
	store := mongorepo.New(mongoClient.Database(cfg.Mongo.DB), cfg.SiteID)
	if err := store.EnsureIndexes(ctx); err != nil {
		slog.Error("ensure indexes failed", "error", err)
		os.Exit(1)
	}

	svc := service.New(store, roomclient.New(nc, cfg.SiteID), publisher.New(nc), &cfg)

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

> Confirm signatures against `history-service/cmd/main.go`: `natsutil.Connect` (return type/args), `mongoutil.Connect`/`Disconnect`, `otelutil.InitTracer`, `natsrouter.New`/`Use`/`Recovery`/`RequestID`/`Logging`/`Shutdown`, `shutdown.Wait`. Adjust to match exactly.

- [ ] **Step 2: Build** `make build SERVICE=user-service && go vet ./user-service/... && make lint` → PASS, `0 issues`.
- [ ] **Step 3: Commit** `git commit -am "feat(user-service): main wiring"`.

---

## Chapter 15: Deploy, docs, remove `mock-user-service`

### Task 15.1: deploy artifacts

**Files:** Create `user-service/deploy/{Dockerfile, docker-compose.yml, azure-pipelines.yml}`.

- [ ] **Step 1:** Base on `room-service/deploy/*` (flat NATS + Mongo, **no JetStream / no Cassandra**). Multi-stage `golang:1.25.11-alpine` → `alpine:3.21`, build context repo root, build target `./user-service`, service name `user-service`, expose nothing HTTP (NATS-only — no `/healthz` route; if room-service's compose has a healthcheck, use a NATS-based or process check). Keep env `MONGO_*`, `NATS_*`, `SITE_ID`, `ALL_SITE_IDS`, `MAX_SUBSCRIPTION_LIMIT`; drop room-specific vars and any `BOOTSTRAP_*` (this service touches no JetStream stream).
- [ ] **Step 2:** `docker build -f user-service/deploy/Dockerfile -t user-service:dev .` → builds.
- [ ] **Step 3: Commit** `git commit -am "build(user-service): deploy artifacts"`.

### Task 15.2: docs (CLAUDE.md + client-api.md)

**Files:** Modify `CLAUDE.md`, `docs/client-api.md`.

- [ ] **Step 1:** Add `user-service` to CLAUDE.md's service descriptions (NATS request/reply, no JetStream; owns user status, subscriptions, apps).
- [ ] **Step 2:** Document the 8 endpoints in `docs/client-api.md` (§ matching the existing layout): request/response schemas, the new error reasons (`user_app_not_found`/`user_app_disabled`/`user_invalid_dm_target`/`user_subscription_not_found`), and these behaviors the frontend relies on:
  - `subscription.list`/`getChannels`/`getDM` replies are **room-info-enriched** — `name`, `lastMsgAt`, `alert` (has-unread), `hasMention` (group-mention) are computed/overwritten server-side; `userCount`/`lastMsgId` present for local rooms only (cross-site rows omit them). Deleted rooms (`Del-` prefix) are filtered out; rooms whose info RPC fails are returned unenriched.
  - `subscription.list` with `favorite=true` filters to favorites **and** moves the self-DM to the front.
  - `subscription.setAppSubscription` is PUT-like (`subscribed` end-state; subscribing clears `muted`).
  - The cross-site `UserStatusUpdated` outbox event.
  - The **removal** of `profile.getByName` and the employee endpoint, and the consolidation of `getCurrent`/`getRooms`/`getApps` → `subscription.list` and `subscribeApp`/`unsubscribeApp` → `subscription.setAppSubscription`.
- [ ] **Step 3: Commit** `git commit -am "docs(user-service): CLAUDE.md + client-api"`.

### Task 15.3: remove `mock-user-service`

- [ ] **Step 1:** `grep -rn "mock-user-service" --include=*.go . ; grep -rn "mock-user-service" docker-local/ docker-compose*.yml 2>/dev/null` → confirm only its own files + compose/pipeline refs remain (all real callers migrated in Ch.1.2).
- [ ] **Step 2:** `git rm -r mock-user-service/` and delete any `docker-local`/compose/pipeline references to it.
- [ ] **Step 3:** `go build ./... && make test && make lint` → PASS, `0 issues`.
- [ ] **Step 4: Commit** `git commit -am "chore: remove superseded mock-user-service"`.

---

## Chapter 16: Final verification

- [ ] `make generate SERVICE=user-service` — mocks current (no diff).
- [ ] `make test SERVICE=user-service` — unit tests pass with `-race`.
- [ ] `make test-integration SERVICE=user-service` — testcontainer integration (mongorepo, roomclient, publisher) passes.
- [ ] `make lint` — `0 issues`; `make sast` — no medium+ findings.
- [ ] **Coverage — applied per package class (the "≥80% every package" floor, read honestly):**
  - **Unit-testable logic packages** — `service` (target **90%+**) and `config` (≥80%): covered by `make test SERVICE=user-service` (no build tag). These MUST meet the floor on the plain unit run.

```bash
for pkg in service config; do
  go test -coverprofile=/tmp/c.out ./user-service/$pkg/ >/dev/null 2>&1 \
    && printf "%-10s " "$pkg" && go tool cover -func=/tmp/c.out | tail -1
done
```

  - **Integration-only packages** — `mongorepo`, `roomclient`, `publisher`: have no unit tests by design (they wrap Mongo/NATS), so `make test` legitimately reports them `0.0%`/untested — **that is expected, not a floor failure.** Their ≥80% is met under the integration build:

```bash
for pkg in mongorepo roomclient publisher; do
  go test -tags integration -coverprofile=/tmp/c.out ./user-service/$pkg/ >/dev/null 2>&1 \
    && printf "%-10s " "$pkg" && go tool cover -func=/tmp/c.out | tail -1
done
```

  - **Data-only packages** — `models` (and the `pkg/model` additions): pure struct declarations with **no executable statements**, so `go tool cover` prints `[no statements]` / `0.0%`. The round-trip `_test.go` files exist for marshal-correctness, but coverage is **N/A** here — do not treat `[no statements]` as a floor failure.

- [ ] **Spec parity sweep** — re-read `docs/superpowers/specs/2026-06-04-user-service-design.md` and confirm each section maps to a shipped task: 8 endpoints, natsrouter-only, models package, `enrichWithRoomInfo` (degrade-not-fallback, `Found==false` keeps sub, `Del-` filter, cross-site no `userCount`/`lastMsgId`), favorite filter, getDM 3-field HRInfo, `type=current` `$facet`, fire-and-forget outbox to all sites, the 3 `Subscription` fields, `mock-user-service` removed, docs updated.
- [ ] Final commit if any sweep fixes: `git commit -am "chore(user-service): final verification fixes"`.

## Chapter 17: Post-merge follow-ups (subscription.getByRoomID + request-id scope correction)

Changes layered on top of the original 8-endpoint service after it merged-in `origin/main` and reached CI-green. Each task is Red→Green→Refactor→commit per CLAUDE.md §4.

### 17a. New endpoint `subscription.getByRoomID` (brings the surface to 9)

- [x] `pkg/subject`: add `UserSubscriptionGetByRoomID(account, siteID)` (`chat.user.%s.request.user.%s.subscription.getByRoomID`) + `UserSubscriptionGetByRoomIDPattern(siteID)`; table-test both in `subject_test.go` (incl. wildcard-reject case).
- [x] `user-service/models`: `GetByRoomIDRequest{ RoomID string \`json:"roomId"\` }` + round-trip test. Response **reuses** `SubscriptionListResponse` (0-or-1 items).
- [x] `user-service/mongorepo`: `GetSubscriptionByRoomID(ctx, account, roomID) (*model.Subscription, error)` — `$match {u.account, roomId}` + `roomsEnrichStages(siteID)` (mirrors the list deleted-room filter; cross-site subs kept). Add to `UserStore` interface, `make generate` the mock, integration test (hit / cross-site / `^Del-` / not-subscribed / other-account).
- [x] `user-service/service`: handler `GetByRoomID` — empty `roomId` → `errcode.BadRequest("roomId required")` (inline, **not** a 404); not-subscribed → `{subscriptions: [], total: 0}`; hit → enriched 1-elem list. Register in `RegisterHandlers`. Unit tests for all paths.
- [x] Docs: `client-api.md` §3.4 `subscription.getByRoomID` subsection; bump `CLAUDE.md` / design spec / consolidation doc 8 → 9.

### 17b. Cross-service request-id propagation — **scope correction**

- [x] **Publisher (outbox, cross-site federation) — KEEP propagation.** `publisher.Publish` uses `nc.PublishMsg(ctx, natsutil.NewMsg(ctx, …))` so outbox events carry `X-Request-ID` (otelnats still injects trace on top). Integration test asserts the header arrives.
- [x] **Roomclient (user-service ⇄ room-service) — REVERT propagation.** `GetRoomsInfo` / `CreateDMRoom` return to plain `c.nc.Request(ctx, subj, data, roomRPCTimeout)` over otelnats (preserving the otel client span/trace on that hop); the `X-Request-ID`-carrying `natsutil.NewMsg` + raw `NatsConn().RequestMsgWithContext` is removed, along with the roomclient propagation test. Rationale: the room-service RPC must keep the otelnats client span, and that hop should not inherit the caller's request-id — room-service stamps its own per `natsrouter.RequestID()`.

### 17d. Review-round fixes (3 reviewers + CodeRabbit)

- [x] **Spec reviewer (critical):** `client-api.md` §3.4 header still read "exposes **8** … endpoints" — bumped to **9** to match the `subscription.getByRoomID` addition.
- [x] **CodeRabbit (major):** `CountActiveSubscriptions` / `GetActiveSubscriptions` only applied `activeSubscriptionFilter` (no room join), so `subscription.count` and the unread fallback counted stale local subs that the list/read endpoints hide. Converted both to the enriched pipeline (`$match activeSubscriptionFilter` + `roomsEnrichStages(siteID)` + `$count` / `$limit`); cross-site subs still kept. Integration test now seeds rooms + a `^Del-` room + a missing-room sub and asserts they are excluded from both the count and the active set. **Pre-existing inconsistency, not introduced by getByRoomID.**
- [x] **Bug reviewer (low):** softened the `GetSubscriptionByRoomID` doc comment that overstated `(account, roomId)` as DB-unique (it is not index-enforced; room-worker is the sole writer in practice).
- [-] **Arch reviewer (high/medium):** getDM-404 vs getByRoomID-empty-list and publisher-KEEP vs roomclient-REVERT request-id asymmetry are deliberate, documented choices (client-api.md "absence is a normal result" / Chapter 17b) — no code change.

### 17c. Verification

- [x] `make generate SERVICE=user-service` clean; `make build`/`make lint`/`make test -race`/`make test-integration SERVICE=user-service`/`make sast` all green.
- [x] 3-reviewer pass (arch / spec-compliance / bug) + CodeRabbit findings resolved; then `/branch_review`.

