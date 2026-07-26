# Open-Room Subscription Flag + `open` RPC Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a per-user `Subscription.open` boolean, an `open` RPC in room-service that sets it true (with cross-site federation), and exclude closed subscriptions from `subscription.list`.

**Architecture:** Mirror the existing `favorite.toggle` flow end-to-end, with two differences: the RPC is a set-true (not a toggle) with no ordering guard, and new subscriptions are born `open: true`. Origin write in room-service → best-effort `subscription.update` core event → cross-site mirror via OUTBOX (`outbox-worker`) → INBOX (`inbox-worker`).

**Tech Stack:** Go 1.25, NATS/JetStream, MongoDB (`mongo-driver/v2`), `natsrouter`, `go.uber.org/mock`, `testify`, testcontainers.

## Global Constraints

- Go 1.25. Use `make` targets only — never raw `go`. Key: `make test SERVICE=<name>`, `make generate SERVICE=<name>`, `make lint`.
- All NATS payloads are JSON with typed `pkg/model` structs — never `map[string]interface{}`.
- Every NATS event struct in `pkg/model` has `Timestamp int64 \`json:"timestamp" bson:"timestamp"\``, set at publish site via `time.Now().UTC().UnixMilli()`.
- Errors: wrap with context (`fmt.Errorf("desc: %w", err)`); client-facing errors via `errcode`/room-service sentinels; never bare `err`.
- Subjects: use `pkg/subject` builders, never raw `fmt.Sprintf` at call sites.
- TDD: Red → Green → Refactor → Commit. Tests in `package main` (services) / `package <pkg>` (libs). Mocks in `mock_store_test.go` via `make generate` — never hand-edit.
- Minimum 80% coverage; target 90% on new handler/store code.
- A client-facing handler (`chat.user.…`) change updates `docs/client-api.md` **and** derived views (`docs/client-api/request-reply.md`, `docs/client-api/events.md`) in the same PR.
- `make generate SERVICE=room-service` and `SERVICE=inbox-worker` after their store interfaces change, before testing.

---

### Task 1: `Subscription.open` model field + round-trip tests

**Files:**
- Modify: `pkg/model/subscription.go` (add `Open` field to `Subscription`, near `Favorite` at line 42)
- Test: `pkg/model/model_test.go` (extend `TestSubscriptionJSON` ~line 571 and `TestSubscriptionJSON_ThreadUnreadOmittedAlertAlwaysPresent` ~line 623)

**Interfaces:**
- Produces: `model.Subscription.Open bool` (json/bson tag `open`, always serialized).

- [ ] **Step 1: Write the failing test.** In `model_test.go`, in `TestSubscriptionJSON`'s constructed `Subscription`, add `Open: true` alongside `Favorite: true`. In `TestSubscriptionJSON_ThreadUnreadOmittedAlertAlwaysPresent`, after the `favorite` present-assertion (~line 651), add:

```go
openVal, hasOpen := raw["open"]
assert.True(t, hasOpen, "open must be present in JSON even when false")
assert.Equal(t, false, openVal)
```

- [ ] **Step 2: Run test to verify it fails.** Run: `make test SERVICE=../pkg/model` — if the model package isn't a SERVICE target, use the package path the Makefile expects; expected FAIL: `raw["open"]` missing / `Open` undefined.

- [ ] **Step 3: Add the field.** In `pkg/model/subscription.go` after the `Favorite` line (42):

```go
	Favorite           bool             `json:"favorite" bson:"favorite"`
	// Open is the per-user sidebar-visibility flag. New subscriptions are born
	// open (room-worker's newSub sets it); subscription.list excludes only rows
	// explicitly closed (open == false). Set true by the room-service open RPC.
	Open               bool             `json:"open" bson:"open"`
```

- [ ] **Step 4: Run tests to verify they pass.** Run the model package tests. Expected: PASS.

- [ ] **Step 5: Commit.**

```bash
git add pkg/model/subscription.go pkg/model/model_test.go
git commit -m "feat(model): add Subscription.open sidebar-visibility flag"
```

---

### Task 2: Subject builders for the `open` RPC

**Files:**
- Modify: `pkg/subject/subject.go` (add near `FavoriteToggle` ~line 701 and `FavoriteTogglePattern` ~line 909)
- Test: `pkg/subject/subject_test.go`

**Interfaces:**
- Produces: `subject.OpenRoom(account, roomID, siteID) string`, `subject.OpenRoomWildcard(siteID) string`, `subject.OpenRoomPattern(siteID) string`.

- [ ] **Step 1: Write the failing test.** In `subject_test.go`:

```go
func TestOpenRoom(t *testing.T) {
	assert.Equal(t, "chat.user.alice.request.room.r1.site-a.open", subject.OpenRoom("alice", "r1", "site-a"))
}

func TestOpenRoomWildcard(t *testing.T) {
	assert.Equal(t, "chat.user.*.request.room.*.site-a.open", subject.OpenRoomWildcard("site-a"))
}

func TestOpenRoomPattern(t *testing.T) {
	assert.Equal(t, "chat.user.{account}.request.room.{roomID}.site-a.open", subject.OpenRoomPattern("site-a"))
}

func TestOpenRoom_ParseUserRoomSubject(t *testing.T) {
	account, roomID, ok := subject.ParseUserRoomSubject(subject.OpenRoom("alice", "r1", "site-a"))
	require.True(t, ok)
	assert.Equal(t, "alice", account)
	assert.Equal(t, "r1", roomID)
}
```

- [ ] **Step 2: Run test to verify it fails.** Run: `make test SERVICE=../pkg/subject` (or the package's test target). Expected FAIL: `OpenRoom` undefined.

- [ ] **Step 3: Add the builders.** After the `FavoriteToggle`/`FavoriteToggleWildcard` block (~line 708):

```go
// OpenRoom returns the concrete subject for the per-user open RPC.
func OpenRoom(account, roomID, siteID string) string {
	return fmt.Sprintf("chat.user.%s.request.room.%s.%s.open", account, roomID, siteID)
}

// OpenRoomWildcard is the per-site subscription pattern for the open RPC.
func OpenRoomWildcard(siteID string) string {
	return fmt.Sprintf("chat.user.*.request.room.*.%s.open", siteID)
}
```

And after `FavoriteTogglePattern` (~line 911):

```go
func OpenRoomPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.room.{roomID}.%s.open", siteID)
}
```

- [ ] **Step 4: Run tests to verify they pass.** Expected: PASS.

- [ ] **Step 5: Commit.**

```bash
git add pkg/subject/subject.go pkg/subject/subject_test.go
git commit -m "feat(subject): add open-room RPC subject builders"
```

---

### Task 3: Wire model — response, inbox event, event-type const, outbox partition

**Files:**
- Modify: `pkg/model/event.go` (add const near line 134; add response + event structs near `FavoriteToggleResponse` ~line 469)
- Modify: `pkg/outbox/outbox.go` (add to `ConcurrentEventTypes` ~line 27)
- Test: `pkg/model/model_test.go`, `pkg/outbox/outbox_test.go` (create test if none for the partition)

**Interfaces:**
- Produces: `model.InboxSubscriptionOpened` (= `"subscription_opened"`), `model.OpenRoomResponse{Status string; Open bool}`, `model.SubscriptionOpenedEvent{Account, RoomID string; Open bool; Timestamp int64}`.

- [ ] **Step 1: Write the failing tests.** In `model_test.go`:

```go
func TestOpenRoomResponseJSON(t *testing.T) {
	r := model.OpenRoomResponse{Status: "ok", Open: true}
	b, err := json.Marshal(r)
	require.NoError(t, err)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(b, &raw))
	assert.Equal(t, "ok", raw["status"])
	assert.Equal(t, true, raw["open"])
	roundTrip(t, &r, &model.OpenRoomResponse{})
}

func TestSubscriptionOpenedEventJSON(t *testing.T) {
	e := model.SubscriptionOpenedEvent{Account: "alice", RoomID: "r1", Open: true, Timestamp: 123}
	roundTrip(t, &e, &model.SubscriptionOpenedEvent{})
}

func TestInboxSubscriptionOpenedConst(t *testing.T) {
	assert.Equal(t, "subscription_opened", model.InboxSubscriptionOpened)
}
```

In `pkg/outbox/outbox_test.go` (create if absent, `package outbox`):

```go
func TestOpenInConcurrentEventTypes(t *testing.T) {
	found := false
	for _, et := range outbox.ConcurrentEventTypes {
		if et == model.InboxSubscriptionOpened {
			found = true
		}
	}
	assert.True(t, found, "subscription_opened must ride the concurrent lane")
}
```

- [ ] **Step 2: Run tests to verify they fail.** Run the `pkg/model` and `pkg/outbox` test targets. Expected FAIL: undefined identifiers.

- [ ] **Step 3: Implement.** In `pkg/model/event.go`, add to the `InboxEventType` const block (after line 134):

```go
	InboxSubscriptionOpened          InboxEventType = "subscription_opened"
```

After `FavoriteToggleResponse` / `SubscriptionFavoriteToggledEvent` (~line 481):

```go
// OpenRoomResponse is the sync reply for the open RPC. Open is always true.
type OpenRoomResponse struct {
	Status string `json:"status"`
	Open   bool   `json:"open"`
}

// SubscriptionOpenedEvent is the InboxEvent.Payload for type "subscription_opened".
type SubscriptionOpenedEvent struct {
	Account   string `json:"account"   bson:"account"`
	RoomID    string `json:"roomId"    bson:"roomId"`
	Open      bool   `json:"open"      bson:"open"`
	Timestamp int64  `json:"timestamp" bson:"timestamp"`
}
```

In `pkg/outbox/outbox.go`, add to `ConcurrentEventTypes` (after `InboxSubscriptionFavoriteToggled`, line 27):

```go
	model.InboxSubscriptionOpened,
```

- [ ] **Step 4: Run tests to verify they pass.** Expected: PASS for both packages.

- [ ] **Step 5: Commit.**

```bash
git add pkg/model/event.go pkg/model/model_test.go pkg/outbox/outbox.go pkg/outbox/outbox_test.go
git commit -m "feat(model): open RPC response, inbox event, outbox partition"
```

---

### Task 4: room-service store — `OpenSubscription` (interface + mock + mongo)

**Files:**
- Modify: `room-service/store.go` (add method to `RoomStore` interface near `ToggleSubscriptionFavorite` ~line 134)
- Modify: `room-service/store_mongo.go` (add impl near `ToggleSubscriptionFavorite` ~line 1075)
- Regenerate: `room-service/mock_store_test.go` via `make generate SERVICE=room-service`
- Test: `room-service/integration_test.go` (add `TestMongoStore_OpenSubscription`)

**Interfaces:**
- Consumes: `MongoStore.findOneAndUpdateSub(ctx, roomID, account, op string, set bson.M)` (existing).
- Produces: `RoomStore.OpenSubscription(ctx context.Context, roomID, account string) (*model.Subscription, error)`.

- [ ] **Step 1: Write the failing integration test.** In `room-service/integration_test.go` (build tag `//go:build integration`, `package main`), mirroring the favorite integration test:

```go
func TestMongoStore_OpenSubscription(t *testing.T) {
	db := testutil.MongoDB(t, "room-svc-open")
	store := NewMongoStore(db, "site-a")
	ctx := context.Background()

	_, err := db.Collection("subscriptions").InsertOne(ctx, bson.M{
		"_id": "s1", "roomId": "r1", "siteId": "site-a",
		"u":    bson.M{"_id": "u_alice", "account": "alice"},
		"open": false,
	})
	require.NoError(t, err)

	sub, err := store.OpenSubscription(ctx, "r1", "alice")
	require.NoError(t, err)
	assert.True(t, sub.Open)

	got, err := store.GetSubscription(ctx, "alice", "r1")
	require.NoError(t, err)
	assert.True(t, got.Open)

	// Idempotent
	sub2, err := store.OpenSubscription(ctx, "r1", "alice")
	require.NoError(t, err)
	assert.True(t, sub2.Open)

	// Missing subscription
	_, err = store.OpenSubscription(ctx, "missing", "alice")
	assert.True(t, errors.Is(err, model.ErrSubscriptionNotFound))
}
```

(Confirm `NewMongoStore` signature against the favorite integration test and match it exactly.)

- [ ] **Step 2: Add the interface method.** In `room-service/store.go` after `ToggleSubscriptionFavorite` (~line 134):

```go
	// OpenSubscription atomically sets open=true for (roomID, account) via a single
	// FindOneAndUpdate and returns the post-update subscription, or
	// model.ErrSubscriptionNotFound (wrapped) when no match.
	OpenSubscription(ctx context.Context, roomID, account string) (*model.Subscription, error)
```

- [ ] **Step 3: Add the mongo impl.** In `room-service/store_mongo.go` after `ToggleSubscriptionFavorite` (~line 1080):

```go
// OpenSubscription sets open=true. Set-not-toggle: no ordering guard is needed
// because applying true repeatedly or out of order always converges to true.
func (s *MongoStore) OpenSubscription(ctx context.Context, roomID, account string) (*model.Subscription, error) {
	return s.findOneAndUpdateSub(ctx, roomID, account, "open subscription", bson.M{
		"open": true,
	})
}
```

- [ ] **Step 4: Regenerate mocks.** Run: `make generate SERVICE=room-service`. Confirm `mock_store_test.go` gains `OpenSubscription`.

- [ ] **Step 5: Run the integration test.** Run: `make test-integration SERVICE=room-service` (requires Docker). Expected: PASS. If Docker is unavailable in this environment, verify compilation via `make build SERVICE=room-service` and note the integration test is written but unrun.

- [ ] **Step 6: Commit.**

```bash
git add room-service/store.go room-service/store_mongo.go room-service/mock_store_test.go room-service/integration_test.go
git commit -m "feat(room-service): OpenSubscription store method"
```

---

### Task 5: room-service handler — `openRoom` RPC + registration

**Files:**
- Modify: `room-service/handler.go` (register at ~line 97; add `openRoom` near `favoriteToggle` ~line 2151)
- Test: `room-service/handler_test.go` (add the seven-test suite, parallel to the favorite suite)

**Interfaces:**
- Consumes: `subject.OpenRoomPattern`, `h.store.OpenSubscription`, `h.store.GetUserSiteID`, `h.publishSubscriptionUpdate(ctx, account, action, sub, roomName, ts)`, `h.federateOne(ctx, roomID, destSiteID, eventType, payload, dedupSeed, ts)`, `errNotRoomMember`, `model.OpenRoomResponse`, `model.SubscriptionOpenedEvent`, `model.InboxSubscriptionOpened`.
- Produces: `(*Handler).openRoom(c *natsrouter.Context) (*model.OpenRoomResponse, error)`.

- [ ] **Step 1: Write the failing tests.** In `room-service/handler_test.go`, copy the favorite-toggle test suite and adapt. Locate the favorite tests (`TestHandler_FavoriteToggle_*`) and mirror them as `TestHandler_OpenRoom_*` per the spec's table. Key adaptations: build the subject with `subject.OpenRoom("alice","r1","site-a")`; mock `OpenSubscription` (not `ToggleSubscriptionFavorite`) to return `&model.Subscription{User: model.SubscriptionUser{ID:"u_alice",Account:"alice"}, RoomID:"r1", Open:true}`; assert `Action:"opened"` on the captured `SubscriptionUpdateEvent`; decode the cross-site `OutboxEvent` inner payload as `model.SubscriptionOpenedEvent` and assert `Open==true`, nonzero `Timestamp`, envelope `Type==model.InboxSubscriptionOpened`; not-member returns `errNotRoomMember`; store error contains `"open subscription"`; get-siteID error contains `"get user siteId"`; cross-site publishToStream failure contains `"federate opened"`; core-publish failure is non-fatal (`require.NoError`, reply `{ok,true}`). Use the exact mock/capture harness the favorite tests use.

- [ ] **Step 2: Run tests to verify they fail.** Run: `make test SERVICE=room-service`. Expected FAIL: `openRoom` undefined / registration missing.

- [ ] **Step 3: Register the handler.** In `room-service/handler.go` `Register`, after the favorite line (97):

```go
	natsrouter.RegisterNoBody(r, subject.OpenRoomPattern(h.siteID), h.openRoom)
```

- [ ] **Step 4: Implement the handler.** After `favoriteToggle` (~line 2201):

```go
func (h *Handler) openRoom(c *natsrouter.Context) (*model.OpenRoomResponse, error) {
	var ctx context.Context = c
	account := c.Param("account")
	roomID := c.Param("roomID")

	if span := trace.SpanFromContext(ctx); span.IsRecording() {
		span.SetAttributes(
			attribute.String("room.id", roomID),
			attribute.String("site.id", h.siteID),
		)
	}

	now := time.Now().UTC()
	sub, err := h.store.OpenSubscription(ctx, roomID, account)
	if err != nil {
		if errors.Is(err, model.ErrSubscriptionNotFound) {
			return nil, errNotRoomMember
		}
		return nil, fmt.Errorf("open subscription: %w", err)
	}

	if _, err := h.publishSubscriptionUpdate(ctx, account, "opened", sub, "", now); err != nil {
		return nil, err
	}

	userSiteID, err := h.store.GetUserSiteID(ctx, account)
	if err != nil {
		return nil, fmt.Errorf("get user siteId: %w", err)
	}
	if userSiteID != "" && userSiteID != h.siteID {
		payload := model.SubscriptionOpenedEvent{
			Account:   account,
			RoomID:    roomID,
			Open:      sub.Open,
			Timestamp: now.UnixMilli(),
		}
		payloadData, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("marshal opened payload: %w", err)
		}
		if err := h.federateOne(ctx, roomID, userSiteID, model.InboxSubscriptionOpened, payloadData, roomID+":"+account, now.UnixMilli()); err != nil {
			return nil, fmt.Errorf("federate opened: %w", err)
		}
	}

	return &model.OpenRoomResponse{Status: "ok", Open: sub.Open}, nil
}
```

- [ ] **Step 5: Run tests to verify they pass.** Run: `make test SERVICE=room-service`. Expected: PASS.

- [ ] **Step 6: Commit.**

```bash
git add room-service/handler.go room-service/handler_test.go
git commit -m "feat(room-service): open RPC handler with cross-site federation"
```

---

### Task 6: room-worker — new subscriptions born open

**Files:**
- Modify: `room-worker/handler.go` (`newSub` ~line 1383)
- Test: `room-worker/handler_test.go`

**Interfaces:**
- Consumes: `newSub(id string, user *model.User, room *model.Room, roles []model.Role, name string, isSubscribed bool, joinedAt time.Time) *model.Subscription` (existing).
- Produces: same signature; result now has `Open: true`.

- [ ] **Step 1: Write the failing test.** In `room-worker/handler_test.go`:

```go
func TestNewSub_OpenTrue(t *testing.T) {
	user := &model.User{ID: "u_alice", Account: "alice"}
	room := &model.Room{ID: "r1", SiteID: "site-a", Type: model.RoomTypeChannel}
	sub := newSub("s1", user, room, nil, "general", false, time.Now())
	assert.True(t, sub.Open, "new subscriptions must be born open")
}
```

(Match the exact `newSub` argument order from `handler.go:1383`.)

- [ ] **Step 2: Run test to verify it fails.** Run: `make test SERVICE=room-worker`. Expected FAIL: `sub.Open` is false.

- [ ] **Step 3: Set the default.** In `newSub`'s returned struct literal (~line 1385), add `Open: true`:

```go
	return &model.Subscription{
		ID:           id,
		User:         model.SubscriptionUser{ID: user.ID, Account: user.Account, IsBot: model.IsBot(user.Account) || model.IsPlatformAdminAccount(user.Account)},
		RoomID:       room.ID,
		SiteID:       room.SiteID,
		Roles:        roles,
		Name:         name,
		RoomType:     room.Type,
		IsSubscribed: isSubscribed,
		JoinedAt:     joinedAt,
		Open:         true,
	}
```

- [ ] **Step 4: Run tests to verify they pass.** Run: `make test SERVICE=room-worker`. Expected: PASS (watch for any existing test asserting a full-struct equality on a `newSub` result — update its expected value to include `Open: true`).

- [ ] **Step 5: Commit.**

```bash
git add room-worker/handler.go room-worker/handler_test.go
git commit -m "feat(room-worker): new subscriptions born open=true"
```

---

### Task 7: user-service — exclude closed rooms from `subscription.list`

**Files:**
- Modify: `user-service/mongorepo/subscriptions.go` (`AggregateSubscriptions` match ~line 242; `subscriptionProjection` ~line 183)
- Test: `user-service/mongorepo/subscriptions_test.go` (integration; check existing `//go:build integration` tests there for the harness)

**Interfaces:**
- Consumes: `SubscriptionRepo.AggregateSubscriptions(ctx, account, listType string, favorite bool, withinDays *int, page)` (existing).
- Produces: same; now excludes `open: false` rows.

- [ ] **Step 1: Write the failing integration test.** In `user-service/mongorepo/subscriptions_test.go` (match the existing test harness — likely `testutil.MongoDB` + `NewSubscriptionRepo`; also insert a matching `rooms` doc so the deleted-filter `$lookup` keeps the sub). Insert three channel subs for `alice`: one `open: true`, one `open: false`, one with no `open` key; each with a non-deleted room doc. Assert the list returns the open and missing-field rows and excludes the `open: false` row:

```go
func TestAggregateSubscriptions_ExcludesClosed(t *testing.T) {
	// ... set up db, repo, insert rooms r_open/r_closed/r_missing (non-"Del-" names)
	// ... insert subs: (r_open, open:true), (r_closed, open:false), (r_missing, no open field)
	res, err := repo.AggregateSubscriptions(ctx, "alice", "rooms", false, nil, mongoutil.OffsetPageRequest{Offset: 0, Limit: 50})
	require.NoError(t, err)
	roomIDs := map[string]bool{}
	for _, s := range res.Data {
		roomIDs[s.RoomID] = true
	}
	assert.True(t, roomIDs["r_open"])
	assert.True(t, roomIDs["r_missing"])
	assert.False(t, roomIDs["r_closed"], "explicitly closed subscription must be excluded")
}
```

- [ ] **Step 2: Run test to verify it fails.** Run: `make test-integration SERVICE=user-service`. Expected FAIL: `r_closed` present. (If Docker unavailable, note as written-but-unrun and rely on `make build`.)

- [ ] **Step 3: Add the filter.** In `AggregateSubscriptions`, after the `favorite` block (~line 244), before building the pipeline:

```go
	// Exclude rooms explicitly closed by the user; a missing field (defensive)
	// and open:true both pass. Applied to subscription.list only.
	match["open"] = bson.M{"$ne": false}
```

- [ ] **Step 4: Surface the field on the projection.** In `subscriptionProjection` (~line 183), after `"favorite": 1,`:

```go
		"open":              1,
```

- [ ] **Step 5: Run tests to verify they pass.** Run: `make test-integration SERVICE=user-service`. Expected: PASS.

- [ ] **Step 6: Commit.**

```bash
git add user-service/mongorepo/subscriptions.go user-service/mongorepo/subscriptions_test.go
git commit -m "feat(user-service): exclude closed subscriptions from subscription.list"
```

---

### Task 8: inbox-worker — mirror `subscription_opened`

**Files:**
- Modify: `inbox-worker/handler.go` (dispatch switch ~line 112; `InboxStore` interface ~line 55; add `handleSubscriptionOpened` near `handleSubscriptionFavoriteToggled` ~line 293)
- Modify: `inbox-worker/main.go` (add `UpdateSubscriptionOpen` near `UpdateSubscriptionFavorite` ~line 267)
- Regenerate: `inbox-worker/mock_store_test.go` via `make generate SERVICE=inbox-worker`
- Test: `inbox-worker/handler_test.go` (extend `stubInboxStore`; three handler tests), `inbox-worker/integration_test.go` (`TestMongoInboxStore_UpdateSubscriptionOpen`)

**Interfaces:**
- Consumes: `s.naksIfSubscriptionMissing(ctx, account, roomID)` (existing), `model.SubscriptionOpenedEvent`.
- Produces: `InboxStore.UpdateSubscriptionOpen(ctx context.Context, roomID, account string, open bool) error`, `(*Handler).handleSubscriptionOpened(ctx, evt *model.InboxEvent) error`.

- [ ] **Step 1: Write the failing handler tests.** In `inbox-worker/handler_test.go`, extend `stubInboxStore` with an `UpdateSubscriptionOpen` method (mutating the in-memory sub, silent no-op on missing), then:

```go
func TestHandler_SubscriptionOpened(t *testing.T) {
	// store seeded with sub (r1, alice, open:false)
	// build InboxEvent{Type:"subscription_opened", Payload: json of SubscriptionOpenedEvent{Account:"alice",RoomID:"r1",Open:true,Timestamp:123}}
	// require.NoError(t, h.HandleEvent(ctx, raw))
	// assert seeded sub now Open==true
}

func TestHandler_SubscriptionOpened_MissingSubscriptionNoOp(t *testing.T) {
	// empty store; payload references unknown account; HandleEvent returns nil
}

func TestHandler_SubscriptionOpened_MalformedPayload(t *testing.T) {
	// Payload: []byte("not-json"); HandleEvent returns error
}
```

(Mirror the exact shape of the `TestHandler_SubscriptionFavoriteToggled*` tests.)

- [ ] **Step 2: Run tests to verify they fail.** Run: `make test SERVICE=inbox-worker`. Expected FAIL: undefined `handleSubscriptionOpened` / `UpdateSubscriptionOpen`.

- [ ] **Step 3: Add the dispatch case.** In `inbox-worker/handler.go` after the favorite case (line 112):

```go
	case "subscription_opened":
		return h.handleSubscriptionOpened(ctx, &evt)
```

- [ ] **Step 4: Add the interface method.** In the `InboxStore` interface after `UpdateSubscriptionFavorite` (~line 58):

```go
	// UpdateSubscriptionOpen sets open by (roomID, account). No ordering guard:
	// set-true is idempotent. Missing sub is a silent no-op.
	UpdateSubscriptionOpen(ctx context.Context, roomID, account string, open bool) error
```

- [ ] **Step 5: Add the handler method.** After `handleSubscriptionFavoriteToggled` (~line 303):

```go
// handleSubscriptionOpened mirrors a room-side open onto the user's home-site subscription.
func (h *Handler) handleSubscriptionOpened(ctx context.Context, evt *model.InboxEvent) error {
	var e model.SubscriptionOpenedEvent
	if err := json.Unmarshal(evt.Payload, &e); err != nil {
		return fmt.Errorf("unmarshal subscription_opened payload: %w", err)
	}
	if err := h.store.UpdateSubscriptionOpen(ctx, e.RoomID, e.Account, e.Open); err != nil {
		return fmt.Errorf("update subscription open for %q in room %q: %w", e.Account, e.RoomID, err)
	}
	return nil
}
```

- [ ] **Step 6: Add the mongo impl.** In `inbox-worker/main.go` after `UpdateSubscriptionFavorite` (~line 285):

```go
// UpdateSubscriptionOpen sets open by (roomID, account). No high-water guard —
// set-true is idempotent and order-insensitive. Missing sub is a silent no-op.
func (s *mongoInboxStore) UpdateSubscriptionOpen(ctx context.Context, roomID, account string, open bool) error {
	res, err := s.subCol.UpdateOne(ctx,
		bson.M{"roomId": roomID, "u.account": account},
		bson.M{"$set": bson.M{"open": open}},
	)
	if err != nil {
		return fmt.Errorf("update subscription open for %q in room %q: %w", account, roomID, err)
	}
	if res.MatchedCount == 0 {
		return s.naksIfSubscriptionMissing(ctx, account, roomID)
	}
	return nil
}
```

- [ ] **Step 7: Regenerate mocks and add the integration test.** Run: `make generate SERVICE=inbox-worker`. In `inbox-worker/integration_test.go` add `TestMongoInboxStore_UpdateSubscriptionOpen` mirroring the favorite integration test: seed a sub with `open:false`, call `UpdateSubscriptionOpen(ctx,"r1","alice",true)`, assert stored `open==true`; call for a missing sub, assert no error and no doc created.

- [ ] **Step 8: Run tests to verify they pass.** Run: `make test SERVICE=inbox-worker`, then `make test-integration SERVICE=inbox-worker` (Docker). Expected: PASS.

- [ ] **Step 9: Commit.**

```bash
git add inbox-worker/handler.go inbox-worker/main.go inbox-worker/mock_store_test.go inbox-worker/handler_test.go inbox-worker/integration_test.go
git commit -m "feat(inbox-worker): mirror subscription_opened cross-site event"
```

---

### Task 9: Client API docs

**Files:**
- Modify: `docs/client-api.md` (new "Open Room" RPC section; `subscription.update` action enum; `Subscription` field table)
- Modify: `docs/client-api/request-reply.md` (derived RPC view)
- Modify: `docs/client-api/events.md` (derived events view — `subscription.update` action enum + `Subscription.open` field)

**Interfaces:**
- Consumes: the wire shapes from Tasks 1–5 (`OpenRoomResponse`, subject, `errNotRoomMember`, `action: "opened"`, `Subscription.open`).

- [ ] **Step 1: Read the favorite sections.** In `docs/client-api.md`, find "Toggle Favorite" and the `subscription.update` event and `Subscription` field table. In the two derived views, find the same. These are the templates.

- [ ] **Step 2: Add the "Open Room" RPC section** to `docs/client-api.md`, sibling to "Toggle Favorite": subject `chat.user.{account}.request.room.{roomID}.{siteID}.open`; empty request body; response `OpenRoomResponse` field table (`status: string`, `open: bool`) with a JSON example `{"status":"ok","open":true}`; error case `errNotRoomMember` (requester has no subscription in the room); triggered `subscription.update` event with `action: "opened"`; note the cross-site home-site mirror.

- [ ] **Step 3: Extend the `subscription.update` action enum** in `docs/client-api.md` to include `"opened"`, and add `open` (bool) to the `Subscription` field table.

- [ ] **Step 4: Mirror the changes** into `docs/client-api/request-reply.md` (the new RPC) and `docs/client-api/events.md` (the `subscription.update` action enum and the `Subscription.open` field), matching each file's existing style.

- [ ] **Step 5: Commit.**

```bash
git add docs/client-api.md docs/client-api/request-reply.md docs/client-api/events.md
git commit -m "docs(client-api): document open RPC, opened action, Subscription.open"
```

---

### Task 10: Full verification sweep

**Files:** none (verification only).

- [ ] **Step 1: Lint.** Run: `make lint`. Fix any findings. Expected: clean.

- [ ] **Step 2: Unit tests across touched packages.** Run: `make test SERVICE=room-service`, `make test SERVICE=room-worker`, `make test SERVICE=inbox-worker`, and the `pkg/model`, `pkg/subject`, `pkg/outbox` test targets. Expected: PASS.

- [ ] **Step 3: Integration tests (if Docker available).** Run: `make test-integration SERVICE=room-service`, `make test-integration SERVICE=user-service`, `make test-integration SERVICE=inbox-worker`. Expected: PASS. If Docker is unavailable, record which integration tests are written-but-unrun.

- [ ] **Step 4: SAST.** Run: `make sast`. Expected: no new medium+ findings.

- [ ] **Step 5: Delete session review reports.** If `docs/reviews/` has files from this session, delete them before any PR (CLAUDE.md §5). No commit needed if the directory is empty.

- [ ] **Step 6: Final confirmation.** Confirm every spec section maps to a completed task: model field (T1), subject (T2), wire/outbox (T3), store (T4), handler (T5), create-default (T6), list filter (T7), inbox mirror (T8), docs (T9).

---

## Self-Review Notes

- **Spec coverage:** model field → T1; subject → T2; response/event/const/outbox → T3; store `OpenSubscription` → T4; handler `openRoom` + registration + federation → T5; `newSub` create-default → T6; `subscription.list` filter + projection → T7; inbox mirror (dispatch, interface, handler, mongo, no-guard) → T8; client-api + derived views → T9; verification → T10. All covered.
- **Placeholder scan:** all code steps carry concrete code; test-heavy steps (T5, T7, T8) reference the exact favorite-suite template to copy and list every adaptation — no vague "add tests".
- **Type consistency:** `OpenSubscription(ctx, roomID, account)`, `UpdateSubscriptionOpen(ctx, roomID, account, open)`, `OpenRoomResponse{Status, Open}`, `SubscriptionOpenedEvent{Account, RoomID, Open, Timestamp}`, `InboxSubscriptionOpened = "subscription_opened"`, action `"opened"`, subject tail `.open` — used identically across T3–T9.
