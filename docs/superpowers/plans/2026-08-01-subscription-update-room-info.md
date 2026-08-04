# Enriched "added" subscription.update Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every `"added"` subscription.update event carries a populated `subscription.room` object (`model.SubscriptionRoom`, incl. the E2E room key), matching a `subscription.list` row; the now-redundant initial `room.key` fan-outs on member.add / create-room are removed.

**Architecture:** Publish-time enrichment in room-worker (the room's home site — the authority for all room fields). A new `subscriptionRoomFor(room, pair)` helper builds the view; the four `"added"` publish sites feed it from the data each already has (fresh `GetRoom` read for member.add, in-memory room for create/DM paths). No new RPCs, no schema changes, no inbox/outbox changes. FE merges the event's `room` like a list row and seeds the key from it.

**Tech Stack:** Go 1.25, NATS, gomock, testify (backend); React + vitest (chat-frontend).

**Spec:** `docs/superpowers/specs/2026-08-01-subscription-update-room-info-design.md`

## Global Constraints

- TDD (Red-Green-Refactor) for every code change; run the failing test before implementing.
- All test/lint commands via `make` targets, never raw `go` (`make test SERVICE=room-worker`, `make lint`).
- `Subscription.Room` is `bson:"-"` — must never be persisted; only set on the event copy, never on the sub pointer passed to Mongo writes.
- Key encoding: std-base64 of the 32-byte secret (identical to user-service `buildLocalRoom` and the legacy `room.key` wire form).
- Keep the flat `RoomName` event field (back-compat).
- Out of scope: `role_updated`/`removed`/`read` etc. actions, Teams-migration path (`teamsroomcreate.go` key fan-out STAYS), rotation fan-out on member removal (STAYS), inbox-worker, previewMessage.
- Never log event payloads (they now contain the key).
- `docs/client-api.md` + `docs/client-api/events.md` must land in the same PR.

---

### Task 1: `subscriptionRoomFor` helper

**Files:**
- Modify: `room-worker/handler.go` (place near `newSub`, ~line 1455)
- Test: `room-worker/handler_test.go`

**Interfaces:**
- Consumes: `model.Room`, `roomkeystore.VersionedKeyPair`, `model.SubscriptionRoom` (all exist).
- Produces: `subscriptionRoomFor(room *model.Room, pair *roomkeystore.VersionedKeyPair) *model.SubscriptionRoom` — used by Tasks 2–4.

- [ ] **Step 1: Write the failing test** (append to `room-worker/handler_test.go`)

```go
func TestSubscriptionRoomFor(t *testing.T) {
	lastMsg := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	mention := time.Date(2026, 7, 1, 9, 0, 0, 0, time.UTC)
	floor := time.Date(2026, 6, 30, 8, 0, 0, 0, time.UTC)
	room := &model.Room{
		ID: "r1", Name: "eng", Type: model.RoomTypeChannel, SiteID: "site-a",
		UserCount: 3, AppCount: 1, LastMsgAt: &lastMsg, LastMsgID: "m123",
		LastMentionAllAt: &mention, MinUserLastSeenAt: &floor, CrossSite: ptrBool(false),
	}

	t.Run("channel with key pair carries every field incl. base64 key", func(t *testing.T) {
		pair := &roomkeystore.VersionedKeyPair{
			Version: 2,
			KeyPair: roomkeystore.RoomKeyPair{PrivateKey: bytes.Repeat([]byte{0x05}, 32)},
		}
		got := subscriptionRoomFor(room, pair)
		assert.Equal(t, "site-a", got.SiteID)
		assert.Equal(t, "eng", got.Name)
		require.NotNil(t, got.CrossSite)
		assert.False(t, *got.CrossSite)
		assert.Equal(t, 3, got.UserCount)
		assert.Equal(t, 1, got.AppCount)
		assert.Equal(t, &lastMsg, got.LastMsgAt)
		assert.Equal(t, "m123", got.LastMsgID)
		assert.Equal(t, &mention, got.LastMentionAllAt)
		assert.Equal(t, &floor, got.MinUserLastSeenAt)
		require.NotNil(t, got.PrivateKey)
		assert.Equal(t, base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x05}, 32)), *got.PrivateKey)
		require.NotNil(t, got.KeyVersion)
		assert.Equal(t, 2, *got.KeyVersion)
		assert.Nil(t, got.PreviewMessage, "previewMessage is never set on events")
	})

	t.Run("nil pair (DM/botDM/self-DM) omits the key fields", func(t *testing.T) {
		got := subscriptionRoomFor(room, nil)
		assert.Nil(t, got.PrivateKey)
		assert.Nil(t, got.KeyVersion)
		assert.Equal(t, "eng", got.Name)
	})

	t.Run("nil time fields and nil CrossSite pass through", func(t *testing.T) {
		bare := &model.Room{ID: "r2", Name: "fresh", SiteID: "site-a", UserCount: 2}
		got := subscriptionRoomFor(bare, nil)
		assert.Nil(t, got.CrossSite)
		assert.Nil(t, got.LastMsgAt)
		assert.Nil(t, got.LastMentionAllAt)
		assert.Nil(t, got.MinUserLastSeenAt)
		assert.Empty(t, got.LastMsgID)
	})
}
```

(`ptrBool` already exists in the test package; add `"bytes"` and `"encoding/base64"` to the test imports if absent.)

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=room-worker` (or `go test ./room-worker/ -run TestSubscriptionRoomFor -race` if make lacks a run filter — prefer make)
Expected: FAIL — `undefined: subscriptionRoomFor`

- [ ] **Step 3: Write minimal implementation** (in `room-worker/handler.go`, near `newSub`; add `"encoding/base64"` import)

```go
// subscriptionRoomFor builds the read-model room view carried on an "added"
// subscription.update so the FE can render the row like a subscription.list
// item without a follow-up RPC. nil pair (DM/botDM/self-DM — keyless by
// design) omits the key fields. PreviewMessage is intentionally never set:
// it isn't on the room doc, and a member added without shared history must
// not see the prior last message.
func subscriptionRoomFor(room *model.Room, pair *roomkeystore.VersionedKeyPair) *model.SubscriptionRoom {
	sr := &model.SubscriptionRoom{
		SiteID:            room.SiteID,
		Name:              room.Name,
		CrossSite:         room.CrossSite,
		UserCount:         room.UserCount,
		AppCount:          room.AppCount,
		LastMsgAt:         room.LastMsgAt,
		LastMsgID:         room.LastMsgID,
		LastMentionAllAt:  room.LastMentionAllAt,
		MinUserLastSeenAt: room.MinUserLastSeenAt,
	}
	if pair != nil {
		enc := base64.StdEncoding.EncodeToString(pair.KeyPair.PrivateKey)
		ver := pair.Version
		sr.PrivateKey = &enc
		sr.KeyVersion = &ver
	}
	return sr
}
```

- [ ] **Step 4: Run test to verify it passes** — `make test SERVICE=room-worker`, expected PASS.

- [ ] **Step 5: Commit** — `git add room-worker/ && git commit -m "feat(room-worker): add subscriptionRoomFor event-enrichment helper"`

---

### Task 2: member.add — enrich event, drop key fan-out

**Files:**
- Modify: `room-worker/handler.go:1101-1136` (in `processAddMembers`)
- Test: `room-worker/handler_test.go` (rewrite `TestHandler_ProcessAddMembers_PublishesSubscriptionUpdateBeforeRoomKey` at :966, update `TestHandler_ProcessAddMembers_BotGetsKeyAndSubUpdate` at :1031, add key-store-failure + GetRoom-failure tests)

**Interfaces:**
- Consumes: `subscriptionRoomFor` (Task 1); existing `SubscriptionStore.GetRoom(ctx, roomID) (*model.Room, error)` (already in `store.go:83`, already mocked).
- Produces: `"added"` events whose `Subscription.Room` is populated from a **fresh `GetRoom` read taken after `ApplyMemberCountDelta`/`ReconcileMemberCounts`** (so `userCount` includes the members just added, and `CrossSite` reflects any `SetRoomCrossSite` write) plus the key pair. NO `room.key` publish on this path anymore.

- [ ] **Step 1: Write/adjust the failing tests**

Replace `TestHandler_ProcessAddMembers_PublishesSubscriptionUpdateBeforeRoomKey` (the ordering invariant is obsolete — there is no room.key on this path) with:

```go
// TestHandler_ProcessAddMembers_AddedEventCarriesRoomInfoAndKey locks in the
// enriched "added" contract: the event embeds a subscription.room built from a
// fresh post-add GetRoom read plus the current key pair, and NO separate
// room.key event is published (the key rides the subscription.update).
func TestHandler_ProcessAddMembers_AddedEventCarriesRoomInfoAndKey(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	pub := &mockPublisher{}
	publish := func(_ context.Context, subj string, data []byte, _ string) error {
		return pub.Publish(subj, data)
	}
	h := NewHandler(store, "site-a", publish, testKeyStore, roomkeysender.NewSender(pub), subject.RouteGlobal)

	store.EXPECT().GetRoomMeta(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a", CrossSite: ptrBool(false)}, nil)
	store.EXPECT().ListAddMemberCandidates(gomock.Any(), nil, []string{"bob", "charlie"}, "r1").
		Return([]AddMemberCandidate{
			{Account: "bob", HasSubscription: false, HasIndividualRoomMember: false},
			{Account: "charlie", HasSubscription: false, HasIndividualRoomMember: false},
		}, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"bob", "charlie"}).Return([]model.User{
		{ID: "u2", Account: "bob", SiteID: "site-a", EngName: "Bob"},
		{ID: "u3", Account: "charlie", SiteID: "site-a", EngName: "Charlie"},
	}, nil)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(&model.User{
		ID: "u1", Account: "alice", SiteID: "site-a", EngName: "Alice",
	}, nil)
	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().ApplyMemberCountDelta(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil)
	store.EXPECT().HasAnyRoomMembers(gomock.Any(), "r1").Return(false, nil)
	lastMsg := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{
		ID: "r1", Name: "eng", Type: model.RoomTypeChannel, SiteID: "site-a",
		UserCount: 4, AppCount: 0, LastMsgAt: &lastMsg, LastMsgID: "m123",
		CrossSite: ptrBool(false),
	}, nil)

	req := model.AddMembersRequest{
		RoomID: "r1", RequesterAccount: "alice", Users: []string{"bob", "charlie"},
		History:   model.HistoryConfig{Mode: model.HistoryModeNone},
		Timestamp: 1,
	}
	reqData, _ := json.Marshal(req)
	require.NoError(t, h.processAddMembers(natsutil.WithRequestID(context.Background(), testRequestID), reqData))

	for _, account := range []string{"bob", "charlie"} {
		var evt model.SubscriptionUpdateEvent
		found := false
		for i, s := range pub.subjects {
			if s == subject.SubscriptionUpdate(account) {
				require.NoError(t, json.Unmarshal(pub.payloads[i], &evt))
				found = true
			}
			assert.NotEqual(t, subject.RoomKeyUpdate(account), s,
				"no separate room.key on add — the key rides subscription.update")
		}
		require.True(t, found, "subscription.update not published for %s", account)
		room := evt.Subscription.Room
		require.NotNil(t, room, "added event must embed subscription.room")
		assert.Equal(t, "eng", room.Name)
		assert.Equal(t, "site-a", room.SiteID)
		assert.Equal(t, 4, room.UserCount)
		assert.Equal(t, "m123", room.LastMsgID)
		require.NotNil(t, room.LastMsgAt)
		assert.True(t, lastMsg.Equal(*room.LastMsgAt))
		require.NotNil(t, room.CrossSite)
		assert.False(t, *room.CrossSite)
		require.NotNil(t, room.PrivateKey, "key must ride the added event")
		assert.Equal(t, base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x05}, 32)), *room.PrivateKey)
		require.NotNil(t, room.KeyVersion)
		assert.Equal(t, 0, *room.KeyVersion)
	}
}
```

Add the failure-path tests:

```go
// A key-store failure must fail the handler BEFORE any subscription.update is
// published — JetStream then redelivers with nothing half-sent.
func TestHandler_ProcessAddMembers_KeyStoreGetFailureFailsBeforePublish(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)
	pub := &mockPublisher{}
	publish := func(_ context.Context, subj string, data []byte, _ string) error {
		return pub.Publish(subj, data)
	}
	keyStore := NewMockRoomKeyStore(ctrl)
	keyStore.EXPECT().Get(gomock.Any(), "r1").Return(nil, errors.New("mongo down"))
	h := NewHandler(store, "site-a", publish, keyStore, roomkeysender.NewSender(pub), subject.RouteGlobal)

	store.EXPECT().GetRoomMeta(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a"}, nil)
	store.EXPECT().ListAddMemberCandidates(gomock.Any(), nil, []string{"bob"}, "r1").
		Return([]AddMemberCandidate{{Account: "bob"}}, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"bob"}).Return([]model.User{{ID: "u2", Account: "bob", SiteID: "site-a"}}, nil)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(&model.User{ID: "u1", Account: "alice", SiteID: "site-a"}, nil)
	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().ApplyMemberCountDelta(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil)
	store.EXPECT().HasAnyRoomMembers(gomock.Any(), "r1").Return(false, nil)
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Name: "eng", Type: model.RoomTypeChannel, SiteID: "site-a"}, nil)

	req := model.AddMembersRequest{
		RoomID: "r1", RequesterAccount: "alice", Users: []string{"bob"},
		History: model.HistoryConfig{Mode: model.HistoryModeNone}, Timestamp: 1,
	}
	reqData, _ := json.Marshal(req)
	require.Error(t, h.processAddMembers(natsutil.WithRequestID(context.Background(), testRequestID), reqData))
	for _, s := range pub.subjects {
		assert.NotEqual(t, subject.SubscriptionUpdate("bob"), s,
			"no subscription.update may be published when the key read failed")
	}
}
```

(If `MockRoomKeyStore` does not exist, check `room-worker/mock_store_test.go` / the `//go:generate` directives; the RoomKeyStore interface is consumed by room-worker — if only `stubRoomKeyStore` exists, add a small inline `failingKeyStore struct{}` implementing `RoomKeyStore` whose `Get` returns an error, mirroring `stubRoomKeyStore`'s shape, instead of a gomock mock.)

Also update `TestHandler_ProcessAddMembers_BotGetsKeyAndSubUpdate` (:1031): drop the two `RoomKeyUpdate` subject assertions; instead decode the bot's subscription.update and assert `Subscription.Room.PrivateKey != nil` (the bot now gets the key inside the event on its encoded subject). Keep the encoded-subject assertions for subscription.update. Add `store.EXPECT().GetRoom(...)` to its mock setup.

- [ ] **Step 2: Run to verify failures** — `make test SERVICE=room-worker`. Expected: the new/updated tests FAIL (`Subscription.Room` nil, room.key still published, `GetRoom` unexpected-call errors on old code paths).

- [ ] **Step 3: Implement** — in `processAddMembers`, replace lines 1101–1136 (the publish loop + key fan-out block) with:

```go
	// Enrich the "added" fan-out with the room view + key so the FE can render
	// the row (and decrypt) from this single event — no separate room.key is
	// published on this path anymore. The room is re-read AFTER the member-count
	// writes so userCount includes the members just added; both reads happen
	// before any publish so a failure redelivers with nothing half-sent.
	// publishSubscriptionUpdate encodes the per-user subject so a dotted ".bot"
	// account lands on the token its NATS JWT is scoped to.
	if len(subs) > 0 {
		freshRoom, err := h.store.GetRoom(ctx, req.RoomID)
		if err != nil {
			return fmt.Errorf("re-read room for subscription fan-out: %w", err)
		}
		pair, err := h.keyStore.Get(ctx, req.RoomID)
		if err != nil {
			roomkeymetrics.StoreErrors.Add(ctx, 1, metric.WithAttributes(attribute.String("op", "Get")))
			return fmt.Errorf("get room key for subscription fan-out: %w", err)
		}
		subRoom := subscriptionRoomFor(freshRoom, pair)
		for _, sub := range subs {
			subCopy := *sub
			subCopy.Room = subRoom
			subEvt := model.SubscriptionUpdateEvent{
				UserID:       sub.User.ID,
				Subscription: subCopy,
				Action:       "added",
				RoomName:     freshRoom.Name,
				Timestamp:    now.UnixMilli(),
			}
			subEvtData, _ := json.Marshal(subEvt)
			h.publishSubscriptionUpdate(ctx, sub.User.Account, subEvtData)
		}
	}
```

Delete the whole `newSubUsers` / `buildAndFanOutRoomKey` block (old lines 1118–1136). Note `Room` is set on `subCopy` only — the `sub` pointers were already handed to `BulkCreateSubscriptions` and must stay clean (`bson:"-"` guards persistence anyway, but don't rely on it).

- [ ] **Step 4: Run the full service tests** — `make test SERVICE=room-worker`. Fix any OTHER processAddMembers tests that now need a `store.EXPECT().GetRoom(...)` (any test whose request creates ≥1 new subscription) or that assert the removed room.key publishes. Expected: PASS.

- [ ] **Step 5: Commit** — `git commit -am "feat(room-worker): added-event carries room info + key on member.add; drop initial room.key fan-out"`

---

### Task 3: create-room — enrich event, drop key fan-out

**Files:**
- Modify: `room-worker/handler.go` — `finishCreateRoom` (:1751-1868)
- Test: `room-worker/handler_test.go` (Task-35 fan-out tests around :3394, botDM name test around :3254)

**Interfaces:**
- Consumes: `subscriptionRoomFor` (Task 1); `pair *roomkeystore.VersionedKeyPair` (already a `finishCreateRoom` parameter, nil for DM/botDM).
- Produces: `"added"` events with `Subscription.Room` built from the in-memory `room` **with `UserCount`/`AppCount` derived from `subs`** (the initial roster IS `subs`, so the counts are exact; the in-memory room's counts are zero because `ReconcileMemberCounts` writes only to Mongo). No `room.key` publish on this path.

- [ ] **Step 1: Write/adjust the failing tests.** In the Task-35 fan-out test (:3394 area), after decoding each of the 2 subscription.update events, add:

```go
		room := evt.Subscription.Room
		require.NotNil(t, room, "create-room added event must embed subscription.room")
		assert.Equal(t, 2, room.UserCount, "userCount derived from the initial roster")
		assert.Equal(t, 0, room.AppCount)
		require.NotNil(t, room.PrivateKey, "channel create delivers the key inline")
		assert.Nil(t, room.PreviewMessage)
```

and a no-room.key sweep over the captured subjects (same pattern as Task 2). For the botDM create test (:3254 area) assert `evt.Subscription.Room != nil && evt.Subscription.Room.PrivateKey == nil` (nil pair). Match each test's actual roster sizes/expectations — read the test before editing.

- [ ] **Step 2: Run to verify failures** — `make test SERVICE=room-worker`. Expected: new assertions FAIL.

- [ ] **Step 3: Implement.** In `finishCreateRoom`:

(a) Before the fan-out loop (:1762), build the event room view:

```go
	// Event room view: the in-memory room's counts are zero (ReconcileMemberCounts
	// writes only to Mongo), but at creation the roster IS subs — derive the
	// counts from it instead of re-reading. The key rides the event; no separate
	// room.key fan-out on create anymore.
	evtRoom := *room
	evtRoom.UserCount, evtRoom.AppCount = 0, 0
	for _, sub := range subs {
		if sub.User.IsBot {
			evtRoom.AppCount++
		} else {
			evtRoom.UserCount++
		}
	}
	subRoom := subscriptionRoomFor(&evtRoom, pair)
```

(b) Inside the loop set the room on the event copy:

```go
	for _, sub := range subs {
		subCopy := *sub
		subCopy.Room = subRoom
		evt := model.SubscriptionUpdateEvent{
			UserID:       sub.User.ID,
			Subscription: subCopy,
			Action:       "added",
			RoomName:     h.resolveSubUpdateRoomName(ctx, sub, userByAccount),
			Timestamp:    now.UnixMilli(),
		}
		// … existing marshal + publishSubscriptionUpdate unchanged
	}
```

(c) Delete the trailing key fan-out block (:1856-1865, the `if pair != nil { buildAndFanOutRoomKey(...) }` and its comment).

- [ ] **Step 4: Run** — `make test SERVICE=room-worker`; fix other create tests asserting the removed room.key publishes. Expected: PASS.

- [ ] **Step 5: Commit** — `git commit -am "feat(room-worker): added-event carries room info + key on create-room; drop initial room.key fan-out"`

---

### Task 4: DM-sync + self-DM — enrich via publishSubscriptionUpdates

**Files:**
- Modify: `room-worker/handler.go` — `publishSubscriptionUpdates` (:2161), call sites :1451 (self-DM) and :2091 (DM/botDM sync)
- Test: `room-worker/handler_test.go` (DM tests using `decodeSubUpdate` :6882), `room-worker/debug_log_test.go:97,104,111`

**Interfaces:**
- Consumes: `subscriptionRoomFor` (Task 1).
- Produces: new signature `publishSubscriptionUpdates(ctx context.Context, room *model.Room, subs []*model.Subscription, users []*model.User, requestID string)` — builds `subscriptionRoomFor(room, nil)` once (DM/botDM/self-DM rooms are keyless) and sets it on each event copy. These in-memory rooms already carry correct `UserCount`/`AppCount`/`CrossSite` (set at build, :1416/:2019).

- [ ] **Step 1: Write/adjust the failing tests.** In the DM-sync tests, after `decodeSubUpdate(t, captured, account)` add for both participants:

```go
	require.NotNil(t, evt.Subscription.Room, "DM added event must embed subscription.room")
	assert.Equal(t, 2, evt.Subscription.Room.UserCount)
	assert.Nil(t, evt.Subscription.Room.PrivateKey, "DM rooms are keyless — no key fields")
	require.NotNil(t, evt.Subscription.Room.CrossSite)
```

For a self-DM test: `UserCount == 1`, `PrivateKey == nil`, `*CrossSite == false`. Update `debug_log_test.go` calls to the new signature, passing `&model.Room{ID: "r1", SiteID: "site-a", Type: model.RoomTypeDM, UserCount: 2}`.

- [ ] **Step 2: Run to verify failures** — `make test SERVICE=room-worker`. Expected: compile errors at call sites (signature), then assertion failures.

- [ ] **Step 3: Implement.** Change the signature and body:

```go
func (h *Handler) publishSubscriptionUpdates(ctx context.Context, room *model.Room, subs []*model.Subscription, users []*model.User, requestID string) {
	userByAccount := make(map[string]*model.User, len(users))
	for _, u := range users {
		userByAccount[u.Account] = u
	}
	// DM/botDM/self-DM rooms are keyless by design → nil pair, no key fields.
	subRoom := subscriptionRoomFor(room, nil)
	for _, sub := range subs {
		subCopy := *sub
		subCopy.Room = subRoom
		evt := model.SubscriptionUpdateEvent{
			UserID:       sub.User.ID,
			Subscription: subCopy,
			Action:       "added",
			RoomName:     h.resolveSubUpdateRoomName(ctx, sub, userByAccount),
			Timestamp:    time.Now().UTC().UnixMilli(),
		}
		// … existing marshal/publish/log lines unchanged
	}
	// … existing flow log unchanged
}
```

Update call sites: `:1451` → `h.publishSubscriptionUpdates(ctx, room, []*model.Subscription{sub}, []*model.User{requester}, requestID)`; `:2091` → `h.publishSubscriptionUpdates(ctx, room, []*model.Subscription{requesterSub, otherSub}, []*model.User{requester, other}, requestID)`.

- [ ] **Step 4: Run** — `make test SERVICE=room-worker`. Expected: PASS.

- [ ] **Step 5: Commit** — `git commit -am "feat(room-worker): added-event carries room info on DM-sync and self-DM paths"`

---

### Task 5: Integration tests + full backend verification

**Files:**
- Modify (as needed): `room-worker/integration_test.go`

**Interfaces:** none new — regression sweep.

- [ ] **Step 1:** `grep -n "RoomKeyUpdate\|room\.key\|SubscriptionUpdate" room-worker/integration_test.go` — update any test that (a) waits for a `room.key` event after member.add/create (now assert the key inside the subscription.update payload instead) or (b) decodes an "added" `SubscriptionUpdateEvent` (may now assert `Subscription.Room != nil`). Rotation-on-removal key tests stay untouched.
- [ ] **Step 2:** `make lint` — fix findings.
- [ ] **Step 3:** `make test` (all services, race detector) — everything green.
- [ ] **Step 4:** `make test-integration SERVICE=room-worker` (requires Docker) — green.
- [ ] **Step 5:** Coverage gate: `go test -coverprofile=coverage.out ./room-worker/ && go tool cover -func=coverage.out | tail -1` — total ≥80%.
- [ ] **Step 6: Commit** — `git commit -am "test(room-worker): align integration tests with inline key delivery"`

---

### Task 6: Docs — client-api.md + events.md

**Files:**
- Modify: `docs/client-api.md` (subscription.update event section + room.key event section — locate via grep), `docs/client-api/events.md` (:101-127 added-shape table + example; :209-246 room.key section)

**Interfaces:** none — docs mirror Tasks 2–4.

- [ ] **Step 1:** In BOTH files, in the subscription.update `added` shape: add a note + field row that on `action: "added"` the `subscription` carries a populated `room` object → link the existing `SubscriptionRoom` schema (`client-api.md#subscriptionroom`), stating `previewMessage` is always omitted on events and `privateKey`/`keyVersion` are present only for encrypted (channel) rooms. Extend the JSON example with a `room` object (name, siteId, crossSite, userCount, appCount, lastMsgAt, lastMsgId, privateKey, keyVersion).
- [ ] **Step 2:** In BOTH files, rewrite the room.key "**When fired**" list: remove the Create Room and Add Members bullets; state the initial key now arrives inline on the `subscription.update` `added` event (and on `subscription.list`); keep the Remove Member rotation bullet and the on-demand `key.get` paragraph. Update the intro sentence "Fired at create, add, and remove" → fired on rotation (member remove) only.
- [ ] **Step 3:** Re-read both diffs for drift between the canonical doc and the derived view — they must say the same thing.
- [ ] **Step 4: Commit** — `git commit -am "docs(client-api): added subscription.update carries room object; room.key is rotation-only"`

---

### Task 7: Frontend — consume room object + inline key

**Files:**
- Modify: `chat-frontend/src/api/types.ts` (verify `Subscription` has `room?: SubscriptionRoom`; add if missing)
- Modify: `chat-frontend/src/context/RoomEventsContext/useRoomSubscriptions.js:423-464` ("added" branch)
- Test: `chat-frontend/src/context/RoomEventsContext/RoomEventsContext.test.jsx` (or the closest existing test of the added branch)

**Interfaces:**
- Consumes: `evt.subscription.room` (Tasks 2–4 wire shape); existing `seedKeys` callback (already a `useRoomSubscriptions` param, used post-`BUCKETS_LOADED` at :490-497); existing `openChannelSub(roomId, crossSite)`.
- Produces: sidebar row rendered with room metadata at add time; room key seeded without a `room.key` event.

- [ ] **Step 1: Write the failing test.** Follow the file's existing pattern for driving a subscription.update callback. Assert that an `added` event with `subscription.room = { name: 'eng', userCount: 3, crossSite: false, privateKey: 'QUJD…', keyVersion: 2, lastMsgAt: '2026-07-01T10:00:00Z' }`:
  1. calls the `seedKeys` prop once with `[{ roomId, version: 2, privateKey }]`;
  2. dispatches `ROOM_ADDED` whose `room` includes `userCount: 3` and `lastMsgAt`;
  3. opens the channel sub with `crossSite: false` (not the `?? true` fallback);
  and that an added event WITHOUT `room.privateKey` does not call `seedKeys`.
- [ ] **Step 2:** `cd chat-frontend && npm test` — verify FAIL.
- [ ] **Step 3: Implement** — replace the `added` branch body (:425-448):

```js
      if (evt.action === 'added' && evt.subscription?.roomId) {
        // Store the full subscription record FIRST so any consumer that
        // wakes up on the ROOM_ADDED dispatch already sees fresh roles /
        // hasMention / alert state.
        safeDispatch({ type: 'SUBSCRIPTION_UPSERTED', subscription: evt.subscription })
        const sub = evt.subscription
        // The added event embeds the room view (subscription.list parity):
        // metadata renders immediately and the E2E key seeds inline — no
        // separate room.key event is sent for adds anymore (rotation only).
        const roomInfo = sub.room
        const room = {
          id: sub.roomId,
          type: sub.roomType,
          siteId: sub.siteId,
          name: sub.name,
          subscriptionName: sub.name,
          userCount: roomInfo?.userCount,
          lastMsgAt: roomInfo?.lastMsgAt,
          crossSite: roomInfo?.crossSite,
        }
        safeDispatch({ type: 'ROOM_ADDED', room })
        if (roomInfo?.privateKey && typeof roomInfo.keyVersion === 'number') {
          seedKeysRef.current([{ roomId: sub.roomId, version: roomInfo.keyVersion, privateKey: roomInfo.privateKey }])
        }
        if (sub.roomType === 'channel') openChannelSub(sub.roomId, roomInfo?.crossSite ?? true)
      }
```

  Check `toSummary` in `reducer.js`: if it ignores unknown fields, `userCount`/`lastMsgAt` must be mapped there for the summary sort — add the mapping (and a reducer test) only if `toSummary` already models those fields for `BUCKETS_LOADED` rooms; mirror that shape exactly.
- [ ] **Step 4:** `npm test` + `npm run typecheck` — PASS.
- [ ] **Step 5: Commit** — `git commit -am "feat(frontend): render room info and seed key from enriched added subscription.update"`

---

### Task 8: Final sweep

- [ ] **Step 1:** `make sast` — no new medium+ findings (the event now carries the key: confirm no publish site logs the payload).
- [ ] **Step 2:** `make fmt && make lint && make test` one last time; `cd chat-frontend && npm test && npm run typecheck`.
- [ ] **Step 3:** Re-read the spec (`docs/superpowers/specs/2026-08-01-subscription-update-room-info-design.md`) section by section against the diff — every Decision implemented; Teams path + rotation fan-out untouched.
- [ ] **Step 4:** Commit any stragglers; delete `docs/reviews/*` if present (pre-PR rule).
