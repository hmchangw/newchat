# Local/Global Room Subject Separation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Route same-site rooms' `chat.room.{roomID}.>` events onto a leaf-filtered local namespace (`chat.local.room.{roomID}.>`) while cross-site rooms stay on the propagated global namespace, so same-site room subscriptions stop bloating the NATS supercluster gateway interest map.

**Architecture:** A sticky `CrossSite` boolean on the `Room` doc (set by room-worker at the same points it federates membership) is the single source of truth. Publishers pick the namespace from it; clients read it via `subscription.list` and subscribe to the matching prefix. Global (`chat.room.>`, unchanged) is the fail-safe default; local is opt-in once the flag is proven false.

**Tech Stack:** Go 1.25, NATS core + JetStream, MongoDB (`mongo-driver/v2`), `go.uber.org/mock`, `stretchr/testify`, testcontainers.

## Global Constraints

- Use `make` targets only, never raw `go` (`make test SERVICE=<name>`, `make lint`, `make generate SERVICE=<name>`, `make test-integration SERVICE=<name>`).
- TDD Red-Green-Refactor for every task: failing test first, confirm it fails, minimal impl, confirm green, commit.
- All model structs carry both `json` and `bson` tags; `camelCase` tags except `_id`.
- NATS subjects only via `pkg/subject` builders — never raw `fmt.Sprintf` at call sites.
- Generated mocks live in `mock_store_test.go` / `service/mocks/`; never hand-edit. Run `make generate SERVICE=<name>` after a store interface changes.
- Minimum 80% package coverage; cover error/edge paths, not just happy path.
- **Fail-safe invariant:** global is the safe default. Misclassifying a global room as local silently breaks remote delivery; the reverse only wastes interest. Never let a code path emit on the local prefix unless `CrossSite == false` is authoritative.
- **Namespace mapping:** `global == true` → `chat.room.{roomID}.…` (propagated); `global == false` → `chat.local.room.{roomID}.…` (leaf-filtered). A publisher passes the room's `CrossSite` value as `global`.
- Client-facing handler/schema changes must update `docs/client-api.md` (+ derived views) in the same PR.

## Rollout Ordering (why the task order is safe)

Every task through Task 11 leaves production behavior unchanged because publishers/clients still pass `global=true` (existing `chat.room.>`). The flag is populated but not yet honored. The flip to honoring it (Task 12) happens only after the backfill (Task 11) and the platform team's `chat.local.room.>` subscribe grant + leaf filter (Task 13 documents the handoff). Each task is independently deployable.

## File Structure

- `pkg/model/room.go` — add `Room.CrossSite`.
- `pkg/model/subscription.go` — add `SubscriptionRoom.CrossSite`, `EnrichedSubscription.CrossSite`.
- `pkg/model/roominfo.go` (or wherever `RoomInfo` lives) — add `RoomInfo.CrossSite`.
- `pkg/subject/subject.go` — locality-aware `RoomEvent`/`RoomMsgStream`/`RoomMetadataUpdate`.
- `pkg/roommetacache/roommetacache.go` — `Meta.CrossSite` + Mongo projection.
- `room-worker/store.go` + `store_mongo.go` — `SetRoomCrossSite`.
- `room-worker/handler.go` — set flag in `processAddMembers` + `processCreateRoom`; transition nudge; publisher routing.
- `room-service/handler.go` — `roomsInfoBatch` returns `CrossSite`; RoomEvent publisher routing.
- `user-service/mongorepo/subscriptions.go` — `$lookup` projects `crossSite`.
- `user-service/service/subscriptions.go` — `buildLocalRoom`/`applyRoomInfo` map `CrossSite`.
- `broadcast-worker/handler.go` — route RoomEvent publishes by `meta.CrossSite`.
- `tools/data-migration/` (or a room-worker one-off) — backfill existing cross-site rooms.
- `auth-service/handler.go`, `docs/client-api.md` (+ `docs/client-api/*`), `docker-local/setup.sh` — permission + docs.

---

### Task 1: Model fields

**Files:**
- Modify: `pkg/model/room.go` (`Room` struct)
- Modify: `pkg/model/subscription.go` (`SubscriptionRoom`, `EnrichedSubscription`)
- Modify: `RoomInfo` struct file (run `grep -rn "type RoomInfo struct" pkg/model` to confirm path)
- Test: `pkg/model/model_test.go`

**Interfaces:**
- Produces: `model.Room.CrossSite *bool`, `model.SubscriptionRoom.CrossSite *bool`, `model.EnrichedSubscription.CrossSite *bool` (bson `crossSite`), `model.RoomInfo.CrossSite *bool` (json `crossSite`). Tri-state; nil/absent == global (fail-safe).

- [ ] **Step 1: Write the failing test** — add `Room` and `SubscriptionRoom` `CrossSite` to the round-trip coverage in `pkg/model/model_test.go`. Find the existing `roundTrip` cases for `Room`/`Subscription` and add an instance with `CrossSite: true`, asserting it survives marshal/unmarshal. Example addition:

```go
func TestRoom_CrossSiteRoundTrip(t *testing.T) {
	r := model.Room{ID: "r1", SiteID: "site-a", CrossSite: true}
	var got model.Room
	roundTrip(t, r, &got)
	assert.True(t, got.CrossSite)

	sr := model.SubscriptionRoom{SiteID: "site-a", CrossSite: true}
	var gotSR model.SubscriptionRoom
	roundTripJSON(t, sr, &gotSR) // SubscriptionRoom is json-only (bson:"-")
	assert.True(t, gotSR.CrossSite)
}
```

If a `roundTripJSON` helper does not exist, marshal/unmarshal with `encoding/json` inline. Confirm which round-trip helpers exist first (`grep -n "func roundTrip" pkg/model/model_test.go`).

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=model` (or `go test ./pkg/model/ -run TestRoom_CrossSiteRoundTrip` via the Makefile wrapper)
Expected: FAIL — `CrossSite` undefined on `model.Room`.

- [ ] **Step 3: Add the fields.**

In `pkg/model/room.go`, inside `type Room struct`, after `ExternalAccess`:
```go
	// CrossSite is tri-state: nil = unclassified, &true = ≥1 member
	// whose home site differs from SiteID, &false = confirmed same-site. Sticky:
	// true is never cleared. Drives local vs global room-subject routing
	// (chat.local.room.> vs chat.room.>). Missing == nil == GLOBAL (fail-safe).
	CrossSite *bool `json:"crossSite,omitempty" bson:"crossSite,omitempty"`
```

In `pkg/model/subscription.go`, inside `type SubscriptionRoom struct` (near `SiteID`):
```go
	// CrossSite mirrors Room.CrossSite so the client subscribes the room to the
	// global (chat.room.>) or local (chat.local.room.>) namespace. Tri-state;
	// absent == nil == global (fail-safe). bson:"-": read-time only.
	CrossSite *bool `json:"crossSite,omitempty" bson:"-"`
```

In `type EnrichedSubscription struct`, alongside the other `$addFields` room projections:
```go
	CrossSite *bool `json:"-" bson:"crossSite,omitempty"`
```

In `type RoomInfo struct`, after `KeyVersion`:
```go
	CrossSite *bool `json:"crossSite,omitempty"`
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=model`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/model/
git commit -m "feat(model): add CrossSite room-locality flag fields"
```

---

### Task 2: Locality-aware subject builders

**Files:**
- Modify: `pkg/subject/subject.go` (`RoomEvent`, `RoomMsgStream`, `RoomMetadataUpdate`)
- Modify callers (pass `true`, no behavior change): `room-service/handler.go:1435,1809`; `broadcast-worker/handler.go:463,669,818`; `room-worker/handler.go:2234`; `tools/loadgen/daily_pool.go:54,181`
- Test: `pkg/subject/subject_test.go`

**Interfaces:**
- Produces: `subject.RoomEvent(roomID string, global bool) string`, `subject.RoomMsgStream(roomID string, global bool) string`, `subject.RoomMetadataUpdate(roomID string, global bool) string`. `global=true` → `chat.room.{roomID}.…`; `global=false` → `chat.local.room.{roomID}.…`.

- [ ] **Step 1: Write the failing test** — add to `pkg/subject/subject_test.go`:

```go
func TestRoomSubjectLocality(t *testing.T) {
	assert.Equal(t, "chat.room.r1.event", subject.RoomEvent("r1", true))
	assert.Equal(t, "chat.local.room.r1.event", subject.RoomEvent("r1", false))
	assert.Equal(t, "chat.room.r1.stream.msg", subject.RoomMsgStream("r1", true))
	assert.Equal(t, "chat.local.room.r1.stream.msg", subject.RoomMsgStream("r1", false))
	assert.Equal(t, "chat.room.r1.event.metadata.update", subject.RoomMetadataUpdate("r1", true))
	assert.Equal(t, "chat.local.room.r1.event.metadata.update", subject.RoomMetadataUpdate("r1", false))
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=subject` (or the Makefile's pkg test wrapper for `pkg/subject`)
Expected: FAIL — too many arguments to `subject.RoomEvent`.

- [ ] **Step 3: Rewrite the three builders** in `pkg/subject/subject.go`:

```go
// roomBase returns the room-scoped subject root. global=true keeps the
// gateway-propagated chat.room.{roomID} root; global=false uses the
// chat.local.room.{roomID} root, which the leaf node denies from interest
// advertisement (site-local delivery for same-site-only rooms).
func roomBase(roomID string, global bool) string {
	if global {
		return "chat.room." + roomID
	}
	return "chat.local.room." + roomID
}

func RoomEvent(roomID string, global bool) string {
	return roomBase(roomID, global) + ".event"
}

func RoomMsgStream(roomID string, global bool) string {
	return roomBase(roomID, global) + ".stream.msg"
}

func RoomMetadataUpdate(roomID string, global bool) string {
	return roomBase(roomID, global) + ".event.metadata.update"
}
```

- [ ] **Step 4: Update every non-test caller to pass `true`** (behavior unchanged this task). At each site listed under Files, change e.g. `subject.RoomEvent(room.ID)` → `subject.RoomEvent(room.ID, true)`. For the loadgen subscriber sites (`daily_pool.go:54,181`) pass `true` as well (they subscribe to the global namespace for now). Update the existing subject tests that call these builders to pass `true`.

- [ ] **Step 5: Run tests to verify green**

Run: `make test SERVICE=subject` then `make lint`
Expected: PASS; no unused-arg or arity errors anywhere.

- [ ] **Step 6: Commit**

```bash
git add pkg/subject/ room-service/ room-worker/ broadcast-worker/ tools/loadgen/
git commit -m "feat(subject): locality-aware room subject builders (callers pass global=true)"
```

---

### Task 3: roommetacache CrossSite

**Files:**
- Modify: `pkg/roommetacache/roommetacache.go` (`Meta`, `FetchFromMongo`, the `Wrapper` doc projection near line 193)
- Test: `pkg/roommetacache/roommetacache_test.go`

**Interfaces:**
- Produces: `roommetacache.Meta.CrossSite *bool` (json `crossSite`), populated by `FetchFromMongo` and the wrapper loader from the room doc's `crossSite`.

- [ ] **Step 1: Write the failing test** — in `roommetacache_test.go`, extend the existing `FetchFromMongo` test (or add one) to insert a room doc with `crossSite: true` and assert `meta.CrossSite == true`; insert one without it and assert `false`. Mirror the harness of the existing `FetchFromMongo` test (`grep -n "FetchFromMongo" pkg/roommetacache/*_test.go`).

```go
func TestFetchFromMongo_CrossSite(t *testing.T) {
	// ... reuse existing test's Mongo setup (testutil.MongoDB) ...
	_, err := rooms.InsertOne(ctx, bson.M{"_id": "rG", "siteId": "site-a", "crossSite": true})
	require.NoError(t, err)
	meta, err := roommetacache.FetchFromMongo(ctx, rooms, "rG")
	require.NoError(t, err)
	assert.True(t, meta.CrossSite)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test-integration SERVICE=roommetacache` (uses testcontainers Mongo) — or the unit variant if the existing test is unit-level.
Expected: FAIL — `CrossSite` undefined / always false.

- [ ] **Step 3: Add the field + projection.**

In `type Meta struct`:
```go
	CrossSite *bool `json:"crossSite,omitempty"`
```
In `FetchFromMongo`'s `SetProjection`, add `"crossSite": 1,`; in its anonymous `doc` struct add a `CrossSite *bool` field tagged `bson:"crossSite"`; in the returned `Meta{...}` add `CrossSite: doc.CrossSite,`.
Apply the same three additions to the `Wrapper` loader's inline projection/struct near line 193-207.

- [ ] **Step 4: Run test to verify it passes**

Run: `make test-integration SERVICE=roommetacache`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/roommetacache/
git commit -m "feat(roommetacache): project room CrossSite into cached Meta"
```

---

### Task 4: room-worker store SetRoomCrossSite

**Files:**
- Modify: `room-worker/store.go` (interface), `room-worker/store_mongo.go` (impl)
- Regenerate: `room-worker/mock_store_test.go` via `make generate SERVICE=room-worker`
- Test: `room-worker/integration_test.go`

**Interfaces:**
- Produces: `SetRoomCrossSite(ctx context.Context, roomID string) error` on room-worker's `Store` — idempotent `$set crossSite=true` (sticky; never sets false).

- [ ] **Step 1: Write the failing integration test** in `room-worker/integration_test.go` (mirror an existing store integration test's setup):

```go
func TestSetRoomCrossSite_Sticky(t *testing.T) {
	// ... existing store test harness: db := testutil.MongoDB(t, "roomworker"); store := newMongoStore(db) ...
	_, err := db.Collection("rooms").InsertOne(ctx, bson.M{"_id": "r1", "siteId": "site-a"})
	require.NoError(t, err)
	require.NoError(t, store.SetRoomCrossSite(ctx, "r1"))
	var got model.Room
	require.NoError(t, db.Collection("rooms").FindOne(ctx, bson.M{"_id": "r1"}).Decode(&got))
	assert.True(t, got.CrossSite)
	// idempotent second call
	require.NoError(t, store.SetRoomCrossSite(ctx, "r1"))
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test-integration SERVICE=room-worker`
Expected: FAIL — `store.SetRoomCrossSite` undefined.

- [ ] **Step 3: Add interface method + impl + mock.**

In `room-worker/store.go` add to the `Store` interface:
```go
	// SetRoomCrossSite marks a room global (has a cross-site member). Sticky:
	// idempotent, only ever sets crossSite=true.
	SetRoomCrossSite(ctx context.Context, roomID string) error
```
In `room-worker/store_mongo.go`:
```go
func (s *mongoStore) SetRoomCrossSite(ctx context.Context, roomID string) error {
	_, err := s.rooms.UpdateOne(ctx, bson.M{"_id": roomID}, bson.M{"$set": bson.M{"crossSite": true}})
	if err != nil {
		return fmt.Errorf("set room crossSite %s: %w", roomID, err)
	}
	return nil
}
```
(Confirm the collection field name — `s.rooms` — against the existing store; adjust if different.)
Then `make generate SERVICE=room-worker`.

- [ ] **Step 4: Run test to verify it passes**

Run: `make test-integration SERVICE=room-worker`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add room-worker/store.go room-worker/store_mongo.go room-worker/mock_store_test.go room-worker/integration_test.go
git commit -m "feat(room-worker): SetRoomCrossSite sticky store method"
```

---

### Task 5: room-worker sets CrossSite on member add

**Files:**
- Modify: `room-worker/handler.go` (`processAddMembers`, near the `accountsBySite` build ~line 1219)
- Test: `room-worker/handler_test.go`

**Interfaces:**
- Consumes: `Store.SetRoomCrossSite` (Task 4).

- [ ] **Step 1: Write the failing unit test** in `room-worker/handler_test.go` (mirror an existing `processAddMembers`/member-add test harness with the mocked store):

```go
func TestProcessAddMembers_SetsCrossSiteForRemoteMember(t *testing.T) {
	// harness: mockStore expects SetRoomCrossSite once when a member's site != h.siteID
	mockStore.EXPECT().SetRoomCrossSite(gomock.Any(), "r1").Return(nil)
	// ... drive processAddMembers with req adding an account whose userMap site = "site-b", h.siteID = "site-a" ...
}

func TestProcessAddMembers_SameSite_NoCrossSiteWrite(t *testing.T) {
	// all added members on h.siteID → SetRoomCrossSite must NOT be called
	mockStore.EXPECT().SetRoomCrossSite(gomock.Any(), gomock.Any()).Times(0)
	// ... drive processAddMembers with only same-site accounts ...
}
```

Copy the exact mock/handler construction from the nearest existing `processAddMembers` test.

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=room-worker`
Expected: FAIL — `SetRoomCrossSite` never called (missing production code).

- [ ] **Step 3: Add the write** in `processAddMembers`, immediately after `accountsBySite` is built (the map already keyed by remote sites, ~line 1219-1234):

```go
	// A non-empty remote-site bucket means this room now spans sites → mark it
	// global so room-subject routing uses chat.room.> (propagated). Sticky.
	if len(accountsBySite) > 0 {
		if err := h.store.SetRoomCrossSite(ctx, req.RoomID); err != nil {
			return fmt.Errorf("mark room cross-site: %w", err)
		}
	}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=room-worker`
Expected: PASS both cases.

- [ ] **Step 5: Commit**

```bash
git add room-worker/handler.go room-worker/handler_test.go
git commit -m "feat(room-worker): mark room CrossSite when adding a remote member"
```

---

### Task 6: room-worker sets CrossSite on room create

**Files:**
- Modify: `room-worker/handler.go` (`processCreateRoom` ~line 1404, and `buildRoom`/`CreateRoom` write ~line 1371)
- Test: `room-worker/handler_test.go`

**Interfaces:**
- Consumes: the created `model.Room` — set `CrossSite` on the doc before `store.CreateRoom` when initial members span sites.

- [ ] **Step 1: Write the failing unit test** — mirror an existing `processCreateRoom` test:

```go
func TestProcessCreateRoom_CrossSiteAtBirth(t *testing.T) {
	// Capture the Room passed to store.CreateRoom.
	var created *model.Room
	mockStore.EXPECT().CreateRoom(gomock.Any(), gomock.AssignableToTypeOf(&model.Room{}), gomock.Any()).
		DoAndReturn(func(_ context.Context, r *model.Room, _ any) (string, error) { created = r; return r.ID, nil })
	// ... drive processCreateRoom with a DM/channel whose members include a site-b user, h.siteID=site-a ...
	assert.True(t, created.CrossSite)
}

func TestProcessCreateRoom_SameSite_NotCrossSite(t *testing.T) {
	// all members on site-a → created.CrossSite == false
}
```

Adjust the `CreateRoom` mock signature to match the real one (Task 4 area shows `store.CreateRoom(ctx, room, nil)`).

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=room-worker`
Expected: FAIL — `created.CrossSite` is false.

- [ ] **Step 3: Add the computation** where `processCreateRoom` assembles the member/site set (it already resolves each member's site to fan out inbox events per remote site — reuse that resolved set). Before `store.CreateRoom`, set:

```go
	// Born global if any initial member is off-site.
	for _, u := range members { // the resolved member users with SiteID
		if u.SiteID != "" && u.SiteID != h.siteID {
			room.CrossSite = true
			break
		}
	}
```

Use the same member/site collection the create path already builds for its remote-site inbox fan-out (`grep -n "remoteSiteAccounts\|u.SiteID" room-worker/handler.go` within `processCreateRoom`); do not introduce a second membership read.

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=room-worker`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add room-worker/handler.go room-worker/handler_test.go
git commit -m "feat(room-worker): set CrossSite at room creation when members span sites"
```

---

### Task 7: room-service roomsInfoBatch returns CrossSite

**Files:**
- Modify: `room-service/handler.go` (`roomsInfoBatch` ~line 1130-1185, and the room projection it reads)
- Test: `room-service/handler_test.go`

**Interfaces:**
- Consumes: `model.Room.CrossSite`. Produces: `model.RoomInfo.CrossSite` in the `RoomsInfoBatch` reply.

- [ ] **Step 1: Write the failing unit test** — extend the existing `roomsInfoBatch` handler test so a room with `CrossSite:true` yields `info.CrossSite == true`. Mirror the existing test's mocked room store returning `[]model.Room`.

```go
func TestRoomsInfoBatch_CarriesCrossSite(t *testing.T) {
	mockStore.EXPECT().GetRoomsInfo(gomock.Any(), gomock.Any()).
		Return([]model.Room{{ID: "r1", SiteID: "site-a", CrossSite: true, Name: "x"}}, nil)
	// ... call h.roomsInfoBatch with RoomRef{RoomID:"r1"} ...
	assert.True(t, resp.Rooms[0].CrossSite)
}
```

Confirm the real store method name the handler uses to load rooms (`grep -n "func (h \*Handler) roomsInfoBatch" -A40 room-service/handler.go`) and that its projection includes `crossSite` (add `"crossSite": 1` if it uses an explicit projection).

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=room-service`
Expected: FAIL — `resp.Rooms[0].CrossSite` false.

- [ ] **Step 3: Map the field** where `roomsInfoBatch` builds each `model.RoomInfo` from a `model.Room`: add `CrossSite: room.CrossSite,`. If the handler projects room fields explicitly, add `crossSite` to that projection.

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=room-service`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add room-service/handler.go room-service/handler_test.go
git commit -m "feat(room-service): return CrossSite from RoomsInfoBatch"
```

---

### Task 8: user-service $lookup projects crossSite

**Files:**
- Modify: `user-service/mongorepo/subscriptions.go` (the `$addFields`/`$project` room stages, ~lines 77 and 98)
- Test: `user-service/mongorepo/subscriptions_integration_test.go` (or the existing aggregation integration test)

**Interfaces:**
- Produces: `EnrichedSubscription.CrossSite` populated from the joined room's `crossSite`.

- [ ] **Step 1: Write the failing integration test** — mirror the existing aggregation test that inserts a room + subscription and asserts enriched fields; add a room with `crossSite:true` and assert the enriched sub's `CrossSite == true`.

- [ ] **Step 2: Run test to verify it fails**

Run: `make test-integration SERVICE=user-service`
Expected: FAIL — `CrossSite` false.

- [ ] **Step 3: Add to the projection.** In the `$addFields` stage that copies `"userCount": "$room.userCount"` (~line 98) add `"crossSite": "$room.crossSite",`. If a preceding `$project`/`$lookup` pipeline (~line 77) whitelists room fields, add `"crossSite": 1` there too so `$room.crossSite` is available.

- [ ] **Step 4: Run test to verify it passes**

Run: `make test-integration SERVICE=user-service`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add user-service/mongorepo/subscriptions.go user-service/mongorepo/
git commit -m "feat(user-service): project room crossSite into enriched subscription"
```

---

### Task 9: user-service enrichment maps CrossSite to client payload

**Files:**
- Modify: `user-service/service/subscriptions.go` (`buildLocalRoom` ~384, `applyRoomInfo` ~427)
- Test: `user-service/service/subscriptions_test.go`

**Interfaces:**
- Consumes: `EnrichedSubscription.CrossSite` (Task 8), `RoomInfo.CrossSite` (Task 7). Produces: `SubscriptionRoom.CrossSite` in `subscription.list` output.

- [ ] **Step 1: Write the failing unit test:**

```go
func TestBuildLocalRoom_CrossSite(t *testing.T) {
	sub := &model.EnrichedSubscription{}
	sub.CrossSite = true
	sub.RoomName = "chan"
	got := buildLocalRoom(sub)
	require.NotNil(t, got)
	assert.True(t, got.CrossSite)
}

func TestApplyRoomInfo_CrossSite(t *testing.T) {
	sub := &model.Subscription{}
	drop := applyRoomInfo(sub, &model.RoomInfo{Found: true, Name: "chan", CrossSite: true})
	assert.False(t, drop)
	require.NotNil(t, sub.Room)
	assert.True(t, sub.Room.CrossSite)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=user-service`
Expected: FAIL — `got.CrossSite` false.

- [ ] **Step 3: Map the field** — in `buildLocalRoom`, add `CrossSite: sub.CrossSite,` to the `&model.SubscriptionRoom{...}` literal; in `applyRoomInfo`, add `CrossSite: info.CrossSite,` to its `&model.SubscriptionRoom{...}` literal.

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=user-service`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add user-service/service/subscriptions.go user-service/service/subscriptions_test.go
git commit -m "feat(user-service): surface room CrossSite in subscription.list payload"
```

---

### Task 10: `pkg/subject` route-mode + `RoomEventTargets` helper

**Why the gate:** publisher and client cannot deploy atomically. A plain flip breaks same-site delivery during the rollout window (server-ahead-of-client OR client-ahead-of-server both drop messages), and "client subscribes to both prefixes" would defeat the interest reduction (the global sub still propagates cross-gateway). The safe migration is a server route-mode: `global` (default, current behavior) → `dual` (local rooms published to BOTH prefixes so old and new clients both receive) → `local` (local-only; reduction achieved with the leaf filter). The client always subscribes by the `crossSite` flag.

**Files:**
- Modify: `pkg/subject/subject.go`
- Test: `pkg/subject/subject_test.go`

**Interfaces:**
- Produces: `type RoomRouteMode int` with `RouteGlobal`/`RouteDual`/`RouteLocal`; `ParseRoomRouteMode(s string) (RoomRouteMode, error)` (accepts `"global"`/`"dual"`/`"local"`, case-insensitive); `RoomEventTargets(roomID string, crossSite bool, mode RoomRouteMode) []string` (the subject(s) a room `.event` publish must fan out to). Reuses `RoomEvent(id, global bool)` from Task 2.

- [ ] **Step 1: Write the failing table test** in `pkg/subject/subject_test.go`:

```go
func TestRoomEventTargets(t *testing.T) {
	g := "chat.room.r1.event"
	l := "chat.local.room.r1.event"
	// cross-site rooms are ALWAYS global, in every mode
	assert.Equal(t, []string{g}, subject.RoomEventTargets("r1", true, subject.RouteGlobal))
	assert.Equal(t, []string{g}, subject.RoomEventTargets("r1", true, subject.RouteDual))
	assert.Equal(t, []string{g}, subject.RoomEventTargets("r1", true, subject.RouteLocal))
	// same-site rooms vary by mode
	assert.Equal(t, []string{g}, subject.RoomEventTargets("r1", false, subject.RouteGlobal))
	assert.Equal(t, []string{l, g}, subject.RoomEventTargets("r1", false, subject.RouteDual))
	assert.Equal(t, []string{l}, subject.RoomEventTargets("r1", false, subject.RouteLocal))
}

func TestParseRoomRouteMode(t *testing.T) {
	for in, want := range map[string]subject.RoomRouteMode{
		"global": subject.RouteGlobal, "dual": subject.RouteDual, "local": subject.RouteLocal,
		"GLOBAL": subject.RouteGlobal,
	} {
		got, err := subject.ParseRoomRouteMode(in)
		require.NoError(t, err)
		assert.Equal(t, want, got)
	}
	_, err := subject.ParseRoomRouteMode("bogus")
	require.Error(t, err)
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `make test SERVICE=subject`
Expected: FAIL — undefined identifiers.

- [ ] **Step 3: Implement** in `pkg/subject/subject.go`:

```go
// RoomRouteMode selects which namespace(s) a same-site room's events publish to.
// Cross-site rooms are always global regardless of mode. This gates a zero-gap
// rollout: global (current) → dual (migration) → local (reduction achieved).
type RoomRouteMode int

const (
	RouteGlobal RoomRouteMode = iota // same-site rooms → chat.room.> (default, no behavior change)
	RouteDual                        // same-site rooms → chat.local.room.> AND chat.room.> (migration window)
	RouteLocal                       // same-site rooms → chat.local.room.> only (final)
)

// ParseRoomRouteMode parses the ROOM_SUBJECT_MODE env value. Unknown → error (caller defaults to global).
func ParseRoomRouteMode(s string) (RoomRouteMode, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "global":
		return RouteGlobal, nil
	case "dual":
		return RouteDual, nil
	case "local":
		return RouteLocal, nil
	default:
		return RouteGlobal, fmt.Errorf("invalid room route mode %q (want global|dual|local)", s)
	}
}

// RoomEventTargets returns the room .event subject(s) to publish to, given the
// room's cross-site flag and the configured route mode. Order matters for dual:
// local first, global second (callers publish to all and fail on first error).
func RoomEventTargets(roomID string, crossSite bool, mode RoomRouteMode) []string {
	global := RoomEvent(roomID, true)
	if crossSite {
		return []string{global}
	}
	switch mode {
	case RouteLocal:
		return []string{RoomEvent(roomID, false)}
	case RouteDual:
		return []string{RoomEvent(roomID, false), global}
	default:
		return []string{global}
	}
}
```

(`strings` is already imported in this file; confirm.)

- [ ] **Step 4: Run to verify it passes**

Run: `make test SERVICE=subject` then `make lint`
Expected: PASS; lint clean.

- [ ] **Step 5: Commit**

```bash
git add pkg/subject/
git commit -m "feat(subject): RoomRouteMode + RoomEventTargets for gated local/global routing"
```

---

### Task 11: broadcast-worker routes room events via the mode gate

**Files:**
- Modify: `broadcast-worker/main.go` (or its `config.go`) — add `ROOM_SUBJECT_MODE` env, parse to `RoomRouteMode`, inject into `Handler`.
- Modify: `broadcast-worker/handler.go` (the three `subject.RoomEvent(..., true)` sites: ~463, 669, 818)
- Test: `broadcast-worker/handler_test.go`, `broadcast-worker/config_test.go`

**Interfaces:**
- Consumes: `subject.RoomEventTargets`, `subject.ParseRoomRouteMode`, `roommetacache.Meta.CrossSite` (Task 3). The `Handler` gains a `routeMode subject.RoomRouteMode` field set in its constructor.

- [ ] **Step 1: Write the failing tests** in `broadcast-worker/handler_test.go` (mirror the existing publish-capture harness; `NewHandler` gains a mode arg — thread a default of `subject.RouteGlobal` through existing test constructions):

```go
func TestBroadcast_GlobalMode_AlwaysGlobal(t *testing.T) {
	// handler routeMode = RouteGlobal, room CrossSite=false → subject chat.room.r1.event (unchanged)
	assert.Contains(t, capturedSubjects, "chat.room.r1.event")
	assert.NotContains(t, capturedSubjects, "chat.local.room.r1.event")
}
func TestBroadcast_LocalMode_SameSiteUsesLocal(t *testing.T) {
	// routeMode = RouteLocal, room CrossSite=false → chat.local.room.r1.event only
	assert.Contains(t, capturedSubjects, "chat.local.room.r1.event")
	assert.NotContains(t, capturedSubjects, "chat.room.r1.event")
}
func TestBroadcast_DualMode_SameSitePublishesBoth(t *testing.T) {
	// routeMode = RouteDual, room CrossSite=false → both subjects
	assert.Contains(t, capturedSubjects, "chat.local.room.r1.event")
	assert.Contains(t, capturedSubjects, "chat.room.r1.event")
}
func TestBroadcast_CrossSiteRoomAlwaysGlobal(t *testing.T) {
	// any mode, room CrossSite=true → chat.room.r1.event only
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `make test SERVICE=broadcast-worker`
Expected: FAIL — `NewHandler` arity / routing not implemented.

- [ ] **Step 3: Add config + route.** In config: add a `RoomSubjectMode string` field tagged `env:"ROOM_SUBJECT_MODE" envDefault:"global"`; in `main.go` parse it via `subject.ParseRoomRouteMode` (log-and-exit on error, per the fail-fast config rule) and pass the `RoomRouteMode` into `NewHandler`, stored as `h.routeMode`. Replace each of the 3 publish sites. Each currently does one `h.pub.Publish(ctx, subject.RoomEvent(id, true), payload)`; change to fan out:

```go
	for _, subj := range subject.RoomEventTargets(room.ID, room.CrossSite, h.routeMode) {
		if err := h.pub.Publish(ctx, subj, payload); err != nil {
			return fmt.Errorf("publish room event to %s: %w", subj, err)
		}
	}
```

For the site at ~818 that uses `meta` and returns the publish error directly, use `meta.ID`, `meta.CrossSite`, and return the loop's error. Confirm the `room` value at 463/669 carries `CrossSite` (it is a `model.Room` / store room struct — if a projection omits `crossSite`, add it; the metacache `Meta` already has it from Task 3).

- [ ] **Step 4: Run to verify they pass**

Run: `make test SERVICE=broadcast-worker` (covers the sonic wire-compat suite too) then `make lint`
Expected: PASS; lint clean. Default `global` mode ⇒ byte-identical to pre-change behavior.

- [ ] **Step 5: Commit**

```bash
git add broadcast-worker/
git commit -m "feat(broadcast-worker): route room events via ROOM_SUBJECT_MODE gate"
```

---

### Task 12: room-service + room-worker route their room-event publishers via the mode gate

**Files:**
- Modify: `room-service/handler.go` (`subject.RoomEvent(..., true)` at ~1435, 1809) + its config/`main.go` for `ROOM_SUBJECT_MODE`.
- Modify: `room-worker/handler.go` (`subject.RoomEvent(..., true)` at ~2234) + its config/`main.go`.
- Test: `room-service/handler_test.go`, `room-worker/handler_test.go`, config tests.

**Interfaces:**
- Consumes: `subject.RoomEventTargets`, `subject.ParseRoomRouteMode`, `model.Room.CrossSite`. Each service's handler gains a `routeMode subject.RoomRouteMode`.

- [ ] **Step 1: Write failing tests** in each file: with the handler's `routeMode=RouteLocal` and a `CrossSite:false` room, the publisher targets `chat.local.room.{id}.event`; with `RouteGlobal` (default) it targets `chat.room.{id}.event`; a `CrossSite:true` room is always global. Capture via the injected publish func. Thread a default `RouteGlobal` into existing handler constructions.

- [ ] **Step 2: Run to verify they fail**

Run: `make test SERVICE=room-service` and `make test SERVICE=room-worker`
Expected: FAIL.

- [ ] **Step 3: Add config + route.** Add a `RoomSubjectMode string` field tagged `env:"ROOM_SUBJECT_MODE" envDefault:"global"` to each service's config, parse via `subject.ParseRoomRouteMode` in `main.go` (fail-fast), inject into the handler as `routeMode`. At each publish site replace the single `Publish(subject.RoomEvent(roomID, true), …)` with a fan-out over `subject.RoomEventTargets(roomID, room.CrossSite, h.routeMode)`, attempting every target and returning the last error (so a dual-mode local-subject failure never skips the global publish; mirror Task 11 Step 3). Confirm the room value in scope carries `CrossSite` (add to the room projection on that path if explicit).

- [ ] **Step 4: Run to verify they pass**

Run: `make test SERVICE=room-service`, `make test SERVICE=room-worker`, `make lint`
Expected: PASS; lint clean; default `global` preserves current behavior.

- [ ] **Step 5: Commit**

```bash
git add room-service/ room-worker/
git commit -m "feat(rooms): route room-event publishers via ROOM_SUBJECT_MODE gate"
```

---

### Task 13: Backfill migration for existing cross-site rooms

> **DROPPED** (post-implementation decision): the deployment starts from fresh data, so there are no pre-existing unclassified rooms to migrate. Runtime classification at create/member-add covers every room. The `tools/backfill-crosssite` job was removed from this PR. This task is retained for historical context only.

**Files:**
- Create: a one-off under `data-migration/` (read that service's structure first to match its pattern).
- Test: migration test (unit where possible; integration is testcontainers-gated in this sandbox — write + vet under `-tags integration`, note non-execution).

**Interfaces:** none exported; operates directly on Mongo.

**Why:** before any site runs in `local`/`dual` mode, every already-cross-site room must have `crossSite=true`, or it would misroute to the filtered local namespace (fail-safe violation).

- [ ] **Step 1: Write the test first** — seed a `rooms` doc `{_id:r1, siteId:site-a}` with subscriptions whose members resolve to sites `{site-a, site-b}` (cross-site) and a second room `{_id:r2, siteId:site-a}` all on `site-a`; run the backfill; assert `r1.crossSite==true` and `r2.crossSite` unset/false.

- [ ] **Step 2: Run to verify it fails** (no migration yet).

- [ ] **Step 3: Implement.** For each room, determine whether any member's home site differs from `room.siteId`. First establish where a member's home site lives: `grep -rn "SiteID" pkg/model/subscription.go pkg/model/user.go` — if the subscription stores only the room-scoped `siteId`, resolve member accounts to `users` and read `user.siteId` in batches. Set `crossSite=true` in bulk (`UpdateMany`) for the matched room IDs. Idempotent and re-runnable. Follow the existing `data-migration/` job wiring (config, Mongo connect, logging).

- [ ] **Step 4: Verify** — `make test`/vet as available for the migration package; `make lint`.

- [ ] **Step 5: Commit**

```bash
git add data-migration/
git commit -m "feat(data-migration): backfill crossSite=true for existing cross-site rooms"
```

---

### Task 14: Local→global transition nudge

*(unchanged from the original plan — publishes a per-user `event.room.update` nudge to existing members when `processAddMembers` flips a room local→global, so connected clients re-subscribe. Offline/dropped clients self-correct via `subscription.list`.)*

**Files:**
- Modify: `room-worker/handler.go` (`processAddMembers` — after the Task 5 `SetRoomCrossSite` call)
- Test: `room-worker/handler_test.go`

- [ ] **Step 1: Write the failing tests:**

```go
func TestProcessAddMembers_NudgesExistingMembersOnFlip(t *testing.T) {
	// room previously local (loaded room CrossSite=false), adding a site-b member →
	// expect a publish on subject.UserRoomUpdate(existingMemberAccount)
}
func TestProcessAddMembers_NoNudgeWhenAlreadyGlobal(t *testing.T) {
	// loaded room CrossSite=true already → no nudge
}
```

- [ ] **Step 2: Run to verify they fail** — `make test SERVICE=room-worker`.

- [ ] **Step 3: Implement**, gating on the room's prior state (already loaded before the write). Only when `!room.CrossSite && len(accountsBySite) > 0`:

```go
	if !room.CrossSite && len(accountsBySite) > 0 {
		// Room just flipped local→global. Nudge existing members' always-on per-user
		// tree so connected clients re-fetch subscription.list and re-subscribe to the
		// global namespace. Best-effort: offline/dropped clients self-correct on reconnect.
		evt := model.RoomMetadataUpdateEvent{RoomID: req.RoomID, Timestamp: now.UnixMilli()}
		data, _ := json.Marshal(evt)
		for _, acc := range existingMemberAccounts { // members present BEFORE this add
			if err := h.publish(ctx, subject.UserRoomUpdate(acc), data, ""); err != nil {
				slog.WarnContext(ctx, "room-locality nudge publish failed", "account", acc, "roomId", req.RoomID)
			}
		}
	}
```

Derive `existingMemberAccounts` from the pre-add membership `loadAddMemberInputs` already reads — no new read. Confirm the client already treats `event.room.update` as a trigger to refresh its subscription list; the nudge only needs to prompt a refresh (the flag is authoritative).

- [ ] **Step 4: Run to verify they pass** — `make test SERVICE=room-worker`, `make lint`.

- [ ] **Step 5: Commit**

```bash
git add room-worker/handler.go room-worker/handler_test.go
git commit -m "feat(room-worker): nudge existing members when a room flips local->global"
```

---

### Task 15: chat-frontend subscribes by the `crossSite` flag

**Files:**
- Modify: `chat-frontend/src/api/_transport/subjects.ts` (the `chat.room.${roomId}.event` builder at ~line 29)
- Modify: `chat-frontend/src/context/RoomEventsContext/` (the code that subscribes per room — it must pick the subject by the room's `crossSite` flag and re-subscribe when the flag changes)
- Test: the matching `*.test.jsx`/`*.test.ts` (vitest) — `chat-frontend/src/context/RoomEventsContext/RoomEventsContext.test.jsx` already asserts on `chat.room.g1.event` subjects; extend it.

**Interfaces:**
- Consumes: `room.crossSite` (the `SubscriptionRoom.crossSite` field now returned by `subscription.list`, Task 9). Cross-site room → `chat.room.{id}.event` (unchanged); same-site room → `chat.local.room.{id}.event`.

- [ ] **Step 1: Read the frontend conventions first.** `cat chat-frontend/CLAUDE.md` and read `subjects.ts` + the RoomEventsContext subscribe logic and its existing test, so the change matches house style (this is a JS/TS + vitest codebase, not Go — use its `npm`/`vitest` scripts from `package.json`, not `make`).

- [ ] **Step 2: Write the failing vitest** in `RoomEventsContext.test.jsx` (mirror the existing subject-assertion tests): given a subscription list where room `L1` has `crossSite:false` and room `G1` has `crossSite:true`, assert the context subscribes to `chat.local.room.L1.event` and `chat.room.G1.event`. Add a case: when a room's `crossSite` flips false→true on a list refresh, the context unsubscribes `chat.local.room.…` and subscribes `chat.room.…`.

- [ ] **Step 3: Run to verify it fails** — `cd chat-frontend && npm test -- RoomEventsContext` (or the repo's vitest invocation).

- [ ] **Step 4: Implement.** In `subjects.ts` add/adjust the builder:

```ts
export function roomEventSubject(roomId: string, crossSite: boolean): string {
  return crossSite ? `chat.room.${roomId}.event` : `chat.local.room.${roomId}.event`
}
```

Update RoomEventsContext to compute each room's subject via `roomEventSubject(room.id, room.crossSite)`, and key its subscribe/unsubscribe bookkeeping on the computed subject so that a `crossSite` change on a `subscription.list` refresh drops the old subscription and creates the new one. Preserve existing behavior for cross-site rooms (they resolve to the unchanged `chat.room.…` subject, so `crossSite` defaulting to falsy must NOT silently route a genuinely-global room to local — confirm the flag is present in the room object the context reads; if absent for a room, default to the GLOBAL subject, matching the server's fail-safe).

- [ ] **Step 5: Run to verify it passes** — `cd chat-frontend && npm test` (run the affected suites; ensure lint/format via the frontend's own tooling, e.g. `npm run lint`).

- [ ] **Step 6: Commit**

```bash
git add chat-frontend/src/api/_transport/subjects.ts chat-frontend/src/context/RoomEventsContext/
git commit -m "feat(chat-frontend): subscribe rooms to local/global namespace by crossSite"
```

---

### Task 16: Permissions, docs, and rollout runbook

**Files:**
- Modify: `auth-service/handler.go` (grants comment ~line 305)
- Modify: `docker-local/setup.sh` (dev JWT sub-allow list)
- Modify: `docs/client-api.md` (+ `docs/client-api/request-reply.md`, `docs/client-api/events.md` if the room payload / subscription.list table appears there)
- Create/Modify: a rollout runbook section (in the design doc or a new `docs/` note)

**Interfaces:** none (config + docs).

- [ ] **Step 1:** `auth-service/handler.go` — add `chat.local.room.>` to the documented `Sub allow:` line, noting the live grant is the platform-team scoped-signing-key template and the leaf node denies `chat.local.>` from gateway interest.

- [ ] **Step 2:** `docker-local/setup.sh` — add `chat.local.room.>` to the dev JWT subscribe allow list so same-site delivery works locally in `local`/`dual` mode.

- [ ] **Step 3:** `docs/client-api.md` — add `crossSite` (type `bool`) to the room object / `subscription.list` payload field table; document that a same-site room's `chat.room.{roomID}.>` events move to `chat.local.room.{roomID}.>`; update §2.1 to list the `chat.local.room.>` subscribe grant. Update the derived views if the table appears there.

- [ ] **Step 4: Rollout runbook** — document the `ROOM_SUBJECT_MODE` migration explicitly:
  1. Platform adds `chat.local.room.>` to the production subscribe template. (No data backfill — deployments start from fresh data; every room is classified at creation/member-add.)
  2. Deploy all publisher services with `ROOM_SUBJECT_MODE=dual` (same-site rooms now published to BOTH prefixes — old clients on `chat.room.>` and new clients on `chat.local.room.>` both receive; zero gap).
  3. Deploy the frontend (Task 15): clients now subscribe same-site rooms on `chat.local.room.>`.
  4. Once frontend rollout is complete, set publishers to `ROOM_SUBJECT_MODE=local` (drop the redundant global publish for same-site rooms).
  5. Platform enables the leaf-node deny for `chat.local.>` → interest-map reduction realized. (Safe at any point from step 4 on; harmless before.)
  Rollback at any step: set `ROOM_SUBJECT_MODE=global` — publishers revert to the current all-global behavior; clients subscribing `chat.local.…` simply receive nothing on same-site rooms until the next `subscription.list` refresh, so pair a rollback with reverting the frontend or accept a brief same-site gap.

- [ ] **Step 5: Verify + commit**

Run: `make lint`; `grep -rn "chat.local.room" docs/ auth-service/ docker-local/`
Expected: prefix present in all three; lint clean.

```bash
git add auth-service/handler.go docker-local/setup.sh docs/client-api.md docs/client-api/ docs/superpowers/
git commit -m "docs: local/global room namespaces, chat.local.room.> grant, ROOM_SUBJECT_MODE rollout"
```

---

## Self-Review

**Spec coverage:**
- Subject namespace scheme → Task 2. ✓
- `Room.CrossSite` source of truth → Tasks 1, 4, 5, 6 (+ 2 fail-safe fixes: channel & serverCreateDM create paths). ✓
- Write path at federation points → Tasks 5 (add), 6 (create); room-worker only (room-service is an RPC front). ✓
- Publisher routing (broadcast-worker + room-service/worker), gated → Tasks 10 (helper+mode), 11, 12. ✓
- Client payload + subscribe → Tasks 7, 8, 9 (payload) + Task 15 (frontend subscribe). ✓
- Transition nudge (per-user, gap-tolerant) → Task 14. ✓
- Backfill of pre-existing cross-site rooms → Task 13. ✓
- Auth grant + docs + rollout → Task 16. ✓
- Non-goals (per-user tree, demote) → excluded. ✓

**Rollout-safety correction (this revision):** the original plan wrongly claimed Tasks 10-11 were behavior-preserving; they are the flip, and the required client-side subscribe change was missing. Fixed by: (a) a server `ROOM_SUBJECT_MODE` gate defaulting to `global` (zero behavior change until ops opts in) with a `dual` migration mode for a zero-gap cutover; (b) an explicit chat-frontend task to subscribe by the `crossSite` flag; (c) a step-by-step rollout runbook (Task 16 Step 4). Dual-publish is used ONLY as a transient migration mode (not per-room transitions — that remains gap-tolerant per the spec).

**Type consistency:** `CrossSite *bool` (tri-state; nil/absent == global fail-safe) across `Room`/`SubscriptionRoom`/`EnrichedSubscription`/`RoomInfo`/`roommetacache.Meta`. `RoomEventTargets(roomID, crossSite, mode)` and `RoomRouteMode`/`ParseRoomRouteMode` consistent across Tasks 10-12. Frontend `roomEventSubject(roomId, crossSite)` maps crossSite→global exactly as the server's `RoomEventTargets` does for the non-dual case.
