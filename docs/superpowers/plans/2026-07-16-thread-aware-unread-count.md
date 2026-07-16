# Thread-Aware Unread Count Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend `user-service`'s `subscription.count` (unread=true) so a room also counts as unread when it has ≥1 unread thread — at most +1 per room, read-side only.

**Architecture:** After the existing room-level unread pass, collect the rooms that came out *read* (`pendingRooms`), resolve their followed threads' last-activity per owning site via `GetThreadRoomInfoBatch`, and bump each such room at most once. Existence-only, so already-unread rooms are never thread-checked and each room stops at its first unread thread. No new persisted state.

**Tech Stack:** Go 1.25, NATS request/reply, MongoDB (`go.mongodb.org/mongo-driver/v2`), `go.uber.org/mock`, `testify`, testcontainers.

## Global Constraints

- Go 1.25; single root `go.mod`. Use `make` targets, never raw `go`.
- TDD: write the failing test first, confirm red, then implement. Table-driven where multiple variations.
- Unit tests in `package service` / `package mongorepo`; mocks in `service/mocks` (never hand-edit generated mocks).
- Integration tests tagged `//go:build integration`, containers from `pkg/testutil`.
- `unread(lastSeen *time.Time, ms *int64) bool` is the single unread predicate (`subscriptions.go:455`) — reuse it, do not reinvent.
- `chunkStrings` and `threadInfoBatchChunk` (=500) already exist in `service/threadunread.go` (same package) — reuse.
- `maxSiteFanout` (=8) bounds concurrent per-site RPCs — reuse.
- Best-effort degradation: a thread read/resolution failure logs WARN with `request_id` and contributes 0; it never fails the RPC or discards the room-level count.
- Response shape stays `{count}` — no `client-api.md` schema-table change (prose note only).

---

### Task 1: Add `RoomID` to `ThreadUnreadRow`

**Files:**
- Modify: `pkg/model/threadsubscription.go:32-38`
- Test: `pkg/model/model_test.go:4300-4307`

**Interfaces:**
- Produces: `model.ThreadUnreadRow.RoomID string` (json/bson `roomId`) — consumed by Tasks 2 and 3.

- [ ] **Step 1: Update the round-trip test to include `RoomID`**

In `pkg/model/model_test.go`, replace `TestThreadUnreadRowJSON`:

```go
func TestThreadUnreadRowJSON(t *testing.T) {
	seen := time.UnixMilli(1717000000000).UTC()
	r := model.ThreadUnreadRow{
		ThreadRoomID: "tr1", RoomID: "r1", SiteID: "site-a", RoomType: model.RoomTypeDM,
		LastSeenAt: &seen, HasMention: true,
	}
	roundTrip(t, &r, &model.ThreadUnreadRow{})
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `make test SERVICE=pkg/model` (or `go test ./pkg/model/ -run TestThreadUnreadRowJSON`)
Expected: FAIL — `unknown field RoomID in struct literal`.

- [ ] **Step 3: Add the field**

In `pkg/model/threadsubscription.go`, add `RoomID` to `ThreadUnreadRow`:

```go
type ThreadUnreadRow struct {
	ThreadRoomID string     `json:"threadRoomId" bson:"threadRoomId"`
	RoomID       string     `json:"roomId"       bson:"roomId"`
	SiteID       string     `json:"siteId"       bson:"siteId"`
	RoomType     RoomType   `json:"roomType"     bson:"roomType"`
	LastSeenAt   *time.Time `json:"lastSeenAt"   bson:"lastSeenAt"`
	HasMention   bool       `json:"hasMention"   bson:"hasMention"`
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `go test ./pkg/model/ -run TestThreadUnreadRowJSON -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/model/threadsubscription.go pkg/model/model_test.go
git commit -m "feat(model): add RoomID to ThreadUnreadRow projection"
```

---

### Task 2: Project `roomId` in `ListByAccount`

**Files:**
- Modify: `user-service/mongorepo/threadsubscriptions.go:74-81`
- Test: `user-service/mongorepo/threadsubscriptions_test.go:26-63`

**Interfaces:**
- Consumes: `model.ThreadUnreadRow.RoomID` (Task 1).
- Produces: `ListByAccount` rows now carry `RoomID` — consumed by Task 3.

- [ ] **Step 1: Assert `RoomID` in the existing integration test**

In `TestThreadSubscriptionRepo_ListByAccount`, after the existing per-site assertions (around line 57), add:

```go
	assert.Equal(t, "r1", bySite["site-a"].RoomID)
	assert.Equal(t, "r2", bySite["site-b"].RoomID)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `make test-integration SERVICE=user-service`
Expected: FAIL — `bySite["site-a"].RoomID` is `""` (projection does not emit `roomId` yet).

- [ ] **Step 3: Add `roomId` to the projection**

In `user-service/mongorepo/threadsubscriptions.go`, add one line to the final `$project`:

```go
		bson.M{"$project": bson.M{
			"_id":          0,
			"threadRoomId": 1,
			"roomId":       1,
			"siteId":       1,
			"lastSeenAt":   1,
			"hasMention":   1,
			"roomType":     bson.M{"$arrayElemAt": bson.A{"$sub.roomType", 0}},
		}},
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `make test-integration SERVICE=user-service`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add user-service/mongorepo/threadsubscriptions.go user-service/mongorepo/threadsubscriptions_test.go
git commit -m "feat(user-service): project roomId in thread-sub ListByAccount"
```

---

### Task 3: Thread-aware `countUnread`

**Files:**
- Modify: `user-service/service/subscriptions.go:560-646` (`countUnread`) + add `countThreadOnlyUnread`
- Modify: `user-service/service/service_test.go:17-33` (`newSvc` — add ListByAccount default)
- Test: `user-service/service/subscriptions_test.go` (new helper + new tests)

**Interfaces:**
- Consumes: `s.threadSubs.ListByAccount(ctx, account) ([]model.ThreadUnreadRow, error)`; `s.rooms.GetThreadRoomInfoBatch(ctx, siteID, ids) ([]model.ThreadRoomInfo, error)`; `unread`, `chunkStrings`, `threadInfoBatchChunk`, `maxSiteFanout`.
- Produces: `countUnread` returns `{count}` = room-level unread + thread-only-unread rooms (each room ≤ 1).

- [ ] **Step 1: Add the ListByAccount default to `newSvc` so existing count tests stay green**

Existing count tests that leave rooms *read* (e.g. `TestCountUnread_MultiSite`, `TestCountUnread_AllRead`) will now reach the thread phase. Default the new call to a no-op in `newSvc` (mirrors the existing `history.RoomsGet` default). In `user-service/service/service_test.go`, after line 31:

```go
	// countUnread's thread phase reads thread-subs; default to none so room-count
	// tests that don't exercise threads need no per-test stub.
	threadSubs.EXPECT().ListByAccount(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
```

- [ ] **Step 2: Add a count-test helper exposing the thread + room mocks**

In `user-service/service/subscriptions_test.go`, add:

```go
// newCountSvc builds a service exposing the subscription, room, and thread-sub
// mocks the thread-aware unread tests drive. maxSubs is large; per-test GetActiveSubscriptions
// stubs control the fetched page directly.
func newCountSvc(t *testing.T) (*UserService, *mocks.MockSubscriptionRepository, *mocks.MockRoomClient, *mocks.MockThreadSubscriptionRepository) {
	t.Helper()
	ctrl := gomock.NewController(t)
	subs := mocks.NewMockSubscriptionRepository(ctrl)
	users := mocks.NewMockUserRepository(ctrl)
	apps := mocks.NewMockAppRepository(ctrl)
	rooms := mocks.NewMockRoomClient(ctrl)
	history := mocks.NewMockHistoryClient(ctrl)
	presence := mocks.NewMockPresenceClient(ctrl)
	pub := mocks.NewMockEventPublisher(ctrl)
	threadSubs := mocks.NewMockThreadSubscriptionRepository(ctrl)
	cfg := &config.Config{SiteID: "site-a", AllSiteIDs: []string{"site-a", "site-b"}, MaxSubscriptionLimit: 1000, DefaultSubscriptionLimit: 40, MaxAppsLimit: 100, DefaultAppsLimit: 20, MaxAccountNames: 100}
	return New(subs, users, apps, threadSubs, rooms, history, presence, pub, cfg), subs, rooms, threadSubs
}
```

- [ ] **Step 3: Write the failing thread-aware tests**

Add to `user-service/service/subscriptions_test.go`:

```go
func TestCountUnread_ReadRoomBumpedByUnreadThread(t *testing.T) {
	svc, subs, rooms, threadSubs := newCountSvc(t)
	seen := time.UnixMilli(100).UTC()
	older := time.UnixMilli(50).UTC()
	subs.EXPECT().CountActiveSubscriptions(gomock.Any(), "alice").Return(1, nil)
	// One LOCAL room, read at the message level (lastMsgAt older than lastSeen).
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", 1).Return([]model.EnrichedSubscription{
		{Subscription: model.Subscription{RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen}, LastMsgAt: &older},
	}, nil)
	// One followed thread in r1, unread (thread lastMsgAt 200 > lastSeen 100).
	threadSubs.EXPECT().ListByAccount(gomock.Any(), "alice").Return([]model.ThreadUnreadRow{
		{ThreadRoomID: "tr1", RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen},
	}, nil)
	rooms.EXPECT().GetThreadRoomInfoBatch(gomock.Any(), "site-a", []string{"tr1"}).
		Return([]model.ThreadRoomInfo{{ThreadRoomID: "tr1", Found: true, LastMsgAt: 200}}, nil)
	yes := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &yes})
	require.NoError(t, err)
	assert.Equal(t, 1, resp.Count)
}

func TestCountUnread_AlreadyUnreadRoomNotDoubleCounted(t *testing.T) {
	svc, subs, rooms, threadSubs := newCountSvc(t)
	seen := time.UnixMilli(100).UTC()
	newer := time.UnixMilli(300).UTC()
	subs.EXPECT().CountActiveSubscriptions(gomock.Any(), "alice").Return(1, nil)
	// r1 is already room-level unread → must not be thread-checked, contributes exactly 1.
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", 1).Return([]model.EnrichedSubscription{
		{Subscription: model.Subscription{RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen}, LastMsgAt: &newer},
	}, nil)
	// Every room already unread ⇒ pendingRooms empty ⇒ ListByAccount never called.
	threadSubs.EXPECT().ListByAccount(gomock.Any(), gomock.Any()).Times(0)
	rooms.EXPECT().GetThreadRoomInfoBatch(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	yes := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &yes})
	require.NoError(t, err)
	assert.Equal(t, 1, resp.Count)
}

func TestCountUnread_MultipleUnreadThreadsCountOnce(t *testing.T) {
	svc, subs, rooms, threadSubs := newCountSvc(t)
	seen := time.UnixMilli(100).UTC()
	older := time.UnixMilli(50).UTC()
	subs.EXPECT().CountActiveSubscriptions(gomock.Any(), "alice").Return(1, nil)
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", 1).Return([]model.EnrichedSubscription{
		{Subscription: model.Subscription{RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen}, LastMsgAt: &older},
	}, nil)
	// Three unread threads, all in r1 → +1 total.
	threadSubs.EXPECT().ListByAccount(gomock.Any(), "alice").Return([]model.ThreadUnreadRow{
		{ThreadRoomID: "tr1", RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen},
		{ThreadRoomID: "tr2", RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen},
		{ThreadRoomID: "tr3", RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen},
	}, nil)
	rooms.EXPECT().GetThreadRoomInfoBatch(gomock.Any(), "site-a", gomock.InAnyOrder([]string{"tr1", "tr2", "tr3"})).
		Return([]model.ThreadRoomInfo{
			{ThreadRoomID: "tr1", Found: true, LastMsgAt: 200},
			{ThreadRoomID: "tr2", Found: true, LastMsgAt: 200},
			{ThreadRoomID: "tr3", Found: true, LastMsgAt: 200},
		}, nil)
	yes := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &yes})
	require.NoError(t, err)
	assert.Equal(t, 1, resp.Count)
}

func TestCountUnread_ThreadInRoomOutsidePageIgnored(t *testing.T) {
	svc, subs, rooms, threadSubs := newCountSvc(t)
	seen := time.UnixMilli(100).UTC()
	older := time.UnixMilli(50).UTC()
	subs.EXPECT().CountActiveSubscriptions(gomock.Any(), "alice").Return(1, nil)
	// Fetched page contains only r1 (read).
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", 1).Return([]model.EnrichedSubscription{
		{Subscription: model.Subscription{RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen}, LastMsgAt: &older},
	}, nil)
	// Thread lives in rX, which is NOT in the fetched page → must be filtered out, no batch call.
	threadSubs.EXPECT().ListByAccount(gomock.Any(), "alice").Return([]model.ThreadUnreadRow{
		{ThreadRoomID: "trX", RoomID: "rX", SiteID: "site-a", LastSeenAt: &seen},
	}, nil)
	rooms.EXPECT().GetThreadRoomInfoBatch(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	yes := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &yes})
	require.NoError(t, err)
	assert.Equal(t, 0, resp.Count)
}

func TestCountUnread_CrossSiteThreadResolution(t *testing.T) {
	svc, subs, rooms, threadSubs := newCountSvc(t)
	seen := time.UnixMilli(100).UTC()
	older := int64(50)
	subs.EXPECT().CountActiveSubscriptions(gomock.Any(), "alice").Return(1, nil)
	// r1 lives on site-b, read at the message level.
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", 1).Return([]model.EnrichedSubscription{
		{Subscription: model.Subscription{RoomID: "r1", SiteID: "site-b", LastSeenAt: &seen}},
	}, nil)
	rooms.EXPECT().GetRoomsInfo(gomock.Any(), "site-b", []string{"r1"}).
		Return([]model.RoomInfo{{RoomID: "r1", Found: true, LastMsgAt: &older}}, nil)
	// A followed thread in r1 on site-b, unread → resolved via the site-b batch.
	threadSubs.EXPECT().ListByAccount(gomock.Any(), "alice").Return([]model.ThreadUnreadRow{
		{ThreadRoomID: "tr1", RoomID: "r1", SiteID: "site-b", LastSeenAt: &seen},
	}, nil)
	rooms.EXPECT().GetThreadRoomInfoBatch(gomock.Any(), "site-b", []string{"tr1"}).
		Return([]model.ThreadRoomInfo{{ThreadRoomID: "tr1", Found: true, LastMsgAt: 200}}, nil)
	yes := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &yes})
	require.NoError(t, err)
	assert.Equal(t, 1, resp.Count)
}

func TestCountUnread_ThreadBatchFailureDegrades(t *testing.T) {
	svc, subs, rooms, threadSubs := newCountSvc(t)
	seen := time.UnixMilli(100).UTC()
	older := time.UnixMilli(50).UTC()
	subs.EXPECT().CountActiveSubscriptions(gomock.Any(), "alice").Return(1, nil)
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", 1).Return([]model.EnrichedSubscription{
		{Subscription: model.Subscription{RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen}, LastMsgAt: &older},
	}, nil)
	threadSubs.EXPECT().ListByAccount(gomock.Any(), "alice").Return([]model.ThreadUnreadRow{
		{ThreadRoomID: "tr1", RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen},
	}, nil)
	// Thread resolution fails → room un-bumped, count degrades to 0, no error.
	rooms.EXPECT().GetThreadRoomInfoBatch(gomock.Any(), "site-a", []string{"tr1"}).
		Return(nil, errors.New("down"))
	yes := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &yes})
	require.NoError(t, err)
	assert.Equal(t, 0, resp.Count)
}

func TestCountUnread_ThreadListErrorDegrades(t *testing.T) {
	svc, subs, rooms, threadSubs := newCountSvc(t)
	seen := time.UnixMilli(100).UTC()
	older := time.UnixMilli(50).UTC()
	subs.EXPECT().CountActiveSubscriptions(gomock.Any(), "alice").Return(1, nil)
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", 1).Return([]model.EnrichedSubscription{
		{Subscription: model.Subscription{RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen}, LastMsgAt: &older},
	}, nil)
	// Local thread-sub read fails → degrade to the room-level count (0), never error.
	threadSubs.EXPECT().ListByAccount(gomock.Any(), "alice").Return(nil, errors.New("db down"))
	rooms.EXPECT().GetThreadRoomInfoBatch(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	yes := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &yes})
	require.NoError(t, err)
	assert.Equal(t, 0, resp.Count)
}

func TestCountUnread_MutedRoomThreadExcluded(t *testing.T) {
	svc, subs, rooms, threadSubs := newCountSvc(t)
	seen := time.UnixMilli(100).UTC()
	older := time.UnixMilli(50).UTC()
	subs.EXPECT().CountActiveSubscriptions(gomock.Any(), "alice").Return(1, nil)
	// GetActiveSubscriptions already excludes muted rooms, so a muted room's parent
	// is never in the fetched page. Only r1 (unmuted, read) is returned.
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", 1).Return([]model.EnrichedSubscription{
		{Subscription: model.Subscription{RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen}, LastMsgAt: &older},
	}, nil)
	// A thread in the muted room "rMuted" is returned by ListByAccount but its room is
	// not in the page → filtered out, no batch, no bump.
	threadSubs.EXPECT().ListByAccount(gomock.Any(), "alice").Return([]model.ThreadUnreadRow{
		{ThreadRoomID: "trM", RoomID: "rMuted", SiteID: "site-a", LastSeenAt: &seen},
	}, nil)
	rooms.EXPECT().GetThreadRoomInfoBatch(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	yes := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &yes})
	require.NoError(t, err)
	assert.Equal(t, 0, resp.Count)
}
```

- [ ] **Step 4: Run the new tests to verify they fail**

Run: `make test SERVICE=user-service`
Expected: FAIL — the thread phase does not exist yet; `TestCountUnread_ReadRoomBumpedByUnreadThread` gets 0 (expected 1) and the `GetThreadRoomInfoBatch` expectations are unmet.

- [ ] **Step 5: Rewrite `countUnread` and add `countThreadOnlyUnread`**

In `user-service/service/subscriptions.go`, replace the body of `countUnread` (currently `560-646`) with the version below. Two behavior changes: (a) collect `pendingRooms` (read rooms) in both the local loop and each cross-site goroutine; (b) always run the thread phase before returning.

```go
// countUnread counts active rooms with unread activity. A room counts once if its
// messages are unread (LOCAL from the $lookup baseline, CROSS-SITE via per-site
// GetRoomsInfo) OR it is message-read but has >=1 unread followed thread. Rooms that
// came out read feed the thread phase (countThreadOnlyUnread); everything degrades
// best-effort — an unreachable site is skipped rather than nuking the count to total.
func (s *UserService) countUnread(ctx context.Context, account string, total int) (*models.CountResponse, error) {
	// Short-circuit zero: min(0, maxSubs)=0 would build a $limit:0 MongoDB rejects.
	if total == 0 {
		return &models.CountResponse{Count: 0}, nil
	}
	// Cap at maxSubs — query-side total can exceed the cap; min keeps the fetch bounded and consistent with the list endpoints.
	subs, err := s.subs.GetActiveSubscriptions(ctx, account, min(total, s.maxSubs))
	if err != nil {
		return nil, fmt.Errorf("count unread: %w", err)
	}

	// LOCAL subs carry room.lastMsgAt on the $lookup baseline — count them with no RPC.
	// Only CROSS-SITE subs need the per-site GetRoomsInfo RPC (their room docs live remotely).
	// pendingRooms collects rooms that came out READ (roomID -> siteID) for the thread phase.
	unreadTotal := 0
	pendingRooms := map[string]string{}
	crossBySite := map[string][]model.EnrichedSubscription{}
	roomIDsBySite := map[string][]string{}
	for i := range subs {
		if subs[i].SiteID == s.siteID {
			if unread(subs[i].LastSeenAt, timeutil.TimeToMillis(subs[i].LastMsgAt)) {
				unreadTotal++
			} else {
				pendingRooms[subs[i].RoomID] = s.siteID
			}
			continue
		}
		site := subs[i].SiteID
		crossBySite[site] = append(crossBySite[site], subs[i])
		roomIDsBySite[site] = append(roomIDsBySite[site], subs[i].RoomID)
	}

	if len(crossBySite) > 0 {
		sites := make([]string, 0, len(crossBySite))
		for site := range crossBySite {
			sites = append(sites, site)
		}
		// Per-site degradation (matches the list path's enrichCrossSite): a failed site is
		// SKIPPED — its subs drop out of the count and out of pendingRooms — while local subs
		// and the sites that did respond still contribute. results[i] is written by exactly
		// one goroutine.
		type siteCount struct {
			unread    int
			readRooms []string
		}
		results := make([]siteCount, len(sites))
		var wg sync.WaitGroup
		sem := make(chan struct{}, maxSiteFanout) // bound concurrent per-site RPCs
		for i, site := range sites {
			// Client already gone — stop firing further ~5s RPCs.
			if ctx.Err() != nil {
				break
			}
			wg.Add(1)
			sem <- struct{}{}
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				if ctx.Err() != nil {
					return
				}
				infos, err := s.rooms.GetRoomsInfo(ctx, site, roomIDsBySite[site])
				if err != nil {
					// Skip this site rather than nuking the whole count to total.
					slog.WarnContext(ctx, "unread count degraded for site", "account", account, "site", site, "request_id", natsutil.RequestIDFromContext(ctx), "error", err)
					return
				}
				lastMsg := make(map[string]*int64, len(infos))
				for k := range infos {
					// Mirror the list path (applyRoomInfo): a not-found or soft-deleted
					// (^Del-) room must not contribute to the count, even though the RPC
					// still returns a stale lastMsgAt for a room soft-deleted at its origin.
					if !infos[k].Found || strings.HasPrefix(infos[k].Name, deletedRoomNamePrefix) {
						continue
					}
					lastMsg[infos[k].RoomID] = infos[k].LastMsgAt
				}
				n := 0
				var read []string
				siteSubs := crossBySite[site]
				for j := range siteSubs {
					rid := siteSubs[j].RoomID
					// Not-found / soft-deleted rooms are absent from lastMsg — neither
					// counted nor a thread candidate.
					if _, ok := lastMsg[rid]; !ok {
						continue
					}
					if unread(siteSubs[j].LastSeenAt, lastMsg[rid]) {
						n++
					} else {
						read = append(read, rid)
					}
				}
				results[i] = siteCount{unread: n, readRooms: read}
			}()
		}
		wg.Wait()
		for i := range results {
			unreadTotal += results[i].unread
			for _, rid := range results[i].readRooms {
				pendingRooms[rid] = sites[i]
			}
		}
	}

	unreadTotal += s.countThreadOnlyUnread(ctx, account, pendingRooms)
	return &models.CountResponse{Count: unreadTotal}, nil
}

// countThreadOnlyUnread returns how many pendingRooms (rooms already READ at the message
// level, roomID -> siteID) have >=1 unread followed thread — at most +1 per room. Thread
// last-activity is resolved per owning site via GetThreadRoomInfoBatch, degrading like the
// room pass: a failed site contributes nothing. A ListByAccount failure logs and returns 0
// so a thread-subsystem hiccup never discards the established room-level count.
func (s *UserService) countThreadOnlyUnread(ctx context.Context, account string, pendingRooms map[string]string) int {
	if len(pendingRooms) == 0 {
		return 0
	}
	rows, err := s.threadSubs.ListByAccount(ctx, account)
	if err != nil {
		slog.WarnContext(ctx, "unread count: thread subscriptions read failed", "account", account, "request_id", natsutil.RequestIDFromContext(ctx), "error", err)
		return 0
	}

	// Keep only threads whose parent room is a pending (read) candidate; group by owning site.
	type threadCand struct {
		roomID     string
		lastSeenAt *time.Time
	}
	idsBySite := map[string][]string{}
	byThread := make(map[string]threadCand, len(rows))
	for i := range rows {
		if _, ok := pendingRooms[rows[i].RoomID]; !ok {
			continue
		}
		idsBySite[rows[i].SiteID] = append(idsBySite[rows[i].SiteID], rows[i].ThreadRoomID)
		byThread[rows[i].ThreadRoomID] = threadCand{roomID: rows[i].RoomID, lastSeenAt: rows[i].LastSeenAt}
	}
	if len(idsBySite) == 0 {
		return 0
	}

	sites := make([]string, 0, len(idsBySite))
	for site := range idsBySite {
		sites = append(sites, site)
	}
	type siteResult struct {
		infos  []model.ThreadRoomInfo
		failed bool
	}
	results := make([]siteResult, len(sites))
	var wg sync.WaitGroup
	sem := make(chan struct{}, maxSiteFanout)
	for i, site := range sites {
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			for _, chunk := range chunkStrings(idsBySite[site], threadInfoBatchChunk) {
				if ctx.Err() != nil {
					results[i].failed = true
					return
				}
				infos, err := s.rooms.GetThreadRoomInfoBatch(ctx, site, chunk)
				if err != nil {
					slog.WarnContext(ctx, "unread count: thread info site degraded", "account", account, "site", site, "request_id", natsutil.RequestIDFromContext(ctx), "error", err)
					results[i].failed = true
					return
				}
				results[i].infos = append(results[i].infos, infos...)
			}
		}()
	}
	wg.Wait()

	// Existence per room: a room bumps at most once; skip a room's remaining threads once bumped.
	bumped := map[string]bool{}
	for i := range results {
		if results[i].failed {
			continue
		}
		for _, info := range results[i].infos {
			if !info.Found {
				continue
			}
			cand, ok := byThread[info.ThreadRoomID]
			if !ok || bumped[cand.roomID] {
				continue
			}
			ms := info.LastMsgAt
			if unread(cand.lastSeenAt, &ms) {
				bumped[cand.roomID] = true
			}
		}
	}
	return len(bumped)
}
```

- [ ] **Step 6: Run the full user-service unit suite to verify it passes**

Run: `make test SERVICE=user-service`
Expected: PASS — the new thread tests pass and every existing `TestCountUnread_*` still passes (read-room tests hit the ListByAccount default returning no threads).

- [ ] **Step 7: Lint**

Run: `make lint`
Expected: clean (no unused vars, imports unchanged — `sync`, `slog`, `strings`, `natsutil`, `timeutil`, `model` are all already imported).

- [ ] **Step 8: Commit**

```bash
git add user-service/service/subscriptions.go user-service/service/service_test.go user-service/service/subscriptions_test.go
git commit -m "feat(user-service): count rooms with unread threads in subscription.count"
```

---

### Task 4: Document the semantic in `client-api.md`

**Files:**
- Modify: `docs/client-api.md` (the `subscription.count` section, ~line 4620-4657)

**Interfaces:** none (docs only).

- [ ] **Step 1: Read the current `subscription.count` section**

Run: open `docs/client-api.md` around line 4620 and locate the prose describing the unread "active set" and per-site degradation.

- [ ] **Step 2: Add the thread note**

Append a sentence to the unread-semantics prose (adjust surrounding wording to fit; do not change the request/response field tables or the JSON example):

```markdown
When `unread` is true, a room also counts as unread if it has at least one unread
followed thread, even when its own messages are all read — at most **+1 per room**
(existence, not a per-thread count). Muted rooms are excluded (as with room-level
unread). Thread state for message-read rooms is resolved per owning site and degrades
best-effort: if a site's thread lookup fails, its rooms are simply not bumped.
```

- [ ] **Step 3: Verify no derived-view drift**

The request/response schema for `subscription.count` is unchanged (`{count}`), so `docs/client-api/request-reply.md` needs no edit. Confirm by checking that the `subscription.count` entry there lists no request/response fields that changed.

Run: `grep -n "subscription.count" docs/client-api/request-reply.md`
Expected: the entry references the same `{count}` response — no change needed.

- [ ] **Step 4: Commit**

```bash
git add docs/client-api.md
git commit -m "docs(client-api): note thread-aware unread in subscription.count"
```

---

## Self-Review

**Spec coverage:**
- Semantics (room unread OR ≥1 unread thread, +1 max) → Task 3 (`countUnread` + `countThreadOnlyUnread`).
- `RoomID` on `ThreadUnreadRow` → Task 1. Projection → Task 2.
- Pruning #1 (skip already-unread rooms) → `pendingRooms` only holds read rooms. Pruning #2 (stop at first unread thread) → `bumped` set short-circuit.
- Muted excluded → covered by `GetActiveSubscriptions` filter + page-membership filter (Task 3 test `MutedRoomThreadExcluded`).
- Cap scope (threads only bump fetched page) → `pendingRooms` built from the fetched subs (Task 3 test `ThreadInRoomOutsidePageIgnored`).
- Best-effort degradation (batch failure, list failure) → Task 3 tests `ThreadBatchFailureDegrades`, `ThreadListErrorDegrades`.
- Bounds (500 thread cap via `ListByAccount`, chunk 500, `maxSiteFanout`) → reused, no new limits.
- Docs prose → Task 4.

**Placeholder scan:** none — every step carries concrete code/commands.

**Type consistency:** `ThreadUnreadRow.RoomID` (Task 1) is read in Task 3's `countThreadOnlyUnread` as `rows[i].RoomID`. `model.ThreadRoomInfo{ThreadRoomID, Found, LastMsgAt int64}` matches `pkg/model/threadsubscription.go:62`. `unread(*time.Time, *int64)` — `ms := info.LastMsgAt; unread(cand.lastSeenAt, &ms)` matches the signature. `GetThreadRoomInfoBatch(ctx, siteID, ids) ([]model.ThreadRoomInfo, error)` matches `service.go:47`. `newCountSvc` returns `(svc, subs, rooms, threadSubs)` and is the only caller shape used by new tests.

**Notes for the executor:**
- The `newSvc` default (Task 3 Step 1) is load-bearing for keeping `TestCountUnread_MultiSite` / `TestCountUnread_AllRead` green; do not skip it.
- Run `make test SERVICE=user-service` after Task 3 before committing — it exercises both new and pre-existing count tests together.
