# User Thread-Unread Badge RPC — Implementation Plan

> **⚠️ SUPERSEDED — historical execution record, do NOT implement from the per-task code blocks below.** This plan captures the *original* pre-fold execution. During implementation the design was folded to source room type from the account's own subscription via a `$lookup`, which changed the contract. The authoritative design is
> [`../specs/2026-07-01-user-thread-unread-rpc-design.md`](../specs/2026-07-01-user-thread-unread-rpc-design.md) and the shipped code. **As-built deltas vs the blocks below:**
> - `ListByAccount` returns `[]model.ThreadUnreadRow` (`{threadRoomId, siteId, roomType, lastSeenAt, hasMention}`) — **not** `ThreadSubRef`. It is a `$lookup` aggregation against `subscriptions` (membership gate + unsubscribed-app drop + roomType source, applied **before** the newest-500 limit), **not** a plain `Find`.
> - `model.ThreadRoomInfo` is `{threadRoomId, found, lastMsgAt}` — **no** `roomType`. `GetThreadRoomInfos` / `ThreadRoomInfoRow` are `lastMsgAt`-only (single `thread_rooms` find, **no** `rooms` join).
> - The DM tally (`unreadDirectMessage`) is computed from each row's joined `roomType`, not from `ThreadRoomInfo`.
> - Shipped names: client RPC `thread.unread.summary` (`UserThreadUnreadSummary`), types `ThreadUnreadSummaryRequest`/`ThreadUnreadSummaryResponse`, handler `GetThreadUnreadSummary`.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a client-facing user-service RPC that returns a per-user thread-unread badge aggregated across every federation site, computed the same way room (subscription) unread already is.

**Architecture:** Thread subscriptions are federated to the user's home site, so user-service reads all of a user's thread-subs from the local replica, groups them by the room's home `siteId`, fetches `{lastMsgAt, roomType}` per thread from each owning site via a new `ThreadRoomInfoBatch` room-service RPC (modeled on `RoomsInfoBatch`), and folds them into one badge using the existing `unread()` helper — uniformly for every site. The now-callerless `ThreadUnreadSummary` leaf is removed.

**Tech Stack:** Go 1.25, NATS request/reply (`pkg/natsrouter`, `otelnats`), MongoDB (`go.mongodb.org/mongo-driver/v2`, `pkg/mongoutil`), `pkg/errcode`, `go.uber.org/mock`, testify, testcontainers via `pkg/testutil`.

## Global Constraints

- Always use `make` targets, never raw `go`. Unit tests: `make test SERVICE=<name>`. Integration: `make test-integration SERVICE=<name>`. Mocks: `make generate SERVICE=<name>`. Lint: `make lint`. SAST: `make sast`.
- All NATS payloads are JSON with typed structs from `pkg/model` (this feature is not on a sonic hot-path — `encoding/json` throughout).
- Client-facing errors use `pkg/errcode` named constructors; infra failures return raw `fmt.Errorf("desc: %w", err)`. Handlers return the typed error and let `natsrouter` marshal the envelope — never log-and-return.
- Subjects come from `pkg/subject` builders, never raw `fmt.Sprintf` at call sites.
- Structured logging via `log/slog` only; never log tokens/bodies.
- Generated mocks (`mock_store_test.go`, `service/mocks/`) are never hand-edited — regenerate.
- Every model struct gets both `json` and `bson` tags; `camelCase` except `_id`.
- Any change to a `chat.user.` RPC updates `docs/client-api.md` in the same PR.
- Commit after each task's tests pass. Branch: `claude/thread-unread-status-rpc-70zmfx`.

---

## File Structure

- `pkg/subject/subject.go` — add `UserThreadUnread`/`UserThreadUnreadPattern` (client) + `ThreadRoomInfoBatch`/`ThreadRoomInfoBatchSubscribe` (server); remove `ThreadUnreadSummary`/`ThreadUnreadSummarySubscribe`.
- `pkg/model/threadsubscription.go` — add `ThreadUnreadRequest`, `ThreadUnreadResponse`, `ThreadRoomInfoBatchRequest`, `ThreadRoomInfo`, `ThreadRoomInfoBatchResponse`, `ThreadSubRef`; remove `ThreadUnreadSummaryRequest`/`ThreadUnreadSummaryResponse`.
- `room-service/store.go`, `store_mongo.go` — add `GetThreadRoomInfos` + `ThreadRoomInfoRow`; remove `GetThreadUnreadSummary` + `ThreadUnreadSummary`.
- `room-service/handler.go` — add `threadRoomInfoBatch` + registration; remove `threadUnreadSummary` + its registration.
- `user-service/roomclient/client.go` — add `GetThreadRoomInfoBatch`.
- `user-service/mongorepo/threadsubscriptions.go` (new) — `ThreadSubscriptionRepo.ListByAccount`.
- `user-service/service/service.go` — extend `RoomClient`; add `ThreadSubscriptionRepository`; wire `threadSubs`.
- `user-service/service/threadunread.go` (new) — `GetThreadUnread` handler.
- `user-service/main.go` — construct + inject `ThreadSubscriptionRepo`.
- `docs/client-api.md` — document the `thread.unread` RPC.
- Test files: `subject_test.go`, `model_test.go`, room-service `handler_test.go`/`integration_test.go`, `roomclient/client_integration_test.go`, `user-service/mongorepo/*integration_test`, `user-service/service/threadunread_test.go`.

---

## Task 1: Remove the superseded `ThreadUnreadSummary` leaf

Delete-only task. The leaf has zero runtime callers; the repo must build and pass after removal.

**Files:**
- Modify: `pkg/subject/subject.go`, `pkg/subject/subject_test.go`
- Modify: `pkg/model/threadsubscription.go`, `pkg/model/model_test.go`
- Modify: `room-service/store.go`, `room-service/store_mongo.go`, `room-service/handler.go`, `room-service/handler_test.go`, `room-service/integration_test.go`
- Regenerate: `room-service/mock_store_test.go`

- [ ] **Step 1: Delete the subject builders**

In `pkg/subject/subject.go` delete the functions `ThreadUnreadSummary` and `ThreadUnreadSummarySubscribe`. In `pkg/subject/subject_test.go` delete any test referencing them (grep first: `grep -n ThreadUnreadSummary pkg/subject/subject_test.go`).

- [ ] **Step 2: Delete the model types**

In `pkg/model/threadsubscription.go` delete `ThreadUnreadSummaryRequest` and `ThreadUnreadSummaryResponse`. In `pkg/model/model_test.go` delete `TestThreadUnreadSummaryResponseJSON` and `TestThreadUnreadSummaryRequestJSON`.

- [ ] **Step 3: Delete the room-service store method + struct**

In `room-service/store.go` delete the `ThreadUnreadSummary` struct and the `GetThreadUnreadSummary(ctx context.Context, account, siteID string) (*ThreadUnreadSummary, error)` line from the `RoomStore` interface. In `room-service/store_mongo.go` delete the `GetThreadUnreadSummary` implementation (the aggregation method). Keep the `threadRooms` handle and the `{userAccount, siteId}` index — both are retained.

- [ ] **Step 4: Delete the handler + registration**

In `room-service/handler.go` delete the `threadUnreadSummary` method and its registration line `natsrouter.Register(r, subject.ThreadUnreadSummarySubscribe(h.siteID), h.threadUnreadSummary)`. In `room-service/handler_test.go` and `room-service/integration_test.go` delete tests referencing `threadUnreadSummary` / `GetThreadUnreadSummary` (grep to find them).

- [ ] **Step 5: Regenerate the room-service mock**

Run: `make generate SERVICE=room-service`
Expected: `room-service/mock_store_test.go` regenerated without `GetThreadUnreadSummary`.

- [ ] **Step 6: Verify nothing references the leaf**

Run: `grep -rn "ThreadUnreadSummary" pkg room-service`
Expected: no matches (docs/ may still reference it — that's fine).

- [ ] **Step 7: Verify build + tests green**

Run: `make test SERVICE=room-service && make test`
Expected: PASS (pkg/model, pkg/subject, room-service all compile and pass).

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "refactor(thread-unread): remove callerless ThreadUnreadSummary leaf"
```

---

## Task 2: Subject builders

**Files:**
- Modify: `pkg/subject/subject.go`
- Test: `pkg/subject/subject_test.go`

**Interfaces:**
- Produces: `subject.UserThreadUnread(account, siteID string) string`, `subject.UserThreadUnreadPattern(siteID string) string`, `subject.ThreadRoomInfoBatch(siteID string) string`, `subject.ThreadRoomInfoBatchSubscribe(siteID string) string`.

- [ ] **Step 1: Write the failing tests**

Add to `pkg/subject/subject_test.go`:

```go
func TestUserThreadUnread(t *testing.T) {
	assert.Equal(t,
		"chat.user.alice.request.user.site-a.thread.unread",
		subject.UserThreadUnread("alice", "site-a"))
	assert.Equal(t,
		"chat.user.{account}.request.user.site-a.thread.unread",
		subject.UserThreadUnreadPattern("site-a"))
}

func TestUserThreadUnread_PanicsOnWildcardAccount(t *testing.T) {
	assert.Panics(t, func() { subject.UserThreadUnread("a.*", "site-a") })
}

func TestThreadRoomInfoBatch(t *testing.T) {
	assert.Equal(t,
		"chat.server.request.room.site-a.thread.info.batch",
		subject.ThreadRoomInfoBatch("site-a"))
	assert.Equal(t,
		"chat.server.request.room.site-a.thread.info.batch",
		subject.ThreadRoomInfoBatchSubscribe("site-a"))
}
```

- [ ] **Step 2: Run to verify failure**

Run: `make test SERVICE=pkg/subject` (or `go test ./pkg/subject/` is wrapped by `make test`)
Expected: FAIL — undefined: `subject.UserThreadUnread`.

- [ ] **Step 3: Implement the builders**

Add to `pkg/subject/subject.go` (near the other `UserThread*` builders):

```go
// UserThreadUnread is the client-facing subject for the cross-site thread
// unread badge. siteID is the CALLER's own home site. Pair with
// UserThreadUnreadPattern for user-service's registration.
func UserThreadUnread(account, siteID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.user.%s.thread.unread", account, siteID)
}

// UserThreadUnreadPattern is the natsrouter pattern user-service registers.
func UserThreadUnreadPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.user.%s.thread.unread", siteID)
}

// ThreadRoomInfoBatch is the server-to-server request subject for a batch
// lookup of thread rooms' lastMsgAt + parent room type. Mirrors RoomsInfoBatch.
func ThreadRoomInfoBatch(siteID string) string {
	return fmt.Sprintf("chat.server.request.room.%s.thread.info.batch", siteID)
}

// ThreadRoomInfoBatchSubscribe is the per-site subscription subject room-service registers.
func ThreadRoomInfoBatchSubscribe(siteID string) string {
	return fmt.Sprintf("chat.server.request.room.%s.thread.info.batch", siteID)
}
```

- [ ] **Step 4: Run to verify pass**

Run: `make test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/subject/subject.go pkg/subject/subject_test.go
git commit -m "feat(subject): add UserThreadUnread + ThreadRoomInfoBatch builders"
```

---

## Task 3: Model types

**Files:**
- Modify: `pkg/model/threadsubscription.go`
- Test: `pkg/model/model_test.go`

**Interfaces:**
- Produces:
  - `model.ThreadUnreadRequest struct{}`
  - `model.ThreadUnreadResponse{ Unread, UnreadDirectMessage, UnreadMention bool; LastMessageAt *int64; UnavailableSites []string }`
  - `model.ThreadRoomInfoBatchRequest{ ThreadRoomIDs []string }`
  - `model.ThreadRoomInfo{ ThreadRoomID string; Found bool; LastMsgAt int64; RoomType model.RoomType }`
  - `model.ThreadRoomInfoBatchResponse{ Threads []ThreadRoomInfo }`
  - `model.ThreadSubRef{ ThreadRoomID, SiteID string; LastSeenAt *time.Time; HasMention bool }`

- [ ] **Step 1: Write the failing round-trip tests**

Add to `pkg/model/model_test.go`:

```go
func TestThreadUnreadResponseJSON(t *testing.T) {
	ts := int64(1717000000000)
	r := model.ThreadUnreadResponse{
		Unread: true, UnreadDirectMessage: false, UnreadMention: true,
		LastMessageAt: &ts, UnavailableSites: []string{"site-b"},
	}
	roundTrip(t, &r, &model.ThreadUnreadResponse{})
}

func TestThreadUnreadRequestJSON(t *testing.T) {
	roundTrip(t, &model.ThreadUnreadRequest{}, &model.ThreadUnreadRequest{})
}

func TestThreadRoomInfoBatchRequestJSON(t *testing.T) {
	r := model.ThreadRoomInfoBatchRequest{ThreadRoomIDs: []string{"tr1", "tr2"}}
	roundTrip(t, &r, &model.ThreadRoomInfoBatchRequest{})
}

func TestThreadRoomInfoBatchResponseJSON(t *testing.T) {
	r := model.ThreadRoomInfoBatchResponse{Threads: []model.ThreadRoomInfo{
		{ThreadRoomID: "tr1", Found: true, LastMsgAt: 1717000000000, RoomType: model.RoomTypeDM},
		{ThreadRoomID: "tr2", Found: false},
	}}
	roundTrip(t, &r, &model.ThreadRoomInfoBatchResponse{})
}

func TestThreadSubRefBSON(t *testing.T) {
	seen := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	r := model.ThreadSubRef{ThreadRoomID: "tr1", SiteID: "site-a", LastSeenAt: &seen, HasMention: true}
	roundTrip(t, &r, &model.ThreadSubRef{})
}
```

- [ ] **Step 2: Run to verify failure**

Run: `make test`
Expected: FAIL — undefined: `model.ThreadUnreadResponse`.

- [ ] **Step 3: Implement the types**

Add to `pkg/model/threadsubscription.go`:

```go
// ThreadUnreadRequest is the client-facing thread-unread badge request. The
// account rides the subject; no body fields.
type ThreadUnreadRequest struct{}

// ThreadUnreadResponse is the cross-site thread-unread badge. Booleans are ORed
// and LastMessageAt is maxed over the responding sites. UnavailableSites lists
// sites whose RPC failed so a client can distinguish degraded from authoritative.
type ThreadUnreadResponse struct {
	Unread              bool     `json:"unread"`
	UnreadDirectMessage bool     `json:"unreadDirectMessage"`
	UnreadMention       bool     `json:"unreadMention"`
	LastMessageAt       *int64   `json:"lastMessageAt,omitempty"` // UnixMilli
	UnavailableSites    []string `json:"unavailableSites,omitempty"`
}

// ThreadRoomInfoBatchRequest asks room-service for a batch of thread rooms' info.
type ThreadRoomInfoBatchRequest struct {
	ThreadRoomIDs []string `json:"threadRoomIds"`
}

// ThreadRoomInfo is one thread room's activity + parent room type. Found=false
// means the thread room does not exist (LastMsgAt is 0). An empty RoomType means
// the parent room is missing.
type ThreadRoomInfo struct {
	ThreadRoomID string   `json:"threadRoomId"`
	Found        bool     `json:"found"`
	LastMsgAt    int64    `json:"lastMsgAt"` // UnixMilli; 0 when Found=false
	RoomType     RoomType `json:"roomType,omitempty"`
}

// ThreadRoomInfoBatchResponse is the batch reply, one entry per requested id.
type ThreadRoomInfoBatchResponse struct {
	Threads []ThreadRoomInfo `json:"threads"`
}

// ThreadSubRef is the projected thread-subscription row user-service reads from
// its local replica to build the badge (json for tests, bson for the query).
type ThreadSubRef struct {
	ThreadRoomID string     `json:"threadRoomId" bson:"threadRoomId"`
	SiteID       string     `json:"siteId"       bson:"siteId"`
	LastSeenAt   *time.Time `json:"lastSeenAt"   bson:"lastSeenAt"`
	HasMention   bool       `json:"hasMention"   bson:"hasMention"`
}
```

- [ ] **Step 4: Run to verify pass**

Run: `make test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/model/threadsubscription.go pkg/model/model_test.go
git commit -m "feat(model): add thread-unread badge + ThreadRoomInfoBatch types"
```

---

## Task 4: room-service store — `GetThreadRoomInfos`

**Files:**
- Modify: `room-service/store.go`, `room-service/store_mongo.go`
- Regenerate: `room-service/mock_store_test.go`
- Test: `room-service/integration_test.go`

**Interfaces:**
- Consumes: existing `MongoStore.threadRooms` handle; existing `RoomStore.ListRoomsByIDs(ctx, ids) ([]model.Room, error)`.
- Produces: `store.ThreadRoomInfoRow{ ThreadRoomID string; LastMsgAt time.Time; RoomType model.RoomType }`; `RoomStore.GetThreadRoomInfos(ctx context.Context, threadRoomIDs []string) ([]ThreadRoomInfoRow, error)`.

- [ ] **Step 1: Write the failing integration test**

Add to `room-service/integration_test.go`:

```go
func TestMongoStore_GetThreadRoomInfos(t *testing.T) {
	db := freshDB(t) // existing helper returning *mongo.Database (see top of file)
	ctx := context.Background()
	store := NewMongoStore(db)

	lastMsg := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	_, err := db.Collection("thread_rooms").InsertOne(ctx, model.ThreadRoom{
		ID: "tr1", RoomID: "room1", LastMsgAt: lastMsg,
	})
	require.NoError(t, err)
	_, err = db.Collection("rooms").InsertOne(ctx, model.Room{ID: "room1", Type: model.RoomTypeDM})
	require.NoError(t, err)

	rows, err := store.GetThreadRoomInfos(ctx, []string{"tr1", "missing"})
	require.NoError(t, err)
	require.Len(t, rows, 1) // "missing" is absent, not an error
	assert.Equal(t, "tr1", rows[0].ThreadRoomID)
	assert.Equal(t, lastMsg.UTC().UnixMilli(), rows[0].LastMsgAt.UTC().UnixMilli())
	assert.Equal(t, model.RoomTypeDM, rows[0].RoomType)
}
```

(If the existing file names its DB helper differently, use that name — grep `func fresh` / `testutil.MongoDB` at the top of `integration_test.go`.)

- [ ] **Step 2: Run to verify failure**

Run: `make test-integration SERVICE=room-service`
Expected: FAIL — `store.GetThreadRoomInfos` undefined.

- [ ] **Step 3: Add the interface + result struct**

In `room-service/store.go`, add the struct near `RoomCounts` and the method to the `RoomStore` interface:

```go
// ThreadRoomInfoRow is one thread room's activity + parent room type, the result
// of GetThreadRoomInfos.
type ThreadRoomInfoRow struct {
	ThreadRoomID string
	LastMsgAt    time.Time
	RoomType     model.RoomType
}
```

```go
	GetThreadRoomInfos(ctx context.Context, threadRoomIDs []string) ([]ThreadRoomInfoRow, error)
```

- [ ] **Step 4: Implement in store_mongo.go**

Add to `room-service/store_mongo.go` (reuses `ListRoomsByIDs`, so no `$lookup`):

```go
// GetThreadRoomInfos returns each existing thread room's lastMsgAt and its
// parent room type. Missing thread rooms are omitted. Two projected finds
// joined in Go (thread_rooms → rooms), never a $lookup.
func (s *MongoStore) GetThreadRoomInfos(ctx context.Context, threadRoomIDs []string) ([]ThreadRoomInfoRow, error) {
	cursor, err := s.threadRooms.Find(ctx,
		bson.M{"_id": bson.M{"$in": threadRoomIDs}},
		options.Find().SetProjection(bson.M{"_id": 1, "lastMsgAt": 1, "roomId": 1}),
	)
	if err != nil {
		return nil, fmt.Errorf("find thread rooms: %w", err)
	}
	defer cursor.Close(ctx)

	var trs []struct {
		ID        string    `bson:"_id"`
		LastMsgAt time.Time `bson:"lastMsgAt"`
		RoomID    string    `bson:"roomId"`
	}
	if err := cursor.All(ctx, &trs); err != nil {
		return nil, fmt.Errorf("decode thread rooms: %w", err)
	}

	roomIDs := make([]string, 0, len(trs))
	seen := make(map[string]struct{}, len(trs))
	for _, tr := range trs {
		if tr.RoomID == "" {
			continue
		}
		if _, dup := seen[tr.RoomID]; dup {
			continue
		}
		seen[tr.RoomID] = struct{}{}
		roomIDs = append(roomIDs, tr.RoomID)
	}

	typeByRoom := make(map[string]model.RoomType, len(roomIDs))
	if len(roomIDs) > 0 {
		rooms, err := s.ListRoomsByIDs(ctx, roomIDs)
		if err != nil {
			return nil, fmt.Errorf("list rooms by ids: %w", err)
		}
		for i := range rooms {
			typeByRoom[rooms[i].ID] = rooms[i].Type
		}
	}

	out := make([]ThreadRoomInfoRow, 0, len(trs))
	for _, tr := range trs {
		out = append(out, ThreadRoomInfoRow{
			ThreadRoomID: tr.ID,
			LastMsgAt:    tr.LastMsgAt,
			RoomType:     typeByRoom[tr.RoomID],
		})
	}
	return out, nil
}
```

- [ ] **Step 5: Regenerate the mock**

Run: `make generate SERVICE=room-service`
Expected: `mock_store_test.go` gains `GetThreadRoomInfos`.

- [ ] **Step 6: Run the integration test**

Run: `make test-integration SERVICE=room-service`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add room-service/store.go room-service/store_mongo.go room-service/mock_store_test.go room-service/integration_test.go
git commit -m "feat(room-service): add GetThreadRoomInfos store method"
```

---

## Task 5: room-service handler — `threadRoomInfoBatch`

**Files:**
- Modify: `room-service/handler.go`
- Test: `room-service/handler_test.go`

**Interfaces:**
- Consumes: `RoomStore.GetThreadRoomInfos`; `h.maxBatchSize`; `h.siteID`.
- Produces: handler `threadRoomInfoBatch(c *natsrouter.Context, req model.ThreadRoomInfoBatchRequest) (*model.ThreadRoomInfoBatchResponse, error)`, registered on `subject.ThreadRoomInfoBatchSubscribe(h.siteID)`.

- [ ] **Step 1: Write the failing unit test**

Add to `room-service/handler_test.go` (mirror `TestHandler_handleRoomsInfoBatch`; use the file's existing mock-store constructor — grep `NewMockRoomStore` / `newTestHandler`):

```go
func TestHandler_threadRoomInfoBatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	h := newTestHandler(store) // existing helper; sets siteID + maxBatchSize

	lastMsg := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	store.EXPECT().
		GetThreadRoomInfos(gomock.Any(), []string{"tr1", "tr2"}).
		Return([]ThreadRoomInfoRow{
			{ThreadRoomID: "tr1", LastMsgAt: lastMsg, RoomType: model.RoomTypeDM},
		}, nil)

	resp, err := h.threadRoomInfoBatch(newCtx(t), model.ThreadRoomInfoBatchRequest{
		ThreadRoomIDs: []string{"tr1", "tr2"},
	})
	require.NoError(t, err)
	require.Len(t, resp.Threads, 2)
	assert.Equal(t, model.ThreadRoomInfo{
		ThreadRoomID: "tr1", Found: true,
		LastMsgAt: lastMsg.UTC().UnixMilli(), RoomType: model.RoomTypeDM,
	}, resp.Threads[0])
	assert.Equal(t, model.ThreadRoomInfo{ThreadRoomID: "tr2", Found: false}, resp.Threads[1])
}

func TestHandler_threadRoomInfoBatch_Empty(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHandler(NewMockRoomStore(ctrl))
	_, err := h.threadRoomInfoBatch(newCtx(t), model.ThreadRoomInfoBatchRequest{})
	var e *errcode.Error
	require.True(t, errors.As(err, &e))
	assert.Equal(t, errcode.CodeBadRequest, e.Code)
}
```

(`newTestHandler` / `newCtx` are whatever the existing tests use — copy their setup. If `maxBatchSize` is 0 in the helper, set it to a positive value there or via the constructor the file already uses.)

- [ ] **Step 2: Run to verify failure**

Run: `make test SERVICE=room-service`
Expected: FAIL — `h.threadRoomInfoBatch` undefined.

- [ ] **Step 3: Implement the handler + registration**

In `room-service/handler.go`, register beside `roomsInfoBatch` (in `RegisterHandlers`):

```go
	natsrouter.Register(r, subject.ThreadRoomInfoBatchSubscribe(h.siteID), h.threadRoomInfoBatch)
```

Add the handler:

```go
func (h *Handler) threadRoomInfoBatch(c *natsrouter.Context, req model.ThreadRoomInfoBatchRequest) (*model.ThreadRoomInfoBatchResponse, error) {
	var ctx context.Context = c
	start := time.Now()
	if len(req.ThreadRoomIDs) == 0 {
		return nil, errcode.BadRequest("threadRoomIds must not be empty")
	}
	if len(req.ThreadRoomIDs) > h.maxBatchSize {
		return nil, errcode.BadRequest(fmt.Sprintf("batch size %d exceeds limit %d", len(req.ThreadRoomIDs), h.maxBatchSize))
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	rows, err := h.store.GetThreadRoomInfos(ctx, req.ThreadRoomIDs)
	if err != nil {
		return nil, fmt.Errorf("get thread room infos: %w", err)
	}
	byID := make(map[string]ThreadRoomInfoRow, len(rows))
	for _, r := range rows {
		byID[r.ThreadRoomID] = r
	}
	threads := make([]model.ThreadRoomInfo, 0, len(req.ThreadRoomIDs))
	for _, id := range req.ThreadRoomIDs {
		if r, ok := byID[id]; ok {
			threads = append(threads, model.ThreadRoomInfo{
				ThreadRoomID: id, Found: true,
				LastMsgAt: r.LastMsgAt.UTC().UnixMilli(), RoomType: r.RoomType,
			})
		} else {
			threads = append(threads, model.ThreadRoomInfo{ThreadRoomID: id, Found: false})
		}
	}
	slog.Debug("thread room info batch handled",
		"site_id", h.siteID, "batch_size", len(req.ThreadRoomIDs),
		"latency_ms", time.Since(start).Milliseconds())
	return &model.ThreadRoomInfoBatchResponse{Threads: threads}, nil
}
```

- [ ] **Step 4: Run to verify pass**

Run: `make test SERVICE=room-service`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add room-service/handler.go room-service/handler_test.go
git commit -m "feat(room-service): add threadRoomInfoBatch RPC handler"
```

---

## Task 6: user-service roomclient — `GetThreadRoomInfoBatch`

**Files:**
- Modify: `user-service/roomclient/client.go`
- Test: `user-service/roomclient/client_integration_test.go`

**Interfaces:**
- Produces: `(*roomclient.Client).GetThreadRoomInfoBatch(ctx context.Context, siteID string, threadRoomIDs []string) ([]model.ThreadRoomInfo, error)`.

- [ ] **Step 1: Write the failing integration test**

Add to `user-service/roomclient/client_integration_test.go` (mirrors `TestGetRoomsInfo_Integration`, uses the file's `dial` helper):

```go
func TestGetThreadRoomInfoBatch_Integration(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		nc := dial(t)
		sub, err := nc.Subscribe(subject.ThreadRoomInfoBatch("site-a"), func(m otelnats.Msg) {
			out, _ := json.Marshal(model.ThreadRoomInfoBatchResponse{
				Threads: []model.ThreadRoomInfo{{ThreadRoomID: "tr1", Found: true, LastMsgAt: 42, RoomType: model.RoomTypeDM}},
			})
			_ = m.Msg.Respond(out)
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = sub.Unsubscribe() })

		got, err := New(nc, "site-a").GetThreadRoomInfoBatch(context.Background(), "site-a", []string{"tr1"})
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, int64(42), got[0].LastMsgAt)
		assert.Equal(t, model.RoomTypeDM, got[0].RoomType)
	})

	t.Run("errcode reply relayed", func(t *testing.T) {
		nc := dial(t)
		sub, err := nc.Subscribe(subject.ThreadRoomInfoBatch("site-a"), func(m otelnats.Msg) {
			data, _ := json.Marshal(errcode.BadRequest("bad"))
			_ = m.Msg.Respond(data)
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = sub.Unsubscribe() })

		_, err = New(nc, "site-a").GetThreadRoomInfoBatch(context.Background(), "site-a", []string{"tr1"})
		var e *errcode.Error
		require.True(t, errors.As(err, &e))
		assert.Equal(t, errcode.CodeBadRequest, e.Code)
	})
}
```

- [ ] **Step 2: Run to verify failure**

Run: `make test-integration SERVICE=user-service`
Expected: FAIL — `GetThreadRoomInfoBatch` undefined.

- [ ] **Step 3: Implement the client method**

Add to `user-service/roomclient/client.go` (mirrors `GetRoomsInfo`):

```go
// GetThreadRoomInfoBatch issues a batch thread-room-info RPC to room-service on
// the given site; non-OK reply envelopes are relayed via errcode.Parse.
func (c *Client) GetThreadRoomInfoBatch(ctx context.Context, siteID string, threadRoomIDs []string) ([]model.ThreadRoomInfo, error) {
	req, err := json.Marshal(model.ThreadRoomInfoBatchRequest{ThreadRoomIDs: threadRoomIDs})
	if err != nil {
		return nil, fmt.Errorf("marshal thread-room-info request: %w", err)
	}
	msg, err := c.nc.Request(ctx, subject.ThreadRoomInfoBatch(siteID), req, roomRPCTimeout)
	if err != nil {
		return nil, fmt.Errorf("thread-room-info rpc: %w", err)
	}
	if e, ok := errcode.Parse(msg.Data); ok {
		return nil, e
	}
	var out model.ThreadRoomInfoBatchResponse
	if err := json.Unmarshal(msg.Data, &out); err != nil {
		return nil, fmt.Errorf("decode thread-room-info response: %w", err)
	}
	return out.Threads, nil
}
```

- [ ] **Step 4: Run to verify pass**

Run: `make test-integration SERVICE=user-service`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add user-service/roomclient/client.go user-service/roomclient/client_integration_test.go
git commit -m "feat(roomclient): add GetThreadRoomInfoBatch"
```

---

## Task 7: user-service mongorepo — `ThreadSubscriptionRepo.ListByAccount`

**Files:**
- Create: `user-service/mongorepo/threadsubscriptions.go`
- Test: `user-service/mongorepo/threadsubscriptions_integration_test.go`

**Interfaces:**
- Produces: `mongorepo.NewThreadSubscriptionRepo(db *mongo.Database) *ThreadSubscriptionRepo`; `(*ThreadSubscriptionRepo).ListByAccount(ctx context.Context, account string) ([]model.ThreadSubRef, error)`.

- [ ] **Step 1: Write the failing integration test**

Create `user-service/mongorepo/threadsubscriptions_integration_test.go`:

```go
//go:build integration

package mongorepo

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestThreadSubscriptionRepo_ListByAccount(t *testing.T) {
	db := testutil.MongoDB(t, "user_service_threadsubs")
	ctx := context.Background()
	seen := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	_, err := db.Collection("thread_subscriptions").InsertMany(ctx, []interface{}{
		model.ThreadSubscription{ID: "1", ThreadRoomID: "tr1", UserAccount: "alice", SiteID: "site-a", LastSeenAt: &seen, HasMention: true},
		model.ThreadSubscription{ID: "2", ThreadRoomID: "tr2", UserAccount: "alice", SiteID: "site-b"},
		model.ThreadSubscription{ID: "3", ThreadRoomID: "tr3", UserAccount: "bob", SiteID: "site-a"},
	})
	require.NoError(t, err)

	repo := NewThreadSubscriptionRepo(db)
	rows, err := repo.ListByAccount(ctx, "alice")
	require.NoError(t, err)
	require.Len(t, rows, 2)

	bySite := map[string]model.ThreadSubRef{}
	for _, r := range rows {
		bySite[r.SiteID] = r
	}
	assert.Equal(t, "tr1", bySite["site-a"].ThreadRoomID)
	assert.True(t, bySite["site-a"].HasMention)
	require.NotNil(t, bySite["site-a"].LastSeenAt)
	assert.Nil(t, bySite["site-b"].LastSeenAt)

	empty, err := repo.ListByAccount(ctx, "nobody")
	require.NoError(t, err)
	assert.Empty(t, empty)
}
```

If `user-service/mongorepo` has no integration `TestMain` yet, add one in a shared `*_integration_test.go` (grep first): `func TestMain(m *testing.M) { testutil.RunTests(m) }`.

- [ ] **Step 2: Run to verify failure**

Run: `make test-integration SERVICE=user-service`
Expected: FAIL — `NewThreadSubscriptionRepo` undefined.

- [ ] **Step 3: Implement the repo**

Create `user-service/mongorepo/threadsubscriptions.go`:

```go
package mongorepo

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
)

const threadSubscriptionsCollection = "thread_subscriptions"

// ThreadSubscriptionRepo reads the local (home-site) thread_subscriptions replica.
type ThreadSubscriptionRepo struct {
	threadSubs *mongoutil.Collection[model.ThreadSubRef]
}

// NewThreadSubscriptionRepo builds a ThreadSubscriptionRepo over db.
func NewThreadSubscriptionRepo(db *mongo.Database) *ThreadSubscriptionRepo {
	return &ThreadSubscriptionRepo{
		threadSubs: mongoutil.NewCollection[model.ThreadSubRef](db.Collection(threadSubscriptionsCollection)),
	}
}

// ListByAccount returns all of the account's thread-subs (every site), projected
// to the fields the badge needs. Backed by the shared {userAccount} index.
func (r *ThreadSubscriptionRepo) ListByAccount(ctx context.Context, account string) ([]model.ThreadSubRef, error) {
	return r.threadSubs.FindMany(ctx,
		bson.M{"userAccount": account},
		mongoutil.WithProjection(bson.M{
			"_id": 0, "threadRoomId": 1, "siteId": 1, "lastSeenAt": 1, "hasMention": 1,
		}),
	)
}
```

- [ ] **Step 4: Run to verify pass**

Run: `make test-integration SERVICE=user-service`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add user-service/mongorepo/threadsubscriptions.go user-service/mongorepo/threadsubscriptions_integration_test.go
git commit -m "feat(user-service): add ThreadSubscriptionRepo.ListByAccount"
```

---

## Task 8: user-service service wiring — interfaces + injection

**Files:**
- Modify: `user-service/service/service.go`
- Modify: `user-service/main.go`
- Regenerate: `user-service/service/mocks/mock_repository.go`

**Interfaces:**
- Consumes: `roomclient.Client.GetThreadRoomInfoBatch` (Task 6); `mongorepo.ThreadSubscriptionRepo.ListByAccount` (Task 7).
- Produces: `RoomClient.GetThreadRoomInfoBatch(...)`; new `ThreadSubscriptionRepository` interface with `ListByAccount(ctx, account string) ([]model.ThreadSubRef, error)`; `UserService.threadSubs`; updated `New(...)` signature with `threadSubs` as its 4th parameter (after `apps`, before `rooms`).

- [ ] **Step 1: Extend the interfaces + struct + constructor**

In `user-service/service/service.go`:

Add to the `RoomClient` interface:

```go
	GetThreadRoomInfoBatch(ctx context.Context, siteID string, threadRoomIDs []string) ([]model.ThreadRoomInfo, error)
```

Add a new interface:

```go
// ThreadSubscriptionRepository reads the local thread_subscriptions replica for
// the thread-unread badge.
type ThreadSubscriptionRepository interface {
	ListByAccount(ctx context.Context, account string) ([]model.ThreadSubRef, error)
}
```

Add `ThreadSubscriptionRepository` to the `//go:generate mockgen` directive's interface list (append `,ThreadSubscriptionRepository`).

Add the field to `UserService`:

```go
	threadSubs      ThreadSubscriptionRepository
```

Update `New(...)` to accept and assign `threadSubs` (place the param right after `apps AppRepository`):

```go
func New(subs SubscriptionRepository, users UserRepository, apps AppRepository, threadSubs ThreadSubscriptionRepository, rooms RoomClient, history HistoryClient, pub EventPublisher, cfg *config.Config) *UserService {
	return &UserService{
		subs:            subs,
		users:           users,
		apps:            apps,
		threadSubs:      threadSubs,
		rooms:           rooms,
		history:         history,
		pub:             pub,
		siteID:          cfg.SiteID,
		allSiteIDs:      cfg.AllSiteIDs,
		maxSubs:         cfg.MaxSubscriptionLimit,
		defaultLimit:    cfg.DefaultSubscriptionLimit,
		maxApps:         cfg.MaxAppsLimit,
		defaultApps:     cfg.DefaultAppsLimit,
		maxAccountNames: cfg.MaxAccountNames,
	}
}
```

- [ ] **Step 2: Wire main.go**

In `user-service/main.go`: add the compile-time assertion beside the others:

```go
	_ service.ThreadSubscriptionRepository = (*mongorepo.ThreadSubscriptionRepo)(nil)
```

Construct the repo and pass it into `service.New` (4th arg):

```go
	threadSubRepo := mongorepo.NewThreadSubscriptionRepo(db)
```
```go
	svc := service.New(subRepo, userRepo, appRepo, threadSubRepo, roomclient.New(nc, cfg.SiteID), historyclient.New(nc), publisher.New(js), &cfg)
```

- [ ] **Step 3: Regenerate the service mocks**

Run: `make generate SERVICE=user-service`
Expected: `user-service/service/mocks/mock_repository.go` gains `MockThreadSubscriptionRepository` and the `GetThreadRoomInfoBatch` method on `MockRoomClient`.

- [ ] **Step 4: Verify build (no test yet uses threadSubs)**

Run: `make test SERVICE=user-service`
Expected: PASS (package compiles; `threadSubs` field currently unused is fine).

- [ ] **Step 5: Commit**

```bash
git add user-service/service/service.go user-service/main.go user-service/service/mocks/mock_repository.go
git commit -m "feat(user-service): wire ThreadSubscriptionRepository + RoomClient batch method"
```

---

## Task 9: user-service handler — `GetThreadUnread`

**Files:**
- Create: `user-service/service/threadunread.go`
- Modify: `user-service/service/service.go` (register the handler)
- Test: `user-service/service/threadunread_test.go`

**Interfaces:**
- Consumes: `s.threadSubs.ListByAccount`; `s.rooms.GetThreadRoomInfoBatch`; the existing `unread(lastSeen *time.Time, ms *int64) bool` helper (same package); `maxSiteFanout` is NOT used (uncapped fan-out per spec).
- Produces: `(*UserService).GetThreadUnread(c *natsrouter.Context, req model.ThreadUnreadRequest) (*model.ThreadUnreadResponse, error)`, registered on `subject.UserThreadUnreadPattern(s.siteID)`.

- [ ] **Step 1: Write the failing unit tests**

Create `user-service/service/threadunread_test.go`:

```go
package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/user-service/service/mocks"
)

func ptrTime(ms int64) *time.Time { t := time.UnixMilli(ms).UTC(); return &t }

// newThreadUnreadService builds a UserService with only the deps GetThreadUnread
// needs; other deps are nil (unused by this handler).
func newThreadUnreadService(t *testing.T, ts *mocks.MockThreadSubscriptionRepository, rc *mocks.MockRoomClient) *UserService {
	t.Helper()
	return &UserService{threadSubs: ts, rooms: rc, siteID: "site-a"}
}

func TestGetThreadUnread_AggregatesAcrossSites(t *testing.T) {
	ctrl := gomock.NewController(t)
	ts := mocks.NewMockThreadSubscriptionRepository(ctrl)
	rc := mocks.NewMockRoomClient(ctrl)

	ts.EXPECT().ListByAccount(gomock.Any(), "alice").Return([]model.ThreadSubRef{
		{ThreadRoomID: "trA", SiteID: "site-a", LastSeenAt: ptrTime(100)},          // local: lastMsg 200 > 100 → unread
		{ThreadRoomID: "trB", SiteID: "site-b", LastSeenAt: nil, HasMention: true}, // remote: never seen + mention + DM
	}, nil)
	rc.EXPECT().GetThreadRoomInfoBatch(gomock.Any(), "site-a", []string{"trA"}).
		Return([]model.ThreadRoomInfo{{ThreadRoomID: "trA", Found: true, LastMsgAt: 200, RoomType: model.RoomTypeChannel}}, nil)
	rc.EXPECT().GetThreadRoomInfoBatch(gomock.Any(), "site-b", []string{"trB"}).
		Return([]model.ThreadRoomInfo{{ThreadRoomID: "trB", Found: true, LastMsgAt: 300, RoomType: model.RoomTypeDM}}, nil)

	svc := newThreadUnreadService(t, ts, rc)
	resp, err := svc.GetThreadUnread(newThreadUnreadCtx(t, "alice"), model.ThreadUnreadRequest{})
	require.NoError(t, err)
	assert.True(t, resp.Unread)
	assert.True(t, resp.UnreadDirectMessage) // trB is DM + unread
	assert.True(t, resp.UnreadMention)       // trB hasMention
	require.NotNil(t, resp.LastMessageAt)
	assert.Equal(t, int64(300), *resp.LastMessageAt)
	assert.Empty(t, resp.UnavailableSites)
}

func TestGetThreadUnread_SiteFailureDegrades(t *testing.T) {
	ctrl := gomock.NewController(t)
	ts := mocks.NewMockThreadSubscriptionRepository(ctrl)
	rc := mocks.NewMockRoomClient(ctrl)

	ts.EXPECT().ListByAccount(gomock.Any(), "alice").Return([]model.ThreadSubRef{
		{ThreadRoomID: "trB", SiteID: "site-b", LastSeenAt: nil},
	}, nil)
	rc.EXPECT().GetThreadRoomInfoBatch(gomock.Any(), "site-b", []string{"trB"}).
		Return(nil, errors.New("boom"))

	svc := newThreadUnreadService(t, ts, rc)
	resp, err := svc.GetThreadUnread(newThreadUnreadCtx(t, "alice"), model.ThreadUnreadRequest{})
	require.NoError(t, err)
	assert.False(t, resp.Unread)
	assert.Equal(t, []string{"site-b"}, resp.UnavailableSites)
}

func TestGetThreadUnread_NoSubs(t *testing.T) {
	ctrl := gomock.NewController(t)
	ts := mocks.NewMockThreadSubscriptionRepository(ctrl)
	rc := mocks.NewMockRoomClient(ctrl)
	ts.EXPECT().ListByAccount(gomock.Any(), "alice").Return(nil, nil)
	// rc: no calls expected.

	svc := newThreadUnreadService(t, ts, rc)
	resp, err := svc.GetThreadUnread(newThreadUnreadCtx(t, "alice"), model.ThreadUnreadRequest{})
	require.NoError(t, err)
	assert.False(t, resp.Unread)
	assert.Nil(t, resp.LastMessageAt)
	assert.Empty(t, resp.UnavailableSites)
}

func TestGetThreadUnread_NotFoundSkipped(t *testing.T) {
	ctrl := gomock.NewController(t)
	ts := mocks.NewMockThreadSubscriptionRepository(ctrl)
	rc := mocks.NewMockRoomClient(ctrl)
	ts.EXPECT().ListByAccount(gomock.Any(), "alice").Return([]model.ThreadSubRef{
		{ThreadRoomID: "gone", SiteID: "site-a", LastSeenAt: nil},
	}, nil)
	rc.EXPECT().GetThreadRoomInfoBatch(gomock.Any(), "site-a", []string{"gone"}).
		Return([]model.ThreadRoomInfo{{ThreadRoomID: "gone", Found: false}}, nil)

	svc := newThreadUnreadService(t, ts, rc)
	resp, err := svc.GetThreadUnread(newThreadUnreadCtx(t, "alice"), model.ThreadUnreadRequest{})
	require.NoError(t, err)
	assert.False(t, resp.Unread)
	assert.Nil(t, resp.LastMessageAt)
}

func TestGetThreadUnread_ListError(t *testing.T) {
	ctrl := gomock.NewController(t)
	ts := mocks.NewMockThreadSubscriptionRepository(ctrl)
	ts.EXPECT().ListByAccount(gomock.Any(), "alice").Return(nil, errors.New("db down"))
	svc := newThreadUnreadService(t, ts, mocks.NewMockRoomClient(ctrl))
	_, err := svc.GetThreadUnread(newThreadUnreadCtx(t, "alice"), model.ThreadUnreadRequest{})
	require.Error(t, err)
}
```

Add a `newThreadUnreadCtx` helper to the test file (a `*natsrouter.Context` whose `Param("account")` returns the given account). Grep the existing service tests for how they build a `*natsrouter.Context` with params (e.g. `TestListSubscriptions`) and copy that construction; name it `newThreadUnreadCtx(t, account)`.

- [ ] **Step 2: Run to verify failure**

Run: `make test SERVICE=user-service`
Expected: FAIL — `svc.GetThreadUnread` undefined.

- [ ] **Step 3: Implement the handler**

Create `user-service/service/threadunread.go`:

```go
package service

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
)

// threadInfoBatchChunk bounds each ThreadRoomInfoBatch request so it never
// exceeds room-service's MAX_BATCH_SIZE (default 1000).
const threadInfoBatchChunk = 500

// GetThreadUnread returns the user's cross-site thread-unread badge. It reads the
// home-replica thread-subs, groups them by the room's home siteId, fetches each
// thread room's {lastMsgAt, roomType} from the owning site, and folds them into
// one badge via the same unread() helper the subscription badge uses.
// NATS: chat.user.{account}.request.user.{siteID}.thread.unread
func (s *UserService) GetThreadUnread(c *natsrouter.Context, _ model.ThreadUnreadRequest) (*model.ThreadUnreadResponse, error) {
	account := c.Param("account")
	c.WithLogValues("account", account)

	rows, err := s.threadSubs.ListByAccount(c, account)
	if err != nil {
		return nil, fmt.Errorf("list thread subscriptions: %w", err)
	}
	if len(rows) == 0 {
		return &model.ThreadUnreadResponse{}, nil
	}

	// Group thread rooms by owning site, and keep each row's read state.
	type subState struct {
		lastSeenAt *time.Time
		hasMention bool
	}
	idsBySite := map[string][]string{}
	stateByThread := make(map[string]subState, len(rows))
	for _, r := range rows {
		idsBySite[r.SiteID] = append(idsBySite[r.SiteID], r.ThreadRoomID)
		stateByThread[r.ThreadRoomID] = subState{lastSeenAt: r.LastSeenAt, hasMention: r.HasMention}
	}

	type siteResult struct {
		infos  []model.ThreadRoomInfo
		failed bool
	}
	sites := make([]string, 0, len(idsBySite))
	for site := range idsBySite {
		sites = append(sites, site)
	}
	results := make([]siteResult, len(sites))

	var wg sync.WaitGroup
	for i, site := range sites {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, chunk := range chunkStrings(idsBySite[site], threadInfoBatchChunk) {
				if c.Err() != nil {
					results[i].failed = true
					return
				}
				infos, err := s.rooms.GetThreadRoomInfoBatch(c, site, chunk)
				if err != nil {
					slog.WarnContext(c, "thread-unread site degraded",
						"account", account, "site", site,
						"request_id", natsutil.RequestIDFromContext(c), "error", err)
					results[i].failed = true
					return
				}
				results[i].infos = append(results[i].infos, infos...)
			}
		}()
	}
	wg.Wait()

	resp := &model.ThreadUnreadResponse{}
	var maxLastMsg int64
	var haveLastMsg bool
	for i, site := range sites {
		if results[i].failed {
			resp.UnavailableSites = append(resp.UnavailableSites, site)
			continue
		}
		for _, info := range results[i].infos {
			if !info.Found {
				continue
			}
			if info.LastMsgAt > maxLastMsg {
				maxLastMsg, haveLastMsg = info.LastMsgAt, true
			}
			st := stateByThread[info.ThreadRoomID]
			ms := info.LastMsgAt
			isUnread := unread(st.lastSeenAt, &ms)
			resp.Unread = resp.Unread || isUnread
			resp.UnreadDirectMessage = resp.UnreadDirectMessage || (isUnread && info.RoomType == model.RoomTypeDM)
			resp.UnreadMention = resp.UnreadMention || st.hasMention
		}
	}
	if haveLastMsg {
		resp.LastMessageAt = &maxLastMsg
	}
	return resp, nil
}

// chunkStrings splits ids into slices of at most size elements.
func chunkStrings(ids []string, size int) [][]string {
	if len(ids) <= size {
		return [][]string{ids}
	}
	var out [][]string
	for start := 0; start < len(ids); start += size {
		end := start + size
		if end > len(ids) {
			end = len(ids)
		}
		out = append(out, ids[start:end])
	}
	return out
}
```

Register the handler in `user-service/service/service.go` `RegisterHandlers`:

```go
	natsrouter.Register(r, subject.UserThreadUnreadPattern(s.siteID), s.GetThreadUnread)
```

- [ ] **Step 4: Run to verify pass**

Run: `make test SERVICE=user-service`
Expected: PASS.

- [ ] **Step 5: Lint (goroutine loopvar / unused checks)**

Run: `make lint`
Expected: PASS. (Go 1.25 per-iteration loop vars make the `go func()` capture of `i`/`site` safe.)

- [ ] **Step 6: Commit**

```bash
git add user-service/service/threadunread.go user-service/service/service.go user-service/service/threadunread_test.go
git commit -m "feat(user-service): add GetThreadUnread cross-site badge handler"
```

---

## Task 10: Document the client API

**Files:**
- Modify: `docs/client-api.md`

- [ ] **Step 1: Add the RPC entry**

Find the `thread.list` entry in `docs/client-api.md` and add a sibling `thread.unread` section. Follow the file's existing style (field tables with explicit types + a success JSON example). Content:

- **Subject:** `chat.user.{account}.request.user.{siteId}.thread.unread`
- **Request body:** empty (`{}`).
- **Response fields:**

| Field | Type | Description |
|-------|------|-------------|
| `unread` | boolean | Any thread has activity newer than the user's last-seen. |
| `unreadDirectMessage` | boolean | `unread` restricted to DM-room threads. |
| `unreadMention` | boolean | The user is @-mentioned in any unread-tracked thread. |
| `lastMessageAt` | integer? | UnixMilli of the newest thread activity across sites; omitted when none. |
| `unavailableSites` | string[]? | Sites whose per-site lookup failed; omitted when all responded. |

- **Success example:**

```json
{
  "unread": true,
  "unreadDirectMessage": false,
  "unreadMention": true,
  "lastMessageAt": 1717000000000
}
```

- **Errors:** `internal` (local thread-subscription read failed). Per-site RPC failures degrade into `unavailableSites` rather than erroring.

- [ ] **Step 2: Commit**

```bash
git add docs/client-api.md
git commit -m "docs(client-api): document thread.unread RPC"
```

---

## Task 11: Full verification + push

- [ ] **Step 1: Regenerate all mocks (drift check)**

Run: `make generate`
Expected: no unexpected diff (mocks already regenerated per task).

- [ ] **Step 2: Unit tests (race)**

Run: `make test`
Expected: PASS.

- [ ] **Step 3: Integration tests (Docker)**

Run: `make test-integration SERVICE=room-service && make test-integration SERVICE=user-service`
Expected: PASS.

- [ ] **Step 4: Lint**

Run: `make lint`
Expected: PASS.

- [ ] **Step 5: SAST**

Run: `make sast`
Expected: PASS (no medium+).

- [ ] **Step 6: Confirm no leaf references remain in code**

Run: `grep -rn "ThreadUnreadSummary" pkg room-service user-service`
Expected: no matches.

- [ ] **Step 7: Push**

```bash
git push -u origin claude/thread-unread-status-rpc-70zmfx
```

---

## Self-Review Notes (traceability to spec)

- Client RPC subject/model → Tasks 2, 3, 9, 10.
- New `ThreadRoomInfoBatch` RPC (with `roomType` via rooms lookup) → Tasks 2, 3, 4, 5, 6.
- Uniform local+cross-site compute via `unread()` helper, uncapped fan-out, chunking, degradation → Task 9.
- Leaf removal (zero callers) → Task 1.
- Local thread-sub discovery from home replica → Task 7.
- No double-counting (group by `siteId`, one row per thread) → Task 9 grouping.
- Accepted trade-offs (post-read false-unread window; O(threads) fetch) are behavioral, not code gates — no task, documented in the spec.
