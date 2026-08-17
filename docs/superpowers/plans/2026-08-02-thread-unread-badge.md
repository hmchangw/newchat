# Thread-Unread Badge Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reintroduce `Subscription.ThreadUnread` with a real write path, make `subscription.count`'s thread phase site-local, and stamp per-recipient badge unread counts (capped at 10) into push notification events via a `badge.count.batch` RPC backed by a Valkey unread-room-set cache.

**Architecture:** Two shippable phases. **Phase A** (Tasks 1–6): the field, its producer (message-worker `UpdateMany` + one `thread_unread_added` outbox event per remote follower site), the resurrected clear paths (no `alert` coupling), and the local `subscription.count` thread check. **Phase B** (Tasks 7–11): `pkg/badgecache` (per-user Valkey SET of unread room IDs, atomic Lua), the server-to-server `badge.count.batch` RPC in user-service, SREM hooks at every read-ingest point, and notification-worker stamping `UnreadCounts` onto push batches. Spec: `docs/superpowers/specs/2026-08-02-thread-unread-badge-design.md`.

**Tech Stack:** Go 1.25, mockgen, testify, testcontainers (`pkg/testutil`, incl. `SharedValkeyCluster`), NATS JetStream, `go-redis` cluster client, MongoDB driver v2.

## Global Constraints

- All commands via `make` targets; never raw `go`. Never commit with `--no-verify`; lint+tests run in the pre-commit hook.
- TDD red-first for every behavior change; delete-only changes carry their tests in the same commit.
- `threadUnread` stores **parent message IDs**; the badge cache stores **room IDs**. Never mix them.
- `alert` is untouched everywhere — no alert writes, no alert recompute.
- All badge-cache operations are **fail-open**: a Valkey error never fails an RPC, a read handler, or an inbox apply.
- Badge counts on the wire are capped at 10 (`min(SCARD, 10)`); `subscription.count`'s `{count}` contract is unchanged (exact, uncapped).
- `GetThreadUnreadSummary` / `ClearAllThreadUnread` / `ThreadUnreadRow` (the derived badge RPCs in user-service) are **not** modified by this plan — only `countUnread`'s thread phase changes.
- Do not write any deployment site counts or traffic percentages into code comments or docs.
- Client-facing doc updates (`docs/client-api.md` + derived views) ship in the same task as the handler change they describe.
- Read the current file state before every edit — Phase A reverses parts of the 2026-08-01 removal, and the deleted code (in git history at commits `986ce7b`/`71c2ca1`/`a146de6`) is the reference for the resurrected shapes.

---

### Task 1: pkg/model + pkg/outbox — field, events, partition

**Files:**
- Modify: `pkg/model/subscription.go` (~line 38, after `HasMention`)
- Modify: `pkg/model/event.go` (`ThreadReadEvent` ~line 166; `InboxEventType` consts ~line 132; new event struct)
- Modify: `pkg/model/push.go` (`PushNotificationEvent`)
- Modify: `pkg/outbox/outbox.go` (`ConcurrentEventTypes` set)
- Test: `pkg/model/model_test.go`, `pkg/outbox/outbox_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `Subscription.ThreadUnread []string`; `ThreadReadEvent.NewThreadUnread []string`; `model.InboxThreadUnreadAdded InboxEventType = "thread_unread_added"`; `model.ThreadUnreadAddedEvent{RoomID, ParentMessageID string; Accounts []string; Timestamp int64}`; `PushNotificationEvent.UnreadCounts map[string]int`; `BadgeCountBatchRequest{RoomID string; Accounts []string}` / `BadgeCountBatchResponse{Counts map[string]int}` (in `pkg/model/subscription.go`).

- [ ] **Step 1: Write failing model round-trip tests** in `pkg/model/model_test.go`:

```go
func TestSubscriptionJSON_ThreadUnreadRoundTrip(t *testing.T) {
	s := model.Subscription{
		ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", RoomType: model.RoomTypeChannel, SiteID: "site-a",
		Roles: []model.Role{model.RoleMember}, JoinedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		ThreadUnread: []string{"p1", "p2"},
	}
	roundTrip(t, &s, &model.Subscription{})

	s.ThreadUnread = nil
	data, err := json.Marshal(&s)
	require.NoError(t, err)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))
	_, present := raw["threadUnread"]
	assert.False(t, present, "nil ThreadUnread must be omitted")
}

func TestThreadUnreadAddedEventJSON(t *testing.T) {
	src := model.ThreadUnreadAddedEvent{
		RoomID: "r1", ParentMessageID: "p1",
		Accounts: []string{"alice", "bob"}, Timestamp: 1735689600000,
	}
	roundTrip(t, &src, &model.ThreadUnreadAddedEvent{})
}

func TestThreadReadEventJSON_NewThreadUnread(t *testing.T) {
	src := model.ThreadReadEvent{
		Account: "alice", ThreadRoomID: "tr1",
		NewThreadUnread: []string{"p2"}, LastSeenAt: 1735689600000, Timestamp: 1735689600001,
	}
	roundTrip(t, &src, &model.ThreadReadEvent{})
	// Wire-compat: a payload without the field decodes to nil (old producers).
	var dst model.ThreadReadEvent
	require.NoError(t, json.Unmarshal([]byte(`{"account":"a","threadRoomId":"tr","lastSeenAt":1,"timestamp":2}`), &dst))
	assert.Nil(t, dst.NewThreadUnread)
}
```

- [ ] **Step 2: Run** `make test SERVICE=pkg/model` — expect FAIL (fields undefined).

- [ ] **Step 3: Implement.** In `subscription.go` after `HasMention`:

```go
	// ThreadUnread lists parent-message IDs of threads in this room with
	// replies the user hasn't read. Written by message-worker on each thread
	// reply (federated to the user's home replica via thread_unread_added),
	// cleared by message.thread.read / thread.read.all. Feeds the badge count
	// only — precise thread state stays on the derived lastSeenAt model.
	ThreadUnread []string `json:"threadUnread,omitempty" bson:"threadUnread,omitempty"`
```

Also in `subscription.go` (near `MessageThreadReadRequest`):

```go
// BadgeCountBatchRequest is the server-to-server badge.count.batch request:
// notification-worker asks the accounts' home-site user-service for badge
// unread counts, naming the room that triggered the notification.
type BadgeCountBatchRequest struct {
	RoomID   string   `json:"roomId"`
	Accounts []string `json:"accounts"`
}

// BadgeCountBatchResponse maps account → unread-room count capped at 10
// (10 renders as "9+"). Accounts whose count could not be computed are absent.
type BadgeCountBatchResponse struct {
	Counts map[string]int `json:"counts"`
}
```

In `event.go`: add `NewThreadUnread []string \`json:"newThreadUnread,omitempty"\`` to `ThreadReadEvent` (comment: post-$pull array; nil/absent means cleared); add const `InboxThreadUnreadAdded InboxEventType = "thread_unread_added"` next to the other inbox consts; add:

```go
// ThreadUnreadAddedEvent is InboxEvent.Payload for "thread_unread_added":
// one event per destination site per thread reply, Accounts scoped to that
// site's followers. The destination $addToSet-merges ParentMessageID into
// each account's subscription.threadUnread for RoomID (idempotent, so the
// event rides the concurrent outbox lane).
type ThreadUnreadAddedEvent struct {
	RoomID          string   `json:"roomId"`
	ParentMessageID string   `json:"parentMessageId"`
	Accounts        []string `json:"accounts"`
	Timestamp       int64    `json:"timestamp"`
}
```

In `push.go`: add to `PushNotificationEvent`:

```go
	// UnreadCounts maps account → badge unread-room count capped at 10,
	// stamped per recipient at notify time. Accounts whose home-site badge
	// RPC failed are absent — clients refresh the true count on open.
	UnreadCounts map[string]int `json:"unreadCounts,omitempty"`
```

In `pkg/outbox/outbox.go`: add `model.InboxThreadUnreadAdded` to `ConcurrentEventTypes` (read the surrounding set literal and match its style); add/extend the partition test in `pkg/outbox/outbox_test.go` asserting `outbox.Publish` accepts the new type.

- [ ] **Step 4: Run** `make test SERVICE=pkg/model` and `make test SERVICE=pkg/outbox` — expect PASS.

- [ ] **Step 5: Commit** `feat(model): thread-unread badge wire types (ThreadUnread, thread_unread_added, UnreadCounts)`.

---

### Task 2: message-worker — the ThreadUnread producer

**Files:**
- Modify: `message-worker/store.go` (Store interface), `message-worker/store_mongo.go`
- Modify: `message-worker/handler.go` (thread branch of `processMessage`, after `markThreadMentions` — read the current flow at ~lines 118-163 first)
- Regenerate: `message-worker` mocks (`make generate SERVICE=message-worker`)
- Test: `message-worker/handler_test.go`, `message-worker/integration_test.go`

**Interfaces:**
- Consumes: Task 1's `model.ThreadUnreadAddedEvent`, `model.InboxThreadUnreadAdded`; existing `outbox.Publish`, existing per-account home-site lookup (`lookupOwnerSiteID` / userstore cache).
- Produces: store method `AddThreadUnread(ctx context.Context, roomID, parentMessageID string, accounts []string) error`; handler helper `fanOutThreadUnread(ctx context.Context, roomID, parentMessageID, sender string, recipients []string) error`.

- [ ] **Step 1: Write the failing store integration test** in `message-worker/integration_test.go`:

```go
func TestMongoStore_AddThreadUnread(t *testing.T) {
	db := setupMongo(t)
	store := newTestStore(db) // use the file's existing store constructor helper
	ctx := context.Background()

	_, err := db.Collection("subscriptions").InsertMany(ctx, []any{
		&model.Subscription{ID: "sA", RoomID: "r1", User: model.SubscriptionUser{ID: "uA", Account: "alice"}},
		&model.Subscription{ID: "sB", RoomID: "r1", User: model.SubscriptionUser{ID: "uB", Account: "bob"}, ThreadUnread: []string{"p1"}},
		&model.Subscription{ID: "sC", RoomID: "r2", User: model.SubscriptionUser{ID: "uA", Account: "alice"}},
	})
	require.NoError(t, err)

	require.NoError(t, store.AddThreadUnread(ctx, "r1", "p1", []string{"alice", "bob"}))
	// Idempotent under redelivery:
	require.NoError(t, store.AddThreadUnread(ctx, "r1", "p1", []string{"alice", "bob"}))

	var a, b, c model.Subscription
	require.NoError(t, db.Collection("subscriptions").FindOne(ctx, bson.M{"_id": "sA"}).Decode(&a))
	require.NoError(t, db.Collection("subscriptions").FindOne(ctx, bson.M{"_id": "sB"}).Decode(&b))
	require.NoError(t, db.Collection("subscriptions").FindOne(ctx, bson.M{"_id": "sC"}).Decode(&c))
	assert.Equal(t, []string{"p1"}, a.ThreadUnread)
	assert.Equal(t, []string{"p1"}, b.ThreadUnread, "$addToSet must not duplicate")
	assert.Nil(t, c.ThreadUnread, "other rooms untouched")
}
```

- [ ] **Step 2: Run** `make test-integration SERVICE=message-worker` (Docker required) — expect FAIL (method undefined). Without Docker, the compile failure is the red gate.

- [ ] **Step 3: Implement the store method** (`store_mongo.go`, wired to the `subscriptions` collection — add the collection to the store struct/constructor if absent, mirroring `GetHistorySharedSince`'s access):

```go
// AddThreadUnread marks parentMessageID unread for accounts' subscriptions in
// roomID via a single $addToSet UpdateMany. Idempotent under JetStream
// redelivery; accounts not subscribed simply match nothing.
func (s *MongoStore) AddThreadUnread(ctx context.Context, roomID, parentMessageID string, accounts []string) error {
	if len(accounts) == 0 {
		return nil
	}
	if _, err := s.subscriptions.UpdateMany(ctx,
		bson.M{"roomId": roomID, "u.account": bson.M{"$in": accounts}},
		bson.M{"$addToSet": bson.M{"threadUnread": parentMessageID}},
	); err != nil {
		return fmt.Errorf("add thread unread %q in room %q: %w", parentMessageID, roomID, err)
	}
	return nil
}
```

Add to the Store interface in `store.go`; run `make generate SERVICE=message-worker`.

- [ ] **Step 4: Write failing handler tests** (`handler_test.go`, table-driven, mocked store + captured outbox publish — follow the file's existing publish-capture fixture):
  - local-only recipients → one `AddThreadUnread` call with `replyAccounts ∪ mentionees − sender`, zero outbox publishes;
  - one remote-home recipient → one `thread_unread_added` outbox publish to that site whose payload `Accounts` contains exactly the remote accounts for that site;
  - two remote recipients on the same site → **one** event for that site;
  - sender excluded even when in `replyAccounts`;
  - migration-tagged message (`isMigration`) → no store call, no publish;
  - store error → handler returns error (NAK path).

- [ ] **Step 5: Run** `make test SERVICE=message-worker` — expect FAIL (helper undefined / calls missing).

- [ ] **Step 6: Implement `fanOutThreadUnread`** in `handler.go` and call it from the thread branch after `markThreadMentions` (so mentionees are included), before the Cassandra save; propagate its error (redelivery-safe: both the UpdateMany and the outbox publish are idempotent — the outbox dedup ID below):

```go
// fanOutThreadUnread marks the reply's parent thread unread for every
// follower except the sender: one local UpdateMany, plus one
// thread_unread_added outbox event per remote home site so each follower's
// origin-site replica converges.
func (h *Handler) fanOutThreadUnread(ctx context.Context, roomID, parentMessageID, sender string, recipients []string) error {
	accounts := make([]string, 0, len(recipients))
	seen := map[string]struct{}{sender: {}}
	for _, a := range recipients {
		if _, dup := seen[a]; dup {
			continue
		}
		seen[a] = struct{}{}
		accounts = append(accounts, a)
	}
	if len(accounts) == 0 {
		return nil
	}
	if err := h.threadStore.AddThreadUnread(ctx, roomID, parentMessageID, accounts); err != nil {
		return fmt.Errorf("add thread unread: %w", err)
	}

	bySite := map[string][]string{}
	for _, a := range accounts {
		site, err := h.lookupOwnerSiteIDByAccount(ctx, a) // cache-backed; reuse the file's existing site-lookup helper shape
		if err != nil || site == "" || site == h.siteID {
			continue // unknown/local homes need no federation; lookup errors degrade (badge converges via reseed)
		}
		bySite[site] = append(bySite[site], a)
	}
	now := time.Now().UTC().UnixMilli()
	for site, accs := range bySite {
		payload, err := json.Marshal(model.ThreadUnreadAddedEvent{
			RoomID: roomID, ParentMessageID: parentMessageID, Accounts: accs, Timestamp: now,
		})
		if err != nil {
			return fmt.Errorf("marshal thread_unread_added: %w", err)
		}
		dedupID := fmt.Sprintf("thread-unread:%s:%s:%s", parentMessageID, h.msgID(ctx), site) // reuse the message ID already in scope; stable across redeliveries
		if err := outbox.Publish(ctx, h.publishToStream, h.siteID, roomID, site, model.InboxThreadUnreadAdded, payload, dedupID, now); err != nil {
			return fmt.Errorf("federate thread_unread_added to %s: %w", site, err)
		}
	}
	return nil
}
```

Adapt the two helper call shapes (`lookupOwnerSiteIDByAccount`, `msgID`) to what the file actually provides (`lookupOwnerSiteID` takes a user — read it first); recipients at the call sites: first-reply path `[parentAuthor] + mentionees`, subsequent-reply path `threadRoom.ReplyAccounts + mentionees`.

- [ ] **Step 7: Run** `make test SERVICE=message-worker` then `make test-integration SERVICE=message-worker` — expect PASS.

- [ ] **Step 8: Commit** `feat(message-worker): fan out threadUnread on thread replies`.

---

### Task 3: clear paths — room-service + inbox-worker (resurrection, no alert)

**Files:**
- Modify: `room-service/store.go`, `room-service/store_mongo.go`, `room-service/handler.go` (`messageThreadRead` ~1557, `clearAllThreadRead` ~1646)
- Modify: `inbox-worker/handler.go` (Store interface + `handleThreadRead`), `inbox-worker/main.go` (`ApplyThreadRead`, `ApplyThreadReadAll`)
- Regenerate: both services' mocks (room-service via `make generate`; inbox-worker via its recorded reflect-mode command `cd inbox-worker && mockgen -destination=mock_store_test.go -package=main github.com/hmchangw/chat/inbox-worker InboxStore`)
- Test: `room-service/handler_test.go`, `room-service/integration_test.go`, `inbox-worker/handler_test.go`, `inbox-worker/integration_test.go`

**Interfaces:**
- Consumes: Task 1's `ThreadReadEvent.NewThreadUnread`.
- Produces: room-service store `UpdateSubscriptionThreadRead(ctx, roomID, account, threadID string) ([]string, error)` (post-$pull array, nil when empty/unset) and `ClearSubscriptionThreadUnreadForAccount(ctx, account string) error`; inbox-worker store `ApplyThreadRead(ctx, roomID, threadRoomID, account string, newThreadUnread []string, lastSeenAt time.Time) error` and `ApplyThreadReadAll` (existing signature, regains the subscription `$unset`).

Reference for all resurrected bodies: `git show 986ce7b^:room-service/store_mongo.go` and `git show 71c2ca1^:inbox-worker/main.go` — reuse those shapes **minus every `alert` write** (`$set {alert:...}` lines dropped; `$unset {threadUnread}` kept).

- [ ] **Step 1: Red — handler tests.** In `room-service/handler_test.go`: re-add `UpdateSubscriptionThreadRead` expectations to `TestHandler_MessageThreadRead_Happy` (return `(nil, nil)`) and the CrossSite test (return `([]string{"p2"}, nil)`, and assert the federated `inner.NewThreadUnread == []string{"p2"}`); re-add a `ClearSubscriptionThreadUnreadForAccount` expectation to each `TestHandler_ClearAllThreadRead_*`. In `inbox-worker/handler_test.go`: extend the `threadRead` capture struct with `roomID`/`newThreadUnread`, update the stub, and assert `handleThreadRead` forwards `e.RoomID`/`e.NewThreadUnread`. Run both `make test SERVICE=...` — expect FAIL (methods/args missing).

- [ ] **Step 2: Green — stores.** room-service `store_mongo.go`:

```go
// UpdateSubscriptionThreadRead removes threadID from threadUnread via $pull
// and returns the resulting array (nil when empty; the field is $unset so an
// empty array is never stored). Missing subscription → ErrSubscriptionNotFound.
func (s *MongoStore) UpdateSubscriptionThreadRead(ctx context.Context, roomID, account, threadID string) ([]string, error) {
	filter := bson.M{"roomId": roomID, "u.account": account}
	opts := options.FindOneAndUpdate().SetReturnDocument(options.After)
	var updated model.Subscription
	err := s.subscriptions.FindOneAndUpdate(ctx, filter,
		bson.M{"$pull": bson.M{"threadUnread": threadID}}, opts).Decode(&updated)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, fmt.Errorf("update subscription thread-read for %q in room %q: %w", account, roomID, model.ErrSubscriptionNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("update subscription thread-read for %q in room %q: %w", account, roomID, err)
	}
	if len(updated.ThreadUnread) == 0 {
		if _, err := s.subscriptions.UpdateOne(ctx, filter, bson.M{"$unset": bson.M{"threadUnread": ""}}); err != nil {
			slog.WarnContext(ctx, "unset empty threadUnread", "error", err, "account", account, "roomID", roomID)
		}
		return nil, nil
	}
	return updated.ThreadUnread, nil
}

// ClearSubscriptionThreadUnreadForAccount removes threadUnread from every one
// of account's subscriptions that has unread threads. alert is untouched.
func (s *MongoStore) ClearSubscriptionThreadUnreadForAccount(ctx context.Context, account string) error {
	if _, err := s.subscriptions.UpdateMany(ctx,
		bson.M{"u.account": account, "threadUnread.0": bson.M{"$exists": true}},
		bson.M{"$unset": bson.M{"threadUnread": ""}},
	); err != nil {
		return fmt.Errorf("clear subscription thread-unread for %q: %w", account, err)
	}
	return nil
}
```

Handler wiring: `messageThreadRead` runs the pair concurrently again (errgroup: `UpdateSubscriptionThreadRead` capturing `newThreadUnread`, `UpdateThreadSubscriptionRead`) and sets `NewThreadUnread: newThreadUnread` on the federated payload; `clearAllThreadRead` regains the `ClearSubscriptionThreadUnreadForAccount` errgroup leg. inbox-worker: `ApplyThreadRead` regains `roomID`+`newThreadUnread` params and, after the `$lt`-guarded thread-sub update **matches** (restore the `MatchedCount` gate from the pre-removal body), applies `$set {threadUnread: newThreadUnread}` or `$unset` when empty; `ApplyThreadReadAll` regains `UpdateMany {u.account, threadUnread.0 exists} $unset`. `handleThreadRead` passes `e.RoomID, e.ThreadRoomID, e.Account, e.NewThreadUnread, lastSeenAt`.

- [ ] **Step 3: Integration tests** (adapt the pre-removal tests from `git show 986ce7b^:room-service/integration_test.go` and `git show 71c2ca1^:inbox-worker/integration_test.go`, dropping every alert seed/assert): `$pull`+`$unset` shape, missing-sub sentinel, concurrent removals, `ApplyThreadRead` guard-gated subscription write, `ApplyThreadReadAll` bulk clear with bystander untouched.

- [ ] **Step 4: Run** unit + integration for both services — expect PASS. **Step 5: Commit** `feat(room-service,inbox-worker): resurrect threadUnread clear paths without alert coupling`.

---

### Task 4: inbox-worker — `thread_unread_added` apply

**Files:**
- Modify: `inbox-worker/handler.go` (Store interface + dispatch switch), `inbox-worker/main.go`
- Regenerate: inbox-worker mock (reflect-mode command above)
- Test: `inbox-worker/handler_test.go`, `inbox-worker/integration_test.go`

**Interfaces:**
- Consumes: Task 1's `ThreadUnreadAddedEvent` / `InboxThreadUnreadAdded`.
- Produces: store `AddThreadUnread(ctx, roomID, parentMessageID string, accounts []string) error` (identical body to Task 2's — inbox-worker's store is a separate type; copy the implementation, it is 12 lines).

- [ ] **Step 1: Red.** Handler test: a `thread_unread_added` `InboxEvent` routes to the stub's `AddThreadUnread` with the payload fields; malformed payload errors; store error propagates. Run — FAIL (no case in the dispatch switch).
- [ ] **Step 2: Green.** Add the dispatch case (`case model.InboxThreadUnreadAdded: h.handleThreadUnreadAdded(ctx, evt)` following the file's existing per-type handler pattern), the 10-line handler (unmarshal → `h.store.AddThreadUnread`), and the store method on `mongoInboxStore` (same `UpdateMany $addToSet` as Task 2 Step 3). Integration test mirrors Task 2 Step 1 against the replica collection.
- [ ] **Step 3: Run** unit + integration — PASS. **Step 4: Commit** `feat(inbox-worker): apply thread_unread_added to home replicas`.

---

### Task 5: user-service — local thread phase + `unreadRooms`

**Files:**
- Modify: `user-service/service/subscriptions.go` (`countUnread` ~555-757), `user-service/mongorepo/subscriptions.go` (projection ~line 180)
- Test: `user-service/service/subscriptions_test.go`

**Interfaces:**
- Consumes: `Subscription.ThreadUnread` (populated on the fetched page once the projection line returns).
- Produces: `unreadRooms(c *natsrouter.Context, account string) ([]string, error)` — unread room IDs (local rooms compared via the `$lookup` baseline, cross-site via the existing `GetRoomsInfo` fan-out, plus any read room whose sub's `ThreadUnread` is non-empty). `countUnread` becomes `len(unreadRooms(...))`. **`GetThreadRoomInfoBatch` and `ListByAccountInRooms` calls disappear from the count path only.**

- [ ] **Step 1: Red.** Rewrite the thread-bump cases in `subscriptions_test.go`: read room with `ThreadUnread: []string{"p1"}` on the sub → +1; three unread threads in one room → +1; already-unread room with threads → still +1; muted excluded (existing filter). Remove the `threadSubs.ListByAccount*` / `GetThreadRoomInfoBatch` mock expectations from count tests (gomock reports the handler's now-unexpected calls — the red signal). Run — FAIL.
- [ ] **Step 2: Green.** Refactor `countUnread` → `unreadRooms` returning IDs (append local unread, cross-site unread, then for each sub in `pendingRooms` with `len(sub.ThreadUnread) > 0` append once); delete `countThreadOnlyUnread` and its helpers; restore `"threadUnread": 1,` to the subscription projection. The count handler returns `{Count: len(ids)}`.
- [ ] **Step 3: Run** `make test SERVICE=user-service` — PASS. **Step 4: Commit** `feat(user-service): subscription.count thread phase reads local threadUnread`.

---

### Task 6: Phase A docs + gates

**Files:** `docs/client-api.md`, `docs/client-api/request-reply.md`, `data-migration/CDC_COVERAGE.md`

- [ ] **Step 1:** `client-api.md`: re-add the Subscription table row `| threadUnread | string[] | Optional. Parent message IDs of threads with unread replies. |`; `message.thread.read` behaviour notes regain the subscription-write + federated `newThreadUnread` description (explicitly: alert untouched); Clear All Thread Unread prose regains "and room-subscription thread-unread state (`threadUnread` removed)"; `subscription.count` note: a room also counts as unread when its subscription's `threadUnread` is non-empty (no cross-site thread fetch). Sync the same sentences in `request-reply.md`. `CDC_COVERAGE.md` `thread_read` rationale: "`Subscription.ThreadUnread` is message-pipeline-owned (producer: message-worker `thread_unread_added`)".
- [ ] **Step 2:** `make lint && make test && make sast` — all PASS; `make test-integration` for the four touched services with Docker.
- [ ] **Step 3: Commit** `docs(client-api): threadUnread schema + behaviour notes (Phase A)`. Phase A is shippable here.

---

### Task 7: `pkg/badgecache` — Valkey unread-room set

**Files:**
- Create: `pkg/badgecache/badgecache.go`, `pkg/badgecache/badgecache_integration_test.go`

**Interfaces:**
- Consumes: `github.com/redis/go-redis/v9` cluster client (the repo's existing Valkey dependency), `pkg/testutil.SharedValkeyCluster`.
- Produces:

```go
func New(rdb redis.UniversalClient, ttl time.Duration) *Cache
func (c *Cache) Bump(ctx context.Context, account, roomID string) (count int, ok bool)   // SADD+EXPIRE+SCARD, capped 10; ok=false → cache miss or error
func (c *Cache) Seed(ctx context.Context, account string, roomIDs []string, triggerRoomID string) (count int, ok bool)
func (c *Cache) ClearRoom(ctx context.Context, account, roomID string)                    // fail-open, no return
func (c *Cache) ClearAll(ctx context.Context, account string)
func (c *Cache) Reseed(ctx context.Context, account string, roomIDs []string)             // DEL + SADD + EXPIRE, fail-open
func Key(account string) string                                                           // "badge:{" + account + "}" — hash-tagged for cluster single-slot Lua
```

- [ ] **Step 1: Red — integration tests** (`//go:build integration`, `TestMain` via `testutil.RunTests`, `t.Cleanup(func() { testutil.FlushValkey(t) })` per test):

```go
func TestBadgeCache_BumpMissThenSeedThenBump(t *testing.T) {
	rdb := testutil.SharedValkeyCluster(t)
	t.Cleanup(func() { testutil.FlushValkey(t) })
	c := badgecache.New(rdb, time.Hour)
	ctx := context.Background()

	_, ok := c.Bump(ctx, "alice", "roomB")
	assert.False(t, ok, "no key yet → miss")

	n, ok := c.Seed(ctx, "alice", []string{"roomA"}, "roomB")
	require.True(t, ok)
	assert.Equal(t, 2, n, "seed ∪ trigger")

	n, ok = c.Bump(ctx, "alice", "roomB")
	require.True(t, ok)
	assert.Equal(t, 2, n, "SADD idempotent")

	n, ok = c.Bump(ctx, "alice", "roomC")
	require.True(t, ok)
	assert.Equal(t, 3, n)

	c.ClearRoom(ctx, "alice", "roomA")
	n, _ = c.Bump(ctx, "alice", "roomC")
	assert.Equal(t, 2, n)

	c.ClearAll(ctx, "alice")
	_, ok = c.Bump(ctx, "alice", "roomC")
	assert.False(t, ok, "cleared key → miss again")
}

func TestBadgeCache_CapAt10(t *testing.T) {
	rdb := testutil.SharedValkeyCluster(t)
	t.Cleanup(func() { testutil.FlushValkey(t) })
	c := badgecache.New(rdb, time.Hour)
	rooms := make([]string, 15)
	for i := range rooms { rooms[i] = fmt.Sprintf("room%02d", i) }
	n, ok := c.Seed(context.Background(), "alice", rooms, "roomX")
	require.True(t, ok)
	assert.Equal(t, 10, n)
}
```

- [ ] **Step 2: Run** `make test-integration SERVICE=pkg/badgecache` — FAIL (package absent).
- [ ] **Step 3: Implement.** Bump = Lua `if EXISTS==0 return miss; SADD; EXPIRE; return SCARD`; Seed = Lua `SADD all∪trigger; EXPIRE; return SCARD` (register scripts with `redis.NewScript`); cap in Go with `min(n, 10)`; every error path returns `ok=false` after a `slog.WarnContext` (fail-open — callers degrade). `ClearAll` = `DEL`; `ClearRoom` = `SREM`.
- [ ] **Step 4: Run** — PASS. **Step 5: Commit** `feat(badgecache): per-user Valkey unread-room set`.

---

### Task 8: user-service — `badge.count.batch` RPC + cache wiring

**Files:**
- Modify: `pkg/subject/subject.go` (+ test): `BadgeCountBatch(siteID)` → `chat.server.request.user.{siteID}.badge.count.batch` (+ pattern func, mirroring `RoomThreadReadAll`'s server-to-server pair)
- Modify: `user-service/config/config.go` (Valkey addrs env, mirroring an existing service's Valkey config block), `user-service/main.go` (client + `badgecache.New` wiring, TTL 24h), `user-service/service/service.go` (register handler; `UserService` gains `badge *badgecache.Cache`)
- Create: `user-service/service/badge.go`, `user-service/service/badge_test.go`

**Interfaces:**
- Consumes: Task 1's request/response types, Task 5's `unreadRooms`, Task 7's `Cache`.
- Produces: handler `BadgeCountBatch(c *natsrouter.Context, req model.BadgeCountBatchRequest) (*model.BadgeCountBatchResponse, error)`; `subscription.count` handler gains a post-count `c.badge.Reseed(ctx, account, ids)` (fail-open, after replying is fine — same goroutine, best-effort).

- [ ] **Step 1: Red — handler tests** (mocked repo + a fake cache injected via a small `badgeCache` interface declared in `service.go` with exactly the `Bump/Seed/Reseed` methods the service consumes — consumer-defined interface per CLAUDE.md): hit → `Bump` count returned, no repo call; miss → `unreadRooms` called once, `Seed` with trigger room, count returned; `unreadRooms` error for one account → that account absent, others present; empty accounts → `BadRequest`.
- [ ] **Step 2:** Implement:

```go
// BadgeCountBatch returns each account's badge unread-room count (capped at
// 10) for a notification in req.RoomID. Cache hit: SADD+SCARD only. Miss:
// seed from unreadRooms (Mongo + cross-site room RPCs) ∪ the trigger room.
// Per-account failures degrade to absence — the push must never block.
func (s *UserService) BadgeCountBatch(c *natsrouter.Context, req model.BadgeCountBatchRequest) (*model.BadgeCountBatchResponse, error) {
	if len(req.Accounts) == 0 || req.RoomID == "" {
		return nil, errcode.BadRequest("roomId and accounts are required")
	}
	resp := &model.BadgeCountBatchResponse{Counts: make(map[string]int, len(req.Accounts))}
	for _, account := range req.Accounts {
		if n, ok := s.badge.Bump(c, account, req.RoomID); ok {
			resp.Counts[account] = n
			continue
		}
		ids, err := s.unreadRooms(c, account)
		if err != nil {
			slog.WarnContext(c, "badge seed degraded", "account", account, "error", err)
			continue
		}
		if n, ok := s.badge.Seed(c, account, ids, req.RoomID); ok {
			resp.Counts[account] = n
			continue
		}
		// Cache down entirely: compute without it (ids ∪ trigger, capped).
		resp.Counts[account] = cappedUnion(ids, req.RoomID)
	}
	return resp, nil
}
```

with `func cappedUnion(ids []string, trigger string) int` (10-cap, membership check) beside it. Register on `subject.BadgeCountBatchPattern(s.siteID)`.
- [ ] **Step 3:** `subscription.count` reseed line + wiring in `main.go`/config (required env only if badge enabled — provide `envDefault` empty ⇒ cache disabled ⇒ `badge` is a no-op implementation so Phase A deploys don't need Valkey).
- [ ] **Step 4: Run** `make test SERVICE=user-service` + subject tests — PASS. **Step 5: Commit** `feat(user-service): badge.count.batch RPC backed by badgecache`.

---

### Task 9: SREM hooks — room-service + inbox-worker

**Files:**
- Modify: `room-service/main.go` + config (Valkey wiring, same optional-env pattern as Task 8), `room-service/handler.go` (`messageRead`, `messageThreadRead`, `clearAllThreadRead`), `room-service/store_mongo.go` (re-add `{Key: "threadUnread", Value: 1},` to `subscriptionReadProjection` + projection test seed/assert)
- Modify: `inbox-worker/main.go` + `inbox-worker/handler.go` (badge cache on the store/handler; SREM in `subscription_read`, `thread_read`, `thread_read_all`, `member_removed` applies)
- Test: both services' handler tests (fake cache interface), projection integration test

**Interfaces:**
- Consumes: Task 7's `ClearRoom`/`ClearAll` (via a consumer-defined 2-method interface in each service; a no-op impl when Valkey unconfigured).
- Produces: clear rules — `message.read`: `ClearRoom` iff `len(sub.ThreadUnread) == 0`, only when `userSiteID == h.siteID`; `message.thread.read`: `ClearRoom` when the post-`$pull` array is empty (home-local reader); `thread.read.all`: `ClearAll` (home-local); inbox-worker mirrors the same rules for federated reads; `member_removed`: `ClearRoom`.

- [ ] **Step 1: Red.** Handler tests assert the fake cache receives exactly the calls above (and, e.g., `message.read` with `ThreadUnread: ["p1"]` on the fetched sub → **no** `ClearRoom`). Run — FAIL.
- [ ] **Step 2: Green.** Wire the calls (each one line, after the existing writes, never affecting the reply; guard with the site check where specified). Restore the projection line + test.
- [ ] **Step 3: Run** both services' unit + integration tests — PASS. **Step 4: Commit** `feat(room-service,inbox-worker): badge cache clear hooks on read paths`.

---

### Task 10: notification-worker — stamp `UnreadCounts`

**Files:**
- Modify: `pkg/roomsubcache/roomsubcache.go` (`Member` gains `SiteID string`; bump the cache key version/namespace constant so old entries expire), `notification-worker/main.go` (projection adds `siteId`; badge RPC client wiring), `notification-worker/handler.go` (post-filter grouping + stamping), `notification-worker/emit.go` (payload passthrough)
- Test: `notification-worker/handler_test.go`, `pkg/roomsubcache` tests

**Interfaces:**
- Consumes: Task 8's RPC (via a consumer-defined `badgeClient` interface: `Counts(ctx context.Context, siteID, roomID string, accounts []string) (map[string]int, error)` implemented with `nc.Request` on `subject.BadgeCountBatch(siteID)`, 5s timeout).
- Produces: `PushNotificationEvent.UnreadCounts` populated for every survivor whose home-site RPC succeeded; absent entries for failed sites; batches unchanged otherwise.

- [ ] **Step 1: Red.** Handler tests: survivors on two home sites → two RPC calls, each with that site's accounts, merged map stamped on every outgoing batch; one site errors → its accounts absent, publish still happens, WARN logged; badge client disabled (nil) → no RPC, no map (Phase A compatibility). `roomsubcache` test: `SiteID` round-trips through the cache codec.
- [ ] **Step 2: Green.** Group `survivors` by `m.SiteID` (empty → skip, degrade); issue the per-site RPCs concurrently (errgroup, results into a mutex-guarded merge map); stamp `UnreadCounts` filtered to each batch's accounts in the chunk loop. RPC failures must not return an error from `HandleMessage` (the push ships without counts).
- [ ] **Step 3: Run** `make test SERVICE=notification-worker` + `make test SERVICE=pkg/roomsubcache` — PASS. **Step 4: Commit** `feat(notification-worker): per-recipient badge counts on push batches`.

---

### Task 11: Phase B docs + full gates + push

**Files:** `docs/notification-worker-downstream-contracts.md`, `docs/client-api.md` (subscription.count note already done in Task 6 — verify), spec status line.

- [ ] **Step 1:** Document in the downstream-contracts doc: the `unreadCounts` field (shape, cap, absence semantics), the `badge.count.batch` subject/request/response, and the accuracy model one-liner (triggering room exact; other divergences ±1 bounded by TTL/reseed).
- [ ] **Step 2:** Full gates: `make lint`, `make fmt` (no diff), `make test`, `make test-integration` (Docker), `make sast`. Fix anything found.
- [ ] **Step 3: Commit** `docs: badge count contracts (Phase B)` and `git push -u origin claude/threadunread-tracking-analysis-wu3g77` (with network-failure retries: 2s/4s/8s/16s backoff).
