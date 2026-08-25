# Thread Delivery During a Cassandra Outage — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver channel thread replies while Cassandra is down, and refuse — visibly — the one case that cannot be delivered.

**Architecture:** Both consumers already query the `thread_rooms` document keyed by `parentMessageId`; widening that projection by one field gives them the thread parent's `createdAt` without Cassandra. When no source resolves the parent, fan-out fails closed (followers keep the message, unverifiable mentionees are dropped) rather than erroring and being dropped by JetStream. The one genuinely unresolvable case — a thread first replied to during the outage — is refused at the gatekeeper, and `chat-frontend` learns to observe its send reply so the user is told.

**Tech Stack:** Go 1.25, NATS JetStream, MongoDB (`go.mongodb.org/mongo-driver/v2`), `go.uber.org/mock`, `testify`, `testcontainers-go`; React 19 + Vite + Vitest on the frontend.

**Spec:** `docs/superpowers/specs/2026-08-25-thread-delivery-cassandra-outage-design.md`

## Global Constraints

- **TDD is mandatory** (CLAUDE.md §4): write the test, run it, confirm it FAILS, then implement. Never write implementation first.
- **Always use `make` targets**, never raw `go` commands. `make test SERVICE=<name>`, `make lint`, `make generate SERVICE=<name>`, `make test-integration SERVICE=<name>`.
- **Run `make generate SERVICE=<name>` whenever a store interface changes** — before running tests. Never hand-edit `mock_*_test.go`.
- **Minimum 80% coverage**; 90%+ for handlers and `pkg/`.
- **Error wrapping:** `fmt.Errorf("short description: %w", err)` describing what *this* function was doing. Never bare `err`, never `"error: %w"`.
- **Client-facing errors** use `pkg/errcode` constructors; domain reasons live in `pkg/errcode/codes_<service>.go`.
- **Logging** is `log/slog` with key-value pairs. Never log tokens, passwords, or message bodies.
- **A pre-commit hook runs lint and tests.** Fix failures before retrying the commit.
- **Never commit to `main`.** All work lands on `claude/thread-messages-cassandra-down-fje1p9`.
- **Two PRs.** Tasks 1–9 are PR 1; Tasks 10–12 are PR 2. Do not open a PR until asked.
- **Delete everything under `docs/reviews/`** before creating a PR.

---

## File Structure

**PR 1 — server fix, gatekeeper refusal, send observability**

| File | Responsibility |
|---|---|
| `pkg/errcode/transient.go` (create) | One definition of transient-vs-terminal for an `errcode`-carrying error |
| `pkg/errcode/transient_test.go` (create) | Table-driven coverage of every category |
| `message-gatekeeper/handler.go` (modify) | Use the shared predicate; refuse an unresolvable thread start |
| `message-gatekeeper/store.go`, `store_mongo.go` (modify) | `ThreadRoomExists` + the `thread_rooms` collection |
| `pkg/errcode/codes_message.go` (modify) | `MessageThreadStartUnavailable` reason |
| `broadcast-worker/store.go`, `store_mongo.go` (modify) | `ThreadRoomInfo` + `GetThreadRoom` replacing `GetThreadFollowers` |
| `broadcast-worker/nats_metrics.go` (modify) | Degraded-fan-out counter |
| `broadcast-worker/handler.go` (modify) | Resolution order + fail-closed degrade |
| `notification-worker/threads.go`, `handler.go` (modify) | The same fallback |
| `chat-frontend/src/api/sendMessage/index.ts` (modify) | Observe the send reply |
| `chat-frontend/scripts/threadOutage.smoke.mjs` (create) | Live-stack outage smoke test |
| `docs/client-api.md` + both derived views (modify) | The new `msg.send` error case |

**PR 2 — client prevention**

| File | Responsibility |
|---|---|
| `chat-frontend/src/context/DegradedContext/` (create) | Site-degraded state and its provider |
| `chat-frontend/src/components/shared/MessageList/MessageRow/MessageActions/MessageActions.jsx` (modify) | Disable thread start while degraded |
| `chat-frontend/src/components/MainApp/ThreadRightBar/ThreadMessageInput/` (modify) | Handle a mid-compose refusal |

---

# PR 1

## Task 1: Shared transient-vs-terminal predicate

`message-gatekeeper` owns the only definition today (`handler.go:574`, unexported). Three services need it.

**Files:**
- Create: `pkg/errcode/transient.go`
- Create: `pkg/errcode/transient_test.go`
- Modify: `message-gatekeeper/handler.go:565-585` (replace `quoteFetchErrIsTerminal`)

**Interfaces:**
- Consumes: nothing.
- Produces: `func errcode.IsTransient(err error) bool` — used by Tasks 4, 6 and 7.

- [ ] **Step 1: Write the failing test**

Create `pkg/errcode/transient_test.go`:

```go
package errcode

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsTransient(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil is not transient", nil, false},
		{"unavailable is transient", Unavailable("history down"), true},
		{"internal is transient", Internal("cassandra read failed"), true},
		{"not found is terminal", NotFound("message not found"), false},
		{"forbidden is terminal", Forbidden("no access"), false},
		{"bad request is terminal", BadRequest("bad id"), false},
		{"conflict is terminal", Conflict("already exists"), false},
		{"plain error is transient", errors.New("unmarshal failed"), true},
		{"wrapped unavailable is transient", fmt.Errorf("fetch: %w", Unavailable("down")), true},
		{"wrapped not found is terminal", fmt.Errorf("fetch: %w", NotFound("gone")), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsTransient(tt.err))
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=pkg/errcode`
Expected: FAIL — `undefined: IsTransient`

- [ ] **Step 3: Write minimal implementation**

Create `pkg/errcode/transient.go`:

```go
package errcode

import "errors"

// IsTransient reports whether err is a retryable infrastructure failure rather
// than a permanent domain answer. A typed *Error is transient only for the
// unavailable and internal categories — history-service collapses a Cassandra
// read failure to internal, so internal must stay retryable. Every other
// category (not_found, forbidden, bad_request, …) is a settled answer that will
// not change on retry. A non-errcode error is an unclassified infra failure
// (unmarshal, transport) and counts as transient.
func IsTransient(err error) bool {
	if err == nil {
		return false
	}
	var ee *Error
	if errors.As(err, &ee) {
		return ee.Code == CodeUnavailable || ee.Code == CodeInternal
	}
	return true
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=pkg/errcode`
Expected: PASS

- [ ] **Step 5: Point the gatekeeper at it**

In `message-gatekeeper/handler.go`, delete the `quoteFetchErrIsTerminal` function (lines 565-585) and change its single call site at line 543 from:

```go
		if quoteFetchErrIsTerminal(err) && errors.As(err, &ee) {
```

to:

```go
		if !errcode.IsTransient(err) && errors.As(err, &ee) {
```

The semantics are identical: terminal was true only for a typed errcode outside {unavailable, internal}, and false for a non-errcode error.

- [ ] **Step 6: Run the gatekeeper suite**

Run: `make test SERVICE=message-gatekeeper`
Expected: PASS. If a test referenced `quoteFetchErrIsTerminal` by name, retarget it at `errcode.IsTransient` rather than reinstating the old function.

- [ ] **Step 7: Commit**

```bash
git add pkg/errcode/transient.go pkg/errcode/transient_test.go message-gatekeeper/handler.go message-gatekeeper/handler_test.go
git commit -m "refactor(errcode): promote the transient-vs-terminal predicate out of message-gatekeeper"
```

---

## Task 2: broadcast-worker reads the parent createdAt from thread_rooms

`GetThreadFollowers` already does a `FindOne` on `thread_rooms` by `parentMessageId` and projects `replyAccounts`. The same document carries `threadParentCreatedAt`.

**Files:**
- Modify: `broadcast-worker/store.go:22` (interface)
- Modify: `broadcast-worker/store_mongo.go:162-181` (implementation)
- Modify: `broadcast-worker/store_mongo_test.go` (if it asserts the old method)
- Regenerate: `broadcast-worker/mock_store_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `type ThreadRoomInfo struct { Followers map[string]struct{}; ParentCreatedAt *time.Time }`
  - `GetThreadRoom(ctx context.Context, parentMessageID string) (ThreadRoomInfo, error)` on `Store`, replacing `GetThreadFollowers`. Used by Task 4.

- [ ] **Step 1: Write the failing test**

Append to `broadcast-worker/store_mongo_test.go`:

```go
func TestThreadRoomInfo_ZeroParentCreatedAtIsUnknown(t *testing.T) {
	// A zero threadParentCreatedAt must surface as nil, never as the epoch:
	// mentionVisible treats a nil parent time as "not visible" (fail closed),
	// while time.Time{} would compare as older than every historySharedSince
	// and admit every mentionee.
	info := threadRoomInfoFrom([]string{"alice", "bob"}, time.Time{})
	assert.Nil(t, info.ParentCreatedAt)
	assert.Len(t, info.Followers, 2)
}

func TestThreadRoomInfo_RealParentCreatedAtIsCarried(t *testing.T) {
	at := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	info := threadRoomInfoFrom([]string{"alice", ""}, at)
	require.NotNil(t, info.ParentCreatedAt)
	assert.Equal(t, at, *info.ParentCreatedAt)
	assert.Len(t, info.Followers, 1, "empty accounts are skipped")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=broadcast-worker`
Expected: FAIL — `undefined: threadRoomInfoFrom`

- [ ] **Step 3: Write minimal implementation**

In `broadcast-worker/store.go`, add the type above the `Store` interface and swap the method:

```go
// ThreadRoomInfo is the thread_rooms projection the channel thread fan-out needs.
// ParentCreatedAt is nil when the document is absent or carries a zero timestamp —
// "unknown", never the epoch (see threadRoomInfoFrom).
type ThreadRoomInfo struct {
	Followers       map[string]struct{}
	ParentCreatedAt *time.Time
}
```

Replace the `GetThreadFollowers` line in the interface with:

```go
	// GetThreadRoom returns the thread's followers and the parent's createdAt from
	// one thread_rooms read. A missing document yields an empty Followers map and a
	// nil ParentCreatedAt, not an error — the thread may simply not exist yet.
	GetThreadRoom(ctx context.Context, parentMessageID string) (ThreadRoomInfo, error)
```

In `broadcast-worker/store_mongo.go`, replace `GetThreadFollowers` (lines 162-181) with:

```go
func (m *mongoStore) GetThreadRoom(ctx context.Context, parentMessageID string) (ThreadRoomInfo, error) {
	var doc struct {
		ReplyAccounts         []string  `bson:"replyAccounts"`
		ThreadParentCreatedAt time.Time `bson:"threadParentCreatedAt"`
	}
	opts := options.FindOne().SetProjection(bson.M{"replyAccounts": 1, "threadParentCreatedAt": 1, "_id": 0})
	err := m.threadRoomCol.FindOne(ctx, bson.M{"parentMessageId": parentMessageID}, opts).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return ThreadRoomInfo{Followers: map[string]struct{}{}}, nil
		}
		return ThreadRoomInfo{}, fmt.Errorf("find thread room by parent %s: %w", parentMessageID, err)
	}
	return threadRoomInfoFrom(doc.ReplyAccounts, doc.ThreadParentCreatedAt), nil
}

// threadRoomInfoFrom builds the projection, mapping a zero parent timestamp to nil.
// model.ThreadRoom.ThreadParentCreatedAt is a non-pointer time.Time, so an
// unresolved parent persists as the zero value rather than as absent.
func threadRoomInfoFrom(replyAccounts []string, parentCreatedAt time.Time) ThreadRoomInfo {
	out := make(map[string]struct{}, len(replyAccounts))
	for _, a := range replyAccounts {
		if a != "" {
			out[a] = struct{}{}
		}
	}
	info := ThreadRoomInfo{Followers: out}
	if !parentCreatedAt.IsZero() {
		at := parentCreatedAt.UTC()
		info.ParentCreatedAt = &at
	}
	return info
}
```

- [ ] **Step 4: Regenerate mocks and run**

Run: `make generate SERVICE=broadcast-worker && make test SERVICE=broadcast-worker`
Expected: the two new tests PASS. Existing handler tests will FAIL to compile on `EXPECT().GetThreadFollowers(...)` — Task 4 fixes them. If you want a green tree at this commit, mechanically rename those expectations to `GetThreadRoom` returning `ThreadRoomInfo{Followers: <old map>}`, which preserves their current behaviour.

- [ ] **Step 5: Commit**

```bash
git add broadcast-worker/store.go broadcast-worker/store_mongo.go broadcast-worker/store_mongo_test.go broadcast-worker/mock_store_test.go broadcast-worker/handler_test.go
git commit -m "feat(broadcast-worker): read the thread parent's createdAt from thread_rooms"
```

---

## Task 3: Degraded-fan-out metric

**Files:**
- Modify: `broadcast-worker/nats_metrics.go`
- Modify: `broadcast-worker/nats_metrics_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `(*broadcastMetrics).ThreadFanOutDegraded(ctx context.Context, reason string)` — called by Task 4. Valid reasons: `"no_thread_room"`, `"fetch_failed"`.

- [ ] **Step 1: Write the failing test**

Append to `broadcast-worker/nats_metrics_test.go`:

```go
func TestBroadcastMetrics_ThreadFanOutDegraded_NilSafe(t *testing.T) {
	var m *broadcastMetrics
	assert.NotPanics(t, func() {
		m.ThreadFanOutDegraded(context.Background(), "no_thread_room")
	}, "a nil metrics receiver must be inert, matching the other recorders")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=broadcast-worker`
Expected: FAIL — `m.ThreadFanOutDegraded undefined`

- [ ] **Step 3: Write minimal implementation**

Follow the shape of `ThreadViewPublishFailed` (`nats_metrics.go:153-160`). Add the counter to the `broadcastMetrics` struct, initialise it in the constructor alongside the other instruments, and add:

```go
// ThreadFanOutDegraded counts thread fan-outs that shipped with a reduced
// audience because the parent could not be resolved. reason is "no_thread_room"
// (the thread has no thread_rooms document yet) or "fetch_failed" (history-service
// could not answer).
func (m *broadcastMetrics) ThreadFanOutDegraded(ctx context.Context, reason string) {
	if m == nil || m.threadFanOutDegraded == nil {
		return
	}
	m.threadFanOutDegraded.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=broadcast-worker`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add broadcast-worker/nats_metrics.go broadcast-worker/nats_metrics_test.go
git commit -m "feat(broadcast-worker): count degraded thread fan-outs by reason"
```

---

## Task 4: Fail-closed thread fan-out

The core fix. Resolution order becomes **event fields → `thread_rooms` → `FetchParent`**, and an unresolved parent degrades instead of erroring.

**Files:**
- Modify: `broadcast-worker/handler.go:1329-1372` (`allowedThreadMentions`, `channelThreadFanOut`)
- Modify: `broadcast-worker/handler_test.go`

**Interfaces:**
- Consumes: `Store.GetThreadRoom` and `ThreadRoomInfo` (Task 2); `(*broadcastMetrics).ThreadFanOutDegraded` (Task 3).
- Produces: no signature change — `channelThreadFanOut(ctx, roomID, siteID, parentMsgID, sender string, mentions []string, eventParentCreatedAt *time.Time, eventParentSenderAccount string) ([]string, error)` keeps its shape.

- [ ] **Step 1: Write the failing tests**

Append to `broadcast-worker/handler_test.go`:

```go
// The thread room supplies the parent createdAt when the event lacks it, so no
// history-service round trip happens at all. This is the Cassandra-outage path
// for every thread that existed before the outage began.
func TestChannelThreadFanOut_ThreadRoomSuppliesParentCreatedAt(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	parentFetcher := NewMockParentFetcher(ctrl) // no EXPECT → FetchParent must NOT be called

	parentAt := time.Date(2026, 4, 1, 9, 0, 0, 0, time.UTC)
	joinedLate := parentAt.Add(time.Hour)

	store.EXPECT().GetThreadRoom(gomock.Any(), "parent-1").Return(ThreadRoomInfo{
		Followers:       map[string]struct{}{"carol": {}},
		ParentCreatedAt: &parentAt,
	}, nil)
	store.EXPECT().GetHistorySharedSince(gomock.Any(), "r1", []string{"bob", "dave"}).
		Return(map[string]*time.Time{"bob": nil, "dave": &joinedLate}, nil)

	h := NewHandler(store, us, &mockPublisher{}, NewMockRoomKeyProvider(ctrl), parentFetcher, false, subject.RouteGlobal)

	got, err := h.channelThreadFanOut(context.Background(), "r1", "site-a", "parent-1", "alice",
		[]string{"bob", "dave"}, nil, "")

	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"alice", "carol", "bob"}, got,
		"sender + follower + unrestricted mentionee; dave joined after the parent")
}

// No thread room and no usable fetch: deliver to whoever we can verify rather
// than returning an error, which JetStream would turn into a dropped message.
func TestChannelThreadFanOut_UnresolvableParent_FailsClosed(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	parentFetcher := NewMockParentFetcher(ctrl)

	store.EXPECT().GetThreadRoom(gomock.Any(), "parent-1").Return(ThreadRoomInfo{
		Followers: map[string]struct{}{},
	}, nil)
	parentFetcher.EXPECT().
		FetchParent(gomock.Any(), "alice", "r1", "site-a", "parent-1").
		Return(nil, errcode.Internal("cassandra unavailable"))

	h := NewHandler(store, us, &mockPublisher{}, NewMockRoomKeyProvider(ctrl), parentFetcher, false, subject.RouteGlobal)

	got, err := h.channelThreadFanOut(context.Background(), "r1", "site-a", "parent-1", "alice",
		[]string{"bob"}, nil, "")

	require.NoError(t, err, "an unresolvable parent must not error — that drops the message")
	assert.Equal(t, []string{"alice"}, got, "mentionee bob is dropped: his history window cannot be checked")
}

// Followers still receive the reply when the parent is unresolvable — only the
// history-gated mentionees are dropped.
func TestChannelThreadFanOut_UnresolvableParent_KeepsFollowers(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	parentFetcher := NewMockParentFetcher(ctrl)

	store.EXPECT().GetThreadRoom(gomock.Any(), "parent-1").Return(ThreadRoomInfo{
		Followers: map[string]struct{}{"carol": {}, "zoe": {}},
	}, nil)
	parentFetcher.EXPECT().
		FetchParent(gomock.Any(), "alice", "r1", "site-a", "parent-1").
		Return(nil, errcode.Internal("cassandra unavailable"))

	h := NewHandler(store, us, &mockPublisher{}, NewMockRoomKeyProvider(ctrl), parentFetcher, false, subject.RouteGlobal)

	got, err := h.channelThreadFanOut(context.Background(), "r1", "site-a", "parent-1", "alice",
		nil, nil, "")

	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"alice", "carol", "zoe"}, got,
		"the parent author is among replyAccounts, so followers already cover them")
}

// A Mongo failure is still an error: thread_rooms is the fallback's own store,
// and losing it means we know nothing, not that we should degrade silently.
func TestChannelThreadFanOut_ThreadRoomStoreError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)

	store.EXPECT().GetThreadRoom(gomock.Any(), "parent-1").Return(ThreadRoomInfo{}, errors.New("db error"))

	h := NewHandler(store, us, &mockPublisher{}, NewMockRoomKeyProvider(ctrl), NewMockParentFetcher(ctrl), false, subject.RouteGlobal)

	_, err := h.channelThreadFanOut(context.Background(), "r1", "site-a", "parent-1", "alice", nil, nil, "")

	require.Error(t, err)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=broadcast-worker -run TestChannelThreadFanOut`
Expected: FAIL — the mock expects `GetThreadRoom` before `GetHistorySharedSince`, and the unresolvable cases currently return an error.

- [ ] **Step 3: Write the implementation**

Replace `channelThreadFanOut` (`handler.go:1350-1372`) with:

```go
// channelThreadFanOut builds the deduplicated channel recipient set: the reply sender
// + the parent's thread followers + history-gated @-mentions, bots excluded.
//
// The parent's createdAt is resolved in three steps, cheapest first: the event (the
// gatekeeper resolved it on the send path), then the thread_rooms document this method
// already reads for followers, then history-service. Only the last touches Cassandra,
// so a thread that existed before an outage resolves entirely from Mongo.
//
// When no step resolves it, the fan-out degrades rather than erroring: followers still
// receive the reply and the history-gated mentionees are dropped. Returning an error
// here would NAK the message, and MaxDeliver would then destroy it — a reply nobody
// receives is strictly worse than one some people receive.
//
// The parent author needs no separate resolution: message-worker seeds them into
// replyAccounts at first reply, so followers already contains them.
func (h *Handler) channelThreadFanOut(ctx context.Context, roomID, siteID, parentMsgID, sender string, mentions []string, eventParentCreatedAt *time.Time, eventParentSenderAccount string) ([]string, error) {
	room, err := h.store.GetThreadRoom(ctx, parentMsgID)
	if err != nil {
		return nil, fmt.Errorf("get thread room for parent %s: %w", parentMsgID, err)
	}

	parentCreatedAt := eventParentCreatedAt
	parentSender := eventParentSenderAccount
	if parentCreatedAt == nil {
		parentCreatedAt = room.ParentCreatedAt
	}
	if parentCreatedAt == nil {
		fetched, ferr := h.parentFetcher.FetchParent(ctx, sender, roomID, siteID, parentMsgID)
		switch {
		case ferr != nil:
			h.metrics.ThreadFanOutDegraded(ctx, "fetch_failed")
			slog.WarnContext(ctx, "thread parent unresolvable; fanning out to followers only",
				"error", ferr, "parent_message_id", parentMsgID, "room_id", roomID,
				"request_id", natsutil.RequestIDFromContext(ctx))
		default:
			parentCreatedAt = &fetched.CreatedAt
			if parentSender == "" {
				parentSender = fetched.SenderAccount
			}
		}
	}
	if parentCreatedAt == nil && len(room.Followers) == 0 {
		h.metrics.ThreadFanOutDegraded(ctx, "no_thread_room")
	}

	// A nil parentCreatedAt makes mentionVisible fail closed for every member with a
	// history window, so the gate never widens on missing data. Skip the query entirely
	// when nothing can pass it.
	var allowed []string
	if parentCreatedAt != nil {
		allowed, err = h.allowedThreadMentions(ctx, roomID, mentions, parentCreatedAt)
		if err != nil {
			return nil, err
		}
	}
	return threadFanOutAccounts(sender, parentSender, room.Followers, allowed), nil
}
```

Delete the now-unused `GetThreadFollowers` call further down the old body — `room.Followers` replaces it.

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=broadcast-worker`
Expected: PASS, including the pre-existing thread tests. `TestHandleThreadCreated_ChannelRoom_MissingSenderAccount_FallsBackToFetch` still passes: the event carries `createdAt` but the thread room in that test has none, so with `eventParentCreatedAt` set the fetch is now skipped — **update that test** to assert the new behaviour (no fetch, parent author supplied by followers) rather than reinstating the round trip.

- [ ] **Step 5: Lint**

Run: `make lint`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add broadcast-worker/handler.go broadcast-worker/handler_test.go
git commit -m "fix(broadcast-worker): resolve the thread parent from Mongo and fail closed instead of dropping"
```

---

## Task 5: Integration test for the Mongo fallback

There is no test today for `FetchParent` returning an error — the exact production path.

**Files:**
- Modify: `broadcast-worker/integration_test.go`

**Interfaces:**
- Consumes: `Store.GetThreadRoom` (Task 2), `channelThreadFanOut` (Task 4).
- Produces: nothing.

- [ ] **Step 1: Write the failing test**

Append to `broadcast-worker/integration_test.go` (the file already carries `//go:build integration` and a `TestMain` calling `testutil.RunTests`):

```go
// A real thread_rooms document rescues the fan-out when history-service is
// unreachable — the Cassandra-outage path end to end through Mongo.
func TestIntegration_ChannelThreadFanOut_ThreadRoomFallback(t *testing.T) {
	db := testutil.MongoDB(t, "bw-threadfallback")
	ctx := context.Background()

	parentAt := time.Date(2026, 4, 1, 9, 0, 0, 0, time.UTC)
	_, err := db.Collection("thread_rooms").InsertOne(ctx, bson.M{
		"_id":                   idgen.GenerateUUIDv7(),
		"parentMessageId":       "parent-1",
		"threadParentCreatedAt": parentAt,
		"roomId":                "r1",
		"replyAccounts":         []string{"carol", "zoe"},
	})
	require.NoError(t, err)

	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"),
		db.Collection("thread_rooms"), nil, time.Minute)

	info, err := store.GetThreadRoom(ctx, "parent-1")
	require.NoError(t, err)
	require.NotNil(t, info.ParentCreatedAt, "the stored parent timestamp must survive the projection")
	assert.Equal(t, parentAt.UTC(), info.ParentCreatedAt.UTC())
	assert.ElementsMatch(t, []string{"carol", "zoe"}, keysOf(info.Followers))
}

// A parent with no thread yet returns empty, not an error — the caller degrades.
func TestIntegration_GetThreadRoom_MissingDocument(t *testing.T) {
	db := testutil.MongoDB(t, "bw-threadmissing")
	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"),
		db.Collection("thread_rooms"), nil, time.Minute)

	info, err := store.GetThreadRoom(context.Background(), "no-such-parent")

	require.NoError(t, err)
	assert.Empty(t, info.Followers)
	assert.Nil(t, info.ParentCreatedAt)
}
```

Add the helper at the bottom of the same file if it does not already exist:

```go
func keysOf(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test-integration SERVICE=broadcast-worker`
Expected: FAIL before Task 2 is applied; after Task 2 it should PASS — if it does, confirm the test is real by temporarily removing `threadParentCreatedAt` from the projection and watching it fail.

- [ ] **Step 3: Run the integration suite**

Run: `make test-integration SERVICE=broadcast-worker`
Expected: PASS. Requires Docker.

- [ ] **Step 4: Commit**

```bash
git add broadcast-worker/integration_test.go
git commit -m "test(broadcast-worker): cover the thread_rooms parent fallback against real Mongo"
```

---

## Task 6: notification-worker uses the same fallback

**Files:**
- Modify: `notification-worker/threads.go:13-56`
- Modify: `notification-worker/handler.go:145-165`
- Modify: `notification-worker/handler_test.go:52-62` (`stubFollowers`)
- Modify: `notification-worker/integration_test.go:150-185`

**Interfaces:**
- Consumes: `errcode.IsTransient` (Task 1).
- Produces: `ThreadRoomInfo` gains `ParentCreatedAt *time.Time`; `ThreadFollowerLister.Lookup` keeps its signature.

- [ ] **Step 1: Write the failing test**

Append to `notification-worker/handler_test.go`:

```go
// The thread room supplies the parent createdAt, so no history-service fetch runs
// and restricted members are still gated correctly.
func TestHandleMessage_ThreadReply_ThreadRoomSuppliesParentCreatedAt(t *testing.T) {
	parentAt := time.Date(2026, 4, 1, 9, 0, 0, 0, time.UTC)
	followers := &stubFollowers{
		out:       map[string]map[string]struct{}{"parent-1": {"carol": {}}},
		parentAt:  map[string]*time.Time{"parent-1": &parentAt},
	}
	// stubParent errs: if the handler consults it, the test fails.
	parent := stubParent{err: errcode.Internal("cassandra unavailable")}

	h := newTestHandlerWithParent(followers, parent)

	err := h.HandleMessage(context.Background(), threadReplyEventJSON(t, "parent-1", "alice"))

	require.NoError(t, err, "the thread room resolved the parent; the fetch must not be needed")
}

// No thread room and an unreachable history-service: followers still get notified,
// mention-only recipients are dropped, and the message is not NAK'd.
func TestHandleMessage_ThreadReply_UnresolvableParent_DoesNotError(t *testing.T) {
	followers := &stubFollowers{out: map[string]map[string]struct{}{}}
	parent := stubParent{err: errcode.Internal("cassandra unavailable")}

	h := newTestHandlerWithParent(followers, parent)

	err := h.HandleMessage(context.Background(), threadReplyEventJSON(t, "parent-1", "alice"))

	require.NoError(t, err, "an unresolvable parent must not NAK — MaxDeliver would destroy the notification")
}
```

Extend `stubFollowers` (line 52) to carry the timestamp:

```go
type stubFollowers struct {
	out      map[string]map[string]struct{}
	parentAt map[string]*time.Time
}

func (s *stubFollowers) Lookup(_ context.Context, parentID string) (ThreadRoomInfo, error) {
	info := ThreadRoomInfo{Followers: map[string]struct{}{}}
	if v, ok := s.out[parentID]; ok {
		info.Followers = v
	}
	if at, ok := s.parentAt[parentID]; ok {
		info.ParentCreatedAt = at
	}
	return info, nil
}
```

Add the two helpers near `newTestHandler` (line 177):

```go
// newTestHandlerWithParent builds a handler whose thread-parent resolution is
// under test; every other collaborator is the existing inert stub.
func newTestHandlerWithParent(followers ThreadFollowerLister, parent ParentFetcher) *Handler {
	return NewHandler(HandlerDeps{
		Members: &stubMembers{out: map[string][]roomsubcache.Member{
			"r1": {
				{ID: "u-alice", Account: "alice", RoomType: model.RoomTypeChannel},
				{ID: "u-carol", Account: "carol", RoomType: model.RoomTypeChannel},
			},
		}},
		Followers:          followers,
		Parent:             parent,
		Presence:           stubPresence{},
		Hook:               stubHook{},
		Emitter:            &stubEmitter{},
		LargeRoomThreshold: 100,
	})
}

// threadReplyEventJSON marshals a tshow=false thread reply — the shape that
// routes through the thread-only notification path.
func threadReplyEventJSON(t *testing.T, parentID, sender string) []byte {
	t.Helper()
	evt := model.MessageEvent{
		Event:     model.EventCreated,
		SiteID:    "site-a",
		Timestamp: time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC).UnixMilli(),
		Message: model.Message{
			ID:                    "reply-1",
			RoomID:                "r1",
			UserID:                "u-alice",
			UserAccount:           sender,
			Content:               "a thread reply",
			CreatedAt:             time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC),
			ThreadParentMessageID: parentID,
			TShow:                 false,
		},
	}
	data, err := json.Marshal(evt)
	require.NoError(t, err)
	return data
}
```

Match the stub type names to those already in the file (`stubMembers`, `stubPresence`, `stubHook`, `stubEmitter`) — if any differ, use the existing names rather than renaming them.

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=notification-worker`
Expected: FAIL — `ThreadRoomInfo` has no `ParentCreatedAt`, and the unresolvable case currently returns an error.

- [ ] **Step 3: Write the implementation**

In `notification-worker/threads.go`, replace the struct and the projection:

```go
// ThreadRoomInfo is the per-thread metadata read from thread_rooms in one query.
// ParentCreatedAt is nil when the document is absent or its timestamp is zero —
// "unknown", never the epoch, so the suppression gate fails closed on missing data.
type ThreadRoomInfo struct {
	Followers       map[string]struct{}
	ParentCreatedAt *time.Time
}
```

```go
	var doc struct {
		ReplyAccounts         []string  `bson:"replyAccounts"`
		ThreadParentCreatedAt time.Time `bson:"threadParentCreatedAt"`
	}
	opts := options.FindOne().SetProjection(bson.M{"replyAccounts": 1, "threadParentCreatedAt": 1, "_id": 0})
```

and before returning:

```go
	info := ThreadRoomInfo{Followers: out}
	if !doc.ThreadParentCreatedAt.IsZero() {
		at := doc.ThreadParentCreatedAt.UTC()
		info.ParentCreatedAt = &at
	}
	return info, nil
```

In `notification-worker/handler.go`, replace the `else` branch at lines 156-165 with:

```go
		if msg.ThreadParentMessageCreatedAt != nil && evt.ThreadParentSenderAccount != "" {
			parentCreatedAt = msg.ThreadParentMessageCreatedAt
			parentSenderAccount = evt.ThreadParentSenderAccount
		} else if info.ParentCreatedAt != nil {
			// The thread room already answered — no history-service round trip, so a
			// Cassandra outage cannot stop notifications for an existing thread.
			parentCreatedAt = info.ParentCreatedAt
		} else {
			// The reply sender can always read the parent they replied to; fetch on their behalf.
			parent, perr := h.deps.Parent.FetchParent(ctx, msg.UserAccount, msg.RoomID, evt.SiteID, msg.ThreadParentMessageID)
			if perr != nil {
				// Degrade rather than NAK: followers below are already known, and returning
				// an error here burns MaxDeliver until the notification is destroyed. A nil
				// parentCreatedAt makes isRestricted fail closed, so nobody gains visibility.
				slog.WarnContext(ctx, "thread parent unresolvable; notifying followers only",
					"error", perr, "parent_message_id", msg.ThreadParentMessageID,
					"request_id", natsutil.RequestIDFromContext(ctx))
			} else {
				pc := parent.CreatedAt
				parentCreatedAt = &pc
				parentSenderAccount = parent.SenderAccount
			}
		}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=notification-worker`
Expected: PASS

- [ ] **Step 5: Update the integration test**

In `notification-worker/integration_test.go`, the existing `Lookup` assertions (lines 150-185) now also assert `ParentCreatedAt`. Add a document with `threadParentCreatedAt` set and assert it round-trips; assert `nil` for a document without it.

Run: `make test-integration SERVICE=notification-worker`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add notification-worker/threads.go notification-worker/handler.go notification-worker/handler_test.go notification-worker/integration_test.go
git commit -m "fix(notification-worker): resolve the thread parent from Mongo and degrade instead of NAKing"
```

---

## Task 7: Gatekeeper refuses an unresolvable thread start

**Files:**
- Modify: `pkg/errcode/codes_message.go`
- Modify: `message-gatekeeper/store.go:25-28` (interface)
- Modify: `message-gatekeeper/store_mongo.go:26-34` (add the collection + method)
- Modify: `message-gatekeeper/handler.go:406-425` (`processMessage`), `:489-515` (`resolveThreadParent`)
- Modify: `message-gatekeeper/handler_test.go`
- Modify: `docs/client-api.md`, `docs/client-api/request-reply.md`
- Regenerate: `message-gatekeeper/mock_store_test.go`

**Interfaces:**
- Consumes: `errcode.IsTransient` (Task 1).
- Produces:
  - `errcode.MessageThreadStartUnavailable Reason = "thread_start_unavailable"`
  - `Store.ThreadRoomExists(ctx context.Context, parentMessageID string) (bool, error)`
  - `resolveThreadParent` gains an error return: `(*time.Time, string, error)`.

- [ ] **Step 1: Write the failing test**

Append to `message-gatekeeper/handler_test.go`:

```go
// Starting a NEW thread while history is unreachable is refused: message-worker
// cannot create the thread_rooms document, so no consumer could ever resolve the
// parent and the reply would reach nobody.
func TestProcessMessage_ThreadStart_HistoryDown_Rejected(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	fetcher := NewMockParentMessageFetcher(ctrl)

	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(testSubscription, nil)
	fetcher.EXPECT().FetchQuotedParent(gomock.Any(), "alice", "r1", "site-a", "parent-1").
		Return(nil, errcode.Internal("cassandra unavailable"))
	store.EXPECT().ThreadRoomExists(gomock.Any(), "parent-1").Return(false, nil)

	h := newTestHandler(store, fetcher)

	_, err := h.processMessage(context.Background(), "alice", "r1", "site-a", &model.SendMessageRequest{
		ID:                    idgen.GenerateMessageID(),
		RequestID:             idgen.GenerateRequestID(),
		Content:               "first reply in a new thread",
		ThreadParentMessageID: "parent-1",
	})

	var ee *errcode.Error
	require.ErrorAs(t, err, &ee)
	assert.Equal(t, errcode.CodeUnavailable, ee.Code)
	assert.Equal(t, errcode.MessageThreadStartUnavailable, ee.Reason)
}

// A thread that already exists is unaffected: broadcast-worker resolves the parent
// from the same thread_rooms document, so the send goes through as normal.
func TestProcessMessage_ExistingThread_HistoryDown_Publishes(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	fetcher := NewMockParentMessageFetcher(ctrl)

	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(testSubscription, nil)
	fetcher.EXPECT().FetchQuotedParent(gomock.Any(), "alice", "r1", "site-a", "parent-1").
		Return(nil, errcode.Internal("cassandra unavailable"))
	store.EXPECT().ThreadRoomExists(gomock.Any(), "parent-1").Return(true, nil)

	h := newTestHandler(store, fetcher)

	out, err := h.processMessage(context.Background(), "alice", "r1", "site-a", &model.SendMessageRequest{
		ID:                    idgen.GenerateMessageID(),
		RequestID:             idgen.GenerateRequestID(),
		Content:               "reply to an existing thread",
		ThreadParentMessageID: "parent-1",
	})

	require.NoError(t, err)
	assert.NotEmpty(t, out)
}

// A terminal fetch error keeps today's soft-fail: the thread_rooms probe is only
// for outage-class failures, so a missing parent does not become "unavailable".
func TestProcessMessage_ThreadStart_TerminalFetchError_SoftFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	fetcher := NewMockParentMessageFetcher(ctrl)

	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(testSubscription, nil)
	fetcher.EXPECT().FetchQuotedParent(gomock.Any(), "alice", "r1", "site-a", "parent-1").
		Return(nil, errcode.NotFound("message not found"))
	// no ThreadRoomExists EXPECT → the probe must not run for a terminal error

	h := newTestHandler(store, fetcher)

	out, err := h.processMessage(context.Background(), "alice", "r1", "site-a", &model.SendMessageRequest{
		ID:                    idgen.GenerateMessageID(),
		RequestID:             idgen.GenerateRequestID(),
		Content:               "reply",
		ThreadParentMessageID: "parent-1",
	})

	require.NoError(t, err)
	assert.NotEmpty(t, out)
}
```

If `newTestHandler` and `testSubscription` do not already exist in that file, add them mirroring the construction used by the surrounding tests.

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=message-gatekeeper`
Expected: FAIL — `ThreadRoomExists` undefined, `MessageThreadStartUnavailable` undefined.

- [ ] **Step 3: Add the reason**

In `pkg/errcode/codes_message.go`, inside the existing `const` block:

```go
	// MessageThreadStartUnavailable: the sender is starting a NEW thread while
	// history is unreachable. The thread has no thread_rooms document, so no
	// consumer can resolve the parent and the reply would reach nobody. Replies to
	// existing threads are unaffected — the frontend uses this to disable thread
	// starts, not thread replies.
	MessageThreadStartUnavailable Reason = "thread_start_unavailable"
```

- [ ] **Step 4: Add the store method**

In `message-gatekeeper/store.go`, add to the `Store` interface:

```go
	// ThreadRoomExists reports whether a thread already exists for parentMessageID.
	// Read only when parent resolution has already failed, to tell "this thread is
	// resolvable from Mongo" from "nothing can resolve it".
	ThreadRoomExists(ctx context.Context, parentMessageID string) (bool, error)
```

In `message-gatekeeper/store_mongo.go`, add `threadRooms *mongo.Collection` to the struct, wire `db.Collection("thread_rooms")` in `NewMongoStore`, and add:

```go
func (s *MongoStore) ThreadRoomExists(ctx context.Context, parentMessageID string) (bool, error) {
	opts := options.FindOne().SetProjection(bson.M{"_id": 1})
	err := s.threadRooms.FindOne(ctx, bson.M{"parentMessageId": parentMessageID}, opts).Err()
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return false, nil
		}
		return false, fmt.Errorf("find thread room by parent %s: %w", parentMessageID, err)
	}
	return true, nil
}
```

- [ ] **Step 5: Wire the refusal**

Change `resolveThreadParent`'s signature to `(*time.Time, string, error)` and replace its fetch-failure branch:

```go
	snap, err := h.parentFetcher.FetchQuotedParent(ctx, account, roomID, siteID, req.ThreadParentMessageID)
	if err != nil || snap == nil {
		slog.WarnContext(ctx, "thread parent resolution failed",
			"error", err,
			"parent_message_id", req.ThreadParentMessageID,
			"request_id", req.RequestID)
		// Only an outage-class failure warrants the thread_rooms probe. A terminal
		// error (not_found, forbidden) keeps the historical soft-fail.
		if err != nil && !errcode.IsTransient(err) {
			return nil, "", nil
		}
		exists, xerr := h.store.ThreadRoomExists(ctx, req.ThreadParentMessageID)
		if xerr != nil {
			// Mongo is the last thing that could have answered. Soft-fail as before
			// rather than refusing a send on a second, unrelated failure.
			slog.WarnContext(ctx, "thread room probe failed; publishing without parent",
				"error", xerr, "parent_message_id", req.ThreadParentMessageID,
				"request_id", req.RequestID)
			return nil, "", nil
		}
		if !exists {
			return nil, "", errcode.Unavailable(
				"cannot start a new thread while message history is unavailable",
				errcode.WithReason(errcode.MessageThreadStartUnavailable))
		}
		// The thread exists: broadcast-worker resolves the parent from the same
		// document, so the send proceeds without the event-carried fields.
		return nil, "", nil
	}
	t := snap.CreatedAt.UTC()
	return &t, snap.Sender.Account, nil
```

Update the call site at `handler.go:419`:

```go
	threadParentCreatedAt, threadParentSenderAccount, err := h.resolveThreadParent(ctx, account, roomID, siteID, req, quotedSnapshot, quotedUnverified)
	if err != nil {
		return nil, err
	}
```

- [ ] **Step 6: Regenerate mocks and run**

Run: `make generate SERVICE=message-gatekeeper && make test SERVICE=message-gatekeeper`
Expected: PASS

- [ ] **Step 7: Document the error**

In `docs/client-api.md`, add a row to the `msg.send` error table (around line 6373), keeping the existing column order:

```markdown
| `cannot start a new thread while message history is unavailable` | `unavailable` | `thread_start_unavailable` | The sender is starting a *new* thread (the parent has no replies yet) while message history is unreachable. Replies to threads that already have replies are unaffected. Clients should disable thread-start on messages with `tcount: 0` until history recovers. |
```

Mirror the row into `docs/client-api/request-reply.md` wherever the `msg.send` error table is reproduced. `docs/client-api/events.md` needs no change — no event schema moved.

- [ ] **Step 8: Lint and commit**

```bash
make lint
git add pkg/errcode/codes_message.go message-gatekeeper/ docs/client-api.md docs/client-api/request-reply.md
git commit -m "feat(message-gatekeeper): refuse a thread start that no consumer could deliver"
```

---

## Task 8: chat-frontend observes the send reply

`sendMessage` publishes and never listens, so **every** `msg.send` rejection is invisible today.

**Files:**
- Modify: `chat-frontend/src/api/sendMessage/index.ts`
- Create: `chat-frontend/src/api/sendMessage/index.test.ts`

**Interfaces:**
- Consumes: `Nats.subscribe(subject, cb) => NatsSubscription`, `Nats.publish`, `userResponse(account, requestId)` from `../_transport/subjects`, `AsyncJobError` from `../_transport/asyncJob`.
- Produces: `sendMessage(nats, args) => Promise<void>` — **was `void`, is now a Promise**. It rejects with `AsyncJobError` carrying `.code` and `.reason`. Task 11 and Task 12 consume `.reason`.

- [ ] **Step 1: Write the failing test**

Create `chat-frontend/src/api/sendMessage/index.test.ts`:

```ts
import { describe, it, expect, vi } from 'vitest'
import { sendMessage } from './index'

function natsDouble(reply?: unknown, opts: { delayMs?: number } = {}) {
  const handlers: Record<string, (data: unknown) => void> = {}
  return {
    user: { account: 'alice' },
    subscribe: vi.fn((subject: string, cb: (data: unknown) => void) => {
      handlers[subject] = cb
      return { unsubscribe: vi.fn() }
    }),
    publish: vi.fn((subject: string) => {
      if (reply === undefined) return
      const respSubject = Object.keys(handlers)[0]
      setTimeout(() => handlers[respSubject]?.(reply), opts.delayMs ?? 0)
    }),
    handlers,
  }
}

const args = {
  roomId: 'r1',
  siteId: 'site-a',
  payload: { id: 'm1', content: 'hi', requestId: '01970a4f-8c2d-7c9a-abcd-e0123456789f' },
}

describe('sendMessage', () => {
  it('resolves when the gatekeeper replies with the stored message', async () => {
    const nats = natsDouble({ id: 'm1', roomId: 'r1' })
    await expect(sendMessage(nats as never, args)).resolves.toBeUndefined()
  })

  it('subscribes to the response subject before publishing', async () => {
    const nats = natsDouble({ id: 'm1' })
    await sendMessage(nats as never, args)
    const subscribeOrder = nats.subscribe.mock.invocationCallOrder[0]
    const publishOrder = nats.publish.mock.invocationCallOrder[0]
    expect(subscribeOrder).toBeLessThan(publishOrder)
  })

  it('rejects with the typed reason when the gatekeeper refuses a thread start', async () => {
    const nats = natsDouble({
      error: 'cannot start a new thread while message history is unavailable',
      code: 'unavailable',
      reason: 'thread_start_unavailable',
    })
    await expect(sendMessage(nats as never, args)).rejects.toMatchObject({
      reason: 'thread_start_unavailable',
      code: 'unavailable',
    })
  })

  it('rejects on timeout when no reply arrives', async () => {
    vi.useFakeTimers()
    const nats = natsDouble()
    const p = sendMessage(nats as never, { ...args, timeoutMs: 1000 })
    const assertion = expect(p).rejects.toMatchObject({ kind: 'timeout' })
    await vi.advanceTimersByTimeAsync(1001)
    await assertion
    vi.useRealTimers()
  })

  it('unsubscribes once settled', async () => {
    const nats = natsDouble({ id: 'm1' })
    await sendMessage(nats as never, args)
    const sub = nats.subscribe.mock.results[0].value
    expect(sub.unsubscribe).toHaveBeenCalled()
  })
})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd chat-frontend && npm test -- sendMessage`
Expected: FAIL — `sendMessage` returns `void`, so `.resolves` throws.

- [ ] **Step 3: Write the implementation**

Rewrite `chat-frontend/src/api/sendMessage/index.ts`:

```ts
import { msgSend, userResponse } from '../_transport/subjects'
import { AsyncJobError } from '../_transport/asyncJob'
import type { Nats } from '../types'

/** Matches asyncJob's sync-reply window; the gatekeeper answers off a JetStream
 *  consumer, so this covers consumer lag as well as the request itself. */
const DEFAULT_SEND_TIMEOUT = 10_000

export interface SendMessagePayload {
  id: string
  content: string
  requestId: string
  quotedParentMessageId?: string
  threadParentMessageId?: string
  threadParentMessageCreatedAt?: number
  /** Base64-encoded Attachment JSON blobs (see lib/attachment.encodeAttachment).
   *  Max 1 today; content may be empty when attachments are present. */
  attachments?: string[]
}

export interface SendMessageArgs {
  roomId: string
  siteId: string
  payload: SendMessagePayload
  timeoutMs?: number
}

interface ErrorEnvelope {
  error?: string
  code?: string
  reason?: string
  metadata?: Record<string, string>
}

/**
 * Submit a new message into a room and settle on the gatekeeper's reply.
 *
 * message-gatekeeper acks every validation and authorization failure on
 * `chat.user.{account}.response.{requestId}` (docs/client-api.md §msg.send).
 * This used to be fire-and-forget, so those replies were discarded and a
 * refused send looked identical to a successful one.
 *
 * @throws {AsyncJobError} `.kind` is 'remote' for a typed refusal (with `.code`
 *   and `.reason` from the envelope) or 'timeout' when no reply arrives.
 */
export function sendMessage(
  { user, publish, subscribe }: Nats,
  { roomId, siteId, payload, timeoutMs = DEFAULT_SEND_TIMEOUT }: SendMessageArgs,
): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    // Subscribe before publishing so a fast gatekeeper cannot beat us to the reply.
    const sub = subscribe(userResponse(user.account, payload.requestId), (data: unknown) => {
      settle(() => {
        const env = (data ?? {}) as ErrorEnvelope
        if (env.error) {
          reject(new AsyncJobError(env.error, 'remote', { code: env.code, reason: env.reason, metadata: env.metadata }))
          return
        }
        resolve()
      })
    })

    const timer = setTimeout(() => {
      settle(() => reject(new AsyncJobError('send timed out', 'timeout')))
    }, timeoutMs)

    let done = false
    function settle(fn: () => void) {
      if (done) return
      done = true
      clearTimeout(timer)
      sub.unsubscribe()
      fn()
    }

    publish(msgSend(user.account, roomId, siteId), payload)
  })
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd chat-frontend && npm test -- sendMessage && npm run typecheck`
Expected: PASS on both. `AsyncJobError`'s `kind` values come from `ASYNC_JOB_ERROR_KINDS` — if `'remote'` or `'timeout'` is not among them, use the nearest existing kinds rather than inventing new ones, and update the test to match.

- [ ] **Step 5: Update callers**

Run: `cd chat-frontend && grep -rn "sendMessage(" src --include=*.jsx --include=*.js --include=*.ts | grep -v test`

Each caller now gets a Promise. Callers that fire and forget must at minimum attach a `.catch` so a refusal is not an unhandled rejection; the composer should surface the error text via the existing `formatAsyncJobError` helper re-exported from `api/index.ts`. Run the full suite afterwards:

Run: `cd chat-frontend && npm test && npm run typecheck`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add chat-frontend/src/api/sendMessage/ chat-frontend/src
git commit -m "fix(chat-frontend): settle msg.send on the gatekeeper reply instead of discarding it"
```

---

## Task 9: Live-stack outage smoke test

**Files:**
- Create: `chat-frontend/scripts/threadOutage.smoke.mjs`
- Modify: `chat-frontend/package.json` (add `smoke:threadoutage`)

**Interfaces:**
- Consumes: `sendMessage` (Task 8); the gatekeeper refusal (Task 7).
- Produces: nothing.

- [ ] **Step 1: Write the script**

Create `chat-frontend/scripts/threadOutage.smoke.mjs`, following the structure of `liveStack.smoke.mjs` (dev login against `AUTH_URL`, `nats.ws` connect with `jwtAuthenticator`, a `check(label, ok, detail)` tally, `process.exit(fail ? 1 : 0)`).

Reuse `liveStack.smoke.mjs`'s `devLogin`, `check` and connection setup verbatim. The body:

```js
// Phase 1 — seed a thread while the stack is healthy, so message-worker
// creates its thread_rooms document. This is the thread that must survive.
const parentId = await sendTopLevel(alice, roomId, 'thread parent')
await waitForRoomEvent(bob, parentId, 5000)
await sendThreadReply(alice, roomId, parentId, 'first reply (healthy)')
const seeded = await waitForThreadEvent(bob, parentId, 5000)
check('healthy thread reply delivered', !!seeded)

// Phase 2 — the outage.
await confirmCassandraStopped()  // prompts unless --assume-stopped

// Assert 1: a thread that already exists still delivers. This is the fix —
// broadcast-worker resolves the parent from thread_rooms, never Cassandra.
await sendThreadReply(alice, roomId, parentId, 'reply during outage')
const during = await waitForThreadEvent(bob, parentId, 5000)
check('existing thread still delivers during the outage', !!during,
  during ? '' : 'bob received no thread event')

// Assert 2: starting a NEW thread is refused, not silently swallowed.
const freshId = await sendTopLevel(alice, roomId, 'a message with no replies')
let refusal = null
try {
  await sendThreadReply(alice, roomId, freshId, 'first reply during outage')
} catch (err) {
  refusal = err
}
check('new thread start refused with a typed reason',
  refusal?.reason === 'thread_start_unavailable',
  refusal ? `got reason=${refusal.reason}` : 'the send was accepted — it should have been refused')

// Phase 3 — recovery restores thread starts.
await confirmCassandraStarted()
await sendThreadReply(alice, roomId, freshId, 'first reply after recovery')
const recovered = await waitForThreadEvent(bob, freshId, 10000)
check('new thread start works again after recovery', !!recovered)

process.exit(fail ? 1 : 0)
```

`sendTopLevel` and `sendThreadReply` wrap the real `sendMessage` from
`../src/api/sendMessage/index.ts` (Task 8), so assert 2 exercises the actual
rejection path rather than a hand-rolled request. `waitForThreadEvent` subscribes
to `bob`'s user-room-event subject and resolves on the first
`new_thread_message` naming `parentId`, rejecting on timeout.
`confirmCassandraStopped` prints
`>>> Stop Cassandra now: docker compose -f docker-local/docker-compose.yml stop cassandra`
and waits for a newline on stdin, returning immediately when `--assume-stopped`
is passed; `confirmCassandraStarted` does the same for `start`.

- [ ] **Step 2: Add the npm script**

In `chat-frontend/package.json`, alongside the other smoke entries:

```json
    "smoke:threadoutage": "node --experimental-strip-types scripts/threadOutage.smoke.mjs",
```

- [ ] **Step 3: Run it against a live stack**

Run: `cd chat-frontend && npm run smoke:threadoutage`
Expected: all checks PASS. Requires the local stack up per `docker-local/`, with `alice`/`bob` seeded.

- [ ] **Step 4: Commit**

```bash
git add chat-frontend/scripts/threadOutage.smoke.mjs chat-frontend/package.json
git commit -m "test(chat-frontend): live-stack smoke for thread delivery through a Cassandra outage"
```

---

## PR 1 wrap-up

- [ ] Run `make lint`, `make test`, `make sast` — all clean.
- [ ] Run `cd chat-frontend && npm test && npm run typecheck` — clean.
- [ ] `rm -rf docs/reviews/` if anything is there.
- [ ] Push: `git push -u origin claude/thread-messages-cassandra-down-fje1p9`
- [ ] **Do not open a PR until asked.** When asked, the description must call out two behaviour changes: thread starts are now *refused* during a history outage, and `msg.send` now settles on its reply for **all** messages, not just thread replies.
- [ ] Spec follow-up 1 is **not** part of this plan: correcting the premise in
  [PR #307](https://github.com/hmchangw/newchat/pull/307)'s design note is a comment on that PR, not a
  code change here. Raise it separately so it is not lost.

---

# PR 2 — client prevention

## Task 10: Degraded context

**Files:**
- Create: `chat-frontend/src/context/DegradedContext/DegradedContext.jsx`
- Create: `chat-frontend/src/context/DegradedContext/index.jsx`
- Create: `chat-frontend/src/context/DegradedContext/DegradedContext.test.jsx`

**Interfaces:**
- Consumes: `AsyncJobError.reason` from Task 8.
- Produces: `useDegraded() => { historyDegraded: boolean, noteHistoryFailure(err): void, noteHistorySuccess(): void }`. Consumed by Tasks 11 and 12.

- [ ] **Step 1: Write the failing test**

Create `chat-frontend/src/context/DegradedContext/DegradedContext.test.jsx`:

```jsx
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { renderHook, act } from '@testing-library/react'
import { DegradedProvider, useDegraded } from './DegradedContext'

const wrapper = ({ children }) => <DegradedProvider>{children}</DegradedProvider>

describe('DegradedContext', () => {
  beforeEach(() => vi.useFakeTimers())
  afterEach(() => vi.useRealTimers())

  it('starts healthy', () => {
    const { result } = renderHook(() => useDegraded(), { wrapper })
    expect(result.current.historyDegraded).toBe(false)
  })

  it('goes degraded on an unavailable history failure', () => {
    const { result } = renderHook(() => useDegraded(), { wrapper })
    act(() => result.current.noteHistoryFailure({ code: 'unavailable' }))
    expect(result.current.historyDegraded).toBe(true)
  })

  it('goes degraded on an internal history failure', () => {
    const { result } = renderHook(() => useDegraded(), { wrapper })
    act(() => result.current.noteHistoryFailure({ code: 'internal' }))
    expect(result.current.historyDegraded).toBe(true)
  })

  it('goes degraded on a thread_start_unavailable refusal', () => {
    const { result } = renderHook(() => useDegraded(), { wrapper })
    act(() => result.current.noteHistoryFailure({ reason: 'thread_start_unavailable' }))
    expect(result.current.historyDegraded).toBe(true)
  })

  it('ignores a terminal error — not_found is not an outage', () => {
    const { result } = renderHook(() => useDegraded(), { wrapper })
    act(() => result.current.noteHistoryFailure({ code: 'not_found' }))
    expect(result.current.historyDegraded).toBe(false)
  })

  it('clears on a successful history load', () => {
    const { result } = renderHook(() => useDegraded(), { wrapper })
    act(() => result.current.noteHistoryFailure({ code: 'unavailable' }))
    act(() => result.current.noteHistorySuccess())
    expect(result.current.historyDegraded).toBe(false)
  })

  it('self-clears after the TTL so a stuck flag cannot outlive the outage', () => {
    const { result } = renderHook(() => useDegraded(), { wrapper })
    act(() => result.current.noteHistoryFailure({ code: 'unavailable' }))
    act(() => vi.advanceTimersByTime(60_000))
    expect(result.current.historyDegraded).toBe(false)
  })
})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd chat-frontend && npm test -- DegradedContext`
Expected: FAIL — module not found.

- [ ] **Step 3: Write the implementation**

Create `chat-frontend/src/context/DegradedContext/DegradedContext.jsx`:

```jsx
import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react'

/** How long a degraded flag survives without corroboration. A stuck flag would
 *  keep threads disabled long after recovery, and the next history load or send
 *  re-arms it if the outage is still on. */
const DEGRADED_TTL_MS = 60_000

/** Outage-class signals. These mirror the categories errcode.IsTransient treats
 *  as retryable infrastructure, so the client and the server agree on what an
 *  outage is. A terminal error (not_found, forbidden) is a settled answer, not
 *  an outage, and must not disable anything. */
function isOutageSignal(err) {
  return err?.code === 'unavailable'
    || err?.code === 'internal'
    || err?.reason === 'thread_start_unavailable'
}

const DegradedContext = createContext(null)

export function DegradedProvider({ children }) {
  const [historyDegraded, setHistoryDegraded] = useState(false)
  const timer = useRef(null)

  const clearTimer = useCallback(() => {
    if (timer.current) {
      clearTimeout(timer.current)
      timer.current = null
    }
  }, [])

  const noteHistoryFailure = useCallback((err) => {
    if (!isOutageSignal(err)) return
    setHistoryDegraded(true)
    clearTimer()
    timer.current = setTimeout(() => {
      timer.current = null
      setHistoryDegraded(false)
    }, DEGRADED_TTL_MS)
  }, [clearTimer])

  const noteHistorySuccess = useCallback(() => {
    clearTimer()
    setHistoryDegraded(false)
  }, [clearTimer])

  useEffect(() => clearTimer, [clearTimer])

  const value = useMemo(
    () => ({ historyDegraded, noteHistoryFailure, noteHistorySuccess }),
    [historyDegraded, noteHistoryFailure, noteHistorySuccess],
  )
  return <DegradedContext.Provider value={value}>{children}</DegradedContext.Provider>
}

export function useDegraded() {
  const ctx = useContext(DegradedContext)
  if (!ctx) throw new Error('useDegraded must be used within a DegradedProvider')
  return ctx
}
```

Create `chat-frontend/src/context/DegradedContext/index.jsx`, per the folder-per-context convention in `chat-frontend/CLAUDE.md`:

```jsx
export { DegradedProvider, useDegraded } from './DegradedContext'
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd chat-frontend && npm test -- DegradedContext`
Expected: PASS

- [ ] **Step 5: Mount the provider and wire the signals**

Mount `DegradedProvider` inside `MainApp` above `ChatPage`. Call `noteHistoryFailure` in the `catch` of the `fetchMessageHistory` call and `noteHistorySuccess` on success.

Run: `cd chat-frontend && npm test && npm run typecheck`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add chat-frontend/src/context/DegradedContext/ chat-frontend/src/components/MainApp
git commit -m "feat(chat-frontend): track history-degraded state from failed loads and refusals"
```

---

## Task 11: Disable thread start while degraded

**Files:**
- Modify: `chat-frontend/src/components/shared/MessageList/MessageRow/MessageActions/MessageActions.jsx`
- Modify: `chat-frontend/src/components/shared/MessageList/MessageRow/MessageActions/MessageActions.test.jsx`

**Interfaces:**
- Consumes: `useDegraded()` (Task 10).
- Produces: nothing.

- [ ] **Step 1: Write the failing test**

Append to `MessageActions.test.jsx`:

```jsx
it('disables "reply in thread" on a message with no replies while degraded', () => {
  renderWithDegraded(<MessageActions message={{ id: 'm1', tcount: 0 }} onThread={() => {}} />, { degraded: true })
  expect(screen.getByRole('button', { name: /thread/i })).toBeDisabled()
})

it('keeps "reply in thread" enabled on a message that already has replies', () => {
  renderWithDegraded(<MessageActions message={{ id: 'm1', tcount: 3 }} onThread={() => {}} />, { degraded: true })
  expect(screen.getByRole('button', { name: /thread/i })).toBeEnabled()
})

it('keeps "reply in thread" enabled when healthy', () => {
  renderWithDegraded(<MessageActions message={{ id: 'm1', tcount: 0 }} onThread={() => {}} />, { degraded: false })
  expect(screen.getByRole('button', { name: /thread/i })).toBeEnabled()
})
```

Add a `renderWithDegraded(ui, { degraded })` helper in the same file that wraps `ui` in `DegradedProvider` and primes the flag via `noteHistoryFailure({ code: 'unavailable' })` when `degraded` is true.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd chat-frontend && npm test -- MessageActions`
Expected: FAIL — the button is always enabled.

- [ ] **Step 3: Write the implementation**

In `MessageActions.jsx`, read `historyDegraded` from `useDegraded()` and compute:

```jsx
  // A message with no replies has no thread_rooms document, so a reply to it
  // cannot be delivered while history is down and the gatekeeper will refuse it.
  // tcount > 0 means the thread exists and resolves from Mongo — leave it alone.
  const threadStartBlocked = historyDegraded && !(message.tcount > 0)
```

Apply `disabled={threadStartBlocked}` and `title={threadStartBlocked ? 'Threads are temporarily unavailable' : undefined}` to the thread button.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd chat-frontend && npm test -- MessageActions`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add chat-frontend/src/components/shared/MessageList/MessageRow/MessageActions/
git commit -m "feat(chat-frontend): disable thread start on reply-less messages while history is degraded"
```

---

## Task 12: Handle a refusal arriving mid-compose

An outage can begin while the thread panel is already open, so prevention alone is not enough.

**Files:**
- Modify: `chat-frontend/src/components/MainApp/ThreadRightBar/ThreadMessageInput/ThreadMessageInput.jsx`
- Modify: `chat-frontend/src/components/MainApp/ThreadRightBar/ThreadMessageInput/ThreadMessageInput.test.jsx`

**Interfaces:**
- Consumes: `sendMessage` rejecting with `.reason` (Task 8); `useDegraded().noteHistoryFailure` (Task 10).
- Produces: nothing.

- [ ] **Step 1: Write the failing test**

Append to `ThreadMessageInput.test.jsx`:

```jsx
it('shows an explanation and keeps the draft when the send is refused', async () => {
  const err = Object.assign(new Error('refused'), { code: 'unavailable', reason: 'thread_start_unavailable' })
  const sendMessage = vi.fn().mockRejectedValue(err)
  renderThreadInput({ sendMessage })

  await userEvent.type(screen.getByRole('textbox'), 'my reply')
  await userEvent.click(screen.getByRole('button', { name: /send/i }))

  expect(await screen.findByText(/temporarily unavailable/i)).toBeInTheDocument()
  expect(screen.getByRole('textbox')).toHaveValue('my reply')
})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd chat-frontend && npm test -- ThreadMessageInput`
Expected: FAIL — no error is rendered and the draft is cleared.

- [ ] **Step 3: Write the implementation**

In `ThreadMessageInput.jsx`, add `const { noteHistoryFailure } = useDegraded()` and `const [sendError, setSendError] = useState(null)`, then wrap the send:

```jsx
  async function handleSend() {
    const draft = text.trim()
    if (!draft) return
    setSendError(null)
    try {
      await sendMessage(nats, {
        roomId,
        siteId,
        payload: { id: generateMessageId(), content: draft, requestId: uuidv7(), threadParentMessageId: parentMessageId },
      })
      setText('') // clear only after the gatekeeper accepted it
    } catch (err) {
      // Keep the draft: the reply was refused, not delivered, and retyping it is
      // the one thing the user should not have to do.
      setSendError(formatAsyncJobError(err))
      noteHistoryFailure(err)
    }
  }
```

and render the message above the composer:

```jsx
      {sendError && <div className="thread-input-error" role="alert">{sendError}</div>}
```

Match the existing prop and helper names in the file (`text`/`setText`, the id generator, the `nats` handle) rather than introducing new ones. `formatAsyncJobError` is re-exported from `api/index.ts` — never deep-import it from `_transport`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd chat-frontend && npm test -- ThreadMessageInput && npm run typecheck`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add chat-frontend/src/components/MainApp/ThreadRightBar/ThreadMessageInput/
git commit -m "feat(chat-frontend): surface a refused thread reply and keep the draft"
```

---

## PR 2 wrap-up

- [ ] Run `cd chat-frontend && npm test && npm run typecheck` — clean.
- [ ] Re-run `npm run smoke:threadoutage` and confirm the thread-start button is disabled in the browser during the outage window.
- [ ] `rm -rf docs/reviews/` if anything is there.
- [ ] Push. Do not open a PR until asked.
