# Thread Viewer Lane Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish channel thread replies, edits, and deletes to a per-thread NATS subject so a user who opens a thread they never participated in receives them live, without becoming a thread follower.

**Architecture:** `broadcast-worker`'s three channel thread handlers each gain one additional publish to a new `chat.{local.,}room.{roomID}.thread.{parentMessageId}.event` subject, carrying the encrypted equivalent of the payload already sent per-account to followers. The existing per-account lane is untouched. No new persisted state, no new RPC, no NATS permission change — clients already hold a wildcard `chat.room.>` subscribe grant, and the client subscribes on thread open and unsubscribes on close.

**Tech Stack:** Go 1.25, NATS core (`nats.go`), `sonic` JSON codec, `go.uber.org/mock`, `testify`, OpenTelemetry metrics.

**Spec:** `docs/superpowers/specs/2026-08-23-thread-viewer-lane-design.md`

## Global Constraints

- **TDD is mandatory.** Red → Green → Refactor → Commit. Never write implementation before its failing test exists. (CLAUDE.md §4)
- **Never run raw `go` commands.** Use `make` targets only: `make test SERVICE=<name>`, `make lint`, `make fmt`, `make generate`, `make sast`. (CLAUDE.md §2)
- **Always `-race`.** The `make test` target already supplies it.
- **Error wrapping:** `fmt.Errorf("short description: %w", err)` describing what the *current* function was doing. Never bare `err`, never `fmt.Errorf("error: %w", err)`.
- **Logging:** `log/slog` only, structured key-value fields, never interpolated strings. Never log message content, tokens, or passwords.
- **Subjects:** built only via `pkg/subject` builders, never inline `fmt.Sprintf`. (CLAUDE.md §6)
- **Coverage floor:** 80% minimum; 90%+ target for handlers and `pkg/`.
- **Test package:** tests live in the same package (`package main` for services, `package subject_test` where the existing file already does so).
- **Commit after each task**, only once that task's tests pass. Pre-commit hooks run lint and tests.
- **Branch:** `claude/thread-metadata-event-5065r0`. Never push elsewhere.

---

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `pkg/subject/subject.go` | Subject construction + routing | Add `ThreadEvent`, `ThreadEventTargets` |
| `pkg/subject/subject_test.go` | Subject unit tests | Add routing matrix tests |
| `broadcast-worker/nats_metrics.go` | Metric label enums + publisher decorator | Add `roomThreadLane` kind |
| `broadcast-worker/nats_metrics_test.go` | Metric label tests | Add normalization test |
| `broadcast-worker/handler.go` | All fan-out logic | Add `publishThreadLaneEvent`; wire into 3 handlers |
| `broadcast-worker/handler_test.go` | Handler unit tests | New tests + fix 6 existing count assertions |
| `.semgrep/room-subject.yml` | Subject-routing guard rule | Add `subject.ThreadEvent` |
| `docs/client-api.md` | Canonical client API doc | New subject + delivery notes |
| `docs/client-api/events.md` | Derived events view | Mirror |
| `docs/client-api/request-reply.md` | Derived RPC view | Mirror |

**Two findings the spec did not anticipate — both are handled below, do not skip them:**

1. **Six existing tests assert exact publish counts** and WILL fail when a fourth publish appears. The spec's claim that "every existing thread test must pass untouched" is wrong. Each is updated in the task that breaks it, with the count change justified in the diff.
2. **`handleThreadCreated` and `handleThreadUpdated` early-return when the fan-out set is empty** (`handler.go:263`, `handler.go:380`), before any payload is built. A bot-authored thread reply produces an empty fan-out (`threadFanOutAccounts` skips bots via `isBot`), so without a fix a viewer watching a bot reply would receive nothing. Tasks 4 and 5 remove those early returns.

---

## Task 1: Subject builders

**Files:**
- Modify: `pkg/subject/subject.go` (add after `RoomMemberEventTargets`, around line 462)
- Test: `pkg/subject/subject_test.go`

**Interfaces:**
- Consumes: existing unexported `roomBase(roomID string, global bool) string` (`subject.go:155`) and `roomRouteGlobals(crossSite *bool, crossSiteAt *time.Time, mode RoomRouteMode, now time.Time) []bool` (`subject.go:421`).
- Produces:
  - `subject.ThreadEvent(roomID, parentMessageID string, global bool) string`
  - `subject.ThreadEventTargets(roomID, parentMessageID string, crossSite *bool, crossSiteAt *time.Time, mode RoomRouteMode, now time.Time) []string`

- [ ] **Step 1: Write the failing tests**

Append to `pkg/subject/subject_test.go`:

```go
func TestThreadEvent(t *testing.T) {
	assert.Equal(t, "chat.room.r1.thread.p1.event", subject.ThreadEvent("r1", "p1", true))
	assert.Equal(t, "chat.local.room.r1.thread.p1.event", subject.ThreadEvent("r1", "p1", false))
}

// TestThreadEventTargets verifies the thread lane routes on exactly the same
// namespaces as RoomEventTargets — they share roomRouteGlobals, so a same-site
// room's thread events land on the local namespace once local mode is enabled.
func TestThreadEventTargets(t *testing.T) {
	g := "chat.room.r1.thread.p1.event"
	l := "chat.local.room.r1.thread.p1.event"
	trueP, falseP := true, false
	now := time.Unix(1_700_000_000, 0).UTC()

	// cross-site rooms with no flip time (born cross-site) are ALWAYS global.
	assert.Equal(t, []string{g}, subject.ThreadEventTargets("r1", "p1", &trueP, nil, subject.RouteGlobal, now))
	assert.Equal(t, []string{g}, subject.ThreadEventTargets("r1", "p1", &trueP, nil, subject.RouteDual, now))
	assert.Equal(t, []string{g}, subject.ThreadEventTargets("r1", "p1", &trueP, nil, subject.RouteLocal, now))

	// same-site rooms vary by mode.
	assert.Equal(t, []string{g}, subject.ThreadEventTargets("r1", "p1", &falseP, nil, subject.RouteGlobal, now))
	assert.Equal(t, []string{l, g}, subject.ThreadEventTargets("r1", "p1", &falseP, nil, subject.RouteDual, now))
	assert.Equal(t, []string{l}, subject.ThreadEventTargets("r1", "p1", &falseP, nil, subject.RouteLocal, now))

	// nil locality is ALWAYS global — the fail-safe.
	assert.Equal(t, []string{g}, subject.ThreadEventTargets("r1", "p1", nil, nil, subject.RouteGlobal, now))
	assert.Equal(t, []string{g}, subject.ThreadEventTargets("r1", "p1", nil, nil, subject.RouteDual, now))
	assert.Equal(t, []string{g}, subject.ThreadEventTargets("r1", "p1", nil, nil, subject.RouteLocal, now))
}

// TestThreadEventTargets_TransitionGrace covers the post-flip grace window: a
// room that just flipped same-site -> cross-site keeps a LOCAL copy for the
// window in local/dual mode, so members still on the local subject don't go
// dark on thread events until they re-subscribe.
func TestThreadEventTargets_TransitionGrace(t *testing.T) {
	g := "chat.room.r1.thread.p1.event"
	l := "chat.local.room.r1.thread.p1.event"
	trueP := true
	flip := time.Unix(1_700_000_000, 0).UTC()
	within := flip.Add(subject.DefaultRoomLocalityGrace - time.Minute)
	after := flip.Add(subject.DefaultRoomLocalityGrace + time.Minute)

	assert.Equal(t, []string{l, g}, subject.ThreadEventTargets("r1", "p1", &trueP, &flip, subject.RouteLocal, within))
	assert.Equal(t, []string{l, g}, subject.ThreadEventTargets("r1", "p1", &trueP, &flip, subject.RouteDual, within))
	// Within grace but global mode: no local audience -> global only.
	assert.Equal(t, []string{g}, subject.ThreadEventTargets("r1", "p1", &trueP, &flip, subject.RouteGlobal, within))
	// After grace: global only.
	assert.Equal(t, []string{g}, subject.ThreadEventTargets("r1", "p1", &trueP, &flip, subject.RouteLocal, after))
	// Exactly at the boundary is already expired (now.Before is strict).
	assert.Equal(t, []string{g}, subject.ThreadEventTargets("r1", "p1", &trueP, &flip, subject.RouteLocal, flip.Add(subject.DefaultRoomLocalityGrace)))
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=pkg/subject`
Expected: FAIL — `undefined: subject.ThreadEvent`, `undefined: subject.ThreadEventTargets`

- [ ] **Step 3: Write the implementation**

In `pkg/subject/subject.go`, immediately after `RoomMemberEventTargets` (ends at line 462):

```go
// ThreadEvent returns the per-thread live lane for a channel room's thread —
// the subject a client subscribes to while a thread pane is open, so a viewer
// who does not follow the thread still receives its replies.
func ThreadEvent(roomID, parentMessageID string, global bool) string {
	return roomBase(roomID, global) + ".thread." + parentMessageID + ".event"
}

// ThreadEventTargets returns the thread-lane subject(s) to publish a thread
// create/edit/delete to. Routes identically to RoomEventTargets (shared
// roomRouteGlobals) so the thread lane follows the room's own namespace: a
// same-site room's thread events land on the local prefix once local mode is
// enabled, and a flipped room dual-publishes during the grace window.
func ThreadEventTargets(roomID, parentMessageID string, crossSite *bool, crossSiteAt *time.Time, mode RoomRouteMode, now time.Time) []string {
	globals := roomRouteGlobals(crossSite, crossSiteAt, mode, now)
	out := make([]string, len(globals))
	for i, g := range globals {
		out[i] = ThreadEvent(roomID, parentMessageID, g)
	}
	return out
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=pkg/subject`
Expected: PASS, all three new tests green, no existing subject test broken.

- [ ] **Step 5: Lint and commit**

```bash
make fmt
make lint
git add pkg/subject/subject.go pkg/subject/subject_test.go
git commit -m "feat(subject): add per-thread event lane builders

ThreadEvent/ThreadEventTargets construct the subject a client subscribes to
while a thread pane is open. Routing delegates to roomRouteGlobals so the
thread lane follows the room's own local/global namespace and grace window."
```

---

## Task 2: `thread_lane` metric room kind

**Files:**
- Modify: `broadcast-worker/nats_metrics.go:15-25` (enum + `allRoomKinds`), `:104-110` (`normalizeRoomKind`)
- Test: `broadcast-worker/nats_metrics_test.go`

**Interfaces:**
- Produces: `roomThreadLane roomKindLabel = "thread_lane"`, usable by Task 3 as `withBroadcastMetricLabels(ctx, roomThreadLane, ...)`.

**Why a new kind rather than reusing `roomThread`:** the `broadcastMetrics` doc comment (`nats_metrics.go:46-55`) promises that for `thread` fan-out, `broadcast_worker_fanout_recipients` and `broadcast_worker_recipient_deliveries_total` are directly comparable, because there is one publish per recipient. The thread lane is one publish to an audience the publisher cannot observe — the channel room-stream shape the same comment explicitly excludes from that ratio. Reusing `thread` would corrupt a series documented as comparable.

- [ ] **Step 1: Write the failing test**

Append to `broadcast-worker/nats_metrics_test.go`:

```go
// TestNormalizeRoomKind_ThreadLane guards the closed-enum contract: a room kind
// missing from normalizeRoomKind's whitelist silently collapses to "unknown",
// which would send every thread-lane delivery to the wrong series.
func TestNormalizeRoomKind_ThreadLane(t *testing.T) {
	assert.Equal(t, roomThreadLane, normalizeRoomKind(roomThreadLane))
	assert.Contains(t, allRoomKinds, roomThreadLane,
		"thread_lane must be in allRoomKinds or its measurement options are never pre-built")
	assert.Equal(t, roomKindLabel("thread_lane"), roomThreadLane)
}

// TestThreadLaneLabelsSurviveContext verifies the label round-trips through the
// context the publisher decorator reads, so Delivery is recorded under
// thread_lane rather than unknown.
func TestThreadLaneLabelsSurviveContext(t *testing.T) {
	ctx := withBroadcastMetricLabels(context.Background(), roomThreadLane, natsmetrics.EventCreated)
	labels := broadcastLabels(ctx)
	assert.Equal(t, roomThreadLane, labels.roomKind)
	assert.Equal(t, natsmetrics.EventCreated, labels.eventType)
}
```

If `broadcast-worker/nats_metrics_test.go` does not exist, create it with:

```go
package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/natsmetrics"
)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=broadcast-worker`
Expected: FAIL — `undefined: roomThreadLane`

- [ ] **Step 3: Write the implementation**

In `broadcast-worker/nats_metrics.go`, extend the enum block (currently lines 17-23):

```go
const (
	roomChannel    roomKindLabel = "channel"
	roomDM         roomKindLabel = "dm"
	roomBotDM      roomKindLabel = "bot_dm"
	roomThread     roomKindLabel = "thread"
	roomThreadLane roomKindLabel = "thread_lane"
	roomUnknown    roomKindLabel = "unknown"
)

var allRoomKinds = []roomKindLabel{roomChannel, roomDM, roomBotDM, roomThread, roomThreadLane, roomUnknown}
```

And extend `normalizeRoomKind` (line 104) — **this is the step that is easy to miss; without it the label collapses to `unknown`:**

```go
func normalizeRoomKind(value roomKindLabel) roomKindLabel {
	switch value {
	case roomChannel, roomDM, roomBotDM, roomThread, roomThreadLane:
		return value
	default:
		return roomUnknown
	}
}
```

Update the `broadcastMetrics` doc comment (line 46-55) so the new kind's semantics are documented alongside the existing ones. Append this sentence to the existing paragraph:

```go
// thread_lane is one publish to a subject whose subscribers the publisher
// cannot enumerate, so it records deliveries only — like the channel
// room-stream case, its two families are NOT a ratio.
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=broadcast-worker`
Expected: PASS — both new tests green, every existing metrics test still green.

- [ ] **Step 5: Lint and commit**

```bash
make fmt
make lint
git add broadcast-worker/nats_metrics.go broadcast-worker/nats_metrics_test.go
git commit -m "feat(broadcast-worker): add thread_lane metric room kind

The thread lane is one publish to an unobservable audience, so it cannot
share the thread kind, whose fanout/deliveries pair is documented as
directly comparable. Records deliveries only."
```

---

## Task 3: `publishThreadLaneEvent` helper

**Files:**
- Modify: `broadcast-worker/handler.go` (add after `publishRoomEvent`, which ends around line 896)
- Modify: `.semgrep/room-subject.yml`
- Test: `broadcast-worker/handler_test.go`

**Interfaces:**
- Consumes: `subject.ThreadEventTargets` (Task 1), `roomThreadLane` (Task 2), existing `h.pub.Publish`, `h.routeMode`, `broadcastLabels`, `withBroadcastMetricLabels`.
- Produces: `func (h *Handler) publishThreadLaneEvent(ctx context.Context, roomID, parentMsgID string, crossSite *bool, crossSiteAt *time.Time, payload []byte, op string) error` — used by Tasks 4, 5, 6.

**Shape rationale:** this mirrors `publishRoomEvent` (`handler.go:884`), NOT `publishToThreadAccounts` (`handler.go:1021`). The lane is one subject (two only in `RouteDual`), so the per-account helper's goroutine-per-recipient fan-out and its "all N publishes failed" error rule are both meaningless here. Three consequences, all asserted by tests below: label with `roomThreadLane`; never call `natsmetrics.MarkTerminalFromContext`; log no audience count.

- [ ] **Step 1: Write the failing tests**

Append to `broadcast-worker/handler_test.go`:

```go
// TestPublishThreadLaneEvent_GlobalRoom verifies the helper publishes to the
// global thread subject for a cross-site room.
func TestPublishThreadLaneEvent_GlobalRoom(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)
	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false, subject.RouteGlobal)

	crossSite := true
	err := h.publishThreadLaneEvent(context.Background(), "r1", "p1", &crossSite, nil, []byte(`{"type":"new_thread_message"}`), "thread create")

	require.NoError(t, err)
	require.Len(t, pub.records, 1)
	assert.Equal(t, "chat.room.r1.thread.p1.event", pub.records[0].subject)
}

// TestPublishThreadLaneEvent_DualMode verifies both namespaces receive the
// event for a same-site room in dual mode, matching publishRoomEvent.
func TestPublishThreadLaneEvent_DualMode(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)
	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false, subject.RouteDual)

	sameSite := false
	err := h.publishThreadLaneEvent(context.Background(), "r1", "p1", &sameSite, nil, []byte(`{}`), "thread create")

	require.NoError(t, err)
	require.Len(t, pub.records, 2)
	assert.Equal(t, "chat.local.room.r1.thread.p1.event", pub.records[0].subject)
	assert.Equal(t, "chat.room.r1.thread.p1.event", pub.records[1].subject)
}

// TestPublishThreadLaneEvent_PublishError_ReturnsError verifies a failed target
// surfaces as an error to the caller, which is responsible for swallowing it.
func TestPublishThreadLaneEvent_PublishError_ReturnsError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{failOn: map[string]error{
		"chat.room.r1.thread.p1.event": errors.New("nats down"),
	}}
	keyStore := NewMockRoomKeyProvider(ctrl)
	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false, subject.RouteGlobal)

	crossSite := true
	err := h.publishThreadLaneEvent(context.Background(), "r1", "p1", &crossSite, nil, []byte(`{}`), "thread create")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "chat.room.r1.thread.p1.event")
	assert.Len(t, pub.records, 1, "the attempt is still recorded")
}

// TestPublishThreadLaneEvent_LabelsAsThreadLane verifies deliveries are
// attributed to the thread_lane kind, not channel (publishRoomEvent's label)
// and not thread (the per-account fan-out's label, whose fanout/deliveries
// pair is documented as directly comparable).
func TestPublishThreadLaneEvent_LabelsAsThreadLane(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &labelCapturingPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)
	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false, subject.RouteGlobal)

	crossSite := true
	require.NoError(t, h.publishThreadLaneEvent(context.Background(), "r1", "p1", &crossSite, nil, []byte(`{}`), "thread create"))

	require.Len(t, pub.seen, 1)
	assert.Equal(t, roomThreadLane, pub.seen[0])
}
```

Add this test double next to `mockPublisher` (around `handler_test.go:39`):

```go
// labelCapturingPublisher records the room-kind label each Publish call carries
// on its context, so tests can assert metric attribution without a real meter.
type labelCapturingPublisher struct {
	mu   sync.Mutex
	seen []roomKindLabel
}

func (p *labelCapturingPublisher) Publish(ctx context.Context, _ string, _ []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.seen = append(p.seen, broadcastLabels(ctx).roomKind)
	return nil
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=broadcast-worker`
Expected: FAIL — `h.publishThreadLaneEvent undefined`

- [ ] **Step 3: Write the implementation**

In `broadcast-worker/handler.go`, immediately after `publishRoomEvent`:

```go
// publishThreadLaneEvent fans a channel thread's create/edit/delete out on the
// per-thread lane, which viewers subscribe to while a thread pane is open.
//
// Mirrors publishRoomEvent, not publishToThreadAccounts: this is one subject
// (two in dual mode), not a per-recipient fan-out, so there is nothing to
// parallelize and no "all recipients failed" rule to apply. Deliveries are
// attributed to roomThreadLane because the lane's audience is unobservable —
// unlike the per-account thread fan-out, whose recipient count is exact.
//
// Callers MUST treat a returned error as best-effort: the per-account lane is
// the delivery guarantee, and failing the handler here would re-fan-out that
// lane on redelivery.
func (h *Handler) publishThreadLaneEvent(ctx context.Context, roomID, parentMsgID string, crossSite *bool, crossSiteAt *time.Time, payload []byte, op string) error {
	labels := broadcastLabels(ctx)
	ctx = withBroadcastMetricLabels(ctx, roomThreadLane, labels.eventType)
	now := time.Now().UTC()
	var pubErr error
	for _, subj := range subject.ThreadEventTargets(roomID, parentMsgID, crossSite, crossSiteAt, h.routeMode, now) {
		if err := h.pub.Publish(ctx, subj, payload); err != nil {
			pubErr = fmt.Errorf("publish %s for thread %s in room %s to %s: %w", op, parentMsgID, roomID, subj, err)
		}
	}
	// No audience figure: the lane's subscriber count is unobservable from here,
	// and an invented number is worse than none.
	slog.Log(ctx, logctx.LevelFlow, "broadcast fan-out", "phase", "fanout",
		"request_id", natsutil.RequestIDFromContext(ctx), "room_id", roomID,
		"parent_message_id", parentMsgID, "delivery", "thread-lane")
	return pubErr
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=broadcast-worker`
Expected: PASS — four new tests green.

- [ ] **Step 5: Extend the semgrep rule**

Open `.semgrep/room-subject.yml`. The `room-subject-publish-must-route` rule lists the subject builders that must not be passed inline to a publish call. Add `subject.ThreadEvent` alongside the existing `subject.RoomEvent` / `RoomMsgStream` / `RoomMetadataUpdate` / `RoomMemberEvent` patterns, following the exact pattern syntax already used in the file, and add `subject.ThreadEvent` to the rule's message text listing the guarded builders.

Run: `make sast-semgrep`
Expected: PASS — no findings. (The helper calls `ThreadEventTargets`, not `ThreadEvent`, so it does not trip its own rule.)

- [ ] **Step 6: Commit**

```bash
make fmt
make lint
git add broadcast-worker/handler.go broadcast-worker/handler_test.go .semgrep/room-subject.yml
git commit -m "feat(broadcast-worker): add publishThreadLaneEvent helper

One subject per thread (two in dual mode), routed through
ThreadEventTargets. Deliveries attributed to thread_lane so the thread
kind's comparable fanout/deliveries pair stays intact. Semgrep rule
extended so nobody inlines subject.ThreadEvent past the router."
```

---

## Task 4: Wire `handleThreadCreated`

**Files:**
- Modify: `broadcast-worker/handler.go:241-312` (`handleThreadCreated`)
- Test: `broadcast-worker/handler_test.go` — new tests + update 3 existing count assertions

**Interfaces:**
- Consumes: `publishThreadLaneEvent` (Task 3), existing `encryptRoomEvent(ctx, roomID string, clientMsg *model.ClientMessage, evt *model.RoomEvent) error` (`handler.go:835`).
- Produces: nothing new.

**Two behavioral changes in this task:**

1. The channel branch gains a thread-lane publish carrying an **encrypted** copy of the same `RoomEvent`. `encryptRoomEvent` mutates its argument (sets `EncryptedMessage`, nils `Message`) and no-ops when `h.encrypt` is false, so the plaintext payload must be marshaled and published to the per-account lane **first**, then a copy encrypted for the lane.
2. **The `len(fanOut) == 0` early return (line 263) is removed.** It skips the user lookup when nobody follows the thread — but a bot-authored reply produces an empty fan-out (`threadFanOutAccounts` drops bots), and a viewer watching that thread must still receive it. `publishToThreadAccounts` already no-ops on an empty account list (`handler.go:1025`), so removing the early return is safe; it costs one user lookup in the rare all-bot case.

- [ ] **Step 1: Write the failing tests**

Append to `broadcast-worker/handler_test.go`:

```go
// TestHandleThreadCreated_ChannelRoom_PublishesThreadLane verifies a viewer
// subscribed to the per-thread subject receives the reply, alongside the
// unchanged per-account fan-out to followers.
func TestHandleThreadCreated_ChannelRoom_PublishesThreadLane(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	store.EXPECT().GetRoomMeta(gomock.Any(), "room-1").Return(metaOf(testChannelRoom), nil)
	store.EXPECT().GetThreadFollowers(gomock.Any(), "parent-1").Return(map[string]struct{}{"bob": {}}, nil)
	us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"alice"}).Return([]model.User{testUsers[0]}, nil)

	evt := model.MessageEvent{
		Event:     model.EventCreated,
		SiteID:    "site-a",
		Timestamp: msgTime.UnixMilli(),
		Message: model.Message{
			ID: "reply-1", RoomID: "room-1", UserID: "u-alice", UserAccount: "alice",
			Content: "a thread reply", CreatedAt: msgTime,
			ThreadParentMessageID: "parent-1", TShow: false,
		},
	}
	data, _ := json.Marshal(evt)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	var lane *publishRecord
	for i := range pub.records {
		if pub.records[i].subject == "chat.room.room-1.thread.parent-1.event" {
			lane = &pub.records[i]
		}
	}
	require.NotNil(t, lane, "thread lane must receive the reply")

	var laneEvt model.RoomEvent
	require.NoError(t, json.Unmarshal(lane.data, &laneEvt))
	assert.Equal(t, model.RoomEventNewThreadMessage, laneEvt.Type)
	assert.Equal(t, "room-1", laneEvt.RoomID)
	require.NotNil(t, laneEvt.Message, "encryption off: content stays plaintext")
	assert.Equal(t, "reply-1", laneEvt.Message.ID)
}

// TestHandleThreadCreated_ThreadLaneEncrypted verifies the lane copy is
// encrypted while the per-account copy stays plaintext, and that the two
// otherwise agree. The lane rides chat.room.>, which any authenticated user
// may subscribe to, so plaintext there would be readable by non-members.
func TestHandleThreadCreated_ThreadLaneEncrypted(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	store.EXPECT().GetRoomMeta(gomock.Any(), "room-1").Return(metaOf(testChannelRoom), nil)
	store.EXPECT().GetThreadFollowers(gomock.Any(), "parent-1").Return(map[string]struct{}{}, nil)
	us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"alice"}).Return([]model.User{testUsers[0]}, nil)
	keyStore.EXPECT().Get(gomock.Any(), "room-1").Return(testRoomKey(t), nil).AnyTimes()

	evt := model.MessageEvent{
		Event: model.EventCreated, SiteID: "site-a", Timestamp: msgTime.UnixMilli(),
		Message: model.Message{
			ID: "reply-1", RoomID: "room-1", UserID: "u-alice", UserAccount: "alice",
			Content: "secret thread reply", CreatedAt: msgTime,
			ThreadParentMessageID: "parent-1", TShow: false,
		},
	}
	data, _ := json.Marshal(evt)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	var lane, perAccount *publishRecord
	for i := range pub.records {
		switch pub.records[i].subject {
		case "chat.room.room-1.thread.parent-1.event":
			lane = &pub.records[i]
		case subject.UserRoomEvent("alice"):
			perAccount = &pub.records[i]
		}
	}
	require.NotNil(t, lane)
	require.NotNil(t, perAccount)

	assert.NotContains(t, string(lane.data), "secret thread reply",
		"thread-lane payload must not carry plaintext content")

	var laneEvt, acctEvt model.RoomEvent
	require.NoError(t, json.Unmarshal(lane.data, &laneEvt))
	require.NoError(t, json.Unmarshal(perAccount.data, &acctEvt))
	assert.Nil(t, laneEvt.Message, "lane copy carries encryptedMessage, not message")
	assert.NotEmpty(t, laneEvt.EncryptedMessage)
	require.NotNil(t, acctEvt.Message, "per-account copy must stay plaintext")
	assert.Equal(t, "secret thread reply", acctEvt.Message.Content)
}

// TestHandleThreadCreated_ThreadLaneKeepsMentions verifies mentions survive on
// the lane copy: the frontend renders them with dedicated styling from this
// resolved Participant list, so a viewer must see what a follower sees.
func TestHandleThreadCreated_ThreadLaneKeepsMentions(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	store.EXPECT().GetRoomMeta(gomock.Any(), "room-1").Return(metaOf(testChannelRoom), nil)
	store.EXPECT().GetThreadFollowers(gomock.Any(), "parent-1").Return(map[string]struct{}{}, nil)
	store.EXPECT().GetHistorySharedSince(gomock.Any(), "room-1", []string{"bob"}).
		Return(map[string]*time.Time{"bob": nil}, nil)
	us.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).
		Return([]model.User{testUsers[0], testUsers[1]}, nil)

	evt := model.MessageEvent{
		Event: model.EventCreated, SiteID: "site-a", Timestamp: msgTime.UnixMilli(),
		Message: model.Message{
			ID: "reply-1", RoomID: "room-1", UserID: "u-alice", UserAccount: "alice",
			Content: "hey @bob", CreatedAt: msgTime,
			ThreadParentMessageID: "parent-1", TShow: false,
		},
	}
	data, _ := json.Marshal(evt)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	var lane *publishRecord
	for i := range pub.records {
		if pub.records[i].subject == "chat.room.room-1.thread.parent-1.event" {
			lane = &pub.records[i]
		}
	}
	require.NotNil(t, lane)

	var laneEvt model.RoomEvent
	require.NoError(t, json.Unmarshal(lane.data, &laneEvt))
	assert.NotEmpty(t, laneEvt.Mentions, "mentions drive frontend styling and must reach viewers")
}

// TestHandleThreadCreated_DMRoom_NoThreadLane verifies DM rooms publish nothing
// to the lane: DM thread replies already reach every member, so a lane publish
// would be pure duplication.
func TestHandleThreadCreated_DMRoom_NoThreadLane(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	store.EXPECT().GetRoomMeta(gomock.Any(), "dm-1").Return(metaOf(testDMRoom), nil)
	us.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).Return([]model.User{testUsers[0]}, nil)
	store.EXPECT().ListSubscriptions(gomock.Any(), "dm-1").Return(testDMSubs, nil)

	evt := model.MessageEvent{
		Event: model.EventCreated, SiteID: "site-a", Timestamp: msgTime.UnixMilli(),
		Message: model.Message{
			ID: "reply-1", RoomID: "dm-1", UserID: "u-alice", UserAccount: "alice",
			Content: "dm thread reply", CreatedAt: msgTime,
			ThreadParentMessageID: "parent-1", TShow: false,
		},
	}
	data, _ := json.Marshal(evt)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	for _, r := range pub.records {
		assert.NotContains(t, r.subject, ".thread.", "DM rooms must not use the thread lane")
	}
}

// TestHandleThreadCreated_ThreadLaneFailure_Swallowed verifies a lane publish
// failure neither fails the handler nor blocks the per-account fan-out. Failing
// here would NAK the canonical message and re-fan-out the per-account lane,
// turning one viewer's cosmetic miss into duplicate delivery for everyone.
func TestHandleThreadCreated_ThreadLaneFailure_Swallowed(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{failOn: map[string]error{
		"chat.room.room-1.thread.parent-1.event": errors.New("nats down"),
	}}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	store.EXPECT().GetRoomMeta(gomock.Any(), "room-1").Return(metaOf(testChannelRoom), nil)
	store.EXPECT().GetThreadFollowers(gomock.Any(), "parent-1").Return(map[string]struct{}{"bob": {}}, nil)
	us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"alice"}).Return([]model.User{testUsers[0]}, nil)

	evt := model.MessageEvent{
		Event: model.EventCreated, SiteID: "site-a", Timestamp: msgTime.UnixMilli(),
		Message: model.Message{
			ID: "reply-1", RoomID: "room-1", UserID: "u-alice", UserAccount: "alice",
			Content: "a thread reply", CreatedAt: msgTime,
			ThreadParentMessageID: "parent-1", TShow: false,
		},
	}
	data, _ := json.Marshal(evt)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), data), "lane failure must not fail the handler")

	subjects := map[string]bool{}
	for _, r := range pub.records {
		subjects[r.subject] = true
	}
	assert.True(t, subjects[subject.UserRoomEvent("alice")], "per-account fan-out must still happen")
	assert.True(t, subjects[subject.UserRoomEvent("bob")])
}

// TestHandleThreadCreated_EmptyFanOut_StillPublishesThreadLane covers a
// bot-authored reply: threadFanOutAccounts drops bots, so the per-account set
// is empty, but a viewer with the pane open must still receive the reply.
func TestHandleThreadCreated_EmptyFanOut_StillPublishesThreadLane(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	store.EXPECT().GetRoomMeta(gomock.Any(), "room-1").Return(metaOf(testChannelRoom), nil)
	store.EXPECT().GetThreadFollowers(gomock.Any(), "parent-1").Return(map[string]struct{}{}, nil)
	us.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

	evt := model.MessageEvent{
		Event: model.EventCreated, SiteID: "site-a", Timestamp: msgTime.UnixMilli(),
		Message: model.Message{
			ID: "reply-1", RoomID: "room-1", UserAccount: "helper.bot", Content: "bot reply",
			CreatedAt: msgTime, ThreadParentMessageID: "parent-1", TShow: false,
		},
	}
	data, _ := json.Marshal(evt)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	var found bool
	for _, r := range pub.records {
		if r.subject == "chat.room.room-1.thread.parent-1.event" {
			found = true
		}
	}
	assert.True(t, found, "an empty per-account fan-out must not suppress the thread lane")
}
```

**Fixtures used above, all already in `handler_test.go`:** `testRoomKey(t)` (line 177 shows the call form — it takes `t`), `metaOf(testChannelRoom)` / `metaOf(testDMRoom)`, `testUsers`, `testDMSubs`, `defaultParentFetcher`, `mockPublisher`, `publishRecord`. Nothing new to declare.

**Watch the room id.** `testChannelRoom.ID` is `"room-1"` and `testDMRoom.ID` is `"dm-1"` (`handler_test.go:86-96`). The lane subject is built from `meta.ID`, so it is `chat.room.room-1.thread.parent-1.event`. Note that some existing tests pass `"room-1"` as the `GetRoomMeta` mock argument while returning `metaOf(testChannelRoom)`, whose ID is `"room-1"` — they get away with it because per-account subjects carry no room id. The lane subject does, so use `"room-1"` throughout.

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=broadcast-worker`
Expected: FAIL — the new tests fail on the missing lane publish. Three existing tests also now fail on their count assertions; that is expected and fixed in Step 4.

- [ ] **Step 3: Write the implementation**

Replace the channel branch of `handleThreadCreated`. Delete the early return at lines 263-268:

```go
		if len(fanOut) == 0 {
			slog.DebugContext(ctx, "no thread subscribers to notify for thread reply",
				"parentMessageID", parentMsgID,
				"request_id", natsutil.RequestIDFromContext(ctx))
			return nil
		}
```

Replace it with a debug log that does not return, so the thread-lane publish below still runs:

```go
		if len(fanOut) == 0 {
			// No per-account recipients (e.g. a bot-authored reply), but the
			// thread lane below still serves viewers with the pane open.
			slog.DebugContext(ctx, "no thread subscribers to notify for thread reply",
				"parentMessageID", parentMsgID,
				"request_id", natsutil.RequestIDFromContext(ctx))
		}
```

Then replace the channel `switch` arm (currently ending `return h.publishToThreadAccounts(...)`) with:

```go
	case model.RoomTypeChannel:
		// Do NOT call SetSubscriptionMentions here: TShow=false replies are invisible
		// in the main channel, so a room-level mention badge would appear with no
		// visible message to explain it.
		roomEvt := buildRoomEvent(&meta, clientMsg, evt.Timestamp)
		roomEvt.Type = model.RoomEventNewThreadMessage
		roomEvt.MentionAll = resolved.MentionAll
		if len(resolved.Participants) > 0 {
			roomEvt.Mentions = resolved.Participants
		}
		payload, err := sonic.Marshal(roomEvt)
		if err != nil {
			return fmt.Errorf("marshal thread created event for parent %s: %w", parentMsgID, err)
		}
		if err := h.publishToThreadAccounts(ctx, fanOut, payload, parentMsgID); err != nil {
			return fmt.Errorf("publish thread created event for parent %s: %w", parentMsgID, err)
		}
		// Thread lane: same event, encrypted, for viewers who do not follow the
		// thread. Encrypt a copy AFTER the plaintext publish above —
		// encryptRoomEvent nils Message in place.
		h.publishThreadLaneCreated(ctx, &meta, clientMsg, parentMsgID, roomEvt)
		return nil
```

Add the companion method immediately after `handleThreadCreated`:

```go
// publishThreadLaneCreated sends the encrypted twin of a thread reply on the
// per-thread lane. Best-effort: errors are logged, never returned, because the
// per-account lane above is the delivery guarantee and failing the handler
// would re-fan-out that lane on redelivery.
func (h *Handler) publishThreadLaneCreated(ctx context.Context, meta *roommetacache.Meta, clientMsg *model.ClientMessage, parentMsgID string, plain model.RoomEvent) {
	laneEvt := plain
	if err := h.encryptRoomEvent(ctx, meta.ID, clientMsg, &laneEvt); err != nil {
		slog.ErrorContext(ctx, "encrypt thread lane event failed",
			"error", err, "room_id", meta.ID, "parentMessageID", parentMsgID,
			"request_id", natsutil.RequestIDFromContext(ctx))
		return
	}
	lanePayload, err := sonic.Marshal(laneEvt)
	if err != nil {
		slog.ErrorContext(ctx, "marshal thread lane event failed",
			"error", err, "room_id", meta.ID, "parentMessageID", parentMsgID,
			"request_id", natsutil.RequestIDFromContext(ctx))
		return
	}
	if err := h.publishThreadLaneEvent(ctx, meta.ID, parentMsgID, meta.CrossSite, meta.CrossSiteAt, lanePayload, "thread create"); err != nil {
		slog.ErrorContext(ctx, "publish thread lane event failed",
			"error", err, "room_id", meta.ID, "parentMessageID", parentMsgID,
			"request_id", natsutil.RequestIDFromContext(ctx))
	}
}
```

- [ ] **Step 4: Update the three existing count assertions**

These tests assert an exact `pub.records` length that is now one higher on the channel path. Update each, and extend each with an assertion naming the new record so the count change is self-documenting rather than a bare number bump:

- `TestHandleThreadCreated_ChannelRoom_FansOutToFollowers` (~line 2427): `require.Len(t, pub.records, 3)` → `4`. The existing loop asserts every record unmarshals as a `RoomEvent` with type `new_thread_message`, which the lane record also satisfies, so only the count and the subject-set assertions need changing: add `assert.True(t, subjects["chat.room.room-1.thread.parent-1.event"])`.
- `TestHandleThreadCreated_ChannelRoom_NoFollowers_SendsToSenderOnly` (~line 2475): `1` → `2`, plus the same subject assertion.
- `TestHandleThreadCreated_ChannelRoom_ParentAuthorFannedOutBeforeThreadRoomExists` (~line 2524): `2` → `3`, plus the same subject assertion.

Leave every DM/BotDM thread test untouched — they must still pass unchanged, which is the proof that DM behavior did not move.

- [ ] **Step 5: Run tests to verify they pass**

Run: `make test SERVICE=broadcast-worker`
Expected: PASS — all new tests green, the three updated tests green, every DM test green with no edit.

- [ ] **Step 6: Commit**

```bash
make fmt
make lint
git add broadcast-worker/handler.go broadcast-worker/handler_test.go
git commit -m "feat(broadcast-worker): publish thread replies to the thread lane

Channel thread replies now also go out encrypted on the per-thread
subject, so a viewer who does not follow the thread receives them live.
The per-account follower lane is unchanged and stays plaintext.

Removes the empty-fan-out early return: a bot-authored reply has no
per-account recipients but must still reach viewers."
```

---

## Task 5: Wire `handleThreadUpdated`

**Files:**
- Modify: `broadcast-worker/handler.go:356-402` (`handleThreadUpdated`)
- Test: `broadcast-worker/handler_test.go` — new tests + update 1 existing count assertion

**Interfaces:**
- Consumes: `publishThreadLaneEvent` (Task 3), existing `encryptEditedContent(ctx context.Context, roomID string, edited *model.EditRoomEvent) error` (`handler.go:802`).

**Note the difference from Task 4:** `encryptEditedContent` does **not** check `h.encrypt` internally (unlike `encryptRoomEvent`). `handleUpdated` guards it explicitly at line 336. The lane path must guard the same way, or an unencrypted deployment will fail on a missing room key.

- [ ] **Step 1: Write the failing tests**

Append to `broadcast-worker/handler_test.go`:

```go
// TestHandleThreadUpdated_ChannelRoom_PublishesThreadLane verifies an edit
// reaches viewers, so a viewer never sits on stale content a follower has seen
// corrected.
func TestHandleThreadUpdated_ChannelRoom_PublishesThreadLane(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	room := &model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a"}
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(room, nil)
	store.EXPECT().GetThreadFollowers(gomock.Any(), "parent-1").Return(map[string]struct{}{"bob": {}}, nil)

	evt := model.MessageEvent{
		Event: model.EventUpdated, SiteID: "site-a", Timestamp: msgTime.UnixMilli(),
		Message: model.Message{
			ID: "reply-1", RoomID: "r1", UserAccount: "alice", Content: "edited reply",
			CreatedAt: msgTime, EditedAt: &msgTime, UpdatedAt: &msgTime,
			ThreadParentMessageID: "parent-1", TShow: false,
		},
	}
	data, _ := json.Marshal(evt)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	var lane *publishRecord
	for i := range pub.records {
		if pub.records[i].subject == "chat.room.r1.thread.parent-1.event" {
			lane = &pub.records[i]
		}
	}
	require.NotNil(t, lane, "thread lane must receive the edit")

	var laneEvt model.EditRoomEvent
	require.NoError(t, json.Unmarshal(lane.data, &laneEvt))
	assert.Equal(t, model.RoomEventMessageEdited, laneEvt.Type)
	assert.Equal(t, "reply-1", laneEvt.MessageID)
	assert.Equal(t, "edited reply", laneEvt.NewContent, "encryption off: plaintext")
}

// TestHandleThreadUpdated_ThreadLaneEncrypted verifies the edit's new content is
// encrypted on the lane and cleared from the plaintext field, while the
// per-account copy keeps plaintext.
func TestHandleThreadUpdated_ThreadLaneEncrypted(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	room := &model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a"}
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(room, nil)
	store.EXPECT().GetThreadFollowers(gomock.Any(), "parent-1").Return(map[string]struct{}{"bob": {}}, nil)
	keyStore.EXPECT().Get(gomock.Any(), "r1").Return(testRoomKey(t), nil).AnyTimes()

	evt := model.MessageEvent{
		Event: model.EventUpdated, SiteID: "site-a", Timestamp: msgTime.UnixMilli(),
		Message: model.Message{
			ID: "reply-1", RoomID: "r1", UserAccount: "alice", Content: "secret edit",
			CreatedAt: msgTime, EditedAt: &msgTime, UpdatedAt: &msgTime,
			ThreadParentMessageID: "parent-1", TShow: false,
		},
	}
	data, _ := json.Marshal(evt)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	var lane, perAccount *publishRecord
	for i := range pub.records {
		switch pub.records[i].subject {
		case "chat.room.r1.thread.parent-1.event":
			lane = &pub.records[i]
		case subject.UserRoomEvent("bob"):
			perAccount = &pub.records[i]
		}
	}
	require.NotNil(t, lane)
	require.NotNil(t, perAccount)

	assert.NotContains(t, string(lane.data), "secret edit")

	var laneEvt, acctEvt model.EditRoomEvent
	require.NoError(t, json.Unmarshal(lane.data, &laneEvt))
	require.NoError(t, json.Unmarshal(perAccount.data, &acctEvt))
	assert.Empty(t, laneEvt.NewContent, "lane copy must clear plaintext newContent")
	assert.NotEmpty(t, laneEvt.EncryptedNewContent)
	assert.Equal(t, "secret edit", acctEvt.NewContent, "per-account copy stays plaintext")
}

// TestHandleThreadUpdated_DMRoom_NoThreadLane verifies DM edits skip the lane.
func TestHandleThreadUpdated_DMRoom_NoThreadLane(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	room := &model.Room{ID: "dm1", Type: model.RoomTypeDM, SiteID: "site-a", Accounts: []string{"alice", "bob"}}
	store.EXPECT().GetRoom(gomock.Any(), "dm1").Return(room, nil)

	evt := model.MessageEvent{
		Event: model.EventUpdated, SiteID: "site-a", Timestamp: msgTime.UnixMilli(),
		Message: model.Message{
			ID: "reply-1", RoomID: "dm1", UserAccount: "alice", Content: "edited",
			CreatedAt: msgTime, EditedAt: &msgTime, UpdatedAt: &msgTime,
			ThreadParentMessageID: "parent-1", TShow: false,
		},
	}
	data, _ := json.Marshal(evt)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	for _, r := range pub.records {
		assert.NotContains(t, r.subject, ".thread.")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=broadcast-worker`
Expected: FAIL — no lane record found. `TestHandleThreadUpdated_ChannelRoom_FansOutToFollowers` also fails on its count; fixed in Step 4.

- [ ] **Step 3: Write the implementation**

In `handleThreadUpdated`, make the empty fan-out non-terminal (same reasoning as Task 4) and add the lane publish. Replace the channel arm:

```go
	case model.RoomTypeChannel:
		parsed := mention.Parse(msg.Content)
		fanOut, err := h.channelThreadFanOut(ctx, room.ID, room.SiteID, parentMsgID, msg.UserAccount, parsed.Accounts, msg.ThreadParentMessageCreatedAt, evt.ThreadParentSenderAccount)
		if err != nil {
			return fmt.Errorf("channel thread fan-out for thread update of parent %s: %w", parentMsgID, err)
		}
		if len(fanOut) == 0 {
			// No per-account recipients, but the thread lane below still serves
			// viewers with the pane open.
			slog.DebugContext(ctx, "no thread subscribers to notify for thread update",
				"parentMessageID", parentMsgID,
				"request_id", natsutil.RequestIDFromContext(ctx))
		}
		payload, err := sonic.Marshal(&edit)
		if err != nil {
			return fmt.Errorf("marshal thread edit event for parent %s: %w", parentMsgID, err)
		}
		if err := h.publishToThreadAccounts(ctx, fanOut, payload, parentMsgID); err != nil {
			return fmt.Errorf("publish thread edit event for parent %s: %w", parentMsgID, err)
		}
		h.publishThreadLaneEdit(ctx, room, parentMsgID, edit)
		return nil
```

Add the companion method immediately after `handleThreadUpdated`:

```go
// publishThreadLaneEdit sends the encrypted twin of a thread-reply edit on the
// per-thread lane. Best-effort, like publishThreadLaneCreated.
//
// encryptEditedContent does not check h.encrypt itself (unlike
// encryptRoomEvent), so the guard is explicit here — matching handleUpdated.
func (h *Handler) publishThreadLaneEdit(ctx context.Context, room *model.Room, parentMsgID string, plain model.EditRoomEvent) {
	laneEdit := plain
	if h.encrypt {
		if err := h.encryptEditedContent(ctx, room.ID, &laneEdit); err != nil {
			slog.ErrorContext(ctx, "encrypt thread lane edit failed",
				"error", err, "room_id", room.ID, "parentMessageID", parentMsgID,
				"request_id", natsutil.RequestIDFromContext(ctx))
			return
		}
	}
	lanePayload, err := sonic.Marshal(&laneEdit)
	if err != nil {
		slog.ErrorContext(ctx, "marshal thread lane edit failed",
			"error", err, "room_id", room.ID, "parentMessageID", parentMsgID,
			"request_id", natsutil.RequestIDFromContext(ctx))
		return
	}
	if err := h.publishThreadLaneEvent(ctx, room.ID, parentMsgID, room.CrossSite, room.CrossSiteAt, lanePayload, "thread edit"); err != nil {
		slog.ErrorContext(ctx, "publish thread lane edit failed",
			"error", err, "room_id", room.ID, "parentMessageID", parentMsgID,
			"request_id", natsutil.RequestIDFromContext(ctx))
	}
}
```

- [ ] **Step 4: Update the existing count assertion**

`TestHandleThreadUpdated_ChannelRoom_FansOutToFollowers` (~line 2851): `require.Len(t, pub.records, 3)` → `4`, and add `assert.True(t, subjects["chat.room.r1.thread.parent-1.event"])` (adjusting the ids to whatever that test uses). Leave `TestHandleThreadUpdated_DMRoom_FansOutToAllMembers` untouched.

- [ ] **Step 5: Run tests to verify they pass**

Run: `make test SERVICE=broadcast-worker`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
make fmt
make lint
git add broadcast-worker/handler.go broadcast-worker/handler_test.go
git commit -m "feat(broadcast-worker): publish thread edits to the thread lane

Without this a viewer keeps rendering content a follower has already seen
corrected. Guards encryptEditedContent on h.encrypt explicitly, matching
handleUpdated — unlike encryptRoomEvent it does not check internally."
```

---

## Task 6: Wire `handleThreadDeleted`

**Files:**
- Modify: `broadcast-worker/handler.go:405-462` (`handleThreadDeleted`)
- Test: `broadcast-worker/handler_test.go` — new tests + update 2 existing count assertions

**Interfaces:**
- Consumes: `publishThreadLaneEvent` (Task 3).

**Simplest of the three:** `DeleteRoomEvent` carries no message content (`buildDeleteRoomEvent`, `handler.go:782`), so there is no encryption step and the same payload serves both lanes.

- [ ] **Step 1: Write the failing tests**

Append to `broadcast-worker/handler_test.go`:

```go
// TestHandleThreadDeleted_ChannelRoom_PublishesThreadLane verifies a delete
// reaches viewers, so a viewer does not keep rendering a removed reply.
func TestHandleThreadDeleted_ChannelRoom_PublishesThreadLane(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	room := &model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a"}
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(room, nil)
	store.EXPECT().GetThreadFollowers(gomock.Any(), "parent-1").Return(map[string]struct{}{"bob": {}}, nil)

	evt := model.MessageEvent{
		Event: model.EventDeleted, SiteID: "site-a", Timestamp: msgTime.UnixMilli(),
		Message: model.Message{
			ID: "reply-1", RoomID: "r1", UserAccount: "alice",
			CreatedAt: msgTime, UpdatedAt: &msgTime,
			ThreadParentMessageID: "parent-1", TShow: false,
		},
	}
	data, _ := json.Marshal(evt)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	var lane *publishRecord
	for i := range pub.records {
		if pub.records[i].subject == "chat.room.r1.thread.parent-1.event" {
			lane = &pub.records[i]
		}
	}
	require.NotNil(t, lane, "thread lane must receive the delete")

	var laneEvt model.DeleteRoomEvent
	require.NoError(t, json.Unmarshal(lane.data, &laneEvt))
	assert.Equal(t, model.RoomEventMessageDeleted, laneEvt.Type)
	assert.Equal(t, "reply-1", laneEvt.MessageID)
	assert.Equal(t, "parent-1", laneEvt.ThreadParentMessageID)
}

// TestHandleThreadDeleted_ThreadLaneNotEncrypted verifies the delete payload is
// identical on both lanes: it carries no message content, so there is nothing
// to encrypt and no reason to build a second payload.
func TestHandleThreadDeleted_ThreadLaneNotEncrypted(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	room := &model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a"}
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(room, nil)
	store.EXPECT().GetThreadFollowers(gomock.Any(), "parent-1").Return(map[string]struct{}{"bob": {}}, nil)

	evt := model.MessageEvent{
		Event: model.EventDeleted, SiteID: "site-a", Timestamp: msgTime.UnixMilli(),
		Message: model.Message{
			ID: "reply-1", RoomID: "r1", UserAccount: "alice",
			CreatedAt: msgTime, UpdatedAt: &msgTime,
			ThreadParentMessageID: "parent-1", TShow: false,
		},
	}
	data, _ := json.Marshal(evt)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, true, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	var lane, perAccount *publishRecord
	for i := range pub.records {
		switch pub.records[i].subject {
		case "chat.room.r1.thread.parent-1.event":
			lane = &pub.records[i]
		case subject.UserRoomEvent("bob"):
			perAccount = &pub.records[i]
		}
	}
	require.NotNil(t, lane)
	require.NotNil(t, perAccount)
	assert.JSONEq(t, string(perAccount.data), string(lane.data),
		"delete carries no content, so both lanes send the same payload")
}

// TestHandleThreadDeleted_DMRoom_NoThreadLane verifies DM deletes skip the lane.
func TestHandleThreadDeleted_DMRoom_NoThreadLane(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	room := &model.Room{ID: "dm1", Type: model.RoomTypeDM, SiteID: "site-a", Accounts: []string{"alice", "bob"}}
	store.EXPECT().GetRoom(gomock.Any(), "dm1").Return(room, nil)

	evt := model.MessageEvent{
		Event: model.EventDeleted, SiteID: "site-a", Timestamp: msgTime.UnixMilli(),
		Message: model.Message{
			ID: "reply-1", RoomID: "dm1", UserAccount: "alice",
			CreatedAt: msgTime, UpdatedAt: &msgTime,
			ThreadParentMessageID: "parent-1", TShow: false,
		},
	}
	data, _ := json.Marshal(evt)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false, subject.RouteGlobal)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	for _, r := range pub.records {
		assert.NotContains(t, r.subject, ".thread.")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=broadcast-worker`
Expected: FAIL — no lane record. Two existing tests also fail on counts; fixed in Step 4.

- [ ] **Step 3: Write the implementation**

In `handleThreadDeleted`'s channel arm, after the existing `publishToThreadAccounts` call, add the lane publish. The existing arm already tolerates an empty fan-out (it guards with `if len(fanOut) > 0` rather than returning), so no early-return change is needed here — but the lane publish must sit **outside** that guard:

```go
	case model.RoomTypeChannel:
		// Parse @-mentions from the deleted message so that non-follower
		// recipients who received the create event (via mention fan-out) also
		// receive the delete. Only the channel path uses mentions; the DM path
		// fans out to all members.
		parsed := mention.Parse(msg.Content)
		fanOut, err := h.channelThreadFanOut(ctx, room.ID, room.SiteID, parentMsgID, msg.UserAccount, parsed.Accounts, msg.ThreadParentMessageCreatedAt, evt.ThreadParentSenderAccount)
		if err != nil {
			return fmt.Errorf("channel thread fan-out for thread delete of parent %s: %w", parentMsgID, err)
		}
		payload, err := sonic.Marshal(&del)
		if err != nil {
			return fmt.Errorf("marshal thread delete event for parent %s: %w", parentMsgID, err)
		}
		if len(fanOut) > 0 {
			if err := h.publishToThreadAccounts(ctx, fanOut, payload, parentMsgID); err != nil {
				return fmt.Errorf("publish thread delete event for parent %s: %w", parentMsgID, err)
			}
		}
		// Thread lane: DeleteRoomEvent carries no message content, so the same
		// payload serves both lanes — no encryption, no second marshal.
		if err := h.publishThreadLaneEvent(ctx, room.ID, parentMsgID, room.CrossSite, room.CrossSiteAt, payload, "thread delete"); err != nil {
			slog.ErrorContext(ctx, "publish thread lane delete failed",
				"error", err, "room_id", room.ID, "parentMessageID", parentMsgID,
				"request_id", natsutil.RequestIDFromContext(ctx))
		}
```

Note the `payload` marshal moved above the `len(fanOut) > 0` guard so the lane publish can reuse it.

- [ ] **Step 4: Update the two existing count assertions**

- `TestHandleThreadDeleted_ChannelRoom_FansOutToFollowers` (~line 3100): `require.Len(t, pub.records, 3)` → `4`, plus a subject assertion for the lane.
- `TestHandleThreadDeleted_ChannelRoom_WithBadgeUpdate` (~line 3156): `require.Len(t, pub.records, 3)` → `4`. This test also asserts the badge (`thread_metadata_updated`) publish; keep that assertion unchanged — the badge is orthogonal to the lane.

Leave `TestHandleThreadDeleted_DMRoom_FansOutToAllMembers` untouched.

- [ ] **Step 5: Run the full unit suite**

Run: `make test`
Expected: PASS across every package — this is the first point where all three handlers are wired, so run the whole suite, not just `broadcast-worker`.

- [ ] **Step 6: Run SAST**

Run: `make sast`
Expected: PASS, no medium-or-higher findings.

- [ ] **Step 7: Commit**

```bash
make fmt
make lint
git add broadcast-worker/handler.go broadcast-worker/handler_test.go
git commit -m "feat(broadcast-worker): publish thread deletes to the thread lane

Completes the viewer lane: a viewer no longer keeps rendering a reply that
has been removed. DeleteRoomEvent carries no content, so one payload
serves both lanes."
```

---

## Task 7: Client API documentation

**Files:**
- Modify: `docs/client-api.md`
- Modify: `docs/client-api/events.md`
- Modify: `docs/client-api/request-reply.md`

**Required by CLAUDE.md §5:** any change to how a client-facing event is delivered must update `docs/client-api.md` and its derived views in the same PR. The derived views must never drift from the canonical doc.

There is no test cycle for this task; its gate is a careful read against the shipped behavior.

- [ ] **Step 1: Update the subject-patterns table**

In `docs/client-api/events.md`, the table under "Subject patterns" (around line 44) lists each subject and the events it carries. Add a row:

| Subject | Events delivered |
|---|---|
| `chat.room.{roomID}.thread.{parentMessageId}.event` (or `chat.local.room.…`, by `crossSite`) | new_thread_message, message_edited, message_deleted — **channel rooms only**; subscribe while a thread pane is open |

Mirror this row into the equivalent table in `docs/client-api.md`.

- [ ] **Step 2: Update the `new_thread_message` delivery note**

`docs/client-api/events.md:488-493` currently states delivery is per-subscriber only. Replace that paragraph with text covering both lanes:

> **Delivery differs from `new_message`.** A channel thread reply is **not** published room-wide on
> `chat.room.{roomID}.event`. It travels two lanes:
>
> 1. **Per-subscriber**, on `chat.user.{account}.event.room`, to the reply sender, the parent-message
>    author, thread followers (anyone who has replied in the thread), and history-gated @-mentioned
>    accounts. Plaintext.
> 2. **Per-thread**, on `chat.room.{roomID}.thread.{parentMessageId}.event`, for clients that
>    subscribe while the thread pane is open — including users who do not follow the thread.
>    Encrypted with the room key when room encryption is enabled, exactly like `new_message` on the
>    room lane.
>
> A follower who has the thread open receives the event on **both** lanes. Deduplicate by
> `message.id` — required regardless, since delivery is at-least-once.
>
> DM/botDM thread replies fan out **per member** on `chat.user.{account}.event.room` and do **not**
> use the thread lane; every member already receives them. The bot account is skipped (`isBot`).

- [ ] **Step 3: Add the same second-lane note to `message_edited` and `message_deleted`**

Both event sections in `docs/client-api/events.md` need a short paragraph stating that when the
message is a thread reply in a channel room (`threadParentMessageId` set, `tshow` false), the event
is also published on `chat.room.{roomID}.thread.{parentMessageId}.event` for thread viewers, and that
the `message_edited` lane copy carries `encryptedNewContent` rather than `newContent` when encryption
is enabled.

- [ ] **Step 4: Document the subscribe lifecycle**

Add a short subsection to `docs/client-api.md` near the thread event documentation:

> **Subscribing to a thread.** Opening a thread pane: subscribe to
> `chat.room.{roomID}.thread.{parentMessageId}.event`, choosing the `chat.room.` or
> `chat.local.room.` prefix from the room's `crossSite` value exactly as for
> `chat.room.{roomID}.event`. Closing the pane: unsubscribe. There is no RPC and no server-side
> state — subscribing does **not** make you a thread follower, does not create a thread
> subscription, does not accrue unread, and does not place the thread in your thread list. Closing
> the pane ends delivery.

- [ ] **Step 5: Update `request-reply.md`'s Send Message emits line**

`docs/client-api/request-reply.md:2279` lists what Send Message emits. Extend the
`new_thread_message` reference so it names both lanes, matching the events.md wording.

- [ ] **Step 6: Verify the three docs agree**

Re-read the three files' thread sections side by side. Every subject string, event name, and
encryption statement must match exactly. A derived view that disagrees with `client-api.md` is a
CLAUDE.md violation.

- [ ] **Step 7: Commit**

```bash
git add docs/client-api.md docs/client-api/events.md docs/client-api/request-reply.md
git commit -m "docs(client-api): document the per-thread viewer lane

Adds the new subject, the two-lane delivery model for channel thread
replies/edits/deletes, the dedupe-by-message-id requirement, and the
subscribe-on-open lifecycle. Mirrors into both derived views."
```

---

## Final verification

- [ ] **Run the full unit suite:** `make test` — all packages green.
- [ ] **Lint:** `make lint` — clean.
- [ ] **SAST:** `make sast` — no medium-or-higher findings.
- [ ] **Confirm DM tests were never edited:** `git diff master...HEAD -- broadcast-worker/handler_test.go | grep -c "DMRoom"` should show only additions from the new `_NoThreadLane` tests, never edits to existing DM assertions. DM behavior not moving is the core regression guarantee.
- [ ] **Confirm the branch:** `git branch --show-current` is `claude/thread-metadata-event-5065r0`.
- [ ] **Push:** `git push -u origin claude/thread-metadata-event-5065r0`

## Out of scope

Per the spec's §11, do NOT implement in this plan: a follow/unfollow RPC, exposing the follower set
to clients, viewer unread/badge/notification behavior, `thread_message_read` on the lane (already
room-wide), or repairing the `ThreadSubscription` / `replyAccounts` non-atomic split.
