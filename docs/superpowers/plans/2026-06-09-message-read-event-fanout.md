# MessageReadEvent Fan-out Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish a typed `message_read` fan-out event from the `messageRead` RPC whenever a room's read floor (`MinUserLastSeenAt`) actually advances, so peer clients update read-receipt / unread UI live.

**Architecture:** Add a flat `MessageReadEvent` model (RFC3339 nullable floor, RoomEvent-family convention). In `room-service`'s `messageRead` handler, inside the existing `!sameFloor(...)` branch, dispatch by room type to two new best-effort fan-out methods — `publishChannelEvent` (one publish to the room subject) and `publishDMEvents` (per-subscriber via `ListSubscriptionsByRoom`). Both publish over core NATS and never fail the RPC.

**Tech Stack:** Go 1.25, NATS core (`publishCore`), `go.uber.org/mock`, `stretchr/testify`, `log/slog`.

**Spec:** `docs/superpowers/specs/2026-06-09-message-read-event-fanout-design.md`

---

## File Structure

- `pkg/model/event.go` — add `RoomEventMessageRead` constant + `MessageReadEvent` struct (Task 1).
- `pkg/model/model_test.go` — round-trip test for the new struct (Task 1).
- `room-service/handler.go` — add `buildMessageReadEvent`, `publishChannelEvent`, `publishDMEvents`; wire dispatch into `messageRead` (Task 2).
- `room-service/handler_test.go` — extend `newMessageReadFixture` with a `publishCore` capture; add fan-out tests (Task 2).
- `docs/client-api.md` — document the new `message_read` event under Mark Messages Read (Task 3).

No new store methods (`ListSubscriptionsByRoom` already exists on `RoomStore`); no mock regeneration needed.

---

## Task 1: Model — `MessageReadEvent`

**Files:**
- Modify: `pkg/model/event.go` (const block at lines 201-210; add struct after `RoomEvent`, ~line 235)
- Test: `pkg/model/model_test.go`

- [ ] **Step 1: Write the failing test**

Append to `pkg/model/model_test.go`:

```go
func TestMessageReadEventJSON(t *testing.T) {
	floor := time.Date(2026, 6, 9, 10, 30, 0, 0, time.UTC)

	t.Run("floor present round-trips", func(t *testing.T) {
		src := model.MessageReadEvent{
			Type:              model.RoomEventMessageRead,
			RoomID:            "room-1",
			MinUserLastSeenAt: &floor,
			Timestamp:         floor.UnixMilli(),
		}
		data, err := json.Marshal(src)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var dst model.MessageReadEvent
		if err := json.Unmarshal(data, &dst); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if !reflect.DeepEqual(src, dst) {
			t.Errorf("round-trip mismatch:\n  got  %+v\n  want %+v", dst, src)
		}
	})

	t.Run("nil floor omitted from wire", func(t *testing.T) {
		src := model.MessageReadEvent{
			Type:      model.RoomEventMessageRead,
			RoomID:    "room-2",
			Timestamp: floor.UnixMilli(),
		}
		data, err := json.Marshal(src)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if strings.Contains(string(data), "minUserLastSeenAt") {
			t.Errorf("nil floor must be omitted, got %s", data)
		}
		var dst model.MessageReadEvent
		if err := json.Unmarshal(data, &dst); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if dst.MinUserLastSeenAt != nil {
			t.Errorf("expected nil floor, got %v", dst.MinUserLastSeenAt)
		}
	})
}

func TestRoomEventMessageReadValue(t *testing.T) {
	if model.RoomEventMessageRead != "message_read" {
		t.Errorf("RoomEventMessageRead = %q, want %q", model.RoomEventMessageRead, "message_read")
	}
}
```

Note: `model_test.go` already imports `encoding/json`, `reflect`, `time`, and `strings` (used by sibling tests) — no import edits needed. If a `strings` import is somehow missing, add it.

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=pkg/model`
Expected: FAIL — `undefined: model.MessageReadEvent` and `undefined: model.RoomEventMessageRead`.

- [ ] **Step 3: Add the constant**

In `pkg/model/event.go`, add to the `RoomEventType` const block (after `RoomEventMessageReacted` at line 209):

```go
	RoomEventMessageRead     RoomEventType = "message_read"
```

- [ ] **Step 4: Add the struct**

In `pkg/model/event.go`, immediately after the `RoomEvent` struct (after line 235), add:

```go
// MessageReadEvent is the live event published when a room's read floor
// (minUserLastSeenAt) advances after a member marks the room read. Channel
// rooms receive it once on the room event subject; DM rooms receive it per
// member on their user event subject. MinUserLastSeenAt is nil when any member
// is still fully unread (the floor cannot be established), in which case the
// field is omitted.
type MessageReadEvent struct {
	Type              RoomEventType `json:"type" bson:"type"`
	RoomID            string        `json:"roomId" bson:"roomId"`
	MinUserLastSeenAt *time.Time    `json:"minUserLastSeenAt,omitempty" bson:"minUserLastSeenAt,omitempty"`
	Timestamp         int64         `json:"timestamp" bson:"timestamp"`
}
```

- [ ] **Step 5: Format (the new constant shifts const-block alignment)**

Run: `make fmt`
Expected: `pkg/model/event.go` reformatted so the `RoomEventType` const block's `=` signs realign.

- [ ] **Step 6: Run test to verify it passes**

Run: `make test SERVICE=pkg/model`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add pkg/model/event.go pkg/model/model_test.go
git commit -m "feat(model): add MessageReadEvent for read-floor fan-out"
```

---

## Task 2: Handler — fan-out in `messageRead`

**Files:**
- Modify: `room-service/handler.go` — `messageRead` `!sameFloor` block (lines 1230-1234); add three methods after `messageRead` (~line 1237)
- Test: `room-service/handler_test.go` — extend `newMessageReadFixture` (lines 2613-2637); add tests

### Part A — extend the test fixture (test scaffolding)

- [ ] **Step 1: Add a `publishCore` capture to the fixture**

Replace the `messageReadFixture` struct (lines 2613-2620) and `newMessageReadFixture` (lines 2622-2637) in `room-service/handler_test.go` with:

```go
type messageReadFixture struct {
	store          *MockRoomStore
	publishedSubj  string
	publishedData  []byte
	publishCallErr error
	publishCalls   int
	coreSubjects   []string
	coreData       [][]byte
	coreErr        error
	handler        *Handler
}

func newMessageReadFixture(t *testing.T) *messageReadFixture {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	f := &messageReadFixture{store: store}
	f.handler = &Handler{
		store:  store,
		siteID: "site-a",
		publishToStream: func(_ context.Context, subj string, data []byte, _ string) error {
			f.publishCalls++
			f.publishedSubj = subj
			f.publishedData = data
			return f.publishCallErr
		},
		publishCore: func(_ context.Context, subj string, data []byte) error {
			f.coreSubjects = append(f.coreSubjects, subj)
			f.coreData = append(f.coreData, data)
			return f.coreErr
		},
	}
	return f
}
```

This is additive — existing `messageRead` tests don't set `Room.Type`, so the new dispatch never fires and `coreSubjects` stays empty for them.

- [ ] **Step 2: Verify existing tests still pass after the fixture change**

Run: `make test SERVICE=room-service`
Expected: PASS — the fixture change is backward-compatible (no existing test triggers the dispatch yet).

- [ ] **Step 3: Commit the scaffolding**

```bash
git add room-service/handler_test.go
git commit -m "test(room-service): add publishCore capture to messageRead fixture"
```

### Part B — fan-out tests (Red)

- [ ] **Step 4: Write the failing fan-out tests**

Append to `room-service/handler_test.go`:

```go
func TestHandler_MessageRead_ChannelFloorMoves_PublishesRoomEvent(t *testing.T) {
	f := newMessageReadFixture(t)
	joined := time.Now().UTC().Add(-2 * time.Hour)
	lastSeen := joined.Add(time.Hour)
	lastMsg := lastSeen.Add(30 * time.Minute)

	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User:   model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", SiteID: "site-a", JoinedAt: joined, LastSeenAt: &lastSeen,
	}, nil)
	f.store.EXPECT().UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any(), false).Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel, LastMsgAt: &lastMsg}, nil)
	minT := lastSeen
	f.store.EXPECT().MinSubscriptionLastSeenByRoomID(gomock.Any(), "r1").Return(&minT, nil)
	f.store.EXPECT().UpdateRoomMinUserLastSeenAt(gomock.Any(), "r1", &minT).Return(nil)

	resp, err := f.handler.messageRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.NoError(t, err)
	assert.Equal(t, "accepted", resp.Status)

	require.Len(t, f.coreSubjects, 1)
	assert.Equal(t, subject.RoomEvent("r1"), f.coreSubjects[0])
	var evt model.MessageReadEvent
	require.NoError(t, json.Unmarshal(f.coreData[0], &evt))
	assert.Equal(t, model.RoomEventMessageRead, evt.Type)
	assert.Equal(t, "r1", evt.RoomID)
	require.NotNil(t, evt.MinUserLastSeenAt)
	assert.True(t, evt.MinUserLastSeenAt.Equal(minT))
	assert.NotZero(t, evt.Timestamp)
}

func TestHandler_MessageRead_DMFloorMoves_PublishesPerSubscriber(t *testing.T) {
	f := newMessageReadFixture(t)
	joined := time.Now().UTC().Add(-2 * time.Hour)
	lastSeen := joined.Add(time.Hour)
	lastMsg := lastSeen.Add(30 * time.Minute)

	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User:   model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", SiteID: "site-a", JoinedAt: joined, LastSeenAt: &lastSeen,
	}, nil)
	f.store.EXPECT().UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any(), false).Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeDM, LastMsgAt: &lastMsg}, nil)
	minT := lastSeen
	f.store.EXPECT().MinSubscriptionLastSeenByRoomID(gomock.Any(), "r1").Return(&minT, nil)
	f.store.EXPECT().UpdateRoomMinUserLastSeenAt(gomock.Any(), "r1", &minT).Return(nil)
	f.store.EXPECT().ListSubscriptionsByRoom(gomock.Any(), "r1").Return([]model.Subscription{
		{User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1"},
		{User: model.SubscriptionUser{ID: "u2", Account: "bob"}, RoomID: "r1"},
	}, nil)

	_, err := f.handler.messageRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.NoError(t, err)

	require.Len(t, f.coreSubjects, 2)
	assert.Equal(t, subject.UserRoomEvent("alice"), f.coreSubjects[0])
	assert.Equal(t, subject.UserRoomEvent("bob"), f.coreSubjects[1])
	var evt model.MessageReadEvent
	require.NoError(t, json.Unmarshal(f.coreData[1], &evt))
	assert.Equal(t, model.RoomEventMessageRead, evt.Type)
	assert.Equal(t, "r1", evt.RoomID)
}

func TestHandler_MessageRead_ChannelNilFloor_OmitsFloorField(t *testing.T) {
	f := newMessageReadFixture(t)
	joined := time.Now().UTC().Add(-2 * time.Hour)
	lastSeen := joined.Add(time.Hour)
	lastMsg := lastSeen.Add(30 * time.Minute)
	storedFloor := lastSeen // room currently has a non-nil floor

	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User:   model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", SiteID: "site-a", JoinedAt: joined, LastSeenAt: &lastSeen,
	}, nil)
	f.store.EXPECT().UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any(), false).Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{
		ID: "r1", Type: model.RoomTypeChannel, LastMsgAt: &lastMsg, MinUserLastSeenAt: &storedFloor,
	}, nil)
	var nilFloor *time.Time
	f.store.EXPECT().MinSubscriptionLastSeenByRoomID(gomock.Any(), "r1").Return(nilFloor, nil)
	f.store.EXPECT().UpdateRoomMinUserLastSeenAt(gomock.Any(), "r1", nilFloor).Return(nil)

	_, err := f.handler.messageRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.NoError(t, err)

	require.Len(t, f.coreSubjects, 1)
	assert.NotContains(t, string(f.coreData[0]), "minUserLastSeenAt")
}

func TestHandler_MessageRead_ChannelPublishError_StillAccepted(t *testing.T) {
	f := newMessageReadFixture(t)
	f.coreErr = fmt.Errorf("nats down")
	joined := time.Now().UTC().Add(-2 * time.Hour)
	lastSeen := joined.Add(time.Hour)
	lastMsg := lastSeen.Add(30 * time.Minute)

	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User:   model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", SiteID: "site-a", JoinedAt: joined, LastSeenAt: &lastSeen,
	}, nil)
	f.store.EXPECT().UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any(), false).Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel, LastMsgAt: &lastMsg}, nil)
	minT := lastSeen
	f.store.EXPECT().MinSubscriptionLastSeenByRoomID(gomock.Any(), "r1").Return(&minT, nil)
	f.store.EXPECT().UpdateRoomMinUserLastSeenAt(gomock.Any(), "r1", &minT).Return(nil)

	resp, err := f.handler.messageRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.NoError(t, err, "fan-out publish failure must not fail the RPC")
	assert.Equal(t, "accepted", resp.Status)
}

func TestHandler_MessageRead_DMListSubscriptionsError_StillAccepted(t *testing.T) {
	f := newMessageReadFixture(t)
	joined := time.Now().UTC().Add(-2 * time.Hour)
	lastSeen := joined.Add(time.Hour)
	lastMsg := lastSeen.Add(30 * time.Minute)

	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User:   model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", SiteID: "site-a", JoinedAt: joined, LastSeenAt: &lastSeen,
	}, nil)
	f.store.EXPECT().UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any(), false).Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeDM, LastMsgAt: &lastMsg}, nil)
	minT := lastSeen
	f.store.EXPECT().MinSubscriptionLastSeenByRoomID(gomock.Any(), "r1").Return(&minT, nil)
	f.store.EXPECT().UpdateRoomMinUserLastSeenAt(gomock.Any(), "r1", &minT).Return(nil)
	f.store.EXPECT().ListSubscriptionsByRoom(gomock.Any(), "r1").Return(nil, fmt.Errorf("mongo down"))

	resp, err := f.handler.messageRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.NoError(t, err, "DM subscription-list failure must not fail the RPC")
	assert.Equal(t, "accepted", resp.Status)
	assert.Empty(t, f.coreSubjects, "no events published when subscription list fails")
}
```

Note: `handler_test.go` already imports `fmt`, `encoding/json`, `time`, `testify/assert`, `testify/require`, `gomock`, the `subject` package, and `model` (all used by sibling tests). No import edits needed.

- [ ] **Step 5: Run tests to verify they fail**

Run: `make test SERVICE=room-service`
Expected: FAIL — the five new tests fail because `messageRead` does not yet dispatch to any fan-out (`f.coreSubjects` is empty; the DM test's `ListSubscriptionsByRoom` expectation is unmet).

### Part C — implementation (Green)

- [ ] **Step 6: Wire the dispatch into `messageRead`**

In `room-service/handler.go`, replace the `!sameFloor` block (lines 1230-1234):

```go
	if !sameFloor(minTime, room.MinUserLastSeenAt) {
		if err := h.store.UpdateRoomMinUserLastSeenAt(ctx, roomID, minTime); err != nil {
			return nil, fmt.Errorf("update room minUserLastSeenAt: %w", err)
		}
	}
```

with:

```go
	if !sameFloor(minTime, room.MinUserLastSeenAt) {
		if err := h.store.UpdateRoomMinUserLastSeenAt(ctx, roomID, minTime); err != nil {
			return nil, fmt.Errorf("update room minUserLastSeenAt: %w", err)
		}
		// Fan out the read-floor advance to clients. Best-effort: the floor write
		// above is the source of truth; a publish failure must not fail the RPC.
		switch room.Type {
		case model.RoomTypeChannel:
			h.publishChannelEvent(ctx, roomID, minTime)
		case model.RoomTypeDM:
			h.publishDMEvents(ctx, roomID, minTime)
		}
	}
```

- [ ] **Step 7: Add the fan-out methods**

In `room-service/handler.go`, immediately after the `messageRead` method (after line 1237, before `messageReadReceipt`), add:

```go
// buildMessageReadEvent constructs the wire payload announcing that a room's
// read floor advanced to floor (nil when no floor can be established).
func (h *Handler) buildMessageReadEvent(roomID string, floor *time.Time) model.MessageReadEvent {
	return model.MessageReadEvent{
		Type:              model.RoomEventMessageRead,
		RoomID:            roomID,
		MinUserLastSeenAt: floor,
		Timestamp:         time.Now().UTC().UnixMilli(),
	}
}

// publishChannelEvent fans a read-floor advance out once to the channel's shared
// room event subject. Best-effort: a marshal or publish failure is logged, not
// returned. Used for RoomTypeChannel.
func (h *Handler) publishChannelEvent(ctx context.Context, roomID string, floor *time.Time) {
	evt := h.buildMessageReadEvent(roomID, floor)
	payload, err := json.Marshal(evt)
	if err != nil {
		slog.Error("marshal message_read channel event failed", "error", err, "roomId", roomID)
		return
	}
	if err := h.publishCore(ctx, subject.RoomEvent(roomID), payload); err != nil {
		slog.Error("publish message_read channel event failed", "error", err, "roomId", roomID)
	}
}

// publishDMEvents fans a read-floor advance out to each DM member on their
// per-user event subject. Mirrors broadcast-worker's publishDMEvents: it lists
// the room's subscriptions and publishes once per subscriber. Best-effort per
// account; a list, marshal, or publish failure is logged, not returned. Used
// for RoomTypeDM.
func (h *Handler) publishDMEvents(ctx context.Context, roomID string, floor *time.Time) {
	subs, err := h.store.ListSubscriptionsByRoom(ctx, roomID)
	if err != nil {
		slog.Error("list subscriptions for message_read DM fan-out failed", "error", err, "roomId", roomID)
		return
	}
	evt := h.buildMessageReadEvent(roomID, floor)
	payload, err := json.Marshal(evt)
	if err != nil {
		slog.Error("marshal message_read DM event failed", "error", err, "roomId", roomID)
		return
	}
	for i := range subs {
		account := subs[i].User.Account
		if err := h.publishCore(ctx, subject.UserRoomEvent(account), payload); err != nil {
			slog.Error("publish message_read DM event failed", "error", err, "roomId", roomID, "account", account)
		}
	}
}
```

- [ ] **Step 8: Run tests to verify they pass**

Run: `make test SERVICE=room-service`
Expected: PASS — all five new tests plus the existing `messageRead` suite are green.

- [ ] **Step 9: Lint**

Run: `make lint`
Expected: no new findings.

- [ ] **Step 10: Commit**

```bash
git add room-service/handler.go room-service/handler_test.go
git commit -m "feat(room-service): fan out message_read event when read floor advances"
```

---

## Task 3: Docs — `docs/client-api.md`

**Files:**
- Modify: `docs/client-api.md` — Mark Messages Read section (lines 1143-1151)

- [ ] **Step 1: Replace the "no fan-out" note and the Triggered-events block**

In `docs/client-api.md`, replace line 1143:

```markdown
- **No system message, no fan-out events:** read receipts are silent; only the requester receives the `accepted` reply.
```

with:

```markdown
- **Read-floor fan-out:** when (and only when) the recompute above changes `Room.MinUserLastSeenAt`, the server publishes a `message_read` room event carrying the new floor, so peers can advance read-receipt / unread UI live. Fan-out is best-effort (a publish failure does not fail the RPC) and never fires on the early-return paths or when the floor is unchanged. No system message is written.
```

Then replace the success-path Triggered-events block (lines 1145-1147):

```markdown
##### Triggered events — success path

`None — reply only.`
```

with:

```markdown
##### Triggered events — success path

Emitted **only when the room read floor (`Room.MinUserLastSeenAt`) changes** (best-effort, core NATS):

- **Channel rooms — `chat.room.{roomID}.event`** — a single `message_read` event to every client subscribed to the room.
- **DM rooms — `chat.user.{account}.event.room`** — one `message_read` event per subscriber.

| Field | Type | Notes |
|---|---|---|
| `type` | string | Always `"message_read"`. |
| `roomId` | string | The room whose floor advanced. |
| `minUserLastSeenAt` | string | Optional. RFC3339 UTC timestamp of the new read floor. **Omitted** when the floor is null (a member is still fully unread). |
| `timestamp` | number | Event publish time, UTC milliseconds since Unix epoch. |

```json
{
  "type": "message_read",
  "roomId": "Rb3kQ2",
  "minUserLastSeenAt": "2026-06-09T10:30:00Z",
  "timestamp": 1749465000123
}
```
```

- [ ] **Step 2: Verify the doc renders consistently**

Run: `grep -n "message_read" docs/client-api.md`
Expected: matches in the Mark Messages Read section (new event table + JSON example).

- [ ] **Step 3: Commit**

```bash
git add docs/client-api.md
git commit -m "docs(client-api): document message_read read-floor event"
```

---

## Final verification

- [ ] **Step 1: Full unit suite with race detector**

Run: `make test`
Expected: PASS across all packages.

- [ ] **Step 2: Lint + SAST**

Run: `make lint && make sast`
Expected: no new findings (the change adds no `InsecureSkipVerify`, conversions, or raw token handling).

- [ ] **Step 3: Push**

```bash
git push -u origin claude/focused-hamilton-d9s5nb
```

---

## Notes for the implementer

- **Best-effort means swallow-and-log.** `publishChannelEvent` / `publishDMEvents` return nothing. Never propagate their failures into the RPC reply — the floor write is already durable and the read succeeded.
- **The event carries `minTime` (the new floor), not the prior `room.MinUserLastSeenAt`.** They differ exactly when `sameFloor` is false, which is the only path that publishes.
- **`RoomTypeBotDM` is intentionally not in the switch** — its floor is always nil, and the task scopes DM only. The `default` (no case) correctly skips it.
- **No mock regeneration.** `ListSubscriptionsByRoom` is already on `RoomStore` and in `mock_store_test.go`.
- **Existing `messageRead` tests stay green** because they don't set `Room.Type`; the new `switch` matches no case for them.
