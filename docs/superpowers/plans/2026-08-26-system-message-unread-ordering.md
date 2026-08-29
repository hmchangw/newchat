# System-Message Unread & Ordering Exclusion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** System messages stop counting as unread and stop re-sorting the sidebar, while a newly added member still sees the room as unread and system messages stay visible in the timeline.

**Architecture:** Add `rooms.lastUserMsgAt` (user messages only, written by broadcast-worker beside `lastMsgAt`, with a one-time sticky freeze on system-only writes). Unread compares `lastUserMsgAt ?? lastMsgAt`; sort uses `(lastUserMsgAt ?? lastMsgAt) ?? createdAt`. `lastMsgAt` keeps full-activity semantics for history's read ceiling. A plaintext `systemMsg` flag on `RoomEvent` lets the client gate encrypted channels.

**Tech Stack:** Go 1.25, MongoDB driver v2, testify + gomock, vitest (frontend).

**Spec:** `docs/superpowers/specs/2026-08-26-system-message-unread-ordering-design.md`

## Global Constraints

- All Go commands via Makefile: `make test SERVICE=<name>`, `make lint`, `make generate` — never raw `go` commands.
- TDD: every step below runs the failing test before the implementation.
- Unread rule everywhere: compare `lastSeenAt` against `lastUserMsgAt ?? lastMsgAt` — **never** fall back to `createdAt` for unread.
- Sort rule everywhere: `(lastUserMsgAt ?? lastMsgAt) ?? createdAt`.
- Never mention model identifiers or AI provenance in commits/comments; commit messages explain the why.
- Integration tests (`make test-integration SERVICE=<name>`) require Docker; if the environment has no Docker, note it in the final report and rely on unit tests + CI.
- Frontend gates: `npm run typecheck` and `npm test` (run from `chat-frontend/`) must pass on every frontend commit.

---

### Task 1: Model fields

**Files:**
- Modify: `pkg/model/room.go` (Room ~line 21, RoomInfo ~line 99)
- Modify: `pkg/model/subscription.go` (EnrichedSubscription ~line 106, SubscriptionRoom ~line 141)
- Modify: `pkg/model/event.go` (RoomEvent ~line 375)
- Modify: `user-service/models/subscription.go` (ActiveSubscription ~line 84)
- Test: `pkg/model/room_lastusermsg_test.go` (new)

**Interfaces (Produces — later tasks rely on these exact names):**
- `model.Room.LastUserMsgAt *time.Time` — json+bson `lastUserMsgAt,omitempty`
- `model.RoomInfo.LastUserMsgAt *int64` — json `lastUserMsgAt,omitempty`
- `model.EnrichedSubscription.LastUserMsgAt *time.Time` — json `-`, bson `lastUserMsgAt,omitempty`
- `model.SubscriptionRoom.LastUserMsgAt *time.Time` — json `lastUserMsgAt,omitempty`, bson `-`
- `model.RoomEvent.SystemMsg bool` — json `systemMsg,omitempty`
- `models.ActiveSubscription.LastUserMsgAt *time.Time` — json+bson `lastUserMsgAt,omitempty`

- [ ] **Step 1: Write the failing test**

Create `pkg/model/room_lastusermsg_test.go`:

```go
package model_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

// lastUserMsgAt is the user-activity position: present on the room doc, the
// enriched wire room object, and the cross-site RoomInfo; absent values stay
// off the wire (omitempty) so pre-deploy payloads are byte-identical.
func TestLastUserMsgAt_JSONTags(t *testing.T) {
	at := time.Date(2026, 8, 26, 10, 0, 0, 0, time.UTC)

	roomJSON, err := json.Marshal(model.Room{ID: "r1", LastUserMsgAt: &at})
	require.NoError(t, err)
	assert.Contains(t, string(roomJSON), `"lastUserMsgAt":"2026-08-26T10:00:00Z"`)

	ms := at.UnixMilli()
	infoJSON, err := json.Marshal(model.RoomInfo{RoomID: "r1", Found: true, LastUserMsgAt: &ms})
	require.NoError(t, err)
	assert.Contains(t, string(infoJSON), `"lastUserMsgAt":1787738400000`)

	subRoomJSON, err := json.Marshal(model.SubscriptionRoom{LastUserMsgAt: &at})
	require.NoError(t, err)
	assert.Contains(t, string(subRoomJSON), `"lastUserMsgAt"`)

	emptyJSON, err := json.Marshal(model.SubscriptionRoom{})
	require.NoError(t, err)
	assert.NotContains(t, string(emptyJSON), "lastUserMsgAt", "omitempty must drop the absent value")
}

// systemMsg is the plaintext marker that lets clients recognize a system
// new_message even when the message body is sealed in encryptedMessage.
func TestRoomEvent_SystemMsgTag(t *testing.T) {
	evtJSON, err := json.Marshal(model.RoomEvent{SystemMsg: true})
	require.NoError(t, err)
	assert.Contains(t, string(evtJSON), `"systemMsg":true`)

	plainJSON, err := json.Marshal(model.RoomEvent{})
	require.NoError(t, err)
	assert.NotContains(t, string(plainJSON), "systemMsg", "omitempty must drop false")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=pkg/model`
Expected: FAIL — `unknown field LastUserMsgAt` / `unknown field SystemMsg` (compile errors).

- [ ] **Step 3: Add the fields**

`pkg/model/room.go` — in `Room`, directly after `LastMsgID`:

```go
	// LastUserMsgAt is the newest NON-system message time — the only input to
	// unread and sidebar ordering. LastMsgAt (all messages, system included)
	// stays the history read ceiling. Absent on rooms untouched since the field
	// shipped; readers fall back to LastMsgAt. See
	// docs/superpowers/specs/2026-08-26-system-message-unread-ordering-design.md.
	LastUserMsgAt *time.Time `json:"lastUserMsgAt,omitempty" bson:"lastUserMsgAt,omitempty"`
```

`pkg/model/room.go` — in `RoomInfo`, after `LastMsgID`:

```go
	// LastUserMsgAt mirrors Room.LastUserMsgAt (epoch ms); nil when absent.
	LastUserMsgAt *int64 `json:"lastUserMsgAt,omitempty"`
```

`pkg/model/subscription.go` — in `EnrichedSubscription`, after `LastMsgID`:

```go
	LastUserMsgAt *time.Time `json:"-" bson:"lastUserMsgAt,omitempty"`
```

`pkg/model/subscription.go` — in `SubscriptionRoom`, after `LastMsgID`:

```go
	// LastUserMsgAt is the last NON-system message time — what hasUnread and
	// sidebar ordering are computed from; nil for rooms with no user messages
	// since the field shipped (clients fall back to LastMsgAt).
	LastUserMsgAt *time.Time `json:"lastUserMsgAt,omitempty" bson:"-"`
```

`pkg/model/event.go` — in `RoomEvent`, after `HasMention`:

```go
	// SystemMsg marks a system-message new_message (IsSystemMessageType) in
	// plaintext, so clients can exclude it from unread/ordering even when the
	// message itself is sealed inside EncryptedMessage.
	SystemMsg bool `json:"systemMsg,omitempty"`
```

`user-service/models/subscription.go` — in `ActiveSubscription`, after `LastMsgAt`:

```go
	// Joined from the room like LastMsgAt; the unread reference is
	// LastUserMsgAt ?? LastMsgAt.
	LastUserMsgAt *time.Time `json:"lastUserMsgAt,omitempty" bson:"lastUserMsgAt,omitempty"`
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=pkg/model && make test SERVICE=user-service`
Expected: PASS (user-service run proves no fixture broke).

- [ ] **Step 5: Commit**

```bash
git add pkg/model/room.go pkg/model/subscription.go pkg/model/event.go user-service/models/subscription.go pkg/model/room_lastusermsg_test.go
git commit -m "feat(model): add lastUserMsgAt fields and RoomEvent.systemMsg marker"
```

---

### Task 2: broadcast-worker coalescer + store write

**Files:**
- Modify: `broadcast-worker/coalescer.go` (`roomLastMessage` ~line 14, `roomLastMsgUpdate` ~line 29, `UpdateRoomLastMessage` ~line 139)
- Modify: `broadcast-worker/store_mongo.go` (`asBuffered` ~line 98, `lastMessageUpdate` ~line 139)
- Test: `broadcast-worker/lastusermsg_test.go` (new)
- Modify test: `broadcast-worker/preview_test.go` (~line 253-266, the previews-off plain-`$set` assertion — see Step 3)

**Interfaces:**
- Consumes: Task 1 model fields.
- Produces: `roomLastMessage.SystemMsg bool`; `roomLastMsgUpdate.userAt time.Time` / `.userMsgID string` (zero `userAt` ⇒ system-only window ⇒ freeze write).

- [ ] **Step 1: Write the failing tests**

Create `broadcast-worker/lastusermsg_test.go`:

```go
package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// A mixed flush window: lastMsgAt follows the newest message of any kind,
// lastUserMsgAt follows the newest USER message only.
func TestCoalescer_SystemMessageDoesNotAdvanceUserAt(t *testing.T) {
	c := newCoalescingStore(nil, &fakeBulkWriter{})
	t0 := time.Date(2026, 8, 26, 10, 0, 0, 0, time.UTC)

	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), roomLastMessage{
		RoomID: "r1", MsgID: "m-user", At: t0,
	}))
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), roomLastMessage{
		RoomID: "r1", MsgID: "m-system", At: t0.Add(time.Minute), SystemMsg: true,
	}))

	u := c.pending["r1"]
	assert.Equal(t, "m-system", u.msgID, "lastMsgAt key follows the newest message, system included")
	assert.True(t, u.at.Equal(t0.Add(time.Minute)))
	assert.Equal(t, "m-user", u.userMsgID, "user key must ignore the system message")
	assert.True(t, u.userAt.Equal(t0), "userAt pinned to the newest USER message")
}

func TestCoalescer_SystemOnlyWindowLeavesUserAtZero(t *testing.T) {
	c := newCoalescingStore(nil, &fakeBulkWriter{})
	t0 := time.Date(2026, 8, 26, 10, 0, 0, 0, time.UTC)
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), roomLastMessage{
		RoomID: "r1", MsgID: "m-system", At: t0, SystemMsg: true,
	}))
	assert.True(t, c.pending["r1"].userAt.IsZero(), "system-only window must signal the freeze path")
}

// User-message window, previews off: the plain $set now carries lastUserMsgAt
// beside lastMsgAt.
func TestLastMessageUpdate_UserMessageSetsLastUserMsgAt(t *testing.T) {
	m := &mongoStore{previews: false}
	at := time.Date(2026, 8, 26, 10, 0, 0, 0, time.UTC)
	got := m.lastMessageUpdate(&roomLastMsgUpdate{msgID: "m1", at: at, userAt: at, userMsgID: "m1"})
	set, ok := got.(bson.M)
	require.True(t, ok, "a user-message window with previews off stays a plain $set")
	fields := set["$set"].(bson.M)
	assert.Equal(t, at, fields["lastUserMsgAt"])
	assert.Equal(t, at, fields["lastMsgAt"])
}

// System-only window: lastUserMsgAt freezes once to the room's pre-system
// position — existing lastUserMsgAt, else the pre-update lastMsgAt, else the
// room's createdAt (brand-new room), else stays absent. Requires the pipeline
// form so the expression reads the PRE-update document, previews on or off.
func TestLastMessageUpdate_SystemOnlyWindowFreezes(t *testing.T) {
	for _, previews := range []bool{false, true} {
		m := &mongoStore{previews: previews}
		at := time.Date(2026, 8, 26, 10, 0, 0, 0, time.UTC)
		got := m.lastMessageUpdate(&roomLastMsgUpdate{msgID: "m-sys", at: at})
		pipe, ok := got.(mongo.Pipeline)
		require.True(t, ok, "system-only window must use a pipeline $set (previews=%v)", previews)
		fields := pipe[0][0].Value.(bson.M)
		assert.Equal(t, bson.M{"$ifNull": bson.A{
			"$lastUserMsgAt",
			bson.M{"$ifNull": bson.A{"$lastMsgAt", bson.M{"$ifNull": bson.A{"$createdAt", "$$REMOVE"}}}},
		}}, fields["lastUserMsgAt"], "previews=%v", previews)
		assert.Equal(t, at, fields["lastMsgAt"], "system message still advances the history ceiling (previews=%v)", previews)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=broadcast-worker`
Expected: FAIL — `unknown field SystemMsg` / `unknown field userAt` (compile errors).

- [ ] **Step 3: Implement**

`broadcast-worker/coalescer.go` — add to `roomLastMessage` after `At`:

```go
	// SystemMsg marks a system message (model.IsSystemMessageType): it advances
	// lastMsgAt (the history ceiling) but never lastUserMsgAt — a system-only
	// flush window freezes lastUserMsgAt instead (see lastMessageUpdate).
	SystemMsg bool
```

Add to `roomLastMsgUpdate` after `at`:

```go
	// userAt/userMsgID track the newest NON-system message in the window, under
	// the same newerRow comparator as msgID/at. Zero userAt = system-only
	// window: the write freezes lastUserMsgAt instead of setting it.
	userAt    time.Time
	userMsgID string
```

In `UpdateRoomLastMessage`, after the existing `if newerRow(upd.At, upd.MsgID, cur.at, cur.msgID) {...}` block:

```go
	if !upd.SystemMsg && newerRow(upd.At, upd.MsgID, cur.userAt, cur.userMsgID) {
		cur.userAt = upd.At
		cur.userMsgID = upd.MsgID
	}
```

`broadcast-worker/store_mongo.go` — `asBuffered` must carry the new fields (its doc comment says every field must be carried):

```go
func asBuffered(upd roomLastMessage) roomLastMsgUpdate {
	b := roomLastMsgUpdate{
		msgID:            upd.MsgID,
		at:               upd.At,
		lastMentionAllAt: mentionAllAt(upd),
		pvw:              upd.Preview,
		pvwFailed:        upd.PreviewFailed,
		pvwAt:            upd.At,
	}
	if !upd.SystemMsg {
		b.userAt = upd.At
		b.userMsgID = upd.MsgID
	}
	return b
}
```

`lastMessageUpdate` — replace the head of the function up to (not including) the `asOf := u.at.UnixMilli()` line with:

```go
func (m *mongoStore) lastMessageUpdate(u *roomLastMsgUpdate) any {
	fields := bson.M{
		"lastMsgAt": u.at,
		"lastMsgId": u.msgID,
		"updatedAt": u.at,
	}
	if !u.lastMentionAllAt.IsZero() {
		fields["lastMentionAllAt"] = u.lastMentionAllAt
	}
	systemOnly := u.userAt.IsZero()
	if !systemOnly {
		fields["lastUserMsgAt"] = u.userAt
	}
	if !m.previews && !systemOnly {
		return bson.M{"$set": fields}
	}

	// A bare "$"-prefixed string reads as a field path in a pipeline stage.
	for k, v := range fields {
		if s, ok := v.(string); ok {
			fields[k] = bson.M{"$literal": s}
		}
	}
	if systemOnly {
		// Sticky freeze: pin lastUserMsgAt ONCE to the room's pre-system
		// position. Every $set expression reads the pre-update document, so
		// "$lastMsgAt" here is the position BEFORE this write advances it; a
		// brand-new room (no lastMsgAt yet) pins to its createdAt, which is
		// what makes the room unread for members who have never opened it
		// while never re-flagging members who have.
		fields["lastUserMsgAt"] = bson.M{"$ifNull": bson.A{
			"$lastUserMsgAt",
			bson.M{"$ifNull": bson.A{"$lastMsgAt", bson.M{"$ifNull": bson.A{"$createdAt", "$$REMOVE"}}}},
		}}
	}
	if !m.previews {
		return mongo.Pipeline{{{Key: "$set", Value: fields}}}
	}
```

…then the existing preview logic (`asOf := u.at.UnixMilli()` onward) continues unchanged.

Also update the existing previews-off assertion in `broadcast-worker/preview_test.go` (~line 253-266, test containing "with previews off the update must stay the plain $set it has always been"): that fixture passes a user-message update, so give its `roomLastMsgUpdate` literal `userAt`/`userMsgID` matching its `at`/`msgID` values (the plain-`$set` expectation then still holds), and extend its field assertions with `assert.Equal(t, <the fixture's at value>, fields["lastUserMsgAt"])`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=broadcast-worker`
Expected: PASS (including the updated preview_test.go).

- [ ] **Step 5: Commit**

```bash
git add broadcast-worker/coalescer.go broadcast-worker/store_mongo.go broadcast-worker/lastusermsg_test.go broadcast-worker/preview_test.go
git commit -m "feat(broadcast-worker): track lastUserMsgAt in the room write; freeze on system-only windows"
```

---

### Task 3: broadcast-worker handler — SystemMsg flag + systemMsg on RoomEvent

**Files:**
- Modify: `broadcast-worker/handler.go` (`handleCreated` ~line 248, `buildRoomEvent` ~line 1198)
- Test: append to `broadcast-worker/lastusermsg_test.go`

**Interfaces:**
- Consumes: Task 2's `roomLastMessage.SystemMsg`; Task 1's `model.RoomEvent.SystemMsg`.
- Produces: every `new_message` RoomEvent for a system message carries `systemMsg: true` (all fan-out paths — `buildRoomEvent` is the single constructor).

- [ ] **Step 1: Write the failing tests**

Append to `broadcast-worker/lastusermsg_test.go` (the file already imports what these need except `model`/`roommetacache` — add `"github.com/hmchangw/chat/pkg/model"` and `"github.com/hmchangw/chat/pkg/roommetacache"` to the import block):

```go
// The single RoomEvent constructor stamps the plaintext systemMsg marker, so
// encrypted channels (message sealed, type invisible) can still gate.
func TestBuildRoomEvent_StampsSystemMsg(t *testing.T) {
	meta := roommetacache.Meta{ID: "r1", Name: "room", Type: model.RoomTypeChannel}
	sys := buildClientMessage(&model.Message{ID: "m1", RoomID: "r1", Type: model.MessageTypeMembersAdded}, nil)
	assert.True(t, buildRoomEvent(&meta, sys, 1).SystemMsg)

	user := buildClientMessage(&model.Message{ID: "m2", RoomID: "r1"}, nil)
	assert.False(t, buildRoomEvent(&meta, user, 1).SystemMsg)

	important := buildClientMessage(&model.Message{ID: "m3", RoomID: "r1", Type: model.MessageTypeImportant}, nil)
	assert.False(t, buildRoomEvent(&meta, important, 1).SystemMsg, "important is a client type, not system")
}

// handleCreated must flag the room-doc update for a system message so the
// store takes the freeze path instead of advancing lastUserMsgAt.
func TestHandleCreated_MarksSystemMessageUpdate(t *testing.T) {
	c := newCoalescingStore(nil, &fakeBulkWriter{})
	at := time.Date(2026, 8, 26, 10, 0, 0, 0, time.UTC)
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), roomLastMessage{
		RoomID: "r1", MsgID: "m-sys", At: at,
		SystemMsg: model.IsSystemMessageType(model.MessageTypeRoomRenamed),
	}))
	assert.True(t, c.pending["r1"].userAt.IsZero())
}
```

Additionally, locate the existing `handleCreated` unit test that exercises a full created event through a mocked store (search `broadcast-worker/handler_test.go` for `UpdateRoomLastMessage` expectations, e.g. via `gomock`): add one table case or sibling test in `handler_test.go` publishing a message with `Type: model.MessageTypeMembersAdded` and assert the captured `roomLastMessage.SystemMsg == true` (use a `gomock.Any()` matcher replaced by a `DoAndReturn` capture, mirroring the neighboring tests' style).

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=broadcast-worker`
Expected: FAIL — `unknown field SystemMsg in struct literal of type model.RoomEvent`? No — Task 1 added it; the failures are `buildRoomEvent(...).SystemMsg` always false and the captured `roomLastMessage.SystemMsg` false.

- [ ] **Step 3: Implement**

`broadcast-worker/handler.go`, `handleCreated` — in the `UpdateRoomLastMessage` call (~line 248), add the flag:

```go
	if err := h.store.UpdateRoomLastMessage(ctx, roomLastMessage{
		RoomID:        msg.RoomID,
		MsgID:         msg.ID,
		At:            msg.CreatedAt,
		SystemMsg:     model.IsSystemMessageType(msg.Type),
		MentionAll:    resolved.MentionAll,
		Preview:       sealed,
		PreviewFailed: sealFailed,
	}); err != nil {
```

`buildRoomEvent` (~line 1198) — add one field to the literal:

```go
		LastMsgID:      clientMsg.ID,
		SystemMsg:      model.IsSystemMessageType(clientMsg.Type),
		Message:        clientMsg,
```

(`clientMsg` embeds `model.Message`, so `clientMsg.Type` is the message type. Thread replies never carry a system type, so the thread path is a no-op.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=broadcast-worker`
Expected: PASS.

- [ ] **Step 5: Integration test (requires Docker)**

Append to `broadcast-worker/integration_test.go`, following the style of the existing "Verify the room doc now has lastMsgAt/lastMsgId persisted" test (~line 359): publish a normal message, flush, then a `members_added` system message, flush, and decode:

```go
	var got struct {
		LastMsgAt     time.Time  `bson:"lastMsgAt"`
		LastMsgID     string     `bson:"lastMsgId"`
		LastUserMsgAt *time.Time `bson:"lastUserMsgAt"`
	}
```

Assert: after the user message both fields equal the user message time; after the system message `LastMsgAt` advanced to the system time and `LastMsgID` to the system id, while `LastUserMsgAt` still equals the user message time (the freeze kept the pre-existing value). Add a second scenario against a fresh room doc that has `createdAt` but no `lastMsgAt`: one system message ⇒ `LastUserMsgAt` equals the room's `createdAt`.

Run: `make test-integration SERVICE=broadcast-worker`
Expected: PASS (skip with a note if Docker is unavailable).

- [ ] **Step 6: Commit**

```bash
git add broadcast-worker/handler.go broadcast-worker/handler_test.go broadcast-worker/lastusermsg_test.go broadcast-worker/integration_test.go
git commit -m "feat(broadcast-worker): flag system messages in the room write and stamp systemMsg on room events"
```

---

### Task 4: user-service repo layer — projections, sort key, activity window

**Files:**
- Modify: `user-service/mongorepo/subscriptions.go` (`roomsEnrichStages` ~line 100/119, `buildListRows` ~line 371, `resolveSortKeys` ~line 476, `roomBaseline` ~line 527, `roomBaselineProjection` ~line 546, `enrichListRows` ~line 587/620, `activeSubscriptionProjection` ~line 773)
- Modify: `user-service/mongorepo/sortkeycache.go` (`roomSortKey` ~line 17)
- Test: `user-service/mongorepo/lastusermsg_test.go` (new)
- Modify tests: any projection-guard tests that enumerate fields (`TestRoomBaselineProjection_MatchesStructTags`, and the active-projection guards in `subscriptions_activeproj_test.go`) — extend expectations with `lastUserMsgAt`.

**Interfaces:**
- Consumes: Task 1 model fields.
- Produces: `roomSortKey.LastUserMsgAt *time.Time`; unexported helper `effectiveUserAt(k roomSortKey) *time.Time`; `EnrichedSubscription.LastUserMsgAt` populated by both the aggregation and `enrichListRows`; `ActiveSubscription.LastUserMsgAt` populated by `GetActiveSubscriptions`.

- [ ] **Step 1: Write the failing unit test**

Create `user-service/mongorepo/lastusermsg_test.go`:

```go
package mongorepo

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Ordering and the withinDays window key off the USER activity position:
// lastUserMsgAt when present, else lastMsgAt (pre-deploy rooms), else createdAt.
func TestBuildListRows_SortAtPrefersLastUserMsgAt(t *testing.T) {
	created := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	userAt := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	sysAt := time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC) // newer system bump

	rows := buildListRows(
		[]subLite{{ID: "s1", RoomID: "r1"}, {ID: "s2", RoomID: "r2"}, {ID: "s3", RoomID: "r3"}},
		map[string]roomSortKey{
			"r1": {LastUserMsgAt: &userAt, LastMsgAt: &sysAt, CreatedAt: &created},
			"r2": {LastMsgAt: &sysAt, CreatedAt: &created}, // pre-deploy room: falls back to lastMsgAt
			"r3": {CreatedAt: &created},                    // no messages at all
		},
		"alice", false, nil,
	)
	require.Len(t, rows, 3)
	byRoom := map[string]*time.Time{}
	for _, r := range rows {
		byRoom[r.sub.RoomID] = r.sortAt
	}
	assert.True(t, byRoom["r1"].Equal(userAt), "user activity outranks the newer system bump")
	assert.True(t, byRoom["r2"].Equal(sysAt), "no lastUserMsgAt yet: lastMsgAt keeps today's ordering")
	assert.True(t, byRoom["r3"].Equal(created))
}

func TestBuildListRows_WindowUsesUserActivity(t *testing.T) {
	created := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	userAt := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	sysAt := time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC)
	cutoff := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)

	rows := buildListRows(
		[]subLite{{ID: "s1", RoomID: "r1"}},
		map[string]roomSortKey{"r1": {LastUserMsgAt: &userAt, LastMsgAt: &sysAt, CreatedAt: &created}},
		"alice", false, &cutoff,
	)
	assert.Empty(t, rows, "a rename must not resurface a dormant room inside the activity window")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=user-service`
Expected: FAIL — `unknown field LastUserMsgAt in struct literal of type roomSortKey`.

- [ ] **Step 3: Implement**

`sortkeycache.go` — extend `roomSortKey`:

```go
type roomSortKey struct {
	// LastUserMsgAt is the user-activity position (non-system messages only);
	// nil on rooms untouched since the field shipped — effectiveUserAt falls
	// back to LastMsgAt so those keep today's ordering.
	LastUserMsgAt *time.Time
	LastMsgAt     *time.Time
	CreatedAt     *time.Time
	Missing       bool
}
```

`subscriptions.go` — add next to `buildListRows`:

```go
// effectiveUserAt is the ordering/window position: the last USER message,
// falling back to lastMsgAt for rooms that predate lastUserMsgAt. Never
// createdAt — that stays buildListRows' final undated fallback only.
func effectiveUserAt(k roomSortKey) *time.Time {
	if k.LastUserMsgAt != nil {
		return k.LastUserMsgAt
	}
	return k.LastMsgAt
}
```

In `buildListRows`, replace the cutoff check and `sortAt` derivation:

```go
		key := keys[subs[i].RoomID]
		at := effectiveUserAt(key)
		if cutoff != nil && (at == nil || at.Before(*cutoff)) {
			continue
		}
		sortAt := at
		if sortAt == nil {
			sortAt = key.CreatedAt
		}
```

In `resolveSortKeys`: the cached-key window re-check swaps `k.LastMsgAt` for `effectiveUserAt(k)`:

```go
		if ok && cutoff != nil && !k.Missing {
			if at := effectiveUserAt(k); at == nil || at.Before(*cutoff) {
				ok = false
			}
		}
```

…the Mongo read projects and decodes the new field:

```go
	cur, err := r.rooms.Find(ctx, bson.M{"_id": bson.M{"$in": misses}},
		options.Find().SetProjection(bson.M{"lastMsgAt": 1, "lastUserMsgAt": 1, "createdAt": 1}))
```

```go
	var docs []struct {
		ID            string     `bson:"_id"`
		LastMsgAt     *time.Time `bson:"lastMsgAt"`
		LastUserMsgAt *time.Time `bson:"lastUserMsgAt"`
		CreatedAt     *time.Time `bson:"createdAt"`
	}
```

```go
		k := roomSortKey{LastUserMsgAt: docs[i].LastUserMsgAt, LastMsgAt: docs[i].LastMsgAt, CreatedAt: docs[i].CreatedAt}
```

`roomsEnrichStages`: add `"lastUserMsgAt": 1,` to the `$lookup` `$project`, and `"lastUserMsgAt": "$room.lastUserMsgAt",` to the `$addFields`.

`activeSubscriptionProjection`: add `"lastUserMsgAt": 1,`.

`roomBaseline`: add `LastUserMsgAt *time.Time \`bson:"lastUserMsgAt"\`` after `LastMsgAt`; `roomBaselineProjection`: add `"lastUserMsgAt": 1,`.

`enrichListRows`: the sort-key cache refresh becomes

```go
		r.sortKeys.add(docs[i].ID, roomSortKey{
			LastUserMsgAt: docs[i].LastUserMsgAt, LastMsgAt: docs[i].LastMsgAt, CreatedAt: docs[i].CreatedAt,
		})
```

…and the row copy adds `es.LastUserMsgAt = b.LastUserMsgAt` beside `es.LastMsgAt = b.LastMsgAt`.

Update the projection-guard tests that now fail (they enumerate exact fields): add `lastUserMsgAt` to their expected sets.

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=user-service`
Expected: PASS.

- [ ] **Step 5: Integration check (requires Docker)**

Append to the mongorepo integration suite (same file/style as the existing `AggregateSubscriptions` ordering tests): seed two rooms — A with `lastUserMsgAt=T1, lastMsgAt=T3`, B with `lastUserMsgAt=T2, lastMsgAt=T2` where `T1 < T2 < T3` — and assert the list orders B before A (the system bump on A must not win). Run: `make test-integration SERVICE=user-service`.

- [ ] **Step 6: Commit**

```bash
git add user-service/mongorepo/
git commit -m "feat(user-service): sort and window subscriptions by user activity, not system bumps"
```

---

### Task 5: user-service service layer — unread seams

**Files:**
- Modify: `user-service/service/subscriptions.go` (`enrichLocal` ~line 256, `buildLocalRoom` ~line 590, `applyRoomInfo` ~line 618, `unreadRooms` ~line 783/833-852)
- Test: append to `user-service/service/subscriptions_test.go`

**Interfaces:**
- Consumes: Task 1 fields; Task 4's populated `EnrichedSubscription.LastUserMsgAt` / `ActiveSubscription.LastUserMsgAt`; `RoomInfo.LastUserMsgAt` (populated by Task 6 — until then nil, which the fallback handles).
- Produces: unexported helpers `coalesceTime(a, b *time.Time) *time.Time` and `coalesceMillis(a, b *int64) *int64` in `user-service/service/subscriptions.go`.

- [ ] **Step 1: Write the failing tests**

Append to `user-service/service/subscriptions_test.go` (uses the existing `newSvc`/`ctx` harness):

```go
// A system event bumps lastMsgAt but not lastUserMsgAt; a member who has read
// the room must not be counted, while a newly added member (no lastSeenAt) is.
func TestCountUnread_SystemBumpDoesNotCount(t *testing.T) {
	svc, subs, _, _, rooms, _, _ := newSvc(t)
	seen := time.UnixMilli(200).UTC()
	userAt := time.UnixMilli(100).UTC() // read
	sysAt := time.UnixMilli(300).UTC()  // newer system bump
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", gomock.Any()).
		Return([]models.ActiveSubscription{
			{RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen, LastUserMsgAt: &userAt, LastMsgAt: &sysAt},
		}, nil)
	rooms.EXPECT().GetRoomsMeta(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	yes := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &yes})
	require.NoError(t, err)
	assert.Equal(t, 0, resp.Count, "system bump past the read position must not count")
}

func TestCountUnread_NewlyAddedMemberCounts(t *testing.T) {
	svc, subs, _, _, rooms, _, _ := newSvc(t)
	frozen := time.UnixMilli(100).UTC() // freeze pinned the pre-system position
	sysAt := time.UnixMilli(300).UTC()
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", gomock.Any()).
		Return([]models.ActiveSubscription{
			{RoomID: "r1", SiteID: "site-a", LastUserMsgAt: &frozen, LastMsgAt: &sysAt}, // no LastSeenAt
		}, nil)
	rooms.EXPECT().GetRoomsMeta(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	yes := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &yes})
	require.NoError(t, err)
	assert.Equal(t, 1, resp.Count, "a never-read subscription counts whenever the room has any user-activity reference")
}

// Cross-site rooms follow the same rule via RoomInfo.lastUserMsgAt, falling
// back to lastMsgAt for peers that predate the field.
func TestCountUnread_CrossSiteSystemBumpDoesNotCount(t *testing.T) {
	svc, subs, _, _, rooms, _, _ := newSvc(t)
	seen := time.UnixMilli(200).UTC()
	userMs, sysMs := int64(100), int64(300)
	subs.EXPECT().GetActiveSubscriptions(gomock.Any(), "alice", gomock.Any()).Return([]models.ActiveSubscription{
		{RoomID: "rb1", SiteID: "site-b", LastSeenAt: &seen},
		{RoomID: "rb2", SiteID: "site-b", LastSeenAt: &seen},
	}, nil)
	rooms.EXPECT().GetRoomsMeta(gomock.Any(), "site-b", gomock.InAnyOrder([]string{"rb1", "rb2"})).
		Return([]model.RoomInfo{
			{RoomID: "rb1", Found: true, LastUserMsgAt: &userMs, LastMsgAt: &sysMs}, // read; system bump ignored
			{RoomID: "rb2", Found: true, LastMsgAt: &sysMs},                         // legacy peer: lastMsgAt rules
		}, nil)
	yes := true
	resp, err := svc.CountSubscriptions(ctx("alice", "site-a"), models.CountRequest{Unread: &yes})
	require.NoError(t, err)
	assert.Equal(t, 1, resp.Count, "rb1 read (user activity older than seen); rb2 unread via legacy fallback")
}
```

Also add a list-path test near the existing enrichment tests (same file) asserting `HasUnread` and the wire field: build an `EnrichedSubscription` with `LastSeenAt=200ms`, `LastUserMsgAt=100ms`, `LastMsgAt=300ms`, run it through `svc.ListSubscriptionsFor` with the store mock returning it (mirror `TestCountUnread_Happy`'s mock style for `AggregateSubscriptions`), and assert the returned row has `HasUnread == false` and `Room.LastUserMsgAt` equal to the 100ms time.

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=user-service`
Expected: FAIL — `TestCountUnread_SystemBumpDoesNotCount` returns count 1 (today the code compares `LastMsgAt`).

- [ ] **Step 3: Implement**

Add helpers near `unread()` in `user-service/service/subscriptions.go`:

```go
// coalesceTime / coalesceMillis pick the user-activity reference:
// lastUserMsgAt when the room has one, else lastMsgAt (rooms and peers that
// predate the field). Never createdAt — see the design doc.
func coalesceTime(a, b *time.Time) *time.Time {
	if a != nil {
		return a
	}
	return b
}

func coalesceMillis(a, b *int64) *int64 {
	if a != nil {
		return a
	}
	return b
}
```

`enrichLocal`:

```go
		subs[j].HasUnread = unread(subs[j].LastSeenAt, timeutil.TimeToMillis(coalesceTime(subs[j].LastUserMsgAt, room.LastMsgAt)))
```

`buildLocalRoom` — add to the `SubscriptionRoom` literal after `LastMsgID`:

```go
		LastUserMsgAt:     sub.LastUserMsgAt,
```

`applyRoomInfo` — add to the room literal after `LastMsgID`:

```go
		LastUserMsgAt:     timeutil.MillisToTime(info.LastUserMsgAt),
```

…and switch the unread source:

```go
	sub.HasUnread = unread(sub.LastSeenAt, coalesceMillis(info.LastUserMsgAt, info.LastMsgAt))
```

`unreadRooms` — local branch:

```go
			if unread(subs[i].LastSeenAt, timeutil.TimeToMillis(coalesceTime(subs[i].LastUserMsgAt, subs[i].LastMsgAt))) || len(subs[i].ThreadUnread) > 0 {
```

…cross-site branch, when building the per-room map:

```go
					lastMsg[infos[k].RoomID] = coalesceMillis(infos[k].LastUserMsgAt, infos[k].LastMsgAt)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=user-service`
Expected: PASS, including all pre-existing CountUnread tests (they set only `LastMsgAt`, which the fallback preserves).

- [ ] **Step 5: Commit**

```bash
git add user-service/service/
git commit -m "feat(user-service): compute unread from lastUserMsgAt with lastMsgAt fallback"
```

---

### Task 6: room-service — carry lastUserMsgAt on the cross-site RPC

**Files:**
- Modify: `room-service/handler.go` (`aggregateRoomInfo` ~line 1305)
- Modify: `room-service/store_mongo.go` (`ListRoomsByIDs` projection ~line 438)
- Test: append to `room-service/handler_test.go` (`TestHandler_handleRoomsInfoBatch_ForwardsCounts` sibling, ~line 2612)

**Interfaces:**
- Consumes: `model.Room.LastUserMsgAt`, `model.RoomInfo.LastUserMsgAt` (Task 1).
- Produces: `rooms.info` / `rooms.meta` replies carry `lastUserMsgAt` (epoch ms) for found rooms.

- [ ] **Step 1: Write the failing test**

Append to `room-service/handler_test.go`, mirroring `TestHandler_handleRoomsInfoBatch_ForwardsCounts` (same store-stub style — return a `model.Room` fixture with `LastUserMsgAt` set):

```go
func TestHandler_handleRoomsInfoBatch_ForwardsLastUserMsgAt(t *testing.T) {
	userAt := time.Date(2026, 8, 26, 10, 0, 0, 0, time.UTC)
	sysAt := userAt.Add(time.Hour)
	h := newRoomsInfoHandler(t, []model.Room{{ // reuse the file's fixture helper; if none fits, copy the ForwardsCounts test's setup verbatim
		ID: "r1", SiteID: "site-a", LastMsgAt: &sysAt, LastUserMsgAt: &userAt,
	}})
	resp, err := h.roomsInfoBatch(ctxParams(map[string]string{}), model.RoomsInfoBatchRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	require.Len(t, resp.Rooms, 1)
	require.NotNil(t, resp.Rooms[0].LastUserMsgAt)
	assert.Equal(t, userAt.UnixMilli(), *resp.Rooms[0].LastUserMsgAt)
	require.NotNil(t, resp.Rooms[0].LastMsgAt)
	assert.Equal(t, sysAt.UnixMilli(), *resp.Rooms[0].LastMsgAt)
}
```

(Adapt the constructor line to the file's actual helper for building a handler with a stubbed `ListRoomsByIDs` — copy the setup used by `TestHandler_handleRoomsInfoBatch_ForwardsCounts`.)

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=room-service`
Expected: FAIL — `resp.Rooms[0].LastUserMsgAt` is nil.

- [ ] **Step 3: Implement**

`room-service/handler.go`, `aggregateRoomInfo` — after the `entry.LastMsgAt` line:

```go
		entry.LastUserMsgAt = timePtrToMillis(r.LastUserMsgAt)
```

`room-service/store_mongo.go`, `ListRoomsByIDs` projection — extend:

```go
			"lastMsgId": 1, "lastMsgAt": 1, "lastUserMsgAt": 1, "lastMentionAllAt": 1, "minUserLastSeenAt": 1, "crossSite": 1,
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=room-service`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add room-service/handler.go room-service/store_mongo.go room-service/handler_test.go
git commit -m "feat(room-service): expose lastUserMsgAt on the rooms info batch RPC"
```

---

### Task 7: chat-frontend — gate live events, seed from lastUserMsgAt

**Files:**
- Modify: `chat-frontend/src/api/types.ts` (room object type with `lastMsgAt?: string` ~line 165: add `lastUserMsgAt?: string`; RoomEvent-ish type carrying `lastMsgAt`/`message`: add `systemMsg?: boolean`)
- Modify: `chat-frontend/src/api/fetchSidebarBuckets/index.ts` (`subToRoom` ~line 226)
- Modify: `chat-frontend/src/context/RoomEventsContext/reducer.js` (`MESSAGE_RECEIVED`, both branches ~lines 628-731)
- Test: `chat-frontend/src/context/RoomEventsContext/reducer.test.js` (append) and `chat-frontend/src/api/fetchSidebarBuckets/index.test.ts` (or the file's existing test sibling — append)

**Interfaces:**
- Consumes: wire `room.lastUserMsgAt` (Tasks 5/6) and `RoomEvent.systemMsg` (Task 3).
- Produces: summary/roomState `lastMsgAt` now means "user activity" client-side; `subToRoom` maps `sub.room?.lastUserMsgAt ?? sub.room?.lastMsgAt`.

- [ ] **Step 1: Write the failing reducer tests**

Append to `reducer.test.js`, using the file's existing fixture helpers for a seeded state (mirror a neighboring `MESSAGE_RECEIVED` test's setup — a state with one summary + live-mode roomState):

```js
describe('MESSAGE_RECEIVED system messages', () => {
  const base = seededStateWithRoom('r1', { lastMsgAt: '2026-08-01T00:00:00Z' }) // reuse/adapt the file's seeding helper

  const sysEvent = {
    type: 'new_message',
    roomId: 'r1',
    lastMsgAt: '2026-08-26T10:00:00.000Z',
    lastMsgId: 'm-sys',
    message: {
      id: 'm-sys', roomId: 'r1', type: 'members_added',
      content: 'bob joined', createdAt: '2026-08-26T10:00:00.000Z',
    },
  }

  it('appends to the timeline but does not bump lastMsgAt, unreadCount, or re-sort', () => {
    const next = roomEventsReducer(base, { type: 'MESSAGE_RECEIVED', event: sysEvent })
    const rs = next.roomState['r1']
    expect(rs.messages.map((m) => m.id)).toContain('m-sys')
    expect(rs.lastMsgAt).toBe('2026-08-01T00:00:00Z')
    expect(rs.unreadCount).toBe(base.roomState['r1']?.unreadCount ?? 0)
    expect(next.summaries).toBe(base.summaries) // untouched reference: no resort, no bump
    expect(next.msgRecvSeq).toBe(base.msgRecvSeq + 1)
  })

  it('gates on the plaintext systemMsg flag when the body is encrypted', () => {
    const encEvent = {
      type: 'new_message', roomId: 'r1', systemMsg: true,
      lastMsgAt: '2026-08-26T10:00:00.000Z', lastMsgId: 'm-sys',
      encryptedMessage: { v: 1 },
    }
    const next = roomEventsReducer(base, { type: 'MESSAGE_RECEIVED', event: encEvent })
    expect(next.roomState['r1'].lastMsgAt).toBe('2026-08-01T00:00:00Z')
    expect(next.summaries).toBe(base.summaries)
  })

  it('a normal message still bumps and re-sorts', () => {
    const userEvent = { ...sysEvent, lastMsgId: 'm-user', message: { ...sysEvent.message, id: 'm-user', type: undefined } }
    const next = roomEventsReducer(base, { type: 'MESSAGE_RECEIVED', event: userEvent })
    expect(next.roomState['r1'].lastMsgAt).toBe('2026-08-26T10:00:00.000Z')
    expect(next.summaries).not.toBe(base.summaries)
  })
})
```

And a `subToRoom` test beside the existing fetchSidebarBuckets tests:

```js
it('subToRoom prefers lastUserMsgAt over lastMsgAt for the summary position', () => {
  const sub = {
    roomId: 'r1', roomType: 'channel', name: 'x',
    room: { lastMsgAt: '2026-08-26T10:00:00Z', lastUserMsgAt: '2026-08-01T00:00:00Z' },
  }
  expect(subToRoom(sub, 'site-a').lastMsgAt).toBe('2026-08-01T00:00:00Z')
})
```

Adapt fixture helpers to the file's actual ones; the assertions above are the contract.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd chat-frontend && npm test -- reducer && npm test -- fetchSidebarBuckets`
Expected: FAIL — system event bumps `lastMsgAt` and replaces `summaries`; `subToRoom` returns the system time.

- [ ] **Step 3: Implement**

`types.ts`: add `lastUserMsgAt?: string` next to the room object's `lastMsgAt` (~line 165) and `systemMsg?: boolean` to the room-event type (search for the interface carrying `lastMsgAt` + `encryptedMessage`).

`subToRoom`:

```ts
    lastMsgAt: sub.room?.lastUserMsgAt ?? sub.room?.lastMsgAt ?? undefined,
```

`reducer.js`, in `MESSAGE_RECEIVED` after `const roomId = evt.roomId` (line ~612), add:

```js
      // A system message renders in the timeline but is not activity: it must
      // not bump the room's position, unread count, or sidebar order. The
      // plaintext evt.systemMsg covers encrypted channels where msg.type is
      // sealed; type/sysMsgData cover plaintext (and the synthesized
      // encrypted placeholder, which has neither, correctly falls through
      // only when the event itself isn't flagged).
      const isSystemEvent =
        evt.systemMsg === true || isSystemMessageType(msg.type) || msg.sysMsgData != null
```

In BOTH branches (historical ~647-671 and live ~703-724), change:

```js
        lastMsgAt: isSystemEvent ? prev.lastMsgAt : (evt.lastMsgAt ?? msg.createdAt ?? prev.lastMsgAt),
```

```js
        unreadCount: isActive || isSystemEvent ? prev.unreadCount : prev.unreadCount + 1,
```

…and guard each `summaries` computation:

```js
      const summaries = !isSystemEvent && state.summaries.some((r) => r.id === roomId)
        ? sortByLastMsgDesc(
            ...unchanged mapping...
          )
        : state.summaries
```

(`isSystemMessageType` is already imported in reducer.js for `previewFromMessage`.)

- [ ] **Step 4: Run tests + gates to verify they pass**

Run: `cd chat-frontend && npm test && npm run typecheck`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add chat-frontend/src/api/types.ts chat-frontend/src/api/fetchSidebarBuckets/ chat-frontend/src/context/RoomEventsContext/
git commit -m "fix(chat-frontend): system messages no longer bump unread or sidebar order"
```

---

### Task 8: Docs

**Files:**
- Modify: `docs/client-api.md` (`hasUnread` row ~line 978, room-object `lastMsgAt` row ~line 1018, `new_message` RoomEvent field table, Message schema note)
- Modify: `docs/client-api/request-reply.md` (subscription.list room table — same rows)
- Modify: `docs/client-api/events.md` (`new_message` event table ~line 372/422)

- [ ] **Step 1: Edit the canonical doc**

In `docs/client-api.md`:
- `hasUnread` row becomes: `Whether the room has unread messages — computed at read time by comparing the room's lastUserMsgAt (falling back to lastMsgAt for rooms that predate it) to the subscription's lastSeenAt (not persisted). System messages never set it; a member who has never opened the room sees it true whenever the room has any user-activity reference.`
- Below the room object's `lastMsgAt` row add: `| lastUserMsgAt | RFC3339 timestamp | Optional. The room's last non-system message time — the value hasUnread and sidebar ordering derive from. Absent when the room has none recorded; clients fall back to lastMsgAt. |`
- In the `new_message` RoomEvent table add: `| systemMsg | boolean | Optional. true when the message is a server-generated system message (room_created, members_added, …). Clients must not advance unread state or sidebar ordering from a flagged event — present in plaintext even when the body is sealed in encryptedMessage. |`
- Keep the `meta.lastMsgAt` history-hint section unchanged (it deliberately stays the full-activity value); add one sentence there: `Use the room's lastMsgAt for this hint, not lastUserMsgAt — the hint bounds the timeline read, which includes system messages.`

- [ ] **Step 2: Mirror in the derived views**

Apply the same three additions to `docs/client-api/request-reply.md` (room object table) and `docs/client-api/events.md` (`new_message` table + note). The views must not drift from the canonical doc.

- [ ] **Step 3: Verify and commit**

Run: `make lint` (docs don't lint, but this catches any stray Go edits) and skim the three files for consistency.

```bash
git add docs/client-api.md docs/client-api/request-reply.md docs/client-api/events.md
git commit -m "docs(client-api): lastUserMsgAt drives unread/ordering; systemMsg flag on new_message"
```

---

### Task 9: Full verification

- [ ] **Step 1:** `make lint` — fix anything it flags in touched files only.
- [ ] **Step 2:** `make test` — full unit suite with race detector.
- [ ] **Step 3:** `make test-integration SERVICE=broadcast-worker && make test-integration SERVICE=user-service` (Docker; skip with a note if unavailable).
- [ ] **Step 4:** `cd chat-frontend && npm test && npm run typecheck`.
- [ ] **Step 5:** Push: `git push -u origin claude/system-events-unread-count-liqen1`.
