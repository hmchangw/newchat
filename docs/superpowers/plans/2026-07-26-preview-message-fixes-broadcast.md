# Preview Message Fixes + Edit/Delete Broadcast Enrichment — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Quoted replies become eligible room-list previews; system-message detection uses an explicit type map instead of an empty-type check; and message edit/delete fan-out events carry the refreshed room preview.

**Architecture:** History-service owns preview resolution (`roomLastMessage`). Fix its skip logic, back system detection with a `model.IsSystemMessageType` map, and — after an edit/delete Cassandra write — recompute the preview and embed it on the canonical `MessageEvent`. Broadcast-worker relays that preview onto the client-facing `EditRoomEvent`/`DeleteRoomEvent` (as `json.RawMessage`: object, `null`=cleared, or absent on thread-reply events).

**Tech Stack:** Go 1.25, NATS JetStream, `pkg/model`, `go.uber.org/mock` (mockgen), `stretchr/testify`. Codec: broadcast-worker uses `github.com/bytedance/sonic`; history-service uses `encoding/json`.

## Global Constraints

- Go 1.25; single root `go.mod`; module `github.com/hmchangw/chat`.
- Always use `make` targets, never raw `go`. Unit tests run with `-race` (Makefile handles it).
- TDD Red→Green→Refactor→Commit for every task. Tests live in `package main` (services) / `package model` (lib), same package as code under test.
- Minimum 80% coverage per package; target 90%+ for handlers/stores.
- All NATS payloads are typed structs from `pkg/model`; no `map[string]interface{}`.
- Event structs in `pkg/model` must keep both `json` and `bson` tags. `bson:"-"` for never-persisted computed fields.
- Client-facing event struct changes (`EditRoomEvent`/`DeleteRoomEvent`) MUST update `docs/client-api.md` AND its derived views `docs/client-api/events.md` in the same PR.
- Commit author: `git config user.email noreply@anthropic.com && git config user.name Claude`. Commit-message footer lines (Co-Authored-By / Claude-Session) as configured for this branch. Do NOT include the model identifier in any commit/PR/code artifact.
- Branch: `claude/preview-message-fixes-broadcast-4jk8qv`.
- Real system-message types are exactly these 7 constants in `pkg/model/event.go`: `MessageTypeRoomCreated`, `MessageTypeMembersAdded`, `MessageTypeMemberRemoved`, `MessageTypeMemberLeft`, `MessageTypeRoomRenamed`, `MessageTypeRoomRestricted`, `MessageTypeTeamsMeetStarted`. (`call_ended`/`call_started` are test-only placeholders; `message_removed`/`teams_system` are excluded — the former is Cassandra-only and already `Deleted`.)

---

## Task 1: `model.IsSystemMessageType` + lookup map

**Files:**
- Modify: `pkg/model/event.go` (add after the `MessageType*` const block ending at line 530)
- Create: `pkg/model/event_test.go`

**Interfaces:**
- Produces: `func model.IsSystemMessageType(t string) bool` — true iff `t` is one of the 7 system-message type constants.

- [ ] **Step 1: Write the failing test** — create `pkg/model/event_test.go`:

```go
package model

import "testing"

func TestIsSystemMessageType(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want bool
	}{
		{"room_created", MessageTypeRoomCreated, true},
		{"members_added", MessageTypeMembersAdded, true},
		{"member_removed", MessageTypeMemberRemoved, true},
		{"member_left", MessageTypeMemberLeft, true},
		{"room_renamed", MessageTypeRoomRenamed, true},
		{"room_restricted", MessageTypeRoomRestricted, true},
		{"teams_meet_started", MessageTypeTeamsMeetStarted, true},
		{"empty is normal user message", "", false},
		{"literal message", "message", false},
		{"cassandra tombstone excluded", "message_removed", false},
		{"teams migration marker excluded", "teams_system", false},
		{"unknown", "call_ended", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsSystemMessageType(tc.in); got != tc.want {
				t.Fatalf("IsSystemMessageType(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=pkg/model` (or `go test ./pkg/model/ -run TestIsSystemMessageType`)
Expected: FAIL — `undefined: IsSystemMessageType`.

- [ ] **Step 3: Write minimal implementation** — in `pkg/model/event.go`, immediately after line 530 (the closing `)` of the `MessageType*` const block):

```go
// systemMessageTypes is the set of Message.Type values denoting a system/event message
// (not user-authored content). Used for fast membership checks — e.g. excluding system
// messages from room-list previews. Keep in sync with the MessageType* constants above.
var systemMessageTypes = map[string]struct{}{
	MessageTypeRoomCreated:      {},
	MessageTypeMembersAdded:     {},
	MessageTypeMemberRemoved:    {},
	MessageTypeMemberLeft:       {},
	MessageTypeRoomRenamed:      {},
	MessageTypeRoomRestricted:   {},
	MessageTypeTeamsMeetStarted: {},
}

// IsSystemMessageType reports whether t is a known system-message type. A normal
// user message has Type == "" and returns false.
func IsSystemMessageType(t string) bool {
	_, ok := systemMessageTypes[t]
	return ok
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=pkg/model`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/model/event.go pkg/model/event_test.go
git commit -m "feat(model): add IsSystemMessageType lookup map for system-message types"
```

---

## Task 2: Preview skip — quoted replies eligible, system via map

**Files:**
- Modify: `history-service/internal/service/rooms.go:96-104` (the skip loop in `roomLastMessage`)
- Modify: `history-service/internal/service/rooms_test.go` (update fixtures that relied on `Type != ""` and quoted-skip)

**Interfaces:**
- Consumes: `model.IsSystemMessageType` (Task 1). `pkgmodel` is the alias for `github.com/hmchangw/chat/pkg/model` already imported in `rooms.go`.
- Produces: no signature change — `roomLastMessage` still returns `(models.PreviewMessage, bool)`.

- [ ] **Step 1: Update the failing tests first (Red).** In `history-service/internal/service/rooms_test.go`:

Replace `TestHistoryService_RoomsGet_SkipsQuotedTail` (currently expects the quoted reply to be skipped) so a quoted reply is now RETURNED as the preview:

```go
// A quoted reply is normal room content and IS eligible as the preview.
func TestHistoryService_RoomsGet_QuotedReplyEligible(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)

	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(roomLastMsgAt, roomCreatedAt, nil)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{
			{MessageID: "m2", RoomID: "r1", Msg: "re: x", QuotedParentMessage: &models.QuotedParentMessage{MessageID: "m0"}, CreatedAt: roomLastMsgAt},
			{MessageID: "m1", RoomID: "r1", Msg: "alive", CreatedAt: roomLastMsgAt.Add(-time.Minute)},
		}, false), nil)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	require.Contains(t, resp.Rooms, "r1")
	assert.Equal(t, "m2", resp.Rooms["r1"].MessageID)
}
```

In `TestHistoryService_RoomsGet_SkipsSystemTail`, change the system message's type from the placeholder `"call_ended"` to a real constant so it still exercises the map path:

```go
			{MessageID: "m2", RoomID: "r1", Type: model.MessageTypeRoomRenamed, CreatedAt: roomLastMsgAt},
			{MessageID: "m1", RoomID: "r1", Msg: "alive", CreatedAt: roomLastMsgAt.Add(-time.Minute)},
```
(expectation stays `"m1"`.)

In `TestHistoryService_RoomsGet_MixedTailSkipsAllIneligible`, the quoted message is now eligible, so it becomes the preview. Rewrite it to keep a real system type + deleted as the only ineligible tail and assert the quoted reply is returned:

```go
// Mixed tail: a real system message + a deleted message precede a quoted reply,
// which IS eligible and becomes the preview.
func TestHistoryService_RoomsGet_MixedTailSkipsIneligible(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)

	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(roomLastMsgAt, roomCreatedAt, nil)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{
			{MessageID: "m4", RoomID: "r1", Type: model.MessageTypeMembersAdded, CreatedAt: roomLastMsgAt},
			{MessageID: "m3", RoomID: "r1", Deleted: true, CreatedAt: roomLastMsgAt.Add(-time.Minute)},
			{MessageID: "m2", RoomID: "r1", Msg: "re: x", QuotedParentMessage: &models.QuotedParentMessage{MessageID: "m0"}, CreatedAt: roomLastMsgAt.Add(-2 * time.Minute)},
			{MessageID: "m1", RoomID: "r1", Msg: "alive", CreatedAt: roomLastMsgAt.Add(-3 * time.Minute)},
		}, false), nil)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	require.Contains(t, resp.Rooms, "r1")
	assert.Equal(t, "m2", resp.Rooms["r1"].MessageID)
}
```

> Note: `model` — confirm the alias used by `rooms_test.go` for `pkg/model` (the package is imported by the existing tests; `models` is the history-service internal package, `model`/`pkgmodel` is `pkg/model`). Use whatever alias the file already imports `pkg/model` under; if it is not yet imported, add `"github.com/hmchangw/chat/pkg/model"`.

- [ ] **Step 2: Run the tests to verify they fail (Red)**

Run: `make test SERVICE=history-service`
Expected: FAIL — `QuotedReplyEligible` returns `m1` (quoted still skipped), and the renamed mixed test returns `m1` not `m2`, because production still uses `m.Type != "" || m.QuotedParentMessage != nil`.

- [ ] **Step 3: Update production skip logic (Green).** In `history-service/internal/service/rooms.go`, replace lines 96-104:

```go
		for i := range page.Data {
			m := page.Data[i]
			// System and deleted messages aren't representative room content — skip to the
			// previous eligible message. Quoted replies ARE eligible (normal user content).
			if m.Deleted || pkgmodel.IsSystemMessageType(m.Type) {
				continue
			}
			return s.toPreviewMessage(ctx, &m), true
		}
```

- [ ] **Step 4: Run tests to verify they pass (Green)**

Run: `make test SERVICE=history-service`
Expected: PASS (including `QuotedReplyEligible`, `SkipsSystemTail`, `MixedTailSkipsIneligible`, `SkipsDeletedTail`, `NormalMessageUnaffected`).

- [ ] **Step 5: Verify lint + coverage**

Run: `make lint` and `make test SERVICE=history-service`
Expected: clean; history-service package ≥80%.

- [ ] **Step 6: Commit**

```bash
git add history-service/internal/service/rooms.go history-service/internal/service/rooms_test.go
git commit -m "fix(history-service): quoted replies eligible as preview; detect system messages via IsSystemMessageType"
```

---

## Task 3: Add `PreviewMessage` to event structs

**Files:**
- Modify: `pkg/model/event.go` — `MessageEvent` (lines 29-47), `EditRoomEvent` (lines 307-319), `DeleteRoomEvent` (lines 322-332)

**Interfaces:**
- Produces:
  - `MessageEvent.PreviewMessage *PreviewMessage` (json `previewMessage,omitempty`; bson `-`) — internal passthrough.
  - `EditRoomEvent.PreviewMessage json.RawMessage` (json `previewMessage,omitempty`) — client-facing.
  - `DeleteRoomEvent.PreviewMessage json.RawMessage` (json `previewMessage,omitempty`) — client-facing.
- `encoding/json` is already imported in `event.go` (line 4). `PreviewMessage` is defined in `pkg/model/message.go`.

- [ ] **Step 1: Write the failing test** — append to `pkg/model/event_test.go`:

```go
import (
	"encoding/json"
	"testing"
)

func TestEditRoomEvent_PreviewMessageRawJSON(t *testing.T) {
	// object case
	e := EditRoomEvent{Type: RoomEventMessageEdited, PreviewMessage: json.RawMessage(`{"messageId":"m1"}`)}
	b, err := json.Marshal(&e)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !contains(b, `"previewMessage":{"messageId":"m1"}`) {
		t.Fatalf("object preview not serialized: %s", b)
	}
	// null case (cleared)
	e.PreviewMessage = json.RawMessage("null")
	b, _ = json.Marshal(&e)
	if !contains(b, `"previewMessage":null`) {
		t.Fatalf("null preview not serialized: %s", b)
	}
	// absent case (thread path leaves it nil → omitempty drops it)
	e.PreviewMessage = nil
	b, _ = json.Marshal(&e)
	if contains(b, "previewMessage") {
		t.Fatalf("nil preview should be omitted: %s", b)
	}
}

func TestDeleteRoomEvent_PreviewMessageRawJSON(t *testing.T) {
	d := DeleteRoomEvent{Type: RoomEventMessageDeleted, PreviewMessage: json.RawMessage("null")}
	b, err := json.Marshal(&d)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !contains(b, `"previewMessage":null`) {
		t.Fatalf("null preview not serialized: %s", b)
	}
	d.PreviewMessage = nil
	b, _ = json.Marshal(&d)
	if contains(b, "previewMessage") {
		t.Fatalf("nil preview should be omitted: %s", b)
	}
}

func contains(b []byte, sub string) bool {
	return len(b) > 0 && (string(b) == sub || bytesIndex(string(b), sub) >= 0)
}

func bytesIndex(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
```

> Merge the `import` block with Task 1's `import "testing"` line (there must be exactly one import block in the file). Since `TestIsSystemMessageType` from Task 1 already imports `testing`, extend it to the two-import block shown here.

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=pkg/model`
Expected: FAIL — `EditRoomEvent has no field PreviewMessage` / `DeleteRoomEvent has no field PreviewMessage`.

- [ ] **Step 3: Add the fields (implementation).**

In `MessageEvent` (after `ThreadParentSenderAccount`, before the closing brace at line 47):
```go
	// PreviewMessage is the room's refreshed last-eligible message, computed by history-service
	// after an edit/delete so broadcast-worker can relay it in the fan-out event. Same resolution
	// as subscription.list. Set only on EventUpdated/EventDeleted for room-visible messages; nil
	// otherwise (including "no eligible message remains" — treated as cleared downstream).
	PreviewMessage *PreviewMessage `json:"previewMessage,omitempty" bson:"-"`
```

In `EditRoomEvent` (after `UpdatedAt`, before the closing brace at line 319):
```go
	// PreviewMessage is the room's refreshed preview after this edit (same resolution as
	// subscription.list). On room-level edits: a serialized PreviewMessage object, or the JSON
	// literal null when the room has no eligible message left (client clears its preview). Absent
	// on thread-reply (TShow==false) edits, which don't affect the room preview. json.RawMessage
	// (not *PreviewMessage) so the shared builder can distinguish absent (thread) from null (cleared).
	PreviewMessage json.RawMessage `json:"previewMessage,omitempty" bson:"previewMessage,omitempty"`
```

In `DeleteRoomEvent` (after `UpdatedAt`, before the closing brace at line 332):
```go
	// PreviewMessage is the room's refreshed preview after this delete (same resolution as
	// subscription.list). On room-level deletes: a serialized PreviewMessage object, or the JSON
	// literal null when the room has no eligible message left (client clears its preview). Absent
	// on thread-reply (TShow==false) deletes, which don't affect the room preview.
	PreviewMessage json.RawMessage `json:"previewMessage,omitempty" bson:"previewMessage,omitempty"`
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=pkg/model`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/model/event.go pkg/model/event_test.go
git commit -m "feat(model): add previewMessage passthrough to MessageEvent and Edit/DeleteRoomEvent"
```

---

## Task 4: history-service embeds preview on edit/delete

**Files:**
- Modify: `history-service/internal/service/messages.go` — `EditMessage` (486-558) and `DeleteMessage` (562-635)
- Modify: `history-service/internal/service/messages_test.go` (add assertions)

**Interfaces:**
- Consumes: `MessageEvent.PreviewMessage` (Task 3); `roomLastMessage(ctx, roomID, now) (models.PreviewMessage, bool)` — `models.PreviewMessage` is a type alias of `pkgmodel.PreviewMessage`, so `&preview` assigns to `*model.PreviewMessage`. `messages.go` imports `pkg/model` as `model` and the internal package as `models`.
- Produces: canonical events now carry `PreviewMessage` for room-visible edits/deletes.

- [ ] **Step 1: Write failing tests.** In `history-service/internal/service/messages_test.go`, add tests asserting the published canonical event's `PreviewMessage`. Match the file's existing pattern for capturing the published event (find how `EditMessage`/`DeleteMessage` tests capture the publisher payload; reuse that harness). The assertions:

```go
// Edit of a room-visible message publishes the refreshed preview.
func TestEditMessage_PublishesPreview(t *testing.T) {
	// ... set up svc with mocked writer + reader so that after UpdateMessageContent,
	//     roomLastMessage resolves to a message with MessageID "m-new".
	//     (Mirror the setup in the existing EditMessage happy-path test.)
	// After calling svc.EditMessage(...):
	//   require published EventUpdated canonical event's evt.PreviewMessage != nil
	//   assert.Equal(t, "m-new", evt.PreviewMessage.MessageID)
}

// Delete of the last eligible message publishes a nil preview (cleared).
func TestDeleteMessage_LastMessage_PublishesNilPreview(t *testing.T) {
	// ... after SoftDeleteMessage applied, roomLastMessage resolves to ok=false
	//     (e.g. GetMessagesBefore returns an empty page).
	// Assert published EventDeleted event's evt.PreviewMessage == nil.
}

// Hidden thread reply (TShow=false, thread parent) edit does NOT compute a preview.
func TestEditMessage_HiddenThreadReply_NoPreview(t *testing.T) {
	// ... msg has ThreadParentID != "" and TShow == false.
	// Assert roomLastMessage's reader (GetMessagesBefore) is NOT called, and
	//     published event's evt.PreviewMessage == nil.
}
```

> Use the exact mock/publisher-capture idiom already in `messages_test.go` (e.g. a stubbed `s.publisher` capturing `subj,payload`, then `json.Unmarshal` into `model.MessageEvent`). Set expectations on the same reader/writer mocks the file already constructs (`msgReader`/`msgWriter`/room-times). For the "no preview" thread case, assert the reader mock's `GetMessagesBefore` gets zero calls (`.Times(0)`), proving the walk was skipped.

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=history-service`
Expected: FAIL — `PreviewMessage` never set (compile passes since field exists; assertions fail: nil where a message expected, and the reader mock unexpectedly uncalled/called).

- [ ] **Step 3: Implement in `EditMessage`.** After the `canonicalEvt := model.MessageEvent{...}` block (currently ending at line 551, before `publishCanonicalBestEffort` at 552), insert:

```go
	// Refresh the room preview so broadcast-worker can relay it (same resolution as
	// subscription.list). Skip hidden thread replies (TShow==false with a parent): they never
	// appear in the room timeline, so the room preview can't have changed. Best-effort: a read
	// failure leaves PreviewMessage nil (downstream treats nil as "cleared").
	if msg.ThreadParentID == "" || msg.TShow {
		if preview, ok := s.roomLastMessage(c, roomID, editedAt); ok {
			canonicalEvt.PreviewMessage = &preview
		}
	}
```

- [ ] **Step 4: Implement in `DeleteMessage`.** After the `canonicalEvt := model.MessageEvent{...}` block (currently ending at line 628, before `publishCanonicalBestEffort` at 629), insert:

```go
	if msg.ThreadParentID == "" || msg.TShow {
		if preview, ok := s.roomLastMessage(c, roomID, actualDeletedAt); ok {
			canonicalEvt.PreviewMessage = &preview
		}
	}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `make test SERVICE=history-service`
Expected: PASS.

- [ ] **Step 6: Lint + coverage**

Run: `make lint` and `make test SERVICE=history-service`
Expected: clean; ≥80%.

- [ ] **Step 7: Commit**

```bash
git add history-service/internal/service/messages.go history-service/internal/service/messages_test.go
git commit -m "feat(history-service): embed refreshed room preview on edit/delete canonical events"
```

---

## Task 5: broadcast-worker relays preview onto fan-out events

**Files:**
- Modify: `broadcast-worker/handler.go` — `handleUpdated` (287-309), `handleDeleted` (484-510); add `previewJSON` helper near the build functions (701-730)
- Modify: `broadcast-worker/handler_test.go` (add assertions)

**Interfaces:**
- Consumes: `MessageEvent.PreviewMessage` (Task 3), `EditRoomEvent.PreviewMessage`/`DeleteRoomEvent.PreviewMessage` (Task 3). `handler.go` already imports `encoding/json` and `github.com/bytedance/sonic`.
- Produces: room-level `EditRoomEvent`/`DeleteRoomEvent` carry `previewMessage`; thread path leaves it absent.

- [ ] **Step 1: Write failing tests.** In `broadcast-worker/handler_test.go`:

Add a preview to the existing channel edit/delete happy-path events and assert relay. New tests (mirroring `TestHandleUpdated_ChannelRoomScopedPublish` setup at line 778):

```go
func TestHandleUpdated_RelaysPreviewObject(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	roomID := "r1"
	room := &model.Room{ID: roomID, Type: model.RoomTypeChannel, SiteID: "site-a"}
	store.EXPECT().GetRoom(gomock.Any(), roomID).Return(room, nil)

	edited := time.Date(2026, 5, 14, 12, 5, 0, 0, time.UTC)
	evt := model.MessageEvent{
		Event: model.EventUpdated, SiteID: "site-a", Timestamp: edited.UnixMilli(),
		Message: model.Message{
			ID: "msg-1", RoomID: roomID, UserID: "u-alice", UserAccount: "alice",
			Content: "updated", CreatedAt: edited.Add(-time.Hour), EditedAt: &edited, UpdatedAt: &edited,
		},
		PreviewMessage: &model.PreviewMessage{MessageID: "msg-1", Content: "updated"},
	}
	data, err := json.Marshal(&evt)
	require.NoError(t, err)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	require.Len(t, pub.records, 1)
	var roomEvt model.EditRoomEvent
	require.NoError(t, json.Unmarshal(pub.records[0].data, &roomEvt))
	require.NotEmpty(t, roomEvt.PreviewMessage)
	var pm model.PreviewMessage
	require.NoError(t, json.Unmarshal(roomEvt.PreviewMessage, &pm))
	assert.Equal(t, "msg-1", pm.MessageID)
}

func TestHandleDeleted_RelaysNilPreviewAsNull(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	roomID := "r1"
	room := &model.Room{ID: roomID, Type: model.RoomTypeChannel, SiteID: "site-a"}
	store.EXPECT().GetRoom(gomock.Any(), roomID).Return(room, nil)

	deleted := time.Date(2026, 5, 14, 12, 5, 0, 0, time.UTC)
	evt := model.MessageEvent{
		Event: model.EventDeleted, SiteID: "site-a", Timestamp: deleted.UnixMilli(),
		Message: model.Message{
			ID: "msg-1", RoomID: roomID, UserID: "u-alice", UserAccount: "alice",
			CreatedAt: deleted.Add(-time.Hour), UpdatedAt: &deleted,
		},
		// PreviewMessage nil -> cleared
	}
	data, err := json.Marshal(&evt)
	require.NoError(t, err)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, false)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	require.Len(t, pub.records, 1)
	assert.Contains(t, string(pub.records[0].data), `"previewMessage":null`)
}
```

Also add a `previewJSON` unit test:

```go
func TestPreviewJSON(t *testing.T) {
	assert.Equal(t, "null", string(previewJSON(nil)))
	b := previewJSON(&model.PreviewMessage{MessageID: "m1"})
	assert.Contains(t, string(b), `"messageId":"m1"`)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=broadcast-worker`
Expected: FAIL — `undefined: previewJSON`; and relay assertions fail (field empty/absent).

- [ ] **Step 3: Add the `previewJSON` helper** in `broadcast-worker/handler.go`, immediately above `buildEditRoomEvent` (line 701):

```go
// previewJSON marshals the refreshed room preview for a room-level edit/delete fan-out event.
// A nil preview marshals to the JSON literal null, signalling the client to clear its room
// preview (the last eligible message was removed). Always returns non-nil so room-level events
// always carry previewMessage; the thread path never calls this, leaving the field absent.
func previewJSON(p *model.PreviewMessage) json.RawMessage {
	b, err := sonic.Marshal(p) // p == nil -> "null"
	if err != nil {
		return json.RawMessage("null")
	}
	return b
}
```

- [ ] **Step 4: Set the field in `handleUpdated`.** Change line 302 area so, after building `edit`, the preview is attached (before the encryption block and publish):

```go
	edit := buildEditRoomEvent(room, evt)
	edit.PreviewMessage = previewJSON(evt.PreviewMessage)
	if room.Type == model.RoomTypeChannel && h.encrypt {
		if err := h.encryptEditedContent(ctx, room.ID, &edit); err != nil {
			return fmt.Errorf("encrypt edit content for room %s: %w", room.ID, err)
		}
	}
	return h.publishMutation(ctx, room, model.RoomEventMessageEdited, msg.ID, &edit)
```

- [ ] **Step 5: Set the field in `handleDeleted`.** After building `del` (line 499):

```go
	del := buildDeleteRoomEvent(room, evt)
	del.PreviewMessage = previewJSON(evt.PreviewMessage)
	if err := h.publishMutation(ctx, room, model.RoomEventMessageDeleted, msg.ID, &del); err != nil {
		return fmt.Errorf("publish delete mutation for room %s message %s: %w", room.ID, msg.ID, err)
	}
```

(Do NOT touch `handleThreadUpdated`/`handleThreadDeleted` — they call the same builders but must leave `previewMessage` absent.)

- [ ] **Step 6: Run tests to verify they pass**

Run: `make test SERVICE=broadcast-worker`
Expected: PASS. Existing thread tests (`TestHandleThreadUpdated_*`, `TestHandleThreadDeleted_*`) still pass with no `previewMessage` in their payloads.

- [ ] **Step 7: Add a thread-path absence assertion.** In one existing thread test (e.g. `TestHandleThreadUpdated_ChannelRoom_FansOutToFollowers` at line 2429), after unmarshaling `roomEvt`, add:

```go
	assert.Empty(t, roomEvt.PreviewMessage, "thread edit must not carry a room preview")
```

- [ ] **Step 8: Run tests, lint, coverage**

Run: `make test SERVICE=broadcast-worker` and `make lint`
Expected: PASS; broadcast-worker ≥80%.

- [ ] **Step 9: Commit**

```bash
git add broadcast-worker/handler.go broadcast-worker/handler_test.go
git commit -m "feat(broadcast-worker): relay refreshed room preview on edit/delete fan-out"
```

---

## Task 6: Client-API docs

**Files:**
- Modify: `docs/client-api.md` — `EditRoomEvent` and `DeleteRoomEvent` sections (field tables + JSON examples)
- Modify: `docs/client-api/events.md` — matching `EditRoomEvent`/`DeleteRoomEvent` entries

**Interfaces:** none (docs only). `PreviewMessage` is already a documented shared schema — link it as `[PreviewMessage](#previewmessage)`.

- [ ] **Step 1: Locate the sections.**

Run: `grep -n "message_edited\|message_deleted\|EditRoomEvent\|DeleteRoomEvent" docs/client-api.md docs/client-api/events.md`
Expected: line numbers for both events in both files.

- [ ] **Step 2: Add the field row to both events in both files.** In each event's field table add a row:

```
| `previewMessage` | [PreviewMessage](#previewmessage) \| null | The room's refreshed preview after this edit/delete. An object when an eligible message remains; `null` when the room has no eligible message left (clear the preview). Omitted on thread-reply (`tshow=false`) edit/delete events, which don't change the room preview. |
```

Match the exact column layout each table already uses (header names/order may differ between `client-api.md` and `events.md` — mirror the neighbouring rows).

- [ ] **Step 3: Update the JSON examples** for `message_edited` and `message_deleted` in both files to include `"previewMessage": { ...a PreviewMessage... }`, and add a one-line note that it is `null` when the last eligible message was removed.

- [ ] **Step 4: Sanity check no drift.**

Run: `grep -n "previewMessage" docs/client-api.md docs/client-api/events.md`
Expected: the new field appears under both events in both files.

- [ ] **Step 5: Commit**

```bash
git add docs/client-api.md docs/client-api/events.md
git commit -m "docs(client-api): document previewMessage on message_edited/message_deleted events"
```

---

## Task 7: Full verification

- [ ] **Step 1: Regenerate mocks if any store interface changed.** (None expected — no store interface was modified. Skip if `git diff` shows no `store.go` change.)

Run: `make generate` (only if a `store.go` changed)

- [ ] **Step 2: Run the full unit suite with race.**

Run: `make test`
Expected: PASS across all packages.

- [ ] **Step 3: Lint.**

Run: `make lint`
Expected: clean.

- [ ] **Step 4: SAST (blocking CI gate).**

Run: `make sast`
Expected: no medium+ findings introduced.

- [ ] **Step 5: Push.**

```bash
git push -u origin claude/preview-message-fixes-broadcast-4jk8qv
```

---

## Self-Review (author checklist — completed at plan-writing time)

- **Spec coverage:** Fix 1 (quoted eligible) → Task 2. Fix 2 (system map) → Tasks 1+2. Feature (edit/delete preview, Option A) → Tasks 3+4+5. Docs → Task 6. Verification → Task 7. All spec sections mapped.
- **Placeholder scan:** Test bodies in Task 4 are described-with-assertions rather than full literals because they must reuse `messages_test.go`'s existing publisher-capture harness (whose exact shape the implementer reads in-file); every other code step has complete code. Flagged explicitly in-task.
- **Type consistency:** `IsSystemMessageType(string) bool`, `MessageEvent.PreviewMessage *PreviewMessage`, `EditRoomEvent/DeleteRoomEvent.PreviewMessage json.RawMessage`, `previewJSON(*model.PreviewMessage) json.RawMessage` — consistent across Tasks 1/3/4/5.
- **Key risk:** `call_ended`/`call_started` in existing history tests are placeholders; Task 2 Step 1 replaces them with real constants so the map path is exercised without regressing real behavior.
