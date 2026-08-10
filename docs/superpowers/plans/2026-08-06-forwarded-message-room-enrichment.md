# Forwarded-Message Room Enrichment Implementation Plan

> **⚠️ SUPERSEDED (2026-08-10).** The read-time room-enrichment approach described
> below was implemented and then removed. Only `roomId` + `roomType` (plus the
> source's `tshow`/`threadRoomId`) are needed, and all of those are immutable —
> so they are now captured once at forward time into the `ForwardedMessage`
> snapshot by `message-gatekeeper`, and there is no read-time lookup at all.
> Kept for the decision record; do not implement from this document.


> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** History-service read paths return each forwarded snapshot with an inline `room` object (`{id, name, type}`; dm/botDM: `{id, type}`) resolved read-time from the local Mongo `rooms` collection.

**Architecture:** `MessageRoom`/`MessageHRInfo`/`MessageAppInfo`/`RoomType` move from `pkg/model` into `pkg/model/cassandra` (aliases left behind, zero consumer changes) so `cassandra.ForwardedMessage` can gain a transient `Room *MessageRoom` field (`cql:"-"`, never persisted). history-service gains one batched, projected Mongo lookup (`GetRoomsNameType`) and an unexported best-effort helper `enrichForwardedRooms` called at the end of every message-returning read path.

**Tech Stack:** Go 1.25, mongo-driver v2 via `pkg/mongoutil`, `go.uber.org/mock` + testify, testcontainers via `pkg/testutil`.

**Spec:** `docs/superpowers/specs/2026-08-06-forwarded-message-room-enrichment-design.md`

## Global Constraints

- Branch: `claude/message-room-enrichment-krp5m3` (based on the message-forwarding branch). Commit after every task; push with `git push -u origin claude/message-room-enrichment-krp5m3`.
- All commands via `make` targets — never raw `go` commands. `SERVICE=` takes a path: `make test SERVICE=pkg/model`, `make test-integration SERVICE=history-service/internal/mongorepo`.
- TDD: write the failing test first, watch it fail, then implement.
- Enrichment is **best-effort**: a Mongo error or missing room doc logs one `slog.Warn` and omits `room`; a read must never fail because of enrichment.
- dm/botDM sources: `room` carries only `id` + `type` (no `name`, no `hrInfo`/`appInfo`). All other types (channel, discussion, unknown): `id` + `name` + `type`.
- Every Mongo find carries an explicit projection.
- Wire compatibility: the moved types keep byte-identical JSON; all existing consumers must compile unchanged via aliases.
- Do not log tokens or message bodies. Never log AND return the same error.
- `docs/reviews/` files (if any get created) are deleted before any PR.

---

### Task 1: Move enrichment carrier types into `pkg/model/cassandra` (aliases in `pkg/model`)

**Files:**
- Create: `pkg/model/cassandra/enrich.go`
- Create: `pkg/model/cassandra/enrich_test.go`
- Modify: `pkg/model/search.go` (delete moved type bodies at lines 54–91, add aliases)
- Modify: `pkg/model/room.go` (replace `type RoomType string` at line 5 with an alias; constants stay)

**Interfaces:**
- Consumes: nothing new.
- Produces: `cassandra.MessageRoom{ID, Name string; Type RoomType; HRInfo *MessageHRInfo; AppInfo *MessageAppInfo}`, `cassandra.MessageHRInfo`, `cassandra.MessageAppInfo`, `cassandra.RoomType` (`type RoomType string`), plus aliases `model.MessageRoom = cassandra.MessageRoom`, `model.MessageHRInfo`, `model.MessageAppInfo`, `model.RoomType = cassandra.RoomType`. Later tasks reference `cassandra.MessageRoom` and `model.RoomTypeDM`/`model.RoomTypeBotDM` (constants unchanged in `pkg/model/room.go`).

- [ ] **Step 1: Write the failing test**

Create `pkg/model/cassandra/enrich_test.go` (uses the existing `roundTrip` helper in `pkg/model/cassandra/message_test.go`, same package):

```go
package cassandra

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMessageRoom_JSON(t *testing.T) {
	sub := true
	r := MessageRoom{
		ID:   "room-1",
		Name: "prj-alpha",
		Type: RoomType("channel"),
		HRInfo: &MessageHRInfo{
			Account: "alice", ChineseName: "爱丽丝", EngName: "Alice",
		},
		AppInfo: &MessageAppInfo{
			ID: "app-1", Name: "MyApp", AssistantName: "bot-my-app", IsSubscribed: &sub,
		},
	}
	roundTrip(t, r)
}

func TestMessageRoom_JSON_IDTypeOnly(t *testing.T) {
	got := roundTrip(t, MessageRoom{ID: "dm-1", Type: RoomType("dm")})
	assert.Empty(t, got.Name)
	assert.Nil(t, got.HRInfo)
	assert.Nil(t, got.AppInfo)

	// dm/botDM shape omits name/hrInfo/appInfo keys entirely on the wire.
	b, err := json.Marshal(MessageRoom{ID: "dm-1", Type: RoomType("dm")})
	require.NoError(t, err)
	assert.JSONEq(t, `{"id":"dm-1","type":"dm"}`, string(b))
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=pkg/model`
Expected: FAIL to compile — `undefined: MessageRoom` (and friends) in `pkg/model/cassandra`.

- [ ] **Step 3: Create `pkg/model/cassandra/enrich.go` with the moved types**

Copy the three structs verbatim from `pkg/model/search.go` (lines 54–91) and add `RoomType`. The bodies must stay byte-identical (json tags unchanged); only the doc comments note the move:

```go
package cassandra

// RoomType is the room classification shared by Mongo room/subscription
// documents and enrichment carriers. Canonical constants (RoomTypeChannel,
// RoomTypeDM, RoomTypeBotDM, RoomTypeDiscussion) live in pkg/model/room.go;
// the type itself lives here so ForwardedMessage.Room can reference it
// without an import cycle (pkg/model imports this package).
type RoomType string

// MessageRoom is the enriched room object attached to a SearchMessage and to
// a ForwardedMessage snapshot. Type is the room type. HRInfo is set only for
// dm rooms; AppInfo only for botDM rooms (search enrichment only — forwarded
// snapshots never set either). Name is the app name (botDM), the
// counterpart's display name (dm), or the canonical room name
// (channel/discussion). On a forwarded snapshot, dm/botDM sources carry only
// ID and Type.
type MessageRoom struct {
	ID      string          `json:"id"`
	Name    string          `json:"name,omitempty"`
	Type    RoomType        `json:"type,omitempty"`
	HRInfo  *MessageHRInfo  `json:"hrInfo,omitempty"`
	AppInfo *MessageAppInfo `json:"appInfo,omitempty"`
}

// MessageHRInfo is the compact HR record on search sender/room objects.
type MessageHRInfo struct {
	Account     string `json:"account"`
	ChineseName string `json:"chineseName,omitempty"`
	EngName     string `json:"engName,omitempty"`
}

// MessageAppInfo is the compact app record on search sender/room objects.
// IsSubscribed is set only on room.appInfo (botDM) — explicit true/false from
// the caller's subscription row — and stays nil (absent) on sender.appInfo.
type MessageAppInfo struct {
	ID            string `json:"id"`
	Name          string `json:"name"`
	AssistantName string `json:"assistantName"`
	IsSubscribed  *bool  `json:"isSubscribed,omitempty"`
}
```

In `pkg/model/search.go`, replace the three struct definitions (lines 54–91) with aliases (keep the file's existing `import` of the cassandra package if present; add it otherwise — `"github.com/hmchangw/chat/pkg/model/cassandra"`):

```go
// MessageRoom, MessageHRInfo, MessageAppInfo live in pkg/model/cassandra so
// the ForwardedMessage snapshot can embed MessageRoom without an import
// cycle; aliased here to keep the enrichment API in pkg/model.
type MessageRoom = cassandra.MessageRoom
type MessageHRInfo = cassandra.MessageHRInfo
type MessageAppInfo = cassandra.MessageAppInfo
```

In `pkg/model/room.go`, replace `type RoomType string` with (add the cassandra import):

```go
// RoomType lives in pkg/model/cassandra (see MessageRoom); aliased here so
// consumers keep using model.RoomType and the constants below.
type RoomType = cassandra.RoomType
```

The constant block (`RoomTypeChannel`, `RoomTypeDM`, `RoomTypeBotDM`, `RoomTypeDiscussion`) stays in `pkg/model/room.go` unchanged.

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=pkg/model`
Expected: PASS (new tests + existing `model_test.go` round-trips still green).

- [ ] **Step 5: Verify the whole repo still compiles (aliases did their job)**

Run: `make lint`
Expected: clean — search-service, room-service, user-service etc. compile against the aliases with zero changes. If any package fails to compile, fix by adjusting the alias placement — NOT by editing consumers.

- [ ] **Step 6: Commit**

```bash
git add pkg/model/cassandra/enrich.go pkg/model/cassandra/enrich_test.go pkg/model/search.go pkg/model/room.go
git commit -m "refactor(model): move MessageRoom carriers into pkg/model/cassandra with aliases"
```

---

### Task 2: Transient `Room` field on `cassandra.ForwardedMessage`

**Files:**
- Modify: `pkg/model/cassandra/message.go` (ForwardedMessage struct, lines 72–81)
- Modify: `pkg/model/cassandra/enrich_test.go` (add round-trip)

**Interfaces:**
- Consumes: Task 1's `cassandra.MessageRoom`.
- Produces: `ForwardedMessage.Room *MessageRoom` (`json:"room,omitempty" cql:"-"`) — Task 4 stamps this field.

- [ ] **Step 1: Write the failing test**

Append to `pkg/model/cassandra/enrich_test.go`:

```go
func TestForwardedMessage_JSON_WithRoom(t *testing.T) {
	fm := ForwardedMessage{
		MessageID: "m-src",
		RoomID:    "room-src",
		Sender:    Participant{ID: "u1", Account: "alice"},
		Msg:       "hello",
		Room:      &MessageRoom{ID: "room-src", Name: "prj-alpha", Type: RoomType("channel")},
	}
	got := roundTrip(t, fm)
	require.NotNil(t, got.Room)
	assert.Equal(t, "prj-alpha", got.Room.Name)
}

func TestForwardedMessage_JSON_RoomOmittedWhenNil(t *testing.T) {
	b, err := json.Marshal(ForwardedMessage{MessageID: "m-src", RoomID: "room-src"})
	require.NoError(t, err)
	assert.NotContains(t, string(b), `"room"`)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=pkg/model`
Expected: FAIL to compile — `unknown field Room in struct literal`.

- [ ] **Step 3: Add the field**

In `pkg/model/cassandra/message.go`, add as the last field of `ForwardedMessage` (after `ThreadParentCreatedAt`):

```go
	// Room is read-time enrichment of RoomID (name/type resolved from the
	// local rooms collection by history-service read paths); transient
	// (cql:"-"), never persisted into the UDT. dm/botDM sources carry only
	// ID and Type. Omitted when the room could not be resolved.
	Room *MessageRoom `json:"room,omitempty" cql:"-"`
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=pkg/model`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/model/cassandra/message.go pkg/model/cassandra/enrich_test.go
git commit -m "feat(model): transient Room enrichment field on ForwardedMessage"
```

---

### Task 3: `mongorepo.GetRoomsNameType` batched lookup

**Files:**
- Modify: `history-service/internal/mongorepo/room.go`
- Modify: `history-service/internal/mongorepo/room_test.go` (integration, `//go:build integration` — already tagged)

**Interfaces:**
- Consumes: existing `RoomRepo` / `mongoutil.Collection[model.Room].FindMany` / `mongoutil.WithProjection`.
- Produces: `mongorepo.RoomNameType{Name string; Type model.RoomType}` and `func (r *RoomRepo) GetRoomsNameType(ctx context.Context, roomIDs []string) (map[string]RoomNameType, error)` — Task 4's interface method must match this signature exactly.

- [ ] **Step 1: Write the failing integration test**

Append to `history-service/internal/mongorepo/room_test.go` (same package, integration-tagged; `setupMongo(t)` already exists there):

```go
func TestRoomRepo_GetRoomsNameType(t *testing.T) {
	db := setupMongo(t)
	repo := NewRoomRepo(db)
	ctx := context.Background()

	rooms := []any{
		model.Room{ID: "nt-ch-1", Name: "prj-alpha", Type: model.RoomTypeChannel, SiteID: "site-A", CreatedAt: time.Now()},
		model.Room{ID: "nt-dm-1", Name: "bob", Type: model.RoomTypeDM, SiteID: "site-A", CreatedAt: time.Now()},
	}
	_, err := db.Collection("rooms").InsertMany(ctx, rooms)
	require.NoError(t, err)

	got, err := repo.GetRoomsNameType(ctx, []string{"nt-ch-1", "nt-dm-1", "nt-missing"})
	require.NoError(t, err)

	assert.Len(t, got, 2) // missing ID absent from the map, not an error
	assert.Equal(t, RoomNameType{Name: "prj-alpha", Type: model.RoomTypeChannel}, got["nt-ch-1"])
	assert.Equal(t, RoomNameType{Name: "bob", Type: model.RoomTypeDM}, got["nt-dm-1"])
	_, ok := got["nt-missing"]
	assert.False(t, ok)
}

func TestRoomRepo_GetRoomsNameType_EmptyInput(t *testing.T) {
	db := setupMongo(t)
	repo := NewRoomRepo(db)

	got, err := repo.GetRoomsNameType(context.Background(), nil)
	require.NoError(t, err)
	assert.Empty(t, got)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test-integration SERVICE=history-service/internal/mongorepo`
Expected: FAIL to compile — `undefined: RoomNameType` / `repo.GetRoomsNameType undefined`.

- [ ] **Step 3: Implement**

Append to `history-service/internal/mongorepo/room.go`:

```go
// RoomNameType is the projected name/type row returned by GetRoomsNameType.
type RoomNameType struct {
	Name string
	Type model.RoomType
}

// GetRoomsNameType returns name/type for each existing room in roomIDs;
// absent IDs are simply missing from the map (not an error). Backs the
// forwarded-message room enrichment on history read paths.
func (r *RoomRepo) GetRoomsNameType(ctx context.Context, roomIDs []string) (map[string]RoomNameType, error) {
	out := make(map[string]RoomNameType, len(roomIDs))
	if len(roomIDs) == 0 {
		return out, nil
	}
	rooms, err := r.rooms.FindMany(ctx,
		bson.M{"_id": bson.M{"$in": roomIDs}},
		mongoutil.WithProjection(bson.M{"name": 1, "type": 1}),
	)
	if err != nil {
		return nil, fmt.Errorf("get rooms name/type: %w", err)
	}
	for i := range rooms {
		out[rooms[i].ID] = RoomNameType{Name: rooms[i].Name, Type: rooms[i].Type}
	}
	return out, nil
}
```

(`_id` rides along in the projection by default — required for keying the map.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test-integration SERVICE=history-service/internal/mongorepo`
Expected: PASS (requires Docker; starts the shared Mongo testcontainer).

- [ ] **Step 5: Commit**

```bash
git add history-service/internal/mongorepo/room.go history-service/internal/mongorepo/room_test.go
git commit -m "feat(history-service): batched rooms name/type lookup for forward enrichment"
```

---

### Task 4: `enrichForwardedRooms` helper wired into `LoadHistory`

**Files:**
- Modify: `history-service/internal/service/service.go` (RoomRepository interface, lines 64–68)
- Create: `history-service/internal/service/enrich_forward.go`
- Create: `history-service/internal/service/enrich_forward_test.go`
- Modify: `history-service/internal/service/messages.go` (`LoadHistory`, before the `return` at line 96)
- Regenerate: `history-service/internal/service/mocks/mock_repository.go` (via `make generate` — never edit manually)

**Interfaces:**
- Consumes: Task 2's `ForwardedMessage.Room`; Task 3's `GetRoomsNameType` + `mongorepo.RoomNameType`.
- Produces: `func (s *HistoryService) enrichForwardedRooms(ctx context.Context, slices ...[]models.Message)` — Task 5 calls this from the remaining read paths. Variadic so thread reads pass replies and parent in one lookup.

- [ ] **Step 1: Extend the RoomRepository interface (needed for mocks before tests compile)**

In `history-service/internal/service/service.go`, add to the `RoomRepository` interface (after `GetRoomUserCount`):

```go
	// GetRoomsNameType returns name/type for the given room IDs; absent IDs
	// are missing from the map. Backs forwarded-message room enrichment.
	GetRoomsNameType(ctx context.Context, roomIDs []string) (map[string]mongorepo.RoomNameType, error)
```

(`mongorepo` is already imported in service.go. The compile-time check `var _ RoomRepository = (*mongorepo.RoomRepo)(nil)` at the bottom of the file will pass because Task 3 implemented the method.)

Run: `make generate SERVICE=history-service`
Expected: `mocks/mock_repository.go` gains `MockRoomRepository.GetRoomsNameType`.

- [ ] **Step 2: Write the failing tests**

Create `history-service/internal/service/enrich_forward_test.go`:

```go
package service_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/history-service/internal/mongorepo"
	"github.com/hmchangw/chat/pkg/model"
)

func fwdMsg(id, srcRoomID string, at time.Time) models.Message {
	return models.Message{
		MessageID: id, RoomID: "r1", CreatedAt: at,
		ForwardedMessage: &models.ForwardedMessage{
			MessageID: "src-" + id, RoomID: srcRoomID, Msg: "src body",
		},
	}
}

func TestLoadHistory_EnrichesForwardedRooms(t *testing.T) {
	svc, msgs, subs, rooms, _, _, _, _ := newServiceWithRoomMock(t)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)

	messages := []models.Message{
		fwdMsg("m1", "src-ch", joinTime.Add(3*time.Minute)),
		fwdMsg("m2", "src-dm", joinTime.Add(2*time.Minute)),
		{MessageID: "m3", RoomID: "r1", CreatedAt: joinTime.Add(time.Minute)}, // no forward
	}
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).
		Return(makePage(messages, false), nil)

	// One batched lookup over the DISTINCT source-room IDs.
	rooms.EXPECT().
		GetRoomsNameType(gomock.Any(), gomock.InAnyOrder([]string{"src-ch", "src-dm"})).
		Return(map[string]mongorepo.RoomNameType{
			"src-ch": {Name: "prj-alpha", Type: model.RoomTypeChannel},
			"src-dm": {Name: "bob", Type: model.RoomTypeDM},
		}, nil)

	resp, err := svc.LoadHistory(c, models.LoadHistoryRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Messages, 3)

	chRoom := resp.Messages[0].ForwardedMessage.Room
	require.NotNil(t, chRoom)
	assert.Equal(t, "src-ch", chRoom.ID)
	assert.Equal(t, "prj-alpha", chRoom.Name)
	assert.Equal(t, model.RoomTypeChannel, chRoom.Type)

	// dm source: id + type ONLY — the counterpart name must not leak.
	dmRoom := resp.Messages[1].ForwardedMessage.Room
	require.NotNil(t, dmRoom)
	assert.Equal(t, "src-dm", dmRoom.ID)
	assert.Equal(t, model.RoomTypeDM, dmRoom.Type)
	assert.Empty(t, dmRoom.Name)
	assert.Nil(t, dmRoom.HRInfo)
	assert.Nil(t, dmRoom.AppInfo)

	assert.Nil(t, resp.Messages[2].ForwardedMessage)
}

func TestLoadHistory_ForwardEnrichment_DedupesRoomIDs(t *testing.T) {
	svc, msgs, subs, rooms, _, _, _, _ := newServiceWithRoomMock(t)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	messages := []models.Message{
		fwdMsg("m1", "src-ch", joinTime.Add(2*time.Minute)),
		fwdMsg("m2", "src-ch", joinTime.Add(time.Minute)),
	}
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).
		Return(makePage(messages, false), nil)

	// Exactly ONE lookup with the single distinct ID.
	rooms.EXPECT().
		GetRoomsNameType(gomock.Any(), []string{"src-ch"}).
		Return(map[string]mongorepo.RoomNameType{"src-ch": {Name: "prj-alpha", Type: model.RoomTypeChannel}}, nil).
		Times(1)

	resp, err := svc.LoadHistory(c, models.LoadHistoryRequest{})
	require.NoError(t, err)
	assert.Equal(t, "prj-alpha", resp.Messages[0].ForwardedMessage.Room.Name)
	assert.Equal(t, "prj-alpha", resp.Messages[1].ForwardedMessage.Room.Name)
}

func TestLoadHistory_ForwardEnrichment_BestEffortOnError(t *testing.T) {
	svc, msgs, subs, rooms, _, _, _, _ := newServiceWithRoomMock(t)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	messages := []models.Message{fwdMsg("m1", "src-ch", joinTime.Add(time.Minute))}
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).
		Return(makePage(messages, false), nil)
	rooms.EXPECT().GetRoomsNameType(gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("mongo down"))

	resp, err := svc.LoadHistory(c, models.LoadHistoryRequest{})
	require.NoError(t, err) // read never fails on enrichment
	assert.Nil(t, resp.Messages[0].ForwardedMessage.Room)
}

func TestLoadHistory_ForwardEnrichment_RoomMissing(t *testing.T) {
	svc, msgs, subs, rooms, _, _, _, _ := newServiceWithRoomMock(t)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	messages := []models.Message{fwdMsg("m1", "src-gone", joinTime.Add(time.Minute))}
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).
		Return(makePage(messages, false), nil)
	rooms.EXPECT().GetRoomsNameType(gomock.Any(), []string{"src-gone"}).
		Return(map[string]mongorepo.RoomNameType{}, nil) // room deleted

	resp, err := svc.LoadHistory(c, models.LoadHistoryRequest{})
	require.NoError(t, err)
	assert.Nil(t, resp.Messages[0].ForwardedMessage.Room) // omitted, client falls back to roomId
}

func TestLoadHistory_NoForwards_NoLookup(t *testing.T) {
	svc, msgs, subs, rooms, _, _, _, _ := newServiceWithRoomMock(t)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	messages := []models.Message{{MessageID: "m1", RoomID: "r1", CreatedAt: joinTime.Add(time.Minute)}}
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).
		Return(makePage(messages, false), nil)
	// NO GetRoomsNameType expectation: zero forwards must cost zero lookups
	// (gomock fails the test on an unexpected call).

	_, err := svc.LoadHistory(c, models.LoadHistoryRequest{})
	require.NoError(t, err)
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `make test SERVICE=history-service/internal/service`
Expected: The five new tests FAIL — `Room` stays nil (helper doesn't exist yet, nothing stamps it) and the mock expectations for `GetRoomsNameType` go unmet. (Compilation succeeds because Step 1 regenerated the mock.)

- [ ] **Step 4: Implement the helper**

Create `history-service/internal/service/enrich_forward.go`:

```go
package service

import (
	"context"
	"log/slog"

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
)

// enrichForwardedRooms stamps ForwardedMessage.Room (id/name/type from the
// local rooms collection) onto every forwarded snapshot in the given message
// slices, with one batched lookup across all distinct source-room IDs.
// Variadic so callers with several slices (thread replies + parent) share the
// lookup. Best-effort: a lookup error or a missing room doc logs one warning
// and leaves Room nil — a read is never failed by enrichment. dm/botDM
// sources get ID+Type only (the counterpart's name must not leak to readers
// outside the source room).
func (s *HistoryService) enrichForwardedRooms(ctx context.Context, slices ...[]models.Message) {
	var snaps []*models.ForwardedMessage
	idSet := map[string]struct{}{}
	for _, msgs := range slices {
		for i := range msgs {
			fm := msgs[i].ForwardedMessage
			if fm == nil || fm.RoomID == "" {
				continue
			}
			snaps = append(snaps, fm)
			idSet[fm.RoomID] = struct{}{}
		}
	}
	if len(snaps) == 0 {
		return
	}
	ids := make([]string, 0, len(idSet))
	for id := range idSet {
		ids = append(ids, id)
	}

	rooms, err := s.rooms.GetRoomsNameType(ctx, ids)
	if err != nil {
		slog.Warn("forwarded room enrichment lookup failed",
			"error", err, "request_id", natsutil.RequestIDFromContext(ctx), "room_count", len(ids))
		return
	}
	if missing := len(ids) - len(rooms); missing > 0 {
		slog.Warn("forwarded room enrichment: rooms not found",
			"request_id", natsutil.RequestIDFromContext(ctx), "missing", missing)
	}
	for _, fm := range snaps {
		nt, ok := rooms[fm.RoomID]
		if !ok {
			continue // room deleted — leave Room unset, client falls back to RoomID
		}
		room := &model.MessageRoom{ID: fm.RoomID, Type: nt.Type}
		if nt.Type != model.RoomTypeDM && nt.Type != model.RoomTypeBotDM {
			room.Name = nt.Name
		}
		fm.Room = room
	}
}
```

Wire into `LoadHistory` in `history-service/internal/service/messages.go` — insert directly after the existing `setDecodedAttachments(c, page.Data)` line (line 95):

```go
	s.enrichForwardedRooms(c, page.Data)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `make test SERVICE=history-service/internal/service`
Expected: PASS — all five new tests plus every pre-existing service test (no forwards in old fixtures → no unexpected mock calls).

- [ ] **Step 6: Commit**

```bash
git add history-service/internal/service/service.go history-service/internal/service/enrich_forward.go history-service/internal/service/enrich_forward_test.go history-service/internal/service/messages.go history-service/internal/service/mocks/mock_repository.go
git commit -m "feat(history-service): forwarded-room enrichment helper, wired into LoadHistory"
```

---

### Task 5: Wire the remaining read paths

**Files:**
- Modify: `history-service/internal/service/messages.go` (`LoadNextMessages` ~line 154, `assembleSurrounding` ~line 376, `loadSurroundingByMessageID` single-message early-return ~line 224, `GetMessageByID` ~line 436, `GetMessagesByIDs` ~line 480)
- Modify: `history-service/internal/service/pin.go` (`ListPinnedMessages` ~line 210)
- Modify: `history-service/internal/service/threads.go` (`GetThreadMessages` ~line 140, `GetThreadParentMessages` ~line 375)
- Modify: `history-service/internal/service/enrich_forward_test.go` (per-path tests)

**Interfaces:**
- Consumes: Task 4's `enrichForwardedRooms(ctx, slices ...[]models.Message)`.
- Produces: nothing new — behavioral completion of the read-path matrix.

- [ ] **Step 1: Write the failing tests**

Append to `history-service/internal/service/enrich_forward_test.go`. Pattern per path: stub access + page read returning one `fwdMsg`, expect one `GetRoomsNameType` call, assert `Room` stamped. Single-message paths reuse the pointer-sharing property (`ForwardedMessage` is a pointer, so enriching a copied `models.Message` stamps the shared snapshot).

```go
func expectSrcChLookup(rooms *mocks.MockRoomRepository) {
	rooms.EXPECT().
		GetRoomsNameType(gomock.Any(), []string{"src-ch"}).
		Return(map[string]mongorepo.RoomNameType{"src-ch": {Name: "prj-alpha", Type: model.RoomTypeChannel}}, nil)
}

func TestLoadNextMessages_EnrichesForwardedRooms(t *testing.T) {
	svc, msgs, subs, rooms, _, _, _, _ := newServiceWithRoomMock(t)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessagesAfter(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{fwdMsg("m1", "src-ch", joinTime.Add(time.Minute))}, false), nil)
	expectSrcChLookup(rooms)

	resp, err := svc.LoadNextMessages(c, models.LoadNextMessagesRequest{})
	require.NoError(t, err)
	require.NotNil(t, resp.Messages[0].ForwardedMessage.Room)
	assert.Equal(t, "prj-alpha", resp.Messages[0].ForwardedMessage.Room.Name)
}

func TestGetMessageByID_EnrichesForwardedRoom(t *testing.T) {
	svc, msgs, subs, rooms, _, _, _, _ := newServiceWithRoomMock(t)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	m := fwdMsg("m1", "src-ch", joinTime.Add(time.Minute))
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(&m, nil)
	expectSrcChLookup(rooms)

	resp, err := svc.GetMessageByID(c, models.GetMessageByIDRequest{MessageID: "m1"})
	require.NoError(t, err)
	require.NotNil(t, resp.ForwardedMessage.Room)
	assert.Equal(t, "prj-alpha", resp.ForwardedMessage.Room.Name)
}

func TestGetMessagesByIDs_EnrichesForwardedRooms(t *testing.T) {
	svc, msgs, subs, rooms, _, _, _, _ := newServiceWithRoomMock(t)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessagesByIDs(gomock.Any(), []string{"m1"}).
		Return([]models.Message{fwdMsg("m1", "src-ch", joinTime.Add(time.Minute))}, nil)
	expectSrcChLookup(rooms)

	resp, err := svc.GetMessagesByIDs(c, models.GetMessagesByIDsRequest{MessageIDs: []string{"m1"}})
	require.NoError(t, err)
	require.NotNil(t, resp.Messages[0].ForwardedMessage.Room)
}

func TestListPinnedMessages_EnrichesForwardedRooms(t *testing.T) {
	svc, msgs, subs, rooms, _, _, _, _ := newServiceWithRoomMock(t)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	pinned := fwdMsg("m1", "src-ch", joinTime.Add(time.Minute))
	pinned.PinnedAt = ptrTime(joinTime.Add(2 * time.Minute))
	msgs.EXPECT().GetPinnedMessages(gomock.Any(), "r1", gomock.Any()).
		Return(makePage([]models.Message{pinned}, false), nil)
	expectSrcChLookup(rooms)

	resp, err := svc.ListPinnedMessages(c, models.ListPinnedMessagesRequest{Limit: 10})
	require.NoError(t, err)
	require.NotNil(t, resp.Messages[0].ForwardedMessage.Room)
}
```

Also append these three (surrounding + thread paths — the thread test puts the forward on the PARENT message, verifying the variadic parent+replies call shares one lookup):

```go
func TestLoadSurroundingMessages_EnrichesForwardedRooms(t *testing.T) {
	svc, msgs, subs, rooms, _, _, _, _ := newServiceWithRoomMock(t)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	central := fwdMsg("m-central", "src-ch", joinTime.Add(10*time.Minute))
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m-central").Return(&central, nil)
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, central.CreatedAt, gomock.Any()).
		Return(makePage(nil, false), nil)
	msgs.EXPECT().GetMessagesAfter(gomock.Any(), "r1", central.CreatedAt, gomock.Any(), gomock.Any()).
		Return(makePage(nil, false), nil)
	expectSrcChLookup(rooms)

	resp, err := svc.LoadSurroundingMessages(c, models.LoadSurroundingMessagesRequest{MessageID: "m-central", Limit: 5})
	require.NoError(t, err)
	require.Len(t, resp.Messages, 1) // the spliced central message
	require.NotNil(t, resp.Messages[0].ForwardedMessage.Room)
	assert.Equal(t, "prj-alpha", resp.Messages[0].ForwardedMessage.Room.Name)
}

func TestGetThreadMessages_EnrichesForwardedParent(t *testing.T) {
	svc, msgs, subs, rooms, _, threadRooms, _, _ := newServiceWithRoomMock(t)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	threadRooms.EXPECT().GetMinThreadUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	c := testContext()

	parent := fwdMsg("m-parent", "src-ch", joinTime.Add(5*time.Minute))
	parent.ThreadRoomID = "tr-1"
	parent.TCount = intPtr(1)
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m-parent").Return(&parent, nil)
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	replies := []models.Message{{
		MessageID: "reply-1", RoomID: "r1", ThreadRoomID: "tr-1",
		ThreadParentID: "m-parent", CreatedAt: parent.CreatedAt.Add(time.Minute),
	}}
	msgs.EXPECT().GetThreadMessages(gomock.Any(), "tr-1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage(replies, false), nil)
	expectSrcChLookup(rooms) // exactly ONE lookup shared by replies + parent

	resp, err := svc.GetThreadMessages(c, models.GetThreadMessagesRequest{ThreadMessageID: "m-parent"})
	require.NoError(t, err)
	require.NotNil(t, resp.ParentMessage.ForwardedMessage.Room)
	assert.Equal(t, "prj-alpha", resp.ParentMessage.ForwardedMessage.Room.Name)
}

func TestGetThreadParentMessages_EnrichesForwardedRooms(t *testing.T) {
	svc, msgs, subs, rooms, _, threadRooms, _, _ := newServiceWithRoomMock(t)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	threadRooms.EXPECT().GetThreadRooms(gomock.Any(), "r1", nil, gomock.Any()).Return(makeThreadPage(2), nil)
	// makeThreadPage's rows reference parent IDs "p1"/"p2" (threads_test.go).
	p1 := fwdMsg("p1", "src-ch", joinTime.Add(time.Minute))
	p2 := models.Message{MessageID: "p2", RoomID: "r1", CreatedAt: joinTime.Add(2 * time.Minute)}
	msgs.EXPECT().GetMessagesByIDs(gomock.Any(), gomock.Any()).Return([]models.Message{p1, p2}, nil)
	expectSrcChLookup(rooms)

	resp, err := svc.GetThreadParentMessages(c, models.GetThreadParentMessagesRequest{Filter: models.ThreadFilterAll, Limit: 20})
	require.NoError(t, err)
	require.Len(t, resp.ParentMessages, 2)
	require.NotNil(t, resp.ParentMessages[0].ForwardedMessage.Room)
	assert.Equal(t, "prj-alpha", resp.ParentMessages[0].ForwardedMessage.Room.Name)
	assert.Nil(t, resp.ParentMessages[1].ForwardedMessage)
}
```

(`fwdMsg` fixes `RoomID: "r1"` — required, since these paths drop cross-room messages. `makeThreadPage`, `intPtr` already exist in `threads_test.go`, same package.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=history-service/internal/service`
Expected: the new per-path tests FAIL (Room nil / unmet mock expectations); Task 4's LoadHistory tests still PASS.

- [ ] **Step 3: Wire the call sites**

Insert after each path's `setDecodedAttachments(...)` call:

- `messages.go` `LoadNextMessages` (after line 154): `s.enrichForwardedRooms(c, page.Data)`
- `messages.go` `assembleSurrounding` (after line 376): `s.enrichForwardedRooms(c, messages)` — covers both surrounding modes including the spliced central message.
- `messages.go` `loadSurroundingByMessageID` limit-1 early return (after `decodeMessageAttachments(c, &only)` at line 224): `s.enrichForwardedRooms(c, []models.Message{only})`
- `messages.go` `GetMessageByID` (after line 436): `s.enrichForwardedRooms(c, []models.Message{*msg})` — pointer-shared snapshot gets stamped.
- `messages.go` `GetMessagesByIDs` (after line 480): `s.enrichForwardedRooms(c, kept)`
- `pin.go` `ListPinnedMessages` (after line 210): `s.enrichForwardedRooms(c, page.Data)` — AFTER `redactUnavailablePins`, so a redacted pin (ForwardedMessage already nil'd) is never looked up.
- `threads.go` `GetThreadMessages` (after line 140): `s.enrichForwardedRooms(c, page.Data, []models.Message{*msg})` — replies + parent share one lookup.
- `threads.go` `GetThreadParentMessages` (after line 375): `s.enrichForwardedRooms(c, parentMessages)`

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=history-service/internal/service`
Expected: PASS — all new per-path tests and the whole existing suite.

- [ ] **Step 5: Run the full history-service unit suite**

Run: `make test SERVICE=history-service`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add history-service/internal/service/messages.go history-service/internal/service/pin.go history-service/internal/service/threads.go history-service/internal/service/enrich_forward_test.go
git commit -m "feat(history-service): forwarded-room enrichment on all message read paths"
```

---

### Task 6: Client API docs

**Files:**
- Modify: `docs/client-api.md` (ForwardedMessage schema table at line ~2820)

**Interfaces:**
- Consumes: the wire shape shipped in Tasks 2/4/5.
- Produces: documented contract for FE.

- [ ] **Step 1: Update the ForwardedMessage schema table**

In `docs/client-api.md` under `##### ForwardedMessage` (line ~2820), append one row to the field table:

```markdown
| `room` | [MessageRoom](#messageroom) | Optional. Read-time enrichment of `roomId`, resolved server-side on the history read paths (`msg.history` / `msg.next` / `msg.surrounding` / `msg.get` / `msg.get.ids` / `msg.pinned.list` / thread reads) — NOT set on the `msg.send` echo or on broadcast events. Best-effort: omitted when the source room could not be resolved (fall back to `roomId`). Channel/discussion sources carry `id`+`name`+`type`; `dm`/`botDM` sources carry `id`+`type` only (never `name`/`hrInfo`/`appInfo`). Names resolve at read time, so a rename is reflected on the next read. |
```

And extend the section's intro paragraph with one sentence:

```markdown
On history reads the snapshot additionally carries a best-effort `room` object resolved from `roomId` at read time (see the field note).
```

The derived views (`docs/client-api/request-reply.md` line 2053, `docs/client-api/events.md` line 337) link to this canonical `#forwardedmessage` section rather than duplicating the table — verify with `grep -n "room" docs/client-api/request-reply.md docs/client-api/events.md` that neither view carries its own copy of the ForwardedMessage field table; no edit needed there if they only link. The existing `#messageroom` anchor (line ~4043, search section) is the link target — append one sentence to that section:

```markdown
On a `forwardedMessage.room` (history reads), `dm`/`botDM` rooms carry only `id` and `type`.
```

- [ ] **Step 2: Update the msg.get / msg.history response examples**

Find the Load History success-response JSON example (section starting line ~2850) and add a forwarded message variant to the example, or extend the existing example message list with:

```json
{
  "messageId": "aB3dE5fG7hJ9kL1mN0pQ",
  "roomId": "dest-room",
  "sender": {"id": "u1", "account": "alice"},
  "createdAt": "2026-08-06T09:00:00Z",
  "msg": "check this out",
  "forwardedMessage": {
    "messageId": "zY8xW6vU4tS2rQ0pN9mL",
    "roomId": "src-room",
    "sender": {"id": "u2", "account": "bob"},
    "createdAt": "2026-08-01T12:00:00Z",
    "msg": "original text",
    "room": {"id": "src-room", "name": "prj-alpha", "type": "channel"}
  }
}
```

(Match the surrounding example's field style/IDs — adapt cosmetically, keep the `room` object exactly as shown.)

- [ ] **Step 3: Commit**

```bash
git add docs/client-api.md
git commit -m "docs(client-api): forwardedMessage.room enrichment on history reads"
```

---

### Task 7: Full verification & push

**Files:** none (verification only)

- [ ] **Step 1: Regenerate + full unit suite**

Run: `make generate && git diff --exit-code` (mocks in sync) then `make test`
Expected: no diff; all unit tests PASS with race detector.

- [ ] **Step 2: Lint**

Run: `make lint`
Expected: clean.

- [ ] **Step 3: Integration tests for the touched service**

Run: `make test-integration SERVICE=history-service`
Expected: PASS (Docker required).

- [ ] **Step 4: Coverage check on touched packages**

Run: `go test -coverprofile=/tmp/claude-0/-home-user-newchat/a010f769-2dbf-58d2-bcfa-fdd63f7130dc/scratchpad/cov.out ./history-service/internal/service/... && go tool cover -func=/tmp/claude-0/-home-user-newchat/a010f769-2dbf-58d2-bcfa-fdd63f7130dc/scratchpad/cov.out | tail -5`
(Exception to the make-only rule: coverage verification is the documented `go test -coverprofile` flow from CLAUDE.md §4 Coverage.)
Expected: total ≥80%; `enrich_forward.go` functions ≥90%.

- [ ] **Step 5: SAST**

Run: `make sast`
Expected: clean (no medium+ findings).

- [ ] **Step 6: Push**

```bash
git push -u origin claude/message-room-enrichment-krp5m3
```

(On network failure retry up to 4 times with 2s/4s/8s/16s backoff.)

---

## Self-Review Notes

- **Spec coverage:** type moves + aliases (Task 1), transient field (Task 2), batched projected lookup (Task 3), best-effort helper + LoadHistory (Task 4), all remaining read paths incl. thread parent + surrounding central + pinned-after-redaction ordering (Task 5), docs (Task 6), quality gates (Task 7). Out-of-scope paths (send echo, broadcast, search, rooms.get) are documented as NOT enriched in Task 6's doc row.
- **Type consistency:** `GetRoomsNameType(ctx, []string) (map[string]mongorepo.RoomNameType, error)` identical in Task 3 (impl), Task 4 (interface + mocks + test stubs), Task 5 (test stubs). `enrichForwardedRooms(ctx, slices ...[]models.Message)` identical in Tasks 4/5.
- **Ordering constraint:** in `ListPinnedMessages` the enrichment call must come after `redactUnavailablePins` (which nils `ForwardedMessage` on redacted pins) so redacted snapshots are never enriched or leaked.
