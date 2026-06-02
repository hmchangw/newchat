# Pin / Unpin Message History — Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add pin, unpin, and list-pinned message RPCs to history-service, gated by a global Mongo kill-switch plus subscription/large-room authorization, persisted to Cassandra, emitting new best-effort canonical events.

**Architecture:** New handlers in `history-service/internal/service/pin.go` mirror the existing edit/delete handlers. Pin state is written to `messages_by_id` (pinned_at/pinned_by) and a denormalized copy in `pinned_messages_by_room`, with the load-bearing invariant `messages_by_id.pinned_at == pinned_messages_by_room.created_at == pinnedAt`. Authorization reads a new `settings` Mongo collection (kill-switch, fail-open), the caller's subscription (roles), and the room's userCount (large-room override mirroring message-gatekeeper).

**Tech Stack:** Go 1.25, NATS (natsrouter), MongoDB (`pkg/mongoutil`), Cassandra (gocql via `cassrepo`), `go.uber.org/mock`, testify, testcontainers.

**Scope:** Phase 1 only (history-service vertical). Phases 2–4 (broadcast-worker, search-sync-worker, room-service) are separate plans. Reference spec: `docs/superpowers/specs/2026-05-19-pin-unpin-message-history-design.md`.

**Conventions for every task:** Use `make` targets, never raw `go`. Unit tests: `make test SERVICE=history-service`. Integration tests: `make test-integration SERVICE=history-service`. Lint: `make lint`. Regenerate mocks: `make generate SERVICE=history-service`. A pre-commit hook runs lint+tests; fix failures before retrying. All commits go to branch `claude/pin-unpin-history-k8p88`.

---

## Task 1: Canonical event constants + pinned fields on `model.Message`

**Files:**
- Modify: `pkg/model/event.go:9-13` (add two `EventType` constants)
- Modify: `pkg/model/message.go:9-25` (add two fields to `Message`)
- Test: `pkg/model/model_test.go`

- [ ] **Step 1: Write the failing test**

Add to `pkg/model/model_test.go` (new top-level test function):

```go
func TestEventPinnedConstants(t *testing.T) {
	if model.EventPinned != "pinned" {
		t.Errorf("EventPinned = %q, want %q", model.EventPinned, "pinned")
	}
	if model.EventUnpinned != "unpinned" {
		t.Errorf("EventUnpinned = %q, want %q", model.EventUnpinned, "unpinned")
	}
}

func TestMessagePinnedFieldsRoundTrip(t *testing.T) {
	at := time.Date(2026, 5, 19, 12, 0, 0, 0, time.UTC)
	src := model.Message{
		ID:       "m1",
		RoomID:   "r1",
		PinnedAt: &at,
		PinnedBy: &model.Participant{Account: "alice"},
	}
	b, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got model.Message
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.PinnedAt == nil || !got.PinnedAt.Equal(at) {
		t.Errorf("PinnedAt round-trip failed: got %v", got.PinnedAt)
	}
	if got.PinnedBy == nil || got.PinnedBy.Account != "alice" {
		t.Errorf("PinnedBy round-trip failed: got %v", got.PinnedBy)
	}
}
```

If `pkg/model/model_test.go` does not already import `encoding/json` and `time`, add them to its import block.

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=model` (or `cd` not needed — Makefile handles it; if the model package has no SERVICE target, run `make test` and locate the `pkg/model` failure).
Expected: FAIL — `model.EventPinned` / `model.Message.PinnedAt` undefined (compile error).

- [ ] **Step 3: Add the constants**

In `pkg/model/event.go`, change the const block (currently lines 9-13):

```go
const (
	EventCreated  EventType = "created"
	EventUpdated  EventType = "updated"
	EventDeleted  EventType = "deleted"
	EventPinned   EventType = "pinned"
	EventUnpinned EventType = "unpinned"
)
```

- [ ] **Step 4: Add the fields**

In `pkg/model/message.go`, append these two fields to the end of the `Message` struct (after `QuotedParentMessage`, before the closing `}`):

```go
	PinnedAt                     *time.Time                     `json:"pinnedAt,omitempty"                     bson:"pinnedAt,omitempty"`
	PinnedBy                     *Participant                   `json:"pinnedBy,omitempty"                     bson:"pinnedBy,omitempty"`
```

`pkg/model/message.go` already imports `time` (existing `time.Time` fields) and `Participant` is defined in the same package — no new imports.

- [ ] **Step 5: Run tests to verify they pass**

Run: `make test`
Expected: PASS (the two new tests plus the existing `pkg/model` suite).

- [ ] **Step 6: Commit**

```bash
git add pkg/model/event.go pkg/model/message.go pkg/model/model_test.go
git commit -m "feat(model): add EventPinned/EventUnpinned and Message pinned fields"
```

---

## Task 2: Subject builders for pin/unpin/list + canonical pinned/unpinned

**Files:**
- Modify: `pkg/subject/subject.go` (add 5 builder funcs near `MsgDeletePattern` ~line 322 and `MsgCanonicalDeleted` ~line 160)
- Test: `pkg/subject/subject_test.go`

- [ ] **Step 1: Write the failing test**

In `pkg/subject/subject_test.go`, add these rows to the existing table literal (the `tests := []struct{ ... }{ ... }` block that contains the `MsgEditPattern`/`MsgDeletePattern` entries near line 101):

```go
		{"MsgPinPattern", subject.MsgPinPattern("site-a"),
			"chat.user.{account}.request.room.{roomID}.site-a.msg.pin"},
		{"MsgUnpinPattern", subject.MsgUnpinPattern("site-a"),
			"chat.user.{account}.request.room.{roomID}.site-a.msg.unpin"},
		{"MsgPinnedListPattern", subject.MsgPinnedListPattern("site-a"),
			"chat.user.{account}.request.room.{roomID}.site-a.msg.pinned.list"},
		{"MsgCanonicalPinned", subject.MsgCanonicalPinned("site-a"),
			"chat.msg.canonical.site-a.pinned"},
		{"MsgCanonicalUnpinned", subject.MsgCanonicalUnpinned("site-a"),
			"chat.msg.canonical.site-a.unpinned"},
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test`
Expected: FAIL — `subject.MsgPinPattern` undefined (compile error in `pkg/subject`).

- [ ] **Step 3: Add the builders**

In `pkg/subject/subject.go`, immediately after the `MsgCanonicalDeleted` function (ends ~line 162), add:

```go
func MsgCanonicalPinned(siteID string) string {
	return fmt.Sprintf("chat.msg.canonical.%s.pinned", siteID)
}

func MsgCanonicalUnpinned(siteID string) string {
	return fmt.Sprintf("chat.msg.canonical.%s.unpinned", siteID)
}
```

Immediately after the `MsgDeletePattern` function (ends ~line 324), add:

```go
// MsgPinPattern is the natsrouter pattern for pinning a message.
// The {account} and {roomID} placeholders are extracted by natsrouter.
func MsgPinPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.room.{roomID}.%s.msg.pin", siteID)
}

// MsgUnpinPattern is the natsrouter pattern for unpinning a message.
func MsgUnpinPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.room.{roomID}.%s.msg.unpin", siteID)
}

// MsgPinnedListPattern is the natsrouter pattern for listing a room's pinned messages.
func MsgPinnedListPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.room.{roomID}.%s.msg.pinned.list", siteID)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/subject/subject.go pkg/subject/subject_test.go
git commit -m "feat(subject): add pin/unpin/list and canonical pinned subjects"
```

---

## Task 3: `LARGE_ROOM_THRESHOLD` config

**Files:**
- Modify: `history-service/internal/config/config.go` (add field to `Config`)
- Modify: `history-service/cmd/main.go` (validate + log)

> No unit test: this project's `config` package has no existing test and the value is exercised end-to-end via Task 9 handler tests. Validation mirrors the existing `MessageBucketHours < 1` guard.

- [ ] **Step 1: Add the config field**

In `history-service/internal/config/config.go`, add to the `Config` struct (after `MessageHistoryFloorDays`):

```go
	LargeRoomThreshold      int             `env:"LARGE_ROOM_THRESHOLD"       envDefault:"500"`
```

- [ ] **Step 2: Validate and log in main.go**

In `history-service/cmd/main.go`, after the existing `if cfg.MessageHistoryFloorDays < 1 { ... }` block, add:

```go
	if cfg.LargeRoomThreshold < 1 {
		slog.Error("invalid config", "LARGE_ROOM_THRESHOLD", cfg.LargeRoomThreshold)
		os.Exit(1)
	}
```

Then add `"largeRoomThreshold", cfg.LargeRoomThreshold,` to the existing `slog.Info("message bucket configured", ...)` call's key-value list.

- [ ] **Step 3: Verify it builds**

Run: `make build SERVICE=history-service`
Expected: builds with no error.

- [ ] **Step 4: Commit**

```bash
git add history-service/internal/config/config.go history-service/cmd/main.go
git commit -m "feat(history-service): add LARGE_ROOM_THRESHOLD config"
```

---

## Task 4: `SettingsRepo` (Mongo kill-switch, fail-open)

**Files:**
- Create: `history-service/internal/mongorepo/settings.go`
- Test (integration): `history-service/internal/mongorepo/settings_test.go`

The well-known doc is `{ _id: "global", pinEnabled: <bool> }` in collection `settings`. Absent doc ⇒ `(true, nil)` (fail-open). A driver error ⇒ wrapped error.

- [ ] **Step 1: Write the failing integration test**

Create `history-service/internal/mongorepo/settings_test.go`:

```go
//go:build integration

package mongorepo

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestSettingsRepo_PinEnabled_DocAbsent_FailsOpen(t *testing.T) {
	db := setupMongo(t)
	repo := NewSettingsRepo(db)

	enabled, err := repo.PinEnabled(context.Background())

	require.NoError(t, err)
	assert.True(t, enabled, "absent settings doc must fail open (enabled)")
}

func TestSettingsRepo_PinEnabled_True(t *testing.T) {
	db := setupMongo(t)
	_, err := db.Collection("settings").InsertOne(context.Background(),
		bson.M{"_id": "global", "pinEnabled": true})
	require.NoError(t, err)

	repo := NewSettingsRepo(db)
	enabled, err := repo.PinEnabled(context.Background())

	require.NoError(t, err)
	assert.True(t, enabled)
}

func TestSettingsRepo_PinEnabled_False(t *testing.T) {
	db := setupMongo(t)
	_, err := db.Collection("settings").InsertOne(context.Background(),
		bson.M{"_id": "global", "pinEnabled": false})
	require.NoError(t, err)

	repo := NewSettingsRepo(db)
	enabled, err := repo.PinEnabled(context.Background())

	require.NoError(t, err)
	assert.False(t, enabled)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test-integration SERVICE=history-service`
Expected: FAIL — `NewSettingsRepo` undefined (compile error).

- [ ] **Step 3: Implement the repo**

Create `history-service/internal/mongorepo/settings.go`:

```go
package mongorepo

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/mongoutil"
)

const settingsCollection = "settings"

// globalSettingsID is the _id of the single well-known global settings document.
const globalSettingsID = "global"

// globalSettings is the storage shape of the kill-switch document.
type globalSettings struct {
	ID         string `bson:"_id"`
	PinEnabled bool   `bson:"pinEnabled"`
}

// SettingsRepo reads global runtime settings from MongoDB. Writes are owned by
// ops/another service and are intentionally not implemented here.
type SettingsRepo struct {
	settings *mongoutil.Collection[globalSettings]
}

func NewSettingsRepo(db *mongo.Database) *SettingsRepo {
	return &SettingsRepo{
		settings: mongoutil.NewCollection[globalSettings](db.Collection(settingsCollection)),
	}
}

// PinEnabled reports whether pinning is globally enabled. A missing document
// fails open (returns true) — absence of the kill-switch is not an outage.
// Only a driver error is surfaced.
func (r *SettingsRepo) PinEnabled(ctx context.Context) (bool, error) {
	doc, err := r.settings.FindOne(ctx,
		bson.M{"_id": globalSettingsID},
		mongoutil.WithProjection(bson.M{"pinEnabled": 1, "_id": 0}),
	)
	if err != nil {
		return false, fmt.Errorf("read global settings: %w", err)
	}
	if doc == nil {
		return true, nil
	}
	return doc.PinEnabled, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test-integration SERVICE=history-service`
Expected: PASS for the three `TestSettingsRepo_*` tests.

- [ ] **Step 5: Commit**

```bash
git add history-service/internal/mongorepo/settings.go history-service/internal/mongorepo/settings_test.go
git commit -m "feat(history-service): add SettingsRepo kill-switch with fail-open"
```

---

## Task 5: `RoomRepo.GetRoomUserCount` + `SubscriptionRepo.GetSubscription` exposure

**Files:**
- Modify: `history-service/internal/mongorepo/room.go` (add `GetRoomUserCount`)
- Test (integration): `history-service/internal/mongorepo/room_test.go`

> `SubscriptionRepo.GetSubscription` already exists in `history-service/internal/mongorepo/subscription.go` (returns `(*model.Subscription, error)`, `nil` when not subscribed). No code change there — it is wired into the service interface in Task 6.

- [ ] **Step 1: Write the failing integration test**

Append to `history-service/internal/mongorepo/room_test.go`:

```go
func TestRoomRepo_GetRoomUserCount(t *testing.T) {
	db := setupMongo(t)
	_, err := db.Collection("rooms").InsertOne(context.Background(),
		bson.M{"_id": "r1", "userCount": 42})
	require.NoError(t, err)

	repo := NewRoomRepo(db)
	count, err := repo.GetRoomUserCount(context.Background(), "r1")

	require.NoError(t, err)
	assert.Equal(t, 42, count)
}

func TestRoomRepo_GetRoomUserCount_RoomMissing(t *testing.T) {
	db := setupMongo(t)
	repo := NewRoomRepo(db)

	_, err := repo.GetRoomUserCount(context.Background(), "missing")

	require.Error(t, err)
}
```

If `room_test.go` does not already import `context`, `bson`, `assert`, `require`, add them (match the imports already used by `settings_test.go` / existing room tests).

- [ ] **Step 2: Run test to verify it fails**

Run: `make test-integration SERVICE=history-service`
Expected: FAIL — `repo.GetRoomUserCount` undefined.

- [ ] **Step 3: Implement `GetRoomUserCount`**

Append to `history-service/internal/mongorepo/room.go`:

```go
// GetRoomUserCount returns the room's userCount via a projected findOne.
// Returns mongo.ErrNoDocuments wrapped when the room does not exist —
// callers treat that as an infrastructure error (reaching this call already
// implies the caller is subscribed to the room).
func (r *RoomRepo) GetRoomUserCount(ctx context.Context, roomID string) (int, error) {
	room, err := r.rooms.FindByID(ctx, roomID, mongoutil.WithProjection(bson.M{"userCount": 1, "_id": 0}))
	if err != nil {
		return 0, fmt.Errorf("get room %s userCount: %w", roomID, err)
	}
	if room == nil {
		return 0, fmt.Errorf("get room %s userCount: %w", roomID, mongo.ErrNoDocuments)
	}
	return room.UserCount, nil
}
```

`room.go` already imports `fmt`, `bson`, `mongo`, `mongoutil` and `model.Room` has `UserCount int` (`pkg/model/room.go:20`) — no new imports.

- [ ] **Step 4: Run test to verify it passes**

Run: `make test-integration SERVICE=history-service`
Expected: PASS for both new tests.

- [ ] **Step 5: Commit**

```bash
git add history-service/internal/mongorepo/room.go history-service/internal/mongorepo/room_test.go
git commit -m "feat(history-service): add RoomRepo.GetRoomUserCount"
```

---

## Task 6: Service interfaces, constructor, mocks, test harness

**Files:**
- Modify: `history-service/internal/service/service.go` (interfaces, struct, `New`, mockgen directive)
- Modify: `history-service/internal/service/mocks/mock_repository.go` (regenerated — never hand-edit)
- Modify: `history-service/internal/service/messages_test.go` (`newServiceWithRoomMock`, `newService`)
- Modify: `history-service/cmd/main.go` (construct `SettingsRepo`, pass to `service.New`)

This task wires dependencies but adds no behavior, so it has no dedicated test; it is "green" when the existing suite compiles and passes with the new signature.

- [ ] **Step 1: Extend interfaces and constructor**

In `history-service/internal/service/service.go`:

Add a method to `SubscriptionRepository`:

```go
type SubscriptionRepository interface {
	GetHistorySharedSince(ctx context.Context, account, roomID string) (*time.Time, bool, error)
	GetSubscription(ctx context.Context, account, roomID string) (*model.Subscription, error)
}
```

Add a method to `RoomRepository`:

```go
type RoomRepository interface {
	GetMinUserLastSeenAt(ctx context.Context, roomID string) (*time.Time, error)
	GetRoomTimes(ctx context.Context, roomID string) (lastMsgAt, createdAt time.Time, err error)
	GetRoomUserCount(ctx context.Context, roomID string) (int, error)
}
```

Add new interfaces and writer/reader methods:

```go
// SettingsRepository reads the global pin kill-switch. Fail-open is the
// implementation's responsibility (see mongorepo.SettingsRepo).
type SettingsRepository interface {
	PinEnabled(ctx context.Context) (bool, error)
}
```

Add to `MessageWriter`:

```go
	PinMessage(ctx context.Context, msg *models.Message, pinnedAt time.Time, pinnedBy models.Participant) error
	UnpinMessage(ctx context.Context, msg *models.Message) error
```

Add to `MessageReader`:

```go
	GetPinnedMessages(ctx context.Context, roomID string, pageReq cassrepo.PageRequest) (cassrepo.Page[models.Message], error)
```

Update the mockgen directive line (top of file) to include `SettingsRepository`:

```go
//go:generate mockgen -destination=mocks/mock_repository.go -package=mocks . MessageReader,MessageWriter,MessageRepository,SubscriptionRepository,RoomRepository,SettingsRepository,EventPublisher,ThreadRoomRepository
```

Add a field to `HistoryService` and a constructor param:

```go
type HistoryService struct {
	msgReader          MessageReader
	msgWriter          MessageWriter
	subscriptions      SubscriptionRepository
	rooms              RoomRepository
	settings           SettingsRepository
	publisher          EventPublisher
	threadRooms        ThreadRoomRepository
	historyFloor       time.Duration
	largeRoomThreshold int
}

func New(
	msgs MessageRepository,
	subs SubscriptionRepository,
	rooms RoomRepository,
	settings SettingsRepository,
	pub EventPublisher,
	threadRooms ThreadRoomRepository,
	historyFloor time.Duration,
	largeRoomThreshold int,
) *HistoryService {
	return &HistoryService{
		msgReader:          msgs,
		msgWriter:          msgs,
		subscriptions:      subs,
		rooms:              rooms,
		settings:           settings,
		publisher:          pub,
		threadRooms:        threadRooms,
		historyFloor:       historyFloor,
		largeRoomThreshold: largeRoomThreshold,
	}
}
```

Add a compile-time check next to the existing ones:

```go
var _ SettingsRepository = (*mongorepo.SettingsRepo)(nil)
```

- [ ] **Step 2: Regenerate mocks**

Run: `make generate SERVICE=history-service`
Expected: `mocks/mock_repository.go` now contains `MockSettingsRepository` and the new methods on `MockSubscriptionRepository`, `MockRoomRepository`, `MockMessageRepository`. Do not hand-edit.

- [ ] **Step 3: Update the test harness**

In `history-service/internal/service/messages_test.go`, replace `newServiceWithRoomMock` so it constructs and returns the settings mock and passes the new `service.New` args. The new body:

```go
func newServiceWithRoomMock(t *testing.T) (*service.HistoryService, *mocks.MockMessageRepository, *mocks.MockSubscriptionRepository, *mocks.MockRoomRepository, *mocks.MockSettingsRepository, *mocks.MockEventPublisher, *mocks.MockThreadRoomRepository) {
	ctrl := gomock.NewController(t)
	msgs := mocks.NewMockMessageRepository(ctrl)
	subs := mocks.NewMockSubscriptionRepository(ctrl)
	rooms := mocks.NewMockRoomRepository(ctrl)
	settings := mocks.NewMockSettingsRepository(ctrl)
	pub := mocks.NewMockEventPublisher(ctrl)
	threadRooms := mocks.NewMockThreadRoomRepository(ctrl)
	rooms.EXPECT().
		GetRoomTimes(gomock.Any(), gomock.Any()).
		Return(defaultRoomLastMsgAt, defaultRoomCreatedAt, nil).
		MinTimes(0)
	// Permissive default: existing edit/delete/history tests don't exercise
	// the kill-switch. Pin/unpin tests override with a stricter expectation.
	settings.EXPECT().PinEnabled(gomock.Any()).Return(true, nil).AnyTimes()
	// Permissive default: only the large-room override path reads userCount.
	rooms.EXPECT().GetRoomUserCount(gomock.Any(), gomock.Any()).Return(0, nil).AnyTimes()
	const historyFloor = 90 * 24 * time.Hour
	return service.New(msgs, subs, rooms, settings, pub, threadRooms, historyFloor, 500), msgs, subs, rooms, settings, pub, threadRooms
}
```

Update `newService` to thread the extra return value through (drop the room+settings mocks for callers that don't need them):

```go
func newService(t *testing.T) (*service.HistoryService, *mocks.MockMessageRepository, *mocks.MockSubscriptionRepository, *mocks.MockEventPublisher, *mocks.MockThreadRoomRepository) {
	svc, msgs, subs, rooms, _, pub, threadRooms := newServiceWithRoomMock(t)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	return svc, msgs, subs, pub, threadRooms
}
```

Then fix every other caller of `newServiceWithRoomMock` in the `service` test package: the call now yields 7 values `(svc, msgs, subs, rooms, settings, pub, threadRooms)` instead of 6. For existing callers that don't need `settings`, assign it to `_`. Search with: `grep -rn "newServiceWithRoomMock(t)" history-service/internal/service/` and update each destructuring assignment to insert one extra `_` (or a named variable) in the 5th position (after `rooms`, before `pub`).

- [ ] **Step 4: Update main.go wiring**

In `history-service/cmd/main.go`, after `roomRepo := mongorepo.NewRoomRepo(db)` add:

```go
	settingsRepo := mongorepo.NewSettingsRepo(db)
```

Change the `svc := service.New(...)` call to:

```go
	svc := service.New(cassRepo, subRepo, roomRepo, settingsRepo, pub, threadRoomRepo, historyFloor, cfg.LargeRoomThreshold)
```

- [ ] **Step 5: Run the full unit suite**

Run: `make test SERVICE=history-service`
Expected: PASS — existing edit/delete/history/thread tests still green with the new signature; nothing behavior-changed yet.

- [ ] **Step 6: Commit**

```bash
git add history-service/internal/service/service.go history-service/internal/service/mocks/mock_repository.go history-service/internal/service/messages_test.go history-service/cmd/main.go
git commit -m "refactor(history-service): wire SettingsRepository, GetSubscription, GetRoomUserCount, large-room threshold"
```

---

## Task 7: Cassandra `PinMessage` / `UnpinMessage` writes

**Files:**
- Modify: `history-service/internal/cassrepo/write.go` (add queries + methods)
- Modify: `history-service/internal/cassrepo/integration_test.go` (extend `pinned_messages_by_room` test DDL)
- Test (integration): `history-service/internal/cassrepo/pin_integration_test.go` (new)

**Invariant (load-bearing):** `PinMessage` writes the *same* `pinnedAt` value to `messages_by_id.pinned_at` and `pinned_messages_by_room.created_at`. The existing edit/delete mirror code (`write.go:83,97`) locates the pinned row by `*msg.PinnedAt`; divergence silently corrupts the pinned copy.

- [ ] **Step 1: Extend the integration test DDL**

In `history-service/internal/cassrepo/integration_test.go`, replace the `pinned_messages_by_room` `CREATE TABLE` block (currently the reduced column set) with the full denormalized schema this task reads/writes:

```go
	require.NoError(t, adminSession.Query(cql(`CREATE TABLE IF NOT EXISTS %s.pinned_messages_by_room (
		room_id TEXT,
		created_at TIMESTAMP,
		message_id TEXT,
		sender FROZEN<"Participant">,
		target_user FROZEN<"Participant">,
		msg TEXT,
		mentions SET<FROZEN<"Participant">>,
		attachments LIST<BLOB>,
		file FROZEN<"File">,
		card FROZEN<"Card">,
		card_action FROZEN<"CardAction">,
		quoted_parent_message FROZEN<"QuotedParentMessage">,
		visible_to TEXT,
		reactions MAP<TEXT, FROZEN<SET<FROZEN<"Participant">>>>,
		deleted BOOLEAN,
		type TEXT,
		sys_msg_data BLOB,
		site_id TEXT,
		edited_at TIMESTAMP,
		updated_at TIMESTAMP,
		pinned_by FROZEN<"Participant">,
		PRIMARY KEY ((room_id), created_at, message_id)
	) WITH CLUSTERING ORDER BY (created_at DESC, message_id DESC)`)).Exec())
```

- [ ] **Step 2: Write the failing integration test**

Create `history-service/internal/cassrepo/pin_integration_test.go`:

```go
//go:build integration

package cassrepo

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/msgbucket"
)

func seedMessage(t *testing.T, repo *Repository, m *models.Message) {
	t.Helper()
	b := msgbucket.New(24 * time.Hour).Of(m.CreatedAt)
	require.NoError(t, repo.session.Query(
		`INSERT INTO messages_by_id (message_id, room_id, created_at, sender, msg, deleted) VALUES (?, ?, ?, ?, ?, ?)`,
		m.MessageID, m.RoomID, m.CreatedAt, m.Sender, m.Msg, false,
	).WithContext(context.Background()).Exec())
	require.NoError(t, repo.session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg, deleted) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		m.RoomID, b, m.CreatedAt, m.MessageID, m.Sender, m.Msg, false,
	).WithContext(context.Background()).Exec())
}

func TestRepository_PinAndUnpinMessage(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365)
	ctx := context.Background()

	created := time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC)
	msg := &models.Message{
		MessageID: "m1", RoomID: "r1", CreatedAt: created,
		Sender: models.Participant{ID: "u1", Account: "alice"}, Msg: "hello",
	}
	seedMessage(t, repo, msg)

	pinnedAt := time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC)
	pinnedBy := models.Participant{ID: "u2", Account: "mod"}
	require.NoError(t, repo.PinMessage(ctx, msg, pinnedAt, pinnedBy))

	// messages_by_id carries the pin fields.
	got, err := repo.GetMessageByID(ctx, "m1")
	require.NoError(t, err)
	require.NotNil(t, got.PinnedAt)
	assert.True(t, got.PinnedAt.Equal(pinnedAt), "pinned_at must equal pinnedAt")
	require.NotNil(t, got.PinnedBy)
	assert.Equal(t, "mod", got.PinnedBy.Account)

	// pinned_messages_by_room has a row keyed by created_at == pinnedAt.
	page, err := repo.GetPinnedMessages(ctx, "r1", PageRequest{PageSize: 10})
	require.NoError(t, err)
	require.Len(t, page.Data, 1)
	assert.Equal(t, "m1", page.Data[0].MessageID)
	require.NotNil(t, page.Data[0].PinnedAt)
	assert.True(t, page.Data[0].PinnedAt.Equal(pinnedAt))
	require.NotNil(t, page.Data[0].PinnedBy)
	assert.Equal(t, "mod", page.Data[0].PinnedBy.Account)

	// Unpin clears messages_by_id and removes the pinned row.
	got.PinnedAt = &pinnedAt // emulate handler reading current pin time
	require.NoError(t, repo.UnpinMessage(ctx, got))

	after, err := repo.GetMessageByID(ctx, "m1")
	require.NoError(t, err)
	assert.Nil(t, after.PinnedAt)
	assert.Nil(t, after.PinnedBy)

	emptyPage, err := repo.GetPinnedMessages(ctx, "r1", PageRequest{PageSize: 10})
	require.NoError(t, err)
	assert.Empty(t, emptyPage.Data)
}

func TestRepository_GetPinnedMessages_OrderAndEmpty(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365)
	ctx := context.Background()

	empty, err := repo.GetPinnedMessages(ctx, "empty-room", PageRequest{PageSize: 10})
	require.NoError(t, err)
	assert.Empty(t, empty.Data)
	assert.False(t, empty.HasNext)

	base := time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC)
	for i, id := range []string{"a", "b", "c"} {
		m := &models.Message{
			MessageID: id, RoomID: "r1", CreatedAt: base,
			Sender: models.Participant{ID: "u1", Account: "alice"}, Msg: id,
		}
		seedMessage(t, repo, m)
		require.NoError(t, repo.PinMessage(ctx, m,
			base.Add(time.Duration(i)*time.Hour),
			models.Participant{ID: "u1", Account: "alice"}))
	}

	page, err := repo.GetPinnedMessages(ctx, "r1", PageRequest{PageSize: 10})
	require.NoError(t, err)
	require.Len(t, page.Data, 3)
	// CLUSTERING ORDER created_at DESC ⇒ most recently pinned first.
	assert.Equal(t, "c", page.Data[0].MessageID)
	assert.Equal(t, "a", page.Data[2].MessageID)
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `make test-integration SERVICE=history-service`
Expected: FAIL — `repo.PinMessage` / `repo.UnpinMessage` / `repo.GetPinnedMessages` undefined.

- [ ] **Step 4: Implement the writes and reader**

In `history-service/internal/cassrepo/write.go`, add to the existing first `const ( ... )` block (the one with `editMsgByID`):

```go
	pinMsgByID   = `UPDATE messages_by_id SET pinned_at = ?, pinned_by = ? WHERE message_id = ? AND created_at = ?`
	unpinMsgByID = `UPDATE messages_by_id SET pinned_at = null, pinned_by = null WHERE message_id = ? AND created_at = ?`

	insertPinnedMsg = `INSERT INTO pinned_messages_by_room (
		room_id, created_at, message_id, sender, target_user, msg, mentions,
		attachments, file, card, card_action, quoted_parent_message, visible_to,
		reactions, deleted, type, sys_msg_data, site_id, edited_at, updated_at, pinned_by
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	deletePinnedRow = `DELETE FROM pinned_messages_by_room WHERE room_id = ? AND created_at = ? AND message_id = ?`
)
```

(Place the closing `)` correctly — append these inside the existing block that currently ends after `deletePinnedMsg`.)

Add these methods to `write.go` (after `SoftDeleteMessage`, before `decrementParentTcount`):

```go
// PinMessage records a pin: it sets pinned_at/pinned_by on messages_by_id and
// inserts a denormalized copy into pinned_messages_by_room with
// created_at == pinnedAt. Invariant: messages_by_id.pinned_at and
// pinned_messages_by_room.created_at MUST be the same value — the edit/delete
// mirror code locates the pinned row by messages_by_id.pinned_at.
func (r *Repository) PinMessage(ctx context.Context, msg *models.Message, pinnedAt time.Time, pinnedBy models.Participant) error {
	if err := r.session.Query(
		pinMsgByID, pinnedAt, pinnedBy, msg.MessageID, msg.CreatedAt,
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("pin message %s in messages_by_id: %w", msg.MessageID, err)
	}
	if err := r.session.Query(
		insertPinnedMsg,
		msg.RoomID, pinnedAt, msg.MessageID, msg.Sender, msg.TargetUser, msg.Msg,
		msg.Mentions, msg.Attachments, msg.File, msg.Card, msg.CardAction,
		msg.QuotedParentMessage, msg.VisibleTo, msg.Reactions, msg.Deleted,
		msg.Type, msg.SysMsgData, msg.SiteID, msg.EditedAt, msg.UpdatedAt, pinnedBy,
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("insert pinned_messages_by_room for message %s in room %s: %w", msg.MessageID, msg.RoomID, err)
	}
	return nil
}

// UnpinMessage clears pinned_at/pinned_by on messages_by_id and deletes the
// pinned_messages_by_room row. msg.PinnedAt MUST be non-nil (the current pin
// time) — it is the row's clustering key. Callers guarantee this by
// short-circuiting when the message is not pinned.
func (r *Repository) UnpinMessage(ctx context.Context, msg *models.Message) error {
	if msg.PinnedAt == nil {
		return fmt.Errorf("unpin message %s: PinnedAt is nil", msg.MessageID)
	}
	if err := r.session.Query(
		unpinMsgByID, msg.MessageID, msg.CreatedAt,
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("unpin message %s in messages_by_id: %w", msg.MessageID, err)
	}
	if err := r.session.Query(
		deletePinnedRow, msg.RoomID, *msg.PinnedAt, msg.MessageID,
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("delete pinned_messages_by_room for message %s in room %s: %w", msg.MessageID, msg.RoomID, err)
	}
	return nil
}
```

Create `history-service/internal/cassrepo/pinned_messages_by_room.go`:

```go
package cassrepo

import (
	"context"
	"fmt"

	"github.com/gocql/gocql"

	"github.com/hmchangw/chat/history-service/internal/models"
)

// pinnedColumns is the denormalized column set of pinned_messages_by_room.
// created_at is the pin timestamp (not the message's original creation time).
const pinnedColumns = "room_id, created_at, message_id, sender, target_user, " +
	"msg, mentions, attachments, file, card, card_action, quoted_parent_message, " +
	"visible_to, reactions, deleted, type, sys_msg_data, site_id, edited_at, " +
	"updated_at, pinned_by"

const pinnedByRoomQuery = "SELECT " + pinnedColumns + " FROM pinned_messages_by_room WHERE room_id = ?"

// GetPinnedMessages returns a room's pinned messages, most-recently-pinned
// first (CLUSTERING ORDER created_at DESC), single-partition paginated via
// Cassandra page state. In each returned Message, PinnedAt is the pin time
// and CreatedAt also holds the pin time (denormalized-row artifact — the
// original creation time is not stored in pinned_messages_by_room; callers
// needing it use GetMessageByID).
func (r *Repository) GetPinnedMessages(ctx context.Context, roomID string, pageReq PageRequest) (Page[models.Message], error) {
	q := r.session.Query(pinnedByRoomQuery, roomID).WithContext(ctx)
	builder := NewQueryBuilder(q).WithPageSize(pageReq.PageSize)
	if pageReq.Cursor != nil {
		builder = builder.WithCursor(pageReq.Cursor)
	}

	var rows []models.Message
	next, err := builder.Fetch(func(iter *gocql.Iter) {
		for {
			var m models.Message
			if !structScan(iter, &m) {
				break
			}
			pinnedAt := m.CreatedAt
			m.PinnedAt = &pinnedAt
			rows = append(rows, m)
		}
	})
	if err != nil {
		return Page[models.Message]{}, fmt.Errorf("query pinned messages for room %s: %w", roomID, err)
	}
	if rows == nil {
		rows = []models.Message{}
	}
	return Page[models.Message]{Data: rows, NextCursor: next, HasNext: next != ""}, nil
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `make test-integration SERVICE=history-service`
Expected: PASS for `TestRepository_PinAndUnpinMessage` and `TestRepository_GetPinnedMessages_OrderAndEmpty`.

- [ ] **Step 6: Commit**

```bash
git add history-service/internal/cassrepo/write.go history-service/internal/cassrepo/pinned_messages_by_room.go history-service/internal/cassrepo/integration_test.go history-service/internal/cassrepo/pin_integration_test.go
git commit -m "feat(history-service): add Cassandra pin/unpin writes and pinned reader"
```

---

## Task 8: Request/response models

**Files:**
- Modify: `history-service/internal/models/message.go`

No dedicated test — these are plain DTOs exercised by Task 9. Add after the `DeleteMessageResponse` block:

- [ ] **Step 1: Add the structs**

```go
type PinMessageRequest struct {
	MessageID string `json:"messageId"`
}

type PinMessageResponse struct {
	MessageID string `json:"messageId"`
	PinnedAt  int64  `json:"pinnedAt"` // UTC millis
}

type UnpinMessageRequest struct {
	MessageID string `json:"messageId"`
}

type UnpinMessageResponse struct {
	MessageID string `json:"messageId"`
}

type ListPinnedMessagesRequest struct {
	Cursor string `json:"cursor,omitempty"`
	Limit  int    `json:"limit,omitempty"`
}

type ListPinnedMessagesResponse struct {
	Messages   []Message `json:"messages"`
	NextCursor string    `json:"nextCursor,omitempty"`
	HasNext    bool      `json:"hasNext"`
}
```

- [ ] **Step 2: Verify it builds**

Run: `make build SERVICE=history-service`
Expected: builds clean.

- [ ] **Step 3: Commit**

```bash
git add history-service/internal/models/message.go
git commit -m "feat(history-service): add pin/unpin/list request and response models"
```

---

## Task 9: Pin / Unpin / ListPinned handlers

**Files:**
- Create: `history-service/internal/service/pin.go`
- Modify: `history-service/internal/service/service.go` (`RegisterHandlers`)
- Test: `history-service/internal/service/pin_test.go`

Handler authorization order (pin & unpin identical): kill-switch → subscription → findMessage → `msg.Deleted ⇒ ErrNotFound` → large-room override → idempotent short-circuit → write → best-effort canonical publish. ListPinned: subscription only (no kill-switch, no large-room).

- [ ] **Step 1: Write the failing handler tests**

Create `history-service/internal/service/pin_test.go`:

```go
package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/history-service/internal/cassrepo"
	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/model"
)

func pinnableMsg() *models.Message {
	return &models.Message{
		MessageID: "m1", RoomID: "r1",
		CreatedAt: time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC),
		Sender:    models.Participant{ID: "u1", Account: "alice"},
		Msg:       "hello",
	}
}

func subFor(roles ...model.Role) *model.Subscription {
	return &model.Subscription{
		RoomID: "r1",
		User:   model.SubscriptionUser{ID: "u1", Account: "u1"},
		Roles:  roles,
	}
}

func TestPinMessage_HappyPath(t *testing.T) {
	svc, msgs, subs, rooms, settings, pub, _ := newServiceWithRoomMock(t)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	settings.EXPECT().PinEnabled(gomock.Any()).Return(true, nil)
	subs.EXPECT().GetSubscription(gomock.Any(), "u1", "r1").Return(subFor(model.RoleMember), nil)
	rooms.EXPECT().GetRoomUserCount(gomock.Any(), "r1").Return(10, nil)
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(pinnableMsg(), nil)
	msgs.EXPECT().PinMessage(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	var published model.MessageEvent
	pub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, data []byte) error {
			return json.Unmarshal(data, &published)
		})

	resp, err := svc.PinMessage(testContext(), "site-a", models.PinMessageRequest{MessageID: "m1"})

	require.NoError(t, err)
	assert.Equal(t, "m1", resp.MessageID)
	assert.NotZero(t, resp.PinnedAt)
	assert.Equal(t, model.EventPinned, published.Event)
	require.NotNil(t, published.Message.PinnedAt)
}

func TestPinMessage_KillSwitchDisabled(t *testing.T) {
	svc, _, _, rooms, settings, _, _ := newServiceWithRoomMock(t)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	settings.EXPECT().PinEnabled(gomock.Any()).Return(false, nil)

	_, err := svc.PinMessage(testContext(), "site-a", models.PinMessageRequest{MessageID: "m1"})

	assertForbiddenErr(t, err, "pinning is disabled")
}

func TestPinMessage_KillSwitchError(t *testing.T) {
	svc, _, _, rooms, settings, _, _ := newServiceWithRoomMock(t)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	settings.EXPECT().PinEnabled(gomock.Any()).Return(false, errors.New("mongo down"))

	_, err := svc.PinMessage(testContext(), "site-a", models.PinMessageRequest{MessageID: "m1"})

	assertInternalErr(t, err, "unable to verify pin setting")
}

func TestPinMessage_NotSubscribed(t *testing.T) {
	svc, _, subs, rooms, settings, _, _ := newServiceWithRoomMock(t)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	settings.EXPECT().PinEnabled(gomock.Any()).Return(true, nil)
	subs.EXPECT().GetSubscription(gomock.Any(), "u1", "r1").Return(nil, nil)

	_, err := svc.PinMessage(testContext(), "site-a", models.PinMessageRequest{MessageID: "m1"})

	assertForbiddenErr(t, err, "not subscribed to room")
}

func TestPinMessage_LargeRoomBlocksMember(t *testing.T) {
	svc, msgs, subs, rooms, settings, _, _ := newServiceWithRoomMock(t)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	settings.EXPECT().PinEnabled(gomock.Any()).Return(true, nil)
	subs.EXPECT().GetSubscription(gomock.Any(), "u1", "r1").Return(subFor(model.RoleMember), nil)
	rooms.EXPECT().GetRoomUserCount(gomock.Any(), "r1").Return(501, nil)
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(pinnableMsg(), nil)

	_, err := svc.PinMessage(testContext(), "site-a", models.PinMessageRequest{MessageID: "m1"})

	assertForbiddenErr(t, err, "room is too large to pin")
}

func TestPinMessage_LargeRoomOwnerBypass(t *testing.T) {
	svc, msgs, subs, rooms, settings, pub, _ := newServiceWithRoomMock(t)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	settings.EXPECT().PinEnabled(gomock.Any()).Return(true, nil)
	subs.EXPECT().GetSubscription(gomock.Any(), "u1", "r1").Return(subFor(model.RoleOwner), nil)
	rooms.EXPECT().GetRoomUserCount(gomock.Any(), "r1").Return(9999, nil)
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(pinnableMsg(), nil)
	msgs.EXPECT().PinMessage(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	pub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	_, err := svc.PinMessage(testContext(), "site-a", models.PinMessageRequest{MessageID: "m1"})

	require.NoError(t, err)
}

func TestPinMessage_DeletedMessageNotFound(t *testing.T) {
	svc, msgs, subs, rooms, settings, _, _ := newServiceWithRoomMock(t)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	settings.EXPECT().PinEnabled(gomock.Any()).Return(true, nil)
	subs.EXPECT().GetSubscription(gomock.Any(), "u1", "r1").Return(subFor(model.RoleMember), nil)
	rooms.EXPECT().GetRoomUserCount(gomock.Any(), "r1").Return(1, nil)
	deleted := pinnableMsg()
	deleted.Deleted = true
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(deleted, nil)

	_, err := svc.PinMessage(testContext(), "site-a", models.PinMessageRequest{MessageID: "m1"})

	assertNotFoundErr(t, err, "message not found")
}

func TestPinMessage_IdempotentAlreadyPinned(t *testing.T) {
	svc, msgs, subs, rooms, settings, pub, _ := newServiceWithRoomMock(t)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	settings.EXPECT().PinEnabled(gomock.Any()).Return(true, nil)
	subs.EXPECT().GetSubscription(gomock.Any(), "u1", "r1").Return(subFor(model.RoleMember), nil)
	rooms.EXPECT().GetRoomUserCount(gomock.Any(), "r1").Return(1, nil)
	already := pinnableMsg()
	at := time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC)
	already.PinnedAt = &at
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(already, nil)
	// No PinMessage, no Publish expected.

	resp, err := svc.PinMessage(testContext(), "site-a", models.PinMessageRequest{MessageID: "m1"})

	require.NoError(t, err)
	assert.Equal(t, at.UnixMilli(), resp.PinnedAt)
}

func TestUnpinMessage_HappyPath(t *testing.T) {
	svc, msgs, subs, rooms, settings, pub, _ := newServiceWithRoomMock(t)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	settings.EXPECT().PinEnabled(gomock.Any()).Return(true, nil)
	subs.EXPECT().GetSubscription(gomock.Any(), "u1", "r1").Return(subFor(model.RoleMember), nil)
	rooms.EXPECT().GetRoomUserCount(gomock.Any(), "r1").Return(1, nil)
	pinned := pinnableMsg()
	at := time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC)
	pinned.PinnedAt = &at
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(pinned, nil)
	msgs.EXPECT().UnpinMessage(gomock.Any(), pinned).Return(nil)

	var published model.MessageEvent
	pub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, data []byte) error {
			return json.Unmarshal(data, &published)
		})

	resp, err := svc.UnpinMessage(testContext(), "site-a", models.UnpinMessageRequest{MessageID: "m1"})

	require.NoError(t, err)
	assert.Equal(t, "m1", resp.MessageID)
	assert.Equal(t, model.EventUnpinned, published.Event)
}

func TestUnpinMessage_IdempotentNotPinned(t *testing.T) {
	svc, msgs, subs, rooms, settings, _, _ := newServiceWithRoomMock(t)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	settings.EXPECT().PinEnabled(gomock.Any()).Return(true, nil)
	subs.EXPECT().GetSubscription(gomock.Any(), "u1", "r1").Return(subFor(model.RoleMember), nil)
	rooms.EXPECT().GetRoomUserCount(gomock.Any(), "r1").Return(1, nil)
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(pinnableMsg(), nil)
	// No UnpinMessage, no Publish expected.

	resp, err := svc.UnpinMessage(testContext(), "site-a", models.UnpinMessageRequest{MessageID: "m1"})

	require.NoError(t, err)
	assert.Equal(t, "m1", resp.MessageID)
}

func TestListPinnedMessages_HappyPath(t *testing.T) {
	svc, msgs, subs, _, _, _, _ := newServiceWithRoomMock(t)
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	page := cassrepo.Page[models.Message]{
		Data:       []models.Message{*pinnableMsg()},
		NextCursor: "abc",
		HasNext:    true,
	}
	msgs.EXPECT().GetPinnedMessages(gomock.Any(), "r1", gomock.Any()).Return(page, nil)

	resp, err := svc.ListPinnedMessages(testContext(), models.ListPinnedMessagesRequest{Limit: 10})

	require.NoError(t, err)
	require.Len(t, resp.Messages, 1)
	assert.Equal(t, "abc", resp.NextCursor)
	assert.True(t, resp.HasNext)
}

func TestListPinnedMessages_NotSubscribed(t *testing.T) {
	svc, _, subs, _, _, _, _ := newServiceWithRoomMock(t)
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, false, nil)

	_, err := svc.ListPinnedMessages(testContext(), models.ListPinnedMessagesRequest{})

	assertForbiddenErr(t, err, "not subscribed to room")
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=history-service`
Expected: FAIL — `svc.PinMessage` / `svc.UnpinMessage` / `svc.ListPinnedMessages` undefined.

- [ ] **Step 3: Implement the handlers**

Create `history-service/internal/service/pin.go`:

```go
package service

import (
	"context"
	"log/slog"
	"regexp"
	"time"

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/subject"
)

// botPattern matches account names treated as bots. Mirrors
// message-gatekeeper/helper.go and room-service/helper.go — keep the three
// copies in sync if this regex changes (promotion to a shared package is a
// deliberate future cleanup, out of scope here).
var botPattern = regexp.MustCompile(`\.bot$|^p_`)

func isBot(account string) bool { return botPattern.MatchString(account) }

// canBypassLargeRoomPin reports whether the subscriber is exempt from the
// large-room pin restriction. Owners, admins, and bot accounts bypass.
func canBypassLargeRoomPin(sub *model.Subscription) bool {
	for _, r := range sub.Roles {
		if r == model.RoleOwner || r == model.RoleAdmin {
			return true
		}
	}
	return isBot(sub.User.Account)
}

// pinAuthorize runs the shared pin/unpin gate: kill-switch, subscription,
// findMessage, deleted check, large-room override. Returns the resolved
// message and subscription on success.
func (s *HistoryService) pinAuthorize(ctx context.Context, account, roomID, messageID string) (*models.Message, *model.Subscription, error) {
	enabled, err := s.settings.PinEnabled(ctx)
	if err != nil {
		slog.Error("check pin setting", "error", err, "roomID", roomID)
		return nil, nil, natsrouter.ErrInternal("unable to verify pin setting")
	}
	if !enabled {
		return nil, nil, natsrouter.ErrForbidden("pinning is disabled")
	}

	sub, err := s.subscriptions.GetSubscription(ctx, account, roomID)
	if err != nil {
		slog.Error("get subscription", "error", err, "account", account, "roomID", roomID)
		return nil, nil, natsrouter.ErrInternal("unable to verify room access")
	}
	if sub == nil {
		return nil, nil, natsrouter.ErrForbidden("not subscribed to room")
	}

	msg, err := s.findMessage(ctx, roomID, messageID)
	if err != nil {
		return nil, nil, err
	}
	if msg.Deleted {
		return nil, nil, natsrouter.ErrNotFound("message not found")
	}

	if !canBypassLargeRoomPin(sub) {
		count, err := s.rooms.GetRoomUserCount(ctx, roomID)
		if err != nil {
			slog.Error("get room user count", "error", err, "roomID", roomID)
			return nil, nil, natsrouter.ErrInternal("unable to verify room size")
		}
		if count > s.largeRoomThreshold {
			slog.Info("pin blocked: large room",
				"account", account, "roomID", roomID,
				"userCount", count, "threshold", s.largeRoomThreshold)
			return nil, nil, natsrouter.ErrForbidden("room is too large to pin")
		}
	}
	return msg, sub, nil
}

func pinnedByParticipant(sub *model.Subscription) models.Participant {
	return models.Participant{ID: sub.User.ID, Account: sub.User.Account}
}

// PinMessage handles chat.user.{account}.request.room.{roomID}.{siteID}.msg.pin.
func (s *HistoryService) PinMessage(c *natsrouter.Context, siteID string, req models.PinMessageRequest) (*models.PinMessageResponse, error) {
	account := c.Param("account")
	roomID := c.Param("roomID")

	msg, sub, err := s.pinAuthorize(c, account, roomID, req.MessageID)
	if err != nil {
		return nil, err
	}

	// Idempotent: already pinned ⇒ echo existing pinnedAt, no re-publish.
	if msg.PinnedAt != nil {
		return &models.PinMessageResponse{MessageID: msg.MessageID, PinnedAt: msg.PinnedAt.UnixMilli()}, nil
	}

	pinnedAt := time.Now().UTC()
	pinnedBy := pinnedByParticipant(sub)
	if err := s.msgWriter.PinMessage(c, msg, pinnedAt, pinnedBy); err != nil {
		slog.Error("pin: write", "error", err, "messageID", req.MessageID)
		return nil, natsrouter.ErrInternal("failed to pin message")
	}

	pinnedAtMs := pinnedAt.UnixMilli()
	evt := model.MessageEvent{
		Event: model.EventPinned,
		Message: model.Message{
			ID:          msg.MessageID,
			RoomID:      msg.RoomID,
			UserID:      msg.Sender.ID,
			UserAccount: msg.Sender.Account,
			CreatedAt:   msg.CreatedAt,
			PinnedAt:    &pinnedAt,
			PinnedBy:    &model.Participant{UserID: sub.User.ID, Account: sub.User.Account},
		},
		SiteID:    siteID,
		Timestamp: pinnedAtMs,
	}
	s.publishCanonicalBestEffort(c, subject.MsgCanonicalPinned(siteID), &evt, msg.MessageID, roomID)

	return &models.PinMessageResponse{MessageID: msg.MessageID, PinnedAt: pinnedAtMs}, nil
}

// UnpinMessage handles chat.user.{account}.request.room.{roomID}.{siteID}.msg.unpin.
func (s *HistoryService) UnpinMessage(c *natsrouter.Context, siteID string, req models.UnpinMessageRequest) (*models.UnpinMessageResponse, error) {
	account := c.Param("account")
	roomID := c.Param("roomID")

	msg, sub, err := s.pinAuthorize(c, account, roomID, req.MessageID)
	if err != nil {
		return nil, err
	}

	// Idempotent: not pinned ⇒ success no-op.
	if msg.PinnedAt == nil {
		return &models.UnpinMessageResponse{MessageID: msg.MessageID}, nil
	}

	if err := s.msgWriter.UnpinMessage(c, msg); err != nil {
		slog.Error("unpin: write", "error", err, "messageID", req.MessageID)
		return nil, natsrouter.ErrInternal("failed to unpin message")
	}

	evt := model.MessageEvent{
		Event: model.EventUnpinned,
		Message: model.Message{
			ID:          msg.MessageID,
			RoomID:      msg.RoomID,
			UserID:      msg.Sender.ID,
			UserAccount: msg.Sender.Account,
			CreatedAt:   msg.CreatedAt,
			PinnedBy:    &model.Participant{UserID: sub.User.ID, Account: sub.User.Account},
		},
		SiteID:    siteID,
		Timestamp: time.Now().UTC().UnixMilli(),
	}
	s.publishCanonicalBestEffort(c, subject.MsgCanonicalUnpinned(siteID), &evt, msg.MessageID, roomID)

	return &models.UnpinMessageResponse{MessageID: msg.MessageID}, nil
}

// ListPinnedMessages handles chat.user.{account}.request.room.{roomID}.{siteID}.msg.pinned.list.
// Subscription-gated only — listing existing pins stays available even when
// new pinning is disabled, and is not a moderation action.
func (s *HistoryService) ListPinnedMessages(c *natsrouter.Context, req models.ListPinnedMessagesRequest) (*models.ListPinnedMessagesResponse, error) {
	account := c.Param("account")
	roomID := c.Param("roomID")

	if _, err := s.getAccessSince(c, account, roomID); err != nil {
		return nil, err
	}

	pageReq, err := parsePageRequest(req.Cursor, req.Limit)
	if err != nil {
		return nil, err
	}

	page, err := s.msgReader.GetPinnedMessages(c, roomID, pageReq)
	if err != nil {
		slog.Error("list pinned messages", "error", err, "roomID", roomID)
		return nil, natsrouter.ErrInternal("failed to list pinned messages")
	}

	return &models.ListPinnedMessagesResponse{
		Messages:   page.Data,
		NextCursor: page.NextCursor,
		HasNext:    page.HasNext,
	}, nil
}
```

- [ ] **Step 4: Register the handlers**

In `history-service/internal/service/service.go`, inside `RegisterHandlers`, after the `MsgDeletePattern` registration block, add:

```go
	natsrouter.Register(r, subject.MsgPinPattern(siteID), func(c *natsrouter.Context, req models.PinMessageRequest) (*models.PinMessageResponse, error) {
		return s.PinMessage(c, siteID, req)
	})
	natsrouter.Register(r, subject.MsgUnpinPattern(siteID), func(c *natsrouter.Context, req models.UnpinMessageRequest) (*models.UnpinMessageResponse, error) {
		return s.UnpinMessage(c, siteID, req)
	})
	natsrouter.Register(r, subject.MsgPinnedListPattern(siteID), s.ListPinnedMessages)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `make test SERVICE=history-service`
Expected: PASS — all `pin_test.go` cases plus the unchanged existing suite. Then run `make lint` and fix any issues.

- [ ] **Step 6: Commit**

```bash
git add history-service/internal/service/pin.go history-service/internal/service/service.go history-service/internal/service/pin_test.go
git commit -m "feat(history-service): add pin/unpin/list-pinned handlers"
```

---

## Task 10: Client API documentation

**Files:**
- Modify: `docs/client-api.md`

Required in the same PR because the three new subjects begin with `chat.user.{account}.request.…` (project guideline).

- [ ] **Step 1: Read the existing edit/delete entries**

Run: `grep -n "msg.edit\|msg.delete\|Edit Message\|Delete Message" docs/client-api.md`
Read those sections to match the document's existing format (request subject, request/response JSON, error cases, triggered events).

- [ ] **Step 2: Add three sections**

Following the exact heading/table/JSON style used by the edit and delete entries, document, immediately after the Delete Message section:

- **Pin Message** — subject `chat.user.{account}.request.room.{roomID}.{siteID}.msg.pin`; request `{ "messageId": string }`; response `{ "messageId": string, "pinnedAt": number /* UTC millis */ }`; errors: `FORBIDDEN "pinning is disabled"`, `FORBIDDEN "not subscribed to room"`, `FORBIDDEN "room is too large to pin"`, `NOT_FOUND "message not found"` (missing, wrong room, or deleted), `INTERNAL`; idempotent (re-pin echoes existing `pinnedAt`); triggers canonical `chat.msg.canonical.{siteID}.pinned` (`event: "pinned"`).
- **Unpin Message** — subject `…msg.unpin`; request `{ "messageId": string }`; response `{ "messageId": string }`; same authorization/error cases as Pin; idempotent (no-op when not pinned); triggers canonical `chat.msg.canonical.{siteID}.unpinned` (`event: "unpinned"`).
- **List Pinned Messages** — subject `…msg.pinned.list`; request `{ "cursor"?: string, "limit"?: number }`; response `{ "messages": Message[], "nextCursor"?: string, "hasNext": boolean }`; subscription-gated only (kill-switch and large-room override do NOT apply); errors `FORBIDDEN "not subscribed to room"`, `BAD_REQUEST "invalid pagination cursor"`, `INTERNAL`. Document the caveat: in returned messages, `pinnedAt` is authoritative and `createdAt` equals the pin time (denormalized-row artifact); clients needing the original creation time use Get Message By ID. Results ordered most-recently-pinned first.

- [ ] **Step 3: Commit**

```bash
git add docs/client-api.md
git commit -m "docs: document pin/unpin/list-pinned client API"
```

---

## Final verification

- [ ] **Step 1: Full unit suite + lint**

Run: `make test SERVICE=history-service && make test SERVICE=model && make lint`
Expected: all PASS.

- [ ] **Step 2: Full integration suite**

Run: `make test-integration SERVICE=history-service`
Expected: all PASS (settings, room, pin/unpin/list Cassandra tests).

- [ ] **Step 3: Push**

```bash
git push -u origin claude/pin-unpin-history-k8p88
```

---

## Self-Review Notes (addressed)

- **Spec coverage:** kill-switch (Task 4, 9), subscription gate (Task 6, 9), large-room override + bot/role bypass (Task 9), Cassandra writes + invariant (Task 7), list reader (Task 7, 9), canonical events (Task 1, 9), config (Task 3), client-api doc (Task 10), TDD tests at every task. Phases 2–4 explicitly out of scope.
- **Type consistency:** `PinMessage(ctx, *models.Message, time.Time, models.Participant)`, `UnpinMessage(ctx, *models.Message)`, `GetPinnedMessages(ctx, string, cassrepo.PageRequest) (cassrepo.Page[models.Message], error)`, `PinEnabled(ctx) (bool, error)`, `GetSubscription(ctx, string, string) (*model.Subscription, error)`, `GetRoomUserCount(ctx, string) (int, error)` — identical across interface (Task 6), implementation (Tasks 4,5,7), and call sites (Task 9).
- **Known limitation (documented):** `ListPinnedMessages` returns `createdAt == pinnedAt` for each message (denormalized-row artifact); `pinnedAt` is authoritative. Documented in Task 7 reader doc comment and Task 10 client-api.
- **Harness ripple:** `newServiceWithRoomMock` gains a 7th return value; every caller in the `service` test package must absorb the extra value (Task 6, Step 3).
