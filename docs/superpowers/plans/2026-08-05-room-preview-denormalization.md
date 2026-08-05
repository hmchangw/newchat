# Room Preview Denormalization (Phase 2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist each room's last eligible preview message on the Mongo room doc so a local `subscription.list` is served by one aggregation (no `rooms.get` RPC for local rooms), with zero new write operations on the message hot path (edit/delete add one guarded write each; warm-back adds one best-effort write per dormant room, decaying to zero).

**Architecture:** broadcast-worker (already the single writer of `rooms.lastMsgAt`) folds a `previewMessage` into its existing coalesced per-room `$set`, guarded by an event-timestamp watermark (`previewAsOf`) expressed as a Mongo aggregation-pipeline update. history-service lazily warm-backs previews it resolves by walking. user-service projects `previewMessage` through the existing subscriptions→rooms `$lookup` and only falls back to `rooms.get` for rooms lacking one. A shared `pkg/preview` package builds identical previews on every path and caps content at 500 runes.

**Tech Stack:** Go 1.25, MongoDB driver v2 (`go.mongodb.org/mongo-driver/v2`), NATS JetStream, mockgen (`go.uber.org/mock`), testify, testcontainers via `pkg/testutil`.

**Spec:** `docs/superpowers/specs/2026-08-05-room-preview-denormalization-design.md`

## Global Constraints

- All commands via `make` targets — never raw `go` commands (`make test SERVICE=<path>`, `make generate SERVICE=<path>`, `make lint`, `make fmt`, `make sast`).
- TDD Red-Green-Refactor for every task; run the failing test before implementing.
- `-race` always (the Makefile test targets include it).
- Coverage ≥80% on touched packages (≥90% target for handlers/stores).
- Integration tests: `//go:build integration` tag, containers only from `pkg/testutil`, `TestMain(m) { testutil.RunTests(m) }`.
- Error wrapping: `fmt.Errorf("what this fn was doing: %w", err)`; never log AND return the same error.
- Structured `log/slog` only; never log message bodies (preview content is a message body — log room_id/asOf, never content).
- Mocks regenerated with `make generate SERVICE=<name>` after any store-interface change; never hand-edit `mock_*_test.go`.
- Truncation cap is **500 runes**, defined once as `preview.MaxContentRunes`.
- Watermark semantics: apply preview iff `incoming asOf >= stored previewAsOf` (missing stored value ⇒ 0; ties ⇒ last writer wins).
- Never commit with failing lint/tests (pre-commit hook enforces).

---

### Task 1: `pkg/model` — persisted preview shape

**Files:**
- Modify: `pkg/model/message.go` (PreviewMessage bson tags + doc comment)
- Modify: `pkg/model/cassandra/attachment.go` (bson tags on Attachment, ImageDimensions)
- Modify: `pkg/model/room.go` (Room.PreviewMessage + Room.PreviewAsOf)
- Modify: `pkg/model/subscription.go` (EnrichedSubscription.PreviewMessage baseline field; refresh SubscriptionRoom.PreviewMessage comment)
- Test: `pkg/model/room_preview_test.go` (create)

**Interfaces:**
- Consumes: nothing new.
- Produces: `model.Room.PreviewMessage *PreviewMessage` (bson `previewMessage`), `model.Room.PreviewAsOf int64` (bson `previewAsOf`, `json:"-"`), `model.EnrichedSubscription.PreviewMessage *PreviewMessage` (bson `previewMessage`, `json:"-"`). All later tasks rely on these exact names.

- [ ] **Step 1: Write the failing test**

Create `pkg/model/room_preview_test.go`:

```go
package model

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/model/cassandra"
)

// The denormalized preview persists on the room doc; its BSON keys must be
// camelCase (matching the JSON shape) and must round-trip losslessly.
func TestRoom_PreviewMessage_BSONRoundTrip(t *testing.T) {
	created := time.Date(2026, 8, 5, 10, 0, 0, 0, time.UTC)
	room := Room{
		ID: "room-1",
		PreviewMessage: &PreviewMessage{
			MessageID: "m1",
			Sender:    Participant{Account: "alice", EngName: "Alice", DisplayName: "Alice A"},
			Content:   "hello",
			CreatedAt: created,
			Attachments: []cassandra.Attachment{{
				ID: "f1", Title: "pic.png", Type: "image", TitleLink: "/f1",
				FileType: "image/png", ImageURL: "/f1/img", ImageSize: 42,
				ImageDimensions: &cassandra.ImageDimensions{Width: 10, Height: 20},
			}},
			Mentions:  []Participant{{Account: "bob"}},
			VisibleTo: "alice",
		},
		PreviewAsOf: 1754388000000,
	}

	raw, err := bson.Marshal(room)
	require.NoError(t, err)

	// Stored keys are camelCase — the doc shape the $lookup projection reads.
	var doc bson.M
	require.NoError(t, bson.Unmarshal(raw, &doc))
	pm, ok := doc["previewMessage"].(bson.M)
	require.True(t, ok, "previewMessage key missing or wrong type: %v", doc)
	assert.Equal(t, "m1", pm["messageId"])
	assert.Equal(t, "hello", pm["content"])
	assert.Contains(t, pm, "createdAt")
	atts, ok := pm["attachments"].(bson.A)
	require.True(t, ok)
	att := atts[0].(bson.M)
	assert.Equal(t, "pic.png", att["title"])
	assert.Equal(t, "image/png", att["fileType"])
	dims := att["imageDimensions"].(bson.M)
	assert.Contains(t, dims, "width")
	assert.EqualValues(t, 1754388000000, doc["previewAsOf"])

	var back Room
	require.NoError(t, bson.Unmarshal(raw, &back))
	assert.Equal(t, room.PreviewMessage, back.PreviewMessage)
	assert.Equal(t, room.PreviewAsOf, back.PreviewAsOf)
}

// PreviewAsOf is a storage-only watermark: never serialized to clients.
func TestRoom_PreviewAsOf_NotInJSON(t *testing.T) {
	room := Room{ID: "r", PreviewAsOf: 123}
	b, err := json.Marshal(room) // encoding/json
	require.NoError(t, err)
	assert.NotContains(t, string(b), "previewAsOf")
}
```

Also check `model_test.go`'s `roundTrip` helper and add `Room{PreviewMessage: ...}` to its case list if the helper table is extensible.

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=pkg/model`
Expected: FAIL — `room.PreviewMessage undefined` / `previewMessage key missing`.

- [ ] **Step 3: Implement the model changes**

In `pkg/model/room.go`, after `LastMsgID` (line ~22):

```go
	// PreviewMessage is the denormalized last eligible preview, written by
	// broadcast-worker (coalesced create path, guarded edit/delete path) and
	// lazily warm-backed by history-service. Serves the local subscription.list
	// without a rooms.get RPC.
	PreviewMessage *PreviewMessage `json:"previewMessage,omitempty" bson:"previewMessage,omitempty"`
	// PreviewAsOf is the ordering watermark for previewMessage writes: the
	// canonical event Timestamp (epoch ms) that produced the stored preview
	// (warm-backs use the preview's own createdAt, always ≤ any event ts).
	// Guard key only; never serialized to clients.
	PreviewAsOf int64 `json:"-" bson:"previewAsOf,omitempty"`
```

In `pkg/model/message.go`, retag `PreviewMessage` (keep field order) and update its comment — content is now a snippet:

```go
// PreviewMessage is a room's most-recent eligible message, enriched for the
// room-list preview. Content is a snippet capped at preview.MaxContentRunes
// (500 runes) — no longer the full body. Sender/mentions carry render-ready
// wire Participants (a bot sender's displayName is its app name). Shared wire
// type: history-service's rooms.get RPC produces it, user-service's
// subscription.list embeds it, and it persists on the room doc (Room.PreviewMessage).
type PreviewMessage struct {
	MessageID   string                 `json:"messageId"             bson:"messageId"`
	Sender      Participant            `json:"sender"                bson:"sender"`
	Content     string                 `json:"content"               bson:"content"`
	CreatedAt   time.Time              `json:"createdAt"             bson:"createdAt"`
	Attachments []cassandra.Attachment `json:"attachments,omitempty" bson:"attachments,omitempty"`
	Mentions    []Participant          `json:"mentions,omitempty"    bson:"mentions,omitempty"`
	// VisibleTo is surfaced now; its write-path (populating the column) is a separate
	// follow-up, so it's empty until that lands.
	VisibleTo string `json:"visibleTo,omitempty" bson:"visibleTo,omitempty"`
	// TODO(#106): forwardSource — wired after the Forwarded snapshot merges.
}
```

In `pkg/model/cassandra/attachment.go`, add camelCase bson tags to every field of `Attachment` and `ImageDimensions` (pattern: `bson:"titleLink"` etc.; keep `omitempty` parity with the json tag):

```go
type ImageDimensions struct {
	Width  int `json:"width"  bson:"width"`
	Height int `json:"height" bson:"height"`
}

type Attachment struct {
	ID                string `json:"id"                bson:"id"`
	Title             string `json:"title"             bson:"title"`
	Type              string `json:"type"              bson:"type"`
	Description       string `json:"description,omitempty" bson:"description,omitempty"`
	TitleLink         string `json:"titleLink"         bson:"titleLink"`
	TitleLinkDownload bool   `json:"titleLinkDownload" bson:"titleLinkDownload"`

	FileType string `json:"fileType,omitempty" bson:"fileType,omitempty"`

	ImageURL        string           `json:"imageUrl,omitempty"        bson:"imageUrl,omitempty"`
	ImageType       string           `json:"imageType,omitempty"       bson:"imageType,omitempty"`
	ImageSize       int64            `json:"imageSize,omitempty"       bson:"imageSize,omitempty"`
	ImageDimensions *ImageDimensions `json:"imageDimensions,omitempty" bson:"imageDimensions,omitempty"`
	ImagePreview    string           `json:"imagePreview,omitempty"    bson:"imagePreview,omitempty"`

	AudioURL  string `json:"audioUrl,omitempty"  bson:"audioUrl,omitempty"`
	AudioType string `json:"audioType,omitempty" bson:"audioType,omitempty"`
	AudioSize int64  `json:"audioSize,omitempty" bson:"audioSize,omitempty"`

	VideoURL  string `json:"videoUrl,omitempty"  bson:"videoUrl,omitempty"`
	VideoType string `json:"videoType,omitempty" bson:"videoType,omitempty"`
	VideoSize int64  `json:"videoSize,omitempty" bson:"videoSize,omitempty"`
}
```

(Keep the existing comments on FileType and the type header — only tags change.)

In `pkg/model/subscription.go`:
- `EnrichedSubscription`: after `RoomKeyVer` add:

```go
	// PreviewMessage is the denormalized room preview projected by the rooms
	// $lookup ($addFields "previewMessage": "$room.previewMessage"). Internal:
	// builds sub.Room.PreviewMessage for LOCAL subs when the read-from-doc flag
	// is on; nil for cross-site subs and unwarmed rooms (rooms.get fallback).
	PreviewMessage *PreviewMessage `json:"-" bson:"previewMessage,omitempty"`
```

- `SubscriptionRoom.PreviewMessage`: replace the stale "(A2 — no denormalized write path)" comment:

```go
	// PreviewMessage is the room's last eligible message. LOCAL subs read it from
	// the denormalized room doc (previewMessage) when available, falling back to
	// history-service's rooms.get RPC; cross-site subs always use the RPC. Omitted
	// when the room has no eligible message, enrichment degraded, or Room==nil.
	PreviewMessage *PreviewMessage `json:"previewMessage,omitempty" bson:"-"`
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=pkg/model`
Expected: PASS (including existing `model_test.go` roundTrip and `event_test.go`).

- [ ] **Step 5: Commit**

```bash
git add pkg/model/
git commit -m "feat(model): persisted room preview shape (previewMessage + previewAsOf watermark)"
```

---

### Task 2: `pkg/preview` — shared builder, truncation, bot-aware name, Mongo guard

**Files:**
- Create: `pkg/preview/preview.go`
- Test: `pkg/preview/preview_test.go`

**Interfaces:**
- Consumes: `model.PreviewMessage`, `model.Participant`, `cassandra.Attachment`, `pkg/displayfmt.CombineWithFallback`, `model.IsBot`.
- Produces (used by Tasks 3, 4, 6, 7 — exact signatures):

```go
package preview

const MaxContentRunes = 500

type Params struct {
	MessageID   string
	Sender      model.Participant // DisplayName must already be bot-aware resolved
	Content     string
	CreatedAt   time.Time
	Attachments []cassandra.Attachment
	Mentions    []model.Participant
	VisibleTo   string
}

func Build(p Params) model.PreviewMessage
func TruncateContent(s string) string

type AppNameLookup func(ctx context.Context, botAccount string) (string, error)
func BotAwareDisplayName(ctx context.Context, lookup AppNameLookup, engName, chineseName, account string) string

// GuardedSetFields returns the aggregation-pipeline $set fields that apply
// pvw+asOf only when asOf >= stored previewAsOf (missing => 0; ties => write).
func GuardedSetFields(pvw *model.PreviewMessage, asOf int64) bson.M
```

- [ ] **Step 1: Write the failing tests**

Create `pkg/preview/preview_test.go` (`package preview`):

```go
package preview

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/model"
)

func TestTruncateContent(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"short passes through", "hello", "hello"},
		{"exactly at cap unchanged", strings.Repeat("a", MaxContentRunes), strings.Repeat("a", MaxContentRunes)},
		{"over cap truncated", strings.Repeat("a", MaxContentRunes+1), strings.Repeat("a", MaxContentRunes)},
		{"multi-byte runes counted as runes not bytes", strings.Repeat("好", MaxContentRunes+3), strings.Repeat("好", MaxContentRunes)},
		{"empty stays empty", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, TruncateContent(tt.in))
		})
	}
}

func TestBuild_TruncatesAndNormalizesUTC(t *testing.T) {
	loc, err := time.LoadLocation("Asia/Taipei")
	require.NoError(t, err)
	long := strings.Repeat("x", MaxContentRunes+50)
	got := Build(Params{
		MessageID: "m1",
		Sender:    model.Participant{Account: "alice"},
		Content:   long,
		CreatedAt: time.Date(2026, 8, 5, 18, 0, 0, 0, loc),
		VisibleTo: "alice",
	})
	assert.Equal(t, "m1", got.MessageID)
	assert.Equal(t, strings.Repeat("x", MaxContentRunes), got.Content)
	assert.Equal(t, time.UTC, got.CreatedAt.Location())
	assert.Equal(t, "alice", got.VisibleTo)
}

func TestBotAwareDisplayName(t *testing.T) {
	appName := func(name string, err error) AppNameLookup {
		return func(context.Context, string) (string, error) { return name, err }
	}
	tests := []struct {
		name    string
		account string
		lookup  AppNameLookup
		want    string
	}{
		// displayfmt.CombineWithFallback(eng, chinese, account) composes the
		// human name; assert against its real output for ("Alice","愛麗絲",...).
		{"human ignores lookup", "alice", appName("ShouldNotAppear", nil), ""},
		{"bot uses app name", "bot.helper", appName("Helper App", nil), "Helper App"},
		{"bot lookup error falls back composed", "bot.helper", appName("", errors.New("db down")), ""},
		{"bot empty app name falls back composed", "bot.helper", appName("", nil), ""},
		{"nil lookup falls back composed", "bot.helper", nil, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := BotAwareDisplayName(context.Background(), tt.lookup, "Alice", "愛麗絲", tt.account)
			if tt.want == "" {
				// Fallback path: must equal the composed name, not the app name.
				composed := BotAwareDisplayName(context.Background(), nil, "Alice", "愛麗絲", "alice")
				assert.Equal(t, composed, got)
				assert.NotContains(t, got, "ShouldNotAppear")
			} else {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

// NOTE: adjust "bot.helper" to an account model.IsBot actually recognizes —
// check pkg/model's IsBot implementation and use a matching fixture.

func TestGuardedSetFields_Shape(t *testing.T) {
	pvw := &model.PreviewMessage{MessageID: "m1", Content: "$notAFieldPath"}
	fields := GuardedSetFields(pvw, 1754388000000)

	// Both guarded fields present, keyed for a $set pipeline stage.
	require.Contains(t, fields, "previewMessage")
	require.Contains(t, fields, "previewAsOf")

	// The preview doc must be wrapped in $literal so "$"-prefixed content
	// strings are never evaluated as aggregation field paths.
	cond := fields["previewMessage"].(bson.M)["$cond"].(bson.A)
	_, hasLiteral := cond[1].(bson.M)["$literal"]
	assert.True(t, hasLiteral, "preview doc must be $literal-wrapped")

	// Guard compares incoming asOf against $ifNull(previewAsOf, 0).
	gte := cond[0].(bson.M)["$gte"].(bson.A)
	assert.EqualValues(t, 1754388000000, gte[0])
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=pkg/preview`
Expected: FAIL — package does not exist / undefined symbols.

- [ ] **Step 3: Implement `pkg/preview/preview.go`**

```go
// Package preview builds the room-list preview message shared by every writer
// and reader path: history-service's read-time walk, broadcast-worker's
// denormalized create/edit/delete writes, and the warm-back. Building here
// keeps the shapes identical regardless of which path produced the preview.
package preview

import (
	"context"
	"log/slog"
	"time"
	"unicode/utf8"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/displayfmt"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
)

// MaxContentRunes caps PreviewMessage.Content: the room list renders a snippet,
// so persisting/shipping the full (≤20KB) body is pure waste.
const MaxContentRunes = 500

// Params carries the resolved fields of one eligible message. Sender must
// arrive with DisplayName already bot-aware resolved (BotAwareDisplayName).
type Params struct {
	MessageID   string
	Sender      model.Participant
	Content     string
	CreatedAt   time.Time
	Attachments []cassandra.Attachment
	Mentions    []model.Participant
	VisibleTo   string
}

// Build assembles the wire/storage preview, truncating content and normalizing
// the timestamp to UTC.
func Build(p Params) model.PreviewMessage {
	return model.PreviewMessage{
		MessageID:   p.MessageID,
		Sender:      p.Sender,
		Content:     TruncateContent(p.Content),
		CreatedAt:   p.CreatedAt.UTC(),
		Attachments: p.Attachments,
		Mentions:    p.Mentions,
		VisibleTo:   p.VisibleTo,
	}
}

// TruncateContent caps s at MaxContentRunes runes (never splitting a rune).
func TruncateContent(s string) string {
	if utf8.RuneCountInString(s) <= MaxContentRunes {
		return s
	}
	runes := []rune(s)
	return string(runes[:MaxContentRunes])
}

// AppNameLookup resolves a bot account's app display name; ("", nil) means no
// app matches.
type AppNameLookup func(ctx context.Context, botAccount string) (string, error)

// BotAwareDisplayName composes a render-ready name; for a bot account it
// prefers the app's display name, degrading to the composed name on
// miss/error/nil lookup.
func BotAwareDisplayName(ctx context.Context, lookup AppNameLookup, engName, chineseName, account string) string {
	name := displayfmt.CombineWithFallback(engName, chineseName, account)
	if model.IsBot(account) && lookup != nil {
		if appName, err := lookup(ctx, account); err != nil {
			slog.WarnContext(ctx, "app name lookup failed, using composed name", "account", account, "error", err)
		} else if appName != "" {
			name = appName
		}
	}
	return name
}

// GuardedSetFields returns the $set fields for an aggregation-pipeline update
// that applies pvw+asOf only when asOf >= the stored previewAsOf (missing => 0;
// ties => last writer wins). $literal shields the preview doc from aggregation
// evaluation (content may legitimately start with "$").
func GuardedSetFields(pvw *model.PreviewMessage, asOf int64) bson.M {
	cond := bson.M{"$gte": bson.A{asOf, bson.M{"$ifNull": bson.A{"$previewAsOf", 0}}}}
	return bson.M{
		"previewMessage": bson.M{"$cond": bson.A{cond, bson.M{"$literal": pvw}, "$previewMessage"}},
		"previewAsOf":    bson.M{"$cond": bson.A{cond, asOf, "$previewAsOf"}},
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=pkg/preview`
Expected: PASS. Fix the `IsBot` fixture account if the bot cases fail (see note in the test).

- [ ] **Step 5: Commit**

```bash
git add pkg/preview/
git commit -m "feat(preview): shared preview builder with 500-rune snippet cap and watermark guard"
```

---

### Task 3: history-service — build previews via `pkg/preview` (truncation activates)

**Files:**
- Modify: `history-service/internal/service/rooms.go:207-235` (`toPreviewMessage`)
- Modify: `history-service/internal/service/reactions.go:171-183` (`botAwareDisplayName`)
- Test: `history-service/internal/service/rooms_test.go` (or the file holding existing `toPreviewMessage`/RoomsGet tests — locate with `grep -rn "toPreviewMessage\|RoomsGet" history-service/internal/service/*_test.go`)

**Interfaces:**
- Consumes: `preview.Build`, `preview.Params`, `preview.BotAwareDisplayName`, `preview.MaxContentRunes` (Task 2).
- Produces: unchanged service API — `toPreviewMessage` and `botAwareDisplayName` keep their existing signatures; previews are now truncated.

- [ ] **Step 1: Write the failing test**

In the test file that already covers the preview walk (find the existing `RoomsGet`/preview tests and add alongside, reusing their mock scaffolding — mocked `MessageReader` returning a page with one eligible message):

```go
func TestRoomsGet_PreviewContentTruncated(t *testing.T) {
	// Arrange the existing RoomsGet happy-path scaffolding (mock room times +
	// one eligible message) but give the message a body longer than the cap.
	long := strings.Repeat("x", preview.MaxContentRunes+100)
	// ... set msg.Msg = long on the mocked walk result ...

	// Act: RoomsGet / roomLastPreviewMessage as the sibling tests do.

	// Assert: the returned preview content is exactly the 500-rune snippet.
	assert.Equal(t, strings.Repeat("x", preview.MaxContentRunes), got.Content)
}
```

(Adapt names to the file's existing table/harness style — copy the nearest happy-path test and change only the message body + assertion.)

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=history-service`
Expected: the new test FAILS (content comes back untruncated); everything else PASSES.

- [ ] **Step 3: Refactor to the shared builder**

`history-service/internal/service/rooms.go` — replace the tail of `toPreviewMessage`:

```go
func (s *HistoryService) toPreviewMessage(ctx context.Context, m *models.Message) models.PreviewMessage {
	// The walk reads raw attachment blobs; other read paths decode via
	// setDecodedAttachments, so decode this one message before mapping.
	decodeMessageAttachments(ctx, m)
	sender := toWireParticipant(&m.Sender)
	sender.DisplayName = s.botAwareDisplayName(ctx, m.Sender.EngName, m.Sender.CompanyName, m.Sender.Account)

	var mentions []pkgmodel.Participant
	if len(m.Mentions) > 0 {
		mentions = make([]pkgmodel.Participant, len(m.Mentions))
		for i := range m.Mentions {
			mentions[i] = toWireParticipant(&m.Mentions[i])
		}
	}

	return preview.Build(preview.Params{
		MessageID:   m.MessageID,
		Sender:      sender,
		Content:     m.Msg,
		CreatedAt:   m.CreatedAt,
		Attachments: m.DecodedAttachments,
		Mentions:    mentions,
		VisibleTo:   m.VisibleTo,
	})
}
```

`history-service/internal/service/reactions.go` — delegate:

```go
// botAwareDisplayName composes a render-ready name; for a bot account it prefers the
// app's display name, degrading to the composed name on lookup miss/error.
func (s *HistoryService) botAwareDisplayName(ctx context.Context, engName, chineseName, account string) string {
	return preview.BotAwareDisplayName(ctx, s.apps.AppNameByAccount, engName, chineseName, account)
}
```

Add `"github.com/hmchangw/chat/pkg/preview"` to both files' imports; drop now-unused imports (`displayfmt`, `slog`, `pkgmodel.IsBot` usage in reactions.go if orphaned).

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=history-service`
Expected: PASS. If a pre-existing test asserts full-body preview content, update it to expect the snippet (this is the spec-approved wire change).

- [ ] **Step 5: Commit**

```bash
git add history-service/ 
git commit -m "refactor(history-service): build previews via pkg/preview (snippet cap active)"
```

---

### Task 4: broadcast-worker store — guarded preview writes + app-name lookup

**Files:**
- Modify: `broadcast-worker/store.go` (interface)
- Modify: `broadcast-worker/store_mongo.go` (impl + apps collection)
- Modify: `broadcast-worker/main.go:122` (wire apps collection)
- Modify: `broadcast-worker/coalescer.go:61` (signature pass-through only — full coalescer logic is Task 5, but the interface change must compile now; make both changes in this task's tree and split the commits as described in Task 5 if needed — otherwise do Tasks 4+5 as one commit series in order)
- Test: `broadcast-worker/integration_test.go` (extend)

**Interfaces:**
- Consumes: `preview.GuardedSetFields` (Task 2), `model.Room.PreviewMessage`/`PreviewAsOf` (Task 1).
- Produces (Tasks 5–6 rely on these exact signatures):

```go
// Store interface additions/changes:
UpdateRoomLastMessage(ctx context.Context, roomID, msgID string, msgAt time.Time, mentionAll bool, pvw *model.PreviewMessage, previewAsOf int64) error
SetRoomPreviewMessage(ctx context.Context, roomID string, pvw *model.PreviewMessage, asOf int64) error
AppNameByAccount(ctx context.Context, botAccount string) (string, error)

// coalescer.go struct extension (buffered entry):
type roomLastMsgUpdate struct {
	msgID            string
	at               time.Time
	lastMentionAllAt time.Time
	preview          *model.PreviewMessage
	previewAsOf      int64
}
```

- [ ] **Step 1: Write the failing integration tests**

Extend `broadcast-worker/integration_test.go` (it already has `//go:build integration`, `TestMain`, and `testutil.MongoDB` scaffolding — follow its existing seeding style):

```go
func TestMongoStore_SetRoomPreviewMessage_WatermarkGuard(t *testing.T) {
	db := testutil.MongoDB(t, "bw-preview")
	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), db.Collection("apps"), nil, time.Minute)
	ctx := context.Background()
	_, err := db.Collection("rooms").InsertOne(ctx, bson.M{"_id": "r1", "name": "room"})
	require.NoError(t, err)

	p1 := &model.PreviewMessage{MessageID: "m1", Content: "one", CreatedAt: time.UnixMilli(1000).UTC()}
	p2 := &model.PreviewMessage{MessageID: "m2", Content: "two", CreatedAt: time.UnixMilli(2000).UTC()}

	// First write lands (doc had no previewAsOf => 0).
	require.NoError(t, store.SetRoomPreviewMessage(ctx, "r1", p2, 200))
	// Older asOf rejected.
	require.NoError(t, store.SetRoomPreviewMessage(ctx, "r1", p1, 100))
	got := readRoomPreview(t, db, "r1") // helper below
	assert.Equal(t, "m2", got.MessageID)

	// Sequential-delete shape: NEWER asOf carrying an OLDER-createdAt preview
	// must land (delete m2 => preview becomes m1, watermark is the event ts).
	require.NoError(t, store.SetRoomPreviewMessage(ctx, "r1", p1, 300))
	got = readRoomPreview(t, db, "r1")
	assert.Equal(t, "m1", got.MessageID)

	// Content with a "$" prefix must persist verbatim ($literal shielding).
	pDollar := &model.PreviewMessage{MessageID: "m3", Content: "$lookup is not a field path", CreatedAt: time.UnixMilli(3000).UTC()}
	require.NoError(t, store.SetRoomPreviewMessage(ctx, "r1", pDollar, 400))
	got = readRoomPreview(t, db, "r1")
	assert.Equal(t, "$lookup is not a field path", got.Content)
}

func readRoomPreview(t *testing.T, db *mongo.Database, roomID string) *model.PreviewMessage {
	t.Helper()
	var room model.Room
	require.NoError(t, db.Collection("rooms").FindOne(context.Background(), bson.M{"_id": roomID}).Decode(&room))
	require.NotNil(t, room.PreviewMessage)
	return room.PreviewMessage
}

func TestMongoStore_BulkUpdateRoomLastMessage_PreviewFolded(t *testing.T) {
	db := testutil.MongoDB(t, "bw-bulk-preview")
	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), db.Collection("apps"), nil, time.Minute)
	ctx := context.Background()
	for _, id := range []string{"ra", "rb"} {
		_, err := db.Collection("rooms").InsertOne(ctx, bson.M{"_id": id, "name": id})
		require.NoError(t, err)
	}

	pvw := &model.PreviewMessage{MessageID: "m1", Content: "hi", CreatedAt: time.UnixMilli(1000).UTC()}
	require.NoError(t, store.BulkUpdateRoomLastMessage(ctx, map[string]roomLastMsgUpdate{
		// ra: eligible message => preview folded into the same $set.
		"ra": {msgID: "m1", at: time.UnixMilli(1000).UTC(), preview: pvw, previewAsOf: 1000},
		// rb: system-message-only flush => lastMsgAt advances, preview untouched.
		"rb": {msgID: "sys1", at: time.UnixMilli(1000).UTC()},
	}))

	var ra model.Room
	require.NoError(t, db.Collection("rooms").FindOne(ctx, bson.M{"_id": "ra"}).Decode(&ra))
	require.NotNil(t, ra.PreviewMessage)
	assert.Equal(t, "m1", ra.PreviewMessage.MessageID)
	assert.Equal(t, "m1", ra.LastMsgID)

	var rb model.Room
	require.NoError(t, db.Collection("rooms").FindOne(ctx, bson.M{"_id": "rb"}).Decode(&rb))
	assert.Nil(t, rb.PreviewMessage)
	assert.Equal(t, "sys1", rb.LastMsgID)

	// A later system-only flush entry must not clobber a stored preview.
	require.NoError(t, store.BulkUpdateRoomLastMessage(ctx, map[string]roomLastMsgUpdate{
		"ra": {msgID: "sys2", at: time.UnixMilli(2000).UTC()},
	}))
	require.NoError(t, db.Collection("rooms").FindOne(ctx, bson.M{"_id": "ra"}).Decode(&ra))
	require.NotNil(t, ra.PreviewMessage, "system flush must not clear the preview")
	assert.Equal(t, "sys2", ra.LastMsgID)
}

func TestMongoStore_AppNameByAccount(t *testing.T) {
	db := testutil.MongoDB(t, "bw-appname")
	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), db.Collection("apps"), nil, time.Minute)
	ctx := context.Background()
	_, err := db.Collection("apps").InsertOne(ctx, bson.M{"_id": "app1", "name": "Helper App", "assistant": bson.M{"name": "bot.helper"}})
	require.NoError(t, err)

	name, err := store.AppNameByAccount(ctx, "bot.helper")
	require.NoError(t, err)
	assert.Equal(t, "Helper App", name)

	name, err = store.AppNameByAccount(ctx, "bot.unknown")
	require.NoError(t, err)
	assert.Equal(t, "", name)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test-integration SERVICE=broadcast-worker`
Expected: FAIL to compile (`NewMongoStore` arity, `SetRoomPreviewMessage`/`AppNameByAccount` undefined, `roomLastMsgUpdate` fields).

- [ ] **Step 3: Implement store changes**

`broadcast-worker/store.go` — update the interface (keep existing comments, add):

```go
	// UpdateRoomLastMessage records the room's newest message; pvw (nil for
	// system messages) is the denormalized preview to persist alongside,
	// previewAsOf its canonical-event-Timestamp watermark.
	UpdateRoomLastMessage(ctx context.Context, roomID, msgID string, msgAt time.Time, mentionAll bool, pvw *model.PreviewMessage, previewAsOf int64) error
	// SetRoomPreviewMessage persists a post-mutation (edit/delete) preview,
	// watermark-guarded so redeliveries and races cannot regress a newer one.
	SetRoomPreviewMessage(ctx context.Context, roomID string, pvw *model.PreviewMessage, asOf int64) error
	// AppNameByAccount returns the app display name for a bot account
	// (assistant.name), or ("", nil) when no app matches.
	AppNameByAccount(ctx context.Context, botAccount string) (string, error)
```

`broadcast-worker/store_mongo.go`:

```go
type mongoStore struct {
	roomCol       *mongo.Collection
	subCol        *mongo.Collection
	threadRoomCol *mongo.Collection
	appCol        *mongo.Collection
	valkey        valkeyutil.Client
	metaTTL       time.Duration
	metaRec       roommetacache.Recorder
}

func NewMongoStore(roomCol, subCol, threadRoomCol, appCol *mongo.Collection, valkey valkeyutil.Client, metaTTL time.Duration) *mongoStore {
	return &mongoStore{
		roomCol: roomCol, subCol: subCol, threadRoomCol: threadRoomCol, appCol: appCol,
		valkey: valkey, metaTTL: metaTTL, metaRec: cachemetrics.For("roommeta", "l2"),
	}
}
```

Replace `UpdateRoomLastMessage` / `BulkUpdateRoomLastMessage` bodies. Both build the same per-room update via one helper so single and bulk writes cannot drift:

```go
// roomLastMsgUpdateModel builds the per-room update. With a preview it becomes
// an aggregation-pipeline update so the watermark guard can compare against the
// stored previewAsOf; without one it stays a plain $set (system messages and
// lastMsgAt-only flushes must never touch the stored preview).
func roomLastMsgUpdateModel(u roomLastMsgUpdate) any {
	fields := bson.M{
		"lastMsgAt": u.at,
		"lastMsgId": u.msgID,
		"updatedAt": u.at,
	}
	if !u.lastMentionAllAt.IsZero() {
		fields["lastMentionAllAt"] = u.lastMentionAllAt
	}
	if u.preview == nil {
		return bson.M{"$set": fields}
	}
	for k, v := range preview.GuardedSetFields(u.preview, u.previewAsOf) {
		fields[k] = v
	}
	// Pipeline form: plain values (time.Time, base62 ids) marshal as literals;
	// only the guarded preview fields are aggregation expressions.
	return mongo.Pipeline{{{Key: "$set", Value: fields}}}
}

func (m *mongoStore) UpdateRoomLastMessage(ctx context.Context, roomID, msgID string, msgAt time.Time, mentionAll bool, pvw *model.PreviewMessage, previewAsOf int64) error {
	u := roomLastMsgUpdate{msgID: msgID, at: msgAt, preview: pvw, previewAsOf: previewAsOf}
	if mentionAll {
		u.lastMentionAllAt = msgAt
	}
	res, err := m.roomCol.UpdateOne(ctx, bson.M{"_id": roomID}, roomLastMsgUpdateModel(u))
	if err != nil {
		return fmt.Errorf("update room last message %s: %w", roomID, err)
	}
	if res.MatchedCount == 0 {
		return fmt.Errorf("update room last message %s: %w", roomID, mongo.ErrNoDocuments)
	}
	return nil
}

func (m *mongoStore) BulkUpdateRoomLastMessage(ctx context.Context, updates map[string]roomLastMsgUpdate) error {
	if len(updates) == 0 {
		return nil
	}
	models := make([]mongo.WriteModel, 0, len(updates))
	for roomID, u := range updates {
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(bson.M{"_id": roomID}).
			SetUpdate(roomLastMsgUpdateModel(u)))
	}
	if _, err := m.roomCol.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false)); err != nil {
		return fmt.Errorf("bulk update room last message (%d rooms): %w", len(updates), err)
	}
	return nil
}

func (m *mongoStore) SetRoomPreviewMessage(ctx context.Context, roomID string, pvw *model.PreviewMessage, asOf int64) error {
	if pvw == nil {
		return nil
	}
	pipeline := mongo.Pipeline{{{Key: "$set", Value: preview.GuardedSetFields(pvw, asOf)}}}
	if _, err := m.roomCol.UpdateOne(ctx, bson.M{"_id": roomID}, pipeline); err != nil {
		return fmt.Errorf("set room preview %s: %w", roomID, err)
	}
	return nil
}

func (m *mongoStore) AppNameByAccount(ctx context.Context, botAccount string) (string, error) {
	var doc struct {
		Name string `bson:"name"`
	}
	opts := options.FindOne().SetProjection(bson.M{"name": 1, "_id": 0})
	err := m.appCol.FindOne(ctx, bson.M{"assistant.name": botAccount}, opts).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return "", nil
		}
		return "", fmt.Errorf("find app by assistant name: %w", err)
	}
	return doc.Name, nil
}
```

Add imports: `"github.com/hmchangw/chat/pkg/preview"`.

`broadcast-worker/coalescer.go` — minimal compile fix now (full merge logic is Task 5): extend the struct with `preview *model.PreviewMessage` and `previewAsOf int64` fields (shown in Interfaces above) and update `UpdateRoomLastMessage`'s signature to accept and buffer them (implementation per Task 5 — do it now; Task 5 adds the tests that pin the semantics).

`broadcast-worker/main.go:122` — add the apps collection:

```go
	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"), db.Collection("thread_rooms"), db.Collection("apps"), metaValkey, cfg.RoomMetaL2TTL)
```

Fix remaining compile errors in `handler.go` (the `UpdateRoomLastMessage` call site — pass `nil, 0` for now; Task 6 wires the real values) and regenerate mocks:

Run: `make generate SERVICE=broadcast-worker`

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test-integration SERVICE=broadcast-worker` and `make test SERVICE=broadcast-worker`
Expected: new integration tests PASS; existing unit tests may fail on the changed mock signature — update their expectations mechanically (`UpdateRoomLastMessage(..., gomock.Any(), gomock.Any())`), keeping behavior assertions for Task 6.

- [ ] **Step 5: Commit**

```bash
git add broadcast-worker/
git commit -m "feat(broadcast-worker): watermark-guarded preview writes + bot app-name lookup"
```

---

### Task 5: broadcast-worker coalescer — buffer the preview independently

**Files:**
- Modify: `broadcast-worker/coalescer.go`
- Test: `broadcast-worker/coalescer_test.go` (extend)

**Interfaces:**
- Consumes: `roomLastMsgUpdate.preview/previewAsOf` (Task 4), Store signature (Task 4).
- Produces: coalescing semantics Task 6 relies on — preview merged by max-`previewAsOf` independently of `msgID/at`; nil preview never clears a buffered one.

- [ ] **Step 1: Write the failing tests**

Extend `broadcast-worker/coalescer_test.go` (reuse its `fakeBulkWriter` — extend the fake to record the full `roomLastMsgUpdate`):

```go
func TestCoalescingStore_PreviewRidesFlush(t *testing.T) {
	f := &fakeBulkWriter{}
	c := newCoalescingStore(nil, f)
	pvw := &model.PreviewMessage{MessageID: "m1", Content: "hi"}
	t1 := time.UnixMilli(1000).UTC()
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), "r1", "m1", t1, false, pvw, 1000))
	require.NoError(t, c.Flush(context.Background()))
	got := f.updates["r1"]
	assert.Equal(t, pvw, got.preview)
	assert.EqualValues(t, 1000, got.previewAsOf)
}

func TestCoalescingStore_SystemMessageKeepsBufferedPreview(t *testing.T) {
	f := &fakeBulkWriter{}
	c := newCoalescingStore(nil, f)
	pvw := &model.PreviewMessage{MessageID: "m1"}
	t1, t2 := time.UnixMilli(1000).UTC(), time.UnixMilli(2000).UTC()
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), "r1", "m1", t1, false, pvw, 1000))
	// System message: newer lastMsgAt, nil preview — must advance the message
	// fields but leave the buffered preview intact.
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), "r1", "sys1", t2, false, nil, 2000))
	require.NoError(t, c.Flush(context.Background()))
	got := f.updates["r1"]
	assert.Equal(t, "sys1", got.msgID)
	require.NotNil(t, got.preview)
	assert.Equal(t, "m1", got.preview.MessageID)
	assert.EqualValues(t, 1000, got.previewAsOf)
}

func TestCoalescingStore_PreviewMergesByMaxAsOf(t *testing.T) {
	f := &fakeBulkWriter{}
	c := newCoalescingStore(nil, f)
	p1 := &model.PreviewMessage{MessageID: "m1"}
	p3 := &model.PreviewMessage{MessageID: "m3"}
	t1, t3 := time.UnixMilli(1000).UTC(), time.UnixMilli(3000).UTC()
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), "r1", "m3", t3, false, p3, 3000))
	// Out-of-order older message must not displace the newer buffered preview.
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), "r1", "m1", t1, false, p1, 1000))
	require.NoError(t, c.Flush(context.Background()))
	got := f.updates["r1"]
	assert.Equal(t, "m3", got.preview.MessageID)
	assert.EqualValues(t, 3000, got.previewAsOf)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=broadcast-worker`
Expected: the new tests FAIL (preview dropped or displaced) if Task 4's compile-fix buffered naively — or fail to compile if the fake wasn't extended. Either way: red first.

- [ ] **Step 3: Implement the merge**

`broadcast-worker/coalescer.go` — final form of the buffered update:

```go
// UpdateRoomLastMessage buffers the update. Always returns nil; the buffered
// write is performed asynchronously by Flush. The preview merges independently
// of msgID/at by max previewAsOf: a system message (pvw==nil) advances the
// message fields while the buffered preview sticks, and an out-of-order older
// preview never displaces a newer one.
func (c *coalescingStore) UpdateRoomLastMessage(_ context.Context, roomID, msgID string, at time.Time, mentionAll bool, pvw *model.PreviewMessage, previewAsOf int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	cur := c.pending[roomID]
	if at.After(cur.at) {
		cur.msgID = msgID
		cur.at = at
	}
	if mentionAll && at.After(cur.lastMentionAllAt) {
		cur.lastMentionAllAt = at
	}
	if pvw != nil && previewAsOf >= cur.previewAsOf {
		cur.preview = pvw
		cur.previewAsOf = previewAsOf
	}
	c.pending[roomID] = cur
	return nil
}
```

(`Flush`/`Run` unchanged — the entry now carries the preview through to `BulkUpdateRoomLastMessage`, which Task 4 taught to fold it in.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=broadcast-worker`
Expected: PASS (including the pre-existing coalescer tests updated for the new signature).

- [ ] **Step 5: Commit**

```bash
git add broadcast-worker/coalescer.go broadcast-worker/coalescer_test.go
git commit -m "feat(broadcast-worker): coalescer buffers preview by max watermark, independent of lastMsgAt"
```

---

### Task 6: broadcast-worker handler — wire create/edit/delete

**Files:**
- Modify: `broadcast-worker/handler.go` (`handleCreated:150-213`, `handleUpdated:289-311`, `handleDeleted:486-512`, `buildClientMessage:917`)
- Test: `broadcast-worker/handler_test.go` (extend)

**Interfaces:**
- Consumes: Store methods (Task 4), coalescer semantics (Task 5), `preview.Build/Params/BotAwareDisplayName` (Task 2), `model.IsSystemMessageType`, `mention.ResolveFromParsed` (`resolved.Participants []model.Participant`), `cassandra.DecodeAttachments`.
- Produces: no new exports; behavior only.

- [ ] **Step 1: Write the failing tests**

Extend `broadcast-worker/handler_test.go`, following its existing mock-driven table style (MockStore etc. from `make generate`):

```go
func TestHandler_HandleCreated_PassesEligiblePreview(t *testing.T) {
	// Arrange the existing handleCreated happy-path scaffolding (mock store,
	// user store, publisher). Message: normal user content, msg.Type == "".
	// evt.Timestamp = 5000. Content longer than preview.MaxContentRunes.

	// Expect UpdateRoomLastMessage with a NON-nil preview whose:
	//  - MessageID == msg.ID
	//  - Content is the 500-rune snippet
	//  - previewAsOf == evt.Timestamp (5000)
	store.EXPECT().UpdateRoomLastMessage(gomock.Any(), "room-1", "msg-1", gomock.Any(), false,
		gomock.AssignableToTypeOf(&model.PreviewMessage{}), int64(5000)).
		DoAndReturn(func(_ context.Context, _, _ string, _ time.Time, _ bool, pvw *model.PreviewMessage, _ int64) error {
			require.NotNil(t, pvw)
			assert.Equal(t, "msg-1", pvw.MessageID)
			assert.Len(t, []rune(pvw.Content), preview.MaxContentRunes)
			return nil
		})
	// ... run handler, assert nil error ...
}

func TestHandler_HandleCreated_SystemMessageNilPreview(t *testing.T) {
	// Same scaffolding; msg.Type = model.MessageTypeMembersAdded.
	store.EXPECT().UpdateRoomLastMessage(gomock.Any(), "room-1", "msg-1", gomock.Any(), false,
		gomock.Nil(), gomock.Any()).Return(nil)
	// ... run handler ...
}

func TestHandler_HandleCreated_BotSenderUsesAppName(t *testing.T) {
	// msg.UserAccount = a bot account (one model.IsBot recognizes).
	store.EXPECT().AppNameByAccount(gomock.Any(), botAccount).Return("Helper App", nil)
	store.EXPECT().UpdateRoomLastMessage(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
		gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _, _ string, _ time.Time, _ bool, pvw *model.PreviewMessage, _ int64) error {
			assert.Equal(t, "Helper App", pvw.Sender.DisplayName)
			return nil
		})
	// Also assert the published client message's sender DisplayName is UNCHANGED
	// (empty) — the bot-aware name is preview-only, not a wire change to room events.
}

func TestHandler_HandleUpdated_PersistsEventPreview(t *testing.T) {
	// evt.PreviewMessage = &model.PreviewMessage{MessageID: "m-prev"}; evt.Timestamp = 7000.
	store.EXPECT().SetRoomPreviewMessage(gomock.Any(), "room-1", evt.PreviewMessage, int64(7000)).Return(nil)
	// ... plus the existing GetRoom/publish expectations; run handleUpdated ...
}

func TestHandler_HandleUpdated_NilPreviewNoPersist(t *testing.T) {
	// evt.PreviewMessage = nil → SetRoomPreviewMessage must NOT be called (no EXPECT).
}

func TestHandler_HandleDeleted_PersistsEventPreview_BestEffort(t *testing.T) {
	// SetRoomPreviewMessage returns an error → handleDeleted still returns nil
	// (persistence is best-effort; the relayed event already carried the preview).
	store.EXPECT().SetRoomPreviewMessage(gomock.Any(), "room-1", evt.PreviewMessage, evt.Timestamp).Return(errors.New("mongo down"))
	// ... run handleDeleted; assert err == nil ...
}
```

(Write these as real tests against the file's existing helpers — copy the nearest create/edit/delete test setup and adjust. Every `// ...` above must be filled from that scaffolding, not left as comments.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=broadcast-worker`
Expected: new tests FAIL (preview currently `nil, 0`; SetRoomPreviewMessage never called).

- [ ] **Step 3: Implement handler wiring**

In `handler.go`, extract the sender construction shared by preview and client message (refactor `buildClientMessage`'s inline block):

```go
// buildSenderParticipant resolves the message sender to a wire Participant from
// the prefetched user map, falling back to the raw account.
func buildSenderParticipant(msg *model.Message, userMap map[string]model.User) model.Participant {
	sender := model.Participant{UserID: msg.UserID, Account: msg.UserAccount}
	if u, ok := userMap[msg.UserAccount]; ok {
		sender.ChineseName = u.ChineseName
		sender.EngName = u.EngName
	} else {
		sender.ChineseName = msg.UserAccount
		sender.EngName = msg.UserAccount
	}
	return sender
}
```

Use it in `buildClientMessage` (replacing its inline sender block, behavior identical) and in a new preview builder:

```go
// buildPreview assembles the denormalized preview for an eligible (non-system)
// created message. The sender copy gets the bot-aware display name — a copy, so
// the client-message sender keeps its current wire shape (no displayName).
func (h *Handler) buildPreview(ctx context.Context, msg *model.Message, userMap map[string]model.User, mentions []model.Participant) *model.PreviewMessage {
	if model.IsSystemMessageType(msg.Type) {
		return nil
	}
	sender := buildSenderParticipant(msg, userMap)
	sender.DisplayName = preview.BotAwareDisplayName(ctx, h.store.AppNameByAccount, sender.EngName, sender.ChineseName, sender.Account)
	decoded, _ := cassandra.DecodeAttachments(msg.Attachments)
	p := preview.Build(preview.Params{
		MessageID:   msg.ID,
		Sender:      sender,
		Content:     msg.Content,
		CreatedAt:   msg.CreatedAt,
		Attachments: decoded,
		Mentions:    mentions,
		VisibleTo:   msg.VisibleTo,
	})
	return &p
}
```

`handleCreated` (line ~173): replace the update call:

```go
	pvw := h.buildPreview(ctx, &msg, userByAccount, resolved.Participants)
	if err := h.store.UpdateRoomLastMessage(ctx, msg.RoomID, msg.ID, msg.CreatedAt, resolved.MentionAll, pvw, evt.Timestamp); err != nil {
		return fmt.Errorf("update room last message %s: %w", msg.RoomID, err)
	}
```

`handleUpdated` (before `publishMutation`, after `buildEditRoomEvent`) and `handleDeleted` (before `publishMutation`, after `buildDeleteRoomEvent`) — same block in both:

```go
	// Persist the post-mutation preview (computed upstream by history-service).
	// Best-effort: the guarded write is idempotent under redelivery, and clients
	// already receive the preview on the relayed event; a store failure must not
	// fail the mutation fan-out.
	if evt.PreviewMessage != nil {
		if err := h.store.SetRoomPreviewMessage(ctx, msg.RoomID, evt.PreviewMessage, evt.Timestamp); err != nil {
			slog.WarnContext(ctx, "persist post-mutation preview failed",
				"room_id", msg.RoomID, "message_id", msg.ID,
				"request_id", natsutil.RequestIDFromContext(ctx), "error", err)
		}
	}
```

(Thread branches — `handleThreadUpdated`/`handleThreadDeleted` — get no persist call: `evt.PreviewMessage` is nil for thread replies by upstream contract, and thread fan-out never touches the room preview.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=broadcast-worker`
Expected: PASS (new + existing).

- [ ] **Step 5: Commit**

```bash
git add broadcast-worker/
git commit -m "feat(broadcast-worker): denormalize preview on create; persist post-mutation previews"
```

---

### Task 7: history-service — lazy warm-back

**Files:**
- Modify: `history-service/internal/service/service.go` (RoomRepository interface, ~line 60-70 where `GetRoomTimesByIDs` is declared)
- Modify: `history-service/internal/mongorepo/room.go` (impl)
- Modify: `history-service/internal/service/rooms.go:137-150` (`resolvePreview`)
- Test: `history-service/internal/service/rooms_test.go` (extend), `history-service/internal/mongorepo/room_test.go` (extend, integration)

**Interfaces:**
- Consumes: `preview.GuardedSetFields` (Task 2), `models.PreviewMessage` (= `model.PreviewMessage`).
- Produces:

```go
// RoomRepository addition:
// SetPreviewMessage warm-backs a walk-resolved preview onto the room doc,
// guarded by asOf (the preview's createdAt millis) so it fills empty docs but
// never regresses a newer event-driven write. Callers treat errors as
// best-effort (log, never fail the read).
SetPreviewMessage(ctx context.Context, roomID string, pvw models.PreviewMessage, asOf int64) error
```

- [ ] **Step 1: Write the failing tests**

Service-level (`rooms_test.go`, mock repo — regenerate mocks first so the new method exists on the mock: add the interface method, run `make generate SERVICE=history-service`, then write tests — the RED state is the unimplemented mongorepo + un-wired resolvePreview):

```go
func TestRoomsGet_WarmsBackResolvedPreview(t *testing.T) {
	// Existing RoomsGet happy-path scaffolding, no preview cache installed.
	// Expect SetPreviewMessage called once with the resolved preview and
	// asOf == preview.CreatedAt.UnixMilli().
	rooms.EXPECT().SetPreviewMessage(gomock.Any(), "room-1", gomock.Any(), msgCreatedAt.UnixMilli()).Return(nil)
	// ... run RoomsGet; assert the preview is still returned ...
}

func TestRoomsGet_WarmBackFailureDoesNotFailRead(t *testing.T) {
	rooms.EXPECT().SetPreviewMessage(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("mongo down"))
	// ... run RoomsGet; assert NO error and the preview entry is present ...
}

func TestRoomsGet_CacheHitSkipsWarmBack(t *testing.T) {
	// With the preview cache installed and pre-warmed for the room, the loader
	// (and therefore SetPreviewMessage) must not run: no EXPECT on SetPreviewMessage.
}
```

Repo-level (`room_test.go`, integration build tag, `testutil.MongoDB`): mirror Task 4's watermark test against `RoomRepo.SetPreviewMessage` — first write lands on an empty doc; a lower asOf is rejected; decode via `model.Room` and assert `PreviewMessage.MessageID`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=history-service` and `make test-integration SERVICE=history-service`
Expected: FAIL (method not implemented / warm-back never called).

- [ ] **Step 3: Implement**

`history-service/internal/mongorepo/room.go`:

```go
// SetPreviewMessage warm-backs a walk-resolved preview, watermark-guarded (see
// pkg/preview.GuardedSetFields). asOf is the preview's own createdAt millis —
// conservative by construction: ≤ any canonical event timestamp that observed
// this message, so a warm-back never outranks broadcast-worker's writes.
func (r *RoomRepo) SetPreviewMessage(ctx context.Context, roomID string, pvw models.PreviewMessage, asOf int64) error {
	pipeline := mongo.Pipeline{{{Key: "$set", Value: preview.GuardedSetFields(&pvw, asOf)}}}
	if _, err := r.rooms.UpdateOne(ctx, bson.M{"_id": roomID}, pipeline); err != nil {
		return fmt.Errorf("warm-back room preview %s: %w", roomID, err)
	}
	return nil
}
```

(Adapt `r.rooms.UpdateOne` to RoomRepo's actual collection handle — check how `GetRoomTimes` accesses it (`mongoutil.Collection` vs raw `*mongo.Collection`) and use the same mechanism; add an `UpdateOne` passthrough to `pkg/mongoutil.Collection` only if none exists — prefer reusing whatever raw handle the repo already holds.)

`history-service/internal/service/rooms.go` — route both resolve paths through one warm-backing loader:

```go
func (s *HistoryService) resolvePreview(ctx context.Context, roomID string, meta *models.RoomMeta, now time.Time) (models.PreviewMessage, bool) {
	load := func(ctx context.Context) (models.PreviewMessage, bool, error) {
		p, found := s.roomLastPreviewMessage(ctx, roomID, meta, now)
		if found {
			s.warmBackPreview(ctx, roomID, p)
		}
		return p, found, nil
	}
	if s.previewCache == nil {
		p, found, _ := load(ctx)
		return p, found
	}
	preview, ok, err := s.previewCache.Get(ctx, roomID, load)
	if err != nil {
		return models.PreviewMessage{}, false
	}
	return preview, ok
}

// warmBackPreview best-effort persists a walk-resolved preview so subsequent
// lists serve it from the room doc instead of re-walking. Guarded write —
// failures only cost the optimization, never the read.
func (s *HistoryService) warmBackPreview(ctx context.Context, roomID string, p models.PreviewMessage) {
	if err := s.rooms.SetPreviewMessage(ctx, roomID, p, p.CreatedAt.UnixMilli()); err != nil {
		slog.WarnContext(ctx, "preview warm-back failed", "room_id", roomID,
			"request_id", natsutil.RequestIDFromContext(ctx), "error", err)
	}
}
```

(`previewAfterMutation` still calls `roomLastPreviewMessage` directly — intentionally no warm-back there; broadcast-worker persists the mutation preview with the authoritative event watermark.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=history-service` and `make test-integration SERVICE=history-service`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add history-service/
git commit -m "feat(history-service): lazy warm-back of walk-resolved previews onto the room doc"
```

---

### Task 8: user-service — serve local previews from the `$lookup`, RPC only for the residual

**Files:**
- Modify: `user-service/config/config.go` (flag)
- Modify: `user-service/service/service.go` (struct field + `New`)
- Modify: `user-service/mongorepo/subscriptions.go:65-117` (`roomsEnrichStages`)
- Modify: `user-service/service/subscriptions.go` (`enrichLocal:241-250`, `buildLocalRoom:395-418`, `enrichLastMessage:331-384`)
- Test: `user-service/service/subscriptions_test.go` (extend), `user-service/mongorepo` integration test file that covers the list aggregation (locate with `grep -rln "roomsEnrichStages\|ListSubscriptions" user-service/mongorepo/*_test.go`)

**Interfaces:**
- Consumes: `model.EnrichedSubscription.PreviewMessage` (Task 1), room docs carrying `previewMessage` (Tasks 4–7).
- Produces: config field `Config.SubscriptionPreviewFromDoc bool` (env `SUBSCRIPTION_PREVIEW_FROM_DOC`, default `true`); `buildLocalRoom(sub *model.EnrichedSubscription, includePreview bool)`.

- [ ] **Step 1: Write the failing tests**

`user-service/service/subscriptions_test.go` (existing mock-driven style; the mock HistoryClient's `RoomsGet` records requested roomIDs):

```go
func TestEnrichLastMessage_LocalPreviewFromDocSkipsRPC(t *testing.T) {
	// Flag on. Two LOCAL subs: r1 carries a baseline PreviewMessage, r2 does not.
	// Expect exactly ONE RoomsGet for the local site with roomIDs == ["r2"];
	// r1's Room.PreviewMessage equals the baseline value.
}

func TestEnrichLastMessage_AllLocalPreviewsPresent_NoLocalRPC(t *testing.T) {
	// Flag on. All LOCAL subs carry baseline previews → RoomsGet is NOT called
	// for the local site at all (no EXPECT); previews come from the baseline.
}

func TestEnrichLastMessage_FlagOff_BehavesAsToday(t *testing.T) {
	// Flag off. Baseline previews present but IGNORED: RoomsGet called with ALL
	// local roomIDs; Room.PreviewMessage comes from the RPC result, and a room
	// the RPC omits ends with a nil preview (baseline never leaks through).
}

func TestEnrichLastMessage_CrossSiteUnaffected(t *testing.T) {
	// Flag on. A cross-site sub still fans out to its site's RoomsGet with the
	// full roomID list (cross-site rooms never have a local baseline preview).
}
```

`user-service/mongorepo` integration test: seed a room doc whose `previewMessage` is set (insert `bson.M{"_id": "r1", ..., "previewMessage": bson.M{"messageId": "m1", "content": "hi", "createdAt": time.Now().UTC()}}`), run the list aggregation, assert the decoded `EnrichedSubscription.PreviewMessage.MessageID == "m1"`; a room without the field decodes nil.

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=user-service` and `make test-integration SERVICE=user-service`
Expected: FAIL (no flag, no projection, RPC called for all rooms).

- [ ] **Step 3: Implement**

`user-service/config/config.go` — in `Config`:

```go
	// SubscriptionPreviewFromDoc serves LOCAL subscription-list previews from the
	// denormalized room doc, falling back to rooms.get only for rooms lacking one.
	// Off = pre-denormalization behavior (every local room via rooms.get).
	SubscriptionPreviewFromDoc bool `env:"SUBSCRIPTION_PREVIEW_FROM_DOC" envDefault:"true"`
```

`user-service/service/service.go` — add `previewFromDoc bool` to `UserService`; in `New`: `previewFromDoc: cfg.SubscriptionPreviewFromDoc,`.

`user-service/mongorepo/subscriptions.go` `roomsEnrichStages` — add to the `$lookup` `$project`:

```go
					"previewMessage":    1,
```

and to the `$addFields`:

```go
			"previewMessage":    "$room.previewMessage",
```

(`roomMatchStages`/member-match path intentionally unchanged: its subs keep the rooms.get fallback — self-healing, out of the hot path.)

`user-service/service/subscriptions.go`:

- `enrichLocal` (line 243): `subs[j].Room = buildLocalRoom(&subs[j], s.previewFromDoc)`
- `buildLocalRoom`: new param + population:

```go
func buildLocalRoom(sub *model.EnrichedSubscription, includePreview bool) *model.SubscriptionRoom {
	// ... existing body unchanged through the room struct literal ...
	if includePreview {
		room.PreviewMessage = sub.PreviewMessage
	}
	// ... existing key block + return ...
}
```

- `enrichLastMessage` — residual filtering per site (replace the loop headers; the RPC/apply mechanics stay as-is):

```go
	for i, site := range sites {
		if c.Err() != nil {
			break
		}
		reqIdx, reqIDs := idxBySite[site], roomIDsBySite[site]
		if site == s.siteID && s.previewFromDoc {
			// Rooms already carrying a baseline preview are served from the doc;
			// only the residual (unwarmed/dormant/soft-deleted-nil) goes to the RPC.
			reqIdx, reqIDs = residualLastMsgRooms(subs, idxBySite[site])
			if len(reqIDs) == 0 {
				continue
			}
		}
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			// ... existing body, but build hints from reqIdx and call
			// s.history.RoomsGet(c, site, reqIDs, hints) ...
		}()
	}
```

```go
// residualLastMsgRooms returns the local subs (and their roomIDs) that still
// need a rooms.get resolve: no room object is excluded elsewhere; a room with a
// baseline preview is already served from the denormalized doc.
func residualLastMsgRooms(subs []model.EnrichedSubscription, localIdx []int) ([]int, []string) {
	var idx []int
	var ids []string
	for _, j := range localIdx {
		if subs[j].Room != nil && subs[j].Room.PreviewMessage != nil {
			continue
		}
		idx = append(idx, j)
		ids = append(ids, subs[j].RoomID)
	}
	return idx, ids
}
```

(The apply loop after `wg.Wait()` needs no change: rooms not requested aren't in the response map, so their baseline preview survives; requested rooms overwrite from the RPC as today. Verify the goroutine captures `reqIdx`/`reqIDs`, not the outer maps.)

Also update the `enrichLastMessage` doc comment (lines 321-330): it still says "read-time resolve, no denormalized write path".

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=user-service` and `make test-integration SERVICE=user-service`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add user-service/
git commit -m "feat(user-service): serve local subscription previews from the room doc (flagged, default on)"
```

---

### Task 9: docs, deploy defaults, full verification

**Files:**
- Modify: `docs/client-api.md` (previewMessage content semantics)
- Modify: `docs/client-api/request-reply.md`, `docs/client-api/events.md` (only where they describe `previewMessage.content`)
- Check: `user-service/deploy/docker-compose.yml` (no change needed — flag defaults true; add `SUBSCRIPTION_PREVIEW_FROM_DOC=true` only if the compose file explicitly lists sibling toggles)

**Interfaces:** none — documentation and verification.

- [ ] **Step 1: Update the client API docs**

`grep -n "previewMessage" docs/client-api.md docs/client-api/request-reply.md docs/client-api/events.md`. In every field table describing `PreviewMessage.content`, change the description from full-body wording to:

> Message content snippet, capped at 500 runes (longer bodies are truncated).

Keep table structure/style; no schema (field/type) changes. Do not touch unrelated rows.

- [ ] **Step 2: Full local verification**

Run, in order:
- `make fmt`
- `make lint`
- `make generate` (confirm no mock drift: `git status` clean of unexpected `mock_*` diffs)
- `make test`
- `make test-integration SERVICE=broadcast-worker && make test-integration SERVICE=history-service && make test-integration SERVICE=user-service`
- `make sast`

Coverage check on the four touched surfaces:
```bash
go test -coverprofile=/tmp/claude-0/-home-user-newchat/9a9ab3f7-7dcc-504c-9983-330a09919f11/scratchpad/cov.out ./pkg/preview/... ./broadcast-worker/... ./history-service/... ./user-service/... && go tool cover -func=/tmp/claude-0/-home-user-newchat/9a9ab3f7-7dcc-504c-9983-330a09919f11/scratchpad/cov.out | tail -20
```
(Exception to the make-only rule: coverage verification is the documented CLAUDE.md coverage workflow.) Expected: ≥80% per touched package.

- [ ] **Step 3: Commit**

```bash
git add docs/
git commit -m "docs(client-api): previewMessage content is a 500-rune snippet"
```

- [ ] **Step 4: Push the branch**

```bash
git push -u origin claude/denormalize-room-last-messages-yei4gr
```

---

## Task order & dependencies

1 → 2 → 3 (history-service refactor) — 4 → 5 → 6 (broadcast-worker chain, needs 1+2) — 7 (needs 2, independent of 4-6) — 8 (needs 1; end-to-end value needs 4-7) — 9 last.

## Explicitly out of scope (do not implement)

- Cross-site preview denormalization (`GetRoomsInfo` extension).
- Proactive backfill migration job.
- Preview writes on any thread fan-out path.
- Member-match (`roomMatchStages`) baseline preview projection.
