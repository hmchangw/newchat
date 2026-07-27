# ES Index Migrator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `data-migration/es-index-migrator`, a standalone one-time/rebuild job that backfills the `messages` (monthly), `spotlight`, and `user-room` Elasticsearch indexes directly from a site's own current-stack Cassandra and MongoDB — for use when a site's ES indexes need to be rebuilt (cluster loss, mapping change, enabling search for an existing site) — while extracting the ES document shapes, Painless scripts, and bulk-result classification that `search-sync-worker` already uses live into shared packages so both consumers can never drift.

**Architecture:** Two phases. Phase A extracts `search-sync-worker`'s currently-private document builders (`MessageSearchIndex`, `SpotlightSearchIndex`, `userRoomUpsertDoc`, the two Painless scripts, `esPropertiesFromStruct`, `isBulkItemSuccess`) into importable packages (`pkg/searchindex`, `pkg/searchengine`), with `search-sync-worker` becoming a thin caller of the same shared code — no behavior change, verified by the existing test suite passing unchanged. Phase B builds the new service: it reads room membership straight from the `subscriptions` collection (each row already carries `roomId`/`roomType`/`name`/`joinedAt`/`historySharedSince` — no legacy room classifier or extra `rooms` lookup needed, unlike the design this repo's now-obsolete sibling once used) and reads message history from Cassandra's `messages_by_room` bucketed table (streaming per-bucket, never materializing a whole room's history in memory). The rows this job reads were themselves written directly into the plaintext `msg`/`attachments`/`card` columns by the migration process that populated `messages_by_room` — never through the live at-rest-encryption write path — so this job reads those columns as-is and has no dependency on `pkg/atrest`/Vault. It writes to ES using the same versioned/idempotent scheme `search-sync-worker` uses in production (external versioning for messages/spotlight, the same scripted delta update for user-room), so a re-run or overlap with live traffic converges instead of racing.

**Tech Stack:** Go 1.25, `go.mongodb.org/mongo-driver/v2`, `github.com/gocql/gocql`, `golang.org/x/sync/errgroup`, `github.com/caarlos0/env/v11`, `go.uber.org/mock` (mockgen), `stretchr/testify`, `testcontainers-go`.

## Global Constraints

- Go 1.25, monorepo single `go.mod`, module root `github.com/hmchangw/chat`.
- New service is flat `package main` at `data-migration/es-index-migrator/`, per-service file layout from CLAUDE.md (`main.go`, `handler`-equivalent split into `runner.go`/`flusher.go`, `store.go` + `//go:generate mockgen`, `*_test.go`, `integration_test.go` tagged `//go:build integration`, `mock_*_test.go` generated — never edit by hand).
- All logging via `log/slog` JSON, structured key-value fields, never `fmt.Println`.
- Error wrapping: `fmt.Errorf("<what this func was doing>: %w", err)`, never bare `err`, never string-compare errors (`errors.Is`/`errors.As`).
- Config via `caarlos0/env`, `SCREAMING_SNAKE_CASE` env vars, fail-fast on missing required config, `envDefault` for non-critical values, never default secrets/connection strings.
- Table-driven tests (`t.Run`), `testify` `assert`/`require`, mocks via `go.uber.org/mock` (never edited by hand), tests fully independent (no shared mutable state, no execution-order dependence). TDD Red-Green-Refactor per CLAUDE.md — every task below writes the failing test first.
- Integration tests use `pkg/testutil` shared containers (`testutil.MongoDB`, `testutil.CassandraKeyspace`, `testutil.Elasticsearch`+`ElasticsearchIndex`), never inline `testcontainers.GenericContainer`, and a `TestMain` via `testutil.RunTests(m)`.
- Minimum 80% coverage per package, target 90%+ for `pkg/` shared code and store/handler-equivalent implementations.
- This service **never touches NATS/JetStream** — no stream bootstrap, no `pkg/stream`, no `pkg/subject`. It writes directly to Elasticsearch via `_bulk`.
- **Site scoping:** run once per site, against that site's own Cassandra/MongoDB/ES only — no cross-site reads. A subscription row's `siteId` scopes it to the subscribing user's home site; a room a foreign-site user is subscribed to is a harmless empty-result Cassandra query on this site's own keyspace (the room's messages simply don't exist here if this site doesn't own the room).
- **Additive-only, by design:** because subscriptions are hard-deleted (`DeleteOne`/`DeleteMany`) the moment a user leaves a room (confirmed in `room-worker/store_mongo.go:457,465` — there is no soft-delete/closed-row state to read), this job can only reconstruct **current** membership state. It cannot detect or evict a stale ES entry for a membership that ended and was never re-added before some earlier ES data loss. This is an accepted limitation of a rebuild tool operating on current-state data — document it in the spec, do not attempt to work around it.
- **Every write is versioned/idempotent**, matching the live worker exactly, so a re-run or an overlapping live write from `search-sync-worker` always converges:
  - Messages: `ActionIndex`, `Version = CreatedAt.UnixMilli()`, `version_type: external`.
  - Spotlight: `ActionIndex`, `Version = JoinedAt.UnixMilli()` (subscriptions are never "closed" rows here, so there is no delete-path version source — every row migrated is index-only).
  - User-room: `ActionUpdate` against the same stored Painless scripts `search-sync-worker` registers, no external version (the script's own `roomTimestamps` LWW guard is the ordering mechanism, exactly as in production).

---

## Phase A — Extract shared ES document/script code

### Task 1: Export `IsBulkItemSuccess` from `pkg/searchengine`

**Files:**
- Modify: `pkg/searchengine/searchengine.go`
- Create: `pkg/searchengine/classify.go`
- Create: `pkg/searchengine/classify_test.go`
- Modify: `search-sync-worker/handler.go`
- Modify: `search-sync-worker/handler_test.go`

**Interfaces:**
- Consumes: `searchengine.ActionType` (`ActionIndex`/`ActionDelete`/`ActionUpdate`), `searchengine.BulkResult{Status int; ErrorType string; Error string}` — both already defined in `pkg/searchengine/searchengine.go`.
- Produces: `searchengine.IsBulkItemSuccess(action ActionType, result BulkResult) bool` — the single shared bulk-result classifier every other task in this plan that writes to ES depends on.

- [ ] **Step 1: Write the failing test**

Create `pkg/searchengine/classify_test.go`:

```go
package searchengine_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/searchengine"
)

func TestIsBulkItemSuccess(t *testing.T) {
	tests := []struct {
		name   string
		action searchengine.ActionType
		result searchengine.BulkResult
		want   bool
	}{
		{"2xx index is success", searchengine.ActionIndex, searchengine.BulkResult{Status: 201}, true},
		{"2xx update is success", searchengine.ActionUpdate, searchengine.BulkResult{Status: 200}, true},
		{"409 index is benign version conflict", searchengine.ActionIndex, searchengine.BulkResult{Status: 409}, true},
		{"409 delete is benign version conflict", searchengine.ActionDelete, searchengine.BulkResult{Status: 409}, true},
		{"409 update is a real conflict", searchengine.ActionUpdate, searchengine.BulkResult{Status: 409}, false},
		{"404 delete with no error type is already-absent doc", searchengine.ActionDelete, searchengine.BulkResult{Status: 404, ErrorType: ""}, true},
		{"404 delete with index_not_found is a real failure", searchengine.ActionDelete, searchengine.BulkResult{Status: 404, ErrorType: "index_not_found_exception"}, false},
		{"404 update with document_missing is benign no-target-yet", searchengine.ActionUpdate, searchengine.BulkResult{Status: 404, ErrorType: "document_missing_exception"}, true},
		{"404 update with index_not_found is a real failure", searchengine.ActionUpdate, searchengine.BulkResult{Status: 404, ErrorType: "index_not_found_exception"}, false},
		{"404 index is always a failure", searchengine.ActionIndex, searchengine.BulkResult{Status: 404}, false},
		{"500 is always a failure", searchengine.ActionIndex, searchengine.BulkResult{Status: 500}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, searchengine.IsBulkItemSuccess(tc.action, tc.result))
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/searchengine/... -run TestIsBulkItemSuccess -v`
Expected: FAIL with `undefined: searchengine.IsBulkItemSuccess`

- [ ] **Step 3: Write minimal implementation**

Create `pkg/searchengine/classify.go`:

```go
package searchengine

// ErrDocumentMissing is the ES error type returned when an ActionUpdate
// targets a document that does not exist yet (the scripted remove-path
// update against a not-yet-created user-room doc, or any other update
// racing a not-yet-indexed target).
const ErrDocumentMissing = "document_missing_exception"

// IsBulkItemSuccess classifies one _bulk response item's outcome for the
// given action type. Both search-sync-worker (the live path) and
// data-migration/es-index-migrator (the one-time/rebuild path) share this
// classifier so a benign, idempotent-redelivery 409/404 can never be
// misclassified as a hard failure by one caller but not the other.
//
//   - 2xx is always success.
//   - 409 is success for ActionIndex/ActionDelete (external-versioning
//     rejected a stale write — the desired state is already there) but a
//     real failure for ActionUpdate (the LWW guard runs inside the script,
//     not via ES versioning, so a 409 on update means the script itself
//     never ran).
//   - 404 is success for ActionDelete with no error block (delete of an
//     already-missing doc) and for ActionUpdate with ErrDocumentMissing
//     (scripted remove against a doc that was never created); any other
//     404 (in particular index_not_found_exception, and any 404 on
//     ActionIndex) is a real failure.
func IsBulkItemSuccess(action ActionType, result BulkResult) bool {
	if result.Status >= 200 && result.Status < 300 {
		return true
	}
	if result.Status == 409 {
		switch action {
		case ActionIndex, ActionDelete:
			return true
		case ActionUpdate:
			return false
		}
	}
	if result.Status == 404 {
		switch action {
		case ActionDelete:
			return result.ErrorType == ""
		case ActionUpdate:
			return result.ErrorType == ErrDocumentMissing
		case ActionIndex:
			return false
		}
	}
	return false
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/searchengine/... -run TestIsBulkItemSuccess -v`
Expected: PASS (all 11 subtests)

- [ ] **Step 5: Update `search-sync-worker/handler.go` to call the shared classifier**

In `search-sync-worker/handler.go`: delete the unexported `isBulkItemSuccess` function and the `esErrDocumentMissing` constant entirely. In `Flush`, change:

```go
if isBulkItemSuccess(actions[i].Action, results[i]) {
```

to:

```go
if searchengine.IsBulkItemSuccess(actions[i].Action, results[i]) {
```

(`searchengine` is already imported in this file.)

- [ ] **Step 6: Update `search-sync-worker/handler_test.go`**

Remove the file-local `TestIsBulkItemSuccess` test (it now lives in `pkg/searchengine/classify_test.go` — this file's job is testing `Handler`, not the classifier). Any other test in this file that referenced `isBulkItemSuccess` directly should call `searchengine.IsBulkItemSuccess` instead.

- [ ] **Step 7: Run tests to verify nothing broke**

Run: `go test ./search-sync-worker/... ./pkg/searchengine/... -race -v`
Expected: PASS, no references to the deleted identifiers remain (`go build ./...` must also pass clean)

- [ ] **Step 8: Commit**

```bash
git add pkg/searchengine/classify.go pkg/searchengine/classify_test.go search-sync-worker/handler.go search-sync-worker/handler_test.go
git commit -m "refactor(searchengine): export IsBulkItemSuccess as the shared bulk-result classifier"
```

---

### Task 2: Extract `MessageDoc` into `pkg/searchindex`

**Files:**
- Create: `pkg/searchindex/messagedoc.go`
- Create: `pkg/searchindex/messagedoc_test.go`
- Modify: `search-sync-worker/messages.go`
- Modify: `search-sync-worker/messages_test.go`
- Modify: `search-sync-worker/template.go` (delete `esPropertiesFromStruct`, moved to Task 5)

**Interfaces:**
- Consumes: `cassandra.Attachment`, `cassandra.Card`, `cassandra.DecodeAttachments(raw [][]byte) (out []Attachment, skipped int)` from `pkg/model/cassandra`.
- Produces: `searchindex.MessageDoc` struct, `searchindex.MessageFields` struct, `searchindex.NewMessageDoc(f MessageFields) MessageDoc`, `searchindex.MessageIndexName(prefix string, createdAt time.Time) string` — consumed by both `search-sync-worker/messages.go` (Task 2, this task) and the new migrator's message-doc builder (Task 10).

- [ ] **Step 1: Write the failing test**

Create `pkg/searchindex/messagedoc_test.go`:

```go
package searchindex_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/searchindex"
)

func TestNewMessageDoc(t *testing.T) {
	createdAt := time.Date(2026, 7, 24, 10, 0, 0, 0, time.UTC)
	f := searchindex.MessageFields{
		MessageID:   "msg1",
		RoomID:      "room1",
		SiteID:      "site-a",
		UserID:      "u1",
		UserAccount: "alice",
		Content:     "hello",
		CreatedAt:   createdAt,
		TShow:       true,
	}

	doc := searchindex.NewMessageDoc(f)

	assert.Equal(t, "msg1", doc.MessageID)
	assert.Equal(t, "room1", doc.RoomID)
	assert.Equal(t, "site-a", doc.SiteID)
	assert.Equal(t, "u1", doc.UserID)
	assert.Equal(t, "alice", doc.UserAccount)
	assert.False(t, doc.IsBot)
	assert.Equal(t, "hello", doc.Content)
	assert.True(t, doc.CreatedAt.Equal(createdAt))
	assert.True(t, doc.TShow)
	assert.Nil(t, doc.EditedAt)
	assert.Empty(t, doc.Attachments)
	assert.Nil(t, doc.Card)
}

func TestNewMessageDoc_IsBotFromAccountSuffix(t *testing.T) {
	doc := searchindex.NewMessageDoc(searchindex.MessageFields{UserAccount: "helper.bot"})
	assert.True(t, doc.IsBot)
}

func TestNewMessageDoc_AttachmentsDecodedAndTextJoined(t *testing.T) {
	blob1, _ := jsonMarshal(cassandra.Attachment{Title: "invoice.pdf", Description: "Q3 numbers"})
	blob2, _ := jsonMarshal(cassandra.Attachment{Title: "logo.png"})

	doc := searchindex.NewMessageDoc(searchindex.MessageFields{
		MessageID:   "msg1",
		Attachments: [][]byte{blob1, blob2},
	})

	assert.Len(t, doc.Attachments, 2)
	assert.Equal(t, "invoice.pdf Q3 numbers logo.png", doc.AttachmentText)
}

func TestNewMessageDoc_MalformedAttachmentBlobSkippedNotFatal(t *testing.T) {
	good, _ := jsonMarshal(cassandra.Attachment{Title: "ok.png"})
	doc := searchindex.NewMessageDoc(searchindex.MessageFields{
		Attachments: [][]byte{[]byte("not json"), good},
	})
	assert.Len(t, doc.Attachments, 1)
	assert.Equal(t, "ok.png", doc.AttachmentText)
}

func TestNewMessageDoc_CardPopulatesCardData(t *testing.T) {
	card := &cassandra.Card{Template: "t1", Data: []byte(`{"k":"v"}`)}
	doc := searchindex.NewMessageDoc(searchindex.MessageFields{Card: card})
	assert.Equal(t, card, doc.Card)
	assert.Equal(t, `{"k":"v"}`, doc.CardData)
}

func TestMessageIndexName(t *testing.T) {
	got := searchindex.MessageIndexName("messages-a-v2", time.Date(2026, 3, 9, 0, 0, 0, 0, time.UTC))
	assert.Equal(t, "messages-a-v2-2026-03", got)
}

func jsonMarshal(v any) ([]byte, error) {
	return json.Marshal(v)
}
```

Add `"encoding/json"` to the test file's imports alongside the others.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/searchindex/... -run TestNewMessageDoc -v`
Expected: FAIL — package `pkg/searchindex` has no `MessageDoc`/`MessageFields`/`NewMessageDoc`/`MessageIndexName`

- [ ] **Step 3: Write minimal implementation**

Create `pkg/searchindex/messagedoc.go`:

```go
package searchindex

import (
	"fmt"
	"strings"
	"time"

	"github.com/hmchangw/chat/pkg/model/cassandra"
)

// MessageDoc is the Elasticsearch document shape for the messages index.
// Shared by search-sync-worker (built live from a MessageEvent) and
// data-migration/es-index-migrator (built from a Cassandra messages_by_room
// row) — this is the one place the wire/mapping contract for the messages
// index is defined; do not redefine it anywhere else.
type MessageDoc struct {
	MessageID             string                 `json:"messageId"                              es:"keyword"`
	RoomID                string                 `json:"roomId"                                 es:"keyword"`
	SiteID                string                 `json:"siteId"                                 es:"keyword"`
	UserID                string                 `json:"userId"                                 es:"keyword"`
	UserAccount           string                 `json:"userAccount"                            es:"keyword"`
	IsBot                 bool                   `json:"isBot,omitempty"                        es:"boolean"`
	Content               string                 `json:"content,omitempty"                      es:"text,custom_analyzer"`
	CreatedAt             time.Time              `json:"createdAt"                              es:"date"`
	EditedAt              *time.Time             `json:"editedAt,omitempty"                     es:"date"`
	UpdatedAt             *time.Time             `json:"updatedAt,omitempty"                    es:"date"`
	ThreadParentID        string                 `json:"threadParentMessageId,omitempty"        es:"keyword"`
	ThreadParentCreatedAt *time.Time             `json:"threadParentMessageCreatedAt,omitempty" es:"date"`
	TShow                 bool                   `json:"tshow,omitempty"                        es:"boolean"`
	AttachmentText        string                 `json:"attachmentText,omitempty"                es:"text,custom_analyzer"`
	CardData              string                 `json:"cardData,omitempty"                      es:"text,custom_analyzer"`
	Attachments            []cassandra.Attachment `json:"attachments,omitempty" es:"object_disabled"`
	Card                   *cassandra.Card        `json:"card,omitempty"        es:"object_disabled"`
}

// MessageFields is the minimal, source-agnostic set of fields needed to
// build a MessageDoc. Callers with different source structs (a live
// MessageEvent's embedded model.Message, or a Cassandra messages_by_room
// row) adapt into this shape so pkg/searchindex never has to depend on
// either source package's full type.
type MessageFields struct {
	MessageID             string
	RoomID                string
	SiteID                string
	UserID                string
	UserAccount           string
	Content               string
	CreatedAt             time.Time
	EditedAt              *time.Time
	UpdatedAt             *time.Time
	ThreadParentID        string
	ThreadParentCreatedAt *time.Time
	TShow                 bool
	// Attachments carries the raw LIST<BLOB> encoding (one JSON blob per
	// attachment) exactly as stored in Cassandra's attachments column /
	// model.Message.Attachments — decoded here via cassandra.DecodeAttachments.
	Attachments [][]byte
	Card        *cassandra.Card
}

// IsBot reports whether account looks like a bot account (".bot" suffix).
// Duplicated here (rather than importing pkg/model, which would create an
// import cycle: pkg/model already imports pkg/model/cassandra, and this
// package's callers include services that import pkg/model) is avoided by
// keeping this a plain string check with the same semantics as
// model.IsBot — both must be kept in sync if the convention ever changes.
func isBotAccount(account string) bool {
	return strings.HasSuffix(account, ".bot")
}

// NewMessageDoc builds the ES document for the messages index from f.
func NewMessageDoc(f MessageFields) MessageDoc {
	doc := MessageDoc{
		MessageID:             f.MessageID,
		RoomID:                f.RoomID,
		SiteID:                f.SiteID,
		UserID:                f.UserID,
		UserAccount:           f.UserAccount,
		IsBot:                 isBotAccount(f.UserAccount),
		Content:               f.Content,
		CreatedAt:             f.CreatedAt,
		EditedAt:              f.EditedAt,
		UpdatedAt:             f.UpdatedAt,
		ThreadParentID:        f.ThreadParentID,
		ThreadParentCreatedAt: f.ThreadParentCreatedAt,
		TShow:                 f.TShow,
	}

	attachments, _ := cassandra.DecodeAttachments(f.Attachments)
	doc.Attachments = attachments
	var attachmentText []string
	for i := range attachments {
		a := &attachments[i]
		if a.Title != "" {
			attachmentText = append(attachmentText, a.Title)
		}
		if a.Description != "" {
			attachmentText = append(attachmentText, a.Description)
		}
	}
	doc.AttachmentText = strings.Join(attachmentText, " ")

	if f.Card != nil {
		doc.Card = f.Card
		doc.CardData = string(f.Card.Data)
	}

	return doc
}

// MessageIndexName returns the monthly index name for a message with the
// given createdAt: "{prefix}-{YYYY-MM}" (UTC).
func MessageIndexName(prefix string, createdAt time.Time) string {
	return fmt.Sprintf("%s-%s", prefix, createdAt.UTC().Format("2006-01"))
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/searchindex/... -run "TestNewMessageDoc|TestMessageIndexName" -v`
Expected: PASS

- [ ] **Step 5: Update `search-sync-worker/messages.go` to use the shared type**

Replace the local `MessageSearchIndex` struct and `newMessageSearchIndex` function entirely. The struct type is now `searchindex.MessageDoc` everywhere it was `MessageSearchIndex` (return types, `esPropertiesFromStruct[MessageSearchIndex]()` calls, etc. — the generic call becomes `esPropertiesFromStruct[searchindex.MessageDoc]()`, still fine, `template.go` hasn't moved yet — Task 5). Replace `newMessageSearchIndex`:

```go
func newMessageSearchIndex(evt *model.MessageEvent) searchindex.MessageDoc {
	attachments, _ := cassandra.DecodeAttachments(evt.Message.Attachments)
	_ = attachments // decoding happens inside searchindex.NewMessageDoc; this line is removed below
	return searchindex.NewMessageDoc(searchindex.MessageFields{
		MessageID:             evt.Message.ID,
		RoomID:                evt.Message.RoomID,
		SiteID:                evt.SiteID,
		UserID:                evt.Message.UserID,
		UserAccount:           evt.Message.UserAccount,
		Content:               evt.Message.Content,
		CreatedAt:             evt.Message.CreatedAt,
		EditedAt:              evt.Message.EditedAt,
		UpdatedAt:             evt.Message.UpdatedAt,
		ThreadParentID:        evt.Message.ThreadParentMessageID,
		ThreadParentCreatedAt: evt.Message.ThreadParentMessageCreatedAt,
		TShow:                 evt.Message.TShow,
		Attachments:           evt.Message.Attachments,
		Card:                  evt.Message.Card,
	})
}
```

(Delete the stray `attachments, _ := ...` / `_ = attachments` lines above — they were only illustrating that decoding no longer happens in this file; the final function body is just the `return searchindex.NewMessageDoc(...)` call.) Remove the now-unused `cassandra` import from `messages.go` if nothing else in the file references it (check with `goimports`/`make fmt`). Update `buildMessageAction` to call `searchindex.MessageIndexName(prefix, createdAt)` instead of the local `indexName(...)` helper (delete the local helper if `buildMessageAction` was its only caller).

- [ ] **Step 6: Update `search-sync-worker/messages_test.go`**

Rename every `MessageSearchIndex{...}` literal to `searchindex.MessageDoc{...}` (add the `searchindex` import). `TestNewMessageSearchIndex*` tests keep their names (they test `newMessageSearchIndex`, the thin wrapper, which still exists) but now assert against `searchindex.MessageDoc` fields — no assertion values change, only the type name.

- [ ] **Step 7: Run tests to verify nothing broke**

Run: `go build ./... && go vet -tags integration ./... && go test ./search-sync-worker/... ./pkg/searchindex/... -race -v`
Expected: PASS. Also check `search-sync-worker/parent_resolver_integration_test.go` (or equivalent, if this repo has one) for any remaining `MessageSearchIndex{` literal — rename to `searchindex.MessageDoc{` if found, since `go vet -tags integration` will not catch it unless run with the integration tag (this exact class of bug broke the original `chat` repo's PR #505 — do not repeat it here).

- [ ] **Step 8: Commit**

```bash
git add pkg/searchindex/messagedoc.go pkg/searchindex/messagedoc_test.go search-sync-worker/messages.go search-sync-worker/messages_test.go
git commit -m "refactor(searchindex): extract MessageDoc into pkg/searchindex"
```

---

### Task 3: Extract `SpotlightDoc` into `pkg/searchindex`

**Files:**
- Create: `pkg/searchindex/spotlightdoc.go`
- Create: `pkg/searchindex/spotlightdoc_test.go`
- Modify: `search-sync-worker/spotlight.go`
- Modify: `search-sync-worker/spotlight_test.go`

**Interfaces:**
- Consumes: nothing beyond the standard library.
- Produces: `searchindex.SpotlightDoc`, `searchindex.SpotlightFields`, `searchindex.NewSpotlightDoc(f SpotlightFields) SpotlightDoc` — consumed by `search-sync-worker/spotlight.go` (this task) and the migrator's spotlight-doc builder (Task 11).

- [ ] **Step 1: Write the failing test**

Create `pkg/searchindex/spotlightdoc_test.go`:

```go
package searchindex_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/searchindex"
)

func TestNewSpotlightDoc(t *testing.T) {
	joinedAt := time.Date(2026, 7, 24, 9, 0, 0, 0, time.UTC)

	doc := searchindex.NewSpotlightDoc(searchindex.SpotlightFields{
		UserAccount: "alice",
		RoomID:      "room1",
		RoomName:    "general",
		RoomType:    "channel",
		SiteID:      "site-a",
		JoinedAt:    joinedAt,
	})

	assert.Equal(t, "alice", doc.UserAccount)
	assert.Equal(t, "room1", doc.RoomID)
	assert.Equal(t, "general", doc.RoomName)
	assert.Equal(t, "channel", doc.RoomType)
	assert.Equal(t, "site-a", doc.SiteID)
	assert.True(t, doc.JoinedAt.Equal(joinedAt))
}

func TestNewSpotlightDoc_ZeroJoinedAt(t *testing.T) {
	doc := searchindex.NewSpotlightDoc(searchindex.SpotlightFields{UserAccount: "alice", RoomID: "room1"})
	assert.True(t, doc.JoinedAt.IsZero())
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/searchindex/... -run TestNewSpotlightDoc -v`
Expected: FAIL — no `SpotlightDoc`/`SpotlightFields`/`NewSpotlightDoc` in the package

- [ ] **Step 3: Write minimal implementation**

Create `pkg/searchindex/spotlightdoc.go`:

```go
package searchindex

import "time"

// SpotlightDoc is the Elasticsearch document shape for the spotlight
// (name-typeahead) index. Shared by search-sync-worker and
// data-migration/es-index-migrator.
type SpotlightDoc struct {
	UserAccount string    `json:"userAccount" es:"keyword"`
	RoomID      string    `json:"roomId"      es:"keyword"`
	RoomName    string    `json:"roomName"    es:"search_as_you_type,custom_analyzer"`
	RoomType    string    `json:"roomType"    es:"keyword"`
	SiteID      string    `json:"siteId"      es:"keyword"`
	JoinedAt    time.Time `json:"joinedAt"    es:"date"`
}

// SpotlightFields is the minimal, source-agnostic input to NewSpotlightDoc.
type SpotlightFields struct {
	UserAccount string
	RoomID      string
	RoomName    string
	RoomType    string
	SiteID      string
	JoinedAt    time.Time
}

// NewSpotlightDoc builds the ES document for the spotlight index from f.
func NewSpotlightDoc(f SpotlightFields) SpotlightDoc {
	return SpotlightDoc{
		UserAccount: f.UserAccount,
		RoomID:      f.RoomID,
		RoomName:    f.RoomName,
		RoomType:    f.RoomType,
		SiteID:      f.SiteID,
		JoinedAt:    f.JoinedAt,
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/searchindex/... -run TestNewSpotlightDoc -v`
Expected: PASS

- [ ] **Step 5: Update `search-sync-worker/spotlight.go`**

Replace `SpotlightSearchIndex` with `searchindex.SpotlightDoc` in the `BuildAction` return path. Replace `newSpotlightSearchIndex(account string, evt *model.InboxMemberEvent) SpotlightSearchIndex` body to build a `searchindex.SpotlightFields` from `account`/`evt.RoomID`/`evt.RoomName`/`evt.RoomType`/`evt.SiteID`/(zero or `time.UnixMilli(evt.JoinedAt).UTC()` when `evt.JoinedAt > 0`) and call `searchindex.NewSpotlightDoc(...)`, returning its result. Keep the function name and signature — only its body and return type change.

- [ ] **Step 6: Update `search-sync-worker/spotlight_test.go`**

Rename `SpotlightSearchIndex{...}` literals to `searchindex.SpotlightDoc{...}` (add the import). No assertion values change.

- [ ] **Step 7: Run tests to verify nothing broke**

Run: `go build ./... && go test ./search-sync-worker/... ./pkg/searchindex/... -race -v`
Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add pkg/searchindex/spotlightdoc.go pkg/searchindex/spotlightdoc_test.go search-sync-worker/spotlight.go search-sync-worker/spotlight_test.go
git commit -m "refactor(searchindex): extract SpotlightDoc into pkg/searchindex"
```

---

### Task 4: Extract user-room scripts + upsert doc into `pkg/searchindex`

**Files:**
- Create: `pkg/searchindex/userroomdoc.go`
- Create: `pkg/searchindex/userroomdoc_test.go`
- Modify: `search-sync-worker/user_room.go`
- Modify: `search-sync-worker/user_room_test.go`

**Interfaces:**
- Consumes: `encoding/json`.
- Produces: `searchindex.UserRoomUpsertDoc`, `searchindex.AddRoomScriptID`/`RemoveRoomScriptID` (string constants), `searchindex.AddRoomScript`/`RemoveRoomScript` (Painless source constants), `searchindex.StoredScriptBody(source string) json.RawMessage`, `searchindex.BuildAddRoomUpdateBody(account, rid string, ts, hss int64) json.RawMessage` (matches production's existing `account`/`Rooms: []string{}`-seeding signature — verify against the current `search-sync-worker/user_room.go` before transcribing, not from memory), `searchindex.BuildRemoveRoomUpdateBody(rid string, ts int64) json.RawMessage` — consumed by `search-sync-worker/user_room.go` (this task) and the migrator's user-room action builder (Task 12) and bootstrap (Task 13).

- [ ] **Step 1: Write the failing test**

Create `pkg/searchindex/userroomdoc_test.go`:

```go
package searchindex_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/searchindex"
)

func TestStoredScriptBody(t *testing.T) {
	body := searchindex.StoredScriptBody("ctx.op = 'none';")

	var decoded struct {
		Script struct {
			Lang   string `json:"lang"`
			Source string `json:"source"`
		} `json:"script"`
	}
	require.NoError(t, json.Unmarshal(body, &decoded))
	assert.Equal(t, "painless", decoded.Script.Lang)
	assert.Equal(t, "ctx.op = 'none';", decoded.Script.Source)
}

func TestBuildAddRoomUpdateBody(t *testing.T) {
	body := searchindex.BuildAddRoomUpdateBody("alice", "room1", 1000, 0)

	var decoded struct {
		Script struct {
			ID     string         `json:"id"`
			Params map[string]any `json:"params"`
		} `json:"script"`
		Upsert searchindex.UserRoomUpsertDoc `json:"upsert"`
	}
	require.NoError(t, json.Unmarshal(body, &decoded))
	assert.Equal(t, searchindex.AddRoomScriptID, decoded.Script.ID)
	assert.Equal(t, "room1", decoded.Script.Params["rid"])
	assert.InDelta(t, 1000, decoded.Script.Params["ts"], 0)
	assert.InDelta(t, 0, decoded.Script.Params["hss"], 0)
	assert.Equal(t, "alice", decoded.Upsert.UserAccount)
}

func TestBuildAddRoomUpdateBody_RestrictedRoomSeedsRestrictedRoomsMap(t *testing.T) {
	body := searchindex.BuildAddRoomUpdateBody("alice", "room1", 1000, 500)

	var decoded struct {
		Upsert searchindex.UserRoomUpsertDoc `json:"upsert"`
	}
	require.NoError(t, json.Unmarshal(body, &decoded))
	assert.Empty(t, decoded.Upsert.Rooms)
	assert.Equal(t, int64(500), decoded.Upsert.RestrictedRooms["room1"])
}

func TestBuildAddRoomUpdateBody_UnrestrictedRoomSeedsRoomsArray(t *testing.T) {
	body := searchindex.BuildAddRoomUpdateBody("alice", "room1", 1000, 0)

	var decoded struct {
		Upsert searchindex.UserRoomUpsertDoc `json:"upsert"`
	}
	require.NoError(t, json.Unmarshal(body, &decoded))
	assert.Equal(t, []string{"room1"}, decoded.Upsert.Rooms)
	assert.Empty(t, decoded.Upsert.RestrictedRooms)
}

func TestBuildRemoveRoomUpdateBody_NoUpsertSeed(t *testing.T) {
	body := searchindex.BuildRemoveRoomUpdateBody("room1", 2000)

	var decoded map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(body, &decoded))
	_, hasUpsert := decoded["upsert"]
	assert.False(t, hasUpsert, "remove path must not carry an upsert seed")

	var script struct {
		ID     string         `json:"id"`
		Params map[string]any `json:"params"`
	}
	require.NoError(t, json.Unmarshal(decoded["script"], &script))
	assert.Equal(t, searchindex.RemoveRoomScriptID, script.ID)
	assert.Equal(t, "room1", script.Params["rid"])
	assert.InDelta(t, 2000, script.Params["ts"], 0)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/searchindex/... -run "TestStoredScriptBody|TestBuildAddRoomUpdateBody|TestBuildRemoveRoomUpdateBody" -v`
Expected: FAIL — none of these identifiers exist yet in `pkg/searchindex`

- [ ] **Step 3: Write minimal implementation**

Create `pkg/searchindex/userroomdoc.go`:

```go
package searchindex

import (
	"encoding/json"
	"time"
)

// AddRoomScriptID / RemoveRoomScriptID are the ES stored-script ids under
// which AddRoomScript / RemoveRoomScript are registered. Bulk member
// updates reference these ids instead of inlining the full source per
// action. If a script's source ever changes incompatibly during a rolling
// deploy, bump the id suffix so old and new pods don't share a single
// mutated definition.
const (
	AddRoomScriptID    = "search-sync-user-room-add-v1"
	RemoveRoomScriptID = "search-sync-user-room-remove-v1"
)

// AddRoomScript / RemoveRoomScript implement application-level last-write-wins
// on (user, room) using params.ts (an event timestamp in millis). A stale
// call short-circuits via ctx.op = 'none', which tells ES to skip the write
// entirely — no version bump, no disk I/O.
//
// AddRoomScript additionally routes by params.hss:
//   - hss > 0  → rid lives in restrictedRooms{rid: hss} and is removed from rooms[]
//   - hss <= 0 → rid lives in rooms[] and is removed from restrictedRooms{}
//
// Painless lacks nullable primitives in script params: callers MUST pass
// hss = 0 for an unrestricted room, never a nil-shaped sentinel — a caller
// that ever passes hss for a room with no restriction must pass literal 0.
const (
	AddRoomScript = `if (ctx._source.roomTimestamps == null) { ctx._source.roomTimestamps = [:]; } ` +
		`if (ctx._source.rooms == null) { ctx._source.rooms = []; } ` +
		`if (ctx._source.restrictedRooms == null) { ctx._source.restrictedRooms = [:]; } ` +
		`long stored = ctx._source.roomTimestamps.containsKey(params.rid) ` +
		`? ((Number)ctx._source.roomTimestamps.get(params.rid)).longValue() : 0L; ` +
		`if (params.ts > stored) { ` +
		`if (params.hss > 0) { ` +
		`ctx._source.restrictedRooms[params.rid] = params.hss; ` +
		`ctx._source.rooms.removeIf(r -> r == params.rid); ` +
		`} else { ` +
		`if (!ctx._source.rooms.contains(params.rid)) { ctx._source.rooms.add(params.rid); } ` +
		`ctx._source.restrictedRooms.remove(params.rid); ` +
		`} ` +
		`ctx._source.roomTimestamps.put(params.rid, params.ts); ` +
		`ctx._source.updatedAt = params.now; ` +
		`} else { ctx.op = 'none'; }`

	RemoveRoomScript = `if (ctx._source.roomTimestamps == null) { ctx._source.roomTimestamps = [:]; } ` +
		`long stored = ctx._source.roomTimestamps.containsKey(params.rid) ` +
		`? ((Number)ctx._source.roomTimestamps.get(params.rid)).longValue() : 0L; ` +
		`if (params.ts > stored) { ` +
		`if (ctx._source.rooms != null) { ` +
		`int idx = ctx._source.rooms.indexOf(params.rid); ` +
		`if (idx >= 0) { ctx._source.rooms.remove(idx); } } ` +
		`if (ctx._source.restrictedRooms != null) { ctx._source.restrictedRooms.remove(params.rid); } ` +
		`ctx._source.roomTimestamps.put(params.rid, params.ts); ` +
		`} else { ctx.op = 'none'; }`
)

// UserRoomUpsertDoc is the full document inserted when the user has no
// prior user-room entry (the first time a room is added for this user).
// Rooms holds unrestricted room IDs; RestrictedRooms maps rid ->
// historySharedSince (millis) for rooms joined with a history restriction.
// RoomTimestamps seeds the per-room LWW timestamp guard used uniformly by
// both scripts.
type UserRoomUpsertDoc struct {
	UserAccount     string           `json:"userAccount"`
	Rooms           []string         `json:"rooms"`
	RestrictedRooms map[string]int64 `json:"restrictedRooms"`
	RoomTimestamps  map[string]int64 `json:"roomTimestamps"`
	CreatedAt       string           `json:"createdAt"`
	UpdatedAt       string           `json:"updatedAt"`
}

// StoredScriptBody wraps a Painless source string in the
// `PUT /_scripts/{id}` request envelope ES expects.
func StoredScriptBody(source string) json.RawMessage {
	body, _ := json.Marshal(map[string]any{
		"script": map[string]string{"lang": "painless", "source": source},
	})
	return body
}

// BuildAddRoomUpdateBody builds the ActionUpdate body for adding rid to
// account's user-room doc at timestamp ts (millis), with hss the room's
// HistorySharedSince in millis (0 for unrestricted). The upsert seed makes
// the first-insert document shape match what the script itself would
// produce on a subsequent update.
func BuildAddRoomUpdateBody(account, rid string, ts, hss int64) json.RawMessage {
	now := time.UnixMilli(ts).UTC().Format(time.RFC3339Nano)
	upsert := UserRoomUpsertDoc{
		UserAccount:     account,
		Rooms:           []string{},
		RestrictedRooms: map[string]int64{},
		RoomTimestamps:  map[string]int64{rid: ts},
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	if hss > 0 {
		upsert.RestrictedRooms[rid] = hss
	} else {
		upsert.Rooms = []string{rid}
	}

	body, _ := json.Marshal(map[string]any{
		"script": map[string]any{
			"id":     AddRoomScriptID,
			"params": map[string]any{"rid": rid, "ts": ts, "hss": hss, "now": now},
		},
		"upsert": upsert,
	})
	return body
}

// BuildRemoveRoomUpdateBody builds the ActionUpdate body for removing rid
// from a user's user-room doc at timestamp ts (millis). No upsert seed —
// removing from a nonexistent doc is the document_missing_exception 404
// case searchengine.IsBulkItemSuccess treats as benign, matching how
// search-sync-worker's own adapter calls this same script.
func BuildRemoveRoomUpdateBody(rid string, ts int64) json.RawMessage {
	body, _ := json.Marshal(map[string]any{
		"script": map[string]any{
			"id":     RemoveRoomScriptID,
			"params": map[string]any{"rid": rid, "ts": ts},
		},
	})
	return body
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/searchindex/... -run "TestStoredScriptBody|TestBuildAddRoomUpdateBody|TestBuildRemoveRoomUpdateBody" -v`
Expected: PASS

- [ ] **Step 5: Update `search-sync-worker/user_room.go`**

Delete the local `userRoomUpsertDoc` struct, `addRoomScriptID`/`removeRoomScriptID` constants, `addRoomScript`/`removeRoomScript` constants, `storedScriptBody`, `buildAddRoomUpdateBody`, `buildRemoveRoomUpdateBody`. `StoredScripts()` becomes:

```go
func (c *userRoomCollection) StoredScripts() map[string]json.RawMessage {
	return map[string]json.RawMessage{
		searchindex.AddRoomScriptID:    searchindex.StoredScriptBody(searchindex.AddRoomScript),
		searchindex.RemoveRoomScriptID: searchindex.StoredScriptBody(searchindex.RemoveRoomScript),
	}
}
```

In `BuildAction`, where the per-account fan-out builds each `searchengine.BulkAction`, the doc ID stays `account` (the file already computes this) and the body becomes `searchindex.BuildAddRoomUpdateBody(account, roomID, ts, hss)` / `searchindex.BuildRemoveRoomUpdateBody(roomID, ts)` respectively — replace whatever local call the file currently makes with these.

- [ ] **Step 6: Update `search-sync-worker/user_room_test.go`**

Rename any `userRoomUpsertDoc{...}` literal to `searchindex.UserRoomUpsertDoc{...}`; rename references to `addRoomScriptID`/`removeRoomScriptID` to `searchindex.AddRoomScriptID`/`searchindex.RemoveRoomScriptID`. No assertion values change.

- [ ] **Step 7: Run tests to verify nothing broke**

Run: `go build ./... && go test ./search-sync-worker/... ./pkg/searchindex/... -race -v`
Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add pkg/searchindex/userroomdoc.go pkg/searchindex/userroomdoc_test.go search-sync-worker/user_room.go search-sync-worker/user_room_test.go
git commit -m "refactor(searchindex): extract user-room scripts and upsert doc into pkg/searchindex"
```

---

### Task 5: Move `esPropertiesFromStruct` and the three template builders into `pkg/searchindex`

**Files:**
- Create: `pkg/searchindex/template.go`
- Create: `pkg/searchindex/template_test.go`
- Modify: `search-sync-worker/messages.go` (delegate `TemplateBody`/`TemplateName`)
- Modify: `search-sync-worker/spotlight.go` (delegate `TemplateBody`/`TemplateName`)
- Modify: `search-sync-worker/user_room.go` (delegate `TemplateBody`/`TemplateName`)
- Delete: `search-sync-worker/template.go` (moved, nothing left to keep here)
- Modify or delete: `search-sync-worker/template_test.go` (its assertions move to `pkg/searchindex/template_test.go`)

**Interfaces:**
- Consumes: `searchindex.MessageDoc`, `searchindex.SpotlightDoc` (Task 2, 3).
- Produces: `searchindex.EsPropertiesFromStruct[T any]() map[string]any`, `searchindex.MessageTemplateName(prefix string) string`, `searchindex.MessageTemplateBody(prefix string, devMode bool) json.RawMessage`, `searchindex.SpotlightTemplateName(indexName string) string`, `searchindex.SpotlightTemplateBody(indexName string, devMode bool) json.RawMessage`, `searchindex.UserRoomTemplateName(indexName string) string`, `searchindex.UserRoomTemplateBody(indexName string) json.RawMessage` — consumed by `search-sync-worker` (this task) and the migrator's bootstrap (Task 13), which needs to idempotently register the same three templates without depending on `search-sync-worker` internals (it is a separate `package main`, unimportable).

- [ ] **Step 1: Write the failing test**

Read `search-sync-worker/template_test.go` and `search-sync-worker/messages.go`/`spotlight.go`/`user_room.go` first to copy the exact existing assertions on `messageTemplateBody`/`spotlightTemplateBody`/`userRoomTemplateBody`/`esPropertiesFromStruct` verbatim into the new file below (shard/replica counts, analyzer definitions, `dynamic: false`, `flattened` mappings for `restrictedRooms`/`roomTimestamps` — these must not change, only move). Create `pkg/searchindex/template_test.go` with the moved test bodies, updated to call the new exported names (`searchindex.EsPropertiesFromStruct[searchindex.MessageDoc]()`, `searchindex.MessageTemplateBody(...)`, etc.) and package `searchindex_test`.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/searchindex/... -v`
Expected: FAIL — the exported template functions don't exist yet in `pkg/searchindex`

- [ ] **Step 3: Write minimal implementation**

Create `pkg/searchindex/template.go` by copying `search-sync-worker/template.go`'s `esPropertiesFromStruct[T any]()` body verbatim (rename to exported `EsPropertiesFromStruct[T any]()`, same reflection-over-`es`/`json`-tags logic, same `object_disabled` handling), then copying `messageTemplateBody`/`spotlightTemplateBody`/`userRoomTemplateBody` from `messages.go`/`spotlight.go`/`user_room.go` verbatim (renamed `MessageTemplateBody`/`SpotlightTemplateBody`/`UserRoomTemplateBody`, operating on `searchindex.MessageDoc`/`searchindex.SpotlightDoc` directly since they now live in this package) plus `MessageTemplateName`/`SpotlightTemplateName` (both `fmt.Sprintf("%s_template", StripVersionBase(prefix))`) and `UserRoomTemplateName` (`fmt.Sprintf("%s_template", indexName)` — no version-stripping, matching the original's unversioned user-room index name).

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/searchindex/... -v`
Expected: PASS, byte-identical template JSON to what the moved test asserted before the move

- [ ] **Step 5: Update `search-sync-worker/messages.go`/`spotlight.go`/`user_room.go`**

Each collection's `TemplateName()`/`TemplateBody()`/`MappingUpdate()` method becomes a one-line delegate, e.g. for messages:

```go
func (c *messageCollection) TemplateName() string { return searchindex.MessageTemplateName(c.indexPrefix) }
func (c *messageCollection) TemplateBody() json.RawMessage { return searchindex.MessageTemplateBody(c.indexPrefix, c.devMode) }
func (c *messageCollection) MappingUpdate() (string, json.RawMessage) {
	body, _ := json.Marshal(map[string]any{"properties": searchindex.EsPropertiesFromStruct[searchindex.MessageDoc]()})
	return searchindex.IndexPattern(c.indexPrefix), body
}
```

Mirror the same delegation pattern for `spotlightCollection`/`userRoomCollection`. Delete `search-sync-worker/template.go` entirely (empty of remaining content) and delete or empty `search-sync-worker/template_test.go` (its assertions now live in `pkg/searchindex/template_test.go`).

- [ ] **Step 6: Run tests to verify nothing broke**

Run: `go build ./... && go vet -tags integration ./... && go test ./search-sync-worker/... ./pkg/searchindex/... -race -v`
Expected: PASS — template JSON output is unchanged (same test assertions, just relocated), `make lint` clean.

- [ ] **Step 7: Commit**

```bash
git add pkg/searchindex/template.go pkg/searchindex/template_test.go search-sync-worker/messages.go search-sync-worker/spotlight.go search-sync-worker/user_room.go
git rm search-sync-worker/template.go search-sync-worker/template_test.go 2>/dev/null || true
git commit -m "refactor(searchindex): move ES template/mapping builders into pkg/searchindex"
```

**Phase A checkpoint:** run `go build ./... && go vet -tags integration ./... && go test ./... -race` and `make lint` before starting Phase B. `search-sync-worker` must behave identically to before Phase A — this is the load-bearing guarantee the rest of the plan depends on.

---

## Phase B — `data-migration/es-index-migrator`

### Task 6: `config.go` — configuration and validation

**Files:**
- Create: `data-migration/es-index-migrator/config.go`
- Create: `data-migration/es-index-migrator/config_test.go`

**Interfaces:**
- Consumes: nothing beyond `caarlos0/env`.
- Produces: `type config struct{...}`, `func loadConfig() (config, error)` — consumed by `main.go` (Task 14).

- [ ] **Step 1: Write the failing test**

Create `data-migration/es-index-migrator/config_test.go`:

```go
package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setRequiredEnv(t *testing.T) {
	t.Helper()
	env := map[string]string{
		"SITE_ID":             "site-a",
		"SEARCH_URL":          "http://localhost:9200",
		"MSG_INDEX_PREFIX":    "messages-a-v1",
		"SPOTLIGHT_INDEX":     "spotlight-a-v1",
		"USER_ROOM_INDEX":     "user-room-a",
		"MIGRATION_START_AT":  "2025-07-01T00:00:00Z",
		"MIGRATION_END_AT":    "2026-07-01T00:00:00Z",
		"MESSAGE_BUCKET_HOURS": "72",
		"MONGO_URI":           "mongodb://localhost:27017",
		"CASSANDRA_HOSTS":     "localhost",
	}
	for k, v := range env {
		t.Setenv(k, v)
	}
}

func TestLoadConfig_Valid(t *testing.T) {
	setRequiredEnv(t)

	cfg, err := loadConfig()

	require.NoError(t, err)
	assert.Equal(t, "site-a", cfg.SiteID)
	assert.Equal(t, 500, cfg.BulkBatchSize)
	assert.Equal(t, 4, cfg.WorkerConcurrency)
	assert.Equal(t, "chat", cfg.MongoDB)
	assert.Equal(t, "chat", cfg.CassandraKeyspace)
	want, _ := time.Parse(time.RFC3339, "2025-07-01T00:00:00Z")
	assert.True(t, cfg.MigrationStartAt.Equal(want))
}

func TestLoadConfig_MissingRequiredField(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("SITE_ID", "")

	_, err := loadConfig()

	require.Error(t, err)
}

func TestLoadConfig_EndNotAfterStart(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("MIGRATION_START_AT", "2026-01-01T00:00:00Z")
	t.Setenv("MIGRATION_END_AT", "2026-01-01T00:00:00Z")

	_, err := loadConfig()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "MIGRATION_END_AT")
}

func TestLoadConfig_NonPositiveBatchSize(t *testing.T) {
	for _, v := range []string{"0", "-5"} {
		t.Run(v, func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv("BULK_BATCH_SIZE", v)

			_, err := loadConfig()

			require.Error(t, err)
			assert.Contains(t, err.Error(), "BULK_BATCH_SIZE")
		})
	}
}

func TestLoadConfig_NonPositiveWorkerConcurrency(t *testing.T) {
	for _, v := range []string{"0", "-1"} {
		t.Run(v, func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv("WORKER_CONCURRENCY", v)

			_, err := loadConfig()

			require.Error(t, err)
		})
	}
}

func TestLoadConfig_NonPositiveMessageBucketHours(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("MESSAGE_BUCKET_HOURS", "0")

	_, err := loadConfig()

	require.Error(t, err)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./data-migration/es-index-migrator/... -run TestLoadConfig -v`
Expected: FAIL — package doesn't exist / `loadConfig` undefined

- [ ] **Step 3: Write minimal implementation**

Create `data-migration/es-index-migrator/config.go`:

```go
package main

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"
)

type config struct {
	SiteID string `env:"SITE_ID,required"`

	SearchURL           string `env:"SEARCH_URL,required"`
	SearchUsername      string `env:"SEARCH_USERNAME"      envDefault:""`
	SearchPassword      string `env:"SEARCH_PASSWORD"      envDefault:""`
	SearchTLSSkipVerify bool   `env:"SEARCH_TLS_SKIP_VERIFY" envDefault:"false"`

	MsgIndexPrefix string `env:"MSG_INDEX_PREFIX,required"`
	SpotlightIndex string `env:"SPOTLIGHT_INDEX,required"`
	UserRoomIndex  string `env:"USER_ROOM_INDEX,required"`

	// MigrationStartAt/MigrationEndAt bound the messages backfill window
	// ([start, end)). Spotlight and user-room backfill the site's full
	// current subscription set unconditionally — see the plan's Global
	// Constraints for why subscriptions aren't windowed.
	MigrationStartAt time.Time `env:"MIGRATION_START_AT,required"`
	MigrationEndAt   time.Time `env:"MIGRATION_END_AT,required"`

	MessageBucketHours int `env:"MESSAGE_BUCKET_HOURS,required"`

	MongoURI      string `env:"MONGO_URI,required"`
	MongoDB       string `env:"MONGO_DB"       envDefault:"chat"`
	MongoUsername string `env:"MONGO_USERNAME" envDefault:""`
	MongoPassword string `env:"MONGO_PASSWORD" envDefault:""`

	CassandraHosts    string `env:"CASSANDRA_HOSTS,required"`
	CassandraKeyspace string `env:"CASSANDRA_KEYSPACE" envDefault:"chat"`
	CassandraUsername string `env:"CASSANDRA_USERNAME" envDefault:""`
	CassandraPassword string `env:"CASSANDRA_PASSWORD" envDefault:""`
	CassandraNumConns int    `env:"CASSANDRA_NUM_CONNS" envDefault:"8"`

	BulkBatchSize     int `env:"BULK_BATCH_SIZE"     envDefault:"500"`
	WorkerConcurrency int `env:"WORKER_CONCURRENCY"  envDefault:"4"`
}

func loadConfig() (config, error) {
	cfg, err := env.ParseAs[config]()
	if err != nil {
		return config{}, fmt.Errorf("parse config: %w", err)
	}

	if !cfg.MigrationEndAt.After(cfg.MigrationStartAt) {
		return config{}, fmt.Errorf("MIGRATION_END_AT (%s) must be after MIGRATION_START_AT (%s)",
			cfg.MigrationEndAt, cfg.MigrationStartAt)
	}
	if cfg.MessageBucketHours <= 0 {
		return config{}, fmt.Errorf("MESSAGE_BUCKET_HOURS must be positive, got %d", cfg.MessageBucketHours)
	}
	if cfg.BulkBatchSize <= 0 {
		return config{}, fmt.Errorf("BULK_BATCH_SIZE must be positive, got %d", cfg.BulkBatchSize)
	}
	if cfg.WorkerConcurrency <= 0 {
		return config{}, fmt.Errorf("WORKER_CONCURRENCY must be positive, got %d", cfg.WorkerConcurrency)
	}

	return cfg, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./data-migration/es-index-migrator/... -run TestLoadConfig -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add data-migration/es-index-migrator/config.go data-migration/es-index-migrator/config_test.go
git commit -m "feat(es-index-migrator): add config parsing and validation"
```

---

### Task 7: `store.go` — interfaces and mocks

**Files:**
- Create: `data-migration/es-index-migrator/store.go`

**Interfaces:**
- Produces:
  - `type MessageSource interface { StreamMessages(ctx context.Context, siteID, roomID string, from, to time.Time, fn func(cassandra.Message) error) error }` — consumed by Task 8 (impl) and Task 14 (runner).
  - `type SubscriptionSource interface { RoomIDs(ctx context.Context, siteID string) ([]string, error); Subscriptions(ctx context.Context, siteID string) ([]model.Subscription, error) }` — consumed by Task 9 (impl) and Task 14 (runner).
  - `type ESStore interface { Bulk(ctx context.Context, actions []searchengine.BulkAction) ([]searchengine.BulkResult, error) }` — narrowed to exactly what the flusher needs (Task 12), satisfied directly by `searchengine.SearchEngine`.

- [ ] **Step 1: Write the failing test**

This task has no independent test — `store.go` only declares interfaces (per CLAUDE.md: "Define interfaces in the consumer, not the implementer"). Its correctness is verified by Task 8/9's tests compiling against these interfaces and by `mock_store_test.go` (generated in this step) compiling. Skip Steps 1-2 (no separate red/green cycle for a pure interface file); proceed to Step 3.

- [ ] **Step 3: Write the interfaces**

Create `data-migration/es-index-migrator/store.go`:

```go
package main

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

import (
	"context"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/searchengine"
)

// MessageSource streams a room's Cassandra message history in [from, to)
// to fn, one row at a time. fn must not retain row beyond its call —
// implementations reuse row's underlying buffers across calls where
// possible to avoid materializing a whole room's history in memory at
// once. A non-nil error from fn aborts the stream and is returned as-is.
type MessageSource interface {
	StreamMessages(ctx context.Context, siteID, roomID string, from, to time.Time, fn func(cassandra.Message) error) error
}

// SubscriptionSource reads a site's current MongoDB subscriptions collection.
type SubscriptionSource interface {
	// RoomIDs returns the distinct room IDs any of the site's subscriptions
	// reference — the candidate set for the messages backfill (Collection 1).
	RoomIDs(ctx context.Context, siteID string) ([]string, error)
	// Subscriptions returns every current subscription for the site — the
	// full source for the spotlight and user-room backfills (Collections
	// 2 and 3). Every row is an active membership (subscriptions are
	// hard-deleted on leave, never soft-flagged — see Global Constraints).
	Subscriptions(ctx context.Context, siteID string) ([]model.Subscription, error)
}

// ESStore is the narrow slice of searchengine.SearchEngine the flusher
// needs. Defined here (the consumer), not in pkg/searchengine, per
// CLAUDE.md's interface convention.
type ESStore interface {
	Bulk(ctx context.Context, actions []searchengine.BulkAction) ([]searchengine.BulkResult, error)
}
```

- [ ] **Step 4: Generate mocks**

Run: `cd data-migration/es-index-migrator && go generate ./...` (or `make generate SERVICE=data-migration/es-index-migrator` once the Makefile's `SERVICE` path-matching picks up the new directory — verify with `make generate SERVICE=data-migration/es-index-migrator` first; fall back to running mockgen directly if the Makefile doesn't yet resolve nested `data-migration/` service paths).
Expected: `mock_store_test.go` is generated with `MockMessageSource`, `MockSubscriptionSource`, `MockESStore`.

- [ ] **Step 5: Commit**

```bash
git add data-migration/es-index-migrator/store.go data-migration/es-index-migrator/mock_store_test.go
git commit -m "feat(es-index-migrator): define store interfaces"
```

---

### Task 8: `messagesource_cassandra.go` — streaming bucket-walk message reader

**Files:**
- Create: `data-migration/es-index-migrator/messagesource_cassandra.go`
- Create: `data-migration/es-index-migrator/messagesource_cassandra_test.go`
- Create: `data-migration/es-index-migrator/messagesource_integration_test.go` (`//go:build integration`)

**Interfaces:**
- Consumes: `pkg/cassutil.Connect`, `pkg/msgbucket.Sizer`, `pkg/model/cassandra.Message`.
- Produces: `type cassandraMessageSource struct{...}`, `func newCassandraMessageSource(session *gocql.Session, sizer msgbucket.Sizer) *cassandraMessageSource` implementing `MessageSource` (Task 7) — consumed by `main.go` (Task 14) and `runner.go` (Task 14).

This job reads `messages_by_room` rows that were themselves written directly into the plaintext `msg`/`attachments`/`card` columns by whatever process populated the table — never through the live at-rest-encryption write path — so it reads those columns as-is. It does not read or decrypt `enc_payload`/`enc_meta` and has no dependency on `pkg/atrest` or Vault. (Contrast with `history-service`, which reads the same table on the *live* serving path and does decrypt — that asymmetry is intentional and specific to how this job's source data was populated, not an oversight.)

- [ ] **Step 1: Write the failing test**

Create `data-migration/es-index-migrator/messagesource_cassandra_test.go` — this task's core logic (bucket iteration bounds, deleted-row skip) is unit-testable only around the bucket-range computation, since the actual row scan needs a real `*gocql.Session`. Write the pure-logic unit tests here, and defer full read-path coverage to the integration test in Step 6:

```go
package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/msgbucket"
)

func TestBucketRange_SingleBucketWindow(t *testing.T) {
	sizer := msgbucket.New(72 * time.Hour)
	from := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 7, 2, 0, 0, 0, 0, time.UTC)

	buckets := bucketRange(sizer, from, to)

	assert.Equal(t, []int64{sizer.Of(from)}, buckets)
}

func TestBucketRange_MultiBucketWindowIncludesEveryBucketTouchingTheRange(t *testing.T) {
	sizer := msgbucket.New(72 * time.Hour)
	from := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	to := from.Add(200 * time.Hour) // spans 3 windows of 72h

	buckets := bucketRange(sizer, from, to)

	assert.Len(t, buckets, 3)
	assert.Equal(t, sizer.Of(from), buckets[0])
	assert.Equal(t, sizer.Of(to.Add(-time.Millisecond)), buckets[len(buckets)-1])
}

func TestBucketRange_ToExactlyOnABucketBoundaryExcludesThatBucket(t *testing.T) {
	sizer := msgbucket.New(72 * time.Hour)
	from := time.UnixMilli(sizer.Of(time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)))
	to := time.UnixMilli(sizer.Next(sizer.Of(from))) // exactly the next bucket boundary

	buckets := bucketRange(sizer, from, to)

	// [from, to) — the bucket starting exactly at `to` holds no row < to, so it must not be walked.
	assert.Equal(t, []int64{sizer.Of(from)}, buckets)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./data-migration/es-index-migrator/... -run TestBucketRange -v`
Expected: FAIL — `bucketRange` undefined

- [ ] **Step 3: Write minimal implementation**

Create `data-migration/es-index-migrator/messagesource_cassandra.go`:

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/gocql/gocql"

	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/msgbucket"
)

// messageColumns is the explicit set of messages_by_room columns this
// service needs, excluding columns no search doc ever uses (mentions,
// quoted_parent_message, card_action, sys_msg_data, pinned_at/pinned_by,
// visible_to, reactions, type) and excluding enc_payload/enc_meta — this
// job's source rows were written directly into the plaintext msg/
// attachments/card columns, never through the live at-rest-encryption
// write path, so there is nothing to decrypt here.
const messageColumns = "room_id, created_at, message_id, sender, msg, attachments, card, " +
	"tshow, thread_parent_id, thread_parent_created_at, deleted, site_id, edited_at, updated_at"

const messagesByRoomQuery = "SELECT " + messageColumns + " FROM messages_by_room " +
	"WHERE room_id = ? AND bucket = ? AND created_at >= ? AND created_at < ?"

type cassandraMessageSource struct {
	session *gocql.Session
	sizer   msgbucket.Sizer
}

func newCassandraMessageSource(session *gocql.Session, sizer msgbucket.Sizer) *cassandraMessageSource {
	return &cassandraMessageSource{session: session, sizer: sizer}
}

// bucketRange returns every bucket value that can contain a row with
// created_at in [from, to). Half-open: a bucket starting exactly at `to`
// holds no row < to, so it is excluded.
func bucketRange(sizer msgbucket.Sizer, from, to time.Time) []int64 {
	var buckets []int64
	for b := sizer.Of(from); b < to.UnixMilli(); b = sizer.Next(b) {
		buckets = append(buckets, b)
	}
	return buckets
}

func (s *cassandraMessageSource) StreamMessages(
	ctx context.Context, siteID, roomID string, from, to time.Time, fn func(cassandra.Message) error,
) error {
	for _, bucket := range bucketRange(s.sizer, from, to) {
		iter := s.session.Query(messagesByRoomQuery, roomID, bucket, from, to).WithContext(ctx).Iter()

		var row cassandra.Message
		for iter.Scan(&row.RoomID, &row.CreatedAt, &row.MessageID, &row.Sender, &row.Msg, &row.Attachments,
			&row.Card, &row.TShow, &row.ThreadParentID, &row.ThreadParentCreatedAt, &row.Deleted, &row.SiteID,
			&row.EditedAt, &row.UpdatedAt) {

			if row.Deleted {
				row = cassandra.Message{}
				continue
			}
			if row.SiteID == "" {
				row.SiteID = siteID
			}

			if err := fn(row); err != nil {
				_ = iter.Close()
				return fmt.Errorf("handle message %s in room %s: %w", row.MessageID, roomID, err)
			}

			row = cassandra.Message{}
		}
		if err := iter.Close(); err != nil {
			return fmt.Errorf("read messages_by_room for room %s bucket %d: %w", roomID, bucket, err)
		}
	}
	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./data-migration/es-index-migrator/... -run TestBucketRange -v`
Expected: PASS

- [ ] **Step 5: Run `go vet`/`go build` to confirm the gocql `Scan` column list matches the struct field types**

Run: `go build ./data-migration/es-index-migrator/...`
Expected: builds clean — this is the step that would catch a positional-scan mismatch between `messageColumns`' order and the `iter.Scan(...)` argument order; if it fails, fix the mismatched pair (`gocql` returns a scan-count-mismatch error at runtime, not a compile error, so double-check the two lists are in the exact same order by eye in addition to running the build).

- [ ] **Step 6: Write the integration test**

Create `data-migration/es-index-migrator/messagesource_integration_test.go`:

```go
//go:build integration

package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/msgbucket"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

func insertTestMessage(t *testing.T, session *gocql.Session, roomID string, bucket int64, createdAt time.Time, msgID, msg string, deleted bool) {
	t.Helper()
	err := session.Query(
		"INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg, deleted, site_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
		roomID, bucket, createdAt, msgID, cassandra.Participant{ID: "u1", Account: "alice"}, msg, deleted, "site-a",
	).Exec()
	require.NoError(t, err)
}

func TestCassandraMessageSource_StreamMessages_MultiBucketWindow(t *testing.T) {
	_, session, _ := testutil.CassandraKeyspace(t, "esmig")
	sizer := msgbucket.New(72 * time.Hour)
	source := newCassandraMessageSource(session, sizer)

	from := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	to := from.Add(200 * time.Hour)
	inFirstBucket := from.Add(time.Hour)
	inThirdBucket := from.Add(150 * time.Hour)
	outsideWindow := to.Add(time.Hour)

	insertTestMessage(t, session, "room1", sizer.Of(inFirstBucket), inFirstBucket, "m1", "hello", false)
	insertTestMessage(t, session, "room1", sizer.Of(inThirdBucket), inThirdBucket, "m2", "world", false)
	insertTestMessage(t, session, "room1", sizer.Of(outsideWindow), outsideWindow, "m3", "too late", false)
	insertTestMessage(t, session, "room1", sizer.Of(inFirstBucket), inFirstBucket, "m4", "gone", true)

	var got []cassandra.Message
	err := source.StreamMessages(context.Background(), "site-a", "room1", from, to, func(m cassandra.Message) error {
		got = append(got, m)
		return nil
	})

	require.NoError(t, err)
	require.Len(t, got, 2, "expects m1 and m2 only: m3 is outside the window, m4 is deleted")
	ids := []string{got[0].MessageID, got[1].MessageID}
	require.ElementsMatch(t, []string{"m1", "m2"}, ids)
}

func TestCassandraMessageSource_StreamMessages_CallbackErrorAborts(t *testing.T) {
	_, session, _ := testutil.CassandraKeyspace(t, "esmig")
	sizer := msgbucket.New(72 * time.Hour)
	source := newCassandraMessageSource(session, sizer)

	from := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	to := from.Add(time.Hour)
	insertTestMessage(t, session, "room2", sizer.Of(from), from, "m1", "hello", false)

	callCount := 0
	err := source.StreamMessages(context.Background(), "site-a", "room2", from, to, func(m cassandra.Message) error {
		callCount++
		return assertErr
	})

	require.ErrorIs(t, err, assertErr)
	require.Equal(t, 1, callCount)
}

var assertErr = errors.New("boom")
```

(Add `"errors"` to this file's imports alongside `"testing"`/`"time"`.)

- [ ] **Step 7: Run the integration test**

Run: `make test-integration SERVICE=data-migration/es-index-migrator` (requires Docker)
Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add data-migration/es-index-migrator/messagesource_cassandra.go data-migration/es-index-migrator/messagesource_cassandra_test.go data-migration/es-index-migrator/messagesource_integration_test.go
git commit -m "feat(es-index-migrator): add streaming Cassandra message source"
```

---

### Task 9: `subscriptionsource_mongo.go` — subscription reader

**Files:**
- Create: `data-migration/es-index-migrator/subscriptionsource_mongo.go`
- Create: `data-migration/es-index-migrator/subscriptionsource_mongo_test.go`
- Create: `data-migration/es-index-migrator/subscriptionsource_mongo_integration_test.go` (`//go:build integration`)

**Interfaces:**
- Consumes: `pkg/mongoutil.Connect`, `pkg/model.Subscription`.
- Produces: `type mongoSubscriptionSource struct{...}`, `func newMongoSubscriptionSource(db *mongo.Database) *mongoSubscriptionSource` implementing `SubscriptionSource` (Task 7) — consumed by `main.go`/`runner.go` (Task 14).

- [ ] **Step 1: Write the failing test**

Create `data-migration/es-index-migrator/subscriptionsource_mongo_test.go` — this task's `distinctStringRoomIDs` conversion helper (Mongo `Distinct` returns `[]interface{}`, must be defensively converted, matching the Mongo-projection/decode conventions elsewhere in this repo) is the pure-logic unit worth testing without a real Mongo:

```go
package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestToStringSlice_DropsNonStringAndEmptyValues(t *testing.T) {
	got := toStringSlice([]any{"room1", "", 42, "room2", nil})
	assert.Equal(t, []string{"room1", "room2"}, got)
}

func TestToStringSlice_EmptyInput(t *testing.T) {
	assert.Empty(t, toStringSlice(nil))
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./data-migration/es-index-migrator/... -run TestToStringSlice -v`
Expected: FAIL — `toStringSlice` undefined

- [ ] **Step 3: Write minimal implementation**

Create `data-migration/es-index-migrator/subscriptionsource_mongo.go`:

```go
package main

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/model"
)

type mongoSubscriptionSource struct {
	col *mongo.Collection
}

func newMongoSubscriptionSource(db *mongo.Database) *mongoSubscriptionSource {
	return &mongoSubscriptionSource{col: db.Collection("subscriptions")}
}

// toStringSlice converts a mongo.Collection.Distinct result (untyped
// []any) to []string, dropping any non-string or empty value defensively
// — a malformed roomId on one row must not break enumeration for every
// other room.
func toStringSlice(values []any) []string {
	out := make([]string, 0, len(values))
	for _, v := range values {
		s, ok := v.(string)
		if !ok || s == "" {
			continue
		}
		out = append(out, s)
	}
	return out
}

func (s *mongoSubscriptionSource) RoomIDs(ctx context.Context, siteID string) ([]string, error) {
	values, err := s.col.Distinct(ctx, "roomId", bson.M{"siteId": siteID})
	if err != nil {
		return nil, fmt.Errorf("distinct roomId for site %s: %w", siteID, err)
	}
	return toStringSlice(values), nil
}

var subscriptionProjection = bson.M{
	"_id": 1, "u": 1, "roomId": 1, "siteId": 1, "name": 1, "roomType": 1,
	"historySharedSince": 1, "joinedAt": 1,
}

func (s *mongoSubscriptionSource) Subscriptions(ctx context.Context, siteID string) ([]model.Subscription, error) {
	cur, err := s.col.Find(ctx, bson.M{"siteId": siteID}, options.Find().SetProjection(subscriptionProjection))
	if err != nil {
		return nil, fmt.Errorf("find subscriptions for site %s: %w", siteID, err)
	}
	defer func() { _ = cur.Close(ctx) }() // read-only cursor; a close error here can't affect already-decoded results

	var subs []model.Subscription
	if err := cur.All(ctx, &subs); err != nil {
		return nil, fmt.Errorf("decode subscriptions for site %s: %w", siteID, err)
	}
	return subs, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./data-migration/es-index-migrator/... -run TestToStringSlice -v`
Expected: PASS

- [ ] **Step 5: Write the integration test**

Create `data-migration/es-index-migrator/subscriptionsource_mongo_integration_test.go`:

```go
//go:build integration

package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMongoSubscriptionSource_RoomIDs_ScopedBySite(t *testing.T) {
	db := testutil.MongoDB(t, "esmig")
	source := newMongoSubscriptionSource(db)
	ctx := context.Background()

	_, err := db.Collection("subscriptions").InsertMany(ctx, []any{
		model.Subscription{ID: "s1", SiteID: "site-a", RoomID: "room1", User: model.SubscriptionUser{Account: "alice"}, JoinedAt: time.Now()},
		model.Subscription{ID: "s2", SiteID: "site-a", RoomID: "room2", User: model.SubscriptionUser{Account: "bob"}, JoinedAt: time.Now()},
		model.Subscription{ID: "s3", SiteID: "site-b", RoomID: "room3", User: model.SubscriptionUser{Account: "carol"}, JoinedAt: time.Now()},
	})
	require.NoError(t, err)

	got, err := source.RoomIDs(ctx, "site-a")

	require.NoError(t, err)
	require.ElementsMatch(t, []string{"room1", "room2"}, got)
}

func TestMongoSubscriptionSource_Subscriptions_ReturnsFullDocsForSite(t *testing.T) {
	db := testutil.MongoDB(t, "esmig")
	source := newMongoSubscriptionSource(db)
	ctx := context.Background()
	joined := time.Now().Truncate(time.Millisecond)

	_, err := db.Collection("subscriptions").InsertOne(ctx, model.Subscription{
		ID: "s1", SiteID: "site-a", RoomID: "room1", RoomType: model.RoomTypeChannel,
		Name: "general", User: model.SubscriptionUser{Account: "alice"}, JoinedAt: joined,
	})
	require.NoError(t, err)

	got, err := source.Subscriptions(ctx, "site-a")

	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "alice", got[0].User.Account)
	require.Equal(t, "general", got[0].Name)
	require.Equal(t, model.RoomTypeChannel, got[0].RoomType)
	require.True(t, got[0].JoinedAt.Equal(joined))
}

func TestMongoSubscriptionSource_Subscriptions_EmptyForUnknownSite(t *testing.T) {
	db := testutil.MongoDB(t, "esmig")
	source := newMongoSubscriptionSource(db)

	got, err := source.Subscriptions(context.Background(), "no-such-site")

	require.NoError(t, err)
	require.Empty(t, got)
}
```

- [ ] **Step 6: Run the integration test**

Run: `make test-integration SERVICE=data-migration/es-index-migrator`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add data-migration/es-index-migrator/subscriptionsource_mongo.go data-migration/es-index-migrator/subscriptionsource_mongo_test.go data-migration/es-index-migrator/subscriptionsource_mongo_integration_test.go
git commit -m "feat(es-index-migrator): add Mongo subscription source"
```

---

### Task 10: `messageaction.go` — build a message bulk action from a Cassandra row

**Files:**
- Create: `data-migration/es-index-migrator/messageaction.go`
- Create: `data-migration/es-index-migrator/messageaction_test.go`

**Interfaces:**
- Consumes: `searchindex.MessageFields`, `searchindex.NewMessageDoc`, `searchindex.MessageIndexName` (Task 2); `cassandra.Message` (Task 8).
- Produces: `func buildMessageAction(msg cassandra.Message, indexPrefix string) (searchengine.BulkAction, error)` — consumed by `runner.go` (Task 14).

- [ ] **Step 1: Write the failing test**

Create `data-migration/es-index-migrator/messageaction_test.go`:

```go
package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/searchengine"
	"github.com/hmchangw/chat/pkg/searchindex"
)

func TestBuildMessageAction(t *testing.T) {
	createdAt := time.Date(2026, 3, 9, 7, 0, 0, 0, time.UTC)
	msg := cassandra.Message{
		RoomID: "room1", MessageID: "msg1", SiteID: "site-a", CreatedAt: createdAt,
		Sender: cassandra.Participant{ID: "u1", Account: "alice"}, Msg: "hello",
	}

	action, err := buildMessageAction(msg, "messages-a-v1")

	require.NoError(t, err)
	assert.Equal(t, searchengine.ActionIndex, action.Action)
	assert.Equal(t, "messages-a-v1-2026-03", action.Index)
	assert.Equal(t, "msg1", action.DocID)
	assert.Equal(t, createdAt.UnixMilli(), action.Version)

	var doc searchindex.MessageDoc
	require.NoError(t, json.Unmarshal(action.Doc, &doc))
	assert.Equal(t, "alice", doc.UserAccount)
	assert.Equal(t, "hello", doc.Content)
}

func TestBuildMessageAction_MissingMessageIDIsAnError(t *testing.T) {
	_, err := buildMessageAction(cassandra.Message{RoomID: "room1"}, "messages-a-v1")
	require.Error(t, err)
}

func TestBuildMessageAction_ZeroCreatedAtIsAnError(t *testing.T) {
	_, err := buildMessageAction(cassandra.Message{MessageID: "m1", RoomID: "room1"}, "messages-a-v1")
	require.Error(t, err)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./data-migration/es-index-migrator/... -run TestBuildMessageAction -v`
Expected: FAIL — `buildMessageAction` undefined

- [ ] **Step 3: Write minimal implementation**

Create `data-migration/es-index-migrator/messageaction.go`:

```go
package main

import (
	"encoding/json"
	"fmt"

	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/searchengine"
	"github.com/hmchangw/chat/pkg/searchindex"
)

// buildMessageAction builds the ES bulk action for one Cassandra message
// row. Always ActionIndex — a historical scan never deletes; deleted rows
// are filtered out before reaching this function (messagesource_cassandra.go).
func buildMessageAction(msg cassandra.Message, indexPrefix string) (searchengine.BulkAction, error) {
	if msg.MessageID == "" {
		return searchengine.BulkAction{}, fmt.Errorf("build message action: empty message id for room %s", msg.RoomID)
	}
	if msg.CreatedAt.IsZero() {
		return searchengine.BulkAction{}, fmt.Errorf("build message action: zero createdAt for message %s", msg.MessageID)
	}

	doc := searchindex.NewMessageDoc(searchindex.MessageFields{
		MessageID:             msg.MessageID,
		RoomID:                msg.RoomID,
		SiteID:                msg.SiteID,
		UserID:                msg.Sender.ID,
		UserAccount:           msg.Sender.Account,
		Content:               msg.Msg,
		CreatedAt:             msg.CreatedAt,
		EditedAt:              msg.EditedAt,
		UpdatedAt:             msg.UpdatedAt,
		ThreadParentID:        msg.ThreadParentID,
		ThreadParentCreatedAt: msg.ThreadParentCreatedAt,
		TShow:                 msg.TShow,
		Attachments:           msg.Attachments,
		Card:                  msg.Card,
	})

	body, err := json.Marshal(doc)
	if err != nil {
		return searchengine.BulkAction{}, fmt.Errorf("marshal message doc for %s: %w", msg.MessageID, err)
	}

	return searchengine.BulkAction{
		Action:  searchengine.ActionIndex,
		Index:   searchindex.MessageIndexName(indexPrefix, msg.CreatedAt),
		DocID:   msg.MessageID,
		Version: msg.CreatedAt.UnixMilli(),
		Doc:     body,
	}, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./data-migration/es-index-migrator/... -run TestBuildMessageAction -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add data-migration/es-index-migrator/messageaction.go data-migration/es-index-migrator/messageaction_test.go
git commit -m "feat(es-index-migrator): build message bulk actions from Cassandra rows"
```

---

### Task 11: `spotlightaction.go` and `userroomaction.go` — build spotlight/user-room actions from a Subscription

**Files:**
- Create: `data-migration/es-index-migrator/spotlightaction.go`
- Create: `data-migration/es-index-migrator/spotlightaction_test.go`
- Create: `data-migration/es-index-migrator/userroomaction.go`
- Create: `data-migration/es-index-migrator/userroomaction_test.go`

**Interfaces:**
- Consumes: `searchindex.SpotlightFields`/`NewSpotlightDoc` (Task 3), `searchindex.BuildAddRoomUpdateBody` (Task 4), `model.Subscription` (Task 9).
- Produces: `func buildSpotlightAction(sub model.Subscription, indexName string) (searchengine.BulkAction, error)`, `func buildUserRoomAction(sub model.Subscription, indexName string) (searchengine.BulkAction, error)` — both consumed by `runner.go` (Task 14).

- [ ] **Step 1: Write the failing tests**

Create `data-migration/es-index-migrator/spotlightaction_test.go`:

```go
package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/searchengine"
	"github.com/hmchangw/chat/pkg/searchindex"
)

func TestBuildSpotlightAction(t *testing.T) {
	joinedAt := time.Date(2026, 7, 1, 9, 0, 0, 0, time.UTC)
	sub := model.Subscription{
		RoomID: "room1", SiteID: "site-a", Name: "general", RoomType: model.RoomTypeChannel,
		User: model.SubscriptionUser{Account: "alice"}, JoinedAt: joinedAt,
	}

	action, err := buildSpotlightAction(sub, "spotlight-a-v1")

	require.NoError(t, err)
	assert.Equal(t, searchengine.ActionIndex, action.Action)
	assert.Equal(t, "spotlight-a-v1", action.Index)
	assert.Equal(t, "alice_room1", action.DocID)
	assert.Equal(t, joinedAt.UnixMilli(), action.Version)

	var doc searchindex.SpotlightDoc
	require.NoError(t, json.Unmarshal(action.Doc, &doc))
	assert.Equal(t, "general", doc.RoomName)
	assert.Equal(t, "channel", doc.RoomType)
}

func TestBuildSpotlightAction_MissingRoomIDIsAnError(t *testing.T) {
	_, err := buildSpotlightAction(model.Subscription{User: model.SubscriptionUser{Account: "alice"}}, "spotlight-a-v1")
	require.Error(t, err)
}

func TestBuildSpotlightAction_MissingAccountIsAnError(t *testing.T) {
	_, err := buildSpotlightAction(model.Subscription{RoomID: "room1"}, "spotlight-a-v1")
	require.Error(t, err)
}
```

Create `data-migration/es-index-migrator/userroomaction_test.go`:

```go
package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/searchengine"
	"github.com/hmchangw/chat/pkg/searchindex"
)

func TestBuildUserRoomAction_Unrestricted(t *testing.T) {
	joinedAt := time.Date(2026, 7, 1, 9, 0, 0, 0, time.UTC)
	sub := model.Subscription{
		RoomID: "room1", User: model.SubscriptionUser{Account: "alice"}, JoinedAt: joinedAt,
	}

	action, err := buildUserRoomAction(sub, "user-room-a")

	require.NoError(t, err)
	assert.Equal(t, searchengine.ActionUpdate, action.Action)
	assert.Equal(t, "user-room-a", action.Index)
	assert.Equal(t, "alice", action.DocID)
	assert.Zero(t, action.Version, "user-room actions carry no external version — the script's own LWW guard is the ordering mechanism")

	var decoded struct {
		Script struct {
			ID     string         `json:"id"`
			Params map[string]any `json:"params"`
		} `json:"script"`
	}
	require.NoError(t, json.Unmarshal(action.Doc, &decoded))
	assert.Equal(t, searchindex.AddRoomScriptID, decoded.Script.ID)
	assert.Equal(t, "room1", decoded.Script.Params["rid"])
	assert.InDelta(t, 0, decoded.Script.Params["hss"], 0)
}

func TestBuildUserRoomAction_Restricted(t *testing.T) {
	joinedAt := time.Date(2026, 7, 1, 9, 0, 0, 0, time.UTC)
	hss := joinedAt.Add(-24 * time.Hour)
	sub := model.Subscription{
		RoomID: "room1", User: model.SubscriptionUser{Account: "alice"},
		JoinedAt: joinedAt, HistorySharedSince: &hss,
	}

	action, err := buildUserRoomAction(sub, "user-room-a")

	require.NoError(t, err)
	var decoded struct {
		Script struct {
			Params map[string]any `json:"params"`
		} `json:"script"`
	}
	require.NoError(t, json.Unmarshal(action.Doc, &decoded))
	assert.InDelta(t, hss.UnixMilli(), decoded.Script.Params["hss"], 0)
}

func TestBuildUserRoomAction_BotSubscriptionIsSkipped(t *testing.T) {
	sub := model.Subscription{
		RoomID: "room1", User: model.SubscriptionUser{Account: "helper.bot", IsBot: true}, JoinedAt: time.Now(),
	}

	action, err := buildUserRoomAction(sub, "user-room-a")

	require.NoError(t, err)
	assert.Equal(t, searchengine.BulkAction{}, action, "bot subscriptions must not be fanned into the user-room index (matches search-sync-worker's live BuildAction)")
}

func TestBuildUserRoomAction_MissingRoomIDIsAnError(t *testing.T) {
	_, err := buildUserRoomAction(model.Subscription{User: model.SubscriptionUser{Account: "alice"}}, "user-room-a")
	require.Error(t, err)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./data-migration/es-index-migrator/... -run "TestBuildSpotlightAction|TestBuildUserRoomAction" -v`
Expected: FAIL — `buildSpotlightAction`/`buildUserRoomAction` undefined

- [ ] **Step 3: Write minimal implementation**

Create `data-migration/es-index-migrator/spotlightaction.go`:

```go
package main

import (
	"encoding/json"
	"fmt"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/searchengine"
	"github.com/hmchangw/chat/pkg/searchindex"
)

// buildSpotlightAction builds the ES bulk action for one subscription row.
// Always ActionIndex — every subscription this job reads is a current,
// active membership (see Global Constraints: subscriptions are hard-deleted
// on leave, so there is no closed-row/delete path to migrate here, unlike
// the live worker's per-INBOX-event add/remove branches).
func buildSpotlightAction(sub model.Subscription, indexName string) (searchengine.BulkAction, error) {
	if sub.RoomID == "" {
		return searchengine.BulkAction{}, fmt.Errorf("build spotlight action: empty roomId for subscription %s", sub.ID)
	}
	if sub.User.Account == "" {
		return searchengine.BulkAction{}, fmt.Errorf("build spotlight action: empty account for subscription %s", sub.ID)
	}

	doc := searchindex.NewSpotlightDoc(searchindex.SpotlightFields{
		UserAccount: sub.User.Account,
		RoomID:      sub.RoomID,
		RoomName:    sub.Name,
		RoomType:    string(sub.RoomType),
		SiteID:      sub.SiteID,
		JoinedAt:    sub.JoinedAt,
	})

	body, err := json.Marshal(doc)
	if err != nil {
		return searchengine.BulkAction{}, fmt.Errorf("marshal spotlight doc for subscription %s: %w", sub.ID, err)
	}

	return searchengine.BulkAction{
		Action:  searchengine.ActionIndex,
		Index:   indexName,
		DocID:   sub.User.Account + "_" + sub.RoomID,
		Version: sub.JoinedAt.UnixMilli(),
		Doc:     body,
	}, nil
}
```

Create `data-migration/es-index-migrator/userroomaction.go`:

```go
package main

import (
	"fmt"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/searchengine"
	"github.com/hmchangw/chat/pkg/searchindex"
)

// buildUserRoomAction builds the ES bulk action for one subscription row.
// Always ActionUpdate against the add-room script — every subscription
// this job reads is a current, active membership (see buildSpotlightAction
// and Global Constraints). Bot subscriptions are skipped, matching
// search-sync-worker's live BuildAction (bots don't search; a bot's
// membership would only inflate the per-user access-control view). A
// skipped subscription returns a zero-value BulkAction and a nil error —
// callers (runner.go) must check for the zero value before flushing it.
func buildUserRoomAction(sub model.Subscription, indexName string) (searchengine.BulkAction, error) {
	if sub.User.IsBot {
		return searchengine.BulkAction{}, nil
	}
	if sub.RoomID == "" {
		return searchengine.BulkAction{}, fmt.Errorf("build user-room action: empty roomId for subscription %s", sub.ID)
	}
	if sub.User.Account == "" {
		return searchengine.BulkAction{}, fmt.Errorf("build user-room action: empty account for subscription %s", sub.ID)
	}

	var hss int64
	if sub.HistorySharedSince != nil {
		hss = sub.HistorySharedSince.UnixMilli()
	}

	return searchengine.BulkAction{
		Action: searchengine.ActionUpdate,
		Index:  indexName,
		DocID:  sub.User.Account,
		Doc:    searchindex.BuildAddRoomUpdateBody(sub.User.Account, sub.RoomID, sub.JoinedAt.UnixMilli(), hss),
	}, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./data-migration/es-index-migrator/... -run "TestBuildSpotlightAction|TestBuildUserRoomAction" -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add data-migration/es-index-migrator/spotlightaction.go data-migration/es-index-migrator/spotlightaction_test.go data-migration/es-index-migrator/userroomaction.go data-migration/es-index-migrator/userroomaction_test.go
git commit -m "feat(es-index-migrator): build spotlight and user-room bulk actions from subscriptions"
```

---

### Task 12: `flusher.go` — buffered bulk flush

**Files:**
- Create: `data-migration/es-index-migrator/flusher.go`
- Create: `data-migration/es-index-migrator/flusher_test.go`

**Interfaces:**
- Consumes: `ESStore` (Task 7), `searchengine.IsBulkItemSuccess` (Task 1).
- Produces: `type flusher struct{...}`, `func newFlusher(store ESStore, batchSize int) *flusher`, `func (f *flusher) Add(ctx context.Context, action searchengine.BulkAction) error`, `func (f *flusher) Flush(ctx context.Context) error`, `func (f *flusher) FailedCount() int` — consumed by `runner.go` (Task 14).

- [ ] **Step 1: Write the failing test**

Create `data-migration/es-index-migrator/flusher_test.go`:

```go
package main

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/searchengine"
)

func TestFlusher_AddAutoFlushesAtBatchSize(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockESStore(ctrl)
	store.EXPECT().Bulk(gomock.Any(), gomock.Len(2)).Return([]searchengine.BulkResult{{Status: 200}, {Status: 200}}, nil)

	f := newFlusher(store, 2)
	require.NoError(t, f.Add(context.Background(), searchengine.BulkAction{Action: searchengine.ActionIndex, DocID: "a"}))
	require.NoError(t, f.Add(context.Background(), searchengine.BulkAction{Action: searchengine.ActionIndex, DocID: "b"}))

	assert.Equal(t, 0, f.FailedCount())
}

func TestFlusher_FlushSendsRemainingBufferedActions(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockESStore(ctrl)
	store.EXPECT().Bulk(gomock.Any(), gomock.Len(1)).Return([]searchengine.BulkResult{{Status: 201}}, nil)

	f := newFlusher(store, 10)
	require.NoError(t, f.Add(context.Background(), searchengine.BulkAction{Action: searchengine.ActionIndex, DocID: "a"}))
	require.NoError(t, f.Flush(context.Background()))
}

func TestFlusher_BulkRequestErrorIsPropagatedAndCountsAsFailures(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockESStore(ctrl)
	store.EXPECT().Bulk(gomock.Any(), gomock.Any()).Return(nil, errors.New("es unreachable"))

	f := newFlusher(store, 10)
	require.NoError(t, f.Add(context.Background(), searchengine.BulkAction{Action: searchengine.ActionIndex, DocID: "a"}))
	err := f.Flush(context.Background())

	require.Error(t, err)
	assert.Equal(t, 1, f.FailedCount())
}

func TestFlusher_Benign409IsNotCountedAsFailed(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockESStore(ctrl)
	store.EXPECT().Bulk(gomock.Any(), gomock.Any()).Return([]searchengine.BulkResult{{Status: 409}}, nil)

	f := newFlusher(store, 10)
	require.NoError(t, f.Add(context.Background(), searchengine.BulkAction{Action: searchengine.ActionIndex, DocID: "a"}))
	require.NoError(t, f.Flush(context.Background()))

	assert.Equal(t, 0, f.FailedCount())
}

func TestFlusher_HardFailureIsCounted(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockESStore(ctrl)
	store.EXPECT().Bulk(gomock.Any(), gomock.Any()).Return([]searchengine.BulkResult{{Status: 500, Error: "internal_error"}}, nil)

	f := newFlusher(store, 10)
	require.NoError(t, f.Add(context.Background(), searchengine.BulkAction{Action: searchengine.ActionIndex, DocID: "a"}))
	err := f.Flush(context.Background())

	require.NoError(t, err, "a per-item bulk failure logs and continues; it must not itself return an error from Flush")
	assert.Equal(t, 1, f.FailedCount())
}

func TestFlusher_FlushOnEmptyBufferIsANoOp(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockESStore(ctrl)
	// no EXPECT().Bulk(...) — must not be called on an empty buffer

	f := newFlusher(store, 10)
	require.NoError(t, f.Flush(context.Background()))
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./data-migration/es-index-migrator/... -run TestFlusher -v`
Expected: FAIL — `flusher`/`newFlusher` undefined

- [ ] **Step 3: Write minimal implementation**

Create `data-migration/es-index-migrator/flusher.go`:

```go
package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/hmchangw/chat/pkg/searchengine"
)

// flusher buffers ES bulk actions and flushes them in batches. It is not
// safe for concurrent use by multiple goroutines without external
// synchronization — see runner.go's flusher-per-worker-pool usage.
//
// Note on the batchSize-overshoot race: if multiple callers observed
// len(buffered) < batchSize before either flushed, the buffer can briefly
// exceed batchSize before the next Add call's own check triggers a flush.
// This is intentional and harmless given the caller (runner.go) uses one
// flusher per collection with WORKER_CONCURRENCY workers feeding it under
// a mutex — the overshoot is bounded by concurrency, not unbounded.
type flusher struct {
	store       ESStore
	batchSize   int
	buffered    []searchengine.BulkAction
	failedCount int
}

func newFlusher(store ESStore, batchSize int) *flusher {
	return &flusher{store: store, batchSize: batchSize, buffered: make([]searchengine.BulkAction, 0, batchSize)}
}

// Add buffers action, auto-flushing when the buffer reaches batchSize.
// A zero-value action (searchengine.BulkAction{}) is silently ignored —
// callers like buildUserRoomAction return one to signal "skip this row"
// (e.g. a bot subscription) without needing a separate sentinel type.
// searchengine.BulkAction is not comparable with == (it embeds a
// json.RawMessage slice field), so the zero-value check is field-level
// on Action rather than a whole-struct comparison.
func (f *flusher) Add(ctx context.Context, action searchengine.BulkAction) error {
	if action.Action == "" {
		return nil
	}
	f.buffered = append(f.buffered, action)
	if len(f.buffered) >= f.batchSize {
		return f.Flush(ctx)
	}
	return nil
}

// Flush sends every buffered action as one ES _bulk request and clears the
// buffer. A per-item failure is logged and counted (FailedCount) but does
// not itself make Flush return an error — the caller decides at the end of
// the run whether any failures occurred. Flush only returns an error for a
// request-level failure (the whole _bulk call failed) or a result-count
// mismatch, both of which mean every buffered action's outcome is unknown.
func (f *flusher) Flush(ctx context.Context) error {
	if len(f.buffered) == 0 {
		return nil
	}
	actions := f.buffered
	f.buffered = make([]searchengine.BulkAction, 0, f.batchSize)

	results, err := f.store.Bulk(ctx, actions)
	if err != nil {
		f.failedCount += len(actions)
		return fmt.Errorf("bulk flush %d actions: %w", len(actions), err)
	}
	if len(results) != len(actions) {
		f.failedCount += len(actions)
		return fmt.Errorf("bulk flush: expected %d results, got %d", len(actions), len(results))
	}

	for i, result := range results {
		if searchengine.IsBulkItemSuccess(actions[i].Action, result) {
			continue
		}
		f.failedCount++
		slog.Error("bulk item failed",
			"status", result.Status, "error", result.Error, "docID", actions[i].DocID, "index", actions[i].Index)
	}
	return nil
}

// FailedCount returns the running total of bulk items that failed across
// every Flush call so far (including ones counted when Flush itself
// returned a request-level error).
func (f *flusher) FailedCount() int { return f.failedCount }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./data-migration/es-index-migrator/... -run TestFlusher -v`
Expected: PASS (all 6 subtests)

- [ ] **Step 5: Commit**

```bash
git add data-migration/es-index-migrator/flusher.go data-migration/es-index-migrator/flusher_test.go
git commit -m "feat(es-index-migrator): add buffered bulk flusher"
```

---

### Task 13: `bootstrap.go` — idempotently ensure ES templates and scripts

**Files:**
- Create: `data-migration/es-index-migrator/bootstrap.go`
- Create: `data-migration/es-index-migrator/bootstrap_test.go`

**Interfaces:**
- Consumes: `searchindex.MessageTemplateName`/`MessageTemplateBody`, `SpotlightTemplateName`/`SpotlightTemplateBody`, `UserRoomTemplateName`/`UserRoomTemplateBody`, `AddRoomScriptID`/`AddRoomScript`, `RemoveRoomScriptID`/`RemoveRoomScript`, `StoredScriptBody` (all Task 5/4).
- Produces: `type TemplateStore interface{...}`, `func bootstrapPrerequisites(ctx context.Context, engine TemplateStore, cfg *config) error` (interface exported to match sibling interfaces in `store.go`; `cfg` taken by pointer since `config` is large enough to trip gocritic's `hugeParam` check) — consumed by `main.go` (Task 15).

- [ ] **Step 1: Write the failing test**

Create `data-migration/es-index-migrator/bootstrap_test.go`:

```go
package main

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/searchindex"
)

func testConfig() config {
	return config{
		SiteID: "site-a", MsgIndexPrefix: "messages-a-v1", SpotlightIndex: "spotlight-a-v1", UserRoomIndex: "user-room-a",
	}
}

func TestBootstrapPrerequisites_RegistersAllThreeTemplatesAndBothScripts(t *testing.T) {
	ctrl := gomock.NewController(t)
	engine := NewMockTemplateStore(ctrl)
	cfg := testConfig()

	engine.EXPECT().UpsertTemplate(gomock.Any(), searchindex.MessageTemplateName(cfg.MsgIndexPrefix), gomock.Any()).Return(nil)
	engine.EXPECT().UpsertTemplate(gomock.Any(), searchindex.SpotlightTemplateName(cfg.SpotlightIndex), gomock.Any()).Return(nil)
	engine.EXPECT().UpsertTemplate(gomock.Any(), searchindex.UserRoomTemplateName(cfg.UserRoomIndex), gomock.Any()).Return(nil)
	engine.EXPECT().PutScript(gomock.Any(), searchindex.AddRoomScriptID, gomock.Any()).Return(nil)
	engine.EXPECT().PutScript(gomock.Any(), searchindex.RemoveRoomScriptID, gomock.Any()).Return(nil)

	err := bootstrapPrerequisites(context.Background(), engine, cfg)

	require.NoError(t, err)
}

func TestBootstrapPrerequisites_TemplateErrorAbortsAndIsWrapped(t *testing.T) {
	ctrl := gomock.NewController(t)
	engine := NewMockTemplateStore(ctrl)
	cfg := testConfig()

	engine.EXPECT().UpsertTemplate(gomock.Any(), searchindex.MessageTemplateName(cfg.MsgIndexPrefix), gomock.Any()).
		Return(errors.New("es down"))

	err := bootstrapPrerequisites(context.Background(), engine, cfg)

	require.Error(t, err)
}

func TestBootstrapPrerequisites_ScriptErrorIsWrapped(t *testing.T) {
	ctrl := gomock.NewController(t)
	engine := NewMockTemplateStore(ctrl)
	cfg := testConfig()

	engine.EXPECT().UpsertTemplate(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(3)
	engine.EXPECT().PutScript(gomock.Any(), searchindex.AddRoomScriptID, gomock.Any()).Return(errors.New("script rejected"))

	err := bootstrapPrerequisites(context.Background(), engine, cfg)

	require.Error(t, err)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./data-migration/es-index-migrator/... -run TestBootstrapPrerequisites -v`
Expected: FAIL — `bootstrapPrerequisites`/`templateStore`/`MockTemplateStore` undefined

- [ ] **Step 3: Write minimal implementation**

Create `data-migration/es-index-migrator/bootstrap.go`:

```go
package main

//go:generate mockgen -source=bootstrap.go -destination=mock_bootstrap_test.go -package=main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/hmchangw/chat/pkg/searchindex"
)

// TemplateStore is the narrow slice of searchengine.SearchEngine this file
// needs — defined here (the consumer), satisfied directly by
// searchengine.SearchEngine. Exported to match the sibling interfaces in
// store.go (MessageSource, SubscriptionSource, ESStore), which are all
// exported despite this being package main.
type TemplateStore interface {
	UpsertTemplate(ctx context.Context, name string, body json.RawMessage) error
	PutScript(ctx context.Context, id string, body json.RawMessage) error
}

// bootstrapPrerequisites idempotently ensures the three ES index templates
// and two user-room stored scripts this job depends on exist, using the
// exact same builders search-sync-worker uses at its own startup
// (pkg/searchindex) — so a fresh site can run this migrator standalone
// without first having ever run search-sync-worker. UpsertTemplate/
// PutScript are both create-or-update and safe to call repeatedly with
// unchanged content. cfg is taken by pointer since config has enough
// fields to trip gocritic's hugeParam check.
func bootstrapPrerequisites(ctx context.Context, engine TemplateStore, cfg *config) error {
	templates := []struct {
		name string
		body json.RawMessage
	}{
		{searchindex.MessageTemplateName(cfg.MsgIndexPrefix), searchindex.MessageTemplateBody(cfg.MsgIndexPrefix, false)},
		{searchindex.SpotlightTemplateName(cfg.SpotlightIndex), searchindex.SpotlightTemplateBody(cfg.SpotlightIndex, false)},
		{searchindex.UserRoomTemplateName(cfg.UserRoomIndex), searchindex.UserRoomTemplateBody(cfg.UserRoomIndex)},
	}
	for _, tpl := range templates {
		if err := engine.UpsertTemplate(ctx, tpl.name, tpl.body); err != nil {
			return fmt.Errorf("upsert template %s: %w", tpl.name, err)
		}
	}

	scripts := []struct {
		id     string
		source string
	}{
		{searchindex.AddRoomScriptID, searchindex.AddRoomScript},
		{searchindex.RemoveRoomScriptID, searchindex.RemoveRoomScript},
	}
	for _, script := range scripts {
		if err := engine.PutScript(ctx, script.id, searchindex.StoredScriptBody(script.source)); err != nil {
			return fmt.Errorf("put script %s: %w", script.id, err)
		}
	}

	return nil
}
```

- [ ] **Step 4: Generate the mock and run the test**

Run: `go generate ./data-migration/es-index-migrator/... && go test ./data-migration/es-index-migrator/... -run TestBootstrapPrerequisites -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add data-migration/es-index-migrator/bootstrap.go data-migration/es-index-migrator/bootstrap_test.go data-migration/es-index-migrator/mock_bootstrap_test.go
git commit -m "feat(es-index-migrator): idempotently bootstrap ES templates and scripts"
```

---

### Task 14: `runner.go` — concurrent per-collection orchestration

**Files:**
- Create: `data-migration/es-index-migrator/runner.go`
- Create: `data-migration/es-index-migrator/runner_test.go`

**Interfaces:**
- Consumes: `MessageSource`, `SubscriptionSource` (Task 7), `buildMessageAction`/`buildSpotlightAction`/`buildUserRoomAction` (Tasks 10-11), `flusher` (Task 12).
- Produces: `func runWithWorkerPool[T any](ctx context.Context, concurrency int, items []T, fn func(context.Context, T) error) error`, `func runMessages(ctx context.Context, subs SubscriptionSource, messages MessageSource, f *flusher, cfg config) error`, `func runSpotlight(ctx context.Context, subs SubscriptionSource, f *flusher, cfg config) error`, `func runUserRoom(ctx context.Context, subs SubscriptionSource, f *flusher, cfg config) error` — consumed by `main.go` (Task 15).

- [ ] **Step 1: Write the failing test**

Create `data-migration/es-index-migrator/runner_test.go`:

```go
package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/searchengine"
)

func TestRunWithWorkerPool_RunsEveryItem(t *testing.T) {
	var seen []int
	var mu sync.Mutex
	err := runWithWorkerPool(context.Background(), 2, []int{1, 2, 3}, func(_ context.Context, item int) error {
		mu.Lock()
		seen = append(seen, item)
		mu.Unlock()
		return nil
	})
	require.NoError(t, err)
	assert.ElementsMatch(t, []int{1, 2, 3}, seen)
}

func TestRunWithWorkerPool_PropagatesFirstError(t *testing.T) {
	boom := errors.New("boom")
	err := runWithWorkerPool(context.Background(), 2, []int{1, 2, 3}, func(_ context.Context, item int) error {
		if item == 2 {
			return boom
		}
		return nil
	})
	require.ErrorIs(t, err, boom)
}

func TestRunMessages_FlushesEvenWhenAWorkerFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	subs := NewMockSubscriptionSource(ctrl)
	messages := NewMockMessageSource(ctrl)
	store := NewMockESStore(ctrl)
	cfg := testConfig()
	cfg.WorkerConcurrency = 1 // deterministic ordering for this test

	subs.EXPECT().RoomIDs(gomock.Any(), cfg.SiteID).Return([]string{"room-ok", "room-fail"}, nil)
	messages.EXPECT().StreamMessages(gomock.Any(), cfg.SiteID, "room-ok", gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _, _ string, _, _ time.Time, fn func(cassandra.Message) error) error {
			return fn(cassandra.Message{MessageID: "m1", RoomID: "room-ok", CreatedAt: time.Now()})
		})
	messages.EXPECT().StreamMessages(gomock.Any(), cfg.SiteID, "room-fail", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(errors.New("cassandra timeout"))
	// The one action buffered from room-ok must still reach Bulk even though room-fail errors.
	store.EXPECT().Bulk(gomock.Any(), gomock.Len(1)).Return([]searchengine.BulkResult{{Status: 200}}, nil)

	f := newFlusher(store, 500)
	err := runMessages(context.Background(), subs, messages, f, cfg)

	require.Error(t, err, "a room read error must still fail the run overall")
	assert.Equal(t, 0, f.FailedCount(), "the room-ok action that did reach ES succeeded and must not count as failed")
}

func TestRunSpotlight_OneActionPerSubscription(t *testing.T) {
	ctrl := gomock.NewController(t)
	subs := NewMockSubscriptionSource(ctrl)
	store := NewMockESStore(ctrl)
	cfg := testConfig()

	subs.EXPECT().Subscriptions(gomock.Any(), cfg.SiteID).Return([]model.Subscription{
		{ID: "s1", RoomID: "room1", SiteID: cfg.SiteID, User: model.SubscriptionUser{Account: "alice"}, JoinedAt: time.Now()},
		{ID: "s2", RoomID: "room2", SiteID: cfg.SiteID, User: model.SubscriptionUser{Account: "bob"}, JoinedAt: time.Now()},
	}, nil)
	store.EXPECT().Bulk(gomock.Any(), gomock.Len(2)).Return([]searchengine.BulkResult{{Status: 200}, {Status: 200}}, nil)

	f := newFlusher(store, 500)
	err := runSpotlight(context.Background(), subs, f, cfg)

	require.NoError(t, err)
	assert.Equal(t, 0, f.FailedCount())
}

func TestRunUserRoom_SkipsBotSubscriptionsWithoutError(t *testing.T) {
	ctrl := gomock.NewController(t)
	subs := NewMockSubscriptionSource(ctrl)
	store := NewMockESStore(ctrl)
	cfg := testConfig()

	subs.EXPECT().Subscriptions(gomock.Any(), cfg.SiteID).Return([]model.Subscription{
		{ID: "s1", RoomID: "room1", User: model.SubscriptionUser{Account: "helper.bot", IsBot: true}, JoinedAt: time.Now()},
		{ID: "s2", RoomID: "room1", User: model.SubscriptionUser{Account: "alice"}, JoinedAt: time.Now()},
	}, nil)
	store.EXPECT().Bulk(gomock.Any(), gomock.Len(1)).Return([]searchengine.BulkResult{{Status: 200}}, nil)

	f := newFlusher(store, 500)
	err := runUserRoom(context.Background(), subs, f, cfg)

	require.NoError(t, err)
}

func TestRunSpotlight_EmptySubscriptionsIsANoOp(t *testing.T) {
	ctrl := gomock.NewController(t)
	subs := NewMockSubscriptionSource(ctrl)
	store := NewMockESStore(ctrl)
	cfg := testConfig()

	subs.EXPECT().Subscriptions(gomock.Any(), cfg.SiteID).Return(nil, nil)
	// no EXPECT().Bulk(...) — nothing to flush

	f := newFlusher(store, 500)
	err := runSpotlight(context.Background(), subs, f, cfg)

	require.NoError(t, err)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./data-migration/es-index-migrator/... -run "TestRunWithWorkerPool|TestRunMessages|TestRunSpotlight|TestRunUserRoom" -v`
Expected: FAIL — `runWithWorkerPool`/`runMessages`/`runSpotlight`/`runUserRoom` undefined

- [ ] **Step 3: Write minimal implementation**

Create `data-migration/es-index-migrator/runner.go`:

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"golang.org/x/sync/errgroup"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
)

// runWithWorkerPool runs fn once per item with at most concurrency
// goroutines in flight. Returns the first error from any fn call; the
// group's derived context is canceled on that first error so in-flight and
// not-yet-started work stop promptly. fn is expected to slog.Error its own
// failure before returning it — errgroup keeps only the first error and
// silently drops the rest, so without its own log line a failure past the
// first vanishes with no trace.
func runWithWorkerPool[T any](ctx context.Context, concurrency int, items []T, fn func(ctx context.Context, item T) error) error {
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)
	for _, item := range items {
		g.Go(func() error {
			return fn(gctx, item)
		})
	}
	return g.Wait()
}

// runMessages iterates every room the site's subscriptions reference,
// streams its Cassandra messages in [MigrationStartAt, MigrationEndAt),
// and buffers a versioned index action per message. A room's read error
// aborts that room's worker but not the others; the run's overall error is
// still the first one seen (via runWithWorkerPool). Flush always runs
// after the worker pool, even when it returned an error, via errors.Join —
// any actions already buffered by the rooms that succeeded must reach ES
// rather than being silently discarded on a later room's failure.
func runMessages(ctx context.Context, subs SubscriptionSource, messages MessageSource, f *flusher, cfg config) error {
	roomIDs, err := subs.RoomIDs(ctx, cfg.SiteID)
	if err != nil {
		return fmt.Errorf("list rooms for site %s: %w", cfg.SiteID, err)
	}

	runErr := runWithWorkerPool(ctx, cfg.WorkerConcurrency, roomIDs, func(ctx context.Context, roomID string) error {
		err := messages.StreamMessages(ctx, cfg.SiteID, roomID, cfg.MigrationStartAt, cfg.MigrationEndAt, func(msg cassandra.Message) error {
			action, err := buildMessageAction(msg, cfg.MsgIndexPrefix)
			if err != nil {
				slog.Error("skip message: build action failed", "roomId", roomID, "error", err)
				return nil
			}
			return f.Add(ctx, action)
		})
		if err != nil {
			wrapped := fmt.Errorf("stream messages for room %s: %w", roomID, err)
			slog.Error("room aborted", "roomId", roomID, "error", wrapped)
			return wrapped
		}
		return nil
	})

	return errors.Join(runErr, f.Flush(ctx))
}

// runSpotlight reads every current subscription for the site and buffers
// one versioned spotlight action per row (see Global Constraints: every
// row is an active membership, so this is always the index path — there
// is no delete path to reconstruct from a point-in-time subscriptions
// read). Flush always runs, matching runMessages' errors.Join reasoning.
func runSpotlight(ctx context.Context, subs SubscriptionSource, f *flusher, cfg config) error {
	rows, err := subs.Subscriptions(ctx, cfg.SiteID)
	if err != nil {
		return fmt.Errorf("read subscriptions for site %s: %w", cfg.SiteID, err)
	}

	runErr := runWithWorkerPool(ctx, cfg.WorkerConcurrency, rows, func(ctx context.Context, sub model.Subscription) error {
		action, err := buildSpotlightAction(sub, cfg.SpotlightIndex)
		if err != nil {
			slog.Error("skip subscription: build spotlight action failed", "subscriptionId", sub.ID, "error", err)
			return nil
		}
		return f.Add(ctx, action)
	})

	return errors.Join(runErr, f.Flush(ctx))
}

// runUserRoom reads every current subscription for the site and buffers
// one scripted user-room update per row (bot subscriptions skipped inside
// buildUserRoomAction). Flush always runs, matching runMessages'
// errors.Join reasoning.
func runUserRoom(ctx context.Context, subs SubscriptionSource, f *flusher, cfg config) error {
	rows, err := subs.Subscriptions(ctx, cfg.SiteID)
	if err != nil {
		return fmt.Errorf("read subscriptions for site %s: %w", cfg.SiteID, err)
	}

	runErr := runWithWorkerPool(ctx, cfg.WorkerConcurrency, rows, func(ctx context.Context, sub model.Subscription) error {
		action, err := buildUserRoomAction(sub, cfg.UserRoomIndex)
		if err != nil {
			slog.Error("skip subscription: build user-room action failed", "subscriptionId", sub.ID, "error", err)
			return nil
		}
		return f.Add(ctx, action)
	})

	return errors.Join(runErr, f.Flush(ctx))
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./data-migration/es-index-migrator/... -run "TestRunWithWorkerPool|TestRunMessages|TestRunSpotlight|TestRunUserRoom" -v -race`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add data-migration/es-index-migrator/runner.go data-migration/es-index-migrator/runner_test.go
git commit -m "feat(es-index-migrator): add concurrent per-collection runner with unconditional flush-on-error"
```

---

### Task 15: `main.go` — wiring, graceful shutdown, exit code

**Files:**
- Create: `data-migration/es-index-migrator/main.go`
- Create: `data-migration/es-index-migrator/main_test.go`
- Create: `data-migration/es-index-migrator/integration_test.go` (`//go:build integration`)

**Interfaces:**
- Consumes: everything from Tasks 6-14, plus `pkg/cassutil.Connect`, `pkg/mongoutil.Connect`, `pkg/searchengine.New`, `pkg/msgbucket.New`.
- Produces: `func main()`, `func run(ctx context.Context, cfg config) error` (the testable body of `main`, mirroring the split every other service in this repo uses between `main()`'s os.Exit-on-error wiring and a plain-error-returning `run`).

- [ ] **Step 1: Write the failing test**

`main_test.go` tests `run`'s exit-code-relevant branching without a real Mongo/Cassandra/ES connection by exercising just the parts that don't require live infra — the shutdown-context wiring and the "exit non-zero only on failures" contract are the two units worth testing in isolation; the full wiring is covered by `integration_test.go` (Step 5). Create `data-migration/es-index-migrator/main_test.go`:

```go
package main

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunAllCollections_AggregatesErrorsAcrossCollections(t *testing.T) {
	callOrder := []string{}
	err := runAllCollections(context.Background(),
		func(context.Context) error { callOrder = append(callOrder, "messages"); return nil },
		func(context.Context) error { callOrder = append(callOrder, "spotlight"); return errors.New("spotlight failed") },
		func(context.Context) error { callOrder = append(callOrder, "user-room"); return nil },
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "spotlight failed")
	assert.ElementsMatch(t, []string{"messages", "spotlight", "user-room"}, callOrder, "every collection must run even if an earlier one failed")
}

func TestRunAllCollections_NilOnAllSuccess(t *testing.T) {
	noop := func(context.Context) error { return nil }
	err := runAllCollections(context.Background(), noop, noop, noop)
	require.NoError(t, err)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./data-migration/es-index-migrator/... -run TestRunAllCollections -v`
Expected: FAIL — `runAllCollections` undefined

- [ ] **Step 3: Write minimal implementation**

Create `data-migration/es-index-migrator/main.go`:

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hmchangw/chat/pkg/cassutil"
	"github.com/hmchangw/chat/pkg/logctx"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/msgbucket"
	"github.com/hmchangw/chat/pkg/searchengine"
)

// runAllCollections runs the three collection functions and joins every
// error they return — a failure in one collection must not prevent the
// others from running, and the run's exit code must reflect all of them,
// not just the first.
func runAllCollections(ctx context.Context, runMsgs, runSpot, runUserRoom func(context.Context) error) error {
	return errors.Join(runMsgs(ctx), runSpot(ctx), runUserRoom(ctx))
}

// run is main's testable body: it returns an error instead of calling
// os.Exit, so its wiring can be exercised by an integration test.
func run(ctx context.Context, cfg config) error {
	cassSession, err := cassutil.Connect(cassutil.Config{
		Hosts: cfg.CassandraHosts, Keyspace: cfg.CassandraKeyspace,
		Username: cfg.CassandraUsername, Password: cfg.CassandraPassword, NumConns: cfg.CassandraNumConns,
	})
	if err != nil {
		return fmt.Errorf("cassandra connect: %w", err)
	}
	defer cassutil.Close(cassSession)

	mongoClient, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword)
	if err != nil {
		return fmt.Errorf("mongodb connect: %w", err)
	}
	defer func() { _ = mongoClient.Disconnect(ctx) }()
	db := mongoClient.Database(cfg.MongoDB)

	engine, err := searchengine.New(ctx, searchengine.Config{
		Backend: "elasticsearch", URL: cfg.SearchURL, Username: cfg.SearchUsername, Password: cfg.SearchPassword,
		TLSSkipVerify: cfg.SearchTLSSkipVerify,
	})
	if err != nil {
		return fmt.Errorf("elasticsearch connect: %w", err)
	}

	if err := bootstrapPrerequisites(ctx, engine, &cfg); err != nil {
		return fmt.Errorf("bootstrap prerequisites: %w", err)
	}

	bucketSizer := msgbucket.New(time.Duration(cfg.MessageBucketHours) * time.Hour)
	subs := newMongoSubscriptionSource(db)
	messages := newCassandraMessageSource(cassSession, bucketSizer)

	msgFlusher := newFlusher(engine, cfg.BulkBatchSize)
	spotlightFlusher := newFlusher(engine, cfg.BulkBatchSize)
	userRoomFlusher := newFlusher(engine, cfg.BulkBatchSize)

	runErr := runAllCollections(ctx,
		func(ctx context.Context) error { return runMessages(ctx, subs, messages, msgFlusher, cfg) },
		func(ctx context.Context) error { return runSpotlight(ctx, subs, spotlightFlusher, cfg) },
		func(ctx context.Context) error { return runUserRoom(ctx, subs, userRoomFlusher, cfg) },
	)

	failed := msgFlusher.FailedCount() + spotlightFlusher.FailedCount() + userRoomFlusher.FailedCount()
	slog.Info("migration run complete",
		"site", cfg.SiteID, "failedBulkItems", failed, "runError", runErr != nil)

	if runErr != nil {
		return fmt.Errorf("migration run: %w", runErr)
	}
	if failed > 0 {
		return fmt.Errorf("migration run: %d bulk items failed", failed)
	}
	return nil
}

func main() {
	logctx.SetupDefault(os.Stdout)

	cfg, err := loadConfig()
	if err != nil {
		slog.Error("load config", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg); err != nil {
		slog.Error("es-index-migrator failed", "error", err)
		os.Exit(1)
	}
	slog.Info("es-index-migrator completed successfully", "site", cfg.SiteID)
}
```

This service is a one-shot batch job, not a long-lived server — `pkg/shutdown.Wait` (built for the "block until signal, then run cleanup funcs" pattern) doesn't fit here. `signal.NotifyContext` threads a cancelable `ctx` through `run` so in-flight Cassandra/Mongo/ES calls observe cancellation promptly on SIGTERM/SIGINT; cleanup runs via ordinary `defer` when `run` returns, matching this repo's convention for the one other non-server, non-JetStream batch job style seen in `data-migration/`.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./data-migration/es-index-migrator/... -run TestRunAllCollections -v`
Expected: PASS

- [ ] **Step 5: Write the integration test**

Create `data-migration/es-index-migrator/integration_test.go`:

```go
//go:build integration

package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestRun_EndToEndBackfillIsIdempotentOnRerun(t *testing.T) {
	db := testutil.MongoDB(t, "esmigrun")
	mongoURI := testutil.MongoURI(t)
	keyspace, cassSession, cassHosts := testutil.CassandraKeyspace(t, "esmigrun")
	esURL := testutil.Elasticsearch(t)
	msgIndex := testutil.ElasticsearchIndex(t, "esmigrun-messages")
	spotlightIndex := testutil.ElasticsearchIndex(t, "esmigrun-spotlight")
	userRoomIndex := testutil.ElasticsearchIndex(t, "esmigrun-userroom")

	joinedAt := time.Date(2026, 7, 1, 9, 0, 0, 0, time.UTC)
	_, err := db.Collection("subscriptions").InsertOne(context.Background(), model.Subscription{
		ID: "s1", SiteID: "site-a", RoomID: "room1", RoomType: model.RoomTypeChannel,
		Name: "general", User: model.SubscriptionUser{Account: "alice"}, JoinedAt: joinedAt,
	})
	require.NoError(t, err)

	createdAt := joinedAt.Add(time.Hour)
	err = cassSession.Query(
		"INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg, deleted, site_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
		"room1", createdAt.Truncate(72*time.Hour).UnixMilli(), createdAt, "m1",
		cassandra.Participant{ID: "u1", Account: "alice"}, "hello world", false, "site-a",
	).Exec()
	require.NoError(t, err)

	// run() dials its own Mongo/Cassandra/ES clients from cfg, so cfg must
	// carry the exact connection info the shared testutil containers
	// actually accept: db.Name() is the per-test isolated database
	// testutil.MongoDB already created on the shared Mongo container at
	// mongoURI, and keyspace/cassHosts are CassandraKeyspace's own
	// already-created per-test keyspace on the shared Cassandra container.
	cfg := config{
		SiteID: "site-a", SearchURL: esURL, MsgIndexPrefix: msgIndex, SpotlightIndex: spotlightIndex, UserRoomIndex: userRoomIndex,
		MigrationStartAt: joinedAt.Add(-time.Hour), MigrationEndAt: joinedAt.Add(24 * time.Hour),
		MessageBucketHours: 72, MongoURI: mongoURI, MongoDB: db.Name(),
		CassandraHosts: cassHosts, CassandraKeyspace: keyspace,
		BulkBatchSize: 500, WorkerConcurrency: 2,
	}

	err = run(context.Background(), cfg)
	require.NoError(t, err)

	// A second identical run must not error and must not double-count
	// failures — every write this job makes is versioned/idempotent.
	err = run(context.Background(), cfg)
	require.NoError(t, err)
}

func TestMain(m *testing.M) { testutil.RunTests(m) }
```

- [ ] **Step 6: Run the integration test**

Run: `make test-integration SERVICE=data-migration/es-index-migrator` (requires Docker)
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add data-migration/es-index-migrator/main.go data-migration/es-index-migrator/main_test.go data-migration/es-index-migrator/integration_test.go
git commit -m "feat(es-index-migrator): wire main() with graceful shutdown and joined error reporting"
```

---

### Task 16: `deploy/` files and `docs/search_index_migration_spec.md`

**Files:**
- Create: `data-migration/es-index-migrator/deploy/Dockerfile`
- Create: `data-migration/es-index-migrator/deploy/docker-compose.yml`
- Create: `data-migration/es-index-migrator/deploy/azure-pipelines.yml`
- Create: `docs/search_index_migration_spec.md`

**Interfaces:** none (infra/docs only).

- [ ] **Step 1: Create the Dockerfile**

Read an existing `data-migration/*/deploy/Dockerfile` (e.g. `data-migration/oplog-connector/deploy/Dockerfile`) first and mirror its exact multi-stage shape (per CLAUDE.md: `golang:1.25.12-alpine` builder, `alpine:3.21` runtime, non-root user, repo-root build context) — only the binary name/path changes:

```dockerfile
FROM golang:1.25.12-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/es-index-migrator ./data-migration/es-index-migrator

FROM alpine:3.21
RUN adduser -D -u 10001 app
USER app
COPY --from=builder /out/es-index-migrator /es-index-migrator
ENTRYPOINT ["/es-index-migrator"]
```

(Replace the body above with whatever the sibling Dockerfile's exact conventions are — CA certs, `apk add`, healthcheck stanza, etc. — this plan step's job is "match the established pattern exactly," not invent a new one; do not deviate without a documented reason.)

- [ ] **Step 2: Create `docker-compose.yml` — join the shared `chat-local` network with the correct service hostnames**

This is the exact bug that broke the equivalent file in the now-obsolete `chat` repo's PR #505: the compose file must join `chat-local` and use the shared stack's real service names (`mongodb`, not `mongo`; `cassandra`; `elasticsearch`), not values that merely look plausible. Cross-check every hostname against `docker-local/compose.deps.yaml`'s actual service names before writing this file.

```yaml
name: es-index-migrator

services:
  es-index-migrator:
    build:
      context: ../../..
      dockerfile: data-migration/es-index-migrator/deploy/Dockerfile
    environment:
      - SITE_ID=site-local
      - SEARCH_URL=http://elasticsearch:9200
      - MSG_INDEX_PREFIX=messages-site-local-v1
      - SPOTLIGHT_INDEX=spotlight-site-local-v1
      - USER_ROOM_INDEX=user-room-mv-site-local
      - MIGRATION_START_AT=2025-07-01T00:00:00Z
      - MIGRATION_END_AT=2026-07-01T00:00:00Z
      - MESSAGE_BUCKET_HOURS=72
      - MONGO_URI=mongodb://mongodb:27017
      - MONGO_DB=chat
      - CASSANDRA_HOSTS=cassandra
      - CASSANDRA_KEYSPACE=chat
    networks:
      - chat-local

networks:
  chat-local:
    external: true
```

This service has no Vault/`ATREST_*` env vars — unlike `message-worker`/`search-sync-worker`, it never decrypts (see Task 8's note: its source rows were written directly into the plaintext `msg`/`attachments`/`card` columns, never through the live at-rest-encryption path). Cross-reference the `search-sync-worker`/`message-worker` compose files' exact index-name values (`MSG_INDEX_PREFIX`, etc.) to keep them consistent with the rest of the local stack — copy those values verbatim rather than inventing new ones, so a local run of this migrator targets the same indexes `search-sync-worker` would.

- [ ] **Step 3: Create `azure-pipelines.yml`**

Read `data-migration/oplog-connector/deploy/azure-pipelines.yml` (or another `data-migration/*` sibling) and copy its structure verbatim, changing only the service path/name (path filters, coverage-floor gate, build/push gating on `main`).

- [ ] **Step 4: Write `docs/search_index_migration_spec.md`**

This replaces the now-obsolete `chat` repo's spec of the same name with one that reflects this repo's actual design (current-stack source, no legacy classifier, no two-pass/connector-handoff narrative — that onboarding problem belongs to `oplog-connector`/`oplog-collections-transformer`, not this tool). Cover, at minimum:

- **Purpose**: rebuild/backfill a site's ES search indexes (messages, spotlight, user-room) from its own current Cassandra/MongoDB — for use after ES data loss, a mapping/template change, or enabling search for an existing site. Not an onboarding tool (that's `oplog-connector` + `oplog-collections-transformer`).
- **Why not route through the live pipeline**: replaying history through `MESSAGES_CANONICAL`/`INBOX` would fire `notification-worker`/`broadcast-worker` side effects wrong for old data; this job writes straight to ES via `_bulk` instead.
- **Site scoping**: one site's own stores only; a foreign-site room id is a harmless empty-result Cassandra query.
- **Additive-only limitation**: because subscriptions are hard-deleted on leave (no soft-delete row to read), this job can only reconstruct current membership state — it cannot detect or evict a stale ES entry for a membership that ended and was never re-added before an earlier ES data loss. State this plainly as an accepted limitation, not a bug.
- **Write model**: messages/spotlight external-versioned (`CreatedAt`/`JoinedAt` in millis); user-room scripted update via the same stored Painless scripts `search-sync-worker` registers, no external version — every write is idempotent/LWW-safe so a re-run or overlap with live traffic converges.
- **Collections 1-3**: source query, field mapping table (Cassandra `Message` column → `MessageDoc` field; `model.Subscription` field → `SpotlightDoc`/user-room script params), index name, doc ID, bulk action — mirror the level of detail in Tasks 8-12 above.
- **No at-rest decryption dependency**: unlike `history-service`, this job never reads `enc_payload`/`enc_meta` or depends on `pkg/atrest`/Vault — its source `messages_by_room` rows were written directly into the plaintext `msg`/`attachments`/`card` columns by the process that populated that table, not through the live at-rest-encryption write path.
- **Configuration table**: every env var from Task 6's `config.go`, required/default noted.
- **Execution shape**: mirrors Task 14/15 — connect, bootstrap prerequisites (idempotent), run three collections concurrently, `slog` progress, exit 0 only if `runAllCollections` succeeded and zero bulk items failed across all three flushers.

- [ ] **Step 5: Commit**

```bash
git add data-migration/es-index-migrator/deploy/ docs/search_index_migration_spec.md
git commit -m "feat(es-index-migrator): add deploy configs and design spec"
```

---

## Final checkpoint

Before opening a PR:

- [ ] `go build ./...` — clean
- [ ] `go vet -tags integration ./...` — clean (catches any stale reference to a moved/renamed type across every build tag, the exact class of bug that broke this feature's first attempt in the now-obsolete `chat` repo)
- [ ] `make lint` — clean
- [ ] `make test` — all unit tests pass with `-race`
- [ ] `make test-integration SERVICE=data-migration/es-index-migrator` and `make test-integration SERVICE=search-sync-worker` — both pass against real Cassandra/MongoDB/Elasticsearch containers
- [ ] `make sast` — clean or every suppression has a justified `// #nosec <RULE> -- reason` comment
- [ ] Coverage check: `go test -coverprofile=coverage.out ./data-migration/es-index-migrator/... ./pkg/searchindex/... ./pkg/searchengine/...` then `go tool cover -func=coverage.out` — every package at or above 80%, `pkg/searchindex`/`pkg/searchengine` at or above 90%
- [ ] Confirm `data-migration/es-index-migrator/deploy/docker-compose.yml` actually brings the service up against `make deps-up`'s shared stack (`make up SERVICE=data-migration/es-index-migrator`) — do not skip this manual check; it is exactly the check that was skipped the first time this feature was built and shipped a non-functional compose file
- [ ] Delete any session-scoped review notes under `docs/reviews/` per CLAUDE.md before creating the PR
