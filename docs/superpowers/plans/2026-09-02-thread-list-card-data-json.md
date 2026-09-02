# Thread List `card.data` JSON Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `thread.list` emit `parentMessage.card.data` and `lastMessage.card.data` as the JSON document they are, instead of the base64 of the stored bytes.

**Architecture:** `buildThreadItems` pre-marshals each hydrated message into an opaque `json.RawMessage` body. A local wrapper struct shadows the embedded `models.Message`'s `card` field during that marshal, so the card's `data` is spliced in as raw JSON. Nothing outside `history-service/internal/service/threads.go` changes: `cassandra.Card` keeps its `[]byte` field, Cassandra storage is untouched, and every other read path keeps its current base64 wire shape.

**Tech Stack:** Go 1.25, `encoding/json`, testify, `go.uber.org/mock`. No new dependencies.

**Spec:** [hmchangw/newchat#464](https://github.com/hmchangw/newchat/issues/464). This is a bounded change agreed in-session; there is no separate design doc. Issue #464's stated root cause ("`buildThreadItems` decoded only `attachments` but not `card`") is **not accurate as written** — no read path in the repo decodes `card`, and `docs/client-api.md` §MessageCard currently documents `data` as base64 everywhere. This plan deliberately changes `thread.list` alone, which makes that one endpoint differ from every other message-returning endpoint. That divergence is intentional and owner-approved; Task 2 documents it so the next reader does not treat it as a bug.

## Global Constraints

- **TDD is mandatory** (CLAUDE.md §4): write the failing test, run it, confirm it fails for the stated reason, then implement. Never write implementation before its test.
- **Never run raw `go` commands** — use the root `Makefile` targets only (CLAUDE.md §2).
- **Test package convention in this directory is split and must be respected:** unexported-helper tests use `package service` (see `attachments_test.go`); RPC-level tests use `package service_test` (see `threadlist_test.go`).
- **Coverage floor is 80%**, target 90%+ for handler logic (CLAUDE.md §4).
- **Client-API docs travel in the same PR** as any client-facing handler change, and the derived views must not drift from the canonical file (CLAUDE.md §5).
- **Logging:** `log/slog` only, structured key-value fields, never interpolated strings; never log full message bodies (CLAUDE.md §3).
- **Commit message trailer** — every commit ends with:
  ```
  Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01VWqrYt3CJHuRhLZxitVk1E
  ```
- **Branch:** all work lands on `claude/issue-464-superpowers-6k84ke`. Never commit to `master`/`main`.

## Verified Facts (do not re-litigate during implementation)

These were checked against the tree at `18d8784` before the plan was written:

1. `cassandra.Card` is `{Template string; Data []byte}` (`pkg/model/cassandra/message.go:21-25`). Go marshals `[]byte` as base64 — that is the entire cause of the reported symptom.
2. `cassandra.QuotedParentMessage` has **no** `Card` field (`pkg/model/cassandra/message.go:49-68`). There is no nested card to handle.
3. `ThreadListItem.ParentMessage` / `.LastMessage` are `json.RawMessage` (`pkg/model/threadlist.go:33-34`), marshaled locally inside `buildThreadItems`. This is why a local override works without touching any shared type.
4. The cross-site aggregator `user-service/service/threads.go` treats both bodies as **opaque** — it never unmarshals them into a message type; `blankOversizeThread` only nils them (`user-service/service/threads.go:338-342`). So no Go consumer in this repo breaks when the body's card shape changes.
5. `SendMessageRequest` has no `card` field — users cannot send cards; only the bot API can. Nothing on the write path needs to change.

---

### Task 1: Emit `card.data` as raw JSON in thread-list bodies

**Files:**
- Modify: `history-service/internal/service/threads.go:198-268` (add the wrapper type above `buildThreadItems`; change the two `json.Marshal` call sites at `:251` and `:258`)
- Test: `history-service/internal/service/threadcard_test.go` (create, `package service` — direct unit tests of the unexported helper, mirroring `attachments_test.go`)
- Test: `history-service/internal/service/threadlist_test.go` (modify — RPC-level assertions, `package service_test`)

**Interfaces:**
- Consumes: `models.Message` (alias of `cassandra.Message`), `models.Card` (alias of `cassandra.Card`), `decodeMessageAttachments(ctx, *models.Message)` — all already present in the package.
- Produces: `threadListMessage` struct and `newThreadListMessage(m models.Message) threadListMessage`. Both unexported and used only by `buildThreadItems`; no later task depends on them.

- [ ] **Step 1: Write the failing helper unit test**

Create `history-service/internal/service/threadcard_test.go`:

```go
package service

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/history-service/internal/models"
)

// cardOf marshals a wrapped message and returns its "card" object, or nil when
// the key is absent. Asserting on the wire map (not a typed struct) is what
// makes key presence and JSON type observable.
func cardOf(t *testing.T, m models.Message) map[string]any {
	t.Helper()
	b, err := json.Marshal(newThreadListMessage(m))
	require.NoError(t, err)
	var wire map[string]any
	require.NoError(t, json.Unmarshal(b, &wire))
	card, ok := wire["card"]
	if !ok {
		return nil
	}
	obj, ok := card.(map[string]any)
	require.True(t, ok, "card must be a JSON object")
	return obj
}

func TestNewThreadListMessage_CardDataIsJSONObject(t *testing.T) {
	m := models.Message{
		MessageID: "p1",
		Card:      &models.Card{Template: "approval", Data: []byte(`{"filters":[{"title":"ADC OOC"}]}`)},
	}

	card := cardOf(t, m)
	require.NotNil(t, card)
	assert.Equal(t, "approval", card["template"])

	data, ok := card["data"].(map[string]any)
	require.True(t, ok, "card.data must be a JSON object, not a base64 string")
	filters, ok := data["filters"].([]any)
	require.True(t, ok)
	require.Len(t, filters, 1)
	assert.Equal(t, "ADC OOC", filters[0].(map[string]any)["title"])
}

func TestNewThreadListMessage_CardDataJSONArray(t *testing.T) {
	m := models.Message{
		MessageID: "p1",
		Card:      &models.Card{Template: "list", Data: []byte(`[{"a":1},{"a":2}]`)},
	}

	data, ok := cardOf(t, m)["data"].([]any)
	require.True(t, ok, "a top-level JSON array must pass through as an array")
	assert.Len(t, data, 2)
}

// A blob that is not valid JSON must not fail the marshal: buildThreadItems
// drops a row whose body fails to marshal, so a strict RawMessage would make
// one corrupt card silently erase the whole thread from the user's list.
func TestNewThreadListMessage_InvalidCardDataStaysBase64(t *testing.T) {
	m := models.Message{
		MessageID: "p1",
		Card:      &models.Card{Template: "approval", Data: []byte{0xff, 0xfe, 0x00}},
	}

	data, ok := cardOf(t, m)["data"].(string)
	require.True(t, ok, "invalid bytes must fall back to the base64 string form")
	assert.Equal(t, "//4A", data)
}

func TestNewThreadListMessage_EmptyCardDataOmitsKey(t *testing.T) {
	m := models.Message{MessageID: "p1", Card: &models.Card{Template: "approval"}}

	card := cardOf(t, m)
	require.NotNil(t, card)
	assert.Equal(t, "approval", card["template"])
	_, present := card["data"]
	assert.False(t, present, "empty data must stay omitted")
}

func TestNewThreadListMessage_NoCardOmitsKey(t *testing.T) {
	assert.Nil(t, cardOf(t, models.Message{MessageID: "p1", Msg: "hello"}))
}

// Everything other than card must marshal exactly as it does today; the wrapper
// is a card-only override, not a reshaping of the message body.
func TestNewThreadListMessage_NonCardFieldsUnchanged(t *testing.T) {
	m := models.Message{
		MessageID: "p1",
		RoomID:    "r1",
		Msg:       "hello",
		Card:      &models.Card{Template: "approval", Data: []byte(`{"a":1}`)},
	}

	wrapped, err := json.Marshal(newThreadListMessage(m))
	require.NoError(t, err)
	plain, err := json.Marshal(&m)
	require.NoError(t, err)

	var gotWrapped, gotPlain map[string]any
	require.NoError(t, json.Unmarshal(wrapped, &gotWrapped))
	require.NoError(t, json.Unmarshal(plain, &gotPlain))
	delete(gotWrapped, "card")
	delete(gotPlain, "card")
	assert.Equal(t, gotPlain, gotWrapped)
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `make test SERVICE=history-service`
Expected: FAIL — compile error, `undefined: newThreadListMessage`.

- [ ] **Step 3: Write the minimal implementation**

In `history-service/internal/service/threads.go`, insert directly above the `buildThreadItems` doc comment (currently line 198):

```go
// threadListCard is the thread-list wire form of a message card: data is the
// JSON document it has always been, not the base64 of its bytes. Thread list is
// the only endpoint that emits it this way — see docs/client-api.md §5.16.
type threadListCard struct {
	Template string          `json:"template"`
	Data     json.RawMessage `json:"data,omitempty"`
}

// threadListMessage overrides the embedded message's card during marshal: the
// outer field is shallower, so encoding/json emits it and drops the embedded
// one. Every other field rides through untouched.
type threadListMessage struct {
	models.Message
	Card *threadListCard `json:"card,omitempty"`
}

// newThreadListMessage wraps m for marshaling. Card data that is not valid JSON
// is left in its base64 form rather than spliced in raw: json.Marshal rejects an
// invalid RawMessage, and buildThreadItems drops a row whose body fails to
// marshal, so one corrupt blob would erase the whole thread from the list.
func newThreadListMessage(m models.Message) threadListMessage {
	out := threadListMessage{Message: m}
	if m.Card == nil {
		return out
	}
	card := &threadListCard{Template: m.Card.Template}
	if json.Valid(m.Card.Data) {
		card.Data = json.RawMessage(m.Card.Data)
	} else if len(m.Card.Data) > 0 {
		// Not valid JSON: fall back to the base64 encoding every other endpoint
		// emits, so the client still receives the bytes.
		b, err := json.Marshal([]byte(m.Card.Data))
		if err != nil {
			return out
		}
		card.Data = json.RawMessage(b)
	}
	out.Card = card
	return out
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `make test SERVICE=history-service`
Expected: PASS — all six `TestNewThreadListMessage_*` tests green.

- [ ] **Step 5: Write the failing RPC-level tests**

Append to `history-service/internal/service/threadlist_test.go` (`package service_test`; `rawBody`, `newThreadListService`, and `testContext` already exist in that file):

```go
// thread.list must emit card.data as the JSON document it is. The raw column is
// []byte, so a body marshaled without the override reaches the client base64-
// encoded — the symptom reported in issue #464.
func TestHistoryService_ListThreadSubscriptions_CardDataIsJSONObject(t *testing.T) {
	svc, msgs, _, _, threadSubs := newThreadListService(t)
	base := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	rows := []mongorepo.ThreadSubRow{
		{ThreadRoomID: "tr-1", RoomID: "r1", SiteID: "site-a", ParentMessageID: "p1", LastMsgID: "m1", LastMsgAt: base.Add(5 * time.Hour)},
	}
	threadSubs.EXPECT().ListUserThreadSubscriptions(gomock.Any(), "alice", gomock.Any(), gomock.Any(), gomock.Any()).Return(rows, false, nil)
	msgs.EXPECT().GetMessagesByIDs(gomock.Any(), gomock.Any()).Return([]models.Message{
		{MessageID: "p1", RoomID: "r1", Msg: "parent", Card: &cassandra.Card{Template: "tmpl-parent", Data: []byte(`{"filters":[{"title":"ADC OOC"}]}`)}},
		{MessageID: "m1", RoomID: "r1", Msg: "last", Card: &cassandra.Card{Template: "tmpl-last", Data: []byte(`{"ok":true}`)}},
	}, nil)

	resp, err := svc.ListThreadSubscriptions(testContext(), pkgmodel.ThreadSubscriptionListRequest{Account: "alice", Limit: 10})
	require.NoError(t, err)
	require.Len(t, resp.Items, 1)

	parentCard, ok := rawBody(t, resp.Items[0].ParentMessage)["card"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "tmpl-parent", parentCard["template"])
	parentData, ok := parentCard["data"].(map[string]any)
	require.True(t, ok, "parentMessage.card.data must be an object, not base64")
	filters, ok := parentData["filters"].([]any)
	require.True(t, ok)
	require.Len(t, filters, 1)

	lastCard, ok := rawBody(t, resp.Items[0].LastMessage)["card"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "tmpl-last", lastCard["template"])
	lastData, ok := lastCard["data"].(map[string]any)
	require.True(t, ok, "lastMessage.card.data must be an object, not base64")
	assert.Equal(t, true, lastData["ok"])
}

// A card whose blob is not valid JSON must degrade to base64 and keep its row:
// a marshal failure would drop the thread from the list entirely.
func TestHistoryService_ListThreadSubscriptions_InvalidCardDataKeepsRow(t *testing.T) {
	svc, msgs, _, _, threadSubs := newThreadListService(t)
	base := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	rows := []mongorepo.ThreadSubRow{
		{ThreadRoomID: "tr-1", RoomID: "r1", SiteID: "site-a", ParentMessageID: "p1", LastMsgID: "m1", LastMsgAt: base.Add(5 * time.Hour)},
	}
	threadSubs.EXPECT().ListUserThreadSubscriptions(gomock.Any(), "alice", gomock.Any(), gomock.Any(), gomock.Any()).Return(rows, false, nil)
	msgs.EXPECT().GetMessagesByIDs(gomock.Any(), gomock.Any()).Return([]models.Message{
		{MessageID: "p1", RoomID: "r1", Msg: "parent", Card: &cassandra.Card{Template: "tmpl", Data: []byte{0xff, 0xfe, 0x00}}},
		{MessageID: "m1", RoomID: "r1", Msg: "last"},
	}, nil)

	resp, err := svc.ListThreadSubscriptions(testContext(), pkgmodel.ThreadSubscriptionListRequest{Account: "alice", Limit: 10})
	require.NoError(t, err)
	require.Len(t, resp.Items, 1, "a corrupt card must not drop the thread")

	card, ok := rawBody(t, resp.Items[0].ParentMessage)["card"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "//4A", card["data"])
}

// A message with no card must not gain a card key.
func TestHistoryService_ListThreadSubscriptions_NoCardOmitsKey(t *testing.T) {
	svc, msgs, _, _, threadSubs := newThreadListService(t)
	base := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	rows := []mongorepo.ThreadSubRow{
		{ThreadRoomID: "tr-1", RoomID: "r1", SiteID: "site-a", ParentMessageID: "p1", LastMsgID: "m1", LastMsgAt: base.Add(5 * time.Hour)},
	}
	threadSubs.EXPECT().ListUserThreadSubscriptions(gomock.Any(), "alice", gomock.Any(), gomock.Any(), gomock.Any()).Return(rows, false, nil)
	msgs.EXPECT().GetMessagesByIDs(gomock.Any(), gomock.Any()).Return([]models.Message{
		{MessageID: "p1", RoomID: "r1", Msg: "parent"},
		{MessageID: "m1", RoomID: "r1", Msg: "last"},
	}, nil)

	resp, err := svc.ListThreadSubscriptions(testContext(), pkgmodel.ThreadSubscriptionListRequest{Account: "alice", Limit: 10})
	require.NoError(t, err)
	require.Len(t, resp.Items, 1)

	_, present := rawBody(t, resp.Items[0].ParentMessage)["card"]
	assert.False(t, present)
}
```

- [ ] **Step 6: Run the RPC tests to verify they fail**

Run: `make test SERVICE=history-service`
Expected: FAIL — `TestHistoryService_ListThreadSubscriptions_CardDataIsJSONObject` fails on `parentMessage.card.data must be an object, not base64` (the type assertion to `map[string]any` fails; the value is still the base64 string). `buildThreadItems` still marshals `&parent` / `&last` directly.

- [ ] **Step 7: Wire the wrapper into `buildThreadItems`**

In `history-service/internal/service/threads.go`, replace the two marshal call sites (currently `:251` and `:258`) so the wrapper is what gets marshaled. Leave the surrounding comment, the `decodeMessageAttachments` calls, the warn logs, and the `continue` branches exactly as they are:

```go
		parentJSON, err := json.Marshal(newThreadListMessage(parent))
```

```go
		lastJSON, err := json.Marshal(newThreadListMessage(last))
```

Also extend the existing comment above `decodeMessageAttachments(c, &parent)` (currently `:247-248`) so the card override is discoverable from the call site:

```go
		// Both bodies ship pre-marshaled, so decode here: the raw blob column is
		// json:"-" and only DecodedAttachments reaches the client. The wrapper
		// additionally emits card.data as raw JSON — thread list only.
```

- [ ] **Step 8: Run the full service test suite**

Run: `make test SERVICE=history-service`
Expected: PASS — the three new RPC tests and all pre-existing thread-list tests green. Pay attention to `TestHistoryService_ListThreadSubscriptions_DecodesAttachments` and `..._DecodesQuotedParentAttachments`: they use the `decodeThreadMsg` helper, which unmarshals a body back into `models.Message`. Those fixtures carry no card, so they are unaffected — but **never add a card to a fixture asserted through `decodeThreadMsg`**, because unmarshaling a JSON object into `Card.Data []byte` fails with an illegal-base64 error. Assert card-bearing bodies through `rawBody` instead.

- [ ] **Step 9: Verify coverage did not regress**

Run: `make test SERVICE=history-service` and confirm the reported coverage for `history-service/internal/service` is at or above its pre-change value (80% floor, CLAUDE.md §4). The new helper is exercised by six direct unit tests plus three RPC tests; if any branch is uncovered, add the missing case rather than lowering the bar.

- [ ] **Step 10: Lint**

Run: `make lint`
Expected: clean. If `goimports` reorders anything, run `make fmt` and re-run `make lint`.

- [ ] **Step 11: Commit**

```bash
git add history-service/internal/service/threads.go \
        history-service/internal/service/threadcard_test.go \
        history-service/internal/service/threadlist_test.go
git commit -m "$(cat <<'EOF'
fix(history-service): emit thread-list card.data as JSON (#464)

thread.list pre-marshals parentMessage/lastMessage into opaque bodies.
cassandra.Card.Data is []byte, so those bodies carried the card payload
base64-encoded. Marshal through a wrapper that shadows the embedded
message's card and splices the stored bytes in as raw JSON.

Data that is not valid JSON keeps its base64 form: json.Marshal rejects an
invalid RawMessage and buildThreadItems drops a row whose body fails to
marshal, so one corrupt blob would otherwise erase the whole thread from
the user's list.

Scoped to thread.list only — every other endpoint keeps the base64 shape
documented in docs/client-api.md §MessageCard.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01VWqrYt3CJHuRhLZxitVk1E
EOF
)"
```

Note: the wrapper lives in `threads.go`, beside its only caller — there is no `threadcard.go`. `threadcard_test.go` is a `package service` test file, so it reaches the unexported helper from a separate file in the same package (the pattern `attachments_test.go` already uses).

---

### Task 2: Document the thread-list-only card shape

**Files:**
- Modify: `docs/client-api.md` (the thread.list section — `parentMessage` / `lastMessage` field rows near `:5866-5920`, and the `MessageCard` table at `:3022-3027`)
- Modify: `docs/client-api/request-reply.md` (the derived request/reply view — only if it carries the thread.list response schema)

**Interfaces:**
- Consumes: the wire behavior implemented in Task 1.
- Produces: nothing code depends on.

- [ ] **Step 1: Confirm which derived views carry thread.list**

Run:
```bash
grep -n "thread.list\|thread\.subscription\.list" docs/client-api.md docs/client-api/request-reply.md docs/client-api/events.md
```
Expected: hits in `docs/client-api.md` and `docs/client-api/request-reply.md`. `events.md` covers server→client events and should have none — if it does, it needs the same note. CLAUDE.md §5 requires every view that carries the schema to be updated in the same PR.

- [ ] **Step 2: Add the divergence note to the thread.list response section**

In `docs/client-api.md`, under the thread.list response field table, immediately after the `parentMessage` / `lastMessage` rows, add:

```markdown
> **`card.data` on this endpoint is a JSON object, not base64.** Unlike every
> other endpoint that returns a [MessageCard](#messagecard), `thread.list`
> emits `parentMessage.card.data` and `lastMessage.card.data` as the JSON
> document itself. A card whose stored bytes are not valid JSON falls back to
> the base64 string form, so clients must accept either an object or a string
> here. `cardAction.data` is unaffected and stays base64.
```

- [ ] **Step 3: Cross-reference from the MessageCard table**

In `docs/client-api.md`, change the `MessageCard` `data` row (currently `:3027`) from:

```markdown
| `data` | string | Optional. Base64-encoded card payload. |
```

to:

```markdown
| `data` | string | Optional. Base64-encoded card payload. **Exception:** on `thread.list` this field is the JSON object itself (a base64 string only when the stored bytes are not valid JSON) — see the note on that endpoint. |
```

- [ ] **Step 4: Mirror the note into the derived view**

Apply the same note from Step 2 to the thread.list response section of `docs/client-api/request-reply.md`, matching that file's existing formatting. The derived views must never drift from the canonical file (CLAUDE.md §5).

- [ ] **Step 5: Verify no other doc claims thread.list cards are base64**

Run:
```bash
grep -rn "base64" docs/client-api.md docs/client-api/*.md | grep -i card
```
Expected: the `MessageCard` row now carries the exception, the `MessageCardAction` row is unchanged, and the search-results note (`:4268`) is unchanged — it describes search hits, not thread list, and remains correct.

- [ ] **Step 6: Commit**

```bash
git add docs/client-api.md docs/client-api/request-reply.md
git commit -m "$(cat <<'EOF'
docs(client-api): note thread.list emits card.data as JSON (#464)

Records the deliberate divergence introduced by the previous commit: card
data is a JSON object on thread.list and base64 everywhere else, with a
base64 fallback when the stored bytes are not valid JSON.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01VWqrYt3CJHuRhLZxitVk1E
EOF
)"
```

---

### Task 3: Full-suite verification and push

**Files:** none modified — this task only verifies and publishes.

**Interfaces:**
- Consumes: Tasks 1 and 2.
- Produces: the pushed branch.

- [ ] **Step 1: Run the whole unit suite**

Run: `make test`
Expected: PASS across all services. Task 1 touches one function in one service, so a failure anywhere else means something unrelated broke — investigate before pushing rather than re-running.

- [ ] **Step 2: Lint the repo**

Run: `make lint`
Expected: clean.

- [ ] **Step 3: Run the SAST gate**

Run: `make sast`
Expected: clean. This is a blocking CI gate (CLAUDE.md §5); the change adds no new I/O, conversion, or crypto, so any finding here is pre-existing — confirm that before suppressing anything, and never suppress without a justified `// #nosec <RULE> -- reason`.

- [ ] **Step 4: Confirm nothing outside the intended scope changed**

Run: `git diff --stat origin/master...HEAD`
Expected: exactly `history-service/internal/service/threads.go`, `history-service/internal/service/threadcard_test.go`, `history-service/internal/service/threadlist_test.go`, `docs/client-api.md`, `docs/client-api/request-reply.md`, plus this plan document. Anything else is scope creep — the owner scoped this change to `buildThreadItems`.

- [ ] **Step 5: Push**

```bash
git push -u origin claude/issue-464-superpowers-6k84ke
```
On network failure only, retry up to 4 times with exponential backoff (2s, 4s, 8s, 16s). Do **not** open a pull request — the owner has not asked for one.

---

## Out of Scope

Named explicitly so an implementer does not widen the change:

- `cardAction.data` — stays base64, on this endpoint and everywhere else.
- `sysMsgData` — stays base64.
- Every other message-returning endpoint (`msg.history`, `thread.messages`, `pin.list`, surrounding-messages, by-id, search hits, live room events) — unchanged.
- `pkg/model/cassandra/Card` — the field stays `[]byte`; no shared type changes.
- The bot API's inbound `card` — bots keep sending base64.
- Cassandra and MongoDB storage — untouched. This is not a data migration.
