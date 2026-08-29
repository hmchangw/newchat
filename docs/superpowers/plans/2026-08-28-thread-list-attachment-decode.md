# Thread-List Attachment Decode — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `chat.user.{account}.request.user.{siteID}.thread.list` return each row's `parentMessage.attachments` and `lastMessage.attachments`, matching the shape every other history read path already emits and the contract `docs/client-api.md` already publishes.

**Architecture:** `buildThreadItems` pre-marshals both message bodies into `json.RawMessage` before user-service forwards them verbatim. It is the only hydration path in the service that skips `decodeMessageAttachments`, so the decoded field is never filled and `omitempty` drops the key. The fix adds the existing decode pass at that one call site — no new helper, no signature change, no repository change.

**Tech Stack:** Go 1.25, Cassandra (`gocql`), `go.uber.org/mock` (mockgen), `stretchr/testify`.

**Spec:** No separate design doc — bounded change agreed in chat. The design is restated in full under "Design Decisions" below; executors read this file alone.

## Global Constraints

- Go 1.25. Single `go.mod` at repo root.
- Never run raw `go` commands — always the root `Makefile` targets (`make test SERVICE=history-service`, `make lint`).
- TDD is mandatory: write the failing test, run it, confirm it fails, then implement.
- Minimum 80% coverage; target 90%+ for this service-layer logic.
- All tests use `-race` (the Makefile handles it).
- Unit tests never connect to a real database, NATS, or any external service.
- Branch: `claude/thread-list-attachments-gi77e7`. Never push elsewhere.

---

## Root Cause

`pkg/model/cassandra/message.go:84-85` splits the attachment column across two fields with deliberately opposite tags:

```go
Attachments        [][]byte     `json:"-"                     cql:"attachments"`
DecodedAttachments []Attachment `json:"attachments,omitempty" cql:"-"`
```

The raw `LIST<BLOB>` column is scanned from Cassandra but never serialized. The field the client actually receives is `DecodedAttachments`, which Cassandra never fills — only `decodeMessageAttachments` (`attachments.go:24`) does.

`buildThreadItems` (`threads.go:203`) marshals the hydrated rows directly:

```go
parentJSON, err := json.Marshal(&parent)   // threads.go:243
lastJSON,   err := json.Marshal(&last)     // threads.go:250
```

`GetMessagesByIDs` **does** select the column (`baseColumns`, `messages_by_room.go:14`), so the blobs are present in memory — they are simply never decoded. Result: no `attachments` key at all in either body.

Every sibling path already decodes — `GetThreadMessages` (`threads.go:61`, `:139`), `GetMessageByID` (`messages.go:441`), `msg.get.ids` (`messages.go:234`), and the room-list preview walk (`rooms.go:279`, whose comment reads *"The walk reads raw blobs; other paths decode via setDecodedAttachments"*). `buildThreadItems` is the single omission.

Nothing downstream contributes: `ThreadListItem.ParentMessage`/`LastMessage` are `json.RawMessage` and user-service forwards them untouched (`pkg/model/threadlist.go:33-34`), so history-service's marshal output is exactly what reaches the client.

---

## Design Decisions (agreed, do not re-litigate)

1. **Two decode calls, nothing more.** Add `decodeMessageAttachments(c, &parent)` and `decodeMessageAttachments(c, &last)` immediately before the two `json.Marshal` calls. Reuse the existing helper; do not add a thread-list-specific variant.

2. **Placement.** After the `hasParent`/`hasLast` skip and after the `LastSeenAt` assignment, before the marshals. Decoding a row that is about to be skipped would be wasted work.

3. **Mutation is safe.** `parent` and `last` are loop-local copies pulled out of the `msgByID` map by value, so mutating them cannot alias another row.

4. **Quoted parents come free.** `decodeMessageAttachments` also decodes `m.QuotedParentMessage`, which the thread list needs for quoted-reply previews. This is a required behavior, not incidental — it gets its own test.

5. **No `decodeMessageCard` — it does not exist and is not needed.** `Card`/`CardAction` are Cassandra UDTs with symmetric tags (`json:"card,omitempty" cql:"card"`), both present in `baseColumns`, so they already scan and marshal correctly on this path. Do not add a card decode function.

6. **`resolveRemovedMemberName` is out of scope.** `GetMessageByID` runs it after decoding; `buildThreadItems` does not. That concerns legacy system-message display names, not attachments, and is explicitly excluded (confirmed in chat).

7. **Leniency is inherited, not reimplemented.** `DecodeAttachments` skips malformed blobs and counts them; the helper logs the count at WARN. A bad blob must never drop the row from the page.

8. **No `docs/client-api.md` change.** `docs/client-api.md:2910` already documents `attachments` on the Message schema, and thread.list's `parentMessage`/`lastMessage` are typed `[Message](#message-schema)` (`:5842-5843`). The server was violating its own published contract; the fix makes it conform. No schema, struct, or handler registration changes, so the derived views (`request-reply.md`, `events.md`) are untouched too.

---

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `history-service/internal/service/threads.go` | Modify (`buildThreadItems`, ~`:240`) | Add the two decode calls before the marshals. |
| `history-service/internal/service/threadlist_test.go` | Modify | Four new tests plus two local helpers. |

No new files, no interface changes, no mock regeneration.

---

## Tasks

### Task 1 — Red: tests that pin the missing decode

- [ ] Add helper `attachmentBlobs(t, ...cassandra.Attachment) [][]byte` wrapping `cassandra.EncodeAttachments`, to build the raw column form.
- [ ] Add helper `rawBody(t, json.RawMessage) map[string]any`, so key presence/absence is assertable (a decode into `models.Message` cannot distinguish "absent" from "null").
- [ ] `TestHistoryService_ListThreadSubscriptions_DecodesAttachments` — parent and last each carry one encoded attachment; assert both bodies expose the decoded objects (id, title, fileType, imageUrl) and that raw blobs stay off the wire.
- [ ] `TestHistoryService_ListThreadSubscriptions_DecodesQuotedParentAttachments` — the last message carries a `QuotedParentMessage` with its own attachment; assert it decodes.
- [ ] `TestHistoryService_ListThreadSubscriptions_MalformedAttachmentSkipped` — one bad blob alongside one good; assert the good one ships and the row is **not** dropped.
- [ ] `TestHistoryService_ListThreadSubscriptions_NoAttachmentsOmitsKey` — no attachments; assert the key is absent, not `null`. (This one passes before the fix — it is the regression guard against emitting `[]`/`null`.)
- [ ] Run `make test SERVICE=history-service`. **Confirm the first three FAIL** with empty attachment slices. Do not proceed until the failure is observed.

### Task 2 — Green: the decode calls

- [ ] In `buildThreadItems`, before `json.Marshal(&parent)`, add:
  ```go
  // Both bodies ship pre-marshaled, so decode here: the raw blob column is
  // json:"-" and only DecodedAttachments reaches the client.
  decodeMessageAttachments(c, &parent)
  decodeMessageAttachments(c, &last)
  ```
- [ ] Run `make test SERVICE=history-service` — all four green, no existing test regressed.

### Task 3 — Verify

- [ ] `make lint` → 0 issues.
- [ ] `make test SERVICE=history-service` → all packages ok.
- [ ] `make sast` → no new medium+ findings.
- [ ] Commit on `claude/thread-list-attachments-gi77e7`.

---

## Risks

| Risk | Assessment |
|---|---|
| Payload size growth | Real but bounded. Decoded attachments are larger than the omitted key, and `user-service` already trims oversize rows via `blankOversizeThread` / `fitThreadPage`, so an inflated row degrades to `truncated: true` rather than breaking the page. Behavior matches every other read path. |
| Double decode | None. `buildThreadItems` is the only pass over these copies, and `DecodeAttachments` is a pure function of the raw column. |
| Client breakage | None expected — this adds a documented, previously-missing field. A client ignoring `attachments` is unaffected. |
