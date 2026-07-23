# Opaque thread.list Message Bodies — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Carry `ThreadListItem.ParentMessage`/`LastMessage` as opaque `json.RawMessage` so `user-service` forwards thread.list message bodies without decoding `cassandra.Message.Reactions` (a struct-keyed map with no JSON decoder), and have `history-service` emit them pre-marshaled.

**Architecture:** Retype the two shared fields on `pkg/model.ThreadListItem` from `*cassandra.Message` to `json.RawMessage`. `history-service` (`buildThreadItems`) marshals each typed body to bytes at build time; `user-service` needs no production change — the raw bytes pass through decode and re-encode untouched. The client-facing wire JSON is byte-for-byte unchanged (still the full `Message` object), so no docs edit is required.

**Tech Stack:** Go 1.25, `encoding/json` (stdlib), `stretchr/testify`, `go.uber.org/mock`. Tests run via `make test SERVICE=<name>` (race detector on by default).

## Global Constraints

- Go 1.25; monorepo, single root `go.mod`. Always use `make` targets, never raw `go`.
- TDD: Red → Green → Refactor → Commit for every change. Never write implementation before its test.
- `encoding/json` on both edited paths (history-service thread-list build + user-service historyclient decode both already use stdlib `encoding/json`, not sonic). Do not introduce sonic here.
- Error handling: never ignore a returned error; wrap infra errors with `fmt.Errorf("...: %w", err)`; log with `log/slog` structured fields (never interpolated). No secrets/bodies in logs.
- `models.Message` (in `history-service/internal/models`) is a type alias of `cassandra.Message`; `models.Reactions`/`models.ReactionKey`/`models.ReactorInfo` alias the `cassandra` types. Use the `models.*` aliases inside history-service tests.
- Minimum 80% coverage; cover happy path + the reactions regression path.
- **Compilation coupling:** retyping the shared field (Task 1) intentionally breaks `history-service` compilation until Task 2 lands. Execute Task 1 → Task 2 → Task 3 in order; only the full suite at Task 3 is expected green repo-wide.

---

### Task 1: Retype `ThreadListItem` message bodies to `json.RawMessage`

**Files:**
- Modify: `pkg/model/threadlist.go` (imports + the two body fields, ~lines 1-3 and 26-28)
- Test: `pkg/model/threadlist_test.go` (rewrite `TestThreadListItemJSON_WithMessages`; add reactions regression test)

**Interfaces:**
- Consumes: nothing (leaf change).
- Produces: `model.ThreadListItem.ParentMessage` and `.LastMessage` are now `json.RawMessage` (`type RawMessage []byte`). Producers assign marshaled `*cassandra.Message` bytes; consumers read raw bytes. `omitempty` omits nil/empty.

- [ ] **Step 1: Rewrite the failing tests (Red)**

In `pkg/model/threadlist_test.go`, add `"time"` to the import block (it already imports `encoding/json`, `testing`, `testify/assert`, `testify/require`, `pkg/model`, `pkg/model/cassandra`).

Replace the existing `TestThreadListItemJSON_WithMessages` with:

```go
// The hydrated parent/last message bodies are carried opaque and survive a JSON
// round trip; user-service forwards these bytes without decoding the Message.
func TestThreadListItemJSON_WithMessages(t *testing.T) {
	parent, err := json.Marshal(&cassandra.Message{MessageID: "msg-parent", RoomID: "room-1", Msg: "anyone?"})
	require.NoError(t, err)
	last, err := json.Marshal(&cassandra.Message{MessageID: "msg-last", RoomID: "room-1", Msg: "on it"})
	require.NoError(t, err)
	src := model.ThreadListItem{
		SiteID: "site-a", RoomID: "room-1", ThreadRoomID: "thr-1",
		ParentMessageID: "msg-parent", LastMsgAt: 1746518400000,
		ParentMessage: parent, LastMessage: last,
	}
	data, err := json.Marshal(&src)
	require.NoError(t, err)
	var dst model.ThreadListItem
	require.NoError(t, json.Unmarshal(data, &dst))
	require.NotNil(t, dst.ParentMessage)
	require.NotNil(t, dst.LastMessage)
	var gotParent, gotLast cassandra.Message
	require.NoError(t, json.Unmarshal(dst.ParentMessage, &gotParent))
	require.NoError(t, json.Unmarshal(dst.LastMessage, &gotLast))
	assert.Equal(t, "msg-parent", gotParent.MessageID)
	assert.Equal(t, "on it", gotLast.Msg)
}

// Regression: a parent/last body carrying reactions must round-trip through
// ThreadListItem without a decode error. The old *cassandra.Message field failed
// here because Reactions is a struct-keyed map with no JSON decoder.
func TestThreadListItemJSON_MessageWithReactions_RoundTrips(t *testing.T) {
	body, err := json.Marshal(&cassandra.Message{
		MessageID: "msg-parent", RoomID: "room-1", Msg: "anyone?",
		Reactions: cassandra.Reactions{
			{Emoji: "👍", UserAccount: "bob"}: {Account: "bob", EngName: "Bob Chen", ReactedAt: time.UnixMilli(1746518400000).UTC()},
		},
	})
	require.NoError(t, err)
	src := model.ThreadListItem{
		SiteID: "site-a", RoomID: "room-1", ThreadRoomID: "thr-1",
		ParentMessageID: "msg-parent", LastMsgAt: 1746518400000,
		ParentMessage: body, LastMessage: body,
	}
	data, err := json.Marshal(&src)
	require.NoError(t, err)
	var dst model.ThreadListItem
	require.NoError(t, json.Unmarshal(data, &dst)) // must NOT error on reactions
	var got map[string]any
	require.NoError(t, json.Unmarshal(dst.ParentMessage, &got))
	assert.Equal(t, "msg-parent", got["messageId"])
	reactions, ok := got["reactions"].(map[string]any)
	require.True(t, ok, "reactions must survive in the forwarded body")
	assert.Contains(t, reactions, "👍")
}
```

- [ ] **Step 2: Run tests to verify they fail (Red)**

Run: `make test SERVICE=model` (or `go test ./pkg/model/` if there is no per-package make target — the plan expects the model package to be reachable; if `SERVICE=model` is unknown, run the repo's model test path per the Makefile).

Expected: **compile failure** — `ParentMessage`/`LastMessage` are still `*cassandra.Message`, so assigning a `[]byte`/`json.RawMessage` and unmarshaling `dst.ParentMessage` do not type-check.

- [ ] **Step 3: Retype the fields (Green)**

In `pkg/model/threadlist.go`, change the import line

```go
import "github.com/hmchangw/chat/pkg/model/cassandra"
```

to

```go
import "encoding/json"
```

and replace the two body fields (currently `*cassandra.Message`) with:

```go
	// Hydrated message bodies, subject to the thread access window. Carried opaque:
	// history-service emits them pre-marshaled from *cassandra.Message and user-service
	// forwards them to the client verbatim, never decoding the Message (avoids parsing
	// Reactions, whose struct-keyed map has no JSON decoder).
	ParentMessage json.RawMessage `json:"parentMessage,omitempty" bson:"parentMessage,omitempty"`
	LastMessage   json.RawMessage `json:"lastMessage,omitempty"   bson:"lastMessage,omitempty"`
```

(`cassandra` is used nowhere else in this file, so the import swap is complete.)

- [ ] **Step 4: Run tests to verify they pass (Green)**

Run: the same model test command from Step 2.
Expected: **PASS**, including `TestThreadListItemJSON_WithMessages`, `TestThreadListItemJSON_MessageWithReactions_RoundTrips`, and the untouched `TestThreadListItemJSON_OmitsNilLastSeenAt` (still asserts `parentMessage` omitted when nil — `omitempty` on `json.RawMessage` omits nil).

- [ ] **Step 5: Commit**

```bash
git add pkg/model/threadlist.go pkg/model/threadlist_test.go
git commit -m "model(thread.list): carry parent/last message bodies as json.RawMessage

Retype ThreadListItem.ParentMessage/LastMessage from *cassandra.Message to
json.RawMessage so consumers forward the bodies without decoding the Message.
Reactions is a struct-keyed map with no JSON decoder, so the old typed field
failed to decode any body carrying reactions. Wire form unchanged.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 2: Pre-marshal bodies in history-service `buildThreadItems`

**Files:**
- Modify: `history-service/internal/service/threads.go` (add `encoding/json` import; the two assignments in `buildThreadItems`, currently `item.ParentMessage = &parent` / `item.LastMessage = &last`, ~lines 241-242)
- Test: `history-service/internal/service/threadlist_test.go` (decode raw bytes in existing assertions; add reactions case + a decode helper)

**Interfaces:**
- Consumes: `model.ThreadListItem.ParentMessage`/`.LastMessage` as `json.RawMessage` (Task 1).
- Produces: `buildThreadItems` sets each field to `json.Marshal(&msg)` bytes, or skips the row on marshal error. `ListThreadSubscriptions` response items carry pre-marshaled bodies.

- [ ] **Step 1: Update the tests (Red)**

In `history-service/internal/service/threadlist_test.go`, add `"encoding/json"` to the import block.

Add a decode helper near the top of the file (after imports):

```go
func decodeThreadMsg(t *testing.T, raw json.RawMessage) models.Message {
	t.Helper()
	var m models.Message
	require.NoError(t, json.Unmarshal(raw, &m))
	return m
}
```

In `TestHistoryService_ListThreadSubscriptions_Success`, replace the parent/last body assertions:

```go
	require.NotNil(t, first.ParentMessage)
	assert.Equal(t, "p1", first.ParentMessage.MessageID)
	require.NotNil(t, first.ParentMessage.TCount)
	assert.Equal(t, 4, *first.ParentMessage.TCount) // reply count rides on the parent
	require.NotNil(t, first.LastMessage)
	assert.Equal(t, "m1", first.LastMessage.MessageID)
```

with:

```go
	require.NotNil(t, first.ParentMessage)
	parent := decodeThreadMsg(t, first.ParentMessage)
	assert.Equal(t, "p1", parent.MessageID)
	require.NotNil(t, parent.TCount)
	assert.Equal(t, 4, *parent.TCount) // reply count rides on the parent
	require.NotNil(t, first.LastMessage)
	assert.Equal(t, "m1", decodeThreadMsg(t, first.LastMessage).MessageID)
```

Add a reactions regression case (place it after `TestHistoryService_ListThreadSubscriptions_Success`):

```go
// A parent carrying reactions still builds and its body rides through as the
// grouped-by-emoji wire form (guards the user-service forward path).
func TestHistoryService_ListThreadSubscriptions_ParentWithReactions(t *testing.T) {
	svc, msgs, _, _, threadSubs := newThreadListService(t)
	base := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	rows := []mongorepo.ThreadSubRow{
		{ThreadRoomID: "tr-1", RoomID: "r1", SiteID: "site-a", ParentMessageID: "p1", LastMsgID: "m1", LastMsgAt: base.Add(5 * time.Hour)},
	}
	threadSubs.EXPECT().ListUserThreadSubscriptions(gomock.Any(), "alice", gomock.Any(), gomock.Any(), gomock.Any()).Return(rows, false, nil)
	msgs.EXPECT().GetMessagesByIDs(gomock.Any(), gomock.Any()).Return([]models.Message{
		{MessageID: "p1", RoomID: "r1", Msg: "parent", Reactions: models.Reactions{
			{Emoji: "👍", UserAccount: "bob"}: {Account: "bob", EngName: "Bob Chen", ReactedAt: base},
		}},
		{MessageID: "m1", RoomID: "r1", Msg: "last"},
	}, nil)

	resp, err := svc.ListThreadSubscriptions(testContext(), pkgmodel.ThreadSubscriptionListRequest{Account: "alice", Limit: 10})
	require.NoError(t, err)
	require.Len(t, resp.Items, 1)
	assert.Equal(t, "p1", decodeThreadMsg(t, resp.Items[0].ParentMessage).MessageID)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(resp.Items[0].ParentMessage, &raw))
	reactions, ok := raw["reactions"].(map[string]any)
	require.True(t, ok, "reactions must be present in the built body")
	assert.Contains(t, reactions, "👍")
}
```

- [ ] **Step 2: Run tests to verify they fail (Red)**

Run: `make test SERVICE=history-service`
Expected: **compile failure** in `buildThreadItems` (`item.ParentMessage = &parent` assigns `*models.Message` to a `json.RawMessage` field) and in the updated test (`first.ParentMessage.MessageID` no longer type-checks). This is the coupling flagged in Global Constraints — Task 1 already retyped the field.

- [ ] **Step 3: Pre-marshal in `buildThreadItems` (Green)**

In `history-service/internal/service/threads.go`, add `"encoding/json"` to the import block (alongside `fmt`, `log/slog`, `time`).

Replace the two assignments:

```go
		item.ParentMessage = &parent
		item.LastMessage = &last
```

with:

```go
		parentJSON, err := json.Marshal(&parent)
		if err != nil {
			slog.WarnContext(c, "thread-list: marshaling parent message, skipping row",
				"request_id", natsutil.RequestIDFromContext(c),
				"thread_room_id", row.ThreadRoomID, "parent_message_id", row.ParentMessageID, "error", err)
			continue
		}
		lastJSON, err := json.Marshal(&last)
		if err != nil {
			slog.WarnContext(c, "thread-list: marshaling last message, skipping row",
				"request_id", natsutil.RequestIDFromContext(c),
				"thread_room_id", row.ThreadRoomID, "last_message_id", row.LastMsgID, "error", err)
			continue
		}
		item.ParentMessage = parentJSON
		item.LastMessage = lastJSON
```

Note: this sits inside the per-row loop after the existing `if !hasParent || !hasLast { continue }` guard and before `items = append(items, item)`, so a row is only appended once both bodies marshal — mirroring the existing "skip half-hydrated rows" rule. `c` is the `*natsrouter.Context` (a `context.Context`), valid for `slog.WarnContext`; `natsutil` is already imported.

- [ ] **Step 4: Run tests to verify they pass (Green)**

Run: `make test SERVICE=history-service`
Expected: **PASS**, including `TestHistoryService_ListThreadSubscriptions_Success`, `_ParentWithReactions`, and the existing missing-parent/ordering cases (they don't read the body).

- [ ] **Step 5: Commit**

```bash
git add history-service/internal/service/threads.go history-service/internal/service/threadlist_test.go
git commit -m "history-service(thread.list): emit parent/last bodies pre-marshaled

buildThreadItems now marshals each hydrated message to json.RawMessage bytes,
skipping the row with a warning on the (effectively impossible) marshal error,
consistent with the existing skip-half-hydrated-row rule.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 3: user-service forward-through regression test + full verification

**Files:**
- Test: `user-service/service/threads_test.go` (add one forward-through test; no production change)
- Verify only: `user-service/historyclient/client.go`, `user-service/service/threads.go` (confirm no edit needed), `docs/client-api.md` (confirm no diff)

**Interfaces:**
- Consumes: `model.ThreadListItem` with `json.RawMessage` bodies (Task 1); `MockHistoryClient.GetThreadList` returning them.
- Produces: nothing new — asserts existing `ListUserThreads` forwards bytes unchanged.

- [ ] **Step 1: Add the failing/forward test (Red)**

In `user-service/service/threads_test.go`, add `"encoding/json"` and `"github.com/hmchangw/chat/pkg/model/cassandra"` to the import block.

Add:

```go
// A message body carrying reactions is aggregated and forwarded verbatim: the
// old typed field would have failed the leaf-response decode inside historyclient;
// with json.RawMessage the bytes pass straight through the merge and re-encode.
func TestUserService_ListUserThreads_ForwardsMessageBodiesWithReactions(t *testing.T) {
	svc, history, _, _ := newThreadSvc(t)
	body, err := json.Marshal(&cassandra.Message{
		MessageID: "p1", RoomID: "r1", Msg: "parent",
		Reactions: cassandra.Reactions{
			{Emoji: "👍", UserAccount: "bob"}: {Account: "bob", EngName: "Bob Chen"},
		},
	})
	require.NoError(t, err)
	items := []model.ThreadListItem{{
		SiteID: "site-a", RoomID: "r1", RoomType: model.RoomTypeChannel, ThreadRoomID: "tr-1",
		ParentMessageID: "p1", LastMsgAt: 100, ParentMessage: body, LastMessage: body,
	}}
	expectThreadList(history, "site-a", items, false)
	expectThreadList(history, "site-b", nil, false)

	resp, err := svc.ListUserThreads(ctx("alice", "site-a"), model.ThreadListRequest{Limit: 10})
	require.NoError(t, err)
	require.Len(t, resp.Items, 1)
	// Bytes forwarded unchanged — no re-encode, no reactions parse.
	assert.Equal(t, []byte(body), []byte(resp.Items[0].ParentMessage))
	assert.Equal(t, []byte(body), []byte(resp.Items[0].LastMessage))
}
```

(`RoomTypeChannel` takes the no-op enrichment branch, so no `users`/`apps` mock expectations are needed. Both fan-out sites `site-a`/`site-b` must be stubbed — see `newThreadSvc`'s `AllSiteIDs`.)

- [ ] **Step 2: Run test to verify it passes (Green — additive)**

Run: `make test SERVICE=user-service`
Expected: **PASS**. `user-service` has no production change; this test compiles against the Task 1 field type and documents the forward-through contract. If it fails to compile, the missing imports from Step 1 are the cause.

- [ ] **Step 3: Confirm no production change is required in user-service**

Verify by reading, not editing:
- `user-service/historyclient/client.go` `GetThreadList` does `json.Unmarshal(msg.Data, &out)` into `model.ThreadSubscriptionListResponse` — with `json.RawMessage` fields this now copies bytes instead of parsing (this is the code path the bug lived on; the pkg/model round-trip test in Task 1 exercises the same stdlib unmarshal into the same type).
- `user-service/service/threads.go` `ListUserThreads` reads only `LastMsgAt`, `ThreadRoomID`, `RoomName`, `RoomType` — never the bodies.

Expected: no edits.

- [ ] **Step 4: Confirm docs need no change**

Run: `git diff --stat docs/` (expect empty) and inspect `docs/client-api.md` around "List User Threads" / "ThreadListItem": `parentMessage`/`lastMessage` are typed `[Message](#message-schema)`, which stays accurate because the wire bytes are still a `cassandra.Message` marshal. No edit to `docs/client-api.md`, `docs/client-api/request-reply.md`, or `docs/client-api/events.md`.

Expected: no docs diff.

- [ ] **Step 5: Full verification**

```bash
make test SERVICE=model
make test SERVICE=history-service
make test SERVICE=user-service
make lint
```

Expected: all green; lint clean.

- [ ] **Step 6: Commit**

```bash
git add user-service/service/threads_test.go
git commit -m "user-service(thread.list): regression test for opaque body forwarding

Asserts ListUserThreads forwards a reaction-carrying message body verbatim
through the cross-site merge without re-encoding or parsing. No production
change: historyclient now copies the json.RawMessage bytes instead of decoding
the Message.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Self-Review

**Spec coverage:**
- Retype `ThreadListItem` bodies to `json.RawMessage` → Task 1. ✓
- history-service emits pre-marshaled → Task 2. ✓
- user-service no production change / forward-through → Task 3 (test + verify). ✓
- Marshal-error → skip row with warn → Task 2 Step 3. ✓
- Reactions regression coverage at all three levels → Task 1 (model round-trip), Task 2 (builder), Task 3 (aggregator forward). ✓
- Wire-preserving, no docs diff → Task 3 Step 4. ✓
- Scope limited to thread.list (no `RoomsGet`/`model.LastMessage` change) → no task touches them. ✓

**Placeholder scan:** No TBD/TODO; all steps show concrete code and commands.

**Type consistency:** `json.RawMessage` used consistently for both fields across all tasks. `decodeThreadMsg` returns `models.Message` (alias of `cassandra.Message`). `models.Reactions`/`cassandra.Reactions` used per package. `ctx(account, site)`, `expectThreadList`, `newThreadSvc`, `newThreadListService`, `testContext`, `intPtr`, `ptrTime` all match existing helpers in their respective test files.
