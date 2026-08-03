# Message Forwarding Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a user forward an existing message into other rooms via `msg.send` — the new message carries an optional comment plus an immutable, server-built `forwardedMessage` snapshot of the source.

**Architecture:** Mirror of the quoting feature with three deltas: the source is fetched from the **source room's** `msg.get` subject (cross-room, authorization for free), fetch failures **hard-fail** (no placeholder, no re-projection), and the snapshot is **text-only** (sources with attachments/cards/system type are rejected). Client fans out one `msg.send` per destination room. Spec: `docs/superpowers/specs/2026-08-03-message-forwarding-design.md`.

**Tech Stack:** Go 1.25, NATS JetStream, Cassandra (gocql), sonic (gatekeeper/worker hot path), go.uber.org/mock, testify, testcontainers via `pkg/testutil`.

## Global Constraints

- All commands via `make` targets — never raw `go` commands (`make test SERVICE=<name>`, `make generate SERVICE=<name>`, `make lint`, `make test-integration SERVICE=<name>`).
- TDD Red-Green-Refactor for every task; run the failing test before implementing.
- Unit tests live in `package main` (services) / same package (libs); integration tests carry `//go:build integration` and use `pkg/testutil` containers.
- Error handling: handlers return typed `*errcode.Error` from named constructors; infra failures return raw `fmt.Errorf("…: %w", err)`. Never log AND return.
- sonic on gatekeeper/message-worker paths; never decode the full `cassandra.Message` under sonic (its `Reactions` map breaks the decoder) — use narrow projections.
- `forwarded_message` column exists in exactly 3 tables: `messages_by_room`, `messages_by_id`, `pinned_messages_by_room`. NEVER add it to `thread_messages_by_thread`.
- Commit after each task (pre-commit hook runs lint + tests). Branch: `claude/message-forwarding-impl-deq7za`.

---

### Task 1: `cassandra.ForwardedMessage` UDT type + schema mirrors

**Files:**
- Modify: `pkg/model/cassandra/message.go` (add type after `QuotedParentMessage`, add field to `Message` after `QuotedParentMessage` field)
- Test: `pkg/model/cassandra/message_test.go`
- Modify: `docs/cassandra_message_model.md`
- Create: `docker-local/cassandra/init/09-udt-forwarded_message.cql`
- Modify: `docker-local/cassandra/init/10-table-messages_by_room.cql`, `docker-local/cassandra/init/12-table-pinned_messages_by_room.cql`, `docker-local/cassandra/init/13-table-messages_by_id.cql`
- Create: `docker-local/cassandra/migrations/2026-08-forwarded-message.cql`
- Modify (inline test DDL): `message-worker/integration_test.go`, `history-service/internal/cassrepo/integration_test.go`, `history-service/internal/service/integration_test.go`, `tools/loadgen/history_integration_test.go`

**Interfaces:**
- Produces: `cassandra.ForwardedMessage` struct (fields below) and `cassandra.Message.ForwardedMessage *ForwardedMessage` — every later task references these exact names.

- [ ] **Step 1: Write the failing tests** — append to `pkg/model/cassandra/message_test.go` (uses the existing `roundTrip` helper in that file):

```go
func TestForwardedMessage_JSON(t *testing.T) {
	threadParent := time.Date(2026, 2, 1, 9, 0, 0, 0, time.UTC)
	f := ForwardedMessage{
		MessageID:             "01970a4f8c2d7c9aQRST",
		RoomID:                "r-src",
		Sender:                Participant{ID: "u1", Account: "alice"},
		CreatedAt:             time.Date(2026, 2, 2, 12, 0, 0, 0, time.UTC),
		Msg:                   "the forwarded body",
		Mentions:              []Participant{{ID: "u2", Account: "bob"}},
		MessageLink:           "https://chat.example.com/r-src/01970a4f8c2d7c9aQRST",
		ThreadParentID:        "01970a4f8c2d7c9aTHRD",
		ThreadParentCreatedAt: &threadParent,
	}
	got := roundTrip(t, f)
	assert.Equal(t, "01970a4f8c2d7c9aTHRD", got.ThreadParentID)
	require.NotNil(t, got.ThreadParentCreatedAt)
}

func TestForwardedMessage_JSON_Minimal(t *testing.T) {
	f := ForwardedMessage{
		MessageID: "m1", RoomID: "r1",
		Sender: Participant{ID: "u1"}, CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	got := roundTrip(t, f)
	assert.Empty(t, got.Msg)
	assert.Nil(t, got.Mentions)
	assert.Empty(t, got.MessageLink)
	assert.Empty(t, got.ThreadParentID)
	assert.Nil(t, got.ThreadParentCreatedAt)
}

func TestMessage_ForwardedMessage_JSON(t *testing.T) {
	msg := Message{
		RoomID: "r-dst", CreatedAt: time.Date(2026, 2, 3, 0, 0, 0, 0, time.UTC),
		MessageID: "m-fwd", Sender: Participant{ID: "u1", Account: "alice"}, Msg: "my comment",
		ForwardedMessage: &ForwardedMessage{
			MessageID: "m-src", RoomID: "r-src",
			Sender: Participant{ID: "u5", Account: "eve"},
			CreatedAt: time.Date(2026, 2, 2, 12, 0, 0, 0, time.UTC), Msg: "original",
		},
	}
	got := roundTrip(t, msg)
	require.NotNil(t, got.ForwardedMessage)
	assert.Equal(t, "m-src", got.ForwardedMessage.MessageID)
	// Absent field stays nil (omitempty):
	minimal := roundTrip(t, Message{RoomID: "r1", MessageID: "m1", CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Sender: Participant{ID: "u1"}, Msg: "hi"})
	assert.Nil(t, minimal.ForwardedMessage)
}
```

- [ ] **Step 2: Run and confirm FAIL** — `make test SERVICE=pkg/model/cassandra` (SERVICE is a path: the Makefile runs `go test -race ./$(SERVICE)/...`; expected: compile error `undefined: ForwardedMessage`).

- [ ] **Step 3: Implement the type** — in `pkg/model/cassandra/message.go`, after the `QuotedParentMessage` type:

```go
// ForwardedMessage maps to the Cassandra "ForwardedMessage" UDT — the immutable
// snapshot of a forwarded source message, captured at forward time by
// message-gatekeeper. Text-only by design: sources carrying attachments or
// cards are rejected at forward time, so the UDT has no attachment fields.
// Never redacted on read (self-contained; the source room's access window was
// enforced once, at forward time).
type ForwardedMessage struct {
	MessageID             string        `json:"messageId"                       cql:"message_id"`
	RoomID                string        `json:"roomId"                          cql:"room_id"`
	Sender                Participant   `json:"sender"                          cql:"sender"`
	CreatedAt             time.Time     `json:"createdAt"                       cql:"created_at"`
	Msg                   string        `json:"msg,omitempty"                   cql:"msg"`
	Mentions              []Participant `json:"mentions,omitempty"              cql:"mentions"`
	MessageLink           string        `json:"messageLink,omitempty"           cql:"message_link"`
	ThreadParentID        string        `json:"threadParentId,omitempty"        cql:"thread_parent_id"`
	ThreadParentCreatedAt *time.Time    `json:"threadParentCreatedAt,omitempty" cql:"thread_parent_created_at"`
}
```

And in the `Message` struct, directly after the `QuotedParentMessage` field:

```go
	ForwardedMessage      *ForwardedMessage    `json:"forwardedMessage,omitempty"      cql:"forwarded_message"`
```

- [ ] **Step 4: Run tests, verify PASS** — `make test SERVICE=pkg/model/cassandra` (or equivalent pkg target).

- [ ] **Step 5: Update `docs/cassandra_message_model.md`** — (a) add a new UDT section after `#### QuotedParentMessage` (alphabetical-ish placement is fine; match surrounding style):

```markdown
#### ForwardedMessage
```cql
CREATE TYPE IF NOT EXISTS "ForwardedMessage"(
  created_at TIMESTAMP,
  mentions SET<FROZEN<"Participant">>,
  message_id TEXT,
  message_link TEXT,
  msg TEXT,
  room_id TEXT,
  sender FROZEN<"Participant">,
  thread_parent_created_at TIMESTAMP,  // source's thread-parent createdAt (source was a thread reply)
  thread_parent_id TEXT                // set when the forwarded source is a thread reply
);
```
Immutable snapshot of a forwarded source message, built server-side by
message-gatekeeper at forward time. Text-only — no attachments column
(sources with attachments/cards are rejected). Present in
`messages_by_room`, `messages_by_id`, `pinned_messages_by_room` only;
`thread_messages_by_thread` is excluded because forwards always land in
the destination room's main timeline. See
`docs/superpowers/specs/2026-08-03-message-forwarding-design.md`.
```

(b) add `forwarded_message FROZEN<"ForwardedMessage">,` to the `messages_by_room`, `pinned_messages_by_room`, and `messages_by_id` CREATE TABLE blocks (alphabetical column position, i.e. after `enc_payload`); do NOT touch `thread_messages_by_thread`. (c) in the "Encryption (at rest)" section, extend the encrypted-columns list: change "and the body fields of `quoted_parent_message`" to "and the body fields of `quoted_parent_message` / `forwarded_message`".

- [ ] **Step 6: Local-dev init DDL** — create `docker-local/cassandra/init/09-udt-forwarded_message.cql` (must sort before the `10-`…`13-` table files):

```cql
CREATE TYPE IF NOT EXISTS chat."ForwardedMessage"(
  created_at TIMESTAMP,
  mentions SET<FROZEN<"Participant">>,
  message_id TEXT,
  message_link TEXT,
  msg TEXT,
  room_id TEXT,
  sender FROZEN<"Participant">,
  thread_parent_created_at TIMESTAMP,
  thread_parent_id TEXT
);
```

First `cat docker-local/cassandra/init/06-udt-quoted_parent_message.cql` and mirror its exact keyspace-qualification style (if it doesn't prefix `chat.`, don't either). Then add `forwarded_message FROZEN<"ForwardedMessage">,` to the column lists in `10-table-messages_by_room.cql`, `12-table-pinned_messages_by_room.cql`, `13-table-messages_by_id.cql`.

- [ ] **Step 7: Production migration** — create `docker-local/cassandra/migrations/2026-08-forwarded-message.cql` (mirror the header/comment style of `2026-06-pinnedat-messages-by-room.cql`):

```cql
-- Message forwarding: snapshot UDT + column on the three main-timeline tables.
-- Additive and online-safe; old rows read back NULL. thread_messages_by_thread
-- is deliberately excluded (forwards never land in threads).
CREATE TYPE IF NOT EXISTS "ForwardedMessage"(
  created_at TIMESTAMP,
  mentions SET<FROZEN<"Participant">>,
  message_id TEXT,
  message_link TEXT,
  msg TEXT,
  room_id TEXT,
  sender FROZEN<"Participant">,
  thread_parent_created_at TIMESTAMP,
  thread_parent_id TEXT
);
ALTER TABLE messages_by_room        ADD forwarded_message FROZEN<"ForwardedMessage">;
ALTER TABLE messages_by_id          ADD forwarded_message FROZEN<"ForwardedMessage">;
ALTER TABLE pinned_messages_by_room ADD forwarded_message FROZEN<"ForwardedMessage">;
```

- [ ] **Step 8: Inline test DDL mirrors** — in each of `message-worker/integration_test.go`, `history-service/internal/cassrepo/integration_test.go`, `history-service/internal/service/integration_test.go`, `tools/loadgen/history_integration_test.go`: next to that file's `CREATE TYPE … "QuotedParentMessage"` statement add (matching the file's single-line vs multi-line style):

```
CREATE TYPE IF NOT EXISTS %s."ForwardedMessage" (message_id TEXT, room_id TEXT, sender FROZEN<"Participant">, created_at TIMESTAMP, msg TEXT, mentions SET<FROZEN<"Participant">>, message_link TEXT, thread_parent_id TEXT, thread_parent_created_at TIMESTAMP)
```

and add `forwarded_message FROZEN<"ForwardedMessage">,` to that file's `messages_by_room`, `messages_by_id`, and `pinned_messages_by_room` CREATE TABLE blocks (whichever of the three exist in the file) — NOT `thread_messages_by_thread`. `data-migration/es-index-migrator` is deliberately untouched: its helpers select explicit columns and never run `cassrepo`.

- [ ] **Step 9: Commit**

```bash
git add pkg/model/cassandra/ docs/cassandra_message_model.md docker-local/cassandra/ message-worker/integration_test.go history-service/internal/cassrepo/integration_test.go history-service/internal/service/integration_test.go tools/loadgen/history_integration_test.go
git commit -m "feat(model): ForwardedMessage snapshot UDT + forwarded_message column mirrors"
```

---

### Task 2: Wire model — `SendMessageRequest` forward fields + `model.Message.ForwardedMessage`

**Files:**
- Modify: `pkg/model/message.go`
- Test: `pkg/model/model_test.go`

**Interfaces:**
- Consumes: `cassandra.ForwardedMessage` (Task 1).
- Produces: `SendMessageRequest.ForwardedMessageID string` / `SendMessageRequest.ForwardedRoomID string` (JSON `forwardedMessageId` / `forwardedRoomId`), `model.Message.ForwardedMessage *cassandra.ForwardedMessage` (JSON/bson `forwardedMessage`).

- [ ] **Step 1: Write failing tests** — append to `pkg/model/model_test.go` (that file has its own round-trip conventions; follow `TestSendMessageRequest_QuotedParentMessageID_JSON` at line ~2409 and `TestMessage_QuotedParentMessage_JSON` at ~2554 as templates):

```go
func TestSendMessageRequest_ForwardFields_JSON(t *testing.T) {
	req := SendMessageRequest{
		ID:                 "01970a4f8c2d7c9aQRST",
		Content:            "optional comment",
		RequestID:          "01970a4f-8c2d-7c9a-abcd-e0123456789f",
		ForwardedMessageID: "01970a4f8c2d7c9aSRCM",
		ForwardedRoomID:    "r-src",
	}
	data, err := json.Marshal(req)
	require.NoError(t, err)
	assert.Contains(t, string(data), `"forwardedMessageId":"01970a4f8c2d7c9aSRCM"`)
	assert.Contains(t, string(data), `"forwardedRoomId":"r-src"`)
	var got SendMessageRequest
	require.NoError(t, json.Unmarshal(data, &got))
	assert.Equal(t, req, got)

	// omitempty: absent when not forwarding
	plain, err := json.Marshal(SendMessageRequest{ID: "x", Content: "y", RequestID: "z"})
	require.NoError(t, err)
	assert.NotContains(t, string(plain), "forwardedMessageId")
	assert.NotContains(t, string(plain), "forwardedRoomId")
}

func TestMessage_ForwardedMessage_JSON(t *testing.T) {
	msg := Message{
		ID: "m-fwd", RoomID: "r-dst", UserID: "u1", UserAccount: "alice",
		Content: "check this out", CreatedAt: time.Date(2026, 8, 3, 10, 0, 0, 0, time.UTC),
		ForwardedMessage: &cassandra.ForwardedMessage{
			MessageID: "m-src", RoomID: "r-src",
			Sender:    cassandra.Participant{ID: "u5", Account: "eve"},
			CreatedAt: time.Date(2026, 8, 1, 9, 0, 0, 0, time.UTC),
			Msg:       "original body",
		},
	}
	data, err := json.Marshal(msg)
	require.NoError(t, err)
	assert.Contains(t, string(data), `"forwardedMessage"`)
	var got Message
	require.NoError(t, json.Unmarshal(data, &got))
	require.NotNil(t, got.ForwardedMessage)
	assert.Equal(t, "m-src", got.ForwardedMessage.MessageID)
	assert.Equal(t, "original body", got.ForwardedMessage.Msg)
}
```

- [ ] **Step 2: Run and confirm FAIL** (undefined fields).

- [ ] **Step 3: Implement** — in `pkg/model/message.go`: add to `SendMessageRequest` after the `Type` field:

```go
	// ForwardedMessageID + ForwardedRoomID request a forward: the gatekeeper
	// fetches the source message from ForwardedRoomID (enforcing the sender's
	// subscription + access window there), embeds an immutable text-only
	// snapshot on the new message, and hard-fails the send when the source
	// can't be fetched or is not forwardable. Both must be set together.
	// Mutually exclusive with ThreadParentMessageID, QuotedParentMessageID,
	// and Attachments; Content becomes optional on a forward.
	ForwardedMessageID string `json:"forwardedMessageId,omitempty"`
	ForwardedRoomID    string `json:"forwardedRoomId,omitempty"`
```

Add to `Message` after the `QuotedParentMessage` field:

```go
	ForwardedMessage             *cassandra.ForwardedMessage    `json:"forwardedMessage,omitempty"             bson:"forwardedMessage,omitempty"`
```

- [ ] **Step 4: Run tests, verify PASS** — `make test SERVICE=pkg/model`.

- [ ] **Step 5: Commit** — `git add pkg/model/ && git commit -m "feat(model): forward fields on SendMessageRequest and Message"`

---

### Task 3: `pkg/atrest` — encrypt the snapshot body

**Files:**
- Modify: `pkg/atrest/atrest.go`, `pkg/atrest/split.go`
- Test: `pkg/atrest/split_test.go`

**Interfaces:**
- Consumes: `cassandra.ForwardedMessage` (Task 1).
- Produces: `atrest.ForwardedEncrypted{Msg string}`, `EncryptedFields.ForwardedContent *ForwardedEncrypted`; `SplitForEncryption`/`StripEncryptedFields`/`ApplyDecryptedFields` handle it. Tasks 6–7 rely on these exact names.

- [ ] **Step 1: Write failing tests** — append to `pkg/atrest/split_test.go` (mirror the existing quoted-content tests in that file):

```go
func TestSplitForEncryption_ForwardedContent(t *testing.T) {
	msg := &cassandra.Message{
		RoomID: "r1", MessageID: "m1", Msg: "comment",
		ForwardedMessage: &cassandra.ForwardedMessage{
			MessageID: "m-src", RoomID: "r-src", Msg: "forwarded body",
			Sender: cassandra.Participant{ID: "u5", Account: "eve"},
		},
	}
	out := SplitForEncryption(msg)
	require.NotNil(t, out.ForwardedContent)
	assert.Equal(t, "forwarded body", out.ForwardedContent.Msg)
	// input not mutated
	assert.Equal(t, "forwarded body", msg.ForwardedMessage.Msg)
}

func TestSplitForEncryption_ForwardedContent_EmptyBodyOmitted(t *testing.T) {
	msg := &cassandra.Message{RoomID: "r1", MessageID: "m1",
		ForwardedMessage: &cassandra.ForwardedMessage{MessageID: "m-src", RoomID: "r-src"}}
	assert.Nil(t, SplitForEncryption(msg).ForwardedContent)
}

func TestStripEncryptedFields_BlanksForwardedBody(t *testing.T) {
	msg := &cassandra.Message{Msg: "comment",
		ForwardedMessage: &cassandra.ForwardedMessage{MessageID: "m-src", Msg: "forwarded body"}}
	StripEncryptedFields(msg)
	assert.Empty(t, msg.ForwardedMessage.Msg)
	assert.Equal(t, "m-src", msg.ForwardedMessage.MessageID, "metadata survives")
}

func TestApplyDecryptedFields_RestoresForwardedBody(t *testing.T) {
	msg := &cassandra.Message{ForwardedMessage: &cassandra.ForwardedMessage{MessageID: "m-src"}}
	ApplyDecryptedFields(msg, &EncryptedFields{Msg: "comment", ForwardedContent: &ForwardedEncrypted{Msg: "forwarded body"}})
	assert.Equal(t, "comment", msg.Msg)
	assert.Equal(t, "forwarded body", msg.ForwardedMessage.Msg)

	// Defensive: bundle carries forward content but the UDT column was lost — struct is created.
	bare := &cassandra.Message{}
	ApplyDecryptedFields(bare, &EncryptedFields{ForwardedContent: &ForwardedEncrypted{Msg: "x"}})
	require.NotNil(t, bare.ForwardedMessage)
	assert.Equal(t, "x", bare.ForwardedMessage.Msg)
}
```

- [ ] **Step 2: Run and confirm FAIL** (undefined `ForwardedEncrypted`).

- [ ] **Step 3: Implement** — in `pkg/atrest/atrest.go`, add to `EncryptedFields` after `QuotedParentContent`:

```go
	ForwardedContent    *ForwardedEncrypted    `json:"forwardedContent,omitempty"`
```

and after the `QuotedParentEncrypted` type:

```go
// ForwardedEncrypted holds the user-authored body of a forwarded-message
// snapshot. Sender, IDs, timestamps and mentions stay plaintext on the
// forwarded_message UDT. No attachments field — the snapshot is text-only.
type ForwardedEncrypted struct {
	Msg string `json:"msg,omitempty"`
}
```

In `pkg/atrest/split.go` — `SplitForEncryption`, after the QuotedParentMessage block:

```go
	if msg.ForwardedMessage != nil && msg.ForwardedMessage.Msg != "" {
		out.ForwardedContent = &ForwardedEncrypted{Msg: msg.ForwardedMessage.Msg}
	}
```

`StripEncryptedFields`, after the quoted block:

```go
	if msg.ForwardedMessage != nil {
		msg.ForwardedMessage.Msg = ""
	}
```

`ApplyDecryptedFields`, after the quoted block:

```go
	if enc.ForwardedContent != nil {
		if msg.ForwardedMessage == nil {
			msg.ForwardedMessage = &cassandra.ForwardedMessage{}
		}
		msg.ForwardedMessage.Msg = enc.ForwardedContent.Msg
	}
```

Also update the `StripEncryptedFields` doc comment's "quoted_parent_message metadata … is preserved" sentence to mention `forwarded_message` the same way.

- [ ] **Step 4: Run tests, verify PASS.**

- [ ] **Step 5: Commit** — `git add pkg/atrest/ && git commit -m "feat(atrest): encrypt forwarded-snapshot body in the at-rest bundle"`

---

### Task 4: Gatekeeper fetcher — `FetchForwardedSource`

**Files:**
- Modify: `message-gatekeeper/store.go` (interface), `message-gatekeeper/fetcher_history.go`
- Regenerate: `message-gatekeeper/mock_store_test.go` via `make generate SERVICE=message-gatekeeper`
- Test: `message-gatekeeper/fetcher_history_test.go`, `message-gatekeeper/sonic_wire_test.go`

**Interfaces:**
- Consumes: `subject.MsgGet(account, roomID, siteID)`, `errcode.Parse`, `messageLink(baseURL, roomID, messageID)` (all existing).
- Produces: `ParentMessageFetcher.FetchForwardedSource(ctx, account, srcRoomID, siteID, messageID string) (*forwardSourceProjection, error)` and the `forwardSourceProjection` struct — Task 5's handler consumes both, and `MockParentMessageFetcher` gains the method via mockgen.

- [ ] **Step 1: Write failing fetcher tests** — append to `message-gatekeeper/fetcher_history_test.go`, reusing that file's in-process NATS server helper (see its `TestHistoryParentFetcher_FetchQuotedParent` for the exact setup — a `natsserver` on a random port, a `nc.Subscribe` responder on `subject.MsgGet(...)`, and `newHistoryParentFetcher(conn, baseURL)`):

```go
func TestHistoryParentFetcher_FetchForwardedSource(t *testing.T) {
	nc, cleanup := startTestNATS(t) // use the exact helper name already in this file
	defer cleanup()
	const (
		account   = "alice"
		srcRoomID = "r-src"
		siteID    = "site-a"
		messageID = "01970a4f8c2d7c9aSRCM"
		baseURL   = "https://chat.example.com"
	)
	fetcher := newHistoryParentFetcher(nc, baseURL)

	t.Run("success projects snapshot fields and reject signals", func(t *testing.T) {
		sub, err := nc.Subscribe(subject.MsgGet(account, srcRoomID, siteID), func(m *nats.Msg) {
			// history-service replies with the full cassandra.Message JSON;
			// include reject-signal fields to prove the projection surfaces them.
			_ = m.Respond([]byte(`{
				"roomId":"r-src",
				"sender":{"id":"u5","account":"eve"},
				"createdAt":"2026-08-01T09:00:00Z",
				"msg":"original body",
				"mentions":[{"id":"u2","account":"bob"}],
				"threadParentId":"01970a4f8c2d7c9aTHRD",
				"deleted":false,
				"attachments":[{"id":"f1","title":"a.png","type":"file"}],
				"card":{"template":"approval"},
				"forwardedMessage":{"messageId":"m-deeper"}
			}`))
		})
		require.NoError(t, err)
		defer func() { _ = sub.Unsubscribe() }()

		got, err := fetcher.FetchForwardedSource(context.Background(), account, srcRoomID, siteID, messageID)
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, "r-src", got.RoomID)
		assert.Equal(t, "eve", got.Sender.Account)
		assert.Equal(t, "original body", got.Msg)
		assert.Len(t, got.Mentions, 1)
		assert.Equal(t, "01970a4f8c2d7c9aTHRD", got.ThreadParentID)
		assert.NotEmpty(t, got.Attachments, "attachment presence must survive the projection")
		assert.NotEmpty(t, got.Card, "card presence must survive the projection")
		assert.NotEmpty(t, got.ForwardedMessage, "chain presence must survive the projection")
		assert.Equal(t, baseURL+"/r-src/"+messageID, got.MessageLink)
	})

	t.Run("errcode envelope propagates typed error", func(t *testing.T) {
		sub, err := nc.Subscribe(subject.MsgGet(account, srcRoomID, siteID), func(m *nats.Msg) {
			_ = m.Respond([]byte(`{"code":"not_found","error":"message not found"}`))
		})
		require.NoError(t, err)
		defer func() { _ = sub.Unsubscribe() }()

		got, err := fetcher.FetchForwardedSource(context.Background(), account, srcRoomID, siteID, messageID)
		assert.Nil(t, got)
		var ee *errcode.Error
		require.ErrorAs(t, err, &ee)
		assert.Equal(t, errcode.CodeNotFound, ee.Code)
	})

	t.Run("no responder wraps transport error", func(t *testing.T) {
		got, err := fetcher.FetchForwardedSource(context.Background(), account, "r-nobody", siteID, messageID)
		assert.Nil(t, got)
		require.Error(t, err)
		var ee *errcode.Error
		assert.False(t, errors.As(err, &ee), "transport failure must stay a bare error")
	})
}
```

Adjust the helper invocation (`startTestNATS`) to whatever the file actually names it; do not spin your own server pattern.

- [ ] **Step 2: Run and confirm FAIL** — `make test SERVICE=message-gatekeeper` (undefined `FetchForwardedSource`).

- [ ] **Step 3: Implement** — in `message-gatekeeper/store.go`, extend the interface (and its doc comment) —:

```go
type ParentMessageFetcher interface {
	FetchQuotedParent(ctx context.Context, account, roomID, siteID, messageID string) (*cassandra.QuotedParentMessage, error)
	// FetchForwardedSource fetches the forward source from the SOURCE room's
	// msg.get subject — history-service enforces the requester's subscription
	// and access window there, so authorization needs no extra check here.
	// Unlike quotes the caller hard-fails on every error (no placeholder).
	FetchForwardedSource(ctx context.Context, account, srcRoomID, siteID, messageID string) (*forwardSourceProjection, error)
}
```

In `message-gatekeeper/fetcher_history.go` add (`"encoding/json"` joins the imports for `json.RawMessage`):

```go
// forwardSourceProjection decodes only the source-message fields the forward
// path needs: the snapshot fields plus the accept/reject signals. Same sonic
// rationale as quotedParentProjection — never decode the full cassandra.Message
// (its struct-keyed Reactions map breaks sonic's decoder). Presence-only
// fields decode as json.RawMessage: the handler branches on presence, never
// the inner shape. All three are omitempty on the wire, so they are absent
// (len 0) rather than JSON null when unset.
type forwardSourceProjection struct {
	RoomID                string                  `json:"roomId"`
	Sender                cassandra.Participant   `json:"sender"`
	CreatedAt             time.Time               `json:"createdAt"`
	Msg                   string                  `json:"msg"`
	Mentions              []cassandra.Participant `json:"mentions"`
	ThreadParentID        string                  `json:"threadParentId"`
	ThreadParentCreatedAt *time.Time              `json:"threadParentCreatedAt"`
	Deleted               bool                    `json:"deleted"`
	Type                  string                  `json:"type"`
	Attachments           json.RawMessage         `json:"attachments"`      // presence-only
	Card                  json.RawMessage         `json:"card"`             // presence-only
	ForwardedMessage      json.RawMessage         `json:"forwardedMessage"` // presence-only (chain detection)
	// MessageLink is built by the fetcher from chatBaseURL, not decoded from the reply.
	MessageLink string `json:"-"`
}

// FetchForwardedSource issues a NATS request to history-service's
// GetMessageByID handler on the SOURCE room's subject and projects the reply.
// Every error (timeout, no responder, errcode envelope, unmarshal) is
// returned — the caller hard-fails the send on any of them.
func (f *historyParentFetcher) FetchForwardedSource(
	ctx context.Context,
	account, srcRoomID, siteID, messageID string,
) (*forwardSourceProjection, error) {
	reqBytes, err := sonic.Marshal(getMessageByIDRequest{MessageID: messageID})
	if err != nil {
		return nil, fmt.Errorf("marshal GetMessageByID request: %w", err)
	}

	subj := subject.MsgGet(account, srcRoomID, siteID)
	msg, err := f.nc.Request(ctx, subj, reqBytes, historyRequestTimeout)
	if err != nil {
		return nil, fmt.Errorf("forward source request: %w", err)
	}

	if ee, ok := errcode.Parse(msg.Data); ok && ee.Code.Valid() {
		return nil, ee
	}

	var src forwardSourceProjection
	if err := sonic.Unmarshal(msg.Data, &src); err != nil {
		return nil, fmt.Errorf("unmarshal forward source: %w", err)
	}
	src.MessageLink = messageLink(f.chatBaseURL, src.RoomID, messageID)
	return &src, nil
}
```

- [ ] **Step 4: Regenerate mocks** — `make generate SERVICE=message-gatekeeper` (updates `mock_store_test.go` with the new method).

- [ ] **Step 5: Run tests, verify PASS** — `make test SERVICE=message-gatekeeper`.

- [ ] **Step 6: Extend the sonic wire test** — in `message-gatekeeper/sonic_wire_test.go`, add to `TestSonic_DecodesClientRequest`'s `orig` literal (keeping existing fields):

```go
		ForwardedMessageID:    "01970a4f8c2d7c9aSRCM",
		ForwardedRoomID:       "r-src",
```

Run `make test SERVICE=message-gatekeeper`; PASS.

- [ ] **Step 7: Commit** — `git add message-gatekeeper/ && git commit -m "feat(gatekeeper): FetchForwardedSource projection via source-room msg.get"`

---

### Task 5: Gatekeeper handler — forward validation + snapshot resolution

**Files:**
- Modify: `message-gatekeeper/handler.go`
- Test: `message-gatekeeper/handler_test.go`

**Interfaces:**
- Consumes: `forwardSourceProjection`, `FetchForwardedSource` (Task 4), `model.SendMessageRequest.ForwardedMessageID/ForwardedRoomID` (Task 2), `cassandra.ForwardedMessage` (Task 1).
- Produces: canonical `model.Message` events with `.ForwardedMessage` set — Tasks 6/8 consume the event shape.

- [ ] **Step 1: Write failing handler tests** — add a new table-driven test to `message-gatekeeper/handler_test.go`, cloning the runner shape of `TestHandler_ProcessMessage` (construct `&Handler{store, publish, siteID, parentFetcher, largeRoomThreshold}` with `NewMockStore` + `NewMockParentMessageFetcher`, call `h.processMessage`, branch on `wantErr`/`wantInfra`/`wantNoPublish`). Shared fixtures inside the test func:

```go
	validID := idgen.GenerateMessageID()
	fwdID := idgen.GenerateMessageID()
	validRequestID := "01970a4f-8c2d-7c9a-abcd-e0123456789f"
	validAccount, validRoomID, validSiteID := "alice", "room-dst", "site-a"
	srcRoomID := "room-src"
	srcCreatedAt := time.Date(2026, 8, 1, 9, 0, 0, 0, time.UTC)
	sub := &model.Subscription{User: model.SubscriptionUser{ID: "u1", Account: validAccount}, RoomID: validRoomID, Roles: []model.Role{model.RoleMember}}
	okSource := func() *forwardSourceProjection {
		return &forwardSourceProjection{
			RoomID: srcRoomID, Sender: cassandra.Participant{ID: "u5", Account: "eve"},
			CreatedAt: srcCreatedAt, Msg: "original body",
			Mentions:    []cassandra.Participant{{ID: "u2", Account: "bob"}},
			MessageLink: "https://chat.example.com/" + srcRoomID + "/" + fwdID,
		}
	}
	subscribed := func(s *MockStore) {
		s.EXPECT().GetSubscription(gomock.Any(), validAccount, validRoomID).Return(sub, nil)
		s.EXPECT().GetRoomMeta(gomock.Any(), validRoomID).Return(roommetacache.Meta{ID: validRoomID, UserCount: 1}, nil)
	}
	fwdReq := func(content string) []byte {
		data, _ := json.Marshal(model.SendMessageRequest{
			ID: validID, Content: content, RequestID: validRequestID,
			ForwardedMessageID: fwdID, ForwardedRoomID: srcRoomID,
		})
		return data
	}
```

Table rows (each row's `setupFetcher` mocks `FetchForwardedSource(gomock.Any(), validAccount, srcRoomID, validSiteID, fwdID)`):

1. **happy path with comment** — fetcher returns `okSource()`; assert reply `Message.ForwardedMessage` non-nil with `MessageID == fwdID`, `RoomID == srcRoomID`, `Msg == "original body"`, `Sender.Account == "eve"`, `MessageLink` set; published canonical event's `evt.Message.ForwardedMessage` equal; `evt.Message.Content == "check this"`.
2. **happy path with empty content** — `fwdReq("")`; fetcher returns `okSource()`; NO error (the empty-content rule is waived on forwards); reply `Content == ""`, snapshot present.
3. **forwardedMessageId without forwardedRoomId → bad_request** — body built with only `forwardedMessageId`; no store/fetcher expectations; `wantErr`, `wantNoPublish`.
4. **forwardedRoomId without forwardedMessageId → bad_request** — mirror of row 3.
5. **invalid forwardedMessageId → bad_request** — `"forwardedMessageId":"nope!"`; no store calls.
6. **forward + quotedParentMessageId → bad_request**.
7. **forward + threadParentMessageId → bad_request**.
8. **forward + attachments on the new message → bad_request** — request includes one attachment blob.
9. **source not found → typed not_found, no publish** — fetcher returns `(nil, errcode.NotFound("message not found"))`; `checkErr` asserts `ee.Code == errcode.CodeNotFound`.
10. **source forbidden → typed forbidden** — fetcher returns `(nil, errcode.Forbidden("not subscribed to room"))`.
11. **transport failure → typed unavailable (Ack, not Nak)** — fetcher returns `(nil, fmt.Errorf("history request: nats: timeout"))`; `wantErr` with `wantInfra: false` (hard-fail is a client reply, never a redelivery loop); `checkErr` asserts `ee.Code == errcode.CodeUnavailable`.
12. **deleted source → bad_request** — `okSource()` with `Deleted: true`.
13. **system-message source → bad_request** — `okSource()` with `Type: model.MessageTypeRoomRenamed`.
14. **source with attachments → bad_request** — `okSource()` with `Attachments: json.RawMessage(`[{"id":"f1"}]`)`.
15. **source with card → bad_request** — `okSource()` with `Card: json.RawMessage(`{"template":"x"}`)`.
16. **forward-of-forward with comment → snapshot carries the comment only** — `okSource()` with `Msg: "their comment"`, `ForwardedMessage: json.RawMessage(`{"messageId":"m-deeper"}`)`; success; assert snapshot `Msg == "their comment"` (chain depth 1 — reply JSON must NOT contain `m-deeper`).
17. **forward-of-forward with empty comment → bad_request** — `okSource()` with `Msg: ""`, `ForwardedMessage: json.RawMessage(`{"messageId":"m-deeper"}`)`.
18. **"important" source is forwardable** — `okSource()` with `Type: model.MessageTypeImportant`; success.

Rows 3–8 fail before the subscription lookup: give them empty `setupStore`. Rows 9–18 need `subscribed(s)`.

- [ ] **Step 2: Run and confirm FAIL** — `make test SERVICE=message-gatekeeper` (validation doesn't exist yet; happy path fails because `ForwardedMessage` is never set).

- [ ] **Step 3: Implement in `message-gatekeeper/handler.go`.**

(a) In `processMessage`, after the quoted-parent-ID validation block (after line ~256) insert:

```go
	// Forward validation: both fields together, a valid source ID, and no
	// combination with thread/quote/attachments — a forward is a main-timeline
	// message carrying a comment plus a server-built snapshot (see
	// docs/superpowers/specs/2026-08-03-message-forwarding-design.md).
	isForward := req.ForwardedMessageID != "" || req.ForwardedRoomID != ""
	if isForward {
		if req.ForwardedMessageID == "" || req.ForwardedRoomID == "" {
			return nil, errcode.BadRequest("forwardedMessageId and forwardedRoomId must be set together")
		}
		if !idgen.IsValidMessageID(req.ForwardedMessageID) {
			return nil, errcode.BadRequest(fmt.Sprintf("invalid forwarded message ID %q: must be a 20-char base62 string", req.ForwardedMessageID))
		}
		if req.QuotedParentMessageID != "" {
			return nil, errcode.BadRequest("a forward cannot also quote a message")
		}
		if req.ThreadParentMessageID != "" {
			return nil, errcode.BadRequest("a forward cannot target a thread")
		}
		if len(req.Attachments) > 0 {
			return nil, errcode.BadRequest("a forward cannot carry attachments")
		}
	}
```

(b) Change the empty-content check to waive it for forwards:

```go
	// A message with attachments may carry empty content; so may a forward
	// (the snapshot is the content).
	if req.Content == "" && len(req.Attachments) == 0 && !isForward {
		return nil, errcode.BadRequest("content must not be empty")
	}
```

(c) After the `resolveQuoteSnapshot` call block, add:

```go
	forwardSnapshot, err := h.resolveForwardSnapshot(ctx, account, siteID, req)
	if err != nil {
		return nil, err
	}
```

(d) Add `ForwardedMessage: forwardSnapshot,` to the `model.Message` literal (after `QuotedParentMessage`).

(e) Add the resolver (place after `resolveQuoteSnapshot`):

```go
// resolveForwardSnapshot fetches the forward source (from the SOURCE room's
// msg.get subject — history-service enforces the forwarder's subscription and
// access window there) and projects it into the immutable snapshot. Hard-fail
// policy: ANY failure rejects the send — no placeholder, no re-projection (a
// forward without its content is meaningless). Transport failures collapse to
// a typed unavailable error so the JetStream msg is replied+acked, never
// Nak-looped on a dead history.
func (h *Handler) resolveForwardSnapshot(ctx context.Context, account, siteID string, req *model.SendMessageRequest) (*cassandra.ForwardedMessage, error) {
	if req.ForwardedMessageID == "" {
		return nil, nil
	}
	src, err := h.parentFetcher.FetchForwardedSource(ctx, account, req.ForwardedRoomID, siteID, req.ForwardedMessageID)
	if err == nil && src == nil {
		err = fmt.Errorf("fetch forward source %s: fetcher returned nil projection", req.ForwardedMessageID)
	}
	if err != nil {
		var ee *errcode.Error
		if errors.As(err, &ee) {
			// Preserve the upstream category (not_found, forbidden,
			// unavailable, …) — hard-fail on all of them, never degrade.
			return nil, ee
		}
		slog.WarnContext(ctx, "forward source fetch failed",
			"request_id", req.RequestID, "forwarded_id", req.ForwardedMessageID, "error", err)
		return nil, errcode.Unavailable("forward source temporarily unavailable")
	}
	if src.Deleted {
		return nil, errcode.BadRequest("cannot forward a deleted message")
	}
	if model.IsSystemMessageType(src.Type) {
		return nil, errcode.BadRequest("cannot forward a system message")
	}
	if len(src.Attachments) > 0 {
		return nil, errcode.BadRequest("cannot forward a message with attachments")
	}
	if len(src.Card) > 0 {
		return nil, errcode.BadRequest("cannot forward a card message")
	}
	// Chain depth stays 1: forwarding a forward captures only that message's
	// own comment; the nested snapshot is dropped, and an empty comment leaves
	// nothing forwardable.
	if len(src.ForwardedMessage) > 0 && src.Msg == "" {
		return nil, errcode.BadRequest("source forward has no forwardable content")
	}
	return &cassandra.ForwardedMessage{
		MessageID:             req.ForwardedMessageID,
		RoomID:                src.RoomID,
		Sender:                src.Sender,
		CreatedAt:             src.CreatedAt,
		Msg:                   src.Msg,
		Mentions:              src.Mentions,
		MessageLink:           src.MessageLink,
		ThreadParentID:        src.ThreadParentID,
		ThreadParentCreatedAt: src.ThreadParentCreatedAt,
	}, nil
}
```

Note: `slog.WarnContext` before returning a DIFFERENT error than the logged one (transport → unavailable) is not a double-log: `Classify` logs the typed unavailable, the Warn preserves the underlying transport cause which the typed error deliberately omits (never leak infra detail to clients).

- [ ] **Step 4: Run tests, verify PASS** — `make test SERVICE=message-gatekeeper`.

- [ ] **Step 5: Commit** — `git add message-gatekeeper/ && git commit -m "feat(gatekeeper): forward sends — validation, hard-fail source fetch, snapshot embed"`

---

### Task 6: message-worker — persist the snapshot

**Files:**
- Modify: `message-worker/store_cassandra.go`
- Test: `message-worker/handler_test.go`, `message-worker/store_cassandra_test.go` (only if it unit-tests `buildCassandraMessage`), `message-worker/integration_test.go`

**Interfaces:**
- Consumes: `model.Message.ForwardedMessage` (Task 2), `atrest.SplitForEncryption`/`StripEncryptedFields` (Task 3), Task 1 DDL in `integration_test.go`.
- Produces: `forwarded_message` written on both main-timeline tables, plaintext and encrypted.

- [ ] **Step 1: Write failing unit test** — append to `message-worker/handler_test.go` a row (or standalone test matching the file's `store.EXPECT().SaveMessage(gomock.Any(), &msg, &expectedSender, "site-a")` style) where the incoming `model.MessageEvent.Message` carries:

```go
	ForwardedMessage: &cassandra.ForwardedMessage{
		MessageID: "01970a4f8c2d7c9aSRCM", RoomID: "r-src",
		Sender: cassandra.Participant{ID: "u5", Account: "eve"},
		CreatedAt: time.Date(2026, 8, 1, 9, 0, 0, 0, time.UTC), Msg: "original body",
	},
```

and asserts the exact `*model.Message` (snapshot pointer included) reaches `SaveMessage` — the strict `&msg` argument match the file already uses covers this once the fixture carries the field. Also add a unit test for the clone semantics of `buildCassandraMessage`:

```go
func TestBuildCassandraMessage_ClonesForwardedMessage(t *testing.T) {
	src := &model.Message{
		ID: "m1", RoomID: "r1", Content: "comment",
		ForwardedMessage: &cassandra.ForwardedMessage{MessageID: "m-src", Msg: "body"},
	}
	cm := buildCassandraMessage(src)
	require.NotNil(t, cm.ForwardedMessage)
	assert.Equal(t, "body", cm.ForwardedMessage.Msg)
	// Fresh struct: stripping the copy must not mutate the caller's message.
	cm.ForwardedMessage.Msg = ""
	assert.Equal(t, "body", src.ForwardedMessage.Msg)
}
```

- [ ] **Step 2: Run and confirm FAIL** — `make test SERVICE=message-worker` (clone test fails: `cm.ForwardedMessage` nil).

- [ ] **Step 3: Implement in `message-worker/store_cassandra.go`.**

(a) `buildCassandraMessage` — after the QuotedParentMessage block:

```go
	if msg.ForwardedMessage != nil {
		f := *msg.ForwardedMessage
		cm.ForwardedMessage = &f
	}
```

(b) `SaveMessage` (plaintext): in BOTH INSERT statements add `forwarded_message` to the column list directly after `quoted_parent_message`, one more `?`, and bind `msg.ForwardedMessage` directly after the `msg.QuotedParentMessage` argument.

(c) `saveMessageEncrypted`: same column addition after `quoted_parent_message` in BOTH statements, binding `cm.ForwardedMessage` directly after `cm.QuotedParentMessage` (the stripped copy — body already moved into `enc_payload`).

(d) `SaveThreadMessage` / `saveThreadMessageEncrypted`: NO changes — gatekeeper rejects forward+thread, so a forward never reaches the thread path.

- [ ] **Step 4: Run unit tests, verify PASS** — `make test SERVICE=message-worker`.

- [ ] **Step 5: Write + run integration tests** — append to `message-worker/integration_test.go` (Task 1 already added the DDL there; follow `TestCassandraStore_SaveMessage`'s setup pattern for session/store construction, plaintext `cipher=nil` and, if the file has an encrypted SaveMessage test, mirror it):

```go
func TestCassandraStore_SaveMessage_ForwardedMessage(t *testing.T) {
	// same keyspace/session/store scaffolding as TestCassandraStore_SaveMessage
	fwd := &cassandra.ForwardedMessage{
		MessageID: "01970a4f8c2d7c9aSRCM", RoomID: "r-src",
		Sender:    cassandra.Participant{ID: "u5", Account: "eve"},
		CreatedAt: time.Date(2026, 8, 1, 9, 0, 0, 0, time.UTC).UTC(),
		Msg:       "original body", MessageLink: "https://chat.example.com/r-src/01970a4f8c2d7c9aSRCM",
	}
	msg := &model.Message{ID: idgen.GenerateMessageID(), RoomID: "r-dst", Content: "comment",
		CreatedAt: time.Now().UTC().Truncate(time.Millisecond), ForwardedMessage: fwd}
	require.NoError(t, store.SaveMessage(ctx, msg, sender, "site-a"))

	var got cassandra.ForwardedMessage
	require.NoError(t, session.Query(
		`SELECT forwarded_message FROM messages_by_id WHERE message_id = ?`, msg.ID,
	).WithContext(ctx).Scan(&got))
	assert.Equal(t, "original body", got.Msg)
	assert.Equal(t, "eve", got.Sender.Account)
	// mirror row
	require.NoError(t, session.Query(
		`SELECT forwarded_message FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
		msg.RoomID, bucket.Of(msg.CreatedAt), msg.CreatedAt, msg.ID,
	).WithContext(ctx).Scan(&got))
	assert.Equal(t, "original body", got.Msg)
}
```

If the file has an encrypted-save integration test, clone it with a `ForwardedMessage`-carrying message and assert: `forwarded_message.msg` reads back EMPTY (stripped), `forwarded_message.message_id` reads back `"01970a4f8c2d7c9aSRCM"` (metadata preserved), and `enc_payload` is non-empty. Adapt scan-into-pointer (`var got *cassandra.ForwardedMessage`) if gocql requires it for null-safety, matching how existing tests scan `quoted_parent_message`.

Run: `make test-integration SERVICE=message-worker`. Expect the new tests to have FAILED before Step 3's binds existed (if you can, run them once before Step 3 to observe Red; container startup cost makes running them once after acceptable — the unit-level Red in Step 2 already covered the cycle).

- [ ] **Step 6: Commit** — `git add message-worker/ && git commit -m "feat(message-worker): persist forwarded_message on both main-timeline tables"`

---

### Task 7: history-service — read, pin, edit paths

**Files:**
- Modify: `history-service/internal/cassrepo/messages_by_room.go` (baseColumns), `history-service/internal/cassrepo/pin.go`, `history-service/internal/cassrepo/write.go`, `history-service/internal/service/pin.go`, `history-service/internal/service/reactions.go`
- Test: `history-service/internal/cassrepo/pin_test.go` or the integration files, `history-service/internal/cassrepo/write_integration_test.go`, `history-service/internal/cassrepo/messages_by_id_integration_test.go`

**Interfaces:**
- Consumes: `cassmodel.ForwardedMessage` (Task 1), `atrest.ForwardedEncrypted` (Task 3), DDL from Task 1's integration files.
- Produces: `forwarded_message` in every main-timeline SELECT; edit path preserves it; pins carry + redact it. The `editOne` signature changes to `editOne(ctx, plainQ, encQ string, ep editPayload, editedAt time.Time, withForwarded bool, whereArgs ...any)`.

- [ ] **Step 1: Write failing integration tests.**

(a) Append to `history-service/internal/cassrepo/messages_by_id_integration_test.go` — insert a row with a populated `forwarded_message` UDT (direct CQL INSERT in the test, matching the file's fixture style), then `GetMessageByID` and assert `msg.ForwardedMessage.Msg == "original body"` and `MessageID`/`Sender.Account` round-trip.

(b) Append to `history-service/internal/cassrepo/write_integration_test.go` — cipher-enabled edit of a message that has `forwarded_message` populated: after `UpdateMessageContent`, read the row back and assert (i) `forwarded_message.message_id` survived, (ii) `forwarded_message.msg` is blank in the UDT column, (iii) `GetMessageByID` (which decrypts) returns the restored forwarded body. Follow the file's existing quoted-parent edit test as the template — same structure, `Forwarded` substituted.

(c) Pin: in whichever file covers `PinMessage` round-trips (`pin_integration_test.go`), pin a message whose `models.Message.ForwardedMessage` is populated and assert the pinned row's `forwarded_message` reads back.

(d) Unit test for pin redaction — append to the service-layer pin unit test file (next to existing `redactUnavailablePins` coverage; find it with `grep -rn redactUnavailablePins history-service/internal/service/*_test.go`):

```go
// inside the existing redaction test's fixtures: give one pre-window pin a
// ForwardedMessage and assert it is nil after redactUnavailablePins.
pinned[0].ForwardedMessage = &models.ForwardedMessage{MessageID: "m-src", Msg: "body"}
redactUnavailablePins(pinned, &accessSince)
assert.Nil(t, pinned[0].ForwardedMessage)
```

If `models.ForwardedMessage` doesn't resolve, add the alias in `history-service/internal/models/message.go` next to the existing one: `type ForwardedMessage = cassandra.ForwardedMessage`.

- [ ] **Step 2: Run and confirm FAIL** — `make test SERVICE=history-service` (redaction test) and `make test-integration SERVICE=history-service` (SELECT lacks the column → forwarded fields come back nil).

- [ ] **Step 3: Implement.**

(a) `cassrepo/messages_by_room.go` — extend `baseColumns` (covers by-room AND by-id queries):

```go
const baseColumns = "room_id, created_at, message_id, thread_room_id, sender, " +
	"msg, mentions, attachments, card, card_action, tshow, tcount, thread_last_msg_at, " +
	"thread_parent_id, thread_parent_created_at, quoted_parent_message, forwarded_message, " +
	"visible_to, reactions, deleted, " +
	"type, sys_msg_data, site_id, edited_at, updated_at, pinned_at, " +
	"enc_payload, enc_meta"
```

Do NOT touch the column const in `thread_messages.go`.

(b) `cassrepo/pin.go` — in `insertPinnedMsg` add `forwarded_message,` after `quoted_parent_message,` (and one `?`); in the `PinMessage` bind list add `msg.ForwardedMessage,` after `msg.QuotedParentMessage,`. In `pinnedColumns` add `forwarded_message, ` after `quoted_parent_message, `.

(c) `service/pin.go` `redactUnavailablePins` — after `pinned[i].QuotedParentMessage = nil` add:

```go
		pinned[i].ForwardedMessage = nil
```

(d) `service/reactions.go` — in the `pkgmodel.Message` literal (~line 139), after `QuotedParentMessage: msg.QuotedParentMessage,` add:

```go
		ForwardedMessage:             msg.ForwardedMessage,
```

(e) `cassrepo/write.go` — five coordinated edits:

1. The three encrypted edit statements for tables that HAVE the column get `forwarded_message = ?` directly after `quoted_parent_message = ?`:

```go
	editMsgByIDEncrypted   = `UPDATE messages_by_id SET enc_payload = ?, enc_meta = ?, msg = null, attachments = null, card = null, card_action = null, quoted_parent_message = ?, forwarded_message = ?, edited_at = ?, updated_at = ? WHERE message_id = ?`
	editMsgByRoomEncrypted = `UPDATE messages_by_room SET enc_payload = ?, enc_meta = ?, msg = null, attachments = null, card = null, card_action = null, quoted_parent_message = ?, forwarded_message = ?, edited_at = ?, updated_at = ? WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`
	editPinnedMsgEncrypted = `UPDATE pinned_messages_by_room SET enc_payload = ?, enc_meta = ?, msg = null, attachments = null, card = null, card_action = null, quoted_parent_message = ?, forwarded_message = ?, edited_at = ?, updated_at = ? WHERE room_id = ? AND pinned_at = ? AND message_id = ?`
```

`editThreadMsgEncrypted` stays untouched (no column on that table).

2. `editPayload` gains a field after `quotedMeta`:

```go
	// forwardedMeta mirrors quotedMeta for the forwarded_message UDT: the
	// existing snapshot with its body blanked (the body moves into payload).
	// nil leaves the column null (not a forward). Unused on the plaintext path
	// and on thread_messages_by_thread (no such column there).
	forwardedMeta *cassmodel.ForwardedMessage
```

3. `readEncryptedFields` — signature becomes:

```go
func (r *Repository) readEncryptedFields(ctx context.Context, msg *models.Message) (atrest.EncryptedFields, *cassmodel.QuotedParentMessage, *cassmodel.ForwardedMessage, error) {
```

Add `forwarded *cassmodel.ForwardedMessage` to the var block, add `forwarded_message` to the SELECT column list and `&forwarded` to the Scan, return `forwarded` from every return path. In the legacy-plaintext promotion branch, after the quoted promotion:

```go
	if forwarded != nil && forwarded.Msg != "" {
		fields.ForwardedContent = &atrest.ForwardedEncrypted{Msg: forwarded.Msg}
	}
```

4. `buildEditPayload` — adjust the call and populate the new field:

```go
	fields, quoted, forwarded, err := r.readEncryptedFields(ctx, msg)
	…
	return editPayload{
		plain:         newMsg,
		payload:       payload,
		meta:          &cassmodel.EncMeta{Nonce: meta.Nonce},
		quotedMeta:    blankQuotedBody(quoted),
		forwardedMeta: blankForwardedBody(forwarded),
	}, nil
```

with the new helper next to `blankQuotedBody`:

```go
// blankForwardedBody returns a copy of the forwarded-snapshot UDT with its
// body cleared, mirroring atrest.StripEncryptedFields — the body moves into
// enc_payload while the metadata stays in the plaintext column. nil in → nil
// out (not a forward, column stays null).
func blankForwardedBody(forwarded *cassmodel.ForwardedMessage) *cassmodel.ForwardedMessage {
	if forwarded == nil {
		return nil
	}
	stripped := *forwarded
	stripped.Msg = ""
	return &stripped
}
```

5. `editOne` gains a `withForwarded bool` parameter (thread table lacks the column, so its statement binds one fewer arg):

```go
func (r *Repository) editOne(ctx context.Context, plainQ, encQ string, ep editPayload, editedAt time.Time, withForwarded bool, whereArgs ...any) error {
	if ep.payload == nil {
		args := append([]any{ep.plain, editedAt, editedAt}, whereArgs...)
		return r.session.Query(plainQ, args...).WithContext(ctx).Exec()
	}
	encArgs := []any{ep.payload, ep.meta, ep.quotedMeta}
	if withForwarded {
		encArgs = append(encArgs, ep.forwardedMeta)
	}
	args := append(append(encArgs, editedAt, editedAt), whereArgs...)
	return r.session.Query(encQ, args...).WithContext(ctx).Exec()
}
```

Update the four call sites: `editInMessagesByID`, `editInMessagesByRoom`, `editInPinnedMessagesByRoom` pass `true`; `editInThreadMessagesByThread` passes `false`. Update `editOne`'s doc comment accordingly.

(f) `decrypt.go` needs NO change — it already calls `atrest.ApplyDecryptedFields`, which Task 3 extended. `service/utils.go` (quote redaction) needs NO change — the forwarded snapshot is never redacted, by design.

- [ ] **Step 4: Run tests, verify PASS** — `make test SERVICE=history-service`, then `make test-integration SERVICE=history-service`.

- [ ] **Step 5: Commit** — `git add history-service/ && git commit -m "feat(history-service): forwarded_message on read/pin/edit paths (never redacted)"`

---

### Task 8: notification-worker — `forwarded` flag

**Files:**
- Modify: `pkg/model/push.go`, `notification-worker/handler.go`
- Test: `notification-worker/handler_test.go`

**Interfaces:**
- Consumes: `model.Message.ForwardedMessage` (Task 2).
- Produces: `PushNotificationData.Forwarded bool` (JSON `forwarded`).

- [ ] **Step 1: Write failing test** — append to `notification-worker/handler_test.go` (clone `TestHandle_PushPayloadSenderFromMemberRecord`'s scaffolding exactly — `stubMembers`, `recordingEmitter`, `newTestHandler`, `msgEvent`):

```go
func TestHandle_ForwardedFlagOnPushPayload(t *testing.T) {
	members := &stubMembers{out: map[string][]roomsubcache.Member{
		"r1": {
			{ID: "alice", Account: "alice", RoomType: model.RoomTypeChannel},
			{ID: "bob", Account: "bob", RoomType: model.RoomTypeChannel},
		},
	}}
	emit := &recordingEmitter{}
	h := newTestHandler(members, &stubFollowers{}, noopPresenceSnapshotter{}, noopVetoer{}, emit)

	require.NoError(t, h.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice",
		Content: "", CreatedAt: time.Now(),
		ForwardedMessage: &cassandra.ForwardedMessage{MessageID: "m-src", RoomID: "r-src", Msg: "body"},
	})))
	require.Len(t, emit.emitted, 1)
	assert.True(t, emit.emitted[0].Data.Forwarded, "forwarded flag must be set")
	assert.Empty(t, emit.emitted[0].Body, "empty comment ships an empty body; client renders from the flag")

	// Non-forward messages must not carry the flag.
	emit2 := &recordingEmitter{}
	h2 := newTestHandler(members, &stubFollowers{}, noopPresenceSnapshotter{}, noopVetoer{}, emit2)
	require.NoError(t, h2.HandleMessage(context.Background(), msgEvent(&model.Message{
		ID: "m2", RoomID: "r1", UserID: "alice", UserAccount: "alice", Content: "hi", CreatedAt: time.Now(),
	})))
	require.Len(t, emit2.emitted, 1)
	assert.False(t, emit2.emitted[0].Data.Forwarded)
}
```

(Add the `cassandra` import if the test file lacks it. If the handler's candidate filter skips empty-content messages before emit, check the first sub-case's expectation against actual behavior — the design requires forwards with empty comments to still notify; if a content-empty guard exists, extend it to pass when `msg.ForwardedMessage != nil`.)

- [ ] **Step 2: Run and confirm FAIL** — `make test SERVICE=notification-worker` (undefined `Data.Forwarded`).

- [ ] **Step 3: Implement** — in `pkg/model/push.go`, add to `PushNotificationData` after `AlsoSendToChannel`:

```go
	// Forwarded marks the message as a forward so clients can render
	// "forwarded a message" even when Body (the comment) is empty.
	Forwarded bool `json:"forwarded,omitempty" bson:"forwarded,omitempty"`
```

In `notification-worker/handler.go`, in the `model.PushNotificationData` literal (~line 218), after `AlsoSendToChannel: msg.TShow,`:

```go
			Forwarded:         msg.ForwardedMessage != nil,
```

- [ ] **Step 4: Run tests, verify PASS** — `make test SERVICE=notification-worker` and `make test SERVICE=pkg/model` (push round-trip tests, if any assert on the struct shape).

- [ ] **Step 5: Commit** — `git add pkg/model/push.go notification-worker/ && git commit -m "feat(notification-worker): forwarded flag on push payload"`

---

### Task 9: Client API docs

**Files:**
- Modify: `docs/client-api.md`, `docs/client-api/request-reply.md`, `docs/client-api/events.md`

No code — but mandatory in the same PR (client-facing `msg.send` handler changed, and `pkg/model` wire structs changed). Keep each edit in the file's existing table style.

- [ ] **Step 1: `docs/client-api.md` — msg.send request table** (~line 5776): add two rows after `quotedParentMessageId`:

```markdown
| `forwardedMessageId` | string | no | Set together with `forwardedRoomId` to forward a message. Must be a valid 17/20-char base62 message ID. The gatekeeper fetches the source from the **source room** (the sender must be subscribed there and inside its access window) and embeds an immutable text-only snapshot. Any fetch failure **rejects the send** — there is no placeholder. Mutually exclusive with `threadParentMessageId`, `quotedParentMessageId`, and `attachments`. `content` (the forward comment) becomes optional. To forward into several rooms, send one `msg.send` per destination room. |
| `forwardedRoomId` | string | no | The source message's room ID. Required with `forwardedMessageId`. |
```

- [ ] **Step 2: Request example** — after the "Quoted message" example add:

```markdown
##### Forwarded message

```json
{
  "id": "01970a4f8c2d7c9aQFWD",
  "content": "sharing from #general",
  "requestId": "01970a4f-8c2d-7c9a-abcd-e0123456789d",
  "forwardedMessageId": "01970a4f8c2d7c9aQRST",
  "forwardedRoomId": "01970a4f8c2d7c9aR"
}
```

`content` is the optional forward comment — it may be empty. The server builds the `forwardedMessage` snapshot from the source at forward time; the snapshot is immutable (later edits/deletes of the source do not touch it) and is never access-window-redacted for readers. Forwarding a message that is itself a forward captures only that message's own comment (chain depth stays 1). Not forwardable: messages with attachments, card messages, system messages, deleted messages, and forwards whose own comment is empty.
```

- [ ] **Step 3: Success-response table** (~line 5839): add after the `quotedParentMessage` row:

```markdown
| `forwardedMessage` | [ForwardedMessage](#forwardedmessage) | Present only for a forward — the server-built snapshot of the source message. |
```

- [ ] **Step 4: Error table** (~line 5871): add rows:

```markdown
| `forwardedMessageId and forwardedRoomId must be set together` | `bad_request` | — | One forward field without the other. |
| `invalid forwarded message ID "…": …` | `bad_request` | — | `forwardedMessageId` is not a valid message ID. |
| `a forward cannot also quote a message` / `a forward cannot target a thread` / `a forward cannot carry attachments` | `bad_request` | — | Forward combined with `quotedParentMessageId`, `threadParentMessageId`, or `attachments`. |
| `message not found` | `not_found` | — | Forward source missing, or `forwardedRoomId` doesn't match the source's room. |
| `not subscribed to room` | `forbidden` | `not_subscribed` | Sender is not a member of the **source** room. |
| `message is outside access window` | `forbidden` | `outside_access_window` | The source predates the sender's history window in the source room. |
| `cannot forward a deleted message` / `cannot forward a system message` / `cannot forward a message with attachments` / `cannot forward a card message` / `source forward has no forwardable content` | `bad_request` | — | Source not forwardable (text-only snapshot; chain depth 1). |
| `forward source temporarily unavailable` | `unavailable` | — | History could not be reached — the send is rejected (hard-fail, no placeholder); retry with a fresh message ID. |
```

Before committing, verify the `outside_access_window` reason string against `pkg/errcode` (`grep -rn MessageOutsideAccessWindow pkg/errcode/`) and use the actual wire value.

- [ ] **Step 5: `ForwardedMessage` schema table** — after the `##### QuotedParentMessage` section (~line 2817) add:

```markdown
##### ForwardedMessage

Immutable snapshot of the forwarded source message, built server-side at forward time. Text-only — never carries attachments or cards. Never redacted by the reader's access window (the source room's window was enforced once, at forward time).

| Field | Type | Notes |
|---|---|---|
| `messageId` | string | The source message's ID. |
| `roomId` | string | The source room. |
| `sender` | [MessageParticipant](#messageparticipant) | The source message's author. |
| `createdAt` | string | RFC 3339. The source message's send time. |
| `msg` | string | Optional. Body snapshot (the source's text). |
| `mentions` | [MessageParticipant](#messageparticipant)[] | Optional. |
| `messageLink` | string | Optional. Deep link to the source message. |
| `threadParentId` | string | Optional. Set when the source is a thread reply. |
| `threadParentCreatedAt` | string | Optional. RFC 3339. |
```

- [ ] **Step 6: Message schema row** — in the §3 Message schema table (~line 2705), after the `quotedParentMessage` row:

```markdown
| `forwardedMessage` | [ForwardedMessage](#forwardedmessage) | Optional. Embedded snapshot of a forwarded source message. |
```

- [ ] **Step 7: Derived views.** `docs/client-api/request-reply.md`: after the `quotedParentMessageId` request row (~line 2031) add:

```markdown
| `forwardedMessageId` | string | no | Forward: the source message's ID. Set with `forwardedRoomId`. The server fetches the source from the source room (subscription + access window enforced there) and embeds an immutable text-only snapshot; any fetch failure rejects the send (hard-fail, no placeholder). Mutually exclusive with `threadParentMessageId` / `quotedParentMessageId` / `attachments`; `content` becomes optional. One `msg.send` per destination room. |
| `forwardedRoomId` | string | no | The source message's room ID. Required with `forwardedMessageId`. |
```

and after its `quotedParentMessage` response row (~line 2049):

```markdown
| `forwardedMessage` | [ForwardedMessage](../client-api.md#forwardedmessage) | Present only for a forward. |
```

`docs/client-api/events.md` (~line 336): add after the `quotedParentMessage` row:

```markdown
| `forwardedMessage` | [ForwardedMessage](../client-api.md#forwardedmessage) | Optional. |
```

- [ ] **Step 8: Commit** — `git add docs/ && git commit -m "docs(client-api): msg.send forward fields, ForwardedMessage schema, error cases"`

---

### Task 10: Full verification sweep

- [ ] **Step 1:** `make generate` (idempotency check — no diff expected beyond Task 4's mock).
- [ ] **Step 2:** `make lint` — fix anything it flags.
- [ ] **Step 3:** `make test` (full unit suite, race detector).
- [ ] **Step 4:** `make test-integration SERVICE=message-worker` and `make test-integration SERVICE=history-service` (Docker required).
- [ ] **Step 5:** Coverage spot-check on the two changed hot paths:

```bash
go test -coverprofile=/tmp/claude-0/-home-user-newchat/ebd4f9f5-2cbb-562c-9535-edebcf532410/scratchpad/cov.out ./message-gatekeeper/ && go tool cover -func=/tmp/claude-0/-home-user-newchat/ebd4f9f5-2cbb-562c-9535-edebcf532410/scratchpad/cov.out | grep -E "resolveForwardSnapshot|FetchForwardedSource|total"
```

(Direct `go test` is acceptable here only because the Makefile has no coverage target; if it does — `grep -n cover Makefile` — use that instead.) Both new funcs must be ≥ 80%; add table rows if not.
- [ ] **Step 6:** `make sast` — expect clean; no `#nosec` should be needed anywhere in this feature.
- [ ] **Step 7:** Final commit of any stragglers, then push:

```bash
git push -u origin claude/message-forwarding-impl-deq7za
```

---

## Self-review notes (already applied)

- Spec coverage: wire model (T2), UDT+DDL (T1), atrest (T3), gatekeeper fetch+validate (T4–5), worker persist (T6), history read/pin/edit (T7), notification flag (T8), docs (T9). Broadcast/search/bot-worker are zero-change by design (spec "What is NOT changed") — no tasks needed. Deploy order and non-goals live in the spec.
- The spec's "history-service `docker-local/docker-compose.yml` DDL" mirror does not exist in the repo; the real inline-DDL mirrors are the four Go files listed in Task 1 Step 8.
- Rejecting a **deleted** source is in Task 5 (the spec's text-only/hard-fail language implies it; `msg.get` returns deleted rows, so the check must be explicit).
- Type-consistency: `forwardSourceProjection`, `FetchForwardedSource`, `resolveForwardSnapshot`, `blankForwardedBody`, `forwardedMeta`, `ForwardedEncrypted`, `ForwardedContent`, `Forwarded` — each name defined once and consumed with the same spelling in later tasks.
