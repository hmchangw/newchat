# room-state-worker Extraction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move broadcast-worker's three MongoDB writes into a new `room-state-worker` service with its own durable consumer, so a MongoDB outage stops causing duplicate client broadcasts and the writes retry until they land.

**Architecture:** `room-state-worker` consumes MESSAGES-CANONICAL on its own durable, derives write intents from each event (a pure function — no store reads), coalesces them into an in-memory batch, and drains the batch to MongoDB on a 250 ms ticker via three ordered unordered-BulkWrites. Consumed messages are held un-acked until their batch lands: Ack on success, Ack on a server-rejected write, NakWithDelay otherwise. During a MongoDB outage `MaxAckPending` fills and JetStream stops delivering to this consumer alone, leaving broadcast fan-out untouched.

**Tech Stack:** Go 1.25, NATS JetStream (`nats.go/jetstream`), MongoDB (`mongo-driver/v2`), `bytedance/sonic`, `caarlos0/env`, `go.uber.org/mock`, `testify`, `testcontainers-go` via `pkg/testutil`.

**Design spec:** `docs/superpowers/specs/2026-08-13-room-state-worker-extraction-design.md`

## Global Constraints

- Branch: `claude/broadcast-worker-mongodb-extract-25ji6b`. Never push elsewhere.
- Use `make` targets only — never raw `go` commands. `make lint`, `make test SERVICE=<name>`, `make test-integration SERVICE=<name>`, `make generate SERVICE=<name>`, `make build SERVICE=<name>`, `make sast`.
- TDD is mandatory: write the failing test, run it, confirm it fails, then implement. Never write implementation before its test.
- No new third-party dependencies. Everything needed is already in `go.mod`.
- Service is `package main`, flat at repo root, no `cmd/` or `internal/`.
- Errors always wrapped with context: `fmt.Errorf("short description: %w", err)`. Never bare `err`, never `fmt.Errorf("error: %w", err)`.
- Logging is `log/slog` JSON only, structured key-value fields. Never log message content, tokens, or passwords.
- All model structs carry both `json` and `bson` tags in `camelCase` (except `_id`).
- Coverage: 80% floor for the package, 90%+ target on `handler.go`, `batch.go`, `flush.go`.
- Integration tests are tagged `//go:build integration`, live in `package main`, use `pkg/testutil` containers, and the package has a `TestMain` calling `testutil.RunTests(m)`.
- Commit messages end with:
  ```
  Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01YK3aAf3kKfkzmtSYtmjicQ
  ```
- Do NOT create a pull request. Push the branch only.

---

## File Structure

**Create — `room-state-worker/`:**

| File | Responsibility |
|---|---|
| `handler.go` | `deriveIntents`: canonical event → `writeIntents`. Pure, no I/O. |
| `batch.go` | `batch`: coalesce intents across messages, hold consumed messages. |
| `flush.go` | `flusher`: ticker loop, ordered bulk writes, error classification, settlement. |
| `store.go` | `Store` interface + `//go:generate mockgen` directive. |
| `store_mongo.go` | `mongoStore`: three BulkWrite implementations + replay-safe filters. |
| `bootstrap.go` | `bootstrapConfig` + `bootstrapStreams` (verify in prod, create in dev). |
| `pretouch.go` | sonic codec warm-up for `model.MessageEvent`. |
| `main.go` | Config, wiring, consumer creation, consume loop, health, graceful shutdown. |
| `handler_test.go`, `batch_test.go`, `flush_test.go`, `main_test.go` | Unit tests. |
| `integration_test.go` | Mongo + NATS integration, incl. replay ordering. |
| `mock_store_test.go` | Generated. Never edited by hand. |
| `deploy/user/`, `deploy/bot/` | Dockerfile, docker-compose.yml, azure-pipelines.yml each. |

**Modify — `broadcast-worker/`:** `store.go`, `store_mongo.go`, `handler.go`, `main.go`, `handler_test.go`, `store_mongo_test.go`, `integration_test.go`. **Delete:** `coalescer.go`, `coalescer_test.go`.

**Modify — repo:** `docker-local/compose.services.yaml`, `CLAUDE.md`, `docs/architecture.md`.

---

### Task 1: Intent derivation

The pure core: one canonical event in, the set of MongoDB writes it implies out. No store, no NATS, no user lookup — `mention.Parse` is a function of content alone and the room id rides on the message.

**Files:**
- Create: `room-state-worker/handler.go`
- Test: `room-state-worker/handler_test.go`

**Interfaces:**
- Consumes: `model.MessageEvent`, `model.Message`, `mention.Parse` from `pkg/mention`.
- Produces: `type writeIntents struct` with fields `RoomID, LastMsgID string; LastMsgAt, LastMentionAllAt time.Time; SenderAccount string; SenderSeenAt time.Time; MentionAccounts []string; MentionAt time.Time`; method `(writeIntents) hasWork() bool`; functions `deriveIntents(evt *model.MessageEvent) writeIntents` and `isHiddenThreadReply(msg *model.Message) bool`.

- [ ] **Step 1: Write the failing test**

Create `room-state-worker/handler_test.go`:

```go
package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/model"
)

func ptrTime(t time.Time) *time.Time { return &t }

func TestDeriveIntents(t *testing.T) {
	created := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)
	edited := time.Date(2026, 8, 13, 11, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		evt  model.MessageEvent
		want writeIntents
	}{
		{
			name: "created plain message updates room pointer and sender lastSeen",
			evt: model.MessageEvent{
				Event: model.EventCreated,
				Message: model.Message{
					ID: "m1", RoomID: "r1", UserAccount: "alice",
					Content: "hello", CreatedAt: created,
				},
			},
			want: writeIntents{
				RoomID: "r1", LastMsgID: "m1", LastMsgAt: created,
				SenderAccount: "alice", SenderSeenAt: created,
			},
		},
		{
			name: "created with mentions badges the mentioned accounts",
			evt: model.MessageEvent{
				Event: model.EventCreated,
				Message: model.Message{
					ID: "m2", RoomID: "r1", UserAccount: "alice",
					Content: "hi @bob and @carol", CreatedAt: created,
				},
			},
			want: writeIntents{
				RoomID: "r1", LastMsgID: "m2", LastMsgAt: created,
				SenderAccount: "alice", SenderSeenAt: created,
				MentionAccounts: []string{"bob", "carol"}, MentionAt: created,
			},
		},
		{
			name: "created with @all sets lastMentionAllAt and no mention accounts",
			evt: model.MessageEvent{
				Event: model.EventCreated,
				Message: model.Message{
					ID: "m3", RoomID: "r1", UserAccount: "alice",
					Content: "@all standup", CreatedAt: created,
				},
			},
			want: writeIntents{
				RoomID: "r1", LastMsgID: "m3", LastMsgAt: created,
				LastMentionAllAt: created,
				SenderAccount:    "alice", SenderSeenAt: created,
			},
		},
		{
			name: "hidden thread reply produces no writes",
			evt: model.MessageEvent{
				Event: model.EventCreated,
				Message: model.Message{
					ID: "m4", RoomID: "r1", UserAccount: "alice",
					Content: "@bob reply", CreatedAt: created,
					ThreadParentMessageID: "p1", TShow: false,
				},
			},
			want: writeIntents{},
		},
		{
			name: "visible thread reply is treated as a room message",
			evt: model.MessageEvent{
				Event: model.EventCreated,
				Message: model.Message{
					ID: "m5", RoomID: "r1", UserAccount: "alice",
					Content: "reply", CreatedAt: created,
					ThreadParentMessageID: "p1", TShow: true,
				},
			},
			want: writeIntents{
				RoomID: "r1", LastMsgID: "m5", LastMsgAt: created,
				SenderAccount: "alice", SenderSeenAt: created,
			},
		},
		{
			name: "updated badges mentions at editedAt and touches nothing else",
			evt: model.MessageEvent{
				Event: model.EventUpdated,
				Message: model.Message{
					ID: "m6", RoomID: "r1", UserAccount: "alice",
					Content: "now with @bob", CreatedAt: created, EditedAt: ptrTime(edited),
				},
			},
			want: writeIntents{
				RoomID: "r1", MentionAccounts: []string{"bob"}, MentionAt: edited,
			},
		},
		{
			name: "updated without mentions produces no writes",
			evt: model.MessageEvent{
				Event: model.EventUpdated,
				Message: model.Message{
					ID: "m7", RoomID: "r1", Content: "no mentions", EditedAt: ptrTime(edited),
				},
			},
			want: writeIntents{},
		},
		{
			name: "updated without editedAt produces no writes",
			evt: model.MessageEvent{
				Event: model.EventUpdated,
				Message: model.Message{
					ID: "m8", RoomID: "r1", Content: "@bob", EditedAt: nil,
				},
			},
			want: writeIntents{},
		},
		{
			name: "hidden thread reply edit produces no writes",
			evt: model.MessageEvent{
				Event: model.EventUpdated,
				Message: model.Message{
					ID: "m9", RoomID: "r1", Content: "@bob", EditedAt: ptrTime(edited),
					ThreadParentMessageID: "p1", TShow: false,
				},
			},
			want: writeIntents{},
		},
		{
			name: "deleted produces no writes",
			evt: model.MessageEvent{
				Event:   model.EventDeleted,
				Message: model.Message{ID: "m10", RoomID: "r1", Content: "@bob"},
			},
			want: writeIntents{},
		},
		{
			name: "reacted produces no writes",
			evt: model.MessageEvent{
				Event:   model.EventReacted,
				Message: model.Message{ID: "m11", RoomID: "r1"},
			},
			want: writeIntents{},
		},
		{
			name: "pinned produces no writes",
			evt: model.MessageEvent{
				Event:   model.EventPinned,
				Message: model.Message{ID: "m12", RoomID: "r1"},
			},
			want: writeIntents{},
		},
		{
			name: "missing roomId produces no writes",
			evt: model.MessageEvent{
				Event:   model.EventCreated,
				Message: model.Message{ID: "m13", RoomID: "", CreatedAt: created},
			},
			want: writeIntents{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := deriveIntents(&tc.evt)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestWriteIntents_HasWork(t *testing.T) {
	assert.False(t, writeIntents{}.hasWork())
	assert.True(t, writeIntents{LastMsgID: "m1"}.hasWork())
	assert.True(t, writeIntents{SenderAccount: "alice"}.hasWork())
	assert.True(t, writeIntents{MentionAccounts: []string{"bob"}}.hasWork())
}
```

- [ ] **Step 2: Run the test and verify it fails**

Run: `make test SERVICE=room-state-worker`
Expected: FAIL — the package does not compile, `undefined: writeIntents`, `undefined: deriveIntents`.

- [ ] **Step 3: Write the implementation**

Create `room-state-worker/handler.go`:

```go
// Package main derives and applies the room-level MongoDB state writes implied
// by MESSAGES-CANONICAL events: rooms.lastMsgAt/lastMsgId, the sender's own
// subscription lastSeenAt, and the hasMention badge. Thread-level state is
// owned by message-worker; fan-out is owned by broadcast-worker.
package main

import (
	"time"

	"github.com/hmchangw/chat/pkg/mention"
	"github.com/hmchangw/chat/pkg/model"
)

// writeIntents is the complete set of MongoDB writes implied by one canonical
// event. The zero value means "no writes"; hasWork reports that. Each group is
// selected by its own presence marker so an event can trigger any subset.
type writeIntents struct {
	RoomID string

	// LastMsgID != "" selects the rooms-collection last-message update.
	LastMsgID        string
	LastMsgAt        time.Time
	LastMentionAllAt time.Time // zero unless the message @all-mentions

	// SenderAccount != "" selects the sender's lastSeenAt advance.
	SenderAccount string
	SenderSeenAt  time.Time

	// MentionAccounts non-empty selects the hasMention badge write.
	MentionAccounts []string
	MentionAt       time.Time
}

func (w writeIntents) hasWork() bool {
	return w.LastMsgID != "" || w.SenderAccount != "" || len(w.MentionAccounts) > 0
}

// isHiddenThreadReply mirrors broadcast-worker's shouldUseThreadFanOut. A
// TShow=false thread reply never touches room-level state: it is invisible in
// the main channel, and message-worker owns thread_rooms/thread_subscriptions
// for it.
func isHiddenThreadReply(msg *model.Message) bool {
	return msg.ThreadParentMessageID != "" && !msg.TShow
}

// deriveIntents maps a canonical event to its room-level writes. Pure by
// construction: mention.Parse is a function of content alone, and the room id
// is carried on the message — so no MongoDB read is needed to decide anything.
func deriveIntents(evt *model.MessageEvent) writeIntents {
	msg := &evt.Message
	if msg.RoomID == "" || isHiddenThreadReply(msg) {
		return writeIntents{}
	}

	switch evt.Event {
	case model.EventCreated:
		parsed := mention.Parse(msg.Content)
		in := writeIntents{
			RoomID:        msg.RoomID,
			LastMsgID:     msg.ID,
			LastMsgAt:     msg.CreatedAt,
			SenderAccount: msg.UserAccount,
			SenderSeenAt:  msg.CreatedAt,
		}
		if parsed.MentionAll {
			in.LastMentionAllAt = msg.CreatedAt
		}
		if len(parsed.Accounts) > 0 {
			in.MentionAccounts = parsed.Accounts
			in.MentionAt = msg.CreatedAt
		}
		return in

	case model.EventUpdated:
		// Additive badge only, mirroring broadcast-worker's
		// badgeNewlyMentionedAccounts: an edit never moves the room pointer and
		// never clears a mention that the edit removed.
		if msg.EditedAt == nil {
			return writeIntents{}
		}
		parsed := mention.Parse(msg.Content)
		if len(parsed.Accounts) == 0 {
			return writeIntents{}
		}
		return writeIntents{
			RoomID:          msg.RoomID,
			MentionAccounts: parsed.Accounts,
			MentionAt:       *msg.EditedAt,
		}

	default:
		return writeIntents{}
	}
}
```

- [ ] **Step 4: Run the test and verify it passes**

Run: `make test SERVICE=room-state-worker`
Expected: PASS, all subtests green.

- [ ] **Step 5: Commit**

```bash
git add room-state-worker/handler.go room-state-worker/handler_test.go
git commit -m "feat(room-state-worker): derive room-level write intents from canonical events

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01YK3aAf3kKfkzmtSYtmjicQ"
```

---

### Task 2: Batch coalescing

Collapse many messages into one write per room and per subscription, and hold every consumed message so it can be settled after the batch lands.

**Files:**
- Create: `room-state-worker/batch.go`
- Test: `room-state-worker/batch_test.go`

**Interfaces:**
- Consumes: `writeIntents` (Task 1), `jsretry.Msg` from `pkg/jsretry` (interface with `Metadata() (*jetstream.MsgMetadata, error)`, `Ack() error`, `NakWithDelay(time.Duration) error`).
- Produces: `type subKey struct{ roomID, account string }`; `type roomLastMsgUpdate struct{ msgID string; at, lastMentionAllAt time.Time }`; `type heldMsg struct{ ctx context.Context; msg jsretry.Msg }`; `type batch struct{ rooms map[string]roomLastMsgUpdate; lastSeen, mentions map[subKey]time.Time; held []heldMsg }`; `newBatch() *batch`; `(*batch) add(in writeIntents, msg heldMsg)`; `(*batch) empty() bool`.

- [ ] **Step 1: Write the failing test**

Create `room-state-worker/batch_test.go`:

```go
package main

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeMsg is a jsretry.Msg that records how it was settled. jsretry.Msg needs
// all three methods — Metadata feeds the backoff schedule selection.
type fakeMsg struct {
	acked     bool
	naked     bool
	nakDelay  time.Duration
	delivered uint64 // 0 is treated as the first delivery by jsretry.backoffFor
}

func (f *fakeMsg) Ack() error { f.acked = true; return nil }

func (f *fakeMsg) NakWithDelay(d time.Duration) error {
	f.naked = true
	f.nakDelay = d
	return nil
}

func (f *fakeMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{NumDelivered: f.delivered}, nil
}

func held(m *fakeMsg) heldMsg { return heldMsg{ctx: context.Background(), msg: m} }

func TestBatch_CoalescesRoomPointerToLatestMessage(t *testing.T) {
	t1 := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Second)

	b := newBatch()
	b.add(writeIntents{RoomID: "r1", LastMsgID: "m2", LastMsgAt: t2}, held(&fakeMsg{}))
	// Older message arrives after the newer one (out-of-order redelivery).
	b.add(writeIntents{RoomID: "r1", LastMsgID: "m1", LastMsgAt: t1}, held(&fakeMsg{}))

	require.Len(t, b.rooms, 1)
	assert.Equal(t, "m2", b.rooms["r1"].msgID)
	assert.Equal(t, t2, b.rooms["r1"].at)
	assert.Len(t, b.held, 2, "every consumed message must be held for settlement")
}

func TestBatch_LastMentionAllAtSticksAcrossLaterPlainMessages(t *testing.T) {
	t1 := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Second)

	b := newBatch()
	b.add(writeIntents{RoomID: "r1", LastMsgID: "m1", LastMsgAt: t1, LastMentionAllAt: t1}, held(&fakeMsg{}))
	b.add(writeIntents{RoomID: "r1", LastMsgID: "m2", LastMsgAt: t2}, held(&fakeMsg{}))

	assert.Equal(t, "m2", b.rooms["r1"].msgID)
	assert.Equal(t, t1, b.rooms["r1"].lastMentionAllAt, "a later plain message must not clear lastMentionAllAt")
}

func TestBatch_MentionAndLastSeenKeepLatestTimePerSubscription(t *testing.T) {
	t1 := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Second)

	b := newBatch()
	b.add(writeIntents{
		RoomID: "r1", LastMsgID: "m1", LastMsgAt: t1,
		SenderAccount: "alice", SenderSeenAt: t1,
		MentionAccounts: []string{"bob"}, MentionAt: t1,
	}, held(&fakeMsg{}))
	b.add(writeIntents{
		RoomID: "r1", LastMsgID: "m2", LastMsgAt: t2,
		SenderAccount: "alice", SenderSeenAt: t2,
		MentionAccounts: []string{"bob", "carol"}, MentionAt: t2,
	}, held(&fakeMsg{}))

	assert.Equal(t, t2, b.lastSeen[subKey{"r1", "alice"}])
	assert.Equal(t, t2, b.mentions[subKey{"r1", "bob"}], "the later mention time wins")
	assert.Equal(t, t2, b.mentions[subKey{"r1", "carol"}])
	assert.Len(t, b.mentions, 2)
}

func TestBatch_HoldsNoOpMessagesForAck(t *testing.T) {
	b := newBatch()
	b.add(writeIntents{}, held(&fakeMsg{}))

	assert.False(t, b.empty(), "a no-op message still needs settling")
	assert.Empty(t, b.rooms)
	assert.Empty(t, b.lastSeen)
	assert.Empty(t, b.mentions)
	assert.Len(t, b.held, 1)
}

func TestBatch_EmptyWhenNothingAdded(t *testing.T) {
	assert.True(t, newBatch().empty())
}
```

- [ ] **Step 2: Run the test and verify it fails**

Run: `make test SERVICE=room-state-worker`
Expected: FAIL — `undefined: newBatch`, `undefined: subKey`, `undefined: heldMsg`.

- [ ] **Step 3: Write the implementation**

Create `room-state-worker/batch.go`:

```go
package main

import (
	"context"
	"time"

	"github.com/hmchangw/chat/pkg/jsretry"
)

// subKey identifies exactly one subscription document: (roomId, u.account).
type subKey struct {
	roomID  string
	account string
}

// roomLastMsgUpdate is the coalesced per-room last-message state.
//
//   - msgID/at carry the LATEST message observed for the room (max by createdAt).
//   - lastMentionAllAt carries the latest createdAt among @all messages and
//     sticks across later non-@all messages until a newer @all arrives.
type roomLastMsgUpdate struct {
	msgID            string
	at               time.Time
	lastMentionAllAt time.Time
}

// heldMsg is a consumed message awaiting settlement until its batch flushes.
// The context is kept so the settle log carries the message's request id.
type heldMsg struct {
	ctx context.Context
	msg jsretry.Msg
}

// batch accumulates write intents between flushes. The write maps are bounded
// by distinct active rooms and mentioned accounts per interval, not by message
// rate — held is not, which is why MaxAckPending must bound the consumer.
type batch struct {
	rooms    map[string]roomLastMsgUpdate
	lastSeen map[subKey]time.Time
	mentions map[subKey]time.Time
	held     []heldMsg
}

func newBatch() *batch {
	return &batch{
		rooms:    make(map[string]roomLastMsgUpdate),
		lastSeen: make(map[subKey]time.Time),
		mentions: make(map[subKey]time.Time),
	}
}

// add merges one message's intents and takes ownership of settling msg. The
// message is held unconditionally: an event that implies no writes (delete,
// react, hidden thread reply) still has to be Acked.
func (b *batch) add(in writeIntents, msg heldMsg) {
	b.held = append(b.held, msg)

	if in.LastMsgID != "" {
		cur := b.rooms[in.RoomID]
		if in.LastMsgAt.After(cur.at) {
			cur.msgID = in.LastMsgID
			cur.at = in.LastMsgAt
		}
		if in.LastMentionAllAt.After(cur.lastMentionAllAt) {
			cur.lastMentionAllAt = in.LastMentionAllAt
		}
		b.rooms[in.RoomID] = cur
	}

	if in.SenderAccount != "" {
		k := subKey{roomID: in.RoomID, account: in.SenderAccount}
		if in.SenderSeenAt.After(b.lastSeen[k]) {
			b.lastSeen[k] = in.SenderSeenAt
		}
	}

	for _, account := range in.MentionAccounts {
		k := subKey{roomID: in.RoomID, account: account}
		if in.MentionAt.After(b.mentions[k]) {
			b.mentions[k] = in.MentionAt
		}
	}
}

// empty reports whether the batch has nothing to settle or write.
func (b *batch) empty() bool { return len(b.held) == 0 }
```

- [ ] **Step 4: Run the test and verify it passes**

Run: `make test SERVICE=room-state-worker`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add room-state-worker/batch.go room-state-worker/batch_test.go
git commit -m "feat(room-state-worker): coalesce write intents into a flushable batch

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01YK3aAf3kKfkzmtSYtmjicQ"
```

---

### Task 3: Store interface and MongoDB bulk writes

Three unordered BulkWrites, all safe to replay out of order. The `lastMsgAt` guard filter is the new part: broadcast-worker's coalescer has no such guard, so a redelivered older message could regress the room pointer.

**Files:**
- Create: `room-state-worker/store.go`, `room-state-worker/store_mongo.go`
- Test: `room-state-worker/store_mongo_test.go`

**Interfaces:**
- Consumes: `roomLastMsgUpdate`, `subKey` (Task 2).
- Produces: `type Store interface` with `BulkUpdateRoomLastMessage(ctx context.Context, updates map[string]roomLastMsgUpdate) error`, `BulkAdvanceLastSeen(ctx context.Context, updates map[subKey]time.Time) error`, `BulkSetMentions(ctx context.Context, updates map[subKey]time.Time) error`; `NewMongoStore(roomCol, subCol *mongo.Collection) *mongoStore`; filter builders `roomLastMsgFilter(roomID string, at time.Time) bson.M` and `mentionFilter(k subKey, at time.Time) bson.M`.

- [ ] **Step 1: Write the failing test**

The filter builders carry the correctness-critical logic and are testable without a database; the writes themselves are covered by the integration test in Task 7.

Create `room-state-worker/store_mongo_test.go`:

```go
package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestRoomLastMsgFilter_RejectsStaleReplay(t *testing.T) {
	at := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)

	got := roomLastMsgFilter("r1", at)

	// $not/$gte (not $lt) so the filter still matches a room whose lastMsgAt is
	// missing or null — a plain $lt would skip those and never set the pointer.
	assert.Equal(t, bson.M{
		"_id":       "r1",
		"lastMsgAt": bson.M{"$not": bson.M{"$gte": at}},
	}, got)
}

func TestMentionFilter_SkipsAccountsThatAlreadyRead(t *testing.T) {
	at := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)

	got := mentionFilter(subKey{roomID: "r1", account: "bob"}, at)

	assert.Equal(t, bson.M{
		"roomId":     "r1",
		"u.account":  "bob",
		"lastSeenAt": bson.M{"$not": bson.M{"$gte": at}},
	}, got)
}
```

- [ ] **Step 2: Run the test and verify it fails**

Run: `make test SERVICE=room-state-worker`
Expected: FAIL — `undefined: roomLastMsgFilter`, `undefined: mentionFilter`.

- [ ] **Step 3: Write the implementation**

Create `room-state-worker/store.go`:

```go
package main

import (
	"context"
	"time"
)

//go:generate mockgen -destination=mock_store_test.go -package=main . Store

// Store is the room-state write surface. Every method issues a single unordered
// BulkWrite and is safe to replay out of order — the flush path retries whole
// batches, so any write may be applied more than once and after a newer one.
type Store interface {
	// BulkUpdateRoomLastMessage sets rooms.lastMsgAt/lastMsgId/updatedAt (and
	// lastMentionAllAt when non-zero), skipping any room already at or beyond
	// the supplied time so a stale replay cannot regress the pointer.
	BulkUpdateRoomLastMessage(ctx context.Context, updates map[string]roomLastMsgUpdate) error
	// BulkAdvanceLastSeen advances each subscription's lastSeenAt via $max, so
	// it never regresses a user who has already read further.
	BulkAdvanceLastSeen(ctx context.Context, updates map[subKey]time.Time) error
	// BulkSetMentions flags each subscription as mentioned unless that account
	// already read past the mentioning message — otherwise an async mention
	// write can clobber a read-clear that happened first (#467).
	BulkSetMentions(ctx context.Context, updates map[subKey]time.Time) error
}
```

Missing rooms and subscriptions are deliberately not surfaced by any of the three methods: these are derived fields, and the message itself is already durable in Cassandra before this service sees the event.

Create `room-state-worker/store_mongo.go`:

```go
package main

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type mongoStore struct {
	roomCol *mongo.Collection
	subCol  *mongo.Collection
}

func NewMongoStore(roomCol, subCol *mongo.Collection) *mongoStore {
	return &mongoStore{roomCol: roomCol, subCol: subCol}
}

// roomLastMsgFilter matches a room only when its stored lastMsgAt is not already
// at or beyond at, so a redelivered older message cannot regress the pointer.
// $not/$gte (not $lt) so it still matches a missing or null lastMsgAt.
func roomLastMsgFilter(roomID string, at time.Time) bson.M {
	return bson.M{
		"_id":       roomID,
		"lastMsgAt": bson.M{"$not": bson.M{"$gte": at}},
	}
}

// mentionFilter matches a subscription that has NOT already read past at.
// Same $not/$gte reasoning as roomLastMsgFilter: a plain $lt would skip a
// never-read subscription whose lastSeenAt is missing.
func mentionFilter(k subKey, at time.Time) bson.M {
	return bson.M{
		"roomId":     k.roomID,
		"u.account":  k.account,
		"lastSeenAt": bson.M{"$not": bson.M{"$gte": at}},
	}
}

func (m *mongoStore) BulkUpdateRoomLastMessage(ctx context.Context, updates map[string]roomLastMsgUpdate) error {
	if len(updates) == 0 {
		return nil
	}
	models := make([]mongo.WriteModel, 0, len(updates))
	for roomID, u := range updates {
		fields := bson.M{
			"lastMsgAt": u.at,
			"lastMsgId": u.msgID,
			"updatedAt": u.at,
		}
		if !u.lastMentionAllAt.IsZero() {
			fields["lastMentionAllAt"] = u.lastMentionAllAt
		}
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(roomLastMsgFilter(roomID, u.at)).
			SetUpdate(bson.M{"$set": fields}))
	}
	if _, err := m.roomCol.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false)); err != nil {
		return fmt.Errorf("bulk update room last message (%d rooms): %w", len(updates), err)
	}
	return nil
}

func (m *mongoStore) BulkAdvanceLastSeen(ctx context.Context, updates map[subKey]time.Time) error {
	if len(updates) == 0 {
		return nil
	}
	models := make([]mongo.WriteModel, 0, len(updates))
	for k, at := range updates {
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(bson.M{"roomId": k.roomID, "u.account": k.account}).
			SetUpdate(bson.M{"$max": bson.M{"lastSeenAt": at}}))
	}
	if _, err := m.subCol.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false)); err != nil {
		return fmt.Errorf("bulk advance subscription lastSeenAt (%d subscriptions): %w", len(updates), err)
	}
	return nil
}

func (m *mongoStore) BulkSetMentions(ctx context.Context, updates map[subKey]time.Time) error {
	if len(updates) == 0 {
		return nil
	}
	models := make([]mongo.WriteModel, 0, len(updates))
	for k, at := range updates {
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(mentionFilter(k, at)).
			SetUpdate(bson.M{"$set": bson.M{"hasMention": true}}))
	}
	if _, err := m.subCol.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false)); err != nil {
		return fmt.Errorf("bulk set subscription mentions (%d subscriptions): %w", len(updates), err)
	}
	return nil
}
```

- [ ] **Step 4: Run the test and verify it passes**

Run: `make test SERVICE=room-state-worker`
Expected: PASS.

- [ ] **Step 5: Generate the mock and confirm the build**

Run: `make generate SERVICE=room-state-worker && make lint`
Expected: `room-state-worker/mock_store_test.go` created; lint clean.

- [ ] **Step 6: Commit**

```bash
git add room-state-worker/store.go room-state-worker/store_mongo.go room-state-worker/store_mongo_test.go room-state-worker/mock_store_test.go
git commit -m "feat(room-state-worker): add replay-safe MongoDB bulk write store

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01YK3aAf3kKfkzmtSYtmjicQ"
```

---

### Task 4: Flush loop, error classification, settlement

Where the durability guarantee lives. Writes run in a fixed order; the batch's held messages are Acked only after the writes land, Nak'd with backoff on a transient failure, and Ack-dropped on a server-rejected document so `MaxDeliver=-1` cannot wedge the consumer forever.

**Files:**
- Create: `room-state-worker/flush.go`
- Test: `room-state-worker/flush_test.go`

**Interfaces:**
- Consumes: `Store` (Task 3), `batch`/`heldMsg`/`writeIntents` (Tasks 1-2), `jsretry.SettleQuiet`, `jsretry.DefaultBackoff`, `errcode.Permanent`, `errcode.Internal`, `errcode.WithCause`.
- Produces: `type flusher struct`; `newFlusher(store Store) *flusher`; `(*flusher) add(in writeIntents, msg heldMsg)`; `(*flusher) Flush(ctx context.Context)`; `(*flusher) Run(ctx context.Context, interval, finalTimeout time.Duration)`; `classifyFlushErr(err error) error`.

- [ ] **Step 1: Verify the driver's bulk-write error shape**

Run: `go doc go.mongodb.org/mongo-driver/v2/mongo BulkWriteException && go doc go.mongodb.org/mongo-driver/v2/mongo BulkWriteError`
Expected: `BulkWriteException` has `WriteConcernError *WriteConcernError`, `WriteErrors []BulkWriteError`, and `Labels []string`; `BulkWriteError` embeds `WriteError`. If any name differs in the pinned driver version, adjust the field references in `classifyFlushErr` (Step 3) and the test literals (Step 2) to match — the classification rule itself does not change.

- [ ] **Step 2: Write the failing test**

Create `room-state-worker/flush_test.go`:

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/errcode"
)

// stubStore records the calls the flusher makes and can fail a chosen one.
type stubStore struct {
	order    []string
	rooms    map[string]roomLastMsgUpdate
	lastSeen map[subKey]time.Time
	mentions map[subKey]time.Time
	failOn   string
	failErr  error
}

func (s *stubStore) BulkUpdateRoomLastMessage(_ context.Context, u map[string]roomLastMsgUpdate) error {
	s.order = append(s.order, "rooms")
	s.rooms = u
	if s.failOn == "rooms" {
		return s.failErr
	}
	return nil
}

func (s *stubStore) BulkAdvanceLastSeen(_ context.Context, u map[subKey]time.Time) error {
	s.order = append(s.order, "lastSeen")
	s.lastSeen = u
	if s.failOn == "lastSeen" {
		return s.failErr
	}
	return nil
}

func (s *stubStore) BulkSetMentions(_ context.Context, u map[subKey]time.Time) error {
	s.order = append(s.order, "mentions")
	s.mentions = u
	if s.failOn == "mentions" {
		return s.failErr
	}
	return nil
}

func TestFlusher_AcksHeldMessagesAfterSuccessfulWrite(t *testing.T) {
	at := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)
	store := &stubStore{}
	f := newFlusher(store)
	m := &fakeMsg{}

	f.add(writeIntents{
		RoomID: "r1", LastMsgID: "m1", LastMsgAt: at,
		SenderAccount: "alice", SenderSeenAt: at,
		MentionAccounts: []string{"bob"}, MentionAt: at,
	}, held(m))
	f.Flush(context.Background())

	assert.True(t, m.acked)
	assert.False(t, m.naked)
	assert.Equal(t, []string{"rooms", "lastSeen", "mentions"}, store.order,
		"lastSeenAt must be written before mentions so a self-mention does not badge the sender")
}

func TestFlusher_NaksHeldMessagesOnTransientWriteFailure(t *testing.T) {
	store := &stubStore{failOn: "rooms", failErr: errors.New("connection refused")}
	f := newFlusher(store)
	m := &fakeMsg{}

	f.add(writeIntents{RoomID: "r1", LastMsgID: "m1", LastMsgAt: time.Now().UTC()}, held(m))
	f.Flush(context.Background())

	assert.True(t, m.naked, "a transient failure must retry, not drop")
	assert.False(t, m.acked)
	assert.Greater(t, m.nakDelay, time.Duration(0))
}

func TestFlusher_AcksHeldMessagesOnServerRejectedWrite(t *testing.T) {
	rejected := mongo.BulkWriteException{
		WriteErrors: []mongo.BulkWriteError{{WriteError: mongo.WriteError{Code: 9, Message: "FailedToParse"}}},
	}
	store := &stubStore{failOn: "mentions", failErr: rejected}
	f := newFlusher(store)
	m := &fakeMsg{}

	f.add(writeIntents{RoomID: "r1", MentionAccounts: []string{"bob"}, MentionAt: time.Now().UTC()}, held(m))
	f.Flush(context.Background())

	assert.True(t, m.acked, "a rejected document never succeeds on retry; it must be dropped, not looped")
	assert.False(t, m.naked)
}

func TestFlusher_StopsAtFirstFailedWrite(t *testing.T) {
	store := &stubStore{failOn: "rooms", failErr: errors.New("connection refused")}
	f := newFlusher(store)

	f.add(writeIntents{
		RoomID: "r1", LastMsgID: "m1", LastMsgAt: time.Now().UTC(),
		SenderAccount: "alice", SenderSeenAt: time.Now().UTC(),
	}, held(&fakeMsg{}))
	f.Flush(context.Background())

	assert.Equal(t, []string{"rooms"}, store.order,
		"the whole batch redelivers, so later writes must not run against a half-failed flush")
}

func TestFlusher_AcksNoOpMessagesWithoutWriting(t *testing.T) {
	store := &stubStore{}
	f := newFlusher(store)
	m := &fakeMsg{}

	f.add(writeIntents{}, held(m))
	f.Flush(context.Background())

	assert.True(t, m.acked)
	require.Empty(t, store.rooms)
	require.Empty(t, store.lastSeen)
	require.Empty(t, store.mentions)
}

func TestFlusher_FlushOnEmptyBatchIsNoOp(t *testing.T) {
	store := &stubStore{}
	newFlusher(store).Flush(context.Background())
	assert.Empty(t, store.order)
}

func TestFlusher_RunFlushesOnCancellation(t *testing.T) {
	store := &stubStore{}
	f := newFlusher(store)
	m := &fakeMsg{}
	f.add(writeIntents{RoomID: "r1", LastMsgID: "m1", LastMsgAt: time.Now().UTC()}, held(m))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { f.Run(ctx, time.Hour, 5*time.Second); close(done) }()
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
	assert.True(t, m.acked, "the final flush must land buffered work during shutdown")
}

func TestClassifyFlushErr(t *testing.T) {
	tests := []struct {
		name          string
		err           error
		wantPermanent bool
		wantNil       bool
	}{
		{name: "nil stays nil", err: nil, wantNil: true},
		{
			name: "plain connection error is transient",
			err:  errors.New("connection refused"),
		},
		{
			name: "document rejection is permanent",
			err: mongo.BulkWriteException{
				WriteErrors: []mongo.BulkWriteError{{WriteError: mongo.WriteError{Code: 9}}},
			},
			wantPermanent: true,
		},
		{
			name: "write-concern failure is transient",
			err: mongo.BulkWriteException{
				WriteErrors:       []mongo.BulkWriteError{{WriteError: mongo.WriteError{Code: 9}}},
				WriteConcernError: &mongo.WriteConcernError{Code: 64},
			},
		},
		{
			name: "retryable label wins over the document rejection",
			err: mongo.BulkWriteException{
				WriteErrors: []mongo.BulkWriteError{{WriteError: mongo.WriteError{Code: 9}}},
				Labels:      []string{"RetryableWriteError"},
			},
		},
		{
			name: "wrapped bulk exception is still classified",
			err: fmt.Errorf("bulk set subscription mentions (1 subscriptions): %w", mongo.BulkWriteException{
				WriteErrors: []mongo.BulkWriteError{{WriteError: mongo.WriteError{Code: 9}}},
			}),
			wantPermanent: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyFlushErr(tc.err)
			if tc.wantNil {
				assert.NoError(t, got)
				return
			}
			require.Error(t, got)
			_, isPermanent := errcode.IsPermanent(got)
			assert.Equal(t, tc.wantPermanent, isPermanent)
		})
	}
}
```

- [ ] **Step 3: Write the implementation**

Create `room-state-worker/flush.go`:

```go
package main

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/jsretry"
)

// flusher owns the pending batch and drains it to MongoDB on a ticker. Held
// messages are settled only after their batch lands, so JetStream — not an
// in-memory buffer — is what survives a MongoDB outage.
type flusher struct {
	store   Store
	backoff []time.Duration

	mu      sync.Mutex
	pending *batch
}

func newFlusher(store Store) *flusher {
	return &flusher{
		store:   store,
		backoff: jsretry.DefaultBackoff,
		pending: newBatch(),
	}
}

func (f *flusher) add(in writeIntents, msg heldMsg) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pending.add(in, msg)
}

// Flush swaps the pending batch out and writes it, then settles every message
// the batch was holding. The lock is held only for the swap, so the writes do
// not block new intents arriving from the consume loop.
func (f *flusher) Flush(ctx context.Context) {
	f.mu.Lock()
	if f.pending.empty() {
		f.mu.Unlock()
		return
	}
	b := f.pending
	f.pending = newBatch()
	f.mu.Unlock()

	f.settle(ctx, b, f.write(ctx, b))
}

// write applies the batch in a fixed order: rooms, then lastSeenAt, then
// mentions. lastSeenAt precedes mentions so the mention filter observes the
// sender's own advance, preserving broadcast-worker's sequential semantics —
// a message that @-mentions its own sender does not badge the sender.
//
// The first failure returns immediately: the whole batch is redelivered, so
// running the later writes would only widen the blast radius of a half-applied
// flush.
func (f *flusher) write(ctx context.Context, b *batch) error {
	if err := f.store.BulkUpdateRoomLastMessage(ctx, b.rooms); err != nil {
		return err
	}
	if err := f.store.BulkAdvanceLastSeen(ctx, b.lastSeen); err != nil {
		return err
	}
	return f.store.BulkSetMentions(ctx, b.mentions)
}

// settle resolves every held message against the flush outcome. SettleQuiet is
// used because the batch-level failure is logged once here — per-message
// logging would emit one identical line per held message.
func (f *flusher) settle(ctx context.Context, b *batch, err error) {
	classified := classifyFlushErr(err)
	if classified != nil {
		slog.ErrorContext(ctx, "room-state flush failed",
			"error", classified,
			"rooms", len(b.rooms),
			"last_seen", len(b.lastSeen),
			"mentions", len(b.mentions),
			"held", len(b.held))
	}
	for _, h := range b.held {
		jsretry.SettleQuiet(h.ctx, h.msg, f.backoff, classified)
	}
}

// classifyFlushErr marks document-level write rejections permanent so they are
// Ack-dropped. With MaxDeliver=-1 a rejected document would otherwise redeliver
// forever and wedge the consumer — the exact stall this service exists to
// avoid. Everything else (network, timeout, not-primary, write-concern) stays
// transient and is retried.
func classifyFlushErr(err error) error {
	if err == nil {
		return nil
	}
	var bwe mongo.BulkWriteException
	if !errors.As(err, &bwe) || len(bwe.WriteErrors) == 0 || bwe.WriteConcernError != nil {
		return err
	}
	for _, label := range bwe.Labels {
		if label == "RetryableWriteError" {
			return err
		}
	}
	return errcode.Permanent(errcode.Internal("mongo rejected room-state bulk write", errcode.WithCause(err)))
}

// Run drives the flush ticker until ctx is cancelled, then performs one final
// flush on a fresh context so a buffered batch still lands — and its messages
// still settle — even though the supplied ctx is already done.
func (f *flusher) Run(ctx context.Context, interval, finalTimeout time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			finalCtx, cancel := context.WithTimeout(context.Background(), finalTimeout)
			f.Flush(finalCtx)
			cancel()
			return
		case <-t.C:
			f.Flush(ctx)
		}
	}
}
```

- [ ] **Step 4: Run the test and verify it passes**

Run: `make test SERVICE=room-state-worker`
Expected: PASS. If `classifyFlushErr` mis-classifies, re-check the field names against Step 1's `go doc` output.

- [ ] **Step 5: Commit**

```bash
git add room-state-worker/flush.go room-state-worker/flush_test.go
git commit -m "feat(room-state-worker): flush batches with ack-after-write settlement

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01YK3aAf3kKfkzmtSYtmjicQ"
```

---

### Task 5: Wiring — config, consumer, consume loop, shutdown

**Files:**
- Create: `room-state-worker/main.go`, `room-state-worker/bootstrap.go`, `room-state-worker/pretouch.go`
- Test: `room-state-worker/main_test.go`

**Interfaces:**
- Consumes: `flusher` (Task 4), `deriveIntents` (Task 1), `NewMongoStore` (Task 3), and from `pkg/`: `stream.Resolve`, `stream.Pipeline`, `stream.ConsumerSettings`, `stream.DurableConsumerDefaults`, `natsutil.Connect`, `natsutil.StampRequestID`, `mongoutil.Connect`, `health.ServeWithPprof`, `shutdown.Wait`, `obs.InitWithLoggerHandler`, `logctx`, `jsonwarm.Pretouch`.
- Produces: `type config struct`; `buildConsumerConfig(s stream.ConsumerSettings, durable, filterSubject string) jetstream.ConsumerConfig`; `type messageIterator interface`; `consumeLoop(iter messageIterator, f *flusher, wg *sync.WaitGroup)`; `bootstrapStreams(ctx context.Context, js streamManager, streamName, subjectFilter string, enabled bool) error`; `pretouchJSON()`.

- [ ] **Step 1: Write the failing test**

Create `room-state-worker/main_test.go`:

```go
package main

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/stream"
)

func TestBuildConsumerConfig_UnlimitedDeliverAndDeliverNew(t *testing.T) {
	cc := buildConsumerConfig(stream.ConsumerSettings{
		AckWait:       30 * time.Second,
		MaxDeliver:    5,
		MaxWaiting:    512,
		MaxAckPending: 1000,
	}, "room-state-worker", "chat.msg.canonical.site-a.>")

	assert.Equal(t, "room-state-worker", cc.Durable)
	assert.Equal(t, "chat.msg.canonical.site-a.>", cc.FilterSubject)
	assert.Equal(t, jetstream.AckExplicitPolicy, cc.AckPolicy)
	// Durable retry: a MongoDB outage must not exhaust MaxDeliver and silently
	// drop badges. Poison protection is classifyFlushErr, not MaxDeliver.
	assert.Equal(t, -1, cc.MaxDeliver)
	// New, not All: replaying the whole canonical stream on first deploy would
	// re-apply historical writes as one large burst for no benefit.
	assert.Equal(t, jetstream.DeliverNewPolicy, cc.DeliverPolicy)
	assert.Equal(t, 30*time.Second, cc.AckWait)
	assert.Equal(t, 1000, cc.MaxAckPending)
}

func TestBuildConsumerConfig_BotModePrefixesDurable(t *testing.T) {
	assert.Equal(t, "bot-room-state-worker", stream.PipelineBot.ConsumerName("room-state-worker"))
	assert.Equal(t, "room-state-worker", stream.PipelineUser.ConsumerName("room-state-worker"))
}
```

- [ ] **Step 2: Run the test and verify it fails**

Run: `make test SERVICE=room-state-worker`
Expected: FAIL — `undefined: buildConsumerConfig`.

- [ ] **Step 3: Write `bootstrap.go` and `pretouch.go`**

Create `room-state-worker/bootstrap.go`:

```go
package main

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"

	o11ynats "github.com/flywindy/o11y/nats"
)

// bootstrapConfig groups fields only meaningful when standing up dev/integration
// against a NATS instance whose streams don't exist yet; in production streams
// are pre-provisioned and Enabled must stay false.
type bootstrapConfig struct {
	// Enabled (BOOTSTRAP_STREAMS) toggles whether the service calls
	// CreateOrUpdateStream at startup for the stream it consumes. Leave false in production.
	Enabled bool `env:"STREAMS" envDefault:"false"`
}

// streamManager is the minimal JetStream surface bootstrapStreams depends on,
// kept service-local so it doesn't pollute pkg/ and tests can inject a fake.
type streamManager interface {
	CreateOrUpdateStream(ctx context.Context, cfg jetstream.StreamConfig) (o11ynats.Stream, error)
	Stream(ctx context.Context, name string) (o11ynats.Stream, error)
}

// bootstrapStreams creates (dev/integration) or verifies (production, fail-fast)
// the JetStream input stream; identity is env-driven so user/bot deployments
// target their own stream.
func bootstrapStreams(ctx context.Context, js streamManager, streamName, subjectFilter string, enabled bool) error {
	if enabled {
		if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:     streamName,
			Subjects: []string{subjectFilter},
		}); err != nil {
			return fmt.Errorf("create stream %s: %w", streamName, err)
		}
		return nil
	}
	if _, err := js.Stream(ctx, streamName); err != nil {
		return fmt.Errorf("verify stream %s: %w", streamName, err)
	}
	return nil
}
```

Create `room-state-worker/pretouch.go`:

```go
package main

import (
	"reflect"

	"github.com/hmchangw/chat/pkg/jsonwarm"
	"github.com/hmchangw/chat/pkg/model"
)

// pretouchTypes are the hot event types whose sonic codecs are warmed at startup.
var pretouchTypes = []reflect.Type{
	reflect.TypeOf(model.MessageEvent{}),
}

func pretouchJSON() { jsonwarm.Pretouch(pretouchTypes...) }
```

- [ ] **Step 4: Write `main.go`**

Create `room-state-worker/main.go`:

```go
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/bytedance/sonic"
	"github.com/caarlos0/env/v11"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/health"
	"github.com/hmchangw/chat/pkg/jsretry"
	"github.com/hmchangw/chat/pkg/logctx"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/obs"
	"github.com/hmchangw/chat/pkg/shutdown"
	"github.com/hmchangw/chat/pkg/stream"
)

type config struct {
	NatsURL       string `env:"NATS_URL"        envDefault:"nats://localhost:4222"`
	NatsCredsFile string `env:"NATS_CREDS_FILE" envDefault:""`
	SiteID        string `env:"SITE_ID"         envDefault:"default"`
	MongoURI      string `env:"MONGO_URI"       envDefault:"mongodb://localhost:27017"`
	MongoDB       string `env:"MONGO_DB"        envDefault:"chat"`
	MongoUsername string `env:"MONGO_USERNAME"  envDefault:""`
	MongoPassword string `env:"MONGO_PASSWORD"  envDefault:""`
	// No MONGO_READ_PREFERENCE: this service only writes, and writes always go
	// to the primary.
	FlushInterval time.Duration `env:"FLUSH_INTERVAL" envDefault:"250ms"`
	HealthAddr    string        `env:"HEALTH_ADDR"    envDefault:":8081"`
	MetricsAddr   string        `env:"METRICS_ADDR"   envDefault:":9090"`
	PProfEnabled  bool          `env:"PPROF_ENABLED"  envDefault:"false"`
	// Mode selects the canonical stream/subject wiring via pkg/stream.Resolve.
	Mode      stream.Pipeline         `env:"MODE,required"`
	Consumer  stream.ConsumerSettings `envPrefix:"CONSUMER_"`
	Bootstrap bootstrapConfig         `envPrefix:"BOOTSTRAP_"`
	DebugLog  logctx.Config           `envPrefix:"DEBUG_LOG_"`
}

func main() {
	logctx.SetupDefault(os.Stdout)
	pretouchJSON()

	cfg, err := env.ParseAs[config]()
	if err != nil {
		slog.Error("parse config", "error", err)
		os.Exit(1)
	}
	logctx.Configure(cfg.DebugLog)

	ctx := context.Background()

	sdk, obsShutdown, err := obs.InitWithLoggerHandler(ctx, logctx.LevelTrace, logctx.NewHandler)
	if err != nil {
		slog.Error("init observability failed", "error", err)
		os.Exit(1)
	}

	mongoClient, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword,
		mongoutil.WithObservability(sdk))
	if err != nil {
		slog.Error("mongo connect failed", "error", err)
		os.Exit(1)
	}
	db := mongoClient.Database(cfg.MongoDB)
	store := NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions"))

	nc, err := natsutil.Connect(ctx, cfg.NatsURL, cfg.NatsCredsFile, sdk.TracerProvider(), sdk.Propagator, sdk.Toggles.Trace)
	if err != nil {
		slog.Error("nats connect failed", "error", err)
		os.Exit(1)
	}

	js, err := nc.JetStream()
	if err != nil {
		slog.Error("jetstream init failed", "error", err)
		os.Exit(1)
	}

	wiring := stream.Resolve(cfg.Mode, cfg.SiteID)
	if err := bootstrapStreams(ctx, js, wiring.CanonicalStream.Name, wiring.CanonicalWildcard, cfg.Bootstrap.Enabled); err != nil {
		slog.Error("bootstrap streams failed", "error", err)
		os.Exit(1)
	}

	cons, err := js.CreateOrUpdateConsumer(ctx, wiring.CanonicalStream.Name,
		buildConsumerConfig(cfg.Consumer, cfg.Mode.ConsumerName("room-state-worker"), wiring.CanonicalWildcard))
	if err != nil {
		slog.Error("create consumer failed", "error", err)
		os.Exit(1)
	}

	f := newFlusher(store)
	flushCtx, flushCancel := context.WithCancel(context.Background())
	flushDone := make(chan struct{})
	go func() { f.Run(flushCtx, cfg.FlushInterval, 5*time.Second); close(flushDone) }()
	slog.Info("room-state flusher started", "flush_interval", cfg.FlushInterval)

	// PullMaxMessages is bounded by MaxAckPending anyway; a modest buffer keeps
	// the single consume goroutine fed without over-fetching during an outage.
	iter, err := cons.Messages(ctx, jetstream.PullMaxMessages(256))
	if err != nil {
		slog.Error("messages failed", "error", err)
		os.Exit(1)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go consumeLoop(iter, f, &wg)

	healthStop, err := health.ServeWithPprof(cfg.HealthAddr, 5*time.Second, cfg.PProfEnabled,
		natsutil.HealthCheck(nc),
	)
	if err != nil {
		slog.Error("health server failed to start", "error", err)
		os.Exit(1)
	}

	slog.Info("room-state-worker started", "site", cfg.SiteID, "mode", string(cfg.Mode))

	shutdown.Wait(ctx, 25*time.Second,
		func(_ context.Context) error { iter.Stop(); return nil },
		func(ctx context.Context) error {
			done := make(chan struct{})
			go func() { wg.Wait(); close(done) }()
			select {
			case <-done:
				return nil
			case <-ctx.Done():
				return fmt.Errorf("consume loop drain timed out: %w", ctx.Err())
			}
		},
		// Stop the flusher only after the consume loop drains, so the final
		// flush includes every intent it added.
		func(ctx context.Context) error {
			flushCancel()
			select {
			case <-flushDone:
				return nil
			case <-ctx.Done():
				return fmt.Errorf("final flush timed out: %w", ctx.Err())
			}
		},
		func(ctx context.Context) error { return natsutil.Drain(ctx, nc) },
		func(ctx context.Context) error { mongoutil.Disconnect(ctx, mongoClient); return nil },
		func(ctx context.Context) error { return healthStop(ctx) },
		// Flush observability LAST so all prior teardown telemetry is exported.
		func(ctx context.Context) error { return obsShutdown(ctx) },
	)
}

// messageIterator is the slice of the o11y/nats MessagesContext the consume loop
// drives — an interface so the loop is testable without a live consumer.
type messageIterator interface {
	Next(...jetstream.NextOpt) (context.Context, jetstream.Msg, error)
}

// consumeLoop drains iter into the flusher. It is a single goroutine with no
// worker pool: the per-message work is a sonic unmarshal plus a regex parse and
// a map merge with no I/O, so concurrency would only add contention on the
// batch mutex. Messages are NOT settled here — the flusher settles them once
// their batch reaches MongoDB.
func consumeLoop(iter messageIterator, f *flusher, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		msgCtx, msg, err := iter.Next()
		if err != nil {
			return
		}
		handlerCtx, _ := natsutil.StampRequestID(msgCtx, msg.Headers(), msg.Subject())
		handlerCtx = logctx.Admit(handlerCtx, msg.Headers())
		logctx.CapturePayload(handlerCtx, "consumed", msg.Subject(), msg.Data())

		var evt model.MessageEvent
		if err := sonic.Unmarshal(msg.Data(), &evt); err != nil {
			// Malformed payload — it will never parse on redelivery. Settle it
			// immediately rather than holding it for a flush it can't join.
			jsretry.Settle(handlerCtx, msg, jsretry.DefaultBackoff,
				errcode.Permanent(errcode.BadRequest("malformed message event")))
			continue
		}
		handlerCtx = obs.ContextWithIdentity(handlerCtx, evt.Message.UserAccount, evt.Message.RoomID, evt.SiteID)
		f.add(deriveIntents(&evt), heldMsg{ctx: handlerCtx, msg: msg})
	}
}

// buildConsumerConfig returns the durable consumer config, centralized so it's
// unit-testable without NATS.
func buildConsumerConfig(s stream.ConsumerSettings, durable, filterSubject string) jetstream.ConsumerConfig {
	cc := stream.DurableConsumerDefaults(s)
	cc.Durable = durable
	cc.FilterSubject = filterSubject
	// Unlimited redelivery: a MongoDB outage must not exhaust MaxDeliver and
	// silently drop badges. Poison messages are handled by classifyFlushErr
	// Ack-dropping server-rejected writes, not by a delivery cap.
	cc.MaxDeliver = -1
	// New (not All): these writes are derived from live traffic. Replaying the
	// whole canonical stream on first deploy would re-apply historical writes as
	// one large burst for no benefit. DeliverPolicy is honored only at creation.
	cc.DeliverPolicy = jetstream.DeliverNewPolicy
	return cc
}
```

- [ ] **Step 5: Run tests and build**

Run: `make test SERVICE=room-state-worker && make build SERVICE=room-state-worker && make lint`
Expected: tests PASS, binary builds, lint clean.

- [ ] **Step 6: Commit**

```bash
git add room-state-worker/main.go room-state-worker/bootstrap.go room-state-worker/pretouch.go room-state-worker/main_test.go
git commit -m "feat(room-state-worker): wire consumer, flusher and graceful shutdown

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01YK3aAf3kKfkzmtSYtmjicQ"
```

---

### Task 6: Deploy artifacts

**Files:**
- Create: `room-state-worker/deploy/user/{Dockerfile,docker-compose.yml,azure-pipelines.yml}`, `room-state-worker/deploy/bot/{Dockerfile,docker-compose.yml,azure-pipelines.yml}`, `room-state-worker/deploy/README.md`
- Modify: `docker-local/compose.services.yaml`

- [ ] **Step 1: Write both Dockerfiles**

`room-state-worker/deploy/user/Dockerfile` and `room-state-worker/deploy/bot/Dockerfile` are byte-identical (the mode is set by compose env, matching broadcast-worker):

```dockerfile
FROM golang:1.25.12-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY pkg/ pkg/
COPY room-state-worker/ room-state-worker/

RUN CGO_ENABLED=0 go build -o /room-state-worker ./room-state-worker/

FROM alpine:3.21

RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app

COPY --from=builder /room-state-worker /room-state-worker

USER app
ENTRYPOINT ["/room-state-worker"]
```

- [ ] **Step 2: Write `room-state-worker/deploy/user/docker-compose.yml`**

```yaml
name: room-state-worker

services:
  room-state-worker:
    build:
      context: ../../..
      dockerfile: room-state-worker/deploy/user/Dockerfile
    environment:
      - OTEL_SERVICE_NAME=room-state-worker
      - OTEL_EXPORTER_OTLP_ENDPOINT=${OTEL_EXPORTER_OTLP_ENDPOINT:-http://otel-collector:4318}
      - NATS_URL=${NATS_URL:-nats://nats:4222}
      - NATS_CREDS_FILE=${NATS_CREDS_FILE:-/etc/nats/backend.creds}
      - SITE_ID=${SITE_ID:-site-local}
      - MODE=user
      - PPROF_ENABLED=${PPROF_ENABLED:-false}
      - MONGO_URI=${MONGO_URI:-mongodb://mongodb:27017}
      - MONGO_DB=${MONGO_DB:-chat}
      # Coalescing window. Held (un-acked) messages grow with this interval, so
      # CONSUMER_MAX_ACK_PENDING must exceed FLUSH_INTERVAL x peak message rate.
      - FLUSH_INTERVAL=${ROOM_STATE_FLUSH_INTERVAL:-250ms}
      # AckWait must exceed the flush interval plus write latency, or a batch
      # still being written will redeliver underneath itself.
      - CONSUMER_ACK_WAIT=${ROOM_STATE_ACK_WAIT:-30s}
      - CONSUMER_MAX_ACK_PENDING=${ROOM_STATE_MAX_ACK_PENDING:-1000}
      - BOOTSTRAP_STREAMS=${BOOTSTRAP_STREAMS:-true}
      - DEBUG_LOG_PAYLOADS=${DEBUG_LOG_PAYLOADS:-false}
      - DEBUG_LOG_RATE=${DEBUG_LOG_RATE:-50}
    volumes:
      - ../../../docker-local/backend.creds:${NATS_CREDS_FILE:-/etc/nats/backend.creds}:ro
    networks:
      - chat-local

networks:
  chat-local:
    external: true
```

- [ ] **Step 3: Write `room-state-worker/deploy/bot/docker-compose.yml`**

Identical to Step 2 with these four substitutions: `name: bot-room-state-worker`, service key `bot-room-state-worker`, `dockerfile: room-state-worker/deploy/bot/Dockerfile`, `OTEL_SERVICE_NAME=bot-room-state-worker`, and `MODE=bot`.

- [ ] **Step 4: Write both azure-pipelines.yml**

`room-state-worker/deploy/user/azure-pipelines.yml` — copy `broadcast-worker/deploy/user/azure-pipelines.yml` verbatim, then change: the two `paths.include` entries from `broadcast-worker/` to `room-state-worker/`, and `SERVICE_NAME: broadcast-worker` to `SERVICE_NAME: room-state-worker`. The `Dockerfile:` input already interpolates `$(SERVICE_NAME)/deploy/user/Dockerfile`, so it needs no edit.

`room-state-worker/deploy/bot/azure-pipelines.yml` — same, from `broadcast-worker/deploy/bot/azure-pipelines.yml`.

- [ ] **Step 5: Register both compose files**

In `docker-local/compose.services.yaml`, add to the `include:` list (keep it alphabetical, so after the `push-notification-service` entries and before `room-service`):

```yaml
  - ../room-state-worker/deploy/user/docker-compose.yml
  - ../room-state-worker/deploy/bot/docker-compose.yml
```

And add to the o11y anchor block, after `room-service: *local-o11y`:

```yaml
  room-state-worker: *local-o11y
  bot-room-state-worker: *local-o11y
```

- [ ] **Step 6: Write `room-state-worker/deploy/README.md`**

```markdown
# room-state-worker deployment

Applies the room-level MongoDB state derived from MESSAGES-CANONICAL:
`rooms.lastMsgAt`/`lastMsgId`/`lastMentionAllAt`, the sender's subscription
`lastSeenAt`, and the `hasMention` badge. broadcast-worker performs no MongoDB
writes; it only fans out.

## Deploy order

**Deploy room-state-worker BEFORE rolling broadcast-worker to the release that
removes its writes.** In that order the old broadcast-worker keeps writing until
the new worker is live, and the overlap is harmless — the writes are idempotent
and additive. The reverse order leaves a window in which nobody writes, and the
mention badges raised in that window are lost.

Rollback is an ordinary image rollback of broadcast-worker; the previous image
still writes.

## Tuning

| Variable | Meaning |
|---|---|
| `FLUSH_INTERVAL` | Coalescing window (default 250ms). Larger = fewer writes, more held messages. |
| `CONSUMER_MAX_ACK_PENDING` | Must exceed `FLUSH_INTERVAL` x peak message rate. Also the MongoDB-outage buffer: once full, JetStream stops delivering to this consumer and broadcast fan-out is unaffected. |
| `CONSUMER_ACK_WAIT` | Must exceed `FLUSH_INTERVAL` plus write latency. |

The consumer runs with `MaxDeliver=-1` so a MongoDB outage retries until it
recovers. Server-rejected documents are Ack-dropped with an ERROR log rather
than retried forever.

The health endpoint deliberately checks only NATS. Adding MongoDB would make
Kubernetes restart the pod during exactly the outage this service is built to
ride out, discarding the held batch each time.
```

- [ ] **Step 7: Verify compose parses**

Run: `docker compose -f docker-local/compose.services.yaml config -q`
Expected: no output, exit 0.

- [ ] **Step 8: Commit**

```bash
git add room-state-worker/deploy docker-local/compose.services.yaml
git commit -m "feat(room-state-worker): add user and bot deploy artifacts

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01YK3aAf3kKfkzmtSYtmjicQ"
```

---

### Task 7: Integration test

**Files:**
- Create: `room-state-worker/integration_test.go`

**Interfaces:**
- Consumes: `testutil.MongoDB`, `testutil.RunTests`, `NewMongoStore`, `newFlusher`, `deriveIntents`, `fakeMsg`/`held` from `batch_test.go` (same package, both build tags compile together under `-tags integration`).

- [ ] **Step 1: Write the failing test**

Create `room-state-worker/integration_test.go`:

```go
//go:build integration

package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

func setupStore(t *testing.T) (*mongoStore, *mongo.Database) {
	t.Helper()
	db := testutil.MongoDB(t, "room_state_worker_test")
	return NewMongoStore(db.Collection("rooms"), db.Collection("subscriptions")), db
}

func seedRoom(t *testing.T, db *mongo.Database, roomID string) {
	t.Helper()
	_, err := db.Collection("rooms").InsertOne(context.Background(), bson.M{
		"_id": roomID, "type": model.RoomTypeChannel, "siteId": "site-a",
	})
	require.NoError(t, err)
}

func seedSubscription(t *testing.T, db *mongo.Database, roomID, account string, lastSeenAt *time.Time) {
	t.Helper()
	doc := bson.M{
		"_id":    roomID + "-" + account,
		"roomId": roomID,
		"u":      bson.M{"account": account},
	}
	if lastSeenAt != nil {
		doc["lastSeenAt"] = *lastSeenAt
	}
	_, err := db.Collection("subscriptions").InsertOne(context.Background(), doc)
	require.NoError(t, err)
}

func readRoom(t *testing.T, db *mongo.Database, roomID string) bson.M {
	t.Helper()
	var got bson.M
	require.NoError(t, db.Collection("rooms").FindOne(context.Background(), bson.M{"_id": roomID}).Decode(&got))
	return got
}

func readSub(t *testing.T, db *mongo.Database, roomID, account string) bson.M {
	t.Helper()
	var got bson.M
	require.NoError(t, db.Collection("subscriptions").
		FindOne(context.Background(), bson.M{"roomId": roomID, "u.account": account}).Decode(&got))
	return got
}

// flushOne runs one event end-to-end through the real flusher and store.
func flushOne(t *testing.T, store Store, evt model.MessageEvent) *fakeMsg {
	t.Helper()
	f := newFlusher(store)
	m := &fakeMsg{}
	f.add(deriveIntents(&evt), held(m))
	f.Flush(context.Background())
	return m
}

func TestIntegration_CreatedMessageWritesRoomPointerSenderSeenAndMention(t *testing.T) {
	store, db := setupStore(t)
	created := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)
	seedRoom(t, db, "r1")
	seedSubscription(t, db, "r1", "alice", nil)
	seedSubscription(t, db, "r1", "bob", nil)

	m := flushOne(t, store, model.MessageEvent{
		Event: model.EventCreated,
		Message: model.Message{
			ID: "m1", RoomID: "r1", UserAccount: "alice",
			Content: "hey @bob", CreatedAt: created,
		},
	})

	assert.True(t, m.acked)
	room := readRoom(t, db, "r1")
	assert.Equal(t, "m1", room["lastMsgId"])
	assert.WithinDuration(t, created, room["lastMsgAt"].(bson.DateTime).Time(), time.Millisecond)

	alice := readSub(t, db, "r1", "alice")
	assert.WithinDuration(t, created, alice["lastSeenAt"].(bson.DateTime).Time(), time.Millisecond)
	assert.Nil(t, alice["hasMention"], "the sender is not badged by their own message")

	bob := readSub(t, db, "r1", "bob")
	assert.Equal(t, true, bob["hasMention"])
}

// The regression this design adds over broadcast-worker's coalescer: a
// redelivered older message must not drag the room pointer backwards.
func TestIntegration_StaleReplayDoesNotRegressRoomPointer(t *testing.T) {
	store, db := setupStore(t)
	older := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)
	newer := older.Add(time.Minute)
	seedRoom(t, db, "r1")

	flushOne(t, store, model.MessageEvent{
		Event:   model.EventCreated,
		Message: model.Message{ID: "m2", RoomID: "r1", UserAccount: "alice", CreatedAt: newer},
	})
	flushOne(t, store, model.MessageEvent{
		Event:   model.EventCreated,
		Message: model.Message{ID: "m1", RoomID: "r1", UserAccount: "alice", CreatedAt: older},
	})

	room := readRoom(t, db, "r1")
	assert.Equal(t, "m2", room["lastMsgId"], "the older replay must not win")
	assert.WithinDuration(t, newer, room["lastMsgAt"].(bson.DateTime).Time(), time.Millisecond)
}

func TestIntegration_MentionSkippedWhenAccountAlreadyRead(t *testing.T) {
	store, db := setupStore(t)
	created := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)
	readAfter := created.Add(time.Minute)
	seedRoom(t, db, "r1")
	seedSubscription(t, db, "r1", "bob", &readAfter)

	flushOne(t, store, model.MessageEvent{
		Event: model.EventCreated,
		Message: model.Message{
			ID: "m1", RoomID: "r1", UserAccount: "alice",
			Content: "@bob", CreatedAt: created,
		},
	})

	bob := readSub(t, db, "r1", "bob")
	assert.Nil(t, bob["hasMention"], "an account that already read past the message must not be badged")
}

func TestIntegration_SenderLastSeenNeverRegresses(t *testing.T) {
	store, db := setupStore(t)
	earlier := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)
	later := earlier.Add(time.Hour)
	seedRoom(t, db, "r1")
	seedSubscription(t, db, "r1", "alice", &later)

	flushOne(t, store, model.MessageEvent{
		Event:   model.EventCreated,
		Message: model.Message{ID: "m1", RoomID: "r1", UserAccount: "alice", CreatedAt: earlier},
	})

	alice := readSub(t, db, "r1", "alice")
	assert.WithinDuration(t, later, alice["lastSeenAt"].(bson.DateTime).Time(), time.Millisecond)
}

func TestIntegration_EditBadgesNewlyMentionedAccount(t *testing.T) {
	store, db := setupStore(t)
	created := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)
	edited := created.Add(time.Minute)
	seedRoom(t, db, "r1")
	seedSubscription(t, db, "r1", "bob", nil)

	flushOne(t, store, model.MessageEvent{
		Event: model.EventUpdated,
		Message: model.Message{
			ID: "m1", RoomID: "r1", UserAccount: "alice",
			Content: "now with @bob", CreatedAt: created, EditedAt: &edited, UpdatedAt: &edited,
		},
	})

	assert.Equal(t, true, readSub(t, db, "r1", "bob")["hasMention"])
	// An edit must not move the room pointer.
	room := readRoom(t, db, "r1")
	assert.Nil(t, room["lastMsgId"])
}

func TestIntegration_AllEventsInOneBatchCoalesceToOneRoomWrite(t *testing.T) {
	store, db := setupStore(t)
	t1 := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)
	seedRoom(t, db, "r1")
	seedSubscription(t, db, "r1", "alice", nil)

	f := newFlusher(store)
	msgs := make([]*fakeMsg, 3)
	for i := range msgs {
		msgs[i] = &fakeMsg{}
		f.add(deriveIntents(&model.MessageEvent{
			Event: model.EventCreated,
			Message: model.Message{
				ID: string(rune('a' + i)), RoomID: "r1", UserAccount: "alice",
				CreatedAt: t1.Add(time.Duration(i) * time.Second),
			},
		}), held(msgs[i]))
	}
	f.Flush(context.Background())

	for i, m := range msgs {
		assert.True(t, m.acked, "message %d must be acked once its batch lands", i)
	}
	assert.Equal(t, "c", readRoom(t, db, "r1")["lastMsgId"], "the latest message in the batch wins")
}
```

- [ ] **Step 2: Run the test and verify it passes**

Run: `make test-integration SERVICE=room-state-worker`
Expected: PASS. Requires Docker. If `bson.DateTime` type assertions fail, the driver returned a different BSON representation — read the actual value with `t.Logf("%T", room["lastMsgAt"])` and adjust the assertion, keeping the assertion's meaning identical.

- [ ] **Step 3: Check coverage**

Run: `go test -tags integration -coverprofile=coverage.out ./room-state-worker/ && go tool cover -func=coverage.out | tail -1`
Expected: total ≥ 80%. If below, add unit cases to the file that is short — do not add assertions that do not test behaviour.

- [ ] **Step 4: Commit**

```bash
git add room-state-worker/integration_test.go
git commit -m "test(room-state-worker): integration coverage incl. stale-replay ordering

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01YK3aAf3kKfkzmtSYtmjicQ"
```

---

### Task 8: Remove the writes from broadcast-worker

Deletion only. broadcast-worker keeps every read; after this it never NAKs because of a MongoDB write, so the duplicate-broadcast-on-redelivery path is gone.

**Files:**
- Modify: `broadcast-worker/store.go`, `broadcast-worker/store_mongo.go`, `broadcast-worker/handler.go`, `broadcast-worker/main.go`, `broadcast-worker/handler_test.go`, `broadcast-worker/store_mongo_test.go`, `broadcast-worker/integration_test.go`, `broadcast-worker/deploy/user/docker-compose.yml`, `broadcast-worker/deploy/bot/docker-compose.yml`
- Delete: `broadcast-worker/coalescer.go`, `broadcast-worker/coalescer_test.go`
- Regenerate: `broadcast-worker/mock_store_test.go`

- [ ] **Step 1: Update the failing tests first**

In `broadcast-worker/handler_test.go`, remove every mock expectation for the three departing methods — search for `UpdateRoomLastMessage`, `SetSubscriptionMentions`, `AdvanceSubscriptionLastSeen` and delete those `EXPECT()` lines and any test whose sole subject is one of them (for example a test asserting a store error on `SetSubscriptionMentions` propagates to the handler). Tests that assert broadcast *publishing* stay untouched.

In `broadcast-worker/store_mongo_test.go`, delete tests covering `subscriptionMentionsFilter`, `UpdateRoomLastMessage`, `BulkUpdateRoomLastMessage`, and `AdvanceSubscriptionLastSeen`.

In `broadcast-worker/integration_test.go`, delete assertions that read `rooms.lastMsgAt`, `rooms.lastMsgId`, `rooms.lastMentionAllAt`, `subscriptions.hasMention`, or `subscriptions.lastSeenAt` — that behaviour now belongs to room-state-worker's integration test.

- [ ] **Step 2: Run the tests and verify they fail**

Run: `make test SERVICE=broadcast-worker`
Expected: FAIL — the mock still requires the removed calls, or tests reference deleted helpers. This is the red state for the deletion.

- [ ] **Step 3: Delete the production code**

Delete the files:

```bash
git rm broadcast-worker/coalescer.go broadcast-worker/coalescer_test.go
```

In `broadcast-worker/store.go`, delete these three methods from the `Store` interface and their doc comments: `UpdateRoomLastMessage`, `SetSubscriptionMentions`, `AdvanceSubscriptionLastSeen`. The interface keeps `GetRoom`, `GetRoomMeta`, `ListSubscriptions`, `GetThreadFollowers`, `GetHistorySharedSince`.

In `broadcast-worker/store_mongo.go`, delete the methods `UpdateRoomLastMessage`, `BulkUpdateRoomLastMessage`, `SetSubscriptionMentions`, `AdvanceSubscriptionLastSeen` and the helper `subscriptionMentionsFilter`. Remove the now-unused `options` import only if nothing else uses it — `GetThreadFollowers` and `GetHistorySharedSince` still do, so it stays.

In `broadcast-worker/handler.go`:
- In `handleCreated`, delete the `UpdateRoomLastMessage` block, the `AdvanceSubscriptionLastSeen` block with its comment, and the `if len(resolved.Accounts) > 0 { SetSubscriptionMentions }` block. The function keeps the mention parse and user lookup (both feed `buildClientMessage` and the event's `Mentions`), `GetRoomMeta`, and the room-type switch.
- Delete `badgeNewlyMentionedAccounts` entirely and its call site in `handleUpdated` (the `if err := h.badgeNewlyMentionedAccounts(...)` block).

In `broadcast-worker/main.go`:
- Delete the `LastMsgFlushInterval` config field.
- Delete the `coalescer := newCoalescingStore(...)`, `flushCtx, flushCancel := ...`, `go coalescer.Run(...)` and the `slog.Info("last-msg coalescer enabled", ...)` lines.
- Change `NewHandler(coalescer, ...)` to `NewHandler(cachedStore, ...)`.
- Delete the `flushCancel()` shutdown hook and its comment.

In both `broadcast-worker/deploy/user/docker-compose.yml` and `broadcast-worker/deploy/bot/docker-compose.yml`, delete any `LAST_MSG_FLUSH_INTERVAL` environment line if present.

- [ ] **Step 4: Regenerate mocks and run everything**

Run: `make generate SERVICE=broadcast-worker && make test SERVICE=broadcast-worker && make lint`
Expected: mock regenerated without the three methods; tests PASS; lint clean.

- [ ] **Step 5: Run broadcast-worker integration tests**

Run: `make test-integration SERVICE=broadcast-worker`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add -A broadcast-worker
git commit -m "refactor(broadcast-worker): drop MongoDB writes now owned by room-state-worker

A MongoDB write failure used to NAK the canonical message, re-running the
handler and re-broadcasting a message clients already had. broadcast-worker
now only reads and fans out.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01YK3aAf3kKfkzmtSYtmjicQ"
```

---

### Task 9: Documentation and full verification

**Files:**
- Modify: `CLAUDE.md`, `docs/architecture.md`

- [ ] **Step 1: Update CLAUDE.md**

In Section 1, replace the **Event flow** sentence:

> **Event flow:** User publishes message to MESSAGES stream → `message-gatekeeper` validates and publishes to MESSAGES-CANONICAL → `message-worker` persists to Cassandra, `broadcast-worker` delivers to room members, `notification-worker` sends notifications → cross-site events are published directly into remote sites' INBOX streams.

with:

> **Event flow:** User publishes message to MESSAGES stream → `message-gatekeeper` validates and publishes to MESSAGES-CANONICAL → `message-worker` persists to Cassandra, `broadcast-worker` delivers to room members, `room-state-worker` applies room/subscription state (`lastMsgAt`, `hasMention`, sender `lastSeenAt`), `notification-worker` sends notifications → cross-site events are published directly into remote sites' INBOX streams.

In Section 6 under **JetStream Streams**, append to the `MESSAGES-CANONICAL-{siteID}` bullet:

> Consumed by `message-worker` (Cassandra persistence), `broadcast-worker` (fan-out, reads only), `room-state-worker` (room/subscription MongoDB writes) and `notification-worker`. `room-state-worker` runs with `MaxDeliver=-1` so a MongoDB outage retries rather than drops; broadcast-worker performs no MongoDB writes, so a MongoDB failure can never make it re-broadcast a message.

- [ ] **Step 2: Update docs/architecture.md**

Read the file first and follow its existing service-listing format. Add a `room-state-worker` entry describing: consumes MESSAGES-CANONICAL; writes `rooms.lastMsgAt`/`lastMsgId`/`lastMentionAllAt` and `subscriptions.hasMention`/`lastSeenAt`; coalesces on a 250 ms flush and settles messages only after the write lands; deploy order requires it to be live before broadcast-worker rolls to the release that removes its writes.

- [ ] **Step 3: Run the full verification suite**

Run each and confirm the output before claiming success:

```bash
make lint
make test
make test-integration SERVICE=room-state-worker
make test-integration SERVICE=broadcast-worker
make sast
```

Expected: all green. `make sast` must show no medium-or-higher findings — it is a blocking CI gate.

- [ ] **Step 4: Confirm no stray artifacts**

Run: `git status --short && ls docs/reviews 2>/dev/null`
Expected: a clean tree; `docs/reviews/` either absent or empty. If it has files, delete them — session review reports must never ship.

- [ ] **Step 5: Commit and push**

```bash
git add CLAUDE.md docs/architecture.md
git commit -m "docs: document room-state-worker in event flow and architecture

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01YK3aAf3kKfkzmtSYtmjicQ"
git push -u origin claude/broadcast-worker-mongodb-extract-25ji6b
```

Do NOT open a pull request.

---

## Verification Summary

| Requirement | Where it is verified |
|---|---|
| Duplicate broadcast on MongoDB failure is gone | Task 8 — broadcast-worker has no write that can return an error |
| Writes retry until MongoDB recovers | Task 5 (`MaxDeliver=-1`) + Task 4 (`TestFlusher_NaksHeldMessagesOnTransientWriteFailure`) |
| A rejected document cannot wedge the consumer | Task 4 (`TestFlusher_AcksHeldMessagesOnServerRejectedWrite`, `TestClassifyFlushErr`) |
| Stale replay never regresses `lastMsgAt` | Task 3 (filter unit test) + Task 7 (`TestIntegration_StaleReplayDoesNotRegressRoomPointer`) |
| Self-mention still does not badge the sender | Task 4 (write-order assertion) + Task 7 (`TestIntegration_CreatedMessageWritesRoomPointerSenderSeenAndMention`) |
| Thread replies produce no room-level writes | Task 1 (`deriveIntents` table) |
| MongoDB is not in the readiness probe | Task 5 (`health.ServeWithPprof` registers only `natsutil.HealthCheck`) |
| Every consumed message is settled exactly once | Task 2 (`TestBatch_HoldsNoOpMessagesForAck`) + Task 4 (all settle tests) |
