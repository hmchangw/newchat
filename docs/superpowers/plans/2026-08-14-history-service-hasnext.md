# Load History `hasNext` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose the Cassandra walker's already-computed `HasNext` flag in the Load History RPC response so the frontend can lazy-load older pages on scroll-up.

**Architecture:** `LoadHistory` already receives `cassrepo.Page[models.Message]` whose `HasNext` field the bucket walker populates on every backward read; the change plumbs that one bool through `LoadHistoryResponse` onto the wire. No repository, walker, or request changes.

**Tech Stack:** Go 1.25, testify + gomock unit tests, NATS request/reply (natsrouter).

**Spec:** `docs/superpowers/specs/2026-08-14-history-service-hasnext-design.md`

## Global Constraints

- All commands via `make` targets, never raw `go` (CLAUDE.md §2).
- TDD: tests first, confirm RED, then implement (CLAUDE.md §4).
- Client-facing RPC change ⇒ `docs/client-api.md` AND `docs/client-api/request-reply.md` must be updated in the same PR (CLAUDE.md §5).
- JSON tag is exactly `hasNext`, always present (no `omitempty`), matching `LoadNextMessagesResponse`.
- Unit tests never touch real databases/NATS; use the existing `mocks` package and `makePage` helper.

---

### Task 1: `HasNext` field — model + handler (TDD)

**Files:**
- Modify: `history-service/internal/models/message.go:30-33` (`LoadHistoryResponse`)
- Modify: `history-service/internal/service/messages.go:96-99` (`LoadHistory` return)
- Test: `history-service/internal/service/messages_test.go` (after `TestHistoryService_LoadHistory_WithBeforeTimestamp`, ~line 230)

**Interfaces:**
- Consumes: `cassrepo.Page[models.Message].HasNext bool` (already returned by `msgReader.GetMessagesBefore` / `GetMessagesBetweenDesc`); test helper `makePage(msgs []models.Message, hasNext bool) cassrepo.Page[models.Message]` (`messages_test.go:130`).
- Produces: `models.LoadHistoryResponse.HasNext bool` with tag `json:"hasNext"` — Task 2 documents this exact field name.

- [ ] **Step 1: Write the failing tests**

Add to `history-service/internal/service/messages_test.go`, after `TestHistoryService_LoadHistory_WithBeforeTimestamp` (~line 230). Both walk paths get a `hasNext=true` case; the `false` case is asserted by extending two existing tests in Step 2.

```go
func TestHistoryService_LoadHistory_HasNextTrue_AccessWindowed(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)

	messages := []models.Message{
		{MessageID: "m1", RoomID: "r1", CreatedAt: joinTime.Add(2 * time.Minute)},
		{MessageID: "m0", RoomID: "r1", CreatedAt: joinTime.Add(time.Minute)},
	}
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).Return(makePage(messages, true), nil)

	resp, err := svc.LoadHistory(c, models.LoadHistoryRequest{})
	require.NoError(t, err)
	assert.True(t, resp.HasNext)
}

func TestHistoryService_LoadHistory_HasNextTrue_OpenWalk(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)

	messages := []models.Message{
		{MessageID: "m1", RoomID: "r1", CreatedAt: joinTime.Add(2 * time.Minute)},
	}
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).Return(makePage(messages, true), nil)

	resp, err := svc.LoadHistory(c, models.LoadHistoryRequest{})
	require.NoError(t, err)
	assert.True(t, resp.HasNext)
}
```

- [ ] **Step 2: Extend two existing tests with the `false` assertions**

In `TestHistoryService_LoadHistory_Success` (windowed path, terminal page), after `assert.Len(t, resp.Messages, 4)` add:

```go
	assert.False(t, resp.HasNext)
```

In `TestHistoryService_LoadHistory_NoHSS` (open-walk path, terminal page), after `assert.Len(t, resp.Messages, 3)` add:

```go
	assert.False(t, resp.HasNext)
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `make test SERVICE=history-service`
Expected: compile FAILURE — `resp.HasNext undefined (type *models.LoadHistoryResponse has no field or method HasNext)`. (A compile error on the not-yet-existing field is this change's RED state.)

- [ ] **Step 4: Add the field to the model**

In `history-service/internal/models/message.go`, change `LoadHistoryResponse` to:

```go
type LoadHistoryResponse struct {
	Messages          []Message `json:"messages"`
	HasNext           bool      `json:"hasNext"`
	MinUserLastSeenAt *int64    `json:"minUserLastSeenAt,omitempty"` // UTC millis
}
```

- [ ] **Step 5: Run tests to verify the new assertions still fail (now meaningfully)**

Run: `make test SERVICE=history-service`
Expected: FAIL — `TestHistoryService_LoadHistory_HasNextTrue_AccessWindowed` and `TestHistoryService_LoadHistory_HasNextTrue_OpenWalk` fail on `assert.True(t, resp.HasNext)` (field exists but is never set). The two extended tests pass (zero value false).

- [ ] **Step 6: Implement the plumb-through**

In `history-service/internal/service/messages.go`, `LoadHistory`, change the return to:

```go
	return &models.LoadHistoryResponse{
		Messages:          page.Data,
		HasNext:           page.HasNext,
		MinUserLastSeenAt: minMs,
	}, nil
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `make test SERVICE=history-service`
Expected: PASS (all history-service unit tests).

- [ ] **Step 8: Lint**

Run: `make lint`
Expected: clean.

- [ ] **Step 9: Commit**

```bash
git add history-service/internal/models/message.go history-service/internal/service/messages.go history-service/internal/service/messages_test.go
git commit -m "feat(history-service): return hasNext in Load History response"
```

---

### Task 2: Client API docs

**Files:**
- Modify: `docs/client-api.md:3011-3032` (§ Load History success response table + JSON example)
- Modify: `docs/client-api/request-reply.md:951-956` (§ Load History success response table)

**Interfaces:**
- Consumes: the wire field `hasNext` (boolean, always present) produced by Task 1.
- Produces: documentation only.

- [ ] **Step 1: Update `docs/client-api.md` § Load History**

In the success response table (line ~3011), insert a `hasNext` row between `messages` and `minUserLastSeenAt`:

```markdown
| `messages` | array<Message> | Most-recent first. See [Message schema](#message-schema). |
| `hasNext` | boolean | `true` if older messages exist beyond this page — fetch the next page with `before` = the oldest returned message's `createdAt`. `false` once the caller's history boundary (room start, history floor, or access window) is reached. |
| `minUserLastSeenAt` | number | Optional. UTC milliseconds since Unix epoch. The room's **strict read floor** — `MIN(lastSeenAt)` across all subscribers, present **only when every member has read** the room. Omitted (the key is absent, never `null`) when any member has not read yet (so botDM rooms, where the bot never reads, never set it), when the most recent read is already past `room.lastMsgAt` (recompute is skipped), or when the value cannot be retrieved (best-effort; messages still load). See the Message Read RPC for how this floor is recomputed. |
```

In the JSON example below the table, add `"hasNext": true` after the `messages` array:

```json
{
  "messages": [
    {
      "roomId": "01970a4f8c2d7c9aQ",
      "createdAt": "2026-05-06T07:55:00Z",
      "messageId": "01970a4f8c2d7c9aQRST",
      "sender": {
        "id": "01970a4f8c2d7c9a01970a4f8c2d7c9a",
        "account": "alice",
        "engName": "Alice"
      },
      "msg": "morning team"
    }
  ],
  "hasNext": true,
  "minUserLastSeenAt": 1746518100000
}
```

- [ ] **Step 2: Update `docs/client-api/request-reply.md` § Load History**

In its success response table (line ~953), insert the same row (derived view uses terser wording, matching its Load Next Messages entry):

```markdown
| `messages` | Message[] | Most-recent first. |
| `hasNext` | boolean | `true` if older messages exist. |
| `minUserLastSeenAt` | number | Optional. UTC ms. The room's strict read floor — present only when every member has read. |
```

- [ ] **Step 3: Verify no other derived-view drift**

Run: `grep -n "hasNext" docs/client-api/events.md`
Expected: no output (events view untouched — no event change).

- [ ] **Step 4: Commit**

```bash
git add docs/client-api.md docs/client-api/request-reply.md
git commit -m "docs(client-api): document hasNext in Load History response"
```

---

## Verification (after all tasks)

- `make lint` — clean.
- `make test` — all unit tests pass (race detector on).
- `make test-integration SERVICE=history-service` — requires Docker; walker `HasNext` behavior already covered by cassrepo integration tests, this confirms no regression.
