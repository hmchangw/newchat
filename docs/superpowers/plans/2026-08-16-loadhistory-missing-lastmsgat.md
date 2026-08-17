# Missing-lastMsgAt Empty-History Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rooms whose Mongo doc lacks `lastMsgAt` must load history normally — `resolveRoomTimes` must stop rewriting the unknown (zero) `lastMsgAt` to `createdAt`.

**Architecture:** One guard in `resolveRoomTimes`' final consistency collapse (`!last.IsZero() &&`). Callers already treat zero `lastMsgAt` as "unknown" (`LoadHistory` skips its `before`-cap; `walkBounds` ceilings at `now+clockSkewTolerance`), so preserving the zero fixes `msg.history`, `msg.next`, and `msg.surrounding` at once.

**Tech Stack:** Go 1.25, testify + gomock (go.uber.org/mock v0.6.0 — `gomock.Cond` available) unit tests.

**Spec:** `docs/superpowers/specs/2026-08-16-loadhistory-missing-lastmsgat-design.md`

## Global Constraints

- All commands via `make` targets, never raw `go` (CLAUDE.md §2).
- TDD: tests first, confirm RED, then implement.
- The hint-consistency refetch and the non-zero inverted-pair collapse keep today's behavior — only the zero-`lastMsgAt` case changes.
- No wire/schema change ⇒ no `docs/client-api.md` edit.
- Internal-package tests (`package service`) may import `history-service/internal/service/mocks` (no import cycle — proven by the existing `room_times_test.go`).

---

### Task 1: Preserve zero lastMsgAt in resolveRoomTimes (TDD)

**Files:**
- Modify: `history-service/internal/service/room_times.go:114-117` (final collapse in `resolveRoomTimes`)
- Test: `history-service/internal/service/room_times_test.go` (append; `package service`)
- Test: `history-service/internal/service/messages_lastmsgat_test.go` (new; `package service`)

**Interfaces:**
- Consumes: `mocks.MockRoomRepository.GetRoomTimes(ctx, roomID) (lastMsgAt, createdAt time.Time, err error)`; `mocks.MockMessageRepository.GetMessagesBefore(ctx, roomID string, before, floor time.Time, pageReq cassrepo.PageRequest) (cassrepo.Page[models.Message], error)`; `service.New(msgs, subs, rooms, pub, threadRooms, threadSubs, users, apps, cfg)`; `HistoryService.historyFloor time.Duration` (unexported field, settable from `package service` tests); `walkBounds(lastMsgAt, createdAt, now time.Time) (ceiling, floor time.Time)`.
- Produces: `resolveRoomTimes` returns `lastMsgAt` **zero** (not `createdAt`) when Mongo has no `lastMsgAt` and no usable hint supplies one.

- [ ] **Step 1: Write the failing resolver tests**

Append to `history-service/internal/service/room_times_test.go`:

```go
// Mongo has no lastMsgAt recorded (zero) — "unknown", NOT "empty room": the room
// may hold messages (legacy docs, failed lastMsgAt update). The resolver must
// return the zero untouched so callers apply their unknown-handling (LoadHistory
// skips its before-cap; walkBounds ceilings at now+skew).
func TestResolveRoomTimes_MissingLastMsgAt_StaysUnknown(t *testing.T) {
	now := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	created := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)

	ctrl := gomock.NewController(t)
	mockResolver := mocks.NewMockRoomRepository(ctrl)
	mockResolver.EXPECT().
		GetRoomTimes(gomock.Any(), "room-1").
		Return(time.Time{}, created, nil).
		Times(1)

	s := &HistoryService{rooms: mockResolver}
	gotLast, gotCreated, err := s.resolveRoomTimes(context.Background(), "room-1", nil, now)
	require.NoError(t, err)
	assert.True(t, gotLast.IsZero(), "missing lastMsgAt must stay zero, got %v", gotLast)
	assert.Equal(t, created, gotCreated.UTC())
}

// A createdAt hint on a no-lastMsgAt room triggers the consistency refetch
// (hint created > zero last); after the refetch the zero must still survive.
func TestResolveRoomTimes_MissingLastMsgAt_CreatedHintRefetchKeepsUnknown(t *testing.T) {
	now := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	created := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	hintMs := now.Add(-30 * 24 * time.Hour).UnixMilli()

	ctrl := gomock.NewController(t)
	mockResolver := mocks.NewMockRoomRepository(ctrl)
	mockResolver.EXPECT().
		GetRoomTimes(gomock.Any(), "room-1").
		Return(time.Time{}, created, nil).
		Times(2)

	s := &HistoryService{rooms: mockResolver}
	gotLast, gotCreated, err := s.resolveRoomTimes(context.Background(), "room-1", &models.RoomMeta{CreatedAt: &hintMs}, now)
	require.NoError(t, err)
	assert.True(t, gotLast.IsZero(), "missing lastMsgAt must stay zero after refetch, got %v", gotLast)
	assert.Equal(t, created, gotCreated.UTC())
}
```

- [ ] **Step 2: Write the walkBounds unit test (coverage hardening — green from the start)**

Append to `history-service/internal/service/room_times_test.go`:

```go
func TestWalkBounds(t *testing.T) {
	now := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	s := &HistoryService{historyFloor: 90 * 24 * time.Hour}
	historyFloor := now.Add(-s.historyFloor)
	created := now.Add(-10 * 24 * time.Hour)
	last := now.Add(-time.Hour)

	tests := []struct {
		name                     string
		lastMsgAt, createdAt     time.Time
		wantCeiling, wantFloor   time.Time
	}{
		{name: "zero lastMsgAt → ceiling now+skew", lastMsgAt: time.Time{}, createdAt: created, wantCeiling: now.Add(clockSkewTolerance), wantFloor: created},
		{name: "non-zero lastMsgAt → ceiling lastMsgAt", lastMsgAt: last, createdAt: created, wantCeiling: last, wantFloor: created},
		{name: "zero createdAt → floor historyFloor", lastMsgAt: last, createdAt: time.Time{}, wantCeiling: last, wantFloor: historyFloor},
		{name: "createdAt older than floor → clamped", lastMsgAt: last, createdAt: now.Add(-200 * 24 * time.Hour), wantCeiling: last, wantFloor: historyFloor},
		{name: "lastMsgAt below floor → ceiling clamped up to floor", lastMsgAt: now.Add(-100 * 24 * time.Hour), createdAt: time.Time{}, wantCeiling: historyFloor, wantFloor: historyFloor},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ceiling, floor := s.walkBounds(tc.lastMsgAt, tc.createdAt, now)
			assert.Equal(t, tc.wantCeiling, ceiling)
			assert.Equal(t, tc.wantFloor, floor)
		})
	}
}
```

- [ ] **Step 3: Write the failing LoadHistory regression test**

Create `history-service/internal/service/messages_lastmsgat_test.go`:

```go
package service

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/history-service/internal/cassrepo"
	"github.com/hmchangw/chat/history-service/internal/config"
	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/history-service/internal/service/mocks"
	"github.com/hmchangw/chat/pkg/natsrouter"
)

// Regression: a room whose Mongo doc has no lastMsgAt (zero from GetRoomTimes)
// but holds messages in Cassandra must load history. Before the fix, the
// resolver collapsed the zero to createdAt, LoadHistory capped before at
// createdAt+1ms, and the walk scanned only pre-creation time — always empty.
func TestLoadHistory_MissingLastMsgAt_ReturnsMessages(t *testing.T) {
	ctrl := gomock.NewController(t)
	msgs := mocks.NewMockMessageRepository(ctrl)
	subs := mocks.NewMockSubscriptionRepository(ctrl)
	rooms := mocks.NewMockRoomRepository(ctrl)
	pub := mocks.NewMockEventPublisher(ctrl)
	threadRooms := mocks.NewMockThreadRoomRepository(ctrl)
	threadSubs := mocks.NewMockThreadSubscriptionRepository(ctrl)
	users := mocks.NewMockUserStore(ctrl)
	apps := mocks.NewMockAppStore(ctrl)
	cfg := &config.Config{MessageHistoryFloorDays: 90, LargeRoomThreshold: 500, MaxPinnedPerRoom: 10, PinEnabled: true}
	s := New(msgs, subs, rooms, pub, threadRooms, threadSubs, users, apps, cfg)

	createdAt := time.Now().UTC().Add(-120 * 24 * time.Hour)
	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(time.Time{}, createdAt, nil)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)

	recent := models.Message{MessageID: "m1", RoomID: "r1", CreatedAt: time.Now().UTC().Add(-time.Hour)}
	// The before bound must NOT be capped at createdAt+1ms — it must stay ≈ now.
	msgs.EXPECT().
		GetMessagesBefore(gomock.Any(), "r1",
			gomock.Cond(func(x any) bool {
				before, ok := x.(time.Time)
				return ok && before.After(createdAt.Add(24*time.Hour))
			}),
			gomock.Any(), gomock.Any()).
		Return(cassrepo.Page[models.Message]{Data: []models.Message{recent}}, nil)

	c := natsrouter.NewContext(map[string]string{"account": "u1", "roomID": "r1"})
	resp, err := s.LoadHistory(c, models.LoadHistoryRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Messages, 1, "room with messages but no lastMsgAt must return them")
	assert.Equal(t, "m1", resp.Messages[0].MessageID)
}
```

- [ ] **Step 4: Run tests to verify RED**

Run: `make test SERVICE=history-service`
Expected: FAIL —
- `TestResolveRoomTimes_MissingLastMsgAt_StaysUnknown`: `gotLast` equals `createdAt`, not zero.
- `TestResolveRoomTimes_MissingLastMsgAt_CreatedHintRefetchKeepsUnknown`: same assertion failure.
- `TestLoadHistory_MissingLastMsgAt_ReturnsMessages`: gomock reports the `GetMessagesBefore` expectation unmatched (LoadHistory calls it with `before = createdAt+1ms`, rejected by the `Cond` matcher).
- `TestWalkBounds`: PASS (documents existing zero-handling).

- [ ] **Step 5: Apply the one-line fix**

In `history-service/internal/service/room_times.go`, replace:

```go
		// Empty room or hint-refetch still inconsistent — collapse the range.
		if created.After(*last) {
			last = created
		}
```

with:

```go
		// Still inverted with a real lastMsgAt — corrupt pair; collapse the
		// range. A zero lastMsgAt stays zero: "not recorded" means UNKNOWN,
		// not "empty room" — the room may hold messages (legacy docs, failed
		// lastMsgAt update); callers treat zero as unknown (LoadHistory skips
		// its before-cap, walkBounds ceilings at now+skew).
		if !last.IsZero() && created.After(*last) {
			last = created
		}
```

- [ ] **Step 6: Run tests to verify GREEN**

Run: `make test SERVICE=history-service`
Expected: PASS — all new tests plus every existing row of `TestResolveRoomTimes` (the refetch row exercises a non-zero Mongo `lastMsgAt`, untouched by the guard).

- [ ] **Step 7: Full unit suite + lint**

Run: `make test` then `make lint`
Expected: all packages ok; 0 lint issues.

- [ ] **Step 8: Commit**

```bash
git add history-service/internal/service/room_times.go history-service/internal/service/room_times_test.go history-service/internal/service/messages_lastmsgat_test.go
git commit -m "fix(history-service): treat missing room lastMsgAt as unknown, not empty room"
```

---

## Verification (after the task)

- `make test-integration SERVICE=history-service` — no regression in the Cassandra walker suites.
- Update PR #285's description to include the bugfix.
