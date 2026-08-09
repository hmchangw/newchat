# Loadgen Verify — Correctness Automation Test Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `loadgen verify` subcommand that asserts messaging correctness under concurrency — delivery completeness, no leakage, exactly-once, persistence, and membership-change effectiveness — producing a PASS/FAIL/INCONCLUSIVE verdict.

**Architecture:** A new scenario runner in `tools/loadgen/` that reuses the daily-IM fixtures, connection pools, and action emitter, but replaces the capacity ramp with a single fixed window (warmup → steady → quiesce → drain → readback). Probe rooms are chosen first and their members forced into the direct pool; a deterministic sample of sends into those rooms is tracked per-recipient by a new `ProbeTracker`. `Collector` is not modified.

**Tech Stack:** Go 1.25, `nats.go` + `jetstream`, `stretchr/testify`, `go.uber.org/mock`. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-08-08-loadgen-verify-correctness-design.md`

## Global Constraints

- All commands run through `make` targets — never raw `go` commands. Unit tests: `make test SERVICE=tools/loadgen`.
- TDD is mandatory: write the test, run it, confirm it FAILS, then implement. Never write implementation before its test exists.
- Minimum 80% coverage per package; target 90%+ on `ProbeTracker` and the verdict evaluator.
- All tests use `-race` (the Makefile handles this).
- Test files are `package main` in `tools/loadgen/`, same package as the code under test.
- Errors wrap with context: `fmt.Errorf("short description: %w", err)`. Never bare `err`, never `fmt.Errorf("error: %w", err)`.
- Logging is `log/slog` with structured key-value fields. Never `fmt.Println`. **Never log message content, tokens, or bodies** — `msgID`, `roomID`, and user IDs only.
- NATS subjects come from `pkg/subject` builders, never raw `fmt.Sprintf`.
- Never use `time.Sleep` for goroutine synchronization — use channels, `sync.WaitGroup`, `sync.Mutex`.
- Never launch a goroutine without a clear termination path.
- `Collector` (`tools/loadgen/collector.go`) MUST NOT be modified. Its first-delivery-wins delete is correct for latency and wrong for fan-out.
- Changes to `daily.go` / `daily_pool.go` must be additive — `daily`, `max-rps`, `soak`, and `members` behaviour must not change.
- Commit after each task's tests pass. Pre-commit hook runs lint + tests.
- Run `make lint` and `make sast` before pushing.

---

## File Structure

| File | Responsibility |
|---|---|
| `tools/loadgen/verify.go` | `runVerify` entry point, config/flag parsing, lifecycle orchestration, preflight |
| `tools/loadgen/verify_rooms.go` | Probe-room selection, member union, reserve floater selection |
| `tools/loadgen/verify_probe.go` | `ProbeTracker` — probe registration, delivery recording, dedupe, leakage, finalize |
| `tools/loadgen/verify_membership.go` | Membership churn driver, epoch/settle bookkeeping, dual-oracle comparison |
| `tools/loadgen/verify_readback.go` | `history-service` persistence readback with bounded retry |
| `tools/loadgen/verify_verdict.go` | PASS/FAIL/INCONCLUSIVE evaluation |
| `tools/loadgen/verify_report.go` | Console and JSON rendering |
| `tools/loadgen/verify_*_test.go` | Unit tests, one per source file above |

Modified: `main.go` (dispatch), `daily_pool.go` (receiver attribution + `SubscribeRoom`), `daily.go` (designated direct-pool seeding), `deploy/Makefile` (`run-verify`), `README.md`.

---

## Task 1: Violation types and probe-room selection

**Files:**
- Create: `tools/loadgen/verify_rooms.go`
- Test: `tools/loadgen/verify_rooms_test.go`

**Interfaces:**
- Consumes: `Fixtures` from `preset.go:113` (`Users []model.User`, `Rooms []model.Room`, `Subscriptions []model.Subscription`)
- Produces: `ViolationKind` constants, `Violation` struct, `ProbeRoomSet` struct, `selectProbeRooms(fx Fixtures, n int, largeThreshold int, seed int64) (ProbeRoomSet, error)`, `selectReserve(fx Fixtures, prs ProbeRoomSet, n int, seed int64) []string`

**Context:** `BuildFixtures` names rooms by band: DM rooms are `room-dm-NNNNNN`, others `room-small-…`, `room-medium-…`, `room-large-…` (see the `ChannelRooms` comment at `daily_user.go:116`). Band is derived from the ID prefix. `model.Room.UserCount` carries room size; `model.Subscription` has `RoomID` and `User.ID`.

DM rooms are a mandatory third of the mix — they are the only band on the per-user lane, and the leakage check only runs there (spec §7.3).

- [ ] **Step 1: Write the failing test**

```go
package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

// fixturesForTest builds a small deterministic Fixtures with a known band mix:
// 6 DM rooms (2 members each), 6 small (5 members), 6 medium (50 members),
// 2 large (600 members). Members are drawn from a 1000-user pool.
func fixturesForTest() Fixtures {
	users := make([]model.User, 1000)
	for i := range users {
		users[i] = model.User{
			ID:      fmtUserID(i),
			Account: fmtAccount(i),
			SiteID:  "site-test",
		}
	}
	var rooms []model.Room
	var subs []model.Subscription
	next := 0
	add := func(id string, size int) {
		rooms = append(rooms, model.Room{ID: id, SiteID: "site-test", UserCount: size})
		for j := 0; j < size; j++ {
			u := users[(next+j)%len(users)]
			subs = append(subs, model.Subscription{
				RoomID: id,
				SiteID: "site-test",
				User:   model.SubscriptionUser{ID: u.ID, Account: u.Account},
			})
		}
		next += size
	}
	for i := 0; i < 6; i++ {
		add(fmtRoomID("dm", i), 2)
	}
	for i := 0; i < 6; i++ {
		add(fmtRoomID("small", i), 5)
	}
	for i := 0; i < 6; i++ {
		add(fmtRoomID("medium", i), 50)
	}
	for i := 0; i < 2; i++ {
		add(fmtRoomID("large", i), 600)
	}
	return Fixtures{Users: users, Rooms: rooms, Subscriptions: subs}
}

func TestSelectProbeRooms_ExcludesLargeRooms(t *testing.T) {
	fx := fixturesForTest()

	prs, err := selectProbeRooms(fx, 9, 500, 42)
	require.NoError(t, err)

	for _, r := range prs.Rooms {
		assert.NotContains(t, r.ID, "large", "large-band room must never be probe-eligible")
		assert.Less(t, r.UserCount, 500, "room at or above threshold must be excluded")
	}
}

func TestSelectProbeRooms_AlwaysIncludesDMs(t *testing.T) {
	fx := fixturesForTest()

	// Every seed must yield at least one DM room — the leakage check has no
	// other lane to run on.
	for seed := int64(0); seed < 25; seed++ {
		prs, err := selectProbeRooms(fx, 9, 500, seed)
		require.NoError(t, err)

		dms := 0
		for _, r := range prs.Rooms {
			if bandOf(r.ID) == bandDM {
				dms++
			}
		}
		assert.Positive(t, dms, "seed %d produced a DM-free probe set", seed)
	}
}

func TestSelectProbeRooms_Deterministic(t *testing.T) {
	fx := fixturesForTest()

	a, err := selectProbeRooms(fx, 9, 500, 42)
	require.NoError(t, err)
	b, err := selectProbeRooms(fx, 9, 500, 42)
	require.NoError(t, err)

	assert.Equal(t, a.RoomIDs(), b.RoomIDs())
	assert.Equal(t, a.Members, b.Members)
}

func TestSelectProbeRooms_MemberUnionIsComplete(t *testing.T) {
	fx := fixturesForTest()

	prs, err := selectProbeRooms(fx, 9, 500, 42)
	require.NoError(t, err)

	// Every subscription belonging to a selected room must appear in Members.
	selected := map[string]bool{}
	for _, r := range prs.Rooms {
		selected[r.ID] = true
	}
	for _, s := range fx.Subscriptions {
		if selected[s.RoomID] {
			assert.Contains(t, prs.Members, s.User.ID,
				"member %s of probe room %s missing from union", s.User.ID, s.RoomID)
		}
	}
}

func TestSelectProbeRooms_MembersByRoomMatchesFixtures(t *testing.T) {
	fx := fixturesForTest()

	prs, err := selectProbeRooms(fx, 9, 500, 42)
	require.NoError(t, err)

	for _, r := range prs.Rooms {
		got := prs.MembersOf(r.ID)
		assert.Len(t, got, r.UserCount, "room %s member count mismatch", r.ID)
	}
}

func TestSelectProbeRooms_ErrorsWhenNotEnoughEligibleRooms(t *testing.T) {
	fx := fixturesForTest()

	_, err := selectProbeRooms(fx, 500, 500, 42)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not enough eligible rooms")
}

func TestSelectReserve_ExcludesProbeRoomMembers(t *testing.T) {
	fx := fixturesForTest()

	prs, err := selectProbeRooms(fx, 9, 500, 42)
	require.NoError(t, err)

	reserve := selectReserve(fx, prs, 20, 42)
	require.Len(t, reserve, 20)

	for _, id := range reserve {
		assert.NotContains(t, prs.Members, id,
			"reserve floater %s must not already be a probe-room member", id)
	}
}

func TestSelectReserve_Deterministic(t *testing.T) {
	fx := fixturesForTest()
	prs, err := selectProbeRooms(fx, 9, 500, 42)
	require.NoError(t, err)

	assert.Equal(t, selectReserve(fx, prs, 20, 42), selectReserve(fx, prs, 20, 42))
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=tools/loadgen`
Expected: FAIL — `undefined: selectProbeRooms`, `undefined: bandOf`, `undefined: fmtUserID`, etc.

- [ ] **Step 3: Write minimal implementation**

Create `tools/loadgen/verify_rooms.go`:

```go
package main

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"

	"github.com/hmchangw/chat/pkg/model"
)

// ViolationKind enumerates the correctness failures verify can report.
// Values are stable strings — they appear in JSON output and operator runbooks.
type ViolationKind string

const (
	KindMissingRecipient            ViolationKind = "missing_recipient"
	KindTotalLoss                   ViolationKind = "total_loss"
	KindDuplicateDelivery           ViolationKind = "duplicate_delivery"
	KindUnexpectedRecipient         ViolationKind = "unexpected_recipient"
	KindPersistenceMiss             ViolationKind = "persistence_miss"
	KindPersistenceMismatch         ViolationKind = "persistence_mismatch"
	KindMembershipNotApplied        ViolationKind = "membership_not_applied"
	KindMembershipAddIneffective    ViolationKind = "membership_add_ineffective"
	KindMembershipRemoveIneffective ViolationKind = "membership_remove_ineffective"
)

// Violation is one concrete correctness failure. Users carries the specific
// user IDs implicated so an operator can grep service logs directly.
// Detail never contains message content.
type Violation struct {
	Kind   ViolationKind `json:"kind"`
	MsgID  string        `json:"msgId,omitempty"`
	RoomID string        `json:"roomId,omitempty"`
	Users  []string      `json:"users,omitempty"`
	Epoch  int           `json:"epoch"`
	Detail string        `json:"detail,omitempty"`
}

// band classifies a fixture room by its ID prefix. BuildFixtures names rooms
// room-dm-NNNNNN / room-small-… / room-medium-… / room-large-….
type band int

const (
	bandUnknown band = iota
	bandDM
	bandSmall
	bandMedium
	bandLarge
)

func bandOf(roomID string) band {
	switch {
	case strings.HasPrefix(roomID, "room-dm-"):
		return bandDM
	case strings.HasPrefix(roomID, "room-small-"):
		return bandSmall
	case strings.HasPrefix(roomID, "room-medium-"):
		return bandMedium
	case strings.HasPrefix(roomID, "room-large-"):
		return bandLarge
	default:
		return bandUnknown
	}
}

// usesUserLane reports whether broadcasts for this room are addressed
// per-recipient (subject.UserRoomEvent) rather than published once to the
// room topic. Only DM rooms use the per-user lane, and only there is the
// leakage check meaningful — see spec §7.3.
func usesUserLane(roomID string) bool { return bandOf(roomID) == bandDM }

// ProbeRoomSet is the outcome of probe-room selection: the chosen rooms, the
// complete union of their members, and a per-room member index.
type ProbeRoomSet struct {
	Rooms   []model.Room
	Members []string // sorted union, deterministic
	byRoom  map[string][]string
}

// RoomIDs returns the selected room IDs in selection order.
func (p ProbeRoomSet) RoomIDs() []string {
	out := make([]string, len(p.Rooms))
	for i, r := range p.Rooms {
		out[i] = r.ID
	}
	return out
}

// MembersOf returns the member user IDs of one probe room.
func (p ProbeRoomSet) MembersOf(roomID string) []string { return p.byRoom[roomID] }

// Has reports whether roomID is a probe room.
func (p ProbeRoomSet) Has(roomID string) bool {
	_, ok := p.byRoom[roomID]
	return ok
}

// probeBands is the fixed band mix. DM is mandatory: it is the only band on
// the per-user lane, so a DM-free probe set would leave the leakage check
// permanently unexercised (spec §6.0 step 1).
var probeBands = []band{bandDM, bandSmall, bandMedium}

// selectProbeRooms picks n rooms deterministically from seed, taking an equal
// share from each of probeBands and excluding any room at or above
// largeThreshold. Returns an error rather than silently under-filling, since a
// thin probe set produces a weak verdict.
func selectProbeRooms(fx Fixtures, n, largeThreshold int, seed int64) (ProbeRoomSet, error) {
	if n <= 0 {
		return ProbeRoomSet{}, fmt.Errorf("probe room count must be positive, got %d", n)
	}

	eligible := map[band][]model.Room{}
	for _, r := range fx.Rooms {
		b := bandOf(r.ID)
		if b == bandLarge || b == bandUnknown {
			continue
		}
		if r.UserCount >= largeThreshold {
			continue // gatekeeper would reject sends here — spec §6.1
		}
		eligible[b] = append(eligible[b], r)
	}

	// Sort each band by ID so selection is independent of fixture iteration order.
	for b := range eligible {
		sort.Slice(eligible[b], func(i, j int) bool { return eligible[b][i].ID < eligible[b][j].ID })
	}

	r := rand.New(rand.NewSource(seed))
	perBand := n / len(probeBands)
	remainder := n % len(probeBands)

	var chosen []model.Room
	for i, b := range probeBands {
		want := perBand
		if i < remainder {
			want++
		}
		pool := eligible[b]
		if len(pool) < want {
			return ProbeRoomSet{}, fmt.Errorf(
				"not enough eligible rooms in band %d: want %d, have %d", b, want, len(pool))
		}
		perm := r.Perm(len(pool))[:want]
		sort.Ints(perm)
		for _, idx := range perm {
			chosen = append(chosen, pool[idx])
		}
	}

	byRoom := make(map[string][]string, len(chosen))
	for _, room := range chosen {
		byRoom[room.ID] = nil
	}
	memberSet := map[string]struct{}{}
	for _, s := range fx.Subscriptions {
		if _, ok := byRoom[s.RoomID]; !ok {
			continue
		}
		byRoom[s.RoomID] = append(byRoom[s.RoomID], s.User.ID)
		memberSet[s.User.ID] = struct{}{}
	}
	for id := range byRoom {
		sort.Strings(byRoom[id])
	}

	members := make([]string, 0, len(memberSet))
	for id := range memberSet {
		members = append(members, id)
	}
	sort.Strings(members)

	return ProbeRoomSet{Rooms: chosen, Members: members, byRoom: byRoom}, nil
}

// selectReserve picks n users who are not already probe-room members. They are
// direct-connected as floaters so a membership change mid-run has an
// observable target (spec §6.0 step 3).
func selectReserve(fx Fixtures, prs ProbeRoomSet, n int, seed int64) []string {
	inProbe := make(map[string]struct{}, len(prs.Members))
	for _, id := range prs.Members {
		inProbe[id] = struct{}{}
	}

	candidates := make([]string, 0, len(fx.Users))
	for _, u := range fx.Users {
		if _, ok := inProbe[u.ID]; !ok {
			candidates = append(candidates, u.ID)
		}
	}
	sort.Strings(candidates)

	// Offset the seed so the reserve permutation is independent of the
	// room permutation drawn from the same seed.
	r := rand.New(rand.NewSource(seed ^ 0x5EED0BE5))
	if n > len(candidates) {
		n = len(candidates)
	}
	perm := r.Perm(len(candidates))[:n]
	sort.Ints(perm)

	out := make([]string, 0, n)
	for _, idx := range perm {
		out = append(out, candidates[idx])
	}
	return out
}
```

Add the test-only ID helpers to `verify_rooms_test.go` (import `fmt`):

```go
func fmtUserID(i int) string           { return fmt.Sprintf("u-%06d", i) }
func fmtAccount(i int) string          { return fmt.Sprintf("user-%d", i) }
func fmtRoomID(b string, i int) string { return fmt.Sprintf("room-%s-%06d", b, i) }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=tools/loadgen`
Expected: PASS, all 8 tests in `verify_rooms_test.go`.

- [ ] **Step 5: Commit**

```bash
git add tools/loadgen/verify_rooms.go tools/loadgen/verify_rooms_test.go
git commit -m "feat(loadgen): probe-room selection and violation types for verify"
```

---

## Task 2: ProbeTracker — registration, delivery, dedupe, completeness

**Files:**
- Create: `tools/loadgen/verify_probe.go`
- Test: `tools/loadgen/verify_probe_test.go`

**Interfaces:**
- Consumes: `Violation`, `ViolationKind`, `usesUserLane` from Task 1
- Produces: `lane` type + `laneGlobal`/`laneLocal`/`laneUser` constants, `NewProbeTracker() *ProbeTracker`, `(*ProbeTracker).RegisterProbe(msgID, roomID, senderID string, epoch int, expected []string, at time.Time)`, `.RecordDelivery(userID, msgID, roomID string, ln lane, at time.Time)`, `.RecordSuppressed()`, `.Counts() ProbeCounts`, `.Finalize() []Violation`

**Context:** The sender IS in its own expected set — `broadcast-worker` echoes to the sender (spec §7.2). Every room is subscribed on both the global and local lane, so a duplicate is only a violation *within* one lane (spec §7.1).

- [ ] **Step 1: Write the failing test**

```go
package main

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func at(sec int) time.Time { return time.Unix(int64(sec), 0).UTC() }

func TestProbeTracker_CompleteDelivery_NoViolations(t *testing.T) {
	tr := NewProbeTracker()
	tr.RegisterProbe("m1", "room-small-000001", "u-1", 0, []string{"u-1", "u-2", "u-3"}, at(1))

	tr.RecordDelivery("u-1", "m1", "room-small-000001", laneGlobal, at(2))
	tr.RecordDelivery("u-2", "m1", "room-small-000001", laneGlobal, at(2))
	tr.RecordDelivery("u-3", "m1", "room-small-000001", laneGlobal, at(2))

	assert.Empty(t, tr.Finalize())
}

func TestProbeTracker_SenderMustBeExpected(t *testing.T) {
	tr := NewProbeTracker()
	// Sender u-1 is in the expected set (broadcast-worker echoes) but never
	// receives its own message.
	tr.RegisterProbe("m1", "room-small-000001", "u-1", 0, []string{"u-1", "u-2"}, at(1))
	tr.RecordDelivery("u-2", "m1", "room-small-000001", laneGlobal, at(2))

	vs := tr.Finalize()
	require.Len(t, vs, 1)
	assert.Equal(t, KindMissingRecipient, vs[0].Kind)
	assert.Equal(t, []string{"u-1"}, vs[0].Users)
}

func TestProbeTracker_PartialDelivery_MissingRecipient(t *testing.T) {
	tr := NewProbeTracker()
	tr.RegisterProbe("m1", "room-small-000001", "u-1", 0, []string{"u-1", "u-2", "u-3"}, at(1))
	tr.RecordDelivery("u-1", "m1", "room-small-000001", laneGlobal, at(2))

	vs := tr.Finalize()
	require.Len(t, vs, 1)
	assert.Equal(t, KindMissingRecipient, vs[0].Kind)
	assert.Equal(t, "m1", vs[0].MsgID)
	assert.Equal(t, []string{"u-2", "u-3"}, vs[0].Users, "missing users must be sorted")
}

func TestProbeTracker_ZeroDelivery_TotalLoss(t *testing.T) {
	tr := NewProbeTracker()
	tr.RegisterProbe("m1", "room-small-000001", "u-1", 0, []string{"u-1", "u-2"}, at(1))

	vs := tr.Finalize()
	require.Len(t, vs, 1)
	assert.Equal(t, KindTotalLoss, vs[0].Kind,
		"zero recipients is total_loss, not missing_recipient — different investigation")
}

func TestProbeTracker_DuplicateWithinLane_IsViolation(t *testing.T) {
	tr := NewProbeTracker()
	tr.RegisterProbe("m1", "room-small-000001", "u-1", 0, []string{"u-1"}, at(1))
	tr.RecordDelivery("u-1", "m1", "room-small-000001", laneGlobal, at(2))
	tr.RecordDelivery("u-1", "m1", "room-small-000001", laneGlobal, at(3))

	vs := tr.Finalize()
	require.Len(t, vs, 1)
	assert.Equal(t, KindDuplicateDelivery, vs[0].Kind)
	assert.Equal(t, []string{"u-1"}, vs[0].Users)
}

func TestProbeTracker_SameMsgOnBothLanes_IsNotDuplicate(t *testing.T) {
	tr := NewProbeTracker()
	tr.RegisterProbe("m1", "room-small-000001", "u-1", 0, []string{"u-1"}, at(1))
	// Both lanes are subscribed to stay ROOM_SUBJECT_MODE-agnostic. One
	// arrival per lane is expected, not a duplicate.
	tr.RecordDelivery("u-1", "m1", "room-small-000001", laneGlobal, at(2))
	tr.RecordDelivery("u-1", "m1", "room-small-000001", laneLocal, at(2))

	assert.Empty(t, tr.Finalize())
}

func TestProbeTracker_UnknownMsgID_Ignored(t *testing.T) {
	tr := NewProbeTracker()
	// Untracked traffic (99% of the workload) must not allocate or panic.
	tr.RecordDelivery("u-9", "not-a-probe", "room-small-000001", laneGlobal, at(2))
	assert.Empty(t, tr.Finalize())
	assert.Zero(t, tr.Counts().Tracked)
}

func TestProbeTracker_Counts(t *testing.T) {
	tr := NewProbeTracker()
	tr.RegisterProbe("m1", "room-small-000001", "u-1", 0, []string{"u-1"}, at(1))
	tr.RegisterProbe("m2", "room-small-000001", "u-1", 0, []string{"u-1"}, at(1))
	tr.RecordDelivery("u-1", "m1", "room-small-000001", laneGlobal, at(2))
	tr.RecordSuppressed()
	tr.RecordSuppressed()

	c := tr.Counts()
	assert.Equal(t, 2, c.Tracked)
	assert.Equal(t, 2, c.Suppressed)
	assert.Equal(t, 1, c.Complete)
	assert.Equal(t, 1, c.TotalLoss)
}

func TestProbeTracker_ConcurrentDeliveries(t *testing.T) {
	tr := NewProbeTracker()
	expected := make([]string, 200)
	for i := range expected {
		expected[i] = fmtUserID(i)
	}
	tr.RegisterProbe("m1", "room-medium-000001", expected[0], 0, expected, at(1))

	var wg sync.WaitGroup
	for i := range expected {
		wg.Add(1)
		go func(u string) {
			defer wg.Done()
			tr.RecordDelivery(u, "m1", "room-medium-000001", laneGlobal, at(2))
		}(expected[i])
	}
	wg.Wait()

	assert.Empty(t, tr.Finalize(), "all 200 concurrent deliveries must be recorded")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=tools/loadgen`
Expected: FAIL — `undefined: NewProbeTracker`, `undefined: laneGlobal`.

- [ ] **Step 3: Write minimal implementation**

Create `tools/loadgen/verify_probe.go`:

```go
package main

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// lane identifies which subscription carried a delivery. Rooms are subscribed
// on both the global and local lanes to stay ROOM_SUBJECT_MODE-agnostic, so a
// duplicate is only a violation within a single lane (spec §7.1).
type lane string

const (
	laneGlobal lane = "global"
	laneLocal  lane = "local"
	laneUser   lane = "user"
)

// deliveryKey is the dedupe key: one delivery per user per lane is expected.
type deliveryKey struct {
	userID string
	ln     lane
}

type probeRecord struct {
	msgID       string
	roomID      string
	senderID    string
	epoch       int
	publishedAt time.Time
	expected    map[string]struct{}
	received    map[deliveryKey]int
}

// ProbeCounts is the summary surfaced in the report.
type ProbeCounts struct {
	Tracked    int
	Suppressed int
	Complete   int
	Partial    int
	TotalLoss  int
}

// ProbeTracker records per-recipient delivery for sampled messages and reports
// completeness, exactly-once, and leakage violations.
//
// Deliberately separate from Collector: Collector deletes its correlation entry
// on first delivery, which is correct for a latency histogram and wrong for
// fan-out accounting.
type ProbeTracker struct {
	mu     sync.Mutex
	probes map[string]*probeRecord

	suppressed int
}

// NewProbeTracker returns a ready-to-use tracker.
func NewProbeTracker() *ProbeTracker {
	return &ProbeTracker{probes: make(map[string]*probeRecord)}
}

// RegisterProbe records that msgID was published into roomID and is expected to
// reach every user in expected. The sender is included in expected by the
// caller — broadcast-worker echoes to the sender (spec §7.2).
func (t *ProbeTracker) RegisterProbe(msgID, roomID, senderID string, epoch int, expected []string, at time.Time) {
	exp := make(map[string]struct{}, len(expected))
	for _, u := range expected {
		exp[u] = struct{}{}
	}
	rec := &probeRecord{
		msgID: msgID, roomID: roomID, senderID: senderID, epoch: epoch,
		publishedAt: at,
		expected:    exp,
		received:    make(map[deliveryKey]int, len(expected)),
	}
	t.mu.Lock()
	t.probes[msgID] = rec
	t.mu.Unlock()
}

// RecordDelivery notes that userID received msgID on the given lane. Deliveries
// for untracked messages are ignored cheaply — 99% of traffic is untracked.
func (t *ProbeTracker) RecordDelivery(userID, msgID, roomID string, ln lane, _ time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()

	rec, ok := t.probes[msgID]
	if !ok {
		return
	}
	if _, expected := rec.expected[userID]; !expected {
		return
	}
	rec.received[deliveryKey{userID: userID, ln: ln}]++
}

// RecordSuppressed notes a send that would have been probed but fell inside a
// settle window (spec §9.2). Suppressed probes do not count toward --min-probes.
func (t *ProbeTracker) RecordSuppressed() {
	t.mu.Lock()
	t.suppressed++
	t.mu.Unlock()
}

// Counts returns summary counters for the report.
func (t *ProbeTracker) Counts() ProbeCounts {
	t.mu.Lock()
	defer t.mu.Unlock()

	c := ProbeCounts{Tracked: len(t.probes), Suppressed: t.suppressed}
	for _, rec := range t.probes {
		got := rec.deliveredUsers()
		switch {
		case len(got) == 0:
			c.TotalLoss++
		case len(got) < len(rec.expected):
			c.Partial++
		default:
			c.Complete++
		}
	}
	return c
}

// deliveredUsers returns the distinct users who received this probe on at least
// one lane. Caller must hold the lock.
func (r *probeRecord) deliveredUsers() map[string]struct{} {
	out := make(map[string]struct{}, len(r.received))
	for k := range r.received {
		out[k.userID] = struct{}{}
	}
	return out
}

// Finalize evaluates every probe and returns the violations found, sorted by
// message ID so output is stable across runs.
func (t *ProbeTracker) Finalize() []Violation {
	t.mu.Lock()
	defer t.mu.Unlock()

	msgIDs := make([]string, 0, len(t.probes))
	for id := range t.probes {
		msgIDs = append(msgIDs, id)
	}
	sort.Strings(msgIDs)

	var out []Violation
	for _, id := range msgIDs {
		rec := t.probes[id]
		out = append(out, rec.violations()...)
	}
	return out
}

// violations evaluates one probe. Caller must hold the lock.
func (r *probeRecord) violations() []Violation {
	var out []Violation

	got := r.deliveredUsers()
	if len(got) == 0 {
		out = append(out, Violation{
			Kind: KindTotalLoss, MsgID: r.msgID, RoomID: r.roomID, Epoch: r.epoch,
			Detail: fmt.Sprintf("reached 0 of %d expected recipients", len(r.expected)),
		})
	} else if len(got) < len(r.expected) {
		var missing []string
		for u := range r.expected {
			if _, ok := got[u]; !ok {
				missing = append(missing, u)
			}
		}
		sort.Strings(missing)
		out = append(out, Violation{
			Kind: KindMissingRecipient, MsgID: r.msgID, RoomID: r.roomID,
			Users: missing, Epoch: r.epoch,
			Detail: fmt.Sprintf("reached %d of %d expected recipients", len(got), len(r.expected)),
		})
	}

	var dupes []string
	for k, n := range r.received {
		if n > 1 {
			dupes = append(dupes, k.userID)
		}
	}
	if len(dupes) > 0 {
		sort.Strings(dupes)
		out = append(out, Violation{
			Kind: KindDuplicateDelivery, MsgID: r.msgID, RoomID: r.roomID,
			Users: dupes, Epoch: r.epoch, Detail: "same messageID delivered twice on one lane",
		})
	}

	return out
}
```

Leakage detection is deliberately absent here — it lands in Task 3 with its
own red phase.

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=tools/loadgen`
Expected: PASS, 9 tests.

- [ ] **Step 5: Commit**

```bash
git add tools/loadgen/verify_probe.go tools/loadgen/verify_probe_test.go
git commit -m "feat(loadgen): ProbeTracker with completeness, dedupe, and leakage checks"
```

---

## Task 3: Leakage detection on the per-user lane

**Files:**
- Modify: `tools/loadgen/verify_probe_test.go`
- Test: same file

**Files (revised):**
- Modify: `tools/loadgen/verify_probe.go`
- Test: `tools/loadgen/verify_probe_test.go`

**Interfaces:**
- Consumes: everything from Task 2
- Produces: `ProbeCounts.Leaked` field; leakage recording in `RecordDelivery`; `KindUnexpectedRecipient` emission in `probeRecord.violations`

**Context:** This is the highest-severity check (a message reaching a non-member is a privacy incident) and the one most likely to be mis-specified. It runs on `laneUser` only — see spec §7.3. Task 2 deliberately left this out so the tests below start red.

- [ ] **Step 1: Write the failing test**

Append to `tools/loadgen/verify_probe_test.go`:

```go
func TestProbeTracker_LeakageOnUserLane_IsViolation(t *testing.T) {
	tr := NewProbeTracker()
	tr.RegisterProbe("m1", "room-dm-000001", "u-1", 0, []string{"u-1", "u-2"}, at(1))

	tr.RecordDelivery("u-1", "m1", "room-dm-000001", laneUser, at(2))
	tr.RecordDelivery("u-2", "m1", "room-dm-000001", laneUser, at(2))
	// u-9 is not a member of this DM.
	tr.RecordDelivery("u-9", "m1", "room-dm-000001", laneUser, at(2))

	vs := tr.Finalize()
	require.Len(t, vs, 1)
	assert.Equal(t, KindUnexpectedRecipient, vs[0].Kind)
	assert.Equal(t, []string{"u-9"}, vs[0].Users)
}

func TestProbeTracker_LeakageOnRoomLane_IsIgnored(t *testing.T) {
	tr := NewProbeTracker()
	tr.RegisterProbe("m1", "room-small-000001", "u-1", 0, []string{"u-1"}, at(1))

	tr.RecordDelivery("u-1", "m1", "room-small-000001", laneGlobal, at(2))
	// A non-member receiving on the room topic reflects who subscribed, which
	// loadgen itself controls (backend.creds has full chat.> permissions).
	// Treating it as leakage would test NATS ACLs, not the chat system.
	tr.RecordDelivery("u-9", "m1", "room-small-000001", laneGlobal, at(2))
	tr.RecordDelivery("u-8", "m1", "room-small-000001", laneLocal, at(2))

	assert.Empty(t, tr.Finalize(),
		"room-lane delivery to a non-member must never be reported as leakage")
}

func TestProbeTracker_LeakageDoesNotCountAsDelivery(t *testing.T) {
	tr := NewProbeTracker()
	tr.RegisterProbe("m1", "room-dm-000001", "u-1", 0, []string{"u-1", "u-2"}, at(1))

	tr.RecordDelivery("u-1", "m1", "room-dm-000001", laneUser, at(2))
	tr.RecordDelivery("u-9", "m1", "room-dm-000001", laneUser, at(2))

	vs := tr.Finalize()
	// u-2 is still missing; the leak must not paper over the gap.
	require.Len(t, vs, 2)
	kinds := []ViolationKind{vs[0].Kind, vs[1].Kind}
	assert.Contains(t, kinds, KindMissingRecipient)
	assert.Contains(t, kinds, KindUnexpectedRecipient)
}

func TestProbeTracker_RepeatedLeakFromSameUser_ReportedOnce(t *testing.T) {
	tr := NewProbeTracker()
	tr.RegisterProbe("m1", "room-dm-000001", "u-1", 0, []string{"u-1", "u-2"}, at(1))
	tr.RecordDelivery("u-1", "m1", "room-dm-000001", laneUser, at(2))
	tr.RecordDelivery("u-2", "m1", "room-dm-000001", laneUser, at(2))
	tr.RecordDelivery("u-9", "m1", "room-dm-000001", laneUser, at(2))
	tr.RecordDelivery("u-9", "m1", "room-dm-000001", laneUser, at(3))

	vs := tr.Finalize()
	require.Len(t, vs, 1)
	assert.Equal(t, []string{"u-9"}, vs[0].Users, "same leaking user must be deduped")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=tools/loadgen`
Expected: FAIL. `TestProbeTracker_LeakageOnUserLane_IsViolation` fails with an empty violation list — Task 2 records no leakage. `TestProbeTracker_LeakageOnRoomLane_IsIgnored` passes already; that is the asymmetry guard and it must stay passing after Step 3.

- [ ] **Step 3: Write minimal implementation**

In `tools/loadgen/verify_probe.go`, add the `leaked` set to `probeRecord`:

```go
type probeRecord struct {
	msgID       string
	roomID      string
	senderID    string
	epoch       int
	publishedAt time.Time
	expected    map[string]struct{}
	received    map[deliveryKey]int
	leaked      map[string]struct{} // userIDs outside expected, user lane only
}
```

Initialise it in `RegisterProbe`:

```go
	rec := &probeRecord{
		msgID: msgID, roomID: roomID, senderID: senderID, epoch: epoch,
		publishedAt: at,
		expected:    exp,
		received:    make(map[deliveryKey]int, len(expected)),
		leaked:      make(map[string]struct{}),
	}
```

Record it in `RecordDelivery`, replacing the bare `return`:

```go
	if _, expected := rec.expected[userID]; !expected {
		// Leakage is only meaningful on the per-user lane, where the system
		// chooses the address. On the room lane, delivery follows from whoever
		// subscribed to the topic, which loadgen controls (spec §7.3).
		if ln == laneUser {
			rec.leaked[userID] = struct{}{}
		}
		return
	}
```

Add the `Leaked` field to `ProbeCounts` and accumulate it in `Counts`:

```go
type ProbeCounts struct {
	Tracked    int
	Suppressed int
	Complete   int
	Partial    int
	TotalLoss  int
	Leaked     int
}
```

```go
		c.Leaked += len(rec.leaked)
```

Emit the violation in `probeRecord.violations`, before the final `return out`:

```go
	if len(r.leaked) > 0 {
		leaked := make([]string, 0, len(r.leaked))
		for u := range r.leaked {
			leaked = append(leaked, u)
		}
		sort.Strings(leaked)
		out = append(out, Violation{
			Kind: KindUnexpectedRecipient, MsgID: r.msgID, RoomID: r.roomID,
			Users: leaked, Epoch: r.epoch, Detail: "delivered to non-member on the per-user lane",
		})
	}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=tools/loadgen`
Expected: PASS, including `TestProbeTracker_LeakageOnRoomLane_IsIgnored` — if that one now fails, the lane guard is wrong and leakage is being recorded on the room lane.

- [ ] **Step 5: Commit**

```bash
git add tools/loadgen/verify_probe.go tools/loadgen/verify_probe_test.go
git commit -m "feat(loadgen): leakage detection scoped to the per-user lane"
```

---

## Task 4: Receiver attribution in the direct pool

**Files:**
- Modify: `tools/loadgen/daily_pool.go:34-118`
- Test: `tools/loadgen/verify_pool_test.go` (create)

**Interfaces:**
- Consumes: `lane`, `ProbeTracker` from Task 2
- Produces: `deliverySink` interface, `directPool.attachSink(s deliverySink)`, `directPool.SubscribeRoom(userID, roomID string) error`

**Context:** Today `directPool.onBroadcast` (`daily_pool.go:98`) discards which user received an event. `directPool.Add` already holds `u` in closure scope at `daily_pool.go:59`, so attribution is a small threading change. `daily`'s behaviour must not change — with a nil sink the pool behaves exactly as today.

`model.RoomEvent` carries `RoomID` (`pkg/model/event.go:270`) and `LastMsgID` (`:279`).

- [ ] **Step 1: Write the failing test**

Create `tools/loadgen/verify_pool_test.go`:

```go
package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// recordingSink captures deliveries for assertions without a NATS connection.
type recordingSink struct {
	calls []sinkCall
}

type sinkCall struct {
	userID, msgID, roomID string
	ln                    lane
}

func (s *recordingSink) RecordDelivery(userID, msgID, roomID string, ln lane, _ time.Time) {
	s.calls = append(s.calls, sinkCall{userID, msgID, roomID, ln})
}

func TestLaneForSubject_Global(t *testing.T) {
	assert.Equal(t, laneGlobal, laneForSubject("chat.room.r-1.event"))
}

func TestLaneForSubject_User(t *testing.T) {
	assert.Equal(t, laneUser, laneForSubject("chat.user.alice.room.event"))
}

func TestDirectPool_NilSink_IsNoop(t *testing.T) {
	p := newDirectPool("nats://unused", "", nil)
	// Must not panic when no sink is attached — this is daily's path.
	p.deliver("u-1", []byte(`{"roomId":"r-1","lastMsgId":"m-1"}`), laneGlobal)
}

func TestDirectPool_Deliver_AttributesToUser(t *testing.T) {
	p := newDirectPool("nats://unused", "", nil)
	sink := &recordingSink{}
	p.attachSink(sink)

	p.deliver("u-1", []byte(`{"roomId":"room-small-000001","lastMsgId":"m-1"}`), laneGlobal)

	assert.Equal(t, []sinkCall{
		{userID: "u-1", msgID: "m-1", roomID: "room-small-000001", ln: laneGlobal},
	}, sink.calls)
}

func TestDirectPool_Deliver_SkipsEventsWithoutMessageID(t *testing.T) {
	p := newDirectPool("nats://unused", "", nil)
	sink := &recordingSink{}
	p.attachSink(sink)

	// Membership and rename events carry no lastMsgId.
	p.deliver("u-1", []byte(`{"roomId":"room-small-000001","type":"room_renamed"}`), laneGlobal)

	assert.Empty(t, sink.calls)
}

func TestDirectPool_Deliver_IgnoresMalformedPayload(t *testing.T) {
	p := newDirectPool("nats://unused", "", nil)
	sink := &recordingSink{}
	p.attachSink(sink)

	p.deliver("u-1", []byte(`{not json`), laneGlobal)

	assert.Empty(t, sink.calls)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=tools/loadgen`
Expected: FAIL — `undefined: laneForSubject`, `p.attachSink undefined`, `p.deliver undefined`.

- [ ] **Step 3: Write minimal implementation**

In `tools/loadgen/daily_pool.go`, add near the `directPool` type:

```go
// deliverySink receives per-recipient delivery attribution. Implemented by
// ProbeTracker. nil in daily runs, which keeps that path byte-for-byte
// identical to its previous behaviour.
type deliverySink interface {
	RecordDelivery(userID, msgID, roomID string, ln lane, at time.Time)
}

// laneForSubject classifies which subscription carried a message. The
// per-user lane is the only one where delivery is addressed rather than
// broadcast to a topic (spec §7.3).
func laneForSubject(subj string) lane {
	if strings.HasPrefix(subj, "chat.user.") {
		return laneUser
	}
	if strings.Contains(subj, ".local.") {
		return laneLocal
	}
	return laneGlobal
}

// attachSink wires a delivery sink. Must be called before Add.
func (p *directPool) attachSink(s deliverySink) {
	p.mu.Lock()
	p.sink = s
	p.mu.Unlock()
}

// deliver decodes a broadcast and attributes it to userID. Split out from the
// subscription callback so it is unit-testable without a NATS connection.
func (p *directPool) deliver(userID string, data []byte, ln lane) {
	p.mu.Lock()
	sink := p.sink
	p.mu.Unlock()
	if sink == nil {
		return
	}
	var evt model.RoomEvent
	if err := json.Unmarshal(data, &evt); err != nil {
		return // ignore malformed
	}
	if evt.LastMsgID == "" {
		return // membership/rename events carry no message
	}
	sink.RecordDelivery(userID, evt.LastMsgID, evt.RoomID, ln, time.Now())
}

// SubscribeRoom adds a room subscription to an already-connected user. Used
// when a reserve floater is added to a probe room mid-run — the same thing a
// real client does on joining.
func (p *directPool) SubscribeRoom(userID, roomID string) error {
	p.mu.Lock()
	du, ok := p.users[userID]
	p.mu.Unlock()
	if !ok {
		return fmt.Errorf("subscribe room %s: user %s not in direct pool", roomID, userID)
	}
	for _, global := range []bool{true, false} {
		subj := subject.RoomEvent(roomID, global)
		sub, err := du.nc.Subscribe(subj, func(m *nats.Msg) {
			p.deliver(userID, m.Data, laneForSubject(m.Subject))
		})
		if err != nil {
			return fmt.Errorf("subscribe room %s for %s: %w", roomID, userID, err)
		}
		p.mu.Lock()
		du.subs = append(du.subs, sub)
		p.mu.Unlock()
	}
	if err := du.nc.Flush(); err != nil {
		return fmt.Errorf("flush room subscription %s for %s: %w", roomID, userID, err)
	}
	return nil
}
```

Add the `sink` field to the `directPool` struct:

```go
type directPool struct {
	// ... existing fields ...
	sink deliverySink
}
```

Change the two subscription callbacks in `directPool.Add` to thread the user ID. At `daily_pool.go:59`:

```go
sub, err := nc.Subscribe(subject.RoomEvent(roomID, global), func(m *nats.Msg) {
	p.onBroadcast(m)
	p.deliver(u.ID, m.Data, laneForSubject(m.Subject))
})
```

And at `daily_pool.go:70`:

```go
userSub, err := nc.Subscribe(subject.UserRoomEvent(u.Account), func(m *nats.Msg) {
	p.onBroadcast(m)
	p.deliver(u.ID, m.Data, laneForSubject(m.Subject))
})
```

`onBroadcast` is left untouched so `daily`'s latency correlation is unchanged.

Ensure `strings` is imported in `daily_pool.go`.

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=tools/loadgen`
Expected: PASS. All existing `daily_pool_test.go` tests must still pass — a failure there means the change was not additive.

- [ ] **Step 5: Commit**

```bash
git add tools/loadgen/daily_pool.go tools/loadgen/verify_pool_test.go
git commit -m "feat(loadgen): per-recipient delivery attribution in the direct pool"
```

---

## Task 5: Deterministic probe selection

**Files:**
- Modify: `tools/loadgen/verify_probe.go`
- Test: `tools/loadgen/verify_probe_test.go`

**Interfaces:**
- Consumes: nothing new
- Produces: `shouldProbe(seed int64, userIdx int, seqNo uint64, rate float64) bool`

**Context:** Selection must be decided at publish time, be reproducible from the seed, and never touch `rand` or wall-clock on the hot path (spec §5).

- [ ] **Step 1: Write the failing test**

Append to `tools/loadgen/verify_probe_test.go`:

```go
func TestShouldProbe_Deterministic(t *testing.T) {
	for seq := uint64(0); seq < 100; seq++ {
		a := shouldProbe(42, 7, seq, 0.5)
		b := shouldProbe(42, 7, seq, 0.5)
		assert.Equal(t, a, b, "same inputs must give the same answer at seq %d", seq)
	}
}

func TestShouldProbe_RateZeroNeverProbes(t *testing.T) {
	for seq := uint64(0); seq < 1000; seq++ {
		assert.False(t, shouldProbe(42, 1, seq, 0))
	}
}

func TestShouldProbe_RateOneAlwaysProbes(t *testing.T) {
	for seq := uint64(0); seq < 1000; seq++ {
		assert.True(t, shouldProbe(42, 1, seq, 1.0))
	}
}

func TestShouldProbe_ApproximatesRate(t *testing.T) {
	const n = 100000
	hits := 0
	for seq := uint64(0); seq < n; seq++ {
		if shouldProbe(42, 3, seq, 0.01) {
			hits++
		}
	}
	// 1% of 100k is 1000; allow generous slack for hash distribution.
	assert.InDelta(t, 1000, hits, 200, "observed rate %d/%d strays from 1%%", hits, n)
}

func TestShouldProbe_DiffersAcrossUsers(t *testing.T) {
	// Adjacent user indices must not produce identical probe streams,
	// otherwise probes cluster on the same senders.
	same := 0
	for seq := uint64(0); seq < 1000; seq++ {
		if shouldProbe(42, 1, seq, 0.1) == shouldProbe(42, 2, seq, 0.1) {
			same++
		}
	}
	assert.Less(t, same, 1000, "user 1 and user 2 produced identical probe streams")
}

func TestShouldProbe_DiffersAcrossSeeds(t *testing.T) {
	diff := 0
	for seq := uint64(0); seq < 1000; seq++ {
		if shouldProbe(1, 5, seq, 0.1) != shouldProbe(2, 5, seq, 0.1) {
			diff++
		}
	}
	assert.Positive(t, diff, "different seeds must produce different probe sets")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=tools/loadgen`
Expected: FAIL — `undefined: shouldProbe`.

- [ ] **Step 3: Write minimal implementation**

Append to `tools/loadgen/verify_probe.go`:

```go
// probeResolution is the denominator for the rate comparison. 1e6 gives
// six significant digits of rate granularity, far finer than any useful
// --probe-rate.
const probeResolution = 1_000_000

// shouldProbe decides deterministically whether one send is tracked.
// Pure function of (seed, userIdx, seqNo) — no rand, no wall-clock — so two
// runs with the same seed select exactly the same probe set.
func shouldProbe(seed int64, userIdx int, seqNo uint64, rate float64) bool {
	if rate <= 0 {
		return false
	}
	if rate >= 1 {
		return true
	}
	h := fnv.New64a()
	var buf [24]byte
	binary.LittleEndian.PutUint64(buf[0:8], uint64(seed))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(userIdx))
	binary.LittleEndian.PutUint64(buf[16:24], seqNo)
	_, _ = h.Write(buf[:])
	return h.Sum64()%probeResolution < uint64(rate*probeResolution)
}
```

Add imports `encoding/binary` and `hash/fnv` to `verify_probe.go`.

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=tools/loadgen`
Expected: PASS, 6 new tests.

- [ ] **Step 5: Commit**

```bash
git add tools/loadgen/verify_probe.go tools/loadgen/verify_probe_test.go
git commit -m "feat(loadgen): deterministic probe selection"
```

---

## Task 6: Membership epochs and the settle window

**Files:**
- Create: `tools/loadgen/verify_membership.go`
- Test: `tools/loadgen/verify_membership_test.go`

**Interfaces:**
- Consumes: `Violation`, `ViolationKind` (Task 1)
- Produces: `NewMembershipModel(prs ProbeRoomSet) *MembershipModel`, `.Epoch(roomID string) int`, `.Members(roomID string) []string`, `.MembersAtEpoch(roomID string, epoch int) []string`, `.ApplyAdd(roomID, userID string, now time.Time)`, `.ApplyRemove(roomID, userID string, now time.Time)`, `.InSettle(roomID string, now time.Time) bool`, `.SetSettle(d time.Duration)`, `.RecordOracle(roomID string, observed []string, epoch int)`, `.RecordSendResult(roomID, userID string, accepted bool, epoch int)`, `.Counts() ChangeCounts`, `.Finalize() []Violation`

**Context:** The expected set becomes time-varying. A change bumps the room's epoch and opens a settle window during which no probes are sent into that room. Probes carry their epoch and are judged against the set in force when published (spec §9.2). Membership is tracked from two oracles — loadgen's model and `subscription.list` — because a lost write would make the system's state and its self-report agree (spec §9.3).

- [ ] **Step 1: Write the failing test**

```go
package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func modelForTest() *MembershipModel {
	prs := ProbeRoomSet{
		byRoom: map[string][]string{
			"room-small-000001": {"u-1", "u-2", "u-3"},
			"room-dm-000001":    {"u-1", "u-2"},
		},
	}
	m := NewMembershipModel(prs)
	m.SetSettle(5 * time.Second)
	return m
}

func TestMembershipModel_InitialEpochIsZero(t *testing.T) {
	m := modelForTest()
	assert.Equal(t, 0, m.Epoch("room-small-000001"))
	assert.Equal(t, []string{"u-1", "u-2", "u-3"}, m.Members("room-small-000001"))
}

func TestMembershipModel_AddBumpsEpochAndMembers(t *testing.T) {
	m := modelForTest()
	m.ApplyAdd("room-small-000001", "u-9", at(10))

	assert.Equal(t, 1, m.Epoch("room-small-000001"))
	assert.Equal(t, []string{"u-1", "u-2", "u-3", "u-9"}, m.Members("room-small-000001"))
}

func TestMembershipModel_RemoveBumpsEpochAndMembers(t *testing.T) {
	m := modelForTest()
	m.ApplyRemove("room-small-000001", "u-2", at(10))

	assert.Equal(t, 1, m.Epoch("room-small-000001"))
	assert.Equal(t, []string{"u-1", "u-3"}, m.Members("room-small-000001"))
}

func TestMembershipModel_ChangeIsScopedToOneRoom(t *testing.T) {
	m := modelForTest()
	m.ApplyAdd("room-small-000001", "u-9", at(10))

	assert.Equal(t, 0, m.Epoch("room-dm-000001"), "unrelated room epoch must not move")
	assert.Equal(t, []string{"u-1", "u-2"}, m.Members("room-dm-000001"))
}

func TestMembershipModel_SettleWindowOpensAndCloses(t *testing.T) {
	m := modelForTest()
	m.ApplyAdd("room-small-000001", "u-9", at(10))

	assert.True(t, m.InSettle("room-small-000001", at(11)), "inside the 5s window")
	assert.True(t, m.InSettle("room-small-000001", at(14)))
	assert.False(t, m.InSettle("room-small-000001", at(15)), "at the boundary the window is closed")
	assert.False(t, m.InSettle("room-small-000001", at(20)))
}

func TestMembershipModel_SettleIsPerRoom(t *testing.T) {
	m := modelForTest()
	m.ApplyAdd("room-small-000001", "u-9", at(10))

	assert.False(t, m.InSettle("room-dm-000001", at(11)),
		"a change in one room must not suspend probing in another")
}

func TestMembershipModel_OracleAgreement_NoViolation(t *testing.T) {
	m := modelForTest()
	m.ApplyAdd("room-small-000001", "u-9", at(10))
	m.RecordOracle("room-small-000001", []string{"u-1", "u-2", "u-3", "u-9"}, 1)

	assert.Empty(t, m.Finalize())
}

func TestMembershipModel_OracleMissingAdd_IsViolation(t *testing.T) {
	m := modelForTest()
	m.ApplyAdd("room-small-000001", "u-9", at(10))
	// subscription.list never picked up the add.
	m.RecordOracle("room-small-000001", []string{"u-1", "u-2", "u-3"}, 1)

	vs := m.Finalize()
	require.Len(t, vs, 1)
	assert.Equal(t, KindMembershipNotApplied, vs[0].Kind)
	assert.Equal(t, "room-small-000001", vs[0].RoomID)
	assert.Equal(t, []string{"u-9"}, vs[0].Users)
}

func TestMembershipModel_OracleStaleRemove_IsViolation(t *testing.T) {
	m := modelForTest()
	m.ApplyRemove("room-small-000001", "u-2", at(10))
	// subscription.list still reports the removed member.
	m.RecordOracle("room-small-000001", []string{"u-1", "u-2", "u-3"}, 1)

	vs := m.Finalize()
	require.Len(t, vs, 1)
	assert.Equal(t, KindMembershipNotApplied, vs[0].Kind)
	assert.Equal(t, []string{"u-2"}, vs[0].Users)
}

func TestMembershipModel_AddedMemberSendRejected_IsViolation(t *testing.T) {
	m := modelForTest()
	m.ApplyAdd("room-small-000001", "u-9", at(10))
	m.RecordOracle("room-small-000001", []string{"u-1", "u-2", "u-3", "u-9"}, 1)
	m.RecordSendResult("room-small-000001", "u-9", false, 1)

	vs := m.Finalize()
	require.Len(t, vs, 1)
	assert.Equal(t, KindMembershipAddIneffective, vs[0].Kind)
}

func TestMembershipModel_RemovedMemberSendAccepted_IsViolation(t *testing.T) {
	m := modelForTest()
	m.ApplyRemove("room-small-000001", "u-2", at(10))
	m.RecordOracle("room-small-000001", []string{"u-1", "u-3"}, 1)
	m.RecordSendResult("room-small-000001", "u-2", true, 1)

	vs := m.Finalize()
	require.Len(t, vs, 1)
	assert.Equal(t, KindMembershipRemoveIneffective, vs[0].Kind,
		"a removed member whose send is still accepted means stale membership on the write path")
}

func TestMembershipModel_EffectiveChanges_NoViolation(t *testing.T) {
	m := modelForTest()
	m.ApplyAdd("room-small-000001", "u-9", at(10))
	m.RecordOracle("room-small-000001", []string{"u-1", "u-2", "u-3", "u-9"}, 1)
	m.RecordSendResult("room-small-000001", "u-9", true, 1)

	m.ApplyRemove("room-dm-000001", "u-2", at(20))
	m.RecordOracle("room-dm-000001", []string{"u-1"}, 1)
	m.RecordSendResult("room-dm-000001", "u-2", false, 1)

	assert.Empty(t, m.Finalize())
}

func TestMembershipModel_MembersAtEpoch_ReturnsHistoricalSet(t *testing.T) {
	m := modelForTest()
	before := m.Members("room-small-000001")
	m.ApplyAdd("room-small-000001", "u-9", at(10))

	// A probe published at epoch 0 must be judged against epoch 0's set even
	// after the epoch advances.
	assert.Equal(t, before, m.MembersAtEpoch("room-small-000001", 0))
	assert.Equal(t, []string{"u-1", "u-2", "u-3", "u-9"}, m.MembersAtEpoch("room-small-000001", 1))
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=tools/loadgen`
Expected: FAIL — `undefined: NewMembershipModel`.

- [ ] **Step 3: Write minimal implementation**

Create `tools/loadgen/verify_membership.go`:

```go
package main

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// changeKind distinguishes an add from a remove.
type changeKind int

const (
	changeAdd changeKind = iota
	changeRemove
)

// membershipChange is one applied add/remove plus the observations made about
// it after its settle window.
type membershipChange struct {
	kind   changeKind
	roomID string
	userID string
	epoch  int
	at     time.Time

	oracleSeen    bool // subscription.list was queried for this epoch
	oracleHasUser bool // and reported the user as a member

	sendSeen     bool // a send was attempted by the target after settle
	sendAccepted bool
}

// roomState tracks one probe room's membership history.
type roomState struct {
	epoch     int
	members   map[string]struct{}
	history   [][]string // index = epoch, value = sorted members at that epoch
	settleEnd time.Time
}

// MembershipModel is loadgen's own model of probe-room membership — the oracle
// for what membership *should* be. It is compared against subscription.list
// (what the system thinks) so a lost write cannot hide behind a self-report
// that agrees with it (spec §9.3).
type MembershipModel struct {
	mu      sync.Mutex
	rooms   map[string]*roomState
	changes []*membershipChange
	settle  time.Duration
}

// NewMembershipModel seeds the model from the probe-room set at epoch 0.
func NewMembershipModel(prs ProbeRoomSet) *MembershipModel {
	m := &MembershipModel{rooms: make(map[string]*roomState), settle: 5 * time.Second}
	for roomID, members := range prs.byRoom {
		set := make(map[string]struct{}, len(members))
		for _, u := range members {
			set[u] = struct{}{}
		}
		initial := append([]string(nil), members...)
		sort.Strings(initial)
		m.rooms[roomID] = &roomState{members: set, history: [][]string{initial}}
	}
	return m
}

// SetSettle configures the post-change quiet window.
func (m *MembershipModel) SetSettle(d time.Duration) {
	m.mu.Lock()
	m.settle = d
	m.mu.Unlock()
}

// Epoch returns the room's current membership epoch.
func (m *MembershipModel) Epoch(roomID string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	if rs, ok := m.rooms[roomID]; ok {
		return rs.epoch
	}
	return 0
}

// Members returns the room's current membership, sorted.
func (m *MembershipModel) Members(roomID string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	rs, ok := m.rooms[roomID]
	if !ok {
		return nil
	}
	return append([]string(nil), rs.history[rs.epoch]...)
}

// MembersAtEpoch returns the membership in force at a past epoch, so a probe
// delivered after a change is still judged against the set that applied when
// it was published (spec §9.2).
func (m *MembershipModel) MembersAtEpoch(roomID string, epoch int) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	rs, ok := m.rooms[roomID]
	if !ok || epoch < 0 || epoch >= len(rs.history) {
		return nil
	}
	return append([]string(nil), rs.history[epoch]...)
}

// InSettle reports whether the room is inside its post-change quiet window.
func (m *MembershipModel) InSettle(roomID string, now time.Time) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	rs, ok := m.rooms[roomID]
	if !ok {
		return false
	}
	return now.Before(rs.settleEnd)
}

// ApplyAdd records that loadgen added userID to roomID.
func (m *MembershipModel) ApplyAdd(roomID, userID string, now time.Time) {
	m.apply(changeAdd, roomID, userID, now)
}

// ApplyRemove records that loadgen removed userID from roomID.
func (m *MembershipModel) ApplyRemove(roomID, userID string, now time.Time) {
	m.apply(changeRemove, roomID, userID, now)
}

func (m *MembershipModel) apply(kind changeKind, roomID, userID string, now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	rs, ok := m.rooms[roomID]
	if !ok {
		return
	}
	if kind == changeAdd {
		rs.members[userID] = struct{}{}
	} else {
		delete(rs.members, userID)
	}

	next := make([]string, 0, len(rs.members))
	for u := range rs.members {
		next = append(next, u)
	}
	sort.Strings(next)

	rs.epoch++
	rs.history = append(rs.history, next)
	rs.settleEnd = now.Add(m.settle)

	m.changes = append(m.changes, &membershipChange{
		kind: kind, roomID: roomID, userID: userID, epoch: rs.epoch, at: now,
	})
}

// RecordOracle records what subscription.list reported for a room at an epoch.
func (m *MembershipModel) RecordOracle(roomID string, observed []string, epoch int) {
	seen := make(map[string]struct{}, len(observed))
	for _, u := range observed {
		seen[u] = struct{}{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, c := range m.changes {
		if c.roomID != roomID || c.epoch != epoch {
			continue
		}
		c.oracleSeen = true
		_, c.oracleHasUser = seen[c.userID]
	}
}

// RecordSendResult records whether the changed user's post-settle send into the
// room was accepted by the gatekeeper.
func (m *MembershipModel) RecordSendResult(roomID, userID string, accepted bool, epoch int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, c := range m.changes {
		if c.roomID == roomID && c.userID == userID && c.epoch == epoch {
			c.sendSeen = true
			c.sendAccepted = accepted
		}
	}
}

// ChangeCounts summarises churn for the report.
type ChangeCounts struct {
	Total, Adds, Removes, Applied, Effective int
}

// Counts returns churn summary counters.
func (m *MembershipModel) Counts() ChangeCounts {
	m.mu.Lock()
	defer m.mu.Unlock()
	var c ChangeCounts
	for _, ch := range m.changes {
		c.Total++
		if ch.kind == changeAdd {
			c.Adds++
		} else {
			c.Removes++
		}
		if ch.oracleSeen && ch.oracleHasUser == (ch.kind == changeAdd) {
			c.Applied++
		}
		if ch.sendSeen && ch.sendAccepted == (ch.kind == changeAdd) {
			c.Effective++
		}
	}
	return c
}

// Finalize evaluates every recorded change and returns its violations.
func (m *MembershipModel) Finalize() []Violation {
	m.mu.Lock()
	defer m.mu.Unlock()

	var out []Violation
	for _, c := range m.changes {
		wantMember := c.kind == changeAdd

		if c.oracleSeen && c.oracleHasUser != wantMember {
			out = append(out, Violation{
				Kind: KindMembershipNotApplied, RoomID: c.roomID,
				Users: []string{c.userID}, Epoch: c.epoch,
				Detail: fmt.Sprintf("subscription.list membership=%t after %s",
					c.oracleHasUser, changeName(c.kind)),
			})
		}

		if c.sendSeen && c.sendAccepted != wantMember {
			kind := KindMembershipRemoveIneffective
			detail := "send still accepted after remove"
			if wantMember {
				kind = KindMembershipAddIneffective
				detail = "send still rejected after add"
			}
			out = append(out, Violation{
				Kind: kind, RoomID: c.roomID, Users: []string{c.userID},
				Epoch: c.epoch, Detail: detail,
			})
		}
	}
	return out
}

func changeName(k changeKind) string {
	if k == changeAdd {
		return "add"
	}
	return "remove"
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=tools/loadgen`
Expected: PASS, 13 tests.

- [ ] **Step 5: Commit**

```bash
git add tools/loadgen/verify_membership.go tools/loadgen/verify_membership_test.go
git commit -m "feat(loadgen): membership epochs, settle windows, and dual-oracle comparison"
```

---

## Task 7: Persistence readback

**Files:**
- Create: `tools/loadgen/verify_readback.go`
- Test: `tools/loadgen/verify_readback_test.go`

**Interfaces:**
- Consumes: `Violation`, `ViolationKind` (Task 1); `requestFn` from `daily_actions.go:26`
- Produces: `ReadbackTarget` struct, `NewReadback(req requestFn, siteID string, attempts int, backoff time.Duration) *Readback`, `.Verify(ctx context.Context, account string, targets []ReadbackTarget) ([]Violation, error)`

**Context:** Reads through `history-service` via `subject.MsgGet(account, roomID, siteID)` rather than direct CQL, to avoid coupling to `MESSAGE_BUCKET_HOURS` (spec §8). `message-worker` writes asynchronously, so storage lag is not storage loss — missing messages retry with bounded backoff. A query error is INCONCLUSIVE, not FAIL.

- [ ] **Step 1: Write the failing test**

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// historyReply is the shape verify decodes from history-service. Only the
// fields the readback asserts on are declared.
func historyReply(t *testing.T, msgs ...map[string]string) []byte {
	t.Helper()
	list := make([]map[string]string, 0, len(msgs))
	list = append(list, msgs...)
	b, err := json.Marshal(map[string]any{"messages": list})
	require.NoError(t, err)
	return b
}

func msg(id, roomID, sender, parent string) map[string]string {
	return map[string]string{"id": id, "roomId": roomID, "userId": sender, "threadParentMessageId": parent}
}

func TestReadback_AllPresent_NoViolations(t *testing.T) {
	req := func(_ context.Context, _ string, _ []byte, _ time.Duration) ([]byte, error) {
		return historyReply(t, msg("m1", "r1", "u-1", "")), nil
	}
	rb := NewReadback(req, "site-test", 3, time.Millisecond)

	vs, err := rb.Verify(context.Background(), "user-1", []ReadbackTarget{
		{MsgID: "m1", RoomID: "r1", SenderID: "u-1"},
	})
	require.NoError(t, err)
	assert.Empty(t, vs)
}

func TestReadback_Missing_AfterRetries_IsPersistenceMiss(t *testing.T) {
	calls := 0
	req := func(_ context.Context, _ string, _ []byte, _ time.Duration) ([]byte, error) {
		calls++
		return historyReply(t), nil
	}
	rb := NewReadback(req, "site-test", 3, time.Millisecond)

	vs, err := rb.Verify(context.Background(), "user-1", []ReadbackTarget{
		{MsgID: "m1", RoomID: "r1", SenderID: "u-1"},
	})
	require.NoError(t, err)
	require.Len(t, vs, 1)
	assert.Equal(t, KindPersistenceMiss, vs[0].Kind)
	assert.Equal(t, 3, calls, "must exhaust the configured attempts before declaring loss")
}

func TestReadback_LateArrival_IsNotAViolation(t *testing.T) {
	calls := 0
	req := func(_ context.Context, _ string, _ []byte, _ time.Duration) ([]byte, error) {
		calls++
		if calls < 2 {
			return historyReply(t), nil // message-worker hasn't caught up yet
		}
		return historyReply(t, msg("m1", "r1", "u-1", "")), nil
	}
	rb := NewReadback(req, "site-test", 3, time.Millisecond)

	vs, err := rb.Verify(context.Background(), "user-1", []ReadbackTarget{
		{MsgID: "m1", RoomID: "r1", SenderID: "u-1"},
	})
	require.NoError(t, err)
	assert.Empty(t, vs, "storage lag is not storage loss")
}

func TestReadback_WrongRoom_IsMismatch(t *testing.T) {
	req := func(_ context.Context, _ string, _ []byte, _ time.Duration) ([]byte, error) {
		return historyReply(t, msg("m1", "WRONG", "u-1", "")), nil
	}
	rb := NewReadback(req, "site-test", 3, time.Millisecond)

	vs, err := rb.Verify(context.Background(), "user-1", []ReadbackTarget{
		{MsgID: "m1", RoomID: "r1", SenderID: "u-1"},
	})
	require.NoError(t, err)
	require.Len(t, vs, 1)
	assert.Equal(t, KindPersistenceMismatch, vs[0].Kind)
}

func TestReadback_WrongSender_IsMismatch(t *testing.T) {
	req := func(_ context.Context, _ string, _ []byte, _ time.Duration) ([]byte, error) {
		return historyReply(t, msg("m1", "r1", "SOMEONE-ELSE", "")), nil
	}
	rb := NewReadback(req, "site-test", 3, time.Millisecond)

	vs, err := rb.Verify(context.Background(), "user-1", []ReadbackTarget{
		{MsgID: "m1", RoomID: "r1", SenderID: "u-1"},
	})
	require.NoError(t, err)
	require.Len(t, vs, 1)
	assert.Equal(t, KindPersistenceMismatch, vs[0].Kind)
}

func TestReadback_WrongThreadParent_IsMismatch(t *testing.T) {
	req := func(_ context.Context, _ string, _ []byte, _ time.Duration) ([]byte, error) {
		return historyReply(t, msg("m1", "r1", "u-1", "OTHER-PARENT")), nil
	}
	rb := NewReadback(req, "site-test", 3, time.Millisecond)

	vs, err := rb.Verify(context.Background(), "user-1", []ReadbackTarget{
		{MsgID: "m1", RoomID: "r1", SenderID: "u-1", ThreadParentID: "p1"},
	})
	require.NoError(t, err)
	require.Len(t, vs, 1)
	assert.Equal(t, KindPersistenceMismatch, vs[0].Kind)
}

func TestReadback_QueryError_ReturnsErrorNotViolation(t *testing.T) {
	req := func(_ context.Context, _ string, _ []byte, _ time.Duration) ([]byte, error) {
		return nil, errors.New("timeout")
	}
	rb := NewReadback(req, "site-test", 2, time.Millisecond)

	vs, err := rb.Verify(context.Background(), "user-1", []ReadbackTarget{
		{MsgID: "m1", RoomID: "r1", SenderID: "u-1"},
	})
	require.Error(t, err, "an unreachable history-service is INCONCLUSIVE, never FAIL")
	assert.Empty(t, vs)
}

func TestReadback_BatchesByRoom(t *testing.T) {
	queried := map[string]int{}
	req := func(_ context.Context, subj string, _ []byte, _ time.Duration) ([]byte, error) {
		queried[subj]++
		return historyReply(t,
			msg("m1", "r1", "u-1", ""),
			msg("m2", "r1", "u-1", ""),
			msg("m3", "r1", "u-1", ""),
		), nil
	}
	rb := NewReadback(req, "site-test", 3, time.Millisecond)

	vs, err := rb.Verify(context.Background(), "user-1", []ReadbackTarget{
		{MsgID: "m1", RoomID: "r1", SenderID: "u-1"},
		{MsgID: "m2", RoomID: "r1", SenderID: "u-1"},
		{MsgID: "m3", RoomID: "r1", SenderID: "u-1"},
	})
	require.NoError(t, err)
	assert.Empty(t, vs)
	assert.Len(t, queried, 1, "three probes in one room must cost one query")
}

func TestReadback_ContextCancelled_Aborts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	req := func(c context.Context, _ string, _ []byte, _ time.Duration) ([]byte, error) {
		return nil, c.Err()
	}
	rb := NewReadback(req, "site-test", 3, time.Millisecond)

	_, err := rb.Verify(ctx, "user-1", []ReadbackTarget{{MsgID: "m1", RoomID: "r1", SenderID: "u-1"}})
	require.Error(t, err)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=tools/loadgen`
Expected: FAIL — `undefined: NewReadback`.

- [ ] **Step 3: Write minimal implementation**

Create `tools/loadgen/verify_readback.go`:

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/hmchangw/chat/pkg/subject"
)

// ReadbackTarget is one probe to confirm in history.
type ReadbackTarget struct {
	MsgID          string
	RoomID         string
	SenderID       string
	ThreadParentID string
}

// historyMessage is the narrow projection verify decodes from history-service.
// Content is deliberately absent — verify asserts metadata only, which keeps
// it clear of encryption and of the no-logging-bodies rule (spec §8).
type historyMessage struct {
	ID                    string `json:"id"`
	RoomID                string `json:"roomId"`
	UserID                string `json:"userId"`
	ThreadParentMessageID string `json:"threadParentMessageId"`
}

type historyResponse struct {
	Messages []historyMessage `json:"messages"`
}

// Readback confirms probes persisted, via the history-service RPC rather than
// direct CQL — this exercises the real client read path and avoids coupling the
// test to MESSAGE_BUCKET_HOURS.
type Readback struct {
	req      requestFn
	siteID   string
	attempts int
	backoff  time.Duration
}

// NewReadback returns a Readback with a bounded retry budget.
func NewReadback(req requestFn, siteID string, attempts int, backoff time.Duration) *Readback {
	if attempts < 1 {
		attempts = 1
	}
	return &Readback{req: req, siteID: siteID, attempts: attempts, backoff: backoff}
}

// Verify checks every target. Returns violations for probes that are genuinely
// absent or stored wrong. Returns an error — never a violation — when the query
// itself fails, since an unreachable service says nothing about the write.
func (r *Readback) Verify(ctx context.Context, account string, targets []ReadbackTarget) ([]Violation, error) {
	byRoom := map[string][]ReadbackTarget{}
	for _, t := range targets {
		byRoom[t.RoomID] = append(byRoom[t.RoomID], t)
	}

	roomIDs := make([]string, 0, len(byRoom))
	for id := range byRoom {
		roomIDs = append(roomIDs, id)
	}
	sort.Strings(roomIDs)

	var out []Violation
	for _, roomID := range roomIDs {
		vs, err := r.verifyRoom(ctx, account, roomID, byRoom[roomID])
		if err != nil {
			return nil, fmt.Errorf("readback room %s: %w", roomID, err)
		}
		out = append(out, vs...)
	}
	return out, nil
}

// verifyRoom queries one room and retries only while something is still missing.
func (r *Readback) verifyRoom(ctx context.Context, account, roomID string, targets []ReadbackTarget) ([]Violation, error) {
	pending := make(map[string]ReadbackTarget, len(targets))
	for _, t := range targets {
		pending[t.MsgID] = t
	}

	var mismatches []Violation

	for attempt := 0; attempt < r.attempts && len(pending) > 0; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("readback cancelled: %w", ctx.Err())
			case <-time.After(r.backoff):
			}
		}

		raw, err := r.req(ctx, subject.MsgGet(account, roomID, r.siteID), nil, defaultRequestTimeout)
		if err != nil {
			return nil, fmt.Errorf("history query: %w", err)
		}
		var resp historyResponse
		if err := json.Unmarshal(raw, &resp); err != nil {
			return nil, fmt.Errorf("decode history response: %w", err)
		}

		for _, hm := range resp.Messages {
			want, ok := pending[hm.ID]
			if !ok {
				continue
			}
			delete(pending, hm.ID)
			if v, bad := compareStored(want, hm); bad {
				mismatches = append(mismatches, v)
			}
		}
	}

	missing := make([]string, 0, len(pending))
	for id := range pending {
		missing = append(missing, id)
	}
	sort.Strings(missing)
	for _, id := range missing {
		mismatches = append(mismatches, Violation{
			Kind: KindPersistenceMiss, MsgID: id, RoomID: roomID,
			Detail: fmt.Sprintf("absent from history after %d attempts", r.attempts),
		})
	}
	return mismatches, nil
}

// compareStored checks the stored metadata against what was published.
func compareStored(want ReadbackTarget, got historyMessage) (Violation, bool) {
	var reason string
	switch {
	case got.RoomID != want.RoomID:
		reason = fmt.Sprintf("roomId=%s want %s", got.RoomID, want.RoomID)
	case got.UserID != want.SenderID:
		reason = fmt.Sprintf("userId=%s want %s", got.UserID, want.SenderID)
	case got.ThreadParentMessageID != want.ThreadParentID:
		reason = fmt.Sprintf("threadParentMessageId=%s want %s",
			got.ThreadParentMessageID, want.ThreadParentID)
	default:
		return Violation{}, false
	}
	return Violation{
		Kind: KindPersistenceMismatch, MsgID: want.MsgID, RoomID: want.RoomID,
		Detail: reason,
	}, true
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=tools/loadgen`
Expected: PASS, 9 tests.

- [ ] **Step 5: Commit**

```bash
git add tools/loadgen/verify_readback.go tools/loadgen/verify_readback_test.go
git commit -m "feat(loadgen): persistence readback through history-service"
```

---

## Task 8: Verdict evaluation

**Files:**
- Create: `tools/loadgen/verify_verdict.go`
- Test: `tools/loadgen/verify_verdict_test.go`

**Interfaces:**
- Consumes: `Violation` (Task 1), `ProbeCounts` (Task 2), `ChangeCounts` (Task 6)
- Produces: `Verdict` type + `VerdictPass`/`VerdictFail`/`VerdictInconclusive`, `VerifyInputs` struct, `evaluateVerify(in VerifyInputs) VerifyResult`, `VerifyResult` struct, `(Verdict).ExitCode() int`

**Context:** INCONCLUSIVE overrides PASS and FAIL — it means the signals cannot be trusted, so reporting either would be a lie. Exit codes: 0 PASS, 1 FAIL, 2 INCONCLUSIVE (spec §4, §10).

- [ ] **Step 1: Write the failing test**

```go
package main

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func passingInputs() VerifyInputs {
	return VerifyInputs{
		Counts:     ProbeCounts{Tracked: 100, Complete: 100},
		MinProbes:  50,
		GCPauseP99: 5,
		GCPauseMax: 50,
	}
}

func TestEvaluateVerify_Clean_IsPass(t *testing.T) {
	r := evaluateVerify(passingInputs())
	assert.Equal(t, VerdictPass, r.Verdict)
	assert.Empty(t, r.Reasons)
	assert.Equal(t, 0, r.Verdict.ExitCode())
}

func TestEvaluateVerify_AnyViolation_IsFail(t *testing.T) {
	in := passingInputs()
	in.Violations = []Violation{{Kind: KindMissingRecipient, MsgID: "m1"}}

	r := evaluateVerify(in)
	assert.Equal(t, VerdictFail, r.Verdict)
	assert.Equal(t, 1, r.Verdict.ExitCode())
}

func TestEvaluateVerify_MembershipViolation_IsFail(t *testing.T) {
	in := passingInputs()
	in.Violations = []Violation{{Kind: KindMembershipRemoveIneffective, RoomID: "r1"}}

	assert.Equal(t, VerdictFail, evaluateVerify(in).Verdict)
}

func TestEvaluateVerify_MultiplexDrop_IsInconclusive(t *testing.T) {
	in := passingInputs()
	in.MultiplexDrops = 1

	r := evaluateVerify(in)
	assert.Equal(t, VerdictInconclusive, r.Verdict)
	assert.Contains(t, r.Reasons[0], "multiplex drop")
	assert.Equal(t, 2, r.Verdict.ExitCode())
}

func TestEvaluateVerify_InconclusiveOverridesFail(t *testing.T) {
	in := passingInputs()
	in.Violations = []Violation{{Kind: KindMissingRecipient, MsgID: "m1"}}
	in.MultiplexDrops = 1

	assert.Equal(t, VerdictInconclusive, evaluateVerify(in).Verdict,
		"a drop means the missing delivery cannot be attributed to the system")
}

func TestEvaluateVerify_TooFewProbes_IsInconclusive(t *testing.T) {
	in := passingInputs()
	in.Counts = ProbeCounts{Tracked: 10, Complete: 10}

	r := evaluateVerify(in)
	assert.Equal(t, VerdictInconclusive, r.Verdict)
	assert.Contains(t, r.Reasons[0], "min-probes")
}

func TestEvaluateVerify_SuppressedProbesDoNotCountTowardFloor(t *testing.T) {
	in := passingInputs()
	in.Counts = ProbeCounts{Tracked: 10, Complete: 10, Suppressed: 500}

	assert.Equal(t, VerdictInconclusive, evaluateVerify(in).Verdict,
		"suppressed probes must not disguise a starved run as covered")
}

func TestEvaluateVerify_ReadbackError_IsInconclusive(t *testing.T) {
	in := passingInputs()
	in.ReadbackErr = errors.New("history-service unreachable")

	r := evaluateVerify(in)
	assert.Equal(t, VerdictInconclusive, r.Verdict)
	assert.Contains(t, r.Reasons[0], "readback")
}

func TestEvaluateVerify_OracleError_IsInconclusive(t *testing.T) {
	in := passingInputs()
	in.OracleErr = errors.New("subscription.list timeout")

	assert.Equal(t, VerdictInconclusive, evaluateVerify(in).Verdict)
}

func TestEvaluateVerify_RecipientDropped_IsInconclusive(t *testing.T) {
	in := passingInputs()
	in.DroppedRecipients = 1

	r := evaluateVerify(in)
	assert.Equal(t, VerdictInconclusive, r.Verdict)
	assert.Contains(t, r.Reasons[0], "connection dropped")
}

func TestEvaluateVerify_EpochChangeAloneIsNotInconclusive(t *testing.T) {
	in := passingInputs()
	in.Changes = ChangeCounts{Total: 12, Adds: 7, Removes: 5, Applied: 12, Effective: 12}

	assert.Equal(t, VerdictPass, evaluateVerify(in).Verdict,
		"membership churn legitimately alters the expected set")
}

func TestEvaluateVerify_ContextCancelled_IsInconclusive(t *testing.T) {
	in := passingInputs()
	in.Cancelled = true

	assert.Equal(t, VerdictInconclusive, evaluateVerify(in).Verdict)
}

func TestEvaluateVerify_GCPressure_IsInconclusive(t *testing.T) {
	in := passingInputs()
	in.GCPauseP99 = 120

	r := evaluateVerify(in)
	assert.Equal(t, VerdictInconclusive, r.Verdict)
	assert.Contains(t, r.Reasons[0], "GC")
}

func TestEvaluateVerify_AllReasonsCollected(t *testing.T) {
	in := passingInputs()
	in.MultiplexDrops = 2
	in.Cancelled = true
	in.GCPauseP99 = 120

	r := evaluateVerify(in)
	assert.Len(t, r.Reasons, 3, "every failing signal must be reported, not just the first")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=tools/loadgen`
Expected: FAIL — `undefined: evaluateVerify`.

- [ ] **Step 3: Write minimal implementation**

Create `tools/loadgen/verify_verdict.go`:

```go
package main

import "fmt"

// Verdict is the run outcome.
type Verdict string

const (
	VerdictPass         Verdict = "PASS"
	VerdictFail         Verdict = "FAIL"
	VerdictInconclusive Verdict = "INCONCLUSIVE"
)

// ExitCode maps a verdict to a process exit code so the command can be
// scripted without parsing stdout.
func (v Verdict) ExitCode() int {
	switch v {
	case VerdictPass:
		return 0
	case VerdictFail:
		return 1
	default:
		return 2
	}
}

// VerifyInputs is everything the evaluator needs.
type VerifyInputs struct {
	Violations        []Violation
	Counts            ProbeCounts
	Changes           ChangeCounts
	MinProbes         int
	MultiplexDrops    int64
	DroppedRecipients int
	ReadbackErr       error
	OracleErr         error
	Cancelled         bool
	GCPauseP99        float64
	GCPauseMax        float64
}

// VerifyResult is the evaluated outcome plus human-readable reasons.
type VerifyResult struct {
	Verdict    Verdict
	Reasons    []string
	Violations []Violation
}

// evaluateVerify decides PASS / FAIL / INCONCLUSIVE.
//
// INCONCLUSIVE overrides both others: it means the signals cannot be trusted,
// so reporting PASS or FAIL would be a lie either way.
func evaluateVerify(in VerifyInputs) VerifyResult {
	var reasons []string

	if in.MultiplexDrops > 0 {
		reasons = append(reasons, fmt.Sprintf(
			"%d multiplex drop(s) recorded — a harness-side loss is indistinguishable from a delivery bug",
			in.MultiplexDrops))
	}
	if in.DroppedRecipients > 0 {
		reasons = append(reasons, fmt.Sprintf(
			"%d tracked recipient connection dropped mid-run — non-delivery cannot be attributed to the system",
			in.DroppedRecipients))
	}
	if in.ReadbackErr != nil {
		reasons = append(reasons, fmt.Sprintf("readback query failed: %v", in.ReadbackErr))
	}
	if in.OracleErr != nil {
		reasons = append(reasons, fmt.Sprintf("membership oracle query failed: %v", in.OracleErr))
	}
	if in.Counts.Tracked < in.MinProbes {
		reasons = append(reasons, fmt.Sprintf(
			"only %d probes tracked, below --min-probes=%d (%d suppressed by settle windows)",
			in.Counts.Tracked, in.MinProbes, in.Counts.Suppressed))
	}
	if in.Cancelled {
		reasons = append(reasons, "run cancelled before completion")
	}
	if in.GCPauseMax > 0 && in.GCPauseP99 > in.GCPauseMax {
		reasons = append(reasons, fmt.Sprintf(
			"loadgen GC pause p99 %.1fms exceeds %.1fms — the load box was saturated",
			in.GCPauseP99, in.GCPauseMax))
	}

	if len(reasons) > 0 {
		return VerifyResult{Verdict: VerdictInconclusive, Reasons: reasons, Violations: in.Violations}
	}
	if len(in.Violations) > 0 {
		return VerifyResult{Verdict: VerdictFail, Violations: in.Violations}
	}
	return VerifyResult{Verdict: VerdictPass}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=tools/loadgen`
Expected: PASS, 14 tests.

- [ ] **Step 5: Commit**

```bash
git add tools/loadgen/verify_verdict.go tools/loadgen/verify_verdict_test.go
git commit -m "feat(loadgen): verify verdict evaluation with INCONCLUSIVE override"
```

---

## Task 9: Report rendering

**Files:**
- Create: `tools/loadgen/verify_report.go`
- Test: `tools/loadgen/verify_report_test.go`

**Interfaces:**
- Consumes: `VerifyResult` (Task 8), `ProbeCounts` (Task 2), `ChangeCounts` (Task 6), `ProbeRoomSet` (Task 1)
- Produces: `VerifyReport` struct, `renderVerifyConsole(rep VerifyReport) string`, `renderVerifyJSON(rep VerifyReport) ([]byte, error)`

**Context:** Console caps violations at 10; `--json` carries full detail. Never prints message content (spec §11, §14).

- [ ] **Step 1: Write the failing test**

```go
package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func reportForTest() VerifyReport {
	return VerifyReport{
		ProbeRooms:     9,
		ProbeMembers:   171,
		DirectPoolSize: 191,
		ReserveSize:    20,
		BackgroundSize: 7383,
		Counts:         ProbeCounts{Tracked: 412, Suppressed: 18, Complete: 410, Partial: 2},
		Changes:        ChangeCounts{Total: 24, Adds: 14, Removes: 10, Applied: 24, Effective: 24},
		Result: VerifyResult{
			Verdict: VerdictFail,
			Violations: []Violation{
				{Kind: KindMissingRecipient, MsgID: "m1", RoomID: "r1", Users: []string{"u-1", "u-2"}},
			},
		},
	}
}

func TestRenderVerifyConsole_ShowsVerdict(t *testing.T) {
	out := renderVerifyConsole(reportForTest())
	assert.Contains(t, out, "VERDICT: FAIL")
}

func TestRenderVerifyConsole_ShowsCoverage(t *testing.T) {
	out := renderVerifyConsole(reportForTest())
	assert.Contains(t, out, "probe rooms:")
	assert.Contains(t, out, "412 tracked")
	assert.Contains(t, out, "18 suppressed")
}

func TestRenderVerifyConsole_ShowsViolationDetail(t *testing.T) {
	out := renderVerifyConsole(reportForTest())
	assert.Contains(t, out, "missing_recipient")
	assert.Contains(t, out, "m1")
	assert.Contains(t, out, "u-1")
}

func TestRenderVerifyConsole_CapsViolationsAtTen(t *testing.T) {
	rep := reportForTest()
	rep.Result.Violations = nil
	for i := 0; i < 25; i++ {
		rep.Result.Violations = append(rep.Result.Violations, Violation{
			Kind: KindMissingRecipient, MsgID: fmtUserID(i), RoomID: "r1",
		})
	}

	out := renderVerifyConsole(rep)
	assert.Contains(t, out, "showing 10 of 25")
	assert.Equal(t, 10, strings.Count(out, "missing_recipient"))
}

func TestRenderVerifyConsole_ShowsInconclusiveReasons(t *testing.T) {
	rep := reportForTest()
	rep.Result = VerifyResult{Verdict: VerdictInconclusive, Reasons: []string{"3 multiplex drop(s) recorded"}}

	out := renderVerifyConsole(rep)
	assert.Contains(t, out, "VERDICT: INCONCLUSIVE")
	assert.Contains(t, out, "multiplex drop")
}

func TestRenderVerifyConsole_PassHasNoViolationBlock(t *testing.T) {
	rep := reportForTest()
	rep.Result = VerifyResult{Verdict: VerdictPass}

	out := renderVerifyConsole(rep)
	assert.Contains(t, out, "VERDICT: PASS")
	assert.NotContains(t, out, "VIOLATIONS")
}

func TestRenderVerifyJSON_RoundTrips(t *testing.T) {
	raw, err := renderVerifyJSON(reportForTest())
	require.NoError(t, err)

	var back VerifyReport
	require.NoError(t, json.Unmarshal(raw, &back))
	assert.Equal(t, VerdictFail, back.Result.Verdict)
	require.Len(t, back.Result.Violations, 1)
	assert.Equal(t, []string{"u-1", "u-2"}, back.Result.Violations[0].Users)
}

func TestRenderVerifyJSON_CarriesAllViolations(t *testing.T) {
	rep := reportForTest()
	rep.Result.Violations = nil
	for i := 0; i < 25; i++ {
		rep.Result.Violations = append(rep.Result.Violations, Violation{
			Kind: KindMissingRecipient, MsgID: fmtUserID(i),
		})
	}

	raw, err := renderVerifyJSON(rep)
	require.NoError(t, err)

	var back VerifyReport
	require.NoError(t, json.Unmarshal(raw, &back))
	assert.Len(t, back.Result.Violations, 25, "JSON must not be capped like the console")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=tools/loadgen`
Expected: FAIL — `undefined: VerifyReport`.

- [ ] **Step 3: Write minimal implementation**

Create `tools/loadgen/verify_report.go`:

```go
package main

import (
	"encoding/json"
	"fmt"
	"strings"
)

// consoleViolationCap bounds terminal output. Full detail goes to --json.
const consoleViolationCap = 10

// VerifyReport is the full run summary, rendered to console and JSON.
type VerifyReport struct {
	ProbeRooms     int          `json:"probeRooms"`
	ProbeMembers   int          `json:"probeMembers"`
	DirectPoolSize int          `json:"directPoolSize"`
	ReserveSize    int          `json:"reserveSize"`
	BackgroundSize int          `json:"backgroundSize"`
	Counts         ProbeCounts  `json:"counts"`
	Changes        ChangeCounts `json:"changes"`
	Result         VerifyResult `json:"result"`
}

// renderVerifyConsole formats the operator-facing summary. Never includes
// message content — IDs only.
func renderVerifyConsole(rep VerifyReport) string {
	var b strings.Builder

	fmt.Fprintf(&b, "probe rooms: %d / %d members / direct pool %d (%d reserve)\n",
		rep.ProbeRooms, rep.ProbeMembers, rep.DirectPoolSize, rep.ReserveSize)
	fmt.Fprintf(&b, "background:  %d users on multiplex\n", rep.BackgroundSize)
	fmt.Fprintf(&b, "probes:      %d tracked / %d suppressed (settle window)\n",
		rep.Counts.Tracked, rep.Counts.Suppressed)
	fmt.Fprintf(&b, "delivery:    %d complete / %d partial / %d total-loss\n",
		rep.Counts.Complete, rep.Counts.Partial, rep.Counts.TotalLoss)
	fmt.Fprintf(&b, "leakage:     %d unexpected recipients (user lane)\n", rep.Counts.Leaked)
	fmt.Fprintf(&b, "membership:  %d changes (%d add, %d remove) / %d applied / %d effective\n",
		rep.Changes.Total, rep.Changes.Adds, rep.Changes.Removes,
		rep.Changes.Applied, rep.Changes.Effective)

	if len(rep.Result.Reasons) > 0 {
		b.WriteString("\nREASONS\n")
		for _, r := range rep.Result.Reasons {
			fmt.Fprintf(&b, "  %s\n", r)
		}
	}

	if n := len(rep.Result.Violations); n > 0 {
		shown := n
		if shown > consoleViolationCap {
			shown = consoleViolationCap
		}
		fmt.Fprintf(&b, "\nVIOLATIONS (showing %d of %d)\n", shown, n)
		for _, v := range rep.Result.Violations[:shown] {
			fmt.Fprintf(&b, "  %-30s room=%s", v.Kind, v.RoomID)
			if v.MsgID != "" {
				fmt.Fprintf(&b, " msg=%s", v.MsgID)
			}
			if len(v.Users) > 0 {
				fmt.Fprintf(&b, " users=%s", strings.Join(v.Users, ","))
			}
			if v.Detail != "" {
				fmt.Fprintf(&b, "\n      %s", v.Detail)
			}
			b.WriteString("\n")
		}
	}

	fmt.Fprintf(&b, "\nVERDICT: %s\n", rep.Result.Verdict)
	return b.String()
}

// renderVerifyJSON emits the full report, uncapped.
func renderVerifyJSON(rep VerifyReport) ([]byte, error) {
	out, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal verify report: %w", err)
	}
	return out, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=tools/loadgen`
Expected: PASS, 8 tests.

- [ ] **Step 5: Commit**

```bash
git add tools/loadgen/verify_report.go tools/loadgen/verify_report_test.go
git commit -m "feat(loadgen): verify console and JSON report rendering"
```

---

## Task 10a: Flags, preflight, and activation ordering

**Scope note:** Task 10 is split. **10a is this task** — pure, fully-specified, unit-testable functions, reviewed under the normal gate. **10b** is the integration glue (`executeVerify`, dispatch wiring), which cannot be unit-tested without the full stack. Do not implement `runVerify` or `executeVerify` here.

**Files:**
- Create: `tools/loadgen/verify.go` (config, flags, preflight, ordering only)
- Test: `tools/loadgen/verify_test.go`

**Interfaces:**
- Consumes: `ProbeRoomSet` (Task 1)
- Produces: `verifyConfig` struct, `parseVerifyFlags(args []string) (verifyConfig, error)`, `preflightVerify(ctx context.Context, vc verifyConfig, prs ProbeRoomSet, directSize int) error`, `orderForActivation(users []*userState, designated []string) []string`

**Context:** `dispatch` in `main.go:113` routes subcommands; each returns an int exit code (`runDaily(ctx, cfg, os.Args[2:])` at `main.go:131`). Preflight must fail fast on a large-room-threshold mismatch and on `--direct-only` with insufficient direct budget (spec §6.1, §6.3).

The `activateUsers` change seeds the direct pool with a designated set before the prefix walk. It must be additive: an empty designated set reproduces today's ordering exactly.

- [ ] **Step 1: Write the failing test**

```go
package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseVerifyFlags_Defaults(t *testing.T) {
	vc, err := parseVerifyFlags(nil)
	require.NoError(t, err)

	assert.Equal(t, "daily-heavy", vc.Preset)
	assert.Equal(t, 50, vc.ProbeRooms)
	assert.Equal(t, 200, vc.ReserveUsers)
	assert.InDelta(t, 0.01, vc.ProbeRate, 1e-9)
	assert.Equal(t, 50, vc.MinProbes)
	assert.Equal(t, 500, vc.LargeRoomThreshold)
	assert.Equal(t, 30*time.Second, vc.Drain)
	assert.Equal(t, 5*time.Second, vc.Settle)
	assert.Equal(t, "both", vc.Lane)
	assert.False(t, vc.DirectOnly)
}

func TestParseVerifyFlags_Overrides(t *testing.T) {
	vc, err := parseVerifyFlags([]string{
		"--preset=daily-light", "--probe-rooms=12", "--probe-rate=0.5",
		"--drain=90s", "--settle=2s", "--lane=global", "--direct-only",
		"--member-churn=0",
	})
	require.NoError(t, err)

	assert.Equal(t, "daily-light", vc.Preset)
	assert.Equal(t, 12, vc.ProbeRooms)
	assert.InDelta(t, 0.5, vc.ProbeRate, 1e-9)
	assert.Equal(t, 90*time.Second, vc.Drain)
	assert.Equal(t, 2*time.Second, vc.Settle)
	assert.Equal(t, "global", vc.Lane)
	assert.True(t, vc.DirectOnly)
	assert.Zero(t, vc.MemberChurn)
}

func TestParseVerifyFlags_RejectsBadLane(t *testing.T) {
	_, err := parseVerifyFlags([]string{"--lane=sideways"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "lane")
}

func TestParseVerifyFlags_RejectsBadProbeRate(t *testing.T) {
	_, err := parseVerifyFlags([]string{"--probe-rate=1.5"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "probe-rate")
}

func TestPreflightVerify_RejectsThresholdMismatch(t *testing.T) {
	vc, err := parseVerifyFlags([]string{"--large-room-threshold=500"})
	require.NoError(t, err)

	prs := ProbeRoomSet{
		Rooms:  []model.Room{{ID: "room-medium-000001", UserCount: 900}},
		byRoom: map[string][]string{"room-medium-000001": {"u-1"}},
	}

	err = preflightVerify(t.Context(), vc, prs, 1)
	require.Error(t, err,
		"a probe room above the threshold means the gatekeeper will reject its sends")
	assert.Contains(t, err.Error(), "threshold")
}

func TestPreflightVerify_RejectsIncompleteDirectPool(t *testing.T) {
	vc, err := parseVerifyFlags(nil)
	require.NoError(t, err)

	prs := ProbeRoomSet{
		Rooms:   []model.Room{{ID: "room-small-000001", UserCount: 3}},
		Members: []string{"u-1", "u-2", "u-3"},
		byRoom:  map[string][]string{"room-small-000001": {"u-1", "u-2", "u-3"}},
	}

	// Only 2 of 3 probe-room members made it into the direct pool.
	err = preflightVerify(t.Context(), vc, prs, 2)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "direct pool")
}

func TestPreflightVerify_AcceptsCompleteSetup(t *testing.T) {
	vc, err := parseVerifyFlags(nil)
	require.NoError(t, err)

	prs := ProbeRoomSet{
		Rooms:   []model.Room{{ID: "room-small-000001", UserCount: 3}},
		Members: []string{"u-1", "u-2", "u-3"},
		byRoom:  map[string][]string{"room-small-000001": {"u-1", "u-2", "u-3"}},
	}

	require.NoError(t, preflightVerify(t.Context(), vc, prs, 3))
}

func TestActivateUsers_EmptyDesignatedSet_PreservesDailyOrder(t *testing.T) {
	// Regression guard: daily must be unaffected by the designated-set change.
	users := make([]*userState, 5)
	for i := range users {
		users[i] = &userState{ID: fmtUserID(i), Account: fmtAccount(i)}
	}

	got := orderForActivation(users, nil)

	want := []string{fmtUserID(0), fmtUserID(1), fmtUserID(2), fmtUserID(3), fmtUserID(4)}
	assert.Equal(t, want, got)
}

func TestActivateUsers_DesignatedSetGoesFirst(t *testing.T) {
	users := make([]*userState, 5)
	for i := range users {
		users[i] = &userState{ID: fmtUserID(i), Account: fmtAccount(i)}
	}

	got := orderForActivation(users, []string{fmtUserID(3), fmtUserID(4)})

	// Designated users lead so they land in the direct pool; the rest keep
	// their original relative order.
	want := []string{fmtUserID(3), fmtUserID(4), fmtUserID(0), fmtUserID(1), fmtUserID(2)}
	assert.Equal(t, want, got)
}
```

Add `"github.com/hmchangw/chat/pkg/model"` to the test imports.

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=tools/loadgen`
Expected: FAIL — `undefined: parseVerifyFlags`, `undefined: orderForActivation`.

- [ ] **Step 3: Write minimal implementation**

Create `tools/loadgen/verify.go`:

```go
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"
)

// verifyConfig is the parsed CLI surface for the verify scenario.
type verifyConfig struct {
	Preset             string
	Users              int
	ProbeRooms         int
	ReserveUsers       int
	MemberChurn        float64
	Settle             time.Duration
	Warmup             time.Duration
	Steady             time.Duration
	Drain              time.Duration
	ProbeRate          float64
	MinProbes          int
	LargeRoomThreshold int
	Lane               string
	DirectOnly         bool
	Seed               int64
	JSONPath           string
}

// parseVerifyFlags parses the verify subcommand's flags and validates them.
func parseVerifyFlags(args []string) (verifyConfig, error) {
	var vc verifyConfig
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	fs.StringVar(&vc.Preset, "preset", "daily-heavy", "daily-light | daily-heavy | daily-power")
	fs.IntVar(&vc.Users, "users", 0, "total activation count (0 = preset default)")
	fs.IntVar(&vc.ProbeRooms, "probe-rooms", 50, "number of probe rooms")
	fs.IntVar(&vc.ReserveUsers, "reserve-users", 200, "direct-connected floaters for membership changes")
	fs.Float64Var(&vc.MemberChurn, "member-churn", 0.2, "membership changes per probe room per minute (0 disables)")
	fs.DurationVar(&vc.Settle, "settle", 5*time.Second, "post-change quiet window per room")
	fs.DurationVar(&vc.Warmup, "warmup", 30*time.Second, "pre-measurement settle")
	fs.DurationVar(&vc.Steady, "steady", 120*time.Second, "probe-generating window")
	fs.DurationVar(&vc.Drain, "drain", 30*time.Second, "post-quiesce wait for in-flight probes")
	fs.Float64Var(&vc.ProbeRate, "probe-rate", 0.01, "fraction of probe-room sends tracked")
	fs.IntVar(&vc.MinProbes, "min-probes", 50, "below this, the verdict is INCONCLUSIVE")
	fs.IntVar(&vc.LargeRoomThreshold, "large-room-threshold", 500, "must match the gatekeeper's setting")
	fs.StringVar(&vc.Lane, "lane", "both", "global | local | both")
	fs.BoolVar(&vc.DirectOnly, "direct-only", false, "disable multiplex; every user gets a dedicated conn")
	fs.Int64Var(&vc.Seed, "seed", 42, "drives fixtures, probe rooms, and probe selection")
	fs.StringVar(&vc.JSONPath, "json", "", "full violation detail output path")

	if err := fs.Parse(args); err != nil {
		return vc, fmt.Errorf("parse verify flags: %w", err)
	}
	switch vc.Lane {
	case "global", "local", "both":
	default:
		return vc, fmt.Errorf("invalid --lane %q: want global, local, or both", vc.Lane)
	}
	if vc.ProbeRate < 0 || vc.ProbeRate > 1 {
		return vc, fmt.Errorf("invalid --probe-rate %v: want a fraction in [0,1]", vc.ProbeRate)
	}
	if vc.ProbeRooms <= 0 {
		return vc, fmt.Errorf("invalid --probe-rooms %d: want a positive count", vc.ProbeRooms)
	}
	return vc, nil
}

// preflightVerify fails fast on configurations that would produce phantom
// violations. Seconds spent here beat a ten-minute run reporting a fake bug.
func preflightVerify(_ context.Context, vc verifyConfig, prs ProbeRoomSet, directSize int) error {
	for _, r := range prs.Rooms {
		if r.UserCount >= vc.LargeRoomThreshold {
			return fmt.Errorf(
				"probe room %s has %d members, at or above --large-room-threshold=%d: "+
					"the gatekeeper rejects non-thread sends from member-role users there, "+
					"which is indistinguishable from message loss",
				r.ID, r.UserCount, vc.LargeRoomThreshold)
		}
	}
	if directSize < len(prs.Members) {
		return fmt.Errorf(
			"direct pool holds %d of %d probe-room members: a member without a dedicated "+
				"connection cannot be observed, which would corrupt the completeness verdict",
			directSize, len(prs.Members))
	}
	return nil
}

// orderForActivation puts designated users first so they land in the direct
// pool, preserving the original relative order of everyone else. With an empty
// designated set the input order is returned unchanged, which is exactly
// daily's existing behaviour.
func orderForActivation(users []*userState, designated []string) []string {
	want := make(map[string]struct{}, len(designated))
	for _, id := range designated {
		want[id] = struct{}{}
	}

	first := make([]string, 0, len(designated))
	rest := make([]string, 0, len(users))
	for _, u := range users {
		if _, ok := want[u.ID]; ok {
			first = append(first, u.ID)
			continue
		}
		rest = append(rest, u.ID)
	}
	return append(first, rest...)
}

```

Imports needed in `verify.go` for 10a: `context`, `flag`, `fmt`, `time`.

- [ ] **Step 4: Run test to verify it passes**

Run: `make test SERVICE=tools/loadgen`
Expected: PASS, 9 tests. Every existing `daily_test.go` test must still pass.

- [ ] **Step 5: Commit**

```bash
git add tools/loadgen/verify.go tools/loadgen/verify_test.go
git commit -m "feat(loadgen): verify flags, preflight, and activation ordering"
```

---

## Task 10b: Lifecycle wiring and subcommand dispatch

**Files:**
- Modify: `tools/loadgen/verify.go` (add `runVerify` and `executeVerify`)
- Modify: `tools/loadgen/main.go:113-143`
- Modify: `tools/loadgen/daily.go` (`activateUsers` designated-set seeding)

**Interfaces:**
- Consumes: everything from Tasks 1–10a
- Produces: `runVerify(ctx context.Context, cfg *config, args []string) int`, `executeVerify(...) (VerifyReport, error)`

**Deferral, stated up front:** `executeVerify` is integration glue over `prodEnvFactory`. It cannot be unit-tested without the full docker-compose stack — the same constraint that keeps `daily_integration_test.go` skipped (`daily_integration_test.go:31`). This task therefore has **no red phase and no new unit tests**; its correctness is established by the end-to-end run in Task 11 Step 2. This is a deliberate, spec-acknowledged exception to the plan's TDD constraint, not an oversight. Do not write a stubbed integration test to satisfy the form — a vacuously-passing test is worse than none.

> **Amendment (2026-08-09, pre-merge fix wave) — the deferral's verification never happened.**
>
> The end-to-end run named above as this task's *only* correctness mechanism was **not performed**: Docker was unavailable in the implementation environment, so Task 11 Step 2 never ran. `executeVerify` and `runVerify` therefore ship with **no executed verification of any kind** — not unit, not integration, not manual. This is recorded, not resolved; the branch is merged with that gap open.
>
> Two further points the original wording obscured:
> - The deferral was scoped to `executeVerify`, but the same commit also landed ~10 pure, trivially unit-testable helpers (`probeSendRate`, `churnInterval`, `churnRooms`, `pickJoinTarget`, `harvest`, `verifyDailyConfig`, `dropAbsentReserve`, `indexUsersByID`, `indexPositions`, `buildVerifyUsers`, `pendingFor`, `roomRequester`) that the blanket "no new unit tests" decision swept along without justification. The pre-merge fix wave adds unit tests for all of them; only `executeVerify` and `runVerify` remain untested.
> - **Outstanding action:** run `loadgen verify` end-to-end against the docker-compose stack before trusting any verdict it produces. Until that happens, a PASS from this tool is unvalidated.

Everything in Tasks 1–10a that *can* be unit-tested already is; this task is only the wiring between them.

- [ ] **Step 1: Add `runVerify` to `verify.go`**

```go
// runVerify is the verify subcommand entry point.
func runVerify(ctx context.Context, cfg *config, args []string) int {
	vc, err := parseVerifyFlags(args)
	if err != nil {
		slog.Error("verify flag parse failed", "err", err)
		return 2
	}

	// BuiltinPreset returns a value; BuildFixtures takes a pointer.
	preset, ok := BuiltinPreset(vc.Preset)
	if !ok {
		slog.Error("unknown preset", "preset", vc.Preset)
		return 2
	}

	fx := BuildFixtures(&preset, vc.Seed, cfg.SiteID)

	prs, err := selectProbeRooms(fx, vc.ProbeRooms, vc.LargeRoomThreshold, vc.Seed)
	if err != nil {
		slog.Error("probe room selection failed", "err", err)
		return 2
	}
	reserve := selectReserve(fx, prs, vc.ReserveUsers, vc.Seed)
	designated := append(append([]string(nil), prs.Members...), reserve...)

	slog.Info("verify starting",
		"preset", vc.Preset,
		"probeRooms", len(prs.Rooms),
		"probeMembers", len(prs.Members),
		"reserve", len(reserve),
		"probeRate", vc.ProbeRate,
		"memberChurn", vc.MemberChurn)

	rep, err := executeVerify(ctx, cfg, vc, fx, prs, designated)
	if err != nil {
		slog.Error("verify run failed", "err", err)
		return 2
	}

	fmt.Print(renderVerifyConsole(rep))

	if vc.JSONPath != "" {
		raw, err := renderVerifyJSON(rep)
		if err != nil {
			slog.Error("render json report failed", "err", err)
			return 2
		}
		if err := os.WriteFile(vc.JSONPath, raw, 0o600); err != nil {
			slog.Error("write json report failed", "path", vc.JSONPath, "err", err)
			return 2
		}
	}

	return rep.Result.Verdict.ExitCode()
}
```

Add imports `log/slog` and `os` to `verify.go`.

- [ ] **Step 2: Implement `executeVerify`**

Read `daily.go:405-460` (`activateUsers`), `daily.go:752-890` (`prodEnvFactory.Build`, `runDailyForTest`), and `daily_pool.go:34-118` (`directPool`) first — `executeVerify` follows the same wiring shape.

Sequence:

1. Build the pools. Honour `vc.DirectOnly` (multiplex size 0) and size the direct cap to at least `len(designated)`.
2. `tracker := NewProbeTracker()`, then `directPool.attachSink(tracker)` **before** any `Add` call, so no delivery is missed.
3. `mm := NewMembershipModel(prs)`; `mm.SetSettle(vc.Settle)`.
4. Activate users in `orderForActivation(env.users, designated)` order so designated users occupy the direct pool.
5. Call `preflightVerify(ctx, vc, prs, directPool.Size())` **after** activation — it asserts the direct pool actually holds every probe-room member. Return its error unchanged.
6. Warmup for `vc.Warmup`, then steady for `vc.Steady`. During steady, the action emitter runs as in daily; sends into probe rooms consult `shouldProbe(vc.Seed, userIdx, seqNo, vc.ProbeRate)`. When a send is probed: if `mm.InSettle(roomID, now)` call `tracker.RecordSuppressed()` and skip tracking, otherwise `tracker.RegisterProbe(msgID, roomID, senderID, mm.Epoch(roomID), mm.MembersAtEpoch(roomID, mm.Epoch(roomID)), now)`.
7. When `vc.MemberChurn > 0`, a churn goroutine issues adds/removes at that per-room-per-minute rate using `subject.MemberAdd` / `subject.MemberRemove`, drawing targets from the reserve. On a successful add, call `directPool.SubscribeRoom(targetID, roomID)` then `mm.ApplyAdd`. After each change's settle window elapses, query `subject.UserSubscriptionList` for the target and call `mm.RecordOracle`, then attempt one send as the target and call `mm.RecordSendResult` with whether it was accepted. The goroutine must exit on `ctx.Done()` and be tracked by a `sync.WaitGroup`.
8. Quiesce: cancel the emitter context, stop churn, wait on the `WaitGroup`. Keep all subscriptions live.
9. Drain: wait `vc.Drain`.
10. Readback: build `[]ReadbackTarget` from the tracked probes and call `NewReadback(request, cfg.SiteID, 4, 4*time.Second).Verify(...)`. Capture the error separately — it feeds `VerifyInputs.ReadbackErr`, it is not a violation.
11. Assemble `VerifyInputs` (violations from `tracker.Finalize()` plus `mm.Finalize()` plus readback violations; counts; `MultiplexDrops` from the collector; `GCPauseP99` via the existing daily self-metrics helper; `Cancelled` from `ctx.Err() != nil`), call `evaluateVerify`, and return the populated `VerifyReport`.

- [ ] **Step 3: Wire the dispatch**

In `main.go`, add after the `daily` case at line 131:

```go
	case "verify":
		return runVerify(ctx, cfg, os.Args[2:])
```

- [ ] **Step 4: Rewire `activateUsers`**

In `daily.go`, change `activateUsers` to walk an order produced by `orderForActivation` rather than indexing `env.users` directly. Add a `designated []string` field to `stepEnv`, left nil by `daily`. With a nil designated set the walk order must be identical to today's — `TestActivateUsers_EmptyDesignatedSet_PreservesDailyOrder` from Task 10a pins this.

- [ ] **Step 5: Verify the build and the full suite**

Run: `make test SERVICE=tools/loadgen && make build SERVICE=tools/loadgen && ./bin/loadgen verify -h`
Expected: all tests pass (no new ones), binary builds, flag list prints.

- [ ] **Step 6: Commit**

```bash
git add tools/loadgen/verify.go tools/loadgen/main.go tools/loadgen/daily.go
git commit -m "feat(loadgen): verify lifecycle wiring and subcommand dispatch"
```

---

## Task 11: Deploy target, README, and resource measurement

**Files:**
- Modify: `tools/loadgen/deploy/Makefile`
- Modify: `tools/loadgen/README.md`
- Modify: `docs/superpowers/specs/2026-08-08-loadgen-verify-correctness-design.md` (§6.3 measured numbers)

**Interfaces:**
- Consumes: the `verify` binary from Task 10
- Produces: `make -C tools/loadgen/deploy run-verify` target

**Context:** Spec §6.3 carries resource estimates explicitly marked unmeasured, and success criterion 7 requires replacing them with measurements. This task closes that.

- [ ] **Step 1: Add the deploy target**

In `tools/loadgen/deploy/Makefile`, following the existing `run-daily` target's shape:

```makefile
run-verify:
	docker compose run --rm loadgen verify \
	  --preset=$(or $(PRESET),daily-heavy) \
	  --probe-rooms=$(or $(PROBE_ROOMS),50) \
	  --steady=$(or $(STEADY),120s) \
	  --drain=$(or $(DRAIN),30s) \
	  --json=/out/verify.json
```

- [ ] **Step 2: Run the end-to-end harness against a healthy stack**

```bash
make -C tools/loadgen/deploy up
make -C tools/loadgen/deploy seed PRESET=daily-heavy
make -C tools/loadgen/deploy run-verify PRESET=daily-heavy STEADY=60s
```

Expected: `VERDICT: PASS`, exit code 0. If INCONCLUSIVE, read the reasons block — the most likely causes are an unraised `ulimit -n` or a `--large-room-threshold` that does not match the gatekeeper's.

- [ ] **Step 3: Measure the direct-pool resource profile**

Run `verify` at increasing `--probe-rooms` with `--direct-only`, recording for each: direct connection count, loadgen RSS (`docker stats`), NATS server RSS, and whether the verdict stayed conclusive.

```bash
for n in 10 25 50 100; do
  make -C tools/loadgen/deploy run-verify PROBE_ROOMS=$n STEADY=60s
done
```

Record the numbers in a table.

- [ ] **Step 4: Replace the estimates in the spec**

Edit §6.3 of the design doc: replace the "**The estimates below are unmeasured**" paragraph and the rough tiers with the measured table from Step 3, and note the host the measurements were taken on (numbers vary by machine — the same caveat the README already makes for `loadgen run`).

- [ ] **Step 5: Document the scenario in the README**

Add a `## Correctness verification (verify)` section to `tools/loadgen/README.md` after the daily-IM section, covering: quick start, prerequisites (including the `--large-room-threshold` match requirement and `ulimit -n`), the flag table, the nine violation classes, how to read the report, exit codes, and the known limitations — sampling means 99% of traffic is unverified, ordering is not checked, leakage is user-lane only, and the run is single-site.

- [ ] **Step 6: Run the full gate**

```bash
make fmt
make lint
make test SERVICE=tools/loadgen
make sast
```

Expected: all clean. Confirm coverage:

```bash
go test -coverprofile=coverage.out ./tools/loadgen/... && go tool cover -func=coverage.out | grep -E 'verify_(probe|verdict)' 
```

Expected: ≥90% on `verify_probe.go` and `verify_verdict.go`, ≥80% overall.

- [ ] **Step 7: Commit**

```bash
git add tools/loadgen/deploy/Makefile tools/loadgen/README.md docs/superpowers/specs/2026-08-08-loadgen-verify-correctness-design.md
git commit -m "docs(loadgen): verify deploy target, operator guide, and measured resource profile"
```

---

## Spec Coverage Check

| Spec section | Covered by |
|---|---|
| §3 violation classes | Tasks 1, 2, 3, 6, 7 |
| §4 lifecycle, exit codes | Tasks 8, 10a, 10b |
| §4.1 CLI flags | Task 10a |
| §5 probe sampling | Task 5 |
| §6.0 probe-room-first activation, reserve | Tasks 1, 10a, 10b |
| §6.1 large-room exclusion + preflight | Tasks 1, 10a |
| §6.2 multiplex never probe recipients | Tasks 4, 8 |
| §6.3 resource budget, `--lane`, `--direct-only` | Tasks 10a, 10b, 11 |
| §7 receiver attribution | Task 4 |
| §7.1 dual-lane dedupe | Task 2 |
| §7.2 sender self-delivery | Task 2 |
| §7.3 lane asymmetry | Task 3 |
| §8 persistence readback | Task 7 |
| §9.1–9.4 membership | Task 6 |
| §10 verdict | Task 8 |
| §11 output | Task 9 |
| §12 implementation layout | all |
| §13 testing | all |
| §14 error handling | Global Constraints + all |
| §16 success criteria | Task 11 |

**Known deferral:** `executeVerify` (Task 10b) is specified prose-first rather than code-first because it is integration glue over `prodEnvFactory`, which cannot be unit-tested without the full stack — the same constraint that keeps `daily_integration_test.go` skipped. Task 10 is split so this deferral is quarantined: **10a** is pure and fully unit-tested under the normal gate, **10b** is the wiring, verified by the Task 11 Step 2 end-to-end run. Task 10b has no red phase by design; a stubbed integration test there would pass vacuously and is explicitly forbidden.
