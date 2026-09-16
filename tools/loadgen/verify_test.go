package main

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand" // #nosec G404 -- load generator randomness, never used for secrets // nosemgrep: math-random-used
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
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

func TestParseVerifyFlags_RejectsUnknownFlag(t *testing.T) {
	_, err := parseVerifyFlags([]string{"--not-a-real-flag=1"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse verify flags")
}

func TestParseVerifyFlags_RejectsNonPositiveProbeRooms(t *testing.T) {
	_, err := parseVerifyFlags([]string{"--probe-rooms=0"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "probe-rooms")
}

func TestParseVerifyFlags_RejectsMinProbesBelowOne(t *testing.T) {
	_, err := parseVerifyFlags([]string{"--min-probes=0"})
	require.Error(t, err,
		"MinProbes==0 disables the probe floor in evaluateVerify (Tracked < 0 never fires), "+
			"so a run tracking zero probes would silently report PASS")
	assert.Contains(t, err.Error(), "min-probes")
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

func TestPreflightVerify_RejectsRoomAtExactThreshold(t *testing.T) {
	vc, err := parseVerifyFlags([]string{"--large-room-threshold=500"})
	require.NoError(t, err)

	prs := ProbeRoomSet{
		Rooms:  []model.Room{{ID: "room-medium-000001", UserCount: 500}},
		byRoom: map[string][]string{"room-medium-000001": {"u-1"}},
	}

	err = preflightVerify(t.Context(), vc, prs, 1)
	require.Error(t, err,
		"the gatekeeper's threshold is inclusive: a room with exactly LargeRoomThreshold "+
			"members is already rejected, so preflight must reject at the boundary too")
	assert.Contains(t, err.Error(), "threshold")
}

func TestPreflightVerify_AcceptsRoomOneBelowThreshold(t *testing.T) {
	vc, err := parseVerifyFlags([]string{"--large-room-threshold=500"})
	require.NoError(t, err)

	prs := ProbeRoomSet{
		Rooms:  []model.Room{{ID: "room-medium-000001", UserCount: 499}},
		byRoom: map[string][]string{"room-medium-000001": {"u-1"}},
	}

	require.NoError(t, preflightVerify(t.Context(), vc, prs, 1))
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

// activationRecorder captures the order activateUsers walks users in. mintJWT
// is invoked for every user before pool assignment, so with both pools nil the
// recorder sees the complete walk (every user is then "skipped" for want of a
// pool, which is exactly what we want — no NATS is involved).
func activationRecorder(users []*userState, designated []string) (*[]string, *stepEnv) {
	seen := new([]string)
	env := &stepEnv{
		users:      users,
		designated: designated,
		mintJWT: func(_ context.Context, account string) error {
			*seen = append(*seen, account)
			return nil
		},
	}
	return seen, env
}

func verifyTestUsers(n int) []*userState {
	users := make([]*userState, n)
	for i := range users {
		users[i] = &userState{ID: fmtUserID(i), Account: fmtAccount(i)}
	}
	return users
}

// TestActivateUsers_NilDesignatedSet_WalksDailyOrder pins the real walk, not
// just the orderForActivation helper: daily passes no designated set and must
// activate users in plain index order.
func TestActivateUsers_NilDesignatedSet_WalksDailyOrder(t *testing.T) {
	seen, env := activationRecorder(verifyTestUsers(5), nil)

	activateUsers(t.Context(), env, 0, 5)

	assert.Equal(t, []string{
		fmtAccount(0), fmtAccount(1), fmtAccount(2), fmtAccount(3), fmtAccount(4),
	}, *seen)
}

// TestActivateUsers_DesignatedSet_WalksDesignatedFirst pins verify's ordering:
// probe-room members and reserve floaters must be offered a pool slot before
// any background user, or they land on multiplex and become unobservable.
func TestActivateUsers_DesignatedSet_WalksDesignatedFirst(t *testing.T) {
	seen, env := activationRecorder(verifyTestUsers(5), []string{fmtUserID(3), fmtUserID(1)})

	activateUsers(t.Context(), env, 0, 5)

	// Designated users lead in their env.users order; everyone else follows in
	// their original relative order.
	assert.Equal(t, []string{
		fmtAccount(1), fmtAccount(3), fmtAccount(0), fmtAccount(2), fmtAccount(4),
	}, *seen)
}

// TestActivateUsers_RespectsRange pins the [from, to) slice semantics runStep
// depends on to activate only the delta between ramp steps.
func TestActivateUsers_RespectsRange(t *testing.T) {
	seen, env := activationRecorder(verifyTestUsers(5), nil)

	activateUsers(t.Context(), env, 2, 4)

	assert.Equal(t, []string{fmtAccount(2), fmtAccount(3)}, *seen)
}

func TestParseVerifyFlags_RejectsNegativeReserveUsers(t *testing.T) {
	_, err := parseVerifyFlags([]string{"--reserve-users=-1"})
	require.Error(t, err,
		"a negative reserve count reaches perm[:n] in selectReserve and panics with a "+
			"slice bounds error instead of a usage message")
	assert.Contains(t, err.Error(), "reserve-users")
}

func TestParseVerifyFlags_AcceptsZeroReserveUsers(t *testing.T) {
	vc, err := parseVerifyFlags([]string{"--reserve-users=0"})
	require.NoError(t, err, "zero reserve is a legitimate choice: churn simply has no targets")
	assert.Zero(t, vc.ReserveUsers)
}

// TestChurnTailroom_KeepsEveryIssuedChangeObservable is the regression guard for
// the silently-discarded-changes bug. driveChurn stops issuing at
// Steady-tailroom, but a change is only ever observed if issued+Settle lands
// inside the steady window with room left for its two observations. A fixed 10s
// tailroom dropped ~18% of changes at --settle=30s, which inflated
// ChangeCounts.Total without Applied/Effective and still reported PASS.
func TestChurnTailroom_KeepsEveryIssuedChangeObservable(t *testing.T) {
	tests := []struct {
		name           string
		steady, settle time.Duration
	}{
		{name: "defaults", steady: 120 * time.Second, settle: 5 * time.Second},
		{name: "settle at the old tailroom", steady: 120 * time.Second, settle: 10 * time.Second},
		{name: "settle above the old tailroom", steady: 120 * time.Second, settle: 30 * time.Second},
		{name: "long settle, long steady", steady: 600 * time.Second, settle: 120 * time.Second},
		{name: "zero settle", steady: 120 * time.Second, settle: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tailroom := churnTailroom(tt.settle)

			// The last moment driveChurn will issue a change, as an offset from
			// the start of the steady window.
			lastIssue := tt.steady - tailroom
			require.Positive(t, lastIssue, "the issue window must not be empty for this config")

			due := lastIssue + tt.settle
			assert.LessOrEqual(t, due+verifyChurnObservation, tt.steady,
				"a change issued at the end of the issue window must settle AND be observed "+
					"before the steady window closes, or it is counted but never harvested")
		})
	}
}

func TestChurnTailroom_FloorsAtObservationBudget(t *testing.T) {
	assert.Equal(t, verifyChurnTailroom, churnTailroom(0))
	assert.Equal(t, verifyChurnTailroom, churnTailroom(-time.Second),
		"a nonsensical negative settle must not shrink the tailroom below the floor")
}

func TestProbeSendRate(t *testing.T) {
	base := func(mut func(*verifyConfig)) *verifyConfig {
		vc := &verifyConfig{MinProbes: 50, ProbeRate: 0.01, Steady: 120 * time.Second}
		mut(vc)
		return vc
	}
	tests := []struct {
		name string
		vc   *verifyConfig
		want float64
	}{
		{
			name: "defaults derive from min-probes and probe-rate",
			vc:   base(func(*verifyConfig) {}),
			// 50 probes * 3 headroom / 0.01 sampled = 15000 sends over 120s.
			want: 125,
		},
		{
			name: "clamps to the ceiling so a tiny probe-rate is not a throughput test",
			vc:   base(func(vc *verifyConfig) { vc.ProbeRate = 0.001 }),
			want: verifyMaxProbeSendRate,
		},
		{
			name: "clamps to the floor of one send per second",
			vc:   base(func(vc *verifyConfig) { vc.MinProbes, vc.ProbeRate = 1, 1 }),
			want: 1,
		},
		{
			name: "zero probe-rate falls back to the floor instead of dividing by zero",
			vc:   base(func(vc *verifyConfig) { vc.ProbeRate = 0 }),
			want: 1,
		},
		{
			name: "zero steady falls back to the floor",
			vc:   base(func(vc *verifyConfig) { vc.Steady = 0 }),
			want: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.InDelta(t, tt.want, probeSendRate(tt.vc), 1e-9)
		})
	}
}

func TestChurnInterval(t *testing.T) {
	tests := []struct {
		name             string
		perRoomPerMinute float64
		rooms            int
		want             time.Duration
	}{
		{name: "defaults", perRoomPerMinute: 0.2, rooms: 30, want: 10 * time.Second},
		{name: "one change per room per minute", perRoomPerMinute: 1, rooms: 60, want: time.Second},
		{name: "churn disabled", perRoomPerMinute: 0, rooms: 30, want: 0},
		{name: "negative churn disabled", perRoomPerMinute: -1, rooms: 30, want: 0},
		{name: "no churnable rooms", perRoomPerMinute: 0.2, rooms: 0, want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, churnInterval(tt.perRoomPerMinute, tt.rooms))
		})
	}
}

// probeRoomSetForTest builds a ProbeRoomSet without going through fixtures.
func probeRoomSetForTest(byRoom map[string][]string, order ...string) ProbeRoomSet {
	prs := ProbeRoomSet{byRoom: byRoom}
	seen := map[string]struct{}{}
	for _, id := range order {
		prs.Rooms = append(prs.Rooms, model.Room{ID: id})
		for _, u := range byRoom[id] {
			seen[u] = struct{}{}
		}
	}
	for u := range seen {
		prs.Members = append(prs.Members, u)
	}
	sort.Strings(prs.Members)
	return prs
}

func TestProbeRoomSet_Has(t *testing.T) {
	prs := probeRoomSetForTest(map[string][]string{
		"room-small-000001": {"u-1"},
		"room-empty":        nil,
	}, "room-small-000001", "room-empty")

	assert.True(t, prs.Has("room-small-000001"))
	assert.True(t, prs.Has("room-empty"),
		"a registered room with no members is still a probe room — Has must key on the "+
			"map entry, not on member count, or emitOneProbe would score it total_loss")
	assert.False(t, prs.Has("room-small-999999"))
	assert.False(t, ProbeRoomSet{}.Has("anything"))
}

// TestVerifyRun_ChurnRooms_ExcludesDMs pins the DM-exclusion rule: room-service
// rejects member add/remove on a non-channel room, so churning a DM would only
// ever produce harness errors.
func TestVerifyRun_ChurnRooms_ExcludesDMs(t *testing.T) {
	r := &verifyRun{prs: probeRoomSetForTest(map[string][]string{
		"room-dm-000001":     {"u-1", "u-2"},
		"room-small-000001":  {"u-1"},
		"room-medium-000001": {"u-2"},
		"room-dm-000002":     {"u-3", "u-4"},
	}, "room-dm-000001", "room-small-000001", "room-medium-000001", "room-dm-000002")}

	assert.Equal(t, []string{"room-small-000001", "room-medium-000001"}, r.churnRooms())
}

func TestVerifyRun_ChurnRooms_AllDMsYieldsNothing(t *testing.T) {
	r := &verifyRun{prs: probeRoomSetForTest(map[string][]string{
		"room-dm-000001": {"u-1", "u-2"},
	}, "room-dm-000001")}

	assert.Empty(t, r.churnRooms(),
		"a DM-only probe set must disable churn rather than issue rejected requests")
}

func TestVerifyRun_PickJoinTarget(t *testing.T) {
	rnd := rand.New(rand.NewSource(1))

	t.Run("empty reserve", func(t *testing.T) {
		r := &verifyRun{}
		assert.Empty(t, r.pickJoinTarget(map[string]struct{}{}, rnd),
			"rnd.Intn(0) panics — the empty-reserve guard is load-bearing")
	})

	t.Run("every floater already in the room", func(t *testing.T) {
		r := &verifyRun{reserve: []string{"u-1", "u-2"}}
		current := map[string]struct{}{"u-1": {}, "u-2": {}}
		assert.Empty(t, r.pickJoinTarget(current, rnd))
	})

	t.Run("picks the only floater outside the room", func(t *testing.T) {
		r := &verifyRun{reserve: []string{"u-1", "u-2", "u-3"}}
		current := map[string]struct{}{"u-1": {}, "u-3": {}}
		assert.Equal(t, "u-2", r.pickJoinTarget(current, rnd))
	})

	t.Run("never picks a current member", func(t *testing.T) {
		r := &verifyRun{reserve: []string{"u-1", "u-2", "u-3", "u-4"}}
		current := map[string]struct{}{"u-1": {}, "u-2": {}}
		for i := 0; i < 50; i++ {
			got := r.pickJoinTarget(current, rnd)
			assert.NotContains(t, current, got)
			assert.Contains(t, []string{"u-3", "u-4"}, got)
		}
	})
}

func TestVerifyRun_RoomRequester(t *testing.T) {
	u1 := &userState{ID: "u-1", Account: "user-1"}
	r := &verifyRun{
		prs: probeRoomSetForTest(map[string][]string{
			"room-small-000001": {"u-1", "u-2"},
			"room-small-000002": nil,
			"room-small-000003": {"u-missing"},
		}, "room-small-000001", "room-small-000002", "room-small-000003"),
		byID: map[string]*userState{"u-1": u1},
	}

	assert.Same(t, u1, r.roomRequester("room-small-000001"),
		"the requester must be an original member (byRoom is sorted, so members[0] is stable)")
	assert.Nil(t, r.roomRequester("room-small-000002"))
	assert.Nil(t, r.roomRequester("room-small-000003"),
		"a member with no runtime state cannot issue the request")
	assert.Nil(t, r.roomRequester("room-unknown"))
}

func TestVerifyRun_PendingFor(t *testing.T) {
	prs := probeRoomSetForTest(map[string][]string{"room-small-000001": {"u-1"}}, "room-small-000001")
	mm := NewMembershipModel(prs)
	r := &verifyRun{vc: &verifyConfig{Settle: 7 * time.Second}, mm: mm}

	now := at(100)
	target := &userState{ID: "u-9", Account: "user-9"}

	pc := r.pendingFor(changeAdd, "room-small-000001", target, now)

	assert.Equal(t, changeAdd, pc.kind)
	assert.Equal(t, "room-small-000001", pc.roomID)
	assert.Equal(t, "u-9", pc.userID)
	assert.Equal(t, "user-9", pc.account)
	assert.Equal(t, 0, pc.epoch)
	assert.Equal(t, now.Add(7*time.Second), pc.due,
		"the observation is due one full settle window after the change")

	mm.ApplyAdd("room-small-000001", "u-9", now)
	assert.Equal(t, 1, r.pendingFor(changeRemove, "room-small-000001", target, now).epoch,
		"the pending change must carry the epoch the change created")
}

// TestVerifyRun_Harvest pins the in-place filter. harvest reuses pending's
// backing array via pending[:0], so a bug there silently drops or duplicates
// changes that are still waiting for their settle window.
func TestVerifyRun_Harvest(t *testing.T) {
	observed := new([]string)
	r := &verifyRun{
		vc: &verifyConfig{Settle: time.Second},
		mm: NewMembershipModel(ProbeRoomSet{byRoom: map[string][]string{}}),
		env: &stepEnv{
			request: func(_ context.Context, _ string, _ []byte, _ time.Duration) ([]byte, error) {
				return []byte(`{"subscriptions":[],"hasMore":false}`), nil
			},
		},
	}

	pending := []pendingChange{
		{roomID: "r-1", userID: "u-1", account: "user-1", due: at(10)},
		{roomID: "r-2", userID: "u-2", account: "user-2", due: at(30)},
		{roomID: "r-3", userID: "u-3", account: "user-3", due: at(20)},
		{roomID: "r-4", userID: "u-4", account: "user-4", due: at(40)},
	}
	// Instrument which changes got observed via the oracle request.
	base := r.env.request
	r.env.request = func(ctx context.Context, subj string, data []byte, d time.Duration) ([]byte, error) {
		*observed = append(*observed, subj)
		return base(ctx, subj, data, d)
	}

	keep := r.harvest(t.Context(), pending, at(25))

	require.Len(t, keep, 2)
	assert.Equal(t, "r-2", keep[0].roomID)
	assert.Equal(t, "r-4", keep[1].roomID,
		"surviving changes must keep their relative order through the in-place filter")
	assert.Len(t, *observed, 2, "exactly the two settled changes are observed")
	// This run has no oracle connection, so the authorization probe answers
	// nothing for either settled change. subscription.list answering alone is
	// not resolution — each of the two is charged one unresolved unit, and none
	// of them silently reads as a clean result.
	n, sample := r.takeOracleErrs()
	assert.Equal(t, 2, n)
	require.Error(t, sample)
	assert.Contains(t, sample.Error(), "authorization probe unobservable")
}

// TestVerifyRun_DriveChurn_CancelledWithPendingChanges_CountsUnobserved pins
// the truncated-run accounting. driveChurn leaves its loop on ctx.Done and
// drops whatever is still inside its settle window; ApplyAdd/ApplyRemove have
// already counted those changes in Changes.Total, so without this they read as
// a silent shortfall in Applied/Effective — the report saying
// "1 change / 0 applied / 0 effective" with verdict PASS, which is what a real
// membership_not_applied looks like.
func TestVerifyRun_DriveChurn_CancelledWithPendingChanges_CountsUnobserved(t *testing.T) {
	const roomID = "room-small-000001"
	prs := probeRoomSetForTest(map[string][]string{roomID: {"u-1", "u-2"}}, roomID)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	r := &verifyRun{
		// Seed 1 makes applyChange's first coin flip pick the remove branch, which
		// needs no directPool subscription and so no live NATS connection.
		vc: &verifyConfig{
			Seed: 1, MemberChurn: 600, Settle: time.Hour, MinProbes: 1,
		},
		prs:     prs,
		mm:      NewMembershipModel(prs),
		siteID:  "site-a",
		byID:    map[string]*userState{"u-2": {ID: "u-2", Account: "user-2"}},
		reserve: []string{"u-2"},
	}
	r.env = &stepEnv{
		request: func(_ context.Context, _ string, _ []byte, _ time.Duration) ([]byte, error) {
			// One accepted change, then end the steady window with it still
			// inside its (one hour) settle window.
			cancel()
			return []byte(`{}`), nil
		},
	}

	// issueUntil far ahead: the cancel above, not the tailroom, ends this run.
	r.driveChurn(ctx, time.Now().Add(time.Hour))

	assert.Equal(t, 1, r.mm.Counts().Total, "the change was issued and counted")
	assert.Equal(t, 0, r.mm.Counts().Applied, "and never observed either way")
	assert.Equal(t, 1, r.takeChangesUnobserved(),
		"a change dropped still-pending is a change whose outcome is unknown")
	n, _ := r.takeOracleErrs()
	assert.Zero(t, n, "an unharvested change is not an oracle query failure")
}

func TestVerifyRun_DriveChurn_NoChurnRate_RecordsNothing(t *testing.T) {
	prs := probeRoomSetForTest(map[string][]string{"room-small-000001": {"u-1"}}, "room-small-000001")
	r := &verifyRun{
		vc:  &verifyConfig{Seed: 1, MemberChurn: 0},
		prs: prs,
		mm:  NewMembershipModel(prs),
	}

	r.driveChurn(t.Context(), time.Now().Add(time.Hour))

	assert.Zero(t, r.takeChangesUnobserved())
}

func TestVerifyRun_RecordUnobservedChanges_Accumulates(t *testing.T) {
	r := &verifyRun{}

	r.recordUnobservedChanges(0)
	assert.Zero(t, r.takeChangesUnobserved(), "a clean exit records nothing")

	r.recordUnobservedChanges(3)
	r.recordUnobservedChanges(2)
	assert.Equal(t, 5, r.takeChangesUnobserved())
}

// TestVerifyRun_Observe_FailedOracleQueryIsCounted pins that a failed oracle
// query is counted rather than recorded as an observation: a change nobody could
// check must not land in Applied, and it must not silently vanish either.
func TestVerifyRun_Observe_FailedOracleQueryIsCounted(t *testing.T) {
	r := &verifyRun{
		vc: &verifyConfig{Settle: time.Second},
		mm: NewMembershipModel(ProbeRoomSet{byRoom: map[string][]string{}}),
		env: &stepEnv{
			request: func(_ context.Context, _ string, _ []byte, _ time.Duration) ([]byte, error) {
				return nil, errors.New("subscription.list timeout")
			},
		},
	}
	pc := pendingChange{roomID: "r-1", userID: "u-1", account: "user-1", due: at(10)}

	r.observe(t.Context(), &pc)

	n, sample := r.takeOracleErrs()
	assert.Equal(t, 1, n)
	require.Error(t, sample)
	assert.Contains(t, sample.Error(), "subscription.list timeout")
	assert.NoError(t, r.takeHarnessErr(), "a query failure is not a harness failure")
}

func TestVerifyRun_RecordOracleErr_CountsAndKeepsFirst(t *testing.T) {
	r := &verifyRun{}

	r.recordOracleErr("r-1", "u-1", errors.New("first"))
	r.recordOracleErr("r-2", "u-2", errors.New("second"))
	r.recordOracleErr("r-3", "u-3", errors.New("third"))

	n, sample := r.takeOracleErrs()
	assert.Equal(t, 3, n, "every failure counts, so one blip is distinguishable from a dead service")
	require.Error(t, sample)
	assert.Equal(t, "first", sample.Error())
}

func TestVerifyRun_RecordOracleErr_NoneRecorded(t *testing.T) {
	r := &verifyRun{}

	n, sample := r.takeOracleErrs()
	assert.Zero(t, n)
	assert.NoError(t, sample)
}

func TestVerifyRun_RecordHarnessErr_FirstWins(t *testing.T) {
	r := &verifyRun{}

	r.recordHarnessErr(errors.New("subscribe failed"))
	r.recordHarnessErr(errors.New("later"))

	require.Error(t, r.takeHarnessErr())
	assert.Equal(t, "subscribe failed", r.takeHarnessErr().Error())
	n, _ := r.takeOracleErrs()
	assert.Zero(t, n, "a harness abort must not inflate the oracle failure count")
}

func TestVerifyRun_RecordErrs_Concurrent(t *testing.T) {
	r := &verifyRun{}

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.recordOracleErr("r-1", "u-1", errors.New("boom"))
			r.recordHarnessErr(errors.New("abort"))
		}()
	}
	wg.Wait()

	n, sample := r.takeOracleErrs()
	assert.Equal(t, 50, n)
	assert.Error(t, sample)
	assert.Error(t, r.takeHarnessErr())
}

func TestVerifyRun_Harvest_NothingDue(t *testing.T) {
	r := &verifyRun{vc: &verifyConfig{}}
	pending := []pendingChange{{roomID: "r-1", due: at(30)}}

	keep := r.harvest(t.Context(), pending, at(10))

	assert.Equal(t, pending, keep)
}

func TestVerifyRun_Harvest_Empty(t *testing.T) {
	r := &verifyRun{vc: &verifyConfig{}}
	assert.Empty(t, r.harvest(t.Context(), nil, at(10)))
}

// TestVerifyDailyConfig pins the §6.2 invariant: the direct cap must cover
// every designated user, or a probe recipient spills into the multiplex pool
// and becomes unobservable.
func TestVerifyDailyConfig(t *testing.T) {
	vc := &verifyConfig{
		Preset: "daily-heavy", Users: 5000,
		Warmup: 30 * time.Second, Steady: 120 * time.Second,
	}

	dc := verifyDailyConfig(vc, 10000, 1400)

	assert.Equal(t, "daily-heavy", dc.Preset)
	assert.Equal(t, 5000, dc.Users)
	assert.Equal(t, 30*time.Second, dc.Warmup)
	assert.Equal(t, 120*time.Second, dc.Hold, "verify's steady window is daily's hold")
	assert.Equal(t, 1400, dc.MaxDirectUsers,
		"the direct cap must equal the designated count, not the total")
	assert.Equal(t, 200, dc.MultiplexPoolSize)
}

func TestVerifyDailyConfig_DirectOnly(t *testing.T) {
	vc := &verifyConfig{Preset: "daily-light", DirectOnly: true}

	dc := verifyDailyConfig(vc, 10000, 1400)

	assert.Equal(t, 10000, dc.MaxDirectUsers, "--direct-only gives every user a dedicated conn")
	assert.Zero(t, dc.MultiplexPoolSize)
}

func TestVerifyRun_DropAbsentReserve(t *testing.T) {
	pool := newDirectPool("nats://unused", "", nil)
	pool.users["u-1"] = &directUser{id: "u-1"}
	pool.users["u-3"] = &directUser{id: "u-3"}
	r := &verifyRun{env: &stepEnv{direct: pool}, reserve: []string{"u-1", "u-2", "u-3"}}

	assert.Equal(t, []string{"u-1", "u-3"}, r.dropAbsentReserve(),
		"a floater that never connected cannot be a churn target — its SubscribeRoom "+
			"would fail and abort churn entirely")
}

func TestVerifyRun_DropAbsentReserve_AllPresent(t *testing.T) {
	pool := newDirectPool("nats://unused", "", nil)
	pool.users["u-1"] = &directUser{id: "u-1"}
	r := &verifyRun{env: &stepEnv{direct: pool}, reserve: []string{"u-1"}}

	assert.Equal(t, []string{"u-1"}, r.dropAbsentReserve())
}

func TestVerifyRun_DropAbsentReserve_AllAbsent(t *testing.T) {
	pool := newDirectPool("nats://unused", "", nil)
	r := &verifyRun{env: &stepEnv{direct: pool}, reserve: []string{"u-1", "u-2"}}

	assert.Empty(t, r.dropAbsentReserve())
}

func TestBuildVerifyUsers(t *testing.T) {
	fx := &Fixtures{
		Users: []model.User{
			{ID: "u-1", Account: "user-1"},
			{ID: "u-2", Account: "user-2"},
		},
		Subscriptions: []model.Subscription{
			{RoomID: "room-small-000001", User: model.SubscriptionUser{ID: "u-1"}},
			{RoomID: "room-dm-000001", User: model.SubscriptionUser{ID: "u-1"}},
			{RoomID: "room-small-000001", User: model.SubscriptionUser{ID: "u-2"}},
		},
	}

	users := buildVerifyUsers(fx)

	require.Len(t, users, 2)
	assert.Equal(t, "u-1", users[0].ID)
	assert.Equal(t, "user-1", users[0].Account)
	assert.Equal(t, []string{"room-small-000001", "room-dm-000001"}, users[0].Rooms)
	assert.Equal(t, []string{"room-small-000001"}, users[0].ChannelRooms,
		"DM rooms are not channel rooms")
	assert.Equal(t, []string{"room-small-000001"}, users[1].Rooms)
}

func TestBuildVerifyUsers_UserWithNoSubscriptions(t *testing.T) {
	fx := &Fixtures{Users: []model.User{{ID: "u-1", Account: "user-1"}}}

	users := buildVerifyUsers(fx)

	require.Len(t, users, 1)
	assert.Empty(t, users[0].Rooms)
}

func TestIndexUsersByID(t *testing.T) {
	users := verifyTestUsers(3)

	idx := indexUsersByID(users)

	require.Len(t, idx, 3)
	assert.Same(t, users[1], idx[fmtUserID(1)])
	assert.Nil(t, idx["not-a-user"])
	assert.Empty(t, indexUsersByID(nil))
}

func TestIndexPositions(t *testing.T) {
	users := verifyTestUsers(3)

	idx := indexPositions(users)

	// The position is shouldProbe's userIdx: it must be the fixture index, or
	// two users share a probe stream.
	assert.Equal(t, map[string]int{
		fmtUserID(0): 0, fmtUserID(1): 1, fmtUserID(2): 2,
	}, idx)
	assert.Empty(t, indexPositions(nil))
}

func TestLaneFlags(t *testing.T) {
	tests := []struct {
		name string
		lane string
		want []bool
	}{
		{name: "global only", lane: "global", want: []bool{true}},
		{name: "local only", lane: "local", want: []bool{false}},
		{name: "both", lane: "both", want: []bool{true, false}},
		{name: "unknown falls back to both", lane: "", want: []bool{true, false}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, laneFlags(tt.lane))
		})
	}
}

// TestVerifyRun_EmitOneProbe_NeverRegistersAcrossAChangeBoundary is the
// regression test for the mutation barrier. A probe registered against an epoch
// other than the one in force when it was published is judged against the wrong
// expected-recipient set, which manufactures a missing_recipient (or an
// unexpected_recipient) on a system that did nothing wrong.
//
// The publish stub records the epoch in force at publish time; afterwards every
// tracked probe's registered epoch must equal it. Settle is zeroed so the only
// thing that can hold the boundary is the barrier itself: with it removed, the
// churn goroutine bumps the epoch between the snapshot and the publish (race 1)
// or between the RPC and the model write (race 2) and the assertion fires.
func TestVerifyRun_EmitOneProbe_NeverRegistersAcrossAChangeBoundary(t *testing.T) {
	const roomID = "room-small-000001"
	prs := probeRoomSetForTest(map[string][]string{roomID: {"u-1", "u-2"}}, roomID)
	mm := NewMembershipModel(prs)
	mm.SetSettle(0)
	tracker := NewProbeTracker()

	var mu sync.Mutex
	epochAtPublish := make(map[string]int)

	r := &verifyRun{
		vc:      &verifyConfig{ProbeRate: 1, Seed: 7},
		prs:     prs,
		mm:      mm,
		tracker: tracker,
		siteID:  "site-a",
		idxByID: map[string]int{"u-1": 0},
		env: &stepEnv{
			publish: func(_ context.Context, _ string, data []byte) error {
				var req model.SendMessageRequest
				if err := json.Unmarshal(data, &req); err != nil {
					return err
				}
				// Widen the window a lost barrier would let churn through.
				runtime.Gosched()
				mu.Lock()
				epochAtPublish[req.ID] = mm.Epoch(roomID)
				mu.Unlock()
				return nil
			},
		},
	}

	churnDone := make(chan struct{})
	go func() {
		defer close(churnDone)
		for i := range 400 {
			release := mm.BeginChange(roomID)
			// Stands in for the membership RPC: the model write lands only
			// after it, which is the window race 2 is about.
			runtime.Gosched()
			if i%2 == 0 {
				mm.ApplyAdd(roomID, "u-9", time.Now())
			} else {
				mm.ApplyRemove(roomID, "u-9", time.Now())
			}
			release()
		}
	}()

	sender := &userState{ID: "u-1", Account: "user-1"}
	var seq uint64
	for done := false; !done; {
		select {
		case <-churnDone:
			done = true
		default:
		}
		seq++
		r.emitOneProbe(t.Context(), sender, roomID, seq)
	}
	<-churnDone

	counts := tracker.Counts()
	mu.Lock()
	defer mu.Unlock()
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	require.NotEmpty(t, tracker.probes, "the run must track probes, or it proves nothing")
	for msgID, rec := range tracker.probes {
		want, ok := epochAtPublish[msgID]
		require.True(t, ok, "every registered probe must have been published")
		require.Equal(t, want, rec.epoch,
			"probe %s was registered against epoch %d but published at epoch %d",
			msgID, rec.epoch, want)
		expected := make([]string, 0, len(rec.expected))
		for u := range rec.expected {
			expected = append(expected, u)
		}
		sort.Strings(expected)
		require.Equal(t, mm.MembersAtEpoch(roomID, want), expected,
			"the expected-recipient set must be the one in force at publish")
	}
	assert.Equal(t, len(epochAtPublish), counts.Tracked+counts.Suppressed,
		"every send is either adjudicated or counted as suppressed — a probe blocked by "+
			"the barrier must not vanish from the probe floor's accounting")
}

// churnRandForTest mirrors driveChurn's RNG derivation, so a test seed selects
// the same add/remove branch the real churn driver would take for it.
func churnRandForTest(seed int64) *rand.Rand {
	return rand.New(rand.NewSource(seed ^ 0x0C0FFEE0)) // #nosec G404 -- test fixture randomness // nosemgrep: math-random-used
}

// TestVerifyRun_ApplyChange_ClosesTheRoomBeforeTheMembershipRPC pins *when* the
// barrier goes up. The settle window opens at ApplyAdd/ApplyRemove, which is
// after room-service has answered; if churn only closed the room then, a probe
// published while the RPC was in flight would be adjudicated against the
// pre-change member set while the backend may already have applied the change.
// The assertion runs inside the RPC stub, i.e. exactly in that gap.
func TestVerifyRun_ApplyChange_ClosesTheRoomBeforeTheMembershipRPC(t *testing.T) {
	const roomID = "room-small-000001"
	prs := probeRoomSetForTest(map[string][]string{roomID: {"u-1", "u-2"}}, roomID)
	mm := NewMembershipModel(prs)
	mm.SetSettle(0)

	r := &verifyRun{
		// Seed 1 makes the first coin flip pick the remove branch, which needs
		// no directPool subscription and so no live NATS connection.
		vc:      &verifyConfig{Seed: 1, Settle: 0},
		prs:     prs,
		mm:      mm,
		siteID:  "site-a",
		byID:    map[string]*userState{"u-2": {ID: "u-2", Account: "user-2"}},
		reserve: []string{"u-2"},
	}
	probedDuringRPC := true
	r.env = &stepEnv{
		request: func(_ context.Context, _ string, _ []byte, _ time.Duration) ([]byte, error) {
			probedDuringRPC = mm.WithStableRoom(roomID, time.Now(), func(int, []string) {})
			return []byte(`{}`), nil
		},
	}

	require.True(t, mm.WithStableRoom(roomID, time.Now(), func(int, []string) {}),
		"an idle room is probeable")

	_, ok, err := r.applyChange(t.Context(), roomID, churnRandForTest(1))

	require.NoError(t, err)
	require.True(t, ok, "the change must have been issued, or the stub never ran")
	assert.False(t, probedDuringRPC,
		"the room must already be closed to probes while the membership RPC is in flight")
	assert.True(t, mm.WithStableRoom(roomID, time.Now(), func(int, []string) {}),
		"and reopened once the change has been applied")
}

// TestVerifyRun_ApplyChange_ReopensTheRoomWhenTheRPCIsRejected pins the other
// exit: a rejected change must not leave the room permanently unprobeable, and
// must not bump the epoch.
//
// The rejection here is a *definite* one — room-service answered with an errcode
// envelope. A transport failure reaches the same barrier exit but is not the
// same event; see TestVerifyRun_ApplyChange_TransportErrorIsFatal.
func TestVerifyRun_ApplyChange_ReopensTheRoomWhenTheRPCIsRejected(t *testing.T) {
	const roomID = "room-small-000001"
	prs := probeRoomSetForTest(map[string][]string{roomID: {"u-1", "u-2"}}, roomID)
	mm := NewMembershipModel(prs)

	r := &verifyRun{
		vc:      &verifyConfig{Seed: 1},
		prs:     prs,
		mm:      mm,
		siteID:  "site-a",
		byID:    map[string]*userState{"u-2": {ID: "u-2", Account: "user-2"}},
		reserve: []string{"u-2"},
		env: &stepEnv{
			request: func(_ context.Context, _ string, _ []byte, _ time.Duration) ([]byte, error) {
				return errcodeEnvelopeForTest("forbidden", "not_room_owner", "only owners may remove"), nil
			},
		},
	}

	_, ok, err := r.applyChange(t.Context(), roomID, churnRandForTest(1))

	require.NoError(t, err)
	assert.False(t, ok, "a rejected change is not applied")
	assert.Equal(t, 0, mm.Epoch(roomID), "and never moves the epoch")
	assert.True(t, mm.WithStableRoom(roomID, time.Now(), func(int, []string) {}),
		"the room reopens to probes immediately")
}

// errcodeEnvelopeForTest builds the wire form of an errcode reply. Hand-rolled
// rather than errnats.Marshal so the test pins the *wire* shape verify parses,
// not whatever the marshaller happens to emit today.
func errcodeEnvelopeForTest(code, reason, message string) []byte {
	body := map[string]string{"code": code, "error": message}
	if reason != "" {
		body["reason"] = reason
	}
	out, err := json.Marshal(body)
	if err != nil {
		panic(err)
	}
	return out
}

// churnRunForTest builds a verifyRun whose churn picks the add branch (u-2 is a
// reserve floater not yet in the room) and whose membership RPC answers with
// reply/err.
func churnRunForTest(roomID string, reply func() ([]byte, error)) *verifyRun {
	prs := probeRoomSetForTest(map[string][]string{roomID: {"u-1"}}, roomID)
	return &verifyRun{
		vc:     &verifyConfig{Seed: 1, MemberChurn: 600, Settle: time.Hour, MinProbes: 1},
		prs:    prs,
		mm:     NewMembershipModel(prs),
		siteID: "site-a",
		byID: map[string]*userState{
			"u-1": {ID: "u-1", Account: "user-1"},
			"u-2": {ID: "u-2", Account: "user-2"},
		},
		reserve: []string{"u-2"},
		env: &stepEnv{
			request: func(_ context.Context, _ string, _ []byte, _ time.Duration) ([]byte, error) {
				return reply()
			},
		},
	}
}

// TestVerifyRun_ApplyChange_TransportErrorIsFatal pins the distinction the
// swallow-everything version erased. A transport failure is not "no change":
// room-service may have committed the add while loadgen's model did not, so the
// model and the system have silently diverged and every later probe in that room
// would be judged against a stale expected set. That is a harness failure, the
// same treatment the post-add SubscribeRoom failure already gets.
func TestVerifyRun_ApplyChange_TransportErrorIsFatal(t *testing.T) {
	const roomID = "room-small-000001"
	r := churnRunForTest(roomID, func() ([]byte, error) {
		return nil, errors.New("room-service timeout")
	})

	_, ok, err := r.applyChange(t.Context(), roomID, churnRandForTest(1))

	require.Error(t, err, "an unanswered membership RPC leaves the outcome unknown")
	assert.False(t, ok)
	assert.ErrorContains(t, err, "room-service timeout", "the cause must survive the wrap")
	assert.Equal(t, 0, r.mm.Epoch(roomID), "an unknown outcome never moves the model")
	assert.True(t, r.mm.WithStableRoom(roomID, time.Now(), func(int, []string) {}),
		"and the barrier still releases, so the room is not wedged for the rest of the run")
}

// TestVerifyRun_ApplyChange_DefiniteRejectionIsNotFatal pins the other half: an
// errcode reply means the server answered and refused, so nothing happened and
// nothing diverged. Churn continues; the membership floor is what catches a run
// where every change is refused, because Changes.Total stays 0.
func TestVerifyRun_ApplyChange_DefiniteRejectionIsNotFatal(t *testing.T) {
	const roomID = "room-small-000001"
	r := churnRunForTest(roomID, func() ([]byte, error) {
		return errcodeEnvelopeForTest("forbidden", "not_room_owner", "only owners may add"), nil
	})

	_, ok, err := r.applyChange(t.Context(), roomID, churnRandForTest(1))

	require.NoError(t, err, "a definite refusal is not a harness failure")
	assert.False(t, ok)
	assert.Equal(t, 0, r.mm.Counts().Total,
		"a refused change was never issued, so the floor sees an unexercised dimension")
}

// TestVerifyRun_DriveChurn_TransportErrorAbortsTheRun walks the whole path: a
// transport failure inside applyChange must reach driveChurn as a harness error
// and make the verdict INCONCLUSIVE, never a PASS built on a diverged model.
func TestVerifyRun_DriveChurn_TransportErrorAbortsTheRun(t *testing.T) {
	const roomID = "room-small-000001"
	r := churnRunForTest(roomID, func() ([]byte, error) {
		return nil, errors.New("room-service timeout")
	})

	r.driveChurn(t.Context(), time.Now().Add(time.Hour))

	harnessErr := r.takeHarnessErr()
	require.Error(t, harnessErr, "churn must abort rather than treat the change as a no-op")
	n, _ := r.takeOracleErrs()
	assert.Zero(t, n, "a churn abort is not an oracle query failure and gets no tolerance")

	in := passingInputs()
	in.ChurnRequested = true
	in.HarnessErr = harnessErr
	res := evaluateVerify(in)
	assert.Equal(t, VerdictInconclusive, res.Verdict)
	require.NotEmpty(t, res.Reasons)
	assert.Contains(t, res.Reasons[0], "harness failed during membership setup")
}

// TestVerifyRun_DriveChurn_DefiniteRejectionDoesNotAbort pins that a refused
// change leaves churn running and the change count untouched.
func TestVerifyRun_DriveChurn_DefiniteRejectionDoesNotAbort(t *testing.T) {
	const roomID = "room-small-000001"
	var calls int
	r := churnRunForTest(roomID, func() ([]byte, error) {
		calls++
		return errcodeEnvelopeForTest("forbidden", "not_room_owner", "only owners may add"), nil
	})
	// Three verifyChurnPoll ticks, so a driver that kept running has visibly
	// issued more than once.
	ctx, cancel := context.WithTimeout(t.Context(), 3*verifyChurnPoll+200*time.Millisecond)
	defer cancel()

	r.driveChurn(ctx, time.Now().Add(time.Hour))

	assert.NoError(t, r.takeHarnessErr(), "a definite refusal never aborts churn")
	assert.Greater(t, calls, 1, "churn kept issuing after the refusal")
	assert.Equal(t, 0, r.mm.Counts().Total, "and no refused change is counted as issued")
	assert.Zero(t, r.takeChangesUnobserved(), "nothing was pending, so nothing is unobserved")
}

// TestClassifySendReply pins the authorization oracle's three-way answer. The
// middle row is the load-bearing one: before it, ANY errcode reply counted as
// "rejected", so an unrelated Internal from the gatekeeper *satisfied* the
// post-remove expectation and masked a real membership_remove_ineffective.
func TestClassifySendReply(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want sendOutcome
	}{
		{
			name: "success reply is an accepted send",
			data: []byte(`{"messageId":"m-1","createdAt":1}`),
			want: sendAccepted,
		},
		{
			name: "not_subscribed is the gatekeeper's definite authorization answer",
			data: errcodeEnvelopeForTest("forbidden", "not_subscribed", "not subscribed"),
			want: sendRejected,
		},
		{
			name: "an unrelated internal error answers nothing about authorization",
			data: errcodeEnvelopeForTest("internal", "", "boom"),
			want: sendUnobservable,
		},
		{
			name: "a forbidden carrying a different reason is not an authorization answer",
			data: errcodeEnvelopeForTest("forbidden", "large_room_post_restricted", "too large"),
			want: sendUnobservable,
		},
		{
			name: "a bad_request is not an authorization answer either",
			data: errcodeEnvelopeForTest("bad_request", "", "malformed"),
			want: sendUnobservable,
		},
		{
			name: "an empty reply body is not an error envelope, so the send was accepted",
			data: []byte(`{}`),
			want: sendAccepted,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := classifySendReply(tc.data)
			assert.Equal(t, tc.want, got)
			if tc.want == sendUnobservable {
				assert.Error(t, err, "an unobservable outcome must carry why")
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// observeRunForTest builds a verifyRun whose subscription.list oracle answers
// with rows (or fails) and whose authorization probe is always unobservable —
// oracleNC is nil, the cheapest way to reach sendUnobservable without NATS.
func observeRunForTest(rows string, listErr error) *verifyRun {
	const roomID = "room-small-000001"
	prs := probeRoomSetForTest(map[string][]string{roomID: {"u-1"}}, roomID)
	mm := NewMembershipModel(prs)
	mm.SetSettle(0)
	mm.ApplyAdd(roomID, "u-2", time.Unix(0, 0))
	return &verifyRun{
		vc:     &verifyConfig{Settle: 0},
		prs:    prs,
		mm:     mm,
		siteID: "site-a",
		env: &stepEnv{
			request: func(_ context.Context, _ string, _ []byte, _ time.Duration) ([]byte, error) {
				if listErr != nil {
					return nil, listErr
				}
				return []byte(rows), nil
			},
		},
	}
}

// TestVerifyRun_Observe_UnobservableSendLeavesTheChangeUnresolved pins the
// silent-PASS hole this fixes. subscription.list answering alone makes the change
// Applied but never Effective: the add/remove effectiveness assertion was simply
// not performed, Finalize emits nothing, and before this the run could PASS with
// the check skipped. A change is resolved only when BOTH oracles answered.
func TestVerifyRun_Observe_UnobservableSendLeavesTheChangeUnresolved(t *testing.T) {
	const roomID = "room-small-000001"
	r := observeRunForTest(`{"subscriptions":[{"roomId":"room-small-000001"}],"hasMore":false}`, nil)
	pc := pendingChange{
		kind: changeAdd, roomID: roomID, userID: "u-2", account: "user-2",
		epoch: r.mm.Epoch(roomID), due: at(10),
	}

	r.observe(t.Context(), &pc)

	counts := r.mm.Counts()
	assert.Equal(t, 1, counts.Applied, "subscription.list answered, so Applied is known")
	assert.Equal(t, 0, counts.Effective, "the authorization probe did not, so Effective is not")
	n, sample := r.takeOracleErrs()
	assert.Equal(t, 1, n,
		"a half-answered change is unresolved: it must spend the budget, not vanish")
	require.Error(t, sample)
	assert.Empty(t, r.mm.Finalize(),
		"and it must not produce a violation either — nobody judged it")
}

// TestVerifyRun_Observe_BothOraclesFailingChargeOneUnit pins that the unresolved
// budget counts changes, not queries: two failures still blind exactly one
// change, and double-charging would misreport "N of M changes unresolved".
func TestVerifyRun_Observe_BothOraclesFailingChargeOneUnit(t *testing.T) {
	const roomID = "room-small-000001"
	r := observeRunForTest("", errors.New("subscription.list timeout"))
	pc := pendingChange{
		kind: changeAdd, roomID: roomID, userID: "u-2", account: "user-2",
		epoch: r.mm.Epoch(roomID), due: at(10),
	}

	r.observe(t.Context(), &pc)

	n, sample := r.takeOracleErrs()
	assert.Equal(t, 1, n, "one blinded change is one unresolved change")
	require.Error(t, sample)
	assert.Contains(t, sample.Error(), "subscription.list timeout",
		"the first failure is still the sample")
}

// TestVerifyRun_Observe_UnresolvedSendTripsTheMembershipFloor wires the new
// counter through to the verdict: a run whose only change had an unobservable
// authorization probe has resolved nothing and must not report PASS.
func TestVerifyRun_Observe_UnresolvedSendTripsTheMembershipFloor(t *testing.T) {
	const roomID = "room-small-000001"
	r := observeRunForTest(`{"subscriptions":[{"roomId":"room-small-000001"}],"hasMore":false}`, nil)
	pc := pendingChange{
		kind: changeAdd, roomID: roomID, userID: "u-2", account: "user-2",
		epoch: r.mm.Epoch(roomID), due: at(10),
	}
	r.observe(t.Context(), &pc)

	oracleErrs, sample := r.takeOracleErrs()
	in := passingInputs()
	in.ChurnRequested = true
	in.Changes = r.mm.Counts()
	in.OracleErrs = oracleErrs
	in.OracleErrSample = sample
	in.ChangesUnobserved = r.takeChangesUnobserved()

	res := evaluateVerify(in)

	assert.Equal(t, VerdictInconclusive, res.Verdict,
		"one change, half-observed, is not a membership signal")
}

// TestVerifyRun_SendAsTarget_NoOracleConnIsUnobservable pins that a missing
// oracle connection reports "unknown", never "rejected" — the latter would
// silently satisfy every post-remove expectation in the run.
func TestVerifyRun_SendAsTarget_NoOracleConnIsUnobservable(t *testing.T) {
	r := &verifyRun{vc: &verifyConfig{}}

	got, err := r.sendAsTarget(t.Context(), &pendingChange{roomID: "r-1", account: "user-2"})

	assert.Equal(t, sendUnobservable, got)
	assert.Error(t, err)
}
