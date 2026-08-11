package main

import (
	"context"
	"errors"
	"math/rand"
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
	n, sample := r.takeOracleErrs()
	assert.Zero(t, n)
	assert.NoError(t, sample)
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
