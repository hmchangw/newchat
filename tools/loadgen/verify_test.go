package main

import (
	"context"
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
