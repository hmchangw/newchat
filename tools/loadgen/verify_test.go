package main

import (
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
