package main

import (
	"math/rand" // #nosec G404 -- load generator randomness, never used for secrets // nosemgrep: math-random-used
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestUserState_StepTransitions(t *testing.T) {
	u := newUserState("u-1", "user-1", []string{"r-1"}, 42)
	u.activeProb = 0.5
	u.idleProb = 0.5
	r := rand.New(rand.NewSource(1))
	activeSeen, idleSeen := false, false
	for i := 0; i < 1000; i++ {
		u.step(r)
		if u.active {
			activeSeen = true
		} else {
			idleSeen = true
		}
	}
	require.True(t, activeSeen)
	require.True(t, idleSeen)
}

func TestPickAction_WeightsApproximatelyMatch(t *testing.T) {
	w := defaultActionWeights()
	r := rand.New(rand.NewSource(7))
	counts := map[actionKind]int{}
	const N = 100000
	for i := 0; i < N; i++ {
		counts[pickAction(r, w)]++
	}
	// Send should dominate (largest weight). Mute/Create should be rare.
	require.Greater(t, counts[actionSend], counts[actionMarkRead])
	require.Greater(t, counts[actionMarkRead], counts[actionScrollHistory])
	require.Less(t, counts[actionMuteToggle], counts[actionRoomCreate]+counts[actionMemberAdd]+10) // tiny
}

func TestActionRate_PerSecond(t *testing.T) {
	// daily-heavy: 60+25+3+5+0.5+0.2+0.2 = 93.9 actions/day = 0.00326/sec per user
	r := actionRatePerSecond(defaultActionWeights().totalPerDay(), 8*time.Hour)
	require.InDelta(t, 0.00326, r, 0.0002)
}

func TestNeighborOf(t *testing.T) {
	cases := []struct {
		account string
		want    string
	}{
		{"user-0", "user-1"},
		{"user-1", "user-0"},
		{"user-9999", "user-9998"},
		{"not-a-user-account", "user-0"}, // fallback
		{"", "user-0"},
	}
	for _, tc := range cases {
		require.Equal(t, tc.want, neighborOf(tc.account), "account=%q", tc.account)
	}
}

func TestNewUserState_ChannelRoomsExcludesDMs(t *testing.T) {
	rooms := []string{
		"room-dm-000001",
		"room-small-000007",
		"room-dm-000042",
		"room-medium-000003",
		"room-large-000000",
	}
	u := newUserState("u-1", "user-1", rooms, 0)
	require.Equal(t, rooms, u.Rooms, "Rooms preserved verbatim")
	require.Equal(t,
		[]string{"room-small-000007", "room-medium-000003", "room-large-000000"},
		u.ChannelRooms,
		"ChannelRooms drops DMs by ID prefix and preserves order of the rest")
}
