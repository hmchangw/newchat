package main

import (
	"fmt"
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

func fmtUserID(i int) string           { return fmt.Sprintf("u-%06d", i) }
func fmtAccount(i int) string          { return fmt.Sprintf("user-%d", i) }
func fmtRoomID(b string, i int) string { return fmt.Sprintf("room-%s-%06d", b, i) }
