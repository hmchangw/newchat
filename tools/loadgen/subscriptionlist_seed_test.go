package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

// The subscription.list match filter is unforgiving about two fields BuildFixtures
// leaves at their zero value, and a miss on either returns an empty page rather
// than an error — a ramp would then measure the cost of finding nothing. These
// tests pin both, plus the sort-key spread the list's comparator needs.
func TestBuildSubscriptionListFixtures_SubscriptionsSurviveTheListFilter(t *testing.T) {
	p, ok := BuiltinPreset("small")
	require.True(t, ok)
	f := BuildSubscriptionListFixtures(&p, 42, "site-a", time.Now().UTC())

	require.NotEmpty(t, f.Subscriptions)
	roomType := map[string]model.RoomType{}
	for i := range f.Rooms {
		roomType[f.Rooms[i].ID] = f.Rooms[i].Type
	}
	for i := range f.Subscriptions {
		s := &f.Subscriptions[i]
		// match["open"] = {$ne: false}: Open has no bson omitempty, so a false
		// here persists as open:false and the row is filtered out.
		assert.True(t, s.Open, "subscription %s must be open", s.ID)
		// match on current: roomType must be one of dm/channel; "" matches neither.
		assert.Contains(t, []model.RoomType{model.RoomTypeChannel, model.RoomTypeDM}, s.RoomType)
		assert.Equal(t, roomType[s.RoomID], s.RoomType, "roomType must mirror the room")
		// subLite projects name for the self-DM pin and the name tiebreak.
		assert.NotEmpty(t, s.Name)
	}
}

func TestBuildSubscriptionListFixtures_RoomsCarrySpreadSortKeys(t *testing.T) {
	p, ok := BuiltinPreset("medium")
	require.True(t, ok)
	now := time.Now().UTC()
	f := BuildSubscriptionListFixtures(&p, 42, "site-a", now)

	require.NotEmpty(t, f.Rooms)
	seen := map[time.Time]bool{}
	for i := range f.Rooms {
		require.NotNil(t, f.Rooms[i].LastMsgAt, "room %s needs a sort key", f.Rooms[i].ID)
		assert.False(t, f.Rooms[i].LastMsgAt.After(now), "sort keys stay in the past")
		seen[*f.Rooms[i].LastMsgAt] = true
	}
	// An all-equal LastMsgAt would collapse the comparator onto its name
	// tiebreak and hide the real sort cost, so require a genuine spread.
	assert.Greater(t, len(seen), len(f.Rooms)/2, "LastMsgAt must be spread across rooms")
}

func TestBuildSubscriptionListFixtures_DeterministicForSeed(t *testing.T) {
	p, ok := BuiltinPreset("small")
	require.True(t, ok)
	now := time.Now().UTC()

	a := BuildSubscriptionListFixtures(&p, 7, "site-a", now)
	b := BuildSubscriptionListFixtures(&p, 7, "site-a", now)
	assert.Equal(t, a.Rooms, b.Rooms)
	assert.Equal(t, a.Subscriptions, b.Subscriptions)

	c := BuildSubscriptionListFixtures(&p, 8, "site-a", now)
	assert.NotEqual(t, a.Rooms, c.Rooms, "a different seed must move the sort keys")
}

// Every account the generator can pick must own at least one row, or the ramp
// silently measures empty pages for that slice of its account space.
func TestBuildSubscriptionListFixtures_AccountsHaveSubscriptions(t *testing.T) {
	p, ok := BuiltinPreset("small")
	require.True(t, ok)
	f := BuildSubscriptionListFixtures(&p, 42, "site-a", time.Now().UTC())

	byAccount := map[string]int{}
	for i := range f.Subscriptions {
		byAccount[f.Subscriptions[i].User.Account]++
	}
	assert.NotEmpty(t, byAccount)
	for acct, n := range byAccount {
		assert.Positive(t, n, "account %s has no subscriptions", acct)
	}
}

// The uniform presets give every account exactly one subscription, so a ramp on
// them measures a one-row page rather than a sidebar cold-open — fast, and
// meaningless. This pins the numbers the workload's startup warning keys on, so
// a preset change that quietly degrades the workload fails here.
func TestSubscriptionsPerAccount_SeparatesSidebarPresetsFromDegenerateOnes(t *testing.T) {
	tests := []struct {
		preset       string
		wantSidebar  bool
		atLeastCount float64
	}{
		{preset: "small", wantSidebar: false},
		{preset: "medium", wantSidebar: false},
		{preset: "realistic", wantSidebar: true, atLeastCount: 2},
	}
	for _, tc := range tests {
		t.Run(tc.preset, func(t *testing.T) {
			p, ok := BuiltinPreset(tc.preset)
			require.True(t, ok)
			f := BuildSubscriptionListFixtures(&p, 42, "site-a", time.Now().UTC())

			mean := subscriptionsPerAccount(&f)
			assert.Positive(t, mean)
			if tc.wantSidebar {
				assert.GreaterOrEqual(t, mean, tc.atLeastCount)
				assert.False(t, degeneratePageFixtures(&f))
			} else {
				assert.Less(t, mean, minSidebarSubsPerAccount)
				assert.True(t, degeneratePageFixtures(&f))
			}
		})
	}
}

func TestSubscriptionsPerAccount_EmptyFixturesAreDegenerate(t *testing.T) {
	empty := Fixtures{}
	assert.InDelta(t, 0.0, subscriptionsPerAccount(&empty), 0.001)
	assert.True(t, degeneratePageFixtures(&empty))
}
