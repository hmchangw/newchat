package main

import (
	"bytes"
	"math/rand" // #nosec G404 -- load generator randomness, never used for secrets // nosemgrep: math-random-used
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

func TestBuiltinPresets_ContainsAllFour(t *testing.T) {
	names := []string{"small", "medium", "large", "realistic"}
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			p, ok := BuiltinPreset(name)
			require.True(t, ok, "preset %q must exist", name)
			assert.Equal(t, name, p.Name)
			assert.Greater(t, p.Users, 0)
			assert.Greater(t, p.Rooms, 0)
		})
	}
}

func TestBuiltinPresets_UnknownReturnsFalse(t *testing.T) {
	_, ok := BuiltinPreset("nonexistent")
	assert.False(t, ok)
}

func TestBuiltinPresets_UniformShape(t *testing.T) {
	for _, name := range []string{"small", "medium", "large"} {
		t.Run(name, func(t *testing.T) {
			p, ok := BuiltinPreset(name)
			require.True(t, ok)
			assert.Equal(t, DistUniform, p.RoomSizeDist)
			assert.Equal(t, DistUniform, p.SenderDist)
			assert.InDelta(t, 0.0, p.MentionRate, 1e-9)
			assert.InDelta(t, 0.0, p.ThreadRate, 1e-9)
		})
	}
}

func TestBuiltinPresets_RealisticShape(t *testing.T) {
	p, ok := BuiltinPreset("realistic")
	require.True(t, ok)
	assert.Equal(t, DistMixed, p.RoomSizeDist)
	assert.Equal(t, DistZipf, p.SenderDist)
	assert.Greater(t, p.MentionRate, 0.0)
	assert.Greater(t, p.ThreadRate, 0.0)
	assert.Greater(t, p.ContentBytes.Max, p.ContentBytes.Min)
}

func TestBuildFixtures_DeterministicAcrossCalls(t *testing.T) {
	p, _ := BuiltinPreset("small")
	a := BuildFixtures(&p, 42, "site-local")
	b := BuildFixtures(&p, 42, "site-local")
	assert.Equal(t, a.Users, b.Users)
	assert.Equal(t, a.Rooms, b.Rooms)
	assert.Equal(t, a.Subscriptions, b.Subscriptions)
	assert.Equal(t, a.RoomKeys, b.RoomKeys)
}

func TestBuildFixtures_RoomKeysOnePerRoom(t *testing.T) {
	p, _ := BuiltinPreset("small")
	f := BuildFixtures(&p, 42, "site-local")
	require.Len(t, f.RoomKeys, len(f.Rooms))
	for _, r := range f.Rooms {
		pair, ok := f.RoomKeys[r.ID]
		require.True(t, ok, "missing key for room %s", r.ID)
		assert.Len(t, pair.PrivateKey, 32)
	}
}

func TestBuildFixtures_RoomKeysDifferAcrossSeeds(t *testing.T) {
	p, _ := BuiltinPreset("small")
	a := BuildFixtures(&p, 42, "site-local")
	b := BuildFixtures(&p, 43, "site-local")
	require.Len(t, a.RoomKeys, len(a.Rooms))
	require.Equal(t, len(a.RoomKeys), len(b.RoomKeys))
	// At least one room should have a different key.
	differ := false
	for id, pa := range a.RoomKeys {
		pb := b.RoomKeys[id]
		if !bytes.Equal(pa.PrivateKey, pb.PrivateKey) {
			differ = true
			break
		}
	}
	assert.True(t, differ, "seeds 42 and 43 must produce different keys")
}

func TestBuildFixtures_SmallCountsAndShape(t *testing.T) {
	p, _ := BuiltinPreset("small")
	f := BuildFixtures(&p, 42, "site-local")
	assert.Len(t, f.Users, 10)
	assert.Len(t, f.Rooms, 5)
	// uniform: every user is in at least one room
	users := make(map[string]bool)
	for _, s := range f.Subscriptions {
		users[s.User.ID] = true
		assert.Equal(t, "site-local", s.SiteID)
	}
	assert.Len(t, users, 10)
	for _, r := range f.Rooms {
		assert.Equal(t, "channel", string(r.Type))
		assert.Equal(t, "site-local", r.SiteID)
	}
}

func TestBuildFixtures_RealisticMixesChannelAndDM(t *testing.T) {
	p, _ := BuiltinPreset("realistic")
	f := BuildFixtures(&p, 42, "site-local")
	var channels, dms int
	for _, r := range f.Rooms {
		switch r.Type { //nolint:exhaustive
		case "channel":
			channels++
		case "dm":
			dms++
		}
	}
	assert.Greater(t, channels, 0)
	assert.Greater(t, dms, 0)
	// DM rooms must have exactly 2 members
	dmMembers := make(map[string]int)
	for _, s := range f.Subscriptions {
		for _, r := range f.Rooms {
			if r.ID == s.RoomID && r.Type == "dm" {
				dmMembers[r.ID]++
			}
		}
	}
	for id, n := range dmMembers {
		assert.Equal(t, 2, n, "dm room %s must have 2 members", id)
	}
}

func TestBuildFixtures_FewerUsersThanRooms_PadsToTwoMembers(t *testing.T) {
	// Synthetic preset: 3 users, 5 rooms — round-robin alone leaves rooms 3
	// and 4 with fewer than 2 members, exercising the padding branch.
	p := &Preset{
		Name: "tiny", Users: 3, Rooms: 5,
		RoomSizeDist: DistUniform, SenderDist: DistUniform,
		ContentBytes: Range{Min: 200, Max: 200},
	}
	f := BuildFixtures(p, 42, "site-local")
	require.Len(t, f.Rooms, 5)
	for i := range f.Rooms {
		assert.GreaterOrEqual(t, f.Rooms[i].UserCount, 2,
			"room %s must have at least 2 members after padding", f.Rooms[i].ID)
	}
}

func TestSampleWithoutReplacement_CapsAtUserCount(t *testing.T) {
	// Requesting more samples than users available silently caps at len(users).
	r := rand.New(rand.NewSource(1))
	users := []model.User{{ID: "u-0"}, {ID: "u-1"}}
	out := sampleWithoutReplacement(r, users, 99)
	assert.Len(t, out, 2)
}

func TestBuildFixtures_DailyBands(t *testing.T) {
	p, _ := BuiltinPreset("daily-heavy")
	p.Users = 200 // shrink for test speed; bands stay the same
	f := BuildFixtures(&p, 42, "site-test")

	require.Equal(t, 200, len(f.Users))

	// Per-user subscription count must equal p.DailyBands.RoomsPerUser
	want := p.DailyBands.RoomsPerUser()
	perUser := map[string]int{}
	for _, s := range f.Subscriptions {
		perUser[s.User.ID]++
	}
	for _, u := range f.Users {
		require.Equal(t, want, perUser[u.ID],
			"user %s wrong subscription count", u.ID)
	}

	// Each band must yield at least one room with the band's size range.
	sizes := map[string]int{}
	for _, r := range f.Rooms {
		sizes[r.ID] = r.UserCount
	}
	var nDM, nSmall, nMed, nLarge int
	for _, sz := range sizes {
		switch {
		case sz == 2:
			nDM++
		case sz >= 5 && sz <= 20:
			nSmall++
		case sz >= 50 && sz <= 200:
			nMed++
		case sz >= 500 && sz <= 2000:
			nLarge++
		}
	}
	require.Greater(t, nDM, 0)
	require.Greater(t, nSmall, 0)
	require.Greater(t, nMed, 0)
	require.Greater(t, nLarge, 0)

	// Determinism: same seed yields identical fixtures.
	f2 := BuildFixtures(&p, 42, "site-test")
	require.Equal(t, f, f2)
}

func TestBuiltinPreset_Daily(t *testing.T) {
	cases := []struct {
		name  string
		users int
		bands DailyBands
	}{
		{"daily-light", 10000, DailyBands{DMs: 15, Small: 10, Medium: 5, Large: 2}},
		{"daily-heavy", 10000, DailyBands{DMs: 25, Small: 20, Medium: 8, Large: 3}},
		{"daily-power", 10000, DailyBands{DMs: 40, Small: 30, Medium: 10, Large: 3}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, ok := BuiltinPreset(tc.name)
			require.True(t, ok, "preset %s missing", tc.name)
			require.Equal(t, tc.users, p.Users)
			require.Equal(t, tc.bands, p.DailyBands)
		})
	}
}

// TestBuildFixtures_DailyHeavy_FastAtScale locks in the band-picker fixes.
// Prior to them, both the DM-band picker (O(N²) without stub-pairing) and
// the small/medium-band weighted-scan picker (O(N×perUser×R)) made fixture
// build unusable at production scale — N=10000 would take ~10+ min, N=100k
// hours. With stub-pairing for DM and the shuffled slot-bag picker for the
// other bands, N=10000 completes in roughly a second. 30s is generous
// ceiling for an occasionally-slow CI runner.
func TestBuildFixtures_DailyHeavy_FastAtScale(t *testing.T) {
	if testing.Short() {
		t.Skip("scale test")
	}
	p, _ := BuiltinPreset("daily-heavy")
	p.Users = 10000
	start := time.Now()
	f := BuildFixtures(&p, 42, "site-test")
	elapsed := time.Since(start)
	t.Logf("BuildFixtures(N=10000) elapsed=%s rooms=%d subs=%d",
		elapsed, len(f.Rooms), len(f.Subscriptions))
	require.Less(t, elapsed, 30*time.Second, "fixture build regressed; was %s", elapsed)

	// Every user should have exactly RoomsPerUser subscriptions.
	want := p.DailyBands.RoomsPerUser()
	perUser := map[string]int{}
	for _, s := range f.Subscriptions {
		perUser[s.User.ID]++
	}
	for _, u := range f.Users {
		require.Equal(t, want, perUser[u.ID], "user %s wrong subscription count", u.ID)
	}
}
