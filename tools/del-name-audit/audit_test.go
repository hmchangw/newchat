package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHasDelPrefix(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want bool
	}{
		{"exact prefix", "Del-general", true},
		{"prefix only", "Del-", true},
		{"no prefix", "general", false},
		{"empty", "", false},
		{"case differs", "del-general", false},
		{"prefix not at start", "room Del-general", false},
		{"partial marker", "Del", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, hasDelPrefix(tc.in))
		})
	}
}

func TestAudit_AllAgree(t *testing.T) {
	rep := audit(&auditInput{
		delRooms: []roomRef{{ID: "r1", Name: "Del-general", Type: "channel"}},
		subsForRooms: []subRef{
			{ID: "s1", Name: "Del-general", RoomID: "r1", RoomType: "channel", SiteID: "site-a"},
			{ID: "s2", Name: "Del-general", RoomID: "r1", RoomType: "channel", SiteID: "site-a"},
		},
		sampleCap: 5,
	})

	assert.Equal(t, 1, rep.DelRooms)
	assert.Equal(t, 2, rep.SubsScanned)
	assert.Equal(t, 2, rep.Agree)
	assert.Zero(t, rep.Disagree)
	assert.Zero(t, rep.RoomsWithNoSubs)
	assert.Empty(t, rep.Samples)
	assert.True(t, rep.safeToMoveFilter())
	require.Contains(t, rep.ByRoomType, "channel")
	assert.Equal(t, 2, rep.ByRoomType["channel"].Agree)
}

func TestAudit_DisagreeIsFatalAndSampled(t *testing.T) {
	rep := audit(&auditInput{
		delRooms: []roomRef{{ID: "r1", Name: "Del-general", Type: "channel"}},
		subsForRooms: []subRef{
			{ID: "s1", Name: "Del-general", RoomID: "r1", RoomType: "channel"},
			{ID: "s2", Name: "general", RoomID: "r1", RoomType: "channel", SiteID: "site-b"},
		},
		sampleCap: 5,
	})

	assert.Equal(t, 1, rep.Agree)
	assert.Equal(t, 1, rep.Disagree)
	assert.False(t, rep.safeToMoveFilter(), "a stale subscription name must block the rewrite")
	require.Len(t, rep.Samples, 1)
	assert.Equal(t, "disagree", rep.Samples[0].Kind)
	assert.Equal(t, "s2", rep.Samples[0].SubID)
	assert.Equal(t, "general", rep.Samples[0].SubName)
	assert.Equal(t, "Del-general", rep.Samples[0].RoomName)
}

// A DM subscription's name is the counterpart account, not the room name, so the
// per-type split must keep a DM divergence visible rather than folding it into
// the channel totals.
func TestAudit_PerRoomTypeSplit(t *testing.T) {
	rep := audit(&auditInput{
		delRooms: []roomRef{
			{ID: "r1", Name: "Del-general", Type: "channel"},
			{ID: "r2", Name: "Del-alice-bob", Type: "dm"},
		},
		subsForRooms: []subRef{
			{ID: "s1", Name: "Del-general", RoomID: "r1", RoomType: "channel"},
			{ID: "s2", Name: "bob", RoomID: "r2", RoomType: "dm"},
			{ID: "s3", Name: "alice", RoomID: "r2", RoomType: "dm"},
		},
		sampleCap: 10,
	})

	require.Contains(t, rep.ByRoomType, "channel")
	require.Contains(t, rep.ByRoomType, "dm")
	assert.Equal(t, 1, rep.ByRoomType["channel"].Agree)
	assert.Zero(t, rep.ByRoomType["channel"].Disagree)
	assert.Zero(t, rep.ByRoomType["dm"].Agree)
	assert.Equal(t, 2, rep.ByRoomType["dm"].Disagree)
	assert.False(t, rep.safeToMoveFilter())
}

func TestAudit_FalsePositiveBlocksRewrite(t *testing.T) {
	rep := audit(&auditInput{
		delNamedSubs: []subRef{
			{ID: "s1", Name: "Del-report", RoomID: "r9", RoomType: "channel"},
		},
		roomsForSubs: map[string]roomRef{
			"r9": {ID: "r9", Name: "quarterly-report", Type: "channel"},
		},
		sampleCap: 5,
	})

	assert.Equal(t, 1, rep.DelNamedSubs)
	assert.Equal(t, 1, rep.FalsePositive)
	assert.Zero(t, rep.Unverifiable)
	assert.False(t, rep.safeToMoveFilter(), "a Del- name over a live room would drop a real room")
	require.Len(t, rep.Samples, 1)
	assert.Equal(t, "falsePositive", rep.Samples[0].Kind)
}

// A Del--named sub whose room has no local doc is cross-site: excluded under both
// the join and the name filter, so it must not block the rewrite.
func TestAudit_UnverifiableDoesNotBlock(t *testing.T) {
	rep := audit(&auditInput{
		delNamedSubs: []subRef{{ID: "s1", Name: "Del-remote", RoomID: "rx", RoomType: "channel"}},
		roomsForSubs: map[string]roomRef{},
		sampleCap:    5,
	})

	assert.Equal(t, 1, rep.Unverifiable)
	assert.Zero(t, rep.FalsePositive)
	assert.True(t, rep.safeToMoveFilter())
}

func TestAudit_DelNamedSubOverDelRoomIsNotAFalsePositive(t *testing.T) {
	rep := audit(&auditInput{
		delNamedSubs: []subRef{{ID: "s1", Name: "Del-general", RoomID: "r1", RoomType: "channel"}},
		roomsForSubs: map[string]roomRef{"r1": {ID: "r1", Name: "Del-general"}},
		sampleCap:    5,
	})

	assert.Zero(t, rep.FalsePositive)
	assert.Zero(t, rep.Unverifiable)
	assert.True(t, rep.safeToMoveFilter())
}

func TestAudit_RoomWithNoSubscriptions(t *testing.T) {
	rep := audit(&auditInput{
		delRooms: []roomRef{
			{ID: "r1", Name: "Del-general", Type: "channel"},
			{ID: "r2", Name: "Del-empty", Type: "channel"},
		},
		subsForRooms: []subRef{{ID: "s1", Name: "Del-general", RoomID: "r1", RoomType: "channel"}},
		sampleCap:    5,
	})

	assert.Equal(t, 2, rep.DelRooms)
	assert.Equal(t, 1, rep.SubsScanned)
	assert.Equal(t, 1, rep.RoomsWithNoSubs)
	assert.True(t, rep.safeToMoveFilter())
}

// Defensive: a sub returned for a room outside the scanned set is ignored rather
// than counted against a room the audit never listed.
func TestAudit_SubForUnlistedRoomIgnored(t *testing.T) {
	rep := audit(&auditInput{
		delRooms:     []roomRef{{ID: "r1", Name: "Del-general", Type: "channel"}},
		subsForRooms: []subRef{{ID: "s9", Name: "other", RoomID: "r-unknown", RoomType: "channel"}},
		sampleCap:    5,
	})

	assert.Zero(t, rep.SubsScanned)
	assert.Zero(t, rep.Disagree)
	assert.Equal(t, 1, rep.RoomsWithNoSubs)
}

func TestAudit_EmptyInput(t *testing.T) {
	rep := audit(&auditInput{sampleCap: 5})

	assert.Zero(t, rep.DelRooms)
	assert.Zero(t, rep.SubsScanned)
	assert.Empty(t, rep.Samples)
	assert.NotNil(t, rep.ByRoomType)
	assert.True(t, rep.safeToMoveFilter(), "no Del- data at all is trivially safe")
}

func TestAudit_SampleCap(t *testing.T) {
	subs := make([]subRef, 0, 10)
	for i := 0; i < 10; i++ {
		subs = append(subs, subRef{ID: "s", Name: "live", RoomID: "r1", RoomType: "channel"})
	}

	t.Run("caps collected samples", func(t *testing.T) {
		rep := audit(&auditInput{
			delRooms:     []roomRef{{ID: "r1", Name: "Del-general"}},
			subsForRooms: subs,
			sampleCap:    3,
		})
		assert.Equal(t, 10, rep.Disagree, "counts are never capped")
		assert.Len(t, rep.Samples, 3)
	})

	t.Run("zero cap disables sampling but keeps counts", func(t *testing.T) {
		rep := audit(&auditInput{
			delRooms:     []roomRef{{ID: "r1", Name: "Del-general"}},
			subsForRooms: subs,
			sampleCap:    0,
		})
		assert.Equal(t, 10, rep.Disagree)
		assert.Empty(t, rep.Samples)
	})
}

func TestDistinctRoomIDs(t *testing.T) {
	tests := []struct {
		name string
		in   []subRef
		want []string
	}{
		{"empty", nil, []string{}},
		{
			"dedupes preserving first-seen order",
			[]subRef{{RoomID: "b"}, {RoomID: "a"}, {RoomID: "b"}, {RoomID: "c"}, {RoomID: "a"}},
			[]string{"b", "a", "c"},
		},
		{"single", []subRef{{RoomID: "a"}}, []string{"a"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, distinctRoomIDs(tc.in))
		})
	}
}

func TestChunk(t *testing.T) {
	tests := []struct {
		name string
		in   []string
		size int
		want [][]string
	}{
		{"empty", nil, 3, nil},
		{"zero size", []string{"a"}, 0, nil},
		{"negative size", []string{"a"}, -1, nil},
		{"exact multiple", []string{"a", "b", "c", "d"}, 2, [][]string{{"a", "b"}, {"c", "d"}}},
		{"trailing partial", []string{"a", "b", "c"}, 2, [][]string{{"a", "b"}, {"c"}}},
		{"size exceeds input", []string{"a"}, 10, [][]string{{"a"}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, chunk(tc.in, tc.size))
		})
	}
}
