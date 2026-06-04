package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

func TestParseShape(t *testing.T) {
	cases := []struct {
		in   string
		want Shape
		err  bool
	}{
		{"users", ShapeUsers, false},
		{"orgs", ShapeOrgs, false},
		{"channels", ShapeChannels, false},
		{"mixed", ShapeMixed, false},
		{"", "", true},
		{"bogus", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, err := ParseShape(tc.in)
			if tc.err {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestValidateInjectShape(t *testing.T) {
	// v1 supports shape=users only. Other shapes are reserved values rejected
	// at validation time. Canonical+channels remains explicitly rejected with
	// a distinct message so the spec's "explicit error" guidance still applies
	// once shapes are widened in v2.
	cases := []struct {
		inject InjectMode
		shape  Shape
		errSub string // empty -> expect no error
	}{
		{InjectFrontdoor, ShapeUsers, ""},
		{InjectCanonical, ShapeUsers, ""},
		{InjectFrontdoor, ShapeOrgs, "shape=orgs not supported"},
		{InjectFrontdoor, ShapeChannels, "shape=channels not supported"},
		{InjectFrontdoor, ShapeMixed, "shape=mixed not supported"},
		{InjectCanonical, ShapeChannels, "incompatible with --inject=canonical"},
	}
	for _, tc := range cases {
		t.Run(string(tc.inject)+"/"+string(tc.shape), func(t *testing.T) {
			err := ValidateInjectShape(tc.inject, tc.shape)
			if tc.errSub == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.errSub)
		})
	}
}

func TestBuiltinMembersPreset(t *testing.T) {
	cases := []string{"members-small", "members-medium", "members-capacity", "members-heavy"}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			p, ok := BuiltinMembersPreset(name)
			require.True(t, ok, "preset %s not registered", name)
			assert.Equal(t, name, p.Name)
			assert.Greater(t, p.Users, 0)
			assert.Greater(t, p.Rooms, 0)
			assert.GreaterOrEqual(t, p.CandidatePool, 1)
			// Sanity: each individual room's baseline+candidate slice must fit
			// in the user pool (some overlap across rooms is fine).
			assert.GreaterOrEqual(t, p.Users, p.BaselineSize+p.CandidatePool,
				"preset %s: Users (%d) < BaselineSize (%d) + CandidatePool (%d)",
				name, p.Users, p.BaselineSize, p.CandidatePool)
		})
	}
}

func TestBuiltinMembersPreset_Unknown(t *testing.T) {
	_, ok := BuiltinMembersPreset("nope")
	assert.False(t, ok)
}

func TestBuildMembersFixtures_Deterministic(t *testing.T) {
	p, _ := BuiltinMembersPreset("members-small")
	a, poolsA := BuildMembersFixtures(&p, 42, "site-A")
	b, poolsB := BuildMembersFixtures(&p, 42, "site-A")
	assert.Equal(t, a.Users, b.Users)
	assert.Equal(t, a.Rooms, b.Rooms)
	assert.Equal(t, a.Subscriptions, b.Subscriptions)
	assert.Equal(t, poolsA, poolsB)
}

func TestBuildMembersFixtures_Shape(t *testing.T) {
	p, _ := BuiltinMembersPreset("members-small")
	f, pools := BuildMembersFixtures(&p, 42, "site-A")

	require.Len(t, f.Users, p.Users)
	require.Len(t, f.Rooms, p.Rooms)

	bySub := map[string]int{}
	for _, s := range f.Subscriptions {
		bySub[s.RoomID]++
	}
	for _, r := range f.Rooms {
		assert.Equal(t, p.BaselineSize, bySub[r.ID], "room %s should have BaselineSize subs", r.ID)
		assert.Equal(t, model.RoomTypeChannel, r.Type)
		assert.Equal(t, p.BaselineSize, r.UserCount)
	}

	ownerCount := map[string]int{}
	for _, s := range f.Subscriptions {
		for _, role := range s.Roles {
			if role == model.RoleOwner {
				ownerCount[s.RoomID]++
			}
		}
	}
	for _, r := range f.Rooms {
		assert.Equal(t, 1, ownerCount[r.ID], "room %s should have exactly one owner", r.ID)
	}

	for _, r := range f.Rooms {
		pool := pools[r.ID]
		require.Len(t, pool, p.CandidatePool, "room %s candidate pool", r.ID)
		seeded := map[string]bool{}
		for _, s := range f.Subscriptions {
			if s.RoomID == r.ID {
				seeded[s.User.Account] = true
			}
		}
		for _, cand := range pool {
			assert.False(t, seeded[cand], "candidate %s already in room %s", cand, r.ID)
		}
	}
}

func TestBuildMembersFixtures_RoomKeys(t *testing.T) {
	p, _ := BuiltinMembersPreset("members-small")
	f, _ := BuildMembersFixtures(&p, 42, "site-A")
	assert.Len(t, f.RoomKeys, p.Rooms)
	for _, r := range f.Rooms {
		_, ok := f.RoomKeys[r.ID]
		assert.True(t, ok, "missing key for room %s", r.ID)
	}
}

func TestOwnersByRoom(t *testing.T) {
	p, _ := BuiltinMembersPreset("members-small")
	f, _ := BuildMembersFixtures(&p, 42, "site-A")

	owners := OwnersByRoom(&f)
	require.Len(t, owners, p.Rooms)
	for _, r := range f.Rooms {
		owner, ok := owners[r.ID]
		require.True(t, ok, "room %s missing owner", r.ID)
		assert.NotEmpty(t, owner)
	}
}
