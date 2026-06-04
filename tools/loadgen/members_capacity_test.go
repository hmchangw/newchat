package main

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSustainableOps(t *testing.T) {
	tests := []struct {
		name        string
		pools       CandidatePools
		usersPerAdd int
		want        int
	}{
		{"nil pools", nil, 10, 0},
		{"empty pools", CandidatePools{}, 10, 0},
		{"zero usersPerAdd", CandidatePools{"r1": make([]string, 50)}, 0, 0},
		{"negative usersPerAdd", CandidatePools{"r1": make([]string, 50)}, -1, 0},
		{"single room exact", CandidatePools{"r1": make([]string, 50)}, 10, 5},
		{"single room floor", CandidatePools{"r1": make([]string, 55)}, 10, 5},
		{"multi room", CandidatePools{"r1": make([]string, 50), "r2": make([]string, 30)}, 10, 8},
		{"room below threshold contributes zero", CandidatePools{"r1": make([]string, 9)}, 10, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, SustainableOps(tc.pools, tc.usersPerAdd))
		})
	}
}

func TestValidateSustainedCapacity_WithinBudget(t *testing.T) {
	// capacity = 2 rooms * floor(50/10) = 10 ops; demand = 5/s * 1s = 5 ops.
	pools := CandidatePools{"r1": make([]string, 50), "r2": make([]string, 50)}
	err := ValidateSustainedCapacity("members-x", pools, 5, time.Second, 10)
	assert.NoError(t, err)
}

func TestValidateSustainedCapacity_AtBudgetBoundary(t *testing.T) {
	// capacity = 10 ops; demand = 10/s * 1s = 10 ops — exactly equal must pass.
	pools := CandidatePools{"r1": make([]string, 50), "r2": make([]string, 50)}
	err := ValidateSustainedCapacity("members-x", pools, 10, time.Second, 10)
	assert.NoError(t, err)
}

func TestValidateSustainedCapacity_Oversubscribed(t *testing.T) {
	// capacity = floor(60000/10) = 6000 ops; demand = 100/s * 120s = 12000 ops.
	// Achievable bounds are non-degenerate: maxRate=50, maxDuration=60s.
	pools := CandidatePools{"r1": make([]string, 60000)}
	err := ValidateSustainedCapacity("members-x", pools, 100, 120*time.Second, 10)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInsufficientPool),
		"oversubscribed validation must wrap ErrInsufficientPool")
	// The message must be actionable: name the preset and the achievable bounds.
	msg := err.Error()
	assert.Contains(t, msg, "members-x")
	assert.Contains(t, msg, "--rate 50")
	assert.Contains(t, msg, "--duration 1m0s")
}

func TestValidateSustainedCapacity_TooSmallForWorkload(t *testing.T) {
	// capacity = 25 ops; at rate=100/60s the achievable bounds round to zero,
	// so the message must steer to a larger preset rather than "--rate 0".
	pools := CandidatePools{"r1": make([]string, 250)}
	err := ValidateSustainedCapacity("members-small", pools, 100, 60*time.Second, 10)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInsufficientPool))
	msg := err.Error()
	assert.Contains(t, msg, "members-small")
	assert.Contains(t, msg, "larger preset")
	assert.NotContains(t, msg, "--rate 0", "must not suggest a nonsensical zero rate")
	assert.NotContains(t, msg, "--duration 0s", "must not suggest a nonsensical zero duration")
}

func TestValidateSustainedCapacity_ZeroUsersPerAdd(t *testing.T) {
	pools := CandidatePools{"r1": make([]string, 50)}
	err := ValidateSustainedCapacity("members-x", pools, 10, time.Second, 0)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInsufficientPool))
}

// The default members-sustained invocation (rate=100, duration=60s,
// usersPerAdd=10) must be satisfiable by the members-medium preset so the
// documented out-of-box command completes instead of aborting on exhaustion.
func TestMembersMedium_SustainsDefaultInvocation(t *testing.T) {
	p, ok := BuiltinMembersPreset("members-medium")
	require.True(t, ok)
	_, pools := BuildMembersFixtures(&p, 42, "site-A")

	const (
		defaultRate        = 100
		defaultDuration    = 60 * time.Second
		defaultUsersPerAdd = 10
	)
	require.NoError(t,
		ValidateSustainedCapacity(p.Name, pools, defaultRate, defaultDuration, defaultUsersPerAdd),
		"members-medium must sustain the default 100rps/60s/10-per-add invocation")

	// Stays under MAX_ROOM_SIZE (1000): baseline members + full pool per room.
	for roomID, pool := range pools {
		assert.LessOrEqual(t, p.BaselineSize+len(pool), 1000,
			"room %s baseline+pool must fit under MAX_ROOM_SIZE", roomID)
	}
}

// members-heavy is the high-rate preset: it must sustain rate=1000 for 60s at
// the default users-per-add=10 (60,000 ops) so a 1000 req/s steady-state run
// clears the preflight guard instead of being capped to a short burst.
func TestMembersHeavy_SustainsThousandRPS(t *testing.T) {
	p, ok := BuiltinMembersPreset("members-heavy")
	require.True(t, ok)
	_, pools := BuildMembersFixtures(&p, 42, "site-A")

	const (
		rate        = 1000
		duration    = 60 * time.Second
		usersPerAdd = 10
	)
	require.NoError(t,
		ValidateSustainedCapacity(p.Name, pools, rate, duration, usersPerAdd),
		"members-heavy must sustain rate=1000/60s at users-per-add=10")

	for roomID, pool := range pools {
		assert.LessOrEqual(t, p.BaselineSize+len(pool), 1000,
			"room %s baseline+pool must fit under MAX_ROOM_SIZE", roomID)
	}
}

func TestValidateCapacityTarget(t *testing.T) {
	// Each room grows by full users-per-add batches, so a room of pool P reaches
	// baseline + ⌊P/usersPerAdd⌋*usersPerAdd at most.
	tests := []struct {
		name         string
		pools        CandidatePools
		baseline     int
		targetSize   int
		usersPerAdd  int
		wantErr      bool
		wantContains []string
	}{
		{
			name:     "target below baseline is a no-op",
			pools:    CandidatePools{"r1": make([]string, 0)},
			baseline: 10, targetSize: 5, usersPerAdd: 10,
			wantErr: false,
		},
		{
			name:     "zero usersPerAdd guarded by caller",
			pools:    CandidatePools{"r1": make([]string, 50)},
			baseline: 10, targetSize: 100, usersPerAdd: 0,
			wantErr: false,
		},
		{
			name:     "every room reaches target",
			pools:    CandidatePools{"r1": make([]string, 100), "r2": make([]string, 100)},
			baseline: 10, targetSize: 100, usersPerAdd: 10, // need 9 batches, have 10
			wantErr: false,
		},
		{
			name:     "a room falls short",
			pools:    CandidatePools{"r1": make([]string, 100), "r2": make([]string, 50)},
			baseline: 10, targetSize: 100, usersPerAdd: 10, // r2 has 5 batches < 9 needed
			wantErr:      true,
			wantContains: []string{"members-x", "target-size=100", "--target-size to 60"}, // 10 + 5*10
		},
		{
			name:     "pool below one batch reaches only baseline",
			pools:    CandidatePools{"r1": make([]string, 9)},
			baseline: 10, targetSize: 11, usersPerAdd: 10, // need 1 batch, have 0
			wantErr:      true,
			wantContains: []string{"--target-size to 10"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateCapacityTarget("members-x", tc.pools, tc.baseline, tc.targetSize, tc.usersPerAdd)
			if !tc.wantErr {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.True(t, errors.Is(err, ErrInsufficientPool))
			for _, sub := range tc.wantContains {
				assert.Contains(t, err.Error(), sub)
			}
		})
	}
}

// members-capacity must clear its preflight at the documented growth target.
func TestMembersCapacity_ReachesDocumentedTarget(t *testing.T) {
	p, ok := BuiltinMembersPreset("members-capacity")
	require.True(t, ok)
	_, pools := BuildMembersFixtures(&p, 42, "site-A")

	require.NoError(t,
		ValidateCapacityTarget(p.Name, pools, p.BaselineSize, 500, 10),
		"members-capacity must reach target-size=500")
	// Asking past baseline+pool must be rejected with the reachable ceiling.
	err := ValidateCapacityTarget(p.Name, pools, p.BaselineSize, p.BaselineSize+p.CandidatePool+1, 10)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInsufficientPool))
}
