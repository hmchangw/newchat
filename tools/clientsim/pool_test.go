package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func poolAccounts(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = string(rune('a' + i))
	}
	return out
}

func TestShardSlice_PartitionsExactlyTarget(t *testing.T) {
	pool := poolAccounts(10)
	cases := []struct {
		name          string
		target, count int
		wantSizes     []int
	}{
		{"target below pool, floor partition not ceil", 10, 3, []int{3, 3, 4}},
		{"target 1 shard-count 3 opens exactly 1 conn", 1, 3, []int{0, 0, 1}},
		{"target above pool clamps to pool", 99, 2, []int{5, 5}},
		{"zero target means whole pool", 0, 2, []int{5, 5}},
		{"single shard", 7, 1, []int{7}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			total := 0
			seen := map[string]bool{}
			for i := 0; i < tt.count; i++ {
				s, err := shardSlice(pool, tt.target, i, tt.count)
				require.NoError(t, err)
				assert.Equal(t, tt.wantSizes[i], len(s), "shard %d size", i)
				for _, a := range s {
					assert.False(t, seen[a], "account %s appears in two shards", a)
					seen[a] = true
				}
				total += len(s)
			}
			want := tt.target
			if want == 0 || want > len(pool) {
				want = len(pool)
			}
			assert.Equal(t, want, total, "shards must partition exactly T")
		})
	}
}

func TestShardSlice_InvalidInputs(t *testing.T) {
	pool := poolAccounts(4)
	for _, tt := range []struct {
		name                 string
		target, index, count int
	}{
		{"index >= count", 4, 2, 2},
		{"negative index", 4, -1, 2},
		{"zero count", 4, 0, 0},
		{"negative target", -1, 0, 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := shardSlice(pool, tt.target, tt.index, tt.count)
			assert.Error(t, err)
		})
	}
}
