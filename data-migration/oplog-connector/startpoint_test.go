package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestResolveStartPoint_Precedence(t *testing.T) {
	cpToken, err := bson.Marshal(bson.M{"_data": "CP_TOKEN"})
	require.NoError(t, err)

	tests := []struct {
		name     string
		cfg      config
		cp       *Checkpoint
		wantKind startKind
		check    func(t *testing.T, sp startPoint)
	}{
		{
			name:     "env resume token wins over checkpoint",
			cfg:      config{StartResumeToken: "ENV_TOKEN", StartMode: "now"},
			cp:       &Checkpoint{ResumeToken: cpToken},
			wantKind: startAfterToken,
			check: func(t *testing.T, sp startPoint) {
				v, lErr := sp.Token.LookupErr("_data")
				require.NoError(t, lErr)
				assert.Equal(t, "ENV_TOKEN", v.StringValue())
			},
		},
		{
			name:     "env start-at-time overrides checkpoint",
			cfg:      config{StartAtTime: "1718100000000", StartMode: "now"},
			cp:       &Checkpoint{ResumeToken: cpToken},
			wantKind: startAtTime,
			check: func(t *testing.T, sp startPoint) {
				assert.Equal(t, int64(1718100000000), sp.TimeMs)
			},
		},
		{
			name:     "checkpoint token used when no override",
			cfg:      config{StartMode: "now"},
			cp:       &Checkpoint{ResumeToken: cpToken},
			wantKind: startAfterToken,
			check: func(t *testing.T, sp startPoint) {
				v, lErr := sp.Token.LookupErr("_data")
				require.NoError(t, lErr)
				assert.Equal(t, "CP_TOKEN", v.StringValue())
			},
		},
		{
			name:     "checkpoint clusterTime fallback when token absent",
			cfg:      config{StartMode: "now"},
			cp:       &Checkpoint{ClusterTime: 1700000000000},
			wantKind: startAtTime,
			check: func(t *testing.T, sp startPoint) {
				assert.Equal(t, int64(1700000000000), sp.TimeMs)
			},
		},
		{
			name:     "cold start now (default)",
			cfg:      config{StartMode: "now"},
			cp:       nil,
			wantKind: startFromNow,
		},
		{
			name:     "cold start time via START_AT_TIME RFC3339",
			cfg:      config{StartMode: "time", StartAtTime: "2024-06-11T00:00:00Z"},
			cp:       nil,
			wantKind: startAtTime,
			check: func(t *testing.T, sp startPoint) {
				assert.Greater(t, sp.TimeMs, int64(0))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sp, err := resolveStartPoint(&tc.cfg, tc.cp)
			require.NoError(t, err)
			assert.Equal(t, tc.wantKind, sp.Kind)
			if tc.check != nil {
				tc.check(t, sp)
			}
		})
	}
}

func TestResolveStartPoint_InvalidStartAtTime(t *testing.T) {
	cfg := config{StartAtTime: "not-a-time", StartMode: "now"}
	_, err := resolveStartPoint(&cfg, nil)
	require.Error(t, err)
}
