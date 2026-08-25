package main

import (
	"os"
	"testing"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/stream"
)

// TestConfig_Mode mirrors broadcast-worker's config_test.go: MODE is
// env:"MODE,required" so the service must fail fast at startup rather than
// silently binding to an undefined stream/subject pairing.
func TestConfig_Mode(t *testing.T) {
	cases := []struct {
		mode    string
		want    stream.Pipeline
		wantErr bool
	}{
		{"user", stream.PipelineUser, false},
		{"bot", stream.PipelineBot, false},
		{"admin", "", true},
		{"", "", true}, // required — missing MODE must fail fast
	}
	for _, tc := range cases {
		name := tc.mode
		if name == "" {
			name = "empty"
		}
		t.Run(name, func(t *testing.T) {
			t.Setenv("MODE", tc.mode) // pin cleanup so host MODE is restored after the test
			if tc.mode == "" {
				require.NoError(t, os.Unsetenv("MODE")) // caarlos0/env treats "" as defined; unset to test the required check
			}
			cfg, err := env.ParseAs[config]()
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, cfg.Mode)
		})
	}
}

// Every online service bounds server selection: a stopped MongoDB goes quiet
// rather than erroring, and the driver's 30s default outlives this worker's
// shutdown budget. Guard the default so a future edit cannot silently restore it.
func TestConfig_BoundsMongoServerSelection(t *testing.T) {
	t.Setenv("MODE", "user")
	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	assert.Equal(t, 2*time.Second, cfg.Pool.ServerSelectionTimeout,
		"an unbounded selection timeout parks the flush loop instead of NAKing")
	assert.Less(t, cfg.Pool.ServerSelectionTimeout, 25*time.Second,
		"must stay under the graceful-shutdown budget")
}

// The default being sane is not the same as a bad override being refused.
// WithPool applies the pool settings, but only Validate rejects the values that
// quietly disable the protection — and a negative selection timeout is read by
// the driver as no bound at all, which is exactly the stopped-Mongo hang this
// worker's back-pressure depends on avoiding. main must call it; this pins the
// contract that makes that call meaningful.
func TestConfig_PoolValidateRejectsAnUnboundedSelectionTimeout(t *testing.T) {
	t.Setenv("MODE", "user")
	t.Setenv("MONGO_SERVER_SELECTION_TIMEOUT", "-1s")

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err, "parsing accepts it; validation is what must refuse it")

	err = cfg.Pool.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MONGO_SERVER_SELECTION_TIMEOUT")
}
