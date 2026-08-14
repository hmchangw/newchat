package main

import (
	"os"
	"testing"

	"github.com/caarlos0/env/v11"
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
