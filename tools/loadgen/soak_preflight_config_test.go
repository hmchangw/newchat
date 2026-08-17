package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSoakConfig_EncryptionPreflightDefaultsOn(t *testing.T) {
	cfg := mustDefaultSoakConfig(t)

	assert.True(t, cfg.EncryptionPreflight,
		"skipping the encrypted-write check must be opted into, never inherited")
	assert.Equal(t, 30*time.Second, cfg.EncryptionPreflightTimeout)
}

func TestValidateSoakConfig_EncryptionPreflightMayOnlyBeSkippedOutsideStaging(t *testing.T) {
	tests := []struct {
		name        string
		environment string
		wantErr     bool
	}{
		{name: "local", environment: "local"},
		{name: "test", environment: "test"},
		{name: "staging", environment: "staging", wantErr: true},
		{name: "production", environment: "production", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validSoakConfig(t)
			cfg.Environment = tt.environment
			cfg.EncryptionPreflight = false

			err := validateSoakConfig(&cfg, "soak_keyspace")

			if !tt.wantErr {
				require.NoError(t, err)
				return
			}
			require.Error(t, err,
				"a promoted environment must not be able to skip the check")
			assert.Contains(t, err.Error(), "SOAK_ENCRYPTION_PREFLIGHT")
		})
	}
}

func TestValidateSoakConfig_EncryptionPreflightTimeoutMustBePositive(t *testing.T) {
	cfg := validSoakConfig(t)
	cfg.EncryptionPreflightTimeout = 0

	err := validateSoakConfig(&cfg, "soak_keyspace")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "SOAK_ENCRYPTION_PREFLIGHT_TIMEOUT")
}

// A disabled preflight must be consulted before anything is sent. Passing nil
// collaborators is the probe: a flag that is stored but never read would reach
// them, which is exactly how six lane rates were once configured and ignored.
func TestRunSoakEncryptionPreflight_DisabledSendsNothing(t *testing.T) {
	err := runSoakEncryptionPreflight(
		context.Background(),
		soakEncryptionPreflightConfig{Enabled: false, Timeout: time.Second},
		nil, nil, nil, nil,
	)

	require.NoError(t, err)
}
