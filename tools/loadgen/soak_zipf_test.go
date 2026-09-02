package main

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateSoakConfig_RejectsAnUnusableZipfExponent(t *testing.T) {
	cfg := validSoakConfig(t)
	cfg.RoomZipfS = 1.0

	err := validateSoakConfig(&cfg, "keyspace")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "SOAK_ROOM_ZIPF_S")
}

func TestValidateSoakConfig_RejectsAnUnusableZipfOffset(t *testing.T) {
	cfg := validSoakConfig(t)
	cfg.RoomZipfV = 0

	err := validateSoakConfig(&cfg, "keyspace")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "SOAK_ROOM_ZIPF_V")
}

// The config defaults must reproduce the hardcoded constants they replaced,
// or every percentile recorded before the knobs existed becomes incomparable.
func TestSoakZipfConfigDefaultsMatchTheReplacedConstants(t *testing.T) {
	cfg := mustDefaultSoakConfig(t)

	assert.Equal(t, 1.2, cfg.RoomZipfS)
	assert.Equal(t, 1.0, cfg.RoomZipfV)
}

func TestValidateSoakConfig_RejectsNonFiniteZipfParameters(t *testing.T) {
	for name, test := range map[string]struct {
		exponent float64
		offset   float64
		want     string
	}{
		"exponent NaN":  {math.NaN(), 1.0, "SOAK_ROOM_ZIPF_S"},
		"exponent +Inf": {math.Inf(1), 1.0, "SOAK_ROOM_ZIPF_S"},
		"offset NaN":    {1.2, math.NaN(), "SOAK_ROOM_ZIPF_V"},
		"offset +Inf":   {1.2, math.Inf(1), "SOAK_ROOM_ZIPF_V"},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := validSoakConfig(t)
			cfg.RoomZipfS, cfg.RoomZipfV = test.exponent, test.offset

			err := validateSoakConfig(&cfg, "keyspace")

			require.Error(t, err)
			assert.Contains(t, err.Error(), test.want)
		})
	}
}
