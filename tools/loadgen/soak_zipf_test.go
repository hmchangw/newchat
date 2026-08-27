package main

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// math/rand's Zipf returns a nil generator for s <= 1 rather than an error, so
// an out-of-range value would surface as a nil dereference on the first send
// instead of a refusal at startup.
func TestNewSoakRoomPicker_RejectsAnExponentTheGeneratorCannotHonor(t *testing.T) {
	for _, s := range []float64{1.0, 0.8, 0} {
		_, err := newSoakRoomPicker(1, 100, s, 1.0)
		require.Error(t, err, "s=%v must be refused", s)
		assert.Contains(t, err.Error(), "greater than 1")
	}
}

func TestNewSoakRoomPicker_RejectsAnOffsetBelowOne(t *testing.T) {
	_, err := newSoakRoomPicker(1, 100, 1.2, 0.5)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at least 1")
}

func TestNewSoakRoomPicker_AcceptsTheDefaults(t *testing.T) {
	picker, err := newSoakRoomPicker(1, 100, soakDefaultRoomZipfS, soakDefaultRoomZipfV)
	require.NoError(t, err)
	require.NotNil(t, picker)
	assert.Less(t, picker.Next(), 100)
}

// The offset is the flattening knob: raising it spreads the same traffic over
// more rooms, which is the only way to model a workspace whose busiest channel
// is not a fifth of the whole site (s cannot go below 1).
func TestNewSoakRoomPicker_AHigherOffsetSpreadsTheLoad(t *testing.T) {
	const rooms, draws = 1000, 20000
	hot := func(v float64) int {
		picker, err := newSoakRoomPicker(7, rooms, 1.2, v)
		require.NoError(t, err)
		count := 0
		for range draws {
			if picker.Next() == 0 {
				count++
			}
		}
		return count
	}

	assert.Greater(t, hot(1.0), hot(10.0),
		"v=1 must concentrate more traffic on the hottest room than v=10")
}

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

// The defaults must reproduce the hardcoded constants this replaced, or every
// percentile recorded before the knob existed becomes incomparable.
func TestSoakZipfDefaultsMatchTheReplacedConstants(t *testing.T) {
	assert.Equal(t, 1.2, soakDefaultRoomZipfS)
	assert.Equal(t, 1.0, soakDefaultRoomZipfV)
}

// math/rand's guard is `s <= 1`, which NaN and +Inf both slip past, and the
// generator it then returns is NOT nil — its Uint64 spins forever. A soak
// started with SOAK_ROOM_ZIPF_S=NaN would hang on the first send with no error
// and no log, which is worse than the nil dereference the bounds exist to stop.
// strconv.ParseFloat accepts both spellings, so env can deliver them.
func TestNewSoakRoomPicker_RejectsNonFiniteParameters(t *testing.T) {
	for name, tc := range map[string]struct{ s, v float64 }{
		"exponent NaN":  {math.NaN(), 1.0},
		"exponent +Inf": {math.Inf(1), 1.0},
		"offset NaN":    {1.2, math.NaN()},
		"offset +Inf":   {1.2, math.Inf(1)},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := newSoakRoomPicker(1, 100, tc.s, tc.v)
			require.Error(t, err)
		})
	}
}

func TestValidateSoakConfig_RejectsNonFiniteZipfParameters(t *testing.T) {
	for name, tc := range map[string]struct {
		s, v float64
		want string
	}{
		"exponent NaN":  {math.NaN(), 1.0, "SOAK_ROOM_ZIPF_S"},
		"exponent +Inf": {math.Inf(1), 1.0, "SOAK_ROOM_ZIPF_S"},
		"offset NaN":    {1.2, math.NaN(), "SOAK_ROOM_ZIPF_V"},
		"offset +Inf":   {1.2, math.Inf(1), "SOAK_ROOM_ZIPF_V"},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := validSoakConfig(t)
			cfg.RoomZipfS, cfg.RoomZipfV = tc.s, tc.v

			err := validateSoakConfig(&cfg, "keyspace")

			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}
