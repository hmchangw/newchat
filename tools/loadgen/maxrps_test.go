package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultSteps(t *testing.T) {
	msgs, err := parseRPSSteps(defaultSteps("messages"))
	require.NoError(t, err)
	assert.Equal(t, []int{500, 1000, 2000, 5000, 10000}, msgs)

	hist, err := parseRPSSteps(defaultSteps("history"))
	require.NoError(t, err)
	assert.Equal(t, []int{200, 500, 1000, 2000, 5000}, hist)
}

func TestBuildThresholds(t *testing.T) {
	th := buildThresholds(100*time.Millisecond, 250*time.Millisecond, 0.001, 1000, 0.05)
	assert.Equal(t, 100*time.Millisecond, th.P95)
	assert.Equal(t, 250*time.Millisecond, th.P99)
	assert.Equal(t, 0.001, th.ErrorRate)
	assert.Equal(t, uint64(1000), th.PendingGrowth)
	assert.Equal(t, 0.05, th.RateTolerance)
}
