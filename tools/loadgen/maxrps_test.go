package main

import (
	"context"
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

func TestDiagnoseBottleneck_NilGuards(t *testing.T) {
	th := buildThresholds(100*time.Millisecond, 250*time.Millisecond, 0.001, 1000, 0.05)
	tripped := []rpsStepResult{{TargetRPS: 1000, Kind: verdictPass}, {TargetRPS: 2000, Kind: verdictTrip}}
	enabled := func() bottleneckConfig { return bottleneckConfig{Enabled: true, PromURL: "http://prom:9090"} }

	// non-messages workload -> nil
	assert.Nil(t, diagnoseBottleneck(context.Background(), &config{Bottleneck: enabled()}, "history", tripped, th))
	// disabled -> nil
	assert.Nil(t, diagnoseBottleneck(context.Background(), &config{Bottleneck: bottleneckConfig{Enabled: false, PromURL: "http://prom:9090"}}, "messages", tripped, th))
	// no prom url -> nil
	assert.Nil(t, diagnoseBottleneck(context.Background(), &config{Bottleneck: bottleneckConfig{Enabled: true, PromURL: ""}}, "messages", tripped, th))
	// no trip -> nil
	noTrip := []rpsStepResult{{TargetRPS: 1000, Kind: verdictPass}}
	assert.Nil(t, diagnoseBottleneck(context.Background(), &config{Bottleneck: enabled()}, "messages", noTrip, th))
}
