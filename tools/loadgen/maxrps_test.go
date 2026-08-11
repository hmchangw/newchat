package main

import (
	"context"
	"flag"
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

// An unusable SLO bound/target is a flag error, so it must exit 2 like every
// other bad flag rather than running a full ramp and reporting INCONCLUSIVE.
func TestValidateSLORatio(t *testing.T) {
	tests := []struct {
		name      string
		ratio     sloRatio
		wantBound bool
		want      bool
	}{
		{"spec SLO-3", sloRatio{Target: 0.99, Bound: time.Second}, true, true},
		{"spec SLO-7 needs no bound", sloRatio{Target: 0.995}, false, true},
		{"target at one is a valid objective", sloRatio{Target: 1, Bound: time.Second}, true, true},
		{"zero bound when one is required", sloRatio{Target: 0.99}, true, false},
		{"negative bound", sloRatio{Target: 0.99, Bound: -time.Second}, true, false},
		{"target above one", sloRatio{Target: 1.5, Bound: time.Second}, true, false},
		{"zero target", sloRatio{Bound: time.Second}, true, false},
		{"negative target", sloRatio{Target: -0.1, Bound: time.Second}, true, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, validateSLORatio("slox", tt.ratio, tt.wantBound))
		})
	}
}

func TestRunMaxRPS_RejectsBadSLOFlags(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{"slo3 target out of range", []string{"--workload=login", "--preset=small", "--slo3-target=1.5"}},
		{"slo3 latency not positive", []string{"--workload=login", "--preset=small", "--slo3-latency=0"}},
		{"slo7 target out of range", []string{"--workload=search", "--preset=small", "--slo7-target=0"}},
		{"slo8 latency not positive", []string{"--workload=search", "--preset=small", "--slo8-latency=-1s"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Exits before any workload is constructed, so no NATS/HTTP needed.
			assert.Equal(t, 2, runMaxRPS(context.Background(), &config{SiteID: "site-local"}, tt.args))
		})
	}
}

// The spec's values are the defaults; the percentile gates are separate knobs
// and must not move the objective.
func TestRegisterSLORatioFlags_DefaultsMatchSpec(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	slos := registerSLORatioFlags(fs)
	require.NoError(t, fs.Parse(nil))

	assert.Equal(t, sloRatio{Target: 0.99, Bound: time.Second}, slos.slo3())
	assert.Equal(t, sloRatio{Target: 0.995}, slos.slo7(), "SLO-7 has no latency component")
	assert.Equal(t, sloRatio{Target: 0.95, Bound: time.Second}, slos.slo8())
}

// The per-SLO flags are independent of the report percentile gates: moving a
// gate must not move an objective. This is the aliasing the flags replaced.
func TestRegisterSLORatioFlags_IndependentOfPercentileGates(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.Duration("slo-p95", 100*time.Millisecond, "")
	fs.Duration("slo-p99", 250*time.Millisecond, "")
	slos := registerSLORatioFlags(fs)
	require.NoError(t, fs.Parse([]string{"--slo-p95=5ms", "--slo-p99=7ms"}))

	assert.Equal(t, time.Second, slos.slo3().Bound)
	assert.Equal(t, time.Second, slos.slo8().Bound)
}
