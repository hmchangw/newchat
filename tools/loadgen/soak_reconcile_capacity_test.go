package main

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The arithmetic behind the floor, pinned separately from how it is reported so
// the reporting can change without losing the numbers.
func TestSoakReconcileCapacityFor_ScalesDemandWithTheObserversEnabled(t *testing.T) {
	tests := []struct {
		name         string
		recipient    bool
		search       bool
		wantSteps    int
		wantRequired float64
		sufficient   bool
	}{
		{name: "history alone fits", wantSteps: 1, wantRequired: 55, sufficient: true},
		{
			name:      "the event-mode recipient observer adds no read step",
			recipient: true, wantSteps: 1, wantRequired: 55, sufficient: true,
		},
		{name: "the search probe does not fit", search: true, wantSteps: 2, wantRequired: 110},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validSoakConfig(t)
			cfg.RecipientObserverEnabled = tc.recipient
			cfg.SearchObserverEnabled = tc.search
			cfg.SendRate = 55
			cfg.ReadRate = 110
			cfg.ReconcileReadShare = 0.75

			capacity := soakReconcileCapacityFor(&cfg)

			assert.Equal(t, tc.wantSteps, capacity.Steps)
			assert.InDelta(t, tc.wantRequired, capacity.Required, 0.001)
			assert.InDelta(t, 82.5, capacity.Supplied, 0.001)
			assert.Equal(t, tc.sufficient, capacity.Sufficient())
		})
	}
}

// The deficit is reported and the run continues. Refusing at startup wastes a
// whole deploy on a configuration mistake that a warning states just as
// clearly, and the operator can decide whether a degraded lane is worth the
// window.
func TestValidateSoakConfig_AcceptsARunBelowTheReconcileFloor(t *testing.T) {
	cfg := validSoakConfig(t)
	cfg.SendRate = 55
	cfg.ReadRate = 50
	cfg.ReconcileReadShare = 0.75

	assert.NoError(t, validateSoakConfig(&cfg, "soak"))
}

// Below the floor the unresolved backlog grows and messages expire unverified,
// which reads identically to a real storage problem — so the warning has to
// carry every number needed to size the remedy, not just say "too slow".
func TestWarnSoakReconcileCapacity_NamesTheDeficitAndTheRemedy(t *testing.T) {
	cfg := validSoakConfig(t)
	cfg.SearchObserverEnabled = true
	cfg.SendRate = 55
	cfg.ReadRate = 110
	cfg.ReconcileReadShare = 0.75

	output := captureSoakLog(t)
	warnSoakReconcileCapacity(&cfg, nil)

	logged := output.String()
	require.Contains(t, logged, `"level":"WARN"`)
	assert.Contains(t, logged, `"observerSteps":2`)
	assert.Contains(t, logged, `"requiredOperationsPerSecond":110`)
	assert.Contains(t, logged, `"suppliedOperationsPerSecond":82.5`)
	assert.Contains(t, logged, "SOAK_READ_RATE")
}

// A warning in the log is not machine-readable, and below the floor every
// message eventually expires unverified — the run produces evidence it cannot
// stand behind. Marking it through the invalidation counter is what lets a
// dashboard and an operator see, from the run's own output, that the window was
// inconclusive from the first second rather than degraded partway through.
func TestWarnSoakReconcileCapacity_MarksTheRunInvalidFromStartup(t *testing.T) {
	cfg := validSoakConfig(t)
	cfg.SearchObserverEnabled = true
	cfg.SendRate = 55
	cfg.ReadRate = 110
	cfg.ReconcileReadShare = 0.75
	metrics := NewMetrics()

	captureSoakLog(t)
	warnSoakReconcileCapacity(&cfg, metrics)

	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.FailureInvalidations.WithLabelValues(invalidReasonReconcileCapacity)))
}

func TestWarnSoakReconcileCapacity_DoesNotInvalidateARunThatFits(t *testing.T) {
	cfg := validSoakConfig(t)
	cfg.SendRate = 55
	cfg.ReadRate = 200
	cfg.ReconcileReadShare = 0.75
	metrics := NewMetrics()

	captureSoakLog(t)
	warnSoakReconcileCapacity(&cfg, metrics)

	assert.Zero(t, testutil.ToFloat64(
		metrics.FailureInvalidations.WithLabelValues(invalidReasonReconcileCapacity)))
}

// The reason has to be in the bounded registry, or a run recovering the same
// invalidation from its journal would record it as "other" and lose the cause.
func TestFailureInvalidationRegistry_KnowsTheReconcileCapacityReason(t *testing.T) {
	_, known := failureInvalidationReasonRegistry[invalidReasonReconcileCapacity]
	assert.True(t, known)
}

func TestWarnSoakReconcileCapacity_SaysNothingWhenTheLaneCanServeTheFloor(t *testing.T) {
	cfg := validSoakConfig(t)
	cfg.RecipientObserverEnabled = true
	cfg.SendRate = 55
	cfg.ReadRate = 200
	cfg.ReconcileReadShare = 0.75

	output := captureSoakLog(t)
	warnSoakReconcileCapacity(&cfg, nil)

	assert.Empty(t, output.String())
}

// The configuration that shipped broken — 55 sends/s against 110 read/s at a
// 0.75 share — now fits, because the recipient observer it enables costs the
// read lane nothing. The warning must scale with the observers that query
// rather than firing at a fixed ratio.
func TestWarnSoakReconcileCapacity_SaysNothingForAHistoryOnlyRunAtTheSameRates(t *testing.T) {
	cfg := validSoakConfig(t)
	cfg.RecipientObserverEnabled = true
	cfg.SearchObserverEnabled = false
	cfg.SendRate = 55
	cfg.ReadRate = 110
	cfg.ReconcileReadShare = 0.75

	output := captureSoakLog(t)
	warnSoakReconcileCapacity(&cfg, nil)

	assert.Empty(t, output.String())
}

// A seed or teardown phase configures a send rate it never uses, so the floor
// does not apply and the warning would be noise.
func TestWarnSoakReconcileCapacity_SaysNothingWithoutASendRate(t *testing.T) {
	cfg := validSoakConfig(t)
	cfg.RecipientObserverEnabled = true
	cfg.SendRate = 0
	cfg.ReadRate = 1
	cfg.ReconcileReadShare = 0.75

	output := captureSoakLog(t)
	warnSoakReconcileCapacity(&cfg, nil)

	assert.Empty(t, output.String())
}

func captureSoakLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&output, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &output
}
