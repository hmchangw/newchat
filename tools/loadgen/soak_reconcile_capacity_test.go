package main

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// soakFailureReconciler.Try advances one observer per claim, so a message costs
// one claim per observer it declares. The count has to come from the observers
// actually enabled: the existing constant assumed history plus search, while
// the run that hit this had history plus recipient — the same two steps, from a
// different pair.
func TestSoakReconcileStepsPerMessage_CountsTheObserversInPlay(t *testing.T) {
	tests := []struct {
		name      string
		recipient bool
		search    bool
		want      int
	}{
		{name: "history alone", want: 1},
		{name: "history and recipient", recipient: true, want: 2},
		{name: "history and search", search: true, want: 2},
		{name: "all three", recipient: true, search: true, want: 3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validSoakConfig(t)
			cfg.RecipientObserverEnabled = tc.recipient
			cfg.SearchObserverEnabled = tc.search
			assert.Equal(t, tc.want, soakReconcileStepsPerMessage(&cfg))
		})
	}
}

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
		{name: "history and recipient does not", recipient: true, wantSteps: 2, wantRequired: 110},
		{name: "all three does not", recipient: true, search: true, wantSteps: 3, wantRequired: 165},
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
	cfg.RecipientObserverEnabled = true
	cfg.LedgerDir = t.TempDir()
	cfg.SearchObserverEnabled = false
	cfg.SendRate = 55
	cfg.ReadRate = 110
	cfg.ReconcileReadShare = 0.75

	assert.NoError(t, validateSoakConfig(&cfg, "soak"))
}

// Below the floor the unresolved backlog grows and messages expire unverified,
// which reads identically to a real storage problem — so the warning has to
// carry every number needed to size the remedy, not just say "too slow".
func TestWarnSoakReconcileCapacity_NamesTheDeficitAndTheRemedy(t *testing.T) {
	cfg := validSoakConfig(t)
	cfg.RecipientObserverEnabled = true
	cfg.SearchObserverEnabled = false
	cfg.SendRate = 55
	cfg.ReadRate = 110
	cfg.ReconcileReadShare = 0.75

	output := captureSoakLog(t)
	warnSoakReconcileCapacity(&cfg)

	logged := output.String()
	require.Contains(t, logged, `"level":"WARN"`)
	assert.Contains(t, logged, `"observerSteps":2`)
	assert.Contains(t, logged, `"requiredOperationsPerSecond":110`)
	assert.Contains(t, logged, `"suppliedOperationsPerSecond":82.5`)
	assert.Contains(t, logged, "SOAK_READ_RATE")
}

func TestWarnSoakReconcileCapacity_SaysNothingWhenTheLaneCanServeTheFloor(t *testing.T) {
	cfg := validSoakConfig(t)
	cfg.RecipientObserverEnabled = true
	cfg.SendRate = 55
	cfg.ReadRate = 200
	cfg.ReconcileReadShare = 0.75

	output := captureSoakLog(t)
	warnSoakReconcileCapacity(&cfg)

	assert.Empty(t, output.String())
}

// A history-only run needs one claim per message, so the same read config that
// cannot serve the recipient observer is fine without it. The warning must
// scale with the observers rather than firing at a fixed ratio.
func TestWarnSoakReconcileCapacity_SaysNothingForAHistoryOnlyRunAtTheSameRates(t *testing.T) {
	cfg := validSoakConfig(t)
	cfg.RecipientObserverEnabled = false
	cfg.SearchObserverEnabled = false
	cfg.SendRate = 55
	cfg.ReadRate = 110
	cfg.ReconcileReadShare = 0.75

	output := captureSoakLog(t)
	warnSoakReconcileCapacity(&cfg)

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
	warnSoakReconcileCapacity(&cfg)

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
