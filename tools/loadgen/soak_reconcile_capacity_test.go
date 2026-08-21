package main

import (
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

// The capacity rule already existed but sat behind an early return that only
// let it run when the search observer was on — and the search observer is
// refused at startup, so a recipient-only run was never checked at all. That is
// how a run shipped at 110 claims/s of demand against 82.5/s of capacity, with
// every operation ageing out unverified and nothing saying why.
func TestValidateSoakReconcileCapacity_RefusesARecipientRunItCannotKeepUp(t *testing.T) {
	cfg := validSoakConfig(t)
	cfg.RecipientObserverEnabled = true
	cfg.SearchObserverEnabled = false
	cfg.SendRate = 55
	cfg.ReadRate = 110
	cfg.ReconcileReadShare = 0.75

	err := validateSoakReconcileCapacity(&cfg)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "SOAK_READ_RATE")
	assert.Contains(t, err.Error(), "110.000")
	assert.Contains(t, err.Error(), "82.500")
}

func TestValidateSoakReconcileCapacity_AcceptsAConfigWithEnoughCapacity(t *testing.T) {
	cfg := validSoakConfig(t)
	cfg.RecipientObserverEnabled = true
	cfg.SendRate = 55
	cfg.ReadRate = 200
	cfg.ReconcileReadShare = 0.75

	assert.NoError(t, validateSoakReconcileCapacity(&cfg))
}

// A history-only run needs one claim per message, so the same read config that
// cannot serve the recipient observer is fine without it. The check must scale
// with the observers rather than refusing every run at a fixed ratio.
func TestValidateSoakReconcileCapacity_AcceptsAHistoryOnlyRunAtTheSameRates(t *testing.T) {
	cfg := validSoakConfig(t)
	cfg.RecipientObserverEnabled = false
	cfg.SearchObserverEnabled = false
	cfg.SendRate = 55
	cfg.ReadRate = 110
	cfg.ReconcileReadShare = 0.75

	assert.NoError(t, validateSoakReconcileCapacity(&cfg))
}

// The whole point is that it runs for the configuration that shipped broken, so
// it has to be reached from validateSoakConfig rather than from the search
// branch.
func TestValidateSoakConfig_ChecksReconcileCapacityWithoutTheSearchObserver(t *testing.T) {
	cfg := validSoakConfig(t)
	cfg.RecipientObserverEnabled = true
	cfg.SearchObserverEnabled = false
	cfg.SendRate = 55
	cfg.ReadRate = 110
	cfg.ReconcileReadShare = 0.75

	err := validateSoakConfig(&cfg, "soak")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "2 observer step(s) per message",
		"the recipient-only run must be reached by the capacity check, not skipped with the search branch")
}
