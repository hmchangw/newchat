package threadcount

import (
	"testing"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mounted mirrors how a service holds the policy: a named field, so the env
// names and their defaults stay declared in this package only.
type mounted struct {
	Thread Policy
}

func TestPolicy_ParsesDefaults(t *testing.T) {
	var cfg mounted
	require.NoError(t, env.Parse(&cfg))

	assert.Equal(t, DefaultPolicy(), cfg.Thread,
		"unset environment must produce exactly the compiled-in tuning")
}

func TestPolicy_ParsesOverrides(t *testing.T) {
	t.Setenv("THREAD_COUNT_SCAN_LIMIT", "250")
	t.Setenv("THREAD_COUNT_REANCHOR_BUDGET", "10")
	t.Setenv("THREAD_COUNT_RECONCILE_ROW_LIMIT", "9000")

	var cfg mounted
	require.NoError(t, env.Parse(&cfg))

	assert.Equal(t, Policy{ScanLimit: 250, ReanchorBudget: 10, ReconcileRowLimit: 9000}, cfg.Thread)
}

func TestPolicy_Validate(t *testing.T) {
	tests := []struct {
		name    string
		policy  Policy
		wantErr string
	}{
		{
			name:   "the shipped tuning is valid",
			policy: DefaultPolicy(),
		},
		{
			name:   "re-anchoring may be turned off",
			policy: Policy{ScanLimit: 1000, ReanchorBudget: 0, ReconcileRowLimit: 50000},
		},
		{
			name:   "a recount may be left uncapped",
			policy: Policy{ScanLimit: 1000, ReanchorBudget: 50, ReconcileRowLimit: 0},
		},
		{
			name:   "counting the whole thread every time is allowed, however costly",
			policy: Policy{ScanLimit: 100_000_000, ReanchorBudget: 50, ReconcileRowLimit: 50000},
		},
		{
			name:    "a scan limit of zero would read every reply of an uncounted thread",
			policy:  Policy{ScanLimit: 0, ReanchorBudget: 50, ReconcileRowLimit: 50000},
			wantErr: "THREAD_COUNT_SCAN_LIMIT",
		},
		{
			name:    "a negative scan limit is meaningless",
			policy:  Policy{ScanLimit: -1, ReanchorBudget: 50, ReconcileRowLimit: 50000},
			wantErr: "THREAD_COUNT_SCAN_LIMIT",
		},
		{
			name:    "a negative re-anchor budget is meaningless",
			policy:  Policy{ScanLimit: 1000, ReanchorBudget: -1, ReconcileRowLimit: 50000},
			wantErr: "THREAD_COUNT_REANCHOR_BUDGET",
		},
		{
			name:    "a negative recount cap is meaningless",
			policy:  Policy{ScanLimit: 1000, ReanchorBudget: 50, ReconcileRowLimit: -1},
			wantErr: "THREAD_COUNT_RECONCILE_ROW_LIMIT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.policy.Validate()
			if tt.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr,
				"the error must name the environment variable an operator has to fix")
		})
	}
}
