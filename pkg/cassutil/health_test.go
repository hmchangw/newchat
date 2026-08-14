package cassutil

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHealthCheckFor(t *testing.T) {
	probeErr := errors.New("gocql: no connections available")

	tests := []struct {
		name    string
		probe   prober
		wantErr error
	}{
		{
			name:  "reachable cluster is ready",
			probe: func(context.Context) error { return nil },
		},
		{
			name:    "unreachable cluster is not ready",
			probe:   func(context.Context) error { return probeErr },
			wantErr: probeErr,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			check := healthCheckFor(tt.probe)
			assert.Equal(t, "cassandra", check.Name)

			err := check.Probe(context.Background())

			if tt.wantErr == nil {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.ErrorIs(t, err, tt.wantErr, "the driver error must stay unwrappable")
			assert.Contains(t, err.Error(), "cassandra")
		})
	}
}

// The readiness handler cancels its context on timeout; the probe must receive
// that context so a hung cluster cannot stall the response.
func TestHealthCheckFor_PassesContextToProbe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var got context.Context
	check := healthCheckFor(func(ctx context.Context) error {
		got = ctx
		return ctx.Err()
	})

	err := check.Probe(ctx)

	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, ctx, got)
}
