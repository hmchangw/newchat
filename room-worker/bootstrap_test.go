package main

import (
	"context"
	"errors"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Marz32onE/instrumentation-go/otel-nats/oteljetstream"
)

type fakeStreamManager struct {
	created  []string
	existing map[string]bool // streams that "exist" for the disabled path
	failOn   string          // stream name to fail on; empty = never fail
	failErr  error           // error to return when failing
}

// Returns nil for the Stream value because bootstrapStreams discards it.
func (f *fakeStreamManager) CreateOrUpdateStream(_ context.Context, cfg jetstream.StreamConfig) (oteljetstream.Stream, error) { //nolint:gocritic // hugeParam: cfg is passed by value to satisfy the streamManager interface
	if f.failOn != "" && cfg.Name == f.failOn {
		return nil, f.failErr
	}
	f.created = append(f.created, cfg.Name)
	return nil, nil
}

func (f *fakeStreamManager) Stream(_ context.Context, name string) (oteljetstream.Stream, error) {
	if f.existing[name] {
		return nil, nil
	}
	return nil, jetstream.ErrStreamNotFound
}

func TestBootstrapStreams(t *testing.T) {
	tests := []struct {
		name        string
		enabled     bool
		existing    map[string]bool
		failOn      string
		failErr     error
		wantCreated []string
		wantErrSub  string
	}{
		{
			name:        "disabled - verifies existing stream",
			enabled:     false,
			existing:    map[string]bool{"ROOMS_test": true},
			wantCreated: nil,
		},
		{
			name:       "disabled - fails when stream missing",
			enabled:    false,
			existing:   map[string]bool{},
			wantErrSub: "verify ROOMS stream",
		},
		{
			name:        "enabled - creates ROOMS",
			enabled:     true,
			existing:    map[string]bool{},
			wantCreated: []string{"ROOMS_test"},
		},
		{
			name:       "enabled - wraps ROOMS creator error",
			enabled:    true,
			existing:   map[string]bool{},
			failOn:     "ROOMS_test",
			failErr:    errors.New("nats down"),
			wantErrSub: "create ROOMS stream",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeStreamManager{failOn: tc.failOn, failErr: tc.failErr, existing: tc.existing}
			err := bootstrapStreams(context.Background(), fake, "test", tc.enabled)
			if tc.wantErrSub != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrSub)
				if tc.enabled {
					assert.ErrorIs(t, err, tc.failErr)
				} else {
					assert.ErrorIs(t, err, jetstream.ErrStreamNotFound)
				}
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantCreated, fake.created)
		})
	}
}
