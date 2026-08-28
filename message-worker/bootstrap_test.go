package main

import (
	"context"
	"errors"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	o11ynats "github.com/flywindy/o11y/nats"
)

type fakeStreamManager struct {
	created  []string
	existing map[string]bool // streams that "exist" for the disabled path
	failOn   string          // stream name to fail on; empty = never fail
	failErr  error           // error to return when failing
}

// Returns nil for the Stream value because bootstrapStreams discards it.
func (f *fakeStreamManager) CreateOrUpdateStream(_ context.Context, cfg jetstream.StreamConfig) (o11ynats.Stream, error) { //nolint:gocritic // hugeParam: cfg is passed by value to satisfy the streamManager interface
	if f.failOn != "" && cfg.Name == f.failOn {
		return nil, f.failErr
	}
	f.created = append(f.created, cfg.Name)
	return nil, nil
}

func (f *fakeStreamManager) Stream(_ context.Context, name string) (o11ynats.Stream, error) {
	if f.existing[name] {
		return nil, nil
	}
	return nil, jetstream.ErrStreamNotFound
}

func TestBootstrapStreams(t *testing.T) {
	tests := []struct {
		name        string
		mode        string
		enabled     bool
		existing    map[string]bool
		failOn      string
		failErr     error
		wantCreated []string
		wantErrSub  string
	}{
		{
			name:        "disabled - verifies existing stream",
			mode:        "default",
			enabled:     false,
			existing:    map[string]bool{"MESSAGES-CANONICAL-test": true},
			wantCreated: nil,
		},
		{
			name:       "disabled - fails when stream missing",
			mode:       "default",
			enabled:    false,
			existing:   map[string]bool{},
			wantErrSub: "verify MESSAGES-CANONICAL-test stream",
		},
		{
			name:        "enabled - creates MESSAGES-CANONICAL",
			mode:        "default",
			enabled:     true,
			existing:    map[string]bool{},
			wantCreated: []string{"MESSAGES-CANONICAL-test"},
		},
		{
			name:        "disabled - creates nothing",
			mode:        "default",
			enabled:     false,
			existing:    map[string]bool{"MESSAGES-CANONICAL-test": true},
			wantCreated: nil,
		},
		{
			name:       "enabled - wraps MESSAGES-CANONICAL creator error",
			mode:       "default",
			enabled:    true,
			existing:   map[string]bool{},
			failOn:     "MESSAGES-CANONICAL-test",
			failErr:    errors.New("nats down"),
			wantErrSub: "create MESSAGES-CANONICAL-test stream",
		},
		{
			name:        "teams mode disabled - verifies MESSAGES-TEAMS",
			mode:        "teams",
			enabled:     false,
			existing:    map[string]bool{"MESSAGES-TEAMS-test": true},
			wantCreated: nil,
		},
		{
			name:        "teams mode enabled - creates MESSAGES-TEAMS",
			mode:        "teams",
			enabled:     true,
			existing:    map[string]bool{},
			wantCreated: []string{"MESSAGES-TEAMS-test"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeStreamManager{failOn: tc.failOn, failErr: tc.failErr, existing: tc.existing}
			err := bootstrapStreams(context.Background(), fake, "test", tc.mode, tc.enabled)
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
