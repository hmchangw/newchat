package main

import (
	"context"
	"errors"
	"testing"

	o11ynats "github.com/flywindy/o11y/nats"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeStreamManager struct {
	created  []string
	existing map[string]bool
	failOn   string
	failErr  error
}

func (f *fakeStreamManager) CreateOrUpdateStream(_ context.Context, cfg jetstream.StreamConfig) (o11ynats.Stream, error) { //nolint:gocritic // cfg is passed by value to satisfy the streamManager interface
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
		enabled     bool
		existing    map[string]bool
		failOn      string
		failErr     error
		wantCreated []string
		wantErrSub  string
	}{
		{
			name:     "disabled - verifies both existing streams",
			enabled:  false,
			existing: map[string]bool{"BOT_MESSAGES_CANONICAL_test": true, "BOT_PUSH_NOTIF_test": true},
		},
		{
			name:       "disabled - fails when BOT_MESSAGES_CANONICAL missing",
			enabled:    false,
			existing:   map[string]bool{"BOT_PUSH_NOTIF_test": true},
			wantErrSub: "verify BOT_MESSAGES_CANONICAL stream",
		},
		{
			name:       "disabled - fails when BOT_PUSH_NOTIF missing",
			enabled:    false,
			existing:   map[string]bool{"BOT_MESSAGES_CANONICAL_test": true},
			wantErrSub: "verify BOT_PUSH_NOTIF stream",
		},
		{
			name:        "enabled - creates both streams",
			enabled:     true,
			wantCreated: []string{"BOT_MESSAGES_CANONICAL_test", "BOT_PUSH_NOTIF_test"},
		},
		{
			name:       "enabled - wraps BOT_MESSAGES_CANONICAL creator error",
			enabled:    true,
			failOn:     "BOT_MESSAGES_CANONICAL_test",
			failErr:    errors.New("nats down"),
			wantErrSub: "create BOT_MESSAGES_CANONICAL stream",
		},
		{
			name:       "enabled - wraps BOT_PUSH_NOTIF creator error",
			enabled:    true,
			failOn:     "BOT_PUSH_NOTIF_test",
			failErr:    errors.New("nats down"),
			wantErrSub: "create BOT_PUSH_NOTIF stream",
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
