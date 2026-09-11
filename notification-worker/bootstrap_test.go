package main

import (
	"context"
	"errors"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/stream"
)

type fakeStreamManager struct {
	created  []string
	configs  map[string]jetstream.StreamConfig
	existing map[string]bool // streams that "exist" for the disabled path
	failOn   string          // stream name to fail on; empty = never fail
	failErr  error           // error to return when failing
}

func (f *fakeStreamManager) CreateOrUpdateStream(_ context.Context, cfg jetstream.StreamConfig) (o11ynats.Stream, error) { //nolint:gocritic // hugeParam: cfg is passed by value to satisfy the streamManager interface
	if f.failOn != "" && cfg.Name == f.failOn {
		return nil, f.failErr
	}
	f.created = append(f.created, cfg.Name)
	if f.configs == nil {
		f.configs = map[string]jetstream.StreamConfig{}
	}
	f.configs[cfg.Name] = cfg
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
			name:        "disabled - verifies existing input stream",
			enabled:     false,
			existing:    map[string]bool{"MESSAGES-CANONICAL-test": true},
			wantCreated: nil,
		},
		{
			name:       "disabled - fails when input stream missing",
			enabled:    false,
			existing:   map[string]bool{},
			wantErrSub: "verify stream MESSAGES-CANONICAL-test",
		},
		{
			name:        "enabled - creates input + output streams",
			enabled:     true,
			existing:    map[string]bool{},
			wantCreated: []string{"MESSAGES-CANONICAL-test", "PUSH-NOTIFICATION-test"},
		},
		{
			name:       "enabled - wraps input stream creator error",
			enabled:    true,
			existing:   map[string]bool{},
			failOn:     "MESSAGES-CANONICAL-test",
			failErr:    errors.New("nats down"),
			wantErrSub: "create stream MESSAGES-CANONICAL-test",
		},
		{
			name:       "enabled - wraps output stream creator error",
			enabled:    true,
			existing:   map[string]bool{},
			failOn:     "PUSH-NOTIFICATION-test",
			failErr:    errors.New("nats down"),
			wantErrSub: "create stream PUSH-NOTIFICATION-test",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeStreamManager{failOn: tc.failOn, failErr: tc.failErr, existing: tc.existing}
			err := bootstrapStreams(context.Background(), fake, "MESSAGES-CANONICAL-test", "chat.msg.canonical.test.>", "PUSH-NOTIFICATION-test", "chat.push.notification.test", tc.enabled)
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

// The consumer carries an outage retry budget, so a source message can be redelivered for about an
// hour; each push batch is protected by its Nats-Msg-Id only while the PUSH stream's duplicate window
// covers that span, or a batch already accepted is published again and a duplicate push goes out.
func TestBootstrapStreams_PushStreamDedupWindowCoversTheRetryBudget(t *testing.T) {
	js := &fakeStreamManager{}
	require.NoError(t, bootstrapStreams(context.Background(), js,
		"MESSAGES-CANONICAL-test", "chat.msg.canonical.test.>", "PUSH-NOTIFICATION-test", "chat.push.test.>", true))
	cfg, ok := js.configs["PUSH-NOTIFICATION-test"]
	require.True(t, ok)
	assert.GreaterOrEqual(t, cfg.Duplicates, stream.OutageRetryWindow)
}
