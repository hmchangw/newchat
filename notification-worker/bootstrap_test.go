package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/stream"
)

type fakeStreamManager struct {
	created   []string
	configs   map[string]jetstream.StreamConfig
	existing  map[string]bool          // streams that "exist" for the disabled path
	dedup     map[string]time.Duration // duplicate window of each existing stream (zero = server default)
	lookupErr map[string]error         // transient lookup failure per stream, distinct from absence
	failOn    string                   // stream name to fail on; empty = never fail
	failErr   error                    // error to return when failing
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
	if err := f.lookupErr[name]; err != nil {
		return nil, err
	}
	if f.existing[name] {
		return &fakeStream{info: &jetstream.StreamInfo{Config: jetstream.StreamConfig{Name: name, Duplicates: f.dedup[name]}}}, nil
	}
	return nil, jetstream.ErrStreamNotFound
}

// fakeStream is the looked-up stream handle; only CachedInfo is consulted.
type fakeStream struct {
	o11ynats.Stream
	info *jetstream.StreamInfo
}

func (s *fakeStream) CachedInfo() *jetstream.StreamInfo { return s.info }

func TestBootstrapStreams(t *testing.T) {
	tests := []struct {
		name        string
		enabled     bool
		existing    map[string]bool
		dedup       map[string]time.Duration
		lookupErr   map[string]error
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
			name:        "disabled - accepts an output stream whose duplicate window covers the retry budget",
			enabled:     false,
			existing:    map[string]bool{"MESSAGES-CANONICAL-test": true, "PUSH-NOTIFICATION-test": true},
			dedup:       map[string]time.Duration{"PUSH-NOTIFICATION-test": stream.OutageRetryWindow},
			wantCreated: nil,
		},
		{
			name:       "disabled - refuses an output stream whose duplicate window is shorter than the retry budget",
			enabled:    false,
			existing:   map[string]bool{"MESSAGES-CANONICAL-test": true, "PUSH-NOTIFICATION-test": true},
			dedup:      map[string]time.Duration{"PUSH-NOTIFICATION-test": 2 * time.Minute},
			wantErrSub: "PUSH-NOTIFICATION-test duplicate window 2m0s is shorter than",
		},
		{
			name:      "disabled - refuses to start when the output stream cannot be looked up",
			enabled:   false,
			existing:  map[string]bool{"MESSAGES-CANONICAL-test": true},
			lookupErr: map[string]error{"PUSH-NOTIFICATION-test": errors.New("i/o timeout")},
			// A transient error is not absence: treating it as such skips the only duplicate-window
			// check, so a short deployed window would go unnoticed and republish accepted batches.
			wantErrSub: "verify stream PUSH-NOTIFICATION-test",
		},
		{
			name:       "disabled - fails when input stream missing",
			enabled:    false,
			existing:   map[string]bool{},
			wantErrSub: "verify stream MESSAGES-CANONICAL-test",
		},
		{
			name:        "enabled - creates input + output + retry streams",
			enabled:     true,
			existing:    map[string]bool{},
			wantCreated: []string{"MESSAGES-CANONICAL-test", "PUSH-NOTIFICATION-test", "RETRY-test"},
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
		{
			name:       "enabled - wraps retry stream creator error",
			enabled:    true,
			existing:   map[string]bool{},
			failOn:     "RETRY-test",
			failErr:    errors.New("nats down"),
			wantErrSub: "create stream RETRY-test",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeStreamManager{failOn: tc.failOn, failErr: tc.failErr, existing: tc.existing, dedup: tc.dedup, lookupErr: tc.lookupErr}
			err := bootstrapStreams(context.Background(), fake, "MESSAGES-CANONICAL-test", "chat.msg.canonical.test.>", "PUSH-NOTIFICATION-test", "chat.push.notification.test", "RETRY-test", "chat.retry.test.>", tc.enabled)
			if tc.wantErrSub != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrSub)
				switch {
				case tc.enabled:
					assert.ErrorIs(t, err, tc.failErr)
				case len(tc.lookupErr) > 0:
				case len(tc.dedup) == 0:
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
		"MESSAGES-CANONICAL-test", "chat.msg.canonical.test.>", "PUSH-NOTIFICATION-test", "chat.push.test.>",
		"RETRY-test", "chat.retry.test.>", true))
	cfg, ok := js.configs["PUSH-NOTIFICATION-test"]
	require.True(t, ok)
	assert.GreaterOrEqual(t, cfg.Duplicates, stream.OutageRetryWindow)
}
