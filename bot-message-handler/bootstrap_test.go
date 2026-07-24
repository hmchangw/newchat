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

func (f *fakeStreamManager) CreateOrUpdateStream(_ context.Context, cfg jetstream.StreamConfig) (o11ynats.Stream, error) { //nolint:gocritic // cfg by value to satisfy interface
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
		{name: "disabled - verifies existing", enabled: false, existing: map[string]bool{"BOT_MESSAGES_CANONICAL_test": true}},
		{name: "disabled - fails when missing", enabled: false, wantErrSub: "verify BOT_MESSAGES_CANONICAL stream"},
		{name: "enabled - creates stream", enabled: true, wantCreated: []string{"BOT_MESSAGES_CANONICAL_test"}},
		{name: "enabled - wraps creator error", enabled: true, failOn: "BOT_MESSAGES_CANONICAL_test", failErr: errors.New("nats down"), wantErrSub: "create BOT_MESSAGES_CANONICAL stream"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeStreamManager{failOn: tc.failOn, failErr: tc.failErr, existing: tc.existing}
			err := bootstrapStreams(context.Background(), fake, "test", tc.enabled)
			if tc.wantErrSub != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrSub)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantCreated, fake.created)
		})
	}
}
