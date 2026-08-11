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

// fakeStreamManager records CreateOrUpdateStream calls.
type fakeStreamManager struct {
	configs []jetstream.StreamConfig
	err     error
}

//nolint:gocritic // hugeParam: signature must satisfy the streamManager interface (jetstream.StreamConfig is passed by value there).
func (f *fakeStreamManager) CreateOrUpdateStream(_ context.Context, cfg jetstream.StreamConfig) (o11ynats.Stream, error) {
	f.configs = append(f.configs, cfg)
	return nil, f.err
}

func TestBootstrapStreams_Disabled_NoOp(t *testing.T) {
	fsm := &fakeStreamManager{}
	require.NoError(t, bootstrapStreams(context.Background(), fsm, "site1", false))
	assert.Empty(t, fsm.configs)
}

func TestBootstrapStreams_Enabled_CreatesDROplog(t *testing.T) {
	fsm := &fakeStreamManager{}
	require.NoError(t, bootstrapStreams(context.Background(), fsm, "site1", true))
	require.Len(t, fsm.configs, 1)
	assert.Equal(t, "DR-OPLOG-site1", fsm.configs[0].Name)
	assert.Equal(t, []string{"chat.dr.oplog.site1.>"}, fsm.configs[0].Subjects)
}

func TestBootstrapStreams_Error_Wrapped(t *testing.T) {
	fsm := &fakeStreamManager{err: errors.New("nats down")}
	err := bootstrapStreams(context.Background(), fsm, "site1", true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "DR-OPLOG")
}
