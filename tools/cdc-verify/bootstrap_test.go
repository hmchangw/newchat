package main

import (
	"context"
	"errors"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBootstrapStreams_Disabled(t *testing.T) {
	// nil js proves the disabled path never touches JetStream at all.
	err := bootstrapStreams(context.Background(), nil, "site1", false)
	require.NoError(t, err)
}

// fakeJetStream embeds a nil jetstream.JetStream (unimplemented methods panic)
// and overrides only CreateOrUpdateStream — all the enabled path uses.
type fakeJetStream struct {
	jetstream.JetStream
	created []jetstream.StreamConfig
	err     error
}

func (f *fakeJetStream) CreateOrUpdateStream(_ context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) { //nolint:gocritic // hugeParam: cfg is passed by value to satisfy jetstream.StreamManager
	f.created = append(f.created, cfg)
	return nil, f.err
}

func TestBootstrapStreams_EnabledCreatesSchemaOnly(t *testing.T) {
	fjs := &fakeJetStream{}
	err := bootstrapStreams(context.Background(), fjs, "site1", true)
	require.NoError(t, err)

	require.Len(t, fjs.created, 1)
	got := fjs.created[0]
	assert.Equal(t, "MIGRATION-OPLOG-site1", got.Name)
	assert.Equal(t, []string{"chat.migration.oplog.site1.>"}, got.Subjects)
	// Federation/consumer config is ops/IaC-owned — never set here.
	assert.Nil(t, got.Sources)
}

func TestBootstrapStreams_EnabledCreateError(t *testing.T) {
	fjs := &fakeJetStream{err: errors.New("boom")}
	err := bootstrapStreams(context.Background(), fjs, "site1", true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create MIGRATION-OPLOG stream")
}
