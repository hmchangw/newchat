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
	created   []jetstream.StreamConfig
	streamErr error // returned by Stream()
	streamHit []string
}

func (f *fakeStreamManager) CreateOrUpdateStream(_ context.Context, cfg jetstream.StreamConfig) (o11ynats.Stream, error) { //nolint:gocritic // hugeParam: cfg is passed by value to satisfy the streamManager interface
	f.created = append(f.created, cfg)
	return nil, nil
}

func (f *fakeStreamManager) Stream(_ context.Context, name string) (o11ynats.Stream, error) {
	f.streamHit = append(f.streamHit, name)
	return nil, f.streamErr
}

func TestBootstrapStreams_DisabledVerifiesOnly(t *testing.T) {
	fsm := &fakeStreamManager{}
	err := bootstrapStreams(context.Background(), fsm, "site1", false)
	require.NoError(t, err)
	assert.Empty(t, fsm.created, "must not create streams when disabled")
	assert.Equal(t, []string{"MIGRATION_OPLOG_site1"}, fsm.streamHit)
}

func TestBootstrapStreams_DisabledMissingStreamFails(t *testing.T) {
	fsm := &fakeStreamManager{streamErr: errors.New("stream not found")}
	err := bootstrapStreams(context.Background(), fsm, "site1", false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "verify MIGRATION_OPLOG stream")
}

func TestBootstrapStreams_EnabledCreatesSchemaOnly(t *testing.T) {
	fsm := &fakeStreamManager{}
	err := bootstrapStreams(context.Background(), fsm, "site1", true)
	require.NoError(t, err)

	require.Len(t, fsm.created, 1)
	got := fsm.created[0]
	assert.Equal(t, "MIGRATION_OPLOG_site1", got.Name)
	assert.Equal(t, []string{"chat.migration.oplog.site1.>"}, got.Subjects)
	// Federation config is ops/IaC-owned — never set here.
	assert.Nil(t, got.Sources)
	assert.Empty(t, fsm.streamHit, "enabled path does not verify")
}
