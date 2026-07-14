package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
)

func TestPublishInsert(t *testing.T) {
	var got *nats.Msg
	pub := func(_ context.Context, m *nats.Msg, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
		got = m
		return &jetstream.PubAck{Sequence: 1}, nil
	}
	p := &canonicalPublisher{siteID: "site1", publish: pub, now: func() int64 { return 123 }}
	msg := model.Message{ID: "m1", RoomID: "r1", Content: "hi", CreatedAt: time.Unix(0, 0)}

	require.NoError(t, p.publishInsert(context.Background(), msg))
	require.NotNil(t, got)
	assert.Equal(t, subject.MsgCanonicalCreated("site1"), got.Subject)
	assert.True(t, natsutil.IsMigrationLive(got))

	var evt model.MessageEvent
	require.NoError(t, json.Unmarshal(got.Data, &evt))
	assert.Equal(t, model.EventCreated, evt.Event)
	assert.Equal(t, "m1", evt.Message.ID)
	assert.Equal(t, int64(123), evt.Timestamp)
}

func TestPublishInsert_PublishError(t *testing.T) {
	wantErr := errors.New("jetstream down")
	pub := func(_ context.Context, _ *nats.Msg, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
		return nil, wantErr
	}
	p := &canonicalPublisher{siteID: "site1", publish: pub, now: func() int64 { return 123 }}

	err := p.publishInsert(context.Background(), model.Message{ID: "m1"})
	require.Error(t, err)
	// The pub-ack failure must propagate (the insert is the only durability handoff — a
	// swallowed error here loses the message).
	assert.ErrorIs(t, err, wantErr)
}
