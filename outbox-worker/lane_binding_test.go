package main

import (
	"context"
	"errors"
	"testing"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	o11ynats "github.com/flywindy/o11y/nats"
)

// stubJS stands in for one connection's JetStream, so a test can tell which of
// two connections a lane actually forwarded through. Only PublishMsg is
// exercised; the rest satisfy the interface.
type stubJS struct {
	published []string
	err       error
}

func (s *stubJS) Publish(_ context.Context, subj string, _ []byte, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	s.published = append(s.published, subj)
	return &jetstream.PubAck{}, s.err
}

func (s *stubJS) PublishMsg(_ context.Context, msg *natsgo.Msg, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	s.published = append(s.published, msg.Subject)
	if s.err != nil {
		return nil, s.err
	}
	return &jetstream.PubAck{}, nil
}

func (s *stubJS) CreateOrUpdateStream(context.Context, jetstream.StreamConfig) (o11ynats.Stream, error) {
	return nil, errors.New("not implemented")
}
func (s *stubJS) Stream(context.Context, string) (o11ynats.Stream, error) {
	return nil, errors.New("not implemented")
}
func (s *stubJS) DeleteStream(context.Context, string) error { return errors.New("not implemented") }
func (s *stubJS) CreateOrUpdateConsumer(context.Context, string, jetstream.ConsumerConfig) (o11ynats.Consumer, error) {
	return nil, errors.New("not implemented")
}
func (s *stubJS) Consumer(context.Context, string, string) (o11ynats.Consumer, error) {
	return nil, errors.New("not implemented")
}
func (s *stubJS) DeleteConsumer(context.Context, string, string) error {
	return errors.New("not implemented")
}

// The failover lane runs because this site's own NATS is unreachable. A lane
// that consumed the buddy-hosted OUTBOX-FAILOVER but forwarded through the home
// JetStream would never deliver — and, worse, would never Ack, so the buffered
// federation event would redeliver forever while appearing to be in flight.
func TestNewLanePublisher_ForwardsOnItsOwnConnection(t *testing.T) {
	home, buddy := &stubJS{}, &stubJS{}
	ctx := context.Background()

	require.NoError(t, newLanePublisher(home)(ctx, "chat.inbox.site-b.external.member_added", []byte(`{}`), "id-home"))
	require.NoError(t, newLanePublisher(buddy)(ctx, "chat.inbox.site-c.external.member_added", []byte(`{}`), "id-buddy"))

	assert.Equal(t, []string{"chat.inbox.site-b.external.member_added"}, home.published)
	assert.Equal(t, []string{"chat.inbox.site-c.external.member_added"}, buddy.published)
}

// A failed forward must surface as an error so jsretry Naks it for redelivery,
// with the destination subject named for triage.
func TestNewLanePublisher_WrapsPublishError(t *testing.T) {
	js := &stubJS{err: errors.New("no responders")}

	err := newLanePublisher(js)(context.Background(), "chat.inbox.site-b.external.member_added", []byte(`{}`), "id-1")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "chat.inbox.site-b.external.member_added")
	assert.Contains(t, err.Error(), "no responders")
}
