package main

import (
	"context"
	"errors"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeJS records what the outbox publisher sends and lets a test fail the
// publish the way an unavailable OUTBOX stream would.
type fakeJS struct {
	calls   int
	lastMsg *nats.Msg
	lastOps int
	err     error
}

func (f *fakeJS) PublishMsg(_ context.Context, m *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	f.calls++
	f.lastMsg = m
	f.lastOps = len(opts)
	if f.err != nil {
		return nil, f.err
	}
	return &jetstream.PubAck{Stream: "OUTBOX-site-a"}, nil
}

// The OUTBOX exists so a cross-site event survives a failed forward. That rests
// entirely on the publish being durable: a core-NATS publish returns nil as soon
// as the bytes reach the local socket buffer, so an unavailable stream would be
// reported as success and the event lost with nothing left to retry. The two
// sibling producers (room-worker, room-service) both block on a PubAck; this one
// must too.
func TestOutboxPublisher_SurfacesAFailedDurableWrite(t *testing.T) {
	js := &fakeJS{err: errors.New("no responders available for JetStream")}
	pub := newOutboxPublisher(js)

	err := pub(context.Background(), "chat.outbox.site-a.site-b.member_removed", []byte(`{}`), "dedup-1")

	require.Error(t, err, "a publish the stream never acknowledged must not be reported as success")
	assert.Equal(t, 1, js.calls)
}

func TestOutboxPublisher_PublishesThroughJetStreamWithDedupID(t *testing.T) {
	js := &fakeJS{}
	pub := newOutboxPublisher(js)

	const subj = "chat.outbox.site-a.site-b.member_removed"
	require.NoError(t, pub(context.Background(), subj, []byte(`{"roomId":"r1"}`), "dedup-1"))

	require.Equal(t, 1, js.calls)
	require.NotNil(t, js.lastMsg)
	assert.Equal(t, subj, js.lastMsg.Subject)
	assert.Equal(t, []byte(`{"roomId":"r1"}`), js.lastMsg.Data)
	// The dedup id rides as a publish option (jetstream.WithMsgID), matching
	// room-worker, so the server applies Nats-Msg-Id de-duplication to a
	// redelivered removal instead of appending a second OUTBOX entry.
	assert.Positive(t, js.lastOps, "the dedup id must reach the server as a publish option")
}
