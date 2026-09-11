package main

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/stream"
)

// The failover lane consumes the buddy-hosted MESSAGES-CANONICAL-FAILOVER stream
// on its own durable, so its cursor is independent of the home lane's, and keeps
// the home lane's unlimited redelivery: a MongoDB outage during a NATS outage
// must still park messages rather than drop room state.
func TestFailoverLaneSpec(t *testing.T) {
	settings := stream.ConsumerSettings{
		AckWait: 30 * time.Second, MaxDeliver: 5, MaxWaiting: 512, MaxAckPending: 1000,
	}
	for _, tc := range []struct {
		name    string
		mode    stream.Pipeline
		durable string
	}{
		{"user pipeline", stream.PipelineUser, "roomlist-worker-failover"},
		{"bot pipeline", stream.PipelineBot, "bot-roomlist-worker-failover"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wiring := stream.Resolve(tc.mode, "site-a")
			spec := failoverLaneSpec(settings, tc.mode, &wiring)

			assert.Equal(t, wiring.CanonicalFailoverStream, spec.Stream)
			assert.Equal(t, tc.durable, spec.Consumer.Durable)
			assert.Equal(t, wiring.CanonicalFailoverWildcard, spec.Consumer.FilterSubject)
			assert.Equal(t, -1, spec.Consumer.MaxDeliver, "the failover lane must park, not drop, during a MongoDB outage")
			assert.Empty(t, spec.AlsoEnsure, "roomlist-worker publishes nothing")
		})
	}
}

// Both lanes run the same per-message body against the same flusher: the room
// state a failover-lane message derives is this site's, written to this site's
// Mongo, exactly as on the home lane. Only where the message came from differs.
func TestCanonicalHandler_SharesTheFlusherWithTheHomeLane(t *testing.T) {
	store := &stubStore{}
	f := newFlusher(store, 0, 0)
	handle := canonicalHandler(f)

	good := &fakeJetstreamMsg{subject: "chat.failover.msg.canonical.site-a.created", data: wellFormedEventBytes(t), headers: nats.Header{}}
	handle(context.Background(), good)

	assert.False(t, good.acked, "held until the batch is flushed, like the home lane")
	require.False(t, f.pending.empty(), "the failover-lane message's intents must reach the shared pending batch")
	assert.Equal(t, "m1", f.pending.rooms["r1"].msgID)

	f.Flush(context.Background())
	assert.True(t, good.acked, "settled by the shared flush")
}

// A malformed payload settles immediately on the failover lane too; with
// unlimited redelivery an un-acked one would otherwise redeliver forever.
func TestCanonicalHandler_MalformedPayloadSettledImmediately(t *testing.T) {
	f := newFlusher(&stubStore{}, 0, 0)
	bad := &fakeJetstreamMsg{subject: "chat.failover.msg.canonical.site-a.created", data: []byte("not json"), headers: nats.Header{}}

	canonicalHandler(f)(context.Background(), bad)

	assert.True(t, bad.acked)
	assert.False(t, bad.naked)
	assert.True(t, f.pending.empty())
}

// consumeLoop must keep driving the same body, so the two lanes cannot drift.
func TestConsumeLoop_DrivesTheSharedHandler(t *testing.T) {
	f := newFlusher(&stubStore{}, 0, 0)
	good := &fakeJetstreamMsg{subject: "chat.msg.canonical.site-a.created", data: wellFormedEventBytes(t), headers: nats.Header{}}

	var wg sync.WaitGroup
	wg.Add(1)
	consumeLoop(&fakeIterator{msgs: []jetstream.Msg{good}}, f, &wg, &consumeState{})

	require.False(t, f.pending.empty())
	assert.Equal(t, "m1", f.pending.rooms["r1"].msgID)
}
