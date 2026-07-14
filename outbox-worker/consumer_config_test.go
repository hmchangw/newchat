package main

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/stream"
)

func TestBuildConcurrentConsumerConfig(t *testing.T) {
	cc := buildConcurrentConsumerConfig(stream.ConsumerSettings{
		AckWait:       30 * time.Second,
		MaxDeliver:    5,
		MaxWaiting:    512,
		MaxAckPending: 1000,
	}, "site-a", "site-b")

	// Per destination, not a single shared consumer: a down peer's parked events
	// fill only its own ack-pending budget instead of stalling healthy peers.
	assert.Equal(t, "outbox-worker-concurrent-site-b", cc.Durable)
	// The concurrent consumer enumerates the order-insensitive event types scoped
	// to this destination; membership event types ride the per-destination FIFO
	// lanes, and the two filter sets partition the stream.
	assert.Equal(t, []string{
		"chat.outbox.site-a.site-b.role_updated",
		"chat.outbox.site-a.site-b.subscription_read",
		"chat.outbox.site-a.site-b.thread_read",
		"chat.outbox.site-a.site-b.subscription_mute_toggled",
		"chat.outbox.site-a.site-b.subscription_favorite_toggled",
		"chat.outbox.site-a.site-b.room_restricted",
	}, cc.FilterSubjects)
	assert.Equal(t, jetstream.AckExplicitPolicy, cc.AckPolicy)
	assert.Equal(t, jetstream.DeliverAllPolicy, cc.DeliverPolicy)
	assert.Equal(t, 512, cc.MaxWaiting)
	// Default budget preserved: concurrency within a peer is unchanged; only the
	// isolation boundary moved from one shared consumer to one per peer.
	assert.Equal(t, 1000, cc.MaxAckPending)
	// Retry forever (delay, never drop): MaxDeliver=-1 overrides the input so a long
	// destination outage delays the relay indefinitely. The per-attempt backoff is
	// owned by jsretry (NakWithDelay in the consume loop), not consumer-level BackOff.
	assert.Equal(t, -1, cc.MaxDeliver)
}

func TestBuildOrderedConsumerConfig(t *testing.T) {
	cc := buildOrderedConsumerConfig(stream.ConsumerSettings{
		AckWait:       30 * time.Second,
		MaxDeliver:    5,
		MaxWaiting:    512,
		MaxAckPending: 1000,
	}, "site-a", "site-b")

	assert.Equal(t, "outbox-worker-ordered-site-b", cc.Durable)
	// Order-sensitive events (membership + room rename) share one per-destination
	// lane so they cannot overtake each other: a stale member_added after a
	// member_removed resurrects the member; a room_renamed before its member_added
	// strands a new member on the old name.
	assert.Equal(t, []string{
		"chat.outbox.site-a.site-b.member_added",
		"chat.outbox.site-a.site-b.member_removed",
		"chat.outbox.site-a.site-b.room_renamed",
	}, cc.FilterSubjects)
	assert.Equal(t, jetstream.AckExplicitPolicy, cc.AckPolicy)
	assert.Equal(t, jetstream.DeliverAllPolicy, cc.DeliverPolicy)
	// FIFO: the server releases message N+1 only after N is acked, preserving
	// enqueue order end-to-end through a destination outage and bounding retry
	// pressure on a down peer to one in-flight probe per backoff interval.
	assert.Equal(t, 1, cc.MaxAckPending)
	// Delay, never drop.
	assert.Equal(t, -1, cc.MaxDeliver)
}

func TestFederationPeers(t *testing.T) {
	tests := []struct {
		name   string
		siteID string
		all    []string
		want   []string
	}{
		{"skips self and blanks", "site-a", []string{"site-a", "", "site-b", "site-c"}, []string{"site-b", "site-c"}},
		{"dedupes preserving order", "site-a", []string{"site-c", "site-b", "site-c"}, []string{"site-c", "site-b"}},
		{"empty config", "site-a", nil, nil},
		{"only self", "site-a", []string{"site-a"}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, federationPeers(tt.siteID, tt.all))
		})
	}
}
