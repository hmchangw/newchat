package natsutil_test

import (
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/stream"
)

func testBinder() *natsutil.FailoverBinder {
	return &natsutil.FailoverBinder{
		Service:    "message-worker",
		SiteID:     "site-a",
		MaxWorkers: 100,
		Buddy:      natsutil.BuddyConfig{SiteID: "site-b", NatsURL: "nats://b:4222"},
	}
}

// The metric attributes must name the FAILOVER stream and durable. Sharing the
// home lane's Consumer would attribute failover traffic to the live stream it
// did not come from, which is the whole reason each lane builds its own.
func TestFailoverBinder_MetricsIdentity(t *testing.T) {
	b := testBinder()
	spec := natsutil.LaneSpec{
		Stream:   stream.MessagesCanonicalFailover("site-a"),
		Consumer: jetstream.ConsumerConfig{Durable: "message-worker-failover", MaxDeliver: 5},
	}

	cc := b.MetricsIdentity(&spec)

	assert.Equal(t, "message-worker", cc.ServiceName)
	assert.Equal(t, "site-a", cc.Site)
	assert.Equal(t, "MESSAGES-CANONICAL-FAILOVER-site-a", cc.Stream)
	assert.Equal(t, "message-worker-failover", cc.Consumer)
}

// Ensure defaults to the consumed stream, so the common single-stream lane says
// it once rather than twice.
func TestLaneSpec_StreamsToEnsure_DefaultsToTheConsumedStream(t *testing.T) {
	spec := natsutil.LaneSpec{Stream: stream.MessagesCanonicalFailover("site-a")}

	got := spec.StreamsToEnsure()

	require.Len(t, got, 1)
	assert.Equal(t, "MESSAGES-CANONICAL-FAILOVER-site-a", got[0].Name)
}

// A lane that publishes to a second standby stream must be able to ready it
// too: both have to exist before the first failover message arrives, or the
// work would be consumed and then have nowhere to go.
func TestLaneSpec_StreamsToEnsure_HonoursExtraStreams(t *testing.T) {
	spec := natsutil.LaneSpec{
		Stream: stream.MessagesFailover("site-a"),
		AlsoEnsure: []stream.Config{
			stream.MessagesCanonicalFailover("site-a"),
		},
	}

	got := spec.StreamsToEnsure()

	require.Len(t, got, 2)
	assert.Equal(t, "MESSAGES-FAILOVER-site-a", got[0].Name)
	assert.Equal(t, "MESSAGES-CANONICAL-FAILOVER-site-a", got[1].Name)
}

// The consumed stream is always ensured first: it is the one the bind depends
// on, so its failure is the one worth reporting.
func TestLaneSpec_StreamsToEnsure_ConsumedStreamComesFirst(t *testing.T) {
	spec := natsutil.LaneSpec{
		Stream:     stream.PushNotificationFailover("site-a"),
		AlsoEnsure: []stream.Config{stream.MessagesCanonicalFailover("site-a")},
	}
	assert.Equal(t, stream.PushNotificationFailover("site-a").Name, spec.StreamsToEnsure()[0].Name)
}

// A binder with no Metrics is valid: not every service is instrumented, and one
// that is not must still get its lane rather than panic.
func TestFailoverBinder_MetricsIdentityWithoutMetrics(t *testing.T) {
	b := testBinder()
	b.Metrics = nil
	assert.NotPanics(t, func() {
		_ = b.MetricsIdentity(&natsutil.LaneSpec{Stream: stream.InboxFailover("site-a")})
	})
}

// Stream ownership is an axis of the spec, not an accident of which fields are
// filled in: a service that does not own its standby stream must not create it,
// and must not assert a placement its owner is responsible for.
func TestLaneSpec_BorrowedStreamsAreNotEnsured(t *testing.T) {
	spec := natsutil.LaneSpec{
		Stream:    stream.PushNotificationFailover("site-a"),
		Ownership: natsutil.BorrowsStreams,
	}
	assert.Empty(t, spec.StreamsToEnsure(),
		"binding the consumer is the existence check for a borrowed stream")
}

func TestLaneSpec_OwnedIsTheDefault(t *testing.T) {
	spec := natsutil.LaneSpec{Stream: stream.InboxFailover("site-a")}
	assert.Equal(t, natsutil.OwnsStreams, spec.Ownership)
	assert.Len(t, spec.StreamsToEnsure(), 1)
}
