package main

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/jsretry"
	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
)

func TestBuildConsumerConfig(t *testing.T) {
	t.Run("propagates settings", func(t *testing.T) {
		cc := buildConsumerConfig(stream.ConsumerSettings{
			AckWait:       30 * time.Second,
			MaxDeliver:    5,
			MaxWaiting:    512,
			MaxAckPending: 1000,
		}, "default")

		assert.Equal(t, "room-worker", cc.Durable)
		assert.Equal(t, 1000, cc.MaxAckPending)
		assert.Equal(t, jetstream.AckExplicitPolicy, cc.AckPolicy)
		assert.Equal(t, 30*time.Second, cc.AckWait)
		assert.Equal(t, 5, cc.MaxDeliver)
		assert.Equal(t, 512, cc.MaxWaiting)
		assert.Equal(t, jetstream.DeliverAllPolicy, cc.DeliverPolicy)
	})

	t.Run("overrides flow through", func(t *testing.T) {
		cc := buildConsumerConfig(stream.ConsumerSettings{
			AckWait:       45 * time.Second,
			MaxDeliver:    3,
			MaxWaiting:    256,
			MaxAckPending: 200,
		}, "default")

		assert.Equal(t, "room-worker", cc.Durable)
		assert.Equal(t, 200, cc.MaxAckPending)
		assert.Equal(t, 45*time.Second, cc.AckWait)
		assert.Equal(t, 3, cc.MaxDeliver)
		assert.Equal(t, 256, cc.MaxWaiting)
	})

	t.Run("teams mode gets its own durable", func(t *testing.T) {
		cc := buildConsumerConfig(stream.ConsumerSettings{MaxAckPending: 100}, "teams")
		assert.Equal(t, "room-worker-teams", cc.Durable)
	})
}

// TestConsumedSubjectsClassifyToBoundedEventTypes ties the consumer metric's
// event_type back to the subjects Handler.Process actually dispatches on. The
// classifier normalizes anything it does not recognize to "unknown", so a new
// dispatch case added without a matching classifier case degrades the metric
// silently rather than failing anywhere.
func TestConsumedSubjectsClassifyToBoundedEventTypes(t *testing.T) {
	const site = "site-a"
	cases := []struct {
		name    string
		subject string
		want    natsmetrics.EventType
	}{
		{name: "member add", subject: subject.RoomCanonical(site, "member.add"), want: natsmetrics.EventMemberAdd},
		{name: "member remove", subject: subject.RoomCanonical(site, "member.remove"), want: natsmetrics.EventMemberRemove},
		{name: "room create", subject: subject.RoomCanonical(site, "create"), want: natsmetrics.EventRoomCreate},
		{name: "room rename", subject: subject.RoomCanonical(site, "room.rename"), want: natsmetrics.EventRoomRename},
		{name: "member muted", subject: subject.RoomCanonicalMemberEvent(site, "muted"), want: natsmetrics.EventMemberMuted},
		{name: "teams create", subject: subject.RoomTeamsCanonicalCreate(site), want: natsmetrics.EventRoomCreate},
		// Pre-cutover subject the dispatch switch still matches transitionally.
		{name: "legacy teams create", subject: subject.RoomCanonical(site, "teams.create"), want: natsmetrics.EventRoomCreate},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := natsmetrics.RoomEventTypeFromSubject(tc.subject)
			assert.Equal(t, tc.want, got)
			assert.NotEqual(t, natsmetrics.EventUnknown, got, "a dispatched subject must not fall back to unknown")
		})
	}
}

// The consumer loop derives its ack heartbeat from the consumer's own AckWait,
// so the two can never disagree. Two heartbeats must fit inside the budget:
// that is the headroom which lets a lost tick pass without the server deciding
// the message went un-acked and redelivering it into a second worker.
func TestHeartbeatIntervalLeavesHeadroomUnderAckWait(t *testing.T) {
	for _, ackWait := range []time.Duration{30 * time.Second, time.Minute, 5 * time.Minute} {
		cc := buildConsumerConfig(stream.ConsumerSettings{
			AckWait: ackWait, MaxDeliver: 5, MaxWaiting: 512, MaxAckPending: 1000,
		}, "default")

		every := jsretry.HeartbeatInterval(cc.AckWait)
		assert.Positive(t, every, "AckWait %s must produce a live heartbeat", ackWait)
		assert.Less(t, 2*every, cc.AckWait, "two heartbeats must fit inside AckWait %s", ackWait)
	}
}

// An unset AckWait means the server picks the deadline, so there is no local
// budget to pace against: disable the heartbeat rather than invent an interval.
func TestHeartbeatDisabledWhenAckWaitUnset(t *testing.T) {
	cc := buildConsumerConfig(stream.ConsumerSettings{MaxDeliver: 5}, "default")
	assert.Zero(t, jsretry.HeartbeatInterval(cc.AckWait))
}
