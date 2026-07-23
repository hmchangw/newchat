package main

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"

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
		}, "notification-worker", subject.MsgCanonicalCreated("site-a"))

		assert.Equal(t, "notification-worker", cc.Durable)
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
			MaxAckPending: 500,
		}, "notification-worker", subject.MsgCanonicalCreated("site-a"))

		assert.Equal(t, "notification-worker", cc.Durable)
		assert.Equal(t, 500, cc.MaxAckPending)
		assert.Equal(t, 45*time.Second, cc.AckWait)
		assert.Equal(t, 3, cc.MaxDeliver)
		assert.Equal(t, 256, cc.MaxWaiting)
	})

	t.Run("filters to created subject only", func(t *testing.T) {
		cc := buildConsumerConfig(stream.ConsumerSettings{}, "notification-worker", subject.MsgCanonicalCreated("site-a"))

		// The worker only acts on created (push fan-out); reacted moved to
		// broadcast-worker. updated/deleted/pinned/unpinned are excluded at
		// the broker so they are never delivered, unmarshaled, or acked.
		assert.ElementsMatch(t, []string{
			subject.MsgCanonicalCreated("site-a"),
		}, cc.FilterSubjects)
	})

	t.Run("durable and filter subject are env-driven", func(t *testing.T) {
		cc := buildConsumerConfig(stream.ConsumerSettings{
			AckWait:       30 * time.Second,
			MaxDeliver:    5,
			MaxWaiting:    512,
			MaxAckPending: 1000,
		}, "bot-notification-worker", "chat.bot.canonical.site-a.created")

		assert.Equal(t, "bot-notification-worker", cc.Durable)
		assert.ElementsMatch(t, []string{"chat.bot.canonical.site-a.created"}, cc.FilterSubjects)
	})
}
