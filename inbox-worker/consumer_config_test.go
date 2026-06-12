package main

import (
	"testing"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
)

func TestConfig_MaxWorkers(t *testing.T) {
	t.Run("defaults to 100", func(t *testing.T) {
		cfg, err := env.ParseAs[config]()
		require.NoError(t, err)
		assert.Equal(t, 100, cfg.MaxWorkers)
	})

	t.Run("honors MAX_WORKERS override", func(t *testing.T) {
		t.Setenv("MAX_WORKERS", "32")
		cfg, err := env.ParseAs[config]()
		require.NoError(t, err)
		assert.Equal(t, 32, cfg.MaxWorkers)
	})
}

func TestIsMembershipSubject(t *testing.T) {
	const siteID = "site-a"
	t.Run("member_added is membership", func(t *testing.T) {
		assert.True(t, isMembershipSubject(subject.InboxMemberAddedAggregate(siteID), siteID))
	})
	t.Run("member_removed is membership", func(t *testing.T) {
		assert.True(t, isMembershipSubject(subject.InboxMemberRemovedAggregate(siteID), siteID))
	})
	t.Run("read receipts are not membership", func(t *testing.T) {
		assert.False(t, isMembershipSubject("chat.inbox.site-a.aggregate.subscription_read", siteID))
		assert.False(t, isMembershipSubject("chat.inbox.site-a.aggregate.thread_read", siteID))
	})
	t.Run("another site's membership subject does not match", func(t *testing.T) {
		assert.False(t, isMembershipSubject(subject.InboxMemberAddedAggregate("site-b"), siteID))
	})
}

func TestBuildConsumerConfig(t *testing.T) {
	siteID := "site-a"

	t.Run("propagates settings", func(t *testing.T) {
		cc := buildConsumerConfig(stream.ConsumerSettings{
			AckWait:       30 * time.Second,
			MaxDeliver:    5,
			MaxWaiting:    512,
			MaxAckPending: 1000,
		}, siteID)

		assert.Equal(t, "inbox-worker", cc.Durable)
		assert.Equal(t, 1000, cc.MaxAckPending)
		assert.Equal(t, []string{subject.InboxAggregateAll(siteID)}, cc.FilterSubjects)
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
			MaxAckPending: 100,
		}, siteID)

		assert.Equal(t, "inbox-worker", cc.Durable)
		assert.Equal(t, 100, cc.MaxAckPending)
		assert.Equal(t, []string{subject.InboxAggregateAll(siteID)}, cc.FilterSubjects)
		assert.Equal(t, 45*time.Second, cc.AckWait)
		assert.Equal(t, 3, cc.MaxDeliver)
		assert.Equal(t, 256, cc.MaxWaiting)
	})
}
