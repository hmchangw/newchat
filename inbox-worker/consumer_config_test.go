package main

import (
	"os"
	"testing"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
)

func TestConfig_BadgeCacheTTL(t *testing.T) {
	t.Run("defaults to 24h", func(t *testing.T) {
		require.NoError(t, os.Unsetenv("BADGE_CACHE_TTL"))
		cfg, err := env.ParseAs[config]()
		require.NoError(t, err)
		assert.Equal(t, 24*time.Hour, cfg.BadgeCacheTTL)
	})

	t.Run("honors BADGE_CACHE_TTL override", func(t *testing.T) {
		t.Setenv("BADGE_CACHE_TTL", "48h")
		cfg, err := env.ParseAs[config]()
		require.NoError(t, err)
		assert.Equal(t, 48*time.Hour, cfg.BadgeCacheTTL)
	})
}

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

func TestConfig_ValkeyDisabledByDefault(t *testing.T) {
	require.NoError(t, os.Unsetenv("VALKEY_ADDRS"))
	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	assert.Empty(t, cfg.ValkeyAddrs, "badge cache must be disabled (no Valkey required) unless VALKEY_ADDRS is set")
}

func TestConfig_ValkeyAddrsParsed(t *testing.T) {
	t.Setenv("VALKEY_ADDRS", "node-1:6379,node-2:6379")
	t.Setenv("VALKEY_PASSWORD", "hunter2")
	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	assert.Equal(t, []string{"node-1:6379", "node-2:6379"}, cfg.ValkeyAddrs)
	assert.Equal(t, "hunter2", cfg.ValkeyPassword)
}

func TestIsMembershipSubject(t *testing.T) {
	const siteID = "site-a"
	t.Run("member_added is membership", func(t *testing.T) {
		assert.True(t, isMembershipSubject(subject.InboxExternal(siteID, model.InboxMemberAdded), siteID))
	})
	t.Run("member_removed is membership", func(t *testing.T) {
		assert.True(t, isMembershipSubject(subject.InboxExternal(siteID, model.InboxMemberRemoved), siteID))
	})
	t.Run("read receipts are not membership", func(t *testing.T) {
		assert.False(t, isMembershipSubject(subject.InboxExternal(siteID, model.InboxSubscriptionRead), siteID))
		assert.False(t, isMembershipSubject(subject.InboxExternal(siteID, model.InboxThreadRead), siteID))
	})
	t.Run("another site's membership subject does not match", func(t *testing.T) {
		assert.False(t, isMembershipSubject(subject.InboxExternal("site-b", model.InboxMemberAdded), siteID))
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
		assert.Equal(t, []string{subject.InboxExternalAll(siteID)}, cc.FilterSubjects)
		assert.Equal(t, jetstream.AckExplicitPolicy, cc.AckPolicy)
		assert.Equal(t, 30*time.Second, cc.AckWait)
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
		assert.Equal(t, []string{subject.InboxExternalAll(siteID)}, cc.FilterSubjects)
		assert.Equal(t, 45*time.Second, cc.AckWait)
		assert.Equal(t, 256, cc.MaxWaiting)
	})

	// Retry forever (delay, never drop), mirroring outbox-worker's lanes: this is
	// federation's last hop, and a dropped event leaves this site permanently out
	// of sync with the origin — no reconciler repairs it. Poison events still
	// leave via errcode.Permanent (jsretry Ack-drops those). The per-attempt
	// backoff is owned by jsretry, not consumer-level BackOff.
	t.Run("overrides MaxDeliver to unlimited", func(t *testing.T) {
		for _, maxDeliver := range []int{5, 3, -1, 0} {
			cc := buildConsumerConfig(stream.ConsumerSettings{MaxDeliver: maxDeliver}, siteID)
			assert.Equal(t, -1, cc.MaxDeliver, "CONSUMER_MAX_DELIVER=%d must not cap federation retries", maxDeliver)
		}
	})
}
