package main

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/jsretry"
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
	assert.Empty(t, cfg.Valkey.Addrs, "badge cache must be disabled (no Valkey required) unless VALKEY_ADDRS is set")
}

func TestConfig_ValkeyAddrsParsed(t *testing.T) {
	t.Setenv("VALKEY_ADDRS", "node-1:6379,node-2:6379")
	t.Setenv("VALKEY_PASSWORD", "hunter2")
	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	assert.Equal(t, []string{"node-1:6379", "node-2:6379"}, cfg.Valkey.Addrs)
	assert.Equal(t, "hunter2", cfg.Valkey.Password)
}

func TestConfig_PoolValidate_RejectsZeroMaxPoolSize(t *testing.T) {
	t.Setenv("MONGO_MAX_POOL_SIZE", "0")
	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	assert.Error(t, cfg.Pool.Validate())
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
			AckWait: 30 * time.Second,
			// The symbol, not a literal: a literal stops matching when the
			// package default moves, and the outage budget then silently
			// stops being applied under test.
			MaxDeliver:    stream.DefaultMaxDeliver,
			MaxWaiting:    512,
			MaxAckPending: 1000,
		}, siteID)

		assert.Equal(t, "inbox-worker", cc.Durable)
		assert.Equal(t, 1000, cc.MaxAckPending)
		assert.Equal(t, []string{subject.InboxExternalAll(siteID)}, cc.FilterSubjects)
		assert.Equal(t, jetstream.AckExplicitPolicy, cc.AckPolicy)
		assert.Equal(t, 30*time.Second, cc.AckWait)
		assert.Equal(t, jsretry.DeliveriesFor(jsretry.DefaultBackoff, stream.OutageRetryWindow), cc.MaxDeliver,
			"a federated event waits on a recovering peer's backlog, so the lane takes the outage budget")
		assert.Equal(t, 512, cc.MaxWaiting)
		assert.Equal(t, jetstream.DeliverAllPolicy, cc.DeliverPolicy)
	})

	t.Run("the outage budget outlasts a peer outage", func(t *testing.T) {
		cc := buildConsumerConfig(stream.ConsumerSettings{
			AckWait: 30 * time.Second, MaxDeliver: stream.DefaultMaxDeliver,
			MaxWaiting: 512, MaxAckPending: 1000,
		}, siteID)

		assert.GreaterOrEqual(t, jsretry.MinWindow(jsretry.DefaultBackoff, cc.MaxDeliver), stream.OutageRetryWindow,
			"the guaranteed window, not the nominal one, is what rides out the outage")
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
		assert.Equal(t, 3, cc.MaxDeliver)
		assert.Equal(t, 256, cc.MaxWaiting)
	})
}

// fakeFederatedMsg is a federatedMsg double recording how settleFederated
// disposed of the delivery.
type fakeFederatedMsg struct {
	subject      string
	numDelivered uint64
	metaErr      error
	termErr      error
	acked        bool
	naked        bool
	termed       bool
}

func (m *fakeFederatedMsg) Subject() string { return m.subject }
func (m *fakeFederatedMsg) Metadata() (*jetstream.MsgMetadata, error) {
	if m.metaErr != nil {
		return nil, m.metaErr
	}
	return &jetstream.MsgMetadata{NumDelivered: m.numDelivered}, nil
}
func (m *fakeFederatedMsg) Ack() error                       { m.acked = true; return nil }
func (m *fakeFederatedMsg) NakWithDelay(time.Duration) error { m.naked = true; return nil }
func (m *fakeFederatedMsg) Term() error                      { m.termed = true; return m.termErr }

func TestSettleFederated(t *testing.T) {
	const deliverCap = 6
	subj := subject.InboxExternal("site-a", model.InboxRoleUpdated)

	tests := []struct {
		name          string
		numDelivered  uint64
		metaErr       error
		termErr       error
		err           error
		wantExhausted bool
		wantAck       bool
	}{
		{
			name:         "success acks",
			numDelivered: 1,
			wantAck:      true,
		},
		{
			name:         "transient failure below the cap naks",
			numDelivered: 5,
			err:          errors.New("subscription not found"),
		},
		{
			name:          "the last allowed delivery is termed, not nak-dropped",
			numDelivered:  deliverCap,
			err:           errors.New("subscription not found"),
			wantExhausted: true,
		},
		{
			name:          "past the cap still terms (a redelivery raced the cap)",
			numDelivered:  deliverCap + 1,
			err:           errors.New("subscription not found"),
			wantExhausted: true,
		},
		{
			name:         "a permanent failure is a deliberate ack-drop, not an exhaustion",
			numDelivered: deliverCap,
			err:          errcode.Permanent(errcode.BadRequest("empty roles")),
			wantAck:      true,
		},
		{
			name:         "unreadable metadata prefers a nak over a premature term",
			numDelivered: deliverCap,
			metaErr:      errors.New("not a jetstream message"),
			err:          errors.New("subscription not found"),
		},
		{
			// The message is out of this worker's hands either way; the Term failure
			// is logged rather than swallowed.
			name:          "a failed term still counts as a give-up",
			numDelivered:  deliverCap,
			termErr:       errors.New("nats: connection closed"),
			err:           errors.New("subscription not found"),
			wantExhausted: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &fakeFederatedMsg{subject: subj, numDelivered: tt.numDelivered, metaErr: tt.metaErr, termErr: tt.termErr}
			assert.Equal(t, tt.wantExhausted, settleFederated(context.Background(), msg, deliverCap, tt.err))
			assert.Equal(t, tt.wantAck, msg.acked)
			assert.Equal(t, tt.wantExhausted, msg.termed)
			assert.Equal(t, !tt.wantAck && !tt.wantExhausted, msg.naked)
		})
	}
}

// An unlimited budget never exhausts, so no delivery count can trip a term.
func TestSettleFederated_UnlimitedBudgetNeverTerms(t *testing.T) {
	msg := &fakeFederatedMsg{subject: subject.InboxExternal("site-a", model.InboxMemberAdded), numDelivered: 10_000}
	assert.False(t, settleFederated(context.Background(), msg, -1, errors.New("unknown user")))
	assert.True(t, msg.naked)
	assert.False(t, msg.termed)
}
