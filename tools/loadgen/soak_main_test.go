package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/stream"
)

type fakeSoakEncryptionStore struct {
	results []bool
	err     error
	calls   int
}

func (s *fakeSoakEncryptionStore) HasWrappedDEK(
	_ context.Context,
	_ string,
) (bool, error) {
	s.calls++
	if s.err != nil {
		return false, s.err
	}
	if len(s.results) == 0 {
		return false, nil
	}
	result := s.results[0]
	s.results = s.results[1:]
	return result, nil
}

func TestParseSoakArgs(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		wantSeed int64
		wantErr  bool
	}{
		{name: "default seed", wantSeed: 42},
		{name: "explicit seed", args: []string{"--seed=99"}, wantSeed: 99},
		{name: "positional argument", args: []string{"unexpected"}, wantErr: true},
		{name: "unknown flag", args: []string{"--direct-cql"}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := parseSoakArgs(tt.args)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantSeed, opts.Seed)
		})
	}
}

func TestWarmSoakPinnedCatalog_UsesPinnedListPath(t *testing.T) {
	data := soakPinnedReply(t, nil, "", false)
	transport := &soakReadTransport{
		replies: []soakRPCFakeReply{{data: data}},
	}
	reader := newTestSoakReader(
		transport,
		nil,
		emptySoakReadCatalog(),
	)

	require.NoError(t, warmSoakPinnedCatalog(
		context.Background(),
		reader,
		[]string{"room-1"},
		2,
	))
	require.Len(t, transport.snapshot(), 1)
}

func TestSoakMeasuredReadConfig_OneScheduledReadEqualsOneRPC(t *testing.T) {
	cfg := soakMeasuredReadConfig("site-1", soakDefaultPageLimit)

	assert.Equal(t, "site-1", cfg.SiteID)
	assert.Equal(t, soakDefaultPageLimit, cfg.PageLimit)
	assert.Equal(t, 1, cfg.MaxPages)
	assert.Equal(t, soakRequestTimeout, cfg.RequestTimeout)
}

func TestSoakConsumerSamplerTargets_CoversEveryConsumerTheLanesFeed(t *testing.T) {
	assert.Equal(t, []soakConsumerSamplerTarget{
		{Stream: stream.Messages("site-test").Name, Durable: "message-gatekeeper"},
		{Stream: stream.MessagesCanonical("site-test").Name, Durable: "message-worker"},
		{Stream: stream.MessagesCanonical("site-test").Name, Durable: "broadcast-worker"},
		{Stream: stream.MessagesCanonical("site-test").Name, Durable: "notification-worker"},
		{Stream: stream.MessagesCanonical("site-test").Name, Durable: "message-sync"},
		{Stream: stream.Rooms("site-test").Name, Durable: "room-worker"},
		{
			Stream:  stream.Rooms("site-test").Name,
			Durable: "notification-worker-room-event-invalidate",
		},
		{Stream: stream.Inbox("site-test").Name, Durable: "spotlight-sync"},
		{Stream: stream.Inbox("site-test").Name, Durable: "user-room-sync"},
	}, soakConsumerSamplerTargets("site-test"))
}

// The room and member lanes publish into ROOMS and INBOX. A consumer with real
// traffic and no sampler is a blind spot exactly where a fault window needs
// backlog evidence, so every stream the lanes feed must appear here.
func TestSoakConsumerSamplerTargets_SampleEveryStreamTheLanesPublishTo(t *testing.T) {
	sampled := map[string]bool{}
	for _, target := range soakConsumerSamplerTargets("site-test") {
		sampled[target.Stream] = true
	}

	for _, streamName := range []string{
		stream.Messages("site-test").Name,
		stream.MessagesCanonical("site-test").Name,
		stream.Rooms("site-test").Name,
		stream.Inbox("site-test").Name,
	} {
		assert.True(t, sampled[streamName], "stream=%s", streamName)
	}
}

func TestNewSoakRuntimeSelector_UsesOnlyPersistedActiveUsers(t *testing.T) {
	cfg := validSoakConfig(t)
	topology := soakTopology{
		ActiveUsers: []model.User{{ID: "active-id", Account: "active"}},
		Rooms:       []model.Room{{ID: "room-1", Type: model.RoomTypeChannel}},
		Subscriptions: []model.Subscription{
			{
				RoomID: "room-1", IsSubscribed: true,
				User: model.SubscriptionUser{ID: "active-id", Account: "active"},
			},
			{
				RoomID: "room-1", IsSubscribed: true,
				User: model.SubscriptionUser{ID: "inactive-id", Account: "inactive"},
			},
		},
	}

	selector, err := newSoakRuntimeSelector(&topology, &cfg, 42)
	require.NoError(t, err)

	for range 100 {
		target, _ := selector.nextSend()
		assert.Equal(t, "active-id", target.UserID)
		assert.Equal(t, "active", target.Account)
		assert.Equal(t, recipientSetSourceTopology, target.RecipientSetSource)
		assert.True(t, target.RecipientSetComplete)
		assert.Equal(t, recipientExpectedRouteRoom, target.RecipientRoute)
	}
}

func TestWaitForSoakWrappedDEK(t *testing.T) {
	t.Run("appears after message worker persists", func(t *testing.T) {
		store := &fakeSoakEncryptionStore{results: []bool{false, true}}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		err := waitForSoakWrappedDEK(ctx, store, "room-a", time.Millisecond)

		require.NoError(t, err)
		assert.Equal(t, 2, store.calls)
	})

	t.Run("missing evidence fails", func(t *testing.T) {
		store := &fakeSoakEncryptionStore{}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
		defer cancel()

		err := waitForSoakWrappedDEK(ctx, store, "room-a", time.Millisecond)

		require.Error(t, err)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Contains(t, err.Error(), "wrapped DEK")
	})

	t.Run("store failure is wrapped", func(t *testing.T) {
		wantErr := errors.New("mongo unavailable")
		store := &fakeSoakEncryptionStore{err: wantErr}

		err := waitForSoakWrappedDEK(
			context.Background(),
			store,
			"room-a",
			time.Millisecond,
		)

		require.Error(t, err)
		assert.ErrorIs(t, err, wantErr)
	})
}

func TestSoakCollectorRecorders_MapComponentSamples(t *testing.T) {
	now := time.Now().UTC()
	collector := NewSoakCollector(NewMetrics(), now, 0, time.Minute)
	recorders := newSoakCollectorRecorders(collector, func() time.Time {
		return now.Add(time.Second)
	})

	recorders.read.Record(&soakReadSample{
		Action: soakRPCLoadHistory, Latency: 5 * time.Millisecond, Retries: 1,
	})
	recorders.mutation.Record(soakMutationSample{
		Action: soakRPCDelete, Skipped: true, TargetMissing: true,
		ErrorClass: soakErrorMutationTargetMissing, Retries: 3,
	})
	recorders.verify.Record(&soakVerifyResult{
		Class: soakVerifyMismatch, Action: soakRPCGetMessage,
		Latency: 7 * time.Millisecond,
	})

	snapshot := collector.Snapshot(now.Add(2 * time.Second))
	assert.Equal(t, uint64(1), snapshot.Actions[soakRPCLoadHistory].Succeeded)
	assert.Equal(t, uint64(1), snapshot.Actions[soakRPCDelete].Skipped)
	assert.Equal(t, uint64(1), snapshot.MutationTargetMissing)
	assert.Equal(
		t,
		uint64(1),
		snapshot.Verifications[soakRPCGetMessage][soakVerifyMismatch],
	)
	assert.Equal(t, uint64(1), snapshot.Actions[soakRPCGetMessage].Failed)
}

// The page size was hardcoded at 50 in three places. A page of 50 messages at
// history-service's 20 KB content cap is ~1 MB, well past this deployment's
// 256 KB max_payload, so a soak run would take oversize replies instead of
// measuring reads. It is a flag now so an operator can match the broker.
func TestParseSoakArgs_PageLimitDefaultsBelowTheBrokerCap(t *testing.T) {
	opts, err := parseSoakArgs(nil)
	require.NoError(t, err)
	assert.Equal(t, soakDefaultPageLimit, opts.PageLimit)
	assert.LessOrEqual(t, opts.PageLimit, 15,
		"the default must leave headroom under a 256 KB max_payload")
}

func TestParseSoakArgs_PageLimitOverride(t *testing.T) {
	opts, err := parseSoakArgs([]string{"-page-limit", "8"})
	require.NoError(t, err)
	assert.Equal(t, 8, opts.PageLimit)
}

func TestParseSoakArgs_RejectsNonPositivePageLimit(t *testing.T) {
	_, err := parseSoakArgs([]string{"-page-limit", "0"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "page-limit")
}

func TestSoakMeasuredReadConfig_UsesTheConfiguredPageLimit(t *testing.T) {
	cfg := soakMeasuredReadConfig("site-test", 12)
	assert.Equal(t, 12, cfg.PageLimit)
}

// Page size is a broker-payload constraint; walk depth is a coverage
// requirement. Tying them together meant lowering the page to fit max_payload
// silently cut how much history the verifier could reach.
func TestSoakMaxPages_HoldsTheRowBudgetAcrossPageSizes(t *testing.T) {
	tests := []struct {
		pageLimit int
		wantPages int
	}{
		{soakDefaultPageLimit, 334}, // rounds up past the budget
		{50, 100},                   // the historical pairing, unchanged
		{1, 5000},
		{5000, 1},
		{10000, 1}, // a page larger than the budget still walks once
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("page-limit=%d", tt.pageLimit), func(t *testing.T) {
			pages := soakMaxPages(tt.pageLimit)
			assert.Equal(t, tt.wantPages, pages)
			assert.GreaterOrEqual(t, pages*tt.pageLimit, soakWalkRowBudget,
				"the walk must still reach the row budget")
		})
	}
}

// Defensive: a non-positive page limit is rejected at flag parse, but the
// helper must not divide by zero or return a walk that cannot advance.
func TestSoakMaxPages_NonPositivePageLimit(t *testing.T) {
	assert.Equal(t, 1, soakMaxPages(0))
	assert.Equal(t, 1, soakMaxPages(-5))
}
