package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
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
			seed, err := parseSoakArgs(tt.args)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantSeed, seed)
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
	cfg := soakMeasuredReadConfig("site-1")

	assert.Equal(t, "site-1", cfg.SiteID)
	assert.Equal(t, 50, cfg.PageLimit)
	assert.Equal(t, 1, cfg.MaxPages)
	assert.Equal(t, soakRequestTimeout, cfg.RequestTimeout)
}

func TestNewSoakRuntimeSelector_UsesOnlyPersistedActiveUsers(t *testing.T) {
	cfg := validSoakConfig(t)
	topology := soakTopology{
		ActiveUsers: []model.User{{ID: "active-id", Account: "active"}},
		Rooms:       []model.Room{{ID: "room-1"}},
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

	recorders.read.Record(soakReadSample{
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
