//go:build integration

package main

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestFailureObservation_AcceptedRecipientHistoryRestart(t *testing.T) {
	natsURL := testutil.NATS(t)
	publisher, err := nats.Connect(natsURL)
	require.NoError(t, err)
	t.Cleanup(publisher.Close)

	now := time.Now().UTC()
	evidenceDirectory := t.TempDir()
	walPath := filepath.Join(evidenceDirectory, "run.wal")
	wal, err := openFailureWAL(walPath)
	require.NoError(t, err)
	contract := newFailureObserverContract(true, false)
	ledger, err := newFailureLedger(&failureLedgerConfig{
		Capacity: 4, Journal: wal, Now: func() time.Time { return now },
		ObserverContract: &contract,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ledger.Close() })
	metrics := NewMetrics()
	t.Cleanup(metrics.stopNATSHealth)
	recipientObserver := newFailureRecipientObserver(
		ledger, metrics, 8, func() time.Time { return now },
		withFailureRecipientEvidenceDir(evidenceDirectory),
	)
	topology := &soakTopology{
		Rooms: []model.Room{{ID: "room-1", Type: model.RoomTypeChannel}},
		Subscriptions: []model.Subscription{
			{RoomID: "room-1", RoomType: model.RoomTypeChannel, IsSubscribed: true, User: model.SubscriptionUser{Account: "alice"}},
			{RoomID: "room-1", RoomType: model.RoomTypeChannel, IsSubscribed: true, User: model.SubscriptionUser{Account: "bob"}},
		},
	}
	recipientSource := newPooledNATSFailureRecipientSource(2, func(int) (failureRecipientConnection, error) {
		connection, connectErr := nats.Connect(natsURL)
		if connectErr != nil {
			return nil, connectErr
		}
		return newNATSFailureRecipientConnection(connection), nil
	})
	t.Cleanup(func() { _ = recipientSource.Close() })
	subscriptions, err := startFailureRecipientSubscriptions(
		recipientSource,
		topology,
		recipientObserver,
	)
	require.NoError(t, err)
	observerCtx, stopObserver := context.WithCancel(context.Background())
	recipientObserver.Run(observerCtx)

	tracker := newSoakFailureTracker(
		ledger, 0, 20*time.Millisecond, func() time.Time { return now },
		withSoakFailureRunID("run-1"),
		withSoakFailureRecipientObserver(recipientObserver),
	)
	pending := &soakPendingSend{
		MessageID: "message-1", RequestID: "request-1", PublishedAt: now,
		Target: soakSendTarget{
			Account: "alice", RoomID: "room-1", RoomType: model.RoomTypeChannel,
			Recipients: []string{"alice", "bob"},
		},
		Content: "content hash only in WAL",
	}
	require.NoError(t, tracker.Start(pending))
	require.NoError(t, tracker.Activate(pending))
	require.NoError(t, tracker.ObserveReply(&soakSendReplyResult{
		Status: soakSendReplyAccepted, MessageID: "message-1",
	}))
	payload, err := json.Marshal(model.RoomEvent{
		Type: model.RoomEventNewMessage, RoomID: "room-1", LastMsgID: "message-1",
	})
	require.NoError(t, err)
	require.NoError(t, publisher.Publish(subject.RoomEvent("room-1", true), payload))
	require.NoError(t, publisher.Flush())
	completed := assert.Eventually(t, func() bool {
		return recipientObserver.evidence.Complete("message-1")
	}, 5*time.Second, 10*time.Millisecond)
	if !completed {
		recipientObserver.evidence.mu.Lock()
		expectation := recipientObserver.evidence.operations["message-1"]
		t.Logf("recipient expectation after timeout: %#v", expectation)
		recipientObserver.evidence.mu.Unlock()
		require.True(t, completed)
	}

	require.NoError(t, shutdownFailureRecipientObserver(subscriptions, stopObserver, recipientObserver))
	now = now.Add(time.Minute)
	require.NoError(t, ledger.Close())

	reopenedWAL, err := openFailureWAL(walPath)
	require.NoError(t, err)
	recovered, err := newFailureLedger(&failureLedgerConfig{
		Capacity: 4, Journal: reopenedWAL, Now: func() time.Time { return now },
		ObserverContract: &contract,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, recovered.Close()) })
	assert.Equal(t, 1, recovered.Snapshot().Recovered)
	recoveredRecipient := newFailureRecipientObserver(
		recovered, metrics, 8, func() time.Time { return now },
		withFailureRecipientEvidenceDir(evidenceDirectory),
	)
	require.NoError(t, recoveredRecipient.Recover(recovered.ActiveOperations()))
	reconciler := newSoakFailureReconciler(
		recovered,
		&fakeSoakFailureVerifier{results: []soakFailureHistoryResult{soakFailureHistoryFound}},
		time.Second,
		func() time.Time { return now },
		withSoakFailureRecipientFinalizer(recoveredRecipient),
	)
	ran, err := reconciler.Try(context.Background())
	require.NoError(t, err)
	assert.True(t, ran)
	ran, err = reconciler.Try(context.Background())
	require.NoError(t, err)
	assert.True(t, ran)
	assert.Equal(t, uint64(1), recovered.Snapshot().Results[failureResultUnverified])

}
