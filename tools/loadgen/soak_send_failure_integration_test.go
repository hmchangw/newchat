package main

import (
	"context"
	"errors"
	"math/rand" // #nosec G404 -- deterministic load-generator test input // nosemgrep: math-random-used
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	failureSendTestMessageID = "0123456789ABCDEFGHIJ"
	failureSendTestRequestID = "01970a4f-8c2d-7c9a-abcd-e0123456789f"
)

type failureSendPublisher struct {
	errors []error
}

func (p *failureSendPublisher) Publish(context.Context, string, []byte) error {
	if len(p.errors) == 0 {
		return nil
	}
	err := p.errors[0]
	p.errors = p.errors[1:]
	return err
}

func TestSoakSender_LocalPublishFailureBecomesNotSent(t *testing.T) {
	now := time.Date(2026, 8, 15, 1, 2, 3, 0, time.UTC)
	ledger, err := newFailureLedger(&failureLedgerConfig{Capacity: 1})
	require.NoError(t, err)
	tracker := newSoakFailureTracker(ledger, 0, time.Minute, func() time.Time { return now })
	publisher := &failureSendPublisher{errors: []error{nats.ErrConnectionClosed}}
	sender := newSoakSender(soakSendConfig{
		SiteID: "site-1", ReplyTimeout: 5 * time.Second,
	}, newSoakCatalog(8, 100, 0, nil), publisher, nil,
		rand.New(rand.NewSource(1)), &soakSendIDs{
			MessageID: func() string { return failureSendTestMessageID },
			RequestID: func() string { return failureSendTestRequestID },
		}, withSoakSendLifecycle(tracker, nil),
	)

	pending, err := sender.Publish(context.Background(), soakSendTarget{
		UserID: "u-1", Account: "alice", RoomID: "room-1",
	}, "hello")

	require.ErrorIs(t, err, nats.ErrConnectionClosed)
	require.NotNil(t, pending)
	snapshot := ledger.Snapshot()
	assert.Zero(t, snapshot.Active)
	assert.Equal(t, uint64(1), snapshot.Results[failureResultNotSent])
	assert.Zero(t, sender.Pending(), "a definite local rejection must not expire as a second admission result")
}

func TestSoakSender_AmbiguousPublishFailureRemainsActive(t *testing.T) {
	now := time.Date(2026, 8, 15, 1, 2, 3, 0, time.UTC)
	ledger, err := newFailureLedger(&failureLedgerConfig{Capacity: 1})
	require.NoError(t, err)
	tracker := newSoakFailureTracker(ledger, 0, time.Minute, func() time.Time { return now })
	publisher := &failureSendPublisher{errors: []error{errors.New("ambiguous publish failure")}}
	sender := newSoakSender(soakSendConfig{
		SiteID: "site-1", ReplyTimeout: 5 * time.Second,
	}, newSoakCatalog(8, 100, 0, nil), publisher, nil,
		rand.New(rand.NewSource(1)), &soakSendIDs{
			MessageID: func() string { return failureSendTestMessageID },
			RequestID: func() string { return failureSendTestRequestID },
		}, withSoakSendLifecycle(tracker, nil),
	)

	_, err = sender.Publish(context.Background(), soakSendTarget{
		UserID: "u-1", Account: "alice", RoomID: "room-1",
	}, "hello")

	require.Error(t, err)
	operation, ok := ledger.Active(failureSendTestMessageID)
	require.True(t, ok)
	assert.Equal(t, failureOperationActive, operation.LifecycleState)
}
