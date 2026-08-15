package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

type memoryFailureJournal struct {
	events []failureLedgerEvent
}

func (j *memoryFailureJournal) Replay() ([]failureLedgerEvent, error) {
	return append([]failureLedgerEvent(nil), j.events...), nil
}

func (j *memoryFailureJournal) Append(event *failureLedgerEvent) error {
	j.events = append(j.events, *event)
	return nil
}

func (j *memoryFailureJournal) Compact(events []failureLedgerEvent) error {
	j.events = append([]failureLedgerEvent(nil), events...)
	return nil
}

func (*memoryFailureJournal) Size() int64  { return 0 }
func (*memoryFailureJournal) Close() error { return nil }

func TestFailureLedger_Version2ReplayAndUnknownVersion(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "run.wal")
	wal, err := openFailureWAL(path)
	require.NoError(t, err)
	ledger, err := newFailureLedger(failureLedgerConfig{Capacity: 2, Journal: wal})
	require.NoError(t, err)
	op := testFailureOperation("message-v2", now)
	op.SchemaVersion = 2
	op.LifecycleState = failureOperationJournaled
	op.RunID = "nats-core-1"
	op.OperationType = failureOperationMessageCreate
	op.Targets = map[string]string{"messageId": op.ID}
	op.Effects = messageCreateExpectedEffects(2, "abc")
	op.Expected = nil
	require.NoError(t, ledger.Start(op))
	require.NoError(t, ledger.Close())

	reopened, err := openFailureWAL(path)
	require.NoError(t, err)
	recovered, err := newFailureLedger(failureLedgerConfig{Capacity: 2, Journal: reopened})
	require.NoError(t, err)
	operation, ok := recovered.ClaimDue(now.Add(time.Minute))
	require.True(t, ok)
	assert.Equal(t, failureOperationMessageCreate, operation.OperationType)
	assert.Len(t, operation.Effects, 3)
	require.NoError(t, recovered.Close())

	badPath := filepath.Join(t.TempDir(), "future.wal")
	require.NoError(t, os.WriteFile(badPath, []byte("{\"recordType\":\"header\",\"schemaVersion\":99}\n"), 0o600))
	badWAL, err := openFailureWAL(badPath)
	require.NoError(t, err)
	_, err = badWAL.Replay()
	assert.Error(t, err)
	require.NoError(t, badWAL.Close())
}

func TestFailureWAL_RejectsMixedVersionFraming(t *testing.T) {
	tests := []struct {
		name     string
		contents string
	}{
		{
			name: "versioned header with legacy record",
			contents: "{\"recordType\":\"header\",\"schemaVersion\":2}\n" +
				"{\"type\":\"checkpoint\",\"at\":\"2026-08-12T01:02:03Z\"}\n",
		},
		{
			name:     "versioned record without header",
			contents: "{\"schemaVersion\":2,\"type\":\"checkpoint\",\"at\":\"2026-08-12T01:02:03Z\"}\n",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "mixed.wal")
			require.NoError(t, os.WriteFile(path, []byte(tc.contents), 0o600))
			wal, err := openFailureWAL(path)
			require.NoError(t, err)
			_, err = wal.Replay()
			assert.ErrorContains(t, err, "version")
			require.NoError(t, wal.Close())
		})
	}
}

func TestFailureLedger_CompactionRestoresTerminalCounters(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "run.wal")
	wal, err := openFailureWAL(path)
	require.NoError(t, err)
	ledger, err := newFailureLedger(failureLedgerConfig{
		Capacity: 2, CompactEvery: 1, Journal: wal, Now: func() time.Time { return now },
	})
	require.NoError(t, err)
	require.NoError(t, ledger.Start(testFailureOperation("complete", now)))
	_, err = ledger.Observe("complete", failureObserverAdmission, failureObservationGood, now)
	require.NoError(t, err)
	_, err = ledger.Observe("complete", failureObserverHistory, failureObservationGood, now)
	require.NoError(t, err)
	require.NoError(t, ledger.Close())

	reopened, err := openFailureWAL(path)
	require.NoError(t, err)
	recovered, err := newFailureLedger(failureLedgerConfig{Capacity: 2, Journal: reopened})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, recovered.Close()) })
	assert.Equal(t, uint64(1), recovered.Snapshot().Results[failureResultGood])
}

func TestFailureWAL_CompactionSyncsContainingDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run.wal")
	wal, err := openFailureWAL(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, wal.Close()) })
	var synced []string
	wal.syncDirectory = func(directory string) error {
		synced = append(synced, directory)
		return nil
	}

	require.NoError(t, wal.Compact(nil))

	assert.Equal(t, []string{filepath.Dir(path), filepath.Dir(path)}, synced)
}

func TestFailureLedger_ReplayRejectsDuplicateStartedOperation(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	operation := testFailureOperation("duplicate", now)
	journal := &memoryFailureJournal{events: []failureLedgerEvent{
		{Type: failureLedgerEventStarted, Operation: operation, At: now},
		{Type: failureLedgerEventStarted, Operation: operation, At: now},
	}}
	_, err := newFailureLedger(failureLedgerConfig{Capacity: 2, Journal: journal})
	assert.ErrorContains(t, err, "already active")
}

func TestFailureWAL_HeaderlessLegacyIsAtomicallyUpgradedBeforeNewWrites(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "legacy.wal")
	legacy, err := json.Marshal(failureLedgerEvent{
		Type: failureLedgerEventStarted, Operation: testFailureOperation("legacy", now), At: now,
	})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, append(legacy, '\n'), 0o600))

	wal, err := openFailureWAL(path)
	require.NoError(t, err)
	ledger, err := newFailureLedger(failureLedgerConfig{Capacity: 3, Journal: wal})
	require.NoError(t, err)
	require.NoError(t, ledger.Start(testFailureOperation("new", now)))
	require.NoError(t, ledger.Close())

	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Contains(t, string(contents), `"recordType":"header"`)
	reopened, err := openFailureWAL(path)
	require.NoError(t, err)
	recovered, err := newFailureLedger(failureLedgerConfig{Capacity: 3, Journal: reopened})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, recovered.Close()) })
	assert.Equal(t, 2, recovered.Snapshot().Active)
}

func TestFailureLedger_DownstreamObservationAfterNotSentInvalidatesAccounting(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	ledger, err := newFailureLedger(failureLedgerConfig{Capacity: 1})
	require.NoError(t, err)
	require.NoError(t, ledger.Start(testFailureOperation("not-sent", now)))
	require.NoError(t, ledger.Abandon("not-sent", failureResultNotSent, now))

	_, err = ledger.Observe("not-sent", failureObserverHistory, failureObservationGood, now)
	assert.ErrorContains(t, err, "accounting invariant")
	assert.Equal(t, "accounting_invariant", ledger.Snapshot().InvalidReason)
}

func TestFailureOperation_RejectsInvalidVersion2Contracts(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	valid := testFailureOperation("message", now)
	valid.SchemaVersion = 2
	valid.LifecycleState = failureOperationJournaled
	valid.RunID = "run"
	valid.OperationType = failureOperationMessageCreate
	valid.Targets = map[string]string{"messageId": "message"}
	valid.Effects = messageCreateExpectedEffects(1, "hash")
	valid.Expected = nil
	tests := []struct {
		name   string
		mutate func(*failureOperation)
	}{
		{name: "future schema", mutate: func(operation *failureOperation) { operation.SchemaVersion = 99 }},
		{name: "unknown scenario", mutate: func(operation *failureOperation) { operation.Scenario = "tenant" }},
		{name: "unknown lane", mutate: func(operation *failureOperation) { operation.Lane = "room-1" }},
		{name: "missing run", mutate: func(operation *failureOperation) { operation.RunID = "" }},
		{name: "unsupported observer effect", mutate: func(operation *failureOperation) { operation.Effects[0].Observer = "dynamic" }},
		{name: "duplicate effect", mutate: func(operation *failureOperation) { operation.Effects = append(operation.Effects, operation.Effects[0]) }},
		{name: "invalid cardinality", mutate: func(operation *failureOperation) { operation.Effects[2].Cardinality.Count = 0 }},
		{name: "verify before start", mutate: func(operation *failureOperation) { operation.VerifyAfter = now.Add(-time.Second) }},
		{name: "deadline before verify", mutate: func(operation *failureOperation) { operation.Deadline = now }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			operation := cloneFailureOperation(valid)
			tc.mutate(operation)
			assert.Error(t, validateFailureOperation(operation))
		})
	}
	assert.Error(t, validateFailureOperation(nil))
	assert.False(t, validFailureObservation("future"))
	assert.False(t, validFailureResult("future"))
}

func TestFailureObserverHealth_TracksIntervals(t *testing.T) {
	start := time.Date(2026, 8, 12, 1, 0, 0, 0, time.UTC)
	health := newFailureObserverHealth(failureObserverRecipient, start)
	(*failureObserverHealth)(nil).Set(true, start, "ignored")
	assert.Empty(t, (*failureObserverHealth)(nil).Snapshot(start).Intervals)
	health.Set(true, start.Add(-time.Second), "out-of-order")
	health.Set(true, start.Add(time.Second), "connected")
	health.Set(true, start.Add(2*time.Second), "probe")
	assert.True(t, health.HealthyThroughout(start.Add(time.Second), start.Add(4*time.Second)))
	health.Set(false, start.Add(5*time.Second), "disconnected")
	health.Set(true, start.Add(9*time.Second), "reconnected")
	snapshot := health.Snapshot(start.Add(10 * time.Second))
	assert.True(t, snapshot.Up)
	require.Len(t, snapshot.Intervals, 4)
	assert.False(t, snapshot.Intervals[0].Up)
	assert.Equal(t, 4*time.Second, snapshot.Intervals[1].End.Sub(snapshot.Intervals[1].Start))
}

func TestFailureObserverHealth_SnapshotClipsAtEvaluationEnd(t *testing.T) {
	start := time.Date(2026, 8, 12, 1, 0, 0, 0, time.UTC)
	health := newFailureObserverHealth(failureObserverRecipient, start)
	health.Set(true, start, "connected")
	endedAt := start.Add(10 * time.Second)
	health.Set(false, endedAt.Add(time.Second), "shutdown")

	snapshot := health.Snapshot(endedAt)

	assert.True(t, snapshot.Up)
	require.Len(t, snapshot.Intervals, 1)
	assert.Equal(t, endedAt, snapshot.Intervals[0].End)
}

func TestFailureRecipientObserver_ExactPartialUnexpectedAndDuplicate(t *testing.T) {
	tests := []struct {
		name                           string
		observed                       []string
		want                           failureObservation
		missing, unexpected, duplicate []string
	}{
		{name: "exact", observed: []string{"bob", "alice"}, want: failureObservationGood},
		{name: "partial", observed: []string{"alice"}, want: failureObservationMissingAfterDeadline, missing: []string{"bob"}},
		{name: "unexpected", observed: []string{"alice", "bob", "mallory"}, want: failureObservationBad, unexpected: []string{"mallory"}},
		{name: "duplicate", observed: []string{"alice", "alice", "bob"}, want: failureObservationBad, duplicate: []string{"alice"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			observer := newRecipientEvidence(false)
			require.NoError(t, observer.Expect("message-1", []string{"alice", "bob"}))
			for _, recipient := range tc.observed {
				observer.Observe("message-1", recipient)
			}
			result := observer.Finalize("message-1", true)
			assert.Equal(t, tc.want, result.Observation)
			assert.Equal(t, tc.missing, result.Missing)
			assert.Equal(t, tc.unexpected, result.Unexpected)
			assert.Equal(t, tc.duplicate, result.Duplicates)
		})
	}
}

func TestFailureRecipientEvidence_ValidationAllowedDuplicatesAndForget(t *testing.T) {
	assert.Error(t, (*recipientEvidence)(nil).Expect("", nil))
	evidence := newRecipientEvidence(true)
	assert.Error(t, evidence.Expect("message", []string{"alice", "alice"}))
	require.NoError(t, evidence.Expect("message", []string{"alice"}))
	assert.Error(t, evidence.Expect("message", []string{"alice"}))
	assert.True(t, evidence.Observe("message", "alice"))
	assert.True(t, evidence.Observe("message", "alice"))
	assert.True(t, evidence.Complete("message"))
	result := evidence.Finalize("message", true)
	assert.Equal(t, failureObservationGood, result.Observation)
	assert.Equal(t, []string{"alice"}, result.Duplicates)
	evidence.Forget("missing")
	assert.False(t, evidence.Observe("missing", "alice"))
	assert.Equal(t, failureObservationUnverified, evidence.Finalize("missing", true).Observation)
}

func TestFailureRecipientObserver_AppliesManifestDuplicatePolicy(t *testing.T) {
	observer := newFailureRecipientObserver(
		nil, nil, 1, time.Now,
		withFailureRecipientDuplicatePolicy(true),
	)
	require.NoError(t, observer.Expect("message", []string{"alice"}))
	assert.True(t, observer.evidence.Observe("message", "alice"))
	assert.True(t, observer.evidence.Observe("message", "alice"))
	assert.Equal(t, failureObservationGood, observer.evidence.Finalize("message", true).Observation)
}

func TestFailureRecipientObserver_QueueOverflowInvalidatesWithoutBlocking(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	metrics := NewMetrics()
	ledger, err := newFailureLedger(failureLedgerConfig{Capacity: 1, Recorder: newFailureLedgerPromRecorder(metrics)})
	require.NoError(t, err)
	observer := newFailureRecipientObserver(ledger, metrics, 1, func() time.Time { return now })
	observer.health.Set(true, now, "subscribed")
	payload, err := json.Marshal(model.RoomEvent{Type: model.RoomEventNewMessage, RoomID: "room-1", LastMsgID: "message-1"})
	require.NoError(t, err)
	assert.True(t, observer.Enqueue("alice", payload))
	assert.False(t, observer.Enqueue("bob", payload))
	assert.Equal(t, "observer_queue", ledger.Snapshot().InvalidReason)
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.FailureObserverEvents.WithLabelValues("recipient_broadcast", "unverified")))
}

func TestFailureRecipientObserver_ProcessesExactSet(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	metrics := NewMetrics()
	ledger, err := newFailureLedger(failureLedgerConfig{Capacity: 1})
	require.NoError(t, err)
	op := testFailureOperation("message-1", now)
	op.SchemaVersion, op.RunID, op.OperationType = 2, "run-1", failureOperationMessageCreate
	op.LifecycleState = failureOperationJournaled
	op.Targets = map[string]string{"messageId": "message-1"}
	op.Attributes = map[string]string{"expected_recipients": "alice\nbob"}
	op.Effects, op.Expected = messageCreateExpectedEffects(2, "hash"), nil
	require.NoError(t, ledger.Start(op))
	evidenceDirectory := t.TempDir()
	observer := newFailureRecipientObserver(
		ledger, metrics, 4, func() time.Time { return now },
		withFailureRecipientEvidenceDir(evidenceDirectory),
	)
	require.NoError(t, observer.Expect("message-1", []string{"alice", "bob"}))
	ctx, cancel := context.WithCancel(context.Background())
	observer.Run(ctx)
	payload, err := json.Marshal(model.RoomEvent{Type: model.RoomEventNewMessage, RoomID: "room-1", LastMsgID: "message-1"})
	require.NoError(t, err)
	require.True(t, observer.Enqueue("alice", payload))
	require.True(t, observer.Enqueue("bob", payload))
	require.Eventually(t, func() bool {
		return observer.evidence.Complete("message-1")
	}, time.Second, time.Millisecond)
	operation, ok := ledger.Active("message-1")
	require.True(t, ok)
	assert.NotContains(t, operation.Observations, failureObserverRecipient)
	cancel()
	observer.Wait()

	recovered := newFailureRecipientObserver(
		ledger, metrics, 4, func() time.Time { return now },
		withFailureRecipientEvidenceDir(evidenceDirectory),
	)
	require.NoError(t, recovered.Recover(ledger.ActiveOperations()))
	assert.True(t, recovered.evidence.Complete("message-1"))
}

func TestFailureRecipientObserver_PersistsExactAnomalySidecars(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	directory := t.TempDir()
	observer := newFailureRecipientObserver(
		nil, NewMetrics(), 4, func() time.Time { return now },
		withFailureRecipientEvidenceDir(directory),
	)
	require.NoError(t, observer.Expect("message-1", []string{"alice", "bob"}))
	assert.True(t, observer.evidence.Observe("message-1", "alice"))
	result := observer.Finalize("message-1", now, now.Add(time.Minute))
	assert.Equal(t, failureObservationUnverified, result.Observation)

	// A startup-down observer makes the absence unverified, but the exact
	// missing recipient remains retained evidence rather than disappearing.
	raw, err := os.ReadFile(filepath.Join(directory, ".recipient-missing.raw.jsonl"))
	require.NoError(t, err)
	assert.Contains(t, string(raw), `"operationId":"message-1"`)
	assert.Contains(t, string(raw), `"recipient":"bob"`)
}

func TestFailureRecipientObserver_SubscriptionsAreAttributedAndFlushedBeforeHealthy(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	source := &fakeFailureRecipientSource{}
	observer := newFailureRecipientObserver(nil, NewMetrics(), 8, func() time.Time { return now })
	topology := &soakTopology{
		Rooms: []model.Room{{ID: "channel-1", Type: model.RoomTypeChannel}},
		Subscriptions: []model.Subscription{
			{RoomID: "channel-1", RoomType: model.RoomTypeChannel, IsSubscribed: true, User: model.SubscriptionUser{Account: "alice"}},
			{RoomID: "channel-1", RoomType: model.RoomTypeChannel, IsSubscribed: true, User: model.SubscriptionUser{Account: "bob"}},
		},
	}
	subscriptions, err := startFailureRecipientSubscriptions(source, topology, observer)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, subscriptions.Close()) })
	assert.True(t, source.flushed)
	assert.True(t, observer.health.Snapshot(now).Up)

	got := append([]string(nil), source.subjects...)
	sort.Strings(got)
	assert.Equal(t, []string{
		subject.RoomEvent("channel-1", false) + "\x00alice",
		subject.RoomEvent("channel-1", false) + "\x00bob",
		subject.RoomEvent("channel-1", true) + "\x00alice",
		subject.RoomEvent("channel-1", true) + "\x00bob",
		subject.UserRoomEvent("alice") + "\x00alice",
		subject.UserRoomEvent("bob") + "\x00bob",
	}, got)
}

func TestFailureRecipientObserver_UsesBoundedRecipientConnectionPool(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	connections := make(map[int]*fakeFailureRecipientConnection)
	source := newPooledNATSFailureRecipientSource(2, func(index int) (failureRecipientConnection, error) {
		connection := &fakeFailureRecipientConnection{}
		connections[index] = connection
		return connection, nil
	})
	observer := newFailureRecipientObserver(nil, NewMetrics(), 8, func() time.Time { return now })
	topology := &soakTopology{
		Rooms: []model.Room{{ID: "channel-1", Type: model.RoomTypeChannel}},
		Subscriptions: []model.Subscription{
			{RoomID: "channel-1", IsSubscribed: true, User: model.SubscriptionUser{Account: "alice"}},
			{RoomID: "channel-1", IsSubscribed: true, User: model.SubscriptionUser{Account: "bob"}},
			{RoomID: "channel-1", IsSubscribed: true, User: model.SubscriptionUser{Account: "carol"}},
		},
	}

	subscriptions, err := startFailureRecipientSubscriptions(source, topology, observer)
	require.NoError(t, err)
	require.Len(t, connections, 2)
	assert.NotSame(t, connections[0], connections[1])
	assert.True(t, connections[0].flushed)
	assert.True(t, connections[1].flushed)

	require.NoError(t, subscriptions.Close())
	assert.True(t, connections[0].drained)
	assert.True(t, connections[1].drained)
}

func TestFailureRecipientObserver_RejectsSubscriptionAndMalformedEvidence(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	ledger, err := newFailureLedger(failureLedgerConfig{Capacity: 1})
	require.NoError(t, err)
	observer := newFailureRecipientObserver(
		ledger, NewMetrics(), 2, func() time.Time { return now },
		withFailureRecipientEvidenceDir(t.TempDir()),
	)
	assert.False(t, (*failureRecipientObserver)(nil).Enqueue("alice", nil))
	assert.Error(t, observer.Expect("", nil))
	assert.Error(t, func() error {
		_, err := startFailureRecipientSubscriptions(nil, nil, nil)
		return err
	}())
	_, err = startFailureRecipientSubscriptions(&fakeFailureRecipientSource{}, &soakTopology{}, observer)
	assert.ErrorContains(t, err, "no eligible subscriptions")

	topology := &soakTopology{
		Rooms: []model.Room{{ID: "room", Type: model.RoomTypeChannel}},
		Subscriptions: []model.Subscription{{
			RoomID: "room", IsSubscribed: true, User: model.SubscriptionUser{Account: "alice"},
		}},
	}
	_, err = startFailureRecipientSubscriptions(&fakeFailureRecipientSource{subscribeErr: assert.AnError}, topology, observer)
	assert.ErrorIs(t, err, assert.AnError)
	_, err = startFailureRecipientSubscriptions(&fakeFailureRecipientSource{flushErr: assert.AnError}, topology, observer)
	assert.ErrorIs(t, err, assert.AnError)

	observer.process(recipientDelivery{recipient: "alice", payload: []byte(`not-json`)})
	assert.Equal(t, "observer_malformed", ledger.Snapshot().InvalidReason)
	assert.Error(t, observer.appendRawEvidence("future", "message", "alice"))
}

func TestFailureRecipientObserver_SyncsDirectoryWhenCreatingRawJournal(t *testing.T) {
	directory := t.TempDir()
	var synced []string
	observer := newFailureRecipientObserver(
		nil,
		NewMetrics(),
		1,
		time.Now,
		withFailureRecipientEvidenceDir(directory),
		withFailureRecipientDirectorySync(func(path string) error {
			synced = append(synced, path)
			return nil
		}),
	)

	require.NoError(t, observer.appendRawEvidence("missing", "message-1", "alice"))
	require.NoError(t, observer.appendRawEvidence("missing", "message-2", "bob"))

	assert.Equal(t, []string{directory}, synced)
}

func TestFailureRecipientObserver_IgnoresUnrelatedRoomEvents(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	ledger, err := newFailureLedger(failureLedgerConfig{Capacity: 1})
	require.NoError(t, err)
	observer := newFailureRecipientObserver(ledger, NewMetrics(), 2, func() time.Time { return now })
	payload, err := json.Marshal(model.RoomEvent{
		Type: model.RoomEventMessageEdited, RoomID: "room-1", LastMsgID: "message-1",
	})
	require.NoError(t, err)

	observer.process(recipientDelivery{recipient: "alice", payload: payload})

	assert.Empty(t, ledger.Snapshot().InvalidReason)
}

func TestFailureRecipientObserver_RejectsWrongRoomAndEventType(t *testing.T) {
	tests := []struct {
		name  string
		event model.RoomEvent
	}{
		{
			name: "wrong room",
			event: model.RoomEvent{
				Type: model.RoomEventNewMessage, RoomID: "room-2", LastMsgID: "message-1",
			},
		},
		{
			name: "wrong event type",
			event: model.RoomEvent{
				Type: model.RoomEventNewThreadMessage, RoomID: "room-1", LastMsgID: "message-1",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
			ledger, err := newFailureLedger(failureLedgerConfig{Capacity: 1})
			require.NoError(t, err)
			observer := newFailureRecipientObserver(ledger, NewMetrics(), 2, func() time.Time { return now })
			observer.health.Set(true, now, "subscribed")
			tracker := newSoakFailureTracker(
				ledger, 0, time.Minute, func() time.Time { return now },
				withSoakFailureRecipientObserver(observer),
			)
			pending := testSoakFailurePending(now)
			pending.Target.Recipients = []string{"alice"}
			require.NoError(t, tracker.Start(pending))
			payload, err := json.Marshal(tc.event)
			require.NoError(t, err)

			observer.process(recipientDelivery{recipient: "alice", payload: payload})
			result := observer.Finalize("message-1", now, now)

			assert.Equal(t, failureObservationBad, result.Observation)
			assert.Equal(t, []string{"alice"}, result.Mismatches)
		})
	}
}

func TestFailureRecipientObserver_ReplaysDurableMismatchAfterRestart(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	directory := t.TempDir()
	first := newFailureRecipientObserver(
		nil,
		NewMetrics(),
		2,
		func() time.Time { return now },
		withFailureRecipientEvidenceDir(directory),
	)
	require.NoError(t, first.ExpectEvent(
		"message-1",
		[]string{"alice"},
		"room-1",
		model.RoomEventNewMessage,
	))
	payload, err := json.Marshal(model.RoomEvent{
		Type: model.RoomEventNewMessage, RoomID: "room-2", LastMsgID: "message-1",
	})
	require.NoError(t, err)
	first.process(recipientDelivery{recipient: "alice", payload: payload})

	restarted := newFailureRecipientObserver(
		nil,
		NewMetrics(),
		2,
		func() time.Time { return now },
		withFailureRecipientEvidenceDir(directory),
	)
	require.NoError(t, restarted.Recover([]failureOperation{{
		ID:       "message-1",
		Expected: []failureObserver{failureObserverRecipient},
		Targets:  map[string]string{"roomId": "room-1"},
		Attributes: map[string]string{
			"expected_recipients":      "alice",
			"expected_recipient_event": string(model.RoomEventNewMessage),
		},
	}}))
	restarted.health.Set(true, now, "subscribed")

	result := restarted.Finalize("message-1", now, now)
	assert.Equal(t, failureObservationBad, result.Observation)
	assert.Equal(t, []string{"alice"}, result.Mismatches)
}

func TestFailureRecipientObserver_DrainProcessesQueuedEvidence(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	observer := newFailureRecipientObserver(nil, NewMetrics(), 2, func() time.Time { return now })
	require.NoError(t, observer.Expect("message-1", []string{"alice"}))
	payload, err := json.Marshal(model.RoomEvent{
		Type: model.RoomEventNewMessage, RoomID: "room-1", LastMsgID: "message-1",
	})
	require.NoError(t, err)
	require.True(t, observer.Enqueue("alice", payload))

	observer.Drain()

	assert.True(t, observer.evidence.Complete("message-1"))
}

func TestFailureRecipientObserver_ShutdownStopsIngressBeforeDrainingQueue(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	var sequence []string
	connection := &fakeFailureRecipientConnection{onDrain: func() { sequence = append(sequence, "ingress") }}
	source := newPooledNATSFailureRecipientSource(1, func(int) (failureRecipientConnection, error) {
		return connection, nil
	})
	observer := newFailureRecipientObserver(nil, NewMetrics(), 2, func() time.Time { return now })
	require.NoError(t, observer.Expect("message-1", []string{"alice"}))
	payload, err := json.Marshal(model.RoomEvent{
		Type: model.RoomEventNewMessage, RoomID: "room-1", LastMsgID: "message-1",
	})
	require.NoError(t, err)
	require.True(t, observer.Enqueue("alice", payload))
	_, err = source.Subscribe("room", "alice", func(*nats.Msg) {})
	require.NoError(t, err)
	subscriptions := &failureRecipientSubscriptions{source: source}

	err = shutdownFailureRecipientObserver(subscriptions, func() {
		sequence = append(sequence, "observer")
	}, observer)

	require.NoError(t, err)
	assert.Equal(t, []string{"ingress", "observer"}, sequence)
	assert.True(t, observer.evidence.Complete("message-1"))
}

func TestFailureRecipientConnection_DrainWaitsForClosedCallback(t *testing.T) {
	closed := make(chan struct{})
	drainCalled := make(chan struct{})
	connection := &natsFailureRecipientConnection{
		closed: closed,
		drain: func() error {
			close(drainCalled)
			return nil
		},
	}
	done := make(chan error, 1)
	go func() { done <- connection.Drain() }()

	<-drainCalled
	select {
	case err := <-done:
		require.Failf(t, "drain returned early", "error: %v", err)
	default:
	}
	close(closed)
	require.NoError(t, <-done)
}

func TestFailureRecipientConnection_DrainWrapsErrors(t *testing.T) {
	wantErr := errors.New("connection unavailable")
	connection := &natsFailureRecipientConnection{
		drain: func() error { return wantErr },
	}

	err := connection.Drain()

	require.ErrorContains(t, err, "drain recipient observer connection")
	assert.ErrorIs(t, err, wantErr)
}

func TestFailureRecipientConnection_DrainWrapsTimeout(t *testing.T) {
	closed := make(chan struct{})
	close(closed)
	connection := &natsFailureRecipientConnection{
		closed:    closed,
		drain:     func() error { return nil },
		lastError: func() error { return nats.ErrDrainTimeout },
	}

	err := connection.Drain()

	require.ErrorContains(t, err, "drain recipient observer connection")
	assert.ErrorIs(t, err, nats.ErrDrainTimeout)
}

func TestSoakRuntimeSelector_RecipientSetsExcludeBots(t *testing.T) {
	topology := &soakTopology{
		ActiveUsers: []model.User{{ID: "u-alice", Account: "alice"}},
		Subscriptions: []model.Subscription{
			{RoomID: "room-1", IsSubscribed: true, User: model.SubscriptionUser{Account: "alice"}},
			{RoomID: "room-1", IsSubscribed: true, User: model.SubscriptionUser{Account: "borrowed-subscriber"}},
			{RoomID: "room-1", IsSubscribed: true, User: model.SubscriptionUser{Account: "bot", IsBot: true}},
			{RoomID: "room-1", IsSubscribed: false, User: model.SubscriptionUser{Account: "departed"}},
		}}

	assert.Equal(t, map[string][]string{"room-1": {"alice", "borrowed-subscriber"}}, soakRecipientSets(topology))
}

func TestFailureRecipientObserver_RecoversPendingExpectation(t *testing.T) {
	observer := newFailureRecipientObserver(nil, nil, 1, time.Now)
	operations := []failureOperation{{
		ID: "pending", Expected: []failureObserver{failureObserverRecipient},
		Targets:    map[string]string{"roomId": "legacy-room"},
		Attributes: map[string]string{"expected_recipients": "alice\nbob"},
	}}
	require.NoError(t, observer.Recover(operations))
	assert.True(t, observer.evidence.Observe("pending", "alice"))
	assert.Error(t, observer.Recover(operations))
	assert.Error(t, (*failureRecipientObserver)(nil).Recover(nil))
}

type fakeFailureRecipientSource struct {
	subjects     []string
	flushed      bool
	subscribeErr error
	flushErr     error
}

func (s *fakeFailureRecipientSource) Subscribe(subjectName, recipient string, _ nats.MsgHandler) (failureRecipientSubscription, error) {
	if s.subscribeErr != nil {
		return nil, s.subscribeErr
	}
	s.subjects = append(s.subjects, subjectName+"\x00"+recipient)
	return fakeFailureRecipientSubscription{}, nil
}

func (s *fakeFailureRecipientSource) Flush() error {
	s.flushed = true
	return s.flushErr
}

type fakeFailureRecipientSubscription struct{}

func (fakeFailureRecipientSubscription) Unsubscribe() error { return nil }

type fakeFailureRecipientConnection struct {
	flushed bool
	drained bool
	onDrain func()
}

func (*fakeFailureRecipientConnection) Subscribe(
	_ string,
	_ nats.MsgHandler,
) (failureRecipientSubscription, error) {
	return fakeFailureRecipientSubscription{}, nil
}

func (c *fakeFailureRecipientConnection) Flush() error {
	c.flushed = true
	return nil
}

func (c *fakeFailureRecipientConnection) Drain() error {
	c.drained = true
	if c.onDrain != nil {
		c.onDrain()
	}
	return nil
}
