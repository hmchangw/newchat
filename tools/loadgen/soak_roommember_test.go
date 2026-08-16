package main

import (
	"context"
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type soakRoomLaneFixture struct {
	lanes     *soakRoomLanes
	pool      *soakRoomStatePool
	ledger    *failureLedger
	verifier  *soakRoomStateVerifier
	store     *soakRoomStateStoreStub
	transport *soakRoomOpsTransport
	metrics   *Metrics
	now       time.Time
}

func (f *soakRoomLaneFixture) advance(d time.Duration) {
	f.now = f.now.Add(d)
}

func newSoakRoomLaneFixture(t *testing.T, reply []byte, requestErr error) *soakRoomLaneFixture {
	t.Helper()
	fixture := &soakRoomLaneFixture{
		now:       time.Date(2026, 8, 16, 7, 0, 0, 0, time.UTC),
		transport: &soakRoomOpsTransport{reply: reply, err: requestErr},
		metrics:   NewMetrics(),
		store:     &soakRoomStateStoreStub{},
	}
	now := func() time.Time { return fixture.now }

	pool, err := newSoakRoomStatePool(
		soakRoomStateTestTopology(3), 8, fixture.metrics, rand.New(rand.NewSource(1)),
	)
	require.NoError(t, err)
	ledger, err := newFailureLedger(failureLedgerConfig{Capacity: 16, Now: now})
	require.NoError(t, err)
	rpc := newSoakRPCClient(
		fixture.transport, soakRetryConfig{MaxAttempts: 3}, &soakRecordingSleeper{}, nil,
	)
	reader := newSoakRoomReader(
		soakRoomReadConfig{SiteID: "site-a", BatchSize: 4}, pool, rpc,
		&soakRoomReadRecorder{}, rand.New(rand.NewSource(2)), now,
	)

	fixture.pool = pool
	fixture.ledger = ledger
	fixture.verifier = newSoakRoomStateVerifier(
		reader, fixture.store, fixture.metrics,
		newFailureObserverHealth(failureObserverRoomState, fixture.now), now,
	)
	fixture.lanes = newSoakRoomLanes(
		soakRoomLaneConfig{
			RunID: "run-1", PersistGrace: time.Second, Deadline: time.Minute,
			RetryInterval: time.Second, RoomCreateBudget: 2, CreateRoomSize: 2,
		},
		pool, newSoakRoomMutator("site-a", rpc, time.Second, now), ledger,
		reader, fixture.store, fixture.metrics, nil, now,
	)
	return fixture
}

func soakSingleActiveOperation(t *testing.T, ledger *failureLedger) failureOperation {
	t.Helper()
	operations := ledger.ActiveOperations()
	require.Len(t, operations, 1)
	return operations[0]
}

func TestSoakRoomLanes_JournalsTheIntentBeforeSending(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, []byte(`{"status":"accepted"}`), nil)

	require.NoError(t, fixture.lanes.MemberMutation(context.Background()))

	operation := soakSingleActiveOperation(t, fixture.ledger)
	assert.Equal(t, soakFailureLaneMemberMutation, operation.Lane)
	assert.Equal(t, failureOperationMemberAdd, operation.OperationType)
	assert.Equal(t, "true", operation.Attributes[soakFailureAttributeExpectedMember])
	assert.Equal(t, failureObservationGood, operation.Observations[failureObserverAdmission])
	assert.Equal(t, failureOperationActive, operation.LifecycleState)
	assert.Equal(t, 1, fixture.transport.calls())
}

func TestSoakRoomLanes_AmbiguousMutationIsUnverifiedAndNeverResent(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, nil, nats.ErrTimeout)

	require.NoError(t, fixture.lanes.MemberMutation(context.Background()))

	operation := soakSingleActiveOperation(t, fixture.ledger)
	assert.Equal(t, failureObservationUnverified, operation.Observations[failureObserverAdmission])
	assert.Equal(t, 1, fixture.transport.calls(),
		"an ambiguous mutation must not be resent")
	assert.Equal(t, uint64(0), fixture.ledger.Snapshot().Results[failureResultNotSent],
		"a timeout is never proof the request stayed local")
}

func TestSoakRoomLanes_ProvenLocalFailureIsNotSent(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, []byte(`{"status":"accepted"}`), nil)
	fixture.lanes.mutator = newSoakRoomMutator("site-a", nil, time.Second, func() time.Time {
		return fixture.now
	})

	require.NoError(t, fixture.lanes.MemberMutation(context.Background()))

	assert.Equal(t, uint64(1), fixture.ledger.Snapshot().Results[failureResultNotSent])
	assert.Empty(t, fixture.ledger.ActiveOperations())
}

func TestSoakRoomLanes_ExplicitRejectionClosesTheOperation(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, []byte(`{"error":"no","code":"forbidden"}`), nil)

	require.NoError(t, fixture.lanes.MemberMutation(context.Background()))

	snapshot := fixture.ledger.Snapshot()
	assert.Equal(t, uint64(1), snapshot.Results[failureResultBad])
	assert.Empty(t, fixture.ledger.ActiveOperations(),
		"a rejected mutation has no effect left to observe")
}

func TestSoakRoomLanes_LedgerRefusalSkipsTheRequest(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, []byte(`{"status":"accepted"}`), nil)
	full, err := newFailureLedger(failureLedgerConfig{
		Capacity: 1, Now: func() time.Time { return fixture.now },
	})
	require.NoError(t, err)
	require.NoError(t, full.Start(&failureOperation{
		SchemaVersion: 2, ID: "filler", RunID: "run-1",
		Scenario: soakFailureScenario, Lane: soakFailureLaneMemberMutation,
		OperationType: failureOperationMemberAdd, LifecycleState: failureOperationJournaled,
		StartedAt: fixture.now, VerifyAfter: fixture.now, Deadline: fixture.now.Add(time.Minute),
		Targets: map[string]string{"roomId": "room-1"}, Effects: memberMutationExpectedEffects(),
	}))
	fixture.lanes.ledger = full

	require.NoError(t, fixture.lanes.MemberMutation(context.Background()))

	assert.Equal(t, 0, fixture.transport.calls(),
		"an unjournaled room mutation must never be sent")
	assert.Equal(t, float64(1), testutil.ToFloat64(
		fixture.metrics.FailureUntracked.WithLabelValues(failureUntrackedReasonStart)))
}

func TestSoakRoomLanes_ReconcileConfirmsAppliedState(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, []byte(`{"status":"accepted"}`), nil)
	require.NoError(t, fixture.lanes.MemberMutation(context.Background()))
	fixture.store.member = true
	fixture.advance(2 * time.Second)

	reconciled, err := fixture.lanes.Reconcile(context.Background(), fixture.verifier)

	require.NoError(t, err)
	assert.True(t, reconciled)
	assert.Equal(t, uint64(1), fixture.ledger.Snapshot().Results[failureResultGood])
}

func TestSoakRoomLanes_ReconcileRetriesBeforeTheDeadline(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, []byte(`{"status":"accepted"}`), nil)
	require.NoError(t, fixture.lanes.MemberMutation(context.Background()))
	fixture.store.member = false
	fixture.advance(2 * time.Second)

	reconciled, err := fixture.lanes.Reconcile(context.Background(), fixture.verifier)

	require.NoError(t, err)
	assert.True(t, reconciled)
	assert.Len(t, fixture.ledger.ActiveOperations(), 1,
		"an absent effect before the deadline is retried, not concluded")
}

func TestSoakRoomLanes_AcceptedThenAbsentAtDeadlineIsMissing(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, []byte(`{"status":"accepted"}`), nil)
	require.NoError(t, fixture.lanes.MemberMutation(context.Background()))
	fixture.store.member = false
	fixture.advance(2 * time.Minute)

	reconciled, err := fixture.lanes.Reconcile(context.Background(), fixture.verifier)

	require.NoError(t, err)
	assert.True(t, reconciled)
	assert.Equal(t, uint64(1),
		fixture.ledger.Snapshot().Results[failureResultMissingAfterDeadline],
		"room-service accepted it and the state never appeared, so this is real loss")
}

func TestSoakRoomLanes_AmbiguousThenAbsentAtDeadlineIsUnverified(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, nil, nats.ErrTimeout)
	require.NoError(t, fixture.lanes.MemberMutation(context.Background()))
	fixture.store.member = false
	fixture.advance(2 * time.Minute)

	reconciled, err := fixture.lanes.Reconcile(context.Background(), fixture.verifier)

	require.NoError(t, err)
	assert.True(t, reconciled)
	snapshot := fixture.ledger.Snapshot()
	assert.Equal(t, uint64(1), snapshot.Results[failureResultUnverified])
	assert.Equal(t, uint64(0), snapshot.Results[failureResultMissingAfterDeadline],
		"an unaccepted request that left no state is not evidence of loss")
}

func TestSoakRoomLanes_UnansweredObserverAtDeadlineIsUnverified(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, []byte(`{"status":"accepted"}`), nil)
	require.NoError(t, fixture.lanes.MemberMutation(context.Background()))
	fixture.transport.err = nats.ErrNoResponders
	fixture.store.memberErr = errors.New("primary unavailable")
	fixture.advance(2 * time.Minute)

	reconciled, err := fixture.lanes.Reconcile(context.Background(), fixture.verifier)

	require.NoError(t, err)
	assert.True(t, reconciled)
	assert.Equal(t, uint64(1), fixture.ledger.Snapshot().Results[failureResultUnverified])
}

func TestSoakRoomLanes_TerminalResultReturnsTheCandidateToThePool(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, []byte(`{"status":"accepted"}`), nil)
	require.NoError(t, fixture.lanes.MemberMutation(context.Background()))
	fixture.store.member = true
	fixture.advance(2 * time.Second)
	_, err := fixture.lanes.Reconcile(context.Background(), fixture.verifier)
	require.NoError(t, err)

	next, ok := fixture.pool.NextMemberIntent()

	require.True(t, ok, "the room lease must be released when the operation concludes")
	assert.False(t, next.Add, "a confirmed add is followed by its paired remove")
}

func TestSoakRoomLanes_RoomMutationAlternatesRenameAndMute(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, []byte(`{"status":"accepted","muted":true}`), nil)

	require.NoError(t, fixture.lanes.RoomMutation(context.Background()))
	require.NoError(t, fixture.lanes.RoomMutation(context.Background()))

	types := make(map[failureOperationType]int)
	for _, operation := range fixture.ledger.ActiveOperations() {
		types[operation.OperationType]++
		assert.Equal(t, soakFailureLaneRoomMutation, operation.Lane)
	}
	assert.Equal(t, 1, types[failureOperationRoomRename])
	assert.Equal(t, 1, types[failureOperationMuteToggle])
}

func TestSoakRoomLanes_RenameCarriesTheExpectedAndPreviousName(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, []byte(`{"status":"accepted"}`), nil)

	require.NoError(t, fixture.lanes.rename(context.Background()))

	operation := soakSingleActiveOperation(t, fixture.ledger)
	assert.NotEmpty(t, operation.Attributes[soakFailureAttributeExpectedName])
	assert.Equal(t, "soak-run-channel-000001",
		operation.Attributes[soakFailureAttributePreviousName])
}

func TestSoakRoomLanes_MuteCarriesTheExpectedState(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, []byte(`{"status":"ok","muted":true}`), nil)

	require.NoError(t, fixture.lanes.muteToggle(context.Background()))

	operation := soakSingleActiveOperation(t, fixture.ledger)
	assert.Equal(t, "true", operation.Attributes[soakFailureAttributeExpectedMuted])
}

func TestSoakRoomLanes_RoomCreateStopsAtBudget(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t,
		[]byte(`{"status":"accepted","roomId":"room-new","roomType":"channel"}`), nil)

	for range 4 {
		require.NoError(t, fixture.lanes.RoomCreate(context.Background()))
	}

	assert.Equal(t, 2, fixture.transport.calls(), "the create lane must stop at its budget")
	assert.Equal(t, float64(0),
		soakGaugeValue(t, fixture.metrics.SoakRoomCreateBudgetRemaining))
	assert.Positive(t, testutil.ToFloat64(
		fixture.metrics.SoakRoomPoolExhausted.WithLabelValues("create_budget")))

	require.NoError(t, fixture.lanes.MemberMutation(context.Background()),
		"the other lanes keep running after the create budget is spent")
	assert.Equal(t, 3, fixture.transport.calls())
}

func TestSoakRoomLanes_RoomCreateTakesOwnershipOnceConfirmed(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t,
		[]byte(`{"status":"accepted","roomId":"room-new","roomType":"channel"}`), nil)
	require.NoError(t, fixture.lanes.RoomCreate(context.Background()))
	fixture.store.nameFound = true
	fixture.store.name = "soak-run-1-created"
	fixture.advance(2 * time.Second)

	reconciled, err := fixture.lanes.Reconcile(context.Background(), fixture.verifier)

	require.NoError(t, err)
	assert.True(t, reconciled)
	assert.Equal(t, []string{"room-new"}, fixture.store.appended,
		"teardown only removes rooms this run has claimed")
}

func TestSoakRoomLanes_RoomCreateSkipsOwnershipWhenNotConfirmed(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t,
		[]byte(`{"status":"accepted","roomId":"room-new","roomType":"channel"}`), nil)
	require.NoError(t, fixture.lanes.RoomCreate(context.Background()))
	fixture.store.nameFound = false
	fixture.advance(2 * time.Minute)

	_, err := fixture.lanes.Reconcile(context.Background(), fixture.verifier)

	require.NoError(t, err)
	assert.Empty(t, fixture.store.appended)
}

func TestSoakRoomLanes_QuarantineProbeResolvesACandidate(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, nil, nats.ErrTimeout)
	require.NoError(t, fixture.lanes.MemberMutation(context.Background()))
	fixture.store.memberErr = errors.New("primary unavailable")
	fixture.advance(2 * time.Minute)
	_, err := fixture.lanes.Reconcile(context.Background(), fixture.verifier)
	require.NoError(t, err)

	fixture.store.memberErr = nil
	fixture.store.member = true
	probed, err := fixture.lanes.ProbeQuarantine(context.Background(), fixture.verifier)

	require.NoError(t, err)
	assert.True(t, probed)
	next, ok := fixture.pool.NextMemberIntent()
	require.True(t, ok)
	assert.False(t, next.Add, "a candidate probed as present is removed, never re-added")
}

func TestSoakRoomLanes_QuarantineProbeKeepsUnansweredPairs(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, nil, nats.ErrTimeout)
	require.NoError(t, fixture.lanes.MemberMutation(context.Background()))
	fixture.advance(2 * time.Minute)
	_, err := fixture.lanes.Reconcile(context.Background(), fixture.verifier)
	require.NoError(t, err)
	fixture.store.memberErr = errors.New("primary unavailable")

	probed, err := fixture.lanes.ProbeQuarantine(context.Background(), fixture.verifier)

	require.Error(t, err)
	assert.True(t, probed)
	assert.Equal(t, float64(1), testutil.ToFloat64(
		fixture.metrics.SoakRoomQuarantineProbes.WithLabelValues("unresolved")))
	_, ok := fixture.pool.NextProbe()
	assert.True(t, ok, "an unanswered probe stays queued")
}

func TestSoakRoomLanes_ProbeIsANoOpWithoutQuarantine(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, []byte(`{"status":"accepted"}`), nil)

	probed, err := fixture.lanes.ProbeQuarantine(context.Background(), fixture.verifier)

	require.NoError(t, err)
	assert.False(t, probed)
}

func TestSoakRoomLanes_ReconcileRequiresConfiguration(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, []byte(`{"status":"accepted"}`), nil)

	_, err := fixture.lanes.Reconcile(context.Background(), nil)
	require.Error(t, err)

	_, err = fixture.lanes.ProbeQuarantine(context.Background(), nil)
	require.Error(t, err)
}

func TestSoakRoomLanes_ReconcileIsIdleWithoutDueOperations(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, []byte(`{"status":"accepted"}`), nil)

	reconciled, err := fixture.lanes.Reconcile(context.Background(), fixture.verifier)

	require.NoError(t, err)
	assert.False(t, reconciled)
}

// soakRoomLaneMutationRecorder captures the latency and error samples the lanes
// emit for the shared soak collector.
type soakRoomLaneMutationRecorder struct {
	samples []soakMutationSample
}

func (r *soakRoomLaneMutationRecorder) Record(sample soakMutationSample) {
	r.samples = append(r.samples, sample)
}

func TestSoakRoomLanes_RecordsMutationSamples(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, []byte(`{"status":"accepted"}`), nil)
	recorder := &soakRoomLaneMutationRecorder{}
	fixture.lanes.recorder = recorder

	require.NoError(t, fixture.lanes.MemberMutation(context.Background()))

	require.Len(t, recorder.samples, 1)
	assert.Equal(t, soakRPCMemberAdd, recorder.samples[0].Action)
	assert.Empty(t, recorder.samples[0].ErrorClass)
}

func TestSoakRoomLanes_LedgerRefusalRestoresEveryIntentKind(t *testing.T) {
	for _, testCase := range []struct {
		name string
		call func(*soakRoomLanes, context.Context) error
	}{
		{
			name: "member",
			call: func(l *soakRoomLanes, ctx context.Context) error { return l.MemberMutation(ctx) },
		},
		{
			name: "rename",
			call: func(l *soakRoomLanes, ctx context.Context) error { return l.rename(ctx) },
		},
		{
			name: "mute",
			call: func(l *soakRoomLanes, ctx context.Context) error { return l.muteToggle(ctx) },
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			fixture := newSoakRoomLaneFixture(t, []byte(`{"status":"accepted"}`), nil)
			closed, err := newFailureLedger(failureLedgerConfig{
				Capacity: 1, Now: func() time.Time { return fixture.now },
			})
			require.NoError(t, err)
			require.NoError(t, closed.Close())
			fixture.lanes.ledger = closed

			require.NoError(t, testCase.call(fixture.lanes, context.Background()))

			assert.Equal(t, 0, fixture.transport.calls())
			assert.Equal(t, float64(1), testutil.ToFloat64(
				fixture.metrics.FailureUntracked.WithLabelValues(failureUntrackedReasonStart)))
		})
	}
}

func TestSoakRoomLanes_RenameMismatchIsRecordedAsBad(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, []byte(`{"status":"accepted"}`), nil)
	require.NoError(t, fixture.lanes.rename(context.Background()))
	fixture.store.nameFound = true
	fixture.store.name = "renamed-by-someone-else"
	fixture.advance(2 * time.Second)

	reconciled, err := fixture.lanes.Reconcile(context.Background(), fixture.verifier)

	require.NoError(t, err)
	assert.True(t, reconciled)
	assert.Equal(t, uint64(1), fixture.ledger.Snapshot().Results[failureResultBad])
}

func TestSoakRoomLanes_MuteReconcilesFromTheStore(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, []byte(`{"status":"ok","muted":true}`), nil)
	require.NoError(t, fixture.lanes.muteToggle(context.Background()))
	fixture.store.mutedFound = true
	fixture.store.muted = true
	fixture.advance(2 * time.Second)

	reconciled, err := fixture.lanes.Reconcile(context.Background(), fixture.verifier)

	require.NoError(t, err)
	assert.True(t, reconciled)
	assert.Equal(t, uint64(1), fixture.ledger.Snapshot().Results[failureResultGood])
}

func TestSoakRoomLanes_MuteProbeResolvesTheParkedPair(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, nil, nats.ErrTimeout)
	require.NoError(t, fixture.lanes.muteToggle(context.Background()))
	fixture.store.mutedErr = errors.New("primary unavailable")
	fixture.advance(2 * time.Minute)
	_, err := fixture.lanes.Reconcile(context.Background(), fixture.verifier)
	require.NoError(t, err)

	fixture.store.mutedErr = nil
	fixture.store.mutedFound = true
	fixture.store.muted = true
	probed, probeErr := fixture.lanes.ProbeQuarantine(context.Background(), fixture.verifier)

	require.NoError(t, probeErr)
	assert.True(t, probed)
	intent, ok := soakNextMuteIntentFor(t, fixture.pool, "user-a0")
	if ok {
		assert.False(t, intent.TargetMuted, "the probe found it muted, so the next toggle unmutes")
	}
}

func TestSoakRoomLanes_MuteProbeKeepsAnUnknownSubscription(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, nil, nats.ErrTimeout)
	require.NoError(t, fixture.lanes.muteToggle(context.Background()))
	fixture.store.mutedErr = errors.New("primary unavailable")
	fixture.advance(2 * time.Minute)
	_, err := fixture.lanes.Reconcile(context.Background(), fixture.verifier)
	require.NoError(t, err)

	// The store answers again but still cannot see the subscription, so the
	// pair's real mute state remains unknown.
	fixture.store.mutedErr = nil
	fixture.store.mutedFound = false
	probed, probeErr := fixture.lanes.ProbeQuarantine(context.Background(), fixture.verifier)

	require.NoError(t, probeErr)
	assert.True(t, probed)
	_, ok := fixture.pool.NextProbe()
	assert.True(t, ok, "a subscription the store cannot confirm stays queued")
}

func TestSoakRoomLanes_RoomCreateReturnsBudgetWhenNoMembersAreAvailable(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t,
		[]byte(`{"status":"accepted","roomId":"room-new"}`), nil)
	empty, err := newSoakRoomStatePool(
		soakRoomStateTestTopology(1), 8, fixture.metrics, rand.New(rand.NewSource(3)),
	)
	require.NoError(t, err)
	// Drain the single candidate so the create lane has nobody to invite.
	intent, ok := empty.NextMemberIntent()
	require.True(t, ok)
	empty.SettleMember(intent, failureResultGood)
	fixture.lanes.pool = empty
	fixture.lanes.cfg.CreateRoomSize = 2

	require.NoError(t, fixture.lanes.RoomCreate(context.Background()))

	assert.Equal(t, 0, fixture.transport.calls())
	assert.Equal(t, float64(2),
		soakGaugeValue(t, fixture.metrics.SoakRoomCreateBudgetRemaining),
		"an unspent attempt must return its budget")
}

func TestSoakRoomLanes_RoomCreateSurfacesRequestFailures(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, nil, nats.ErrTimeout)

	err := fixture.lanes.RoomCreate(context.Background())

	require.Error(t, err)
	assert.Empty(t, fixture.ledger.ActiveOperations(),
		"a create with no room ID has no target to reconcile")
}

func TestSoakRoomLanes_RoomCreateWithoutRoomIDIsNotTracked(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, []byte(`{"status":"accepted"}`), nil)

	require.NoError(t, fixture.lanes.RoomCreate(context.Background()))

	assert.Empty(t, fixture.ledger.ActiveOperations())
}

func TestSoakRoomLanes_OwnershipFailureIsCountedAsUntracked(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t,
		[]byte(`{"status":"accepted","roomId":"room-new"}`), nil)
	fixture.store.appendErr = errors.New("mongo unavailable")
	require.NoError(t, fixture.lanes.RoomCreate(context.Background()))
	fixture.store.nameFound = true
	fixture.advance(2 * time.Second)

	_, err := fixture.lanes.Reconcile(context.Background(), fixture.verifier)

	require.NoError(t, err)
	assert.Equal(t, float64(1), testutil.ToFloat64(
		fixture.metrics.FailureUntracked.WithLabelValues("ownership")))
}

func TestSoakRoomLanes_SettleIgnoresUnknownOperations(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, []byte(`{"status":"accepted"}`), nil)

	assert.NotPanics(t, func() {
		fixture.lanes.settle("missing-operation", failureResultGood)
		fixture.lanes.finishCreate("", failureResultGood)
		fixture.lanes.finishCreate("room-x", failureResultUnverified)
	})
	assert.Empty(t, fixture.store.appended)
}

func TestSoakRoomLanes_WorkWithoutMetricsOrRecorder(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, []byte(`{"status":"accepted"}`), nil)
	fixture.lanes.metrics = nil
	fixture.lanes.recorder = nil

	assert.NotPanics(t, func() {
		require.NoError(t, fixture.lanes.MemberMutation(context.Background()))
		fixture.lanes.countExhausted("create_budget")
		fixture.lanes.countProbe("resolved")
		fixture.lanes.countUntracked(failureUntrackedReasonStart)
		fixture.lanes.setBudgetGauge()
	})
}

func TestSoakRoomMutationNeverSent_OnlyProvenLocalFailures(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		outcome soakRoomMutationOutcome
		err     error
		want    bool
	}{
		{name: "no error", want: false},
		{
			name: "marshal failure", err: errors.New("marshal"),
			outcome: soakRoomMutationOutcome{ErrorClass: soakErrorDecode}, want: true,
		},
		{
			name: "never attempted", err: errors.New("no rpc client"), want: true,
		},
		{
			name: "timeout", err: nats.ErrTimeout,
			outcome: soakRoomMutationOutcome{ErrorClass: soakErrorTimeout}, want: false,
		},
		{
			name: "disconnected", err: nats.ErrConnectionClosed,
			outcome: soakRoomMutationOutcome{ErrorClass: soakErrorDisconnected}, want: false,
		},
		{
			name: "context canceled", err: context.Canceled, want: false,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			assert.Equal(t, testCase.want,
				soakRoomMutationNeverSent(&testCase.outcome, testCase.err))
		})
	}
}

func TestSoakRoomTerminalResult_MapsEveryObservation(t *testing.T) {
	assert.Equal(t, failureResultGood, soakRoomTerminalResult(failureObservationGood))
	assert.Equal(t, failureResultBad, soakRoomTerminalResult(failureObservationBad))
	assert.Equal(t, failureResultMissingAfterDeadline,
		soakRoomTerminalResult(failureObservationMissingAfterDeadline))
	assert.Equal(t, failureResultUnverified,
		soakRoomTerminalResult(failureObservationUnverified))
}

func TestSoakRoomLanes_ObserveReleasesTheClaimOnLedgerFailure(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, []byte(`{"status":"accepted"}`), nil)
	require.NoError(t, fixture.lanes.MemberMutation(context.Background()))
	operation := soakSingleActiveOperation(t, fixture.ledger)

	err := fixture.lanes.observe(
		operation.ID, failureObservationBad, failureReasonNone, fixture.now,
	)

	require.Error(t, err, "a bad observation without a bounded reason must be rejected")
	assert.Len(t, fixture.ledger.ActiveOperations(), 1)
}

func TestSoakRoomLanes_ReleaseRejectsUnknownOperations(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, []byte(`{"status":"accepted"}`), nil)

	err := fixture.lanes.release("missing-operation", fixture.now)

	require.Error(t, err)
}
