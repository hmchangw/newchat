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

	"github.com/hmchangw/chat/pkg/subject"
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
	ledger, err := newFailureLedger(&failureLedgerConfig{Capacity: 16, Now: now})
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
		reader, fixture.store, "site-a", fixture.metrics,
		newFailureObserverHealth(failureObserverRoomState, fixture.now), now,
	)
	fixture.lanes = newSoakRoomLanes(
		soakRoomLaneConfig{
			RunID: "run-1", SiteID: "site-a",
			PersistGrace: time.Second, Deadline: time.Minute,
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
	full, err := newFailureLedger(&failureLedgerConfig{
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
	assert.Equal(t, float64(0), testutil.ToFloat64(
		fixture.metrics.SoakLaneAttempts.WithLabelValues(
			soakFailureLaneMemberMutation, soakLaneAttemptSent,
		)),
		"a refused mutation is not offered load")
	assert.Equal(t, float64(1), testutil.ToFloat64(
		fixture.metrics.SoakLaneAttempts.WithLabelValues(
			soakFailureLaneMemberMutation, soakLaneAttemptRefused,
		)))
}

// The traffic-validity gate reads sent alone, so each way a slot can pass
// without a request has to land on its own outcome rather than on sent.
func TestSoakRoomLanes_CountsLaneAttemptsByOutcome(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, []byte(`{"status":"accepted"}`), nil)

	require.NoError(t, fixture.lanes.MemberMutation(context.Background()))
	assert.Equal(t, float64(1), testutil.ToFloat64(
		fixture.metrics.SoakLaneAttempts.WithLabelValues(
			soakFailureLaneMemberMutation, soakLaneAttemptSent,
		)))

	// Every room now holds a member lease, so the next slot finds no target.
	for range 8 {
		require.NoError(t, fixture.lanes.MemberMutation(context.Background()))
	}
	assert.Positive(t, testutil.ToFloat64(
		fixture.metrics.SoakLaneAttempts.WithLabelValues(
			soakFailureLaneMemberMutation, soakLaneAttemptNoTarget,
		)),
		"an exhausted pool must not look like offered load")
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

func TestSoakRoomLanes_ReconcileBacksOffRepeatedRoomStateProbes(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, []byte(`{"status":"accepted"}`), nil)
	require.NoError(t, fixture.lanes.MemberMutation(context.Background()))
	fixture.store.member = false
	fixture.advance(2 * time.Second)

	reconciled, err := fixture.lanes.Reconcile(context.Background(), fixture.verifier)
	require.NoError(t, err)
	assert.True(t, reconciled)
	fixture.advance(time.Second)
	reconciled, err = fixture.lanes.Reconcile(context.Background(), fixture.verifier)
	require.NoError(t, err)
	assert.True(t, reconciled)
	operation := soakSingleActiveOperation(t, fixture.ledger)
	assert.Equal(t, fixture.now.Add(2*time.Second), operation.nextVerifyAt)

	fixture.advance(time.Second)
	reconciled, err = fixture.lanes.Reconcile(context.Background(), fixture.verifier)
	require.NoError(t, err)
	assert.False(t, reconciled, "the fixed one-second retry would probe again here")
}

func TestSoakRoomLanes_ReconcileSchedulesProbeFromVerificationCompletion(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, []byte(`{"status":"accepted"}`), nil)
	require.NoError(t, fixture.lanes.MemberMutation(context.Background()))
	fixture.advance(2 * time.Second)
	fixture.store.contextErr = func(context.Context) error {
		fixture.advance(5 * time.Second)
		return errors.New("primary unavailable")
	}

	reconciled, err := fixture.lanes.Reconcile(context.Background(), fixture.verifier)

	require.NoError(t, err)
	assert.True(t, reconciled)
	operation := soakSingleActiveOperation(t, fixture.ledger)
	wait := max(
		fixture.now.Sub(operation.VerifyAfter), fixture.lanes.cfg.RetryInterval,
	)
	assert.Equal(t, fixture.now.Add(wait), operation.nextVerifyAt,
		"verification latency must not make the released probe immediately due")
}

func TestSoakRoomLanes_RepeatedAbsentProbesStillReachTheDeadlineVerdict(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, []byte(`{"status":"accepted"}`), nil)
	require.NoError(t, fixture.lanes.MemberMutation(context.Background()))
	fixture.store.member = false

	probes := 0
	for probes < 16 && len(fixture.ledger.ActiveOperations()) > 0 {
		operation := soakSingleActiveOperation(t, fixture.ledger)
		if operation.nextVerifyAt.After(fixture.now) {
			fixture.advance(operation.nextVerifyAt.Sub(fixture.now))
		}
		reconciled, err := fixture.lanes.Reconcile(context.Background(), fixture.verifier)
		require.NoError(t, err)
		require.True(t, reconciled)
		probes++
	}

	assert.Greater(t, probes, 2, "the regression requires a real backoff walk")
	assert.Empty(t, fixture.ledger.ActiveOperations())
	assert.Equal(t, uint64(1),
		fixture.ledger.Snapshot().Results[failureResultMissingAfterDeadline],
		"the last authoritative probe must run at the deadline instead of expiring unverified")
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
	fixture.store.byNameOK = true
	fixture.store.byName = "room-new"
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
	fixture.store.byNameOK = false
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

func TestSoakRoomLanes_QuarantineProbeBoundsAuthoritativeReads(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, nil, nats.ErrTimeout)
	require.NoError(t, fixture.lanes.MemberMutation(context.Background()))
	fixture.advance(2 * time.Minute)
	_, err := fixture.lanes.Reconcile(context.Background(), fixture.verifier)
	require.NoError(t, err)
	fixture.store.contextErr = func(ctx context.Context) error {
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		assert.WithinDuration(
			t, time.Now().Add(soakRoomStateTimeout), deadline, time.Second,
		)
		return errors.New("primary unavailable")
	}

	probed, err := fixture.lanes.ProbeQuarantine(context.Background(), fixture.verifier)

	require.Error(t, err)
	assert.True(t, probed)
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
			closed, err := newFailureLedger(&failureLedgerConfig{
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
	require.True(t, ok, "a resolved probe must return the pair to the pool")
	assert.False(t, intent.TargetMuted, "the probe found it muted, so the next toggle unmutes")
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

// A timed-out create may still have made the room. The intent is journaled
// before the request precisely so that ambiguity stays reconcilable instead of
// leaving an unowned room behind.
func TestSoakRoomLanes_RoomCreateKeepsATimedOutAttemptReconcilable(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, nil, nats.ErrTimeout)

	require.NoError(t, fixture.lanes.RoomCreate(context.Background()))

	operations := fixture.ledger.ActiveOperations()
	require.Len(t, operations, 1)
	assert.Equal(t, soakFailureLaneRoomCreate, operations[0].Lane)
	assert.NotEmpty(t, operations[0].Targets["roomName"],
		"the client-generated name is the only handle a lost reply leaves")
	assert.Equal(t, failureObservationUnverified,
		operations[0].Observations[failureObserverAdmission],
		"a timeout proves nothing about whether the room was created")
}

// A reply that omits the room ID is not a failure: the name still identifies
// the room, and the store resolves the ID when ownership is claimed.
func TestSoakRoomLanes_RoomCreateTracksAReplyWithoutARoomID(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, []byte(`{"status":"accepted"}`), nil)

	require.NoError(t, fixture.lanes.RoomCreate(context.Background()))

	operations := fixture.ledger.ActiveOperations()
	require.Len(t, operations, 1)
	assert.NotEmpty(t, operations[0].Targets["roomName"])
}

// Ownership is retried while the deadline allows it; past the deadline the room
// is a real leak and has to be reported rather than quietly closed.
func TestSoakRoomLanes_OwnershipFailureIsCountedAsUntracked(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t,
		[]byte(`{"status":"accepted","roomId":"room-new"}`), nil)
	fixture.store.appendErr = errors.New("mongo unavailable")
	require.NoError(t, fixture.lanes.RoomCreate(context.Background()))
	fixture.store.byNameOK = true
	fixture.store.byName = "room-new"
	fixture.advance(2 * time.Minute)

	_, err := fixture.lanes.Reconcile(context.Background(), fixture.verifier)

	require.NoError(t, err)
	assert.Equal(t, float64(1), testutil.ToFloat64(
		fixture.metrics.FailureUntracked.WithLabelValues("ownership")))
}

func TestSoakRoomLanes_SettleIgnoresUnknownOperations(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, []byte(`{"status":"accepted"}`), nil)

	assert.NotPanics(t, func() {
		fixture.lanes.settle("missing-operation", failureResultGood)
	})
	assert.Empty(t, fixture.store.appended)

	// A create with no journaled name has no handle to claim by, so ownership
	// fails loudly rather than silently marking nothing.
	require.Error(t, fixture.lanes.claimCreatedRoom(
		context.Background(), &failureOperation{ID: "operation-1"},
	))
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
			name: "request never encoded", err: errors.New("marshal"),
			outcome: soakRoomMutationOutcome{ErrorClass: soakErrorRequestEncode}, want: true,
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

func TestSoakRoomLanes_ReleaseProbeRejectsUnknownOperations(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, []byte(`{"status":"accepted"}`), nil)

	err := fixture.lanes.releaseProbe(&failureOperation{
		ID:          "missing-operation",
		VerifyAfter: fixture.now,
		Deadline:    fixture.now.Add(time.Minute),
	}, fixture.now)

	require.Error(t, err)
}

func TestNewSoakRoomLanes_AppliesSafeDefaults(t *testing.T) {
	lanes := newSoakRoomLanes(
		soakRoomLaneConfig{RunID: "run-1"}, nil, nil, nil, nil, nil, nil, nil, nil,
	)

	assert.Equal(t, 10*time.Minute, lanes.cfg.Deadline)
	assert.Equal(t, time.Second, lanes.cfg.RetryInterval)
	assert.Equal(t, 2, lanes.cfg.CreateRoomSize)
	assert.NotNil(t, lanes.now)
	assert.Equal(t, 0, lanes.budget)
}

func TestSoakRoomLanes_ReleasesReservationsTheLedgerExpired(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, []byte(`{"status":"accepted"}`), nil)
	require.NoError(t, fixture.lanes.MemberMutation(context.Background()))

	// The background expiry sweep finalizes any unclaimed operation past its
	// deadline, so a reconcile slot is not guaranteed to be the path that ends
	// an operation. The lane must still get its room lease back.
	fixture.advance(2 * time.Minute)
	expiredIDs, err := fixture.ledger.Expire(fixture.now)
	expired := len(expiredIDs)
	require.NoError(t, err)
	require.Equal(t, 1, expired)

	settled := fixture.lanes.SettleFinalized()

	assert.Equal(t, 1, settled)
	_, ok := fixture.pool.NextMemberIntent()
	assert.True(t, ok, "a room whose operation expired must keep producing mutations")
}

func TestSoakRoomLanes_ExpiredReservationIsTreatedAsUnknownState(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, []byte(`{"status":"accepted"}`), nil)
	require.NoError(t, fixture.lanes.MemberMutation(context.Background()))
	fixture.advance(2 * time.Minute)
	_, err := fixture.ledger.Expire(fixture.now)
	require.NoError(t, err)

	fixture.lanes.SettleFinalized()

	probe, ok := fixture.pool.NextProbe()
	require.True(t, ok,
		"an expired mutation was never verified, so the candidate's state is unknown")
	assert.False(t, probe.Mute)
}

func TestSoakRoomLanes_SettleFinalizedKeepsLiveReservations(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, []byte(`{"status":"accepted"}`), nil)
	require.NoError(t, fixture.lanes.MemberMutation(context.Background()))

	assert.Equal(t, 0, fixture.lanes.SettleFinalized(),
		"an operation still awaiting verification keeps its reservation")
	_, ok := fixture.pool.NextMemberIntent()
	assert.False(t, ok, "the room lease is still held")
}

func TestSoakRoomLanes_ReadReceiptJournalsTheBaseline(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, []byte(`{"status":"accepted"}`), nil)

	require.NoError(t, fixture.lanes.ReadReceipt(context.Background()))

	operation := soakSingleActiveOperation(t, fixture.ledger)
	assert.Equal(t, soakFailureLaneReadReceipt, operation.Lane)
	assert.Equal(t, failureOperationMessageRead, operation.OperationType)
	assert.Equal(t, failureObservationGood, operation.Observations[failureObserverAdmission])
	assert.Equal(t, subject.MessageRead(
		operation.Attributes[soakFailureAttributeTargetAccount], "room-1", "site-a",
	), fixture.transport.subjects[0])
}

func TestSoakRoomLanes_ReadReceiptConfirmsAnAdvancedCursor(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, []byte(`{"status":"accepted"}`), nil)
	require.NoError(t, fixture.lanes.ReadReceipt(context.Background()))
	fixture.store.seenFound = true
	fixture.store.lastSeen = fixture.now.Add(time.Second)
	fixture.advance(2 * time.Second)

	reconciled, err := fixture.lanes.Reconcile(context.Background(), fixture.verifier)

	require.NoError(t, err)
	assert.True(t, reconciled)
	assert.Equal(t, uint64(1), fixture.ledger.Snapshot().Results[failureResultGood])
}

func TestSoakRoomLanes_ReadReceiptMissingCursorAtDeadlineIsMissing(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, []byte(`{"status":"accepted"}`), nil)
	require.NoError(t, fixture.lanes.ReadReceipt(context.Background()))
	fixture.store.seenFound = true
	fixture.store.lastSeen = time.Time{}
	fixture.advance(2 * time.Minute)

	reconciled, err := fixture.lanes.Reconcile(context.Background(), fixture.verifier)

	require.NoError(t, err)
	assert.True(t, reconciled)
	assert.Equal(t, uint64(1),
		fixture.ledger.Snapshot().Results[failureResultMissingAfterDeadline],
		"room-service accepted the mark-read and the cursor never moved")
}

func TestSoakRoomLanes_ReadReceiptCursorBecomesTheNextBaseline(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, []byte(`{"status":"accepted"}`), nil)
	require.NoError(t, fixture.lanes.ReadReceipt(context.Background()))
	first := soakSingleActiveOperation(t, fixture.ledger)
	observed := fixture.now.Add(time.Second).UTC()
	fixture.store.seenFound = true
	fixture.store.lastSeen = observed
	fixture.advance(2 * time.Second)
	_, err := fixture.lanes.Reconcile(context.Background(), fixture.verifier)
	require.NoError(t, err)

	account := first.Attributes[soakFailureAttributeTargetAccount]
	next, found := soakNextReadIntentFor(t, fixture.pool, account)
	require.True(t, found)
	assert.True(t, next.Known)
	assert.Equal(t, observed, next.Baseline)
}

func TestSoakRoomLanes_ReadReceiptWithoutBaselineAcceptsAnyCursor(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, []byte(`{"status":"accepted"}`), nil)
	require.NoError(t, fixture.lanes.ReadReceipt(context.Background()))
	operation := soakSingleActiveOperation(t, fixture.ledger)
	require.NotContains(t, operation.Attributes, soakFailureAttributeReadBaseline,
		"the seeded subscriptions were never read, so there is no baseline to beat")

	fixture.store.seenFound = true
	fixture.store.lastSeen = fixture.now.Add(-time.Hour)
	fixture.advance(2 * time.Second)
	reconciled, err := fixture.lanes.Reconcile(context.Background(), fixture.verifier)

	require.NoError(t, err)
	assert.True(t, reconciled)
	assert.Equal(t, uint64(1), fixture.ledger.Snapshot().Results[failureResultGood])
}

// A body that could not be encoded never reached the wire. A reply that could
// not be decoded did: the server answered, so the mutation very likely ran.
// Collapsing both into one class made an executed mutation settle as not_sent,
// which closes the operation and drops a real effect from the accounting.
func TestSoakRoomMutationNeverSent_SeparatesEncodeFromDecode(t *testing.T) {
	for _, testCase := range []struct {
		name  string
		class soakErrorClass
		want  bool
	}{
		{name: "request never encoded", class: soakErrorRequestEncode, want: true},
		{name: "reply arrived but did not decode", class: soakErrorResponseDecode, want: false},
		{name: "timeout is ambiguous", class: soakErrorTimeout, want: false},
		{name: "no responder is ambiguous", class: soakErrorNoResponder, want: false},
		{name: "disconnected is ambiguous", class: soakErrorDisconnected, want: false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			outcome := soakRoomMutationOutcome{ErrorClass: testCase.class}

			got := soakRoomMutationNeverSent(&outcome, assert.AnError)

			assert.Equal(t, testCase.want, got)
		})
	}
}

// A replacement process must retake the reservations its predecessor held, and
// must rebuild each intent from the journaled attributes. settle writes the
// pool's next expectation from the intent, so a defaulted Add or TargetMuted
// records the opposite of what the operation actually asked for.
func TestSoakRoomLanes_RehydrateRestoresIntentsFromTheJournal(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, []byte(`{"status":"accepted"}`), nil)

	inFlight := fixture.lanes.Rehydrate([]failureOperation{
		{
			ID: "op-add", Lane: soakFailureLaneMemberMutation,
			OperationType: failureOperationMemberAdd,
			Targets:       map[string]string{"roomId": "room-1", "account": "user-a1"},
			Attributes: map[string]string{
				soakFailureAttributeRequester: "user-a0",
			},
		},
		{
			ID: "op-mute", Lane: soakFailureLaneRoomMutation,
			OperationType: failureOperationMuteToggle,
			Targets:       map[string]string{"roomId": "room-1", "account": "user-a2"},
			Attributes:    map[string]string{soakFailureAttributeExpectedMuted: "true"},
		},
		{
			ID: "op-create", Lane: soakFailureLaneRoomCreate,
			OperationType: failureOperationRoomCreate,
			Targets:       map[string]string{"roomName": "soak-run-1-created-abc"},
		},
		{
			ID: "op-message", Lane: soakFailureLaneMessageSend,
			OperationType: failureOperationMessageCreate,
			Targets:       map[string]string{"roomId": "room-1"},
		},
	})

	assert.Equal(t, 1, inFlight, "only creates consume the run's room budget")

	fixture.lanes.mu.Lock()
	add := fixture.lanes.pending["op-add"]
	mute := fixture.lanes.pending["op-mute"]
	create := fixture.lanes.pending["op-create"]
	_, tookMessageLane := fixture.lanes.pending["op-message"]
	fixture.lanes.mu.Unlock()

	assert.True(t, add.member.Add, "an add that settles as a remove corrupts the ring")
	assert.Equal(t, "user-a0", add.member.Requester)
	assert.True(t, mute.mute.TargetMuted, "the pool would otherwise record the opposite state")
	assert.Equal(t, "soak-run-1-created-abc", create.roomName)
	assert.False(t, tookMessageLane, "the message lane holds no pool reservation")

	// The reservation is real: the room is now leased, so the lane cannot issue
	// a second member mutation against it.
	assert.False(t, fixture.pool.ReserveInFlight(failureOperationMemberAdd, "room-1", "user-a3"))
}

func TestSoakRoomLanes_SpendCreateBudgetMakesTheCapPerRun(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, []byte(`{"status":"accepted"}`), nil)

	fixture.lanes.SpendCreateBudget(fixture.lanes.cfg.RoomCreateBudget)

	require.NoError(t, fixture.lanes.RoomCreate(context.Background()))
	assert.Empty(t, fixture.ledger.ActiveOperations(),
		"a run that already spent its budget must not create more rooms after a restart")
}

// A responder that answers with a status this run does not recognise has still
// answered, and may well have applied the mutation. Scoring that as an explicit
// rejection closes the operation, hands the candidate back as if nothing
// happened, and leaves any real effect unreconciled. Only a bounded error
// envelope is a rejection; anything else is ambiguous.
func TestSoakRoomLanes_UnrecognisedReplyStaysAmbiguous(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t, []byte(`{"status":"something-new"}`), nil)

	require.NoError(t, fixture.lanes.MemberMutation(context.Background()))

	operation := soakSingleActiveOperation(t, fixture.ledger)
	assert.Equal(t, failureObservationUnverified,
		operation.Observations[failureObserverAdmission],
		"an unreadable verdict is not proof the mutation was refused")
	assert.Equal(t, uint64(0), fixture.ledger.Snapshot().Results[failureResultBad])
}

// Ownership is what teardown navigates by, and the ledger has nothing left to
// retry once it closes the operation. A failed append before the deadline must
// therefore keep the operation open rather than finalize an unclaimable room.
func TestSoakRoomLanes_RoomCreateHoldsTheOperationUntilOwnershipLands(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t,
		[]byte(`{"status":"accepted","roomId":"room-new","roomType":"channel"}`), nil)
	require.NoError(t, fixture.lanes.RoomCreate(context.Background()))
	fixture.store.byNameOK = true
	fixture.store.byName = "room-new"
	fixture.store.appendErr = errors.New("primary unavailable")
	fixture.advance(2 * time.Second)

	reconciled, err := fixture.lanes.Reconcile(context.Background(), fixture.verifier)

	require.NoError(t, err)
	assert.True(t, reconciled)
	assert.Len(t, fixture.ledger.ActiveOperations(), 1,
		"a room that could not be claimed is not finished")
	assert.Equal(t, uint64(0), fixture.ledger.Snapshot().Results[failureResultGood])

	fixture.store.appendErr = nil
	fixture.advance(2 * time.Second)
	reconciled, err = fixture.lanes.Reconcile(context.Background(), fixture.verifier)

	require.NoError(t, err)
	assert.True(t, reconciled)
	assert.Equal(t, []string{"room-new"}, fixture.store.appended)
	assert.Equal(t, uint64(1), fixture.ledger.Snapshot().Results[failureResultGood])
}

func TestSoakRoomLanes_RoomCreateRetriesOwnershipFailuresAtTheFlatInterval(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t,
		[]byte(`{"status":"accepted","roomId":"room-new","roomType":"channel"}`), nil)
	require.NoError(t, fixture.lanes.RoomCreate(context.Background()))
	fixture.store.byNameOK = true
	fixture.store.byName = "room-new"
	fixture.store.appendErr = errors.New("primary unavailable")
	fixture.store.appendHook = func() { fixture.advance(5 * time.Second) }
	fixture.advance(2 * time.Second)

	reconciled, err := fixture.lanes.Reconcile(context.Background(), fixture.verifier)
	require.NoError(t, err)
	assert.True(t, reconciled)
	first := soakSingleActiveOperation(t, fixture.ledger)
	assert.Equal(t, fixture.now.Add(fixture.lanes.cfg.RetryInterval), first.nextVerifyAt)

	fixture.advance(fixture.lanes.cfg.RetryInterval)
	reconciled, err = fixture.lanes.Reconcile(context.Background(), fixture.verifier)
	require.NoError(t, err)
	assert.True(t, reconciled)
	second := soakSingleActiveOperation(t, fixture.ledger)
	assert.Equal(t, fixture.now.Add(fixture.lanes.cfg.RetryInterval), second.nextVerifyAt,
		"a failed ownership call must not inherit pending-effect backoff")
}

func TestSoakRoomLanes_RoomCreateBoundsOwnershipPersistence(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t,
		[]byte(`{"status":"accepted","roomId":"room-new","roomType":"channel"}`), nil)
	require.NoError(t, fixture.lanes.RoomCreate(context.Background()))
	fixture.store.byNameOK = true
	fixture.store.byName = "room-new"
	fixture.store.appendContextErr = func(ctx context.Context) error {
		deadline, ok := ctx.Deadline()
		require.True(t, ok, "ownership persistence must have a bounded context")
		assert.WithinDuration(
			t, time.Now().Add(soakRoomStateTimeout), deadline, time.Second,
		)
		return errors.New("primary unavailable")
	}
	fixture.advance(2 * time.Second)

	reconciled, err := fixture.lanes.Reconcile(context.Background(), fixture.verifier)

	require.NoError(t, err)
	assert.True(t, reconciled)
	assert.Len(t, fixture.ledger.ActiveOperations(), 1)
}

func TestSoakRoomLanes_RoomCreateCapsOwnershipRetryAtDeadline(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t,
		[]byte(`{"status":"accepted","roomId":"room-new","roomType":"channel"}`), nil)
	require.NoError(t, fixture.lanes.RoomCreate(context.Background()))
	operation := soakSingleActiveOperation(t, fixture.ledger)
	fixture.store.byNameOK = true
	fixture.store.byName = "room-new"
	fixture.store.appendErr = errors.New("primary unavailable")
	fixture.advance(operation.Deadline.Sub(fixture.now) - 500*time.Millisecond)

	reconciled, err := fixture.lanes.Reconcile(context.Background(), fixture.verifier)

	require.NoError(t, err)
	assert.True(t, reconciled)
	retry := soakSingleActiveOperation(t, fixture.ledger)
	assert.Equal(t, operation.Deadline, retry.nextVerifyAt,
		"the terminal ownership attempt must remain claimable at the deadline")
}

func TestSoakRoomLanes_ExpiryLeavesTerminalOwnershipAttemptToTheLane(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t,
		[]byte(`{"status":"accepted","roomId":"room-new","roomType":"channel"}`), nil)
	require.NoError(t, fixture.lanes.RoomCreate(context.Background()))
	operation := soakSingleActiveOperation(t, fixture.ledger)
	fixture.store.byNameOK = true
	fixture.store.byName = "room-new"
	fixture.store.appendErr = errors.New("primary unavailable")
	fixture.advance(operation.Deadline.Sub(fixture.now) - 500*time.Millisecond)

	reconciled, err := fixture.lanes.Reconcile(context.Background(), fixture.verifier)
	require.NoError(t, err)
	require.True(t, reconciled)
	retry := soakSingleActiveOperation(t, fixture.ledger)
	require.Equal(t, operation.Deadline, retry.nextVerifyAt)

	fixture.advance(500 * time.Millisecond)
	expiredIDs, err := fixture.ledger.Expire(fixture.now)
	require.NoError(t, err)
	assert.Empty(t, expiredIDs,
		"expiry grace must leave a deadline probe to the reconciliation lane")

	reconciled, err = fixture.lanes.Reconcile(context.Background(), fixture.verifier)
	require.NoError(t, err)
	assert.True(t, reconciled)
	assert.Empty(t, fixture.ledger.ActiveOperations())
	assert.Equal(t, float64(1), testutil.ToFloat64(
		fixture.metrics.FailureUntracked.WithLabelValues("ownership")),
		"a terminal ownership failure must remain visible to teardown operators")
	assert.Equal(t, uint64(1), fixture.ledger.Snapshot().Results[failureResultGood])
}

// Reads against a created room are issued as the account that created it. Any
// other account is not a member, so the room_read lane would measure
// authorization failures instead of the reads it is there to exercise.
func TestSoakRoomLanes_RoomCreateRegistersTheRequesterAsReader(t *testing.T) {
	fixture := newSoakRoomLaneFixture(t,
		[]byte(`{"status":"accepted","roomId":"room-new","roomType":"channel"}`), nil)
	require.NoError(t, fixture.lanes.RoomCreate(context.Background()))
	operation := soakSingleActiveOperation(t, fixture.ledger)
	requester := operation.Attributes[soakFailureAttributeRequester]
	require.NotEmpty(t, requester)
	fixture.store.byNameOK = true
	fixture.store.byName = "room-new"
	fixture.advance(2 * time.Second)

	_, err := fixture.lanes.Reconcile(context.Background(), fixture.verifier)
	require.NoError(t, err)

	account, ok := fixture.lanes.reader.Account("room-new")
	require.True(t, ok, "a created room joins the read mix")
	assert.Equal(t, requester, account)
}
