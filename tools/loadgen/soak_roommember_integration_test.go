//go:build integration

package main

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
)

// soakRoomMemberEvidenceHarness drives the member lane against a real NATS
// connection, a real WAL on disk, and a real MongoDB, so the paths that only
// exist outside unit tests — transport, fsync, replay, primary reads — are the
// ones under test.
type soakRoomMemberEvidenceHarness struct {
	t         *testing.T
	natsURL   string
	ledgerDir string
	store     *mongoSoakStore
	metrics   *Metrics
	now       time.Time
	pool      *soakRoomStatePool
}

func newSoakRoomMemberEvidenceHarness(t *testing.T) *soakRoomMemberEvidenceHarness {
	t.Helper()
	harness := &soakRoomMemberEvidenceHarness{
		t: t, natsURL: testutil.NATS(t), ledgerDir: t.TempDir(),
		store:   &mongoSoakStore{db: testutil.MongoDB(t, "loadgen_soak_roommember")},
		metrics: NewMetrics(), now: time.Now().UTC(),
	}
	t.Cleanup(harness.metrics.stopNATSHealth)

	responder, err := nats.Connect(harness.natsURL)
	require.NoError(t, err)
	t.Cleanup(responder.Close)
	subscription, err := responder.Subscribe(
		subject.MemberAddPattern("site-a"),
		func(msg *nats.Msg) {
			_ = msg.Respond([]byte(`{"status":"accepted"}`))
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = subscription.Unsubscribe() })
	require.NoError(t, responder.Flush())

	pool, err := newSoakRoomStatePool(
		soakRoomStateTestTopology(3), 8, harness.metrics, rand.New(rand.NewSource(1)),
	)
	require.NoError(t, err)
	harness.pool = pool
	return harness
}

func (h *soakRoomMemberEvidenceHarness) openLedger(t *testing.T, epoch string) *failureLedger {
	t.Helper()
	wal, err := openFailureWAL(failureWALPath(h.ledgerDir, "run-1", epoch))
	require.NoError(t, err)
	contract := newFailureObserverContract(false)
	ledger, err := newFailureLedger(failureLedgerConfig{
		Capacity: 8, Journal: wal, Now: func() time.Time { return h.now },
		ObserverContract: &contract,
	})
	require.NoError(t, err)
	return ledger
}

func (h *soakRoomMemberEvidenceHarness) lanes(
	t *testing.T,
	ledger *failureLedger,
) (*soakRoomLanes, *soakRoomStateVerifier) {
	t.Helper()
	connection, err := nats.Connect(h.natsURL)
	require.NoError(t, err)
	t.Cleanup(connection.Close)
	now := func() time.Time { return h.now }

	rpc := newSoakRPCClient(
		newNATSHistoryRequester(connection), soakRetryConfig{MaxAttempts: 1}, nil, nil,
	)
	reader := newSoakRoomReader(
		soakRoomReadConfig{SiteID: "site-a", BatchSize: 4, RequestTimeout: time.Second},
		h.pool, rpc, &soakRoomReadRecorder{}, rand.New(rand.NewSource(2)), now,
	)
	verifier := newSoakRoomStateVerifier(
		reader, h.store, h.metrics,
		newFailureObserverHealth(failureObserverRoomState, h.now), now,
	)
	lanes := newSoakRoomLanes(
		soakRoomLaneConfig{
			RunID: "run-1", PersistGrace: time.Millisecond, Deadline: time.Minute,
			RetryInterval: time.Millisecond, RoomCreateBudget: 1, CreateRoomSize: 2,
		},
		h.pool, newSoakRoomMutator("site-a", rpc, time.Second, now), ledger,
		reader, h.store, h.metrics, nil, now,
	)
	return lanes, verifier
}

func TestFailureObservation_RoomMemberEvidenceSurvivesRestart(t *testing.T) {
	harness := newSoakRoomMemberEvidenceHarness(t)
	ctx := context.Background()

	ledger := harness.openLedger(t, "v1")
	lanes, _ := harness.lanes(t, ledger)
	require.NoError(t, lanes.MemberMutation(ctx))

	operations := ledger.ActiveOperations()
	require.Len(t, operations, 1)
	operation := operations[0]
	assert.Equal(t, soakFailureLaneMemberMutation, operation.Lane)
	assert.Equal(t, failureObservationGood, operation.Observations[failureObserverAdmission],
		"the real room-service reply must be recorded as an accepted admission")

	// Replace the process without resolving the operation.
	require.NoError(t, ledger.Close())

	account := operation.Attributes[soakFailureAttributeTargetAccount]
	roomID := operation.Targets["roomId"]
	_, err := harness.store.db.Collection("room_members").InsertOne(ctx, bson.M{
		"_id": "rm-1", "rid": roomID,
		"member": bson.M{"type": "individual", "id": "u9", "account": account},
	})
	require.NoError(t, err)

	recovered := harness.openLedger(t, "v1")
	t.Cleanup(func() { require.NoError(t, recovered.Close()) })
	assert.Equal(t, 1, recovered.Snapshot().Recovered,
		"an unresolved room mutation must be recoverable from the WAL after pod replacement")

	recoveredLanes, verifier := harness.lanes(t, recovered)
	harness.now = harness.now.Add(time.Second)

	reconciled, err := recoveredLanes.Reconcile(ctx, verifier)
	require.NoError(t, err)
	assert.True(t, reconciled)
	assert.Equal(t, uint64(1), recovered.Snapshot().Results[failureResultGood])
}

func TestFailureObservation_RoomMemberMissingStateAfterDeadline(t *testing.T) {
	harness := newSoakRoomMemberEvidenceHarness(t)
	ctx := context.Background()

	ledger := harness.openLedger(t, "v1")
	t.Cleanup(func() { require.NoError(t, ledger.Close()) })
	lanes, verifier := harness.lanes(t, ledger)
	require.NoError(t, lanes.MemberMutation(ctx))

	// Nothing is written to MongoDB, so the accepted membership never appears.
	harness.now = harness.now.Add(2 * time.Minute)

	reconciled, err := lanes.Reconcile(ctx, verifier)

	require.NoError(t, err)
	assert.True(t, reconciled)
	assert.Equal(t, uint64(1),
		ledger.Snapshot().Results[failureResultMissingAfterDeadline],
		"room-service accepted the add and the member never appeared, which is real loss")
}

func TestFailureObservation_RoomMemberEpochStartsAFreshJournal(t *testing.T) {
	harness := newSoakRoomMemberEvidenceHarness(t)
	ctx := context.Background()

	ledger := harness.openLedger(t, "v1")
	lanes, _ := harness.lanes(t, ledger)
	require.NoError(t, lanes.MemberMutation(ctx))
	require.NoError(t, ledger.Close())

	upgraded := harness.openLedger(t, "v2")
	t.Cleanup(func() { require.NoError(t, upgraded.Close()) })

	assert.Equal(t, 0, upgraded.Snapshot().Recovered,
		"a new epoch starts a fresh journal instead of replaying an incompatible one")
	assert.FileExists(t, failureWALPath(harness.ledgerDir, "run-1", "v1"),
		"the earlier journal stays on disk as evidence")
}
