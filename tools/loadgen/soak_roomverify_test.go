package main

import (
	"context"
	"errors"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type soakRoomStateStoreStub struct {
	name       string
	nameFound  bool
	nameErr    error
	member     bool
	memberErr  error
	muted      bool
	mutedFound bool
	mutedErr   error
	lastSeen   time.Time
	seenFound  bool
	seenErr    error
	appended   []string
	appendErr  error
	byName     string
	byNameOK   bool
	byNameErr  error
	ownedRooms int
	ownedErr   error
}

func (s *soakRoomStateStoreStub) RoomName(context.Context, string) (string, bool, error) {
	return s.name, s.nameFound, s.nameErr
}

func (s *soakRoomStateStoreStub) CountCreatedRooms(context.Context, string) (int, error) {
	return s.ownedRooms, s.ownedErr
}

func (s *soakRoomStateStoreStub) RoomIDByName(
	context.Context,
	string,
	string,
) (string, bool, error) {
	return s.byName, s.byNameOK, s.byNameErr
}

func (s *soakRoomStateStoreStub) IsRoomMember(context.Context, string, string) (bool, error) {
	return s.member, s.memberErr
}

func (s *soakRoomStateStoreStub) SubscriptionMuted(
	context.Context, string, string,
) (bool, bool, error) {
	return s.muted, s.mutedFound, s.mutedErr
}

func (s *soakRoomStateStoreStub) SubscriptionLastSeen(
	context.Context, string, string,
) (time.Time, bool, error) {
	return s.lastSeen, s.seenFound, s.seenErr
}

func (s *soakRoomStateStoreStub) AppendOwnedRooms(
	_ context.Context, _ string, roomIDs []string,
) error {
	s.appended = append(s.appended, roomIDs...)
	return s.appendErr
}

func newSoakRoomVerifyFixture(
	t *testing.T,
	transport soakRPCTransport,
	store soakRoomStateStore,
) (*soakRoomStateVerifier, *Metrics) {
	t.Helper()
	pool, _ := newSoakRoomStateTestPool(t, 3, 8)
	metrics := NewMetrics()
	reader := newSoakRoomReader(
		soakRoomReadConfig{SiteID: "site-a", BatchSize: 4},
		pool,
		newSoakRPCClient(transport, soakRetryConfig{MaxAttempts: 1}, &soakRecordingSleeper{}, nil),
		&soakRoomReadRecorder{},
		rand.New(rand.NewSource(1)),
		nil,
	)
	health := newFailureObserverHealth(failureObserverRoomState, time.Now())
	return newSoakRoomStateVerifier(reader, store, "site-a", metrics, health, nil), metrics
}

func soakMemberOperation(add bool) *failureOperation {
	expected := "false"
	if add {
		expected = "true"
	}
	operationType := failureOperationMemberRemove
	if add {
		operationType = failureOperationMemberAdd
	}
	return &failureOperation{
		ID: "operation-1", OperationType: operationType,
		Targets: map[string]string{"roomId": "room-1", "account": "user-b0"},
		Attributes: map[string]string{
			soakFailureAttributeTargetAccount:  "user-b0",
			soakFailureAttributeExpectedMember: expected,
			soakFailureAttributeRequester:      "user-a0",
		},
	}
}

func TestSoakRoomStateVerifier_MemberAddMatchesWhenBothSourcesAgree(t *testing.T) {
	transport := &soakRoomOpsTransport{
		reply: []byte(`{"members":[{"id":"m1","rid":"room-1","member":{"account":"user-b0"}}]}`),
	}
	verifier, metrics := newSoakRoomVerifyFixture(t, transport, &soakRoomStateStoreStub{member: true})

	result, reason, err := verifier.Verify(context.Background(), soakMemberOperation(true))

	require.NoError(t, err)
	assert.Equal(t, soakRoomStateMatched, result)
	assert.Equal(t, failureReasonNone, reason)
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.SoakRoomStateSources.WithLabelValues(soakRoomStateSourceRPC, "matched")))
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.SoakRoomStateSources.WithLabelValues(soakRoomStateSourceStore, "matched")))
}

func TestSoakRoomStateVerifier_MemberAddIsAbsentWhenBothSourcesAgree(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"members":[]}`)}
	verifier, _ := newSoakRoomVerifyFixture(t, transport, &soakRoomStateStoreStub{member: false})

	result, reason, err := verifier.Verify(context.Background(), soakMemberOperation(true))

	require.NoError(t, err)
	assert.Equal(t, soakRoomStateAbsent, result)
	assert.Equal(t, failureReasonRoomStateMissing, reason)
}

func TestSoakRoomStateVerifier_AuthoritativeSourceOverridesTheRPCPath(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"members":[]}`)}
	verifier, _ := newSoakRoomVerifyFixture(t, transport, &soakRoomStateStoreStub{member: true})

	result, _, err := verifier.Verify(context.Background(), soakMemberOperation(true))

	require.NoError(t, err)
	assert.Equal(t, soakRoomStateMatched, result,
		"a primary read that finds the member proves the write landed")
}

func TestSoakRoomStateVerifier_RPCSuccessCarriesAnUnreachableStore(t *testing.T) {
	transport := &soakRoomOpsTransport{
		reply: []byte(`{"members":[{"id":"m1","rid":"room-1","member":{"account":"user-b0"}}]}`),
	}
	verifier, metrics := newSoakRoomVerifyFixture(t, transport,
		&soakRoomStateStoreStub{memberErr: errors.New("primary unavailable")})

	result, _, err := verifier.Verify(context.Background(), soakMemberOperation(true))

	require.NoError(t, err)
	assert.Equal(t, soakRoomStateMatched, result)
	assert.Equal(t, float64(0), testutil.ToFloat64(
		metrics.FailureObserverUp.WithLabelValues(string(failureObserverRoomState))))
}

func TestSoakRoomStateVerifier_BothSourcesDownIsUnknown(t *testing.T) {
	transport := &soakRoomOpsTransport{err: errors.New("no responders")}
	verifier, _ := newSoakRoomVerifyFixture(t, transport,
		&soakRoomStateStoreStub{memberErr: errors.New("primary unavailable")})

	result, reason, err := verifier.Verify(context.Background(), soakMemberOperation(true))

	require.NoError(t, err)
	assert.Equal(t, soakRoomStateUnknown, result,
		"an unanswerable observer must never claim absence")
	assert.Equal(t, failureReasonNone, reason)
}

func TestSoakRoomStateVerifier_MemberRemoveExpectsAbsence(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"members":[]}`)}
	verifier, _ := newSoakRoomVerifyFixture(t, transport, &soakRoomStateStoreStub{member: false})

	result, _, err := verifier.Verify(context.Background(), soakMemberOperation(false))

	require.NoError(t, err)
	assert.Equal(t, soakRoomStateMatched, result)
}

func TestSoakRoomStateVerifier_MemberRemoveIsAbsentWhileStillPresent(t *testing.T) {
	transport := &soakRoomOpsTransport{
		reply: []byte(`{"members":[{"id":"m1","rid":"room-1","member":{"account":"user-b0"}}]}`),
	}
	verifier, _ := newSoakRoomVerifyFixture(t, transport, &soakRoomStateStoreStub{member: true})

	result, reason, err := verifier.Verify(context.Background(), soakMemberOperation(false))

	require.NoError(t, err)
	assert.Equal(t, soakRoomStateAbsent, result)
	assert.Equal(t, failureReasonRoomStateMissing, reason)
}

func soakRenameOperation() *failureOperation {
	return &failureOperation{
		ID: "operation-2", OperationType: failureOperationRoomRename,
		Targets: map[string]string{"roomId": "room-1"},
		Attributes: map[string]string{
			soakFailureAttributeExpectedName: "soak-channel-r2",
			soakFailureAttributePreviousName: "soak-channel-r1",
			soakFailureAttributeRequester:    "user-a0",
		},
	}
}

func TestSoakRoomStateVerifier_RenameVerdicts(t *testing.T) {
	for _, testCase := range []struct {
		name       string
		storeName  string
		storeFound bool
		want       soakRoomStateResult
		wantReason failureReason
	}{
		{
			name: "applied", storeName: "soak-channel-r2", storeFound: true,
			want: soakRoomStateMatched, wantReason: failureReasonNone,
		},
		{
			name: "not applied", storeName: "soak-channel-r1", storeFound: true,
			want: soakRoomStateAbsent, wantReason: failureReasonRoomStateMissing,
		},
		{
			name: "impossible name", storeName: "someone-else", storeFound: true,
			want: soakRoomStateMismatch, wantReason: failureReasonRoomNameMismatch,
		},
		{
			name: "room vanished", storeFound: false,
			want: soakRoomStateMismatch, wantReason: failureReasonRoomNameMismatch,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			transport := &soakRoomOpsTransport{reply: []byte(`{"rooms":[]}`)}
			verifier, _ := newSoakRoomVerifyFixture(t, transport, &soakRoomStateStoreStub{
				name: testCase.storeName, nameFound: testCase.storeFound,
			})

			result, reason, err := verifier.Verify(context.Background(), soakRenameOperation())

			require.NoError(t, err)
			assert.Equal(t, testCase.want, result)
			assert.Equal(t, testCase.wantReason, reason)
		})
	}
}

func soakMuteOperation(expectMuted bool) *failureOperation {
	expected := "false"
	if expectMuted {
		expected = "true"
	}
	return &failureOperation{
		ID: "operation-3", OperationType: failureOperationMuteToggle,
		Targets: map[string]string{"roomId": "room-1", "account": "user-b0"},
		Attributes: map[string]string{
			soakFailureAttributeTargetAccount: "user-b0",
			soakFailureAttributeExpectedMuted: expected,
		},
	}
}

func TestSoakRoomStateVerifier_MuteVerdicts(t *testing.T) {
	for _, testCase := range []struct {
		name       string
		storeMuted bool
		storeFound bool
		want       soakRoomStateResult
		wantReason failureReason
	}{
		{
			name: "toggle applied", storeMuted: true, storeFound: true,
			want: soakRoomStateMatched, wantReason: failureReasonNone,
		},
		{
			name: "toggle not applied", storeMuted: false, storeFound: true,
			want: soakRoomStateAbsent, wantReason: failureReasonRoomStateMissing,
		},
		{
			name: "subscription vanished", storeFound: false,
			want: soakRoomStateMismatch, wantReason: failureReasonMuteStateMismatch,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			transport := &soakRoomOpsTransport{reply: []byte(`{"subscriptions":[]}`)}
			verifier, _ := newSoakRoomVerifyFixture(t, transport, &soakRoomStateStoreStub{
				muted: testCase.storeMuted, mutedFound: testCase.storeFound,
			})

			result, reason, err := verifier.Verify(context.Background(), soakMuteOperation(true))

			require.NoError(t, err)
			assert.Equal(t, testCase.want, result)
			assert.Equal(t, testCase.wantReason, reason)
		})
	}
}

func TestSoakRoomStateVerifier_MuteReadsTheRPCSubscriptionList(t *testing.T) {
	transport := &soakRoomOpsTransport{
		reply: []byte(`{"subscriptions":[{"roomId":"room-1","muted":true}]}`),
	}
	verifier, metrics := newSoakRoomVerifyFixture(t, transport,
		&soakRoomStateStoreStub{mutedErr: errors.New("primary unavailable")})

	result, _, err := verifier.Verify(context.Background(), soakMuteOperation(true))

	require.NoError(t, err)
	assert.Equal(t, soakRoomStateMatched, result)
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.SoakRoomStateSources.WithLabelValues(soakRoomStateSourceRPC, "matched")))
}

func TestSoakRoomStateVerifier_RoomCreateVerdicts(t *testing.T) {
	// The room is identified by the name journaled before the request; its ID
	// does not exist until the server answers, so a lost reply leaves the name
	// as the only handle.
	operation := &failureOperation{
		ID: "operation-4", OperationType: failureOperationRoomCreate,
		Targets: map[string]string{"roomName": "soak-run-1-created-abc"},
	}

	transport := &soakRoomOpsTransport{reply: []byte(`{"rooms":[]}`)}
	verifier, _ := newSoakRoomVerifyFixture(t, transport,
		&soakRoomStateStoreStub{byName: "room-created", byNameOK: true})
	result, reason, err := verifier.Verify(context.Background(), operation)
	require.NoError(t, err)
	assert.Equal(t, soakRoomStateMatched, result)
	assert.Equal(t, failureReasonNone, reason)

	verifier, _ = newSoakRoomVerifyFixture(t, transport, &soakRoomStateStoreStub{byNameOK: false})
	result, reason, err = verifier.Verify(context.Background(), operation)
	require.NoError(t, err)
	assert.Equal(t, soakRoomStateAbsent, result)
	assert.Equal(t, failureReasonRoomStateMissing, reason)
}

// A create operation without its name has nothing to look up, so it must error
// rather than silently report the room absent.
func TestSoakRoomStateVerifier_RoomCreateRequiresTheRoomName(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"rooms":[]}`)}
	verifier, _ := newSoakRoomVerifyFixture(t, transport, &soakRoomStateStoreStub{})

	_, _, err := verifier.Verify(context.Background(), &failureOperation{
		ID: "operation-5", OperationType: failureOperationRoomCreate,
	})

	require.Error(t, err)
}

func TestSoakRoomStateVerifier_RejectsIncompleteOperations(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"members":[]}`)}
	verifier, _ := newSoakRoomVerifyFixture(t, transport, &soakRoomStateStoreStub{})

	for _, testCase := range []struct {
		name      string
		operation *failureOperation
	}{
		{name: "missing operation"},
		{
			name:      "missing room target",
			operation: &failureOperation{ID: "o", OperationType: failureOperationMemberAdd},
		},
		{
			name: "missing account",
			operation: &failureOperation{
				ID: "o", OperationType: failureOperationMemberAdd,
				Targets: map[string]string{"roomId": "room-1"},
			},
		},
		{
			name: "missing expected name",
			operation: &failureOperation{
				ID: "o", OperationType: failureOperationRoomRename,
				Targets: map[string]string{"roomId": "room-1"},
			},
		},
		{
			name: "missing mute account",
			operation: &failureOperation{
				ID: "o", OperationType: failureOperationMuteToggle,
				Targets: map[string]string{"roomId": "room-1"},
			},
		},
		{
			name: "unsupported type",
			operation: &failureOperation{
				ID: "o", OperationType: failureOperationMessageCreate,
				Targets: map[string]string{"roomId": "room-1"},
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			result, _, err := verifier.Verify(context.Background(), testCase.operation)

			require.Error(t, err)
			assert.Equal(t, soakRoomStateUnknown, result)
		})
	}
}

func TestSoakRoomStateVerifier_RejectsMissingStore(t *testing.T) {
	verifier := newSoakRoomStateVerifier(nil, nil, "site-a", nil, nil, nil)

	_, _, err := verifier.Verify(context.Background(), soakMemberOperation(true))

	require.Error(t, err)
}

func TestSoakRoomStateVerifier_FallsBackToTheOperationRequester(t *testing.T) {
	transport := &soakRoomOpsTransport{
		reply: []byte(`{"members":[{"id":"m1","rid":"room-unknown","member":{"account":"user-b0"}}]}`),
	}
	verifier, metrics := newSoakRoomVerifyFixture(t, transport, &soakRoomStateStoreStub{member: true})
	operation := soakMemberOperation(true)
	operation.Targets["roomId"] = "room-unknown"

	result, _, err := verifier.Verify(context.Background(), operation)

	require.NoError(t, err)
	assert.Equal(t, soakRoomStateMatched, result)
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.SoakRoomStateSources.WithLabelValues(soakRoomStateSourceRPC, "matched")),
		"a room outside the pool is still readable through the operation's requester")
}

func TestSoakRoomStateVerifier_SkipsTheRPCSourceWithoutAnAccount(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"members":[]}`)}
	verifier, metrics := newSoakRoomVerifyFixture(t, transport, &soakRoomStateStoreStub{member: true})
	operation := soakMemberOperation(true)
	operation.Targets["roomId"] = "room-unknown"
	delete(operation.Attributes, soakFailureAttributeRequester)

	result, _, err := verifier.Verify(context.Background(), operation)

	require.NoError(t, err)
	assert.Equal(t, soakRoomStateMatched, result)
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.SoakRoomStateSources.WithLabelValues(soakRoomStateSourceRPC, "unknown")))
}

func TestSoakRoomStateVerifier_WorksWithoutMetricsOrHealth(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"members":[]}`)}
	pool, _ := newSoakRoomStateTestPool(t, 3, 8)
	reader := newSoakRoomReader(
		soakRoomReadConfig{SiteID: "site-a"}, pool,
		newSoakRPCClient(transport, soakRetryConfig{MaxAttempts: 1}, &soakRecordingSleeper{}, nil),
		&soakRoomReadRecorder{}, rand.New(rand.NewSource(20)), nil,
	)
	verifier := newSoakRoomStateVerifier(reader, &soakRoomStateStoreStub{}, "site-a", nil, nil, nil)

	assert.NotPanics(t, func() {
		_, _, err := verifier.Verify(context.Background(), soakMemberOperation(false))
		require.NoError(t, err)
	})
}

func TestFlipSoakRoomStatePresence_LeavesNonPresenceVerdicts(t *testing.T) {
	assert.Equal(t, soakRoomStateAbsent, flipSoakRoomStatePresence(soakRoomStateMatched))
	assert.Equal(t, soakRoomStateMatched, flipSoakRoomStatePresence(soakRoomStateAbsent))
	assert.Equal(t, soakRoomStateUnknown, flipSoakRoomStatePresence(soakRoomStateUnknown))
	assert.Equal(t, soakRoomStateMismatch, flipSoakRoomStatePresence(soakRoomStateMismatch))
}

func soakReadOperation(baseline time.Time, hasBaseline bool) *failureOperation {
	attributes := map[string]string{
		soakFailureAttributeTargetAccount: "user-b0",
		soakFailureAttributeRequester:     "user-b0",
	}
	if hasBaseline {
		attributes[soakFailureAttributeReadBaseline] =
			strconv.FormatInt(baseline.UTC().UnixMilli(), 10)
	}
	return &failureOperation{
		ID: "operation-5", OperationType: failureOperationMessageRead,
		Targets:    map[string]string{"roomId": "room-1", "account": "user-b0"},
		Attributes: attributes,
	}
}

func TestSoakRoomStateVerifier_ReadCursorVerdicts(t *testing.T) {
	baseline := time.Date(2026, 8, 16, 10, 0, 0, 0, time.UTC)
	for _, testCase := range []struct {
		name       string
		lastSeen   time.Time
		found      bool
		want       soakRoomStateResult
		wantReason failureReason
	}{
		{
			name: "cursor advanced", lastSeen: baseline.Add(time.Second), found: true,
			want: soakRoomStateMatched, wantReason: failureReasonNone,
		},
		{
			name: "cursor unchanged", lastSeen: baseline, found: true,
			want: soakRoomStateAbsent, wantReason: failureReasonRoomStateMissing,
		},
		{
			name: "cursor moved backwards", lastSeen: baseline.Add(-time.Minute), found: true,
			want: soakRoomStateMismatch, wantReason: failureReasonReadStateRegressed,
		},
		{
			name: "cursor never set", found: true,
			want: soakRoomStateAbsent, wantReason: failureReasonRoomStateMissing,
		},
		{
			name: "subscription vanished",
			want: soakRoomStateMismatch, wantReason: failureReasonReadStateRegressed,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			transport := &soakRoomOpsTransport{reply: []byte(`{"subscriptions":[]}`)}
			verifier, _ := newSoakRoomVerifyFixture(t, transport, &soakRoomStateStoreStub{
				lastSeen: testCase.lastSeen, seenFound: testCase.found,
			})

			result, reason, err := verifier.Verify(
				context.Background(), soakReadOperation(baseline, true),
			)

			require.NoError(t, err)
			assert.Equal(t, testCase.want, result)
			assert.Equal(t, testCase.wantReason, reason)
		})
	}
}

func TestSoakRoomStateVerifier_ReadCursorWithoutBaselineOnlyNeedsAValue(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"subscriptions":[]}`)}
	verifier, _ := newSoakRoomVerifyFixture(t, transport, &soakRoomStateStoreStub{
		lastSeen: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), seenFound: true,
	})

	result, _, err := verifier.Verify(
		context.Background(), soakReadOperation(time.Time{}, false),
	)

	require.NoError(t, err)
	assert.Equal(t, soakRoomStateMatched, result,
		"without a trusted baseline a stale cursor cannot be called a loss")
}

func TestSoakRoomStateVerifier_ReadCursorUsesTheRPCSubscriptionList(t *testing.T) {
	baseline := time.Date(2026, 8, 16, 10, 0, 0, 0, time.UTC)
	advanced := baseline.Add(time.Minute)
	transport := &soakRoomOpsTransport{
		reply: []byte(`{"subscriptions":[{"roomId":"room-1","lastSeenAt":"` +
			advanced.Format(time.RFC3339Nano) + `"}]}`),
	}
	verifier, metrics := newSoakRoomVerifyFixture(t, transport, &soakRoomStateStoreStub{
		seenErr: errors.New("primary unavailable"),
	})

	result, _, err := verifier.Verify(context.Background(), soakReadOperation(baseline, true))

	require.NoError(t, err)
	assert.Equal(t, soakRoomStateMatched, result)
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.SoakRoomStateSources.WithLabelValues(soakRoomStateSourceRPC, "matched")))
}

func TestSoakRoomStateVerifier_ReadCursorRejectsAnUnreadableBaseline(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"subscriptions":[]}`)}
	verifier, _ := newSoakRoomVerifyFixture(t, transport, &soakRoomStateStoreStub{})
	operation := soakReadOperation(time.Time{}, false)
	operation.Attributes[soakFailureAttributeReadBaseline] = "not-a-number"

	_, _, err := verifier.Verify(context.Background(), operation)

	require.Error(t, err)
}

func TestSoakRoomStateVerifier_ReadCursorRequiresAnAccount(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"subscriptions":[]}`)}
	verifier, _ := newSoakRoomVerifyFixture(t, transport, &soakRoomStateStoreStub{})
	operation := soakReadOperation(time.Time{}, false)
	delete(operation.Attributes, soakFailureAttributeTargetAccount)

	_, _, err := verifier.Verify(context.Background(), operation)

	require.Error(t, err)
}
