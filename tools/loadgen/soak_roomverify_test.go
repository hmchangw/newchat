package main

import (
	"context"
	"errors"
	"math/rand"
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
	appended   []string
	appendErr  error
}

func (s *soakRoomStateStoreStub) RoomName(context.Context, string) (string, bool, error) {
	return s.name, s.nameFound, s.nameErr
}

func (s *soakRoomStateStoreStub) IsRoomMember(context.Context, string, string) (bool, error) {
	return s.member, s.memberErr
}

func (s *soakRoomStateStoreStub) SubscriptionMuted(
	context.Context, string, string,
) (bool, bool, error) {
	return s.muted, s.mutedFound, s.mutedErr
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
	return newSoakRoomStateVerifier(reader, store, metrics, health, nil), metrics
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
	operation := &failureOperation{
		ID: "operation-4", OperationType: failureOperationRoomCreate,
		Targets: map[string]string{"roomId": "room-created"},
	}

	transport := &soakRoomOpsTransport{reply: []byte(`{"rooms":[]}`)}
	verifier, _ := newSoakRoomVerifyFixture(t, transport,
		&soakRoomStateStoreStub{name: "soak-room", nameFound: true})
	result, reason, err := verifier.Verify(context.Background(), operation)
	require.NoError(t, err)
	assert.Equal(t, soakRoomStateMatched, result)
	assert.Equal(t, failureReasonNone, reason)

	verifier, _ = newSoakRoomVerifyFixture(t, transport, &soakRoomStateStoreStub{nameFound: false})
	result, reason, err = verifier.Verify(context.Background(), operation)
	require.NoError(t, err)
	assert.Equal(t, soakRoomStateAbsent, result)
	assert.Equal(t, failureReasonRoomStateMissing, reason)
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
	verifier := newSoakRoomStateVerifier(nil, nil, nil, nil, nil)

	_, _, err := verifier.Verify(context.Background(), soakMemberOperation(true))

	require.Error(t, err)
}
