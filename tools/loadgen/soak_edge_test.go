package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
)

type configurableSoakLifecycleStore struct {
	manifest *soakManifest
	getErr   error
	putErr   error
	putCalls int
}

func (s *configurableSoakLifecycleStore) GetManifest(
	_ context.Context,
	_ string,
) (*soakManifest, error) {
	if s.getErr != nil {
		return nil, s.getErr
	}
	if s.manifest == nil {
		return nil, nil
	}
	cloned := *s.manifest
	return &cloned, nil
}

func (s *configurableSoakLifecycleStore) PutManifest(
	_ context.Context,
	manifest *soakManifest,
) error {
	s.putCalls++
	if s.putErr != nil {
		return s.putErr
	}
	cloned := *manifest
	s.manifest = &cloned
	return nil
}

func (s *configurableSoakLifecycleStore) TouchHeartbeat(
	_ context.Context,
	_ string,
	_ time.Time,
) error {
	return nil
}

type failingSoakSleeper struct {
	err error
}

func (s failingSoakSleeper) Sleep(context.Context, time.Duration) error {
	return s.err
}

func TestPrepareSoakRun_RejectsInvalidInputsAndManifest(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	tests := []struct {
		name     string
		store    soakLifecycleStore
		runID    string
		duration time.Duration
	}{
		{name: "nil store", runID: "run-1", duration: time.Hour},
		{
			name: "empty run ID", store: &configurableSoakLifecycleStore{},
			duration: time.Hour,
		},
		{
			name: "non-positive duration", store: &configurableSoakLifecycleStore{},
			runID: "run-1",
		},
		{
			name: "missing manifest", store: &configurableSoakLifecycleStore{},
			runID: "run-1", duration: time.Hour,
		},
		{
			name: "invalid manifest state",
			store: &configurableSoakLifecycleStore{manifest: &soakManifest{
				ID: "run-1", State: soakManifestCleaned,
			}},
			runID: "run-1", duration: time.Hour,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := prepareSoakRun(
				context.Background(),
				tt.store,
				tt.runID,
				tt.duration,
				now,
			)
			require.Error(t, err)
		})
	}
}

func TestSoakLifecycle_WrapsStoreFailures(t *testing.T) {
	wantErr := errors.New("mongo unavailable")
	now := time.Unix(100, 0).UTC()

	t.Run("prepare get", func(t *testing.T) {
		store := &configurableSoakLifecycleStore{getErr: wantErr}
		_, err := prepareSoakRun(
			context.Background(), store, "run-1", time.Hour, now,
		)
		assert.ErrorIs(t, err, wantErr)
	})

	t.Run("prepare put", func(t *testing.T) {
		store := &configurableSoakLifecycleStore{
			manifest: &soakManifest{ID: "run-1", State: soakManifestSeeded},
			putErr:   wantErr,
		}
		_, err := prepareSoakRun(
			context.Background(), store, "run-1", time.Hour, now,
		)
		assert.ErrorIs(t, err, wantErr)
	})

	t.Run("complete missing", func(t *testing.T) {
		err := completeSoakRun(
			context.Background(),
			&configurableSoakLifecycleStore{},
			"run-1",
			now,
		)
		require.Error(t, err)
	})

	t.Run("complete get", func(t *testing.T) {
		err := completeSoakRun(
			context.Background(),
			&configurableSoakLifecycleStore{getErr: wantErr},
			"run-1",
			now,
		)
		assert.ErrorIs(t, err, wantErr)
	})

	t.Run("complete put", func(t *testing.T) {
		err := completeSoakRun(
			context.Background(),
			&configurableSoakLifecycleStore{
				manifest: &soakManifest{ID: "run-1", State: soakManifestRunning},
				putErr:   wantErr,
			},
			"run-1",
			now,
		)
		assert.ErrorIs(t, err, wantErr)
	})
}

func TestNewSoakWorkload_AppliesSafeDefaults(t *testing.T) {
	workload := newSoakWorkload(nil, nil, &soakWorkloadActions{}, nil, nil, nil)

	assert.Equal(t, 256, workload.cfg.MaxInFlight)
	assert.NotNil(t, workload.dispatch)
	assert.NotNil(t, workload.now)
	assert.NotNil(t, workload.onSaturation)
	// A named set rather than a count: it says which lane went missing, and a
	// new lane has to be added here deliberately rather than by bumping a
	// number that carries no meaning.
	names := make([]string, 0, len(workload.lanes()))
	for _, lane := range workload.lanes() {
		names = append(names, lane.name)
	}
	assert.ElementsMatch(t, []string{
		"send", "read", "mutation", "reaction", "pinned_list", "verify",
		soakFailureLaneMemberMutation, soakFailureLaneRoomMutation,
		"room_read", "user_read", "search_read",
		soakFailureLaneRoomCreate, soakFailureLaneReadReceipt, "presence",
	}, names)
}

func TestSoakConstructors_DoNotMutateCallerConfig(t *testing.T) {
	workloadConfig := soakWorkloadConfig{}
	newSoakWorkload(
		&workloadConfig,
		nil,
		&soakWorkloadActions{},
		nil,
		nil,
		nil,
	)
	assert.Equal(t, soakWorkloadConfig{}, workloadConfig)

	mutationConfig := soakMutationConfig{}
	newSoakMutator(
		&mutationConfig,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	assert.Equal(t, soakMutationConfig{}, mutationConfig)

	verifyConfig := soakVerifyConfig{}
	newSoakVerifier(&verifyConfig, nil, nil, nil, nil)
	assert.Equal(t, soakVerifyConfig{}, verifyConfig)
}

func TestNewSoakRuntimeSelector_ValidatesInputs(t *testing.T) {
	cfg := validSoakConfig(t)
	tests := []struct {
		name     string
		topology *soakTopology
		cfg      *soakConfig
	}{
		{name: "nil topology", cfg: &cfg},
		{name: "empty topology", topology: &soakTopology{}, cfg: &cfg},
		{
			name:     "nil config",
			topology: &soakTopology{Rooms: []model.Room{{ID: "room-1"}}},
		},
		{
			name: "room without usable member",
			topology: &soakTopology{
				Rooms: []model.Room{{ID: "room-1"}},
				Subscriptions: []model.Subscription{{
					RoomID: "room-1",
					User:   model.SubscriptionUser{ID: "u-1"},
				}},
			},
			cfg: &cfg,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := newSoakRuntimeSelector(tt.topology, tt.cfg, 42)
			require.Error(t, err)
		})
	}
}

func TestNewSoakRuntimeSelector_SelectsOnlyValidMembers(t *testing.T) {
	cfg := validSoakConfig(t)
	selector, err := newSoakRuntimeSelector(&soakTopology{
		ActiveUsers: []model.User{{ID: "u-1", Account: "alice"}},
		Rooms:       []model.Room{{ID: "room-1"}},
		Subscriptions: []model.Subscription{
			{
				RoomID: "room-1",
				User:   model.SubscriptionUser{ID: "u-1", Account: "alice"},
			},
			{
				RoomID: "room-1",
				User:   model.SubscriptionUser{ID: "u-2", Account: "bob"},
			},
			{
				RoomID: "room-1", IsSubscribed: true,
				User: model.SubscriptionUser{ID: "", Account: "missing-id"},
			},
		},
	}, &cfg, 42)
	require.NoError(t, err)

	target, content := selector.nextSend()
	assert.Equal(t, "room-1", selector.nextRoom())
	assert.Equal(t, "alice", target.Account)
	assert.Equal(t, "room-1", target.RoomID)
	assert.NotEmpty(t, content)
}

func TestNewSoakRPCClient_InputFailures(t *testing.T) {
	client := newSoakRPCClient(
		&soakRPCFakeTransport{},
		soakRetryConfig{},
		nil,
		nil,
	)
	assert.Empty(t, classifySoakRPCError(nil))

	result, err := client.Call(context.Background(), soakRPCRequest{
		Action: soakRPCAction("invalid"),
	}, nil)
	require.Error(t, err)
	assert.Equal(t, soakErrorInternal, result.ErrorClass)

	result, err = client.Call(context.Background(), soakRPCRequest{
		Action: soakRPCGetMessage,
		Body:   make(chan int),
	}, nil)
	require.Error(t, err)
	assert.Equal(t, soakErrorRequestEncode, result.ErrorClass)
}

func TestSoakRPCClient_PropagatesResolverAndSleeperFailures(t *testing.T) {
	resolverErr := errors.New("state read failed")
	client := newSoakRPCClient(
		&soakRPCFakeTransport{replies: []soakRPCFakeReply{{err: nats.ErrTimeout}}},
		soakRetryConfig{MaxAttempts: 2},
		&soakRecordingSleeper{},
		nil,
	)
	result, err := client.Call(context.Background(), soakRPCRequest{
		Action: soakRPCReact, RetryMode: soakRetryAmbiguous,
		ResolveAmbiguity: func(context.Context) (bool, error) {
			return false, resolverErr
		},
	}, nil)
	assert.ErrorIs(t, err, resolverErr)
	assert.Equal(t, soakErrorInternal, result.ErrorClass)

	sleepErr := errors.New("sleep interrupted")
	client = newSoakRPCClient(
		&soakRPCFakeTransport{replies: []soakRPCFakeReply{{err: nats.ErrTimeout}}},
		soakRetryConfig{MaxAttempts: 2},
		failingSoakSleeper{err: sleepErr},
		nil,
	)
	_, err = client.Call(context.Background(), soakRPCRequest{
		Action: soakRPCGetMessage, RetryMode: soakRetrySafe,
	}, nil)
	assert.ErrorIs(t, err, sleepErr)
}

func TestSoakTimerSleeper_ObservesCancellationAndTimer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.ErrorIs(t, (soakTimerSleeper{}).Sleep(ctx, time.Hour), context.Canceled)
	require.NoError(t, (soakTimerSleeper{}).Sleep(context.Background(), 0))
}

func TestSoakMutator_SkipsUnavailableTargetsAndActors(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0))
	empty := newSoakCatalog(8, 100, 0, clock)
	mutator := newTestSoakMutator(
		empty,
		&soakRPCFakeTransport{},
		&soakMutationRecorder{},
		mutationTopology(),
		clock,
	)

	edit, err := mutator.Edit(context.Background(), "room-1", "edited")
	require.NoError(t, err)
	assert.True(t, edit.Skipped)
	deleted, err := mutator.Delete(context.Background(), "room-1")
	require.NoError(t, err)
	assert.True(t, deleted.Skipped)
	pinned, err := mutator.PinOrUnpin(context.Background(), "room-1")
	require.NoError(t, err)
	assert.True(t, pinned.Skipped)
	reaction, err := mutator.React(context.Background(), "room-1")
	require.NoError(t, err)
	assert.True(t, reaction.Skipped)

	catalog := acceptedMutationMessage(t, clock, "message-1", "alice")
	mutator = newTestSoakMutator(
		catalog,
		&soakRPCFakeTransport{},
		&soakMutationRecorder{},
		&soakTopology{},
		clock,
	)
	reaction, err = mutator.React(context.Background(), "room-1")
	require.NoError(t, err)
	assert.True(t, reaction.Skipped)
}

func TestSoakMutator_RejectsMismatchedMutationReplies(t *testing.T) {
	tests := []struct {
		name   string
		setup  func(*soakCatalog)
		invoke func(*soakMutator) (soakMutationOutcome, error)
	}{
		{
			name: "edit",
			invoke: func(mutator *soakMutator) (soakMutationOutcome, error) {
				return mutator.Edit(context.Background(), "room-1", "edited")
			},
		},
		{
			name: "delete",
			invoke: func(mutator *soakMutator) (soakMutationOutcome, error) {
				return mutator.Delete(context.Background(), "room-1")
			},
		},
		{
			name: "pin",
			invoke: func(mutator *soakMutator) (soakMutationOutcome, error) {
				return mutator.PinOrUnpin(context.Background(), "room-1")
			},
		},
		{
			name:  "unpin",
			setup: func(catalog *soakCatalog) { catalog.SetPinned("room-1", "message-1", true) },
			invoke: func(mutator *soakMutator) (soakMutationOutcome, error) {
				return mutator.PinOrUnpin(context.Background(), "room-1")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clock := newFakeSoakClock(time.Unix(100, 0))
			catalog := acceptedMutationMessage(t, clock, "message-1", "alice")
			if tt.setup != nil {
				tt.setup(catalog)
			}
			recorder := &soakMutationRecorder{}
			mutator := newTestSoakMutator(
				catalog,
				&soakRPCFakeTransport{replies: []soakRPCFakeReply{{
					data: []byte(`{"messageId":"different"}`),
				}}},
				recorder,
				mutationTopology(),
				clock,
			)

			_, err := tt.invoke(mutator)
			require.Error(t, err)
			assert.Equal(t, soakErrorAssertion, classifySoakRPCError(err))
			require.NotEmpty(t, recorder.snapshot())
			assert.Equal(t, soakErrorAssertion, recorder.snapshot()[0].ErrorClass)
		})
	}
}

func TestSoakReaderAndVerifier_ApplyDefaultsAndSkipEmptyCatalog(t *testing.T) {
	catalog := newSoakCatalog(8, 100, 0, nil)
	reader := newSoakReader(
		soakReadConfig{},
		&soakTopology{Subscriptions: []model.Subscription{
			{
				RoomID: "room-1",
				User:   model.SubscriptionUser{},
			},
		}},
		catalog,
		nil,
		nil,
		nil,
		nil,
	)
	outcome, err := reader.LoadHistory(context.Background(), "room-1")
	require.NoError(t, err)
	assert.True(t, outcome.Skipped)

	verifier := newSoakVerifier(nil, catalog, nil, nil, nil)
	result := verifier.Sample(context.Background(), "room-1")
	assert.Equal(t, soakVerifySkipped, result.Class)
	result = verifier.VerifyHistory(context.Background(), "room-1", "missing")
	assert.Equal(t, soakVerifySkipped, result.Class)
}

func TestSoakReader_RecordsRPCFailuresByEndpoint(t *testing.T) {
	tests := []struct {
		name   string
		invoke func(*soakReader) (soakReadOutcome, error)
	}{
		{
			name: "load history",
			invoke: func(reader *soakReader) (soakReadOutcome, error) {
				return reader.LoadHistory(context.Background(), "room-1")
			},
		},
		{
			name: "get thread",
			invoke: func(reader *soakReader) (soakReadOutcome, error) {
				return reader.GetThreadMessages(context.Background(), "room-1")
			},
		},
		{
			name: "get message",
			invoke: func(reader *soakReader) (soakReadOutcome, error) {
				return reader.GetMessageByID(context.Background(), "room-1")
			},
		},
		{
			name: "pinned list",
			invoke: func(reader *soakReader) (soakReadOutcome, error) {
				return reader.ListPinnedMessages(context.Background(), "room-1")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clock := newFakeSoakClock(time.Unix(100, 0))
			// The parent carries a reply so the thread-read subtest has a
			// thread to fetch; a zero-reply parent is skipped by design and
			// would never reach the transport.
			catalog := acceptedSoakReadThread(
				t,
				clock,
				"room-1",
				"AAAAAAAAAAAAAAAAAAAA",
			)
			recorder := &soakReadRecorder{}
			reader := newTestSoakReader(
				&soakRPCFakeTransport{replies: []soakRPCFakeReply{{
					data: []byte(`{"error":"denied","code":"forbidden"}`),
				}}},
				recorder,
				catalog,
			)

			_, err := tt.invoke(reader)
			require.Error(t, err)
			require.Len(t, recorder.snapshot(), 1)
			assert.Equal(t, soakErrorForbidden, recorder.snapshot()[0].ErrorClass)
		})
	}
}

func TestCompareSoakVerifiedMessage_ClassifiesEveryMismatch(t *testing.T) {
	editedAt := time.Unix(101, 0).UTC()
	expected := soakCatalogMessage{
		Candidate: soakCatalogCandidate{
			ID: "message-1", RoomID: "room-1", Author: "alice",
			ContentSHA256: soakContentDigest("hello"),
		},
		Edited: true,
	}
	valid := soakVerifyMessage{
		MessageID: "message-1", RoomID: "room-1",
		Sender: modelParticipant("alice"),
		Msg:    "hello", EditedAt: &editedAt,
	}
	tests := []struct {
		name   string
		mutate func(*soakVerifyMessage)
		field  soakVerifyField
	}{
		{
			name: "message ID",
			mutate: func(actual *soakVerifyMessage) {
				actual.MessageID = "different"
			},
			field: "message_id",
		},
		{
			name: "room ID",
			mutate: func(actual *soakVerifyMessage) {
				actual.RoomID = "different"
			},
			field: "room_id",
		},
		{
			name: "author",
			mutate: func(actual *soakVerifyMessage) {
				actual.Sender.Account = "bob"
			},
			field: "author",
		},
		{
			name: "deleted",
			mutate: func(actual *soakVerifyMessage) {
				actual.Deleted = true
			},
			field: "deleted",
		},
		{
			name: "content",
			mutate: func(actual *soakVerifyMessage) {
				actual.Msg = "different"
			},
			field: "content",
		},
		{
			name: "edited timestamp",
			mutate: func(actual *soakVerifyMessage) {
				actual.EditedAt = nil
			},
			field: "edited_at",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := valid
			tt.mutate(&actual)
			result := soakVerifyResult{}
			compareSoakVerifiedMessage(&result, &expected, &actual)
			assert.Equal(t, soakVerifyMismatch, result.Class)
			assert.Equal(t, tt.field, result.Field)
		})
	}

	result := soakVerifyResult{Field: "stale"}
	compareSoakVerifiedMessage(&result, &expected, &valid)
	assert.Equal(t, soakVerifyOK, result.Class)
	assert.Empty(t, result.Field)

	deletedExpected := expected
	deletedExpected.Deleted = true
	deletedActual := valid
	deletedActual.Deleted = true
	deletedActual.Msg = ""
	compareSoakVerifiedMessage(&result, &deletedExpected, &deletedActual)
	assert.Equal(t, soakVerifyOK, result.Class)
}

func TestClassifySoakVerifyRPCError_CoversTerminalAndTransientClasses(t *testing.T) {
	tests := []struct {
		class soakErrorClass
		want  soakVerifyClass
	}{
		{class: soakErrorNotFound, want: soakVerifyMissing},
		{class: soakErrorRequestEncode, want: soakVerifyMalformed},
		{class: soakErrorResponseDecode, want: soakVerifyMalformed},
		{class: soakErrorTimeout, want: soakVerifyRetryable},
		{class: soakErrorForbidden, want: soakVerifyRPCError},
	}
	for _, tt := range tests {
		result := soakVerifyResult{}
		classifySoakVerifyRPCError(&result, tt.class, "")
		assert.Equal(t, tt.want, result.Class)
		assert.Equal(t, tt.class, result.RPCErrorClass)
	}
}

func TestSoakTopology_RejectsInvalidIdentitySources(t *testing.T) {
	users := makeSoakUsers(4, "site-a")
	cfg := validSoakConfig(t)
	_, err := buildSoakTopology(users, &cfg, "site-a", 1, nil)
	require.Error(t, err)
	_, err = buildSoakTopology(users, &cfg, "site-a", 1, &soakIDs{})
	require.Error(t, err)
}

func modelParticipant(account string) cassandra.Participant {
	return cassandra.Participant{ID: "user-" + account, Account: account}
}
