package main

import (
	"context"
	"errors"
	"math/rand"
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
			name: "room without subscribed member",
			topology: &soakTopology{
				Rooms: []model.Room{{ID: "room-1"}},
				Subscriptions: []model.Subscription{{
					RoomID: "room-1",
					User:   model.SubscriptionUser{ID: "u-1", Account: "alice"},
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
		Rooms: []model.Room{{ID: "room-1"}},
		Subscriptions: []model.Subscription{
			{
				RoomID: "room-1", IsSubscribed: true,
				User: model.SubscriptionUser{ID: "u-1", Account: "alice"},
			},
			{
				RoomID: "room-1", IsSubscribed: false,
				User: model.SubscriptionUser{ID: "u-2", Account: "bob"},
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

func TestNewSoakRPCClient_DefaultsAndInputFailures(t *testing.T) {
	client := newSoakRPCClient(
		&soakRPCFakeTransport{},
		soakRetryConfig{},
		nil,
		nil,
	)
	assert.Equal(t, 1, client.retry.MaxAttempts)
	assert.Equal(t, 100*time.Millisecond, client.retry.MinBackoff)
	assert.Equal(t, client.retry.MinBackoff, client.retry.MaxBackoff)
	assert.NotNil(t, client.sleeper)
	assert.Equal(t, 0.5, client.random())
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
	assert.Equal(t, soakErrorDecode, result.ErrorClass)
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

func TestNewSoakMutator_AppliesDefaultsAndFiltersMembers(t *testing.T) {
	mutator := newSoakMutator(
		nil,
		&soakTopology{Subscriptions: []model.Subscription{
			{
				RoomID: "room-1", IsSubscribed: true,
				User: model.SubscriptionUser{Account: "alice"},
			},
			{
				RoomID: "room-1", IsSubscribed: false,
				User: model.SubscriptionUser{Account: "bob"},
			},
		}},
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	)

	assert.Zero(t, mutator.cfg.MutationRetries)
	assert.Equal(t, 100*time.Millisecond, mutator.cfg.RetryMinBackoff)
	assert.Equal(t, mutator.cfg.RetryMinBackoff, mutator.cfg.RetryMaxBackoff)
	assert.Equal(t, 10, mutator.cfg.MaxPinnedPerRoom)
	assert.Equal(t, 1, mutator.cfg.ReactionsPerHotMessage)
	assert.Equal(t, 5*time.Second, mutator.cfg.RequestTimeout)
	assert.NotNil(t, mutator.rng)
	assert.NotNil(t, mutator.clock)
	assert.NotNil(t, mutator.sleeper)
	assert.Len(t, mutator.members["room-1"], 1)

	scheduler := newSoakMutationScheduler(2, nil)
	scheduler.ObserveAcceptedSend()
	assert.Equal(t, soakMutationDelete, scheduler.Next())
	scheduler = newSoakMutationScheduler(-1, rand.New(rand.NewSource(1)))
	assert.NotEqual(t, soakMutationDelete, scheduler.Next())
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
				RoomID: "room-1", IsSubscribed: false,
				User: model.SubscriptionUser{Account: "ignored"},
			},
		}},
		catalog,
		nil,
		nil,
		nil,
		nil,
	)
	assert.Equal(t, 50, reader.cfg.PageLimit)
	assert.Equal(t, 100, reader.cfg.MaxPages)
	assert.Equal(t, 5*time.Second, reader.cfg.RequestTimeout)
	assert.NotNil(t, reader.rng)
	assert.NotNil(t, reader.now)
	outcome, err := reader.LoadHistory(context.Background(), "room-1")
	require.NoError(t, err)
	assert.True(t, outcome.Skipped)

	verifier := newSoakVerifier(nil, catalog, nil, nil, nil)
	assert.Equal(t, 50, verifier.cfg.PageLimit)
	assert.Equal(t, 20, verifier.cfg.MaxPages)
	assert.Equal(t, 5*time.Second, verifier.cfg.RequestTimeout)
	assert.NotNil(t, verifier.now)
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
		soakCatalogCandidate: soakCatalogCandidate{
			ID: "message-1", RoomID: "room-1", Author: "alice",
			Content: "hello",
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
		field  string
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
		{class: soakErrorDecode, want: soakVerifyMalformed},
		{class: soakErrorTimeout, want: soakVerifyRetryable},
		{class: soakErrorForbidden, want: soakVerifyRPCError},
	}
	for _, tt := range tests {
		result := soakVerifyResult{}
		classifySoakVerifyRPCError(&result, tt.class)
		assert.Equal(t, tt.want, result.Class)
		assert.Equal(t, tt.class, result.RPCErrorClass)
	}
}

func TestSoakCatalog_RejectsInvalidAndRepeatedTransitions(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0))
	catalog := newSoakCatalog(1, 1, -time.Second, nil)
	require.Error(t, catalog.TrackPublished(nil))
	require.Error(t, catalog.TrackPublished(&soakCatalogCandidate{}))
	assert.False(t, catalog.Accept("missing", "missing"))
	assert.False(t, catalog.Reject("missing", "missing"))

	candidate := &soakCatalogCandidate{
		ID: "message-1", RoomID: "room-1", Author: "alice",
		CreatedAt: clock.Now(),
	}
	require.NoError(t, catalog.TrackPublished(candidate))
	require.Error(t, catalog.TrackPublished(candidate))
	require.True(t, catalog.Accept("room-1", "message-1"))
	assert.False(t, catalog.Accept("room-1", "message-1"))
	require.Error(t, catalog.TrackPublished(candidate))

	for _, action := range []soakCatalogAction{
		soakCatalogEdit, soakCatalogDelete, soakCatalogThreadParent,
		soakCatalogPin, soakCatalogReaction, soakCatalogAction("invalid"),
	} {
		_, ok := catalog.PickAnyEligible("missing", action)
		assert.False(t, ok)
	}
	_, ok := catalog.GetEligible("missing", "message-1", soakCatalogEdit)
	assert.False(t, ok)
	_, ok = catalog.PickPinCandidate("missing", false)
	assert.False(t, ok)
	assert.Zero(t, catalog.PinnedCount("missing"))
	_, ok = catalog.PickVerificationCandidate("missing", false)
	assert.False(t, ok)
	_, ok = catalog.GetVerificationCandidate("missing", "message-1")
	assert.False(t, ok)

	assert.False(t, catalog.MarkEdited("missing", "message-1", "edited"))
	assert.False(t, catalog.MarkDeleted("missing", "message-1"))
	assert.False(t, catalog.SetPinned("missing", "message-1", true))
	assert.False(t, catalog.SetReaction("missing", "message-1", "wave", "bob", true))
	assert.False(t, catalog.ReserveThreadReply("missing", "message-1"))
	assert.False(t, catalog.SetReaction("room-1", "message-1", "", "bob", true))
	assert.False(t, catalog.SetReaction("room-1", "message-1", "wave", "", true))
	assert.False(t, catalog.SetReaction("room-1", "message-1", "wave", "bob", false))
	assert.True(t, catalog.MarkDeleted("room-1", "message-1"))
	assert.False(t, catalog.MarkDeleted("room-1", "message-1"))
	assert.False(t, catalog.MarkEdited("room-1", "message-1", "edited"))
	assert.False(t, catalog.SetPinned("room-1", "message-1", true))
	assert.False(t, catalog.ReserveThreadReply("room-1", "message-1"))
}

func TestSoakTopology_RejectsInvalidLimitsAndIdentitySources(t *testing.T) {
	users := makeSoakUsers(4, "site-a")
	tests := []struct {
		name        string
		maxUsers    int
		activeUsers int
	}{
		{name: "zero max", activeUsers: 1},
		{name: "over max", maxUsers: maxBorrowedSoakUsers + 1, activeUsers: 1},
		{name: "zero active", maxUsers: 4},
		{name: "active over max", maxUsers: 2, activeUsers: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := selectSoakUsers(
				users, "site-a", tt.maxUsers, tt.activeUsers, 1,
			)
			require.Error(t, err)
		})
	}

	cfg := validSoakConfig(t)
	_, err := buildSoakTopology(users, &cfg, "site-a", 1, nil)
	require.Error(t, err)
	_, err = buildSoakTopology(users, &cfg, "site-a", 1, &soakIDs{})
	require.Error(t, err)

	used := make(map[string]struct{})
	assert.False(t, reserveSoakPair(&users[0], &users[0], used))
	assert.True(t, reserveSoakPair(&users[0], &users[1], used))
	assert.False(t, reserveSoakPair(&users[1], &users[0], used))
}

func modelParticipant(account string) cassandra.Participant {
	return cassandra.Participant{ID: "user-" + account, Account: account}
}
