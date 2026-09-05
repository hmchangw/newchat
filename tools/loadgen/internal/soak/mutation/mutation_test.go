package mutation

import (
	"context"
	"encoding/json"
	"math/rand" // #nosec G404 -- load generator randomness, never used for secrets // nosemgrep: math-random-used
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/emoji"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
	soakcatalog "github.com/hmchangw/chat/tools/loadgen/internal/soak/catalog"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/rpc"
	soaktopology "github.com/hmchangw/chat/tools/loadgen/internal/soak/topology"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/wire"
)

func TestSoakReactionShortcode_MatchesHistoryServiceContract(t *testing.T) {
	canonical, err := emoji.Canonicalize(ReactionShortcode)
	require.NoError(t, err)
	assert.Equal(t, ReactionShortcode, canonical)
}

func TestNewSoakMutator_AppliesDefaultsAndFiltersMembers(t *testing.T) {
	mutator := New(
		nil,
		&soaktopology.Topology{
			ActiveUsers: []model.User{{ID: "u-1", Account: "alice"}},
			Subscriptions: []model.Subscription{
				{
					RoomID: "room-1",
					User:   model.SubscriptionUser{ID: "u-1", Account: "alice"},
				},
				{
					RoomID: "room-1",
					User:   model.SubscriptionUser{ID: "u-2", Account: "bob"},
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

	mutator = New(
		&Config{MutationRetries: -1},
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	assert.Zero(t, mutator.cfg.MutationRetries)

	scheduler := NewScheduler(2, nil)
	scheduler.ObserveAcceptedSend()
	assert.Equal(t, KindDelete, scheduler.Next())
	scheduler = NewScheduler(-1, rand.New(rand.NewSource(1)))
	assert.NotEqual(t, KindDelete, scheduler.Next())
}

func TestSoakMutator_EditAndDeleteUseOriginalSender(t *testing.T) {
	tests := []struct {
		name        string
		run         func(*Mutator) (Outcome, error)
		reply       []byte
		wantAction  rpc.Action
		wantSubject string
		assertState func(*testing.T, soakcatalog.Message)
	}{
		{
			name: "edit",
			run: func(mutator *Mutator) (Outcome, error) {
				return mutator.Edit(context.Background(), "room-1", "updated")
			},
			reply:       []byte(`{"messageId":"message-1","editedAt":1000}`),
			wantAction:  rpc.ActionEdit,
			wantSubject: subject.MsgEdit("alice", "room-1", "site-1"),
			assertState: func(t *testing.T, message soakcatalog.Message) {
				assert.True(t, message.Edited)
				assert.Equal(t, soakcatalog.ContentDigest("updated"), message.ContentSHA256)
			},
		},
		{
			name: "soft delete",
			run: func(mutator *Mutator) (Outcome, error) {
				return mutator.Delete(context.Background(), "room-1")
			},
			reply:       []byte(`{"messageId":"message-1","deletedAt":1000}`),
			wantAction:  rpc.ActionDelete,
			wantSubject: subject.MsgDelete("alice", "room-1", "site-1"),
			assertState: func(t *testing.T, message soakcatalog.Message) {
				assert.True(t, message.Deleted)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clock := newFakeSoakClock(time.Unix(100, 0))
			catalog := acceptedMutationMessage(t, clock, "message-1", "alice")
			transport := &soakReadTransport{replies: []soakRPCFakeReply{{data: tt.reply}}}
			recorder := &soakMutationRecorder{}
			mutator := newTestSoakMutator(catalog, transport, recorder, mutationTopology(), clock)

			outcome, err := tt.run(mutator)
			require.NoError(t, err)
			assert.Equal(t, tt.wantAction, outcome.Action)
			assert.False(t, outcome.Skipped)

			calls := transport.snapshot()
			require.Len(t, calls, 1)
			assert.Equal(t, tt.wantSubject, calls[0].subject)
			message, ok := catalog.Get("room-1", "message-1")
			require.True(t, ok)
			tt.assertState(t, message)

			samples := recorder.snapshot()
			require.Len(t, samples, 1)
			assert.Equal(t, tt.wantAction, samples[0].Action)
			assert.Empty(t, samples[0].ErrorClass)
		})
	}
}

func TestSoakMutationScheduler_DeletesAtConfiguredAcceptedMessageRatio(t *testing.T) {
	scheduler := NewScheduler(0.001, rand.New(rand.NewSource(42)))
	const accepted = 1000000
	for range accepted {
		scheduler.ObserveAcceptedSend()
	}

	deletes := 0
	for scheduler.Next() == KindDelete {
		deletes++
	}
	assert.InDelta(t, accepted*0.001, deletes, accepted*0.00015)
}

func TestSoakMutationScheduler_NonDeleteBudgetSplitsEditAndPin(t *testing.T) {
	scheduler := NewScheduler(0, rand.New(rand.NewSource(42)))
	counts := map[Kind]int{}
	for range 100000 {
		counts[scheduler.Next()]++
	}
	assert.InDelta(t, 0.50, float64(counts[KindEdit])/100000, 0.01)
	assert.InDelta(t, 0.50, float64(counts[KindPinFamily])/100000, 0.01)
	assert.Zero(t, counts[KindDelete])
}

func TestSoakMutator_PinUnpinTransitionsAndAvoidsPinLimit(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0))
	catalog := acceptedMutationMessage(t, clock, "message-1", "alice")
	transport := &soakReadTransport{replies: []soakRPCFakeReply{
		{data: []byte(`{"messageId":"message-1","pinnedAt":1000}`)},
		{data: []byte(`{"messageId":"message-1"}`)},
	}}
	mutator := newTestSoakMutator(catalog, transport, &soakMutationRecorder{}, mutationTopology(), clock)
	mutator.cfg.MaxPinnedPerRoom = 1

	pin, err := mutator.PinOrUnpin(context.Background(), "room-1")
	require.NoError(t, err)
	assert.Equal(t, rpc.ActionPin, pin.Action)
	message, ok := catalog.Get("room-1", "message-1")
	require.True(t, ok)
	assert.True(t, message.Pinned)

	unpin, err := mutator.PinOrUnpin(context.Background(), "room-1")
	require.NoError(t, err)
	assert.Equal(t, rpc.ActionUnpin, unpin.Action)
	message, ok = catalog.Get("room-1", "message-1")
	require.True(t, ok)
	assert.False(t, message.Pinned)

	assert.Len(t, transport.snapshot(), 2)
}

func TestSoakMutator_AtPinLimitChoosesUnpinInsteadOfInvalidPin(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0))
	catalog := acceptedMutationMessage(t, clock, "message-1", "alice")
	require.True(t, catalog.SetPinned("room-1", "message-1", true))
	acceptMutationCatalogMessage(t, catalog, clock, "message-2", "bob")
	transport := &soakReadTransport{replies: []soakRPCFakeReply{{
		data: []byte(`{"messageId":"message-1"}`),
	}}}
	mutator := newTestSoakMutator(catalog, transport, &soakMutationRecorder{}, mutationTopology(), clock)
	mutator.cfg.MaxPinnedPerRoom = 1

	outcome, err := mutator.PinOrUnpin(context.Background(), "room-1")
	require.NoError(t, err)
	assert.Equal(t, rpc.ActionUnpin, outcome.Action)
	calls := transport.snapshot()
	require.Len(t, calls, 1)
	assert.Equal(t, subject.MsgUnpin("alice", "room-1", "site-1"), calls[0].subject)
}

func TestSoakMutator_ReactionActorsAreMembersUniqueAndClamped(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0))
	catalog := acceptedMutationMessage(t, clock, "message-1", "alice")
	transport := &soakReadTransport{replies: []soakRPCFakeReply{
		{data: reactionSuccess("message-1", model.ReactionActionAdded)},
		{data: reactionSuccess("message-1", model.ReactionActionAdded)},
		{data: reactionSuccess("message-1", model.ReactionActionRemoved)},
	}}
	topology := mutationTopology()
	recorder := &soakMutationRecorder{}
	mutator := newTestSoakMutator(catalog, transport, recorder, topology, clock)
	mutator.cfg.ReactionsPerHotMessage = 99
	mutator.cfg.ReactionRemoveShare = 0

	for range 3 {
		_, err := mutator.React(context.Background(), "room-1")
		require.NoError(t, err)
	}

	calls := transport.snapshot()
	require.Len(t, calls, 3)
	actors := make([]string, 0, 2)
	for index, call := range calls {
		account := ""
		switch call.subject {
		case subject.MsgReact("alice", "room-1", "site-1"):
			account = "alice"
		case subject.MsgReact("bob", "room-1", "site-1"):
			account = "bob"
		}
		require.NotEmpty(t, account)
		if index < 2 {
			assert.NotContains(t, actors, account)
			actors = append(actors, account)
		}
	}
	assert.Len(t, actors, 2, "width is clamped to two room members")

	message, ok := catalog.Get("room-1", "message-1")
	require.True(t, ok)
	assert.Len(t, message.Reactions[ReactionShortcode], 1, "third operation removes at the width cap")
}

func TestSoakMutator_ReactionRemoveShareUsesExistingActor(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0))
	catalog := acceptedMutationMessage(t, clock, "message-1", "alice")
	require.True(t, catalog.SetReaction(
		"room-1",
		"message-1",
		ReactionShortcode,
		"bob",
		true,
	))
	transport := &soakReadTransport{replies: []soakRPCFakeReply{{
		data: reactionSuccess("message-1", model.ReactionActionRemoved),
	}}}
	mutator := newTestSoakMutator(catalog, transport, &soakMutationRecorder{}, mutationTopology(), clock)
	mutator.cfg.ReactionRemoveShare = 1

	outcome, err := mutator.React(context.Background(), "room-1")
	require.NoError(t, err)
	assert.Equal(t, model.ReactionActionRemoved, outcome.ReactionAction)
	calls := transport.snapshot()
	require.Len(t, calls, 1)
	assert.Equal(t, subject.MsgReact("bob", "room-1", "site-1"), calls[0].subject)
}

func TestSoakMutator_ReactionReconcilesAuthoritativeToggleAfterRestart(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0))
	catalog := acceptedMutationMessage(t, clock, "message-1", "alice")
	transport := &soakReadTransport{replies: []soakRPCFakeReply{{
		data: reactionSuccess("message-1", model.ReactionActionRemoved),
	}}}
	mutator := newTestSoakMutator(
		catalog,
		transport,
		&soakMutationRecorder{},
		mutationTopology(),
		clock,
	)
	mutator.cfg.ReactionRemoveShare = 0

	outcome, err := mutator.React(context.Background(), "room-1")

	require.NoError(t, err)
	assert.Equal(t, model.ReactionActionRemoved, outcome.ReactionAction)
	message, ok := catalog.Get("room-1", "message-1")
	require.True(t, ok)
	assert.Empty(t, message.Reactions[ReactionShortcode])
}

func TestSoakMutator_HotOnlyScopeKeepsBuildingSameMessage(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0))
	catalog := acceptedMutationMessage(t, clock, "message-1", "alice")
	transport := &soakReadTransport{replies: []soakRPCFakeReply{
		{data: reactionSuccess("message-1", model.ReactionActionAdded)},
		{data: reactionSuccess("message-1", model.ReactionActionAdded)},
	}}
	mutator := newTestSoakMutator(catalog, transport, &soakMutationRecorder{}, mutationTopology(), clock)
	mutator.cfg.ReactionMessageScope = "hot_only"
	mutator.cfg.ReactionsPerHotMessage = 2

	_, err := mutator.React(context.Background(), "room-1")
	require.NoError(t, err)
	acceptMutationCatalogMessage(t, catalog, clock, "message-2", "bob")
	second, err := mutator.React(context.Background(), "room-1")
	require.NoError(t, err)
	assert.Equal(t, "message-1", second.MessageID)
}

func TestSoakMutator_AllMessagesScopeUsesNewestEligibleMessage(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0))
	catalog := acceptedMutationMessage(t, clock, "message-1", "alice")
	transport := &soakReadTransport{replies: []soakRPCFakeReply{
		{data: reactionSuccess("message-1", model.ReactionActionAdded)},
		{data: reactionSuccess("message-2", model.ReactionActionAdded)},
	}}
	mutator := newTestSoakMutator(catalog, transport, &soakMutationRecorder{}, mutationTopology(), clock)
	mutator.cfg.ReactionMessageScope = "all_messages"

	_, err := mutator.React(context.Background(), "room-1")
	require.NoError(t, err)
	acceptMutationCatalogMessage(t, catalog, clock, "message-2", "bob")
	second, err := mutator.React(context.Background(), "room-1")
	require.NoError(t, err)
	assert.Equal(t, "message-2", second.MessageID)
}

func TestSoakMutator_NotFoundRetriesThenReportsTargetMissing(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0))
	catalog := acceptedMutationMessage(t, clock, "message-1", "alice")
	notFound := []byte(`{"error":"message not found","code":"not_found"}`)
	transport := &soakReadTransport{replies: []soakRPCFakeReply{
		{data: notFound},
		{data: notFound},
		{data: notFound},
	}}
	recorder := &soakMutationRecorder{}
	mutator := newTestSoakMutator(catalog, transport, recorder, mutationTopology(), clock)
	mutator.cfg.MutationRetries = 2

	outcome, err := mutator.Edit(context.Background(), "room-1", "updated")
	require.NoError(t, err, "exhausted not-found is a soft skip")
	assert.True(t, outcome.Skipped)
	assert.True(t, outcome.TargetMissing)
	assert.Equal(t, 2, outcome.Retries)
	assert.Len(t, transport.snapshot(), 3)

	samples := recorder.snapshot()
	require.Len(t, samples, 1)
	assert.Equal(t, rpc.ErrorMutationTargetMissing, samples[0].ErrorClass)
	assert.True(t, samples[0].TargetMissing)
	message, ok := catalog.Get("room-1", "message-1")
	require.True(t, ok)
	assert.False(t, message.Edited, "catalog reconciles only after success")
}

func TestSoakMutator_LatencyExcludesTargetRetryBackoff(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0))
	catalog := acceptedMutationMessage(t, clock, "message-1", "alice")
	notFound := []byte(`{"error":"message not found","code":"not_found"}`)
	transport := &soakReadTransport{replies: []soakRPCFakeReply{
		{data: notFound},
		{data: notFound},
		{data: notFound},
	}}
	recorder := &soakMutationRecorder{}
	mutator := newTestSoakMutator(
		catalog,
		transport,
		recorder,
		mutationTopology(),
		clock,
	)
	mutator.cfg.RetryMinBackoff = 100 * time.Millisecond
	mutator.cfg.RetryMaxBackoff = 100 * time.Millisecond
	mutator.sleeper = &advancingSoakSleeper{clock: clock}
	startedAt := clock.Now()

	_, err := mutator.Edit(context.Background(), "room-1", "updated")

	require.NoError(t, err)
	samples := recorder.snapshot()
	require.Len(t, samples, 1)
	assert.Zero(t, samples[0].Latency)
	assert.Equal(t, 200*time.Millisecond, clock.Now().Sub(startedAt))
}

func TestSoakMutator_ReactionNotFoundUsesTargetRetryPolicy(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0))
	catalog := acceptedMutationMessage(t, clock, "message-1", "alice")
	notFound := []byte(`{"error":"message not found","code":"not_found"}`)
	transport := &soakReadTransport{replies: []soakRPCFakeReply{
		{data: notFound},
		{data: notFound},
		{data: notFound},
	}}
	recorder := &soakMutationRecorder{}
	mutator := newTestSoakMutator(catalog, transport, recorder, mutationTopology(), clock)
	mutator.cfg.MutationRetries = 2
	mutator.cfg.ReactionRemoveShare = 0

	outcome, err := mutator.React(context.Background(), "room-1")
	require.NoError(t, err)
	assert.True(t, outcome.Skipped)
	assert.True(t, outcome.TargetMissing)
	assert.Equal(t, 2, outcome.Retries)
	assert.Len(t, transport.snapshot(), 3)

	samples := recorder.snapshot()
	require.Len(t, samples, 1)
	assert.Equal(t, rpc.ErrorMutationTargetMissing, samples[0].ErrorClass)
	message, ok := catalog.Get("room-1", "message-1")
	require.True(t, ok)
	assert.Empty(t, message.Reactions)
}

func TestSoakMutator_ReactionTimeoutReadsStateInsteadOfBlindToggleRetry(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0))
	catalog := acceptedMutationMessage(t, clock, "message-1", "alice")
	state := []byte(`{
		"roomId":"room-1",
		"messageId":"message-1",
		"reactions":{"thumbsup":[{"account":"bob","displayName":"Bob"}]}
	}`)
	transport := &soakReadTransport{replies: []soakRPCFakeReply{
		{err: context.DeadlineExceeded},
		{data: state},
	}}
	mutator := newTestSoakMutator(catalog, transport, &soakMutationRecorder{}, mutationTopology(), clock)
	mutator.rpc = rpc.NewClient(
		transport,
		rpc.RetryConfig{
			MaxAttempts: 2,
			MinBackoff:  time.Millisecond,
			MaxBackoff:  time.Millisecond,
		},
		&soakRecordingSleeper{},
		nil,
	)
	mutator.cfg.ReactionRemoveShare = 0

	outcome, err := mutator.React(context.Background(), "room-1")
	require.NoError(t, err)
	assert.True(t, outcome.AmbiguityResolved)
	assert.Equal(t, model.ReactionActionAdded, outcome.ReactionAction)

	calls := transport.snapshot()
	require.Len(t, calls, 2)
	assert.Equal(t, subject.MsgReact("bob", "room-1", "site-1"), calls[0].subject)
	assert.Equal(t, subject.MsgGet("bob", "room-1", "site-1"), calls[1].subject)
	message, ok := catalog.Get("room-1", "message-1")
	require.True(t, ok)
	assert.Equal(t, []string{"bob"}, message.Reactions[ReactionShortcode])
}

func TestSoakMutator_ReactionTimeoutRetriesWhenStateDidNotChange(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0))
	catalog := acceptedMutationMessage(t, clock, "message-1", "alice")
	state := []byte(`{
		"roomId":"room-1",
		"messageId":"message-1",
		"reactions":{}
	}`)
	transport := &soakReadTransport{replies: []soakRPCFakeReply{
		{err: context.DeadlineExceeded},
		{data: state},
		{data: reactionSuccess("message-1", model.ReactionActionAdded)},
	}}
	mutator := newTestSoakMutator(catalog, transport, &soakMutationRecorder{}, mutationTopology(), clock)
	mutator.rpc = rpc.NewClient(
		transport,
		rpc.RetryConfig{
			MaxAttempts: 2,
			MinBackoff:  time.Millisecond,
			MaxBackoff:  time.Millisecond,
		},
		&soakRecordingSleeper{},
		nil,
	)
	mutator.cfg.ReactionRemoveShare = 0

	outcome, err := mutator.React(context.Background(), "room-1")
	require.NoError(t, err)
	assert.False(t, outcome.AmbiguityResolved)
	assert.Equal(t, 1, outcome.Retries)
	assert.Equal(t, model.ReactionActionAdded, outcome.ReactionAction)

	calls := transport.snapshot()
	require.Len(t, calls, 3)
	assert.Equal(t, subject.MsgReact("bob", "room-1", "site-1"), calls[0].subject)
	assert.Equal(t, subject.MsgGet("bob", "room-1", "site-1"), calls[1].subject)
	assert.Equal(t, subject.MsgReact("bob", "room-1", "site-1"), calls[2].subject)
}

func TestSoakMutator_DeletedMessagesAreExcludedFromFutureActions(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0))
	catalog := acceptedMutationMessage(t, clock, "message-1", "alice")
	require.True(t, catalog.MarkDeleted("room-1", "message-1"))
	transport := &soakReadTransport{}
	recorder := &soakMutationRecorder{}
	mutator := newTestSoakMutator(catalog, transport, recorder, mutationTopology(), clock)

	edit, err := mutator.Edit(context.Background(), "room-1", "updated")
	require.NoError(t, err)
	assert.True(t, edit.Skipped)
	pin, err := mutator.PinOrUnpin(context.Background(), "room-1")
	require.NoError(t, err)
	assert.True(t, pin.Skipped)
	reaction, err := mutator.React(context.Background(), "room-1")
	require.NoError(t, err)
	assert.True(t, reaction.Skipped)
	assert.Empty(t, transport.snapshot())
}

func TestSoakMutator_SkipsUnavailableTargetsAndActors(t *testing.T) {
	clock := newFakeSoakClock(time.Unix(100, 0))
	empty := soakcatalog.New(8, 100, 0, clock)
	mutator := newTestSoakMutator(
		empty,
		&soakReadTransport{},
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
		&soakReadTransport{},
		&soakMutationRecorder{},
		&soaktopology.Topology{},
		clock,
	)
	reaction, err = mutator.React(context.Background(), "room-1")
	require.NoError(t, err)
	assert.True(t, reaction.Skipped)
}

func TestSoakMutator_RejectsMismatchedMutationReplies(t *testing.T) {
	tests := []struct {
		name   string
		setup  func(*soakcatalog.Catalog)
		invoke func(*Mutator) (Outcome, error)
	}{
		{
			name: "edit",
			invoke: func(mutator *Mutator) (Outcome, error) {
				return mutator.Edit(context.Background(), "room-1", "edited")
			},
		},
		{
			name: "delete",
			invoke: func(mutator *Mutator) (Outcome, error) {
				return mutator.Delete(context.Background(), "room-1")
			},
		},
		{
			name: "pin",
			invoke: func(mutator *Mutator) (Outcome, error) {
				return mutator.PinOrUnpin(context.Background(), "room-1")
			},
		},
		{
			name:  "unpin",
			setup: func(catalog *soakcatalog.Catalog) { catalog.SetPinned("room-1", "message-1", true) },
			invoke: func(mutator *Mutator) (Outcome, error) {
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
				&soakReadTransport{replies: []soakRPCFakeReply{{
					data: []byte(`{"messageId":"different"}`),
				}}},
				recorder,
				mutationTopology(),
				clock,
			)

			_, err := tt.invoke(mutator)
			require.Error(t, err)
			assert.Equal(t, rpc.ErrorAssertion, rpc.ClassifyError(err))
			require.NotEmpty(t, recorder.snapshot())
			assert.Equal(t, rpc.ErrorAssertion, recorder.snapshot()[0].ErrorClass)
		})
	}
}

type soakMutationRecorder struct {
	mu      sync.Mutex
	samples []Sample
}

func (r *soakMutationRecorder) Record(sample Sample) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.samples = append(r.samples, sample)
}

func (r *soakMutationRecorder) snapshot() []Sample {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Sample(nil), r.samples...)
}

func newTestSoakMutator(
	catalog *soakcatalog.Catalog,
	transport rpc.Transport,
	recorder SampleRecorder,
	topology *soaktopology.Topology,
	clock *fakeSoakClock,
) *Mutator {
	return New(&Config{
		SiteID:                 "site-1",
		MutationRetries:        2,
		RetryMinBackoff:        time.Millisecond,
		RetryMaxBackoff:        time.Millisecond,
		MaxPinnedPerRoom:       10,
		ReactionsPerHotMessage: 30,
		ReactionRemoveShare:    0.20,
		ReactionMessageScope:   "hot_only",
		RequestTimeout:         time.Second,
	}, topology, catalog, rpc.NewClient(
		transport,
		rpc.RetryConfig{
			MaxAttempts: 1,
			MinBackoff:  time.Millisecond,
			MaxBackoff:  time.Millisecond,
		},
		&soakRecordingSleeper{},
		nil,
	), recorder, rand.New(rand.NewSource(1)), clock, &soakRecordingSleeper{})
}

func mutationTopology() *soaktopology.Topology {
	return &soaktopology.Topology{Subscriptions: []model.Subscription{
		{
			RoomID: "room-1", IsSubscribed: true,
			User: model.SubscriptionUser{ID: "u-alice", Account: "alice"},
		},
		{
			RoomID: "room-1", IsSubscribed: true,
			User: model.SubscriptionUser{ID: "u-bob", Account: "bob"},
		},
	}}
}

func acceptedMutationMessage(
	t *testing.T,
	clock *fakeSoakClock,
	messageID string,
	author string,
) *soakcatalog.Catalog {
	t.Helper()
	catalog := soakcatalog.New(16, 100, 0, clock)
	acceptMutationCatalogMessage(t, catalog, clock, messageID, author)
	return catalog
}

func acceptMutationCatalogMessage(
	t *testing.T,
	catalog *soakcatalog.Catalog,
	clock *fakeSoakClock,
	messageID string,
	author string,
) {
	t.Helper()
	require.NoError(t, catalog.TrackPublished(&soakcatalog.Candidate{
		ID: messageID, RoomID: "room-1", Author: author, Content: "original",
		CreatedAt: clock.Now(), ThreadReplyLimit: 10,
	}))
	require.True(t, catalog.Accept("room-1", messageID))
	clock.Advance(time.Millisecond)
}

func reactionSuccess(messageID string, action model.ReactionAction) []byte {
	data, err := json.Marshal(wire.ReactMessageResponse{
		MessageID: messageID,
		Shortcode: ReactionShortcode,
		Action:    action,
		ReactedAt: 1000,
	})
	if err != nil {
		panic(err)
	}
	return data
}

type advancingSoakSleeper struct {
	clock *fakeSoakClock
}

func (s *advancingSoakSleeper) Sleep(
	ctx context.Context,
	delay time.Duration,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.clock.Advance(delay)
	return nil
}
