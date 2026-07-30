package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/roomkeysender"
	"github.com/hmchangw/chat/pkg/roomkeystore"
	"github.com/hmchangw/chat/pkg/subject"
)

func newTeamsTestHandler(t *testing.T, store *MockSubscriptionStore) (*Handler, *[]publishedMsg) {
	t.Helper()
	var published []publishedMsg
	publish := func(_ context.Context, subj string, data []byte, _ string) error {
		published = append(published, publishedMsg{subj: subj, data: data})
		return nil
	}
	h := NewHandler(store, "site-a", publish, testKeyStore, testKeySender)
	return h, &published
}

func teamsCreateEvent(chats ...model.TeamsRoomCreateChat) []byte {
	data, _ := json.Marshal(model.TeamsRoomCreateEvent{Chats: chats, Timestamp: 1000})
	return data
}

// membershipEvents decodes every published InboxMemberEvent whose subject contains
// subjMatch, so a test can assert the event type (in the subject) + its account set.
func membershipEvents(t *testing.T, published []publishedMsg, subjMatch string) []model.InboxMemberEvent {
	t.Helper()
	var out []model.InboxMemberEvent
	for _, p := range published {
		if !strings.Contains(p.subj, subjMatch) {
			continue
		}
		raw := p.data
		if strings.Contains(p.subj, "outbox") { // outbox double-wraps: OutboxEvent.Envelope = InboxEvent, .Payload = the member event
			var ob model.OutboxEvent
			require.NoError(t, json.Unmarshal(p.data, &ob))
			var ie model.InboxEvent
			require.NoError(t, json.Unmarshal(ob.Envelope, &ie))
			raw = ie.Payload
		}
		var e model.InboxMemberEvent
		require.NoError(t, json.Unmarshal(raw, &e))
		out = append(out, e)
	}
	return out
}

// TestProcessTeamsRoomCreate_AddOnly: a fresh room with three brand-new members —
// one known locally (alice), one unknown requiring the publish path (carol), and
// one known at a remote site (dave) exercising cross-site federation.
func TestProcessTeamsRoomCreate_AddOnly(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)
	h, published := newTeamsTestHandler(t, store)
	carolID := idgen.DeterministicID([]byte("aad3"))
	h.publishUsers = func(_ context.Context, users []model.IUserWithChange) error {
		require.Len(t, users, 1, "only the unknown member is published")
		// The publisher owns a deterministic _id (Graph-id hash) so every site's
		// hr-sync-worker upserts the same identity; no employeeId — not an employee.
		assert.Equal(t, "carol", users[0].Account)
		assert.Equal(t, carolID, users[0].ID)
		assert.Equal(t, "Carol Wang 王小美", users[0].ChineseName, "displayName lands in ChineseName")
		assert.Empty(t, users[0].EmployeeID, "externals aren't employees — no employeeId")
		return nil
	}

	store.EXPECT().CreateRoom(gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil)
	store.EXPECT().ListByRoom(gomock.Any(), "chat1").Return(nil, nil)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(&model.User{ID: "u1", Account: "alice", SiteID: "site-a"}, nil)
	store.EXPECT().GetUser(gomock.Any(), "carol").Return(nil, ErrUserNotFound)
	store.EXPECT().GetUser(gomock.Any(), "dave").Return(&model.User{ID: "u4", Account: "dave", SiteID: "site-b"}, nil)
	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, subs []*model.Subscription) error {
			require.Len(t, subs, 3)
			for _, s := range subs {
				assert.Equal(t, "chat1", s.RoomID)
				assert.Equal(t, model.RoomTypeChannel, s.RoomType)
				assert.Equal(t, []model.Role{model.RoleMember}, s.Roles)
				assert.Equal(t, model.OriginTeams, s.Origin)
			}
			return nil
		})
	store.EXPECT().ReconcileMemberCounts(gomock.Any(), "chat1").Return(nil)

	chat := model.TeamsRoomCreateChat{
		ID:   "chat1",
		Name: "Project Sync",
		Members: []model.TeamsRoomCreateMember{
			{ID: "aad1", Account: "alice"},
			{ID: "aad3", Account: "carol", DisplayName: "Carol Wang 王小美"},
			{ID: "aad4", Account: "dave"},
		},
		CreatedDateTime: time.Unix(0, 0).UTC(),
	}
	err := h.processTeamsRoomCreate(context.Background(), teamsCreateEvent(chat))
	require.NoError(t, err)

	// Local InboxInternal member_added carries all three added accounts; the
	// federated outbox event targets dave's home site (site-b) with dave only.
	local := membershipEvents(t, *published, "chat.inbox.site-a.internal.member_added")
	require.Len(t, local, 1)
	assert.ElementsMatch(t, []string{"alice", "carol", "dave"}, local[0].Accounts)
	fed := membershipEvents(t, *published, "chat.outbox.site-a.site-b.member_added")
	require.Len(t, fed, 1)
	assert.Equal(t, []string{"dave"}, fed[0].Accounts)
	assert.Empty(t, membershipEvents(t, *published, "member_removed"), "add-only batch emits no removals")
}

// TestProcessTeamsRoomCreate_FansOutRoomKeyToAddedMembers: a migrated channel
// room stays live, so it gets an encryption key fanned out to each added member
// like a native channel — clients need it to decrypt ongoing chat.
func TestProcessTeamsRoomCreate_FansOutRoomKeyToAddedMembers(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)
	pub := &mockPublisher{}
	publish := func(_ context.Context, subj string, data []byte, _ string) error {
		return pub.Publish(subj, data)
	}
	h := NewHandler(store, "site-a", publish, testKeyStore, roomkeysender.NewSender(pub))

	store.EXPECT().CreateRoom(gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil)
	store.EXPECT().ListByRoom(gomock.Any(), "chat1").Return(nil, nil)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(&model.User{ID: "u1", Account: "alice", SiteID: "site-a"}, nil)
	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().ReconcileMemberCounts(gomock.Any(), "chat1").Return(nil)

	chat := model.TeamsRoomCreateChat{
		ID: "chat1", Name: "Project Sync",
		Members: []model.TeamsRoomCreateMember{{ID: "aad1", Account: "alice"}},
	}
	require.NoError(t, h.processTeamsRoomCreate(context.Background(), teamsCreateEvent(chat)))

	assert.Contains(t, pub.subjects, subject.RoomKeyUpdate("alice"),
		"added member must receive the room key")
}

// TestProcessTeamsRoomCreate_DedupsDuplicateAccounts: the same account listed
// twice in a chat's members creates exactly one subscription (resolved once).
func TestProcessTeamsRoomCreate_DedupsDuplicateAccounts(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)
	h, _ := newTeamsTestHandler(t, store)

	store.EXPECT().CreateRoom(gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil)
	store.EXPECT().ListByRoom(gomock.Any(), "chat1").Return(nil, nil)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(&model.User{ID: "u1", Account: "alice", SiteID: "site-a"}, nil) // exactly once
	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, subs []*model.Subscription) error {
			require.Len(t, subs, 1, "a duplicate account must create only one subscription")
			assert.Equal(t, "alice", subs[0].User.Account)
			return nil
		})
	store.EXPECT().ReconcileMemberCounts(gomock.Any(), "chat1").Return(nil)

	chat := model.TeamsRoomCreateChat{
		ID: "chat1",
		Members: []model.TeamsRoomCreateMember{
			{ID: "aad1", Account: "alice"},
			{ID: "aad1b", Account: "alice"}, // duplicate account
		},
	}
	require.NoError(t, h.processTeamsRoomCreate(context.Background(), teamsCreateEvent(chat)))
}

// TestProcessTeamsRoomCreate_HardRemoveOnly: a departed member is hard-deleted
// (same as the live remove path); a returning member just gets a fresh sub later.
func TestProcessTeamsRoomCreate_HardRemoveOnly(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)
	h, published := newTeamsTestHandler(t, store)

	store.EXPECT().CreateRoom(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil)
	store.EXPECT().ListByRoom(gomock.Any(), "chat1").Return([]model.Subscription{
		{User: model.SubscriptionUser{Account: "alice"}, RoomID: "chat1", SiteID: "site-a"},
		{User: model.SubscriptionUser{Account: "bob"}, RoomID: "chat1", SiteID: "site-b"},
	}, nil)
	store.EXPECT().DeleteSubscriptionsByAccounts(gomock.Any(), "chat1", []string{"bob"}).Return(int64(1), nil)
	store.EXPECT().ReconcileMemberCounts(gomock.Any(), "chat1").Return(nil)

	chat := model.TeamsRoomCreateChat{
		ID:      "chat1",
		Name:    "Project Sync",
		Members: []model.TeamsRoomCreateMember{{ID: "aad1", Account: "alice"}}, // bob departed
	}
	err := h.processTeamsRoomCreate(context.Background(), teamsCreateEvent(chat))
	require.NoError(t, err)

	// Local + federated events are member_removed for bob; the federation targets
	// bob's home site (site-b). No member_added on a remove-only batch.
	fed := membershipEvents(t, *published, "chat.outbox.site-a.site-b.member_removed")
	require.Len(t, fed, 1, "departed bob's home site (site-b) gets one federated member_removed")
	assert.Equal(t, []string{"bob"}, fed[0].Accounts)
	local := membershipEvents(t, *published, "chat.inbox.site-a.internal.member_removed")
	require.Len(t, local, 1)
	assert.Equal(t, []string{"bob"}, local[0].Accounts)
	assert.Empty(t, membershipEvents(t, *published, "member_added"), "remove-only batch emits no adds")
}

// TestProcessTeamsRoomCreate_IdempotentNoOp: re-running the same batch against an
// already-converged room writes nothing further (no BulkCreate/Delete/Reconcile).
func TestProcessTeamsRoomCreate_IdempotentNoOp(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)
	h, published := newTeamsTestHandler(t, store)

	store.EXPECT().CreateRoom(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil)
	store.EXPECT().ListByRoom(gomock.Any(), "chat1").Return([]model.Subscription{
		{User: model.SubscriptionUser{Account: "alice"}, RoomID: "chat1", SiteID: "site-a"},
	}, nil)

	chat := model.TeamsRoomCreateChat{
		ID:      "chat1",
		Members: []model.TeamsRoomCreateMember{{ID: "aad1", Account: "alice"}},
	}
	err := h.processTeamsRoomCreate(context.Background(), teamsCreateEvent(chat))
	require.NoError(t, err)
	assert.Empty(t, *published, "converged room should publish nothing")
}

// TestProcessTeamsRoomCreate_PerChatIsolation: one chat's reconcile error is
// logged and skipped; the batch still succeeds (no Nak) and the sibling chat's
// reconcile still runs.
func TestProcessTeamsRoomCreate_PerChatIsolation(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)
	h, _ := newTeamsTestHandler(t, store)

	// chat "bad" errors at CreateRoom; chat "good" proceeds normally.
	store.EXPECT().CreateRoom(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, room *model.Room, _ *roomkeystore.RoomKeyPair) (bool, error) {
			if room.ID == "bad" {
				return false, assert.AnError
			}
			return true, nil
		}).Times(2)
	store.EXPECT().ListByRoom(gomock.Any(), "good").Return(nil, nil)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(&model.User{ID: "u1", Account: "alice", SiteID: "site-a"}, nil)
	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().ReconcileMemberCounts(gomock.Any(), "good").Return(nil)

	chats := []model.TeamsRoomCreateChat{
		{ID: "bad", Members: []model.TeamsRoomCreateMember{{ID: "x", Account: "zoe"}}},
		{ID: "good", Members: []model.TeamsRoomCreateMember{{ID: "a", Account: "alice"}}},
	}
	err := h.processTeamsRoomCreate(context.Background(), teamsCreateEvent(chats...))
	require.NoError(t, err, "a per-chat failure must never Nak the batch")
}

// TestResolveMember_AlignmentInvariant: an account already known to user-service
// (e.g. because a message sender on the same account already resolved it) is
// reused as-is — no external-identity create/fanout.
func TestResolveMember_AlignmentInvariant(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)
	h, _ := newTeamsTestHandler(t, store)
	h.publishUsers = func(context.Context, []model.IUserWithChange) error {
		t.Fatal("publishUsers must not be called for an already-known account")
		return nil
	}

	want := &model.User{ID: "u9", Account: "dave", SiteID: "site-a"}
	store.EXPECT().GetUser(gomock.Any(), "dave").Return(want, nil)

	got, err := h.resolveMember(context.Background(), "dave", "aad-dave", "Dave 大衛")
	require.NoError(t, err)
	assert.Same(t, want, got)
	assert.Empty(t, got.ChineseName, "existing user keeps its name — the passed displayName is ignored")
}

// TestResolveMember_PublishesDeterministicIdentity: an unknown account is
// published (never written locally first) with _id == employeeId == the
// deterministic Graph-id hash, and that same user is returned for the caller to
// build the subscription — no read-back.
func TestResolveMember_PublishesDeterministicIdentity(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)
	h, _ := newTeamsTestHandler(t, store)

	wantID := idgen.DeterministicID([]byte("aad-erin"))
	var fanoutCalled bool
	h.publishUsers = func(_ context.Context, users []model.IUserWithChange) error {
		fanoutCalled = true
		require.Len(t, users, 1)
		assert.Equal(t, "erin", users[0].Account)
		assert.Equal(t, wantID, users[0].ID)
		assert.Equal(t, "Erin Wang 王小恩", users[0].ChineseName, "displayName lands in ChineseName")
		assert.Empty(t, users[0].EmployeeID, "externals aren't employees — no employeeId")
		assert.Equal(t, model.IChangeTypeNewHire, users[0].ChangeType)
		return nil
	}
	store.EXPECT().GetUser(gomock.Any(), "erin").Return(nil, ErrUserNotFound)

	got, err := h.resolveMember(context.Background(), "erin", "aad-erin", "Erin Wang 王小恩")
	require.NoError(t, err)
	assert.True(t, fanoutCalled)
	assert.Equal(t, wantID, got.ID)
	assert.Equal(t, "Erin Wang 王小恩", got.ChineseName)
	assert.Empty(t, got.EmployeeID)
	assert.Equal(t, "site-a", got.SiteID)
}

// TestProcessTeamsRoomCreate_MalformedBatch: an unparsable envelope is Permanent
// (poison) — never redelivered.
func TestProcessTeamsRoomCreate_MalformedBatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)
	h, _ := newTeamsTestHandler(t, store)

	err := h.processTeamsRoomCreate(context.Background(), []byte("{not json"))
	require.Error(t, err)
	assert.ErrorIs(t, err, errPermanent, "malformed envelope must be Permanent so JetStream Acks")
}

// TestFederateTeamsMembership_MigrationHeaderStamped confirms the ctx-based
// X-Migration propagation (natsutil.WithMigrationLiveContext) actually reaches
// the publish call — the mechanism the whole handler relies on for silence.
func TestFederateTeamsMembership_MigrationHeaderStamped(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)
	var gotHeader bool
	publish := func(ctx context.Context, _ string, _ []byte, _ string) error {
		gotHeader = natsutil.HeaderForContext(ctx).Get(natsutil.HeaderMigration) == natsutil.MigrationLive
		return nil
	}
	h := NewHandler(store, "site-a", publish, testKeyStore, testKeySender)

	ctx := natsutil.WithMigrationLiveContext(context.Background())
	room := &model.Room{ID: "chat1", Name: "n", Type: model.RoomTypeChannel, SiteID: "site-a"}
	err := h.federateTeamsMembership(ctx, room, []string{"alice"}, model.InboxMemberAdded,
		map[string]string{"alice": "site-a"}, time.Now())
	require.NoError(t, err)
	assert.True(t, gotHeader)
}

// TestProcessTeamsRoomCreate_StampsMigrationHeader exercises the stamp at the
// entry point — so removing WithMigrationLiveContext from processTeamsRoomCreate
// (not just federateTeamsMembership) is caught.
func TestProcessTeamsRoomCreate_StampsMigrationHeader(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)
	var gotHeader bool
	publish := func(ctx context.Context, _ string, _ []byte, _ string) error {
		if natsutil.HeaderForContext(ctx).Get(natsutil.HeaderMigration) == natsutil.MigrationLive {
			gotHeader = true
		}
		return nil
	}
	h := NewHandler(store, "site-a", publish, testKeyStore, testKeySender)

	store.EXPECT().CreateRoom(gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil)
	store.EXPECT().ListByRoom(gomock.Any(), "chat1").Return(nil, nil)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(&model.User{ID: "u1", Account: "alice", SiteID: "site-a"}, nil)
	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().ReconcileMemberCounts(gomock.Any(), "chat1").Return(nil)

	chat := model.TeamsRoomCreateChat{
		ID:      "chat1",
		Name:    "n",
		Members: []model.TeamsRoomCreateMember{{ID: "aad1", Account: "alice"}},
	}
	require.NoError(t, h.processTeamsRoomCreate(context.Background(), teamsCreateEvent(chat)))
	assert.True(t, gotHeader, "processTeamsRoomCreate must stamp X-Migration: live on publishes")
}
