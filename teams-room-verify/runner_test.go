package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
)

// chat builds a flagged chat with n members, all with accounts.
func chat(id, site string, accounts ...string) model.TeamsChat {
	members := make([]model.TeamsChatMember, 0, len(accounts))
	for _, a := range accounts {
		members = append(members, model.TeamsChatMember{ID: "g-" + a, Account: a})
	}
	return model.TeamsChat{
		ID:        id,
		SiteID:    site,
		Members:   members,
		UpdatedAt: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
	}
}

// recordingVerifier returns a verifyFunc that answers from results (keyed by
// chat id) and records the base URLs it was called with.
type recordingVerifier struct {
	mu      sync.Mutex
	calls   []string
	results map[string]model.TeamsRoomVerifyResult
	err     error
}

func (rv *recordingVerifier) fn(_ context.Context, baseURL string, chatIDs []string) (*model.TeamsRoomVerifyResponse, error) {
	rv.mu.Lock()
	rv.calls = append(rv.calls, baseURL)
	rv.mu.Unlock()
	if rv.err != nil {
		return nil, rv.err
	}
	resp := &model.TeamsRoomVerifyResponse{RequestedCount: len(chatIDs)}
	for _, id := range chatIDs {
		res, ok := rv.results[id]
		if !ok {
			continue // simulates an inspector that omitted a requested id
		}
		if res.RoomExists {
			resp.FoundCount++
		}
		resp.Chats = append(resp.Chats, res)
	}
	return resp, nil
}

func result(chatID string, exists bool, subs int) model.TeamsRoomVerifyResult {
	return model.TeamsRoomVerifyResult{
		ChatID: chatID, RoomID: idgen.DeterministicID([]byte(chatID)),
		RoomExists: exists, SubscriptionCount: subs, RoomUserCount: subs,
	}
}

func testRunConfig() runConfig {
	return runConfig{BatchSize: 10, MaxWorkers: 4, SiteURLs: map[string]string{
		"site-a": "http://inspector-a", "site-b": "http://inspector-b",
	}}
}

func TestRunner_ClearsOnlyConvergedChats(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockTeamsChatStore(ctrl)

	chats := []model.TeamsChat{
		chat("c-ok", "site-a", "alice", "bob"),
		chat("c-short", "site-a", "alice", "bob", "carol"),
		chat("c-extra", "site-a", "alice"),
		chat("c-missing", "site-a", "alice", "bob"),
	}
	store.EXPECT().ListChatsNeedingVerify(gomock.Any()).Return(chats, nil)

	rv := &recordingVerifier{results: map[string]model.TeamsRoomVerifyResult{
		"c-ok":      result("c-ok", true, 2),
		"c-short":   result("c-short", true, 2),
		"c-extra":   result("c-extra", true, 3),
		"c-missing": result("c-missing", false, 0),
	}}

	var marked []VerifiedRef
	store.EXPECT().MarkVerified(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, refs []VerifiedRef) error {
			marked = append(marked, refs...)
			return nil
		})

	r := newRunner(store, rv.fn, testRunConfig())
	require.NoError(t, r.run(context.Background()))

	require.Len(t, marked, 1, "only the converged chat is cleared")
	assert.Equal(t, "c-ok", marked[0].ID)
	assert.Equal(t, chats[0].UpdatedAt, marked[0].UpdatedAt, "the CAS token is the listed updatedAt")
}

func TestRunner_MembersWithoutAccountsStillCountAsExpected(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockTeamsChatStore(ctrl)

	// A guest with no account: room-worker skips it, so the site holds 1
	// subscription while the raw member count is 2. Per spec, expected is the
	// raw member count, so this reports as a mismatch and keeps its flag.
	guestChat := chat("c-guest", "site-a", "alice")
	guestChat.Members = append(guestChat.Members, model.TeamsChatMember{ID: "g-x", Account: ""})
	store.EXPECT().ListChatsNeedingVerify(gomock.Any()).Return([]model.TeamsChat{guestChat}, nil)

	rv := &recordingVerifier{results: map[string]model.TeamsRoomVerifyResult{
		"c-guest": result("c-guest", true, 1),
	}}
	store.EXPECT().MarkVerified(gomock.Any(), gomock.Len(0)).Return(nil).AnyTimes()

	r := newRunner(store, rv.fn, testRunConfig())
	require.NoError(t, r.run(context.Background()))
}

func TestRunner_RoutesEachSiteToItsOwnInspector(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockTeamsChatStore(ctrl)
	store.EXPECT().ListChatsNeedingVerify(gomock.Any()).Return([]model.TeamsChat{
		chat("c-a", "site-a", "alice"),
		chat("c-b", "site-b", "bob"),
	}, nil)

	rv := &recordingVerifier{results: map[string]model.TeamsRoomVerifyResult{
		"c-a": result("c-a", true, 1),
		"c-b": result("c-b", true, 1),
	}}
	store.EXPECT().MarkVerified(gomock.Any(), gomock.Any()).Return(nil).Times(2)

	r := newRunner(store, rv.fn, testRunConfig())
	require.NoError(t, r.run(context.Background()))

	assert.ElementsMatch(t, []string{"http://inspector-a", "http://inspector-b"}, rv.calls)

	stA := r.stats["site-a"]
	require.NotNil(t, stA, "site-a must have recorded stats")
	assert.Equal(t, 1, stA.ok)

	stB := r.stats["site-b"]
	require.NotNil(t, stB, "site-b must have recorded stats")
	assert.Equal(t, 1, stB.ok)
}

func TestRunner_UnknownSiteIsSkipped(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockTeamsChatStore(ctrl)
	store.EXPECT().ListChatsNeedingVerify(gomock.Any()).Return([]model.TeamsChat{
		chat("c-x", "site-unknown", "alice"),
	}, nil)
	// No MarkVerified and no inspector call: the chat keeps its flag.

	rv := &recordingVerifier{results: map[string]model.TeamsRoomVerifyResult{}}
	r := newRunner(store, rv.fn, testRunConfig())
	require.NoError(t, r.run(context.Background()))
	assert.Empty(t, rv.calls)
}

func TestRunner_InspectorFailureLeavesFlags(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockTeamsChatStore(ctrl)
	store.EXPECT().ListChatsNeedingVerify(gomock.Any()).Return([]model.TeamsChat{
		chat("c-a", "site-a", "alice"),
	}, nil)

	rv := &recordingVerifier{err: errors.New("connection refused")}
	r := newRunner(store, rv.fn, testRunConfig())
	require.NoError(t, r.run(context.Background()), "a site failure must not fail the whole run")
}

func TestRunner_MissingResultLeavesFlag(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockTeamsChatStore(ctrl)
	store.EXPECT().ListChatsNeedingVerify(gomock.Any()).Return([]model.TeamsChat{
		chat("c-present", "site-a", "alice"),
		chat("c-omitted", "site-a", "alice"),
	}, nil)

	// The inspector answers about one chat only; the omitted one is not treated
	// as a missing room, it is simply left unverified.
	rv := &recordingVerifier{results: map[string]model.TeamsRoomVerifyResult{
		"c-present": result("c-present", true, 1),
	}}
	var marked []VerifiedRef
	store.EXPECT().MarkVerified(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, refs []VerifiedRef) error {
			marked = append(marked, refs...)
			return nil
		})

	r := newRunner(store, rv.fn, testRunConfig())
	require.NoError(t, r.run(context.Background()))
	require.Len(t, marked, 1)
	assert.Equal(t, "c-present", marked[0].ID)

	st := r.stats["site-a"]
	require.NotNil(t, st, "site-a must have recorded stats")
	assert.Equal(t, 1, st.unanswered, "the omitted chat is counted as unanswered")
	assert.Equal(t, 0, st.roomsMissing, "an unanswered chat must never be counted as a missing room")
}

func TestRunner_BatchesLargeSites(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockTeamsChatStore(ctrl)

	chats := make([]model.TeamsChat, 0, 25)
	results := map[string]model.TeamsRoomVerifyResult{}
	for i := 0; i < 25; i++ {
		id := "c" + string(rune('A'+i))
		chats = append(chats, chat(id, "site-a", "alice"))
		results[id] = result(id, true, 1)
	}
	store.EXPECT().ListChatsNeedingVerify(gomock.Any()).Return(chats, nil)
	store.EXPECT().MarkVerified(gomock.Any(), gomock.Any()).Return(nil).Times(3)

	rv := &recordingVerifier{results: results}
	r := newRunner(store, rv.fn, testRunConfig()) // BatchSize 10 → 10/10/5
	require.NoError(t, r.run(context.Background()))
	assert.Len(t, rv.calls, 3, "25 chats at batch size 10 is three calls")
}

func TestRunner_EmptyListIsNoop(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockTeamsChatStore(ctrl)
	store.EXPECT().ListChatsNeedingVerify(gomock.Any()).Return(nil, nil)

	rv := &recordingVerifier{}
	r := newRunner(store, rv.fn, testRunConfig())
	require.NoError(t, r.run(context.Background()))
	assert.Empty(t, rv.calls)
}

func TestRunner_ListErrorFailsRun(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockTeamsChatStore(ctrl)
	store.EXPECT().ListChatsNeedingVerify(gomock.Any()).Return(nil, errors.New("mongo down"))

	rv := &recordingVerifier{}
	r := newRunner(store, rv.fn, testRunConfig())
	require.Error(t, r.run(context.Background()))
}

func TestRunner_MarkVerifiedErrorDoesNotFailRun(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockTeamsChatStore(ctrl)
	store.EXPECT().ListChatsNeedingVerify(gomock.Any()).Return([]model.TeamsChat{
		chat("c-a", "site-a", "alice"),
	}, nil)
	store.EXPECT().MarkVerified(gomock.Any(), gomock.Any()).Return(errors.New("write failed"))

	rv := &recordingVerifier{results: map[string]model.TeamsRoomVerifyResult{
		"c-a": result("c-a", true, 1),
	}}
	r := newRunner(store, rv.fn, testRunConfig())
	require.NoError(t, r.run(context.Background()), "verification is read-only; a repeat next run is harmless")
}

func TestPlanBatches(t *testing.T) {
	chats := []model.TeamsChat{
		chat("c1", "site-a", "alice"),
		chat("c2", "site-b", "bob"),
		chat("c3", "site-a", "carol"),
	}
	got := planBatches(chats, 1)
	require.Len(t, got, 3)
	assert.Equal(t, "site-a", got[0].siteID)
	assert.Equal(t, "c1", got[0].chats[0].ID)
	assert.Equal(t, "site-a", got[1].siteID, "a site's chats stay contiguous in input order")
	assert.Equal(t, "c3", got[1].chats[0].ID)
	assert.Equal(t, "site-b", got[2].siteID)
}
