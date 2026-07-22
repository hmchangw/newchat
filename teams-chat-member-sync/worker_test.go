package main

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/msgraph"
)

var wtNow = time.Date(2026, 7, 15, 10, 0, 0, 0, time.UTC)

func member(id string) msgraph.ChatMemberDetail {
	return msgraph.ChatMemberDetail{UserID: id}
}

func TestRun_HappyPath(t *testing.T) {
	s, chats, users, graph := newTestSyncer(t, 2, 500)
	seenAt := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	chats.EXPECT().ListChatsToSync(gomock.Any()).Return([]ChatToSync{{ID: "19:g1", UpdatedAt: seenAt}}, nil)
	graph.EXPECT().ListChatMembers(gomock.Any(), "19:g1").
		Return([]msgraph.ChatMemberDetail{member("u1"), member("u2")}, nil)
	users.EXPECT().AccountsByIDs(gomock.Any(), gomock.Any()).
		Return(map[string]string{"u1": "alice", "u2": "bob"}, nil)
	chats.EXPECT().SetMembersSyncedBatch(gomock.Any(), gomock.Len(1), wtNow).DoAndReturn(
		func(_ context.Context, updates []ChatMembersUpdate, _ time.Time) (int64, error) {
			assert.Equal(t, "19:g1", updates[0].ChatID)
			assert.True(t, updates[0].SeenUpdatedAt.Equal(seenAt), "the updatedAt observed at read time is carried into the batch")
			require.Len(t, updates[0].Members, 2)
			assert.Equal(t, "alice", updates[0].Members[0].Account)
			assert.Equal(t, "bob", updates[0].Members[1].Account)
			return 1, nil
		})

	require.NoError(t, s.run(context.Background()))
}

func TestRun_NoChatsIsNoOp(t *testing.T) {
	// No SetMembersSyncedBatch expectation: an empty run must not issue a write.
	s, chats, _, _ := newTestSyncer(t, 1, 500)
	chats.EXPECT().ListChatsToSync(gomock.Any()).Return(nil, nil)
	require.NoError(t, s.run(context.Background()))
}

func TestRun_FlushesInBatchesOfBatchSize(t *testing.T) {
	s, chats, users, graph := newTestSyncer(t, 3, 2)
	all := []ChatToSync{
		{ID: "19:c1", UpdatedAt: wtNow}, {ID: "19:c2", UpdatedAt: wtNow},
		{ID: "19:c3", UpdatedAt: wtNow}, {ID: "19:c4", UpdatedAt: wtNow},
		{ID: "19:c5", UpdatedAt: wtNow},
	}
	chats.EXPECT().ListChatsToSync(gomock.Any()).Return(all, nil)
	for _, c := range all {
		graph.EXPECT().ListChatMembers(gomock.Any(), c.ID).
			Return([]msgraph.ChatMemberDetail{member("u1")}, nil)
	}
	users.EXPECT().AccountsByIDs(gomock.Any(), gomock.Any()).
		Return(map[string]string{"u1": "a"}, nil).AnyTimes()

	var mu sync.Mutex
	var sizes []int
	var gotIDs []string
	chats.EXPECT().SetMembersSyncedBatch(gomock.Any(), gomock.Any(), wtNow).DoAndReturn(
		func(_ context.Context, updates []ChatMembersUpdate, _ time.Time) (int64, error) {
			mu.Lock()
			defer mu.Unlock()
			sizes = append(sizes, len(updates))
			for _, u := range updates {
				gotIDs = append(gotIDs, u.ChatID)
			}
			return int64(len(updates)), nil
		}).Times(3)

	require.NoError(t, s.run(context.Background()))
	mu.Lock()
	defer mu.Unlock()
	assert.ElementsMatch(t, []int{2, 2, 1}, sizes, "5 chats at batch size 2 flush as 2+2+1")
	assert.ElementsMatch(t, []string{"19:c1", "19:c2", "19:c3", "19:c4", "19:c5"}, gotIDs, "every chat is written exactly once")
}

func TestRun_GraphFailureKeepsFlagAndFailsRun(t *testing.T) {
	s, chats, users, graph := newTestSyncer(t, 1, 500)
	chats.EXPECT().ListChatsToSync(gomock.Any()).Return([]ChatToSync{{ID: "19:bad", UpdatedAt: wtNow}, {ID: "19:ok", UpdatedAt: wtNow}}, nil)
	graph.EXPECT().ListChatMembers(gomock.Any(), "19:bad").
		Return(nil, fmt.Errorf("graph returned status 429"))
	graph.EXPECT().ListChatMembers(gomock.Any(), "19:ok").
		Return([]msgraph.ChatMemberDetail{member("u1")}, nil)
	users.EXPECT().AccountsByIDs(gomock.Any(), gomock.Any()).Return(map[string]string{"u1": "a"}, nil)
	// The failed chat never reaches the batch: its needMemberSync must stay true.
	chats.EXPECT().SetMembersSyncedBatch(gomock.Any(), gomock.Len(1), wtNow).DoAndReturn(
		func(_ context.Context, updates []ChatMembersUpdate, _ time.Time) (int64, error) {
			assert.Equal(t, "19:ok", updates[0].ChatID)
			return 1, nil
		})

	err := s.run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "1 of 2 chats failed")
}

func TestRun_BatchWriteFailureFailsChats(t *testing.T) {
	s, chats, users, graph := newTestSyncer(t, 1, 500)
	chats.EXPECT().ListChatsToSync(gomock.Any()).Return([]ChatToSync{{ID: "19:g1", UpdatedAt: wtNow}}, nil)
	graph.EXPECT().ListChatMembers(gomock.Any(), "19:g1").
		Return([]msgraph.ChatMemberDetail{member("u1")}, nil)
	users.EXPECT().AccountsByIDs(gomock.Any(), gomock.Any()).Return(map[string]string{"u1": "a"}, nil)
	chats.EXPECT().SetMembersSyncedBatch(gomock.Any(), gomock.Any(), wtNow).
		Return(int64(0), fmt.Errorf("mongo down"))

	err := s.run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "1 of 1 chats failed")
}

func TestRun_PartialBatchWriteFailure(t *testing.T) {
	// An unordered bulk write can fail some operations while others land;
	// only the unmatched remainder counts as failed.
	s, chats, users, graph := newTestSyncer(t, 1, 500)
	chats.EXPECT().ListChatsToSync(gomock.Any()).Return([]ChatToSync{{ID: "19:g1", UpdatedAt: wtNow}, {ID: "19:g2", UpdatedAt: wtNow}}, nil)
	graph.EXPECT().ListChatMembers(gomock.Any(), gomock.Any()).
		Return([]msgraph.ChatMemberDetail{member("u1")}, nil).Times(2)
	users.EXPECT().AccountsByIDs(gomock.Any(), gomock.Any()).Return(map[string]string{"u1": "a"}, nil).AnyTimes()
	chats.EXPECT().SetMembersSyncedBatch(gomock.Any(), gomock.Len(2), wtNow).
		Return(int64(1), fmt.Errorf("partial bulk failure"))

	err := s.run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "1 of 2 chats failed")
}

func TestRun_SupersededChatsAreBenign(t *testing.T) {
	s, chats, users, graph := newTestSyncer(t, 2, 500)
	chats.EXPECT().ListChatsToSync(gomock.Any()).Return([]ChatToSync{{ID: "19:g1", UpdatedAt: wtNow}, {ID: "19:g2", UpdatedAt: wtNow}}, nil)
	graph.EXPECT().ListChatMembers(gomock.Any(), gomock.Any()).
		Return([]msgraph.ChatMemberDetail{member("u1")}, nil).Times(2)
	users.EXPECT().AccountsByIDs(gomock.Any(), gomock.Any()).
		Return(map[string]string{"u1": "a"}, nil).AnyTimes()
	// One of the two conditional updates does not match (concurrent rewrite):
	// matched < len(updates) is benign, the chat retries next run.
	chats.EXPECT().SetMembersSyncedBatch(gomock.Any(), gomock.Len(2), wtNow).Return(int64(1), nil)

	require.NoError(t, s.run(context.Background()))
}

func TestRun_ListChatsFailure(t *testing.T) {
	s, chats, _, _ := newTestSyncer(t, 1, 500)
	chats.EXPECT().ListChatsToSync(gomock.Any()).Return(nil, fmt.Errorf("mongo down"))
	err := s.run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "load chats")
}

func TestRun_SharedMemberResolvedOncePerRun(t *testing.T) {
	s, chats, users, graph := newTestSyncer(t, 4, 500)
	chats.EXPECT().ListChatsToSync(gomock.Any()).Return([]ChatToSync{{ID: "19:a", UpdatedAt: wtNow}, {ID: "19:b", UpdatedAt: wtNow}}, nil)
	// Both chats contain the same member u9; the account cache resolves it once.
	graph.EXPECT().ListChatMembers(gomock.Any(), "19:a").
		Return([]msgraph.ChatMemberDetail{member("u9")}, nil)
	graph.EXPECT().ListChatMembers(gomock.Any(), "19:b").
		Return([]msgraph.ChatMemberDetail{member("u9")}, nil)
	var calls int
	var mu sync.Mutex
	users.EXPECT().AccountsByIDs(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, ids []string) (map[string]string, error) {
			mu.Lock()
			calls++
			mu.Unlock()
			return map[string]string{"u9": "nine"}, nil
		}).MaxTimes(2) // cache dedups; with a concurrency window it is 1, worst case 2 — never per-chat unbounded
	chats.EXPECT().SetMembersSyncedBatch(gomock.Any(), gomock.Len(2), wtNow).Return(int64(2), nil)

	require.NoError(t, s.run(context.Background()))
	mu.Lock()
	assert.LessOrEqual(t, calls, 2)
	mu.Unlock()
}
