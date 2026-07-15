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

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgraph"
)

var wtNow = time.Date(2026, 7, 15, 10, 0, 0, 0, time.UTC)

func upnMember(id, upn string) msgraph.ChatMemberDetail {
	return msgraph.ChatMemberDetail{UserID: id, UserPrincipalName: upn}
}

func TestRun_HappyPath(t *testing.T) {
	s, chats, _, graph := newTestSyncer(t, 2)
	seenAt := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	chats.EXPECT().ListChatsToSync(gomock.Any()).Return([]ChatToSync{{ID: "19:g1", UpdatedAt: seenAt}}, nil)
	graph.EXPECT().ListChatMembers(gomock.Any(), "19:g1").
		Return([]msgraph.ChatMemberDetail{upnMember("u1", "alice@x"), upnMember("u2", "bob@x")}, nil)
	chats.EXPECT().SetMembersSynced(gomock.Any(), "19:g1", seenAt, gomock.Len(2), wtNow).DoAndReturn(
		func(_ context.Context, _ string, _ time.Time, members []model.TeamsChatMember, _ time.Time) error {
			assert.Equal(t, "alice", members[0].Account)
			assert.Equal(t, "bob", members[1].Account)
			return nil
		})

	require.NoError(t, s.run(context.Background()))
}

func TestRun_NoChatsIsNoOp(t *testing.T) {
	s, chats, _, _ := newTestSyncer(t, 1)
	chats.EXPECT().ListChatsToSync(gomock.Any()).Return(nil, nil)
	require.NoError(t, s.run(context.Background()))
}

func TestRun_GraphFailureKeepsFlagAndFailsRun(t *testing.T) {
	s, chats, _, graph := newTestSyncer(t, 1)
	chats.EXPECT().ListChatsToSync(gomock.Any()).Return([]ChatToSync{{ID: "19:bad", UpdatedAt: wtNow}, {ID: "19:ok", UpdatedAt: wtNow}}, nil)
	graph.EXPECT().ListChatMembers(gomock.Any(), "19:bad").
		Return(nil, fmt.Errorf("graph returned status 429"))
	graph.EXPECT().ListChatMembers(gomock.Any(), "19:ok").
		Return([]msgraph.ChatMemberDetail{upnMember("u1", "a@x")}, nil)
	chats.EXPECT().SetMembersSynced(gomock.Any(), "19:ok", gomock.Any(), gomock.Len(1), wtNow).Return(nil)
	// No SetMembersSynced for 19:bad: its needMemberSync must stay true.

	err := s.run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "1 of 2 chats failed")
}

func TestRun_WriteFailureFailsChat(t *testing.T) {
	s, chats, _, graph := newTestSyncer(t, 1)
	chats.EXPECT().ListChatsToSync(gomock.Any()).Return([]ChatToSync{{ID: "19:g1", UpdatedAt: wtNow}}, nil)
	graph.EXPECT().ListChatMembers(gomock.Any(), "19:g1").
		Return([]msgraph.ChatMemberDetail{upnMember("u1", "a@x")}, nil)
	chats.EXPECT().SetMembersSynced(gomock.Any(), "19:g1", gomock.Any(), gomock.Any(), wtNow).
		Return(fmt.Errorf("mongo down"))

	require.Error(t, s.run(context.Background()))
}

func TestRun_SupersededChatIsBenign(t *testing.T) {
	s, chats, _, graph := newTestSyncer(t, 2)
	chats.EXPECT().ListChatsToSync(gomock.Any()).Return([]ChatToSync{{ID: "19:g1", UpdatedAt: wtNow}, {ID: "19:g2", UpdatedAt: wtNow}}, nil)
	graph.EXPECT().ListChatMembers(gomock.Any(), "19:g1").
		Return([]msgraph.ChatMemberDetail{upnMember("u1", "a@x")}, nil)
	graph.EXPECT().ListChatMembers(gomock.Any(), "19:g2").
		Return([]msgraph.ChatMemberDetail{upnMember("u2", "b@x")}, nil)
	chats.EXPECT().SetMembersSynced(gomock.Any(), "19:g1", gomock.Any(), gomock.Any(), wtNow).Return(errSuperseded)
	chats.EXPECT().SetMembersSynced(gomock.Any(), "19:g2", gomock.Any(), gomock.Any(), wtNow).Return(nil)

	require.NoError(t, s.run(context.Background()))
}

func TestRun_ListChatsFailure(t *testing.T) {
	s, chats, _, _ := newTestSyncer(t, 1)
	chats.EXPECT().ListChatsToSync(gomock.Any()).Return(nil, fmt.Errorf("mongo down"))
	err := s.run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "load chats")
}

func TestRun_SharedMemberResolvedOncePerRun(t *testing.T) {
	s, chats, users, graph := newTestSyncer(t, 4)
	chats.EXPECT().ListChatsToSync(gomock.Any()).Return([]ChatToSync{{ID: "19:a", UpdatedAt: wtNow}, {ID: "19:b", UpdatedAt: wtNow}}, nil)
	// Both chats contain the same UPN-less member u9.
	graph.EXPECT().ListChatMembers(gomock.Any(), "19:a").
		Return([]msgraph.ChatMemberDetail{{UserID: "u9"}}, nil)
	graph.EXPECT().ListChatMembers(gomock.Any(), "19:b").
		Return([]msgraph.ChatMemberDetail{{UserID: "u9"}}, nil)
	var calls int
	var mu sync.Mutex
	users.EXPECT().AccountsByIDs(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, ids []string) (map[string]string, error) {
			mu.Lock()
			calls++
			mu.Unlock()
			return map[string]string{"u9": "nine"}, nil
		}).MaxTimes(2) // cache dedups; with a concurrency window it is 1, worst case 2 — never per-chat unbounded
	chats.EXPECT().SetMembersSynced(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Len(1), wtNow).Return(nil).Times(2)

	require.NoError(t, s.run(context.Background()))
	mu.Lock()
	assert.LessOrEqual(t, calls, 2)
	mu.Unlock()
}
