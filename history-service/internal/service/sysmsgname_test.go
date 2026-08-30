package service

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/history-service/internal/config"
	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/history-service/internal/service/mocks"
	"github.com/hmchangw/chat/pkg/model"
)

func TestExtractRemovedAccount(t *testing.T) {
	tests := []struct {
		name        string
		msgType     string
		msg         string
		wantAccount string
		wantOK      bool
	}{
		{
			name:        "legacy row yields the account prefix",
			msgType:     "members_removed",
			msg:         "bob has been removed from the channel.",
			wantAccount: "bob",
			wantOK:      true,
		},
		{
			name:        "account containing spaces is kept whole",
			msgType:     "members_removed",
			msg:         "bob smith has been removed from the channel.",
			wantAccount: "bob smith",
			wantOK:      true,
		},
		{
			name:    "modern member_removed type is not rewritten",
			msgType: "member_removed",
			msg:     "bob has been removed from the channel.",
			wantOK:  false,
		},
		{
			name:    "ordinary user message with no type is never touched",
			msgType: "",
			msg:     "bob has been removed from the channel.",
			wantOK:  false,
		},
		{
			name:    "missing trailing period does not match",
			msgType: "members_removed",
			msg:     "bob has been removed from the channel",
			wantOK:  false,
		},
		{
			name:    "suffix with no account prefix is not a rewrite candidate",
			msgType: "members_removed",
			msg:     " has been removed from the channel.",
			wantOK:  false,
		},
		{
			name:    "empty msg",
			msgType: "members_removed",
			msg:     "",
			wantOK:  false,
		},
		{
			name:    "unrelated text on the legacy type is left alone",
			msgType: "members_removed",
			msg:     "something else entirely",
			wantOK:  false,
		},
		{
			name:    "suffix in the middle rather than at the end",
			msgType: "members_removed",
			msg:     "bob has been removed from the channel. and then rejoined",
			wantOK:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := models.Message{Type: tc.msgType, Msg: tc.msg}
			account, ok := extractRemovedAccount(&m)
			assert.Equal(t, tc.wantOK, ok)
			assert.Equal(t, tc.wantAccount, account)
		})
	}
}

// newSysMsgNameService wires a service whose only live dependency is the user store.
func newSysMsgNameService(t *testing.T, users UserStore) *HistoryService {
	t.Helper()
	ctrl := gomock.NewController(t)
	return closeOnCleanupIn(t, New(
		mocks.NewMockMessageRepository(ctrl),
		mocks.NewMockSubscriptionRepository(ctrl),
		mocks.NewMockRoomRepository(ctrl),
		mocks.NewMockEventPublisher(ctrl),
		mocks.NewMockThreadRoomRepository(ctrl),
		mocks.NewMockThreadSubscriptionRepository(ctrl),
		users,
		mocks.NewMockAppStore(ctrl),
		&config.Config{MessageHistoryFloorDays: 90, LargeRoomThreshold: 500, MaxPinnedPerRoom: 10},
	))
}

// legacyRemoved builds a legacy members_removed row for the given account.
func legacyRemoved(account string) models.Message {
	return models.Message{Type: "members_removed", Msg: account + " has been removed from the channel."}
}

// The whole point of the pass: many rows, ONE query, accounts deduped.
func TestResolveRemovedMemberNames_OneBatchedQueryForTheWholePage(t *testing.T) {
	ctrl := gomock.NewController(t)
	users := mocks.NewMockUserStore(ctrl)
	users.EXPECT().
		FindUsersByAccounts(gomock.Any(), gomock.Len(2)).
		DoAndReturn(func(_ context.Context, accounts []string) ([]model.User, error) {
			assert.ElementsMatch(t, []string{"bob", "carol"}, accounts)
			return []model.User{
				{Account: "bob", EngName: "Bob", ChineseName: "鮑勃"},
				{Account: "carol", EngName: "Carol"},
			}, nil
		}).
		Times(1)
	s := newSysMsgNameService(t, users)

	msgs := []models.Message{
		legacyRemoved("bob"),
		{Msg: "an ordinary message"},
		legacyRemoved("carol"),
		legacyRemoved("bob"),
	}
	s.resolveRemovedMemberNames(context.Background(), msgs)

	assert.Equal(t, "Bob 鮑勃 has been removed from the channel.", msgs[0].Msg)
	assert.Equal(t, "an ordinary message", msgs[1].Msg)
	assert.Equal(t, "Carol has been removed from the channel.", msgs[2].Msg)
	assert.Equal(t, "Bob 鮑勃 has been removed from the channel.", msgs[3].Msg)
}

// A page with no legacy rows is the overwhelmingly common case: it must not
// touch Mongo at all. gomock fails the test if the store is called.
func TestResolveRemovedMemberNames_NoQualifyingRowsIssuesNoQuery(t *testing.T) {
	ctrl := gomock.NewController(t)
	users := mocks.NewMockUserStore(ctrl)
	s := newSysMsgNameService(t, users)

	msgs := []models.Message{
		{Msg: "hello"},
		{Type: "member_removed", Msg: "bob has been removed from the channel."},
	}
	s.resolveRemovedMemberNames(context.Background(), msgs)

	assert.Equal(t, "hello", msgs[0].Msg)
	assert.Equal(t, "bob has been removed from the channel.", msgs[1].Msg)
}

func TestResolveRemovedMemberNames_EmptySliceIssuesNoQuery(t *testing.T) {
	ctrl := gomock.NewController(t)
	users := mocks.NewMockUserStore(ctrl)
	s := newSysMsgNameService(t, users)

	s.resolveRemovedMemberNames(context.Background(), nil)
	s.resolveRemovedMemberNames(context.Background(), []models.Message{})
}

// A name is a nicety; history is the product. A store failure leaves the rows
// exactly as they read today.
func TestResolveRemovedMemberNames_StoreErrorLeavesRowsUntouched(t *testing.T) {
	ctrl := gomock.NewController(t)
	users := mocks.NewMockUserStore(ctrl)
	users.EXPECT().
		FindUsersByAccounts(gomock.Any(), gomock.Any()).
		Return(nil, errors.New("mongo unavailable")).
		Times(1)
	s := newSysMsgNameService(t, users)

	msgs := []models.Message{legacyRemoved("bob")}
	s.resolveRemovedMemberNames(context.Background(), msgs)

	assert.Equal(t, "bob has been removed from the channel.", msgs[0].Msg)
}

// An account with no user document (deleted, or never migrated) keeps its raw form.
func TestResolveRemovedMemberNames_UnresolvedAccountKeepsRawText(t *testing.T) {
	ctrl := gomock.NewController(t)
	users := mocks.NewMockUserStore(ctrl)
	users.EXPECT().
		FindUsersByAccounts(gomock.Any(), gomock.Any()).
		Return([]model.User{{Account: "bob", EngName: "Bob"}}, nil).
		Times(1)
	s := newSysMsgNameService(t, users)

	msgs := []models.Message{legacyRemoved("bob"), legacyRemoved("ghost")}
	s.resolveRemovedMemberNames(context.Background(), msgs)

	assert.Equal(t, "Bob has been removed from the channel.", msgs[0].Msg)
	assert.Equal(t, "ghost has been removed from the channel.", msgs[1].Msg)
}

// A user document with no names at all must not blank the sentence.
func TestResolveRemovedMemberNames_UserWithNoNamesFallsBackToAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	users := mocks.NewMockUserStore(ctrl)
	users.EXPECT().
		FindUsersByAccounts(gomock.Any(), gomock.Any()).
		Return([]model.User{{Account: "bob"}}, nil).
		Times(1)
	s := newSysMsgNameService(t, users)

	msgs := []models.Message{legacyRemoved("bob")}
	s.resolveRemovedMemberNames(context.Background(), msgs)

	assert.Equal(t, "bob has been removed from the channel.", msgs[0].Msg)
}

// The single-message wrapper serves GetMessageByID and the spliced central row.
func TestResolveRemovedMemberName_SingleMessage(t *testing.T) {
	ctrl := gomock.NewController(t)
	users := mocks.NewMockUserStore(ctrl)
	users.EXPECT().
		FindUsersByAccounts(gomock.Any(), gomock.Len(1)).
		Return([]model.User{{Account: "bob", EngName: "Bob", ChineseName: "鮑勃"}}, nil).
		Times(1)
	s := newSysMsgNameService(t, users)

	m := legacyRemoved("bob")
	s.resolveRemovedMemberName(context.Background(), &m)

	assert.Equal(t, "Bob 鮑勃 has been removed from the channel.", m.Msg)
}

func TestResolveRemovedMemberName_NilAndNonQualifyingAreNoOps(t *testing.T) {
	ctrl := gomock.NewController(t)
	users := mocks.NewMockUserStore(ctrl)
	s := newSysMsgNameService(t, users)

	s.resolveRemovedMemberName(context.Background(), nil)

	m := models.Message{Msg: "hello"}
	s.resolveRemovedMemberName(context.Background(), &m)
	assert.Equal(t, "hello", m.Msg)
}

// A nil user store must degrade, not panic — New accepts one.
func TestResolveRemovedMemberNames_NilStoreDegrades(t *testing.T) {
	s := newSysMsgNameService(t, nil)

	msgs := []models.Message{legacyRemoved("bob")}
	require.NotPanics(t, func() { s.resolveRemovedMemberNames(context.Background(), msgs) })
	assert.Equal(t, "bob has been removed from the channel.", msgs[0].Msg)
}
