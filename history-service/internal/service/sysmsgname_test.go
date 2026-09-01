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
			name:        "legacy row yields the account inside the quotes",
			msgType:     "members_removed",
			msg:         `"bob" has been removed from the channel.`,
			wantAccount: "bob",
			wantOK:      true,
		},
		{
			name:        "account containing spaces is kept whole",
			msgType:     "members_removed",
			msg:         `"bob smith" has been removed from the channel.`,
			wantAccount: "bob smith",
			wantOK:      true,
		},
		{
			name:    "unquoted account does not match",
			msgType: "members_removed",
			msg:     "bob has been removed from the channel.",
			wantOK:  false,
		},
		{
			name:    "modern member_removed type is not rewritten",
			msgType: "member_removed",
			msg:     `"bob" has been removed from the channel.`,
			wantOK:  false,
		},
		{
			name:    "ordinary user message with no type is never touched",
			msgType: "",
			msg:     `"bob" has been removed from the channel.`,
			wantOK:  false,
		},
		{
			name:    "missing trailing period does not match",
			msgType: "members_removed",
			msg:     `"bob" has been removed from the channel`,
			wantOK:  false,
		},
		{
			name:    "empty quotes carry no account",
			msgType: "members_removed",
			msg:     `"" has been removed from the channel.`,
			wantOK:  false,
		},
		{
			name:    "opening quote only",
			msgType: "members_removed",
			msg:     `"bob has been removed from the channel.`,
			wantOK:  false,
		},
		{
			name:    "closing quote only",
			msgType: "members_removed",
			msg:     `bob" has been removed from the channel.`,
			wantOK:  false,
		},
		{
			name:    "a lone quote is not an empty quoted pair",
			msgType: "members_removed",
			msg:     `" has been removed from the channel.`,
			wantOK:  false,
		},
		{
			name:    "suffix with no prefix at all",
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
			msg:     `"bob" has been removed from the channel. and then rejoined`,
			wantOK:  false,
		},
		{
			name:        "an account that itself contains a quote keeps its inner quote",
			msgType:     "members_removed",
			msg:         `"bo"b" has been removed from the channel.`,
			wantAccount: `bo"b`,
			wantOK:      true,
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
	return newSysMsgNameServiceWith(t, users, mocks.NewMockAppStore(gomock.NewController(t)))
}

// newSysMsgNameServiceWith is the two-store form, for the bot cases. Each call builds
// its own service, so the cache behind s.appName is per-test and lookup counts are real.
func newSysMsgNameServiceWith(t *testing.T, users UserStore, apps AppStore) *HistoryService {
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
		apps,
		&config.Config{MessageHistoryFloorDays: 90, LargeRoomThreshold: 500, MaxPinnedPerRoom: 10},
	))
}

// legacyRemoved builds a legacy members_removed row for the given account.
func legacyRemoved(account string) models.Message {
	return models.Message{Type: "members_removed", Msg: `"` + account + `" has been removed from the channel.`}
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

	assert.Equal(t, `"Bob 鮑勃" has been removed from the channel.`, msgs[0].Msg)
	assert.Equal(t, "an ordinary message", msgs[1].Msg)
	assert.Equal(t, `"Carol" has been removed from the channel.`, msgs[2].Msg)
	assert.Equal(t, `"Bob 鮑勃" has been removed from the channel.`, msgs[3].Msg)
}

// A page with no legacy rows is the overwhelmingly common case: it must not
// touch Mongo at all. gomock fails the test if the store is called.
func TestResolveRemovedMemberNames_NoQualifyingRowsIssuesNoQuery(t *testing.T) {
	ctrl := gomock.NewController(t)
	users := mocks.NewMockUserStore(ctrl)
	s := newSysMsgNameService(t, users)

	msgs := []models.Message{
		{Msg: "hello"},
		{Type: "member_removed", Msg: `"bob" has been removed from the channel.`},
	}
	s.resolveRemovedMemberNames(context.Background(), msgs)

	assert.Equal(t, "hello", msgs[0].Msg)
	assert.Equal(t, `"bob" has been removed from the channel.`, msgs[1].Msg)
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

	assert.Equal(t, `"bob" has been removed from the channel.`, msgs[0].Msg)
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

	assert.Equal(t, `"Bob" has been removed from the channel.`, msgs[0].Msg)
	assert.Equal(t, `"ghost" has been removed from the channel.`, msgs[1].Msg)
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

	assert.Equal(t, `"bob" has been removed from the channel.`, msgs[0].Msg)
}

// The single-message wrapper serves GetMessageByID and the spliced central row.
func TestNormalizeLegacySysMsg_SingleUserMessage(t *testing.T) {
	ctrl := gomock.NewController(t)
	users := mocks.NewMockUserStore(ctrl)
	users.EXPECT().
		FindUsersByAccounts(gomock.Any(), gomock.Len(1)).
		Return([]model.User{{Account: "bob", EngName: "Bob", ChineseName: "鮑勃"}}, nil).
		Times(1)
	s := newSysMsgNameService(t, users)

	m := legacyRemoved("bob")
	s.normalizeLegacySysMsg(context.Background(), &m)

	assert.Equal(t, `"Bob 鮑勃" has been removed from the channel.`, m.Msg)
	assert.Equal(t, model.MessageTypeMemberRemoved, m.Type)
}

// A nil user store must degrade, not panic — New accepts one.
func TestResolveRemovedMemberNames_NilStoreDegrades(t *testing.T) {
	s := newSysMsgNameService(t, nil)

	msgs := []models.Message{legacyRemoved("bob")}
	require.NotPanics(t, func() { s.resolveRemovedMemberNames(context.Background(), msgs) })
	assert.Equal(t, `"bob" has been removed from the channel.`, msgs[0].Msg)
}

// Migrated rows carry plural types no client knows; the wire must show the modern
// ones. The rewrite is unconditional — it does not care whether the row's text
// matched the name-resolution suffix.
func TestNormalizeLegacySysMsgTypes(t *testing.T) {
	tests := []struct {
		name     string
		msgType  string
		msg      string
		wantType string
	}{
		{
			name:     "legacy members_removed becomes member_removed",
			msgType:  "members_removed",
			msg:      `"bob" has been removed from the channel.`,
			wantType: model.MessageTypeMemberRemoved,
		},
		{
			name:     "legacy members_removed is normalized even when its text never matched",
			msgType:  "members_removed",
			msg:      "something else entirely",
			wantType: model.MessageTypeMemberRemoved,
		},
		{
			name:     "legacy members_left becomes member_left",
			msgType:  "members_left",
			msg:      `"bob" has left the channel.`,
			wantType: model.MessageTypeMemberLeft,
		},
		{
			name:     "modern member_removed is already correct",
			msgType:  model.MessageTypeMemberRemoved,
			msg:      `"bob" has been removed from the channel.`,
			wantType: model.MessageTypeMemberRemoved,
		},
		{
			name:     "modern member_left is already correct",
			msgType:  model.MessageTypeMemberLeft,
			msg:      "anything",
			wantType: model.MessageTypeMemberLeft,
		},
		{
			name:     "an unrelated system type is left alone",
			msgType:  model.MessageTypeMembersAdded,
			msg:      "anything",
			wantType: model.MessageTypeMembersAdded,
		},
		{
			name:     "an ordinary user message keeps its empty type",
			msgType:  "",
			msg:      "hello",
			wantType: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msgs := []models.Message{{Type: tc.msgType, Msg: tc.msg}}
			normalizeLegacySysMsgTypes(msgs)
			assert.Equal(t, tc.wantType, msgs[0].Type)
			assert.Equal(t, tc.msg, msgs[0].Msg, "the type rewrite must never touch the body")
		})
	}
}

func TestNormalizeLegacySysMsgTypes_EmptyAndNilAreNoOps(t *testing.T) {
	require.NotPanics(t, func() {
		normalizeLegacySysMsgTypes(nil)
		normalizeLegacySysMsgTypes([]models.Message{})
	})
}

// A bot account usually has no user document at all, so the users batch alone
// leaves the row raw. The app name is what makes it readable.
func TestResolveRemovedMemberNames_BotResolvesAppName(t *testing.T) {
	ctrl := gomock.NewController(t)
	users := mocks.NewMockUserStore(ctrl)
	users.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"helper.bot"}).Return(nil, nil).Times(1)
	apps := mocks.NewMockAppStore(ctrl)
	apps.EXPECT().AppNameByAccount(gomock.Any(), "helper.bot").Return("Helper Bot", nil).Times(1)
	s := newSysMsgNameServiceWith(t, users, apps)

	msgs := []models.Message{legacyRemoved("helper.bot")}
	s.resolveRemovedMemberNames(context.Background(), msgs)

	assert.Equal(t, `"Helper Bot" has been removed from the channel.`, msgs[0].Msg)
}

// When a bot does have a user document, the registered app name still wins — the
// same precedence room-worker applies on the live path.
func TestResolveRemovedMemberNames_BotWithUserDocPrefersAppName(t *testing.T) {
	ctrl := gomock.NewController(t)
	users := mocks.NewMockUserStore(ctrl)
	users.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"helper.bot"}).
		Return([]model.User{{Account: "helper.bot", EngName: "Helper"}}, nil).Times(1)
	apps := mocks.NewMockAppStore(ctrl)
	apps.EXPECT().AppNameByAccount(gomock.Any(), "helper.bot").Return("Helper Bot", nil).Times(1)
	s := newSysMsgNameServiceWith(t, users, apps)

	msgs := []models.Message{legacyRemoved("helper.bot")}
	s.resolveRemovedMemberNames(context.Background(), msgs)

	assert.Equal(t, `"Helper Bot" has been removed from the channel.`, msgs[0].Msg)
}

// No app row: fall back to whatever the user document composes to, exactly as a
// non-bot account would.
func TestResolveRemovedMemberNames_BotWithNoAppRowFallsBackToUserDoc(t *testing.T) {
	ctrl := gomock.NewController(t)
	users := mocks.NewMockUserStore(ctrl)
	users.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"helper.bot"}).
		Return([]model.User{{Account: "helper.bot", EngName: "Helper"}}, nil).Times(1)
	apps := mocks.NewMockAppStore(ctrl)
	apps.EXPECT().AppNameByAccount(gomock.Any(), "helper.bot").Return("", nil).Times(1)
	s := newSysMsgNameServiceWith(t, users, apps)

	msgs := []models.Message{legacyRemoved("helper.bot")}
	s.resolveRemovedMemberNames(context.Background(), msgs)

	assert.Equal(t, `"Helper" has been removed from the channel.`, msgs[0].Msg)
}

// Neither an app row nor a user document: the sentence must read as it does today.
func TestResolveRemovedMemberNames_BotWithNothingToResolveKeepsRawAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	users := mocks.NewMockUserStore(ctrl)
	users.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"helper.bot"}).Return(nil, nil).Times(1)
	apps := mocks.NewMockAppStore(ctrl)
	apps.EXPECT().AppNameByAccount(gomock.Any(), "helper.bot").Return("", nil).Times(1)
	s := newSysMsgNameServiceWith(t, users, apps)

	msgs := []models.Message{legacyRemoved("helper.bot")}
	s.resolveRemovedMemberNames(context.Background(), msgs)

	assert.Equal(t, `"helper.bot" has been removed from the channel.`, msgs[0].Msg)
}

// An apps read failure is logged and swallowed, never surfaced.
func TestResolveRemovedMemberNames_BotAppLookupErrorKeepsRawAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	users := mocks.NewMockUserStore(ctrl)
	users.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"helper.bot"}).Return(nil, nil).Times(1)
	apps := mocks.NewMockAppStore(ctrl)
	apps.EXPECT().AppNameByAccount(gomock.Any(), "helper.bot").
		Return("", errors.New("mongo unavailable")).Times(1)
	s := newSysMsgNameServiceWith(t, users, apps)

	msgs := []models.Message{legacyRemoved("helper.bot")}
	s.resolveRemovedMemberNames(context.Background(), msgs)

	assert.Equal(t, `"helper.bot" has been removed from the channel.`, msgs[0].Msg)
}

// A users failure must not abandon the bot half of the page: the two collections
// are read independently, so one being down does not blind the other.
func TestResolveRemovedMemberNames_UserStoreErrorStillResolvesBots(t *testing.T) {
	ctrl := gomock.NewController(t)
	users := mocks.NewMockUserStore(ctrl)
	users.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).
		Return(nil, errors.New("mongo unavailable")).Times(1)
	apps := mocks.NewMockAppStore(ctrl)
	apps.EXPECT().AppNameByAccount(gomock.Any(), "helper.bot").Return("Helper Bot", nil).Times(1)
	s := newSysMsgNameServiceWith(t, users, apps)

	msgs := []models.Message{legacyRemoved("helper.bot"), legacyRemoved("bob")}
	s.resolveRemovedMemberNames(context.Background(), msgs)

	assert.Equal(t, `"Helper Bot" has been removed from the channel.`, msgs[0].Msg)
	assert.Equal(t, `"bob" has been removed from the channel.`, msgs[1].Msg)
}

// The point of the whole pass: one users batch for the page, and one apps read per
// DISTINCT bot however many rows that bot occupies.
func TestResolveRemovedMemberNames_MixedPageOneBatchOneReadPerBot(t *testing.T) {
	ctrl := gomock.NewController(t)
	users := mocks.NewMockUserStore(ctrl)
	users.EXPECT().
		FindUsersByAccounts(gomock.Any(), gomock.Len(2)).
		DoAndReturn(func(_ context.Context, accounts []string) ([]model.User, error) {
			assert.ElementsMatch(t, []string{"bob", "helper.bot"}, accounts)
			return []model.User{{Account: "bob", EngName: "Bob", ChineseName: "鮑勃"}}, nil
		}).
		Times(1)
	apps := mocks.NewMockAppStore(ctrl)
	apps.EXPECT().AppNameByAccount(gomock.Any(), "helper.bot").Return("Helper Bot", nil).Times(1)
	s := newSysMsgNameServiceWith(t, users, apps)

	msgs := []models.Message{
		legacyRemoved("bob"),
		legacyRemoved("helper.bot"),
		{Msg: "an ordinary message"},
		legacyRemoved("bob"),
		legacyRemoved("helper.bot"),
	}
	s.resolveRemovedMemberNames(context.Background(), msgs)

	assert.Equal(t, `"Bob 鮑勃" has been removed from the channel.`, msgs[0].Msg)
	assert.Equal(t, `"Helper Bot" has been removed from the channel.`, msgs[1].Msg)
	assert.Equal(t, "an ordinary message", msgs[2].Msg)
	assert.Equal(t, `"Bob 鮑勃" has been removed from the channel.`, msgs[3].Msg)
	assert.Equal(t, `"Helper Bot" has been removed from the channel.`, msgs[4].Msg)
}

// A nil user store degrades to the bot half rather than skipping the pass wholesale.
func TestResolveRemovedMemberNames_NilUserStoreStillResolvesBots(t *testing.T) {
	ctrl := gomock.NewController(t)
	apps := mocks.NewMockAppStore(ctrl)
	apps.EXPECT().AppNameByAccount(gomock.Any(), "helper.bot").Return("Helper Bot", nil).Times(1)
	s := newSysMsgNameServiceWith(t, nil, apps)

	msgs := []models.Message{legacyRemoved("helper.bot"), legacyRemoved("bob")}
	require.NotPanics(t, func() { s.resolveRemovedMemberNames(context.Background(), msgs) })

	assert.Equal(t, `"Helper Bot" has been removed from the channel.`, msgs[0].Msg)
	assert.Equal(t, `"bob" has been removed from the channel.`, msgs[1].Msg)
}

// The two passes are ordered, and this test is what pins the order: the text
// rewrite gates on the LEGACY plural type, so normalizing the type first would
// leave every legacy sentence stuck with its raw account.
func TestNormalizeLegacySysMsgs_ResolvesNamesThenNormalizesTypes(t *testing.T) {
	ctrl := gomock.NewController(t)
	users := mocks.NewMockUserStore(ctrl)
	users.EXPECT().
		FindUsersByAccounts(gomock.Any(), gomock.Len(2)).
		Return([]model.User{{Account: "bob", EngName: "Bob", ChineseName: "鮑勃"}}, nil).
		Times(1)
	apps := mocks.NewMockAppStore(ctrl)
	apps.EXPECT().AppNameByAccount(gomock.Any(), "helper.bot").Return("Helper Bot", nil).Times(1)
	s := newSysMsgNameServiceWith(t, users, apps)

	left := models.Message{Type: "members_left", Msg: `"bob" has left the channel.`}
	msgs := []models.Message{legacyRemoved("bob"), legacyRemoved("helper.bot"), left}
	s.normalizeLegacySysMsgs(context.Background(), msgs)

	assert.Equal(t, model.MessageTypeMemberRemoved, msgs[0].Type)
	assert.Equal(t, `"Bob 鮑勃" has been removed from the channel.`, msgs[0].Msg)

	assert.Equal(t, model.MessageTypeMemberRemoved, msgs[1].Type)
	assert.Equal(t, `"Helper Bot" has been removed from the channel.`, msgs[1].Msg)

	// members_left is type-only: its stored sentence is returned verbatim.
	assert.Equal(t, model.MessageTypeMemberLeft, msgs[2].Type)
	assert.Equal(t, `"bob" has left the channel.`, msgs[2].Msg)
}

// The one-message form resolves a bot through `apps` just as the page form does.
func TestNormalizeLegacySysMsg_SingleBotMessage(t *testing.T) {
	ctrl := gomock.NewController(t)
	users := mocks.NewMockUserStore(ctrl)
	users.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Len(1)).Return(nil, nil).Times(1)
	apps := mocks.NewMockAppStore(ctrl)
	apps.EXPECT().AppNameByAccount(gomock.Any(), "helper.bot").Return("Helper Bot", nil).Times(1)
	s := newSysMsgNameServiceWith(t, users, apps)

	m := legacyRemoved("helper.bot")
	s.normalizeLegacySysMsg(context.Background(), &m)

	assert.Equal(t, model.MessageTypeMemberRemoved, m.Type)
	assert.Equal(t, `"Helper Bot" has been removed from the channel.`, m.Msg)
}

func TestNormalizeLegacySysMsg_NilAndNonQualifyingAreNoOps(t *testing.T) {
	ctrl := gomock.NewController(t)
	users := mocks.NewMockUserStore(ctrl)
	s := newSysMsgNameService(t, users)

	require.NotPanics(t, func() { s.normalizeLegacySysMsg(context.Background(), nil) })

	m := models.Message{Msg: "hello"}
	s.normalizeLegacySysMsg(context.Background(), &m)
	assert.Equal(t, "hello", m.Msg)
	assert.Equal(t, "", m.Type)
}
