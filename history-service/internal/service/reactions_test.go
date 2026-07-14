package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/history-service/internal/service/mocks"
	"github.com/hmchangw/chat/pkg/displayfmt"
	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/userstore"
)

type reactFixture struct {
	svc interface {
		ReactMessage(c *natsrouter.Context, siteID string, req models.ReactMessageRequest) (*models.ReactMessageResponse, error)
	}
	msgs  *mocks.MockMessageRepository
	subs  *mocks.MockSubscriptionRepository
	pub   *mocks.MockEventPublisher
	users *mocks.MockUserStore
	apps  *mocks.MockAppStore
}

func newReactFixture(t *testing.T) reactFixture {
	svc, msgs, subs, rooms, pub, _, users, apps := newServiceWithRoomMock(t)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	rooms.EXPECT().GetRoomTimes(gomock.Any(), gomock.Any()).Return(defaultRoomLastMsgAt, defaultRoomCreatedAt, nil).AnyTimes()
	return reactFixture{svc: svc, msgs: msgs, subs: subs, pub: pub, users: users, apps: apps}
}

func aliceUser() model.User {
	return model.User{
		ID:          "user-alice",
		Account:     "u1",
		SiteID:      "site-test",
		EngName:     "Alice",
		ChineseName: "Alice CN",
	}
}

func aliceUserPtr() *model.User {
	u := aliceUser()
	return &u
}

func TestHistoryService_ReactMessage_NotSubscribed(t *testing.T) {
	f := newReactFixture(t)
	f.subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, false, nil)

	resp, err := f.svc.ReactMessage(testContext(), "site-test",
		models.ReactMessageRequest{MessageID: "m1", Shortcode: "thumbsup"})
	assert.Nil(t, resp)
	assertForbiddenErr(t, err, "not subscribed to room")
}

// Every pre-storage rejection path: empty fields, regex fail. Shortcode acceptance
// is format-only (emoji.Canonicalize) — there is no registration lookup to fail.
func TestHistoryService_ReactMessage_ValidationErrors(t *testing.T) {
	cases := []struct {
		name         string
		shortcode    string
		messageID    string
		wantCategory errcode.Code // CodeBadRequest
		wantMsg      string
	}{
		{
			name:      "empty messageID",
			shortcode: "thumbsup", messageID: "",
			wantCategory: errcode.CodeBadRequest,
			wantMsg:      "messageId is required",
		},
		{
			name:      "empty shortcode",
			shortcode: "", messageID: "m1",
			wantCategory: errcode.CodeBadRequest,
			wantMsg:      "shortcode is required",
		},
		{
			name:      "invalid shortcode format (colons)",
			shortcode: ":thumbsup:", messageID: "m1",
			wantCategory: errcode.CodeBadRequest,
			wantMsg:      "invalid reaction shortcode",
		},
		{
			name:      "invalid shortcode format (uppercase)",
			shortcode: "ThumbsUp", messageID: "m1",
			wantCategory: errcode.CodeBadRequest,
			wantMsg:      "invalid reaction shortcode",
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			f := newReactFixture(t)
			resp, err := f.svc.ReactMessage(testContext(), "site-test",
				models.ReactMessageRequest{MessageID: tc.messageID, Shortcode: tc.shortcode})
			assert.Nil(t, resp)
			switch tc.wantCategory {
			case errcode.CodeBadRequest:
				assertBadRequestErr(t, err, tc.wantMsg)
			default:
				t.Fatalf("unhandled category: %s", tc.wantCategory)
			}
		})
	}
}

// TestHistoryService_ReactMessage_UnregisteredShortcode_Accepted covers the
// behavior flip: a well-formed shortcode that is neither a standard emoji nor
// registered in any site's custom_emojis collection is now accepted — format
// validation (emoji.Canonicalize) is the only gate.
func TestHistoryService_ReactMessage_UnregisteredShortcode_Accepted(t *testing.T) {
	f := newReactFixture(t)
	createdAt := joinTime.Add(1 * time.Minute)
	f.subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	target := &models.Message{
		MessageID: "m1",
		RoomID:    "r1",
		CreatedAt: createdAt,
		Sender:    models.Participant{ID: "user-bob", Account: "bob"},
	}
	f.msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(target, nil)
	f.users.EXPECT().FindUserByAccount(gomock.Any(), "u1").Return(aliceUserPtr(), nil)

	expectedKey := models.ReactionKey{Emoji: "totally_unknown_emoji", UserAccount: "u1"}
	f.msgs.EXPECT().
		AddReaction(gomock.Any(), target, expectedKey, gomock.Any()).
		Return(nil)
	f.pub.EXPECT().Publish(gomock.Any(), subject.MsgCanonicalReacted("site-test"), gomock.Any(), gomock.Any()).Return(nil)

	resp, err := f.svc.ReactMessage(testContext(), "site-test",
		models.ReactMessageRequest{MessageID: "m1", Shortcode: "totally_unknown_emoji"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, model.ReactionActionAdded, resp.Action)
	assert.Equal(t, "totally_unknown_emoji", resp.Shortcode)
}

func TestHistoryService_ReactMessage_MessageNotFound(t *testing.T) {
	f := newReactFixture(t)
	f.subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	f.msgs.EXPECT().GetMessageByID(gomock.Any(), "missing").Return(nil, nil)
	resp, err := f.svc.ReactMessage(testContext(), "site-test",
		models.ReactMessageRequest{MessageID: "missing", Shortcode: "thumbsup"})
	assert.Nil(t, resp)
	assertNotFoundErr(t, err, "message not found")
}

func TestHistoryService_ReactMessage_AddOnDeleted_Blocked(t *testing.T) {
	f := newReactFixture(t)
	createdAt := joinTime.Add(1 * time.Minute)
	f.subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	deleted := &models.Message{
		MessageID: "m1",
		RoomID:    "r1",
		CreatedAt: createdAt,
		Sender:    models.Participant{ID: "user-bob", Account: "bob"},
		Deleted:   true,
		Reactions: nil,
	}
	f.msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(deleted, nil)
	f.users.EXPECT().FindUserByAccount(gomock.Any(), "u1").Return(aliceUserPtr(), nil)

	resp, err := f.svc.ReactMessage(testContext(), "site-test",
		models.ReactMessageRequest{MessageID: "m1", Shortcode: "thumbsup"})
	assert.Nil(t, resp)
	assertNotFoundErr(t, err, "message not found")
}

func TestHistoryService_ReactMessage_RemoveOnDeleted_Allowed(t *testing.T) {
	f := newReactFixture(t)
	createdAt := joinTime.Add(1 * time.Minute)
	f.subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	deleted := &models.Message{
		MessageID: "m1",
		RoomID:    "r1",
		CreatedAt: createdAt,
		Sender:    models.Participant{ID: "user-bob", Account: "bob"},
		Deleted:   true,
		Reactions: models.Message{}.Reactions, // initialised below
	}
	deleted.Reactions = map[models.ReactionKey]models.ReactorInfo{
		{Emoji: "thumbsup", UserAccount: "u1"}: {UserID: "user-alice", Account: "u1", EngName: "Alice", ReactedAt: createdAt},
	}
	f.msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(deleted, nil)
	f.users.EXPECT().FindUserByAccount(gomock.Any(), "u1").Return(aliceUserPtr(), nil)
	f.msgs.EXPECT().
		RemoveReaction(gomock.Any(), deleted, models.ReactionKey{Emoji: "thumbsup", UserAccount: "u1"}).
		Return(nil)
	f.pub.EXPECT().
		Publish(gomock.Any(), subject.MsgCanonicalReacted("site-test"), gomock.Any(), gomock.Any()).
		Return(nil)

	resp, err := f.svc.ReactMessage(testContext(), "site-test",
		models.ReactMessageRequest{MessageID: "m1", Shortcode: "thumbsup"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, model.ReactionActionRemoved, resp.Action)
	assert.Equal(t, "thumbsup", resp.Shortcode)
}

func TestHistoryService_ReactMessage_Add_Success_PublishesEvent(t *testing.T) {
	f := newReactFixture(t)
	createdAt := joinTime.Add(1 * time.Minute)
	f.subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	target := &models.Message{
		MessageID: "m1",
		RoomID:    "r1",
		CreatedAt: createdAt,
		Sender:    models.Participant{ID: "user-bob", Account: "bob"},
	}
	f.msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(target, nil)
	f.users.EXPECT().FindUserByAccount(gomock.Any(), "u1").Return(aliceUserPtr(), nil)

	expectedKey := models.ReactionKey{Emoji: "thumbsup", UserAccount: "u1"}
	f.msgs.EXPECT().
		AddReaction(gomock.Any(), target, expectedKey, gomock.Any()).
		DoAndReturn(func(_ context.Context, _ *models.Message, key models.ReactionKey, reactor models.ReactorInfo) error {
			assert.Equal(t, expectedKey, key)
			assert.Equal(t, "user-alice", reactor.UserID)
			assert.Equal(t, "u1", reactor.Account)
			assert.Equal(t, "Alice", reactor.EngName)
			assert.Equal(t, "Alice CN", reactor.ChnName)
			assert.False(t, reactor.ReactedAt.IsZero())
			return nil
		})

	var publishedPayload []byte
	f.pub.EXPECT().
		Publish(gomock.Any(), subject.MsgCanonicalReacted("site-test"), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, data []byte, _ string) error {
			publishedPayload = data
			return nil
		})

	resp, err := f.svc.ReactMessage(testContext(), "site-test",
		models.ReactMessageRequest{MessageID: "m1", Shortcode: "thumbsup"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, model.ReactionActionAdded, resp.Action)
	assert.Equal(t, "m1", resp.MessageID)
	assert.Equal(t, "thumbsup", resp.Shortcode)
	assert.Positive(t, resp.ReactedAt)

	require.NotEmpty(t, publishedPayload)
	var evt model.MessageEvent
	require.NoError(t, json.Unmarshal(publishedPayload, &evt))
	assert.Equal(t, model.EventReacted, evt.Event)
	assert.Equal(t, "m1", evt.Message.ID)
	assert.Equal(t, "user-bob", evt.Message.UserID)
	assert.Equal(t, "site-test", evt.SiteID)
	require.NotNil(t, evt.ReactionDelta)
	assert.Equal(t, "thumbsup", evt.ReactionDelta.Shortcode)
	assert.Equal(t, model.ReactionActionAdded, evt.ReactionDelta.Action)
	assert.Equal(t, "user-alice", evt.ReactionDelta.Actor.UserID)
	assert.Equal(t, "u1", evt.ReactionDelta.Actor.Account)
	assert.Equal(t, displayfmt.CombineWithFallback(aliceUser().EngName, aliceUser().ChineseName, aliceUser().Account), evt.ReactionDelta.Actor.DisplayName)
	// CLAUDE.md §6: event Timestamp must be set and match resp.ReactedAt.
	assert.Positive(t, evt.Timestamp)
	assert.Equal(t, resp.ReactedAt, evt.Timestamp)
}

func TestHistoryService_ReactMessage_Remove_Success(t *testing.T) {
	f := newReactFixture(t)
	createdAt := joinTime.Add(1 * time.Minute)
	f.subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	target := &models.Message{
		MessageID: "m1",
		RoomID:    "r1",
		CreatedAt: createdAt,
		Sender:    models.Participant{ID: "user-bob", Account: "bob"},
		Reactions: map[models.ReactionKey]models.ReactorInfo{
			{Emoji: "thumbsup", UserAccount: "u1"}: {UserID: "user-alice", Account: "u1", EngName: "Alice", ReactedAt: createdAt},
		},
	}
	f.msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(target, nil)
	f.users.EXPECT().FindUserByAccount(gomock.Any(), "u1").Return(aliceUserPtr(), nil)
	f.msgs.EXPECT().
		RemoveReaction(gomock.Any(), target, models.ReactionKey{Emoji: "thumbsup", UserAccount: "u1"}).
		Return(nil)
	f.pub.EXPECT().Publish(gomock.Any(), subject.MsgCanonicalReacted("site-test"), gomock.Any(), gomock.Any()).Return(nil)

	resp, err := f.svc.ReactMessage(testContext(), "site-test",
		models.ReactMessageRequest{MessageID: "m1", Shortcode: "thumbsup"})
	require.NoError(t, err)
	assert.Equal(t, model.ReactionActionRemoved, resp.Action)
}

func TestHistoryService_ReactMessage_UserLookupError(t *testing.T) {
	f := newReactFixture(t)
	createdAt := joinTime.Add(1 * time.Minute)
	f.subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	target := &models.Message{
		MessageID: "m1", RoomID: "r1", CreatedAt: createdAt,
		Sender: models.Participant{ID: "user-bob", Account: "bob"},
	}
	f.msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(target, nil)
	f.users.EXPECT().FindUserByAccount(gomock.Any(), "u1").Return(nil, errors.New("mongo down"))

	resp, err := f.svc.ReactMessage(testContext(), "site-test",
		models.ReactMessageRequest{MessageID: "m1", Shortcode: "thumbsup"})
	assert.Nil(t, resp)
	assertInternalErr(t, err, "resolve actor")
}

func TestHistoryService_ReactMessage_UserNotFound(t *testing.T) {
	f := newReactFixture(t)
	createdAt := joinTime.Add(1 * time.Minute)
	f.subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	target := &models.Message{
		MessageID: "m1", RoomID: "r1", CreatedAt: createdAt,
		Sender: models.Participant{ID: "user-bob", Account: "bob"},
	}
	f.msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(target, nil)
	f.users.EXPECT().FindUserByAccount(gomock.Any(), "u1").Return(nil, userstore.ErrUserNotFound)

	resp, err := f.svc.ReactMessage(testContext(), "site-test",
		models.ReactMessageRequest{MessageID: "m1", Shortcode: "thumbsup"})
	assert.Nil(t, resp)
	assertInternalErr(t, err, "actor not found")
}

func TestHistoryService_ReactMessage_AddStoreError(t *testing.T) {
	f := newReactFixture(t)
	createdAt := joinTime.Add(1 * time.Minute)
	f.subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	target := &models.Message{
		MessageID: "m1", RoomID: "r1", CreatedAt: createdAt,
		Sender: models.Participant{ID: "user-bob", Account: "bob"},
	}
	f.msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(target, nil)
	f.users.EXPECT().FindUserByAccount(gomock.Any(), "u1").Return(aliceUserPtr(), nil)
	f.msgs.EXPECT().
		AddReaction(gomock.Any(), target, gomock.Any(), gomock.Any()).
		Return(errors.New("cassandra down"))

	resp, err := f.svc.ReactMessage(testContext(), "site-test",
		models.ReactMessageRequest{MessageID: "m1", Shortcode: "thumbsup"})
	assert.Nil(t, resp)
	assertInternalErr(t, err, "react: add")
}

func TestHistoryService_ReactMessage_RemoveStoreError(t *testing.T) {
	f := newReactFixture(t)
	createdAt := joinTime.Add(1 * time.Minute)
	f.subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	target := &models.Message{
		MessageID: "m1", RoomID: "r1", CreatedAt: createdAt,
		Sender: models.Participant{ID: "user-bob", Account: "bob"},
		Reactions: map[models.ReactionKey]models.ReactorInfo{
			{Emoji: "thumbsup", UserAccount: "u1"}: {UserID: "user-alice", Account: "u1"},
		},
	}
	f.msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(target, nil)
	f.users.EXPECT().FindUserByAccount(gomock.Any(), "u1").Return(aliceUserPtr(), nil)
	f.msgs.EXPECT().
		RemoveReaction(gomock.Any(), target, gomock.Any()).
		Return(errors.New("cassandra down"))

	resp, err := f.svc.ReactMessage(testContext(), "site-test",
		models.ReactMessageRequest{MessageID: "m1", Shortcode: "thumbsup"})
	assert.Nil(t, resp)
	assertInternalErr(t, err, "react: remove")
}

// TestHistoryService_ReactMessage_PublishesFullMessage covers #459: the reacted-to
// message in the canonical event must carry the full message body (content, tshow,
// thread-reply linkage, type, mentions), not just the 5-field skeleton.
func TestHistoryService_ReactMessage_PublishesFullMessage(t *testing.T) {
	createdAt := joinTime.Add(1 * time.Minute)
	threadParentCreatedAt := joinTime

	cases := []struct {
		name   string
		target *models.Message
	}{
		{
			name: "channel message",
			target: &models.Message{
				MessageID: "m1", RoomID: "r1", CreatedAt: createdAt,
				Sender: models.Participant{ID: "user-bob", Account: "bob", EngName: "Bob", CompanyName: "鮑伯"},
				Msg:    "hello world",
				Type:   "text",
				Mentions: []models.Participant{
					{ID: "user-carol", Account: "carol", EngName: "Carol", CompanyName: "卡蘿"},
				},
			},
		},
		{
			name: "thread reply",
			target: &models.Message{
				MessageID: "m2", RoomID: "r1", CreatedAt: createdAt,
				Sender:                models.Participant{ID: "user-bob", Account: "bob", EngName: "Bob"},
				Msg:                   "a reply",
				ThreadParentID:        "m1",
				ThreadParentCreatedAt: &threadParentCreatedAt,
			},
		},
		{
			name: "tshow reply",
			target: &models.Message{
				MessageID: "m3", RoomID: "r1", CreatedAt: createdAt,
				Sender:                models.Participant{ID: "user-bob", Account: "bob", EngName: "Bob"},
				Msg:                   "also sent to channel",
				TShow:                 true,
				ThreadParentID:        "m1",
				ThreadParentCreatedAt: &threadParentCreatedAt,
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			f := newReactFixture(t)
			f.subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
			f.msgs.EXPECT().GetMessageByID(gomock.Any(), tc.target.MessageID).Return(tc.target, nil)
			f.users.EXPECT().FindUserByAccount(gomock.Any(), "u1").Return(aliceUserPtr(), nil)
			f.msgs.EXPECT().
				AddReaction(gomock.Any(), tc.target, gomock.Any(), gomock.Any()).
				Return(nil)

			var publishedPayload []byte
			f.pub.EXPECT().
				Publish(gomock.Any(), subject.MsgCanonicalReacted("site-test"), gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, _ string, data []byte, _ string) error {
					publishedPayload = data
					return nil
				})

			resp, err := f.svc.ReactMessage(testContext(), "site-test",
				models.ReactMessageRequest{MessageID: tc.target.MessageID, Shortcode: "thumbsup"})
			require.NoError(t, err)
			require.NotNil(t, resp)

			require.NotEmpty(t, publishedPayload)
			var evt model.MessageEvent
			require.NoError(t, json.Unmarshal(publishedPayload, &evt))

			assert.Equal(t, tc.target.Msg, evt.Message.Content)
			assert.Equal(t, tc.target.TShow, evt.Message.TShow)
			assert.Equal(t, tc.target.ThreadParentID, evt.Message.ThreadParentMessageID)
			assert.Equal(t, tc.target.Type, evt.Message.Type)
			if tc.target.ThreadParentCreatedAt != nil {
				require.NotNil(t, evt.Message.ThreadParentMessageCreatedAt)
				assert.True(t, tc.target.ThreadParentCreatedAt.Equal(*evt.Message.ThreadParentMessageCreatedAt))
			} else {
				assert.Nil(t, evt.Message.ThreadParentMessageCreatedAt)
			}
			require.Len(t, evt.Message.Mentions, len(tc.target.Mentions))
			for i, m := range tc.target.Mentions {
				assert.Equal(t, m.Account, evt.Message.Mentions[i].Account)
				assert.Equal(t, m.ID, evt.Message.Mentions[i].UserID)
				assert.Equal(t, m.EngName, evt.Message.Mentions[i].EngName)
				// ChineseName is carried by the Cassandra company_name field.
				assert.Equal(t, m.CompanyName, evt.Message.Mentions[i].ChineseName)
			}
			// Sender display name folds in the Chinese name (company_name).
			assert.Equal(t,
				displayfmt.CombineWithFallback(tc.target.Sender.EngName, tc.target.Sender.CompanyName, tc.target.Sender.Account),
				evt.Message.UserDisplayName)
		})
	}
}

// --- Bot reactor displayName resolution (chat#460) ---

func botUser() *model.User {
	return &model.User{ID: "user-acme-bot", Account: "acme.bot", SiteID: "site-test", EngName: "acme.bot"}
}

// reactAsFixture drives a full ADD react as actor and returns the response plus the
// decoded canonical event, so callers can assert Actor.DisplayName resolution.
func reactAsFixture(t *testing.T, f reactFixture, actor *model.User) (*models.ReactMessageResponse, model.MessageEvent) {
	t.Helper()
	createdAt := joinTime.Add(1 * time.Minute)
	f.subs.EXPECT().GetHistorySharedSince(gomock.Any(), actor.Account, "r1").Return(nil, true, nil)
	target := &models.Message{
		MessageID: "m1", RoomID: "r1", CreatedAt: createdAt,
		Sender: models.Participant{ID: "user-bob", Account: "bob"},
	}
	f.msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(target, nil)
	f.users.EXPECT().FindUserByAccount(gomock.Any(), actor.Account).Return(actor, nil)
	f.msgs.EXPECT().AddReaction(gomock.Any(), target, gomock.Any(), gomock.Any()).Return(nil)

	var payload []byte
	f.pub.EXPECT().
		Publish(gomock.Any(), subject.MsgCanonicalReacted("site-test"), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, data []byte, _ string) error {
			payload = data
			return nil
		})

	ctx := natsrouter.NewContext(map[string]string{"account": actor.Account, "roomID": "r1"})
	resp, err := f.svc.ReactMessage(ctx, "site-test", models.ReactMessageRequest{MessageID: "m1", Shortcode: "thumbsup"})
	require.NoError(t, err)
	require.NotEmpty(t, payload)
	var evt model.MessageEvent
	require.NoError(t, json.Unmarshal(payload, &evt))
	return resp, evt
}

func TestHistoryService_ReactMessage_HumanReactor_ComposedName_AppStoreNotCalled(t *testing.T) {
	f := newReactFixture(t)
	f.apps.EXPECT().AppNameByAccount(gomock.Any(), gomock.Any()).Times(0)
	_, evt := reactAsFixture(t, f, aliceUserPtr())
	assert.Equal(t, displayfmt.CombineWithFallback(aliceUser().EngName, aliceUser().ChineseName, aliceUser().Account), evt.ReactionDelta.Actor.DisplayName)
}

func TestHistoryService_ReactMessage_BotReactor_UsesAppName(t *testing.T) {
	f := newReactFixture(t)
	f.apps.EXPECT().AppNameByAccount(gomock.Any(), "acme.bot").Return("Acme Assistant", nil)
	_, evt := reactAsFixture(t, f, botUser())
	assert.Equal(t, "Acme Assistant", evt.ReactionDelta.Actor.DisplayName)
}

func TestHistoryService_ReactMessage_BotReactor_AppStoreMiss_FallsBackToComposed(t *testing.T) {
	f := newReactFixture(t)
	f.apps.EXPECT().AppNameByAccount(gomock.Any(), "acme.bot").Return("", nil)
	_, evt := reactAsFixture(t, f, botUser())
	assert.Equal(t, displayfmt.CombineWithFallback(botUser().EngName, botUser().ChineseName, botUser().Account), evt.ReactionDelta.Actor.DisplayName)
}

func TestHistoryService_ReactMessage_BotReactor_AppStoreError_DegradesToComposed(t *testing.T) {
	f := newReactFixture(t)
	f.apps.EXPECT().AppNameByAccount(gomock.Any(), "acme.bot").Return("", errors.New("mongo down"))
	resp, evt := reactAsFixture(t, f, botUser())
	require.NotNil(t, resp)
	assert.Equal(t, displayfmt.CombineWithFallback(botUser().EngName, botUser().ChineseName, botUser().Account), evt.ReactionDelta.Actor.DisplayName)
}
