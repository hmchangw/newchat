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
	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/subject"
)

type reactFixture struct {
	svc interface {
		ReactMessage(c *natsrouter.Context, siteID string, req models.ReactMessageRequest) (*models.ReactMessageResponse, error)
	}
	msgs         *mocks.MockMessageRepository
	subs         *mocks.MockSubscriptionRepository
	pub          *mocks.MockEventPublisher
	users        *mocks.MockUserStore
	customEmojis *mocks.MockCustomEmojiStore
}

func newReactFixture(t *testing.T) reactFixture {
	svc, msgs, subs, rooms, pub, _, users, customEmojis := newServiceWithRoomMock(t)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	rooms.EXPECT().GetRoomTimes(gomock.Any(), gomock.Any()).Return(defaultRoomLastMsgAt, defaultRoomCreatedAt, nil).AnyTimes()
	return reactFixture{svc: svc, msgs: msgs, subs: subs, pub: pub, users: users, customEmojis: customEmojis}
}

// stubShortcodeKnown makes the validator's lookup return true for shortcode.
func (f reactFixture) stubShortcodeKnown(shortcode string) {
	f.customEmojis.EXPECT().
		CustomEmojiExists(gomock.Any(), "site-test", shortcode).
		Return(true, nil).
		AnyTimes()
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

func TestHistoryService_ReactMessage_NotSubscribed(t *testing.T) {
	f := newReactFixture(t)
	f.stubShortcodeKnown("thumbsup")
	f.subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, false, nil)

	resp, err := f.svc.ReactMessage(testContext(), "site-test",
		models.ReactMessageRequest{MessageID: "m1", Shortcode: "thumbsup"})
	assert.Nil(t, resp)
	assertForbiddenErr(t, err, "not subscribed to room")
}

// Every pre-storage rejection path: empty fields, regex fail, unknown shortcode, validator-internal error.
func TestHistoryService_ReactMessage_ValidationErrors(t *testing.T) {
	cases := []struct {
		name         string
		shortcode    string
		messageID    string
		stubLookup   func(f reactFixture, shortcode string)
		wantCategory errcode.Code // CodeBadRequest, CodeInternal
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
			name:      "invalid shortcode format",
			shortcode: ":thumbsup:", messageID: "m1",
			wantCategory: errcode.CodeBadRequest,
			wantMsg:      "invalid reaction shortcode",
		},
		{
			name:      "unknown custom shortcode",
			shortcode: "no_such_emoji", messageID: "m1",
			stubLookup: func(f reactFixture, sc string) {
				f.customEmojis.EXPECT().CustomEmojiExists(gomock.Any(), "site-test", sc).Return(false, nil)
			},
			wantCategory: errcode.CodeBadRequest,
			wantMsg:      "invalid reaction shortcode",
		},
		{
			name:      "validator internal error (lookup down)",
			shortcode: "thumbsup", messageID: "m1",
			stubLookup: func(f reactFixture, sc string) {
				f.customEmojis.EXPECT().CustomEmojiExists(gomock.Any(), "site-test", sc).Return(false, errors.New("mongo down"))
			},
			wantCategory: errcode.CodeInternal,
			wantMsg:      "validate shortcode",
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			f := newReactFixture(t)
			if tc.stubLookup != nil {
				tc.stubLookup(f, tc.shortcode)
			}
			resp, err := f.svc.ReactMessage(testContext(), "site-test",
				models.ReactMessageRequest{MessageID: tc.messageID, Shortcode: tc.shortcode})
			assert.Nil(t, resp)
			switch tc.wantCategory {
			case errcode.CodeBadRequest:
				assertBadRequestErr(t, err, tc.wantMsg)
			case errcode.CodeInternal:
				assertInternalErr(t, err, tc.wantMsg)
			default:
				t.Fatalf("unhandled category: %s", tc.wantCategory)
			}
		})
	}
}

func TestHistoryService_ReactMessage_MessageNotFound(t *testing.T) {
	f := newReactFixture(t)
	f.stubShortcodeKnown("thumbsup")
	f.subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	f.msgs.EXPECT().GetMessageByID(gomock.Any(), "missing").Return(nil, nil)
	resp, err := f.svc.ReactMessage(testContext(), "site-test",
		models.ReactMessageRequest{MessageID: "missing", Shortcode: "thumbsup"})
	assert.Nil(t, resp)
	assertNotFoundErr(t, err, "message not found")
}

func TestHistoryService_ReactMessage_AddOnDeleted_Blocked(t *testing.T) {
	f := newReactFixture(t)
	f.stubShortcodeKnown("thumbsup")
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
	f.users.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"u1"}).Return([]model.User{aliceUser()}, nil)

	resp, err := f.svc.ReactMessage(testContext(), "site-test",
		models.ReactMessageRequest{MessageID: "m1", Shortcode: "thumbsup"})
	assert.Nil(t, resp)
	assertNotFoundErr(t, err, "message not found")
}

func TestHistoryService_ReactMessage_RemoveOnDeleted_Allowed(t *testing.T) {
	f := newReactFixture(t)
	f.stubShortcodeKnown("thumbsup")
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
	f.users.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"u1"}).Return([]model.User{aliceUser()}, nil)
	f.msgs.EXPECT().
		RemoveReaction(gomock.Any(), deleted, models.ReactionKey{Emoji: "thumbsup", UserAccount: "u1"}, gomock.Any()).
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
	f.stubShortcodeKnown("thumbsup")
	createdAt := joinTime.Add(1 * time.Minute)
	f.subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	target := &models.Message{
		MessageID: "m1",
		RoomID:    "r1",
		CreatedAt: createdAt,
		Sender:    models.Participant{ID: "user-bob", Account: "bob"},
	}
	f.msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(target, nil)
	f.users.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"u1"}).Return([]model.User{aliceUser()}, nil)

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
	// CLAUDE.md §6: event Timestamp must be set and match resp.ReactedAt.
	assert.Positive(t, evt.Timestamp)
	assert.Equal(t, resp.ReactedAt, evt.Timestamp)
}

func TestHistoryService_ReactMessage_Remove_Success(t *testing.T) {
	f := newReactFixture(t)
	f.stubShortcodeKnown("thumbsup")
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
	f.users.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"u1"}).Return([]model.User{aliceUser()}, nil)
	f.msgs.EXPECT().
		RemoveReaction(gomock.Any(), target, models.ReactionKey{Emoji: "thumbsup", UserAccount: "u1"}, gomock.Any()).
		Return(nil)
	f.pub.EXPECT().Publish(gomock.Any(), subject.MsgCanonicalReacted("site-test"), gomock.Any(), gomock.Any()).Return(nil)

	resp, err := f.svc.ReactMessage(testContext(), "site-test",
		models.ReactMessageRequest{MessageID: "m1", Shortcode: "thumbsup"})
	require.NoError(t, err)
	assert.Equal(t, model.ReactionActionRemoved, resp.Action)
}

func TestHistoryService_ReactMessage_UserLookupError(t *testing.T) {
	f := newReactFixture(t)
	f.stubShortcodeKnown("thumbsup")
	createdAt := joinTime.Add(1 * time.Minute)
	f.subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	target := &models.Message{
		MessageID: "m1", RoomID: "r1", CreatedAt: createdAt,
		Sender: models.Participant{ID: "user-bob", Account: "bob"},
	}
	f.msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(target, nil)
	f.users.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"u1"}).Return(nil, errors.New("mongo down"))

	resp, err := f.svc.ReactMessage(testContext(), "site-test",
		models.ReactMessageRequest{MessageID: "m1", Shortcode: "thumbsup"})
	assert.Nil(t, resp)
	assertInternalErr(t, err, "resolve actor")
}

func TestHistoryService_ReactMessage_UserNotFound(t *testing.T) {
	f := newReactFixture(t)
	f.stubShortcodeKnown("thumbsup")
	createdAt := joinTime.Add(1 * time.Minute)
	f.subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	target := &models.Message{
		MessageID: "m1", RoomID: "r1", CreatedAt: createdAt,
		Sender: models.Participant{ID: "user-bob", Account: "bob"},
	}
	f.msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(target, nil)
	f.users.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"u1"}).Return(nil, nil)

	resp, err := f.svc.ReactMessage(testContext(), "site-test",
		models.ReactMessageRequest{MessageID: "m1", Shortcode: "thumbsup"})
	assert.Nil(t, resp)
	assertInternalErr(t, err, "actor not found")
}

func TestHistoryService_ReactMessage_AddStoreError(t *testing.T) {
	f := newReactFixture(t)
	f.stubShortcodeKnown("thumbsup")
	createdAt := joinTime.Add(1 * time.Minute)
	f.subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	target := &models.Message{
		MessageID: "m1", RoomID: "r1", CreatedAt: createdAt,
		Sender: models.Participant{ID: "user-bob", Account: "bob"},
	}
	f.msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(target, nil)
	f.users.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"u1"}).Return([]model.User{aliceUser()}, nil)
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
	f.stubShortcodeKnown("thumbsup")
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
	f.users.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"u1"}).Return([]model.User{aliceUser()}, nil)
	f.msgs.EXPECT().
		RemoveReaction(gomock.Any(), target, gomock.Any(), gomock.Any()).
		Return(errors.New("cassandra down"))

	resp, err := f.svc.ReactMessage(testContext(), "site-test",
		models.ReactMessageRequest{MessageID: "m1", Shortcode: "thumbsup"})
	assert.Nil(t, resp)
	assertInternalErr(t, err, "react: remove")
}

func TestHistoryService_ReactMessage_CustomEmojiFound_Success(t *testing.T) {
	f := newReactFixture(t)
	createdAt := joinTime.Add(1 * time.Minute)
	f.customEmojis.EXPECT().CustomEmojiExists(gomock.Any(), "site-test", "acme_party").Return(true, nil)
	f.subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	target := &models.Message{
		MessageID: "m1", RoomID: "r1", CreatedAt: createdAt,
		Sender: models.Participant{ID: "user-bob", Account: "bob"},
	}
	f.msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(target, nil)
	f.users.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"u1"}).Return([]model.User{aliceUser()}, nil)
	f.msgs.EXPECT().
		AddReaction(gomock.Any(), target, models.ReactionKey{Emoji: "acme_party", UserAccount: "u1"}, gomock.Any()).
		Return(nil)
	f.pub.EXPECT().Publish(gomock.Any(), subject.MsgCanonicalReacted("site-test"), gomock.Any(), gomock.Any()).Return(nil)

	resp, err := f.svc.ReactMessage(testContext(), "site-test",
		models.ReactMessageRequest{MessageID: "m1", Shortcode: "acme_party"})
	require.NoError(t, err)
	assert.Equal(t, model.ReactionActionAdded, resp.Action)
}
