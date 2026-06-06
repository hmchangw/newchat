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

	"github.com/hmchangw/chat/history-service/internal/cassrepo"
	"github.com/hmchangw/chat/history-service/internal/config"
	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/history-service/internal/service"
	"github.com/hmchangw/chat/history-service/internal/service/mocks"
	"github.com/hmchangw/chat/pkg/model"
)

func pinnableMsg() *models.Message {
	return &models.Message{
		MessageID: "m1", RoomID: "r1",
		CreatedAt: time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC),
		Sender:    models.Participant{ID: "u1", Account: "alice"},
		Msg:       "hello",
	}
}

func subFor(roles ...model.Role) *model.Subscription {
	return &model.Subscription{
		RoomID: "r1",
		User:   model.SubscriptionUser{ID: "u1", Account: "u1"},
		Roles:  roles,
	}
}

// newPinTestService is a strict-mock harness — no AnyTimes default for
// GetRoomUserCount, so unexpected calls fail. PinEnabled hard-coded true.
func newPinTestService(t *testing.T) (*service.HistoryService, *mocks.MockMessageRepository, *mocks.MockSubscriptionRepository, *mocks.MockRoomRepository, *mocks.MockEventPublisher, *mocks.MockThreadRoomRepository) {
	ctrl := gomock.NewController(t)
	msgs := mocks.NewMockMessageRepository(ctrl)
	subs := mocks.NewMockSubscriptionRepository(ctrl)
	rooms := mocks.NewMockRoomRepository(ctrl)
	pub := mocks.NewMockEventPublisher(ctrl)
	threadRooms := mocks.NewMockThreadRoomRepository(ctrl)
	users := mocks.NewMockUserStore(ctrl)
	customEmojis := mocks.NewMockCustomEmojiStore(ctrl)
	rooms.EXPECT().
		GetRoomTimes(gomock.Any(), gomock.Any()).
		Return(defaultRoomLastMsgAt, defaultRoomCreatedAt, nil).
		MinTimes(0)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	cfg := &config.Config{
		MessageHistoryFloorDays: 90,
		LargeRoomThreshold:      500,
		MaxPinnedPerRoom:        testMaxPinnedPerRoom,
		PinEnabled:              true,
	}
	return service.New(msgs, subs, rooms, pub, threadRooms, users, customEmojis, cfg), msgs, subs, rooms, pub, threadRooms
}

// testMaxPinnedPerRoom: kept small (3) so fixtures stay short.
const testMaxPinnedPerRoom = 3

// pinnedList returns N empty pinned messages — only the count matters to enforcePinLimit.
func pinnedList(n int) []models.Message {
	return make([]models.Message, n)
}

// pinnedPage wraps a slice in a single-page cassrepo.Page. Pagination-specific
// tests construct cassrepo.Page literals inline to assert NextCursor/HasNext.
func pinnedPage(msgs []models.Message) cassrepo.Page[models.Message] {
	return cassrepo.Page[models.Message]{Data: msgs}
}

func TestPinMessage_HappyPath(t *testing.T) {
	// Uses newServiceWithRoomMock — no explicit GetRoomUserCount override to avoid gomock FIFO conflicts.
	svc, msgs, subs, rooms, pub, _, _, _ := newServiceWithRoomMock(t)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	subs.EXPECT().GetSubscription(gomock.Any(), "u1", "r1").Return(subFor(model.RoleMember), nil)
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(pinnableMsg(), nil)
	msgs.EXPECT().GetAllPinnedMessages(gomock.Any(), "r1").Return(pinnedList(0), nil)
	msgs.EXPECT().PinMessage(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	var published model.MessageEvent
	pub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, data []byte, _ string) error {
			return json.Unmarshal(data, &published)
		})

	resp, err := svc.PinMessage(testContext(), "site-a", models.PinMessageRequest{MessageID: "m1"})

	require.NoError(t, err)
	assert.Equal(t, "m1", resp.MessageID)
	assert.NotZero(t, resp.PinnedAt)
	assert.Equal(t, model.EventPinned, published.Event)
	require.NotNil(t, published.Message.PinnedAt)
}

func TestPinMessage_KillSwitchDisabled(t *testing.T) {
	// pinPreCheck short-circuits before any repo call when PinEnabled=false.
	ctrl := gomock.NewController(t)
	msgs := mocks.NewMockMessageRepository(ctrl)
	subs := mocks.NewMockSubscriptionRepository(ctrl)
	rooms := mocks.NewMockRoomRepository(ctrl)
	pub := mocks.NewMockEventPublisher(ctrl)
	threadRooms := mocks.NewMockThreadRoomRepository(ctrl)
	users := mocks.NewMockUserStore(ctrl)
	customEmojis := mocks.NewMockCustomEmojiStore(ctrl)
	rooms.EXPECT().
		GetRoomTimes(gomock.Any(), gomock.Any()).
		Return(defaultRoomLastMsgAt, defaultRoomCreatedAt, nil).
		MinTimes(0)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	svc := service.New(msgs, subs, rooms, pub, threadRooms, users, customEmojis, &config.Config{
		MessageHistoryFloorDays: 90,
		LargeRoomThreshold:      500,
		MaxPinnedPerRoom:        testMaxPinnedPerRoom,
		PinEnabled:              false,
	})

	_, err := svc.PinMessage(testContext(), "site-a", models.PinMessageRequest{MessageID: "m1"})

	assertForbiddenErr(t, err, "pinning is disabled")
}

func TestPinMessage_NotSubscribed(t *testing.T) {
	svc, _, subs, rooms, _, _, _, _ := newServiceWithRoomMock(t)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	subs.EXPECT().GetSubscription(gomock.Any(), "u1", "r1").Return(nil, nil)

	_, err := svc.PinMessage(testContext(), "site-a", models.PinMessageRequest{MessageID: "m1"})

	assertForbiddenErr(t, err, "not subscribed to room")
}

func TestPinMessage_LargeRoomBlocksMember(t *testing.T) {
	svc, msgs, subs, rooms, _, _ := newPinTestService(t)
	subs.EXPECT().GetSubscription(gomock.Any(), "u1", "r1").Return(subFor(model.RoleMember), nil)
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(pinnableMsg(), nil)
	rooms.EXPECT().GetRoomUserCount(gomock.Any(), "r1").Return(501, nil)

	_, err := svc.PinMessage(testContext(), "site-a", models.PinMessageRequest{MessageID: "m1"})

	assertForbiddenErr(t, err, "room is too large to pin")
}

func TestPinMessage_LargeRoomOwnerBypass(t *testing.T) {
	// Owner bypasses large-room check (no GetRoomUserCount call); pin-limit still runs.
	svc, msgs, subs, _, pub, _ := newPinTestService(t)
	subs.EXPECT().GetSubscription(gomock.Any(), "u1", "r1").Return(subFor(model.RoleOwner), nil)
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(pinnableMsg(), nil)
	msgs.EXPECT().GetAllPinnedMessages(gomock.Any(), "r1").Return(pinnedList(0), nil)
	msgs.EXPECT().PinMessage(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	pub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	_, err := svc.PinMessage(testContext(), "site-a", models.PinMessageRequest{MessageID: "m1"})

	require.NoError(t, err)
}

func TestPinMessage_DeletedMessageNotFound(t *testing.T) {
	// Deleted gate fires before enforceLargeRoomPin (only PinMessage gates;
	// UnpinMessage accepts — see TestUnpinMessage_AllowsDeletedMessage).
	svc, msgs, subs, _, _, _ := newPinTestService(t)
	subs.EXPECT().GetSubscription(gomock.Any(), "u1", "r1").Return(subFor(model.RoleMember), nil)
	deleted := pinnableMsg()
	deleted.Deleted = true
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(deleted, nil)

	_, err := svc.PinMessage(testContext(), "site-a", models.PinMessageRequest{MessageID: "m1"})

	assertNotFoundErr(t, err, "message not found")
}

func TestPinMessage_IdempotentAlreadyPinned(t *testing.T) {
	// Idempotent short-circuit fires before the large-room gate (strict mock proves it).
	svc, msgs, subs, _, _, _ := newPinTestService(t)
	subs.EXPECT().GetSubscription(gomock.Any(), "u1", "r1").Return(subFor(model.RoleMember), nil)
	already := pinnableMsg()
	at := time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC)
	already.PinnedAt = &at
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(already, nil)

	resp, err := svc.PinMessage(testContext(), "site-a", models.PinMessageRequest{MessageID: "m1"})

	require.NoError(t, err)
	assert.Equal(t, at.UnixMilli(), resp.PinnedAt)
}

func TestPinMessage_IdempotentAlreadyPinned_LargeRoomMemberStillSucceeds(t *testing.T) {
	// Member re-pinning in a large room: idempotent succeeds without consulting GetRoomUserCount.
	svc, msgs, subs, _, _, _ := newPinTestService(t)
	subs.EXPECT().GetSubscription(gomock.Any(), "u1", "r1").Return(subFor(model.RoleMember), nil)
	already := pinnableMsg()
	at := time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC)
	already.PinnedAt = &at
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(already, nil)

	resp, err := svc.PinMessage(testContext(), "site-a", models.PinMessageRequest{MessageID: "m1"})

	require.NoError(t, err)
	assert.Equal(t, at.UnixMilli(), resp.PinnedAt)
}

func TestUnpinMessage_HappyPath(t *testing.T) {
	svc, msgs, subs, rooms, pub, _ := newPinTestService(t)
	subs.EXPECT().GetSubscription(gomock.Any(), "u1", "r1").Return(subFor(model.RoleMember), nil)
	rooms.EXPECT().GetRoomUserCount(gomock.Any(), "r1").Return(1, nil)
	pinned := pinnableMsg()
	at := time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC)
	pinned.PinnedAt = &at
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(pinned, nil)
	msgs.EXPECT().UnpinMessage(gomock.Any(), pinned).Return(nil)

	var published model.MessageEvent
	pub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, data []byte, _ string) error {
			return json.Unmarshal(data, &published)
		})

	resp, err := svc.UnpinMessage(testContext(), "site-a", models.UnpinMessageRequest{MessageID: "m1"})

	require.NoError(t, err)
	assert.Equal(t, "m1", resp.MessageID)
	assert.Equal(t, model.EventUnpinned, published.Event)
}

func TestUnpinMessage_AllowsDeletedMessage(t *testing.T) {
	// Soft-deleted pin still occupies a slot; unpin must proceed so moderators can free it.
	// (Owner bypasses large-room check, so GetRoomUserCount is not expected.)
	svc, msgs, subs, _, pub, _ := newPinTestService(t)
	subs.EXPECT().GetSubscription(gomock.Any(), "u1", "r1").Return(subFor(model.RoleOwner), nil)

	deletedPinned := pinnableMsg()
	at := time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC)
	deletedPinned.PinnedAt = &at
	deletedPinned.Deleted = true
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(deletedPinned, nil)

	var unpinned *models.Message
	msgs.EXPECT().UnpinMessage(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, m *models.Message) error {
			unpinned = m
			return nil
		})
	pub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	resp, err := svc.UnpinMessage(testContext(), "site-a", models.UnpinMessageRequest{MessageID: "m1"})

	require.NoError(t, err)
	assert.Equal(t, "m1", resp.MessageID)
	require.NotNil(t, unpinned, "msgWriter.UnpinMessage must be invoked even for a deleted message")
	assert.True(t, unpinned.Deleted, "the deleted flag must reach the writer so it knows the underlying message is gone")
}

func TestUnpinMessage_IdempotentNotPinned(t *testing.T) {
	// Idempotent short-circuit fires before the large-room gate (strict mock proves it).
	svc, msgs, subs, _, _, _ := newPinTestService(t)
	subs.EXPECT().GetSubscription(gomock.Any(), "u1", "r1").Return(subFor(model.RoleMember), nil)
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(pinnableMsg(), nil)

	resp, err := svc.UnpinMessage(testContext(), "site-a", models.UnpinMessageRequest{MessageID: "m1"})

	require.NoError(t, err)
	assert.Equal(t, "m1", resp.MessageID)
}

func TestListPinnedMessages_HappyPath(t *testing.T) {
	svc, msgs, subs, _, _, _, _, _ := newServiceWithRoomMock(t)
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	msgs.EXPECT().GetPinnedMessages(gomock.Any(), "r1", gomock.Any()).Return(pinnedPage([]models.Message{*pinnableMsg()}), nil)

	resp, err := svc.ListPinnedMessages(testContext(), models.ListPinnedMessagesRequest{})

	require.NoError(t, err)
	require.Len(t, resp.Messages, 1)
}

func TestListPinnedMessages_PaginationPlumbedThrough(t *testing.T) {
	// Request Cursor/Limit → repo PageRequest; repo NextCursor/HasNext → response.
	svc, msgs, subs, _, _, _, _, _ := newServiceWithRoomMock(t)
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)

	var gotPageReq cassrepo.PageRequest
	msgs.EXPECT().GetPinnedMessages(gomock.Any(), "r1", gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, pageReq cassrepo.PageRequest) (cassrepo.Page[models.Message], error) {
			gotPageReq = pageReq
			return cassrepo.Page[models.Message]{
				Data:       []models.Message{*pinnableMsg()},
				NextCursor: "next-cursor-token",
				HasNext:    true,
			}, nil
		})

	resp, err := svc.ListPinnedMessages(testContext(), models.ListPinnedMessagesRequest{
		Cursor: "", // first-page cursor is OK
		Limit:  5,
	})

	require.NoError(t, err)
	assert.Equal(t, 5, gotPageReq.PageSize, "request.Limit must reach the repo")
	require.Len(t, resp.Messages, 1)
	assert.Equal(t, "next-cursor-token", resp.NextCursor, "repo's NextCursor must surface on the response")
	assert.True(t, resp.HasNext)
}

func TestListPinnedMessages_StubsPreAccessPinContent(t *testing.T) {
	// Restricted caller sees same row count; pre-access pin content is blanked
	// while identifiers/sender/pin-metadata stay for placeholder rendering.
	svc, msgs, subs, _, _, _, _, _ := newServiceWithRoomMock(t)
	accessSince := time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC)
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&accessSince, true, nil)

	preAccess := models.Message{
		MessageID:   "old",
		RoomID:      "r1",
		CreatedAt:   time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC),
		Sender:      models.Participant{ID: "u2", Account: "bob"},
		Msg:         "secret from before user joined",
		Attachments: [][]byte{{0x01, 0x02}},
		Reactions: models.Reactions{
			models.ReactionKey{Emoji: "👍", UserAccount: "u3"}: models.ReactorInfo{UserID: "u3", Account: "u3"},
		},
	}
	postAccess := models.Message{
		MessageID: "new",
		RoomID:    "r1",
		CreatedAt: time.Date(2026, 5, 12, 9, 0, 0, 0, time.UTC),
		Sender:    models.Participant{ID: "u2", Account: "bob"},
		Msg:       "visible content",
	}
	msgs.EXPECT().GetPinnedMessages(gomock.Any(), "r1", gomock.Any()).
		Return(pinnedPage([]models.Message{postAccess, preAccess}), nil)

	resp, err := svc.ListPinnedMessages(testContext(), models.ListPinnedMessagesRequest{})

	require.NoError(t, err)
	require.Len(t, resp.Messages, 2, "row count must stay the same for restricted callers")
	assert.Equal(t, "visible content", resp.Messages[0].Msg)

	stub := resp.Messages[1]
	assert.Equal(t, "old", stub.MessageID, "identifier stays visible so the frontend can render a placeholder")
	assert.Equal(t, "bob", stub.Sender.Account, "sender stays visible")
	assert.Equal(t, "This message is unavailable", stub.Msg)
	assert.Empty(t, stub.Attachments)
	assert.Empty(t, stub.Reactions)
}

func TestListPinnedMessages_StubsSystemMessageMetadata(t *testing.T) {
	// Pinned system messages carry event data in Type/SysMsgData. Without
	// scrubbing those, "Msg = unavailable" would still leak the original
	// event type and payload to a restricted caller.
	svc, msgs, subs, _, _, _, _, _ := newServiceWithRoomMock(t)
	accessSince := time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC)
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&accessSince, true, nil)

	sysPin := models.Message{
		MessageID:  "sys-evt",
		RoomID:     "r1",
		CreatedAt:  time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC), // pre-access
		Sender:     models.Participant{ID: "u2", Account: "bob"},
		Msg:        "user joined",
		Type:       "user_joined",
		SysMsgData: []byte(`{"newMember":"alice","invitedBy":"bob"}`),
	}
	msgs.EXPECT().GetPinnedMessages(gomock.Any(), "r1", gomock.Any()).Return(pinnedPage([]models.Message{sysPin}), nil)

	resp, err := svc.ListPinnedMessages(testContext(), models.ListPinnedMessagesRequest{})

	require.NoError(t, err)
	require.Len(t, resp.Messages, 1)
	stub := resp.Messages[0]
	assert.Equal(t, "This message is unavailable", stub.Msg)
	assert.Empty(t, stub.Type, "system-message Type must be scrubbed")
	assert.Empty(t, stub.SysMsgData, "system-message payload must be scrubbed")
}

func TestListPinnedMessages_StubsThreadReplyWithPreAccessParent(t *testing.T) {
	// Post-access reply with pre-access parent → stubbed. Unlike quoteInaccessible,
	// pinInaccessible doesn't gate on TShow (TestListPinnedMessages_StubsThreadOnlyReply... covers TShow=false).
	svc, msgs, subs, _, _, _, _, _ := newServiceWithRoomMock(t)
	accessSince := time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC)
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&accessSince, true, nil)

	parentCreated := time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC) // pre-accessSince
	reply := models.Message{
		MessageID:             "reply",
		RoomID:                "r1",
		CreatedAt:             time.Date(2026, 5, 12, 9, 0, 0, 0, time.UTC), // post-accessSince
		Sender:                models.Participant{ID: "u2", Account: "bob"},
		Msg:                   "reply content that would leak parent context",
		TShow:                 true,
		ThreadParentID:        "parent-msg",
		ThreadParentCreatedAt: &parentCreated,
	}
	msgs.EXPECT().GetPinnedMessages(gomock.Any(), "r1", gomock.Any()).Return(pinnedPage([]models.Message{reply}), nil)

	resp, err := svc.ListPinnedMessages(testContext(), models.ListPinnedMessagesRequest{})

	require.NoError(t, err)
	require.Len(t, resp.Messages, 1)
	assert.Equal(t, "This message is unavailable", resp.Messages[0].Msg, "reply must be stubbed because its parent is inaccessible")
	assert.Equal(t, "reply", resp.Messages[0].MessageID, "identifier stays visible for the placeholder")
}

func TestListPinnedMessages_StubsThreadReplyWithMissingParentTime(t *testing.T) {
	// nil ThreadParentCreatedAt → redact conservatively. TShow=false locks in
	// that the fallback fires for ALL thread replies, not just TShow=true ones.
	svc, msgs, subs, _, _, _, _, _ := newServiceWithRoomMock(t)
	accessSince := time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC)
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&accessSince, true, nil)

	reply := models.Message{
		MessageID:      "legacy-reply",
		RoomID:         "r1",
		CreatedAt:      time.Date(2026, 5, 12, 9, 0, 0, 0, time.UTC),
		Sender:         models.Participant{ID: "u2", Account: "bob"},
		Msg:            "reply with no captured parent time",
		TShow:          false, // ← proves the gate fires on ThreadParentID alone
		ThreadParentID: "parent-msg",
		// ThreadParentCreatedAt intentionally nil
	}
	msgs.EXPECT().GetPinnedMessages(gomock.Any(), "r1", gomock.Any()).Return(pinnedPage([]models.Message{reply}), nil)

	resp, err := svc.ListPinnedMessages(testContext(), models.ListPinnedMessagesRequest{})

	require.NoError(t, err)
	require.Len(t, resp.Messages, 1)
	assert.Equal(t, "This message is unavailable", resp.Messages[0].Msg)
}

func TestListPinnedMessages_StubsThreadOnlyReplyWithPreAccessParent(t *testing.T) {
	// TShow=false reply with pre-access parent → stubbed; user can't reach
	// thread-only replies without thread access, so the pin must be redacted.
	svc, msgs, subs, _, _, _, _, _ := newServiceWithRoomMock(t)
	accessSince := time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC)
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&accessSince, true, nil)

	parentCreated := time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC) // pre-accessSince
	reply := models.Message{
		MessageID:             "thread-only-reply",
		RoomID:                "r1",
		CreatedAt:             time.Date(2026, 5, 12, 9, 0, 0, 0, time.UTC),
		Sender:                models.Participant{ID: "u2", Account: "bob"},
		Msg:                   "thread-only content",
		TShow:                 false, // ← thread-only, not broadcast to channel
		ThreadParentID:        "parent-msg",
		ThreadParentCreatedAt: &parentCreated,
	}
	msgs.EXPECT().GetPinnedMessages(gomock.Any(), "r1", gomock.Any()).Return(pinnedPage([]models.Message{reply}), nil)

	resp, err := svc.ListPinnedMessages(testContext(), models.ListPinnedMessagesRequest{})

	require.NoError(t, err)
	require.Len(t, resp.Messages, 1)
	assert.Equal(t, "This message is unavailable", resp.Messages[0].Msg, "TShow=false reply must redact like TShow=true when parent is inaccessible")
}

func TestListPinnedMessages_ShowsThreadReplyWithPostAccessParent(t *testing.T) {
	// Both reply and parent post-access → fully visible (positive case).
	svc, msgs, subs, _, _, _, _, _ := newServiceWithRoomMock(t)
	accessSince := time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC)
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&accessSince, true, nil)

	parentCreated := time.Date(2026, 5, 11, 9, 0, 0, 0, time.UTC) // post-accessSince
	reply := models.Message{
		MessageID:             "reply",
		RoomID:                "r1",
		CreatedAt:             time.Date(2026, 5, 12, 9, 0, 0, 0, time.UTC),
		Sender:                models.Participant{ID: "u2", Account: "bob"},
		Msg:                   "visible reply",
		TShow:                 true,
		ThreadParentID:        "parent-msg",
		ThreadParentCreatedAt: &parentCreated,
	}
	msgs.EXPECT().GetPinnedMessages(gomock.Any(), "r1", gomock.Any()).Return(pinnedPage([]models.Message{reply}), nil)

	resp, err := svc.ListPinnedMessages(testContext(), models.ListPinnedMessagesRequest{})

	require.NoError(t, err)
	require.Len(t, resp.Messages, 1)
	assert.Equal(t, "visible reply", resp.Messages[0].Msg)
}

func TestListPinnedMessages_RedactsPreAccessQuotedParent(t *testing.T) {
	// Post-access pin with pre-access quoted parent → quote body redacted.
	svc, msgs, subs, _, _, _, _, _ := newServiceWithRoomMock(t)
	accessSince := time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC)
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&accessSince, true, nil)

	preAccessQuote := &models.QuotedParentMessage{
		MessageID: "old-msg",
		Msg:       "secret content from before user joined",
		CreatedAt: time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC), // pre-accessSince
	}
	pinned := pinnableMsg()
	pinned.CreatedAt = time.Date(2026, 5, 12, 9, 0, 0, 0, time.UTC) // post-accessSince
	pinned.QuotedParentMessage = preAccessQuote

	msgs.EXPECT().GetPinnedMessages(gomock.Any(), "r1", gomock.Any()).
		Return(pinnedPage([]models.Message{*pinned}), nil)

	resp, err := svc.ListPinnedMessages(testContext(), models.ListPinnedMessagesRequest{})

	require.NoError(t, err)
	require.Len(t, resp.Messages, 1)
	require.NotNil(t, resp.Messages[0].QuotedParentMessage)
	assert.Equal(t, "This message is unavailable", resp.Messages[0].QuotedParentMessage.Msg)
	assert.Empty(t, resp.Messages[0].QuotedParentMessage.MessageID, "redacted quote should not leak the original messageId")
}

func TestListPinnedMessages_NotSubscribed(t *testing.T) {
	svc, _, subs, _, _, _, _, _ := newServiceWithRoomMock(t)
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, false, nil)

	_, err := svc.ListPinnedMessages(testContext(), models.ListPinnedMessagesRequest{})

	assertForbiddenErr(t, err, "not subscribed to room")
}

func TestPinMessage_SubscriptionError(t *testing.T) {
	svc, _, subs, _, _, _ := newPinTestService(t)
	subs.EXPECT().GetSubscription(gomock.Any(), "u1", "r1").Return(nil, errors.New("mongo down"))

	_, err := svc.PinMessage(testContext(), "site-a", models.PinMessageRequest{MessageID: "m1"})

	assertInternalErr(t, err, "get subscription")
}

func TestPinMessage_RoomUserCountError(t *testing.T) {
	svc, msgs, subs, rooms, _, _ := newPinTestService(t)
	subs.EXPECT().GetSubscription(gomock.Any(), "u1", "r1").Return(subFor(model.RoleMember), nil)
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(pinnableMsg(), nil)
	rooms.EXPECT().GetRoomUserCount(gomock.Any(), "r1").Return(0, errors.New("mongo down"))

	_, err := svc.PinMessage(testContext(), "site-a", models.PinMessageRequest{MessageID: "m1"})

	assertInternalErr(t, err, "get room user count")
}

func TestPinMessage_WriteError(t *testing.T) {
	// Passes authz; PinMessage write fails. No publish expected.
	svc, msgs, subs, rooms, _, _ := newPinTestService(t)
	subs.EXPECT().GetSubscription(gomock.Any(), "u1", "r1").Return(subFor(model.RoleMember), nil)
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(pinnableMsg(), nil)
	rooms.EXPECT().GetRoomUserCount(gomock.Any(), "r1").Return(1, nil)
	msgs.EXPECT().GetAllPinnedMessages(gomock.Any(), "r1").Return(pinnedList(0), nil)
	msgs.EXPECT().PinMessage(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("cass down"))

	_, err := svc.PinMessage(testContext(), "site-a", models.PinMessageRequest{MessageID: "m1"})

	assertInternalErr(t, err, "pin message m1")
}

func TestUnpinMessage_WriteError(t *testing.T) {
	// UnpinMessage write fails. No publish expected.
	svc, msgs, subs, rooms, _, _ := newPinTestService(t)
	subs.EXPECT().GetSubscription(gomock.Any(), "u1", "r1").Return(subFor(model.RoleMember), nil)
	rooms.EXPECT().GetRoomUserCount(gomock.Any(), "r1").Return(1, nil)
	pinned := pinnableMsg()
	at := time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC)
	pinned.PinnedAt = &at
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(pinned, nil)
	msgs.EXPECT().UnpinMessage(gomock.Any(), pinned).Return(errors.New("cass down"))

	_, err := svc.UnpinMessage(testContext(), "site-a", models.UnpinMessageRequest{MessageID: "m1"})

	assertInternalErr(t, err, "unpin message m1")
}

func TestListPinnedMessages_StoreError(t *testing.T) {
	svc, msgs, subs, _, _, _, _, _ := newServiceWithRoomMock(t)
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	msgs.EXPECT().GetPinnedMessages(gomock.Any(), "r1", gomock.Any()).
		Return(cassrepo.Page[models.Message]{}, errors.New("cass down"))

	_, err := svc.ListPinnedMessages(testContext(), models.ListPinnedMessagesRequest{})

	assertInternalErr(t, err, "list pinned messages")
}

func TestPinMessage_BotAccountBypassesLargeRoom(t *testing.T) {
	// Bot account (matches \.bot$|^p_) bypasses large-room check; pin-limit still runs.
	svc, msgs, subs, _, pub, _ := newPinTestService(t)
	botSub := &model.Subscription{
		RoomID: "r1",
		User:   model.SubscriptionUser{ID: "u1", Account: "svc.bot"},
		Roles:  []model.Role{model.RoleMember},
	}
	subs.EXPECT().GetSubscription(gomock.Any(), "u1", "r1").Return(botSub, nil)
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(pinnableMsg(), nil)
	msgs.EXPECT().GetAllPinnedMessages(gomock.Any(), "r1").Return(pinnedList(0), nil)
	msgs.EXPECT().PinMessage(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	pub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	_, err := svc.PinMessage(testContext(), "site-a", models.PinMessageRequest{MessageID: "m1"})

	require.NoError(t, err)
}

func TestPinMessage_SelfHealsHalfAppliedPin(t *testing.T) {
	// Half-applied prior pin (msg.PinnedAt=nil but row exists at T1) → retry
	// must reuse T1 so the INSERT is an idempotent UPSERT, not a duplicate row.
	svc, msgs, subs, rooms, pub, _ := newPinTestService(t)
	subs.EXPECT().GetSubscription(gomock.Any(), "u1", "r1").Return(subFor(model.RoleMember), nil)
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(pinnableMsg(), nil)
	rooms.EXPECT().GetRoomUserCount(gomock.Any(), "r1").Return(1, nil)

	orphanPinnedAt := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	orphan := models.Message{MessageID: "m1", PinnedAt: &orphanPinnedAt}
	msgs.EXPECT().GetAllPinnedMessages(gomock.Any(), "r1").
		Return([]models.Message{orphan}, nil)

	var gotPinnedAt time.Time
	msgs.EXPECT().PinMessage(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ *models.Message, pinnedAt time.Time, _ models.Participant) error {
			gotPinnedAt = pinnedAt
			return nil
		})
	pub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	_, err := svc.PinMessage(testContext(), "site-a", models.PinMessageRequest{MessageID: "m1"})

	require.NoError(t, err)
	assert.True(t, gotPinnedAt.Equal(orphanPinnedAt), "must reuse orphan's pinned_at, not generate a new one")
}

func TestPinMessage_OrphanBypassesPinLimit(t *testing.T) {
	// Room at cap with orphan for this message → cap doesn't block; we're healing, not adding.
	svc, msgs, subs, rooms, pub, _ := newPinTestService(t)
	subs.EXPECT().GetSubscription(gomock.Any(), "u1", "r1").Return(subFor(model.RoleMember), nil)
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(pinnableMsg(), nil)
	rooms.EXPECT().GetRoomUserCount(gomock.Any(), "r1").Return(1, nil)

	orphanPinnedAt := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	full := make([]models.Message, testMaxPinnedPerRoom)
	full[0] = models.Message{MessageID: "m1", PinnedAt: &orphanPinnedAt}
	msgs.EXPECT().GetAllPinnedMessages(gomock.Any(), "r1").Return(full, nil)
	msgs.EXPECT().PinMessage(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	pub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	_, err := svc.PinMessage(testContext(), "site-a", models.PinMessageRequest{MessageID: "m1"})

	require.NoError(t, err)
}

// --- Pin-limit tests ---

func TestPinMessage_PinLimitReached(t *testing.T) {
	// Hard cap (no role bypass): room at testMaxPinnedPerRoom → forbidden.
	svc, msgs, subs, rooms, _, _ := newPinTestService(t)
	subs.EXPECT().GetSubscription(gomock.Any(), "u1", "r1").Return(subFor(model.RoleMember), nil)
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(pinnableMsg(), nil)
	rooms.EXPECT().GetRoomUserCount(gomock.Any(), "r1").Return(1, nil)
	msgs.EXPECT().GetAllPinnedMessages(gomock.Any(), "r1").Return(pinnedList(testMaxPinnedPerRoom), nil)

	_, err := svc.PinMessage(testContext(), "site-a", models.PinMessageRequest{MessageID: "m1"})

	assertForbiddenErr(t, err, "room pin limit reached")
}

func TestPinMessage_PinLimitReached_OwnerNoBypass(t *testing.T) {
	// Owners bypass large-room but NOT the pin cap.
	svc, msgs, subs, _, _, _ := newPinTestService(t)
	subs.EXPECT().GetSubscription(gomock.Any(), "u1", "r1").Return(subFor(model.RoleOwner), nil)
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(pinnableMsg(), nil)
	msgs.EXPECT().GetAllPinnedMessages(gomock.Any(), "r1").Return(pinnedList(testMaxPinnedPerRoom), nil)

	_, err := svc.PinMessage(testContext(), "site-a", models.PinMessageRequest{MessageID: "m1"})

	assertForbiddenErr(t, err, "room pin limit reached")
}

func TestPinMessage_PinLimitJustUnderSucceeds(t *testing.T) {
	// cap-1 existing → new pin fits; write + publish proceed.
	svc, msgs, subs, rooms, pub, _ := newPinTestService(t)
	subs.EXPECT().GetSubscription(gomock.Any(), "u1", "r1").Return(subFor(model.RoleMember), nil)
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(pinnableMsg(), nil)
	rooms.EXPECT().GetRoomUserCount(gomock.Any(), "r1").Return(1, nil)
	msgs.EXPECT().GetAllPinnedMessages(gomock.Any(), "r1").Return(pinnedList(testMaxPinnedPerRoom-1), nil)
	msgs.EXPECT().PinMessage(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	pub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	_, err := svc.PinMessage(testContext(), "site-a", models.PinMessageRequest{MessageID: "m1"})

	require.NoError(t, err)
}

func TestPinMessage_PinLimitCountError(t *testing.T) {
	// GetAllPinnedMessages error → wrapped "count pinned messages" → internal at boundary; no write/publish.
	svc, msgs, subs, rooms, _, _ := newPinTestService(t)
	subs.EXPECT().GetSubscription(gomock.Any(), "u1", "r1").Return(subFor(model.RoleMember), nil)
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(pinnableMsg(), nil)
	rooms.EXPECT().GetRoomUserCount(gomock.Any(), "r1").Return(1, nil)
	msgs.EXPECT().GetAllPinnedMessages(gomock.Any(), "r1").
		Return(nil, errors.New("cass down"))

	_, err := svc.PinMessage(testContext(), "site-a", models.PinMessageRequest{MessageID: "m1"})

	assertInternalErr(t, err, "count pinned messages")
}

func TestPinMessage_PinLimitSkippedOnIdempotentRepin(t *testing.T) {
	// Already pinned: idempotent short-circuit fires before enforcePinLimit
	// (and enforceLargeRoomPin); strict mock proves GetAllPinnedMessages is not called.
	svc, msgs, subs, _, _, _ := newPinTestService(t)
	subs.EXPECT().GetSubscription(gomock.Any(), "u1", "r1").Return(subFor(model.RoleMember), nil)
	already := pinnableMsg()
	at := time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC)
	already.PinnedAt = &at
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(already, nil)

	resp, err := svc.PinMessage(testContext(), "site-a", models.PinMessageRequest{MessageID: "m1"})

	require.NoError(t, err)
	assert.Equal(t, at.UnixMilli(), resp.PinnedAt)
}
