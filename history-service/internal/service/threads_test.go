package service_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/history-service/internal/cassrepo"
	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/history-service/internal/service"
	pkgmodel "github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsrouter"
)

func makeThreadRooms() []pkgmodel.ThreadRoom {
	return []pkgmodel.ThreadRoom{
		{ID: "tr-1", RoomID: "r1", ParentMessageID: "p1", LastMsgAt: time.Date(2026, 2, 1, 2, 0, 0, 0, time.UTC), ReplyAccounts: []string{"alice"}},
		{ID: "tr-2", RoomID: "r1", ParentMessageID: "p2", LastMsgAt: time.Date(2026, 2, 1, 1, 0, 0, 0, time.UTC), ReplyAccounts: []string{"bob"}},
	}
}

func intPtr(v int) *int { return &v }

func makeCassMessages() []models.Message {
	return []models.Message{
		{MessageID: "p1", RoomID: "r1", Msg: "parent 1", TCount: intPtr(5)},
		{MessageID: "p2", RoomID: "r1", Msg: "parent 2", TCount: intPtr(3)},
	}
}

func makeThreadPage(total int64) mongoutil.OffsetPage[pkgmodel.ThreadRoom] {
	return mongoutil.OffsetPage[pkgmodel.ThreadRoom]{Data: makeThreadRooms(), Total: total}
}

// ============================================================
// GetThreadMessages
// ============================================================

func TestHistoryService_GetThreadMessages_Success(t *testing.T) {
	svc, msgs, subs, _, _, _ := newService(t, true)
	c := testContext()

	parentCreatedAt := joinTime.Add(5 * time.Minute)
	parent := &models.Message{MessageID: "m-parent", RoomID: "r1", CreatedAt: parentCreatedAt, ThreadRoomID: "tr-1"}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m-parent").Return(parent, nil)
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)

	replies := []models.Message{
		{MessageID: "reply-2", RoomID: "r1", ThreadRoomID: "tr-1", ThreadParentID: "m-parent", CreatedAt: parentCreatedAt.Add(2 * time.Minute)},
		{MessageID: "reply-1", RoomID: "r1", ThreadRoomID: "tr-1", ThreadParentID: "m-parent", CreatedAt: parentCreatedAt.Add(1 * time.Minute)},
	}
	msgs.EXPECT().GetThreadMessages(gomock.Any(), "r1", "tr-1", gomock.Any(), gomock.Any(), gomock.Any()).Return(makePage(replies, false), nil)

	resp, err := svc.GetThreadMessages(c, models.GetThreadMessagesRequest{ThreadMessageID: "m-parent"})
	require.NoError(t, err)
	assert.Len(t, resp.Messages, 2)
	assert.Equal(t, "reply-2", resp.Messages[0].MessageID)
	assert.False(t, resp.HasNext)
	assert.Empty(t, resp.NextCursor)
}

func TestHistoryService_GetThreadMessages_HasNextAndCursor(t *testing.T) {
	svc, msgs, subs, _, _, _ := newService(t, true)
	c := testContext()

	parent := &models.Message{MessageID: "m-parent", RoomID: "r1", CreatedAt: joinTime.Add(5 * time.Minute), ThreadRoomID: "tr-1"}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m-parent").Return(parent, nil)
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)

	replies := []models.Message{
		{MessageID: "reply-2", RoomID: "r1", ThreadRoomID: "tr-1", CreatedAt: joinTime.Add(7 * time.Minute)},
		{MessageID: "reply-1", RoomID: "r1", ThreadRoomID: "tr-1", CreatedAt: joinTime.Add(6 * time.Minute)},
	}
	msgs.EXPECT().GetThreadMessages(gomock.Any(), "r1", "tr-1", gomock.Any(), gomock.Any(), gomock.Any()).Return(makePage(replies, true), nil)

	resp, err := svc.GetThreadMessages(c, models.GetThreadMessagesRequest{ThreadMessageID: "m-parent"})
	require.NoError(t, err)
	assert.True(t, resp.HasNext)
	assert.NotEmpty(t, resp.NextCursor)
}

func TestHistoryService_GetThreadMessages_EmptyThreadMessageID(t *testing.T) {
	svc, _, _, _, _, _ := newService(t, true)
	c := testContext()

	_, err := svc.GetThreadMessages(c, models.GetThreadMessagesRequest{})
	require.Error(t, err)
	assertBadRequestErr(t, err, "threadMessageId is required")
}

func TestHistoryService_GetThreadMessages_ParentNotFound(t *testing.T) {
	svc, msgs, subs, _, _, _ := newService(t, true)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m-unknown").Return(nil, nil)

	_, err := svc.GetThreadMessages(c, models.GetThreadMessagesRequest{ThreadMessageID: "m-unknown"})
	require.Error(t, err)
	assertNotFoundErr(t, err, "message not found")
}

func TestHistoryService_GetThreadMessages_ParentLookupError(t *testing.T) {
	svc, msgs, subs, _, _, _ := newService(t, true)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m-parent").Return(nil, fmt.Errorf("db down"))

	_, err := svc.GetThreadMessages(c, models.GetThreadMessagesRequest{ThreadMessageID: "m-parent"})
	require.Error(t, err)
	assertInternalErr(t, err, "failed to retrieve message")
}

func TestHistoryService_GetThreadMessages_NotSubscribed(t *testing.T) {
	svc, _, subs, _, _, _ := newService(t, true)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, false, nil)

	_, err := svc.GetThreadMessages(c, models.GetThreadMessagesRequest{ThreadMessageID: "m-parent"})
	require.Error(t, err)
	assertForbiddenErr(t, err, "not subscribed to room")
}

func TestHistoryService_GetThreadMessages_SubscriptionStoreError(t *testing.T) {
	svc, _, subs, _, _, _ := newService(t, true)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, false, fmt.Errorf("db error"))

	_, err := svc.GetThreadMessages(c, models.GetThreadMessagesRequest{ThreadMessageID: "m-parent"})
	require.Error(t, err)
	assertInternalErr(t, err, "unable to verify room access")
}

func TestHistoryService_GetThreadMessages_ParentBeforeAccessSince(t *testing.T) {
	svc, msgs, subs, _, _, _ := newService(t, true)
	c := testContext()

	parent := &models.Message{MessageID: "m-parent", RoomID: "r1", CreatedAt: joinTime.Add(-1 * time.Hour)}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m-parent").Return(parent, nil)
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)

	_, err := svc.GetThreadMessages(c, models.GetThreadMessagesRequest{ThreadMessageID: "m-parent"})
	require.Error(t, err)
	assertForbiddenErr(t, err, "thread is outside access window")
}

func TestHistoryService_GetThreadMessages_NoHSS(t *testing.T) {
	svc, msgs, subs, _, _, _ := newService(t, true)
	c := testContext()

	parent := &models.Message{MessageID: "m-parent", RoomID: "r1", CreatedAt: joinTime.Add(-1 * time.Hour), ThreadRoomID: "tr-1"}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m-parent").Return(parent, nil)
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	msgs.EXPECT().GetThreadMessages(gomock.Any(), "r1", "tr-1", gomock.Any(), gomock.Any(), gomock.Any()).Return(makePage(nil, false), nil)

	resp, err := svc.GetThreadMessages(c, models.GetThreadMessagesRequest{ThreadMessageID: "m-parent"})
	require.NoError(t, err)
	assert.Empty(t, resp.Messages)
}

// ThreadRoomID is empty when message-worker couldn't stamp it (ThreadParentMessageCreatedAt absent).
func TestHistoryService_GetThreadMessages_ThreadRoomIDEmpty(t *testing.T) {
	svc, msgs, subs, _, _, _ := newService(t, true)
	c := testContext()

	parent := &models.Message{MessageID: "m-parent", RoomID: "r1", CreatedAt: joinTime.Add(5 * time.Minute), ThreadRoomID: ""}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m-parent").Return(parent, nil)
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)

	resp, err := svc.GetThreadMessages(c, models.GetThreadMessagesRequest{ThreadMessageID: "m-parent"})
	require.NoError(t, err)
	assert.Empty(t, resp.Messages)
	assert.False(t, resp.HasNext)
}

func TestHistoryService_GetThreadMessages_InvalidCursor(t *testing.T) {
	svc, msgs, subs, _, _, _ := newService(t, true)
	c := testContext()

	parent := &models.Message{MessageID: "m-parent", RoomID: "r1", CreatedAt: joinTime.Add(5 * time.Minute), ThreadRoomID: "tr-1"}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m-parent").Return(parent, nil)
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)

	_, err := svc.GetThreadMessages(c, models.GetThreadMessagesRequest{
		ThreadMessageID: "m-parent",
		Cursor:          "!!not-base64!!",
	})
	require.Error(t, err)
	assertBadRequestErr(t, err, "invalid pagination cursor")
}

func TestHistoryService_GetThreadMessages_RepoError(t *testing.T) {
	svc, msgs, subs, _, _, _ := newService(t, true)
	c := testContext()

	parent := &models.Message{MessageID: "m-parent", RoomID: "r1", CreatedAt: joinTime.Add(5 * time.Minute), ThreadRoomID: "tr-1"}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m-parent").Return(parent, nil)
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetThreadMessages(gomock.Any(), "r1", "tr-1", gomock.Any(), gomock.Any(), gomock.Any()).Return(cassrepo.Page[models.Message]{}, fmt.Errorf("db error"))

	_, err := svc.GetThreadMessages(c, models.GetThreadMessagesRequest{ThreadMessageID: "m-parent"})
	require.Error(t, err)
	assertInternalErr(t, err, "failed to load thread messages")
}

func TestHistoryService_GetThreadMessages_Limits(t *testing.T) {
	cases := []struct {
		name         string
		limit        int
		wantPageSize int
	}{
		{"default (zero)", 0, 20},
		{"negative", -5, 20},
		{"clamps to max", 999, 100},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, msgs, subs, _, _, _ := newService(t, true)
			c := testContext()

			parent := &models.Message{MessageID: "m-parent", RoomID: "r1", CreatedAt: joinTime.Add(5 * time.Minute), ThreadRoomID: "tr-1"}
			subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
			msgs.EXPECT().GetMessageByID(gomock.Any(), "m-parent").Return(parent, nil)
			msgs.EXPECT().GetThreadMessages(gomock.Any(), "r1", "tr-1", gomock.Any(), gomock.Any(), gomock.Cond(func(x any) bool {
				pr, ok := x.(cassrepo.PageRequest)
				return ok && pr.PageSize == tc.wantPageSize
			})).Return(makePage(nil, false), nil)

			_, err := svc.GetThreadMessages(c, models.GetThreadMessagesRequest{ThreadMessageID: "m-parent", Limit: tc.limit})
			require.NoError(t, err)
		})
	}
}

func TestHistoryService_GetThreadMessages_ReplyIDReturnsBadRequest(t *testing.T) {
	svc, msgs, subs, _, _, _ := newService(t, true)
	c := testContext()

	reply := &models.Message{MessageID: "reply-1", RoomID: "r1", CreatedAt: joinTime.Add(10 * time.Minute), ThreadParentID: "m-parent"}

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessageByID(gomock.Any(), "reply-1").Return(reply, nil)

	_, err := svc.GetThreadMessages(c, models.GetThreadMessagesRequest{ThreadMessageID: "reply-1"})
	require.Error(t, err)
	assertBadRequestErr(t, err, "threadMessageId must be a top-level message, not a reply")
}

func TestHistoryService_GetThreadMessages_ParentBeforeAccessSinceReturnsForbidden(t *testing.T) {
	svc, msgs, subs, _, _, _ := newService(t, true)
	c := testContext()

	parent := &models.Message{MessageID: "m-parent", RoomID: "r1", CreatedAt: joinTime.Add(-1 * time.Hour), ThreadRoomID: "tr-1"}

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m-parent").Return(parent, nil)

	_, err := svc.GetThreadMessages(c, models.GetThreadMessagesRequest{ThreadMessageID: "m-parent"})
	require.Error(t, err)
	assertForbiddenErr(t, err, "thread is outside access window")
}

// ============================================================
// GetThreadParentMessages
// ============================================================

func TestHistoryService_GetThreadParentMessages_All(t *testing.T) {
	svc, msgs, subs, _, threadRooms, _ := newService(t, true)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	threadRooms.EXPECT().GetThreadRooms(gomock.Any(), "r1", nil, gomock.Any()).Return(makeThreadPage(2), nil)
	msgs.EXPECT().GetMessagesByIDs(gomock.Any(), gomock.Any()).Return(makeCassMessages(), nil)

	resp, err := svc.GetThreadParentMessages(c, models.GetThreadParentMessagesRequest{Filter: models.ThreadFilterAll, Limit: 20})
	require.NoError(t, err)
	assert.Len(t, resp.ParentMessages, 2)
	assert.Equal(t, int64(2), resp.Total)
	assert.Equal(t, "p1", resp.ParentMessages[0].MessageID)
	assert.Equal(t, intPtr(5), resp.ParentMessages[0].TCount)
}

func TestHistoryService_GetThreadParentMessages_Total(t *testing.T) {
	svc, msgs, subs, _, threadRooms, _ := newService(t, true)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	// Total=42 simulates a large result set with only 2 items on this page
	threadRooms.EXPECT().GetThreadRooms(gomock.Any(), "r1", nil, gomock.Any()).Return(makeThreadPage(42), nil)
	msgs.EXPECT().GetMessagesByIDs(gomock.Any(), gomock.Any()).Return(makeCassMessages(), nil)

	resp, err := svc.GetThreadParentMessages(c, models.GetThreadParentMessagesRequest{Limit: 2})
	require.NoError(t, err)
	assert.Equal(t, int64(42), resp.Total)
}

func TestHistoryService_GetThreadParentMessages_Following(t *testing.T) {
	svc, msgs, subs, _, threadRooms, _ := newService(t, true)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	threadRooms.EXPECT().GetFollowingThreadRooms(gomock.Any(), "r1", "u1", nil, gomock.Any()).Return(makeThreadPage(2), nil)
	msgs.EXPECT().GetMessagesByIDs(gomock.Any(), gomock.Any()).Return(makeCassMessages(), nil)

	resp, err := svc.GetThreadParentMessages(c, models.GetThreadParentMessagesRequest{Filter: models.ThreadFilterFollowing, Limit: 20})
	require.NoError(t, err)
	assert.Len(t, resp.ParentMessages, 2)
}

func TestHistoryService_GetThreadParentMessages_Unread(t *testing.T) {
	svc, msgs, subs, _, threadRooms, _ := newService(t, true)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	threadRooms.EXPECT().GetUnreadThreadRooms(gomock.Any(), "r1", "u1", nil, gomock.Any()).Return(makeThreadPage(2), nil)
	msgs.EXPECT().GetMessagesByIDs(gomock.Any(), gomock.Any()).Return(makeCassMessages(), nil)

	resp, err := svc.GetThreadParentMessages(c, models.GetThreadParentMessagesRequest{Filter: models.ThreadFilterUnread, Limit: 20})
	require.NoError(t, err)
	assert.Len(t, resp.ParentMessages, 2)
	assert.Equal(t, int64(2), resp.Total)
}

func TestHistoryService_GetThreadParentMessages_NotSubscribed(t *testing.T) {
	svc, _, subs, _, _, _ := newService(t, true)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, false, nil)

	_, err := svc.GetThreadParentMessages(c, models.GetThreadParentMessagesRequest{Limit: 20})
	require.Error(t, err)
	assertForbiddenErr(t, err, "not subscribed to room")
}

func TestHistoryService_GetThreadParentMessages_Empty(t *testing.T) {
	svc, _, subs, _, threadRooms, _ := newService(t, true)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	threadRooms.EXPECT().GetThreadRooms(gomock.Any(), "r1", nil, gomock.Any()).Return(
		mongoutil.OffsetPage[pkgmodel.ThreadRoom]{Data: []pkgmodel.ThreadRoom{}, Total: 0}, nil,
	)

	resp, err := svc.GetThreadParentMessages(c, models.GetThreadParentMessagesRequest{Limit: 20})
	require.NoError(t, err)
	assert.Empty(t, resp.ParentMessages)
	assert.Equal(t, int64(0), resp.Total)
}

func TestHistoryService_GetThreadParentMessages_SubscriptionError(t *testing.T) {
	svc, _, subs, _, _, _ := newService(t, true)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, false, fmt.Errorf("db error"))

	_, err := svc.GetThreadParentMessages(c, models.GetThreadParentMessagesRequest{Limit: 20})
	require.Error(t, err)
	assertInternalErr(t, err, "unable to verify room access")
}

func TestHistoryService_GetThreadParentMessages_ThreadRoomError(t *testing.T) {
	svc, _, subs, _, threadRooms, _ := newService(t, true)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	threadRooms.EXPECT().GetThreadRooms(gomock.Any(), "r1", nil, gomock.Any()).Return(
		mongoutil.OffsetPage[pkgmodel.ThreadRoom]{}, fmt.Errorf("mongo down"),
	)

	_, err := svc.GetThreadParentMessages(c, models.GetThreadParentMessagesRequest{Limit: 20})
	require.Error(t, err)
	assertInternalErr(t, err, "failed to load thread parent messages")
}

func TestHistoryService_GetThreadParentMessages_CassandraError(t *testing.T) {
	svc, msgs, subs, _, threadRooms, _ := newService(t, true)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	threadRooms.EXPECT().GetThreadRooms(gomock.Any(), "r1", nil, gomock.Any()).Return(makeThreadPage(2), nil)
	msgs.EXPECT().GetMessagesByIDs(gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("cassandra down"))

	_, err := svc.GetThreadParentMessages(c, models.GetThreadParentMessagesRequest{Limit: 20})
	require.Error(t, err)
	assertInternalErr(t, err, "failed to load thread parent messages")
}

func TestHistoryService_GetThreadParentMessages_MissingParentIgnored(t *testing.T) {
	svc, msgs, subs, _, threadRooms, _ := newService(t, true)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	threadRooms.EXPECT().GetThreadRooms(gomock.Any(), "r1", nil, gomock.Any()).Return(makeThreadPage(2), nil)
	msgs.EXPECT().GetMessagesByIDs(gomock.Any(), gomock.Any()).Return(
		[]models.Message{{MessageID: "p1", RoomID: "r1", Msg: "parent 1"}}, nil,
	)

	resp, err := svc.GetThreadParentMessages(c, models.GetThreadParentMessagesRequest{Limit: 20})
	require.NoError(t, err)
	assert.Len(t, resp.ParentMessages, 1)
	assert.Equal(t, "p1", resp.ParentMessages[0].MessageID)
	// Total is from MongoDB count, not hydrated count
	assert.Equal(t, int64(2), resp.Total)
}

func TestHistoryService_GetThreadParentMessages_DeduplicatesParentIDs(t *testing.T) {
	svc, msgs, subs, _, threadRooms, _ := newService(t, true)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)

	// Two thread rooms pointing to the same ParentMessageID — seenIDs must deduplicate.
	dupPage := mongoutil.OffsetPage[pkgmodel.ThreadRoom]{
		Data: []pkgmodel.ThreadRoom{
			{ID: "tr-1", RoomID: "r1", ParentMessageID: "p1"},
			{ID: "tr-2", RoomID: "r1", ParentMessageID: "p1"},
		},
		Total: 2,
	}
	threadRooms.EXPECT().GetThreadRooms(gomock.Any(), "r1", nil, gomock.Any()).Return(dupPage, nil)

	// Must be called with exactly ["p1"], not ["p1","p1"].
	msgs.EXPECT().GetMessagesByIDs(gomock.Any(), []string{"p1"}).Return(
		[]models.Message{{MessageID: "p1", RoomID: "r1", Msg: "the parent"}}, nil,
	)

	resp, err := svc.GetThreadParentMessages(c, models.GetThreadParentMessagesRequest{Limit: 20})
	require.NoError(t, err)
	assert.Len(t, resp.ParentMessages, 1)
	assert.Equal(t, "p1", resp.ParentMessages[0].MessageID)
}

func TestHistoryService_GetThreadParentMessages_InvalidFilter(t *testing.T) {
	svc, _, subs, _, _, _ := newService(t, true)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)

	_, err := svc.GetThreadParentMessages(c, models.GetThreadParentMessagesRequest{Filter: "bogus", Limit: 20})
	require.Error(t, err)
	var routeErr *natsrouter.RouteError
	require.ErrorAs(t, err, &routeErr)
	assert.Equal(t, natsrouter.CodeBadRequest, routeErr.Code)
}

// ============================================================
// Quote redaction — thread endpoints
// ============================================================

func TestHistoryService_GetThreadMessages_RedactsQuoteBeforeAccessSince(t *testing.T) {
	svc, msgs, subs, _, _, _ := newService(t, true)
	c := testContext()

	parentCreatedAt := joinTime.Add(5 * time.Minute)
	parent := &models.Message{MessageID: "m-parent", RoomID: "r1", CreatedAt: parentCreatedAt, ThreadRoomID: "tr-1"}
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m-parent").Return(parent, nil)

	quoteCreatedAt := joinTime.Add(-1 * time.Hour) // before accessSince
	replies := []models.Message{
		{
			MessageID: "reply-1", RoomID: "r1", CreatedAt: parentCreatedAt.Add(time.Minute),
			QuotedParentMessage: &models.QuotedParentMessage{
				MessageID: "old-msg", Msg: "secret content", CreatedAt: quoteCreatedAt,
			},
		},
	}
	msgs.EXPECT().GetThreadMessages(gomock.Any(), "r1", "tr-1", gomock.Any(), gomock.Any(), gomock.Any()).Return(makePage(replies, false), nil)

	resp, err := svc.GetThreadMessages(c, models.GetThreadMessagesRequest{ThreadMessageID: "m-parent"})
	require.NoError(t, err)
	require.Len(t, resp.Messages, 1)
	q := resp.Messages[0].QuotedParentMessage
	require.NotNil(t, q)
	assert.Equal(t, service.UnavailableQuoteMsg, q.Msg)
	assert.Empty(t, q.MessageID)
}

func TestHistoryService_GetThreadMessages_KeepsQuoteAfterAccessSince(t *testing.T) {
	svc, msgs, subs, _, _, _ := newService(t, true)
	c := testContext()

	parentCreatedAt := joinTime.Add(5 * time.Minute)
	parent := &models.Message{MessageID: "m-parent", RoomID: "r1", CreatedAt: parentCreatedAt, ThreadRoomID: "tr-1"}
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m-parent").Return(parent, nil)

	quoteCreatedAt := joinTime.Add(time.Minute) // after accessSince — should not be redacted
	replies := []models.Message{
		{
			MessageID: "reply-1", RoomID: "r1", CreatedAt: parentCreatedAt.Add(time.Minute),
			QuotedParentMessage: &models.QuotedParentMessage{
				MessageID: "visible-msg", Msg: "visible content", CreatedAt: quoteCreatedAt,
			},
		},
	}
	msgs.EXPECT().GetThreadMessages(gomock.Any(), "r1", "tr-1", gomock.Any(), gomock.Any(), gomock.Any()).Return(makePage(replies, false), nil)

	resp, err := svc.GetThreadMessages(c, models.GetThreadMessagesRequest{ThreadMessageID: "m-parent"})
	require.NoError(t, err)
	require.Len(t, resp.Messages, 1)
	q := resp.Messages[0].QuotedParentMessage
	require.NotNil(t, q)
	assert.Equal(t, "visible content", q.Msg)
	assert.Equal(t, "visible-msg", q.MessageID)
}

func TestHistoryService_GetThreadMessages_RedactsLegacyTShowMissingParentTimestamp(t *testing.T) {
	svc, msgs, subs, _, _, _ := newService(t, true)
	c := testContext()

	parentCreatedAt := joinTime.Add(5 * time.Minute)
	parent := &models.Message{MessageID: "m-parent", RoomID: "r1", CreatedAt: parentCreatedAt, ThreadRoomID: "tr-1"}
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m-parent").Return(parent, nil)

	// quoteCreatedAt is after accessSince — without the legacy-TShow branch it would pass through.
	quoteCreatedAt := joinTime.Add(time.Hour)
	replies := []models.Message{
		{
			MessageID: "reply-1", RoomID: "r1", CreatedAt: parentCreatedAt.Add(time.Minute),
			TShow: true,
			QuotedParentMessage: &models.QuotedParentMessage{
				MessageID:             "legacy-msg",
				Msg:                   "content",
				CreatedAt:             quoteCreatedAt,
				ThreadParentID:        "tp-1",
				ThreadParentCreatedAt: nil, // not captured by legacy message-worker
			},
		},
	}
	msgs.EXPECT().GetThreadMessages(gomock.Any(), "r1", "tr-1", gomock.Any(), gomock.Any(), gomock.Any()).Return(makePage(replies, false), nil)

	resp, err := svc.GetThreadMessages(c, models.GetThreadMessagesRequest{ThreadMessageID: "m-parent"})
	require.NoError(t, err)
	require.Len(t, resp.Messages, 1)
	q := resp.Messages[0].QuotedParentMessage
	require.NotNil(t, q)
	assert.Equal(t, service.UnavailableQuoteMsg, q.Msg)
	assert.Empty(t, q.MessageID)
}

func TestHistoryService_GetThreadParentMessages_RedactsQuoteBeforeAccessSince(t *testing.T) {
	svc, msgs, subs, _, threadRooms, _ := newService(t, true)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	threadRooms.EXPECT().GetThreadRooms(gomock.Any(), "r1", &joinTime, gomock.Any()).Return(makeThreadPage(1), nil)

	quoteCreatedAt := joinTime.Add(-1 * time.Hour) // before accessSince
	msgs.EXPECT().GetMessagesByIDs(gomock.Any(), gomock.Any()).Return([]models.Message{
		{
			MessageID: "p1", RoomID: "r1", Msg: "parent 1",
			CreatedAt: joinTime.Add(time.Minute), // parent itself is after accessSince
			QuotedParentMessage: &models.QuotedParentMessage{
				MessageID: "old-msg", Msg: "secret content", CreatedAt: quoteCreatedAt,
			},
		},
	}, nil)

	resp, err := svc.GetThreadParentMessages(c, models.GetThreadParentMessagesRequest{Limit: 20})
	require.NoError(t, err)
	require.Len(t, resp.ParentMessages, 1)
	q := resp.ParentMessages[0].QuotedParentMessage
	require.NotNil(t, q)
	assert.Equal(t, service.UnavailableQuoteMsg, q.Msg)
	assert.Empty(t, q.MessageID)
}

func TestHistoryService_GetThreadParentMessages_KeepsQuoteAfterAccessSince(t *testing.T) {
	svc, msgs, subs, _, threadRooms, _ := newService(t, true)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	threadRooms.EXPECT().GetThreadRooms(gomock.Any(), "r1", &joinTime, gomock.Any()).Return(makeThreadPage(1), nil)

	quoteCreatedAt := joinTime.Add(time.Minute) // after accessSince — should not be redacted
	msgs.EXPECT().GetMessagesByIDs(gomock.Any(), gomock.Any()).Return([]models.Message{
		{
			MessageID: "p1", RoomID: "r1", Msg: "parent 1",
			CreatedAt: joinTime.Add(time.Minute), // parent itself is after accessSince
			QuotedParentMessage: &models.QuotedParentMessage{
				MessageID: "visible-msg", Msg: "visible content", CreatedAt: quoteCreatedAt,
			},
		},
	}, nil)

	resp, err := svc.GetThreadParentMessages(c, models.GetThreadParentMessagesRequest{Limit: 20})
	require.NoError(t, err)
	require.Len(t, resp.ParentMessages, 1)
	q := resp.ParentMessages[0].QuotedParentMessage
	require.NotNil(t, q)
	assert.Equal(t, "visible content", q.Msg)
	assert.Equal(t, "visible-msg", q.MessageID)
}

func TestHistoryService_GetThreadParentMessages_PostHydrationAccessCheck(t *testing.T) {
	svc, msgs, subs, _, threadRooms, _ := newService(t, true)
	c := testContext()

	// MongoDB filter uses threadParentCreatedAt which can be zero (field absent from original event).
	// The thread room record has a zero ThreadParentCreatedAt, so MongoDB's $match on
	// threadParentCreatedAt > accessSince is bypassed. After hydrating from Cassandra, the
	// actual CreatedAt is before accessSince — the row must be excluded from results.
	earlyCreatedAt := joinTime.Add(-1 * time.Hour)
	threadRoom := pkgmodel.ThreadRoom{
		ID: "tr-early", RoomID: "r1", ParentMessageID: "p-early",
		// ThreadParentCreatedAt is zero — absent from original event, bypasses $match
	}
	page := mongoutil.OffsetPage[pkgmodel.ThreadRoom]{Data: []pkgmodel.ThreadRoom{threadRoom}, Total: 1}

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	threadRooms.EXPECT().GetThreadRooms(gomock.Any(), "r1", &joinTime, gomock.Any()).Return(page, nil)
	msgs.EXPECT().GetMessagesByIDs(gomock.Any(), []string{"p-early"}).Return([]models.Message{
		{MessageID: "p-early", RoomID: "r1", Msg: "old parent", CreatedAt: earlyCreatedAt},
	}, nil)

	resp, err := svc.GetThreadParentMessages(c, models.GetThreadParentMessagesRequest{Limit: 20})
	require.NoError(t, err)
	// The parent pre-dates accessSince — must be filtered out even though MongoDB passed it.
	assert.Empty(t, resp.ParentMessages)
	// Total reflects MongoDB's pre-hydration count, not the post-filter result.
	assert.Equal(t, int64(1), resp.Total)
}
