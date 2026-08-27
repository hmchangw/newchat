package service_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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
	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
)

var joinTime = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func testContext() *natsrouter.Context {
	return natsrouter.NewContext(map[string]string{"account": "u1", "roomID": "r1"})
}

func millis(t time.Time) *int64 {
	ms := t.UnixMilli()
	return &ms
}

func ptrTime(t time.Time) *time.Time { return &t }

// GetRoomTimes defaults wide enough that fixtures aren't clipped by the bucket-walk floor/ceiling.
var defaultRoomLastMsgAt = joinTime.Add(24 * time.Hour)
var defaultRoomCreatedAt = joinTime.Add(-30 * 24 * time.Hour)

func newService(t *testing.T, opts ...service.Option) (*service.HistoryService, *mocks.MockMessageRepository, *mocks.MockSubscriptionRepository, *mocks.MockEventPublisher, *mocks.MockThreadRoomRepository) {
	svc, msgs, subs, rooms, pub, threadRooms, _, _ := newServiceWithRoomMock(t, opts...)
	// Permissive defaults: existing tests don't care about the room reads.
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	// The mutation path persists what its walk resolved, and the read path warm-backs.
	// Tests that assert on those writes take the room mock (newServiceWithRoomMock) and
	// set their own expectations instead of inheriting these.
	rooms.EXPECT().UpdatePreviewBody(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes()
	rooms.EXPECT().ClearPreview(gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes()
	rooms.EXPECT().InvalidatePreviewKey(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	rooms.EXPECT().SetPreviewMessage(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	rooms.EXPECT().
		GetRoomTimes(gomock.Any(), gomock.Any()).
		Return(defaultRoomLastMsgAt, defaultRoomCreatedAt, nil).
		MinTimes(0)
	// Permissive default: existing GetThreadMessages tests don't verify the floor field.
	threadRooms.EXPECT().GetMinThreadUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	return svc, msgs, subs, pub, threadRooms
}

// newServiceWithRoomMock additionally exposes the room mock, pre-stubbed with a permissive
// GetRoomTimes default (override with Times(N) to assert resolver behaviour); no UserStore/AppStore pre-stubs.
func newServiceWithRoomMock(t *testing.T, opts ...service.Option) (*service.HistoryService, *mocks.MockMessageRepository, *mocks.MockSubscriptionRepository, *mocks.MockRoomRepository, *mocks.MockEventPublisher, *mocks.MockThreadRoomRepository, *mocks.MockUserStore, *mocks.MockAppStore) {
	ctrl := gomock.NewController(t)
	msgs := mocks.NewMockMessageRepository(ctrl)
	subs := mocks.NewMockSubscriptionRepository(ctrl)
	rooms := mocks.NewMockRoomRepository(ctrl)
	pub := mocks.NewMockEventPublisher(ctrl)
	threadRooms := mocks.NewMockThreadRoomRepository(ctrl)
	threadSubs := mocks.NewMockThreadSubscriptionRepository(ctrl)
	users := mocks.NewMockUserStore(ctrl)
	apps := mocks.NewMockAppStore(ctrl)
	rooms.EXPECT().
		GetRoomTimes(gomock.Any(), gomock.Any()).
		Return(defaultRoomLastMsgAt, defaultRoomCreatedAt, nil).
		MinTimes(0)
	// Permissive default: only the large-room override path reads userCount.
	rooms.EXPECT().GetRoomUserCount(gomock.Any(), gomock.Any()).Return(0, nil).AnyTimes()
	// No AnyTimes for GetPinnedMessages — it would shadow TestListPinnedMessages_* expectations.
	// Floor=90d never clips fixtures; PinEnabled=true (the kill-switch test builds its own service).
	cfg := &config.Config{
		MessageHistoryFloorDays: 90,
		LargeRoomThreshold:      500,
		MaxPinnedPerRoom:        10,
		PinEnabled:              true,
	}
	return service.New(msgs, subs, rooms, pub, threadRooms, threadSubs, users, apps, cfg, opts...), msgs, subs, rooms, pub, threadRooms, users, apps
}

// assertInternalErr verifies err collapses to the generic "internal error" envelope at the
// boundary; wantCause is matched against the server-side wrapped chain, never the client message.
func assertInternalErr(t *testing.T, err error, wantCause string) {
	t.Helper()
	require.Error(t, err)
	assert.Contains(t, err.Error(), wantCause)
	ec := errcode.Classify(context.Background(), err)
	require.NotNil(t, ec)
	assert.Equal(t, errcode.CodeInternal, ec.Code)
	assert.Equal(t, "internal error", ec.Message)
}

func assertForbiddenErr(t *testing.T, err error, wantMsg string) {
	t.Helper()
	var ec *errcode.Error
	require.ErrorAs(t, err, &ec)
	assert.Equal(t, errcode.CodeForbidden, ec.Code)
	assert.Equal(t, wantMsg, ec.Message)
}

func assertBadRequestErr(t *testing.T, err error, wantMsg string) {
	t.Helper()
	var ec *errcode.Error
	require.ErrorAs(t, err, &ec)
	assert.Equal(t, errcode.CodeBadRequest, ec.Code)
	assert.Equal(t, wantMsg, ec.Message)
}

func assertNotFoundErr(t *testing.T, err error, wantMsg string) {
	t.Helper()
	var ec *errcode.Error
	require.ErrorAs(t, err, &ec)
	assert.Equal(t, errcode.CodeNotFound, ec.Code)
	assert.Equal(t, wantMsg, ec.Message)
}

// expectEmptyPreviewWalk stubs the edit/delete preview walk to find nothing, for tests that
// don't assert on the refreshed preview.
func expectEmptyPreviewWalk(msgs *mocks.MockMessageRepository) {
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage(nil, false), nil).AnyTimes()
}

func makePage(msgs []models.Message, hasNext bool) cassrepo.Page[models.Message] {
	nextCursor := ""
	if hasNext {
		// Base64: the walk decodes this through cassrepo.NewCursor, so an
		// undecodable fixture would exercise the give-up path, not continuation.
		nextCursor = base64.StdEncoding.EncodeToString([]byte("fake-next-cursor"))
	}
	return cassrepo.Page[models.Message]{Data: msgs, NextCursor: nextCursor, HasNext: hasNext}
}

// --- LoadHistory ---

func TestHistoryService_LoadHistory_Success(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)

	messages := make([]models.Message, 4)
	for i := range messages {
		messages[i] = models.Message{
			MessageID: fmt.Sprintf("m%d", i), RoomID: "r1",
			CreatedAt: joinTime.Add(time.Duration(4-i) * time.Minute),
		}
	}
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).Return(makePage(messages, false), nil)

	resp, err := svc.LoadHistory(c, models.LoadHistoryRequest{})
	require.NoError(t, err)
	assert.Len(t, resp.Messages, 4)
	assert.False(t, resp.HasNext)
}

func TestHistoryService_LoadHistory_StoreError(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).Return(cassrepo.Page[models.Message]{}, fmt.Errorf("db down"))

	_, err := svc.LoadHistory(c, models.LoadHistoryRequest{})
	require.Error(t, err)
	assertInternalErr(t, err, "loading history")
}

func TestHistoryService_LoadHistory_SubscriptionError(t *testing.T) {
	svc, _, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, false, fmt.Errorf("db error"))

	_, err := svc.LoadHistory(c, models.LoadHistoryRequest{})
	require.Error(t, err)
	assertInternalErr(t, err, "verifying room access")
}

func TestHistoryService_LoadHistory_EmptyResult(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).Return(makePage(nil, false), nil)

	resp, err := svc.LoadHistory(c, models.LoadHistoryRequest{})
	require.NoError(t, err)
	assert.Empty(t, resp.Messages)
}

func TestHistoryService_LoadHistory_NoHSS(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)

	messages := make([]models.Message, 3)
	for i := range messages {
		messages[i] = models.Message{MessageID: fmt.Sprintf("m%d", i), RoomID: "r1", CreatedAt: time.Now().Add(time.Duration(i) * time.Minute)}
	}
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).Return(makePage(messages, false), nil)

	resp, err := svc.LoadHistory(c, models.LoadHistoryRequest{})
	require.NoError(t, err)
	assert.Len(t, resp.Messages, 3)
	assert.False(t, resp.HasNext)
}

func TestHistoryService_LoadHistory_WithBeforeTimestamp(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	beforeTime := joinTime.Add(5 * time.Minute)
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)

	pageMessages := []models.Message{
		{MessageID: "m3", RoomID: "r1", CreatedAt: joinTime.Add(3 * time.Minute)},
		{MessageID: "m2", RoomID: "r1", CreatedAt: joinTime.Add(2 * time.Minute)},
	}
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, beforeTime, gomock.Any()).Return(makePage(pageMessages, false), nil)

	resp, err := svc.LoadHistory(c, models.LoadHistoryRequest{
		Before: millis(beforeTime),
	})
	require.NoError(t, err)
	assert.Len(t, resp.Messages, 2)
}

func TestHistoryService_LoadHistory_HasNext(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)

	messages := []models.Message{
		{MessageID: "m1", RoomID: "r1", CreatedAt: joinTime.Add(2 * time.Minute)},
		{MessageID: "m0", RoomID: "r1", CreatedAt: joinTime.Add(time.Minute)},
	}
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).Return(makePage(messages, true), nil)

	resp, err := svc.LoadHistory(c, models.LoadHistoryRequest{})
	require.NoError(t, err)
	assert.True(t, resp.HasNext)
}

// An empty-but-resumable page (budget-exhausted walk) must report hasNext=false:
// with no oldest message to derive `before` from, the client could never advance.
func TestHistoryService_LoadHistory_HasNextFalse_EmptyResumablePage(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).Return(makePage(nil, true), nil)

	resp, err := svc.LoadHistory(c, models.LoadHistoryRequest{})
	require.NoError(t, err)
	assert.Empty(t, resp.Messages)
	assert.False(t, resp.HasNext)
}

func TestHistoryService_LoadHistory_ReturnsMinUserLastSeenAt(t *testing.T) {
	svc, msgs, subs, rooms, _, _, _, _ := newServiceWithRoomMock(t)
	c := testContext()

	floor := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).Return(makePage(nil, false), nil)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), "r1").Return(&floor, nil)

	resp, err := svc.LoadHistory(c, models.LoadHistoryRequest{})
	require.NoError(t, err)
	require.NotNil(t, resp.MinUserLastSeenAt)
	assert.Equal(t, floor.UTC().UnixMilli(), *resp.MinUserLastSeenAt)
}

func TestHistoryService_LoadHistory_NoMinUserLastSeenAt(t *testing.T) {
	svc, msgs, subs, rooms, _, _, _, _ := newServiceWithRoomMock(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).Return(makePage(nil, false), nil)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), "r1").Return(nil, nil)

	resp, err := svc.LoadHistory(c, models.LoadHistoryRequest{})
	require.NoError(t, err)
	assert.Nil(t, resp.MinUserLastSeenAt)

	// omitempty must keep the field out of the JSON.
	raw, err := json.Marshal(resp)
	require.NoError(t, err)
	assert.NotContains(t, string(raw), "minUserLastSeenAt")
}

func TestHistoryService_LoadHistory_RoomReadError_DegradesGracefully(t *testing.T) {
	svc, msgs, subs, rooms, _, _, _, _ := newServiceWithRoomMock(t)
	c := testContext()

	pageMessages := []models.Message{
		{MessageID: "m1", RoomID: "r1", CreatedAt: joinTime.Add(time.Minute)},
	}
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).Return(makePage(pageMessages, false), nil)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), "r1").Return(nil, fmt.Errorf("mongo down"))

	resp, err := svc.LoadHistory(c, models.LoadHistoryRequest{})
	require.NoError(t, err)
	assert.Len(t, resp.Messages, 1)
	assert.Nil(t, resp.MinUserLastSeenAt)
}

// TestHistoryService_LoadHistory_AccessErrorTakesPrecedence pins that when the
// access check and the room-times resolve (run concurrently) both fail, the
// access error wins and neither the page read nor the receipt read is reached.
func TestHistoryService_LoadHistory_AccessErrorTakesPrecedence(t *testing.T) {
	ctrl := gomock.NewController(t)
	msgs := mocks.NewMockMessageRepository(ctrl)
	subs := mocks.NewMockSubscriptionRepository(ctrl)
	rooms := mocks.NewMockRoomRepository(ctrl)
	pub := mocks.NewMockEventPublisher(ctrl)
	threadRooms := mocks.NewMockThreadRoomRepository(ctrl)
	threadSubs := mocks.NewMockThreadSubscriptionRepository(ctrl)
	users := mocks.NewMockUserStore(ctrl)
	apps := mocks.NewMockAppStore(ctrl)
	cfg := &config.Config{
		MessageHistoryFloorDays: 90,
		LargeRoomThreshold:      500,
		MaxPinnedPerRoom:        10,
		PinEnabled:              true,
	}
	svc := service.New(msgs, subs, rooms, pub, threadRooms, threadSubs, users, apps, cfg)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, false, errors.New("access db error"))
	// Room-times may run in parallel and also fail; it must not change the result.
	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(time.Time{}, time.Time{}, errors.New("mongo down")).AnyTimes()
	// No page read or receipt read must be reached on access failure.

	_, err := svc.LoadHistory(c, models.LoadHistoryRequest{})
	require.Error(t, err)
	assertInternalErr(t, err, "verifying room access")
}

func TestHistoryService_LoadNextMessages_ReturnsMinUserLastSeenAt(t *testing.T) {
	svc, msgs, subs, rooms, _, _, _, _ := newServiceWithRoomMock(t)
	c := testContext()

	floor := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessagesAfter(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).Return(makePage(nil, false), nil)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), "r1").Return(&floor, nil)

	resp, err := svc.LoadNextMessages(c, models.LoadNextMessagesRequest{})
	require.NoError(t, err)
	require.NotNil(t, resp.MinUserLastSeenAt)
	assert.Equal(t, floor.UTC().UnixMilli(), *resp.MinUserLastSeenAt)
}

func TestHistoryService_LoadNextMessages_NoMinUserLastSeenAt(t *testing.T) {
	svc, msgs, subs, rooms, _, _, _, _ := newServiceWithRoomMock(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessagesAfter(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).Return(makePage(nil, false), nil)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), "r1").Return(nil, nil)

	resp, err := svc.LoadNextMessages(c, models.LoadNextMessagesRequest{})
	require.NoError(t, err)
	assert.Nil(t, resp.MinUserLastSeenAt)

	// omitempty must keep the field out of the JSON.
	raw, err := json.Marshal(resp)
	require.NoError(t, err)
	assert.NotContains(t, string(raw), "minUserLastSeenAt")
}

func TestHistoryService_LoadNextMessages_RoomReadError_DegradesGracefully(t *testing.T) {
	svc, msgs, subs, rooms, _, _, _, _ := newServiceWithRoomMock(t)
	c := testContext()

	pageMessages := []models.Message{{MessageID: "m1", RoomID: "r1", CreatedAt: joinTime.Add(time.Minute)}}
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessagesAfter(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).Return(makePage(pageMessages, false), nil)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), "r1").Return(nil, fmt.Errorf("mongo down"))

	resp, err := svc.LoadNextMessages(c, models.LoadNextMessagesRequest{})
	require.NoError(t, err)
	assert.Len(t, resp.Messages, 1)
	assert.Nil(t, resp.MinUserLastSeenAt)
}

func TestHistoryService_LoadSurroundingMessages_ReturnsMinUserLastSeenAt(t *testing.T) {
	svc, msgs, subs, rooms, _, _, _, _ := newServiceWithRoomMock(t)
	c := testContext()

	floor := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	central := models.Message{MessageID: "mC", RoomID: "r1", CreatedAt: joinTime.Add(2 * time.Minute)}
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessageByID(gomock.Any(), "mC").Return(&central, nil)
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, central.CreatedAt, gomock.Any()).Return(makePage(nil, false), nil)
	msgs.EXPECT().GetMessagesAfter(gomock.Any(), "r1", central.CreatedAt, gomock.Any(), gomock.Any()).Return(makePage(nil, false), nil)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), "r1").Return(&floor, nil)

	resp, err := svc.LoadSurroundingMessages(c, models.LoadSurroundingMessagesRequest{MessageID: "mC", Limit: 10})
	require.NoError(t, err)
	require.NotNil(t, resp.MinUserLastSeenAt)
	assert.Equal(t, floor.UTC().UnixMilli(), *resp.MinUserLastSeenAt)
}

func TestHistoryService_LoadSurroundingMessages_NoMinUserLastSeenAt(t *testing.T) {
	svc, msgs, subs, rooms, _, _, _, _ := newServiceWithRoomMock(t)
	c := testContext()

	central := models.Message{MessageID: "mC", RoomID: "r1", CreatedAt: joinTime.Add(2 * time.Minute)}
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessageByID(gomock.Any(), "mC").Return(&central, nil)
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, central.CreatedAt, gomock.Any()).Return(makePage(nil, false), nil)
	msgs.EXPECT().GetMessagesAfter(gomock.Any(), "r1", central.CreatedAt, gomock.Any(), gomock.Any()).Return(makePage(nil, false), nil)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), "r1").Return(nil, nil)

	resp, err := svc.LoadSurroundingMessages(c, models.LoadSurroundingMessagesRequest{MessageID: "mC", Limit: 10})
	require.NoError(t, err)
	assert.Nil(t, resp.MinUserLastSeenAt)

	// omitempty must keep the field out of the JSON.
	raw, err := json.Marshal(resp)
	require.NoError(t, err)
	assert.NotContains(t, string(raw), "minUserLastSeenAt")
}

func TestHistoryService_LoadSurroundingMessages_RoomReadError_DegradesGracefully(t *testing.T) {
	svc, msgs, subs, rooms, _, _, _, _ := newServiceWithRoomMock(t)
	c := testContext()

	central := models.Message{MessageID: "mC", RoomID: "r1", CreatedAt: joinTime.Add(2 * time.Minute)}
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessageByID(gomock.Any(), "mC").Return(&central, nil)
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, central.CreatedAt, gomock.Any()).Return(makePage(nil, false), nil)
	msgs.EXPECT().GetMessagesAfter(gomock.Any(), "r1", central.CreatedAt, gomock.Any(), gomock.Any()).Return(makePage(nil, false), nil)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), "r1").Return(nil, fmt.Errorf("mongo down"))

	resp, err := svc.LoadSurroundingMessages(c, models.LoadSurroundingMessagesRequest{MessageID: "mC", Limit: 10})
	require.NoError(t, err)
	assert.Len(t, resp.Messages, 1) // central still returned
	assert.Nil(t, resp.MinUserLastSeenAt)
}

func TestHistoryService_LoadSurroundingMessages_Limit1_ReturnsMinUserLastSeenAt(t *testing.T) {
	svc, msgs, subs, rooms, _, _, _, _ := newServiceWithRoomMock(t)
	c := testContext()

	floor := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	central := models.Message{MessageID: "mC", RoomID: "r1", CreatedAt: joinTime.Add(2 * time.Minute)}
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessageByID(gomock.Any(), "mC").Return(&central, nil)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), "r1").Return(&floor, nil)

	resp, err := svc.LoadSurroundingMessages(c, models.LoadSurroundingMessagesRequest{MessageID: "mC", Limit: 1})
	require.NoError(t, err)
	require.Len(t, resp.Messages, 1)
	require.NotNil(t, resp.MinUserLastSeenAt)
	assert.Equal(t, floor.UTC().UnixMilli(), *resp.MinUserLastSeenAt)
}

// --- LoadNextMessages ---

func TestHistoryService_LoadNextMessages_BothAfterAndHSS(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	// after (joinTime+1min) > HSS (joinTime) — effective lower bound = max = after
	afterTime := joinTime.Add(1 * time.Minute)
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)

	messages := []models.Message{
		{MessageID: "m2", RoomID: "r1", CreatedAt: joinTime.Add(2 * time.Minute)},
		{MessageID: "m3", RoomID: "r1", CreatedAt: joinTime.Add(3 * time.Minute)},
	}
	msgs.EXPECT().GetMessagesAfter(gomock.Any(), "r1", afterTime, gomock.Any(), gomock.Any()).Return(makePage(messages, false), nil)

	resp, err := svc.LoadNextMessages(c, models.LoadNextMessagesRequest{
		After: millis(afterTime),
	})
	require.NoError(t, err)
	assert.Len(t, resp.Messages, 2)
	assert.False(t, resp.HasNext)
}

func TestHistoryService_LoadNextMessages_OnlyHSS(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	// No after in request, HSS present — effective lower bound = HSS, uses GetMessagesAfter
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessagesAfter(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).Return(makePage(nil, false), nil)

	_, err := svc.LoadNextMessages(c, models.LoadNextMessagesRequest{})
	require.NoError(t, err)
}

func TestHistoryService_LoadNextMessages_OnlyAfter(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	// after present, HSS not found — effective lower bound = after
	afterTime := joinTime.Add(5 * time.Minute)
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	msgs.EXPECT().GetMessagesAfter(gomock.Any(), "r1", afterTime, gomock.Any(), gomock.Any()).Return(makePage(nil, false), nil)

	_, err := svc.LoadNextMessages(c, models.LoadNextMessagesRequest{
		After: millis(afterTime),
	})
	require.NoError(t, err)
}

func TestHistoryService_LoadNextMessages_BothNil(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	// Neither after nor HSS — no lower bound → GetAllMessagesAsc
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	msgs.EXPECT().GetAllMessagesAsc(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).Return(makePage(nil, false), nil)

	_, err := svc.LoadNextMessages(c, models.LoadNextMessagesRequest{})
	require.NoError(t, err)
}

func TestHistoryService_LoadNextMessages_AfterBeforeHSS_ClampsToHSS(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	// after is before HSS — effective lower bound = HSS (the greater one)
	earlyTime := joinTime.Add(-1 * time.Hour)
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessagesAfter(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).Return(makePage(nil, false), nil)

	_, err := svc.LoadNextMessages(c, models.LoadNextMessagesRequest{
		After: millis(earlyTime),
	})
	require.NoError(t, err)
}

func TestHistoryService_LoadNextMessages_SubscriptionStoreError(t *testing.T) {
	svc, _, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, false, fmt.Errorf("db error"))

	_, err := svc.LoadNextMessages(c, models.LoadNextMessagesRequest{})
	require.Error(t, err)
	assertInternalErr(t, err, "verifying room access")
}

func TestHistoryService_LoadNextMessages_StoreErrorAfter(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	// HSS present → GetMessagesAfter path
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessagesAfter(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).Return(cassrepo.Page[models.Message]{}, fmt.Errorf("db error"))

	_, err := svc.LoadNextMessages(c, models.LoadNextMessagesRequest{})
	require.Error(t, err)
	assertInternalErr(t, err, "loading next messages")
}

func TestHistoryService_LoadNextMessages_StoreErrorLatest(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	// No HSS, no after → GetAllMessagesAsc path
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	msgs.EXPECT().GetAllMessagesAsc(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).Return(cassrepo.Page[models.Message]{}, fmt.Errorf("db error"))

	_, err := svc.LoadNextMessages(c, models.LoadNextMessagesRequest{})
	require.Error(t, err)
	assertInternalErr(t, err, "loading next messages")
}

func TestHistoryService_LoadNextMessages_HasNext(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	messages := []models.Message{
		{MessageID: "m1", RoomID: "r1", CreatedAt: joinTime.Add(1 * time.Minute)},
		{MessageID: "m2", RoomID: "r1", CreatedAt: joinTime.Add(2 * time.Minute)},
	}
	msgs.EXPECT().GetMessagesAfter(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).Return(makePage(messages, true), nil)

	resp, err := svc.LoadNextMessages(c, models.LoadNextMessagesRequest{})
	require.NoError(t, err)
	assert.Len(t, resp.Messages, 2)
	assert.True(t, resp.HasNext)
	assert.NotEmpty(t, resp.NextCursor)
}

func TestHistoryService_LoadNextMessages_DefaultLimit(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	msgs.EXPECT().GetAllMessagesAsc(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Cond(func(x any) bool {
		pr, ok := x.(cassrepo.PageRequest)
		return ok && pr.PageSize == 20
	})).Return(makePage(nil, false), nil)

	resp, err := svc.LoadNextMessages(c, models.LoadNextMessagesRequest{})
	require.NoError(t, err)
	assert.Empty(t, resp.Messages)
}

func TestHistoryService_LoadNextMessages_LimitClampsToMax(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	msgs.EXPECT().GetAllMessagesAsc(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Cond(func(x any) bool {
		pr, ok := x.(cassrepo.PageRequest)
		return ok && pr.PageSize == 100
	})).Return(makePage(nil, false), nil)

	resp, err := svc.LoadNextMessages(c, models.LoadNextMessagesRequest{Limit: 999})
	require.NoError(t, err)
	assert.Empty(t, resp.Messages)
}

// --- GetMessageByID ---

func TestHistoryService_GetMessageByID_Success(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	createdAt := joinTime.Add(1 * time.Minute)
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msg := &models.Message{MessageID: "m1", RoomID: "r1", CreatedAt: createdAt}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(msg, nil)

	result, err := svc.GetMessageByID(c, models.GetMessageByIDRequest{MessageID: "m1"})
	require.NoError(t, err)
	assert.Equal(t, "m1", result.MessageID)
}

func TestHistoryService_GetMessageByID_OutsideAccessWindow(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	createdAt := joinTime.Add(-1 * time.Hour)
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msg := &models.Message{MessageID: "m1", RoomID: "r1", CreatedAt: createdAt}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(msg, nil)

	_, err := svc.GetMessageByID(c, models.GetMessageByIDRequest{MessageID: "m1"})
	require.Error(t, err)
}

func TestHistoryService_GetMessageByID_NotFound(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(nil, nil)

	_, err := svc.GetMessageByID(c, models.GetMessageByIDRequest{MessageID: "m1"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestHistoryService_GetMessageByID_WrongRoom(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	createdAt := joinTime.Add(1 * time.Minute)
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	// Message exists but belongs to a different room.
	msg := &models.Message{MessageID: "m1", RoomID: "r-other", CreatedAt: createdAt}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(msg, nil)

	_, err := svc.GetMessageByID(c, models.GetMessageByIDRequest{MessageID: "m1"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestHistoryService_GetMessageByID_StoreError(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(nil, fmt.Errorf("db error"))

	_, err := svc.GetMessageByID(c, models.GetMessageByIDRequest{MessageID: "m1"})
	require.Error(t, err)
	assertInternalErr(t, err, "retrieving message")
}

func TestHistoryService_GetMessageByID_NoHSS(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	createdAt := joinTime.Add(-1 * time.Hour)
	// nil HSS means no restriction — any message is accessible
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	msg := &models.Message{MessageID: "m1", RoomID: "r1", CreatedAt: createdAt}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(msg, nil)

	result, err := svc.GetMessageByID(c, models.GetMessageByIDRequest{MessageID: "m1"})
	require.NoError(t, err)
	assert.Equal(t, "m1", result.MessageID)
}

func TestHistoryService_GetMessageByID_DecodesAttachments(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	blob, err := json.Marshal(model.Attachment{ID: "f1", Title: "a.png", Type: "file"})
	require.NoError(t, err)
	createdAt := joinTime.Add(1 * time.Minute)
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msg := &models.Message{MessageID: "m1", RoomID: "r1", CreatedAt: createdAt, Attachments: [][]byte{blob}}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(msg, nil)

	result, err := svc.GetMessageByID(c, models.GetMessageByIDRequest{MessageID: "m1"})
	require.NoError(t, err)
	require.Len(t, result.DecodedAttachments, 1)
	assert.Equal(t, "f1", result.DecodedAttachments[0].ID)
}

func TestHistoryService_LoadNextMessages_HasNextFalse(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessagesAfter(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).Return(makePage(nil, false), nil)

	resp, err := svc.LoadNextMessages(c, models.LoadNextMessagesRequest{})
	require.NoError(t, err)
	assert.False(t, resp.HasNext)
	assert.Empty(t, resp.NextCursor)
}

// --- LoadSurroundingMessages ---

func TestHistoryService_LoadSurroundingMessages_Success(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)

	centralMsg := &models.Message{MessageID: "m5", RoomID: "r1", CreatedAt: joinTime.Add(5 * time.Minute)}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m5").Return(centralMsg, nil)

	beforeMsgs := []models.Message{{MessageID: "m4", RoomID: "r1", CreatedAt: joinTime.Add(4 * time.Minute)}}
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, centralMsg.CreatedAt, gomock.Any()).Return(makePage(beforeMsgs, false), nil)

	afterMsgs := []models.Message{{MessageID: "m6", RoomID: "r1", CreatedAt: joinTime.Add(6 * time.Minute)}}
	msgs.EXPECT().GetMessagesAfter(gomock.Any(), "r1", centralMsg.CreatedAt, gomock.Any(), gomock.Any()).Return(makePage(afterMsgs, false), nil)

	resp, err := svc.LoadSurroundingMessages(c, models.LoadSurroundingMessagesRequest{
		MessageID: "m5", Limit: 6,
	})
	require.NoError(t, err)
	// before (reversed) + central + after = [m4, m5, m6]
	assert.Len(t, resp.Messages, 3)
	assert.Equal(t, "m4", resp.Messages[0].MessageID)
	assert.Equal(t, "m5", resp.Messages[1].MessageID)
	assert.Equal(t, "m6", resp.Messages[2].MessageID)
	assert.False(t, resp.MoreBefore)
	assert.False(t, resp.MoreAfter)
}

func TestHistoryService_LoadSurroundingMessages_MoreBeforeAndAfter(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)

	centralMsg := &models.Message{MessageID: "m5", RoomID: "r1", CreatedAt: joinTime.Add(5 * time.Minute)}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m5").Return(centralMsg, nil)

	beforeMsgs := []models.Message{{MessageID: "m4", RoomID: "r1", CreatedAt: joinTime.Add(4 * time.Minute)}}
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, centralMsg.CreatedAt, gomock.Any()).Return(makePage(beforeMsgs, true), nil)

	afterMsgs := []models.Message{{MessageID: "m6", RoomID: "r1", CreatedAt: joinTime.Add(6 * time.Minute)}}
	msgs.EXPECT().GetMessagesAfter(gomock.Any(), "r1", centralMsg.CreatedAt, gomock.Any(), gomock.Any()).Return(makePage(afterMsgs, true), nil)

	resp, err := svc.LoadSurroundingMessages(c, models.LoadSurroundingMessagesRequest{
		MessageID: "m5", Limit: 4,
	})
	require.NoError(t, err)
	assert.True(t, resp.MoreBefore)
	assert.True(t, resp.MoreAfter)
}

func TestHistoryService_LoadSurroundingMessages_HSSBeforeMessage(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	// accessSince set and before central message — before-page uses GetMessagesBetweenDesc,
	// after-page uses GetMessagesAfter (no access constraint needed for newer messages)
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)

	centralMsg := &models.Message{MessageID: "m5", RoomID: "r1", CreatedAt: joinTime.Add(5 * time.Minute)}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m5").Return(centralMsg, nil)

	beforeMsgs := []models.Message{{MessageID: "m4", RoomID: "r1", CreatedAt: joinTime.Add(4 * time.Minute)}}
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, centralMsg.CreatedAt, gomock.Any()).Return(makePage(beforeMsgs, false), nil)

	afterMsgs := []models.Message{{MessageID: "m6", RoomID: "r1", CreatedAt: joinTime.Add(6 * time.Minute)}}
	msgs.EXPECT().GetMessagesAfter(gomock.Any(), "r1", centralMsg.CreatedAt, gomock.Any(), gomock.Any()).Return(makePage(afterMsgs, false), nil)

	resp, err := svc.LoadSurroundingMessages(c, models.LoadSurroundingMessagesRequest{
		MessageID: "m5", Limit: 6,
	})
	require.NoError(t, err)
	assert.Len(t, resp.Messages, 3)
	assert.Equal(t, "m4", resp.Messages[0].MessageID)
	assert.Equal(t, "m5", resp.Messages[1].MessageID)
	assert.Equal(t, "m6", resp.Messages[2].MessageID)
}

func TestHistoryService_LoadSurroundingMessages_NoHSS(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	// nil accessSince — no lower bound restriction, full history access
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)

	centralMsg := &models.Message{MessageID: "m5", RoomID: "r1", CreatedAt: joinTime.Add(5 * time.Minute)}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m5").Return(centralMsg, nil)

	beforeMsgs := []models.Message{{MessageID: "m4", RoomID: "r1", CreatedAt: joinTime.Add(4 * time.Minute)}}
	// since is zero — no lower bound, uses GetMessagesBefore (upper bound only)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", centralMsg.CreatedAt, gomock.Any(), gomock.Any()).Return(makePage(beforeMsgs, false), nil)

	afterMsgs := []models.Message{{MessageID: "m6", RoomID: "r1", CreatedAt: joinTime.Add(6 * time.Minute)}}
	msgs.EXPECT().GetMessagesAfter(gomock.Any(), "r1", centralMsg.CreatedAt, gomock.Any(), gomock.Any()).Return(makePage(afterMsgs, false), nil)

	resp, err := svc.LoadSurroundingMessages(c, models.LoadSurroundingMessagesRequest{
		MessageID: "m5", Limit: 6,
	})
	require.NoError(t, err)
	assert.Len(t, resp.Messages, 3)
}

func TestHistoryService_LoadSurroundingMessages_SubscriptionError(t *testing.T) {
	svc, _, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, false, fmt.Errorf("db error"))

	_, err := svc.LoadSurroundingMessages(c, models.LoadSurroundingMessagesRequest{
		MessageID: "m5", Limit: 6,
	})
	require.Error(t, err)
	assertInternalErr(t, err, "verifying room access")
}

func TestHistoryService_LoadSurroundingMessages_WrongRoom(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)

	// Central message exists but belongs to a different room.
	wrongRoomMsg := &models.Message{MessageID: "m5", RoomID: "r-other", CreatedAt: joinTime.Add(5 * time.Minute)}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m5").Return(wrongRoomMsg, nil)

	_, err := svc.LoadSurroundingMessages(c, models.LoadSurroundingMessagesRequest{
		MessageID: "m5", Limit: 6,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestHistoryService_LoadSurroundingMessages_CentralMessageOutsideWindow(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)

	oldMsg := &models.Message{MessageID: "m_old", RoomID: "r1", CreatedAt: joinTime.Add(-1 * time.Hour)}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m_old").Return(oldMsg, nil)

	_, err := svc.LoadSurroundingMessages(c, models.LoadSurroundingMessagesRequest{
		MessageID: "m_old", Limit: 6,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "outside access window")
}

func TestHistoryService_LoadSurroundingMessages_MessageNotFound(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessageByID(gomock.Any(), "nonexistent").Return(nil, nil)

	_, err := svc.LoadSurroundingMessages(c, models.LoadSurroundingMessagesRequest{
		MessageID: "nonexistent", Limit: 6,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestHistoryService_LoadSurroundingMessages_StoreError(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m5").Return(nil, fmt.Errorf("db error"))

	_, err := svc.LoadSurroundingMessages(c, models.LoadSurroundingMessagesRequest{
		MessageID: "m5", Limit: 6,
	})
	require.Error(t, err)
	assertInternalErr(t, err, "retrieving message")
}

func TestHistoryService_LoadSurroundingMessages_BeforePageError(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	centralMsg := &models.Message{MessageID: "m5", RoomID: "r1", CreatedAt: joinTime.Add(5 * time.Minute)}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m5").Return(centralMsg, nil)
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, centralMsg.CreatedAt, gomock.Any()).Return(cassrepo.Page[models.Message]{}, fmt.Errorf("db error"))
	// before- and after-walks run in parallel, so the after-walk may also be invoked.
	msgs.EXPECT().GetMessagesAfter(gomock.Any(), "r1", centralMsg.CreatedAt, gomock.Any(), gomock.Any()).Return(makePage(nil, false), nil).MaxTimes(1)

	_, err := svc.LoadSurroundingMessages(c, models.LoadSurroundingMessagesRequest{
		MessageID: "m5", Limit: 6,
	})
	require.Error(t, err)
	assertInternalErr(t, err, "loading surrounding messages")
}

func TestHistoryService_LoadSurroundingMessages_BeforePageError_NoHSS(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	centralMsg := &models.Message{MessageID: "m5", RoomID: "r1", CreatedAt: joinTime.Add(5 * time.Minute)}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m5").Return(centralMsg, nil)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", centralMsg.CreatedAt, gomock.Any(), gomock.Any()).Return(cassrepo.Page[models.Message]{}, fmt.Errorf("db error"))
	// before- and after-walks run in parallel, so the after-walk may also be invoked.
	msgs.EXPECT().GetMessagesAfter(gomock.Any(), "r1", centralMsg.CreatedAt, gomock.Any(), gomock.Any()).Return(makePage(nil, false), nil).MaxTimes(1)

	_, err := svc.LoadSurroundingMessages(c, models.LoadSurroundingMessagesRequest{
		MessageID: "m5", Limit: 6,
	})
	require.Error(t, err)
	assertInternalErr(t, err, "loading surrounding messages")
}

func TestHistoryService_LoadSurroundingMessages_AfterPageError(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	centralMsg := &models.Message{MessageID: "m5", RoomID: "r1", CreatedAt: joinTime.Add(5 * time.Minute)}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m5").Return(centralMsg, nil)
	beforeMsgs := []models.Message{{MessageID: "m4", RoomID: "r1", CreatedAt: joinTime.Add(4 * time.Minute)}}
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, centralMsg.CreatedAt, gomock.Any()).Return(makePage(beforeMsgs, false), nil)
	msgs.EXPECT().GetMessagesAfter(gomock.Any(), "r1", centralMsg.CreatedAt, gomock.Any(), gomock.Any()).Return(cassrepo.Page[models.Message]{}, fmt.Errorf("db error"))

	_, err := svc.LoadSurroundingMessages(c, models.LoadSurroundingMessagesRequest{
		MessageID: "m5", Limit: 6,
	})
	require.Error(t, err)
	assertInternalErr(t, err, "loading surrounding messages")
}

func TestHistoryService_LoadSurroundingMessages_Limit1_OnlyCentral(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	centralMsg := &models.Message{MessageID: "m5", RoomID: "r1", CreatedAt: joinTime.Add(5 * time.Minute)}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m5").Return(centralMsg, nil)
	// No before/after queries expected — half = 1/2 = 0

	resp, err := svc.LoadSurroundingMessages(c, models.LoadSurroundingMessagesRequest{
		MessageID: "m5", Limit: 1,
	})
	require.NoError(t, err)
	assert.Len(t, resp.Messages, 1)
	assert.Equal(t, "m5", resp.Messages[0].MessageID)
	assert.False(t, resp.MoreBefore)
	assert.False(t, resp.MoreAfter)
}

func TestHistoryService_LoadSurroundingMessages_Limit1_RedactsInaccessibleQuote(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	centralMsg := &models.Message{
		MessageID: "m5", RoomID: "r1", CreatedAt: joinTime.Add(5 * time.Minute),
		QuotedParentMessage: &models.QuotedParentMessage{
			MessageID: "old-msg", Msg: "secret", CreatedAt: joinTime.Add(-time.Hour),
		},
	}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m5").Return(centralMsg, nil)

	resp, err := svc.LoadSurroundingMessages(c, models.LoadSurroundingMessagesRequest{
		MessageID: "m5", Limit: 1,
	})
	require.NoError(t, err)
	require.Len(t, resp.Messages, 1)
	q := resp.Messages[0].QuotedParentMessage
	require.NotNil(t, q)
	assert.Equal(t, service.UnavailableQuoteMsg, q.Msg)
	assert.Empty(t, q.MessageID)
}

// --- LoadSurroundingMessages: timestamp pivot ---

// tsPivot reconstructs a pivot the way the handler does (time.UnixMilli(ms).UTC())
// and returns the +1ms inclusive upper bound the before-read is called with, so
// the expected mock args are DeepEqual to the values the handler passes.
func tsPivot(base time.Time) (pivot, beforeUpper time.Time, ts *int64) {
	ms := base.UnixMilli()
	pivot = time.UnixMilli(ms).UTC()
	return pivot, pivot.Add(time.Millisecond), &ms
}

func TestHistoryService_LoadSurroundingMessages_ByTimestamp_NoHSS(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)

	pivot, beforeUpper, ts := tsPivot(joinTime.Add(5 * time.Minute))
	// No HSS → strict before-read with pivot+1ms (== created_at <= pivot) anchors an at-pivot message.
	beforeMsgs := []models.Message{{MessageID: "m5", RoomID: "r1", CreatedAt: pivot}, {MessageID: "m4", RoomID: "r1", CreatedAt: joinTime.Add(4 * time.Minute)}}
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", beforeUpper, gomock.Any(), gomock.Any()).Return(makePage(beforeMsgs, false), nil)

	afterMsgs := []models.Message{{MessageID: "m6", RoomID: "r1", CreatedAt: joinTime.Add(6 * time.Minute)}}
	msgs.EXPECT().GetMessagesAfter(gomock.Any(), "r1", pivot, gomock.Any(), gomock.Any()).Return(makePage(afterMsgs, false), nil)

	resp, err := svc.LoadSurroundingMessages(c, models.LoadSurroundingMessagesRequest{Timestamp: ts, Limit: 6})
	require.NoError(t, err)
	// Reversed before [m4, m5] + after [m6]; no central splice.
	require.Len(t, resp.Messages, 3)
	assert.Equal(t, "m4", resp.Messages[0].MessageID)
	assert.Equal(t, "m5", resp.Messages[1].MessageID)
	assert.Equal(t, "m6", resp.Messages[2].MessageID)
	assert.False(t, resp.MoreBefore)
	assert.False(t, resp.MoreAfter)
}

func TestHistoryService_LoadSurroundingMessages_ByTimestamp_WithHSS(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)

	pivot, beforeUpper, ts := tsPivot(joinTime.Add(5 * time.Minute))
	// HSS set and pivot within window → strict between-read, lower bound = accessSince,
	// upper bound = pivot+1ms (== created_at <= pivot).
	beforeMsgs := []models.Message{{MessageID: "m4", RoomID: "r1", CreatedAt: joinTime.Add(4 * time.Minute)}}
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, beforeUpper, gomock.Any()).Return(makePage(beforeMsgs, false), nil)

	afterMsgs := []models.Message{{MessageID: "m6", RoomID: "r1", CreatedAt: joinTime.Add(6 * time.Minute)}}
	msgs.EXPECT().GetMessagesAfter(gomock.Any(), "r1", pivot, gomock.Any(), gomock.Any()).Return(makePage(afterMsgs, false), nil)

	resp, err := svc.LoadSurroundingMessages(c, models.LoadSurroundingMessagesRequest{Timestamp: ts, Limit: 6})
	require.NoError(t, err)
	require.Len(t, resp.Messages, 2)
	assert.Equal(t, "m4", resp.Messages[0].MessageID)
	assert.Equal(t, "m6", resp.Messages[1].MessageID)
}

func TestHistoryService_LoadSurroundingMessages_ByTimestamp_MoreBeforeAndAfter(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)

	pivot, beforeUpper, ts := tsPivot(joinTime.Add(5 * time.Minute))
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", beforeUpper, gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{{MessageID: "m4", RoomID: "r1", CreatedAt: joinTime.Add(4 * time.Minute)}}, true), nil)
	msgs.EXPECT().GetMessagesAfter(gomock.Any(), "r1", pivot, gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{{MessageID: "m6", RoomID: "r1", CreatedAt: joinTime.Add(6 * time.Minute)}}, true), nil)

	resp, err := svc.LoadSurroundingMessages(c, models.LoadSurroundingMessagesRequest{Timestamp: ts, Limit: 4})
	require.NoError(t, err)
	assert.True(t, resp.MoreBefore)
	assert.True(t, resp.MoreAfter)
}

func TestHistoryService_LoadSurroundingMessages_ByTimestamp_OutsideWindow(t *testing.T) {
	svc, _, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)

	// Pivot strictly before the access floor → forbidden, no reads issued.
	_, _, ts := tsPivot(joinTime.Add(-1 * time.Hour))
	_, err := svc.LoadSurroundingMessages(c, models.LoadSurroundingMessagesRequest{Timestamp: ts, Limit: 6})
	require.Error(t, err)
	assertForbiddenErr(t, err, "timestamp is outside access window")
	assert.True(t, errcode.HasReason(err, errcode.MessageOutsideAccessWindow))
}

func TestHistoryService_LoadSurroundingMessages_ByTimestamp_Limit1_NoAfterRead(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)

	pivot, beforeUpper, ts := tsPivot(joinTime.Add(5 * time.Minute))
	// limit=1 → beforeCount=1, afterCount=0: only the before-read runs.
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", beforeUpper, gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{{MessageID: "m5", RoomID: "r1", CreatedAt: pivot}}, false), nil)
	// No GetMessagesAfter expectation — the after-read must be skipped.

	resp, err := svc.LoadSurroundingMessages(c, models.LoadSurroundingMessagesRequest{Timestamp: ts, Limit: 1})
	require.NoError(t, err)
	require.Len(t, resp.Messages, 1)
	assert.Equal(t, "m5", resp.Messages[0].MessageID)
	assert.False(t, resp.MoreAfter)
}

func TestHistoryService_LoadSurroundingMessages_ByTimestamp_BeforeError(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	pivot, beforeUpper, ts := tsPivot(joinTime.Add(5 * time.Minute))
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", beforeUpper, gomock.Any(), gomock.Any()).
		Return(cassrepo.Page[models.Message]{}, fmt.Errorf("db error"))
	// after-read runs in parallel; it may or may not be reached.
	msgs.EXPECT().GetMessagesAfter(gomock.Any(), "r1", pivot, gomock.Any(), gomock.Any()).
		Return(makePage(nil, false), nil).MaxTimes(1)

	_, err := svc.LoadSurroundingMessages(c, models.LoadSurroundingMessagesRequest{Timestamp: ts, Limit: 6})
	require.Error(t, err)
	assertInternalErr(t, err, "loading surrounding messages")
}

func TestHistoryService_LoadSurroundingMessages_BothPivots(t *testing.T) {
	svc, _, _, _, _ := newService(t)
	c := testContext()

	_, _, ts := tsPivot(joinTime.Add(5 * time.Minute))
	_, err := svc.LoadSurroundingMessages(c, models.LoadSurroundingMessagesRequest{MessageID: "m5", Timestamp: ts, Limit: 6})
	require.Error(t, err)
	assertBadRequestErr(t, err, "provide either messageId or timestamp, not both")
}

func TestHistoryService_LoadSurroundingMessages_ByTimestamp_NonPositive(t *testing.T) {
	svc, _, _, _, _ := newService(t)
	c := testContext()

	zero := int64(0)
	_, err := svc.LoadSurroundingMessages(c, models.LoadSurroundingMessagesRequest{Timestamp: &zero, Limit: 6})
	require.Error(t, err)
	assertBadRequestErr(t, err, "timestamp must be positive")
}

func TestHistoryService_LoadSurroundingMessages_ByTimestamp_NotSubscribed(t *testing.T) {
	svc, _, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, false, nil)

	_, _, ts := tsPivot(joinTime.Add(5 * time.Minute))
	_, err := svc.LoadSurroundingMessages(c, models.LoadSurroundingMessagesRequest{Timestamp: ts, Limit: 6})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not subscribed to room")
}

// --- Access Control: Not Subscribed ---

func TestHistoryService_LoadHistory_NotSubscribed(t *testing.T) {
	svc, _, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, false, nil)

	_, err := svc.LoadHistory(c, models.LoadHistoryRequest{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not subscribed to room")
}

func TestHistoryService_LoadNextMessages_NotSubscribed(t *testing.T) {
	svc, _, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, false, nil)

	_, err := svc.LoadNextMessages(c, models.LoadNextMessagesRequest{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not subscribed to room")
}

func TestHistoryService_LoadSurroundingMessages_NotSubscribed(t *testing.T) {
	svc, _, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, false, nil)

	_, err := svc.LoadSurroundingMessages(c, models.LoadSurroundingMessagesRequest{
		MessageID: "m5", Limit: 6,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not subscribed to room")
}

func TestHistoryService_GetMessageByID_MissingMessageID(t *testing.T) {
	svc, _, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)

	_, err := svc.GetMessageByID(c, models.GetMessageByIDRequest{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "messageId is required")
}

func TestHistoryService_LoadSurroundingMessages_MissingMessageID(t *testing.T) {
	svc, _, _, _, _ := newService(t)
	c := testContext()

	// Exactly-one-of validation precedes the access check — no subscription read.
	_, err := svc.LoadSurroundingMessages(c, models.LoadSurroundingMessagesRequest{Limit: 6})
	require.Error(t, err)
	assertBadRequestErr(t, err, "messageId or timestamp is required")
}

func TestHistoryService_GetMessageByID_NotSubscribed(t *testing.T) {
	svc, _, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, false, nil)

	_, err := svc.GetMessageByID(c, models.GetMessageByIDRequest{MessageID: "m1"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not subscribed to room")
}

// --- EditMessage ---

func TestHistoryService_EditMessage_NotSubscribed(t *testing.T) {
	svc, _, subs, _, _ := newService(t)
	c := testContext()

	// Not subscribed — the helper returns ErrForbidden before we touch anything else.
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, false, nil)

	resp, err := svc.EditMessage(c, "site-test", models.EditMessageRequest{MessageID: "m-abc", NewMsg: "x"})
	assert.Nil(t, resp)

	var routeErr *errcode.Error
	require.ErrorAs(t, err, &routeErr)
	assert.Equal(t, errcode.CodeForbidden, routeErr.Code)
	assert.Equal(t, "not subscribed to room", routeErr.Message)
}

func TestHistoryService_EditMessage_NotSender(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)

	// Message exists in the expected room but a different account is the sender.
	hydrated := &models.Message{
		MessageID: "m-abc",
		RoomID:    "r1",
		Sender:    models.Participant{Account: "someone-else"},
	}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m-abc").Return(hydrated, nil)

	resp, err := svc.EditMessage(c, "site-test", models.EditMessageRequest{MessageID: "m-abc", NewMsg: "x"})
	assert.Nil(t, resp)

	var routeErr *errcode.Error
	require.ErrorAs(t, err, &routeErr)
	assert.Equal(t, errcode.CodeForbidden, routeErr.Code)
	assert.Equal(t, "only the sender can edit", routeErr.Message)
}

func TestHistoryService_EditMessage_NotFound(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	msgs.EXPECT().GetMessageByID(gomock.Any(), "missing").Return(nil, nil)

	resp, err := svc.EditMessage(c, "site-test", models.EditMessageRequest{MessageID: "missing", NewMsg: "x"})
	assert.Nil(t, resp)

	var routeErr *errcode.Error
	require.ErrorAs(t, err, &routeErr)
	assert.Equal(t, errcode.CodeNotFound, routeErr.Code)
}

func TestHistoryService_EditMessage_WrongRoom(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)

	// Message exists but in a different room — findMessage returns ErrNotFound (no leak).
	hydrated := &models.Message{
		MessageID: "m-abc",
		RoomID:    "other-room",
		Sender:    models.Participant{Account: "u1"},
	}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m-abc").Return(hydrated, nil)

	resp, err := svc.EditMessage(c, "site-test", models.EditMessageRequest{MessageID: "m-abc", NewMsg: "x"})
	assert.Nil(t, resp)

	var routeErr *errcode.Error
	require.ErrorAs(t, err, &routeErr)
	assert.Equal(t, errcode.CodeNotFound, routeErr.Code)
}

func TestHistoryService_EditMessage_AlreadyDeleted(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)

	// A soft-deleted message must be invisible to edits; NotFound (not Forbidden) keeps the
	// leak surface symmetric with WrongRoom and prevents a delete -> edit event sequence.
	hydrated := &models.Message{
		MessageID: "m-abc",
		RoomID:    "r1",
		Sender:    models.Participant{Account: "u1"},
		Deleted:   true,
	}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m-abc").Return(hydrated, nil)

	resp, err := svc.EditMessage(c, "site-test", models.EditMessageRequest{MessageID: "m-abc", NewMsg: "x"})
	assert.Nil(t, resp)

	var routeErr *errcode.Error
	require.ErrorAs(t, err, &routeErr)
	assert.Equal(t, errcode.CodeNotFound, routeErr.Code)
}

func TestHistoryService_EditMessage_EmptyNewMsg(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)

	hydrated := &models.Message{
		MessageID: "m-abc",
		RoomID:    "r1",
		Sender:    models.Participant{Account: "u1"},
	}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m-abc").Return(hydrated, nil)

	resp, err := svc.EditMessage(c, "site-test", models.EditMessageRequest{MessageID: "m-abc", NewMsg: "   "})
	assert.Nil(t, resp)

	var routeErr *errcode.Error
	require.ErrorAs(t, err, &routeErr)
	assert.Equal(t, errcode.CodeBadRequest, routeErr.Code)
	assert.Equal(t, "newMsg must not be empty", routeErr.Message)
}

func TestHistoryService_EditMessage_TooLarge(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)

	hydrated := &models.Message{
		MessageID: "m-abc",
		RoomID:    "r1",
		Sender:    models.Participant{Account: "u1"},
	}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m-abc").Return(hydrated, nil)

	// 20 KB + 1 byte
	oversize := strings.Repeat("a", 20*1024+1)

	resp, err := svc.EditMessage(c, "site-test", models.EditMessageRequest{MessageID: "m-abc", NewMsg: oversize})
	assert.Nil(t, resp)

	var routeErr *errcode.Error
	require.ErrorAs(t, err, &routeErr)
	assert.Equal(t, errcode.CodeBadRequest, routeErr.Code)
	assert.Equal(t, "newMsg exceeds maximum size", routeErr.Message)
}

func TestHistoryService_EditMessage_UpdateFails(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)

	hydrated := &models.Message{
		MessageID: "m-abc",
		RoomID:    "r1",
		Sender:    models.Participant{Account: "u1"},
	}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m-abc").Return(hydrated, nil)
	msgs.EXPECT().
		UpdateMessageContent(gomock.Any(), hydrated, "new content", gomock.Any()).
		Return(fmt.Errorf("cassandra timeout"))

	// The publisher mock expects no calls — gomock fails the test if the failed UPDATE still publishes.

	resp, err := svc.EditMessage(c, "site-test", models.EditMessageRequest{MessageID: "m-abc", NewMsg: "new content"})
	assert.Nil(t, resp)
	assertInternalErr(t, err, "editing message")
}

// A concurrent delete landing between findMessage and the CAS edit makes the repo return
// ErrMessageNotFound; it must map to NotFound, not 5xx — benign race, not a server fault.
func TestHistoryService_EditMessage_RaceWithDelete_MapsToNotFound(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)

	hydrated := &models.Message{
		MessageID: "m-race",
		RoomID:    "r1",
		Sender:    models.Participant{Account: "u1"},
	}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m-race").Return(hydrated, nil)
	msgs.EXPECT().
		UpdateMessageContent(gomock.Any(), hydrated, "new content", gomock.Any()).
		Return(fmt.Errorf("edit message m-race: %w", cassrepo.ErrMessageNotFound))

	resp, err := svc.EditMessage(c, "site-test", models.EditMessageRequest{MessageID: "m-race", NewMsg: "new content"})
	assert.Nil(t, resp)
	assertNotFoundErr(t, err, "message not found")
}

func TestHistoryService_EditMessage_PublishesCanonicalUpdatedEvent(t *testing.T) {
	svc, msgs, subs, pub, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	hydrated := &models.Message{
		MessageID: "msg-1",
		RoomID:    "r1",
		Sender:    models.Participant{Account: "u1", ID: "u1-id"},
		CreatedAt: time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC),
		Msg:       "original content",
	}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "msg-1").Return(hydrated, nil)
	msgs.EXPECT().UpdateMessageContent(gomock.Any(), hydrated, "updated content", gomock.Any()).Return(nil)

	pub.EXPECT().
		Publish(gomock.Any(), subject.MsgCanonicalUpdated("site-test"), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, data []byte, _ string) error {
			var evt model.MessageEvent
			require.NoError(t, json.Unmarshal(data, &evt))
			assert.Equal(t, model.EventUpdated, evt.Event)
			assert.Equal(t, "msg-1", evt.Message.ID)
			assert.Equal(t, "r1", evt.Message.RoomID)
			assert.Equal(t, "updated content", evt.Message.Content)
			require.NotNil(t, evt.Message.EditedAt)
			require.NotNil(t, evt.Message.UpdatedAt)
			assert.Equal(t, "site-test", evt.SiteID)
			assert.NotZero(t, evt.Timestamp)
			return nil
		})

	expectEmptyPreviewWalk(msgs)

	resp, err := svc.EditMessage(c, "site-test", models.EditMessageRequest{
		MessageID: "msg-1",
		NewMsg:    "updated content",
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
}

// .updated is a full-doc replace: it must carry attachments and card,
// or the re-index wipes those fields from the search document.
func TestHistoryService_EditMessage_CarriesAttachmentsAndCard(t *testing.T) {
	svc, msgs, subs, pub, _ := newService(t)
	c := testContext()

	attachments := [][]byte{[]byte(`{"title":"q3.pdf","description":"numbers","fileType":"application/pdf"}`)}
	card := &models.Card{Template: "expense-v1", Data: []byte(`{"text":"hi"}`)}
	cardAction := &models.CardAction{Verb: "approve", DisplayText: "Approved by Ann"}

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	hydrated := &models.Message{
		MessageID:   "msg-1",
		RoomID:      "r1",
		Sender:      models.Participant{Account: "u1", ID: "u1-id"},
		CreatedAt:   time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC),
		Msg:         "original content",
		Attachments: attachments,
		Card:        card,
		CardAction:  cardAction,
	}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "msg-1").Return(hydrated, nil)
	msgs.EXPECT().UpdateMessageContent(gomock.Any(), hydrated, "updated content", gomock.Any()).Return(nil)

	pub.EXPECT().
		Publish(gomock.Any(), subject.MsgCanonicalUpdated("site-test"), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, data []byte, _ string) error {
			var evt model.MessageEvent
			require.NoError(t, json.Unmarshal(data, &evt))
			assert.Equal(t, attachments, evt.Message.Attachments)
			require.NotNil(t, evt.Message.Card)
			assert.Equal(t, card.Template, evt.Message.Card.Template)
			assert.Equal(t, card.Data, evt.Message.Card.Data)
			// CardAction stays off the wire: no .updated consumer reads it,
			// and its Data blob would inflate every edit event.
			assert.Nil(t, evt.Message.CardAction)
			return nil
		})

	expectEmptyPreviewWalk(msgs)

	_, err := svc.EditMessage(c, "site-test", models.EditMessageRequest{
		MessageID: "msg-1",
		NewMsg:    "updated content",
	})
	require.NoError(t, err)
}

// Canonical publish is best-effort — a failure must not roll back the edit (Cassandra is the source of truth).
func TestHistoryService_EditMessage_PublishFailureDoesNotFailRPC(t *testing.T) {
	svc, msgs, subs, pub, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	hydrated := &models.Message{
		MessageID: "msg-1",
		RoomID:    "r1",
		Sender:    models.Participant{Account: "u1", ID: "u1-id"},
		Msg:       "original content",
	}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "msg-1").Return(hydrated, nil)
	msgs.EXPECT().UpdateMessageContent(gomock.Any(), hydrated, "updated content", gomock.Any()).Return(nil)

	pub.EXPECT().
		Publish(gomock.Any(), subject.MsgCanonicalUpdated("site-test"), gomock.Any(), gomock.Any()).
		Return(errors.New("nats down"))

	expectEmptyPreviewWalk(msgs)

	resp, err := svc.EditMessage(c, "site-test", models.EditMessageRequest{
		MessageID: "msg-1",
		NewMsg:    "updated content",
	})
	require.NoError(t, err, "publish failure must not fail the RPC")
	require.NotNil(t, resp)
}

// Nats-Msg-Id "{messageID}:updated:{editedAtMs}": the op suffix avoids gatekeeper's
// bare-ID .created key; editedAtMs keys each distinct edit.
func TestHistoryService_EditMessage_PassesDedupMessageID(t *testing.T) {
	svc, msgs, subs, pub, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	hydrated := &models.Message{
		MessageID: "msg-1",
		RoomID:    "r1",
		Sender:    models.Participant{Account: "u1", ID: "u1-id"},
		CreatedAt: time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC),
		Msg:       "original",
	}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "msg-1").Return(hydrated, nil)
	msgs.EXPECT().UpdateMessageContent(gomock.Any(), hydrated, "updated", gomock.Any()).Return(nil)

	pub.EXPECT().
		Publish(gomock.Any(), subject.MsgCanonicalUpdated("site-test"), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, data []byte, msgID string) error {
			var evt model.MessageEvent
			require.NoError(t, json.Unmarshal(data, &evt))
			assert.Equal(t, natsutil.CanonicalDedupID(&evt), msgID)
			return nil
		})

	expectEmptyPreviewWalk(msgs)

	_, err := svc.EditMessage(c, "site-test", models.EditMessageRequest{MessageID: "msg-1", NewMsg: "updated"})
	require.NoError(t, err)
}

// --- DeleteMessage ---

func TestHistoryService_DeleteMessage_AlreadyDeleted_ShortCircuits(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)

	priorUpdatedAt := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	hydrated := &models.Message{
		MessageID: "m-abc",
		RoomID:    "r1",
		Sender:    models.Participant{Account: "u1"},
		Deleted:   true,
		UpdatedAt: &priorUpdatedAt,
	}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m-abc").Return(hydrated, nil)

	// Already-deleted: no parent lookup, no publish. tcount was persisted by
	// countAndSetParentTcount on the first delete and is durable in Cassandra.
	resp, err := svc.DeleteMessage(c, "site-test", models.DeleteMessageRequest{MessageID: "m-abc"})
	require.NoError(t, err)
	assert.Equal(t, "m-abc", resp.MessageID)
	assert.Equal(t, priorUpdatedAt.UnixMilli(), resp.DeletedAt, "short-circuit should echo the existing updated_at")
}

func TestHistoryService_DeleteMessage_AlreadyDeleted_ThreadReply_ShortCircuits(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)

	priorUpdatedAt := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	hydrated := &models.Message{
		MessageID:      "reply-abc",
		RoomID:         "r1",
		Sender:         models.Participant{Account: "u1", ID: "u1-id"},
		Deleted:        true,
		UpdatedAt:      &priorUpdatedAt,
		ThreadParentID: "parent-xyz",
		TShow:          false,
	}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "reply-abc").Return(hydrated, nil)

	// No parent lookup, no publish: tcount is durable in Cassandra from the first delete.
	resp, err := svc.DeleteMessage(c, "site-test", models.DeleteMessageRequest{MessageID: "reply-abc"})
	require.NoError(t, err)
	assert.Equal(t, "reply-abc", resp.MessageID)
	assert.Equal(t, priorUpdatedAt.UnixMilli(), resp.DeletedAt)
}

// TestHistoryService_DeleteMessage_AlreadyDeleted_NilUpdatedAt verifies that a
// deleted record with nil UpdatedAt returns success with DeletedAt=0.
func TestHistoryService_DeleteMessage_AlreadyDeleted_NilUpdatedAt(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)

	hydrated := &models.Message{
		MessageID: "m-legacy",
		RoomID:    "r1",
		Sender:    models.Participant{Account: "u1", ID: "u1-id"},
		Deleted:   true,
		UpdatedAt: nil, // legacy record: no delete timestamp stored
	}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m-legacy").Return(hydrated, nil)

	resp, err := svc.DeleteMessage(c, "site-test", models.DeleteMessageRequest{MessageID: "m-legacy"})
	require.NoError(t, err, "already-deleted with nil UpdatedAt must still return success")
	assert.Equal(t, "m-legacy", resp.MessageID)
	assert.Equal(t, int64(0), resp.DeletedAt, "DeletedAt should be 0 when UpdatedAt is nil")
}

// TestHistoryService_DeleteMessage_AlreadyDeleted_ThreadReply_NilUpdatedAt verifies that a
// deleted thread reply with nil UpdatedAt returns success with DeletedAt=0, no parent lookup.
func TestHistoryService_DeleteMessage_AlreadyDeleted_ThreadReply_NilUpdatedAt(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)

	hydrated := &models.Message{
		MessageID:      "reply-legacy",
		RoomID:         "r1",
		Sender:         models.Participant{Account: "u1", ID: "u1-id"},
		Deleted:        true,
		UpdatedAt:      nil, // legacy thread reply with no stored delete timestamp
		ThreadParentID: "parent-xyz",
		TShow:          false,
	}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "reply-legacy").Return(hydrated, nil)

	resp, err := svc.DeleteMessage(c, "site-test", models.DeleteMessageRequest{MessageID: "reply-legacy"})
	require.NoError(t, err, "already-deleted thread reply with nil UpdatedAt must return success")
	assert.Equal(t, "reply-legacy", resp.MessageID)
	assert.Equal(t, int64(0), resp.DeletedAt)
}

func TestHistoryService_DeleteMessage_NotSubscribed(t *testing.T) {
	svc, _, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, false, nil)

	resp, err := svc.DeleteMessage(c, "site-test", models.DeleteMessageRequest{MessageID: "m-abc"})
	assert.Nil(t, resp)

	var routeErr *errcode.Error
	require.ErrorAs(t, err, &routeErr)
	assert.Equal(t, errcode.CodeForbidden, routeErr.Code)
	assert.Equal(t, "not subscribed to room", routeErr.Message)
}

func TestHistoryService_DeleteMessage_NotSender(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)

	hydrated := &models.Message{
		MessageID: "m-abc",
		RoomID:    "r1",
		Sender:    models.Participant{Account: "someone-else"},
	}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m-abc").Return(hydrated, nil)

	resp, err := svc.DeleteMessage(c, "site-test", models.DeleteMessageRequest{MessageID: "m-abc"})
	assert.Nil(t, resp)

	var routeErr *errcode.Error
	require.ErrorAs(t, err, &routeErr)
	assert.Equal(t, errcode.CodeForbidden, routeErr.Code)
	assert.Equal(t, "only the sender can delete", routeErr.Message)
}

func TestHistoryService_DeleteMessage_NotFound(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	msgs.EXPECT().GetMessageByID(gomock.Any(), "missing").Return(nil, nil)

	resp, err := svc.DeleteMessage(c, "site-test", models.DeleteMessageRequest{MessageID: "missing"})
	assert.Nil(t, resp)

	var routeErr *errcode.Error
	require.ErrorAs(t, err, &routeErr)
	assert.Equal(t, errcode.CodeNotFound, routeErr.Code)
}

func TestHistoryService_DeleteMessage_WrongRoom(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)

	// Message exists but in a different room — findMessage returns ErrNotFound (no leak).
	hydrated := &models.Message{
		MessageID: "m-abc",
		RoomID:    "other-room",
		Sender:    models.Participant{Account: "u1"},
	}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m-abc").Return(hydrated, nil)

	resp, err := svc.DeleteMessage(c, "site-test", models.DeleteMessageRequest{MessageID: "m-abc"})
	assert.Nil(t, resp)

	var routeErr *errcode.Error
	require.ErrorAs(t, err, &routeErr)
	assert.Equal(t, errcode.CodeNotFound, routeErr.Code)
}

func TestHistoryService_DeleteMessage_SoftDeleteFails(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)

	hydrated := &models.Message{
		MessageID: "m-abc",
		RoomID:    "r1",
		Sender:    models.Participant{Account: "u1"},
	}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m-abc").Return(hydrated, nil)
	msgs.EXPECT().
		SoftDeleteMessage(gomock.Any(), hydrated, gomock.Any()).
		Return(time.Time{}, false, (*int)(nil), (*time.Time)(nil), fmt.Errorf("cassandra timeout"))

	// No Publish expected when the UPDATE fails.

	resp, err := svc.DeleteMessage(c, "site-test", models.DeleteMessageRequest{MessageID: "m-abc"})
	assert.Nil(t, resp)
	assertInternalErr(t, err, "deleting message")
}

// Two simultaneous deletes: hydrate sees deleted=false but the LWT returns applied=false.
// The loser must not publish a duplicate event and returns the winner's timestamp.
func TestHistoryService_DeleteMessage_ConcurrentDeleteSkipsPublish(t *testing.T) {
	svc, msgs, subs, pub, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)

	hydrated := &models.Message{
		MessageID: "m-abc",
		RoomID:    "r1",
		Sender:    models.Participant{Account: "u1"},
		Deleted:   false,
	}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m-abc").Return(hydrated, nil)

	winnerWrote := time.Date(2026, 4, 28, 9, 0, 0, 0, time.UTC)
	msgs.EXPECT().
		SoftDeleteMessage(gomock.Any(), hydrated, gomock.Any()).
		Return(winnerWrote, false, (*int)(nil), (*time.Time)(nil), nil)

	// Critically, NO Publish call is expected — gomock will fail the test if
	// the handler tries to publish on the LWT-not-applied path.

	resp, err := svc.DeleteMessage(c, "site-test", models.DeleteMessageRequest{MessageID: "m-abc"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "m-abc", resp.MessageID)
	assert.Equal(t, winnerWrote.UnixMilli(), resp.DeletedAt)

	_ = pub // unused: asserting absence of Publish via gomock strict expectations
}

func TestHistoryService_DeleteMessage_PublishFails(t *testing.T) {
	svc, msgs, subs, pub, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)

	hydrated := &models.Message{
		MessageID: "m-abc",
		RoomID:    "r1",
		Sender:    models.Participant{Account: "u1"},
	}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m-abc").Return(hydrated, nil)
	msgs.EXPECT().
		SoftDeleteMessage(gomock.Any(), hydrated, gomock.Any()).
		DoAndReturn(func(_ context.Context, _ *models.Message, deletedAt time.Time) (time.Time, bool, *int, *time.Time, error) {
			return deletedAt, true, nil, nil, nil
		})

	pub.EXPECT().
		Publish(gomock.Any(), subject.MsgCanonicalDeleted("site-test"), gomock.Any(), gomock.Any()).
		Return(fmt.Errorf("nats disconnected"))

	expectEmptyPreviewWalk(msgs)

	resp, err := svc.DeleteMessage(c, "site-test", models.DeleteMessageRequest{MessageID: "m-abc"})
	require.NoError(t, err, "best-effort publish: failure is logged, not returned")
	require.NotNil(t, resp)
	assert.Equal(t, "m-abc", resp.MessageID)
}

func TestHistoryService_DeleteMessage_PublishesCanonicalDeletedEvent(t *testing.T) {
	svc, msgs, subs, pub, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)

	hydrated := &models.Message{
		MessageID: "msg-1",
		RoomID:    "r1",
		Sender:    models.Participant{Account: "u1", ID: "u1-id"},
		CreatedAt: time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC),
		Msg:       "content",
	}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "msg-1").Return(hydrated, nil)
	msgs.EXPECT().
		SoftDeleteMessage(gomock.Any(), hydrated, gomock.Any()).
		DoAndReturn(func(_ context.Context, _ *models.Message, deletedAt time.Time) (time.Time, bool, *int, *time.Time, error) {
			return deletedAt, true, nil, nil, nil
		})

	pub.EXPECT().
		Publish(gomock.Any(), subject.MsgCanonicalDeleted("site-test"), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, data []byte, _ string) error {
			var evt model.MessageEvent
			require.NoError(t, json.Unmarshal(data, &evt))
			assert.Equal(t, model.EventDeleted, evt.Event)
			assert.Equal(t, "msg-1", evt.Message.ID)
			assert.Equal(t, "r1", evt.Message.RoomID)
			require.NotNil(t, evt.Message.UpdatedAt, "deleted message must carry UpdatedAt = delete time")
			assert.Equal(t, "site-test", evt.SiteID)
			assert.NotZero(t, evt.Timestamp)
			return nil
		})

	expectEmptyPreviewWalk(msgs)

	resp, err := svc.DeleteMessage(c, "site-test", models.DeleteMessageRequest{MessageID: "msg-1"})
	require.NoError(t, err)
	require.NotNil(t, resp)
}

// EditMessage recomputes the room preview after the write and embeds it on the canonical event.
func TestHistoryService_EditMessage_EmbedsRefreshedPreview(t *testing.T) {
	svc, msgs, subs, pub, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	hydrated := &models.Message{
		MessageID: "msg-1",
		RoomID:    "r1",
		Sender:    models.Participant{Account: "u1", ID: "u1-id"},
		CreatedAt: time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC),
		Msg:       "original content",
	}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "msg-1").Return(hydrated, nil)
	msgs.EXPECT().UpdateMessageContent(gomock.Any(), hydrated, "updated content", gomock.Any()).Return(nil)
	// roomLastMessage walk: the refreshed preview is the edited message itself.
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{
			{MessageID: "msg-1", RoomID: "r1", Sender: models.Participant{Account: "u1", ID: "u1-id"}, Msg: "updated content", CreatedAt: hydrated.CreatedAt},
		}, false), nil)

	pub.EXPECT().
		Publish(gomock.Any(), subject.MsgCanonicalUpdated("site-test"), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, data []byte, _ string) error {
			var evt model.MessageEvent
			require.NoError(t, json.Unmarshal(data, &evt))
			require.NotNil(t, evt.PreviewMessage, "edit event must carry the refreshed room preview")
			assert.Equal(t, "msg-1", evt.PreviewMessage.MessageID)
			assert.Equal(t, "updated content", evt.PreviewMessage.Content)
			return nil
		})

	_, err := svc.EditMessage(c, "site-test", models.EditMessageRequest{MessageID: "msg-1", NewMsg: "updated content"})
	require.NoError(t, err)
}

// Deleting the room's last eligible message publishes a nil preview so clients clear
// it, AND clears the stored one. This service owns both halves: it is the only one that
// can tell "no eligible message left" from "the walk gave up".
func TestHistoryService_DeleteMessage_LastMessage_ClearsStoredPreview(t *testing.T) {
	svc, msgs, subs, rooms, pub, _, _, _ := newServiceWithRoomMock(t)
	c := testContext()
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	rooms.EXPECT().ClearPreview(gomock.Any(), "r1", gomock.Any()).Return(true, nil).Times(1)

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	hydrated := &models.Message{
		MessageID: "msg-1",
		RoomID:    "r1",
		Sender:    models.Participant{Account: "u1", ID: "u1-id"},
		CreatedAt: time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC),
		Msg:       "content",
	}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "msg-1").Return(hydrated, nil)
	msgs.EXPECT().
		SoftDeleteMessage(gomock.Any(), hydrated, gomock.Any()).
		DoAndReturn(func(_ context.Context, _ *models.Message, deletedAt time.Time) (time.Time, bool, *int, *time.Time, error) {
			return deletedAt, true, nil, nil, nil
		})
	// roomLastMessage walk finds no eligible message → cleared preview.
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage(nil, false), nil)

	pub.EXPECT().
		Publish(gomock.Any(), subject.MsgCanonicalDeleted("site-test"), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, data []byte, _ string) error {
			var evt model.MessageEvent
			require.NoError(t, json.Unmarshal(data, &evt))
			assert.Nil(t, evt.PreviewMessage, "deleting the last eligible message clears the preview")
			return nil
		})

	_, err := svc.DeleteMessage(c, "site-test", models.DeleteMessageRequest{MessageID: "msg-1"})
	require.NoError(t, err)
}

// The canonical event must be published BEFORE the preview is persisted. The Cassandra
// delete has already committed by this point, so a store that stalls until the request
// deadline would leave the mutation invisible to every canonical consumer — strictly
// worse than a stale room-list row, which the next read repairs anyway.
func TestHistoryService_DeleteMessage_PublishesCanonicalBeforePersistingPreview(t *testing.T) {
	svc, msgs, subs, rooms, pub, _, _, _ := newServiceWithRoomMock(t)
	c := testContext()
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	hydrated := &models.Message{
		MessageID: "msg-1",
		RoomID:    "r1",
		Sender:    models.Participant{Account: "u1", ID: "u1-id"},
		CreatedAt: time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC),
		Msg:       "content",
	}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "msg-1").Return(hydrated, nil)
	msgs.EXPECT().
		SoftDeleteMessage(gomock.Any(), hydrated, gomock.Any()).
		DoAndReturn(func(_ context.Context, _ *models.Message, deletedAt time.Time) (time.Time, bool, *int, *time.Time, error) {
			return deletedAt, true, nil, nil, nil
		})
	// A survivor remains, so the mutation takes the body-update branch.
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{{
			MessageID: "msg-0",
			RoomID:    "r1",
			Sender:    models.Participant{Account: "u1", ID: "u1-id"},
			CreatedAt: time.Date(2026, 5, 14, 11, 0, 0, 0, time.UTC),
			Msg:       "survivor",
		}}, false), nil)

	var order []string
	pub.EXPECT().
		Publish(gomock.Any(), subject.MsgCanonicalDeleted("site-test"), gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, string, []byte, string) error {
			order = append(order, "publish")
			return nil
		})
	rooms.EXPECT().UpdatePreviewBody(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, string, model.PreviewMessage, string, int64) (bool, error) {
			order = append(order, "persist")
			return true, nil
		})

	_, err := svc.DeleteMessage(c, "site-test", models.DeleteMessageRequest{MessageID: "msg-1"})
	require.NoError(t, err)
	assert.Equal(t, []string{"publish", "persist"}, order,
		"an optional store write must never queue ahead of the canonical publish")
}

// --- #226: a mutation that cannot replace the body it changed must withdraw its key ---

// stageDeleteOfPreviewedMessage stages the invariant half of a delete-path preview test:
// the access check, the message, the soft delete and the canonical publish. Each caller
// stubs the walk and the preview writes, which is what these tests are actually about.
func stageDeleteOfPreviewedMessage(rooms *mocks.MockRoomRepository, msgs *mocks.MockMessageRepository, subs *mocks.MockSubscriptionRepository, pub *mocks.MockEventPublisher) {
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	hydrated := &models.Message{
		MessageID: "msg-1",
		RoomID:    "r1",
		Sender:    models.Participant{Account: "u1", ID: "u1-id"},
		CreatedAt: time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC),
		Msg:       "content",
	}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "msg-1").Return(hydrated, nil)
	msgs.EXPECT().
		SoftDeleteMessage(gomock.Any(), hydrated, gomock.Any()).
		DoAndReturn(func(_ context.Context, _ *models.Message, deletedAt time.Time) (time.Time, bool, *int, *time.Time, error) {
			return deletedAt, true, nil, nil, nil
		})
	pub.EXPECT().
		Publish(gomock.Any(), subject.MsgCanonicalDeleted("site-test"), gomock.Any(), gomock.Any()).
		Return(nil)
}

// expectSurvivorWalk stubs the mutation walk to resolve an older eligible message, so the
// mutation takes the body-update branch.
func expectSurvivorWalk(msgs *mocks.MockMessageRepository) {
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{{
			MessageID: "msg-0",
			RoomID:    "r1",
			Sender:    models.Participant{Account: "u1", ID: "u1-id"},
			CreatedAt: time.Date(2026, 5, 14, 11, 0, 0, 0, time.UTC),
			Msg:       "survivor",
		}}, false), nil)
}

// The write that would have replaced the body failed, so the body still describes the
// message just deleted — and a mutation never moves lastMsgId, so the reader's identity
// check goes on passing and serves deleted content forever. Withdrawing the key is what
// makes the next read miss, walk and repair (#226).
func TestHistoryService_DeleteMessage_PreviewWriteFails_WithdrawsTheFreshnessKey(t *testing.T) {
	svc, msgs, subs, rooms, pub, _, _, _ := newServiceWithRoomMock(t)
	stageDeleteOfPreviewedMessage(rooms, msgs, subs, pub)
	expectSurvivorWalk(msgs)

	rooms.EXPECT().UpdatePreviewBody(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(false, errors.New("vault unavailable"))
	// Keyed on the message this mutation changed, NOT on the id the walk observed: the
	// freshness key can name a message the body does not describe.
	rooms.EXPECT().InvalidatePreviewKey(gomock.Any(), "r1", "msg-1", gomock.Any()).Return(nil)

	_, err := svc.DeleteMessage(testContext(), "site-test", models.DeleteMessageRequest{MessageID: "msg-1"})
	require.NoError(t, err, "the repair is best-effort; the delete itself still succeeds")
}

// A guarded write that loses its guard returns no error, and the stored body is just as
// stale as if it had failed outright. Without the applied signal this case is invisible.
func TestHistoryService_DeleteMessage_PreviewWriteRejected_WithdrawsTheFreshnessKey(t *testing.T) {
	svc, msgs, subs, rooms, pub, _, _, _ := newServiceWithRoomMock(t)
	stageDeleteOfPreviewedMessage(rooms, msgs, subs, pub)
	expectSurvivorWalk(msgs)

	rooms.EXPECT().UpdatePreviewBody(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(false, nil)
	rooms.EXPECT().InvalidatePreviewKey(gomock.Any(), "r1", "msg-1", gomock.Any()).Return(nil)

	_, err := svc.DeleteMessage(testContext(), "site-test", models.DeleteMessageRequest{MessageID: "msg-1"})
	require.NoError(t, err)
}

// The happy path must not withdraw anything: the body was replaced, so the key still
// certifies it and the room keeps serving from the document with no walk.
func TestHistoryService_DeleteMessage_PreviewWriteApplied_KeepsTheFreshnessKey(t *testing.T) {
	svc, msgs, subs, rooms, pub, _, _, _ := newServiceWithRoomMock(t)
	stageDeleteOfPreviewedMessage(rooms, msgs, subs, pub)
	expectSurvivorWalk(msgs)

	// No InvalidatePreviewKey expectation: the call would fail the test.
	rooms.EXPECT().UpdatePreviewBody(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(true, nil)

	_, err := svc.DeleteMessage(testContext(), "site-test", models.DeleteMessageRequest{MessageID: "msg-1"})
	require.NoError(t, err)
}

// A clear that fails leaves the deleted message's own body stored and certified — the
// worst shape of #226, since the content is gone from history but still on the room list.
func TestHistoryService_DeleteMessage_ClearFails_WithdrawsTheFreshnessKey(t *testing.T) {
	svc, msgs, subs, rooms, pub, _, _, _ := newServiceWithRoomMock(t)
	stageDeleteOfPreviewedMessage(rooms, msgs, subs, pub)
	// An empty walk: the deleted message was the room's last eligible one.
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage(nil, false), nil)

	rooms.EXPECT().ClearPreview(gomock.Any(), "r1", gomock.Any()).Return(false, errors.New("mongo timeout"))
	rooms.EXPECT().InvalidatePreviewKey(gomock.Any(), "r1", "msg-1", gomock.Any()).Return(nil)

	_, err := svc.DeleteMessage(testContext(), "site-test", models.DeleteMessageRequest{MessageID: "msg-1"})
	require.NoError(t, err)
}

// A walk that gives up mid-flight is NOT evidence the room is empty, so the BODY must
// survive — that distinction is what the three-state walk exists for, and clearing here
// would drop a preview that is merely unread. Its certification is another matter: the
// mutation did change the message the body describes, and nothing re-derived it, so the
// key comes off and the next read resolves what the walk could not.
func TestHistoryService_DeleteMessage_DegradedWalk_WithdrawsTheKeyButNotTheBody(t *testing.T) {
	svc, msgs, subs, rooms, pub, _, _, _ := newServiceWithRoomMock(t)
	stageDeleteOfPreviewedMessage(rooms, msgs, subs, pub)
	// No ClearPreview/UpdatePreviewBody expectations: either call fails the test.
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(cassrepo.Page[models.Message]{}, errors.New("cassandra unavailable"))

	rooms.EXPECT().InvalidatePreviewKey(gomock.Any(), "r1", "msg-1", gomock.Any()).Return(nil)

	_, err := svc.DeleteMessage(testContext(), "site-test", models.DeleteMessageRequest{MessageID: "msg-1"})
	require.NoError(t, err)
}

// The edit path carries the identical exposure: an edit does not move lastMsgId either,
// so a body it failed to reseal keeps reading as current with the pre-edit content.
func TestHistoryService_EditMessage_PreviewWriteFails_WithdrawsTheFreshnessKey(t *testing.T) {
	svc, msgs, subs, rooms, pub, _, _, _ := newServiceWithRoomMock(t)
	c := testContext()
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	hydrated := &models.Message{
		MessageID: "msg-1",
		RoomID:    "r1",
		Sender:    models.Participant{Account: "u1", ID: "u1-id"},
		CreatedAt: time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC),
		Msg:       "before",
	}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "msg-1").Return(hydrated, nil)
	msgs.EXPECT().UpdateMessageContent(gomock.Any(), hydrated, "after", gomock.Any()).Return(nil)
	expectSurvivorWalk(msgs)
	pub.EXPECT().
		Publish(gomock.Any(), subject.MsgCanonicalUpdated("site-test"), gomock.Any(), gomock.Any()).
		Return(nil)

	rooms.EXPECT().UpdatePreviewBody(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(false, errors.New("vault unavailable"))
	rooms.EXPECT().InvalidatePreviewKey(gomock.Any(), "r1", "msg-1", gomock.Any()).Return(nil)

	_, err := svc.EditMessage(c, "site-test", models.EditMessageRequest{MessageID: "msg-1", NewMsg: "after"})
	require.NoError(t, err)
}

// A hidden thread reply never reaches the room timeline, so no stored preview can
// describe it. There is nothing to repair and nothing to withdraw — the walk is skipped
// and the room document is not touched at all.
func TestHistoryService_DeleteMessage_HiddenThreadReply_TouchesNoStoredPreview(t *testing.T) {
	svc, msgs, subs, rooms, pub, _, _, _ := newServiceWithRoomMock(t)
	c := testContext()
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	// No preview expectations of any kind: any room-preview call fails the test.

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	hydrated := &models.Message{
		MessageID:      "reply-1",
		RoomID:         "r1",
		Sender:         models.Participant{Account: "u1", ID: "u1-id"},
		CreatedAt:      time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC),
		Msg:            "hidden reply",
		ThreadParentID: "parent-1",
		TShow:          false,
	}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "reply-1").Return(hydrated, nil)
	msgs.EXPECT().
		SoftDeleteMessage(gomock.Any(), hydrated, gomock.Any()).
		DoAndReturn(func(_ context.Context, _ *models.Message, deletedAt time.Time) (time.Time, bool, *int, *time.Time, error) {
			return deletedAt, true, nil, nil, nil
		})
	pub.EXPECT().
		Publish(gomock.Any(), subject.MsgCanonicalDeleted("site-test"), gomock.Any(), gomock.Any()).
		Return(nil)

	_, err := svc.DeleteMessage(c, "site-test", models.DeleteMessageRequest{MessageID: "reply-1"})
	require.NoError(t, err)
}

// A hidden thread reply (TShow=false) edit skips the room-preview walk// A hidden thread reply (TShow=false) edit skips the room-preview walk (no GetMessagesBefore) and
// carries no preview; clients tell it apart via ThreadParentMessageID and drive the preview themselves.
func TestHistoryService_EditMessage_HiddenThreadReply_SkipsPreviewWalk(t *testing.T) {
	svc, msgs, subs, pub, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	parentCreatedAt := time.Date(2026, 5, 10, 9, 0, 0, 0, time.UTC)
	hydrated := &models.Message{
		MessageID:             "reply-1",
		RoomID:                "r1",
		Sender:                models.Participant{Account: "u1", ID: "u1-id"},
		CreatedAt:             time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC),
		Msg:                   "original reply",
		ThreadParentID:        "parent-1",
		ThreadParentCreatedAt: &parentCreatedAt,
		TShow:                 false,
	}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "reply-1").Return(hydrated, nil)
	msgs.EXPECT().UpdateMessageContent(gomock.Any(), hydrated, "edited reply", gomock.Any()).Return(nil)
	// No GetMessagesBefore expectation: the walk must be skipped for hidden thread replies.

	pub.EXPECT().
		Publish(gomock.Any(), subject.MsgCanonicalUpdated("site-test"), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, data []byte, _ string) error {
			var evt model.MessageEvent
			require.NoError(t, json.Unmarshal(data, &evt))
			assert.Nil(t, evt.PreviewMessage, "hidden thread reply edit carries no room preview")
			assert.Equal(t, "parent-1", evt.Message.ThreadParentMessageID, "thread linkage must survive for the client to key on")
			return nil
		})

	_, err := svc.EditMessage(c, "site-test", models.EditMessageRequest{MessageID: "reply-1", NewMsg: "edited reply"})
	require.NoError(t, err)
}

// A TShow=true thread reply appears in the room timeline, so its edit DOES refresh the preview.
func TestHistoryService_EditMessage_TShowThreadReply_EmbedsPreview(t *testing.T) {
	svc, msgs, subs, pub, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	parentCreatedAt := time.Date(2026, 5, 10, 9, 0, 0, 0, time.UTC)
	hydrated := &models.Message{
		MessageID:             "reply-1",
		RoomID:                "r1",
		Sender:                models.Participant{Account: "u1", ID: "u1-id"},
		CreatedAt:             time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC),
		Msg:                   "original reply",
		ThreadParentID:        "parent-1",
		ThreadParentCreatedAt: &parentCreatedAt,
		TShow:                 true,
	}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "reply-1").Return(hydrated, nil)
	msgs.EXPECT().UpdateMessageContent(gomock.Any(), hydrated, "edited reply", gomock.Any()).Return(nil)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{
			{MessageID: "m-latest", RoomID: "r1", Sender: models.Participant{Account: "u1", ID: "u1-id"}, Msg: "latest", CreatedAt: hydrated.CreatedAt},
		}, false), nil)

	pub.EXPECT().
		Publish(gomock.Any(), subject.MsgCanonicalUpdated("site-test"), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, data []byte, _ string) error {
			var evt model.MessageEvent
			require.NoError(t, json.Unmarshal(data, &evt))
			require.NotNil(t, evt.PreviewMessage, "TShow thread reply edit must refresh the room preview")
			assert.Equal(t, "m-latest", evt.PreviewMessage.MessageID)
			return nil
		})

	_, err := svc.EditMessage(c, "site-test", models.EditMessageRequest{MessageID: "reply-1", NewMsg: "edited reply"})
	require.NoError(t, err)
}

// An edited thread reply must carry ThreadParentMessageID/TShow on the canonical event so
// broadcast-worker can route it to thread subscribers and search-sync keeps the linkage.
func TestHistoryService_EditMessage_ThreadReply_CarriesThreadFields(t *testing.T) {
	svc, msgs, subs, pub, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	parentCreatedAt := time.Date(2026, 5, 10, 9, 0, 0, 0, time.UTC)
	hydrated := &models.Message{
		MessageID:             "reply-1",
		RoomID:                "r1",
		Sender:                models.Participant{Account: "u1", ID: "u1-id"},
		CreatedAt:             time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC),
		Msg:                   "original reply",
		ThreadParentID:        "parent-1",
		ThreadParentCreatedAt: &parentCreatedAt,
		TShow:                 false,
	}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "reply-1").Return(hydrated, nil)
	msgs.EXPECT().UpdateMessageContent(gomock.Any(), hydrated, "edited reply", gomock.Any()).Return(nil)
	// No GetMessagesBefore expectation: thread replies skip the preview walk.

	pub.EXPECT().
		Publish(gomock.Any(), subject.MsgCanonicalUpdated("site-test"), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, data []byte, _ string) error {
			var evt model.MessageEvent
			require.NoError(t, json.Unmarshal(data, &evt))
			assert.Equal(t, "parent-1", evt.Message.ThreadParentMessageID, "edit event must carry ThreadParentMessageID for thread routing")
			assert.False(t, evt.Message.TShow, "edit event must carry TShow")
			require.NotNil(t, evt.Message.ThreadParentMessageCreatedAt, "edit event must carry parent createdAt for the visibility gate")
			assert.Equal(t, parentCreatedAt.UTC(), evt.Message.ThreadParentMessageCreatedAt.UTC())
			return nil
		})

	resp, err := svc.EditMessage(c, "site-test", models.EditMessageRequest{
		MessageID: "reply-1",
		NewMsg:    "edited reply",
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
}

// A deleted thread reply must carry ThreadParentMessageID/TShow so broadcast-worker can route it to thread subscribers.
func TestHistoryService_DeleteMessage_ThreadReply_CarriesThreadFields(t *testing.T) {
	svc, msgs, subs, pub, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	parentCreatedAt := time.Date(2026, 5, 10, 9, 0, 0, 0, time.UTC)
	hydrated := &models.Message{
		MessageID:             "reply-1",
		RoomID:                "r1",
		Sender:                models.Participant{Account: "u1", ID: "u1-id"},
		CreatedAt:             time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC),
		Msg:                   "reply",
		ThreadParentID:        "parent-1",
		ThreadParentCreatedAt: &parentCreatedAt,
		TShow:                 false,
	}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "reply-1").Return(hydrated, nil)
	msgs.EXPECT().
		SoftDeleteMessage(gomock.Any(), hydrated, gomock.Any()).
		DoAndReturn(func(_ context.Context, _ *models.Message, deletedAt time.Time) (time.Time, bool, *int, *time.Time, error) {
			return deletedAt, true, nil, nil, nil
		})

	// No GetMessagesBefore expectation: thread replies skip the preview walk.

	pub.EXPECT().
		Publish(gomock.Any(), subject.MsgCanonicalDeleted("site-test"), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, data []byte, _ string) error {
			var evt model.MessageEvent
			require.NoError(t, json.Unmarshal(data, &evt))
			assert.Equal(t, "parent-1", evt.Message.ThreadParentMessageID, "delete event must carry ThreadParentMessageID for thread routing")
			assert.False(t, evt.Message.TShow, "delete event must carry TShow")
			require.NotNil(t, evt.Message.ThreadParentMessageCreatedAt, "delete event must carry parent createdAt for the visibility gate")
			assert.Equal(t, parentCreatedAt.UTC(), evt.Message.ThreadParentMessageCreatedAt.UTC())
			return nil
		})

	resp, err := svc.DeleteMessage(c, "site-test", models.DeleteMessageRequest{MessageID: "reply-1"})
	require.NoError(t, err)
	require.NotNil(t, resp)
}

// Nats-Msg-Id "{messageID}:deleted" is distinct from the .created key so the JetStream
// dedup window can't collapse a delete against an earlier create.
func TestHistoryService_DeleteMessage_PassesDedupMessageID(t *testing.T) {
	svc, msgs, subs, pub, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	hydrated := &models.Message{
		MessageID: "msg-1",
		RoomID:    "r1",
		Sender:    models.Participant{Account: "u1", ID: "u1-id"},
		Msg:       "content",
	}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "msg-1").Return(hydrated, nil)
	msgs.EXPECT().
		SoftDeleteMessage(gomock.Any(), hydrated, gomock.Any()).
		DoAndReturn(func(_ context.Context, _ *models.Message, deletedAt time.Time) (time.Time, bool, *int, *time.Time, error) {
			return deletedAt, true, nil, nil, nil
		})

	pub.EXPECT().
		Publish(gomock.Any(), subject.MsgCanonicalDeleted("site-test"), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, data []byte, msgID string) error {
			var evt model.MessageEvent
			require.NoError(t, json.Unmarshal(data, &evt))
			assert.Equal(t, natsutil.CanonicalDedupID(&evt), msgID)
			return nil
		})

	expectEmptyPreviewWalk(msgs)

	_, err := svc.DeleteMessage(c, "site-test", models.DeleteMessageRequest{MessageID: "msg-1"})
	require.NoError(t, err)
}

// Deleting a thread reply sets NewTCount on the canonical event so broadcast-worker can do DM-aware routing.
func TestHistoryService_DeleteMessage_ThreadReply_PublishesThreadMetadataEvent(t *testing.T) {
	svc, msgs, subs, pub, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)

	parentCreatedAt := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	hydrated := &models.Message{
		MessageID:             "reply-1",
		RoomID:                "r1",
		Sender:                models.Participant{Account: "u1", ID: "u1-id"},
		CreatedAt:             time.Date(2026, 5, 14, 13, 0, 0, 0, time.UTC),
		Msg:                   "reply content",
		ThreadParentID:        "parent-1",
		ThreadParentCreatedAt: &parentCreatedAt,
		TShow:                 false,
	}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "reply-1").Return(hydrated, nil)

	newTcount := 4
	newTlm := time.Date(2026, 5, 14, 12, 30, 0, 0, time.UTC)
	msgs.EXPECT().
		SoftDeleteMessage(gomock.Any(), hydrated, gomock.Any()).
		DoAndReturn(func(_ context.Context, _ *models.Message, deletedAt time.Time) (time.Time, bool, *int, *time.Time, error) {
			return deletedAt, true, &newTcount, &newTlm, nil
		})

	// No GetMessagesBefore expectation: thread replies skip the preview walk.

	pub.EXPECT().
		Publish(gomock.Any(), subject.MsgCanonicalDeleted("site-test"), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, data []byte, _ string) error {
			var evt model.MessageEvent
			require.NoError(t, json.Unmarshal(data, &evt))
			assert.Equal(t, model.EventDeleted, evt.Event)
			require.NotNil(t, evt.NewTCount)
			assert.Equal(t, 4, *evt.NewTCount)
			require.NotNil(t, evt.NewThreadLastMsgAt, "delete event must carry the surviving thread last-message time")
			assert.True(t, evt.NewThreadLastMsgAt.Equal(newTlm), "NewThreadLastMsgAt must equal the newest surviving reply's createdAt")
			assert.Equal(t, "reply-1", evt.Message.ID)
			assert.Equal(t, "r1", evt.Message.RoomID)
			assert.Equal(t, "parent-1", evt.Message.ThreadParentMessageID)
			return nil
		})

	resp, err := svc.DeleteMessage(c, "site-test", models.DeleteMessageRequest{MessageID: "reply-1"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "reply-1", resp.MessageID)
}

// If the canonical deleted event fails to publish, DeleteMessage still succeeds — Cassandra is the source of truth.
func TestHistoryService_DeleteMessage_ThreadReply_PublishFailsButDeleteSucceeds(t *testing.T) {
	svc, msgs, subs, pub, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)

	parentCreatedAt := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	hydrated := &models.Message{
		MessageID:             "reply-1",
		RoomID:                "r1",
		Sender:                models.Participant{Account: "u1", ID: "u1-id"},
		CreatedAt:             time.Date(2026, 5, 14, 13, 0, 0, 0, time.UTC),
		Msg:                   "reply content",
		ThreadParentID:        "parent-1",
		ThreadParentCreatedAt: &parentCreatedAt,
	}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "reply-1").Return(hydrated, nil)

	newTcount := 4
	msgs.EXPECT().
		SoftDeleteMessage(gomock.Any(), hydrated, gomock.Any()).
		DoAndReturn(func(_ context.Context, _ *models.Message, deletedAt time.Time) (time.Time, bool, *int, *time.Time, error) {
			return deletedAt, true, &newTcount, nil, nil
		})

	// No GetMessagesBefore expectation: thread replies skip the preview walk.

	pub.EXPECT().
		Publish(gomock.Any(), subject.MsgCanonicalDeleted("site-test"), gomock.Any(), gomock.Any()).
		Return(fmt.Errorf("nats disconnected"))

	resp, err := svc.DeleteMessage(c, "site-test", models.DeleteMessageRequest{MessageID: "reply-1"})
	require.NoError(t, err, "best-effort publish: failure must be logged, not returned")
	require.NotNil(t, resp)
	assert.Equal(t, "reply-1", resp.MessageID)
}

// No ThreadMetadataUpdatedEvent when the repo returns nil tcount (CAS skipped: parent missing or tcount never written).
func TestHistoryService_DeleteMessage_ThreadReply_NoMetadataEventWhenTCountNil(t *testing.T) {
	svc, msgs, subs, pub, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)

	hydrated := &models.Message{
		MessageID:      "reply-1",
		RoomID:         "r1",
		Sender:         models.Participant{Account: "u1"},
		CreatedAt:      time.Date(2026, 5, 14, 13, 0, 0, 0, time.UTC),
		ThreadParentID: "parent-1",
	}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "reply-1").Return(hydrated, nil)
	msgs.EXPECT().
		SoftDeleteMessage(gomock.Any(), hydrated, gomock.Any()).
		DoAndReturn(func(_ context.Context, _ *models.Message, deletedAt time.Time) (time.Time, bool, *int, *time.Time, error) {
			return deletedAt, true, nil, nil, nil
		})

	// No GetMessagesBefore expectation: thread replies skip the preview walk.

	pub.EXPECT().
		Publish(gomock.Any(), subject.MsgCanonicalDeleted("site-test"), gomock.Any(), gomock.Any()).
		Return(nil)

	resp, err := svc.DeleteMessage(c, "site-test", models.DeleteMessageRequest{MessageID: "reply-1"})
	require.NoError(t, err)
	require.NotNil(t, resp)
}

// --- Quote redaction ---

func TestHistoryService_QuoteRedact_BeforeAccessSince(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)

	quotedAt := joinTime.Add(-1 * time.Hour)
	msg := models.Message{
		MessageID: "m1",
		RoomID:    "r1",
		CreatedAt: joinTime.Add(time.Hour),
		QuotedParentMessage: &models.QuotedParentMessage{
			MessageID: "q1",
			Msg:       "original text",
			CreatedAt: quotedAt,
		},
	}
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{msg}, false), nil)

	resp, err := svc.LoadHistory(c, models.LoadHistoryRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Messages, 1)
	q := resp.Messages[0].QuotedParentMessage
	require.NotNil(t, q)
	assert.Equal(t, service.UnavailableQuoteMsg, q.Msg)
	assert.Empty(t, q.MessageID)
}

func TestHistoryService_QuoteRedact_AfterAccessSince(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)

	quotedAt := joinTime.Add(30 * time.Minute)
	msg := models.Message{
		MessageID: "m1",
		RoomID:    "r1",
		CreatedAt: joinTime.Add(time.Hour),
		QuotedParentMessage: &models.QuotedParentMessage{
			MessageID: "q1",
			Msg:       "original text",
			CreatedAt: quotedAt,
		},
	}
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{msg}, false), nil)

	resp, err := svc.LoadHistory(c, models.LoadHistoryRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Messages, 1)
	q := resp.Messages[0].QuotedParentMessage
	require.NotNil(t, q)
	assert.Equal(t, "original text", q.Msg)
	assert.Equal(t, "q1", q.MessageID)
}

func TestHistoryService_QuoteRedact_NoAccessWindow(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)

	quotedAt := joinTime.Add(-24 * time.Hour)
	msg := models.Message{
		MessageID: "m1",
		RoomID:    "r1",
		CreatedAt: joinTime.Add(time.Hour),
		QuotedParentMessage: &models.QuotedParentMessage{
			MessageID: "q1",
			Msg:       "old text",
			CreatedAt: quotedAt,
		},
	}
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{msg}, false), nil)

	resp, err := svc.LoadHistory(c, models.LoadHistoryRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Messages, 1)
	q := resp.Messages[0].QuotedParentMessage
	require.NotNil(t, q)
	assert.Equal(t, "old text", q.Msg, "no redaction when accessSince is nil")
}

func TestHistoryService_QuoteRedact_SingleMessage(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)

	quotedAt := joinTime.Add(-2 * time.Hour)
	msg := &models.Message{
		MessageID: "m1",
		RoomID:    "r1",
		CreatedAt: joinTime.Add(time.Hour),
		QuotedParentMessage: &models.QuotedParentMessage{
			MessageID: "q1",
			Msg:       "secret",
			CreatedAt: quotedAt,
		},
	}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m1").Return(msg, nil)

	resp, err := svc.GetMessageByID(c, models.GetMessageByIDRequest{MessageID: "m1"})
	require.NoError(t, err)
	require.NotNil(t, resp.QuotedParentMessage)
	assert.Equal(t, service.UnavailableQuoteMsg, resp.QuotedParentMessage.Msg)
	assert.Empty(t, resp.QuotedParentMessage.MessageID)
}

// --- TShow redaction ---

// A quoted ThreadParentCreatedAt pre-dating accessSince gets the unavailable stub;
// the timestamp is embedded at write time, no Cassandra fetch needed.
func TestHistoryService_TShow_ParentBeforeAccessSince(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)

	msg := models.Message{
		MessageID: "m1",
		RoomID:    "r1",
		CreatedAt: joinTime.Add(time.Hour),
		TShow:     true,
		QuotedParentMessage: &models.QuotedParentMessage{
			MessageID:             "p1",
			Msg:                   "thread parent text",
			CreatedAt:             joinTime.Add(30 * time.Minute),
			ThreadParentID:        "p1",
			ThreadParentCreatedAt: ptrTime(joinTime.Add(-2 * time.Hour)), // before accessSince → redact
		},
	}
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{msg}, false), nil)

	resp, err := svc.LoadHistory(c, models.LoadHistoryRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Messages, 1)
	q := resp.Messages[0].QuotedParentMessage
	require.NotNil(t, q)
	assert.Equal(t, service.UnavailableQuoteMsg, q.Msg)
	assert.Empty(t, q.MessageID)
}

// TShow message whose QuotedParentMessage.ThreadParentCreatedAt is within the access
// window → not redacted.
func TestHistoryService_TShow_ParentAfterAccessSince(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)

	msg := models.Message{
		MessageID: "m1",
		RoomID:    "r1",
		CreatedAt: joinTime.Add(time.Hour),
		TShow:     true,
		QuotedParentMessage: &models.QuotedParentMessage{
			MessageID:             "p1",
			Msg:                   "thread parent text",
			CreatedAt:             joinTime.Add(30 * time.Minute),
			ThreadParentID:        "p1",
			ThreadParentCreatedAt: ptrTime(joinTime.Add(10 * time.Minute)), // within window → keep
		},
	}
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{msg}, false), nil)

	resp, err := svc.LoadHistory(c, models.LoadHistoryRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Messages, 1)
	q := resp.Messages[0].QuotedParentMessage
	require.NotNil(t, q)
	assert.Equal(t, "thread parent text", q.Msg, "parent is accessible; snapshot must not be redacted")
}

// TShow message with no QuotedParentMessage → nothing to redact.
func TestHistoryService_TShow_NoQuotedParentMessage(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)

	msg := models.Message{
		MessageID: "m1",
		RoomID:    "r1",
		CreatedAt: joinTime.Add(time.Hour),
		TShow:     true,
		// QuotedParentMessage intentionally nil
	}
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{msg}, false), nil)

	resp, err := svc.LoadHistory(c, models.LoadHistoryRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Messages, 1)
	assert.Nil(t, resp.Messages[0].QuotedParentMessage)
}

// Two TShow messages pointing to the same inaccessible thread parent → both redacted.
func TestHistoryService_TShow_TwoMessagesWithSameParent_BothRedacted(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)

	makeMsg := func(id string) models.Message {
		return models.Message{
			MessageID: id,
			RoomID:    "r1",
			CreatedAt: joinTime.Add(time.Hour),
			TShow:     true,
			QuotedParentMessage: &models.QuotedParentMessage{
				MessageID:             "p1",
				Msg:                   "shared parent",
				CreatedAt:             joinTime.Add(30 * time.Minute),
				ThreadParentID:        "p1",
				ThreadParentCreatedAt: ptrTime(joinTime.Add(-2 * time.Hour)), // before accessSince
			},
		}
	}
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{makeMsg("m1"), makeMsg("m2")}, false), nil)

	resp, err := svc.LoadHistory(c, models.LoadHistoryRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Messages, 2)
	assert.Equal(t, service.UnavailableQuoteMsg, resp.Messages[0].QuotedParentMessage.Msg)
	assert.Equal(t, service.UnavailableQuoteMsg, resp.Messages[1].QuotedParentMessage.Msg)
}

// The canonical EventDeleted must carry the message body so broadcast-worker can parse @-mentions for the thread-delete fan-out.
func TestHistoryService_DeleteMessage_EventDeletedCarriesContent(t *testing.T) {
	svc, msgs, subs, pub, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)

	hydrated := &models.Message{
		MessageID: "m-content",
		RoomID:    "r1",
		Sender:    models.Participant{Account: "u1", ID: "u1-id"},
		Deleted:   false,
		Msg:       "hey @dave check this out",
	}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "m-content").Return(hydrated, nil)

	deletedAt := time.Now().UTC()
	msgs.EXPECT().
		SoftDeleteMessage(gomock.Any(), hydrated, gomock.Any()).
		Return(deletedAt, true, (*int)(nil), (*time.Time)(nil), nil)

	pub.EXPECT().
		Publish(gomock.Any(), subject.MsgCanonicalDeleted("site-test"), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, data []byte, _ string) error {
			var evt model.MessageEvent
			require.NoError(t, json.Unmarshal(data, &evt))
			assert.Equal(t, model.EventDeleted, evt.Event)
			assert.Equal(t, "hey @dave check this out", evt.Message.Content,
				"EventDeleted must carry Content so broadcast-worker can parse @-mentions for thread-delete fan-out")
			return nil
		})

	expectEmptyPreviewWalk(msgs)

	resp, err := svc.DeleteMessage(c, "site-test", models.DeleteMessageRequest{MessageID: "m-content"})
	require.NoError(t, err)
	assert.Equal(t, "m-content", resp.MessageID)
}

// TShow message where ThreadParentCreatedAt is nil (message-worker didn't populate it) →
// conservatively redacted because the access window cannot be verified.
func TestHistoryService_TShow_ThreadParentCreatedAtNil_ConservativeRedaction(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)

	msg := models.Message{
		MessageID: "m1",
		RoomID:    "r1",
		CreatedAt: joinTime.Add(time.Hour),
		TShow:     true,
		QuotedParentMessage: &models.QuotedParentMessage{
			MessageID:             "p1",
			Msg:                   "parent text",
			CreatedAt:             joinTime.Add(30 * time.Minute), // within window
			ThreadParentID:        "p1",
			ThreadParentCreatedAt: nil, // not set by message-worker
		},
	}
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{msg}, false), nil)

	resp, err := svc.LoadHistory(c, models.LoadHistoryRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Messages, 1)
	// ThreadParentCreatedAt nil → conservative redaction applied.
	assert.Equal(t, service.UnavailableQuoteMsg, resp.Messages[0].QuotedParentMessage.Msg)
}

// --- GetMessagesByIDs ---

const maxGetByIDsBatchSize = 100

func TestHistoryService_GetMessagesByIDs_Success(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)

	m1 := models.Message{MessageID: "m1", RoomID: "r1", CreatedAt: joinTime.Add(1 * time.Minute)}
	m2 := models.Message{MessageID: "m2", RoomID: "r1", CreatedAt: joinTime.Add(2 * time.Minute)}
	m3 := models.Message{MessageID: "m3", RoomID: "r1", CreatedAt: joinTime.Add(3 * time.Minute)}
	msgs.EXPECT().GetMessagesByIDs(gomock.Any(), []string{"m1", "m2", "m3"}).Return([]models.Message{m1, m2, m3}, nil)

	result, err := svc.GetMessagesByIDs(c, models.GetMessagesByIDsRequest{MessageIDs: []string{"m1", "m2", "m3"}})
	require.NoError(t, err)
	require.Len(t, result.Messages, 3)
	assert.Equal(t, "m1", result.Messages[0].MessageID)
	assert.Equal(t, "m2", result.Messages[1].MessageID)
	assert.Equal(t, "m3", result.Messages[2].MessageID)
}

func TestHistoryService_GetMessagesByIDs_DecodesAttachments(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	blob, err := json.Marshal(model.Attachment{ID: "f1", Title: "a.png", Type: "file"})
	require.NoError(t, err)
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	fetched := []models.Message{{MessageID: "m1", RoomID: "r1", CreatedAt: joinTime.Add(time.Minute), Attachments: [][]byte{blob}}}
	msgs.EXPECT().GetMessagesByIDs(gomock.Any(), []string{"m1"}).Return(fetched, nil)

	resp, err := svc.GetMessagesByIDs(c, models.GetMessagesByIDsRequest{MessageIDs: []string{"m1"}})
	require.NoError(t, err)
	require.Len(t, resp.Messages, 1)
	require.Len(t, resp.Messages[0].DecodedAttachments, 1)
	assert.Equal(t, "f1", resp.Messages[0].DecodedAttachments[0].ID)
}

func TestHistoryService_GetMessagesByIDs_PartialResult_MissingIDsSilentlyOmitted(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)

	m1 := models.Message{MessageID: "m1", RoomID: "r1", CreatedAt: joinTime.Add(1 * time.Minute)}
	// m2 not found — store returns only [m1]
	msgs.EXPECT().GetMessagesByIDs(gomock.Any(), []string{"m1", "m2"}).Return([]models.Message{m1}, nil)

	result, err := svc.GetMessagesByIDs(c, models.GetMessagesByIDsRequest{MessageIDs: []string{"m1", "m2"}})
	require.NoError(t, err)
	require.Len(t, result.Messages, 1)
	assert.Equal(t, "m1", result.Messages[0].MessageID)
}

func TestHistoryService_GetMessagesByIDs_EmptyMessageIDs_BadRequest(t *testing.T) {
	svc, _, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)

	_, err := svc.GetMessagesByIDs(c, models.GetMessagesByIDsRequest{MessageIDs: []string{}})
	require.Error(t, err)
	assertBadRequestErr(t, err, "messageIds must not be empty")
}

func TestHistoryService_GetMessagesByIDs_OverCap_BadRequest(t *testing.T) {
	svc, _, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)

	ids := make([]string, maxGetByIDsBatchSize+1)
	for i := range ids {
		ids[i] = fmt.Sprintf("m%d", i)
	}
	_, err := svc.GetMessagesByIDs(c, models.GetMessagesByIDsRequest{MessageIDs: ids})
	require.Error(t, err)
	assertBadRequestErr(t, err, "too many messageIds")
}

func TestHistoryService_GetMessagesByIDs_DropsCrossRoomMessages(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)

	inRoom := models.Message{MessageID: "m1", RoomID: "r1", CreatedAt: joinTime.Add(1 * time.Minute)}
	crossRoom := models.Message{MessageID: "m2", RoomID: "r-other", CreatedAt: joinTime.Add(2 * time.Minute)}
	msgs.EXPECT().GetMessagesByIDs(gomock.Any(), []string{"m1", "m2"}).Return([]models.Message{inRoom, crossRoom}, nil)

	result, err := svc.GetMessagesByIDs(c, models.GetMessagesByIDsRequest{MessageIDs: []string{"m1", "m2"}})
	require.NoError(t, err)
	require.Len(t, result.Messages, 1)
	assert.Equal(t, "m1", result.Messages[0].MessageID)
}

func TestHistoryService_GetMessagesByIDs_AccessWindowFiltering(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)

	// m1 is before the access window; m2 is within it.
	m1 := models.Message{MessageID: "m1", RoomID: "r1", CreatedAt: joinTime.Add(-1 * time.Hour)}
	m2 := models.Message{MessageID: "m2", RoomID: "r1", CreatedAt: joinTime.Add(1 * time.Minute)}
	msgs.EXPECT().GetMessagesByIDs(gomock.Any(), []string{"m1", "m2"}).Return([]models.Message{m1, m2}, nil)

	result, err := svc.GetMessagesByIDs(c, models.GetMessagesByIDsRequest{MessageIDs: []string{"m1", "m2"}})
	require.NoError(t, err)
	// m1 silently omitted; only m2 returned.
	require.Len(t, result.Messages, 1)
	assert.Equal(t, "m2", result.Messages[0].MessageID)
}

func TestHistoryService_GetMessagesByIDs_StoreError_Internal(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessagesByIDs(gomock.Any(), []string{"m1"}).Return(nil, fmt.Errorf("cassandra unavailable"))

	_, err := svc.GetMessagesByIDs(c, models.GetMessagesByIDsRequest{MessageIDs: []string{"m1"}})
	require.Error(t, err)
	assertInternalErr(t, err, "fetching messages by IDs")
}

func TestHistoryService_GetMessagesByIDs_NotSubscribed_Forbidden(t *testing.T) {
	svc, _, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, false, nil)

	_, err := svc.GetMessagesByIDs(c, models.GetMessagesByIDsRequest{MessageIDs: []string{"m1"}})
	require.Error(t, err)
	assertForbiddenErr(t, err, "not subscribed to room")
}

func TestHistoryService_GetMessagesByIDs_QuoteRedaction(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)

	// m1 has a quoted message that falls before the access window — should be redacted.
	m1 := models.Message{
		MessageID: "m1",
		RoomID:    "r1",
		CreatedAt: joinTime.Add(1 * time.Minute),
		QuotedParentMessage: &models.QuotedParentMessage{
			MessageID: "q1",
			Msg:       "secret old message",
			CreatedAt: joinTime.Add(-1 * time.Hour), // before access window
		},
	}
	msgs.EXPECT().GetMessagesByIDs(gomock.Any(), []string{"m1"}).Return([]models.Message{m1}, nil)

	result, err := svc.GetMessagesByIDs(c, models.GetMessagesByIDsRequest{MessageIDs: []string{"m1"}})
	require.NoError(t, err)
	require.Len(t, result.Messages, 1)
	assert.Equal(t, service.UnavailableQuoteMsg, result.Messages[0].QuotedParentMessage.Msg)
}

// A legacy members_removed row must come back name-resolved through the real
// LoadHistory path, with one batched lookup.
func TestHistoryService_LoadHistory_ResolvesRemovedMemberNames(t *testing.T) {
	svc, msgs, subs, rooms, _, _, users, _ := newServiceWithRoomMock(t)
	c := testContext()

	// newServiceWithRoomMock leaves the read-floor read to the caller.
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)

	messages := []models.Message{{
		MessageID: "m1", RoomID: "r1", CreatedAt: joinTime.Add(time.Minute),
		Type: "members_removed", Msg: "bob has been removed from the channel.",
	}}
	msgs.EXPECT().
		GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).
		Return(makePage(messages, false), nil)
	users.EXPECT().
		FindUsersByAccounts(gomock.Any(), []string{"bob"}).
		Return([]model.User{{Account: "bob", EngName: "Bob", ChineseName: "鮑勃"}}, nil).
		Times(1)

	resp, err := svc.LoadHistory(c, models.LoadHistoryRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Messages, 1)
	assert.Equal(t, "Bob 鮑勃 has been removed from the channel.", resp.Messages[0].Msg)
}

// The ordinary page must not acquire a Mongo round trip: gomock fails the test
// if FindUsersByAccounts is called without an expectation.
func TestHistoryService_LoadHistory_OrdinaryPageIssuesNoUserLookup(t *testing.T) {
	svc, msgs, subs, rooms, _, _, _, _ := newServiceWithRoomMock(t)
	c := testContext()

	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)

	messages := []models.Message{{
		MessageID: "m1", RoomID: "r1", CreatedAt: joinTime.Add(time.Minute), Msg: "hello",
	}}
	msgs.EXPECT().
		GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).
		Return(makePage(messages, false), nil)

	resp, err := svc.LoadHistory(c, models.LoadHistoryRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Messages, 1)
	assert.Equal(t, "hello", resp.Messages[0].Msg)
}
