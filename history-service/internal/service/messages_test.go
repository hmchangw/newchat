package service_test

import (
	"context"
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

// defaultRoomLastMsgAt and defaultRoomCreatedAt are the sensible defaults
// newService uses for GetRoomTimes so existing tests that don't supply meta
// don't get their fixtures clipped by the bucket-walk floor/ceiling.
var defaultRoomLastMsgAt = joinTime.Add(24 * time.Hour)
var defaultRoomCreatedAt = joinTime.Add(-30 * 24 * time.Hour)

func newService(t *testing.T) (*service.HistoryService, *mocks.MockMessageRepository, *mocks.MockSubscriptionRepository, *mocks.MockEventPublisher, *mocks.MockThreadRoomRepository) {
	svc, msgs, subs, rooms, pub, threadRooms, _, _ := newServiceWithRoomMock(t)
	// Permissive defaults: existing tests don't care about the room reads.
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	rooms.EXPECT().
		GetRoomTimes(gomock.Any(), gomock.Any()).
		Return(defaultRoomLastMsgAt, defaultRoomCreatedAt, nil).
		MinTimes(0)
	return svc, msgs, subs, pub, threadRooms
}

// newServiceWithRoomMock returns the same fixtures plus the room mock so a test
// can set its own GetMinUserLastSeenAt expectations. The mock IS pre-populated
// with a permissive GetRoomTimes default — every handler invokes the bucket-
// walk resolver, and almost no test cares about its return. Tests asserting
// resolver behaviour should override with a stricter Times(N). UserStore and
// CustomEmojiStore mocks are returned without pre-stubs; only reaction tests
// exercise them.
func newServiceWithRoomMock(t *testing.T) (*service.HistoryService, *mocks.MockMessageRepository, *mocks.MockSubscriptionRepository, *mocks.MockRoomRepository, *mocks.MockEventPublisher, *mocks.MockThreadRoomRepository, *mocks.MockUserStore, *mocks.MockCustomEmojiStore) {
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
	// Permissive default: only the large-room override path reads userCount.
	rooms.EXPECT().GetRoomUserCount(gomock.Any(), gomock.Any()).Return(0, nil).AnyTimes()
	// Note: no AnyTimes default for GetPinnedMessages — pin tests that reach the
	// pin-limit check (PinMessage success paths) set their own expectation. An
	// AnyTimes default here would shadow the explicit expectations set by
	// TestListPinnedMessages_* and break them.
	// MessageHistoryFloorDays=90: long enough that the floor never clips test fixtures.
	// PinEnabled=true: kill-switch on by default; TestPinMessage_KillSwitchDisabled
	// builds its own service with false.
	cfg := &config.Config{
		MessageHistoryFloorDays: 90,
		LargeRoomThreshold:      500,
		MaxPinnedPerRoom:        10,
		PinEnabled:              true,
	}
	return service.New(msgs, subs, rooms, pub, threadRooms, users, customEmojis, cfg), msgs, subs, rooms, pub, threadRooms, users, customEmojis
}

// assertInternalErr verifies err collapses to the internal category. Internal
// failures are now propagated as raw wrapped errors (fmt.Errorf("...: %w", err))
// that errcode.Classify turns into a generic "internal error" envelope at the
// transport boundary, so the test classifies the error the same way. wantCause
// is asserted against the (server-only) wrapped chain, never the client message.
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

func makePage(msgs []models.Message, hasNext bool) cassrepo.Page[models.Message] {
	nextCursor := ""
	if hasNext {
		nextCursor = "fake-next-cursor"
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

func TestHistoryService_LoadNextMessages_DoesNotReadRoom(t *testing.T) {
	svc, msgs, subs, rooms, _, _, _, _ := newServiceWithRoomMock(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessagesAfter(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).Return(makePage(nil, false), nil)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Times(0)

	_, err := svc.LoadNextMessages(c, models.LoadNextMessagesRequest{})
	require.NoError(t, err)
}

func TestHistoryService_LoadSurroundingMessages_DoesNotReadRoom(t *testing.T) {
	svc, msgs, subs, rooms, _, _, _, _ := newServiceWithRoomMock(t)
	c := testContext()

	central := models.Message{MessageID: "mC", RoomID: "r1", CreatedAt: joinTime.Add(2 * time.Minute)}
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessageByID(gomock.Any(), "mC").Return(&central, nil)
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, central.CreatedAt, gomock.Any()).Return(makePage(nil, false), nil)
	msgs.EXPECT().GetMessagesAfter(gomock.Any(), "r1", central.CreatedAt, gomock.Any(), gomock.Any()).Return(makePage(nil, false), nil)
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Times(0)

	_, err := svc.LoadSurroundingMessages(c, models.LoadSurroundingMessagesRequest{MessageID: "mC", Limit: 10})
	require.NoError(t, err)
}

// --- LoadNextMessages ---

func TestHistoryService_LoadNextMessages_BothAfterAndHSS(t *testing.T) {
	svc, msgs, subs, _, _ := newService(t)
	c := testContext()

	// Both after and HSS present — effective lower bound = max(after, HSS)
	// after (joinTime+1min) > HSS (joinTime), so effective = joinTime+1min
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
	svc, _, subs, _, _ := newService(t)
	c := testContext()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)

	_, err := svc.LoadSurroundingMessages(c, models.LoadSurroundingMessagesRequest{Limit: 6})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "messageId is required")
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

	// A soft-deleted message should be invisible to the edit path. Returning
	// ErrNotFound (not ErrForbidden) keeps the leak surface symmetric with the
	// WrongRoom case and prevents an impossible delete -> edit event sequence.
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

	// No publish should happen when the UPDATE fails. The mock publisher is
	// not configured to expect any call; gomock will fail the test if Publish
	// is invoked.

	resp, err := svc.EditMessage(c, "site-test", models.EditMessageRequest{MessageID: "m-abc", NewMsg: "new content"})
	assert.Nil(t, resp)
	assertInternalErr(t, err, "editing message")
}

// TestHistoryService_EditMessage_RaceWithDelete_MapsToNotFound verifies the
// TOCTOU between findMessage and the LWT-gated UpdateMessageContent doesn't
// surface as a 5xx. When a concurrent SoftDelete or hard-delete lands
// between findMessage's read and the CAS edit, the repo returns
// cassrepo.ErrMessageNotFound; the handler must map it to ErrNotFound so
// it doesn't pollute 5xx telemetry — it's a benign race, not a server
// fault.
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

	resp, err := svc.EditMessage(c, "site-test", models.EditMessageRequest{
		MessageID: "msg-1",
		NewMsg:    "updated content",
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
}

// TestHistoryService_EditMessage_PublishFailureDoesNotFailRPC verifies the
// canonical publish is best-effort — a publish failure must not roll back the
// user-visible edit (Cassandra is the source of truth).
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

	resp, err := svc.EditMessage(c, "site-test", models.EditMessageRequest{
		MessageID: "msg-1",
		NewMsg:    "updated content",
	})
	require.NoError(t, err, "publish failure must not fail the RPC")
	require.NotNil(t, resp)
}

// Nats-Msg-Id shape "{messageID}:updated:{editedAtMs}": the op suffix avoids
// collision with gatekeeper's `.created` key (bare messageID); editedAtMs
// gives each distinct edit its own key.
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

	// No SoftDeleteMessage call expected. No Publish call expected. gomock will
	// fail the test if either is invoked unexpectedly.

	resp, err := svc.DeleteMessage(c, "site-test", models.DeleteMessageRequest{MessageID: "m-abc"})
	require.NoError(t, err)
	assert.Equal(t, "m-abc", resp.MessageID)
	assert.Equal(t, priorUpdatedAt.UnixMilli(), resp.DeletedAt, "short-circuit should echo the existing updated_at")
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
		Return(time.Time{}, false, fmt.Errorf("cassandra timeout"))

	// No Publish expected when the UPDATE fails.

	resp, err := svc.DeleteMessage(c, "site-test", models.DeleteMessageRequest{MessageID: "m-abc"})
	assert.Nil(t, resp)
	assertInternalErr(t, err, "deleting message")
}

// TestHistoryService_DeleteMessage_ConcurrentDeleteSkipsPublish covers the
// race case where two clients delete the same message simultaneously: hydrate
// sees deleted=false (so the handler-level short-circuit doesn't fire), but
// the repo's LWT returns applied=false because a parallel goroutine already
// flipped the row. The handler must NOT publish a duplicate message_deleted
// event and must return the timestamp the winning goroutine wrote.
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
		Return(winnerWrote, false, nil)

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
		DoAndReturn(func(_ context.Context, _ *models.Message, deletedAt time.Time) (time.Time, bool, error) {
			return deletedAt, true, nil
		})

	pub.EXPECT().
		Publish(gomock.Any(), subject.MsgCanonicalDeleted("site-test"), gomock.Any(), gomock.Any()).
		Return(fmt.Errorf("nats disconnected"))

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
		DoAndReturn(func(_ context.Context, _ *models.Message, deletedAt time.Time) (time.Time, bool, error) {
			return deletedAt, true, nil
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

	resp, err := svc.DeleteMessage(c, "site-test", models.DeleteMessageRequest{MessageID: "msg-1"})
	require.NoError(t, err)
	require.NotNil(t, resp)
}

// Nats-Msg-Id shape "{messageID}:deleted": distinct from the `.created` key
// so the JetStream dedup window doesn't collapse a delete against an earlier
// create.
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
		DoAndReturn(func(_ context.Context, _ *models.Message, deletedAt time.Time) (time.Time, bool, error) {
			return deletedAt, true, nil
		})

	pub.EXPECT().
		Publish(gomock.Any(), subject.MsgCanonicalDeleted("site-test"), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, data []byte, msgID string) error {
			var evt model.MessageEvent
			require.NoError(t, json.Unmarshal(data, &evt))
			assert.Equal(t, natsutil.CanonicalDedupID(&evt), msgID)
			return nil
		})

	_, err := svc.DeleteMessage(c, "site-test", models.DeleteMessageRequest{MessageID: "msg-1"})
	require.NoError(t, err)
}

// ============================================================
// Quote redaction
// ============================================================

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

// ============================================================
// TShow redaction
// ============================================================

// TShow message whose QuotedParentMessage.ThreadParentCreatedAt pre-dates accessSince →
// snapshot replaced with unavailable stub. ThreadParentCreatedAt is embedded at write
// time by message-worker; no Cassandra fetch needed.
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
