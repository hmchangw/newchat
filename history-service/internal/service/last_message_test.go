package service_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/history-service/internal/config"
	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/history-service/internal/service"
	"github.com/hmchangw/chat/history-service/internal/service/mocks"
	pkgmodel "github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
)

// newLastMsgService builds a service with NO permissive room-times pre-stub so
// tests can assert exact GetRoomTimes behaviour (including the not-found path).
func newLastMsgService(t *testing.T) (*service.HistoryService, *mocks.MockMessageRepository, *mocks.MockRoomRepository) {
	ctrl := gomock.NewController(t)
	msgs := mocks.NewMockMessageRepository(ctrl)
	subs := mocks.NewMockSubscriptionRepository(ctrl)
	rooms := mocks.NewMockRoomRepository(ctrl)
	pub := mocks.NewMockEventPublisher(ctrl)
	threadRooms := mocks.NewMockThreadRoomRepository(ctrl)
	threadSubs := mocks.NewMockThreadSubscriptionRepository(ctrl)
	users := mocks.NewMockUserStore(ctrl)
	apps := mocks.NewMockAppStore(ctrl)
	cfg := &config.Config{MessageHistoryFloorDays: 90, LargeRoomThreshold: 500, MaxPinnedPerRoom: 10, PinEnabled: true}
	return service.New(msgs, subs, rooms, pub, threadRooms, threadSubs, users, apps, cfg), msgs, rooms
}

func TestHistoryService_GetLastRoomMessage_Success(t *testing.T) {
	svc, msgs, rooms := newLastMsgService(t)

	// Anchor times relative to real now so the historyFloor clamp never kicks in
	// and the repo bounds can be asserted exactly.
	lastMsgAt := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	createdAt := time.Now().UTC().Add(-10 * 24 * time.Hour).Truncate(time.Millisecond)
	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(lastMsgAt, createdAt, nil)

	msgCreatedAt := lastMsgAt
	editedAt := lastMsgAt.Add(-time.Minute)
	row := &models.Message{
		MessageID:   "m1",
		RoomID:      "r1",
		Sender:      models.Participant{ID: "u1", Account: "alice", EngName: "Alice Smith"},
		Msg:         "hello world",
		CreatedAt:   msgCreatedAt,
		EditedAt:    &editedAt,
		Attachments: cassandra.EncodeAttachments([]cassandra.Attachment{{ID: "f1", Title: "a.png", Type: "file"}, {ID: "f2", Title: "b.pdf", Type: "file"}}),
	}
	// before = lastMsgAt+1ms (so the newest row survives the strict < bound);
	// floor = createdAt (inside the 90-day history floor).
	msgs.EXPECT().
		GetLastRoomMessage(gomock.Any(), "r1", lastMsgAt.Add(time.Millisecond), createdAt).
		Return(nil, row, nil)

	resp, err := svc.GetLastRoomMessage(testContext(), pkgmodel.LastRoomMessageRequest{RoomID: "r1"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.LastMessage)

	preview := resp.LastMessage
	assert.Equal(t, "m1", preview.MessageID)
	assert.Empty(t, preview.Type, "ordinary user messages carry no type")
	assert.Equal(t, "alice", preview.SenderAccount)
	assert.Equal(t, "Alice Smith", preview.SenderName)
	assert.Equal(t, "hello world", preview.Msg)
	assert.Equal(t, msgCreatedAt, preview.CreatedAt)
	require.NotNil(t, preview.EditedAt)
	assert.Equal(t, editedAt, *preview.EditedAt)
	assert.Equal(t, 2, preview.AttachmentCount)
}

func TestHistoryService_GetLastRoomMessage_SenderNameFallbacks(t *testing.T) {
	lastMsgAt := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	createdAt := time.Now().UTC().Add(-10 * 24 * time.Hour).Truncate(time.Millisecond)

	tests := []struct {
		name     string
		sender   models.Participant
		wantName string
	}{
		{name: "eng name wins", sender: models.Participant{Account: "alice", EngName: "Alice", AppName: "Bot"}, wantName: "Alice"},
		{name: "bot falls back to app name", sender: models.Participant{Account: "bot-1", AppName: "Reminder Bot", IsBot: true}, wantName: "Reminder Bot"},
		{name: "account is the last resort", sender: models.Participant{Account: "bob"}, wantName: "bob"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svc, msgs, rooms := newLastMsgService(t)
			rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(lastMsgAt, createdAt, nil)
			msgs.EXPECT().
				GetLastRoomMessage(gomock.Any(), "r1", gomock.Any(), gomock.Any()).
				Return(nil, &models.Message{MessageID: "m1", RoomID: "r1", Sender: tc.sender, CreatedAt: lastMsgAt}, nil)

			resp, err := svc.GetLastRoomMessage(testContext(), pkgmodel.LastRoomMessageRequest{RoomID: "r1"})
			require.NoError(t, err)
			require.NotNil(t, resp.LastMessage)
			assert.Equal(t, tc.wantName, resp.LastMessage.SenderName)
			assert.Equal(t, tc.sender.Account, resp.LastMessage.SenderAccount)
		})
	}
}

func TestHistoryService_GetLastRoomMessage_NoQualifyingMessage(t *testing.T) {
	svc, msgs, rooms := newLastMsgService(t)

	lastMsgAt := time.Now().UTC().Add(-time.Hour)
	createdAt := time.Now().UTC().Add(-10 * 24 * time.Hour)
	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(lastMsgAt, createdAt, nil)
	msgs.EXPECT().
		GetLastRoomMessage(gomock.Any(), "r1", gomock.Any(), gomock.Any()).
		Return(nil, nil, nil)

	resp, err := svc.GetLastRoomMessage(testContext(), pkgmodel.LastRoomMessageRequest{RoomID: "r1"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Nil(t, resp.LastMessage, "all-deleted / all-system room yields a nil preview, not an error")
}

func TestHistoryService_GetLastRoomMessage_RepoError(t *testing.T) {
	svc, msgs, rooms := newLastMsgService(t)

	lastMsgAt := time.Now().UTC().Add(-time.Hour)
	createdAt := time.Now().UTC().Add(-10 * 24 * time.Hour)
	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(lastMsgAt, createdAt, nil)
	msgs.EXPECT().
		GetLastRoomMessage(gomock.Any(), "r1", gomock.Any(), gomock.Any()).
		Return(nil, nil, fmt.Errorf("cassandra timeout"))

	resp, err := svc.GetLastRoomMessage(testContext(), pkgmodel.LastRoomMessageRequest{RoomID: "r1"})
	assert.Nil(t, resp)
	assertInternalErr(t, err, "loading last room message")
}

func TestHistoryService_GetLastRoomMessage_EmptyRoomID(t *testing.T) {
	svc, _, _ := newLastMsgService(t)

	resp, err := svc.GetLastRoomMessage(testContext(), pkgmodel.LastRoomMessageRequest{})
	assert.Nil(t, resp)
	assertBadRequestErr(t, err, "roomId is required")
}

// A room unknown to Mongo answers "no last message" rather than an error: the
// RPC is server-to-server and callers treat a missing room like an empty one.
func TestHistoryService_GetLastRoomMessage_RoomNotFound(t *testing.T) {
	svc, msgs, rooms := newLastMsgService(t)

	rooms.EXPECT().
		GetRoomTimes(gomock.Any(), "r-missing").
		Return(time.Time{}, time.Time{}, fmt.Errorf("get room times for r-missing: %w", mongo.ErrNoDocuments))
	// Critically no GetLastRoomMessage expectation — the Cassandra walk must be skipped.

	resp, err := svc.GetLastRoomMessage(testContext(), pkgmodel.LastRoomMessageRequest{RoomID: "r-missing"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Nil(t, resp.LastMessage)

	_ = msgs // absence of expectations asserts the repo is never touched
}

// Preview bodies are room-list snippets: the service edge trims to the shared rune
// cap so full bodies never ride the RPC into Mongo and every delete event.
func TestHistoryService_GetLastRoomMessage_TrimsPreviewContent(t *testing.T) {
	svc, msgs, rooms := newLastMsgService(t)

	lastMsgAt := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	createdAt := time.Now().UTC().Add(-10 * 24 * time.Hour).Truncate(time.Millisecond)
	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(lastMsgAt, createdAt, nil)

	long := strings.Repeat("測", pkgmodel.LastMessagePreviewMaxRunes+44)
	msgs.EXPECT().
		GetLastRoomMessage(gomock.Any(), "r1", gomock.Any(), gomock.Any()).
		Return(nil, &models.Message{
			MessageID: "m1", RoomID: "r1",
			Sender:    models.Participant{Account: "alice", EngName: "Alice"},
			Msg:       long,
			CreatedAt: lastMsgAt,
		}, nil)

	resp, err := svc.GetLastRoomMessage(testContext(), pkgmodel.LastRoomMessageRequest{RoomID: "r1"})
	require.NoError(t, err)
	require.NotNil(t, resp.LastMessage)
	got := resp.LastMessage.Msg
	assert.Equal(t, pkgmodel.LastMessagePreviewMaxRunes, len([]rune(got)), "preview trimmed to the rune cap")
	assert.Equal(t, strings.Repeat("測", pkgmodel.LastMessagePreviewMaxRunes), got, "trim must cut on rune boundaries")
}

// Denormalized lastMsgAt can lag coalesced creates — a caller-supplied Before must
// widen the walk ceiling so buffered-but-unflushed survivors stay in the window.
func TestHistoryService_GetLastRoomMessage_BeforeWidensStaleCeiling(t *testing.T) {
	svc, msgs, rooms := newLastMsgService(t)

	staleLastMsgAt := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	createdAt := time.Now().UTC().Add(-10 * 24 * time.Hour).Truncate(time.Millisecond)
	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(staleLastMsgAt, createdAt, nil)

	// Delete-event time 30m after the stale pointer: the walk must start there.
	deleteAt := staleLastMsgAt.Add(30 * time.Minute)
	msgs.EXPECT().
		GetLastRoomMessage(gomock.Any(), "r1", deleteAt.Add(time.Millisecond), createdAt).
		Return(nil, nil, nil)

	resp, err := svc.GetLastRoomMessage(testContext(), pkgmodel.LastRoomMessageRequest{RoomID: "r1", Before: deleteAt.UnixMilli()})
	require.NoError(t, err)
	require.NotNil(t, resp)
}

// A bogus far-future Before must be clamped to now+skew so it can't push the walk ceiling
// out and force maxBuckets of empty future-bucket reads.
func TestHistoryService_GetLastRoomMessage_FarFutureBeforeClamped(t *testing.T) {
	svc, msgs, rooms := newLastMsgService(t)

	lastMsgAt := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	createdAt := time.Now().UTC().Add(-10 * 24 * time.Hour).Truncate(time.Millisecond)
	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(lastMsgAt, createdAt, nil)

	farFuture := time.Now().UTC().Add(100 * 365 * 24 * time.Hour)
	var gotBefore time.Time
	msgs.EXPECT().
		GetLastRoomMessage(gomock.Any(), "r1", gomock.Any(), createdAt).
		DoAndReturn(func(_ context.Context, _ string, before, _ time.Time) (*pkgmodel.LastMessagePointer, *models.Message, error) {
			gotBefore = before
			return nil, nil, nil
		})

	_, err := svc.GetLastRoomMessage(testContext(), pkgmodel.LastRoomMessageRequest{RoomID: "r1", Before: farFuture.UnixMilli()})
	require.NoError(t, err)
	// Clamped to ~now+skew (1h) + 1ms; assert it's nowhere near the far-future value but still
	// widened past the stale stored lastMsgAt. Generous window to avoid now()-drift flakiness.
	assert.True(t, gotBefore.Before(time.Now().UTC().Add(2*time.Hour)),
		"far-future Before must be clamped to ~now+skew, got %s", gotBefore)
	assert.True(t, gotBefore.After(lastMsgAt), "clamped ceiling must still exceed the stale lastMsgAt")
}

// An OLDER Before must never shrink the ceiling below the stored lastMsgAt.
func TestHistoryService_GetLastRoomMessage_OlderBeforeDoesNotShrinkCeiling(t *testing.T) {
	svc, msgs, rooms := newLastMsgService(t)

	lastMsgAt := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	createdAt := time.Now().UTC().Add(-10 * 24 * time.Hour).Truncate(time.Millisecond)
	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(lastMsgAt, createdAt, nil)

	msgs.EXPECT().
		GetLastRoomMessage(gomock.Any(), "r1", lastMsgAt.Add(time.Millisecond), createdAt).
		Return(nil, nil, nil)

	resp, err := svc.GetLastRoomMessage(testContext(), pkgmodel.LastRoomMessageRequest{
		RoomID: "r1", Before: lastMsgAt.Add(-2 * time.Hour).UnixMilli(),
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
}

// The repo's pointer rides the response so the caller can rewind lastMsgId/lastMsgAt
// to a system notice while the preview shows the newest user message.
func TestHistoryService_GetLastRoomMessage_PointerRidesResponse(t *testing.T) {
	svc, msgs, rooms := newLastMsgService(t)

	lastMsgAt := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	createdAt := time.Now().UTC().Add(-10 * 24 * time.Hour).Truncate(time.Millisecond)
	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(lastMsgAt, createdAt, nil)

	ptr := &pkgmodel.LastMessagePointer{MessageID: "m-sys", CreatedAt: lastMsgAt}
	msgs.EXPECT().
		GetLastRoomMessage(gomock.Any(), "r1", gomock.Any(), gomock.Any()).
		Return(ptr, &models.Message{
			MessageID: "m-user", RoomID: "r1",
			Sender:    models.Participant{Account: "alice", EngName: "Alice"},
			Msg:       "hello",
			CreatedAt: lastMsgAt.Add(-time.Minute),
		}, nil)

	resp, err := svc.GetLastRoomMessage(testContext(), pkgmodel.LastRoomMessageRequest{RoomID: "r1"})
	require.NoError(t, err)
	require.NotNil(t, resp.Pointer)
	assert.Equal(t, "m-sys", resp.Pointer.MessageID)
	require.NotNil(t, resp.LastMessage)
	assert.Equal(t, "m-user", resp.LastMessage.MessageID)
}

// Pointer without preview: only system messages survive — the response says
// so instead of pretending the room is empty.
func TestHistoryService_GetLastRoomMessage_PointerOnlyWhenOnlySystemSurvives(t *testing.T) {
	svc, msgs, rooms := newLastMsgService(t)

	lastMsgAt := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	createdAt := time.Now().UTC().Add(-10 * 24 * time.Hour).Truncate(time.Millisecond)
	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(lastMsgAt, createdAt, nil)

	ptr := &pkgmodel.LastMessagePointer{MessageID: "m-sys", CreatedAt: lastMsgAt}
	msgs.EXPECT().
		GetLastRoomMessage(gomock.Any(), "r1", gomock.Any(), gomock.Any()).
		Return(ptr, nil, nil)

	resp, err := svc.GetLastRoomMessage(testContext(), pkgmodel.LastRoomMessageRequest{RoomID: "r1"})
	require.NoError(t, err)
	require.NotNil(t, resp.Pointer)
	assert.Nil(t, resp.LastMessage)
}
