package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/history-service/internal/cassrepo"
	"github.com/hmchangw/chat/history-service/internal/config"
	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/history-service/internal/mongorepo"
	readcache "github.com/hmchangw/chat/history-service/internal/readcache"
	"github.com/hmchangw/chat/history-service/internal/service"
	"github.com/hmchangw/chat/history-service/internal/service/mocks"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/preview"
)

// newRoomsService builds a service with bare mocks. rooms.get is server-to-server
// now — no access (subscription) check — so tests only set room-time + message reads.
// A permissive default on SetPreviewMessage lets every pre-existing fixture ignore
// the warm-back call it now triggers on a successful walk; tests that care about
// warm-back specifically (rooms_warmback_test.go) build their own mocks instead, so
// their precise EXPECT()s aren't shadowed by this default (gomock matches the first
// registered expectation, so a shared AnyTimes() here must never be the only one a
// warm-back-specific test relies on).
func newRoomsService(t *testing.T) (*service.HistoryService, *mocks.MockMessageRepository, *mocks.MockRoomRepository) {
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
	rooms.EXPECT().SetPreviewMessage(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	svc := service.New(msgs, subs, rooms, pub, threadRooms, threadSubs, users, apps, cfg)
	return svc, msgs, rooms
}

// newRoomsServiceWithApps also exposes the AppStore mock, needed by the preview
// enrichment tests (bot sender → app name). See newRoomsService for the default
// SetPreviewMessage stub rationale.
func newRoomsServiceWithApps(t *testing.T) (*service.HistoryService, *mocks.MockMessageRepository, *mocks.MockRoomRepository, *mocks.MockAppStore) {
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
	rooms.EXPECT().SetPreviewMessage(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	svc := service.New(msgs, subs, rooms, pub, threadRooms, threadSubs, users, apps, cfg)
	return svc, msgs, rooms, apps
}

// newRoomsServiceWithPreviewCache installs a real PreviewCache so RoomsGet routes
// through it (cache-hit behavior on the second/third read of the same room). See
// newRoomsService for the default SetPreviewMessage stub rationale.
func newRoomsServiceWithPreviewCache(t *testing.T) (*service.HistoryService, *mocks.MockMessageRepository, *mocks.MockRoomRepository) {
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
	pc, err := readcache.NewPreviewCache(100, time.Minute)
	require.NoError(t, err)
	rooms.EXPECT().SetPreviewMessage(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	svc := service.New(msgs, subs, rooms, pub, threadRooms, threadSubs, users, apps, cfg, service.WithPreviewCache(pc))
	return svc, msgs, rooms
}

func roomsCtx() *natsrouter.Context {
	return natsrouter.NewContext(map[string]string{"account": "alice"})
}

var roomLastMsgAt = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
var roomCreatedAt = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// Mirror the production caps (house pattern — see maxGetByIDsBatchSize in messages_test).
const roomsGetMaxBatch = 100

// stubRoomTimes arranges the single unhinted-batch read RoomsGet issues for ids (no Hints
// in the request), returning last/created for every id — the common happy-path room times
// most fixtures below share. gomock.Any() on the ids arg since most tests don't care about
// exactly which ids were batched (TestHistoryService_RoomsGet_NoHints_BatchesAllIDs asserts
// that explicitly).
func stubRoomTimes(rooms *mocks.MockRoomRepository, ids []string, last, created time.Time) {
	stubRoomTimesN(rooms, ids, last, created, 1)
}

// stubRoomTimesN is stubRoomTimes with an explicit call count, for tests (e.g. the preview-cache
// ones) that invoke RoomsGet more than once for the same unhinted room set.
func stubRoomTimesN(rooms *mocks.MockRoomRepository, ids []string, last, created time.Time, times int) {
	out := make(map[string]mongorepo.RoomTimes, len(ids))
	for _, id := range ids {
		out[id] = mongorepo.RoomTimes{LastMsgAt: last, CreatedAt: created}
	}
	rooms.EXPECT().GetRoomTimesByIDs(gomock.Any(), gomock.Any()).Return(out, nil).Times(times)
}

func TestHistoryService_RoomsGet_PreviewCacheHitSkipsRead(t *testing.T) {
	svc, msgs, rooms := newRoomsServiceWithPreviewCache(t)

	// The unhinted-batch Mongo read runs once per RoomsGet call regardless of the preview
	// cache (it's resolved up front, before the per-room cache check) — Times(3) over the 3
	// calls below. The expensive Cassandra preview walk is what the cache actually guards,
	// so GetMessagesBefore staying at Times(1) is what proves the 2nd/3rd calls were cache hits.
	stubRoomTimesN(rooms, []string{"r1"}, roomLastMsgAt, roomCreatedAt, 3)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{
			{MessageID: "m1", RoomID: "r1", Msg: "hi", CreatedAt: roomLastMsgAt, Sender: models.Participant{Account: "alice"}},
		}, false), nil).Times(1)

	for i := 0; i < 3; i++ {
		resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
		require.NoError(t, err)
		require.Equal(t, "m1", resp.Rooms["r1"].MessageID)
	}
}

func TestHistoryService_RoomsGet_LatestMessage(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)

	stubRoomTimes(rooms, []string{"r1"}, roomLastMsgAt, roomCreatedAt)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{{MessageID: "m1", RoomID: "r1", Msg: "hello", CreatedAt: roomLastMsgAt, Sender: models.Participant{ID: "u1", Account: "alice"}}}, false), nil)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	require.Contains(t, resp.Rooms, "r1")
	lm := resp.Rooms["r1"]
	assert.Equal(t, "m1", lm.MessageID)
	assert.Equal(t, "hello", lm.Content)
	assert.Equal(t, "alice", lm.Sender.Account)
	assert.Equal(t, roomLastMsgAt.UTC(), lm.CreatedAt)
}

func TestHistoryService_RoomsGet_EligibleNewest_SingleQuery(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)

	var sizes []int
	stubRoomTimes(rooms, []string{"r1"}, roomLastMsgAt, roomCreatedAt)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, _ time.Time, _ time.Time, pr cassrepo.PageRequest) (cassrepo.Page[models.Message], error) {
			sizes = append(sizes, pr.PageSize)
			return makePage([]models.Message{
				{MessageID: "m1", RoomID: "r1", Msg: "hi", CreatedAt: roomLastMsgAt, Sender: models.Participant{Account: "alice"}},
			}, false), nil
		}).Times(1)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	require.Contains(t, resp.Rooms, "r1")
	assert.Equal(t, "m1", resp.Rooms["r1"].MessageID)
	assert.Equal(t, []int{1}, sizes, "first (and only) walk page must be size 1")
}

func TestHistoryService_RoomsGet_EscalatesPastIneligibleTail(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)

	var sizes []int
	stubRoomTimes(rooms, []string{"r1"}, roomLastMsgAt, roomCreatedAt)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, _ time.Time, _ time.Time, pr cassrepo.PageRequest) (cassrepo.Page[models.Message], error) {
			sizes = append(sizes, pr.PageSize)
			if pr.PageSize == 1 {
				// Newest is deleted; more remains (HasNext=true) → escalate.
				return makePage([]models.Message{
					{MessageID: "d1", RoomID: "r1", Deleted: true, CreatedAt: roomLastMsgAt},
				}, true), nil
			}
			// Next (larger) page reaches the eligible survivor.
			return makePage([]models.Message{
				{MessageID: "m1", RoomID: "r1", Msg: "alive", CreatedAt: roomLastMsgAt.Add(-time.Minute), Sender: models.Participant{Account: "alice"}},
			}, false), nil
		}).Times(2)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	require.Contains(t, resp.Rooms, "r1")
	assert.Equal(t, "m1", resp.Rooms["r1"].MessageID)
	assert.Equal(t, []int{1, 8}, sizes, "page size grows 1 → 8 on an ineligible first page")
}

// The scan budget (250) is exhausted by an unbroken run of deleted messages,
// each returned page full (len == PageSize) with HasNext=true, so the walk
// never sees a terminal signal from the store — it must stop on the budget
// itself, not loop forever. Also verifies the 100-row escalation clamp: the
// 4th page is capped at 100 (not 512), and the 5th page is the remaining 77
// to close out the 250 budget.
func TestHistoryService_RoomsGet_ScanBudgetExhausted_NoEligibleMessage(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)

	var sizes []int
	stubRoomTimes(rooms, []string{"r1"}, roomLastMsgAt, roomCreatedAt)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, _ time.Time, _ time.Time, pr cassrepo.PageRequest) (cassrepo.Page[models.Message], error) {
			sizes = append(sizes, pr.PageSize)
			deleted := make([]models.Message, pr.PageSize)
			for i := range deleted {
				deleted[i] = models.Message{
					MessageID: "d", RoomID: "r1", Deleted: true,
					CreatedAt: roomLastMsgAt.Add(-time.Duration(i) * time.Second),
				}
			}
			return makePage(deleted, true), nil
		}).Times(5)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	assert.NotContains(t, resp.Rooms, "r1")
	assert.Equal(t, []int{1, 8, 64, 100, 77}, sizes, "escalation clamps at 100 then the remaining budget, terminating at the 250-row scan cap")
}

func TestHistoryService_RoomsGet_EmptyRoomOmitted(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)

	// The batched read returns a zero lastMsgAt (room never messaged); sanitizeLastMsgAt
	// rejects the zero value the same way it would reject any implausible hint, so
	// resolveRoomTimes falls back to the per-room GetRoomTimes read for this one room.
	stubRoomTimes(rooms, []string{"r1"}, time.Time{}, roomCreatedAt)
	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(time.Time{}, roomCreatedAt, nil)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage(nil, false), nil)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	assert.NotContains(t, resp.Rooms, "r1")
	assert.NotNil(t, resp.Rooms)
}

func TestHistoryService_RoomsGet_PerRoomDegradeKeepsSiblings(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)

	// r1 history read errors → omitted; r2 succeeds → returned. Both are unhinted, so
	// they share the one batched GetRoomTimesByIDs([r1, r2]) call.
	stubRoomTimes(rooms, []string{"r1", "r2"}, roomLastMsgAt, roomCreatedAt)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage(nil, false), errors.New("cassandra timeout"))
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r2", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{{MessageID: "m2", RoomID: "r2", Msg: "ok", CreatedAt: roomLastMsgAt}}, false), nil)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1", "r2"}})
	require.NoError(t, err)
	assert.NotContains(t, resp.Rooms, "r1")
	require.Contains(t, resp.Rooms, "r2")
}

// Latest message deleted → walk back within the page and return the first survivor.
func TestHistoryService_RoomsGet_SkipsDeletedTail(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)

	stubRoomTimes(rooms, []string{"r1"}, roomLastMsgAt, roomCreatedAt)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{
			{MessageID: "m3", RoomID: "r1", Msg: "", Deleted: true, CreatedAt: roomLastMsgAt},
			{MessageID: "m2", RoomID: "r1", Msg: "", Deleted: true, CreatedAt: roomLastMsgAt.Add(-time.Minute)},
			{MessageID: "m1", RoomID: "r1", Msg: "alive", CreatedAt: roomLastMsgAt.Add(-2 * time.Minute), Sender: models.Participant{Account: "alice"}},
		}, false), nil)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	require.Contains(t, resp.Rooms, "r1")
	assert.Equal(t, "m1", resp.Rooms["r1"].MessageID)
	assert.Equal(t, "alive", resp.Rooms["r1"].Content)
}

// Every message in the page is deleted (and the page is the whole room) → no entry.
func TestHistoryService_RoomsGet_AllDeletedOmitted(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)

	stubRoomTimes(rooms, []string{"r1"}, roomLastMsgAt, roomCreatedAt)
	// A short all-deleted page (below the walk page size) means no older messages
	// remain, so a single read is enough to conclude "no last message".
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{
			{MessageID: "m2", RoomID: "r1", Msg: "", Deleted: true, CreatedAt: roomLastMsgAt},
			{MessageID: "m1", RoomID: "r1", Msg: "", Deleted: true, CreatedAt: roomLastMsgAt.Add(-time.Minute)},
		}, false), nil).Times(1)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	assert.NotContains(t, resp.Rooms, "r1")
}

func TestHistoryService_RoomsGet_DedupsRoomIDs(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)

	// A duplicate roomId resolves once (Times(1) on each per-room read).
	stubRoomTimes(rooms, []string{"r1"}, roomLastMsgAt, roomCreatedAt)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{{MessageID: "m1", RoomID: "r1", Msg: "x", CreatedAt: roomLastMsgAt}}, false), nil).Times(1)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1", "r1"}})
	require.NoError(t, err)
	assert.Len(t, resp.Rooms, 1)
}

// Content beyond the preview cap is truncated to a rune-safe snippet — multi-byte
// runes must not be split mid-character.
func TestHistoryService_RoomsGet_ContentTruncatedAtCapMultibyte(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)
	long := strings.Repeat("世", 1000)

	stubRoomTimes(rooms, []string{"r1"}, roomLastMsgAt, roomCreatedAt)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{{MessageID: "m1", RoomID: "r1", Msg: long, CreatedAt: roomLastMsgAt}}, false), nil)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	require.Contains(t, resp.Rooms, "r1")
	assert.Equal(t, strings.Repeat("世", preview.MaxContentRunes), resp.Rooms["r1"].Content)
}

// Content beyond the preview cap is truncated to exactly preview.MaxContentRunes
// runes — the room list renders a snippet, not the full (≤20KB) body.
func TestHistoryService_RoomsGet_PreviewContentTruncated(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)
	long := strings.Repeat("x", preview.MaxContentRunes+100)

	stubRoomTimes(rooms, []string{"r1"}, roomLastMsgAt, roomCreatedAt)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{{MessageID: "m1", RoomID: "r1", Msg: long, CreatedAt: roomLastMsgAt}}, false), nil)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	require.Contains(t, resp.Rooms, "r1")
	assert.Equal(t, strings.Repeat("x", preview.MaxContentRunes), resp.Rooms["r1"].Content)
}

// Latest message is a system message → walk back to the first non-system, non-quoted survivor.
func TestHistoryService_RoomsGet_SkipsSystemTail(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)

	stubRoomTimes(rooms, []string{"r1"}, roomLastMsgAt, roomCreatedAt)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{
			{MessageID: "m2", RoomID: "r1", Type: model.MessageTypeRoomRenamed, CreatedAt: roomLastMsgAt},
			{MessageID: "m1", RoomID: "r1", Msg: "alive", CreatedAt: roomLastMsgAt.Add(-time.Minute)},
		}, false), nil)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	require.Contains(t, resp.Rooms, "r1")
	assert.Equal(t, "m1", resp.Rooms["r1"].MessageID)
}

// Every system type is skipped in the preview; a client type (important) is not.
func TestHistoryService_RoomsGet_SkipsEachSystemTypeButKeepsImportant(t *testing.T) {
	for _, st := range []string{
		model.MessageTypeRoomCreated, model.MessageTypeMembersAdded,
		model.MessageTypeMemberRemoved, model.MessageTypeMemberLeft,
		model.MessageTypeRoomRenamed, model.MessageTypeRoomRestricted,
		model.MessageTypeTeamsMeetStarted,
	} {
		svc, msgs, rooms := newRoomsService(t)
		stubRoomTimes(rooms, []string{"r1"}, roomLastMsgAt, roomCreatedAt)
		msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
			Return(makePage([]models.Message{
				{MessageID: "m2", RoomID: "r1", Type: st, CreatedAt: roomLastMsgAt},
				{MessageID: "m1", RoomID: "r1", Msg: "alive", CreatedAt: roomLastMsgAt.Add(-time.Minute)},
			}, false), nil)

		resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
		require.NoError(t, err)
		assert.Equal(t, "m1", resp.Rooms["r1"].MessageID, "system type %q must be skipped", st)
	}
}

func TestHistoryService_RoomsGet_KeepsImportantMessage(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)
	stubRoomTimes(rooms, []string{"r1"}, roomLastMsgAt, roomCreatedAt)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{
			{MessageID: "m2", RoomID: "r1", Msg: "urgent", Type: model.MessageTypeImportant, CreatedAt: roomLastMsgAt},
		}, false), nil)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	assert.Equal(t, "m2", resp.Rooms["r1"].MessageID, "an important (client) message must preview")
}

// A quoted reply is normal room content and IS eligible as the preview.
func TestHistoryService_RoomsGet_QuotedReplyEligible(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)

	stubRoomTimes(rooms, []string{"r1"}, roomLastMsgAt, roomCreatedAt)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{
			{MessageID: "m2", RoomID: "r1", Msg: "re: x", QuotedParentMessage: &models.QuotedParentMessage{MessageID: "m0"}, CreatedAt: roomLastMsgAt},
			{MessageID: "m1", RoomID: "r1", Msg: "alive", CreatedAt: roomLastMsgAt.Add(-time.Minute)},
		}, false), nil)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	require.Contains(t, resp.Rooms, "r1")
	assert.Equal(t, "m2", resp.Rooms["r1"].MessageID)
}

// Mixed tail: a real system message + a deleted message precede a quoted reply,
// which IS eligible and becomes the preview.
func TestHistoryService_RoomsGet_MixedTailSkipsIneligible(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)

	stubRoomTimes(rooms, []string{"r1"}, roomLastMsgAt, roomCreatedAt)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{
			{MessageID: "m4", RoomID: "r1", Type: model.MessageTypeMembersAdded, CreatedAt: roomLastMsgAt},
			{MessageID: "m3", RoomID: "r1", Deleted: true, CreatedAt: roomLastMsgAt.Add(-time.Minute)},
			{MessageID: "m2", RoomID: "r1", Msg: "re: x", QuotedParentMessage: &models.QuotedParentMessage{MessageID: "m0"}, CreatedAt: roomLastMsgAt.Add(-2 * time.Minute)},
			{MessageID: "m1", RoomID: "r1", Msg: "alive", CreatedAt: roomLastMsgAt.Add(-3 * time.Minute)},
		}, false), nil)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	require.Contains(t, resp.Rooms, "r1")
	assert.Equal(t, "m2", resp.Rooms["r1"].MessageID)
}

// A normal message (no type, no quote, not deleted) is returned as-is.
func TestHistoryService_RoomsGet_NormalMessageUnaffected(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)

	stubRoomTimes(rooms, []string{"r1"}, roomLastMsgAt, roomCreatedAt)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{{MessageID: "m1", RoomID: "r1", Msg: "hi", CreatedAt: roomLastMsgAt}}, false), nil)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	require.Contains(t, resp.Rooms, "r1")
	assert.Equal(t, "m1", resp.Rooms["r1"].MessageID)
}

func TestHistoryService_RoomsGet_EmptyRoomIDs(t *testing.T) {
	svc, _, _ := newRoomsService(t)
	_, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: nil})
	assertBadRequestErr(t, err, "roomIds must not be empty")
}

func TestHistoryService_RoomsGet_TooManyRoomIDs(t *testing.T) {
	svc, _, _ := newRoomsService(t)
	ids := make([]string, roomsGetMaxBatch+1)
	for i := range ids {
		ids[i] = "r" + string(rune('a'+i%26))
	}
	_, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: ids})
	assertBadRequestErr(t, err, "too many roomIds")
}

// Preview enrichment: attachments, mentions (wire Participants), and visibleTo are
// carried; a non-bot sender's chineseName comes from the Cassandra company_name and no
// app lookup happens.
func TestHistoryService_RoomsGet_EnrichesPreview(t *testing.T) {
	svc, msgs, rooms, apps := newRoomsServiceWithApps(t)
	apps.EXPECT().AppNameByAccount(gomock.Any(), gomock.Any()).Times(0) // no bot → no lookup

	stubRoomTimes(rooms, []string{"r1"}, roomLastMsgAt, roomCreatedAt)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{{
			MessageID:   "m1",
			RoomID:      "r1",
			Msg:         "hi",
			CreatedAt:   roomLastMsgAt,
			Sender:      models.Participant{ID: "u1", Account: "alice", EngName: "Alice", CompanyName: "愛麗絲"},
			Attachments: cassandra.EncodeAttachments([]cassandra.Attachment{{ID: "f1", Title: "a.png", Type: "file"}}),
			Mentions:    []models.Participant{{ID: "u2", Account: "bob", CompanyName: "小明"}},
			VisibleTo:   "u1",
		}}, false), nil)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	require.Contains(t, resp.Rooms, "r1")
	pm := resp.Rooms["r1"]
	assert.Equal(t, "愛麗絲", pm.Sender.ChineseName)       // company_name → chineseName
	assert.Equal(t, "Alice 愛麗絲", pm.Sender.DisplayName) // composed, not a bot
	assert.Equal(t, "u1", pm.Sender.UserID)
	require.Len(t, pm.Attachments, 1)
	assert.Equal(t, "a.png", pm.Attachments[0].Title)
	require.Len(t, pm.Mentions, 1)
	assert.Equal(t, "bob", pm.Mentions[0].Account)
	assert.Equal(t, "小明", pm.Mentions[0].ChineseName)
	assert.Equal(t, "u1", pm.VisibleTo)
}

// A bot sender's displayName is its app name (mirrors the reaction actor path).
func TestHistoryService_RoomsGet_BotSenderAppName(t *testing.T) {
	svc, msgs, rooms, apps := newRoomsServiceWithApps(t)
	apps.EXPECT().AppNameByAccount(gomock.Any(), "acme.bot").Return("Acme Assistant", nil)

	stubRoomTimes(rooms, []string{"r1"}, roomLastMsgAt, roomCreatedAt)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{{
			MessageID: "m1", RoomID: "r1", Msg: "beep", CreatedAt: roomLastMsgAt,
			Sender: models.Participant{ID: "b1", Account: "acme.bot", EngName: "acme"},
		}}, false), nil)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	require.Contains(t, resp.Rooms, "r1")
	assert.Equal(t, "Acme Assistant", resp.Rooms["r1"].Sender.DisplayName)
}

// Empty attachments/mentions serialize away (omitempty) — no [] noise in the preview.
func TestHistoryService_RoomsGet_EmptyCollectionsOmitted(t *testing.T) {
	svc, msgs, rooms, _ := newRoomsServiceWithApps(t)

	stubRoomTimes(rooms, []string{"r1"}, roomLastMsgAt, roomCreatedAt)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{{MessageID: "m1", RoomID: "r1", Msg: "hi", CreatedAt: roomLastMsgAt, Sender: models.Participant{Account: "alice"}}}, false), nil)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	pm := resp.Rooms["r1"]
	assert.Nil(t, pm.Attachments)
	assert.Nil(t, pm.Mentions)

	data, err := json.Marshal(pm)
	require.NoError(t, err)
	assert.NotContains(t, string(data), "attachments")
	assert.NotContains(t, string(data), "mentions")
	assert.NotContains(t, string(data), "visibleTo")
}

// msPtr converts a time to the UTC-millis pointer RoomTimeHint/RoomMeta wire fields use.
func msPtr(t time.Time) *int64 {
	ms := t.UnixMilli()
	return &ms
}

// (a) Every room carries a usable lastMsgAt hint: resolveRoomTimes never needs Mongo at
// all, so neither the per-room GetRoomTimes nor the batched GetRoomTimesByIDs is called.
func TestHistoryService_RoomsGet_AllHinted_NoStoreReads(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)
	rooms.EXPECT().GetRoomTimes(gomock.Any(), gomock.Any()).Times(0)
	rooms.EXPECT().GetRoomTimesByIDs(gomock.Any(), gomock.Any()).Times(0)

	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{{MessageID: "m1", RoomID: "r1", Msg: "hi", CreatedAt: roomLastMsgAt}}, false), nil)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r2", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{{MessageID: "m2", RoomID: "r2", Msg: "hey", CreatedAt: roomLastMsgAt}}, false), nil)

	req := models.RoomsGetRequest{
		RoomIDs: []string{"r1", "r2"},
		Hints: map[string]model.RoomTimeHint{
			"r1": {LastMsgAt: msPtr(roomLastMsgAt)},                                  // lastMsgAt-only hint, still sufficient
			"r2": {LastMsgAt: msPtr(roomLastMsgAt), CreatedAt: msPtr(roomCreatedAt)}, // both fields hinted
		},
	}
	resp, err := svc.RoomsGet(roomsCtx(), req)
	require.NoError(t, err)
	assert.Equal(t, "m1", resp.Rooms["r1"].MessageID)
	assert.Equal(t, "m2", resp.Rooms["r2"].MessageID)
}

// (b) No hints at all: the whole id set is unhinted, batched into exactly one
// GetRoomTimesByIDs call carrying every id; the per-room GetRoomTimes is never used.
func TestHistoryService_RoomsGet_NoHints_BatchesAllIDs(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)
	rooms.EXPECT().GetRoomTimes(gomock.Any(), gomock.Any()).Times(0)
	rooms.EXPECT().
		GetRoomTimesByIDs(gomock.Any(), gomock.InAnyOrder([]string{"r1", "r2"})).
		Return(map[string]mongorepo.RoomTimes{
			"r1": {LastMsgAt: roomLastMsgAt, CreatedAt: roomCreatedAt},
			"r2": {LastMsgAt: roomLastMsgAt, CreatedAt: roomCreatedAt},
		}, nil).
		Times(1)

	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{{MessageID: "m1", RoomID: "r1", Msg: "hi", CreatedAt: roomLastMsgAt}}, false), nil)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r2", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{{MessageID: "m2", RoomID: "r2", Msg: "hey", CreatedAt: roomLastMsgAt}}, false), nil)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1", "r2"}})
	require.NoError(t, err)
	assert.Equal(t, "m1", resp.Rooms["r1"].MessageID)
	assert.Equal(t, "m2", resp.Rooms["r2"].MessageID)
}

// (c) Mixed hinted/unhinted: only r2 (unhinted) is in the batch call; r1's hint means it
// never reaches Mongo at all.
func TestHistoryService_RoomsGet_MixedHints_BatchesOnlyUnhinted(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)
	rooms.EXPECT().GetRoomTimes(gomock.Any(), gomock.Any()).Times(0)
	rooms.EXPECT().
		GetRoomTimesByIDs(gomock.Any(), []string{"r2"}).
		Return(map[string]mongorepo.RoomTimes{"r2": {LastMsgAt: roomLastMsgAt, CreatedAt: roomCreatedAt}}, nil).
		Times(1)

	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{{MessageID: "m1", RoomID: "r1", Msg: "hi", CreatedAt: roomLastMsgAt}}, false), nil)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r2", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{{MessageID: "m2", RoomID: "r2", Msg: "hey", CreatedAt: roomLastMsgAt}}, false), nil)

	req := models.RoomsGetRequest{
		RoomIDs: []string{"r1", "r2"},
		Hints: map[string]model.RoomTimeHint{
			"r1": {LastMsgAt: msPtr(roomLastMsgAt)},
			// r2 has no entry in Hints at all → unhinted.
		},
	}
	resp, err := svc.RoomsGet(roomsCtx(), req)
	require.NoError(t, err)
	assert.Equal(t, "m1", resp.Rooms["r1"].MessageID)
	assert.Equal(t, "m2", resp.Rooms["r2"].MessageID)
}

// (d) The batch read errors: RoomsGet still succeeds. The unhinted room falls back through
// resolveRoomTimes' existing per-room GetRoomTimes path — the batch failure never fails the RPC.
func TestHistoryService_RoomsGet_BatchReadError_DegradesToPerRoomFallback(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)
	rooms.EXPECT().
		GetRoomTimesByIDs(gomock.Any(), []string{"r1"}).
		Return(nil, errors.New("mongo down")).
		Times(1)
	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(roomLastMsgAt, roomCreatedAt, nil)

	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{{MessageID: "m1", RoomID: "r1", Msg: "hi", CreatedAt: roomLastMsgAt}}, false), nil)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	assert.Equal(t, "m1", resp.Rooms["r1"].MessageID)
}

// (e) An implausible hint lastMsgAt (pre-2020 epoch) is rejected by the same
// sanitizeLastMsgAt every per-room resolve uses, so the room is treated as unhinted and
// goes into the batch set instead of being trusted at face value.
func TestHistoryService_RoomsGet_ImplausibleHint_TreatedAsUnhinted(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)
	bogus := int64(0) // epoch 0 (1970) — before minPlausibleEpoch
	rooms.EXPECT().
		GetRoomTimesByIDs(gomock.Any(), []string{"r1"}).
		Return(map[string]mongorepo.RoomTimes{"r1": {LastMsgAt: roomLastMsgAt, CreatedAt: roomCreatedAt}}, nil).
		Times(1)

	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{{MessageID: "m1", RoomID: "r1", Msg: "hi", CreatedAt: roomLastMsgAt}}, false), nil)

	req := models.RoomsGetRequest{
		RoomIDs: []string{"r1"},
		Hints:   map[string]model.RoomTimeHint{"r1": {LastMsgAt: &bogus}},
	}
	resp, err := svc.RoomsGet(roomsCtx(), req)
	require.NoError(t, err)
	assert.Equal(t, "m1", resp.Rooms["r1"].MessageID)
}

// An oversized Hints map is malformed (hints are only consulted for the capped
// RoomIDs) and is rejected symmetrically with the RoomIDs cap, before any store read.
func TestHistoryService_RoomsGet_TooManyHints_BadRequest(t *testing.T) {
	svc, _, _ := newRoomsService(t)
	hints := make(map[string]model.RoomTimeHint, roomsGetMaxBatch+1)
	for i := 0; i <= roomsGetMaxBatch; i++ {
		hints["r"+strconv.Itoa(i)] = model.RoomTimeHint{LastMsgAt: msPtr(roomLastMsgAt)}
	}
	_, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}, Hints: hints})
	assertBadRequestErr(t, err, "too many hints")
}

// A requested unhinted room that GetRoomTimesByIDs does not return (e.g. deleted
// between the caller's list read and this resolve, so it's absent from the batch
// result map) keeps a nil meta and falls through to the per-room GetRoomTimes path —
// it must not be silently dropped by the batch step.
func TestHistoryService_RoomsGet_UnhintedRoomAbsentFromBatch_FallsBackPerRoom(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)
	// Batch returns an empty map: r1 is unhinted but absent from the result.
	rooms.EXPECT().
		GetRoomTimesByIDs(gomock.Any(), []string{"r1"}).
		Return(map[string]mongorepo.RoomTimes{}, nil).
		Times(1)
	// Absent from the batch → the per-room fallback fires exactly once.
	rooms.EXPECT().GetRoomTimes(gomock.Any(), "r1").Return(roomLastMsgAt, roomCreatedAt, nil).Times(1)

	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{{MessageID: "m1", RoomID: "r1", Msg: "hi", CreatedAt: roomLastMsgAt}}, false), nil)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	assert.Equal(t, "m1", resp.Rooms["r1"].MessageID)
}
