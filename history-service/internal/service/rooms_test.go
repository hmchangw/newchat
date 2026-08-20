package service_test

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/history-service/internal/cassrepo"
	"github.com/hmchangw/chat/history-service/internal/config"
	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/history-service/internal/mongorepo"
	"github.com/hmchangw/chat/history-service/internal/readcache"
	"github.com/hmchangw/chat/history-service/internal/service"
	"github.com/hmchangw/chat/history-service/internal/service/mocks"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/natsrouter"
)

// newRoomsService builds a service with bare mocks; WithPreviewCache exercises the cache.
func newRoomsService(t *testing.T, opts ...service.Option) (*service.HistoryService, *mocks.MockMessageRepository, *mocks.MockRoomRepository) {
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
	svc := service.New(msgs, subs, rooms, pub, threadRooms, threadSubs, users, apps, cfg, opts...)
	return svc, msgs, rooms
}

func roomsCtx() *natsrouter.Context {
	return natsrouter.NewContext(map[string]string{"account": "alice"})
}

var roomLastMsgAt = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
var roomCreatedAt = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// Mirror the production caps (house pattern — see maxGetByIDsBatchSize in messages_test).
const roomsGetMaxBatch = 100

// storedRow is a room doc that carries a preview; nil pvw is a room that has none.
func storedRow(pvw *model.PreviewMessage) mongorepo.RoomTimes {
	return mongorepo.RoomTimes{LastMsgAt: roomLastMsgAt, CreatedAt: roomCreatedAt, Preview: pvw}
}

// allowWarmBack permits, without asserting on, the write a resolved walk performs.
func allowWarmBack(rooms *mocks.MockRoomRepository) {
	rooms.EXPECT().
		SetPreviewMessage(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil).AnyTimes()
}

// Warm-back makes an eager-path miss self-healing: the first read repairs the doc.
func TestHistoryService_RoomsGet_ResolvedWalkWarmsBackTheStoredPreview(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)
	walked := walkedMsg("r1", "m-walked", "resolved lazily")

	rooms.EXPECT().GetRoomTimesByIDs(gomock.Any(), gomock.Any()).
		Return(map[string]mongorepo.RoomTimes{"r1": storedRow(nil)}, nil)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{walked}, false), nil)
	// The watermark itself is pinned by TestHistoryService_RoomsGet_WarmBackIsOrderedByWalkTimeNotMessageAge.
	rooms.EXPECT().
		SetPreviewMessage(gomock.Any(), "r1", gomock.Any(), "m-walked", gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, pvw model.PreviewMessage, _ string, _ int64) error {
			assert.Equal(t, "m-walked", pvw.MessageID)
			assert.Equal(t, "resolved lazily", pvw.Content)
			return nil
		}).Times(1)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	assert.Equal(t, "m-walked", resp.Rooms["r1"].MessageID)
}

// The watermark must be walk time, not createdAt: a mutation stamps a later previewAsOf,
// so an edited room would reject the warm-back — and its repairs — forever.
func TestHistoryService_RoomsGet_WarmBackIsOrderedByWalkTimeNotMessageAge(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)
	// An old message: a createdAt-ordered warm-back would submit this, and lose.
	old := walkedMsg("r1", "m-old", "written long ago")
	old.CreatedAt = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	before := time.Now().UTC().UnixMilli()

	rooms.EXPECT().GetRoomTimesByIDs(gomock.Any(), gomock.Any()).
		Return(map[string]mongorepo.RoomTimes{"r1": storedRow(nil)}, nil)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{old}, false), nil)

	var gotAsOf int64
	rooms.EXPECT().SetPreviewMessage(gomock.Any(), "r1", gomock.Any(), "m-old", gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, _ model.PreviewMessage, _ string, asOf int64) error {
			gotAsOf = asOf
			return nil
		}).Times(1)

	_, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)

	assert.GreaterOrEqual(t, gotAsOf, before, "the watermark must be the walk's own clock")
	assert.NotEqual(t, old.CreatedAt.UnixMilli(), gotAsOf, "ordering by the message's age deadlocks against mutation watermarks")
}

// The key must be the newest message SEEN, not the eligible one landed on: otherwise a
// room whose newest message is a system message re-walks on every read.
func TestHistoryService_RoomsGet_WarmBackKeysOnNewestObservedNotThePreview(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)
	system := walkedMsg("r1", "m-system", "joined")
	system.Type = model.MessageTypeMembersAdded
	eligible := walkedMsg("r1", "m-eligible", "real content")

	rooms.EXPECT().GetRoomTimesByIDs(gomock.Any(), gomock.Any()).
		Return(map[string]mongorepo.RoomTimes{"r1": storedRow(nil)}, nil)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{system, eligible}, false), nil)
	rooms.EXPECT().
		SetPreviewMessage(gomock.Any(), "r1", gomock.Any(), "m-system", gomock.Any()).
		Return(nil).Times(1)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	assert.Equal(t, "m-eligible", resp.Rooms["r1"].MessageID, "the served preview is still the eligible message")
}

// Nothing resolved, nothing to warm back: an empty key could never be invalidated.
func TestHistoryService_RoomsGet_UnresolvedWalkWarmsBackNothing(t *testing.T) {
	tests := []struct {
		name string
		page cassrepo.Page[models.Message]
		err  error
	}{
		{name: "empty room", page: makePage(nil, false)},
		{name: "degraded read", err: errors.New("cassandra down")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svc, msgs, rooms := newRoomsService(t)
			rooms.EXPECT().GetRoomTimesByIDs(gomock.Any(), gomock.Any()).
				Return(map[string]mongorepo.RoomTimes{"r1": storedRow(nil)}, nil)
			msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
				Return(tc.page, tc.err)
			// No SetPreviewMessage expectation: any warm-back here fails the test.

			resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
			require.NoError(t, err)
			assert.Empty(t, resp.Rooms)
		})
	}
}

// A failed warm-back costs the optimization, never the response.
func TestHistoryService_RoomsGet_WarmBackFailureStillServesThePreview(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)

	rooms.EXPECT().GetRoomTimesByIDs(gomock.Any(), gomock.Any()).
		Return(map[string]mongorepo.RoomTimes{"r1": storedRow(nil)}, nil)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{walkedMsg("r1", "m-walked", "served anyway")}, false), nil)
	rooms.EXPECT().SetPreviewMessage(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(errors.New("mongo down")).Times(1)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	assert.Equal(t, "served anyway", resp.Rooms["r1"].Content)
}

// The point of writing previews eagerly: a doc hit costs one batched read and no
// Cassandra. The lazy fallback exists, but a stored preview must never reach it.
func TestHistoryService_RoomsGet_ServesStoredPreviewWithoutReadingMessages(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)
	stored := &model.PreviewMessage{MessageID: "m-1", Content: "hello", Sender: model.Participant{Account: "alice"}}

	rooms.EXPECT().GetRoomTimesByIDs(gomock.Any(), []string{"r1"}).
		Return(map[string]mongorepo.RoomTimes{"r1": storedRow(stored)}, nil)
	// No expectations: a Cassandra read OR a warm-back write here fails the test, since
	// re-writing what we just read would be a write per read on the hot path.
	_ = msgs

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	require.Len(t, resp.Rooms, 1)
	assert.Equal(t, "m-1", resp.Rooms["r1"].MessageID)
	assert.Equal(t, "hello", resp.Rooms["r1"].Content)
}

// walkedMsg is an eligible Cassandra row the lazy fallback can resolve into a preview.
func walkedMsg(roomID, msgID, body string) models.Message {
	return models.Message{
		MessageID: msgID, RoomID: roomID, Msg: body, CreatedAt: roomLastMsgAt,
		Sender: cassandra.Participant{ID: "u-bob", Account: "bob", EngName: "Bob"},
	}
}

// Eager persistence is an optimization, not the only path: a room with no usable stored
// preview — pre-rollout, or a site not writing them — still resolves one the lazy way.
func TestHistoryService_RoomsGet_RoomWithoutStoredPreviewFallsBackToTheWalk(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)
	allowWarmBack(rooms)

	rooms.EXPECT().GetRoomTimesByIDs(gomock.Any(), gomock.Any()).Return(map[string]mongorepo.RoomTimes{
		"r1": storedRow(&model.PreviewMessage{MessageID: "m-stored"}),
		"r2": storedRow(nil),
	}, nil)
	// Only the unstored room may be walked; a second expectation for r1 would fail this mock.
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r2", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{walkedMsg("r2", "m-walked", "resolved lazily")}, false), nil)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1", "r2"}})
	require.NoError(t, err)
	require.Len(t, resp.Rooms, 2)
	assert.Equal(t, "m-stored", resp.Rooms["r1"].MessageID, "a stored preview must not be re-walked")
	assert.Equal(t, "m-walked", resp.Rooms["r2"].MessageID)
	assert.Equal(t, "resolved lazily", resp.Rooms["r2"].Content)
}

// The fallback is per-room and must degrade, never cancel siblings: one room whose
// Cassandra read fails cannot cost a batch of a hundred its other previews.
func TestHistoryService_RoomsGet_WalkFailureOmitsOnlyThatRoom(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)
	allowWarmBack(rooms)

	rooms.EXPECT().GetRoomTimesByIDs(gomock.Any(), gomock.Any()).Return(map[string]mongorepo.RoomTimes{
		"r1": storedRow(nil),
		"r2": storedRow(nil),
	}, nil)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{walkedMsg("r1", "m-1", "fine")}, false), nil)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r2", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(cassrepo.Page[models.Message]{}, errors.New("cassandra down"))

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1", "r2"}})
	require.NoError(t, err, "a per-room failure must not fail the batch")
	assert.Len(t, resp.Rooms, 1)
	assert.Contains(t, resp.Rooms, "r1")
	assert.NotContains(t, resp.Rooms, "r2")
}

// A completed walk that finds nothing is a real "no preview": the room is simply absent.
func TestHistoryService_RoomsGet_RoomWithNoEligibleMessageIsOmitted(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)

	rooms.EXPECT().GetRoomTimesByIDs(gomock.Any(), gomock.Any()).
		Return(map[string]mongorepo.RoomTimes{"r1": storedRow(nil)}, nil)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage(nil, false), nil)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	assert.Empty(t, resp.Rooms)
}

// A room Mongo doesn't return is absent and must not be walked: no times, no bounds.
func TestHistoryService_RoomsGet_MissingRoomIsOmittedAndNotWalked(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)

	rooms.EXPECT().GetRoomTimesByIDs(gomock.Any(), gomock.Any()).
		Return(map[string]mongorepo.RoomTimes{}, nil)
	_ = msgs // no expectations: any Cassandra read here fails the test

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"ghost"}})
	require.NoError(t, err)
	assert.Empty(t, resp.Rooms)
}

// The cache keeps the fallback affordable where nothing writes eagerly: a second request
// for the same unstored room must not walk Cassandra again.
func TestHistoryService_RoomsGet_CacheFrontsTheLazyFallback(t *testing.T) {
	pc, err := readcache.NewPreviewCache(16, time.Minute)
	require.NoError(t, err)
	svc, msgs, rooms := newRoomsService(t, service.WithPreviewCache(pc))

	rooms.EXPECT().GetRoomTimesByIDs(gomock.Any(), gomock.Any()).
		Return(map[string]mongorepo.RoomTimes{"r1": storedRow(nil)}, nil).Times(2)
	// Exactly once across both requests — the second is a cache hit.
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{walkedMsg("r1", "m-1", "cached")}, false), nil).Times(1)
	// Warm-back rides the walk, not the request: a cache hit must not re-write the doc.
	rooms.EXPECT().SetPreviewMessage(gomock.Any(), "r1", gomock.Any(), "m-1", gomock.Any()).
		Return(nil).Times(1)

	for range 2 {
		resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
		require.NoError(t, err)
		require.Len(t, resp.Rooms, 1)
		assert.Equal(t, "cached", resp.Rooms["r1"].Content)
	}
}

// A failed read must surface: an empty map is indistinguishable from "no previews" at the
// client and would blank every row in the list.
func TestHistoryService_RoomsGet_ReadFailureIsAnError(t *testing.T) {
	svc, _, rooms := newRoomsService(t)

	rooms.EXPECT().GetRoomTimesByIDs(gomock.Any(), gomock.Any()).
		Return(nil, errors.New("mongo down"))

	_, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.Error(t, err)
}

func TestHistoryService_RoomsGet_DedupsRoomIDs(t *testing.T) {
	svc, _, rooms := newRoomsService(t)

	rooms.EXPECT().GetRoomTimesByIDs(gomock.Any(), []string{"r1"}).
		Return(map[string]mongorepo.RoomTimes{"r1": storedRow(&model.PreviewMessage{MessageID: "m-1"})}, nil)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1", "r1", "r1"}})
	require.NoError(t, err)
	assert.Len(t, resp.Rooms, 1)
}

// Hints bounded a walk this path no longer performs; still accepted, must change nothing.
func TestHistoryService_RoomsGet_HintsAreIgnored(t *testing.T) {
	svc, _, rooms := newRoomsService(t)
	last := roomLastMsgAt.UnixMilli()

	rooms.EXPECT().GetRoomTimesByIDs(gomock.Any(), []string{"r1"}).
		Return(map[string]mongorepo.RoomTimes{"r1": storedRow(&model.PreviewMessage{MessageID: "m-1"})}, nil)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{
		RoomIDs: []string{"r1"},
		Hints:   map[string]model.RoomTimeHint{"r1": {LastMsgAt: &last}},
	})
	require.NoError(t, err)
	assert.Len(t, resp.Rooms, 1)
}

func TestHistoryService_RoomsGet_EmptyRoomIDs(t *testing.T) {
	svc, _, _ := newRoomsService(t)
	_, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{})
	require.Error(t, err)
}

func TestHistoryService_RoomsGet_TooManyRoomIDs(t *testing.T) {
	svc, _, _ := newRoomsService(t)
	ids := make([]string, roomsGetMaxBatch+1)
	for i := range ids {
		ids[i] = "r" + strconv.Itoa(i)
	}
	_, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: ids})
	require.Error(t, err)
}

func TestHistoryService_RoomsGet_TooManyHints_BadRequest(t *testing.T) {
	svc, _, _ := newRoomsService(t)
	last := roomLastMsgAt.UnixMilli()
	hints := make(map[string]model.RoomTimeHint, roomsGetMaxBatch+1)
	for i := range roomsGetMaxBatch + 1 {
		hints["r"+strconv.Itoa(i)] = model.RoomTimeHint{LastMsgAt: &last}
	}
	_, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}, Hints: hints})
	require.Error(t, err)
}

// The warm-back is optional and holds one of the 16 lazy slots while it runs. When the
// request budget cannot absorb it, serving the preview must beat storing it: a skipped
// warm-back costs one re-walk on a later read, an overrun costs the whole response.
func TestHistoryService_RoomsGet_SkipsWarmBackWhenTheRequestBudgetCannotAbsorbIt(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)
	walked := walkedMsg("r1", "m-walked", "resolved lazily")

	rooms.EXPECT().GetRoomTimesByIDs(gomock.Any(), gomock.Any()).
		Return(map[string]mongorepo.RoomTimes{"r1": storedRow(nil)}, nil)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{walked}, false), nil)
	// No SetPreviewMessage expectation: a warm-back on this budget fails the test.

	rc := roomsCtx()
	deadlined, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	rc.SetContext(deadlined)

	resp, err := svc.RoomsGet(rc, models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	assert.Equal(t, "m-walked", resp.Rooms["r1"].MessageID,
		"the resolved preview is still served when its warm-back is skipped")
}

// Cancellation after the last lazy worker launches must not read as "these rooms have no
// preview" — the same reason the batched read errors rather than returning an empty map.
func TestHistoryService_RoomsGet_CancellationDuringTheWalkIsAnErrorNotAPartialAnswer(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)

	rc := roomsCtx()
	cctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rc.SetContext(cctx)

	rooms.EXPECT().GetRoomTimesByIDs(gomock.Any(), gomock.Any()).
		Return(map[string]mongorepo.RoomTimes{"r1": storedRow(nil)}, nil)
	// The caller goes away mid-walk; the walk yields nothing for its room.
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, string, time.Time, time.Time, cassrepo.PageRequest) (cassrepo.Page[models.Message], error) {
			cancel()
			return cassrepo.Page[models.Message]{}, context.Canceled
		})

	_, err := svc.RoomsGet(rc, models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.Error(t, err, "a cancelled request must not return a partial map as success")
	assert.ErrorIs(t, err, context.Canceled)
}
