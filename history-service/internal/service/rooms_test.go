package service_test

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
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
	"github.com/hmchangw/chat/pkg/natsutil"
)

// newRoomsService builds a service with bare mocks; WithPreviewCache exercises the cache.
func newRoomsService(t *testing.T, opts ...service.Option) (*service.HistoryService, *mocks.MockMessageRepository, *mocks.MockRoomRepository) {
	return newRoomsServiceWithConfig(t, nil, opts...)
}

// newRoomsServiceWithWarmer sizes the background preview writer, for the tests that assert
// on saturation; non-positive values take the production defaults. Warm-backs are queued,
// so every caller drains on cleanup — that is what makes the mock's call count
// deterministic. Registered after gomock's own cleanup so it runs first (LIFO).
func newRoomsServiceWithWarmer(t *testing.T, warmWorkers, warmQueue int, opts ...service.Option) (*service.HistoryService, *mocks.MockMessageRepository, *mocks.MockRoomRepository) {
	return newRoomsServiceWithConfig(t, func(c *config.Config) {
		c.PreviewWarmBackWorkers, c.PreviewWarmBackQueue = warmWorkers, warmQueue
	}, opts...)
}

// disableWarmBack turns the background preview writer off, the way an operator does.
func disableWarmBack(c *config.Config) { c.PreviewWarmBackEnabled = false }

// newRoomsServiceWithConfig builds a service whose config the caller adjusts first, for the
// tests that flip a toggle rather than size a queue. A nil tweak takes the defaults below.
func newRoomsServiceWithConfig(t *testing.T, tweak func(*config.Config), opts ...service.Option) (*service.HistoryService, *mocks.MockMessageRepository, *mocks.MockRoomRepository) {
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
		MessageHistoryFloorDays: 90, LargeRoomThreshold: 500, MaxPinnedPerRoom: 10, PinEnabled: true,
		PreviewWarmBackEnabled: true,
	}
	if tweak != nil {
		tweak(cfg)
	}
	svc := closeOnCleanup(t, service.New(msgs, subs, rooms, pub, threadRooms, threadSubs, users, apps, cfg, opts...))
	return svc, msgs, rooms
}

// closeOnCleanup drains the service's background preview writer when the test ends. New
// starts those workers, so every construction needs a termination path — and draining is
// also what makes a queued warm-back's call count deterministic for the mock. Registered
// after gomock's own cleanup so it runs first (LIFO).
func closeOnCleanup(t *testing.T, svc *service.HistoryService) *service.HistoryService {
	t.Helper()
	t.Cleanup(func() { _ = svc.Close(context.Background()) })
	return svc
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
	require.NoError(t, svc.Close(context.Background()), "the warm-back is queued; drain before reading what it wrote")

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

// A mutation changes what the room previews, so the cached entry describes a message
// that just changed -- and after a delete, one that is gone. Serving it from cache would
// undo the clear that the same request performed (#292).
func TestHistoryService_RoomsGet_MutationDropsTheCachedPreview(t *testing.T) {
	pc, err := readcache.NewPreviewCache(16, time.Minute)
	require.NoError(t, err)
	svc, msgs, rooms := newRoomsService(t, service.WithPreviewCache(pc))

	rooms.EXPECT().GetRoomTimesByIDs(gomock.Any(), gomock.Any()).
		Return(map[string]mongorepo.RoomTimes{"r1": storedRow(nil)}, nil).AnyTimes()
	rooms.EXPECT().SetPreviewMessage(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil).AnyTimes()
	// Twice, not once: the invalidation must force the second request to re-walk.
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{walkedMsg("r1", "m-1", "before")}, false), nil).Times(1)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	require.Equal(t, "before", resp.Rooms["r1"].Content)

	pc.Invalidate("r1")

	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{walkedMsg("r1", "m-2", "after")}, false), nil).Times(1)
	resp, err = svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	assert.Equal(t, "after", resp.Rooms["r1"].Content,
		"an invalidated room must re-resolve, not serve the pre-mutation entry")
}

// The batched read already returned the room document, so a room with no messages must
// not send history-service back to Mongo for the same two fields (#291).
func TestHistoryService_RoomsGet_NeverMessagedRoomDoesNotRereadRoomTimes(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)

	// Zero lastMsgAt: the room exists and has never been posted in.
	rooms.EXPECT().GetRoomTimesByIDs(gomock.Any(), gomock.Any()).
		Return(map[string]mongorepo.RoomTimes{"r1": {}}, nil)
	// No GetRoomTimes EXPECT at all — the mock controller fails if the walk re-reads.
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage(nil, false), nil)

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	assert.Empty(t, resp.Rooms, "a room with no eligible message is omitted")
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

// A spent request budget must not decide whether a room ever heals. The warm-back is
// queued, not written inline, so the write keeps its own clock: the rooms that most need
// repair are exactly the ones whose walk left no budget behind, and skipping them there
// made the miss permanent — the next read re-walks, stays slow, and skips again.
func TestHistoryService_RoomsGet_WarmsBackEvenWhenTheRequestBudgetIsSpent(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)
	walked := walkedMsg("r1", "m-walked", "resolved lazily")

	rooms.EXPECT().GetRoomTimesByIDs(gomock.Any(), gomock.Any()).
		Return(map[string]mongorepo.RoomTimes{"r1": storedRow(nil)}, nil)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{walked}, false), nil)
	rooms.EXPECT().SetPreviewMessage(gomock.Any(), "r1", gomock.Any(), "m-walked", gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ string, _ model.PreviewMessage, _ string, _ int64) error {
			assert.NoError(t, ctx.Err(), "the queued write must not inherit the request's exhausted budget")
			return nil
		}).Times(1)

	rc := roomsCtx()
	deadlined, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	rc.SetContext(deadlined)

	resp, err := svc.RoomsGet(rc, models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	assert.Equal(t, "m-walked", resp.Rooms["r1"].MessageID)
	require.NoError(t, svc.Close(context.Background()))
}

// The client hanging up cancels the request, never the repair it already paid for: the
// walk has run, and dropping its result would make the next reader run it again.
func TestHistoryService_RoomsGet_WarmBackSurvivesRequestCancellation(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)
	walked := walkedMsg("r1", "m-walked", "resolved lazily")

	rc := roomsCtx()
	cancellable, cancel := context.WithCancel(context.Background())
	rc.SetContext(cancellable)

	rooms.EXPECT().GetRoomTimesByIDs(gomock.Any(), gomock.Any()).
		Return(map[string]mongorepo.RoomTimes{"r1": storedRow(nil)}, nil)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{walked}, false), nil)
	rooms.EXPECT().SetPreviewMessage(gomock.Any(), "r1", gomock.Any(), "m-walked", gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ string, _ model.PreviewMessage, _ string, _ int64) error {
			assert.NoError(t, ctx.Err(), "a cancelled request must not cancel the queued write")
			return nil
		}).Times(1)

	_, err := svc.RoomsGet(rc, models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	cancel()
	require.NoError(t, svc.Close(context.Background()))
}

// Detaching from the request must not detach from its logs: the write is still that
// request's work, and a warm-back failure is only diagnosable if it names the read.
func TestHistoryService_RoomsGet_WarmBackCarriesTheRequestID(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)
	walked := walkedMsg("r1", "m-walked", "resolved lazily")

	rc := roomsCtx()
	rc.SetContext(natsutil.WithRequestID(context.Background(), "01970a4f-8c2d-7c9a-abcd-e0123456789f"))

	rooms.EXPECT().GetRoomTimesByIDs(gomock.Any(), gomock.Any()).
		Return(map[string]mongorepo.RoomTimes{"r1": storedRow(nil)}, nil)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{walked}, false), nil)
	var gotID string
	rooms.EXPECT().SetPreviewMessage(gomock.Any(), "r1", gomock.Any(), "m-walked", gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ string, _ model.PreviewMessage, _ string, _ int64) error {
			gotID = natsutil.RequestIDFromContext(ctx)
			return nil
		}).Times(1)

	_, err := svc.RoomsGet(rc, models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	require.NoError(t, svc.Close(context.Background()))
	assert.Equal(t, "01970a4f-8c2d-7c9a-abcd-e0123456789f", gotID)
}

// Saturation drops the write, never the reply. Queueing is what decouples the two, so an
// unbounded queue would trade the old latency problem for a memory one; a dropped
// warm-back is self-correcting, since the next read re-walks and re-submits.
func TestHistoryService_RoomsGet_WarmBackDropsWhenTheWriterIsSaturated(t *testing.T) {
	svc, msgs, rooms := newRoomsServiceWithWarmer(t, 1, 1)
	release := make(chan struct{})
	var writes atomic.Int32

	rooms.EXPECT().GetRoomTimesByIDs(gomock.Any(), gomock.Any()).
		Return(map[string]mongorepo.RoomTimes{
			"r1": storedRow(nil), "r2": storedRow(nil), "r3": storedRow(nil), "r4": storedRow(nil),
		}, nil).AnyTimes()
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, roomID string, _, _ time.Time, _ cassrepo.PageRequest) (cassrepo.Page[models.Message], error) {
			return makePage([]models.Message{walkedMsg(roomID, "m-"+roomID, "resolved lazily")}, false), nil
		}).AnyTimes()
	// The single worker parks on the first write, so the queue fills and the rest are shed.
	rooms.EXPECT().SetPreviewMessage(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, string, model.PreviewMessage, string, int64) error {
			writes.Add(1)
			<-release
			return nil
		}).AnyTimes()

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1", "r2", "r3", "r4"}})
	require.NoError(t, err)
	assert.Len(t, resp.Rooms, 4, "every preview is served even when its warm-back is shed")

	close(release)
	require.NoError(t, svc.Close(context.Background()))
	assert.LessOrEqual(t, int(writes.Load()), 2, "a saturated writer sheds rather than queues without bound")
}

// Close is the queue's termination path: shutdown must flush what it accepted, so a
// warm-back is not silently lost to a deploy.
func TestHistoryService_Close_DrainsPendingWarmBacks(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)
	walked := walkedMsg("r1", "m-walked", "resolved lazily")

	rooms.EXPECT().GetRoomTimesByIDs(gomock.Any(), gomock.Any()).
		Return(map[string]mongorepo.RoomTimes{"r1": storedRow(nil)}, nil)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{walked}, false), nil)
	var written atomic.Bool
	rooms.EXPECT().SetPreviewMessage(gomock.Any(), "r1", gomock.Any(), "m-walked", gomock.Any()).
		DoAndReturn(func(context.Context, string, model.PreviewMessage, string, int64) error {
			written.Store(true)
			return nil
		}).Times(1)

	_, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	require.NoError(t, svc.Close(context.Background()))
	assert.True(t, written.Load(), "Close must flush the queue it accepted work into")
}

// A read still in flight when shutdown starts must not send on the closed queue: the
// warm-back is dropped, and the reply it belongs to is served as normal.
func TestHistoryService_RoomsGet_WarmBackAfterCloseIsDroppedNotFatal(t *testing.T) {
	svc, msgs, rooms := newRoomsService(t)

	rooms.EXPECT().GetRoomTimesByIDs(gomock.Any(), gomock.Any()).
		Return(map[string]mongorepo.RoomTimes{"r1": storedRow(nil)}, nil)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{walkedMsg("r1", "m-walked", "resolved lazily")}, false), nil)
	// No SetPreviewMessage expectation: the writer is shut, so the job is shed.

	require.NoError(t, svc.Close(context.Background()))

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	assert.Equal(t, "m-walked", resp.Rooms["r1"].MessageID)
}

// Close is wired into a shutdown chain that may already be out of budget; it must give up
// on the optional writes rather than hold the whole shutdown past its deadline.
func TestHistoryService_Close_AbandonsTheDrainOnAnExpiredBudget(t *testing.T) {
	svc, msgs, rooms := newRoomsServiceWithWarmer(t, 1, 8)
	release := make(chan struct{})
	defer close(release)

	rooms.EXPECT().GetRoomTimesByIDs(gomock.Any(), gomock.Any()).
		Return(map[string]mongorepo.RoomTimes{"r1": storedRow(nil)}, nil)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{walkedMsg("r1", "m-walked", "resolved lazily")}, false), nil)
	rooms.EXPECT().SetPreviewMessage(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, string, model.PreviewMessage, string, int64) error {
			<-release
			return nil
		}).AnyTimes()

	_, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)

	expired, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.Error(t, svc.Close(expired), "an over-budget drain reports rather than blocks")
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

// PREVIEW_WARMBACK_ENABLED=false withholds the optional write, not the read. Distinct from
// the after-Close test above: that one sheds inside a live warmer, this one pins that the
// no-op is what New installs.
func TestHistoryService_RoomsGet_WarmBackDisabledStillServesTheWalkedPreview(t *testing.T) {
	svc, msgs, rooms := newRoomsServiceWithConfig(t, disableWarmBack)
	walked := walkedMsg("r1", "m-walked", "resolved lazily")

	rooms.EXPECT().GetRoomTimesByIDs(gomock.Any(), gomock.Any()).
		Return(map[string]mongorepo.RoomTimes{"r1": storedRow(nil)}, nil)
	msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{walked}, false), nil)
	// No SetPreviewMessage expectation: gomock fails the test if the write is attempted,
	// which is the whole point of the toggle.

	resp, err := svc.RoomsGet(roomsCtx(), models.RoomsGetRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	assert.Equal(t, "m-walked", resp.Rooms["r1"].MessageID)
}

// The no-op holds no queue and no workers, so shutdown must not wait on any — and Close
// must stay safe to call, twice, rather than becoming conditional at the call site.
func TestHistoryService_Close_DisabledWarmBackDrainsImmediately(t *testing.T) {
	svc, _, _ := newRoomsServiceWithConfig(t, disableWarmBack)

	// No workers to wait on, so a tight budget is ample — a disabled writer must not be
	// able to hold shutdown. Called twice: Close stays idempotent rather than conditional.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, svc.Close(ctx))
	require.NoError(t, svc.Close(ctx))
}
