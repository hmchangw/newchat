package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/history-service/internal/service/mocks"
)

func TestResolveRoomTimes(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	last := time.Date(2026, 4, 30, 8, 0, 0, 0, time.UTC)
	created := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	future := now.Add(2 * time.Hour) // beyond +1h skew tolerance

	tsPtr := func(t time.Time) *int64 {
		ms := t.UnixMilli()
		return &ms
	}
	msPtr := func(ms int64) *int64 { return &ms }

	tests := []struct {
		name        string
		meta        *models.RoomMeta
		mongoCalls  int
		wantLast    time.Time
		wantCreated time.Time
	}{
		{name: "no meta → mongo fallback", meta: nil, mongoCalls: 1, wantLast: last, wantCreated: created},
		{name: "both meta valid → no mongo", meta: &models.RoomMeta{LastMsgAt: tsPtr(last), CreatedAt: tsPtr(created)}, mongoCalls: 0, wantLast: last, wantCreated: created},
		{name: "lastMsgAt missing → mongo fallback for both", meta: &models.RoomMeta{CreatedAt: tsPtr(created)}, mongoCalls: 1, wantLast: last, wantCreated: created},
		// Relaxation: a usable lastMsgAt hint alone is sufficient — createdAt only feeds
		// walkBounds' floor (clamped to now-historyFloor), so this must NOT read Mongo;
		// createdAt comes back zero rather than Mongo's value.
		{name: "createdAt missing → lastMsgAt hint alone skips mongo", meta: &models.RoomMeta{LastMsgAt: tsPtr(last)}, mongoCalls: 0, wantLast: last, wantCreated: time.Time{}},
		{name: "lastMsgAt too far in future → ignored", meta: &models.RoomMeta{LastMsgAt: tsPtr(future), CreatedAt: tsPtr(created)}, mongoCalls: 1, wantLast: last, wantCreated: created},
		// Same relaxation as above: lastMsgAt alone is valid, so an out-of-range createdAt
		// hint is simply dropped (zero), not fetched from Mongo.
		{name: "createdAt in future → ignored", meta: &models.RoomMeta{LastMsgAt: tsPtr(last), CreatedAt: tsPtr(future)}, mongoCalls: 0, wantLast: last, wantCreated: time.Time{}},
		{name: "implausibly old values (pre-2020) → ignored", meta: &models.RoomMeta{LastMsgAt: msPtr(0), CreatedAt: msPtr(0)}, mongoCalls: 1, wantLast: last, wantCreated: created},
		// Hint pair is internally inconsistent (createdAt > lastMsgAt). Both meta are
		// individually sane, so they pass sanitization; the consistency-refetch path
		// kicks in and replaces both with Mongo's coherent values.
		{name: "createdAt > lastMsgAt → mongo refetch", meta: &models.RoomMeta{LastMsgAt: tsPtr(created), CreatedAt: tsPtr(last)}, mongoCalls: 1, wantLast: last, wantCreated: created},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockResolver := mocks.NewMockRoomRepository(ctrl)
			mockResolver.EXPECT().
				GetRoomTimes(gomock.Any(), "room-1").
				Return(last, created, nil).
				Times(tc.mongoCalls)

			s := &HistoryService{rooms: mockResolver, roomTimes: nopRoomTimesCache{}}
			gotLast, gotCreated, err := s.resolveRoomTimes(context.Background(), "room-1", tc.meta, now)
			require.NoError(t, err)
			assert.Equal(t, tc.wantLast.UTC(), gotLast.UTC())
			assert.Equal(t, tc.wantCreated.UTC(), gotCreated.UTC())
		})
	}
}

// Mongo has no lastMsgAt recorded (zero) — "unknown", NOT "empty room": the room
// may hold messages (legacy docs, failed lastMsgAt update). The resolver must
// return the zero untouched rather than collapsing it to createdAt, which would
// drag the consistency logic below into a false inversion.
func TestResolveRoomTimes_MissingLastMsgAt_StaysUnknown(t *testing.T) {
	now := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	created := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)

	ctrl := gomock.NewController(t)
	mockResolver := mocks.NewMockRoomRepository(ctrl)
	mockResolver.EXPECT().
		GetRoomTimes(gomock.Any(), "room-1").
		Return(time.Time{}, created, nil).
		Times(1)

	s := &HistoryService{rooms: mockResolver, roomTimes: nopRoomTimesCache{}}
	gotLast, gotCreated, err := s.resolveRoomTimes(context.Background(), "room-1", nil, now)
	require.NoError(t, err)
	assert.True(t, gotLast.IsZero(), "missing lastMsgAt must stay zero, got %v", gotLast)
	assert.Equal(t, created, gotCreated.UTC())
}

// A createdAt hint on a no-lastMsgAt room triggers the consistency refetch
// (hint created > zero last); after the refetch the zero must still survive.
func TestResolveRoomTimes_MissingLastMsgAt_CreatedHintRefetchKeepsUnknown(t *testing.T) {
	now := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	created := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	hintMs := now.Add(-30 * 24 * time.Hour).UnixMilli()

	ctrl := gomock.NewController(t)
	mockResolver := mocks.NewMockRoomRepository(ctrl)
	mockResolver.EXPECT().
		GetRoomTimes(gomock.Any(), "room-1").
		Return(time.Time{}, created, nil).
		Times(2)

	s := &HistoryService{rooms: mockResolver, roomTimes: nopRoomTimesCache{}}
	gotLast, gotCreated, err := s.resolveRoomTimes(context.Background(), "room-1", &models.RoomMeta{CreatedAt: &hintMs}, now)
	require.NoError(t, err)
	assert.True(t, gotLast.IsZero(), "missing lastMsgAt must stay zero after refetch, got %v", gotLast)
	assert.Equal(t, created, gotCreated.UTC())
}

func TestWalkBounds(t *testing.T) {
	now := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	s := &HistoryService{historyFloor: 90 * 24 * time.Hour}
	historyFloor := now.Add(-s.historyFloor)
	created := now.Add(-10 * 24 * time.Hour)

	tests := []struct {
		name                   string
		createdAt              time.Time
		wantCeiling, wantFloor time.Time
	}{
		{name: "ceiling is always now+skew", createdAt: created, wantCeiling: now.Add(clockSkewTolerance), wantFloor: created},
		{name: "zero createdAt → floor historyFloor", createdAt: time.Time{}, wantCeiling: now.Add(clockSkewTolerance), wantFloor: historyFloor},
		{name: "createdAt older than floor → clamped", createdAt: now.Add(-200 * 24 * time.Hour), wantCeiling: now.Add(clockSkewTolerance), wantFloor: historyFloor},
		{name: "createdAt in the future → still a legal range", createdAt: now.Add(24 * time.Hour), wantCeiling: now.Add(clockSkewTolerance), wantFloor: now.Add(24 * time.Hour)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ceiling, floor := s.walkBounds(tc.createdAt, now)
			assert.Equal(t, tc.wantCeiling, ceiling)
			assert.Equal(t, tc.wantFloor, floor)
		})
	}
}

func TestResolveRoomTimes_MongoError(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	wantErr := errors.New("mongo down")

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockResolver := mocks.NewMockRoomRepository(ctrl)
	mockResolver.EXPECT().
		GetRoomTimes(gomock.Any(), "room-1").
		Return(time.Time{}, time.Time{}, wantErr).
		Times(1)

	s := &HistoryService{rooms: mockResolver, roomTimes: nopRoomTimesCache{}}
	_, _, err := s.resolveRoomTimes(context.Background(), "room-1", nil, now)
	require.Error(t, err)
	assert.ErrorIs(t, err, wantErr, "wrapped mongo error must propagate via errors.Is")
}

// Fail-open exists so a MongoDB outage cannot block a read. A cancelled or
// timed-out CALLER is not that: the client is gone, and widening the walk to
// now/floor sends the request on to do a year of Cassandra bucket reads for a
// response nobody will receive. Cancellation must surface as the error it is.
func TestResolveRoomTimesOrError_CancelledContextDoesNotFailOpen(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name    string
		ctxErr  error
		fromCtx func() (context.Context, context.CancelFunc)
	}{
		{
			name:   "caller cancelled",
			ctxErr: context.Canceled,
			fromCtx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, func() {}
			},
		},
		{
			name:   "caller deadline exceeded",
			ctxErr: context.DeadlineExceeded,
			fromCtx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				return ctx, cancel
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockResolver := mocks.NewMockRoomRepository(ctrl)
			mockResolver.EXPECT().
				GetRoomTimes(gomock.Any(), "room-1").
				Return(time.Time{}, time.Time{}, tt.ctxErr).
				AnyTimes()

			ctx, cancel := tt.fromCtx()
			defer cancel()

			s := &HistoryService{rooms: mockResolver, roomTimes: nopRoomTimesCache{}}
			got, err := s.resolveRoomTimesOrError(ctx, "room-1", nil, now)

			require.Error(t, err, "a cancelled caller must not be served a degraded success")
			assert.ErrorIs(t, err, tt.ctxErr)
			assert.True(t, got.createdAt.IsZero(), "no walk bounds for a request nobody is waiting on")
		})
	}
}

// A genuine Mongo failure must still fail open — that is the whole point.
func TestResolveRoomTimesOrError_MongoFailureStillFailsOpen(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockResolver := mocks.NewMockRoomRepository(ctrl)
	mockResolver.EXPECT().
		GetRoomTimes(gomock.Any(), "room-1").
		Return(time.Time{}, time.Time{}, errors.New("mongo down")).
		Times(1)

	s := &HistoryService{rooms: mockResolver, roomTimes: nopRoomTimesCache{}}
	got, err := s.resolveRoomTimesOrError(context.Background(), "room-1", nil, now)

	require.NoError(t, err, "an outage must not block the read")
	assert.True(t, got.createdAt.IsZero())
}

// --- L2 room-times fallback ------------------------------------------------

// fakeRoomTimesTier records what the service stored and serves what a prior
// outage-free read would have left behind.
type fakeRoomTimesTier struct {
	stored    map[string]time.Time
	fallback  map[string]time.Time
	fallbacks int
}

func newFakeRoomTimesTier() *fakeRoomTimesTier {
	return &fakeRoomTimesTier{stored: map[string]time.Time{}, fallback: map[string]time.Time{}}
}

func (f *fakeRoomTimesTier) Store(_ context.Context, roomID string, createdAt time.Time) {
	f.stored[roomID] = createdAt
}

func (f *fakeRoomTimesTier) Fallback(_ context.Context, roomID string) (time.Time, bool) {
	f.fallbacks++
	v, ok := f.fallback[roomID]
	return v, ok
}

// A confirmed source-of-truth answer is what seeds the tier — that is the only
// write path, so nothing a client said can ever become another client's hint.
func TestResolveRoomTimesOrError_HealthyReadPopulatesTheTier(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	last := now.Add(-30 * 24 * time.Hour)
	created := now.Add(-400 * 24 * time.Hour)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	rooms := mocks.NewMockRoomRepository(ctrl)
	rooms.EXPECT().GetRoomTimes(gomock.Any(), "room-1").Return(last, created, nil).Times(1)

	tier := newFakeRoomTimesTier()
	s := &HistoryService{rooms: rooms, roomTimes: tier}

	got, err := s.resolveRoomTimesOrError(context.Background(), "room-1", nil, now)

	require.NoError(t, err)
	assert.Equal(t, created, got.createdAt)
	assert.Equal(t, created, tier.stored["room-1"], "only the immutable time is cacheable")
}

// A client-supplied hint short-circuits the Mongo read. It must not reach the
// shared tier: a bogus lastMsgAt would become another reader's skip hint and
// could make their walk jump past real messages.
func TestResolveRoomTimesOrError_ClientHintNeverPopulatesTheTier(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	hintMs := now.Add(-time.Hour).UnixMilli()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	rooms := mocks.NewMockRoomRepository(ctrl)
	rooms.EXPECT().GetRoomTimes(gomock.Any(), gomock.Any()).Times(0)

	tier := newFakeRoomTimesTier()
	s := &HistoryService{rooms: rooms, roomTimes: tier}

	_, err := s.resolveRoomTimesOrError(context.Background(), "room-1",
		&models.RoomMeta{LastMsgAt: &hintMs}, now)

	require.NoError(t, err)
	assert.Empty(t, tier.stored, "the tier is seeded from the source of truth only")
}

// The heart of it: during an outage the cached createdAt bounds the floor and
// the ceiling stays unknown, so walkBounds widens it to now — messages written
// during the outage sit under that ceiling and must still be reachable.
//
// The cached lastMsgAt is deliberately not consulted for anything. It is frozen
// for the outage while messages keep landing in Cassandra, so it is neither a
// ceiling nor a skip hint; see pkg/roomtimescache's package doc.
func TestResolveRoomTimesOrError_DegradedUsesTheCachedCreatedAtAsFloor(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	cachedCreated := now.Add(-400 * 24 * time.Hour)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	rooms := mocks.NewMockRoomRepository(ctrl)
	rooms.EXPECT().GetRoomTimes(gomock.Any(), "room-1").
		Return(time.Time{}, time.Time{}, errors.New("mongo down")).Times(1)

	tier := newFakeRoomTimesTier()
	tier.fallback["room-1"] = cachedCreated
	s := &HistoryService{rooms: rooms, roomTimes: tier}

	got, err := s.resolveRoomTimesOrError(context.Background(), "room-1", nil, now)

	require.NoError(t, err, "an outage must not block the read")
	assert.Equal(t, cachedCreated, got.createdAt, "createdAt is immutable, so it is safe as the floor")
}

func TestResolveRoomTimesOrError_DegradedWithNoCachedEntryIsUnchanged(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	rooms := mocks.NewMockRoomRepository(ctrl)
	rooms.EXPECT().GetRoomTimes(gomock.Any(), "room-1").
		Return(time.Time{}, time.Time{}, errors.New("mongo down")).Times(1)

	tier := newFakeRoomTimesTier()
	s := &HistoryService{rooms: rooms, roomTimes: tier}

	got, err := s.resolveRoomTimesOrError(context.Background(), "room-1", nil, now)

	require.NoError(t, err)
	assert.True(t, got.createdAt.IsZero())
}

// A cancelled caller is not served a degraded walk, and must not spend a Valkey
// round trip on a hint for a response nobody is waiting for.
func TestResolveRoomTimesOrError_CancelledCallerSkipsTheFallback(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	rooms := mocks.NewMockRoomRepository(ctrl)
	rooms.EXPECT().GetRoomTimes(gomock.Any(), "room-1").
		Return(time.Time{}, time.Time{}, context.Canceled).AnyTimes()

	tier := newFakeRoomTimesTier()
	s := &HistoryService{rooms: rooms, roomTimes: tier}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := s.resolveRoomTimesOrError(ctx, "room-1", nil, now)

	require.Error(t, err)
	assert.Zero(t, tier.fallbacks)
}
