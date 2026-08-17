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

			s := &HistoryService{rooms: mockResolver}
			gotLast, gotCreated, err := s.resolveRoomTimes(context.Background(), "room-1", tc.meta, now)
			require.NoError(t, err)
			assert.Equal(t, tc.wantLast.UTC(), gotLast.UTC())
			assert.Equal(t, tc.wantCreated.UTC(), gotCreated.UTC())
		})
	}
}

// Mongo has no lastMsgAt recorded (zero) — "unknown", NOT "empty room": the room
// may hold messages (legacy docs, failed lastMsgAt update). The resolver must
// return the zero untouched so callers apply their unknown-handling (LoadHistory
// skips its before-cap; walkBounds ceilings at now+skew).
func TestResolveRoomTimes_MissingLastMsgAt_StaysUnknown(t *testing.T) {
	now := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	created := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)

	ctrl := gomock.NewController(t)
	mockResolver := mocks.NewMockRoomRepository(ctrl)
	mockResolver.EXPECT().
		GetRoomTimes(gomock.Any(), "room-1").
		Return(time.Time{}, created, nil).
		Times(1)

	s := &HistoryService{rooms: mockResolver}
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

	s := &HistoryService{rooms: mockResolver}
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
	last := now.Add(-time.Hour)

	tests := []struct {
		name                   string
		lastMsgAt, createdAt   time.Time
		wantCeiling, wantFloor time.Time
	}{
		{name: "zero lastMsgAt → ceiling now+skew", lastMsgAt: time.Time{}, createdAt: created, wantCeiling: now.Add(clockSkewTolerance), wantFloor: created},
		{name: "non-zero lastMsgAt → ceiling lastMsgAt", lastMsgAt: last, createdAt: created, wantCeiling: last, wantFloor: created},
		{name: "zero createdAt → floor historyFloor", lastMsgAt: last, createdAt: time.Time{}, wantCeiling: last, wantFloor: historyFloor},
		{name: "createdAt older than floor → clamped", lastMsgAt: last, createdAt: now.Add(-200 * 24 * time.Hour), wantCeiling: last, wantFloor: historyFloor},
		{name: "lastMsgAt below floor → ceiling clamped up to floor", lastMsgAt: now.Add(-100 * 24 * time.Hour), createdAt: time.Time{}, wantCeiling: historyFloor, wantFloor: historyFloor},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ceiling, floor := s.walkBounds(tc.lastMsgAt, tc.createdAt, now)
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

	s := &HistoryService{rooms: mockResolver}
	_, _, err := s.resolveRoomTimes(context.Background(), "room-1", nil, now)
	require.Error(t, err)
	assert.ErrorIs(t, err, wantErr, "wrapped mongo error must propagate via errors.Is")
}
