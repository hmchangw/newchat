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
		{name: "createdAt missing → mongo fallback for both", meta: &models.RoomMeta{LastMsgAt: tsPtr(last)}, mongoCalls: 1, wantLast: last, wantCreated: created},
		{name: "lastMsgAt too far in future → ignored", meta: &models.RoomMeta{LastMsgAt: tsPtr(future), CreatedAt: tsPtr(created)}, mongoCalls: 1, wantLast: last, wantCreated: created},
		{name: "createdAt in future → ignored", meta: &models.RoomMeta{LastMsgAt: tsPtr(last), CreatedAt: tsPtr(future)}, mongoCalls: 1, wantLast: last, wantCreated: created},
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
