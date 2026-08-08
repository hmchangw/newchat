package service

import (
	"context"
	"log/slog"

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
)

// enrichForwardedRooms stamps ForwardedMessage.Room (id/name/type from the
// local rooms collection) onto every forwarded snapshot in the given message
// slices, with one batched lookup across all distinct source-room IDs.
// Variadic so callers with several slices (thread replies + parent) share the
// lookup. Best-effort: a lookup error or a missing room doc logs one warning
// and leaves Room nil — a read is never failed by enrichment. dm/botDM
// sources get ID+Type only (the counterpart's name must not leak to readers
// outside the source room).
func (s *HistoryService) enrichForwardedRooms(ctx context.Context, slices ...[]models.Message) {
	var snaps []*models.ForwardedMessage
	var rawIDs []string
	for _, msgs := range slices {
		for i := range msgs {
			fm := msgs[i].ForwardedMessage
			if fm == nil || fm.RoomID == "" {
				continue
			}
			snaps = append(snaps, fm)
			rawIDs = append(rawIDs, fm.RoomID)
		}
	}
	if len(snaps) == 0 {
		return
	}
	ids := dedupRoomIDs(rawIDs)

	rooms, err := s.rooms.GetRoomsNameType(ctx, ids)
	if err != nil {
		slog.Warn("forwarded room enrichment lookup failed",
			"error", err, "request_id", natsutil.RequestIDFromContext(ctx), "room_count", len(ids))
		return
	}
	if missing := len(ids) - len(rooms); missing > 0 {
		slog.Warn("forwarded room enrichment: rooms not found",
			"request_id", natsutil.RequestIDFromContext(ctx), "missing", missing)
	}
	for _, fm := range snaps {
		nt, ok := rooms[fm.RoomID]
		if !ok {
			continue // room deleted — leave Room unset, client falls back to RoomID
		}
		room := &model.MessageRoom{ID: fm.RoomID, Type: nt.Type}
		if nt.Type != model.RoomTypeDM && nt.Type != model.RoomTypeBotDM {
			room.Name = nt.Name
		}
		fm.Room = room
	}
}

// enrichForwardedRoom is the single-message form of enrichForwardedRooms,
// mirroring decodeMessageAttachments; the nil fast-path spares the slice
// wrap on the common not-a-forward case.
func (s *HistoryService) enrichForwardedRoom(ctx context.Context, m *models.Message) {
	if m.ForwardedMessage == nil {
		return
	}
	s.enrichForwardedRooms(ctx, []models.Message{*m})
}
