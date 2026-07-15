package service

import (
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/errcode"
	pkgmodel "github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
)

// GetLastRoomMessage is the server-to-server last-room-message RPC: it returns
// the room's newest non-deleted, non-system message as a LastMessagePreview
// (nil when none qualifies). No account param and no access-window gate —
// callers are trusted services refreshing a room-list preview.
// NATS: chat.server.request.msg.{siteID}.room.last
func (s *HistoryService) GetLastRoomMessage(c *natsrouter.Context, req pkgmodel.LastRoomMessageRequest) (*pkgmodel.LastRoomMessageResponse, error) {
	if req.RoomID == "" {
		return nil, errcode.BadRequest("roomId is required")
	}
	c.WithLogValues("room_id", req.RoomID)
	now := time.Now().UTC()

	lastMsgAt, createdAt, err := s.resolveRoomTimes(c, req.RoomID, nil, now)
	if err != nil {
		// A room unknown to Mongo has no last message — answer nil rather than
		// erroring, so callers treat it like an empty room.
		if errors.Is(err, mongo.ErrNoDocuments) {
			return &pkgmodel.LastRoomMessageResponse{}, nil
		}
		return nil, fmt.Errorf("resolving room times for %s: %w", req.RoomID, err)
	}

	ceiling, floor := s.walkBounds(lastMsgAt, createdAt, now)
	// +1ms so the newest row (created_at == lastMsgAt) survives the walker's
	// strict created_at < before predicate — mirrors LoadHistory's cap.
	before := ceiling.Add(time.Millisecond)

	msg, err := s.msgReader.GetLastRoomMessage(c, req.RoomID, before, floor)
	if err != nil {
		return nil, fmt.Errorf("loading last room message: %w", err)
	}
	if msg == nil {
		return &pkgmodel.LastRoomMessageResponse{}, nil
	}
	decodeMessageAttachments(c, msg)
	return &pkgmodel.LastRoomMessageResponse{LastMessage: lastMessagePreview(msg)}, nil
}

// lastMessagePreview projects a Cassandra row onto the shared preview shape.
// SenderName cascades EngName → AppName (bots) → Account.
func lastMessagePreview(m *models.Message) *pkgmodel.LastMessagePreview {
	name := m.Sender.EngName
	if name == "" {
		name = m.Sender.AppName
	}
	if name == "" {
		name = m.Sender.Account
	}
	return &pkgmodel.LastMessagePreview{
		MessageID:       m.MessageID,
		Type:            m.Type,
		SenderAccount:   m.Sender.Account,
		SenderName:      name,
		Msg:             m.Msg,
		CreatedAt:       m.CreatedAt,
		EditedAt:        m.EditedAt,
		AttachmentCount: len(m.DecodedAttachments),
	}
}
