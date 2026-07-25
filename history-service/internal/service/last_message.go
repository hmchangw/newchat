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

// GetLastRoomMessage is the server-to-server last-room-message RPC (no access gate —
// trusted callers). NATS: chat.server.request.msg.{siteID}.room.last
func (s *HistoryService) GetLastRoomMessage(c *natsrouter.Context, req pkgmodel.LastRoomMessageRequest) (*pkgmodel.LastRoomMessageResponse, error) {
	if req.RoomID == "" {
		return nil, errcode.BadRequest("roomId is required")
	}
	c.WithLogValues("room_id", req.RoomID)
	now := time.Now().UTC()

	lastMsgAt, createdAt, err := s.resolveRoomTimes(c, req.RoomID, nil, now)
	if err != nil {
		// A room unknown to Mongo has no last message — answer nil, so callers treat it like an empty room.
		if errors.Is(err, mongo.ErrNoDocuments) {
			return &pkgmodel.LastRoomMessageResponse{}, nil
		}
		return nil, fmt.Errorf("resolving room times for %s: %w", req.RoomID, err)
	}

	// Denormalized lastMsgAt can lag coalesced creates, so a caller-supplied Before
	// widens the ceiling. max(), not replace: an old Before must never shrink it. Clamp to
	// now+skew (as sanitizeLastMsgAt does) so a bogus far-future Before can't push the ceiling
	// out and force maxBuckets of empty future-bucket reads.
	if req.Before > 0 {
		t := time.UnixMilli(req.Before).UTC()
		if maxCeiling := now.Add(clockSkewTolerance); t.After(maxCeiling) {
			t = maxCeiling
		}
		if t.After(lastMsgAt) {
			lastMsgAt = t
		}
	}

	ceiling, floor := s.walkBounds(lastMsgAt, createdAt, now)
	// +1ms so the newest row (created_at == lastMsgAt) survives the walker's
	// strict created_at < before predicate — mirrors LoadHistory's cap.
	before := ceiling.Add(time.Millisecond)

	pointer, msg, err := s.msgReader.GetLastRoomMessage(c, req.RoomID, before, floor)
	if err != nil {
		return nil, fmt.Errorf("loading last room message: %w", err)
	}
	resp := &pkgmodel.LastRoomMessageResponse{Pointer: pointer}
	if msg != nil {
		decodeMessageAttachments(c, msg)
		resp.LastMessage = lastMessagePreview(msg)
	}
	return resp, nil
}

// lastMessagePreview projects a row onto the preview shape: SenderName cascades
// EngName → AppName (bots) → Account; body trimmed so full content never leaves the RPC.
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
		Msg:             previewContent(m.Msg),
		CreatedAt:       m.CreatedAt,
		EditedAt:        m.EditedAt,
		AttachmentCount: len(m.DecodedAttachments),
	}
}
