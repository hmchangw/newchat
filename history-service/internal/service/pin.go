package service

import (
	"fmt"
	"regexp"
	"time"

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/subject"
)

// botPattern matches bot account names. Equivalent rules live in room-service,
// room-worker (regexp), broadcast-worker, message-gatekeeper, inbox-worker (HasSuffix/HasPrefix), pkg/pipelines (bson regex) — keep all in sync.
var botPattern = regexp.MustCompile(`\.bot$|^p_`)

func isBot(account string) bool { return botPattern.MatchString(account) }

// canBypassLargeRoomPin: owners, admins, bots bypass the large-room pin restriction.
func canBypassLargeRoomPin(sub *model.Subscription) bool {
	for _, r := range sub.Roles {
		if r == model.RoleOwner || r == model.RoleAdmin {
			return true
		}
	}
	return isBot(sub.User.Account)
}

// pinPreCheck: kill-switch → subscription → findMessage. PinMessage rejects msg.Deleted;
// UnpinMessage intentionally accepts it so a soft-deleted pin can still be unpinned to free its slot.
func (s *HistoryService) pinPreCheck(c *natsrouter.Context, account, roomID, messageID string) (*models.Message, *model.Subscription, error) {
	if !s.pinEnabled {
		return nil, nil, errcode.Forbidden("pinning is disabled", errcode.WithReason(errcode.PinDisabled))
	}

	sub, err := s.subscriptions.GetSubscription(c, account, roomID)
	if err != nil {
		return nil, nil, fmt.Errorf("get subscription: %w", err)
	}
	if sub == nil {
		return nil, nil, errcode.Forbidden("not subscribed to room", errcode.WithReason(errcode.MessageNotSubscribed))
	}

	msg, err := s.findMessage(c, roomID, messageID)
	if err != nil {
		return nil, nil, err
	}

	return msg, sub, nil
}

// enforcePinLimit caps per-room pins; reuses the partition read to detect a
// half-applied prior pin so the retry can reuse its pinned_at (bypass the cap).
func (s *HistoryService) enforcePinLimit(c *natsrouter.Context, roomID, messageID string) (*time.Time, error) {
	pinned, err := s.msgReader.GetAllPinnedMessages(c, roomID)
	if err != nil {
		return nil, fmt.Errorf("count pinned messages: %w", err)
	}
	for i := range pinned {
		if pinned[i].MessageID == messageID {
			return pinned[i].PinnedAt, nil
		}
	}
	if len(pinned) >= s.maxPinnedPerRoom {
		return nil, errcode.Forbidden("room pin limit reached", errcode.WithReason(errcode.PinLimitReached))
	}
	return nil, nil
}

// enforceLargeRoomPin gates pin/unpin in large rooms for non-bypass members.
func (s *HistoryService) enforceLargeRoomPin(c *natsrouter.Context, roomID string, sub *model.Subscription) error {
	if !canBypassLargeRoomPin(sub) {
		count, err := s.rooms.GetRoomUserCount(c, roomID)
		if err != nil {
			return fmt.Errorf("get room user count: %w", err)
		}
		if count > s.largeRoomThreshold {
			return errcode.Forbidden("room is too large to pin", errcode.WithReason(errcode.PinRoomTooLarge))
		}
	}
	return nil
}

// PinMessage handles chat.user.{account}.request.room.{roomID}.{siteID}.msg.pin.
func (s *HistoryService) PinMessage(c *natsrouter.Context, siteID string, req models.PinMessageRequest) (*models.PinMessageResponse, error) {
	account := c.Param("account")
	roomID := c.Param("roomID")
	c.WithLogValues("account", account, "room_id", roomID)

	msg, sub, err := s.pinPreCheck(c, account, roomID, req.MessageID)
	if err != nil {
		return nil, err
	}
	if msg.Deleted {
		return nil, errcode.NotFound("message not found")
	}

	// Already pinned: echo existing pinnedAt, no write/publish/large-room check.
	if msg.PinnedAt != nil {
		return &models.PinMessageResponse{MessageID: msg.MessageID, PinnedAt: msg.PinnedAt.UnixMilli()}, nil
	}

	if err := s.enforceLargeRoomPin(c, roomID, sub); err != nil {
		return nil, err
	}

	orphanPinnedAt, err := s.enforcePinLimit(c, roomID, req.MessageID)
	if err != nil {
		return nil, err
	}

	// Orphan reuse: INSERT becomes idempotent UPSERT on the existing clustering key.
	pinnedAt := time.Now().UTC()
	if orphanPinnedAt != nil {
		pinnedAt = *orphanPinnedAt
	}
	pinnedBy := models.Participant{ID: sub.User.ID, Account: sub.User.Account}
	if err := s.msgWriter.PinMessage(c, msg, pinnedAt, pinnedBy); err != nil {
		return nil, fmt.Errorf("pin message %s: %w", req.MessageID, err)
	}

	pinnedAtMs := pinnedAt.UnixMilli()
	evt := model.MessageEvent{
		Event: model.EventPinned,
		Message: model.Message{
			ID:          msg.MessageID,
			RoomID:      msg.RoomID,
			UserID:      msg.Sender.ID,
			UserAccount: msg.Sender.Account,
			CreatedAt:   msg.CreatedAt,
			PinnedAt:    &pinnedAt,
			PinnedBy:    &model.Participant{UserID: sub.User.ID, Account: sub.User.Account},
		},
		SiteID:    siteID,
		Timestamp: pinnedAtMs,
	}
	s.publishCanonicalBestEffort(c, subject.MsgCanonicalPinned(siteID), &evt)

	return &models.PinMessageResponse{MessageID: msg.MessageID, PinnedAt: pinnedAtMs}, nil
}

// UnpinMessage handles ...msg.unpin. Accepts deleted messages: soft-deleted pins still occupy a slot.
func (s *HistoryService) UnpinMessage(c *natsrouter.Context, siteID string, req models.UnpinMessageRequest) (*models.UnpinMessageResponse, error) {
	account := c.Param("account")
	roomID := c.Param("roomID")
	c.WithLogValues("account", account, "room_id", roomID)

	msg, sub, err := s.pinPreCheck(c, account, roomID, req.MessageID)
	if err != nil {
		return nil, err
	}

	// Not pinned: no-op success, no write/publish/large-room check.
	if msg.PinnedAt == nil {
		return &models.UnpinMessageResponse{MessageID: msg.MessageID}, nil
	}

	if err := s.enforceLargeRoomPin(c, roomID, sub); err != nil {
		return nil, err
	}

	if err := s.msgWriter.UnpinMessage(c, msg); err != nil {
		return nil, fmt.Errorf("unpin message %s: %w", req.MessageID, err)
	}

	evt := model.MessageEvent{
		Event: model.EventUnpinned,
		Message: model.Message{
			ID:          msg.MessageID,
			RoomID:      msg.RoomID,
			UserID:      msg.Sender.ID,
			UserAccount: msg.Sender.Account,
			CreatedAt:   msg.CreatedAt,
			PinnedBy:    &model.Participant{UserID: sub.User.ID, Account: sub.User.Account},
		},
		SiteID:    siteID,
		Timestamp: time.Now().UTC().UnixMilli(),
	}
	s.publishCanonicalBestEffort(c, subject.MsgCanonicalUnpinned(siteID), &evt)

	return &models.UnpinMessageResponse{MessageID: msg.MessageID}, nil
}

// ListPinnedMessages handles ...msg.pinned.list. Subscription-gated; cursor-paginated.
func (s *HistoryService) ListPinnedMessages(c *natsrouter.Context, req models.ListPinnedMessagesRequest) (*models.ListPinnedMessagesResponse, error) {
	account := c.Param("account")
	roomID := c.Param("roomID")
	c.WithLogValues("account", account, "room_id", roomID)

	accessSince, err := s.getAccessSince(c, account, roomID)
	if err != nil {
		return nil, err
	}

	pageReq, err := parsePageRequest(req.Cursor, req.Limit)
	if err != nil {
		return nil, err
	}

	page, err := s.msgReader.GetPinnedMessages(c, roomID, pageReq)
	if err != nil {
		return nil, fmt.Errorf("list pinned messages: %w", err)
	}

	// Stub pre-access pins, then stub pre-access quoted parents inside survivors.
	redactUnavailablePins(page.Data, accessSince)
	redactUnavailableQuotes(page.Data, accessSince)
	setDecodedAttachments(c, page.Data)

	return &models.ListPinnedMessagesResponse{
		Messages:   page.Data,
		NextCursor: page.NextCursor,
		HasNext:    page.HasNext,
	}, nil
}

// pinInaccessible: thread replies also gate on parent's createdAt (nil → redact conservatively).
func pinInaccessible(m *models.Message, accessSince time.Time) bool {
	if m.CreatedAt.Before(accessSince) {
		return true
	}
	if m.ThreadParentID != "" {
		if m.ThreadParentCreatedAt == nil || m.ThreadParentCreatedAt.Before(accessSince) {
			return true
		}
	}
	return false
}

// redactUnavailablePins blanks rich content on pins outside accessSince (identifiers/sender/timestamps stay for placeholder rendering).
func redactUnavailablePins(pinned []models.Message, accessSince *time.Time) {
	if accessSince == nil {
		return
	}
	for i := range pinned {
		if !pinInaccessible(&pinned[i], *accessSince) {
			continue
		}
		pinned[i].Msg = UnavailableQuoteMsg
		pinned[i].Mentions = nil
		pinned[i].Attachments = nil
		pinned[i].DecodedAttachments = nil
		pinned[i].Card = nil
		pinned[i].CardAction = nil
		pinned[i].QuotedParentMessage = nil
		pinned[i].Reactions = nil
		// System messages carry event metadata in Type/SysMsgData (e.g.
		// "user_joined" with a payload); scrub both so pre-access system
		// pins don't leak event details past the placeholder.
		pinned[i].Type = ""
		pinned[i].SysMsgData = nil
	}
}
