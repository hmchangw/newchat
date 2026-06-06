package service

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/emoji"
	"github.com/hmchangw/chat/pkg/errcode"
	pkgmodel "github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/subject"
)

// ReactMessage toggles one (emoji, user_account) reaction on a message.
// Any subscribed member may react; adding to a soft-deleted message is
// rejected, removing an existing reaction is always allowed. After the
// Cassandra write succeeds the canonical MessageEvent is published
// best-effort for downstream fan-out.
func (s *HistoryService) ReactMessage(c *natsrouter.Context, siteID string, req models.ReactMessageRequest) (*models.ReactMessageResponse, error) {
	account := c.Param("account")
	roomID := c.Param("roomID")

	if strings.TrimSpace(req.MessageID) == "" {
		return nil, errcode.BadRequest("messageId is required")
	}
	if req.Shortcode == "" {
		return nil, errcode.BadRequest("shortcode is required")
	}

	shortcode, err := s.emojiValidator.Validate(c, siteID, req.Shortcode)
	if err != nil {
		if errors.Is(err, emoji.ErrInvalidShortcode) || errors.Is(err, emoji.ErrUnknownShortcode) {
			return nil, errcode.BadRequest("invalid reaction shortcode")
		}
		return nil, fmt.Errorf("react: validate shortcode %q: %w", req.Shortcode, err)
	}
	// From here on, use the validator-returned shortcode (NFC-canonical) for
	// any storage key or wire echo; req.Shortcode is raw input.

	if _, err := s.getAccessSince(c, account, roomID); err != nil {
		return nil, err
	}

	msg, err := s.findMessage(c, roomID, req.MessageID)
	if err != nil {
		return nil, err
	}

	users, err := s.users.FindUsersByAccounts(c, []string{account})
	if err != nil {
		return nil, fmt.Errorf("react: resolve actor %s: %w", account, err)
	}
	if len(users) == 0 {
		slog.WarnContext(c, "react: actor not found", "account", account)
		return nil, fmt.Errorf("react: actor not found for account %s", account)
	}
	actor := users[0]

	// In-row map decides add vs remove without an extra read.
	key := models.ReactionKey{Emoji: shortcode, UserAccount: actor.Account}
	_, alreadyReacted := msg.Reactions[key]

	// Block ADD on deleted messages; allow REMOVE so users can clean up.
	if msg.Deleted && !alreadyReacted {
		return nil, errcode.NotFound("message not found")
	}

	reactedAt := time.Now().UTC()
	var action pkgmodel.ReactionAction
	if alreadyReacted {
		action = pkgmodel.ReactionActionRemoved
		if err := s.msgWriter.RemoveReaction(c, msg, key, reactedAt); err != nil {
			return nil, fmt.Errorf("react: remove %s shortcode %s: %w", req.MessageID, shortcode, err)
		}
	} else {
		action = pkgmodel.ReactionActionAdded
		reactor := models.ReactorInfo{
			UserID:    actor.ID,
			EngName:   actor.EngName,
			ChnName:   actor.ChineseName,
			Account:   actor.Account,
			ReactedAt: reactedAt,
		}
		if err := s.msgWriter.AddReaction(c, msg, key, reactor); err != nil {
			return nil, fmt.Errorf("react: add %s shortcode %s: %w", req.MessageID, shortcode, err)
		}
	}

	canonicalEvt := pkgmodel.MessageEvent{
		Event: pkgmodel.EventReacted,
		Message: pkgmodel.Message{
			ID:          msg.MessageID,
			RoomID:      msg.RoomID,
			UserID:      msg.Sender.ID,
			UserAccount: msg.Sender.Account,
			CreatedAt:   msg.CreatedAt,
			UpdatedAt:   &reactedAt,
		},
		SiteID:    siteID,
		Timestamp: reactedAt.UnixMilli(),
		ReactionDelta: &pkgmodel.ReactionDelta{
			Shortcode: shortcode,
			Action:    action,
			Actor: pkgmodel.Participant{
				UserID:      actor.ID,
				Account:     actor.Account,
				SiteID:      actor.SiteID,
				ChineseName: actor.ChineseName,
				EngName:     actor.EngName,
			},
		},
	}
	s.publishCanonicalBestEffort(c, subject.MsgCanonicalReacted(siteID), &canonicalEvt)

	return &models.ReactMessageResponse{
		MessageID: req.MessageID,
		Shortcode: shortcode,
		Action:    action,
		ReactedAt: reactedAt.UnixMilli(),
	}, nil
}
