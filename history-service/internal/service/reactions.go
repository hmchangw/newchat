package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/displayfmt"
	"github.com/hmchangw/chat/pkg/emoji"
	"github.com/hmchangw/chat/pkg/errcode"
	pkgmodel "github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/preview"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/userstore"
)

// ReactMessage toggles one (emoji, user_account) reaction. Adding to a soft-deleted
// message is rejected; removing is always allowed. Publishes the canonical event after.
func (s *HistoryService) ReactMessage(c *natsrouter.Context, siteID string, req models.ReactMessageRequest) (*models.ReactMessageResponse, error) {
	account := c.Param("account")
	roomID := c.Param("roomID")

	if strings.TrimSpace(req.MessageID) == "" {
		return nil, errcode.BadRequest("messageId is required")
	}
	if req.Shortcode == "" {
		return nil, errcode.BadRequest("shortcode is required")
	}

	// Reactions accept any emoji (no support check): CanonicalizeReaction only
	// NFC-normalizes and caps size, so a raw-unicode or Teams-migrated emoji isn't
	// rejected — the FE decides renderability. Its only error is an oversized blob,
	// already a friendly client-facing BadRequest, so return it as-is.
	shortcode, err := emoji.CanonicalizeReaction(req.Shortcode)
	if err != nil {
		return nil, err
	}
	// Use the canonicalized shortcode from here on; req.Shortcode is raw input.

	if _, err := s.getAccessSince(c, account, roomID); err != nil {
		return nil, err
	}

	msg, err := s.findMessage(c, roomID, req.MessageID)
	if err != nil {
		return nil, err
	}

	actor, err := s.users.FindUserByAccount(c, account)
	if err != nil {
		if errors.Is(err, userstore.ErrUserNotFound) {
			slog.WarnContext(c, "react: actor not found", "account", account)
			return nil, fmt.Errorf("react: actor not found for account %s: %w", account, err)
		}
		return nil, fmt.Errorf("react: resolve actor %s: %w", account, err)
	}

	// In-row map decides add vs remove without an extra read.
	key := models.ReactionKey{Emoji: shortcode, UserAccount: actor.Account}
	_, alreadyReacted := msg.Reactions[key]

	// Block ADD on deleted messages; allow REMOVE so users can clean up.
	if msg.Deleted && !alreadyReacted {
		return nil, errcode.NotFound("message not found")
	}

	reactedAt := time.Now().UTC()
	action := pkgmodel.ReactionActionAdded
	if alreadyReacted {
		action = pkgmodel.ReactionActionRemoved
		if err := s.msgWriter.RemoveReaction(c, msg, key); err != nil {
			return nil, fmt.Errorf("react: remove %s shortcode %s: %w", req.MessageID, shortcode, err)
		}
	} else {
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

	// Bot reactor: prefer the app's display name; degrade to composed on miss/error.
	displayName := s.botAwareDisplayName(c, actor.EngName, actor.ChineseName, actor.Account)

	canonicalEvt := pkgmodel.MessageEvent{
		Event:     pkgmodel.EventReacted,
		Message:   toWireMessage(msg, &reactedAt),
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
				DisplayName: displayName,
			},
		},
	}
	s.publishCanonicalBestEffort(c, siteID, subject.CanonicalReacted, &canonicalEvt)

	return &models.ReactMessageResponse{
		MessageID: req.MessageID,
		Shortcode: shortcode,
		Action:    action,
		ReactedAt: reactedAt.UnixMilli(),
	}, nil
}

// toWireMessage builds the full wire Message for a reaction event; updatedAt is toggle time.
func toWireMessage(msg *cassandra.Message, updatedAt *time.Time) pkgmodel.Message {
	var mentions []pkgmodel.Participant
	if msg.Mentions != nil {
		mentions = make([]pkgmodel.Participant, len(msg.Mentions))
		for i := range msg.Mentions {
			mentions[i] = toWireParticipant(&msg.Mentions[i])
		}
	}
	var pinnedBy *pkgmodel.Participant
	if msg.PinnedBy != nil {
		p := toWireParticipant(msg.PinnedBy)
		pinnedBy = &p
	}
	return pkgmodel.Message{
		ID:                           msg.MessageID,
		RoomID:                       msg.RoomID,
		UserID:                       msg.Sender.ID,
		UserAccount:                  msg.Sender.Account,
		UserDisplayName:              displayfmt.CombineWithFallback(msg.Sender.EngName, msg.Sender.CompanyName, msg.Sender.Account),
		Content:                      msg.Msg,
		Attachments:                  msg.Attachments,
		Card:                         msg.Card,
		CardAction:                   msg.CardAction,
		Mentions:                     mentions,
		CreatedAt:                    msg.CreatedAt,
		EditedAt:                     msg.EditedAt,
		UpdatedAt:                    updatedAt,
		ThreadParentMessageID:        msg.ThreadParentID,
		ThreadParentMessageCreatedAt: msg.ThreadParentCreatedAt,
		TShow:                        msg.TShow,
		Type:                         msg.Type,
		SysMsgData:                   msg.SysMsgData,
		QuotedParentMessage:          msg.QuotedParentMessage,
		PinnedAt:                     msg.PinnedAt,
		PinnedBy:                     pinnedBy,
	}
}

// toWireParticipant maps Cassandra fields to the wire Participant; ChineseName is company_name.
func toWireParticipant(p *cassandra.Participant) pkgmodel.Participant {
	return pkgmodel.Participant{UserID: p.ID, Account: p.Account, EngName: p.EngName, ChineseName: p.CompanyName}
}

// botAwareDisplayName composes a render-ready name, preferring a bot's app name. The
// cached lookup is built once in New; nil here means no app store was wired.
func (s *HistoryService) botAwareDisplayName(ctx context.Context, engName, chineseName, account string) string {
	return preview.BotAwareDisplayName(ctx, s.appName, engName, chineseName, account)
}
