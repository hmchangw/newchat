package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/displayfmt"
	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/subject"
)

type (
	BotSendRoomRequest = model.BotSendMessageRequest
	BotSendResponse    = model.BotSendResponse
	BotIdentity        = model.BotIdentity
)

const publishTimeout = 2 * time.Second

// Publisher is the narrow JetStream publish surface; tests substitute a fake.
// MsgID is a first-class parameter so the fake can assert on it (jetstream.pubOpts is unexported).
type Publisher interface {
	PublishWithMsgID(ctx context.Context, subj string, data []byte, msgID string) (*jetstream.PubAck, error)
}

type jetStreamAPI interface {
	Publish(ctx context.Context, subj string, data []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error)
}

// JetStreamPublisher adapts o11ynats.JetStream to Publisher.
type JetStreamPublisher struct{ JS jetStreamAPI }

func (j JetStreamPublisher) PublishWithMsgID(ctx context.Context, subj string, data []byte, msgID string) (*jetstream.PubAck, error) {
	return j.JS.Publish(ctx, subj, data, jetstream.WithMsgID(msgID))
}

type handler struct {
	store  Store
	pub    Publisher
	siteID string
}

func newHandler(store Store, pub Publisher, siteID string) *handler {
	return &handler{store: store, pub: pub, siteID: siteID}
}

// verifyRoomExists is a defence-in-depth local-Mongo check.
// BP routes to the room's home site, so a miss indicates a dangling subscription.
func (h *handler) verifyRoomExists(ctx context.Context, roomID string) error {
	if _, err := h.store.FindRoom(ctx, roomID); err != nil {
		if errors.Is(err, ErrNotFound) {
			return errcode.NotFound("room not found",
				errcode.WithReason(errcode.BotRoomNotFound))
		}
		return fmt.Errorf("find local room: %w", err)
	}
	return nil
}

// Register attaches send-in-room + send-DM routes.
func (h *handler) Register(r *natsrouter.Router) {
	natsrouter.Register[BotSendRoomRequest, BotSendResponse](r,
		subject.ServerBotMsgRoomSendPattern(h.siteID), h.handleSendRoom)
	natsrouter.Register[BotSendRoomRequest, BotSendResponse](r,
		subject.ServerBotDMSendPattern(h.siteID), h.handleSendDM)
}

// handleSendDM sends to a DM room. BP has already ensured the room exists
// (calling bot-room-service.dm.ensure on first-DM before forwarding), so the
// subscription check here is defence-in-depth and never triggers DM-ensure.
func (h *handler) handleSendDM(c *natsrouter.Context, req BotSendRoomRequest) (*BotSendResponse, error) { //nolint:gocritic // hugeParam: natsrouter contract
	targetUserID := c.Params.Get("userID")
	if targetUserID == "" {
		return nil, errcode.BadRequest("target userID missing from subject")
	}
	ident, err := parseIdentity(c.Msg.Header)
	if err != nil {
		return nil, err
	}
	messageID, createdAt, err := parseHeaderIDs(c.Msg.Header)
	if err != nil {
		return nil, err
	}

	roomID := idgen.BuildDMRoomID(ident.ID, targetUserID)

	if _, err := h.store.FindSubscription(c, roomID, ident.ID); err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, errcode.Forbidden("bot has no DM subscription — BP must ensure first",
				errcode.WithReason(errcode.BotNotARoomMember))
		}
		return nil, fmt.Errorf("find dm subscription: %w", err)
	}

	if err := validateContent(req.Content); err != nil {
		return nil, err
	}
	mentions, err := h.canonicalizeMentions(c, roomID, req.Mentions)
	if err != nil {
		return nil, err
	}

	msg := model.Message{
		ID: messageID, RoomID: roomID,
		UserID: ident.ID, UserAccount: ident.Account,
		UserDisplayName: displayfmt.CombineWithFallback(ident.EngName, ident.ChineseName, ident.Account),
		Content:         req.Content, Card: req.Card, Mentions: mentions,
		CreatedAt:                    createdAt,
		ThreadParentMessageID:        req.ThreadParentMessageID,
		ThreadParentMessageCreatedAt: req.ThreadParentMessageCreatedAt,
		TShow:                        req.TShow && req.ThreadParentMessageID != "",
	}
	if err := h.publishCanonical(c, &msg); err != nil {
		return nil, err
	}
	return &BotSendResponse{Message: msg}, nil
}

// handleSendRoom sends into an existing room.
// BP has already routed to the room's home site, so subscription/room checks are defence-in-depth.
func (h *handler) handleSendRoom(c *natsrouter.Context, req BotSendRoomRequest) (*BotSendResponse, error) { //nolint:gocritic // hugeParam: natsrouter.Register signature requires the request by value
	roomID := c.Params.Get("roomID")
	if roomID == "" {
		return nil, errcode.BadRequest("roomID missing from subject")
	}

	ident, err := parseIdentity(c.Msg.Header)
	if err != nil {
		return nil, err
	}
	messageID, createdAt, err := parseHeaderIDs(c.Msg.Header)
	if err != nil {
		return nil, err
	}

	if _, err := h.store.FindSubscription(c, roomID, ident.ID); err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, errcode.Forbidden("bot is not a room member",
				errcode.WithReason(errcode.BotNotARoomMember))
		}
		return nil, fmt.Errorf("find subscription: %w", err)
	}

	if err := h.verifyRoomExists(c, roomID); err != nil {
		return nil, err
	}

	if err := validateContent(req.Content); err != nil {
		return nil, err
	}

	mentions, err := h.canonicalizeMentions(c, roomID, req.Mentions)
	if err != nil {
		return nil, err
	}

	msg := model.Message{
		ID:                           messageID,
		RoomID:                       roomID,
		UserID:                       ident.ID,
		UserAccount:                  ident.Account,
		UserDisplayName:              displayfmt.CombineWithFallback(ident.EngName, ident.ChineseName, ident.Account),
		Content:                      req.Content,
		Card:                         req.Card,
		Mentions:                     mentions,
		CreatedAt:                    createdAt,
		ThreadParentMessageID:        req.ThreadParentMessageID,
		ThreadParentMessageCreatedAt: req.ThreadParentMessageCreatedAt,
		TShow:                        req.TShow && req.ThreadParentMessageID != "",
	}

	if err := h.publishCanonical(c, &msg); err != nil {
		return nil, err
	}

	return &BotSendResponse{Message: msg}, nil
}

// publishCanonical wraps msg in the shared MessageEvent envelope and publishes to BOT_MESSAGES_CANONICAL.
func (h *handler) publishCanonical(ctx context.Context, msg *model.Message) error {
	evt := model.MessageEvent{
		Event:     model.EventCreated,
		Message:   *msg,
		SiteID:    h.siteID,
		Timestamp: msg.CreatedAt.UnixMilli(),
	}
	data, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal canonical: %w", err)
	}
	pubCtx, cancel := context.WithTimeout(ctx, publishTimeout)
	defer cancel()
	if _, err := h.pub.PublishWithMsgID(pubCtx, subject.BotCanonicalCreated(h.siteID), data, msg.ID); err != nil {
		return errcode.Internal("publish canonical", errcode.WithCause(err))
	}
	return nil
}

// parseIdentity decodes the X-Bot-Identity header.
// Missing/malformed is a BP wiring bug (only BP can publish here), so the envelope surfaces it.
func parseIdentity(h nats.Header) (*BotIdentity, error) {
	raw := h.Get(model.HeaderBotIdentity)
	if raw == "" {
		return nil, errcode.BadRequest("missing X-Bot-Identity header",
			errcode.WithReason(errcode.BotInvalidHeader))
	}
	var ident BotIdentity
	if err := json.Unmarshal([]byte(raw), &ident); err != nil {
		return nil, errcode.BadRequest("malformed X-Bot-Identity header",
			errcode.WithReason(errcode.BotInvalidHeader), errcode.WithCause(err))
	}
	if ident.ID == "" || ident.Account == "" {
		return nil, errcode.BadRequest("X-Bot-Identity missing id or account",
			errcode.WithReason(errcode.BotInvalidHeader))
	}
	return &ident, nil
}

// parseHeaderIDs extracts messageID + createdAt (unix ms) from BP-supplied headers.
// Used verbatim so retries within the sentinel window dedupe at the Cassandra PK layer.
func parseHeaderIDs(h nats.Header) (string, time.Time, error) {
	messageID := h.Get(model.HeaderBotMessageID)
	if !idgen.IsValidMessageID(messageID) {
		return "", time.Time{}, errcode.BadRequest("missing or malformed X-Bot-Message-ID",
			errcode.WithReason(errcode.BotInvalidHeader))
	}
	createdAtRaw := h.Get(model.HeaderBotCreatedAt)
	if createdAtRaw == "" {
		return "", time.Time{}, errcode.BadRequest("missing X-Bot-Created-At header",
			errcode.WithReason(errcode.BotInvalidHeader))
	}
	ms, err := strconv.ParseInt(createdAtRaw, 10, 64)
	if err != nil {
		return "", time.Time{}, errcode.BadRequest("malformed X-Bot-Created-At (want unix ms)",
			errcode.WithReason(errcode.BotInvalidHeader), errcode.WithCause(err))
	}
	return messageID, time.UnixMilli(ms).UTC(), nil
}

// validateContent enforces 1 <= len(content) <= BotContentMaxBytes.
func validateContent(content string) error {
	if len(content) == 0 {
		return errcode.BadRequest("content is required",
			errcode.WithReason(errcode.BotContentInvalid))
	}
	if len(content) > model.BotContentMaxBytes {
		return errcode.BadRequest(
			fmt.Sprintf("content exceeds %d-byte limit (got %d)", model.BotContentMaxBytes, len(content)),
			errcode.WithReason(errcode.BotContentInvalid))
	}
	return nil
}

// canonicalizeMentions replaces client-supplied mention fields with server-authoritative user data.
// Only UserID is trusted from the request; non-member mentions reject the whole request.
func (h *handler) canonicalizeMentions(ctx context.Context, roomID string, requested []model.Participant) ([]model.Participant, error) {
	if len(requested) == 0 {
		return nil, nil
	}
	memberIDs, err := h.store.ListMemberIDs(ctx, roomID)
	if err != nil {
		return nil, fmt.Errorf("list room members: %w", err)
	}
	members := make(map[string]struct{}, len(memberIDs))
	for _, id := range memberIDs {
		members[id] = struct{}{}
	}
	out := make([]model.Participant, 0, len(requested))
	for _, m := range requested {
		if m.UserID == "" {
			return nil, errcode.BadRequest("mention missing userId",
				errcode.WithReason(errcode.BotMentionInvalid))
		}
		if _, ok := members[m.UserID]; !ok {
			return nil, errcode.BadRequest(
				fmt.Sprintf("mention %s is not a room member", m.UserID),
				errcode.WithReason(errcode.BotMentionInvalid))
		}
		u, err := h.store.FindUser(ctx, m.UserID)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				return nil, errcode.BadRequest(
					fmt.Sprintf("mention %s not found", m.UserID),
					errcode.WithReason(errcode.BotMentionInvalid))
			}
			return nil, fmt.Errorf("find mention user: %w", err)
		}
		out = append(out, model.Participant{
			UserID:      u.ID,
			Account:     u.Account,
			SiteID:      u.SiteID,
			EngName:     u.EngName,
			ChineseName: u.ChineseName,
			DisplayName: displayfmt.CombineWithFallback(u.EngName, u.ChineseName, u.Account),
		})
	}
	return out, nil
}
