package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/hmchangw/chat/pkg/mention"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roomcrypto"
	"github.com/hmchangw/chat/pkg/roomkeystore"
	"github.com/hmchangw/chat/pkg/roommetacache"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/userstore"
)

// errNoCurrentKey is returned when a room has no encryption key in Valkey.
var errNoCurrentKey = errors.New("no current key")

// Publisher abstracts NATS publishing so the handler is testable.
type Publisher interface {
	Publish(ctx context.Context, subject string, data []byte) error
}

// RoomKeyProvider fetches the current encryption key for a room.
// Defined here (not imported from pkg/roomkeystore directly) to keep the
// handler's dependency contract narrow — only Get is used.
type RoomKeyProvider interface {
	Get(ctx context.Context, roomID string) (*roomkeystore.VersionedKeyPair, error)
}

// Handler processes MESSAGES_CANONICAL messages and broadcasts room events.
type Handler struct {
	store     Store
	userStore userstore.UserStore
	pub       Publisher
	keyStore  RoomKeyProvider
	encrypt   bool
	encoder   *roomcrypto.Encoder
}

func NewHandler(store Store, userStore userstore.UserStore, pub Publisher, keyStore RoomKeyProvider, encrypt bool) *Handler {
	return &Handler{
		store:     store,
		userStore: userStore,
		pub:       pub,
		keyStore:  keyStore,
		encrypt:   encrypt,
		encoder:   roomcrypto.NewEncoder(),
	}
}

// HandleMessage processes a single MESSAGES_CANONICAL message payload.
func (h *Handler) HandleMessage(ctx context.Context, data []byte) error {
	var evt model.MessageEvent
	if err := json.Unmarshal(data, &evt); err != nil {
		return fmt.Errorf("unmarshal message event: %w", err)
	}

	switch evt.Event {
	case model.EventCreated:
		return h.handleCreated(ctx, &evt)
	case model.EventUpdated:
		return h.handleUpdated(ctx, &evt)
	case model.EventDeleted:
		return h.handleDeleted(ctx, &evt)
	case model.EventPinned:
		return h.handlePinned(ctx, &evt)
	case model.EventUnpinned:
		return h.handleUnpinned(ctx, &evt)
	default:
		slog.Warn("unknown message event type, skipping", "event", evt.Event, "messageID", evt.Message.ID)
		return nil
	}
}

func (h *Handler) handleCreated(ctx context.Context, evt *model.MessageEvent) error {
	msg := evt.Message

	// One user-store round-trip covers both mention enrichment and sender
	// enrichment: parse mentions, dedupe with the sender, fetch once, then
	// hand the resulting map to ResolveFromParsed (skips a second parse) and
	// to buildClientMessage.
	parsed := mention.Parse(msg.Content)
	lookupAccounts := dedupedAccounts(msg.UserAccount, parsed.Accounts)
	users, lookupErr := h.userStore.FindUsersByAccounts(ctx, lookupAccounts)
	if lookupErr != nil {
		slog.Warn("user lookup failed, falling back to account", "error", lookupErr)
	}
	userByAccount := make(map[string]model.User, len(users))
	for i := range users {
		userByAccount[users[i].Account] = users[i]
	}

	resolved := mention.ResolveFromParsed(parsed, userByAccount)

	if err := h.store.UpdateRoomLastMessage(ctx, msg.RoomID, msg.ID, msg.CreatedAt, resolved.MentionAll); err != nil {
		return fmt.Errorf("update room last message %s: %w", msg.RoomID, err)
	}
	meta, err := h.store.GetRoomMeta(ctx, msg.RoomID)
	if err != nil {
		return fmt.Errorf("get room meta %s: %w", msg.RoomID, err)
	}

	if len(resolved.Accounts) > 0 {
		if err := h.store.SetSubscriptionMentions(ctx, meta.ID, resolved.Accounts); err != nil {
			return fmt.Errorf("set subscription mentions: %w", err)
		}
	}

	clientMsg := buildClientMessage(&msg, userByAccount)

	switch meta.Type {
	case model.RoomTypeChannel:
		return h.publishChannelEvent(ctx, meta, clientMsg, resolved.MentionAll, resolved.Participants)
	case model.RoomTypeDM:
		return h.publishDMEvents(ctx, meta, clientMsg, resolved.Accounts)
	default:
		slog.Warn("unknown room type, skipping fan-out", "type", meta.Type, "roomID", meta.ID)
		return nil
	}
}

func (h *Handler) handleUpdated(ctx context.Context, evt *model.MessageEvent) error {
	msg := evt.Message
	if msg.EditedAt == nil || msg.UpdatedAt == nil {
		return fmt.Errorf("updated event missing EditedAt or UpdatedAt: %s", msg.ID)
	}

	room, err := h.store.GetRoom(ctx, msg.RoomID)
	if err != nil {
		return fmt.Errorf("fetch room %s: %w", msg.RoomID, err)
	}

	edit := model.EditRoomEvent{
		Type:       model.RoomEventMessageEdited,
		RoomID:     room.ID,
		SiteID:     room.SiteID,
		Timestamp:  time.Now().UTC().UnixMilli(),
		MessageID:  msg.ID,
		NewContent: msg.Content,
		EditedBy:   msg.UserAccount,
		EditedAt:   *msg.EditedAt,
		UpdatedAt:  *msg.UpdatedAt,
	}
	if room.Type == model.RoomTypeChannel && h.encrypt {
		if err := h.encryptEditedContent(ctx, room.ID, &edit); err != nil {
			return err
		}
	}
	return h.publishMutation(ctx, room, model.RoomEventMessageEdited, msg.ID, &edit)
}

func (h *Handler) handleDeleted(ctx context.Context, evt *model.MessageEvent) error {
	msg := evt.Message
	if msg.UpdatedAt == nil {
		return fmt.Errorf("deleted event missing UpdatedAt: %s", msg.ID)
	}

	room, err := h.store.GetRoom(ctx, msg.RoomID)
	if err != nil {
		return fmt.Errorf("fetch room %s: %w", msg.RoomID, err)
	}

	del := model.DeleteRoomEvent{
		Type:      model.RoomEventMessageDeleted,
		RoomID:    room.ID,
		SiteID:    room.SiteID,
		Timestamp: time.Now().UTC().UnixMilli(),
		MessageID: msg.ID,
		DeletedBy: msg.UserAccount,
		DeletedAt: *msg.UpdatedAt,
		UpdatedAt: *msg.UpdatedAt,
	}
	return h.publishMutation(ctx, room, model.RoomEventMessageDeleted, msg.ID, &del)
}

func (h *Handler) handlePinned(ctx context.Context, evt *model.MessageEvent) error {
	msg := evt.Message
	if msg.PinnedAt == nil {
		return fmt.Errorf("pinned event missing PinnedAt: %s", msg.ID)
	}

	room, err := h.store.GetRoom(ctx, msg.RoomID)
	if err != nil {
		return fmt.Errorf("fetch room %s: %w", msg.RoomID, err)
	}

	pin := model.PinRoomEvent{
		Type:      model.RoomEventMessagePinned,
		RoomID:    room.ID,
		SiteID:    room.SiteID,
		Timestamp: time.Now().UTC().UnixMilli(),
		MessageID: msg.ID,
		PinnedBy:  msg.PinnedBy,
		PinnedAt:  *msg.PinnedAt,
	}
	return h.publishMutation(ctx, room, model.RoomEventMessagePinned, msg.ID, &pin)
}

func (h *Handler) handleUnpinned(ctx context.Context, evt *model.MessageEvent) error {
	msg := evt.Message
	// UnpinnedAt comes from evt.Timestamp (set at publish): the canonical unpin
	// payload from history-service clears PinnedAt, so the message itself
	// carries no unpin timestamp.

	room, err := h.store.GetRoom(ctx, msg.RoomID)
	if err != nil {
		return fmt.Errorf("fetch room %s: %w", msg.RoomID, err)
	}

	unpin := model.UnpinRoomEvent{
		Type:       model.RoomEventMessageUnpinned,
		RoomID:     room.ID,
		SiteID:     room.SiteID,
		Timestamp:  time.Now().UTC().UnixMilli(),
		MessageID:  msg.ID,
		UnpinnedBy: msg.PinnedBy,
		UnpinnedAt: time.UnixMilli(evt.Timestamp).UTC(),
	}
	return h.publishMutation(ctx, room, model.RoomEventMessageUnpinned, msg.ID, &unpin)
}

// publishMutation marshals a flattened edit/delete event and routes it by room
// type: channel events go to the room stream, DM/botDM events fan out per
// non-bot member. evt must marshal to the wire payload for roomEvtType.
func (h *Handler) publishMutation(ctx context.Context, room *model.Room, roomEvtType model.RoomEventType, messageID string, evt any) error {
	payload, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal %s event: %w", roomEvtType, err)
	}

	switch room.Type {
	case model.RoomTypeChannel:
		if err := h.pub.Publish(ctx, subject.RoomEvent(room.ID), payload); err != nil {
			return fmt.Errorf("publish %s event for room %s message %s: %w", roomEvtType, room.ID, messageID, err)
		}
		return nil

	case model.RoomTypeDM, model.RoomTypeBotDM:
		for _, account := range room.Accounts {
			if isBot(account) {
				continue
			}
			if err := h.pub.Publish(ctx, subject.UserRoomEvent(account), payload); err != nil {
				slog.Error("publish DM mutation event failed",
					"error", err,
					"type", roomEvtType,
					"account", account,
					"messageID", messageID,
					"roomID", room.ID,
				)
			}
		}
		return nil

	default:
		slog.Warn("unknown room type, skipping mutation fan-out", "type", room.Type, "roomID", room.ID)
		return nil
	}
}

func (h *Handler) encryptEditedContent(ctx context.Context, roomID string, edited *model.EditRoomEvent) error {
	key, err := h.currentRoomKey(ctx, roomID)
	if err != nil {
		return err
	}
	encrypted, err := h.encoder.Encode(roomID, edited.NewContent, key.KeyPair.PrivateKey, key.Version)
	if err != nil {
		return fmt.Errorf("encrypt edit content for room %s: %w", roomID, err)
	}
	encJSON, err := json.Marshal(encrypted)
	if err != nil {
		return fmt.Errorf("marshal encrypted edit content: %w", err)
	}
	edited.EncryptedNewContent = json.RawMessage(encJSON)
	edited.NewContent = ""
	return nil
}

// currentRoomKey fetches the room's encryption key, treating a missing key as
// an error (the room is configured for encryption but no key is provisioned).
func (h *Handler) currentRoomKey(ctx context.Context, roomID string) (*roomkeystore.VersionedKeyPair, error) {
	key, err := h.keyStore.Get(ctx, roomID)
	if err != nil {
		return nil, fmt.Errorf("get room key for room %s: %w", roomID, err)
	}
	if key == nil {
		return nil, fmt.Errorf("get room key for room %s: %w", roomID, errNoCurrentKey)
	}
	return key, nil
}

func (h *Handler) publishChannelEvent(ctx context.Context, meta roommetacache.Meta, clientMsg *model.ClientMessage, mentionAll bool, mentions []model.Participant) error {
	evt := buildRoomEvent(meta, clientMsg)
	evt.MentionAll = mentionAll
	if len(mentions) > 0 {
		evt.Mentions = mentions
	}

	if h.encrypt {
		msgJSON, err := json.Marshal(clientMsg)
		if err != nil {
			return fmt.Errorf("marshal client message: %w", err)
		}

		key, err := h.currentRoomKey(ctx, meta.ID)
		if err != nil {
			return err
		}

		encrypted, err := h.encoder.Encode(meta.ID, string(msgJSON), key.KeyPair.PrivateKey, key.Version)
		if err != nil {
			return fmt.Errorf("encrypt message for room %s: %w", meta.ID, err)
		}

		encJSON, err := json.Marshal(encrypted)
		if err != nil {
			return fmt.Errorf("marshal encrypted message: %w", err)
		}

		evt.EncryptedMessage = json.RawMessage(encJSON)
		evt.Message = nil
	}
	// when h.encrypt is false, evt.Message is already set by buildRoomEvent

	payload, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal channel event: %w", err)
	}

	return h.pub.Publish(ctx, subject.RoomEvent(meta.ID), payload)
}

func (h *Handler) publishDMEvents(ctx context.Context, meta roommetacache.Meta, clientMsg *model.ClientMessage, mentionedAccounts []string) error {
	subs, err := h.store.ListSubscriptions(ctx, meta.ID)
	if err != nil {
		return fmt.Errorf("list subscriptions for DM room %s: %w", meta.ID, err)
	}

	mentionSet := make(map[string]struct{}, len(mentionedAccounts))
	for _, name := range mentionedAccounts {
		mentionSet[name] = struct{}{}
	}

	for i := range subs {
		_, hasMention := mentionSet[subs[i].User.Account]

		evt := buildRoomEvent(meta, clientMsg)
		evt.HasMention = hasMention

		payload, err := json.Marshal(evt)
		if err != nil {
			return fmt.Errorf("marshal DM event for user %s: %w", subs[i].User.Account, err)
		}
		if err := h.pub.Publish(ctx, subject.UserRoomEvent(subs[i].User.Account), payload); err != nil {
			slog.Error("publish DM event failed", "error", err, "account", subs[i].User.Account)
		}
	}
	return nil
}

func buildRoomEvent(meta roommetacache.Meta, clientMsg *model.ClientMessage) model.RoomEvent {
	return model.RoomEvent{
		Type:      model.RoomEventNewMessage,
		RoomID:    meta.ID,
		Timestamp: time.Now().UTC().UnixMilli(),
		RoomName:  meta.Name,
		RoomType:  meta.Type,
		SiteID:    meta.SiteID,
		UserCount: meta.UserCount,
		LastMsgAt: clientMsg.CreatedAt,
		LastMsgID: clientMsg.ID,
		Message:   clientMsg,
	}
}

func buildClientMessage(msg *model.Message, userMap map[string]model.User) *model.ClientMessage {
	sender := model.Participant{
		UserID:  msg.UserID,
		Account: msg.UserAccount,
	}
	if u, ok := userMap[msg.UserAccount]; ok {
		sender.ChineseName = u.ChineseName
		sender.EngName = u.EngName
	} else {
		sender.ChineseName = msg.UserAccount
		sender.EngName = msg.UserAccount
	}
	return &model.ClientMessage{
		Message: *msg,
		Sender:  &sender,
	}
}
