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
}

func NewHandler(store Store, userStore userstore.UserStore, pub Publisher, keyStore RoomKeyProvider) *Handler {
	return &Handler{store: store, userStore: userStore, pub: pub, keyStore: keyStore}
}

// HandleMessage processes a single MESSAGES_CANONICAL message payload.
func (h *Handler) HandleMessage(ctx context.Context, data []byte) error {
	var evt model.MessageEvent
	if err := json.Unmarshal(data, &evt); err != nil {
		return fmt.Errorf("unmarshal message event: %w", err)
	}

	msg := evt.Message

	resolved, err := mention.Resolve(ctx, msg.Content, h.userStore.FindUsersByAccounts)
	if err != nil {
		slog.Warn("mention resolve failed", "error", err)
	}

	room, err := h.store.FetchAndUpdateRoom(ctx, msg.RoomID, msg.ID, msg.CreatedAt, resolved.MentionAll)
	if err != nil {
		return fmt.Errorf("fetch and update room %s: %w", msg.RoomID, err)
	}

	if len(resolved.Accounts) > 0 {
		if err := h.store.SetSubscriptionMentions(ctx, room.ID, resolved.Accounts); err != nil {
			return fmt.Errorf("set subscription mentions: %w", err)
		}
	}

	senderMap := make(map[string]model.User)
	senderUsers, err := h.userStore.FindUsersByAccounts(ctx, []string{msg.UserAccount})
	if err != nil {
		slog.Warn("sender lookup failed, falling back to account", "error", err)
	} else {
		for i := range senderUsers {
			senderMap[senderUsers[i].Account] = senderUsers[i]
		}
	}

	clientMsg := buildClientMessage(&msg, senderMap)

	switch room.Type {
	case model.RoomTypeChannel:
		return h.publishChannelEvent(ctx, room, clientMsg, resolved.MentionAll, resolved.Participants)
	case model.RoomTypeDM:
		return h.publishDMEvents(ctx, room, clientMsg, resolved.Accounts)
	default:
		slog.Warn("unknown room type, skipping fan-out", "type", room.Type, "roomID", room.ID)
		return nil
	}
}

func (h *Handler) publishChannelEvent(ctx context.Context, room *model.Room, clientMsg *model.ClientMessage, mentionAll bool, mentions []model.Participant) error {
	evt := buildRoomEvent(room, clientMsg)
	evt.MentionAll = mentionAll
	if len(mentions) > 0 {
		evt.Mentions = mentions
	}

	msgJSON, err := json.Marshal(clientMsg)
	if err != nil {
		return fmt.Errorf("marshal client message: %w", err)
	}

	key, err := h.keyStore.Get(ctx, room.ID)
	if err != nil {
		return fmt.Errorf("get room key for room %s: %w", room.ID, err)
	}
	if key == nil {
		return fmt.Errorf("get room key for room %s: %w", room.ID, errNoCurrentKey)
	}

	encrypted, err := roomcrypto.Encode(string(msgJSON), key.KeyPair.PublicKey, key.Version)
	if err != nil {
		return fmt.Errorf("encrypt message for room %s: %w", room.ID, err)
	}

	encJSON, err := json.Marshal(encrypted)
	if err != nil {
		return fmt.Errorf("marshal encrypted message: %w", err)
	}

	evt.EncryptedMessage = json.RawMessage(encJSON)
	evt.Message = nil

	payload, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal channel event: %w", err)
	}

	return h.pub.Publish(ctx, subject.RoomEvent(room.ID), payload)
}

func (h *Handler) publishDMEvents(ctx context.Context, room *model.Room, clientMsg *model.ClientMessage, mentionedAccounts []string) error {
	subs, err := h.store.ListSubscriptions(ctx, room.ID)
	if err != nil {
		return fmt.Errorf("list subscriptions for DM room %s: %w", room.ID, err)
	}

	mentionSet := make(map[string]struct{}, len(mentionedAccounts))
	for _, name := range mentionedAccounts {
		mentionSet[name] = struct{}{}
	}

	for i := range subs {
		_, hasMention := mentionSet[subs[i].User.Account]

		evt := buildRoomEvent(room, clientMsg)
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

func buildRoomEvent(room *model.Room, clientMsg *model.ClientMessage) model.RoomEvent {
	return model.RoomEvent{
		Type:      model.RoomEventNewMessage,
		RoomID:    room.ID,
		Timestamp: time.Now().UTC().UnixMilli(),
		RoomName:  room.Name,
		RoomType:  room.Type,
		SiteID:    room.SiteID,
		UserCount: room.UserCount,
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
