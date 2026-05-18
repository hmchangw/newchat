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
	encrypt   bool
}

func NewHandler(store Store, userStore userstore.UserStore, pub Publisher, keyStore RoomKeyProvider, encrypt bool) *Handler {
	return &Handler{store: store, userStore: userStore, pub: pub, keyStore: keyStore, encrypt: encrypt}
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
	default:
		slog.Warn("unknown message event type, skipping", "event", evt.Event, "messageID", evt.Message.ID)
		return nil
	}
}

func (h *Handler) handleCreated(ctx context.Context, evt *model.MessageEvent) error {
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

func (h *Handler) handleUpdated(ctx context.Context, evt *model.MessageEvent) error {
	msg := evt.Message
	if msg.EditedAt == nil || msg.UpdatedAt == nil {
		return fmt.Errorf("updated event missing EditedAt or UpdatedAt: %s", msg.ID)
	}
	return h.fanOutMutationEvent(ctx, evt, model.RoomEventMessageEdited, &model.MessageEditedPayload{
		MessageID:  msg.ID,
		NewContent: msg.Content,
		EditedBy:   msg.UserAccount,
		EditedAt:   *msg.EditedAt,
		UpdatedAt:  *msg.UpdatedAt,
	}, nil)
}

func (h *Handler) handleDeleted(ctx context.Context, evt *model.MessageEvent) error {
	msg := evt.Message
	if msg.UpdatedAt == nil {
		return fmt.Errorf("deleted event missing UpdatedAt: %s", msg.ID)
	}
	return h.fanOutMutationEvent(ctx, evt, model.RoomEventMessageDeleted, nil, &model.MessageDeletedPayload{
		MessageID: msg.ID,
		DeletedBy: msg.UserAccount,
		DeletedAt: *msg.UpdatedAt,
		UpdatedAt: *msg.UpdatedAt,
	})
}

// fanOutMutationEvent routes the live event by room type, same as the create
// path. Exactly one of edited/deleted is non-nil.
func (h *Handler) fanOutMutationEvent(
	ctx context.Context,
	evt *model.MessageEvent,
	roomEvtType model.RoomEventType,
	edited *model.MessageEditedPayload,
	deleted *model.MessageDeletedPayload,
) error {
	msg := evt.Message

	room, err := h.store.GetRoom(ctx, msg.RoomID)
	if err != nil {
		return fmt.Errorf("fetch room %s: %w", msg.RoomID, err)
	}

	roomEvt := model.RoomEvent{
		Type:           roomEvtType,
		RoomID:         room.ID,
		Timestamp:      evt.Timestamp,
		SiteID:         room.SiteID,
		MessageEdited:  edited,
		MessageDeleted: deleted,
	}

	switch room.Type {
	case model.RoomTypeChannel:
		if h.encrypt && edited != nil {
			if err := h.encryptEditedContent(ctx, room.ID, edited); err != nil {
				return err
			}
		}
		payload, err := json.Marshal(&roomEvt)
		if err != nil {
			return fmt.Errorf("marshal %s channel event: %w", roomEvtType, err)
		}
		return h.pub.Publish(ctx, subject.RoomEvent(room.ID), payload)

	case model.RoomTypeDM, model.RoomTypeBotDM:
		payload, err := json.Marshal(&roomEvt)
		if err != nil {
			return fmt.Errorf("marshal %s DM event: %w", roomEvtType, err)
		}
		for _, account := range room.Accounts {
			if isBot(account) {
				continue
			}
			if err := h.pub.Publish(ctx, subject.UserRoomEvent(account), payload); err != nil {
				slog.Error("publish DM mutation event failed",
					"error", err,
					"type", roomEvtType,
					"account", account,
					"messageID", msg.ID,
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

func (h *Handler) encryptEditedContent(ctx context.Context, roomID string, edited *model.MessageEditedPayload) error {
	key, err := h.currentRoomKey(ctx, roomID)
	if err != nil {
		return err
	}
	encrypted, err := roomcrypto.Encode(edited.NewContent, key.KeyPair.PublicKey, key.Version)
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

func (h *Handler) publishChannelEvent(ctx context.Context, room *model.Room, clientMsg *model.ClientMessage, mentionAll bool, mentions []model.Participant) error {
	evt := buildRoomEvent(room, clientMsg)
	evt.MentionAll = mentionAll
	if len(mentions) > 0 {
		evt.Mentions = mentions
	}

	if h.encrypt {
		msgJSON, err := json.Marshal(clientMsg)
		if err != nil {
			return fmt.Errorf("marshal client message: %w", err)
		}

		key, err := h.currentRoomKey(ctx, room.ID)
		if err != nil {
			return err
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
	}
	// when h.encrypt is false, evt.Message is already set by buildRoomEvent

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
