// Package main fans out MESSAGES_CANONICAL room events with NAK-on-failure;
// handleReacted also publishes the reaction author-notification with log-and-swallow.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"sync"
	"sync/atomic"

	"github.com/hmchangw/chat/pkg/logctx"
	"github.com/hmchangw/chat/pkg/mention"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/roomcrypto"
	"github.com/hmchangw/chat/pkg/roomkeystore"
	"github.com/hmchangw/chat/pkg/roommetacache"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/userstore"
)

// errNoCurrentKey is returned when a room has no encryption key in its room document.
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
	case model.EventReacted:
		return h.handleReacted(ctx, &evt)
	default:
		slog.WarnContext(ctx, "unknown message event type, skipping",
			"event", evt.Event,
			"messageID", evt.Message.ID,
			"request_id", natsutil.RequestIDFromContext(ctx))
		return nil
	}
}

// HandleServerBroadcast processes a single server-broadcast core-NATS message
// (chat.server.broadcast.{siteID}.>). Currently handles EventThreadReplyAdded
// badge events published by message-worker.
func (h *Handler) HandleServerBroadcast(ctx context.Context, data []byte) {
	var evt model.MessageEvent
	if err := json.Unmarshal(data, &evt); err != nil {
		slog.ErrorContext(ctx, "unmarshal server-broadcast event failed; dropping",
			"error", err,
			"request_id", natsutil.RequestIDFromContext(ctx))
		return
	}
	switch evt.Event {
	case model.EventThreadReplyAdded:
		if err := h.handleThreadTCountUpdated(ctx, &evt); err != nil {
			slog.ErrorContext(ctx, "handle thread tcount update failed",
				"error", err,
				"messageID", evt.Message.ID,
				"request_id", natsutil.RequestIDFromContext(ctx))
		}
	default:
		slog.WarnContext(ctx, "unknown server-broadcast event type; dropping",
			"event", evt.Event,
			"request_id", natsutil.RequestIDFromContext(ctx))
	}
}

// shouldUseThreadFanOut reports whether a message should be routed through the
// thread fan-out path (thread subscribers + @-mentions) rather than the room
// broadcast path. True when the message is a thread reply hidden from the main
// channel (TShow=false).
func shouldUseThreadFanOut(msg *model.Message) bool {
	return msg.ThreadParentMessageID != "" && !msg.TShow
}

func (h *Handler) handleCreated(ctx context.Context, evt *model.MessageEvent) error {
	msg := evt.Message

	if shouldUseThreadFanOut(&msg) {
		return h.handleThreadCreated(ctx, evt)
	}

	// One user-store round-trip covers both mention enrichment and sender
	// enrichment: parse mentions, dedupe with the sender, fetch once, then
	// hand the resulting map to ResolveFromParsed (skips a second parse) and
	// to buildClientMessage.
	parsed := mention.Parse(msg.Content)
	lookupAccounts := dedupedAccounts(msg.UserAccount, parsed.Accounts)
	users, lookupErr := h.userStore.FindUsersByAccounts(ctx, lookupAccounts)
	if lookupErr != nil {
		slog.WarnContext(ctx, "user lookup failed, falling back to account",
			"error", lookupErr,
			"request_id", natsutil.RequestIDFromContext(ctx))
	}
	userByAccount := usersByAccount(users)

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

	// debug: how this message was routed for fan-out (metadata only).
	slog.DebugContext(ctx, "broadcast routing", "request_id", natsutil.RequestIDFromContext(ctx),
		"room_id", meta.ID, "type", meta.Type, "mentions", len(resolved.Accounts), "mention_all", resolved.MentionAll)

	switch meta.Type {
	case model.RoomTypeChannel:
		return h.publishChannelEvent(ctx, meta, clientMsg, evt.Timestamp, resolved.MentionAll, resolved.Participants)
	case model.RoomTypeDM, model.RoomTypeBotDM:
		return h.publishDMEvents(ctx, meta, clientMsg, evt.Timestamp, resolved.Accounts)
	default:
		slog.WarnContext(ctx, "unknown room type, skipping fan-out",
			"type", meta.Type,
			"room_id", meta.ID,
			"request_id", natsutil.RequestIDFromContext(ctx))
		return nil
	}
}

func (h *Handler) handleThreadCreated(ctx context.Context, evt *model.MessageEvent) error {
	msg := evt.Message
	parentMsgID := msg.ThreadParentMessageID

	parsed := mention.Parse(msg.Content)

	// Fetch room type first so DM/BotDM rooms skip the thread-subscription query
	// entirely — their fan-out uses ListSubscriptions, not thread subscribers.
	meta, err := h.store.GetRoomMeta(ctx, msg.RoomID)
	if err != nil {
		return fmt.Errorf("get room meta %s: %w", msg.RoomID, err)
	}

	// Channel rooms: only thread subscribers and @-mentioned accounts receive the
	// event. Fetch the subscriber list and build fanOut before any further work.
	var fanOut []string
	if meta.Type == model.RoomTypeChannel {
		fanOut, err = h.channelThreadFanOut(ctx, parentMsgID, msg.UserAccount, parsed.Accounts)
		if err != nil {
			return fmt.Errorf("channel thread fan-out for parent %s: %w", parentMsgID, err)
		}
		if len(fanOut) == 0 {
			slog.DebugContext(ctx, "no thread subscribers to notify for thread reply",
				"parentMessageID", parentMsgID,
				"request_id", natsutil.RequestIDFromContext(ctx))
			return nil
		}
	}

	lookupAccounts := dedupedAccounts(msg.UserAccount, parsed.Accounts)
	users, lookupErr := h.userStore.FindUsersByAccounts(ctx, lookupAccounts)
	if lookupErr != nil {
		slog.WarnContext(ctx, "user lookup failed for thread reply, falling back to account",
			"error", lookupErr,
			"parentMessageID", parentMsgID,
			"request_id", natsutil.RequestIDFromContext(ctx))
	}
	userByAccount := usersByAccount(users)

	resolved := mention.ResolveFromParsed(parsed, userByAccount)

	clientMsg := buildClientMessage(&msg, userByAccount)

	switch meta.Type {
	case model.RoomTypeChannel:
		// Do NOT call SetSubscriptionMentions here: TShow=false replies are invisible
		// in the main channel, so a room-level mention badge would appear with no
		// visible message to explain it.
		roomEvt := buildRoomEvent(meta, clientMsg, evt.Timestamp)
		roomEvt.MentionAll = resolved.MentionAll
		if len(resolved.Participants) > 0 {
			roomEvt.Mentions = resolved.Participants
		}
		payload, err := json.Marshal(roomEvt)
		if err != nil {
			return fmt.Errorf("marshal thread created event for parent %s: %w", parentMsgID, err)
		}
		return h.publishToThreadAccounts(ctx, fanOut, payload, parentMsgID)
	case model.RoomTypeDM, model.RoomTypeBotDM:
		// DM thread replies fan out to all members. @-mention badges are correct
		// since DM members can see the reply. lastMsgAt is intentionally NOT
		// updated: thread replies must not trigger hasUnread for non-participants.
		if len(resolved.Accounts) > 0 {
			if err := h.store.SetSubscriptionMentions(ctx, meta.ID, resolved.Accounts); err != nil {
				return fmt.Errorf("set subscription mentions: %w", err)
			}
		}
		return h.publishDMEvents(ctx, meta, clientMsg, evt.Timestamp, resolved.Accounts)
	default:
		slog.WarnContext(ctx, "unknown room type, skipping thread fan-out",
			"type", meta.Type,
			"room_id", meta.ID,
			"request_id", natsutil.RequestIDFromContext(ctx))
		return nil
	}
}

func (h *Handler) handleUpdated(ctx context.Context, evt *model.MessageEvent) error {
	msg := evt.Message
	if msg.EditedAt == nil || msg.UpdatedAt == nil {
		return fmt.Errorf("updated event missing EditedAt or UpdatedAt: %s", msg.ID)
	}

	if shouldUseThreadFanOut(&msg) {
		return h.handleThreadUpdated(ctx, evt)
	}

	room, err := h.store.GetRoom(ctx, msg.RoomID)
	if err != nil {
		return fmt.Errorf("fetch room %s: %w", msg.RoomID, err)
	}

	edit := buildEditRoomEvent(room, evt)
	if room.Type == model.RoomTypeChannel && h.encrypt {
		if err := h.encryptEditedContent(ctx, room.ID, &edit); err != nil {
			return fmt.Errorf("encrypt edit content for room %s: %w", room.ID, err)
		}
	}
	return h.publishMutation(ctx, room, model.RoomEventMessageEdited, msg.ID, &edit)
}

func (h *Handler) handleThreadUpdated(ctx context.Context, evt *model.MessageEvent) error {
	msg := evt.Message
	if msg.EditedAt == nil || msg.UpdatedAt == nil {
		return fmt.Errorf("updated event missing EditedAt or UpdatedAt for thread reply %s", msg.ID)
	}
	parentMsgID := msg.ThreadParentMessageID

	// GetRoom (not GetRoomMeta) so the DM/BotDM branch has room.Accounts for
	// fan-out. Fetched first so the routing decision is made before any
	// thread-follower lookup.
	room, err := h.store.GetRoom(ctx, msg.RoomID)
	if err != nil {
		return fmt.Errorf("get room %s: %w", msg.RoomID, err)
	}

	edit := buildEditRoomEvent(room, evt)

	switch room.Type {
	case model.RoomTypeChannel:
		parsed := mention.Parse(msg.Content)
		fanOut, err := h.channelThreadFanOut(ctx, parentMsgID, msg.UserAccount, parsed.Accounts)
		if err != nil {
			return fmt.Errorf("channel thread fan-out for thread update of parent %s: %w", parentMsgID, err)
		}
		if len(fanOut) == 0 {
			slog.DebugContext(ctx, "no thread subscribers to notify for thread update",
				"parentMessageID", parentMsgID,
				"request_id", natsutil.RequestIDFromContext(ctx))
			return nil
		}
		payload, err := json.Marshal(&edit)
		if err != nil {
			return fmt.Errorf("marshal thread edit event for parent %s: %w", parentMsgID, err)
		}
		return h.publishToThreadAccounts(ctx, fanOut, payload, parentMsgID)
	case model.RoomTypeDM, model.RoomTypeBotDM:
		// DM thread replies are visible to every member, so edits fan out to
		// all members (consistent with handleThreadCreated), not just thread
		// subscribers.
		return h.publishMutation(ctx, room, model.RoomEventMessageEdited, msg.ID, &edit)
	default:
		slog.WarnContext(ctx, "unknown room type, skipping thread update fan-out",
			"type", room.Type,
			"room_id", room.ID,
			"request_id", natsutil.RequestIDFromContext(ctx))
		return nil
	}
}

func (h *Handler) handleThreadDeleted(ctx context.Context, evt *model.MessageEvent) error {
	msg := evt.Message
	parentMsgID := msg.ThreadParentMessageID

	if msg.UpdatedAt == nil {
		return fmt.Errorf("missing UpdatedAt for thread message %s", msg.ID)
	}

	// GetRoom first so the routing decision (thread followers vs all DM
	// members) is made from the authoritative room type and Accounts.
	room, err := h.store.GetRoom(ctx, msg.RoomID)
	if err != nil {
		return fmt.Errorf("get room %s: %w", msg.RoomID, err)
	}

	del := buildDeleteRoomEvent(room, evt)

	switch room.Type {
	case model.RoomTypeChannel:
		// Parse @-mentions from the deleted message so that non-follower
		// recipients who received the create event (via mention fan-out) also
		// receive the delete. Only the channel path uses mentions; the DM path
		// fans out to all members.
		parsed := mention.Parse(msg.Content)
		fanOut, err := h.channelThreadFanOut(ctx, parentMsgID, msg.UserAccount, parsed.Accounts)
		if err != nil {
			return fmt.Errorf("channel thread fan-out for thread delete of parent %s: %w", parentMsgID, err)
		}
		if len(fanOut) > 0 {
			payload, err := json.Marshal(&del)
			if err != nil {
				return fmt.Errorf("marshal thread delete event for parent %s: %w", parentMsgID, err)
			}
			if err := h.publishToThreadAccounts(ctx, fanOut, payload, parentMsgID); err != nil {
				return fmt.Errorf("publish thread delete event for parent %s: %w", parentMsgID, err)
			}
		}
	case model.RoomTypeDM, model.RoomTypeBotDM:
		// DM thread replies are visible to every member, so deletes fan out to
		// all members (consistent with handleThreadCreated), not just thread
		// subscribers.
		if err := h.publishMutation(ctx, room, model.RoomEventMessageDeleted, msg.ID, &del); err != nil {
			return fmt.Errorf("publish thread delete mutation for room %s message %s: %w", room.ID, msg.ID, err)
		}
	default:
		slog.WarnContext(ctx, "unknown room type, skipping thread delete fan-out",
			"type", room.Type,
			"room_id", room.ID,
			"request_id", natsutil.RequestIDFromContext(ctx))
		// No return: the badge update below is safe for all room types;
		// publishThreadMetadata handles unknown types by logging and skipping.
	}

	// Badge (tcount) update applies to all room types.
	if evt.NewTCount != nil {
		h.publishThreadBadge(ctx, room, *evt.NewTCount, parentMsgID, msg.ID, evt.Timestamp)
	}

	return nil
}

func (h *Handler) handleThreadTCountUpdated(ctx context.Context, evt *model.MessageEvent) error {
	if evt.NewTCount == nil {
		slog.WarnContext(ctx, "thread_reply_added event missing NewTCount, skipping",
			"messageID", evt.Message.ID,
			"request_id", natsutil.RequestIDFromContext(ctx))
		return nil
	}
	if evt.Message.ThreadParentMessageID == "" {
		slog.WarnContext(ctx, "thread_reply_added event missing ThreadParentMessageID, skipping",
			"messageID", evt.Message.ID,
			"request_id", natsutil.RequestIDFromContext(ctx))
		return nil
	}
	room, err := h.store.GetRoom(ctx, evt.Message.RoomID)
	if err != nil {
		return fmt.Errorf("get room %s: %w", evt.Message.RoomID, err)
	}
	return h.publishThreadMetadata(ctx, room, *evt.NewTCount,
		evt.Message.ThreadParentMessageID, evt.Message.ID,
		model.ThreadActionReplyAdded, evt.Timestamp)
}

func (h *Handler) publishThreadMetadata(ctx context.Context, room *model.Room, newTcount int,
	parentMsgID, replyMsgID string, action model.ThreadAction, eventTimestamp int64) error {
	evt := model.ThreadMetadataUpdatedEvent{
		Type:            model.RoomEventThreadMetadataUpdated,
		RoomID:          room.ID,
		SiteID:          room.SiteID,
		ParentMessageID: parentMsgID,
		ReplyMessageID:  replyMsgID,
		NewTCount:       newTcount,
		Action:          action,
		Timestamp:       time.Now().UTC().UnixMilli(),
		EventTimestamp:  eventTimestamp,
	}
	payload, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal thread metadata event for room %s: %w", room.ID, err)
	}
	switch room.Type {
	case model.RoomTypeChannel:
		if err := h.pub.Publish(ctx, subject.RoomEvent(room.ID), payload); err != nil {
			return fmt.Errorf("publish thread metadata for channel room %s: %w", room.ID, err)
		}
	case model.RoomTypeDM, model.RoomTypeBotDM:
		for _, account := range room.Accounts {
			if isBot(account) {
				continue
			}
			if err := h.pub.Publish(ctx, subject.UserRoomEvent(account), payload); err != nil {
				return fmt.Errorf("publish thread metadata to DM member %s in room %s: %w", account, room.ID, err)
			}
		}
	default:
		slog.WarnContext(ctx, "unknown room type for thread metadata, skipping",
			"type", room.Type,
			"room_id", room.ID,
			"request_id", natsutil.RequestIDFromContext(ctx))
	}
	return nil
}

func (h *Handler) handleDeleted(ctx context.Context, evt *model.MessageEvent) error {
	msg := evt.Message
	if msg.UpdatedAt == nil {
		return fmt.Errorf("deleted event missing UpdatedAt: %s", msg.ID)
	}

	if shouldUseThreadFanOut(&msg) {
		return h.handleThreadDeleted(ctx, evt)
	}

	room, err := h.store.GetRoom(ctx, msg.RoomID)
	if err != nil {
		return fmt.Errorf("fetch room %s: %w", msg.RoomID, err)
	}

	del := buildDeleteRoomEvent(room, evt)
	if err := h.publishMutation(ctx, room, model.RoomEventMessageDeleted, msg.ID, &del); err != nil {
		return fmt.Errorf("publish delete mutation for room %s message %s: %w", room.ID, msg.ID, err)
	}
	// TShow=true thread replies appear in the main room (handled by publishMutation
	// above) but still count toward the thread's reply-count badge. Since
	// handleThreadDeleted is bypassed for TShow=true, we publish the badge update here.
	if msg.ThreadParentMessageID != "" && evt.NewTCount != nil {
		h.publishThreadBadge(ctx, room, *evt.NewTCount, msg.ThreadParentMessageID, msg.ID, evt.Timestamp)
	}
	return nil
}

// publishThreadBadge publishes a thread-metadata badge update for a deleted
// reply. Errors are logged but not returned: badge updates are best-effort and
// JetStream will redeliver the parent event on failure.
func (h *Handler) publishThreadBadge(ctx context.Context, room *model.Room, newTCount int, parentMsgID, replyMsgID string, timestamp int64) {
	if err := h.publishThreadMetadata(ctx, room, newTCount, parentMsgID, replyMsgID, model.ThreadActionReplyDeleted, timestamp); err != nil {
		slog.ErrorContext(ctx, "publish thread badge for deleted reply failed",
			"error", err,
			"parentMessageID", parentMsgID,
			"request_id", natsutil.RequestIDFromContext(ctx))
	}
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

	pin := model.PinStateRoomEvent{
		Type:           model.RoomEventMessagePinned,
		RoomID:         room.ID,
		SiteID:         room.SiteID,
		Timestamp:      time.Now().UTC().UnixMilli(),
		EventTimestamp: evt.Timestamp,
		MessageID:      msg.ID,
		Pinned:         true,
		By:             msg.PinnedBy,
		At:             *msg.PinnedAt,
	}
	return h.publishMutation(ctx, room, model.RoomEventMessagePinned, msg.ID, &pin)
}

func (h *Handler) handleUnpinned(ctx context.Context, evt *model.MessageEvent) error {
	msg := evt.Message
	// At comes from evt.Timestamp (set at publish): the canonical unpin
	// payload from history-service clears PinnedAt, so the message itself
	// carries no unpin timestamp.

	room, err := h.store.GetRoom(ctx, msg.RoomID)
	if err != nil {
		return fmt.Errorf("fetch room %s: %w", msg.RoomID, err)
	}

	unpin := model.PinStateRoomEvent{
		Type:           model.RoomEventMessageUnpinned,
		RoomID:         room.ID,
		SiteID:         room.SiteID,
		Timestamp:      time.Now().UTC().UnixMilli(),
		EventTimestamp: evt.Timestamp,
		MessageID:      msg.ID,
		Pinned:         false,
		By:             msg.PinnedBy,
		At:             time.UnixMilli(evt.Timestamp).UTC(),
	}
	return h.publishMutation(ctx, room, model.RoomEventMessageUnpinned, msg.ID, &unpin)
}

// handleReacted fans out a single-actor reaction delta to clients in the
// room. Reactions carry no content, so the encryption branch is skipped.
func (h *Handler) handleReacted(ctx context.Context, evt *model.MessageEvent) error {
	msg := evt.Message
	// Log-and-drop on malformed payloads: NAK would loop forever on a publisher contract violation.
	if evt.ReactionDelta == nil {
		slog.ErrorContext(ctx, "reacted event missing ReactionDelta; dropping",
			"messageID", msg.ID,
			"roomID", msg.RoomID,
			"siteID", evt.SiteID,
			"request_id", natsutil.RequestIDFromContext(ctx),
		)
		return nil
	}
	if msg.UpdatedAt == nil {
		slog.ErrorContext(ctx, "reacted event missing UpdatedAt; dropping",
			"messageID", msg.ID,
			"roomID", msg.RoomID,
			"siteID", evt.SiteID,
			"request_id", natsutil.RequestIDFromContext(ctx),
		)
		return nil
	}

	room, err := h.store.GetRoom(ctx, msg.RoomID)
	if err != nil {
		return fmt.Errorf("fetch room %s: %w", msg.RoomID, err)
	}

	react := model.ReactRoomEvent{
		Type:           model.RoomEventMessageReacted,
		RoomID:         room.ID,
		SiteID:         room.SiteID,
		Timestamp:      time.Now().UTC().UnixMilli(),
		EventTimestamp: evt.Timestamp,
		MessageID:      msg.ID,
		Shortcode:      evt.ReactionDelta.Shortcode,
		Action:         evt.ReactionDelta.Action,
		Actor:          evt.ReactionDelta.Actor,
		ReactedAt:      *msg.UpdatedAt,
		UpdatedAt:      *msg.UpdatedAt,
	}
	if err := h.publishMutation(ctx, room, model.RoomEventMessageReacted, msg.ID, &react); err != nil {
		return err
	}

	// Author notification: added + author != actor + non-empty author; publish failure swallowed.
	if evt.ReactionDelta.Action != model.ReactionActionAdded {
		return nil
	}
	authorAccount := msg.UserAccount
	if authorAccount == "" || authorAccount == evt.ReactionDelta.Actor.Account {
		return nil
	}
	notif := model.NotificationEvent{
		Type:          "reaction",
		RoomID:        msg.RoomID,
		Message:       msg,
		ReactionDelta: evt.ReactionDelta,
		Timestamp:     time.Now().UTC().UnixMilli(),
	}
	data, marshalErr := json.Marshal(notif)
	if marshalErr != nil {
		slog.ErrorContext(ctx, "marshal reaction author notification failed",
			"error", marshalErr,
			"messageID", msg.ID,
			"roomID", msg.RoomID,
			"siteID", evt.SiteID,
			"request_id", natsutil.RequestIDFromContext(ctx),
		)
	} else if pubErr := h.pub.Publish(ctx, subject.Notification(authorAccount), data); pubErr != nil {
		slog.ErrorContext(ctx, "publish reaction author notification failed",
			"error", pubErr,
			"author", authorAccount,
			"messageID", msg.ID,
			"roomID", msg.RoomID,
			"siteID", evt.SiteID,
			"request_id", natsutil.RequestIDFromContext(ctx),
		)
	}
	return nil
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
				slog.ErrorContext(ctx, "publish DM mutation event failed",
					"error", err,
					"type", roomEvtType,
					"account", account,
					"messageID", messageID,
					"room_id", room.ID,
					"request_id", natsutil.RequestIDFromContext(ctx),
				)
			}
		}
		return nil

	default:
		slog.WarnContext(ctx, "unknown room type, skipping mutation fan-out",
			"type", room.Type,
			"room_id", room.ID,
			"request_id", natsutil.RequestIDFromContext(ctx))
		return nil
	}
}

func buildEditRoomEvent(room *model.Room, evt *model.MessageEvent) model.EditRoomEvent {
	msg := evt.Message
	return model.EditRoomEvent{
		Type:           model.RoomEventMessageEdited,
		RoomID:         room.ID,
		SiteID:         room.SiteID,
		Timestamp:      time.Now().UTC().UnixMilli(),
		EventTimestamp: evt.Timestamp,
		MessageID:      msg.ID,
		NewContent:     msg.Content,
		EditedBy:       msg.UserAccount,
		EditedAt:       *msg.EditedAt,
		UpdatedAt:      *msg.UpdatedAt,
	}
}

func buildDeleteRoomEvent(room *model.Room, evt *model.MessageEvent) model.DeleteRoomEvent {
	msg := evt.Message
	return model.DeleteRoomEvent{
		Type:           model.RoomEventMessageDeleted,
		RoomID:         room.ID,
		SiteID:         room.SiteID,
		Timestamp:      time.Now().UTC().UnixMilli(),
		EventTimestamp: evt.Timestamp,
		MessageID:      msg.ID,
		DeletedBy:      msg.UserAccount,
		DeletedAt:      *msg.UpdatedAt,
		UpdatedAt:      *msg.UpdatedAt,
	}
}

func (h *Handler) encryptEditedContent(ctx context.Context, roomID string, edited *model.EditRoomEvent) error {
	key, err := h.currentRoomKey(ctx, roomID)
	if err != nil {
		return fmt.Errorf("get encryption key for room %s: %w", roomID, err)
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

// encryptRoomEvent applies room encryption to evt if h.encrypt is true,
// replacing evt.Message with an EncryptedMessage envelope built from clientMsg.
func (h *Handler) encryptRoomEvent(ctx context.Context, roomID string, clientMsg *model.ClientMessage, evt *model.RoomEvent) error {
	if !h.encrypt {
		return nil
	}
	msgJSON, err := json.Marshal(clientMsg)
	if err != nil {
		return fmt.Errorf("marshal client message for room %s: %w", roomID, err)
	}
	key, err := h.currentRoomKey(ctx, roomID)
	if err != nil {
		return fmt.Errorf("get encryption key for room %s: %w", roomID, err)
	}
	encrypted, err := h.encoder.Encode(roomID, string(msgJSON), key.KeyPair.PrivateKey, key.Version)
	if err != nil {
		return fmt.Errorf("encrypt message for room %s: %w", roomID, err)
	}
	encJSON, err := json.Marshal(encrypted)
	if err != nil {
		return fmt.Errorf("marshal encrypted message for room %s: %w", roomID, err)
	}
	evt.EncryptedMessage = json.RawMessage(encJSON)
	evt.Message = nil
	return nil
}

func (h *Handler) publishChannelEvent(ctx context.Context, meta roommetacache.Meta, clientMsg *model.ClientMessage, timestamp int64, mentionAll bool, mentions []model.Participant) error {
	evt := buildRoomEvent(meta, clientMsg, timestamp)
	evt.MentionAll = mentionAll
	if len(mentions) > 0 {
		evt.Mentions = mentions
	}
	if err := h.encryptRoomEvent(ctx, meta.ID, clientMsg, &evt); err != nil {
		return fmt.Errorf("encrypt channel event for room %s: %w", meta.ID, err)
	}
	payload, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal channel event: %w", err)
	}
	// flow: one room-stream publish; NATS fans out to subscribers downstream, so
	// this reports the room audience, not per-recipient deliveries from here.
	slog.Log(ctx, logctx.LevelFlow, "broadcast fan-out", "phase", "fanout",
		"request_id", natsutil.RequestIDFromContext(ctx), "room_id", meta.ID,
		"type", string(meta.Type), "delivery", "room-stream", "audience", meta.UserCount)
	return h.pub.Publish(ctx, subject.RoomEvent(meta.ID), payload)
}

// debugFlowFanout emits the flow-rung outcome of a per-recipient fan-out:
// recipients = individual deliveries attempted in this hop, failed = how many of
// those errored (delivered = recipients - failed). Metadata only. The room-stream
// (channel) path is NOT per-recipient — it reports `audience` inline instead.
func debugFlowFanout(ctx context.Context, roomID, roomType, delivery string, recipients, failed int) {
	slog.Log(ctx, logctx.LevelFlow, "broadcast fan-out", "phase", "fanout",
		"request_id", natsutil.RequestIDFromContext(ctx), "room_id", roomID,
		"type", roomType, "delivery", delivery, "recipients", recipients, "failed", failed)
}

// debugTraceDelivered emits the trace-rung per-recipient delivery line — the
// "did it reach user X?" detail. Recipient account identifiers are permitted at
// trace (never message content); off unless a request is flagged trace.
func debugTraceDelivered(ctx context.Context, account, roomID string) {
	slog.Log(ctx, logctx.LevelTrace, "broadcast delivered",
		"request_id", natsutil.RequestIDFromContext(ctx), "account", account, "room_id", roomID)
}

func (h *Handler) publishDMEvents(ctx context.Context, meta roommetacache.Meta, clientMsg *model.ClientMessage, timestamp int64, mentionedAccounts []string) error {
	subs, err := h.store.ListSubscriptions(ctx, meta.ID)
	if err != nil {
		return fmt.Errorf("list subscriptions for DM room %s: %w", meta.ID, err)
	}

	mentionSet := make(map[string]struct{}, len(mentionedAccounts))
	for _, name := range mentionedAccounts {
		mentionSet[name] = struct{}{}
	}

	recipients, failed := 0, 0
	for i := range subs {
		account := subs[i].User.Account
		// Skip bots: live UI events go to human clients only, consistent with
		// publishMutation and publishThreadMetadata. Bots receive messages via
		// their own server-side integration, not the websocket event channel.
		if isBot(account) {
			continue
		}
		_, hasMention := mentionSet[account]

		evt := buildRoomEvent(meta, clientMsg, timestamp)
		evt.HasMention = hasMention

		payload, err := json.Marshal(evt)
		if err != nil {
			return fmt.Errorf("marshal DM event for user %s: %w", account, err)
		}
		recipients++
		// Publish errors are intentionally swallowed here (log-and-continue). DM thread
		// replies have no JetStream retry guarantee by design — the DM path uses
		// publishDMEvents which is fire-and-forget, consistent with how all DM fan-out
		// works in this service (publishMutation). Channel thread events propagate errors
		// via publishToThreadAccounts so JetStream can redeliver.
		if err := h.pub.Publish(ctx, subject.UserRoomEvent(account), payload); err != nil {
			slog.ErrorContext(ctx, "publish DM event failed",
				"error", err,
				"account", account,
				"room_id", meta.ID,
				"request_id", natsutil.RequestIDFromContext(ctx))
			failed++
			continue // don't emit a "delivered" trace for a failed publish
		}
		debugTraceDelivered(ctx, account, meta.ID)
	}
	debugFlowFanout(ctx, meta.ID, string(meta.Type), "per-member", recipients, failed)
	return nil
}

func buildRoomEvent(meta roommetacache.Meta, clientMsg *model.ClientMessage, eventTimestamp int64) model.RoomEvent {
	return model.RoomEvent{
		Type:           model.RoomEventNewMessage,
		RoomID:         meta.ID,
		Timestamp:      time.Now().UTC().UnixMilli(),
		EventTimestamp: eventTimestamp,
		RoomName:       meta.Name,
		RoomType:       meta.Type,
		SiteID:         meta.SiteID,
		UserCount:      meta.UserCount,
		LastMsgAt:      clientMsg.CreatedAt,
		LastMsgID:      clientMsg.ID,
		Message:        clientMsg,
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

// publishToThreadAccounts publishes payload concurrently to every account in
// the list. Only returns an error (triggering JetStream redelivery) when every
// publish fails — partial failure is tolerated to avoid duplicate delivery to
// accounts that already received the event on the first attempt.
func (h *Handler) publishToThreadAccounts(ctx context.Context, accounts []string, payload []byte, parentMsgID string) error {
	if len(accounts) == 0 {
		return nil
	}
	var wg sync.WaitGroup
	var failCount atomic.Int64
	for _, account := range accounts {
		account := account
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := h.pub.Publish(ctx, subject.UserRoomEvent(account), payload); err != nil {
				slog.ErrorContext(ctx, "publish thread event failed",
					"error", err,
					"account", account,
					"parentMessageID", parentMsgID,
					"request_id", natsutil.RequestIDFromContext(ctx))
				failCount.Add(1)
				return
			}
			debugTraceDelivered(ctx, account, parentMsgID)
		}()
	}
	wg.Wait()
	debugFlowFanout(ctx, parentMsgID, "thread", "per-follower", len(accounts), int(failCount.Load()))
	if failCount.Load() == int64(len(accounts)) {
		return fmt.Errorf("all %d thread account publishes failed for parent %s", len(accounts), parentMsgID)
	}
	return nil
}

// threadFanOutAccounts builds the deduplicated fan-out recipient list for
// a thread event. senderAccount is always excluded. extraAccounts
// (e.g. @mentioned users from the message payload) are added after the
// follower pass.
func threadFanOutAccounts(senderAccount string, followers map[string]struct{}, extraAccounts []string) []string {
	seen := map[string]struct{}{senderAccount: {}}
	var fanOut []string
	for acc := range followers {
		if _, ok := seen[acc]; ok {
			continue
		}
		if isBot(acc) {
			continue
		}
		seen[acc] = struct{}{}
		fanOut = append(fanOut, acc)
	}
	for _, acc := range extraAccounts {
		if _, ok := seen[acc]; ok {
			continue
		}
		if isBot(acc) {
			continue
		}
		seen[acc] = struct{}{}
		fanOut = append(fanOut, acc)
	}
	return fanOut
}

// channelThreadFanOut resolves the deduplicated recipient list for a channel
// thread event: it fetches the parent message's thread followers and merges
// them with the @-mentioned accounts, excluding the sender. Shared by the
// channel branch of every thread handler (created/updated/deleted).
func (h *Handler) channelThreadFanOut(ctx context.Context, parentMsgID, sender string, mentions []string) ([]string, error) {
	followers, err := h.store.GetThreadFollowers(ctx, parentMsgID)
	if err != nil {
		return nil, fmt.Errorf("get thread followers for parent %s: %w", parentMsgID, err)
	}
	return threadFanOutAccounts(sender, followers, mentions), nil
}

// usersByAccount indexes a slice of users by their Account for O(1) lookup
// during mention resolution and client-message enrichment.
func usersByAccount(users []model.User) map[string]model.User {
	byAccount := make(map[string]model.User, len(users))
	for i := range users {
		byAccount[users[i].Account] = users[i]
	}
	return byAccount
}
