package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/logctx"
	"github.com/hmchangw/chat/pkg/mention"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/userstore"
)

// PublishFunc publishes data; non-empty msgID sets Nats-Msg-Id for JetStream stream-level dedup.
// Mirrors room-worker's PublishFunc signature so message-worker can plug into the same publish closure.
type PublishFunc func(ctx context.Context, subj string, data []byte, msgID string) error

type Handler struct {
	store       Store
	userStore   userstore.UserStore
	threadStore ThreadStore
	siteID      string
	publish     PublishFunc
}

func NewHandler(store Store, userStore userstore.UserStore, threadStore ThreadStore, siteID string, publish PublishFunc) *Handler {
	return &Handler{
		store:       store,
		userStore:   userStore,
		threadStore: threadStore,
		siteID:      siteID,
		publish:     publish,
	}
}

func (h *Handler) HandleJetStreamMsg(ctx context.Context, msg jetstream.Msg) {
	// flow: hop entry — stream-wait latency the inter-hop time-diff can't see.
	// Gate the whole block so msg.Metadata() and arg-building are skipped on the
	// unflagged hot path (slog.Log evaluates its args before Enabled runs).
	if logctx.Enabled(ctx, logctx.LevelFlow) {
		streamWaitMs := int64(-1)
		if meta, err := msg.Metadata(); err == nil && meta != nil {
			streamWaitMs = time.Since(meta.Timestamp).Milliseconds()
		}
		slog.Log(ctx, logctx.LevelFlow, "message-worker received",
			"phase", "received", "request_id", natsutil.RequestIDFromContext(ctx),
			"subject", msg.Subject(), "bytes", len(msg.Data()), "stream_wait_ms", streamWaitMs)
	}

	if err := h.processMessage(ctx, msg.Data()); err != nil {
		slog.Log(ctx, logctx.LevelFlow, "message-worker nak", "phase", "nak", "request_id", natsutil.RequestIDFromContext(ctx))
		slog.ErrorContext(ctx, "process message failed", "error", err, "request_id", natsutil.RequestIDFromContext(ctx))
		if nakErr := msg.Nak(); nakErr != nil {
			slog.ErrorContext(ctx, "failed to nack message", "error", nakErr, "request_id", natsutil.RequestIDFromContext(ctx))
		}
		return
	}

	if err := msg.Ack(); err != nil {
		slog.ErrorContext(ctx, "failed to ack message", "error", err, "request_id", natsutil.RequestIDFromContext(ctx))
	}
}

func (h *Handler) processMessage(ctx context.Context, data []byte) error {
	var evt model.MessageEvent
	if err := json.Unmarshal(data, &evt); err != nil {
		return fmt.Errorf("unmarshal message event: %w", err)
	}

	resolved, err := mention.Resolve(ctx, evt.Message.Content, h.userStore.FindUsersByAccounts)
	if err != nil {
		return fmt.Errorf("resolve mentions: %w", err)
	}
	evt.Message.Mentions = resolved.Participants
	// debug: mention resolution is the first decision step — count only, no content.
	slog.DebugContext(ctx, "message-worker mentions resolved",
		"request_id", natsutil.RequestIDFromContext(ctx), "mentions", len(resolved.Participants))

	var sender *cassParticipant
	user, err := h.userStore.FindUserByID(ctx, evt.Message.UserID)
	if err != nil {
		if evt.Message.Type != "" {
			// System messages may have no real user; proceed with nil sender.
			slog.WarnContext(ctx, "user not found for system message, using nil sender",
				"user_id", evt.Message.UserID, "type", evt.Message.Type,
				"request_id", natsutil.RequestIDFromContext(ctx))
		} else {
			return fmt.Errorf("lookup user %s: %w", evt.Message.UserID, err)
		}
	} else {
		sender = &cassParticipant{
			ID:          user.ID,
			EngName:     user.EngName,
			CompanyName: user.ChineseName,
			Account:     evt.Message.UserAccount,
		}
	}
	// debug: which sender the message resolved to (system messages have none).
	slog.DebugContext(ctx, "message-worker sender resolved",
		"request_id", natsutil.RequestIDFromContext(ctx), "has_sender", sender != nil)

	if evt.Message.ThreadParentMessageID != "" {
		// Resolve (or create) the thread room first so we have the threadRoomID
		// before persisting the message to Cassandra.
		threadRoomID, err := h.handleThreadRoomAndSubscriptions(ctx, &evt.Message, evt.SiteID, user)
		if err != nil {
			return fmt.Errorf("handle thread room and subscriptions: %w", err)
		}
		if err := h.markThreadMentions(ctx, &evt.Message, threadRoomID, evt.SiteID); err != nil {
			return fmt.Errorf("mark thread mentions: %w", err)
		}
		newTcount, err := h.store.SaveThreadMessage(ctx, &evt.Message, sender, evt.SiteID, threadRoomID)
		if err != nil {
			return fmt.Errorf("save thread message: %w", err)
		}
		debugFlowPersisted(ctx, evt.Message.ID, true)
		if newTcount != nil {
			if err := h.publishThreadReplyEvent(ctx, &evt.Message, *newTcount); err != nil {
				return fmt.Errorf("publish thread reply event: %w", err)
			}
		}
	} else {
		if err := h.store.SaveMessage(ctx, &evt.Message, sender, evt.SiteID); err != nil {
			return fmt.Errorf("save message: %w", err)
		}
		debugFlowPersisted(ctx, evt.Message.ID, false)
	}

	return nil
}

// debugFlowPersisted emits the flow-rung breadcrumb marking the message as
// stored — the "was it persisted?" handoff for this hop. Metadata only.
func debugFlowPersisted(ctx context.Context, messageID string, thread bool) {
	slog.Log(ctx, logctx.LevelFlow, "message-worker persisted",
		"phase", "persisted", "request_id", natsutil.RequestIDFromContext(ctx),
		"message_id", messageID, "thread", thread)
}

// handleThreadRoomAndSubscriptions creates the ThreadRoom on first reply and
// inserts ThreadSubscriptions for the parent author and replier. On subsequent
// replies it upserts both subscriptions and bumps the room's last-message pointer.
// It returns the threadRoomID so the caller can pass it to SaveThreadMessage.
//
// `replier` may be nil for system messages with no real user (rare in thread
// paths); subscriptions for the replier are skipped in that case.
func (h *Handler) handleThreadRoomAndSubscriptions(ctx context.Context, msg *model.Message, eventSiteID string, replier *model.User) (string, error) {
	now := msg.CreatedAt

	var parentCreatedAt time.Time
	if msg.ThreadParentMessageCreatedAt != nil {
		parentCreatedAt = *msg.ThreadParentMessageCreatedAt
	}
	threadRoom := model.ThreadRoom{
		ID:                    idgen.GenerateUUIDv7(),
		ParentMessageID:       msg.ThreadParentMessageID,
		ThreadParentCreatedAt: parentCreatedAt,
		RoomID:                msg.RoomID,
		SiteID:                eventSiteID,
		LastMsgAt:             now,
		LastMsgID:             msg.ID,
		ReplyAccounts:         []string{msg.UserAccount},
		CreatedAt:             now,
		UpdatedAt:             now,
	}

	err := h.threadStore.CreateThreadRoom(ctx, &threadRoom)
	switch {
	case err == nil:
		return threadRoom.ID, h.handleFirstThreadReply(ctx, msg, eventSiteID, threadRoom.ID, replier, now)
	case errors.Is(err, errThreadRoomExists):
		return h.handleSubsequentThreadReply(ctx, msg, eventSiteID, replier, now)
	default:
		return "", fmt.Errorf("create thread room: %w", err)
	}
}

// handleFirstThreadReply runs after the thread room has just been created.
// It inserts subscriptions for the parent author and (if distinct) the replier.
// Subscription.SiteID is the room's site (eventSiteID); the owner's home site
// is resolved separately and used only to decide cross-site outbox routing.
func (h *Handler) handleFirstThreadReply(ctx context.Context, msg *model.Message, eventSiteID, threadRoomID string, replier *model.User, now time.Time) error {
	parentSender, err := h.store.GetMessageSender(ctx, msg.ThreadParentMessageID)
	if err != nil {
		if errors.Is(err, errMessageNotFound) {
			slog.WarnContext(ctx, "thread reply parent not found — skipping subscription creation",
				"parentMessageID", msg.ThreadParentMessageID,
				"replyID", msg.ID,
				"request_id", natsutil.RequestIDFromContext(ctx))
			return nil
		}
		return fmt.Errorf("get parent message sender: %w", err)
	}

	parentOwnerSite, err := h.lookupOwnerSiteID(ctx, parentSender.ID, "first-reply parent")
	if err != nil {
		return fmt.Errorf("lookup parent owner site: %w", err)
	}
	parentSub := h.buildThreadSubscription(msg, threadRoomID, parentSender.ID, parentSender.Account, eventSiteID, now)
	if err := h.threadStore.InsertThreadSubscription(ctx, parentSub); err != nil {
		return fmt.Errorf("insert parent author thread subscription: %w", err)
	}
	// Parent author joins the thread's replyAccounts set so they appear as a
	// follower in notification-worker and history-service's "following" feed,
	// even before they reply themselves. $addToSet dedups against the replier seed.
	if err := h.threadStore.AddReplyAccounts(ctx, threadRoomID, []string{parentSender.Account}); err != nil {
		return fmt.Errorf("add parent author to thread room replyAccounts: %w", err)
	}
	// Outbox publish is gated on parentOwnerSite — if the parent user is missing
	// from userStore, we can't route the cross-site copy, but the local Insert
	// above is independent of that and still happens.
	if parentOwnerSite != "" {
		if err := h.publishThreadSubOutboxIfRemote(ctx, parentSub, parentOwnerSite, msg.ID); err != nil {
			return fmt.Errorf("publish parent thread subscription outbox: %w", err)
		}
	}

	if replier != nil && msg.UserID != parentSender.ID {
		replierSub := h.buildThreadSubscription(msg, threadRoomID, msg.UserID, msg.UserAccount, eventSiteID, now)
		if err := h.threadStore.InsertThreadSubscription(ctx, replierSub); err != nil {
			return fmt.Errorf("insert replier thread subscription: %w", err)
		}
		if err := h.publishThreadSubOutboxIfRemote(ctx, replierSub, replier.SiteID, msg.ID); err != nil {
			return fmt.Errorf("publish replier thread subscription outbox: %w", err)
		}
	}

	// Requires ThreadParentMessageCreatedAt; missing → permanent silent thread-fetch failure.
	if msg.ThreadParentMessageCreatedAt != nil {
		if err := h.store.UpdateParentMessageThreadRoomID(ctx, msg.ThreadParentMessageID, msg.RoomID, *msg.ThreadParentMessageCreatedAt, threadRoomID); err != nil {
			return fmt.Errorf("stamp thread_room_id on parent message: %w", err)
		}
	} else {
		slog.ErrorContext(ctx, "first thread reply: ThreadParentMessageCreatedAt is nil, parent thread_room_id stamp skipped",
			"request_id", natsutil.RequestIDFromContext(ctx),
			"replyID", msg.ID,
			"parentMessageID", msg.ThreadParentMessageID,
			"threadRoomID", threadRoomID,
			"room_id", msg.RoomID,
		)
	}

	return nil
}

// handleSubsequentThreadReply runs when CreateThreadRoom reported an existing room.
// Upserts subscriptions for both the parent author and the replier (idempotent
// on redelivery), then bumps the room's last-message pointer. Returns the
// existing thread room ID so the caller can pass it to SaveThreadMessage.
// Subscription.SiteID is the room's site (eventSiteID); owner-site routing
// for the cross-site publish happens via separate lookups.
func (h *Handler) handleSubsequentThreadReply(ctx context.Context, msg *model.Message, eventSiteID string, replier *model.User, now time.Time) (string, error) {
	existingRoom, err := h.threadStore.GetThreadRoomByParentMessageID(ctx, msg.ThreadParentMessageID)
	if err != nil {
		return "", fmt.Errorf("get existing thread room: %w", err)
	}

	parentFound := true
	parentSender, err := h.store.GetMessageSender(ctx, msg.ThreadParentMessageID)
	switch {
	case err == nil:
		parentOwnerSite, lookupErr := h.lookupOwnerSiteID(ctx, parentSender.ID, "subsequent-reply parent")
		if lookupErr != nil {
			return "", fmt.Errorf("lookup parent owner site: %w", lookupErr)
		}
		parentSub := h.buildThreadSubscription(msg, existingRoom.ID, parentSender.ID, parentSender.Account, eventSiteID, now)
		if err := h.threadStore.UpsertThreadSubscription(ctx, parentSub); err != nil {
			return "", fmt.Errorf("upsert parent author thread subscription: %w", err)
		}
		if parentOwnerSite != "" {
			if err := h.publishThreadSubOutboxIfRemote(ctx, parentSub, parentOwnerSite, msg.ID); err != nil {
				return "", fmt.Errorf("publish parent thread subscription outbox: %w", err)
			}
		}
		if replier != nil && msg.UserID != parentSender.ID {
			replierSub := h.buildThreadSubscription(msg, existingRoom.ID, msg.UserID, msg.UserAccount, eventSiteID, now)
			if err := h.threadStore.UpsertThreadSubscription(ctx, replierSub); err != nil {
				return "", fmt.Errorf("upsert replier thread subscription: %w", err)
			}
			if err := h.publishThreadSubOutboxIfRemote(ctx, replierSub, replier.SiteID, msg.ID); err != nil {
				return "", fmt.Errorf("publish replier thread subscription outbox: %w", err)
			}
		}
	case errors.Is(err, errMessageNotFound):
		parentFound = false
		slog.WarnContext(ctx, "thread reply parent not found — skipping parent subscription upsert",
			"parentMessageID", msg.ThreadParentMessageID,
			"replyID", msg.ID,
			"request_id", natsutil.RequestIDFromContext(ctx))
		if replier != nil {
			replierSub := h.buildThreadSubscription(msg, existingRoom.ID, msg.UserID, msg.UserAccount, eventSiteID, now)
			if err := h.threadStore.UpsertThreadSubscription(ctx, replierSub); err != nil {
				return "", fmt.Errorf("upsert replier thread subscription: %w", err)
			}
			if err := h.publishThreadSubOutboxIfRemote(ctx, replierSub, replier.SiteID, msg.ID); err != nil {
				return "", fmt.Errorf("publish replier thread subscription outbox: %w", err)
			}
		}
	default:
		return "", fmt.Errorf("get parent message sender: %w", err)
	}

	// Update lastMsg pointer AND merge replier + parent author into replyAccounts in one write.
	// Folding the parent-author $addToSet here (vs a separate AddReplyAccounts call) halves the
	// per-reply Mongo round-trips and also covers the migration for thread_rooms created before
	// the parent author was seeded.
	replyAccounts := []string{msg.UserAccount}
	if parentFound {
		replyAccounts = append(replyAccounts, parentSender.Account)
	}
	if err := h.threadStore.UpdateThreadRoomLastMessage(ctx, existingRoom.ID, msg.ID, replyAccounts, now); err != nil {
		return "", fmt.Errorf("update thread room last message: %w", err)
	}

	// Re-stamp handles redelivery: first attempt may have created the thread room
	// but crashed before the stamp landed. IF EXISTS in the store prevents phantom rows.
	switch {
	case parentFound && msg.ThreadParentMessageCreatedAt != nil:
		if err := h.store.UpdateParentMessageThreadRoomID(ctx, msg.ThreadParentMessageID, msg.RoomID, *msg.ThreadParentMessageCreatedAt, existingRoom.ID); err != nil {
			return "", fmt.Errorf("stamp thread_room_id on parent message: %w", err)
		}
	case !parentFound:
		slog.ErrorContext(ctx, "subsequent thread reply: parent not found in messages_by_id, thread_room_id stamp skipped",
			"request_id", natsutil.RequestIDFromContext(ctx),
			"replyID", msg.ID,
			"parentMessageID", msg.ThreadParentMessageID,
			"threadRoomID", existingRoom.ID,
			"room_id", msg.RoomID,
		)
	default: // msg.ThreadParentMessageCreatedAt == nil
		slog.ErrorContext(ctx, "subsequent thread reply: ThreadParentMessageCreatedAt is nil, parent thread_room_id stamp skipped",
			"request_id", natsutil.RequestIDFromContext(ctx),
			"replyID", msg.ID,
			"parentMessageID", msg.ThreadParentMessageID,
			"threadRoomID", existingRoom.ID,
			"room_id", msg.RoomID,
		)
	}

	return existingRoom.ID, nil
}

// lookupOwnerSiteID resolves a user's home site by ID.
// Returns ("", nil) when the user is not found (logs a warning) so callers
// can skip that user gracefully — parallels the errMessageNotFound branch
// already in this file. Other DB errors are returned for the caller to NAK on.
func (h *Handler) lookupOwnerSiteID(ctx context.Context, userID, role string) (string, error) {
	user, err := h.userStore.FindUserByID(ctx, userID)
	if err != nil {
		if errors.Is(err, userstore.ErrUserNotFound) {
			slog.WarnContext(ctx, "owner user not found — skipping cross-site outbox publish; local thread subscription insert/upsert continues",
				"user_id", userID, "role", role,
				"request_id", natsutil.RequestIDFromContext(ctx))
			return "", nil
		}
		return "", fmt.Errorf("lookup user %s: %w", userID, err)
	}
	return user.SiteID, nil
}

// buildThreadSubscription constructs a ThreadSubscription for (threadRoomID, userID).
// siteID is the home site of the **room** that contains this thread — same
// semantic as Subscription.SiteID. The owner's home site is implicit (it's
// the site where the document is stored after federation); the cross-site
// publish decision is made separately by the caller.
// lastSeenAt is always nil; the field is owned by user-action paths, not message-worker.
func (h *Handler) buildThreadSubscription(msg *model.Message, threadRoomID, userID, userAccount, siteID string, now time.Time) *model.ThreadSubscription {
	return &model.ThreadSubscription{
		ID:              idgen.GenerateUUIDv7(),
		ParentMessageID: msg.ThreadParentMessageID,
		RoomID:          msg.RoomID,
		ThreadRoomID:    threadRoomID,
		UserID:          userID,
		UserAccount:     userAccount,
		SiteID:          siteID,
		LastSeenAt:      nil,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
}

// markThreadMentions flips hasMention=true on the thread subscription of every
// @account mentionee in msg (auto-creating the subscription if absent), and
// also adds them to thread_rooms.replyAccounts so they appear as thread followers
// for notification fan-out and the "following threads" feed. The sender is
// excluded and @all is ignored at the thread level. Subscription.SiteID is the
// room's site (eventSiteID); the mentionee's home site (Participant.SiteID) is
// used only for the cross-site outbox routing.
func (h *Handler) markThreadMentions(ctx context.Context, msg *model.Message, threadRoomID, eventSiteID string) error {
	var mentionedAccounts []string
	for i := range msg.Mentions {
		p := &msg.Mentions[i]
		if p.Account == "all" {
			continue
		}
		if p.UserID == msg.UserID {
			continue
		}
		sub := h.buildThreadSubscription(msg, threadRoomID, p.UserID, p.Account, eventSiteID, msg.CreatedAt)
		sub.HasMention = true
		if err := h.threadStore.MarkThreadSubscriptionMention(ctx, sub); err != nil {
			return fmt.Errorf("mark thread subscription mention for user %s: %w", p.UserID, err)
		}
		if err := h.publishThreadSubOutboxIfRemote(ctx, sub, p.SiteID, msg.ID); err != nil {
			return fmt.Errorf("publish thread mention outbox for user %s: %w", p.UserID, err)
		}
		mentionedAccounts = append(mentionedAccounts, p.Account)
	}
	if len(mentionedAccounts) > 0 {
		if err := h.threadStore.AddReplyAccounts(ctx, threadRoomID, mentionedAccounts); err != nil {
			return fmt.Errorf("add mentioned accounts to thread room replyAccounts: %w", err)
		}
	}
	return nil
}

// publishThreadSubOutboxIfRemote publishes a thread_subscription_upserted
// outbox event to ownerSiteID when that site differs from the local site.
// Same-site or empty ownerSiteID is a no-op (empty logs a warning — it
// indicates a caller bug). ownerSiteID is the subscription owner's home
// site — NOT sub.SiteID, which is the room's home site.
//
// The dedup-ID seed is (threadRoomID, userID, msgID): msg.ID is unique per
// reply, and (msg.ID, userID) is unique within a reply, so the seed is stable
// across MESSAGES_CANONICAL redeliveries and JetStream stream-level dedup
// absorbs duplicates within the dedup window.
func (h *Handler) publishThreadSubOutboxIfRemote(ctx context.Context, sub *model.ThreadSubscription, ownerSiteID, msgID string) error {
	if ownerSiteID == "" {
		slog.WarnContext(ctx, "owner siteID empty, skipping outbox publish",
			"threadRoomID", sub.ThreadRoomID, "user_id", sub.UserID, "msgID", msgID,
			"request_id", natsutil.RequestIDFromContext(ctx))
		return nil
	}
	if ownerSiteID == h.siteID {
		return nil
	}

	payload, err := json.Marshal(sub)
	if err != nil {
		return fmt.Errorf("marshal thread subscription: %w", err)
	}
	outbox := model.OutboxEvent{
		Type:       model.OutboxThreadSubscriptionUpserted,
		SiteID:     h.siteID,
		DestSiteID: ownerSiteID,
		Payload:    payload,
		Timestamp:  time.Now().UTC().UnixMilli(),
	}
	data, err := json.Marshal(outbox)
	if err != nil {
		return fmt.Errorf("marshal outbox event: %w", err)
	}
	// Dedup ID format: {payloadSeed}:{destSiteID}, where payloadSeed encodes
	// per-publish uniqueness (threadRoomID + userID + msg.ID). msg.ID is
	// stable across MESSAGES_CANONICAL redeliveries → same publish always
	// produces the same dedup ID. Different users on the same destination get
	// different dedup IDs because their userIDs differ in the seed.
	dedupID := fmt.Sprintf("thread-sub-outbox:%s:%s:%s:%s", sub.ThreadRoomID, sub.UserID, msgID, ownerSiteID)
	subj := subject.Outbox(h.siteID, ownerSiteID, model.OutboxThreadSubscriptionUpserted)
	if err := h.publish(ctx, subj, data, dedupID); err != nil {
		return fmt.Errorf("publish thread subscription outbox to %s: %w", ownerSiteID, err)
	}
	return nil
}

// publishThreadReplyEvent fires a badge event via core NATS so broadcast-worker
// can update the reply-count badge for thread followers. Published to
// chat.server.broadcast.{siteID}.thread.tcount (not MESSAGES_CANONICAL) because
// badge updates are best-effort and do not belong in the message CRUD event store.
func (h *Handler) publishThreadReplyEvent(ctx context.Context, msg *model.Message, newTcount int) error {
	evt := model.MessageEvent{
		Event: model.EventThreadReplyAdded,
		Message: model.Message{
			ID:                    msg.ID,
			RoomID:                msg.RoomID,
			ThreadParentMessageID: msg.ThreadParentMessageID,
		},
		SiteID:    h.siteID,
		Timestamp: time.Now().UTC().UnixMilli(),
		NewTCount: &newTcount,
	}
	data, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal thread reply event: %w", err)
	}
	return h.publish(ctx, subject.ServerBroadcastThreadTCount(h.siteID), data, "")
}
