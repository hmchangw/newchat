package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/bytedance/sonic"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/jsretry"
	"github.com/hmchangw/chat/pkg/logctx"
	"github.com/hmchangw/chat/pkg/mention"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/outbox"
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

	// Migrated (X-Migration: live) events are persisted, but downstream thread side-effects are suppressed (see processMessage).
	isMigration := natsutil.IsMigrationLiveHeader(msg.Headers())
	// Sole persister of message history to Cassandra: transient failures must
	// retry with backoff (never drop); malformed events Ack-drop as poison.
	jsretry.Settle(ctx, msg, jsretry.DefaultBackoff, h.processMessage(ctx, msg.Data(), isMigration))
}

func (h *Handler) processMessage(ctx context.Context, data []byte, isMigration bool) error {
	var evt model.MessageEvent
	if err := sonic.Unmarshal(data, &evt); err != nil {
		// Malformed payload — it will never parse on redelivery. Mark permanent
		// so the handler Acks (drops) it instead of retrying until MaxDeliver.
		return errcode.Permanent(errcode.BadRequest("malformed message event"))
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

	// Correct an untrusted degraded-mode (placeholder) quoted snapshot before any
	// durable write, so a fabricated snapshot never persists or re-renders.
	if err := h.reprojectUnverifiedQuote(ctx, &evt); err != nil {
		return fmt.Errorf("re-project unverified quote: %w", err)
	}

	if evt.Message.ThreadParentMessageID != "" {
		// The gatekeeper resolves the parent's createdAt best-effort at send time
		// and ships it on the event; trust it when present. Otherwise resolve
		// authoritatively from messages_by_id. A miss → parent's canonical write
		// hasn't landed → NAK for redelivery (bounded by MaxDeliver) rather than
		// persist a null, corrupting partition coords.
		if evt.Message.ThreadParentMessageCreatedAt == nil {
			createdAt, found, err := h.store.GetMessageCreatedAt(ctx, evt.Message.ThreadParentMessageID)
			if err != nil {
				return fmt.Errorf("resolve thread parent createdAt: %w", err)
			}
			if !found {
				return fmt.Errorf("thread parent %s not yet persisted in messages_by_id", evt.Message.ThreadParentMessageID)
			}
			evt.Message.ThreadParentMessageCreatedAt = &createdAt
		}

		// Resolve (or create) the thread room first so we have the threadRoomID
		// before persisting the message to Cassandra.
		threadRoomID, replierLastSeenAdvanced, err := h.handleThreadRoomAndSubscriptions(ctx, &evt.Message, evt.SiteID, user, isMigration)
		if err != nil {
			return fmt.Errorf("handle thread room and subscriptions: %w", err)
		}
		// Replying implies the replier read up to their own reply: advance their thread
		// lastSeenAt so the read-floor doesn't count them (#396). The hot subsequent-reply
		// path folds this into the replier's subscription upsert (one write instead of two),
		// reporting replierLastSeenAdvanced=true; this standalone $max covers the paths that
		// write no replier subscription (migration, self-reply, system message). Best-effort.
		if !replierLastSeenAdvanced {
			if err := h.threadStore.AdvanceThreadSubscriptionLastSeen(ctx, threadRoomID, evt.Message.UserAccount, evt.Message.CreatedAt); err != nil {
				slog.WarnContext(ctx, "advance replier thread lastSeenAt failed",
					"error", err, "thread_room_id", threadRoomID, "account", evt.Message.UserAccount,
					"request_id", natsutil.RequestIDFromContext(ctx))
			}
		}
		if err := h.markThreadMentions(ctx, &evt.Message, threadRoomID, evt.SiteID, isMigration); err != nil {
			return fmt.Errorf("mark thread mentions: %w", err)
		}
		newTcount, err := h.store.SaveThreadMessage(ctx, &evt.Message, sender, evt.SiteID, threadRoomID)
		if err != nil {
			return fmt.Errorf("save thread message: %w", err)
		}
		debugFlowPersisted(ctx, evt.Message.ID, true)
		// Suppress the live tcount badge for migrated replies: the source already delivered it, and the
		// badge carries no migration header so broadcast-worker would re-notify. The count is persisted above.
		if newTcount != nil && !isMigration {
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

// reprojectUnverifiedQuote corrects an untrusted quoted-parent snapshot before the
// durable write. When the gatekeeper set QuotedParentUnverified (it degraded to a
// server-built placeholder during a transient history outage), re-read the
// authoritative snapshot from Cassandra and overwrite the sensitive fields —
// preserving the gatekeeper-built MessageLink — or drop the quote when the parent
// can't be confirmed, so a fabricated snapshot never persists. No-op on the happy
// path; a Cassandra failure NAKs and replays.
func (h *Handler) reprojectUnverifiedQuote(ctx context.Context, evt *model.MessageEvent) error {
	if !evt.QuotedParentUnverified || evt.Message.QuotedParentMessage == nil {
		return nil
	}
	q := evt.Message.QuotedParentMessage
	snap, found, err := h.store.GetQuotedParentSnapshot(ctx, q.MessageID)
	if err != nil {
		return fmt.Errorf("get authoritative quoted parent %s: %w", q.MessageID, err)
	}
	// The quote is resolved authoritatively from here on, so the marker is
	// cleared regardless of whether the parent was found.
	evt.QuotedParentUnverified = false
	if !found {
		// Accepted trade-off: MESSAGES_CANONICAL doesn't order the parent's persist
		// relative to this reply, so a parent row still in flight reads as not-found
		// and the quote is dropped permanently (no bounded retry). Quoting a parent
		// that hasn't landed yet is a narrow race; dropping the quote is preferred
		// over NAK-looping the reply on the hot path.
		slog.WarnContext(ctx, "unverified quoted parent not found in history — dropping quote",
			"request_id", natsutil.RequestIDFromContext(ctx),
			"quoted_id", q.MessageID, "message_id", evt.Message.ID)
		evt.Message.QuotedParentMessage = nil
		return nil
	}
	if !authoritativeQuoteMatchesConversation(&evt.Message, snap) {
		// On the degrade path the gatekeeper could not enforce the
		// same-conversation rule (history was down), so re-check it here against
		// the authoritative snapshot. Drop the quote rather than persist a parent
		// from a foreign room/thread that a client referenced by raw message ID.
		slog.WarnContext(ctx, "authoritative quoted parent is in a different room/thread — dropping quote",
			"request_id", natsutil.RequestIDFromContext(ctx),
			"quoted_id", q.MessageID, "message_id", evt.Message.ID,
			"quoted_room_id", snap.RoomID, "message_room_id", evt.Message.RoomID,
			"quoted_thread_parent_id", snap.ThreadParentID,
			"message_thread_parent_id", evt.Message.ThreadParentMessageID)
		evt.Message.QuotedParentMessage = nil
		return nil
	}
	snap.MessageLink = q.MessageLink // preserve the gatekeeper-built (trusted) link
	evt.Message.QuotedParentMessage = snap
	return nil
}

// authoritativeQuoteMatchesConversation reports whether the authoritative quoted
// parent snap belongs to the same conversation as msg — same room and same
// thread context — mirroring the gatekeeper's checkQuoteThreadContext rule plus a
// room check. The gatekeeper enforces this on the happy path (via history-service);
// the worker re-checks it here for the degraded re-projection, where the snapshot is
// read from messages_by_id by ID alone with no access control.
func authoritativeQuoteMatchesConversation(msg *model.Message, snap *cassandra.QuotedParentMessage) bool {
	if snap.RoomID != msg.RoomID {
		return false
	}
	if msg.ThreadParentMessageID == "" {
		// Main-room message: may only quote a main-room parent.
		return snap.ThreadParentID == ""
	}
	// Thread reply: quote a same-thread message, or the thread's own root (a
	// main-room message whose ID is the thread parent).
	return snap.ThreadParentID == msg.ThreadParentMessageID ||
		(snap.ThreadParentID == "" && snap.MessageID == msg.ThreadParentMessageID)
}

// debugFlowPersisted emits the flow-rung breadcrumb marking the message as
// stored — the "was it persisted?" handoff for this hop. Metadata only.
func debugFlowPersisted(ctx context.Context, messageID string, thread bool) {
	slog.Log(ctx, logctx.LevelFlow, "message-worker persisted",
		"phase", "persisted", "request_id", natsutil.RequestIDFromContext(ctx),
		"message_id", messageID, "thread", thread)
}

// handleThreadRoomAndSubscriptions resolves the ThreadRoom via a single upserting
// EnsureThreadRoom call (one round trip, no failed insert on the hot path). On the first
// reply (EnsureThreadRoom created the room) it inserts ThreadSubscriptions for the parent
// author and replier; on subsequent replies it upserts only the replier's subscription
// (the parent author's was created on the first reply) and bumps the last-message pointer.
// It returns the threadRoomID so the caller can pass it to SaveThreadMessage, and a bool
// reporting whether the replier's lastSeenAt was already advanced as part of that
// subscription write (so the caller can skip the standalone $max).
//
// `replier` may be nil for system messages with no real user (rare in thread
// paths); subscriptions for the replier are skipped in that case.
func (h *Handler) handleThreadRoomAndSubscriptions(ctx context.Context, msg *model.Message, eventSiteID string, replier *model.User, isMigration bool) (string, bool, error) {
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

	existingRoom, created, err := h.threadStore.EnsureThreadRoom(ctx, &threadRoom)
	if err != nil {
		return "", false, fmt.Errorf("ensure thread room: %w", err)
	}
	if created {
		// First reply (once per thread): it advances the replier's lastSeenAt via the standalone
		// $max in the caller, so it reports replierLastSeenAdvanced=false.
		return existingRoom.ID, false, h.handleFirstThreadReply(ctx, msg, eventSiteID, existingRoom.ID, replier, now, isMigration)
	}
	return h.handleSubsequentThreadReply(ctx, msg, eventSiteID, existingRoom, replier, now, isMigration)
}

// handleFirstThreadReply runs after the thread room has just been created.
// It inserts subscriptions for the parent author and (if distinct) the replier.
// Subscription.SiteID is the room's site (eventSiteID); the owner's home site
// is resolved separately and used only to decide cross-site inbox routing.
func (h *Handler) handleFirstThreadReply(ctx context.Context, msg *model.Message, eventSiteID, threadRoomID string, replier *model.User, now time.Time, isMigration bool) error {
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

	// Parent author joins the thread's replyAccounts set so they appear as a
	// follower in notification-worker and history-service's "following" feed,
	// even before they reply themselves. $addToSet dedups against the replier seed.
	if err := h.threadStore.AddReplyAccounts(ctx, threadRoomID, []string{parentSender.Account}); err != nil {
		return fmt.Errorf("add parent author to thread room replyAccounts: %w", err)
	}

	// Skip thread_subscription writes + cross-site inbox for migrated replies: the collections migration
	// owns them (migrated unfiltered); re-deriving here would dup-key the unique (threadRoomId,userAccount).
	if !isMigration {
		parentOwnerSite, err := h.lookupOwnerSiteID(ctx, parentSender.ID, "first-reply parent")
		if err != nil {
			return fmt.Errorf("lookup parent owner site: %w", err)
		}
		parentSub := h.buildThreadSubscription(msg, threadRoomID, parentSender.ID, parentSender.Account, eventSiteID, now)
		if err := h.threadStore.InsertThreadSubscription(ctx, parentSub); err != nil {
			return fmt.Errorf("insert parent author thread subscription: %w", err)
		}
		// Inbox publish is gated on parentOwnerSite — if the parent user is missing
		// from userStore, we can't route the cross-site copy, but the local Insert
		// above is independent of that and still happens.
		if parentOwnerSite != "" {
			if err := h.publishThreadSubInboxIfRemote(ctx, parentSub, parentOwnerSite, msg.ID); err != nil {
				return fmt.Errorf("publish parent thread subscription inbox: %w", err)
			}
		}

		if replier != nil && msg.UserID != parentSender.ID {
			replierSub := h.buildThreadSubscription(msg, threadRoomID, msg.UserID, msg.UserAccount, eventSiteID, now)
			if err := h.threadStore.InsertThreadSubscription(ctx, replierSub); err != nil {
				return fmt.Errorf("insert replier thread subscription: %w", err)
			}
			if err := h.publishThreadSubInboxIfRemote(ctx, replierSub, replier.SiteID, msg.ID); err != nil {
				return fmt.Errorf("publish replier thread subscription inbox: %w", err)
			}
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

// handleSubsequentThreadReply runs when the thread room already existed (resolved by the
// caller's EnsureThreadRoom and passed in as existingRoom, so no second fetch is needed).
// It upserts only the replier's subscription — folding the replier's own lastSeenAt
// advance into that single write (#396) — then bumps the room's last-message pointer.
// The parent author's subscription is intentionally NOT re-upserted here: it was
// created on the first reply, so re-writing it on every subsequent reply is a no-op
// write on the hottest collection (thread_subscriptions). Trade-off: a crash between
// the first reply's room creation and its parent InsertThreadSubscription would leave
// the parent unsubscribed for this thread — a narrow window not worth a per-reply write
// to self-heal.
//
// Returns the existing thread room ID and whether the replier's lastSeenAt was
// advanced as part of the subscription write (false on the migration / self-reply /
// nil-replier paths, where the caller's standalone $max handles it instead).
// Subscription.SiteID is the room's site (eventSiteID); the replier's own home site
// (replier.SiteID) drives cross-site inbox routing without an extra lookup.
func (h *Handler) handleSubsequentThreadReply(ctx context.Context, msg *model.Message, eventSiteID string, existingRoom *model.ThreadRoom, replier *model.User, now time.Time, isMigration bool) (string, bool, error) {
	// Migrated replies: resolve the parent for replyAccounts, but skip all thread_subscription writes (collections owns them).
	parentFound := true
	replierLastSeenAdvanced := false
	parentSender, err := h.store.GetMessageSender(ctx, msg.ThreadParentMessageID)
	switch {
	case err == nil:
		if !isMigration && replier != nil && msg.UserID != parentSender.ID {
			replierSub := h.buildThreadSubscription(msg, existingRoom.ID, msg.UserID, msg.UserAccount, eventSiteID, now)
			if err := h.threadStore.UpsertThreadSubscriptionAdvancingLastSeen(ctx, replierSub, msg.CreatedAt); err != nil {
				return "", false, fmt.Errorf("upsert replier thread subscription: %w", err)
			}
			replierLastSeenAdvanced = true
			if err := h.publishThreadSubInboxIfRemote(ctx, replierSub, replier.SiteID, msg.ID); err != nil {
				return "", false, fmt.Errorf("publish replier thread subscription inbox: %w", err)
			}
		}
	case errors.Is(err, errMessageNotFound):
		parentFound = false
		slog.WarnContext(ctx, "thread reply parent not found — skipping parent subscription upsert",
			"parentMessageID", msg.ThreadParentMessageID,
			"replyID", msg.ID,
			"request_id", natsutil.RequestIDFromContext(ctx))
		if !isMigration && replier != nil {
			replierSub := h.buildThreadSubscription(msg, existingRoom.ID, msg.UserID, msg.UserAccount, eventSiteID, now)
			if err := h.threadStore.UpsertThreadSubscriptionAdvancingLastSeen(ctx, replierSub, msg.CreatedAt); err != nil {
				return "", false, fmt.Errorf("upsert replier thread subscription: %w", err)
			}
			replierLastSeenAdvanced = true
			if err := h.publishThreadSubInboxIfRemote(ctx, replierSub, replier.SiteID, msg.ID); err != nil {
				return "", false, fmt.Errorf("publish replier thread subscription inbox: %w", err)
			}
		}
	default:
		return "", false, fmt.Errorf("get parent message sender: %w", err)
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
		return "", false, fmt.Errorf("update thread room last message: %w", err)
	}

	// The parent message's thread_room_id is stamped once, on the first reply
	// (handleFirstThreadReply). It never changes for a thread, so re-stamping it on every
	// subsequent reply is a redundant Cassandra write — skipped here. Trade-off: if the
	// first reply created the room but crashed before its stamp landed, the parent stays
	// unstamped (no per-reply self-heal) — a narrow window not worth a write on every reply.

	return existingRoom.ID, replierLastSeenAdvanced, nil
}

// lookupOwnerSiteID resolves a user's home site by ID.
// Returns ("", nil) when the user is not found (logs a warning) so callers
// can skip that user gracefully — parallels the errMessageNotFound branch
// already in this file. Other DB errors are returned for the caller to NAK on.
func (h *Handler) lookupOwnerSiteID(ctx context.Context, userID, role string) (string, error) {
	user, err := h.userStore.FindUserByID(ctx, userID)
	if err != nil {
		if errors.Is(err, userstore.ErrUserNotFound) {
			slog.WarnContext(ctx, "owner user not found — skipping cross-site inbox publish; local thread subscription insert/upsert continues",
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
// used only for the cross-site inbox routing.
func (h *Handler) markThreadMentions(ctx context.Context, msg *model.Message, threadRoomID, eventSiteID string, isMigration bool) error {
	// Collect mention candidates (excluding @all and the sender) and their accounts
	// in one pass; candidates hold pointers into msg.Mentions to avoid struct copies.
	candidates := make([]*model.Participant, 0, len(msg.Mentions))
	accounts := make([]string, 0, len(msg.Mentions))
	for i := range msg.Mentions {
		p := &msg.Mentions[i]
		if p.Account == "all" {
			continue
		}
		if p.UserID == msg.UserID {
			continue
		}
		candidates = append(candidates, p)
		accounts = append(accounts, p.Account)
	}
	if len(candidates) == 0 {
		return nil
	}

	windows, err := h.threadStore.GetHistorySharedSince(ctx, msg.RoomID, accounts)
	if err != nil {
		return fmt.Errorf("get history windows for thread mentions: %w", err)
	}

	var mentionedAccounts []string
	for _, p := range candidates {
		hss, isMember := windows[p.Account]
		// Skip non-members (no room subscription) and members whose history window
		// starts after the thread's parent — neither may see the parent, so they are
		// not subscribed, not inboxed, and not added as a follower.
		if !isMember || !mentionVisible(hss, msg.ThreadParentMessageCreatedAt) {
			continue
		}
		// Migrated replies skip the hasMention write + inbox (collections owns it); still collect accounts for replyAccounts.
		if !isMigration {
			sub := h.buildThreadSubscription(msg, threadRoomID, p.UserID, p.Account, eventSiteID, msg.CreatedAt)
			sub.HasMention = true
			if err := h.threadStore.MarkThreadSubscriptionMention(ctx, sub); err != nil {
				return fmt.Errorf("mark thread subscription mention for user %s: %w", p.UserID, err)
			}
			if err := h.publishThreadSubInboxIfRemote(ctx, sub, p.SiteID, msg.ID); err != nil {
				return fmt.Errorf("publish thread mention inbox for user %s: %w", p.UserID, err)
			}
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

// publishThreadSubInboxIfRemote federates a thread_subscription_upserted event
// to ownerSiteID when that site differs from the local site, via the durable
// OUTBOX relay (outbox-worker forwards it to the destination INBOX with
// retry-forever, so a destination outage delays — never drops — the event
// within retention). Same-site is a no-op; empty ownerSiteID is a no-op that
// logs a warning (caller bug). ownerSiteID is the subscription owner's home
// site — NOT sub.SiteID, which is the room's home site.
func (h *Handler) publishThreadSubInboxIfRemote(ctx context.Context, sub *model.ThreadSubscription, ownerSiteID, msgID string) error {
	if ownerSiteID == "" {
		slog.WarnContext(ctx, "owner siteID empty, skipping outbox publish",
			"threadRoomID", sub.ThreadRoomID, "user_id", sub.UserID, "msgID", msgID,
			"request_id", natsutil.RequestIDFromContext(ctx))
		return nil
	}
	// outbox.Publish also no-ops a local destination, but short-circuit here so the
	// marshal below is skipped on the common same-site path.
	if ownerSiteID == h.siteID {
		return nil
	}

	payload, err := sonic.Marshal(sub)
	if err != nil {
		return fmt.Errorf("marshal thread subscription: %w", err)
	}
	// Dedup-ID seed (threadRoomID + userID + msg.ID + hasMention + destSiteID):
	// msg.ID is stable across MESSAGES_CANONICAL redeliveries so the same publish
	// yields the same ID; different users on the same destination differ via userID;
	// hasMention is in the seed so a HasMention=false upsert and a later
	// HasMention=true update get distinct dedup IDs (else stream-level dedup would
	// swallow the mention update). It rides the OUTBOX publish as its Nats-Msg-Id
	// AND the forward's Nats-Msg-Id at the destination.
	dedupID := fmt.Sprintf("thread-sub-inbox:%s:%s:%s:%t:%s", sub.ThreadRoomID, sub.UserID, msgID, sub.HasMention, ownerSiteID)
	if err := outbox.Publish(ctx, h.publish, h.siteID, sub.RoomID, ownerSiteID,
		model.InboxThreadSubscriptionUpserted, payload, dedupID, time.Now().UTC().UnixMilli()); err != nil {
		return fmt.Errorf("publish thread subscription outbox to %s: %w", ownerSiteID, err)
	}
	return nil
}

// publishThreadReplyEvent fires a badge event via core NATS so broadcast-worker
// can update the reply-count badge for thread followers. Published to
// chat.server.broadcast.{siteID}.thread.tcount (not MESSAGES_CANONICAL) because
// badge updates are best-effort and do not belong in the message CRUD event store.
func (h *Handler) publishThreadReplyEvent(ctx context.Context, msg *model.Message, newTcount int) error {
	tlm := msg.CreatedAt
	evt := model.MessageEvent{
		Event: model.EventThreadReplyAdded,
		Message: model.Message{
			ID:                    msg.ID,
			RoomID:                msg.RoomID,
			ThreadParentMessageID: msg.ThreadParentMessageID,
		},
		SiteID:             h.siteID,
		Timestamp:          time.Now().UTC().UnixMilli(),
		NewTCount:          &newTcount,
		NewThreadLastMsgAt: &tlm,
	}
	data, err := sonic.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal thread reply event: %w", err)
	}
	return h.publish(ctx, subject.ServerBroadcastThreadTCount(h.siteID), data, "")
}
