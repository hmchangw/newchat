package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/bytedance/sonic"

	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/jsretry"
	"github.com/hmchangw/chat/pkg/logctx"
	"github.com/hmchangw/chat/pkg/mention"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/obs"
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
	// outboxFailover targets the buddy-hosted OUTBOX-FAILOVER instead of the
	// live OUTBOX. Set on the failover lane, whose whole reason to exist is that
	// the cluster hosting the live OUTBOX is unreachable.
	outboxFailover bool
	metrics        *persistenceMetrics
}

type messageWorkerHandlerOption func(*messageWorkerHandlerOptions)

type messageWorkerHandlerOptions struct {
	metrics        *persistenceMetrics
	outboxFailover bool
}

// withOutboxLane selects the buddy-hosted OUTBOX-FAILOVER lane, which is how
// this site keeps federating outward while its own NATS is down.
func withOutboxLane(failover bool) messageWorkerHandlerOption {
	return func(opts *messageWorkerHandlerOptions) { opts.outboxFailover = failover }
}

func withPersistenceMetrics(metrics *persistenceMetrics) messageWorkerHandlerOption {
	return func(opts *messageWorkerHandlerOptions) { opts.metrics = metrics }
}

func NewHandler(store Store, userStore userstore.UserStore, threadStore ThreadStore, siteID string, publish PublishFunc, options ...messageWorkerHandlerOption) *Handler {
	var opts messageWorkerHandlerOptions
	for _, option := range options {
		option(&opts)
	}
	if opts.metrics == nil {
		opts.metrics = newPersistenceMetrics(otel.Meter("message-worker"))
	}
	return &Handler{
		store:          store,
		userStore:      userStore,
		threadStore:    threadStore,
		siteID:         siteID,
		publish:        publish,
		outboxFailover: opts.outboxFailover,
		metrics:        opts.metrics,
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
		natsmetrics.MarkTerminalFromContext(ctx, natsmetrics.TerminalInvalidPayload)
		// Malformed payload — it will never parse on redelivery. Mark permanent
		// so the handler Acks (drops) it instead of retrying until MaxDeliver.
		return errcode.Permanent(errcode.BadRequest("malformed message event"))
	}
	ctx = obs.ContextWithIdentity(ctx, evt.Message.UserAccount, evt.Message.RoomID, evt.SiteID)

	resolved, err := mention.Resolve(ctx, evt.Message.Content, h.userStore.FindUsersByAccounts)
	if err != nil {
		// Fail-open: mention resolution is enrichment, not durability. The content
		// (including the literal @tokens) persists intact. Resolve still returns
		// whatever it could — the user store's warm entries, plus @all, which needs
		// no lookup — so keep those rather than discarding a partial answer; the
		// unresolved accounts are the only ones that lose their notification.
		// Blocking the write would be strictly worse.
		slog.WarnContext(ctx, "mention resolution degraded, persisting the mentions that resolved",
			"error", err, "message_id", evt.Message.ID,
			"resolved", len(resolved.Participants), "parsed", len(resolved.Accounts),
			"request_id", natsutil.RequestIDFromContext(ctx))
	}
	evt.Message.Mentions = resolved.Participants
	// debug: mention resolution is the first decision step — count only, no content.
	slog.DebugContext(ctx, "message-worker mentions resolved",
		"request_id", natsutil.RequestIDFromContext(ctx), "mentions", len(evt.Message.Mentions))

	var sender *cassParticipant
	user, err := h.userStore.FindUserByAccount(ctx, evt.Message.UserAccount)
	switch {
	// user != nil is defensive, not a live fix: every current UserStore reports a
	// miss as ErrUserNotFound rather than (nil, nil). It costs one comparison and
	// keeps a future implementation that returns the looser (nil, nil) from
	// panicking on the sole message-persistence path.
	case err == nil && user != nil:
		sender = senderFrom(user, evt.Message.UserAccount)
	case model.IsSystemMessageType(evt.Message.Type):
		// System messages may have no real user; proceed with nil sender.
		// A client type (e.g. important) has a real sender, so a lookup failure
		// there falls through to the fail-open branch like a normal message.
		slog.WarnContext(ctx, "user not found for system message, using nil sender",
			"account", evt.Message.UserAccount, "type", evt.Message.Type,
			"request_id", natsutil.RequestIDFromContext(ctx))
	default:
		// Fail-open: project the sender from the canonical event, which already
		// carries the identity the gatekeeper resolved at send time. Only the
		// EngName/ChineseName split is lost (UserDisplayName is already the
		// composed render-ready name), so the write proceeds rather than
		// NAK-buffering until Mongo returns.
		slog.WarnContext(ctx, "sender lookup failed, projecting sender from event",
			"error", err, "account", evt.Message.UserAccount,
			"message_id", evt.Message.ID,
			"request_id", natsutil.RequestIDFromContext(ctx))
		// SiteID is deliberately left zero. It reaches
		// publishThreadSubInboxIfRemote as ownerSiteID — the subscription
		// owner's HOME site, not the room's site (evt.SiteID) — and we do not
		// know it without the user doc. Guessing it either misroutes the
		// federated event or silently short-circuits it; an empty value takes
		// the documented skip-and-warn branch instead.
		user = &model.User{
			ID:      evt.Message.UserID,
			Account: evt.Message.UserAccount,
			EngName: evt.Message.UserDisplayName,
		}
		// Derived from the projected user, not written out a second time: the
		// two identities must stay the same identity, and a field added to
		// cassParticipant must not be able to reach one and miss the other.
		sender = senderFrom(user, evt.Message.UserAccount)
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
			switch {
			case found:
				evt.Message.ThreadParentMessageCreatedAt = &createdAt
			case !parentResolveExhausted(ctx):
				// The parent's own canonical write may still land — MESSAGES-CANONICAL
				// does not order it relative to this reply — so retry rather than
				// persist a null and corrupt the parent's partition coords.
				return fmt.Errorf("thread parent %s not yet persisted in messages_by_id", evt.Message.ThreadParentMessageID)
			default:
				// Budget spent: the choice is now "persist without parent coords" or
				// "lose the reply". message-worker is the sole persister of history and
				// nothing dead-letters a MaxDeliver drop, so salvage the content. The
				// writes below already tolerate a nil parent createdAt — the
				// thread_room_id stamp is skipped and logged instead.
				slog.ErrorContext(ctx, "thread parent never persisted — saving reply without parent linkage",
					"request_id", natsutil.RequestIDFromContext(ctx),
					"replyID", evt.Message.ID,
					"parentMessageID", evt.Message.ThreadParentMessageID,
					"room_id", evt.Message.RoomID)
			}
		}

		// Resolve (or create) the thread room first so we have the threadRoomID
		// before persisting the message to Cassandra.
		threadRoomID, followers, err := h.handleThreadRoomAndSubscriptions(ctx, &evt.Message, evt.SiteID, user, isMigration)
		if err != nil {
			return fmt.Errorf("handle thread room and subscriptions: %w", err)
		}
		// Replying implies the replier read up to their own reply: advance their thread
		// lastSeenAt so the read-floor doesn't count them (#396). Best-effort. Runs on
		// migration too ($max only moves forward → lands on the replier's last reply).
		if err := h.threadStore.AdvanceThreadSubscriptionLastSeen(ctx, threadRoomID, evt.Message.UserAccount, evt.Message.CreatedAt); err != nil {
			slog.WarnContext(ctx, "advance replier thread lastSeenAt failed",
				"error", err, "thread_room_id", threadRoomID, "account", evt.Message.UserAccount,
				"request_id", natsutil.RequestIDFromContext(ctx))
		}
		mentionedAccounts, err := h.markThreadMentions(ctx, &evt.Message, threadRoomID, evt.SiteID, isMigration)
		if err != nil {
			return fmt.Errorf("mark thread mentions: %w", err)
		}
		// Suppressed for migrated replies like the thread-sub writes above: the
		// source already carries threadUnread state, and re-deriving it here
		// would re-mark threads the migration already resolved.
		if !isMigration {
			recipients := make([]string, 0, len(followers)+len(mentionedAccounts))
			recipients = append(recipients, followers...)
			recipients = append(recipients, mentionedAccounts...)
			if err := h.fanOutThreadUnread(ctx, evt.Message.RoomID, evt.Message.ThreadParentMessageID, evt.Message.ID, evt.Message.UserAccount, recipients); err != nil {
				return fmt.Errorf("fan out thread unread: %w", err)
			}
		}
		newTcount, err := h.store.SaveThreadMessage(ctx, &evt.Message, sender, evt.SiteID, threadRoomID)
		if err != nil {
			h.metrics.Record(ctx, kindThreadReply, persistError)
			return fmt.Errorf("save thread message: %w", err)
		}
		h.metrics.Record(ctx, kindThreadReply, persistSuccess)
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
			h.metrics.Record(ctx, messageKind(&evt.Message), persistError)
			return fmt.Errorf("save message: %w", err)
		}
		h.metrics.Record(ctx, messageKind(&evt.Message), persistSuccess)
		debugFlowPersisted(ctx, evt.Message.ID, false)
	}

	return nil
}

func messageKind(msg *model.Message) messageKindLabel {
	if msg.ThreadParentMessageID != "" {
		return kindThreadReply
	}
	if model.IsSystemMessageType(msg.Type) {
		return kindSystem
	}
	if msg.Type == "" || msg.Type == model.MessageTypeImportant {
		return kindUser
	}
	return kindUnknown
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
		// Accepted trade-off: MESSAGES-CANONICAL doesn't order the parent's persist
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

// handleThreadRoomAndSubscriptions creates the ThreadRoom on first reply and
// inserts ThreadSubscriptions for the parent author and replier. On subsequent
// replies it upserts both subscriptions and bumps the room's last-message pointer.
// It returns the threadRoomID so the caller can pass it to SaveThreadMessage,
// plus the thread's pre-existing followers (the accounts fanOutThreadUnread
// should mark unread — the parent author alone on a first reply, or the room's
// established ReplyAccounts on a subsequent one).
//
// `replier` is nil only for system messages with no real user, and the replier
// subscription is skipped in that case. A real user whose Mongo sender lookup
// failed (fail-open) is projected from the canonical event, so their
// subscription still lands even during a Mongo outage.
func (h *Handler) handleThreadRoomAndSubscriptions(ctx context.Context, msg *model.Message, eventSiteID string, replier *model.User, isMigration bool) (string, []string, error) {
	now := msg.CreatedAt

	// history-service gates the thread list on threadParentCreatedAt >= the
	// member's historySharedSince (mongorepo.buildBaseThreadMatch), so a zero
	// value here hides the thread from every member with a history window —
	// permanently, and silently. On the salvage path the parent does not exist in
	// history, so the thread's only content is this reply: gate it on the reply's
	// own time, which is exactly what a member entitled to see the reply is
	// entitled to see. Deliberately scoped to this document — msg's own parent
	// coords stay unknown, because fabricating them would point the
	// messages_by_id/messages_by_room writes at a partition that isn't the parent's.
	parentCreatedAt := msg.CreatedAt
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
		followers, ferr := h.handleFirstThreadReply(ctx, msg, eventSiteID, threadRoom.ID, replier, now, isMigration)
		return threadRoom.ID, followers, ferr
	case errors.Is(err, errThreadRoomExists):
		return h.handleSubsequentThreadReply(ctx, msg, eventSiteID, replier, now, isMigration)
	default:
		return "", nil, fmt.Errorf("create thread room: %w", err)
	}
}

// handleFirstThreadReply runs after the thread room has just been created.
// It inserts subscriptions for the parent author and (if distinct) the replier.
// Subscription.SiteID is the room's site (eventSiteID); the owner's home site
// is resolved separately and used only to decide cross-site inbox routing.
// Returns the parent author's account as the thread's sole pre-existing
// follower (nil when the parent message can't be resolved), for the caller to
// fold into fanOutThreadUnread's recipient set.
func (h *Handler) handleFirstThreadReply(ctx context.Context, msg *model.Message, eventSiteID, threadRoomID string, replier *model.User, now time.Time, isMigration bool) ([]string, error) {
	parentSender, err := h.store.GetMessageSender(ctx, msg.ThreadParentMessageID)
	if err != nil {
		if errors.Is(err, errMessageNotFound) {
			slog.WarnContext(ctx, "thread reply parent not found — skipping subscription creation",
				"parentMessageID", msg.ThreadParentMessageID,
				"replyID", msg.ID,
				"request_id", natsutil.RequestIDFromContext(ctx))
			return nil, nil
		}
		return nil, fmt.Errorf("get parent message sender: %w", err)
	}

	// Parent author joins the thread's replyAccounts set so they appear as a
	// follower in notification-worker and history-service's "following" feed,
	// even before they reply themselves. $addToSet dedups against the replier seed.
	if err := h.threadStore.AddReplyAccounts(ctx, threadRoomID, []string{parentSender.Account}); err != nil {
		return nil, fmt.Errorf("add parent author to thread room replyAccounts: %w", err)
	}

	// Skip thread_subscription writes + cross-site inbox for migrated replies: the collections migration
	// owns them (migrated unfiltered); re-deriving here would dup-key the unique (threadRoomId,userAccount).
	if !isMigration {
		parentOwnerSite, err := h.lookupOwnerSiteID(ctx, parentSender.Account, "first-reply parent")
		if err != nil {
			return nil, fmt.Errorf("lookup parent owner site: %w", err)
		}
		parentSub := h.buildThreadSubscription(msg, threadRoomID, parentSender.ID, parentSender.Account, eventSiteID, now)
		if err := h.threadStore.InsertThreadSubscription(ctx, parentSub); err != nil {
			return nil, fmt.Errorf("insert parent author thread subscription: %w", err)
		}
		// Inbox publish is gated on parentOwnerSite — if the parent user is missing
		// from userStore, we can't route the cross-site copy, but the local Insert
		// above is independent of that and still happens.
		if parentOwnerSite != "" {
			if err := h.publishThreadSubInboxIfRemote(ctx, parentSub, parentOwnerSite, msg.ID); err != nil {
				return nil, fmt.Errorf("publish parent thread subscription inbox: %w", err)
			}
		}

		if replier != nil && msg.UserID != parentSender.ID {
			replierSub := h.buildThreadSubscription(msg, threadRoomID, msg.UserID, msg.UserAccount, eventSiteID, now)
			if err := h.threadStore.InsertThreadSubscription(ctx, replierSub); err != nil {
				return nil, fmt.Errorf("insert replier thread subscription: %w", err)
			}
			if err := h.publishThreadSubInboxIfRemote(ctx, replierSub, replier.SiteID, msg.ID); err != nil {
				return nil, fmt.Errorf("publish replier thread subscription inbox: %w", err)
			}
		}
	}

	// Requires ThreadParentMessageCreatedAt; missing → permanent silent thread-fetch failure.
	if msg.ThreadParentMessageCreatedAt != nil {
		if err := h.store.UpdateParentMessageThreadRoomID(ctx, msg.ThreadParentMessageID, msg.RoomID, *msg.ThreadParentMessageCreatedAt, threadRoomID); err != nil {
			return nil, fmt.Errorf("stamp thread_room_id on parent message: %w", err)
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

	return []string{parentSender.Account}, nil
}

// handleSubsequentThreadReply runs when CreateThreadRoom reported an existing room.
// Upserts subscriptions for both the parent author and the replier (idempotent
// on redelivery), then bumps the room's last-message pointer. Returns the
// existing thread room ID so the caller can pass it to SaveThreadMessage, plus
// the thread's followers as they stood *before* this reply (existingRoom's
// ReplyAccounts, captured up front) with the parent author appended when
// resolvable — legacy thread_rooms predating the parent-author seed lack the
// author in ReplyAccounts, and without the explicit append they would miss
// this reply's unread mark. That combined list is the audience
// fanOutThreadUnread should mark unread; it dedups, and the sender is
// filtered out there regardless of whether they were already a follower.
func (h *Handler) handleSubsequentThreadReply(ctx context.Context, msg *model.Message, eventSiteID string, replier *model.User, now time.Time, isMigration bool) (string, []string, error) {
	existingRoom, err := h.threadStore.GetThreadRoomByParentMessageID(ctx, msg.ThreadParentMessageID)
	if err != nil {
		return "", nil, fmt.Errorf("get existing thread room: %w", err)
	}
	followers := existingRoom.ReplyAccounts

	// Migrated replies: resolve the parent for replyAccounts, but skip all thread_subscription writes (collections owns them).
	parentFound := true
	parentSender, err := h.store.GetMessageSender(ctx, msg.ThreadParentMessageID)
	switch {
	case err == nil:
		if !isMigration {
			parentOwnerSite, lookupErr := h.lookupOwnerSiteID(ctx, parentSender.Account, "subsequent-reply parent")
			if lookupErr != nil {
				return "", nil, fmt.Errorf("lookup parent owner site: %w", lookupErr)
			}
			parentSub := h.buildThreadSubscription(msg, existingRoom.ID, parentSender.ID, parentSender.Account, eventSiteID, now)
			if err := h.threadStore.UpsertThreadSubscription(ctx, parentSub); err != nil {
				return "", nil, fmt.Errorf("upsert parent author thread subscription: %w", err)
			}
			if parentOwnerSite != "" {
				if err := h.publishThreadSubInboxIfRemote(ctx, parentSub, parentOwnerSite, msg.ID); err != nil {
					return "", nil, fmt.Errorf("publish parent thread subscription inbox: %w", err)
				}
			}
			if replier != nil && msg.UserID != parentSender.ID {
				replierSub := h.buildThreadSubscription(msg, existingRoom.ID, msg.UserID, msg.UserAccount, eventSiteID, now)
				if err := h.threadStore.UpsertThreadSubscription(ctx, replierSub); err != nil {
					return "", nil, fmt.Errorf("upsert replier thread subscription: %w", err)
				}
				if err := h.publishThreadSubInboxIfRemote(ctx, replierSub, replier.SiteID, msg.ID); err != nil {
					return "", nil, fmt.Errorf("publish replier thread subscription inbox: %w", err)
				}
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
			if err := h.threadStore.UpsertThreadSubscription(ctx, replierSub); err != nil {
				return "", nil, fmt.Errorf("upsert replier thread subscription: %w", err)
			}
			if err := h.publishThreadSubInboxIfRemote(ctx, replierSub, replier.SiteID, msg.ID); err != nil {
				return "", nil, fmt.Errorf("publish replier thread subscription inbox: %w", err)
			}
		}
	default:
		return "", nil, fmt.Errorf("get parent message sender: %w", err)
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
		return "", nil, fmt.Errorf("update thread room last message: %w", err)
	}

	// The parent author is always part of this reply's unread audience, even on
	// legacy thread_rooms created before the parent-author seed (whose
	// replyAccounts lacks them — the merge above repairs the document only for
	// FUTURE replies, after `followers` was captured). fanOutThreadUnread dedups
	// and strips the sender, so this is a no-op when the author already follows
	// or is the replier.
	if parentFound {
		followers = append(followers, parentSender.Account)
	}

	// Re-stamp handles redelivery: first attempt may have created the thread room
	// but crashed before the stamp landed. IF EXISTS in the store prevents phantom rows.
	switch {
	case parentFound && msg.ThreadParentMessageCreatedAt != nil:
		if err := h.store.UpdateParentMessageThreadRoomID(ctx, msg.ThreadParentMessageID, msg.RoomID, *msg.ThreadParentMessageCreatedAt, existingRoom.ID); err != nil {
			return "", nil, fmt.Errorf("stamp thread_room_id on parent message: %w", err)
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

	return existingRoom.ID, followers, nil
}

// lookupOwnerSiteID resolves a user's home site by account.
// Returns ("", nil) when the user is not found (logs a warning) so callers
// can skip that user gracefully — parallels the errMessageNotFound branch
// already in this file. Other DB errors are returned for the caller to NAK on.
func (h *Handler) lookupOwnerSiteID(ctx context.Context, account, role string) (string, error) {
	user, err := h.userStore.FindUserByAccount(ctx, account)
	if err != nil {
		if errors.Is(err, userstore.ErrUserNotFound) {
			slog.WarnContext(ctx, "owner user not found — skipping cross-site inbox publish; local thread subscription insert/upsert continues",
				"account", account, "role", role,
				"request_id", natsutil.RequestIDFromContext(ctx))
			return "", nil
		}
		return "", fmt.Errorf("lookup user %s: %w", account, err)
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
// used only for the cross-site inbox routing. Returns the mentioned accounts
// (nil if none) so the caller can fold them into fanOutThreadUnread's
// recipient set alongside the thread's existing followers.
func (h *Handler) markThreadMentions(ctx context.Context, msg *model.Message, threadRoomID, eventSiteID string, isMigration bool) ([]string, error) {
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
		return nil, nil
	}

	windows, err := h.threadStore.GetHistorySharedSince(ctx, msg.RoomID, accounts)
	if err != nil {
		return nil, fmt.Errorf("get history windows for thread mentions: %w", err)
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
				return nil, fmt.Errorf("mark thread subscription mention for user %s: %w", p.UserID, err)
			}
			if err := h.publishThreadSubInboxIfRemote(ctx, sub, p.SiteID, msg.ID); err != nil {
				return nil, fmt.Errorf("publish thread mention inbox for user %s: %w", p.UserID, err)
			}
		}
		mentionedAccounts = append(mentionedAccounts, p.Account)
	}
	if len(mentionedAccounts) > 0 {
		if err := h.threadStore.AddReplyAccounts(ctx, threadRoomID, mentionedAccounts); err != nil {
			return nil, fmt.Errorf("add mentioned accounts to thread room replyAccounts: %w", err)
		}
	}
	return mentionedAccounts, nil
}

// fanOutThreadUnread marks the reply's parent thread unread for every
// follower except the sender: one local UpdateMany plus one
// thread_unread_added outbox event per remote home site. recipients may
// contain the sender and duplicates; both are stripped before any write.
//
// msgID must be part of the outbox dedup key: reads clear threadUnread, so a
// second reply after a read is a genuinely new unread event — keying only on
// (parentMessageID, site) would let the Nats-Msg-Id dedup window swallow it.
func (h *Handler) fanOutThreadUnread(ctx context.Context, roomID, parentMessageID, msgID, sender string, recipients []string) error {
	accounts := make([]string, 0, len(recipients))
	seen := map[string]struct{}{sender: {}}
	for _, a := range recipients {
		if _, dup := seen[a]; dup {
			continue
		}
		seen[a] = struct{}{}
		accounts = append(accounts, a)
	}
	if len(accounts) == 0 {
		return nil
	}
	if err := h.threadStore.AddThreadUnread(ctx, roomID, parentMessageID, accounts); err != nil {
		return fmt.Errorf("add thread unread: %w", err)
	}

	// One batched lookup resolves every recipient's home site; a lookup error
	// NAKs, an unknown user just isn't federated (the local write stands).
	users, err := h.userStore.FindUsersByAccounts(ctx, accounts)
	if err != nil {
		return fmt.Errorf("lookup thread-unread recipients: %w", err)
	}
	siteByAccount := make(map[string]string, len(users))
	for i := range users {
		siteByAccount[users[i].Account] = users[i].SiteID
	}
	bySite := map[string][]string{}
	var missing []string
	for _, a := range accounts {
		site, ok := siteByAccount[a]
		switch {
		case !ok:
			missing = append(missing, a)
		case site != "" && site != h.siteID:
			bySite[site] = append(bySite[site], a)
		}
	}
	if len(missing) > 0 {
		slog.WarnContext(ctx, "thread-unread recipients not found — skipping federation",
			"accounts", missing, "request_id", natsutil.RequestIDFromContext(ctx))
	}

	now := time.Now().UTC().UnixMilli()
	for site, accs := range bySite {
		payload, err := sonic.Marshal(model.ThreadUnreadAddedEvent{
			RoomID: roomID, ParentMessageID: parentMessageID, Accounts: accs, Timestamp: now,
		})
		if err != nil {
			return errcode.MarshalFailed("thread_unread_added event", err)
		}
		dedupID := fmt.Sprintf("thread-unread:%s:%s:%s", parentMessageID, msgID, site)
		if err := outbox.PublishTo(ctx, h.publish, h.siteID, roomID, site, model.InboxThreadUnreadAdded, payload, dedupID, now, h.outboxFailover); err != nil {
			return fmt.Errorf("federate thread_unread_added to %s: %w", site, err)
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
		return errcode.MarshalFailed("thread subscription", err)
	}
	// Dedup-ID seed (threadRoomID + userID + msg.ID + hasMention + destSiteID):
	// msg.ID is stable across MESSAGES-CANONICAL redeliveries so the same publish
	// yields the same ID; different users on the same destination differ via userID;
	// hasMention is in the seed so a HasMention=false upsert and a later
	// HasMention=true update get distinct dedup IDs (else stream-level dedup would
	// swallow the mention update). It rides the OUTBOX publish as its Nats-Msg-Id
	// AND the forward's Nats-Msg-Id at the destination.
	dedupID := fmt.Sprintf("thread-sub-inbox:%s:%s:%s:%t:%s", sub.ThreadRoomID, sub.UserID, msgID, sub.HasMention, ownerSiteID)
	if err := outbox.PublishTo(ctx, h.publish, h.siteID, sub.RoomID, ownerSiteID,
		model.InboxThreadSubscriptionUpserted, payload, dedupID, time.Now().UTC().UnixMilli(), h.outboxFailover); err != nil {
		return fmt.Errorf("publish thread subscription outbox to %s: %w", ownerSiteID, err)
	}
	return nil
}

// publishThreadReplyEvent fires a badge event via core NATS so broadcast-worker
// can update the reply-count badge for thread followers. Published to
// chat.server.broadcast.{siteID}.thread.tcount (not MESSAGES-CANONICAL) because
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
		return errcode.MarshalFailed("thread reply event", err)
	}
	return h.publish(ctx, subject.ServerBroadcastThreadTCount(h.siteID), data, "")
}

// senderFrom renders the Cassandra participant for a resolved user. One mapping
// for both identities the create path can produce — the one looked up in Mongo
// and the one projected from the canonical event when that lookup fails — so a
// field added to cassParticipant cannot reach one and miss the other. account
// stays a parameter because the event's account is what the row is keyed by.
func senderFrom(u *model.User, account string) *cassParticipant {
	return &cassParticipant{
		ID:          u.ID,
		EngName:     u.EngName,
		CompanyName: u.ChineseName,
		Account:     account,
	}
}

// parentResolveAttempts caps how many deliveries are spent waiting for a thread
// parent's own canonical write to land. The race it covers is milliseconds —
// message-worker persists the parent as soon as it processes it — so the full
// MaxDeliver budget buys nothing and costs a lot: on DefaultBackoff each waiting
// reply holds its ack-pending slot for 756s, and ~1.3 of them per second fills
// the consumer's budget and stops it consuming anything at all.
const parentResolveAttempts = 2

// parentResolveExhausted reports whether the parent-resolution retry budget is
// spent for this delivery. An untracked context or unreadable metadata reports
// false, so a missing count retries rather than silently degrading the write.
func parentResolveExhausted(ctx context.Context) bool {
	attempt, ok := natsmetrics.DeliveryAttemptFromContext(ctx)
	return ok && attempt >= parentResolveAttempts
}
