package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/migration"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
)

// sourceThreadSub is the subset of a company_thread_subscriptions doc the mapper decodes (handles $date).
type sourceThreadSub struct {
	ID string `bson:"_id"`
	U  struct {
		ID       string `bson:"_id"`
		Username string `bson:"username"`
	} `bson:"u"`
	RID           string `bson:"rid"`
	ParentMessage struct {
		ID string `bson:"_id"`
	} `bson:"parentMessage"`
	LastSeenAt    *time.Time `bson:"lastSeenAt"`
	UnreadMention int        `bson:"unreadMention"`
	CreatedAt     time.Time  `bson:"createdAt"`
}

// handleThreadSub maps a company_thread_subscriptions change event (§4.4 / §4.0): delete → skip;
// insert/replace/update → resolve the thread_room + user FKs, then publish thread_subscription_upserted.
//
//nolint:gocritic // ev passed by value to mirror handle's signature; off the hot path.
func (h *handler) handleThreadSub(ctx context.Context, ev oplogEvent) error {
	reqID := natsutil.RequestIDFromContext(ctx)

	if ev.Op == "delete" {
		// Un-actionable: the delete event carries only the source _id, and there is no inbox
		// removal handler for thread-sub unfollows (spec §4.4 / D2).
		slog.Debug("skip thread_sub delete (un-actionable, no inbox removal handler)",
			"eventId", ev.EventID, "request_id", reqID)
		h.metrics.onSkipped(ctx, "thread_sub_delete")
		return migration.ErrSkipped
	}

	doc, skip, err := h.resolveDoc(ctx, ev)
	if err != nil {
		return err
	}
	if skip {
		h.metrics.onSkipped(ctx, ev.Op+"_skip")
		return migration.ErrSkipped
	}

	var sts sourceThreadSub
	if uerr := bson.UnmarshalExtJSON(doc, false, &sts); uerr != nil {
		return fmt.Errorf("%w: decode source thread_sub: %v", migration.ErrPoison, uerr) //nolint:errorlint // intentional single-%w sentinel wrap; decode err is informational only
	}

	parentMessageID := sts.ParentMessage.ID
	account := sts.U.Username

	// A blank parentMessage._id or account is structurally invalid source data: the FK lookups
	// below would always miss and Nak-storm to MAX_DELIVER. Poison instead — redelivery can't fix it.
	if parentMessageID == "" {
		return fmt.Errorf("%w: thread_sub has empty parentMessage._id", migration.ErrPoison)
	}
	if account == "" {
		return fmt.Errorf("%w: thread_sub has empty u.username", migration.ErrPoison)
	}

	// Resolve thread_room by parent message id — yields roomID, threadRoomID, and the room's
	// siteID (thread-subs inherit the room's site per spec §6).
	roomID, threadRoomID, roomSiteID, found, err := h.target.FindThreadRoom(ctx, parentMessageID)
	if err != nil {
		return fmt.Errorf("find thread room for parentMessage %s: %w", parentMessageID, err)
	}
	if !found {
		// Thread room hasn't been created yet by the message migration — Nak and retry.
		h.metrics.onResolveMiss(ctx, "thread_room")
		return fmt.Errorf("thread_room not found for parentMessage %s", parentMessageID)
	}

	// Cross-check: if the source rid is non-empty and differs from the resolved roomID,
	// log a warning — the resolved room is authoritative.
	if sts.RID != "" && sts.RID != roomID {
		slog.Warn("thread_sub source rid differs from resolved roomID — using resolved",
			"source_rid", sts.RID, "resolved_room_id", roomID,
			"parentMessageId", parentMessageID, "request_id", reqID)
	}

	// Resolve user by account — needed for the UserID FK.
	userID, uFound, err := h.target.FindUserID(ctx, account)
	if err != nil {
		return fmt.Errorf("find user for account %s: %w", account, err)
	}
	if !uFound {
		// User hasn't been seeded yet — Nak and retry.
		h.metrics.onResolveMiss(ctx, "user")
		return fmt.Errorf("user not found for account %s", account)
	}

	now := time.UnixMilli(h.nowMillis()).UTC()

	sub := model.ThreadSubscription{
		ID:              idgen.GenerateUUIDv7(),
		ParentMessageID: parentMessageID,
		RoomID:          roomID,
		ThreadRoomID:    threadRoomID,
		UserID:          userID,
		UserAccount:     account,
		SiteID:          roomSiteID,
		LastSeenAt:      sts.LastSeenAt,
		HasMention:      sts.UnreadMention > 0,
		CreatedAt:       sts.CreatedAt.UTC(),
		UpdatedAt:       now,
	}

	payload := mustMarshal(sub)
	return h.pub.Publish(ctx, h.inboxEvent(model.InboxThreadSubscriptionUpserted, roomSiteID, payload))
}
