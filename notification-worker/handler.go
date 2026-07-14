package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/bytedance/sonic"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/mention"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/roommetacache"
	"github.com/hmchangw/chat/pkg/roomsubcache"
)

// defaultRecipientBatchSize mirrors PUSH_RECIPIENT_BATCH_SIZE's envDefault so unit tests don't re-declare it.
const defaultRecipientBatchSize = 100

// MemberCache reads the cached member list and supports targeted invalidation.
type MemberCache interface {
	GetMembers(ctx context.Context, roomID string) ([]roomsubcache.Member, error)
	Invalidate(ctx context.Context, roomID string)
}

// RoomMetaGetter returns cached room metadata so push-service doesn't hit Mongo.
type RoomMetaGetter interface {
	Get(ctx context.Context, roomID string) (roommetacache.Meta, error)
}

// HandlerDeps groups the handler's collaborators.
type HandlerDeps struct {
	Members            MemberCache
	Followers          ThreadFollowerLister
	Parent             ParentFetcher // resolves a thread's parent author + createdAt from history-service
	Presence           PresenceSnapshotter
	Hook               Vetoer
	Emitter            Emitter
	RoomMeta           RoomMetaGetter // nil → title falls back to sender.Account
	LargeRoomThreshold int
	RecipientBatchSize int // per-event cap (≥ 1); 0 → defaultRecipientBatchSize
}

// Handler runs the per-message fan-out pipeline:
//
//	Stage 1 — exclusion filters (sender / mute / restricted / thread-non-follower)
//	Stage 2 — in-process hook veto (suppress-only, fail-open on error)
//	Stage 3 — pure routing predicate (EligibleForPush)
//	Stage 4 — one bulk presence RPC, then per-account shouldPush
//
// followed by one Emitter.Emit per surviving recipient.
type Handler struct {
	deps HandlerDeps
}

// isNotifiable reports whether a message type produces push notifications.
// Safe-by-default allowlist: new system types never notify. "" is the only
// regular type today; add tcard/tcard_execute/app_execute here as they land.
func isNotifiable(msgType string) bool {
	return msgType == ""
}

func NewHandler(deps HandlerDeps) *Handler { //nolint:gocritic // hugeParam: one-time constructor arg
	if deps.LargeRoomThreshold <= 0 {
		deps.LargeRoomThreshold = 500
	}
	if deps.RecipientBatchSize <= 0 {
		deps.RecipientBatchSize = defaultRecipientBatchSize
	}
	return &Handler{deps: deps}
}

func (h *Handler) HandleMessage(ctx context.Context, data []byte) error {
	var evt model.MessageEvent
	if err := sonic.Unmarshal(data, &evt); err != nil {
		// Malformed payload — it will never parse on redelivery. Mark permanent
		// so the caller Acks (drops) it instead of retrying until MaxDeliver.
		return errcode.Permanent(errcode.BadRequest("malformed message event"))
	}
	// Non-created events are filtered at the broker; defensive backstop only.
	if evt.Event != model.EventCreated && evt.Event != "" {
		return nil
	}
	msg := evt.Message

	// Phase 1 — side effects: member-change sys-messages invalidate the member
	// cache (Option C; safe because room-worker guards add/remove to channels).
	switch msg.Type {
	case model.MessageTypeMembersAdded, model.MessageTypeMemberLeft, model.MessageTypeMemberRemoved:
		h.deps.Members.Invalidate(ctx, msg.RoomID)
	}

	// Phase 2 — notification gate: only regular types push; every system type
	// (current and future) is non-notifying. See isNotifiable.
	if !isNotifiable(msg.Type) {
		return nil
	}

	members, err := h.deps.Members.GetMembers(ctx, msg.RoomID)
	if err != nil {
		return fmt.Errorf("get members for room %s: %w", msg.RoomID, err)
	}
	if len(members) == 0 {
		return nil
	}

	mentionInfo := mention.Parse(msg.Content)
	mentionedAccounts := mentionedSet(mentionInfo)
	// @here is deliberately NOT a push trigger — the legacy frontend doesn't render it.
	mentionsAll := mentionInfo.MentionAll
	isLargeRoom := len(members) > h.deps.LargeRoomThreshold
	isThreadOnlyReply := msg.ThreadParentMessageID != "" && !msg.TShow

	var followers map[string]struct{}
	// parentCreatedAt + parentSenderAccount feed the suppression gate and the
	// parent-author recipient. The gatekeeper resolves both best-effort on the send
	// path and carries them on the event; use them when present and fall back to a
	// history-service fetch only when either is absent (edit/delete events bypass the
	// gatekeeper, or a gatekeeper soft-fail). The parent pre-exists, so the fetch is
	// race-free and authoritative.
	var parentCreatedAt *time.Time
	var parentSenderAccount string
	if isThreadOnlyReply {
		// A clean thread_rooms miss returns empty followers + nil error (the first-reply
		// race); an actual Mongo failure must NAK rather than silently ack and drop
		// follower-only recipients.
		info, ferr := h.deps.Followers.Lookup(ctx, msg.ThreadParentMessageID)
		if ferr != nil {
			return fmt.Errorf("lookup thread room for parent %s: %w", msg.ThreadParentMessageID, ferr)
		}
		followers = info.Followers
		if msg.ThreadParentMessageCreatedAt != nil && evt.ThreadParentSenderAccount != "" {
			parentCreatedAt = msg.ThreadParentMessageCreatedAt
			parentSenderAccount = evt.ThreadParentSenderAccount
		} else {
			// The reply sender can always read the parent they replied to; fetch on their behalf.
			parent, perr := h.deps.Parent.FetchParent(ctx, msg.UserAccount, msg.RoomID, evt.SiteID, msg.ThreadParentMessageID)
			if perr != nil {
				return fmt.Errorf("fetch thread parent %s: %w", msg.ThreadParentMessageID, perr)
			}
			pc := parent.CreatedAt
			parentCreatedAt = &pc
			parentSenderAccount = parent.SenderAccount
		}
	}

	roomType := members[0].RoomType

	// Sender display name is composed by message-gatekeeper at write time; no per-message lookup here.
	sender := &model.Participant{
		UserID:      msg.UserID,
		Account:     msg.UserAccount,
		DisplayName: msg.SenderDisplayName(),
	}

	candidates := make([]roomsubcache.Member, 0, len(members))
	accounts := make([]string, 0, len(members))
	for i := range members {
		m := members[i]
		if m.ID == msg.UserID {
			continue
		}
		if m.Muted {
			continue
		}
		if isRestricted(m, msg, isThreadOnlyReply, parentCreatedAt) {
			continue
		}

		mentioned := mentionsAll || mentionedAccounts[m.Account]

		if isThreadOnlyReply {
			_, follows := followers[m.Account]
			// The parent author is always notified of replies to their own thread, even
			// before thread_rooms exists (they aren't yet in replyAccounts). The restricted-
			// room gate above still applies, but never excludes them — they were present
			// when they authored the parent.
			if m.Account == parentSenderAccount {
				follows = true
			}
			if !follows && !mentioned {
				continue
			}
		}

		// Stage 2: hook veto (fail-open on error).
		allow, herr := h.deps.Hook.Allow(ctx, &msg, m)
		if herr != nil {
			slog.Warn("hook errored, allowing", "error", herr, "account", m.Account,
				"request_id", natsutil.RequestIDFromContext(ctx))
			allow = true
		}
		if !allow {
			continue
		}

		if !EligibleForPush(&m, roomType, isLargeRoom, mentioned) {
			continue
		}

		candidates = append(candidates, m)
		accounts = append(accounts, m.Account)
	}
	if len(candidates) == 0 {
		return nil
	}

	snapshot, _ := h.deps.Presence.Snapshot(ctx, accounts) // fail-open: error → empty map

	// Sort survivors so batch N has a deterministic account set across redeliveries —
	// required for the {messageID}-b{N} Nats-Msg-Id to dedup correctly.
	survivors := make([]string, 0, len(candidates))
	for _, c := range candidates {
		if !shouldPush(snapshot[c.Account]) {
			continue
		}
		survivors = append(survivors, c.Account)
	}
	if len(survivors) == 0 {
		return nil
	}
	sort.Strings(survivors)

	now := time.Now().UTC()
	// Template carries fields shared across every batch — only ID and Accounts change per batch.
	pushEvt := model.PushNotificationEvent{
		RoomID: msg.RoomID,
		Title:  h.resolveTitle(ctx, msg.RoomID, roomType, sender),
		Body:   msg.Content,
		Data: model.PushNotificationData{
			RoomID:            msg.RoomID,
			MessageID:         msg.ID,
			Type:              shortRoomType(roomType),
			Sender:            sender,
			ThreadMessageID:   msg.ThreadParentMessageID,
			PushTime:          now.Format(time.RFC3339),
			AlsoSendToChannel: msg.TShow,
		},
		Timestamp: now.UnixMilli(),
	}

	batchSize := h.deps.RecipientBatchSize
	// Aggregate per-batch errors so one bad batch doesn't punish the others; still return
	// an error so the caller naks and JetStream redelivers. {messageId}-b{N} dedup protects
	// against duplicate emission of batches that already succeeded.
	var emitErrs []error
	for i, batchIdx := 0, 0; i < len(survivors); i, batchIdx = i+batchSize, batchIdx+1 {
		end := i + batchSize
		if end > len(survivors) {
			end = len(survivors)
		}
		batchAccounts := make([]string, end-i)
		copy(batchAccounts, survivors[i:end])

		evt := pushEvt
		evt.ID = fmt.Sprintf("%s-b%d", msg.ID, batchIdx)
		evt.Accounts = batchAccounts
		if err := h.deps.Emitter.Emit(ctx, evt); err != nil {
			slog.Error("emit push batch failed", "error", err, "batch", batchIdx,
				"recipients", len(batchAccounts), "messageId", msg.ID,
				"request_id", natsutil.RequestIDFromContext(ctx))
			emitErrs = append(emitErrs, fmt.Errorf("emit push batch %d: %w", batchIdx, err))
		}
	}
	if len(emitErrs) > 0 {
		return fmt.Errorf("emit push batches for message %s: %w", msg.ID, errors.Join(emitErrs...))
	}
	return nil
}

// mentionedSet returns mentioned accounts as a set for O(1) per-recipient lookup.
// msg.Mentions is not populated by message-gatekeeper, so only Parse output is used.
func mentionedSet(parsed mention.ParseResult) map[string]bool {
	out := make(map[string]bool, len(parsed.Accounts))
	for _, a := range parsed.Accounts {
		out[a] = true
	}
	return out
}

// isRestricted filters members who joined after the message timestamp (the parent's
// createdAt for thread-only replies); a nil parentCreatedAt suppresses, not leaks.
func isRestricted(m roomsubcache.Member, msg model.Message, isThreadOnlyReply bool, parentCreatedAt *time.Time) bool { //nolint:gocritic // hugeParam: hot loop, pointer indirection adds no benefit
	if m.HistorySharedSince == nil {
		return false
	}
	if isThreadOnlyReply {
		if parentCreatedAt == nil {
			return true
		}
		return parentCreatedAt.UnixMilli() < *m.HistorySharedSince
	}
	return msg.CreatedAt.UnixMilli() < *m.HistorySharedSince
}

func shortRoomType(t model.RoomType) string {
	switch t {
	case model.RoomTypeDM, model.RoomTypeBotDM:
		return "d"
	case model.RoomTypeDiscussion:
		return "p"
	default:
		return "c"
	}
}

// resolveTitle returns the room name when present, else the sender's account (the legacy rule).
// DM/botDM rooms skip the cache lookup — they never have names. RoomMeta failures fall back to
// the sender so push-service still gets a usable title.
func (h *Handler) resolveTitle(ctx context.Context, roomID string, roomType model.RoomType, sender *model.Participant) string {
	if h.deps.RoomMeta != nil && roomType != model.RoomTypeDM && roomType != model.RoomTypeBotDM {
		meta, err := h.deps.RoomMeta.Get(ctx, roomID)
		switch {
		case err == nil && meta.Name != "":
			return meta.Name
		case err != nil:
			slog.Warn("room meta lookup failed, falling back to sender",
				"error", err, "roomId", roomID, "request_id", natsutil.RequestIDFromContext(ctx))
		}
	}
	if sender != nil {
		return sender.Account
	}
	return ""
}
