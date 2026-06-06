package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/mention"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/roommetacache"
	"github.com/hmchangw/chat/pkg/roomsubcache"
	"github.com/hmchangw/chat/pkg/subject"
)

// natsPublisher is a thin Publisher backed by a *nats.Conn for the reaction
// notification fan-out. Sync publish; caller wraps errors.
type natsPublisher struct {
	nc *nats.Conn
}

func (p natsPublisher) Publish(_ context.Context, subj string, data []byte) error {
	return p.nc.Publish(subj, data)
}

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

// Publisher publishes a single message-author notification for reaction events.
// Separate from Emitter (which batches mobile push); reactions go to the legacy
// chat.user.{account}.notification subject the FE already listens on.
type Publisher interface {
	Publish(ctx context.Context, subj string, data []byte) error
}

// HandlerDeps groups the handler's collaborators.
type HandlerDeps struct {
	Members            MemberCache
	Followers          ThreadFollowerLister
	Presence           PresenceSnapshotter
	Hook               Vetoer
	Emitter            Emitter
	ReactionPub        Publisher      // nil → reaction notifications are dropped
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
	if err := json.Unmarshal(data, &evt); err != nil {
		return fmt.Errorf("unmarshal message event: %w", err)
	}
	// Reactions take a separate author-only path; the full push pipeline below
	// is for create events. Edits/deletes are silently dropped.
	switch evt.Event {
	case model.EventReacted:
		return h.handleReaction(ctx, &evt)
	case model.EventCreated, "":
		// fall through to push pipeline
	default:
		return nil
	}
	msg := evt.Message

	// Member-change sys-messages drive cache invalidation (Option C; safe because room-worker guards add/remove to channels).
	if msg.Type != "" {
		switch msg.Type {
		case model.MessageTypeMembersAdded, model.MessageTypeMemberLeft, model.MessageTypeMemberRemoved:
			h.deps.Members.Invalidate(ctx, msg.RoomID)
		}
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
	if isThreadOnlyReply {
		f, ferr := h.deps.Followers.Followers(ctx, msg.ThreadParentMessageID)
		if ferr != nil {
			slog.Warn("thread followers lookup failed, treating as empty",
				"error", ferr, "parentMessageId", msg.ThreadParentMessageID,
				"request_id", natsutil.RequestIDFromContext(ctx))
			f = map[string]struct{}{}
		}
		followers = f
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
		if isRestricted(m, msg, isThreadOnlyReply) {
			continue
		}

		mentioned := mentionsAll || mentionedAccounts[m.Account]

		if isThreadOnlyReply {
			_, follows := followers[m.Account]
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

// isRestricted filters members who joined after the relevant message timestamp.
// Thread replies use the parent's CreatedAt; a nil parent ts is "no access" (legacy records).
func isRestricted(m roomsubcache.Member, msg model.Message, isThreadOnlyReply bool) bool { //nolint:gocritic // hugeParam: hot loop, pointer indirection adds no benefit
	if m.HistorySharedSince == nil {
		return false
	}
	if isThreadOnlyReply {
		if msg.ThreadParentMessageCreatedAt == nil {
			return true
		}
		return msg.ThreadParentMessageCreatedAt.UnixMilli() < *m.HistorySharedSince
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

// handleReaction notifies the message author of a reaction. Only "added"
// toggles notify; un-reacts and self-reacts are silent.
func (h *Handler) handleReaction(ctx context.Context, evt *model.MessageEvent) error {
	if h.deps.ReactionPub == nil {
		return nil
	}
	if evt.ReactionDelta == nil {
		slog.Error("reacted event missing ReactionDelta; dropping",
			"messageID", evt.Message.ID,
			"roomID", evt.Message.RoomID,
			"siteID", evt.SiteID,
		)
		return nil
	}
	if evt.ReactionDelta.Action != model.ReactionActionAdded {
		return nil
	}
	authorAccount := evt.Message.UserAccount
	if authorAccount == "" || authorAccount == evt.ReactionDelta.Actor.Account {
		return nil
	}

	notif := model.NotificationEvent{
		Type:          "reaction",
		RoomID:        evt.Message.RoomID,
		Message:       evt.Message,
		ReactionDelta: evt.ReactionDelta,
		Timestamp:     time.Now().UTC().UnixMilli(),
	}
	data, err := natsutil.MarshalResponse(notif)
	if err != nil {
		return fmt.Errorf("marshal reaction notification: %w", err)
	}
	if err := h.deps.ReactionPub.Publish(ctx, subject.Notification(authorAccount), data); err != nil {
		return fmt.Errorf("publish reaction notification to %s: %w", authorAccount, err)
	}
	return nil
}
