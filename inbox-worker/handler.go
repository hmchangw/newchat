package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
)

// InboxStore abstracts the data store operations needed by the inbox worker.
type InboxStore interface {
	CreateSubscription(ctx context.Context, sub *model.Subscription) error
	BulkCreateSubscriptions(ctx context.Context, subs []*model.Subscription) error
	UpsertRoom(ctx context.Context, room *model.Room) error
	UpdateSubscriptionRoles(ctx context.Context, account, roomID string, roles []model.Role) error
	DeleteSubscriptionsByAccounts(ctx context.Context, roomID string, accounts []string) error
	FindUsersByAccounts(ctx context.Context, accounts []string) ([]model.User, error)
	// UpdateSubscriptionRead sets lastSeenAt and alert on the subscription
	// keyed by (roomID, account). Idempotent and order-safe: the write
	// only applies when the stored lastSeenAt is missing or strictly
	// earlier than the supplied value. Older or duplicate events are
	// silent no-ops. Missing-subscription is also a silent no-op.
	UpdateSubscriptionRead(ctx context.Context, roomID, account string, lastSeenAt time.Time, alert bool) error
	UpsertThreadSubscription(ctx context.Context, sub *model.ThreadSubscription) error
}

// Handler processes incoming cross-site OutboxEvent messages.
type Handler struct {
	store InboxStore
}

// NewHandler creates a Handler with the given store.
func NewHandler(store InboxStore) *Handler {
	return &Handler{store: store}
}

// HandleEvent processes a single JetStream message payload.
func (h *Handler) HandleEvent(ctx context.Context, data []byte) error {
	var evt model.OutboxEvent
	if err := json.Unmarshal(data, &evt); err != nil {
		return fmt.Errorf("unmarshal outbox event: %w", err)
	}

	switch evt.Type {
	case "member_added":
		return h.handleMemberAdded(ctx, &evt)
	case "member_removed":
		return h.handleMemberRemoved(ctx, &evt)
	case "room_sync":
		return h.handleRoomSync(ctx, &evt)
	case "role_updated":
		return h.handleRoleUpdated(ctx, &evt)
	case "subscription_read":
		return h.handleSubscriptionRead(ctx, &evt)
	case "thread_subscription_upserted":
		return h.handleThreadSubscriptionUpserted(ctx, &evt)
	case model.MessageTypeRoomCreated:
		return h.handleRoomCreated(ctx, &evt)
	default:
		slog.Warn("unknown event type, skipping", "type", evt.Type)
		return nil
	}
}

func (h *Handler) handleMemberAdded(ctx context.Context, evt *model.OutboxEvent) error {
	var event model.MemberAddEvent
	if err := json.Unmarshal(evt.Payload, &event); err != nil {
		return fmt.Errorf("unmarshal member_added payload: %w", err)
	}

	// 1. Look up users locally
	users, err := h.store.FindUsersByAccounts(ctx, event.Accounts)
	if err != nil {
		return fmt.Errorf("find users by accounts: %w", err)
	}
	userMap := make(map[string]model.User, len(users))
	for i := range users {
		userMap[users[i].Account] = users[i]
	}

	joinedAt := time.UnixMilli(event.JoinedAt).UTC()
	var historySharedSince *time.Time
	if event.HistorySharedSince != nil && *event.HistorySharedSince > 0 {
		t := time.UnixMilli(*event.HistorySharedSince).UTC()
		historySharedSince = &t
	}

	// 2. Build subscriptions
	subs := make([]*model.Subscription, 0, len(event.Accounts))
	for _, account := range event.Accounts {
		user, ok := userMap[account]
		if !ok {
			slog.Warn("user not found for account", "account", account)
			continue
		}
		// RoomType is fixed to channel: cross-site member_added events only
		// originate from rooms that support add-member (channel/discussion),
		// never from DM/botDM.
		sub := &model.Subscription{
			ID:                 idgen.GenerateUUIDv7(),
			User:               model.SubscriptionUser{ID: user.ID, Account: user.Account},
			RoomID:             event.RoomID,
			RoomType:           model.RoomTypeChannel,
			SiteID:             event.SiteID,
			Roles:              []model.Role{model.RoleMember},
			Name:               event.RoomName,
			HistorySharedSince: historySharedSince,
			JoinedAt:           joinedAt,
		}
		subs = append(subs, sub)
	}

	// 3. Bulk create subscriptions
	if err := h.store.BulkCreateSubscriptions(ctx, subs); err != nil {
		return fmt.Errorf("bulk create subscriptions: %w", err)
	}

	// No SubscriptionUpdateEvent is published here — room-worker already publishes
	// to the user's subject and the NATS supercluster routes it to the user's
	// home site.
	return nil
}

// handleMemberRemoved deletes the subscriptions for the accounts listed in the
// event. The room's home site has already filtered out dual-membership users,
// so this site only needs to sync subscriptions in a single round trip. No
// SubscriptionUpdateEvent is published here — room-worker already publishes
// to the user's subject and the NATS supercluster routes it to the user's
// home site.
func (h *Handler) handleMemberRemoved(ctx context.Context, evt *model.OutboxEvent) error {
	var memberEvt model.MemberRemoveEvent
	if err := json.Unmarshal(evt.Payload, &memberEvt); err != nil {
		return fmt.Errorf("unmarshal member removed payload: %w", err)
	}
	if len(memberEvt.Accounts) == 0 {
		return nil
	}
	if err := h.store.DeleteSubscriptionsByAccounts(ctx, memberEvt.RoomID, memberEvt.Accounts); err != nil {
		return fmt.Errorf("delete subscriptions for room %s: %w", memberEvt.RoomID, err)
	}
	return nil
}

func (h *Handler) handleRoomSync(ctx context.Context, evt *model.OutboxEvent) error {
	var room model.Room
	if err := json.Unmarshal(evt.Payload, &room); err != nil {
		return fmt.Errorf("unmarshal room_sync payload: %w", err)
	}

	if err := h.store.UpsertRoom(ctx, &room); err != nil {
		return fmt.Errorf("upsert room: %w", err)
	}

	return nil
}

// handleRoleUpdated updates the local subscription roles.
// No SubscriptionUpdateEvent is published here — room-worker already publishes to
// the user's subject, and NATS supercluster routes it to the user's site.
func (h *Handler) handleRoleUpdated(ctx context.Context, evt *model.OutboxEvent) error {
	var subEvt model.SubscriptionUpdateEvent
	if err := json.Unmarshal(evt.Payload, &subEvt); err != nil {
		return fmt.Errorf("unmarshal role_updated payload: %w", err)
	}
	account := subEvt.Subscription.User.Account
	roomID := subEvt.Subscription.RoomID
	roles := subEvt.Subscription.Roles
	if len(roles) == 0 {
		return fmt.Errorf("role_updated event has empty roles")
	}
	if err := h.store.UpdateSubscriptionRoles(ctx, account, roomID, roles); err != nil {
		return fmt.Errorf("update subscription roles: %w", err)
	}
	return nil
}

// handleSubscriptionRead is idempotent and order-safe — the store's $lt
// guard rejects writes whose lastSeenAt is not strictly later than the
// stored one, so out-of-order federated delivery cannot regress read state.
func (h *Handler) handleSubscriptionRead(ctx context.Context, evt *model.OutboxEvent) error {
	var e model.SubscriptionReadEvent
	if err := json.Unmarshal(evt.Payload, &e); err != nil {
		return fmt.Errorf("unmarshal subscription_read payload: %w", err)
	}
	lastSeenAt := time.UnixMilli(e.LastSeenAt).UTC()
	if err := h.store.UpdateSubscriptionRead(ctx, e.RoomID, e.Account, lastSeenAt, e.Alert); err != nil {
		return fmt.Errorf("update subscription read for %q in room %q: %w", e.Account, e.RoomID, err)
	}
	return nil
}

// handleThreadSubscriptionUpserted upserts a ThreadSubscription on the local
// site when message-worker on another site reports that a user (parent author,
// replier, or mentionee) is participating in a thread. The Mongo store layer
// is responsible for the monotonic hasMention merge — see store impl.
func (h *Handler) handleThreadSubscriptionUpserted(ctx context.Context, evt *model.OutboxEvent) error {
	var sub model.ThreadSubscription
	if err := json.Unmarshal(evt.Payload, &sub); err != nil {
		return fmt.Errorf("unmarshal thread_subscription_upserted payload: %w", err)
	}
	if err := h.store.UpsertThreadSubscription(ctx, &sub); err != nil {
		return fmt.Errorf("upsert thread subscription (threadRoomID %q, userID %q): %w",
			sub.ThreadRoomID, sub.UserID, err)
	}
	return nil
}

// errPermanent signals a non-retryable error; callers should Ack and move on.
var errPermanent = errors.New("permanent")

func rolesForType(t model.RoomType) []model.Role {
	if t == model.RoomTypeChannel {
		return []model.Role{model.RoleMember}
	}
	return nil
}

func subscriptionName(d *model.RoomCreatedOutbox, u *model.User) string {
	switch d.RoomType {
	case model.RoomTypeChannel, model.RoomTypeDiscussion:
		return d.RoomName
	case model.RoomTypeDM, model.RoomTypeBotDM:
		// On the remote site, the "other party" relative to u is the requester.
		return d.RequesterAccount
	}
	return ""
}

// isBot mirrors the bot predicate used by room-service/helper.go and pkg/pipelines:
// accounts ending in ".bot" or starting with "p_" (webhook-style bots).
func isBot(account string) bool {
	return strings.HasSuffix(account, ".bot") || strings.HasPrefix(account, "p_")
}

func subscriptionIsSubscribed(d *model.RoomCreatedOutbox, u *model.User) bool {
	if d.RoomType != model.RoomTypeBotDM {
		return false
	}
	return !isBot(u.Account)
}

func (h *Handler) handleRoomCreated(ctx context.Context, evt *model.OutboxEvent) error {
	requestID := natsutil.RequestIDFromContext(ctx)
	if requestID == "" {
		return fmt.Errorf("missing X-Request-ID: %w", errPermanent)
	}

	var data model.RoomCreatedOutbox
	if err := json.Unmarshal(evt.Payload, &data); err != nil {
		return fmt.Errorf("unmarshal room_created payload: %w: %w", err, errPermanent)
	}
	if len(data.Accounts) == 0 {
		slog.Warn("room_created event with empty Accounts list",
			"requestId", requestID, "roomId", data.RoomID)
		return nil
	}

	users, err := h.store.FindUsersByAccounts(ctx, data.Accounts)
	if err != nil {
		return fmt.Errorf("find users by accounts: %w", err)
	}
	// FindUsersByAccounts can return a subset; treat any account in
	// data.Accounts that didn't come back as a hard failure rather than
	// silently materializing partial remote-side state with no retry signal.
	userByAccount := make(map[string]model.User, len(users))
	for i := range users {
		userByAccount[users[i].Account] = users[i]
	}
	for _, account := range data.Accounts {
		if _, ok := userByAccount[account]; !ok {
			return fmt.Errorf("find users by accounts: missing account %q (room %s home %s)",
				account, data.RoomID, data.HomeSiteID)
		}
	}

	acceptedAt := time.UnixMilli(data.Timestamp).UTC()
	subs := make([]*model.Subscription, 0, len(data.Accounts))
	for _, account := range data.Accounts {
		u := userByAccount[account]
		sub := &model.Subscription{
			ID:           idgen.GenerateUUIDv7(),
			User:         model.SubscriptionUser{ID: u.ID, Account: u.Account},
			RoomID:       data.RoomID,
			SiteID:       data.HomeSiteID,
			Roles:        rolesForType(data.RoomType),
			Name:         subscriptionName(&data, &u),
			RoomType:     data.RoomType,
			IsSubscribed: subscriptionIsSubscribed(&data, &u),
			JoinedAt:     acceptedAt,
		}
		subs = append(subs, sub)
	}

	if len(subs) == 0 {
		return nil
	}
	if err := h.store.BulkCreateSubscriptions(ctx, subs); err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return nil
		}
		return fmt.Errorf("bulk create subs: %w", err)
	}
	return nil
}
