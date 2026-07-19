package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
)

// InboxStore abstracts the data store operations needed by the inbox worker.
type InboxStore interface {
	CreateSubscription(ctx context.Context, sub *model.Subscription) error
	BulkCreateSubscriptions(ctx context.Context, subs []*model.Subscription) error
	// UpsertRoom replicates room metadata, guarded by the incoming room's
	// UpdatedAt: an event carrying an older (or equal) UpdatedAt than the
	// stored one is a silent no-op, so out-of-order federated delivery cannot
	// regress room metadata.
	UpsertRoom(ctx context.Context, room *model.Room) error
	// UpdateSubscriptionRoles applies roles guarded by rolesUpdatedAt (the source
	// event's publish time): older/duplicate events are silent no-ops. A
	// genuinely missing subscription still returns an error so the event is
	// redelivered until member_added lands (federation race).
	UpdateSubscriptionRoles(ctx context.Context, account, roomID string, roles []model.Role, rolesUpdatedAt time.Time) error
	DeleteSubscriptionsByAccounts(ctx context.Context, roomID string, accounts []string) error
	FindUsersByAccounts(ctx context.Context, accounts []string) ([]model.User, error)
	// UpdateSubscriptionRead sets lastSeenAt and alert on the subscription
	// keyed by (roomID, account). Idempotent and order-safe: the write
	// only applies when the stored lastSeenAt is missing or strictly
	// earlier than the supplied value. Older or duplicate events are silent no-ops.
	// A genuinely missing sub returns an error (Nak) so the event redelivers until member_added lands.
	UpdateSubscriptionRead(ctx context.Context, roomID, account string, lastSeenAt time.Time, alert bool) error
	UpsertThreadSubscription(ctx context.Context, sub *model.ThreadSubscription) error
	// ApplyThreadRead writes ThreadSubscription under a $lt lastSeenAt guard, then the Subscription only if the guard accepted.
	ApplyThreadRead(ctx context.Context, roomID, threadRoomID, account string, newThreadUnread []string, alert bool, lastSeenAt time.Time) error
	// UpdateSubscriptionMute sets muted by (roomID, account), guarded by
	// muteUpdatedAt (the source event's publish time): older/duplicate events
	// are silent no-ops. A genuinely missing sub returns an error (Nak) so the event redelivers until member_added lands.
	UpdateSubscriptionMute(ctx context.Context, roomID, account string, muted bool, muteUpdatedAt time.Time) error
	// UpdateSubscriptionFavorite sets favorite by (roomID, account), guarded by
	// favoriteUpdatedAt (the source event's publish time): older/duplicate events
	// are silent no-ops. A genuinely missing sub returns an error (Nak) so the event redelivers until member_added lands.
	UpdateSubscriptionFavorite(ctx context.Context, roomID, account string, favorite bool, favoriteUpdatedAt time.Time) error
	// UpdateSubscriptionNamesForRoom sets name on every subscription in the room,
	// each guarded by its own nameUpdatedAt so an out-of-order rename cannot regress
	// a sub to a stale name. Used when a channel is renamed — replicated via the
	// cross-site inbox to remote sites.
	UpdateSubscriptionNamesForRoom(ctx context.Context, roomID, newName string, nameUpdatedAt time.Time) error
	// ApplySubscriptionRestriction writes {restricted, externalAccess, roles} to all subs
	// in the room, each guarded by its own restrictUpdatedAt so an out-of-order
	// visibility change cannot regress the flags/roles. When restricted=true and
	// ownerAccount is non-empty, a $cond pipeline demotes all accounts except
	// ownerAccount to RoleMember.
	ApplySubscriptionRestriction(ctx context.Context, roomID string, restricted, externalAccess bool, ownerAccount string, restrictUpdatedAt time.Time) error
	// UpdateUserStatus replicates a cross-site status change onto the local users doc keyed by
	// account, guarded by statusUpdatedAt (the event publish time): an older/equal high-water
	// mark is a no-op so out-of-order multi-site delivery can't regress the status. statusIsShow
	// is written only when non-nil. A missing user (no doc on this site) is a logged no-op.
	UpdateUserStatus(ctx context.Context, account, statusText string, statusIsShow *bool, statusUpdatedAt time.Time) error
	// UpdateUserSettings replaces the local users doc's settings sub-document with the
	// full post-update settings from the origin site, guarded by settingsUpdatedAt so an
	// out-of-order or duplicate delivery can't regress. A missing user is a logged no-op.
	UpdateUserSettings(ctx context.Context, account string, settings *model.UserSettings, updatedAt time.Time) error
}

// Handler processes cross-site InboxEvent messages; replicates only subscription/room metadata, never room keys.
type Handler struct {
	store InboxStore
}

// NewHandler creates a Handler with the given store.
func NewHandler(store InboxStore) *Handler {
	return &Handler{store: store}
}

// HandleEvent processes a single JetStream message payload.
func (h *Handler) HandleEvent(ctx context.Context, data []byte) error {
	var evt model.InboxEvent
	if err := json.Unmarshal(data, &evt); err != nil {
		return fmt.Errorf("unmarshal inbox event: %w", err)
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
	case "subscription_mute_toggled":
		return h.handleSubscriptionMuteToggled(ctx, &evt)
	case "subscription_favorite_toggled":
		return h.handleSubscriptionFavoriteToggled(ctx, &evt)
	case "thread_subscription_upserted":
		return h.handleThreadSubscriptionUpserted(ctx, &evt)
	case "thread_read":
		return h.handleThreadRead(ctx, &evt)
	case model.InboxRoomRenamed:
		return h.handleRoomRenamed(ctx, &evt)
	case model.InboxRoomRestricted:
		return h.handleRoomVisibilityChanged(ctx, &evt)
	case model.InboxUserStatusUpdated:
		return h.handleUserStatusUpdated(ctx, &evt)
	case model.InboxUserSettingsUpdated:
		return h.handleUserSettingsUpdated(ctx, &evt)
	default:
		slog.Warn("unknown event type, skipping", "type", evt.Type)
		return nil
	}
}

func (h *Handler) handleMemberAdded(ctx context.Context, evt *model.InboxEvent) error {
	var event model.MemberAddEvent
	if err := json.Unmarshal(evt.Payload, &event); err != nil {
		return fmt.Errorf("unmarshal member_added payload: %w", err)
	}

	roomType := event.RoomType
	if roomType == "" {
		roomType = model.RoomTypeChannel
	}

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

	subs := make([]*model.Subscription, 0, len(event.Accounts))
	var missing []string
	for _, account := range event.Accounts {
		user, ok := userMap[account]
		if !ok {
			missing = append(missing, account)
			continue
		}
		sub := &model.Subscription{
			ID:                 idgen.GenerateUUIDv7(),
			User:               model.SubscriptionUser{ID: user.ID, Account: user.Account},
			RoomID:             event.RoomID,
			RoomType:           roomType,
			SiteID:             event.SiteID,
			Roles:              rolesForType(roomType),
			Name:               subscriptionName(roomType, event.RoomName, event.RequesterAccount),
			IsSubscribed:       subscriptionIsSubscribed(roomType, &user),
			HistorySharedSince: historySharedSince,
			JoinedAt:           joinedAt,
		}
		subs = append(subs, sub)
	}

	if len(subs) > 0 {
		if err := h.store.BulkCreateSubscriptions(ctx, subs); err != nil {
			if !mongo.IsDuplicateKeyError(err) {
				return fmt.Errorf("bulk create subscriptions: %w", err)
			}
		}
	}

	// A referenced user that isn't present yet is a federation/migration race, not a
	// permanent failure: return a (transient) error so JetStream redelivers the event
	// until the user lands. The resolvable subscriptions above are created first to make
	// progress; redelivery re-upserts them idempotently (guarded by the unique index).
	if len(missing) > 0 {
		return fmt.Errorf("member_added references unknown users %v in room %s", missing, event.RoomID)
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
func (h *Handler) handleMemberRemoved(ctx context.Context, evt *model.InboxEvent) error {
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

func (h *Handler) handleRoomSync(ctx context.Context, evt *model.InboxEvent) error {
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
func (h *Handler) handleRoleUpdated(ctx context.Context, evt *model.InboxEvent) error {
	var subEvt model.SubscriptionUpdateEvent
	if err := json.Unmarshal(evt.Payload, &subEvt); err != nil {
		return fmt.Errorf("unmarshal role_updated payload: %w", err)
	}
	account := subEvt.Subscription.User.Account
	roomID := subEvt.Subscription.RoomID
	roles := subEvt.Subscription.Roles
	if len(roles) == 0 {
		// Poison message — return errcode.Permanent so main.go's consume loop
		// Acks (vs Nak-forever on a malformed payload).
		slog.WarnContext(ctx, "role_updated event has empty roles",
			"account", account, "room_id", roomID)
		return errcode.Permanent(errcode.BadRequest("role_updated event has empty roles"))
	}
	if err := h.store.UpdateSubscriptionRoles(ctx, account, roomID, roles, time.UnixMilli(subEvt.Timestamp).UTC()); err != nil {
		return fmt.Errorf("update subscription roles: %w", err)
	}
	return nil
}

// handleSubscriptionRead is idempotent and order-safe — the store's $lt
// guard rejects writes whose lastSeenAt is not strictly later than the
// stored one, so out-of-order federated delivery cannot regress read state.
func (h *Handler) handleSubscriptionRead(ctx context.Context, evt *model.InboxEvent) error {
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

// handleSubscriptionMuteToggled mirrors a room-side mute toggle onto the user's home-site subscription.
func (h *Handler) handleSubscriptionMuteToggled(ctx context.Context, evt *model.InboxEvent) error {
	var e model.SubscriptionMuteToggledEvent
	if err := json.Unmarshal(evt.Payload, &e); err != nil {
		return fmt.Errorf("unmarshal subscription_mute_toggled payload: %w", err)
	}
	if err := h.store.UpdateSubscriptionMute(ctx, e.RoomID, e.Account, e.Muted, time.UnixMilli(e.Timestamp).UTC()); err != nil {
		return fmt.Errorf("update subscription mute for %q in room %q: %w", e.Account, e.RoomID, err)
	}
	return nil
}

// handleSubscriptionFavoriteToggled mirrors a room-side favorite toggle onto the user's home-site subscription.
func (h *Handler) handleSubscriptionFavoriteToggled(ctx context.Context, evt *model.InboxEvent) error {
	var e model.SubscriptionFavoriteToggledEvent
	if err := json.Unmarshal(evt.Payload, &e); err != nil {
		return fmt.Errorf("unmarshal subscription_favorite_toggled payload: %w", err)
	}
	if err := h.store.UpdateSubscriptionFavorite(ctx, e.RoomID, e.Account, e.Favorite, time.UnixMilli(e.Timestamp).UTC()); err != nil {
		return fmt.Errorf("update subscription favorite for %q in room %q: %w", e.Account, e.RoomID, err)
	}
	return nil
}

// handleThreadSubscriptionUpserted upserts a ThreadSubscription on the local
// site when message-worker on another site reports that a user (parent author,
// replier, or mentionee) is participating in a thread. The Mongo store layer
// is responsible for the monotonic hasMention merge — see store impl.
func (h *Handler) handleThreadSubscriptionUpserted(ctx context.Context, evt *model.InboxEvent) error {
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

func (h *Handler) handleThreadRead(ctx context.Context, evt *model.InboxEvent) error {
	var e model.ThreadReadEvent
	if err := json.Unmarshal(evt.Payload, &e); err != nil {
		return fmt.Errorf("unmarshal thread_read payload: %w", err)
	}
	lastSeenAt := time.UnixMilli(e.LastSeenAt).UTC()
	if err := h.store.ApplyThreadRead(ctx, e.RoomID, e.ThreadRoomID, e.Account, e.NewThreadUnread, e.Alert, lastSeenAt); err != nil {
		return fmt.Errorf("apply thread read (room %q, thread %q, account %q): %w",
			e.RoomID, e.ThreadRoomID, e.Account, err)
	}
	return nil
}

func (h *Handler) handleRoomRenamed(ctx context.Context, evt *model.InboxEvent) error {
	var p model.RoomRenamedInboxPayload
	if err := json.Unmarshal(evt.Payload, &p); err != nil {
		return errcode.Permanent(errcode.BadRequest("unmarshal room_renamed payload"))
	}
	if err := h.store.UpdateSubscriptionNamesForRoom(ctx, p.RoomID, p.NewName, time.UnixMilli(p.Timestamp).UTC()); err != nil {
		return fmt.Errorf("update subscription names for room %s: %w", p.RoomID, err)
	}
	return nil
}

func (h *Handler) handleRoomVisibilityChanged(ctx context.Context, evt *model.InboxEvent) error {
	var p model.RoomRestrictedInboxPayload
	if err := json.Unmarshal(evt.Payload, &p); err != nil {
		return errcode.Permanent(errcode.BadRequest("unmarshal room_restricted payload"))
	}
	if err := h.store.ApplySubscriptionRestriction(ctx, p.RoomID, p.Restricted, p.ExternalAccess, p.OwnerAccount, time.UnixMilli(p.Timestamp).UTC()); err != nil {
		return fmt.Errorf("apply subscription visibility for room %s: %w", p.RoomID, err)
	}
	return nil
}

// handleUserStatusUpdated mirrors a cross-site status change onto the local users doc, guarded by
// the event Timestamp so an out-of-order or duplicate fan-out delivery can't regress the status.
func (h *Handler) handleUserStatusUpdated(ctx context.Context, evt *model.InboxEvent) error {
	var e model.UserStatusUpdated
	if err := json.Unmarshal(evt.Payload, &e); err != nil {
		return fmt.Errorf("unmarshal user_status_updated payload: %w", err)
	}
	if err := h.store.UpdateUserStatus(ctx, e.Account, e.StatusText, e.StatusIsShow, time.UnixMilli(e.Timestamp).UTC()); err != nil {
		return fmt.Errorf("update user status for %q: %w", e.Account, err)
	}
	return nil
}

// handleUserSettingsUpdated mirrors a cross-site settings change onto the local users doc,
// guarded by the event Timestamp so an out-of-order or duplicate delivery can't regress.
func (h *Handler) handleUserSettingsUpdated(ctx context.Context, evt *model.InboxEvent) error {
	var e model.UserSettingsUpdated
	if err := json.Unmarshal(evt.Payload, &e); err != nil {
		return fmt.Errorf("unmarshal user_settings_updated payload: %w", err)
	}
	if err := h.store.UpdateUserSettings(ctx, e.Account, &e.Settings, time.UnixMilli(e.Timestamp).UTC()); err != nil {
		return fmt.Errorf("update user settings for %q: %w", e.Account, err)
	}
	return nil
}

func rolesForType(t model.RoomType) []model.Role {
	if t == model.RoomTypeChannel {
		return []model.Role{model.RoleMember}
	}
	return nil
}

func subscriptionName(roomType model.RoomType, roomName, requesterAccount string) string {
	switch roomType {
	case model.RoomTypeChannel, model.RoomTypeDiscussion:
		return roomName
	case model.RoomTypeDM, model.RoomTypeBotDM:
		return requesterAccount
	}
	return ""
}

// isBot mirrors the bot predicate used by room-service/helper.go and pkg/pipelines:
// accounts ending in ".bot" or starting with "p_" (webhook-style bots).
func isBot(account string) bool {
	return strings.HasSuffix(account, ".bot") || strings.HasPrefix(account, "p_")
}

func subscriptionIsSubscribed(roomType model.RoomType, u *model.User) bool {
	if roomType != model.RoomTypeBotDM {
		return false
	}
	return !isBot(u.Account)
}
