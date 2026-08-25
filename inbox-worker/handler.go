package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subauthcache"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// InboxStore abstracts the data store operations needed by the inbox worker.
//
//go:generate mockgen -destination=mock_store_test.go -package=main . InboxStore
type InboxStore interface {
	CreateSubscription(ctx context.Context, sub *model.Subscription) error
	BulkCreateSubscriptions(ctx context.Context, subs []*model.Subscription) error
	// BulkRefreshJoinedAt sets joinedAt on existing (roomId, account) replicas —
	// the Teams migration's cross-site joinedAt correction; a missing sub is a no-op.
	BulkRefreshJoinedAt(ctx context.Context, roomID string, joinedAtByAccount map[string]time.Time) error
	// UpsertRoom replicates room metadata, guarded by the incoming room's
	// UpdatedAt: an event carrying an older (or equal) UpdatedAt than the
	// stored one is a silent no-op, so out-of-order federated delivery cannot
	// regress room metadata.
	UpsertRoom(ctx context.Context, room *model.Room) error
	// UpsertRemoteRoomActivity records a remote room's ordering position under a
	// $max guard, so a late or duplicate event is a no-op rather than a
	// regression — what will let the activity-refresh publisher ride a lossy,
	// unordered transport. Until it lands member_added is the only writer, so a
	// dropped write is NOT self-healing.
	UpsertRemoteRoomActivity(ctx context.Context, roomID, siteID string, lastMsgAt time.Time) error
	// DeleteRemoteRoomActivity drops a remote room's ordering row once this site
	// has no member left in it, so the collection does not accumulate rows for
	// rooms nobody here can see.
	DeleteRemoteRoomActivity(ctx context.Context, roomID string) error
	// HasRoomSubscription reports whether this site holds any subscription for
	// roomID. The activity refresh is broadcast to every peer, so this is what
	// stops a site accumulating ordering rows for rooms none of its users are in.
	HasRoomSubscription(ctx context.Context, roomID string) (bool, error)
	// UpdateSubscriptionRoles applies roles guarded by rolesUpdatedAt (the source
	// event's publish time): older/duplicate events are silent no-ops. A
	// genuinely missing subscription still returns an error so the event is
	// redelivered until member_added lands (federation race).
	UpdateSubscriptionRoles(ctx context.Context, account, roomID string, roles []model.Role, rolesUpdatedAt time.Time) error
	DeleteSubscriptionsByAccounts(ctx context.Context, roomID string, accounts []string) error
	// DeleteThreadSubscriptions removes the accounts' per-thread read-state docs
	// in roomID on a federated removal (#308). This site holds no thread_rooms
	// (replyAccounts lives only at the room's home site), so nothing else to clean.
	DeleteThreadSubscriptions(ctx context.Context, roomID string, accounts []string) error
	FindUsersByAccounts(ctx context.Context, accounts []string) ([]model.User, error)
	// UpdateSubscriptionRead sets lastSeenAt and alert on the subscription
	// keyed by (roomID, account), guarded so an out-of-order or replayed event
	// cannot regress the read position. Returns applied=false when that guard
	// matched nothing (no write happened), and the number of unread followed
	// threads left on the subscription after the write.
	UpdateSubscriptionRead(ctx context.Context, roomID, account string, lastSeenAt time.Time, alert bool) (bool, int, error)
	UpsertThreadSubscription(ctx context.Context, sub *model.ThreadSubscription) error
	// ApplyThreadRead advances the home-replica ThreadSubscription read state
	// under a $lt guard and, when the guard matches, $pulls parentMessageID
	// from the subscription's threadUnread (per-ID pull commutes with other
	// threads' $addToSet; empty parentMessageID skips the pull).
	ApplyThreadRead(ctx context.Context, roomID, threadRoomID, account, parentMessageID string, lastSeenAt time.Time) error
	// ApplyThreadReadAll is the federated "mark all threads read" bulk clear on the
	// user's home replica: it advances every one of account's thread subscriptions
	// to lastSeenAt under a per-doc $lt guard (clearing hasMention), and $unsets
	// threadUnread on every subscription that currently has unread threads.
	ApplyThreadReadAll(ctx context.Context, account string, lastSeenAt time.Time) error
	// AddThreadUnread marks parentMessageID unread for accounts' subscriptions in
	// roomID via a single $addToSet UpdateMany. Idempotent under JetStream
	// redelivery; accounts not subscribed simply match nothing.
	AddThreadUnread(ctx context.Context, roomID, parentMessageID string, accounts []string) error
	// UpdateSubscriptionMute sets muted by (roomID, account), guarded by
	// muteUpdatedAt (the source event's publish time): older/duplicate events
	// are silent no-ops. A genuinely missing sub returns an error (Nak) so the event redelivers until member_added lands.
	UpdateSubscriptionMute(ctx context.Context, roomID, account string, muted bool, muteUpdatedAt time.Time) error
	// UpdateSubscriptionFavorite sets favorite by (roomID, account), guarded by
	// favoriteUpdatedAt (the source event's publish time): older/duplicate events
	// are silent no-ops. A genuinely missing sub returns an error (Nak) so the event redelivers until member_added lands.
	UpdateSubscriptionFavorite(ctx context.Context, roomID, account string, favorite bool, favoriteUpdatedAt time.Time) error
	// UpdateSubscriptionOpen sets open by (roomID, account). No ordering guard:
	// set-true is idempotent. A genuinely missing sub returns an error (Nak) so the
	// event redelivers until the member_added that creates the sub lands.
	UpdateSubscriptionOpen(ctx context.Context, roomID, account string, open bool) error
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
	// ListSubscriptionAccountsByRoom returns the accounts subscribed to roomID
	// on this site's local replica. Used to drive the room_restricted bust
	// loop: ApplySubscriptionRestriction can bulk-rewrite Roles for every
	// local subscriber, not just OwnerAccount, so every one of them needs an
	// L2 bust. Mirrors room-service's ListSubscriptionsByRoom.
	ListSubscriptionAccountsByRoom(ctx context.Context, roomID string) ([]string, error)
	// UpdateUserStatus replicates a cross-site status change onto the local users doc keyed by
	// account, guarded by statusUpdatedAt (the event publish time): an older/equal high-water
	// mark is a no-op so out-of-order multi-site delivery can't regress the status. statusIsShow
	// is written only when non-nil. A missing user (no doc on this site) is a logged no-op.
	UpdateUserStatus(ctx context.Context, account, statusText string, statusIsShow *bool, statusUpdatedAt time.Time) error
	// UpdateUserSettings replaces the local users doc's settings sub-document with the
	// full post-update settings from the origin site, guarded by settingsUpdatedAt so an
	// out-of-order or duplicate delivery can't regress. A missing user is a logged no-op.
	UpdateUserSettings(ctx context.Context, account string, settings *model.UserSettings, updatedAt time.Time) error
	// ApplyUserPermissions applies one permission state to the accounts under the
	// per-key watermark guard. A missing user (no doc on this site) is a silent no-op.
	ApplyUserPermissions(ctx context.Context, permission model.PermissionKey, accounts []string, state model.PermissionState) error
	// UpdateUserChatlist replaces the local users doc's chatlist sub-document with the
	// full post-update state from the origin site, guarded by chatlistUpdatedAt so an
	// out-of-order or duplicate delivery can't regress. A missing user is a logged no-op.
	// updatedAt is unix-millis (int64) — matches how user-service writes
	// chatlistUpdatedAt (mongorepo/users.go), unlike the other Update* methods here
	// which take time.Time.
	UpdateUserChatlist(ctx context.Context, account string, chatlist *model.ChatlistState, updatedAt int64) error
	// UpdateSubscriptionSection sets sectionId+sectionOrder (or clears both when
	// sectionID==nil) on (roomID, account), guarded by sectionUpdatedAt so an
	// out-of-order or duplicate move can't regress. A missing sub NAKs for retry.
	UpdateSubscriptionSection(ctx context.Context, roomID, account string, sectionID *string, order float64, updatedAt time.Time) error
	// SetSubscriptionMentions flags accounts as mentioned in roomID, skipping any
	// that already read past msgCreatedAt (#467). Mirrors broadcast-worker's
	// origin-side write; a non-subscriber matches nothing.
	SetSubscriptionMentions(ctx context.Context, roomID string, accounts []string, msgCreatedAt time.Time) error
}

// badgeCache is the badge cache's Valkey accelerator (pkg/badgecache.Cache
// satisfies it). Nil when VALKEY_ADDRS is unset — call sites nil-check, so a
// disabled cache is a silent no-op.
type badgeCache interface {
	ClearRoom(ctx context.Context, account, roomID string)
	ClearAll(ctx context.Context, account string)
}

// Handler processes cross-site InboxEvent messages; replicates only subscription/room metadata, never room keys.
type Handler struct {
	store InboxStore
	// badge is the badge cache; nil (VALKEY_ADDRS unset) disables the
	// invalidation hooks. Injected post-construction.
	badge badgeCache
	// valkey is the L2 (Valkey) client used only to invalidate this site's
	// local subauthcache entries after a federated write that replicates a
	// role change or member removal onto this site's own subscription copy.
	// nil disables invalidation (best-effort). Set post-construction,
	// mirroring room-worker/room-service's valkey field.
	valkey valkeyutil.Client
	// roomSubs memoizes the "is this site a member of this room" check the
	// activity refresh performs; nil disables it and every refresh reads through.
	roomSubs *roomSubCache
}

// HandlerOption configures optional Handler behaviour.
type HandlerOption func(*Handler)

// WithRoomSubCache memoizes the room-membership check on the activity-refresh
// lane. A non-positive size or ttl leaves it disabled.
func WithRoomSubCache(size int, ttl time.Duration) HandlerOption {
	return func(h *Handler) { h.roomSubs = newRoomSubCache(size, ttl) }
}

// NewHandler creates a Handler with the given store.
func NewHandler(store InboxStore, opts ...HandlerOption) *Handler {
	h := &Handler{store: store}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

// HandleRoomActivity applies a core-NATS activity refresh for a room owned by
// another site. Dropped when this site holds no subscription for the room: the
// refresh is broadcast to every peer, and a row here would otherwise be an
// orphan nothing deletes.
//
// Errors are returned for logging only — there is no ack on this lane, and the
// $max guard makes the next refresh idempotent, so a loss self-heals on the
// room's next message.
func (h *Handler) HandleRoomActivity(ctx context.Context, data []byte) error {
	var evt model.RoomActivityEvent
	if err := json.Unmarshal(data, &evt); err != nil {
		return fmt.Errorf("unmarshal room activity event: %w", err)
	}
	if evt.RoomID == "" || evt.LastMsgAt <= 0 {
		return fmt.Errorf("room activity event missing roomId or lastMsgAt")
	}
	subscribed, err := h.hasRoomSubscription(ctx, evt.RoomID)
	if err != nil {
		return fmt.Errorf("check room subscription: %w", err)
	}
	if !subscribed {
		return nil
	}
	if err := h.store.UpsertRemoteRoomActivity(ctx, evt.RoomID, evt.SiteID, time.UnixMilli(evt.LastMsgAt).UTC()); err != nil {
		return fmt.Errorf("apply room activity: %w", err)
	}
	return nil
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
	case model.InboxMemberJoinedAtRefreshed:
		return h.handleMemberJoinedAtRefreshed(ctx, &evt)
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
	case "subscription_opened":
		return h.handleSubscriptionOpened(ctx, &evt)
	case "thread_subscription_upserted":
		return h.handleThreadSubscriptionUpserted(ctx, &evt)
	case "thread_read":
		return h.handleThreadRead(ctx, &evt)
	case model.InboxThreadReadAll:
		return h.handleThreadReadAll(ctx, &evt)
	case model.InboxThreadUnreadAdded:
		return h.handleThreadUnreadAdded(ctx, &evt)
	case model.InboxRoomRenamed:
		return h.handleRoomRenamed(ctx, &evt)
	case model.InboxRoomRestricted:
		return h.handleRoomVisibilityChanged(ctx, &evt)
	case model.InboxUserStatusUpdated:
		return h.handleUserStatusUpdated(ctx, &evt)
	case model.InboxUserSettingsUpdated:
		return h.handleUserSettingsUpdated(ctx, &evt)
	case model.InboxUserPermissionsUpdated:
		return h.handleUserPermissionsUpdated(ctx, &evt)
	case model.InboxUserChatlistUpdated:
		return h.handleUserChatlistUpdated(ctx, &evt)
	case model.InboxSubscriptionSectionMoved:
		return h.handleSubscriptionSectionMoved(ctx, &evt)
	case model.InboxSubscriptionMention:
		return h.handleSubscriptionMention(ctx, &evt)
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
			Open:               true,
			// Stamp provenance on the federated sub so the origin filter (which reads
			// the sub's own origin, not the null cross-site $room.origin) can hide a
			// Teams room from a remote member. Empty for native rooms.
			Origin: event.Origin,
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

	// After the missing-user return, so a room that gained no subscriber leaves
	// no orphan row (nothing deletes them). Best-effort — the subscriptions above
	// must not be re-run because an ordering row failed — but not self-healing
	// yet, so alert on this log line.
	h.roomSubs.set(event.RoomID, true)
	if event.LastMsgAt != nil {
		if err := h.store.UpsertRemoteRoomActivity(ctx, event.RoomID, event.SiteID, time.UnixMilli(*event.LastMsgAt).UTC()); err != nil {
			slog.WarnContext(ctx, "seed remote room activity failed",
				"room_id", event.RoomID, "site", event.SiteID, "error", err)
		}
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
// handleMemberJoinedAtRefreshed applies the Teams migration's joinedAt correction
// to the home-site replicas (a joinedAt-only $set; a replica not yet present is a
// no-op — the next member_added carries the corrected joinedAt).
func (h *Handler) handleMemberJoinedAtRefreshed(ctx context.Context, evt *model.InboxEvent) error {
	var event model.MemberAddEvent
	if err := json.Unmarshal(evt.Payload, &event); err != nil {
		return fmt.Errorf("unmarshal member_joinedat_refreshed payload: %w", err)
	}
	if len(event.Accounts) == 0 {
		return nil
	}
	joinedAt := time.UnixMilli(event.JoinedAt).UTC()
	byAccount := make(map[string]time.Time, len(event.Accounts))
	for _, account := range event.Accounts {
		byAccount[account] = joinedAt
	}
	if err := h.store.BulkRefreshJoinedAt(ctx, event.RoomID, byAccount); err != nil {
		return fmt.Errorf("refresh joinedAt on replicas: %w", err)
	}
	return nil
}

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
	// Bust AFTER the write, in one batched round trip: this site's local
	// replica of each removed member's subscription is gone, so their cached
	// positive decision must die immediately, not linger for the L2 TTL.
	subauthcache.BustSubs(ctx, h.valkey, memberEvt.RoomID, memberEvt.Accounts)
	// Other members may remain, so re-resolve rather than caching a guess. When
	// none do, the ordering row has no reader left here — drop it, or it becomes
	// the orphan the seed path is careful not to create. Best-effort: a stale row
	// only costs space, and re-adding a member re-seeds it.
	h.roomSubs.invalidate(memberEvt.RoomID)
	if stillMember, err := h.hasRoomSubscription(ctx, memberEvt.RoomID); err != nil {
		slog.WarnContext(ctx, "check remaining members failed; leaving ordering row",
			"room_id", memberEvt.RoomID, "error", err)
	} else if !stillMember {
		if err := h.store.DeleteRemoteRoomActivity(ctx, memberEvt.RoomID); err != nil {
			slog.WarnContext(ctx, "delete remote room activity failed",
				"room_id", memberEvt.RoomID, "error", err)
		}
	}
	// Scrub the removed accounts' thread read-state on this site too (#308).
	if err := h.store.DeleteThreadSubscriptions(ctx, memberEvt.RoomID, memberEvt.Accounts); err != nil {
		return fmt.Errorf("delete thread subscriptions for room %s: %w", memberEvt.RoomID, err)
	}
	// A removed member's badge entry for this room is stale.
	if h.badge != nil {
		for _, account := range memberEvt.Accounts {
			h.badge.ClearRoom(ctx, account, memberEvt.RoomID)
		}
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
	// Bust AFTER the write: this site's local replica's cached Roles must not
	// keep serving the pre-change decision.
	subauthcache.BustSub(ctx, h.valkey, roomID, account)
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
	applied, threadUnread, err := h.store.UpdateSubscriptionRead(ctx, e.RoomID, e.Account, lastSeenAt, e.Alert)
	if err != nil {
		return fmt.Errorf("update subscription read for %q in room %q: %w", e.Account, e.RoomID, err)
	}
	// The read settles message-unread, so the room stays unread only via an
	// unread followed thread. Skip entirely when the order guard rejected the
	// event — nothing was written, so nothing should be invalidated.
	if applied && h.badge != nil && threadUnread == 0 {
		h.badge.ClearRoom(ctx, e.Account, e.RoomID)
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
	// Mute is an exact removal (set stays fresh); unmute drops the set so the
	// next recompute re-adds the room iff unread.
	if h.badge != nil {
		if e.Muted {
			h.badge.ClearRoom(ctx, e.Account, e.RoomID)
		} else {
			h.badge.ClearAll(ctx, e.Account)
		}
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

// handleSubscriptionOpened mirrors a room-side open onto the user's home-site subscription.
func (h *Handler) handleSubscriptionOpened(ctx context.Context, evt *model.InboxEvent) error {
	var e model.SubscriptionOpenedEvent
	if err := json.Unmarshal(evt.Payload, &e); err != nil {
		return fmt.Errorf("unmarshal subscription_opened payload: %w", err)
	}
	if err := h.store.UpdateSubscriptionOpen(ctx, e.RoomID, e.Account, e.Open); err != nil {
		return fmt.Errorf("update subscription open for %q in room %q: %w", e.Account, e.RoomID, err)
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
	if err := h.store.ApplyThreadRead(ctx, e.RoomID, e.ThreadRoomID, e.Account, e.ParentMessageID, lastSeenAt); err != nil {
		return fmt.Errorf("apply thread read (thread %q, account %q): %w",
			e.ThreadRoomID, e.Account, err)
	}
	// A thread read shrinks the unread set — drop it and recompute on next
	// count; the recompute also absorbs stale/redelivered events and racing
	// thread_unread_added writes.
	if h.badge != nil {
		h.badge.ClearAll(ctx, e.Account)
	}
	return nil
}

func (h *Handler) handleThreadReadAll(ctx context.Context, evt *model.InboxEvent) error {
	var e model.ThreadReadAllEvent
	if err := json.Unmarshal(evt.Payload, &e); err != nil {
		return fmt.Errorf("unmarshal thread_read_all payload: %w", err)
	}
	lastSeenAt := time.UnixMilli(e.LastSeenAt).UTC()
	if err := h.store.ApplyThreadReadAll(ctx, e.Account, lastSeenAt); err != nil {
		return fmt.Errorf("apply thread read all (account %q): %w", e.Account, err)
	}
	// Bulk dismiss on the home replica — ClearAll always applies.
	if h.badge != nil {
		h.badge.ClearAll(ctx, e.Account)
	}
	return nil
}

// handleThreadUnreadAdded $addToSet-merges parentMessageID into the
// home-replica Subscription.threadUnread for each account in the event.
func (h *Handler) handleThreadUnreadAdded(ctx context.Context, evt *model.InboxEvent) error {
	var e model.ThreadUnreadAddedEvent
	if err := json.Unmarshal(evt.Payload, &e); err != nil {
		return fmt.Errorf("unmarshal thread_unread_added payload: %w", err)
	}
	if err := h.store.AddThreadUnread(ctx, e.RoomID, e.ParentMessageID, e.Accounts); err != nil {
		return fmt.Errorf("add thread unread %q in room %q: %w", e.ParentMessageID, e.RoomID, err)
	}
	return nil
}

// handleSubscriptionMention replicates a room-level @-mention badge onto the
// mentionees' home replicas. The store's read guard makes it idempotent and
// order-safe against a concurrent subscription_read.
func (h *Handler) handleSubscriptionMention(ctx context.Context, evt *model.InboxEvent) error {
	var e model.SubscriptionMentionEvent
	if err := json.Unmarshal(evt.Payload, &e); err != nil {
		return errcode.Permanent(errcode.BadRequest("unmarshal subscription_mention payload"))
	}
	// Poison payload: a blank room matches nothing, an empty account list has no
	// destination, and a zero mentionedAt badges as 1970 — which makes the read
	// guard skip everyone who has ever read the room.
	if e.RoomID == "" || len(e.Accounts) == 0 || e.MentionedAt <= 0 {
		slog.WarnContext(ctx, "subscription_mention missing roomId, accounts or mentionedAt",
			"room_id", e.RoomID, "accounts", len(e.Accounts), "mentioned_at", e.MentionedAt,
			"origin_site", evt.SiteID)
		return errcode.Permanent(errcode.BadRequest("subscription_mention missing roomId, accounts or mentionedAt"))
	}
	if err := h.store.SetSubscriptionMentions(ctx, e.RoomID, e.Accounts, time.UnixMilli(e.MentionedAt).UTC()); err != nil {
		return fmt.Errorf("set subscription mentions in room %q: %w", e.RoomID, err)
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
	// Bust every local subscriber's subauthcache L2 entry in one batched round
	// trip: ApplySubscriptionRestriction is the same store method
	// room-service's roomRestricted calls, and it can bulk-rewrite Roles for
	// every subscriber (owner set, everyone else demoted) alongside the
	// restricted/externalAccess flags — not just OwnerAccount.
	//
	// A listing failure is RETRYABLE, not best-effort. By this point the write
	// has already made every cached authorization decision for this room wrong,
	// so Acking here would leave demoted members passing authorization from L2
	// for the rest of the TTL. The whole event is idempotent — the restriction
	// write is timestamp-guarded and the bust is a delete — so returning the
	// error costs one redelivery and completes the invalidation.
	accounts, err := h.store.ListSubscriptionAccountsByRoom(ctx, p.RoomID)
	if err != nil {
		return fmt.Errorf("list local subscribers for subauthcache bust (room %s): %w", p.RoomID, err)
	}
	subauthcache.BustSubs(ctx, h.valkey, p.RoomID, accounts)
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

// handleUserPermissionsUpdated applies one chunk of an admin permission batch. A missing
// user doc is a silent no-op (store-level guard), matching the other user events.
func (h *Handler) handleUserPermissionsUpdated(ctx context.Context, evt *model.InboxEvent) error {
	var e model.UserPermissionsUpdated
	if err := json.Unmarshal(evt.Payload, &e); err != nil {
		return fmt.Errorf("unmarshal user_permissions_updated payload: %w", err)
	}
	if _, ok := model.PermissionFieldName(e.Permission); !ok {
		// A future permission key reaching a not-yet-upgraded site: retrying cannot
		// succeed and must not poison the consumer — warn and Ack.
		slog.WarnContext(ctx, "unknown permission key in user_permissions_updated", "permission", string(e.Permission))
		return nil
	}
	if err := h.store.ApplyUserPermissions(ctx, e.Permission, e.Accounts, e.State); err != nil {
		return fmt.Errorf("apply user permissions: %w", err)
	}
	return nil
}

// handleUserChatlistUpdated mirrors a cross-site chatlist change onto the local users doc,
// guarded by the event Timestamp so an out-of-order or duplicate delivery can't regress.
func (h *Handler) handleUserChatlistUpdated(ctx context.Context, evt *model.InboxEvent) error {
	var e model.UserChatlistUpdated
	if err := json.Unmarshal(evt.Payload, &e); err != nil {
		return fmt.Errorf("unmarshal user_chatlist_updated payload: %w", err)
	}
	if err := h.store.UpdateUserChatlist(ctx, e.Account, &e.Chatlist, e.Timestamp); err != nil {
		return fmt.Errorf("update user chatlist for %q: %w", e.Account, err)
	}
	return nil
}

// handleSubscriptionSectionMoved mirrors a room-side section move onto the user's
// home-site subscription, guarded by sectionUpdatedAt (the event Timestamp).
func (h *Handler) handleSubscriptionSectionMoved(ctx context.Context, evt *model.InboxEvent) error {
	var e model.SubscriptionSectionMovedEvent
	if err := json.Unmarshal(evt.Payload, &e); err != nil {
		return fmt.Errorf("unmarshal subscription_section_moved payload: %w", err)
	}
	if err := h.store.UpdateSubscriptionSection(ctx, e.RoomID, e.Account, e.SectionID, e.SectionOrder, time.UnixMilli(e.Timestamp).UTC()); err != nil {
		return fmt.Errorf("update subscription section for %q in room %q: %w", e.Account, e.RoomID, err)
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

// isBot reports whether account is bot-like — a real ".bot" bot or the
// "p_admin" platform-admin pseudo-account — via the model taxonomy. Plain
// "p_" QA test accounts are ordinary users and return false.
func isBot(account string) bool {
	return model.IsBot(account) || model.IsPlatformAdminAccount(account)
}

func subscriptionIsSubscribed(roomType model.RoomType, u *model.User) bool {
	if roomType != model.RoomTypeBotDM {
		return false
	}
	return !isBot(u.Account)
}
