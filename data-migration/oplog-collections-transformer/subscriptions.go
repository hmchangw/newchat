package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/migration"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
)

// sourceSubscription is the subset of a rocketchat_subscriptions doc the mapper decodes (handles $date).
type sourceSubscription struct {
	ID string `bson:"_id"`
	U  struct {
		ID       string `bson:"_id"`
		Username string `bson:"username"`
	} `bson:"u"`
	RID                  string    `bson:"rid"`
	T                    string    `bson:"t"`
	Name                 string    `bson:"name"`
	FName                string    `bson:"fname"`
	Roles                []string  `bson:"roles"`
	Open                 bool      `bson:"open"`
	F                    bool      `bson:"f"`
	DisableNotifications bool      `bson:"disableNotifications"`
	LS                   time.Time `bson:"ls"`
	LR                   time.Time `bson:"lr"`
	Alert                bool      `bson:"alert"`
	TS                   time.Time `bson:"ts"`
	UpdatedAt            time.Time `bson:"_updatedAt"`
	// Federation.Origin is the subscription's home site (absent ⇒ local); drives siteId stamping.
	Federation struct {
		Origin string `bson:"origin"`
	} `bson:"federation"`
}

// lastSeenMillis returns max(ls, lr) in unix ms — the furthest point consumed by either the
// scrolled cursor (ls) or the explicit mark-read (lr), per spec §4.3 / D1.
func (s *sourceSubscription) lastSeenMillis() int64 {
	// Zero-guard each: a zero time.Time.UnixMilli() is a large negative (year-0001) that would
	// leak a bogus lastSeenAt into the inbox event. Absent ls/lr → 0 (never-read subscription).
	var ls, lr int64
	if !s.LS.IsZero() {
		ls = s.LS.UTC().UnixMilli()
	}
	if !s.LR.IsZero() {
		lr = s.LR.UTC().UnixMilli()
	}
	if lr > ls {
		return lr
	}
	return ls
}

// subUpdateDescription is the connector's update delta; only changed field keys matter, values are opaque.
type subUpdateDescription struct {
	UpdatedFields map[string]any `bson:"updatedFields" json:"updatedFields"`
	RemovedFields []string       `bson:"removedFields" json:"removedFields"`
}

// subChanged reports whether the named field appears in the update delta (set or removed).
func subChanged(desc subUpdateDescription, field string) bool {
	if _, ok := desc.UpdatedFields[field]; ok {
		return true
	}
	for _, rf := range desc.RemovedFields {
		if rf == field {
			return true
		}
	}
	return false
}

// handleSubscription maps a rocketchat_subscriptions change event to inbox InboxEvents (§4.3 / §4.0):
// insert/replace reproduce the source row, update emits the matching event(s), delete maps by _id.
//
//nolint:gocritic // ev passed by value to mirror handle's signature; off the hot path.
func (h *handler) handleSubscription(ctx context.Context, ev oplogEvent) error {
	if ev.Op == "delete" {
		// True row delete is un-actionable (spec §4.0/§4.3): the event carries only the source _id,
		// which doesn't map to the destination sub (keyed by a generated UUIDv7). A genuine leave
		// arrives as an open:false update (→ member_removed) instead, so true deletes are rare.
		slog.Debug("skip subscription delete (un-actionable; leave is open:false)",
			"eventId", ev.EventID, "request_id", natsutil.RequestIDFromContext(ctx))
		h.metrics.onSkipped(ctx, "subscription_delete")
		return migration.ErrSkipped
	}

	doc, skip, err := h.resolveDoc(ctx, ev)
	if err != nil {
		return fmt.Errorf("resolve subscription doc: %w", err)
	}
	if skip {
		h.metrics.onSkipped(ctx, ev.Op+"_skip")
		return migration.ErrSkipped
	}

	var ss sourceSubscription
	if uerr := bson.UnmarshalExtJSON(doc, false, &ss); uerr != nil {
		return fmt.Errorf("%w: decode source subscription: %v", migration.ErrPoison, uerr) //nolint:errorlint // intentional single-%w sentinel wrap; decode err is informational only
	}

	if ev.Op == "update" {
		return h.handleSubscriptionUpdate(ctx, ev, &ss)
	}
	// insert / replace → rebuild the full source row.
	return h.publishSubscriptionState(ctx, &ss, true)
}

// handleSubscriptionUpdate emits the event(s) matching the changed fields. An open toggle is the
// dominant action (membership lifecycle); other recognized field deltas map to a single state event.
//
//nolint:gocritic // ev passed by value to mirror handle's signature; off the hot path.
func (h *handler) handleSubscriptionUpdate(ctx context.Context, ev oplogEvent, ss *sourceSubscription) error {
	var desc subUpdateDescription
	if len(ev.UpdateDescription) > 0 {
		if err := bson.UnmarshalExtJSON(ev.UpdateDescription, false, &desc); err != nil {
			return fmt.Errorf("%w: decode subscription updateDescription: %v", migration.ErrPoison, err) //nolint:errorlint // intentional single-%w sentinel wrap; decode err is informational only
		}
	}

	// Membership leave/rejoin is an open toggle and dominates: re-read decides the action.
	if subChanged(desc, "open") {
		if ss.Open {
			// Re-subscribe: rebuild the full state (a rejoin starts fresh).
			return h.publishSubscriptionState(ctx, ss, true)
		}
		return h.pub.Publish(ctx, h.memberRemovedEvent(ss))
	}

	siteID := siteIDFromOrigin(ss.Federation.Origin, h.siteID)
	emitted := false

	// Emit even when roles became empty: a roles-cleared update (e.g. owner demoted) must propagate,
	// otherwise the destination keeps stale roles. roleUpdatedEvent maps nil → nil cleanly.
	if subChanged(desc, "roles") {
		if err := h.pub.Publish(ctx, h.roleUpdatedEvent(ss, siteID)); err != nil {
			return fmt.Errorf("publish role_updated: %w", err)
		}
		emitted = true
	}
	if subChanged(desc, "disableNotifications") {
		if err := h.pub.Publish(ctx, h.muteEvent(ss, siteID)); err != nil {
			return err
		}
		emitted = true
	}
	if subChanged(desc, "f") {
		if err := h.pub.Publish(ctx, h.favoriteEvent(ss, siteID)); err != nil {
			return err
		}
		emitted = true
	}
	// ls/lr/alert all map to a single subscription_read using the current max(ls,lr)+alert.
	if subChanged(desc, "ls") || subChanged(desc, "lr") || subChanged(desc, "alert") {
		if err := h.pub.Publish(ctx, h.readEvent(ss, siteID)); err != nil {
			return err
		}
		emitted = true
	}

	if !emitted {
		// name/fname changes are driven by the room rename path, not the sub; any other field is noise.
		slog.Debug("skip subscription update (no recognized field changed)",
			"eventId", ev.EventID, "request_id", natsutil.RequestIDFromContext(ctx))
		h.metrics.onSkipped(ctx, "subscription_update_noop")
		return migration.ErrSkipped
	}
	return nil
}

// publishSubscriptionState emits member_added followed by the state events that reproduce the
// source row (role/mute/favorite/read). Used by insert/replace and by an open false→true rejoin.
func (h *handler) publishSubscriptionState(ctx context.Context, ss *sourceSubscription, withMemberAdded bool) error {
	siteID := siteIDFromOrigin(ss.Federation.Origin, h.siteID)

	if withMemberAdded {
		if err := h.pub.Publish(ctx, h.memberAddedEvent(ss, siteID)); err != nil {
			return err
		}
	}
	if len(ss.Roles) > 0 {
		if err := h.pub.Publish(ctx, h.roleUpdatedEvent(ss, siteID)); err != nil {
			return err
		}
	}
	if err := h.pub.Publish(ctx, h.muteEvent(ss, siteID)); err != nil {
		return err
	}
	if err := h.pub.Publish(ctx, h.favoriteEvent(ss, siteID)); err != nil {
		return err
	}
	return h.pub.Publish(ctx, h.readEvent(ss, siteID))
}

// memberAddedEvent builds the member_added InboxEvent. RoomType is classified from t alone
// (a sub can't see prid/teamId/bot, so discussion/botDM degrade to channel/dm); roles via role_updated.
func (h *handler) memberAddedEvent(ss *sourceSubscription, siteID string) model.InboxEvent {
	class := classifyRoom(ss.T, false, false, false, 2)
	// Zero-guard ts → now so an absent source ts never becomes a year-0001 JoinedAt.
	joinedAt := ss.TS.UTC().UnixMilli()
	if ss.TS.IsZero() {
		joinedAt = h.nowMillis()
	}
	payload := mustMarshal(model.MemberAddEvent{
		Type:     "member_added",
		RoomID:   ss.RID,
		Accounts: []string{ss.U.Username},
		RoomType: class.Type,
		RoomName: ss.FName,
		// inbox-worker names a DM/botDM subscription after RequesterAccount (subscriptionName
		// switches on room type); RC stores the peer username in the sub's name field, so pass it
		// through — otherwise every migrated DM lands with Name="".
		RequesterAccount: ss.Name,
		SiteID:           siteID,
		JoinedAt:         joinedAt,
		Timestamp:        h.nowMillis(),
	})
	return h.inboxEvent(model.InboxMemberAdded, siteID, payload)
}

// memberRemovedEvent builds the member_removed InboxEvent (open true→false leave).
func (h *handler) memberRemovedEvent(ss *sourceSubscription) model.InboxEvent {
	siteID := siteIDFromOrigin(ss.Federation.Origin, h.siteID)
	payload := mustMarshal(model.MemberRemoveEvent{
		Type:      "member_removed",
		RoomID:    ss.RID,
		Accounts:  []string{ss.U.Username},
		SiteID:    siteID,
		Timestamp: h.nowMillis(),
	})
	return h.inboxEvent(model.InboxMemberRemoved, siteID, payload)
}

// roleUpdatedEvent builds the role_updated InboxEvent. inbox-worker.handleRoleUpdated decodes a
// SubscriptionUpdateEvent and applies Subscription.{User.Account, RoomID, Roles}.
// subUpdatedAtMillis is the source subscription's _updatedAt — the high-water mark the field-update
// guards (mute/favorite/roles) stamp on their events, stable across redelivery so a re-delivered
// inline insert snapshot can't out-rank a newer update. Falls back to now() when the source omits it.
func (h *handler) subUpdatedAtMillis(ss *sourceSubscription) int64 {
	if ss.UpdatedAt.IsZero() {
		return h.nowMillis()
	}
	return ss.UpdatedAt.UTC().UnixMilli()
}

func (h *handler) roleUpdatedEvent(ss *sourceSubscription, siteID string) model.InboxEvent {
	payload := mustMarshal(model.SubscriptionUpdateEvent{
		Subscription: model.Subscription{
			User:   model.SubscriptionUser{ID: ss.U.ID, Account: ss.U.Username},
			RoomID: ss.RID,
			Roles:  mapSubscriptionRoles(ss.Roles),
		},
		Action:    "role_updated",
		Timestamp: h.subUpdatedAtMillis(ss),
	})
	return h.inboxEvent(model.InboxEventType("role_updated"), siteID, payload)
}

func (h *handler) muteEvent(ss *sourceSubscription, siteID string) model.InboxEvent {
	payload := mustMarshal(model.SubscriptionMuteToggledEvent{
		Account:   ss.U.Username,
		RoomID:    ss.RID,
		Muted:     ss.DisableNotifications,
		Timestamp: h.subUpdatedAtMillis(ss),
	})
	return h.inboxEvent(model.InboxSubscriptionMuteToggled, siteID, payload)
}

func (h *handler) favoriteEvent(ss *sourceSubscription, siteID string) model.InboxEvent {
	payload := mustMarshal(model.SubscriptionFavoriteToggledEvent{
		Account:   ss.U.Username,
		RoomID:    ss.RID,
		Favorite:  ss.F,
		Timestamp: h.subUpdatedAtMillis(ss),
	})
	return h.inboxEvent(model.InboxSubscriptionFavoriteToggled, siteID, payload)
}

func (h *handler) readEvent(ss *sourceSubscription, siteID string) model.InboxEvent {
	payload := mustMarshal(model.SubscriptionReadEvent{
		Account:    ss.U.Username,
		RoomID:     ss.RID,
		LastSeenAt: ss.lastSeenMillis(),
		Alert:      ss.Alert,
		Timestamp:  h.nowMillis(),
	})
	return h.inboxEvent(model.InboxSubscriptionRead, siteID, payload)
}

// mapSubscriptionRoles maps RocketChat role strings to model.Role: "owner" → RoleOwner; everything
// else (RC "moderator"/"leader"/"user", which the new model lacks) → RoleMember. Empty source roles
// (a RocketChat demotion clears the array) map to the [member] floor — the new stack's invariant is
// roles are never empty (room-service writes ["member"] after a live demotion), and inbox-worker
// permanently drops a role_updated with no roles, so an empty mapping would silently lose demotions.
func mapSubscriptionRoles(roles []string) []model.Role {
	if len(roles) == 0 {
		return []model.Role{model.RoleMember}
	}
	out := make([]model.Role, 0, len(roles))
	for _, r := range roles {
		if r == string(model.RoleOwner) {
			out = append(out, model.RoleOwner)
		} else {
			out = append(out, model.RoleMember)
		}
	}
	return out
}

// mustMarshal JSON-encodes a fixed-shape model payload; json.Marshal cannot fail on these,
// so an error is a programmer error and panics.
func mustMarshal(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("marshal inbox payload: %v", err))
	}
	return b
}
