package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
)

// processTeamsRoomCreate reconciles each chat in a Teams room-creation batch
// against its full member list. Migration import, not a live mutation: no sys
// messages or live push — clients pick up the change on their next fetch. Every
// publish carries X-Migration: live so a live-delivery consumer won't re-notify.
// Per-chat failure is isolated (WARN + continue); only a malformed envelope is poison.
func (h *Handler) processTeamsRoomCreate(ctx context.Context, data []byte) error {
	var evt model.TeamsRoomCreateEvent
	if err := json.Unmarshal(data, &evt); err != nil {
		return permanent(errcode.BadRequest("unmarshal teams room-create batch"))
	}
	ctx = natsutil.WithMigrationLiveContext(ctx)
	acceptedAt := time.UnixMilli(evt.Timestamp).UTC()
	for i := range evt.Chats {
		chat := &evt.Chats[i]
		if err := h.reconcileTeamsRoom(ctx, chat, acceptedAt); err != nil {
			slog.WarnContext(ctx, "teams room-create: chat reconcile failed, skipping",
				"chat_id", chat.ID, "error", err)
		}
	}
	return nil
}

// reconcileTeamsRoom upserts the chat's room, then reconciles subscriptions to
// chat.Members: add new, hard-delete departed (same as the live remove path). A
// member re-added in a later batch just gets a fresh sub. Both directions federate home.
func (h *Handler) reconcileTeamsRoom(ctx context.Context, chat *model.TeamsRoomCreateChat, acceptedAt time.Time) error {
	if chat.ID == "" {
		return errors.New("chat has no id")
	}

	room := &model.Room{
		ID:        chat.ID, // chat id is the room id — idempotent on redelivery
		Name:      chat.Name,
		Type:      model.RoomTypeChannel,
		SiteID:    h.siteID,
		Origin:    model.OriginTeams,
		CreatedAt: chat.CreatedDateTime.UTC(),
		UpdatedAt: acceptedAt,
	}
	// Teams-migrated rooms carry no E2E room key, matching #107's message path
	// (which persists migrated history without one): nil key.
	if _, err := h.store.CreateRoom(ctx, room, nil); err != nil {
		return fmt.Errorf("create room: %w", err)
	}

	existingSubs, err := h.store.ListByRoom(ctx, room.ID)
	if err != nil {
		return fmt.Errorf("list existing subs: %w", err)
	}
	existingByAccount := make(map[string]*model.Subscription, len(existingSubs))
	for i := range existingSubs {
		existingByAccount[existingSubs[i].User.Account] = &existingSubs[i]
	}

	wantAccounts := make(map[string]struct{}, len(chat.Members))
	memberSite := make(map[string]string, len(chat.Members)) // account -> home site, for federation
	var newSubs []*model.Subscription

	for _, member := range chat.Members {
		if member.Account == "" {
			slog.WarnContext(ctx, "teams room-create: skip member with no account", "chat_id", chat.ID)
			continue
		}
		if _, seen := wantAccounts[member.Account]; seen {
			continue // duplicate account in the batch — already handled
		}
		wantAccounts[member.Account] = struct{}{}
		if _, ok := existingByAccount[member.Account]; ok {
			continue // already a member — no change
		}
		user, err := h.resolveMember(ctx, member.Account)
		if err != nil {
			slog.WarnContext(ctx, "teams room-create: skip member, resolve failed",
				"chat_id", chat.ID, "account", member.Account, "error", err)
			continue
		}
		memberSite[member.Account] = user.SiteID
		sub := newSub(idgen.GenerateUUIDv7(), user, room, []model.Role{model.RoleMember}, room.Name, false, acceptedAt)
		sub.Origin = model.OriginTeams
		if !member.VisibleHistoryStartDateTime.IsZero() {
			t := member.VisibleHistoryStartDateTime.UTC()
			sub.HistorySharedSince = &t
		}
		newSubs = append(newSubs, sub)
	}

	var removed []string
	for account, existing := range existingByAccount {
		if _, want := wantAccounts[account]; !want {
			removed = append(removed, account)
			memberSite[account] = existing.SiteID
		}
	}

	if len(newSubs) > 0 {
		if err := h.store.BulkCreateSubscriptions(ctx, newSubs); err != nil {
			return fmt.Errorf("bulk create subs: %w", err)
		}
	}
	if len(removed) > 0 {
		if _, err := h.store.DeleteSubscriptionsByAccounts(ctx, room.ID, removed); err != nil {
			return fmt.Errorf("delete departed subs: %w", err)
		}
	}

	added := make([]string, 0, len(newSubs))
	for _, s := range newSubs {
		added = append(added, s.User.Account)
	}
	if len(added) == 0 && len(removed) == 0 {
		return nil // fully converged already — idempotent no-op, skip counts/federation
	}

	if err := h.store.ReconcileMemberCounts(ctx, room.ID); err != nil {
		return fmt.Errorf("reconcile member counts: %w", err)
	}
	h.bustRoomMeta(ctx, room.ID)

	if err := h.federateTeamsMembership(ctx, room, added, model.InboxMemberAdded, memberSite, acceptedAt); err != nil {
		return fmt.Errorf("federate added members: %w", err)
	}
	if err := h.federateTeamsMembership(ctx, room, removed, model.InboxMemberRemoved, memberSite, acceptedAt); err != nil {
		return fmt.Errorf("federate departed members: %w", err)
	}
	return nil
}

// resolveMember returns account's user, creating a keyless external identity
// (publish-first) when none exists — mirrors message-worker's Teams sender
// resolver so member and sender on the same account converge on one _id.
func (h *Handler) resolveMember(ctx context.Context, account string) (*model.User, error) {
	u, err := h.store.GetUser(ctx, account)
	if err == nil {
		return u, nil
	}
	if !errors.Is(err, ErrUserNotFound) {
		return nil, fmt.Errorf("get user: %w", err)
	}

	nu := model.User{Account: account, SiteID: h.siteID}
	iuc := model.IUserWithChange{User: nu, ChangeType: model.IChangeTypeNewHire}
	if h.publishUsers != nil {
		// Publish-first: if it failed after the local write, redelivery would
		// find the user, skip the publish, and leave it un-fanned to other sites.
		if err := h.publishUsers(ctx, []model.IUserWithChange{iuc}); err != nil {
			return nil, fmt.Errorf("publish user identity fanout: %w", err)
		}
	}
	if err := h.store.UpsertExternalUserIdentity(ctx, &nu); err != nil {
		return nil, fmt.Errorf("upsert external user identity: %w", err)
	}
	created, err := h.store.GetUser(ctx, account)
	if err != nil {
		return nil, fmt.Errorf("read back created identity: %w", err)
	}
	return created, nil
}

// federateTeamsMembership publishes one local InboxInternal event (so
// search-sync-worker indexes the membership change) plus one federated event
// per remote destination site, bucketing accounts by their home site the same
// way the live add/remove paths do. No-op on an empty account list.
func (h *Handler) federateTeamsMembership(ctx context.Context, room *model.Room, accounts []string, eventType model.InboxEventType, memberSite map[string]string, acceptedAt time.Time) error {
	if len(accounts) == 0 {
		return nil
	}
	evt := model.InboxMemberEvent{
		RoomID:    room.ID,
		RoomName:  room.Name,
		RoomType:  room.Type,
		SiteID:    h.siteID,
		Accounts:  accounts,
		Timestamp: acceptedAt.UnixMilli(),
	}
	payload, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal membership event: %w", err)
	}
	seed := fmt.Sprintf("%s:%s:%d", room.ID, eventType, acceptedAt.UnixMilli())
	if err := h.publish(ctx, subject.InboxInternal(h.siteID, eventType), payload, natsutil.InboxDedupID(ctx, h.siteID, seed)); err != nil {
		return fmt.Errorf("local inbox publish: %w", err)
	}

	bySite := make(map[string][]string)
	for _, acc := range accounts {
		if site := memberSite[acc]; site != "" && site != h.siteID {
			bySite[site] = append(bySite[site], acc)
		}
	}
	for destSite, siteAccounts := range bySite {
		siteEvt := evt
		siteEvt.Accounts = siteAccounts
		siteData, err := json.Marshal(siteEvt)
		if err != nil {
			return fmt.Errorf("marshal federated membership event (dest %s): %w", destSite, err)
		}
		dedupID := natsutil.InboxDedupID(ctx, destSite, seed)
		if err := h.federate(ctx, room.ID, destSite, eventType, siteData, dedupID, acceptedAt.UnixMilli()); err != nil {
			return fmt.Errorf("federate to %s: %w", destSite, err)
		}
	}
	return nil
}
