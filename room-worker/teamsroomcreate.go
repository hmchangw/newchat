package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/roomkeystore"
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

	// A Teams chat id (…@thread.v2 / …@unq.gbl.spaces) can't be a room id — its
	// dots/@ break NATS subject tokenisation — so derive a base62 id from it
	// deterministically (same id on every redelivery). Name falls back to the
	// members when the chat has no topic (a DM or an unnamed group).
	name := chat.Name
	if name == "" {
		name = composeMigratedRoomName(chat.Members)
	}
	room := &model.Room{
		ID:        idgen.DeterministicID([]byte(chat.ID)),
		Name:      name,
		Type:      roomTypeFromTeamsChatID(chat.ID),
		SiteID:    h.siteID,
		Origin:    model.OriginTeams,
		CreatedAt: chat.CreatedDateTime.UTC(),
		UpdatedAt: acceptedAt,
	}
	// A migrated room stays live — members keep chatting — so a channel room needs
	// an encryption key like any native channel: generate it, persist it with the
	// room, and fan it out to members below. On redelivery reuse the persisted key.
	newPair, err := roomkeystore.GenerateKeyPair()
	if err != nil {
		return fmt.Errorf("generate room key: %w", err)
	}
	inserted, err := h.store.CreateRoom(ctx, room, newPair)
	if err != nil {
		return fmt.Errorf("create room: %w", err)
	}
	pair := &roomkeystore.VersionedKeyPair{Version: 0, KeyPair: *newPair}
	if !inserted {
		pair, err = h.existingRoomKey(ctx, room.ID, newPair)
		if err != nil {
			return err
		}
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
	var addedUsers []model.User // for the room-key fan-out

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
		user, err := h.resolveMember(ctx, member.Account, member.ID, member.DisplayName)
		if err != nil {
			slog.WarnContext(ctx, "teams room-create: skip member, resolve failed",
				"chat_id", chat.ID, "account", member.Account, "error", err)
			continue
		}
		memberSite[member.Account] = user.SiteID
		// Human members get owner+member so they can admin the migrated room; bot
		// and platform-admin accounts stay member-only.
		roles := []model.Role{model.RoleOwner, model.RoleMember}
		if model.IsBot(member.Account) || model.IsPlatformAdminAccount(member.Account) {
			roles = []model.Role{model.RoleMember}
		}
		sub := newSub(idgen.GenerateUUIDv7(), user, room, roles, room.Name, false, acceptedAt)
		sub.Origin = model.OriginTeams
		if !member.VisibleHistoryStartDateTime.IsZero() {
			t := member.VisibleHistoryStartDateTime.UTC()
			sub.HistorySharedSince = &t
		}
		newSubs = append(newSubs, sub)
		addedUsers = append(addedUsers, *user)
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

	// Bulk migration keeps the standalone room.key event (interactive paths deliver it inline on
	// "added"): no live added event fires here, but the key must land before migrated history decrypts.
	if len(addedUsers) > 0 {
		if err := h.buildAndFanOutRoomKey(ctx, room.ID, pair, addedUsers); err != nil {
			return fmt.Errorf("fan out room key: %w", err)
		}
	}

	if err := h.federateTeamsMembership(ctx, room, added, model.InboxMemberAdded, memberSite, acceptedAt); err != nil {
		return fmt.Errorf("federate added members: %w", err)
	}
	if err := h.federateTeamsMembership(ctx, room, removed, model.InboxMemberRemoved, memberSite, acceptedAt); err != nil {
		return fmt.Errorf("federate departed members: %w", err)
	}
	return nil
}

// teamsDMChatIDSuffix marks a Teams 1:1 chat id; group/meeting ids end in @thread.v2.
const teamsDMChatIDSuffix = "@unq.gbl.spaces"

// roomTypeFromTeamsChatID classifies a migrated room by the Teams chat id suffix:
// a 1:1 (…@unq.gbl.spaces) is a DM, anything else (group/meeting) a channel.
func roomTypeFromTeamsChatID(chatID string) model.RoomType {
	if strings.HasSuffix(chatID, teamsDMChatIDSuffix) {
		return model.RoomTypeDM
	}
	return model.RoomTypeChannel
}

// composeMigratedRoomName joins the members' display names with ", " — the room
// name for a DM or an unnamed group (a chat with no Teams topic). Isolated so the
// format is a one-place change.
func composeMigratedRoomName(members []model.TeamsRoomCreateMember) string {
	names := make([]string, 0, len(members))
	for _, m := range members {
		if m.DisplayName != "" {
			names = append(names, m.DisplayName)
		}
	}
	return strings.Join(names, ", ")
}

// resolveMember returns account's user, publishing a new identity when none
// exists. The publisher owns the deterministic _id so every site's hr-sync-worker
// upserts the same one; nothing is written locally first, so a redelivery
// re-derives the same _id instead of splitting the user. Mirrors message-worker's
// Teams sender resolver.
func (h *Handler) resolveMember(ctx context.Context, account, graphID, displayName string) (*model.User, error) {
	u, err := h.store.GetUser(ctx, account)
	if err == nil {
		return u, nil
	}
	if !errors.Is(err, ErrUserNotFound) {
		return nil, fmt.Errorf("get user: %w", err)
	}

	// A deterministic _id from the Graph id means this person maps to one identity
	// at every site (hr-sync-worker keys the upsert on account). No employeeId —
	// migrated externals aren't employees.
	id := idgen.DeterministicID([]byte(graphID))
	// displayName lands in ChineseName (mirrors message-worker's sender resolver).
	nu := model.User{ID: id, Account: account, SiteID: h.siteID, ChineseName: displayName}
	if h.publishUsers != nil {
		iuc := model.IUserWithChange{User: nu, ChangeType: model.IChangeTypeNewHire}
		if err := h.publishUsers(ctx, []model.IUserWithChange{iuc}); err != nil {
			return nil, fmt.Errorf("publish user identity fanout: %w", err)
		}
	}
	return &nu, nil
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
	// The internal lane carries the InboxEvent envelope like every other
	// InboxInternal publisher — search-sync-worker decodes evt.Payload. The
	// federated branch below stays unwrapped: outbox.Publish builds its own.
	internalData, err := json.Marshal(model.InboxEvent{
		Type:       eventType,
		SiteID:     h.siteID,
		DestSiteID: h.siteID,
		Payload:    payload,
		Timestamp:  acceptedAt.UnixMilli(),
	})
	if err != nil {
		return fmt.Errorf("marshal internal inbox envelope: %w", err)
	}
	seed := fmt.Sprintf("%s:%s:%d", room.ID, eventType, acceptedAt.UnixMilli())
	if err := h.publish(ctx, subject.InboxInternal(h.siteID, eventType), internalData, natsutil.InboxDedupID(ctx, h.siteID, seed)); err != nil {
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
