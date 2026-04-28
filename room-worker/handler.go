package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

// PublishFunc publishes data; non-empty msgID sets Nats-Msg-Id for JetStream stream-level dedup.
type PublishFunc func(ctx context.Context, subj string, data []byte, msgID string) error

type Handler struct {
	store   SubscriptionStore
	siteID  string
	publish PublishFunc
}

func NewHandler(store SubscriptionStore, siteID string, publish PublishFunc) *Handler {
	return &Handler{store: store, siteID: siteID, publish: publish}
}

func (h *Handler) HandleJetStreamMsg(ctx context.Context, msg jetstream.Msg) {
	subj := msg.Subject()
	var err error
	switch {
	case strings.HasSuffix(subj, ".member.role-update"):
		err = h.processRoleUpdate(ctx, msg.Data())
	case strings.HasSuffix(subj, ".member.add"):
		err = h.processAddMembers(ctx, msg.Data())
	case strings.HasSuffix(subj, ".member.remove"):
		err = h.processRemoveMember(ctx, msg.Data())
	default:
		slog.Warn("unknown member operation", "subject", subj)
	}
	if err != nil {
		slog.Error("process message failed", "error", err, "subject", subj)
		if nakErr := msg.Nak(); nakErr != nil {
			slog.Error("failed to nak message", "error", nakErr)
		}
		return
	}
	if err := msg.Ack(); err != nil {
		slog.Error("failed to ack message", "error", err)
	}
}

func (h *Handler) processRoleUpdate(ctx context.Context, data []byte) error {
	var req model.UpdateRoleRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return fmt.Errorf("unmarshal role update request: %w", err)
	}

	// Promote: add "owner" to roles. Demote: remove "owner" from roles.
	switch req.NewRole {
	case model.RoleOwner:
		if err := h.store.AddRole(ctx, req.Account, req.RoomID, model.RoleOwner); err != nil {
			return fmt.Errorf("add owner role: %w", err)
		}
	case model.RoleMember:
		// Ensure member role exists before removing owner (prevents empty roles array)
		if err := h.store.AddRole(ctx, req.Account, req.RoomID, model.RoleMember); err != nil {
			return fmt.Errorf("ensure member role: %w", err)
		}
		if err := h.store.RemoveRole(ctx, req.Account, req.RoomID, model.RoleOwner); err != nil {
			return fmt.Errorf("remove owner role: %w", err)
		}
	default:
		return fmt.Errorf("unsupported role: %s", req.NewRole)
	}

	// Re-read subscription to get the updated roles for the event
	updatedSub, err := h.store.GetSubscription(ctx, req.Account, req.RoomID)
	if err != nil {
		return fmt.Errorf("get updated subscription: %w", err)
	}

	now := time.Now().UTC()
	subEvt := model.SubscriptionUpdateEvent{
		UserID:       updatedSub.User.ID,
		Subscription: *updatedSub,
		Action:       "role_updated",
		Timestamp:    now.UnixMilli(),
	}
	subEvtData, err := json.Marshal(subEvt)
	if err != nil {
		return fmt.Errorf("marshal subscription update event: %w", err)
	}
	if err := h.publish(ctx, subject.SubscriptionUpdate(updatedSub.User.Account), subEvtData, ""); err != nil {
		return fmt.Errorf("publish subscription update: %w", err)
	}

	// Look up user's siteID to determine if cross-site
	user, err := h.store.GetUser(ctx, req.Account)
	if err != nil {
		return fmt.Errorf("get user: %w", err)
	}

	// If user's site differs from room's site (h.siteID), publish outbox to user's home site
	if user.SiteID != h.siteID {
		outbox := model.OutboxEvent{
			Type:       "role_updated",
			SiteID:     h.siteID,
			DestSiteID: user.SiteID,
			Payload:    subEvtData,
			Timestamp:  now.UnixMilli(),
		}
		outboxData, err := json.Marshal(outbox)
		if err != nil {
			return fmt.Errorf("marshal outbox event: %w", err)
		}
		outboxSubj := subject.Outbox(h.siteID, user.SiteID, "role_updated")
		// req.Timestamp is the stable acceptance time — keeps Nats-Msg-Id identical on redelivery.
		dedupID := idgen.DeriveID(fmt.Sprintf("roleupd-outbox:%s:%s:%s:%d", req.RoomID, req.Account, req.NewRole, req.Timestamp))
		if err := h.publish(ctx, outboxSubj, outboxData, dedupID); err != nil {
			return fmt.Errorf("publish outbox: %w", err)
		}
	}
	return nil
}

func (h *Handler) processRemoveMember(ctx context.Context, data []byte) error {
	var req model.RemoveMemberRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return fmt.Errorf("unmarshal RemoveMemberRequest: %w", err)
	}

	if req.OrgID != "" {
		return h.processRemoveOrg(ctx, &req)
	}
	return h.processRemoveIndividual(ctx, &req)
}

func (h *Handler) processRemoveIndividual(ctx context.Context, req *model.RemoveMemberRequest) error {
	isSelfLeave := req.Requester == req.Account

	user, err := h.store.GetUserWithMembership(ctx, req.RoomID, req.Account)
	if err != nil {
		return fmt.Errorf("get user with membership: %w", err)
	}

	// room_members.member.id stores the user's internal ID, not the account.
	if err := h.store.DeleteRoomMember(ctx, req.RoomID, model.RoomMemberIndividual, user.ID); err != nil {
		return fmt.Errorf("delete room member (individual): %w", err)
	}

	// Dual-membership: user stays via org source; strip owner role (org members can't be owners).
	if user.HasOrgMembership {
		if slices.Contains(user.Roles, model.RoleOwner) {
			if err := h.store.RemoveRole(ctx, req.Account, req.RoomID, model.RoleOwner); err != nil {
				return fmt.Errorf("demote dual-member owner: %w", err)
			}
		}
		return nil
	}

	// Individual-only: delete sub, reconcile userCount, publish leave/removed events.
	if _, err := h.store.DeleteSubscription(ctx, req.RoomID, req.Account); err != nil {
		return fmt.Errorf("delete subscription: %w", err)
	}

	if err := h.store.ReconcileUserCount(ctx, req.RoomID); err != nil {
		return fmt.Errorf("reconcile user count: %w", err)
	}

	now := time.Now().UTC()

	// Subscription update event
	subEvt := model.SubscriptionUpdateEvent{
		UserID: user.ID,
		Subscription: model.Subscription{
			RoomID: req.RoomID,
			User:   model.SubscriptionUser{ID: user.ID, Account: req.Account},
		},
		Action:    "removed",
		Timestamp: now.UnixMilli(),
	}
	subEvtData, _ := json.Marshal(subEvt)
	if err := h.publish(ctx, subject.SubscriptionUpdate(req.Account), subEvtData, ""); err != nil {
		slog.Error("subscription update publish failed", "error", err, "account", req.Account)
	}

	// Member change event
	evtType := "member_left"
	if !isSelfLeave {
		evtType = "member_removed"
	}
	memberEvt := model.MemberRemoveEvent{
		Type:      evtType,
		RoomID:    req.RoomID,
		Accounts:  []string{req.Account},
		SiteID:    h.siteID,
		Timestamp: now.UnixMilli(),
	}
	memberEvtData, _ := json.Marshal(memberEvt)
	if err := h.publish(ctx, subject.MemberEvent(req.RoomID), memberEvtData, ""); err != nil {
		slog.Error("member event publish failed", "error", err, "roomID", req.RoomID)
	}

	// System message
	sysMsgUser := model.SysMsgUser{
		Account:     user.Account,
		EngName:     user.EngName,
		ChineseName: user.ChineseName,
	}
	var sysMsgData []byte
	if isSelfLeave {
		sysMsgData, _ = json.Marshal(model.MemberLeft{User: sysMsgUser})
	} else {
		sysMsgData, _ = json.Marshal(model.MemberRemoved{User: &sysMsgUser, RemovedUsersCount: 1})
	}
	sysMsg := model.Message{
		ID:         idgen.DeriveID(fmt.Sprintf("rmindiv:%s:%s:%d", req.RoomID, req.Account, req.Timestamp)),
		RoomID:     req.RoomID,
		Type:       evtType,
		SysMsgData: sysMsgData,
		CreatedAt:  now,
	}
	msgEvt := model.MessageEvent{
		Message:   sysMsg,
		SiteID:    h.siteID,
		Timestamp: now.UnixMilli(),
	}
	msgEvtData, _ := json.Marshal(msgEvt)
	sysMsgDedupID := idgen.DeriveID(fmt.Sprintf("rmindiv-msg:%s:%s:%d", req.RoomID, req.Account, req.Timestamp))
	if err := h.publish(ctx, subject.MsgCanonicalCreated(h.siteID), msgEvtData, sysMsgDedupID); err != nil {
		return fmt.Errorf("publish individual removal system message: %w", err)
	}

	// Cross-site outbox for federated users
	if user.SiteID != h.siteID {
		outbox := model.OutboxEvent{
			Type:       "member_removed",
			SiteID:     h.siteID,
			DestSiteID: user.SiteID,
			Payload:    memberEvtData,
			Timestamp:  now.UnixMilli(),
		}
		outboxData, _ := json.Marshal(outbox)
		outboxDedupID := idgen.DeriveID(fmt.Sprintf("rmindiv-outbox:%s:%s:%s:%d", req.RoomID, req.Account, user.SiteID, req.Timestamp))
		if err := h.publish(ctx, subject.Outbox(h.siteID, user.SiteID, "member_removed"), outboxData, outboxDedupID); err != nil {
			return fmt.Errorf("outbox publish to %s: %w", user.SiteID, err)
		}
	}

	return nil
}

func (h *Handler) processRemoveOrg(ctx context.Context, req *model.RemoveMemberRequest) error {
	members, err := h.store.GetOrgMembersWithIndividualStatus(ctx, req.RoomID, req.OrgID)
	if err != nil {
		return fmt.Errorf("get org members with individual status: %w", err)
	}

	var toRemove []OrgMemberStatus
	for _, m := range members {
		if !m.HasIndividualMembership {
			toRemove = append(toRemove, m)
		}
	}

	accounts := make([]string, len(toRemove))
	for i, m := range toRemove {
		accounts[i] = m.Account
	}

	if len(accounts) > 0 {
		if _, err := h.store.DeleteSubscriptionsByAccounts(ctx, req.RoomID, accounts); err != nil {
			return fmt.Errorf("delete subscriptions by accounts: %w", err)
		}
	}

	if err := h.store.DeleteRoomMember(ctx, req.RoomID, model.RoomMemberOrg, req.OrgID); err != nil {
		return fmt.Errorf("delete room member (org): %w", err)
	}

	if err := h.store.ReconcileUserCount(ctx, req.RoomID); err != nil {
		return fmt.Errorf("reconcile user count: %w", err)
	}

	now := time.Now().UTC()

	// Publish per-account subscription update and collect cross-site accounts
	sectName := ""
	for _, m := range toRemove {
		if m.SectName != "" {
			sectName = m.SectName
		}
		subEvt := model.SubscriptionUpdateEvent{
			Subscription: model.Subscription{
				RoomID: req.RoomID,
				User:   model.SubscriptionUser{Account: m.Account},
			},
			Action:    "removed",
			Timestamp: now.UnixMilli(),
		}
		subEvtData, _ := json.Marshal(subEvt)
		if err := h.publish(ctx, subject.SubscriptionUpdate(m.Account), subEvtData, ""); err != nil {
			slog.Error("subscription update publish failed", "error", err, "account", m.Account)
		}
	}

	// Member change event with all removed accounts
	if len(accounts) > 0 {
		memberEvt := model.MemberRemoveEvent{
			Type:      "member_removed",
			RoomID:    req.RoomID,
			Accounts:  accounts,
			SiteID:    h.siteID,
			OrgID:     req.OrgID,
			Timestamp: now.UnixMilli(),
		}
		memberEvtData, _ := json.Marshal(memberEvt)
		if err := h.publish(ctx, subject.MemberEvent(req.RoomID), memberEvtData, ""); err != nil {
			slog.Error("member event publish failed", "error", err, "roomID", req.RoomID)
		}
	}

	// System message
	sysMsgPayload, _ := json.Marshal(model.MemberRemoved{
		OrgID:             req.OrgID,
		SectName:          sectName,
		RemovedUsersCount: len(toRemove),
	})
	sysMsg := model.Message{
		ID:         idgen.DeriveID(fmt.Sprintf("rmorg:%s:%s:%d", req.RoomID, req.OrgID, req.Timestamp)),
		RoomID:     req.RoomID,
		Type:       "member_removed",
		SysMsgData: sysMsgPayload,
		CreatedAt:  now,
	}
	msgEvt := model.MessageEvent{
		Message:   sysMsg,
		SiteID:    h.siteID,
		Timestamp: now.UnixMilli(),
	}
	msgEvtData, _ := json.Marshal(msgEvt)
	sysMsgDedupID := idgen.DeriveID(fmt.Sprintf("rmorg-msg:%s:%s:%d", req.RoomID, req.OrgID, req.Timestamp))
	if err := h.publish(ctx, subject.MsgCanonicalCreated(h.siteID), msgEvtData, sysMsgDedupID); err != nil {
		return fmt.Errorf("publish org removal system message: %w", err)
	}

	// Cross-site outbox grouped by destination site
	siteAccounts := make(map[string][]string)
	for _, m := range toRemove {
		if m.SiteID != h.siteID {
			siteAccounts[m.SiteID] = append(siteAccounts[m.SiteID], m.Account)
		}
	}
	for destSiteID, accounts := range siteAccounts {
		evt := model.MemberRemoveEvent{
			Type:      "member_removed",
			RoomID:    req.RoomID,
			Accounts:  accounts,
			SiteID:    h.siteID,
			OrgID:     req.OrgID,
			Timestamp: now.UnixMilli(),
		}
		outbox := model.OutboxEvent{
			Type:       "member_removed",
			SiteID:     h.siteID,
			DestSiteID: destSiteID,
			Payload:    mustMarshal(evt),
			Timestamp:  now.UnixMilli(),
		}
		outboxData, _ := json.Marshal(outbox)
		outboxDedupID := idgen.DeriveID(fmt.Sprintf("rmorg-outbox:%s:%s:%s:%d", req.RoomID, req.OrgID, destSiteID, req.Timestamp))
		if err := h.publish(ctx, subject.Outbox(h.siteID, destSiteID, "member_removed"), outboxData, outboxDedupID); err != nil {
			return fmt.Errorf("outbox publish to %s: %w", destSiteID, err)
		}
	}

	return nil
}

func (h *Handler) processAddMembers(ctx context.Context, data []byte) error {
	var req model.AddMembersRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return fmt.Errorf("unmarshal add members request: %w", err)
	}

	room, err := h.store.GetRoom(ctx, req.RoomID)
	if err != nil {
		return fmt.Errorf("get room: %w", err)
	}

	// Expand org IDs + direct accounts to actual account list, excluding already-subscribed
	accounts, err := h.store.ListNewMembers(ctx, req.Orgs, req.Users, req.RoomID)
	if err != nil {
		return fmt.Errorf("list new members: %w", err)
	}
	if len(accounts) == 0 {
		return nil
	}

	users, err := h.store.FindUsersByAccounts(ctx, accounts)
	if err != nil {
		return fmt.Errorf("find users by accounts: %w", err)
	}
	userMap := make(map[string]model.User, len(users))
	for i := range users {
		userMap[users[i].Account] = users[i]
	}

	// acceptedAt is the stable request-acceptance time (set by room-service).
	// It's used for every domain-level timestamp that must survive event replay
	// (subscription.JoinedAt, historySharedSince, room_members.Ts, system
	// message CreatedAt, MemberAddEvent.JoinedAt) so that reprocessing yields
	// the same values. `now` below is the event-emission time and is only used
	// for transient event metadata (Timestamp fields on outbound events).
	acceptedAt := time.UnixMilli(req.Timestamp).UTC()
	now := time.Now().UTC()

	// Build subscriptions and collect the resolved accounts in a single pass
	// so we don't re-iterate `subs` later to build an account set or an
	// actualAccounts slice.
	subs := make([]*model.Subscription, 0, len(accounts))
	actualAccounts := make([]string, 0, len(accounts))
	resolvedAccountSet := make(map[string]struct{}, len(accounts))
	for _, account := range accounts {
		user, ok := userMap[account]
		if !ok {
			slog.Warn("user not found for account", "account", account)
			continue
		}
		sub := &model.Subscription{
			ID:       idgen.GenerateID(),
			User:     model.SubscriptionUser{ID: user.ID, Account: user.Account},
			RoomID:   req.RoomID,
			SiteID:   room.SiteID,
			Roles:    []model.Role{model.RoleMember},
			JoinedAt: acceptedAt,
		}
		if req.History.Mode == model.HistoryModeNone {
			histTime := acceptedAt
			sub.HistorySharedSince = &histTime
		}
		subs = append(subs, sub)
		actualAccounts = append(actualAccounts, user.Account)
		resolvedAccountSet[user.Account] = struct{}{}
	}

	if err := h.store.BulkCreateSubscriptions(ctx, subs); err != nil {
		return fmt.Errorf("bulk create subscriptions: %w", err)
	}

	writeIndividuals := len(req.Orgs) > 0
	if !writeIndividuals {
		hasOrgs, err := h.store.HasOrgRoomMembers(ctx, req.RoomID)
		if err != nil {
			slog.Warn("check existing org room members failed", "error", err, "roomID", req.RoomID)
		}
		writeIndividuals = hasOrgs
	}

	// Collect all room_member docs to write in a single bulk insert:
	// new individuals + new orgs + (optional) backfill of existing subscribers.
	roomMembers := make([]*model.RoomMember, 0, len(subs)+len(req.Orgs))
	if writeIndividuals {
		for _, sub := range subs {
			roomMembers = append(roomMembers, &model.RoomMember{
				ID:     idgen.GenerateID(),
				RoomID: req.RoomID,
				Ts:     acceptedAt,
				Member: model.RoomMemberEntry{
					ID:      sub.User.ID,
					Type:    model.RoomMemberIndividual,
					Account: sub.User.Account,
				},
			})
		}
	}
	for _, org := range req.Orgs {
		roomMembers = append(roomMembers, &model.RoomMember{
			ID:     idgen.GenerateID(),
			RoomID: req.RoomID,
			Ts:     acceptedAt,
			Member: model.RoomMemberEntry{
				ID:   org,
				Type: model.RoomMemberOrg,
			},
		})
	}

	// Backfill existing subscribers into room_members only when orgs are
	// joining for the first time and we're starting to track individuals.
	if writeIndividuals && len(req.Orgs) > 0 {
		existingAccounts, err := h.store.GetSubscriptionAccounts(ctx, req.RoomID)
		if err != nil {
			slog.Warn("get subscription accounts for backfill failed", "error", err)
		} else {
			var backfillAccounts []string
			for _, account := range existingAccounts {
				if _, isNew := resolvedAccountSet[account]; !isNew {
					backfillAccounts = append(backfillAccounts, account)
				}
			}
			if len(backfillAccounts) > 0 {
				backfillUsers, err := h.store.FindUsersByAccounts(ctx, backfillAccounts)
				if err != nil {
					slog.Warn("find users for backfill failed", "error", err)
				} else {
					for i := range backfillUsers {
						roomMembers = append(roomMembers, &model.RoomMember{
							ID:     idgen.GenerateID(),
							RoomID: req.RoomID,
							Ts:     acceptedAt,
							Member: model.RoomMemberEntry{
								ID:      backfillUsers[i].ID,
								Type:    model.RoomMemberIndividual,
								Account: backfillUsers[i].Account,
							},
						})
					}
				}
			}
		}
	}

	if len(roomMembers) > 0 {
		if err := h.store.BulkCreateRoomMembers(ctx, roomMembers); err != nil {
			return fmt.Errorf("bulk create room members (room %s): %w", req.RoomID, err)
		}
	}

	// 6. Reconcile userCount. Idempotent $set converges to the correct value
	// even under JetStream redelivery; an upstream log-and-continue would
	// leave the counter drifted forever, so we propagate the error.
	if err := h.store.ReconcileUserCount(ctx, req.RoomID); err != nil {
		return fmt.Errorf("reconcile user count: %w", err)
	}

	for _, sub := range subs {
		subEvt := model.SubscriptionUpdateEvent{
			UserID:       sub.User.ID,
			Subscription: *sub,
			Action:       "added",
			Timestamp:    now.UnixMilli(),
		}
		subEvtData, _ := json.Marshal(subEvt)
		if err := h.publish(ctx, subject.SubscriptionUpdate(sub.User.Account), subEvtData, ""); err != nil {
			slog.Error("subscription update publish failed", "error", err, "account", sub.User.Account)
		}
	}

	// 8. Publish MemberAddEvent (actualAccounts was built above alongside subs)
	// Never emit &0 — the painless sentinel `hss <= 0` would misroute the
	// restricted room into `rooms[]`. If the upstream request is malformed
	// (restricted mode but missing timestamp), leave the pointer nil + log —
	// we can't silently translate to &0.
	var historySharedSincePtr *int64
	if req.History.Mode == model.HistoryModeNone {
		if req.Timestamp > 0 {
			v := req.Timestamp
			historySharedSincePtr = &v
		} else {
			slog.Error("restricted history with missing timestamp, emitting nil",
				"roomID", req.RoomID, "mode", req.History.Mode)
		}
	}
	memberAddEvt := model.MemberAddEvent{
		Type:               "member_added",
		RoomID:             req.RoomID,
		Accounts:           actualAccounts,
		SiteID:             room.SiteID,
		JoinedAt:           req.Timestamp,
		HistorySharedSince: historySharedSincePtr,
		Timestamp:          now.UnixMilli(),
	}
	memberAddData, _ := json.Marshal(memberAddEvt)
	if err := h.publish(ctx, subject.RoomMemberEvent(req.RoomID), memberAddData, ""); err != nil {
		slog.Error("member add event publish failed", "error", err, "roomID", req.RoomID)
	}

	membersAdded := model.MembersAdded{
		Individuals:     actualAccounts,
		Orgs:            req.Orgs,
		Channels:        req.Channels,
		AddedUsersCount: len(subs),
	}
	sysMsgData, _ := json.Marshal(membersAdded)
	// Include requester + sorted accounts/orgs so same-ms requests don't collide.
	seedAccounts := slices.Clone(actualAccounts)
	slices.Sort(seedAccounts)
	seedOrgs := slices.Clone(req.Orgs)
	slices.Sort(seedOrgs)
	addMembersSeed := fmt.Sprintf(
		"addmembers:%s:%s:%d:%s:%s",
		req.RoomID,
		req.RequesterAccount,
		req.Timestamp,
		strings.Join(seedAccounts, ","),
		strings.Join(seedOrgs, ","),
	)
	sysMsg := model.Message{
		ID:          idgen.DeriveID(addMembersSeed),
		RoomID:      req.RoomID,
		UserID:      req.RequesterID,
		UserAccount: req.RequesterAccount,
		Type:        "members_added",
		SysMsgData:  sysMsgData,
		CreatedAt:   acceptedAt,
	}
	msgEvt := model.MessageEvent{
		Event:     model.EventCreated,
		Message:   sysMsg,
		SiteID:    room.SiteID,
		Timestamp: now.UnixMilli(),
	}
	msgEvtData, _ := json.Marshal(msgEvt)
	addMsgDedupID := idgen.DeriveID("addmembers-msg:" + addMembersSeed)
	if err := h.publish(ctx, subject.MsgCanonicalCreated(room.SiteID), msgEvtData, addMsgDedupID); err != nil {
		return fmt.Errorf("publish add-members system message: %w", err)
	}

	// 10. Outbox for cross-site members — batched by destination site
	remoteSiteMembers := make(map[string][]string)
	for _, sub := range subs {
		user, ok := userMap[sub.User.Account]
		if !ok || user.SiteID == room.SiteID {
			continue
		}
		remoteSiteMembers[user.SiteID] = append(remoteSiteMembers[user.SiteID], sub.User.Account)
	}
	for destSiteID, accounts := range remoteSiteMembers {
		siteEvt := model.MemberAddEvent{
			Type:               "member_added",
			RoomID:             req.RoomID,
			Accounts:           accounts,
			SiteID:             room.SiteID,
			JoinedAt:           req.Timestamp,
			HistorySharedSince: historySharedSincePtr,
			Timestamp:          now.UnixMilli(),
		}
		siteEvtData, _ := json.Marshal(siteEvt)
		outbox := model.OutboxEvent{
			Type: "member_added", SiteID: room.SiteID, DestSiteID: destSiteID,
			Payload: siteEvtData, Timestamp: now.UnixMilli(),
		}
		outboxData, _ := json.Marshal(outbox)
		siteSeedAccounts := slices.Clone(accounts)
		slices.Sort(siteSeedAccounts)
		addOutboxDedupID := idgen.DeriveID(fmt.Sprintf(
			"addmembers-outbox:%s:%s:%d:%s",
			req.RoomID,
			destSiteID,
			req.Timestamp,
			strings.Join(siteSeedAccounts, ","),
		))
		if err := h.publish(ctx, subject.Outbox(room.SiteID, destSiteID, "member_added"), outboxData, addOutboxDedupID); err != nil {
			return fmt.Errorf("outbox publish to %s failed: %w", destSiteID, err)
		}
	}

	return nil
}

func mustMarshal(v any) []byte {
	data, _ := json.Marshal(v)
	return data
}
