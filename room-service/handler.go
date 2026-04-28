package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/Marz32onE/instrumentation-go/otel-nats/otelnats"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/roomkeystore"
	"github.com/hmchangw/chat/pkg/subject"
)

type Handler struct {
	store            RoomStore
	keyStore         RoomKeyStore
	memberListClient MemberListClient
	siteID           string
	maxRoomSize      int
	maxBatchSize     int
	publishToStream  func(ctx context.Context, subj string, data []byte) error
}

func NewHandler(store RoomStore, keyStore RoomKeyStore, memberListClient MemberListClient, siteID string, maxRoomSize, maxBatchSize int, publishToStream func(context.Context, string, []byte) error) *Handler {
	return &Handler{store: store, keyStore: keyStore, memberListClient: memberListClient, siteID: siteID, maxRoomSize: maxRoomSize, maxBatchSize: maxBatchSize, publishToStream: publishToStream}
}

// RegisterCRUD registers NATS request/reply handlers for room CRUD with queue group.
func (h *Handler) RegisterCRUD(nc *otelnats.Conn) error {
	const queue = "room-service"
	if _, err := nc.QueueSubscribe(subject.RoomsCreateWildcard(), queue, h.natsCreateRoom); err != nil {
		return err
	}
	if _, err := nc.QueueSubscribe(subject.RoomsListWildcard(), queue, h.natsListRooms); err != nil {
		return err
	}
	if _, err := nc.QueueSubscribe(subject.RoomsGetWildcard(), queue, h.natsGetRoom); err != nil {
		return err
	}
	if _, err := nc.QueueSubscribe(subject.RoomsInfoBatchSubscribe(h.siteID), queue, h.natsRoomsInfoBatch); err != nil {
		return err
	}
	if _, err := nc.QueueSubscribe(subject.MemberRoleUpdateWildcard(h.siteID), queue, h.natsUpdateRole); err != nil {
		return fmt.Errorf("subscribe member role update: %w", err)
	}
	if _, err := nc.QueueSubscribe(subject.MemberRemoveWildcard(h.siteID), queue, h.NatsHandleRemoveMember); err != nil {
		return fmt.Errorf("subscribe member remove: %w", err)
	}
	if _, err := nc.QueueSubscribe(subject.MemberAddWildcard(h.siteID), queue, h.natsAddMembers); err != nil {
		return fmt.Errorf("subscribe member add: %w", err)
	}
	if _, err := nc.QueueSubscribe(subject.MemberListWildcard(h.siteID), queue, h.natsListMembers); err != nil {
		return fmt.Errorf("subscribe member list: %w", err)
	}
	if _, err := nc.QueueSubscribe(subject.OrgMembersWildcard(), queue, h.natsListOrgMembers); err != nil {
		return fmt.Errorf("subscribe org members: %w", err)
	}
	return nil
}

func (h *Handler) natsCreateRoom(m otelnats.Msg) {
	resp, err := h.handleCreateRoom(m.Context(), m.Msg.Data)
	if err != nil {
		natsutil.ReplyError(m.Msg, err.Error())
		return
	}
	if err := m.Msg.Respond(resp); err != nil {
		slog.Error("failed to respond to message", "error", err)
	}
}

func (h *Handler) natsListRooms(m otelnats.Msg) {
	rooms, err := h.store.ListRooms(m.Context())
	if err != nil {
		natsutil.ReplyError(m.Msg, err.Error())
		return
	}
	natsutil.ReplyJSON(m.Msg, model.ListRoomsResponse{Rooms: rooms})
}

func (h *Handler) natsGetRoom(m otelnats.Msg) {
	parts := strings.Split(m.Msg.Subject, ".")
	roomID := parts[len(parts)-1]
	room, err := h.store.GetRoom(m.Context(), roomID)
	if err != nil {
		natsutil.ReplyError(m.Msg, err.Error())
		return
	}
	natsutil.ReplyJSON(m.Msg, room)
}

func (h *Handler) handleCreateRoom(ctx context.Context, data []byte) ([]byte, error) {
	var req model.CreateRoomRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	now := time.Now().UTC()
	room := model.Room{
		ID:        idgen.GenerateID(),
		Name:      req.Name,
		Type:      req.Type,
		CreatedBy: req.CreatedBy,
		SiteID:    req.SiteID,
		UserCount: 1,
		CreatedAt: now,
		UpdatedAt: now,
	}

	if err := h.store.CreateRoom(ctx, &room); err != nil {
		return nil, fmt.Errorf("create room: %w", err)
	}

	// Auto-create owner subscription
	sub := model.Subscription{
		ID:                 idgen.GenerateID(),
		User:               model.SubscriptionUser{ID: req.CreatedBy, Account: req.CreatedByAccount},
		RoomID:             room.ID,
		SiteID:             req.SiteID,
		Roles:              []model.Role{model.RoleOwner},
		HistorySharedSince: &now,
		JoinedAt:           now,
	}
	if err := h.store.CreateSubscription(ctx, &sub); err != nil {
		slog.Warn("create owner subscription failed", "error", err)
	}

	return json.Marshal(room)
}

// NatsHandleRemoveMember handles remove-member authorization requests.
func (h *Handler) NatsHandleRemoveMember(m otelnats.Msg) {
	resp, err := h.handleRemoveMember(m.Context(), m.Msg.Subject, m.Msg.Data)
	if err != nil {
		slog.Error("remove member failed", "error", err)
		natsutil.ReplyError(m.Msg, sanitizeError(err))
		return
	}
	if err := m.Msg.Respond(resp); err != nil {
		slog.Error("failed to respond to message", "error", err)
	}
}

func (h *Handler) natsListMembers(m otelnats.Msg) {
	resp, err := h.handleListMembers(m.Context(), m.Msg.Subject, m.Msg.Data)
	if err != nil {
		slog.Error("list members failed", "error", err)
		natsutil.ReplyError(m.Msg, sanitizeError(err))
		return
	}
	natsutil.ReplyJSON(m.Msg, resp)
}

func (h *Handler) natsListOrgMembers(m otelnats.Msg) {
	resp, err := h.handleListOrgMembers(m.Context(), m.Msg.Subject)
	if err != nil {
		slog.Error("list org members failed", "error", err)
		natsutil.ReplyError(m.Msg, sanitizeError(err))
		return
	}
	natsutil.ReplyJSON(m.Msg, resp)
}

func (h *Handler) handleListOrgMembers(ctx context.Context, subj string) (model.ListOrgMembersResponse, error) {
	orgID, ok := subject.ParseOrgMembersSubject(subj)
	if !ok {
		return model.ListOrgMembersResponse{}, fmt.Errorf("invalid org-members subject")
	}
	members, err := h.store.ListOrgMembers(ctx, orgID)
	if err != nil {
		if errors.Is(err, errInvalidOrg) {
			return model.ListOrgMembersResponse{}, errInvalidOrg
		}
		return model.ListOrgMembersResponse{}, fmt.Errorf("get org members: %w", err)
	}
	return model.ListOrgMembersResponse{Members: members}, nil
}

func (h *Handler) handleListMembers(ctx context.Context, subj string, data []byte) (model.ListRoomMembersResponse, error) {
	requesterAccount, roomID, ok := subject.ParseUserRoomSubject(subj)
	if !ok {
		return model.ListRoomMembersResponse{}, fmt.Errorf("invalid list-members subject")
	}

	_, err := h.store.GetSubscription(ctx, requesterAccount, roomID)
	switch {
	case errors.Is(err, model.ErrSubscriptionNotFound):
		return model.ListRoomMembersResponse{}, errNotRoomMember
	case err != nil:
		return model.ListRoomMembersResponse{}, fmt.Errorf("check room membership: %w", err)
	}

	var req model.ListRoomMembersRequest
	if len(data) > 0 {
		if err := json.Unmarshal(data, &req); err != nil {
			return model.ListRoomMembersResponse{}, fmt.Errorf("invalid request: %w", err)
		}
	}
	if req.Limit != nil && *req.Limit <= 0 {
		return model.ListRoomMembersResponse{}, fmt.Errorf("limit must be > 0")
	}
	if req.Offset != nil && *req.Offset < 0 {
		return model.ListRoomMembersResponse{}, fmt.Errorf("offset must be >= 0")
	}

	members, err := h.store.ListRoomMembers(ctx, roomID, req.Limit, req.Offset, req.Enrich)
	if err != nil {
		return model.ListRoomMembersResponse{}, fmt.Errorf("get room members: %w", err)
	}
	return model.ListRoomMembersResponse{Members: members}, nil
}

func (h *Handler) handleRemoveMember(ctx context.Context, subj string, data []byte) ([]byte, error) {
	requesterAccount, roomID, ok := subject.ParseUserRoomSubject(subj)
	if !ok {
		return nil, fmt.Errorf("invalid remove-member subject: %s", subj)
	}

	var req model.RemoveMemberRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	if req.RoomID != "" && req.RoomID != roomID {
		return nil, fmt.Errorf("room ID mismatch")
	}
	req.RoomID = roomID
	req.Requester = requesterAccount

	// Exactly one of Account or OrgID must be set.
	if (req.Account == "") == (req.OrgID == "") {
		return nil, fmt.Errorf("exactly one of account or orgId must be set")
	}

	if req.Account != "" {
		// Individual removal: cheapest-first validation (target → requester → counts).
		target, err := h.store.GetSubscriptionWithMembership(ctx, roomID, req.Account)
		if err != nil {
			return nil, fmt.Errorf("get target subscription: %w", err)
		}
		if target.HasOrgMembership && !target.HasIndividualMembership {
			return nil, fmt.Errorf("org members cannot leave individually")
		}
		if req.Account != requesterAccount {
			requesterSub, err := h.store.GetSubscription(ctx, requesterAccount, roomID)
			if err != nil {
				return nil, fmt.Errorf("get requester subscription: %w", err)
			}
			if !hasRole(requesterSub.Roles, model.RoleOwner) {
				return nil, fmt.Errorf("only owners can remove members")
			}
		}
		counts, err := h.store.CountMembersAndOwners(ctx, roomID)
		if err != nil {
			return nil, fmt.Errorf("count members: %w", err)
		}
		if counts.MemberCount <= 1 {
			return nil, fmt.Errorf("cannot remove the last member of the room")
		}
		if hasRole(target.Subscription.Roles, model.RoleOwner) && counts.OwnerCount <= 1 {
			return nil, fmt.Errorf("last owner cannot leave the room")
		}
	} else {
		// Owner-removes-org: only the requester's owner role matters here; org members resolved downstream.
		sub, err := h.store.GetSubscription(ctx, requesterAccount, roomID)
		if err != nil {
			return nil, fmt.Errorf("get requester subscription: %w", err)
		}
		if !hasRole(sub.Roles, model.RoleOwner) {
			return nil, fmt.Errorf("only owners can remove members")
		}
	}

	// Stable seed for room-worker's deterministic system-message IDs across JetStream redeliveries.
	req.Timestamp = time.Now().UTC().UnixMilli()

	// Publish to ROOMS stream for room-worker processing.
	var err error
	data, err = json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal remove member request: %w", err)
	}
	if err := h.publishToStream(ctx, subject.RoomCanonical(h.siteID, "member.remove"), data); err != nil {
		return nil, fmt.Errorf("publish to stream: %w", err)
	}

	return json.Marshal(map[string]string{"status": "accepted"})
}

func (h *Handler) natsUpdateRole(m otelnats.Msg) {
	resp, err := h.handleUpdateRole(m.Context(), m.Msg.Subject, m.Msg.Data)
	if err != nil {
		slog.Error("update role failed", "error", err)
		natsutil.ReplyError(m.Msg, sanitizeError(err))
		return
	}
	if err := m.Msg.Respond(resp); err != nil {
		slog.Error("failed to respond to update-role message", "error", err)
	}
}

func (h *Handler) handleUpdateRole(ctx context.Context, subj string, data []byte) ([]byte, error) {
	requester, roomID, ok := subject.ParseUserRoomSubject(subj)
	if !ok {
		return nil, fmt.Errorf("invalid subject: %s", subj)
	}
	var req model.UpdateRoleRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}
	if req.RoomID != "" && req.RoomID != roomID {
		return nil, fmt.Errorf("invalid request: room ID mismatch")
	}
	req.RoomID = roomID
	if req.NewRole != model.RoleOwner && req.NewRole != model.RoleMember {
		return nil, errInvalidRole
	}
	room, err := h.store.GetRoom(ctx, roomID)
	if err != nil {
		return nil, fmt.Errorf("get room: %w", err)
	}
	if room.Type != model.RoomTypeChannel {
		return nil, errRoomTypeGuard
	}
	requesterSub, err := h.store.GetSubscription(ctx, requester, roomID)
	if err != nil {
		return nil, fmt.Errorf("requester not found: %w", err)
	}
	if !hasRole(requesterSub.Roles, model.RoleOwner) {
		return nil, errOnlyOwners
	}
	// Covers both role check and membership-source guard; missing sub → errTargetNotMember.
	target, err := h.store.GetSubscriptionWithMembership(ctx, roomID, req.Account)
	if err != nil {
		if errors.Is(err, model.ErrSubscriptionNotFound) || errors.Is(err, mongo.ErrNoDocuments) {
			return nil, errTargetNotMember
		}
		return nil, fmt.Errorf("get target subscription: %w", err)
	}
	// Promote: target must not already be owner. Demote: target must be owner.
	if req.NewRole == model.RoleOwner && hasRole(target.Subscription.Roles, model.RoleOwner) {
		return nil, errAlreadyOwner
	}
	if req.NewRole == model.RoleMember && !hasRole(target.Subscription.Roles, model.RoleOwner) {
		return nil, errNotOwner
	}
	// Reject only provably org-only members; subscription-only members (both flags false) are promotable.
	if req.NewRole == model.RoleOwner && target.HasOrgMembership && !target.HasIndividualMembership {
		return nil, errPromoteRequiresIndividual
	}
	// Last-owner guard only needed on self-demotion; rule #5 ensures requester is an owner.
	if req.NewRole == model.RoleMember && req.Account == requester {
		count, err := h.store.CountOwners(ctx, roomID)
		if err != nil {
			return nil, fmt.Errorf("count owners: %w", err)
		}
		if count <= 1 {
			return nil, errCannotDemoteLast
		}
	}
	// Stable acceptance time → stable Nats-Msg-Id across redeliveries.
	req.Timestamp = time.Now().UTC().UnixMilli()
	data, err = json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal role update request: %w", err)
	}
	if err := h.publishToStream(ctx, subject.RoomCanonical(h.siteID, "member.role-update"), data); err != nil {
		return nil, fmt.Errorf("publish to stream: %w", err)
	}
	return json.Marshal(map[string]string{"status": "accepted"})
}

func (h *Handler) natsAddMembers(m otelnats.Msg) {
	resp, err := h.handleAddMembers(m.Context(), m.Msg.Subject, m.Msg.Data)
	if err != nil {
		slog.Error("add-members failed", "error", err)
		natsutil.ReplyError(m.Msg, sanitizeError(err))
		return
	}
	if err := m.Msg.Respond(resp); err != nil {
		slog.Error("failed to respond to add-members", "error", err)
	}
}

func (h *Handler) handleAddMembers(ctx context.Context, subj string, data []byte) ([]byte, error) {
	// 1. Parse subject → requester, roomID
	requester, roomID, ok := subject.ParseUserRoomSubject(subj)
	if !ok {
		return nil, fmt.Errorf("invalid add-members subject: %s", subj)
	}

	// 2. Verify requester is in room
	sub, err := h.store.GetSubscription(ctx, requester, roomID)
	if err != nil {
		return nil, fmt.Errorf("requester not in room: %w", err)
	}

	// 3. Get room and guard on type
	room, err := h.store.GetRoom(ctx, roomID)
	if err != nil {
		return nil, fmt.Errorf("get room: %w", err)
	}
	if room.Type != model.RoomTypeChannel {
		return nil, fmt.Errorf("cannot add members to a non-channel room")
	}
	if room.Restricted && !hasRole(sub.Roles, model.RoleOwner) {
		return nil, fmt.Errorf("only owners can add members to a restricted room")
	}

	// 4. Unmarshal request
	var req model.AddMembersRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}
	if req.RoomID != "" && req.RoomID != roomID {
		return nil, fmt.Errorf("invalid request: room ID mismatch")
	}

	// 5. Expand channels
	channelOrgIDs, channelAccounts, err := h.expandChannelRefs(ctx, requester, req.Channels)
	if err != nil {
		return nil, fmt.Errorf("expand channels: %w", err)
	}

	// 6. Dedup orgs and direct accounts
	allOrgs := dedup(append(req.Orgs, channelOrgIDs...))
	allUsers := dedup(append(req.Users, channelAccounts...))

	// 7. Count net-new members (count-only — actual list materialized in room-worker)
	newCount, err := h.store.CountNewMembers(ctx, allOrgs, allUsers, roomID)
	if err != nil {
		return nil, fmt.Errorf("count new members: %w", err)
	}

	// 8. Capacity check — use room.UserCount (kept current by room-worker's
	// ReconcileUserCount after each membership change) instead of issuing a
	// separate CountSubscriptions query.
	if room.UserCount+newCount > h.maxRoomSize {
		return nil, fmt.Errorf("room is at maximum capacity (%d): cannot add %d members to room with %d existing", h.maxRoomSize, newCount, room.UserCount)
	}

	// 9. Normalize and publish — Users and Orgs ship as merged-but-unresolved.
	// room-worker's ListNewMembers reproduces resolution at write time.
	req.Users = allUsers
	req.Orgs = allOrgs
	req.RoomID = roomID
	req.RequesterID = sub.User.ID
	req.RequesterAccount = sub.User.Account
	req.Timestamp = time.Now().UTC().UnixMilli()
	normalized, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal add-members request: %w", err)
	}
	if err := h.publishToStream(ctx, subject.RoomCanonical(h.siteID, "member.add"), normalized); err != nil {
		return nil, fmt.Errorf("publish to stream: %w", err)
	}

	// 10. Reply accepted
	return json.Marshal(map[string]string{"status": "accepted"})
}

func (h *Handler) expandChannelRefs(ctx context.Context, requester string, refs []model.ChannelRef) (orgIDs, accounts []string, err error) {
	// maxRoomSize+1 is enough to distinguish "fits" from "exceeds the cap" without
	// ever materializing an unbounded result set in memory.
	listLimit := h.maxRoomSize + 1
	for _, ref := range refs {
		var members []model.RoomMember

		if ref.SiteID == h.siteID {
			if _, subErr := h.store.GetSubscription(ctx, requester, ref.RoomID); subErr != nil {
				if errors.Is(subErr, model.ErrSubscriptionNotFound) {
					return nil, nil, errNotRoomMember
				}
				return nil, nil, fmt.Errorf("subscription check %s: %w", ref.RoomID, subErr)
			}
			members, err = h.store.ListRoomMembers(ctx, ref.RoomID, &listLimit, nil, false)
			if err != nil {
				return nil, nil, fmt.Errorf("local list-members %s: %w", ref.RoomID, err)
			}
		} else {
			members, err = h.memberListClient.ListMembers(ctx, requester, ref, listLimit)
			if err != nil {
				// Pass the sentinel through unwrapped so same-site and cross-site "not a member"
				// produce identical behavior — errors.Is(err, errNotRoomMember) matches both.
				if errors.Is(err, errNotRoomMember) {
					return nil, nil, errNotRoomMember
				}
				return nil, nil, fmt.Errorf("remote list-members %s@%s: %w", ref.RoomID, ref.SiteID, err)
			}
		}
		// Apply the size cap uniformly to both same-site and cross-site results.
		// The listLimit above caps the response at maxRoomSize+1 so we never
		// load more than that into memory; if we hit the cap, the source room
		// is too large and the downstream capacity check would reject anyway.
		if len(members) > h.maxRoomSize {
			return nil, nil, fmt.Errorf("list-members %s@%s: response size %d exceeds max %d", ref.RoomID, ref.SiteID, len(members), h.maxRoomSize)
		}

		for i := range members {
			m := &members[i].Member
			switch m.Type {
			case model.RoomMemberOrg:
				orgIDs = append(orgIDs, m.ID)
			case model.RoomMemberIndividual:
				accounts = append(accounts, m.Account)
			default:
				// Schema skew between sites — log so the issue is visible without
				// breaking the request. Same-site (m.Type from our own Mongo) shouldn't
				// hit this in practice; cross-site can if a peer adds new types.
				slog.Warn("expandChannelRefs: skipping member with unknown type",
					"roomId", ref.RoomID,
					"siteId", ref.SiteID,
					"memberType", m.Type,
					"memberId", m.ID,
				)
			}
		}
	}
	return orgIDs, accounts, nil
}

func (h *Handler) natsRoomsInfoBatch(m otelnats.Msg) {
	resp, err := h.handleRoomsInfoBatch(m.Context(), m.Msg.Data)
	if err != nil {
		slog.Error("rooms info batch failed", "error", err)
		natsutil.ReplyError(m.Msg, sanitizeError(err))
		return
	}
	if err := m.Msg.Respond(resp); err != nil {
		slog.Error("failed to respond to message", "error", err)
	}
}

func (h *Handler) handleRoomsInfoBatch(ctx context.Context, data []byte) ([]byte, error) {
	start := time.Now()
	var req model.RoomsInfoBatchRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}
	if len(req.RoomIDs) == 0 {
		return nil, fmt.Errorf("roomIds must not be empty")
	}
	if len(req.RoomIDs) > h.maxBatchSize {
		return nil, fmt.Errorf("batch size %d exceeds limit %d", len(req.RoomIDs), h.maxBatchSize)
	}

	if span := trace.SpanFromContext(ctx); span.IsRecording() {
		span.SetAttributes(
			attribute.Int("batch_size", len(req.RoomIDs)),
			attribute.String("site_id", h.siteID),
		)
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var (
		rooms []model.Room
		keys  map[string]*roomkeystore.VersionedKeyPair
	)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		r, err := h.store.ListRoomsByIDs(gctx, req.RoomIDs)
		if err != nil {
			return fmt.Errorf("list rooms by ids: %w", err)
		}
		rooms = r
		return nil
	})
	g.Go(func() error {
		k, err := chunkedGetKeys(gctx, h.keyStore, req.RoomIDs)
		if err != nil {
			return fmt.Errorf("get room keys: %w", err)
		}
		keys = k
		return nil
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}

	infos, foundCount, keyedCount := h.aggregateRoomInfo(req.RoomIDs, rooms, keys)

	slog.Debug("rooms info batch handled",
		"site_id", h.siteID,
		"batch_size", len(req.RoomIDs),
		"found_count", foundCount,
		"keyed_count", keyedCount,
		"latency_ms", time.Since(start).Milliseconds(),
	)

	return json.Marshal(model.RoomsInfoBatchResponse{Rooms: infos})
}

func (h *Handler) aggregateRoomInfo(ids []string, rooms []model.Room, keys map[string]*roomkeystore.VersionedKeyPair) ([]model.RoomInfo, int, int) {
	byID := make(map[string]*model.Room, len(rooms))
	for i := range rooms {
		byID[rooms[i].ID] = &rooms[i]
	}
	out := make([]model.RoomInfo, len(ids))
	var foundCount, keyedCount int
	for i, id := range ids {
		entry := model.RoomInfo{RoomID: id}
		r, ok := byID[id]
		if !ok {
			out[i] = entry
			continue
		}
		entry.Found = true
		foundCount++
		entry.SiteID = r.SiteID
		entry.Name = r.Name
		if r.LastMsgAt != nil && !r.LastMsgAt.IsZero() {
			ms := r.LastMsgAt.UTC().UnixMilli()
			entry.LastMsgAt = &ms
		}
		if r.LastMentionAllAt != nil && !r.LastMentionAllAt.IsZero() {
			ms := r.LastMentionAllAt.UTC().UnixMilli()
			entry.LastMentionAllAt = &ms
		}
		if kp, ok := keys[id]; ok && kp != nil {
			enc := base64.StdEncoding.EncodeToString(kp.KeyPair.PrivateKey)
			ver := kp.Version
			entry.PrivateKey = &enc
			entry.KeyVersion = &ver
			keyedCount++
		}
		out[i] = entry
	}
	return out, foundCount, keyedCount
}

const queryChunkSize = 500

func chunkedGetKeys(ctx context.Context, ks RoomKeyStore, ids []string) (map[string]*roomkeystore.VersionedKeyPair, error) {
	if len(ids) <= queryChunkSize {
		return ks.GetMany(ctx, ids)
	}
	merged := make(map[string]*roomkeystore.VersionedKeyPair, len(ids))
	for i := 0; i < len(ids); i += queryChunkSize {
		end := i + queryChunkSize
		if end > len(ids) {
			end = len(ids)
		}
		chunk, err := ks.GetMany(ctx, ids[i:end])
		if err != nil {
			return nil, err
		}
		for k, v := range chunk {
			merged[k] = v
		}
	}
	return merged, nil
}
