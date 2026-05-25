package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Marz32onE/instrumentation-go/otel-nats/otelnats"
	"github.com/nats-io/nats.go"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/roomkeymetrics"
	"github.com/hmchangw/chat/pkg/roomkeystore"
	"github.com/hmchangw/chat/pkg/subject"
)

type Handler struct {
	store RoomStore
	// keyStore is set when VALKEY_ADDRS is configured (always in production; tests may pass nil).
	keyStore          RoomKeyStore
	memberListClient  MemberListClient
	msgReader         MessageReader
	siteID            string
	maxRoomSize       int
	maxBatchSize      int
	memberListTimeout time.Duration
	publishToStream   func(ctx context.Context, subj string, data []byte) error
	publishCore       func(ctx context.Context, subj string, data []byte) error
}

func NewHandler(store RoomStore, keyStore RoomKeyStore, memberListClient MemberListClient, msgReader MessageReader, siteID string, maxRoomSize, maxBatchSize int, memberListTimeout time.Duration, publishToStream func(context.Context, string, []byte) error, publishCore func(context.Context, string, []byte) error) *Handler {
	return &Handler{
		store:             store,
		keyStore:          keyStore,
		memberListClient:  memberListClient,
		msgReader:         msgReader,
		siteID:            siteID,
		maxRoomSize:       maxRoomSize,
		maxBatchSize:      maxBatchSize,
		memberListTimeout: memberListTimeout,
		publishToStream:   publishToStream,
		publishCore:       publishCore,
	}
}

// wrappedCtx returns m.Context() augmented with X-Request-ID from the inbound msg header; entry ctx for every nats* handler.
func wrappedCtx(m otelnats.Msg) context.Context {
	return natsutil.ContextWithRequestIDFromHeaders(m.Context(), m.Msg.Header)
}

// RegisterCRUD registers NATS request/reply handlers for room CRUD with queue group.
func (h *Handler) RegisterCRUD(nc *otelnats.Conn) error {
	const queue = "room-service"
	if _, err := nc.QueueSubscribe(subject.RoomCreateWildcard(h.siteID), queue, h.natsCreateRoom); err != nil {
		return fmt.Errorf("subscribe room.create: %w", err)
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
	if _, err := nc.QueueSubscribe(subject.MessageReadWildcard(h.siteID), queue, h.natsMessageRead); err != nil {
		return fmt.Errorf("subscribe message read: %w", err)
	}
	if _, err := nc.QueueSubscribe(subject.MessageReadReceiptWildcard(h.siteID), queue, h.natsMessageReadReceipt); err != nil {
		return fmt.Errorf("subscribe message read-receipt: %w", err)
	}
	if _, err := nc.QueueSubscribe(subject.MessageThreadReadWildcard(h.siteID), queue, h.natsMessageThreadRead); err != nil {
		return fmt.Errorf("subscribe message thread read: %w", err)
	}
	if _, err := nc.QueueSubscribe(subject.MemberListWildcard(h.siteID), queue, h.natsListMembers); err != nil {
		return fmt.Errorf("subscribe member list: %w", err)
	}
	if _, err := nc.QueueSubscribe(subject.OrgMembersWildcard(), queue, h.natsListOrgMembers); err != nil {
		return fmt.Errorf("subscribe org members: %w", err)
	}
	if _, err := nc.QueueSubscribe(subject.RoomKeyEnsure(h.siteID), queue, h.NatsHandleEnsureRoomKey); err != nil {
		return fmt.Errorf("subscribe room key ensure: %w", err)
	}
	if _, err := nc.QueueSubscribe(subject.MuteToggleWildcard(h.siteID), queue, h.natsMuteToggle); err != nil {
		return fmt.Errorf("subscribe mute toggle: %w", err)
	}
	return nil
}

func (h *Handler) natsCreateRoom(m otelnats.Msg) {
	ctx := wrappedCtx(m)
	resp, err := h.handleCreateRoom(ctx, m.Msg.Subject, m.Msg.Data)
	if err != nil {
		var dmExists *dmExistsError
		if errors.As(err, &dmExists) {
			h.replyDMExists(m.Msg, dmExists.RoomID())
			return
		}
		slog.Error("create-room failed", "error", err, "subject", m.Msg.Subject)
		natsutil.ReplyError(m.Msg, sanitizeError(err))
		return
	}
	if err := m.Msg.Respond(resp); err != nil {
		slog.Error("failed to respond to create-room", "error", err)
	}
}

func (h *Handler) replyDMExists(msg *nats.Msg, existingRoomID string) {
	body, err := json.Marshal(model.ErrorResponse{
		Error:  "dm already exists",
		RoomID: existingRoomID,
	})
	if err != nil {
		natsutil.ReplyError(msg, "internal error")
		return
	}
	if err := msg.Respond(body); err != nil {
		slog.Error("failed to respond DM exists", "error", err)
	}
}

func (h *Handler) natsListRooms(m otelnats.Msg) {
	ctx := wrappedCtx(m)
	rooms, err := h.store.ListRooms(ctx)
	if err != nil {
		natsutil.ReplyError(m.Msg, err.Error())
		return
	}
	natsutil.ReplyJSON(m.Msg, model.ListRoomsResponse{Rooms: rooms})
}

func (h *Handler) natsGetRoom(m otelnats.Msg) {
	ctx := wrappedCtx(m)
	parts := strings.Split(m.Msg.Subject, ".")
	roomID := parts[len(parts)-1]
	room, err := h.store.GetRoom(ctx, roomID)
	if err != nil {
		natsutil.ReplyError(m.Msg, err.Error())
		return
	}
	natsutil.ReplyJSON(m.Msg, room)
}

func (h *Handler) handleCreateRoom(ctx context.Context, subj string, data []byte) ([]byte, error) {
	requesterAccount, ok := subject.ParseRoomCreateSubject(subj)
	if !ok {
		return nil, fmt.Errorf("invalid create-room subject: %s", subj)
	}

	requestID := natsutil.RequestIDFromContext(ctx)
	if requestID == "" {
		return nil, errMissingRequestID
	}
	if !idgen.IsValidUUID(requestID) {
		return nil, errInvalidRequestID
	}

	var req model.CreateRoomRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	roomType, err := classifyAndValidate(&req, requesterAccount)
	if err != nil {
		return nil, err
	}

	requester, err := h.store.GetUser(ctx, requesterAccount)
	if err != nil {
		if errors.Is(err, ErrUserNotFound) {
			return nil, errUserNotFound
		}
		return nil, fmt.Errorf("get requester: %w", err)
	}
	if requester.EngName == "" || requester.ChineseName == "" {
		return nil, errInvalidUserData
	}

	switch roomType {
	case model.RoomTypeChannel:
		return h.handleCreateRoomChannel(ctx, &req, requester, requesterAccount, roomType)
	case model.RoomTypeDM, model.RoomTypeBotDM:
		return h.handleCreateRoomDMOrBotDM(ctx, &req, requester, roomType)
	default:
		return nil, fmt.Errorf("unknown room type: %s", roomType)
	}
}

// classifyAndValidate runs all input-only validations in priority order
// (empty → self-DM → channel-name → channel-name-length → bot-in-channel)
// and returns the classified room type. No DB calls.
//
// Dedup/strip of req.Users happens after the empty check and before
// self-DM detection: the post-strip length, combined with the pre-strip
// dedup'd length, lets us detect "users == [requester]" (self-DM) in
// a single pass.
func classifyAndValidate(req *model.CreateRoomRequest, requesterAccount string) (model.RoomType, error) {
	if req.Name == "" && len(req.Users) == 0 && len(req.Orgs) == 0 && len(req.Channels) == 0 {
		return "", errEmptyCreateRequest
	}

	// Single dedup + strip pass; capture the pre-strip dedup'd length so we
	// can detect self-DM (originalUsers == [requesterAccount]) without a
	// second pass.
	deduped := dedup(req.Users)
	req.Users = stripAccount(deduped, requesterAccount)

	if req.Name == "" && len(req.Orgs) == 0 && len(req.Channels) == 0 {
		if len(deduped) == 1 && len(req.Users) == 0 {
			// Pre-strip set was [requester] and post-strip is empty →
			// self-DM.
			return "", errSelfDM
		}
	}

	roomType := determineRoomType(req)

	if roomType == model.RoomTypeChannel {
		if strings.TrimSpace(req.Name) == "" {
			return "", errChannelNameRequired
		}
		if utf8.RuneCountInString(req.Name) > maxChannelNameRunes {
			return "", errChannelNameTooLong
		}
		for _, a := range req.Users {
			if isBot(a) {
				return "", errBotInChannel
			}
		}
	}

	return roomType, nil
}

// maxChannelNameRunes caps the rune length of a client-supplied channel name.
const maxChannelNameRunes = 100

func (h *Handler) handleCreateRoomDMOrBotDM(ctx context.Context, req *model.CreateRoomRequest, requester *model.User, roomType model.RoomType) ([]byte, error) {
	otherAccount := req.Users[0]
	other, err := h.store.GetUser(ctx, otherAccount)
	if err != nil {
		if errors.Is(err, ErrUserNotFound) {
			return nil, errUserNotFound
		}
		return nil, fmt.Errorf("get counterpart: %w", err)
	}
	if roomType == model.RoomTypeDM && (other.EngName == "" || other.ChineseName == "") {
		// botDMs counterpart is an app/bot whose users-collection record
		// typically has empty name fields; the GetApp + Assistant.Enabled
		// check below is the right validation for that case.
		return nil, errInvalidUserData
	}

	req.RoomID = idgen.BuildDMRoomID(requester.ID, other.ID)
	// DM/BotDM resolved set matches the literal counterpart list — there is no expansion.
	req.ResolvedUsers = append([]string(nil), req.Users...)

	// Dedup BEFORE bot-availability check so an existing botDM still resolves
	// to the existing roomId even if the bot was later disabled — preserves
	// the deterministic "open-or-create" contract for DMs.
	existing, err := h.store.FindDMSubscription(ctx, requester.Account, other.Account)
	if err == nil && existing != nil {
		return nil, newDMExistsError(existing.RoomID)
	}
	if err != nil && !errors.Is(err, model.ErrSubscriptionNotFound) {
		return nil, fmt.Errorf("dm dedup check: %w", err)
	}

	if roomType == model.RoomTypeBotDM {
		app, err := h.store.GetApp(ctx, other.Account)
		if err != nil {
			if errors.Is(err, ErrAppNotFound) {
				return nil, errBotNotAvailable
			}
			return nil, fmt.Errorf("get app: %w", err)
		}
		if app.Assistant == nil || !app.Assistant.Enabled {
			return nil, errBotNotAvailable
		}
	}

	return h.publishCreateRoom(ctx, req, requester, roomType)
}

func (h *Handler) handleCreateRoomChannel(ctx context.Context, req *model.CreateRoomRequest, requester *model.User, requesterAccount string, roomType model.RoomType) ([]byte, error) {
	channelOrgIDs, channelAccounts, err := h.expandChannelRefs(ctx, requester.Account, req.Channels)
	if err != nil {
		return nil, fmt.Errorf("expand channels: %w", err)
	}
	// Strip bots from channel-ref expansion so they can't leak into a new channel.
	channelAccounts = filterBots(channelAccounts)
	allOrgs := dedup(append(append([]string{}, req.Orgs...), channelOrgIDs...))
	allUsers := stripAccount(dedup(append(append([]string{}, req.Users...), channelAccounts...)), requesterAccount)

	if len(allUsers) == 0 && len(allOrgs) == 0 {
		return nil, errEmptyCreateRequest
	}

	// Reject phantom orgs before sizing/publishing, same reason as
	// handleAddMembers: the worker writes room_members + sys-msg without
	// rechecking org validity.
	if err := h.validateOrgIDs(ctx, allOrgs); err != nil {
		return nil, err
	}
	if err := h.validateAccountsExist(ctx, allUsers); err != nil {
		return nil, err
	}

	// Pass requesterAccount as excludeAccount: the requester was stripped from
	// allUsers but can still be re-added by org expansion (when their account
	// is in any of the resolved orgs). Excluding them from the count lets us
	// add exactly +1 below for the owner row without double-counting.
	newCount, err := h.store.CountNewMembers(ctx, allOrgs, allUsers, "", requesterAccount)
	if err != nil {
		return nil, fmt.Errorf("count new members: %w", err)
	}
	if newCount == 0 {
		return nil, errEmptyCreateRequest
	}
	// Creator is added implicitly as the channel owner. Count them in the
	// capacity check so a maxRoomSize=N bound caps the materialized room at
	// N members, not N+1.
	totalMembers := 1 + newCount
	if totalMembers > h.maxRoomSize {
		return nil, fmt.Errorf("exceeds maximum capacity (%d): would create %d members", h.maxRoomSize, totalMembers)
	}

	// Preserve req.Users / req.Orgs as the literal client request for sys-message payloads.
	// The worker uses ResolvedUsers / ResolvedOrgs for capacity and member materialization.
	req.ResolvedUsers = allUsers
	req.ResolvedOrgs = allOrgs
	req.RoomID = idgen.GenerateID()
	return h.publishCreateRoom(ctx, req, requester, roomType)
}

func (h *Handler) publishCreateRoom(ctx context.Context, req *model.CreateRoomRequest, requester *model.User, roomType model.RoomType) ([]byte, error) {
	req.RequesterID = requester.ID
	req.RequesterAccount = requester.Account
	req.Timestamp = time.Now().UTC().UnixMilli()
	if span := trace.SpanFromContext(ctx); span.IsRecording() {
		span.SetAttributes(
			attribute.String("room.id", req.RoomID),
			attribute.String("room.type", string(roomType)),
			attribute.String("site.id", h.siteID),
		)
	}

	// Generate and store room key BEFORE canonical event so worker's Get gate succeeds.
	if h.keyStore != nil {
		pair, err := roomkeystore.GenerateKeyPair()
		if err != nil {
			return nil, fmt.Errorf("generate room key: %w", err)
		}
		if _, err := h.keyStore.Set(ctx, req.RoomID, *pair); err != nil {
			roomkeymetrics.ValkeyErrors.Add(ctx, 1, metric.WithAttributes(attribute.String("op", "Set")))
			return nil, fmt.Errorf("store room key: %w", err)
		}
	}

	payload, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal canonical event: %w", err)
	}
	if err := h.publishToStream(ctx, subject.RoomCanonical(h.siteID, "create"), payload); err != nil {
		return nil, fmt.Errorf("publish canonical: %w", err)
	}
	return json.Marshal(model.CreateRoomReply{
		Status:   model.CreateRoomReplyAccepted,
		RoomID:   req.RoomID,
		RoomType: string(roomType),
	})
}

// NatsHandleRemoveMember handles remove-member authorization requests.
func (h *Handler) NatsHandleRemoveMember(m otelnats.Msg) {
	ctx := wrappedCtx(m)
	resp, err := h.handleRemoveMember(ctx, m.Msg.Subject, m.Msg.Data)
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
	ctx := wrappedCtx(m)
	resp, err := h.handleListMembers(ctx, m.Msg.Subject, m.Msg.Data)
	if err != nil {
		slog.Error("list members failed", "error", err)
		natsutil.ReplyError(m.Msg, sanitizeError(err))
		return
	}
	natsutil.ReplyJSON(m.Msg, resp)
}

func (h *Handler) natsListOrgMembers(m otelnats.Msg) {
	ctx := wrappedCtx(m)
	resp, err := h.handleListOrgMembers(ctx, m.Msg.Subject)
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

	// Channel-only: DM/botDM removals are not supported.
	room, err := h.store.GetRoom(ctx, roomID)
	if err != nil {
		return nil, fmt.Errorf("get room: %w", err)
	}
	if room.Type != model.RoomTypeChannel {
		return nil, fmt.Errorf("remove-member only supported on channel rooms, got %s", room.Type)
	}
	// Carry room type to room-worker to avoid a redundant GetRoom round-trip there.
	req.RoomType = room.Type

	// Exactly one of Account or OrgID must be set.
	if (req.Account == "") == (req.OrgID == "") {
		return nil, fmt.Errorf("exactly one of account or orgId must be set")
	}

	// Permission + last-member checks. Dual-membership / no-actual-removal detection moves to room-worker (it owns deletion).
	if req.Account != "" {
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
	ctx := wrappedCtx(m)
	resp, err := h.handleUpdateRole(ctx, m.Msg.Subject, m.Msg.Data)
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
	ctx := wrappedCtx(m)
	resp, err := h.handleAddMembers(ctx, m.Msg.Subject, m.Msg.Data)
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

	// Reject direct bots up front — mirrors classifyAndValidate in
	// create-channel: a client that explicitly lists a bot must see a hard
	// error rather than a silent drop.
	for _, a := range req.Users {
		if isBot(a) {
			return nil, errBotInChannel
		}
	}

	// 5. Expand channels
	channelOrgIDs, channelAccounts, err := h.expandChannelRefs(ctx, requester, req.Channels)
	if err != nil {
		return nil, fmt.Errorf("expand channels: %w", err)
	}
	// Strip bots from channel-ref expansion so a source channel can never
	// silently inject a bot into this channel. Mirrors create-channel.
	channelAccounts = filterBots(channelAccounts)

	// 6. Dedup orgs and direct accounts
	allOrgs := dedup(append(req.Orgs, channelOrgIDs...))
	allUsers := dedup(append(req.Users, channelAccounts...))

	// 6a. Reject phantom orgs up front. Without this, room-worker writes a
	// room_members row for the bogus orgId and fans out a "members added"
	// sys-msg even though no user matches the org.
	if err := h.validateOrgIDs(ctx, allOrgs); err != nil {
		return nil, err
	}
	// 6b. Reject phantom users symmetrically — a typo'd account would be
	// silently dropped by the candidates pipeline and the async job would
	// still report success.
	if err := h.validateAccountsExist(ctx, allUsers); err != nil {
		return nil, err
	}

	// 7. Count net-new members (count-only — actual list materialized in room-worker)
	newCount, err := h.store.CountNewMembers(ctx, allOrgs, allUsers, roomID, "")
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

// validateAccountsExist wraps errUserNotFound with the first phantom account
// (via fmt.Errorf("user %q: %w", …)) when any account has no matching user
// document; errors.Is(err, errUserNotFound) holds. Without this gate a typo'd
// account is silently dropped and the async job reports success.
func (h *Handler) validateAccountsExist(ctx context.Context, accounts []string) error {
	if len(accounts) == 0 {
		return nil
	}
	existing, err := h.store.FindExistingAccounts(ctx, accounts)
	if err != nil {
		return fmt.Errorf("validate accounts: %w", err)
	}
	if len(existing) == len(accounts) {
		return nil
	}
	have := make(map[string]struct{}, len(existing))
	for _, a := range existing {
		have[a] = struct{}{}
	}
	for _, a := range accounts {
		if _, ok := have[a]; !ok {
			return fmt.Errorf("user %q: %w", a, errUserNotFound)
		}
	}
	return nil
}

// validateOrgIDs wraps errInvalidOrg with the first phantom orgID (via
// fmt.Errorf("org %q: %w", …)) when any orgID has zero backing users
// (no user with sectId==orgID or deptId==orgID); errors.Is(err, errInvalidOrg)
// holds. No-op when orgIDs is empty.
func (h *Handler) validateOrgIDs(ctx context.Context, orgIDs []string) error {
	if len(orgIDs) == 0 {
		return nil
	}
	existing, err := h.store.FindExistingOrgIDs(ctx, orgIDs)
	if err != nil {
		return fmt.Errorf("validate org ids: %w", err)
	}
	if len(existing) == len(orgIDs) {
		return nil
	}
	have := make(map[string]struct{}, len(existing))
	for _, id := range existing {
		have[id] = struct{}{}
	}
	for _, id := range orgIDs {
		if _, ok := have[id]; !ok {
			return fmt.Errorf("org %q: %w", id, errInvalidOrg)
		}
	}
	return nil
}

func (h *Handler) expandChannelRefs(ctx context.Context, requester string, refs []model.ChannelRef) (orgIDs, accounts []string, err error) {
	// maxRoomSize+1 is enough to distinguish "fits" from "exceeds the cap" without
	// ever materializing an unbounded result set in memory.
	listLimit := h.maxRoomSize + 1
	for _, ref := range refs {
		var members []model.RoomMember

		// Per-ref deadline so a slow same-site Mongo query or unresponsive
		// remote site cannot stall the create/add request indefinitely; a
		// timeout here surfaces to the caller as channelExpandTimeoutError
		// with site+roomId so the requester can see which channel stalled.
		refCtx, cancel := h.contextWithMemberListTimeout(ctx)

		if ref.SiteID == h.siteID {
			if _, subErr := h.store.GetSubscription(refCtx, requester, ref.RoomID); subErr != nil {
				cancel()
				if errors.Is(subErr, context.DeadlineExceeded) {
					return nil, nil, newChannelExpandTimeoutError(ref.SiteID, ref.RoomID)
				}
				if errors.Is(subErr, model.ErrSubscriptionNotFound) {
					return nil, nil, errNotRoomMember
				}
				return nil, nil, fmt.Errorf("subscription check %s: %w", ref.RoomID, subErr)
			}
			members, err = h.store.ListRoomMembers(refCtx, ref.RoomID, &listLimit, nil, false)
			cancel()
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					return nil, nil, newChannelExpandTimeoutError(ref.SiteID, ref.RoomID)
				}
				return nil, nil, fmt.Errorf("local list-members %s: %w", ref.RoomID, err)
			}
		} else {
			members, err = h.memberListClient.ListMembers(refCtx, requester, ref, listLimit)
			cancel()
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					return nil, nil, newChannelExpandTimeoutError(ref.SiteID, ref.RoomID)
				}
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
	ctx := wrappedCtx(m)
	resp, err := h.handleRoomsInfoBatch(ctx, m.Msg.Data)
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

func (h *Handler) natsMessageRead(m otelnats.Msg) {
	ctx := wrappedCtx(m)
	resp, err := h.handleMessageRead(ctx, m.Msg.Subject, m.Msg.Data)
	if err != nil {
		slog.Error("message read failed", "error", err)
		natsutil.ReplyError(m.Msg, sanitizeError(err))
		return
	}
	if err := m.Msg.Respond(resp); err != nil {
		slog.Error("failed to respond to message read", "error", err)
	}
}

func (h *Handler) handleMessageRead(ctx context.Context, subj string, _ []byte) ([]byte, error) {

	account, roomID, ok := subject.ParseUserRoomSubject(subj)
	if !ok {
		return nil, fmt.Errorf("invalid message-read subject: %s", subj)
	}

	sub, err := h.store.GetSubscription(ctx, account, roomID)
	switch {
	case errors.Is(err, model.ErrSubscriptionNotFound):
		return nil, errNotRoomMember
	case err != nil:
		return nil, fmt.Errorf("get subscription: %w", err)
	}

	newAlert := sub.Alert && len(sub.ThreadUnread) > 0
	now := time.Now().UTC()

	if err := h.store.UpdateSubscriptionRead(ctx, roomID, account, now, newAlert); err != nil {
		return nil, fmt.Errorf("update subscription read: %w", err)
	}

	var (
		userSiteID string
		room       *model.Room
	)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		s, err := h.store.GetUserSiteID(gctx, account)
		if err != nil {
			return fmt.Errorf("get user siteId: %w", err)
		}
		userSiteID = s
		return nil
	})
	g.Go(func() error {
		r, err := h.store.GetRoom(gctx, roomID)
		if err != nil {
			return fmt.Errorf("get room: %w", err)
		}
		room = r
		return nil
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}

	switch {
	case userSiteID == "":
		slog.Warn("user not found locally; skipping cross-site outbox", "account", account)
	case userSiteID != h.siteID:
		payload := model.SubscriptionReadEvent{
			Account:    account,
			RoomID:     roomID,
			LastSeenAt: now.UnixMilli(),
			Alert:      newAlert,
			Timestamp:  now.UnixMilli(),
		}
		payloadData, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("marshal subscription_read payload: %w", err)
		}
		outbox := model.OutboxEvent{
			Type:       model.OutboxSubscriptionRead,
			SiteID:     h.siteID,
			DestSiteID: userSiteID,
			Payload:    payloadData,
			Timestamp:  now.UnixMilli(),
		}
		outboxData, err := json.Marshal(outbox)
		if err != nil {
			return nil, fmt.Errorf("marshal outbox event: %w", err)
		}
		if err := h.publishToStream(ctx, subject.Outbox(h.siteID, userSiteID, model.OutboxSubscriptionRead), outboxData); err != nil {
			return nil, fmt.Errorf("publish subscription_read outbox: %w", err)
		}
	}

	// Skip the room-floor recompute when the room has no content, or when
	// this user already had a recorded read past the latest message
	if room.LastMsgAt == nil {
		return json.Marshal(map[string]string{"status": "accepted"})
	}
	if sub.LastSeenAt != nil && sub.LastSeenAt.After(*room.LastMsgAt) {
		return json.Marshal(map[string]string{"status": "accepted"})
	}

	minTime, err := h.store.MinSubscriptionLastSeenByRoomID(ctx, roomID)
	if err != nil {
		return nil, fmt.Errorf("min subscription lastSeenAt: %w", err)
	}
	if err := h.store.UpdateRoomMinUserLastSeenAt(ctx, roomID, minTime); err != nil {
		return nil, fmt.Errorf("update room minUserLastSeenAt: %w", err)
	}

	return json.Marshal(map[string]string{"status": "accepted"})
}

func (h *Handler) natsMessageReadReceipt(m otelnats.Msg) {
	ctx := wrappedCtx(m)
	resp, err := h.handleMessageReadReceipt(ctx, m.Msg.Subject, m.Msg.Data)
	if err != nil {
		slog.Error("message read-receipt failed", "error", err)
		natsutil.ReplyError(m.Msg, sanitizeError(err))
		return
	}
	if err := m.Msg.Respond(resp); err != nil {
		slog.Error("failed to respond to message read-receipt", "error", err)
	}
}

func (h *Handler) handleMessageReadReceipt(ctx context.Context, subj string, data []byte) ([]byte, error) {
	requesterAccount, roomID, ok := subject.ParseUserRoomSubject(subj)
	if !ok {
		return nil, fmt.Errorf("invalid message-read-receipt subject: %s", subj)
	}

	var req model.ReadReceiptRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}
	if req.MessageID == "" {
		return nil, fmt.Errorf("invalid request: messageId is required")
	}

	if span := trace.SpanFromContext(ctx); span.IsRecording() {
		span.SetAttributes(
			attribute.String("room.id", roomID),
			attribute.String("message.id", req.MessageID),
			attribute.String("site.id", h.siteID),
		)
	}

	var (
		msgRoomID    string
		msgCreatedAt time.Time
		msgSender    string
		msgFound     bool
		subErr       error
	)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		_, err := h.store.GetSubscription(gctx, requesterAccount, roomID)
		subErr = err
		return nil
	})
	g.Go(func() error {
		var err error
		msgRoomID, msgCreatedAt, msgSender, msgFound, err = h.msgReader.GetMessageRoomAndCreatedAt(gctx, req.MessageID)
		if err != nil {
			return fmt.Errorf("get message: %w", err)
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}
	if subErr != nil {
		if errors.Is(subErr, model.ErrSubscriptionNotFound) {
			return nil, errNotRoomMember
		}
		return nil, fmt.Errorf("get subscription: %w", subErr)
	}
	if !msgFound {
		return nil, errMessageNotFound
	}
	if msgRoomID != roomID {
		return nil, errMessageRoomMismatch
	}
	if msgSender != requesterAccount {
		return nil, errNotMessageSender
	}

	rows, err := h.store.ListReadReceipts(ctx, roomID, msgCreatedAt, msgSender, h.maxRoomSize)
	if err != nil {
		return nil, fmt.Errorf("list read receipts: %w", err)
	}

	entries := make([]model.ReadReceiptEntry, len(rows))
	for i, r := range rows {
		entries[i] = model.ReadReceiptEntry{
			UserID:      r.UserID,
			Account:     r.Account,
			ChineseName: r.ChineseName,
			EngName:     r.EngName,
		}
	}

	return json.Marshal(model.ReadReceiptResponse{Readers: entries})
}

func (h *Handler) natsMessageThreadRead(m otelnats.Msg) {
	ctx := wrappedCtx(m)
	resp, err := h.handleMessageThreadRead(ctx, m.Msg.Subject, m.Msg.Data)
	if err != nil {
		slog.Error("message thread-read failed", "error", err)
		natsutil.ReplyError(m.Msg, sanitizeError(err))
		return
	}
	if err := m.Msg.Respond(resp); err != nil {
		slog.Error("failed to respond to message thread-read", "error", err)
	}
}

func (h *Handler) handleMessageThreadRead(ctx context.Context, subj string, data []byte) ([]byte, error) {
	account, roomID, ok := subject.ParseUserRoomSubject(subj)
	if !ok {
		return nil, fmt.Errorf("invalid message-thread-read subject: %s", subj)
	}

	var req model.MessageThreadReadRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, fmt.Errorf("unmarshal thread-read request: %w", err)
	}
	if strings.TrimSpace(req.ThreadID) == "" {
		return nil, errInvalidThreadID
	}

	// Manual priority after Wait(): errNotRoomMember > errThreadSubNotFound > internal errors.
	// Plain errgroup.Group (not WithContext) so a NotFound from one goroutine does NOT cancel
	// the siblings — otherwise context.Canceled in subErr/userSiteErr would outrank tsubErr.
	var (
		sub                          *model.Subscription
		tsub                         *model.ThreadSubscription
		userSiteID                   string
		subErr, tsubErr, userSiteErr error
	)
	var g errgroup.Group
	g.Go(func() error {
		s, err := h.store.GetSubscription(ctx, account, roomID)
		sub, subErr = s, err
		return err
	})
	g.Go(func() error {
		t, err := h.store.GetThreadSubscriptionByParent(ctx, account, req.ThreadID, roomID)
		tsub, tsubErr = t, err
		return err
	})
	g.Go(func() error {
		s, err := h.store.GetUserSiteID(ctx, account)
		userSiteID, userSiteErr = s, err
		return err
	})
	_ = g.Wait()
	// Specific NotFound sentinels first so they always outrank any sibling
	// goroutine's generic error (defends against accidental ctx cancellation).
	switch {
	case errors.Is(subErr, model.ErrSubscriptionNotFound):
		return nil, errNotRoomMember
	case errors.Is(tsubErr, model.ErrThreadSubscriptionNotFound):
		return nil, errThreadSubNotFound
	case subErr != nil:
		return nil, fmt.Errorf("get subscription: %w", subErr)
	case tsubErr != nil:
		return nil, fmt.Errorf("get thread subscription: %w", tsubErr)
	case userSiteErr != nil:
		return nil, fmt.Errorf("get user siteId: %w", userSiteErr)
	}

	newThreadUnread := slices.DeleteFunc(slices.Clone(sub.ThreadUnread), func(s string) bool { return s == req.ThreadID })
	newAlert := sub.Alert && len(newThreadUnread) > 0
	now := time.Now().UTC()

	wg, wctx := errgroup.WithContext(ctx)
	wg.Go(func() error {
		if err := h.store.UpdateSubscriptionThreadRead(wctx, roomID, account, newThreadUnread, newAlert); err != nil {
			return fmt.Errorf("update subscription thread-read: %w", err)
		}
		return nil
	})
	wg.Go(func() error {
		if err := h.store.UpdateThreadSubscriptionRead(wctx, tsub.ThreadRoomID, account, now); err != nil {
			return fmt.Errorf("update thread subscription read: %w", err)
		}
		return nil
	})
	if err := wg.Wait(); err != nil {
		return nil, err
	}

	switch {
	case userSiteID == "":
		slog.Warn("user not found locally; skipping cross-site outbox", "account", account)
	case userSiteID != h.siteID:
		payload := model.ThreadReadEvent{
			Account:         account,
			RoomID:          roomID,
			ThreadRoomID:    tsub.ThreadRoomID,
			ParentMessageID: req.ThreadID,
			NewThreadUnread: newThreadUnread,
			Alert:           newAlert,
			LastSeenAt:      now.UnixMilli(),
			Timestamp:       now.UnixMilli(),
		}
		payloadData, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("marshal thread_read payload: %w", err)
		}
		outbox := model.OutboxEvent{
			Type:       model.OutboxThreadRead,
			SiteID:     h.siteID,
			DestSiteID: userSiteID,
			Payload:    payloadData,
			Timestamp:  now.UnixMilli(),
		}
		outboxData, err := json.Marshal(outbox)
		if err != nil {
			return nil, fmt.Errorf("marshal outbox event: %w", err)
		}
		if err := h.publishToStream(ctx, subject.Outbox(h.siteID, userSiteID, model.OutboxThreadRead), outboxData); err != nil {
			return nil, fmt.Errorf("publish thread_read outbox: %w", err)
		}
	}

	return json.Marshal(map[string]string{"status": "accepted"})
}

// NatsHandleEnsureRoomKey handles server-to-server requests to ensure a room
// has an encryption key pair in Valkey. Generates and stores a new pair if
// missing. The reply confirms the room and version but does not return key
// bytes — encryption/decryption is performed by broadcast-worker and clients,
// which read keys from Valkey directly.
func (h *Handler) NatsHandleEnsureRoomKey(m otelnats.Msg) {
	ctx := wrappedCtx(m)
	resp, err := h.handleEnsureRoomKey(ctx, m.Msg.Data)
	if err != nil {
		slog.Error("ensure room key failed", "error", err)
		natsutil.ReplyError(m.Msg, sanitizeError(err))
		return
	}
	if err := m.Msg.Respond(resp); err != nil {
		slog.Error("failed to respond to ensure room key", "error", err)
	}
}

func (h *Handler) handleEnsureRoomKey(ctx context.Context, data []byte) ([]byte, error) {
	if h.keyStore == nil {
		return nil, fmt.Errorf("ensure room key: key store not configured")
	}
	var req model.RoomKeyEnsureRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, fmt.Errorf("ensure room key: decode request: %w", err)
	}
	if req.RoomID == "" {
		return nil, fmt.Errorf("ensure room key: roomId is required")
	}

	existing, err := h.keyStore.Get(ctx, req.RoomID)
	if err != nil {
		return nil, fmt.Errorf("ensure room key: get: %w", err)
	}
	if existing != nil {
		return json.Marshal(model.RoomKeyEnsureResponse{
			RoomID:  req.RoomID,
			Version: existing.Version,
		})
	}

	newPair, err := roomkeystore.GenerateKeyPair()
	if err != nil {
		return nil, fmt.Errorf("ensure room key: generate key pair: %w", err)
	}
	ver, err := h.keyStore.Set(ctx, req.RoomID, *newPair)
	if err != nil {
		return nil, fmt.Errorf("ensure room key: set: %w", err)
	}
	return json.Marshal(model.RoomKeyEnsureResponse{
		RoomID:  req.RoomID,
		Version: ver,
	})
}

func (h *Handler) natsMuteToggle(m otelnats.Msg) {
	ctx := wrappedCtx(m)
	resp, err := h.handleMuteToggle(ctx, m.Msg.Subject, m.Msg.Data)
	if err != nil {
		slog.Error("mute toggle failed", "error", err, "subject", m.Msg.Subject)
		natsutil.ReplyError(m.Msg, sanitizeError(err))
		return
	}
	if err := m.Msg.Respond(resp); err != nil {
		slog.Error("failed to respond to mute toggle", "error", err)
	}
}

func (h *Handler) handleMuteToggle(ctx context.Context, subj string, _ []byte) ([]byte, error) {
	account, roomID, ok := subject.ParseUserRoomSubject(subj)
	if !ok {
		return nil, fmt.Errorf("invalid mute-toggle subject: %s", subj)
	}

	if span := trace.SpanFromContext(ctx); span.IsRecording() {
		span.SetAttributes(
			attribute.String("room.id", roomID),
			attribute.String("site.id", h.siteID),
		)
	}

	sub, err := h.store.ToggleSubscriptionMute(ctx, roomID, account)
	if err != nil {
		if errors.Is(err, model.ErrSubscriptionNotFound) {
			return nil, errNotRoomMember
		}
		return nil, fmt.Errorf("toggle subscription mute: %w", err)
	}

	now := time.Now().UTC()

	subEvt := model.SubscriptionUpdateEvent{
		UserID:       sub.User.ID,
		Subscription: *sub,
		Action:       "mute_toggled",
		Timestamp:    now.UnixMilli(),
	}
	subEvtData, err := json.Marshal(subEvt)
	if err != nil {
		return nil, fmt.Errorf("marshal subscription update event: %w", err)
	}
	if err := h.publishCore(ctx, subject.SubscriptionUpdate(account), subEvtData); err != nil {
		slog.Error("subscription update publish failed", "error", err, "account", account)
		// Non-fatal — the DB write is the source of truth; clients will reconcile on next refetch.
	}

	userSiteID, err := h.store.GetUserSiteID(ctx, account)
	if err != nil {
		return nil, fmt.Errorf("get user siteId: %w", err)
	}
	if userSiteID != "" && userSiteID != h.siteID {
		payload := model.SubscriptionMuteToggledEvent{
			Account:   account,
			RoomID:    roomID,
			Muted:     sub.Muted,
			Timestamp: now.UnixMilli(),
		}
		payloadData, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("marshal mute-toggled payload: %w", err)
		}
		outbox := model.OutboxEvent{
			Type:       model.OutboxSubscriptionMuteToggled,
			SiteID:     h.siteID,
			DestSiteID: userSiteID,
			Payload:    payloadData,
			Timestamp:  now.UnixMilli(),
		}
		outboxData, err := json.Marshal(outbox)
		if err != nil {
			return nil, fmt.Errorf("marshal outbox event: %w", err)
		}
		if err := h.publishToStream(ctx, subject.Outbox(h.siteID, userSiteID, model.OutboxSubscriptionMuteToggled), outboxData); err != nil {
			return nil, fmt.Errorf("publish mute-toggled outbox: %w", err)
		}
	}

	return json.Marshal(model.MuteToggleResponse{Status: "ok", Muted: sub.Muted})
}
