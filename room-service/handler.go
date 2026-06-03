package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Marz32onE/instrumentation-go/otel-nats/otelnats"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	"github.com/hmchangw/chat/pkg/displayfmt"
	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errnats"
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
	keyStore RoomKeyStore
	// dekProvisioner is set in main when ATREST_ENABLED; nil disables eager
	// at-rest DEK creation at room-create time (message-worker's lazy create
	// still covers remote sites and pre-rollout rooms). Injected as a field
	// rather than a constructor arg to avoid churning every NewHandler caller.
	dekProvisioner           DEKProvisioner
	memberListClient         MemberListClient
	msgReader                MessageReader
	siteID                   string
	maxRoomSize              int
	maxBatchSize             int
	memberListTimeout        time.Duration
	publishToStream          func(ctx context.Context, subj string, data []byte, msgID string) error
	publishCore              func(ctx context.Context, subj string, data []byte) error
	restrictedRoomMinMembers int
	siteURL                  *url.URL
	maxResponseBytes         int64
}

func NewHandler(store RoomStore, keyStore RoomKeyStore, memberListClient MemberListClient, msgReader MessageReader, siteID string, maxRoomSize, maxBatchSize int, memberListTimeout time.Duration, restrictedRoomMinMembers int, publishToStream func(context.Context, string, []byte, string) error, publishCore func(context.Context, string, []byte) error, siteURL *url.URL, maxResponseBytes int64) *Handler {
	return &Handler{
		store:                    store,
		keyStore:                 keyStore,
		memberListClient:         memberListClient,
		msgReader:                msgReader,
		siteID:                   siteID,
		maxRoomSize:              maxRoomSize,
		maxBatchSize:             maxBatchSize,
		memberListTimeout:        memberListTimeout,
		restrictedRoomMinMembers: restrictedRoomMinMembers,
		publishToStream:          publishToStream,
		publishCore:              publishCore,
		siteURL:                  siteURL,
		maxResponseBytes:         maxResponseBytes,
	}
}

// wrappedCtx validates the inbound X-Request-ID via natsutil.RequireRequestID
// (strict mode) and returns m.Context() seeded with the id for the centralized
// errcode.Classify log line. Missing/malformed headers return an
// errcode.BadRequest that the caller must reply to via errnats.Reply.
//
// Strict mode is required here — not the mint-on-missing default — because
// room-service handlers fan out to room-worker, whose JetStream publishes
// derive Nats-Msg-Id / message IDs from this request ID (OutboxDedupID,
// messageDedupSeed, idgen.MessageIDFromRequestID). A silently-minted server-
// side ID would break dedup across client retries. See docs/error-handling.md
// §3a.
func wrappedCtx(m otelnats.Msg) (context.Context, error) {
	ctx, id, err := natsutil.RequireRequestID(m.Context(), m.Msg.Header, m.Msg.Subject)
	if err != nil {
		return m.Context(), err
	}
	return errcode.WithLogValues(ctx, "request_id", id), nil
}

// RegisterCRUD registers NATS request/reply handlers for room CRUD with queue group.
func (h *Handler) RegisterCRUD(nc *otelnats.Conn) error {
	const queue = "room-service"
	if _, err := nc.QueueSubscribe(subject.RoomCreateWildcard(h.siteID), queue, h.natsCreateRoom); err != nil {
		return fmt.Errorf("subscribe room.create: %w", err)
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
	if _, err := nc.QueueSubscribe(subject.OrgMembersWildcard(h.siteID), queue, h.natsListOrgMembers); err != nil {
		return fmt.Errorf("subscribe org members: %w", err)
	}
	if _, err := nc.QueueSubscribe(subject.RoomKeyEnsure(h.siteID), queue, h.NatsHandleEnsureRoomKey); err != nil {
		return fmt.Errorf("subscribe room key ensure: %w", err)
	}
	if _, err := nc.QueueSubscribe(subject.RoomKeyGetWildcard(h.siteID), queue, h.natsGetRoomKey); err != nil {
		return fmt.Errorf("subscribe room key get: %w", err)
	}
	if _, err := nc.QueueSubscribe(subject.MuteToggleWildcard(h.siteID), queue, h.natsMuteToggle); err != nil {
		return fmt.Errorf("subscribe mute toggle: %w", err)
	}
	if _, err := nc.QueueSubscribe(subject.FavoriteToggleWildcard(h.siteID), queue, h.natsFavoriteToggle); err != nil {
		return fmt.Errorf("subscribe favorite toggle: %w", err)
	}
	if _, err := nc.QueueSubscribe(subject.RoomRenameWildcard(h.siteID), queue, h.natsRoomRename); err != nil {
		return fmt.Errorf("subscribe room rename: %w", err)
	}
	if _, err := nc.QueueSubscribe(subject.RoomRestricted(h.siteID), queue, h.natsRoomRestricted); err != nil {
		return fmt.Errorf("subscribe room restricted: %w", err)
	}
	if _, err := nc.QueueSubscribe(subject.RoomAppTabsWildcard(h.siteID), queue, h.natsGetRoomAppTabs); err != nil {
		return fmt.Errorf("subscribe app tabs: %w", err)
	}
	if _, err := nc.QueueSubscribe(subject.RoomAppCmdMenuWildcard(h.siteID), queue, h.natsGetRoomAppCommandMenu); err != nil {
		return fmt.Errorf("subscribe app cmd-menu: %w", err)
	}
	return nil
}

func (h *Handler) natsCreateRoom(m otelnats.Msg) {
	ctx, err := wrappedCtx(m)
	if err != nil {
		errnats.Reply(ctx, m.Msg, err)
		return
	}
	resp, err := h.handleCreateRoom(ctx, m.Msg.Subject, m.Msg.Data)
	if err != nil {
		errnats.Reply(ctx, m.Msg, err)
		return
	}
	if err := m.Msg.Respond(resp); err != nil {
		slog.Error("failed to respond to create-room", "error", err)
	}
}

func (h *Handler) handleCreateRoom(ctx context.Context, subj string, data []byte) ([]byte, error) {
	requesterAccount, ok := subject.ParseRoomCreateSubject(subj)
	if !ok {
		return nil, fmt.Errorf("invalid create-room subject: %s", subj)
	}

	var req model.CreateRoomRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, errcode.BadRequest("invalid request")
	}

	roomType, err := classifyAndValidate(&req, requesterAccount)
	if err != nil {
		return nil, err
	}

	requester, err := h.store.GetUser(ctx, requesterAccount)
	if err != nil {
		if errors.Is(err, ErrUserNotFound) {
			return nil, errcode.NotFound("user not found", errcode.WithReason(errcode.RoomUserNotFound))
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
			return nil, errcode.NotFound("user not found", errcode.WithReason(errcode.RoomUserNotFound))
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
		// DM already exists: this is a success ("open-or-create"), not an error.
		// Return the existing room ID so the client opens it. RoomType is left
		// empty on this branch, matching the prior error-reply behaviour.
		return json.Marshal(model.CreateRoomReply{
			Status: model.CreateRoomStatusExists,
			RoomID: existing.RoomID,
		})
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

	// Reject phantom orgs and users before sizing/publishing (run concurrently),
	// same reason as handleAddMembers: the worker writes room_members + sys-msg
	// without rechecking validity.
	if err := h.validateMembershipRefs(ctx, allOrgs, allUsers); err != nil {
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
		return nil, errcode.Conflict(
			fmt.Sprintf("exceeds maximum capacity (%d): would create %d members", h.maxRoomSize, totalMembers),
			errcode.WithReason(errcode.RoomMaxSizeReached),
			errcode.WithMetadata("maxRoomSize", strconv.Itoa(h.maxRoomSize), "attempted", strconv.Itoa(totalMembers)),
		)
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

	// Provision the at-rest DEK BEFORE the canonical event so the first message
	// write doesn't pay the create cost. Blocking, like the room key above;
	// message-worker's lazy creation still covers remote sites (the DEK is
	// per-site) and rooms created before this rollout.
	if h.dekProvisioner != nil {
		if err := h.dekProvisioner.EnsureDEK(ctx, req.RoomID); err != nil {
			return nil, fmt.Errorf("provision at-rest DEK: %w", err)
		}
	}

	payload, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal canonical event: %w", err)
	}
	if err := h.publishToStream(ctx, subject.RoomCanonical(h.siteID, "create"), payload, ""); err != nil {
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
	ctx, err := wrappedCtx(m)
	if err != nil {
		errnats.Reply(ctx, m.Msg, err)
		return
	}
	resp, err := h.handleRemoveMember(ctx, m.Msg.Subject, m.Msg.Data)
	if err != nil {
		errnats.Reply(ctx, m.Msg, err)
		return
	}
	if err := m.Msg.Respond(resp); err != nil {
		slog.Error("failed to respond to message", "error", err)
	}
}

func (h *Handler) natsListMembers(m otelnats.Msg) {
	ctx, err := wrappedCtx(m)
	if err != nil {
		errnats.Reply(ctx, m.Msg, err)
		return
	}
	resp, err := h.handleListMembers(ctx, m.Msg.Subject, m.Msg.Data)
	if err != nil {
		errnats.Reply(ctx, m.Msg, err)
		return
	}
	natsutil.ReplyJSON(m.Msg, resp)
}

func (h *Handler) natsListOrgMembers(m otelnats.Msg) {
	ctx, err := wrappedCtx(m)
	if err != nil {
		errnats.Reply(ctx, m.Msg, err)
		return
	}
	resp, err := h.handleListOrgMembers(ctx, m.Msg.Subject)
	if err != nil {
		errnats.Reply(ctx, m.Msg, err)
		return
	}
	natsutil.ReplyJSON(m.Msg, resp)
}

func (h *Handler) handleListOrgMembers(ctx context.Context, subj string) (model.ListOrgMembersResponse, error) {
	orgID, _, ok := subject.ParseOrgMembersSubject(subj)
	if !ok {
		return model.ListOrgMembersResponse{}, fmt.Errorf("invalid org-members subject")
	}
	members, err := h.store.ListOrgMembers(ctx, orgID)
	if err != nil {
		if errcode.HasReason(err, errcode.RoomInvalidOrg) {
			return model.ListOrgMembersResponse{}, errcode.BadRequest("invalid org", errcode.WithReason(errcode.RoomInvalidOrg))
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
			return model.ListRoomMembersResponse{}, errcode.BadRequest("invalid request")
		}
	}
	if req.Limit != nil && *req.Limit <= 0 {
		return model.ListRoomMembersResponse{}, errListLimitInvalid
	}
	if req.Offset != nil && *req.Offset < 0 {
		return model.ListRoomMembersResponse{}, errListOffsetInvalid
	}

	members, err := h.store.ListRoomMembers(ctx, roomID, req.Limit, req.Offset, req.Enrich)
	if err != nil {
		return model.ListRoomMembersResponse{}, fmt.Errorf("get room members: %w", err)
	}
	return model.ListRoomMembersResponse{Members: members}, nil
}

func (h *Handler) natsGetRoomKey(m otelnats.Msg) {
	ctx, err := wrappedCtx(m)
	if err != nil {
		errnats.Reply(ctx, m.Msg, err)
		return
	}
	resp, err := h.handleGetRoomKey(ctx, m.Msg.Subject, m.Msg.Data)
	if err != nil {
		errnats.Reply(ctx, m.Msg, err)
		return
	}
	if err := m.Msg.Respond(resp); err != nil {
		slog.Error("failed to respond to get room key", "error", err)
	}
}

func (h *Handler) handleGetRoomKey(ctx context.Context, subj string, data []byte) ([]byte, error) {
	if h.keyStore == nil {
		return nil, fmt.Errorf("get room key: key store not configured")
	}
	requesterAccount, roomID, ok := subject.ParseUserRoomSubject(subj)
	if !ok {
		return nil, fmt.Errorf("invalid get-room-key subject")
	}

	_, err := h.store.GetSubscription(ctx, requesterAccount, roomID)
	switch {
	case errors.Is(err, model.ErrSubscriptionNotFound):
		return nil, errNotRoomMember
	case err != nil:
		return nil, fmt.Errorf("check room membership: %w", err)
	}

	var req model.RoomKeyGetRequest
	if len(data) > 0 {
		if err := json.Unmarshal(data, &req); err != nil {
			return nil, errcode.BadRequest("invalid request")
		}
	}

	if req.Version == nil {
		existing, err := h.keyStore.Get(ctx, roomID)
		if err != nil {
			return nil, fmt.Errorf("get room key: %w", err)
		}
		if existing == nil {
			return nil, errRoomKeyAbsent
		}
		// #nosec G117 -- RoomKeyGetResponse.PrivateKey is the intended payload: on-demand key delivery to the authorized room member over an auth-callout-gated per-user NATS subject, not a leak
		return json.Marshal(model.RoomKeyGetResponse{
			RoomID:     roomID,
			Version:    existing.Version,
			PrivateKey: existing.KeyPair.PrivateKey,
		})
	}

	pair, err := h.keyStore.GetByVersion(ctx, roomID, *req.Version)
	if err != nil {
		return nil, fmt.Errorf("get room key: %w", err)
	}
	if pair == nil {
		return nil, errRoomKeyAbsent
	}
	// #nosec G117 -- RoomKeyGetResponse.PrivateKey is the intended payload: on-demand key delivery to the authorized room member over an auth-callout-gated per-user NATS subject, not a leak
	return json.Marshal(model.RoomKeyGetResponse{
		RoomID:     roomID,
		Version:    *req.Version,
		PrivateKey: pair.PrivateKey,
	})
}

func (h *Handler) handleRemoveMember(ctx context.Context, subj string, data []byte) ([]byte, error) {
	requesterAccount, roomID, ok := subject.ParseUserRoomSubject(subj)
	if !ok {
		return nil, fmt.Errorf("invalid remove-member subject: %s", subj)
	}

	var req model.RemoveMemberRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, errcode.BadRequest("invalid request")
	}

	if req.RoomID != "" && req.RoomID != roomID {
		return nil, errRoomIDMismatch
	}
	req.RoomID = roomID
	req.Requester = requesterAccount

	// Channel-only: DM/botDM removals are not supported.
	room, err := h.store.GetRoom(ctx, roomID)
	if err != nil {
		return nil, fmt.Errorf("get room: %w", err)
	}
	if room.Type != model.RoomTypeChannel {
		// Preserve sentinel identity (errors.Is matches via %w unwrap) while
		// carrying the actual room type for client-side context.
		return nil, fmt.Errorf("%w (got %s)", errRemoveChannelOnly, room.Type)
	}
	// Carry room type to room-worker to avoid a redundant GetRoom round-trip there.
	req.RoomType = room.Type

	// Exactly one of Account or OrgID must be set.
	if (req.Account == "") == (req.OrgID == "") {
		return nil, errRemoveTargetAmbiguous
	}

	// Permission + last-member checks. Dual-membership / no-actual-removal detection moves to room-worker (it owns deletion).
	if req.Account != "" {
		target, err := h.store.GetSubscriptionWithMembership(ctx, roomID, req.Account)
		if err != nil {
			return nil, fmt.Errorf("get target subscription: %w", err)
		}
		if target.HasOrgMembership && !target.HasIndividualMembership {
			return nil, errOrgMemberCannotLeaveSolo
		}
		if req.Account != requesterAccount {
			requesterSub, err := h.store.GetSubscription(ctx, requesterAccount, roomID)
			if err != nil {
				return nil, fmt.Errorf("get requester subscription: %w", err)
			}
			if !hasRole(requesterSub.Roles, model.RoleOwner) {
				return nil, errOnlyOwnersCanRemove
			}
		}
		counts, err := h.store.CountMembersAndOwners(ctx, roomID)
		if err != nil {
			return nil, fmt.Errorf("count members: %w", err)
		}
		if counts.MemberCount <= 1 {
			return nil, errCannotRemoveLastMember
		}
		if hasRole(target.Subscription.Roles, model.RoleOwner) && counts.OwnerCount <= 1 {
			return nil, errLastOwnerCannotLeave
		}
	} else {
		// Owner-removes-org: only the requester's owner role matters here; org members resolved downstream.
		sub, err := h.store.GetSubscription(ctx, requesterAccount, roomID)
		if err != nil {
			return nil, fmt.Errorf("get requester subscription: %w", err)
		}
		if !hasRole(sub.Roles, model.RoleOwner) {
			return nil, errOnlyOwnersCanRemove
		}
	}

	// Stable seed for room-worker's deterministic system-message IDs across JetStream redeliveries.
	req.Timestamp = time.Now().UTC().UnixMilli()

	// Publish to ROOMS stream for room-worker processing.
	data, err = json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal remove member request: %w", err)
	}
	if err := h.publishToStream(ctx, subject.RoomCanonical(h.siteID, "member.remove"), data, ""); err != nil {
		return nil, fmt.Errorf("publish to stream: %w", err)
	}

	return json.Marshal(map[string]string{"status": "accepted"})
}

func (h *Handler) natsUpdateRole(m otelnats.Msg) {
	ctx, err := wrappedCtx(m)
	if err != nil {
		errnats.Reply(ctx, m.Msg, err)
		return
	}
	resp, err := h.handleUpdateRole(ctx, m.Msg.Subject, m.Msg.Data)
	if err != nil {
		errnats.Reply(ctx, m.Msg, err)
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
		return nil, errcode.BadRequest("invalid request")
	}
	if req.RoomID != "" && req.RoomID != roomID {
		return nil, errRoomIDMismatch
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
	if err := h.publishToStream(ctx, subject.RoomCanonical(h.siteID, "member.role-update"), data, ""); err != nil {
		return nil, fmt.Errorf("publish to stream: %w", err)
	}
	return json.Marshal(map[string]string{"status": "accepted"})
}

func (h *Handler) natsAddMembers(m otelnats.Msg) {
	ctx, err := wrappedCtx(m)
	if err != nil {
		errnats.Reply(ctx, m.Msg, err)
		return
	}
	resp, err := h.handleAddMembers(ctx, m.Msg.Subject, m.Msg.Data)
	if err != nil {
		errnats.Reply(ctx, m.Msg, err)
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

	// 2. Verify requester is in room. Distinguish "not a member" (typed
	// forbidden — the user genuinely can't add members) from an infra failure
	// (Mongo timeout etc. — must NOT collapse to a 403 user-error).
	sub, err := h.store.GetSubscription(ctx, requester, roomID)
	if err != nil {
		if errors.Is(err, model.ErrSubscriptionNotFound) {
			return nil, errNotRoomMember
		}
		return nil, fmt.Errorf("check requester room membership: %w", err)
	}

	// 3. Get room and guard on type
	room, err := h.store.GetRoom(ctx, roomID)
	if err != nil {
		return nil, fmt.Errorf("get room: %w", err)
	}
	if room.Type != model.RoomTypeChannel {
		return nil, errAddMembersChannelOnly
	}
	if room.Restricted && !hasRole(sub.Roles, model.RoleOwner) {
		return nil, errOnlyOwnersCanAddToRes
	}

	// 4. Unmarshal request
	var req model.AddMembersRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, errcode.BadRequest("invalid request")
	}
	if req.RoomID != "" && req.RoomID != roomID {
		return nil, errRoomIDMismatch
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

	// 6a/6b. Reject phantom orgs and users up front (run concurrently). Without
	// this, room-worker writes a room_members row for the bogus orgId/account
	// and fans out a "members added" sys-msg even though no user matches.
	if err := h.validateMembershipRefs(ctx, allOrgs, allUsers); err != nil {
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
		return nil, errcode.Conflict(
			fmt.Sprintf("room is at maximum capacity (%d): cannot add %d members to room with %d existing", h.maxRoomSize, newCount, room.UserCount),
			errcode.WithReason(errcode.RoomMaxSizeReached),
			errcode.WithMetadata("maxRoomSize", strconv.Itoa(h.maxRoomSize),
				"currentUserCount", strconv.Itoa(room.UserCount),
				"attempted", strconv.Itoa(room.UserCount+newCount)),
		)
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
	if err := h.publishToStream(ctx, subject.RoomCanonical(h.siteID, "member.add"), normalized, ""); err != nil {
		return nil, fmt.Errorf("publish to stream: %w", err)
	}

	// 10. Reply accepted
	return json.Marshal(map[string]string{"status": "accepted"})
}

// validateAccountsExist returns a RoomUserNotFound-reason errcode naming the
// first phantom account when any account has no matching user document.
// errcode.HasReason(err, errcode.RoomUserNotFound) holds. Without this gate a
// typo'd account is silently dropped and the async job reports success.
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
			return errcode.NotFound(fmt.Sprintf("user %q not found", a), errcode.WithReason(errcode.RoomUserNotFound))
		}
	}
	return nil
}

// validateOrgIDs returns a RoomInvalidOrg-reason errcode naming the first
// phantom orgID when any orgID has zero backing users (no user with
// sectId==orgID or deptId==orgID). errcode.HasReason(err, errcode.RoomInvalidOrg)
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
			return errcode.BadRequest(fmt.Sprintf("invalid org %q", id), errcode.WithReason(errcode.RoomInvalidOrg))
		}
	}
	return nil
}

// validateMembershipRefs runs the org and account existence checks
// concurrently — they hit the users collection independently, so there is no
// reason to serialize them. Uses a plain errgroup (no shared context
// cancellation) so both checks always complete, and applies the org error in
// preference to the account error to preserve the prior sequential priority.
func (h *Handler) validateMembershipRefs(ctx context.Context, orgIDs, accounts []string) error {
	var orgErr, acctErr error
	var g errgroup.Group
	g.Go(func() error { orgErr = h.validateOrgIDs(ctx, orgIDs); return orgErr })
	g.Go(func() error { acctErr = h.validateAccountsExist(ctx, accounts); return acctErr })
	_ = g.Wait()
	if orgErr != nil {
		return orgErr
	}
	return acctErr
}

func (h *Handler) expandChannelRefs(ctx context.Context, requester string, refs []model.ChannelRef) (orgIDs, accounts []string, err error) {
	// maxRoomSize+1 is enough to distinguish "fits" from "exceeds the cap" without
	// ever materializing an unbounded result set in memory.
	listLimit := h.maxRoomSize + 1
	for _, ref := range refs {
		var members []model.RoomMember

		// Per-ref deadline so a slow same-site Mongo query or unresponsive
		// remote site cannot stall the create/add request indefinitely; a
		// timeout here surfaces to the caller as an Unavailable errcode with
		// site+roomId so the requester can see which channel stalled.
		refCtx, cancel := h.contextWithMemberListTimeout(ctx)

		if ref.SiteID == h.siteID {
			if _, subErr := h.store.GetSubscription(refCtx, requester, ref.RoomID); subErr != nil {
				cancel()
				if errors.Is(subErr, context.DeadlineExceeded) {
					return nil, nil, errcode.Unavailable(fmt.Sprintf("timeout listing members of channel %s@%s", ref.RoomID, ref.SiteID))
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
					return nil, nil, errcode.Unavailable(fmt.Sprintf("timeout listing members of channel %s@%s", ref.RoomID, ref.SiteID))
				}
				return nil, nil, fmt.Errorf("local list-members %s: %w", ref.RoomID, err)
			}
		} else {
			members, err = h.memberListClient.ListMembers(refCtx, requester, ref, listLimit)
			cancel()
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					return nil, nil, errcode.Unavailable(fmt.Sprintf("timeout listing members of channel %s@%s", ref.RoomID, ref.SiteID))
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
	ctx, err := wrappedCtx(m)
	if err != nil {
		errnats.Reply(ctx, m.Msg, err)
		return
	}
	resp, err := h.handleRoomsInfoBatch(ctx, m.Msg.Data)
	if err != nil {
		errnats.Reply(ctx, m.Msg, err)
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
		return nil, errcode.BadRequest("invalid request")
	}
	if len(req.RoomIDs) == 0 {
		return nil, errcode.BadRequest("roomIds must not be empty")
	}
	if len(req.RoomIDs) > h.maxBatchSize {
		return nil, errcode.BadRequest(fmt.Sprintf("batch size %d exceeds limit %d", len(req.RoomIDs), h.maxBatchSize))
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
	ctx, err := wrappedCtx(m)
	if err != nil {
		errnats.Reply(ctx, m.Msg, err)
		return
	}
	resp, err := h.handleMessageRead(ctx, m.Msg.Subject, m.Msg.Data)
	if err != nil {
		errnats.Reply(ctx, m.Msg, err)
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
		if err := h.publishToStream(ctx, subject.Outbox(h.siteID, userSiteID, model.OutboxSubscriptionRead), outboxData, ""); err != nil {
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
	// Skip the write when the recomputed floor matches what the room already
	// carries. For a busy room the floor is unchanged on almost every read (the
	// reader is rarely the most-behind member, and large rooms usually have an
	// unread member that pins the floor to nil), so this avoids a no-op Mongo
	// round trip and the write-intent lock on the hot rooms document.
	if !sameFloor(minTime, room.MinUserLastSeenAt) {
		if err := h.store.UpdateRoomMinUserLastSeenAt(ctx, roomID, minTime); err != nil {
			return nil, fmt.Errorf("update room minUserLastSeenAt: %w", err)
		}
	}

	return json.Marshal(map[string]string{"status": "accepted"})
}

func (h *Handler) natsMessageReadReceipt(m otelnats.Msg) {
	ctx, err := wrappedCtx(m)
	if err != nil {
		errnats.Reply(ctx, m.Msg, err)
		return
	}
	resp, err := h.handleMessageReadReceipt(ctx, m.Msg.Subject, m.Msg.Data)
	if err != nil {
		errnats.Reply(ctx, m.Msg, err)
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
		return nil, errcode.BadRequest("invalid request")
	}
	if req.MessageID == "" {
		return nil, errcode.BadRequest("invalid request: messageId is required")
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
	ctx, err := wrappedCtx(m)
	if err != nil {
		errnats.Reply(ctx, m.Msg, err)
		return
	}
	resp, err := h.handleMessageThreadRead(ctx, m.Msg.Subject, m.Msg.Data)
	if err != nil {
		errnats.Reply(ctx, m.Msg, err)
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
		return nil, errcode.BadRequest("invalid request")
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
		if err := h.publishToStream(ctx, subject.Outbox(h.siteID, userSiteID, model.OutboxThreadRead), outboxData, ""); err != nil {
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
	ctx, err := wrappedCtx(m)
	if err != nil {
		errnats.Reply(ctx, m.Msg, err)
		return
	}
	resp, err := h.handleEnsureRoomKey(ctx, m.Msg.Data)
	if err != nil {
		errnats.Reply(ctx, m.Msg, err)
		return
	}
	if err := m.Msg.Respond(resp); err != nil {
		slog.Error("failed to respond to ensure room key", "error", err)
	}
}

func (h *Handler) handleEnsureRoomKey(ctx context.Context, data []byte) ([]byte, error) {
	if h.keyStore == nil {
		// Local Valkey disabled — surfaces to peer sites as a transient outage
		// (symmetric with the timeout-class failures in :808/:819/:828).
		return nil, errcode.Unavailable("room key store not configured")
	}
	var req model.RoomKeyEnsureRequest
	if err := json.Unmarshal(data, &req); err != nil {
		// Per doc.go and pkg/errcode logging contract: json.SyntaxError /
		// UnmarshalTypeError strings embed the offending substring and field
		// shape from an unauthenticated payload — never WithCause(err) here.
		// Same shape as message-gatekeeper:173.
		return nil, errcode.BadRequest("invalid ensure-room-key request")
	}
	if req.RoomID == "" {
		return nil, errcode.BadRequest("roomId is required")
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

func (h *Handler) natsRoomRename(m otelnats.Msg) {
	ctx, err := wrappedCtx(m)
	if err != nil {
		errnats.Reply(ctx, m.Msg, err)
		return
	}
	resp, err := h.handleRoomRename(ctx, m.Msg.Subject, m.Msg.Data)
	if err != nil {
		errnats.Reply(ctx, m.Msg, err)
		return
	}
	if err := m.Msg.Respond(resp); err != nil {
		slog.Error("failed to respond to rename", "error", err)
	}
}

func (h *Handler) handleRoomRename(ctx context.Context, subj string, data []byte) ([]byte, error) {
	account, roomID, ok := subject.ParseUserRoomSubject(subj)
	if !ok {
		return nil, fmt.Errorf("%w: %s", errInvalidRenameSubject, subj)
	}
	requestID := natsutil.RequestIDFromContext(ctx)

	// Client body carries only newName — roomID and account are taken from the
	// subject (the authoritative identity), never from the wire body.
	var body struct {
		NewName string `json:"newName"`
	}
	if err := json.Unmarshal(data, &body); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	slog.Debug("processing room.rename",
		"op", model.AsyncJobOpRoomRename,
		"requester", account,
		"roomID", roomID,
		"requestID", requestID)

	name := strings.TrimSpace(body.NewName)
	if name == "" || utf8.RuneCountInString(name) > 100 {
		return nil, errInvalidName
	}

	requesterUser, getUserErr := h.store.GetUser(ctx, account)
	if getUserErr != nil && !errors.Is(getUserErr, ErrUserNotFound) {
		return nil, fmt.Errorf("get user: %w", getUserErr)
	}

	room, err := h.store.GetRoom(ctx, roomID)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, errRoomNotFound
		}
		return nil, fmt.Errorf("get room: %w", err)
	}
	if room.Type != model.RoomTypeChannel {
		return nil, errRenameChannelOnly
	}

	if !isPlatformAdmin(requesterUser) {
		sub, subErr := h.store.GetSubscription(ctx, account, roomID)
		if subErr != nil {
			if errors.Is(subErr, mongo.ErrNoDocuments) || errors.Is(subErr, model.ErrSubscriptionNotFound) {
				return nil, errOnlyOwnersOrAdmins
			}
			return nil, fmt.Errorf("get requester subscription: %w", subErr)
		}
		if !hasRole(sub.Roles, model.RoleOwner) {
			return nil, errOnlyOwnersOrAdmins
		}
	}

	canonical := model.RenameRoomRequest{
		RoomID:    roomID,
		Account:   account,
		NewName:   name,
		Timestamp: time.Now().UTC().UnixMilli(),
	}
	out, err := json.Marshal(canonical)
	if err != nil {
		return nil, fmt.Errorf("marshal rename request: %w", err)
	}
	if err := h.publishToStream(ctx, subject.RoomCanonical(h.siteID, "room.rename"), out, ""); err != nil {
		return nil, fmt.Errorf("publish to stream: %w", err)
	}
	return json.Marshal(map[string]string{"status": "accepted", "requestId": requestID})
}

func (h *Handler) natsRoomRestricted(m otelnats.Msg) {
	ctx, err := wrappedCtx(m)
	if err != nil {
		errnats.Reply(ctx, m.Msg, err)
		return
	}
	resp, err := h.handleRoomRestricted(ctx, m.Msg.Data)
	if err != nil {
		errnats.Reply(ctx, m.Msg, err)
		return
	}
	if err := m.Msg.Respond(resp); err != nil {
		slog.Error("failed to respond to restricted", "error", err)
	}
}

// handleRoomRestricted is the sync chat.server.> RPC. Account in the body is
// the audit identity (no subject prefix authenticates the caller — this RPC
// is server-side admin tooling). Mongo writes + sys-message publish + outbox
// fan-out happen inline; caller retries safely via dedup IDs.
func (h *Handler) handleRoomRestricted(ctx context.Context, data []byte) ([]byte, error) {
	requestID := natsutil.RequestIDFromContext(ctx)

	var req model.RoomRestrictedRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}
	if req.RoomID == "" || req.Account == "" {
		return nil, fmt.Errorf("%w: roomId and account are required", errInvalidRestrictedSubject)
	}

	// Admin-only RPC is rare; info-level audit trail is justified.
	slog.Info("processing room.restricted",
		"requester", req.Account,
		"roomID", req.RoomID,
		"requestID", requestID)

	requesterUser, getUserErr := h.store.GetUser(ctx, req.Account)
	if getUserErr != nil && !errors.Is(getUserErr, ErrUserNotFound) {
		return nil, fmt.Errorf("get user: %w", getUserErr)
	}
	if !isPlatformAdmin(requesterUser) {
		return nil, errOnlyAdmins
	}

	room, err := h.store.GetRoom(ctx, req.RoomID)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, errRoomNotFound
		}
		return nil, fmt.Errorf("get room: %w", err)
	}
	if room.Type != model.RoomTypeChannel {
		return nil, errRestrictedChannelOnly
	}

	isTransition := req.Restricted && !room.Restricted

	if req.Restricted && req.OwnerAccount != "" {
		if _, subErr := h.store.GetSubscription(ctx, req.OwnerAccount, req.RoomID); subErr != nil {
			if errors.Is(subErr, mongo.ErrNoDocuments) || errors.Is(subErr, model.ErrSubscriptionNotFound) {
				return nil, errOwnerNotMember
			}
			return nil, fmt.Errorf("get owner subscription: %w", subErr)
		}
	}
	if isTransition {
		if req.OwnerAccount == "" {
			return nil, errOwnerAccountRequired
		}
		if room.UserCount < h.restrictedRoomMinMembers {
			return nil, fmt.Errorf("%w (need at least %d)", errNotEnoughMembers, h.restrictedRoomMinMembers)
		}
	}

	req.Timestamp = time.Now().UTC().UnixMilli()

	if err := h.store.UpdateRoomVisibility(ctx, req.RoomID, req.Restricted, req.ExternalAccess); err != nil {
		if errors.Is(err, ErrRoomNotFound) {
			return nil, errRoomNotFound
		}
		return nil, fmt.Errorf("update room restricted: %w", err)
	}
	if err := h.store.ApplySubscriptionVisibility(ctx, req.RoomID, req.Restricted, req.ExternalAccess, req.OwnerAccount); err != nil {
		if errors.Is(err, ErrOwnerNotSubscribed) {
			return nil, errOwnerNotMember
		}
		return nil, fmt.Errorf("apply subscription restricted: %w", err)
	}

	sysData, err := json.Marshal(model.RoomRestrictedSysData{
		Restricted:     req.Restricted,
		ExternalAccess: req.ExternalAccess,
		ByAccount:      req.Account,
		OwnerAccount:   req.OwnerAccount,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal restricted sys data: %w", err)
	}
	requesterDisplay := displayfmt.CombineWithFallback(requesterUser.EngName, requesterUser.ChineseName, requesterUser.Account)
	sysMsg := model.Message{
		ID:          idgen.MessageIDFromRequestID(requestID, "room_restricted"),
		RoomID:      req.RoomID,
		UserAccount: req.Account,
		Type:        model.MessageTypeRoomRestricted,
		Content:     fmt.Sprintf("%q changed the channel restricted state", requesterDisplay),
		SysMsgData:  sysData,
		CreatedAt:   time.UnixMilli(req.Timestamp).UTC(),
	}
	msgEvt := model.MessageEvent{
		Event:     model.EventCreated,
		Message:   sysMsg,
		SiteID:    h.siteID,
		Timestamp: req.Timestamp,
	}
	msgEvtData, err := json.Marshal(msgEvt)
	if err != nil {
		return nil, fmt.Errorf("marshal sys message event: %w", err)
	}
	if err := h.publishToStream(ctx, subject.MsgCanonicalCreated(h.siteID), msgEvtData, natsutil.CanonicalDedupID(&msgEvt)); err != nil {
		return nil, fmt.Errorf("publish room_restricted sys message: %w", err)
	}

	subs, err := h.store.ListSubscriptionsByRoom(ctx, req.RoomID)
	if err != nil {
		return nil, fmt.Errorf("list subscriptions: %w", err)
	}
	accounts := make([]string, 0, len(subs))
	for i := range subs {
		accounts = append(accounts, subs[i].User.Account)
	}
	users, err := h.store.FindUsersByAccounts(ctx, accounts)
	if err != nil {
		return nil, fmt.Errorf("find users for outbox fan-out: %w", err)
	}
	seenSites := make(map[string]struct{})
	var remoteSites []string
	for i := range users {
		if users[i].SiteID == "" || users[i].SiteID == h.siteID {
			continue
		}
		if _, dup := seenSites[users[i].SiteID]; dup {
			continue
		}
		seenSites[users[i].SiteID] = struct{}{}
		remoteSites = append(remoteSites, users[i].SiteID)
	}
	if len(remoteSites) > 0 {
		payload, err := json.Marshal(model.RoomRestrictedOutboxPayload{
			RoomID:         req.RoomID,
			Restricted:     req.Restricted,
			ExternalAccess: req.ExternalAccess,
			OwnerAccount:   req.OwnerAccount,
			Timestamp:      req.Timestamp,
		})
		if err != nil {
			return nil, fmt.Errorf("marshal restricted outbox payload: %w", err)
		}
		for _, remoteSiteID := range remoteSites {
			evt := model.OutboxEvent{
				Type: model.OutboxRoomRestricted, SiteID: h.siteID, DestSiteID: remoteSiteID,
				Payload: payload, Timestamp: time.Now().UTC().UnixMilli(),
			}
			evtData, mErr := json.Marshal(evt)
			if mErr != nil {
				return nil, fmt.Errorf("marshal restricted outbox event: %w", mErr)
			}
			if err := h.publishToStream(ctx, subject.Outbox(h.siteID, remoteSiteID, model.OutboxRoomRestricted), evtData, natsutil.OutboxDedupID(ctx, remoteSiteID, requestID)); err != nil {
				return nil, fmt.Errorf("publish restricted outbox to %s: %w", remoteSiteID, err)
			}
		}
	}

	return json.Marshal(map[string]string{"status": "ok", "requestId": requestID})
}

func (h *Handler) natsMuteToggle(m otelnats.Msg) {
	ctx, err := wrappedCtx(m)
	if err != nil {
		errnats.Reply(ctx, m.Msg, err)
		return
	}
	resp, err := h.handleMuteToggle(ctx, m.Msg.Subject, m.Msg.Data)
	if err != nil {
		errnats.Reply(ctx, m.Msg, err)
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

	// Canonical room-stream event consumed by notification-worker for cache invalidation.
	// One event per mutation, room-scoped (not per-user). Non-fatal: TTL reconciles on miss.
	canonEvt := model.CanonicalMemberEvent{
		Type:      model.CanonicalMemberEventMuted,
		RoomID:    sub.RoomID,
		Account:   account,
		Muted:     sub.Muted,
		Timestamp: now.UnixMilli(),
	}
	if canonData, err := json.Marshal(canonEvt); err == nil {
		if err := h.publishToStream(ctx, subject.RoomCanonicalMemberEvent(h.siteID, model.CanonicalMemberEventMuted), canonData, ""); err != nil {
			slog.Error("canonical member event publish failed", "error", err, "type", "muted", "roomID", sub.RoomID, "account", account)
		}
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
		if err := h.publishToStream(ctx, subject.Outbox(h.siteID, userSiteID, model.OutboxSubscriptionMuteToggled), outboxData, ""); err != nil {
			return nil, fmt.Errorf("publish mute-toggled outbox: %w", err)
		}
	}

	return json.Marshal(model.MuteToggleResponse{Status: "ok", Muted: sub.Muted})
}

func (h *Handler) natsFavoriteToggle(m otelnats.Msg) {
	ctx, err := wrappedCtx(m)
	if err != nil {
		errnats.Reply(ctx, m.Msg, err)
		return
	}
	resp, err := h.handleFavoriteToggle(ctx, m.Msg.Subject, m.Msg.Data)
	if err != nil {
		errnats.Reply(ctx, m.Msg, err)
		return
	}
	if err := m.Msg.Respond(resp); err != nil {
		slog.Error("failed to respond to favorite toggle", "error", err)
	}
}

func (h *Handler) handleFavoriteToggle(ctx context.Context, subj string, _ []byte) ([]byte, error) {
	account, roomID, ok := subject.ParseUserRoomSubject(subj)
	if !ok {
		return nil, fmt.Errorf("invalid favorite-toggle subject: %s", subj)
	}

	if span := trace.SpanFromContext(ctx); span.IsRecording() {
		span.SetAttributes(
			attribute.String("room.id", roomID),
			attribute.String("site.id", h.siteID),
		)
	}

	sub, err := h.store.ToggleSubscriptionFavorite(ctx, roomID, account)
	if err != nil {
		if errors.Is(err, model.ErrSubscriptionNotFound) {
			return nil, errNotRoomMember
		}
		return nil, fmt.Errorf("toggle subscription favorite: %w", err)
	}

	now := time.Now().UTC()

	subEvt := model.SubscriptionUpdateEvent{
		UserID:       sub.User.ID,
		Subscription: *sub,
		Action:       "favorite_toggled",
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
		payload := model.SubscriptionFavoriteToggledEvent{
			Account:   account,
			RoomID:    roomID,
			Favorite:  sub.Favorite,
			Timestamp: now.UnixMilli(),
		}
		payloadData, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("marshal favorite-toggled payload: %w", err)
		}
		outbox := model.OutboxEvent{
			Type:       model.OutboxSubscriptionFavoriteToggled,
			SiteID:     h.siteID,
			DestSiteID: userSiteID,
			Payload:    payloadData,
			Timestamp:  now.UnixMilli(),
		}
		outboxData, err := json.Marshal(outbox)
		if err != nil {
			return nil, fmt.Errorf("marshal outbox event: %w", err)
		}
		if err := h.publishToStream(ctx, subject.Outbox(h.siteID, userSiteID, model.OutboxSubscriptionFavoriteToggled), outboxData, ""); err != nil {
			return nil, fmt.Errorf("publish favorite-toggled outbox: %w", err)
		}
	}

	return json.Marshal(model.FavoriteToggleResponse{Status: "ok", Favorite: sub.Favorite})
}

// authorizeRoomAppRead allows the request iff the caller has a
// subscription in roomID OR is a platform admin in the local users
// collection AND the room actually exists. The room-existence check
// gates only the admin bypass — without it, an admin could query app
// metadata for a fabricated room ID and receive a plausible-looking
// response (e.g. a non-empty default-tabs list, or an empty cmd-menu
// list that looks like success). Cross-site admin authority is out of
// scope: an admin whose users document lives on a different site is
// denied.
func (h *Handler) authorizeRoomAppRead(ctx context.Context, account, roomID string) error {
	sub, err := h.store.GetSubscription(ctx, account, roomID)
	if err != nil && !errors.Is(err, model.ErrSubscriptionNotFound) {
		return fmt.Errorf("check room membership: %w", err)
	}
	if model.IsRoomMember(sub) {
		return nil
	}
	user, err := h.store.GetUser(ctx, account)
	if err != nil && !errors.Is(err, ErrUserNotFound) {
		return fmt.Errorf("check platform admin: %w", err)
	}
	if !model.IsPlatformAdmin(user) {
		return errAppAccessDenied
	}
	// Admin bypass: verify the room exists before allowing the read.
	// Without this, admins could query app metadata for fabricated room
	// IDs and get plausible-looking responses.
	if _, err := h.store.GetRoom(ctx, roomID); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return errAppAccessDenied
		}
		return fmt.Errorf("check room existence: %w", err)
	}
	return nil
}

// buildTabURL applies the SITE_URL-based scheme/host/path-prefix
// rewrite and the ${roomId}/${siteId} substitution to a channelTab URL
// template. Returns (url, true) on success; (_, false) when the
// template is empty, unparseable, or when siteURL is nil or the IDs
// fail the URL-safety check.
func (h *Handler) buildTabURL(tmpl, roomID string) (string, bool) {
	if tmpl == "" {
		return "", false
	}
	if h.siteURL == nil {
		return "", false
	}
	if !isURLSafeIDToken(roomID) || !isURLSafeIDToken(h.siteID) {
		return "", false
	}
	// Substitute BEFORE parsing so url.URL.String() doesn't percent-encode
	// the substituted values (roomID/siteID are URL-safe by construction).
	tmpl = strings.ReplaceAll(tmpl, "${roomId}", roomID)
	tmpl = strings.ReplaceAll(tmpl, "${siteId}", h.siteID)
	u, err := url.Parse(tmpl)
	if err != nil {
		return "", false
	}
	joined := h.siteURL.JoinPath(u.Path)
	joined.User = nil
	joined.RawQuery = u.RawQuery
	joined.Fragment = u.Fragment
	return joined.String(), true
}

func (h *Handler) handleGetRoomAppTabs(ctx context.Context, subj string, _ []byte) (model.GetRoomAppTabsResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	account, roomID, ok := subject.ParseUserRoomSubject(subj)
	if !ok {
		return model.GetRoomAppTabsResponse{}, errcode.BadRequest("invalid request")
	}

	if span := trace.SpanFromContext(ctx); span.IsRecording() {
		span.SetAttributes(
			attribute.String("room.id", roomID),
			attribute.String("site.id", h.siteID),
			attribute.String("account", account),
		)
	}

	if err := h.authorizeRoomAppRead(ctx, account, roomID); err != nil {
		return model.GetRoomAppTabsResponse{}, err
	}

	apps, err := h.store.ListDefaultChannelTabApps(ctx)
	if err != nil {
		return model.GetRoomAppTabsResponse{}, fmt.Errorf("list default channel-tab apps: %w", err)
	}

	out := make([]model.RoomApp, 0, len(apps))
	for i := range apps {
		app := &apps[i]
		if app.ChannelTab == nil {
			slog.Warn("skipping app with nil ChannelTab",
				"appId", app.ID, "roomId", roomID,
				"requestId", natsutil.RequestIDFromContext(ctx))
			continue
		}
		tabURL, ok := h.buildTabURL(app.ChannelTab.URL.Default, roomID)
		if !ok {
			slog.Warn("skipping app with empty or unparseable channelTab url",
				"appId", app.ID, "roomId", roomID,
				"requestId", natsutil.RequestIDFromContext(ctx))
			continue
		}
		out = append(out, model.RoomApp{
			ID:        app.ID,
			Name:      app.ChannelTab.Name,
			TabURL:    tabURL,
			Assistant: app.Assistant,
			AvatarURL: app.AvatarURL,
		})
	}
	return model.GetRoomAppTabsResponse{Apps: out}, nil
}

func (h *Handler) handleGetRoomAppCommandMenu(ctx context.Context, subj string, _ []byte) (model.GetRoomAppCommandMenuResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	account, roomID, ok := subject.ParseUserRoomSubject(subj)
	if !ok {
		return model.GetRoomAppCommandMenuResponse{}, errcode.BadRequest("invalid request")
	}

	if span := trace.SpanFromContext(ctx); span.IsRecording() {
		span.SetAttributes(
			attribute.String("room.id", roomID),
			attribute.String("site.id", h.siteID),
			attribute.String("account", account),
		)
	}

	if err := h.authorizeRoomAppRead(ctx, account, roomID); err != nil {
		return model.GetRoomAppCommandMenuResponse{}, err
	}

	bots, err := h.store.ListRoomBotApps(ctx, roomID)
	if err != nil {
		return model.GetRoomAppCommandMenuResponse{}, fmt.Errorf("list room bot apps: %w", err)
	}
	if span := trace.SpanFromContext(ctx); span.IsRecording() {
		span.SetAttributes(attribute.Int("bot.count", len(bots)))
	}

	if len(bots) == 0 {
		return model.GetRoomAppCommandMenuResponse{
			AppAssistants: make([]model.RoomAppAssistant, 0),
		}, nil
	}

	names := make([]string, 0, len(bots))
	for _, b := range bots {
		names = append(names, b.AssistantName)
	}
	menus, err := h.store.ListActiveCmdMenus(ctx, names)
	if err != nil {
		return model.GetRoomAppCommandMenuResponse{}, fmt.Errorf("list active cmd menus: %w", err)
	}
	byName := make(map[string][]model.CmdBlock, len(menus))
	for _, m := range menus {
		byName[m.Name] = m.CmdBlocks
	}

	out := make([]model.RoomAppAssistant, 0, len(bots))
	for _, b := range bots {
		out = append(out, model.RoomAppAssistant{
			AppName:   b.AppName,
			Name:      b.AssistantName,
			CmdBlocks: byName[b.AssistantName],
		})
	}
	return model.GetRoomAppCommandMenuResponse{AppAssistants: out}, nil
}

func (h *Handler) natsGetRoomAppCommandMenu(m otelnats.Msg) {
	ctx, err := wrappedCtx(m)
	if err != nil {
		errnats.Reply(ctx, m.Msg, err)
		return
	}
	resp, err := h.handleGetRoomAppCommandMenu(ctx, m.Msg.Subject, m.Msg.Data)
	if err != nil {
		errnats.Reply(ctx, m.Msg, err)
		return
	}
	h.replyBoundedJSON(ctx, m.Msg, resp)
}

func (h *Handler) natsGetRoomAppTabs(m otelnats.Msg) {
	ctx, err := wrappedCtx(m)
	if err != nil {
		errnats.Reply(ctx, m.Msg, err)
		return
	}
	resp, err := h.handleGetRoomAppTabs(ctx, m.Msg.Subject, m.Msg.Data)
	if err != nil {
		errnats.Reply(ctx, m.Msg, err)
		return
	}
	h.replyBoundedJSON(ctx, m.Msg, resp)
}
