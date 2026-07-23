package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	"github.com/hmchangw/chat/pkg/displayfmt"
	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/logctx"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgraph"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/outbox"
	"github.com/hmchangw/chat/pkg/roomkeystore"
	"github.com/hmchangw/chat/pkg/subject"
)

type Handler struct {
	store RoomStore
	// keyStore reads/writes room keys in the rooms collection (always wired in
	// production; tests may pass nil).
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

	// Microsoft Teams integration. graphClient is nil-safe: only the meetings
	// RPC uses it (the deep-link RPCs are pure string building). teamsEmailDomain
	// derives a member's email as account@domain. teamsMeetingStore backs the
	// per-room idempotency record (Mongo unique key on roomId+siteId).
	// roomMembersLimit / roomMembersCallLimit cap the member set for meetings and
	// calls respectively.
	graphClient          msgraph.Client
	teamsMeetingStore    TeamsMeetingStore
	teamsEmailDomain     string
	roomMembersLimit     int
	roomMembersCallLimit int
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

// Register wires every room-service RPC onto the natsrouter Router. The base
// middleware mints an X-Request-ID when absent (RequestID), so every handler has
// a request ID for logging without rejecting header-less server-to-server calls.
// Handlers that derive dedup keys from the request ID (e.g. roomRestricted) rely
// on callers sending a stable X-Request-ID across retries — see docs/client-api.md.
// Register/RegisterNoBody panic on subscription failure (fatal at startup).
func (h *Handler) Register(r *natsrouter.Router) {
	natsrouter.RegisterNoBody(r, subject.MuteTogglePattern(h.siteID), h.muteToggle)
	natsrouter.RegisterNoBody(r, subject.FavoriteTogglePattern(h.siteID), h.favoriteToggle)
	natsrouter.RegisterNoBody(r, subject.RoomAppTabsPattern(h.siteID), h.getRoomAppTabs)
	natsrouter.RegisterNoBody(r, subject.RoomAppCmdMenuPattern(h.siteID), h.getRoomAppCommandMenu)
	natsrouter.RegisterNoBody(r, subject.OrgMembersPattern(h.siteID), h.listOrgMembers)
	natsrouter.RegisterNoBody(r, subject.MemberListPattern(h.siteID), h.listMembers)
	natsrouter.RegisterNoBody(r, subject.MemberStatusesPattern(h.siteID), h.listMemberStatuses)
	natsrouter.RegisterNoBody(r, subject.MentionableSubscriptionsPattern(h.siteID), h.listMentionableSubscriptions)
	natsrouter.RegisterNoBody(r, subject.RoomKeyGetPattern(h.siteID), h.getRoomKey)
	natsrouter.RegisterNoBody(r, subject.MessageReadPattern(h.siteID), h.messageRead)
	natsrouter.Register(r, subject.MessageReadReceiptPattern(h.siteID), h.messageReadReceipt)
	natsrouter.Register(r, subject.MessageThreadReadPattern(h.siteID), h.messageThreadRead)
	natsrouter.Register(r, subject.MemberRoleUpdatePattern(h.siteID), h.updateRole)
	natsrouter.Register(r, subject.MemberRemovePattern(h.siteID), h.removeMember)
	natsrouter.Register(r, subject.MemberAddPattern(h.siteID), h.addMembers)
	natsrouter.Register(r, subject.RoomRenamePattern(h.siteID), h.roomRename)
	natsrouter.Register(r, subject.RoomRestricted(h.siteID), h.roomRestricted)
	natsrouter.Register(r, subject.RoomsInfoBatchSubscribe(h.siteID), h.roomsInfoBatch)
	natsrouter.Register(r, subject.ThreadRoomInfoBatch(h.siteID), h.threadRoomInfoBatch)
	natsrouter.Register(r, subject.RoomThreadReadAllSubscribe(h.siteID), h.clearAllThreadRead)
	natsrouter.Register(r, subject.RoomKeyEnsure(h.siteID), h.ensureRoomKey)
	natsrouter.Register(r, subject.RoomCreatePattern(h.siteID), h.createRoom)
	natsrouter.Register(r, subject.TeamsRoomCallPattern(h.siteID), h.teamsRoomCall)
	natsrouter.Register(r, subject.TeamsUserCallPattern(h.siteID), h.teamsUserCall)
	natsrouter.Register(r, subject.TeamsMeetingPattern(h.siteID), h.teamsMeeting)
}

func (h *Handler) createRoom(c *natsrouter.Context, req model.CreateRoomRequest) (*model.CreateRoomReply, error) { //nolint:gocritic // hugeParam: req is passed by value to satisfy the natsrouter.Register handler signature
	var ctx context.Context = c
	requesterAccount := c.Param("account")

	roomType, err := classifyAndValidate(&req, requesterAccount)
	if err != nil {
		return nil, err
	}
	// debug: the classified room type drives all downstream routing.
	slog.DebugContext(ctx, "room-service createRoom classified",
		"request_id", natsutil.RequestIDFromContext(ctx), "type", roomType)

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

	// A DM with no post-strip counterpart is a self-DM (classifyAndValidate only
	// emits RoomTypeDM with empty Users for that case). Handle it before the switch
	// so each switch case stays single-purpose.
	if roomType == model.RoomTypeDM && len(req.Users) == 0 {
		return h.handleCreateSelfDM(ctx, &req, requester)
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
			// Pre-strip set was [requester] and post-strip is empty → self-DM
			// (note-to-self). A DM with zero post-strip users is unambiguously
			// the self case; createRoom routes it to handleCreateSelfDM.
			return model.RoomTypeDM, nil
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
			if model.IsBot(a) || model.IsPlatformAdminAccount(a) {
				return "", errBotInChannel
			}
		}
	}

	return roomType, nil
}

// maxChannelNameRunes caps the rune length of a client-supplied channel name.
const maxChannelNameRunes = 100

// handleCreateSelfDM opens-or-creates the caller's one self-DM (note-to-self)
// through the same async create path as a 2-party DM: dedup, then publish the
// canonical create event with the deterministic self-DM room id. room-worker's
// processCreateRoom builds the single-member room. One-per-user is the same
// FindDMSubscription dedup the 2-party path uses (and the deterministic id makes
// a redelivery idempotent on the worker side).
func (h *Handler) handleCreateSelfDM(ctx context.Context, req *model.CreateRoomRequest, requester *model.User) (*model.CreateRoomReply, error) {
	existing, err := h.store.FindDMSubscription(ctx, requester.Account, requester.Account)
	if err == nil && existing != nil {
		return &model.CreateRoomReply{Status: model.CreateRoomStatusExists, RoomID: existing.RoomID}, nil
	}
	if err != nil && !errors.Is(err, model.ErrSubscriptionNotFound) {
		return nil, fmt.Errorf("self-dm dedup check: %w", err)
	}

	// Deterministic id (BuildDMRoomID with the requester on both sides); the
	// requester is its own counterpart, so the worker types this as a DM and
	// builds a single-member room.
	req.RoomID = idgen.BuildDMRoomID(requester.ID, requester.ID)
	req.Users = []string{requester.Account}
	req.ResolvedUsers = []string{requester.Account}
	return h.publishCreateRoom(ctx, req, requester, model.RoomTypeDM)
}

func (h *Handler) handleCreateRoomDMOrBotDM(ctx context.Context, req *model.CreateRoomRequest, requester *model.User, roomType model.RoomType) (*model.CreateRoomReply, error) {
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
		// debug: open-or-create short-circuit — the DM already exists.
		slog.DebugContext(ctx, "room-service DM exists, returning existing",
			"request_id", natsutil.RequestIDFromContext(ctx), "room_id", existing.RoomID)
		// DM already exists: this is a success ("open-or-create"), not an error.
		// Return the existing room ID so the client opens it. RoomType is left
		// empty on this branch, matching the prior error-reply behaviour.
		return &model.CreateRoomReply{
			Status: model.CreateRoomStatusExists,
			RoomID: existing.RoomID,
		}, nil
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

func (h *Handler) handleCreateRoomChannel(ctx context.Context, req *model.CreateRoomRequest, requester *model.User, requesterAccount string, roomType model.RoomType) (*model.CreateRoomReply, error) {
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
	// same reason as addMembers: the worker writes room_members + sys-msg
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

	// Preserve req.Users / req.Orgs as the literal request for room_created; the worker
	// uses ResolvedUsers / ResolvedOrgs for materialization and the members_added sys-msg.
	req.ResolvedUsers = allUsers
	req.ResolvedOrgs = allOrgs
	req.RoomID = idgen.GenerateID()
	return h.publishCreateRoom(ctx, req, requester, roomType)
}

func (h *Handler) publishCreateRoom(ctx context.Context, req *model.CreateRoomRequest, requester *model.User, roomType model.RoomType) (*model.CreateRoomReply, error) {
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

	// The room encryption key is a field of the room document and is provisioned
	// by room-worker when it inserts the room, so room-service no longer
	// pre-provisions it here.

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
	// flow: the sync RPC accepted and handed the room create off to room-worker.
	slog.Log(ctx, logctx.LevelFlow, "room-service create handoff", "phase", "published",
		"request_id", natsutil.RequestIDFromContext(ctx), "room_id", req.RoomID, "type", roomType)
	return &model.CreateRoomReply{
		Status:   model.CreateRoomReplyAccepted,
		RoomID:   req.RoomID,
		RoomType: string(roomType),
	}, nil
}

func (h *Handler) listOrgMembers(c *natsrouter.Context) (*model.ListOrgMembersResponse, error) {
	var ctx context.Context = c
	orgID := c.Param("orgID")
	members, err := h.store.ListOrgMembers(ctx, orgID)
	if err != nil {
		if errcode.HasReason(err, errcode.RoomInvalidOrg) {
			return nil, errcode.BadRequest("invalid org", errcode.WithReason(errcode.RoomInvalidOrg))
		}
		return nil, fmt.Errorf("get org members: %w", err)
	}
	return &model.ListOrgMembersResponse{Members: members}, nil
}

func (h *Handler) listMembers(c *natsrouter.Context) (*model.ListRoomMembersResponse, error) {
	var ctx context.Context = c
	requesterAccount := c.Param("account")
	roomID := c.Param("roomID")

	err := h.store.CheckMembership(ctx, requesterAccount, roomID)
	switch {
	case errors.Is(err, model.ErrSubscriptionNotFound):
		return nil, errNotRoomMember
	case err != nil:
		return nil, fmt.Errorf("check room membership: %w", err)
	}

	var req model.ListRoomMembersRequest
	if c.Msg != nil && len(c.Msg.Data) > 0 {
		if err := json.Unmarshal(c.Msg.Data, &req); err != nil {
			return nil, errcode.BadRequest("invalid request")
		}
	}
	if req.Limit != nil && *req.Limit <= 0 {
		return nil, errListLimitInvalid
	}
	if req.Offset != nil && *req.Offset < 0 {
		return nil, errListOffsetInvalid
	}

	members, err := h.store.ListRoomMembers(ctx, roomID, req.Limit, req.Offset, req.Enrich)
	if err != nil {
		return nil, fmt.Errorf("get room members: %w", err)
	}
	return &model.ListRoomMembersResponse{Members: members}, nil
}

func (h *Handler) getRoomKey(c *natsrouter.Context) (*model.RoomKeyGetResponse, error) {
	var ctx context.Context = c
	if h.keyStore == nil {
		return nil, fmt.Errorf("get room key: key store not configured")
	}
	requesterAccount := c.Param("account")
	roomID := c.Param("roomID")

	err := h.store.CheckMembership(ctx, requesterAccount, roomID)
	switch {
	case errors.Is(err, model.ErrSubscriptionNotFound):
		return nil, errNotRoomMember
	case err != nil:
		return nil, fmt.Errorf("check room membership: %w", err)
	}

	var req model.RoomKeyGetRequest
	if c.Msg != nil && len(c.Msg.Data) > 0 {
		if err := json.Unmarshal(c.Msg.Data, &req); err != nil {
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
		return &model.RoomKeyGetResponse{
			RoomID:     roomID,
			Version:    existing.Version,
			PrivateKey: existing.KeyPair.PrivateKey,
		}, nil
	}

	pair, err := h.keyStore.GetByVersion(ctx, roomID, *req.Version)
	if err != nil {
		return nil, fmt.Errorf("get room key: %w", err)
	}
	if pair == nil {
		return nil, errRoomKeyAbsent
	}
	// #nosec G117 -- RoomKeyGetResponse.PrivateKey is the intended payload: on-demand key delivery to the authorized room member over an auth-callout-gated per-user NATS subject, not a leak
	return &model.RoomKeyGetResponse{
		RoomID:     roomID,
		Version:    *req.Version,
		PrivateKey: pair.PrivateKey,
	}, nil
}

const (
	defaultMemberStatusesLimit = 3
	defaultMentionableLimit    = 3
)

// requireMembershipAndGetRoom checks the requester's room membership and
// loads the room document in parallel — both reads are independent and the
// second RTT is wasted on the happy path. Uses sync.WaitGroup (not
// errgroup.WithContext) so a fast GetRoom failure doesn't cancel
// GetSubscription and surface as context.Canceled, masking the real
// not-member sentinel. Membership errors take precedence over room-fetch
// errors so a non-member always sees errNotRoomMember regardless of which
// goroutine returns first. The subscription itself is discarded; callers
// only need the gate to pass.
func (h *Handler) requireMembershipAndGetRoom(ctx context.Context, account, roomID string) (*model.Room, error) {
	var (
		room    *model.Room
		subErr  error
		roomErr error
		wg      sync.WaitGroup
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		subErr = h.store.CheckMembership(ctx, account, roomID)
	}()
	go func() {
		defer wg.Done()
		room, roomErr = h.store.GetRoom(ctx, roomID)
	}()
	wg.Wait()
	if errors.Is(subErr, model.ErrSubscriptionNotFound) {
		return nil, errNotRoomMember
	}
	if subErr != nil {
		return nil, fmt.Errorf("check room membership: %w", subErr)
	}
	if roomErr != nil {
		return nil, fmt.Errorf("get room: %w", roomErr)
	}
	return room, nil
}

func (h *Handler) listMemberStatuses(c *natsrouter.Context) (*model.ListMemberStatusesResponse, error) {
	var ctx context.Context = c
	requesterAccount := c.Param("account")
	roomID := c.Param("roomID")
	if span := trace.SpanFromContext(ctx); span.IsRecording() {
		span.SetAttributes(
			attribute.String("room.id", roomID),
			attribute.String("site.id", h.siteID),
		)
	}

	var req model.ListMemberStatusesRequest
	if c.Msg != nil && len(c.Msg.Data) > 0 {
		if err := json.Unmarshal(c.Msg.Data, &req); err != nil {
			return nil, errcode.BadRequest("invalid request")
		}
	}

	room, err := h.requireMembershipAndGetRoom(ctx, requesterAccount, roomID)
	if err != nil {
		return nil, err
	}

	// Clamp the default to the room cap so a small no-limit room doesn't trip
	// the explicit-limit guard. Client-supplied values stay strictly validated.
	var limit int
	if req.Limit == nil {
		if room.UserCount == 0 {
			return &model.ListMemberStatusesResponse{Members: []model.MemberStatus{}}, nil
		}
		limit = min(defaultMemberStatusesLimit, room.UserCount)
	} else {
		limit = *req.Limit
		if limit <= 0 || limit > room.UserCount {
			return nil, errMemberStatusesLimitInvalid
		}
	}

	members, err := h.store.ListMemberStatuses(ctx, roomID, limit)
	if err != nil {
		return nil, fmt.Errorf("list member statuses: %w", err)
	}
	return &model.ListMemberStatusesResponse{Members: members}, nil
}

func (h *Handler) listMentionableSubscriptions(c *natsrouter.Context) (*model.MentionableSubscriptionsResponse, error) {
	var ctx context.Context = c
	requesterAccount := c.Param("account")
	roomID := c.Param("roomID")
	if span := trace.SpanFromContext(ctx); span.IsRecording() {
		span.SetAttributes(
			attribute.String("room.id", roomID),
			attribute.String("site.id", h.siteID),
		)
	}

	var req model.MentionableSubscriptionsRequest
	if c.Msg != nil && len(c.Msg.Data) > 0 {
		if err := json.Unmarshal(c.Msg.Data, &req); err != nil {
			return nil, errcode.BadRequest("invalid request")
		}
	}

	room, err := h.requireMembershipAndGetRoom(ctx, requesterAccount, roomID)
	if err != nil {
		return nil, err
	}

	mentionableCap := room.UserCount + room.AppCount
	if mentionableCap == 0 {
		return &model.MentionableSubscriptionsResponse{Subscriptions: []model.MentionableSubscription{}}, nil
	}
	var limit int
	if req.Limit == nil {
		limit = min(defaultMentionableLimit, mentionableCap)
	} else {
		limit = *req.Limit
		if limit <= 0 {
			return nil, errMentionableLimitInvalid
		}
		limit = min(limit, mentionableCap) // clamp over-cap instead of rejecting
	}

	// Filter is a literal substring. QuoteMeta escapes regex metacharacters
	// so a user typing "a.b" doesn't match every "a<any>b" account. Empty stays empty.
	escapedFilter := regexp.QuoteMeta(req.Filter)

	subs, err := h.store.ListMentionableSubscriptions(ctx, roomID, requesterAccount, escapedFilter, limit)
	if err != nil {
		return nil, fmt.Errorf("list mentionable subscriptions: %w", err)
	}
	return &model.MentionableSubscriptionsResponse{Subscriptions: subs}, nil
}

func (h *Handler) removeMember(c *natsrouter.Context, req model.RemoveMemberRequest) (*model.StatusReply, error) { //nolint:gocritic // hugeParam: req is passed by value to satisfy the natsrouter.Register handler signature
	var ctx context.Context = c
	requesterAccount := c.Param("account")
	roomID := c.Param("roomID")

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
		// Bot targets skip the last-member guard (can't orphan humans); the guard
		// counts humans only. The last-owner guard stays generic — a legacy
		// bot-owner must still not strand the room ownerless.
		targetIsBot := model.IsBot(req.Account) || model.IsPlatformAdminAccount(req.Account)
		targetIsOwner := hasRole(target.Subscription.Roles, model.RoleOwner)
		if !targetIsBot || targetIsOwner {
			counts, err := h.store.CountMembersAndOwners(ctx, roomID)
			if err != nil {
				return nil, fmt.Errorf("count members: %w", err)
			}
			if !targetIsBot && counts.HumanCount <= 1 {
				return nil, errCannotRemoveLastMember
			}
			if targetIsOwner && counts.OwnerCount <= 1 {
				return nil, errLastOwnerCannotLeave
			}
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
	data, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal remove member request: %w", err)
	}
	if err := h.publishToStream(ctx, subject.RoomCanonical(h.siteID, "member.remove"), data, ""); err != nil {
		return nil, fmt.Errorf("publish to stream: %w", err)
	}

	return &model.StatusReply{Status: "accepted"}, nil
}

func (h *Handler) updateRole(c *natsrouter.Context, req model.UpdateRoleRequest) (*model.StatusReply, error) {
	var ctx context.Context = c
	requester := c.Param("account")
	roomID := c.Param("roomID")
	if req.RoomID != "" && req.RoomID != roomID {
		return nil, errRoomIDMismatch
	}
	req.RoomID = roomID
	if req.NewRole != model.RoleOwner && req.NewRole != model.RoleMember {
		return nil, errInvalidRole
	}
	// Promote-only guard: demoting a legacy bot-owner back to member stays
	// allowed so operators can repair such rooms.
	if req.NewRole == model.RoleOwner && (model.IsBot(req.Account) || model.IsPlatformAdminAccount(req.Account)) {
		return nil, errBotCannotBeOwner
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
	// One instant shared by the origin write and the published event: the doc's
	// rolesUpdatedAt must equal the event timestamp so remote replicas guard against
	// the same high-water mark.
	now := time.Now().UTC()
	sub, err := h.store.SetOwnerRole(ctx, roomID, req.Account, req.NewRole == model.RoleOwner, now)
	if err != nil {
		if errors.Is(err, model.ErrSubscriptionNotFound) {
			return nil, errTargetNotMember // defensive: target removed between validation and mutate
		}
		return nil, fmt.Errorf("set owner role: %w", err)
	}

	// Role updates are channel-only (guarded above); the channel name is already in hand.
	subEvtData, err := h.publishSubscriptionUpdate(ctx, req.Account, "role_updated", sub, room.Name, now)
	if err != nil {
		return nil, err
	}

	userSiteID, err := h.store.GetUserSiteID(ctx, req.Account)
	if err != nil {
		return nil, fmt.Errorf("get user siteId: %w", err)
	}
	if userSiteID != "" && userSiteID != h.siteID {
		if err := h.federateOne(ctx, roomID, userSiteID, model.InboxRoleUpdated, subEvtData, req.Account, now.UnixMilli()); err != nil {
			return nil, fmt.Errorf("federate role-updated: %w", err)
		}
	}

	return &model.StatusReply{Status: "ok"}, nil
}

// publishSubscriptionUpdate best-effort publishes a SubscriptionUpdateEvent
// (sub, action, roomName) over core NATS; a publish failure is logged, not
// returned — the DB write is the source of truth and clients reconcile on next
// refetch. Returns the marshaled event so callers can reuse it (e.g. as a
// cross-site inbox payload).
func (h *Handler) publishSubscriptionUpdate(ctx context.Context, account, action string, sub *model.Subscription, roomName string, ts time.Time) ([]byte, error) {
	subEvt := model.SubscriptionUpdateEvent{
		UserID:       sub.User.ID,
		Subscription: *sub,
		Action:       action,
		RoomName:     roomName,
		Timestamp:    ts.UnixMilli(),
	}
	data, err := json.Marshal(subEvt)
	if err != nil {
		return nil, fmt.Errorf("marshal subscription update event: %w", err)
	}
	if err := h.publishCore(ctx, subject.SubscriptionUpdate(account), data); err != nil {
		slog.ErrorContext(ctx, "subscription update publish failed",
			"request_id", natsutil.RequestIDFromContext(ctx), "error", err, "account", account)
	}
	return data, nil
}

// federateOne durably relays one cross-site event onto the local OUTBOX stream
// (the durability boundary — only a local publish failure reaches the client);
// outbox-worker forwards it to destSiteID's INBOX. No-op when destSiteID is
// empty or local (outbox.Publish owns that guard, and the envelope build). The
// dedupID derived from dedupSeed is the OUTBOX publish's Nats-Msg-Id too, so a
// client retry can't double-enqueue the same (destination, event) into the
// outbox.
func (h *Handler) federateOne(ctx context.Context, roomID, destSiteID string, eventType model.InboxEventType, payload []byte, dedupSeed string, ts int64) error {
	dedupID := natsutil.InboxDedupID(ctx, destSiteID, dedupSeed)
	return outbox.Publish(ctx, h.publishToStream, h.siteID, roomID, destSiteID, eventType, payload, dedupID, ts)
}

func (h *Handler) addMembers(c *natsrouter.Context, req model.AddMembersRequest) (*model.StatusReply, error) { //nolint:gocritic // hugeParam: req is passed by value to satisfy the natsrouter.Register handler signature
	var ctx context.Context = c
	// 1. Subject params → requester, roomID
	requester := c.Param("account")
	roomID := c.Param("roomID")

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

	// 4. Cross-check optional body roomID against the subject roomID.
	if req.RoomID != "" && req.RoomID != roomID {
		return nil, errRoomIDMismatch
	}

	// Explicitly listed bots are admitted (create-channel still rejects them).
	// Each must resolve to an enabled app assistant and be same-site (the feed
	// is site-local). Deduped so a repeated bot costs one validation.
	for _, a := range dedup(req.Users) {
		if !model.IsBot(a) && !model.IsPlatformAdminAccount(a) {
			continue
		}
		app, err := h.store.GetApp(ctx, a)
		if err != nil {
			if errors.Is(err, ErrAppNotFound) {
				return nil, errBotNotAvailable
			}
			return nil, fmt.Errorf("get app for bot %s: %w", a, err)
		}
		if app.Assistant == nil || !app.Assistant.Enabled {
			return nil, errBotNotAvailable
		}
		botSiteID, err := h.store.GetUserSiteID(ctx, a)
		if err != nil {
			return nil, fmt.Errorf("get bot siteId: %w", err)
		}
		// Empty siteId is treated as local (legacy bot docs); a phantom account
		// is still rejected later by validateMembershipRefs.
		if botSiteID != "" && botSiteID != h.siteID {
			return nil, errBotCrossSite
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

	// 7/8. Capacity check. Short-circuit: with no orgs, the request adds at most
	// len(allUsers) new individuals (bot/dup/already-subscribed pruning can only
	// make it fewer), so when the room has headroom for that upper bound we
	// accept without the costlier CountNewMembers resolution. room.UserCount is
	// kept current by room-worker; a rare transient undercount can only
	// over-admit by the drift, and only matters near the cap — where the
	// condition below falls through to the exact count. Org requests have no
	// cheap upper bound, so they always take the precise path.
	newCount := -1 // -1 records the short-circuited case (capacity satisfied by the upper bound, not counted)
	if len(allOrgs) > 0 || room.UserCount+len(allUsers) > h.maxRoomSize {
		// Count net-new members (count-only — actual list materialized in room-worker).
		n, err := h.store.CountNewMembers(ctx, allOrgs, allUsers, roomID, "")
		if err != nil {
			return nil, fmt.Errorf("count new members: %w", err)
		}
		newCount = n

		// debug: how the requested refs resolved and the capacity arithmetic.
		slog.DebugContext(ctx, "room-service addMembers resolved",
			"request_id", natsutil.RequestIDFromContext(ctx), "room_id", roomID,
			"orgs", len(allOrgs), "users", len(allUsers), "new_count", newCount,
			"current_count", room.UserCount, "max_size", h.maxRoomSize)

		if room.UserCount+newCount > h.maxRoomSize {
			return nil, errcode.Conflict(
				fmt.Sprintf("room is at maximum capacity (%d): cannot add %d members to room with %d existing", h.maxRoomSize, newCount, room.UserCount),
				errcode.WithReason(errcode.RoomMaxSizeReached),
				errcode.WithMetadata("maxRoomSize", strconv.Itoa(h.maxRoomSize),
					"currentUserCount", strconv.Itoa(room.UserCount),
					"attempted", strconv.Itoa(room.UserCount+newCount)),
			)
		}
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
	// flow: accepted and handed the member-add off to room-worker.
	slog.Log(ctx, logctx.LevelFlow, "room-service member.add handoff", "phase", "published",
		"request_id", natsutil.RequestIDFromContext(ctx), "room_id", roomID, "new_count", newCount)

	// 10. Reply accepted
	return &model.StatusReply{Status: "accepted"}, nil
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
			if subErr := h.store.CheckMembership(refCtx, requester, ref.RoomID); subErr != nil {
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

func (h *Handler) roomsInfoBatch(c *natsrouter.Context, req model.RoomsInfoBatchRequest) (*model.RoomsInfoBatchResponse, error) {
	var ctx context.Context = c
	start := time.Now()
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

	return &model.RoomsInfoBatchResponse{Rooms: infos}, nil
}

func (h *Handler) threadRoomInfoBatch(c *natsrouter.Context, req model.ThreadRoomInfoBatchRequest) (*model.ThreadRoomInfoBatchResponse, error) {
	var ctx context.Context = c
	start := time.Now()
	if len(req.ThreadRoomIDs) == 0 {
		return nil, errcode.BadRequest("threadRoomIds must not be empty")
	}
	if len(req.ThreadRoomIDs) > h.maxBatchSize {
		return nil, errcode.BadRequest(fmt.Sprintf("batch size %d exceeds limit %d", len(req.ThreadRoomIDs), h.maxBatchSize))
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	rows, err := h.store.GetThreadRoomInfos(ctx, req.ThreadRoomIDs)
	if err != nil {
		return nil, fmt.Errorf("get thread room infos: %w", err)
	}
	byID := make(map[string]ThreadRoomInfoRow, len(rows))
	for _, r := range rows {
		byID[r.ThreadRoomID] = r
	}
	threads := make([]model.ThreadRoomInfo, 0, len(req.ThreadRoomIDs))
	for _, id := range req.ThreadRoomIDs {
		if r, ok := byID[id]; ok {
			threads = append(threads, model.ThreadRoomInfo{
				ThreadRoomID: id, Found: true,
				LastMsgAt: r.LastMsgAt.UTC().UnixMilli(),
			})
		} else {
			threads = append(threads, model.ThreadRoomInfo{ThreadRoomID: id, Found: false})
		}
	}
	slog.DebugContext(ctx, "thread room info batch handled",
		"site_id", h.siteID, "batch_size", len(req.ThreadRoomIDs),
		"request_id", natsutil.RequestIDFromContext(ctx),
		"latency_ms", time.Since(start).Milliseconds())
	return &model.ThreadRoomInfoBatchResponse{Threads: threads}, nil
}

// timePtrToMillis converts a nullable timestamp to UnixMilli for wire responses,
// returning nil for a nil or zero time so the field is omitted.
func timePtrToMillis(t *time.Time) *int64 {
	if t == nil || t.IsZero() {
		return nil
	}
	ms := t.UTC().UnixMilli()
	return &ms
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
		entry.UserCount = r.UserCount
		entry.AppCount = r.AppCount
		entry.LastMsgID = r.LastMsgID
		entry.LastMsgAt = timePtrToMillis(r.LastMsgAt)
		entry.LastMentionAllAt = timePtrToMillis(r.LastMentionAllAt)
		entry.MinUserLastSeenAt = timePtrToMillis(r.MinUserLastSeenAt)
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

func (h *Handler) messageRead(c *natsrouter.Context) (*model.StatusReply, error) {
	var ctx context.Context = c
	account := c.Param("account")
	roomID := c.Param("roomID")

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
		slog.Warn("user not found locally; skipping cross-site inbox", "account", account)
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
		if err := h.federateOne(ctx, roomID, userSiteID, model.InboxSubscriptionRead, payloadData, roomID+":"+account, now.UnixMilli()); err != nil {
			return nil, fmt.Errorf("federate subscription_read: %w", err)
		}
	}

	// Skip the room-floor recompute when the room has no content, or when
	// this user already had a recorded read past the latest message
	if room.LastMsgAt == nil {
		return &model.StatusReply{Status: "accepted"}, nil
	}
	if sub.LastSeenAt != nil && sub.LastSeenAt.After(*room.LastMsgAt) {
		return &model.StatusReply{Status: "accepted"}, nil
	}

	// Best-effort subscription.update to the reader's account (multi-device sync).
	if !model.IsBot(account) {
		updatedSub := *sub
		updatedSub.LastSeenAt = &now
		updatedSub.Alert = newAlert
		// Set the derived flags explicitly (don't rely on the projection omitting
		// them). Reading the room clears both hasMention and hasGroupMention.
		updatedSub.HasMention = false
		updatedSub.HasGroupMention = false
		// roomName omitted: clients don't use it on read events.
		if _, err := h.publishSubscriptionUpdate(ctx, account, "read", &updatedSub, "", now); err != nil {
			slog.Error("subscription update on read failed", "error", err,
				"request_id", natsutil.RequestIDFromContext(ctx), "account", account)
		}
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
		// Fan out the read-floor advance to clients. Best-effort: the floor write
		// above is the source of truth; a publish failure must not fail the RPC.
		switch room.Type {
		case model.RoomTypeChannel:
			h.publishChannelEvent(ctx, roomID, minTime)
		case model.RoomTypeDM:
			h.publishDMEvents(ctx, roomID, minTime)
		default:
			// botDM (floor is always nil) and other types get no read-floor
			// fan-out — only channel and dm rooms surface read receipts.
		}
	}

	return &model.StatusReply{Status: "accepted"}, nil
}

// buildMessageReadEvent constructs the wire payload announcing that a room's
// read floor advanced to floor (nil when no floor can be established).
func (h *Handler) buildMessageReadEvent(roomID string, floor *time.Time) model.MessageReadEvent {
	return model.MessageReadEvent{
		Type:              model.RoomEventMessageRead,
		RoomID:            roomID,
		MinUserLastSeenAt: floor,
		Timestamp:         time.Now().UTC().UnixMilli(),
	}
}

// publishChannelEvent fans a read-floor advance out once to the channel's shared
// room event subject. Best-effort: a marshal or publish failure is logged, not
// returned. Used for RoomTypeChannel.
func (h *Handler) publishChannelEvent(ctx context.Context, roomID string, floor *time.Time) {
	evt := h.buildMessageReadEvent(roomID, floor)
	payload, err := json.Marshal(evt)
	if err != nil {
		slog.Error("marshal message_read channel event failed", "error", err, "roomId", roomID)
		return
	}
	if err := h.publishCore(ctx, subject.RoomEvent(roomID), payload); err != nil {
		slog.Error("publish message_read channel event failed", "error", err, "roomId", roomID)
	}
}

// publishDMEvents fans a read-floor advance out to each DM member on their
// per-user event subject. Mirrors broadcast-worker's publishDMEvents: it lists
// the room's subscriptions and publishes once per subscriber. Best-effort per
// account; a list, marshal, or publish failure is logged, not returned. Used
// for RoomTypeDM.
func (h *Handler) publishDMEvents(ctx context.Context, roomID string, floor *time.Time) {
	subs, err := h.store.ListSubscriptionsByRoom(ctx, roomID)
	if err != nil {
		slog.Error("list subscriptions for message_read DM fan-out failed", "error", err, "roomId", roomID)
		return
	}
	evt := h.buildMessageReadEvent(roomID, floor)
	payload, err := json.Marshal(evt)
	if err != nil {
		slog.Error("marshal message_read DM event failed", "error", err, "roomId", roomID)
		return
	}
	for i := range subs {
		account := subs[i].User.Account
		if err := h.publishCore(ctx, subject.UserRoomEvent(account), payload); err != nil {
			slog.Error("publish message_read DM event failed", "error", err, "roomId", roomID, "account", account)
		}
	}
}

func (h *Handler) messageReadReceipt(c *natsrouter.Context, req model.ReadReceiptRequest) (*model.ReadReceiptResponse, error) {
	var ctx context.Context = c
	requesterAccount := c.Param("account")
	roomID := c.Param("roomID")

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
		meta     MessageReadMeta
		msgFound bool
		subErr   error
		msgErr   error
	)
	// Plain errgroup (no WithContext): both tasks always return nil and capture
	// their errors above, so we can enforce error precedence ourselves —
	// membership before the message lookup. The message is resolved via
	// history-service, so a history-service outage surfaces as msgErr
	// (errcode.Unavailable); checking subErr first means a non-member still gets
	// not_room_member rather than the unavailable error.
	var g errgroup.Group
	g.Go(func() error {
		subErr = h.store.CheckMembership(ctx, requesterAccount, roomID)
		return nil
	})
	g.Go(func() error {
		meta, msgFound, msgErr = h.msgReader.GetMessageReadMeta(ctx, requesterAccount, roomID, req.MessageID)
		return nil
	})
	_ = g.Wait()

	if subErr != nil {
		if errors.Is(subErr, model.ErrSubscriptionNotFound) {
			return nil, errNotRoomMember
		}
		return nil, fmt.Errorf("get subscription: %w", subErr)
	}
	if msgErr != nil {
		return nil, fmt.Errorf("get message: %w", msgErr)
	}
	if !msgFound {
		return nil, errMessageNotFound
	}
	// Belt-and-suspenders: history-service already scopes the lookup to roomID
	// (a wrong-room message comes back as not-found), so this guards only against
	// a future reader that does not pre-filter by room.
	if meta.RoomID != roomID {
		return nil, errMessageRoomMismatch
	}
	if meta.Sender != requesterAccount {
		return nil, errNotMessageSender
	}

	// A thread-only reply never appears in the channel, so channel read-position
	// isn't evidence of reading it — resolve readers from thread read-state instead (#443).
	var (
		rows []ReadReceiptRow
		err  error
	)
	if meta.ThreadOnly {
		rows, err = h.store.ListThreadReadReceipts(ctx, meta.ThreadRoomID, meta.CreatedAt, meta.Sender, h.maxRoomSize)
	} else {
		rows, err = h.store.ListReadReceipts(ctx, roomID, meta.CreatedAt, meta.Sender, h.maxRoomSize)
	}
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

	return &model.ReadReceiptResponse{Readers: entries}, nil
}

func (h *Handler) messageThreadRead(c *natsrouter.Context, req model.MessageThreadReadRequest) (*model.StatusReply, error) {
	var ctx context.Context = c
	account := c.Param("account")
	roomID := c.Param("roomID")

	if strings.TrimSpace(req.ThreadID) == "" {
		return nil, errInvalidThreadID
	}

	// Manual priority after Wait(): errNotRoomMember > thread-sub-missing no-op > internal errors.
	// Plain errgroup.Group (not WithContext) so a NotFound from one goroutine does NOT cancel
	// the siblings — otherwise context.Canceled in subErr/userSiteErr would outrank tsubErr.
	var (
		tsub                         *model.ThreadSubscription
		userSiteID                   string
		subErr, tsubErr, userSiteErr error
	)
	var g errgroup.Group
	g.Go(func() error {
		err := h.store.CheckMembership(ctx, account, roomID)
		subErr = err
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
		// The caller is a room member but does not follow this thread, so there is
		// no thread-read state to advance. Treat the mark-as-read as an idempotent
		// no-op and return success rather than surfacing an error to the client.
		return &model.StatusReply{Status: "accepted"}, nil
	case subErr != nil:
		return nil, fmt.Errorf("get subscription: %w", subErr)
	case tsubErr != nil:
		return nil, fmt.Errorf("get thread subscription: %w", tsubErr)
	case userSiteErr != nil:
		return nil, fmt.Errorf("get user siteId: %w", userSiteErr)
	}

	now := time.Now().UTC()

	var newThreadUnread []string
	var newAlert bool
	wg, wctx := errgroup.WithContext(ctx)
	wg.Go(func() error {
		var err error
		newThreadUnread, newAlert, err = h.store.UpdateSubscriptionThreadRead(wctx, roomID, account, req.ThreadID)
		if err != nil {
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
		slog.Warn("user not found locally; skipping cross-site inbox", "account", account)
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
		if err := h.federateOne(ctx, roomID, userSiteID, model.InboxThreadRead, payloadData, tsub.ThreadRoomID+":"+account, now.UnixMilli()); err != nil {
			return nil, fmt.Errorf("federate thread_read: %w", err)
		}
	}

	// Recompute the thread-room read floor, mirroring the room read-floor logic
	// in messageRead. Best-effort: a failure here must not fail the RPC.
	if err := h.recomputeThreadFloor(ctx, tsub.ThreadRoomID); err != nil {
		slog.ErrorContext(ctx, "recompute thread floor failed", "error", err,
			"request_id", natsutil.RequestIDFromContext(ctx), "threadRoomId", tsub.ThreadRoomID)
	}

	return &model.StatusReply{Status: "accepted"}, nil
}

// clearAllThreadRead clears every one of the account's thread-unread indicators on
// this site: thread-subscription read state (lastSeenAt=now, hasMention=false) and
// room-subscription thread-unread state (threadUnread removed, alert=false). It is
// the per-site leaf of the user-service clear-all-thread-unread aggregator. Unlike
// the single-thread path it deliberately skips the thread-room read-floor recompute
// and thread_message_read fan-out (a bulk dismiss must not advance sender receipts).
// For a cross-site user the whole dismiss rides one thread_read_all event, which
// inbox-worker applies as the same bulk clear on the user's home replica.
func (h *Handler) clearAllThreadRead(c *natsrouter.Context, req model.RoomThreadReadAllRequest) (*model.RoomThreadReadAllResponse, error) {
	var ctx context.Context = c
	account := strings.TrimSpace(req.Account)
	if account == "" {
		return nil, errcode.BadRequest("account is required")
	}
	c.WithLogValues("account", account)

	now := time.Now().UTC()

	var (
		homeSite                  string
		clearErr, subErr, siteErr error
	)
	var g errgroup.Group
	g.Go(func() error {
		clearErr = h.store.ClearThreadSubscriptionsForAccount(ctx, account, now)
		return clearErr
	})
	g.Go(func() error {
		subErr = h.store.ClearSubscriptionThreadUnreadForAccount(ctx, account)
		return subErr
	})
	g.Go(func() error {
		homeSite, siteErr = h.store.GetUserSiteID(ctx, account)
		return siteErr
	})
	_ = g.Wait()
	switch {
	case clearErr != nil:
		return nil, fmt.Errorf("clear thread subscriptions: %w", clearErr)
	case subErr != nil:
		return nil, fmt.Errorf("clear subscription thread-unread: %w", subErr)
	case siteErr != nil:
		return nil, fmt.Errorf("get user siteId: %w", siteErr)
	}

	switch {
	case homeSite == "":
		slog.WarnContext(ctx, "user not found locally; skipping cross-site inbox", "account", account)
	case homeSite != h.siteID:
		payload := model.ThreadReadAllEvent{
			Account:    account,
			LastSeenAt: now.UnixMilli(),
			Timestamp:  now.UnixMilli(),
		}
		data, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("marshal thread_read_all payload: %w", err)
		}
		// Not room-scoped: one bulk-dismiss event per remote peer, deduped by request ID.
		if err := h.federateOne(ctx, "", homeSite, model.InboxThreadReadAll, data, account, now.UnixMilli()); err != nil {
			return nil, fmt.Errorf("federate thread_read_all: %w", err)
		}
	}

	return &model.RoomThreadReadAllResponse{}, nil
}

// recomputeThreadFloor fetches the thread room document, applies the
// skip-guard (if the thread room has no messages yet, skip), computes
// MIN(lastSeenAt) across all thread_subscriptions, and writes the result
// to thread_rooms.minUserLastSeenAt when the floor changes, then fans out a
// best-effort thread_message_read event.
func (h *Handler) recomputeThreadFloor(ctx context.Context, threadRoomID string) error {
	tr, err := h.store.GetThreadRoomByID(ctx, threadRoomID)
	if err != nil {
		return fmt.Errorf("get thread room: %w", err)
	}
	if tr == nil {
		// Thread room not yet persisted — nothing to update.
		return nil
	}
	// Skip when the thread room has never received a message (mirrors
	// the room.LastMsgAt == nil guard in messageRead).
	if tr.LastMsgAt.IsZero() {
		return nil
	}

	minTime, err := h.store.MinThreadSubscriptionLastSeenByThreadRoomID(ctx, threadRoomID)
	if err != nil {
		return fmt.Errorf("min thread subscription lastSeenAt: %w", err)
	}
	if sameFloor(minTime, tr.MinUserLastSeenAt) {
		return nil
	}
	if err := h.store.UpdateThreadRoomMinUserLastSeenAt(ctx, threadRoomID, minTime); err != nil {
		return fmt.Errorf("update thread room minUserLastSeenAt: %w", err)
	}
	// Best-effort: the floor write above is the source of truth; a fan-out
	// failure must not fail the RPC.
	h.publishThreadMessageReadEvent(ctx, tr, minTime)
	return nil
}

// publishThreadMessageReadEvent fans a thread read-floor advance out to the
// parent room's audience, routed by the parent room's type. Best-effort: every
// get/list/marshal/publish failure is logged, never returned.
func (h *Handler) publishThreadMessageReadEvent(ctx context.Context, tr *model.ThreadRoom, floor *time.Time) {
	room, err := h.store.GetRoom(ctx, tr.RoomID)
	if err != nil {
		slog.Error("get parent room for thread_message_read fan-out failed", "error", err, "roomId", tr.RoomID, "threadRoomId", tr.ID)
		return
	}
	if room == nil {
		// Best-effort no-op on a missing parent room — never panic the RPC (GetThreadRoomByID can return (nil,nil)).
		return
	}
	switch room.Type {
	case model.RoomTypeChannel:
		h.publishThreadChannelEvent(ctx, tr, floor)
	case model.RoomTypeDM:
		h.publishThreadDMEvents(ctx, tr, floor)
	default:
		// botDM and other types get no read-floor fan-out, matching messageRead.
	}
}

// buildThreadMessageReadEvent constructs the wire payload announcing that a
// thread's read floor advanced to floor (nil when no floor can be established).
func (h *Handler) buildThreadMessageReadEvent(tr *model.ThreadRoom, floor *time.Time) model.ThreadMessageReadEvent {
	return model.ThreadMessageReadEvent{
		Type:              model.RoomEventThreadMessageRead,
		RoomID:            tr.RoomID,
		ThreadRoomID:      tr.ID,
		MinUserLastSeenAt: floor,
		Timestamp:         time.Now().UTC().UnixMilli(),
	}
}

// publishThreadChannelEvent fans the advance out once to the parent channel's
// room event subject. Used for a RoomTypeChannel parent.
func (h *Handler) publishThreadChannelEvent(ctx context.Context, tr *model.ThreadRoom, floor *time.Time) {
	payload, err := json.Marshal(h.buildThreadMessageReadEvent(tr, floor))
	if err != nil {
		slog.Error("marshal thread_message_read channel event failed", "error", err, "roomId", tr.RoomID, "threadRoomId", tr.ID)
		return
	}
	if err := h.publishCore(ctx, subject.RoomEvent(tr.RoomID), payload); err != nil {
		slog.Error("publish thread_message_read channel event failed", "error", err, "roomId", tr.RoomID, "threadRoomId", tr.ID)
	}
}

// publishThreadDMEvents fans the advance out to each DM member on their per-user
// event subject. Used for a RoomTypeDM parent.
func (h *Handler) publishThreadDMEvents(ctx context.Context, tr *model.ThreadRoom, floor *time.Time) {
	subs, err := h.store.ListSubscriptionsByRoom(ctx, tr.RoomID)
	if err != nil {
		slog.Error("list subscriptions for thread_message_read DM fan-out failed", "error", err, "roomId", tr.RoomID, "threadRoomId", tr.ID)
		return
	}
	payload, err := json.Marshal(h.buildThreadMessageReadEvent(tr, floor))
	if err != nil {
		slog.Error("marshal thread_message_read DM event failed", "error", err, "roomId", tr.RoomID, "threadRoomId", tr.ID)
		return
	}
	for i := range subs {
		account := subs[i].User.Account
		if err := h.publishCore(ctx, subject.UserRoomEvent(account), payload); err != nil {
			slog.Error("publish thread_message_read DM event failed", "error", err, "roomId", tr.RoomID, "threadRoomId", tr.ID, "account", account)
		}
	}
}

// ensureRoomKey handles server-to-server requests to ensure a room
// has an encryption key pair stored in its room document. Generates and stores
// a new pair if missing. The reply confirms the room and version but does not
// return key bytes — encryption/decryption is performed by broadcast-worker and
// clients, which read keys from the room store directly.
func (h *Handler) ensureRoomKey(c *natsrouter.Context, req model.RoomKeyEnsureRequest) (*model.RoomKeyEnsureResponse, error) {
	var ctx context.Context = c
	if h.keyStore == nil {
		// Local key store disabled — surfaces to peer sites as a transient outage
		// (symmetric with the timeout-class failures in :808/:819/:828).
		return nil, errcode.Unavailable("room key store not configured")
	}
	if req.RoomID == "" {
		return nil, errcode.BadRequest("roomId is required")
	}

	existing, err := h.keyStore.Get(ctx, req.RoomID)
	if err != nil {
		return nil, fmt.Errorf("ensure room key: get: %w", err)
	}
	if existing != nil {
		return &model.RoomKeyEnsureResponse{
			RoomID:  req.RoomID,
			Version: existing.Version,
		}, nil
	}

	newPair, err := roomkeystore.GenerateKeyPair()
	if err != nil {
		return nil, fmt.Errorf("ensure room key: generate key pair: %w", err)
	}
	ver, err := h.keyStore.Set(ctx, req.RoomID, *newPair)
	if err != nil {
		return nil, fmt.Errorf("ensure room key: set: %w", err)
	}
	return &model.RoomKeyEnsureResponse{
		RoomID:  req.RoomID,
		Version: ver,
	}, nil
}

func (h *Handler) roomRename(c *natsrouter.Context, req model.RoomRenameRequest) (*model.StatusWithRequestReply, error) {
	var ctx context.Context = c
	account := c.Param("account")
	roomID := c.Param("roomID")
	requestID := natsutil.RequestIDFromContext(c)

	// Client body carries only newName — roomID and account are taken from the
	// subject (the authoritative identity), never from the wire body.
	slog.Debug("processing room.rename",
		"op", model.AsyncJobOpRoomRename,
		"requester", account,
		"roomID", roomID,
		"request_id", requestID)

	name := strings.TrimSpace(req.NewName)
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

	if !model.IsPlatformAdmin(requesterUser) {
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
	return &model.StatusWithRequestReply{Status: "accepted", RequestID: requestID}, nil
}

// roomRestricted is the sync chat.server.> RPC. Account in the body is
// the audit identity (no subject prefix authenticates the caller — this RPC
// is server-side admin tooling). Mongo writes + sys-message publish + inbox
// fan-out happen inline; caller retries safely via dedup IDs.
func (h *Handler) roomRestricted(c *natsrouter.Context, req model.RoomRestrictedRequest) (*model.StatusWithRequestReply, error) {
	var ctx context.Context = c
	requestID := natsutil.RequestIDFromContext(c)

	if req.RoomID == "" || req.Account == "" {
		return nil, fmt.Errorf("%w: roomId and account are required", errInvalidRestrictedSubject)
	}

	// Admin-only RPC is rare; info-level audit trail is justified.
	slog.Info("processing room.restricted",
		"requester", req.Account,
		"roomID", req.RoomID,
		"request_id", requestID)

	requesterUser, getUserErr := h.store.GetUser(ctx, req.Account)
	if getUserErr != nil && !errors.Is(getUserErr, ErrUserNotFound) {
		return nil, fmt.Errorf("get user: %w", getUserErr)
	}
	if !model.IsPlatformAdmin(requesterUser) {
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
		if subErr := h.store.CheckMembership(ctx, req.OwnerAccount, req.RoomID); subErr != nil {
			if errors.Is(subErr, model.ErrSubscriptionNotFound) {
				return nil, errOwnerNotMember
			}
			return nil, fmt.Errorf("check owner membership: %w", subErr)
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
	if err := h.store.ApplySubscriptionRestriction(ctx, req.RoomID, req.Restricted, req.ExternalAccess, req.OwnerAccount, time.UnixMilli(req.Timestamp).UTC()); err != nil {
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
		return nil, fmt.Errorf("find users for inbox fan-out: %w", err)
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
		payload, err := json.Marshal(model.RoomRestrictedInboxPayload{
			RoomID:         req.RoomID,
			Restricted:     req.Restricted,
			ExternalAccess: req.ExternalAccess,
			OwnerAccount:   req.OwnerAccount,
			Timestamp:      req.Timestamp,
		})
		if err != nil {
			return nil, fmt.Errorf("marshal restricted inbox payload: %w", err)
		}
		for _, remoteSiteID := range remoteSites {
			if err := h.federateOne(ctx, req.RoomID, remoteSiteID, model.InboxRoomRestricted, payload, requestID, req.Timestamp); err != nil {
				return nil, fmt.Errorf("federate room_restricted to %s: %w", remoteSiteID, err)
			}
		}
	}

	return &model.StatusWithRequestReply{Status: "ok", RequestID: requestID}, nil
}

func (h *Handler) muteToggle(c *natsrouter.Context) (*model.MuteToggleResponse, error) {
	var ctx context.Context = c
	account := c.Param("account")
	roomID := c.Param("roomID")

	if span := trace.SpanFromContext(ctx); span.IsRecording() {
		span.SetAttributes(
			attribute.String("room.id", roomID),
			attribute.String("site.id", h.siteID),
		)
	}

	// One instant shared by the origin write and the published event: the doc's
	// muteUpdatedAt must equal the event timestamp so remote replicas guard against
	// the same high-water mark.
	now := time.Now().UTC()
	sub, err := h.store.ToggleSubscriptionMute(ctx, roomID, account, now)
	if err != nil {
		if errors.Is(err, model.ErrSubscriptionNotFound) {
			return nil, errNotRoomMember
		}
		return nil, fmt.Errorf("toggle subscription mute: %w", err)
	}

	// roomName omitted: the frontend doesn't use it for mute, so we avoid an extra lookup.
	if _, err := h.publishSubscriptionUpdate(ctx, account, "mute_toggled", sub, "", now); err != nil {
		return nil, err
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
		if err := h.federateOne(ctx, roomID, userSiteID, model.InboxSubscriptionMuteToggled, payloadData, roomID+":"+account, now.UnixMilli()); err != nil {
			return nil, fmt.Errorf("federate mute-toggled: %w", err)
		}
	}

	return &model.MuteToggleResponse{Status: "ok", Muted: sub.Muted}, nil
}

func (h *Handler) favoriteToggle(c *natsrouter.Context) (*model.FavoriteToggleResponse, error) {
	var ctx context.Context = c
	account := c.Param("account")
	roomID := c.Param("roomID")

	if span := trace.SpanFromContext(ctx); span.IsRecording() {
		span.SetAttributes(
			attribute.String("room.id", roomID),
			attribute.String("site.id", h.siteID),
		)
	}

	// One instant shared by the origin write and the published event: the doc's
	// favoriteUpdatedAt must equal the event timestamp so remote replicas guard
	// against the same high-water mark.
	now := time.Now().UTC()
	sub, err := h.store.ToggleSubscriptionFavorite(ctx, roomID, account, now)
	if err != nil {
		if errors.Is(err, model.ErrSubscriptionNotFound) {
			return nil, errNotRoomMember
		}
		return nil, fmt.Errorf("toggle subscription favorite: %w", err)
	}

	// roomName omitted: the frontend doesn't use it for favorite, so we avoid an extra lookup.
	if _, err := h.publishSubscriptionUpdate(ctx, account, "favorite_toggled", sub, "", now); err != nil {
		return nil, err
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
		if err := h.federateOne(ctx, roomID, userSiteID, model.InboxSubscriptionFavoriteToggled, payloadData, roomID+":"+account, now.UnixMilli()); err != nil {
			return nil, fmt.Errorf("federate favorite-toggled: %w", err)
		}
	}

	return &model.FavoriteToggleResponse{Status: "ok", Favorite: sub.Favorite}, nil
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

func (h *Handler) getRoomAppTabs(c *natsrouter.Context) (*model.GetRoomAppTabsResponse, error) {
	ctx, cancel := context.WithTimeout(c, 5*time.Second)
	defer cancel()

	account := c.Param("account")
	roomID := c.Param("roomID")

	if span := trace.SpanFromContext(ctx); span.IsRecording() {
		span.SetAttributes(
			attribute.String("room.id", roomID),
			attribute.String("site.id", h.siteID),
			attribute.String("account", account),
		)
	}

	if err := h.authorizeRoomAppRead(ctx, account, roomID); err != nil {
		return nil, err
	}

	apps, err := h.store.ListDefaultChannelTabApps(ctx)
	if err != nil {
		return nil, fmt.Errorf("list default channel-tab apps: %w", err)
	}

	out := make([]model.RoomApp, 0, len(apps))
	for i := range apps {
		app := &apps[i]
		if app.ChannelTab == nil {
			slog.Warn("skipping app with nil ChannelTab",
				"appId", app.ID, "roomId", roomID,
				"request_id", natsutil.RequestIDFromContext(ctx))
			continue
		}
		tabURL, ok := h.buildTabURL(app.ChannelTab.URL.Default, roomID)
		if !ok {
			slog.Warn("skipping app with empty or unparseable channelTab url",
				"appId", app.ID, "roomId", roomID,
				"request_id", natsutil.RequestIDFromContext(ctx))
			continue
		}
		out = append(out, model.RoomApp{
			ID:        app.ID,
			Name:      app.ChannelTab.Name,
			TabURL:    tabURL,
			Assistant: app.Assistant,
		})
	}
	return boundedReply(h, &model.GetRoomAppTabsResponse{Apps: out})
}

func (h *Handler) getRoomAppCommandMenu(c *natsrouter.Context) (*model.GetRoomAppCommandMenuResponse, error) {
	ctx, cancel := context.WithTimeout(c, 5*time.Second)
	defer cancel()

	account := c.Param("account")
	roomID := c.Param("roomID")

	if span := trace.SpanFromContext(ctx); span.IsRecording() {
		span.SetAttributes(
			attribute.String("room.id", roomID),
			attribute.String("site.id", h.siteID),
			attribute.String("account", account),
		)
	}

	if err := h.authorizeRoomAppRead(ctx, account, roomID); err != nil {
		return nil, err
	}

	bots, err := h.store.ListRoomBotApps(ctx, roomID)
	if err != nil {
		return nil, fmt.Errorf("list room bot apps: %w", err)
	}
	if span := trace.SpanFromContext(ctx); span.IsRecording() {
		span.SetAttributes(attribute.Int("bot.count", len(bots)))
	}

	if len(bots) == 0 {
		return &model.GetRoomAppCommandMenuResponse{
			AppAssistants: make([]model.RoomAppAssistant, 0),
		}, nil
	}

	names := make([]string, 0, len(bots))
	for _, b := range bots {
		names = append(names, b.AssistantName)
	}
	menus, err := h.store.ListActiveCmdMenus(ctx, names)
	if err != nil {
		return nil, fmt.Errorf("list active cmd menus: %w", err)
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
	return boundedReply(h, &model.GetRoomAppCommandMenuResponse{AppAssistants: out})
}
