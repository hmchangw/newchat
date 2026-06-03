package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/Marz32onE/instrumentation-go/otel-nats/otelnats"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errnats"
	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/roomkeymetrics"
	"github.com/hmchangw/chat/pkg/roomkeysender"
	"github.com/hmchangw/chat/pkg/roomkeystore"
	"github.com/hmchangw/chat/pkg/subject"
)

// errPermanent marks non-retryable errors (caller Acks instead of Nak).
// Aliased onto the consolidated errcode.ErrPermanent sentinel so the existing
// errors.Is(err, errPermanent) call sites (handler + ~18 test sites) keep
// working without churn.
var errPermanent = errcode.ErrPermanent

// errRoomKeyAbsent fires when keyStore.Get returns (nil, nil) — Valkey responded but the room
// has no current key. Distinct from transient Valkey errors so operators can alert separately.
var errRoomKeyAbsent = errors.New("room key absent")

// PublishFunc publishes data; non-empty msgID sets Nats-Msg-Id for JetStream stream-level dedup.
type PublishFunc func(ctx context.Context, subj string, data []byte, msgID string) error

// defaultKeyFanoutWorkers caps concurrent in-flight key publishes when the
// caller doesn't override via SetKeyFanoutWorkers. Sized to absorb common
// channel sizes (32 members fan out in one batch) without unbounded goroutine
// growth on giant rooms (10k members → 32 goroutines, not 10k).
const defaultKeyFanoutWorkers = 32

type Handler struct {
	store    SubscriptionStore
	siteID   string
	publish  PublishFunc
	keyStore RoomKeyStore
	// dekProvisioner is set in main when ATREST_ENABLED; nil disables eager
	// at-rest DEK creation for synchronously-created DM rooms (message-worker's
	// lazy create still covers them). Injected as a field rather than a
	// constructor arg to avoid churning every NewHandler caller.
	dekProvisioner   DEKProvisioner
	keySender        *roomkeysender.Sender
	keyFanoutWorkers int
}

func NewHandler(store SubscriptionStore, siteID string, publish PublishFunc, keyStore RoomKeyStore, keySender *roomkeysender.Sender) *Handler {
	return &Handler{
		store:            store,
		siteID:           siteID,
		publish:          publish,
		keyStore:         keyStore,
		keySender:        keySender,
		keyFanoutWorkers: defaultKeyFanoutWorkers,
	}
}

// publishSubscriptionUpdate fans out the per-user subscription.update event for the FE; best-effort.
func (h *Handler) publishSubscriptionUpdate(ctx context.Context, account string, subEvtData []byte) {
	if err := h.publish(ctx, subject.SubscriptionUpdate(account), subEvtData, ""); err != nil {
		slog.Error("subscription update publish failed", "error", err, "account", account)
	}
}

// SetKeyFanoutWorkers overrides the bounded-worker pool size used by
// fanOutKey. Values <= 0 are ignored so partial-deployment misconfig can't
// disable the cap. main wires this from KEY_FANOUT_WORKERS at startup.
func (h *Handler) SetKeyFanoutWorkers(n int) {
	if n > 0 {
		h.keyFanoutWorkers = n
	}
}

// messageDedupSeed returns the X-Request-ID from ctx, or payloadSeed when absent (partial-deployment safety, with a warn log).
func messageDedupSeed(ctx context.Context, handler, roomID, payloadSeed string) string {
	if seed := natsutil.RequestIDFromContext(ctx); seed != "" {
		return seed
	}
	slog.WarnContext(ctx, "missing X-Request-ID; falling back to payload-derived seed",
		"handler", handler, "room_id", roomID)
	return payloadSeed
}

// historySharedSincePtr returns nil for unrestricted history; req.Timestamp under HistoryModeNone.
func historySharedSincePtr(history model.HistoryConfig, timestamp int64, roomID string) *int64 {
	if history.Mode != model.HistoryModeNone {
		return nil
	}
	if timestamp <= 0 {
		slog.Error("restricted history with missing timestamp, emitting nil", "room_id", roomID, "mode", history.Mode)
		return nil
	}
	return &timestamp
}

// publishAsyncJobResult publishes a success/failure event to the requester's reply subject; best-effort.
func (h *Handler) publishAsyncJobResult(ctx context.Context, requesterAccount, operation, roomID string, jobErr error) {
	requestID := natsutil.RequestIDFromContext(ctx)
	if requestID == "" || requesterAccount == "" {
		return
	}
	result := model.AsyncJobResult{
		RequestID: requestID,
		Operation: operation,
		Status:    model.AsyncJobStatusOK,
		RoomID:    roomID,
		Timestamp: time.Now().UTC().UnixMilli(),
	}
	if jobErr != nil {
		// Enrich the ctx so fillAsyncError's single Classify log line carries these
		// fields at a category-aware level — no separate (ERROR-forced) log here.
		ctx = errcode.WithLogValues(ctx, "request_id", requestID, "operation", operation, "room_id", roomID)
		h.fillAsyncError(ctx, &result, jobErr)
	}
	data, _ := json.Marshal(result)
	if err := h.publish(ctx, subject.UserResponse(requesterAccount, requestID), data, ""); err != nil {
		slog.WarnContext(ctx, "publish async job result failed", "error", err, "request_id", requestID)
	}
}

// permanent wraps an *errcode.Error as a non-retryable job failure. Thin local
// alias for errcode.Permanent so call sites stay short — the marker type and
// sentinel-Is shim now live in pkg/errcode (Task 20.15).
func permanent(ec *errcode.Error) error { return errcode.Permanent(ec) }

// fillAsyncError classifies jobErr once and populates the result's error
// envelope fields. The Ack/Nak decision is INDEPENDENT of this — it stays keyed
// on the explicit errcode.Permanent marker (see HandleJetStreamMsg).
func (h *Handler) fillAsyncError(ctx context.Context, result *model.AsyncJobResult, jobErr error) {
	e := errcode.Classify(ctx, jobErr)
	result.Status = model.AsyncJobStatusError
	result.Error, result.Code, result.Reason = e.Message, string(e.Code), string(e.Reason)
}

// reconcileRoomOnDuplicateKey verifies the existing room is structurally compatible with the want spec; one source of truth for both create paths.
// The structural check (Type + SiteID match) is sufficient: the caller's
// BulkCreateSubscriptions runs idempotently (unique index dedups racing
// inserts; missing inserts are completed on retry). Crucially, this means a
// mid-write crash (CreateRoom succeeded but the worker died before
// BulkCreateSubscriptions) is recovered by JetStream redelivery — the retry
// finds the existing room, finishes the subscription writes, and the room
// is not orphaned.
func (h *Handler) reconcileRoomOnDuplicateKey(ctx context.Context, want *model.Room) (*model.Room, error) {
	existing, err := h.store.GetRoom(ctx, want.ID)
	if err != nil {
		return nil, fmt.Errorf("fetch existing room on duplicate-key: %w", err)
	}
	if existing.Type != want.Type || existing.SiteID != want.SiteID {
		// Conflict mirrors the sync-DM path's errRoomIDCollision; Classify then
		// logs at INFO instead of ERROR — this IS an expected data condition
		// (concurrent create with mismatched type), not a server fault.
		return nil, permanent(errcode.Conflict(fmt.Sprintf("room ID collision (existing type=%s site=%s; want %s/%s)",
			existing.Type, existing.SiteID, want.Type, want.SiteID)))
	}
	return existing, nil
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
	case strings.HasSuffix(subj, ".create"):
		err = h.processCreateRoom(ctx, msg.Data())
	case strings.HasSuffix(subj, ".room.rename"):
		err = h.processRoomRename(ctx, msg.Data())
	default:
		slog.WarnContext(ctx, "unknown member operation", "subject", subj)
	}
	if err != nil {
		// NB: do NOT slog.Error here — fillAsyncError → errcode.Classify already
		// logs the failure exactly once at a category-aware level (internal/
		// unavailable → ERROR, expected client errors → INFO). An extra ERROR
		// line here would double-log every failure and force ERROR on client-
		// category permanent errors (e.g. NotFound for missing user), defeating
		// the category-aware level the migration shipped.
		//
		// Permanent failures must Ack so JetStream stops redelivering. The async-job
		// error event has already been published to the requester via the per-handler
		// defer in processCreateRoom / processAddMembers / processRemove*. Permanence
		// is explicit (the errcode.Permanent marker), never inferred from the errcode
		// category — many permanent errors classify to internal and would otherwise
		// be Nak'd forever.
		if _, ok := errcode.IsPermanent(err); ok {
			if ackErr := msg.Ack(); ackErr != nil {
				slog.ErrorContext(ctx, "failed to ack permanent-error message", "error", ackErr)
			}
			return
		}
		if nakErr := msg.Nak(); nakErr != nil {
			slog.ErrorContext(ctx, "failed to nak message", "error", nakErr)
		}
		return
	}
	if err := msg.Ack(); err != nil {
		slog.ErrorContext(ctx, "failed to ack message", "error", err)
	}
}

func (h *Handler) processRoleUpdate(ctx context.Context, data []byte) error {
	var req model.UpdateRoleRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return permanent(errcode.BadRequest("unmarshal role update request"))
	}
	if req.Timestamp <= 0 {
		req.Timestamp = time.Now().UTC().UnixMilli()
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
		return permanent(errcode.BadRequest(fmt.Sprintf("unsupported role: %s", req.NewRole)))
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
		payloadSeed := fmt.Sprintf("%s:%s:%s:%d", req.RoomID, req.Account, req.NewRole, req.Timestamp)
		dedupID := natsutil.OutboxDedupID(ctx, user.SiteID, payloadSeed)
		if err := h.publish(ctx, outboxSubj, outboxData, dedupID); err != nil {
			return fmt.Errorf("publish outbox: %w", err)
		}
	}
	return nil
}

func (h *Handler) processRemoveMember(ctx context.Context, data []byte) (err error) {
	// Subhandlers (processRemoveOrg, processRemoveIndividual) own their own
	// async-result publish; dispatched=true tells our defer to skip publishing
	// on the happy path. Pre-dispatch failures (unmarshal, type-guard, key-get)
	// publish from here using the generic remove operation.
	var (
		requesterAccount string
		roomID           string
		dispatched       bool
	)
	defer func() {
		if dispatched {
			return
		}
		h.publishAsyncJobResult(ctx, requesterAccount, model.AsyncJobOpRoomMemberRemove, roomID, err)
	}()

	var req model.RemoveMemberRequest
	if err = json.Unmarshal(data, &req); err != nil {
		return permanent(errcode.BadRequest("unmarshal RemoveMemberRequest"))
	}
	requesterAccount = req.Requester
	roomID = req.RoomID

	// Pre-upgrade senders omit RoomType; treat zero value as channel since room-service validated it.
	if req.RoomType != "" && req.RoomType != model.RoomTypeChannel {
		return permanent(errcode.BadRequest(fmt.Sprintf("remove-member only valid on channel rooms, got %s", req.RoomType)))
	}
	// Removed-user-read window: between this canonical event being published and the Mongo
	// delete below, broadcast-worker may still address the removed user with the old key.
	// Accepted as a documented limitation; see docs/superpowers/specs/2026-05-08-room-encryption-keys-design.md.
	currentPair, err := h.keyStore.Get(ctx, req.RoomID)
	if err != nil {
		roomkeymetrics.ValkeyErrors.Add(ctx, 1, metric.WithAttributes(attribute.String("op", "Get")))
		return fmt.Errorf("get room key: %w", err)
	}

	dispatched = true
	if req.OrgID != "" {
		return h.processRemoveOrg(ctx, &req, currentPair)
	}
	return h.processRemoveIndividual(ctx, &req, currentPair)
}

// rotateAndFanOut generates v+1, fans it out to survivors, then commits via Valkey Rotate.
// Fan-out before Rotate is intentional so survivors hold v+1 before broadcast-worker switches.
// survivorAccounts is a pre-computed post-deletion snapshot of the room's member accounts.
func (h *Handler) rotateAndFanOut(ctx context.Context, roomID string, currentPair *roomkeystore.VersionedKeyPair, survivorAccounts []string) error {
	newPair, err := roomkeystore.GenerateKeyPair()
	if err != nil {
		return fmt.Errorf("generate room key: %w", err)
	}
	predictedVersion := 0
	if currentPair != nil {
		predictedVersion = currentPair.Version + 1
	}
	versioned := &roomkeystore.VersionedKeyPair{Version: predictedVersion, KeyPair: *newPair}
	h.fanOutRoomKeyToSurvivors(ctx, roomID, versioned, survivorAccounts)

	if currentPair == nil {
		if _, err := h.keyStore.Set(ctx, roomID, *newPair); err != nil {
			roomkeymetrics.ValkeyErrors.Add(ctx, 1, metric.WithAttributes(attribute.String("op", "Set")))
			return fmt.Errorf("store room key (no prior): %w", err)
		}
		return nil
	}
	if _, err := h.keyStore.Rotate(ctx, roomID, *newPair); err != nil {
		if errors.Is(err, roomkeystore.ErrNoCurrentKey) {
			// Fan-out already committed survivors to predictedVersion; persist at
			// the same version so broadcast-worker reads under the same key clients
			// hold. Using Set here would stamp v0 and create a version mismatch.
			if setErr := h.keyStore.SetWithVersion(ctx, roomID, *newPair, predictedVersion); setErr != nil {
				roomkeymetrics.ValkeyErrors.Add(ctx, 1, metric.WithAttributes(attribute.String("op", "SetWithVersion")))
				return fmt.Errorf("store room key (fallback): %w", setErr)
			}
			return nil
		}
		roomkeymetrics.ValkeyErrors.Add(ctx, 1, metric.WithAttributes(attribute.String("op", "Rotate")))
		return fmt.Errorf("rotate room key: %w", err)
	}
	return nil
}

func (h *Handler) processRemoveIndividual(ctx context.Context, req *model.RemoveMemberRequest, currentPair *roomkeystore.VersionedKeyPair) (err error) {
	if req.Timestamp <= 0 {
		req.Timestamp = time.Now().UTC().UnixMilli()
	}
	isSelfLeave := req.Requester == req.Account
	// Defer the result publish covers all subsequent return paths.
	defer func() {
		h.publishAsyncJobResult(ctx, req.Requester, model.AsyncJobOpRoomMemberRemove, req.RoomID, err)
	}()

	user, err := h.store.GetUserWithMembership(ctx, req.RoomID, req.Account)
	if err != nil {
		return fmt.Errorf("get user with membership: %w", err)
	}

	// room_members.member.id stores the user's internal ID, not the account.
	if err := h.store.DeleteRoomMember(ctx, req.RoomID, model.RoomMemberIndividual, user.ID); err != nil {
		return fmt.Errorf("delete room member (individual): %w", err)
	}

	// Dual-membership: user stays via org source; strip owner role (org members can't be owners). No rotation since no sub deleted.
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

	if err := h.store.ReconcileMemberCounts(ctx, req.RoomID); err != nil {
		return fmt.Errorf("reconcile member counts: %w", err)
	}

	// Rotate after delete + reconcile; GetSubscriptionAccounts returns the
	// post-deletion survivor accounts (projected — fan-out only needs accounts,
	// not full subscription docs).
	survivorAccounts, listErr := h.store.GetSubscriptionAccounts(ctx, req.RoomID)
	if listErr != nil {
		return fmt.Errorf("list survivors for key fan-out (room %s): %w", req.RoomID, listErr)
	}
	if err := h.rotateAndFanOut(ctx, req.RoomID, currentPair, survivorAccounts); err != nil {
		return fmt.Errorf("rotate and fan-out room key after remove-individual: %w", err)
	}

	now := time.Now().UTC()

	// Subscription update event. RoomType is fixed to channel: room-service
	// rejects member.remove for any other room kind.
	subEvt := model.SubscriptionRemovedEvent{
		UserID: user.ID,
		Subscription: model.RemovedSubscriptionRef{
			RoomID:   req.RoomID,
			RoomType: model.RoomTypeChannel,
			U:        model.SubscriptionUser{ID: user.ID, Account: req.Account},
		},
		Action:    "removed",
		Timestamp: now.UnixMilli(),
	}
	subEvtData, _ := json.Marshal(subEvt)
	h.publishSubscriptionUpdate(ctx, req.Account, subEvtData)

	// Member change event
	evtType := model.MessageTypeMemberLeft
	if !isSelfLeave {
		evtType = model.MessageTypeMemberRemoved
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
		slog.ErrorContext(ctx, "member event publish failed", "error", err, "room_id", req.RoomID)
	}

	// Wrapper Type collapses to member_removed even for self-leave so
	// search-sync-worker dispatches on one MV op; inner Type is preserved.
	inboxOutbox := model.OutboxEvent{
		Type:       model.OutboxMemberRemoved,
		SiteID:     h.siteID,
		DestSiteID: h.siteID,
		Payload:    memberEvtData,
		Timestamp:  now.UnixMilli(),
	}
	inboxData, _ := json.Marshal(inboxOutbox)
	inboxSeed := fmt.Sprintf("%s:%s:%d", req.RoomID, req.Account, req.Timestamp)
	if err := h.publish(ctx, subject.InboxMemberRemoved(h.siteID), inboxData, natsutil.OutboxDedupID(ctx, h.siteID, inboxSeed)); err != nil {
		slog.ErrorContext(ctx, "local inbox member_removed publish failed", "error", err, "room_id", req.RoomID)
	}

	// Sys-msg sender: leaving user for self-leave, requester for forced removal.
	requester := &user.User
	if !isSelfLeave {
		requester, err = h.store.GetUser(ctx, req.Requester)
		if err != nil {
			if errors.Is(err, ErrUserNotFound) {
				return permanent(errcode.NotFound(fmt.Sprintf("requester %s not found (room %s)", req.Requester, req.RoomID), errcode.WithReason(errcode.RoomUserNotFound)))
			}
			return fmt.Errorf("get requester: %w", err)
		}
	}
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
	seed := messageDedupSeed(ctx, "processRemoveIndividual", req.RoomID,
		fmt.Sprintf("%s:%s:%d", req.RoomID, req.Account, req.Timestamp))
	var content string
	if isSelfLeave {
		content = formatLeft(&user.User)
	} else {
		content = formatRemovedUser(&user.User)
	}
	sysMsg := model.Message{
		ID:          idgen.MessageIDFromRequestID(seed, "rmindiv"),
		RoomID:      req.RoomID,
		UserID:      requester.ID,
		UserAccount: requester.Account,
		Type:        evtType,
		Content:     content,
		SysMsgData:  sysMsgData,
		CreatedAt:   now,
	}
	msgEvt := model.MessageEvent{
		Event:     model.EventCreated,
		Message:   sysMsg,
		SiteID:    h.siteID,
		Timestamp: now.UnixMilli(),
	}
	msgEvtData, _ := json.Marshal(msgEvt)
	if err := h.publish(ctx, subject.MsgCanonicalCreated(h.siteID), msgEvtData, natsutil.CanonicalDedupID(&msgEvt)); err != nil {
		return fmt.Errorf("publish individual removal system message: %w", err)
	}

	// Cross-site outbox for federated users
	if user.SiteID != h.siteID {
		outbox := model.OutboxEvent{
			Type:       model.OutboxMemberRemoved,
			SiteID:     h.siteID,
			DestSiteID: user.SiteID,
			Payload:    memberEvtData,
			Timestamp:  now.UnixMilli(),
		}
		outboxData, _ := json.Marshal(outbox)
		payloadSeed := fmt.Sprintf("%s:%s:%d", req.RoomID, req.Account, req.Timestamp)
		dedupID := natsutil.OutboxDedupID(ctx, user.SiteID, payloadSeed)
		if err := h.publish(ctx, subject.Outbox(h.siteID, user.SiteID, model.OutboxMemberRemoved), outboxData, dedupID); err != nil {
			return fmt.Errorf("outbox publish to %s: %w", user.SiteID, err)
		}
	}

	return nil
}

func (h *Handler) processRemoveOrg(ctx context.Context, req *model.RemoveMemberRequest, currentPair *roomkeystore.VersionedKeyPair) (err error) {
	if req.Timestamp <= 0 {
		req.Timestamp = time.Now().UTC().UnixMilli()
	}
	// Defer the result publish covers all subsequent return paths.
	defer func() {
		h.publishAsyncJobResult(ctx, req.Requester, model.AsyncJobOpRoomMemberRemoveOrg, req.RoomID, err)
	}()

	members, err := h.store.GetOrgMembersWithIndividualStatus(ctx, req.RoomID, req.OrgID)
	if err != nil {
		return fmt.Errorf("get org members with individual status: %w", err)
	}

	// Single pass: dept wins on overlap; otherwise first sect row. Stash the
	// first sect candidate as we scan so we don't need a second pass — the
	// dept row (if any) overrides it. Name/TCName harvested from the
	// UNFILTERED members slice so they remain correct when every org member
	// also has an individual sub and toRemove ends up empty. The orgID
	// fallback in displayOrg/CombineWithFallback guarantees a non-empty
	// rendered string even when all names are empty, so an all-empty result
	// is no longer a permanent error.
	var name, tcName string
	var sectName, sectTCName string
	var sectFound bool
	for _, m := range members {
		if m.IsDept {
			name, tcName = m.Name, m.TCName
			break
		}
		if !sectFound {
			sectName, sectTCName = m.Name, m.TCName
			sectFound = true
		}
	}
	if name == "" && tcName == "" && sectFound {
		name, tcName = sectName, sectTCName
	}
	if name == "" && tcName == "" {
		slog.WarnContext(ctx, "org-remove: no name resolved from any member; falling back to orgID",
			"request_id", natsutil.RequestIDFromContext(ctx),
			"room_id", req.RoomID, "orgID", req.OrgID)
	}

	// Skip members who still have an individual row OR are still reachable
	// via another org row in the same room. The latter matters because this
	// PR's dept-aware matching lets the same user be covered by two org rows
	// concurrently (one matching their sectId, one matching their deptId);
	// removing one of those orgs must not orphan the user's subscription
	// while the sibling row still claims them as a member.
	var toRemove []OrgMemberStatus
	for _, m := range members {
		if m.HasIndividualMembership || m.HasOtherOrgMembership {
			continue
		}
		toRemove = append(toRemove, m)
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

	if err := h.store.ReconcileMemberCounts(ctx, req.RoomID); err != nil {
		return fmt.Errorf("reconcile member counts: %w", err)
	}

	// Rotate only when something was actually deleted; GetSubscriptionAccounts
	// returns the post-deletion survivor accounts (projected — fan-out only
	// needs accounts, not full subscription docs).
	if len(accounts) > 0 {
		survivorAccounts, listErr := h.store.GetSubscriptionAccounts(ctx, req.RoomID)
		if listErr != nil {
			return fmt.Errorf("list survivors for key fan-out (room %s): %w", req.RoomID, listErr)
		}
		if err := h.rotateAndFanOut(ctx, req.RoomID, currentPair, survivorAccounts); err != nil {
			return fmt.Errorf("rotate and fan-out room key after remove-org: %w", err)
		}
	}

	now := time.Now().UTC()

	// Publish per-account subscription update and collect cross-site accounts
	for _, m := range toRemove {
		subEvt := model.SubscriptionRemovedEvent{
			Subscription: model.RemovedSubscriptionRef{
				RoomID:   req.RoomID,
				RoomType: model.RoomTypeChannel,
				U:        model.SubscriptionUser{Account: m.Account},
			},
			Action:    "removed",
			Timestamp: now.UnixMilli(),
		}
		subEvtData, _ := json.Marshal(subEvt)
		h.publishSubscriptionUpdate(ctx, m.Account, subEvtData)
	}

	// Member change event with all removed accounts
	if len(accounts) > 0 {
		memberEvt := model.MemberRemoveEvent{
			Type:      model.OutboxMemberRemoved,
			RoomID:    req.RoomID,
			Accounts:  accounts,
			SiteID:    h.siteID,
			OrgID:     req.OrgID,
			Timestamp: now.UnixMilli(),
		}
		memberEvtData, _ := json.Marshal(memberEvt)
		if err := h.publish(ctx, subject.MemberEvent(req.RoomID), memberEvtData, ""); err != nil {
			slog.ErrorContext(ctx, "member event publish failed", "error", err, "room_id", req.RoomID)
		}

		inboxOutbox := model.OutboxEvent{
			Type:       model.OutboxMemberRemoved,
			SiteID:     h.siteID,
			DestSiteID: h.siteID,
			Payload:    memberEvtData,
			Timestamp:  now.UnixMilli(),
		}
		inboxData, _ := json.Marshal(inboxOutbox)
		inboxSeed := fmt.Sprintf("%s:%s:%d", req.RoomID, req.OrgID, req.Timestamp)
		if err := h.publish(ctx, subject.InboxMemberRemoved(h.siteID), inboxData, natsutil.OutboxDedupID(ctx, h.siteID, inboxSeed)); err != nil {
			slog.ErrorContext(ctx, "local inbox member_removed publish failed", "error", err, "room_id", req.RoomID)
		}
	}

	// Sys-msg sender is the requester.
	requester, err := h.store.GetUser(ctx, req.Requester)
	if err != nil {
		if errors.Is(err, ErrUserNotFound) {
			return permanent(errcode.NotFound(fmt.Sprintf("requester %s not found (room %s)", req.Requester, req.RoomID), errcode.WithReason(errcode.RoomUserNotFound)))
		}
		return fmt.Errorf("get requester: %w", err)
	}
	sysMsgPayload, _ := json.Marshal(model.MemberRemoved{
		OrgID:             req.OrgID,
		SectName:          displayOrg(name, tcName, req.OrgID),
		RemovedUsersCount: len(toRemove),
	})
	seed := messageDedupSeed(ctx, "processRemoveOrg", req.RoomID,
		fmt.Sprintf("%s:%s:%d", req.RoomID, req.OrgID, req.Timestamp))
	sysMsg := model.Message{
		ID:          idgen.MessageIDFromRequestID(seed, "rmorg"),
		RoomID:      req.RoomID,
		UserID:      requester.ID,
		UserAccount: requester.Account,
		Type:        model.MessageTypeMemberRemoved,
		Content:     formatRemovedOrg(name, tcName, req.OrgID),
		SysMsgData:  sysMsgPayload,
		CreatedAt:   now,
	}
	msgEvt := model.MessageEvent{
		Event:     model.EventCreated,
		Message:   sysMsg,
		SiteID:    h.siteID,
		Timestamp: now.UnixMilli(),
	}
	msgEvtData, _ := json.Marshal(msgEvt)
	if err := h.publish(ctx, subject.MsgCanonicalCreated(h.siteID), msgEvtData, natsutil.CanonicalDedupID(&msgEvt)); err != nil {
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
			Type:      model.OutboxMemberRemoved,
			RoomID:    req.RoomID,
			Accounts:  accounts,
			SiteID:    h.siteID,
			OrgID:     req.OrgID,
			Timestamp: now.UnixMilli(),
		}
		outbox := model.OutboxEvent{
			Type:       model.OutboxMemberRemoved,
			SiteID:     h.siteID,
			DestSiteID: destSiteID,
			Payload:    mustMarshal(evt),
			Timestamp:  now.UnixMilli(),
		}
		outboxData, _ := json.Marshal(outbox)
		payloadSeed := fmt.Sprintf("%s:%s:%d", req.RoomID, req.OrgID, req.Timestamp)
		dedupID := natsutil.OutboxDedupID(ctx, destSiteID, payloadSeed)
		if err := h.publish(ctx, subject.Outbox(h.siteID, destSiteID, model.OutboxMemberRemoved), outboxData, dedupID); err != nil {
			return fmt.Errorf("outbox publish to %s: %w", destSiteID, err)
		}
	}

	return nil
}

func (h *Handler) processAddMembers(ctx context.Context, data []byte) (err error) {
	// Defer must cover early failures; populate requesterAccount/roomID once available.
	var (
		requesterAccount string
		roomID           string
	)
	defer func() {
		h.publishAsyncJobResult(ctx, requesterAccount, model.AsyncJobOpRoomMemberAdd, roomID, err)
	}()

	var req model.AddMembersRequest
	if err = json.Unmarshal(data, &req); err != nil {
		return permanent(errcode.BadRequest("unmarshal add members request"))
	}
	requesterAccount = req.RequesterAccount
	roomID = req.RoomID
	if req.Timestamp <= 0 {
		req.Timestamp = time.Now().UTC().UnixMilli()
	}

	room, err := h.store.GetRoom(ctx, req.RoomID)
	if err != nil {
		return fmt.Errorf("get room: %w", err)
	}
	// Defensive channel-only guard.
	if room.Type != model.RoomTypeChannel {
		return permanent(errcode.BadRequest(fmt.Sprintf("add-member only valid on channel rooms, got %s", room.Type)))
	}

	// Resolve candidates and per-candidate flags (has-sub / has-individual-row).
	// Splits the writes into needSub (no subscription yet) and needIRM (no
	// individual room_members row yet, writeIndividuals-gated): this is what
	// makes the org→individual upgrade path work — alice already has a sub
	// from an earlier org expansion, but no individual row, so an explicit
	// re-add via req.Users only needs to write the missing IRM row.
	candidates, err := h.store.ListAddMemberCandidates(ctx, req.Orgs, req.Users, req.RoomID)
	if err != nil {
		return fmt.Errorf("list add-member candidates: %w", err)
	}

	// Fail closed: defaulting hadOrgsBefore=false on error would trigger spurious first-org backfill.
	hadOrgsBefore, err := h.store.HasOrgRoomMembers(ctx, req.RoomID)
	if err != nil {
		return fmt.Errorf("check existing org room members: %w", err)
	}
	writeIndividuals := len(req.Orgs) > 0 || hadOrgsBefore

	allowedIndividual := make(map[string]struct{}, len(req.Users))
	for _, acc := range req.Users {
		allowedIndividual[acc] = struct{}{}
	}

	// needSub = no sub yet; needIRM = no individual row yet (writeIndividuals-gated, req.Users only).
	needSub := make([]AddMemberCandidate, 0, len(candidates))
	needIRM := make([]AddMemberCandidate, 0, len(candidates))
	for _, c := range candidates {
		if !c.HasSubscription {
			needSub = append(needSub, c)
		}
		if writeIndividuals && !c.HasIndividualRoomMember {
			if _, ok := allowedIndividual[c.Account]; ok {
				needIRM = append(needIRM, c)
			}
		}
	}

	// Nothing to write: no new subs, no individual upgrades, no org rows.
	if len(needSub) == 0 && len(needIRM) == 0 && len(req.Orgs) == 0 {
		return nil
	}

	// Lookup-account set: anyone whose sub or individual row we'll write.
	lookupAccounts := make([]string, 0, len(needSub)+len(needIRM))
	seen := make(map[string]struct{}, len(needSub)+len(needIRM))
	for _, c := range needSub {
		if _, ok := seen[c.Account]; !ok {
			lookupAccounts = append(lookupAccounts, c.Account)
			seen[c.Account] = struct{}{}
		}
	}
	for _, c := range needIRM {
		if _, ok := seen[c.Account]; !ok {
			lookupAccounts = append(lookupAccounts, c.Account)
			seen[c.Account] = struct{}{}
		}
	}

	var userMap map[string]model.User
	if len(lookupAccounts) > 0 {
		users, err := h.store.FindUsersByAccounts(ctx, lookupAccounts)
		if err != nil {
			return fmt.Errorf("find users by accounts: %w", err)
		}
		userMap = make(map[string]model.User, len(users))
		for i := range users {
			userMap[users[i].Account] = users[i]
		}
		for _, acc := range lookupAccounts {
			if _, ok := userMap[acc]; !ok {
				return permanent(errcode.NotFound(fmt.Sprintf("user %s not found in room.member.add (room %s)", acc, req.RoomID), errcode.WithReason(errcode.RoomUserNotFound)))
			}
		}
	}

	requester, err := h.store.GetUser(ctx, req.RequesterAccount)
	if err != nil {
		if errors.Is(err, ErrUserNotFound) {
			return permanent(errcode.NotFound(fmt.Sprintf("requester %s not found (room %s)", req.RequesterAccount, req.RoomID), errcode.WithReason(errcode.RoomUserNotFound)))
		}
		return fmt.Errorf("get requester: %w", err)
	}

	// acceptedAt is the stable request-acceptance time (set by room-service).
	// It's used for every domain-level timestamp that must survive event replay
	// (subscription.JoinedAt, historySharedSince, room_members.Ts, system
	// message CreatedAt, MemberAddEvent.JoinedAt) so that reprocessing yields
	// the same values. `now` below is the event-emission time and is only used
	// for transient event metadata (Timestamp fields on outbound events).
	acceptedAt := time.UnixMilli(req.Timestamp).UTC()
	now := time.Now().UTC()

	// Build subscriptions for accounts that don't yet have one. RoomType is
	// fixed to channel: room-service rejects member.add for any other room
	// kind. actualAccounts mirrors needSub for downstream event payloads
	// (MemberAddEvent, sys-msg multi/single content).
	subs := make([]*model.Subscription, 0, len(needSub))
	actualAccounts := make([]string, 0, len(needSub))
	for _, c := range needSub {
		user := userMap[c.Account]
		// newSub stamps u.isBot from the account; room is the channel fetched by
		// req.RoomID so RoomType/SiteID/Name/ID all match the prior inline build.
		sub := newSub(idgen.GenerateUUIDv7(), &user, room, []model.Role{model.RoleMember}, room.Name, false, acceptedAt)
		// Resolve once via the shared helper so the local sub, the per-user
		// SubscriptionUpdateEvent fan-out, and the cross-site MemberAddEvent
		// all carry the same HistorySharedSince value.
		if ms := historySharedSincePtr(req.History, req.Timestamp, req.RoomID); ms != nil {
			t := time.UnixMilli(*ms).UTC()
			sub.HistorySharedSince = &t
		}
		subs = append(subs, sub)
		actualAccounts = append(actualAccounts, user.Account)
	}

	if len(subs) > 0 {
		if err := h.store.BulkCreateSubscriptions(ctx, subs); err != nil {
			return fmt.Errorf("bulk create subscriptions: %w", err)
		}
	}

	// Collect all room_member docs to write in a single bulk insert:
	// new individuals (from needSub ∩ req.Users) + individual upgrades
	// (needIRM = req.Users with existing sub but no IRM row) + new orgs +
	// (optional) backfill of existing subscribers. processedAccounts tracks
	// every account we've already issued a sub or individual row for, so the
	// backfill step below can skip them.
	roomMembers := make([]*model.RoomMember, 0, len(needIRM)+len(req.Orgs))
	processedAccounts := make(map[string]struct{}, len(needSub)+len(needIRM))
	for _, c := range needSub {
		processedAccounts[c.Account] = struct{}{}
	}
	for _, c := range needIRM {
		processedAccounts[c.Account] = struct{}{}
		user := userMap[c.Account]
		roomMembers = append(roomMembers, &model.RoomMember{
			ID:     idgen.GenerateUUIDv7(),
			RoomID: req.RoomID,
			Ts:     acceptedAt,
			Member: model.RoomMemberEntry{
				ID:      user.ID,
				Type:    model.RoomMemberIndividual,
				Account: user.Account,
			},
		})
	}
	for _, org := range req.Orgs {
		roomMembers = append(roomMembers, &model.RoomMember{
			ID:     idgen.GenerateUUIDv7(),
			RoomID: req.RoomID,
			Ts:     acceptedAt,
			Member: model.RoomMemberEntry{ID: org, Type: model.RoomMemberOrg},
		})
	}

	// Backfill existing subscribers into room_members only when orgs are
	// joining for the first time and we're starting to track individuals.
	// Backfill errors propagate: log-and-continue would silently corrupt
	// room_members (existing subs would never get IRM rows). Retry is safe —
	// subs are already written so needSub is empty, hadOrgsBefore stays false
	// until BulkCreateRoomMembers commits, and the backfill re-runs cleanly.
	if len(req.Orgs) > 0 && !hadOrgsBefore {
		existingAccounts, err := h.store.GetSubscriptionAccounts(ctx, req.RoomID)
		if err != nil {
			return fmt.Errorf("get subscription accounts for backfill: %w", err)
		}
		var backfillAccounts []string
		for _, account := range existingAccounts {
			if _, processed := processedAccounts[account]; !processed {
				backfillAccounts = append(backfillAccounts, account)
			}
		}
		if len(backfillAccounts) > 0 {
			backfillUsers, err := h.store.FindUsersByAccounts(ctx, backfillAccounts)
			if err != nil {
				return fmt.Errorf("find users for backfill: %w", err)
			}
			// Fail-hard if any requested account is missing. A partial result
			// would commit some IRM rows + flip hadOrgsBefore=true (once
			// BulkCreateRoomMembers writes the org row), after which no future
			// redelivery can repair the missing rows — backfill only fires on
			// the first-org transition. Better to halt and surface the stale
			// sub via the async-job error than to bake permanent divergence
			// between subscriptions and room_members.
			found := make(map[string]struct{}, len(backfillUsers))
			for i := range backfillUsers {
				found[backfillUsers[i].Account] = struct{}{}
			}
			for _, acc := range backfillAccounts {
				if _, ok := found[acc]; !ok {
					return permanent(errcode.NotFound(fmt.Sprintf("backfill user %s not found in room.member.add (room %s)", acc, req.RoomID), errcode.WithReason(errcode.RoomUserNotFound)))
				}
			}
			for i := range backfillUsers {
				roomMembers = append(roomMembers, &model.RoomMember{
					ID:     idgen.GenerateUUIDv7(),
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

	if len(roomMembers) > 0 {
		if err := h.store.BulkCreateRoomMembers(ctx, roomMembers); err != nil {
			return fmt.Errorf("bulk create room members (room %s): %w", req.RoomID, err)
		}
	}

	// 6. Reconcile userCount. Idempotent $set converges to the correct value
	// even under JetStream redelivery; an upstream log-and-continue would
	// leave the counter drifted forever, so we propagate the error.
	if err := h.store.ReconcileMemberCounts(ctx, req.RoomID); err != nil {
		return fmt.Errorf("reconcile member counts: %w", err)
	}

	// Publish subscription.update BEFORE room.key so clients have a sub entry to store the key under.
	for _, sub := range subs {
		subEvt := model.SubscriptionUpdateEvent{
			UserID:       sub.User.ID,
			Subscription: *sub,
			Action:       "added",
			Timestamp:    now.UnixMilli(),
		}
		subEvtData, _ := json.Marshal(subEvt)
		h.publishSubscriptionUpdate(ctx, sub.User.Account, subEvtData)
	}

	// Fan out the room key only to newly-subscribed accounts. Accounts in
	// needIRM already had a subscription (and thus already received the key
	// on their original add), so they don't need a fresh delivery here.
	// Get is intentionally post-Mongo: the key was created at room-create
	// time and is not re-rotated for adds, so we just fetch the current pair.
	newSubUsers := make([]model.User, 0, len(needSub))
	for _, c := range needSub {
		newSubUsers = append(newSubUsers, userMap[c.Account])
	}
	if len(newSubUsers) > 0 {
		pair, err := h.keyStore.Get(ctx, req.RoomID)
		if err != nil {
			roomkeymetrics.ValkeyErrors.Add(ctx, 1, metric.WithAttributes(attribute.String("op", "Get")))
			return fmt.Errorf("get room key for fan-out: %w", err)
		}
		if err := h.buildAndFanOutRoomKey(ctx, req.RoomID, pair, newSubUsers); err != nil {
			return fmt.Errorf("fan out room key: %w", err)
		}
	}

	// 8. Publish MemberAddEvent (actualAccounts was built above alongside subs).
	// Gate on "actual membership change visible to room": new individual subs
	// (actualAccounts) or new org rows (req.Orgs). The org→individual upgrade
	// path (only needIRM populated) writes the missing individual room_members
	// row silently — no membership state changed for the room itself, so
	// emitting an empty MemberAddEvent and a "added members to the channel"
	// sys-msg with no actual members listed would mislead end users.
	historySharedSince := historySharedSincePtr(req.History, req.Timestamp, req.RoomID)
	if len(actualAccounts) > 0 || len(req.Orgs) > 0 {
		memberAddEvt := model.MemberAddEvent{
			Type:               model.OutboxMemberAdded,
			RoomID:             req.RoomID,
			RoomName:           room.Name,
			RoomType:           room.Type,
			Accounts:           actualAccounts,
			SiteID:             room.SiteID,
			RequesterAccount:   req.RequesterAccount,
			JoinedAt:           req.Timestamp,
			HistorySharedSince: historySharedSince,
			Timestamp:          now.UnixMilli(),
		}
		memberAddData, _ := json.Marshal(memberAddEvt)
		if err := h.publish(ctx, subject.RoomMemberEvent(req.RoomID), memberAddData, ""); err != nil {
			slog.ErrorContext(ctx, "member add event publish failed",
				"error", err,
				"room_id", req.RoomID,
				"request_id", natsutil.RequestIDFromContext(ctx),
			)
		}

		if len(actualAccounts) > 0 {
			inboxOutbox := model.OutboxEvent{
				Type:       model.OutboxMemberAdded,
				SiteID:     room.SiteID,
				DestSiteID: room.SiteID,
				Payload:    memberAddData,
				Timestamp:  now.UnixMilli(),
			}
			inboxData, _ := json.Marshal(inboxOutbox)
			inboxSeed := fmt.Sprintf("%s:%s:%d", req.RoomID, req.RequesterAccount, req.Timestamp)
			if err := h.publish(ctx, subject.InboxMemberAdded(room.SiteID), inboxData, natsutil.OutboxDedupID(ctx, room.SiteID, inboxSeed)); err != nil {
				slog.ErrorContext(ctx, "local inbox member_added publish failed",
					"error", err,
					"room_id", req.RoomID,
					"request_id", natsutil.RequestIDFromContext(ctx),
				)
			}
		}

		membersAdded := model.MembersAdded{
			Individuals:     actualAccounts,
			Orgs:            req.Orgs,
			Channels:        req.Channels,
			AddedUsersCount: len(subs),
		}
		sysMsgData, _ := json.Marshal(membersAdded)
		seed := messageDedupSeed(ctx, "processAddMembers", req.RoomID,
			fmt.Sprintf("%s:%s:%d", req.RoomID, req.RequesterAccount, req.Timestamp))
		// Single form only for direct 1-user adds; org-bearing adds always use multi.
		content := formatAddedMulti(requester)
		if len(subs) == 1 && len(req.Orgs) == 0 {
			onlyUser := userMap[subs[0].User.Account]
			content = formatAddedSingle(requester, &onlyUser)
		}
		sysMsg := model.Message{
			ID:          idgen.MessageIDFromRequestID(seed, "addmembers"),
			RoomID:      req.RoomID,
			UserID:      requester.ID,
			UserAccount: requester.Account,
			Type:        model.MessageTypeMembersAdded,
			Content:     content,
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
		if err := h.publish(ctx, subject.MsgCanonicalCreated(room.SiteID), msgEvtData, natsutil.CanonicalDedupID(&msgEvt)); err != nil {
			return fmt.Errorf("publish add-members system message: %w", err)
		}
	}

	// 10. Outbox for cross-site members — one event per destination site.
	// Single-pass bucket: accounts → home site, skipping the local site. The map
	// keys are the distinct remote sites; each entry already carries the
	// per-site filtered account list, so the downstream loop is O(sites) not
	// O(sites × accounts). Sending the full list would over-pressure NATS and
	// ship subscription identities to sites that have no business knowing them,
	// even though inbox-worker would filter on the destination.
	accountsBySite := make(map[string][]string)
	for _, acc := range actualAccounts {
		siteID := userMap[acc].SiteID
		if siteID == "" || siteID == h.siteID {
			continue
		}
		accountsBySite[siteID] = append(accountsBySite[siteID], acc)
	}
	for destSiteID, siteAccounts := range accountsBySite {
		siteEvt := model.MemberAddEvent{
			Type:               model.OutboxMemberAdded,
			RoomID:             req.RoomID,
			RoomName:           room.Name,
			RoomType:           room.Type,
			Accounts:           siteAccounts,
			SiteID:             room.SiteID,
			RequesterAccount:   req.RequesterAccount,
			JoinedAt:           req.Timestamp,
			HistorySharedSince: historySharedSince,
			Timestamp:          now.UnixMilli(),
		}
		siteEvtData, _ := json.Marshal(siteEvt)
		outbox := model.OutboxEvent{
			Type: model.OutboxMemberAdded, SiteID: room.SiteID, DestSiteID: destSiteID,
			Payload: siteEvtData, Timestamp: now.UnixMilli(),
		}
		outboxData, _ := json.Marshal(outbox)
		payloadSeed := fmt.Sprintf("%s:%s:%d", req.RoomID, req.RequesterAccount, req.Timestamp)
		dedupID := natsutil.OutboxDedupID(ctx, destSiteID, payloadSeed)
		if err := h.publish(ctx, subject.Outbox(room.SiteID, destSiteID, model.OutboxMemberAdded), outboxData, dedupID); err != nil {
			return fmt.Errorf("outbox publish to %s failed: %w", destSiteID, err)
		}
	}

	return nil
}

func mustMarshal(v any) []byte {
	data, _ := json.Marshal(v)
	return data
}

// resolveRoomName: DM/botDM use empty string (per-subscriber name lives on
// Subscription.Name); channels use req.Name verbatim (room-service has
// already validated non-empty and ≤ 100 runes).
func resolveRoomName(req *model.CreateRoomRequest, roomType model.RoomType) string {
	if roomType == model.RoomTypeChannel {
		return req.Name
	}
	return ""
}

// buildDMSubs returns the two DM subs (each names the counterpart, IsSubscribed=false).

func buildDMSubs(requester, other *model.User, room *model.Room, acceptedAt time.Time) []*model.Subscription {
	return []*model.Subscription{
		newSub(idgen.GenerateUUIDv7(), requester, room, nil, other.Account, false, acceptedAt),
		newSub(idgen.GenerateUUIDv7(), other, room, nil, requester.Account, false, acceptedAt),
	}
}

// buildBotDMSubs returns the two botDM subs (human IsSubscribed=true, bot IsSubscribed=false).
func buildBotDMSubs(requester, bot *model.User, room *model.Room, acceptedAt time.Time) []*model.Subscription {
	return []*model.Subscription{
		newSub(idgen.GenerateUUIDv7(), requester, room, nil, bot.Account, true, acceptedAt),
		newSub(idgen.GenerateUUIDv7(), bot, room, nil, requester.Account, false, acceptedAt),
	}
}

// buildSelfDMSub builds the sole self-DM subscription: subscribed, self-named, favorited.
func buildSelfDMSub(user *model.User, room *model.Room, joinedAt time.Time) *model.Subscription {
	sub := newSub(idgen.GenerateUUIDv7(), user, room, nil, user.Account, true, joinedAt)
	sub.Favorite = true
	return sub
}

// newSub constructs a Subscription from its constituent parts.
func newSub(id string, user *model.User, room *model.Room, roles []model.Role,
	name string, isSubscribed bool, joinedAt time.Time) *model.Subscription {
	return &model.Subscription{
		ID:           id,
		User:         model.SubscriptionUser{ID: user.ID, Account: user.Account, IsBot: model.IsBotAccount(user.Account)},
		RoomID:       room.ID,
		SiteID:       room.SiteID,
		Roles:        roles,
		Name:         name,
		RoomType:     room.Type,
		IsSubscribed: isSubscribed,
		JoinedAt:     joinedAt,
	}
}

func (h *Handler) processCreateRoom(ctx context.Context, data []byte) (err error) {
	// Defer must cover early failures; populate requester/roomID as soon as we have them.
	var (
		requesterAccount string
		roomID           string
	)
	defer func() {
		h.publishAsyncJobResult(ctx, requesterAccount, model.AsyncJobOpRoomCreate, roomID, err)
	}()

	requestID := natsutil.RequestIDFromContext(ctx)

	var req model.CreateRoomRequest
	if err := json.Unmarshal(data, &req); err != nil {
		// Never interpolate err.Error() — json.SyntaxError embeds the offending
		// payload substring from an unauthenticated entry-point (see doc.go).
		return permanent(errcode.BadRequest("unmarshal create-room"))
	}
	requesterAccount = req.RequesterAccount
	roomID = req.RoomID

	// Gate: key MUST exist before any Mongo write.
	pair, err := h.keyStore.Get(ctx, req.RoomID)
	if err != nil {
		roomkeymetrics.ValkeyErrors.Add(ctx, 1, metric.WithAttributes(attribute.String("op", "Get")))
		return fmt.Errorf("get room key: %w", err)
	}
	if pair == nil {
		roomkeymetrics.KeyAbsentErrors.Add(ctx, 1)
		return permanent(errcode.Internal("room key absent", errcode.WithCause(errRoomKeyAbsent)))
	}

	requester, err := h.store.GetUser(ctx, req.RequesterAccount)
	if err != nil {
		if errors.Is(err, ErrUserNotFound) {
			return permanent(errcode.NotFound("requester not found", errcode.WithReason(errcode.RoomUserNotFound)))
		}
		return fmt.Errorf("get requester: %w", err)
	}

	roomType := determineRoomTypeFromPayload(&req)
	acceptedAt := time.UnixMilli(req.Timestamp).UTC()
	now := time.Now().UTC()

	room := &model.Room{
		ID:        req.RoomID,
		Name:      resolveRoomName(&req, roomType),
		Type:      roomType,
		SiteID:    h.siteID,
		CreatedAt: acceptedAt,
		UpdatedAt: acceptedAt,
	}

	// Fetch the DM/botDM counterpart upfront so the room can be inserted in a
	// single write with UIDs/Accounts populated, matching the sync DM path.
	var counterpart *model.User
	if roomType == model.RoomTypeDM || roomType == model.RoomTypeBotDM {
		counterpart, err = h.store.GetUser(ctx, req.Users[0])
		if err != nil {
			if errors.Is(err, ErrUserNotFound) {
				if roomType == model.RoomTypeBotDM {
					return permanent(errcode.NotFound("bot user not found", errcode.WithReason(errcode.RoomBotNotAvailable)))
				}
				return permanent(errcode.NotFound("counterpart not found", errcode.WithReason(errcode.RoomUserNotFound)))
			}
			return fmt.Errorf("get counterpart: %w", err)
		}
		room.UIDs, room.Accounts = model.BuildDMParticipants(requester, counterpart)
	}

	if err := h.store.CreateRoom(ctx, room); err != nil {
		if !mongo.IsDuplicateKeyError(err) {
			return fmt.Errorf("create room: %w", err)
		}
		existing, err := h.reconcileRoomOnDuplicateKey(ctx, room)
		if err != nil {
			return fmt.Errorf("reconcile room on duplicate-key: %w", err)
		}
		room = existing
	}

	switch roomType {
	case model.RoomTypeDM, model.RoomTypeBotDM:
		var subs []*model.Subscription
		if roomType == model.RoomTypeBotDM {
			subs = buildBotDMSubs(requester, counterpart, room, acceptedAt)
		} else {
			subs = buildDMSubs(requester, counterpart, room, acceptedAt)
		}
		if err := h.store.BulkCreateSubscriptions(ctx, subs); err != nil {
			return fmt.Errorf("bulk create subs: %w", err)
		}
		// Re-read canonical subs: BulkCreate is a $setOnInsert upsert, so on a
		// JetStream redelivery the in-memory _id/JoinedAt may not match the
		// persisted document. Hand the canonical pair to finishCreateRoom.
		requesterSub, counterpartSub, err := h.store.FindDMSubscriptionPair(ctx, room.ID, requester.Account)
		if err != nil {
			return fmt.Errorf("re-read DM subs after write: %w", err)
		}
		subs = []*model.Subscription{requesterSub, counterpartSub}
		return h.finishCreateRoom(ctx, &req, room, requester, pair, []model.User{*requester, *counterpart}, subs, requestID, now)
	case model.RoomTypeChannel:
		return h.processCreateRoomChannel(ctx, &req, room, requester, pair, requestID, acceptedAt, now)
	default:
		// Client-provided value — BadRequest is the right category (Classify
		// then logs at INFO, not ERROR).
		return permanent(errcode.BadRequest(fmt.Sprintf("unknown room type %q", roomType)))
	}
}

// determineRoomTypeFromPayload mirrors room-service's determineRoomType on the
// canonical payload. model.IsBotAccount classifies webhook-style bots (".bot"
// suffix or "p_" prefix) consistently with room-service/helper.go and pkg/pipelines.
func determineRoomTypeFromPayload(req *model.CreateRoomRequest) model.RoomType {
	if req.Name == "" && len(req.Orgs) == 0 && len(req.Channels) == 0 && len(req.Users) == 1 {
		if model.IsBotAccount(req.Users[0]) {
			return model.RoomTypeBotDM
		}
		return model.RoomTypeDM
	}
	return model.RoomTypeChannel
}

func (h *Handler) processCreateRoomChannel(ctx context.Context, req *model.CreateRoomRequest, room *model.Room, requester *model.User, pair *roomkeystore.VersionedKeyPair, requestID string, acceptedAt, now time.Time) error {
	// Pass requester.Account as excludeAccount so org-expansion can't re-
	// introduce the requester (who joins separately as owner). Mirrors the
	// room-service capacity-check exclusion exactly.
	accounts, err := h.store.ListNewMembersForNewRoom(ctx, req.ResolvedOrgs, req.ResolvedUsers, requester.Account)
	if err != nil {
		return fmt.Errorf("list new members: %w", err)
	}

	users, err := h.store.FindUsersByAccounts(ctx, accounts)
	if err != nil {
		return fmt.Errorf("find users: %w", err)
	}
	// FindUsersByAccounts can return a subset when an account doesn't exist.
	// Treat any missing account as a permanent error rather than silently
	// creating the room without that member — the requester would otherwise
	// see "ok" while observing a smaller room than they requested.
	userSet := make(map[string]struct{}, len(users))
	for i := range users {
		userSet[users[i].Account] = struct{}{}
	}
	for _, account := range accounts {
		if _, ok := userSet[account]; !ok {
			return permanent(errcode.NotFound(fmt.Sprintf("user %s not found", account), errcode.WithReason(errcode.RoomUserNotFound)))
		}
	}

	allUsers := make([]model.User, 0, len(users)+1)
	allUsers = append(allUsers, *requester)
	allUsers = append(allUsers, users...)

	subs := make([]*model.Subscription, 0, len(allUsers))
	for i := range allUsers {
		u := &allUsers[i]
		roles := []model.Role{model.RoleMember}
		if u.ID == requester.ID {
			roles = []model.Role{model.RoleOwner}
		}
		subs = append(subs, newSub(idgen.GenerateUUIDv7(), u, room, roles, room.Name, false, acceptedAt))
	}

	if err := h.store.BulkCreateSubscriptions(ctx, subs); err != nil {
		return fmt.Errorf("bulk create subs: %w", err)
	}

	if len(req.ResolvedOrgs) > 0 {
		allowedIndividual := make(map[string]struct{}, len(req.ResolvedUsers)+1)
		allowedIndividual[requester.Account] = struct{}{}
		for _, acc := range req.ResolvedUsers {
			allowedIndividual[acc] = struct{}{}
		}
		members := make([]*model.RoomMember, 0, len(subs)+len(req.ResolvedOrgs))
		for _, sub := range subs {
			if _, ok := allowedIndividual[sub.User.Account]; !ok {
				continue
			}
			members = append(members, &model.RoomMember{
				ID:     idgen.GenerateUUIDv7(),
				RoomID: room.ID,
				Ts:     acceptedAt,
				Member: model.RoomMemberEntry{ID: sub.User.ID, Type: model.RoomMemberIndividual, Account: sub.User.Account},
			})
		}
		for _, org := range req.ResolvedOrgs {
			members = append(members, &model.RoomMember{
				ID:     idgen.GenerateUUIDv7(),
				RoomID: room.ID,
				Ts:     acceptedAt,
				Member: model.RoomMemberEntry{ID: org, Type: model.RoomMemberOrg},
			})
		}
		if err := h.store.BulkCreateRoomMembers(ctx, members); err != nil {
			return fmt.Errorf("bulk create room members: %w", err)
		}
	}
	// No-orgs lite-mode: room_members stays empty until an org joins.
	// Membership is implicit in `subscriptions`; the first add-member that
	// brings in an org will backfill existing accounts (including the owner)
	// into `room_members`.

	return h.finishCreateRoom(ctx, req, room, requester, pair, allUsers, subs, requestID, now)
}

func (h *Handler) finishCreateRoom(ctx context.Context, req *model.CreateRoomRequest, room *model.Room, requester *model.User, pair *roomkeystore.VersionedKeyPair, allUsers []model.User, subs []*model.Subscription, requestID string, now time.Time) error {
	if err := h.store.ReconcileMemberCounts(ctx, room.ID); err != nil {
		return fmt.Errorf("reconcile member counts: %w", err)
	}

	// Task 35: subscription.update fan-out per sub
	for _, sub := range subs {
		evt := model.SubscriptionUpdateEvent{
			UserID:       sub.User.ID,
			Subscription: *sub,
			Action:       "added",
			Timestamp:    now.UnixMilli(),
		}
		data, err := json.Marshal(evt)
		if err != nil {
			slog.ErrorContext(ctx, "marshal subscription.update failed", "error", err, "account", sub.User.Account)
			continue
		}
		h.publishSubscriptionUpdate(ctx, sub.User.Account, data)
	}

	// Task 36: channel-only sys-messages
	if room.Type == model.RoomTypeChannel {
		if err := h.publishChannelSysMessages(ctx, req, room, requester, len(subs)-1, requestID, now); err != nil {
			return fmt.Errorf("publish sys messages: %w", err)
		}
	}

	accounts := make([]string, 0, len(subs))
	for _, sub := range subs {
		accounts = append(accounts, sub.User.Account)
	}
	inner := model.MemberAddEvent{
		Type:               model.OutboxMemberAdded,
		RoomID:             room.ID,
		RoomName:           room.Name,
		RoomType:           room.Type,
		Accounts:           accounts,
		SiteID:             room.SiteID,
		RequesterAccount:   requester.Account,
		JoinedAt:           req.Timestamp,
		HistorySharedSince: nil,
		Timestamp:          now.UnixMilli(),
	}
	innerData, _ := json.Marshal(inner)
	outbox := model.OutboxEvent{
		Type:       model.OutboxMemberAdded,
		SiteID:     room.SiteID,
		DestSiteID: room.SiteID,
		Payload:    innerData,
		Timestamp:  now.UnixMilli(),
	}
	outboxData, _ := json.Marshal(outbox)
	payloadSeed := fmt.Sprintf("%s:%s:%d", room.ID, requester.Account, req.Timestamp)
	if err := h.publish(ctx, subject.InboxMemberAdded(room.SiteID), outboxData, natsutil.OutboxDedupID(ctx, room.SiteID, payloadSeed)); err != nil {
		slog.ErrorContext(ctx, "local inbox member_added publish failed", "error", err, "room_id", room.ID, "request_id", requestID)
	}

	// Task 37: outbox per remote site
	remoteSiteAccounts := map[string][]string{}
	for i := range allUsers {
		u := &allUsers[i]
		if u.SiteID == h.siteID || u.SiteID == "" {
			continue
		}
		remoteSiteAccounts[u.SiteID] = append(remoteSiteAccounts[u.SiteID], u.Account)
	}
	for destSiteID, accounts := range remoteSiteAccounts {
		memberEvt := model.MemberAddEvent{
			Type:               model.OutboxMemberAdded,
			RoomID:             room.ID,
			RoomName:           room.Name,
			RoomType:           room.Type,
			Accounts:           accounts,
			SiteID:             room.SiteID,
			RequesterAccount:   requester.Account,
			JoinedAt:           req.Timestamp,
			HistorySharedSince: nil,
			Timestamp:          now.UnixMilli(),
		}
		memberData, _ := json.Marshal(memberEvt)
		memberEnvelope := model.OutboxEvent{
			Type:       model.OutboxMemberAdded,
			SiteID:     room.SiteID,
			DestSiteID: destSiteID,
			Payload:    memberData,
			Timestamp:  now.UnixMilli(),
		}
		memberOutboxData, _ := json.Marshal(memberEnvelope)
		memberSeed := fmt.Sprintf("%s:%s:%d", room.ID, requester.Account, req.Timestamp)
		if err := h.publish(ctx, subject.Outbox(room.SiteID, destSiteID, model.OutboxMemberAdded), memberOutboxData, natsutil.OutboxDedupID(ctx, destSiteID, memberSeed)); err != nil {
			return fmt.Errorf("publish member_added outbox to %s: %w", destSiteID, err)
		}
	}

	// Fan out current key to every local-site member. If this fails the room and
	// subscriptions are durable but no member received the initial key event;
	// NAK so JetStream retries the whole handler rather than persisting silent
	// missing-key state.
	if err := h.buildAndFanOutRoomKey(ctx, room.ID, pair, allUsers); err != nil {
		return fmt.Errorf("room key fan-out (room %s): %w", room.ID, err)
	}

	return nil
}

func (h *Handler) publishChannelSysMessages(ctx context.Context, req *model.CreateRoomRequest, room *model.Room, requester *model.User, addedUsersCount int, requestID string, now time.Time) error {
	acceptedAt := time.UnixMilli(req.Timestamp).UTC()

	sysData1, err := json.Marshal(model.RoomCreated{
		Name:            room.Name,
		Users:           req.Users,
		Orgs:            req.Orgs,
		Channels:        req.Channels,
		AddedUsersCount: addedUsersCount,
	})
	if err != nil {
		return fmt.Errorf("marshal room_created sys data: %w", err)
	}
	msg1 := model.Message{
		ID:          idgen.MessageIDFromRequestID(requestID, "room_created"),
		RoomID:      room.ID,
		UserID:      requester.ID,
		UserAccount: requester.Account,
		Type:        model.MessageTypeRoomCreated,
		Content:     "A new room has been created",
		SysMsgData:  sysData1,
		CreatedAt:   acceptedAt,
	}
	if err := h.publishCanonical(ctx, &msg1, room.SiteID, now); err != nil {
		return fmt.Errorf("publish room_created: %w", err)
	}

	sysData2, err := json.Marshal(model.MembersAdded{
		Individuals:     req.Users,
		Orgs:            req.Orgs,
		Channels:        req.Channels,
		AddedUsersCount: addedUsersCount,
	})
	if err != nil {
		return fmt.Errorf("marshal members_added sys data: %w", err)
	}
	msg2 := model.Message{
		ID:          idgen.MessageIDFromRequestID(requestID, "members_added"),
		RoomID:      room.ID,
		UserID:      requester.ID,
		UserAccount: requester.Account,
		Type:        model.MessageTypeMembersAdded,
		Content:     formatAddedMulti(requester),
		SysMsgData:  sysData2,
		CreatedAt:   acceptedAt.Add(time.Millisecond),
	}
	if err := h.publishCanonical(ctx, &msg2, room.SiteID, now); err != nil {
		return fmt.Errorf("publish members_added: %w", err)
	}
	return nil
}

func (h *Handler) publishCanonical(ctx context.Context, msg *model.Message, siteID string, now time.Time) error {
	evt := model.MessageEvent{
		Event:     model.EventCreated,
		Message:   *msg,
		SiteID:    siteID,
		Timestamp: now.UnixMilli(),
	}
	data, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal MessageEvent: %w", err)
	}
	return h.publish(ctx, subject.MsgCanonicalCreated(siteID), data, natsutil.CanonicalDedupID(&evt))
}

// Sync DM endpoint handlers (chat.server.request.room.{siteID}.create.dm).

var (
	errInvalidSyncDMRequest = errcode.BadRequest("invalid sync DM request")
	// errUserLookupFailed stays a raw error so Classify collapses it to internal
	// (the requester learns the room couldn't be created, not who is missing).
	errUserLookupFailed   = errors.New("user lookup failed")
	errCrossSiteRequester = errcode.BadRequest("requester is not on this site")
	// errRoomIDCollision is an unrecoverable structural collision: permanent so
	// the JetStream-driven create paths Ack, conflict so the client sees 409.
	errRoomIDCollision = permanent(errcode.Conflict("room id collision (existing room metadata mismatch)"))
)

// handleSyncCreateDM creates a DM, self-DM, or botDM room and returns the requester's subscription.
// Errors flow through the centralized errcode.Classify path (the legacy
// sanitizeSyncDMError helper was retired by the errcode migration).
func (h *Handler) handleSyncCreateDM(ctx context.Context, data []byte) (*model.SyncCreateDMReply, error) {
	requestID := natsutil.RequestIDFromContext(ctx)

	var req model.SyncCreateDMRequest
	if err := json.Unmarshal(data, &req); err != nil {
		// Single %w on the errcode sentinel preserves errors.Is identity;
		// the json.Unmarshal error text is folded in as %v so it surfaces in
		// Classify's server-side log line without adding a second errcode to
		// the chain (the semgrep no-multi-%w rule trips on two %w verbs).
		return nil, fmt.Errorf("%w: %v", errInvalidSyncDMRequest, err)
	}
	if err := validateSyncCreateDMShape(&req); err != nil {
		return nil, err
	}

	// Self-DM sends the same account twice; dedup so the lookup queries one account.
	accounts := []string{req.RequesterAccount}
	if req.OtherAccount != req.RequesterAccount {
		accounts = append(accounts, req.OtherAccount)
	}
	users, err := h.store.FindUsersByAccounts(ctx, accounts)
	if err != nil {
		return nil, fmt.Errorf("find dm users: %w", err)
	}
	byAccount := make(map[string]*model.User, len(users))
	for i := range users {
		byAccount[users[i].Account] = &users[i]
	}
	requester, ok := byAccount[req.RequesterAccount]
	if !ok {
		return nil, errUserLookupFailed
	}
	if requester.SiteID != h.siteID {
		return nil, errCrossSiteRequester
	}

	// Self-DM (requester == counterpart): a single-member room.
	if req.RequesterAccount == req.OtherAccount {
		return h.createSelfDM(ctx, requester, requestID)
	}

	other, ok := byAccount[req.OtherAccount]
	if !ok {
		return nil, errUserLookupFailed
	}

	// roomCreatedAt drives the room doc and the outbox dedup seed (must stay
	// stable across NATS retries). joinedAt drives the subscription's JoinedAt
	// field — on a dup-key retry it tracks the room's original creation time
	// so JetStream redelivery is idempotent (user-service guards against
	// genuine re-subscribe so we never need to refresh JoinedAt here).
	roomCreatedAt := time.Now().UTC()
	joinedAt := roomCreatedAt
	roomID := idgen.BuildDMRoomID(requester.ID, other.ID)

	uids, accounts := model.BuildDMParticipants(requester, other)

	// DMs/botDMs have a fixed 2-member roster — set counts at creation; no Reconcile needed.
	userCount, appCount := 2, 0
	if req.RoomType == model.RoomTypeBotDM {
		userCount, appCount = 1, 1
	}

	room := &model.Room{
		ID:        roomID,
		Name:      "",
		Type:      req.RoomType,
		SiteID:    h.siteID,
		UserCount: userCount,
		AppCount:  appCount,
		UIDs:      uids,
		Accounts:  accounts,
		CreatedAt: roomCreatedAt,
		UpdatedAt: roomCreatedAt,
	}
	// Provision the room's at-rest DEK before persisting the room so the first
	// message write doesn't pay the create cost. Blocking and provisioned first
	// so a Vault outage fails DM creation rather than persisting a room whose
	// DEK is absent; idempotent on NATS retries. message-worker's lazy creation
	// still covers pre-rollout rooms.
	if h.dekProvisioner != nil {
		if err := h.dekProvisioner.EnsureDEK(ctx, room.ID); err != nil {
			return nil, fmt.Errorf("provision at-rest DEK for DM room %s: %w", room.ID, err)
		}
	}
	if err := h.store.CreateRoom(ctx, room); err != nil {
		if !mongo.IsDuplicateKeyError(err) {
			return nil, fmt.Errorf("create room: %w", err)
		}
		existing, reconcileErr := h.reconcileRoomOnDuplicateKey(ctx, room)
		if reconcileErr != nil {
			// Permanent errors from reconcile mean an unrecoverable collision; the
			// sync-DM caller surfaces errRoomIDCollision verbatim, so map any
			// permanent error onto that sentinel and keep the rich detail in the log.
			if _, ok := errcode.IsPermanent(reconcileErr); ok {
				slog.ErrorContext(ctx, "sync DM: room ID collision",
					"room_id", room.ID,
					"request_id", requestID,
					"error", reconcileErr)
				return nil, errRoomIDCollision
			}
			return nil, fmt.Errorf("reconcile sync DM room on duplicate-key: %w", reconcileErr)
		}
		// Sync-path duplicate-key: forward-only — no UIDs/Accounts backfill on the existing room.
		room = existing
		joinedAt = existing.CreatedAt
	}

	// validateSyncCreateDMShape already gated this to {dm, botDM}. Both share
	// the same idempotent-insert path: BulkCreateSubscriptions does
	// $setOnInsert so a JetStream redelivery is a Mongo no-op, and the
	// subsequent FindDMSubscriptionPair returns the canonical persisted pair.
	var subs []*model.Subscription
	if req.RoomType == model.RoomTypeBotDM {
		subs = buildBotDMSubs(requester, other, room, joinedAt)
	} else {
		subs = buildDMSubs(requester, other, room, joinedAt)
	}
	if err := h.store.BulkCreateSubscriptions(ctx, subs); err != nil {
		return nil, fmt.Errorf("bulk create subs: %w", err)
	}
	requesterSub, otherSub, err := h.store.FindDMSubscriptionPair(ctx, room.ID, requester.Account)
	if err != nil {
		return nil, fmt.Errorf("re-read DM subs after write: %w", err)
	}

	h.publishSubscriptionUpdates(ctx, []*model.Subscription{requesterSub, otherSub}, requestID)

	// Outbox failure means the remote site won't learn about the room; fail the request.
	if err := h.publishSyncDMOutbox(ctx, room, requester, other, requesterSub.JoinedAt); err != nil {
		return nil, fmt.Errorf("publish room_created outbox: %w", err)
	}

	return &model.SyncCreateDMReply{Success: true, Subscription: *requesterSub}, nil
}

// createSelfDM creates a single-member self-DM: one favorited subscription in a
// channel-id dm room, no outbox. "One per user" is enforced by the caller.
func (h *Handler) createSelfDM(ctx context.Context, requester *model.User, requestID string) (*model.SyncCreateDMReply, error) {
	now := time.Now().UTC() // one stamp for room + sub; random id means no retry-idempotency concern.
	room := &model.Room{
		ID:        idgen.GenerateID(),
		Type:      model.RoomTypeDM,
		SiteID:    h.siteID,
		UserCount: 1,
		UIDs:      []string{requester.ID},
		Accounts:  []string{requester.Account},
		CreatedAt: now,
		UpdatedAt: now,
	}
	// Provision the at-rest DEK before persisting the room (see handleSyncCreateDM).
	if h.dekProvisioner != nil {
		if err := h.dekProvisioner.EnsureDEK(ctx, room.ID); err != nil {
			return nil, fmt.Errorf("provision at-rest DEK for self-DM room %s: %w", room.ID, err)
		}
	}
	if err := h.store.CreateRoom(ctx, room); err != nil {
		return nil, fmt.Errorf("create self-DM room %s for %s: %w", room.ID, requester.Account, err)
	}

	sub := buildSelfDMSub(requester, room, now)
	if err := h.store.BulkCreateSubscriptions(ctx, []*model.Subscription{sub}); err != nil {
		return nil, fmt.Errorf("create self-DM subscription: %w", err)
	}

	// No read-back: a fresh room id means a pure insert, so sub is what persisted.
	h.publishSubscriptionUpdates(ctx, []*model.Subscription{sub}, requestID)
	return &model.SyncCreateDMReply{Success: true, Subscription: *sub}, nil
}

func validateSyncCreateDMShape(req *model.SyncCreateDMRequest) error {
	switch req.RoomType {
	case model.RoomTypeDM, model.RoomTypeBotDM:
	default:
		return errInvalidSyncDMRequest
	}
	if req.RequesterAccount == "" || req.OtherAccount == "" {
		return errInvalidSyncDMRequest
	}
	// A bot can't DM itself: reject a self-botDM.
	if req.RequesterAccount == req.OtherAccount && req.RoomType == model.RoomTypeBotDM {
		return errInvalidSyncDMRequest
	}
	return nil
}

func (h *Handler) publishSubscriptionUpdates(ctx context.Context, subs []*model.Subscription, requestID string) {
	for _, sub := range subs {
		evt := model.SubscriptionUpdateEvent{
			UserID:       sub.User.ID,
			Subscription: *sub,
			Action:       "added",
			Timestamp:    time.Now().UTC().UnixMilli(),
		}
		data, err := json.Marshal(evt)
		if err != nil {
			slog.ErrorContext(ctx, "sync DM: marshal subscription.update failed",
				"error", err, "account", sub.User.Account, "request_id", requestID)
			continue
		}
		h.publishSubscriptionUpdate(ctx, sub.User.Account, data)
	}
}

// findRemoteSitesForAccounts looks up the home site of each account and returns
// the deduplicated set of remote sites (siteID != h.siteID). Empty in → empty out.
func (h *Handler) findRemoteSitesForAccounts(ctx context.Context, accounts []string) ([]string, error) {
	if len(accounts) == 0 {
		return []string{}, nil
	}
	users, err := h.store.FindUsersByAccounts(ctx, accounts)
	if err != nil {
		return nil, fmt.Errorf("find users by accounts: %w", err)
	}
	seen := make(map[string]struct{}, len(users))
	out := make([]string, 0, len(users))
	for i := range users {
		if users[i].SiteID == h.siteID {
			continue
		}
		if _, dup := seen[users[i].SiteID]; dup {
			continue
		}
		seen[users[i].SiteID] = struct{}{}
		out = append(out, users[i].SiteID)
	}
	return out, nil
}

func (h *Handler) processRoomRename(ctx context.Context, data []byte) (err error) {
	var requesterAccount, roomID string
	defer func() {
		h.publishAsyncJobResult(ctx, requesterAccount, model.AsyncJobOpRoomRename, roomID, err)
	}()

	requestID := natsutil.RequestIDFromContext(ctx)
	if requestID == "" {
		return permanent(errcode.BadRequest("missing X-Request-ID"))
	}
	if !idgen.IsValidUUID(requestID) {
		return permanent(errcode.BadRequest("invalid X-Request-ID: must be a hyphenated UUID"))
	}

	var req model.RenameRoomRequest
	if err = json.Unmarshal(data, &req); err != nil {
		return permanent(errcode.BadRequest(fmt.Sprintf("unmarshal rename request: %s", err.Error())))
	}
	requesterAccount, roomID = req.Account, req.RoomID
	slog.Info("processing room.rename",
		"op", model.AsyncJobOpRoomRename,
		"requester", req.Account,
		"roomID", req.RoomID,
		"requestID", requestID)

	if err = h.store.UpdateRoomName(ctx, req.RoomID, req.NewName); err != nil {
		if errors.Is(err, ErrRoomNotFound) {
			return permanent(errcode.NotFound("room not found"))
		}
		if errors.Is(err, ErrNotChannelRoom) {
			return permanent(errcode.BadRequest("rename is only allowed in channel rooms", errcode.WithReason(errcode.RoomNonChannelOperation)))
		}
		return fmt.Errorf("update room name: %w", err)
	}
	if err = h.store.UpdateSubscriptionNamesForRoom(ctx, req.RoomID, req.NewName); err != nil {
		return fmt.Errorf("update subscription names: %w", err)
	}

	sysData, err := json.Marshal(model.RoomRenamedSysData{NewName: req.NewName, ByAccount: req.Account})
	if err != nil {
		return fmt.Errorf("marshal sys data: %w", err)
	}
	requester, err := h.store.GetUser(ctx, req.Account)
	if err != nil && !errors.Is(err, ErrUserNotFound) {
		return fmt.Errorf("get requester for sys message: %w", err)
	}
	requesterLabel := req.Account
	if requester != nil {
		requesterLabel = displayName(requester)
	}
	msg := model.Message{
		ID:          idgen.MessageIDFromRequestID(requestID, "room_renamed"),
		RoomID:      req.RoomID,
		UserAccount: req.Account,
		Type:        model.MessageTypeRoomRenamed,
		Content:     fmt.Sprintf("%q renamed the channel to %q", requesterLabel, req.NewName),
		SysMsgData:  sysData,
		CreatedAt:   time.UnixMilli(req.Timestamp).UTC(),
	}
	if err = h.publishCanonical(ctx, &msg, h.siteID, time.Now().UTC()); err != nil {
		return fmt.Errorf("publish room_renamed sys message: %w", err)
	}

	// Single room-scoped event (the room_renamed sys message published above)
	// is sufficient — clients update their subscription state from the room
	// event without per-subscription fan-out.
	subs, err := h.store.ListByRoom(ctx, req.RoomID)
	if err != nil {
		return fmt.Errorf("list subscriptions: %w", err)
	}

	accounts := make([]string, 0, len(subs))
	for i := range subs {
		accounts = append(accounts, subs[i].User.Account)
	}
	remoteSites, err := h.findRemoteSitesForAccounts(ctx, accounts)
	if err != nil {
		return fmt.Errorf("find remote sites for outbox fan-out: %w", err)
	}
	renamedPayload, err := json.Marshal(model.RoomRenamedOutboxPayload{
		RoomID: req.RoomID, NewName: req.NewName, Timestamp: req.Timestamp,
	})
	if err != nil {
		return fmt.Errorf("marshal rename outbox payload: %w", err)
	}
	for _, remoteSiteID := range remoteSites {
		evt := model.OutboxEvent{
			Type: model.OutboxRoomRenamed, SiteID: h.siteID, DestSiteID: remoteSiteID,
			Payload: renamedPayload, Timestamp: time.Now().UTC().UnixMilli(),
		}
		evtData, mErr := json.Marshal(evt)
		if mErr != nil {
			return fmt.Errorf("marshal rename outbox event: %w", mErr)
		}
		if err = h.publish(ctx, subject.Outbox(h.siteID, remoteSiteID, model.OutboxRoomRenamed),
			evtData, natsutil.OutboxDedupID(ctx, remoteSiteID, requestID)); err != nil {
			return fmt.Errorf("publish rename outbox to %s: %w", remoteSiteID, err)
		}
	}
	return nil
}

func (h *Handler) publishSyncDMOutbox(ctx context.Context, room *model.Room, requester, other *model.User, joinedAt time.Time) error {
	if other.SiteID == "" || other.SiteID == h.siteID {
		return nil
	}

	now := time.Now().UTC().UnixMilli()
	memberEvt := model.MemberAddEvent{
		Type:             model.OutboxMemberAdded,
		RoomID:           room.ID,
		RoomName:         "",
		RoomType:         room.Type,
		Accounts:         []string{other.Account},
		SiteID:           room.SiteID,
		RequesterAccount: requester.Account,
		JoinedAt:         joinedAt.UnixMilli(),
		Timestamp:        now,
	}
	pData, err := json.Marshal(memberEvt)
	if err != nil {
		return fmt.Errorf("marshal member_added outbox payload: %w", err)
	}
	envelope := model.OutboxEvent{
		Type:       model.OutboxMemberAdded,
		SiteID:     room.SiteID,
		DestSiteID: other.SiteID,
		Payload:    pData,
		Timestamp:  now,
	}
	eData, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshal outbox envelope: %w", err)
	}
	// Dedup seed keys on room.CreatedAt (stable across retries and re-subscribes)
	// rather than joinedAt — botDM re-subscribes carry a fresh joinedAt that
	// would otherwise defeat JetStream dedup on a NATS request retry.
	payloadSeed := fmt.Sprintf("%s:%s:%d", room.ID, requester.Account, room.CreatedAt.UnixMilli())
	return h.publish(ctx,
		subject.Outbox(room.SiteID, other.SiteID, model.OutboxMemberAdded),
		eData,
		natsutil.OutboxDedupID(ctx, other.SiteID, payloadSeed),
	)
}

// requireDedupRequestID is the strict X-Request-ID gate used by sync entry
// points (natsServerCreateDM) whose downstream pipeline derives JetStream
// Nats-Msg-Id and message-ID dedup keys from the request ID. Silently minting
// would break client-retry dedup; see docs/error-handling.md §3a. Thin wrapper
// over natsutil.RequireRequestID so the test sits in the same package.
func requireDedupRequestID(ctx context.Context, headers nats.Header, subject string) (context.Context, string, error) {
	return natsutil.RequireRequestID(ctx, headers, subject)
}

// natsServerCreateDM is the NATS entry point for chat.server.request.room.{siteID}.create.dm.
func (h *Handler) natsServerCreateDM(m otelnats.Msg) {
	ctx, id, err := requireDedupRequestID(m.Context(), m.Msg.Header, m.Msg.Subject)
	if err != nil {
		errnats.Reply(errcode.WithLogValues(m.Context(), "subject", m.Msg.Subject), m.Msg, err)
		return
	}
	ctx = errcode.WithLogValues(ctx, "request_id", id, "subject", m.Msg.Subject)
	reply, err := h.handleSyncCreateDM(ctx, m.Msg.Data)
	if err != nil {
		errnats.Reply(ctx, m.Msg, err)
		return
	}
	natsutil.ReplyJSON(m.Msg, reply)
}

// fanOutRoomKeyToSurvivors sends the already-fetched room key to every survivor
// account (local + remote). NATS supercluster routes user-subjects to home
// sites. survivorAccounts is a pre-computed post-deletion snapshot supplied by
// the caller; pair must be non-nil.
func (h *Handler) fanOutRoomKeyToSurvivors(ctx context.Context, roomID string, pair *roomkeystore.VersionedKeyPair, survivorAccounts []string) {
	// PublicKey omitted: server-side only, read from Valkey by broadcast-worker.
	evt := model.RoomKeyEvent{
		RoomID:     roomID,
		Version:    pair.Version,
		PrivateKey: pair.KeyPair.PrivateKey,
	}
	h.fanOutKey(ctx, roomID, survivorAccounts, &evt)
}

// buildAndFanOutRoomKey publishes pair as a RoomKeyEvent to every account in users.
// Caller owns the Get; nil pair returns a permanent error as a defensive guard.
func (h *Handler) buildAndFanOutRoomKey(ctx context.Context, roomID string, pair *roomkeystore.VersionedKeyPair, users []model.User) error {
	if pair == nil {
		roomkeymetrics.KeyAbsentErrors.Add(ctx, 1)
		return permanent(errcode.Internal("room key absent", errcode.WithCause(errRoomKeyAbsent)))
	}
	// PublicKey omitted: server-side only, read from Valkey by broadcast-worker.
	evt := model.RoomKeyEvent{
		RoomID:     roomID,
		Version:    pair.Version,
		PrivateKey: pair.KeyPair.PrivateKey,
	}
	accounts := make([]string, len(users))
	for i := range users {
		accounts[i] = users[i].Account
	}
	h.fanOutKey(ctx, roomID, accounts, &evt)
	return nil
}

// fanOutKey distributes evt to every account using up to h.keyFanoutWorkers
// concurrent goroutines. The event is marshaled exactly once and the resulting
// bytes are published to each account — on a giant room this avoids one
// json.Marshal per recipient. Per-account errors are logged and counted via
// roomkeymetrics; partial fan-out is acceptable because JetStream redelivers on
// permanent failure and recipients are idempotent on key version.
//
// evt is taken by pointer so the 80-byte struct isn't copied per fan-out call;
// callers must not mutate it after passing it in.
func (h *Handler) fanOutKey(ctx context.Context, roomID string, accounts []string, evt *model.RoomKeyEvent) {
	if len(accounts) == 0 {
		return
	}
	data, err := h.keySender.Marshal(*evt)
	if err != nil {
		// Marshaling a RoomKeyEvent effectively never fails; if it somehow does,
		// no recipient can be served, so count the whole batch and bail. The
		// caller treats fan-out as best-effort and JetStream redelivers.
		slog.Error("marshal room key for fan-out", "error", err, "roomId", roomID, "accounts", len(accounts))
		roomkeymetrics.FanoutErrors.Add(ctx, int64(len(accounts)), metric.WithAttributes(attribute.String("roomId", roomID)))
		return
	}
	workers := h.keyFanoutWorkers
	if workers <= 0 {
		// Defensive default for tests and any future construction path that
		// bypasses NewHandler with a zero-value Handler — without this an
		// unbuffered semaphore deadlocks the first publish.
		workers = defaultKeyFanoutWorkers
	}
	if workers > len(accounts) {
		workers = len(accounts)
	}
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	for _, account := range accounts {
		sem <- struct{}{}
		wg.Add(1)
		go func(acct string) {
			defer func() {
				<-sem
				wg.Done()
			}()
			if err := h.keySender.SendData(acct, data); err != nil {
				slog.ErrorContext(ctx, "send room key", "error", err, "account", acct, "roomId", roomID)
				roomkeymetrics.FanoutErrors.Add(ctx, 1, metric.WithAttributes(attribute.String("roomId", roomID)))
			}
		}(account)
	}
	wg.Wait()
}
