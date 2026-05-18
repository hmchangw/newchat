package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/Marz32onE/instrumentation-go/otel-nats/otelnats"
	"github.com/nats-io/nats.go/jetstream"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/roomkeymetrics"
	"github.com/hmchangw/chat/pkg/roomkeysender"
	"github.com/hmchangw/chat/pkg/roomkeystore"
	"github.com/hmchangw/chat/pkg/subject"
)

// errPermanent marks non-retryable errors (caller Acks instead of Nak).
var errPermanent = errors.New("permanent")

// errRoomKeyAbsent fires when keyStore.Get returns (nil, nil) — Valkey responded but the room
// has no current key. Distinct from transient Valkey errors so operators can alert separately.
var errRoomKeyAbsent = errors.New("room key absent")

// PublishFunc publishes data; non-empty msgID sets Nats-Msg-Id for JetStream stream-level dedup.
type PublishFunc func(ctx context.Context, subj string, data []byte, msgID string) error

type Handler struct {
	store     SubscriptionStore
	siteID    string
	publish   PublishFunc
	keyStore  RoomKeyStore
	keySender *roomkeysender.Sender
}

func NewHandler(store SubscriptionStore, siteID string, publish PublishFunc, keyStore RoomKeyStore, keySender *roomkeysender.Sender) *Handler {
	return &Handler{store: store, siteID: siteID, publish: publish, keyStore: keyStore, keySender: keySender}
}

// messageDedupSeed returns the X-Request-ID from ctx, or payloadSeed when absent (partial-deployment safety, with a warn log).
func messageDedupSeed(ctx context.Context, handler, roomID, payloadSeed string) string {
	if seed := natsutil.RequestIDFromContext(ctx); seed != "" {
		return seed
	}
	slog.Warn("missing X-Request-ID; falling back to payload-derived seed",
		"handler", handler, "roomID", roomID)
	return payloadSeed
}

// historySharedSincePtr returns nil for unrestricted history; req.Timestamp under HistoryModeNone.
func historySharedSincePtr(history model.HistoryConfig, timestamp int64, roomID string) *int64 {
	if history.Mode != model.HistoryModeNone {
		return nil
	}
	if timestamp <= 0 {
		slog.Error("restricted history with missing timestamp, emitting nil", "roomID", roomID, "mode", history.Mode)
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
		result.Status = model.AsyncJobStatusError
		result.Error = sanitizeAsyncJobError(jobErr)
		slog.Error("async room job failed", "error", jobErr, "operation", operation, "requestID", requestID, "roomID", roomID)
	}
	data, _ := json.Marshal(result)
	if err := h.publish(ctx, subject.UserResponse(requesterAccount, requestID), data, ""); err != nil {
		slog.Warn("publish async job result failed", "error", err, "requestID", requestID)
	}
}

// permanentError pairs a user-safe message with the errPermanent sentinel so
// HandleJetStreamMsg can Ack the JetStream message AND publishAsyncJobResult
// can render a clean per-cause string without depending on suffix matching of
// the wrapped Error() output. An optional cause allows errors.Is(err, cause) checks.
type permanentError struct {
	msg   string
	cause error // optional; allows errors.Is(err, cause) matching
}

func newPermanent(format string, args ...any) error {
	return &permanentError{msg: fmt.Sprintf(format, args...)}
}

// newPermanentAbsent returns a permanent error that also satisfies errors.Is(err, errRoomKeyAbsent).
func newPermanentAbsent(format string, args ...any) error {
	return &permanentError{msg: fmt.Sprintf(format, args...), cause: errRoomKeyAbsent}
}

func (e *permanentError) Error() string { return e.msg }
func (e *permanentError) Unwrap() error { return e.cause }
func (e *permanentError) Is(target error) bool {
	if target == errPermanent {
		return true
	}
	_, ok := target.(*permanentError)
	return ok
}

// sanitizeAsyncJobError surfaces permanent errors verbatim and collapses everything else.
func sanitizeAsyncJobError(err error) string {
	if err == nil {
		return ""
	}
	var pe *permanentError
	if errors.As(err, &pe) {
		return pe.msg
	}
	if errors.Is(err, errPermanent) {
		// Legacy %w-wrapped errPermanent: trim the trailing ": permanent" suffix.
		msg := err.Error()
		if idx := strings.LastIndex(msg, ": "+errPermanent.Error()); idx >= 0 {
			msg = msg[:idx]
		}
		return msg
	}
	return "operation failed"
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
	default:
		slog.Warn("unknown member operation", "subject", subj)
	}
	if err != nil {
		slog.Error("process message failed", "error", err, "subject", subj)
		// Permanent failures must Ack so JetStream stops redelivering. The async-job
		// error event has already been published to the requester via the per-handler
		// defer in processCreateRoom / processAddMembers / processRemove*.
		if errors.Is(err, errPermanent) {
			if ackErr := msg.Ack(); ackErr != nil {
				slog.Error("failed to ack permanent-error message", "error", ackErr)
			}
			return
		}
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
		payloadSeed := fmt.Sprintf("%s:%s:%s:%d", req.RoomID, req.Account, req.NewRole, req.Timestamp)
		dedupID := natsutil.OutboxDedupID(ctx, user.SiteID, payloadSeed)
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

	// Pre-upgrade senders omit RoomType; treat zero value as channel since room-service validated it.
	if req.RoomType != "" && req.RoomType != model.RoomTypeChannel {
		return newPermanent("remove-member only valid on channel rooms, got %s", req.RoomType)
	}
	// Removed-user-read window: between this canonical event being published and the Mongo
	// delete below, broadcast-worker may still address the removed user with the old key.
	// Accepted as a documented limitation; see docs/superpowers/specs/2026-05-08-room-encryption-keys-design.md.
	currentPair, err := h.keyStore.Get(ctx, req.RoomID)
	if err != nil {
		roomkeymetrics.ValkeyErrors.Add(ctx, 1, metric.WithAttributes(attribute.String("op", "Get")))
		return fmt.Errorf("get room key: %w", err)
	}
	// Skip-rotation guard: a prior redelivery of this canonical event already rotated Valkey past req.BaseKeyVersion.
	shouldRotate := currentPair == nil || currentPair.Version <= req.BaseKeyVersion

	if req.OrgID != "" {
		return h.processRemoveOrg(ctx, &req, currentPair, shouldRotate)
	}
	return h.processRemoveIndividual(ctx, &req, currentPair, shouldRotate)
}

// rotateAndFanOut generates v+1, fans it out to survivors, then commits via Valkey Rotate.
// Fan-out before Rotate is intentional so survivors hold v+1 before broadcast-worker switches.
func (h *Handler) rotateAndFanOut(ctx context.Context, roomID string, currentPair *roomkeystore.VersionedKeyPair, survivors []model.Subscription) error {
	newPair, err := roomkeystore.GenerateKeyPair()
	if err != nil {
		return fmt.Errorf("generate room key: %w", err)
	}
	predictedVersion := 0
	if currentPair != nil {
		predictedVersion = currentPair.Version + 1
	}
	versioned := &roomkeystore.VersionedKeyPair{Version: predictedVersion, KeyPair: newPair}
	h.fanOutRoomKeyToSurvivors(ctx, roomID, versioned, survivors)

	if currentPair == nil {
		if _, err := h.keyStore.Set(ctx, roomID, newPair); err != nil {
			roomkeymetrics.ValkeyErrors.Add(ctx, 1, metric.WithAttributes(attribute.String("op", "Set")))
			return fmt.Errorf("store room key (no prior): %w", err)
		}
		roomkeymetrics.KeyGenerated.Add(ctx, 1)
		return nil
	}
	if _, err := h.keyStore.Rotate(ctx, roomID, newPair); err != nil {
		if errors.Is(err, roomkeystore.ErrNoCurrentKey) {
			if _, setErr := h.keyStore.Set(ctx, roomID, newPair); setErr != nil {
				roomkeymetrics.ValkeyErrors.Add(ctx, 1, metric.WithAttributes(attribute.String("op", "Set")))
				return fmt.Errorf("store room key (fallback): %w", setErr)
			}
			roomkeymetrics.KeyGenerated.Add(ctx, 1)
			return nil
		}
		roomkeymetrics.ValkeyErrors.Add(ctx, 1, metric.WithAttributes(attribute.String("op", "Rotate")))
		return fmt.Errorf("rotate room key: %w", err)
	}
	roomkeymetrics.KeyRotated.Add(ctx, 1)
	return nil
}

func (h *Handler) processRemoveIndividual(ctx context.Context, req *model.RemoveMemberRequest, currentPair *roomkeystore.VersionedKeyPair, shouldRotate bool) (err error) {
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

	// Rotate after delete + reconcile; ListByRoom returns post-deletion survivors.
	if shouldRotate {
		survivors, listErr := h.store.ListByRoom(ctx, req.RoomID)
		if listErr != nil {
			return fmt.Errorf("list survivors for key fan-out (room %s): %w", req.RoomID, listErr)
		}
		if err := h.rotateAndFanOut(ctx, req.RoomID, currentPair, survivors); err != nil {
			return err
		}
	}

	now := time.Now().UTC()

	// Subscription update event. RoomType is fixed to channel: room-service
	// rejects member.remove for any other room kind.
	subEvt := model.SubscriptionUpdateEvent{
		UserID: user.ID,
		Subscription: model.Subscription{
			RoomID:   req.RoomID,
			RoomType: model.RoomTypeChannel,
			User:     model.SubscriptionUser{ID: user.ID, Account: req.Account},
		},
		Action:    "removed",
		Timestamp: now.UnixMilli(),
	}
	subEvtData, _ := json.Marshal(subEvt)
	if err := h.publish(ctx, subject.SubscriptionUpdate(req.Account), subEvtData, ""); err != nil {
		slog.Error("subscription update publish failed", "error", err, "account", req.Account)
	}

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
		slog.Error("member event publish failed", "error", err, "roomID", req.RoomID)
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
		slog.Error("local inbox member_removed publish failed", "error", err, "roomID", req.RoomID)
	}

	// Sys-msg sender: leaving user for self-leave, requester for forced removal.
	requester := &user.User
	if !isSelfLeave {
		requester, err = h.store.GetUser(ctx, req.Requester)
		if err != nil {
			if errors.Is(err, ErrUserNotFound) {
				return newPermanent("requester %s not found (room %s)", req.Requester, req.RoomID)
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
	if err := h.publish(ctx, subject.MsgCanonicalCreated(h.siteID), msgEvtData, sysMsg.ID); err != nil {
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

func (h *Handler) processRemoveOrg(ctx context.Context, req *model.RemoveMemberRequest, currentPair *roomkeystore.VersionedKeyPair, shouldRotate bool) (err error) {
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

	// SectName is harvested from the UNFILTERED members slice (not toRemove) so
	// it remains correct when every org member also has an individual sub and
	// toRemove ends up empty. Pick the first non-empty SectName; an all-empty
	// result is a data inconsistency upstream and must short-circuit before any
	// mutating store call so the org-doc deletion is not lost to a malformed
	// sys-message.
	sectName := ""
	for _, m := range members {
		if m.SectName != "" {
			sectName = m.SectName
			break
		}
	}
	if sectName == "" {
		return newPermanent("org %s missing SectName on all members (room %s)", req.OrgID, req.RoomID)
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

	if err := h.store.ReconcileMemberCounts(ctx, req.RoomID); err != nil {
		return fmt.Errorf("reconcile member counts: %w", err)
	}

	// Rotate only when something was actually deleted; ListByRoom returns post-deletion survivors.
	if shouldRotate && len(accounts) > 0 {
		survivors, listErr := h.store.ListByRoom(ctx, req.RoomID)
		if listErr != nil {
			return fmt.Errorf("list survivors for key fan-out (room %s): %w", req.RoomID, listErr)
		}
		if err := h.rotateAndFanOut(ctx, req.RoomID, currentPair, survivors); err != nil {
			return err
		}
	}

	now := time.Now().UTC()

	// Publish per-account subscription update and collect cross-site accounts
	for _, m := range toRemove {
		subEvt := model.SubscriptionUpdateEvent{
			Subscription: model.Subscription{
				RoomID:   req.RoomID,
				RoomType: model.RoomTypeChannel,
				User:     model.SubscriptionUser{Account: m.Account},
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
			Type:      model.OutboxMemberRemoved,
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
			slog.Error("local inbox member_removed publish failed", "error", err, "roomID", req.RoomID)
		}
	}

	// Sys-msg sender is the requester.
	requester, err := h.store.GetUser(ctx, req.Requester)
	if err != nil {
		if errors.Is(err, ErrUserNotFound) {
			return newPermanent("requester %s not found (room %s)", req.Requester, req.RoomID)
		}
		return fmt.Errorf("get requester: %w", err)
	}
	sysMsgPayload, _ := json.Marshal(model.MemberRemoved{
		OrgID:             req.OrgID,
		SectName:          sectName,
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
		Content:     formatRemovedOrg(sectName),
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
	if err := h.publish(ctx, subject.MsgCanonicalCreated(h.siteID), msgEvtData, sysMsg.ID); err != nil {
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
	var req model.AddMembersRequest
	if err = json.Unmarshal(data, &req); err != nil {
		return fmt.Errorf("unmarshal add members request: %w", err)
	}
	requestID := natsutil.RequestIDFromContext(ctx)
	if requestID == "" {
		return newPermanent("missing X-Request-ID")
	}
	if !idgen.IsValidUUID(requestID) {
		return newPermanent("invalid X-Request-ID: must be a hyphenated UUID")
	}
	if req.Timestamp <= 0 {
		req.Timestamp = time.Now().UTC().UnixMilli()
	}
	// Now req is populated; defer the result publish covers all subsequent return paths.
	defer func() {
		h.publishAsyncJobResult(ctx, req.RequesterAccount, model.AsyncJobOpRoomMemberAdd, req.RoomID, err)
	}()

	room, err := h.store.GetRoom(ctx, req.RoomID)
	if err != nil {
		return fmt.Errorf("get room: %w", err)
	}
	// Defensive channel-only guard.
	if room.Type != model.RoomTypeChannel {
		return newPermanent("add-member only valid on channel rooms, got %s", room.Type)
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
	// `accounts` is the resolved set from ListNewMembers (which queries the
	// users collection), so a missing entry here means the user was deleted
	// between resolution and lookup — a hard data inconsistency that won't
	// resolve via JetStream redelivery. Mirror the create-room contract and
	// fail permanently rather than silently materializing partial membership.
	for _, account := range accounts {
		if _, ok := userMap[account]; !ok {
			return newPermanent("user %s not found in room.member.add (room %s)", account, req.RoomID)
		}
	}

	requester, err := h.store.GetUser(ctx, req.RequesterAccount)
	if err != nil {
		if errors.Is(err, ErrUserNotFound) {
			return newPermanent("requester %s not found (room %s)", req.RequesterAccount, req.RoomID)
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

	// Build subscriptions and collect the resolved accounts in a single pass
	// so we don't re-iterate `subs` later to build an account set or an
	// actualAccounts slice.
	subs := make([]*model.Subscription, 0, len(accounts))
	actualAccounts := make([]string, 0, len(accounts))
	resolvedAccountSet := make(map[string]struct{}, len(accounts))
	for _, account := range accounts {
		// Presence guaranteed by the userMap completeness check above.
		user := userMap[account]
		// RoomType is fixed to channel: room-service rejects member.add for
		// any other room kind.
		sub := &model.Subscription{
			ID:       idgen.GenerateUUIDv7(),
			User:     model.SubscriptionUser{ID: user.ID, Account: user.Account},
			RoomID:   req.RoomID,
			Name:     room.Name,
			RoomType: model.RoomTypeChannel,
			SiteID:   room.SiteID,
			Roles:    []model.Role{model.RoleMember},
			JoinedAt: acceptedAt,
		}
		// Resolve once via the shared helper so the local sub, the per-user
		// SubscriptionUpdateEvent fan-out, and the cross-site MemberAddEvent
		// all carry the same HistorySharedSince value.
		if ms := historySharedSincePtr(req.History, req.Timestamp, req.RoomID); ms != nil {
			t := time.UnixMilli(*ms).UTC()
			sub.HistorySharedSince = &t
		}
		subs = append(subs, sub)
		actualAccounts = append(actualAccounts, user.Account)
		resolvedAccountSet[user.Account] = struct{}{}
	}

	if err := h.store.BulkCreateSubscriptions(ctx, subs); err != nil {
		return fmt.Errorf("bulk create subscriptions: %w", err)
	}

	// Fail closed: defaulting hadOrgsBefore=false on error would trigger spurious first-org backfill.
	hadOrgsBefore, err := h.store.HasOrgRoomMembers(ctx, req.RoomID)
	if err != nil {
		return fmt.Errorf("check existing org room members: %w", err)
	}
	writeIndividuals := len(req.Orgs) > 0 || hadOrgsBefore

	// Collect all room_member docs to write in a single bulk insert:
	// new individuals + new orgs + (optional) backfill of existing subscribers.
	roomMembers := make([]*model.RoomMember, 0, len(subs)+len(req.Orgs))
	allowedIndividual := make(map[string]struct{}, len(req.Users))
	for _, acc := range req.Users {
		allowedIndividual[acc] = struct{}{}
	}
	if writeIndividuals {
		for _, sub := range subs {
			if _, ok := allowedIndividual[sub.User.Account]; !ok {
				continue
			}
			roomMembers = append(roomMembers, &model.RoomMember{
				ID:     idgen.GenerateUUIDv7(),
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
			ID:     idgen.GenerateUUIDv7(),
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
	if len(req.Orgs) > 0 && !hadOrgsBefore {
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
		if err := h.publish(ctx, subject.SubscriptionUpdate(sub.User.Account), subEvtData, ""); err != nil {
			slog.Error("subscription update publish failed", "error", err, "account", sub.User.Account)
		}
	}

	if err := h.buildAndFanOutRoomKey(ctx, req.RoomID, users); err != nil {
		return fmt.Errorf("fan out room key: %w", err)
	}

	// 8. Publish MemberAddEvent (actualAccounts was built above alongside subs)
	historySharedSince := historySharedSincePtr(req.History, req.Timestamp, req.RoomID)
	memberAddEvt := model.MemberAddEvent{
		Type:               "member_added",
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
		slog.Error("member add event publish failed", "error", err, "roomID", req.RoomID)
	}

	if len(actualAccounts) > 0 {
		inboxOutbox := model.OutboxEvent{
			Type:       "member_added",
			SiteID:     room.SiteID,
			DestSiteID: room.SiteID,
			Payload:    memberAddData,
			Timestamp:  now.UnixMilli(),
		}
		inboxData, _ := json.Marshal(inboxOutbox)
		inboxSeed := fmt.Sprintf("%s:%s:%d", req.RoomID, req.RequesterAccount, req.Timestamp)
		if err := h.publish(ctx, subject.InboxMemberAdded(room.SiteID), inboxData, natsutil.OutboxDedupID(ctx, room.SiteID, inboxSeed)); err != nil {
			slog.Error("local inbox member_added publish failed", "error", err, "roomID", req.RoomID)
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
	if err := h.publish(ctx, subject.MsgCanonicalCreated(room.SiteID), msgEvtData, sysMsg.ID); err != nil {
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
			RoomName:           room.Name,
			RoomType:           room.Type,
			Accounts:           accounts,
			SiteID:             room.SiteID,
			RequesterAccount:   req.RequesterAccount,
			JoinedAt:           req.Timestamp,
			HistorySharedSince: historySharedSince,
			Timestamp:          now.UnixMilli(),
		}
		siteEvtData, _ := json.Marshal(siteEvt)
		outbox := model.OutboxEvent{
			Type: "member_added", SiteID: room.SiteID, DestSiteID: destSiteID,
			Payload: siteEvtData, Timestamp: now.UnixMilli(),
		}
		outboxData, _ := json.Marshal(outbox)
		payloadSeed := fmt.Sprintf("%s:%s:%d", req.RoomID, req.RequesterAccount, req.Timestamp)
		dedupID := natsutil.OutboxDedupID(ctx, destSiteID, payloadSeed)
		if err := h.publish(ctx, subject.Outbox(room.SiteID, destSiteID, "member_added"), outboxData, dedupID); err != nil {
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

// newSub constructs a Subscription from its constituent parts.
func newSub(id string, user *model.User, room *model.Room, roles []model.Role,
	name string, isSubscribed bool, joinedAt time.Time) *model.Subscription {
	return &model.Subscription{
		ID:           id,
		User:         model.SubscriptionUser{ID: user.ID, Account: user.Account},
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
	if requestID == "" {
		return newPermanent("missing X-Request-ID")
	}
	if !idgen.IsValidUUID(requestID) {
		return newPermanent("invalid X-Request-ID: must be a hyphenated UUID")
	}

	var req model.CreateRoomRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return newPermanent("unmarshal create-room: %s", err.Error())
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
		return newPermanentAbsent("room key absent for %s", req.RoomID)
	}

	requester, err := h.store.GetUser(ctx, req.RequesterAccount)
	if err != nil {
		if errors.Is(err, ErrUserNotFound) {
			return newPermanent("requester not found")
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
		CreatedBy: requester.ID,
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
					return newPermanent("bot user not found")
				}
				return newPermanent("counterpart not found")
			}
			return fmt.Errorf("get counterpart: %w", err)
		}
		room.UIDs, room.Accounts = model.BuildDMParticipants(requester, counterpart)
	}

	if err := h.store.CreateRoom(ctx, room); err != nil {
		if mongo.IsDuplicateKeyError(err) {
			existing, fetchErr := h.store.GetRoom(ctx, room.ID)
			if fetchErr != nil {
				return fmt.Errorf("fetch on duplicate-key: %w", fetchErr)
			}
			// Replay equivalence: only treat the collision as a redelivery
			// when the existing room is identical on all immutable identity
			// fields (Type, SiteID, Name, CreatedBy). Any mismatch means the
			// same ID resolves to a different room — appending subscriptions
			// or system messages to it would corrupt unrelated state.
			if existing.Type != room.Type ||
				existing.SiteID != room.SiteID ||
				existing.Name != room.Name ||
				existing.CreatedBy != room.CreatedBy {
				return newPermanent("room ID collision (existing type=%s site=%s name=%q createdBy=%q; want %s/%s/%q/%q)",
					existing.Type, existing.SiteID, existing.Name, existing.CreatedBy,
					room.Type, room.SiteID, room.Name, room.CreatedBy)
			}
			room = existing
		} else {
			return fmt.Errorf("create room: %w", err)
		}
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
		return h.finishCreateRoom(ctx, &req, room, requester, []model.User{*requester, *counterpart}, subs, requestID, now)
	case model.RoomTypeChannel:
		return h.processCreateRoomChannel(ctx, &req, room, requester, requestID, acceptedAt, now)
	default:
		return newPermanent("unknown room type %q", roomType)
	}
}

// determineRoomTypeFromPayload mirrors room-service's determineRoomType on the canonical payload.
// botPattern matches both ".bot" suffix and "p_" prefix to classify webhook-style bots
// consistently with room-service/helper.go and pkg/pipelines.
func determineRoomTypeFromPayload(req *model.CreateRoomRequest) model.RoomType {
	if req.Name == "" && len(req.Orgs) == 0 && len(req.Channels) == 0 && len(req.Users) == 1 {
		acct := req.Users[0]
		if strings.HasSuffix(acct, ".bot") || strings.HasPrefix(acct, "p_") {
			return model.RoomTypeBotDM
		}
		return model.RoomTypeDM
	}
	return model.RoomTypeChannel
}

func (h *Handler) processCreateRoomChannel(ctx context.Context, req *model.CreateRoomRequest, room *model.Room, requester *model.User, requestID string, acceptedAt, now time.Time) error {
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
			return newPermanent("user %s not found", account)
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

	return h.finishCreateRoom(ctx, req, room, requester, allUsers, subs, requestID, now)
}

func (h *Handler) finishCreateRoom(ctx context.Context, req *model.CreateRoomRequest, room *model.Room, requester *model.User, allUsers []model.User, subs []*model.Subscription, requestID string, now time.Time) error {
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
			slog.Error("marshal subscription.update failed", "error", err, "account", sub.User.Account)
			continue
		}
		if err := h.publish(ctx, subject.SubscriptionUpdate(sub.User.Account), data, ""); err != nil {
			slog.Error("publish subscription.update failed", "error", err, "account", sub.User.Account)
		}
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
		slog.Error("local inbox member_added publish failed", "error", err, "roomID", room.ID, "requestID", requestID)
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
	if err := h.buildAndFanOutRoomKey(ctx, room.ID, allUsers); err != nil {
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
	return h.publish(ctx, subject.MsgCanonicalCreated(siteID), data, msg.ID)
}

// Sync DM endpoint handlers (chat.server.request.room.{siteID}.create.dm).

var (
	errMissingRequestID     = errors.New("missing X-Request-ID header")
	errInvalidRequestID     = errors.New("invalid X-Request-ID header")
	errInvalidSyncDMRequest = errors.New("invalid sync DM request")
	errUserLookupFailed     = errors.New("user lookup failed")
	errCrossSiteRequester   = errors.New("requester is not on this site")
	errRoomIDCollision      = errors.New("room ID collision (existing room metadata mismatch)")
)

// sanitizeSyncDMError surfaces sentinel messages; masks anything else as "internal error".
func sanitizeSyncDMError(err error) string {
	if err == nil {
		return ""
	}
	switch {
	case errors.Is(err, errMissingRequestID),
		errors.Is(err, errInvalidRequestID),
		errors.Is(err, errInvalidSyncDMRequest),
		errors.Is(err, errUserLookupFailed),
		errors.Is(err, errCrossSiteRequester):
		return err.Error()
	default:
		return "internal error"
	}
}

// handleSyncCreateDM creates a DM/botDM room + 2 subs and returns the requester's sub.
func (h *Handler) handleSyncCreateDM(ctx context.Context, data []byte) (*model.SyncCreateDMReply, error) {
	requestID := natsutil.RequestIDFromContext(ctx)
	if requestID == "" {
		return nil, errMissingRequestID
	}
	if !idgen.IsValidUUID(requestID) {
		return nil, errInvalidRequestID
	}

	var req model.SyncCreateDMRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, errInvalidSyncDMRequest
	}
	if err := validateSyncCreateDMShape(&req); err != nil {
		return nil, err
	}

	users, err := h.store.FindUsersByAccounts(ctx, []string{req.RequesterAccount, req.OtherAccount})
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
	other, ok := byAccount[req.OtherAccount]
	if !ok {
		return nil, errUserLookupFailed
	}

	acceptedAt := time.Now().UTC()
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
		CreatedBy: requester.ID,
		SiteID:    h.siteID,
		UserCount: userCount,
		AppCount:  appCount,
		UIDs:      uids,
		Accounts:  accounts,
		CreatedAt: acceptedAt,
		UpdatedAt: acceptedAt,
	}
	if err := h.store.CreateRoom(ctx, room); err != nil {
		if !mongo.IsDuplicateKeyError(err) {
			return nil, fmt.Errorf("create room: %w", err)
		}
		existing, fetchErr := h.store.GetRoom(ctx, room.ID)
		if fetchErr != nil {
			return nil, fmt.Errorf("fetch room on duplicate-key: %w", fetchErr)
		}
		if existing.Type != room.Type ||
			existing.SiteID != room.SiteID ||
			existing.Name != room.Name ||
			existing.CreatedBy != room.CreatedBy {
			slog.Error("sync DM: room ID collision",
				"roomID", room.ID,
				"existingType", existing.Type, "wantType", room.Type,
				"existingSiteID", existing.SiteID, "wantSiteID", room.SiteID,
				"existingCreatedBy", existing.CreatedBy, "wantCreatedBy", room.CreatedBy,
				"requestID", requestID)
			return nil, errRoomIDCollision
		}
		// Sync-path duplicate-key: forward-only — no UIDs/Accounts backfill on the existing room.
		room = existing
		acceptedAt = existing.CreatedAt
	}

	// validateSyncCreateDMShape already gated this to {dm, botDM}.
	var subs []*model.Subscription
	if req.RoomType == model.RoomTypeBotDM {
		subs = buildBotDMSubs(requester, other, room, acceptedAt)
	} else {
		subs = buildDMSubs(requester, other, room, acceptedAt)
	}

	if err := h.store.BulkCreateSubscriptions(ctx, subs); err != nil {
		return nil, fmt.Errorf("bulk create subs: %w", err)
	}
	// Re-read canonical subs: BulkCreateSubscriptions swallows dup-key races, so the
	// in-memory subs may carry IDs/JoinedAt that never made it to Mongo. Publish from
	// the persisted pair instead.
	requesterSub, err := h.store.FindDMSubscription(ctx, requester.Account, other.Account)
	if err != nil {
		return nil, fmt.Errorf("find requester sub after insert: %w", err)
	}
	otherSub, err := h.store.FindDMSubscription(ctx, other.Account, requester.Account)
	if err != nil {
		return nil, fmt.Errorf("find counterpart sub after insert: %w", err)
	}

	h.publishSubscriptionUpdates(ctx, []*model.Subscription{requesterSub, otherSub}, requestID)

	// Outbox failure means the remote site won't learn about the room; fail the request.
	if err := h.publishSyncDMOutbox(ctx, room, requester, other, acceptedAt); err != nil {
		return nil, fmt.Errorf("publish room_created outbox: %w", err)
	}

	return &model.SyncCreateDMReply{Success: true, Subscription: *requesterSub}, nil
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
	if req.RequesterAccount == req.OtherAccount {
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
			slog.Error("sync DM: marshal subscription.update failed",
				"error", err, "account", sub.User.Account, "requestID", requestID)
			continue
		}
		if err := h.publish(ctx, subject.SubscriptionUpdate(sub.User.Account), data, ""); err != nil {
			slog.Error("sync DM: publish subscription.update failed",
				"error", err, "account", sub.User.Account, "requestID", requestID)
		}
	}
}

func (h *Handler) publishSyncDMOutbox(ctx context.Context, room *model.Room, requester, other *model.User, acceptedAt time.Time) error {
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
		JoinedAt:         acceptedAt.UnixMilli(),
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
	payloadSeed := fmt.Sprintf("%s:%s:%d", room.ID, requester.Account, acceptedAt.UnixMilli())
	return h.publish(ctx,
		subject.Outbox(room.SiteID, other.SiteID, model.OutboxMemberAdded),
		eData,
		natsutil.OutboxDedupID(ctx, other.SiteID, payloadSeed),
	)
}

// natsServerCreateDM is the NATS entry point for chat.server.request.room.{siteID}.create.dm.
func (h *Handler) natsServerCreateDM(m otelnats.Msg) {
	ctx := natsutil.ContextWithRequestIDFromHeaders(m.Context(), m.Msg.Header)
	reply, err := h.handleSyncCreateDM(ctx, m.Msg.Data)
	if err != nil {
		slog.Error("sync DM: handler failed",
			"error", err, "subject", m.Msg.Subject,
			"requestID", natsutil.RequestIDFromContext(ctx))
		natsutil.ReplyError(m.Msg, sanitizeSyncDMError(err))
		return
	}
	natsutil.ReplyJSON(m.Msg, reply)
}

// fanOutRoomKeyToSurvivors sends the already-fetched room key to every room member in survivors
// (local + remote). NATS supercluster routes user-subjects to home sites.
// survivors is a pre-computed post-deletion snapshot supplied by the caller; pair must be non-nil.
func (h *Handler) fanOutRoomKeyToSurvivors(ctx context.Context, roomID string, pair *roomkeystore.VersionedKeyPair, survivors []model.Subscription) {
	// PublicKey omitted: server-side only, read from Valkey by broadcast-worker.
	evt := model.RoomKeyEvent{
		RoomID:     roomID,
		Version:    pair.Version,
		PrivateKey: pair.KeyPair.PrivateKey,
	}
	for i := range survivors {
		if err := h.keySender.Send(survivors[i].User.Account, evt); err != nil {
			slog.Error("send room key", "error", err, "account", survivors[i].User.Account, "roomId", roomID)
			roomkeymetrics.FanoutErrors.Add(ctx, 1, metric.WithAttributes(attribute.String("roomId", roomID)))
		}
	}
}

// buildAndFanOutRoomKey fetches the current key from Valkey, builds the RoomKeyEvent,
// and fans it out to every room member account in users (local + remote).
// NATS supercluster routes user-subjects to home sites.
func (h *Handler) buildAndFanOutRoomKey(ctx context.Context, roomID string, users []model.User) error {
	pair, err := h.keyStore.Get(ctx, roomID)
	if err != nil {
		roomkeymetrics.ValkeyErrors.Add(ctx, 1, metric.WithAttributes(attribute.String("op", "Get")))
		return fmt.Errorf("get room key: %w", err)
	}
	if pair == nil {
		roomkeymetrics.KeyAbsentErrors.Add(ctx, 1)
		return newPermanentAbsent("room key absent for %s", roomID)
	}
	// PublicKey omitted: server-side only, read from Valkey by broadcast-worker.
	evt := model.RoomKeyEvent{
		RoomID:     roomID,
		Version:    pair.Version,
		PrivateKey: pair.KeyPair.PrivateKey,
	}
	for i := range users {
		u := &users[i]
		if err := h.keySender.Send(u.Account, evt); err != nil {
			slog.Error("send room key", "error", err, "account", u.Account, "roomId", roomID)
			roomkeymetrics.FanoutErrors.Add(ctx, 1, metric.WithAttributes(attribute.String("roomId", roomID)))
		}
	}
	return nil
}
