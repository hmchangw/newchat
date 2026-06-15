package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/displayfmt"
	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errnats"
	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/logctx"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
)

const maxContentBytes = 20 * 1024 // 20 KB

// replyFunc is the function signature for publishing a reply to a NATS subject.
type replyFunc func(ctx context.Context, msg *nats.Msg) error

// publishFunc is the function signature for publishing to JetStream.
type publishFunc func(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error)

// UserGetter is the narrow user-record surface gatekeeper needs for sender
// display-name resolution. *userstore.Cache satisfies this; tests stub it.
type UserGetter interface {
	FindUserByID(ctx context.Context, id string) (*model.User, error)
}

// Handler processes messages from the MESSAGES stream and validates them
// before publishing to MESSAGES_CANONICAL.
type Handler struct {
	store              Store
	users              UserGetter
	publish            publishFunc
	reply              replyFunc
	siteID             string
	parentFetcher      ParentMessageFetcher
	largeRoomThreshold int
}

// NewHandler constructs a new Handler with the given dependencies.
// users may be nil; when nil, sender display-name resolution is skipped and
// downstream consumers fall back to UserAccount.
func NewHandler(store Store, users UserGetter, publish publishFunc, reply replyFunc, siteID string, parentFetcher ParentMessageFetcher, largeRoomThreshold int) *Handler {
	return &Handler{
		store:              store,
		users:              users,
		publish:            publish,
		reply:              reply,
		siteID:             siteID,
		parentFetcher:      parentFetcher,
		largeRoomThreshold: largeRoomThreshold,
	}
}

// HandleJetStreamMsg processes a JetStream message from the MESSAGES stream.
func (h *Handler) HandleJetStreamMsg(ctx context.Context, msg jetstream.Msg) {
	// Parse the body once; reused for log enrichment, reply routing, and
	// processMessage validation (was triple-decoded on the hot path).
	rawData := msg.Data()
	var req model.SendMessageRequest
	parseErr := json.Unmarshal(rawData, &req)

	// Enrich the logger before the subject parse so even the malformed-subject
	// path carries request_id + a best-effort account. roomID is added later.
	ctx = errcode.WithLogValues(ctx,
		"request_id", req.RequestID,
		"account", accountFromSubject(msg.Subject()))

	// flow: the gatekeeper hop entry — carries stream-wait latency and size.
	debugFlowReceived(ctx, msg, req.RequestID)

	account, roomID, siteID, ok := subject.ParseUserRoomSiteSubject(msg.Subject())
	if !ok {
		slog.Warn("invalid subject", "subject", msg.Subject())
		debugFlowRejected(ctx, req.RequestID, "invalid_subject")
		// Best-effort error reply so the client doesn't hang; sendReply no-ops
		// when account or requestId is unusable. Ack — malformed is not retryable.
		h.sendReply(ctx, accountFromSubject(msg.Subject()), &req, errnats.Marshal(ctx, errcode.BadRequest("invalid message subject")))
		if err := msg.Ack(); err != nil {
			slog.Error("failed to ack message", "error", err)
		}
		return
	}

	ctx = errcode.WithLogValues(ctx, "room_id", roomID)

	if parseErr != nil {
		// Do not WithCause(parseErr) — json.SyntaxError strings embed the
		// offending substring from an unauthenticated entry-point (see doc.go).
		bad := errcode.BadRequest("unmarshal send message request")
		debugFlowRejected(ctx, req.RequestID, "unmarshal")
		h.sendReply(ctx, account, &req, errnats.Marshal(ctx, bad))
		if err := msg.Ack(); err != nil {
			slog.Error("failed to ack message", "error", err)
		}
		return
	}

	replyData, err := h.processMessage(ctx, account, roomID, siteID, &req)
	if err != nil {
		// Typed *errcode.Error → client-facing validation/permanence: reply + Ack.
		// Bare error (raw fmt.Errorf) → transient infra failure: Nak for redelivery.
		// errnats.Marshal runs Classify which logs once at category-aware level —
		// validation branch must NOT also log here. Infra branch owns its log.
		var ee *errcode.Error
		if errors.As(err, &ee) {
			debugFlowRejected(ctx, req.RequestID, string(ee.Code))
			h.sendReply(ctx, account, &req, errnats.Marshal(ctx, err))
			if err := msg.Ack(); err != nil {
				slog.Error("failed to ack message", "error", err)
			}
		} else {
			// flow terminal for the infra path; the Error line below carries the cause.
			slog.Log(ctx, logctx.LevelFlow, "gatekeeper nak", "phase", "nak", "request_id", req.RequestID)
			slog.ErrorContext(ctx, "process message failed (infra)", "error", err, "account", account, "room_id", roomID)
			if err := msg.Nak(); err != nil {
				slog.Error("failed to nack message", "error", err)
			}
		}
		return
	}

	h.sendReply(ctx, account, &req, replyData)

	if err := msg.Ack(); err != nil {
		slog.Error("failed to ack message", "err", err)
	}
}

// debugFlowReceived emits the flow-rung "received" breadcrumb at the gatekeeper
// hop entry. It carries payload size and stream_wait_ms — the time the message
// sat in MESSAGES before this consumer picked it up, the queue latency that
// inter-hop timestamp-diffing cannot see. Metadata only — never the body.
func debugFlowReceived(ctx context.Context, msg jetstream.Msg, requestID string) {
	if !logctx.Enabled(ctx, logctx.LevelFlow) {
		return // skip msg.Metadata() and arg-building on the unflagged hot path
	}
	streamWaitMs := int64(-1)
	if meta, err := msg.Metadata(); err == nil && meta != nil {
		streamWaitMs = time.Since(meta.Timestamp).Milliseconds()
	}
	slog.Log(ctx, logctx.LevelFlow, "gatekeeper received",
		"phase", "received", "request_id", requestID, "subject", msg.Subject(),
		"bytes", len(msg.Data()), "stream_wait_ms", streamWaitMs)
}

// debugFlowRejected emits the flow-rung terminal breadcrumb for a message the
// gatekeeper rejected; reason is a coarse, body-free tag.
func debugFlowRejected(ctx context.Context, requestID, reason string) {
	slog.Log(ctx, logctx.LevelFlow, "gatekeeper rejected",
		"phase", "rejected", "request_id", requestID, "reason", reason)
}

// sendReply publishes the reply payload to the user's response subject. Pass
// a zero-value *req when parsing failed — the empty RequestID gate no-ops.
func (h *Handler) sendReply(ctx context.Context, account string, req *model.SendMessageRequest, replyData []byte) {
	if account == "" {
		return
	}
	// Skip when requestId is missing or not a valid hyphenated UUID — the reply
	// subject chat.user.{account}.response.{requestId} would be unroutable, and
	// processMessage already rejects such requests upstream.
	if req.RequestID == "" || !idgen.IsValidUUID(req.RequestID) {
		return
	}
	respSubj := subject.UserResponse(account, req.RequestID)
	replyMsg := natsutil.NewMsg(ctx, respSubj, replyData)
	if err := h.reply(ctx, replyMsg); err != nil {
		slog.Error("reply to client failed", "error", err, "subject", respSubj)
	}
}

// accountFromSubject best-effort extracts the {account} token from a
// chat.user.{account}.… subject. Returns "" when the subject is too malformed
// to recover an account, in which case no error reply can be addressed.
func accountFromSubject(subj string) string {
	parts := strings.Split(subj, ".")
	if len(parts) >= 3 && parts[0] == "chat" && parts[1] == "user" {
		return parts[2]
	}
	return ""
}

// processMessage validates a SendMessageRequest and publishes a MessageEvent
// to MESSAGES_CANONICAL. Validation errors are typed *errcode.Error (reply +
// Ack); transient infra failures are bare fmt.Errorf (Nak for redelivery).
func (h *Handler) processMessage(ctx context.Context, account, roomID, siteID string, req *model.SendMessageRequest) ([]byte, error) {
	// Validate siteID matches this service's siteID
	if siteID != h.siteID {
		return nil, errcode.BadRequest(fmt.Sprintf("siteID mismatch: got %s, want %s", siteID, h.siteID))
	}

	// Validate requestId is a hyphenated UUID. It is required: the async reply
	// is published to chat.user.{account}.response.{requestId}, so an empty or
	// malformed value would leave the client unable to correlate (or receive)
	// the reply. Rejecting here fails fast instead of publishing an
	// unacknowledgeable message to MESSAGES_CANONICAL.
	if !idgen.IsValidUUID(req.RequestID) {
		return nil, errcode.BadRequest(fmt.Sprintf("invalid requestId %q: must be a hyphenated UUID", req.RequestID))
	}

	// Payload requestId is the canonical source for X-Request-ID — upstream publishers may
	// or may not set the NATS header, so overwrite ctx unconditionally before any downstream publish.
	ctx = natsutil.WithRequestID(ctx, req.RequestID)

	// Validate ID is a valid 20-char base62 message ID
	if !idgen.IsValidMessageID(req.ID) {
		return nil, errcode.BadRequest(fmt.Sprintf("invalid message ID %q: must be a 20-char base62 string", req.ID))
	}

	if req.ThreadParentMessageID != "" && !idgen.IsValidMessageID(req.ThreadParentMessageID) {
		return nil, errcode.BadRequest(fmt.Sprintf("invalid thread parent message ID %q: must be a 20-char base62 string", req.ThreadParentMessageID))
	}

	// Validate content is non-empty
	if req.Content == "" {
		return nil, errcode.BadRequest("content must not be empty")
	}

	// Validate content does not exceed 20KB
	if len(req.Content) > maxContentBytes {
		return nil, errcode.BadRequest(
			fmt.Sprintf("content exceeds maximum size of %d bytes", maxContentBytes),
			errcode.WithMetadata("maxContentBytes", strconv.Itoa(maxContentBytes), "attempted", strconv.Itoa(len(req.Content))),
		)
	}

	// Validate thread parent fields are paired
	if req.ThreadParentMessageID != "" && req.ThreadParentMessageCreatedAt == nil {
		return nil, errcode.BadRequest("validate thread parent fields: threadParentMessageCreatedAt is required when threadParentMessageId is set")
	}

	// Verify subscription
	sub, err := h.store.GetSubscription(ctx, account, roomID)
	if err != nil {
		if errors.Is(err, errNotSubscribed) {
			// Return the wrapped err so server-side logs keep the full chain
			// (store wrapped it with %w; errors.Is upstream still matches).
			return nil, err
		}
		return nil, fmt.Errorf("get subscription for user %s in room %s: %w", account, roomID, err)
	}
	// debug: sender is subscribed — the first decision a flagged message clears.
	slog.DebugContext(ctx, "gatekeeper subscription resolved", "request_id", req.RequestID, "roles", len(sub.Roles))

	// Large-room post restriction: in rooms with more than the configured
	// threshold of members, only owners, admins, and bots may send top-level
	// messages. Thread replies are exempt regardless of room size; bypass-eligible
	// senders (owner/admin role, or bot account name) are exempt regardless of
	// room size. Both bypasses skip the Room fetch entirely (approach B —
	// owner fast-path generalized).
	isThreadReply := req.ThreadParentMessageID != ""
	bypass := canBypassLargeRoomCap(sub)
	if !isThreadReply && !bypass {
		meta, err := h.store.GetRoomMeta(ctx, roomID)
		if err != nil {
			return nil, fmt.Errorf("get room meta for %s: %w", roomID, err)
		}
		if meta.UserCount > h.largeRoomThreshold {
			slog.Info("send blocked",
				"reason", string(errcode.MessageLargeRoomPostRestricted),
				"account", account,
				"room_id", roomID,
				"userCount", meta.UserCount,
				"threshold", h.largeRoomThreshold,
			)
			return nil, errLargeRoomPostRestricted
		}
	}
	// debug: how the large-room gate was decided (metadata only).
	slog.DebugContext(ctx, "gatekeeper large-room gate", "request_id", req.RequestID,
		"thread_reply", isThreadReply, "bypassed", bypass)

	// Build Message
	now := time.Now().UTC()

	var threadParentCreatedAt *time.Time
	if req.ThreadParentMessageCreatedAt != nil {
		t := time.UnixMilli(*req.ThreadParentMessageCreatedAt).UTC()
		threadParentCreatedAt = &t
	}

	quotedSnapshot, err := h.resolveQuoteSnapshot(ctx, account, roomID, siteID, req.QuotedParentMessageID, req.ThreadParentMessageID)
	if err != nil {
		return nil, err
	}
	if req.QuotedParentMessageID != "" {
		// debug: quote passed the same-conversation-context check.
		slog.DebugContext(ctx, "gatekeeper quote resolved", "request_id", req.RequestID, "quoted_id", req.QuotedParentMessageID)
	}

	// Compose the sender's render-ready display name once at write time so every
	// downstream consumer (notification-worker, future search-sync-worker) reads
	// from the canonical message instead of doing its own user lookup. The lookup
	// is best-effort — on miss/error we fall back to UserAccount via
	// model.DisplayName's empty-fields branch; message validation already passed
	// the sender check so missing display data does not warrant blocking the post.
	displayName := sub.User.Account
	if h.users != nil {
		u, uerr := h.users.FindUserByID(ctx, sub.User.ID)
		if uerr == nil && u != nil {
			displayName = displayfmt.CombineWithFallback(u.EngName, u.ChineseName, sub.User.Account)
		} else if uerr != nil {
			slog.Warn("sender user-meta lookup failed, display name falls back to account",
				"error", uerr, "userId", sub.User.ID, "account", sub.User.Account, "messageId", req.ID)
		}
	}

	// tshow ("Also send to channel") is only meaningful on a thread reply: it asks for the
	// reply to also appear in the parent room's channel timeline. On a
	// non-thread send it is normalized to false (ignored, not rejected) — see
	// docs/client-api.md §msg.send.
	tshow := req.TShow && req.ThreadParentMessageID != ""

	msg := model.Message{
		ID:                           req.ID,
		RoomID:                       roomID,
		UserID:                       sub.User.ID,
		UserAccount:                  sub.User.Account,
		UserDisplayName:              displayName,
		Content:                      req.Content,
		CreatedAt:                    now,
		ThreadParentMessageID:        req.ThreadParentMessageID,
		ThreadParentMessageCreatedAt: threadParentCreatedAt,
		TShow:                        tshow,
		QuotedParentMessage:          quotedSnapshot,
	}

	// Publish MessageEvent to MESSAGES_CANONICAL
	evt := model.MessageEvent{Event: model.EventCreated, Message: msg, SiteID: siteID, Timestamp: now.UnixMilli()}
	evtData, err := json.Marshal(evt)
	if err != nil {
		return nil, fmt.Errorf("marshal message event: %w", err)
	}

	canonicalSubj := subject.MsgCanonicalCreated(siteID)
	canonicalMsg := natsutil.NewMsg(ctx, canonicalSubj, evtData)
	if _, err := h.publish(ctx, canonicalMsg, jetstream.WithMsgID(natsutil.CanonicalDedupID(&evt))); err != nil {
		return nil, fmt.Errorf("publish to MESSAGES_CANONICAL: %w", err)
	}
	// flow: the message cleared the gate and was handed off to MESSAGES_CANONICAL.
	slog.Log(ctx, logctx.LevelFlow, "gatekeeper published to canonical",
		"phase", "published", "request_id", req.RequestID, "subject", canonicalSubj, "bytes", len(evtData))

	return json.Marshal(msg)
}

// resolveQuoteSnapshot fetches the quoted parent and returns its snapshot.
// The strict same-conversation-context rule rejects cross-thread quotes:
// main-room messages may only quote main-room parents, and thread-T messages
// may only quote other thread-T messages — including the thread's own parent.
func (h *Handler) resolveQuoteSnapshot(ctx context.Context, account, roomID, siteID, quotedParentMessageID, newMessageThreadID string) (*cassandra.QuotedParentMessage, error) {
	if quotedParentMessageID == "" {
		return nil, nil
	}
	snap, err := h.parentFetcher.FetchQuotedParent(ctx, account, roomID, siteID, quotedParentMessageID)
	switch {
	case err != nil:
		// Preserve upstream errcode classification (transient → Unavailable,
		// real 404 → NotFound). For non-errcode infra failures (NATS timeout,
		// no-responders, unmarshal), classify as Unavailable — a transient
		// quoted-parent fetch failure shouldn't surface to the client as 404.
		var ee *errcode.Error
		if errors.As(err, &ee) {
			return nil, ee
		}
		return nil, errcode.Unavailable(fmt.Sprintf("fetch quoted parent %s", quotedParentMessageID), errcode.WithCause(err))
	case snap == nil:
		// A nil snapshot with no error is a fetcher contract violation, not a
		// genuine missing parent. Return a bare error so the caller's branch
		// classifies this as infra (Nak for redelivery + log) rather than
		// permanently dropping the message via a 404 reply+Ack.
		return nil, fmt.Errorf("fetch quoted parent %s: fetcher returned nil snapshot", quotedParentMessageID)
	case snap.ThreadParentID != newMessageThreadID:
		return nil, errcode.BadRequest(fmt.Sprintf("quoted parent %s thread context mismatch: parent thread %q, new message thread %q",
			quotedParentMessageID, snap.ThreadParentID, newMessageThreadID))
	default:
		return snap, nil
	}
}

// canBypassLargeRoomCap reports whether the subscriber is exempt from the
// large-room post restriction. Owners, admins, and bots bypass.
//
// "Bot" is detected by account-name pattern (\.bot$|^p_) — see helper.go.
// This single function is the edit point if/when the bypass policy changes
// (e.g. promoting isBot to a shared package, adding new roles, etc.).
func canBypassLargeRoomCap(sub *model.Subscription) bool {
	for _, r := range sub.Roles {
		if r == model.RoleOwner || r == model.RoleAdmin {
			return true
		}
	}
	return isBot(sub.User.Account)
}
