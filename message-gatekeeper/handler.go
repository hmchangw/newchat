package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/bytedance/sonic"

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

// quotedParentUnavailablePlaceholder is the degraded-mode quoted-parent body used
// when the authoritative fetch fails transiently. It never persists —
// message-worker re-projects the real snapshot before the durable write.
const quotedParentUnavailablePlaceholder = "Content temporarily unavailable"

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
	maxAttachments     int
	maxAttachmentBytes int
	// chatBaseURL builds the messageLink on the degraded-mode placeholder quoted
	// snapshot, from trusted inputs (the send room + the validated quoted message
	// ID) so the link is correct even on the outage path.
	chatBaseURL string
}

// NewHandler constructs a new Handler with the given dependencies.
// users may be nil; when nil, sender display-name resolution is skipped and
// downstream consumers fall back to UserAccount.
func NewHandler(store Store, users UserGetter, publish publishFunc, reply replyFunc, siteID string, parentFetcher ParentMessageFetcher, largeRoomThreshold, maxAttachments, maxAttachmentBytes int, chatBaseURL string) *Handler {
	return &Handler{
		store:              store,
		users:              users,
		publish:            publish,
		reply:              reply,
		siteID:             siteID,
		parentFetcher:      parentFetcher,
		largeRoomThreshold: largeRoomThreshold,
		maxAttachments:     maxAttachments,
		maxAttachmentBytes: maxAttachmentBytes,
		chatBaseURL:        chatBaseURL,
	}
}

// HandleJetStreamMsg processes a JetStream message from the MESSAGES stream.
func (h *Handler) HandleJetStreamMsg(ctx context.Context, msg jetstream.Msg) {
	// Parse the body once; reused for log enrichment, reply routing, and
	// processMessage validation (was triple-decoded on the hot path).
	rawData := msg.Data()
	var req model.SendMessageRequest
	parseErr := sonic.Unmarshal(rawData, &req)

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
		// Do not WithCause(parseErr) — JSON parse error strings embed the
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

	// Validate the quoted parent ID at the boundary too: on the degrade path it is
	// copied verbatim into the snapshot MessageID and messageLink, so a malformed
	// value must fail fast rather than leak into the canonical event.
	if req.QuotedParentMessageID != "" && !idgen.IsValidMessageID(req.QuotedParentMessageID) {
		return nil, errcode.BadRequest(fmt.Sprintf("invalid quoted parent message ID %q: must be a 20-char base62 string", req.QuotedParentMessageID))
	}

	// A message with attachments may carry empty content.
	if req.Content == "" && len(req.Attachments) == 0 {
		return nil, errcode.BadRequest("content must not be empty")
	}

	// Validate content does not exceed 20KB
	if len(req.Content) > maxContentBytes {
		return nil, errcode.BadRequest(
			fmt.Sprintf("content exceeds maximum size of %d bytes", maxContentBytes),
			errcode.WithMetadata("maxContentBytes", strconv.Itoa(maxContentBytes), "attempted", strconv.Itoa(len(req.Content))),
		)
	}

	// Validate attachments: count + total byte caps. Blobs are otherwise opaque
	// here (decoded leniently on the read path) — but an empty blob is rejected
	// since it carries no attachment and would yield a contentless message.
	if len(req.Attachments) > h.maxAttachments {
		return nil, errcode.BadRequest(fmt.Sprintf("too many attachments: max %d", h.maxAttachments))
	}
	var attachmentBytes int
	for i, a := range req.Attachments {
		if len(a) == 0 {
			return nil, errcode.BadRequest(fmt.Sprintf("attachment[%d] must not be empty", i))
		}
		attachmentBytes += len(a)
	}
	if attachmentBytes > h.maxAttachmentBytes {
		return nil, errcode.BadRequest(fmt.Sprintf("attachments exceed maximum size of %d bytes", h.maxAttachmentBytes))
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

	quotedSnapshot, quotedUnverified, err := h.resolveQuoteSnapshot(ctx, account, roomID, siteID, req.QuotedParentMessageID, req.ThreadParentMessageID, now)
	if err != nil {
		return nil, err
	}
	if req.QuotedParentMessageID != "" {
		// debug: quote passed the same-conversation-context check.
		slog.DebugContext(ctx, "gatekeeper quote resolved", "request_id", req.RequestID, "quoted_id", req.QuotedParentMessageID, "unverified", quotedUnverified)
	}

	// Resolve the thread parent's createdAt + sender account server-side,
	// best-effort: a fetch failure ships the event without the values (each
	// consumer falls back to a store it owns), so a Cassandra outage never blocks
	// the send path. Both ride the same fetch.
	threadParentCreatedAt, threadParentSenderAccount := h.resolveThreadParent(ctx, account, roomID, siteID, req, quotedSnapshot, quotedUnverified)

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
		Attachments:                  req.Attachments,
	}

	// Publish MessageEvent to MESSAGES_CANONICAL. QuotedParentUnverified rides the
	// envelope (not the persisted Message) so message-worker knows to re-project
	// the authoritative snapshot before the durable write when the gatekeeper had
	// to fall back to the untrusted client snapshot.
	evt := model.MessageEvent{Event: model.EventCreated, Message: msg, SiteID: siteID, Timestamp: now.UnixMilli(), QuotedParentUnverified: quotedUnverified, ThreadParentSenderAccount: threadParentSenderAccount}
	evtData, err := sonic.Marshal(evt)
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

	return sonic.Marshal(msg)
}

// resolveThreadParent resolves the thread parent's createdAt and sender account
// server-side, returning (nil, "") for a non-thread reply. It reuses the quote
// snapshot when the parent is also the verified quoted message (the unverified
// placeholder carries a synthetic timestamp), otherwise fetches by ID. Both
// values come from the same snapshot in one fetch. Best-effort: any failure logs
// a warning and returns (nil, "") — downstream consumers fall back to their own
// stores.
func (h *Handler) resolveThreadParent(
	ctx context.Context,
	account, roomID, siteID string,
	req *model.SendMessageRequest,
	quotedSnapshot *cassandra.QuotedParentMessage,
	quotedUnverified bool,
) (*time.Time, string) {
	if req.ThreadParentMessageID == "" {
		return nil, ""
	}
	if quotedSnapshot != nil && !quotedUnverified && req.QuotedParentMessageID == req.ThreadParentMessageID {
		t := quotedSnapshot.CreatedAt.UTC()
		return &t, quotedSnapshot.Sender.Account
	}
	if h.parentFetcher == nil {
		return nil, ""
	}
	snap, err := h.parentFetcher.FetchQuotedParent(ctx, account, roomID, siteID, req.ThreadParentMessageID)
	if err != nil || snap == nil {
		slog.WarnContext(ctx, "thread parent resolution failed, publishing without it",
			"error", err,
			"parent_message_id", req.ThreadParentMessageID,
			"request_id", req.RequestID)
		return nil, ""
	}
	t := snap.CreatedAt.UTC()
	return &t, snap.Sender.Account
}

// resolveQuoteSnapshot resolves the quoted parent into a snapshot, preferring the
// authoritative history fetch and enforcing the same-conversation rule (a message
// may only quote parents in its own thread or main room). The bool is the
// "unverified" marker — true when the snapshot is the degraded placeholder.
//
// Fetch failures are tiered:
//   - terminal (not_found, forbidden, bad_request): reject — a quote must never
//     resurrect a missing parent or bypass access control.
//   - transient (history unavailable/internal, NATS timeout): degrade to a
//     placeholder (marked unverified) with the fixed "Content temporarily
//     unavailable" body — which message-worker re-projects (or drops) before the
//     durable write.
func (h *Handler) resolveQuoteSnapshot(ctx context.Context, account, roomID, siteID, quotedParentMessageID, newMessageThreadID string, now time.Time) (*cassandra.QuotedParentMessage, bool, error) {
	if quotedParentMessageID == "" {
		return nil, false, nil
	}
	snap, err := h.parentFetcher.FetchQuotedParent(ctx, account, roomID, siteID, quotedParentMessageID)
	if err == nil && snap == nil {
		// A nil snapshot with no error is a fetcher contract violation, not a
		// genuine missing parent. Synthesize a transient error so we degrade to the
		// placeholder rather than treating it as authoritative-empty.
		err = fmt.Errorf("fetch quoted parent %s: fetcher returned nil snapshot", quotedParentMessageID)
	}
	if err != nil {
		var ee *errcode.Error
		if quoteFetchErrIsTerminal(err) && errors.As(err, &ee) {
			// Terminal typed *errcode.Error → reply + Ack; preserves the upstream
			// category (not_found, forbidden, …) for the client. The errors.As guard
			// is belt-and-suspenders: quoteFetchErrIsTerminal only reports true for a
			// typed errcode today, but guarding the bool means a future predicate
			// change can't fall through here returning a nil *errcode.Error.
			return nil, false, ee
		}
		// Transient failure: degrade to a placeholder snapshot rather than NAK, so
		// the message still flows through the outage. message-worker re-projects the
		// authoritative snapshot (or drops the quote) before the durable write.
		ph := h.placeholderQuoteSnapshot(roomID, quotedParentMessageID, newMessageThreadID, now)
		slog.WarnContext(ctx, "quoted-parent fetch failed; using placeholder snapshot",
			"request_id", natsutil.RequestIDFromContext(ctx), "quoted_id", quotedParentMessageID, "error", err)
		return ph, true, nil
	}
	if cerr := checkQuoteThreadContext(snap, quotedParentMessageID, newMessageThreadID, roomID); cerr != nil {
		return nil, false, cerr
	}
	return snap, false, nil
}

// quoteFetchErrIsTerminal reports whether a quoted-parent fetch error is a
// permanent reason not to quote (reject) vs a transient infra failure (degrade
// to the placeholder). Only unavailable/internal errcodes and non-errcode infra
// failures (NATS timeout, no-responders, unmarshal) are transient; every other
// errcode category (not_found, forbidden, bad_request, …) is terminal.
// history-service collapses a Cassandra read failure to code=internal, so
// internal is treated as transient here.
func quoteFetchErrIsTerminal(err error) bool {
	var ee *errcode.Error
	if errors.As(err, &ee) {
		switch ee.Code {
		case errcode.CodeUnavailable, errcode.CodeInternal:
			return false
		default:
			return true
		}
	}
	return false
}

// placeholderQuoteSnapshot builds the degraded-mode quoted-parent snapshot for a
// transient fetch failure. Only identity/link fields are real; the body is always
// the fixed "Content temporarily unavailable" placeholder (the client cannot
// supply the quoted-parent content). ThreadParentID mirrors the quoting message's
// thread to satisfy the same-conversation rule, and CreatedAt is cosmetic. The
// caller marks it unverified (see resolveQuoteSnapshot); the body is re-projected
// authoritatively by message-worker before the durable write.
func (h *Handler) placeholderQuoteSnapshot(roomID, messageID, newMessageThreadID string, now time.Time) *cassandra.QuotedParentMessage {
	return &cassandra.QuotedParentMessage{
		MessageID:      messageID,
		RoomID:         roomID,
		CreatedAt:      now,
		Msg:            quotedParentUnavailablePlaceholder,
		MessageLink:    messageLink(h.chatBaseURL, roomID, messageID),
		ThreadParentID: newMessageThreadID,
	}
}

// checkQuoteThreadContext enforces the same-conversation rule between the quoted
// parent and the quoting message. newMessageRoomID is the quoting message's own
// room, needed for the TShow carve-out below.
func checkQuoteThreadContext(snap *cassandra.QuotedParentMessage, quotedParentMessageID, newMessageThreadID, newMessageRoomID string) error {
	// TShow ("also send to channel") carve-out: the parent also lives in its channel
	// room, so quoting it from that room (a main-room message) is legitimate.
	tshowChannelQuote := snap.TShow && newMessageThreadID == "" && snap.RoomID == newMessageRoomID
	if snap.ThreadParentID != newMessageThreadID &&
		// Thread-root quote: starter is a main-room msg (ThreadParentID=="") whose ID is the thread parent — allowed.
		(snap.ThreadParentID != "" || quotedParentMessageID != newMessageThreadID) &&
		!tshowChannelQuote {
		return errcode.BadRequest(fmt.Sprintf("quoted parent %s thread context mismatch: parent thread %q, new message thread %q",
			quotedParentMessageID, snap.ThreadParentID, newMessageThreadID))
	}
	return nil
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
