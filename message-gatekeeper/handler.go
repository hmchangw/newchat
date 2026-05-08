package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
)

const maxContentBytes = 20 * 1024 // 20 KB

// infraError represents a transient failure that should be nack'd for retry.
type infraError struct {
	cause error
}

func (e *infraError) Error() string {
	return e.cause.Error()
}

func (e *infraError) Unwrap() error {
	return e.cause
}

// replyFunc is the function signature for publishing a reply to a NATS subject.
type replyFunc func(ctx context.Context, msg *nats.Msg) error

// publishFunc is the function signature for publishing to JetStream.
type publishFunc func(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error)

// Handler processes messages from the MESSAGES stream and validates them
// before publishing to MESSAGES_CANONICAL.
type Handler struct {
	store              Store
	publish            publishFunc
	reply              replyFunc
	siteID             string
	parentFetcher      ParentMessageFetcher
	largeRoomThreshold int
}

// NewHandler constructs a new Handler with the given dependencies.
func NewHandler(store Store, publish publishFunc, reply replyFunc, siteID string, parentFetcher ParentMessageFetcher, largeRoomThreshold int) *Handler {
	return &Handler{
		store:              store,
		publish:            publish,
		reply:              reply,
		siteID:             siteID,
		parentFetcher:      parentFetcher,
		largeRoomThreshold: largeRoomThreshold,
	}
}

// HandleJetStreamMsg processes a JetStream message from the MESSAGES stream.
func (h *Handler) HandleJetStreamMsg(ctx context.Context, msg jetstream.Msg) {
	account, roomID, siteID, ok := subject.ParseUserRoomSiteSubject(msg.Subject())
	if !ok {
		slog.Warn("invalid subject", "subject", msg.Subject())
		if err := msg.Ack(); err != nil {
			slog.Error("failed to ack message", "error", err)
		}
		return
	}

	replyData, err := h.processMessage(ctx, account, roomID, siteID, msg.Data())
	if err != nil {
		slog.Error("process message failed", "error", err, "account", account, "roomID", roomID)
		var ie *infraError
		if errors.As(err, &ie) {
			if err := msg.Nak(); err != nil {
				slog.Error("failed to nack message", "error", err)
			}
		} else {
			// Validation error: reply with error and ack.
			h.sendReply(ctx, account, msg.Data(), h.marshalErrorReply(err))
			if err := msg.Ack(); err != nil {
				slog.Error("failed to ack message", "error", err)
			}
		}
		return
	}

	h.sendReply(ctx, account, msg.Data(), replyData)

	if err := msg.Ack(); err != nil {
		slog.Error("failed to ack message", "err", err)
	}
}

// sendReply extracts the requestID from the raw message data and publishes the
// reply payload to the user's response subject.
func (h *Handler) sendReply(ctx context.Context, account string, rawData []byte, replyData []byte) {
	var req model.SendMessageRequest
	if err := json.Unmarshal(rawData, &req); err != nil {
		slog.Error("unmarshal request for reply", "error", err)
		return
	}
	if req.RequestID == "" {
		return
	}
	respSubj := subject.UserResponse(account, req.RequestID)
	replyMsg := natsutil.NewMsg(ctx, respSubj, replyData)
	if err := h.reply(ctx, replyMsg); err != nil {
		slog.Error("reply to client failed", "error", err, "subject", respSubj)
	}
}

// processMessage validates a SendMessageRequest and publishes a MessageEvent to MESSAGES_CANONICAL.
// Returns the serialized Message on success, or an error.
// Validation errors (bad input) are plain errors; transient failures are *infraError.
func (h *Handler) processMessage(ctx context.Context, account, roomID, siteID string, data []byte) ([]byte, error) {
	// Validate siteID matches this service's siteID
	if siteID != h.siteID {
		return nil, fmt.Errorf("siteID mismatch: got %s, want %s", siteID, h.siteID)
	}

	// Unmarshal request
	var req model.SendMessageRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, fmt.Errorf("unmarshal send message request: %w", err)
	}

	// Validate ID is a valid 20-char base62 message ID
	if !idgen.IsValidMessageID(req.ID) {
		return nil, fmt.Errorf("invalid message ID %q: must be a 20-char base62 string", req.ID)
	}

	if req.ThreadParentMessageID != "" && !idgen.IsValidMessageID(req.ThreadParentMessageID) {
		return nil, fmt.Errorf("invalid thread parent message ID %q: must be a 20-char base62 string", req.ThreadParentMessageID)
	}

	// Validate content is non-empty
	if req.Content == "" {
		return nil, fmt.Errorf("content must not be empty")
	}

	// Validate content does not exceed 20KB
	if len(req.Content) > maxContentBytes {
		return nil, fmt.Errorf("content exceeds maximum size of %d bytes", maxContentBytes)
	}

	// Validate thread parent fields are paired
	if req.ThreadParentMessageID != "" && req.ThreadParentMessageCreatedAt == nil {
		return nil, fmt.Errorf("validate thread parent fields: threadParentMessageCreatedAt is required when threadParentMessageId is set")
	}

	// Verify subscription
	sub, err := h.store.GetSubscription(ctx, account, roomID)
	if err != nil {
		if errors.Is(err, errNotSubscribed) {
			return nil, fmt.Errorf("user %s is not subscribed to room %s", account, roomID)
		}
		return nil, &infraError{cause: fmt.Errorf("get subscription for user %s in room %s: %w", account, roomID, err)}
	}

	// Large-room post restriction: in rooms with more than the configured
	// threshold of members, only owners, admins, and bots may send top-level
	// messages. Thread replies are exempt regardless of room size; bypass-eligible
	// senders (owner/admin role, or bot account name) are exempt regardless of
	// room size. Both bypasses skip the Room fetch entirely (approach B —
	// owner fast-path generalized).
	isThreadReply := req.ThreadParentMessageID != ""
	if !isThreadReply && !canBypassLargeRoomCap(sub) {
		userCount, err := h.store.GetRoomUserCount(ctx, roomID)
		if err != nil {
			return nil, &infraError{cause: fmt.Errorf("get user count for room %s: %w", roomID, err)}
		}
		if userCount > h.largeRoomThreshold {
			slog.Info("send blocked",
				"reason", codeLargeRoomPostRestricted,
				"account", account,
				"roomID", roomID,
				"userCount", userCount,
				"threshold", h.largeRoomThreshold,
			)
			return nil, errLargeRoomPostRestricted
		}
	}

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

	msg := model.Message{
		ID:                           req.ID,
		RoomID:                       roomID,
		UserID:                       sub.User.ID,
		UserAccount:                  sub.User.Account,
		Content:                      req.Content,
		CreatedAt:                    now,
		ThreadParentMessageID:        req.ThreadParentMessageID,
		ThreadParentMessageCreatedAt: threadParentCreatedAt,
		QuotedParentMessage:          quotedSnapshot,
	}

	// Publish MessageEvent to MESSAGES_CANONICAL
	evt := model.MessageEvent{Message: msg, SiteID: siteID, Timestamp: now.UnixMilli()}
	evtData, err := json.Marshal(evt)
	if err != nil {
		return nil, &infraError{cause: fmt.Errorf("marshal message event: %w", err)}
	}

	canonicalSubj := subject.MsgCanonicalCreated(siteID)
	canonicalMsg := natsutil.NewMsg(ctx, canonicalSubj, evtData)
	if _, err := h.publish(ctx, canonicalMsg, jetstream.WithMsgID(msg.ID)); err != nil {
		return nil, &infraError{cause: fmt.Errorf("publish to MESSAGES_CANONICAL: %w", err)}
	}

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
		return nil, fmt.Errorf("fetch quoted parent %s: %w", quotedParentMessageID, err)
	case snap == nil:
		// Treat the fetcher's contract violation as a hard failure rather than
		// silently dereferencing snap.ThreadParentID below.
		return nil, fmt.Errorf("fetch quoted parent %s: fetcher returned nil snapshot", quotedParentMessageID)
	case snap.ThreadParentID != newMessageThreadID:
		return nil, fmt.Errorf("quoted parent %s thread context mismatch: parent thread %q, new message thread %q",
			quotedParentMessageID, snap.ThreadParentID, newMessageThreadID)
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

// marshalErrorReply produces the JSON reply payload for a validation error.
// If the error is (or wraps) a *codedError, the reply carries the code;
// otherwise the reply is the legacy uncoded shape.
func (h *Handler) marshalErrorReply(err error) []byte {
	var ce *codedError
	if errors.As(err, &ce) {
		return natsutil.MarshalErrorWithCode(ce.Message, ce.Code)
	}
	return natsutil.MarshalError(err.Error())
}
