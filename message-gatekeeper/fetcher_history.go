package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/bytedance/sonic"
	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/subject"
)

// historyRequestTimeout matches the nats.go default request timeout.
const historyRequestTimeout = 2 * time.Second

// historyParentFetcher implements ParentMessageFetcher by issuing a NATS
// request to history-service's GetMessageByID handler. The base URL is used
// to build messageLink; it is injected so unit tests can supply any value.
type historyParentFetcher struct {
	nc          *o11ynats.Conn
	chatBaseURL string
}

func newHistoryParentFetcher(nc *o11ynats.Conn, chatBaseURL string) *historyParentFetcher {
	return &historyParentFetcher{nc: nc, chatBaseURL: chatBaseURL}
}

// getMessageByIDRequest mirrors history-service's GetMessageByIDRequest wire
// shape (the source struct lives under internal/ and isn't importable).
type getMessageByIDRequest struct {
	MessageID string `json:"messageId"`
}

// quotedParentProjection decodes only the fields FetchQuotedParent copies into
// the snapshot. It deliberately omits the full cassandra.Message — most notably
// the marshal-only Reactions map (struct-keyed, no UnmarshalJSON) whose decoder
// sonic rejects — so the reply decodes under sonic with no codec exception.
//
// MessageID is intentionally absent: a get-by-id reply's message_id is
// tautologically the requested id (history queries WHERE message_id = ?), so the
// caller's param is authoritative. RoomID is kept from the reply so the snapshot
// records the message's actual room rather than trusting history-service's
// cross-room guard.
type quotedParentProjection struct {
	RoomID                string                  `json:"roomId"`
	Sender                cassandra.Participant   `json:"sender"`
	CreatedAt             time.Time               `json:"createdAt"`
	Msg                   string                  `json:"msg"`
	Mentions              []cassandra.Participant `json:"mentions"`
	DecodedAttachments    []cassandra.Attachment  `json:"attachments"`
	ThreadParentID        string                  `json:"threadParentId"`
	ThreadParentCreatedAt *time.Time              `json:"threadParentCreatedAt"`
	TShow                 bool                    `json:"tshow"`
}

// requestMessageByID issues history-service's GetMessageByID RPC for one
// message and decodes the reply into dst. Shared by the quote and forward
// fetches: both hit subject.MsgGet with the same payload and differ only in
// the projection they decode and in how their caller treats failure (quotes
// soft-fail, forwards hard-fail). label names the operation inside wrapped
// errors so the two call sites stay distinguishable in logs.
//
// A typed remote *errcode.Error is returned unwrapped so callers can preserve
// the upstream classification (a transient infra failure stays unavailable,
// not collapsed to not_found).
func (f *historyParentFetcher) requestMessageByID(
	ctx context.Context,
	label, account, roomID, siteID, messageID string,
	dst any,
) error {
	reqBytes, err := sonic.Marshal(getMessageByIDRequest{MessageID: messageID})
	if err != nil {
		return fmt.Errorf("marshal %s request: %w", label, err)
	}

	subj := subject.MsgGet(account, roomID, siteID)
	msg, err := f.nc.Request(ctx, subj, reqBytes, historyRequestTimeout)
	if err != nil {
		return fmt.Errorf("%s request: %w", label, err)
	}

	// Detect the errcode error envelope first; a real Message has no top-level
	// "error" field so this cannot false-positive.
	if ee, ok := errcode.Parse(msg.Data); ok && ee.Code.Valid() {
		return ee
	}

	if err := sonic.Unmarshal(msg.Data, dst); err != nil {
		return fmt.Errorf("unmarshal %s: %w", label, err)
	}
	return nil
}

// FetchQuotedParent issues the msg.get RPC against the quoted parent's room and
// projects the reply into a cassandra.QuotedParentMessage snapshot. Any error
// (NATS timeout, no responder, natsrouter error envelope, unmarshal failure) is
// returned — the caller treats every error as a soft-fail signal.
func (f *historyParentFetcher) FetchQuotedParent(
	ctx context.Context,
	account, roomID, siteID, messageID string,
) (*cassandra.QuotedParentMessage, error) {
	var parent quotedParentProjection
	if err := f.requestMessageByID(ctx, "quoted parent", account, roomID, siteID, messageID, &parent); err != nil {
		return nil, err
	}

	return &cassandra.QuotedParentMessage{
		MessageID:             messageID,     // param — tautological for a by-id reply
		RoomID:                parent.RoomID, // reply — the message's actual room
		Sender:                parent.Sender,
		CreatedAt:             parent.CreatedAt,
		Msg:                   parent.Msg,
		Mentions:              parent.Mentions,
		DecodedAttachments:    parent.DecodedAttachments,
		MessageLink:           messageLink(f.chatBaseURL, parent.RoomID, messageID),
		ThreadParentID:        parent.ThreadParentID,
		ThreadParentCreatedAt: parent.ThreadParentCreatedAt,
		TShow:                 parent.TShow,
	}, nil
}

// forwardSourceProjection decodes only the source-message fields the forward
// path needs: the snapshot fields plus the accept/reject signals. Same sonic
// rationale as quotedParentProjection — never decode the full cassandra.Message
// (its struct-keyed Reactions map breaks sonic's decoder). Presence-only
// fields decode as json.RawMessage: the handler branches on presence, never
// the inner shape. All three are omitempty on the wire, so they are absent
// (len 0) rather than JSON null when unset.
type forwardSourceProjection struct {
	RoomID                string                  `json:"roomId"`
	Sender                cassandra.Participant   `json:"sender"`
	CreatedAt             time.Time               `json:"createdAt"`
	Msg                   string                  `json:"msg"`
	Mentions              []cassandra.Participant `json:"mentions"`
	ThreadParentID        string                  `json:"threadParentId"`
	ThreadParentCreatedAt *time.Time              `json:"threadParentCreatedAt"`
	ThreadRoomID          string                  `json:"threadRoomId"`
	TShow                 bool                    `json:"tshow"`
	Deleted               bool                    `json:"deleted"`
	Type                  string                  `json:"type"`
	Attachments           json.RawMessage         `json:"attachments"`      // presence-only
	Card                  json.RawMessage         `json:"card"`             // presence-only
	ForwardedMessage      json.RawMessage         `json:"forwardedMessage"` // presence-only (chain detection)
}

// FetchForwardedSource issues the msg.get RPC against the SOURCE room and
// projects the reply. Every error (timeout, no responder, errcode envelope,
// unmarshal) is returned — the caller hard-fails the send on any of them.
func (f *historyParentFetcher) FetchForwardedSource(
	ctx context.Context,
	account, srcRoomID, siteID, messageID string,
) (*forwardSourceProjection, error) {
	var src forwardSourceProjection
	if err := f.requestMessageByID(ctx, "forward source", account, srcRoomID, siteID, messageID, &src); err != nil {
		return nil, err
	}
	return &src, nil
}
