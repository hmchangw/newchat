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

// FetchQuotedParent issues a NATS request to history-service's GetMessageByID
// handler at subject.MsgGet(account, roomID, siteID). On a successful reply,
// projects the returned cassandra.Message into a cassandra.QuotedParentMessage
// snapshot. Any error (NATS timeout, no responder, natsrouter error envelope,
// unmarshal failure) is wrapped and returned — the caller treats every error
// as a soft-fail signal.
func (f *historyParentFetcher) FetchQuotedParent(
	ctx context.Context,
	account, roomID, siteID, messageID string,
) (*cassandra.QuotedParentMessage, error) {
	reqBytes, err := sonic.Marshal(getMessageByIDRequest{MessageID: messageID})
	if err != nil {
		return nil, fmt.Errorf("marshal GetMessageByID request: %w", err)
	}

	subj := subject.MsgGet(account, roomID, siteID)
	msg, err := f.nc.Request(ctx, subj, reqBytes, historyRequestTimeout)
	if err != nil {
		return nil, fmt.Errorf("history request: %w", err)
	}

	// Detect the errcode error envelope first; a real Message has no top-level
	// "error" field so this cannot false-positive. Propagate the typed remote
	// errcode so the caller can preserve the upstream classification (a
	// transient infra failure stays unavailable, not collapsed to not_found).
	if ee, ok := errcode.Parse(msg.Data); ok && ee.Code.Valid() {
		return nil, ee
	}

	var parent quotedParentProjection
	if err := sonic.Unmarshal(msg.Data, &parent); err != nil {
		return nil, fmt.Errorf("unmarshal parent message: %w", err)
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
	Deleted               bool                    `json:"deleted"`
	Type                  string                  `json:"type"`
	Attachments           json.RawMessage         `json:"attachments"`      // presence-only
	Card                  json.RawMessage         `json:"card"`             // presence-only
	ForwardedMessage      json.RawMessage         `json:"forwardedMessage"` // presence-only (chain detection)
	// MessageLink is built by the fetcher from chatBaseURL, not decoded from the reply.
	MessageLink string `json:"-"`
}

// FetchForwardedSource issues a NATS request to history-service's
// GetMessageByID handler on the SOURCE room's subject and projects the reply.
// Every error (timeout, no responder, errcode envelope, unmarshal) is
// returned — the caller hard-fails the send on any of them.
func (f *historyParentFetcher) FetchForwardedSource(
	ctx context.Context,
	account, srcRoomID, siteID, messageID string,
) (*forwardSourceProjection, error) {
	reqBytes, err := sonic.Marshal(getMessageByIDRequest{MessageID: messageID})
	if err != nil {
		return nil, fmt.Errorf("marshal GetMessageByID request: %w", err)
	}

	subj := subject.MsgGet(account, srcRoomID, siteID)
	msg, err := f.nc.Request(ctx, subj, reqBytes, historyRequestTimeout)
	if err != nil {
		return nil, fmt.Errorf("forward source request: %w", err)
	}

	if ee, ok := errcode.Parse(msg.Data); ok && ee.Code.Valid() {
		return nil, ee
	}

	var src forwardSourceProjection
	if err := sonic.Unmarshal(msg.Data, &src); err != nil {
		return nil, fmt.Errorf("unmarshal forward source: %w", err)
	}
	src.MessageLink = messageLink(f.chatBaseURL, src.RoomID, messageID)
	return &src, nil
}
