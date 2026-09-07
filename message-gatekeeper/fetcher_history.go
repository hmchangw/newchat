package main

import (
	"context"
	"fmt"
	"time"

	"github.com/bytedance/sonic"
	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/natsutil"
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
	// metrics is injected: building a second natsmetrics.Metrics here would
	// duplicate all nine shared instruments on a different construction path.
	// The zero value is safe and records nothing.
	metrics natsmetrics.Publisher
}

func newHistoryParentFetcher(nc *o11ynats.Conn, chatBaseURL string, metrics natsmetrics.Publisher) *historyParentFetcher {
	return &historyParentFetcher{nc: nc, chatBaseURL: chatBaseURL, metrics: metrics}
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
) (_ *cassandra.QuotedParentMessage, resultErr error) {
	reqBytes, err := sonic.Marshal(getMessageByIDRequest{MessageID: messageID})
	if err != nil {
		return nil, errcode.MarshalFailed("GetMessageByID request", err)
	}

	subj := subject.MsgGet(account, roomID, siteID)
	// Recorded from the function's final outcome, not from nc.Request's return.
	// A remote errcode envelope and a decode failure both arrive as a
	// successful transport read, so recording at the call site labelled them
	// success and lost error.type — the client histogram then disagreed with
	// the callee's server histogram about whether the call failed.
	started := time.Now()
	defer func() {
		f.metrics.RecordRPCClientCall(ctx, natsmetrics.MethodGetMessage, time.Since(started), resultErr)
	}()
	msg, err := f.nc.Request(ctx, subj, reqBytes, historyRequestTimeout)
	if err != nil {
		return nil, natsutil.RequestFailure("history request", err)
	}

	// FromReply, not Parse: an envelope is always a failure, and an
	// unrecognised code must not be relayed as a typed *errcode.Error. See its
	// doc comment for the two ways hand-rolling this goes wrong.
	if remoteErr := errcode.FromReply(msg.Data); remoteErr != nil {
		return nil, remoteErr
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
