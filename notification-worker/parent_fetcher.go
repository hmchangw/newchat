package main

import (
	"context"
	"fmt"
	"time"

	"github.com/bytedance/sonic"

	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
)

// parentFetchTimeout matches the nats.go default request timeout.
const parentFetchTimeout = 2 * time.Second

// ParentMessageInfo is the subset of a thread's parent message the notification
// fan-out needs: the author (always a recipient) and the creation time (feeds the
// restricted-room suppression gate).
type ParentMessageInfo struct {
	SenderAccount string
	CreatedAt     time.Time
}

// ParentFetcher resolves a thread's parent message from history-service. The parent
// pre-exists (a reply targets it), so this is race-free — unlike thread_rooms, which
// message-worker may not have created yet on the first reply.
type ParentFetcher interface {
	FetchParent(ctx context.Context, account, roomID, siteID, messageID string) (*ParentMessageInfo, error)
}

// historyParentFetcher resolves the parent via a NATS request to history-service's
// GetMessageByID handler, reading the author + createdAt the fan-out needs.
type historyParentFetcher struct {
	nc *o11ynats.Conn
	// metrics is injected: building a second natsmetrics.Metrics here would
	// duplicate all nine shared instruments on a different construction path.
	// The zero value is safe and records nothing.
	metrics natsmetrics.Publisher
}

func newHistoryParentFetcher(nc *o11ynats.Conn, metrics natsmetrics.Publisher) *historyParentFetcher {
	return &historyParentFetcher{nc: nc, metrics: metrics}
}

// getMessageByIDRequest mirrors history-service's GetMessageByIDRequest wire shape.
type getMessageByIDRequest struct {
	MessageID string `json:"messageId"`
}

// parentMessageProjection decodes only what the fan-out needs — createdAt and the
// sender's account — rather than the full cassandra.Message (whose marshal-only
// Reactions map sonic can't decode).
type parentMessageProjection struct {
	CreatedAt time.Time `json:"createdAt"`
	Sender    struct {
		Account string `json:"account"`
	} `json:"sender"`
}

// FetchParent requests the parent message at subject.MsgGet(account, roomID, siteID)
// and projects it to ParentMessageInfo. account is the reply sender, who can always
// see the parent they are replying to. Any error (timeout, no responder, remote
// errcode envelope, unmarshal) is wrapped and returned so the caller NAKs.
func (f *historyParentFetcher) FetchParent(ctx context.Context, account, roomID, siteID, messageID string) (_ *ParentMessageInfo, resultErr error) {
	reqBytes, err := sonic.Marshal(getMessageByIDRequest{MessageID: messageID})
	if err != nil {
		return nil, errcode.MarshalFailed("GetMessageByID request", err)
	}
	// Recorded from the function's final outcome, not from nc.Request's return.
	// A remote errcode envelope and a decode failure both arrive as a
	// successful transport read, so recording at the call site labelled them
	// success and lost error.type — the client histogram then disagreed with
	// the callee's server histogram about whether the call failed.
	started := time.Now()
	defer func() {
		f.metrics.RecordRPCClientCall(ctx, natsmetrics.MethodGetMessage, time.Since(started), resultErr)
	}()
	msg, err := f.nc.Request(ctx, subject.MsgGet(account, roomID, siteID), reqBytes, parentFetchTimeout)
	if err != nil {
		return nil, natsutil.RequestFailure(fmt.Sprintf("history request for parent %s", messageID), err)
	}
	// FromReply, not Parse: an envelope is always a failure, and an
	// unrecognised code must not be relayed as a typed *errcode.Error. See its
	// doc comment for the two ways hand-rolling this goes wrong.
	if remoteErr := errcode.FromReply(msg.Data); remoteErr != nil {
		return nil, remoteErr
	}
	var parent parentMessageProjection
	if err := sonic.Unmarshal(msg.Data, &parent); err != nil {
		return nil, fmt.Errorf("unmarshal parent message %s: %w", messageID, err)
	}
	return &ParentMessageInfo{SenderAccount: parent.Sender.Account, CreatedAt: parent.CreatedAt}, nil
}
