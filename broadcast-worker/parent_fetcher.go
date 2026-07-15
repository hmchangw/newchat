package main

import (
	"context"
	"fmt"
	"time"

	"github.com/bytedance/sonic"

	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/subject"
)

// parentFetchTimeout matches the nats.go default request timeout.
const parentFetchTimeout = 2 * time.Second

// historyParentFetcher resolves a thread's parent message via a NATS request to
// history-service's GetMessageByID handler, reading the author + createdAt the
// channel thread fan-out needs.
type historyParentFetcher struct {
	nc *o11ynats.Conn
}

func newHistoryParentFetcher(nc *o11ynats.Conn) *historyParentFetcher {
	return &historyParentFetcher{nc: nc}
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
func (f *historyParentFetcher) FetchParent(ctx context.Context, account, roomID, siteID, messageID string) (*ParentMessageInfo, error) {
	reqBytes, err := sonic.Marshal(getMessageByIDRequest{MessageID: messageID})
	if err != nil {
		return nil, fmt.Errorf("marshal GetMessageByID request: %w", err)
	}
	msg, err := f.nc.Request(ctx, subject.MsgGet(account, roomID, siteID), reqBytes, parentFetchTimeout)
	if err != nil {
		return nil, fmt.Errorf("history request for parent %s: %w", messageID, err)
	}
	// The errcode envelope has a top-level "error"; a real Message never does, so this
	// can't false-positive. Propagate the typed remote error for accurate classification.
	if ee, ok := errcode.Parse(msg.Data); ok && ee.Code.Valid() {
		return nil, ee
	}
	var parent parentMessageProjection
	if err := sonic.Unmarshal(msg.Data, &parent); err != nil {
		return nil, fmt.Errorf("unmarshal parent message %s: %w", messageID, err)
	}
	return &ParentMessageInfo{SenderAccount: parent.Sender.Account, CreatedAt: parent.CreatedAt}, nil
}
