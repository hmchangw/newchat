package main

import (
	"context"
	"fmt"
	"time"

	"github.com/bytedance/sonic"

	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

// LastMessageFetcher resolves a room's newest surviving (non-deleted,
// non-system) message preview from history-service, so a delete event can
// carry the room-list preview. A nil preview with a nil error is the valid
// "no surviving message" signal for an emptied room.
type LastMessageFetcher interface {
	FetchLastMessage(ctx context.Context, roomID string) (*model.LastMessagePreview, error)
}

// lastMsgFetchTimeout matches the nats.go default request timeout.
const lastMsgFetchTimeout = 2 * time.Second

// historyLastMessageFetcher resolves the surviving room preview via a NATS
// request to history-service's last-room-message handler on this site.
type historyLastMessageFetcher struct {
	nc     *o11ynats.Conn
	siteID string
}

func newHistoryLastMessageFetcher(nc *o11ynats.Conn, siteID string) *historyLastMessageFetcher {
	return &historyLastMessageFetcher{nc: nc, siteID: siteID}
}

// FetchLastMessage requests the room's newest surviving message preview at
// subject.MsgRoomLast(siteID). Any error (timeout, no responder, remote
// errcode envelope, unmarshal) is wrapped and returned so the caller NAKs.
func (f *historyLastMessageFetcher) FetchLastMessage(ctx context.Context, roomID string) (*model.LastMessagePreview, error) {
	reqBytes, err := sonic.Marshal(model.LastRoomMessageRequest{RoomID: roomID})
	if err != nil {
		return nil, fmt.Errorf("marshal LastRoomMessage request: %w", err)
	}
	msg, err := f.nc.Request(ctx, subject.MsgRoomLast(f.siteID), reqBytes, lastMsgFetchTimeout)
	if err != nil {
		return nil, fmt.Errorf("history request for room %s last message: %w", roomID, err)
	}
	// The errcode envelope has a top-level "error"; a real response never does, so
	// this can't false-positive. Propagate the typed remote error for accurate
	// classification.
	if ee, ok := errcode.Parse(msg.Data); ok && ee.Code.Valid() {
		return nil, ee
	}
	var resp model.LastRoomMessageResponse
	if err := sonic.Unmarshal(msg.Data, &resp); err != nil {
		// Deliberately NOT wrapped: decode errors quote reply fragments, which
		// for plaintext rooms is message content, and this error is logged at
		// the jsretry boundary. The byte count is enough to debug a bad reply.
		return nil, fmt.Errorf("unmarshal last room message for room %s: malformed reply (%d bytes)", roomID, len(msg.Data))
	}
	return resp.LastMessage, nil
}
