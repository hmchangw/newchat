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

// LastMessageFetcher resolves a room's surviving last-message state from
// history-service, so a delete event can carry the room-list preview and the
// Mongo rewind can re-point room sorting: the preview is the newest surviving
// non-system message, the pointer the newest surviving message of ANY type
// (system notices included). before is the walk ceiling — callers pass the
// delete-event time so survivors newer than the (possibly coalescer-lagged)
// stored lastMsgAt stay in the window. A nil pointer with a nil error is the
// valid "no surviving message" signal for an emptied room; preview non-nil
// implies pointer non-nil.
type LastMessageFetcher interface {
	FetchLastMessage(ctx context.Context, roomID string, before time.Time) (*model.LastMessagePreview, *model.LastMessagePointer, error)
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

// FetchLastMessage requests the room's surviving last-message state at
// subject.MsgRoomLast(siteID). Any error (timeout, no responder, remote
// errcode envelope, unmarshal) is wrapped and returned so the caller NAKs.
func (f *historyLastMessageFetcher) FetchLastMessage(ctx context.Context, roomID string, before time.Time) (*model.LastMessagePreview, *model.LastMessagePointer, error) {
	reqBytes, err := sonic.Marshal(model.LastRoomMessageRequest{RoomID: roomID, Before: before.UnixMilli()})
	if err != nil {
		return nil, nil, fmt.Errorf("marshal LastRoomMessage request: %w", err)
	}
	msg, err := f.nc.Request(ctx, subject.MsgRoomLast(f.siteID), reqBytes, lastMsgFetchTimeout)
	if err != nil {
		return nil, nil, fmt.Errorf("history request for room %s last message: %w", roomID, err)
	}
	// The errcode envelope has a top-level "error"; a real response never does, so
	// this can't false-positive. Propagate the typed remote error for accurate
	// classification.
	if ee, ok := errcode.Parse(msg.Data); ok && ee.Code.Valid() {
		return nil, nil, ee
	}
	var resp model.LastRoomMessageResponse
	if err := sonic.Unmarshal(msg.Data, &resp); err != nil {
		// Deliberately NOT wrapped: decode errors quote reply fragments, which
		// for plaintext rooms is message content, and this error is logged at
		// the jsretry boundary. The byte count is enough to debug a bad reply.
		return nil, nil, fmt.Errorf("unmarshal last room message for room %s: malformed reply (%d bytes)", roomID, len(msg.Data))
	}
	pointer := resp.Pointer
	if pointer == nil && resp.LastMessage != nil {
		// Pre-pointer server during a rolling deploy: the preview row IS the
		// newest survivor it knows about — derive the pointer from it.
		pointer = &model.LastMessagePointer{MessageID: resp.LastMessage.MessageID, CreatedAt: resp.LastMessage.CreatedAt}
	}
	return resp.LastMessage, pointer, nil
}
