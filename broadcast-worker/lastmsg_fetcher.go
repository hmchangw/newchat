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

// LastMessageFetcher resolves a room's surviving last-message state from history-service:
// preview (newest non-system) + pointer (newest any type); before caps the walk, nil pointer = empty.
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

// FetchLastMessage requests the surviving last-message state at
// subject.MsgRoomLast; any error is wrapped so the caller NAKs.
func (f *historyLastMessageFetcher) FetchLastMessage(ctx context.Context, roomID string, before time.Time) (*model.LastMessagePreview, *model.LastMessagePointer, error) {
	reqBytes, err := sonic.Marshal(model.LastRoomMessageRequest{RoomID: roomID, Before: before.UnixMilli()})
	if err != nil {
		return nil, nil, fmt.Errorf("marshal LastRoomMessage request: %w", err)
	}
	msg, err := f.nc.Request(ctx, subject.MsgRoomLast(f.siteID), reqBytes, lastMsgFetchTimeout)
	if err != nil {
		return nil, nil, fmt.Errorf("history request for room %s last message: %w", roomID, err)
	}
	// A real response never has a top-level "error", so this can't false-positive.
	if ee, ok := errcode.Parse(msg.Data); ok && ee.Code.Valid() {
		return nil, nil, ee
	}
	var resp model.LastRoomMessageResponse
	if err := sonic.Unmarshal(msg.Data, &resp); err != nil {
		// Not wrapped: decode errors quote reply fragments (message content for
		// plaintext rooms), and this is logged at the jsretry boundary.
		return nil, nil, fmt.Errorf("unmarshal last room message for room %s: malformed reply (%d bytes)", roomID, len(msg.Data))
	}
	pointer := resp.Pointer
	if pointer == nil && resp.LastMessage != nil {
		// Rolling deploy: a pre-pointer reply has no pointer — derive it from the preview.
		pointer = &model.LastMessagePointer{MessageID: resp.LastMessage.MessageID, CreatedAt: resp.LastMessage.CreatedAt}
	}
	return resp.LastMessage, pointer, nil
}
