package roomclient

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

// roomRPCTimeout bounds each room-service request/reply round trip.
const roomRPCTimeout = 5 * time.Second

// Client implements service.RoomClient via NATS request/reply RPCs to room-service and room-worker.
type Client struct {
	nc     *o11ynats.Conn
	siteID string
}

// New returns a Client wired to nc and scoped to siteID.
func New(nc *o11ynats.Conn, siteID string) *Client { return &Client{nc: nc, siteID: siteID} }

// GetRoomsInfo issues a batch room-info RPC; non-OK reply envelopes are relayed via errcode.Parse to preserve the remote classification.
func (c *Client) GetRoomsInfo(ctx context.Context, siteID string, roomIDs []string) ([]model.RoomInfo, error) {
	req, err := json.Marshal(model.RoomsInfoBatchRequest{RoomIDs: roomIDs})
	if err != nil {
		return nil, fmt.Errorf("marshal rooms-info request: %w", err)
	}
	msg, err := c.nc.Request(ctx, subject.RoomsInfoBatch(siteID), req, roomRPCTimeout)
	if err != nil {
		return nil, fmt.Errorf("rooms-info rpc: %w", err)
	}
	// Relay any remote error envelope as-is — including one carrying a code outside
	// our closed set — so the original classification/message is never masked.
	if e, ok := errcode.Parse(msg.Data); ok {
		return nil, e
	}
	var out model.RoomsInfoBatchResponse
	if err := json.Unmarshal(msg.Data, &out); err != nil {
		return nil, fmt.Errorf("decode rooms-info response: %w", err)
	}
	return out.Rooms, nil
}

// GetThreadRoomInfoBatch issues a batch thread-room-info RPC to room-service on
// the given site; non-OK reply envelopes are relayed via errcode.Parse.
func (c *Client) GetThreadRoomInfoBatch(ctx context.Context, siteID string, threadRoomIDs []string) ([]model.ThreadRoomInfo, error) {
	req, err := json.Marshal(model.ThreadRoomInfoBatchRequest{ThreadRoomIDs: threadRoomIDs})
	if err != nil {
		return nil, fmt.Errorf("marshal thread-room-info request: %w", err)
	}
	msg, err := c.nc.Request(ctx, subject.ThreadRoomInfoBatch(siteID), req, roomRPCTimeout)
	if err != nil {
		return nil, fmt.Errorf("thread-room-info rpc: %w", err)
	}
	if e, ok := errcode.Parse(msg.Data); ok {
		return nil, e
	}
	var out model.ThreadRoomInfoBatchResponse
	if err := json.Unmarshal(msg.Data, &out); err != nil {
		return nil, fmt.Errorf("decode thread-room-info response: %w", err)
	}
	return out.Threads, nil
}

// ClearAllThreadUnread issues the bulk clear-all-thread-unread RPC to room-service
// on the given site; non-OK reply envelopes are relayed via errcode.Parse. The
// reply carries no payload — success is a nil error.
func (c *Client) ClearAllThreadUnread(ctx context.Context, siteID, account string) error {
	req, err := json.Marshal(model.RoomThreadReadAllRequest{Account: account})
	if err != nil {
		return fmt.Errorf("marshal clear-all-thread-unread request: %w", err)
	}
	msg, err := c.nc.Request(ctx, subject.RoomThreadReadAll(siteID), req, roomRPCTimeout)
	if err != nil {
		return fmt.Errorf("clear-all-thread-unread rpc: %w", err)
	}
	if e, ok := errcode.Parse(msg.Data); ok {
		return e
	}
	return nil
}

// CreateDMRoom issues a DM-room creation RPC to room-worker; non-OK reply envelopes are relayed via errcode.Parse.
func (c *Client) CreateDMRoom(ctx context.Context, account, otherAccount string, roomType model.RoomType) (model.Subscription, error) {
	body, err := json.Marshal(model.SyncCreateDMRequest{
		RoomType:         roomType,
		RequesterAccount: account,
		OtherAccount:     otherAccount,
	})
	if err != nil {
		return model.Subscription{}, fmt.Errorf("marshal create-dm request: %w", err)
	}
	msg, err := c.nc.Request(ctx, subject.RoomCreateDMSync(c.siteID), body, roomRPCTimeout)
	if err != nil {
		return model.Subscription{}, fmt.Errorf("create-dm rpc: %w", err)
	}
	// Relay any remote error envelope as-is — including one carrying a code outside
	// our closed set — so the original classification/message is never masked.
	if e, ok := errcode.Parse(msg.Data); ok {
		return model.Subscription{}, e
	}
	var reply model.SyncCreateDMReply
	if err := json.Unmarshal(msg.Data, &reply); err != nil {
		return model.Subscription{}, fmt.Errorf("decode create-dm reply: %w", err)
	}
	// Success=false without an errcode envelope is unexpected; guard rather than return a zero Subscription.
	if !reply.Success {
		return model.Subscription{}, errcode.Internal("create-dm reported failure")
	}
	return reply.Subscription, nil
}
