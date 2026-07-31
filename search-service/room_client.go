package main

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

// roomRPCTimeout bounds each RoomsInfoBatch round trip.
const roomRPCTimeout = 5 * time.Second

// roomClient is the NATS-backed RoomInfoClient. It issues the RoomsInfoBatch
// server↔server RPC to room-service on the target site.
type roomClient struct {
	nc *o11ynats.Conn
}

func newRoomClient(nc *o11ynats.Conn) *roomClient { return &roomClient{nc: nc} }

func (c *roomClient) GetRoomsInfo(ctx context.Context, siteID string, roomIDs []string) ([]model.RoomInfo, error) {
	req, err := json.Marshal(model.RoomsInfoBatchRequest{RoomIDs: roomIDs})
	if err != nil {
		return nil, fmt.Errorf("marshal rooms-info request: %w", err)
	}
	msg, err := c.nc.Request(ctx, subject.RoomsInfoBatch(siteID), req, roomRPCTimeout)
	if err != nil {
		return nil, fmt.Errorf("rooms-info rpc: %w", err)
	}
	if e, ok := errcode.Parse(msg.Data); ok {
		return nil, e
	}
	var out model.RoomsInfoBatchResponse
	if err := json.Unmarshal(msg.Data, &out); err != nil {
		return nil, fmt.Errorf("decode rooms-info response: %w", err)
	}
	return out.Rooms, nil
}
