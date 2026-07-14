package main

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/user-service/models"
)

type captured struct {
	mu   sync.Mutex
	pubs []capturedPub
	reqs []capturedReq
}
type capturedPub struct {
	Subj string
	Data []byte
}
type capturedReq struct {
	Subj string
	Data []byte
}

func (c *captured) publish(_ context.Context, subj string, data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pubs = append(c.pubs, capturedPub{Subj: subj, Data: append([]byte(nil), data...)})
	return nil
}
func (c *captured) request(_ context.Context, subj string, data []byte, _ time.Duration) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reqs = append(c.reqs, capturedReq{Subj: subj, Data: append([]byte(nil), data...)})
	return []byte(`{"ok":true}`), nil
}

func TestSendMessage_PublishesToFrontdoor(t *testing.T) {
	c := &captured{}
	u := &userState{ID: "u-1", Account: "user-1", Rooms: []string{"room-a", "room-b"}}
	ctx := actionCtx{Ctx: context.Background(), Publish: c.publish, Request: c.request, SiteID: "site-test"}
	err := sendMessage(ctx, u, "hello")
	require.NoError(t, err)
	require.Len(t, c.pubs, 1)
	got := c.pubs[0]
	require.True(t, got.Subj == subject.MsgSend("user-1", "room-a", "site-test") ||
		got.Subj == subject.MsgSend("user-1", "room-b", "site-test"))
	var req model.SendMessageRequest
	require.NoError(t, json.Unmarshal(got.Data, &req))
	require.Equal(t, "hello", req.Content)
}

func TestMarkRead_Requests(t *testing.T) {
	c := &captured{}
	u := &userState{ID: "u-1", Account: "user-1", Rooms: []string{"room-a"}}
	ctx := actionCtx{Ctx: context.Background(), Publish: c.publish, Request: c.request, SiteID: "site-test"}
	err := markRead(ctx, u, "msg-1")
	require.NoError(t, err)
	require.Len(t, c.reqs, 1)
	require.Len(t, c.pubs, 0)
	require.Equal(t, subject.MessageRead("user-1", "room-a", "site-test"), c.reqs[0].Subj)
}

func TestRefreshRoomList_Requests(t *testing.T) {
	c := &captured{}
	u := &userState{ID: "u-1", Account: "user-1"}
	ctx := actionCtx{Ctx: context.Background(), Publish: c.publish, Request: c.request, SiteID: "site-test"}
	require.NoError(t, refreshRoomList(ctx, u))
	require.Len(t, c.reqs, 1)
	require.Equal(t, subject.UserSubscriptionList("user-1", "site-test"), c.reqs[0].Subj)
	var got models.SubscriptionListRequest
	require.NoError(t, json.Unmarshal(c.reqs[0].Data, &got))
	require.Equal(t, models.SubscriptionListRequest{Type: "rooms"}, got)
}

func TestScrollHistory_Requests(t *testing.T) {
	c := &captured{}
	u := &userState{ID: "u-1", Account: "user-1", Rooms: []string{"room-a"}}
	ctx := actionCtx{Ctx: context.Background(), Publish: c.publish, Request: c.request, SiteID: "site-test"}
	require.NoError(t, scrollHistory(ctx, u))
	require.Len(t, c.reqs, 1)
	require.Contains(t, c.reqs[0].Subj, "room-a")
}

func TestMuteToggle_Publishes(t *testing.T) {
	c := &captured{}
	u := &userState{ID: "u-1", Account: "user-1", Rooms: []string{"room-a"}}
	ctx := actionCtx{Ctx: context.Background(), Publish: c.publish, Request: c.request, SiteID: "site-test"}
	require.NoError(t, muteToggle(ctx, u))
	require.Len(t, c.reqs, 1)
	require.Equal(t, subject.MuteToggle("user-1", "room-a", "site-test"), c.reqs[0].Subj)
}

func TestRoomCreate_Requests(t *testing.T) {
	c := &captured{}
	u := &userState{ID: "u-1", Account: "user-1", Neighbor: "user-0"}
	ctx := actionCtx{Ctx: context.Background(), Publish: c.publish, Request: c.request, SiteID: "site-test"}
	require.NoError(t, roomCreate(ctx, u))
	require.Len(t, c.reqs, 1)
	require.Equal(t, subject.RoomCreate("user-1", "site-test"), c.reqs[0].Subj)
	// Payload must include a `users` list — room-service rejects channel-create with no invitees.
	var payload struct {
		Name  string   `json:"name"`
		Users []string `json:"users"`
	}
	require.NoError(t, json.Unmarshal(c.reqs[0].Data, &payload))
	require.NotEmpty(t, payload.Name)
	require.Equal(t, []string{"user-0"}, payload.Users)
}

func TestMemberAdd_Requests(t *testing.T) {
	c := &captured{}
	u := &userState{ID: "u-1", Account: "user-1",
		Rooms:        []string{"room-a"},
		ChannelRooms: []string{"room-a"}}
	ctx := actionCtx{Ctx: context.Background(), Publish: c.publish, Request: c.request, SiteID: "site-test"}
	require.NoError(t, memberAdd(ctx, u, "user-2"))
	require.Len(t, c.reqs, 1)
	require.Equal(t, subject.MemberAdd("user-1", "room-a", "site-test"), c.reqs[0].Subj)
}

func TestMemberAdd_SkipsWhenNoChannelRooms(t *testing.T) {
	c := &captured{}
	u := &userState{ID: "u-1", Account: "user-1",
		Rooms:        []string{"room-dm-000001"},
		ChannelRooms: nil}
	ctx := actionCtx{Ctx: context.Background(), Publish: c.publish, Request: c.request, SiteID: "site-test"}
	require.NoError(t, memberAdd(ctx, u, "user-2"))
	require.Len(t, c.reqs, 0)
}

func TestThreadReply_Publishes(t *testing.T) {
	c := &captured{}
	u := &userState{ID: "u-1", Account: "user-1", Rooms: []string{"room-a"}}
	ctx := actionCtx{Ctx: context.Background(), Publish: c.publish, Request: c.request, SiteID: "site-test"}
	require.NoError(t, threadReply(ctx, u, "parent-msg-1", "reply text"))
	require.Len(t, c.pubs, 1)
	require.Equal(t, subject.MsgSend("user-1", "room-a", "site-test"), c.pubs[0].Subj)
	var req model.SendMessageRequest
	require.NoError(t, json.Unmarshal(c.pubs[0].Data, &req))
	require.Equal(t, "parent-msg-1", req.ThreadParentMessageID)
}
