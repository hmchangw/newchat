package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubMembersRequester replays canned getChannels response pages in order and
// records each request's decoded body.
type stubMembersRequester struct {
	pages [][]byte
	calls []getChannelsRequest
	i     int
}

func (s *stubMembersRequester) Request(_ context.Context, _ string, data []byte, _ time.Duration) (*nats.Msg, error) {
	var req getChannelsRequest
	_ = json.Unmarshal(data, &req)
	s.calls = append(s.calls, req)
	page := s.pages[s.i]
	if s.i < len(s.pages)-1 {
		s.i++
	}
	return &nats.Msg{Data: page}, nil
}

func membersPage(hasMore bool, roomIDs ...string) []byte {
	var out getChannelsResponse
	for _, id := range roomIDs {
		out.Subscriptions = append(out.Subscriptions, struct {
			RoomID string `json:"roomId"`
		}{RoomID: id})
	}
	out.HasMore = hasMore
	b, _ := json.Marshal(out)
	return b
}

func TestMembersClient_GetChannels_PagesUntilExhausted(t *testing.T) {
	stub := &stubMembersRequester{pages: [][]byte{
		membersPage(true, "r1", "r2"),
		membersPage(false, "r3"),
	}}
	c := &membersClient{req: stub}

	rooms, err := c.GetChannels(context.Background(), "alice", "site-a", []string{"bob"})
	require.NoError(t, err)
	assert.Equal(t, []string{"r1", "r2", "r3"}, rooms, "all pages accumulated, not just page 1")
	require.Len(t, stub.calls, 2, "paged twice")
	assert.Equal(t, 0, stub.calls[0].Offset)
	assert.Equal(t, 2, stub.calls[1].Offset, "second-page offset advances by the first page's count")
	assert.Equal(t, []string{"bob"}, stub.calls[0].AccountNames)
}

func TestMembersClient_GetChannels_SinglePageNoExtraRoundTrip(t *testing.T) {
	stub := &stubMembersRequester{pages: [][]byte{membersPage(false, "r1")}}
	c := &membersClient{req: stub}

	rooms, err := c.GetChannels(context.Background(), "alice", "site-a", []string{"bob"})
	require.NoError(t, err)
	assert.Equal(t, []string{"r1"}, rooms)
	require.Len(t, stub.calls, 1, "no extra round trip when hasMore is false")
}
