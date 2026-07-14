package model_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
)

func i64(v int64) *int64 { return &v }

func TestThreadListItemJSON(t *testing.T) {
	src := model.ThreadListItem{
		SiteID:          "site-a",
		RoomID:          "room-1",
		RoomName:        "general",
		RoomType:        model.RoomTypeChannel,
		ThreadRoomID:    "thr-1",
		ParentMessageID: "msg-parent",
		LastSeenAt:      i64(1746518100000),
		HasMention:      true,
		Unread:          true,
		LastMsgAt:       1746518400000,
	}
	roundTrip(t, &src, &model.ThreadListItem{})
}

// A DM thread row carries the counterpart's HR record, which survives a round trip.
func TestThreadListItemJSON_WithHRInfo(t *testing.T) {
	src := model.ThreadListItem{
		SiteID: "site-a", RoomID: "dm1", RoomName: "bob", RoomType: model.RoomTypeDM,
		ThreadRoomID: "thr-1", LastMsgAt: 1746518400000,
		HRInfo: &model.SubscriptionHRInfo{Account: "bob", Name: "鮑勃", EngName: "Bob Chen"},
	}
	roundTrip(t, &src, &model.ThreadListItem{})
}

// hrInfo is omitted for non-DM rows (no counterpart record).
func TestThreadListItemJSON_OmitsNilHRInfo(t *testing.T) {
	src := model.ThreadListItem{SiteID: "site-a", RoomID: "room-1", RoomType: model.RoomTypeChannel, ThreadRoomID: "thr-1"}
	data, err := json.Marshal(&src)
	require.NoError(t, err)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))
	_, hasHR := raw["hrInfo"]
	assert.False(t, hasHR, "nil hrInfo must be omitted")
}

// LastSeenAt is omitted when nil (never-seen thread).
func TestThreadListItemJSON_OmitsNilLastSeenAt(t *testing.T) {
	src := model.ThreadListItem{SiteID: "site-a", RoomID: "room-1", ThreadRoomID: "thr-1"}
	data, err := json.Marshal(&src)
	require.NoError(t, err)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))
	_, hasSeen := raw["lastSeenAt"]
	assert.False(t, hasSeen, "nil lastSeenAt must be omitted")
	_, hasParent := raw["parentMessage"]
	assert.False(t, hasParent, "nil parentMessage must be omitted")
}

// The hydrated parent/last message bodies survive a JSON round trip.
func TestThreadListItemJSON_WithMessages(t *testing.T) {
	parent := &cassandra.Message{MessageID: "msg-parent", RoomID: "room-1", Msg: "anyone?"}
	last := &cassandra.Message{MessageID: "msg-last", RoomID: "room-1", Msg: "on it"}
	src := model.ThreadListItem{
		SiteID: "site-a", RoomID: "room-1", ThreadRoomID: "thr-1",
		ParentMessageID: "msg-parent", LastMsgAt: 1746518400000,
		ParentMessage: parent, LastMessage: last,
	}
	data, err := json.Marshal(&src)
	require.NoError(t, err)
	var dst model.ThreadListItem
	require.NoError(t, json.Unmarshal(data, &dst))
	require.NotNil(t, dst.ParentMessage)
	require.NotNil(t, dst.LastMessage)
	assert.Equal(t, "msg-parent", dst.ParentMessage.MessageID)
	assert.Equal(t, "on it", dst.LastMessage.Msg)
}

func TestThreadSubscriptionListRequestJSON(t *testing.T) {
	src := model.ThreadSubscriptionListRequest{
		Account:            "alice",
		CursorLastMsgAt:    i64(1746518400000),
		CursorThreadRoomID: "thr-9",
		Limit:              50,
	}
	roundTrip(t, &src, &model.ThreadSubscriptionListRequest{})
}

func TestThreadSubscriptionListResponseJSON(t *testing.T) {
	src := model.ThreadSubscriptionListResponse{
		Items:   []model.ThreadListItem{{SiteID: "site-a", ThreadRoomID: "thr-1", LastMsgAt: 1}},
		HasMore: true,
	}
	roundTrip(t, &src, &model.ThreadSubscriptionListResponse{})
}

func TestThreadListResponseJSON(t *testing.T) {
	src := model.ThreadListResponse{
		Items:            []model.ThreadListItem{{SiteID: "site-a", ThreadRoomID: "thr-1", LastMsgAt: 1}},
		NextCursor:       "eyJsYXN0TXNnQXQiOjF9",
		HasNext:          true,
		UnavailableSites: []string{"site-b"},
	}
	roundTrip(t, &src, &model.ThreadListResponse{})
}
