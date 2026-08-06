package cassandra

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMessageRoom_JSON(t *testing.T) {
	sub := true
	r := MessageRoom{
		ID:   "room-1",
		Name: "prj-alpha",
		Type: RoomType("channel"),
		HRInfo: &MessageHRInfo{
			Account: "alice", ChineseName: "爱丽丝", EngName: "Alice",
		},
		AppInfo: &MessageAppInfo{
			ID: "app-1", Name: "MyApp", AssistantName: "bot-my-app", IsSubscribed: &sub,
		},
	}
	roundTrip(t, r)
}

func TestMessageRoom_JSON_IDTypeOnly(t *testing.T) {
	got := roundTrip(t, MessageRoom{ID: "dm-1", Type: RoomType("dm")})
	assert.Empty(t, got.Name)
	assert.Nil(t, got.HRInfo)
	assert.Nil(t, got.AppInfo)

	// dm/botDM shape omits name/hrInfo/appInfo keys entirely on the wire.
	b, err := json.Marshal(MessageRoom{ID: "dm-1", Type: RoomType("dm")})
	require.NoError(t, err)
	assert.JSONEq(t, `{"id":"dm-1","type":"dm"}`, string(b))
}

func TestForwardedMessage_JSON_WithRoom(t *testing.T) {
	fm := ForwardedMessage{
		MessageID: "m-src",
		RoomID:    "room-src",
		Sender:    Participant{ID: "u1", Account: "alice"},
		Msg:       "hello",
		Room:      &MessageRoom{ID: "room-src", Name: "prj-alpha", Type: RoomType("channel")},
	}
	got := roundTrip(t, fm)
	require.NotNil(t, got.Room)
	assert.Equal(t, "prj-alpha", got.Room.Name)
}

func TestForwardedMessage_JSON_RoomOmittedWhenNil(t *testing.T) {
	b, err := json.Marshal(ForwardedMessage{MessageID: "m-src", RoomID: "room-src"})
	require.NoError(t, err)
	assert.NotContains(t, string(b), `"room"`)
}
