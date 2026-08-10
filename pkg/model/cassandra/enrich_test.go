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

// A forwarded snapshot carries the source room's type inline and never a
// nested room object — the name it would have held is mutable, so the snapshot
// deliberately does not reproduce it.
func TestForwardedMessage_JSON_RoomTypeInlineNoRoomObject(t *testing.T) {
	b, err := json.Marshal(ForwardedMessage{
		MessageID: "m-src", RoomID: "room-src", RoomType: RoomType("channel"),
	})
	require.NoError(t, err)
	assert.Contains(t, string(b), `"roomType":"channel"`)
	assert.NotContains(t, string(b), `"room":`)
	assert.NotContains(t, string(b), `"name"`)
}
