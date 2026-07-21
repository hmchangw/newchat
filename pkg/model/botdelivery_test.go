package model_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

func TestBotEventTypeConstants(t *testing.T) {
	assert.Equal(t, "room_join", model.BotEventRoomJoin)
	assert.Equal(t, "room_leave", model.BotEventRoomLeave)
}

func TestNewBotRoomNatsEvent_Join_WireShape(t *testing.T) {
	evt, err := model.NewBotRoomNatsEvent(&model.BotRoomEventParams{
		EventType:     model.BotEventRoomJoin,
		EventID:       "evt-1",
		Timestamp:     "2026-07-17T12:00:00Z",
		SiteID:        "site-a",
		Subscriptions: []string{"weather.bot"},
		Room:          model.BotRoomRef{ID: "r1", Name: "engineering", Type: model.RoomTypeChannel},
		User:          model.BotRoomUser{ID: "u1", Username: "weather.bot", Name: "天氣", EngName: "Weather Bot"},
	})
	require.NoError(t, err)

	data, err := json.Marshal(evt)
	require.NoError(t, err)

	var m map[string]any
	require.NoError(t, json.Unmarshal(data, &m))

	// Envelope: exact platform-facing json keys.
	assert.Equal(t, "evt-1", m["eventId"])
	assert.Equal(t, "2026-07-17T12:00:00Z", m["ts"])
	assert.Equal(t, "site-a", m["origin"])
	assert.Equal(t, "r1", m["publishId"], "publishId is the room id")
	assert.Equal(t, []any{"weather.bot"}, m["subscriptions"], "the target bot goes in subscriptions, not data")
	assert.Equal(t, "room_join", m["event"], "BotEvent.Type flattens to the top-level event key")

	// data = {room, user}, forwarded to the bot verbatim.
	d, ok := m["data"].(map[string]any)
	require.True(t, ok)
	room, ok := d["room"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "r1", room["_id"], "room id key is _id, not rid")
	assert.Equal(t, "engineering", room["name"])
	assert.Equal(t, "channel", room["type"])

	usr, ok := d["user"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "u1", usr["_id"])
	assert.Equal(t, "weather.bot", usr["username"])
	assert.Equal(t, "天氣", usr["name"], "name is the chinese name")
	assert.Equal(t, "Weather Bot", usr["engName"])
}

func TestNewBotRoomNatsEvent_Leave_OmitsRoomName(t *testing.T) {
	evt, err := model.NewBotRoomNatsEvent(&model.BotRoomEventParams{
		EventType:     model.BotEventRoomLeave,
		EventID:       "evt-2",
		Timestamp:     "2026-07-17T12:00:01Z",
		SiteID:        "site-a",
		Subscriptions: []string{"weather.bot"},
		Room:          model.BotRoomRef{ID: "r1", Type: model.RoomTypeChannel}, // name intentionally empty
		User:          model.BotRoomUser{ID: "u1", Username: "weather.bot", Name: "天氣", EngName: "Weather Bot"},
	})
	require.NoError(t, err)
	assert.Equal(t, "room_leave", evt.Type)

	data, err := json.Marshal(evt)
	require.NoError(t, err)
	var m map[string]any
	require.NoError(t, json.Unmarshal(data, &m))

	room := m["data"].(map[string]any)["room"].(map[string]any)
	_, hasName := room["name"]
	assert.False(t, hasName, "room.name is omitempty and absent on leave")
	assert.Equal(t, "r1", room["_id"])

	// user stays present on leave.
	usr := m["data"].(map[string]any)["user"].(map[string]any)
	assert.Equal(t, "weather.bot", usr["username"])
}

func TestBotRoomUser_OmitsEmptyID(t *testing.T) {
	// The org-sweep leave path has no user _id; it must be omitted, not "".
	data, err := json.Marshal(model.BotRoomUser{Username: "weather.bot", Name: "天氣", EngName: "Weather Bot"})
	require.NoError(t, err)
	var m map[string]any
	require.NoError(t, json.Unmarshal(data, &m))
	_, hasID := m["_id"]
	assert.False(t, hasID, "_id is omitempty when absent (org-sweep leave path)")
	assert.Equal(t, "weather.bot", m["username"])
}
