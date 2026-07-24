package searchindex_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/searchindex"
)

func TestStoredScriptBody(t *testing.T) {
	body := searchindex.StoredScriptBody("ctx.op = 'none';")

	var decoded struct {
		Script struct {
			Lang   string `json:"lang"`
			Source string `json:"source"`
		} `json:"script"`
	}
	require.NoError(t, json.Unmarshal(body, &decoded))
	assert.Equal(t, "painless", decoded.Script.Lang)
	assert.Equal(t, "ctx.op = 'none';", decoded.Script.Source)
}

func TestBuildAddRoomUpdateBody(t *testing.T) {
	body := searchindex.BuildAddRoomUpdateBody("alice", "room1", 1000, 0)

	var decoded struct {
		Script struct {
			ID     string         `json:"id"`
			Params map[string]any `json:"params"`
		} `json:"script"`
		Upsert searchindex.UserRoomUpsertDoc `json:"upsert"`
	}
	require.NoError(t, json.Unmarshal(body, &decoded))
	assert.Equal(t, searchindex.AddRoomScriptID, decoded.Script.ID)
	assert.Equal(t, "room1", decoded.Script.Params["rid"])
	assert.InDelta(t, 1000, decoded.Script.Params["ts"], 0)
	assert.InDelta(t, 0, decoded.Script.Params["hss"], 0)
}

func TestBuildAddRoomUpdateBody_RestrictedRoomSeedsRestrictedRoomsMap(t *testing.T) {
	body := searchindex.BuildAddRoomUpdateBody("alice", "room1", 1000, 500)

	var decoded struct {
		Upsert searchindex.UserRoomUpsertDoc `json:"upsert"`
	}
	require.NoError(t, json.Unmarshal(body, &decoded))
	assert.Empty(t, decoded.Upsert.Rooms)
	assert.Equal(t, int64(500), decoded.Upsert.RestrictedRooms["room1"])
}

func TestBuildAddRoomUpdateBody_UnrestrictedRoomSeedsRoomsArray(t *testing.T) {
	body := searchindex.BuildAddRoomUpdateBody("alice", "room1", 1000, 0)

	var decoded struct {
		Upsert searchindex.UserRoomUpsertDoc `json:"upsert"`
	}
	require.NoError(t, json.Unmarshal(body, &decoded))
	assert.Equal(t, []string{"room1"}, decoded.Upsert.Rooms)
	assert.Empty(t, decoded.Upsert.RestrictedRooms)
}

func TestBuildRemoveRoomUpdateBody_NoUpsertSeed(t *testing.T) {
	body := searchindex.BuildRemoveRoomUpdateBody("room1", 2000)

	var decoded map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(body, &decoded))
	_, hasUpsert := decoded["upsert"]
	assert.False(t, hasUpsert, "remove path must not carry an upsert seed")

	var script struct {
		ID     string         `json:"id"`
		Params map[string]any `json:"params"`
	}
	require.NoError(t, json.Unmarshal(decoded["script"], &script))
	assert.Equal(t, searchindex.RemoveRoomScriptID, script.ID)
	assert.Equal(t, "room1", script.Params["rid"])
	assert.InDelta(t, 2000, script.Params["ts"], 0)
}
