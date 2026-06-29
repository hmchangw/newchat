package models

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRoomsGetRequest_JSON(t *testing.T) {
	req := RoomsGetRequest{RoomIDs: []string{"r1", "r2"}}
	data, err := json.Marshal(req)
	require.NoError(t, err)
	assert.JSONEq(t, `{"roomIds":["r1","r2"]}`, string(data))

	var decoded RoomsGetRequest
	require.NoError(t, json.Unmarshal(data, &decoded))
	assert.Equal(t, req, decoded)
}

func TestRoomsGetResponse_JSONRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		in   RoomsGetResponse
	}{
		{name: "empty", in: RoomsGetResponse{Rooms: map[string]LastMessage{}}},
		{name: "one room", in: RoomsGetResponse{Rooms: map[string]LastMessage{
			"r1": {MessageID: "m1", Sender: Participant{ID: "u1", Account: "alice"}, Content: "hi", CreatedAt: 1_714_000_000_000},
		}}},
		{name: "deleted last", in: RoomsGetResponse{Rooms: map[string]LastMessage{
			"r2": {MessageID: "m9", Sender: Participant{ID: "u2", Account: "bob"}, Content: "", CreatedAt: 1_714_000_000_999, Deleted: true},
		}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := json.Marshal(tc.in)
			require.NoError(t, err)
			var got RoomsGetResponse
			require.NoError(t, json.Unmarshal(data, &got))
			assert.Equal(t, tc.in, got)
		})
	}
}

// deleted=false must be omitted from the wire (omitempty) so the common,
// not-deleted case stays compact.
func TestLastMessage_OmitsDeletedWhenFalse(t *testing.T) {
	data, err := json.Marshal(LastMessage{MessageID: "m1", Content: "hi", CreatedAt: 1})
	require.NoError(t, err)
	assert.NotContains(t, string(data), "deleted")
}
