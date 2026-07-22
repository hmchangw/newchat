package models

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
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
		{name: "empty", in: RoomsGetResponse{Rooms: map[string]PreviewMessage{}}},
		{name: "one room", in: RoomsGetResponse{Rooms: map[string]PreviewMessage{
			"r1": {MessageID: "m1", Sender: model.Participant{UserID: "u1", Account: "alice"}, Content: "hi", CreatedAt: 1_714_000_000_000},
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
