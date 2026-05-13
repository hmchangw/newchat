package models

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEditMessageRequest_JSON(t *testing.T) {
	req := EditMessageRequest{
		MessageID: "m-abc",
		NewMsg:    "corrected text",
	}
	data, err := json.Marshal(req)
	require.NoError(t, err)
	assert.JSONEq(t, `{"messageId":"m-abc","newMsg":"corrected text"}`, string(data))

	var decoded EditMessageRequest
	require.NoError(t, json.Unmarshal(data, &decoded))
	assert.Equal(t, req, decoded)
}

func TestEditMessageResponse_JSON(t *testing.T) {
	resp := EditMessageResponse{
		MessageID: "m-abc",
		EditedAt:  1_714_000_000_000,
	}
	data, err := json.Marshal(resp)
	require.NoError(t, err)
	assert.JSONEq(t, `{"messageId":"m-abc","editedAt":1714000000000}`, string(data))

	var decoded EditMessageResponse
	require.NoError(t, json.Unmarshal(data, &decoded))
	assert.Equal(t, resp, decoded)
}

func TestMessageEditedEvent_JSON(t *testing.T) {
	evt := MessageEditedEvent{
		Type:      "message_edited",
		Timestamp: 1_714_000_000_000,
		RoomID:    "r1",
		MessageID: "m-abc",
		NewMsg:    "corrected text",
		EditedBy:  "alice",
		EditedAt:  1_714_000_000_000,
	}
	data, err := json.Marshal(evt)
	require.NoError(t, err)
	assert.JSONEq(t, `{
		"type":"message_edited",
		"timestamp":1714000000000,
		"roomId":"r1",
		"messageId":"m-abc",
		"newMsg":"corrected text",
		"editedBy":"alice",
		"editedAt":1714000000000
	}`, string(data))

	var decoded MessageEditedEvent
	require.NoError(t, json.Unmarshal(data, &decoded))
	assert.Equal(t, evt, decoded)
}

func TestDeleteMessageRequest_JSON(t *testing.T) {
	req := DeleteMessageRequest{MessageID: "m-abc"}
	data, err := json.Marshal(req)
	require.NoError(t, err)
	assert.JSONEq(t, `{"messageId":"m-abc"}`, string(data))

	var decoded DeleteMessageRequest
	require.NoError(t, json.Unmarshal(data, &decoded))
	assert.Equal(t, req, decoded)
}

func TestDeleteMessageResponse_JSON(t *testing.T) {
	resp := DeleteMessageResponse{
		MessageID: "m-abc",
		DeletedAt: 1_714_000_000_000,
	}
	data, err := json.Marshal(resp)
	require.NoError(t, err)
	assert.JSONEq(t, `{"messageId":"m-abc","deletedAt":1714000000000}`, string(data))

	var decoded DeleteMessageResponse
	require.NoError(t, json.Unmarshal(data, &decoded))
	assert.Equal(t, resp, decoded)
}

func TestRoomMeta_JSONRoundTrip(t *testing.T) {
	last := int64(1234567890123)
	created := int64(1234567000000)
	cases := []struct {
		name string
		in   RoomMeta
	}{
		{name: "both fields", in: RoomMeta{LastMsgAt: &last, CreatedAt: &created}},
		{name: "only LastMsgAt", in: RoomMeta{LastMsgAt: &last}},
		{name: "only CreatedAt", in: RoomMeta{CreatedAt: &created}},
		{name: "empty", in: RoomMeta{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := json.Marshal(tc.in)
			require.NoError(t, err)
			var got RoomMeta
			require.NoError(t, json.Unmarshal(data, &got))
			assert.Equal(t, tc.in, got)
		})
	}
}

func TestLoadHistoryRequest_WithMeta_Roundtrip(t *testing.T) {
	last := int64(1234567890123)
	cases := []struct {
		name string
		in   LoadHistoryRequest
	}{
		{name: "meta nil", in: LoadHistoryRequest{Limit: 50}},
		{name: "meta populated", in: LoadHistoryRequest{Limit: 50, Meta: &RoomMeta{LastMsgAt: &last}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := json.Marshal(tc.in)
			require.NoError(t, err)
			var got LoadHistoryRequest
			require.NoError(t, json.Unmarshal(data, &got))
			assert.Equal(t, tc.in, got)
		})
	}
}

func TestLoadNextMessagesRequest_WithMeta_Roundtrip(t *testing.T) {
	last := int64(1234567890123)
	created := int64(1234567000000)
	cases := []struct {
		name string
		in   LoadNextMessagesRequest
	}{
		{name: "meta nil", in: LoadNextMessagesRequest{Limit: 25, Cursor: "cur1"}},
		{name: "meta populated", in: LoadNextMessagesRequest{Limit: 25, Cursor: "cur1", Meta: &RoomMeta{LastMsgAt: &last, CreatedAt: &created}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := json.Marshal(tc.in)
			require.NoError(t, err)
			var got LoadNextMessagesRequest
			require.NoError(t, json.Unmarshal(data, &got))
			assert.Equal(t, tc.in, got)
		})
	}
}

func TestLoadSurroundingMessagesRequest_WithMeta_Roundtrip(t *testing.T) {
	created := int64(1234567000000)
	cases := []struct {
		name string
		in   LoadSurroundingMessagesRequest
	}{
		{name: "meta nil", in: LoadSurroundingMessagesRequest{MessageID: "m-abc", Limit: 20}},
		{name: "meta populated", in: LoadSurroundingMessagesRequest{MessageID: "m-abc", Limit: 20, Meta: &RoomMeta{CreatedAt: &created}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := json.Marshal(tc.in)
			require.NoError(t, err)
			var got LoadSurroundingMessagesRequest
			require.NoError(t, json.Unmarshal(data, &got))
			assert.Equal(t, tc.in, got)
		})
	}
}

func TestGetThreadMessagesRequest_WithMeta_Roundtrip(t *testing.T) {
	last := int64(1234567890123)
	created := int64(1234567000000)
	cases := []struct {
		name string
		in   GetThreadMessagesRequest
	}{
		{name: "meta nil", in: GetThreadMessagesRequest{ThreadMessageID: "t-abc", Limit: 10}},
		{name: "meta populated", in: GetThreadMessagesRequest{ThreadMessageID: "t-abc", Limit: 10, Meta: &RoomMeta{LastMsgAt: &last, CreatedAt: &created}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := json.Marshal(tc.in)
			require.NoError(t, err)
			var got GetThreadMessagesRequest
			require.NoError(t, json.Unmarshal(data, &got))
			assert.Equal(t, tc.in, got)
		})
	}
}

func TestMessageDeletedEvent_JSON(t *testing.T) {
	evt := MessageDeletedEvent{
		Type:      "message_deleted",
		Timestamp: 1_714_000_000_000,
		RoomID:    "r1",
		MessageID: "m-abc",
		DeletedBy: "alice",
		DeletedAt: 1_714_000_000_000,
	}
	data, err := json.Marshal(evt)
	require.NoError(t, err)
	assert.JSONEq(t, `{
		"type":"message_deleted",
		"timestamp":1714000000000,
		"roomId":"r1",
		"messageId":"m-abc",
		"deletedBy":"alice",
		"deletedAt":1714000000000
	}`, string(data))

	var decoded MessageDeletedEvent
	require.NoError(t, json.Unmarshal(data, &decoded))
	assert.Equal(t, evt, decoded)
}
