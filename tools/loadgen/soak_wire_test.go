package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
)

func TestSoakWireRequests_MatchHistoryServiceJSON(t *testing.T) {
	before := int64(1710000000000)
	created := int64(1700000000000)
	meta := &soakRoomMeta{LastMsgAt: &before, CreatedAt: &created}

	tests := []struct {
		name string
		body any
		want string
	}{
		{
			name: "load history",
			body: soakLoadHistoryRequest{Before: &before, Limit: 50, Meta: meta},
			want: `{"before":1710000000000,"limit":50,"meta":{"lastMsgAt":1710000000000,"createdAt":1700000000000}}`,
		},
		{
			name: "load next",
			body: soakLoadNextMessagesRequest{After: &created, Limit: 50, Cursor: "next", Meta: meta},
			want: `{"after":1700000000000,"limit":50,"cursor":"next","meta":{"lastMsgAt":1710000000000,"createdAt":1700000000000}}`,
		},
		{
			name: "get message",
			body: soakGetMessageByIDRequest{MessageID: "m1"},
			want: `{"messageId":"m1"}`,
		},
		{
			name: "edit",
			body: soakEditMessageRequest{MessageID: "m1", NewMsg: "updated"},
			want: `{"messageId":"m1","newMsg":"updated"}`,
		},
		{
			name: "delete",
			body: soakDeleteMessageRequest{MessageID: "m1"},
			want: `{"messageId":"m1"}`,
		},
		{
			name: "pin",
			body: soakPinMessageRequest{MessageID: "m1"},
			want: `{"messageId":"m1"}`,
		},
		{
			name: "unpin",
			body: soakUnpinMessageRequest{MessageID: "m1"},
			want: `{"messageId":"m1"}`,
		},
		{
			name: "pinned list",
			body: soakListPinnedMessagesRequest{Cursor: "page-2", Limit: 25},
			want: `{"cursor":"page-2","limit":25}`,
		},
		{
			name: "reaction",
			body: soakReactMessageRequest{MessageID: "m1", Shortcode: ":wave:"},
			want: `{"messageId":"m1","shortcode":":wave:"}`,
		},
		{
			name: "thread",
			body: soakGetThreadMessagesRequest{ThreadMessageID: "parent", Cursor: "cursor", Limit: 50},
			want: `{"threadMessageId":"parent","cursor":"cursor","limit":50}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := json.Marshal(tt.body)
			require.NoError(t, err)
			assert.JSONEq(t, tt.want, string(got))
		})
	}
}

func TestSoakWireResponses_DecodeHistoryServiceJSON(t *testing.T) {
	createdAt := time.Date(2026, 7, 24, 10, 0, 0, 0, time.UTC)
	messageJSON, err := json.Marshal(cassandra.Message{
		RoomID:    "room-1",
		CreatedAt: createdAt,
		MessageID: "message-1",
		Sender:    cassandra.Participant{ID: "user-1", Account: "a@example.com"},
		Msg:       "hello",
	})
	require.NoError(t, err)

	tests := []struct {
		name    string
		payload string
		target  any
		assert  func(*testing.T, any)
	}{
		{
			name:    "get message is a direct message object",
			payload: string(messageJSON),
			target:  &cassandra.Message{},
			assert: func(t *testing.T, target any) {
				got := target.(*cassandra.Message)
				assert.Equal(t, "message-1", got.MessageID)
			},
		},
		{
			name:    "load history",
			payload: `{"messages":[` + string(messageJSON) + `],"minUserLastSeenAt":1700000000000}`,
			target:  &soakLoadHistoryResponse{},
			assert: func(t *testing.T, target any) {
				got := target.(*soakLoadHistoryResponse)
				require.Len(t, got.Messages, 1)
				assert.Equal(t, int64(1700000000000), *got.MinUserLastSeenAt)
			},
		},
		{
			name:    "load next",
			payload: `{"messages":[],"nextCursor":"next","hasNext":true}`,
			target:  &soakLoadNextMessagesResponse{},
			assert: func(t *testing.T, target any) {
				got := target.(*soakLoadNextMessagesResponse)
				assert.True(t, got.HasNext)
				assert.Equal(t, "next", got.NextCursor)
			},
		},
		{
			name:    "edit",
			payload: `{"messageId":"m1","editedAt":1710000000000}`,
			target:  &soakEditMessageResponse{},
			assert: func(t *testing.T, target any) {
				assert.Equal(t, int64(1710000000000), target.(*soakEditMessageResponse).EditedAt)
			},
		},
		{
			name:    "delete",
			payload: `{"messageId":"m1","deletedAt":1710000000001}`,
			target:  &soakDeleteMessageResponse{},
			assert: func(t *testing.T, target any) {
				assert.Equal(t, int64(1710000000001), target.(*soakDeleteMessageResponse).DeletedAt)
			},
		},
		{
			name:    "pin",
			payload: `{"messageId":"m1","pinnedAt":1710000000002}`,
			target:  &soakPinMessageResponse{},
			assert: func(t *testing.T, target any) {
				assert.Equal(t, int64(1710000000002), target.(*soakPinMessageResponse).PinnedAt)
			},
		},
		{
			name:    "unpin",
			payload: `{"messageId":"m1"}`,
			target:  &soakUnpinMessageResponse{},
			assert: func(t *testing.T, target any) {
				assert.Equal(t, "m1", target.(*soakUnpinMessageResponse).MessageID)
			},
		},
		{
			name:    "pinned list",
			payload: `{"messages":[],"nextCursor":"p2","hasNext":true}`,
			target:  &soakListPinnedMessagesResponse{},
			assert: func(t *testing.T, target any) {
				assert.True(t, target.(*soakListPinnedMessagesResponse).HasNext)
			},
		},
		{
			name:    "reaction",
			payload: `{"messageId":"m1","shortcode":":wave:","action":"added","reactedAt":1710000000003}`,
			target:  &soakReactMessageResponse{},
			assert: func(t *testing.T, target any) {
				assert.Equal(t, model.ReactionActionAdded, target.(*soakReactMessageResponse).Action)
			},
		},
		{
			name:    "thread",
			payload: `{"messages":[],"nextCursor":"t2","hasNext":true,"parentMessage":` + string(messageJSON) + `}`,
			target:  &soakGetThreadMessagesResponse{},
			assert: func(t *testing.T, target any) {
				got := target.(*soakGetThreadMessagesResponse)
				require.NotNil(t, got.ParentMessage)
				assert.Equal(t, "message-1", got.ParentMessage.MessageID)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.NoError(t, json.Unmarshal([]byte(tt.payload), tt.target))
			tt.assert(t, tt.target)
		})
	}
}

func TestSoakWireRequests_OmitOptionalFieldsLikeHistoryService(t *testing.T) {
	body, err := json.Marshal(soakLoadHistoryRequest{Limit: 50})
	require.NoError(t, err)
	assert.JSONEq(t, `{"limit":50}`, string(body))

	body, err = json.Marshal(soakListPinnedMessagesRequest{Limit: 25})
	require.NoError(t, err)
	assert.JSONEq(t, `{"limit":25}`, string(body))
}
