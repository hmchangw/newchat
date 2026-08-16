package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	usermodels "github.com/hmchangw/chat/user-service/models"
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
			target:  &soakWireMessage{},
			assert: func(t *testing.T, target any) {
				got := target.(*soakWireMessage)
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

func TestSoakWireResponses_DecodeMessagesWithWireReactions(t *testing.T) {
	payload := `{
		"roomId":"room-1",
		"createdAt":"2026-07-24T10:00:00Z",
		"messageId":"message-1",
		"sender":{"id":"user-1","account":"a@example.com"},
		"msg":"hello",
		"reactions":{"thumbsup":[{"account":"b@example.com","displayName":"Bob"}]}
	}`

	tests := []struct {
		name   string
		target any
	}{
		{name: "get message", target: &soakWireMessage{}},
		{
			name:   "load history",
			target: &soakLoadHistoryResponse{},
		},
		{
			name:   "thread messages",
			target: &soakGetThreadMessagesResponse{},
		},
		{
			name:   "pinned messages",
			target: &soakListPinnedMessagesResponse{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := payload
			if tt.name != "get message" {
				body = `{"messages":[` + payload + `]}`
			}
			require.NoError(t, json.Unmarshal([]byte(body), tt.target))
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

func TestSoakAddMembersRequest_MatchesModelContract(t *testing.T) {
	encoded, err := json.Marshal(soakAddMembersRequest{RoomID: "r1", Users: []string{"u1"}})
	require.NoError(t, err)

	var decoded model.AddMembersRequest
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	assert.Equal(t, "r1", decoded.RoomID)
	assert.Equal(t, []string{"u1"}, decoded.Users)
}

func TestSoakRemoveMemberRequest_MatchesModelContract(t *testing.T) {
	encoded, err := json.Marshal(soakRemoveMemberRequest{RoomID: "r1", Account: "u1"})
	require.NoError(t, err)

	var decoded model.RemoveMemberRequest
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	assert.Equal(t, "r1", decoded.RoomID)
	assert.Equal(t, "u1", decoded.Account)
}

func TestSoakRoomRenameRequest_MatchesModelContract(t *testing.T) {
	encoded, err := json.Marshal(soakRoomRenameRequest{NewName: "soak-room-r1"})
	require.NoError(t, err)

	var decoded model.RoomRenameRequest
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	assert.Equal(t, "soak-room-r1", decoded.NewName)
}

func TestSoakCreateRoomRequest_MatchesModelContract(t *testing.T) {
	encoded, err := json.Marshal(soakCreateRoomRequest{Name: "soak-room", Users: []string{"u1", "u2"}})
	require.NoError(t, err)

	var decoded model.CreateRoomRequest
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	assert.Equal(t, "soak-room", decoded.Name)
	assert.Equal(t, []string{"u1", "u2"}, decoded.Users)
}

func TestSoakCreateRoomReply_DecodesRoomServiceReply(t *testing.T) {
	encoded, err := json.Marshal(model.CreateRoomReply{
		Status: model.CreateRoomReplyAccepted, RoomID: "room-1",
		RoomType: string(model.RoomTypeChannel),
	})
	require.NoError(t, err)

	var decoded soakCreateRoomReply
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	assert.Equal(t, "accepted", decoded.Status)
	assert.Equal(t, "room-1", decoded.RoomID)
	assert.Equal(t, "channel", decoded.RoomType)
}

func TestSoakMuteToggleReply_DecodesRoomServiceReply(t *testing.T) {
	encoded, err := json.Marshal(model.MuteToggleResponse{Status: "ok", Muted: true})
	require.NoError(t, err)

	var decoded soakMuteToggleReply
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	assert.True(t, decoded.Muted)
}

func TestSoakListMembersResponse_DecodesRoomServiceReply(t *testing.T) {
	encoded, err := json.Marshal(model.ListRoomMembersResponse{
		Members: []model.RoomMember{{
			ID: "m1", RoomID: "r1",
			Member: model.RoomMemberEntry{ID: "u1", Type: model.RoomMemberIndividual, Account: "user-1"},
		}},
	})
	require.NoError(t, err)

	var decoded soakListMembersResponse
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	require.Len(t, decoded.Members, 1)
	assert.Equal(t, "r1", decoded.Members[0].RoomID)
	assert.Equal(t, "user-1", decoded.Members[0].Member.Account)
}

func TestSoakRoomsInfo_MatchesModelContract(t *testing.T) {
	encoded, err := json.Marshal(soakRoomsInfoRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	var decodedRequest model.RoomsInfoBatchRequest
	require.NoError(t, json.Unmarshal(encoded, &decodedRequest))
	assert.Equal(t, []string{"r1"}, decodedRequest.RoomIDs)

	encodedResponse, err := json.Marshal(model.RoomsInfoBatchResponse{
		Rooms: []model.RoomInfo{{RoomID: "r1", Found: true, Name: "soak-channel", UserCount: 3}},
	})
	require.NoError(t, err)
	var decodedResponse soakRoomsInfoResponse
	require.NoError(t, json.Unmarshal(encodedResponse, &decodedResponse))
	require.Len(t, decodedResponse.Rooms, 1)
	assert.True(t, decodedResponse.Rooms[0].Found)
	assert.Equal(t, "soak-channel", decodedResponse.Rooms[0].Name)
}

func TestSoakSubscriptionListResponse_DecodesUserServiceReply(t *testing.T) {
	encoded := []byte(`{"subscriptions":[{"roomId":"r1","muted":true}],"hasMore":false}`)

	var decoded soakSubscriptionListResponse
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	require.Len(t, decoded.Subscriptions, 1)
	assert.Equal(t, "r1", decoded.Subscriptions[0].RoomID)
	assert.True(t, decoded.Subscriptions[0].Muted)
}

func TestSoakStatusReply_DecodesRoomServiceAcceptance(t *testing.T) {
	encoded, err := json.Marshal(model.StatusWithRequestReply{Status: "accepted", RequestID: "req-1"})
	require.NoError(t, err)

	var decoded soakStatusReply
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	assert.Equal(t, "accepted", decoded.Status)
	assert.Equal(t, "req-1", decoded.RequestID)
}

// The user-service read carriers are decoded from replies the soak run never
// asserts on, so a wrong key is invisible at runtime: it either reports every
// read as an error the service never made, or reports a full page as empty.
// These marshal the real reply types and decode the carrier from that output.
func TestSoakUserReadCarriers_DecodeUserServiceReplies(t *testing.T) {
	t.Run("apps categories are objects", func(t *testing.T) {
		encoded, err := json.Marshal(usermodels.AppCategoriesResponse{
			Categories: []usermodels.AppCategory{{ID: "cat-1", Name: "Ops", SiteID: "site-a"}},
		})
		require.NoError(t, err)

		var decoded soakUserAppCategoriesResponse
		require.NoError(t, json.Unmarshal(encoded, &decoded),
			"decoding a category object as a string would fail every apps.categories read")
		require.Len(t, decoded.Categories, 1)
		assert.Equal(t, "cat-1", decoded.Categories[0].ID)
	})

	t.Run("thread list pages under items", func(t *testing.T) {
		encoded, err := json.Marshal(model.ThreadListResponse{
			Items:      []model.ThreadListItem{{ThreadRoomID: "thread-1"}},
			NextCursor: "cursor-1", HasNext: true,
		})
		require.NoError(t, err)

		var decoded soakUserThreadListResponse
		require.NoError(t, json.Unmarshal(encoded, &decoded))
		require.Len(t, decoded.Items, 1, "a page read through the wrong key looks empty")
		assert.Equal(t, "thread-1", decoded.Items[0].ThreadRoomID)
		assert.Equal(t, "cursor-1", decoded.NextCursor)
		assert.True(t, decoded.HasNext)
	})

	t.Run("thread unread badge", func(t *testing.T) {
		encoded, err := json.Marshal(model.ThreadUnreadSummaryResponse{Unread: true})
		require.NoError(t, err)

		var decoded soakUserThreadUnreadResponse
		require.NoError(t, json.Unmarshal(encoded, &decoded))
		assert.True(t, decoded.Unread)
	})

	t.Run("settings carry the evaluated permissions", func(t *testing.T) {
		encoded, err := json.Marshal(usermodels.SettingsGetResponse{
			Permissions: map[model.PermissionKey]bool{"canPost": true},
		})
		require.NoError(t, err)

		var decoded soakUserSettingsResponse
		require.NoError(t, json.Unmarshal(encoded, &decoded))
		assert.True(t, decoded.Permissions["canPost"])
	})

	t.Run("apps list", func(t *testing.T) {
		encoded, err := json.Marshal(usermodels.AppsListResponse{
			Apps:    []usermodels.AppListItem{{App: model.App{ID: "app-1"}}},
			HasMore: true,
		})
		require.NoError(t, err)

		var decoded soakUserAppsResponse
		require.NoError(t, json.Unmarshal(encoded, &decoded))
		require.Len(t, decoded.Apps, 1)
		assert.Equal(t, "app-1", decoded.Apps[0].ID)
		assert.True(t, decoded.HasMore)
	})

	t.Run("me and status share the flat account field", func(t *testing.T) {
		encoded, err := json.Marshal(usermodels.MeResponse{
			UserStatusView: usermodels.UserStatusView{Account: "alice"},
			Presence:       model.StatusOnline,
		})
		require.NoError(t, err)

		var decoded soakUserMeResponse
		require.NoError(t, json.Unmarshal(encoded, &decoded))
		assert.Equal(t, "alice", decoded.Account)
	})

	t.Run("priority contacts", func(t *testing.T) {
		encoded, err := json.Marshal(usermodels.PriorityContactsResponse{
			Contacts: []usermodels.PriorityContactItem{{Account: "bob", Type: "user"}},
		})
		require.NoError(t, err)

		var decoded soakUserPriorityContactsResponse
		require.NoError(t, json.Unmarshal(encoded, &decoded))
		require.Len(t, decoded.Contacts, 1)
		assert.Equal(t, "bob", decoded.Contacts[0].Account)
	})

	t.Run("chatlist sections", func(t *testing.T) {
		encoded, err := json.Marshal(model.ChatlistState{
			Sections: []model.ChatlistSection{{ID: "favorites", Name: "Favorites"}},
		})
		require.NoError(t, err)

		var decoded soakUserChatlistResponse
		require.NoError(t, json.Unmarshal(encoded, &decoded))
		require.Len(t, decoded.Sections, 1)
		assert.Equal(t, "favorites", decoded.Sections[0].ID)
	})

	t.Run("subscription count", func(t *testing.T) {
		encoded, err := json.Marshal(usermodels.CountResponse{Count: 7})
		require.NoError(t, err)

		var decoded soakUserCountResponse
		require.NoError(t, json.Unmarshal(encoded, &decoded))
		assert.Equal(t, 7, decoded.Count)
	})
}

// The request bodies are sent, so a wrong key is worse than a wrong reply key:
// the server falls back to its default and the lane silently stops asking what
// it thinks it is asking.
func TestSoakUserReadRequests_MatchUserServiceJSON(t *testing.T) {
	for _, testCase := range []struct {
		name string
		soak any
		real any
	}{
		{
			name: "get by name",
			soak: soakUserNameRequest{Name: "alice"},
			real: usermodels.StatusGetByNameRequest{Name: "alice"},
		},
		{
			name: "get dm",
			soak: soakUserAccountNameRequest{AccountName: "bob"},
			real: usermodels.GetDMRequest{AccountName: "bob"},
		},
		{
			name: "get by room",
			soak: soakUserRoomRequest{RoomID: "room-1"},
			real: usermodels.GetByRoomIDRequest{RoomID: "room-1"},
		},
		{
			name: "subscription count",
			soak: soakUserCountRequest{},
			real: usermodels.CountRequest{},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			soakJSON, err := json.Marshal(testCase.soak)
			require.NoError(t, err)
			realJSON, err := json.Marshal(testCase.real)
			require.NoError(t, err)
			assert.JSONEq(t, string(realJSON), string(soakJSON))
		})
	}
}

// The page request serves apps.list, subscription.getChannels and thread.list,
// which each declare their own paging fields. Only the keys have to agree.
//
// thread.list carries a cursor rather than an offset, so its case is the body
// the lane actually sends it — limit alone. Pairing it with an offset would
// assert a field model.ThreadListRequest does not have.
func TestSoakUserPageRequest_MatchesEveryPagedReader(t *testing.T) {
	for _, testCase := range []struct {
		name string
		soak soakUserPageRequest
		real any
	}{
		{
			name: "apps list",
			soak: soakUserPageRequest{Limit: 20, Offset: 40},
			real: usermodels.AppsListRequest{Limit: 20, Offset: 40},
		},
		{
			name: "subscription channels",
			soak: soakUserPageRequest{Limit: 20, Offset: 40},
			real: usermodels.GetChannelsRequest{Limit: 20, Offset: 40},
		},
		{
			name: "thread list",
			soak: soakUserPageRequest{Limit: 20},
			real: model.ThreadListRequest{Limit: 20},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			soakJSON, err := json.Marshal(testCase.soak)
			require.NoError(t, err)
			realJSON, err := json.Marshal(testCase.real)
			require.NoError(t, err)
			assert.JSONEq(t, string(realJSON), string(soakJSON))
		})
	}
}
