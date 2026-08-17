package main

import (
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
)

// These wire carriers mirror history-service/internal/models. loadgen cannot
// import that internal package, so compatibility is protected by JSON contract
// tests in soak_wire_test.go.
type soakRoomMeta struct {
	LastMsgAt *int64 `json:"lastMsgAt,omitempty"`
	CreatedAt *int64 `json:"createdAt,omitempty"`
}

type soakLoadHistoryRequest struct {
	Before *int64        `json:"before,omitempty"`
	Limit  int           `json:"limit"`
	Meta   *soakRoomMeta `json:"meta,omitempty"`
}

type soakLoadHistoryResponse struct {
	Messages          []soakWireMessage `json:"messages"`
	MinUserLastSeenAt *int64            `json:"minUserLastSeenAt,omitempty"`
}

type soakLoadNextMessagesRequest struct {
	After  *int64        `json:"after,omitempty"`
	Limit  int           `json:"limit"`
	Cursor string        `json:"cursor"`
	Meta   *soakRoomMeta `json:"meta,omitempty"`
}

type soakLoadNextMessagesResponse struct {
	Messages          []soakWireMessage `json:"messages"`
	NextCursor        string            `json:"nextCursor,omitempty"`
	HasNext           bool              `json:"hasNext"`
	MinUserLastSeenAt *int64            `json:"minUserLastSeenAt,omitempty"`
}

type soakGetMessageByIDRequest struct {
	MessageID string `json:"messageId"`
}

// soakWireMessage is the read-side JSON projection required by Run A.
// In particular, it intentionally omits reactions: cassandra.Reactions is a
// storage map with struct keys, while history-service emits a grouped JSON map
// that cannot be unmarshaled back into that storage type.
type soakWireMessage struct {
	RoomID         string                `json:"roomId"`
	CreatedAt      time.Time             `json:"createdAt"`
	MessageID      string                `json:"messageId"`
	Sender         cassandra.Participant `json:"sender"`
	Msg            string                `json:"msg"`
	ThreadParentID string                `json:"threadParentId,omitempty"`
	Deleted        bool                  `json:"deleted,omitempty"`
	EditedAt       *time.Time            `json:"editedAt,omitempty"`
	PinnedAt       *time.Time            `json:"pinnedAt,omitempty"`
}

type soakEditMessageRequest struct {
	MessageID string `json:"messageId"`
	NewMsg    string `json:"newMsg"`
}

type soakEditMessageResponse struct {
	MessageID string `json:"messageId"`
	EditedAt  int64  `json:"editedAt"`
}

type soakDeleteMessageRequest struct {
	MessageID string `json:"messageId"`
}

type soakDeleteMessageResponse struct {
	MessageID string `json:"messageId"`
	DeletedAt int64  `json:"deletedAt"`
}

type soakPinMessageRequest struct {
	MessageID string `json:"messageId"`
}

type soakPinMessageResponse struct {
	MessageID string `json:"messageId"`
	PinnedAt  int64  `json:"pinnedAt"`
}

type soakUnpinMessageRequest struct {
	MessageID string `json:"messageId"`
}

type soakUnpinMessageResponse struct {
	MessageID string `json:"messageId"`
}

type soakListPinnedMessagesRequest struct {
	Cursor string `json:"cursor,omitempty"`
	Limit  int    `json:"limit"`
}

type soakListPinnedMessagesResponse struct {
	Messages   []soakWireMessage `json:"messages"`
	NextCursor string            `json:"nextCursor,omitempty"`
	HasNext    bool              `json:"hasNext"`
}

type soakReactMessageRequest struct {
	MessageID string `json:"messageId"`
	Shortcode string `json:"shortcode"`
}

type soakReactMessageResponse struct {
	MessageID string               `json:"messageId"`
	Shortcode string               `json:"shortcode"`
	Action    model.ReactionAction `json:"action"`
	ReactedAt int64                `json:"reactedAt"`
}

type soakGetThreadMessagesRequest struct {
	ThreadMessageID string `json:"threadMessageId"`
	Cursor          string `json:"cursor,omitempty"`
	Limit           int    `json:"limit"`
}

type soakGetThreadMessagesResponse struct {
	Messages          []soakWireMessage `json:"messages"`
	NextCursor        string            `json:"nextCursor,omitempty"`
	HasNext           bool              `json:"hasNext"`
	ParentMessage     *soakWireMessage  `json:"parentMessage,omitempty"`
	MinUserLastSeenAt *int64            `json:"minUserLastSeenAt,omitempty"`
}

// Room and member request/reply carriers. They mirror pkg/model but stay local
// so a loadgen change can never widen a production struct; the contract tests
// in soak_wire_test.go marshal them against the real model types.
type soakAddMembersRequest struct {
	RoomID string   `json:"roomId"`
	Users  []string `json:"users"`
}

type soakRemoveMemberRequest struct {
	RoomID  string `json:"roomId"`
	Account string `json:"account"`
}

type soakRoomRenameRequest struct {
	NewName string `json:"newName"`
}

type soakCreateRoomRequest struct {
	Name  string   `json:"name"`
	Users []string `json:"users"`
}

type soakStatusReply struct {
	Status    string `json:"status"`
	RequestID string `json:"requestId,omitempty"`
}

type soakCreateRoomReply struct {
	Status   string `json:"status"`
	RoomID   string `json:"roomId"`
	RoomType string `json:"roomType"`
}

type soakMuteToggleReply struct {
	Status string `json:"status"`
	Muted  bool   `json:"muted"`
}

type soakRoomMemberEntry struct {
	ID      string `json:"id"`
	Type    string `json:"type"`
	Account string `json:"account,omitempty"`
}

type soakRoomMember struct {
	ID     string              `json:"id"`
	RoomID string              `json:"rid"`
	Member soakRoomMemberEntry `json:"member"`
}

type soakListMembersResponse struct {
	Members []soakRoomMember `json:"members"`
}

type soakRoomsInfoRequest struct {
	RoomIDs []string `json:"roomIds"`
}

type soakRoomInfo struct {
	RoomID    string `json:"roomId"`
	Found     bool   `json:"found"`
	Name      string `json:"name,omitempty"`
	UserCount int    `json:"userCount,omitempty"`
}

type soakRoomsInfoResponse struct {
	Rooms []soakRoomInfo `json:"rooms"`
}

type soakSubscriptionRow struct {
	RoomID     string     `json:"roomId"`
	Muted      bool       `json:"muted"`
	LastSeenAt *time.Time `json:"lastSeenAt,omitempty"`
}

type soakReadReceiptRequest struct {
	MessageID string `json:"messageId"`
}

type soakReadReceiptReader struct {
	UserID  string `json:"userId"`
	Account string `json:"account"`
}

type soakReadReceiptResponse struct {
	Readers []soakReadReceiptReader `json:"readers"`
}

type soakSubscriptionListResponse struct {
	Subscriptions []soakSubscriptionRow `json:"subscriptions"`
	HasMore       bool                  `json:"hasMore"`
}

// soakSubscriptionListType is the list user-service serves for the room and
// user lanes. It must be one of user-service's accepted types; the zero value a
// missing body decodes to is rejected as an unknown type, which is how both
// callers silently failed every request before.
const soakSubscriptionListType = "rooms"

type soakSubscriptionListRequest struct {
	Type string `json:"type"`
}

// user-service read carriers. Each decodes only the fields the read lane
// samples; user-service returns more, and the lane deliberately does not
// assert on payload content it has no expectation for.

type soakUserNameRequest struct {
	Name string `json:"name"`
}

type soakUserAccountNameRequest struct {
	AccountName string `json:"accountName"`
}

type soakUserRoomRequest struct {
	RoomID string `json:"roomId"`
}

type soakUserPageRequest struct {
	Limit  int `json:"limit,omitempty"`
	Offset int `json:"offset,omitempty"`
}

// soakUserChannelsRequest carries getChannels' member selector. user-service
// requires exactly one of membersContain / accountNames, so the page fields
// alone are rejected — MembersContain is always set by the caller.
type soakUserChannelsRequest struct {
	MembersContain string `json:"membersContain"`
	Limit          int    `json:"limit,omitempty"`
	Offset         int    `json:"offset,omitempty"`
}

type soakUserCountRequest struct {
	Unread *bool `json:"unread,omitempty"`
}

type soakUserEmptyRequest struct{}

type soakUserMeResponse struct {
	Account string `json:"account"`
}

type soakUserStatusResponse struct {
	Account string `json:"account"`
}

// soakUserSettingsResponse mirrors user-service's SettingsGetResponse, which
// inlines the user's settings and adds the evaluated permissions. Only the
// permissions map is decoded — it is the one key the reply always carries.
type soakUserSettingsResponse struct {
	Permissions map[string]bool `json:"permissions"`
}

type soakUserChatlistSection struct {
	ID string `json:"id"`
}

type soakUserChatlistResponse struct {
	Sections []soakUserChatlistSection `json:"sections"`
}

type soakUserPriorityContactsResponse struct {
	Contacts []soakUserContact `json:"contacts"`
}

type soakUserContact struct {
	Account string `json:"account"`
}

type soakUserApp struct {
	ID string `json:"id"`
}

type soakUserAppsResponse struct {
	Apps    []soakUserApp `json:"apps"`
	HasMore bool          `json:"hasMore"`
}

// soakUserAppCategory mirrors user-service's AppCategory. The categories are
// objects, not names: decoding them as strings fails outright and would report
// every apps.categories read as an error the service never made.
type soakUserAppCategory struct {
	ID string `json:"id"`
}

type soakUserAppCategoriesResponse struct {
	Categories []soakUserAppCategory `json:"categories"`
}

type soakUserCountResponse struct {
	Count int `json:"count"`
}

type soakUserDMResponse struct {
	Subscription soakSubscriptionRow `json:"subscription"`
}

type soakUserThread struct {
	ThreadRoomID string `json:"threadRoomId"`
}

// soakUserThreadListResponse mirrors model.ThreadListResponse. The page is
// "items", not "threads": reading the wrong key decodes cleanly and silently
// reports every thread-list read as returning nothing.
type soakUserThreadListResponse struct {
	Items            []soakUserThread `json:"items"`
	NextCursor       string           `json:"nextCursor,omitempty"`
	HasNext          bool             `json:"hasNext"`
	UnavailableSites []string         `json:"unavailableSites,omitempty"`
}

type soakUserThreadUnreadResponse struct {
	Unread           bool     `json:"unread"`
	UnavailableSites []string `json:"unavailableSites,omitempty"`
}
