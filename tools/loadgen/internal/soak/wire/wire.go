package wire

import (
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
)

// These wire carriers mirror history-service/internal/models. loadgen cannot
// import that internal package, so compatibility is protected by JSON contract
// tests in soak_wire_test.go.
type RoomMeta struct {
	LastMsgAt *int64 `json:"lastMsgAt,omitempty"`
	CreatedAt *int64 `json:"createdAt,omitempty"`
}

type LoadHistoryRequest struct {
	Before *int64    `json:"before,omitempty"`
	Limit  int       `json:"limit"`
	Meta   *RoomMeta `json:"meta,omitempty"`
}

type LoadHistoryResponse struct {
	Messages          []Message `json:"messages"`
	MinUserLastSeenAt *int64    `json:"minUserLastSeenAt,omitempty"`
}

type LoadNextMessagesRequest struct {
	After  *int64    `json:"after,omitempty"`
	Limit  int       `json:"limit"`
	Cursor string    `json:"cursor"`
	Meta   *RoomMeta `json:"meta,omitempty"`
}

type LoadNextMessagesResponse struct {
	Messages          []Message `json:"messages"`
	NextCursor        string    `json:"nextCursor,omitempty"`
	HasNext           bool      `json:"hasNext"`
	MinUserLastSeenAt *int64    `json:"minUserLastSeenAt,omitempty"`
}

type GetMessageByIDRequest struct {
	MessageID string `json:"messageId"`
}

// Message is the read-side JSON projection required by Run A.
// In particular, it intentionally omits reactions: cassandra.Reactions is a
// storage map with struct keys, while history-service emits a grouped JSON map
// that cannot be unmarshaled back into that storage type.
type Message struct {
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

type EditMessageRequest struct {
	MessageID string `json:"messageId"`
	NewMsg    string `json:"newMsg"`
}

type EditMessageResponse struct {
	MessageID string `json:"messageId"`
	EditedAt  int64  `json:"editedAt"`
}

type DeleteMessageRequest struct {
	MessageID string `json:"messageId"`
}

type DeleteMessageResponse struct {
	MessageID string `json:"messageId"`
	DeletedAt int64  `json:"deletedAt"`
}

type PinMessageRequest struct {
	MessageID string `json:"messageId"`
}

type PinMessageResponse struct {
	MessageID string `json:"messageId"`
	PinnedAt  int64  `json:"pinnedAt"`
}

type UnpinMessageRequest struct {
	MessageID string `json:"messageId"`
}

type UnpinMessageResponse struct {
	MessageID string `json:"messageId"`
}

type ListPinnedMessagesRequest struct {
	Cursor string `json:"cursor,omitempty"`
	Limit  int    `json:"limit"`
}

type ListPinnedMessagesResponse struct {
	Messages   []Message `json:"messages"`
	NextCursor string    `json:"nextCursor,omitempty"`
	HasNext    bool      `json:"hasNext"`
}

type ReactMessageRequest struct {
	MessageID string `json:"messageId"`
	Shortcode string `json:"shortcode"`
}

type ReactMessageResponse struct {
	MessageID string               `json:"messageId"`
	Shortcode string               `json:"shortcode"`
	Action    model.ReactionAction `json:"action"`
	ReactedAt int64                `json:"reactedAt"`
}

type GetThreadMessagesRequest struct {
	ThreadMessageID string `json:"threadMessageId"`
	Cursor          string `json:"cursor,omitempty"`
	Limit           int    `json:"limit"`
}

type GetThreadMessagesResponse struct {
	Messages          []Message `json:"messages"`
	NextCursor        string    `json:"nextCursor,omitempty"`
	HasNext           bool      `json:"hasNext"`
	ParentMessage     *Message  `json:"parentMessage,omitempty"`
	MinUserLastSeenAt *int64    `json:"minUserLastSeenAt,omitempty"`
}

// Room and member request/reply carriers. They mirror pkg/model but stay local
// so a loadgen change can never widen a production struct; the contract tests
// in soak_wire_test.go marshal them against the real model types.
type AddMembersRequest struct {
	RoomID string   `json:"roomId"`
	Users  []string `json:"users"`
}

type RemoveMemberRequest struct {
	RoomID  string `json:"roomId"`
	Account string `json:"account"`
}

type RoomRenameRequest struct {
	NewName string `json:"newName"`
}

type CreateRoomRequest struct {
	Name  string   `json:"name"`
	Users []string `json:"users"`
}

type StatusReply struct {
	Status    string `json:"status"`
	RequestID string `json:"requestId,omitempty"`
}

type CreateRoomReply struct {
	Status   string `json:"status"`
	RoomID   string `json:"roomId"`
	RoomType string `json:"roomType"`
}

type MuteToggleReply struct {
	Status string `json:"status"`
	Muted  bool   `json:"muted"`
}

type RoomMemberEntry struct {
	ID      string `json:"id"`
	Type    string `json:"type"`
	Account string `json:"account,omitempty"`
}

type RoomMember struct {
	ID     string          `json:"id"`
	RoomID string          `json:"rid"`
	Member RoomMemberEntry `json:"member"`
}

type ListMembersResponse struct {
	Members []RoomMember `json:"members"`
}

type RoomsInfoRequest struct {
	RoomIDs []string `json:"roomIds"`
}

type RoomInfo struct {
	RoomID    string `json:"roomId"`
	Found     bool   `json:"found"`
	Name      string `json:"name,omitempty"`
	UserCount int    `json:"userCount,omitempty"`
}

type RoomsInfoResponse struct {
	Rooms []RoomInfo `json:"rooms"`
}

type SubscriptionRow struct {
	RoomID     string     `json:"roomId"`
	Muted      bool       `json:"muted"`
	LastSeenAt *time.Time `json:"lastSeenAt,omitempty"`
}

type ReadReceiptRequest struct {
	MessageID string `json:"messageId"`
}

type ReadReceiptReader struct {
	UserID  string `json:"userId"`
	Account string `json:"account"`
}

type ReadReceiptResponse struct {
	Readers []ReadReceiptReader `json:"readers"`
}

type SubscriptionListResponse struct {
	Subscriptions []SubscriptionRow `json:"subscriptions"`
	HasMore       bool              `json:"hasMore"`
}

// SubscriptionListType is the list user-service serves for the room and
// user lanes. It must be one of user-service's accepted types; the zero value a
// missing body decodes to is rejected as an unknown type, which is how both
// callers silently failed every request before.
const SubscriptionListType = "rooms"

// Limit and IncludeLastMessage are both omitted when unset, which is not
// cosmetic: user-service reads a missing limit as its own default and a missing
// includeLastMessage as true, so sending a zero value would change the workload
// rather than leave it alone.
type SubscriptionListRequest struct {
	Type               string `json:"type"`
	Limit              int    `json:"limit,omitempty"`
	IncludeLastMessage *bool  `json:"includeLastMessage,omitempty"`
}

// user-service read carriers. Each decodes only the fields the read lane
// samples; user-service returns more, and the lane deliberately does not
// assert on payload content it has no expectation for.

type UserNameRequest struct {
	Name string `json:"name"`
}

type UserAccountNameRequest struct {
	AccountName string `json:"accountName"`
}

type UserRoomRequest struct {
	RoomID string `json:"roomId"`
}

type UserPageRequest struct {
	Limit  int `json:"limit,omitempty"`
	Offset int `json:"offset,omitempty"`
}

// UserChannelsRequest carries getChannels' member selector. user-service
// requires exactly one of membersContain / accountNames, so the page fields
// alone are rejected — MembersContain is always set by the caller.
type UserChannelsRequest struct {
	MembersContain string `json:"membersContain"`
	Limit          int    `json:"limit,omitempty"`
	Offset         int    `json:"offset,omitempty"`
}

type UserCountRequest struct {
	Unread *bool `json:"unread,omitempty"`
}

type UserEmptyRequest struct{}

type UserMeResponse struct {
	Account string `json:"account"`
}

type UserStatusResponse struct {
	Account string `json:"account"`
}

// UserSettingsResponse mirrors user-service's SettingsGetResponse, which
// inlines the user's settings and adds the evaluated permissions. Only the
// permissions map is decoded — it is the one key the reply always carries.
type UserSettingsResponse struct {
	Permissions map[string]bool `json:"permissions"`
}

type UserChatlistSection struct {
	ID string `json:"id"`
}

type UserChatlistResponse struct {
	Sections []UserChatlistSection `json:"sections"`
}

type UserPriorityContactsResponse struct {
	Contacts []UserContact `json:"contacts"`
}

type UserContact struct {
	Account string `json:"account"`
}

type UserApp struct {
	ID string `json:"id"`
}

type UserAppsResponse struct {
	Apps    []UserApp `json:"apps"`
	HasMore bool      `json:"hasMore"`
}

// UserAppCategory mirrors user-service's AppCategory. The categories are
// objects, not names: decoding them as strings fails outright and would report
// every apps.categories read as an error the service never made.
type UserAppCategory struct {
	ID string `json:"id"`
}

type UserAppCategoriesResponse struct {
	Categories []UserAppCategory `json:"categories"`
}

type UserCountResponse struct {
	Count int `json:"count"`
}

type UserDMResponse struct {
	Subscription SubscriptionRow `json:"subscription"`
}

type UserThread struct {
	ThreadRoomID string `json:"threadRoomId"`
}

// UserThreadListResponse mirrors model.ThreadListResponse. The page is
// "items", not "threads": reading the wrong key decodes cleanly and silently
// reports every thread-list read as returning nothing.
type UserThreadListResponse struct {
	Items            []UserThread `json:"items"`
	NextCursor       string       `json:"nextCursor,omitempty"`
	HasNext          bool         `json:"hasNext"`
	UnavailableSites []string     `json:"unavailableSites,omitempty"`
}

type UserThreadUnreadResponse struct {
	Unread           bool     `json:"unread"`
	UnavailableSites []string `json:"unavailableSites,omitempty"`
}
