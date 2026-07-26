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
