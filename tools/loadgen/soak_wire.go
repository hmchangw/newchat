package main

import (
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
	Messages          []cassandra.Message `json:"messages"`
	MinUserLastSeenAt *int64              `json:"minUserLastSeenAt,omitempty"`
}

type soakLoadNextMessagesRequest struct {
	After  *int64        `json:"after,omitempty"`
	Limit  int           `json:"limit"`
	Cursor string        `json:"cursor"`
	Meta   *soakRoomMeta `json:"meta,omitempty"`
}

type soakLoadNextMessagesResponse struct {
	Messages          []cassandra.Message `json:"messages"`
	NextCursor        string              `json:"nextCursor,omitempty"`
	HasNext           bool                `json:"hasNext"`
	MinUserLastSeenAt *int64              `json:"minUserLastSeenAt,omitempty"`
}

type soakGetMessageByIDRequest struct {
	MessageID string `json:"messageId"`
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
	Messages   []cassandra.Message `json:"messages"`
	NextCursor string              `json:"nextCursor,omitempty"`
	HasNext    bool                `json:"hasNext"`
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
	Messages          []cassandra.Message `json:"messages"`
	NextCursor        string              `json:"nextCursor,omitempty"`
	HasNext           bool                `json:"hasNext"`
	ParentMessage     *cassandra.Message  `json:"parentMessage,omitempty"`
	MinUserLastSeenAt *int64              `json:"minUserLastSeenAt,omitempty"`
}
