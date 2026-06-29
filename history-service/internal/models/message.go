package models

import (
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
)

type Message = cassandra.Message
type Participant = cassandra.Participant
type Card = cassandra.Card
type CardAction = cassandra.CardAction
type QuotedParentMessage = cassandra.QuotedParentMessage
type Reactions = cassandra.Reactions
type ReactionKey = cassandra.ReactionKey
type ReactorInfo = cassandra.ReactorInfo

// RoomMeta carries client-cached room times so the server can skip a Mongo lookup; both
// fields are optional and individually validated server-side (bad values fall back to Mongo).
type RoomMeta struct {
	LastMsgAt *int64 `json:"lastMsgAt,omitempty"` // UTC millis
	CreatedAt *int64 `json:"createdAt,omitempty"` // UTC millis
}

type LoadHistoryRequest struct {
	Before *int64    `json:"before,omitempty"` // UTC millis; nil = now
	Limit  int       `json:"limit"`
	Meta   *RoomMeta `json:"meta,omitempty"`
}

type LoadHistoryResponse struct {
	Messages          []Message `json:"messages"`
	MinUserLastSeenAt *int64    `json:"minUserLastSeenAt,omitempty"` // UTC millis
}

type LoadNextMessagesRequest struct {
	After  *int64    `json:"after,omitempty"` // UTC millis; nil = no lower bound
	Limit  int       `json:"limit"`
	Cursor string    `json:"cursor"` // pagination cursor from previous response
	Meta   *RoomMeta `json:"meta,omitempty"`
}

type LoadNextMessagesResponse struct {
	Messages          []Message `json:"messages"`
	NextCursor        string    `json:"nextCursor,omitempty"`
	HasNext           bool      `json:"hasNext"`
	MinUserLastSeenAt *int64    `json:"minUserLastSeenAt,omitempty"` // UTC millis
}

type LoadSurroundingMessagesRequest struct {
	MessageID string    `json:"messageId"` // central message ID
	Limit     int       `json:"limit"`     // total messages including central
	Meta      *RoomMeta `json:"meta,omitempty"`
}

type LoadSurroundingMessagesResponse struct {
	Messages          []Message `json:"messages"`
	MoreBefore        bool      `json:"moreBefore"`
	MoreAfter         bool      `json:"moreAfter"`
	MinUserLastSeenAt *int64    `json:"minUserLastSeenAt,omitempty"` // UTC millis
}

type GetMessageByIDRequest struct {
	MessageID string `json:"messageId"`
}

// GetMessagesByIDsRequest is the request body for the msg.get.ids batch RPC.
type GetMessagesByIDsRequest struct {
	MessageIDs []string `json:"messageIds"`
}

// GetMessagesByIDsResponse is the response body for the msg.get.ids batch RPC.
type GetMessagesByIDsResponse struct {
	Messages []Message `json:"messages"`
}

// RoomsGetRequest is the request body for the rooms.get batch RPC: the rooms whose
// last message the caller wants. The account+site are taken from the subject.
type RoomsGetRequest struct {
	RoomIDs []string `json:"roomIds"`
}

// LastMessage is a room's most-recent message, resolved at read time (A2). Content
// is preview-trimmed. Deleted is returned as-is — a soft-deleted last message
// surfaces with deleted=true and no walk-back to an earlier surviving message.
type LastMessage struct {
	MessageID string      `json:"messageId"`
	Sender    Participant `json:"sender"`
	Content   string      `json:"content"`
	CreatedAt int64       `json:"createdAt"` // UTC millis
	Deleted   bool        `json:"deleted,omitempty"`
}

// RoomsGetResponse maps each requested roomId that has a resolvable last message to
// it. Rooms with no message, or that the caller can't access, or that degraded, are
// omitted (best-effort) rather than failing the whole batch.
type RoomsGetResponse struct {
	Rooms map[string]LastMessage `json:"rooms"`
}

type EditMessageRequest struct {
	MessageID string `json:"messageId"`
	NewMsg    string `json:"newMsg"`
}

type EditMessageResponse struct {
	MessageID string `json:"messageId"`
	EditedAt  int64  `json:"editedAt"` // UTC millis
}

type DeleteMessageRequest struct {
	MessageID string `json:"messageId"`
}

type DeleteMessageResponse struct {
	MessageID string `json:"messageId"`
	DeletedAt int64  `json:"deletedAt"` // UTC millis; mirrors updated_at (no separate deleted_at column)
}

type PinMessageRequest struct {
	MessageID string `json:"messageId"`
}

type PinMessageResponse struct {
	MessageID string `json:"messageId"`
	PinnedAt  int64  `json:"pinnedAt"` // UTC millis
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

// ReactMessageRequest is the client payload for the reaction toggle handler.
type ReactMessageRequest struct {
	MessageID string `json:"messageId"`
	Shortcode string `json:"shortcode"`
}

// ReactMessageResponse echoes the action the server applied ("added" or "removed").
type ReactMessageResponse struct {
	MessageID string               `json:"messageId"`
	Shortcode string               `json:"shortcode"`
	Action    model.ReactionAction `json:"action"`
	ReactedAt int64                `json:"reactedAt"` // UTC millis
}

type GetThreadMessagesRequest struct {
	ThreadMessageID string `json:"threadMessageId"` // must be a top-level thread message ID, not a reply
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
