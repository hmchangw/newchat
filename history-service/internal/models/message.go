package models

import "github.com/hmchangw/chat/pkg/model/cassandra"

type Message = cassandra.Message
type Participant = cassandra.Participant
type File = cassandra.File
type Card = cassandra.Card
type CardAction = cassandra.CardAction
type QuotedParentMessage = cassandra.QuotedParentMessage

type LoadHistoryRequest struct {
	Before *int64 `json:"before,omitempty"` // UTC millis; nil = now
	Limit  int    `json:"limit"`
}

type LoadHistoryResponse struct {
	Messages []Message `json:"messages"`
}

type LoadNextMessagesRequest struct {
	After  *int64 `json:"after,omitempty"` // UTC millis; nil = no lower bound
	Limit  int    `json:"limit"`
	Cursor string `json:"cursor"` // pagination cursor from previous response
}

type LoadNextMessagesResponse struct {
	Messages   []Message `json:"messages"`
	NextCursor string    `json:"nextCursor,omitempty"`
	HasNext    bool      `json:"hasNext"`
}

type LoadSurroundingMessagesRequest struct {
	MessageID string `json:"messageId"` // central message ID
	Limit     int    `json:"limit"`     // total messages including central
}

type LoadSurroundingMessagesResponse struct {
	Messages   []Message `json:"messages"`
	MoreBefore bool      `json:"moreBefore"`
	MoreAfter  bool      `json:"moreAfter"`
}

type GetMessageByIDRequest struct {
	MessageID string `json:"messageId"`
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

type GetThreadMessagesRequest struct {
	ThreadMessageID string `json:"threadMessageId"` // must be a top-level thread message ID, not a reply
	Cursor          string `json:"cursor,omitempty"`
	Limit           int    `json:"limit"`
}

type GetThreadMessagesResponse struct {
	Messages   []Message `json:"messages"`
	NextCursor string    `json:"nextCursor,omitempty"`
	HasNext    bool      `json:"hasNext"`
}
