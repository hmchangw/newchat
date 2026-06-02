package model

import (
	"time"

	"github.com/hmchangw/chat/pkg/model/cassandra"
)

type Message struct {
	ID                           string                         `json:"id"                                     bson:"_id"`
	RoomID                       string                         `json:"roomId"                                 bson:"roomId"`
	UserID                       string                         `json:"userId"                                 bson:"userId"`
	UserAccount                  string                         `json:"userAccount"                            bson:"userAccount"`
	Content                      string                         `json:"content"                                bson:"content"`
	Mentions                     []Participant                  `json:"mentions,omitempty"                     bson:"mentions,omitempty"`
	CreatedAt                    time.Time                      `json:"createdAt"                              bson:"createdAt"`
	EditedAt                     *time.Time                     `json:"editedAt,omitempty"                     bson:"editedAt,omitempty"`
	UpdatedAt                    *time.Time                     `json:"updatedAt,omitempty"                    bson:"updatedAt,omitempty"`
	ThreadParentMessageID        string                         `json:"threadParentMessageId,omitempty"        bson:"threadParentMessageId,omitempty"`
	ThreadParentMessageCreatedAt *time.Time                     `json:"threadParentMessageCreatedAt,omitempty" bson:"threadParentMessageCreatedAt,omitempty"`
	TShow                        bool                           `json:"tshow,omitempty"                        bson:"tshow,omitempty"`
	Type                         string                         `json:"type,omitempty"                         bson:"type,omitempty"`
	SysMsgData                   []byte                         `json:"sysMsgData,omitempty"                   bson:"sysMsgData,omitempty"`
	QuotedParentMessage          *cassandra.QuotedParentMessage `json:"quotedParentMessage,omitempty"          bson:"quotedParentMessage,omitempty"`
	PinnedAt                     *time.Time                     `json:"pinnedAt,omitempty"                     bson:"pinnedAt,omitempty"`
	PinnedBy                     *Participant                   `json:"pinnedBy,omitempty"                     bson:"pinnedBy,omitempty"`
}

type SendMessageRequest struct {
	ID                           string `json:"id"`
	Content                      string `json:"content"`
	RequestID                    string `json:"requestId"`
	ThreadParentMessageID        string `json:"threadParentMessageId,omitempty"`
	ThreadParentMessageCreatedAt *int64 `json:"threadParentMessageCreatedAt,omitempty"`
	QuotedParentMessageID        string `json:"quotedParentMessageId,omitempty"`
}
