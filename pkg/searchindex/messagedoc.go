package searchindex

import (
	"fmt"
	"strings"
	"time"

	"github.com/hmchangw/chat/pkg/model/cassandra"
)

// MessageDoc is the Elasticsearch document shape for the messages index.
// Shared by search-sync-worker (built live from a MessageEvent) and
// data-migration/es-index-migrator (built from a Cassandra messages_by_room
// row) — this is the one place the wire/mapping contract for the messages
// index is defined; do not redefine it anywhere else.
type MessageDoc struct {
	MessageID             string                 `json:"messageId"                              es:"keyword"`
	RoomID                string                 `json:"roomId"                                 es:"keyword"`
	SiteID                string                 `json:"siteId"                                 es:"keyword"`
	UserID                string                 `json:"userId"                                 es:"keyword"`
	UserAccount           string                 `json:"userAccount"                            es:"keyword"`
	IsBot                 bool                   `json:"isBot,omitempty"                        es:"boolean"`
	Content               string                 `json:"content,omitempty"                      es:"text,custom_analyzer"`
	CreatedAt             time.Time              `json:"createdAt"                              es:"date"`
	EditedAt              *time.Time             `json:"editedAt,omitempty"                     es:"date"`
	UpdatedAt             *time.Time             `json:"updatedAt,omitempty"                    es:"date"`
	ThreadParentID        string                 `json:"threadParentMessageId,omitempty"        es:"keyword"`
	ThreadParentCreatedAt *time.Time             `json:"threadParentMessageCreatedAt,omitempty" es:"date"`
	TShow                 bool                   `json:"tshow,omitempty"                        es:"boolean"`
	AttachmentText        string                 `json:"attachmentText,omitempty"                es:"text,custom_analyzer"`
	CardData              string                 `json:"cardData,omitempty"                      es:"text,custom_analyzer"`
	Attachments           []cassandra.Attachment `json:"attachments,omitempty" es:"object_disabled"`
	Card                  *cassandra.Card        `json:"card,omitempty"        es:"object_disabled"`
}

// MessageFields is the minimal, source-agnostic set of fields needed to
// build a MessageDoc. Callers with different source structs (a live
// MessageEvent's embedded model.Message, or a Cassandra messages_by_room
// row) adapt into this shape so pkg/searchindex never has to depend on
// either source package's full type.
type MessageFields struct {
	MessageID             string
	RoomID                string
	SiteID                string
	UserID                string
	UserAccount           string
	Content               string
	CreatedAt             time.Time
	EditedAt              *time.Time
	UpdatedAt             *time.Time
	ThreadParentID        string
	ThreadParentCreatedAt *time.Time
	TShow                 bool
	// Attachments carries the raw LIST<BLOB> encoding (one JSON blob per
	// attachment) exactly as stored in Cassandra's attachments column /
	// model.Message.Attachments — decoded here via cassandra.DecodeAttachments.
	Attachments [][]byte
	Card        *cassandra.Card
}

// isBotAccount reports whether account looks like a bot account (".bot" suffix).
// Duplicated here (rather than importing pkg/model, which would create an
// import cycle: pkg/model already imports pkg/model/cassandra, and this
// package's callers include services that import pkg/model) is avoided by
// keeping this a plain string check with the same semantics as
// model.IsBot — both must be kept in sync if the convention ever changes.
func isBotAccount(account string) bool {
	return strings.HasSuffix(account, ".bot")
}

// NewMessageDoc builds the ES document for the messages index from f.
//
//nolint:gocritic // hugeParam: f is passed by value to satisfy the builder interface; struct copy is negligible for 200 bytes
func NewMessageDoc(f MessageFields) MessageDoc {
	doc := MessageDoc{
		MessageID:             f.MessageID,
		RoomID:                f.RoomID,
		SiteID:                f.SiteID,
		UserID:                f.UserID,
		UserAccount:           f.UserAccount,
		IsBot:                 isBotAccount(f.UserAccount),
		Content:               f.Content,
		CreatedAt:             f.CreatedAt,
		EditedAt:              f.EditedAt,
		UpdatedAt:             f.UpdatedAt,
		ThreadParentID:        f.ThreadParentID,
		ThreadParentCreatedAt: f.ThreadParentCreatedAt,
		TShow:                 f.TShow,
	}

	attachments, _ := cassandra.DecodeAttachments(f.Attachments)
	doc.Attachments = attachments
	var attachmentText []string
	for i := range attachments {
		a := &attachments[i]
		if a.Title != "" {
			attachmentText = append(attachmentText, a.Title)
		}
		if a.Description != "" {
			attachmentText = append(attachmentText, a.Description)
		}
	}
	doc.AttachmentText = strings.Join(attachmentText, " ")

	if f.Card != nil {
		doc.Card = f.Card
		doc.CardData = string(f.Card.Data)
	}

	return doc
}

// MessageIndexName returns the monthly index name for a message with the
// given createdAt: "{prefix}-{YYYY-MM}" (UTC).
func MessageIndexName(prefix string, createdAt time.Time) string {
	return fmt.Sprintf("%s-%s", prefix, createdAt.UTC().Format("2006-01"))
}
