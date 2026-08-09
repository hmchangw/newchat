package searchindex

import (
	"fmt"
	"strings"
	"time"

	"github.com/hmchangw/chat/pkg/model"
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
	// UserName is the sender's pre-composed display name, indexed so free-text
	// query also matches on sender name (see MessageFields.UserName).
	UserName string `json:"userName,omitempty" es:"text,custom_analyzer"`
	// FileTypes is the deduped set of attachment categories on this message
	// (image/pdf/excel/powerpoint/word/zip/others), derived from Attachments'
	// FileType MIME (see fileTypeCategory) — this is the field the "file search"
	// filter matches, folded into the messages index rather than a new subject.
	FileTypes []string `json:"fileTypes,omitempty" es:"keyword"`
	// Mentions is the deduped set of mentioned accounts (MessageFields.MentionAccounts).
	// Indexed now; the mentionedMe filter that consumes it ships in a follow-up once
	// mentions are resolved onto the canonical event (they currently ride empty).
	Mentions []string `json:"mentions,omitempty" es:"keyword"`
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
	// UserName is the sender's pre-composed display name (Message.UserDisplayName).
	UserName string
	// MentionAccounts is the list of mentioned accounts (Message.Mentions[].Account).
	MentionAccounts []string
}

// NewMessageDoc builds the ES document for the messages index from f. The
// returned doc is always complete and usable; a non-nil error only signals
// that one or more of f.Attachments were malformed and skipped (see
// cassandra.DecodeAttachments) — callers should log it, not treat it as fatal.
//
//nolint:gocritic // hugeParam: f is passed by value to satisfy the builder interface; struct copy is negligible for 200 bytes
func NewMessageDoc(f MessageFields) (MessageDoc, error) {
	doc := MessageDoc{
		MessageID:             f.MessageID,
		RoomID:                f.RoomID,
		SiteID:                f.SiteID,
		UserID:                f.UserID,
		UserAccount:           f.UserAccount,
		IsBot:                 model.IsBot(f.UserAccount),
		Content:               f.Content,
		CreatedAt:             f.CreatedAt,
		EditedAt:              f.EditedAt,
		UpdatedAt:             f.UpdatedAt,
		ThreadParentID:        f.ThreadParentID,
		ThreadParentCreatedAt: f.ThreadParentCreatedAt,
		TShow:                 f.TShow,
		UserName:              f.UserName,
		Mentions:              dedupeStrings(f.MentionAccounts),
	}

	attachments, skipped := cassandra.DecodeAttachments(f.Attachments)
	doc.Attachments = attachments
	var attachmentText []string
	var fileTypes []string
	for i := range attachments {
		a := &attachments[i]
		if a.Title != "" {
			attachmentText = append(attachmentText, a.Title)
		}
		if a.Description != "" {
			attachmentText = append(attachmentText, a.Description)
		}
		fileTypes = append(fileTypes, fileTypeCategory(a.FileType))
	}
	doc.AttachmentText = strings.Join(attachmentText, " ")
	doc.FileTypes = dedupeStrings(fileTypes)

	if f.Card != nil {
		doc.Card = f.Card
		doc.CardData = string(f.Card.Data)
	}

	var err error
	if skipped > 0 {
		err = fmt.Errorf("skipped %d malformed attachment(s) for message %s", skipped, f.MessageID)
	}
	return doc, err
}

// MessageIndexName returns the monthly index name for a message with the
// given createdAt: "{prefix}-{YYYY-MM}" (UTC).
func MessageIndexName(prefix string, createdAt time.Time) string {
	return fmt.Sprintf("%s-%s", prefix, createdAt.UTC().Format("2006-01"))
}
