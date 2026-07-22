// Package cassandra holds Cassandra-only row and UDT carriers.
// No bson tags — these types never reach MongoDB (Mongo-bound types live in pkg/model).
package cassandra

import (
	"time"
)

// Participant maps to the Cassandra "Participant" UDT.
// cql tags are required because gocql lowercases Go field names (e.g. "EngName" → "engname"), not snake_case.
type Participant struct {
	ID          string `json:"id"                    cql:"id"`
	EngName     string `json:"engName,omitempty"     cql:"eng_name"`
	CompanyName string `json:"companyName,omitempty" cql:"company_name"`
	AppID       string `json:"appId,omitempty"       cql:"app_id"`
	AppName     string `json:"appName,omitempty"     cql:"app_name"`
	IsBot       bool   `json:"isBot,omitempty"       cql:"is_bot"`
	Account     string `json:"account,omitempty"     cql:"account"`
}

// Card maps to the Cassandra "Card" UDT.
type Card struct {
	Template string `json:"template"       cql:"template"`
	Data     []byte `json:"data,omitempty" cql:"data"`
}

// CardAction maps to the Cassandra "CardAction" UDT.
type CardAction struct {
	Verb        string `json:"verb"                  cql:"verb"`
	Text        string `json:"text,omitempty"        cql:"text"`
	CardID      string `json:"cardId,omitempty"      cql:"card_id"`
	DisplayText string `json:"displayText,omitempty" cql:"display_text"`
	HideExecLog bool   `json:"hideExecLog,omitempty" cql:"hide_exec_log"`
	CardTmID    string `json:"cardTmId,omitempty"    cql:"card_tmid"`
	Data        []byte `json:"data,omitempty"        cql:"data"`
}

// EncMeta maps to the Cassandra "EncMeta" UDT. Nonce is the 12-byte
// AES-256-GCM nonce used to encrypt the row's enc_payload column.
type EncMeta struct {
	Nonce []byte `json:"nonce" cql:"nonce"`
}

// QuotedParentMessage maps to the Cassandra "QuotedParentMessage" UDT.
type QuotedParentMessage struct {
	MessageID          string        `json:"messageId"             cql:"message_id"`
	RoomID             string        `json:"roomId"                cql:"room_id"`
	Sender             Participant   `json:"sender"                cql:"sender"`
	CreatedAt          time.Time     `json:"createdAt"             cql:"created_at"`
	Msg                string        `json:"msg,omitempty"         cql:"msg"`
	Mentions           []Participant `json:"mentions,omitempty"    cql:"mentions"`
	Attachments        [][]byte      `json:"-" cql:"attachments"`
	DecodedAttachments []Attachment  `json:"attachments,omitempty" cql:"-"`
	MessageLink        string        `json:"messageLink,omitempty" cql:"message_link"`
	// ThreadParentID and ThreadParentCreatedAt are set by message-worker when the quoted message is a TShow reply,
	// embedding the parent's identity so history-service can enforce access-window checks without an extra read.
	ThreadParentID        string     `json:"threadParentId,omitempty"        cql:"thread_parent_id"`
	ThreadParentCreatedAt *time.Time `json:"threadParentCreatedAt,omitempty" cql:"thread_parent_created_at"`
	// TShow mirrors the quoted message's own flag: an "also send to channel" thread
	// reply, quotable from its parent channel room (see checkQuoteThreadContext).
	// Transient (cql:"-") — resolved per-request from history's reply, never persisted
	// into the quoted_parent_message UDT.
	TShow bool `json:"tshow,omitempty" cql:"-"`
}

// ForwardedMessage maps to the Cassandra "ForwardedMessage" UDT — a snapshot of a
// forwarded source message. Non-nil marks the message as a forward. Mirrors
// QuotedParentMessage minus the thread-context fields: a forward is not bound to the
// source's conversation the way a quote is.
type ForwardedMessage struct {
	MessageID          string        `json:"messageId"             cql:"message_id"`
	RoomID             string        `json:"roomId"                cql:"room_id"`
	Sender             Participant   `json:"sender"                cql:"sender"`
	CreatedAt          time.Time     `json:"createdAt"             cql:"created_at"`
	Msg                string        `json:"msg,omitempty"         cql:"msg"`
	Mentions           []Participant `json:"mentions,omitempty"    cql:"mentions"`
	Attachments        [][]byte      `json:"-"                     cql:"attachments"`
	DecodedAttachments []Attachment  `json:"attachments,omitempty" cql:"-"`
	MessageLink        string        `json:"messageLink,omitempty" cql:"message_link"`
}

// Message represents a message row in the Cassandra message tables
// (messages_by_room, messages_by_id, thread_messages_by_thread).
//
// cql tags are consumed by the structScan helper in history-service/internal/cassrepo
// to map returned Cassandra columns to struct fields by name, eliminating
// positional scan maintenance.
type Message struct {
	RoomID                string               `json:"roomId"                          cql:"room_id"`
	Bucket                int64                `json:"-"                                cql:"bucket"`
	CreatedAt             time.Time            `json:"createdAt"                       cql:"created_at"`
	MessageID             string               `json:"messageId"                       cql:"message_id"`
	Sender                Participant          `json:"sender"                          cql:"sender"`
	Msg                   string               `json:"msg"                             cql:"msg"`
	Mentions              []Participant        `json:"mentions,omitempty"              cql:"mentions"`
	Attachments           [][]byte             `json:"-"                               cql:"attachments"`
	DecodedAttachments    []Attachment         `json:"attachments,omitempty"           cql:"-"`
	Card                  *Card                `json:"card,omitempty"                  cql:"card"`
	CardAction            *CardAction          `json:"cardAction,omitempty"            cql:"card_action"`
	TShow                 bool                 `json:"tshow,omitempty"                 cql:"tshow"`
	TCount                *int                 `json:"tcount,omitempty"                cql:"tcount"`
	ThreadLastMsgAt       *time.Time           `json:"threadLastMsgAt,omitempty"       cql:"thread_last_msg_at"`
	ThreadParentID        string               `json:"threadParentId,omitempty"        cql:"thread_parent_id"`
	ThreadParentCreatedAt *time.Time           `json:"threadParentCreatedAt,omitempty" cql:"thread_parent_created_at"`
	QuotedParentMessage   *QuotedParentMessage `json:"quotedParentMessage,omitempty"   cql:"quoted_parent_message"`
	Forwarded             *ForwardedMessage    `json:"forwarded,omitempty"             cql:"forwarded"`
	VisibleTo             string               `json:"visibleTo,omitempty"             cql:"visible_to"`
	// Reactions is nil when absent (omitted from JSON); not modified by edit/delete paths.
	Reactions    Reactions    `json:"reactions,omitempty"             cql:"reactions"`
	Deleted      bool         `json:"deleted,omitempty"               cql:"deleted"`
	Type         string       `json:"type,omitempty"                  cql:"type"`
	SysMsgData   []byte       `json:"sysMsgData,omitempty"            cql:"sys_msg_data"`
	SiteID       string       `json:"siteId,omitempty"                cql:"site_id"`
	EditedAt     *time.Time   `json:"editedAt,omitempty"              cql:"edited_at"`
	UpdatedAt    *time.Time   `json:"updatedAt,omitempty"             cql:"updated_at"`
	EncPayload   []byte       `json:"encPayload,omitempty"            cql:"enc_payload"`
	EncMeta      *EncMeta     `json:"encMeta,omitempty"               cql:"enc_meta"`
	ThreadRoomID string       `json:"threadRoomId,omitempty"          cql:"thread_room_id"`
	PinnedAt     *time.Time   `json:"pinnedAt,omitempty"              cql:"pinned_at"`
	PinnedBy     *Participant `json:"pinnedBy,omitempty"              cql:"pinned_by"`
}
