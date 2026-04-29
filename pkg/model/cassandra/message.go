package cassandra

import (
	"time"
)

// Participant maps to the Cassandra "Participant" UDT.
// cql struct tags tell gocql's reflection-based UDT marshaler how to map each
// Go field to its Cassandra UDT field name. Without these tags, gocql would
// lowercase the Go field names (e.g. "EngName" → "engname") which would not
// match the snake_case UDT fields (e.g. "eng_name").
type Participant struct {
	ID          string `json:"id"                    cql:"id"`
	EngName     string `json:"engName,omitempty"     cql:"eng_name"`
	CompanyName string `json:"companyName,omitempty" cql:"company_name"`
	AppID       string `json:"appId,omitempty"       cql:"app_id"`
	AppName     string `json:"appName,omitempty"     cql:"app_name"`
	IsBot       bool   `json:"isBot,omitempty"       cql:"is_bot"`
	Account     string `json:"account,omitempty"     cql:"account"`
}

// File maps to the Cassandra "File" UDT.
type File struct {
	ID   string `json:"id"   cql:"id"`
	Name string `json:"name" cql:"name"`
	Type string `json:"type" cql:"type"`
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

// QuotedParentMessage maps to the Cassandra "QuotedParentMessage" UDT.
type QuotedParentMessage struct {
	MessageID   string        `json:"messageId"             cql:"message_id"`
	RoomID      string        `json:"roomId"                cql:"room_id"`
	Sender      Participant   `json:"sender"                cql:"sender"`
	CreatedAt   time.Time     `json:"createdAt"             cql:"created_at"`
	Msg         string        `json:"msg,omitempty"         cql:"msg"`
	Mentions    []Participant `json:"mentions,omitempty"    cql:"mentions"`
	Attachments [][]byte      `json:"attachments,omitempty" cql:"attachments"`
	MessageLink string        `json:"messageLink,omitempty" cql:"message_link"`
	// ThreadParentID and ThreadParentCreatedAt are populated by message-worker when
	// the quoted message is a TShow reply. They embed the thread parent's identity and
	// actual CreatedAt so history-service can enforce access-window checks without a
	// Cassandra round-trip at read time.
	ThreadParentID        string     `json:"threadParentId,omitempty"        cql:"thread_parent_id"`
	ThreadParentCreatedAt *time.Time `json:"threadParentCreatedAt,omitempty" cql:"thread_parent_created_at"`
}

// Message represents a message row in the Cassandra message tables
// (messages_by_room, messages_by_id, thread_messages_by_room).
//
// Row mapping is driven by the hard-coded column lists and positional Scan
// calls in cassrepo (baseScanDest / threadMessageScanDest) — gocql reflection
// never touches this struct directly, so no cql tags are needed here.
type Message struct {
	RoomID                string                   `json:"roomId"`
	CreatedAt             time.Time                `json:"createdAt"`
	MessageID             string                   `json:"messageId"`
	Sender                Participant              `json:"sender"`
	TargetUser            *Participant             `json:"targetUser,omitempty"`
	Msg                   string                   `json:"msg"`
	Mentions              []Participant            `json:"mentions,omitempty"`
	Attachments           [][]byte                 `json:"attachments,omitempty"`
	File                  *File                    `json:"file,omitempty"`
	Card                  *Card                    `json:"card,omitempty"`
	CardAction            *CardAction              `json:"cardAction,omitempty"`
	TShow                 bool                     `json:"tshow,omitempty"`
	TCount                *int                     `json:"tcount,omitempty"`
	ThreadParentID        string                   `json:"threadParentId,omitempty"`
	ThreadParentCreatedAt *time.Time               `json:"threadParentCreatedAt,omitempty"`
	QuotedParentMessage   *QuotedParentMessage     `json:"quotedParentMessage,omitempty"`
	VisibleTo             string                   `json:"visibleTo,omitempty"`
	Unread                bool                     `json:"unread,omitempty"`
	Reactions             map[string][]Participant `json:"reactions,omitempty"`
	Deleted               bool                     `json:"deleted,omitempty"`
	Type                  string                   `json:"type,omitempty"`
	SysMsgData            []byte                   `json:"sysMsgData,omitempty"`
	SiteID                string                   `json:"siteId,omitempty"`
	EditedAt              *time.Time               `json:"editedAt,omitempty"`
	UpdatedAt             *time.Time               `json:"updatedAt,omitempty"`
	ThreadRoomID          string                   `json:"threadRoomId,omitempty"`
	PinnedAt              *time.Time               `json:"pinnedAt,omitempty"`
	PinnedBy              *Participant             `json:"pinnedBy,omitempty"`
}
