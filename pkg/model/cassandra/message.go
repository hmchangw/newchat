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
//
// ThreadParentID and ThreadParentCreatedAt capture the parent message's
// thread context (when the parent was itself a thread reply). Used by
// gatekeeper to enforce same-thread-context quoting and by clients to
// render thread-aware quote previews.
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
// cql tags are consumed by the structScan helper in history-service/internal/cassrepo
// to map returned Cassandra columns to struct fields by name, eliminating
// positional scan maintenance.
type Message struct {
	RoomID                string                   `json:"roomId"                          cql:"room_id"`
	Bucket                int64                    `json:"-"                                cql:"bucket"`
	CreatedAt             time.Time                `json:"createdAt"                       cql:"created_at"`
	MessageID             string                   `json:"messageId"                       cql:"message_id"`
	Sender                Participant              `json:"sender"                          cql:"sender"`
	TargetUser            *Participant             `json:"targetUser,omitempty"            cql:"target_user"`
	Msg                   string                   `json:"msg"                             cql:"msg"`
	Mentions              []Participant            `json:"mentions,omitempty"              cql:"mentions"`
	Attachments           [][]byte                 `json:"attachments,omitempty"           cql:"attachments"`
	File                  *File                    `json:"file,omitempty"                  cql:"file"`
	Card                  *Card                    `json:"card,omitempty"                  cql:"card"`
	CardAction            *CardAction              `json:"cardAction,omitempty"            cql:"card_action"`
	TShow                 bool                     `json:"tshow,omitempty"                 cql:"tshow"`
	TCount                *int                     `json:"tcount,omitempty"                cql:"tcount"`
	ThreadParentID        string                   `json:"threadParentId,omitempty"        cql:"thread_parent_id"`
	ThreadParentCreatedAt *time.Time               `json:"threadParentCreatedAt,omitempty" cql:"thread_parent_created_at"`
	QuotedParentMessage   *QuotedParentMessage     `json:"quotedParentMessage,omitempty"   cql:"quoted_parent_message"`
	VisibleTo             string                   `json:"visibleTo,omitempty"             cql:"visible_to"`
	Reactions             map[string][]Participant `json:"reactions,omitempty"             cql:"reactions"`
	Deleted               bool                     `json:"deleted,omitempty"               cql:"deleted"`
	Type                  string                   `json:"type,omitempty"                  cql:"type"`
	SysMsgData            []byte                   `json:"sysMsgData,omitempty"            cql:"sys_msg_data"`
	SiteID                string                   `json:"siteId,omitempty"                cql:"site_id"`
	EditedAt              *time.Time               `json:"editedAt,omitempty"              cql:"edited_at"`
	UpdatedAt             *time.Time               `json:"updatedAt,omitempty"             cql:"updated_at"`
	ThreadRoomID          string                   `json:"threadRoomId,omitempty"          cql:"thread_room_id"`
	PinnedAt              *time.Time               `json:"pinnedAt,omitempty"              cql:"pinned_at"`
	PinnedBy              *Participant             `json:"pinnedBy,omitempty"              cql:"pinned_by"`
}
