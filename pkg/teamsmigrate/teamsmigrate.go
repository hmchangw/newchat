// Package teamsmigrate holds the source-agnostic Teams message-history payload
// types and transform helpers shared by the migration consumer (message-worker,
// which persists) and the search indexer (search-sync-worker, which indexes the
// same batch without a Mongo lookup). Sender-resolving logic stays in
// message-worker — only the parts that derive purely from the raw payload live here.
package teamsmigrate

import (
	"html"
	"regexp"
	"strings"
	"time"

	"github.com/hmchangw/chat/pkg/idgen"
)

// Message is the decoder's view of a Teams history message. The exact shape is
// the migration's own concern (overridable via a custom transformer), not the RPC contract.
type Message struct {
	ID              string     `json:"id"`
	RoomID          string     `json:"roomId"` // nextgen target room; the caller supplies it
	From            User       `json:"from"`
	Body            Body       `json:"body"`
	CreatedDateTime time.Time  `json:"createdDateTime"`
	MessageType     string     `json:"messageType"` // "message" (user) or a system event type
	ReplyToMessage  *Message   `json:"replyToMessage,omitempty"`
	Mentions        []Mention  `json:"mentions,omitempty"`
	Reactions       []Reaction `json:"reactions,omitempty"`
	Forwarded       bool       `json:"forwarded,omitempty"`
}

type User struct {
	ID          string `json:"id"`
	DisplayName string `json:"displayName"`
}

type Body struct {
	ContentType string `json:"contentType"` // "html" or "text"
	Content     string `json:"content"`
}

type Mention struct {
	UserID      string `json:"userId"`
	DisplayName string `json:"displayName"`
}

type Reaction struct {
	ReactionType string `json:"reactionType"`
	UserID       string `json:"userId"`
}

// BodyToContent renders a Teams body as content text: HTML → supported markdown,
// text bodies pass through raw.
func BodyToContent(b Body) string {
	if strings.EqualFold(b.ContentType, "html") {
		return htmlToMarkdown(b.Content)
	}
	return b.Content
}

// EmployeeIDFromGraphID derives a deterministic 17-char base62 id from the Graph
// object id — the same id shape as native users, and the same hash the HR sync uses,
// so a person resolved by either path is one identity. The indexer reuses it to derive
// the author key without a Mongo read.
func EmployeeIDFromGraphID(graphID string) string {
	return idgen.DeterministicID([]byte(graphID))
}

// DeterministicMessageID is a stable, valid message id derived from the room scope +
// the Teams message id. Teams ids are unique only per conversation, so scoping by room
// keeps a batch re-run idempotent AND prevents cross-room id collisions.
func DeterministicMessageID(chatScope, teamsID string) string {
	return idgen.MessageIDFromRequestID(chatScope+":"+teamsID, "teams")
}

// MessageType returns "" for a normal user message; any other Teams type is a
// system message tagged with a stable marker.
func MessageType(msgType string) string {
	if msgType == "" || msgType == "message" {
		return ""
	}
	return "teams_system"
}

var (
	reBold   = regexp.MustCompile(`(?is)<(?:b|strong)\b[^>]*>(.*?)</(?:b|strong)>`)
	reItalic = regexp.MustCompile(`(?is)<(?:i|em)\b[^>]*>(.*?)</(?:i|em)>`)
	reLink   = regexp.MustCompile(`(?is)<a\b[^>]*\bhref="([^"]*)"[^>]*>(.*?)</a>`)
	reBreak  = regexp.MustCompile(`(?i)<br\s*/?>`)
	reTag    = regexp.MustCompile(`(?s)<[^>]+>`)
)

// htmlToMarkdown converts the supported subset (bold/italic/link/line-break) to
// markdown and drops any other tag to its inner text — unsupported markup degrades
// to raw string content. Entities are unescaped last.
// ponytail: regex-level, not a full HTML parser; inject a custom transformer for richer HTML.
func htmlToMarkdown(s string) string {
	s = reBreak.ReplaceAllString(s, "\n")
	s = reBold.ReplaceAllString(s, "**$1**")
	s = reItalic.ReplaceAllString(s, "*$1*")
	s = reLink.ReplaceAllString(s, "[$2]($1)")
	s = reTag.ReplaceAllString(s, "")
	return html.UnescapeString(s)
}
