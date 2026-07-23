package main

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"regexp"
	"strings"
	"time"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
)

// MessageTransformer converts a source-shaped payload into a canonical message.
// Source-agnostic (raw is opaque JSON) so a non-Teams source is a different impl.
type MessageTransformer interface {
	Transform(ctx context.Context, raw json.RawMessage) (model.Message, error)
}

// teamsMessage is the DefaultTransformer's view of a Teams history message. The
// exact shape is this transformer's own concern (overridable), not the RPC contract.
type teamsMessage struct {
	ID              string          `json:"id"`
	RoomID          string          `json:"roomId"` // nextgen target room; the caller supplies it (room mapping is Phase 2)
	From            teamsUser       `json:"from"`
	Body            teamsBody       `json:"body"`
	CreatedDateTime time.Time       `json:"createdDateTime"`
	MessageType     string          `json:"messageType"` // "message" (user) or a system event type
	ReplyToMessage  *teamsMessage   `json:"replyToMessage,omitempty"`
	Mentions        []teamsMention  `json:"mentions,omitempty"`
	Reactions       []teamsReaction `json:"reactions,omitempty"`
	Forwarded       bool            `json:"forwarded,omitempty"`
}

type teamsUser struct {
	ID          string `json:"id"`
	DisplayName string `json:"displayName"`
}

type teamsBody struct {
	ContentType string `json:"contentType"` // "html" or "text"
	Content     string `json:"content"`
}

type teamsMention struct {
	UserID      string `json:"userId"`
	DisplayName string `json:"displayName"`
}

type teamsReaction struct {
	ReactionType string `json:"reactionType"`
	UserID       string `json:"userId"`
}

// DefaultTransformer is the built-in Teams transform. Inject a custom
// MessageTransformer to migrate a different source.
type DefaultTransformer struct {
	resolver identityResolver
}

func NewDefaultTransformer(r identityResolver) *DefaultTransformer {
	return &DefaultTransformer{resolver: r}
}

func (t *DefaultTransformer) Transform(ctx context.Context, raw json.RawMessage) (model.Message, error) {
	var tm teamsMessage
	if err := json.Unmarshal(raw, &tm); err != nil {
		return model.Message{}, fmt.Errorf("decode teams message: %w", err)
	}

	sender, err := t.resolver.resolve(ctx, tm.From.ID, tm.From.DisplayName)
	if err != nil {
		return model.Message{}, fmt.Errorf("resolve sender: %w", err)
	}

	msg := model.Message{
		RoomID:          tm.RoomID,
		UserAccount:     sender.Account,
		UserID:          sender.UserID,
		UserDisplayName: sender.DisplayName,
		Content:         bodyToContent(tm.Body),
		CreatedAt:       tm.CreatedDateTime,
		Type:            teamsMessageType(tm.MessageType),
	}

	mentions, err := t.resolveMentions(ctx, tm.Mentions)
	if err != nil {
		return model.Message{}, err
	}
	msg.Mentions = mentions

	if tm.ReplyToMessage != nil {
		quoted, err := t.buildQuotedParent(ctx, tm.RoomID, tm.ReplyToMessage)
		if err != nil {
			return model.Message{}, err
		}
		msg.QuotedParentMessage = quoted
	}

	// ponytail: the forward branch depends on the Forwarded model field (lands with the
	// forward feature); until then a forwarded message migrates as a plain message and no
	// forward field is set. TODO(forward): populate the forward snapshot once the field exists.
	_ = tm.Forwarded

	// ponytail: Teams reactions map via reactionShortcode, but model.Message carries no
	// reactions field — they migrate as separate reacted events in a follow-up, not here.
	return msg, nil
}

func (t *DefaultTransformer) resolveMentions(ctx context.Context, mentions []teamsMention) ([]model.Participant, error) {
	if len(mentions) == 0 {
		return nil, nil
	}
	out := make([]model.Participant, 0, len(mentions))
	for _, m := range mentions {
		s, err := t.resolver.resolve(ctx, m.UserID, m.DisplayName)
		if err != nil {
			return nil, fmt.Errorf("resolve mention %q: %w", m.UserID, err)
		}
		out = append(out, model.Participant{
			Account: s.Account, UserID: s.UserID, EngName: s.EngName,
			ChineseName: s.ChineseName, DisplayName: s.DisplayName,
		})
	}
	return out, nil
}

// roomID is the OUTER reply's room — the conversation the quote belongs to. The
// nested parent payload's own roomId may be absent, so scope by roomID for both the
// parent's deterministic id and its RoomID.
func (t *DefaultTransformer) buildQuotedParent(ctx context.Context, roomID string, parent *teamsMessage) (*cassandra.QuotedParentMessage, error) {
	s, err := t.resolver.resolve(ctx, parent.From.ID, parent.From.DisplayName)
	if err != nil {
		return nil, fmt.Errorf("resolve quoted sender: %w", err)
	}
	return &cassandra.QuotedParentMessage{
		MessageID: deterministicMessageID(roomID, parent.ID),
		RoomID:    roomID,
		Sender:    cassandra.Participant{ID: s.UserID, Account: s.Account, EngName: s.EngName},
		CreatedAt: parent.CreatedDateTime,
		Msg:       bodyToContent(parent.Body),
	}, nil
}

// deterministicMessageID is a stable, valid message id derived from the Teams id
// scoped by conversation, so a batch re-run overwrites the same row (idempotent).
// Teams message ids are unique only within a conversation, so roomId (the nextgen
// room, 1:1 with the Teams chat) is folded into the seed to avoid cross-chat collisions.
func deterministicMessageID(chatScope, teamsID string) string {
	return idgen.MessageIDFromRequestID(chatScope+":"+teamsID, "teams")
}

// teamsMessageType returns "" for a normal user message; any other Teams type is a
// system message tagged with a stable marker (the exact taxonomy is overridable).
func teamsMessageType(msgType string) string {
	if msgType == "" || msgType == "message" {
		return ""
	}
	return "teams_system"
}

// bodyToContent renders the Teams body as content text: HTML → supported markdown,
// text bodies pass through raw.
func bodyToContent(b teamsBody) string {
	if strings.EqualFold(b.ContentType, "html") {
		return htmlToMarkdown(b.Content)
	}
	return b.Content
}

var (
	reBold   = regexp.MustCompile(`(?is)<(?:b|strong)\b[^>]*>(.*?)</(?:b|strong)>`)
	reItalic = regexp.MustCompile(`(?is)<(?:i|em)\b[^>]*>(.*?)</(?:i|em)>`)
	reLink   = regexp.MustCompile(`(?is)<a\b[^>]*\bhref="([^"]*)"[^>]*>(.*?)</a>`)
	reBreak  = regexp.MustCompile(`(?i)<br\s*/?>`)
	reTag    = regexp.MustCompile(`(?s)<[^>]+>`)
)

// htmlToMarkdown converts the supported subset (bold/italic/link/line-break) to
// markdown and drops any other tag to its inner text — i.e. unsupported markup
// degrades to raw string content. Entities are unescaped last.
// ponytail: regex-level, not a full HTML parser; inject a custom transformer for richer HTML.
func htmlToMarkdown(s string) string {
	s = reBreak.ReplaceAllString(s, "\n")
	s = reBold.ReplaceAllString(s, "**$1**")
	s = reItalic.ReplaceAllString(s, "*$1*")
	s = reLink.ReplaceAllString(s, "[$2]($1)")
	s = reTag.ReplaceAllString(s, "")
	return html.UnescapeString(s)
}

// reactionShortcode maps a Teams reactionType to a nextgen emoji shortcode.
// ponytail: unknown types fall through to a colon-wrapped literal; extend the table as needed.
func reactionShortcode(reactionType string) string {
	switch strings.ToLower(reactionType) {
	case "like":
		return ":thumbsup:"
	case "heart":
		return ":heart:"
	case "laugh":
		return ":laughing:"
	case "surprised":
		return ":open_mouth:"
	case "sad":
		return ":cry:"
	case "angry":
		return ":angry:"
	case "":
		return ""
	default:
		return ":" + reactionType + ":"
	}
}
