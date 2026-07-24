package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/teamsmigrate"
)

// MessageTransformer converts a source-shaped payload into a canonical message.
// Source-agnostic (raw is opaque JSON) so a non-Teams source is a different impl.
type MessageTransformer interface {
	Transform(ctx context.Context, raw json.RawMessage) (model.Message, error)
}

// DefaultTransformer is the built-in Teams transform. Inject a custom
// MessageTransformer to migrate a different source. The payload types + payload-only
// helpers live in pkg/teamsmigrate; the sender resolution stays here (it needs Mongo).
type DefaultTransformer struct {
	resolver identityResolver
}

func NewDefaultTransformer(r identityResolver) *DefaultTransformer {
	return &DefaultTransformer{resolver: r}
}

func (t *DefaultTransformer) Transform(ctx context.Context, raw json.RawMessage) (model.Message, error) {
	var tm teamsmigrate.Message
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
		Content:         teamsmigrate.BodyToContent(tm.Body),
		CreatedAt:       tm.CreatedDateTime,
		Type:            teamsmigrate.MessageType(tm.MessageType),
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
	// reactions field â they migrate as separate reacted events in a follow-up, not here.
	return msg, nil
}

func (t *DefaultTransformer) resolveMentions(ctx context.Context, mentions []teamsmigrate.Mention) ([]model.Participant, error) {
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

// roomID is the OUTER reply's room â the conversation the quote belongs to. The
// nested parent payload's own roomId may be absent, so the outer roomID is its RoomID;
// the parent’s id derives from the room scope + its Teams message id (unique per conversation).
func (t *DefaultTransformer) buildQuotedParent(ctx context.Context, roomID string, parent *teamsmigrate.Message) (*cassandra.QuotedParentMessage, error) {
	s, err := t.resolver.resolve(ctx, parent.From.ID, parent.From.DisplayName)
	if err != nil {
		return nil, fmt.Errorf("resolve quoted sender: %w", err)
	}
	return &cassandra.QuotedParentMessage{
		MessageID: teamsmigrate.DeterministicMessageID(roomID, parent.ID),
		RoomID:    roomID,
		Sender:    cassandra.Participant{ID: s.UserID, Account: s.Account, EngName: s.EngName},
		CreatedAt: parent.CreatedDateTime,
		Msg:       teamsmigrate.BodyToContent(parent.Body),
	}, nil
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
