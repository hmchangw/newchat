package main

import (
	"time"

	"github.com/hmchangw/chat/pkg/model"
)

// eventProjection is the slice of a MESSAGES-CANONICAL event this service
// actually reads, decoded in place of the full model.MessageEvent.
//
// model.Message carries 21 fields; deriveIntents reads nine of them. Decoding
// the rest allocates per message for values that are discarded immediately —
// the attachment descriptors and sysMsgData are base64 on the wire, so they
// cost a decode and a copy each, and Mentions, QuotedParentMessage, Card,
// CardAction, PreviewMessage and ReactionDelta each cost an allocation.
//
// Narrowing also decouples this service from the shape of the fields it does
// not read: a field added to model.Message with a codec sonic handles awkwardly
// cannot break a consumer that never decodes it. message-gatekeeper's
// quotedParentProjection exists for exactly that reason (see CLAUDE.md §6).
//
// The tags must track model.MessageEvent and model.Message. projection_test.go
// pins that by decoding one payload both ways and comparing every field.
type eventProjection struct {
	Event   model.EventType   `json:"event"`
	SiteID  string            `json:"siteId"`
	Message messageProjection `json:"message"`
}

// messageProjection mirrors the model.Message fields deriveIntents reads.
type messageProjection struct {
	ID          string    `json:"id"`
	RoomID      string    `json:"roomId"`
	UserAccount string    `json:"userAccount"`
	Content     string    `json:"content"`
	CreatedAt   time.Time `json:"createdAt"`
	// EditedAt selects the edit path; nil on a create.
	EditedAt *time.Time `json:"editedAt,omitempty"`
	// ThreadParentMessageID and TShow together decide whether the message is
	// visible in the room's main timeline — see IsHiddenThreadReply.
	ThreadParentMessageID string `json:"threadParentMessageId,omitempty"`
	TShow                 bool   `json:"tshow,omitempty"`
}

// IsHiddenThreadReply defers to pkg/model so the projection cannot drift from
// the rule the other services branch on — the whole reason that rule was hoisted
// out of the three copies it used to have.
func (m *messageProjection) IsHiddenThreadReply() bool {
	return model.IsHiddenThreadReply(m.ThreadParentMessageID, m.TShow)
}
