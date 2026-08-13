// Package main derives and applies the room-level MongoDB state writes implied
// by MESSAGES-CANONICAL events: rooms.lastMsgAt/lastMsgId, the sender's own
// subscription lastSeenAt, and the hasMention badge. Thread-level state is
// owned by message-worker; fan-out is owned by broadcast-worker.
package main

import (
	"time"

	"github.com/hmchangw/chat/pkg/mention"
	"github.com/hmchangw/chat/pkg/model"
)

// writeIntents is the complete set of MongoDB writes implied by one canonical
// event. The zero value means "no writes". Each group is selected by its own
// presence marker so an event can trigger any subset.
type writeIntents struct {
	RoomID string

	// LastMsgID != "" selects the rooms-collection last-message update.
	LastMsgID        string
	LastMsgAt        time.Time
	LastMentionAllAt time.Time // zero unless the message @all-mentions

	// SenderAccount != "" selects the sender's lastSeenAt advance. Sending
	// implies the sender has read up to their own message: advancing it here
	// keeps the room read-floor (rooms.minUserLastSeenAt, computed by
	// room-service as MinSubscriptionLastSeenByRoomID) from counting the
	// sender against their own message (#396). Carried through from
	// broadcast-worker's AdvanceSubscriptionLastSeen — this is read-floor
	// input, not decoration; see the design spec's "Consistency trade-off"
	// section for the ordering guarantee this extraction gave up.
	SenderAccount string
	SenderSeenAt  time.Time

	// MentionAccounts non-empty selects the hasMention badge write.
	MentionAccounts []string
	MentionAt       time.Time
}

// isHiddenThreadReply mirrors broadcast-worker's shouldUseThreadFanOut. A
// TShow=false thread reply never touches room-level state: it is invisible in
// the main channel, and message-worker owns thread_rooms/thread_subscriptions
// for it.
func isHiddenThreadReply(msg *model.Message) bool {
	return msg.ThreadParentMessageID != "" && !msg.TShow
}

// deriveIntents maps a canonical event to its room-level writes. Pure by
// construction: mention.Parse is a function of content alone, and the room id
// is carried on the message — so no MongoDB read is needed to decide anything.
func deriveIntents(evt *model.MessageEvent) writeIntents {
	msg := &evt.Message
	if msg.RoomID == "" || isHiddenThreadReply(msg) {
		return writeIntents{}
	}

	switch evt.Event {
	case model.EventCreated:
		parsed := mention.Parse(msg.Content)
		in := writeIntents{
			RoomID:        msg.RoomID,
			LastMsgID:     msg.ID,
			LastMsgAt:     msg.CreatedAt,
			SenderAccount: msg.UserAccount,
			SenderSeenAt:  msg.CreatedAt,
		}
		if parsed.MentionAll {
			in.LastMentionAllAt = msg.CreatedAt
		}
		if len(parsed.Accounts) > 0 {
			in.MentionAccounts = parsed.Accounts
			in.MentionAt = msg.CreatedAt
		}
		return in

	case model.EventUpdated:
		// Additive badge only, mirroring broadcast-worker's
		// badgeNewlyMentionedAccounts: an edit never moves the room pointer and
		// never clears a mention that the edit removed.
		if msg.EditedAt == nil {
			return writeIntents{}
		}
		parsed := mention.Parse(msg.Content)
		if len(parsed.Accounts) == 0 {
			return writeIntents{}
		}
		return writeIntents{
			RoomID:          msg.RoomID,
			MentionAccounts: parsed.Accounts,
			MentionAt:       *msg.EditedAt,
		}

	default:
		return writeIntents{}
	}
}
