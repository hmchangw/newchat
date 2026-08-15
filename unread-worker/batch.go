package main

import (
	"context"
	"time"

	"github.com/hmchangw/chat/pkg/jsretry"
)

// subKey identifies exactly one subscription document: (roomId, u.account).
type subKey struct {
	roomID  string
	account string
}

// roomLastMsgUpdate is the coalesced per-room last-message state.
//
//   - msgID/at carry the LATEST message observed for the room (max by createdAt).
//   - lastMentionAllAt carries the latest createdAt among @all messages and
//     sticks across later non-@all messages until a newer @all arrives.
type roomLastMsgUpdate struct {
	msgID            string
	at               time.Time
	lastMentionAllAt time.Time
}

// heldMsg is a consumed message awaiting settlement until its batch flushes.
// The context is kept so the settle log carries the message's request id.
type heldMsg struct {
	ctx context.Context
	msg jsretry.Msg
}

// batch accumulates write intents between flushes. The write maps are bounded
// by held, which MaxAckPending bounds: a map entry only ever arrives with a held
// message, and a failing flush swaps the batch out rather than accumulating.
// mentions is the exception — it grows with mentioned accounts per message, so
// it alone can exceed MaxAckPending.
type batch struct {
	rooms    map[string]roomLastMsgUpdate
	lastSeen map[subKey]time.Time
	mentions map[subKey]time.Time
	held     []heldMsg
}

// newBatch sizes the maps and the held slice from the previous batch: under
// steady traffic the last interval is a good predictor, and it stops each flush
// regrowing them from zero.
func newBatch(prev *batch) *batch {
	var nRooms, nSeen, nMentions, nHeld int
	if prev != nil {
		nRooms, nSeen, nMentions, nHeld = len(prev.rooms), len(prev.lastSeen), len(prev.mentions), len(prev.held)
	}
	return &batch{
		rooms:    make(map[string]roomLastMsgUpdate, nRooms),
		lastSeen: make(map[subKey]time.Time, nSeen),
		mentions: make(map[subKey]time.Time, nMentions),
		held:     make([]heldMsg, 0, nHeld),
	}
}

// add merges one message's intents and takes ownership of settling msg. The
// message is held unconditionally: an event that implies no writes (delete,
// react, hidden thread reply) still has to be Acked.
//
//nolint:gocritic // hugeParam: coalescing large writeIntents struct is intentional
func (b *batch) add(in writeIntents, msg heldMsg) {
	b.held = append(b.held, msg)

	if in.LastMsgID != "" {
		cur := b.rooms[in.RoomID]
		if in.LastMsgAt.After(cur.at) {
			cur.msgID = in.LastMsgID
			cur.at = in.LastMsgAt
		}
		if in.LastMentionAllAt.After(cur.lastMentionAllAt) {
			cur.lastMentionAllAt = in.LastMentionAllAt
		}
		b.rooms[in.RoomID] = cur
	}

	if in.SenderAccount != "" {
		k := subKey{roomID: in.RoomID, account: in.SenderAccount}
		if in.SenderSeenAt.After(b.lastSeen[k]) {
			b.lastSeen[k] = in.SenderSeenAt
		}
	}

	for _, account := range in.MentionAccounts {
		k := subKey{roomID: in.RoomID, account: account}
		if in.MentionAt.After(b.mentions[k]) {
			b.mentions[k] = in.MentionAt
		}
	}
}

// empty reports whether the batch has nothing to settle or write.
func (b *batch) empty() bool { return len(b.held) == 0 }
