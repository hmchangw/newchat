package main

import (
	"context"
	"time"

	"github.com/hmchangw/chat/pkg/jsretry"
	"github.com/hmchangw/chat/pkg/msgbucket"
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
	// userAt/userMsgID name the newest NON-system message in the window, under
	// the same NewerRow comparator as msgID/at. A zero userAt means the window
	// carried only system messages, which the write path reads to freeze the
	// stored user position rather than set one.
	userAt    time.Time
	userMsgID string
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

// maxReuseCap bounds what one batch may pre-allocate for the next. Comfortably
// above a steady-state interval at the default MaxAckPending of 1000, so it
// never binds in normal traffic — it exists for mentions, the one map with no
// MaxAckPending bound. Without it a single message carrying thousands of
// @tokens teaches every later batch to reserve that much again, for the life of
// the process.
const maxReuseCap = 4096

// reuseCap clamps a previous batch's size to what the next one may reserve.
func reuseCap(n int) int {
	return min(n, maxReuseCap)
}

// newBatch sizes the maps and the held slice from the previous batch: under
// steady traffic the last interval is a good predictor, and it stops each flush
// regrowing them from zero. Clamped, so one anomalous batch cannot set the
// floor permanently.
func newBatch(prev *batch) *batch {
	var nRooms, nSeen, nMentions, nHeld int
	if prev != nil {
		nRooms, nSeen, nMentions, nHeld = len(prev.rooms), len(prev.lastSeen), len(prev.mentions), len(prev.held)
	}
	return &batch{
		rooms:    make(map[string]roomLastMsgUpdate, reuseCap(nRooms)),
		lastSeen: make(map[subKey]time.Time, reuseCap(nSeen)),
		mentions: make(map[subKey]time.Time, reuseCap(nMentions)),
		held:     make([]heldMsg, 0, reuseCap(nHeld)),
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
		// msgbucket.NewerRow, not LastMsgAt.After: broadcast-worker buffers the
		// room-list preview against the same stream and must agree with this
		// service on which message is the room's newest, or the reader's
		// previewForMsgId == lastMsgId check fails and the preview reads as a
		// miss. A plain time comparison resolves a same-millisecond pair by
		// arrival order, which the two services need not observe alike.
		if msgbucket.NewerRow(in.LastMsgAt, in.LastMsgID, cur.at, cur.msgID) {
			cur.msgID = in.LastMsgID
			cur.at = in.LastMsgAt
		}
		// Same comparator as the pointer, but NOT for the same reason: this
		// service is the only writer of lastUserMsgAt, so no cross-service
		// agreement rides on it. It is here so the two dimensions coalesced in
		// this one function cannot disagree about which row is newer — and
		// because lastUserMsgId is the obvious next field to write, at which
		// point the tie would start to matter.
		if !in.SystemMsg && msgbucket.NewerRow(in.LastMsgAt, in.LastMsgID, cur.userAt, cur.userMsgID) {
			cur.userAt = in.LastMsgAt
			cur.userMsgID = in.LastMsgID
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
