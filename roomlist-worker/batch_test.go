package main

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/flushloop"
)

// fakeMsg is a jsretry.Msg that records how it was settled. jsretry.Msg needs
// all three methods — Metadata feeds the backoff schedule selection.
type fakeMsg struct {
	acked     bool
	naked     bool
	nakDelay  time.Duration
	delivered uint64 // 0 is treated as the first delivery by jsretry.backoffFor
}

func (f *fakeMsg) Ack() error { f.acked = true; return nil }

func (f *fakeMsg) NakWithDelay(d time.Duration) error {
	f.naked = true
	f.nakDelay = d
	return nil
}

func (f *fakeMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{NumDelivered: f.delivered}, nil
}

func held(m *fakeMsg) heldMsg { return heldMsg{ctx: context.Background(), msg: m} }

func TestBatch_CoalescesRoomPointerToLatestMessage(t *testing.T) {
	t1 := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Second)

	b := newBatch(nil)
	b.add(writeIntents{RoomID: "r1", LastMsgID: "m2", LastMsgAt: t2}, held(&fakeMsg{}))
	// Older message arrives after the newer one (out-of-order redelivery).
	b.add(writeIntents{RoomID: "r1", LastMsgID: "m1", LastMsgAt: t1}, held(&fakeMsg{}))

	require.Len(t, b.rooms, 1)
	assert.Equal(t, "m2", b.rooms["r1"].msgID)
	assert.Equal(t, t2, b.rooms["r1"].at)
	assert.Len(t, b.held, 2, "every consumed message must be held for settlement")
}

// Two messages at the same millisecond: created_at cannot order them, so the id has
// to. broadcast-worker coalesces the same pair with the same comparator for the room's
// preview, and history-service serves a stored preview only while previewForMsgId
// equals lastMsgId — so a tie broken by arrival order here would leave the two services
// naming different messages and silently send that room to the Cassandra walk.
func TestBatch_SameMillisecondTieBreaksOnTheMessageID(t *testing.T) {
	at := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)

	for _, order := range [][2]string{{"m-1", "m-2"}, {"m-2", "m-1"}} {
		t.Run(order[0]+" then "+order[1], func(t *testing.T) {
			b := newBatch(nil)
			for _, id := range order {
				b.add(writeIntents{RoomID: "r1", LastMsgID: id, LastMsgAt: at}, held(&fakeMsg{}))
			}
			assert.Equal(t, "m-2", b.rooms["r1"].msgID, "the higher id wins regardless of arrival order")
		})
	}
}

// Sub-millisecond precision is Go's, not Cassandra's: created_at stores milliseconds,
// so two messages inside one are a single clustering position and the id still decides.
func TestBatch_SubMillisecondDifferenceDoesNotOrderTwoMessages(t *testing.T) {
	at := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)

	b := newBatch(nil)
	b.add(writeIntents{RoomID: "r1", LastMsgID: "m-2", LastMsgAt: at}, held(&fakeMsg{}))
	b.add(writeIntents{RoomID: "r1", LastMsgID: "m-1", LastMsgAt: at.Add(400 * time.Microsecond)}, held(&fakeMsg{}))

	assert.Equal(t, "m-2", b.rooms["r1"].msgID,
		"400µs is one Cassandra timestamp, so the lower id must not win on it")
}

func TestBatch_LastMentionAllAtSticksAcrossLaterPlainMessages(t *testing.T) {
	t1 := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Second)

	b := newBatch(nil)
	b.add(writeIntents{RoomID: "r1", LastMsgID: "m1", LastMsgAt: t1, LastMentionAllAt: t1}, held(&fakeMsg{}))
	b.add(writeIntents{RoomID: "r1", LastMsgID: "m2", LastMsgAt: t2}, held(&fakeMsg{}))

	assert.Equal(t, "m2", b.rooms["r1"].msgID)
	assert.Equal(t, t1, b.rooms["r1"].lastMentionAllAt, "a later plain message must not clear lastMentionAllAt")
}

func TestBatch_MentionAndLastSeenKeepLatestTimePerSubscription(t *testing.T) {
	t1 := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Second)

	b := newBatch(nil)
	// First add with newer timestamps.
	b.add(writeIntents{
		RoomID: "r1", LastMsgID: "m1", LastMsgAt: t2,
		SenderAccount: "alice", SenderSeenAt: t2,
		MentionAccounts: []string{"bob"}, MentionAt: t2,
	}, held(&fakeMsg{}))
	// Out-of-order add with older timestamps (redelivery).
	b.add(writeIntents{
		RoomID: "r1", LastMsgID: "m2", LastMsgAt: t1,
		SenderAccount: "alice", SenderSeenAt: t1,
		MentionAccounts: []string{"bob", "carol"}, MentionAt: t1,
	}, held(&fakeMsg{}))

	// Newer timestamps must be retained; older redeliveries must not overwrite.
	assert.Equal(t, t2, b.lastSeen[subKey{"r1", "alice"}], "older SenderSeenAt must not overwrite newer")
	assert.Equal(t, t2, b.mentions[subKey{"r1", "bob"}], "older MentionAt must not overwrite newer")
	// carol is only mentioned in the older message, so it gets that timestamp.
	assert.Equal(t, t1, b.mentions[subKey{"r1", "carol"}])
	assert.Len(t, b.mentions, 2)
}

func TestBatch_HoldsNoOpMessagesForAck(t *testing.T) {
	b := newBatch(nil)
	b.add(writeIntents{}, held(&fakeMsg{}))

	assert.False(t, b.empty(), "a no-op message still needs settling")
	assert.Empty(t, b.rooms)
	assert.Empty(t, b.lastSeen)
	assert.Empty(t, b.mentions)
	assert.Len(t, b.held, 1)
}

func TestBatch_EmptyWhenNothingAdded(t *testing.T) {
	assert.True(t, newBatch(nil).empty())
}

// The held slice is the one reused capacity Go lets a test observe directly,
// so it stands in for the clamp being wired into newBatch at all.
func TestNewBatch_DoesNotInheritAnOutsizedCapacity(t *testing.T) {
	prev := &batch{
		rooms:    make(map[string]roomLastMsgUpdate),
		lastSeen: make(map[subKey]time.Time),
		mentions: make(map[subKey]time.Time),
		held:     make([]heldMsg, flushloop.MaxReuseCap*3),
	}

	got := newBatch(prev)

	assert.Equal(t, flushloop.MaxReuseCap, cap(got.held))
}

// The room pointer follows the newest message of ANY kind; the user position
// follows only the newest non-system one. A system message arriving after a user
// message must move the first and not the second.
func TestBatch_UserPositionIgnoresSystemMessages(t *testing.T) {
	b := newBatch(nil)
	t1 := time.Date(2026, 8, 27, 10, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Second)

	b.add(writeIntents{RoomID: "r1", LastMsgID: "m-user", LastMsgAt: t1}, held(&fakeMsg{}))
	b.add(writeIntents{RoomID: "r1", LastMsgID: "m-sys", LastMsgAt: t2, SystemMsg: true}, held(&fakeMsg{}))

	got := b.rooms["r1"]
	assert.Equal(t, "m-sys", got.msgID, "the pointer follows the newest message of any kind")
	assert.Equal(t, t1, got.userAt, "the user position must stay on the newest NON-system message")
	assert.Equal(t, "m-user", got.userMsgID)
}

// A window carrying only system messages names no user position at all. The zero
// value is what the write path reads to choose the freeze instead of a set.
func TestBatch_SystemOnlyWindowNamesNoUserPosition(t *testing.T) {
	b := newBatch(nil)
	at := time.Date(2026, 8, 27, 10, 0, 0, 0, time.UTC)

	b.add(writeIntents{RoomID: "r1", LastMsgID: "m-sys", LastMsgAt: at, SystemMsg: true}, held(&fakeMsg{}))

	assert.True(t, b.rooms["r1"].userAt.IsZero(),
		"a system-only window must not name a user position; the write freezes what is stored instead")
}
