package main

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
