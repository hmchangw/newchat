package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roommetacache"
)

// A mixed flush window: lastMsgAt follows the newest message of any kind,
// lastUserMsgAt follows the newest USER message only.
func TestCoalescer_SystemMessageDoesNotAdvanceUserAt(t *testing.T) {
	c := newCoalescingStore(nil, &fakeBulkWriter{})
	t0 := time.Date(2026, 8, 26, 10, 0, 0, 0, time.UTC)

	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), roomLastMessage{
		RoomID: "r1", MsgID: "m-user", At: t0,
	}))
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), roomLastMessage{
		RoomID: "r1", MsgID: "m-system", At: t0.Add(time.Minute), SystemMsg: true,
	}))

	u := c.pending["r1"]
	assert.Equal(t, "m-system", u.msgID, "lastMsgAt key follows the newest message, system included")
	assert.True(t, u.at.Equal(t0.Add(time.Minute)))
	assert.Equal(t, "m-user", u.userMsgID, "user key must ignore the system message")
	assert.True(t, u.userAt.Equal(t0), "userAt pinned to the newest USER message")
}

func TestCoalescer_SystemOnlyWindowLeavesUserAtZero(t *testing.T) {
	c := newCoalescingStore(nil, &fakeBulkWriter{})
	t0 := time.Date(2026, 8, 26, 10, 0, 0, 0, time.UTC)
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), roomLastMessage{
		RoomID: "r1", MsgID: "m-system", At: t0, SystemMsg: true,
	}))
	assert.True(t, c.pending["r1"].userAt.IsZero(), "system-only window must signal the freeze path")
}

// User-message window, previews off: the plain $set now carries lastUserMsgAt
// beside lastMsgAt.
func TestLastMessageUpdate_UserMessageSetsLastUserMsgAt(t *testing.T) {
	m := &mongoStore{previews: false}
	at := time.Date(2026, 8, 26, 10, 0, 0, 0, time.UTC)
	got := m.lastMessageUpdate(&roomLastMsgUpdate{msgID: "m1", at: at, userAt: at, userMsgID: "m1"})
	set, ok := got.(bson.M)
	require.True(t, ok, "a user-message window with previews off stays a plain $set")
	fields := set["$set"].(bson.M)
	assert.Equal(t, at, fields["lastUserMsgAt"])
	assert.Equal(t, at, fields["lastMsgAt"])
}

// System-only window: an existing lastUserMsgAt is kept, and otherwise only a
// room that has never carried a message gets its createdAt pinned as the floor.
// A room that already has a lastMsgAt is left alone — that timestamp is
// unclassified (it may itself be a system message), so promoting it would
// persist a system position as user activity. Requires the pipeline form so the
// expression reads the PRE-update document, previews on or off.
func TestLastMessageUpdate_SystemOnlyWindowFreezes(t *testing.T) {
	want := bson.M{"$ifNull": bson.A{
		"$lastUserMsgAt",
		bson.M{"$cond": bson.A{
			bson.M{"$eq": bson.A{bson.M{"$ifNull": bson.A{"$lastMsgAt", nil}}, nil}},
			bson.M{"$ifNull": bson.A{"$createdAt", "$$REMOVE"}},
			"$$REMOVE",
		}},
	}}
	for _, previews := range []bool{false, true} {
		m := &mongoStore{previews: previews}
		at := time.Date(2026, 8, 26, 10, 0, 0, 0, time.UTC)
		got := m.lastMessageUpdate(&roomLastMsgUpdate{msgID: "m-sys", at: at})
		pipe, ok := got.(mongo.Pipeline)
		require.True(t, ok, "system-only window must use a pipeline $set (previews=%v)", previews)
		fields := pipe[0][0].Value.(bson.M)
		assert.Equal(t, want, fields["lastUserMsgAt"], "previews=%v", previews)
		assert.Equal(t, at, fields["lastMsgAt"], "system message still advances the history ceiling (previews=%v)", previews)
	}
}

// The single RoomEvent constructor stamps the plaintext systemMsg marker, so
// encrypted channels (message sealed, type invisible) can still gate.
func TestBuildRoomEvent_StampsSystemMsg(t *testing.T) {
	meta := roommetacache.Meta{ID: "r1", Name: "room", Type: model.RoomTypeChannel}
	sys := buildClientMessage(&model.Message{ID: "m1", RoomID: "r1", Type: model.MessageTypeMembersAdded}, nil)
	assert.True(t, buildRoomEvent(&meta, sys, 1).SystemMsg)

	user := buildClientMessage(&model.Message{ID: "m2", RoomID: "r1"}, nil)
	assert.False(t, buildRoomEvent(&meta, user, 1).SystemMsg)

	important := buildClientMessage(&model.Message{ID: "m3", RoomID: "r1", Type: model.MessageTypeImportant}, nil)
	assert.False(t, buildRoomEvent(&meta, important, 1).SystemMsg, "important is a client type, not system")
}

// handleCreated must flag the room-doc update for a system message so the
// store takes the freeze path instead of advancing lastUserMsgAt.
func TestHandleCreated_MarksSystemMessageUpdate(t *testing.T) {
	c := newCoalescingStore(nil, &fakeBulkWriter{})
	at := time.Date(2026, 8, 26, 10, 0, 0, 0, time.UTC)
	require.NoError(t, c.UpdateRoomLastMessage(context.Background(), roomLastMessage{
		RoomID: "r1", MsgID: "m-sys", At: at,
		SystemMsg: model.IsSystemMessageType(model.MessageTypeRoomRenamed),
	}))
	assert.True(t, c.pending["r1"].userAt.IsZero())
}
