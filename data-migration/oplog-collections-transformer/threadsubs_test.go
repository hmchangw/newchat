package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/migration"
	"github.com/hmchangw/chat/pkg/model"
)

// threadSubTarget is a targetStore fake wired for thread-sub FK resolution tests.
// It returns configurable results for FindThreadRoom and FindUserID while delegating
// the room-member methods to the embedded fakeTarget so existing tests are unaffected.
type threadSubTarget struct {
	fakeTarget
	// FindThreadRoom return values.
	threadRoomFound bool
	threadRoomErr   error
	roomID          string
	threadRoomID    string
	roomSiteID      string

	// FindUserID return values.
	userFound bool
	userErr   error
	userID    string
}

func (f *threadSubTarget) FindThreadRoom(_ context.Context, _ string) (string, string, string, bool, error) {
	return f.roomID, f.threadRoomID, f.roomSiteID, f.threadRoomFound, f.threadRoomErr
}

func (f *threadSubTarget) FindUserID(_ context.Context, _ string) (string, bool, error) {
	return f.userID, f.userFound, f.userErr
}

// newResolvedTarget builds a threadSubTarget where both FK lookups succeed.
func newResolvedTarget() *threadSubTarget {
	return &threadSubTarget{
		threadRoomFound: true,
		roomID:          "room1",
		threadRoomID:    "thread1",
		roomSiteID:      testSiteID,
		userFound:       true,
		userID:          "user1",
	}
}

// threadSubEv builds an oplogEvent for the company_thread_subscriptions collection.
func threadSubEv(op, doc string) oplogEvent {
	ev := oplogEvent{Op: op, Collection: threadSubColl, EventID: "ts1"}
	if doc != "" {
		ev.FullDocument = json.RawMessage(doc)
	}
	ev.DocumentKey = json.RawMessage(`{"_id":"ts1"}`)
	return ev
}

// A full source thread_sub doc matching SOURCE_DATA.md §5.
const fullThreadSubDoc = `{
	"_id":"ts1",
	"u":{"_id":"u1","username":"alice"},
	"rid":"room1",
	"parentMessage":{"_id":"tmid1"},
	"lastSeenAt":{"$date":"2024-01-15T10:00:00.000Z"},
	"unreadMention":2,
	"createdAt":{"$date":"2024-01-01T00:00:00.000Z"}
}`

// A thread_sub doc with no lastSeenAt (optional field).
const threadSubNoLastSeen = `{
	"_id":"ts2",
	"u":{"_id":"u1","username":"alice"},
	"rid":"room1",
	"parentMessage":{"_id":"tmid1"},
	"unreadMention":0,
	"createdAt":{"$date":"2024-01-01T00:00:00.000Z"}
}`

func TestHandleThreadSub_Insert_BothFKsResolve(t *testing.T) {
	pub := &fakePublisher{}
	target := newResolvedTarget()
	h := newTestHandler(pub, target, &fakeLookup{})

	err := h.handleThreadSub(context.Background(), threadSubEv("insert", fullThreadSubDoc))
	require.NoError(t, err)

	require.Len(t, pub.events, 1)
	evt := pub.events[0]
	assert.Equal(t, model.InboxThreadSubscriptionUpserted, evt.Type)
	assert.Equal(t, testSiteID, evt.SiteID)
	assert.Equal(t, testSiteID, evt.DestSiteID)

	var sub model.ThreadSubscription
	require.NoError(t, json.Unmarshal(evt.Payload, &sub))
	assert.Equal(t, "tmid1", sub.ParentMessageID)
	assert.Equal(t, "room1", sub.RoomID)
	assert.Equal(t, "thread1", sub.ThreadRoomID)
	assert.Equal(t, "user1", sub.UserID)
	assert.Equal(t, "alice", sub.UserAccount)
	assert.Equal(t, testSiteID, sub.SiteID)
	assert.True(t, sub.HasMention, "unreadMention=2 should set HasMention=true")
	require.NotNil(t, sub.LastSeenAt)
	wantLastSeen := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	assert.Equal(t, wantLastSeen, sub.LastSeenAt.UTC())
	wantCreatedAt := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	assert.Equal(t, wantCreatedAt, sub.CreatedAt.UTC())
	// UpdatedAt is the handler's now time (1700000000000 ms in tests).
	assert.Equal(t, time.UnixMilli(1700000000000).UTC(), sub.UpdatedAt.UTC())
	// ID must be non-empty (generated UUIDv7).
	assert.NotEmpty(t, sub.ID)
}

func TestHandleThreadSub_Insert_ThreadRoomMissing_TransientError(t *testing.T) {
	pub := &fakePublisher{}
	target := &threadSubTarget{
		threadRoomFound: false,
		userFound:       true,
		userID:          "user1",
	}
	h := newTestHandler(pub, target, &fakeLookup{})

	err := h.handleThreadSub(context.Background(), threadSubEv("insert", fullThreadSubDoc))
	require.Error(t, err)
	assert.NotErrorIs(t, err, migration.ErrSkipped, "missing thread_room must Nak, not skip")
	assert.NotErrorIs(t, err, migration.ErrPoison, "missing thread_room must Nak, not poison")
	assert.Empty(t, pub.events, "no event published when thread_room missing")
}

func TestHandleThreadSub_Insert_UserMissing_TransientError(t *testing.T) {
	pub := &fakePublisher{}
	target := &threadSubTarget{
		threadRoomFound: true,
		roomID:          "room1",
		threadRoomID:    "thread1",
		roomSiteID:      testSiteID,
		userFound:       false,
	}
	h := newTestHandler(pub, target, &fakeLookup{})

	err := h.handleThreadSub(context.Background(), threadSubEv("insert", fullThreadSubDoc))
	require.Error(t, err)
	assert.NotErrorIs(t, err, migration.ErrSkipped, "missing user must Nak, not skip")
	assert.NotErrorIs(t, err, migration.ErrPoison, "missing user must Nak, not poison")
	assert.Empty(t, pub.events, "no event published when user missing")
}

func TestHandleThreadSub_Update_BothFKsResolve(t *testing.T) {
	pub := &fakePublisher{}
	target := newResolvedTarget()
	// fakeLookup returns the full doc on update re-read.
	h := newTestHandler(pub, target, &fakeLookup{doc: json.RawMessage(fullThreadSubDoc)})

	ev := oplogEvent{
		Op:                "update",
		Collection:        threadSubColl,
		EventID:           "ts1",
		DocumentKey:       json.RawMessage(`{"_id":"ts1"}`),
		UpdateDescription: json.RawMessage(`{"updatedFields":{"lastSeenAt":{"$date":"2024-01-15T10:00:00.000Z"}}}`),
	}
	err := h.handleThreadSub(context.Background(), ev)
	require.NoError(t, err)

	require.Len(t, pub.events, 1)
	assert.Equal(t, model.InboxThreadSubscriptionUpserted, pub.events[0].Type)
	var sub model.ThreadSubscription
	require.NoError(t, json.Unmarshal(pub.events[0].Payload, &sub))
	assert.Equal(t, "thread1", sub.ThreadRoomID)
	assert.Equal(t, "room1", sub.RoomID)
	assert.Equal(t, "user1", sub.UserID)
}

func TestHandleThreadSub_Delete_Skip(t *testing.T) {
	pub := &fakePublisher{}
	h := newTestHandler(pub, newResolvedTarget(), &fakeLookup{})

	err := h.handleThreadSub(context.Background(), threadSubEv("delete", ""))
	assert.ErrorIs(t, err, migration.ErrSkipped)
	assert.Empty(t, pub.events)
}

func TestHandleThreadSub_UnreadMention_HasMention(t *testing.T) {
	tests := []struct {
		name          string
		unreadMention int
		wantMention   bool
	}{
		{"unreadMention=0 → HasMention=false", 0, false},
		{"unreadMention=3 → HasMention=true", 3, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pub := &fakePublisher{}
			target := newResolvedTarget()
			h := newTestHandler(pub, target, &fakeLookup{})

			docJSON, err := json.Marshal(map[string]any{
				"_id":           "ts1",
				"u":             map[string]any{"_id": "u1", "username": "alice"},
				"rid":           "room1",
				"parentMessage": map[string]any{"_id": "tmid1"},
				"unreadMention": tc.unreadMention,
				"createdAt":     map[string]any{"$date": "2024-01-01T00:00:00.000Z"},
			})
			require.NoError(t, err)

			herr := h.handleThreadSub(context.Background(), threadSubEv("insert", string(docJSON)))
			require.NoError(t, herr)
			require.Len(t, pub.events, 1)

			var sub model.ThreadSubscription
			require.NoError(t, json.Unmarshal(pub.events[0].Payload, &sub))
			assert.Equal(t, tc.wantMention, sub.HasMention)
		})
	}
}

func TestHandleThreadSub_NoLastSeenAt_NilInPayload(t *testing.T) {
	pub := &fakePublisher{}
	target := newResolvedTarget()
	h := newTestHandler(pub, target, &fakeLookup{})

	err := h.handleThreadSub(context.Background(), threadSubEv("insert", threadSubNoLastSeen))
	require.NoError(t, err)

	require.Len(t, pub.events, 1)
	var sub model.ThreadSubscription
	require.NoError(t, json.Unmarshal(pub.events[0].Payload, &sub))
	assert.Nil(t, sub.LastSeenAt, "absent lastSeenAt must decode as nil")
	assert.False(t, sub.HasMention, "unreadMention=0 → HasMention=false")
}

func TestHandleThreadSub_Insert_NoFullDocument_Poison(t *testing.T) {
	pub := &fakePublisher{}
	h := newTestHandler(pub, newResolvedTarget(), &fakeLookup{})

	ev := oplogEvent{Op: "insert", Collection: threadSubColl, EventID: "ts1",
		DocumentKey: json.RawMessage(`{"_id":"ts1"}`)}
	err := h.handleThreadSub(context.Background(), ev)
	assert.ErrorIs(t, err, migration.ErrPoison)
	assert.Empty(t, pub.events)
}

// emptyParentThreadSubDoc has a blank parentMessage._id — structurally invalid, can never
// resolve a thread_room, so it must poison rather than Nak-storm to MAX_DELIVER.
const emptyParentThreadSubDoc = `{
	"_id":"ts1",
	"u":{"_id":"u1","username":"alice"},
	"rid":"room1",
	"parentMessage":{"_id":""},
	"unreadMention":0,
	"createdAt":{"$date":"2024-01-01T00:00:00.000Z"}
}`

// emptyAccountThreadSubDoc has a blank u.username — can never resolve a user, must poison.
const emptyAccountThreadSubDoc = `{
	"_id":"ts1",
	"u":{"_id":"u1","username":""},
	"rid":"room1",
	"parentMessage":{"_id":"tmid1"},
	"unreadMention":0,
	"createdAt":{"$date":"2024-01-01T00:00:00.000Z"}
}`

func TestHandleThreadSub_Insert_EmptyParentMessageID_Poison(t *testing.T) {
	pub := &fakePublisher{}
	// A resolving target would otherwise succeed — the guard must trip before any FK lookup.
	h := newTestHandler(pub, newResolvedTarget(), &fakeLookup{})

	err := h.handleThreadSub(context.Background(), threadSubEv("insert", emptyParentThreadSubDoc))
	assert.ErrorIs(t, err, migration.ErrPoison)
	assert.NotErrorIs(t, err, migration.ErrSkipped)
	assert.Empty(t, pub.events)
}

func TestHandleThreadSub_Insert_EmptyAccount_Poison(t *testing.T) {
	pub := &fakePublisher{}
	h := newTestHandler(pub, newResolvedTarget(), &fakeLookup{})

	err := h.handleThreadSub(context.Background(), threadSubEv("insert", emptyAccountThreadSubDoc))
	assert.ErrorIs(t, err, migration.ErrPoison)
	assert.NotErrorIs(t, err, migration.ErrSkipped)
	assert.Empty(t, pub.events)
}

func TestHandleThreadSub_Replace_BothFKsResolve(t *testing.T) {
	pub := &fakePublisher{}
	target := newResolvedTarget()
	h := newTestHandler(pub, target, &fakeLookup{})

	err := h.handleThreadSub(context.Background(), threadSubEv("replace", fullThreadSubDoc))
	require.NoError(t, err)

	require.Len(t, pub.events, 1)
	assert.Equal(t, model.InboxThreadSubscriptionUpserted, pub.events[0].Type)
}
