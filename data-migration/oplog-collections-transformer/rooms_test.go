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

func roomEv(op string, doc, updateDesc string) oplogEvent {
	ev := oplogEvent{Op: op, Collection: roomsColl, EventID: "e1"}
	if doc != "" {
		ev.FullDocument = json.RawMessage(doc)
	}
	if updateDesc != "" {
		ev.UpdateDescription = json.RawMessage(updateDesc)
	}
	ev.DocumentKey = json.RawMessage(`{"_id":"r1"}`)
	return ev
}

func TestHandleRoom_InsertChannel(t *testing.T) {
	pub := &fakePublisher{}
	h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{})

	doc := `{"_id":"r1","t":"c","fname":"General","name":"general","restricted":false,"uids":["u1","u2"],"usernames":["alice","bob"]}`
	err := h.handleRoom(context.Background(), roomEv("insert", doc, ""))
	require.NoError(t, err)

	require.Len(t, pub.events, 1)
	evt := pub.events[0]
	assert.Equal(t, model.InboxEventType("room_sync"), evt.Type)
	assert.Equal(t, testSiteID, evt.SiteID)
	assert.Equal(t, testSiteID, evt.DestSiteID)

	var room model.Room
	require.NoError(t, json.Unmarshal(evt.Payload, &room))
	assert.Equal(t, "r1", room.ID)
	assert.Equal(t, model.RoomTypeChannel, room.Type)
	assert.Equal(t, "General", room.Name)
	assert.Equal(t, testSiteID, room.SiteID)
	assert.False(t, room.Restricted)
	assert.False(t, room.ExternalAccess)
	assert.Equal(t, []string{"alice", "bob"}, room.Accounts)
}

func TestHandleRoom_InsertDiscussion(t *testing.T) {
	pub := &fakePublisher{}
	h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{})

	doc := `{"_id":"r1","t":"p","prid":"parent1","fname":"Topic","uids":["u1"]}`
	err := h.handleRoom(context.Background(), roomEv("insert", doc, ""))
	require.NoError(t, err)

	require.Len(t, pub.events, 1)
	var room model.Room
	require.NoError(t, json.Unmarshal(pub.events[0].Payload, &room))
	assert.Equal(t, model.RoomTypeDiscussion, room.Type)
}

func TestHandleRoom_InsertLivechatSkipped(t *testing.T) {
	pub := &fakePublisher{}
	h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{})

	doc := `{"_id":"r1","t":"l","fname":"Support"}`
	err := h.handleRoom(context.Background(), roomEv("insert", doc, ""))
	assert.ErrorIs(t, err, migration.ErrSkipped)
	assert.Empty(t, pub.events)
}

func TestHandleRoom_InsertGroupDMSkipped(t *testing.T) {
	pub := &fakePublisher{}
	h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{})

	doc := `{"_id":"r1","t":"d","uids":["u1","u2","u3"]}`
	err := h.handleRoom(context.Background(), roomEv("insert", doc, ""))
	assert.ErrorIs(t, err, migration.ErrSkipped)
	assert.Empty(t, pub.events)
}

func TestHandleRoom_UpdateNameChange(t *testing.T) {
	pub := &fakePublisher{}
	full := `{"_id":"r1","t":"c","fname":"New Name","uids":["u1"]}`
	h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{doc: json.RawMessage(full)})

	err := h.handleRoom(context.Background(), roomEv("update", "", `{"updatedFields":{"fname":"New Name"}}`))
	require.NoError(t, err)

	// name change → [room_renamed, room_sync]
	require.Len(t, pub.events, 2)
	evt := pub.events[0]
	assert.Equal(t, model.InboxRoomRenamed, evt.Type)

	var p model.RoomRenamedInboxPayload
	require.NoError(t, json.Unmarshal(evt.Payload, &p))
	assert.Equal(t, "r1", p.RoomID)
	assert.Equal(t, "New Name", p.NewName)
	// No source _updatedAt in this doc → falls back to h.now() = 1700000000000.
	assert.Equal(t, int64(1700000000000), p.Timestamp)
	assert.Equal(t, model.InboxEventType("room_sync"), pub.events[1].Type)
}

func TestHandleRoom_UpdateRestrictedChange(t *testing.T) {
	pub := &fakePublisher{}
	full := `{"_id":"r1","t":"c","fname":"Room","restricted":true,"uids":["u1"]}`
	h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{doc: json.RawMessage(full)})

	err := h.handleRoom(context.Background(), roomEv("update", "", `{"updatedFields":{"restricted":true}}`))
	require.NoError(t, err)

	// restricted change → [room_restricted, room_sync]
	require.Len(t, pub.events, 2)
	evt := pub.events[0]
	assert.Equal(t, model.InboxRoomRestricted, evt.Type)

	var p model.RoomRestrictedInboxPayload
	require.NoError(t, json.Unmarshal(evt.Payload, &p))
	assert.Equal(t, "r1", p.RoomID)
	assert.True(t, p.Restricted)
	assert.False(t, p.ExternalAccess)
	assert.Empty(t, p.OwnerAccount)
	assert.Equal(t, model.InboxEventType("room_sync"), pub.events[1].Type)
}

func TestHandleRoom_UpdateOtherFieldReSync(t *testing.T) {
	pub := &fakePublisher{}
	full := `{"_id":"r1","t":"c","fname":"Room","uids":["u1","u2"]}`
	h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{doc: json.RawMessage(full)})

	err := h.handleRoom(context.Background(), roomEv("update", "", `{"updatedFields":{"description":"hi"}}`))
	require.NoError(t, err)

	require.Len(t, pub.events, 1)
	assert.Equal(t, model.InboxEventType("room_sync"), pub.events[0].Type)
}

func TestHandleRoom_Delete(t *testing.T) {
	pub := &fakePublisher{}
	h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{})

	err := h.handleRoom(context.Background(), roomEv("delete", "", ""))
	assert.ErrorIs(t, err, migration.ErrSkipped)
	assert.Empty(t, pub.events)
}

func TestHandleRoom_FederatedOriginSiteID(t *testing.T) {
	pub := &fakePublisher{}
	h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{})

	doc := `{"_id":"r1","t":"c","fname":"Remote","federation":{"origin":"0030204.tchat-test.test.company.com"},"uids":["u1"]}`
	err := h.handleRoom(context.Background(), roomEv("insert", doc, ""))
	require.NoError(t, err)

	require.Len(t, pub.events, 1)
	assert.Equal(t, "0030204", pub.events[0].SiteID)

	var room model.Room
	require.NoError(t, json.Unmarshal(pub.events[0].Payload, &room))
	assert.Equal(t, "0030204", room.SiteID)
}

// Bug 1 tests — room_sync UpdatedAt/CreatedAt stamping.

// TestHandleRoom_InsertUpdatedAtFromSource asserts that a room_sync payload carries
// UpdatedAt equal to the source _updatedAt field and is never the zero time.
func TestHandleRoom_InsertUpdatedAtFromSource(t *testing.T) {
	pub := &fakePublisher{}
	h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{})

	// Source _updatedAt in relaxed extended JSON ($date).
	sourceUpdatedAt := time.Date(2024, 3, 15, 10, 0, 0, 0, time.UTC)
	sourceCreatedAt := time.Date(2024, 1, 5, 8, 30, 0, 0, time.UTC)
	doc := `{"_id":"r1","t":"c","fname":"General","uids":["u1"],` +
		`"_updatedAt":{"$date":"2024-03-15T10:00:00.000Z"},` +
		`"ts":{"$date":"2024-01-05T08:30:00.000Z"}}`

	err := h.handleRoom(context.Background(), roomEv("insert", doc, ""))
	require.NoError(t, err)

	require.Len(t, pub.events, 1)
	var room model.Room
	require.NoError(t, json.Unmarshal(pub.events[0].Payload, &room))

	assert.False(t, room.UpdatedAt.IsZero(), "UpdatedAt must not be zero")
	assert.Equal(t, sourceUpdatedAt, room.UpdatedAt.UTC())
	assert.Equal(t, sourceCreatedAt, room.CreatedAt.UTC())
}

// TestHandleRoom_InsertMissingUpdatedAtFallsBackToNow asserts that when a source doc
// has no _updatedAt, the room_sync carries the now-fallback (non-zero) timestamp.
func TestHandleRoom_InsertMissingUpdatedAtFallsBackToNow(t *testing.T) {
	pub := &fakePublisher{}
	h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{})

	// Doc deliberately missing _updatedAt and ts.
	doc := `{"_id":"r1","t":"c","fname":"General","uids":["u1"]}`

	err := h.handleRoom(context.Background(), roomEv("insert", doc, ""))
	require.NoError(t, err)

	require.Len(t, pub.events, 1)
	var room model.Room
	require.NoError(t, json.Unmarshal(pub.events[0].Payload, &room))

	assert.False(t, room.UpdatedAt.IsZero(), "UpdatedAt must not be zero when source field is absent")
	// The handler's now() returns 1700000000000 ms — verify the fallback matches.
	assert.Equal(t, time.UnixMilli(1700000000000).UTC(), room.UpdatedAt.UTC())
	assert.Equal(t, time.UnixMilli(1700000000000).UTC(), room.CreatedAt.UTC())
}

// Bug 2 tests — dual-event emission on rename/restrict updates.

// TestHandleRoom_UpdateNameChangeEmitsBothEvents asserts that a name/fname change
// publishes room_renamed (for subs) AND room_sync (for the room doc), in that order.
func TestHandleRoom_UpdateNameChangeEmitsBothEvents(t *testing.T) {
	pub := &fakePublisher{}
	// Source doc has _updatedAt so the room_sync carries the source timestamp.
	full := `{"_id":"r1","t":"c","fname":"New Name","uids":["u1"],"_updatedAt":{"$date":"2024-03-15T10:00:00.000Z"}}`
	h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{doc: json.RawMessage(full)})

	err := h.handleRoom(context.Background(), roomEv("update", "", `{"updatedFields":{"fname":"New Name"}}`))
	require.NoError(t, err)

	require.Len(t, pub.events, 2, "update with name change must publish room_renamed AND room_sync")

	// First event: room_renamed for subscriptions.
	assert.Equal(t, model.InboxRoomRenamed, pub.events[0].Type)
	var rp model.RoomRenamedInboxPayload
	require.NoError(t, json.Unmarshal(pub.events[0].Payload, &rp))
	assert.Equal(t, "r1", rp.RoomID)
	assert.Equal(t, "New Name", rp.NewName)
	// Timestamp uses source _updatedAt millis (zero-guarded).
	sourceMillis := time.Date(2024, 3, 15, 10, 0, 0, 0, time.UTC).UnixMilli()
	assert.Equal(t, sourceMillis, rp.Timestamp)

	// Second event: room_sync for the room doc, carrying the new name.
	assert.Equal(t, model.InboxEventType("room_sync"), pub.events[1].Type)
	var room model.Room
	require.NoError(t, json.Unmarshal(pub.events[1].Payload, &room))
	assert.Equal(t, "r1", room.ID)
	assert.Equal(t, "New Name", room.Name)
	assert.False(t, room.UpdatedAt.IsZero(), "room_sync UpdatedAt must not be zero")
	assert.Equal(t, time.Date(2024, 3, 15, 10, 0, 0, 0, time.UTC), room.UpdatedAt.UTC())
}

// TestHandleRoom_UpdateRestrictedChangeEmitsBothEvents asserts that a ro change
// publishes room_restricted (for subs) AND room_sync (for the room doc).
func TestHandleRoom_UpdateRestrictedChangeEmitsBothEvents(t *testing.T) {
	pub := &fakePublisher{}
	full := `{"_id":"r1","t":"c","fname":"Room","restricted":true,"uids":["u1"],"_updatedAt":{"$date":"2024-05-01T12:00:00.000Z"}}`
	h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{doc: json.RawMessage(full)})

	err := h.handleRoom(context.Background(), roomEv("update", "", `{"updatedFields":{"restricted":true}}`))
	require.NoError(t, err)

	require.Len(t, pub.events, 2, "update with ro change must publish room_restricted AND room_sync")

	// First event: room_restricted for subscriptions.
	assert.Equal(t, model.InboxRoomRestricted, pub.events[0].Type)
	var rp model.RoomRestrictedInboxPayload
	require.NoError(t, json.Unmarshal(pub.events[0].Payload, &rp))
	assert.Equal(t, "r1", rp.RoomID)
	assert.True(t, rp.Restricted)
	sourceMillis := time.Date(2024, 5, 1, 12, 0, 0, 0, time.UTC).UnixMilli()
	assert.Equal(t, sourceMillis, rp.Timestamp)

	// Second event: room_sync.
	assert.Equal(t, model.InboxEventType("room_sync"), pub.events[1].Type)
	var room model.Room
	require.NoError(t, json.Unmarshal(pub.events[1].Payload, &room))
	assert.Equal(t, "r1", room.ID)
	assert.True(t, room.Restricted)
}

// TestHandleRoom_UpdateOtherFieldEmitsOnlyRoomSync asserts that an unrelated field
// change still emits exactly one room_sync event.
func TestHandleRoom_UpdateOtherFieldEmitsOnlyRoomSync(t *testing.T) {
	pub := &fakePublisher{}
	full := `{"_id":"r1","t":"c","fname":"Room","uids":["u1","u2"]}`
	h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{doc: json.RawMessage(full)})

	err := h.handleRoom(context.Background(), roomEv("update", "", `{"updatedFields":{"description":"hi"}}`))
	require.NoError(t, err)

	require.Len(t, pub.events, 1, "unrelated field update must emit exactly one room_sync")
	assert.Equal(t, model.InboxEventType("room_sync"), pub.events[0].Type)
}

func TestHandleRoom_UpdateNameAndRo_EmitsRenamedRestrictedAndSync(t *testing.T) {
	pub := &fakePublisher{}
	full := `{"_id":"r1","t":"c","fname":"New","restricted":true,"uids":["u1"],"_updatedAt":{"$date":"2024-05-01T12:00:00.000Z"}}`
	h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{doc: json.RawMessage(full)})

	err := h.handleRoom(context.Background(), roomEv("update", "", `{"updatedFields":{"fname":"New","restricted":true}}`))
	require.NoError(t, err)

	require.Len(t, pub.events, 3, "a combined name+ro update must emit room_renamed, room_restricted, and room_sync")
	types := []model.InboxEventType{pub.events[0].Type, pub.events[1].Type, pub.events[2].Type}
	assert.Contains(t, types, model.InboxRoomRenamed)
	assert.Contains(t, types, model.InboxRoomRestricted)
	assert.Contains(t, types, model.InboxEventType("room_sync"))
}

func TestHandleRoom_Replace_EmitsAllFieldEventsConservatively(t *testing.T) {
	pub := &fakePublisher{}
	h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{})

	// replace carries the full doc but NO updateDescription delta — there is no way to know which
	// fields changed, so all field-level events must be emitted conservatively (they're idempotent
	// and guarded on the destination), or a rename/visibility change inside a whole-doc replace
	// would update the rooms doc (room_sync) while every subscription kept the stale
	// denormalized name/visibility forever.
	doc := `{"_id":"r1","t":"c","fname":"Replaced","name":"replaced","restricted":true,"uids":["u1"],"_updatedAt":{"$date":"2024-05-01T12:00:00.000Z"}}`
	require.NoError(t, h.handleRoom(context.Background(), roomEv("replace", doc, "")))

	require.Len(t, pub.events, 3, "replace must emit room_renamed, room_restricted, and room_sync")
	types := []model.InboxEventType{pub.events[0].Type, pub.events[1].Type, pub.events[2].Type}
	assert.Contains(t, types, model.InboxRoomRenamed)
	assert.Contains(t, types, model.InboxRoomRestricted)
	assert.Contains(t, types, model.InboxEventType("room_sync"))

	byType := eventsByType(pub.events)
	var renamed model.RoomRenamedInboxPayload
	require.NoError(t, json.Unmarshal(byType[model.InboxRoomRenamed].Payload, &renamed))
	assert.Equal(t, "Replaced", renamed.NewName)
	assert.Equal(t, int64(1714564800000), renamed.Timestamp, "guard timestamp is the source _updatedAt millis")

	var restricted model.RoomRestrictedInboxPayload
	require.NoError(t, json.Unmarshal(byType[model.InboxRoomRestricted].Payload, &restricted))
	assert.True(t, restricted.Restricted)
	assert.Equal(t, int64(1714564800000), restricted.Timestamp)
}

func TestHandleRoom_DegradedInsertRecoversViaSourceLookup(t *testing.T) {
	pub := &fakePublisher{}
	full := `{"_id":"r1","t":"c","fname":"Recovered","uids":["u1"]}`
	h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{doc: json.RawMessage(full)})

	// Degraded insert: the connector couldn't encode fullDocument (empty) but flagged Degraded.
	ev := oplogEvent{Op: "insert", Collection: roomsColl, EventID: "e1",
		DocumentKey: json.RawMessage(`{"_id":"r1"}`), Degraded: true, DegradedReason: "fullDocument encode failed"}
	require.NoError(t, h.handleRoom(context.Background(), ev))

	require.Len(t, pub.events, 1)
	var room model.Room
	require.NoError(t, json.Unmarshal(pub.events[0].Payload, &room))
	assert.Equal(t, "Recovered", room.Name)
}

func TestHandleRoom_NonDegradedInsertWithoutFullDocument_Poisons(t *testing.T) {
	h := newTestHandler(&fakePublisher{}, &fakeTarget{}, &fakeLookup{})
	ev := oplogEvent{Op: "insert", Collection: roomsColl, DocumentKey: json.RawMessage(`{"_id":"r1"}`)}
	assert.ErrorIs(t, h.handleRoom(context.Background(), ev), migration.ErrPoison)
}
