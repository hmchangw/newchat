package main

import (
	"context"
	"encoding/json"
	"strings"
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

// TestHandleRoom_SoftDeletedSkipped: a room already carrying the "Del-" soft-delete rename is never
// imported. The events that can be carrying the deletion itself — the rename update and any replace
// — are the exception, applied by the two tests below.
func TestHandleRoom_SoftDeletedSkipped(t *testing.T) {
	tests := []struct {
		name string
		doc  string
	}{
		{"fname carries the prefix", `{"_id":"r1","t":"c","name":"general","fname":"Del-General","uids":["u1"]}`},
		{"name carries the prefix", `{"_id":"r1","t":"c","name":"Del-general","fname":"General","uids":["u1"]}`},
		{"both carry the prefix", `{"_id":"r1","t":"c","name":"Del-general","fname":"Del-General","uids":["u1"]}`},
		{"discussion", `{"_id":"r1","t":"p","prid":"p1","fname":"Del-Topic","uids":["u1"]}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("insert", func(t *testing.T) {
				pub := &fakePublisher{}
				h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{})
				err := h.handleRoom(context.Background(), roomEv("insert", tc.doc, ""))
				assert.ErrorIs(t, err, migration.ErrSkipped)
				assert.Empty(t, pub.events)
			})
			// An update that leaves the name alone is churn on an already-dead room.
			for _, delta := range []string{`{"updatedFields":{"restricted":true}}`, `{"updatedFields":{"description":"hi"}}`} {
				t.Run("update "+delta, func(t *testing.T) {
					pub := &fakePublisher{}
					h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{doc: json.RawMessage(tc.doc)})
					err := h.handleRoom(context.Background(), roomEv("update", "", delta))
					assert.ErrorIs(t, err, migration.ErrSkipped)
					assert.Empty(t, pub.events)
				})
			}
		})
	}
}

// TestHandleRoom_SoftDeleteRenameApplied: the rename INTO "Del-" is the source's only delete
// signal — apply it, so the destination room takes the prefixed name and user-service's ^Del-
// filter hides it. Skipping it would leave a deleted room visible to every migrated member.
func TestHandleRoom_SoftDeleteRenameApplied(t *testing.T) {
	const deleted = `{"_id":"r1","t":"c","name":"Del-general","fname":"Del-General","uids":["u1"],"_updatedAt":{"$date":"2024-02-01T00:00:00.000Z"}}`

	for _, delta := range []string{`{"updatedFields":{"fname":"Del-General"}}`, `{"updatedFields":{"name":"Del-general"}}`} {
		pub := &fakePublisher{}
		h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{doc: json.RawMessage(deleted)})
		require.NoError(t, h.handleRoom(context.Background(), roomEv("update", "", delta)), "delta %s", delta)

		byType := eventsByType(pub.events)
		require.Contains(t, byType, model.InboxRoomRenamed, "delta %s", delta)
		require.Contains(t, byType, model.InboxEventType("room_sync"), "delta %s", delta)

		var renamed model.RoomRenamedInboxPayload
		require.NoError(t, json.Unmarshal(byType[model.InboxRoomRenamed].Payload, &renamed))
		assert.Equal(t, "Del-General", renamed.NewName, "the subs' denormalized name must carry the prefix too")

		var room model.Room
		require.NoError(t, json.Unmarshal(byType[model.InboxEventType("room_sync")].Payload, &room))
		assert.Equal(t, "Del-General", room.Name, "user-service filters on the rooms doc name")
	}
}

// TestHandleRoom_NonDeletedNameKept: only the exact "Del-" prefix on a non-DM room marks a soft
// delete. A DM's name fields hold the peer's username/display name, not a room name.
func TestHandleRoom_NonDeletedNameKept(t *testing.T) {
	tests := []struct {
		name string
		doc  string
	}{
		{"prefix without hyphen", `{"_id":"r1","t":"c","name":"delta","fname":"Delta","uids":["u1"]}`},
		{"lowercase del-", `{"_id":"r1","t":"c","name":"del-general","fname":"del-General","uids":["u1"]}`},
		{"prefix mid-name", `{"_id":"r1","t":"c","name":"team-Del-old","fname":"Team Del-old","uids":["u1"]}`},
		{"dm with a Del- peer name", `{"_id":"r1","t":"d","name":"Del-bob","fname":"Del-Bob","uids":["u1","u2"]}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pub := &fakePublisher{}
			h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{})
			require.NoError(t, h.handleRoom(context.Background(), roomEv("insert", tc.doc, "")))
			assert.Len(t, pub.events, 1)
		})
	}
}

// TestHandleRoom_MalformedUpdateDescriptionPoisons: the soft-delete guard needs the delta, so an
// undecodable one on a migratable room poisons instead of being silently treated as "no rename".
func TestHandleRoom_MalformedUpdateDescriptionPoisons(t *testing.T) {
	full := `{"_id":"r1","t":"c","name":"Del-general","fname":"Del-General","uids":["u1"]}`
	h := newTestHandler(&fakePublisher{}, &fakeTarget{}, &fakeLookup{doc: json.RawMessage(full)})
	err := h.handleRoom(context.Background(), roomEv("update", "", `{"updatedFields":`))
	assert.ErrorIs(t, err, migration.ErrPoison)
}

// TestHandleRoom_DeletionAppliedCarriesPrefixedName: the destination hides a room by matching
// ^Del- on the rooms doc name, and the mapped name prefers fname — so when the source renamed only
// the machine name, the mapped name must still carry the marker or the deletion is a no-op.
func TestHandleRoom_DeletionAppliedCarriesPrefixedName(t *testing.T) {
	tests := []struct {
		name     string
		doc      string
		delta    string
		wantName string
	}{
		{
			name:     "both fields prefixed",
			doc:      `{"_id":"r1","t":"c","name":"Del-general","fname":"Del-General","uids":["u1"]}`,
			delta:    `{"updatedFields":{"fname":"Del-General"}}`,
			wantName: "Del-General",
		},
		{
			name:     "only the machine name prefixed",
			doc:      `{"_id":"r1","t":"c","name":"Del-general","fname":"General","uids":["u1"]}`,
			delta:    `{"updatedFields":{"name":"Del-general"}}`,
			wantName: "Del-General",
		},
		{
			name:     "only the machine name prefixed, no fname at all",
			doc:      `{"_id":"r1","t":"c","name":"Del-general","uids":["u1"]}`,
			delta:    `{"updatedFields":{"name":"Del-general"}}`,
			wantName: "Del-general",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pub := &fakePublisher{}
			h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{doc: json.RawMessage(tc.doc)})
			require.NoError(t, h.handleRoom(context.Background(), roomEv("update", "", tc.delta)))

			byType := eventsByType(pub.events)
			var room model.Room
			require.NoError(t, json.Unmarshal(byType[model.InboxEventType("room_sync")].Payload, &room))
			assert.Equal(t, tc.wantName, room.Name, "user-service filters ^Del- on the rooms doc name")
			assert.True(t, strings.HasPrefix(room.Name, "Del-"), "a deleted room must be hidden downstream")

			var renamed model.RoomRenamedInboxPayload
			require.NoError(t, json.Unmarshal(byType[model.InboxRoomRenamed].Payload, &renamed))
			assert.Equal(t, tc.wantName, renamed.NewName, "subs' denormalized name must match the room doc")
		})
	}
}

// TestHandleRoom_SoftDeletedReplaceApplied: a replace carries no delta, so the deletion can't be
// recognised from the event — apply it rather than drop it. Leaving a deleted room visible is worse
// than an extra hidden room doc, and this is what the pre-guard code already did.
func TestHandleRoom_SoftDeletedReplaceApplied(t *testing.T) {
	doc := `{"_id":"r1","t":"c","name":"Del-general","fname":"Del-General","uids":["u1"]}`
	pub := &fakePublisher{}
	h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{})
	require.NoError(t, h.handleRoom(context.Background(), roomEv("replace", doc, "")))

	byType := eventsByType(pub.events)
	require.Contains(t, byType, model.InboxRoomRenamed)
	var room model.Room
	require.NoError(t, json.Unmarshal(byType[model.InboxEventType("room_sync")].Payload, &room))
	assert.Equal(t, "Del-General", room.Name)
}

// TestHandleRoom_ExcludedTypeWinsOverSoftDelete: exclusion is classified before the soft-delete
// guard, so a Del- livechat/voip room keeps metering its type reason and a malformed delta on one
// still Skips rather than Terming.
func TestHandleRoom_ExcludedTypeWinsOverSoftDelete(t *testing.T) {
	t.Run("livechat insert", func(t *testing.T) {
		h := newTestHandler(&fakePublisher{}, &fakeTarget{}, &fakeLookup{})
		doc := `{"_id":"r1","t":"l","name":"Del-support","fname":"Del-Support"}`
		assert.ErrorIs(t, h.handleRoom(context.Background(), roomEv("insert", doc, "")), migration.ErrSkipped)
	})

	t.Run("malformed delta on an excluded type skips, not poisons", func(t *testing.T) {
		doc := `{"_id":"r1","t":"v","name":"Del-call"}`
		h := newTestHandler(&fakePublisher{}, &fakeTarget{}, &fakeLookup{doc: json.RawMessage(doc)})
		err := h.handleRoom(context.Background(), roomEv("update", "", `{"updatedFields":`))
		assert.ErrorIs(t, err, migration.ErrSkipped)
		assert.NotErrorIs(t, err, migration.ErrPoison)
	})
}

// TestHandleRoom_DeletionAppliedOnRemovedNameField: a delta can REMOVE a name field as well as set
// it, and a removal changes the mapped name too — so it counts as the deletion carrier.
func TestHandleRoom_DeletionAppliedOnRemovedNameField(t *testing.T) {
	doc := `{"_id":"r1","t":"c","name":"Del-general","uids":["u1"]}`
	pub := &fakePublisher{}
	h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{doc: json.RawMessage(doc)})
	require.NoError(t, h.handleRoom(context.Background(), roomEv("update", "", `{"removedFields":["fname"]}`)))

	byType := eventsByType(pub.events)
	require.Contains(t, byType, model.InboxRoomRenamed)
	var room model.Room
	require.NoError(t, json.Unmarshal(byType[model.InboxEventType("room_sync")].Payload, &room))
	assert.Equal(t, "Del-general", room.Name)
}
