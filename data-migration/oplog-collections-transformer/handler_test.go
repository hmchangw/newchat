package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/migration"
	"github.com/hmchangw/chat/pkg/model"
)

// fakePublisher captures the InboxEvents the handler publishes.
type fakePublisher struct {
	events []model.InboxEvent
	err    error
}

//nolint:gocritic // signature pinned by the inboxPublisher interface.
func (f *fakePublisher) Publish(_ context.Context, evt model.InboxEvent) error {
	if f.err != nil {
		return f.err
	}
	f.events = append(f.events, evt)
	return nil
}

// fakeLookup returns a canned doc for FindByID (used by the update path).
type fakeLookup struct {
	doc []byte
	err error
}

func (f *fakeLookup) FindByID(_ context.Context, _ string) ([]byte, error) {
	return f.doc, f.err
}

// fakeTarget records room-member writes and answers the FK lookups.
type fakeTarget struct {
	roomMemberUpserts []model.RoomMember
	roomMemberDeletes []string
	roomMemberErr     error
	// deleteNoop makes DeleteRoomMember report deleted=false (row already absent), like a delete
	// for an id that was never mapped/upserted.
	deleteNoop bool

	// userIDs maps account -> new-stack user id for FindUserID resolution; absent key ⇒ not found.
	userIDs map[string]string
	// findUserErr, when set, makes FindUserID return it instead of consulting userIDs.
	findUserErr error
}

func (f *fakeTarget) FindThreadRoom(_ context.Context, _ string) (string, string, string, bool, error) {
	return "", "", "", false, nil
}

func (f *fakeTarget) FindUserID(_ context.Context, account string) (string, bool, error) {
	if f.findUserErr != nil {
		return "", false, f.findUserErr
	}
	id, found := f.userIDs[account]
	return id, found, nil
}

//nolint:gocritic // signature pinned by the targetStore interface.
func (f *fakeTarget) UpsertRoomMember(_ context.Context, rm model.RoomMember) error {
	if f.roomMemberErr != nil {
		return f.roomMemberErr
	}
	f.roomMemberUpserts = append(f.roomMemberUpserts, rm)
	return nil
}

func (f *fakeTarget) DeleteRoomMember(_ context.Context, id string) (bool, error) {
	if f.roomMemberErr != nil {
		return false, f.roomMemberErr
	}
	f.roomMemberDeletes = append(f.roomMemberDeletes, id)
	return !f.deleteNoop, nil
}

const (
	testSiteID    = "s1"
	roomsColl     = "rocketchat_rooms"
	subsColl      = "rocketchat_subscriptions"
	threadSubColl = "company_thread_subscriptions"
)

func newTestHandler(pub inboxPublisher, target targetStore, lookup migration.SourceLookup) *handler {
	return &handler{
		siteID:          testSiteID,
		roomsColl:       roomsColl,
		subsColl:        subsColl,
		threadSubsColl:  threadSubColl,
		roomMembersColl: rmColl,
		pub:             pub,
		target:          target,
		lookups: map[string]migration.SourceLookup{
			roomsColl:     lookup,
			subsColl:      lookup,
			threadSubColl: lookup,
			rmColl:        lookup,
			"":            lookup, // bare resolveDoc tests that don't set Collection
		},
		now: func() int64 { return 1700000000000 },
	}
}

func TestSiteIDFromOrigin(t *testing.T) {
	tests := []struct {
		name   string
		origin string
		want   string
	}{
		{"absent → deployment site", "", testSiteID},
		{"literal local → deployment site", "local", testSiteID},
		{"federated domain → first label", "0030204.tchat-test.test.company.com", "0030204"},
		{"no dot → whole origin", "0030204", "0030204"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, siteIDFromOrigin(tc.origin, testSiteID))
		})
	}
}

func TestHandle_Dispatch(t *testing.T) {
	t.Run("subscriptions collection routes to handleSubscription", func(t *testing.T) {
		pub := &fakePublisher{}
		h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{})
		doc := `{"_id":"sub1","u":{"_id":"u1","username":"alice"},"rid":"r1","t":"c","fname":"General","open":true}`
		err := h.handle(context.Background(), subEv("insert", doc, ""))
		require.NoError(t, err)
		assert.NotEmpty(t, pub.events)
	})

	t.Run("thread-subs routes to handleThreadSub (poison is branch-specific)", func(t *testing.T) {
		h := newTestHandler(&fakePublisher{}, &fakeTarget{}, &fakeLookup{})
		// Empty parentMessage._id poisons ONLY on the thread-sub branch; the default-skip would
		// return ErrSkipped, so ErrPoison proves the event actually reached handleThreadSub.
		err := h.handle(context.Background(), oplogEvent{Op: "insert", Collection: threadSubColl,
			FullDocument: json.RawMessage(`{"_id":"ts1","u":{"username":"alice"},"parentMessage":{"_id":""}}`)})
		assert.ErrorIs(t, err, migration.ErrPoison)
	})

	t.Run("unknown collection skipped", func(t *testing.T) {
		h := newTestHandler(&fakePublisher{}, &fakeTarget{}, &fakeLookup{})
		err := h.handle(context.Background(), oplogEvent{Op: "insert", Collection: "other"})
		assert.ErrorIs(t, err, migration.ErrSkipped)
	})

	t.Run("rooms collection routes to handleRoom", func(t *testing.T) {
		pub := &fakePublisher{}
		h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{})
		doc := `{"_id":"r1","t":"c","fname":"General","uids":["u1"]}`
		err := h.handle(context.Background(), roomEv("insert", doc, ""))
		require.NoError(t, err)
		assert.Len(t, pub.events, 1)
	})

	t.Run("users collection is no longer routed — skipped", func(t *testing.T) {
		h := newTestHandler(&fakePublisher{}, &fakeTarget{}, &fakeLookup{})
		err := h.handle(context.Background(), oplogEvent{Op: "insert", Collection: "users",
			FullDocument: json.RawMessage(`{"_id":"u1","username":"alice"}`)})
		assert.ErrorIs(t, err, migration.ErrSkipped)
	})
}

func TestHandleRoom_PublishError(t *testing.T) {
	pub := &fakePublisher{err: errors.New("inbox down")}
	h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{})
	doc := `{"_id":"r1","t":"c","fname":"General","uids":["u1"]}`
	err := h.handleRoom(context.Background(), roomEv("insert", doc, ""))
	require.Error(t, err)
	assert.NotErrorIs(t, err, migration.ErrSkipped)
	assert.NotErrorIs(t, err, migration.ErrPoison, "a transient publish failure must Nak, not poison")
}

func TestResolveDoc(t *testing.T) {
	full := json.RawMessage(`{"_id":"r1"}`)

	t.Run("insert carries full doc inline", func(t *testing.T) {
		h := newTestHandler(&fakePublisher{}, &fakeTarget{}, &fakeLookup{})
		doc, skip, err := h.resolveDoc(context.Background(), oplogEvent{Op: "insert", FullDocument: full})
		require.NoError(t, err)
		assert.False(t, skip)
		assert.JSONEq(t, string(full), string(doc))
	})

	t.Run("insert without fullDocument is poison", func(t *testing.T) {
		h := newTestHandler(&fakePublisher{}, &fakeTarget{}, &fakeLookup{})
		_, _, err := h.resolveDoc(context.Background(), oplogEvent{Op: "insert"})
		assert.ErrorIs(t, err, migration.ErrPoison)
	})

	t.Run("update re-reads via lookup", func(t *testing.T) {
		h := newTestHandler(&fakePublisher{}, &fakeTarget{}, &fakeLookup{doc: full})
		doc, skip, err := h.resolveDoc(context.Background(), oplogEvent{
			Op: "update", DocumentKey: json.RawMessage(`{"_id":"r1"}`),
		})
		require.NoError(t, err)
		assert.False(t, skip)
		assert.JSONEq(t, string(full), string(doc))
	})

	t.Run("update lookup miss → skip", func(t *testing.T) {
		h := newTestHandler(&fakePublisher{}, &fakeTarget{}, &fakeLookup{doc: nil})
		_, skip, err := h.resolveDoc(context.Background(), oplogEvent{
			Op: "update", DocumentKey: json.RawMessage(`{"_id":"r1"}`),
		})
		require.NoError(t, err)
		assert.True(t, skip)
	})

	t.Run("update lookup error → transient (Nak)", func(t *testing.T) {
		h := newTestHandler(&fakePublisher{}, &fakeTarget{}, &fakeLookup{err: errors.New("source down")})
		_, _, err := h.resolveDoc(context.Background(), oplogEvent{
			Op: "update", DocumentKey: json.RawMessage(`{"_id":"r1"}`),
		})
		require.Error(t, err)
		assert.NotErrorIs(t, err, migration.ErrPoison)
		assert.NotErrorIs(t, err, migration.ErrSkipped)
	})

	t.Run("update for collection with no source lookup → poison", func(t *testing.T) {
		h := newTestHandler(&fakePublisher{}, &fakeTarget{}, &fakeLookup{doc: full})
		// A collection the lookups map doesn't know about (filter subjects/map disagree).
		_, _, err := h.resolveDoc(context.Background(), oplogEvent{
			Op: "update", Collection: "unwatched", DocumentKey: json.RawMessage(`{"_id":"r1"}`),
		})
		assert.ErrorIs(t, err, migration.ErrPoison)
	})

	t.Run("update with bad documentKey → poison", func(t *testing.T) {
		h := newTestHandler(&fakePublisher{}, &fakeTarget{}, &fakeLookup{})
		_, _, err := h.resolveDoc(context.Background(), oplogEvent{
			Op: "update", DocumentKey: json.RawMessage(`{}`),
		})
		assert.ErrorIs(t, err, migration.ErrPoison)
	})

	t.Run("delete → skip", func(t *testing.T) {
		h := newTestHandler(&fakePublisher{}, &fakeTarget{}, &fakeLookup{})
		_, skip, err := h.resolveDoc(context.Background(), oplogEvent{Op: "delete"})
		require.NoError(t, err)
		assert.True(t, skip)
	})
}
