package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/migration"
	"github.com/hmchangw/chat/pkg/model"
)

type recordPublisher struct{ inserts []model.Message }

//nolint:gocritic // value param required to satisfy the inserter interface.
func (r *recordPublisher) publishInsert(_ context.Context, m model.Message) error {
	r.inserts = append(r.inserts, m)
	return nil
}

type recordHistory struct {
	edits   []model.MigrationEditRequest
	deletes []model.MigrationDeleteRequest
}

//nolint:gocritic // value param required to satisfy the historyClient interface.
func (r *recordHistory) Edit(_ context.Context, req model.MigrationEditRequest) error {
	r.edits = append(r.edits, req)
	return nil
}

//nolint:gocritic // value param required to satisfy the historyClient interface.
func (r *recordHistory) Delete(_ context.Context, req model.MigrationDeleteRequest) error {
	r.deletes = append(r.deletes, req)
	return nil
}

type fakeLookup map[string][]byte

func (f fakeLookup) FindByID(_ context.Context, id string) ([]byte, error) { return f[id], nil }

func newTestHandler(pub inserter, hist historyClient, look sourceLookup) *handler {
	return &handler{collection: "rocketchat_message", softDeleteType: "rm", publisher: pub, history: hist, lookup: look}
}

func TestHandle_Insert(t *testing.T) {
	pub := &recordPublisher{}
	h := newTestHandler(pub, &recordHistory{}, fakeLookup{})
	require.NoError(t, h.handle(context.Background(), oplogEvent{Collection: "rocketchat_message", Op: "insert", FullDocument: loadDoc(t, "insert.json")}))
	require.Len(t, pub.inserts, 1)
	assert.Equal(t, "abc123def456ghi78", pub.inserts[0].ID)
}

func TestHandle_UpdateEdit(t *testing.T) {
	hist := &recordHistory{}
	look := fakeLookup{"abc123def456ghi78": loadDoc(t, "edit.json")}
	h := newTestHandler(&recordPublisher{}, hist, look)
	require.NoError(t, h.handle(context.Background(), oplogEvent{Collection: "rocketchat_message", Op: "update", DocumentKey: []byte(`{"_id":"abc123def456ghi78"}`)}))
	require.Len(t, hist.edits, 1)
	assert.Equal(t, "edited text", hist.edits[0].Content)
	// messageId is the event's documentKey._id (the resolved key), not re-read from the doc.
	assert.Equal(t, "abc123def456ghi78", hist.edits[0].MessageID)
}

func TestHandle_UpdateSoftDelete(t *testing.T) {
	hist := &recordHistory{}
	look := fakeLookup{"abc123def456ghi78": loadDoc(t, "softdelete.json")}
	h := newTestHandler(&recordPublisher{}, hist, look)
	require.NoError(t, h.handle(context.Background(), oplogEvent{Collection: "rocketchat_message", Op: "update", DocumentKey: []byte(`{"_id":"abc123def456ghi78"}`), ClusterTime: 1700000000000}))
	require.Len(t, hist.deletes, 1)
	// Soft-delete is id-only: MessageID set, DeletedAt non-zero; no room/createdAt locator.
	assert.Equal(t, "abc123def456ghi78", hist.deletes[0].MessageID)
	assert.False(t, hist.deletes[0].DeletedAt.IsZero())
}

func TestHandle_ReplaceUsesEventDoc(t *testing.T) {
	hist := &recordHistory{}
	h := newTestHandler(&recordPublisher{}, hist, fakeLookup{}) // empty lookup -> must use event doc
	require.NoError(t, h.handle(context.Background(), oplogEvent{Collection: "rocketchat_message", Op: "replace", DocumentKey: []byte(`{"_id":"abc123def456ghi78"}`), FullDocument: loadDoc(t, "edit.json")}))
	require.Len(t, hist.edits, 1)
	assert.Equal(t, "abc123def456ghi78", hist.edits[0].MessageID)
}

func TestHandle_NonDegradedReplaceNilDocumentKeyPoison(t *testing.T) {
	h := newTestHandler(&recordPublisher{}, &recordHistory{}, fakeLookup{})
	err := h.handle(context.Background(), oplogEvent{
		Collection:   "rocketchat_message",
		Op:           "replace",
		FullDocument: loadDoc(t, "edit.json"),
		// no DocumentKey on a non-degraded event = contract violation = poison.
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, migration.ErrPoison), "a non-degraded replace without a documentKey is poison")
}

func TestHandle_DeleteOpRoutesByID(t *testing.T) {
	hist := &recordHistory{}
	// Empty lookup proves no source lookup is consulted for a hard delete.
	h := newTestHandler(&recordPublisher{}, hist, fakeLookup{})
	require.NoError(t, h.handle(context.Background(), oplogEvent{Collection: "rocketchat_message", Op: "delete", DocumentKey: []byte(`{"_id":"abc123def456ghi78"}`), ClusterTime: 1700000000000}))
	require.Len(t, hist.deletes, 1)
	assert.Equal(t, "abc123def456ghi78", hist.deletes[0].MessageID)
	assert.False(t, hist.deletes[0].DeletedAt.IsZero())
}

func TestDeleteTime(t *testing.T) {
	got := deleteTime(1700000000000)
	assert.Equal(t, time.UnixMilli(1700000000000).UTC(), got, "positive clusterTime maps verbatim")

	before := time.Now().UTC()
	fallback := deleteTime(0)
	assert.False(t, fallback.IsZero(), "clusterTime 0 must not produce a 1970 epoch")
	assert.GreaterOrEqual(t, fallback.UnixMilli(), before.UnixMilli(), "non-positive clusterTime falls back to publish-time")
	assert.False(t, deleteTime(-5).IsZero(), "negative clusterTime falls back too")
}

func TestHandle_InsertThreadReplyLinksParent(t *testing.T) {
	pub := &recordPublisher{}
	// The reply's tmid points at the parent; the lookup returns the parent doc (insert.json).
	look := fakeLookup{"abc123def456ghi78": loadDoc(t, "insert.json")}
	h := newTestHandler(pub, &recordHistory{}, look)
	require.NoError(t, h.handle(context.Background(), oplogEvent{Collection: "rocketchat_message", Op: "insert", FullDocument: loadDoc(t, "threadreply.json")}))
	require.Len(t, pub.inserts, 1)
	assert.Equal(t, "abc123def456ghi78", pub.inserts[0].ThreadParentMessageID)
	require.NotNil(t, pub.inserts[0].ThreadParentMessageCreatedAt, "thread reply must carry the parent's createdAt so message-worker can link the thread")
	assert.Equal(t, time.Date(2023, 1, 2, 3, 4, 5, 0, time.UTC), pub.inserts[0].ThreadParentMessageCreatedAt.UTC())
}

func TestHandle_InsertThreadReplyParentMissingBestEffort(t *testing.T) {
	pub := &recordPublisher{}
	h := newTestHandler(pub, &recordHistory{}, fakeLookup{}) // empty lookup -> parent not found
	require.NoError(t, h.handle(context.Background(), oplogEvent{Collection: "rocketchat_message", Op: "insert", FullDocument: loadDoc(t, "threadreply.json")}))
	require.Len(t, pub.inserts, 1)
	assert.Nil(t, pub.inserts[0].ThreadParentMessageCreatedAt, "parent gone from source -> publish best-effort without the link, not an error")
}

func TestHandle_InsertThreadReplyParentCorruptBestEffort(t *testing.T) {
	pub := &recordPublisher{}
	// Parent doc is present but undecodable → publish the reply best-effort without the link.
	look := fakeLookup{"abc123def456ghi78": []byte(`{"ts":"not-a-date"}`)}
	h := newTestHandler(pub, &recordHistory{}, look)
	require.NoError(t, h.handle(context.Background(), oplogEvent{Collection: "rocketchat_message", Op: "insert", FullDocument: loadDoc(t, "threadreply.json")}))
	require.Len(t, pub.inserts, 1)
	assert.Nil(t, pub.inserts[0].ThreadParentMessageCreatedAt, "corrupt parent -> best-effort, no link, no error")
}

func TestHandle_InsertThreadReplyParentLookupErrorNaks(t *testing.T) {
	h := newTestHandler(&recordPublisher{}, &recordHistory{}, errLookup{err: errors.New("source down")})
	err := h.handle(context.Background(), oplogEvent{Collection: "rocketchat_message", Op: "insert", FullDocument: loadDoc(t, "threadreply.json")})
	require.Error(t, err)
	assert.False(t, errors.Is(err, migration.ErrPoison), "a transient parent-lookup error must Nak (retry), not Term")
}

func TestHandle_InsertForeignOriginSkipped(t *testing.T) {
	pub := &recordPublisher{}
	h := newTestHandler(pub, &recordHistory{}, fakeLookup{})
	err := h.handle(context.Background(), oplogEvent{Collection: "rocketchat_message", Op: "insert", FullDocument: loadDoc(t, "foreign.json")})
	require.ErrorIs(t, err, migration.ErrSkipped, "a deliberate skip returns migration.ErrSkipped (Acked, not counted as processed)")
	assert.Empty(t, pub.inserts, "a foreign-origin insert must not be published (defense-in-depth behind the connector $match)")
}

func TestHandle_UpdateForeignOriginSkipped(t *testing.T) {
	hist := &recordHistory{}
	// The lookup returns a foreign-origin doc; the transformer must skip it without any history call.
	look := fakeLookup{"fgn456abc789def01": loadDoc(t, "foreign.json")}
	h := newTestHandler(&recordPublisher{}, hist, look)
	err := h.handle(context.Background(), oplogEvent{Collection: "rocketchat_message", Op: "update", DocumentKey: []byte(`{"_id":"fgn456abc789def01"}`)})
	require.ErrorIs(t, err, migration.ErrSkipped)
	assert.Empty(t, hist.edits, "foreign-origin update must not edit")
	assert.Empty(t, hist.deletes, "foreign-origin update must not delete")
}

func TestHandle_UnknownCollectionSkipped(t *testing.T) {
	h := newTestHandler(&recordPublisher{}, &recordHistory{}, fakeLookup{})
	err := h.handle(context.Background(), oplogEvent{Collection: "users", Op: "insert", FullDocument: []byte(`{}`)})
	require.ErrorIs(t, err, migration.ErrSkipped, "a non-message collection is skipped (Acked, not counted)")
}

func TestHandle_InsertSystemMessageSkipped(t *testing.T) {
	pub := &recordPublisher{}
	h := newTestHandler(pub, &recordHistory{}, fakeLookup{})
	err := h.handle(context.Background(), oplogEvent{Collection: "rocketchat_message", Op: "insert", FullDocument: loadDoc(t, "system.json")})
	require.ErrorIs(t, err, migration.ErrSkipped)
	assert.Empty(t, pub.inserts, "system messages (t set) must not be published as inserts")
}

func TestHandle_UpdateSystemMessageSkipped(t *testing.T) {
	hist := &recordHistory{}
	look := fakeLookup{"sysMsg00000000001": loadDoc(t, "system.json")}
	h := newTestHandler(&recordPublisher{}, hist, look)
	err := h.handle(context.Background(), oplogEvent{Collection: "rocketchat_message", Op: "update", DocumentKey: []byte(`{"_id":"sysMsg00000000001"}`), ClusterTime: 1700000000000})
	require.ErrorIs(t, err, migration.ErrSkipped)
	assert.Empty(t, hist.edits, "system message update must not edit")
	assert.Empty(t, hist.deletes, "system message update must not delete")
}

func TestHandle_LookupMissSkipped(t *testing.T) {
	h := newTestHandler(&recordPublisher{}, &recordHistory{}, fakeLookup{})
	err := h.handle(context.Background(), oplogEvent{Collection: "rocketchat_message", Op: "update", DocumentKey: []byte(`{"_id":"gone"}`)})
	require.ErrorIs(t, err, migration.ErrSkipped, "update lookup miss is ack-skipped (the doc is gone, nothing to apply)")
}

// errLookup is a sourceLookup that always returns an error (a transient source failure).
type errLookup struct{ err error }

func (e errLookup) FindByID(_ context.Context, _ string) ([]byte, error) { return nil, e.err }

func TestHandle_DegradedInsertRecovered(t *testing.T) {
	pub := &recordPublisher{}
	look := fakeLookup{"abc123def456ghi78": loadDoc(t, "insert.json")}
	h := newTestHandler(pub, &recordHistory{}, look)
	err := h.handle(context.Background(), oplogEvent{
		Collection:     "rocketchat_message",
		Op:             "insert",
		FullDocument:   nil,
		Degraded:       true,
		DegradedReason: "fullDocument: too large",
		DocumentKey:    []byte(`{"_id":"abc123def456ghi78"}`),
	})
	require.NoError(t, err)
	require.Len(t, pub.inserts, 1)
	assert.Equal(t, "abc123def456ghi78", pub.inserts[0].ID)
}

func TestHandle_DegradedInsertLookupMissNaks(t *testing.T) {
	pub := &recordPublisher{}
	h := newTestHandler(pub, &recordHistory{}, fakeLookup{}) // empty lookup -> miss
	err := h.handle(context.Background(), oplogEvent{
		Collection:   "rocketchat_message",
		Op:           "insert",
		FullDocument: nil,
		Degraded:     true,
		DocumentKey:  []byte(`{"_id":"abc123def456ghi78"}`),
	})
	require.Error(t, err)
	assert.False(t, errors.Is(err, migration.ErrPoison), "a degraded-insert lookup miss must Nak (retry), not Term")
	assert.Empty(t, pub.inserts)
}

func TestHandle_DegradedInsertLookupErrorNaks(t *testing.T) {
	h := newTestHandler(&recordPublisher{}, &recordHistory{}, errLookup{err: errors.New("source down")})
	err := h.handle(context.Background(), oplogEvent{
		Collection:   "rocketchat_message",
		Op:           "insert",
		FullDocument: nil,
		Degraded:     true,
		DocumentKey:  []byte(`{"_id":"abc123def456ghi78"}`),
	})
	require.Error(t, err)
	assert.False(t, errors.Is(err, migration.ErrPoison), "a degraded-insert lookup error must Nak (retry), not Term")
}

func TestHandle_DegradedInsertNilDocumentKeyNaks(t *testing.T) {
	h := newTestHandler(&recordPublisher{}, &recordHistory{}, fakeLookup{})
	err := h.handle(context.Background(), oplogEvent{
		Collection:   "rocketchat_message",
		Op:           "insert",
		FullDocument: nil,
		Degraded:     true,
		DocumentKey:  nil,
	})
	require.Error(t, err)
	assert.False(t, errors.Is(err, migration.ErrPoison), "a degraded event with nil documentKey must Nak, not Term")
}

func TestHandle_NonDegradedInsertEmptyDocPoison(t *testing.T) {
	h := newTestHandler(&recordPublisher{}, &recordHistory{}, fakeLookup{})
	err := h.handle(context.Background(), oplogEvent{
		Collection:   "rocketchat_message",
		Op:           "insert",
		FullDocument: nil,
		Degraded:     false,
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, migration.ErrPoison), "a non-degraded insert without fullDocument is a contract violation = poison")
}

func TestHandle_DegradedUpdateNilDocumentKeyNaks(t *testing.T) {
	h := newTestHandler(&recordPublisher{}, &recordHistory{}, fakeLookup{})
	err := h.handle(context.Background(), oplogEvent{
		Collection:  "rocketchat_message",
		Op:          "update",
		Degraded:    true,
		DocumentKey: nil,
	})
	require.Error(t, err)
	assert.False(t, errors.Is(err, migration.ErrPoison), "a degraded update with nil documentKey must Nak, not Term")
}

func TestHandle_NonDegradedUpdateBadDocumentKeyPoison(t *testing.T) {
	h := newTestHandler(&recordPublisher{}, &recordHistory{}, fakeLookup{})
	err := h.handle(context.Background(), oplogEvent{
		Collection:  "rocketchat_message",
		Op:          "update",
		DocumentKey: []byte(`{bad`),
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, migration.ErrPoison), "a malformed documentKey on a non-degraded event is poison")
}

func TestHandle_DegradedReplaceRecovered(t *testing.T) {
	hist := &recordHistory{}
	look := fakeLookup{"abc123def456ghi78": loadDoc(t, "edit.json")}
	h := newTestHandler(&recordPublisher{}, hist, look)
	err := h.handle(context.Background(), oplogEvent{
		Collection:   "rocketchat_message",
		Op:           "replace",
		FullDocument: nil,
		Degraded:     true,
		DocumentKey:  []byte(`{"_id":"abc123def456ghi78"}`),
	})
	require.NoError(t, err)
	require.Len(t, hist.edits, 1)
	assert.Equal(t, "edited text", hist.edits[0].Content)
}

func TestHandle_DegradedReplaceLookupMissNaks(t *testing.T) {
	h := newTestHandler(&recordPublisher{}, &recordHistory{}, fakeLookup{}) // empty lookup -> miss
	err := h.handle(context.Background(), oplogEvent{
		Collection:   "rocketchat_message",
		Op:           "replace",
		FullDocument: nil,
		Degraded:     true,
		DocumentKey:  []byte(`{"_id":"abc123def456ghi78"}`),
	})
	require.Error(t, err)
	assert.False(t, errors.Is(err, migration.ErrPoison), "a degraded-replace lookup miss must Nak (retry), not Term")
}

func TestHandle_NonDegradedReplaceEmptyDocPoison(t *testing.T) {
	h := newTestHandler(&recordPublisher{}, &recordHistory{}, fakeLookup{})
	err := h.handle(context.Background(), oplogEvent{
		Collection:   "rocketchat_message",
		Op:           "replace",
		FullDocument: nil,
		Degraded:     false,
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, migration.ErrPoison), "a non-degraded replace without fullDocument is poison")
}

func TestHandle_UnknownOpSkipped(t *testing.T) {
	h := newTestHandler(&recordPublisher{}, &recordHistory{}, fakeLookup{})
	err := h.handle(context.Background(), oplogEvent{
		Collection: "rocketchat_message",
		Op:         "rename", // not one of insert/update/replace/delete
	})
	require.ErrorIs(t, err, migration.ErrSkipped, "an unknown op is skipped (Acked, not counted)")
}

func TestHandle_UpdateLookupErrorNaks(t *testing.T) {
	h := newTestHandler(&recordPublisher{}, &recordHistory{}, errLookup{err: errors.New("source down")})
	err := h.handle(context.Background(), oplogEvent{
		Collection:  "rocketchat_message",
		Op:          "update",
		DocumentKey: []byte(`{"_id":"abc123def456ghi78"}`),
	})
	require.Error(t, err)
	assert.False(t, errors.Is(err, migration.ErrPoison), "a transient source lookup error must Nak (retry), not Term")
}

func TestHandle_UpdateMalformedDocPoison(t *testing.T) {
	// The looked-up doc is present but un-decodable (present-but-corrupt) → poison/Term.
	look := fakeLookup{"abc123def456ghi78": []byte(`{"ts":"not-a-date"}`)}
	h := newTestHandler(&recordPublisher{}, &recordHistory{}, look)
	err := h.handle(context.Background(), oplogEvent{
		Collection:  "rocketchat_message",
		Op:          "update",
		DocumentKey: []byte(`{"_id":"abc123def456ghi78"}`),
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, migration.ErrPoison), "a present-but-corrupt looked-up doc is poison")
}

func TestHandle_DeleteNilDocumentKeyDegradedNaks(t *testing.T) {
	h := newTestHandler(&recordPublisher{}, &recordHistory{}, fakeLookup{})
	err := h.handle(context.Background(), oplogEvent{
		Collection:  "rocketchat_message",
		Op:          "delete",
		Degraded:    true,
		DocumentKey: nil,
	})
	require.Error(t, err)
	assert.False(t, errors.Is(err, migration.ErrPoison), "a degraded delete with nil documentKey must Nak, not Term")
}

func TestHandle_DeleteBadDocumentKeyPoison(t *testing.T) {
	h := newTestHandler(&recordPublisher{}, &recordHistory{}, fakeLookup{})
	err := h.handle(context.Background(), oplogEvent{
		Collection:  "rocketchat_message",
		Op:          "delete",
		DocumentKey: []byte(`{bad`),
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, migration.ErrPoison), "a malformed documentKey on a non-degraded delete is poison")
}
