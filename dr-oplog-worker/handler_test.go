package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/migration"
)

// fakeTarget records upserts/deletes.
type fakeTarget struct {
	upserts   []writeCall
	deletes   []writeCall
	upsertErr error
	deleteErr error
}

type writeCall struct {
	collection string
	id         any
	doc        bson.D
}

func (f *fakeTarget) UpsertByID(_ context.Context, collection string, id any, doc bson.D) error {
	if f.upsertErr != nil {
		return f.upsertErr
	}
	f.upserts = append(f.upserts, writeCall{collection, id, doc})
	return nil
}

func (f *fakeTarget) DeleteByID(_ context.Context, collection string, id any) error {
	if f.deleteErr != nil {
		return f.deleteErr
	}
	f.deletes = append(f.deletes, writeCall{collection: collection, id: id})
	return nil
}

const testColl = "rooms"

func newTestHandler(target targetStore) *handler {
	return &handler{
		collections: map[string]struct{}{testColl: {}},
		target:      target,
	}
}

func TestHandle_Insert_Upserts(t *testing.T) {
	tgt := &fakeTarget{}
	h := newTestHandler(tgt)
	ev := oplogEvent{
		Op: "insert", Collection: testColl,
		DocumentKey:  json.RawMessage(`{"_id":"r1"}`),
		FullDocument: json.RawMessage(`{"_id":"r1","name":"general","crossSite":true}`),
	}
	require.NoError(t, h.handle(context.Background(), ev))
	require.Len(t, tgt.upserts, 1)
	assert.Equal(t, testColl, tgt.upserts[0].collection)
	assert.Equal(t, "r1", tgt.upserts[0].id)
	assert.Empty(t, tgt.deletes)
}

func TestHandle_Replace_Upserts(t *testing.T) {
	tgt := &fakeTarget{}
	h := newTestHandler(tgt)
	ev := oplogEvent{
		Op: "replace", Collection: testColl,
		DocumentKey:  json.RawMessage(`{"_id":"r1"}`),
		FullDocument: json.RawMessage(`{"_id":"r1","name":"renamed"}`),
	}
	require.NoError(t, h.handle(context.Background(), ev))
	require.Len(t, tgt.upserts, 1)
}

// The DR feed uses updateLookup, so update events carry the full post-image inline — the applier
// upserts directly from fullDocument and never reads back to the origin site's Mongo.
func TestHandle_Update_UpsertsFromFullDocument_NoLookup(t *testing.T) {
	tgt := &fakeTarget{}
	h := newTestHandler(tgt)
	ev := oplogEvent{
		Op: "update", Collection: testColl,
		DocumentKey:  json.RawMessage(`{"_id":"r1"}`),
		FullDocument: json.RawMessage(`{"_id":"r1","name":"updated","crossSite":false}`),
	}
	require.NoError(t, h.handle(context.Background(), ev))
	require.Len(t, tgt.upserts, 1)
	assert.Equal(t, "r1", tgt.upserts[0].id)
}

func TestHandle_Delete_DeletesByID(t *testing.T) {
	tgt := &fakeTarget{}
	h := newTestHandler(tgt)
	ev := oplogEvent{
		Op: "delete", Collection: testColl,
		DocumentKey: json.RawMessage(`{"_id":"r1"}`),
	}
	require.NoError(t, h.handle(context.Background(), ev))
	require.Len(t, tgt.deletes, 1)
	assert.Equal(t, "r1", tgt.deletes[0].id)
	assert.Empty(t, tgt.upserts)
}

// A native (non-string) _id — e.g. an ObjectID — round-trips as its native BSON type. The DR
// applier upserts/deletes by native id (no string source-lookup), so this is NOT poison.
func TestHandle_NativeObjectID_Upserts(t *testing.T) {
	tgt := &fakeTarget{}
	h := newTestHandler(tgt)
	ev := oplogEvent{
		Op: "insert", Collection: testColl,
		DocumentKey:  json.RawMessage(`{"_id":{"$oid":"5f9b1c2d3e4a5b6c7d8e9f01"}}`),
		FullDocument: json.RawMessage(`{"_id":{"$oid":"5f9b1c2d3e4a5b6c7d8e9f01"},"name":"x"}`),
	}
	require.NoError(t, h.handle(context.Background(), ev))
	require.Len(t, tgt.upserts, 1)
	assert.IsType(t, bson.ObjectID{}, tgt.upserts[0].id)
}

func TestHandle_OtherCollection_Skips(t *testing.T) {
	tgt := &fakeTarget{}
	h := newTestHandler(tgt)
	ev := oplogEvent{Op: "insert", Collection: "not_watched", DocumentKey: json.RawMessage(`{"_id":"r1"}`)}
	err := h.handle(context.Background(), ev)
	assert.ErrorIs(t, err, migration.ErrSkipped)
}

func TestHandle_UnknownOp_Skips(t *testing.T) {
	tgt := &fakeTarget{}
	h := newTestHandler(tgt)
	ev := oplogEvent{Op: "drop", Collection: testColl, DocumentKey: json.RawMessage(`{"_id":"r1"}`)}
	err := h.handle(context.Background(), ev)
	assert.ErrorIs(t, err, migration.ErrSkipped)
	assert.Empty(t, tgt.upserts)
	assert.Empty(t, tgt.deletes)
}

func TestHandle_BadDocumentKey_Poison(t *testing.T) {
	tgt := &fakeTarget{}
	h := newTestHandler(tgt)
	ev := oplogEvent{Op: "delete", Collection: testColl, DocumentKey: json.RawMessage(`{}`)}
	err := h.handle(context.Background(), ev)
	assert.ErrorIs(t, err, migration.ErrPoison)
}

// Self-contained contract: an insert/replace/update with no fullDocument can never succeed on a
// DR applier (there is no source to recover from), so it poisons rather than Naks forever.
func TestHandle_Insert_NoFullDocument_Poison(t *testing.T) {
	tgt := &fakeTarget{}
	h := newTestHandler(tgt)
	ev := oplogEvent{Op: "insert", Collection: testColl, DocumentKey: json.RawMessage(`{"_id":"r1"}`)}
	err := h.handle(context.Background(), ev)
	assert.ErrorIs(t, err, migration.ErrPoison)
	assert.Empty(t, tgt.upserts)
}

func TestHandle_Update_NoFullDocument_Poison(t *testing.T) {
	tgt := &fakeTarget{}
	h := newTestHandler(tgt)
	ev := oplogEvent{Op: "update", Collection: testColl, DocumentKey: json.RawMessage(`{"_id":"r1"}`)}
	err := h.handle(context.Background(), ev)
	assert.ErrorIs(t, err, migration.ErrPoison)
	assert.Empty(t, tgt.upserts)
}

// An explicit JSON null post-image (a producer that emits "fullDocument":null rather than omitting
// it) is treated the same as absent — poison, never an empty-doc upsert.
func TestHandle_ExplicitNullFullDocument_Poison(t *testing.T) {
	tgt := &fakeTarget{}
	h := newTestHandler(tgt)
	ev := oplogEvent{
		Op: "update", Collection: testColl,
		DocumentKey:  json.RawMessage(`{"_id":"r1"}`),
		FullDocument: json.RawMessage(`null`),
	}
	err := h.handle(context.Background(), ev)
	assert.ErrorIs(t, err, migration.ErrPoison)
	assert.Empty(t, tgt.upserts)
}

func TestHandle_BadFullDocument_Poison(t *testing.T) {
	tgt := &fakeTarget{}
	h := newTestHandler(tgt)
	ev := oplogEvent{
		Op: "insert", Collection: testColl,
		DocumentKey:  json.RawMessage(`{"_id":"r1"}`),
		FullDocument: json.RawMessage(`{not json`),
	}
	err := h.handle(context.Background(), ev)
	assert.ErrorIs(t, err, migration.ErrPoison)
}

func TestHandle_UpsertTargetError_Nak(t *testing.T) {
	tgt := &fakeTarget{upsertErr: errors.New("mongo down")}
	h := newTestHandler(tgt)
	ev := oplogEvent{
		Op: "insert", Collection: testColl,
		DocumentKey:  json.RawMessage(`{"_id":"r1"}`),
		FullDocument: json.RawMessage(`{"_id":"r1","name":"x"}`),
	}
	err := h.handle(context.Background(), ev)
	require.Error(t, err)
	assert.NotErrorIs(t, err, migration.ErrPoison)
	assert.NotErrorIs(t, err, migration.ErrSkipped)
}

func TestHandle_DeleteTargetError_Nak(t *testing.T) {
	tgt := &fakeTarget{deleteErr: errors.New("mongo down")}
	h := newTestHandler(tgt)
	ev := oplogEvent{Op: "delete", Collection: testColl, DocumentKey: json.RawMessage(`{"_id":"r1"}`)}
	err := h.handle(context.Background(), ev)
	require.Error(t, err)
	assert.NotErrorIs(t, err, migration.ErrPoison)
	assert.NotErrorIs(t, err, migration.ErrSkipped)
}
