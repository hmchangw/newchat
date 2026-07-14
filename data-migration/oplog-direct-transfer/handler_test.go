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

// fakeLookup returns a fixed doc (or nil = vanished) for the update re-read.
type fakeLookup struct {
	doc []byte
	err error
}

func (f *fakeLookup) FindByID(_ context.Context, _ string) ([]byte, error) {
	return f.doc, f.err
}

const testColl = "rocketchat_avatar"

func newTestHandler(target targetStore, lk migration.SourceLookup) *handler {
	return &handler{
		collections: map[string]struct{}{testColl: {}},
		lookups:     map[string]migration.SourceLookup{testColl: lk},
		target:      target,
	}
}

func TestHandle_Insert_Upserts(t *testing.T) {
	tgt := &fakeTarget{}
	h := newTestHandler(tgt, &fakeLookup{})
	ev := oplogEvent{
		Op: "insert", Collection: testColl,
		DocumentKey:  json.RawMessage(`{"_id":"a1"}`),
		FullDocument: json.RawMessage(`{"_id":"a1","blob":"x"}`),
	}
	require.NoError(t, h.handle(context.Background(), ev))
	require.Len(t, tgt.upserts, 1)
	assert.Equal(t, testColl, tgt.upserts[0].collection)
	assert.Equal(t, "a1", tgt.upserts[0].id)
	assert.Empty(t, tgt.deletes)
}

func TestHandle_Replace_Upserts(t *testing.T) {
	tgt := &fakeTarget{}
	h := newTestHandler(tgt, &fakeLookup{})
	ev := oplogEvent{
		Op: "replace", Collection: testColl,
		DocumentKey:  json.RawMessage(`{"_id":"a1"}`),
		FullDocument: json.RawMessage(`{"_id":"a1","blob":"y"}`),
	}
	require.NoError(t, h.handle(context.Background(), ev))
	require.Len(t, tgt.upserts, 1)
}

func TestHandle_Update_ReReadsThenUpserts(t *testing.T) {
	tgt := &fakeTarget{}
	h := newTestHandler(tgt, &fakeLookup{doc: []byte(`{"_id":"a1","blob":"fresh"}`)})
	ev := oplogEvent{
		Op: "update", Collection: testColl,
		DocumentKey: json.RawMessage(`{"_id":"a1"}`),
	}
	require.NoError(t, h.handle(context.Background(), ev))
	require.Len(t, tgt.upserts, 1)
	assert.Equal(t, "a1", tgt.upserts[0].id)
}

func TestHandle_Update_DocVanished_Skips(t *testing.T) {
	tgt := &fakeTarget{}
	h := newTestHandler(tgt, &fakeLookup{doc: nil}) // vanished
	ev := oplogEvent{
		Op: "update", Collection: testColl,
		DocumentKey: json.RawMessage(`{"_id":"a1"}`),
	}
	err := h.handle(context.Background(), ev)
	assert.ErrorIs(t, err, migration.ErrSkipped)
	assert.Empty(t, tgt.upserts)
}

func TestHandle_Delete_DeletesByID(t *testing.T) {
	tgt := &fakeTarget{}
	h := newTestHandler(tgt, &fakeLookup{})
	ev := oplogEvent{
		Op: "delete", Collection: testColl,
		DocumentKey: json.RawMessage(`{"_id":"a1"}`),
	}
	require.NoError(t, h.handle(context.Background(), ev))
	require.Len(t, tgt.deletes, 1)
	assert.Equal(t, "a1", tgt.deletes[0].id)
	assert.Empty(t, tgt.upserts)
}

func TestHandle_OtherCollection_Skips(t *testing.T) {
	tgt := &fakeTarget{}
	h := newTestHandler(tgt, &fakeLookup{})
	ev := oplogEvent{Op: "insert", Collection: "not_watched", DocumentKey: json.RawMessage(`{"_id":"a1"}`)}
	err := h.handle(context.Background(), ev)
	assert.ErrorIs(t, err, migration.ErrSkipped)
}

func TestHandle_Insert_NoFullDocument_Poison(t *testing.T) {
	tgt := &fakeTarget{}
	h := newTestHandler(tgt, &fakeLookup{})
	ev := oplogEvent{Op: "insert", Collection: testColl, DocumentKey: json.RawMessage(`{"_id":"a1"}`)}
	err := h.handle(context.Background(), ev)
	assert.ErrorIs(t, err, migration.ErrPoison)
}

func TestHandle_BadDocumentKey_Poison(t *testing.T) {
	tgt := &fakeTarget{}
	h := newTestHandler(tgt, &fakeLookup{})
	ev := oplogEvent{Op: "delete", Collection: testColl, DocumentKey: json.RawMessage(`{}`)}
	err := h.handle(context.Background(), ev)
	assert.ErrorIs(t, err, migration.ErrPoison)
}

func TestHandle_Delete_TargetError_Nak(t *testing.T) {
	tgt := &fakeTarget{deleteErr: errors.New("mongo down")}
	h := newTestHandler(tgt, &fakeLookup{})
	ev := oplogEvent{Op: "delete", Collection: testColl, DocumentKey: json.RawMessage(`{"_id":"a1"}`)}
	err := h.handle(context.Background(), ev)
	require.Error(t, err)
	assert.NotErrorIs(t, err, migration.ErrPoison)
	assert.NotErrorIs(t, err, migration.ErrSkipped)
}

func TestHandle_Update_NonStringID_Poison(t *testing.T) {
	// A non-string _id (ObjectID) can't be re-read via the string SourceLookup. The update must
	// poison loudly (visible Term), not silently mis-skip as "vanished".
	tgt := &fakeTarget{}
	h := newTestHandler(tgt, &fakeLookup{doc: []byte(`{"_id":"whatever"}`)})
	ev := oplogEvent{
		Op: "update", Collection: testColl,
		DocumentKey: json.RawMessage(`{"_id":{"$oid":"5f9b1c2d3e4a5b6c7d8e9f01"}}`),
	}
	err := h.handle(context.Background(), ev)
	assert.ErrorIs(t, err, migration.ErrPoison)
	assert.NotErrorIs(t, err, migration.ErrSkipped)
	assert.Empty(t, tgt.upserts)
}

func TestHandle_DegradedInsert_RecoversViaLookup(t *testing.T) {
	// The connector left fullDocument nil but flagged Degraded — recover the live doc by _id
	// (source re-read) and upsert, rather than poisoning it.
	tgt := &fakeTarget{}
	h := newTestHandler(tgt, &fakeLookup{doc: []byte(`{"_id":"a1","blob":"recovered"}`)})
	ev := oplogEvent{
		Op: "insert", Collection: testColl, Degraded: true, DegradedReason: "unencodable",
		DocumentKey: json.RawMessage(`{"_id":"a1"}`),
	}
	require.NoError(t, h.handle(context.Background(), ev))
	require.Len(t, tgt.upserts, 1)
	assert.Equal(t, "a1", tgt.upserts[0].id)
}

func TestHandle_UnknownOp_Skips(t *testing.T) {
	tgt := &fakeTarget{}
	h := newTestHandler(tgt, &fakeLookup{})
	ev := oplogEvent{Op: "drop", Collection: testColl, DocumentKey: json.RawMessage(`{"_id":"a1"}`)}
	err := h.handle(context.Background(), ev)
	assert.ErrorIs(t, err, migration.ErrSkipped)
	assert.Empty(t, tgt.upserts)
	assert.Empty(t, tgt.deletes)
}
