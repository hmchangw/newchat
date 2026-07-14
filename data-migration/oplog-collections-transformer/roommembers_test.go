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

const rmColl = "company_room_members"

func rmInsertEvent(doc string) oplogEvent {
	return oplogEvent{
		Op: "insert", Collection: rmColl,
		DocumentKey:  json.RawMessage(`{"_id":"src1"}`),
		FullDocument: json.RawMessage(doc),
	}
}

func TestHandleRoomMember_OrgInsert_Upserts(t *testing.T) {
	tgt := &fakeTarget{}
	h := newTestHandler(nil, tgt, nil)
	ev := rmInsertEvent(`{"_id":"src1","rid":"GENERAL","member":{"type":"org","id":"org-9"},"ts":{"$date":"2026-07-01T00:00:00Z"}}`)
	require.NoError(t, h.handle(context.Background(), ev))
	require.Len(t, tgt.roomMemberUpserts, 1)
	rm := tgt.roomMemberUpserts[0]
	assert.Equal(t, "src1", rm.ID) // source _id adopted
	assert.Equal(t, "GENERAL", rm.RoomID)
	assert.Equal(t, model.RoomMemberOrg, rm.Member.Type)
	assert.Equal(t, "org-9", rm.Member.ID)
	assert.Empty(t, rm.Member.Account)
	assert.Equal(t, 2026, rm.Ts.Year())
}

func TestHandleRoomMember_IndividualInsert_ResolvesUser(t *testing.T) {
	tgt := &fakeTarget{userIDs: map[string]string{"jdoe": "newUser42"}}
	h := newTestHandler(nil, tgt, nil)
	ev := rmInsertEvent(`{"_id":"src2","rid":"GENERAL","member":{"type":"individual","id":"legacyU1","username":"jdoe"},"ts":{"$date":"2026-07-02T00:00:00Z"}}`)
	require.NoError(t, h.handle(context.Background(), ev))
	require.Len(t, tgt.roomMemberUpserts, 1)
	rm := tgt.roomMemberUpserts[0]
	assert.Equal(t, model.RoomMemberIndividual, rm.Member.Type)
	assert.Equal(t, "newUser42", rm.Member.ID) // NEW-stack id, not legacyU1
	assert.Equal(t, "jdoe", rm.Member.Account)
}

func TestHandleRoomMember_IndividualUnseededUser_Naks(t *testing.T) {
	tgt := &fakeTarget{userIDs: map[string]string{}} // jdoe not seeded yet
	h := newTestHandler(nil, tgt, nil)
	ev := rmInsertEvent(`{"_id":"src3","rid":"GENERAL","member":{"type":"individual","id":"legacyU1","username":"jdoe"},"ts":{"$date":"2026-07-02T00:00:00Z"}}`)
	err := h.handle(context.Background(), ev)
	require.Error(t, err) // plain error => Nak-retry until seeded
	assert.NotErrorIs(t, err, migration.ErrSkipped)
	assert.NotErrorIs(t, err, migration.ErrPoison)
	assert.Empty(t, tgt.roomMemberUpserts)
}

func TestHandleRoomMember_IndividualEmptyUsername_Poisons(t *testing.T) {
	for name, doc := range map[string]string{
		"empty":  `{"_id":"src6","rid":"GENERAL","member":{"type":"individual","id":"legacyU1","username":""},"ts":{"$date":"2026-07-02T00:00:00Z"}}`,
		"absent": `{"_id":"src7","rid":"GENERAL","member":{"type":"individual","id":"legacyU1"},"ts":{"$date":"2026-07-02T00:00:00Z"}}`,
		"blank":  `{"_id":"src8","rid":"GENERAL","member":{"type":"individual","id":"legacyU1","username":"   "},"ts":{"$date":"2026-07-02T00:00:00Z"}}`,
	} {
		t.Run(name, func(t *testing.T) {
			tgt := &fakeTarget{}
			h := newTestHandler(nil, tgt, nil)
			ev := rmInsertEvent(doc)
			err := h.handle(context.Background(), ev)
			assert.ErrorIs(t, err, migration.ErrPoison)
			assert.Empty(t, tgt.roomMemberUpserts)
		})
	}
}

func TestHandleRoomMember_UnmappedTypes_SkipLoudly(t *testing.T) {
	for _, typ := range []string{"app", "user", "something_new"} {
		t.Run(typ, func(t *testing.T) {
			tgt := &fakeTarget{}
			h := newTestHandler(nil, tgt, nil)
			ev := rmInsertEvent(`{"_id":"src4","rid":"GENERAL","member":{"type":"` + typ + `","id":"x"},"ts":{"$date":"2026-07-02T00:00:00Z"}}`)
			err := h.handle(context.Background(), ev)
			assert.ErrorIs(t, err, migration.ErrSkipped)
			assert.Empty(t, tgt.roomMemberUpserts)
		})
	}
}

func TestHandleRoomMember_Delete_DeletesByID(t *testing.T) {
	tgt := &fakeTarget{}
	h := newTestHandler(nil, tgt, nil)
	ev := oplogEvent{Op: "delete", Collection: rmColl, DocumentKey: json.RawMessage(`{"_id":"src1"}`)}
	require.NoError(t, h.handle(context.Background(), ev))
	assert.Equal(t, []string{"src1"}, tgt.roomMemberDeletes)
}

func TestHandleRoomMember_Update_ReReadsAndUpserts(t *testing.T) {
	// Contract violation per SOURCE_DATA §7 (legacy never updates) — handled defensively.
	tgt := &fakeTarget{}
	lk := &fakeLookup{doc: []byte(`{"_id":"src5","rid":"GENERAL","member":{"type":"org","id":"org-7"},"ts":{"$date":"2026-07-03T00:00:00Z"}}`)}
	h := newTestHandler(nil, tgt, lk)
	ev := oplogEvent{Op: "update", Collection: rmColl, DocumentKey: json.RawMessage(`{"_id":"src5"}`)}
	require.NoError(t, h.handle(context.Background(), ev))
	require.Len(t, tgt.roomMemberUpserts, 1)
	assert.Equal(t, "org-7", tgt.roomMemberUpserts[0].Member.ID)
}

func TestHandleRoomMember_TargetError_Naks(t *testing.T) {
	tgt := &fakeTarget{roomMemberErr: errors.New("mongo down")}
	h := newTestHandler(nil, tgt, nil)
	ev := oplogEvent{Op: "delete", Collection: rmColl, DocumentKey: json.RawMessage(`{"_id":"src1"}`)}
	err := h.handle(context.Background(), ev)
	require.Error(t, err)
	assert.NotErrorIs(t, err, migration.ErrSkipped)
	assert.NotErrorIs(t, err, migration.ErrPoison)
}

func TestHandleRoomMember_UpsertError_Naks(t *testing.T) {
	tgt := &fakeTarget{roomMemberErr: errors.New("mongo down")}
	h := newTestHandler(nil, tgt, nil)
	ev := rmInsertEvent(`{"_id":"src9","rid":"GENERAL","member":{"type":"org","id":"org-9"},"ts":{"$date":"2026-07-01T00:00:00Z"}}`)
	err := h.handle(context.Background(), ev)
	require.Error(t, err)
	assert.NotErrorIs(t, err, migration.ErrSkipped)
	assert.NotErrorIs(t, err, migration.ErrPoison)
	assert.Empty(t, tgt.roomMemberUpserts)
}

func TestHandleRoomMember_FindUserIDError_Naks(t *testing.T) {
	tgt := &fakeTarget{findUserErr: errors.New("mongo down")}
	h := newTestHandler(nil, tgt, nil)
	ev := rmInsertEvent(`{"_id":"src10","rid":"GENERAL","member":{"type":"individual","id":"legacyU1","username":"jdoe"},"ts":{"$date":"2026-07-02T00:00:00Z"}}`)
	err := h.handle(context.Background(), ev)
	require.Error(t, err)
	assert.NotErrorIs(t, err, migration.ErrSkipped)
	assert.NotErrorIs(t, err, migration.ErrPoison)
	assert.Empty(t, tgt.roomMemberUpserts)
}

func TestHandleRoomMember_UnknownOp_Skips(t *testing.T) {
	tgt := &fakeTarget{}
	h := newTestHandler(nil, tgt, nil)
	ev := oplogEvent{Op: "drop", Collection: rmColl, DocumentKey: json.RawMessage(`{"_id":"src1"}`)}
	err := h.handle(context.Background(), ev)
	assert.ErrorIs(t, err, migration.ErrSkipped)
	assert.Empty(t, tgt.roomMemberUpserts)
	assert.Empty(t, tgt.roomMemberDeletes)
}

func TestHandleRoomMember_Update_DocGone_Skips(t *testing.T) {
	tgt := &fakeTarget{}
	lk := &fakeLookup{doc: nil}
	h := newTestHandler(nil, tgt, lk)
	ev := oplogEvent{Op: "update", Collection: rmColl, DocumentKey: json.RawMessage(`{"_id":"src5"}`)}
	err := h.handle(context.Background(), ev)
	assert.ErrorIs(t, err, migration.ErrSkipped)
	assert.Empty(t, tgt.roomMemberUpserts)
	assert.Empty(t, tgt.roomMemberDeletes)
}

func TestHandleRoomMember_Delete_MalformedDocumentKey_Poisons(t *testing.T) {
	tgt := &fakeTarget{}
	h := newTestHandler(nil, tgt, nil)
	ev := oplogEvent{Op: "delete", Collection: rmColl, DocumentKey: json.RawMessage(`{}`)}
	err := h.handle(context.Background(), ev)
	assert.ErrorIs(t, err, migration.ErrPoison)
	assert.Empty(t, tgt.roomMemberDeletes)
}

func TestHandleRoomMember_Delete_Noop(t *testing.T) {
	tgt := &fakeTarget{deleteNoop: true}
	h := newTestHandler(nil, tgt, nil)
	ev := oplogEvent{Op: "delete", Collection: rmColl, DocumentKey: json.RawMessage(`{"_id":"ghost"}`)}
	require.NoError(t, h.handle(context.Background(), ev))
	assert.Equal(t, []string{"ghost"}, tgt.roomMemberDeletes)
}

func TestHandleRoomMember_BlankRequiredFields_Poison(t *testing.T) {
	tests := map[string]string{
		"blank _id":           `{"_id":"","rid":"GENERAL","member":{"type":"org","id":"org-9"},"ts":{"$date":"2026-07-01T00:00:00Z"}}`,
		"blank rid":           `{"_id":"src11","rid":"","member":{"type":"org","id":"org-9"},"ts":{"$date":"2026-07-01T00:00:00Z"}}`,
		"blank org member.id": `{"_id":"src12","rid":"GENERAL","member":{"type":"org","id":""},"ts":{"$date":"2026-07-01T00:00:00Z"}}`,
	}
	for name, doc := range tests {
		t.Run(name, func(t *testing.T) {
			tgt := &fakeTarget{}
			h := newTestHandler(nil, tgt, nil)
			ev := rmInsertEvent(doc)
			err := h.handle(context.Background(), ev)
			assert.ErrorIs(t, err, migration.ErrPoison)
			assert.Empty(t, tgt.roomMemberUpserts)
		})
	}
}

func TestHandleRoomMember_MalformedDoc_Poisons(t *testing.T) {
	tgt := &fakeTarget{}
	h := newTestHandler(nil, tgt, nil)
	ev := rmInsertEvent(`{not json`)
	err := h.handle(context.Background(), ev)
	assert.ErrorIs(t, err, migration.ErrPoison)
}
