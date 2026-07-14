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

func userEv(op, doc string) oplogEvent {
	ev := oplogEvent{Op: op, Collection: usersColl, EventID: "e1"}
	if doc != "" {
		ev.FullDocument = json.RawMessage(doc)
	}
	ev.DocumentKey = json.RawMessage(`{"_id":"u1"}`)
	return ev
}

func TestHandleUser_InsertMapsFields(t *testing.T) {
	target := &fakeTarget{inserted: true}
	h := newTestHandler(&fakePublisher{}, target, &fakeLookup{})

	doc := `{"_id":"u1","username":"alice","type":"user","statusText":"hi",` +
		`"roles":["admin","user"],` +
		`"customFields":{"engName":"Alice","companyName":"愛麗絲","deptId":"D1","deptName":"Dept","sectId":"S1","sectName":"Sect"}}`
	err := h.handleUser(context.Background(), userEv("insert", doc))
	require.NoError(t, err)

	require.Len(t, target.upserted, 1)
	u := target.upserted[0]
	assert.Equal(t, "alice", u.Account)
	assert.Equal(t, "Alice", u.EngName)
	assert.Equal(t, "愛麗絲", u.ChineseName)
	assert.Equal(t, "D1", u.DeptID)
	assert.Equal(t, "Dept", u.DeptName)
	assert.Equal(t, "S1", u.SectID)
	assert.Equal(t, "Sect", u.SectName)
	assert.Equal(t, "hi", u.StatusText)
	assert.Equal(t, testSiteID, u.SiteID)
	assert.NotEmpty(t, u.ID)
	require.Len(t, u.Roles, 2)
	assert.Equal(t, model.UserRoleAdmin, u.Roles[0])
	assert.Equal(t, model.UserRoleUser, u.Roles[1])
}

func TestHandleUser_Delete(t *testing.T) {
	target := &fakeTarget{}
	h := newTestHandler(&fakePublisher{}, target, &fakeLookup{})

	err := h.handleUser(context.Background(), userEv("delete", ""))
	assert.ErrorIs(t, err, migration.ErrSkipped)
	assert.Empty(t, target.upserted)
}

func TestHandleUser_FederatedOriginSiteID(t *testing.T) {
	target := &fakeTarget{inserted: true}
	h := newTestHandler(&fakePublisher{}, target, &fakeLookup{})

	doc := `{"_id":"u1","username":"bob","federation":{"origin":"0030204.tchat-test.test.company.com"}}`
	err := h.handleUser(context.Background(), userEv("insert", doc))
	require.NoError(t, err)

	require.Len(t, target.upserted, 1)
	assert.Equal(t, "0030204", target.upserted[0].SiteID)
}

func TestHandleUser_AlreadyPresent(t *testing.T) {
	target := &fakeTarget{inserted: false}
	h := newTestHandler(&fakePublisher{}, target, &fakeLookup{})

	doc := `{"_id":"u1","username":"carol"}`
	err := h.handleUser(context.Background(), userEv("insert", doc))
	require.NoError(t, err)
	require.Len(t, target.upserted, 1)
	assert.Equal(t, "carol", target.upserted[0].Account)
}

func TestHandleUser_StatusTextUpdate_FansToAllSites(t *testing.T) {
	pub := &fakePublisher{}
	doc := `{"_id":"u1","username":"alice","statusText":"in a meeting"}`
	target := &fakeTarget{}
	h := newTestHandler(pub, target, &fakeLookup{doc: []byte(doc)})

	ev := oplogEvent{
		Op:                "update",
		Collection:        usersColl,
		EventID:           "e1",
		DocumentKey:       json.RawMessage(`{"_id":"u1"}`),
		UpdateDescription: json.RawMessage(`{"updatedFields":{"statusText":"in a meeting"}}`),
	}
	require.NoError(t, h.handleUser(context.Background(), ev))

	// An update must never (re-)seed the user — the company-wide sync owns the record post-seed.
	assert.Empty(t, target.upserted, "a statusText update must not upsert the user")

	// statusText is chat-originated and global-visibility: fan to every site in allSiteIDs (s1 incl. self, s2).
	require.Len(t, pub.events, 2)
	var dests []string
	for _, e := range pub.events {
		assert.Equal(t, model.InboxUserStatusUpdated, e.Type)
		assert.Equal(t, testSiteID, e.SiteID)
		dests = append(dests, e.DestSiteID)
		var p model.UserStatusUpdated
		require.NoError(t, json.Unmarshal(e.Payload, &p))
		assert.Equal(t, "alice", p.Account)
		assert.Equal(t, "in a meeting", p.StatusText)
		assert.Nil(t, p.StatusIsShow, "statusIsShow is not sourced — left nil for the user sync to own")
	}
	assert.ElementsMatch(t, []string{testSiteID, "s2"}, dests)
}

func TestHandleUser_StatusTextUpdate_NoSites_WarnsAndSkips(t *testing.T) {
	pub := &fakePublisher{}
	doc := `{"_id":"u1","username":"alice","statusText":"in a meeting"}`
	h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{doc: []byte(doc)})
	h.allSiteIDs = nil // ALL_SITE_IDS empty (misconfig, or a partial deployment without status fan-out)

	ev := oplogEvent{
		Op:                "update",
		Collection:        usersColl,
		EventID:           "e1",
		DocumentKey:       json.RawMessage(`{"_id":"u1"}`),
		UpdateDescription: json.RawMessage(`{"updatedFields":{"statusText":"in a meeting"}}`),
	}
	err := h.handleUser(context.Background(), ev)
	// No destinations: warn + Ack-skip — not a retry-storm error, not a silent drop.
	assert.ErrorIs(t, err, migration.ErrSkipped)
	assert.Empty(t, pub.events)
}

func TestHandleUser_NonStatusUpdate_SkipsWithoutIO(t *testing.T) {
	pub := &fakePublisher{}
	target := &fakeTarget{}
	// fakeLookup errors if consulted — an update with no statusText change must skip BEFORE the
	// source-DB lookup (and before any target upsert), so this error must never surface.
	h := newTestHandler(pub, target, &fakeLookup{err: errors.New("resolveDoc must not run for a non-statusText update")})

	ev := oplogEvent{
		Op:                "update",
		Collection:        usersColl,
		DocumentKey:       json.RawMessage(`{"_id":"u1"}`),
		UpdateDescription: json.RawMessage(`{"updatedFields":{"customFields.deptName":"NewDept"}}`),
	}
	err := h.handleUser(context.Background(), ev)
	assert.ErrorIs(t, err, migration.ErrSkipped, "a non-statusText update is a clean ack-skip")
	assert.Empty(t, pub.events, "an HR-field update must not fan a status event")
	assert.Empty(t, target.upserted, "an HR-field update must not re-seed the user")
}
