package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/migration"
)

// TestNoDelPrefixEverReachesMongo is the invariant this guard exists for: a room or subscription
// carrying the "Del-" soft-delete marker must never appear in the destination Mongo. Every name the
// transformer can write comes from a published event — room_sync (rooms.name), room_renamed
// (subscriptions.name for the whole room), member_added (the sub's name) — so it is enough to prove
// no published payload ever contains the marker.
func TestNoDelPrefixEverReachesMongo(t *testing.T) {
	roomDocs := []struct {
		name string
		doc  string
	}{
		{"channel, both names prefixed", `{"_id":"r1","t":"c","name":"Del-general","fname":"Del-General","uids":["u1"]}`},
		{"channel, only fname prefixed", `{"_id":"r1","t":"c","name":"general","fname":"Del-General","uids":["u1"]}`},
		{"channel, only machine name prefixed", `{"_id":"r1","t":"c","name":"Del-general","fname":"General","uids":["u1"]}`},
		{"channel, no fname at all", `{"_id":"r1","t":"c","name":"Del-general","uids":["u1"]}`},
		{"private group", `{"_id":"r1","t":"p","name":"Del-secret","fname":"Del-Secret","uids":["u1"]}`},
		{"discussion", `{"_id":"r1","t":"p","prid":"p1","name":"Del-topic","fname":"Del-Topic","uids":["u1"]}`},
		{"dm", `{"_id":"r1","t":"d","name":"Del-bob","fname":"Del-Bob","uids":["u1","u2"]}`},
	}
	subDocs := []struct {
		name string
		doc  string
	}{
		{"channel sub, both names prefixed", `{"_id":"s1","u":{"_id":"u1","username":"alice"},"rid":"r1","t":"c","name":"Del-general","fname":"Del-General","open":true}`},
		{"channel sub, only fname prefixed", `{"_id":"s1","u":{"_id":"u1","username":"alice"},"rid":"r1","t":"c","name":"general","fname":"Del-General","open":true}`},
		{"channel sub, only name prefixed", `{"_id":"s1","u":{"_id":"u1","username":"alice"},"rid":"r1","t":"c","name":"Del-general","fname":"General","open":true}`},
		{"dm sub", `{"_id":"s1","u":{"_id":"u1","username":"alice"},"rid":"r1","t":"d","name":"Del-bob","fname":"Del-Bob","open":true}`},
	}
	// Every op, and for updates every delta shape — including the rename that carries the deletion
	// and a name removal, which are the paths most likely to be re-opened by a future change.
	deltas := []string{
		`{"updatedFields":{"fname":"Del-General"}}`,
		`{"updatedFields":{"name":"Del-general"}}`,
		`{"updatedFields":{"restricted":true}}`,
		`{"updatedFields":{"open":false}}`,
		`{"updatedFields":{"roles":["owner"]}}`,
		`{"removedFields":["fname"]}`,
	}

	assertNothingPublished := func(t *testing.T, pub *fakePublisher, err error) {
		t.Helper()
		assert.ErrorIs(t, err, migration.ErrSkipped, "a Del- record must be skipped")
		for _, e := range pub.events {
			assert.NotContains(t, string(e.Payload), "Del-",
				"published %s carries the soft-delete marker into Mongo", e.Type)
		}
		assert.Empty(t, pub.events, "a Del- record must publish nothing at all")
	}

	for _, tc := range roomDocs {
		t.Run("room/"+tc.name, func(t *testing.T) {
			for _, op := range []string{"insert", "replace"} {
				t.Run(op, func(t *testing.T) {
					pub := &fakePublisher{}
					h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{})
					assertNothingPublished(t, pub, h.handleRoom(context.Background(), roomEv(op, tc.doc, "")))
				})
			}
			for _, delta := range deltas {
				t.Run("update "+delta, func(t *testing.T) {
					pub := &fakePublisher{}
					h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{doc: json.RawMessage(tc.doc)})
					assertNothingPublished(t, pub, h.handleRoom(context.Background(), roomEv("update", "", delta)))
				})
			}
		})
	}

	for _, tc := range subDocs {
		t.Run("subscription/"+tc.name, func(t *testing.T) {
			for _, op := range []string{"insert", "replace"} {
				t.Run(op, func(t *testing.T) {
					pub := &fakePublisher{}
					h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{})
					assertNothingPublished(t, pub, h.handleSubscription(context.Background(), subEv(op, tc.doc, "")))
				})
			}
			for _, delta := range deltas {
				t.Run("update "+delta, func(t *testing.T) {
					pub := &fakePublisher{}
					h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{doc: json.RawMessage(tc.doc)})
					assertNothingPublished(t, pub, h.handleSubscription(context.Background(), subEv("update", "", delta)))
				})
			}
		})
	}
}

// TestLiveRecordsStillPublishTheirName guards the other side of the invariant: the guard must not
// swallow rooms whose name merely resembles the marker.
func TestLiveRecordsStillPublishTheirName(t *testing.T) {
	for _, doc := range []string{
		`{"_id":"r1","t":"c","name":"delta","fname":"Delta","uids":["u1"]}`,
		`{"_id":"r1","t":"c","name":"del-general","fname":"del-General","uids":["u1"]}`,
		`{"_id":"r1","t":"c","name":"team-Del-old","fname":"Team Del-old","uids":["u1"]}`,
	} {
		pub := &fakePublisher{}
		h := newTestHandler(pub, &fakeTarget{}, &fakeLookup{})
		if err := h.handleRoom(context.Background(), roomEv("insert", doc, "")); err != nil {
			t.Fatalf("live room skipped: %v", err)
		}
		assert.NotEmpty(t, pub.events)
		assert.False(t, strings.HasPrefix(mustRoomName(t, pub), "Del-"))
	}
}

func mustRoomName(t *testing.T, pub *fakePublisher) string {
	t.Helper()
	var room struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(pub.events[0].Payload, &room); err != nil {
		t.Fatalf("unmarshal room_sync: %v", err)
	}
	return room.Name
}
