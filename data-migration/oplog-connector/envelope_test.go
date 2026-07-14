package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func mustRaw(t *testing.T, m bson.M) bson.Raw {
	t.Helper()
	r, err := bson.Marshal(m)
	require.NoError(t, err)
	return r
}

func TestBuildEnvelope_OpsAndSubjects(t *testing.T) {
	const site = "site1"
	const nowMs = int64(1718100000123)

	tests := []struct {
		name          string
		op            string
		hasFull       bool // native fullDocument (insert/replace)
		hasUpdateDesc bool // delta (update)
		wantSubject   string
	}{
		{"insert", "insert", true, false, "chat.migration.oplog.site1.rocketchat_message.insert"},
		{"update", "update", false, true, "chat.migration.oplog.site1.rocketchat_message.update"},
		{"replace", "replace", true, false, "chat.migration.oplog.site1.rocketchat_message.replace"},
		{"delete", "delete", false, false, "chat.migration.oplog.site1.rocketchat_message.delete"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ev := changeEvent{
				EventID:       "EVT-" + tc.op,
				Op:            tc.op,
				DB:            "rocketchat",
				Collection:    "rocketchat_message",
				DocumentKey:   mustRaw(t, bson.M{"_id": "abc"}),
				ClusterTimeMs: 1718100000000,
			}
			if tc.hasFull {
				ev.FullDocument = mustRaw(t, bson.M{"_id": "abc", "msg": "hi"})
			}
			if tc.hasUpdateDesc {
				ev.UpdateDescription = mustRaw(t, bson.M{"updatedFields": bson.M{"msg": "edited"}})
			}

			subj, msgID, evt := buildEnvelope(&ev, site, nowMs)

			assert.Equal(t, tc.wantSubject, subj)
			assert.Equal(t, ev.EventID, msgID, "msgID must equal eventID")
			assert.Equal(t, ev.EventID, evt.EventID)
			assert.Equal(t, tc.op, evt.Op)
			assert.Equal(t, "rocketchat", evt.DB)
			assert.Equal(t, "rocketchat_message", evt.Collection)
			assert.Equal(t, site, evt.SiteID)
			assert.Equal(t, nowMs, evt.Timestamp, "event-level timestamp is the injected publish time")
			assert.Equal(t, int64(1718100000000), evt.ClusterTime)

			// documentKey is always present and valid JSON.
			assert.True(t, json.Valid(evt.DocumentKey))

			if tc.hasFull {
				require.NotNil(t, evt.FullDocument, "insert/replace carry the native document")
				assert.True(t, json.Valid(evt.FullDocument))
			} else {
				assert.Nil(t, evt.FullDocument, "update/delete carry no post-image (no lookup)")
			}

			if tc.hasUpdateDesc {
				require.NotNil(t, evt.UpdateDescription, "update carries the raw delta")
				assert.True(t, json.Valid(evt.UpdateDescription))
			} else {
				assert.Nil(t, evt.UpdateDescription)
			}

			assert.False(t, evt.Degraded, "well-formed events are not degraded")
			assert.Empty(t, evt.DegradedReason)
		})
	}
}

func TestBuildEnvelope_DegradesOnFieldEncodeFailure(t *testing.T) {
	// Lock the fixture: this raw declares length 5 but its terminator byte is
	// 0x01 (not 0x00), so MarshalExtJSON rejects it.
	bad := bson.Raw{0x05, 0x00, 0x00, 0x00, 0x01}
	_, e := rawToJSON(bad)
	require.Error(t, e, "fixture must make rawToJSON fail")

	ev := changeEvent{
		EventID:      "EVT-degrade",
		Op:           "insert",
		DB:           "rocketchat",
		Collection:   "rocketchat_message",
		DocumentKey:  mustRaw(t, bson.M{"_id": "abc"}),
		FullDocument: bad, // forces the encode failure
	}

	_, msgID, evt := buildEnvelope(&ev, "site1", 1)

	assert.True(t, evt.Degraded, "a field that fails to encode degrades the event")
	assert.NotEmpty(t, evt.DegradedReason, "degraded events carry a reason")
	assert.Contains(t, evt.DegradedReason, "fullDocument", "reason names the failed field")
	assert.Nil(t, evt.FullDocument, "the failed field is omitted")

	// Non-failing fields are still populated — the event is published, not dropped.
	assert.Equal(t, "EVT-degrade", msgID)
	assert.Equal(t, "EVT-degrade", evt.EventID)
	assert.Equal(t, "insert", evt.Op)
	assert.Equal(t, "rocketchat_message", evt.Collection)
	require.NotNil(t, evt.DocumentKey)
	assert.True(t, json.Valid(evt.DocumentKey))
}

func TestBuildEnvelope_OpaqueDocumentContents(t *testing.T) {
	ev := changeEvent{
		EventID:      "E1",
		Op:           "insert",
		DB:           "rocketchat",
		Collection:   "users",
		DocumentKey:  mustRaw(t, bson.M{"_id": "u1"}),
		FullDocument: mustRaw(t, bson.M{"_id": "u1", "name": "alice", "active": true}),
	}
	_, _, evt := buildEnvelope(&ev, "site1", 1)

	// The connector does not interpret the doc; it round-trips as JSON.
	var decoded map[string]any
	require.NoError(t, json.Unmarshal(evt.FullDocument, &decoded))
	assert.Equal(t, "alice", decoded["name"])
	assert.Equal(t, true, decoded["active"])
}
