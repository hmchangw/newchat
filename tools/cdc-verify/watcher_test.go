package main

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
)

type captureSubmitter struct {
	events      []CDCEvent
	undecodable []CDCEvent // collection/op only
}

func (c *captureSubmitter) Submit(ev CDCEvent) { c.events = append(c.events, ev) }
func (c *captureSubmitter) SubmitUndecodable(collection, op string) {
	c.undecodable = append(c.undecodable, CDCEvent{Collection: collection, Op: op})
}

func TestDeliverPolicy(t *testing.T) {
	filter := "chat.migration.oplog.s1.>"
	cfg := orderedConfig(time.Time{}, filter)
	assert.Equal(t, jetstream.DeliverNewPolicy, cfg.DeliverPolicy)
	assert.Nil(t, cfg.OptStartTime)
	assert.Equal(t, []string{filter}, cfg.FilterSubjects)

	at := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	cfg = orderedConfig(at, filter)
	assert.Equal(t, jetstream.DeliverByStartTimePolicy, cfg.DeliverPolicy)
	assert.Equal(t, at, *cfg.OptStartTime)
	assert.Equal(t, []string{filter}, cfg.FilterSubjects)
}

func TestParseOplogSubject(t *testing.T) {
	tests := []struct {
		name           string
		subject        string
		wantCollection string
		wantOp         string
		wantOK         bool
	}{
		{"valid", "chat.migration.oplog.site1.rocketchat_message.insert", "rocketchat_message", "insert", true},
		{"valid delete", "chat.migration.oplog.s1.users.delete", "users", "delete", true},
		{"wrong prefix", "chat.msg.canonical.site1.created", "", "", false},
		{"too few tokens", "chat.migration.oplog.site1.coll", "", "", false},
		{"too many tokens", "chat.migration.oplog.site1.coll.insert.extra", "", "", false},
		{"empty", "", "", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			collection, op, ok := parseOplogSubject(tt.subject)
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.wantCollection, collection)
			assert.Equal(t, tt.wantOp, op)
		})
	}
}

func TestHandleMsg(t *testing.T) {
	sub := &captureSubmitter{}
	w := &watcher{sub: sub}

	w.handleMsg([]byte(`{"op":"insert","coll":"rocketchat_message","documentKey":{"_id":"m1"}}`),
		"chat.migration.oplog.s1.rocketchat_message.insert")
	assert.Len(t, sub.events, 1)
	assert.Equal(t, "m1", sub.events[0].DocID)

	// The subject is authoritative for routing: a payload that disagrees is
	// still verified, but under the subject's collection/op.
	w.handleMsg([]byte(`{"op":"delete","coll":"other_coll","documentKey":{"_id":"m2"}}`),
		"chat.migration.oplog.s1.rocketchat_message.update")
	assert.Len(t, sub.events, 2)
	assert.Equal(t, "rocketchat_message", sub.events[1].Collection)
	assert.Equal(t, "update", sub.events[1].Op)
	assert.Equal(t, "m2", sub.events[1].DocID)

	// An undecodable payload on a well-formed subject surfaces as a skipped
	// row instead of vanishing into the log.
	w.handleMsg([]byte(`not json`), "chat.migration.oplog.s1.rocketchat_room.insert")
	assert.Len(t, sub.events, 2)
	assert.Len(t, sub.undecodable, 1)
	assert.Equal(t, "rocketchat_room", sub.undecodable[0].Collection)
	assert.Equal(t, "insert", sub.undecodable[0].Op)

	// A malformed subject can't be classified at all — log-only skip.
	w.handleMsg([]byte(`{}`), "some.other.subject")
	assert.Len(t, sub.events, 2)
	assert.Len(t, sub.undecodable, 1)
}
