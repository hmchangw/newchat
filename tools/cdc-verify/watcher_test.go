package main

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
)

type captureSubmitter struct{ events []CDCEvent }

func (c *captureSubmitter) Submit(ev CDCEvent) { c.events = append(c.events, ev) }

func TestDeliverPolicy(t *testing.T) {
	cfg := orderedConfig(time.Time{})
	assert.Equal(t, jetstream.DeliverNewPolicy, cfg.DeliverPolicy)
	assert.Nil(t, cfg.OptStartTime)

	at := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	cfg = orderedConfig(at)
	assert.Equal(t, jetstream.DeliverByStartTimePolicy, cfg.DeliverPolicy)
	assert.Equal(t, at, *cfg.OptStartTime)
}

func TestHandleMsg(t *testing.T) {
	sub := &captureSubmitter{}
	w := &watcher{sub: sub}

	w.handleMsg([]byte(`{"op":"insert","coll":"rocketchat_message","documentKey":{"_id":"m1"}}`), "chat.migration.oplog.s1.rocketchat_message.insert")
	assert.Len(t, sub.events, 1)
	assert.Equal(t, "m1", sub.events[0].DocID)

	w.handleMsg([]byte(`not json`), "chat.migration.oplog.s1.x.insert") // must not panic, not submit
	assert.Len(t, sub.events, 1)
}
