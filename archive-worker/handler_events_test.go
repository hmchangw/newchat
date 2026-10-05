package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/auditarchive"
	"github.com/hmchangw/chat/pkg/model"
)

func loadEvents(t *testing.T) map[string]model.MessageEvent {
	t.Helper()
	raw, err := os.ReadFile("testdata/events.json")
	require.NoError(t, err)
	var list []model.MessageEvent
	require.NoError(t, json.Unmarshal(raw, &list))
	out := map[string]model.MessageEvent{}
	for i := range list {
		out[string(list[i].Event)] = list[i]
	}
	return out
}

func TestBuildEventItem(t *testing.T) {
	c, err := auditarchive.NewCipher(testDEK())
	require.NoError(t, err)
	events := loadEvents(t)
	now := time.Date(2026, 10, 5, 14, 0, 0, 0, time.UTC)
	mk := func(ev model.MessageEvent, seq uint64) (*fakeMsg, []byte) {
		data, err := json.Marshal(ev)
		require.NoError(t, err)
		return &fakeMsg{subject: "chat.msg.canonical.site-a." + string(ev.Event), data: data, seq: seq, stream: "MESSAGES-CANONICAL-site-a"}, data
	}

	t.Run("created carries body, frame opens to the record", func(t *testing.T) {
		msg, data := mk(events["created"], 41)
		it, err := buildEventItem(context.Background(), "site-a", msg, data, c, now)
		require.NoError(t, err)
		assert.Equal(t, uint64(41), it.seq)
		require.Len(t, it.docs, 1)
		d := it.docs[0].Doc.(*auditarchive.EventDoc)
		assert.Equal(t, "audit-events-site-a-2026.10.05", it.docs[0].Index, "index follows the archive clock (now), not the 2025 event time")
		assert.Equal(t, time.UnixMilli(events["created"].Timestamp).UTC(), d.EventAt, "eventAt stays the event's own time")
		assert.Equal(t, "site-a-41", it.docs[0].ID)
		assert.Equal(t, "created", d.EventType)
		assert.Equal(t, "m1", d.MessageID)
		assert.Equal(t, "p.ortiz", d.SenderAccount)
		assert.Equal(t, 1, d.AttachmentCount)
		assert.Equal(t, []string{"image/png"}, d.AttachmentTypes)
		assert.NotEmpty(t, d.EncBody)
		assert.Regexp(t, `^hmac-sha256:`, d.ContentHash)

		plain, err := c.Open(it.frame, auditarchive.FrameAAD("site-a", 41))
		require.NoError(t, err)
		var rec auditarchive.Record
		require.NoError(t, json.Unmarshal(plain, &rec))
		assert.Equal(t, uint64(41), rec.Seq)
		assert.JSONEq(t, string(data), string(rec.Payload))
		assert.Equal(t, c.Digest(plain), d.ContentHash)

		body, err := c.Open(d.EncBody, auditarchive.BodyAAD("site-a", 41))
		require.NoError(t, err)
		var eb eventBody
		require.NoError(t, json.Unmarshal(body, &eb))
		assert.Equal(t, events["created"].Message.Content, eb.Content)
	})
	t.Run("deleted has no body and names the actor", func(t *testing.T) {
		msg, data := mk(events["deleted"], 42)
		it, err := buildEventItem(context.Background(), "site-a", msg, data, c, now)
		require.NoError(t, err)
		d := it.docs[0].Doc.(*auditarchive.EventDoc)
		assert.Empty(t, d.EncBody)
		assert.Equal(t, "p.ortiz", d.ActorAccount)
	})
	t.Run("pinned names the pinner, not the author", func(t *testing.T) {
		msg, data := mk(events["pinned"], 48)
		it, err := buildEventItem(context.Background(), "site-a", msg, data, c, now)
		require.NoError(t, err)
		d := it.docs[0].Doc.(*auditarchive.EventDoc)
		assert.Equal(t, "h.brandt", d.ActorAccount)
		assert.Equal(t, "p.ortiz", d.SenderAccount)
		assert.Empty(t, d.EncBody)
	})
	t.Run("pinned without pinnedBy falls back to the author", func(t *testing.T) {
		ev := events["pinned"]
		ev.Message.PinnedBy = nil
		msg, data := mk(ev, 49)
		it, err := buildEventItem(context.Background(), "site-a", msg, data, c, now)
		require.NoError(t, err)
		assert.Equal(t, "p.ortiz", it.docs[0].Doc.(*auditarchive.EventDoc).ActorAccount)
	})
	t.Run("unpinned names the pinner", func(t *testing.T) {
		ev := events["pinned"]
		ev.Event = model.EventUnpinned
		msg, data := mk(ev, 50)
		it, err := buildEventItem(context.Background(), "site-a", msg, data, c, now)
		require.NoError(t, err)
		assert.Equal(t, "h.brandt", it.docs[0].Doc.(*auditarchive.EventDoc).ActorAccount)
	})
	t.Run("reacted names the reactor", func(t *testing.T) {
		msg, data := mk(events["reacted"], 43)
		it, err := buildEventItem(context.Background(), "site-a", msg, data, c, now)
		require.NoError(t, err)
		assert.Equal(t, "h.brandt", it.docs[0].Doc.(*auditarchive.EventDoc).ActorAccount)
	})
	t.Run("thread_reply_added is skipped", func(t *testing.T) {
		ev := events["created"]
		ev.Event = model.EventThreadReplyAdded
		msg, data := mk(ev, 44)
		_, err := buildEventItem(context.Background(), "site-a", msg, data, c, now)
		assert.True(t, errors.Is(err, errSkip))
	})
	t.Run("malformed json is poison", func(t *testing.T) {
		msg := &fakeMsg{subject: "x", data: []byte("{nope"), seq: 45, stream: "s"}
		_, err := buildEventItem(context.Background(), "site-a", msg, []byte("{nope"), c, now)
		assert.True(t, errors.Is(err, errPoison))
	})
	t.Run("missing message id is poison", func(t *testing.T) {
		ev := events["created"]
		ev.Message.ID = ""
		msg, data := mk(ev, 46)
		_, err := buildEventItem(context.Background(), "site-a", msg, data, c, now)
		assert.True(t, errors.Is(err, errPoison))
	})
	t.Run("empty event type means created", func(t *testing.T) {
		ev := events["created"]
		ev.Event = ""
		msg, data := mk(ev, 47)
		it, err := buildEventItem(context.Background(), "site-a", msg, data, c, now)
		require.NoError(t, err)
		assert.Equal(t, "created", it.docs[0].Doc.(*auditarchive.EventDoc).EventType)
	})
	t.Run("index is named for the archive day even for a late event", func(t *testing.T) {
		late := now.Add(30 * 24 * time.Hour)
		msg, data := mk(events["created"], 51)
		it, err := buildEventItem(context.Background(), "site-a", msg, data, c, late)
		require.NoError(t, err)
		assert.Equal(t, auditarchive.EventsIndex("site-a", late), it.docs[0].Index)
	})
	t.Run("missing timestamp falls back to the stream store time, never the clock", func(t *testing.T) {
		ev := events["created"]
		ev.Timestamp = 0
		msg, data := mk(ev, 52)
		stored := time.Date(2026, 10, 4, 23, 59, 58, 123_000_000, time.UTC)
		msg.storedAt = stored
		first, err := buildEventItem(context.Background(), "site-a", msg, data, c, now)
		require.NoError(t, err)
		again, err := buildEventItem(context.Background(), "site-a", msg, data, c, now.Add(time.Hour))
		require.NoError(t, err)
		d1, d2 := first.docs[0].Doc.(*auditarchive.EventDoc), again.docs[0].Doc.(*auditarchive.EventDoc)
		assert.Equal(t, stored, d1.EventAt)
		assert.Equal(t, d1.EventAt, d2.EventAt, "a redelivery builds the same eventAt")
		assert.Equal(t, d1.ContentHash, d2.ContentHash, "and so the same record hash")
		assert.Equal(t, d1.ContentHash, first.hash, "the item carries the record hash for conflict checks")
		assert.Equal(t, stored, first.eventAt)
	})
}
