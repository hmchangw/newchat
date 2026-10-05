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
		data, _ := json.Marshal(ev)
		return &fakeMsg{subject: "chat.msg.canonical.site-a." + string(ev.Event), data: data, seq: seq, stream: "MESSAGES-CANONICAL-site-a"}, data
	}

	t.Run("created carries body, frame opens to the record", func(t *testing.T) {
		msg, data := mk(events["created"], 41)
		it, err := buildEventItem(context.Background(), "site-a", msg, data, c, now)
		require.NoError(t, err)
		assert.Equal(t, uint64(41), it.seq)
		require.Len(t, it.docs, 1)
		d := it.docs[0].Doc.(*auditarchive.EventDoc)
		assert.Equal(t, "audit-events-site-a-2025.10.05", it.docs[0].Index)
		assert.Equal(t, "site-a-41", it.docs[0].ID)
		assert.Equal(t, "created", d.EventType)
		assert.Equal(t, "m1", d.MessageID)
		assert.Equal(t, "p.ortiz", d.SenderAccount)
		assert.Equal(t, 1, d.AttachmentCount)
		assert.Equal(t, []string{"image/png"}, d.AttachmentTypes)
		assert.NotEmpty(t, d.EncBody)
		assert.Regexp(t, `^sha256:`, d.ContentHash)

		plain, err := c.Open(it.frame, auditarchive.FrameAAD("site-a", 41))
		require.NoError(t, err)
		var rec auditarchive.Record
		require.NoError(t, json.Unmarshal(plain, &rec))
		assert.Equal(t, uint64(41), rec.Seq)
		assert.JSONEq(t, string(data), string(rec.Payload))
		assert.Equal(t, auditarchive.HashBytes(plain), d.ContentHash)

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
}
