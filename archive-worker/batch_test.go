package main

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/auditarchive"
	"github.com/hmchangw/chat/pkg/searchengine"
)

func mkItem(seq uint64, size int) item {
	return item{ctx: context.Background(), seq: seq, frame: bytes.Repeat([]byte{1}, size),
		docs: []docSpec{{Index: "audit-events-site-a-2026.10.05", ID: auditarchive.EventDocID("site-a", seq), Doc: &auditarchive.EventDoc{Seq: seq}}}}
}

func TestBatcher_Bounds(t *testing.T) {
	t0 := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	t.Run("count bound", func(t *testing.T) {
		b := newBatcher(2, 1<<20, time.Minute)
		assert.False(t, b.add(mkItem(1, 10), t0))
		assert.True(t, b.add(mkItem(2, 10), t0))
		assert.Len(t, b.take(), 2)
		assert.Nil(t, b.take())
	})
	t.Run("byte bound", func(t *testing.T) {
		b := newBatcher(100, 25, time.Minute)
		assert.False(t, b.add(mkItem(1, 10), t0))
		assert.True(t, b.add(mkItem(2, 20), t0))
	})
	t.Run("time bound", func(t *testing.T) {
		b := newBatcher(100, 1<<20, 10*time.Second)
		assert.False(t, b.due(t0))
		b.add(mkItem(1, 1), t0)
		assert.False(t, b.due(t0.Add(9*time.Second)))
		assert.True(t, b.due(t0.Add(10*time.Second)))
	})
}

func TestSeal(t *testing.T) {
	now := time.Date(2026, 10, 5, 14, 0, 0, 0, time.UTC)
	items := []item{mkItem(7, 3), mkItem(5, 4), mkItem(9, 5)}
	s, err := seal("site-a", "events", "kid-1", items, now)
	require.NoError(t, err)
	assert.Regexp(t, `^site-a/2026/10/05/14/events-5-9-[0-9a-f]{8}\.seg$`, s.key, "key uses min and max seq plus a random suffix")

	h, frames, err := auditarchive.ReadSegment(bytes.NewReader(s.body))
	require.NoError(t, err)
	assert.Equal(t, uint32(3), h.Count)
	assert.Equal(t, "events", h.Lane)
	assert.Equal(t, "kid-1", h.KeyID, "the header names the DEK that sealed the frames")
	assert.Equal(t, [][]byte{items[0].frame, items[1].frame, items[2].frame}, frames, "frames keep delivery order")

	require.Len(t, s.actions, 3)
	for i, a := range s.actions {
		assert.Equal(t, searchengine.ActionCreate, a.Action)
		assert.Equal(t, "audit-events-site-a-2026.10.05", a.Index)
		var d auditarchive.EventDoc
		require.NoError(t, json.Unmarshal(a.Doc, &d))
		assert.Equal(t, s.key, d.SegmentKey)
		f, err := auditarchive.ReadFrameAt(bytes.NewReader(s.body), d.FrameOffset)
		require.NoError(t, err)
		assert.Equal(t, items[i].frame, f, "offset %d points at its own frame", i)
	}
	t.Run("empty is an error", func(t *testing.T) {
		_, err := seal("site-a", "events", "kid-1", nil, now)
		assert.Error(t, err)
	})
}
