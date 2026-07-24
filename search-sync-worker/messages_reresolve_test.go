package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/searchindex"
)

// fakeParentResolver is a test double for parentCreatedAtResolver.
type fakeParentResolver struct {
	val    time.Time
	ok     bool
	calls  int
	lastID string
}

func (f *fakeParentResolver) ResolveParentCreatedAt(_ context.Context, messageID string) (time.Time, bool) {
	f.calls++
	f.lastID = messageID
	return f.val, f.ok
}

func threadReplyData(t *testing.T, event model.EventType, parentCreatedAt *time.Time) []byte {
	t.Helper()
	evt := model.MessageEvent{
		Event: event,
		Message: model.Message{
			ID: "reply-1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
			Content:                      "a reply",
			CreatedAt:                    time.Date(2026, 1, 20, 12, 0, 0, 0, time.UTC),
			ThreadParentMessageID:        "parent-1",
			ThreadParentMessageCreatedAt: parentCreatedAt,
		},
		SiteID:    "site-a",
		Timestamp: 1737964678390,
	}
	data, err := json.Marshal(evt)
	require.NoError(t, err)
	return data
}

func indexedThreadParentCreatedAt(t *testing.T, doc json.RawMessage) *time.Time {
	t.Helper()
	var idx searchindex.MessageDoc
	require.NoError(t, json.Unmarshal(doc, &idx))
	return idx.ThreadParentCreatedAt
}

func TestMessageCollection_BuildAction_ReresolvesThreadParent(t *testing.T) {
	eventVal := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC) // gatekeeper-resolved, rides the event
	resolved := time.Date(2024, 6, 1, 9, 30, 0, 0, time.UTC) // what the ES resolver would return

	t.Run("event-carried value is trusted — resolver not called", func(t *testing.T) {
		resolver := &fakeParentResolver{val: resolved, ok: true}
		coll := newMessageCollection("msgs-v1", time.Time{}, false)
		coll.parentResolver = resolver

		actions, err := coll.BuildAction(threadReplyData(t, model.EventCreated, &eventVal))
		require.NoError(t, err)
		require.Len(t, actions, 1)
		assert.Zero(t, resolver.calls, "resolver must not run when the event carries the value")
		got := indexedThreadParentCreatedAt(t, actions[0].Doc)
		require.NotNil(t, got)
		assert.True(t, got.Equal(eventVal), "the event value must be indexed as-is, got %v", got)
	})

	t.Run("absent value re-resolved from ES (fallback)", func(t *testing.T) {
		resolver := &fakeParentResolver{val: resolved, ok: true}
		coll := newMessageCollection("msgs-v1", time.Time{}, false)
		coll.parentResolver = resolver

		actions, err := coll.BuildAction(threadReplyData(t, model.EventCreated, nil))
		require.NoError(t, err)
		require.Len(t, actions, 1)
		got := indexedThreadParentCreatedAt(t, actions[0].Doc)
		require.NotNil(t, got)
		assert.True(t, got.Equal(resolved), "indexed value must come from the resolver, got %v", got)
		assert.Equal(t, "parent-1", resolver.lastID)
	})

	t.Run("absent value stays unset when resolver returns ok=false", func(t *testing.T) {
		resolver := &fakeParentResolver{ok: false}
		coll := newMessageCollection("msgs-v1", time.Time{}, false)
		coll.parentResolver = resolver

		actions, err := coll.BuildAction(threadReplyData(t, model.EventCreated, nil))
		require.NoError(t, err)
		got := indexedThreadParentCreatedAt(t, actions[0].Doc)
		assert.Nil(t, got)
	})

	t.Run("does not resolve a non-thread message", func(t *testing.T) {
		resolver := &fakeParentResolver{val: resolved, ok: true}
		coll := newMessageCollection("msgs-v1", time.Time{}, false)
		coll.parentResolver = resolver

		evt := model.MessageEvent{
			Event:     model.EventCreated,
			Message:   model.Message{ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice", Content: "hi", CreatedAt: time.Date(2026, 1, 20, 12, 0, 0, 0, time.UTC)},
			SiteID:    "site-a",
			Timestamp: 1737964678390,
		}
		data, _ := json.Marshal(evt)
		_, err := coll.BuildAction(data)
		require.NoError(t, err)
		assert.Zero(t, resolver.calls)
	})

	t.Run("does not resolve a delete", func(t *testing.T) {
		resolver := &fakeParentResolver{val: resolved, ok: true}
		coll := newMessageCollection("msgs-v1", time.Time{}, false)
		coll.parentResolver = resolver

		_, err := coll.BuildAction(threadReplyData(t, model.EventDeleted, nil))
		require.NoError(t, err)
		assert.Zero(t, resolver.calls)
	})

	t.Run("nil resolver keeps event value (feature off)", func(t *testing.T) {
		coll := newMessageCollection("msgs-v1", time.Time{}, false)
		actions, err := coll.BuildAction(threadReplyData(t, model.EventCreated, &eventVal))
		require.NoError(t, err)
		got := indexedThreadParentCreatedAt(t, actions[0].Doc)
		require.NotNil(t, got)
		assert.True(t, got.Equal(eventVal))
	})
}
