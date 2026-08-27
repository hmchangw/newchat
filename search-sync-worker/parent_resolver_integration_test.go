//go:build integration

package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/searchengine"
	"github.com/hmchangw/chat/pkg/searchindex"
)

// TestParentResolver_Integration exercises the parent lookup against a real index.
//
// Deliberately no refreshIndex before reading: the point of the point-read path is
// that a get is realtime, so it must see a parent that has been indexed but not yet
// refreshed. A search cannot, which is why the old cross-month lookup missed exactly
// the recent-parent case that matters most.
func TestParentResolver_Integration(t *testing.T) {
	esURL := setupElasticsearch(t)
	ctx := context.Background()

	prefix := "msgs-esread-v1"
	engine, err := searchengine.New(ctx, searchengine.Config{Backend: "elasticsearch", URL: esURL})
	require.NoError(t, err)
	waitForClusterGreen(t, esURL, 120*time.Second)

	coll := newMessageCollection(prefix, "site-a", time.Time{}, true)
	require.NoError(t, engine.UpsertTemplate(ctx, coll.TemplateName(), overrideIndexSettings(searchindex.MessageTemplateBody(prefix, true))))
	index := prefix + "-2026-03"
	preCreateIndex(t, esURL, index)
	preCreateIndex(t, esURL, prefix+"-2026-02")
	waitForClusterGreen(t, esURL, 120*time.Second)

	indexParent := func(t *testing.T, id, idx string, createdAt time.Time) {
		t.Helper()
		doc, err := json.Marshal(searchindex.MessageDoc{
			MessageID: id, RoomID: "r1", SiteID: "site-a", CreatedAt: createdAt,
		})
		require.NoError(t, err)
		_, err = engine.Bulk(ctx, []searchengine.BulkAction{{
			Action: searchengine.ActionIndex, Index: idx, DocID: id,
			Version: createdAt.UnixMilli(), Doc: doc,
		}})
		require.NoError(t, err)
	}

	resolver := newESParentResolver(engine, prefix)
	replyAt := time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC)

	t.Run("resolves a same-month parent without waiting for a refresh", func(t *testing.T) {
		parentAt := time.Date(2026, 3, 9, 7, 0, 0, 0, time.UTC)
		indexParent(t, "parent-same-month", index, parentAt)

		got, ok := resolver.ResolveParentCreatedAt(ctx, "parent-same-month", replyAt)

		require.True(t, ok, "a get is realtime; an un-refreshed parent must still resolve")
		assert.True(t, got.Equal(parentAt), "got %v want %v", got, parentAt)
	})

	t.Run("resolves a parent in the previous month", func(t *testing.T) {
		parentAt := time.Date(2026, 2, 25, 8, 0, 0, 0, time.UTC)
		indexParent(t, "parent-prev-month", prefix+"-2026-02", parentAt)

		got, ok := resolver.ResolveParentCreatedAt(ctx, "parent-prev-month", replyAt)

		require.True(t, ok)
		assert.True(t, got.Equal(parentAt), "got %v want %v", got, parentAt)
	})

	t.Run("ok=false for a parent nothing has indexed", func(t *testing.T) {
		_, ok := resolver.ResolveParentCreatedAt(ctx, "missing-parent", replyAt)
		assert.False(t, ok)
	})
}
