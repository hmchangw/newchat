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
)

// TestNewESRead_Integration exercises the ES self-lookup against a real index:
// it reads a parent message's own (authoritative) createdAt by message ID.
func TestNewESRead_Integration(t *testing.T) {
	esURL := setupElasticsearch(t)
	ctx := context.Background()

	prefix := "msgs-esread-v1"
	engine, err := searchengine.New(ctx, searchengine.Config{Backend: "elasticsearch", URL: esURL})
	require.NoError(t, err)
	waitForClusterGreen(t, esURL, 120*time.Second)

	coll := newMessageCollection(prefix, "site-a", time.Time{}, true)
	require.NoError(t, engine.UpsertTemplate(ctx, coll.TemplateName(), overrideIndexSettings(messageTemplateBody(prefix, true))))
	index := prefix + "-2026-03"
	preCreateIndex(t, esURL, index)
	waitForClusterGreen(t, esURL, 120*time.Second)

	// Index a parent message doc carrying its own (authoritative) createdAt.
	parentCreatedAt := time.Date(2026, 3, 9, 7, 0, 0, 0, time.UTC)
	doc, _ := json.Marshal(MessageSearchIndex{
		MessageID: "parent-es-1",
		RoomID:    "r1",
		SiteID:    "site-a",
		CreatedAt: parentCreatedAt,
	})
	_, err = engine.Bulk(ctx, []searchengine.BulkAction{{
		Action:  searchengine.ActionIndex,
		Index:   index,
		DocID:   "parent-es-1",
		Version: parentCreatedAt.UnixMilli(),
		Doc:     doc,
	}})
	require.NoError(t, err)
	refreshIndex(t, esURL, prefix+"-*")

	read := newESRead(engine, prefix)

	got, ok := read(ctx, "parent-es-1")
	require.True(t, ok)
	assert.True(t, got.Equal(parentCreatedAt), "got %v want %v", got, parentCreatedAt)

	_, ok = read(ctx, "missing-parent")
	assert.False(t, ok)
}
