package main

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/searchengine"
)

func TestMessageCollection_TemplateName_StripsVersion(t *testing.T) {
	coll := newMessageCollection("messages-site1-v1", time.Time{})
	assert.Equal(t, "messages-site1_template", coll.TemplateName())
}

func TestMessageCollection_TemplateName_BareBaseFallback(t *testing.T) {
	coll := newMessageCollection("messages-site1", time.Time{})
	assert.Equal(t, "messages-site1_template", coll.TemplateName())
}

func TestMessageCollection_TemplateBody_PatternStripsVersion(t *testing.T) {
	coll := newMessageCollection("messages-site1-v1", time.Time{})
	body := coll.TemplateBody()
	require.NotNil(t, body)

	var parsed map[string]any
	require.NoError(t, json.Unmarshal(body, &parsed))

	patterns, ok := parsed["index_patterns"].([]any)
	require.True(t, ok)
	require.Len(t, patterns, 1)
	assert.Equal(t, "messages-site1-*", patterns[0])

	tmpl := parsed["template"].(map[string]any)
	mappings := tmpl["mappings"].(map[string]any)
	props := mappings["properties"].(map[string]any)
	assert.Contains(t, props, "messageId")
	assert.Contains(t, props, "roomId")
	assert.Contains(t, props, "siteId")
	assert.Contains(t, props, "userId")
	assert.Contains(t, props, "userAccount")
	assert.Contains(t, props, "content")
	assert.Contains(t, props, "createdAt")
	assert.Contains(t, props, "tshow")
	assert.Equal(t, "boolean", props["tshow"].(map[string]any)["type"])
	assert.Equal(t, false, mappings["dynamic"])

	settings := tmpl["settings"].(map[string]any)
	analysis := settings["analysis"].(map[string]any)
	analyzers := analysis["analyzer"].(map[string]any)
	assert.Contains(t, analyzers, "custom_analyzer")
}

func TestMessageCollection_StreamConfig(t *testing.T) {
	coll := newMessageCollection("msgs-v1", time.Time{})
	cfg := coll.StreamConfig("site-a")
	assert.Equal(t, "MESSAGES_CANONICAL_site-a", cfg.Name)
}

func TestMessageCollection_ConsumerName(t *testing.T) {
	coll := newMessageCollection("msgs-v1", time.Time{})
	assert.Equal(t, "message-sync", coll.ConsumerName())
}

func TestIndexName(t *testing.T) {
	tests := []struct {
		name      string
		prefix    string
		createdAt time.Time
		want      string
	}{
		{"jan 2026", "messages-site1-v1", time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC), "messages-site1-v1-2026-01"},
		{"dec 2025", "msgs-v2", time.Date(2025, 12, 31, 23, 59, 0, 0, time.UTC), "msgs-v2-2025-12"},
		{"non-UTC normalized", "msgs", time.Date(2026, 1, 1, 5, 0, 0, 0, time.FixedZone("EST", -5*3600)), "msgs-2026-01"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := indexName(tt.prefix, tt.createdAt)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestBuildMessageAction(t *testing.T) {
	ts := time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC)

	t.Run("created event produces index action", func(t *testing.T) {
		evt := &model.MessageEvent{
			Event: model.EventCreated,
			Message: model.Message{
				ID: "msg-1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
				Content: "hello", CreatedAt: ts,
			},
			SiteID:    "site-a",
			Timestamp: 1737964678390,
		}
		action := buildMessageAction(evt, "msgs-v1")
		assert.Equal(t, searchengine.ActionIndex, action.Action)
		assert.Equal(t, "msgs-v1-2026-01", action.Index)
		assert.Equal(t, "msg-1", action.DocID)
		assert.Equal(t, int64(1737964678390), action.Version)
		require.NotNil(t, action.Doc)

		var doc map[string]any
		require.NoError(t, json.Unmarshal(action.Doc, &doc))
		assert.Equal(t, "msg-1", doc["messageId"])
		assert.Equal(t, "r1", doc["roomId"])
		assert.Equal(t, "site-a", doc["siteId"])
		assert.Equal(t, "u1", doc["userId"])
		assert.Equal(t, "alice", doc["userAccount"])
		assert.Equal(t, "hello", doc["content"])
	})

	t.Run("updated event produces index action (full replace)", func(t *testing.T) {
		evt := &model.MessageEvent{
			Event: model.EventUpdated,
			Message: model.Message{
				ID: "msg-1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
				Content: "updated", CreatedAt: ts,
			},
			SiteID:    "site-a",
			Timestamp: 1737964699000,
		}
		action := buildMessageAction(evt, "msgs-v1")
		assert.Equal(t, searchengine.ActionIndex, action.Action)
		assert.Equal(t, int64(1737964699000), action.Version)
	})

	t.Run("deleted event produces delete action", func(t *testing.T) {
		evt := &model.MessageEvent{
			Event:     model.EventDeleted,
			Message:   model.Message{ID: "msg-1", RoomID: "r1", CreatedAt: ts},
			SiteID:    "site-a",
			Timestamp: 1737964710000,
		}
		action := buildMessageAction(evt, "msgs-v1")
		assert.Equal(t, searchengine.ActionDelete, action.Action)
		assert.Nil(t, action.Doc)
	})

	t.Run("empty event defaults to created (backward compat)", func(t *testing.T) {
		evt := &model.MessageEvent{
			Message: model.Message{
				ID: "msg-1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
				Content: "hello", CreatedAt: ts,
			},
			SiteID:    "site-a",
			Timestamp: 1735689600000,
		}
		action := buildMessageAction(evt, "msgs-v1")
		assert.Equal(t, searchengine.ActionIndex, action.Action)
	})
}

func TestMessageTemplateProperties_MatchesStruct(t *testing.T) {
	props := messageTemplateProperties()

	// Every MessageSearchIndex field with an es tag must have a corresponding template property.
	typ := reflect.TypeOf(MessageSearchIndex{})
	for i := range typ.NumField() {
		field := typ.Field(i)
		esTag := field.Tag.Get("es")
		if esTag == "" || esTag == "-" {
			continue
		}
		jsonTag := field.Tag.Get("json")
		name, _, _ := strings.Cut(jsonTag, ",")

		prop, ok := props[name]
		assert.True(t, ok, "template missing property for struct field %s (json: %s)", field.Name, name)

		esType, _, _ := strings.Cut(esTag, ",")
		propMap := prop.(map[string]any)
		assert.Equal(t, esType, propMap["type"], "type mismatch for field %s", name)
	}

	// Template should have exactly as many properties as struct fields with es tags.
	esFieldCount := 0
	for i := range typ.NumField() {
		if tag := typ.Field(i).Tag.Get("es"); tag != "" && tag != "-" {
			esFieldCount++
		}
	}
	assert.Equal(t, esFieldCount, len(props), "template property count should match struct es-tagged field count")
}

func TestNewMessageSearchIndex(t *testing.T) {
	ts := time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC)
	parentTS := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)
	editedTS := time.Date(2026, 1, 15, 10, 35, 0, 0, time.UTC)
	updatedTS := time.Date(2026, 1, 15, 10, 36, 0, 0, time.UTC)
	evt := &model.MessageEvent{
		Message: model.Message{
			ID: "msg-1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
			Content: "hello", CreatedAt: ts,
			EditedAt:                     &editedTS,
			UpdatedAt:                    &updatedTS,
			ThreadParentMessageID:        "parent-1",
			ThreadParentMessageCreatedAt: &parentTS,
			TShow:                        true,
		},
		SiteID: "site-a",
	}
	doc := newMessageSearchIndex(evt)
	assert.Equal(t, "msg-1", doc.MessageID)
	assert.Equal(t, "r1", doc.RoomID)
	assert.Equal(t, "site-a", doc.SiteID)
	assert.Equal(t, "u1", doc.UserID)
	assert.Equal(t, "alice", doc.UserAccount)
	assert.Equal(t, "hello", doc.Content)
	assert.Equal(t, ts, doc.CreatedAt)
	require.NotNil(t, doc.EditedAt)
	assert.Equal(t, editedTS, *doc.EditedAt)
	require.NotNil(t, doc.UpdatedAt)
	assert.Equal(t, updatedTS, *doc.UpdatedAt)
	assert.Equal(t, "parent-1", doc.ThreadParentID)
	require.NotNil(t, doc.ThreadParentCreatedAt)
	assert.Equal(t, parentTS, *doc.ThreadParentCreatedAt)
	assert.True(t, doc.TShow)
}

// Never-edited messages must omit editedAt/updatedAt so index entries stay
// compact for the common case.
func TestNewMessageSearchIndex_EditedUpdatedOmittedWhenNil(t *testing.T) {
	evt := &model.MessageEvent{
		Message: model.Message{
			ID: "msg-1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
			Content: "hello", CreatedAt: time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC),
		},
		SiteID: "site-a",
	}
	doc := newMessageSearchIndex(evt)
	assert.Nil(t, doc.EditedAt)
	assert.Nil(t, doc.UpdatedAt)

	data, err := json.Marshal(doc)
	require.NoError(t, err)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))
	_, hasEdited := raw["editedAt"]
	_, hasUpdated := raw["updatedAt"]
	assert.False(t, hasEdited, "editedAt should be omitted when nil")
	assert.False(t, hasUpdated, "updatedAt should be omitted when nil")
}

// TestNewMessageSearchIndex_TShowOmittedWhenFalse verifies that a message with
// the default TShow (false) marshals without a `tshow` key so unmarked thread
// replies don't bloat the index and so range/term queries on `tshow` only
// match explicitly-flagged docs.
func TestNewMessageSearchIndex_TShowOmittedWhenFalse(t *testing.T) {
	evt := &model.MessageEvent{
		Message: model.Message{
			ID: "msg-1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
			Content: "hello", CreatedAt: time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC),
		},
		SiteID: "site-a",
	}
	doc := newMessageSearchIndex(evt)
	assert.False(t, doc.TShow)

	data, err := json.Marshal(doc)
	require.NoError(t, err)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))
	_, present := raw["tshow"]
	assert.False(t, present, "tshow should be omitted when false")
}

func TestMessageCollection_BuildAction(t *testing.T) {
	coll := newMessageCollection("msgs-v1", time.Time{})
	evt := model.MessageEvent{
		Event: model.EventCreated,
		Message: model.Message{
			ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
			Content: "hello", CreatedAt: time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC),
		},
		SiteID: "site-a", Timestamp: 100,
	}
	data, _ := json.Marshal(evt)

	actions, err := coll.BuildAction(data)
	require.NoError(t, err)
	require.Len(t, actions, 1)
	assert.Equal(t, searchengine.ActionIndex, actions[0].Action)
	assert.Equal(t, "msgs-v1-2026-01", actions[0].Index)
	assert.Equal(t, "m1", actions[0].DocID)

	t.Run("malformed JSON returns error", func(t *testing.T) {
		_, err := coll.BuildAction([]byte("{invalid"))
		assert.Error(t, err)
	})
}

func TestMessageCollection_BuildAction_SyncFromFilter(t *testing.T) {
	cutoff := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	coll := newMessageCollection("msgs-v1", cutoff)

	mkEvent := func(createdAt time.Time) []byte {
		evt := model.MessageEvent{
			Event: model.EventCreated,
			Message: model.Message{
				ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
				Content: "hi", CreatedAt: createdAt,
			},
			SiteID: "site-a", Timestamp: createdAt.UnixMilli(),
		}
		data, _ := json.Marshal(evt)
		return data
	}

	t.Run("CreatedAt before cutoff is filtered (no actions, no error)", func(t *testing.T) {
		actions, err := coll.BuildAction(mkEvent(time.Date(2025, 12, 31, 23, 59, 59, 0, time.UTC)))
		require.NoError(t, err)
		assert.Empty(t, actions)
	})

	t.Run("CreatedAt exactly at cutoff is kept", func(t *testing.T) {
		actions, err := coll.BuildAction(mkEvent(cutoff))
		require.NoError(t, err)
		assert.Len(t, actions, 1)
	})

	t.Run("CreatedAt after cutoff is kept", func(t *testing.T) {
		actions, err := coll.BuildAction(mkEvent(time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)))
		require.NoError(t, err)
		assert.Len(t, actions, 1)
	})

	t.Run("zero cutoff disables filter — old data still indexed", func(t *testing.T) {
		uncapped := newMessageCollection("msgs-v1", time.Time{})
		actions, err := uncapped.BuildAction(mkEvent(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)))
		require.NoError(t, err)
		assert.Len(t, actions, 1)
	})
}
