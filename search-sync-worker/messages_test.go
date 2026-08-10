package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/searchengine"
	"github.com/hmchangw/chat/pkg/searchindex"
	"github.com/hmchangw/chat/pkg/teamsmigrate"
)

func TestMessageCollection_TemplateName_StripsVersion(t *testing.T) {
	coll := newMessageCollection("messages-site1-v1", "site-a", time.Time{}, false)
	assert.Equal(t, "messages-site1_template", coll.TemplateName())
}

func TestMessageCollection_TemplateName_BareBaseFallback(t *testing.T) {
	coll := newMessageCollection("messages-site1", "site-a", time.Time{}, false)
	assert.Equal(t, "messages-site1_template", coll.TemplateName())
}

func TestMessageCollection_TemplateBody_PatternStripsVersion(t *testing.T) {
	coll := newMessageCollection("messages-site1-v1", "site-a", time.Time{}, false)
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
	coll := newMessageCollection("msgs-v1", "site-a", time.Time{}, false)
	cfg := coll.StreamConfig("site-a")
	assert.Equal(t, "MESSAGES-CANONICAL-site-a", cfg.Name)
}

func TestMessageCollection_ConsumerName(t *testing.T) {
	coll := newMessageCollection("msgs-v1", "site-a", time.Time{}, false)
	assert.Equal(t, "message-sync", coll.ConsumerName())
}

func TestMessageCollection_StoredScripts(t *testing.T) {
	coll := newMessageCollection("msgs-v1", "site-a", time.Time{}, false)
	assert.Empty(t, coll.StoredScripts(), "messages collection uses no stored scripts")
}

// Templates apply only to new indices, so existing monthly indices need the
// additive mapping update or new fields stay unmapped until rollover.
func TestMessageCollection_MappingUpdate(t *testing.T) {
	coll := newMessageCollection("messages-site1-v1", "site-a", time.Time{}, false)
	pattern, body := coll.MappingUpdate()
	assert.Equal(t, "messages-site1-*", pattern, "pattern must strip the version suffix like the template's index_patterns")
	require.NotNil(t, body)

	var parsed struct {
		Properties map[string]any `json:"properties"`
	}
	require.NoError(t, json.Unmarshal(body, &parsed))
	assert.Contains(t, parsed.Properties, "attachmentText")
	assert.Contains(t, parsed.Properties, "cardData")
	assert.Contains(t, parsed.Properties, "content", "full property set keeps the update idempotent")

	// Render payloads are stored but never indexed: object + enabled:false.
	for _, key := range []string{"attachments", "card"} {
		prop, ok := parsed.Properties[key].(map[string]any)
		require.True(t, ok, "%s must be mapped", key)
		assert.Equal(t, "object", prop["type"], key)
		assert.Equal(t, false, prop["enabled"], "%s must not be indexed", key)
	}
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
			got := searchindex.MessageIndexName(tt.prefix, tt.createdAt)
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
	doc, err := newMessageSearchIndex(evt)
	require.NoError(t, err)
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
	doc, err := newMessageSearchIndex(evt)
	require.NoError(t, err)
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
	doc, err := newMessageSearchIndex(evt)
	require.NoError(t, err)
	assert.False(t, doc.TShow)

	data, err := json.Marshal(doc)
	require.NoError(t, err)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))
	_, present := raw["tshow"]
	assert.False(t, present, "tshow should be omitted when false")
}

func TestMessageCollection_BuildAction(t *testing.T) {
	coll := newMessageCollection("msgs-v1", "site-a", time.Time{}, false)
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

// TestMessageCollection_BuildAction_SystemMessagesNotIndexed: room-worker
// publishes membership/rename sys-messages to MESSAGES-CANONICAL via
// publishCanonical, so they arrive here like any other message. They are UI
// chrome, not searchable content — message-worker, notification-worker and
// history-service all gate on IsSystemMessageType already; this collection was
// the outlier that indexed them.
func TestMessageCollection_BuildAction_SystemMessagesNotIndexed(t *testing.T) {
	coll := newMessageCollection("msgs-v1", "site-a", time.Time{}, false)

	mkEvent := func(msgType string) []byte {
		evt := model.MessageEvent{
			Event: model.EventCreated,
			Message: model.Message{
				ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
				Type:      msgType,
				Content:   "hello",
				CreatedAt: time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC),
			},
			SiteID: "site-a", Timestamp: 100,
		}
		data, _ := json.Marshal(evt)
		return data
	}

	systemTypes := []string{
		model.MessageTypeRoomCreated,
		model.MessageTypeMembersAdded,
		model.MessageTypeMemberRemoved,
		model.MessageTypeMemberLeft,
		model.MessageTypeRoomRenamed,
		model.MessageTypeRoomRestricted,
		model.MessageTypeTeamsMeetStarted,
	}
	for _, st := range systemTypes {
		t.Run("system: "+st, func(t *testing.T) {
			actions, err := coll.BuildAction(mkEvent(st))
			require.NoError(t, err, "a system message is filtered, not an error")
			assert.Empty(t, actions)
		})
	}

	// A normal message has Type "" and an `important` message is client-set, not
	// system — both stay searchable.
	for _, keep := range []string{"", model.MessageTypeImportant} {
		t.Run("indexed: "+keep, func(t *testing.T) {
			actions, err := coll.BuildAction(mkEvent(keep))
			require.NoError(t, err)
			require.Len(t, actions, 1)
			assert.Equal(t, "m1", actions[0].DocID)
		})
	}
}

func TestMessageCollection_BuildAction_SyncFromFilter(t *testing.T) {
	cutoff := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	coll := newMessageCollection("msgs-v1", "site-a", cutoff, false)

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
		uncapped := newMessageCollection("msgs-v1", "site-a", time.Time{}, false)
		actions, err := uncapped.BuildAction(mkEvent(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)))
		require.NoError(t, err)
		assert.Len(t, actions, 1)
	})
}

// Slim (no-content) events must never upsert: pin/unpin would wipe indexed
// fields, and unpin-after-delete would resurrect a stub doc.
func TestMessageCollection_BuildAction_SlimEventsSkipped(t *testing.T) {
	coll := newMessageCollection("msgs-v1", "site-a", time.Time{}, false)

	mkEvent := func(eventType model.EventType) []byte {
		evt := model.MessageEvent{
			Event: eventType,
			Message: model.Message{
				ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
				CreatedAt: time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC),
			},
			SiteID: "site-a", Timestamp: 100,
		}
		data, err := json.Marshal(evt)
		require.NoError(t, err)
		return data
	}

	tests := []struct {
		name  string
		event model.EventType
	}{
		{"pinned skipped", model.EventPinned},
		{"unpinned skipped", model.EventUnpinned},
		{"thread_reply_added skipped", model.EventThreadReplyAdded},
		{"unknown future type skipped", model.EventType("archived")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actions, err := coll.BuildAction(mkEvent(tt.event))
			require.NoError(t, err)
			assert.Empty(t, actions, "event %q must not produce an ES action", tt.event)
		})
	}
}

func TestBuildDocument_AttachmentFields(t *testing.T) {
	ts := time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC)

	mkBlob := func(t *testing.T, a cassandra.Attachment) []byte {
		b, err := json.Marshal(a)
		require.NoError(t, err)
		return b
	}

	t.Run("searched projections and full render objects are indexed", func(t *testing.T) {
		evt := &model.MessageEvent{
			Event: model.EventCreated,
			Message: model.Message{
				ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
				Content: "see attached", CreatedAt: ts,
				Attachments: [][]byte{
					mkBlob(t, cassandra.Attachment{ID: "f1", Title: "q3-report.pdf", Description: "Quarterly numbers", FileType: "application/pdf", TitleLink: "api/v1/file/rooms/r1/file/f1"}),
					mkBlob(t, cassandra.Attachment{ID: "f2", Title: "team.png", FileType: "image/png"}),
				},
			},
			SiteID: "site-a", Timestamp: 100,
		}
		var doc map[string]any
		require.NoError(t, json.Unmarshal(buildDocument(evt), &doc))
		// One string pools every title+description so AND queries can mix
		// words from both (and across attachments of the same message).
		assert.Equal(t, "q3-report.pdf Quarterly numbers team.png", doc["attachmentText"])

		// The whole decoded objects ride along (render-only, never indexed)
		// so search hits can display attachments without a history lookup.
		atts, ok := doc["attachments"].([]any)
		require.True(t, ok, "attachments must be an array of full objects")
		require.Len(t, atts, 2)
		first := atts[0].(map[string]any)
		assert.Equal(t, "f1", first["id"])
		assert.Equal(t, "q3-report.pdf", first["title"])
		assert.Equal(t, "Quarterly numbers", first["description"])
		assert.Equal(t, "application/pdf", first["fileType"])
		assert.Equal(t, "api/v1/file/rooms/r1/file/f1", first["titleLink"])
	})

	t.Run("malformed blob is skipped, valid ones kept", func(t *testing.T) {
		evt := &model.MessageEvent{
			Event: model.EventCreated,
			Message: model.Message{
				ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
				Content: "x", CreatedAt: ts,
				Attachments: [][]byte{
					[]byte("{not json"),
					mkBlob(t, cassandra.Attachment{ID: "f1", Title: "ok.txt", FileType: "text/plain"}),
				},
			},
			SiteID: "site-a", Timestamp: 100,
		}
		var doc map[string]any
		require.NoError(t, json.Unmarshal(buildDocument(evt), &doc))
		assert.Equal(t, "ok.txt", doc["attachmentText"])
	})

	t.Run("no attachments omits the fields", func(t *testing.T) {
		evt := &model.MessageEvent{
			Event: model.EventCreated,
			Message: model.Message{
				ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
				Content: "x", CreatedAt: ts,
			},
			SiteID: "site-a", Timestamp: 100,
		}
		var doc map[string]any
		require.NoError(t, json.Unmarshal(buildDocument(evt), &doc))
		for _, key := range []string{"attachmentText", "attachments"} {
			_, present := doc[key]
			assert.False(t, present, "%s should be omitted when there are no attachments", key)
		}
	})
}

func TestBuildDocument_CardFields(t *testing.T) {
	ts := time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC)

	t.Run("card template and stringified card data are indexed", func(t *testing.T) {
		data := `{"type":"AdaptiveCard","body":[{"type":"TextBlock","text":"Expense request from Bob"},{"title":"Amount","value":"$120"}]}`
		evt := &model.MessageEvent{
			Event: model.EventCreated,
			Message: model.Message{
				ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
				CreatedAt: ts,
				Card: &cassandra.Card{
					Template: "expense-approval-v1",
					Data:     []byte(data),
				},
				CardAction: &cassandra.CardAction{
					Verb: "approve", Text: "Approve the expense", DisplayText: "Bob approved",
				},
			},
			SiteID: "site-a", Timestamp: 100,
		}
		var doc map[string]any
		require.NoError(t, json.Unmarshal(buildDocument(evt), &doc))
		assert.Equal(t, data, doc["cardData"], "card data is indexed verbatim as text")

		// The card object rides along as-is (render-only) — template + data,
		// same wire shape as history reads ([]byte data → base64 string).
		card, ok := doc["card"].(map[string]any)
		require.True(t, ok, "card must be the full object")
		assert.Equal(t, "expense-approval-v1", card["template"])
		assert.Equal(t, base64.StdEncoding.EncodeToString([]byte(data)), card["data"])
	})

	t.Run("no card omits the fields", func(t *testing.T) {
		evt := &model.MessageEvent{
			Event: model.EventCreated,
			Message: model.Message{
				ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
				Content: "x", CreatedAt: ts,
			},
			SiteID: "site-a", Timestamp: 100,
		}
		var doc map[string]any
		require.NoError(t, json.Unmarshal(buildDocument(evt), &doc))
		for _, key := range []string{"card", "cardData"} {
			_, present := doc[key]
			assert.False(t, present, "%s should be omitted when there is no card", key)
		}
	})

	t.Run("card with empty data carries the object but no cardData", func(t *testing.T) {
		evt := &model.MessageEvent{
			Event: model.EventCreated,
			Message: model.Message{
				ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
				CreatedAt: ts,
				Card:      &cassandra.Card{Template: "welcome-v1"},
			},
			SiteID: "site-a", Timestamp: 100,
		}
		var doc map[string]any
		require.NoError(t, json.Unmarshal(buildDocument(evt), &doc))
		card, ok := doc["card"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "welcome-v1", card["template"])
		_, present := doc["cardData"]
		assert.False(t, present, "empty card data should be omitted")
	})
}

func TestMessageCollection_BuildAction_ReactedSkipped(t *testing.T) {
	coll := newMessageCollection("msgs-v1", "site-a", time.Time{}, false)
	evt := model.MessageEvent{
		Event: model.EventReacted,
		Message: model.Message{
			ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
			CreatedAt: time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC),
		},
		SiteID: "site-a", Timestamp: 100,
		ReactionDelta: &model.ReactionDelta{
			Shortcode: "thumbsup", Action: "added",
			Actor: model.Participant{Account: "bob"},
		},
	}
	data, _ := json.Marshal(evt)
	actions, err := coll.BuildAction(data)
	require.NoError(t, err)
	assert.Empty(t, actions, "reactions must not produce an ES action — content is unchanged")
}

// --- Teams-batch indexing (folded from the retired teamsMigrationCollection) ---

func teamsBatch(t *testing.T, msgs ...teamsmigrate.Message) []byte {
	t.Helper()
	raws := make([]json.RawMessage, 0, len(msgs))
	for i := range msgs {
		b, err := json.Marshal(msgs[i])
		require.NoError(t, err)
		raws = append(raws, b)
	}
	data, err := json.Marshal(model.TeamsBatchRequest{Messages: raws})
	require.NoError(t, err)
	return data
}

func TestMessageCollection_FilterSubjects(t *testing.T) {
	// The teams batch subject now lives on its own MESSAGES-TEAMS stream/consumer —
	// neither the user nor the bot collection binds it anymore.
	user := newMessageCollection("msgs-v1", "site-a", time.Time{}, false)
	assert.Equal(t, []string{"chat.msg.canonical.site-a.*"}, user.FilterSubjects("site-a"))

	bot := newBotMessageCollection("msgs-v1", false)
	assert.Equal(t, []string{"chat.msg.canonical.site-a.*"}, bot.FilterSubjects("site-a"))

	teams := newTeamsMessageCollection("msgs-v1", "site-a", false)
	assert.Equal(t, []string{"chat.teams.msg.canonical.site-a.batch"}, teams.FilterSubjects("site-a"))
	assert.Empty(t, teams.TemplateName(), "teams-only collection skips template — the user collection already owns it")
	assert.Nil(t, teams.TemplateBody())
	pattern, body := teams.MappingUpdate()
	assert.Empty(t, pattern)
	assert.Nil(t, body)
}

// fakeTeamsUserResolver returns a fixed id→identity map (nil error).
type fakeTeamsUserResolver map[string]teamsIdentity

func (f fakeTeamsUserResolver) ResolveIdentities(_ context.Context, ids []string) (map[string]teamsIdentity, error) {
	out := make(map[string]teamsIdentity, len(ids))
	for _, id := range ids {
		if v, ok := f[id]; ok {
			out[id] = v
		}
	}
	return out, nil
}

func TestMessageCollection_BuildAction_TeamsBatch(t *testing.T) {
	c := newMessageCollection("messages-site-a-v1", "site-a", time.Time{}, false)
	c.teamsUsers = fakeTeamsUserResolver{
		"graph-1": {Account: "alice", UserID: "uid-alice"},
		"graph-2": {Account: "bob", UserID: "uid-bob"},
	}
	ts := time.Date(2026, 7, 20, 10, 0, 0, 0, time.UTC)

	data := teamsBatch(t,
		teamsmigrate.Message{
			ID: "tm-1", RoomID: "room-1", MessageType: "message",
			From: teamsmigrate.User{ID: "graph-1"},
			Body: teamsmigrate.Body{ContentType: "text", Content: "one"}, CreatedDateTime: ts,
		},
		teamsmigrate.Message{
			ID: "tm-2", RoomID: "room-1", MessageType: "message",
			From: teamsmigrate.User{ID: "graph-2"},
			Body: teamsmigrate.Body{ContentType: "html", Content: "<b>two</b>"}, CreatedDateTime: ts,
		},
	)
	actions, err := c.BuildAction(data)
	require.NoError(t, err)
	require.Len(t, actions, 2)

	assert.Equal(t, teamsmigrate.DeterministicMessageID("room-1", "tm-1"), actions[0].DocID)

	var doc searchindex.MessageDoc
	require.NoError(t, json.Unmarshal(actions[0].Doc, &doc))
	assert.Equal(t, "uid-alice", doc.UserID)  // account-resolved user _id
	assert.Equal(t, "alice", doc.UserAccount) // account from teams_user
	assert.Equal(t, "room-1", doc.RoomID)
	assert.Equal(t, "site-a", doc.SiteID)
	assert.Equal(t, "one", doc.Content)
	assert.Equal(t, ts, doc.CreatedAt)

	var doc2 searchindex.MessageDoc
	require.NoError(t, json.Unmarshal(actions[1].Doc, &doc2))
	assert.Equal(t, "**two**", doc2.Content) // html body renders to markdown
}

func TestMessageCollection_BuildAction_TeamsBatch_Skips(t *testing.T) {
	c := newMessageCollection("messages-site-a-v1", "site-a", time.Time{}, false)
	ts := time.Now().UTC()

	data := teamsBatch(t,
		teamsmigrate.Message{ID: "", RoomID: "room-1", MessageType: "message", CreatedDateTime: ts},                                       // no id
		teamsmigrate.Message{ID: "tm-2", RoomID: "", MessageType: "message", CreatedDateTime: ts},                                         // no roomId
		teamsmigrate.Message{ID: "tm-3", RoomID: "room-1", MessageType: "systemEventMessage", CreatedDateTime: ts},                        // system
		teamsmigrate.Message{ID: "tm-4", RoomID: "room-1", MessageType: "message", From: teamsmigrate.User{ID: "g"}, CreatedDateTime: ts}, // kept
	)
	actions, err := c.BuildAction(data)
	require.NoError(t, err)
	require.Len(t, actions, 1)
	assert.Equal(t, teamsmigrate.DeterministicMessageID("room-1", "tm-4"), actions[0].DocID)
}

func TestMessageCollection_BuildAction_TeamsBatch_MalformedRecordDoesNotDropSiblings(t *testing.T) {
	c := newMessageCollection("messages-site-a-v1", "site-a", time.Time{}, false)
	ts := time.Now().UTC()

	valid, err := json.Marshal(teamsmigrate.Message{
		ID: "tm-1", RoomID: "room-1", MessageType: "message",
		From: teamsmigrate.User{ID: "g"}, CreatedDateTime: ts,
	})
	require.NoError(t, err)

	req := model.TeamsBatchRequest{Messages: []json.RawMessage{
		json.RawMessage("123"), // valid JSON syntax, wrong shape — fails to unmarshal into teamsmigrate.Message
		valid,
	}}
	data, err := json.Marshal(req)
	require.NoError(t, err)

	actions, buildErr := c.BuildAction(data)
	require.NoError(t, buildErr)
	require.Len(t, actions, 1, "the malformed record must be skipped, not abort the whole batch")
	assert.Equal(t, teamsmigrate.DeterministicMessageID("room-1", "tm-1"), actions[0].DocID)
}
