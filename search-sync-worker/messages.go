package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/searchengine"
	"github.com/hmchangw/chat/pkg/searchindex"
	"github.com/hmchangw/chat/pkg/stream"
)

// messageCollection implements Collection for message search sync.
//
// syncFrom is the SYNC_MESSAGES_FROM cutoff (UTC); a zero value disables
// it. When set, BuildAction skips any event whose Message.CreatedAt
// predates the cutoff. This is the legacy-migration filter: a migrator
// replaying historical data into JetStream stamps evt.Timestamp at
// publish-time, so we compare against the message's own CreatedAt to
// reflect original data age. Only the messages collection has a cutoff —
// spotlight and user-room remain unfiltered so a user can still discover
// and search rooms they joined before the date.
type messageCollection struct {
	indexPrefix string
	syncFrom    time.Time
}

func newMessageCollection(indexPrefix string, syncFrom time.Time) *messageCollection {
	return &messageCollection{indexPrefix: indexPrefix, syncFrom: syncFrom}
}

func (c *messageCollection) StreamConfig(siteID string) jetstream.StreamConfig {
	cfg := stream.MessagesCanonical(siteID)
	return jetstream.StreamConfig{
		Name:     cfg.Name,
		Subjects: cfg.Subjects,
	}
}

func (c *messageCollection) ConsumerName() string {
	return "message-sync"
}

func (c *messageCollection) FilterSubjects(_ string) []string {
	// Stream has a single subject pattern — no extra filtering needed.
	return nil
}

func (c *messageCollection) TemplateName() string {
	return fmt.Sprintf("%s_template", searchindex.StripVersionBase(c.indexPrefix))
}

func (c *messageCollection) TemplateBody() json.RawMessage {
	return messageTemplateBody(c.indexPrefix)
}

func (c *messageCollection) BuildAction(data []byte) ([]searchengine.BulkAction, error) {
	var evt model.MessageEvent
	if err := json.Unmarshal(data, &evt); err != nil {
		return nil, fmt.Errorf("unmarshal message event: %w", err)
	}
	if evt.Message.ID == "" {
		return nil, fmt.Errorf("build message action: missing message id")
	}
	if evt.Message.CreatedAt.IsZero() {
		return nil, fmt.Errorf("build message action: missing createdAt")
	}
	if evt.Timestamp <= 0 {
		return nil, fmt.Errorf("build message action: missing timestamp")
	}
	if !c.syncFrom.IsZero() && evt.Message.CreatedAt.Before(c.syncFrom) {
		return nil, nil
	}
	return []searchengine.BulkAction{buildMessageAction(&evt, c.indexPrefix)}, nil
}

// --- Message-specific internals ---

// MessageSearchIndex defines the Elasticsearch document structure for messages.
// Fields mirror pkg/model.Message plus SiteID from the event envelope.
// The `es` struct tag drives the index template mapping via messageTemplateProperties():
//   - "keyword", "text", "date", "boolean" → ES field type
//   - "text,custom_analyzer" → text field with named analyzer
//
// When adding fields to Message (pkg/model), add them here with an `es` tag
// and populate them in newMessageSearchIndex(). The template auto-updates.
type MessageSearchIndex struct {
	MessageID             string     `json:"messageId"                              es:"keyword"`
	RoomID                string     `json:"roomId"                                 es:"keyword"`
	SiteID                string     `json:"siteId"                                 es:"keyword"`
	UserID                string     `json:"userId"                                 es:"keyword"`
	UserAccount           string     `json:"userAccount"                            es:"keyword"`
	Content               string     `json:"content,omitempty"                      es:"text,custom_analyzer"`
	CreatedAt             time.Time  `json:"createdAt"                              es:"date"`
	EditedAt              *time.Time `json:"editedAt,omitempty"                     es:"date"`
	UpdatedAt             *time.Time `json:"updatedAt,omitempty"                    es:"date"`
	ThreadParentID        string     `json:"threadParentMessageId,omitempty"        es:"keyword"`
	ThreadParentCreatedAt *time.Time `json:"threadParentMessageCreatedAt,omitempty" es:"date"`
	TShow                 bool       `json:"tshow,omitempty"                        es:"boolean"`
}

// newMessageSearchIndex maps a MessageEvent to a search index document.
func newMessageSearchIndex(evt *model.MessageEvent) MessageSearchIndex {
	return MessageSearchIndex{
		MessageID:             evt.Message.ID,
		RoomID:                evt.Message.RoomID,
		SiteID:                evt.SiteID,
		UserID:                evt.Message.UserID,
		UserAccount:           evt.Message.UserAccount,
		Content:               evt.Message.Content,
		CreatedAt:             evt.Message.CreatedAt,
		EditedAt:              evt.Message.EditedAt,
		UpdatedAt:             evt.Message.UpdatedAt,
		ThreadParentID:        evt.Message.ThreadParentMessageID,
		ThreadParentCreatedAt: evt.Message.ThreadParentMessageCreatedAt,
		TShow:                 evt.Message.TShow,
	}
}

func indexName(prefix string, createdAt time.Time) string {
	return fmt.Sprintf("%s-%s", prefix, createdAt.UTC().Format("2006-01"))
}

func buildMessageAction(evt *model.MessageEvent, indexPrefix string) searchengine.BulkAction {
	index := indexName(indexPrefix, evt.Message.CreatedAt)

	// Only an explicit EventDeleted removes the doc; created/updated (and any
	// unstamped legacy/replayed event) take the index upsert path.
	if evt.Event == model.EventDeleted {
		return searchengine.BulkAction{
			Action:  searchengine.ActionDelete,
			Index:   index,
			DocID:   evt.Message.ID,
			Version: evt.Timestamp,
		}
	}

	doc := buildDocument(evt)
	return searchengine.BulkAction{
		Action:  searchengine.ActionIndex,
		Index:   index,
		DocID:   evt.Message.ID,
		Version: evt.Timestamp,
		Doc:     doc,
	}
}

func buildDocument(evt *model.MessageEvent) json.RawMessage {
	doc := newMessageSearchIndex(evt)
	data, _ := json.Marshal(doc)
	return data
}

// messageTemplateProperties generates ES mapping properties from
// MessageSearchIndex struct tags. The `es` tag is the source of truth.
func messageTemplateProperties() map[string]any {
	return esPropertiesFromStruct[MessageSearchIndex]()
}

func messageTemplateBody(prefix string) json.RawMessage {
	tmpl := map[string]any{
		"index_patterns": []string{fmt.Sprintf("%s-*", searchindex.StripVersionBase(prefix))},
		"template": map[string]any{
			"settings": map[string]any{
				"index": map[string]any{
					"number_of_shards":   4,
					"number_of_replicas": 2,
					"refresh_interval":   "30s",
				},
				"analysis": map[string]any{
					"analyzer": map[string]any{
						"custom_analyzer": map[string]any{
							"type":        "custom",
							"tokenizer":   "underscore_preserving",
							"filter":      []string{"underscore_subword", "cjk_bigram", "lowercase"},
							"char_filter": []string{"html_strip"},
						},
					},
					"tokenizer": map[string]any{
						"underscore_preserving": map[string]any{
							"type":    "pattern",
							"pattern": `[\s,;!?()\[\]{}"'<>]+`,
						},
					},
					"filter": map[string]any{
						"underscore_subword": map[string]any{
							"type":                 "word_delimiter_graph",
							"split_on_case_change": false,
							"split_on_numerics":    false,
							"preserve_original":    true,
						},
					},
				},
			},
			"mappings": map[string]any{
				"dynamic":    false,
				"properties": messageTemplateProperties(),
			},
		},
	}
	data, _ := json.Marshal(tmpl)
	return data
}
