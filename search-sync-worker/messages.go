package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/searchengine"
	"github.com/hmchangw/chat/pkg/searchindex"
	"github.com/hmchangw/chat/pkg/stream"
)

// parentCreatedAtResolver resolves a thread parent's authoritative createdAt; ok=false leaves the field unset. Never errors. Satisfied by *esParentResolver.
type parentCreatedAtResolver interface {
	ResolveParentCreatedAt(ctx context.Context, messageID string) (time.Time, bool)
}

// messageCollection implements Collection for message search sync; streamCfg + consumerName are
// parameterized so one type consumes user or bot canonical streams. syncFrom is the legacy-replay cutoff (zero disables it).
type messageCollection struct {
	indexPrefix    string
	syncFrom       time.Time
	devMode        bool
	streamCfg      func(siteID string) jetstream.StreamConfig
	consumerName   string
	parentResolver parentCreatedAtResolver
}

// newMessageCollection binds to the user MESSAGES_CANONICAL stream.
func newMessageCollection(indexPrefix string, syncFrom time.Time, devMode bool) *messageCollection {
	return &messageCollection{
		indexPrefix:  indexPrefix,
		syncFrom:     syncFrom,
		devMode:      devMode,
		streamCfg:    userMessagesStreamCfg,
		consumerName: "message-sync",
	}
}

// newBotMessageCollection binds to BOT_MESSAGES_CANONICAL and shares BuildAction with the user flow.
func newBotMessageCollection(indexPrefix string, devMode bool) *messageCollection {
	return &messageCollection{
		indexPrefix:  indexPrefix,
		devMode:      devMode,
		streamCfg:    botMessagesStreamCfg,
		consumerName: "bot-message-sync",
	}
}

func userMessagesStreamCfg(siteID string) jetstream.StreamConfig {
	cfg := stream.MessagesCanonical(siteID)
	return jetstream.StreamConfig{Name: cfg.Name, Subjects: cfg.Subjects}
}

func botMessagesStreamCfg(siteID string) jetstream.StreamConfig {
	cfg := stream.BotMessagesCanonical(siteID)
	return jetstream.StreamConfig{Name: cfg.Name, Subjects: cfg.Subjects}
}

func (c *messageCollection) StreamConfig(siteID string) jetstream.StreamConfig {
	return c.streamCfg(siteID)
}

func (c *messageCollection) ConsumerName() string {
	return c.consumerName
}

func (c *messageCollection) FilterSubjects(_ string) []string {
	// Stream has a single subject pattern — no extra filtering needed.
	return nil
}

func (c *messageCollection) TemplateName() string {
	return fmt.Sprintf("%s_template", searchindex.StripVersionBase(c.indexPrefix))
}

func (c *messageCollection) TemplateBody() json.RawMessage {
	return messageTemplateBody(c.indexPrefix, c.devMode)
}

// StoredScripts returns nil — message indexing uses plain index/delete bulk actions with no painless scripts.
func (c *messageCollection) StoredScripts() map[string]json.RawMessage {
	return nil
}

// MappingUpdate pushes the full (idempotent) property set onto existing
// monthly indices; the same pattern the template targets.
func (c *messageCollection) MappingUpdate() (string, json.RawMessage) {
	// Error discarded: input is a static map of literals, marshal cannot fail.
	body, _ := json.Marshal(map[string]any{"properties": messageTemplateProperties()})
	return searchindex.IndexPattern(c.indexPrefix), body
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
	// Slim events (reacted/pinned/unpinned/…) carry no content: upserting them
	// would wipe indexed fields or resurrect deleted docs. "" = legacy created.
	if !actionableEvent(evt.Event) {
		return nil, nil
	}
	c.resolveThreadParentCreatedAt(&evt)
	return []searchengine.BulkAction{buildMessageAction(&evt, c.indexPrefix)}, nil
}

// resolveThreadParentCreatedAt fills the parent createdAt for a thread reply; the gatekeeper's
// value wins when present, else re-resolve from the ES index. No-op for nil resolver/non-thread/delete.
func (c *messageCollection) resolveThreadParentCreatedAt(evt *model.MessageEvent) {
	if c.parentResolver == nil || evt.Message.ThreadParentMessageID == "" || evt.Event == model.EventDeleted {
		return
	}
	if evt.Message.ThreadParentMessageCreatedAt != nil {
		return
	}
	if createdAt, ok := c.parentResolver.ResolveParentCreatedAt(context.Background(), evt.Message.ThreadParentMessageID); ok {
		evt.Message.ThreadParentMessageCreatedAt = &createdAt
	}
}

// --- Message-specific internals ---

// actionableEvent reports whether an event type produces a bulk action at
// all — index/replace (created, updated, legacy "") or delete (deleted).
func actionableEvent(e model.EventType) bool {
	switch e {
	case model.EventCreated, model.EventUpdated, model.EventDeleted, "":
		return true
	default:
		return false
	}
}

// MessageSearchIndex is the Elasticsearch document for messages, mirroring pkg/model.Message; the
// `es` tag (keyword/text/date/boolean, text,custom_analyzer, or object_disabled) drives the template — add new Message fields here and populate them in newMessageSearchIndex().
type MessageSearchIndex struct {
	MessageID   string `json:"messageId"                              es:"keyword"`
	RoomID      string `json:"roomId"                                 es:"keyword"`
	SiteID      string `json:"siteId"                                 es:"keyword"`
	UserID      string `json:"userId"                                 es:"keyword"`
	UserAccount string `json:"userAccount"                            es:"keyword"`
	// IsBot flags bot-authored messages so search-service can filter/facet by source.
	IsBot                 bool       `json:"isBot,omitempty"                        es:"boolean"`
	Content               string     `json:"content,omitempty"                      es:"text,custom_analyzer"`
	CreatedAt             time.Time  `json:"createdAt"                              es:"date"`
	EditedAt              *time.Time `json:"editedAt,omitempty"                     es:"date"`
	UpdatedAt             *time.Time `json:"updatedAt,omitempty"                    es:"date"`
	ThreadParentID        string     `json:"threadParentMessageId,omitempty"        es:"keyword"`
	ThreadParentCreatedAt *time.Time `json:"threadParentMessageCreatedAt,omitempty" es:"date"`
	TShow                 bool       `json:"tshow,omitempty"                        es:"boolean"`

	// Searched attachment/tcard projections. AttachmentText is one string —
	// every attachment title+description joined — so an AND query can mix
	// words from both. CardData duplicates card.data — accepted.
	AttachmentText string `json:"attachmentText,omitempty" es:"text,custom_analyzer"`
	CardData       string `json:"cardData,omitempty"       es:"text,custom_analyzer"`

	// Render payloads stored as-is (never indexed) so search hits can be
	// rendered on the frontend without a history-service lookup.
	Attachments []cassandra.Attachment `json:"attachments,omitempty" es:"object_disabled"`
	Card        *cassandra.Card        `json:"card,omitempty"        es:"object_disabled"`
}

// newMessageSearchIndex maps a MessageEvent to a search index document.
func newMessageSearchIndex(evt *model.MessageEvent) MessageSearchIndex {
	doc := MessageSearchIndex{
		MessageID:             evt.Message.ID,
		RoomID:                evt.Message.RoomID,
		SiteID:                evt.SiteID,
		UserID:                evt.Message.UserID,
		UserAccount:           evt.Message.UserAccount,
		IsBot:                 model.IsBot(evt.Message.UserAccount),
		Content:               evt.Message.Content,
		CreatedAt:             evt.Message.CreatedAt,
		EditedAt:              evt.Message.EditedAt,
		UpdatedAt:             evt.Message.UpdatedAt,
		ThreadParentID:        evt.Message.ThreadParentMessageID,
		ThreadParentCreatedAt: evt.Message.ThreadParentMessageCreatedAt,
		TShow:                 evt.Message.TShow,
	}

	// Lenient decode: a malformed blob is skipped by DecodeAttachments; one
	// bad attachment must not block indexing the rest of the message.
	attachments, _ := cassandra.DecodeAttachments(evt.Message.Attachments)
	doc.Attachments = attachments
	var attachmentText []string
	for i := range attachments {
		a := &attachments[i]
		if a.Title != "" {
			attachmentText = append(attachmentText, a.Title)
		}
		if a.Description != "" {
			attachmentText = append(attachmentText, a.Description)
		}
	}
	doc.AttachmentText = strings.Join(attachmentText, " ")

	if evt.Message.Card != nil {
		doc.Card = evt.Message.Card
		doc.CardData = string(evt.Message.Card.Data)
	}

	return doc
}

func indexName(prefix string, createdAt time.Time) string {
	return fmt.Sprintf("%s-%s", prefix, createdAt.UTC().Format("2006-01"))
}

func buildMessageAction(evt *model.MessageEvent, indexPrefix string) searchengine.BulkAction {
	index := indexName(indexPrefix, evt.Message.CreatedAt)

	// Only an explicit EventDeleted removes the doc; created/updated (and any unstamped legacy/replayed event) take the index upsert path.
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

// messageTemplateProperties generates ES mapping properties from MessageSearchIndex struct tags. The `es` tag is the source of truth.
func messageTemplateProperties() map[string]any {
	return esPropertiesFromStruct[MessageSearchIndex]()
}

func messageTemplateBody(prefix string, devMode bool) json.RawMessage {
	shards := 4
	replicas := 2
	if devMode {
		shards = 1
		replicas = 0
	}
	tmpl := map[string]any{
		"index_patterns": []string{searchindex.IndexPattern(prefix)},
		"template": map[string]any{
			"settings": map[string]any{
				"index": map[string]any{
					"number_of_shards":   shards,
					"number_of_replicas": replicas,
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
