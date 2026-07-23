package main

import (
	"encoding/json"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/searchengine"
	"github.com/hmchangw/chat/pkg/searchindex"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/teamsmigrate"
)

// teamsMigrationCollection indexes migrated Teams-history messages. message-worker
// persists each batch without re-publishing a .created event (the silent migration),
// so this dedicated consumer over the .teams.batch subject is the only path that
// indexes migrated content. It derives the author key straight from the Teams graph
// id hash — the same _id the migration writes — so it needs no Mongo lookup. It shares
// the message index (template, mapping, MessageSearchIndex doc) with messageCollection.
type teamsMigrationCollection struct {
	indexPrefix string
	siteID      string
	devMode     bool
}

func newTeamsMigrationCollection(indexPrefix, siteID string, devMode bool) *teamsMigrationCollection {
	return &teamsMigrationCollection{indexPrefix: indexPrefix, siteID: siteID, devMode: devMode}
}

// Same MESSAGES_CANONICAL stream as messageCollection; only the subject filter differs.
func (c *teamsMigrationCollection) StreamConfig(siteID string) jetstream.StreamConfig {
	return userMessagesStreamCfg(siteID)
}

// A distinct durable so its cursor is independent of the .created (message-sync) consumer.
func (c *teamsMigrationCollection) ConsumerName() string { return "message-sync-teams" }

// Only .teams.batch — the mirror of messageCollection's .* which excludes it.
func (c *teamsMigrationCollection) FilterSubjects(siteID string) []string {
	return []string{subject.MsgCanonicalTeamsBatch(siteID)}
}

func (c *teamsMigrationCollection) TemplateName() string {
	return fmt.Sprintf("%s_template", searchindex.StripVersionBase(c.indexPrefix))
}

func (c *teamsMigrationCollection) TemplateBody() json.RawMessage {
	return messageTemplateBody(c.indexPrefix, c.devMode)
}

func (c *teamsMigrationCollection) StoredScripts() map[string]json.RawMessage { return nil }

// BuildAction fans one batch out into one index action per migrated message. It mirrors
// the migration consumer's skips (no id / no roomId can't be addressed idempotently;
// system messages carry no indexable content) so the index matches what was persisted.
func (c *teamsMigrationCollection) BuildAction(data []byte) ([]searchengine.BulkAction, error) {
	var req model.TeamsBatchRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, fmt.Errorf("unmarshal teams batch: %w", err)
	}
	actions := make([]searchengine.BulkAction, 0, len(req.Messages))
	for _, raw := range req.Messages {
		var tm teamsmigrate.Message
		if err := json.Unmarshal(raw, &tm); err != nil {
			continue // one malformed record must not drop its valid siblings
		}
		if tm.ID == "" || tm.RoomID == "" || tm.CreatedDateTime.IsZero() {
			continue // can't address idempotently / no index bucket
		}
		if teamsmigrate.MessageType(tm.MessageType) != "" {
			continue // system message — not indexed content
		}
		// UserID is the exact author key: the same employeeId hash the migration writes as
		// the user's _id. UserAccount reuses it best-effort (no UPN at the message layer).
		empID := teamsmigrate.EmployeeIDFromGraphID(tm.From.ID)
		doc := MessageSearchIndex{
			MessageID:   teamsmigrate.DeterministicMessageID(tm.RoomID, tm.ID),
			RoomID:      tm.RoomID,
			SiteID:      c.siteID,
			UserID:      empID,
			UserAccount: empID,
			Content:     teamsmigrate.BodyToContent(tm.Body),
			CreatedAt:   tm.CreatedDateTime,
		}
		body, _ := json.Marshal(doc)
		actions = append(actions, searchengine.BulkAction{
			Action: searchengine.ActionIndex,
			Index:  indexName(c.indexPrefix, tm.CreatedDateTime),
			DocID:  doc.MessageID,
			// Deterministic id + createdAt as the external version make a batch replay
			// idempotent (a re-index of the same doc 409s, handled as success).
			Version: tm.CreatedDateTime.UnixNano(),
			Doc:     body,
		})
	}
	return actions, nil
}
