package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/searchengine"
	"github.com/hmchangw/chat/pkg/searchindex"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/teamsmigrate"
)

// parentCreatedAtResolver resolves a thread parent's authoritative createdAt; ok=false leaves the field unset. Never errors. Satisfied by *esParentResolver.
type parentCreatedAtResolver interface {
	ResolveParentCreatedAt(ctx context.Context, messageID string) (time.Time, bool)
}

// teamsIdentity is a migrated sender's nextgen identity for the search index: the
// account (from teams_user) and the user _id (from the account's users row, empty
// until message-worker has created it).
type teamsIdentity struct {
	Account string
	UserID  string
}

// teamsUserResolver batch-maps AAD user ids → teamsIdentity for indexing migrated Teams
// history. Ids with no teams_user record are omitted. Satisfied by *mongoTeamsUserResolver.
type teamsUserResolver interface {
	ResolveIdentities(ctx context.Context, teamsUserIDs []string) (map[string]teamsIdentity, error)
}

// messageCollection implements Collection for message search sync; streamCfg + consumerName are
// parameterized so one type consumes user, bot, or Teams-migration canonical streams. syncFrom is
// the legacy-replay cutoff (zero disables it).
type messageCollection struct {
	indexPrefix string
	siteID      string // for .teams.batch index docs (the normal path reads evt.SiteID per message)
	syncFrom    time.Time
	devMode     bool
	// teamsOnly switches this collection to the Teams-migration-only wiring: it binds
	// MESSAGES-TEAMS instead of a canonical stream, filters to only the teams batch
	// subject, and skips the template/mapping pushes the primary (user) collection
	// already owns for the same indexPrefix.
	teamsOnly      bool
	streamCfg      func(siteID string) jetstream.StreamConfig
	consumerName   string
	parentResolver parentCreatedAtResolver
	// teamsUsers resolves migrated senders' account + user _id; nil on collections that
	// don't index Teams batches (bot) and in tests that don't exercise the teams path.
	teamsUsers teamsUserResolver
}

// newMessageCollection binds to the user MESSAGES-CANONICAL stream (live messages only —
// migrated Teams history now indexes off its own consumer, see newTeamsMessageCollection).
func newMessageCollection(indexPrefix, siteID string, syncFrom time.Time, devMode bool) *messageCollection {
	return &messageCollection{
		indexPrefix:  indexPrefix,
		siteID:       siteID,
		syncFrom:     syncFrom,
		devMode:      devMode,
		streamCfg:    userMessagesStreamCfg,
		consumerName: "message-sync",
	}
}

// newBotMessageCollection binds to BOT-MESSAGES-CANONICAL and shares BuildAction with the user flow.
func newBotMessageCollection(indexPrefix string, devMode bool) *messageCollection {
	return &messageCollection{
		indexPrefix:  indexPrefix,
		devMode:      devMode,
		streamCfg:    botMessagesStreamCfg,
		consumerName: "bot-message-sync",
	}
}

// newTeamsMessageCollection binds to MESSAGES-TEAMS (message-worker's teams mode
// persists the batch with no .created event on the canonical stream, so this is a
// separate consumer, not a filter on the user collection). teamsOnly switches
// BuildAction to decode only the batch shape, and skips the template/mapping pushes
// the user collection already performs for the same indexPrefix.
func newTeamsMessageCollection(indexPrefix, siteID string, devMode bool) *messageCollection {
	return &messageCollection{
		indexPrefix:  indexPrefix,
		siteID:       siteID,
		devMode:      devMode,
		teamsOnly:    true,
		streamCfg:    teamsMessagesStreamCfg,
		consumerName: "message-sync-teams",
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

func teamsMessagesStreamCfg(siteID string) jetstream.StreamConfig {
	cfg := stream.MessagesTeams(siteID)
	return jetstream.StreamConfig{Name: cfg.Name, Subjects: cfg.Subjects}
}

func (c *messageCollection) StreamConfig(siteID string) jetstream.StreamConfig {
	return c.streamCfg(siteID)
}

func (c *messageCollection) ConsumerName() string {
	return c.consumerName
}

func (c *messageCollection) FilterSubjects(siteID string) []string {
	if c.teamsOnly {
		// Own stream (MESSAGES-TEAMS), own subject — no message wildcard here.
		return []string{subject.MsgTeamsCanonicalBatch(siteID)}
	}
	// Single-token message events (created/updated/deleted/...); the teams batch
	// subject now lives on a separate stream (see newTeamsMessageCollection).
	return []string{subject.MsgCanonicalMessageWildcard(siteID)}
}

func (c *messageCollection) TemplateName() string {
	if c.teamsOnly {
		// The user collection already owns/pushes the template for this indexPrefix.
		return ""
	}
	return searchindex.MessageTemplateName(c.indexPrefix)
}

func (c *messageCollection) TemplateBody() json.RawMessage {
	if c.teamsOnly {
		return nil
	}
	return searchindex.MessageTemplateBody(c.indexPrefix, c.devMode)
}

// StoredScripts returns nil — message indexing uses plain index/delete bulk actions with no painless scripts.
func (c *messageCollection) StoredScripts() map[string]json.RawMessage {
	return nil
}

// MappingUpdate pushes the full (idempotent) property set onto existing
// monthly indices; the same pattern the template targets. Skipped for the teams-only
// collection — the user collection already pushes it for the same indexPrefix.
func (c *messageCollection) MappingUpdate() (string, json.RawMessage) {
	if c.teamsOnly {
		return "", nil
	}
	// Error discarded: input is a static map of literals, marshal cannot fail.
	body, _ := json.Marshal(map[string]any{"properties": searchindex.EsPropertiesFromStruct[searchindex.MessageDoc]()})
	return searchindex.IndexPattern(c.indexPrefix), body
}

func (c *messageCollection) BuildAction(data []byte) ([]searchengine.BulkAction, error) {
	// Each collection consumes a single payload shape by mode: the teams-only
	// collection binds MESSAGES-TEAMS (batch envelopes), every other binds a
	// canonical stream (single MessageEvents). Decode exactly that shape and fail
	// loud on a mismatch rather than silently trying the other.
	if c.teamsOnly {
		var batch model.TeamsBatchRequest
		if err := json.Unmarshal(data, &batch); err != nil {
			return nil, fmt.Errorf("teams batch: expected TeamsBatchRequest: %w", err)
		}
		if len(batch.Messages) == 0 {
			return nil, fmt.Errorf("teams batch: expected TeamsBatchRequest with messages, got empty/other shape")
		}
		return c.buildTeamsActions(batch), nil
	}

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
	// Sys-messages (membership/rename) arrive on MESSAGES-CANONICAL like any
	// other message but are UI chrome, not searchable content. Filtered before
	// the parent lookup; `important` is client-set, not system, and stays.
	if model.IsSystemMessageType(evt.Message.Type) {
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

// buildTeamsActions fans one migrated-history batch out into one index action per
// message, mirroring the migration writer's skips (no id / no roomId can't be addressed
// idempotently; system messages carry no indexable content) so the index matches what
// was persisted. Sender account + user _id are batch-resolved via teams_user + users to
// match the same account-keyed identity message-worker persists.
func (c *messageCollection) buildTeamsActions(req model.TeamsBatchRequest) []searchengine.BulkAction {
	keeps := make([]teamsmigrate.Message, 0, len(req.Messages))
	idSet := make(map[string]struct{})
	for _, raw := range req.Messages {
		var tm teamsmigrate.Message
		if err := json.Unmarshal(raw, &tm); err != nil {
			// One malformed record must not drop its valid siblings, but a
			// batch that should only ever contain what the migration writer
			// itself persisted having an unparseable entry is unexpected —
			// surface it instead of vanishing silently.
			slog.Warn("skip teams migration record: unmarshal failed", "error", err)
			continue
		}
		if tm.ID == "" || tm.RoomID == "" || tm.CreatedDateTime.IsZero() {
			// The migration writer already filters these before persisting
			// (see model.TeamsBatchSkipped), so this shouldn't normally
			// trigger — log it as a signal that invariant was violated.
			slog.Warn("skip teams migration record: missing id/roomId/createdAt",
				"messageId", tm.ID, "roomId", tm.RoomID)
			continue // can't address idempotently / no index bucket
		}
		if teamsmigrate.MessageType(tm.MessageType) != "" {
			continue // system message — not indexed content, not a failure
		}
		keeps = append(keeps, tm)
		if tm.From.ID != "" {
			idSet[tm.From.ID] = struct{}{}
		}
	}

	identities := c.resolveTeamsIdentities(idSet)

	actions := make([]searchengine.BulkAction, 0, len(keeps))
	for i := range keeps {
		tm := &keeps[i]
		id := identities[tm.From.ID] // zero value (empty account/userID) when unresolved
		// Built directly rather than via searchindex.NewMessageDoc: that
		// constructor decodes cassandra.Message's attachment column, which
		// has no counterpart in teamsmigrate.Message.
		doc := searchindex.MessageDoc{
			MessageID:   teamsmigrate.DeterministicMessageID(tm.RoomID, tm.ID),
			RoomID:      tm.RoomID,
			SiteID:      c.siteID,
			UserID:      id.UserID,
			UserAccount: id.Account,
			Content:     teamsmigrate.BodyToContent(tm.Body),
			CreatedAt:   tm.CreatedDateTime,
			Origin:      model.OriginTeams,
		}
		body, _ := json.Marshal(doc)
		actions = append(actions, searchengine.BulkAction{
			Action: searchengine.ActionIndex,
			Index:  searchindex.MessageIndexName(c.indexPrefix, tm.CreatedDateTime),
			DocID:  doc.MessageID,
			// Deterministic id + createdAt as the external version make a batch replay
			// idempotent (a re-index of the same doc 409s, handled as success).
			Version: tm.CreatedDateTime.UnixNano(),
			Doc:     body,
		})
	}
	return actions
}

// resolveTeamsIdentities batch-maps sender ids → account + user _id via the injected
// resolver. A nil resolver or a lookup error yields empty identities (best-effort: a
// migrated doc still indexes its content, just without an author key).
func (c *messageCollection) resolveTeamsIdentities(idSet map[string]struct{}) map[string]teamsIdentity {
	if c.teamsUsers == nil || len(idSet) == 0 {
		return nil
	}
	ids := make([]string, 0, len(idSet))
	for id := range idSet {
		ids = append(ids, id)
	}
	identities, err := c.teamsUsers.ResolveIdentities(context.Background(), ids)
	if err != nil {
		slog.Error("resolve teams sender identities", "error", err)
		return nil
	}
	return identities
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

// newMessageSearchIndex maps a MessageEvent to a search index document.
func newMessageSearchIndex(evt *model.MessageEvent) (searchindex.MessageDoc, error) {
	return searchindex.NewMessageDoc(searchindex.MessageFields{
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
		Attachments:           evt.Message.Attachments,
		Card:                  evt.Message.Card,
	})
}

func buildMessageAction(evt *model.MessageEvent, indexPrefix string) searchengine.BulkAction {
	index := searchindex.MessageIndexName(indexPrefix, evt.Message.CreatedAt)

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
	doc, err := newMessageSearchIndex(evt)
	if err != nil {
		// Not fatal — the doc is still fully usable, just missing the skipped
		// attachment(s). Logged so silent data loss is at least observable.
		slog.Warn("message doc built with skipped attachments", "error", err,
			"messageId", evt.Message.ID, "roomId", evt.Message.RoomID)
	}
	data, _ := json.Marshal(doc)
	return data
}
