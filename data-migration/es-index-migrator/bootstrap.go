package main

//go:generate mockgen -source=bootstrap.go -destination=mock_bootstrap_test.go -package=main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/hmchangw/chat/pkg/searchindex"
)

// TemplateStore is the narrow slice of searchengine.SearchEngine this file
// needs — defined here (the consumer), satisfied directly by
// searchengine.SearchEngine.
type TemplateStore interface {
	UpsertTemplate(ctx context.Context, name string, body json.RawMessage) error
	PutScript(ctx context.Context, id string, body json.RawMessage) error
}

// bootstrapPrerequisites idempotently ensures the three ES index templates
// and two user-room stored scripts this job depends on exist, using the
// exact same builders search-sync-worker uses at its own startup
// (pkg/searchindex) — so a fresh site can run this migrator standalone
// without first having ever run search-sync-worker. UpsertTemplate/
// PutScript are both create-or-update and safe to call repeatedly with
// unchanged content.
func bootstrapPrerequisites(ctx context.Context, engine TemplateStore, cfg *config) error {
	templates := []struct {
		name string
		body json.RawMessage
	}{
		{searchindex.MessageTemplateName(cfg.MsgIndexPrefix), searchindex.MessageTemplateBody(cfg.MsgIndexPrefix, false)},
		{searchindex.SpotlightTemplateName(cfg.SpotlightIndex), searchindex.SpotlightTemplateBody(cfg.SpotlightIndex, false)},
		{searchindex.UserRoomTemplateName(cfg.UserRoomIndex), searchindex.UserRoomTemplateBody(cfg.UserRoomIndex)},
	}
	for _, tpl := range templates {
		if err := engine.UpsertTemplate(ctx, tpl.name, tpl.body); err != nil {
			return fmt.Errorf("upsert template %s: %w", tpl.name, err)
		}
	}

	scripts := []struct {
		id     string
		source string
	}{
		{searchindex.AddRoomScriptID, searchindex.AddRoomScript},
		{searchindex.RemoveRoomScriptID, searchindex.RemoveRoomScript},
	}
	for _, script := range scripts {
		if err := engine.PutScript(ctx, script.id, searchindex.StoredScriptBody(script.source)); err != nil {
			return fmt.Errorf("put script %s: %w", script.id, err)
		}
	}

	return nil
}
