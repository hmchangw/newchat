package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/nats-io/nats.go/jetstream"

	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/auditarchive"
	"github.com/hmchangw/chat/pkg/stream"
)

// streamManager is the JetStream surface bootstrap needs; tests inject a fake.
type streamManager interface {
	CreateOrUpdateStream(ctx context.Context, cfg jetstream.StreamConfig) (o11ynats.Stream, error)
	Stream(ctx context.Context, name string) (o11ynats.Stream, error)
}

// bootstrapStreams follows the repo convention: schema-only creation of the
// stream this worker reads when BOOTSTRAP_STREAMS=true (dev), verification
// otherwise. INBOX belongs to inbox-worker and is only ever verified.
func bootstrapStreams(ctx context.Context, js streamManager, siteID string, enabled bool) error {
	canonical := stream.MessagesCanonical(siteID)
	if enabled {
		if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{Name: canonical.Name, Subjects: canonical.Subjects}); err != nil {
			return fmt.Errorf("create stream %s: %w", canonical.Name, err)
		}
	} else if _, err := js.Stream(ctx, canonical.Name); err != nil {
		return fmt.Errorf("verify stream %s: %w", canonical.Name, err)
	}
	inbox := stream.Inbox(siteID)
	if _, err := js.Stream(ctx, inbox.Name); err != nil {
		return fmt.Errorf("verify stream %s (owned by inbox-worker): %w", inbox.Name, err)
	}
	return nil
}

// bootstrapIndex is idempotent and runs on every start: the policy is created
// only if absent so operator edits survive, templates are upserted.
func bootstrapIndex(ctx context.Context, idx indexStore, siteID, retention string, devMode bool) error {
	created, err := idx.EnsureLifecyclePolicy(ctx, auditarchive.LifecyclePolicyName, auditarchive.LifecyclePolicyBody(retention))
	if err != nil {
		return fmt.Errorf("ensure lifecycle policy: %w", err)
	}
	slog.Info("lifecycle policy ensured", "name", auditarchive.LifecyclePolicyName, "created", created)
	for _, tpl := range auditarchive.Templates(siteID, devMode) {
		if err := idx.UpsertTemplate(ctx, tpl.Name, tpl.Body); err != nil {
			return fmt.Errorf("upsert template %s: %w", tpl.Name, err)
		}
	}
	return nil
}
