package main

import (
	"context"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgraph"
)

//go:generate mockgen -source=store.go -destination=mock_test.go -package=main

// activeLister returns accounts with a live connection (the sweep-index
// members). Only these can be shown in-call, so the sync scopes reconciliation
// to them rather than every account in the site. Satisfied by *presencestore.Store.
type activeLister interface {
	ActiveAccounts(ctx context.Context) ([]string, error)
}

// userResolver resolves accounts to Azure object IDs (batched, prefix match,
// keyed by account) to fill cache-missing accounts. Satisfied by
// msgraph.DirectoryReader.
type userResolver interface {
	ResolveAccountIDs(ctx context.Context, accounts []string) (map[string]string, error)
}

// presenceReader reads Teams presence (Graph ROPC). Satisfied by
// msgraph.PresenceReader.
type presenceReader interface {
	GetPresencesByUserId(ctx context.Context, ids []string) ([]msgraph.Presence, error)
}

// externalApplier applies the per-account external status and reports whether
// the effective status changed. Satisfied by *presencestore.Store.
type externalApplier interface {
	SetExternal(ctx context.Context, account string, status model.PresenceStatus, ttl time.Duration) (bool, model.PresenceStatus, error)
}

// inCallIndex tracks accounts currently marked in-call so a run can clear those
// no longer in a call.
type inCallIndex interface {
	Members(ctx context.Context) ([]string, error)
	Add(ctx context.Context, account string) error
	Remove(ctx context.Context, account string) error
}

// idMapStore is the permanent account -> azureObjectID cache. The mapping never
// changes (Azure object ids are immutable), so entries are stored without
// expiry; the sync only fetches from Graph to fill accounts missing from it.
type idMapStore interface {
	Resolve(ctx context.Context, accounts []string) (map[string]string, error)
	Store(ctx context.Context, mapping map[string]string) error
}

// statePublisher publishes a PresenceState change (best-effort fan-out).
type statePublisher interface {
	Publish(ctx context.Context, account string, status model.PresenceStatus)
}
