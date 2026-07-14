package main

import (
	"context"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/user-presence-service/presencestore"
)

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

// UserDirectory resolves accounts to their home site. Backed by the replicated
// users collection (pkg/userstore); the presence fan-out uses it to decide
// which accounts to serve locally vs. fetch from a peer site.
type UserDirectory interface {
	FindUsersByAccounts(ctx context.Context, accounts []string) ([]model.User, error)
}

// PresenceStore is the Valkey-backed presence state. All mutating methods
// return whether the account's effective status changed (so the caller can
// publish only on change) plus the new effective status.
type PresenceStore interface {
	// Ping refreshes a connection's liveness (upsert + reset TTL) and
	// reschedules the account in the sweep index. It recomputes effective status
	// only when the ping creates a not-yet-seen connection (the offline->online
	// edge); refreshing an existing connection never changes status.
	Ping(ctx context.Context, account, connID string) (changed bool, effective model.PresenceStatus, err error)

	// SetActivity marks a connection active (away=false) or inactive (away=true),
	// upserting it, then recomputes. Used by both hello (connection init) and
	// activity (idle-edge) messages.
	SetActivity(ctx context.Context, account, connID string, away bool) (changed bool, effective model.PresenceStatus, err error)

	// RemoveConnection drops a connection (graceful bye) and recomputes.
	RemoveConnection(ctx context.Context, account, connID string) (changed bool, effective model.PresenceStatus, err error)

	// SetManual sets (or clears, when status == StatusNone) the manual override
	// and recomputes.
	SetManual(ctx context.Context, account string, status model.PresenceStatus) (changed bool, effective model.PresenceStatus, err error)

	// BatchGet returns the materialized effective status for each account
	// (StatusOffline for accounts with no record).
	BatchGet(ctx context.Context, accounts []string) (map[string]model.PresenceStatus, error)

	// Sweep recomputes every account whose sweep deadline is <= now,
	// reschedules or removes it, and returns the accounts whose status changed.
	Sweep(ctx context.Context, now time.Time) ([]presencestore.StatusChange, error)

	Close() error
}
