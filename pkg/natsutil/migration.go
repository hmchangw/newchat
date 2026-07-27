package natsutil

import (
	"context"

	"github.com/nats-io/nats.go"
)

// HeaderMigration marks an event as produced by the data migration. Live-delivery
// consumers (broadcast, notification) skip it; persistence/index consumers ignore it.
const HeaderMigration = "X-Migration"

// MigrationLive is the only value: "persist & index, but do not re-deliver — the
// source system already delivered this message to users."
const MigrationLive = "live"

// SetMigrationLive stamps msg as a migrated event.
func SetMigrationLive(msg *nats.Msg) {
	if msg.Header == nil {
		msg.Header = nats.Header{}
	}
	msg.Header.Set(HeaderMigration, MigrationLive)
}

// IsMigrationLive reports whether msg carries X-Migration: live.
func IsMigrationLive(msg *nats.Msg) bool {
	return msg != nil && IsMigrationLiveHeader(msg.Header)
}

// IsMigrationLiveHeader reports whether h carries X-Migration: live (nil-safe). The shared skip
// predicate for live-delivery workers (broadcast, notification) that must not re-deliver migrated events.
func IsMigrationLiveHeader(h nats.Header) bool {
	return h.Get(HeaderMigration) == MigrationLive
}

type migrationCtxKey struct{}

// WithMigrationLiveContext marks ctx so every publish built via NewMsg stamps
// X-Migration: live — a producer with no per-call header param still gets it.
func WithMigrationLiveContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, migrationCtxKey{}, true)
}

// migrationLiveFromContext reports whether ctx was marked by WithMigrationLiveContext.
func migrationLiveFromContext(ctx context.Context) bool {
	v, _ := ctx.Value(migrationCtxKey{}).(bool)
	return v
}
