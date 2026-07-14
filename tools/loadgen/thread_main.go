package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/hmchangw/chat/pkg/cassutil"
	"github.com/hmchangw/chat/pkg/msgbucket"
)

// runSeedThread seeds the messages fixtures (rooms/subs/room-keys in Mongo) then
// the per-room thread parents in Cassandra, so the thread max-rps workload's
// replies reference resolvable parents. parentsPerRoom <= 0 uses the default.
func runSeedThread(ctx context.Context, cfg *config, preset string, seed int64, usersOverride, parentsPerRoom int) int {
	if cfg.CassandraHosts == "" {
		fmt.Fprintln(os.Stderr, "thread workload requires CASSANDRA_HOSTS")
		return 2
	}
	p, ok := BuiltinPreset(preset)
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown preset: %s\n", preset)
		return 2
	}
	if usersOverride > 0 {
		p.Users = usersOverride
	}

	db, keyStore, cleanup, err := connectStores(ctx, cfg)
	if err != nil {
		return 1
	}
	defer cleanup()

	session, err := connectCassandra(cfg)
	if err != nil {
		slog.Error("cassandra connect", "error", err)
		return 1
	}
	defer cassutil.Close(session)

	fixtures := BuildThreadFixtures(&p, seed, parentsPerRoom, cfg.SiteID)
	if err := Seed(ctx, db, &fixtures.Fixtures); err != nil {
		slog.Error("seed mongo fixtures", "error", err)
		return 1
	}
	if err := SeedRoomKeys(ctx, keyStore, fixtures.RoomKeys); err != nil {
		slog.Error("seed room keys", "error", err)
		return 1
	}
	sizer := msgbucket.New(time.Duration(cfg.MessageBucketHours) * time.Hour)
	parentCount, err := SeedThreadParents(ctx, session, sizer, &fixtures, cfg.SiteID, time.Now().UTC())
	if err != nil {
		slog.Error("seed thread parents", "error", err)
		return 1
	}

	slog.Info("seed complete (thread)",
		"preset", p.Name,
		"users", len(fixtures.Users),
		"rooms", len(fixtures.Rooms),
		"subs", len(fixtures.Subscriptions),
		"parentsPerRoom", fixtures.ParentsPerRoom,
		"threadParents", parentCount,
		"bucketHours", cfg.MessageBucketHours)
	return 0
}

// runTeardownThread clears the Mongo fixtures and TRUNCATEs the message tables
// (parents + any replies produced during the run).
func runTeardownThread(ctx context.Context, cfg *config, preset string, seed int64) int {
	if cfg.CassandraHosts == "" {
		fmt.Fprintln(os.Stderr, "thread workload requires CASSANDRA_HOSTS")
		return 2
	}
	p, ok := BuiltinPreset(preset)
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown preset: %s\n", preset)
		return 2
	}

	db, keyStore, cleanup, err := connectStores(ctx, cfg)
	if err != nil {
		return 1
	}
	defer cleanup()

	session, err := connectCassandra(cfg)
	if err != nil {
		slog.Error("cassandra connect", "error", err)
		return 1
	}
	defer cassutil.Close(session)

	fixtures := BuildThreadFixtures(&p, seed, 0, cfg.SiteID)
	roomIDs := roomIDsOf(fixtures.Rooms)

	if err := Teardown(ctx, db); err != nil {
		slog.Error("teardown mongo", "error", err)
		return 1
	}
	if err := TeardownRoomKeys(ctx, keyStore, roomIDs); err != nil {
		slog.Error("teardown room keys", "error", err)
		return 1
	}
	if err := TeardownThreadParents(ctx, session); err != nil {
		slog.Error("teardown thread parents", "error", err)
		return 1
	}
	slog.Info("teardown complete (thread)", "preset", preset)
	return 0
}
