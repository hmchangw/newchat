package main

import (
	"context"
	"fmt"

	"github.com/gocql/gocql"
)

const soakTeardownPageSize = 2000

var soakCassandraTables = []string{
	"messages_by_room",
	"messages_by_id",
	"thread_messages_by_thread",
	"pinned_messages_by_room",
}

type soakTeardownStore interface {
	FindManifest(ctx context.Context, runID string) (*soakManifest, error)
	NextOwnershipPage(ctx context.Context, runID, after string, limit int) (*soakOwnershipPage, error)
	DeleteOwnedRoomBatch(ctx context.Context, runID string, roomIDs []string) error
	DeleteOwnership(ctx context.Context, runID string) error
	MarkCleaned(ctx context.Context, runID string) error
}

type soakOwnershipPage struct {
	Cursor  string
	RoomIDs []string
}

type soakCassandraCleaner interface {
	Keyspace() string
	Truncate(ctx context.Context, table string) error
}

type gocqlSoakCleaner struct {
	session  *gocql.Session
	keyspace string
}

func (c *gocqlSoakCleaner) Keyspace() string {
	return c.keyspace
}

func (c *gocqlSoakCleaner) Truncate(ctx context.Context, table string) error {
	if err := c.session.Query("TRUNCATE " + table).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("truncate %s: %w", table, err)
	}
	return nil
}

func teardownSoak(
	ctx context.Context,
	store soakTeardownStore,
	cassandra soakCassandraCleaner,
	cfg *soakConfig,
	configuredKeyspace string,
) (bool, error) {
	if cfg.CassandraCleanup == "truncate" {
		if cfg.ConfirmKeyspace == "" ||
			cfg.ConfirmKeyspace != configuredKeyspace ||
			cassandra == nil ||
			cassandra.Keyspace() != configuredKeyspace {
			return false, fmt.Errorf(
				"refuse Cassandra truncate: SOAK_CONFIRM_KEYSPACE, CASSANDRA_KEYSPACE, and connected keyspace must match",
			)
		}
	} else if cfg.CassandraCleanup != "none" {
		return false, fmt.Errorf("refuse unknown Cassandra cleanup mode %q", cfg.CassandraCleanup)
	}

	manifest, err := store.FindManifest(ctx, cfg.RunID)
	if err != nil {
		return false, fmt.Errorf("find soak manifest %q: %w", cfg.RunID, err)
	}
	if manifest == nil {
		return false, nil
	}
	if manifest.State == soakManifestCleaned {
		return true, nil
	}

	after := ""
	for {
		page, err := store.NextOwnershipPage(ctx, cfg.RunID, after, soakTeardownPageSize)
		if err != nil {
			return true, fmt.Errorf("page ownership for run %q: %w", cfg.RunID, err)
		}
		if page == nil {
			break
		}
		if page.Cursor == "" || len(page.RoomIDs) == 0 {
			return true, fmt.Errorf("invalid empty ownership page for run %q", cfg.RunID)
		}
		if err := store.DeleteOwnedRoomBatch(ctx, cfg.RunID, page.RoomIDs); err != nil {
			return true, fmt.Errorf("delete owned Mongo data for run %q: %w", cfg.RunID, err)
		}
		after = page.Cursor
	}
	if err := store.DeleteOwnership(ctx, cfg.RunID); err != nil {
		return true, fmt.Errorf("delete ownership records for run %q: %w", cfg.RunID, err)
	}

	if cfg.CassandraCleanup == "truncate" {
		for _, table := range soakCassandraTables {
			if err := cassandra.Truncate(ctx, table); err != nil {
				return true, fmt.Errorf("truncate Cassandra table %s: %w", table, err)
			}
		}
	}
	if err := store.MarkCleaned(ctx, cfg.RunID); err != nil {
		return true, fmt.Errorf("mark soak run %q cleaned: %w", cfg.RunID, err)
	}
	return true, nil
}
