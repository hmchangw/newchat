package run

import (
	"context"
	"fmt"
	"time"

	"github.com/gocql/gocql"
)

const teardownPageSize = 2000

var cassandraTables = []string{
	"messages_by_room",
	"messages_by_id",
	"thread_messages_by_thread",
	"pinned_messages_by_room",
}

type TeardownConfig struct {
	RunID               string
	CassandraCleanup    string
	ConfirmKeyspace     string
	HeartbeatStaleAfter time.Duration
	BatchRooms          int
	BatchDelay          time.Duration
	BatchTimeout        time.Duration
}

type TeardownStore interface {
	FindManifest(context.Context, string) (*Manifest, error)
	NextOwnershipPage(context.Context, string, string, int) (*OwnershipPage, error)
	DeleteOwnedRoomBatch(context.Context, string, []string) error
	DeleteOwnership(context.Context, string) error
	MarkCleaned(context.Context, string) error
}

type OwnershipPage struct {
	Cursor  string
	RoomIDs []string
}

type CassandraCleaner interface {
	Keyspace() string
	Truncate(context.Context, string) error
}

type cassandraCleaner struct {
	session  *gocql.Session
	keyspace string
}

func NewCassandraCleaner(session *gocql.Session, keyspace string) CassandraCleaner {
	return &cassandraCleaner{session: session, keyspace: keyspace}
}

func (c *cassandraCleaner) Keyspace() string {
	return c.keyspace
}

func (c *cassandraCleaner) Truncate(ctx context.Context, table string) error {
	if err := c.session.Query("TRUNCATE " + table).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("truncate %s: %w", table, err)
	}
	return nil
}

func Teardown(
	ctx context.Context,
	store TeardownStore,
	cassandra CassandraCleaner,
	cfg *TeardownConfig,
	configuredKeyspace string,
) (bool, error) {
	if cfg == nil {
		return false, fmt.Errorf("soak teardown configuration is required")
	}
	if cfg.CassandraCleanup == "truncate" {
		if cfg.ConfirmKeyspace == "" || cfg.ConfirmKeyspace != configuredKeyspace ||
			cassandra == nil || cassandra.Keyspace() != configuredKeyspace {
			return false, fmt.Errorf(
				"refuse Cassandra truncate: SOAK_CONFIRM_KEYSPACE, CASSANDRA_KEYSPACE, and connected keyspace must match")
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
	if manifest.State == StateCleaned {
		return true, nil
	}
	if manifest.State == StateRunning {
		if manifest.LastHeartbeatAt == nil ||
			time.Since(*manifest.LastHeartbeatAt) <= cfg.HeartbeatStaleAfter {
			return true, fmt.Errorf("refuse teardown for active soak run %q", cfg.RunID)
		}
	}

	after := ""
	deletedAnyBatch := false
	for {
		page, pageErr := store.NextOwnershipPage(ctx, cfg.RunID, after, teardownPageSize)
		if pageErr != nil {
			return true, fmt.Errorf("page ownership for run %q: %w", cfg.RunID, pageErr)
		}
		if page == nil {
			break
		}
		if page.Cursor == "" || len(page.RoomIDs) == 0 {
			return true, fmt.Errorf("invalid empty ownership page for run %q", cfg.RunID)
		}
		if page.Cursor <= after {
			return true, fmt.Errorf("ownership cursor did not advance for run %q", cfg.RunID)
		}
		for _, batch := range ChunkRoomIDs(page.RoomIDs, cfg.BatchRooms) {
			if cfg.BatchDelay > 0 && deletedAnyBatch {
				if err := wait(ctx, cfg.BatchDelay); err != nil {
					return true, fmt.Errorf("pace Mongo teardown: %w", err)
				}
			}
			batchCtx, cancelBatch := context.WithTimeout(ctx, cfg.BatchTimeout)
			err := store.DeleteOwnedRoomBatch(batchCtx, cfg.RunID, batch)
			cancelBatch()
			if err != nil {
				return true, fmt.Errorf("delete owned Mongo data for run %q: %w", cfg.RunID, err)
			}
			deletedAnyBatch = true
		}
		after = page.Cursor
	}
	if err := store.DeleteOwnership(ctx, cfg.RunID); err != nil {
		return true, fmt.Errorf("delete ownership records for run %q: %w", cfg.RunID, err)
	}

	if cfg.CassandraCleanup == "truncate" {
		for _, table := range cassandraTables {
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

func wait(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
