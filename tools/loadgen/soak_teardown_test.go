package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTeardownSoak_UnknownRunIsNoOp(t *testing.T) {
	store := &recordingSoakTeardownStore{}
	cassandra := &recordingSoakCassandra{keyspace: "chat"}
	cfg := validSoakConfig(t)

	found, err := teardownSoak(context.Background(), store, cassandra, &cfg, "chat")

	require.NoError(t, err)
	assert.False(t, found)
	assert.Empty(t, store.deletedBatches)
	assert.Empty(t, cassandra.tables)
	assert.False(t, store.cleaned)
}

func TestTeardownSoak_DeletesOwnedChunksAndMarksCleaned(t *testing.T) {
	store := &recordingSoakTeardownStore{
		manifest: &soakManifest{ID: "run-a", State: soakManifestSeeded},
		pages: [][]string{
			{"room-1", "room-2"},
			{"room-3"},
		},
	}
	cfg := validSoakConfig(t)
	cfg.RunID = "run-a"
	cfg.CassandraCleanup = "none"
	cfg.TeardownBatchRooms = 2
	cfg.TeardownBatchDelay = 0

	found, err := teardownSoak(
		context.Background(),
		store,
		&recordingSoakCassandra{keyspace: "chat"},
		&cfg,
		"chat",
	)

	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, store.pages, store.deletedBatches)
	assert.True(t, store.ownershipDeleted)
	assert.True(t, store.cleaned)
}

func TestTeardownSoak_SplitsOwnershipPagesIntoRateLimitedBatches(t *testing.T) {
	store := &recordingSoakTeardownStore{
		manifest: &soakManifest{ID: "run-a", State: soakManifestSeeded},
		pages: [][]string{
			{"room-1", "room-2", "room-3", "room-4", "room-5"},
		},
	}
	cfg := validSoakConfig(t)
	cfg.RunID = "run-a"
	cfg.TeardownBatchRooms = 2
	cfg.TeardownBatchDelay = 0

	_, err := teardownSoak(context.Background(), store, nil, &cfg, "chat")

	require.NoError(t, err)
	assert.Equal(t, [][]string{
		{"room-1", "room-2"},
		{"room-3", "room-4"},
		{"room-5"},
	}, store.deletedBatches)
}

func TestTeardownSoak_RejectsNonAdvancingOwnershipCursor(t *testing.T) {
	store := &recordingSoakTeardownStore{
		manifest: &soakManifest{ID: "run-a", State: soakManifestSeeded},
		pages:    [][]string{{"room-1"}, {"room-2"}},
		cursors:  []string{"chunk-1", "chunk-1"},
	}
	cfg := validSoakConfig(t)
	cfg.RunID = "run-a"
	cfg.TeardownBatchDelay = 0

	_, err := teardownSoak(context.Background(), store, nil, &cfg, "chat")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "did not advance")
	assert.Equal(t, [][]string{{"room-1"}}, store.deletedBatches)
	assert.False(t, store.ownershipDeleted)
}

func TestTeardownSoak_GuardsCassandraTruncate(t *testing.T) {
	tests := []struct {
		name              string
		mode              string
		confirm           string
		configKeyspace    string
		connectedKeyspace string
	}{
		{"cleanup mode none", "none", "", "chat", "chat"},
		{"missing confirmation", "truncate", "", "chat", "chat"},
		{"config confirmation mismatch", "truncate", "wrong", "chat", "chat"},
		{"connected keyspace mismatch", "truncate", "chat", "chat", "other"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &recordingSoakTeardownStore{
				manifest: &soakManifest{ID: "run-a", State: soakManifestSeeded},
			}
			cassandra := &recordingSoakCassandra{keyspace: tt.connectedKeyspace}
			cfg := validSoakConfig(t)
			cfg.RunID = "run-a"
			cfg.CassandraCleanup = tt.mode
			cfg.ConfirmKeyspace = tt.confirm

			_, err := teardownSoak(context.Background(), store, cassandra, &cfg, tt.configKeyspace)

			if tt.mode == "none" {
				require.NoError(t, err)
				assert.Empty(t, cassandra.tables)
				return
			}
			require.Error(t, err)
			assert.Empty(t, store.deletedBatches, "guard must fail before Mongo changes")
			assert.Empty(t, cassandra.tables)
			assert.False(t, store.cleaned)
		})
	}
}

func TestTeardownSoak_TruncatesExactTables(t *testing.T) {
	store := &recordingSoakTeardownStore{
		manifest: &soakManifest{ID: "run-a", State: soakManifestSeeded},
	}
	cassandra := &recordingSoakCassandra{keyspace: "chat"}
	cfg := validSoakConfig(t)
	cfg.RunID = "run-a"
	cfg.CassandraCleanup = "truncate"
	cfg.ConfirmKeyspace = "chat"

	found, err := teardownSoak(context.Background(), store, cassandra, &cfg, "chat")

	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, []string{
		"messages_by_room",
		"messages_by_id",
		"thread_messages_by_thread",
		"pinned_messages_by_room",
	}, cassandra.tables)
	assert.True(t, store.cleaned)
}

func TestTeardownSoak_DoesNotMarkCleanedAfterFailure(t *testing.T) {
	store := &recordingSoakTeardownStore{
		manifest:  &soakManifest{ID: "run-a", State: soakManifestSeeded},
		pages:     [][]string{{"room-1"}},
		deleteErr: errors.New("delete failed"),
	}
	cfg := validSoakConfig(t)
	cfg.RunID = "run-a"

	_, err := teardownSoak(
		context.Background(),
		store,
		&recordingSoakCassandra{keyspace: "chat"},
		&cfg,
		"chat",
	)

	require.Error(t, err)
	assert.False(t, store.cleaned)
}

func TestTeardownSoak_RepeatIsIdempotent(t *testing.T) {
	store := &recordingSoakTeardownStore{
		manifest: &soakManifest{ID: "run-a", State: soakManifestCleaned},
	}
	cfg := validSoakConfig(t)
	cfg.RunID = "run-a"

	found, err := teardownSoak(
		context.Background(),
		store,
		&recordingSoakCassandra{keyspace: "chat"},
		&cfg,
		"chat",
	)

	require.NoError(t, err)
	assert.True(t, found)
	assert.Empty(t, store.deletedBatches)
	assert.False(t, store.cleaned)
}

func TestTeardownSoak_RejectsActiveContinuousRun(t *testing.T) {
	now := time.Now().UTC()
	cfg := validSoakConfig(t)
	cfg.RunID = "run-a"
	tests := []struct {
		name      string
		heartbeat *time.Time
	}{
		{name: "fresh heartbeat", heartbeat: &now},
		{name: "missing heartbeat"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &recordingSoakTeardownStore{
				manifest: &soakManifest{
					ID: "run-a", State: soakManifestRunning,
					RunMode:         soakRunModeContinuous,
					LastHeartbeatAt: tt.heartbeat,
				},
			}

			_, err := teardownSoak(
				context.Background(),
				store,
				nil,
				&cfg,
				"chat",
			)

			require.Error(t, err)
			assert.Contains(t, err.Error(), "active")
			assert.Empty(t, store.deletedBatches)
			assert.False(t, store.ownershipDeleted)
			assert.False(t, store.cleaned)
		})
	}
}

func TestTeardownSoak_AllowsExpiredHeartbeatLease(t *testing.T) {
	cfg := validSoakConfig(t)
	cfg.RunID = "run-a"
	expired := time.Now().UTC().Add(-cfg.HeartbeatStaleAfter - time.Minute)
	store := &recordingSoakTeardownStore{
		manifest: &soakManifest{
			ID: "run-a", State: soakManifestRunning,
			RunMode:         soakRunModeContinuous,
			LastHeartbeatAt: &expired,
		},
	}

	found, err := teardownSoak(
		context.Background(),
		store,
		nil,
		&cfg,
		"chat",
	)

	require.NoError(t, err)
	assert.True(t, found)
	assert.True(t, store.ownershipDeleted)
	assert.True(t, store.cleaned)
}

type recordingSoakTeardownStore struct {
	manifest         *soakManifest
	pages            [][]string
	cursors          []string
	nextPage         int
	deletedBatches   [][]string
	deleteErr        error
	ownershipDeleted bool
	cleaned          bool
}

func (s *recordingSoakTeardownStore) FindManifest(
	_ context.Context,
	_ string,
) (*soakManifest, error) {
	return s.manifest, nil
}

func (s *recordingSoakTeardownStore) NextOwnershipPage(
	_ context.Context,
	_ string,
	_ string,
	_ int,
) (*soakOwnershipPage, error) {
	if s.nextPage >= len(s.pages) {
		return nil, nil
	}
	page := s.pages[s.nextPage]
	s.nextPage++
	cursor := fmt.Sprintf("chunk-%06d", s.nextPage)
	if len(s.cursors) >= s.nextPage {
		cursor = s.cursors[s.nextPage-1]
	}
	return &soakOwnershipPage{
		Cursor:  cursor,
		RoomIDs: page,
	}, nil
}

func (s *recordingSoakTeardownStore) DeleteOwnedRoomBatch(
	_ context.Context,
	_ string,
	roomIDs []string,
) error {
	if s.deleteErr != nil {
		return s.deleteErr
	}
	s.deletedBatches = append(s.deletedBatches, append([]string(nil), roomIDs...))
	return nil
}

func (s *recordingSoakTeardownStore) DeleteOwnership(_ context.Context, _ string) error {
	s.ownershipDeleted = true
	return nil
}

func (s *recordingSoakTeardownStore) MarkCleaned(_ context.Context, _ string) error {
	s.cleaned = true
	return nil
}

type recordingSoakCassandra struct {
	keyspace string
	tables   []string
}

func (c *recordingSoakCassandra) Keyspace() string {
	return c.keyspace
}

func (c *recordingSoakCassandra) Truncate(_ context.Context, table string) error {
	c.tables = append(c.tables, table)
	return nil
}
