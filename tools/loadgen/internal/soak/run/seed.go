package run

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roomkeystore"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/topology"
)

const OwnershipChunkSize = 2000

var (
	ErrManifestNotFound = errors.New("soak run manifest not found")
	ErrRunNotActive     = errors.New("soak run is not active")
)

type ManifestState string

const (
	StateSeeding   ManifestState = "seeding"
	StateSeeded    ManifestState = "seeded"
	StateRunning   ManifestState = "running"
	StateStopped   ManifestState = "stopped"
	StateCompleted ManifestState = "completed"
	StateCleaned   ManifestState = "cleaned"
)

type Manifest struct {
	ID                 string        `bson:"_id"`
	State              ManifestState `bson:"state"`
	RunMode            string        `bson:"runMode"`
	SiteID             string        `bson:"siteId"`
	MongoDatabase      string        `bson:"mongoDatabase"`
	CassandraKeyspace  string        `bson:"cassandraKeyspace"`
	ConfigDigest       string        `bson:"configDigest"`
	BorrowedUserCount  int           `bson:"borrowedUserCount"`
	ActiveUserCount    int           `bson:"activeUserCount"`
	ActiveUserIDs      []string      `bson:"activeUserIds"`
	RoomCount          int           `bson:"roomCount"`
	SubscriptionCount  int           `bson:"subscriptionCount"`
	StartedAt          time.Time     `bson:"startedAt"`
	UpdatedAt          time.Time     `bson:"updatedAt"`
	SeededAt           *time.Time    `bson:"seededAt,omitempty"`
	CleanedAt          *time.Time    `bson:"cleanedAt,omitempty"`
	FirstStartedAt     *time.Time    `bson:"firstStartedAt,omitempty"`
	Deadline           *time.Time    `bson:"deadline,omitempty"`
	CompletedAt        *time.Time    `bson:"completedAt,omitempty"`
	LastStoppedAt      *time.Time    `bson:"lastStoppedAt,omitempty"`
	LastHeartbeatAt    *time.Time    `bson:"lastHeartbeatAt,omitempty"`
	ConfiguredDuration time.Duration `bson:"configuredDuration,omitempty"`
	RestartCount       int           `bson:"restartCount,omitempty"`
}

type SeedInput struct {
	RunID               string
	RunMode             string
	SiteID              string
	MongoDatabase       string
	CassandraKeyspace   string
	ConfigDigest        string
	HeartbeatStaleAfter time.Duration
	Seed                int64
	Topology            topology.BuildConfig
}

type SeedStore interface {
	FindManifest(context.Context, string) (*Manifest, error)
	BorrowUsers(context.Context, string, int) ([]model.User, error)
	FindConflictingRoomIDs(context.Context, string, []string) ([]string, error)
	ResetOwned(context.Context, string) error
	PutManifest(context.Context, *Manifest) error
	InsertOwnedRooms(context.Context, string, []model.Room) error
	InsertOwnedSubscriptions(context.Context, string, []model.Subscription) error
	ReplaceOwnershipChunks(context.Context, string, [][]string) error
}

type KeyStore interface {
	Set(context.Context, string, roomkeystore.RoomKeyPair) (int, error)
}

func Seed(
	ctx context.Context,
	store SeedStore,
	keys KeyStore,
	input *SeedInput,
	ids *topology.IdentitySource,
) (topology.Topology, error) {
	if input == nil {
		return topology.Topology{}, fmt.Errorf("soak seed input is required")
	}
	if input.RunID == "" || input.RunID != input.Topology.RunID {
		return topology.Topology{}, fmt.Errorf("seed run ID must match topology run ID")
	}
	existing, err := store.FindManifest(ctx, input.RunID)
	if err != nil {
		return topology.Topology{}, fmt.Errorf("check existing soak manifest: %w", err)
	}
	if ActiveManifest(existing, input.HeartbeatStaleAfter, time.Now().UTC()) {
		return topology.Topology{}, fmt.Errorf("refuse seed for active soak run %q", input.RunID)
	}

	users, err := store.BorrowUsers(ctx, input.SiteID, input.Topology.MaxUsers)
	if err != nil {
		return topology.Topology{}, fmt.Errorf("borrow real users: %w", err)
	}
	built, err := topology.Build(users, &input.Topology, input.SiteID, input.Seed, ids)
	if err != nil {
		return topology.Topology{}, fmt.Errorf("build soak topology: %w", err)
	}
	roomIDs := make([]string, len(built.Rooms))
	for i := range built.Rooms {
		roomIDs[i] = built.Rooms[i].ID
	}
	conflicts, err := store.FindConflictingRoomIDs(ctx, input.RunID, roomIDs)
	if err != nil {
		return topology.Topology{}, fmt.Errorf("check soak room ID conflicts: %w", err)
	}
	if len(conflicts) > 0 {
		return topology.Topology{}, fmt.Errorf(
			"refuse seed: %d room ID conflicts with non-owned data", len(conflicts))
	}

	now := time.Now().UTC()
	manifest := Manifest{
		ID: input.RunID, State: StateSeeding, RunMode: input.RunMode,
		SiteID: input.SiteID, MongoDatabase: input.MongoDatabase,
		CassandraKeyspace: input.CassandraKeyspace, ConfigDigest: input.ConfigDigest,
		BorrowedUserCount: len(built.BorrowedUsers), ActiveUserCount: len(built.ActiveUsers),
		ActiveUserIDs: make([]string, len(built.ActiveUsers)), RoomCount: len(built.Rooms),
		SubscriptionCount: len(built.Subscriptions), StartedAt: now, UpdatedAt: now,
	}
	for i := range built.ActiveUsers {
		manifest.ActiveUserIDs[i] = built.ActiveUsers[i].ID
	}
	if err := store.ResetOwned(ctx, input.RunID); err != nil {
		return topology.Topology{}, fmt.Errorf("reset partial topology for run %q: %w", input.RunID, err)
	}
	if err := store.PutManifest(ctx, &manifest); err != nil {
		return topology.Topology{}, fmt.Errorf("record seeding manifest: %w", err)
	}
	if err := store.ReplaceOwnershipChunks(ctx, input.RunID, ChunkRoomIDs(roomIDs, OwnershipChunkSize)); err != nil {
		return topology.Topology{}, fmt.Errorf("record soak ownership: %w", err)
	}
	if err := store.InsertOwnedRooms(ctx, input.RunID, built.Rooms); err != nil {
		return topology.Topology{}, fmt.Errorf("insert soak rooms: %w", err)
	}
	roomKeys, err := BuildRoomKeys(built.Rooms)
	if err != nil {
		return topology.Topology{}, fmt.Errorf("generate soak room keys: %w", err)
	}
	if err := seedRoomKeys(ctx, keys, roomKeys); err != nil {
		return topology.Topology{}, fmt.Errorf("seed soak room keys: %w", err)
	}
	if err := store.InsertOwnedSubscriptions(ctx, input.RunID, built.Subscriptions); err != nil {
		return topology.Topology{}, fmt.Errorf("insert soak subscriptions: %w", err)
	}
	seededAt := time.Now().UTC()
	manifest.State = StateSeeded
	manifest.UpdatedAt = seededAt
	manifest.SeededAt = &seededAt
	if err := store.PutManifest(ctx, &manifest); err != nil {
		return topology.Topology{}, fmt.Errorf("record seeded manifest: %w", err)
	}
	return built, nil
}

func ActiveManifest(manifest *Manifest, staleAfter time.Duration, now time.Time) bool {
	if manifest == nil || manifest.State != StateRunning {
		return false
	}
	if manifest.LastHeartbeatAt == nil {
		return true
	}
	return now.Sub(manifest.LastHeartbeatAt.UTC()) <= staleAfter
}

func ChunkRoomIDs(roomIDs []string, size int) [][]string {
	if size <= 0 || len(roomIDs) == 0 {
		return nil
	}
	chunks := make([][]string, 0, (len(roomIDs)+size-1)/size)
	for start := 0; start < len(roomIDs); start += size {
		end := min(start+size, len(roomIDs))
		chunks = append(chunks, append([]string(nil), roomIDs[start:end]...))
	}
	return chunks
}

func BuildRoomKeys(rooms []model.Room) (map[string]roomkeystore.RoomKeyPair, error) {
	keys := make(map[string]roomkeystore.RoomKeyPair, len(rooms))
	for i := range rooms {
		pair, err := roomkeystore.GenerateKeyPair()
		if err != nil {
			return nil, fmt.Errorf("generate room %q key: %w", rooms[i].ID, err)
		}
		keys[rooms[i].ID] = *pair
	}
	return keys, nil
}

func seedRoomKeys(ctx context.Context, store KeyStore, keys map[string]roomkeystore.RoomKeyPair) error {
	for roomID, keyPair := range keys {
		if _, err := store.Set(ctx, roomID, keyPair); err != nil {
			return fmt.Errorf("set room key %q: %w", roomID, err)
		}
	}
	return nil
}
