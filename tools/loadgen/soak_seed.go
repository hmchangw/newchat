package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roomkeystore"
)

type soakManifestState string

const (
	soakManifestSeeding   soakManifestState = "seeding"
	soakManifestSeeded    soakManifestState = "seeded"
	soakManifestRunning   soakManifestState = "running"
	soakManifestStopped   soakManifestState = "stopped"
	soakManifestCompleted soakManifestState = "completed"
	soakManifestCleaned   soakManifestState = "cleaned"
)

type soakManifest struct {
	ID                 string            `bson:"_id"`
	State              soakManifestState `bson:"state"`
	RunMode            string            `bson:"runMode"`
	SiteID             string            `bson:"siteId"`
	MongoDatabase      string            `bson:"mongoDatabase"`
	CassandraKeyspace  string            `bson:"cassandraKeyspace"`
	ConfigDigest       string            `bson:"configDigest"`
	BorrowedUserCount  int               `bson:"borrowedUserCount"`
	ActiveUserCount    int               `bson:"activeUserCount"`
	ActiveUserIDs      []string          `bson:"activeUserIds"`
	RoomCount          int               `bson:"roomCount"`
	SubscriptionCount  int               `bson:"subscriptionCount"`
	StartedAt          time.Time         `bson:"startedAt"`
	UpdatedAt          time.Time         `bson:"updatedAt"`
	SeededAt           *time.Time        `bson:"seededAt,omitempty"`
	CleanedAt          *time.Time        `bson:"cleanedAt,omitempty"`
	FirstStartedAt     *time.Time        `bson:"firstStartedAt,omitempty"`
	Deadline           *time.Time        `bson:"deadline,omitempty"`
	CompletedAt        *time.Time        `bson:"completedAt,omitempty"`
	LastStoppedAt      *time.Time        `bson:"lastStoppedAt,omitempty"`
	LastHeartbeatAt    *time.Time        `bson:"lastHeartbeatAt,omitempty"`
	ConfiguredDuration time.Duration     `bson:"configuredDuration,omitempty"`
	RestartCount       int               `bson:"restartCount,omitempty"`
}

type soakSeedInput struct {
	RunID             string
	SiteID            string
	MongoDatabase     string
	CassandraKeyspace string
	Seed              int64
	Config            *soakConfig
}

func seedSoak(
	ctx context.Context,
	store soakSeedStore,
	keys roomKeyStore,
	input *soakSeedInput,
	ids *soakIDs,
) (soakTopology, error) {
	if input.Config == nil {
		return soakTopology{}, fmt.Errorf("soak configuration is required")
	}
	if input.RunID == "" || input.RunID != input.Config.RunID {
		return soakTopology{}, fmt.Errorf("seed run ID must match SOAK_RUN_ID")
	}
	existing, err := store.FindManifest(ctx, input.RunID)
	if err != nil {
		return soakTopology{}, fmt.Errorf("check existing soak manifest: %w", err)
	}
	if isActiveSoakManifest(existing, input.Config.HeartbeatStaleAfter, time.Now().UTC()) {
		return soakTopology{}, fmt.Errorf(
			"refuse seed for active soak run %q",
			input.RunID,
		)
	}

	users, err := store.BorrowUsers(ctx, input.SiteID, input.Config.MaxUsers)
	if err != nil {
		return soakTopology{}, fmt.Errorf("borrow real users: %w", err)
	}
	topology, err := buildSoakTopology(users, input.Config, input.SiteID, input.Seed, ids)
	if err != nil {
		return soakTopology{}, fmt.Errorf("build soak topology: %w", err)
	}
	roomIDs := make([]string, len(topology.Rooms))
	for i := range topology.Rooms {
		roomIDs[i] = topology.Rooms[i].ID
	}
	conflicts, err := store.FindConflictingRoomIDs(ctx, input.RunID, roomIDs)
	if err != nil {
		return soakTopology{}, fmt.Errorf("check soak room ID conflicts: %w", err)
	}
	if len(conflicts) > 0 {
		return soakTopology{}, fmt.Errorf(
			"refuse seed: %d room ID conflicts with non-owned data",
			len(conflicts),
		)
	}

	now := time.Now().UTC()
	manifest := soakManifest{
		ID:                input.RunID,
		State:             soakManifestSeeding,
		RunMode:           input.Config.RunMode,
		SiteID:            input.SiteID,
		MongoDatabase:     input.MongoDatabase,
		CassandraKeyspace: input.CassandraKeyspace,
		ConfigDigest:      digestSoakConfig(input.Config),
		BorrowedUserCount: len(topology.BorrowedUsers),
		ActiveUserCount:   len(topology.ActiveUsers),
		ActiveUserIDs:     make([]string, len(topology.ActiveUsers)),
		RoomCount:         len(topology.Rooms),
		SubscriptionCount: len(topology.Subscriptions),
		StartedAt:         now,
		UpdatedAt:         now,
	}
	for i := range topology.ActiveUsers {
		manifest.ActiveUserIDs[i] = topology.ActiveUsers[i].ID
	}
	if err := store.ResetOwned(ctx, input.RunID); err != nil {
		return soakTopology{}, fmt.Errorf("reset partial topology for run %q: %w", input.RunID, err)
	}
	if err := store.PutManifest(ctx, &manifest); err != nil {
		return soakTopology{}, fmt.Errorf("record seeding manifest: %w", err)
	}
	chunks := chunkSoakRoomIDs(roomIDs, soakOwnershipChunkSize)
	if err := store.ReplaceOwnershipChunks(ctx, input.RunID, chunks); err != nil {
		return soakTopology{}, fmt.Errorf("record soak ownership: %w", err)
	}
	if err := store.InsertOwnedRooms(ctx, input.RunID, topology.Rooms); err != nil {
		return soakTopology{}, fmt.Errorf("insert soak rooms: %w", err)
	}
	roomKeys, err := buildSoakRoomKeys(topology.Rooms)
	if err != nil {
		return soakTopology{}, fmt.Errorf("generate soak room keys: %w", err)
	}
	if err := SeedRoomKeys(ctx, keys, roomKeys); err != nil {
		return soakTopology{}, fmt.Errorf("seed soak room keys: %w", err)
	}
	if err := store.InsertOwnedSubscriptions(ctx, input.RunID, topology.Subscriptions); err != nil {
		return soakTopology{}, fmt.Errorf("insert soak subscriptions: %w", err)
	}
	seededAt := time.Now().UTC()
	manifest.State = soakManifestSeeded
	manifest.UpdatedAt = seededAt
	manifest.SeededAt = &seededAt
	if err := store.PutManifest(ctx, &manifest); err != nil {
		return soakTopology{}, fmt.Errorf("record seeded manifest: %w", err)
	}
	return topology, nil
}

func isActiveSoakManifest(
	manifest *soakManifest,
	staleAfter time.Duration,
	now time.Time,
) bool {
	if manifest == nil || manifest.State != soakManifestRunning {
		return false
	}
	if manifest.LastHeartbeatAt == nil {
		return true
	}
	return now.Sub(manifest.LastHeartbeatAt.UTC()) <= staleAfter
}

func chunkSoakRoomIDs(roomIDs []string, size int) [][]string {
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

func digestSoakConfig(cfg *soakConfig) string {
	data, err := json.Marshal(cfg)
	if err != nil {
		panic("marshal soak config: " + err.Error())
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func buildSoakRoomKeys(
	rooms []model.Room,
) (map[string]roomkeystore.RoomKeyPair, error) {
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
