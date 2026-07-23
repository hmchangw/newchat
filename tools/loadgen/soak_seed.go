package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roomkeystore"
)

type soakManifestState string

const (
	soakManifestSeeding   soakManifestState = "seeding"
	soakManifestSeeded    soakManifestState = "seeded"
	soakManifestRunning   soakManifestState = "running"
	soakManifestCompleted soakManifestState = "completed"
	soakManifestCleaned   soakManifestState = "cleaned"
)

type soakManifest struct {
	ID                string            `bson:"_id"`
	State             soakManifestState `bson:"state"`
	SiteID            string            `bson:"siteId"`
	MongoDatabase     string            `bson:"mongoDatabase"`
	CassandraKeyspace string            `bson:"cassandraKeyspace"`
	ConfigDigest      string            `bson:"configDigest"`
	BorrowedUserCount int               `bson:"borrowedUserCount"`
	ActiveUserCount   int               `bson:"activeUserCount"`
	RoomCount         int               `bson:"roomCount"`
	SubscriptionCount int               `bson:"subscriptionCount"`
	StartedAt         time.Time         `bson:"startedAt"`
	UpdatedAt         time.Time         `bson:"updatedAt"`
	SeededAt          *time.Time        `bson:"seededAt,omitempty"`
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

	users, err := store.BorrowUsers(ctx, input.SiteID, input.Config.MaxUsers)
	if err != nil {
		return soakTopology{}, fmt.Errorf("borrow real users: %w", err)
	}
	topology, err := buildSoakTopology(users, input.Config, input.SiteID, input.Seed, ids)
	if err != nil {
		return soakTopology{}, fmt.Errorf("build soak topology: %w", err)
	}

	now := time.Now().UTC()
	manifest := soakManifest{
		ID:                input.RunID,
		State:             soakManifestSeeding,
		SiteID:            input.SiteID,
		MongoDatabase:     input.MongoDatabase,
		CassandraKeyspace: input.CassandraKeyspace,
		ConfigDigest:      digestSoakConfig(input.Config),
		BorrowedUserCount: len(topology.BorrowedUsers),
		ActiveUserCount:   len(topology.ActiveUsers),
		RoomCount:         len(topology.Rooms),
		SubscriptionCount: len(topology.Subscriptions),
		StartedAt:         now,
		UpdatedAt:         now,
	}
	if err := store.PutManifest(ctx, &manifest); err != nil {
		return soakTopology{}, fmt.Errorf("record seeding manifest: %w", err)
	}
	if err := store.ResetOwned(ctx, input.RunID); err != nil {
		return soakTopology{}, fmt.Errorf("reset partial topology for run %q: %w", input.RunID, err)
	}
	if err := store.InsertOwnedRooms(ctx, input.RunID, topology.Rooms); err != nil {
		return soakTopology{}, fmt.Errorf("insert soak rooms: %w", err)
	}
	if err := SeedRoomKeys(ctx, keys, buildSoakRoomKeys(topology.Rooms, input.Seed)); err != nil {
		return soakTopology{}, fmt.Errorf("seed soak room keys: %w", err)
	}
	if err := store.InsertOwnedSubscriptions(ctx, input.RunID, topology.Subscriptions); err != nil {
		return soakTopology{}, fmt.Errorf("insert soak subscriptions: %w", err)
	}
	roomIDs := make([]string, len(topology.Rooms))
	for i := range topology.Rooms {
		roomIDs[i] = topology.Rooms[i].ID
	}
	chunks := chunkSoakRoomIDs(roomIDs, soakOwnershipChunkSize)
	if err := store.ReplaceOwnershipChunks(ctx, input.RunID, chunks); err != nil {
		return soakTopology{}, fmt.Errorf("record soak ownership: %w", err)
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

func buildSoakRoomKeys(rooms []model.Room, seed int64) map[string]roomkeystore.RoomKeyPair {
	rng := rand.New(rand.NewSource(seed))
	keys := make(map[string]roomkeystore.RoomKeyPair, len(rooms))
	for i := range rooms {
		keys[rooms[i].ID] = deterministicRoomKeyPair(rng)
	}
	return keys
}
