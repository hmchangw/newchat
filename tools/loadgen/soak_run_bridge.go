package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/tools/loadgen/internal/soak/run"
	soaktopology "github.com/hmchangw/chat/tools/loadgen/internal/soak/topology"
)

// Root aliases keep the still-unextracted failure runtime compiling. The
// non-failure entry points switch to package run in the next commit.
type soakManifest = run.Manifest
type mongoSoakStore = run.Mongo
type soakSeedStore = run.SeedStore
type soakTeardownStore = run.TeardownStore
type soakCassandraCleaner = run.CassandraCleaner

const (
	soakManifestSeeding   = run.StateSeeding
	soakManifestSeeded    = run.StateSeeded
	soakManifestRunning   = run.StateRunning
	soakManifestStopped   = run.StateStopped
	soakManifestCompleted = run.StateCompleted
	soakManifestCleaned   = run.StateCleaned

	soakManifestCollection  = run.ManifestCollection
	soakOwnershipCollection = run.OwnershipCollection
	soakOwnershipChunkSize  = run.OwnershipChunkSize
)

func newMongoSoakStore(db *mongo.Database) *mongoSoakStore {
	return run.NewMongo(db)
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
	if input == nil || input.Config == nil {
		return soakTopology{}, fmt.Errorf("soak configuration is required")
	}
	cfg := input.Config
	return run.Seed(ctx, store, keys, &run.SeedInput{
		RunID: input.RunID, RunMode: cfg.RunMode, SiteID: input.SiteID,
		MongoDatabase: input.MongoDatabase, CassandraKeyspace: input.CassandraKeyspace,
		ConfigDigest: digestSoakConfig(cfg), HeartbeatStaleAfter: cfg.HeartbeatStaleAfter,
		Seed: input.Seed,
		Topology: soaktopology.BuildConfig{
			RunID: cfg.RunID, MaxUsers: cfg.MaxUsers, ActiveUsers: cfg.ActiveUsers,
			RoomCount: cfg.RoomCount, ChannelRatio: cfg.ChannelRatio,
			ChannelMembers: cfg.ChannelMembers,
		},
	}, ids)
}

func teardownSoak(
	ctx context.Context,
	store soakTeardownStore,
	cassandra soakCassandraCleaner,
	cfg *soakConfig,
	configuredKeyspace string,
) (bool, error) {
	if cfg == nil {
		return false, fmt.Errorf("soak configuration is required")
	}
	return run.Teardown(ctx, store, cassandra, &run.TeardownConfig{
		RunID: cfg.RunID, CassandraCleanup: cfg.CassandraCleanup,
		ConfirmKeyspace: cfg.ConfirmKeyspace, HeartbeatStaleAfter: cfg.HeartbeatStaleAfter,
		BatchRooms: cfg.TeardownBatchRooms, BatchDelay: cfg.TeardownBatchDelay,
		BatchTimeout: cfg.TeardownBatchTimeout,
	}, configuredKeyspace)
}

func digestSoakConfig(cfg *soakConfig) string {
	data, err := json.Marshal(cfg)
	if err != nil {
		panic("marshal soak config: " + err.Error())
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

var soakCreatedRoomPrefix = run.CreatedRoomPrefix
