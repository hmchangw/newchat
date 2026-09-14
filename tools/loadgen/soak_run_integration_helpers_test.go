//go:build integration

package main

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/tools/loadgen/internal/soak/run"
	soaktopology "github.com/hmchangw/chat/tools/loadgen/internal/soak/topology"
)

// These helpers keep the existing root integration scenarios concise while
// exercising the extracted run package through its public API.
type mongoSoakStore = run.Mongo
type soakSeedStore = run.SeedStore
type soakTeardownStore = run.TeardownStore
type soakCassandraCleaner = run.CassandraCleaner

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
	return run.Teardown(
		ctx,
		store,
		cassandra,
		soakTeardownConfigFrom(cfg),
		configuredKeyspace,
	)
}

func newProductionSoakIDs() *soakIDs {
	return soaktopology.NewProductionIdentitySource()
}
