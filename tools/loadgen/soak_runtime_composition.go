package main

import (
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/run"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/topology"
)

func soakSeedInputFrom(cfg *config, seed int64) *run.SeedInput {
	if cfg == nil {
		return nil
	}
	soak := &cfg.Soak
	return &run.SeedInput{
		RunID: soak.RunID, RunMode: soak.RunMode, SiteID: cfg.SiteID,
		MongoDatabase: cfg.MongoDB, CassandraKeyspace: cfg.CassandraKeyspace,
		ConfigDigest:        digestSoakConfig(soak),
		HeartbeatStaleAfter: soak.HeartbeatStaleAfter,
		Seed:                seed,
		Topology: topology.BuildConfig{
			RunID: soak.RunID, MaxUsers: soak.MaxUsers,
			ActiveUsers: soak.ActiveUsers, RoomCount: soak.RoomCount,
			ChannelRatio: soak.ChannelRatio, ChannelMembers: soak.ChannelMembers,
		},
	}
}

func soakTeardownConfigFrom(cfg *soakConfig) *run.TeardownConfig {
	if cfg == nil {
		return nil
	}
	return &run.TeardownConfig{
		RunID: cfg.RunID, CassandraCleanup: cfg.CassandraCleanup,
		ConfirmKeyspace:     cfg.ConfirmKeyspace,
		HeartbeatStaleAfter: cfg.HeartbeatStaleAfter,
		BatchRooms:          cfg.TeardownBatchRooms, BatchDelay: cfg.TeardownBatchDelay,
		BatchTimeout: cfg.TeardownBatchTimeout,
	}
}
