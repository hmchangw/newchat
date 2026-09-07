package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSoakRuntimeComposition_MapsSeedAndTeardownInputs(t *testing.T) {
	soak := soakConfig{
		RunID: "run-a", RunMode: "continuous", MaxUsers: 100,
		ActiveUsers: 20, RoomCount: 30, ChannelRatio: 0.4, ChannelMembers: 5,
		HeartbeatStaleAfter: 3 * time.Minute,
		CassandraCleanup:    "none", ConfirmKeyspace: "confirm",
		TeardownBatchRooms: 50, TeardownBatchDelay: 100 * time.Millisecond,
		TeardownBatchTimeout: 10 * time.Second,
	}
	cfg := config{
		SiteID: "site-a", MongoDB: "mongo-a", CassandraKeyspace: "cass-a",
		Soak: soak,
	}

	seed := soakSeedInputFrom(&cfg, 42)
	require.NotNil(t, seed)
	assert.Equal(t, "run-a", seed.RunID)
	assert.Equal(t, "continuous", seed.RunMode)
	assert.Equal(t, "site-a", seed.SiteID)
	assert.Equal(t, "mongo-a", seed.MongoDatabase)
	assert.Equal(t, "cass-a", seed.CassandraKeyspace)
	assert.Equal(t, int64(42), seed.Seed)
	assert.Equal(t, "run-a", seed.Topology.RunID)
	assert.Equal(t, 100, seed.Topology.MaxUsers)
	assert.Equal(t, 20, seed.Topology.ActiveUsers)
	assert.Equal(t, 30, seed.Topology.RoomCount)
	assert.Equal(t, 0.4, seed.Topology.ChannelRatio)
	assert.Equal(t, 5, seed.Topology.ChannelMembers)
	assert.NotEmpty(t, seed.ConfigDigest)

	teardown := soakTeardownConfigFrom(&soak)
	require.NotNil(t, teardown)
	assert.Equal(t, "run-a", teardown.RunID)
	assert.Equal(t, "none", teardown.CassandraCleanup)
	assert.Equal(t, "confirm", teardown.ConfirmKeyspace)
	assert.Equal(t, 3*time.Minute, teardown.HeartbeatStaleAfter)
	assert.Equal(t, 50, teardown.BatchRooms)
	assert.Equal(t, 100*time.Millisecond, teardown.BatchDelay)
	assert.Equal(t, 10*time.Second, teardown.BatchTimeout)

	assert.Nil(t, soakSeedInputFrom(nil, 42))
	assert.Nil(t, soakTeardownConfigFrom(nil))
}

func TestSoakRuntimeComposition_UsesExtractedPackageEntryPoints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		file      string
		forbidden []string
	}{
		{
			file: "main.go",
			forbidden: []string{
				"newMongoSoakStore", "teardownSoak",
			},
		},
		{
			file: "soak_main.go",
			forbidden: []string{
				"seedSoak", "newMongoSoakStore", "newProductionSoakIDs",
				"newSoakCatalog", "newSoakRPCClient", "newSoakReader",
				"newSoakVerifier", "newSoakMutationScheduler", "newSoakMutator",
				"newSoakSearchReader", "newNATSSoakResponseSource", "newSoakSender",
				"startSoakSendResponsesWithObserver", "newNATSSoakPresencePublisher",
				"newSoakPresenceLane", "newSoakWorkload",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.file, func(t *testing.T) {
			t.Parallel()
			parsed, err := parser.ParseFile(token.NewFileSet(), tt.file, nil, 0)
			require.NoError(t, err)
			seen := make(map[string]bool)
			ast.Inspect(parsed, func(node ast.Node) bool {
				identifier, ok := node.(*ast.Ident)
				if ok {
					seen[identifier.Name] = true
				}
				return true
			})
			for _, name := range tt.forbidden {
				assert.Falsef(t, seen[name], "%s still composes through %s", tt.file, name)
			}
		})
	}
}
