package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

func TestSoakOwnershipIDFilter_UsesRunScopedPrimaryKeyRange(t *testing.T) {
	assert.Equal(t, bson.D{{
		Key: "_id",
		Value: bson.D{
			{Key: "$gte", Value: "run-a:"},
			{Key: "$lt", Value: "run-a;"},
		},
	}}, soakOwnershipIDFilter("run-a", ""))

	assert.Equal(t, bson.D{{
		Key: "_id",
		Value: bson.D{
			{Key: "$gt", Value: "run-a:000001"},
			{Key: "$lt", Value: "run-a;"},
		},
	}}, soakOwnershipIDFilter("run-a", "run-a:000001"))
}

func TestSoakManifestWriteConcern_IsMajorityAndJournaled(t *testing.T) {
	concern := soakManifestWriteConcern()

	require.NotNil(t, concern)
	assert.Equal(t, writeconcern.WCMajority, concern.W)
	require.NotNil(t, concern.Journal)
	assert.True(t, *concern.Journal)
}

func TestSoakHeartbeatUpdate_AdvancesTheLeaseMonotonically(t *testing.T) {
	at := time.Date(2026, 8, 25, 9, 0, 0, 0, time.UTC)

	update := soakHeartbeatUpdate(at)

	// $max, not $set: an attempt abandoned at its client timeout can still be
	// executing server-side and land after a later retry has already written a
	// newer beat. Under $set that regresses the persisted lease teardown reads
	// while the loop keeps trusting the newer one it saw acknowledged.
	require.Len(t, update, 1)
	assert.Equal(t, "$max", update[0].Key)
	assert.Equal(t, bson.D{
		{Key: "lastHeartbeatAt", Value: at},
		{Key: "updatedAt", Value: at},
	}, update[0].Value)
}
