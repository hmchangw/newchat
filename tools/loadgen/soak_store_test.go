package main

import (
	"testing"

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
