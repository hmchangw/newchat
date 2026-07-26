package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/v2/bson"
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
