//go:build integration

package testutil

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// IndexSpecs maps each of coll's index key specs ("field:dir,...", bson.D order
// preserved) to whether that index is unique, so a test asserts the exact
// compound index and its options rather than an index count.
func IndexSpecs(t *testing.T, coll *mongo.Collection) map[string]bool {
	t.Helper()
	cur, err := coll.Indexes().List(context.Background())
	require.NoError(t, err)
	var idx []struct {
		Key    bson.D `bson:"key"`
		Unique bool   `bson:"unique"`
	}
	require.NoError(t, cur.All(context.Background(), &idx))
	out := make(map[string]bool, len(idx))
	for _, ix := range idx {
		parts := make([]string, 0, len(ix.Key))
		for _, e := range ix.Key {
			parts = append(parts, fmt.Sprintf("%s:%v", e.Key, e.Value))
		}
		out[strings.Join(parts, ",")] = ix.Unique
	}
	return out
}
