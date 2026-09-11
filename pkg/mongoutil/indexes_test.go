package mongoutil

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// resolveIndexOptions materializes a builder's setters into the options struct
// the driver would send, so a test can assert on what a restore requests.
func resolveIndexOptions(t *testing.T, m mongo.IndexModel) *options.IndexOptions {
	t.Helper()
	var got options.IndexOptions
	for _, set := range m.Options.List() {
		require.NoError(t, set(&got))
	}
	return &got
}

func TestExistingIndex_Model_TTL(t *testing.T) {
	tests := []struct {
		name string
		spec bson.M
		want *int32
	}{
		{
			name: "no ttl",
			spec: bson.M{},
			want: nil,
		},
		{
			name: "int32 ttl preserved",
			spec: bson.M{"expireAfterSeconds": int32(3600)},
			want: ptr(int32(3600)),
		},
		{
			name: "int64 ttl preserved",
			spec: bson.M{"expireAfterSeconds": int64(3600)},
			want: ptr(int32(3600)),
		},
		{
			name: "int64 ttl at int32 max preserved",
			spec: bson.M{"expireAfterSeconds": int64(math.MaxInt32)},
			want: ptr(int32(math.MaxInt32)),
		},
		{
			name: "int64 ttl above int32 max dropped, never wrapped",
			spec: bson.M{"expireAfterSeconds": int64(math.MaxInt32) + 1},
			want: nil,
		},
		{
			name: "negative int64 ttl dropped",
			spec: bson.M{"expireAfterSeconds": int64(-1)},
			want: nil,
		},
		{
			name: "negative int32 ttl dropped",
			spec: bson.M{"expireAfterSeconds": int32(-1)},
			want: nil,
		},
		{
			name: "non-numeric ttl dropped",
			spec: bson.M{"expireAfterSeconds": "3600"},
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := existingIndex{name: "createdAt_1", keys: bson.D{{Key: "createdAt", Value: 1}}, spec: tt.spec}

			got := resolveIndexOptions(t, e.model(context.Background()))

			if tt.want == nil {
				assert.Nil(t, got.ExpireAfterSeconds)
				return
			}
			require.NotNil(t, got.ExpireAfterSeconds)
			assert.Equal(t, *tt.want, *got.ExpireAfterSeconds)
		})
	}
}

func TestExistingIndex_Model_PreservesSpec(t *testing.T) {
	keys := bson.D{{Key: "account", Value: 1}, {Key: "siteId", Value: -1}}
	filter := bson.M{"deleted": false}
	e := existingIndex{
		name: "account_1_siteId_-1",
		keys: keys,
		spec: bson.M{
			"unique":                  true,
			"sparse":                  true,
			"hidden":                  true,
			"partialFilterExpression": filter,
		},
	}

	m := e.model(context.Background())
	got := resolveIndexOptions(t, m)

	assert.Equal(t, keys, m.Keys)
	require.NotNil(t, got.Name)
	assert.Equal(t, "account_1_siteId_-1", *got.Name)
	assert.Equal(t, ptr(true), got.Unique)
	assert.Equal(t, ptr(true), got.Sparse)
	assert.Equal(t, ptr(true), got.Hidden)
	assert.Equal(t, filter, got.PartialFilterExpression)
}

func TestExistingIndex_Model_OmitsUnsetOptions(t *testing.T) {
	e := existingIndex{
		name: "account_1",
		keys: bson.D{{Key: "account", Value: 1}},
		spec: bson.M{"unique": false, "sparse": false, "hidden": false},
	}

	got := resolveIndexOptions(t, e.model(context.Background()))

	assert.Nil(t, got.Unique)
	assert.Nil(t, got.Sparse)
	assert.Nil(t, got.Hidden)
	assert.Nil(t, got.PartialFilterExpression)
	assert.Nil(t, got.ExpireAfterSeconds)
}

func ptr[T any](v T) *T { return &v }

func TestMissingUniqueIndexes(t *testing.T) {
	tests := []struct {
		name          string
		have          map[string]bool // index name -> unique
		names         []string
		wantAbsent    []string
		wantNonUnique []string
	}{
		{
			name:  "all present and unique",
			have:  map[string]bool{"a_1": true, "b_1": true},
			names: []string{"a_1", "b_1"},
		},
		{
			name:       "absent index reported as absent",
			have:       map[string]bool{"a_1": true},
			names:      []string{"a_1", "b_1"},
			wantAbsent: []string{"b_1"},
		},
		{
			name:          "present but non-unique reported separately, not as absent",
			have:          map[string]bool{"a_1": false},
			names:         []string{"a_1"},
			wantNonUnique: []string{"a_1"},
		},
		{
			name:          "mixed",
			have:          map[string]bool{"a_1": true, "b_1": false},
			names:         []string{"a_1", "b_1", "c_1"},
			wantAbsent:    []string{"c_1"},
			wantNonUnique: []string{"b_1"},
		},
		{
			name: "no names wanted",
			have: map[string]bool{"a_1": false},
		},
		{
			name:       "empty listing",
			have:       map[string]bool{},
			names:      []string{"a_1"},
			wantAbsent: []string{"a_1"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			absent, nonUnique := missingUniqueIndexes(tt.have, tt.names...)
			assert.Equal(t, tt.wantAbsent, absent)
			assert.Equal(t, tt.wantNonUnique, nonUnique)
		})
	}
}
