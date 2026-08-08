package mongoutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

func TestParseReadPreference(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantMode readpref.Mode
		wantErr  bool
	}{
		{name: "empty defaults to primary", input: "", wantMode: readpref.PrimaryMode},
		{name: "whitespace defaults to primary", input: "   ", wantMode: readpref.PrimaryMode},
		{name: "primary", input: "primary", wantMode: readpref.PrimaryMode},
		{name: "primaryPreferred", input: "primaryPreferred", wantMode: readpref.PrimaryPreferredMode},
		{name: "secondary", input: "secondary", wantMode: readpref.SecondaryMode},
		{name: "secondaryPreferred", input: "secondaryPreferred", wantMode: readpref.SecondaryPreferredMode},
		{name: "nearest", input: "nearest", wantMode: readpref.NearestMode},
		{name: "case insensitive", input: "SecondaryPreferred", wantMode: readpref.SecondaryPreferredMode},
		{name: "surrounding whitespace trimmed", input: "  secondary  ", wantMode: readpref.SecondaryMode},
		{name: "invalid value errors", input: "quorum", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rp, err := ParseReadPreference(tc.input)
			if tc.wantErr {
				require.Error(t, err)
				assert.Nil(t, rp)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, rp)
			assert.Equal(t, tc.wantMode, rp.Mode())
		})
	}
}

func TestWithReadPreference(t *testing.T) {
	t.Run("sets read preference on connect config", func(t *testing.T) {
		cfg := newConnectConfig(WithReadPreference(readpref.Secondary()))
		require.NotNil(t, cfg.readPref)
		assert.Equal(t, readpref.SecondaryMode, cfg.readPref.Mode())
	})

	t.Run("nil read preference leaves config unset", func(t *testing.T) {
		cfg := newConnectConfig(WithReadPreference(nil))
		assert.Nil(t, cfg.readPref)
	})

	t.Run("composes with other options", func(t *testing.T) {
		cfg := newConnectConfig(WithReadPreference(readpref.SecondaryPreferred()))
		require.NotNil(t, cfg.readPref)
		assert.Equal(t, readpref.SecondaryPreferredMode, cfg.readPref.Mode())
	})
}

func TestCollection_WithReadPreference(t *testing.T) {
	// Connect is lazy — no server contact is needed to construct handles.
	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://localhost:27017"))
	require.NoError(t, err)
	t.Cleanup(func() { Disconnect(context.Background(), client) })
	base := NewCollection[bson.M](client.Database("testdb").Collection("things"))

	t.Run("nil returns the same receiver", func(t *testing.T) {
		assert.Same(t, base, base.WithReadPreference(nil))
	})

	t.Run("binds a distinct handle with the same name", func(t *testing.T) {
		sec := base.WithReadPreference(readpref.SecondaryPreferred())
		require.NotNil(t, sec)
		assert.NotSame(t, base, sec)
		assert.NotSame(t, base.Raw(), sec.Raw(), "must clone the underlying handle, not mutate the base")
		assert.Equal(t, base.Raw().Name(), sec.Raw().Name())
	})

	t.Run("does not mutate the base handle", func(t *testing.T) {
		before := base.Raw()
		_ = base.WithReadPreference(readpref.Secondary())
		assert.Same(t, before, base.Raw(), "base handle unchanged after cloning")
	})
}
