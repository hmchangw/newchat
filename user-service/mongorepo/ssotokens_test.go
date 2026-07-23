//go:build integration

package mongorepo

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestSSOTokenRepo_UpsertInsertsWithGeneratedID(t *testing.T) {
	r, db := newTestSSOTokenRepo(t)

	require.NoError(t, r.Upsert(context.Background(), "alice", "access-1", 1735689600000, "refresh-1"))

	var raw bson.M
	require.NoError(t, db.Collection("sso_tokens").
		FindOne(context.Background(), bson.M{"username": "alice"}).Decode(&raw))
	assert.Len(t, raw["_id"], 17, "new docs get 17-char idgen.GenerateID ids")
	assert.Equal(t, "access-1", raw["idToken"])
	assert.Equal(t, "1735689600000", raw["idTokenExp"], "persisted as a decimal-millis string")
	assert.Equal(t, "refresh-1", raw["refreshToken"])
	assert.NotNil(t, raw["_updatedAt"])
}

func TestSSOTokenRepo_UpsertUpdatesKeepingID(t *testing.T) {
	r, db := newTestSSOTokenRepo(t)
	// Simulate a migrated legacy doc (foreign _id kept verbatim; exp is a string).
	seed(t, db, "sso_tokens", bson.M{
		"_id": "legacyMeteorId17c", "username": "bob",
		"idToken": "old-access", "idTokenExp": "1000", "refreshToken": "old-refresh",
		"_updatedAt": time.Now().Add(-time.Hour),
	})

	require.NoError(t, r.Upsert(context.Background(), "bob", "new-access", 2000, "new-refresh"))

	got, err := r.GetByUsername(context.Background(), "bob")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "new-access", got.IDToken)
	assert.Equal(t, int64(2000), got.IDTokenExp, "millis string parsed back to int64")
	assert.Equal(t, "new-refresh", got.RefreshToken)

	var raw bson.M
	require.NoError(t, db.Collection("sso_tokens").
		FindOne(context.Background(), bson.M{"username": "bob"}).Decode(&raw))
	assert.Equal(t, "legacyMeteorId17c", raw["_id"], "update keeps the legacy _id")
}

func TestSSOTokenRepo_GetByUsernameMissingIsNilNil(t *testing.T) {
	r, _ := newTestSSOTokenRepo(t)
	got, err := r.GetByUsername(context.Background(), "nobody")
	require.NoError(t, err)
	assert.Nil(t, got)
}

// idTokenExp is a string: a numeric millis string parses to int64; a non-numeric/odd legacy value parses to 0 (safe — reads as expired) rather than erroring the read.
func TestSSOTokenRepo_GetByUsernameStringExpDecode(t *testing.T) {
	r, db := newTestSSOTokenRepo(t)
	seed(t, db, "sso_tokens", bson.M{
		"_id": "legacyMillsId17c", "username": "dave",
		"idToken": "acc", "idTokenExp": "1735689600000", "refreshToken": "ref",
	})
	got, err := r.GetByUsername(context.Background(), "dave")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, int64(1735689600000), got.IDTokenExp)

	// Non-numeric legacy value → 0, no error.
	seed(t, db, "sso_tokens", bson.M{
		"_id": "legacyOddId17chr", "username": "erin",
		"idToken": "acc2", "idTokenExp": "not-a-number", "refreshToken": "ref2",
	})
	got, err = r.GetByUsername(context.Background(), "erin")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, int64(0), got.IDTokenExp)
}

func TestSSOTokenRepo_UsernameUniqueIndex(t *testing.T) {
	r, db := newTestSSOTokenRepo(t)
	require.NoError(t, r.Upsert(context.Background(), "carol", "a", 1, "r"))
	_, err := db.Collection("sso_tokens").InsertOne(context.Background(),
		bson.M{"_id": "otherid", "username": "carol", "idToken": "b", "idTokenExp": "2", "refreshToken": "r2"})
	require.Error(t, err)
}
