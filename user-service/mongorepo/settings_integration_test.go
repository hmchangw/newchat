//go:build integration

package mongorepo

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/testutil"
)

func newTestSettingsRepo(t *testing.T) (*SettingsRepo, *mongo.Database) {
	t.Helper()
	db := testutil.MongoDB(t, "user-service-settings")
	r := NewSettingsRepo(db)
	require.NoError(t, r.EnsureIndexes(context.Background()))
	return r, db
}

// AC-2.1 — a missing settings document is represented by (nil, nil).
func TestSettingsRepository_GetUserSettings_Missing_Integration(t *testing.T) {
	r, _ := newTestSettingsRepo(t)

	settings, err := r.GetUserSettings(context.Background(), "alice", "site-a")

	require.NoError(t, err)
	assert.Nil(t, settings)
}

// AC-2.2 — the first unconditional write creates version 1 with the supplied data and a recent timestamp.
func TestSettingsRepository_SetUserSettings_InitialVersion_Integration(t *testing.T) {
	r, _ := newTestSettingsRepo(t)
	data := json.RawMessage(`{"theme":"dark","density":"compact"}`)
	before := time.Now().UTC()

	settings, err := r.SetUserSettings(context.Background(), "alice", "site-a", data, nil)

	require.NoError(t, err)
	require.NotNil(t, settings)
	assert.Equal(t, "alice", settings.Account)
	assert.Equal(t, "site-a", settings.SiteID)
	assert.Equal(t, int64(1), settings.Version)
	assert.Equal(t, data, []byte(settings.Data))
	assert.WithinDuration(t, time.Now().UTC(), settings.UpdatedAt, time.Minute)
	assert.False(t, settings.UpdatedAt.Before(before))
}

// AC-2.3 — a matching conditional version atomically updates the document and increments its version.
func TestSettingsRepository_SetUserSettings_MatchingVersion_Integration(t *testing.T) {
	r, db := newTestSettingsRepo(t)
	seed(t, db, settingsCollection, bson.M{
		"account": "alice", "siteId": "site-a", "data": []byte(`{"revision":3}`),
		"version": int64(3), "updatedAt": time.Now().UTC(), "internal": "not projected",
	})
	ifVersion := int64(3)

	settings, err := r.SetUserSettings(context.Background(), "alice", "site-a", []byte(`{"revision":4}`), &ifVersion)

	require.NoError(t, err)
	require.NotNil(t, settings)
	assert.Equal(t, int64(4), settings.Version)
	assert.Equal(t, []byte(`{"revision":4}`), []byte(settings.Data))
	stored, err := r.GetUserSettings(context.Background(), "alice", "site-a")
	require.NoError(t, err)
	require.NotNil(t, stored)
	assert.Equal(t, int64(4), stored.Version)
}

// AC-2.4 — a stale conditional version returns a typed conflict and leaves the stored document unchanged.
func TestSettingsRepository_SetUserSettings_StaleVersionConflict_Integration(t *testing.T) {
	r, db := newTestSettingsRepo(t)
	originalData := json.RawMessage(`{"revision":3}`)
	seed(t, db, settingsCollection, bson.M{
		"account": "alice", "siteId": "site-a", "data": originalData,
		"version": int64(3), "updatedAt": time.Now().UTC(),
	})
	ifVersion := int64(2)

	settings, err := r.SetUserSettings(context.Background(), "alice", "site-a", []byte(`{"revision":4}`), &ifVersion)

	assert.Nil(t, settings)
	var coded *errcode.Error
	require.ErrorAs(t, err, &coded)
	assert.Equal(t, errcode.CodeConflict, coded.Code)
	stored, getErr := r.GetUserSettings(context.Background(), "alice", "site-a")
	require.NoError(t, getErr)
	require.NotNil(t, stored)
	assert.Equal(t, int64(3), stored.Version)
	assert.Equal(t, originalData, []byte(stored.Data))
}

// AC-2.5 — concurrent unconditional writes each apply atomically and produce version 2 with one complete payload.
func TestSettingsRepository_SetUserSettings_ConcurrentUnconditionalWrites_Integration(t *testing.T) {
	r, _ := newTestSettingsRepo(t)
	ctx := context.Background()
	start := make(chan struct{})
	dataA := json.RawMessage(`{"writer":"a","value":"alpha"}`)
	dataB := json.RawMessage(`{"writer":"b","value":"bravo"}`)
	results := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	for _, data := range []json.RawMessage{dataA, dataB} {
		go func(data json.RawMessage) {
			defer wg.Done()
			<-start
			_, err := r.SetUserSettings(ctx, "alice", "site-a", data, nil)
			results <- err
		}(data)
	}
	close(start)
	wg.Wait()
	close(results)
	for err := range results {
		require.NoError(t, err)
	}

	stored, err := r.GetUserSettings(ctx, "alice", "site-a")
	require.NoError(t, err)
	require.NotNil(t, stored)
	assert.Equal(t, int64(2), stored.Version)
	assert.Contains(t, []string{string(dataA), string(dataB)}, string(stored.Data))
}

// AC-2.6 — concurrent conditional writes with the same expected version have exactly one winner and one conflict.
func TestSettingsRepository_SetUserSettings_ConcurrentConditionalWrites_Integration(t *testing.T) {
	r, _ := newTestSettingsRepo(t)
	ctx := context.Background()
	_, err := r.SetUserSettings(ctx, "alice", "site-a", []byte(`{"writer":"initial"}`), nil)
	require.NoError(t, err)

	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	for _, writer := range []string{"a", "b"} {
		go func(writer string) {
			defer wg.Done()
			<-start
			ifVersion := int64(1)
			_, err := r.SetUserSettings(ctx, "alice", "site-a", json.RawMessage(`{"writer":"`+writer+`"}`), &ifVersion)
			results <- err
		}(writer)
	}
	close(start)
	wg.Wait()
	close(results)

	var successes, conflicts int
	for err := range results {
		if err == nil {
			successes++
			continue
		}
		var coded *errcode.Error
		require.True(t, errors.As(err, &coded))
		assert.Equal(t, errcode.CodeConflict, coded.Code)
		conflicts++
	}
	assert.Equal(t, 1, successes)
	assert.Equal(t, 1, conflicts)
	stored, err := r.GetUserSettings(ctx, "alice", "site-a")
	require.NoError(t, err)
	require.NotNil(t, stored)
	assert.Equal(t, int64(2), stored.Version)
}

func TestSettingsRepository_UniqueAccountSiteIndex_Integration(t *testing.T) {
	r, db := newTestSettingsRepo(t)
	ctx := context.Background()
	seed(t, db, settingsCollection, bson.M{
		"account": "alice", "siteId": "site-a", "version": int64(1), "data": []byte(`{}`),
	})

	_, err := db.Collection(settingsCollection).InsertOne(ctx, bson.M{
		"account": "alice", "siteId": "site-a", "version": int64(1), "data": []byte(`{}`),
	})

	require.Error(t, err)
	assert.True(t, mongo.IsDuplicateKeyError(err))
	assert.NotNil(t, r)
}
