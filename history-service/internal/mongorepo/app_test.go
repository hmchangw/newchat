//go:build integration

package mongorepo

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

// seedApps inserts one apps document per (id, assistantAccount, name) triple.
// An empty assistantAccount seeds an app with no assistant at all.
func seedApps(t *testing.T, repo *AppRepo, apps []model.App) {
	t.Helper()
	docs := make([]any, 0, len(apps))
	for i := range apps {
		docs = append(docs, apps[i])
	}
	_, err := repo.apps.Raw().InsertMany(context.Background(), docs)
	require.NoError(t, err)
}

func TestAppRepo_AppNamesByAccounts(t *testing.T) {
	db := setupMongo(t)
	repo := NewAppRepo(db)

	seedApps(t, repo, []model.App{
		{ID: "app-1", Name: "Helper Bot", Assistant: &model.AppAssistant{Enabled: true, Name: "helper.bot"}},
		{ID: "app-2", Name: "Weather Bot", Assistant: &model.AppAssistant{Enabled: true, Name: "weather.bot"}},
		{ID: "app-3", Name: "Unassisted App"},
	})

	t.Run("resolves every matching account in one read", func(t *testing.T) {
		names, err := repo.AppNamesByAccounts(context.Background(), []string{"helper.bot", "weather.bot"})
		require.NoError(t, err)
		assert.Equal(t, map[string]string{"helper.bot": "Helper Bot", "weather.bot": "Weather Bot"}, names)
	})

	t.Run("an account with no app is absent rather than an error", func(t *testing.T) {
		names, err := repo.AppNamesByAccounts(context.Background(), []string{"helper.bot", "ghost.bot"})
		require.NoError(t, err)
		assert.Equal(t, map[string]string{"helper.bot": "Helper Bot"}, names)
	})

	t.Run("no matches yields an empty map", func(t *testing.T) {
		names, err := repo.AppNamesByAccounts(context.Background(), []string{"ghost.bot"})
		require.NoError(t, err)
		assert.Empty(t, names)
	})

	t.Run("empty input reads nothing", func(t *testing.T) {
		names, err := repo.AppNamesByAccounts(context.Background(), nil)
		require.NoError(t, err)
		assert.Empty(t, names)
	})

	// The projection carries assistant.name because it is the map key; an app
	// without an assistant must not land in the map under the empty account.
	t.Run("an app with no assistant is skipped", func(t *testing.T) {
		names, err := repo.AppNamesByAccounts(context.Background(), []string{""})
		require.NoError(t, err)
		assert.Empty(t, names)
	})
}
