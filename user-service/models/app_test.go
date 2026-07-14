package models

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

func TestSetAppSubscriptionRequest_RoundTrip(t *testing.T) {
	in := SetAppSubscriptionRequest{AppID: "app-1", Subscribed: true}
	b, err := json.Marshal(in)
	require.NoError(t, err)
	var out SetAppSubscriptionRequest
	require.NoError(t, json.Unmarshal(b, &out))
	require.Equal(t, in, out)
}

func TestAppListItem_RoundTrip(t *testing.T) {
	t.Run("IsSubscribed true flattens with embedded App fields", func(t *testing.T) {
		in := AppListItem{
			App:          model.App{ID: "a1", Name: "Helper"},
			IsSubscribed: true,
		}
		b, err := json.Marshal(in)
		require.NoError(t, err)
		var out AppListItem
		require.NoError(t, json.Unmarshal(b, &out))
		require.Equal(t, in, out)
		// Assert that isSubscribed, id, and name are all top-level fields (embedded App flattens).
		var raw map[string]any
		require.NoError(t, json.Unmarshal(b, &raw))
		_, appNested := raw["app"]
		require.False(t, appNested, "App fields must not be nested under \"app\" key")
		_, idPresent := raw["id"]
		require.True(t, idPresent, "id must be a top-level JSON field")
		_, namePresent := raw["name"]
		require.True(t, namePresent, "name must be a top-level JSON field")
		isSubscribed, ok := raw["isSubscribed"]
		require.True(t, ok, "isSubscribed must be a top-level JSON field")
		require.Equal(t, true, isSubscribed)
	})

	t.Run("IsSubscribed false is present and not omitted", func(t *testing.T) {
		in := AppListItem{
			App:          model.App{ID: "a2", Name: "Bot"},
			IsSubscribed: false,
		}
		b, err := json.Marshal(in)
		require.NoError(t, err)
		var out AppListItem
		require.NoError(t, json.Unmarshal(b, &out))
		require.Equal(t, in, out)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(b, &raw))
		isSubscribed, ok := raw["isSubscribed"]
		require.True(t, ok, "isSubscribed must be present even when false (no omitempty)")
		require.Equal(t, false, isSubscribed)
	})
}

func TestOKResponse_RoundTrip(t *testing.T) {
	in := OKResponse{Success: true}
	b, err := json.Marshal(in)
	require.NoError(t, err)
	var out OKResponse
	require.NoError(t, json.Unmarshal(b, &out))
	require.Equal(t, in, out)
}

func TestAppsListResponse_RoundTrip(t *testing.T) {
	in := AppsListResponse{
		Apps: []AppListItem{
			{App: model.App{ID: "a1", Name: "Helper"}, IsSubscribed: true},
			{App: model.App{ID: "a2", Name: "Bot"}, IsSubscribed: false},
		},
		HasMore: true,
	}
	b, err := json.Marshal(in)
	require.NoError(t, err)
	var out AppsListResponse
	require.NoError(t, json.Unmarshal(b, &out))
	require.Equal(t, in, out)
}

func TestAppCategory_RoundTrip(t *testing.T) {
	in := AppCategory{ID: "64226446224a1b2c3d4e5f60", Name: "F22", SiteID: "00600000"}
	b, err := json.Marshal(in)
	require.NoError(t, err)
	var out AppCategory
	require.NoError(t, json.Unmarshal(b, &out))
	require.Equal(t, in, out)

	// The identifier is exposed as "id" (hex ObjectID), matching model.App and
	// the repo-wide client-facing convention — never the raw Mongo "_id".
	var raw map[string]any
	require.NoError(t, json.Unmarshal(b, &raw))
	require.Contains(t, raw, "id")
	require.NotContains(t, raw, "_id", "the wire key is id (matches model.App), never the raw Mongo _id")
	require.Contains(t, raw, "name")
	require.Contains(t, raw, "siteId")
}

func TestAppCategoriesResponse_RoundTrip(t *testing.T) {
	in := AppCategoriesResponse{Categories: []AppCategory{
		{ID: "64226446224a1b2c3d4e5f60", Name: "F22", SiteID: "00600000"},
		{ID: "64226446224a1b2c3d4e5f61", Name: "F14", SiteID: "00700000"},
	}}
	b, err := json.Marshal(in)
	require.NoError(t, err)
	var out AppCategoriesResponse
	require.NoError(t, json.Unmarshal(b, &out))
	require.Equal(t, in, out)
}

func TestAppCategoriesResponse_EmptyMarshalsAsArray(t *testing.T) {
	// A nil slice would marshal to null; the contract is always a JSON array.
	b, err := json.Marshal(AppCategoriesResponse{Categories: []AppCategory{}})
	require.NoError(t, err)
	require.JSONEq(t, `{"categories":[]}`, string(b))
}
