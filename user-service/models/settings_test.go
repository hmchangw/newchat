package models

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestUserSettingsView_RoundTrip(t *testing.T) {
	in := UserSettingsView{
		Account:   "alice",
		SiteID:    "site-a",
		Data:      json.RawMessage(`{"theme":"dark"}`),
		Version:   7,
		UpdatedAt: time.Date(2026, time.July, 10, 12, 0, 0, 0, time.UTC),
	}
	b, err := json.Marshal(in)
	require.NoError(t, err)
	var out UserSettingsView
	require.NoError(t, json.Unmarshal(b, &out))
	require.Equal(t, in.Account, out.Account)
	require.Equal(t, in.SiteID, out.SiteID)
	require.Equal(t, []byte(in.Data), []byte(out.Data))
	require.Equal(t, in.Version, out.Version)
	require.True(t, in.UpdatedAt.Equal(out.UpdatedAt))
}

func TestUserSettingsView_DataIsOpaqueJSON(t *testing.T) {
	in := UserSettingsView{
		Account: "bob",
		SiteID:  "site-b",
		Data:    json.RawMessage(`{"channelSections":{"order":["favorites"],"collapsed":[],"sections":{}}}`),
		Version: 1,
	}
	b, err := json.Marshal(in)
	require.NoError(t, err)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(b, &raw))
	require.Equal(t, "bob", raw["account"])
	require.Equal(t, float64(1), raw["version"])
	require.NotEmpty(t, raw["data"])
}

func TestSetUserSettingsRequest_RoundTrip(t *testing.T) {
	version := int64(3)
	in := SetUserSettingsRequest{
		Data:      json.RawMessage(`{"theme":"light"}`),
		IfVersion: &version,
	}
	b, err := json.Marshal(in)
	require.NoError(t, err)
	var out SetUserSettingsRequest
	require.NoError(t, json.Unmarshal(b, &out))
	require.Equal(t, []byte(in.Data), []byte(out.Data))
	require.NotNil(t, out.IfVersion)
	require.Equal(t, *in.IfVersion, *out.IfVersion)
}

func TestSetUserSettingsRequest_IfVersionOmittedWhenNil(t *testing.T) {
	in := SetUserSettingsRequest{
		Data: json.RawMessage(`{"theme":"light"}`),
	}
	b, err := json.Marshal(in)
	require.NoError(t, err)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(b, &raw))
	_, present := raw["ifVersion"]
	require.False(t, present, "ifVersion must be omitted from JSON when nil")
	require.NotEmpty(t, raw["data"])
}
