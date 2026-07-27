package main

import (
	"testing"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLegacyRoomOrigins_UnmarshalText(t *testing.T) {
	var l legacyRoomOrigins
	require.NoError(t, l.UnmarshalText([]byte(
		`[{"siteID":"site-a","origin":"https://legacy.site-a.com"},`+
			`{"siteID":"site-b","origin":"https://legacy.site-b.com"}]`)))
	assert.Equal(t, "https://legacy.site-a.com", l.byID["site-a"])
	assert.Equal(t, "https://legacy.site-b.com", l.byID["site-b"])
	assert.Empty(t, l.byID["site-x"], "unconfigured site reads as empty string")
}

func TestLegacyRoomOrigins_UnmarshalText_Errors(t *testing.T) {
	var l legacyRoomOrigins
	assert.Error(t, l.UnmarshalText([]byte(`not-json`)))
	assert.ErrorContains(t,
		l.UnmarshalText([]byte(`[{"siteID":"s1","origin":"a"},{"siteID":"s1","origin":"b"}]`)),
		"duplicate siteID")
}

// LEGACY_ROOM_ORIGINS is populated from a JSON env string; unset or empty is
// valid and means every ${roomOrigin} substitutes to "".
func TestLegacyRoomOrigins_EnvParse(t *testing.T) {
	type originsConfig struct {
		LegacyRoomOrigins legacyRoomOrigins `env:"LEGACY_ROOM_ORIGINS"`
	}

	t.Setenv("LEGACY_ROOM_ORIGINS", `[{"siteID":"site-a","origin":"https://legacy.site-a.com"}]`)
	cfg, err := env.ParseAs[originsConfig]()
	require.NoError(t, err)
	assert.Equal(t, "https://legacy.site-a.com", cfg.LegacyRoomOrigins.byID["site-a"])

	t.Setenv("LEGACY_ROOM_ORIGINS", "")
	cfg, err = env.ParseAs[originsConfig]()
	require.NoError(t, err)
	assert.Empty(t, cfg.LegacyRoomOrigins.byID)

	t.Setenv("LEGACY_ROOM_ORIGINS", `[{"siteID":"s1"`)
	_, err = env.ParseAs[originsConfig]()
	assert.Error(t, err, "malformed JSON must fail startup, not be silently ignored")
}
