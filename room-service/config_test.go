package main

import (
	"testing"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormalizeLegacyRoomOrigins(t *testing.T) {
	tests := []struct {
		name string
		in   map[string]string
		want map[string]string
	}{
		{name: "nil map yields empty map", in: nil, want: map[string]string{}},
		{
			name: "trims whitespace around keys and values",
			in:   map[string]string{" site-a": " https://legacy.site-a.com "},
			want: map[string]string{"site-a": "https://legacy.site-a.com"},
		},
		{
			name: "drops entries empty after trimming",
			in:   map[string]string{"site-a": "  ", "": "https://x.example.com"},
			want: map[string]string{},
		},
		{
			name: "clean entries pass through",
			in: map[string]string{
				"site-a": "https://legacy.site-a.com",
				"site-b": "https://legacy.site-b.com",
			},
			want: map[string]string{
				"site-a": "https://legacy.site-a.com",
				"site-b": "https://legacy.site-b.com",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, normalizeLegacyRoomOrigins(tt.in))
		})
	}
}

// TestLegacyRoomOrigins_EnvWireFormat pins the LEGACY_ROOM_ORIGINS wire
// format: comma-separated entries, first-colon key:value split (URL values
// keep "://"), spaces after the colon tolerated via normalization.
func TestLegacyRoomOrigins_EnvWireFormat(t *testing.T) {
	type originsConfig struct {
		LegacyRoomOrigins map[string]string `env:"LEGACY_ROOM_ORIGINS"`
	}

	t.Setenv("LEGACY_ROOM_ORIGINS", "site-a:https://legacy.site-a.com,site-b: https://legacy.site-b.com")
	cfg, err := env.ParseAs[originsConfig]()
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"site-a": "https://legacy.site-a.com",
		"site-b": "https://legacy.site-b.com",
	}, normalizeLegacyRoomOrigins(cfg.LegacyRoomOrigins))

	t.Setenv("LEGACY_ROOM_ORIGINS", "")
	cfg, err = env.ParseAs[originsConfig]()
	require.NoError(t, err, "empty value must not be a parse error")
	assert.Empty(t, normalizeLegacyRoomOrigins(cfg.LegacyRoomOrigins))
}
