package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

func baseConfig() Config {
	return Config{BatchSize: 200, MaxWorkers: 8}
}

func TestValidateConfig(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr bool
	}{
		{"valid", func(*Config) {}, false},
		{"zero batch size", func(c *Config) { c.BatchSize = 0 }, true},
		{"negative batch size", func(c *Config) { c.BatchSize = -1 }, true},
		{"batch size at the inspector cap", func(c *Config) { c.BatchSize = model.TeamsRoomVerifyMaxChatIDs }, false},
		{"batch size above the inspector cap", func(c *Config) { c.BatchSize = model.TeamsRoomVerifyMaxChatIDs + 1 }, true},
		{"zero workers", func(c *Config) { c.MaxWorkers = 0 }, true},
		{"negative workers", func(c *Config) { c.MaxWorkers = -3 }, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := baseConfig()
			tt.mutate(&cfg)
			err := validateConfig(cfg)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			assert.NoError(t, err)
		})
	}
}

func TestParseSiteURLs(t *testing.T) {
	t.Run("valid registry", func(t *testing.T) {
		sites, err := parseSiteURLs(`{"site-a":"http://teams-room-inspector.site-a:8080","site-b":"http://teams-room-inspector.site-b:8080"}`)
		require.NoError(t, err)
		assert.Len(t, sites, 2)
		assert.Equal(t, "http://teams-room-inspector.site-a:8080", sites["site-a"])
	})

	tests := []struct {
		name string
		raw  string
	}{
		{"not json", `site-a=http://x`},
		{"wrong shape", `{"site-a":{"baseUrl":"http://x"}}`},
		{"empty object", `{}`},
		{"empty url", `{"site-a":""}`},
		{"blank string", ``},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseSiteURLs(tt.raw)
			require.Error(t, err)
		})
	}
}
