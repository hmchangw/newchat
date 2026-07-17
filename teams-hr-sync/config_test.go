package main

import (
	"testing"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func validEnv() map[string]string {
	return map[string]string{
		"TEAMS_TENANT_ID":     "t",
		"TEAMS_CLIENT_ID":     "c",
		"TEAMS_CLIENT_SECRET": "s",
		"SYNC_GROUPS":         `[{"groupId":"g1","siteId":"site-a"}]`,
		"CENTRAL_SITE_ID":     "central",
		"MONGO_READ_URI":      "mongodb://localhost:27017",
		"NATS_URL":            "nats://localhost:4222",
	}
}

func TestConfig_Defaults(t *testing.T) {
	cfg, err := env.ParseAsWithOptions[config](env.Options{Environment: validEnv()})
	require.NoError(t, err)
	assert.Equal(t, 500, cfg.GraphPageSize)
	assert.Equal(t, "", cfg.GraphBaseURL)
	assert.Equal(t, "group", cfg.OrgType)
	assert.Equal(t, "chat", cfg.MongoReadDB)
}

func TestConfig_RequiredVars(t *testing.T) {
	for _, key := range []string{
		"TEAMS_TENANT_ID", "TEAMS_CLIENT_ID", "TEAMS_CLIENT_SECRET",
		"SYNC_GROUPS", "CENTRAL_SITE_ID", "MONGO_READ_URI", "NATS_URL",
	} {
		t.Run(key, func(t *testing.T) {
			e := validEnv()
			delete(e, key)
			_, err := env.ParseAsWithOptions[config](env.Options{Environment: e})
			assert.Error(t, err, "missing %s must fail fast", key)
		})
	}
}

func TestParseSyncGroups(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    []syncGroup
		wantErr string
	}{
		{
			name: "valid multi-group",
			raw:  `[{"groupId":"g1","siteId":"site-a"},{"groupId":"g2","siteId":"site-b"}]`,
			want: []syncGroup{{GroupID: "g1", SiteID: "site-a"}, {GroupID: "g2", SiteID: "site-b"}},
		},
		{name: "invalid json", raw: `{`, wantErr: "decode SYNC_GROUPS"},
		{name: "empty list", raw: `[]`, wantErr: "at least one group"},
		{name: "missing groupId", raw: `[{"siteId":"site-a"}]`, wantErr: "both required"},
		{name: "missing siteId", raw: `[{"groupId":"g1"}]`, wantErr: "both required"},
		{
			name:    "duplicate groupId",
			raw:     `[{"groupId":"g1","siteId":"site-a"},{"groupId":"g1","siteId":"site-b"}]`,
			wantErr: `duplicate groupId "g1"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseSyncGroups(tt.raw)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseSiteOverrides(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    map[string]string
		wantErr string
	}{
		{name: "valid", raw: `[{"account":"alice","siteId":"site-x"}]`, want: map[string]string{"alice": "site-x"}},
		{name: "empty string", raw: "", want: map[string]string{}},
		{name: "empty list", raw: `[]`, want: map[string]string{}},
		{name: "missing siteId", raw: `[{"account":"alice"}]`, wantErr: "both required"},
		{name: "missing account", raw: `[{"siteId":"site-x"}]`, wantErr: "both required"},
		{name: "duplicate account", raw: `[{"account":"a","siteId":"x"},{"account":"a","siteId":"y"}]`, wantErr: `duplicate account "a"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseSiteOverrides(tt.raw)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
