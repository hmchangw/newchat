package main

import (
	"testing"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClusterBaseURL(t *testing.T) {
	c := config{ClusterDomains: clusterDomains{byID: map[string]string{"site-b": "https://avatar-b"}}}
	assert.Equal(t, "https://avatar-b", c.clusterBaseURL("site-b"))
	assert.Equal(t, "", c.clusterBaseURL("unknown"))
}

func TestClusterDomains_UnmarshalText(t *testing.T) {
	var cd clusterDomains
	require.NoError(t, cd.UnmarshalText([]byte(`[{"siteID":"s1","domain":"https://a"},{"siteID":"s2","domain":"https://b"}]`)))
	assert.Equal(t, "https://a", cd.baseURL("s1"))
	assert.Equal(t, "https://b", cd.baseURL("s2"))
	assert.Equal(t, "", cd.baseURL("missing"))

	assert.Error(t, (&clusterDomains{}).UnmarshalText([]byte(`not json`)))
}

func TestClusterDomains_UnmarshalText_RejectsDuplicateSiteID(t *testing.T) {
	var cd clusterDomains
	err := cd.UnmarshalText([]byte(`[{"siteID":"s1","domain":"https://a"},{"siteID":"s1","domain":"https://b"}]`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate siteID")
	assert.Contains(t, err.Error(), "s1")
}

// TestClusterDomains_EnvParse proves caarlos0/env honors the TextUnmarshaler, so
// CLUSTER_DOMAINS is populated from a JSON env string.
func TestClusterDomains_EnvParse(t *testing.T) {
	t.Setenv("CD", `[{"siteID":"s1","domain":"https://a"}]`)
	type probe struct {
		CD clusterDomains `env:"CD"`
	}
	p, err := env.ParseAs[probe]()
	require.NoError(t, err)
	assert.Equal(t, "https://a", p.CD.baseURL("s1"))
}

func TestConfig_EmojiAndNATSDefaults(t *testing.T) {
	t.Setenv("SITE_ID", "s1")
	t.Setenv("CLUSTER_DOMAINS", `[{"siteID":"s1","domain":"http://localhost:8080"}]`)
	t.Setenv("EMPLOYEE_PHOTO_BASE_URL", "https://photos.example.com")
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
	t.Setenv("MINIO_ENDPOINT", "localhost:9000")
	t.Setenv("MINIO_ACCESS_KEY", "k")
	t.Setenv("MINIO_SECRET_KEY", "s")
	t.Setenv("NATS_URL", "nats://localhost:4222")

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)
	assert.Equal(t, "nats://localhost:4222", cfg.NatsURL)
	assert.Empty(t, cfg.NatsCredsFile)
	assert.Equal(t, int64(262144), cfg.EmojiMaxUploadBytes)
	assert.Equal(t, 512, cfg.EmojiMaxDimension)
	assert.False(t, cfg.EmojiDeleteEnabled)
}

func TestConfig_NATSURLRequired(t *testing.T) {
	t.Setenv("SITE_ID", "s1")
	t.Setenv("CLUSTER_DOMAINS", `[]`)
	t.Setenv("EMPLOYEE_PHOTO_BASE_URL", "https://photos.example.com")
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
	t.Setenv("MINIO_ENDPOINT", "localhost:9000")
	t.Setenv("MINIO_ACCESS_KEY", "k")
	t.Setenv("MINIO_SECRET_KEY", "s")

	_, err := env.ParseAs[config]()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NATS_URL")
}
