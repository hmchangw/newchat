package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadConfig_Defaults(t *testing.T) {
	t.Setenv("SITE_ID", "site-local")
	t.Setenv("MONGO_URI", "mongodb://x")
	cfg, err := loadConfig()
	require.NoError(t, err)
	assert.Equal(t, "8082", cfg.Port)
	assert.Equal(t, "chat", cfg.MongoDB)
	assert.Equal(t, 10, cfg.BcryptCost)
}

func TestLoadConfig_RequiresSiteAndMongo(t *testing.T) {
	_, err := loadConfig()
	assert.Error(t, err)
}
