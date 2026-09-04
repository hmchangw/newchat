package main

import (
	"os"
	"testing"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConfig_MongoURIRequired guards the session-token branch's store: the
// sessions collection is the only validator now, so a pod without a Mongo URI
// must refuse to start rather than 503 every authToken request.
func TestConfig_MongoURIRequired(t *testing.T) {
	t.Setenv("AUTH_SCOPED_SIGNING_KEY", "seed")
	t.Setenv("AUTH_ACCOUNT_PUB_KEY", "pub")
	t.Setenv("MONGO_URI", "") // pin cleanup so the host value is restored
	require.NoError(t, os.Unsetenv("MONGO_URI"))

	_, err := env.ParseAs[config]()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "MONGO_URI")
}

func TestConfig_MongoDBDefault(t *testing.T) {
	t.Setenv("AUTH_SCOPED_SIGNING_KEY", "seed")
	t.Setenv("AUTH_ACCOUNT_PUB_KEY", "pub")
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")

	cfg, err := env.ParseAs[config]()

	require.NoError(t, err)
	assert.Equal(t, "chat", cfg.MongoDB)
}
