package main

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/poolartifact"
)

func TestWritePoolArtifact_AccountsInOrder(t *testing.T) {
	users := []model.User{
		{ID: "id-b", Account: "user-1"},
		{ID: "id-a", Account: "user-0"},
	}
	path := filepath.Join(t.TempDir(), "pool.json")

	require.NoError(t, writePoolArtifact(path, "seed-medium-42", "site-a", "d1g3st", users))

	a, err := poolartifact.Load(path, "site-a")
	require.NoError(t, err)
	assert.Equal(t, []string{"user-1", "user-0"}, a.Accounts,
		"fixture order preserved, accounts not IDs")
	assert.Equal(t, "d1g3st", a.ConfigDigest)
	assert.Equal(t, "seed-medium-42", a.RunID)
}

func TestWritePoolArtifact_EmptyUsers(t *testing.T) {
	err := writePoolArtifact(filepath.Join(t.TempDir(), "p.json"), "r", "s", "d", nil)
	assert.Error(t, err)
}

// Schema equivalence across the two seeders (spec §11): the local fixture
// path and the staging topology path write the same artifact shape.
func TestWritePoolArtifact_SchemaMatchesLoadRoundTrip(t *testing.T) {
	topo := []model.User{{ID: "mongo-hex-id", Account: "alice@corp"}}
	path := filepath.Join(t.TempDir(), "pool.json")
	require.NoError(t, writePoolArtifact(path, "soak-run-7", "site-a", "cfg", topo))
	a, err := poolartifact.Load(path, "site-a")
	require.NoError(t, err)
	assert.Equal(t, []string{"alice@corp"}, a.Accounts)
}

func TestSeedConfigDigest_DeterministicAndInputSensitive(t *testing.T) {
	d1 := seedConfigDigest("medium", 42, 1000)
	assert.Equal(t, d1, seedConfigDigest("medium", 42, 1000), "deterministic")
	assert.NotEqual(t, d1, seedConfigDigest("medium", 43, 1000), "seed-sensitive")
	assert.NotEqual(t, d1, seedConfigDigest("medium", 42, 2000), "users-sensitive")
	assert.NotEqual(t, d1, seedConfigDigest("large", 42, 1000), "preset-sensitive")
}

func TestSeedRejectsPoolOutOnUnsupportedWorkload(t *testing.T) {
	// The flag reaches only the messages and soak seeders. Accepting it
	// elsewhere returned success without an artifact, so clientsim would then
	// fail to start with nothing pointing back at this flag.
	code := runSeed(context.Background(), &config{}, []string{
		"--workload=members", "--preset=small", "--pool-out=/tmp/should-not-be-written.json",
	})
	assert.Equal(t, 2, code)
}
