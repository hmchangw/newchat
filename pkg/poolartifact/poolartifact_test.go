package poolartifact

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func validArtifact() *Artifact {
	return &Artifact{RunID: "seed-medium-42", SiteID: "site-a",
		ConfigDigest: "abc123", Accounts: []string{"user-0", "user-1"}}
}

func TestWriteLoad_RoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pool.json")
	require.NoError(t, Write(path, validArtifact()))

	got, err := Load(path, "site-a")
	require.NoError(t, err)
	assert.Equal(t, SchemaVersion, got.SchemaVersion)
	assert.Equal(t, "seed-medium-42", got.RunID)
	assert.Equal(t, "abc123", got.ConfigDigest)
	assert.Equal(t, []string{"user-0", "user-1"}, got.Accounts)
}

func TestWrite_RejectsIncomplete(t *testing.T) {
	dir := t.TempDir()
	cases := []struct {
		name   string
		mutate func(*Artifact)
	}{
		{"empty accounts", func(a *Artifact) { a.Accounts = nil }},
		{"empty siteID", func(a *Artifact) { a.SiteID = "" }},
		{"empty runID", func(a *Artifact) { a.RunID = "" }},
		{"empty configDigest", func(a *Artifact) { a.ConfigDigest = "" }},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			a := validArtifact()
			tt.mutate(a)
			assert.Error(t, Write(filepath.Join(dir, "out.json"), a))
		})
	}
}

func TestLoad_FailFast(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pool.json")
	require.NoError(t, Write(path, validArtifact()))

	t.Run("siteID mismatch", func(t *testing.T) {
		_, err := Load(path, "site-b")
		assert.ErrorContains(t, err, "site")
	})
	t.Run("unknown schema version", func(t *testing.T) {
		// #nosec G304 -- reads the test's own TempDir fixture
		// nosemgrep: gosec.G304-1 -- reads the test's own TempDir fixture
		raw, err := os.ReadFile(path)
		require.NoError(t, err)
		mutated := strings.Replace(string(raw), `"schemaVersion": 1`, `"schemaVersion": 99`, 1)
		require.NotEqual(t, string(raw), mutated, "fixture must contain the schemaVersion field")
		bad := filepath.Join(t.TempDir(), "bad.json")
		// #nosec G703 -- writes the test's own TempDir fixture
		require.NoError(t, os.WriteFile(bad, []byte(mutated), 0o600))
		_, err = Load(bad, "site-a")
		assert.ErrorContains(t, err, "schema")
	})
	t.Run("empty accounts in file", func(t *testing.T) {
		bad := filepath.Join(t.TempDir(), "empty.json")
		require.NoError(t, os.WriteFile(bad,
			[]byte(`{"schemaVersion":1,"runId":"r","siteId":"site-a","configDigest":"d","accounts":[]}`), 0o600))
		_, err := Load(bad, "site-a")
		assert.ErrorContains(t, err, "accounts")
	})
	t.Run("missing file", func(t *testing.T) {
		_, err := Load(filepath.Join(t.TempDir(), "nope.json"), "site-a")
		assert.Error(t, err)
	})
	t.Run("malformed json", func(t *testing.T) {
		bad := filepath.Join(t.TempDir(), "garbage.json")
		require.NoError(t, os.WriteFile(bad, []byte("{"), 0o600))
		_, err := Load(bad, "site-a")
		assert.Error(t, err)
	})
}

func TestLoad_RejectsEmptyAccountEntry(t *testing.T) {
	bad := filepath.Join(t.TempDir(), "empty-entry.json")
	require.NoError(t, os.WriteFile(bad,
		[]byte(`{"schemaVersion":1,"runId":"r","siteId":"site-a","configDigest":"d","accounts":["user-0","","user-2"]}`), 0o600))
	_, err := Load(bad, "site-a")
	assert.ErrorContains(t, err, "account 1 is empty")
}

func TestLoad_RejectsOversizeArtifact(t *testing.T) {
	big := filepath.Join(t.TempDir(), "big.json")
	// One byte past the cap is enough; the reader must stop without slurping.
	require.NoError(t, os.WriteFile(big, make([]byte, maxArtifactBytes+1), 0o600))
	_, err := Load(big, "site-a")
	assert.ErrorContains(t, err, "cap")
}

func TestWrite_FailsOnUnwritablePath(t *testing.T) {
	err := Write(filepath.Join(t.TempDir(), "no-such-dir", "pool.json"), validArtifact())
	assert.ErrorContains(t, err, "write pool artifact")
}

func TestLoad_RejectsArtifactsMissingRunCorrelation(t *testing.T) {
	// Write requires both fields; Load accepting them empty let clientsim
	// start with blank run-correlation metadata, so a run's connections could
	// not be tied back to the seed that produced them.
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "no runId",
			raw:  `{"schemaVersion":1,"runId":"","siteId":"site-a","configDigest":"d","accounts":["u1"]}`,
			want: "no runID",
		},
		{
			name: "no configDigest",
			raw:  `{"schemaVersion":1,"runId":"r","siteId":"site-a","configDigest":"","accounts":["u1"]}`,
			want: "no configDigest",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "pool.json")
			require.NoError(t, os.WriteFile(path, []byte(tt.raw), 0o600))
			_, err := Load(path, "site-a")
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.want)
		})
	}
}

// The byte cap bounds the read, not the decode. A 64 MB file of short account
// names is millions of entries, and every one of them becomes a connection
// the tool tries to hold — an accidental pool file would take the site down
// rather than test it.
func TestLoad_RejectsAnImplausibleAccountCount(t *testing.T) {
	accounts := make([]string, maxAccounts+1)
	for i := range accounts {
		accounts[i] = "u"
	}
	path := filepath.Join(t.TempDir(), "pool.json")
	require.NoError(t, Write(path, &Artifact{
		RunID: "r", SiteID: "s", ConfigDigest: "d", Accounts: accounts,
	}))
	_, err := Load(path, "s")
	require.Error(t, err)
	assert.ErrorContains(t, err, "accounts")
}
