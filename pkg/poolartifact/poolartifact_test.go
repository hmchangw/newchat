package poolartifact

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
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
	// Written directly rather than through Write, which now rejects the same
	// oversized artifact — this fixture exists to exercise the READ path.
	path := writeOversizedFixture(t)
	_, err := Load(path, "s")
	require.Error(t, err)
	assert.ErrorContains(t, err, "accounts")
}

// A duplicated account is counted in the shard the readiness floor is measured
// against, but the swarm deliberately starts it only once — so the target can
// never be reached, and MIN_READY_RATIO either fails the run for a reason that
// has nothing to do with the system under test or silently absorbs the gap in
// its slack. Across shards it is worse: two pods each connect the same account,
// and every room they share double-counts its deliveries.
func TestLoad_RejectsDuplicateAccounts(t *testing.T) {
	// Written directly: Write now rejects the same input, so it can no longer
	// be used to produce a file that only Load refuses.
	path := filepath.Join(t.TempDir(), "pool.json")
	require.NoError(t, os.WriteFile(path,
		[]byte(`{"schemaVersion":1,"runId":"r","siteId":"s","configDigest":"d","accounts":["user-0","user-1","user-0"]}`), 0o600))
	_, err := Load(path, "s")
	require.Error(t, err)
	assert.ErrorContains(t, err, "duplicate")
	assert.ErrorContains(t, err, "user-0")
}

func TestLoad_AcceptsDistinctAccounts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pool.json")
	require.NoError(t, Write(path, &Artifact{
		RunID: "r", SiteID: "s", ConfigDigest: "d",
		Accounts: []string{"user-0", "user-1", "user-2"},
	}))
	a, err := Load(path, "s")
	require.NoError(t, err)
	assert.Len(t, a.Accounts, 3)
}

// writeAccountFixture emits an artifact with n accounts, as raw JSON text so
// building the fixture does not itself cost a slice of n strings.
func writeAccountFixture(t *testing.T, n int) string {
	t.Helper()
	var b bytes.Buffer
	b.WriteString(`{"schemaVersion":1,"runId":"r","siteId":"s","configDigest":"d","accounts":[`)
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(`"u"`)
	}
	b.WriteString(`]}`)
	path := filepath.Join(t.TempDir(), "pool.json")
	require.NoError(t, os.WriteFile(path, b.Bytes(), 0o600))
	return path
}

func writeOversizedFixture(t *testing.T) string {
	t.Helper()
	return writeAccountFixture(t, maxAccounts+1)
}

// Write and Load must agree on what a valid artifact is. A seeder that can
// emit an artifact the consumer refuses at startup turns a bad --users value
// into a failure hours later, in the wrong tool.
func TestWrite_RejectsAnImplausibleAccountCount(t *testing.T) {
	accounts := make([]string, maxAccounts+1)
	for i := range accounts {
		accounts[i] = "u" + strconv.Itoa(i)
	}
	err := Write(filepath.Join(t.TempDir(), "pool.json"), &Artifact{
		RunID: "r", SiteID: "s", ConfigDigest: "d", Accounts: accounts,
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "accounts")
}

// The byte cap bounds the FILE, not the decode. Unmarshalling into []string
// materialises every account in the file before any count check can run, so
// the cost tracked the file's CONTENTS: a file three times over the cap cost
// three times as much to reject. Decoding the array incrementally and
// abandoning it at the cap makes the cost track the CAP instead — which is
// the only thing the cap can honestly promise, since a legitimate pool of
// maxAccounts costs that much either way.
func TestLoad_DecodeCostTracksTheCapNotTheFile(t *testing.T) {
	measure := func(path string) uint64 {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		_, err := Load(path, "s")
		require.Error(t, err)
		runtime.ReadMemStats(&after)
		return after.TotalAlloc - before.TotalAlloc
	}
	justOver := measure(writeAccountFixture(t, maxAccounts+1))
	farOver := measure(writeAccountFixture(t, maxAccounts*3))

	assert.Less(t, farOver, justOver*2,
		"rejecting a file three times over the cap must not cost three times as much")
}

// Write is the producer half of the same contract Load enforces. Anything
// Write persists but Load refuses turns a bad seed into a failure hours later,
// in the wrong tool and with the seeder already gone.
func TestWrite_RejectsWhatLoadRejects(t *testing.T) {
	tests := []struct {
		name     string
		accounts []string
		want     string
	}{
		{name: "empty entry", accounts: []string{"u1", "", "u3"}, want: "account 1 is empty"},
		{name: "duplicate", accounts: []string{"u1", "u2", "u1"}, want: "duplicate account"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "pool.json")
			err := Write(path, &Artifact{
				RunID: "r", SiteID: "s", ConfigDigest: "d", Accounts: tt.accounts,
			})
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.want)
			// Nothing is left behind for a later run to pick up.
			_, statErr := os.Stat(path)
			assert.True(t, os.IsNotExist(statErr), "a rejected artifact must not be written")
		})
	}
}

// The artifact has to travel through a ConfigMap-sized or object-store hop,
// and 30k real account names run past 1 MiB uncompressed. Writing and reading
// .gz keeps the same contract on both ends.
func TestWriteLoad_GzipRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pool.json.gz")
	want := &Artifact{
		RunID: "r1", SiteID: "site-a", ConfigDigest: "d1",
		Accounts: []string{"alice", "bob", "carol"},
	}
	require.NoError(t, Write(path, want))

	// #nosec G304 -- reads the test's own TempDir fixture
	// nosemgrep: gosec.G304-1 -- reads the test's own TempDir fixture
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, []byte{0x1f, 0x8b}, raw[:2], "a .gz path must produce gzip bytes, not plain JSON")

	got, err := Load(path, "site-a")
	require.NoError(t, err)
	assert.Equal(t, want.Accounts, got.Accounts)
	assert.Equal(t, SchemaVersion, got.SchemaVersion)
	assert.Equal(t, "r1", got.RunID)
	assert.Equal(t, "d1", got.ConfigDigest)
}

// The byte cap has to bind the DECOMPRESSED stream. Bounding the file instead
// would let a few KiB of gzip expand into gigabytes of heap before any count
// check runs — the same hazard the streaming decoder closed for plain JSON,
// reopened by compression.
func TestLoad_GzipCapsTheDecompressedStream(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bomb.json.gz")
	// #nosec G304 -- writes the test's own TempDir fixture
	// nosemgrep: gosec.G304-1 -- writes the test's own TempDir fixture
	f, err := os.Create(path)
	require.NoError(t, err)
	zw := gzip.NewWriter(f)
	// Highly compressible filler: tiny on disk, far past the cap expanded.
	chunk := bytes.Repeat([]byte{'a'}, 1<<20)
	for written := int64(0); written <= maxArtifactBytes; written += int64(len(chunk)) {
		_, err := zw.Write(chunk)
		require.NoError(t, err)
	}
	require.NoError(t, zw.Close())
	require.NoError(t, f.Close())

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Less(t, info.Size(), int64(maxArtifactBytes),
		"the fixture must be small on disk, or it would not be testing the bomb case")

	_, err = Load(path, "site-a")
	require.Error(t, err)
	assert.ErrorContains(t, err, "cap")
}

// A truncated or non-gzip file under a .gz name is a deployment mistake, and
// it must say so rather than surfacing as a JSON parse error.
func TestLoad_RejectsCorruptGzip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "corrupt.json.gz")
	require.NoError(t, os.WriteFile(path, []byte("this is not gzip"), 0o600))
	_, err := Load(path, "site-a")
	require.Error(t, err)
	assert.ErrorContains(t, err, "gzip")
}

// writeGzipFiller writes a .gz whose DECOMPRESSED size is about n bytes, from
// filler that compresses hard — so the file on disk stays tiny however large
// n is. That gap is the whole point: it is what a byte cap on the compressed
// stream would fail to see.
func writeGzipFiller(t *testing.T, n int64) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "filler.json.gz")
	// #nosec G304 -- writes the test's own TempDir fixture
	// nosemgrep: gosec.G304-1 -- writes the test's own TempDir fixture
	f, err := os.Create(path)
	require.NoError(t, err)
	zw := gzip.NewWriter(f)
	chunk := bytes.Repeat([]byte{'a'}, 1<<20)
	for written := int64(0); written < n; written += int64(len(chunk)) {
		_, err := zw.Write(chunk)
		require.NoError(t, err)
	}
	require.NoError(t, zw.Close())
	require.NoError(t, f.Close())
	return path
}

// The same promise as TestLoad_DecodeCostTracksTheCapNotTheFile, one layer
// out. Compression breaks the file-size proxy the plain path relied on: a few
// KiB of gzip expands to whatever the writer chose, so a reader that
// decompresses first and checks the size afterwards pays for the ATTACKER's
// number, not the cap's. Reading through a bounded reader makes the cost
// track the cap again.
func TestLoad_GzipRejectionCostTracksTheCapNotTheExpansion(t *testing.T) {
	measure := func(path string) uint64 {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		_, err := Load(path, "s")
		require.Error(t, err)
		runtime.ReadMemStats(&after)
		return after.TotalAlloc - before.TotalAlloc
	}
	justOver := measure(writeGzipFiller(t, maxArtifactBytes+(1<<20)))
	farOver := measure(writeGzipFiller(t, maxArtifactBytes*3))

	assert.Less(t, farOver, justOver*2,
		"rejecting a bomb that expands to three times the cap must not cost three times as much")
}

// The pool's accounts become NATS subject tokens. subject.UserSubscriptionList
// PANICS on one carrying a dot, wildcard, whitespace or control rune, so an
// artifact holding one takes the clientsim pod down at startup rather than
// failing it cleanly. Reject at both ends, using the same validator the
// subject builders use, so the two cannot drift.
func TestValidateAccounts_RejectsUnsafeSubjectTokens(t *testing.T) {
	for _, bad := range []string{"weather.site-a.bot", "*", ">", "has space", "ctl\x01"} {
		err := Write(filepath.Join(t.TempDir(), "p.json"), &Artifact{
			RunID: "r", SiteID: "s", ConfigDigest: "d", Accounts: []string{"ok", bad},
		})
		require.Error(t, err, "account %q must be refused by the producer", bad)
		assert.Contains(t, err.Error(), "subject token")
	}
}

// The account cap is symmetric between Write and Load for a stated reason —
// a producer that can emit what the consumer refuses turns a bad input into a
// failure hours later, in the wrong tool. That rule was applied to the count
// and not to the byte size, leaving one window open: a population inside the
// account cap whose names are long enough to blow the byte cap. Narrow, but
// the consequence is every pod refusing an artifact that published cleanly.
func TestWrite_RefusesAnArtifactAboveTheReadCap(t *testing.T) {
	// Few accounts, each enormous: the account cap cannot fire here, so this
	// isolates the byte cap rather than passing through the other guard.
	long := strings.Repeat("a", 100_000)
	accounts := make([]string, 0, 700)
	for i := range 700 {
		accounts = append(accounts, fmt.Sprintf("%s%06d", long, i))
	}
	err := Write(filepath.Join(t.TempDir(), "p.json"), &Artifact{
		RunID: "r", SiteID: "s", ConfigDigest: "d", Accounts: accounts,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cap")
}
