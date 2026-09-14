//go:build integration

package poolartifact

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/testutil"
)

// The unit tests run against a fake object store, so nothing there exercises
// the real client: the content type, the size argument, and gzip bytes
// surviving a round trip through S3 are all only true if MinIO agrees.
func TestIntegration_Store_RoundTrip(t *testing.T) {
	_, bucket := testutil.MinIO(t, "poolartifact")
	endpoint, accessKey, secretKey := testutil.MinIOEndpoint(t)

	s, err := NewStore(&StoreConfig{
		Endpoint: endpoint, AccessKey: accessKey, SecretKey: secretKey,
		Bucket: bucket, Prefix: "clientsim", UseSSL: false,
	})
	require.NoError(t, err)

	ctx := context.Background()
	key := s.Key("site-a", "run-int", "pool.json.gz")
	want := &Artifact{
		RunID: "run-int", SiteID: "site-a", ConfigDigest: "dig",
		Accounts: []string{"alice", "bob", "carol"},
	}
	require.NoError(t, s.Put(ctx, key, want))

	got, err := s.Load(ctx, key, "site-a")
	require.NoError(t, err)
	assert.Equal(t, want.Accounts, got.Accounts)
	assert.Equal(t, want.RunID, got.RunID)
	assert.Equal(t, SchemaVersion, got.SchemaVersion)

	// A key that was never written must fail loudly, not yield an empty pool.
	_, err = s.Load(ctx, s.Key("site-a", "no-such-run", "pool.json.gz"), "site-a")
	assert.Error(t, err)
}

// A pool of realistic size is the case that matters for the transport hop:
// it is why the artifact is gzipped at all.
func TestIntegration_Store_LargePoolRoundTrip(t *testing.T) {
	_, bucket := testutil.MinIO(t, "poolartifact-large")
	endpoint, accessKey, secretKey := testutil.MinIOEndpoint(t)

	s, err := NewStore(&StoreConfig{
		Endpoint: endpoint, AccessKey: accessKey, SecretKey: secretKey,
		Bucket: bucket, UseSSL: false,
	})
	require.NoError(t, err)

	// Dot-free: an account carrying one is not a usable NATS subject token,
	// and this fixture asserting otherwise was the artifact validator's first
	// catch. Real non-bot accounts have no dots, and bots never reach a pool.
	accounts := make([]string, 30_000)
	for i := range accounts {
		accounts[i] = "stg-employee-" + strconv.Itoa(i)
	}
	ctx := context.Background()
	key := s.Key("site-a", "run-large", "pool.json.gz")
	require.NoError(t, s.Put(ctx, key, &Artifact{
		RunID: "run-large", SiteID: "site-a", ConfigDigest: "dig", Accounts: accounts,
	}))

	got, err := s.Load(ctx, key, "site-a")
	require.NoError(t, err)
	require.Len(t, got.Accounts, len(accounts))
	assert.Equal(t, accounts[0], got.Accounts[0])
	assert.Equal(t, accounts[len(accounts)-1], got.Accounts[len(accounts)-1])
}

// The conditional write is the whole overwrite guard, and MinIO's If-None-Match
// is a server-side extension — the fake cannot prove the server honours it.
func TestIntegration_Store_PutIfAbsentClaimsOnce(t *testing.T) {
	client, bucket := testutil.MinIO(t, "poolartifact")
	s := newStore(minioObjects{c: client}, bucket, "p")
	key := s.Key("site-a", "run-1", "pool.json.gz")
	ctx := context.Background()

	first := &Artifact{RunID: "run-1", SiteID: "site-a", ConfigDigest: "d1", Accounts: []string{"anna", "bob"}}
	require.NoError(t, s.PutIfAbsent(ctx, key, first))

	second := &Artifact{RunID: "run-1", SiteID: "site-a", ConfigDigest: "d2", Accounts: []string{"mallory"}}
	err := s.PutIfAbsent(ctx, key, second)
	require.ErrorIs(t, err, ErrObjectExists, "the server must reject the second claim")

	got, err := s.Load(ctx, key, "site-a")
	require.NoError(t, err)
	assert.Equal(t, []string{"anna", "bob"}, got.Accounts, "the first writer's population must survive")
}
