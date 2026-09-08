package poolartifact

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeObjects is an in-memory object store keyed by "bucket/key".
type fakeObjects struct {
	data   map[string][]byte
	putErr error
	getErr error
}

func newFakeObjects() *fakeObjects { return &fakeObjects{data: map[string][]byte{}} }

func (f *fakeObjects) put(_ context.Context, bucket, key string, r io.Reader, _ int64) error {
	if f.putErr != nil {
		return f.putErr
	}
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	f.data[bucket+"/"+key] = b
	return nil
}

func (f *fakeObjects) get(_ context.Context, bucket, key string) (io.ReadCloser, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	b, ok := f.data[bucket+"/"+key]
	if !ok {
		return nil, errors.New("no such object")
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

func testArtifact() *Artifact {
	return &Artifact{
		RunID: "run-1", SiteID: "site-a", ConfigDigest: "dig-1",
		Accounts: []string{"alice", "bob"},
	}
}

// The object store is a transport hop, so the contract on both ends must be
// the one Load already enforces for files: same validation, same gzip rule.
func TestStore_PutLoadRoundTrip(t *testing.T) {
	f := newFakeObjects()
	s := newStore(f, "pool-bucket", "clientsim")
	key := s.Key("site-a", "run-1", "pool.json.gz")

	require.NoError(t, s.Put(context.Background(), key, testArtifact()))

	stored := f.data["pool-bucket/"+key]
	require.NotEmpty(t, stored)
	assert.Equal(t, []byte{0x1f, 0x8b}, stored[:2], "a .gz key must store gzip bytes")

	got, err := s.Load(context.Background(), key, "site-a")
	require.NoError(t, err)
	assert.Equal(t, []string{"alice", "bob"}, got.Accounts)
	assert.Equal(t, "run-1", got.RunID)
}

// Run-scoped keys are what make the object store double as the record of
// which accounts a given run used.
func TestStore_KeyIsRunScoped(t *testing.T) {
	s := newStore(newFakeObjects(), "b", "clientsim")
	assert.Equal(t, "clientsim/site-a/run-1/pool.json.gz", s.Key("site-a", "run-1", "pool.json.gz"))

	bare := newStore(newFakeObjects(), "b", "")
	assert.Equal(t, "site-a/run-1/pool.json.gz", bare.Key("site-a", "run-1", "pool.json.gz"),
		"an empty prefix must not leave a leading slash")
}

// The consumer must refuse an artifact belonging to another site as loudly
// over the wire as it does from a file — a mismatched pool connects accounts
// that do not exist on the site under test.
func TestStore_LoadRejectsAnotherSitesArtifact(t *testing.T) {
	f := newFakeObjects()
	s := newStore(f, "b", "p")
	key := s.Key("site-a", "run-1", "pool.json.gz")
	require.NoError(t, s.Put(context.Background(), key, testArtifact()))

	_, err := s.Load(context.Background(), key, "site-b")
	require.Error(t, err)
	assert.ErrorContains(t, err, "site")
}

// Credentials and location have no safe default: a half-configured store that
// silently pointed at the wrong bucket would surface as an empty pool hours
// into a run.
func TestStoreConfig_Validate(t *testing.T) {
	full := StoreConfig{Endpoint: "e", AccessKey: "a", SecretKey: "s", Bucket: "b"}
	require.NoError(t, full.Validate())

	tests := []struct {
		name string
		mut  func(*StoreConfig)
		want string
	}{
		{name: "no endpoint", mut: func(c *StoreConfig) { c.Endpoint = "" }, want: "POOL_S3_ENDPOINT"},
		{name: "no access key", mut: func(c *StoreConfig) { c.AccessKey = "" }, want: "POOL_S3_ACCESS_KEY"},
		{name: "no secret key", mut: func(c *StoreConfig) { c.SecretKey = "" }, want: "POOL_S3_SECRET_KEY"},
		{name: "no bucket", mut: func(c *StoreConfig) { c.Bucket = "" }, want: "POOL_S3_BUCKET"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := full
			tt.mut(&cfg)
			err := cfg.Validate()
			require.Error(t, err)
			assert.ErrorContains(t, err, tt.want)
		})
	}
}

// The URL is what a deployment sets, so its parse errors are startup errors.
func TestParsePoolURL(t *testing.T) {
	tests := []struct {
		name        string
		in          string
		bucket, key string
		wantErr     string
	}{
		{name: "bucket and key", in: "s3://my-bucket/clientsim/site-a/run-1/pool.json.gz",
			bucket: "my-bucket", key: "clientsim/site-a/run-1/pool.json.gz"},
		{name: "nested key", in: "s3://b/a/b/c.json.gz", bucket: "b", key: "a/b/c.json.gz"},
		{name: "no key", in: "s3://only-bucket", wantErr: "object key"},
		{name: "empty key", in: "s3://only-bucket/", wantErr: "object key"},
		{name: "wrong scheme", in: "http://host/key", wantErr: "s3://"},
		{name: "not a url", in: "::::", wantErr: "parse"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, key, err := ParsePoolURL(tt.in)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.bucket, bucket)
			assert.Equal(t, tt.key, key)
		})
	}
}

// Both tools accept a file path instead, so an unset store is a valid
// configuration. Configured is what lets them tell "not using S3" apart from
// "using S3, badly configured" — the second must fail at startup.
func TestStoreConfig_Configured(t *testing.T) {
	var unset StoreConfig
	assert.False(t, unset.Configured(), "an entirely unset store is not in use")
	defaultsOnly := StoreConfig{Prefix: "clientsim", UseSSL: true}
	assert.False(t, defaultsOnly.Configured(),
		"layout knobs carry envDefaults, so they cannot signal intent on their own")

	partial := StoreConfig{Bucket: "b"}
	assert.True(t, partial.Configured(), "one field set means someone meant to use it")
	assert.Error(t, partial.Validate(), "...and a partial config must then fail loudly")
}

func TestNewStore_RejectsIncompleteConfig(t *testing.T) {
	_, err := NewStore(&StoreConfig{Endpoint: "localhost:9000"})
	require.Error(t, err)
	assert.ErrorContains(t, err, "POOL_S3_ACCESS_KEY")
}

// Constructing the client must not dial: a consumer that only fails on the
// first read gives a clearer error than one that fails at startup for an
// endpoint that is merely slow to come up.
func TestNewStore_DoesNotDial(t *testing.T) {
	s, err := NewStore(&StoreConfig{
		Endpoint: "127.0.0.1:1", AccessKey: "a", SecretKey: "s", Bucket: "b", Prefix: "p",
	})
	require.NoError(t, err)
	assert.Equal(t, "p/site-a/run-1/pool.json.gz", s.Key("site-a", "run-1", "pool.json.gz"))
}

// A producer must not be able to publish something the consumer will refuse.
// Put runs the same validation Write does, before it touches the network.
func TestStore_PutRejectsWhatLoadWouldRefuse(t *testing.T) {
	f := newFakeObjects()
	s := newStore(f, "b", "p")
	tests := []struct {
		name string
		a    *Artifact
		want string
	}{
		{name: "no accounts", a: &Artifact{RunID: "r", SiteID: "s", ConfigDigest: "d"}, want: "empty accounts"},
		{name: "no runID", a: &Artifact{SiteID: "s", ConfigDigest: "d", Accounts: []string{"a"}}, want: "empty runID"},
		{name: "duplicate account", a: &Artifact{RunID: "r", SiteID: "s", ConfigDigest: "d",
			Accounts: []string{"a", "a"}}, want: "duplicate"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := s.Put(context.Background(), "k.json.gz", tt.a)
			require.Error(t, err)
			assert.ErrorContains(t, err, tt.want)
			assert.Empty(t, f.data, "nothing may be stored when validation failed")
		})
	}
}

// Transport failures reach the caller rather than yielding an empty pool: a
// consumer that started on a silently-missing artifact would connect nobody
// and report a healthy-looking zero.
func TestStore_PropagatesTransportErrors(t *testing.T) {
	boom := errors.New("network is unreachable")

	f := newFakeObjects()
	f.putErr = boom
	err := newStore(f, "b", "p").Put(context.Background(), "k.json.gz", testArtifact())
	assert.ErrorIs(t, err, boom)

	g := newFakeObjects()
	g.getErr = boom
	_, err = newStore(g, "b", "p").Load(context.Background(), "k.json.gz", "site-a")
	assert.ErrorIs(t, err, boom)
}

// An object stored under a key without .gz is plain JSON on both ends. The
// filename is the only thing deciding it, so the two directions cannot
// disagree — the same rule the file path uses.
func TestStore_UncompressedKeyRoundTrips(t *testing.T) {
	f := newFakeObjects()
	s := newStore(f, "b", "p")
	require.NoError(t, s.Put(context.Background(), "plain.json", testArtifact()))

	assert.Equal(t, byte('{'), f.data["b/plain.json"][0], "a key without .gz stores plain JSON")
	got, err := s.Load(context.Background(), "plain.json", "site-a")
	require.NoError(t, err)
	assert.Equal(t, []string{"alice", "bob"}, got.Accounts)
}
