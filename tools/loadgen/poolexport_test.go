package main

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/poolartifact"
)

type fakeAccountSource struct {
	accounts []string
	err      error
	gotSite  string
}

func (f *fakeAccountSource) channelSubscriberAccounts(_ context.Context, siteID string) ([]string, error) {
	f.gotSite = siteID
	if f.err != nil {
		return nil, f.err
	}
	return f.accounts, nil
}

type fakePublisher struct {
	artifacts map[string]*poolartifact.Artifact
	blobs     map[string]any
	putErr    error
}

func newFakePublisher() *fakePublisher {
	return &fakePublisher{artifacts: map[string]*poolartifact.Artifact{}, blobs: map[string]any{}}
}

func (f *fakePublisher) Key(siteID, runID, name string) string {
	return siteID + "/" + runID + "/" + name
}

func (f *fakePublisher) Put(_ context.Context, key string, a *poolartifact.Artifact) error {
	if f.putErr != nil {
		return f.putErr
	}
	f.artifacts[key] = a
	return nil
}

func (f *fakePublisher) PutJSON(_ context.Context, key string, v any) error {
	if f.putErr != nil {
		return f.putErr
	}
	f.blobs[key] = v
	return nil
}

// The export publishes two objects under one run: the artifact the fleet
// reads, and a manifest that says how it was produced. The manifest is what
// makes the run reproducible months later — the artifact alone says who
// connected, not why those accounts.
func TestExportPool_PublishesArtifactAndManifest(t *testing.T) {
	src := &fakeAccountSource{accounts: []string{"anna", "bob", "cleo"}}
	pub := newFakePublisher()

	res, err := exportPool(context.Background(), src, pub, poolExportOptions{
		RunID: "run-1", SiteID: "site-a",
	})
	require.NoError(t, err)
	assert.Equal(t, "site-a", src.gotSite)
	assert.Equal(t, 3, res.Accounts)

	art := pub.artifacts["site-a/run-1/pool.json.gz"]
	require.NotNil(t, art, "the artifact must land under the run-scoped key")
	assert.Equal(t, []string{"anna", "bob", "cleo"}, art.Accounts)
	assert.Equal(t, "run-1", art.RunID)
	assert.NotEmpty(t, art.ConfigDigest)

	man, ok := pub.blobs["site-a/run-1/pool-manifest.json"].(poolManifest)
	require.True(t, ok, "the manifest must land beside it")
	assert.Equal(t, 3, man.Accounts)
	assert.Equal(t, art.ConfigDigest, man.ConfigDigest, "both halves must name the same population")
	assert.NotEmpty(t, man.Query, "the manifest records the query, or the run is not reproducible")
}

// The digest fingerprints the POPULATION, not the seed parameters — a Mongo
// export has no preset or RNG seed to hash. Two exports of the same accounts
// must agree, and one changed account must not.
func TestExportPool_DigestTracksThePopulation(t *testing.T) {
	digestOf := func(accounts []string) string {
		pub := newFakePublisher()
		_, err := exportPool(context.Background(), &fakeAccountSource{accounts: accounts}, pub,
			poolExportOptions{RunID: "r", SiteID: "s"})
		require.NoError(t, err)
		return pub.artifacts["s/r/pool.json.gz"].ConfigDigest
	}
	base := digestOf([]string{"anna", "bob"})
	assert.Equal(t, base, digestOf([]string{"anna", "bob"}), "the same population is the same digest")
	assert.NotEqual(t, base, digestOf([]string{"anna", "carol"}), "a changed population must be visible")
	assert.NotEqual(t, base, digestOf([]string{"anna", "bob", "cleo"}))
}

// A limit lets a run target a slice of a large site without connecting all of
// it. Truncation is from the head of a sorted list, so it is stable across
// exports rather than an arbitrary sample.
func TestExportPool_LimitTruncatesDeterministically(t *testing.T) {
	src := &fakeAccountSource{accounts: []string{"anna", "bob", "cleo", "dan"}}
	pub := newFakePublisher()

	res, err := exportPool(context.Background(), src, pub, poolExportOptions{
		RunID: "r", SiteID: "s", Limit: 2,
	})
	require.NoError(t, err)
	assert.Equal(t, 2, res.Accounts)
	assert.Equal(t, []string{"anna", "bob"}, pub.artifacts["s/r/pool.json.gz"].Accounts)
}

// An empty result is the failure this whole export exists to prevent: a fleet
// that starts against nobody reports a healthy zero. It must fail here, in
// the tool that can still say why.
func TestExportPool_RejectsAnEmptyPopulation(t *testing.T) {
	pub := newFakePublisher()
	_, err := exportPool(context.Background(), &fakeAccountSource{}, pub,
		poolExportOptions{RunID: "r", SiteID: "s"})
	require.Error(t, err)
	assert.ErrorContains(t, err, "no accounts")
	assert.Empty(t, pub.artifacts, "nothing may be published for an empty population")
}

func TestExportPool_PropagatesSourceAndSinkErrors(t *testing.T) {
	boom := errors.New("mongo is down")
	_, err := exportPool(context.Background(), &fakeAccountSource{err: boom}, newFakePublisher(),
		poolExportOptions{RunID: "r", SiteID: "s"})
	assert.ErrorIs(t, err, boom)

	pub := newFakePublisher()
	pub.putErr = boom
	_, err = exportPool(context.Background(), &fakeAccountSource{accounts: []string{"a"}}, pub,
		poolExportOptions{RunID: "r", SiteID: "s"})
	assert.ErrorIs(t, err, boom)
}
