package main

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/poolartifact"
)

type fakeAccountSource struct {
	gotLimit int
	accounts []string
	err      error
	gotSite  string
}

// channelSubscriberAccounts mirrors the pipeline: bots are excluded BEFORE the
// bound, so the bound counts only accounts that survive to the artifact. A fake
// that ignored the bound would make every test about it vacuous.
func (f *fakeAccountSource) channelSubscriberAccounts(_ context.Context, siteID string, limit int) ([]string, error) {
	f.gotSite = siteID
	f.gotLimit = limit
	if f.err != nil {
		return nil, f.err
	}
	out := dropUnusable(f.accounts)
	if n := cursorLimit(limit); len(out) > n {
		out = out[:n]
	}
	return out, nil
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

// PutIfAbsent models the store's conditional write: the claim is the write,
// so a second writer loses rather than overwriting.
func (f *fakePublisher) PutIfAbsent(_ context.Context, key string, a *poolartifact.Artifact) error {
	if f.putErr != nil {
		return f.putErr
	}
	if _, exists := f.artifacts[key]; exists {
		return fmt.Errorf("%w: %s", poolartifact.ErrObjectExists, key)
	}
	f.artifacts[key] = a
	return nil
}

func (f *fakePublisher) Load(_ context.Context, key, _ string) (*poolartifact.Artifact, error) {
	a, ok := f.artifacts[key]
	if !ok {
		return nil, poolartifact.ErrObjectNotFound
	}
	return a, nil
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

// Overwriting a run ID with a DIFFERENT population breaks the invariant the
// single object exists to hold. Pods that started before the overwrite and
// pods that restart after would slice different arrays, so shardSlice would
// hand out overlapping or disjoint ranges — accounts connected twice or not
// at all, with every pod still reporting ready.
func TestExportPool_RefusesToOverwriteADifferentPopulation(t *testing.T) {
	pub := newFakePublisher()
	opts := poolExportOptions{RunID: "run-1", SiteID: "site-a"}

	_, err := exportPool(context.Background(), &fakeAccountSource{accounts: []string{"anna", "bob"}}, pub, opts)
	require.NoError(t, err)

	// The same population is genuinely idempotent: a retried Job must not fail.
	_, err = exportPool(context.Background(), &fakeAccountSource{accounts: []string{"anna", "bob"}}, pub, opts)
	require.NoError(t, err, "re-exporting the same population is a safe retry")

	// A changed one is not.
	_, err = exportPool(context.Background(), &fakeAccountSource{accounts: []string{"anna", "carol"}}, pub, opts)
	require.Error(t, err)
	assert.ErrorContains(t, err, "--run-id")
	assert.Equal(t, []string{"anna", "bob"}, pub.artifacts["site-a/run-1/pool.json.gz"].Accounts,
		"the published pool must be left as it was")
}

// The run ID becomes a path segment. path.Join normalises "..", so an
// unvalidated one could write the artifact into another site's scope.
func TestValidatePoolRunID(t *testing.T) {
	for _, ok := range []string{"run-1", "soak20260908a", "a.b_c-d"} {
		assert.NoError(t, validatePoolRunID(ok), ok)
	}
	for _, bad := range []string{"", "..", ".", "../site-b/r", "a/b", "-leading", "with space"} {
		assert.Error(t, validatePoolRunID(bad), bad)
	}
}

// A bot that owns a room holds a genuine channel subscription
// (bot-room-service/handler.go:213-216 writes IsBot:true with RoomTypeChannel),
// so the Mongo filter alone is one flag away from letting one through. It must
// not reach the artifact: clientsim authenticates on the user JWT path bots
// never take, and a dotted ".bot" account spans subject tokens — it panics
// subject.UserSubscriptionList before a request is ever made. The drop is here
// rather than only in the query so it holds for any source, and so a row whose
// isBot was never stored is still caught.
func TestExportPool_DropsBotAccounts(t *testing.T) {
	src := &fakeAccountSource{accounts: []string{
		"anna", "legacy.site-a.bot", "bob", "weather.site-a.bot",
	}}
	pub := newFakePublisher()

	res, err := exportPool(context.Background(), src, pub, poolExportOptions{
		RunID: "run-1", SiteID: "site-a",
	})
	require.NoError(t, err)

	art := pub.artifacts["site-a/run-1/pool.json.gz"]
	require.NotNil(t, art)
	assert.Equal(t, []string{"anna", "bob"}, art.Accounts)
	assert.Equal(t, 2, res.Accounts, "the reported count is the population that will connect")
}

// A site whose only channel subscribers are bots is empty for clientsim's
// purposes. It must fail like any other empty population rather than publish
// an artifact no pod can use.
func TestExportPool_RejectsAPopulationOfOnlyBots(t *testing.T) {
	src := &fakeAccountSource{accounts: []string{"weather.site-a.bot", "alerts.site-a.bot"}}

	_, err := exportPool(context.Background(), src, newFakePublisher(), poolExportOptions{
		RunID: "run-1", SiteID: "site-a",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no accounts")
}

// --run-id was validated as a single path segment; siteId, its immediate
// neighbour in the same path.Join, was not. One guarded segment beside an
// unguarded one is not a guard: SITE_ID=../other escapes the same way.
func TestValidatePoolSegments_GuardsBothPathSegments(t *testing.T) {
	assert.NoError(t, validatePoolRunID("run-1"))
	assert.NoError(t, validatePoolSiteID("site-a"))

	for _, bad := range []string{"..", "../other", "a/b", ""} {
		assert.Error(t, validatePoolRunID(bad), "--run-id %q must be rejected", bad)
		assert.Error(t, validatePoolSiteID(bad), "SITE_ID %q must be rejected", bad)
	}
}

// The claim is now the write, so the retry path has to be proven rather than
// assumed: a Job that reruns on the same population must succeed, not trip
// its own guard.
func TestExportPool_SamePopulationIsAnIdempotentRetry(t *testing.T) {
	src := &fakeAccountSource{accounts: []string{"anna", "bob"}}
	pub := newFakePublisher()
	opts := poolExportOptions{RunID: "run-1", SiteID: "site-a"}

	first, err := exportPool(context.Background(), src, pub, opts)
	require.NoError(t, err)

	second, err := exportPool(context.Background(), src, pub, opts)
	require.NoError(t, err, "re-exporting the same population must be a safe retry")
	assert.Equal(t, first.ConfigDigest, second.ConfigDigest)
	assert.Equal(t, []string{"anna", "bob"}, pub.artifacts[first.ArtifactKey].Accounts)
}

// The retry path compared existing.ConfigDigest — a field the stored artifact
// reports about ITSELF. An artifact whose digest disagrees with its accounts
// (a truncated write, a hand-edited object) would be accepted as "the same
// population" and the fleet would connect to something nobody exported.
func TestExportPool_RetryVerifiesTheStoredAccountsNotItsSelfReportedDigest(t *testing.T) {
	src := &fakeAccountSource{accounts: []string{"anna", "bob"}}
	pub := newFakePublisher()
	opts := poolExportOptions{RunID: "run-1", SiteID: "site-a"}

	first, err := exportPool(context.Background(), src, pub, opts)
	require.NoError(t, err)

	// The stored digest still claims the original population; the accounts no
	// longer match it.
	pub.artifacts[first.ArtifactKey].Accounts = []string{"mallory"}

	_, err = exportPool(context.Background(), src, pub, opts)
	require.Error(t, err, "a stored artifact whose accounts contradict its digest must not be accepted")
}

// The server-side bound must apply to the population that SURVIVES bot
// removal, not before it. Bounding first and filtering after silently
// under-delivers --limit whenever a legacy ".bot" account sits in the sorted
// head: the caller asks for N, the site holds more than N eligible accounts,
// and the run still gets fewer.
func TestExportPool_LimitIsHonouredWhenABotSitsInTheHead(t *testing.T) {
	src := &fakeAccountSource{accounts: []string{
		"aaa", "bbb.bot", "ccc", "ddd", "eee", "fff",
	}}
	pub := newFakePublisher()

	res, err := exportPool(context.Background(), src, pub, poolExportOptions{
		RunID: "run-1", SiteID: "site-a", Limit: 3,
	})
	require.NoError(t, err)
	assert.Equal(t, 3, res.Accounts, "--limit 3 must deliver 3 eligible accounts, not 3-minus-the-bots")
	assert.Equal(t, []string{"aaa", "ccc", "ddd"}, pub.artifacts[res.ArtifactKey].Accounts)
}

// The empty account is the other half of the same ordering defect. It sorts
// FIRST (""), so it consumes a --limit slot before the Go pass drops it: a
// site whose rows include one empty account reports "no accounts" for
// --limit=1 while holding plenty of eligible ones.
func TestExportPool_AnEmptyAccountDoesNotConsumeALimitSlot(t *testing.T) {
	src := &fakeAccountSource{accounts: []string{"", "aaa", "bbb"}}
	pub := newFakePublisher()

	res, err := exportPool(context.Background(), src, pub, poolExportOptions{
		RunID: "run-1", SiteID: "site-a", Limit: 1,
	})
	require.NoError(t, err, "a leading empty account must not empty the population")
	assert.Equal(t, 1, res.Accounts)
	assert.Equal(t, []string{"aaa"}, pub.artifacts[res.ArtifactKey].Accounts)
}

// The manifest is what makes a run reproducible months later. A recorded query
// that describes a different selection than the pipeline runs is worse than no
// record: it looks authoritative and is wrong.
func TestPoolExportQuery_DescribesThePipelineItRuns(t *testing.T) {
	assert.Contains(t, poolExportQuery, "group", "the exclusion runs after the group; the record must say so")
	assert.NotContains(t, poolExportQuery, `u.isBot: {$ne: true}} ->`,
		"that shape claims the flag is filtered on rows before dedup, which is the bug this pipeline fixed")
}
