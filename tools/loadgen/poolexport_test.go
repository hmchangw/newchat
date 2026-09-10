package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
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
	// raw models a source that neither filters nor reports — the case the belt
	// under it exists for. The Mongo source does both, so on that path the
	// belt finds nothing; a test about what the belt catches needs a source
	// that hands it something.
	raw bool
}

// channelSubscriberAccounts mirrors the real source: it applies the usability
// rule, bounds on what SURVIVES it, and reports what it dropped. A fake that
// filtered without reporting would make every test about the skip evidence
// pass while production reported nothing — the defect this contract exists to
// prevent.
func (f *fakeAccountSource) channelSubscriberAccounts(_ context.Context, siteID string, limit int) (poolCandidates, error) {
	f.gotSite = siteID
	f.gotLimit = limit
	if f.err != nil {
		return poolCandidates{}, f.err
	}
	if f.raw {
		// A source that does none of it, which is what the belt is for.
		return poolCandidates{Accounts: f.accounts}, nil
	}
	kept, skipped := dropUnusable(f.accounts)
	if n := cursorLimit(limit); len(kept) > n {
		kept = kept[:n]
	}
	return poolCandidates{Accounts: kept, Skipped: len(skipped), Sample: sampleAccounts(skipped)}, nil
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
	assert.Contains(t, err.Error(), "none a clientsim run can connect as")
	assert.Contains(t, err.Error(), "weather.site-a.bot", "the failure must name what it dropped")
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
	assert.Contains(t, poolExportQuery, "usable",
		"the bound applies to usable accounts, and the record has to say which selection produced the pool")
}

// The staging population holds leftovers from other load tools —
// "k6.test-1.user" is a real one. Nothing about it is a bot: no isBot flag, no
// ".bot" suffix. Its dots still span subject tokens, so it reached
// poolartifact's validator and failed the WHOLE export: one stale row from a
// tool nobody is running any more blocks every run against that site. It is
// the same class as an empty account or a bot — an account clientsim cannot
// connect as — and belongs in the same drop.
func TestExportPool_DropsAccountsThatCannotBeSubjectTokens(t *testing.T) {
	src := &fakeAccountSource{raw: true, accounts: []string{
		"anna", "k6.test-1.user", "bob", "has space", "wild*card", "tail>token", "ctrl\x07name",
	}}
	pub := newFakePublisher()

	res, err := exportPool(context.Background(), src, pub, poolExportOptions{
		RunID: "run-1", SiteID: "site-a",
	})
	require.NoError(t, err, "one unusable account must not fail an export the rest of the site can serve")

	art := pub.artifacts["site-a/run-1/pool.json.gz"]
	require.NotNil(t, art)
	assert.Equal(t, []string{"anna", "bob"}, art.Accounts)
	assert.Equal(t, 2, res.Accounts)
}

// Skipping quietly is the other way to be wrong: the day the count goes from
// one stale k6 row to the whole site, the operator has to be able to see it.
// The export reports what it dropped so the caller can say so.
func TestExportPool_ReportsWhatItSkipped(t *testing.T) {
	src := &fakeAccountSource{accounts: []string{"anna", "k6.test-1.user", "weather.site-a.bot", ""}}
	pub := newFakePublisher()

	res, err := exportPool(context.Background(), src, pub, poolExportOptions{
		RunID: "run-1", SiteID: "site-a",
	})
	require.NoError(t, err)
	assert.Equal(t, 3, res.Skipped, "every dropped account is counted, whatever disqualified it")
	assert.Equal(t, []string{"k6.test-1.user", "weather.site-a.bot", ""}, res.SkippedSample,
		"and quoted, so an operator knows which leftover to clean up")

	man, ok := pub.blobs["site-a/run-1/pool-manifest.json"].(poolManifest)
	require.True(t, ok)
	assert.Equal(t, 3, man.SkippedAccounts,
		"the manifest records the gap between the rows read and the accounts published")
}

// A site whose channel subscribers are ALL unusable is empty for clientsim,
// and must fail — but not with the same bare "no accounts" as a site with no
// subscriptions at all. The two need different fixes: one is a load-tool
// leftover to clean up, the other is a site nobody uses.
func TestExportPool_RejectsAPopulationOfOnlyUnusableAccounts(t *testing.T) {
	src := &fakeAccountSource{accounts: []string{"k6.test-1.user", "k6.test-2.user"}}

	_, err := exportPool(context.Background(), src, newFakePublisher(), poolExportOptions{
		RunID: "run-1", SiteID: "site-a",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "k6.test-1.user",
		"an export that drops everything must name what it dropped")
}

// The same ordering rule the bot and the empty account already proved: $limit
// bounds whatever reaches it, so an unusable account excluded only in Go
// consumes a slot and then vanishes. "k6.test-1.user" sorts among the k's, so
// this is not hypothetical on a site whose names run past it.
func TestExportPool_LimitIsHonouredWhenAnUnusableAccountSitsInTheHead(t *testing.T) {
	src := &fakeAccountSource{accounts: []string{"aaa", "bbb.test.user", "ccc", "ddd", "eee"}}
	pub := newFakePublisher()

	res, err := exportPool(context.Background(), src, pub, poolExportOptions{
		RunID: "run-1", SiteID: "site-a", Limit: 3,
	})
	require.NoError(t, err)
	assert.Equal(t, 3, res.Accounts)
	assert.Equal(t, []string{"aaa", "ccc", "ddd"}, pub.artifacts[res.ArtifactKey].Accounts)
}

// The sample is what an operator acts on, and the cap is what keeps one bad
// site from turning a warning into a wall of names. Both halves matter: a cap
// that also truncated a short list would hide the only account there was.
func TestSampleAccounts_CapsWithoutTruncatingShortLists(t *testing.T) {
	short := []string{"a", "b"}
	assert.Equal(t, short, sampleAccounts(short))

	long := []string{"a", "b", "c", "d", "e", "f", "g"}
	assert.Equal(t, []string{"a", "b", "c", "d", "e"}, sampleAccounts(long))
	assert.Len(t, long, 7, "sampling must not modify the caller's slice")
}

// The belt's job, stated as a test: a source that hands over unusable accounts
// without saying so must not be able to make the export lie. Whatever the belt
// removes is added to the same count and sample the source contributes to.
func TestExportPool_CountsWhatTheBeltCatchesFromAnUnreportingSource(t *testing.T) {
	src := &fakeAccountSource{raw: true, accounts: []string{"anna", "k6.test-1.user", "bob"}}
	pub := newFakePublisher()

	res, err := exportPool(context.Background(), src, pub, poolExportOptions{
		RunID: "run-1", SiteID: "site-a",
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"anna", "bob"}, pub.artifacts[res.ArtifactKey].Accounts)
	assert.Equal(t, 1, res.Skipped)
	assert.Equal(t, []string{"k6.test-1.user"}, res.SkippedSample)

	man := pub.blobs[res.ManifestKey].(poolManifest)
	assert.Equal(t, 1, man.SkippedAccounts)
}

// Both layers can skip in the same export: the source drops what it saw while
// bounding, the belt drops what the source let through. The count is the sum,
// or one of them is invisible.
func TestExportPool_SumsSkipsFromBothLayers(t *testing.T) {
	// A source that reports one skip of its own, then hands over an account it
	// did not check.
	src := &countingSource{accounts: []string{"anna", "k6.test-2.user"}, skipped: 1, sample: []string{"k6.test-1.user"}}
	pub := newFakePublisher()

	res, err := exportPool(context.Background(), src, pub, poolExportOptions{RunID: "run-1", SiteID: "site-a"})
	require.NoError(t, err)
	assert.Equal(t, 2, res.Skipped, "the source's count and the belt's must both be in it")
	assert.Equal(t, []string{"k6.test-1.user", "k6.test-2.user"}, res.SkippedSample)
	assert.Equal(t, 2, pub.blobs[res.ManifestKey].(poolManifest).SkippedAccounts)
}

type countingSource struct {
	accounts []string
	skipped  int
	sample   []string
}

func (c *countingSource) channelSubscriberAccounts(_ context.Context, _ string, _ int) (poolCandidates, error) {
	return poolCandidates{Accounts: c.accounts, Skipped: c.skipped, Sample: c.sample}, nil
}

// The WARN is the only place an operator sees the leftovers before the run
// starts, so it is worth pinning: it fires when something was skipped, carries
// the count and the sample, and stays quiet when nothing was.
func TestLogSkippedAccounts(t *testing.T) {
	capture := func(skipped int, sample []string) string {
		var buf bytes.Buffer
		prev := slog.Default()
		slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
		t.Cleanup(func() { slog.SetDefault(prev) })
		logSkippedAccounts("site-a", skipped, sample)
		return buf.String()
	}

	line := capture(2, []string{"k6.test-1.user", ""})
	assert.Contains(t, line, `"level":"WARN"`)
	assert.Contains(t, line, `"skipped":2`)
	assert.Contains(t, line, "k6.test-1.user", "the sample is what tells an operator which leftover to clean up")

	assert.Empty(t, capture(0, nil), "an export that skipped nothing must not warn about it")
}

// fakeRows is a cursor over a fixed, sorted candidate list — what the
// aggregation hands the walk, without a database.
type fakeRows struct {
	accounts  []string
	i         int
	decodeErr error
	err       error
	// stopped records how far the walk read, which is the whole point of
	// bounding client-side: the rest of the population is never transferred.
	stopped int
}

func (f *fakeRows) Next(context.Context) bool {
	if f.i >= len(f.accounts) {
		return false
	}
	f.i++
	f.stopped = f.i
	return true
}

func (f *fakeRows) Decode(v any) error {
	if f.decodeErr != nil {
		return f.decodeErr
	}
	row, ok := v.(*struct {
		Account string `bson:"_id"`
	})
	if !ok {
		return fmt.Errorf("unexpected decode target %T", v)
	}
	row.Account = f.accounts[f.i-1]
	return nil
}

func (f *fakeRows) Err() error { return f.err }

// The bound counts accounts a run can CONNECT AS. A bound applied to rows —
// which is what a server-side $limit does — stops early on a site whose sorted
// head holds leftovers, and hands back fewer accounts than asked while
// eligible ones sit unread.
func TestCollectUsableAccounts_BoundsOnUsableAccountsNotRows(t *testing.T) {
	rows := &fakeRows{accounts: []string{"anna", "k6.test-1.user", "has space", "bob", "cleo", "dave"}}

	got, err := collectUsableAccounts(context.Background(), rows, 3)
	require.NoError(t, err)
	assert.Equal(t, []string{"anna", "bob", "cleo"}, got.Accounts, "--limit 3 must deliver 3 usable accounts")
	assert.Equal(t, 2, got.Skipped)
	assert.Equal(t, []string{"k6.test-1.user", "has space"}, got.Sample)
	assert.Equal(t, 5, rows.stopped, "the walk stops at the bound rather than reading the whole site")
}

// Every rune class the validator refuses, including the two no practical regex
// spells out the same way in two engines — which is why the rule lives here
// rather than in the aggregation.
func TestCollectUsableAccounts_SkipsEveryUnusableShape(t *testing.T) {
	rows := &fakeRows{accounts: []string{
		"", "anna", "ctrl\x07name", "has space", "k6.test-1.user", "nbsp name",
		"tail>token", "weather.site-a.bot", "wild*card",
	}}

	got, err := collectUsableAccounts(context.Background(), rows, 100)
	require.NoError(t, err)
	assert.Equal(t, []string{"anna"}, got.Accounts)
	assert.Equal(t, 8, got.Skipped)
	assert.Len(t, got.Sample, skipSample, "the sample is capped; the count carries the scale")
}

// A cursor that fails mid-walk must not look like a small population: an
// export built on a truncated read would publish a pool nobody selected.
func TestCollectUsableAccounts_PropagatesCursorErrors(t *testing.T) {
	_, err := collectUsableAccounts(context.Background(),
		&fakeRows{accounts: []string{"anna"}, err: errors.New("connection reset")}, 10)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read channel subscribers")

	_, err = collectUsableAccounts(context.Background(),
		&fakeRows{accounts: []string{"anna"}, decodeErr: errors.New("type mismatch")}, 10)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decode channel subscriber")
}
