package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/poolartifact"
	"github.com/hmchangw/chat/pkg/subject"
)

// poolCandidates is what a source found: the accounts a run can connect as,
// and what it left behind getting there. The two travel together because they
// are decided together — a source that filtered for itself and reported
// nothing would make the WARN and the manifest's skippedAccounts a fiction,
// which is worse than not recording them at all.
type poolCandidates struct {
	// Accounts are usable, sorted, and already bounded by the caller's limit —
	// bounded by USABLE accounts, so --limit N means N connections.
	Accounts []string
	// Skipped counts what was rejected as unusable and Sample quotes a few. A
	// count and a sample rather than the whole list: a site can hold more junk
	// rows than pool, and neither a log line nor an error should scale with it.
	Skipped int
	Sample  []string
}

// poolAccountSource yields the accounts a clientsim run should connect as.
// limit is the caller's --limit (0 = unbounded). A source applies both the
// usability rule and the bound itself, in that order: whichever layer decides
// what counts against the bound is the only one that can report what it
// dropped, so they cannot be split without one of them lying.
type poolAccountSource interface {
	channelSubscriberAccounts(ctx context.Context, siteID string, limit int) (poolCandidates, error)
}

// cursorLimit converts --limit into the server-side bound. The pipeline
// filters bots before this bound applies, so the bound counts only accounts
// that will survive to the artifact — no headroom needed.
//
// An unbounded export still gets a bound: maxAccounts+1, so a population over
// the artifact cap is still DETECTED (by the extra row) rather than silently
// truncated to it.
func cursorLimit(limit int) int {
	if limit > 0 && limit <= poolartifact.MaxAccounts {
		return limit
	}
	return poolartifact.MaxAccounts + 1
}

// poolPublisher is the object-store slice the export needs.
type poolPublisher interface {
	Key(siteID, runID, name string) string
	PutIfAbsent(ctx context.Context, key string, a *poolartifact.Artifact) error
	PutJSON(ctx context.Context, key string, v any) error
	Load(ctx context.Context, key, wantSiteID string) (*poolartifact.Artifact, error)
}

// validatePoolRunID keeps the run ID a single safe path segment. The rule
// itself lives in pkg/poolartifact beside Key, which composes the path, so the
// composer and its callers cannot drift on what is safe.
func validatePoolRunID(runID string) error {
	if err := poolartifact.ValidateKeySegment(runID); err != nil {
		return fmt.Errorf("--run-id must be a single safe path segment: %w", err)
	}
	return nil
}

// validatePoolSiteID guards the segment NEXT TO the run ID in the same
// path.Join. Guarding one and not the other is not a guard at all: SITE_ID
// escapes the prefix exactly the way an unchecked --run-id would, and lands
// the artifact in another site's scope where a fleet may already be reading.
func validatePoolSiteID(siteID string) error {
	if err := poolartifact.ValidateKeySegment(siteID); err != nil {
		return fmt.Errorf("SITE_ID must be a single safe path segment: %w", err)
	}
	return nil
}

const (
	poolArtifactName = "pool.json.gz"
	poolManifestName = "pool-manifest.json"
)

// poolExportQuery is recorded in the manifest verbatim, so a reader can tell
// which population an export described without reading this source.
//
// Derived from user-service's own subscription.list match (mongorepo
// listMatch: roomType in [dm, channel] with open != false), narrowed to
// channel: clientsim opens room lanes only for channels, because DM traffic
// arrives on the user lane instead. An account with no channel subscription
// would connect cleanly and then measure nothing.
const poolExportQuery = `subscriptions: match {siteId, roomType: "channel", open: {$ne: false}, origin: {$ne: "teams"}} ` +
	`-> group by u.account with isBot = $max(u.isBot) ` +
	`-> match {isBot: {$ne: true}, _id: not null} -> sort _id asc ` +
	`-> keep accounts usable as NATS subject tokens, until limit`

type poolExportOptions struct {
	RunID  string
	SiteID string
	// Limit truncates the sorted list. Zero means the whole population.
	Limit int
	// OutFile optionally also writes the artifact locally, for a run driven
	// from a laptop rather than a Job.
	OutFile string
}

type poolExportResult struct {
	Accounts     int
	ConfigDigest string
	ArtifactKey  string
	ManifestKey  string
	// Skipped counts every account excluded as unusable — by the source while
	// it was bounding the run, and by the belt below it — and SkippedSample
	// quotes a few of them, so the caller can say what shrank the pool.
	Skipped       int
	SkippedSample []string
}

// poolManifest explains an export. The artifact says WHO connected; this says
// how that set was chosen, which is what makes the run reproducible rather
// than merely identifiable.
type poolManifest struct {
	RunID        string `json:"runId"`
	SiteID       string `json:"siteId"`
	ConfigDigest string `json:"configDigest"`
	Accounts     int    `json:"accounts"`
	// SkippedAccounts closes the gap between the rows the source returned and
	// the accounts published, so a shrinking pool is visible in the record
	// rather than only in a log line nobody kept.
	SkippedAccounts int       `json:"skippedAccounts,omitempty"`
	Limit           int       `json:"limit,omitempty"`
	Query           string    `json:"query"`
	Source          string    `json:"source"`
	ExportedAt      time.Time `json:"exportedAt"`
}

// poolDigest fingerprints the POPULATION, not the seed parameters: a Mongo
// export has no preset or RNG seed to hash. Two exports of the same accounts
// agree, and one changed account does not — so a reader can tell whether two
// runs measured the same fleet.
func poolDigest(accounts []string) string {
	h := sha256.New()
	for _, a := range accounts {
		_, _ = h.Write([]byte(a))
		_, _ = h.Write([]byte{0}) // separator: "ab","c" must not hash as "a","bc"
	}
	return hex.EncodeToString(h.Sum(nil)[:8])
}

// accountUsable reports whether a clientsim run can connect as this account.
// One predicate, used by the source that bounds --limit and by the belt that
// re-checks it, so the layer doing the bounding and the layer doing the
// reporting cannot disagree about what "usable" means.
//
// Two kinds are not usable, for one shared reason — the account becomes a NATS
// subject token:
//
//   - Bots. A bot that owns a room holds a genuine channel subscription
//     (bot-room-service/handler.go:213-216 writes IsBot:true with
//     RoomTypeChannel, and its $setOnInsert never sets `open`), so every
//     condition the query matches on is legitimately true of it. clientsim
//     cannot connect as one either way: bots authenticate over HTTP through
//     pkg/botauth rather than the user JWT path.
//   - Anything that is not a valid subject token — a dot, wildcard, whitespace
//     or control rune, and the empty account. subject.UserSubscriptionList
//     PANICS on one, so such an account is not a degraded pod but a dead one.
//     Real sites carry them: "k6.test-1.user" is a leftover subscription row
//     from another load tool, with no isBot flag and no ".bot" suffix to mark
//     it as anything else.
//
// The rule lives in Go rather than in the aggregation, deliberately. A regex
// approximating subject.IsValidAccountToken is a second dialect of the same
// rule — evaluated by a different engine, with its own reading of \s — and the
// two can only ever drift apart. The cost is that unusable rows travel to the
// client; they are a rounding error beside the population itself.
func accountUsable(account string) bool {
	return !model.IsBot(account) && subject.IsValidAccountToken(account)
}

// dropUnusable splits accounts by accountUsable. It is the belt under a source
// that already applied the same rule: it must find nothing on the Mongo path,
// and it keeps the guarantee for any source that does less. Whatever it does
// find is returned rather than dropped quietly, so the caller's count stays
// the whole truth.
func dropUnusable(accounts []string) (kept, skipped []string) {
	kept = accounts[:0:0]
	for _, a := range accounts {
		if !accountUsable(a) {
			skipped = append(skipped, a)
			continue
		}
		kept = append(kept, a)
	}
	return kept, skipped
}

// skipSample bounds what a log line or an error quotes from a skipped set: a
// site that churns out thousands of unusable rows must not turn one warning
// into thousands of lines. The count carries the scale; the sample carries the
// shape, which is what tells an operator WHICH leftover to clean up.
const skipSample = 5

func sampleAccounts(accounts []string) []string {
	if len(accounts) > skipSample {
		return accounts[:skipSample]
	}
	return accounts
}

// exportPool reads the population, publishes the artifact the fleet consumes
// and the manifest that explains it.
func exportPool(ctx context.Context, src poolAccountSource, pub poolPublisher, opts poolExportOptions) (poolExportResult, error) {
	cand, err := src.channelSubscriberAccounts(ctx, opts.SiteID, opts.Limit)
	if err != nil {
		return poolExportResult{}, fmt.Errorf("list channel subscribers for %s: %w", opts.SiteID, err)
	}
	// The source reports what it skipped while bounding the run; the belt adds
	// anything it let through. Summed, because either alone under-reports.
	accounts, beltSkipped := dropUnusable(cand.Accounts)
	skipped := cand.Skipped + len(beltSkipped)
	sample := sampleAccounts(append(append([]string{}, cand.Sample...), beltSkipped...))
	// The failure this export exists to prevent: a fleet that starts against
	// nobody reports a healthy zero. Fail here, in the tool that can say why.
	if len(accounts) == 0 {
		if skipped > 0 {
			// A different failure from an unused site, and a different fix:
			// these rows exist, they are just unusable. Name them, or the
			// operator is left guessing which tool left them behind.
			return poolExportResult{}, fmt.Errorf(
				"site %q has %d channel subscribers but none a clientsim run can connect as (e.g. %q)",
				opts.SiteID, skipped, sample)
		}
		return poolExportResult{}, fmt.Errorf("site %q has no accounts with a channel subscription", opts.SiteID)
	}
	if opts.Limit > 0 && len(accounts) > opts.Limit {
		// From the head of a sorted list, so a repeated export with the same
		// limit picks the same accounts rather than an arbitrary sample.
		accounts = accounts[:opts.Limit]
	}

	digest := poolDigest(accounts)
	art := &poolartifact.Artifact{
		RunID: opts.RunID, SiteID: opts.SiteID, ConfigDigest: digest, Accounts: accounts,
	}
	artifactKey := pub.Key(opts.SiteID, opts.RunID, poolArtifactName)

	// A run ID names one immutable population. The claim is the write, not a
	// preceding Load: two exporters can both read the run as unpublished and
	// both go on to publish, and then pods that started before and pods that
	// restart after slice different arrays — accounts connected twice or not
	// at all, every pod still reporting ready. Re-exporting the SAME
	// population stays a safe retry, reconciled below against what is
	// actually stored rather than against what we read a moment ago.
	switch err := pub.PutIfAbsent(ctx, artifactKey, art); {
	case err == nil:
	case errors.Is(err, poolartifact.ErrObjectExists):
		existing, loadErr := pub.Load(ctx, artifactKey, opts.SiteID)
		if loadErr != nil {
			return poolExportResult{}, fmt.Errorf("read the pool that already holds run %q: %w", opts.RunID, loadErr)
		}
		// Recompute rather than trust: ConfigDigest is what the stored object
		// says about ITSELF, so an artifact whose accounts no longer match it
		// would be waved through as "the same population".
		if poolDigest(existing.Accounts) != digest || existing.ConfigDigest != digest {
			return poolExportResult{}, fmt.Errorf(
				"run %q already holds a pool of %d accounts (digest %s); this export is a different population of %d (digest %s) — use a new --run-id rather than overwrite a pool a fleet may already be reading",
				opts.RunID, len(existing.Accounts), existing.ConfigDigest, len(accounts), digest)
		}
	default:
		return poolExportResult{}, fmt.Errorf("publish pool artifact: %w", err)
	}

	manifestKey := pub.Key(opts.SiteID, opts.RunID, poolManifestName)
	if err := pub.PutJSON(ctx, manifestKey, poolManifest{
		RunID: opts.RunID, SiteID: opts.SiteID, ConfigDigest: digest,
		Accounts: len(accounts), SkippedAccounts: skipped, Limit: opts.Limit,
		Query: poolExportQuery, Source: "mongodb",
		ExportedAt: time.Now().UTC(),
	}); err != nil {
		// The artifact is already claimed and immutable at this point. Re-running
		// repairs this ONLY while the population is unchanged — the retry loses
		// the claim, matches the digest, and falls through to write the manifest.
		// If the site churns first, the digest no longer matches and the run ID
		// is spent: the artifact is readable but has no record of how it was
		// selected. Say so, rather than leave the operator to discover it.
		return poolExportResult{}, fmt.Errorf(
			"publish pool manifest for run %q (the artifact is already claimed — re-run promptly to repair it, or use a new --run-id if the population may have changed since): %w",
			opts.RunID, err)
	}

	if opts.OutFile != "" {
		if err := poolartifact.Write(opts.OutFile, art); err != nil {
			return poolExportResult{}, fmt.Errorf("write local pool copy: %w", err)
		}
	}
	return poolExportResult{
		Accounts: len(accounts), ConfigDigest: digest,
		ArtifactKey: artifactKey, ManifestKey: manifestKey,
		Skipped: skipped, SkippedSample: sample,
	}, nil
}

// mongoPoolSource reads the population from the operational subscriptions
// collection.
type mongoPoolSource struct{ db *mongo.Database }

func (m mongoPoolSource) channelSubscriberAccounts(ctx context.Context, siteID string, limit int) (poolCandidates, error) {
	// $group, not $lookup — no join, and it projects to the single field the
	// export needs. $sort after the group makes the order stable, which is
	// what shardSlice depends on: every clientsim pod slices the same array,
	// so an unstable order would overlap or skip accounts across pods.
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.M{
			"siteId":   siteID,
			"roomType": "channel",
			"open":     bson.M{"$ne": false},
			// user-service hides Teams-origin rooms from subscription.list
			// unless SHOW_TEAMS_ROOM (default false) or the account is
			// allowlisted. Without this an account whose only channel
			// subscriptions are Teams rooms is exported, walks to an EMPTY
			// plan, and reports ready while subscribing to nothing — exactly
			// the failure the readiness design exists to prevent.
			//
			// Applied unconditionally rather than mirroring the per-account
			// allowlist: for a load pool, under-selecting costs a few
			// connections and over-selecting costs silent measurement loss,
			// so it fails safe in the same direction as roomGlobal.
			"origin": bson.M{"$ne": model.OriginTeams},
		}}},
		// Bots hold channel subscriptions like anyone else; see accountUsable
		// for why they cannot be in a clientsim pool. The exclusion runs AFTER
		// the group, on the account, not before it on the row: an account with
		// one flagged row and one row whose flag was never written would
		// otherwise have the flagged row filtered out and survive on the other.
		// $max over the group returns true if ANY row carries the flag.
		//
		// This is the population's definition — which subscriptions count —
		// not a judgement about the account string. Everything of that second
		// kind is decided below, in Go, where it can also be counted.
		{{Key: "$group", Value: bson.M{"_id": "$u.account", "isBot": bson.M{"$max": "$u.isBot"}}}},
		{{Key: "$match", Value: bson.M{"isBot": bson.M{"$ne": true}}}},
		// A row with no u.account groups under null, which is not an account
		// at all — it decodes into no string and stands for no connection, so
		// it is dropped here rather than counted as a skipped account.
		{{Key: "$match", Value: bson.M{"_id": bson.M{"$ne": nil}}}},
		{{Key: "$sort", Value: bson.M{"_id": 1}}},
		// No $limit stage. The bound counts USABLE accounts, and only Go knows
		// which those are, so bounding here would count rows that are then
		// dropped — the caller asks for N and the run gets fewer. The cursor
		// below stops early instead, so the Job's heap holds the bound, not
		// the population. The cost is a $sort the server cannot cap at top-N;
		// it sorts the grouped keys, which the $group already materialised.
	}
	// $group and $sort are blocking stages with a per-stage memory limit; a
	// site large enough to be worth load-testing is exactly the one that
	// exceeds it, so let the server spill rather than fail the export.
	cur, err := m.db.Collection("subscriptions").Aggregate(ctx, pipeline,
		options.Aggregate().SetAllowDiskUse(true))
	if err != nil {
		return poolCandidates{}, fmt.Errorf("aggregate channel subscribers: %w", err)
	}
	defer cur.Close(ctx) //nolint:errcheck // read-only cursor

	// An unbounded export still gets a bound — maxAccounts+1, so a population
	// over the artifact cap is DETECTED by the extra account rather than
	// silently truncated to it.
	return collectUsableAccounts(ctx, cur, cursorLimit(limit))
}

// accountRows is the slice of *mongo.Cursor collectUsableAccounts needs.
// Declared here, in the consumer, so the walk that decides the bound and the
// skip evidence can be tested without a database — it is the part of this file
// most worth getting wrong quietly.
type accountRows interface {
	Next(ctx context.Context) bool
	Decode(v any) error
	Err() error
}

// collectUsableAccounts walks sorted candidates and stops once want USABLE
// accounts are in hand, counting everything it passed over.
//
// The bound is applied here rather than as a $limit for two reasons that are
// really one: only this layer knows which accounts a run can connect as, so a
// server-side bound would count rows that are then dropped (--limit N quietly
// delivering fewer), and a server-side filter would hide those rows from the
// only layer that can report them. Stopping early keeps the Job's heap sized
// by the bound rather than by the site.
func collectUsableAccounts(ctx context.Context, rows accountRows, want int) (poolCandidates, error) {
	var out poolCandidates
	for rows.Next(ctx) {
		var row struct {
			Account string `bson:"_id"`
		}
		if err := rows.Decode(&row); err != nil {
			return poolCandidates{}, fmt.Errorf("decode channel subscriber: %w", err)
		}
		if !accountUsable(row.Account) {
			// Counted, not passed over in silence: this walk is the only place
			// that ever sees "k6.test-1.user", so if it says nothing, nothing
			// downstream can.
			out.Skipped++
			if len(out.Sample) < skipSample {
				out.Sample = append(out.Sample, row.Account)
			}
			continue
		}
		out.Accounts = append(out.Accounts, row.Account)
		if len(out.Accounts) >= want {
			break
		}
	}
	if err := rows.Err(); err != nil {
		return poolCandidates{}, fmt.Errorf("read channel subscribers: %w", err)
	}
	return out, nil
}

// logSkippedAccounts is the operator-facing half of the skip evidence. Not an
// error — one stale row must not block a run the rest of the site can serve —
// but not silent either: the day this count stops being a handful of load-tool
// leftovers, the pool is quietly shrinking and this is where it shows.
func logSkippedAccounts(siteID string, skipped int, sample []string) {
	if skipped == 0 {
		return
	}
	slog.Warn("skipped accounts a clientsim run cannot connect as",
		"siteId", siteID, "skipped", skipped, "sample", sample)
}

func runPoolExport(ctx context.Context, cfg *config, args []string) int {
	fs := flag.NewFlagSet("pool-export", flag.ExitOnError)
	runID := fs.String("run-id", "", "run identifier the artifact is filed under (required)")
	limit := fs.Int("limit", 0, "cap the exported accounts (0 = the whole population)")
	outFile := fs.String("out", "", "also write the artifact to this local path")
	_ = fs.Parse(args)
	if *runID == "" {
		fmt.Fprintln(os.Stderr, "--run-id required")
		return 2
	}
	if err := validatePoolRunID(*runID); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	if err := validatePoolSiteID(cfg.SiteID); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	if *limit < 0 {
		fmt.Fprintf(os.Stderr, "--limit must be >= 0, got %d\n", *limit)
		return 2
	}
	if err := cfg.Pool.Validate(); err != nil {
		slog.Error("pool export configuration", "error", err)
		return 2
	}
	if cfg.Pool.PlaintextEndpoint() {
		// Same exposure as clientsim's fetch, in the other direction: this
		// UPLOADS the account list, and the request carries the access key ID.
		slog.Warn("publishing the pool over plaintext HTTP — the account list and the store access key ID cross the network in the clear; set POOL_S3_USE_SSL=true outside local development",
			"endpoint", cfg.Pool.Endpoint)
	}
	store, err := poolartifact.NewStore(&cfg.Pool)
	if err != nil {
		slog.Error("connect pool object store", "error", err)
		return 1
	}
	db, _, cleanup, err := connectStores(ctx, cfg)
	if err != nil {
		return 1
	}
	defer cleanup()

	res, err := exportPool(ctx, mongoPoolSource{db: db}, store, poolExportOptions{
		RunID: *runID, SiteID: cfg.SiteID, Limit: *limit, OutFile: *outFile,
	})
	if err != nil {
		slog.Error("pool export", "error", err)
		return 1
	}
	logSkippedAccounts(cfg.SiteID, res.Skipped, res.SkippedSample)
	slog.Info("pool exported",
		"runId", *runID, "siteId", cfg.SiteID, "accounts", res.Accounts,
		"skipped", res.Skipped,
		"configDigest", res.ConfigDigest,
		"artifactKey", res.ArtifactKey, "manifestKey", res.ManifestKey)
	return 0
}
