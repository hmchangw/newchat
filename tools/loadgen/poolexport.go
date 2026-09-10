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

// poolAccountSource yields the accounts a clientsim run should connect as.
// limit is the caller's --limit (0 = unbounded); a source is free to push it
// down rather than materialise the whole population first.
type poolAccountSource interface {
	channelSubscriberAccounts(ctx context.Context, siteID string, limit int) ([]string, error)
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
	`-> match {isBot: {$ne: true}, _id: not empty and not /[.*>\s]/} ` +
	`-> sort _id asc -> limit`

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
	// Skipped are the accounts the source returned that no clientsim pod can
	// connect as. Normally empty — the query excludes them server-side — so a
	// non-empty one says the query and the Go pass disagree about the rule.
	Skipped []string
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

// dropUnusable splits the population into the accounts a clientsim run can
// connect as and the ones it cannot, so the caller can report the second set
// rather than let it vanish.
//
// Three kinds are unusable, for one shared reason — the account becomes a NATS
// subject token:
//
//   - Bots. A bot that owns a room holds a genuine channel subscription
//     (bot-room-service/handler.go:213-216 writes IsBot:true with
//     RoomTypeChannel, and its $setOnInsert never sets `open`), so every
//     condition the query matches on is legitimately true of it. clientsim
//     cannot connect as one either way: bots authenticate over HTTP through
//     pkg/botauth rather than the user JWT path.
//   - Anything that is not a valid subject token — a dot, wildcard, whitespace
//     or control rune. subject.UserSubscriptionList PANICS on one, so a single
//     such account is not a degraded pod but a dead one. Real sites carry them:
//     "k6.test-1.user" is a leftover subscription row from another load tool,
//     with no isBot flag and no ".bot" suffix to mark it as anything else.
//   - The empty account, which IsValidAccountToken already refuses: it builds
//     subjects like chat.user..event.room, which subscribe cleanly and receive
//     nothing.
//
// Belt to the query's braces, and not redundant with it: the query keys on the
// stored u.isBot flag and on the account's shape as a regex, this keys on the
// same validator the subject builders use — so a row written without the flag,
// or a rune the regex does not spell out, is caught only here. Cheap — the
// population is already in memory and sorted.
func dropUnusable(accounts []string) (kept, skipped []string) {
	kept = accounts[:0:0]
	for _, a := range accounts {
		if model.IsBot(a) || !subject.IsValidAccountToken(a) {
			skipped = append(skipped, a)
			continue
		}
		kept = append(kept, a)
	}
	return kept, skipped
}

// unusableAccountPattern is the server-side half of subject.IsValidAccountToken:
// an account matching it cannot be a NATS subject token. Named rather than
// inlined so a test can hold the two halves against each other — the pattern
// and the validator disagreeing is exactly the drift that let "k6.test-1.user"
// reach poolartifact and fail a whole export.
//
// It is deliberately the WEAKER half. Regex \s is the ASCII spaces, while the
// validator refuses every unicode space and control rune, so the pipeline can
// let through what the Go belt then removes. Weaker in that direction costs a
// --limit slot; stronger would drop accounts the pods can serve.
const unusableAccountPattern = `[.*>\s]`

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
	accounts, err := src.channelSubscriberAccounts(ctx, opts.SiteID, opts.Limit)
	if err != nil {
		return poolExportResult{}, fmt.Errorf("list channel subscribers for %s: %w", opts.SiteID, err)
	}
	accounts, skipped := dropUnusable(accounts)
	// The failure this export exists to prevent: a fleet that starts against
	// nobody reports a healthy zero. Fail here, in the tool that can say why.
	if len(accounts) == 0 {
		if len(skipped) > 0 {
			// A different failure from an unused site, and a different fix:
			// these rows exist, they are just unusable. Name them, or the
			// operator is left guessing which tool left them behind.
			return poolExportResult{}, fmt.Errorf(
				"site %q has %d channel subscribers but none usable as a NATS subject token (e.g. %q)",
				opts.SiteID, len(skipped), sampleAccounts(skipped))
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
		Accounts: len(accounts), SkippedAccounts: len(skipped), Limit: opts.Limit,
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
		Skipped: skipped,
	}, nil
}

// mongoPoolSource reads the population from the operational subscriptions
// collection.
type mongoPoolSource struct{ db *mongo.Database }

func (m mongoPoolSource) channelSubscriberAccounts(ctx context.Context, siteID string, limit int) ([]string, error) {
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
		// Bots hold channel subscriptions like anyone else; see dropUnusable for
		// why they cannot be in a clientsim pool. The exclusion runs AFTER the
		// group, on the account, not before it on the row: an account with one
		// flagged row and one row whose flag was never written would otherwise
		// have the flagged row filtered out and survive on the other — the
		// exact hole the two layers exist to close, reopened by filter order.
		// $max over the group returns true if ANY row carries the flag.
		{{Key: "$group", Value: bson.M{"_id": "$u.account", "isBot": bson.M{"$max": "$u.isBot"}}}},
		{{Key: "$match", Value: bson.M{"isBot": bson.M{"$ne": true}}}},
		// Everything clientsim cannot connect as is excluded HERE too, not only
		// in dropUnusable, for one reason: the $limit below bounds whatever
		// reaches it. An account removed afterwards has already consumed a
		// --limit slot and then vanished — the caller asks for N, the site
		// holds more than N eligible accounts, and the run still gets fewer.
		// Filtering before the bound makes the bound mean what it says.
		//
		// The regex is the subject-token rule, not a ".bot" rule: any dot,
		// wildcard or whitespace rune spans or breaks a token, and an account
		// carrying one panics subject.UserSubscriptionList inside the pod. It
		// subsumes the ".bot" suffix (a bot account is dotted by construction)
		// and catches what no flag marks — "k6.test-1.user", left behind by
		// another load tool, is a real row on a real staging site.
		// dropUnusable stays as the belt: it runs the validator itself, so a
		// control rune this pattern does not spell out is still caught, and the
		// rule holds for any other source.
		{{Key: "$match", Value: bson.M{
			"_id": bson.M{"$nin": bson.A{"", nil}, "$not": bson.Regex{Pattern: unusableAccountPattern}},
		}}},
		{{Key: "$sort", Value: bson.M{"_id": 1}}},
		// Bound the cursor server-side. cur.All materialises whatever comes
		// back, so applying --limit only afterwards holds the WHOLE site
		// population in the Job's heap first — and an unbounded export has no
		// ceiling at all until the artifact's own account cap rejects it, long
		// after the memory was spent. maxAccounts+1 keeps that cap detectable.
		{{Key: "$limit", Value: int64(cursorLimit(limit))}},
	}
	// $group and $sort are blocking stages with a per-stage memory limit; a
	// site large enough to be worth load-testing is exactly the one that
	// exceeds it, so let the server spill rather than fail the export.
	cur, err := m.db.Collection("subscriptions").Aggregate(ctx, pipeline,
		options.Aggregate().SetAllowDiskUse(true))
	if err != nil {
		return nil, fmt.Errorf("aggregate channel subscribers: %w", err)
	}
	defer cur.Close(ctx) //nolint:errcheck // read-only cursor
	var rows []struct {
		Account string `bson:"_id"`
	}
	if err := cur.All(ctx, &rows); err != nil {
		return nil, fmt.Errorf("read channel subscribers: %w", err)
	}
	// Returned as the query selected them, unfiltered: dropUnusable is the one
	// place that decides what a pod can connect as, so whatever slips past the
	// pipeline is dropped AND counted there rather than vanishing here.
	accounts := make([]string, 0, len(rows))
	for _, r := range rows {
		accounts = append(accounts, r.Account)
	}
	return accounts, nil
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
	if len(res.Skipped) > 0 {
		// Not an error — one stale row must not block a run the rest of the
		// site can serve — but not silent either: the day this count stops
		// being a handful of load-tool leftovers, the pool is quietly shrinking
		// and the operator has to be able to see it.
		slog.Warn("skipped accounts that cannot be NATS subject tokens",
			"siteId", cfg.SiteID, "skipped", len(res.Skipped),
			"sample", sampleAccounts(res.Skipped))
	}
	slog.Info("pool exported",
		"runId", *runID, "siteId", cfg.SiteID, "accounts", res.Accounts,
		"skipped", len(res.Skipped),
		"configDigest", res.ConfigDigest,
		"artifactKey", res.ArtifactKey, "manifestKey", res.ManifestKey)
	return 0
}
