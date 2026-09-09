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

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/poolartifact"
)

// poolAccountSource yields the accounts a clientsim run should connect as.
type poolAccountSource interface {
	channelSubscriberAccounts(ctx context.Context, siteID string) ([]string, error)
}

// poolPublisher is the object-store slice the export needs.
type poolPublisher interface {
	Key(siteID, runID, name string) string
	Put(ctx context.Context, key string, a *poolartifact.Artifact) error
	PutJSON(ctx context.Context, key string, v any) error
	Load(ctx context.Context, key, wantSiteID string) (*poolartifact.Artifact, error)
}

// validatePoolRunID keeps the run ID a single safe path segment. It becomes
// one, and path.Join normalises "..", so an unvalidated value could write the
// artifact into another site's scope. Same pattern the soak ledger uses.
func validatePoolRunID(runID string) error {
	if !failureRunIDPattern.MatchString(runID) || runID == "." || runID == ".." {
		return fmt.Errorf("--run-id must be a single path segment matching %s, got %q",
			failureRunIDPattern.String(), runID)
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
const poolExportQuery = `subscriptions: {siteId, roomType: "channel", open: {$ne: false}, origin: {$ne: "teams"}, u.isBot: {$ne: true}} -> distinct u.account, sorted, then ".bot" accounts dropped`

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
}

// poolManifest explains an export. The artifact says WHO connected; this says
// how that set was chosen, which is what makes the run reproducible rather
// than merely identifiable.
type poolManifest struct {
	RunID        string    `json:"runId"`
	SiteID       string    `json:"siteId"`
	ConfigDigest string    `json:"configDigest"`
	Accounts     int       `json:"accounts"`
	Limit        int       `json:"limit,omitempty"`
	Query        string    `json:"query"`
	Source       string    `json:"source"`
	ExportedAt   time.Time `json:"exportedAt"`
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

// exportPool reads the population, publishes the artifact the fleet consumes
// and the manifest that explains it.
// dropBots removes bot accounts from a population. A bot that owns a room
// holds a genuine channel subscription (bot-room-service/handler.go:213-216
// writes IsBot:true with RoomTypeChannel, and its $setOnInsert never sets
// `open`, so the open filter passes it too), so every condition the query
// matches on is legitimately true of it.
//
// clientsim cannot connect as one either way: bots authenticate over HTTP
// through pkg/botauth rather than the user JWT path, and a dotted ".bot"
// account spans subject tokens — subject.UserSubscriptionList panics on it
// before a request is made.
//
// Belt to the query's braces, and not redundant with it: the query keys on the
// stored u.isBot flag, this keys on the account shape, and a row written
// without the flag is caught only here. Cheap — the population is already in
// memory and sorted.
func dropBots(accounts []string) []string {
	kept := accounts[:0:0]
	for _, a := range accounts {
		if model.IsBot(a) {
			continue
		}
		kept = append(kept, a)
	}
	return kept
}

func exportPool(ctx context.Context, src poolAccountSource, pub poolPublisher, opts poolExportOptions) (poolExportResult, error) {
	accounts, err := src.channelSubscriberAccounts(ctx, opts.SiteID)
	if err != nil {
		return poolExportResult{}, fmt.Errorf("list channel subscribers for %s: %w", opts.SiteID, err)
	}
	accounts = dropBots(accounts)
	// The failure this export exists to prevent: a fleet that starts against
	// nobody reports a healthy zero. Fail here, in the tool that can say why.
	if len(accounts) == 0 {
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

	// A run ID names one immutable population. Overwriting it with a
	// different one breaks the invariant the single object exists to hold:
	// pods that started before and pods that restart after would slice
	// different arrays, so shardSlice hands out overlapping or disjoint
	// ranges — accounts connected twice or not at all, every pod still ready.
	// Re-exporting the SAME population stays a safe retry.
	switch existing, err := pub.Load(ctx, artifactKey, opts.SiteID); {
	case errors.Is(err, poolartifact.ErrObjectNotFound):
	case err != nil:
		return poolExportResult{}, fmt.Errorf("check the published pool for run %q: %w", opts.RunID, err)
	case existing.ConfigDigest != digest:
		return poolExportResult{}, fmt.Errorf(
			"run %q already holds a pool of %d accounts (digest %s); this export is a different population of %d (digest %s) — use a new --run-id rather than overwrite a pool a fleet may already be reading",
			opts.RunID, len(existing.Accounts), existing.ConfigDigest, len(accounts), digest)
	}

	if err := pub.Put(ctx, artifactKey, art); err != nil {
		return poolExportResult{}, fmt.Errorf("publish pool artifact: %w", err)
	}

	manifestKey := pub.Key(opts.SiteID, opts.RunID, poolManifestName)
	if err := pub.PutJSON(ctx, manifestKey, poolManifest{
		RunID: opts.RunID, SiteID: opts.SiteID, ConfigDigest: digest,
		Accounts: len(accounts), Limit: opts.Limit,
		Query: poolExportQuery, Source: "mongodb",
		ExportedAt: time.Now().UTC(),
	}); err != nil {
		return poolExportResult{}, fmt.Errorf("publish pool manifest: %w", err)
	}

	if opts.OutFile != "" {
		if err := poolartifact.Write(opts.OutFile, art); err != nil {
			return poolExportResult{}, fmt.Errorf("write local pool copy: %w", err)
		}
	}
	return poolExportResult{
		Accounts: len(accounts), ConfigDigest: digest,
		ArtifactKey: artifactKey, ManifestKey: manifestKey,
	}, nil
}

// mongoPoolSource reads the population from the operational subscriptions
// collection.
type mongoPoolSource struct{ db *mongo.Database }

func (m mongoPoolSource) channelSubscriberAccounts(ctx context.Context, siteID string) ([]string, error) {
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
			// Bots hold channel subscriptions like anyone else; see dropBots
			// for why they cannot be in a clientsim pool. Dropped here so the
			// bulk never leaves the server, and again in Go on the account
			// shape, for a row whose flag was never stored.
			"u.isBot": bson.M{"$ne": true},
		}}},
		{{Key: "$group", Value: bson.M{"_id": "$u.account"}}},
		{{Key: "$sort", Value: bson.M{"_id": 1}}},
	}
	cur, err := m.db.Collection("subscriptions").Aggregate(ctx, pipeline)
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
	accounts := make([]string, 0, len(rows))
	for _, r := range rows {
		// An empty account builds subjects like chat.user..event.room, which
		// subscribe cleanly and receive nothing. poolartifact.Write refuses
		// them too; dropping here keeps the count in the manifest honest.
		if r.Account == "" {
			continue
		}
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
	if *limit < 0 {
		fmt.Fprintf(os.Stderr, "--limit must be >= 0, got %d\n", *limit)
		return 2
	}
	if err := cfg.Pool.Validate(); err != nil {
		slog.Error("pool export configuration", "error", err)
		return 2
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
	slog.Info("pool exported",
		"runId", *runID, "siteId", cfg.SiteID, "accounts", res.Accounts,
		"configDigest", res.ConfigDigest,
		"artifactKey", res.ArtifactKey, "manifestKey", res.ManifestKey)
	return 0
}
