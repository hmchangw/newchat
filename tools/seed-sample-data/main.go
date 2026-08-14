// Package main is the seed-sample-data CLI: populates MongoDB and Valkey
// with a small, well-formed, idempotent dataset for local development.
// Run via `make seed` after `make deps-up`.
//
// Flags:
//
//	(none)      idempotent populate
//	--reset     drop seed records then populate
//	--dry-run   print the plan and exit
//	--site      home site to seed (default site-local)
//	--mongo-db  target database, overriding MONGO_DB
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/roomkeystore"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

type config struct {
	MongoURI       string   `env:"MONGO_URI"       envDefault:"mongodb://localhost:27017/?directConnection=true"`
	MongoDB        string   `env:"MONGO_DB"        envDefault:"chat"`
	MongoUsername  string   `env:"MONGO_USERNAME"  envDefault:""`
	MongoPassword  string   `env:"MONGO_PASSWORD"  envDefault:""`
	ValkeyAddrs    []string `env:"VALKEY_ADDRS"    envDefault:"localhost:6379" envSeparator:","`
	ValkeyPassword string   `env:"VALKEY_PASSWORD" envDefault:""`
}

// parseConfig loads config from the supplied env map. Test seam — callers
// pass their own map so tests don't touch os.Environ.
func parseConfig(envVars map[string]string) (config, error) {
	var cfg config
	if err := env.ParseWithOptions(&cfg, env.Options{Environment: envVars}); err != nil {
		return cfg, fmt.Errorf("parse env: %w", err)
	}
	return cfg, nil
}

func envFromOS() map[string]string {
	out := map[string]string{}
	for _, e := range os.Environ() {
		i := strings.IndexByte(e, '=')
		if i < 0 {
			continue
		}
		out[e[:i]] = e[i+1:]
	}
	return out
}

// dryRunSummary returns a multi-line human-readable plan: one line per
// collection plus the two side-store domains (MongoDB room keys and the Valkey
// restricted-rooms cache), in `<key> <count>` format, filtered to what would
// actually be written for site.
func dryRunSummary(site string) string {
	lines := []string{
		fmt.Sprintf("site %s", site),
		fmt.Sprintf("users %d", len(BuildUsers())),
		fmt.Sprintf("hr_employee %d", len(BuildHREmployees())),
		fmt.Sprintf("rooms %d", len(filterBySite(BuildRooms(), site, roomHomeSite))),
		fmt.Sprintf("subscriptions %d", len(filterBySite(BuildSubscriptions(), site, subscriptionHomeSite))),
		fmt.Sprintf("room_members %d", len(filterBySite(BuildRoomMembers(), site, memberHomeSite))),
		fmt.Sprintf("messages %d", len(filterBySite(BuildMessages(), site, messageHomeSite))),
		fmt.Sprintf("thread_rooms %d", len(filterBySite(BuildThreadRooms(), site, threadRoomHomeSite))),
		fmt.Sprintf("thread_subscriptions %d", len(filterBySite(BuildThreadSubscriptions(), site, threadSubscriptionHomeSite))),
		fmt.Sprintf("mongo:roomKeys %d", len(filterBySite(BuildRoomKeys(), site, roomKeyHomeSite))),
		fmt.Sprintf("valkey:restrictedCache %d", len(filterBySite(BuildRestrictedCache(), site, restrictedCacheHomeSite))),
	}
	return strings.Join(lines, "\n")
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	reset := flag.Bool("reset", false, "delete seed records before re-populating")
	dryRun := flag.Bool("dry-run", false, "print the plan and exit without writing")
	site := flag.String("site", "site-local", "home site to seed: site-local or site-remote")
	mongoDB := flag.String("mongo-db", "", "target database, overriding MONGO_DB")
	flag.Parse()

	if *dryRun {
		slog.Info("seed dry-run summary", "site", *site, "plan", dryRunSummary(*site))
		return
	}

	// run() handles all setup/teardown so deferred cleanup runs before
	// any non-zero exit. main() only translates the error into an exit code.
	if err := run(*reset, *site, *mongoDB); err != nil {
		slog.Error("seed failed", "error", err)
		os.Exit(1)
	}
}

func run(reset bool, site, mongoDBFlag string) error {
	cfg, err := parseConfig(envFromOS())
	if err != nil {
		return err
	}
	dbName, site := resolveTarget(cfg.MongoDB, mongoDBFlag, site)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	mongoClient, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword)
	if err != nil {
		return fmt.Errorf("mongo connect: %w", err)
	}
	defer mongoutil.Disconnect(ctx, mongoClient)
	db := mongoClient.Database(dbName)

	// Room keys live in the rooms collection; upsertAll inserts the rooms before
	// writeSideStores provisions their keys.
	keyStore := roomkeystore.NewMongoStore(db.Collection("rooms"), 5*time.Minute)

	valkeyClient, err := valkeyutil.ConnectCluster(ctx, cfg.ValkeyAddrs, cfg.ValkeyPassword)
	if err != nil {
		return fmt.Errorf("valkey client connect: %w", err)
	}
	defer valkeyutil.Disconnect(valkeyClient)

	if reset {
		if err := deleteAll(ctx, db); err != nil {
			return fmt.Errorf("mongo reset: %w", err)
		}
		if err := deleteSideStores(ctx, keyStore, valkeyClient); err != nil {
			return fmt.Errorf("side-store reset: %w", err)
		}
		slog.Info("seed reset complete")
	}

	mc, err := upsertAll(ctx, db, site)
	if err != nil {
		return fmt.Errorf("mongo upsert: %w", err)
	}

	vc, err := writeSideStores(ctx, keyStore, valkeyClient, site)
	if err != nil {
		return fmt.Errorf("side-store write: %w", err)
	}

	slog.Info("seed complete",
		"site", site,
		"users", mc.Users,
		"hrEmployees", mc.HREmployees,
		"rooms", mc.Rooms,
		"subscriptions", mc.Subscriptions,
		"roomMembers", mc.RoomMembers,
		"messages", mc.Messages,
		"threadRooms", mc.ThreadRooms,
		"threadSubscriptions", mc.ThreadSubscriptions,
		"mongoRoomKeys", vc.RoomKeys,
		"valkeyRestrictedCacheEntries", vc.CacheEntries,
	)
	return nil
}
