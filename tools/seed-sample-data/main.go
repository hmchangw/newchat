// Package main is the seed-sample-data CLI: populates MongoDB and Valkey
// with a small, well-formed, idempotent dataset for local development.
// Run via `make seed` after `make deps-up`.
//
// Flags:
//
//	(none)     idempotent populate
//	--reset    drop seed records then populate
//	--dry-run  print the plan and exit
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
	MongoURI       string   `env:"MONGO_URI"       envDefault:"mongodb://localhost:27017"`
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
// collection plus the two valkey domains, in `<key> <count>` format.
func dryRunSummary() string {
	lines := []string{
		fmt.Sprintf("users %d", len(BuildUsers())),
		fmt.Sprintf("rooms %d", len(BuildRooms())),
		fmt.Sprintf("subscriptions %d", len(BuildSubscriptions())),
		fmt.Sprintf("room_members %d", len(BuildRoomMembers())),
		fmt.Sprintf("messages %d", len(BuildMessages())),
		fmt.Sprintf("thread_rooms %d", len(BuildThreadRooms())),
		fmt.Sprintf("thread_subscriptions %d", len(BuildThreadSubscriptions())),
		fmt.Sprintf("valkey:roomKeys %d", len(BuildRoomKeys())),
		fmt.Sprintf("valkey:restrictedCache %d", len(BuildRestrictedCache())),
	}
	return strings.Join(lines, "\n")
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	reset := flag.Bool("reset", false, "delete seed records before re-populating")
	dryRun := flag.Bool("dry-run", false, "print the plan and exit without writing")
	flag.Parse()

	if *dryRun {
		slog.Info("seed dry-run summary", "plan", dryRunSummary())
		return
	}

	// run() handles all setup/teardown so deferred cleanup runs before
	// any non-zero exit. main() only translates the error into an exit code.
	if err := run(*reset); err != nil {
		slog.Error("seed failed", "error", err)
		os.Exit(1)
	}
}

func run(reset bool) error {
	cfg, err := parseConfig(envFromOS())
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	mongoClient, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword)
	if err != nil {
		return fmt.Errorf("mongo connect: %w", err)
	}
	defer mongoutil.Disconnect(ctx, mongoClient)
	db := mongoClient.Database(cfg.MongoDB)

	keyStore, err := roomkeystore.NewValkeyClusterStore(roomkeystore.ClusterConfig{
		Addrs:       cfg.ValkeyAddrs,
		Password:    cfg.ValkeyPassword,
		GracePeriod: 5 * time.Minute,
	})
	if err != nil {
		return fmt.Errorf("valkey roomkeystore connect: %w", err)
	}
	defer func() {
		if cerr := keyStore.Close(); cerr != nil {
			slog.Warn("valkey roomkeystore close", "error", cerr)
		}
	}()

	valkeyClient, err := valkeyutil.ConnectCluster(ctx, cfg.ValkeyAddrs, cfg.ValkeyPassword)
	if err != nil {
		return fmt.Errorf("valkey client connect: %w", err)
	}
	defer valkeyutil.Disconnect(valkeyClient)

	if reset {
		if err := deleteAll(ctx, db); err != nil {
			return fmt.Errorf("mongo reset: %w", err)
		}
		if err := deleteValkey(ctx, keyStore, valkeyClient); err != nil {
			return fmt.Errorf("valkey reset: %w", err)
		}
		slog.Info("seed reset complete")
	}

	mc, err := upsertAll(ctx, db)
	if err != nil {
		return fmt.Errorf("mongo upsert: %w", err)
	}

	vc, err := writeValkey(ctx, keyStore, valkeyClient)
	if err != nil {
		return fmt.Errorf("valkey write: %w", err)
	}

	slog.Info("seed complete",
		"users", mc.Users,
		"rooms", mc.Rooms,
		"subscriptions", mc.Subscriptions,
		"roomMembers", mc.RoomMembers,
		"messages", mc.Messages,
		"threadRooms", mc.ThreadRooms,
		"threadSubscriptions", mc.ThreadSubscriptions,
		"valkeyRoomKeys", vc.RoomKeys,
		"valkeyCacheEntries", vc.CacheEntries,
	)
	return nil
}
