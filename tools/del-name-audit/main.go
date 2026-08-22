// Package main is the del-name-audit CLI: a read-only probe that answers whether
// user-service can drop its rooms $lookup from the subscription count/list paths
// and filter on the subscription's own denormalized name instead.
//
// It compares both directions:
//
//	forward — every subscription of a "Del-" room must itself carry the marker
//	reverse — no "Del-"-named subscription may point at a live (non-Del-) room
//
// Exits 0 when the data supports the rewrite, 1 when it does not, 2 on error.
//
// Flags:
//
//	--rooms    max Del- rooms to scan (0 = all, default 0)
//	--subs     max Del--named subscriptions to scan (0 = all, default 0)
//	--samples  counter-examples to print (default 20)
//	--json     emit the report as JSON instead of text
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/hmchangw/chat/pkg/mongoutil"
)

type config struct {
	MongoURI      string `env:"MONGO_URI"      envDefault:"mongodb://localhost:27017/?directConnection=true"`
	MongoDB       string `env:"MONGO_DB"       envDefault:"chat"`
	MongoUsername string `env:"MONGO_USERNAME" envDefault:""`
	MongoPassword string `env:"MONGO_PASSWORD" envDefault:""`
	// Timeout bounds the whole audit; a full scan of a large deployment needs
	// more than a request-shaped default.
	Timeout time.Duration `env:"AUDIT_TIMEOUT" envDefault:"10m"`
}

// parseConfig loads config from the supplied env map. Test seam — callers pass
// their own map so tests don't touch os.Environ.
func parseConfig(envVars map[string]string) (config, error) {
	var cfg config
	if err := env.ParseWithOptions(&cfg, env.Options{Environment: envVars}); err != nil {
		return config{}, fmt.Errorf("parse env: %w", err)
	}
	return cfg, nil
}

type flags struct {
	rooms   int
	subs    int
	samples int
	asJSON  bool
}

// main keeps only the exit so run's deferred cleanup always executes.
func main() { os.Exit(run()) }

// run performs the audit and returns the process exit code: 0 safe, 1 not safe,
// 2 configuration/connection/query error.
func run() int {
	var f flags
	flag.IntVar(&f.rooms, "rooms", 0, "max Del- rooms to scan (0 = all)")
	flag.IntVar(&f.subs, "subs", 0, "max Del--named subscriptions to scan (0 = all)")
	flag.IntVar(&f.samples, "samples", 20, "counter-examples to print")
	flag.BoolVar(&f.asJSON, "json", false, "emit the report as JSON")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))

	cfg, err := parseConfig(envMap())
	if err != nil {
		slog.Error("invalid configuration", "error", err)
		return 2
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()

	// ConnectRead: secondaryPreferred. The audit is a bulk scan and must not add
	// read load to the primary; a soft-delete rename that lands during the scan
	// is caught by the next run.
	client, err := mongoutil.ConnectRead(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword)
	if err != nil {
		slog.Error("connect mongo", "error", err)
		return 2
	}
	defer func() {
		if derr := client.Disconnect(context.Background()); derr != nil {
			slog.Warn("disconnect mongo", "error", derr)
		}
	}()

	slog.Info("starting del-name audit", "database", cfg.MongoDB, "roomLimit", f.rooms, "subLimit", f.subs)
	in, err := collect(ctx, newMongoStore(client.Database(cfg.MongoDB)), f.rooms, f.subs, f.samples)
	if err != nil {
		slog.Error("collect audit input", "error", err)
		return 2
	}
	rep := audit(&in)

	if err := render(os.Stdout, &rep, f.asJSON); err != nil {
		slog.Error("render report", "error", err)
		return 2
	}
	if !rep.safeToMoveFilter() {
		slog.Warn("subscription names do not agree with room names — keep the rooms $lookup",
			"disagree", rep.Disagree, "falsePositive", rep.FalsePositive)
		return 1
	}
	slog.Info("subscription names agree with room names — the rooms $lookup can be replaced by a name filter",
		"subsScanned", rep.SubsScanned, "delRooms", rep.DelRooms)
	return 0
}

// envMap snapshots the process environment for parseConfig.
func envMap() map[string]string {
	out := map[string]string{}
	for _, kv := range os.Environ() {
		for i := 0; i < len(kv); i++ {
			if kv[i] == '=' {
				out[kv[:i]] = kv[i+1:]
				break
			}
		}
	}
	return out
}

// render writes the report to w as JSON or as a human-readable summary.
func render(w io.Writer, rep *report, asJSON bool) error {
	if asJSON {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		if err := enc.Encode(rep); err != nil {
			return fmt.Errorf("encode json report: %w", err)
		}
		return nil
	}
	return renderText(w, rep)
}

func renderText(w io.Writer, rep *report) error {
	var b []byte
	add := func(format string, args ...any) { b = append(b, fmt.Sprintf(format, args...)...) }

	add("Del- name agreement audit\n")
	add("=========================\n\n")
	add("Forward (every subscription of a Del- room carries the marker)\n")
	add("  Del- rooms scanned      : %d\n", rep.DelRooms)
	add("  ...with no subscriptions: %d\n", rep.RoomsWithNoSubs)
	add("  subscriptions scanned   : %d\n", rep.SubsScanned)
	add("  agree                   : %d\n", rep.Agree)
	add("  DISAGREE                : %d\n\n", rep.Disagree)

	if len(rep.ByRoomType) > 0 {
		add("  by roomType:\n")
		types := make([]string, 0, len(rep.ByRoomType))
		for t := range rep.ByRoomType {
			types = append(types, t)
		}
		sort.Strings(types)
		for _, t := range types {
			st := rep.ByRoomType[t]
			add("    %-8s agree=%-8d disagree=%d\n", t, st.Agree, st.Disagree)
		}
		add("\n")
	}

	add("Reverse (no Del--named subscription points at a live room)\n")
	add("  Del--named subscriptions: %d\n", rep.DelNamedSubs)
	add("  FALSE POSITIVE          : %d\n", rep.FalsePositive)
	add("  unverifiable (no local room, cross-site): %d\n\n", rep.Unverifiable)

	if len(rep.Samples) > 0 {
		add("Counter-examples (max %d):\n", len(rep.Samples))
		for _, m := range rep.Samples {
			add("  [%s] room=%s %q  sub=%s %q  type=%s site=%s\n",
				m.Kind, m.RoomID, m.RoomName, m.SubID, m.SubName, m.RoomType, m.SiteID)
		}
		add("\n")
	}

	if rep.safeToMoveFilter() {
		add("VERDICT: SAFE — the rooms $lookup can be replaced by a subscription-name filter.\n")
	} else {
		add("VERDICT: NOT SAFE — keep the rooms $lookup; see the counter-examples above.\n")
	}
	if _, err := w.Write(b); err != nil {
		return fmt.Errorf("write report: %w", err)
	}
	return nil
}
