// Package poolartifact defines the versioned connection-pool artifact the
// loadgen seeders emit and clientsim consumes: the ordered account list a
// load-test run's simulated clients connect as. It is the only data
// contract between the two tools.
package poolartifact

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
)

// SchemaVersion is the artifact schema this package reads and writes.
const SchemaVersion = 1

type Artifact struct {
	SchemaVersion int      `json:"schemaVersion"`
	RunID         string   `json:"runId"`
	SiteID        string   `json:"siteId"`
	ConfigDigest  string   `json:"configDigest"`
	Accounts      []string `json:"accounts"`
}

// Write stamps the current SchemaVersion (mutating the caller's struct —
// callers pass literals) and persists the artifact atomically (tmp +
// rename), so a concurrent Load from another process never sees a
// truncated file.
func Write(path string, a *Artifact) error {
	switch {
	case len(a.Accounts) == 0:
		return errors.New("write pool artifact: empty accounts")
	case a.SiteID == "":
		return errors.New("write pool artifact: empty siteID")
	case a.RunID == "":
		return errors.New("write pool artifact: empty runID")
	case a.ConfigDigest == "":
		return errors.New("write pool artifact: empty configDigest — the artifact would be unmatchable to its run")
	}
	a.SchemaVersion = SchemaVersion
	data, err := json.MarshalIndent(a, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal pool artifact: %w", err)
	}
	tmp := path + ".tmp"
	// #nosec G306 -- the artifact is a non-secret account list deliberately
	// world-readable: it is mounted into clientsim/issuer containers that run
	// as different users.
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write pool artifact: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("finalize pool artifact: %w", err)
	}
	return nil
}

// maxArtifactBytes caps the whole-file read: 100k accounts is ~10 MB, so
// 64 MB is generous headroom, not a real limit.
const maxArtifactBytes = 64 << 20

// maxAccounts bounds the DECODED pool, which the byte cap does not: 64 MB of
// short account names is millions of entries, and every entry becomes a
// connection the consumer tries to hold. A pool file pointed at the wrong
// thing would then take a site down instead of testing it. One million is
// ten times the largest pool the tooling is designed for, so it can only ever
// catch a mistake.
const maxAccounts = 1_000_000

// Load reads and validates an artifact. Unknown schema, wrong site, or an
// empty pool are startup errors for the consumer — fail fast, never limp.
func Load(path, wantSiteID string) (*Artifact, error) {
	// #nosec G304 -- the artifact path comes from deployment config
	// (CLIENTSIM_POOL_FILE / --pool-out), not user input.
	// nosemgrep: gosec.G304-1 -- same justification; semgrep suppression is independent of gosec's #nosec
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open pool artifact: %w", err)
	}
	defer f.Close() //nolint:errcheck // read-only handle
	// Cap the read itself rather than a prior Stat: a Stat-then-read lets a
	// file that grows in between — or a fifo, which has no meaningful size —
	// past the limit entirely.
	data, err := io.ReadAll(io.LimitReader(f, maxArtifactBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read pool artifact: %w", err)
	}
	if int64(len(data)) > maxArtifactBytes {
		return nil, fmt.Errorf("pool artifact exceeds the %d-byte cap", maxArtifactBytes)
	}
	var a Artifact
	if err := json.Unmarshal(data, &a); err != nil {
		return nil, fmt.Errorf("parse pool artifact: %w", err)
	}
	if a.SchemaVersion != SchemaVersion {
		return nil, fmt.Errorf("pool artifact schema version %d, want %d", a.SchemaVersion, SchemaVersion)
	}
	if a.SiteID != wantSiteID {
		return nil, fmt.Errorf("pool artifact siteID %q does not match configured site %q", a.SiteID, wantSiteID)
	}
	// Symmetric with Write: an artifact missing either field is unmatchable to
	// the run that produced it, and JSON unmarshalling leaves both silently
	// empty rather than failing.
	if a.RunID == "" {
		return nil, errors.New("pool artifact has no runID")
	}
	if a.ConfigDigest == "" {
		return nil, errors.New("pool artifact has no configDigest")
	}
	if len(a.Accounts) == 0 {
		return nil, errors.New("pool artifact has no accounts")
	}
	if len(a.Accounts) > maxAccounts {
		return nil, fmt.Errorf("pool artifact has %d accounts, above the %d cap", len(a.Accounts), maxAccounts)
	}
	// A duplicate is counted in the shard the readiness floor is measured
	// against, but a consumer starts each account once — so the target can
	// never be reached and MIN_READY_RATIO either fails the run for a reason
	// unrelated to the system under test or absorbs the gap in its slack.
	// Split across shards it is worse: two pods connect the same account, and
	// every room they share double-counts its deliveries.
	seen := make(map[string]int, len(a.Accounts))
	for i, account := range a.Accounts {
		// An empty entry builds subjects like chat.user..event.room, which
		// subscribe cleanly and receive nothing.
		if account == "" {
			return nil, fmt.Errorf("pool artifact account %d is empty", i)
		}
		if first, dup := seen[account]; dup {
			return nil, fmt.Errorf("pool artifact has duplicate account %q at positions %d and %d", account, first, i)
		}
		seen[account] = i
	}
	return &a, nil
}
