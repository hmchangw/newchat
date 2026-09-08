// Package poolartifact defines the versioned connection-pool artifact the
// loadgen seeders emit and clientsim consumes: the ordered account list a
// load-test run's simulated clients connect as. It is the only data
// contract between the two tools.
package poolartifact

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

// SchemaVersion is the artifact schema this package reads and writes.
const SchemaVersion = 1

// Artifact is the account pool one load-test run connects as: the ordered
// list, plus the metadata that ties it back to the run that produced it.
// RunID and ConfigDigest are what let a clientsim fleet's connections be
// matched to the seed behind them, so both are required at either end.
type Artifact struct {
	SchemaVersion int      `json:"schemaVersion"`
	RunID         string   `json:"runId"`
	SiteID        string   `json:"siteId"`
	ConfigDigest  string   `json:"configDigest"`
	Accounts      []string `json:"accounts"`
}

// validateAccounts is the entry-level half of the contract, shared by Write
// and Load so a producer cannot report success on a file the consumer refuses.
//
// A duplicate is counted in the shard the readiness floor is measured against,
// but a consumer starts each account once — so the target can never be reached
// and MIN_READY_RATIO either fails the run for a reason unrelated to the system
// under test or absorbs the gap in its slack. Split across shards it is worse:
// two pods connect the same account, and every room they share double-counts
// its deliveries.
func validateAccounts(accounts []string) error {
	seen := make(map[string]int, len(accounts))
	for i, account := range accounts {
		// An empty entry builds subjects like chat.user..event.room, which
		// subscribe cleanly and receive nothing.
		if account == "" {
			return fmt.Errorf("pool artifact account %d is empty", i)
		}
		if first, dup := seen[account]; dup {
			return fmt.Errorf("pool artifact has duplicate account %q at positions %d and %d", account, first, i)
		}
		seen[account] = i
	}
	return nil
}

// Write stamps the current SchemaVersion (mutating the caller's struct —
// callers pass literals) and persists the artifact atomically (tmp +
// rename), so a concurrent Load from another process never sees a
// truncated file.
func Write(path string, a *Artifact) error {
	data, err := marshalArtifact(a)
	if err != nil {
		return err
	}
	if isGzipPath(path) {
		if data, err = gzipBytes(data); err != nil {
			return fmt.Errorf("compress pool artifact: %w", err)
		}
	}
	return writeAtomic(path, data)
}

// marshalArtifact is the producer-side contract, shared by the file and
// object-store writers: whatever one refuses, the other refuses too.
func marshalArtifact(a *Artifact) ([]byte, error) {
	switch {
	case len(a.Accounts) == 0:
		return nil, errors.New("write pool artifact: empty accounts")
	// Symmetric with Load: a seeder that can emit an artifact the consumer
	// refuses at startup turns a bad --users value into a failure hours later,
	// in the wrong tool.
	case len(a.Accounts) > maxAccounts:
		return nil, fmt.Errorf("write pool artifact: %d accounts, above the %d cap", len(a.Accounts), maxAccounts)
	case a.SiteID == "":
		return nil, errors.New("write pool artifact: empty siteID")
	case a.RunID == "":
		return nil, errors.New("write pool artifact: empty runID")
	case a.ConfigDigest == "":
		return nil, errors.New("write pool artifact: empty configDigest — the artifact would be unmatchable to its run")
	}
	if err := validateAccounts(a.Accounts); err != nil {
		return nil, fmt.Errorf("write pool artifact: %w", err)
	}
	a.SchemaVersion = SchemaVersion
	data, err := json.MarshalIndent(a, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal pool artifact: %w", err)
	}
	return data, nil
}

// writeAtomic persists through tmp + rename, so a concurrent Load from
// another process never sees a truncated file.
func writeAtomic(path string, data []byte) error {
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

// isGzipPath decides compression from the name alone, so the two ends of the
// contract cannot disagree: whatever Write compressed, Load decompresses.
func isGzipPath(path string) bool { return strings.HasSuffix(path, ".gz") }

// gzipBytes compresses at the best ratio available. The artifact is written
// once per run and read once per pod, so the CPU is irrelevant beside the
// bytes: a 30k-account pool of real account names goes from ~1.1 MiB to a few
// hundred KiB, which is the difference between fitting a transport hop and not.
func gzipBytes(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	if err != nil {
		return nil, fmt.Errorf("init gzip writer: %w", err)
	}
	if _, err := zw.Write(data); err != nil {
		return nil, fmt.Errorf("gzip pool artifact: %w", err)
	}
	// Closed explicitly, not deferred: Close flushes the trailer, and a
	// deferred one would run after buf was already read.
	if err := zw.Close(); err != nil {
		return nil, fmt.Errorf("finish gzip pool artifact: %w", err)
	}
	return buf.Bytes(), nil
}

// readCapped reads the artifact, decompressing first when it is gzipped.
//
// Cap the read itself rather than a prior Stat: a Stat-then-read lets a file
// that grows in between — or a fifo, which has no meaningful size — past the
// limit entirely. And the cap binds the DECOMPRESSED stream, because bounding
// the compressed bytes would let a few KiB of gzip expand into gigabytes of
// heap before any count check could run — the hazard the streaming decoder
// closed for plain JSON, reopened by compression.
func readCapped(r io.Reader, gzipped bool) ([]byte, error) {
	if gzipped {
		zr, err := gzip.NewReader(r)
		if err != nil {
			return nil, fmt.Errorf("open gzip pool artifact: %w", err)
		}
		defer zr.Close() //nolint:errcheck // read-only handle
		r = zr
	}
	data, err := io.ReadAll(io.LimitReader(r, maxArtifactBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read pool artifact: %w", err)
	}
	if int64(len(data)) > maxArtifactBytes {
		return nil, fmt.Errorf("pool artifact exceeds the %d-byte cap", maxArtifactBytes)
	}
	return data, nil
}

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
	data, err := readCapped(f, isGzipPath(path))
	if err != nil {
		return nil, err
	}
	return decodeAndValidate(data, wantSiteID)
}

// decodeAndValidate is the consumer-side contract, shared by the file and
// object-store readers. Unknown schema, wrong site, or an empty pool are
// startup errors for the consumer — fail fast, never limp.
func decodeAndValidate(data []byte, wantSiteID string) (*Artifact, error) {
	a, err := decodeArtifact(data)
	if err != nil {
		return nil, err
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
	// The account cap is enforced inside decodeArtifact, which abandons the
	// array the moment it passes maxAccounts — it has to be checked there for
	// the cap to bound memory rather than merely report on it, so a check
	// here could never fire.
	if err := validateAccounts(a.Accounts); err != nil {
		return nil, err
	}
	return a, nil
}

// decodeArtifact decodes the envelope, then the accounts array INCREMENTALLY,
// abandoning it the moment it passes the cap.
//
// maxArtifactBytes bounds the file, not the decode. json.Unmarshal into
// []string materialises every account as its own heap string plus the slice's
// doubling growth, so a file well inside the byte cap costs several times its
// own size in live heap before any count check can run — measured at ~148 MB
// to reject a single oversized fixture. Streaming the array makes the cap
// mean what it says.
func decodeArtifact(data []byte) (*Artifact, error) {
	var env struct {
		SchemaVersion int             `json:"schemaVersion"`
		RunID         string          `json:"runId"`
		SiteID        string          `json:"siteId"`
		ConfigDigest  string          `json:"configDigest"`
		Accounts      json.RawMessage `json:"accounts"`
	}
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, fmt.Errorf("parse pool artifact: %w", err)
	}
	a := &Artifact{
		SchemaVersion: env.SchemaVersion,
		RunID:         env.RunID,
		SiteID:        env.SiteID,
		ConfigDigest:  env.ConfigDigest,
	}
	if len(env.Accounts) == 0 {
		return a, nil // absent or null: the emptiness check below reports it
	}
	dec := json.NewDecoder(bytes.NewReader(env.Accounts))
	tok, err := dec.Token()
	if err != nil {
		return nil, fmt.Errorf("parse pool artifact accounts: %w", err)
	}
	if delim, ok := tok.(json.Delim); !ok || delim != json.Delim(0x5B) {
		return nil, errors.New("pool artifact accounts is not an array")
	}
	for dec.More() {
		if len(a.Accounts) == maxAccounts {
			return nil, fmt.Errorf("pool artifact has more accounts than the %d cap", maxAccounts)
		}
		var account string
		if err := dec.Decode(&account); err != nil {
			return nil, fmt.Errorf("parse pool artifact accounts: %w", err)
		}
		a.Accounts = append(a.Accounts, account)
	}
	return a, nil
}
