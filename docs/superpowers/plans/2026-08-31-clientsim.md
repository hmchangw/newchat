# clientsim Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `tools/clientsim` — a standalone tool holding tens of thousands of real WSS client connections (auth exchange → NATS WebSocket → the production frontend's subscription walk) — plus its three enablers: a shared pool-artifact package, an auth-service dev-mint guard, and a ws-enabled NATS testutil helper.

**Architecture:** Flat `package main` tool under `tools/`, following loadgen's conventions (env config via `caarlos0/env`, private Prometheus registry served over HTTP, slog JSON, no OTel). Per simulated user: mint a JWT through a TokenProvider (cached, single-mint per refresh), connect over `ws://` with `nats.UserJWT`, bootstrap subscriptions via paginated `subscription.list`, keep them live via `subscription.update`, count deliveries and observe two edge latencies. Sharding is `floor` partition over a file-based pool artifact.

**Tech Stack:** Go 1.25, `nats-io/nats.go v1.50.0` (has `nc.ForceReconnect()`), `nats-io/nkeys`, `prometheus/client_golang v1.23.2`, `go-resty/resty v2` via `pkg/restyutil`, `caarlos0/env/v11`, testcontainers (integration).

**Spec:** `docs/superpowers/specs/2026-08-29-clientsim-design.md` — the plan argues from the spec; executors read both.

## Global Constraints

- All commands via `make` targets, never raw `go` (`make test SERVICE=<path>` runs `go test -race ./<path>/...`; same shape for `test-integration`, `generate`, `lint`).
- TDD Red-Green-Refactor for every task; tests first, confirm they fail, then implement.
- Error handling: `fmt.Errorf("what this fn was doing: %w", err)` for infra; `errcode` constructors + `WithReason` only at client boundaries; never log AND return the same error.
- Logging: `log/slog` JSON only; never log tokens, JWTs, or message bodies.
- No `time.Sleep` for synchronization; every goroutine has a termination path.
- Never edit `mock_store_test.go` by hand (this plan needs no mockgen — interfaces here are tiny and hand-faked in tests, matching loadgen's style).
- Integration tests: `//go:build integration`, same package, `TestMain(m) { testutil.RunTests(m) }`.
- New env vars: `SCREAMING_SNAKE_CASE`, prefix `CLIENTSIM_`; secrets `required`, others `envDefault`.
- Coverage floor 80% per package (target 90% for the walk/lifecycle logic).
- `docs/client-api.md` must be updated in the same commit as the auth-service handler change (Task 3).
- Commit after each task's tests pass. Never push to a branch other than `claude/nats-load-test-tool-design-0gspyw`.

## File Structure (locked)

```
pkg/poolartifact/poolartifact.go        # Artifact type, Write, Load (+validation)
pkg/poolartifact/poolartifact_test.go
tools/loadgen/poolout.go                # writePoolArtifact bridge from []model.User
tools/loadgen/poolout_test.go
tools/loadgen/main.go                   # seed: --pool-out flag threading (modify)
tools/loadgen/soak_main.go              # soak seed: artifact emission (modify)
auth-service/devguard.go                # newDevAccountGuard
auth-service/devguard_test.go
auth-service/handler.go                 # guard field + option + handleDevAuth check (modify)
auth-service/handler_test.go            # guard cases (modify)
auth-service/main.go                    # config fields + wiring (modify)
pkg/errcode/codes_auth.go               # AuthAccountNotAllowed (modify)
docs/client-api.md                      # §2.2 error row + reason index (modify)
pkg/testutil/nats_ws.go                 # shared ws-NATS container + network
pkg/testutil/terminate.go               # wire TerminateNATSWebSocket (modify)
pkg/roomkeysender/integration_test.go   # migrate onto testutil helper (modify)
tools/clientsim/main.go                 # config, wiring, metrics server, shutdown
tools/clientsim/pool.go                 # shardSlice
tools/clientsim/pool_test.go
tools/clientsim/metrics.go              # private prometheus registry
tools/clientsim/metrics_test.go
tools/clientsim/token.go                # TokenProvider, devProvider, authClient
tools/clientsim/token_test.go
tools/clientsim/subwalk.go              # paginated bootstrap → subscription plan
tools/clientsim/subwalk_test.go
tools/clientsim/liveupdate.go           # subscription.update reconcile
tools/clientsim/liveupdate_test.go
tools/clientsim/delivery.go             # RoomEvent decode + latency observation
tools/clientsim/delivery_test.go
tools/clientsim/client.go               # simClient lifecycle, JWT cache, modes
tools/clientsim/client_test.go
tools/clientsim/swarm.go                # ramp/churn orchestrator + summary
tools/clientsim/swarm_test.go
tools/clientsim/integration_test.go     # end-to-end over ws-NATS
tools/clientsim/main_test.go            # TestMain (integration tag)
tools/clientsim/deploy/Dockerfile
tools/clientsim/deploy/docker-compose.yml
tools/clientsim/deploy/azure-pipelines.yml
tools/clientsim/README.md
```

One deliberate spec deviation, carried through Tasks 6 and 11: the spec's slow-consumer counter names the OTel-based `pkg/natsutil` helper, but clientsim (like loadgen) runs no OTel SDK, so that counter would be a silent no-op. We implement `clientsim_slow_consumer_events_total` on the tool's own Prometheus registry with the identical per-episode semantics documented in `pkg/natsutil/slowconsumer.go:14-25` (one increment per Active→SlowConsumer transition; never add `Subscription.Dropped()`). Note this in the PR description.

---

### Task 1: `pkg/poolartifact`

**Files:**
- Create: `pkg/poolartifact/poolartifact.go`
- Test: `pkg/poolartifact/poolartifact_test.go`

**Interfaces:**
- Consumes: nothing (stdlib only).
- Produces (used by Tasks 2, 5):
  - `const SchemaVersion = 1`
  - `type Artifact struct { SchemaVersion int; RunID, SiteID, ConfigDigest string; Accounts []string }` (json tags `schemaVersion`, `runId`, `siteId`, `configDigest`, `accounts`)
  - `func Write(path string, a *Artifact) error` — stamps `SchemaVersion`, refuses empty `Accounts`/`SiteID`/`RunID`
  - `func Load(path, wantSiteID string) (*Artifact, error)` — fail-fast on unknown schema, siteID mismatch, empty accounts

- [ ] **Step 1: Write the failing tests**

```go
package poolartifact

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func validArtifact() *Artifact {
	return &Artifact{RunID: "seed-medium-42", SiteID: "site-a",
		ConfigDigest: "abc123", Accounts: []string{"user-0", "user-1"}}
}

func TestWriteLoad_RoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pool.json")
	require.NoError(t, Write(path, validArtifact()))

	got, err := Load(path, "site-a")
	require.NoError(t, err)
	assert.Equal(t, SchemaVersion, got.SchemaVersion)
	assert.Equal(t, "seed-medium-42", got.RunID)
	assert.Equal(t, []string{"user-0", "user-1"}, got.Accounts)
}

func TestWrite_RejectsIncomplete(t *testing.T) {
	dir := t.TempDir()
	cases := []struct {
		name   string
		mutate func(*Artifact)
	}{
		{"empty accounts", func(a *Artifact) { a.Accounts = nil }},
		{"empty siteID", func(a *Artifact) { a.SiteID = "" }},
		{"empty runID", func(a *Artifact) { a.RunID = "" }},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			a := validArtifact()
			tt.mutate(a)
			assert.Error(t, Write(filepath.Join(dir, tt.name+".json"), a))
		})
	}
}

func TestLoad_FailFast(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pool.json")
	require.NoError(t, Write(path, validArtifact()))

	t.Run("siteID mismatch", func(t *testing.T) {
		_, err := Load(path, "site-b")
		assert.ErrorContains(t, err, "site")
	})
	t.Run("unknown schema version", func(t *testing.T) {
		raw, err := os.ReadFile(path)
		require.NoError(t, err)
		bad := filepath.Join(t.TempDir(), "bad.json")
		require.NoError(t, os.WriteFile(bad,
			[]byte(`{"schemaVersion":99,`+string(raw[len(`{"schemaVersion":1,`):]), 0o644))
		_, err = Load(bad, "site-a")
		assert.ErrorContains(t, err, "schema")
	})
	t.Run("missing file", func(t *testing.T) {
		_, err := Load(filepath.Join(t.TempDir(), "nope.json"), "site-a")
		assert.Error(t, err)
	})
	t.Run("malformed json", func(t *testing.T) {
		bad := filepath.Join(t.TempDir(), "garbage.json")
		require.NoError(t, os.WriteFile(bad, []byte("{"), 0o644))
		_, err := Load(bad, "site-a")
		assert.Error(t, err)
	})
}
```

- [ ] **Step 2: Run tests, verify failure**

Run: `make test SERVICE=pkg/poolartifact`
Expected: FAIL — `undefined: Artifact`, `undefined: Write`, etc.

- [ ] **Step 3: Implement**

```go
// Package poolartifact defines the versioned connection-pool artifact the
// loadgen seeders emit and clientsim consumes: the ordered account list a
// load-test run's simulated clients connect as. It is the only data
// contract between the two tools.
package poolartifact

import (
	"encoding/json"
	"errors"
	"fmt"
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

// Write stamps the current SchemaVersion and persists the artifact.
func Write(path string, a *Artifact) error {
	switch {
	case len(a.Accounts) == 0:
		return errors.New("write pool artifact: empty accounts")
	case a.SiteID == "":
		return errors.New("write pool artifact: empty siteID")
	case a.RunID == "":
		return errors.New("write pool artifact: empty runID")
	}
	a.SchemaVersion = SchemaVersion
	data, err := json.MarshalIndent(a, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal pool artifact: %w", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write pool artifact: %w", err)
	}
	return nil
}

// Load reads and validates an artifact, failing fast per the clientsim spec:
// unknown schema, wrong site, or an empty pool are startup errors.
func Load(path, wantSiteID string) (*Artifact, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read pool artifact: %w", err)
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
	if len(a.Accounts) == 0 {
		return nil, errors.New("pool artifact has no accounts")
	}
	return &a, nil
}
```

- [ ] **Step 4: Run tests, verify pass**

Run: `make test SERVICE=pkg/poolartifact`
Expected: PASS

- [ ] **Step 5: Lint and commit**

```bash
make lint
git add pkg/poolartifact
git commit -m "feat(poolartifact): versioned pool artifact shared by loadgen and clientsim"
```

---

### Task 2: loadgen `--pool-out` emission

**Files:**
- Create: `tools/loadgen/poolout.go`, `tools/loadgen/poolout_test.go`
- Modify: `tools/loadgen/main.go` (seed FlagSet at :153-195, `runSeedMessages` at :197-226), `tools/loadgen/soak_main.go` (`runSoakSeed` at :362-394 and the soak seed-phase option plumbing)

**Interfaces:**
- Consumes: `poolartifact.Write`, `poolartifact.Artifact` (Task 1); loadgen's `Fixtures.Users []model.User` (`preset.go:113`), `soakTopology.ActiveUsers []model.User` (`soak_topology.go:14`); `model.User.Account` (`pkg/model/user.go:51`).
- Produces: `func writePoolArtifact(path, runID, siteID, digest string, users []model.User) error`; seed subcommand flag `--pool-out` (empty = no artifact, existing behavior unchanged).

- [ ] **Step 1: Write the failing test**

```go
// tools/loadgen/poolout_test.go
package main

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/poolartifact"
)

func TestWritePoolArtifact_AccountsInOrder(t *testing.T) {
	users := []model.User{
		{ID: "id-b", Account: "user-1"},
		{ID: "id-a", Account: "user-0"},
	}
	path := filepath.Join(t.TempDir(), "pool.json")

	require.NoError(t, writePoolArtifact(path, "seed-medium-42", "site-a", "d1g3st", users))

	a, err := poolartifact.Load(path, "site-a")
	require.NoError(t, err)
	// Order preserved from the fixture slice, accounts not IDs.
	assert.Equal(t, []string{"user-1", "user-0"}, a.Accounts)
	assert.Equal(t, "d1g3st", a.ConfigDigest)
}

func TestWritePoolArtifact_EmptyUsers(t *testing.T) {
	err := writePoolArtifact(filepath.Join(t.TempDir(), "p.json"), "r", "s", "d", nil)
	assert.Error(t, err)
}

// Schema equivalence across the two seeders (spec §11): the local fixture
// path and the staging topology path must write byte-compatible artifacts.
func TestWritePoolArtifact_SchemaMatchesLoadRoundTrip(t *testing.T) {
	topo := []model.User{{ID: "mongo-hex-id", Account: "alice@corp"}}
	path := filepath.Join(t.TempDir(), "pool.json")
	require.NoError(t, writePoolArtifact(path, "soak-run-7", "site-a", "cfg", topo))
	a, err := poolartifact.Load(path, "site-a")
	require.NoError(t, err)
	assert.Equal(t, []string{"alice@corp"}, a.Accounts)
}
```

- [ ] **Step 2: Run tests, verify failure**

Run: `make test SERVICE=tools/loadgen` (or narrower: `go test -race ./tools/loadgen/ -run TestWritePoolArtifact` via the Makefile pattern)
Expected: FAIL — `undefined: writePoolArtifact`

- [ ] **Step 3: Implement `poolout.go`**

```go
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/poolartifact"
)

// writePoolArtifact projects the seeded users' accounts (fixture order
// preserved) into the shared pool artifact clientsim consumes.
func writePoolArtifact(path, runID, siteID, digest string, users []model.User) error {
	if len(users) == 0 {
		return errors.New("write pool artifact: no seeded users")
	}
	accounts := make([]string, len(users))
	for i := range users {
		accounts[i] = users[i].Account
	}
	return poolartifact.Write(path, &poolartifact.Artifact{
		RunID: runID, SiteID: siteID, ConfigDigest: digest, Accounts: accounts,
	})
}

// seedConfigDigest fingerprints the fixture inputs so a pool artifact can be
// matched to the seed run that produced it.
func seedConfigDigest(presetName string, seed int64, users int) string {
	sum := sha256.Sum256(fmt.Appendf(nil, "%s|%d|%d", presetName, seed, users))
	return hex.EncodeToString(sum[:8])
}
```

- [ ] **Step 4: Thread the flag through the messages seeder**

In `main.go` seed FlagSet (after the existing `parentsPerRoom` flag, ~:161):

```go
poolOut := fs.String("pool-out", "", "write the clientsim pool artifact (ordered accounts) to this path; empty = skip")
```

Pass `*poolOut` into `runSeedMessages` (change its signature to `runSeedMessages(ctx context.Context, cfg *config, preset string, seed int64, usersOverride int, poolOut string) int`, updating the call site). In `runSeedMessages`, after `SeedRoomKeys` succeeds and before the final `slog.Info("seed complete (messages)", ...)`:

```go
if poolOut != "" {
	runID := fmt.Sprintf("seed-%s-%d", p.Name, seed)
	digest := seedConfigDigest(p.Name, seed, p.Users)
	if err := writePoolArtifact(poolOut, runID, cfg.SiteID, digest, fixtures.Users); err != nil {
		slog.Error("write pool artifact", "error", err, "path", poolOut)
		return 1
	}
	slog.Info("pool artifact written", "path", poolOut, "accounts", len(fixtures.Users))
}
```

- [ ] **Step 5: Thread the flag through the soak seeder**

The seed FlagSet already parses before the `--workload=soak` short-circuit (`main.go:167-169`), so add a `PoolOut string` field to `soakOptions` and pass `PoolOut: *poolOut` at that call site. In `runSoakSeed` (`soak_main.go:362`), after `seedSoak` returns successfully and before the `slog.Info("Cassandra soak topology seeded", ...)` at :387: emit with `topology.ActiveUsers`, `RunID` = the same run ID placed into `soakSeedInput.RunID`, `SiteID` = `input.SiteID`, and digest = the same value assigned to `soakManifest.ConfigDigest` in `seedSoak` (`soak_seed.go`, the manifest construction around :119 — reuse the function that computes it; if it is computed inline, extract it to a named function `soakConfigDigest(...)` first so both sites share it). Read those three at their definition sites before wiring — the names above are from the current code, verify while editing.

```go
if opts.PoolOut != "" {
	if err := writePoolArtifact(opts.PoolOut, input.RunID, input.SiteID, digest, topology.ActiveUsers); err != nil {
		slog.Error("write pool artifact", "error", err, "path", opts.PoolOut)
		return 1
	}
	slog.Info("pool artifact written", "path", opts.PoolOut, "accounts", len(topology.ActiveUsers))
}
```

- [ ] **Step 6: Run tests + verify existing seeds unchanged**

Run: `make test SERVICE=tools/loadgen`
Expected: PASS (new tests green, existing loadgen tests untouched — no behavior change with the flag unset).

- [ ] **Step 7: Lint and commit**

```bash
make lint
git add tools/loadgen
git commit -m "feat(loadgen): emit clientsim pool artifact from seed and soak seeders (--pool-out)"
```

---

### Task 3: auth-service dev-mint guard

**Files:**
- Create: `auth-service/devguard.go`, `auth-service/devguard_test.go`
- Modify: `auth-service/handler.go` (AuthHandler field + Option + `handleDevAuth` at :278), `auth-service/handler_test.go`, `auth-service/main.go` (config at :25-44 + wiring at :77-106), `pkg/errcode/codes_auth.go`, `docs/client-api.md` (§2.2 error table ~:241 and the reason index ~:6885)

**Interfaces:**
- Consumes: existing `Option func(*AuthHandler)` pattern (`handler.go:74`), `errcode.Forbidden` + `WithReason`, `errtest.AssertCode/AssertReason`.
- Produces:
  - `type devAccountGuard func(account string) bool`
  - `func newDevAccountGuard(prefix, allowlistPath string) (devAccountGuard, error)` (nil,nil when both empty; error when both set)
  - `func WithDevAccountGuard(g devAccountGuard) Option`
  - `errcode.AuthAccountNotAllowed Reason = "account_not_allowed"` (403)
  - New envs `DEV_MODE_ACCOUNT_PREFIX`, `DEV_MODE_ACCOUNT_ALLOWLIST_FILE`

- [ ] **Step 1: Write the failing guard-constructor tests**

```go
// auth-service/devguard_test.go
package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewDevAccountGuard(t *testing.T) {
	allow := filepath.Join(t.TempDir(), "allow.txt")
	require.NoError(t, os.WriteFile(allow, []byte("alice@corp\n\n bob \n"), 0o644))

	t.Run("both unset returns nil guard", func(t *testing.T) {
		g, err := newDevAccountGuard("", "")
		require.NoError(t, err)
		assert.Nil(t, g)
	})
	t.Run("both set is a config error", func(t *testing.T) {
		_, err := newDevAccountGuard("user-", allow)
		assert.Error(t, err)
	})
	t.Run("prefix guard", func(t *testing.T) {
		g, err := newDevAccountGuard("user-", "")
		require.NoError(t, err)
		assert.True(t, g("user-0"))
		assert.False(t, g("alice@corp"))
		assert.False(t, g("USER-0")) // exact prefix, no case folding
	})
	t.Run("allowlist guard trims and skips blanks", func(t *testing.T) {
		g, err := newDevAccountGuard("", allow)
		require.NoError(t, err)
		assert.True(t, g("alice@corp"))
		assert.True(t, g("bob"))
		assert.False(t, g("mallory"))
	})
	t.Run("missing allowlist file errors", func(t *testing.T) {
		_, err := newDevAccountGuard("", filepath.Join(t.TempDir(), "nope"))
		assert.Error(t, err)
	})
	t.Run("empty allowlist file errors", func(t *testing.T) {
		empty := filepath.Join(t.TempDir(), "empty.txt")
		require.NoError(t, os.WriteFile(empty, []byte("\n\n"), 0o644))
		_, err := newDevAccountGuard("", empty)
		assert.Error(t, err)
	})
}
```

- [ ] **Step 2: Write the failing handler tests** (append to `handler_test.go`, mirroring the existing `setupRouter`/`mustAccountKP`/`mustUserNKey` helpers and the dev-mode request shape from `TestHandleAuth_InvalidAccountFormat` at :571)

```go
func TestHandleAuth_DevMode_AccountGuard(t *testing.T) {
	signingKP, accPub := mustAccountKP(t)

	post := func(t *testing.T, router *gin.Engine, account string) *httptest.ResponseRecorder {
		t.Helper()
		payload, err := json.Marshal(map[string]string{"account": account, "natsPublicKey": mustUserNKey(t)})
		require.NoError(t, err)
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/auth", strings.NewReader(string(payload)))
		req.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(w, req)
		return w
	}

	t.Run("prefix guard rejects non-matching account with 403 account_not_allowed", func(t *testing.T) {
		g, err := newDevAccountGuard("user-", "")
		require.NoError(t, err)
		handler := NewAuthHandler(nil, signingKP, accPub, 2*time.Hour, true, WithDevAccountGuard(g))
		router := setupRouter(t, handler)

		w := post(t, router, "alice@corp")
		assert.Equal(t, http.StatusForbidden, w.Code)
		errtest.AssertCode(t, w.Body.Bytes(), errcode.CodeForbidden)
		errtest.AssertReason(t, w.Body.Bytes(), errcode.AuthAccountNotAllowed)
	})
	t.Run("prefix guard mints for matching account", func(t *testing.T) {
		g, err := newDevAccountGuard("user-", "")
		require.NoError(t, err)
		handler := NewAuthHandler(nil, signingKP, accPub, 2*time.Hour, true, WithDevAccountGuard(g))
		router := setupRouter(t, handler)

		w := post(t, router, "user-0")
		assert.Equal(t, http.StatusOK, w.Code)
	})
	t.Run("nil guard keeps current behavior", func(t *testing.T) {
		handler := NewAuthHandler(nil, signingKP, accPub, 2*time.Hour, true)
		router := setupRouter(t, handler)

		w := post(t, router, "anyone-at-all")
		assert.Equal(t, http.StatusOK, w.Code)
	})
}
```

(Check `errcode.CodeForbidden` is the exported name in `pkg/errcode/category.go`; the wire value is `forbidden`.)

- [ ] **Step 3: Run tests, verify failure**

Run: `make test SERVICE=auth-service`
Expected: FAIL — `undefined: newDevAccountGuard`, `undefined: WithDevAccountGuard`, `undefined: errcode.AuthAccountNotAllowed`

- [ ] **Step 4: Implement**

`pkg/errcode/codes_auth.go` — append, matching the file's one-line comment style:

```go
	// 403 — dev-mode mint refused: the account is outside the configured
	// prefix/allowlist guard.
	AuthAccountNotAllowed Reason = "account_not_allowed"
```

`auth-service/devguard.go`:

```go
package main

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

// devAccountGuard reports whether dev mode may mint a JWT for account.
// A nil guard means unrestricted (current behavior).
type devAccountGuard func(account string) bool

// newDevAccountGuard builds the guard from the two mutually-exclusive
// config knobs. Both empty -> nil guard.
func newDevAccountGuard(prefix, allowlistPath string) (devAccountGuard, error) {
	switch {
	case prefix != "" && allowlistPath != "":
		return nil, errors.New("DEV_MODE_ACCOUNT_PREFIX and DEV_MODE_ACCOUNT_ALLOWLIST_FILE are mutually exclusive")
	case prefix != "":
		return func(a string) bool { return strings.HasPrefix(a, prefix) }, nil
	case allowlistPath != "":
		data, err := os.ReadFile(allowlistPath)
		if err != nil {
			return nil, fmt.Errorf("read dev-mint allowlist: %w", err)
		}
		allowed := map[string]struct{}{}
		for _, line := range strings.Split(string(data), "\n") {
			if a := strings.TrimSpace(line); a != "" {
				allowed[a] = struct{}{}
			}
		}
		if len(allowed) == 0 {
			return nil, errors.New("dev-mint allowlist file has no accounts")
		}
		return func(a string) bool { _, ok := allowed[a]; return ok }, nil
	default:
		return nil, nil
	}
}
```

`auth-service/handler.go` — add field `devGuard devAccountGuard` to `AuthHandler` (after `devMode bool`), the option next to the existing ones:

```go
// WithDevAccountGuard restricts which accounts the dev-mode branch will
// mint for. A nil guard leaves dev mint unrestricted.
func WithDevAccountGuard(g devAccountGuard) Option {
	return func(h *AuthHandler) { h.devGuard = g }
}
```

and in `handleDevAuth`, directly after the `IsValidAccountToken` check:

```go
	if h.devGuard != nil && !h.devGuard(req.Account) {
		errhttp.Write(ctx, c, errcode.Forbidden("account not allowed for dev-mode mint",
			errcode.WithReason(errcode.AuthAccountNotAllowed)))
		return
	}
```

`auth-service/main.go` — config fields under `DevMode`:

```go
	DevModeAccountPrefix    string `env:"DEV_MODE_ACCOUNT_PREFIX"`
	DevModeAllowlistFile    string `env:"DEV_MODE_ACCOUNT_ALLOWLIST_FILE"`
```

and in the option assembly (before the DevMode branch):

```go
	guard, err := newDevAccountGuard(cfg.DevModeAccountPrefix, cfg.DevModeAllowlistFile)
	if err != nil {
		return fmt.Errorf("dev account guard: %w", err)
	}
	if guard != nil {
		opts = append(opts, WithDevAccountGuard(guard))
		slog.Info("dev-mode account guard enabled")
	}
```

(match the surrounding function's error-return vs log-and-exit style — main.go's run function returns errors.)

- [ ] **Step 5: Run tests, verify pass**

Run: `make test SERVICE=auth-service && make test SERVICE=pkg/errcode`
Expected: PASS, including all pre-existing dev-mode tests (nil-guard path unchanged).

- [ ] **Step 6: Update `docs/client-api.md`**

In §2.2's error table (~line 241), add the row:

```
| 403 | `forbidden` | `account_not_allowed` | `{ "code": "forbidden", "reason": "account_not_allowed", "error": "account not allowed for dev-mode mint" }` — dev-mode mint only: the account falls outside the deployment's configured mint guard. |
```

In the reason index table (~line 6885), add:

```
| `account_not_allowed` | `forbidden` | auth-service `POST /api/v1/auth` (dev-mode mint guard) |
```

- [ ] **Step 7: Lint and commit**

```bash
make lint
git add auth-service pkg/errcode/codes_auth.go docs/client-api.md
git commit -m "feat(auth-service): dev-mint account guard (prefix / allowlist) for load-test side issuer"
```

---

### Task 4: `pkg/testutil` ws-NATS helper + roomkeysender migration

**Files:**
- Create: `pkg/testutil/nats_ws.go`
- Modify: `pkg/testutil/terminate.go` (add `TerminateNATSWebSocket()` to `TerminateAll`), `pkg/roomkeysender/integration_test.go` (drop `setupNetwork`/`setupNATS` at :32-102, use the helper)

**Interfaces:**
- Consumes: the container recipe currently inlined at `pkg/roomkeysender/integration_test.go:46-102` (image `testimages.NATS`, config file with `websocket { listen: "0.0.0.0:8080"; no_tls: true }`, wait for "Server is ready"); `testcontainers-go` + `network.New`; the package's `sync.Once` + `TerminateXxx` convention (`pkg/testutil/nats.go:20-95`).
- Produces (used by Task 13 and the migrated roomkeysender tests):

```go
type NATSWebSocketInfo struct {
	WSURL      string // host-reachable ws://<host>:<mapped 8080>
	TCPURL     string // host-reachable nats://<host>:<mapped 4222>
	Network    string // shared docker network name, for sibling containers
	AliasWSURL string // ws://nats-ws:8080 as seen from that network
}
func NATSWebSocket(t *testing.T) NATSWebSocketInfo
func EnsureNATSWebSocket() error
func TerminateNATSWebSocket()
```

- [ ] **Step 1: Implement the helper** (`//go:build integration`; testutil helpers are exercised through their consumers, per the package's existing convention — nats.go has no unit test either)

```go
//go:build integration

package testutil

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/hmchangw/chat/pkg/testutil/testimages"
)

// NATSWebSocketInfo describes the shared WebSocket-enabled NATS instance.
type NATSWebSocketInfo struct {
	WSURL      string
	TCPURL     string
	Network    string
	AliasWSURL string
}

const natsWSAlias = "nats-ws"

var (
	natsWSOnce      sync.Once
	natsWSContainer testcontainers.Container
	natsWSNetwork   *testcontainers.DockerNetwork
	natsWSInfo      NATSWebSocketInfo
	natsWSInitErr   error
)

func ensureNATSWebSocket() (NATSWebSocketInfo, error) {
	natsWSOnce.Do(func() {
		ctx := context.Background()
		nw, err := network.New(ctx)
		if err != nil {
			natsWSInitErr = fmt.Errorf("create ws-nats network: %w", err)
			return
		}
		natsConf := `
listen: 0.0.0.0:4222
websocket {
  listen: "0.0.0.0:8080"
  no_tls: true
}
`
		c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:        testimages.NATS,
				ExposedPorts: []string{"4222/tcp", "8080/tcp"},
				Cmd:          []string{"--config", "/nats.conf"},
				Files: []testcontainers.ContainerFile{{
					Reader:            strings.NewReader(natsConf),
					ContainerFilePath: "/nats.conf",
					FileMode:          0o644,
				}},
				Networks:       []string{nw.Name},
				NetworkAliases: map[string][]string{nw.Name: {natsWSAlias}},
				WaitingFor:     wait.ForLog("Server is ready").WithStartupTimeout(60 * time.Second),
			},
			Started: true,
		})
		if err != nil {
			_ = nw.Remove(ctx)
			natsWSInitErr = fmt.Errorf("start ws-nats: %w", err)
			return
		}
		host, err := c.Host(ctx)
		if err == nil {
			var tcpPort, wsPort interface{ Port() string }
			tcpPort, err = c.MappedPort(ctx, "4222")
			if err == nil {
				wsPort, err = c.MappedPort(ctx, "8080")
				if err == nil {
					natsWSInfo = NATSWebSocketInfo{
						WSURL:      fmt.Sprintf("ws://%s:%s", host, wsPort.Port()),
						TCPURL:     fmt.Sprintf("nats://%s:%s", host, tcpPort.Port()),
						Network:    nw.Name,
						AliasWSURL: fmt.Sprintf("ws://%s:8080", natsWSAlias),
					}
				}
			}
		}
		if err != nil {
			_ = c.Terminate(ctx)
			_ = nw.Remove(ctx)
			natsWSInitErr = fmt.Errorf("resolve ws-nats endpoints: %w", err)
			return
		}
		natsWSContainer = c
		natsWSNetwork = nw
	})
	return natsWSInfo, natsWSInitErr
}

// NATSWebSocket returns the shared WebSocket-enabled NATS instance (no auth,
// no JetStream — a plain broker for client-transport tests).
func NATSWebSocket(t *testing.T) NATSWebSocketInfo {
	t.Helper()
	info, err := ensureNATSWebSocket()
	if err != nil {
		t.Fatalf("testutil.NATSWebSocket: %v", err)
	}
	return info
}

// EnsureNATSWebSocket pre-warms the shared instance for TestMain.
func EnsureNATSWebSocket() error { _, err := ensureNATSWebSocket(); return err }

// TerminateNATSWebSocket stops the shared instance and its network.
func TerminateNATSWebSocket() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if natsWSContainer != nil {
		if err := natsWSContainer.Terminate(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "terminate ws-nats: %v\n", err)
		}
		natsWSContainer = nil
	}
	if natsWSNetwork != nil {
		_ = natsWSNetwork.Remove(ctx)
		natsWSNetwork = nil
	}
}
```

Add `import "testing"` as needed; add `TerminateNATSWebSocket()` to `TerminateAll` in `pkg/testutil/terminate.go`.

- [ ] **Step 2: Migrate roomkeysender**

In `pkg/roomkeysender/integration_test.go`: delete `setupNetwork` and `setupNATS`; at each call site (tests at :187 and :242) replace with:

```go
	info := testutil.NATSWebSocket(t)
	nc, err := nats.Connect(info.TCPURL)
	require.NoError(t, err, "connect to NATS")
	t.Cleanup(func() { nc.Close() })
	wsURL := info.AliasWSURL
```

and pass `info.Network` where the Node container setup (`setupNode`, :106) previously took `nw.Name` (change its parameter from `*testcontainers.DockerNetwork` to `networkName string`). Remove now-unused imports (`network`, possibly `strings`).

- [ ] **Step 3: Run integration tests to verify the migration**

Run: `make test-integration SERVICE=pkg/roomkeysender`
Expected: PASS (same tests, shared container). Note: requires Docker; skips on VFS per the file's `skipOnVFS`.

- [ ] **Step 4: Lint and commit**

```bash
make lint
git add pkg/testutil pkg/roomkeysender
git commit -m "feat(testutil): shared WebSocket-enabled NATS helper; migrate roomkeysender onto it"
```

---

### Task 5: clientsim scaffold — config, pool, shard math

**Files:**
- Create: `tools/clientsim/main.go`, `tools/clientsim/pool.go`, `tools/clientsim/pool_test.go`

**Interfaces:**
- Consumes: `poolartifact.Load` (Task 1), `caarlos0/env/v11`.
- Produces (used by every later task):

```go
type config struct { ... }                    // full env surface below
func shardSlice(accounts []string, target, index, count int) ([]string, error)
```

- [ ] **Step 1: Write the failing shard tests**

```go
// tools/clientsim/pool_test.go
package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func accounts(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = string(rune('a' + i))
	}
	return out
}

func TestShardSlice_PartitionsExactlyTarget(t *testing.T) {
	pool := accounts(10)
	cases := []struct {
		name          string
		target, count int
		wantSizes     []int
	}{
		{"target below pool, 3 shards", 10, 3, []int{3, 4, 3}},
		{"spec regression: target 10 shards 3 sums to 10 not 12", 10, 3, []int{3, 4, 3}},
		{"target 1 shard 3 opens exactly 1 conn", 1, 3, []int{0, 0, 1}},
		{"target above pool clamps to pool", 99, 2, []int{5, 5}},
		{"zero target means whole pool", 0, 2, []int{5, 5}},
		{"single shard", 7, 1, []int{7}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			total := 0
			seen := map[string]bool{}
			for i := 0; i < tt.count; i++ {
				s, err := shardSlice(pool, tt.target, i, tt.count)
				require.NoError(t, err)
				assert.Equal(t, tt.wantSizes[i], len(s), "shard %d size", i)
				for _, a := range s {
					assert.False(t, seen[a], "account %s in two shards", a)
					seen[a] = true
				}
				total += len(s)
			}
			want := tt.target
			if want == 0 || want > len(pool) {
				want = len(pool)
			}
			assert.Equal(t, want, total, "shards must partition exactly T")
		})
	}
}

func TestShardSlice_InvalidInputs(t *testing.T) {
	pool := accounts(4)
	for _, tt := range []struct {
		name                 string
		target, index, count int
	}{
		{"index >= count", 4, 2, 2},
		{"negative index", 4, -1, 2},
		{"zero count", 4, 0, 0},
		{"negative target", -1, 0, 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := shardSlice(pool, tt.target, tt.index, tt.count)
			assert.Error(t, err)
		})
	}
}
```

(Note the floor formula gives sizes `{3,4,3}` for T=10,n=3: bounds 0,3,6,10. The test pins the exact distribution so an accidental ceil can't sneak back.)

- [ ] **Step 2: Run tests, verify failure**

Run: `make test SERVICE=tools/clientsim`
Expected: FAIL — package doesn't exist / `undefined: shardSlice`

- [ ] **Step 3: Implement `pool.go`**

```go
package main

import "fmt"

// shardSlice returns the accounts this shard owns. With T =
// min(target, len(accounts)) (target 0 = whole pool), shard i of n owns
// accounts[floor(T*i/n) : floor(T*(i+1)/n)] — the shards partition exactly
// T accounts with no overlap and sizes differing by at most one (spec §5.1).
func shardSlice(accounts []string, target, index, count int) ([]string, error) {
	if count <= 0 || index < 0 || index >= count || target < 0 {
		return nil, fmt.Errorf("invalid shard parameters: target=%d index=%d count=%d", target, index, count)
	}
	t := target
	if t == 0 || t > len(accounts) {
		t = len(accounts)
	}
	start := t * index / count
	end := t * (index + 1) / count
	return accounts[start:end], nil
}
```

- [ ] **Step 4: Write `main.go` skeleton** (compiles and fail-fasts; the run loop lands in Task 12)

```go
// Command clientsim holds real WSS client connections against a site: per
// simulated user it mints a JWT through the auth exchange, connects over
// NATS WebSocket, performs the production frontend's subscription walk, and
// counts deliveries. See docs/superpowers/specs/2026-08-29-clientsim-design.md.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/hmchangw/chat/pkg/poolartifact"
)

type config struct {
	NATSWSURL         string        `env:"CLIENTSIM_NATS_WS_URL,required"`
	AuthURL           string        `env:"CLIENTSIM_AUTH_URL,required"`
	PoolFile          string        `env:"CLIENTSIM_POOL_FILE,required"`
	SiteID            string        `env:"CLIENTSIM_SITE_ID,required"`
	TargetConns       int           `env:"CLIENTSIM_TARGET_CONNS" envDefault:"0"`
	ShardIndex        int           `env:"CLIENTSIM_SHARD_INDEX" envDefault:"0"`
	ShardCount        int           `env:"CLIENTSIM_SHARD_COUNT" envDefault:"1"`
	RampRate          float64       `env:"CLIENTSIM_RAMP_RATE" envDefault:"50"`
	ChurnRate         float64       `env:"CLIENTSIM_CHURN_RATE" envDefault:"0"`
	JWTMode           string        `env:"CLIENTSIM_JWT_MODE" envDefault:"proactive"`
	SubPendingMsgs    int           `env:"CLIENTSIM_SUB_PENDING_MSGS" envDefault:"512"`
	SubPendingBytes   int           `env:"CLIENTSIM_SUB_PENDING_BYTES" envDefault:"1048576"`
	ReconnectBufBytes int           `env:"CLIENTSIM_RECONNECT_BUF_BYTES" envDefault:"65536"`
	PingInterval      time.Duration `env:"CLIENTSIM_PING_INTERVAL" envDefault:"2m"`
	MetricsAddr       string        `env:"CLIENTSIM_METRICS_ADDR" envDefault:":2112"`
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	if err := run(); err != nil {
		slog.Error("clientsim failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := env.ParseAs[config]()
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}
	if cfg.JWTMode != "proactive" && cfg.JWTMode != "expiry" {
		return fmt.Errorf("CLIENTSIM_JWT_MODE must be proactive or expiry, got %q", cfg.JWTMode)
	}

	pool, err := poolartifact.Load(cfg.PoolFile, cfg.SiteID)
	if err != nil {
		return fmt.Errorf("load pool artifact: %w", err)
	}
	shard, err := shardSlice(pool.Accounts, cfg.TargetConns, cfg.ShardIndex, cfg.ShardCount)
	if err != nil {
		return fmt.Errorf("compute shard: %w", err)
	}
	slog.Info("clientsim starting",
		"runId", pool.RunID, "configDigest", pool.ConfigDigest,
		"shardIndex", cfg.ShardIndex, "shardCount", cfg.ShardCount,
		"shardAccounts", len(shard), "jwtMode", cfg.JWTMode)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	_ = ctx // Task 12 wires the swarm; the skeleton just validates and exits.
	return nil
}
```

- [ ] **Step 5: Run tests + build, verify pass**

Run: `make test SERVICE=tools/clientsim && make build SERVICE=tools/clientsim`
Expected: tests PASS; build succeeds (if `make build` expects service-root layout and fails on a tools/ path, mirror how loadgen builds — its deploy Dockerfile runs the module build — and note the working command in the README in Task 14).

- [ ] **Step 6: Lint and commit**

```bash
make lint
git add tools/clientsim
git commit -m "feat(clientsim): scaffold — config, pool artifact loading, floor-partition sharding"
```

---

### Task 6: clientsim metrics registry

**Files:**
- Create: `tools/clientsim/metrics.go`, `tools/clientsim/metrics_test.go`

**Interfaces:**
- Consumes: `prometheus/client_golang` (private-registry pattern from `tools/loadgen/metrics.go:161,689,730`).
- Produces (used by Tasks 8-13):

```go
type metrics struct {
	Registry          *prometheus.Registry
	ConnsActive       prometheus.Gauge      // clientsim_conns_active
	ConnsConnecting   prometheus.Gauge      // clientsim_conns_connecting
	AuthDuration      prometheus.Histogram  // clientsim_auth_duration_seconds
	ConnectDuration   prometheus.Histogram  // clientsim_connect_duration_seconds
	Disconnects       *prometheus.CounterVec // clientsim_disconnects_total{reason}
	Reconnects        prometheus.Counter    // clientsim_reconnects_total
	JWTRefreshes      *prometheus.CounterVec // clientsim_jwt_refreshes_total{mode}
	Delivered         *prometheus.CounterVec // clientsim_msgs_delivered_total{lane}
	BroadcastLatency  prometheus.Histogram  // clientsim_broadcast_to_client_latency_seconds
	CanonicalLatency  prometheus.Histogram  // clientsim_canonical_to_client_latency_seconds
	DecodeFailures    prometheus.Counter    // clientsim_decode_failures_total
	InvalidTimestamp  prometheus.Counter    // clientsim_invalid_timestamp_total
	SlowConsumer      prometheus.Counter    // clientsim_slow_consumer_events_total (per-EPISODE — see plan preamble)
}
func newMetrics() *metrics
func (m *metrics) Handler() http.Handler
```

- [ ] **Step 1: Write the failing test**

```go
// tools/clientsim/metrics_test.go
package main

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewMetrics_RegistersAllSeries(t *testing.T) {
	m := newMetrics()
	m.ConnsActive.Set(3)
	m.Delivered.WithLabelValues("channel").Add(2)
	m.JWTRefreshes.WithLabelValues("proactive").Inc()
	m.Disconnects.WithLabelValues("auth_expired").Inc()
	m.BroadcastLatency.Observe(0.05)
	m.SlowConsumer.Inc()

	names := []string{
		"clientsim_conns_active",
		"clientsim_msgs_delivered_total",
		"clientsim_jwt_refreshes_total",
		"clientsim_disconnects_total",
		"clientsim_broadcast_to_client_latency_seconds",
		"clientsim_slow_consumer_events_total",
	}
	got, err := testutil.GatherAndCount(m.Registry, names...)
	require.NoError(t, err)
	assert.Equal(t, len(names), got)

	assert.InDelta(t, 3, testutil.ToFloat64(m.ConnsActive), 0.001)
	assert.InDelta(t, 2, testutil.ToFloat64(m.Delivered.WithLabelValues("channel")), 0.001)
}

func TestMetrics_HandlerServesRegistry(t *testing.T) {
	m := newMetrics()
	m.DecodeFailures.Inc()
	body := scrape(t, m) // helper below
	assert.True(t, strings.Contains(body, "clientsim_decode_failures_total 1"))
}
```

with a small `scrape(t, m)` helper using `httptest.NewServer(m.Handler())` + `http.Get` + read body.

- [ ] **Step 2: Run tests, verify failure**

Run: `make test SERVICE=tools/clientsim`
Expected: FAIL — `undefined: newMetrics`

- [ ] **Step 3: Implement `metrics.go`**

```go
package main

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// latencyBuckets match loadgen's shared histogram buckets so the two tools'
// Grafana panels line up (loadgen metrics.go:163).
var latencyBuckets = prometheus.ExponentialBucketsRange(0.001, 5.0, 12)

type metrics struct {
	Registry *prometheus.Registry

	ConnsActive      prometheus.Gauge
	ConnsConnecting  prometheus.Gauge
	AuthDuration     prometheus.Histogram
	ConnectDuration  prometheus.Histogram
	Disconnects      *prometheus.CounterVec
	Reconnects       prometheus.Counter
	JWTRefreshes     *prometheus.CounterVec
	Delivered        *prometheus.CounterVec
	BroadcastLatency prometheus.Histogram
	CanonicalLatency prometheus.Histogram
	DecodeFailures   prometheus.Counter
	InvalidTimestamp prometheus.Counter
	// SlowConsumer counts slow-consumer EPISODES (Active->SlowConsumer
	// transitions), never dropped-message totals — Subscription.Dropped()
	// is lifetime-cumulative and callback-adding it double-counts; see
	// pkg/natsutil/slowconsumer.go for the full trap description.
	SlowConsumer prometheus.Counter
}

func newMetrics() *metrics {
	r := prometheus.NewRegistry()
	m := &metrics{
		Registry:        r,
		ConnsActive:     prometheus.NewGauge(prometheus.GaugeOpts{Name: "clientsim_conns_active", Help: "Connections currently established."}),
		ConnsConnecting: prometheus.NewGauge(prometheus.GaugeOpts{Name: "clientsim_conns_connecting", Help: "Connections mid-handshake (auth or dial)."}),
		AuthDuration:    prometheus.NewHistogram(prometheus.HistogramOpts{Name: "clientsim_auth_duration_seconds", Help: "POST /api/v1/auth exchange duration.", Buckets: latencyBuckets}),
		ConnectDuration: prometheus.NewHistogram(prometheus.HistogramOpts{Name: "clientsim_connect_duration_seconds", Help: "NATS WebSocket connect duration.", Buckets: latencyBuckets}),
		Disconnects:     prometheus.NewCounterVec(prometheus.CounterOpts{Name: "clientsim_disconnects_total", Help: "Disconnections by reason."}, []string{"reason"}),
		Reconnects:      prometheus.NewCounter(prometheus.CounterOpts{Name: "clientsim_reconnects_total", Help: "Successful reconnects."}),
		JWTRefreshes:    prometheus.NewCounterVec(prometheus.CounterOpts{Name: "clientsim_jwt_refreshes_total", Help: "JWT re-mints by lifecycle mode."}, []string{"mode"}),
		Delivered:       prometheus.NewCounterVec(prometheus.CounterOpts{Name: "clientsim_msgs_delivered_total", Help: "Fan-out copies received, by lane. Per-connection copies — NOT comparable to loadgen's logical send counters."}, []string{"lane"}),
		BroadcastLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "clientsim_broadcast_to_client_latency_seconds", Buckets: latencyBuckets,
			Help: "receive - RoomEvent.Timestamp (broadcast publish -> client edge; carries inter-host clock skew)."}),
		CanonicalLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "clientsim_canonical_to_client_latency_seconds", Buckets: latencyBuckets,
			Help: "receive - RoomEvent.EventTimestamp (canonical publish -> client edge; carries inter-host clock skew)."}),
		DecodeFailures:   prometheus.NewCounter(prometheus.CounterOpts{Name: "clientsim_decode_failures_total", Help: "Envelope decode failures; any increment marks the window degraded."}),
		InvalidTimestamp: prometheus.NewCounter(prometheus.CounterOpts{Name: "clientsim_invalid_timestamp_total", Help: "Zero or negative observed event age; any increment marks the window degraded."}),
		SlowConsumer:     prometheus.NewCounter(prometheus.CounterOpts{Name: "clientsim_slow_consumer_events_total", Help: "Slow-consumer episodes (per transition, not per dropped message); any increment marks the window degraded."}),
	}
	r.MustRegister(
		m.ConnsActive, m.ConnsConnecting, m.AuthDuration, m.ConnectDuration,
		m.Disconnects, m.Reconnects, m.JWTRefreshes, m.Delivered,
		m.BroadcastLatency, m.CanonicalLatency,
		m.DecodeFailures, m.InvalidTimestamp, m.SlowConsumer,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return m
}

// Handler serves this registry (mounted at the metrics server root, like loadgen).
func (m *metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}
```

- [ ] **Step 4: Run tests, verify pass**

Run: `make test SERVICE=tools/clientsim`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
make lint
git add tools/clientsim
git commit -m "feat(clientsim): prometheus metrics registry (conn, latency, loss-visibility series)"
```

---

### Task 7: TokenProvider + auth exchange client

**Files:**
- Create: `tools/clientsim/token.go`, `tools/clientsim/token_test.go`

**Interfaces:**
- Consumes: `pkg/restyutil.New(baseURL, opts...)` (`pkg/restyutil/restyutil.go:55`), auth-service wire shapes (request `{account, natsPublicKey}`; success `{"natsJwt": "..."}`; error = errcode envelope `{"code","reason","error"}` — `docs/client-api.md` §2.2).
- Produces (used by Task 11):

```go
type authRequestFields struct {
	SSOToken      string `json:"ssoToken,omitempty"`
	AuthToken     string `json:"authToken,omitempty"`
	Account       string `json:"account,omitempty"`
	NATSPublicKey string `json:"natsPublicKey"`
}
type TokenProvider interface{ Material(account string) (authRequestFields, error) }
type devProvider struct{}
func newAuthClient(baseURL string, p TokenProvider, m *metrics) *authClient
func (c *authClient) Mint(ctx context.Context, account, natsPubKey string) (string, error)
```

- [ ] **Step 1: Write the failing tests** (httptest server shaped like auth-service)

```go
// tools/clientsim/token_test.go
package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDevProvider_SendsAccountOnly(t *testing.T) {
	f, err := devProvider{}.Material("user-7")
	require.NoError(t, err)
	assert.Equal(t, "user-7", f.Account)
	assert.Empty(t, f.SSOToken)
	assert.Empty(t, f.AuthToken)
}

func TestAuthClient_Mint(t *testing.T) {
	var gotBody map[string]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/v1/auth", r.URL.Path)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&gotBody))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"natsJwt":"eyJ.fake.jwt","userInfo":{"account":"user-7"}}`))
	}))
	defer srv.Close()

	c := newAuthClient(srv.URL, devProvider{}, newMetrics())
	jwt, err := c.Mint(context.Background(), "user-7", "UABCDEF")
	require.NoError(t, err)
	assert.Equal(t, "eyJ.fake.jwt", jwt)
	assert.Equal(t, "user-7", gotBody["account"])
	assert.Equal(t, "UABCDEF", gotBody["natsPublicKey"])
	assert.NotContains(t, gotBody, "ssoToken") // omitempty holds
}

func TestAuthClient_Mint_ErrorEnvelope(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"code":"forbidden","reason":"account_not_allowed","error":"account not allowed for dev-mode mint"}`))
	}))
	defer srv.Close()

	c := newAuthClient(srv.URL, devProvider{}, newMetrics())
	_, err := c.Mint(context.Background(), "alice", "UABCDEF")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "account_not_allowed")
}

func TestAuthClient_Mint_ServerDown(t *testing.T) {
	c := newAuthClient("http://127.0.0.1:1", devProvider{}, newMetrics())
	_, err := c.Mint(context.Background(), "user-1", "UABCDEF")
	assert.Error(t, err)
}
```

- [ ] **Step 2: Run tests, verify failure**

Run: `make test SERVICE=tools/clientsim`
Expected: FAIL — `undefined: devProvider`, `undefined: newAuthClient`

- [ ] **Step 3: Implement `token.go`**

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/go-resty/resty/v2"

	"github.com/hmchangw/chat/pkg/restyutil"
)

// authRequestFields mirrors auth-service's authRequest wire shape.
type authRequestFields struct {
	SSOToken      string `json:"ssoToken,omitempty"`
	AuthToken     string `json:"authToken,omitempty"`
	Account       string `json:"account,omitempty"`
	NATSPublicKey string `json:"natsPublicKey"`
}

// TokenProvider supplies the auth material for one account. devProvider is
// the only implementation today; a file-backed SSO/session provider is the
// spec's future extension point.
type TokenProvider interface {
	Material(account string) (authRequestFields, error)
}

type devProvider struct{}

func (devProvider) Material(account string) (authRequestFields, error) {
	return authRequestFields{Account: account}, nil
}

type authClient struct {
	rc       *resty.Client
	provider TokenProvider
	metrics  *metrics
}

func newAuthClient(baseURL string, p TokenProvider, m *metrics) *authClient {
	return &authClient{
		rc:       restyutil.New(baseURL, restyutil.WithTimeout(10*time.Second)),
		provider: p,
		metrics:  m,
	}
}

type authMintResponse struct {
	NATSJWT string `json:"natsJwt"`
}

type authErrorEnvelope struct {
	Code    string `json:"code"`
	Reason  string `json:"reason"`
	Message string `json:"error"`
}

// Mint runs the full auth exchange and returns the freshly minted NATS user
// JWT. It never logs the JWT or any token material.
func (c *authClient) Mint(ctx context.Context, account, natsPubKey string) (string, error) {
	fields, err := c.provider.Material(account)
	if err != nil {
		return "", fmt.Errorf("token material for %s: %w", account, err)
	}
	fields.NATSPublicKey = natsPubKey

	start := time.Now()
	var ok authMintResponse
	var bad authErrorEnvelope
	resp, err := c.rc.R().SetContext(ctx).
		SetBody(fields).SetResult(&ok).SetError(&bad).
		Post("/api/v1/auth")
	if err != nil {
		return "", fmt.Errorf("auth exchange: %w", err)
	}
	c.metrics.AuthDuration.Observe(time.Since(start).Seconds())
	if resp.IsError() {
		return "", fmt.Errorf("auth exchange rejected (%d %s/%s): %s",
			resp.StatusCode(), bad.Code, bad.Reason, bad.Message)
	}
	if ok.NATSJWT == "" {
		return "", fmt.Errorf("auth exchange returned no natsJwt (status %d)", resp.StatusCode())
	}
	return ok.NATSJWT, nil
}
```

(Before finishing, open `auth-service/handler.go`'s `authResponse` and confirm the JSON tag of `NATSJWT` is `natsJwt`; adjust `authMintResponse` if it differs.)

- [ ] **Step 4: Run tests, verify pass**

Run: `make test SERVICE=tools/clientsim`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
make lint
git add tools/clientsim
git commit -m "feat(clientsim): TokenProvider and auth exchange client (dev-mint path)"
```

---

### Task 8: subscription walk planner

**Files:**
- Create: `tools/clientsim/subwalk.go`, `tools/clientsim/subwalk_test.go`

**Interfaces:**
- Consumes: wire shapes of `subscription.list` (request `{"type","offset","limit"}`; reply `{"subscriptions":[...],"hasMore":bool}` raw, no wrapper — `user-service/models/subscription.go:14-39`; rows are flat Subscription fields: `roomId`, `roomType`, nested `room.crossSite *bool`).
- Produces (used by Tasks 9, 11, 12, 13):

```go
type subscriptionLister interface {
	List(ctx context.Context, req subListRequest) (*subListPage, error)
}
type subListRequest struct {
	Type   string `json:"type"`
	Offset int    `json:"offset,omitempty"`
	Limit  int    `json:"limit,omitempty"`
}
type subRoom struct {
	CrossSite *bool `json:"crossSite,omitempty"`
}
type subRow struct {
	RoomID   string   `json:"roomId"`
	RoomType string   `json:"roomType"`
	Room     *subRoom `json:"room,omitempty"`
}
type subListPage struct {
	Subscriptions []subRow `json:"subscriptions"`
	HasMore       bool     `json:"hasMore"`
}
// subscription plan: roomID -> global (true = chat.room.*, false = chat.local.room.*)
func fetchSubscriptionPlan(ctx context.Context, l subscriptionLister) (map[string]bool, error)
func roomGlobal(room *subRoom) bool // the frontend tri-state: only explicit false is local
const subListPageLimit = 40
```

- [ ] **Step 1: Write the failing tests**

```go
// tools/clientsim/subwalk_test.go
package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeLister struct {
	pages []subListPage
	reqs  []subListRequest
	err   error
}

func (f *fakeLister) List(_ context.Context, req subListRequest) (*subListPage, error) {
	f.reqs = append(f.reqs, req)
	if f.err != nil {
		return nil, f.err
	}
	i := len(f.reqs) - 1
	if i >= len(f.pages) {
		return &subListPage{}, nil
	}
	return &f.pages[i], nil
}

func bptr(b bool) *bool { return &b }

func TestFetchSubscriptionPlan_PaginatesAndFilters(t *testing.T) {
	l := &fakeLister{pages: []subListPage{
		{Subscriptions: []subRow{
			{RoomID: "r1", RoomType: "channel", Room: &subRoom{CrossSite: bptr(true)}},
			{RoomID: "d1", RoomType: "dm"},     // DM: user lane, never a room sub
			{RoomID: "b1", RoomType: "botDM"},  // botDM: filtered too
		}, HasMore: true},
		{Subscriptions: []subRow{
			{RoomID: "r2", RoomType: "channel", Room: &subRoom{CrossSite: bptr(false)}}, // explicit false -> local
			{RoomID: "r1", RoomType: "channel", Room: &subRoom{CrossSite: bptr(true)}},  // cross-page duplicate
			{RoomID: "r3", RoomType: "channel"},                                          // no room object -> global fail-safe
			{RoomID: "r4", RoomType: "channel", Room: &subRoom{}},                        // nil crossSite -> global fail-safe
		}, HasMore: false},
	}}

	plan, err := fetchSubscriptionPlan(context.Background(), l)
	require.NoError(t, err)
	assert.Equal(t, map[string]bool{"r1": true, "r2": false, "r3": true, "r4": true}, plan)

	// Request contract: type is always "rooms", offset advances by the sent limit.
	require.Len(t, l.reqs, 2)
	assert.Equal(t, subListRequest{Type: "rooms", Offset: 0, Limit: subListPageLimit}, l.reqs[0])
	assert.Equal(t, subListRequest{Type: "rooms", Offset: subListPageLimit, Limit: subListPageLimit}, l.reqs[1])
}

func TestFetchSubscriptionPlan_EmptySidebar(t *testing.T) {
	l := &fakeLister{pages: []subListPage{{HasMore: false}}}
	plan, err := fetchSubscriptionPlan(context.Background(), l)
	require.NoError(t, err)
	assert.Empty(t, plan)
}

func TestFetchSubscriptionPlan_ListerError(t *testing.T) {
	l := &fakeLister{err: assert.AnError}
	_, err := fetchSubscriptionPlan(context.Background(), l)
	assert.Error(t, err)
}

func TestRoomGlobal_TriState(t *testing.T) {
	assert.True(t, roomGlobal(nil), "missing room object -> global")
	assert.True(t, roomGlobal(&subRoom{}), "nil crossSite -> global")
	assert.True(t, roomGlobal(&subRoom{CrossSite: bptr(true)}))
	assert.False(t, roomGlobal(&subRoom{CrossSite: bptr(false)}), "only explicit false is local")
}
```

- [ ] **Step 2: Run tests, verify failure**

Run: `make test SERVICE=tools/clientsim`
Expected: FAIL — `undefined: fetchSubscriptionPlan` etc.

- [ ] **Step 3: Implement `subwalk.go`** — the pagination loop with `hasMore`/offset advance, `roomId` dedupe (first occurrence wins), `roomType == "channel"` filter, and the tri-state helper:

```go
package main

import (
	"context"
	"fmt"
)

// subListPageLimit is the server's default page size; we request it
// explicitly so the walk is deterministic (docs/client-api.md §subscription.list).
const subListPageLimit = 40

type subListRequest struct {
	Type   string `json:"type"`
	Offset int    `json:"offset,omitempty"`
	Limit  int    `json:"limit,omitempty"`
}

type subRoom struct {
	CrossSite *bool `json:"crossSite,omitempty"`
}

type subRow struct {
	RoomID   string   `json:"roomId"`
	RoomType string   `json:"roomType"`
	Room     *subRoom `json:"room,omitempty"`
}

type subListPage struct {
	Subscriptions []subRow `json:"subscriptions"`
	HasMore       bool     `json:"hasMore"`
}

type subscriptionLister interface {
	List(ctx context.Context, req subListRequest) (*subListPage, error)
}

// roomGlobal applies the frontend's tri-state crossSite rule: only an
// explicit false routes to the local namespace; missing data fails safe to
// global (chat-frontend subjects.ts:40).
func roomGlobal(room *subRoom) bool {
	return room == nil || room.CrossSite == nil || *room.CrossSite
}

// fetchSubscriptionPlan runs the paginated subscription.list bootstrap and
// returns roomID -> global for every channel subscription. Cross-page
// duplicate rows are deduped by roomID (multi-page drains are best-effort
// ordered; docs/client-api.md).
func fetchSubscriptionPlan(ctx context.Context, l subscriptionLister) (map[string]bool, error) {
	plan := map[string]bool{}
	for offset := 0; ; offset += subListPageLimit {
		page, err := l.List(ctx, subListRequest{Type: "rooms", Offset: offset, Limit: subListPageLimit})
		if err != nil {
			return nil, fmt.Errorf("subscription.list page at offset %d: %w", offset, err)
		}
		for _, row := range page.Subscriptions {
			if row.RoomType != "channel" {
				continue
			}
			if _, seen := plan[row.RoomID]; seen {
				continue
			}
			plan[row.RoomID] = roomGlobal(row.Room)
		}
		if !page.HasMore {
			return plan, nil
		}
	}
}
```

- [ ] **Step 4: Run tests, verify pass**

Run: `make test SERVICE=tools/clientsim`
Expected: PASS

- [ ] **Step 5: Add the NATS-backed lister** (same file) — used by Tasks 11/13; a thin adapter, tested through the integration test:

```go
// natsLister issues the real client RPC over the simulated user's own
// connection, exactly as the frontend does.
type natsLister struct {
	nc      *nats.Conn
	subject string // subject.UserSubscriptionList(account, siteID)
	timeout time.Duration
}

func (l *natsLister) List(ctx context.Context, req subListRequest) (*subListPage, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal subscription.list request: %w", err)
	}
	msg, err := l.nc.RequestWithContext(ctx, l.subject, body)
	if err != nil {
		return nil, fmt.Errorf("subscription.list request: %w", err)
	}
	if ec := errcode.Parse(msg.Data); ec != nil {
		return nil, fmt.Errorf("subscription.list rejected: %w", ec)
	}
	var page subListPage
	if err := json.Unmarshal(msg.Data, &page); err != nil {
		return nil, fmt.Errorf("decode subscription.list reply: %w", err)
	}
	return &page, nil
}
```

(Confirm `errcode.Parse`'s exact signature at `pkg/errcode/parse.go:11` — it returns the parsed `*Error` or nil; adapt the guard if the shape differs. Add imports: `encoding/json`, `time`, `github.com/nats-io/nats.go`, `github.com/hmchangw/chat/pkg/errcode`.)

- [ ] **Step 6: Run tests + lint, commit**

```bash
make test SERVICE=tools/clientsim && make lint
git add tools/clientsim
git commit -m "feat(clientsim): paginated subscription.list bootstrap walk with dedupe and crossSite tri-state"
```

---

### Task 9: delivery handler

**Files:**
- Create: `tools/clientsim/delivery.go`, `tools/clientsim/delivery_test.go`

**Interfaces:**
- Consumes: `model.RoomEvent` (`pkg/model/event.go:376` — `Timestamp int64` ms, `EventTimestamp int64` ms, both event-level), `metrics` (Task 6).
- Produces (used by Tasks 11, 13): `func handleDelivery(m *metrics, lane string, data []byte, now time.Time)` — lanes `"user"` and `"channel"`.

- [ ] **Step 1: Write the failing tests**

```go
// tools/clientsim/delivery_test.go
package main

import (
	"encoding/json"
	"testing"
	"time"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

func roomEventJSON(t *testing.T, ts, eventTS int64) []byte {
	t.Helper()
	data, err := json.Marshal(model.RoomEvent{
		Type: model.RoomEventNewMessage, RoomID: "r1",
		Timestamp: ts, EventTimestamp: eventTS,
	})
	require.NoError(t, err)
	return data
}

func TestHandleDelivery_ObservesBothLatencies(t *testing.T) {
	m := newMetrics()
	now := time.Now()
	data := roomEventJSON(t, now.Add(-50*time.Millisecond).UnixMilli(), now.Add(-120*time.Millisecond).UnixMilli())

	handleDelivery(m, "channel", data, now)

	assert.InDelta(t, 1, promtestutil.ToFloat64(m.Delivered.WithLabelValues("channel")), 0.001)
	assert.Equal(t, uint64(1), histogramCount(t, m.Registry, "clientsim_broadcast_to_client_latency_seconds"))
	assert.Equal(t, uint64(1), histogramCount(t, m.Registry, "clientsim_canonical_to_client_latency_seconds"))
	assert.InDelta(t, 0, promtestutil.ToFloat64(m.InvalidTimestamp), 0.001)
}

func TestHandleDelivery_InvalidTimestamps(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name        string
		ts, eventTS int64
		wantInvalid float64
		wantBcast   uint64
	}{
		{"zero broadcast ts", 0, now.Add(-1 * time.Millisecond).UnixMilli(), 1, 0},
		{"future broadcast ts (negative age)", now.Add(time.Minute).UnixMilli(), now.Add(-1 * time.Millisecond).UnixMilli(), 1, 0},
		{"zero canonical ts still observes broadcast", now.Add(-5 * time.Millisecond).UnixMilli(), 0, 1, 1},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			m := newMetrics()
			handleDelivery(m, "user", roomEventJSON(t, tt.ts, tt.eventTS), now)
			assert.InDelta(t, tt.wantInvalid, promtestutil.ToFloat64(m.InvalidTimestamp), 0.001)
			assert.Equal(t, tt.wantBcast, histogramCount(t, m.Registry, "clientsim_broadcast_to_client_latency_seconds"))
			// Delivery is counted regardless of timestamp quality.
			assert.InDelta(t, 1, promtestutil.ToFloat64(m.Delivered.WithLabelValues("user")), 0.001)
		})
	}
}

func TestHandleDelivery_DecodeFailure(t *testing.T) {
	m := newMetrics()
	handleDelivery(m, "user", []byte("{not json"), time.Now())
	assert.InDelta(t, 1, promtestutil.ToFloat64(m.DecodeFailures), 0.001)
	assert.InDelta(t, 1, promtestutil.ToFloat64(m.Delivered.WithLabelValues("user")), 0.001)
}

func TestHandleDelivery_NonRoomEventJSONStillCounts(t *testing.T) {
	// Other event types on the same subjects (edits, read receipts) decode
	// but carry zero Timestamp fields -> counted, no latency observation.
	m := newMetrics()
	handleDelivery(m, "channel", []byte(`{"type":"message_read","roomId":"r1"}`), time.Now())
	assert.InDelta(t, 1, promtestutil.ToFloat64(m.Delivered.WithLabelValues("channel")), 0.001)
	assert.Equal(t, uint64(0), histogramCount(t, m.Registry, "clientsim_broadcast_to_client_latency_seconds"))
}
```

Add the `histogramCount(t, reg, name)` helper: gather the registry, find the family by name, return `family.Metric[0].Histogram.GetSampleCount()`.

- [ ] **Step 2: Run tests, verify failure**

Run: `make test SERVICE=tools/clientsim`
Expected: FAIL — `undefined: handleDelivery`

- [ ] **Step 3: Implement `delivery.go`**

```go
package main

import (
	"encoding/json"
	"time"

	"github.com/hmchangw/chat/pkg/model"
)

// handleDelivery records one received fan-out copy. The payload is counted,
// its cleartext envelope timestamps observed, and then dropped — never
// stored, never logged (spec §6.4).
func handleDelivery(m *metrics, lane string, data []byte, now time.Time) {
	m.Delivered.WithLabelValues(lane).Inc()

	var evt model.RoomEvent
	if err := json.Unmarshal(data, &evt); err != nil {
		m.DecodeFailures.Inc()
		return
	}
	observeAge(m, m.BroadcastLatency, evt.Timestamp, now)
	observeAge(m, m.CanonicalLatency, evt.EventTimestamp, now)
}

// observeAge records now - tsMillis when the age is positive. A zero
// timestamp on an otherwise-valid event (a non-RoomEvent type on the same
// subject) is not an error — only RoomEvent stamps these fields — so zero
// skips silently for the canonical field and counts invalid only when the
// event claimed a timestamp that produces a non-positive age.
func observeAge(m *metrics, h prometheus.Histogram, tsMillis int64, now time.Time) {
	if tsMillis == 0 {
		return
	}
	age := now.Sub(time.UnixMilli(tsMillis))
	if age <= 0 {
		m.InvalidTimestamp.Inc()
		return
	}
	h.Observe(age.Seconds())
}
```

Reconcile with the tests: the "zero broadcast ts" case expects `InvalidTimestamp` to increment — a RoomEvent `new_message` without `Timestamp` is a contract violation while other event types legitimately omit it. Implement that distinction: in `handleDelivery`, when `evt.Type == model.RoomEventNewMessage` and `evt.Timestamp == 0`, increment `InvalidTimestamp`; otherwise a zero timestamp skips silently. (This is exactly what the four test cases pin down — write the implementation to make them pass.)

- [ ] **Step 4: Run tests, verify pass**

Run: `make test SERVICE=tools/clientsim`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
make lint
git add tools/clientsim
git commit -m "feat(clientsim): delivery handler — lane counters and edge-latency observation"
```

---

### Task 10: live subscription.update reconcile

**Files:**
- Create: `tools/clientsim/liveupdate.go`, `tools/clientsim/liveupdate_test.go`

**Interfaces:**
- Consumes: wire shapes on `chat.user.{EncodeAccount(account)}.event.subscription.update` — `model.SubscriptionUpdateEvent` (`pkg/model/event.go:81`, action `added`/`role_updated`/... with full `Subscription`) and `model.SubscriptionRemovedEvent` (`event.go:556`, action `removed`, lean `{roomId, roomType, u}`). Both decode into one local struct.
- Produces (used by Tasks 11, 12):

```go
type subChangeOp int
const (
	subOpen subChangeOp = iota
	subClose
)
type subChange struct {
	Op     subChangeOp
	RoomID string
	Global bool // meaningful for subOpen
}
// applySubscriptionUpdate mutates plan and returns the subscription changes
// the connection must apply (0, 1, or 2 — namespace flip closes then opens).
func applySubscriptionUpdate(plan map[string]bool, data []byte) ([]subChange, error)
// diffPlans returns changes to move from old to new (post-reconnect resync).
func diffPlans(old, new map[string]bool) []subChange
```

- [ ] **Step 1: Write the failing tests**

```go
// tools/clientsim/liveupdate_test.go
package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func updJSON(action, roomID, roomType string, crossSite *bool) []byte {
	room := ""
	if crossSite != nil {
		room = fmt.Sprintf(`,"room":{"crossSite":%v}`, *crossSite)
	}
	return []byte(fmt.Sprintf(
		`{"action":%q,"subscription":{"roomId":%q,"roomType":%q%s},"timestamp":1}`,
		action, roomID, roomType, room))
}

func TestApplySubscriptionUpdate(t *testing.T) {
	tr, fa := true, false
	cases := []struct {
		name     string
		plan     map[string]bool
		payload  []byte
		wantPlan map[string]bool
		want     []subChange
	}{
		{"added channel opens global", map[string]bool{},
			updJSON("added", "r1", "channel", &tr),
			map[string]bool{"r1": true},
			[]subChange{{Op: subOpen, RoomID: "r1", Global: true}}},
		{"added channel missing crossSite fails safe to global", map[string]bool{},
			updJSON("added", "r1", "channel", nil),
			map[string]bool{"r1": true},
			[]subChange{{Op: subOpen, RoomID: "r1", Global: true}}},
		{"added dm is user-lane only, no change", map[string]bool{},
			updJSON("added", "d1", "dm", nil),
			map[string]bool{}, nil},
		{"removed closes and forgets", map[string]bool{"r1": true},
			updJSON("removed", "r1", "channel", nil),
			map[string]bool{},
			[]subChange{{Op: subClose, RoomID: "r1"}}},
		{"removed unknown room is a no-op", map[string]bool{},
			updJSON("removed", "rX", "channel", nil),
			map[string]bool{}, nil},
		{"crossSite flip closes old namespace, opens new", map[string]bool{"r1": true},
			updJSON("added", "r1", "channel", &fa),
			map[string]bool{"r1": false},
			[]subChange{{Op: subClose, RoomID: "r1"}, {Op: subOpen, RoomID: "r1", Global: false}}},
		{"same namespace re-add is a no-op (never double-subscribed)", map[string]bool{"r1": true},
			updJSON("added", "r1", "channel", &tr),
			map[string]bool{"r1": true}, nil},
		{"role_updated carrying a subscription does not touch subs", map[string]bool{"r1": true},
			updJSON("role_updated", "r1", "channel", &tr),
			map[string]bool{"r1": true}, nil},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			changes, err := applySubscriptionUpdate(tt.plan, tt.payload)
			require.NoError(t, err)
			assert.Equal(t, tt.want, changes)
			assert.Equal(t, tt.wantPlan, tt.plan)
		})
	}
}

func TestApplySubscriptionUpdate_Malformed(t *testing.T) {
	_, err := applySubscriptionUpdate(map[string]bool{}, []byte("{oops"))
	assert.Error(t, err)
}

func TestDiffPlans_Resync(t *testing.T) {
	old := map[string]bool{"r1": true, "r2": false, "r3": true}
	new := map[string]bool{"r1": true, "r2": true, "r4": false}
	changes := diffPlans(old, new)
	assert.ElementsMatch(t, []subChange{
		{Op: subClose, RoomID: "r2"}, {Op: subOpen, RoomID: "r2", Global: true}, // namespace flip
		{Op: subClose, RoomID: "r3"},                                            // vanished
		{Op: subOpen, RoomID: "r4", Global: false},                              // appeared
	}, changes)
}
```

- [ ] **Step 2: Run tests, verify failure**

Run: `make test SERVICE=tools/clientsim`
Expected: FAIL — `undefined: applySubscriptionUpdate`

- [ ] **Step 3: Implement `liveupdate.go`** — one decode struct (`Action string` + `Subscription subRow` reusing Task 8's `subRow`), switch on action: `"added"` with `RoomType == "channel"` → compute `roomGlobal`, compare with plan, emit open / close+open / nothing; `"removed"` → close if present; everything else → nil. `diffPlans` iterates old (close vanished / flip) then new (open appeared).

- [ ] **Step 4: Run tests, verify pass**

Run: `make test SERVICE=tools/clientsim`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
make lint
git add tools/clientsim
git commit -m "feat(clientsim): live subscription.update reconcile and post-reconnect plan diff"
```

---

### Task 11: simClient lifecycle

**Files:**
- Create: `tools/clientsim/client.go`, `tools/clientsim/client_test.go`

**Interfaces:**
- Consumes: `authClient.Mint` (Task 7), `fetchSubscriptionPlan`/`natsLister`/`roomGlobal` (Task 8), `handleDelivery` (Task 9), `applySubscriptionUpdate`/`diffPlans` (Task 10), `metrics` (Task 6); `pkg/subject` builders `UserRoomEvent(account)`, `SubscriptionUpdate(account)` (encodes dots), `UserSubscriptionList(account, siteID)`, `RoomEvent(roomID, global)`; `nats.UserJWT(userCB, sigCB)`, `nkeys.CreateUser()`, `nc.ForceReconnect()` (nats.go v1.50.0).
- Produces (used by Tasks 12, 13):

```go
type minter interface {
	Mint(ctx context.Context, account, natsPubKey string) (string, error)
}
type simClient struct { ... }
func newSimClient(account string, cfg *config, mint minter, m *metrics) (*simClient, error)
func (s *simClient) run(ctx context.Context) error  // connect, walk, live-maintain; returns when ctx ends
func (s *simClient) close()
// internal, unit-tested directly:
type jwtCache struct{ ... }  // get/set, plus expiry captured at set time
func refreshDelay(expiresAt time.Time, now time.Time, randFloat func() float64) time.Duration // 80% ±5% of remaining life
```

- [ ] **Step 1: Write the failing unit tests** — the pure pieces (cache, refresh delay, single-mint invariant) with a fake minter; no real NATS in unit tests:

```go
// tools/clientsim/client_test.go
package main

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type countingMinter struct {
	calls atomic.Int64
	jwt   string
	err   error
}

func (c *countingMinter) Mint(context.Context, string, string) (string, error) {
	c.calls.Add(1)
	if c.err != nil {
		return "", c.err
	}
	return c.jwt, nil
}

func TestRefreshDelay_EightyPercentWithJitter(t *testing.T) {
	now := time.Now()
	expires := now.Add(2 * time.Hour)
	t.Run("midpoint rand -> exactly 80%", func(t *testing.T) {
		d := refreshDelay(expires, now, func() float64 { return 0.5 })
		assert.InDelta(t, (96 * time.Minute).Seconds(), d.Seconds(), 1)
	})
	t.Run("jitter bounds are ±5% of remaining life", func(t *testing.T) {
		lo := refreshDelay(expires, now, func() float64 { return 0 })
		hi := refreshDelay(expires, now, func() float64 { return 1 })
		assert.InDelta(t, (2 * time.Hour * 75 / 100).Seconds(), lo.Seconds(), 1)
		assert.InDelta(t, (2 * time.Hour * 85 / 100).Seconds(), hi.Seconds(), 1)
	})
	t.Run("already expired -> immediate", func(t *testing.T) {
		d := refreshDelay(now.Add(-time.Minute), now, func() float64 { return 0.5 })
		assert.LessOrEqual(t, d, time.Duration(0))
	})
}

func TestJWTCache_SingleMintInvariant_Proactive(t *testing.T) {
	// In proactive mode the connect callback must ONLY read the cache; a
	// refresh cycle costs exactly one Mint (spec §5.2 step 3/6).
	mint := &countingMinter{jwt: mintTestJWT(t, time.Now().Add(2*time.Hour))}
	s := newTestSimClient(t, "user-1", "proactive", mint)

	require.NoError(t, s.primeJWT(context.Background())) // initial mint
	assert.Equal(t, int64(1), mint.calls.Load())

	// Simulate three (re)connect callback invocations: no further mints.
	for i := 0; i < 3; i++ {
		jwtStr, err := s.userCB()
		require.NoError(t, err)
		assert.NotEmpty(t, jwtStr)
	}
	assert.Equal(t, int64(1), mint.calls.Load())

	// One proactive refresh cycle: exactly one more mint.
	require.NoError(t, s.refreshJWT(context.Background()))
	assert.Equal(t, int64(2), mint.calls.Load())
}

func TestJWTCache_ExpiryModeMintsOnExpiredCache(t *testing.T) {
	mint := &countingMinter{jwt: mintTestJWT(t, time.Now().Add(2*time.Hour))}
	s := newTestSimClient(t, "user-1", "expiry", mint)
	require.NoError(t, s.primeJWT(context.Background()))
	require.Equal(t, int64(1), mint.calls.Load())

	// Valid cache: callback reads it, no mint.
	_, err := s.userCB()
	require.NoError(t, err)
	assert.Equal(t, int64(1), mint.calls.Load())

	// Force the cache to look expired; the next callback (the reconnect
	// path) mints exactly once.
	s.cache.forceExpireForTest()
	_, err = s.userCB()
	require.NoError(t, err)
	assert.Equal(t, int64(2), mint.calls.Load())
}

func TestSigCB_SignsWithClientNKey(t *testing.T) {
	s := newTestSimClient(t, "user-1", "proactive", &countingMinter{jwt: "j"})
	sig, err := s.sigCB([]byte("nonce"))
	require.NoError(t, err)
	assert.NotEmpty(t, sig)
}
```

Helpers to define in the test file: `newTestSimClient(t, account, mode, mint)` building a `simClient` via `newSimClient` with a minimal `config{JWTMode: mode, ...}` and `newMetrics()`; `mintTestJWT(t, expires)` producing a parseable NATS user JWT — use `jwt.NewUserClaims(pub)` with `Expires = expires.Unix()` encoded by a throwaway `nkeys.CreateAccount()` key (same pattern as `tools/nats-debug/hub_nats_test.go:26-46`). The cache stores the expiry parsed from the JWT's claims at `set` time (`jwt.DecodeUserClaims`), so `refreshDelay` and expiry-mode checks need no clock plumbing beyond `time.Now`.

- [ ] **Step 2: Run tests, verify failure**

Run: `make test SERVICE=tools/clientsim`
Expected: FAIL — `undefined: refreshDelay`, `undefined: newSimClient`, ...

- [ ] **Step 3: Implement `client.go`**

Core pieces (write in full; sketched here to fix names and behavior):

- `jwtCache`: mutex-guarded `{token string; expiresAt time.Time}`; `set(tok)` decodes claims via `jwt.DecodeUserClaims` for `expiresAt`; `get() (string, time.Time)`; `forceExpireForTest()` zeroes `expiresAt` (test seam, kept in client.go — it's three lines).
- `simClient` fields: `account`, `cfg *config`, `mint minter`, `m *metrics`, `nkeyPair nkeys.KeyPair`, `nkeyPub string`, `cache jwtCache`, `nc *nats.Conn`, `plan map[string]bool`, `roomSubs map[string]*nats.Subscription`, `mu sync.Mutex`.
- `newSimClient`: `nkeys.CreateUser()`, store pair + pub.
- `primeJWT(ctx)`: mint → `cache.set`.
- `refreshJWT(ctx)`: mint → `cache.set` → `m.JWTRefreshes.WithLabelValues(cfg.JWTMode).Inc()`.
- `userCB() (string, error)`: proactive → return cached token (error if empty); expiry → if `expiresAt` in the past, `refreshJWT(context.Background())` first (the one reconnect-path mint), then return cache.
- `sigCB(nonce []byte) ([]byte, error)`: `s.nkeyPair.Sign(nonce)`.
- `connect(ctx)`: observe `ConnectDuration`; `nats.Connect(cfg.NATSWSURL, nats.UserJWT(s.userCB, s.sigCB), nats.MaxReconnects(-1), nats.ReconnectBufSize(cfg.ReconnectBufBytes), nats.PingInterval(cfg.PingInterval), nats.ReconnectHandler(...Reconnects.Inc + resync), nats.DisconnectErrHandler(...Disconnects{reason: errString}.Inc), nats.ErrorHandler(...))`. The error handler increments `m.SlowConsumer` when `errors.Is(err, nats.ErrSlowConsumer)`, `m.Disconnects` otherwise-not — keep it episode-semantics only, per the metric's doc comment.
- Subscriptions: after connect, `SetPendingLimits(cfg.SubPendingMsgs, cfg.SubPendingBytes)` on every subscription; user lane on `subject.UserRoomEvent(s.account)` → `handleDelivery(m, "user", msg.Data, time.Now())`; update lane on `subject.SubscriptionUpdate(s.account)` (subscribed **before** the bootstrap walk) → `applySubscriptionUpdate` + apply changes; then `fetchSubscriptionPlan` via `natsLister{nc, subject.UserSubscriptionList(s.account, cfg.SiteID), 5s}` and open each room sub on `subject.RoomEvent(roomID, global)` → `handleDelivery(m, "channel", ...)`.
- `applyChanges([]subChange)`: subClose → unsubscribe + delete from `roomSubs`; subOpen → subscribe + pending limits + store.
- Resync in the `ReconnectHandler`: re-run `fetchSubscriptionPlan`, `diffPlans(old, new)`, apply — spawned on a goroutine guarded so only one resync runs at a time, exiting on ctx cancel.
- Proactive refresh loop inside `run(ctx)`: `timer := time.NewTimer(refreshDelay(expiresAt, time.Now(), rand.Float64))`; on fire → `refreshJWT` → `s.nc.ForceReconnect()` → re-arm from the new expiry; select against `ctx.Done()`. Expiry mode starts no timer.
- `run(ctx)` sequence: `primeJWT` → `connect` → subscribe lanes → walk → block on ctx/refresh loop; `m.ConnsConnecting.Inc()` when the handshake (mint + dial) starts and `.Dec()` when it resolves either way; `m.ConnsActive.Inc()` on connect, `.Dec()` + drain/close in `close()`.
- `refreshDelay(expiresAt, now, randFloat)`: `remaining := expiresAt.Sub(now); if remaining <= 0 { return 0 }; frac := 0.75 + 0.10*randFloat(); return time.Duration(float64(remaining) * frac)` — 80% ±5% of remaining life, mirroring `useJwtRefresh.js` REFRESH_FRACTION/REFRESH_JITTER.

- [ ] **Step 4: Run tests, verify pass**

Run: `make test SERVICE=tools/clientsim`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
make lint
git add tools/clientsim
git commit -m "feat(clientsim): simulated client lifecycle — cached JWT, proactive/expiry modes, live subscription maintenance"
```

---

### Task 12: swarm orchestrator + summary + main wiring

**Files:**
- Create: `tools/clientsim/swarm.go`, `tools/clientsim/swarm_test.go`
- Modify: `tools/clientsim/main.go` (replace the Task 5 stub tail of `run()`)

**Interfaces:**
- Consumes: `simClient` (Task 11), `metrics` (Task 6), config.
- Produces:

```go
type clientFactory func(account string) (runnable, error)
type runnable interface {
	run(ctx context.Context) error
	close()
}
func runSwarm(ctx context.Context, accounts []string, rampRate, churnRate float64, factory clientFactory, m *metrics) error
func printSummary(m *metrics) // gathers the registry, prints slog JSON summary; flags degraded windows
```

- [ ] **Step 1: Write the failing tests** (fake `runnable` records starts/stops; churn tested by observing restarts)

```go
// tools/clientsim/swarm_test.go
package main

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeClient struct {
	started atomic.Int64
	closed  atomic.Int64
}

func (f *fakeClient) run(ctx context.Context) error { f.started.Add(1); <-ctx.Done(); return nil }
func (f *fakeClient) close()                        { f.closed.Add(1) }

func TestRunSwarm_StartsEveryAccountAndStopsOnCancel(t *testing.T) {
	var mu sync.Mutex
	clients := map[string]*fakeClient{}
	factory := func(account string) (runnable, error) {
		mu.Lock()
		defer mu.Unlock()
		fc := &fakeClient{}
		clients[account] = fc
		return fc, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runSwarm(ctx, []string{"a", "b", "c"}, 1000, 0, factory, newMetrics()) }()

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(clients) == 3
	}, 5*time.Second, 10*time.Millisecond, "ramp should start all clients")

	cancel()
	require.NoError(t, <-done)
	mu.Lock()
	defer mu.Unlock()
	for account, fc := range clients {
		assert.Equal(t, int64(1), fc.closed.Load(), "client %s closed on shutdown", account)
	}
}

func TestRunSwarm_RampPacesStarts(t *testing.T) {
	var count atomic.Int64
	factory := func(string) (runnable, error) { count.Add(1); return &fakeClient{}, nil }

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	_ = runSwarm(ctx, []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}, 20, 0, factory, newMetrics())
	// 20 conns/sec for ~0.25s ≈ 5 starts; assert we did NOT start all 10 at once.
	assert.Less(t, count.Load(), int64(10), "ramp must pace, not thundering-herd")
	assert.Greater(t, count.Load(), int64(1))
}

func TestRunSwarm_FactoryErrorDoesNotAbortOthers(t *testing.T) {
	var ok atomic.Int64
	factory := func(account string) (runnable, error) {
		if account == "bad" {
			return nil, assert.AnError
		}
		ok.Add(1)
		return &fakeClient{}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = runSwarm(ctx, []string{"bad", "good1", "good2"}, 1000, 0, factory, newMetrics())
	assert.Equal(t, int64(2), ok.Load())
}
```

- [ ] **Step 2: Run tests, verify failure**

Run: `make test SERVICE=tools/clientsim`
Expected: FAIL — `undefined: runSwarm`

- [ ] **Step 3: Implement `swarm.go`**

Ramp: `time.Ticker` at `time.Duration(float64(time.Second)/rampRate)` (floor at 1ms), starting one client per tick, each in its own goroutine tracked by a `sync.WaitGroup`; a factory error logs and continues (a bad account must not kill the shard). Churn (when `churnRate > 0`): a second ticker at `1/churnRate` seconds; each tick picks a random running client, `close()`s it, and restarts it through the factory + ramp semantics. On `ctx.Done()`: stop tickers, `close()` every client, `wg.Wait()` with a 25s timeout (matching the repo's shutdown budget), then return. `printSummary(m)`: `m.Registry.Gather()`, log one slog record with delivered totals per lane, disconnect totals, histogram `count/sum` for the two latency series, and `degraded=true` when `DecodeFailures + InvalidTimestamp + SlowConsumer > 0`.

- [ ] **Step 4: Wire `main.go`**

Replace the stub tail of `run()`:

```go
	m := newMetrics()
	metricsSrv := &http.Server{Addr: cfg.MetricsAddr, Handler: m.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Warn("metrics server stopped", "error", err)
		}
	}()

	mintClient := newAuthClient(cfg.AuthURL, devProvider{}, m)
	factory := func(account string) (runnable, error) {
		return newSimClient(account, &cfg, mintClient, m)
	}

	err = runSwarm(ctx, shard, cfg.RampRate, cfg.ChurnRate, factory, m)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = metricsSrv.Shutdown(shutdownCtx)
	printSummary(m)
	return err
```

- [ ] **Step 5: Run all clientsim tests + build**

Run: `make test SERVICE=tools/clientsim && make lint`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add tools/clientsim
git commit -m "feat(clientsim): swarm orchestrator — paced ramp, churn, graceful shutdown, run summary"
```

---

### Task 13: integration test over ws-NATS

**Files:**
- Create: `tools/clientsim/integration_test.go`, `tools/clientsim/main_test.go`

**Interfaces:**
- Consumes: `testutil.NATSWebSocket` (Task 4), `simClient` (Task 11), subjects from `pkg/subject`, `model.RoomEvent`.

- [ ] **Step 1: Write `main_test.go`**

```go
//go:build integration

package main

import (
	"testing"

	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }
```

- [ ] **Step 2: Write the failing integration test**

```go
//go:build integration

package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
)

// fixedMinter satisfies minter without an auth-service: the test broker
// runs no auth, so any JWT-shaped string is accepted... except the broker
// ignores auth entirely, so we skip UserJWT by minting a decodable JWT.
type fixedMinter struct{ jwt string }

func (f fixedMinter) Mint(context.Context, string, string) (string, error) { return f.jwt, nil }

func TestSimClient_EndToEnd_WSSubscribeWalkAndCount(t *testing.T) {
	info := testutil.NATSWebSocket(t)
	const account, site = "user-0", "site-a"

	// Backend stub on the TCP side: answer subscription.list with one
	// channel room (global) across two pages to exercise pagination.
	backend, err := nats.Connect(info.TCPURL)
	require.NoError(t, err)
	t.Cleanup(backend.Close)

	pages := []subListPage{
		{Subscriptions: []subRow{{RoomID: "room-1", RoomType: "channel"}}, HasMore: true},
		{Subscriptions: []subRow{{RoomID: "dm-1", RoomType: "dm"}}, HasMore: false},
	}
	var call int
	_, err = backend.Subscribe(subject.UserSubscriptionList(account, site), func(msg *nats.Msg) {
		page := pages[min(call, len(pages)-1)]
		call++
		data, _ := json.Marshal(page)
		_ = msg.Respond(data)
	})
	require.NoError(t, err)

	cfg := config{
		NATSWSURL: info.WSURL, SiteID: site, JWTMode: "proactive",
		SubPendingMsgs: 512, SubPendingBytes: 1 << 20,
		ReconnectBufBytes: 1 << 16, PingInterval: 2 * time.Minute,
	}
	m := newMetrics()
	sc, err := newSimClient(account, &cfg, fixedMinter{jwt: mintTestJWT(t, time.Now().Add(time.Hour))}, m)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- sc.run(ctx) }()
	t.Cleanup(func() { cancel(); <-done; sc.close() })

	// Wait until the walk finished (the room sub exists) by publishing and
	// polling the delivered counter.
	evt := model.RoomEvent{Type: model.RoomEventNewMessage, RoomID: "room-1",
		Timestamp: time.Now().UTC().UnixMilli(), EventTimestamp: time.Now().UTC().Add(-20 * time.Millisecond).UnixMilli()}
	payload, err := json.Marshal(evt)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		_ = backend.Publish(subject.RoomEvent("room-1", true), payload)
		return promtestutil.ToFloat64(m.Delivered.WithLabelValues("channel")) >= 1
	}, 15*time.Second, 100*time.Millisecond, "channel fan-out must reach the ws client")

	// User lane delivery.
	require.Eventually(t, func() bool {
		_ = backend.Publish(subject.UserRoomEvent(account), payload)
		return promtestutil.ToFloat64(m.Delivered.WithLabelValues("user")) >= 1
	}, 15*time.Second, 100*time.Millisecond)

	// Live update: added channel room-2 (local namespace) starts receiving.
	upd := []byte(`{"action":"added","subscription":{"roomId":"room-2","roomType":"channel","room":{"crossSite":false}},"timestamp":1}`)
	require.NoError(t, backend.Publish(subject.SubscriptionUpdate(account), upd))
	evt2 := evt
	evt2.RoomID = "room-2"
	payload2, _ := json.Marshal(evt2)
	require.Eventually(t, func() bool {
		_ = backend.Publish(subject.RoomEvent("room-2", false), payload2)
		return promtestutil.ToFloat64(m.Delivered.WithLabelValues("channel")) >= 2
	}, 15*time.Second, 100*time.Millisecond, "live-added room must start receiving")

	assert.Equal(t, 2, call, "walk must paginate")
	assert.InDelta(t, 0, promtestutil.ToFloat64(m.DecodeFailures), 0.001)
}
```

Move `mintTestJWT` from `client_test.go` into a shared `_test.go` helper file if the build complains about the integration tag split (unit helpers live in `client_test.go` which is untagged, so integration files can't see it — put `mintTestJWT` in a new untagged `helpers_test.go`... note: untagged test files ARE compiled under the integration tag too, so `client_test.go` helpers are visible; only do the move if a name collision or tag issue actually appears).

Note the ws URL: the simClient connects to `info.WSURL` — nats.go v1.50.0 dials `ws://` URLs natively over the WebSocket transport.

- [ ] **Step 3: Run, verify it fails only for the right reason first** (optional red check: run before Task 11's walk wiring exists → if Tasks run in order this is green-path validation instead; acceptable — the unit tests carried red-green)

Run: `make test-integration SERVICE=tools/clientsim`
Expected: PASS (requires Docker)

- [ ] **Step 4: Commit**

```bash
make lint
git add tools/clientsim
git commit -m "test(clientsim): end-to-end integration over WebSocket NATS — walk, live update, delivery counting"
```

---

### Task 14: deploy assets + README

**Files:**
- Create: `tools/clientsim/deploy/Dockerfile`, `tools/clientsim/deploy/docker-compose.yml`, `tools/clientsim/deploy/azure-pipelines.yml`, `tools/clientsim/README.md`

**Interfaces:**
- Consumes: loadgen's deploy overlay as the template (`tools/loadgen/deploy/docker-compose.yml` — external `chat-local` network, build context `../../..`); auth-service's Dockerfile (`auth-service/deploy/Dockerfile`) for the side issuer; docker-local's generated env (`AUTH_SCOPED_SIGNING_KEY`, `AUTH_ACCOUNT_PUB_KEY` — exported by `docker-local/setup.sh:147-148`).

- [ ] **Step 1: Dockerfile** — copy loadgen's structure exactly (multi-stage `golang:1.25.13-alpine` → `alpine:3.21`, build context repo root, `go build ./tools/clientsim`). Read `tools/loadgen/deploy/Dockerfile` first and mirror it, changing only the package path and binary name.

- [ ] **Step 2: docker-compose.yml**

```yaml
# clientsim overlay — joins the docker-local stack as an external client.
# Bring up the base stack first (repo root: make up), then:
#   docker compose -f tools/clientsim/deploy/docker-compose.yml up -d
name: clientsim

services:
  auth-sideissuer:
    build:
      context: ../../..
      dockerfile: auth-service/deploy/Dockerfile
    environment:
      DEV_MODE: "true"
      DEV_MODE_ACCOUNT_PREFIX: "user-"
      AUTH_SCOPED_SIGNING_KEY: "${AUTH_SCOPED_SIGNING_KEY}"
      AUTH_ACCOUNT_PUB_KEY: "${AUTH_ACCOUNT_PUB_KEY}"
      PORT: "8080"
    networks: [chat-local]

  clientsim:
    build:
      context: ../../..
      dockerfile: tools/clientsim/deploy/Dockerfile
    entrypoint: ["sleep", "infinity"]   # exec the binary manually, loadgen-style
    environment:
      CLIENTSIM_NATS_WS_URL: "ws://chat-local-nats:9222"
      CLIENTSIM_AUTH_URL: "http://auth-sideissuer:8080"
      CLIENTSIM_POOL_FILE: "/var/lib/clientsim/pool.json"
      CLIENTSIM_SITE_ID: "site-a"
      CLIENTSIM_METRICS_ADDR: ":2112"
    ports:
      - "2113:2112"
    volumes:
      - clientsim-pool:/var/lib/clientsim
    networks: [chat-local]

networks:
  chat-local:
    external: true

volumes:
  clientsim-pool:
```

Then verify the interpolations against reality and fix: (a) the NATS container's DNS name on the `chat-local` network and its ws port (the loadgen overlay's prometheus config references `chat-local-nats:8222`, and docker-local publishes ws on 9222 — confirm the in-network ws port is 9222 from `docker-local/compose.deps.yaml`); (b) how `${AUTH_SCOPED_SIGNING_KEY}` reaches compose — check how `docker-local/compose.services.yaml` feeds auth-service those two variables (env file vs shell export) and mirror that mechanism (add `env_file:` if that's what the base stack uses); (c) the `SITE_ID` value docker-local actually uses (grep `SITE_ID` in `docker-local/`). Adjust the file, then `docker compose config` must parse cleanly.

- [ ] **Step 3: azure-pipelines.yml** — copy `tools/loadgen/deploy/azure-pipelines.yml` and substitute the service name/path. If loadgen has none, copy a service's (e.g. `auth-service/deploy/azure-pipelines.yml`) and adjust the Dockerfile path and image name.

- [ ] **Step 4: README.md** — quick start (seed with `--pool-out`, copy artifact into the volume, run `clientsim` inside the container), the config table from the spec §7, the k8s notes: StatefulSet ordinal → `CLIENTSIM_SHARD_INDEX` via downward API, side issuer isolation (ClusterIP + NetworkPolicy + allowlist from the pool artifact), `ulimit -n` and the ~60k port-tuple ceiling, and a pointer to the spec. State that k8s manifests live with the clusters' existing service manifests (ops-owned), not in this repo.

- [ ] **Step 5: Validate and commit**

Run: `docker compose -f tools/clientsim/deploy/docker-compose.yml config >/dev/null && make lint`
Expected: compose parses; lint clean.

```bash
git add tools/clientsim/deploy tools/clientsim/README.md
git commit -m "feat(clientsim): deploy overlay (side issuer + clientsim) and README"
```

---

## Execution notes

- **Order:** Tasks 1→2→3→4 are independent of each other except 2 needs 1; Tasks 5-12 are strictly ordered within clientsim (5, 6 before 7-12; 13 needs 4+11; 14 last). Parallel-safe pairs for subagents: {1,3,4} first wave; {2} after 1; {5} after 1; then 6..12 serial; 13, 14.
- **Verification gate before the final push:** `make lint && make test && make test-integration SERVICE=tools/clientsim && make test-integration SERVICE=pkg/roomkeysender && make sast`. SAST note: the dev-guard file read (`os.ReadFile` on a config-provided path) may trip gosec G304 — that path is operator-supplied config, suppress with `// #nosec G304 -- allowlist path comes from deployment config, not user input` on the line above if flagged.
- **Out of scope (per spec):** k8s manifests (ops repo), `fileProvider`, expected-deliveries export, any loadgen verdict integration. M1's real-cluster soak is an operational follow-up after this plan lands, not a task in it.
