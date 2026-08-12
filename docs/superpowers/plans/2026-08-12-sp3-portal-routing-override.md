# SP3-core: Portal Health-Aware Routing Override Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `portal-service` return the backup site's connection coordinates for a down site's SSO users, keyed on SP4's `servingTarget`, while keeping `siteId` = home.

**Architecture:** A TTL-cached `failoverReader` wraps SP4's `FailoverStore`; a `servingURLs` helper swaps the home registry entry for the reserved backup entry when `servingTarget == backup`; `resolve()` (the `/api/userInfo` path) calls it. Only `baseUrl`/`natsUrl` change — `siteId` stays home. Bot/admin `/api/v1/login` is untouched (deferred).

**Tech Stack:** Go 1.25, Gin, MongoDB (`mongo-driver/v2`), `caarlos0/env`, `go.uber.org/mock`, `stretchr/testify`.

**Spec:** `docs/superpowers/specs/2026-08-11-sp3-portal-routing-override.md`

## Global Constraints

- Build via `make`: `make test SERVICE=portal-service`, `make test-integration SERVICE=portal-service`, `make lint`, `make build SERVICE=portal-service`.
- Errors: infra `fmt.Errorf("...: %w", err)`; client-facing via `pkg/errcode` + `errhttp.Write`; `errors.Is` for comparison.
- Logging: `log/slog` JSON, structured fields, request-context-aware. Never log secrets.
- `json`+`bson` tags on model structs; DI = accept interfaces (define in consumer).
- **`FailoverState` is passed by pointer** (gocritic hugeParam); its `ServingTarget()` is a pointer-receiver method on an addressable value.
- TDD: Red → Green → Refactor → Commit. Tests in `package main`. Integration tests `//go:build integration`, reuse the `TestMain` in `portal-service/integration_test.go`.
- Coverage ≥80% floor; ≥90% on `servingURLs` + `failoverReader`.
- **This slice touches no `chat.user.*` RPC** — the `docs/client-api.md` edit is a courtesy narrative note, not a required RPC-doc change.

## File Structure

- **Create** `portal-service/failover_reader.go` — `failoverReader` (TTL cache over `FailoverStore`) + `newFailoverReader`.
- **Create** `portal-service/failover_reader_test.go` — unit tests (injected clock, `MockFailoverStore`).
- **Create** `portal-service/failover_reader_integration_test.go` — reader over real Mongo (`//go:build integration`).
- **Modify** `portal-service/handler.go` — add `failoverTargeter` interface, `failover`/`backupSiteID` fields + options, `servingURLs`, wire `resolve()`.
- **Modify** `portal-service/handler_test.go` — override tests (fake targeter).
- **Create** `portal-service/failover_userinfo_integration_test.go` — `/api/userInfo` over real Mongo failover state (`//go:build integration`).
- **Modify** `portal-service/main.go` — config (`FAILOVER_STATE_TTL`, `PORTAL_BACKUP_SITE_ID`), construct the store unconditionally, build the reader, pass options, reuse the store in the ops block.
- **Modify** `docs/client-api.md` — §2 site-discovery narrative note.

---

### Task 1: The TTL-cached failover reader

**Files:**
- Create: `portal-service/failover_reader.go`
- Test: `portal-service/failover_reader_test.go`, `portal-service/failover_reader_integration_test.go`

**Interfaces:**
- Consumes: `FailoverStore` (Get), `FailoverState.ServingTarget()`, `ServingHome`/`ServingBackup` (SP4). `MockFailoverStore` (generated).
- Produces: `func newFailoverReader(store FailoverStore, ttl time.Duration) *failoverReader`; method `ServingTarget(ctx context.Context, siteID string) ServingTarget`; overridable `now func() time.Time` field for tests.

- [ ] **Step 1: Write the failing unit tests**

Create `portal-service/failover_reader_test.go`:

```go
package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	gomock "go.uber.org/mock/gomock"
)

func TestFailoverReader_MissThenCacheHit(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockFailoverStore(ctrl)
	// Store is consulted exactly once; the second call is served from cache.
	store.EXPECT().Get(gomock.Any(), "site-a").
		Return(FailoverState{SiteID: "site-a", Status: StatusFailedOver, Version: 1}, nil).
		Times(1)

	r := newFailoverReader(store, time.Minute)
	now := time.UnixMilli(1000)
	r.now = func() time.Time { return now }

	assert.Equal(t, ServingBackup, r.ServingTarget(context.Background(), "site-a"))
	assert.Equal(t, ServingBackup, r.ServingTarget(context.Background(), "site-a"))
}

func TestFailoverReader_RefreshesAfterTTL(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockFailoverStore(ctrl)
	gomock.InOrder(
		store.EXPECT().Get(gomock.Any(), "site-a").
			Return(FailoverState{SiteID: "site-a", Status: StatusFailedOver, Version: 1}, nil),
		store.EXPECT().Get(gomock.Any(), "site-a").
			Return(FailoverState{SiteID: "site-a", Status: StatusHealthy, Version: 2}, nil),
	)

	r := newFailoverReader(store, 5*time.Second)
	now := time.UnixMilli(1000)
	r.now = func() time.Time { return now }

	assert.Equal(t, ServingBackup, r.ServingTarget(context.Background(), "site-a"))
	now = now.Add(6 * time.Second) // past TTL
	assert.Equal(t, ServingHome, r.ServingTarget(context.Background(), "site-a"))
}

func TestFailoverReader_StoreErrorFailsSafeHomeUncached(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockFailoverStore(ctrl)
	// Error path is not cached: both calls hit the store.
	store.EXPECT().Get(gomock.Any(), "site-a").Return(FailoverState{}, errors.New("mongo down")).Times(2)

	r := newFailoverReader(store, time.Minute)
	r.now = func() time.Time { return time.UnixMilli(1000) }

	assert.Equal(t, ServingHome, r.ServingTarget(context.Background(), "site-a"))
	assert.Equal(t, ServingHome, r.ServingTarget(context.Background(), "site-a"))
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=portal-service`
Expected: FAIL — `newFailoverReader` undefined.

- [ ] **Step 3: Write the reader**

Create `portal-service/failover_reader.go`:

```go
package main

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// failoverReader answers "where is this site served from right now?" from SP4's
// FailoverStore, behind a short TTL cache so routing does not hit Mongo on every
// login. Fail-safe: a store error resolves to home and is not cached.
type failoverReader struct {
	store FailoverStore
	ttl   time.Duration
	now   func() time.Time

	mu    sync.Mutex
	cache map[string]cachedTarget
}

type cachedTarget struct {
	target  ServingTarget
	expires time.Time
}

func newFailoverReader(store FailoverStore, ttl time.Duration) *failoverReader {
	return &failoverReader{
		store: store,
		ttl:   ttl,
		now:   time.Now,
		cache: make(map[string]cachedTarget),
	}
}

// ServingTarget returns home or backup for siteID. On a cache hit within the TTL
// it returns the cached value; otherwise it reads the store, derives the target,
// and caches it. A store error resolves to home (fail-safe) and is not cached.
func (r *failoverReader) ServingTarget(ctx context.Context, siteID string) ServingTarget {
	r.mu.Lock()
	if c, ok := r.cache[siteID]; ok && r.now().Before(c.expires) {
		r.mu.Unlock()
		return c.target
	}
	r.mu.Unlock()

	// Read outside the lock so a slow Mongo call does not serialize all readers.
	st, err := r.store.Get(ctx, siteID)
	if err != nil {
		slog.WarnContext(ctx, "failover reader: store read failed, defaulting to home",
			"siteId", siteID, "error", err)
		return ServingHome
	}
	target := st.ServingTarget()

	r.mu.Lock()
	r.cache[siteID] = cachedTarget{target: target, expires: r.now().Add(r.ttl)}
	r.mu.Unlock()
	return target
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=portal-service`
Expected: PASS.

- [ ] **Step 5: Write the integration test (compiles now; runs in CI)**

Create `portal-service/failover_reader_integration_test.go`:

```go
//go:build integration

package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/testutil"
)

func TestFailoverReader_OverMongo(t *testing.T) {
	db := testutil.MongoDB(t, "portal")
	store := newMongoFailoverStore(db)
	ctx := context.Background()

	r := newFailoverReader(store, time.Minute)

	// No document yet => healthy => home.
	assert.Equal(t, ServingHome, r.ServingTarget(ctx, "site-a"))

	// Fail the site over; a fresh reader (empty cache) sees backup.
	require.NoError(t, store.Transition(ctx, &FailoverState{SiteID: "site-a", Status: StatusFailedOver, Version: 1, Since: 1, Timestamp: 1}))
	r2 := newFailoverReader(store, time.Minute)
	assert.Equal(t, ServingBackup, r2.ServingTarget(ctx, "site-a"))
}
```

- [ ] **Step 6: Verify integration compiles**

Run: `go vet -tags integration ./portal-service/...`
Expected: no errors. (Execution needs Docker → CI.)

- [ ] **Step 7: Commit**

```bash
git add portal-service/failover_reader.go portal-service/failover_reader_test.go portal-service/failover_reader_integration_test.go
git commit -m "portal-service: TTL-cached failover reader over SP4 store"
```

---

### Task 2: The routing override (`servingURLs` + `resolve()`)

**Files:**
- Modify: `portal-service/handler.go`
- Test: `portal-service/handler_test.go`, `portal-service/failover_userinfo_integration_test.go`

**Interfaces:**
- Consumes: `failoverReader` (Task 1) via a `failoverTargeter` interface; `siteURL`, `PortalHandler`, `PortalHandlerOption` (existing).
- Produces:
  - `type failoverTargeter interface { ServingTarget(ctx context.Context, siteID string) ServingTarget }`
  - `PortalHandler` fields `failover failoverTargeter`, `backupSiteID string`.
  - `func WithFailoverReader(f failoverTargeter) PortalHandlerOption`, `func WithBackupSiteID(id string) PortalHandlerOption`.
  - `func (h *PortalHandler) servingURLs(ctx context.Context, siteID string) (siteURL, error)`.

- [ ] **Step 1: Write the failing tests**

Add to `portal-service/handler_test.go`:

```go
// fakeTargeter is a failoverTargeter stub returning a fixed target.
type fakeTargeter struct{ target ServingTarget }

func (f fakeTargeter) ServingTarget(_ context.Context, _ string) ServingTarget { return f.target }

// testSitesWithBackup extends testSites with a reserved backup entry.
var testSitesWithBackup = map[string]siteURL{
	"site-a":     {BaseURL: "https://site-a.example.com", NATSURL: "wss://nats-3.site-a.example.com"},
	"site-b":     {BaseURL: "https://site-b.example.com", NATSURL: "wss://nats.site-b.example.com"},
	"site-local": {BaseURL: "http://localhost:3000", NATSURL: "ws://localhost:9222"},
	"_backup":    {BaseURL: "https://backup.example.com", NATSURL: "wss://nats.backup.example.com"},
}

func newFailoverHandler(cache *directoryCache, target ServingTarget) *PortalHandler {
	return NewPortalHandler(cache, false, "site-local", "ws://localhost:9222", testSitesWithBackup, testSettings,
		WithFailoverReader(fakeTargeter{target: target}), WithBackupSiteID("_backup"))
}

func TestHandleUserInfo_FailoverRoutesToBackup(t *testing.T) {
	h := newFailoverHandler(cacheWith(alice), ServingBackup)
	w := getUserInfo(t, setupRouter(t, h), "alice")

	require.Equal(t, http.StatusOK, w.Code)
	var resp userInfoResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "https://backup.example.com", resp.BaseURL, "baseUrl swaps to backup")
	assert.Equal(t, "wss://nats.backup.example.com", resp.NATSURL, "natsUrl swaps to backup")
	assert.Equal(t, "site-a", resp.SiteID, "siteId MUST stay the home site")
	assert.Equal(t, "E001", resp.EmployeeID, "identity fields unchanged")
}

func TestHandleUserInfo_HealthyRoutesHome(t *testing.T) {
	h := newFailoverHandler(cacheWith(alice), ServingHome)
	w := getUserInfo(t, setupRouter(t, h), "alice")

	require.Equal(t, http.StatusOK, w.Code)
	var resp userInfoResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "https://site-a.example.com", resp.BaseURL)
	assert.Equal(t, "site-a", resp.SiteID)
}

func TestHandleUserInfo_BackupMissingFromRegistryIsInternal(t *testing.T) {
	// servingTarget=backup but no backup entry configured => loud 500.
	h := NewPortalHandler(cacheWith(alice), false, "site-local", "ws://localhost:9222", testSites, testSettings,
		WithFailoverReader(fakeTargeter{target: ServingBackup}), WithBackupSiteID("_backup"))
	w := getUserInfo(t, setupRouter(t, h), "alice")
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestHandleUserInfo_NoFailoverConfiguredRoutesHome(t *testing.T) {
	// newTestHandler sets no failover reader; the override must no-op to home.
	h := newTestHandler(cacheWith(alice), false)
	w := getUserInfo(t, setupRouter(t, h), "alice")
	require.Equal(t, http.StatusOK, w.Code)
	var resp userInfoResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "https://site-a.example.com", resp.BaseURL)
	assert.Equal(t, "site-a", resp.SiteID)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=portal-service`
Expected: FAIL — `WithFailoverReader`, `WithBackupSiteID` undefined.

- [ ] **Step 3: Add the interface, fields, options, and `servingURLs`**

In `portal-service/handler.go`, add near the `PortalHandler` type:

```go
// failoverTargeter reads SP4's per-site serving target. *failoverReader
// implements it; defined here (the consumer) so tests can inject a fake.
type failoverTargeter interface {
	ServingTarget(ctx context.Context, siteID string) ServingTarget
}
```

Add two fields to the `PortalHandler` struct:

```go
	// failover resolves per-site failover routing (SP4). nil => always home.
	failover failoverTargeter
	// backupSiteID is the reserved PORTAL_SITE_URLS entry served during failover.
	backupSiteID string
```

Add two options next to the existing `With*` options:

```go
// WithFailoverReader injects the SP4 failover target reader that drives the
// health-aware routing override. Without it, routing is always home.
func WithFailoverReader(f failoverTargeter) PortalHandlerOption {
	return func(h *PortalHandler) { h.failover = f }
}

// WithBackupSiteID sets the reserved PORTAL_SITE_URLS id served for a
// failed-over site (PORTAL_BACKUP_SITE_ID).
func WithBackupSiteID(id string) PortalHandlerOption {
	return func(h *PortalHandler) { h.backupSiteID = id }
}
```

Add the helper:

```go
// servingURLs returns the coordinates to hand a client for an account homed on
// siteID, applying SP4's failover override. siteID (the home site) is never
// changed by this — only which registry entry's URLs are returned. A configured
// backup that is missing from the registry is a loud internal error rather than
// a silent mis-route to the down home site.
func (h *PortalHandler) servingURLs(ctx context.Context, siteID string) (siteURL, error) {
	if h.failover != nil && h.failover.ServingTarget(ctx, siteID) == ServingBackup {
		b, ok := h.sites[h.backupSiteID]
		if !ok {
			return siteURL{}, fmt.Errorf("serving target is backup but backup site %q missing from registry", h.backupSiteID)
		}
		return b, nil
	}
	home, ok := h.sites[siteID]
	if !ok {
		return siteURL{}, fmt.Errorf("no URLs configured for siteId %q", siteID)
	}
	return home, nil
}
```

- [ ] **Step 4: Wire `resolve()` to use it**

In `resolve()`, replace this block:

```go
	site, siteOK := h.sites[e.SiteID]
	if !siteOK && !devFallback {
		// A directory entry homed on a site missing from the registry is an ops
		// misconfiguration, not a client error — surface it as internal.
		errhttp.Write(ctx, c, fmt.Errorf("no URLs configured for siteId %q", e.SiteID))
		return
	}
	natsURL := site.NATSURL
	if !siteOK {
		// The dev-fallback site itself isn't in the registry — fall back to
		// the legacy PORTAL_DEV_FALLBACK_NATS_URL so local logins keep working.
		natsURL = h.devFallbackNatsURL
	}
```

with:

```go
	var baseURL, natsURL string
	if devFallback {
		// Dev-fallback site never fails over; keep the legacy URL handling.
		site, siteOK := h.sites[e.SiteID]
		baseURL, natsURL = site.BaseURL, site.NATSURL
		if !siteOK {
			natsURL = h.devFallbackNatsURL // baseURL stays "" as before
		}
	} else {
		su, err := h.servingURLs(ctx, e.SiteID)
		if err != nil {
			errhttp.Write(ctx, c, err) // raw wrapped => internal at the boundary
			return
		}
		baseURL, natsURL = su.BaseURL, su.NATSURL
	}
```

Then in the two `c.JSON(...)` response builders below, replace `BaseURL: site.BaseURL` with `BaseURL: baseURL` (the `NATSURL: natsURL` line is already correct). Both the `userInfoBotResponse` and `userInfoResponse` builders use `baseURL`.

- [ ] **Step 5: Write the `/api/userInfo`-over-Mongo integration test**

Create `portal-service/failover_userinfo_integration_test.go`:

```go
//go:build integration

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/testutil"
)

func TestUserInfo_FailoverOverMongo(t *testing.T) {
	db := testutil.MongoDB(t, "portal")
	store := newMongoFailoverStore(db)
	ctx := context.Background()

	// Short TTL so the flip-back is observable within the test.
	reader := newFailoverReader(store, 20*time.Millisecond)
	h := NewPortalHandler(cacheWith(alice), false, "site-local", "ws://localhost:9222",
		testSitesWithBackup, testSettings,
		WithFailoverReader(reader), WithBackupSiteID("_backup"))
	r := setupRouter(t, h)

	// Fail site-a over -> userInfo returns backup coords, home siteId.
	require.NoError(t, store.Transition(ctx, &FailoverState{SiteID: "site-a", Status: StatusFailedOver, Version: 1, Since: 1, Timestamp: 1}))
	var resp userInfoResponse
	require.NoError(t, json.Unmarshal(getUserInfo(t, r, "alice").Body.Bytes(), &resp))
	assert.Equal(t, http.StatusOK, getUserInfo(t, r, "alice").Code)
	assert.Equal(t, "https://backup.example.com", resp.BaseURL)
	assert.Equal(t, "site-a", resp.SiteID)
}
```

- [ ] **Step 6: Run unit tests and verify the integration compiles**

Run: `make test SERVICE=portal-service`
Expected: PASS (new override tests + all existing handler tests still green).

Run: `go vet -tags integration ./portal-service/...`
Expected: no errors.

- [ ] **Step 7: Commit**

```bash
git add portal-service/handler.go portal-service/handler_test.go portal-service/failover_userinfo_integration_test.go
git commit -m "portal-service: health-aware routing override on /api/userInfo"
```

---

### Task 3: Wire the reader into `main.go`

**Files:**
- Modify: `portal-service/main.go`

**Interfaces:**
- Consumes: `newFailoverReader` (Task 1), `WithFailoverReader`/`WithBackupSiteID` (Task 2), `newMongoFailoverStore` (SP4).

- [ ] **Step 1: Add config fields**

In the `config` struct, next to the SP4 failover fields, add:

```go
	// FailoverStateTTL bounds how long portal caches a site's serving target
	// before re-reading it (routing freshness vs. Mongo load).
	FailoverStateTTL time.Duration `env:"FAILOVER_STATE_TTL" envDefault:"5s"`
	// BackupSiteID is the reserved PORTAL_SITE_URLS id served for a failed-over
	// site. Empty in single-site/dev deployments (no failover occurs there).
	BackupSiteID string `env:"PORTAL_BACKUP_SITE_ID" envDefault:""`
```

- [ ] **Step 2: Construct the store + reader before the handler, and pass the options**

The failover store must exist unconditionally (routing needs it even when the control surface is off). Immediately before the `handler := NewPortalHandler(...)` call, add:

```go
	failoverStore := newMongoFailoverStore(mongoClient.Database(cfg.MongoDB))
	failoverReader := newFailoverReader(failoverStore, cfg.FailoverStateTTL)
```

Then extend the `NewPortalHandler(...)` call's option list with:

```go
		WithFailoverReader(failoverReader), WithBackupSiteID(cfg.BackupSiteID),
```

- [ ] **Step 3: Reuse the store in the SP4 ops block**

In the `if cfg.FailoverOpsToken != ""` block, delete the line that re-creates the store:

```go
		failoverStore := newMongoFailoverStore(mongoClient.Database(cfg.MongoDB))
```

so the block uses the `failoverStore` built in Step 2 (it is already in scope). The `NewFailoverHandler(failoverStore)` call is unchanged.

- [ ] **Step 4: Verify build, lint, tests**

Run: `make build SERVICE=portal-service`
Expected: builds clean (no "declared and not used", no shadowing).

Run: `make lint`
Expected: 0 issues.

Run: `make test SERVICE=portal-service`
Expected: PASS.

- [ ] **Step 5: Add local-dev backup coordinates (compose)**

In `portal-service/deploy/docker-compose.yml`, extend `PORTAL_SITE_URLS` with a `_backup` entry and set `PORTAL_BACKUP_SITE_ID`. Replace the existing `PORTAL_SITE_URLS` line and add the id line in the `environment:` list:

```yaml
      - 'PORTAL_SITE_URLS={"site-local":{"baseUrl":"http://localhost:7777","natsUrl":"ws://localhost:9222"},"_backup":{"baseUrl":"http://localhost:7777","natsUrl":"ws://localhost:9222"}}'
      - PORTAL_BACKUP_SITE_ID=_backup
```

(The dev backup points at the same local stack — there is no separate backup site locally; this just makes the config valid and exercisable.)

- [ ] **Step 6: Commit**

```bash
git add portal-service/main.go portal-service/deploy/docker-compose.yml
git commit -m "portal-service: wire failover reader into routing + config"
```

---

### Task 4: Client-API doc note

**Files:**
- Modify: `docs/client-api.md`

- [ ] **Step 1: Add the site-discovery note**

Find the §2 site-discovery / `GET /api/userInfo` subsection. Add a short note (matching the doc's prose style) after the response description:

```markdown
> **Failover routing.** During a home-site outage, `baseUrl` and `natsUrl` may
> point at the shared backup site while `siteId` stays the account's home site
> (data on the backup is namespaced by the origin `siteId`). Clients need no
> special handling: the existing reconnect-on-connection-failure path re-queries
> this endpoint and receives the current coordinates, in both the failover and
> failback directions.
```

- [ ] **Step 2: Commit**

```bash
git add docs/client-api.md
git commit -m "docs(client-api): note failover routing on /api/userInfo"
```

---

## Self-Review

**1. Spec coverage:**
- §2 thin override at resolve seam → Task 2 (`servingURLs` + `resolve()`). ✓
- §2.1 one helper → Task 2 `servingURLs`. ✓
- §2.2 scope userInfo only, bots untouched → Task 2 modifies only `resolve()`; `HandleLogin` unchanged. ✓
- §3 swaps URLs, `siteId` stays home → Task 2 keeps `SiteID: e.SiteID`, test `TestHandleUserInfo_FailoverRoutesToBackup` asserts it. ✓
- §4 reserved backup entry / `PORTAL_BACKUP_SITE_ID` → Task 3 config + Task 2 `servingURLs` lookup. ✓
- §5 client reconnect (no SP3 code) → nothing to implement; doc note Task 4. ✓
- §6 failoverReader TTL + fail-safe home → Task 1. ✓
- §6.1 main.go refactor (store unconditional) → Task 3. ✓
- §7 split-brain / fail-safe home → Task 1 (error→home, uncached) + Task 2 (nil failover→home). ✓
- §8 testing → Task 1 unit+integration, Task 2 unit+integration. ✓
- §9 client-api note → Task 4. ✓

**2. Placeholder scan:** No TBD/TODO; every step has code or an exact command. ✓

**3. Type consistency:** `failoverReader`/`newFailoverReader`/`ServingTarget`, `failoverTargeter`, `servingURLs`, `WithFailoverReader`/`WithBackupSiteID`, `backupSiteID`, `FailoverStore`/`FailoverState`/`ServingHome`/`ServingBackup` — consistent across Tasks 1→3. `FailoverState` passed by pointer in `store.Transition(ctx, &FailoverState{...})` per SP4. ✓

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-08-12-sp3-portal-routing-override.md`.
