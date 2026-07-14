# Portal Service (Site Discovery + Provisioning Gate) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **⚠️ Partially superseded (2026-06-12).** This plan is the executed working
> record; the portal's data model changed during implementation review, and the
> spec's [Amendments](../specs/2026-06-11-portal-service-design.md#amendments-2026-06-12)
> override it. As shipped: the directory is the HR-owned `hr_employee`
> collection served from an in-memory cache (`ListEmployees`-only store — no
> portal `users`/`sites` collections, no `EnsureIndexes`); `authServiceUrl` is
> derived from `PORTAL_AUTH_URL_TEMPLATE` (plus `PORTAL_CACHE_REFRESH_INTERVAL`
> config); a portal miss returns `account_not_ready` while
> `account_not_provisioned` is emitted only by the auth-service minting gate;
> `GET /readyz` exists alongside `/healthz`; and the dev fallback is synthesized
> (no seeded site record). Identity was also hardened during review:
> `Claims.Account()` trusts **only** `preferred_username` — Task 2's `name`
> fallback snippets are obsolete and must not be implemented. Do not implement
> from this plan without reading the amendments first.

**Goal:** Build `portal-service` (post-Keycloak site discovery: account → `{account, employeeId, authServiceUrl, natsUrl, siteId}`), add an enforced provisioning gate to auth-service, and rewire chat-frontend so no user ever types a Site ID.

**Architecture:** Discovery directory — portal validates the SSO token, reads a portal-owned Mongo directory (`users` + `sites`), and returns connection coordinates; the frontend then calls the resolved auth-service directly (refresh included) and dials the resolved NATS. Enforcement lives in auth-service: mint only when `{account, siteId: SITE_ID}` exists in the site's `users` collection. Shared middleware moves to `pkg/ginutil`; account derivation becomes `pkg/oidc Claims.Account()`.

**Tech Stack:** Go 1.25, Gin, mongo-driver v2, `pkg/errcode` + `errhttp`, mockgen (`go.uber.org/mock`), testify, testcontainers via `pkg/testutil`; React + vitest on the frontend.

**Spec:** `docs/superpowers/specs/2026-06-11-portal-service-design.md`
**Branch:** `claude/eager-einstein-7u2je6` (already checked out; never push elsewhere)

**Conventions that apply to every task:**
- Always use `make` targets, never raw `go` commands — except the coverage commands CLAUDE.md itself prescribes and one-package test runs during red/green, where `go test ./<pkg>/...` keeps the loop fast (the Makefile's `test` target is the same command repo-wide).
- TDD: write the failing test, run it, watch it fail, implement, watch it pass, commit.
- If `mockgen` is missing: `go install go.uber.org/mock/mockgen@$(go list -m -f '{{.Version}}' go.uber.org/mock)`.
- Integration tests need Docker. If the execution environment lacks Docker, still write the tests, verify they compile with `go vet -tags integration ./<service>/...`, note it in the commit, and rely on CI's integration job.
- Commit messages must NOT mention AI/session/model identifiers.

---

## Implementation Tasks

### Task 1: `account_not_provisioned` reason in `pkg/errcode`

**Files:**
- Create: `pkg/errcode/codes_portal.go`
- Modify: `pkg/errcode/codes_test.go:8-20` (the `allReasons` slice)

- [ ] **Step 1: Write the failing test (compile-red)**

In `pkg/errcode/codes_test.go`, add `PortalAccountNotProvisioned` to the `allReasons` slice, after the `AuthTokenExpired, ...` line:

```go
	AuthTokenExpired, AuthInvalidToken, AuthInvalidRequest, AuthInvalidNKey, AuthMissingFields,
	PortalAccountNotProvisioned,
	RequestIDRequired,
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./pkg/errcode/...`
Expected: FAIL — `undefined: PortalAccountNotProvisioned`

- [ ] **Step 3: Write the implementation**

Create `pkg/errcode/codes_portal.go`:

```go
package errcode

// Reasons emitted by portal-service and the auth-service minting gate.
const (
	// PortalAccountNotProvisioned: the account authenticated successfully but
	// is not provisioned in the chat directory (portal lookup) or not
	// provisioned for the target site (auth-service minting gate).
	PortalAccountNotProvisioned Reason = "account_not_provisioned"
)
```

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./pkg/errcode/...`
Expected: PASS (snake-case + uniqueness tests cover the new constant)

- [ ] **Step 5: Commit**

```bash
git add pkg/errcode/codes_portal.go pkg/errcode/codes_test.go
git commit -m "feat(errcode): add account_not_provisioned reason for portal/auth provisioning"
```

---

### Task 2: `Claims.Account()` in `pkg/oidc`

**Files:**
- Modify: `pkg/oidc/oidc.go` (add method after the `Claims` struct, ~line 28)
- Modify: `pkg/oidc/oidc_test.go` (append test)

- [ ] **Step 1: Write the failing test**

Append to `pkg/oidc/oidc_test.go`:

```go
func TestClaims_Account(t *testing.T) {
	tests := []struct {
		name   string
		claims Claims
		want   string
	}{
		{"preferred_username wins", Claims{PreferredUsername: "alice", Name: "Alice W"}, "alice"},
		{"falls back to name", Claims{Name: "alice"}, "alice"},
		{"both blank is blank", Claims{}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.claims.Account(); got != tt.want {
				t.Errorf("Account() = %q, want %q", got, tt.want)
			}
		})
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./pkg/oidc/...`
Expected: FAIL — `tt.claims.Account undefined`

- [ ] **Step 3: Implement**

In `pkg/oidc/oidc.go`, directly below the `Claims` struct definition:

```go
// Account returns the chat account carried by the claims:
// preferred_username, falling back to name. An empty result means the token
// carries no usable account and the caller must reject it.
func (c Claims) Account() string {
	if c.PreferredUsername != "" {
		return c.PreferredUsername
	}
	return c.Name
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./pkg/oidc/...`
Expected: PASS

- [ ] **Step 5: Use it in auth-service (keep behavior identical)**

In `auth-service/handler.go:152-155`, replace:

```go
	account := claims.PreferredUsername
	if account == "" {
		account = claims.Name
	}
```

with:

```go
	account := claims.Account()
```

- [ ] **Step 6: Run auth-service tests**

Run: `go test ./auth-service/...`
Expected: PASS (behavior unchanged; `TestHandleAuth_ValidToken` et al. still green)

- [ ] **Step 7: Commit**

```bash
git add pkg/oidc/oidc.go pkg/oidc/oidc_test.go auth-service/handler.go
git commit -m "feat(oidc): add Claims.Account() and use it in auth-service"
```

---

### Task 3: extract Gin middleware into `pkg/ginutil`

**Files:**
- Create: `pkg/ginutil/middleware.go` (content = `auth-service/middleware.go` with exported names)
- Create: `pkg/ginutil/middleware_test.go` (content = `auth-service/middleware_test.go`, renamed calls)
- Modify: `auth-service/main.go:80-82`
- Delete: `auth-service/middleware.go`, `auth-service/middleware_test.go`

- [ ] **Step 1: Write the failing tests (move the test file first)**

Create `pkg/ginutil/middleware_test.go`: copy `auth-service/middleware_test.go` verbatim, then change the package line to `package ginutil` and rename every middleware constructor call: `requestIDMiddleware()` → `RequestID()` (4 occurrences), `accessLogMiddleware()` → `AccessLog()` (1), `corsMiddleware()` → `CORS()` (2). Rename the test functions `TestRequestIDMiddleware_*` → `TestRequestID_*`, `TestAccessLogMiddleware_*` → `TestAccessLog_*`, `TestCorsMiddleware_*` → `TestCORS_*` (same bodies).

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./pkg/ginutil/...`
Expected: FAIL — `undefined: RequestID` (package has tests but no source)

- [ ] **Step 3: Implement `pkg/ginutil/middleware.go`**

Copy `auth-service/middleware.go` and adapt the header + names:

```go
// Package ginutil holds the Gin middleware shared by the HTTP services
// (auth-service, portal-service): request-ID propagation, JSON access
// logging, and the wildcard-CORS policy for browser-called endpoints.
package ginutil

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/natsutil"
)

// RequestID funnels HTTP X-Request-ID through idgen.ResolveRequestID
// (the same primitive the NATS path uses via natsutil.StampRequestID) so the
// mint-vs-pass-through policy has a single owner. Missing → silent mint;
// malformed → mint + Warn preserving the inbound value for traceability.
func RequestID() gin.HandlerFunc {
	return func(c *gin.Context) {
		inbound := c.GetHeader(natsutil.RequestIDHeader)
		id, replaced := idgen.ResolveRequestID(inbound)
		c.Set("request_id", id)
		c.Request = c.Request.WithContext(natsutil.WithRequestID(c.Request.Context(), id))
		c.Header(natsutil.RequestIDHeader, id)
		if replaced {
			slog.WarnContext(c.Request.Context(), "minted request_id (inbound invalid)", "inbound", inbound, "path", c.Request.URL.Path)
		}
		c.Next()
	}
}

// CORS allows browser clients from any origin to call the API and
// short-circuits the preflight OPTIONS request with 204. The wildcard origin
// is incompatible with credentialed requests (cookies / Authorization), but
// these endpoints use neither — they accept a JSON body and return a JWT or
// connection coordinates.
func CORS() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Request-ID")
		c.Header("Access-Control-Max-Age", "300")
		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	}
}

// AccessLog logs method, path, status, and latency for each request.
func AccessLog() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()

		slog.Info("request",
			"request_id", c.GetString("request_id"),
			"method", c.Request.Method,
			"path", c.Request.URL.Path,
			"status", c.Writer.Status(),
			"latency_ms", time.Since(start).Milliseconds(),
			"client_ip", c.ClientIP(),
		)
	}
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./pkg/ginutil/...`
Expected: PASS (all 7 moved tests)

- [ ] **Step 5: Rewire auth-service and delete the originals**

In `auth-service/main.go`: add `"github.com/hmchangw/chat/pkg/ginutil"` to imports, replace lines 80-82:

```go
	r.Use(ginutil.RequestID())
	r.Use(ginutil.AccessLog())
	r.Use(ginutil.CORS())
```

Then: `git rm auth-service/middleware.go auth-service/middleware_test.go`

- [ ] **Step 6: Verify auth-service still builds and passes**

Run: `go test ./auth-service/... && make build SERVICE=auth-service`
Expected: PASS, clean build

- [ ] **Step 7: Commit**

```bash
git add pkg/ginutil auth-service/main.go
git commit -m "refactor: extract shared Gin middleware into pkg/ginutil"
```

---

### Task 4: auth-service provisioning gate

**Files:**
- Create: `auth-service/store.go`, `auth-service/store_mongo.go`
- Generate: `auth-service/mock_store_test.go`
- Modify: `auth-service/handler.go` (struct, option, gate in `HandleAuth`)
- Modify: `auth-service/main.go` (config + wiring + shutdown)
- Modify: `auth-service/handler_test.go` (new tests), `auth-service/integration_test.go` (TestMain + store test)
- Modify: `auth-service/deploy/docker-compose.yml` (gate env)

- [ ] **Step 1: Define the store interface (scaffolding for the mock)**

Create `auth-service/store.go`:

```go
package main

import "context"

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

// ProvisionStore answers whether an account is provisioned for this site.
type ProvisionStore interface {
	// AccountProvisioned reports whether the account exists in this site's
	// users collection homed on siteID. The compound predicate matters: the
	// per-site users collection also holds other sites' users (for
	// cross-site rooms), so existence alone is not provisioning.
	AccountProvisioned(ctx context.Context, account, siteID string) (bool, error)
}
```

- [ ] **Step 2: Generate the mock**

Run: `make generate SERVICE=auth-service`
Expected: `auth-service/mock_store_test.go` created (contains `MockProvisionStore`)

- [ ] **Step 3: Write the failing handler tests**

Append to `auth-service/handler_test.go` (also add `"errors"` and `"go.uber.org/mock/gomock"` to its imports):

```go
func TestHandleAuth_ProvisionGate(t *testing.T) {
	signingKP := mustAccountKP(t)

	tests := []struct {
		name        string
		account     string
		provisioned bool
		storeErr    error
		wantStatus  int
		wantReason  errcode.Reason
	}{
		{"provisioned account mints", "alice", true, nil, http.StatusOK, ""},
		{"unprovisioned account refused", "mallory", false, nil, http.StatusForbidden, errcode.PortalAccountNotProvisioned},
		// ivan exists in the users collection but is homed on site-b; this
		// auth-service runs site-a — the compound predicate refuses him.
		{"wrong-site account refused", "ivan", false, nil, http.StatusForbidden, errcode.PortalAccountNotProvisioned},
		{"store error fails closed", "alice", false, errors.New("mongo down"), http.StatusInternalServerError, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockProvisionStore(ctrl)
			store.EXPECT().AccountProvisioned(gomock.Any(), tt.account, "site-a").
				Return(tt.provisioned, tt.storeErr)

			validator := &fakeValidator{account: tt.account, subject: "uuid-" + tt.account}
			handler := NewAuthHandler(validator, signingKP, 2*time.Hour, false,
				WithProvisionGate(store, "site-a"))
			router := setupRouter(t, handler)

			userPub := mustUserNKey(t)
			body := `{"ssoToken":"valid-token","natsPublicKey":"` + userPub + `"}`
			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/auth", strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			router.ServeHTTP(w, req)

			assert.Equal(t, tt.wantStatus, w.Code)
			if tt.wantReason != "" {
				errtest.AssertCode(t, w.Body.Bytes(), errcode.CodeForbidden)
				errtest.AssertReason(t, w.Body.Bytes(), tt.wantReason)
			}
		})
	}
}

func TestHandleAuth_ProvisionGate_SkippedInDevMode(t *testing.T) {
	signingKP := mustAccountKP(t)
	ctrl := gomock.NewController(t)
	store := NewMockProvisionStore(ctrl) // no EXPECT — any call fails the test

	handler := NewAuthHandler(nil, signingKP, 2*time.Hour, true,
		WithProvisionGate(store, "site-a"))
	router := setupRouter(t, handler)

	userPub := mustUserNKey(t)
	body := `{"account":"anyone","natsPublicKey":"` + userPub + `"}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/auth", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}
```

- [ ] **Step 4: Run to verify they fail**

Run: `go test ./auth-service/...`
Expected: FAIL — `undefined: WithProvisionGate`

- [ ] **Step 5: Implement the gate in `auth-service/handler.go`**

Add fields to `AuthHandler` (after `devMode bool`):

```go
	store  ProvisionStore // nil = gate disabled (dev mode or REQUIRE_PROVISIONED=false)
	siteID string
```

Add the option (after `WithRandFloat`):

```go
// WithProvisionGate enables the minting gate: an account must exist in this
// site's users collection (account + siteID compound predicate) before a JWT
// is signed. Dev mode never consults the gate.
func WithProvisionGate(store ProvisionStore, siteID string) Option {
	return func(h *AuthHandler) {
		h.store = store
		h.siteID = siteID
	}
}
```

In `HandleAuth`, after `ctx = errcode.WithLogValues(ctx, "account", account)` and before `natsJWT, err := h.signNATSJWT(...)`:

```go
	if h.store != nil {
		provisioned, err := h.store.AccountProvisioned(ctx, account, h.siteID)
		if err != nil {
			errhttp.Write(ctx, c, fmt.Errorf("check account provisioning: %w", err))
			return
		}
		if !provisioned {
			errhttp.Write(ctx, c, errcode.Forbidden("account not provisioned for this site",
				errcode.WithReason(errcode.PortalAccountNotProvisioned)))
			return
		}
	}
```

(`handleDevAuth` is untouched — dev mode never reaches the gate.)

- [ ] **Step 6: Run to verify they pass**

Run: `go test ./auth-service/...`
Expected: PASS (new + all pre-existing tests)

- [ ] **Step 7: Implement the Mongo store**

Create `auth-service/store_mongo.go`:

```go
package main

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type mongoProvisionStore struct {
	users *mongo.Collection
}

func newMongoProvisionStore(db *mongo.Database) *mongoProvisionStore {
	return &mongoProvisionStore{users: db.Collection("users")}
}

// AccountProvisioned checks the compound {account, siteId} predicate against
// the site's users collection. Projection keeps the read index-only-ish; a
// missing document is a clean false, anything else is an infra error.
func (s *mongoProvisionStore) AccountProvisioned(ctx context.Context, account, siteID string) (bool, error) {
	err := s.users.FindOne(ctx,
		bson.M{"account": account, "siteId": siteID},
		options.FindOne().SetProjection(bson.M{"_id": 1}),
	).Err()
	if errors.Is(err, mongo.ErrNoDocuments) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("query users for provisioning: %w", err)
	}
	return true, nil
}
```

- [ ] **Step 8: Write the integration test**

In `auth-service/integration_test.go`, add imports (`context`, `bson`, `testutil`) and append:

```go
func TestMain(m *testing.M) { testutil.RunTests(m) }

func TestMongoProvisionStore_AccountProvisioned(t *testing.T) {
	db := testutil.MongoDB(t, "authsvc")
	store := newMongoProvisionStore(db)
	ctx := context.Background()

	_, err := db.Collection("users").InsertMany(ctx, []any{
		bson.M{"_id": "u-alice", "account": "alice", "siteId": "site-a"},
		bson.M{"_id": "u-ivan", "account": "ivan", "siteId": "site-b"},
	})
	require.NoError(t, err)

	tests := []struct {
		name    string
		account string
		siteID  string
		want    bool
	}{
		{"provisioned on this site", "alice", "site-a", true},
		{"homed on another site", "ivan", "site-a", false},
		{"unknown account", "carol", "site-a", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := store.AccountProvisioned(ctx, tt.account, tt.siteID)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
```

Import block addition for that file:

```go
	"context"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/testutil"
```

- [ ] **Step 9: Run the integration test**

Run: `make test-integration SERVICE=auth-service`
Expected: PASS (needs Docker; see Conventions if unavailable)

- [ ] **Step 10: Wire config in `auth-service/main.go`**

Add to the `config` struct:

```go
	// Provisioning gate — see "Enforced provisioning" in the design spec.
	SiteID             string `env:"SITE_ID"`
	RequireProvisioned bool   `env:"REQUIRE_PROVISIONED" envDefault:"true"`
	MongoURI           string `env:"MONGO_URI"`
	MongoDB            string `env:"MONGO_DB"            envDefault:"chat"`
	MongoUsername      string `env:"MONGO_USERNAME"      envDefault:""`
	MongoPassword      string `env:"MONGO_PASSWORD"      envDefault:""`
```

In `run()`, build the options list and gate wiring. Replace the two `NewAuthHandler(...)` calls so both use a shared `opts` slice, and connect Mongo only when the gate is active:

```go
	opts := []Option{WithJitter(cfg.NATSJWTExpiryJitter)}

	var mongoClient *mongo.Client
	if !cfg.DevMode && cfg.RequireProvisioned {
		if cfg.MongoURI == "" || cfg.SiteID == "" {
			return fmt.Errorf("MONGO_URI and SITE_ID are required when REQUIRE_PROVISIONED is true (set REQUIRE_PROVISIONED=false to defer the gate)")
		}
		mongoClient, err = mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword)
		if err != nil {
			return fmt.Errorf("connect mongo for provisioning gate: %w", err)
		}
		opts = append(opts, WithProvisionGate(newMongoProvisionStore(mongoClient.Database(cfg.MongoDB)), cfg.SiteID))
	}
```

Dev branch becomes `NewAuthHandler(nil, signingKP, cfg.NATSJWTExpiry, true, opts...)`; prod branch `NewAuthHandler(oidcValidator, signingKP, cfg.NATSJWTExpiry, false, opts...)`. Imports to add: `"go.mongodb.org/mongo-driver/v2/mongo"`, `"github.com/hmchangw/chat/pkg/mongoutil"`. In the `shutdown.Wait` closure, after `srv.Shutdown(ctx)` (HTTP first, then DBs):

```go
			err := srv.Shutdown(ctx)
			if mongoClient != nil {
				mongoutil.Disconnect(ctx, mongoClient)
			}
			return err
```

- [ ] **Step 11: Update `auth-service/deploy/docker-compose.yml`**

Add to the `environment:` list (keeps the documented "flip DEV_MODE to false" path working):

```yaml
      # Provisioning gate (active only when DEV_MODE=false).
      - SITE_ID=site-local
      - REQUIRE_PROVISIONED=${REQUIRE_PROVISIONED:-true}
      - MONGO_URI=mongodb://mongodb:27017
      - MONGO_DB=chat
```

- [ ] **Step 12: Full verify + commit**

Run: `go test ./auth-service/... && make build SERVICE=auth-service && make lint`
Expected: PASS, clean

```bash
git add auth-service
git commit -m "feat(auth-service): enforce account provisioning at the minting gate

Mint a NATS JWT only when {account, siteId: SITE_ID} exists in the
site's users collection; 403 account_not_provisioned otherwise, 500
fail-closed on store errors. Gate is skipped in DEV_MODE and can be
deferred with REQUIRE_PROVISIONED=false for staged rollout."
```

---

### Task 5: portal-service store (TDD via integration tests)

**Files:**
- Create: `portal-service/store.go`, `portal-service/integration_test.go`, `portal-service/store_mongo.go`
- Generate: `portal-service/mock_store_test.go`

- [ ] **Step 1: Define the contract**

Create `portal-service/store.go`:

```go
package main

import (
	"context"
	"errors"

	"github.com/hmchangw/chat/pkg/model"
)

var (
	ErrUserNotFound = errors.New("user not found") // FindUserByAccount: no directory entry
	ErrSiteNotFound = errors.New("site not found") // FindSiteByID: no sites entry
)

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

// site is one row of the ops-owned sites collection: the connection
// coordinates for a single site.
type site struct {
	ID             string `json:"siteId"         bson:"_id"`
	AuthServiceURL string `json:"authServiceUrl" bson:"authServiceUrl"`
	NATSURL        string `json:"natsUrl"        bson:"natsUrl"`
}

// DirectoryStore reads the global account→site directory and the per-site
// connection coordinates.
type DirectoryStore interface {
	FindUserByAccount(ctx context.Context, account string) (*model.User, error)
	FindSiteByID(ctx context.Context, siteID string) (*site, error)
	EnsureIndexes(ctx context.Context) error
}
```

- [ ] **Step 2: Write the failing integration tests**

Create `portal-service/integration_test.go`:

```go
//go:build integration

package main

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

func seedDirectory(t *testing.T) *mongoDirectoryStore {
	t.Helper()
	db := testutil.MongoDB(t, "portal")
	store := newMongoDirectoryStore(db)
	ctx := context.Background()
	require.NoError(t, store.EnsureIndexes(ctx))

	_, err := db.Collection("users").InsertOne(ctx,
		bson.M{"_id": "u-alice", "account": "alice", "siteId": "site-a", "employeeId": "E001"})
	require.NoError(t, err)
	_, err = db.Collection("sites").InsertOne(ctx,
		bson.M{"_id": "site-a", "authServiceUrl": "https://auth.site-a.example.com", "natsUrl": "wss://nats.site-a.example.com"})
	require.NoError(t, err)
	return store
}

func TestMongoDirectoryStore_FindUserByAccount(t *testing.T) {
	store := seedDirectory(t)
	ctx := context.Background()

	u, err := store.FindUserByAccount(ctx, "alice")
	require.NoError(t, err)
	assert.Equal(t, "alice", u.Account)
	assert.Equal(t, "site-a", u.SiteID)
	assert.Equal(t, "E001", u.EmployeeID)

	_, err = store.FindUserByAccount(ctx, "nobody")
	assert.ErrorIs(t, err, ErrUserNotFound)
}

func TestMongoDirectoryStore_FindSiteByID(t *testing.T) {
	store := seedDirectory(t)
	ctx := context.Background()

	s, err := store.FindSiteByID(ctx, "site-a")
	require.NoError(t, err)
	assert.Equal(t, "site-a", s.ID)
	assert.Equal(t, "https://auth.site-a.example.com", s.AuthServiceURL)
	assert.Equal(t, "wss://nats.site-a.example.com", s.NATSURL)

	_, err = store.FindSiteByID(ctx, "site-z")
	assert.ErrorIs(t, err, ErrSiteNotFound)
}

func TestMongoDirectoryStore_EnsureIndexes(t *testing.T) {
	store := seedDirectory(t)
	ctx := context.Background()

	// Idempotent: a second call must not error.
	require.NoError(t, store.EnsureIndexes(ctx))

	// Uniqueness: a second document with the same account must be rejected.
	_, err := store.users.InsertOne(ctx,
		bson.M{"_id": "u-alice-dup", "account": "alice", "siteId": "site-b"})
	require.Error(t, err)
	assert.True(t, mongo.IsDuplicateKeyError(err))
}
```

- [ ] **Step 3: Run to verify they fail**

Run: `go vet -tags integration ./portal-service/...`
Expected: FAIL — `undefined: newMongoDirectoryStore`

- [ ] **Step 4: Implement `portal-service/store_mongo.go`**

```go
package main

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/model"
)

type mongoDirectoryStore struct {
	users *mongo.Collection
	sites *mongo.Collection
}

func newMongoDirectoryStore(db *mongo.Database) *mongoDirectoryStore {
	return &mongoDirectoryStore{
		users: db.Collection("users"),
		sites: db.Collection("sites"),
	}
}

func (s *mongoDirectoryStore) FindUserByAccount(ctx context.Context, account string) (*model.User, error) {
	var u model.User
	err := s.users.FindOne(ctx,
		bson.M{"account": account},
		options.FindOne().SetProjection(bson.M{"_id": 1, "account": 1, "siteId": 1, "employeeId": 1}),
	).Decode(&u)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, fmt.Errorf("find user %q: %w", account, ErrUserNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("find user %q: %w", account, err)
	}
	return &u, nil
}

func (s *mongoDirectoryStore) FindSiteByID(ctx context.Context, siteID string) (*site, error) {
	var st site
	err := s.sites.FindOne(ctx, bson.M{"_id": siteID}).Decode(&st)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, fmt.Errorf("find site %q: %w", siteID, ErrSiteNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("find site %q: %w", siteID, err)
	}
	return &st, nil
}

// EnsureIndexes creates the unique account index the lookup path depends on.
// Mongo treats index creation as idempotent when the key spec matches.
func (s *mongoDirectoryStore) EnsureIndexes(ctx context.Context) error {
	_, err := s.users.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "account", Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	if err != nil {
		return fmt.Errorf("ensure users (account) unique index: %w", err)
	}
	return nil
}
```

- [ ] **Step 5: Run to verify they pass**

Run: `make test-integration SERVICE=portal-service`
Expected: PASS (3 tests)

- [ ] **Step 6: Generate the mock for the handler task**

Run: `make generate SERVICE=portal-service`
Expected: `portal-service/mock_store_test.go` created (`MockDirectoryStore`)

- [ ] **Step 7: Commit**

```bash
git add portal-service
git commit -m "feat(portal-service): Mongo directory store (users + sites) with integration tests"
```

---

### Task 6: portal-service handler + routes

**Files:**
- Create: `portal-service/handler_test.go` (first), `portal-service/handler.go`, `portal-service/routes.go`

- [ ] **Step 1: Write the failing handler tests**

Create `portal-service/handler_test.go`:

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errtest"
	"github.com/hmchangw/chat/pkg/model"
	pkgoidc "github.com/hmchangw/chat/pkg/oidc"
)

// fakeValidator implements TokenValidator for testing.
type fakeValidator struct {
	account string
	name    string
	expired bool
	invalid bool
}

func (f *fakeValidator) Validate(_ context.Context, _ string) (pkgoidc.Claims, error) {
	if f.expired {
		return pkgoidc.Claims{}, pkgoidc.ErrTokenExpired
	}
	if f.invalid {
		return pkgoidc.Claims{}, fmt.Errorf("oidc token verification failed: invalid signature")
	}
	return pkgoidc.Claims{PreferredUsername: f.account, Name: f.name}, nil
}

func setupRouter(t *testing.T, h *PortalHandler) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	registerRoutes(r, h)
	return r
}

func postLookup(t *testing.T, r *gin.Engine, body string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/lookup", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	return w
}

var testSite = &site{
	ID:             "site-a",
	AuthServiceURL: "https://auth.site-a.example.com",
	NATSURL:        "wss://nats.site-a.example.com",
}

func TestHandleLookup_HappyPath(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockDirectoryStore(ctrl)
	store.EXPECT().FindUserByAccount(gomock.Any(), "alice").
		Return(&model.User{ID: "u-alice", Account: "alice", SiteID: "site-a", EmployeeID: "E001"}, nil)
	store.EXPECT().FindSiteByID(gomock.Any(), "site-a").Return(testSite, nil)

	h := NewPortalHandler(&fakeValidator{account: "alice"}, store, false, "site-local")
	w := postLookup(t, setupRouter(t, h), `{"ssoToken":"valid-token"}`)

	require.Equal(t, http.StatusOK, w.Code)
	var resp lookupResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, lookupResponse{
		Account:        "alice",
		EmployeeID:     "E001",
		AuthServiceURL: "https://auth.site-a.example.com",
		NATSURL:        "wss://nats.site-a.example.com",
		SiteID:         "site-a",
	}, resp)
}

func TestHandleLookup_AccountFallsBackToNameClaim(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockDirectoryStore(ctrl)
	store.EXPECT().FindUserByAccount(gomock.Any(), "alice").
		Return(&model.User{Account: "alice", SiteID: "site-a"}, nil)
	store.EXPECT().FindSiteByID(gomock.Any(), "site-a").Return(testSite, nil)

	h := NewPortalHandler(&fakeValidator{name: "alice"}, store, false, "site-local")
	w := postLookup(t, setupRouter(t, h), `{"ssoToken":"valid-token"}`)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestHandleLookup_TokenErrors(t *testing.T) {
	tests := []struct {
		name       string
		validator  *fakeValidator
		wantReason errcode.Reason
	}{
		{"expired token", &fakeValidator{expired: true}, errcode.AuthTokenExpired},
		{"invalid token", &fakeValidator{invalid: true}, errcode.AuthInvalidToken},
		{"blank account claim", &fakeValidator{}, errcode.AuthInvalidToken},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockDirectoryStore(ctrl) // no EXPECT — must not be touched

			h := NewPortalHandler(tt.validator, store, false, "site-local")
			w := postLookup(t, setupRouter(t, h), `{"ssoToken":"tok"}`)

			assert.Equal(t, http.StatusUnauthorized, w.Code)
			errtest.AssertCode(t, w.Body.Bytes(), errcode.CodeUnauthenticated)
			errtest.AssertReason(t, w.Body.Bytes(), tt.wantReason)
		})
	}
}

func TestHandleLookup_MissingBody(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := NewPortalHandler(&fakeValidator{account: "alice"}, NewMockDirectoryStore(ctrl), false, "site-local")
	router := setupRouter(t, h)

	for _, body := range []string{`{}`, ``, `{"account":"alice"}`} {
		w := postLookup(t, router, body)
		assert.Equal(t, http.StatusBadRequest, w.Code, "body: %s", body)
		errtest.AssertCode(t, w.Body.Bytes(), errcode.CodeBadRequest)
		errtest.AssertReason(t, w.Body.Bytes(), errcode.AuthMissingFields)
	}
}

func TestHandleLookup_AccountNotProvisioned(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockDirectoryStore(ctrl)
	store.EXPECT().FindUserByAccount(gomock.Any(), "mallory").
		Return(nil, fmt.Errorf("find user: %w", ErrUserNotFound))

	h := NewPortalHandler(&fakeValidator{account: "mallory"}, store, false, "site-local")
	w := postLookup(t, setupRouter(t, h), `{"ssoToken":"tok"}`)

	assert.Equal(t, http.StatusForbidden, w.Code)
	errtest.AssertCode(t, w.Body.Bytes(), errcode.CodeForbidden)
	errtest.AssertReason(t, w.Body.Bytes(), errcode.PortalAccountNotProvisioned)
}

func TestHandleLookup_StoreErrors(t *testing.T) {
	tests := []struct {
		name  string
		setup func(store *MockDirectoryStore)
	}{
		{"user query fails", func(store *MockDirectoryStore) {
			store.EXPECT().FindUserByAccount(gomock.Any(), "alice").
				Return(nil, errors.New("mongo down"))
		}},
		{"site missing for known user", func(store *MockDirectoryStore) {
			store.EXPECT().FindUserByAccount(gomock.Any(), "alice").
				Return(&model.User{Account: "alice", SiteID: "site-gone"}, nil)
			store.EXPECT().FindSiteByID(gomock.Any(), "site-gone").
				Return(nil, fmt.Errorf("find site: %w", ErrSiteNotFound))
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockDirectoryStore(ctrl)
			tt.setup(store)

			h := NewPortalHandler(&fakeValidator{account: "alice"}, store, false, "site-local")
			w := postLookup(t, setupRouter(t, h), `{"ssoToken":"tok"}`)

			assert.Equal(t, http.StatusInternalServerError, w.Code)
			errtest.AssertCode(t, w.Body.Bytes(), errcode.CodeInternal)
			assert.NotContains(t, w.Body.String(), "mongo down", "raw cause must not leak")
		})
	}
}

func TestHandleLookup_DevMode(t *testing.T) {
	t.Run("known account resolves normally", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockDirectoryStore(ctrl)
		store.EXPECT().FindUserByAccount(gomock.Any(), "alice").
			Return(&model.User{Account: "alice", SiteID: "site-a", EmployeeID: "E001"}, nil)
		store.EXPECT().FindSiteByID(gomock.Any(), "site-a").Return(testSite, nil)

		h := NewPortalHandler(nil, store, true, "site-local")
		w := postLookup(t, setupRouter(t, h), `{"account":"alice"}`)
		require.Equal(t, http.StatusOK, w.Code)
		var resp lookupResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		assert.Equal(t, "site-a", resp.SiteID)
	})

	t.Run("unknown account falls back to the dev site", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockDirectoryStore(ctrl)
		store.EXPECT().FindUserByAccount(gomock.Any(), "newdev").
			Return(nil, fmt.Errorf("find user: %w", ErrUserNotFound))
		store.EXPECT().FindSiteByID(gomock.Any(), "site-local").Return(&site{
			ID: "site-local", AuthServiceURL: "http://localhost:8080", NATSURL: "ws://localhost:9222",
		}, nil)

		h := NewPortalHandler(nil, store, true, "site-local")
		w := postLookup(t, setupRouter(t, h), `{"account":"newdev"}`)
		require.Equal(t, http.StatusOK, w.Code)
		var resp lookupResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		assert.Equal(t, lookupResponse{
			Account: "newdev", EmployeeID: "",
			AuthServiceURL: "http://localhost:8080", NATSURL: "ws://localhost:9222",
			SiteID: "site-local",
		}, resp)
	})

	t.Run("fallback site unseeded is internal", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockDirectoryStore(ctrl)
		store.EXPECT().FindUserByAccount(gomock.Any(), "newdev").
			Return(nil, fmt.Errorf("find user: %w", ErrUserNotFound))
		store.EXPECT().FindSiteByID(gomock.Any(), "site-local").
			Return(nil, fmt.Errorf("find site: %w", ErrSiteNotFound))

		h := NewPortalHandler(nil, store, true, "site-local")
		w := postLookup(t, setupRouter(t, h), `{"account":"newdev"}`)
		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})

	t.Run("missing account is bad request", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		h := NewPortalHandler(nil, NewMockDirectoryStore(ctrl), true, "site-local")
		w := postLookup(t, setupRouter(t, h), `{}`)
		assert.Equal(t, http.StatusBadRequest, w.Code)
		errtest.AssertReason(t, w.Body.Bytes(), errcode.AuthMissingFields)
	})
}

func TestHandleHealth(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := NewPortalHandler(&fakeValidator{}, NewMockDirectoryStore(ctrl), false, "site-local")
	router := setupRouter(t, h)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "ok")
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./portal-service/...`
Expected: FAIL — `undefined: PortalHandler` / `registerRoutes`

- [ ] **Step 3: Implement `portal-service/handler.go`**

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
	"github.com/hmchangw/chat/pkg/model"
	pkgoidc "github.com/hmchangw/chat/pkg/oidc"
)

// TokenValidator validates an SSO token and returns OIDC claims.
type TokenValidator interface {
	Validate(ctx context.Context, rawToken string) (pkgoidc.Claims, error)
}

type lookupRequest struct {
	SSOToken string `json:"ssoToken" binding:"required"`
}

type devLookupRequest struct {
	Account string `json:"account" binding:"required"`
}

type lookupResponse struct {
	Account        string `json:"account"`
	EmployeeID     string `json:"employeeId"`
	AuthServiceURL string `json:"authServiceUrl"`
	NATSURL        string `json:"natsUrl"`
	SiteID         string `json:"siteId"`
}

// PortalHandler resolves a logged-in user's home-site connection coordinates
// from the portal directory. It is a discovery endpoint — the authoritative
// provisioning gate lives in auth-service.
type PortalHandler struct {
	validator         TokenValidator
	store             DirectoryStore
	devMode           bool
	devFallbackSiteID string
}

// NewPortalHandler creates a PortalHandler. In devMode the SSO token is not
// required and unknown accounts fall back to devFallbackSiteID.
func NewPortalHandler(validator TokenValidator, store DirectoryStore, devMode bool, devFallbackSiteID string) *PortalHandler {
	return &PortalHandler{
		validator:         validator,
		store:             store,
		devMode:           devMode,
		devFallbackSiteID: devFallbackSiteID,
	}
}

// HandleLookup validates the SSO token, derives the account from its claims,
// and resolves the account's home-site connection coordinates.
func (h *PortalHandler) HandleLookup(c *gin.Context) {
	if h.devMode {
		h.handleDevLookup(c)
		return
	}

	ctx := errcode.WithLogValues(c.Request.Context(), "request_id", c.GetString("request_id"))

	var req lookupRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		errhttp.Write(ctx, c, errcode.BadRequest("ssoToken is required",
			errcode.WithReason(errcode.AuthMissingFields)))
		return
	}

	claims, err := h.validator.Validate(ctx, req.SSOToken)
	if err != nil {
		if errors.Is(err, pkgoidc.ErrTokenExpired) {
			errhttp.Write(ctx, c, errcode.Unauthenticated("SSO token has expired, please re-login",
				errcode.WithReason(errcode.AuthTokenExpired)))
			return
		}
		errhttp.Write(ctx, c, errcode.Unauthenticated("invalid SSO token",
			errcode.WithReason(errcode.AuthInvalidToken),
			errcode.WithCause(err)))
		return
	}

	account := claims.Account()
	if account == "" {
		errhttp.Write(ctx, c, errcode.Unauthenticated("token missing account claim",
			errcode.WithReason(errcode.AuthInvalidToken)))
		return
	}
	ctx = errcode.WithLogValues(ctx, "account", account)

	h.resolve(ctx, c, account, false)
}

// handleDevLookup accepts a raw account without OIDC, for local development.
func (h *PortalHandler) handleDevLookup(c *gin.Context) {
	ctx := errcode.WithLogValues(c.Request.Context(), "request_id", c.GetString("request_id"))

	var req devLookupRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		errhttp.Write(ctx, c, errcode.BadRequest("account is required",
			errcode.WithReason(errcode.AuthMissingFields)))
		return
	}
	ctx = errcode.WithLogValues(ctx, "account", req.Account)

	h.resolve(ctx, c, req.Account, true)
}

// resolve maps account → directory user → site coordinates and writes the
// lookup response. devFallback substitutes the dev site for unknown accounts
// so local logins need no per-account seeding.
func (h *PortalHandler) resolve(ctx context.Context, c *gin.Context, account string, devFallback bool) {
	user, err := h.store.FindUserByAccount(ctx, account)
	switch {
	case err == nil:
	case errors.Is(err, ErrUserNotFound) && devFallback:
		user = &model.User{Account: account, SiteID: h.devFallbackSiteID}
	case errors.Is(err, ErrUserNotFound):
		errhttp.Write(ctx, c, errcode.Forbidden("account not provisioned for chat",
			errcode.WithReason(errcode.PortalAccountNotProvisioned)))
		return
	default:
		errhttp.Write(ctx, c, fmt.Errorf("find user by account: %w", err))
		return
	}

	st, err := h.store.FindSiteByID(ctx, user.SiteID)
	if err != nil {
		// Includes ErrSiteNotFound: a user pointing at an unconfigured site
		// is an ops data bug, not a client error.
		errhttp.Write(ctx, c, fmt.Errorf("find site %q: %w", user.SiteID, err))
		return
	}

	c.JSON(http.StatusOK, lookupResponse{
		Account:        user.Account,
		EmployeeID:     user.EmployeeID,
		AuthServiceURL: st.AuthServiceURL,
		NATSURL:        st.NATSURL,
		SiteID:         st.ID,
	})
}

func (h *PortalHandler) HandleHealth(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}
```

Create `portal-service/routes.go`:

```go
package main

import "github.com/gin-gonic/gin"

func registerRoutes(r *gin.Engine, h *PortalHandler) {
	r.POST("/lookup", h.HandleLookup)
	r.GET("/healthz", h.HandleHealth)
}
```

- [ ] **Step 4: Run to verify they pass**

Run: `go test ./portal-service/...`
Expected: PASS (all handler tests)

- [ ] **Step 5: Commit**

```bash
git add portal-service
git commit -m "feat(portal-service): POST /lookup handler — token-validated site discovery"
```

---

### Task 7: portal-service main, deploy files, docker-local wiring

**Files:**
- Create: `portal-service/main.go`, `portal-service/deploy/Dockerfile`, `portal-service/deploy/docker-compose.yml`, `portal-service/deploy/azure-pipelines.yml`
- Modify: `docker-local/compose.services.yaml`, `docker-local/setup.sh:111-117`

- [ ] **Step 1: Write `portal-service/main.go`**

```go
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/ginutil"
	"github.com/hmchangw/chat/pkg/mongoutil"
	pkgoidc "github.com/hmchangw/chat/pkg/oidc"
	"github.com/hmchangw/chat/pkg/shutdown"
)

type config struct {
	Port              string `env:"PORT"                        envDefault:"8081"`
	DevMode           bool   `env:"DEV_MODE"                    envDefault:"false"`
	DevFallbackSiteID string `env:"PORTAL_DEV_FALLBACK_SITE_ID" envDefault:"site-local"`

	// OIDC settings — required when DEV_MODE is false.
	OIDCIssuerURL string   `env:"OIDC_ISSUER_URL"`
	OIDCAudiences []string `env:"OIDC_AUDIENCES" envSeparator:","`
	TLSSkipVerify bool     `env:"TLS_SKIP_VERIFY" envDefault:"false"`

	MongoURI      string `env:"MONGO_URI,required"`
	MongoDB       string `env:"MONGO_DB"       envDefault:"portal"`
	MongoUsername string `env:"MONGO_USERNAME" envDefault:""`
	MongoPassword string `env:"MONGO_PASSWORD" envDefault:""`
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	if err := run(); err != nil {
		slog.Error("fatal error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := env.ParseAs[config]()
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}

	ctx := context.Background()

	mongoClient, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword)
	if err != nil {
		return fmt.Errorf("connect mongo: %w", err)
	}

	store := newMongoDirectoryStore(mongoClient.Database(cfg.MongoDB))
	idxCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := store.EnsureIndexes(idxCtx); err != nil {
		return fmt.Errorf("ensure directory indexes: %w", err)
	}

	var handler *PortalHandler
	if cfg.DevMode {
		slog.Info("dev mode enabled — OIDC validation disabled")
		handler = NewPortalHandler(nil, store, true, cfg.DevFallbackSiteID)
	} else {
		if cfg.OIDCIssuerURL == "" || len(cfg.OIDCAudiences) == 0 {
			return fmt.Errorf("OIDC_ISSUER_URL and OIDC_AUDIENCES are required when DEV_MODE is false")
		}
		oidcValidator, err := pkgoidc.NewValidator(ctx, pkgoidc.Config{
			IssuerURL:     cfg.OIDCIssuerURL,
			Audiences:     cfg.OIDCAudiences,
			TLSSkipVerify: cfg.TLSSkipVerify,
		})
		if err != nil {
			return fmt.Errorf("create oidc validator: %w", err)
		}
		slog.Info("oidc validator initialized", "issuer", cfg.OIDCIssuerURL)
		handler = NewPortalHandler(oidcValidator, store, false, cfg.DevFallbackSiteID)
	}

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(ginutil.RequestID())
	r.Use(ginutil.AccessLog())
	r.Use(ginutil.CORS())
	registerRoutes(r, handler)

	addr := fmt.Sprintf(":%s", cfg.Port)
	srv := &http.Server{
		Addr:         addr,
		Handler:      r,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	srvErr := make(chan error, 1)
	go func() {
		slog.Info("portal service starting", "addr", addr)
		srvErr <- srv.ListenAndServe()
	}()

	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		shutdown.Wait(ctx, 25*time.Second, func(ctx context.Context) error {
			slog.Info("shutting down portal service")
			err := srv.Shutdown(ctx)
			mongoutil.Disconnect(ctx, mongoClient)
			return err
		})
	}()

	err = <-srvErr
	if err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("listen portal server: %w", err)
	}
	<-shutdownDone

	return nil
}
```

- [ ] **Step 2: Verify build + tests**

Run: `make build SERVICE=portal-service && go test ./portal-service/...`
Expected: clean build, tests PASS

- [ ] **Step 3: Write `portal-service/deploy/Dockerfile`**

```dockerfile
FROM golang:1.25.11-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ pkg/
COPY portal-service/ portal-service/
RUN CGO_ENABLED=0 go build -o /portal-service ./portal-service/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /portal-service /portal-service
USER app
ENTRYPOINT ["/portal-service"]
```

- [ ] **Step 4: Write `portal-service/deploy/docker-compose.yml`**

```yaml
name: portal-service

services:
  # One-shot directory seed: the local site's connection coordinates plus the
  # demo users that tools/seed-sample-data also creates in the site DB.
  portal-seed:
    image: mongo:8.2.9
    entrypoint:
      - mongosh
      - --host
      - mongodb
      - --quiet
      - --eval
      - |
        const p = db.getSiblingDB('portal');
        p.sites.replaceOne(
          { _id: 'site-local' },
          { authServiceUrl: 'http://localhost:8080', natsUrl: 'ws://localhost:9222' },
          { upsert: true },
        );
        [['alice', 'E001'], ['bob', 'E002']].forEach(([account, employeeId]) =>
          p.users.replaceOne(
            { _id: 'portal-demo-' + account },
            { account: account, siteId: 'site-local', employeeId: employeeId },
            { upsert: true },
          ),
        );
        print('portal seed: sites=' + p.sites.countDocuments() + ' users=' + p.users.countDocuments());
    networks:
      - chat-local

  portal-service:
    build:
      context: ../..
      dockerfile: portal-service/deploy/Dockerfile
    pull_policy: build
    depends_on:
      portal-seed:
        condition: service_completed_successfully
    ports:
      - "8081:8081"
    env_file:
      - path: ../../docker-local/.env
        required: false
    environment:
      - PORT=8081
      # Bypass OIDC; accept any account name. Flip to false to test OIDC.
      - DEV_MODE=${DEV_MODE:-true}
      - PORTAL_DEV_FALLBACK_SITE_ID=site-local
      - MONGO_URI=mongodb://mongodb:27017
      - MONGO_DB=portal
      - OIDC_ISSUER_URL=http://keycloak:8080/realms/chatapp
      - OIDC_AUDIENCES=nats-chat
      - TLS_SKIP_VERIFY=false
    networks:
      - chat-local

networks:
  chat-local:
    external: true
```

- [ ] **Step 5: Write `portal-service/deploy/azure-pipelines.yml`**

Copy `auth-service/deploy/azure-pipelines.yml` verbatim, replacing every `auth-service` with `portal-service` (trigger/pr path includes, `SERVICE_NAME`).

- [ ] **Step 6: Wire docker-local**

In `docker-local/compose.services.yaml`, add to the `include:` list (alphabetical, after `notification-worker`):

```yaml
  - ../portal-service/deploy/docker-compose.yml
```

In `docker-local/setup.sh`, replace the `.env.local` heredoc body (lines 112-116):

```bash
if [ ! -f "$FRONTEND_ENV_FILE" ]; then
  cat > "$FRONTEND_ENV_FILE" <<EOF
VITE_PORTAL_URL=http://localhost:8081
EOF
fi
```

- [ ] **Step 7: Validate compose files parse**

Run: `docker compose -f portal-service/deploy/docker-compose.yml config -q && docker compose -f docker-local/compose.services.yaml config -q`
Expected: exit 0 (no output). If Docker is unavailable, skip — YAML is checked in Task 14's lint anyway.

- [ ] **Step 8: Commit**

```bash
git add portal-service docker-local/compose.services.yaml docker-local/setup.sh
git commit -m "feat(portal-service): service entrypoint, deploy files, docker-local wiring"
```

---

### Task 8: documentation

**Files:**
- Modify: `docs/client-api.md` (§2.1 narrative, §2.2 error table, new §2.3, §6 catalog)
- Modify: `chat-frontend/CLAUDE.md` (reason catalog)

- [ ] **Step 1: §2.1 — point connections at the portal**

In `docs/client-api.md` §2.1, after the first sentence ("A client connects to NATS using a user NKey pair plus a signed JWT obtained from the auth-service (§2.2)."), append to the same paragraph:

```markdown
The auth-service base URL, the NATS WebSocket URL, and the user's `siteId` are not static client config — they are resolved per user at login via the portal lookup (§2.3).
```

- [ ] **Step 2: §2.2 — add the gate's error row**

In the §2.2 error table, after the `invalid_sso_token` row:

```markdown
| 403 | `forbidden` | `account_not_provisioned` | `{ "code": "forbidden", "reason": "account_not_provisioned", "error": "account not provisioned for this site" }` — the account is not in this site's user directory (unprovisioned, or homed on a different site). Applies to initial login and background renewal alike. |
```

- [ ] **Step 3: add §2.3**

Insert after §2.2's final "Triggered events — error path" block (before `## 3. Request/Reply Methods`):

````markdown
### 2.3 HTTP — POST /lookup (portal-service)

**Endpoint:** `POST /lookup`
**Reply:** synchronous HTTP response

Site discovery — called once per login, **before** §2.2. Validates the SSO token, derives the account from the token's claims (`preferred_username`, falling back to `name`), looks it up in the global portal directory, and returns the home site's connection coordinates. The client then calls `POST {authServiceUrl}/auth` (§2.2) and connects to `natsUrl` (§2.1). JWT renewal does **not** re-query the portal — site assignment is stable within a session.

#### Request body

| Field | Type | Required | Notes |
|---|---|---|---|
| `ssoToken` | string | yes | OIDC-issued SSO token. |

```json
{ "ssoToken": "<sso-token>" }
```

#### Success response

`HTTP 200`

| Field | Type | Notes |
|---|---|---|
| `account` | string | The `{account}` used in every NATS subject. |
| `employeeId` | string | From the portal directory; informational. |
| `authServiceUrl` | string | Base URL of the home site's auth-service — call `POST {authServiceUrl}/auth` next. |
| `natsUrl` | string | WebSocket URL of the home site's NATS. |
| `siteId` | string | The user's home site; scopes site-suffixed NATS subjects. |

```json
{
  "account": "alice",
  "employeeId": "E12345",
  "authServiceUrl": "https://auth.site-a.example.com",
  "natsUrl": "wss://nats.site-a.example.com",
  "siteId": "site-a"
}
```

#### Error response

See [Error envelope](#6-error-envelope-reference). HTTP statuses:

| Status | `code` | `reason` | Example body |
|---|---|---|---|
| 400 | `bad_request` | `missing_fields` | `{ "code": "bad_request", "reason": "missing_fields", "error": "ssoToken is required" }` |
| 401 | `unauthenticated` | `sso_token_expired` | `{ "code": "unauthenticated", "reason": "sso_token_expired", "error": "SSO token has expired, please re-login" }` |
| 401 | `unauthenticated` | `invalid_sso_token` | `{ "code": "unauthenticated", "reason": "invalid_sso_token", "error": "invalid SSO token" }` |
| 403 | `forbidden` | `account_not_provisioned` | `{ "code": "forbidden", "reason": "account_not_provisioned", "error": "account not provisioned for chat" }` |
| 500 | `internal` | — | `{ "code": "internal", "error": "internal error" }` |

#### Triggered events — success path

`None — HTTP-only.`

#### Triggered events — error path

`None.`
````

- [ ] **Step 4: §6 catalog row**

After the `missing_fields` row (~line 3509):

```markdown
| `account_not_provisioned` | forbidden | portal-service `POST /lookup`; auth-service `POST /auth` minting gate |
```

- [ ] **Step 5: frontend reason catalog**

In `chat-frontend/CLAUDE.md`, in the "Reasons emitted today" list, after the auth-service line:

```markdown
- `account_not_provisioned` — portal-service lookup / auth-service minting gate (account passed Keycloak but is not provisioned for chat; show "contact your administrator" copy)
```

- [ ] **Step 6: Commit**

```bash
git add docs/client-api.md chat-frontend/CLAUDE.md
git commit -m "docs(client-api): document POST /lookup and the account_not_provisioned reason"
```

---

### Task 9: frontend runtimeConfig + vite proxy

**Files:**
- Modify: `chat-frontend/src/lib/runtimeConfig.js`, `chat-frontend/src/lib/runtimeConfig.test.js`, `chat-frontend/vite.config.js`

- [ ] **Step 1: Write the failing tests**

Append to `runtimeConfig.test.js` inside the existing `describe`:

```js
  it('PORTAL_URL defaults to localhost:8081', async () => {
    const { PORTAL_URL } = await import('./runtimeConfig.js')
    expect(PORTAL_URL).toBe('http://localhost:8081')
  })

  it('PORTAL_URL reads from window.__APP_CONFIG__', async () => {
    window.__APP_CONFIG__ = { PORTAL_URL: 'https://portal.example.com' }
    const { PORTAL_URL } = await import('./runtimeConfig.js')
    expect(PORTAL_URL).toBe('https://portal.example.com')
  })

  it('no longer exports the retired static connection vars', async () => {
    const mod = await import('./runtimeConfig.js')
    expect(mod.AUTH_URL).toBeUndefined()
    expect(mod.NATS_URL).toBeUndefined()
    expect(mod.DEFAULT_SITE_ID).toBeUndefined()
  })
```

- [ ] **Step 2: Run to verify failure**

Run: `cd chat-frontend && npx vitest run src/lib/runtimeConfig.test.js`
Expected: FAIL (PORTAL_URL undefined; retired vars still exported)

- [ ] **Step 3: Implement**

Replace lines 6-13 of `runtimeConfig.js` (the `AUTH_URL`, `NATS_URL`, `DEFAULT_SITE_ID` exports) with:

```js
export const PORTAL_URL =
  runtime.PORTAL_URL || import.meta.env.VITE_PORTAL_URL || 'http://localhost:8081'
```

(`DEV_MODE`, `OIDC_ISSUER_URL`, `OIDC_CLIENT_ID` stay.)

In `vite.config.js`, delete the dead proxy from the `server` block, leaving:

```js
  server: {
    port: 3000,
  },
```

- [ ] **Step 4: Run to verify pass**

Run: `cd chat-frontend && npx vitest run src/lib/runtimeConfig.test.js`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add chat-frontend/src/lib/runtimeConfig.js chat-frontend/src/lib/runtimeConfig.test.js chat-frontend/vite.config.js
git commit -m "feat(frontend): replace static AUTH_URL/NATS_URL/DEFAULT_SITE_ID with PORTAL_URL"
```

> Note: `NatsContext.jsx` and `LoginPage.jsx` still import the removed exports after this commit, so the **suite-wide** test run is red until Tasks 10-12 land. The per-file runs above stay green; run the full suite only at Task 12 Step 6. If your pre-commit hook runs the full frontend suite, commit Tasks 9-12 together at Task 12 instead.

---

### Task 10: `useJwtRefresh` takes a dynamic auth URL

**Files:**
- Modify: `chat-frontend/src/context/NatsContext/useJwtRefresh.js`, `chat-frontend/src/context/NatsContext/useJwtRefresh.test.js`

- [ ] **Step 1: Update the tests (red)**

In `useJwtRefresh.test.js`, change the `setup` helper to pass a getter:

```js
function setup({ ncRef = { current: { reconnect: vi.fn() } } } = {}) {
  const view = renderHook(() => useJwtRefresh({ getAuthUrl: () => 'http://auth', ncRef }))
  return { ...view, ncRef }
}
```

Add one new test at the end of the `describe`:

```js
  it('re-mints against the URL the getter returns at refresh time', async () => {
    renewSsoToken.mockResolvedValue('fresh-sso')
    global.fetch.mockResolvedValue(okResp(makeJwt(3600)))
    let authUrl = 'http://auth-initial'
    const { result } = renderHook(() =>
      useJwtRefresh({ getAuthUrl: () => authUrl, ncRef: { current: null } }))

    authUrl = 'http://auth.site-a' // resolved later, e.g. by the portal lookup
    act(() => {
      result.current.setCredentials({ jwt: makeJwt(100), seed: new Uint8Array([9]), natsPublicKey: 'UPUB', refreshable: true })
    })
    await vi.advanceTimersByTimeAsync(95 * 1000)
    expect(global.fetch).toHaveBeenCalledWith('http://auth.site-a/auth', expect.anything())
  })
```

- [ ] **Step 2: Run to verify failure**

Run: `cd chat-frontend && npx vitest run src/context/NatsContext/useJwtRefresh.test.js`
Expected: FAIL (hook still destructures `authUrl`; fetch hits `undefined/auth`)

- [ ] **Step 3: Implement**

In `useJwtRefresh.js`:
- Signature: `export function useJwtRefresh({ getAuthUrl, ncRef }) {`
- The doc comment gains one line under "Returns:": `getAuthUrl is read at each refresh, so the target can be resolved after mount (portal lookup).`
- The re-mint fetch becomes:

```js
      const resp = await fetch(`${getAuthUrl()}/auth`, {
```

- The `refresh` callback's dependency array: `[getAuthUrl, ncRef, scheduleRefresh, redirect, armTimer]`

- [ ] **Step 4: Run to verify pass**

Run: `cd chat-frontend && npx vitest run src/context/NatsContext/useJwtRefresh.test.js`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add chat-frontend/src/context/NatsContext/useJwtRefresh.js chat-frontend/src/context/NatsContext/useJwtRefresh.test.js
git commit -m "feat(frontend): useJwtRefresh resolves the auth URL via getter at refresh time"
```

---

### Task 11: NatsContext — portal lookup drives connect

**Files:**
- Modify: `chat-frontend/src/context/NatsContext/NatsContext.jsx`, `chat-frontend/src/context/NatsContext/NatsContext.test.jsx`

- [ ] **Step 1: Rewrite the connect-wiring tests (red)**

Replace the `beforeEach` fetch mock and the connect tests in `NatsContext.test.jsx` with:

```js
const PORTAL_RESP = {
  account: 'alice',
  employeeId: 'E001',
  authServiceUrl: 'http://auth.site-a',
  natsUrl: 'ws://nats.site-a',
  siteId: 'site-a',
}

describe('NatsProvider connect wiring', () => {
  beforeEach(() => {
    setCredentials.mockReset()
    stop.mockReset()
    natsConnect.mockReset().mockResolvedValue({ closed: () => new Promise(() => {}) })
    global.fetch = vi.fn(async (url) => {
      if (String(url).endsWith('/lookup')) {
        return { ok: true, json: async () => PORTAL_RESP }
      }
      return { ok: true, json: async () => ({ natsJwt: 'JWT123', user: { account: 'alice' } }) }
    })
  })
  afterEach(() => { vi.restoreAllMocks() })

  it('resolves the site via portal, then auths and connects with the resolved URLs', async () => {
    const { result } = renderHook(() => useNats(), { wrapper })
    await act(async () => {
      await result.current.connect({ mode: 'sso', ssoToken: 'tok' })
    })

    expect(global.fetch).toHaveBeenNthCalledWith(1, 'http://localhost:8081/lookup',
      expect.objectContaining({ body: JSON.stringify({ ssoToken: 'tok' }) }))
    expect(global.fetch).toHaveBeenNthCalledWith(2, 'http://auth.site-a/auth', expect.anything())
    expect(setCredentials).toHaveBeenCalledWith({
      jwt: 'JWT123',
      seed: new Uint8Array([7]),
      natsPublicKey: 'UPUBKEY',
      refreshable: true,
    })
    expect(natsConnect).toHaveBeenCalledWith(
      expect.objectContaining({ servers: 'ws://nats.site-a', authenticator: fakeAuthenticator }))
    await waitFor(() => expect(result.current.connected).toBe(true))
    expect(result.current.user.siteId).toBe('site-a')
  })

  it('dev mode sends the account to the portal and is non-refreshable', async () => {
    const { result } = renderHook(() => useNats(), { wrapper })
    await act(async () => {
      await result.current.connect({ mode: 'dev', account: 'alice' })
    })
    expect(global.fetch).toHaveBeenNthCalledWith(1, 'http://localhost:8081/lookup',
      expect.objectContaining({ body: JSON.stringify({ account: 'alice' }) }))
    expect(setCredentials).toHaveBeenCalledWith(expect.objectContaining({ refreshable: false }))
  })

  it('propagates the portal error envelope and never dials auth or NATS', async () => {
    global.fetch = vi.fn(async () => ({
      ok: false,
      json: async () => ({ code: 'forbidden', reason: 'account_not_provisioned', error: 'account not provisioned for chat' }),
    }))
    const { result } = renderHook(() => useNats(), { wrapper })
    let thrown
    await act(async () => {
      try { await result.current.connect({ mode: 'sso', ssoToken: 'tok' }) } catch (err) { thrown = err }
    })
    expect(thrown.reason).toBe('account_not_provisioned')
    expect(thrown.code).toBe('forbidden')
    expect(global.fetch).toHaveBeenCalledTimes(1)
    expect(natsConnect).not.toHaveBeenCalled()
  })

  it('stops the refresh loop on disconnect', async () => {
    natsConnect.mockResolvedValue({
      closed: () => new Promise(() => {}),
      drain: vi.fn().mockResolvedValue(),
    })
    const { result } = renderHook(() => useNats(), { wrapper })
    await act(async () => {
      await result.current.connect({ mode: 'sso', ssoToken: 'tok' })
    })
    await act(async () => { await result.current.disconnect() })
    expect(stop).toHaveBeenCalledTimes(1)
  })
})
```

- [ ] **Step 2: Run to verify failure**

Run: `cd chat-frontend && npx vitest run src/context/NatsContext/NatsContext.test.jsx`
Expected: FAIL (still imports removed `AUTH_URL`/`NATS_URL`; no portal call)

- [ ] **Step 3: Implement in `NatsContext.jsx`**

Import change (line 4): `import { PORTAL_URL } from '@/lib/runtimeConfig'`.

Replace lines 22-26 (`const authUrl = AUTH_URL` … `useJwtRefresh({ authUrl, ncRef })`) with:

```jsx
  // Resolved per user by the portal lookup at connect time; the JWT-refresh
  // loop reads it through the getter so re-mints follow the resolved site.
  const authUrlRef = useRef(null)
  const getAuthUrl = useCallback(() => authUrlRef.current, [])

  const { authenticator, setCredentials, stop } = useJwtRefresh({ getAuthUrl, ncRef })
```

Replace `connectToNats` (keep the JSDoc, updating `@param`s: drop `siteId`, note the portal step) with:

```jsx
  const connectToNats = useCallback(async (opts) => {
    setError(null)

    const { mode, account, ssoToken } = opts || {}

    // 1) Site discovery: which auth-service, which NATS, which siteId.
    const lookupResp = await fetch(`${PORTAL_URL}/lookup`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(mode === 'sso' ? { ssoToken } : { account }),
    })
    if (!lookupResp.ok) {
      const errBody = await lookupResp.json().catch(() => ({}))
      throw new AsyncJobError(
        errBody.error || `Portal lookup failed: ${lookupResp.status}`,
        ASYNC_JOB_ERROR_KINDS.SyncError,
        { code: errBody.code, reason: errBody.reason, metadata: errBody.metadata },
      )
    }
    const portal = await lookupResp.json()
    authUrlRef.current = portal.authServiceUrl

    // 2) Mint the NATS JWT at the resolved site's auth-service.
    const nkey = createUser()
    const natsPublicKey = nkey.getPublicKey()

    const body =
      mode === 'sso'
        ? { ssoToken, natsPublicKey }
        : { account, natsPublicKey }

    const authResp = await fetch(`${portal.authServiceUrl}/auth`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    })

    if (!authResp.ok) {
      const errBody = await authResp.json().catch(() => ({}))
      throw new AsyncJobError(
        errBody.error || `Auth failed: ${authResp.status}`,
        ASYNC_JOB_ERROR_KINDS.SyncError,
        { code: errBody.code, reason: errBody.reason, metadata: errBody.metadata },
      )
    }

    const { natsJwt, user: userInfo } = await authResp.json()

    // Populate the credential refs BEFORE connecting so the dynamic
    // authenticator's getters return the right values during the handshake.
    setCredentials({
      jwt: natsJwt,
      seed: nkey.getSeed(),
      natsPublicKey,
      refreshable: mode === 'sso',
    })

    // 3) Dial the resolved site's NATS.
    const nc = await natsConnect({
      servers: portal.natsUrl,
      authenticator,
    })

    ncRef.current = nc
    setUser({ ...userInfo, siteId: portal.siteId })
    setConnected(true)

    nc.closed().then((err) => {
      if (err) {
        setError(`Disconnected: ${err.message}`)
      }
      setConnected(false)
    })
  }, [authenticator, setCredentials])
```

(Also add `useRef` is already imported; keep the existing error-envelope comments.)

- [ ] **Step 4: Run to verify pass**

Run: `cd chat-frontend && npx vitest run src/context/NatsContext/NatsContext.test.jsx`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add chat-frontend/src/context/NatsContext
git commit -m "feat(frontend): NatsContext resolves site via portal lookup before auth + connect"
```

---

### Task 12: LoginPage, OidcCallback, oidcClient, REASON_COPY

**Files:**
- Modify: `chat-frontend/src/pages/LoginPage/LoginPage.jsx` + `LoginPage.test.jsx`
- Modify: `chat-frontend/src/pages/OidcCallback/OidcCallback.jsx` + `OidcCallback.test.jsx`
- Modify: `chat-frontend/src/api/auth/oidcClient.js`
- Modify: `chat-frontend/src/api/_transport/asyncJob.ts`

- [ ] **Step 1: Update the page tests (red)**

`LoginPage.test.jsx` — remove `DEFAULT_SITE_ID: 'site-local',` from the runtimeConfig mock; in the DEV_MODE=true block replace the two affected tests:

```js
  it('renders the dev account form without a Site ID field', () => {
    useNats.mockReturnValue({ connect: vi.fn(), error: null })
    render(<LoginPage />)
    expect(screen.getByLabelText(/account/i)).toBeInTheDocument()
    expect(screen.queryByLabelText(/site id/i)).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /keycloak/i })).not.toBeInTheDocument()
  })

  it('submits with {mode: "dev", account}', async () => {
    const connect = vi.fn().mockResolvedValue(undefined)
    useNats.mockReturnValue({ connect, error: null })

    render(<LoginPage />)

    fireEvent.change(screen.getByLabelText(/account/i), { target: { value: 'alice' } })
    fireEvent.click(screen.getByRole('button', { name: /connect/i }))

    await waitFor(() => {
      expect(connect).toHaveBeenCalledWith({ mode: 'dev', account: 'alice' })
    })
  })
```

In the DEV_MODE=false block, replace the sessionStorage test:

```js
  it('redirects to Keycloak without any Site ID input or stash', async () => {
    useNats.mockReturnValue({ connect: vi.fn(), error: null })
    const signinRedirect = vi.fn().mockResolvedValue(undefined)
    getOidcManager.mockReturnValue({ signinRedirect })

    render(<LoginPage />)
    expect(screen.queryByLabelText(/site id/i)).not.toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: /keycloak/i }))

    await waitFor(() => {
      expect(signinRedirect).toHaveBeenCalled()
    })
    expect(window.sessionStorage.getItem('oidc.siteId')).toBeNull()
  })
```

`OidcCallback.test.jsx` — in the success test, delete the `window.sessionStorage.setItem('oidc.siteId', 'site-A')` line and change the assertion to:

```js
      expect(connect).toHaveBeenCalledWith({
        mode: 'sso',
        ssoToken: 'access-token-123',
      })
```

Also delete the `window.sessionStorage.setItem('oidc.siteId', 'site-A')` line in the "connect() fails" test.

- [ ] **Step 2: Run to verify failure**

Run: `cd chat-frontend && npx vitest run src/pages`
Expected: FAIL (components still render Site ID / pass siteId)

- [ ] **Step 3: Implement the components**

`LoginPage.jsx` — full replacement:

```jsx
import { useState } from 'react'
import { useNats } from '@/context/NatsContext'
import { DEV_MODE } from '@/lib/runtimeConfig'
import { getOidcManager, isSSOTokenInvalidError, redirectToReloginOnTokenInvalid } from '@/api/auth/oidcClient'
import { formatAsyncJobError } from '@/api'
import './style.css'

export default function LoginPage() {
  const { connect, error: natsError } = useNats()

  const [account, setAccount] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  const handleDevSubmit = async (e) => {
    e.preventDefault()
    if (!account.trim()) return

    setLoading(true)
    setError(null)
    try {
      await connect({
        mode: 'dev',
        account: account.trim(),
      })
    } catch (err) {
      if (isSSOTokenInvalidError(err)) {
        await redirectToReloginOnTokenInvalid()
        return
      }
      setError(formatAsyncJobError(err))
    } finally {
      setLoading(false)
    }
  }

  const handleKeycloakLogin = async () => {
    setLoading(true)
    setError(null)
    try {
      const manager = getOidcManager()
      await manager.signinRedirect()
      // Browser navigates away — code below this point is unreachable in prod.
    } catch (err) {
      if (isSSOTokenInvalidError(err)) {
        await redirectToReloginOnTokenInvalid()
        return
      }
      setError(formatAsyncJobError(err))
      setLoading(false)
    }
  }

  if (DEV_MODE) {
    return (
      <div className="login-page">
        <form className="login-form" onSubmit={handleDevSubmit}>
          <h1>Chat</h1>
          <p className="login-subtitle">Dev Mode Login</p>

          <label htmlFor="account">Account</label>
          <input
            id="account"
            type="text"
            value={account}
            onChange={(e) => setAccount(e.target.value)}
            placeholder="e.g. alice"
            autoFocus
            disabled={loading}
          />

          <button type="submit" disabled={loading || !account.trim()}>
            {loading ? 'Connecting...' : 'Connect'}
          </button>

          {(error || natsError) && (
            <div className="login-error">{error || natsError}</div>
          )}
        </form>
      </div>
    )
  }

  return (
    <div className="login-page">
      <div className="login-form">
        <h1>Chat</h1>
        <p className="login-subtitle">Sign in with Keycloak</p>

        <button type="button" onClick={handleKeycloakLogin} disabled={loading}>
          {loading ? 'Redirecting...' : 'Sign in with Keycloak'}
        </button>

        {(error || natsError) && (
          <div className="login-error">{error || natsError}</div>
        )}
      </div>
    </div>
  )
}
```

`OidcCallback.jsx` — in `run()`, delete the `const siteId = window.sessionStorage.getItem('oidc.siteId') || ''` line and change the connect call to:

```jsx
        await connect({
          mode: 'sso',
          ssoToken: user.access_token,
        })
```

`oidcClient.js` — in `redirectToReloginOnTokenInvalid`, delete the line `window.sessionStorage.removeItem('oidc.siteId')` and change the preceding comment to `// Clear oidc-client-ts's stashed user state.`

`asyncJob.ts` — add to `REASON_COPY` (after the `pin_room_too_large` line):

```ts
  account_not_provisioned: "Your account isn't set up for chat yet — contact your administrator.",
```

- [ ] **Step 4: Run to verify pass**

Run: `cd chat-frontend && npx vitest run src/pages src/api`
Expected: PASS

- [ ] **Step 5: Run the whole frontend suite + typecheck**

Run: `cd chat-frontend && npm test && npm run typecheck`
Expected: PASS — this is the point where the Task 9 breakage window must be fully closed. Fix any straggler imports of the retired config exports.

- [ ] **Step 6: Commit**

```bash
git add chat-frontend/src
git commit -m "feat(frontend): remove typed Site ID — portal resolves the site in both login modes"
```

---

### Task 13: frontend deploy config

**Files:**
- Modify: `chat-frontend/deploy/config.js.template`, `chat-frontend/deploy/30-render-config.sh`, `chat-frontend/deploy/docker-compose.yml`

- [ ] **Step 1: `config.js.template`**

```js
// Generated at container start — edit template, not output.
window.__APP_CONFIG__ = {
  PORTAL_URL: "${PORTAL_URL}",
  DEV_MODE: "${DEV_MODE}",
  OIDC_ISSUER_URL: "${OIDC_ISSUER_URL}",
  OIDC_CLIENT_ID: "${OIDC_CLIENT_ID}"
};
```

- [ ] **Step 2: `30-render-config.sh`**

```sh
#!/bin/sh
# Renders /config.js from env vars at container start.
set -eu

: "${PORTAL_URL:=http://localhost:8081}"
: "${DEV_MODE:=false}"
: "${OIDC_ISSUER_URL:=}"
: "${OIDC_CLIENT_ID:=nats-chat}"
export PORTAL_URL DEV_MODE OIDC_ISSUER_URL OIDC_CLIENT_ID

envsubst '${PORTAL_URL} ${DEV_MODE} ${OIDC_ISSUER_URL} ${OIDC_CLIENT_ID}' \
  < /etc/config.js.template \
  > /usr/share/nginx/html/config.js

echo "rendered /config.js  PORTAL_URL=$PORTAL_URL  DEV_MODE=$DEV_MODE  OIDC_ISSUER_URL=$OIDC_ISSUER_URL  OIDC_CLIENT_ID=$OIDC_CLIENT_ID"
```

- [ ] **Step 3: `deploy/docker-compose.yml` environment block**

```yaml
    environment:
      PORTAL_URL: ${PORTAL_URL:-http://localhost:8081}
      # OIDC values are only consumed when DEV_MODE=false. The Keycloak
      # realm at OIDC_ISSUER_URL must serve OIDC_CLIENT_ID with a redirect
      # URI matching this frontend's /oidc-callback.
      DEV_MODE: ${DEV_MODE:-false}
      OIDC_ISSUER_URL: ${OIDC_ISSUER_URL:-http://localhost:8180/realms/chatapp}
      OIDC_CLIENT_ID: ${OIDC_CLIENT_ID:-nats-chat}
```

- [ ] **Step 4: Commit**

```bash
git add chat-frontend/deploy
git commit -m "feat(frontend): deploy config serves PORTAL_URL instead of static auth/NATS URLs"
```

---

### Task 14: full verification sweep + push

- [ ] **Step 1: Format + lint**

Run: `make fmt && make lint`
Expected: no diffs, no findings. Fix anything reported.

- [ ] **Step 2: All unit tests**

Run: `make test`
Expected: PASS across the repo.

- [ ] **Step 3: Integration tests for the touched services**

Run: `make test-integration SERVICE=portal-service && make test-integration SERVICE=auth-service`
Expected: PASS (Docker required).

- [ ] **Step 4: Coverage floor (CLAUDE.md: ≥80%)**

```bash
go test -race -tags integration -coverprofile=coverage.out ./portal-service/... ./auth-service/... ./pkg/ginutil/... ./pkg/oidc/... ./pkg/errcode/...
go tool cover -func=coverage.out | tail -5
```

Expected: every listed package ≥80% total. If portal-service falls short, the uncovered lines are almost certainly in `main.go` — do NOT pad with fake tests; check the handler/store branches first.

- [ ] **Step 5: Frontend suite, typecheck, production build**

Run: `cd chat-frontend && npm test && npm run typecheck && npm run build`
Expected: all green, clean build.

- [ ] **Step 6: SAST (blocking CI gate)**

Run: `make sast`
Expected: PASS. If tools are missing run `make tools` first; if the environment can't install them, note it and rely on the CI `sast` job.

- [ ] **Step 7: Push**

```bash
git push -u origin claude/eager-einstein-7u2je6
```

(Retry per repo policy on network failure: 2s/4s/8s/16s backoff.)

---

## Spec-coverage checklist (self-review)

- POST /lookup prod + dev shapes, response object — Tasks 5, 6
- Error semantics incl. `account_not_provisioned` 403 — Tasks 1, 6
- Dev fallback site — Task 6 (`resolve` devFallback branch)
- Mongo users + sites, unique account index, sites ownership (docs) — Tasks 5, 8
- Enforced provisioning gate with siteId match, fail-closed, REQUIRE_PROVISIONED, dev skip — Task 4
- `pkg/ginutil`, `Claims.Account()` — Tasks 2, 3
- Frontend: PORTAL_URL, connect via portal, dynamic refresh URL, no Site ID anywhere, REASON_COPY, vite proxy removal, deploy templates — Tasks 9-13
- docker-local: compose include, seed, setup.sh env — Task 7
- client-api.md §2.1/§2.2/§2.3/§6 + frontend CLAUDE.md catalog — Task 8
- Testing pyramid per spec (unit, integration, frontend) — Tasks 4-6, 9-12; coverage gate — Task 14
