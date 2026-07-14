# admin-service + admin room-override (backend) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the `admin-service` (REST account-management backend) plus the shared/model changes it needs and platform-admin override in the room services, so site operators can manage accounts/sessions and administer any room.

**Architecture:** A new flat Gin service (`admin-service`) authorizes every request by hashing the caller's existing bot/admin session `authToken` and looking it up in the shared `sessions` collection (role must be `admin`), then performs user/session CRUD directly against MongoDB. Token hashing is shared with botplatform via a new `pkg/sessiontoken`. `room-service` (and, if it re-checks, `room-worker`) bypass owner/member gates when the actor is a platform admin.

**Tech Stack:** Go 1.25, Gin, `go.mongodb.org/mongo-driver/v2`, `pkg/errcode`/`errhttp`, `pkg/mongoutil`, `caarlos0/env`, `go.uber.org/mock`, `stretchr/testify`, `pkg/testutil` (testcontainers).

> **Descoped 2026-07-03:** Tasks 11–12's **room admin-override** was NOT shipped — admins
> get normal-user NATS scope and no owner-check bypass ("no extra permissions for admin
> now"; the base already has its own platform-admin room logic). Tasks 1–10 (the
> admin-service + foundations) shipped as one squashed commit on `chat-frontend-bot-login`.

## Global Constraints

- Go 1.25; single root `go.mod`; services are flat `package main` dirs at repo root.
- Use `make` targets, never raw `go` (`make test SERVICE=admin-service`, `make generate SERVICE=admin-service`, `make lint`, `make sast`).
- TDD mandatory (Red→Green→Refactor→Commit); ≥80% coverage, 90%+ for handlers/stores.
- All client-facing errors via `pkg/errcode` constructors; reply with `errhttp.Write`. Never log+return the same error.
- Mongo: native driver v2, `mongoutil.Connect`, explicit projections always, check `mongo.ErrNoDocuments`, app-generated `_id` via `pkg/idgen`.
- Config via `caarlos0/env` typed struct; `SCREAMING_SNAKE_CASE`; required secrets have no default; fail fast.
- Logging: `log/slog` JSON only; never log tokens, passwords, or bcrypt hashes.
- Password recipe everywhere: `bcrypt(sha256_hex(plaintext))` at `BCRYPT_COST` (default 10).
- Session `_id` scheme (must match botplatform): `base64.StdEncoding(sha256(rawToken))`.
- Each integration test package: `func TestMain(m *testing.M){ testutil.RunTests(m) }`; containers from `pkg/testutil`; `-race` always (Makefile handles it).
- Commit trailers per repo `CLAUDE.md`/git rules; never mention model identity in artifacts.

---

## File Structure

**New**
- `pkg/sessiontoken/sessiontoken.go` (+ `_test.go`) — `New()` / `Hash()`.
- `pkg/errcode/codes_admin.go` — admin reasons.
- `admin-service/{main.go,config.go,handler.go,routes.go,middleware.go,store.go,store_mongo.go}`
- `admin-service/{config_test.go,handler_test.go,middleware_test.go,integration_test.go,mock_store_test.go}`
- `admin-service/deploy/{Dockerfile,docker-compose.yml,azure-pipelines.yml}`

**Modify**
- `pkg/model/user.go` — add `Deactivated` field (+ model test).
- `botplatform-service/store_mongo.go` (or wherever the local hash/token helper lives) — use `pkg/sessiontoken`; reject deactivated accounts at login; `me.active = !Deactivated`.
- `room-service/handler.go` — admin override in `addMembers`/`removeMember`/`updateRole` (+ tests).
- `room-worker/*` — only if it re-checks ownership (audit; mirror override + tests).
- `docs/client-api.md` — admin-service endpoints + behavior note on room RPCs.

---

### Task 1: `pkg/sessiontoken` shared token primitives

**Files:**
- Create: `pkg/sessiontoken/sessiontoken.go`
- Test: `pkg/sessiontoken/sessiontoken_test.go`

**Interfaces:**
- Produces: `func New() (string, error)` (43-char base64url raw token); `func Hash(raw string) string` (44-char std-base64 of sha256).

- [ ] **Step 1: Write failing tests**

```go
package sessiontoken

import (
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew_LengthAndCharset(t *testing.T) {
	tok, err := New()
	require.NoError(t, err)
	assert.Len(t, tok, 43) // 32 bytes, RawURLEncoding
	_, err = base64.RawURLEncoding.DecodeString(tok)
	assert.NoError(t, err)
}

func TestNew_Unique(t *testing.T) {
	a, _ := New()
	b, _ := New()
	assert.NotEqual(t, a, b)
}

func TestHash_DeterministicAndKnownScheme(t *testing.T) {
	// Golden: base64.StdEncoding(sha256("token")) — must match botplatform's prior scheme.
	assert.Equal(t, "Ym9HOFh1eGFCWHkxNGRPL3BUWG9TVVZneU5tT2pUZ2NMZkZyVklXTGRCYz0=", "")[:0] // placeholder guard removed below
	h1 := Hash("token")
	h2 := Hash("token")
	assert.Equal(t, h1, h2)
	assert.Len(t, h1, 44)
}
```

> Replace the golden assertion: compute the expected value once with the real
> botplatform helper output for input `"token"` and assert equality (see Step 3),
> so this test pins byte-compatibility with existing stored session ids.

- [ ] **Step 2: Run, verify fail**

Run: `make test SERVICE=../pkg/sessiontoken` (or `go test ./pkg/sessiontoken/...` via the Makefile pkg path)
Expected: FAIL (package/functions undefined).

- [ ] **Step 3: Implement**

```go
// Package sessiontoken generates and hashes opaque session tokens shared by
// botplatform-service (issuer) and admin-service (validator). The hashing scheme
// is the stored sessions._id and MUST stay byte-identical across services.
package sessiontoken

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
)

// New returns a 43-char base64url (RawURLEncoding) token from 32 random bytes.
func New() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate session token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// Hash maps a raw token to its stored sessions._id: base64.StdEncoding(sha256(raw)).
func Hash(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return base64.StdEncoding.EncodeToString(sum[:])
}
```

Then pin the golden in the test: set the expected `Hash("token")` to the literal
`base64.StdEncoding(sha256("token"))` value and remove the placeholder guard line.

- [ ] **Step 4: Run, verify pass**

Run: `go test ./pkg/sessiontoken/... -race`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/sessiontoken
git commit -m "feat(pkg/sessiontoken): shared session token gen + hash"
```

---

### Task 2: Refactor botplatform to use `pkg/sessiontoken`

**Files:**
- Modify: `botplatform-service/store_mongo.go` (and any helper file defining the local raw-token/hash funcs — grep `sha256`/`RawURLEncoding` in `botplatform-service/`).

**Interfaces:**
- Consumes: `sessiontoken.New`, `sessiontoken.Hash`.

- [ ] **Step 1: Locate the local helpers**

Run: `rg -n "RawURLEncoding|sha256|func .*[Tt]oken|func .*[Hh]ash" botplatform-service`
Identify the raw-token generator and the hash func used for `session._id`.

- [ ] **Step 2: Replace bodies with the shared calls**

Swap the local raw-token generation for `sessiontoken.New()` and the local hashing for `sessiontoken.Hash(raw)`. Delete the now-dead local funcs. Add the import `"github.com/hmchangw/chat/pkg/sessiontoken"`.

- [ ] **Step 3: Run botplatform tests (unchanged behavior)**

Run: `make test SERVICE=botplatform-service`
Expected: PASS (login/validate/change-pwd tests unchanged — byte-compatible hash).

- [ ] **Step 4: Commit**

```bash
git add botplatform-service pkg/sessiontoken
git commit -m "refactor(botplatform-service): use pkg/sessiontoken for token gen/hash"
```

---

### Task 3: `pkg/model` `Deactivated` field

**Files:**
- Modify: `pkg/model/user.go`
- Test: `pkg/model/model_test.go` (extend the round-trip coverage)

**Interfaces:**
- Produces: `User.Deactivated bool` (json/bson `deactivated,omitempty`).

- [ ] **Step 1: Write failing test**

In `pkg/model/model_test.go`, add a case asserting round-trip of a `User` with `Deactivated: true` (use the existing `roundTrip` helper) and that the bson tag is `deactivated`.

```go
func TestUser_DeactivatedRoundTrip(t *testing.T) {
	u := User{ID: "u1", Account: "alice", SiteID: "site-local", Deactivated: true}
	got := roundTrip(t, u)
	assert.True(t, got.Deactivated)
}
```

- [ ] **Step 2: Run, verify fail** — `make test SERVICE=../pkg/model` → FAIL (field missing).

- [ ] **Step 3: Implement** — add to `User` struct (place near `RequirePasswordChange`):

```go
	Deactivated bool `json:"deactivated,omitempty" bson:"deactivated,omitempty"`
```

- [ ] **Step 4: Run, verify pass** — `go test ./pkg/model/... -race` → PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/model
git commit -m "feat(pkg/model): add User.Deactivated"
```

---

### Task 4: botplatform login rejects deactivated accounts + truthful `me.active`

**Files:**
- Modify: `botplatform-service/handler.go` (login path), `store_mongo.go` (ensure `deactivated` is projected by `FindUserByAccount`).
- Test: `botplatform-service/handler_test.go`

**Interfaces:**
- Consumes: `model.User.Deactivated`.

- [ ] **Step 1: Write failing test** — in `handler_test.go`, a deactivated admin/bot account returns uniform `401 invalid_credentials`; an active account's response has `me.active == true`; a deactivated one (if it could reach response) would be `false`.

```go
func TestLogin_DeactivatedAccountRejected(t *testing.T) {
	// store returns an admin user with Deactivated:true and a valid bcrypt for the password
	// expect 401 invalid_credentials (uniform), no session inserted.
}
```

- [ ] **Step 2: Run, verify fail** — `make test SERVICE=botplatform-service` → FAIL.

- [ ] **Step 3: Implement** — after the role/site/password checks pass, add:
`if user.Deactivated { /* run bcrypt-dummy already done; */ return errcode...invalid_credentials }`
(place the check so timing parity with other 401s is preserved — i.e. after password verify, mirroring the existing uniform-rejection pattern). Set the `me.active` field from `!user.Deactivated`. Ensure `FindUserByAccount`'s projection includes `deactivated`.

- [ ] **Step 4: Run, verify pass** — PASS.

- [ ] **Step 5: Commit**

```bash
git add botplatform-service
git commit -m "feat(botplatform-service): reject deactivated accounts at login; me.active reflects state"
```

---

### Task 5: `pkg/errcode/codes_admin.go`

**Files:**
- Create: `pkg/errcode/codes_admin.go`

**Interfaces:**
- Produces: `errcode.AdminNotAuthorized`, `AdminInvalidToken`, `AdminUserNotFound`, `AdminAccountExists` (type `Reason`).

- [ ] **Step 1: Implement (mirror `codes_botplatform.go`)**

```go
package errcode

// Admin-service reasons. Emitted by admin-service handlers/middleware.
const (
	AdminNotAuthorized Reason = "not_admin"       // 403: valid session, role != admin
	AdminInvalidToken  Reason = "invalid_token"   // 401: missing/unknown session token
	AdminUserNotFound  Reason = "user_not_found"  // 404
	AdminAccountExists Reason = "account_exists"  // 409: duplicate account on create
)
```

- [ ] **Step 2: Build** — `go build ./pkg/errcode/...` → OK.

- [ ] **Step 3: Commit**

```bash
git add pkg/errcode/codes_admin.go
git commit -m "feat(pkg/errcode): admin-service reason codes"
```

---

### Task 6: admin-service config + store interface + mock

**Files:**
- Create: `admin-service/config.go`, `admin-service/store.go`
- Test: `admin-service/config_test.go`
- Generated: `admin-service/mock_store_test.go`

**Interfaces:**
- Produces: `Config` struct; `AdminStore` interface (per spec §1.5); `Session` struct; `UserUpdate` struct; sentinels `ErrUserNotFound`, `ErrAccountExists`.

- [ ] **Step 1: Write failing config test**

```go
func TestLoadConfig_Defaults(t *testing.T) {
	t.Setenv("SITE_ID", "site-local")
	t.Setenv("MONGO_URI", "mongodb://x")
	cfg, err := loadConfig()
	require.NoError(t, err)
	assert.Equal(t, "8082", cfg.Port)
	assert.Equal(t, "chat", cfg.MongoDB)
	assert.Equal(t, 10, cfg.BcryptCost)
}

func TestLoadConfig_RequiresSiteAndMongo(t *testing.T) {
	_, err := loadConfig()
	assert.Error(t, err)
}
```

- [ ] **Step 2: Run, verify fail** — `make test SERVICE=admin-service` → FAIL.

- [ ] **Step 3: Implement `config.go`**

```go
package main

import "github.com/caarlos0/env/v11"

type Config struct {
	Port          string `env:"PORT" envDefault:"8082"`
	SiteID        string `env:"SITE_ID,required"`
	MongoURI      string `env:"MONGO_URI,required"`
	MongoDB       string `env:"MONGO_DB" envDefault:"chat"`
	MongoUsername string `env:"MONGO_USERNAME"`
	MongoPassword string `env:"MONGO_PASSWORD"`
	BcryptCost    int    `env:"BCRYPT_COST" envDefault:"10"`
	DevMode       bool   `env:"DEV_MODE" envDefault:"false"`
}

func loadConfig() (Config, error) {
	var c Config
	if err := env.Parse(&c); err != nil {
		return Config{}, err
	}
	return c, nil
}
```

- [ ] **Step 4: Implement `store.go`** (interface + types + `//go:generate`)

```go
package main

import (
	"context"
	"errors"

	"github.com/hmchangw/chat/pkg/model"
)

var (
	ErrUserNotFound  = errors.New("user not found")
	ErrAccountExists = errors.New("account exists")
)

// Session mirrors the botplatform-issued session row (read/write here).
type Session struct {
	ID       string   `bson:"_id"`
	UserID   string   `bson:"userId"`
	Account  string   `bson:"account"`
	SiteID   string   `bson:"siteId"`
	Roles    []string `bson:"roles"`
	IssuedAt int64    `bson:"issuedAt"`
}

// UserUpdate carries optional account-management edits (nil = leave unchanged).
type UserUpdate struct {
	EngName     *string
	ChineseName *string
	Roles       *[]model.UserRole
	Deactivated *bool
}

// AuditEntry records one mutating admin action. Details holds non-secret context
// only — never passwords, hashes, or tokens.
type AuditEntry struct {
	ID            string            `bson:"_id"`
	ActorUserID   string            `bson:"actorUserId"`
	ActorAccount  string            `bson:"actorAccount"`
	Action        string            `bson:"action"`
	TargetUserID  string            `bson:"targetUserId,omitempty"`
	TargetAccount string            `bson:"targetAccount,omitempty"`
	Details       map[string]string `bson:"details,omitempty"`
	SiteID        string            `bson:"siteId"`
	Timestamp     int64             `bson:"timestamp"`
}

// AuditFilter narrows an audit listing; zero-value fields are ignored.
type AuditFilter struct {
	TargetUserID string
	Actor        string
	Action       string
}

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

type AdminStore interface {
	SearchUsers(ctx context.Context, siteID, q string, page, limit int) ([]model.User, int64, error)
	GetUserByID(ctx context.Context, id string) (*model.User, error)
	GetUserByAccount(ctx context.Context, siteID, account string) (*model.User, error)
	CreateUser(ctx context.Context, u *model.User) error
	UpdateUser(ctx context.Context, id string, fields UserUpdate) error
	UpdateUserPassword(ctx context.Context, id, bcryptHash string, requireChange bool) error

	FindSessionByHash(ctx context.Context, hash string) (*Session, error)
	ListSessionsByUser(ctx context.Context, userID string) ([]Session, error)
	DeleteSessionsByUser(ctx context.Context, userID string) (int64, error)
	DeleteSession(ctx context.Context, sessionID string) (int64, error)

	AppendAudit(ctx context.Context, e *AuditEntry) error
	ListAudit(ctx context.Context, siteID string, f AuditFilter, page, limit int) ([]AuditEntry, int64, error)

	EnsureIndexes(ctx context.Context) error
	Ping(ctx context.Context) error
}
```

- [ ] **Step 5: Generate mock + verify config tests pass**

Run: `make generate SERVICE=admin-service && make test SERVICE=admin-service`
Expected: mock created; config tests PASS.

- [ ] **Step 6: Commit**

```bash
git add admin-service/config.go admin-service/config_test.go admin-service/store.go admin-service/mock_store_test.go
git commit -m "feat(admin-service): config + store interface + mock"
```

---

### Task 7: `requireAdmin` middleware

**Files:**
- Create: `admin-service/middleware.go`
- Test: `admin-service/middleware_test.go`

**Interfaces:**
- Consumes: `AdminStore.FindSessionByHash`, `sessiontoken.Hash`, `errcode`.
- Produces: `func requireAdmin(store AdminStore) gin.HandlerFunc`; context key for the principal (`ctxPrincipal`), accessor `principalFrom(c) Session`.

- [ ] **Step 1: Write failing table test** — cases: no bearer → 401 `invalid_token`; unknown hash → 401 `invalid_token`; session without admin role → 403 `not_admin`; admin session → next handler runs, principal in context. Use the generated `MockAdminStore` and `gin.CreateTestContext`.

- [ ] **Step 2: Run, verify fail** — FAIL.

- [ ] **Step 3: Implement**

```go
package main

import (
	"net/http"
	"slices"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errhttp"
	"github.com/hmchangw/chat/pkg/sessiontoken"
)

const ctxPrincipal = "adminPrincipal"

func bearer(c *gin.Context) string {
	h := c.GetHeader("Authorization")
	if after, ok := strings.CutPrefix(h, "Bearer "); ok {
		return strings.TrimSpace(after)
	}
	return ""
}

func requireAdmin(store AdminStore) gin.HandlerFunc {
	return func(c *gin.Context) {
		tok := bearer(c)
		if tok == "" {
			errhttp.Write(c.Request.Context(), c, errcode.Unauthenticated("missing session token", errcode.WithReason(errcode.AdminInvalidToken)))
			c.Abort()
			return
		}
		sess, err := store.FindSessionByHash(c.Request.Context(), sessiontoken.Hash(tok))
		if err != nil {
			errhttp.Write(c.Request.Context(), c, errcode.Unauthenticated("invalid session token", errcode.WithReason(errcode.AdminInvalidToken)))
			c.Abort()
			return
		}
		if !slices.Contains(sess.Roles, string(model_UserRoleAdmin)) {
			errhttp.Write(c.Request.Context(), c, errcode.Forbidden("admin role required", errcode.WithReason(errcode.AdminNotAuthorized)))
			c.Abort()
			return
		}
		c.Set(ctxPrincipal, *sess)
		c.Next()
	}
}

func principalFrom(c *gin.Context) Session { v, _ := c.Get(ctxPrincipal); s, _ := v.(Session); return s }
```

> Use the real constant `model.UserRoleAdmin` (string `"admin"`); the `model_UserRoleAdmin`
> token above is shorthand — import `pkg/model` and write `string(model.UserRoleAdmin)`.
> Note: `FindSessionByHash` must return a non-nil error on `mongo.ErrNoDocuments` so the
> not-found path maps to 401.

- [ ] **Step 4: Run, verify pass** — PASS.

- [ ] **Step 5: Commit**

```bash
git add admin-service/middleware.go admin-service/middleware_test.go
git commit -m "feat(admin-service): requireAdmin session-token middleware"
```

---

### Task 8: User-management handlers

**Files:**
- Create: `admin-service/handler.go`
- Test: `admin-service/handler_test.go`

**Interfaces:**
- Consumes: `AdminStore`, `Config`, `principalFrom`, `bcrypt`/`sha256`, `errcode`/`errhttp`, `idgen`.
- Produces: `type Handler struct{ store AdminStore; cfg Config }`, `NewHandler(...)`; methods `listUsers`, `getUser`, `createUser`, `updateUser`, `setPassword`.

- [ ] **Step 1: Write failing table tests** (mocked store), covering per spec §8:
  - `createUser`: happy (201, projected, no bcrypt in JSON); empty fields → 400 `missing_fields`; duplicate → 409 `account_exists`; `siteId` forced to `cfg.SiteID`; `requirePasswordChange` default true; password stored as `bcrypt(sha256_hex(pw))` (assert the store received a hash that verifies).
  - `listUsers`: passes `cfg.SiteID`, `q`, paging to store; returns projected list + total.
  - `getUser`: hit; miss → 404 `user_not_found`.
  - `updateUser`: roles/deactivated/names; deactivating calls `DeleteSessionsByUser`.
  - `setPassword`: hashes, sets requireChange, calls `DeleteSessionsByUser`; empty → 400.

- [ ] **Step 2: Run, verify fail** — FAIL.

- [ ] **Step 3: Implement** — `handler.go`. Helper for password:

```go
func hashPassword(plaintext string, cost int) (string, error) {
	sum := sha256.Sum256([]byte(plaintext))
	hexd := hex.EncodeToString(sum[:])
	b, err := bcrypt.GenerateFromPassword([]byte(hexd), cost)
	if err != nil {
		return "", fmt.Errorf("hash password: %w", err)
	}
	return string(b), nil
}
```

Implement each handler binding its request struct via Gin, validating non-empty required fields (→ `errcode.BadRequest(..., WithReason(errcode.AuthMissingFields))`), forcing `SiteID = cfg.SiteID` on create, generating `_id` via `idgen.GenerateUUIDv7()` (match the users-collection id convention used elsewhere — confirm via `rg "idgen\." user-service botplatform-service`), defaulting `RequirePasswordChange` to true when the body omits it, and returning a `userView` projection struct (no `Services`). On `updateUser` with `Deactivated==true` and on `setPassword`, call `store.DeleteSessionsByUser`. Map `ErrAccountExists`→409, `ErrUserNotFound`→404 via `errcode`. Reply success with `c.JSON`.

**Audit write (every mutating handler).** After the mutation succeeds, append an audit entry; a write failure is logged but not fatal. Add a helper and call it from `createUser`/`updateUser`/`setPassword` (and the session handlers in Task 9):

```go
func (h *Handler) audit(ctx context.Context, c *gin.Context, action, targetUserID, targetAccount string, details map[string]string) {
	p := principalFrom(c)
	e := &AuditEntry{
		ID:            idgen.GenerateUUIDv7(),
		ActorUserID:   p.UserID,
		ActorAccount:  p.Account,
		Action:        action,
		TargetUserID:  targetUserID,
		TargetAccount: targetAccount,
		Details:       details, // non-secret only — NEVER password/hash/token
		SiteID:        h.cfg.SiteID,
		Timestamp:     time.Now().UTC().UnixMilli(),
	}
	if err := h.store.AppendAudit(ctx, e); err != nil {
		slog.ErrorContext(ctx, "append audit entry failed", "action", action, "error", err)
	}
}
```

Actions: `user.create`, `user.update` (details may carry e.g. `{"deactivated":"true"}` or changed role set), `user.password.set`. Add a test asserting `createUser` triggers `AppendAudit` with `action="user.create"` and **no** password/hash in `details`.

- [ ] **Step 4: Run, verify pass** — PASS; check no `bcrypt`/`Services` field appears in any response (assert in tests).

- [ ] **Step 5: Commit**

```bash
git add admin-service/handler.go admin-service/handler_test.go
git commit -m "feat(admin-service): user-management handlers"
```

---

### Task 9: Session + audit handlers + routes + main

**Files:**
- Modify: `admin-service/handler.go` (add `listSessions`, `revokeAllSessions`, `revokeSession`, `listAudit`)
- Create: `admin-service/routes.go`, `admin-service/main.go`
- Test: `admin-service/handler_test.go` (session + audit cases)

**Interfaces:**
- Consumes: `AdminStore` session + audit methods, `requireAdmin`.
- Produces: `registerRoutes(r *gin.Engine, h *Handler, store AdminStore)`; HTTP server wiring with graceful shutdown.

- [ ] **Step 1: Write failing tests** — `listSessions` returns `[{id,userId,siteId,issuedAt}]` (no token/hash beyond the `_id`); `revokeAllSessions` calls `DeleteSessionsByUser` **and** audits `session.revoke_all`; `revokeSession` calls `DeleteSession` **and** audits `session.revoke`; `listAudit` passes `cfg.SiteID` + filters + paging to `store.ListAudit` and returns entries newest-first.

- [ ] **Step 2: Run, verify fail** — FAIL.

- [ ] **Step 3: Implement handlers + `routes.go`**

```go
package main

import "github.com/gin-gonic/gin"

func registerRoutes(r *gin.Engine, h *Handler, store AdminStore) {
	r.GET("/healthz", h.healthz)
	r.GET("/readyz", h.readyz)

	admin := r.Group("/v1/admin", requireAdmin(store))
	admin.GET("/users", h.listUsers)
	admin.POST("/users", h.createUser)
	admin.GET("/users/:id", h.getUser)
	admin.PATCH("/users/:id", h.updateUser)
	admin.POST("/users/:id/password", h.setPassword)
	admin.GET("/users/:id/sessions", h.listSessions)
	admin.DELETE("/users/:id/sessions", h.revokeAllSessions)
	admin.DELETE("/users/:id/sessions/:sessionId", h.revokeSession)
	admin.GET("/audit", h.listAudit)
}
```

Implement `healthz` (`{"status":"ok"}`) and `readyz` (`store.Ping` → 200/503). `revokeAllSessions`/`revokeSession` call `h.audit(...)` after deletion; `listAudit` binds `targetUserId`/`actor`/`action`/`page`/`limit` query params into an `AuditFilter` and calls `store.ListAudit(ctx, cfg.SiteID, filter, page, limit)`.

- [ ] **Step 4: Implement `main.go`** — load config, `mongoutil.Connect`, build `storeMongo` (Task 10), `EnsureIndexes`, gin engine (Recovery + a request-id/access-log middleware following `auth-service`/`botplatform-service` middleware — reuse `pkg/ginutil` if present, else mirror botplatform's `middleware.go`), `registerRoutes`, HTTP server with timeouts, `pkg/shutdown.Wait` (drain → disconnect Mongo).

- [ ] **Step 5: Run, verify pass** — `make test SERVICE=admin-service` PASS; `make build SERVICE=admin-service` OK.

- [ ] **Step 6: Commit**

```bash
git add admin-service/handler.go admin-service/routes.go admin-service/main.go admin-service/handler_test.go
git commit -m "feat(admin-service): session handlers, routes, main wiring"
```

---

### Task 10: Mongo store implementation + integration tests + deploy

**Files:**
- Create: `admin-service/store_mongo.go`, `admin-service/integration_test.go`, `admin-service/deploy/{Dockerfile,docker-compose.yml,azure-pipelines.yml}`

**Interfaces:**
- Consumes: `*mongo.Database`, `Config`.
- Produces: `type storeMongo struct{...}`, `func newStoreMongo(db *mongo.Database) *storeMongo` implementing `AdminStore`.

- [ ] **Step 1: Write failing integration tests** (`//go:build integration`, `TestMain` → `testutil.RunTests`, DB via `testutil.MongoDB(t,"adminsvc")`):
  - `CreateUser` + unique-index enforced (second create same account → `ErrAccountExists`).
  - `SearchUsers` filters by `siteId`, matches `q`, paginates, returns total.
  - `GetUserByID` hit/miss (`ErrUserNotFound`).
  - `UpdateUser` roles/deactivated/names; `UpdateUserPassword` sets hash + clears `requirePasswordChange`.
  - sessions: insert rows (direct), `FindSessionByHash` hit/miss, `ListSessionsByUser`, `DeleteSessionsByUser`, `DeleteSession` counts.
  - audit: `AppendAudit` then `ListAudit` filtered by `targetUserId`/`actor`/`action`, paginated, newest-first by `timestamp`, site-scoped.
  - `EnsureIndexes` idempotent.

- [ ] **Step 2: Run, verify fail** — `make test-integration SERVICE=admin-service` → FAIL.

- [ ] **Step 3: Implement `store_mongo.go`** — collections `users`, `sessions`, `admin_audit`. Every find uses an explicit projection. `SearchUsers`: filter `{siteId, $or:[account/engName/chineseName regex]}`, projection of the management fields, `Skip/Limit`, plus `CountDocuments`. `CreateUser`: `InsertOne`, map duplicate-key → `ErrAccountExists`. `UpdateUser`/`UpdateUserPassword`: `$set` only provided fields; password update also `$set requirePasswordChange` and `$unset`/`$set` accordingly. Session methods operate on `sessions`. `AppendAudit`: `InsertOne` into `admin_audit`. `ListAudit`: filter `{siteId, +optional targetUserId/actorAccount/action}`, sort `{timestamp:-1}`, `Skip/Limit` + `CountDocuments`. `EnsureIndexes`: unique index on `users(account)` (or `{account:1}` unique scoped appropriately) — match botplatform's existing index to avoid conflicts; create `sessions(userId,issuedAt)` only if not already present (botplatform owns it; creating idempotently is safe); create `admin_audit(siteId,timestamp)` and `admin_audit(targetUserId,timestamp)`. `Ping`: `db.Client().Ping`.

- [ ] **Step 4: Run, verify pass** — `make test-integration SERVICE=admin-service` PASS.

- [ ] **Step 5: Deploy files** — mirror `botplatform-service/deploy/*`: multi-stage Dockerfile (build context repo root), `docker-compose.yml` joining local Mongo with `MONGO_DB=chat`, `SITE_ID=site-local`, `PORT=8082`, `BCRYPT_COST=10`; `azure-pipelines.yml` cloned from a sibling service.

- [ ] **Step 6: Commit**

```bash
git add admin-service/store_mongo.go admin-service/integration_test.go admin-service/deploy
git commit -m "feat(admin-service): mongo store, integration tests, deploy"
```

---

### Task 11: room-service admin override

**Files:**
- Modify: `room-service/handler.go`
- Test: `room-service/handler_test.go`

**Interfaces:**
- Consumes: `store.GetUser`, `model.IsPlatformAdmin`.
- Produces: `func (h *Handler) isPlatformAdmin(ctx, account) (bool, error)`.

- [ ] **Step 1: Write failing tests** — for `addMembers`, `removeMember`, `updateRole`: an actor that is NOT an owner but IS a platform admin (mock `GetUser` returns roles `["admin"]`) succeeds; a non-owner non-admin still gets the existing forbidden error; admin still gets the genuine not-found/not-member error when the target room/member is absent.

- [ ] **Step 2: Run, verify fail** — `make test SERVICE=room-service` → FAIL.

- [ ] **Step 3: Implement** — add the helper:

```go
func (h *Handler) isPlatformAdmin(ctx context.Context, account string) (bool, error) {
	u, err := h.store.GetUser(ctx, account)
	if err != nil {
		return false, fmt.Errorf("load actor for admin check: %w", err)
	}
	return model.IsPlatformAdmin(u), nil
}
```

In each gate, compute `admin, err := h.isPlatformAdmin(ctx, requester)` (handle err), and change `if !isOwner {return errOnly...}` to `if !isOwner && !admin {return errOnly...}`. Keep all not-found/not-member precondition errors before the ownership branch so admins still fail on genuinely missing targets. Confirm `h.store` already exposes `GetUser` (the extract confirms it does); if the room-service store interface lacks it on this path, add it + regenerate the mock.

- [ ] **Step 4: Run, verify pass** — PASS. Run `make generate SERVICE=room-service` if the store interface changed.

- [ ] **Step 5: Commit**

```bash
git add room-service
git commit -m "feat(room-service): platform-admin override for member/role ops"
```

---

### Task 12: room-worker audit + docs

**Files:**
- Audit: `room-worker/*.go`; Modify only if a duplicate ownership gate exists.
- Modify: `docs/client-api.md`

- [ ] **Step 1: Audit room-worker** — `rg -n "RoomNotOwner|RoomNotMember|hasRole|owner" room-worker`. If the async apply path re-enforces ownership, replicate the Task 11 admin bypass with a test; if it performs no permission check, record "room-worker carries no duplicate gate; no change needed" in the commit message.

- [ ] **Step 2: (If needed) implement + test the worker bypass**, mirroring Task 11. Run `make test SERVICE=room-worker`.

- [ ] **Step 3: Update `docs/client-api.md`** — add an admin-service subsection (the `/v1/admin/users*`, `/v1/admin/users/:id/sessions*`, and `/v1/admin/audit` endpoints: request/response field tables, JSON examples, error tables in current style); add `not_admin` and `account_exists` to the §6 reason catalog; add a one-line behavior note to the existing `addMembers`/`removeMember`/`updateRole` entries that platform admins bypass owner checks.

- [ ] **Step 4: Verify gates** — `make lint && make test && make sast`. Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add room-worker docs/client-api.md
git commit -m "feat(room-worker): admin override audit; docs: admin-service API + room admin note"
```

---

## Self-Review

- **Spec coverage:** §1 admin-service → Tasks 6–10; §1.1 middleware → Task 7; §1.2 users → Task 8; §1.3 sessions → Task 9; §1.3a audit log → Tasks 6 (types/store), 8–9 (writes + list endpoint), 10 (store impl + integration); §2 model+botplatform → Tasks 3–4; §3 room override → Tasks 11–12; §4 sessiontoken → Tasks 1–2; §1.7 errcode → Task 5; §7 docs → Task 12; §8 testing folded into each task. Frontend (§5) is deliberately the separate Plan 2.
- **Placeholder scan:** the one golden-value and the `model_UserRoleAdmin`/`GetUser`-on-store notes are explicit "resolve at implementation" callouts with the exact resolution stated, not vague TODOs.
- **Type consistency:** `AdminStore` method names/signatures defined in Task 6 are used verbatim in Tasks 7–10; `Session`/`UserUpdate` consistent; `sessiontoken.New/Hash` consistent across Tasks 1/2/7; `model.IsPlatformAdmin`/`User.Deactivated` consistent across Tasks 3/4/11.

## Open item

Room admin-override (Tasks 11–12) implements **full override** per the approved spec. If the team later restricts admin room power, drop those two tasks; nothing else depends on them.
