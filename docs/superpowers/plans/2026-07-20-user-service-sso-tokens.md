# user-service SSO Token Endpoints Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Two NATS request/reply endpoints in user-service — `sso.set` (admin-only: verify + store a user's SSO token pair) and `sso.refresh` (self + admin-for-others: return the stored `ssoToken`, transparently refreshing it against Keycloak when near expiry) — backed by the `sso_tokens` Mongo collection (legacy field names kept).

**Architecture:** All OIDC mechanics (verify + refresh grant) live in the shared `pkg/oidc` (additive changes only; auth-service untouched). user-service gains a new Mongo repo over `sso_tokens`, two handler methods behind consumer-defined interfaces, and an optional OIDC config block — issuer unset ⇒ endpoints reply `unavailable`. A new `natsrouter.RegisterOptionalBody` variant lets `sso.refresh` accept an empty payload.

**Tech Stack:** Go 1.25, NATS core request/reply (`pkg/natsrouter`), MongoDB (`mongo-driver/v2` via `pkg/mongoutil`), coreos/go-oidc v3 (`pkg/oidc`), mockgen, testify, testcontainers (`pkg/testutil`).

**Spec:** `docs/superpowers/specs/2026-07-20-user-service-sso-tokens-design.md` — read it before starting any task.

## Global Constraints

- All commands via `make` targets — never raw `go` commands (`make test SERVICE=user-service`, `make generate SERVICE=user-service`, `make lint`, `make fmt`, `make sast`, `make test-integration SERVICE=user-service`).
- TDD Red-Green-Refactor for every task: write the failing test, SEE it fail, implement, SEE it pass, commit. Coverage: 80% floor, 90%+ target (handlers, store, `pkg/` are all core logic).
- Never log or wrap token material (`ssoToken`, `refreshToken`, request bodies) into messages, `errcode.WithCause` on client-visible paths is fine ONLY for non-token errors (verification errors from go-oidc contain no token bytes — auth-service precedent). Log only `account`, expiry values, request IDs.
- Wire naming: `ssoToken` (camelCase). DB field names are legacy: `username`, `idToken`, `idTokenExp`, `refreshToken`, `_updatedAt` in collection `sso_tokens`.
- Vault token type: the platform `ssoToken` = Keycloak **access token**. Refresh stores the response's `access_token`, never `id_token`.
- Errors: Tier-1 errcode constructors returned from handlers; the router replies. Never `slog.Error` + return the same error (double-log).
- New third-party dependencies are forbidden (no `x/oauth2` promotion; the oidc test harness hand-rolls RS256 with stdlib `crypto/rsa`).
- Every commit message: imperative, conventional-commit style. Lint + unit tests are enforced by pre-commit hook.
- Branch: `feat/user-service-sso-token-endpoints`. Do NOT create a PR.

---

### Task 1: Spec corrections (docs-only)

Two factual errors surfaced after the spec was committed. Fix them so later tasks don't inherit them.

**Files:**
- Modify: `docs/superpowers/specs/2026-07-20-user-service-sso-tokens-design.md`

**Interfaces:** none (docs).

- [ ] **Step 1: Fix the interface-assertion claim (§4, last paragraph)**

Replace the sentence:

> Interface conformance is enforced the same way as the existing repos — by passing the concrete repo to `service.New(...)` in `main.go` (no `var _` assertion blocks exist in this service; none are introduced).

with:

> Interface conformance is enforced by the two existing compile-time assertion blocks — `user-service/main.go:24-34` (`var ( _ service.UserRepository = (*mongorepo.UserRepo)(nil) … )`) and `user-service/mongorepo/setup_test.go:17-21` (the `go vet -tags integration` guard). `SSOTokenRepository` is added to **both** blocks.

- [ ] **Step 2: Fix the `upstream_unavailable` constant name (§8 table, last-but-one row)**

The reason constant is `BotplatformUpstreamUnavailable` (in `pkg/errcode/codes_botplatform.go:27`), not `AuthUpstreamUnavailable`. Change the row's parenthetical to `(BotplatformUpstreamUnavailable, auth-service BOTPLATFORM_URL-unset precedent)`.

- [ ] **Step 3: Resolve the spec §4 open item — legacy `idTokenExp` type (CONFIRMED: string)**

The product owner confirmed the legacy `idTokenExp` is stored as a **string** (the collection is written by a legacy app that upserts by `username`, so usernames are unique too — no dedup concern for the Task 11 unique index). Resolution baked into the plan:
- **Persistence is a string.** The repo (Task 11) stores `idTokenExp` as a decimal-millis string (`strconv.FormatInt`) and reads it via a repo-local doc whose `idTokenExp` is `string`, parsing to `int64` millis. A non-numeric/odd legacy value parses to `0`, which reads as "expired" and safely triggers a refresh (self-healing) rather than erroring.
- **In-memory / service layer stays `int64` millis** (`model.SSOToken.IDTokenExp int64`), so handlers and their tests never deal with the string form — the string↔int64 conversion is confined to the repo boundary.

Update spec §4's `idTokenExp` row to: "stored as a decimal-millis **string** (legacy schema); repo converts to `int64` millis on read (non-numeric ⇒ 0 ⇒ refresh); new writes are `strconv.FormatInt(exp.UnixMilli())`." Remove the ⚠️ open-item marker.

- [ ] **Step 4: Commit**

```bash
git add docs/superpowers/specs/2026-07-20-user-service-sso-tokens-design.md
git commit -m "docs: correct assertion-block and errcode-constant facts, resolve idTokenExp BSON-type open item"
```

---

### Task 2: `pkg/model` — SSOToken domain type

**Files:**
- Create: `pkg/model/ssotoken.go`
- Test: `pkg/model/model_test.go` (append)

**Interfaces:**
- Produces: `model.SSOToken{ID string, Username string, IDToken string, IDTokenExp int64, RefreshToken string, UpdatedAt time.Time}` with `String()` redaction. Consumed by Tasks 11–13.

- [ ] **Step 1: Write the failing tests** — append to `pkg/model/model_test.go`:

```go
func TestSSOTokenJSON(t *testing.T) {
	// Secrets carry json:"-" so a src with them unset round-trips cleanly.
	src := model.SSOToken{
		ID:         "abc123",
		Username:   "alice",
		IDTokenExp: 1735689600000,
		UpdatedAt:  time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC),
	}
	var dst model.SSOToken
	roundTrip(t, &src, &dst)
}

func TestSSOToken_SecretsNeverSerialize(t *testing.T) {
	tok := model.SSOToken{
		ID: "abc123", Username: "alice",
		IDToken: "SECRET-ACCESS-TOKEN", RefreshToken: "SECRET-REFRESH-TOKEN",
	}
	data, err := json.Marshal(tok)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(data), "SECRET") {
		t.Errorf("token material leaked into JSON: %s", data)
	}
	if s := tok.String(); strings.Contains(s, "SECRET") {
		t.Errorf("token material leaked into String(): %s", s)
	}
}
```

Add `"strings"` and `"time"` to the test file's imports if not present.

- [ ] **Step 2: Run tests, verify they fail**

Run: `make test SERVICE=pkg/model` — the Makefile `test` target expands to `go test -race ./$(SERVICE)/...`, so a pkg path works.
Expected: FAIL — `undefined: model.SSOToken`.

- [ ] **Step 3: Implement** — create `pkg/model/ssotoken.go`:

```go
package model

import (
	"fmt"
	"time"
)

// SSOToken is one user's stored SSO token pair in the legacy
// sso_tokens collection (field names kept verbatim for backward
// compatibility with migrated documents). IDToken holds the platform
// "ssoToken" — the Keycloak ACCESS token, despite the legacy field name.
// Secrets carry json:"-" so no outbound payload can ever include them
// (precedent: PasswordCredentials).
type SSOToken struct {
	ID           string    `json:"id"          bson:"_id"`
	Username     string    `json:"username"    bson:"username"`
	IDToken      string    `json:"-"           bson:"idToken"`
	IDTokenExp   int64     `json:"idTokenExp"  bson:"idTokenExp"` // in-memory epoch millis; PERSISTED as a decimal string (legacy schema) — the repo (Task 11) converts at the boundary
	RefreshToken string    `json:"-"           bson:"refreshToken"`
	UpdatedAt    time.Time `json:"updatedAt"   bson:"_updatedAt"`
}

// String formats an SSOToken for log lines, deliberately omitting both token
// values so a stray %v / %+v / structured log call never carries credential
// material to disk (precedent: User.String).
func (s SSOToken) String() string {
	return fmt.Sprintf("SSOToken{ID:%q Username:%q IDTokenExp:%d}", s.ID, s.Username, s.IDTokenExp)
}
```

- [ ] **Step 4: Run tests, verify they pass**

Run: `make test SERVICE=pkg/model`. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/model/ssotoken.go pkg/model/model_test.go
git commit -m "feat(model): SSOToken domain type with secret-redacting serialization"
```

---

### Task 3: `pkg/errcode` — `sso_token_not_found` reason

**Files:**
- Modify: `pkg/errcode/codes_user.go`
- Test: `pkg/errcode/codes_test.go` (the `allReasons` list)

**Interfaces:**
- Produces: `errcode.UserSSOTokenNotFound Reason = "sso_token_not_found"`. Consumed by Task 13.

- [ ] **Step 1: Add the constant to BOTH test sites FIRST** — in `pkg/errcode/codes_test.go`, extend the `User*` line of `allReasons`:

```go
	UserAppNotFound, UserAppDisabled, UserInvalidDMTarget, UserSubscriptionNotFound, UserSSOTokenNotFound,
```

And add the wire-value assertion to the `cases` map in `pkg/errcode/codes_user_test.go`'s `TestUserReasons` (keeps the new reason covered like its siblings):

```go
		UserSSOTokenNotFound:     "sso_token_not_found",
```

- [ ] **Step 2: Run tests, verify compile failure**

Run: `make test` (errcode package). Expected: FAIL — `undefined: UserSSOTokenNotFound`.

- [ ] **Step 3: Implement** — in `pkg/errcode/codes_user.go`:

```go
const (
	UserAppNotFound          Reason = "app_not_found"
	UserAppDisabled          Reason = "app_disabled"
	UserInvalidDMTarget      Reason = "invalid_dm_target"
	UserSubscriptionNotFound Reason = "subscription_not_found"
	UserSSOTokenNotFound     Reason = "sso_token_not_found"
)
```

- [ ] **Step 4: Run tests, verify pass** (snake_case + uniqueness tests cover the new entry). Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/errcode/codes_user.go pkg/errcode/codes_test.go pkg/errcode/codes_user_test.go
git commit -m "feat(errcode): sso_token_not_found reason for user-service SSO vault"
```

---

### Task 4: `pkg/subject` — SSO subject builders

**Files:**
- Modify: `pkg/subject/subject.go` (user-service builders section, after `UserAppsCategories` ≈ line 1015; pattern builders after `UserAppsCategoriesPattern` ≈ line 1055)
- Test: `pkg/subject/subject_test.go` (append cases to `TestUserServiceBuilders` ≈ line 529, `TestUserServiceBuildersRejectWildcardAccounts` ≈ line 700, `TestUserServicePatternBuilders` ≈ line 820)

**Interfaces:**
- Produces (consumed by Tasks 12–15):
  - `subject.UserSSOSet(account, siteID string) string` → `chat.user.<account>.request.user.<siteID>.sso.set`
  - `subject.UserSSOSetPattern(siteID string) string` → `chat.user.{account}.request.user.<siteID>.sso.set`
  - `subject.UserSSORefresh(account, siteID string) string` → `…sso.refresh`
  - `subject.UserSSORefreshPattern(siteID string) string` → `chat.user.{account}.request.user.<siteID>.sso.refresh`

- [ ] **Step 1: Write the failing tests** — add table cases:

In `TestUserServiceBuilders`:
```go
		{"sso.set", subject.UserSSOSet("alice", "s1"), "chat.user.alice.request.user.s1.sso.set"},
		{"sso.refresh", subject.UserSSORefresh("alice", "s1"), "chat.user.alice.request.user.s1.sso.refresh"},
```
In `TestUserServiceBuildersRejectWildcardAccounts`:
```go
		{"UserSSOSet", func() { subject.UserSSOSet("*", "s1") }},
		{"UserSSORefresh", func() { subject.UserSSORefresh(">", "s1") }},
```
In `TestUserServicePatternBuilders`:
```go
		{"sso.set", subject.UserSSOSetPattern("s1"), "chat.user.{account}.request.user.s1.sso.set"},
		{"sso.refresh", subject.UserSSORefreshPattern("s1"), "chat.user.{account}.request.user.s1.sso.refresh"},
```

- [ ] **Step 2: Run tests, verify FAIL** (`undefined: subject.UserSSOSet`).

- [ ] **Step 3: Implement** — in `pkg/subject/subject.go`, after `UserAppsCategories` add:

```go
func UserSSOSet(account, siteID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.user.%s.sso.set", account, siteID)
}

func UserSSORefresh(account, siteID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.user.%s.sso.refresh", account, siteID)
}
```

After `UserAppsCategoriesPattern` add:

```go
func UserSSOSetPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.user.%s.sso.set", siteID)
}

func UserSSORefreshPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.user.%s.sso.refresh", siteID)
}
```

Do NOT touch `ParseUserSubject`'s area whitelist (zero production callers; `thread`/`me` precedent).

- [ ] **Step 4: Run tests, verify PASS.**

- [ ] **Step 5: Commit**

```bash
git add pkg/subject/subject.go pkg/subject/subject_test.go
git commit -m "feat(subject): sso.set / sso.refresh user-service subject builders"
```

---

### Task 5: `pkg/natsrouter` — RegisterOptionalBody

**Files:**
- Modify: `pkg/natsrouter/register.go`
- Test: `pkg/natsrouter/router_test.go` (append; reuse `startTestNATS`, `testReq`, `testResp` already defined there)

**Interfaces:**
- Produces: `natsrouter.RegisterOptionalBody[Req, Resp any](r *Router, pattern string, fn func(c *Context, req Req) (*Resp, error))` — zero-length payload ⇒ zero-value `Req`; non-empty ⇒ normal unmarshal; malformed ⇒ `bad_request`. Consumed by Task 13's registration.

- [ ] **Step 1: Write the failing tests** — append to `pkg/natsrouter/router_test.go`:

```go
func TestRegisterOptionalBody_EmptyPayloadYieldsZeroValue(t *testing.T) {
	nc := startTestNATS(t)
	r := New(nc, "test-service")

	RegisterOptionalBody(r, "chat.user.{account}.request.user.s1.opt.test",
		func(c *Context, req testReq) (*testResp, error) {
			return &testResp{Greeting: "name=" + req.Name}, nil
		})

	resp, err := nc.Request(context.Background(), "chat.user.alice.request.user.s1.opt.test", nil, 2*time.Second)
	require.NoError(t, err)
	var out testResp
	require.NoError(t, json.Unmarshal(resp.Data, &out))
	assert.Equal(t, "name=", out.Greeting)
}

func TestRegisterOptionalBody_NonEmptyPayloadUnmarshals(t *testing.T) {
	nc := startTestNATS(t)
	r := New(nc, "test-service")

	RegisterOptionalBody(r, "chat.user.{account}.request.user.s1.opt2.test",
		func(c *Context, req testReq) (*testResp, error) {
			return &testResp{Greeting: "name=" + req.Name}, nil
		})

	data, _ := json.Marshal(testReq{Name: "bob"})
	resp, err := nc.Request(context.Background(), "chat.user.alice.request.user.s1.opt2.test", data, 2*time.Second)
	require.NoError(t, err)
	var out testResp
	require.NoError(t, json.Unmarshal(resp.Data, &out))
	assert.Equal(t, "name=bob", out.Greeting)
}

func TestRegisterOptionalBody_MalformedPayloadIsBadRequest(t *testing.T) {
	nc := startTestNATS(t)
	r := New(nc, "test-service")

	RegisterOptionalBody(r, "chat.user.{account}.request.user.s1.opt3.test",
		func(c *Context, req testReq) (*testResp, error) {
			t.Fatal("handler must not run on malformed payload")
			return nil, nil
		})

	resp, err := nc.Request(context.Background(), "chat.user.alice.request.user.s1.opt3.test", []byte("{not json"), 2*time.Second)
	require.NoError(t, err)
	var envelope struct {
		Code string `json:"code"`
	}
	require.NoError(t, json.Unmarshal(resp.Data, &envelope))
	assert.Equal(t, "bad_request", envelope.Code)
}
```

- [ ] **Step 2: Run tests, verify FAIL** (`undefined: RegisterOptionalBody`).

- [ ] **Step 3: Implement** — append to `pkg/natsrouter/register.go` after `RegisterNoBody`:

```go
// RegisterOptionalBody subscribes a typed handler whose request body is
// optional: a zero-length payload yields the zero-value request (instead of
// Register's bad_request), a non-empty payload unmarshals normally. Use for
// endpoints whose request struct has only optional fields (e.g. sso.refresh).
func RegisterOptionalBody[Req, Resp any](
	r *Router,
	pattern string,
	fn func(c *Context, req Req) (*Resp, error),
) {
	handler := HandlerFunc(func(c *Context) {
		var req Req
		if len(c.Msg.Data) > 0 {
			if err := json.Unmarshal(c.Msg.Data, &req); err != nil {
				replyErr(c, errcode.BadRequest("invalid request payload", errcode.WithCause(err)))
				return
			}
		}

		resp, err := fn(c, req)
		if err != nil {
			replyErr(c, err)
			return
		}

		c.ReplyJSON(resp)
	})

	r.addRoute(pattern, []HandlerFunc{handler})
}
```

- [ ] **Step 4: Run tests, verify PASS.**

- [ ] **Step 5: Commit**

```bash
git add pkg/natsrouter/register.go pkg/natsrouter/router_test.go
git commit -m "feat(natsrouter): RegisterOptionalBody for endpoints with optional request bodies"
```

---

### Task 6: `pkg/oidc` — fake-issuer test harness + `Claims.Expiry`

The package currently has NO issuer harness (tests cover only `containsAudience`/`Account()`). Build it stdlib-only — hand-rolled RS256, **no new dependencies**.

**Files:**
- Create: `pkg/oidc/issuer_test.go` (harness)
- Modify: `pkg/oidc/oidc.go` (`Claims` gains `Expiry`; `Validate` fills it)
- Test: `pkg/oidc/oidc_test.go` (append `Validate` tests)

**Interfaces:**
- Produces for tests in Task 7: `newFakeIssuer(t) *fakeIssuer` with `.URL()`, `.Mint(overrides map[string]any) string`, settable `.TokenHandler http.HandlerFunc`.
- Produces for prod code: `Claims.Expiry time.Time` (consumed by Tasks 12–13 via `claims.Expiry.UnixMilli()`).

- [ ] **Step 1: Build the harness** — create `pkg/oidc/issuer_test.go`:

```go
package oidc

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeIssuer is a stdlib-only OIDC issuer: discovery, JWKS, RS256 minting,
// and a swappable token endpoint for refresh-grant tests.
type fakeIssuer struct {
	t            *testing.T
	key          *rsa.PrivateKey
	srv          *httptest.Server
	TokenHandler http.HandlerFunc // set per-test; 500s when nil
}

func newFakeIssuer(t *testing.T) *fakeIssuer {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	f := &fakeIssuer{t: t, key: key}

	mux := http.NewServeMux()
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)

	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, map[string]any{
			"issuer":                                f.srv.URL,
			"jwks_uri":                              f.srv.URL + "/keys",
			"token_endpoint":                        f.srv.URL + "/token",
			"id_token_signing_alg_values_supported": []string{"RS256"},
		})
	})
	mux.HandleFunc("/keys", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, map[string]any{"keys": []map[string]any{{
			"kty": "RSA", "alg": "RS256", "use": "sig", "kid": "test-key",
			"n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
			"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
		}}})
	})
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		if f.TokenHandler == nil {
			http.Error(w, "no token handler configured", http.StatusInternalServerError)
			return
		}
		f.TokenHandler(w, r)
	})
	return f
}

func (f *fakeIssuer) URL() string { return f.srv.URL }

// Mint signs an RS256 JWT with sane defaults; overrides replace/add claims
// (set a value to nil to delete a default claim).
func (f *fakeIssuer) Mint(overrides map[string]any) string {
	f.t.Helper()
	claims := map[string]any{
		"iss":                f.srv.URL,
		"aud":                "nats-chat",
		"sub":                "user-1",
		"preferred_username": "alice",
		"iat":                time.Now().Add(-time.Minute).Unix(),
		"exp":                time.Now().Add(time.Hour).Unix(),
	}
	for k, v := range overrides {
		if v == nil {
			delete(claims, k)
			continue
		}
		claims[k] = v
	}
	header := map[string]any{"alg": "RS256", "typ": "JWT", "kid": "test-key"}
	signing := b64JSON(f.t, header) + "." + b64JSON(f.t, claims)
	digest := sha256.Sum256([]byte(signing))
	sig, err := rsa.SignPKCS1v15(rand.Reader, f.key, crypto.SHA256, digest[:])
	require.NoError(f.t, err)
	return signing + "." + base64.RawURLEncoding.EncodeToString(sig)
}

func b64JSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return base64.RawURLEncoding.EncodeToString(b)
}

func writeJSON(t *testing.T, w http.ResponseWriter, v any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	require.NoError(t, json.NewEncoder(w).Encode(v))
}
```

- [ ] **Step 2: Write the failing `Validate` tests** — append to `pkg/oidc/oidc_test.go`:

```go
func newTestValidator(t *testing.T, f *fakeIssuer, cfg Config) *Validator {
	t.Helper()
	if cfg.IssuerURL == "" {
		cfg.IssuerURL = f.URL()
	}
	if len(cfg.Audiences) == 0 {
		cfg.Audiences = []string{"nats-chat"}
	}
	v, err := NewValidator(context.Background(), cfg)
	require.NoError(t, err)
	return v
}

func TestValidate_HappyPath_FillsExpiry(t *testing.T) {
	f := newFakeIssuer(t)
	v := newTestValidator(t, f, Config{})

	exp := time.Now().Add(30 * time.Minute).Truncate(time.Second)
	claims, err := v.Validate(context.Background(), f.Mint(map[string]any{"exp": exp.Unix()}))
	require.NoError(t, err)
	assert.Equal(t, "alice", claims.Account())
	assert.WithinDuration(t, exp, claims.Expiry, time.Second)
}

func TestValidate_ExpiredToken(t *testing.T) {
	f := newFakeIssuer(t)
	v := newTestValidator(t, f, Config{})

	_, err := v.Validate(context.Background(), f.Mint(map[string]any{"exp": time.Now().Add(-time.Hour).Unix()}))
	assert.ErrorIs(t, err, ErrTokenExpired)
}

func TestValidate_WrongAudience(t *testing.T) {
	f := newFakeIssuer(t)
	v := newTestValidator(t, f, Config{})

	_, err := v.Validate(context.Background(), f.Mint(map[string]any{"aud": "other-client"}))
	assert.ErrorIs(t, err, ErrAudienceNotAllowed)
}
```

Add `"context"`, `"time"`, and `"github.com/stretchr/testify/require"` to the test imports.

- [ ] **Step 3: Run tests, verify the Expiry assertion FAILS** (`claims.Expiry undefined` compile error).

Run: `make test` (oidc package). Expected: FAIL.

- [ ] **Step 4: Implement `Claims.Expiry`** — in `pkg/oidc/oidc.go`:

Add to `Claims`:
```go
	// Expiry is the verified token's exp claim (zero when unset).
	Expiry time.Time
```
Add `"time"` to imports (already imported). In `Validate`, in the final return, add:
```go
		Expiry:            idToken.Expiry,
```

- [ ] **Step 5: Run tests, verify PASS** (all three new tests + existing ones).

- [ ] **Step 6: Commit**

```bash
git add pkg/oidc/issuer_test.go pkg/oidc/oidc_test.go pkg/oidc/oidc.go
git commit -m "feat(oidc): Claims.Expiry + stdlib fake-issuer test harness with Validate coverage"
```

---

### Task 7: `pkg/oidc` — refresh grant

**Files:**
- Modify: `pkg/oidc/oidc.go`
- Test: `pkg/oidc/oidc_test.go` (append)

**Interfaces:**
- Produces (consumed by Tasks 12–13 via the `service.TokenRefresher` interface):
  - `Config.ClientID string`
  - `TokenSet{SSOToken string; RefreshToken string; Expiry time.Time}`
  - `func (v *Validator) Refresh(ctx context.Context, refreshToken string) (TokenSet, error)`
  - Sentinels: `ErrRefreshRejected`, `ErrNoAccessToken`

- [ ] **Step 1: Write the failing tests** — append to `pkg/oidc/oidc_test.go`:

```go
func TestRefresh_Success(t *testing.T) {
	f := newFakeIssuer(t)
	v := newTestValidator(t, f, Config{ClientID: "nats-chat"})

	exp := time.Now().Add(20 * time.Minute).Truncate(time.Second)
	newAccess := f.Mint(map[string]any{"exp": exp.Unix()})
	f.TokenHandler = func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, r.ParseForm())
		assert.Equal(t, "refresh_token", r.PostForm.Get("grant_type"))
		assert.Equal(t, "old-refresh", r.PostForm.Get("refresh_token"))
		assert.Equal(t, "nats-chat", r.PostForm.Get("client_id"))
		writeJSON(t, w, map[string]any{"access_token": newAccess, "refresh_token": "rotated-refresh"})
	}

	ts, err := v.Refresh(context.Background(), "old-refresh")
	require.NoError(t, err)
	assert.Equal(t, newAccess, ts.SSOToken)
	assert.Equal(t, "rotated-refresh", ts.RefreshToken)
	assert.WithinDuration(t, exp, ts.Expiry, time.Second)
}

func TestRefresh_InvalidGrantIsRejected(t *testing.T) {
	f := newFakeIssuer(t)
	v := newTestValidator(t, f, Config{ClientID: "nats-chat"})
	f.TokenHandler = func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"invalid_grant","error_description":"Token is not active"}`))
	}

	_, err := v.Refresh(context.Background(), "dead-refresh")
	assert.ErrorIs(t, err, ErrRefreshRejected)
}

func TestRefresh_ServerErrorIsNotRejected(t *testing.T) {
	f := newFakeIssuer(t)
	v := newTestValidator(t, f, Config{ClientID: "nats-chat"})
	f.TokenHandler = func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}

	_, err := v.Refresh(context.Background(), "any")
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrRefreshRejected) // transport/5xx is NOT an OAuth rejection
}

func TestRefresh_MissingAccessToken(t *testing.T) {
	f := newFakeIssuer(t)
	v := newTestValidator(t, f, Config{ClientID: "nats-chat"})
	f.TokenHandler = func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, map[string]any{"refresh_token": "r2"}) // no access_token
	}

	_, err := v.Refresh(context.Background(), "any")
	assert.ErrorIs(t, err, ErrNoAccessToken)
}

func TestRefresh_UnverifiableAccessTokenFails(t *testing.T) {
	f := newFakeIssuer(t)
	v := newTestValidator(t, f, Config{ClientID: "nats-chat"})
	f.TokenHandler = func(w http.ResponseWriter, _ *http.Request) {
		expired := f.Mint(map[string]any{"exp": time.Now().Add(-time.Hour).Unix()})
		writeJSON(t, w, map[string]any{"access_token": expired})
	}

	_, err := v.Refresh(context.Background(), "any")
	require.Error(t, err) // returned token must verify before we hand it out
}

func TestRefresh_WithoutClientIDFails(t *testing.T) {
	f := newFakeIssuer(t)
	v := newTestValidator(t, f, Config{}) // no ClientID
	_, err := v.Refresh(context.Background(), "any")
	require.Error(t, err)
}

func TestRefresh_RespectsContextCancellation(t *testing.T) {
	f := newFakeIssuer(t)
	v := newTestValidator(t, f, Config{ClientID: "nats-chat"})
	// Token endpoint blocks until the request's context is cancelled, proving
	// Refresh is bounded by ctx (and, in prod, by defaultRefreshClient's timeout).
	f.TokenHandler = func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := v.Refresh(ctx, "any")
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrRefreshRejected)
}
```

Add `"net/http"` to test imports. (This covers the spec §9 "timeout behavior" case via context deadline; the production timeout is `defaultRefreshClient`'s 10s.)

- [ ] **Step 2: Run tests, verify FAIL** (`v.Refresh undefined`).

- [ ] **Step 3: Implement** — in `pkg/oidc/oidc.go`:

Extend `Config` and `Validator`, add sentinels + timeout + default client:

```go
// Config controls how the OIDC validator behaves.
type Config struct {
	IssuerURL string
	// A token is accepted when any of its `aud` claim entries appears here.
	Audiences     []string
	TLSSkipVerify bool
	// ClientID is the OAuth client used by Refresh (public client, no
	// secret). Optional — validators that never call Refresh may omit it.
	ClientID string
}
```

```go
var (
	ErrTokenExpired       = errors.New("oidc: token has expired")
	ErrNoAudiences        = errors.New("oidc: at least one allowed audience is required")
	ErrAudienceNotAllowed = errors.New("oidc: token audience not in allowed list")
	// ErrRefreshRejected marks an OAuth-level refresh rejection (invalid_grant
	// et al.) as opposed to a transport/server failure.
	ErrRefreshRejected = errors.New("oidc: refresh token rejected by issuer")
	// ErrNoAccessToken marks a token response without an access_token.
	ErrNoAccessToken = errors.New("oidc: token response missing access_token")
)

const refreshTimeout = 10 * time.Second

// defaultRefreshClient bounds refresh POSTs when the validator has no custom
// client (TLSSkipVerify=false) — never fall through to the timeout-less
// http.DefaultClient.
var defaultRefreshClient = &http.Client{Timeout: refreshTimeout}
```

In `Validator`, add fields:
```go
type Validator struct {
	verifier      *oidc.IDTokenVerifier
	httpClient    *http.Client
	audiences     []string
	tokenEndpoint string
	clientID      string
}
```

In `NewValidator`, after `provider, err := oidc.NewProvider(...)` succeeds, read the token endpoint (avoids promoting `x/oauth2` to a direct dep, which `provider.Endpoint()` would):
```go
	var meta struct {
		TokenEndpoint string `json:"token_endpoint"`
	}
	if err := provider.Claims(&meta); err != nil {
		return nil, fmt.Errorf("parse oidc issuer metadata: %w", err)
	}
```
and include in the returned struct:
```go
	return &Validator{
		verifier:      provider.Verifier(oidcConfig),
		httpClient:    httpClient,
		audiences:     cfg.Audiences,
		tokenEndpoint: meta.TokenEndpoint,
		clientID:      cfg.ClientID,
	}, nil
```

Add `TokenSet` + `Refresh` at the bottom of the file:

```go
// TokenSet is the verified outcome of a refresh grant. SSOToken is the token
// response's access_token — the platform's "ssoToken" convention.
type TokenSet struct {
	SSOToken     string
	RefreshToken string
	Expiry       time.Time
}

// Refresh exchanges refreshToken at the issuer's token endpoint
// (grant_type=refresh_token, public client) and verifies the returned access
// token before handing it out. OAuth-level rejections wrap ErrRefreshRejected;
// transport and server failures return plain wrapped errors. No token
// material is ever included in an error or log line.
func (v *Validator) Refresh(ctx context.Context, refreshToken string) (TokenSet, error) {
	if v.tokenEndpoint == "" {
		return TokenSet{}, errors.New("oidc: issuer exposes no token_endpoint")
	}
	if v.clientID == "" {
		return TokenSet{}, errors.New("oidc: refresh requires Config.ClientID")
	}

	form := url.Values{
		"grant_type":    {"refresh_token"},
		"refresh_token": {refreshToken},
		"client_id":     {v.clientID},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, v.tokenEndpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return TokenSet{}, fmt.Errorf("build token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	client := v.httpClient
	if client == nil {
		client = defaultRefreshClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return TokenSet{}, fmt.Errorf("post token endpoint: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck // read-only body close

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return TokenSet{}, fmt.Errorf("read token response: %w", err)
	}

	switch {
	case resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusUnauthorized:
		// OAuth error envelope — the error code is safe to surface (no token
		// material); the description is not read at all.
		var oauthErr struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(body, &oauthErr)
		return TokenSet{}, fmt.Errorf("%w: %s", ErrRefreshRejected, oauthErr.Error)
	case resp.StatusCode != http.StatusOK:
		return TokenSet{}, fmt.Errorf("token endpoint returned status %d", resp.StatusCode)
	}

	var tokenResp struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
	}
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return TokenSet{}, fmt.Errorf("parse token response: %w", err)
	}
	if tokenResp.AccessToken == "" {
		return TokenSet{}, ErrNoAccessToken
	}

	claims, err := v.Validate(ctx, tokenResp.AccessToken)
	if err != nil {
		return TokenSet{}, fmt.Errorf("verify refreshed access token: %w", err)
	}

	return TokenSet{
		SSOToken:     tokenResp.AccessToken,
		RefreshToken: tokenResp.RefreshToken,
		Expiry:       claims.Expiry,
	}, nil
}
```

Add imports: `"encoding/json"`, `"io"`, `"net/url"`, `"strings"`.

- [ ] **Step 4: Run tests, verify PASS** (all six new + all prior).

- [ ] **Step 5: Run `make fmt` and `make lint` on the package. Commit**

```bash
git add pkg/oidc/oidc.go pkg/oidc/oidc_test.go
git commit -m "feat(oidc): refresh-grant support with verified access-token round-trip"
```

---

### Task 8: user-service `models` — SSO DTOs

**Files:**
- Create: `user-service/models/sso.go`
- Test: `user-service/models/sso_test.go`

**Interfaces:**
- Produces (consumed by Tasks 12–13): `models.SSOSetRequest{SSOToken, RefreshToken, Account string}`, `models.SSORefreshRequest{Account string}`, `models.SSORefreshResponse{SSOToken string}`. The set response reuses the existing `models.OKResponse`.

- [ ] **Step 1: Write the failing test** — create `user-service/models/sso_test.go` (mirror the sibling `*_test.go` marshal style):

```go
package models

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSSOSetRequest_JSON(t *testing.T) {
	var req SSOSetRequest
	require.NoError(t, json.Unmarshal([]byte(`{"ssoToken":"at","refreshToken":"rt","account":"bob"}`), &req))
	assert.Equal(t, "at", req.SSOToken)
	assert.Equal(t, "rt", req.RefreshToken)
	assert.Equal(t, "bob", req.Account)
}

func TestSSORefreshRequest_AccountOptional(t *testing.T) {
	var req SSORefreshRequest
	require.NoError(t, json.Unmarshal([]byte(`{}`), &req))
	assert.Empty(t, req.Account)
}

func TestSSORefreshResponse_JSON(t *testing.T) {
	out, err := json.Marshal(SSORefreshResponse{SSOToken: "at"})
	require.NoError(t, err)
	assert.JSONEq(t, `{"ssoToken":"at"}`, string(out))
}
```

- [ ] **Step 2: Run, verify FAIL** (`undefined: SSOSetRequest`).

Run: `make test SERVICE=user-service`. Expected: FAIL.

- [ ] **Step 3: Implement** — create `user-service/models/sso.go`:

```go
package models

// SSOSetRequest stores a user's SSO token pair (sso.set). Account targets
// another user (admin only); empty means the caller. The set response reuses
// OKResponse.
type SSOSetRequest struct {
	SSOToken     string `json:"ssoToken"`
	RefreshToken string `json:"refreshToken"`
	Account      string `json:"account,omitempty"`
}

// SSORefreshRequest retrieves (and maybe refreshes) a stored SSO token
// (sso.refresh). All fields optional — an empty payload means self-service.
type SSORefreshRequest struct {
	Account string `json:"account,omitempty"`
}

// SSORefreshResponse carries the stored or freshly-refreshed ssoToken.
type SSORefreshResponse struct {
	SSOToken string `json:"ssoToken"`
}
```

- [ ] **Step 4: Run, verify PASS.**

- [ ] **Step 5: Commit**

```bash
git add user-service/models/sso.go user-service/models/sso_test.go
git commit -m "feat(user-service): SSO set/refresh request-reply DTOs"
```

---

### Task 9: user-service `config` — optional OIDC block

**Files:**
- Modify: `user-service/config/config.go`
- Test: `user-service/config/config_test.go` (append)

**Interfaces:**
- Produces (consumed by Task 14): `Config.OIDCIssuerURL`, `Config.OIDCAudiences []string`, `Config.TLSSkipVerify bool`, `Config.OIDCClientID string`, `Config.SSORefreshWindow time.Duration`, and `Config.SSOEnabled() bool` (= `OIDCIssuerURL != ""`). Task 12 consumes `SSORefreshWindow` via `service.New`.

- [ ] **Step 1: Write the failing tests** — append to `user-service/config/config_test.go`:

```go
func TestLoad_SSODisabledByDefault(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://x")
	t.Setenv("NATS_URL", "nats://x")
	t.Setenv("SITE_ID", "site-a")
	cfg, err := Load()
	require.NoError(t, err)
	require.False(t, cfg.SSOEnabled())
	require.Equal(t, time.Hour, cfg.SSORefreshWindow)
}

func TestLoad_SSOEnabledRequiresAudiencesAndClientID(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://x")
	t.Setenv("NATS_URL", "nats://x")
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("OIDC_ISSUER_URL", "http://keycloak:8080/realms/chatapp")
	_, err := Load()
	require.ErrorContains(t, err, "OIDC_AUDIENCES")

	t.Setenv("OIDC_AUDIENCES", "nats-chat")
	_, err = Load()
	require.ErrorContains(t, err, "OIDC_CLIENT_ID")

	t.Setenv("OIDC_CLIENT_ID", "nats-chat")
	cfg, err := Load()
	require.NoError(t, err)
	require.True(t, cfg.SSOEnabled())
	require.Equal(t, []string{"nats-chat"}, cfg.OIDCAudiences)
}

func TestLoad_SSORefreshWindowMustBePositive(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://x")
	t.Setenv("NATS_URL", "nats://x")
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("OIDC_ISSUER_URL", "http://keycloak:8080/realms/chatapp")
	t.Setenv("OIDC_AUDIENCES", "nats-chat")
	t.Setenv("OIDC_CLIENT_ID", "nats-chat")
	t.Setenv("SSO_REFRESH_WINDOW", "0s")
	_, err := Load()
	require.ErrorContains(t, err, "SSO_REFRESH_WINDOW")
}
```

- [ ] **Step 2: Run, verify FAIL** (`cfg.SSOEnabled undefined`).

- [ ] **Step 3: Implement** — in `user-service/config/config.go`, add fields to `Config` (after `HandlerTimeout`):

```go
	// OIDC settings for the SSO token vault (sso.set / sso.refresh) — optional
	// as a unit: unset OIDC_ISSUER_URL disables the endpoints (they reply
	// unavailable); when set, AUDIENCES and CLIENT_ID are required and the
	// service fails fast if the issuer is unreachable at startup.
	OIDCIssuerURL    string        `env:"OIDC_ISSUER_URL"    envDefault:""`
	OIDCAudiences    []string      `env:"OIDC_AUDIENCES"     envDefault:"" envSeparator:","`
	TLSSkipVerify    bool          `env:"TLS_SKIP_VERIFY"    envDefault:"false"`
	OIDCClientID     string        `env:"OIDC_CLIENT_ID"     envDefault:""`
	SSORefreshWindow time.Duration `env:"SSO_REFRESH_WINDOW" envDefault:"1h"`
```

Add the method and validation:

```go
// SSOEnabled reports whether the SSO token-vault endpoints are configured.
func (c *Config) SSOEnabled() bool { return c.OIDCIssuerURL != "" }
```

In `Load()`, after the existing checks:

```go
	if cfg.SSOEnabled() {
		if len(cfg.OIDCAudiences) == 0 {
			return Config{}, fmt.Errorf("OIDC_AUDIENCES is required when OIDC_ISSUER_URL is set")
		}
		if cfg.OIDCClientID == "" {
			return Config{}, fmt.Errorf("OIDC_CLIENT_ID is required when OIDC_ISSUER_URL is set")
		}
		if cfg.SSORefreshWindow <= 0 {
			return Config{}, fmt.Errorf("SSO_REFRESH_WINDOW must be > 0, got %s", cfg.SSORefreshWindow)
		}
	}
```

- [ ] **Step 4: Run, verify PASS** (`make test SERVICE=user-service`).

- [ ] **Step 5: Commit**

```bash
git add user-service/config/config.go user-service/config/config_test.go
git commit -m "feat(user-service): optional OIDC config block for the SSO token vault"
```

---

### Task 10: `UserRepository.GetUserRoles` (repo + interface + mocks)

**Files:**
- Modify: `user-service/mongorepo/users.go`, `user-service/service/service.go` (interface only)
- Regenerate: `user-service/service/mocks/mock_repository.go` (via `make generate`)
- Test: `user-service/mongorepo/users_test.go` (append, integration)

**Interfaces:**
- Produces (consumed by Tasks 12–13): `GetUserRoles(ctx context.Context, account string) (*model.User, error)` on `service.UserRepository` and `mongorepo.UserRepo` — projection `{"_id":0,"account":1,"roles":1}`, `activeUserFilter`, `(nil, nil)` on miss.

- [ ] **Step 1: Write the failing integration test** — append to `user-service/mongorepo/users_test.go`:

```go
func TestUserRepo_GetUserRoles(t *testing.T) {
	r, db := newTestUserRepo(t)
	seed(t, db, "users",
		bson.M{"_id": "u1", "account": "admin-user", "roles": bson.A{"admin"}, "statusText": "hi"},
		bson.M{"_id": "u2", "account": "plain-user"},
		bson.M{"_id": "u3", "account": "gone-user", "roles": bson.A{"admin"}, "active": false},
	)

	u, err := r.GetUserRoles(context.Background(), "admin-user")
	require.NoError(t, err)
	require.NotNil(t, u)
	assert.Equal(t, "admin-user", u.Account)
	assert.True(t, model.IsPlatformAdmin(u))
	assert.Empty(t, u.StatusText, "statusText is outside the roles projection")

	u, err = r.GetUserRoles(context.Background(), "plain-user")
	require.NoError(t, err)
	require.NotNil(t, u)
	assert.False(t, model.IsPlatformAdmin(u))

	u, err = r.GetUserRoles(context.Background(), "gone-user")
	require.NoError(t, err)
	assert.Nil(t, u, "deactivated users are filtered by activeUserFilter")

	u, err = r.GetUserRoles(context.Background(), "missing")
	require.NoError(t, err)
	assert.Nil(t, u)
}
```

- [ ] **Step 2: Run, verify FAIL**

Run: `make test-integration SERVICE=user-service` (requires Docker). Expected: FAIL — `r.GetUserRoles undefined`.

- [ ] **Step 3: Implement repo method** — append to `user-service/mongorepo/users.go`:

```go
// GetUserRoles returns the active user's account + roles (all other fields
// zero-valued) for platform-admin checks, or (nil, nil) when no active user
// matched.
func (r *UserRepo) GetUserRoles(ctx context.Context, account string) (*model.User, error) {
	return r.users.FindOne(ctx, activeUserFilter(account),
		mongoutil.WithProjection(bson.M{"_id": 0, "account": 1, "roles": 1}),
	)
}
```

- [ ] **Step 4: Add to the interface** — in `user-service/service/service.go`, extend `UserRepository`:

```go
type UserRepository interface {
	GetUserStatus(ctx context.Context, account string) (*model.User, error)
	SetUserStatus(ctx context.Context, account, text string, isShow *bool) (*model.User, error)
	GetHRInfoByAccounts(ctx context.Context, accounts []string) (map[string]*model.SubscriptionHRInfo, error)
	GetUserSettings(ctx context.Context, account string) (*model.User, error)
	UpdateUserSettings(ctx context.Context, account string, set *model.UserSettings) (*model.User, error)
	GetUserRoles(ctx context.Context, account string) (*model.User, error)
}
```

- [ ] **Step 5: Regenerate mocks**

Run: `make generate SERVICE=user-service`
Expected: `service/mocks/mock_repository.go` gains `MockUserRepository.GetUserRoles`.

- [ ] **Step 6: Run unit + integration tests, verify PASS**

Run: `make test SERVICE=user-service` then `make test-integration SERVICE=user-service`. Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add user-service/mongorepo/users.go user-service/service/service.go user-service/service/mocks/mock_repository.go user-service/mongorepo/users_test.go
git commit -m "feat(user-service): GetUserRoles projection for platform-admin checks"
```

---

### Task 11: `mongorepo.SSOTokenRepo` over `sso_tokens`

**Files:**
- Create: `user-service/mongorepo/ssotokens.go`
- Test: `user-service/mongorepo/ssotokens_test.go` (integration)
- Modify: `user-service/mongorepo/setup_test.go` (helper; the interface assertion lands in Task 12 once the interface exists)

**Interfaces:**
- Produces (consumed by Tasks 12–14):
  - `mongorepo.NewSSOTokenRepo(db *mongo.Database) *SSOTokenRepo`
  - `(*SSOTokenRepo) EnsureIndexes(ctx context.Context) error`
  - `(*SSOTokenRepo) GetByUsername(ctx context.Context, username string) (*model.SSOToken, error)` — `(nil, nil)` on miss
  - `(*SSOTokenRepo) Upsert(ctx context.Context, username, ssoToken string, ssoTokenExpMs int64, refreshToken string) error`

- [ ] **Step 1: Add the test helper** — in `user-service/mongorepo/setup_test.go`, after `newTestAppRepo`:

```go
// newTestSSOTokenRepo builds an SSOTokenRepo over an isolated test database.
func newTestSSOTokenRepo(t *testing.T) (*SSOTokenRepo, *mongo.Database) {
	t.Helper()
	db := testutil.MongoDB(t, "user-service")
	r := NewSSOTokenRepo(db)
	require.NoError(t, r.EnsureIndexes(context.Background()))
	return r, db
}
```

- [ ] **Step 2: Write the failing integration tests** — create `user-service/mongorepo/ssotokens_test.go`:

```go
//go:build integration

package mongorepo

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestSSOTokenRepo_UpsertInsertsWithGeneratedID(t *testing.T) {
	r, db := newTestSSOTokenRepo(t)

	require.NoError(t, r.Upsert(context.Background(), "alice", "access-1", 1735689600000, "refresh-1"))

	// Raw read: verify legacy field names and generated _id shape.
	var raw bson.M
	require.NoError(t, db.Collection("sso_tokens").
		FindOne(context.Background(), bson.M{"username": "alice"}).Decode(&raw))
	assert.Len(t, raw["_id"], 17, "new docs get 17-char idgen.GenerateID ids")
	assert.Equal(t, "access-1", raw["idToken"])
	assert.Equal(t, "1735689600000", raw["idTokenExp"], "persisted as a decimal-millis string")
	assert.Equal(t, "refresh-1", raw["refreshToken"])
	assert.NotNil(t, raw["_updatedAt"])
}

func TestSSOTokenRepo_UpsertUpdatesKeepingID(t *testing.T) {
	r, db := newTestSSOTokenRepo(t)
	// Simulate a migrated legacy doc (foreign _id kept verbatim; exp is a string).
	seed(t, db, "sso_tokens", bson.M{
		"_id": "legacyMeteorId17c", "username": "bob",
		"idToken": "old-access", "idTokenExp": "1000", "refreshToken": "old-refresh",
		"_updatedAt": time.Now().Add(-time.Hour),
	})

	require.NoError(t, r.Upsert(context.Background(), "bob", "new-access", 2000, "new-refresh"))

	got, err := r.GetByUsername(context.Background(), "bob")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "new-access", got.IDToken)
	assert.Equal(t, int64(2000), got.IDTokenExp, "millis string parsed back to int64")
	assert.Equal(t, "new-refresh", got.RefreshToken)

	var raw bson.M
	require.NoError(t, db.Collection("sso_tokens").
		FindOne(context.Background(), bson.M{"username": "bob"}).Decode(&raw))
	assert.Equal(t, "legacyMeteorId17c", raw["_id"], "update keeps the legacy _id")
}

func TestSSOTokenRepo_GetByUsernameMissingIsNilNil(t *testing.T) {
	r, _ := newTestSSOTokenRepo(t)
	got, err := r.GetByUsername(context.Background(), "nobody")
	require.NoError(t, err)
	assert.Nil(t, got)
}

// idTokenExp is a string: a numeric millis string parses to int64; a
// non-numeric/odd legacy value parses to 0 (safe — reads as expired) rather
// than erroring the read.
func TestSSOTokenRepo_GetByUsernameStringExpDecode(t *testing.T) {
	r, db := newTestSSOTokenRepo(t)
	seed(t, db, "sso_tokens", bson.M{
		"_id": "legacyMillsId17c", "username": "dave",
		"idToken": "acc", "idTokenExp": "1735689600000", "refreshToken": "ref",
	})
	got, err := r.GetByUsername(context.Background(), "dave")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, int64(1735689600000), got.IDTokenExp)

	// Non-numeric legacy value → 0, no error.
	seed(t, db, "sso_tokens", bson.M{
		"_id": "legacyOddId17chr", "username": "erin",
		"idToken": "acc2", "idTokenExp": "not-a-number", "refreshToken": "ref2",
	})
	got, err = r.GetByUsername(context.Background(), "erin")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, int64(0), got.IDTokenExp)
}

func TestSSOTokenRepo_UsernameUniqueIndex(t *testing.T) {
	r, db := newTestSSOTokenRepo(t)
	require.NoError(t, r.Upsert(context.Background(), "carol", "a", 1, "r"))
	// A second raw insert with the same username must violate the unique index.
	_, err := db.Collection("sso_tokens").InsertOne(context.Background(),
		bson.M{"_id": "otherid", "username": "carol", "idToken": "b", "idTokenExp": "2", "refreshToken": "r2"})
	require.Error(t, err)
}
```

- [ ] **Step 3: Run, verify FAIL** (`undefined: NewSSOTokenRepo`).

Run: `make test-integration SERVICE=user-service`. Expected: compile FAIL.

- [ ] **Step 4: Implement** — create `user-service/mongorepo/ssotokens.go`:

```go
package mongorepo

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
)

// ssoTokensCollection stores SSO token pairs (legacy field names kept).
// Migrated legacy token docs must be loaded into this collection.
const ssoTokensCollection = "sso_tokens"

// SSOTokenRepo is the Mongo implementation of service.SSOTokenRepository.
// Documents are upserted by username (one per user); token values are never
// logged (model.SSOToken.String redacts them).
type SSOTokenRepo struct {
	tokens *mongoutil.Collection[model.SSOToken]
}

// ssoTokenDoc is the repo-local read model (sibling idiom: appCategoryDoc,
// hrUser). The legacy idTokenExp is a STRING (confirmed), so it is decoded as
// a string here and converted to int64 millis before crossing the store
// boundary — the consumer interface exchanges only pkg/model types.
type ssoTokenDoc struct {
	Username     string `bson:"username"`
	IDToken      string `bson:"idToken"`
	IDTokenExp   string `bson:"idTokenExp"`
	RefreshToken string `bson:"refreshToken"`
}

// NewSSOTokenRepo builds an SSOTokenRepo over db.
func NewSSOTokenRepo(db *mongo.Database) *SSOTokenRepo {
	return &SSOTokenRepo{
		tokens: mongoutil.NewCollection[model.SSOToken](db.Collection(ssoTokensCollection)),
	}
}

// EnsureIndexes creates the unique username index. The legacy writer upserts
// by username, so migrated docs are already unique on this key.
func (r *SSOTokenRepo) EnsureIndexes(ctx context.Context) error {
	_, err := r.tokens.Raw().Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "username", Value: 1}}, Options: options.Index().SetUnique(true),
	})
	if err != nil {
		return fmt.Errorf("create sso token index: %w", err)
	}
	return nil
}

// GetByUsername returns the stored token pair for username, or (nil, nil).
// idTokenExp is a decimal-millis string; a non-numeric/legacy value parses to
// 0, which reads as "expired" downstream and safely triggers a refresh.
func (r *SSOTokenRepo) GetByUsername(ctx context.Context, username string) (*model.SSOToken, error) {
	col := mongoutil.NewCollection[ssoTokenDoc](r.tokens.Raw())
	d, err := col.FindOne(ctx, bson.M{"username": username},
		mongoutil.WithProjection(bson.M{
			"_id": 0, "username": 1, "idToken": 1, "idTokenExp": 1, "refreshToken": 1,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("find sso token: %w", err)
	}
	if d == nil {
		return nil, nil
	}
	expMs, _ := strconv.ParseInt(d.IDTokenExp, 10, 64) // non-numeric ⇒ 0 ⇒ treated as expired
	return &model.SSOToken{
		Username:     d.Username,
		IDToken:      d.IDToken,
		IDTokenExp:   expMs,
		RefreshToken: d.RefreshToken,
	}, nil
}

// Upsert stores the token pair for username — one doc per user, last-write-
// wins. idTokenExp is persisted as a decimal-millis string (legacy schema).
// New docs get a 17-char idgen id (same length as legacy ids); existing docs
// (including migrated ones) keep their _id.
func (r *SSOTokenRepo) Upsert(ctx context.Context, username, ssoToken string, ssoTokenExpMs int64, refreshToken string) error {
	_, err := r.tokens.Raw().UpdateOne(ctx,
		bson.M{"username": username},
		bson.M{
			"$set": bson.M{
				"idToken":      ssoToken,
				"idTokenExp":   strconv.FormatInt(ssoTokenExpMs, 10),
				"refreshToken": refreshToken,
				"_updatedAt":   time.Now().UTC(),
			},
			"$setOnInsert": bson.M{"_id": idgen.GenerateID()},
		},
		options.UpdateOne().SetUpsert(true),
	)
	if err != nil {
		return fmt.Errorf("upsert sso token: %w", err)
	}
	return nil
}
```

- [ ] **Step 5: Run, verify PASS**

Run: `make test-integration SERVICE=user-service`. Expected: PASS (all new tests, including the string-exp-decode guard).

- [ ] **Step 6: Commit**

```bash
git add user-service/mongorepo/ssotokens.go user-service/mongorepo/ssotokens_test.go user-service/mongorepo/setup_test.go
git commit -m "feat(user-service): SSOTokenRepo over sso_tokens collection"
```

---

### Task 12: service scaffolding + `SSOSet` handler

**Files:**
- Modify: `user-service/service/service.go` (interfaces, `//go:generate`, struct fields, `New`, `RegisterHandlers`)
- Create: `user-service/service/sso.go`, `user-service/service/sso_test.go`
- Modify: `user-service/service/service_test.go` (`newSvc` passes the new deps)
- Modify: `user-service/mongorepo/setup_test.go` (add `SSOTokenRepository` assertion)
- Regenerate: `user-service/service/mocks/mock_repository.go`

**Interfaces:**
- Consumes: `models.SSOSetRequest`/`models.OKResponse` (Task 8), `oidc.Claims`/`oidc.ErrTokenExpired` (Task 6), `GetUserRoles` (Task 10), `SSOTokenRepo` shape (Task 11), `subject.UserSSOSetPattern` (Task 4), `config.SSORefreshWindow` (Task 9).
- Produces (consumed by Tasks 13–14):
  - `service.SSOTokenRepository`, `service.TokenValidator`, `service.TokenRefresher` interfaces (exact shapes below)
  - `New(subs, users, apps, threadSubs, rooms, history, presence, pub, clientPub EventPublisher, ssoTokens SSOTokenRepository, tokenValidator TokenValidator, tokenRefresher TokenRefresher, cfg *config.Config) *UserService`
  - `(*UserService) SSOSet(c *natsrouter.Context, req models.SSOSetRequest) (*models.OKResponse, error)`
  - Test helper `newSSOSvc(t)` in `sso_test.go`

- [ ] **Step 1: Declare interfaces + wire the struct** — in `user-service/service/service.go`:

Update the generate directive (one line):
```go
//go:generate mockgen -destination=mocks/mock_repository.go -package=mocks . SubscriptionRepository,UserRepository,AppRepository,RoomClient,HistoryClient,PresenceClient,EventPublisher,ThreadSubscriptionRepository,SSOTokenRepository,TokenValidator,TokenRefresher
```

Add after `EventPublisher` (imports gain `"time"` and `"github.com/hmchangw/chat/pkg/oidc"`):
```go
// SSOTokenRepository is the consumer-defined interface for the SSO token
// vault (sso_tokens collection; legacy field names kept).
type SSOTokenRepository interface {
	GetByUsername(ctx context.Context, username string) (*model.SSOToken, error)
	Upsert(ctx context.Context, username, ssoToken string, ssoTokenExpMs int64, refreshToken string) error
}

// TokenValidator verifies an SSO token against the configured OIDC issuer.
// Nil when the SSO feature is not configured (endpoints reply unavailable).
type TokenValidator interface {
	Validate(ctx context.Context, raw string) (oidc.Claims, error)
}

// TokenRefresher exchanges a refresh token at the issuer's token endpoint.
// Nil when the SSO feature is not configured.
type TokenRefresher interface {
	Refresh(ctx context.Context, refreshToken string) (oidc.TokenSet, error)
}
```

Add struct fields to `UserService` (after `clientPub`):
```go
	ssoTokens        SSOTokenRepository
	tokenValidator   TokenValidator
	tokenRefresher   TokenRefresher
	ssoRefreshWindow time.Duration
```

Change `New` (full new signature — callers in `main.go` and tests must pass the three new deps before `cfg`):
```go
func New(subs SubscriptionRepository, users UserRepository, apps AppRepository, threadSubs ThreadSubscriptionRepository, rooms RoomClient, history HistoryClient, presence PresenceClient, pub, clientPub EventPublisher, ssoTokens SSOTokenRepository, tokenValidator TokenValidator, tokenRefresher TokenRefresher, cfg *config.Config) *UserService {
```
and in the returned struct add:
```go
		ssoTokens:        ssoTokens,
		tokenValidator:   tokenValidator,
		tokenRefresher:   tokenRefresher,
		ssoRefreshWindow: cfg.SSORefreshWindow,
```

Register the set endpoint at the end of `RegisterHandlers`:
```go
	natsrouter.Register(r, subject.UserSSOSetPattern(s.siteID), s.SSOSet)
```

- [ ] **Step 2: Update `newSvc`** — in `user-service/service/service_test.go`, inside `newSvc` create the new mocks and pass them (return tuple unchanged so existing tests don't break):

```go
	ssoTokens := mocks.NewMockSSOTokenRepository(ctrl)
	validator := mocks.NewMockTokenValidator(ctrl)
	refresher := mocks.NewMockTokenRefresher(ctrl)
```
and change the return's constructor call to:
```go
	return New(subs, users, apps, threadSubs, rooms, history, presence, pub, pub, ssoTokens, validator, refresher, cfg), subs, users, apps, rooms, history, pub
```
Also add `SSORefreshWindow: time.Hour` to the `cfg` literal (import `"time"`).

- [ ] **Step 3: Regenerate mocks + add the vet assertion**

Run: `make generate SERVICE=user-service` — mocks gain `MockSSOTokenRepository`, `MockTokenValidator`, `MockTokenRefresher`.

In `user-service/mongorepo/setup_test.go`, extend the assertion block:
```go
var (
	_ service.SubscriptionRepository = (*SubscriptionRepo)(nil)
	_ service.UserRepository         = (*UserRepo)(nil)
	_ service.AppRepository          = (*AppRepo)(nil)
	_ service.SSOTokenRepository     = (*SSOTokenRepo)(nil)
)
```

Run: `make test SERVICE=user-service` — everything still compiles and passes EXCEPT `main.go` (fixed in Task 14; if the Makefile builds the whole service, temporarily update `main.go`'s `service.New(...)` call by inserting `nil, nil, nil,` before `&cfg` — Task 14 replaces it with real wiring).

- [ ] **Step 4: Write the failing `SSOSet` tests** — create `user-service/service/sso_test.go`:

```go
package service

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/oidc"
	"github.com/hmchangw/chat/user-service/config"
	"github.com/hmchangw/chat/user-service/models"
	"github.com/hmchangw/chat/user-service/service/mocks"
)

// newSSOSvc builds a UserService exposing the SSO-relevant mocks. Other deps
// are mocked but unused by the sso handlers.
func newSSOSvc(t *testing.T) (*UserService, *mocks.MockUserRepository, *mocks.MockSSOTokenRepository, *mocks.MockTokenValidator, *mocks.MockTokenRefresher) {
	t.Helper()
	ctrl := gomock.NewController(t)
	users := mocks.NewMockUserRepository(ctrl)
	ssoTokens := mocks.NewMockSSOTokenRepository(ctrl)
	validator := mocks.NewMockTokenValidator(ctrl)
	refresher := mocks.NewMockTokenRefresher(ctrl)
	cfg := &config.Config{SiteID: "site-a", SSORefreshWindow: time.Hour}
	svc := New(
		mocks.NewMockSubscriptionRepository(ctrl), users, mocks.NewMockAppRepository(ctrl),
		mocks.NewMockThreadSubscriptionRepository(ctrl), mocks.NewMockRoomClient(ctrl),
		mocks.NewMockHistoryClient(ctrl), mocks.NewMockPresenceClient(ctrl),
		mocks.NewMockEventPublisher(ctrl), mocks.NewMockEventPublisher(ctrl),
		ssoTokens, validator, refresher, cfg,
	)
	return svc, users, ssoTokens, validator, refresher
}

func adminUser(account string) *model.User {
	return &model.User{Account: account, Roles: []model.UserRole{model.UserRoleAdmin}}
}

func TestSSOSet_HappyPath_Self(t *testing.T) {
	svc, users, ssoTokens, validator, _ := newSSOSvc(t)
	exp := time.Now().Add(30 * time.Minute)
	users.EXPECT().GetUserRoles(gomock.Any(), "alice").Return(adminUser("alice"), nil)
	validator.EXPECT().Validate(gomock.Any(), "access-tok").
		Return(oidc.Claims{PreferredUsername: "alice", Expiry: exp}, nil)
	ssoTokens.EXPECT().Upsert(gomock.Any(), "alice", "access-tok", exp.UnixMilli(), "refresh-tok").Return(nil)

	resp, err := svc.SSOSet(ctx("alice", "site-a"), models.SSOSetRequest{SSOToken: "access-tok", RefreshToken: "refresh-tok"})
	require.NoError(t, err)
	assert.True(t, resp.Success)
}

func TestSSOSet_HappyPath_OnBehalfOf(t *testing.T) {
	svc, users, ssoTokens, validator, _ := newSSOSvc(t)
	exp := time.Now().Add(30 * time.Minute)
	users.EXPECT().GetUserRoles(gomock.Any(), "alice").Return(adminUser("alice"), nil)
	users.EXPECT().GetUserRoles(gomock.Any(), "bob").Return(&model.User{Account: "bob"}, nil)
	validator.EXPECT().Validate(gomock.Any(), "bob-tok").
		Return(oidc.Claims{PreferredUsername: "bob", Expiry: exp}, nil)
	ssoTokens.EXPECT().Upsert(gomock.Any(), "bob", "bob-tok", exp.UnixMilli(), "bob-refresh").Return(nil)

	_, err := svc.SSOSet(ctx("alice", "site-a"), models.SSOSetRequest{SSOToken: "bob-tok", RefreshToken: "bob-refresh", Account: "bob"})
	require.NoError(t, err)
}

func TestSSOSet_MissingFields(t *testing.T) {
	svc, _, _, _, _ := newSSOSvc(t)
	for name, req := range map[string]models.SSOSetRequest{
		"no ssoToken":     {RefreshToken: "r"},
		"no refreshToken": {SSOToken: "a"},
		"neither":         {},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := svc.SSOSet(ctx("alice", "site-a"), req)
			requireCode(t, err, errcode.CodeBadRequest)
		})
	}
}

func TestSSOSet_NonAdminForbidden(t *testing.T) {
	svc, users, _, _, _ := newSSOSvc(t)
	users.EXPECT().GetUserRoles(gomock.Any(), "alice").Return(&model.User{Account: "alice"}, nil)
	_, err := svc.SSOSet(ctx("alice", "site-a"), models.SSOSetRequest{SSOToken: "a", RefreshToken: "r"})
	requireCode(t, err, errcode.CodeForbidden)
}

func TestSSOSet_DeactivatedCallerForbidden(t *testing.T) {
	svc, users, _, _, _ := newSSOSvc(t)
	users.EXPECT().GetUserRoles(gomock.Any(), "alice").Return(nil, nil) // activeUserFilter miss
	_, err := svc.SSOSet(ctx("alice", "site-a"), models.SSOSetRequest{SSOToken: "a", RefreshToken: "r"})
	requireCode(t, err, errcode.CodeForbidden)
}

func TestSSOSet_InvalidTargetAccount(t *testing.T) {
	svc, _, _, _, _ := newSSOSvc(t)
	_, err := svc.SSOSet(ctx("alice", "site-a"), models.SSOSetRequest{SSOToken: "a", RefreshToken: "r", Account: "evil.*"})
	requireCode(t, err, errcode.CodeBadRequest)
}

func TestSSOSet_TargetNotFound(t *testing.T) {
	svc, users, _, _, _ := newSSOSvc(t)
	users.EXPECT().GetUserRoles(gomock.Any(), "alice").Return(adminUser("alice"), nil)
	users.EXPECT().GetUserRoles(gomock.Any(), "ghost").Return(nil, nil)
	_, err := svc.SSOSet(ctx("alice", "site-a"), models.SSOSetRequest{SSOToken: "a", RefreshToken: "r", Account: "ghost"})
	requireCode(t, err, errcode.CodeNotFound)
}

func TestSSOSet_ExpiredToken(t *testing.T) {
	svc, users, _, validator, _ := newSSOSvc(t)
	users.EXPECT().GetUserRoles(gomock.Any(), "alice").Return(adminUser("alice"), nil)
	validator.EXPECT().Validate(gomock.Any(), "old").Return(oidc.Claims{}, oidc.ErrTokenExpired)
	_, err := svc.SSOSet(ctx("alice", "site-a"), models.SSOSetRequest{SSOToken: "old", RefreshToken: "r"})
	requireCode(t, err, errcode.CodeUnauthenticated)
	var ee *errcode.Error
	require.ErrorAs(t, err, &ee)
	assert.Equal(t, errcode.AuthTokenExpired, ee.Reason)
}

func TestSSOSet_InvalidToken(t *testing.T) {
	svc, users, _, validator, _ := newSSOSvc(t)
	users.EXPECT().GetUserRoles(gomock.Any(), "alice").Return(adminUser("alice"), nil)
	validator.EXPECT().Validate(gomock.Any(), "junk").Return(oidc.Claims{}, errors.New("bad signature"))
	_, err := svc.SSOSet(ctx("alice", "site-a"), models.SSOSetRequest{SSOToken: "junk", RefreshToken: "r"})
	requireCode(t, err, errcode.CodeUnauthenticated)
	var ee *errcode.Error
	require.ErrorAs(t, err, &ee)
	assert.Equal(t, errcode.AuthInvalidToken, ee.Reason)
}

func TestSSOSet_TokenOwnerMismatch(t *testing.T) {
	svc, users, _, validator, _ := newSSOSvc(t)
	users.EXPECT().GetUserRoles(gomock.Any(), "alice").Return(adminUser("alice"), nil)
	validator.EXPECT().Validate(gomock.Any(), "tok").
		Return(oidc.Claims{PreferredUsername: "mallory", Expiry: time.Now().Add(time.Hour)}, nil)
	_, err := svc.SSOSet(ctx("alice", "site-a"), models.SSOSetRequest{SSOToken: "tok", RefreshToken: "r"})
	requireCode(t, err, errcode.CodeBadRequest)
}

func TestSSOSet_StoreErrorIsInternal(t *testing.T) {
	svc, users, ssoTokens, validator, _ := newSSOSvc(t)
	users.EXPECT().GetUserRoles(gomock.Any(), "alice").Return(adminUser("alice"), nil)
	validator.EXPECT().Validate(gomock.Any(), "tok").
		Return(oidc.Claims{PreferredUsername: "alice", Expiry: time.Now().Add(time.Hour)}, nil)
	ssoTokens.EXPECT().Upsert(gomock.Any(), "alice", "tok", gomock.Any(), "r").Return(errors.New("mongo down"))
	_, err := svc.SSOSet(ctx("alice", "site-a"), models.SSOSetRequest{SSOToken: "tok", RefreshToken: "r"})
	requireCode(t, err, errcode.CodeInternal)
}

func TestSSOSet_FeatureOffUnavailable(t *testing.T) {
	svc, _, _, _, _ := newSSOSvc(t)
	svc.tokenValidator = nil // simulate unset OIDC_ISSUER_URL
	_, err := svc.SSOSet(ctx("alice", "site-a"), models.SSOSetRequest{SSOToken: "a", RefreshToken: "r"})
	requireCode(t, err, errcode.CodeUnavailable)
}
```

NOTE: check `pkg/errcode`'s exported code constants (`CodeBadRequest`, `CodeForbidden`, `CodeNotFound`, `CodeUnauthenticated`, `CodeInternal`, `CodeUnavailable`) and the `errcode.Error` field names (`Code`, `Reason`) against `pkg/errcode` source before running — `requireCode` in `service_test.go:46` shows `ee.Code` usage; mirror whatever the real names are.

- [ ] **Step 5: Run, verify FAIL** (`svc.SSOSet undefined`).

- [ ] **Step 6: Implement** — create `user-service/service/sso.go`:

```go
package service

import (
	"errors"
	"fmt"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/oidc"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/user-service/models"
)

// Sentinels — reason-less forbidden per room-service precedent; the feature-
// off sentinel reuses the upstream_unavailable reason (auth-service
// BOTPLATFORM_URL-unset precedent).
var (
	errSSONotConfigured = errcode.Unavailable("sso is not configured on this site", errcode.WithReason(errcode.BotplatformUpstreamUnavailable))
	errSSOAdminOnly     = errcode.Forbidden("admin role required")
	errSSOTokenMismatch = errcode.BadRequest("sso token does not belong to the target account")
	errSSOInvalidTarget = errcode.BadRequest("invalid account")
	errSSOUserNotFound  = errcode.NotFound("user not found")
)

// SSOSet verifies and stores a user's SSO token pair (admin-only always).
func (s *UserService) SSOSet(c *natsrouter.Context, req models.SSOSetRequest) (*models.OKResponse, error) {
	if s.tokenValidator == nil || s.ssoTokens == nil {
		return nil, errSSONotConfigured
	}
	caller := c.Param("account")
	c.WithLogValues("account", caller)

	if req.SSOToken == "" || req.RefreshToken == "" {
		return nil, errcode.BadRequest("ssoToken and refreshToken are required", errcode.WithReason(errcode.AuthMissingFields))
	}
	target := caller
	if req.Account != "" {
		target = req.Account
	}
	if !subject.IsValidAccountToken(target) {
		return nil, errSSOInvalidTarget
	}
	c.WithLogValues("target", target)

	callerUser, err := s.users.GetUserRoles(c, caller)
	if err != nil {
		return nil, fmt.Errorf("get caller roles: %w", err)
	}
	if !model.IsPlatformAdmin(callerUser) { // nil-safe: missing/deactivated caller is not admin
		return nil, errSSOAdminOnly
	}
	if target != caller {
		targetUser, err := s.users.GetUserRoles(c, target)
		if err != nil {
			return nil, fmt.Errorf("get target user: %w", err)
		}
		if targetUser == nil {
			return nil, errSSOUserNotFound
		}
	}

	claims, err := s.tokenValidator.Validate(c, req.SSOToken)
	if err != nil {
		if errors.Is(err, oidc.ErrTokenExpired) {
			return nil, errcode.Unauthenticated("sso token has expired", errcode.WithReason(errcode.AuthTokenExpired))
		}
		// Cause carries the verification error (never token bytes) to the
		// server log only — auth-service handleSSO precedent.
		return nil, errcode.Unauthenticated("invalid sso token", errcode.WithReason(errcode.AuthInvalidToken), errcode.WithCause(err))
	}
	if claims.Account() != target {
		return nil, errSSOTokenMismatch
	}

	if err := s.ssoTokens.Upsert(c, target, req.SSOToken, claims.Expiry.UnixMilli(), req.RefreshToken); err != nil {
		return nil, fmt.Errorf("store sso token: %w", err)
	}
	return &models.OKResponse{Success: true}, nil
}
```

- [ ] **Step 7: Run, verify PASS**

Run: `make test SERVICE=user-service`. Expected: all sso_test.go set-tests PASS; all pre-existing service tests still PASS.

- [ ] **Step 8: Commit**

```bash
git add user-service/service/service.go user-service/service/sso.go user-service/service/sso_test.go user-service/service/service_test.go user-service/service/mocks/mock_repository.go user-service/mongorepo/setup_test.go user-service/main.go
git commit -m "feat(user-service): sso.set endpoint — admin-only verified token storage"
```

---

### Task 13: `SSORefresh` handler

**Files:**
- Modify: `user-service/service/sso.go`, `user-service/service/service.go` (one registration line)
- Test: `user-service/service/sso_test.go` (append)

**Interfaces:**
- Consumes: everything Task 12 produced, `models.SSORefreshRequest/Response` (Task 8), `oidc.TokenSet` (Task 7), `errcode.UserSSOTokenNotFound` (Task 3), `subject.UserSSORefreshPattern` (Task 4), `natsrouter.RegisterOptionalBody` (Task 5).
- Produces: `(*UserService) SSORefresh(c *natsrouter.Context, req models.SSORefreshRequest) (*models.SSORefreshResponse, error)`.

- [ ] **Step 1: Write the failing tests** — append to `user-service/service/sso_test.go`:

```go
func TestSSORefresh_FreshTokenReturnedUnchanged(t *testing.T) {
	svc, users, ssoTokens, _, _ := newSSOSvc(t)
	users.EXPECT().GetUserRoles(gomock.Any(), "alice").Return(&model.User{Account: "alice"}, nil)
	ssoTokens.EXPECT().GetByUsername(gomock.Any(), "alice").Return(&model.SSOToken{
		Username: "alice", IDToken: "stored-access",
		IDTokenExp: time.Now().Add(2 * time.Hour).UnixMilli(), // beyond 1h window
		RefreshToken: "stored-refresh",
	}, nil)

	resp, err := svc.SSORefresh(ctx("alice", "site-a"), models.SSORefreshRequest{})
	require.NoError(t, err)
	assert.Equal(t, "stored-access", resp.SSOToken)
}

func TestSSORefresh_WithinWindowRefreshes(t *testing.T) {
	svc, users, ssoTokens, _, refresher := newSSOSvc(t)
	newExp := time.Now().Add(30 * time.Minute)
	users.EXPECT().GetUserRoles(gomock.Any(), "alice").Return(&model.User{Account: "alice"}, nil)
	ssoTokens.EXPECT().GetByUsername(gomock.Any(), "alice").Return(&model.SSOToken{
		Username: "alice", IDToken: "stale-access",
		IDTokenExp: time.Now().Add(10 * time.Minute).UnixMilli(), // inside 1h window
		RefreshToken: "stored-refresh",
	}, nil)
	refresher.EXPECT().Refresh(gomock.Any(), "stored-refresh").
		Return(oidc.TokenSet{SSOToken: "new-access", RefreshToken: "rotated", Expiry: newExp}, nil)
	ssoTokens.EXPECT().Upsert(gomock.Any(), "alice", "new-access", newExp.UnixMilli(), "rotated").Return(nil)

	resp, err := svc.SSORefresh(ctx("alice", "site-a"), models.SSORefreshRequest{})
	require.NoError(t, err)
	assert.Equal(t, "new-access", resp.SSOToken)
}

func TestSSORefresh_AlreadyExpiredRefreshes(t *testing.T) {
	svc, users, ssoTokens, _, refresher := newSSOSvc(t)
	newExp := time.Now().Add(30 * time.Minute)
	users.EXPECT().GetUserRoles(gomock.Any(), "alice").Return(&model.User{Account: "alice"}, nil)
	ssoTokens.EXPECT().GetByUsername(gomock.Any(), "alice").Return(&model.SSOToken{
		Username: "alice", IDToken: "dead-access",
		IDTokenExp: time.Now().Add(-time.Hour).UnixMilli(), // already expired
		RefreshToken: "stored-refresh",
	}, nil)
	refresher.EXPECT().Refresh(gomock.Any(), "stored-refresh").
		Return(oidc.TokenSet{SSOToken: "new-access", RefreshToken: "rotated", Expiry: newExp}, nil)
	ssoTokens.EXPECT().Upsert(gomock.Any(), "alice", "new-access", newExp.UnixMilli(), "rotated").Return(nil)

	resp, err := svc.SSORefresh(ctx("alice", "site-a"), models.SSORefreshRequest{})
	require.NoError(t, err)
	assert.Equal(t, "new-access", resp.SSOToken)
}

func TestSSORefresh_RefreshFailureIsTokenExpired(t *testing.T) {
	svc, users, ssoTokens, _, refresher := newSSOSvc(t)
	users.EXPECT().GetUserRoles(gomock.Any(), "alice").Return(&model.User{Account: "alice"}, nil)
	ssoTokens.EXPECT().GetByUsername(gomock.Any(), "alice").Return(&model.SSOToken{
		Username: "alice", IDToken: "x",
		IDTokenExp: 1, RefreshToken: "dead-refresh",
	}, nil)
	refresher.EXPECT().Refresh(gomock.Any(), "dead-refresh").
		Return(oidc.TokenSet{}, oidc.ErrRefreshRejected)

	_, err := svc.SSORefresh(ctx("alice", "site-a"), models.SSORefreshRequest{})
	requireCode(t, err, errcode.CodeUnauthenticated)
	var ee *errcode.Error
	require.ErrorAs(t, err, &ee)
	assert.Equal(t, errcode.AuthTokenExpired, ee.Reason)
}

func TestSSORefresh_NoStoredToken(t *testing.T) {
	svc, users, ssoTokens, _, _ := newSSOSvc(t)
	users.EXPECT().GetUserRoles(gomock.Any(), "alice").Return(&model.User{Account: "alice"}, nil)
	ssoTokens.EXPECT().GetByUsername(gomock.Any(), "alice").Return(nil, nil)

	_, err := svc.SSORefresh(ctx("alice", "site-a"), models.SSORefreshRequest{})
	requireCode(t, err, errcode.CodeNotFound)
	var ee *errcode.Error
	require.ErrorAs(t, err, &ee)
	assert.Equal(t, errcode.UserSSOTokenNotFound, ee.Reason)
}

func TestSSORefresh_CallerNotFound(t *testing.T) {
	svc, users, _, _, _ := newSSOSvc(t)
	users.EXPECT().GetUserRoles(gomock.Any(), "alice").Return(nil, nil)
	_, err := svc.SSORefresh(ctx("alice", "site-a"), models.SSORefreshRequest{})
	requireCode(t, err, errcode.CodeNotFound)
}

func TestSSORefresh_AdminForOther(t *testing.T) {
	svc, users, ssoTokens, _, _ := newSSOSvc(t)
	users.EXPECT().GetUserRoles(gomock.Any(), "alice").Return(adminUser("alice"), nil)
	users.EXPECT().GetUserRoles(gomock.Any(), "bob").Return(&model.User{Account: "bob"}, nil)
	ssoTokens.EXPECT().GetByUsername(gomock.Any(), "bob").Return(&model.SSOToken{
		Username: "bob", IDToken: "bob-access",
		IDTokenExp: time.Now().Add(2 * time.Hour).UnixMilli(), RefreshToken: "r",
	}, nil)

	resp, err := svc.SSORefresh(ctx("alice", "site-a"), models.SSORefreshRequest{Account: "bob"})
	require.NoError(t, err)
	assert.Equal(t, "bob-access", resp.SSOToken)
}

func TestSSORefresh_NonAdminForOtherForbidden(t *testing.T) {
	svc, users, _, _, _ := newSSOSvc(t)
	users.EXPECT().GetUserRoles(gomock.Any(), "alice").Return(&model.User{Account: "alice"}, nil)
	_, err := svc.SSORefresh(ctx("alice", "site-a"), models.SSORefreshRequest{Account: "bob"})
	requireCode(t, err, errcode.CodeForbidden)
}

func TestSSORefresh_StoreErrorIsInternal(t *testing.T) {
	svc, users, ssoTokens, _, _ := newSSOSvc(t)
	users.EXPECT().GetUserRoles(gomock.Any(), "alice").Return(&model.User{Account: "alice"}, nil)
	ssoTokens.EXPECT().GetByUsername(gomock.Any(), "alice").Return(nil, errors.New("mongo down"))
	_, err := svc.SSORefresh(ctx("alice", "site-a"), models.SSORefreshRequest{})
	requireCode(t, err, errcode.CodeInternal)
}

func TestSSORefresh_FeatureOffUnavailable(t *testing.T) {
	svc, _, _, _, _ := newSSOSvc(t)
	svc.tokenRefresher = nil
	_, err := svc.SSORefresh(ctx("alice", "site-a"), models.SSORefreshRequest{})
	requireCode(t, err, errcode.CodeUnavailable)
}

func TestSSORefresh_InvalidTargetAccount(t *testing.T) {
	svc, _, _, _, _ := newSSOSvc(t)
	_, err := svc.SSORefresh(ctx("alice", "site-a"), models.SSORefreshRequest{Account: "evil.>"})
	requireCode(t, err, errcode.CodeBadRequest)
}

func TestSSORefresh_PreservesRefreshTokenWhenResponseOmitsIt(t *testing.T) {
	svc, users, ssoTokens, _, refresher := newSSOSvc(t)
	newExp := time.Now().Add(30 * time.Minute)
	users.EXPECT().GetUserRoles(gomock.Any(), "alice").Return(&model.User{Account: "alice"}, nil)
	ssoTokens.EXPECT().GetByUsername(gomock.Any(), "alice").Return(&model.SSOToken{
		Username: "alice", IDToken: "stale", IDTokenExp: 1, RefreshToken: "kept-refresh",
	}, nil)
	// IdP returns no refresh_token — the stored one must be preserved.
	refresher.EXPECT().Refresh(gomock.Any(), "kept-refresh").
		Return(oidc.TokenSet{SSOToken: "new-access", RefreshToken: "", Expiry: newExp}, nil)
	ssoTokens.EXPECT().Upsert(gomock.Any(), "alice", "new-access", newExp.UnixMilli(), "kept-refresh").Return(nil)

	resp, err := svc.SSORefresh(ctx("alice", "site-a"), models.SSORefreshRequest{})
	require.NoError(t, err)
	assert.Equal(t, "new-access", resp.SSOToken)
}
```

- [ ] **Step 2: Run, verify FAIL** (`svc.SSORefresh undefined`).

- [ ] **Step 3: Implement** — append to `user-service/service/sso.go` (imports gain `"time"`):

```go
// SSORefresh returns the stored ssoToken for the target account, refreshing
// it against the issuer when it is within ssoRefreshWindow of expiry (or
// already expired). Self-service by default; admin role required to target
// another account.
func (s *UserService) SSORefresh(c *natsrouter.Context, req models.SSORefreshRequest) (*models.SSORefreshResponse, error) {
	if s.tokenRefresher == nil || s.ssoTokens == nil {
		return nil, errSSONotConfigured
	}
	caller := c.Param("account")
	target := caller
	if req.Account != "" {
		target = req.Account
	}
	if !subject.IsValidAccountToken(target) {
		return nil, errSSOInvalidTarget
	}
	c.WithLogValues("account", caller, "target", target)

	callerUser, err := s.users.GetUserRoles(c, caller)
	if err != nil {
		return nil, fmt.Errorf("get caller roles: %w", err)
	}
	if target != caller {
		if !model.IsPlatformAdmin(callerUser) {
			return nil, errSSOAdminOnly
		}
		targetUser, err := s.users.GetUserRoles(c, target)
		if err != nil {
			return nil, fmt.Errorf("get target user: %w", err)
		}
		if targetUser == nil {
			return nil, errSSOUserNotFound
		}
	} else if callerUser == nil {
		return nil, errSSOUserNotFound
	}

	stored, err := s.ssoTokens.GetByUsername(c, target)
	if err != nil {
		return nil, fmt.Errorf("get sso token: %w", err)
	}
	if stored == nil {
		return nil, errcode.NotFound("no sso token stored for this account", errcode.WithReason(errcode.UserSSOTokenNotFound))
	}

	if time.UnixMilli(stored.IDTokenExp).After(time.Now().Add(s.ssoRefreshWindow)) {
		return &models.SSORefreshResponse{SSOToken: stored.IDToken}, nil
	}

	ts, err := s.tokenRefresher.Refresh(c, stored.RefreshToken)
	if err != nil {
		// Product decision (spec §8): ANY refresh failure sends the client to
		// re-login. Cause carries the refresh error (never token bytes).
		return nil, errcode.Unauthenticated("sso token has expired, please re-login", errcode.WithReason(errcode.AuthTokenExpired), errcode.WithCause(err))
	}
	// Keycloak rotates the refresh token on this grant, but guard against a
	// response that omits it so we never overwrite a still-valid refresh token
	// with an empty string.
	newRefresh := ts.RefreshToken
	if newRefresh == "" {
		newRefresh = stored.RefreshToken
	}
	if err := s.ssoTokens.Upsert(c, target, ts.SSOToken, ts.Expiry.UnixMilli(), newRefresh); err != nil {
		return nil, fmt.Errorf("store refreshed sso token: %w", err)
	}
	return &models.SSORefreshResponse{SSOToken: ts.SSOToken}, nil
}
```

Register it — in `service.go`'s `RegisterHandlers`, after the `SSOSet` line:
```go
	natsrouter.RegisterOptionalBody(r, subject.UserSSORefreshPattern(s.siteID), s.SSORefresh)
```

- [ ] **Step 4: Run, verify PASS** (`make test SERVICE=user-service`).

- [ ] **Step 5: Commit**

```bash
git add user-service/service/sso.go user-service/service/sso_test.go user-service/service/service.go
git commit -m "feat(user-service): sso.refresh endpoint — stored token retrieval with auto-refresh"
```

---

### Task 14: `main.go` wiring + deploy config

**Files:**
- Modify: `user-service/main.go`, `user-service/deploy/docker-compose.yml`

**Interfaces:**
- Consumes: `config.SSOEnabled()` (Task 9), `mongorepo.NewSSOTokenRepo` (Task 11), `service.New` new signature + `TokenValidator`/`TokenRefresher` (Task 12), `pkgoidc.NewValidator` with `ClientID` (Task 7).

- [ ] **Step 1: Wire in `main.go`**

Add import `pkgoidc "github.com/hmchangw/chat/pkg/oidc"`.

Extend the assertion block:
```go
	_ service.SSOTokenRepository = (*mongorepo.SSOTokenRepo)(nil)
```

After `threadSubRepo := ...` add:
```go
	ssoTokenRepo := mongorepo.NewSSOTokenRepo(db)
```
and after the existing `EnsureIndexes` calls:
```go
	if err := ssoTokenRepo.EnsureIndexes(ctx); err != nil {
		slog.Error("ensure indexes failed", "error", err)
		os.Exit(1)
	}
```

Before `svc := service.New(...)`:
```go
	// SSO vault OIDC wiring — optional as a unit: issuer unset ⇒ endpoints
	// reply unavailable; set ⇒ fail fast if the issuer is unreachable.
	var tokenValidator service.TokenValidator
	var tokenRefresher service.TokenRefresher
	if cfg.SSOEnabled() {
		v, err := pkgoidc.NewValidator(ctx, pkgoidc.Config{
			IssuerURL:     cfg.OIDCIssuerURL,
			Audiences:     cfg.OIDCAudiences,
			TLSSkipVerify: cfg.TLSSkipVerify,
			ClientID:      cfg.OIDCClientID,
		})
		if err != nil {
			slog.Error("oidc validator init failed", "error", err)
			os.Exit(1)
		}
		tokenValidator, tokenRefresher = v, v
	} else {
		slog.Warn("OIDC_ISSUER_URL not set — sso.set/sso.refresh will reply unavailable")
	}
```

Replace the constructor call (drop any temporary `nil, nil, nil` from Task 12):
```go
	svc := service.New(subRepo, userRepo, appRepo, threadSubRepo, roomclient.New(nc, cfg.SiteID), historyclient.New(nc), presenceclient.New(nc), publisher.New(js), publisher.NewCore(nc), ssoTokenRepo, tokenValidator, tokenRefresher, &cfg)
```

- [ ] **Step 2: Add dev envs** — in `user-service/deploy/docker-compose.yml` `environment:` list:

```yaml
      - OIDC_ISSUER_URL=http://keycloak:8080/realms/chatapp
      - OIDC_AUDIENCES=nats-chat
      - OIDC_CLIENT_ID=nats-chat
```
(`TLS_SKIP_VERIFY` and `SSO_REFRESH_WINDOW` keep their defaults.)

- [ ] **Step 3: Build + full service test**

Run: `make build SERVICE=user-service` — expected: compiles.
Run: `make test SERVICE=user-service` — expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add user-service/main.go user-service/deploy/docker-compose.yml
git commit -m "feat(user-service): wire SSO token vault — repo, optional OIDC validator, dev envs"
```

---

### Task 15: Documentation

**Files:**
- Modify: `docs/client-api.md`, `docs/client-api/request-reply.md`, `CLAUDE.md`

**Interfaces:** none (docs). Field tables must match the DTOs from Task 8 exactly.

- [ ] **Step 1: `docs/client-api.md` — §3.4 user-service endpoints**

Locate the user-service section (§3.4) and its endpoint-count line (currently **16**, per `docs/client-api.md:4059` — increment to 18; re-grep to confirm the exact current number before editing), the ToC (~line 61), and the routing table (~line 4064). Add two subsections following the existing endpoint format exactly (Subject / Request table / Success response + JSON example / Errors table):

````markdown
#### sso.set

Store (upsert) a user's SSO token pair in the site-local vault. **Platform-admin only.** The submitted `ssoToken` is verified against the site's OIDC issuer; its `preferred_username` must equal the target account. The stored expiry is derived server-side from the token's `exp` claim.

**Subject:** `chat.user.{account}.request.user.{siteID}.sso.set`

**Request**

| Field | Type | Required | Description |
|---|---|---|---|
| `ssoToken` | string | yes | The SSO token (Keycloak access token) to store |
| `refreshToken` | string | yes | The matching refresh token |
| `account` | string | no | Target account (admin on-behalf-of); defaults to the caller |

**Success response**

| Field | Type | Description |
|---|---|---|
| `success` | boolean | Always `true` |

```json
{ "success": true }
```

**Errors**

| Code | Reason | When |
|---|---|---|
| `bad_request` | `missing_fields` | `ssoToken` or `refreshToken` missing |
| `bad_request` | — | invalid `account` value, or token's `preferred_username` ≠ target account |
| `forbidden` | — | caller lacks the platform admin role (or is deactivated) |
| `not_found` | — | target account does not exist or is deactivated |
| `unauthenticated` | `sso_token_expired` | submitted token is expired |
| `unauthenticated` | `invalid_sso_token` | submitted token fails verification |
| `unavailable` | `upstream_unavailable` | SSO is not configured on this site |

#### sso.refresh

Return the caller's stored `ssoToken`, transparently refreshing it against the OIDC issuer when it is within the refresh window (default 1h) of expiry or already expired. Self-service; platform admins may target another account. The request body is optional — an empty payload means self.

**Subject:** `chat.user.{account}.request.user.{siteID}.sso.refresh`

**Request**

| Field | Type | Required | Description |
|---|---|---|---|
| `account` | string | no | Target account (admin only); defaults to the caller |

**Success response**

| Field | Type | Description |
|---|---|---|
| `ssoToken` | string | The stored or freshly-refreshed SSO token |

```json
{ "ssoToken": "eyJhbGciOiJSUzI1NiIs..." }
```

**Errors**

| Code | Reason | When |
|---|---|---|
| `bad_request` | — | invalid `account` value |
| `forbidden` | — | non-admin caller targeting another account |
| `not_found` | — | caller/target account does not exist or is deactivated |
| `not_found` | `sso_token_not_found` | no token pair stored for the target account |
| `unauthenticated` | `sso_token_expired` | token needed refreshing and the refresh failed (re-login) |
| `unavailable` | `upstream_unavailable` | SSO is not configured on this site |
````

- [ ] **Step 2: `docs/client-api.md` — routing table, ToC, §6 reason catalog**

- Routing table: add two rows mapping the subjects to user-service, in the section's existing format.
- ToC: add `sso.set` / `sso.refresh` entries under user-service.
- Endpoint count for user-service: 16 → 18.
- §6 reason catalog: add a `sso_token_not_found` row (`not_found`, emitted by user-service `sso.refresh`); amend the "Emitted by" cells of `sso_token_expired`, `invalid_sso_token`, and `missing_fields` to also list user-service `sso.set`/`sso.refresh`; amend `upstream_unavailable` to list user-service (feature unconfigured).

- [ ] **Step 3: Mirror into `docs/client-api/request-reply.md`**

Add the same two endpoints to the derived request/reply view in that file's format (check how existing user-service endpoints appear there and replicate). `events.md` is untouched (no events emitted).

- [ ] **Step 4: `CLAUDE.md` — per-entity `_id` table**

In Section 6 (MongoDB → primary-key bullet list), add one line:

```markdown
  - **SSO tokens** (`sso_tokens`): 17-char base62 via `idgen.GenerateID()` for new docs — same length as the legacy ids migrated verbatim from the old stack; migrated docs keep their original `_id`
```

- [ ] **Step 5: Commit**

```bash
git add docs/client-api.md docs/client-api/request-reply.md CLAUDE.md
git commit -m "docs: client-api + CLAUDE.md coverage for sso.set / sso.refresh"
```

---

### Task 16: Full verification sweep

**Files:** none new — fixes only if gates fail.

- [ ] **Step 1: Regenerate + format + lint**

```bash
make generate && make fmt && make lint
```
Expected: no diffs from generate (already regenerated), lint clean.

- [ ] **Step 2: Full unit tests with race detector**

```bash
make test
```
Expected: PASS across the repo (pkg/model, pkg/errcode, pkg/subject, pkg/natsrouter, pkg/oidc, user-service).

- [ ] **Step 3: Integration tests**

```bash
make test-integration SERVICE=user-service
```
Expected: PASS (ssotokens + users repo tests).

- [ ] **Step 4: Coverage check on the touched cores**

```bash
go test -coverprofile=/tmp/cover.out ./pkg/oidc/ ./user-service/service/ ./user-service/models/ && go tool cover -func=/tmp/cover.out | tail -5
```
(There is no `make` coverage target; CLAUDE.md §4 *Coverage* explicitly sanctions this raw `go test -coverprofile` + `go tool cover` form.) Expected: ≥80% per package, 90%+ on `user-service/service` and `pkg/oidc`.

- [ ] **Step 5: SAST**

```bash
make sast
```
Expected: clean (no medium+). The refresh POST and JWKS code contain no `InsecureSkipVerify` additions; the only pre-existing `#nosec` in `pkg/oidc` is untouched.

- [ ] **Step 6: Fix anything that failed, re-run the failing gate, then commit any fixes**

```bash
git add -A && git commit -m "chore: post-implementation verification fixes"  # only if fixes were needed
git push -u origin feat/user-service-sso-token-endpoints
```

---

## Self-Review (performed at plan-writing time)

- **Spec coverage:** §2 subjects → Task 4; §2.1 set → Task 12; §2.2 refresh + optional body → Tasks 5, 13; §3 authz/deactivated/integrity → Tasks 10, 12, 13; §4 data model → Tasks 2, 11; §5 refresh flow → Tasks 7, 13; §5.1 IdP note → docs only (spec); §6 pkg/oidc → Tasks 6, 7; §7 config/wiring → Tasks 9, 14; §8 errors → Tasks 3, 12, 13; §9 testing/docs → every task + Task 15; §10 non-goals → nothing planned against them. Spec corrections → Task 1.
- **Type consistency:** `SSOToken`/`IDTokenExp int64` (Task 2) ⇄ repo Upsert `ssoTokenExpMs int64` (Task 11) ⇄ `claims.Expiry.UnixMilli()` (Tasks 12–13); `oidc.TokenSet.SSOToken` (Task 7) ⇄ handler `ts.SSOToken` (Task 13); `New(...)` signature (Task 12) ⇄ `main.go` call (Task 14) ⇄ `newSvc` (Task 12).
- **Known verify-at-implementation points (not placeholders — the authoritative source is named):** exact errcode code-constant identifiers in Task 12 Step 4 (mirror `pkg/errcode` source), and the request-reply.md derived format (Task 15 Step 3 — replicate the file's existing entries).

## Post-review revision notes (three parallel reviewers: correctness / conventions / spec-fidelity)

- **BLOCKER resolved — legacy `idTokenExp` type (spec §4 open item), CONFIRMED by product owner: it is a string.** Task 1 Step 3 records this; Task 11's repo-local `ssoTokenDoc` decodes `idTokenExp` as a `string` and converts to `int64` millis (non-numeric ⇒ 0 ⇒ safe refresh), persists via `strconv.FormatInt`, and the service layer stays `int64` throughout (no handler/test churn). The unique-username index is safe — the legacy writer upserts by username, so migrated docs are already unique (also confirmed).
- **SHOULD-FIX resolved — pkg/oidc timeout test** added to Task 7 (`TestRefresh_RespectsContextCancellation`).
- **NIT resolved — refresh-token wipe** guarded in Task 13 (preserve stored refresh token when the IdP response omits one) + test.
- **NITs resolved:** new reason asserted in `codes_user_test.go` (Task 3); endpoint count pinned at 16 → 18 (Task 15); removed the unnecessary `make test SERVICE=pkg/model` hedges (Tasks 2, 16); coverage command correctly attributed to CLAUDE.md §4.
- **Confirmed clean by reviewers (no change needed):** `WithCause` is panic-safe on all oidc error paths (pkg/oidc never returns `*errcode.Error`); package-level errcode sentinels are safe to share (room-service precedent); no token bytes reach logs/causes; `New(...)` arg order consistent across constructor/main/test-helpers; the Task 12→14 temporary `nil,nil,nil` main.go wiring is a valid TDD intermediate.
