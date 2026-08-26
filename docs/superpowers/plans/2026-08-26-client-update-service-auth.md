# Client-Update Service-Account Auth Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Gate `client-update-service`'s upload endpoint behind an Ed25519 service-account JWT, and give `admin-service` an admin-only endpoint that streams an artifact pair through to it.

**Architecture:** A new stdlib-only `pkg/svcjwt` mints and verifies Ed25519 JWTs. `admin-service` holds the private key and signs a short-lived token in-process on every upload; `client-update-service` holds only the public key, verifies the token in Gin middleware, and checks the `sub` against an allowlist. The upload is forwarded by re-encoding the inbound multipart stream into the outbound request through an `io.Pipe`, so peak memory is independent of artifact size.

**Tech Stack:** Go 1.25, Gin, `crypto/ed25519` (stdlib — no new dependency), `pkg/errcode`, `pkg/restyutil`, testify, testcontainers (`pkg/testutil`).

**Spec:** `docs/superpowers/specs/2026-08-26-client-update-service-auth-design.md`

## Global Constraints

- **No new third-party dependency.** `go.mod` must not gain a require line. Ed25519 is `crypto/ed25519` in the standard library.
- **TDD is mandatory** (CLAUDE.md §4): write the failing test, run it and see it fail, implement the minimum, see it pass, commit. Never write implementation before its test.
- **Minimum 80% coverage** per package; target 90%+ for `pkg/svcjwt`.
- Use `make` targets, never raw `go` commands: `make test SERVICE=<name>`, `make lint`, `make fmt`, `make sast`.
- Always test with `-race` (the Makefile handles this).
- **Never log a token, key, or secret.** Never place one in an `errcode` message or cause — a cause reaches the server log.
- Errors: wrap with context (`fmt.Errorf("short description: %w", err)`), never return bare `err`, never compare errors by string.
- Config comes from env via `caarlos0/env` into a typed struct; secrets are `required` and never defaulted.
- Struct tags are `camelCase` for both `json` and `bson`.
- Every service keeps `GET /healthz` unauthenticated.
- Branch: `claude/service-auth-admin-upload-njomeg`. Commit after each task; never merge to `master`/`main` directly.
- **Out of scope:** the `admin-frontend` React upload page, and gating the download endpoint (spec §10).

---

### Task 1: `pkg/svcjwt` — Ed25519 service-account tokens

**Files:**
- Create: `pkg/svcjwt/svcjwt.go`
- Test: `pkg/svcjwt/svcjwt_test.go`

**Interfaces:**
- Consumes: nothing (leaf package, plus `pkg/idgen` for the `jti`).
- Produces:
  - `svcjwt.Claims{Issuer, Subject, Audience string; IssuedAt, ExpiresAt int64; JTI string}`
  - `svcjwt.ErrInvalidToken error` (sentinel; every verification failure wraps it)
  - `svcjwt.NewSigner(seedB64, issuer string) (*Signer, error)`
  - `(*Signer).Sign(subject, audience string, ttl time.Duration) (token string, expiresAt int64, err error)`
  - `svcjwt.NewVerifier(pubKeyB64, issuer, audience string) (*Verifier, error)`
  - `(*Verifier).Verify(token string) (*Claims, error)`

**Why a sentinel and not an `*errcode.Error`:** `pkg/svcjwt` is a library, not a request handler. CLAUDE.md's Tier-1 rule ("return a typed error from a named constructor") governs handler code; a leaf package returning a sentinel lets each consuming service attach its own domain `reason` at the boundary. `client-update-service` does exactly that in Task 3.

- [ ] **Step 1: Write the failing test**

Create `pkg/svcjwt/svcjwt_test.go`. Note the tests are in-package (`package svcjwt`) so they can drive `Verifier.now` without sleeping, per CLAUDE.md ("test files live in the same package to access unexported types").

```go
package svcjwt

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testIssuer   = "admin-service"
	testAudience = "client-update-service"
	testSubject  = "svc-updater"
)

// testKeys returns a fresh keypair in the base64 form the constructors expect.
func testKeys(t *testing.T) (seedB64, pubB64 string) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	enc := base64.StdEncoding
	return enc.EncodeToString(priv.Seed()), enc.EncodeToString(pub)
}

// testPair returns a signer and a verifier that trust each other.
func testPair(t *testing.T) (*Signer, *Verifier) {
	t.Helper()
	seed, pub := testKeys(t)
	s, err := NewSigner(seed, testIssuer)
	require.NoError(t, err)
	v, err := NewVerifier(pub, testIssuer, testAudience)
	require.NoError(t, err)
	return s, v
}

// retoken re-signs mutated claims with the signer's key, so a test can change a
// claim without invalidating the signature (isolating claim checks from the
// signature check).
func retoken(t *testing.T, s *Signer, c Claims) string {
	t.Helper()
	hb, err := json.Marshal(header{Alg: alg, Typ: "JWT"})
	require.NoError(t, err)
	cb, err := json.Marshal(c)
	require.NoError(t, err)
	signing := b64(hb) + "." + b64(cb)
	return signing + "." + b64(ed25519.Sign(s.key, []byte(signing)))
}

// claimsOf decodes a token's payload without verifying it.
func claimsOf(t *testing.T, token string) Claims {
	t.Helper()
	parts := strings.Split(token, ".")
	require.Len(t, parts, 3)
	raw, err := unb64(parts[1])
	require.NoError(t, err)
	var c Claims
	require.NoError(t, json.Unmarshal(raw, &c))
	return c
}

func TestSigner_Verifier_RoundTrip(t *testing.T) {
	s, v := testPair(t)

	token, exp, err := s.Sign(testSubject, testAudience, time.Hour)
	require.NoError(t, err)
	assert.NotEmpty(t, token)
	assert.Greater(t, exp, time.Now().Unix())

	got, err := v.Verify(token)
	require.NoError(t, err)
	assert.Equal(t, testIssuer, got.Issuer)
	assert.Equal(t, testSubject, got.Subject)
	assert.Equal(t, testAudience, got.Audience)
	assert.Equal(t, exp, got.ExpiresAt)
	assert.NotEmpty(t, got.JTI, "every token needs a unique id")
}

func TestSigner_Sign_UniqueJTI(t *testing.T) {
	s, _ := testPair(t)
	a, _, err := s.Sign(testSubject, testAudience, time.Hour)
	require.NoError(t, err)
	b, _, err := s.Sign(testSubject, testAudience, time.Hour)
	require.NoError(t, err)
	assert.NotEqual(t, claimsOf(t, a).JTI, claimsOf(t, b).JTI)
}

func TestSigner_Sign_RejectsBadArguments(t *testing.T) {
	s, _ := testPair(t)
	tests := []struct {
		name              string
		subject, audience string
		ttl               time.Duration
	}{
		{"empty subject", "", testAudience, time.Hour},
		{"empty audience", testSubject, "", time.Hour},
		{"zero ttl", testSubject, testAudience, 0},
		{"negative ttl", testSubject, testAudience, -time.Second},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := s.Sign(tc.subject, tc.audience, tc.ttl)
			assert.Error(t, err)
		})
	}
}

// TestVerify_RejectsInvalidTokens is the security core: one case per rule in
// spec §4.2. Every failure must wrap ErrInvalidToken so callers answer
// uniformly on the wire.
func TestVerify_RejectsInvalidTokens(t *testing.T) {
	s, v := testPair(t)
	valid, _, err := s.Sign(testSubject, testAudience, time.Hour)
	require.NoError(t, err)
	parts := strings.Split(valid, ".")

	// A second, unrelated keypair: a correctly-formed token signed by the wrong key.
	otherSeed, _ := testKeys(t)
	other, err := NewSigner(otherSeed, testIssuer)
	require.NoError(t, err)
	foreign, _, err := other.Sign(testSubject, testAudience, time.Hour)
	require.NoError(t, err)

	// swapAlg re-encodes the header with a different alg, keeping the original
	// payload and signature.
	swapAlg := func(a string) string {
		hb, err := json.Marshal(map[string]string{"alg": a, "typ": "JWT"})
		require.NoError(t, err)
		return b64(hb) + "." + parts[1] + "." + parts[2]
	}

	tests := []struct {
		name  string
		token string
	}{
		{"empty", ""},
		{"two segments", parts[0] + "." + parts[1]},
		{"four segments", valid + ".extra"},
		{"header not base64", "!!!." + parts[1] + "." + parts[2]},
		{"header not json", b64([]byte("not json")) + "." + parts[1] + "." + parts[2]},
		{"alg none", swapAlg("none")},
		{"alg HS256", swapAlg("HS256")},
		{"alg empty", swapAlg("")},
		{"signature not base64", parts[0] + "." + parts[1] + ".!!!"},
		{"signature tampered", parts[0] + "." + parts[1] + "." + b64([]byte("wrong signature bytes here"))},
		{"payload tampered", parts[0] + "." + b64([]byte(`{"iss":"admin-service","sub":"attacker"}`)) + "." + parts[2]},
		{"signed by another key", foreign},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := v.Verify(tc.token)
			assert.Nil(t, got)
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrInvalidToken,
				"every verification failure must wrap ErrInvalidToken")
		})
	}
}

// TestVerify_RejectsBadClaims isolates the claim rules: each token is correctly
// signed, so only the claim under test can cause the failure.
func TestVerify_RejectsBadClaims(t *testing.T) {
	s, v := testPair(t)
	now := time.Now().UTC()
	base := Claims{
		Issuer: testIssuer, Subject: testSubject, Audience: testAudience,
		IssuedAt: now.Unix(), ExpiresAt: now.Add(time.Hour).Unix(), JTI: "test-jti",
	}

	mutate := func(f func(*Claims)) string {
		c := base
		f(&c)
		return retoken(t, s, c)
	}

	tests := []struct {
		name  string
		token string
	}{
		{"wrong issuer", mutate(func(c *Claims) { c.Issuer = "someone-else" })},
		{"empty issuer", mutate(func(c *Claims) { c.Issuer = "" })},
		{"wrong audience", mutate(func(c *Claims) { c.Audience = "upload-service" })},
		{"empty audience", mutate(func(c *Claims) { c.Audience = "" })},
		{"empty subject", mutate(func(c *Claims) { c.Subject = "" })},
		{"no expiry", mutate(func(c *Claims) { c.ExpiresAt = 0 })},
		{"expired beyond leeway", mutate(func(c *Claims) {
			c.ExpiresAt = now.Add(-leeway - time.Minute).Unix()
		})},
		{"issued far in the future", mutate(func(c *Claims) {
			c.IssuedAt = now.Add(leeway + time.Minute).Unix()
		})},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := v.Verify(tc.token)
			assert.Nil(t, got)
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrInvalidToken)
		})
	}
}

// TestVerify_ExpiryLeeway pins the skew allowance: just-expired passes,
// expired past the leeway does not.
func TestVerify_ExpiryLeeway(t *testing.T) {
	s, v := testPair(t)
	token, _, err := s.Sign(testSubject, testAudience, time.Minute)
	require.NoError(t, err)

	v.now = func() time.Time { return time.Now().Add(time.Minute + leeway/2) }
	_, err = v.Verify(token)
	assert.NoError(t, err, "just past exp but inside leeway must still verify")

	v.now = func() time.Time { return time.Now().Add(time.Minute + leeway + time.Second) }
	_, err = v.Verify(token)
	assert.ErrorIs(t, err, ErrInvalidToken, "past exp+leeway must fail")
}

// TestVerify_ErrorsCarryNoToken guards the logging rule: the token must never
// appear in an error string, since the caller attaches it as an errcode cause.
func TestVerify_ErrorsCarryNoToken(t *testing.T) {
	s, v := testPair(t)
	token, _, err := s.Sign(testSubject, testAudience, time.Hour)
	require.NoError(t, err)
	parts := strings.Split(token, ".")
	tampered := parts[0] + "." + parts[1] + "." + b64([]byte("nope"))

	_, err = v.Verify(tampered)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), parts[1], "claims segment must not leak into the error")
	assert.NotContains(t, err.Error(), tampered)
}

func TestNewSigner_RejectsBadKey(t *testing.T) {
	seed, _ := testKeys(t)
	tests := []struct {
		name, key, issuer string
	}{
		{"empty key", "", testIssuer},
		{"not base64", "!!!not base64!!!", testIssuer},
		{"wrong length", base64.StdEncoding.EncodeToString([]byte("too short")), testIssuer},
		{"empty issuer", seed, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NewSigner(tc.key, tc.issuer)
			assert.Nil(t, got)
			assert.Error(t, err)
		})
	}
}

func TestNewVerifier_RejectsBadKey(t *testing.T) {
	_, pub := testKeys(t)
	tests := []struct {
		name, key, issuer, audience string
	}{
		{"empty key", "", testIssuer, testAudience},
		{"not base64", "!!!", testIssuer, testAudience},
		{"wrong length", base64.StdEncoding.EncodeToString([]byte("short")), testIssuer, testAudience},
		{"empty issuer", pub, "", testAudience},
		{"empty audience", pub, testIssuer, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NewVerifier(tc.key, tc.issuer, tc.audience)
			assert.Nil(t, got)
			assert.Error(t, err)
		})
	}
}

// TestVerify_AudienceIsolation proves a token minted for one service is
// refused by another — the reason `aud` exists.
func TestVerify_AudienceIsolation(t *testing.T) {
	seed, pub := testKeys(t)
	s, err := NewSigner(seed, testIssuer)
	require.NoError(t, err)
	other, err := NewVerifier(pub, testIssuer, "upload-service")
	require.NoError(t, err)

	token, _, err := s.Sign(testSubject, testAudience, time.Hour)
	require.NoError(t, err)

	_, err = other.Verify(token)
	assert.ErrorIs(t, err, ErrInvalidToken)
}

var _ = errors.Is // keep the errors import meaningful if assertions change
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `make test SERVICE=pkg/svcjwt`
Expected: FAIL — the package does not compile (`undefined: NewSigner`, `undefined: Claims`, …).

- [ ] **Step 3: Write the minimal implementation**

Create `pkg/svcjwt/svcjwt.go`:

```go
// Package svcjwt mints and verifies the Ed25519 tokens that authenticate one
// internal service to another. Signing is asymmetric on purpose: the minting
// service holds the private key and the verifying service holds only the public
// key, so a compromised verifier cannot forge tokens for itself.
package svcjwt

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/hmchangw/chat/pkg/idgen"
)

// alg is the only signing algorithm this package accepts. Verify COMPARES the
// token header against this constant rather than dispatching on it: there is no
// table mapping an algorithm name to a verifier, so "none" and HS256-confusion
// have no code path to reach.
const alg = "EdDSA"

// leeway absorbs clock skew between the minting and verifying services.
const leeway = 30 * time.Second

// ErrInvalidToken is wrapped by every Verify failure — bad structure, unknown
// algorithm, bad signature, wrong issuer or audience, expired. The wrapping
// message names the broken rule for the server log; callers match the sentinel
// and answer uniformly, so the wire never reveals which rule failed.
var ErrInvalidToken = errors.New("invalid service token")

// header is the fixed JWT header, never negotiated per token.
type header struct {
	Alg string `json:"alg"`
	Typ string `json:"typ"`
}

// Claims is the token payload: registered JWT names only.
type Claims struct {
	Issuer    string `json:"iss"`
	Subject   string `json:"sub"`
	Audience  string `json:"aud"`
	IssuedAt  int64  `json:"iat"`
	ExpiresAt int64  `json:"exp"`
	JTI       string `json:"jti"`
}

// Signer mints tokens. Construct once at startup and reuse.
type Signer struct {
	key    ed25519.PrivateKey
	issuer string
}

// NewSigner decodes seedB64 — base64 of the raw 32-byte Ed25519 seed — into a
// signer. A malformed or wrong-length seed fails here, at startup, rather than
// at the first request.
func NewSigner(seedB64, issuer string) (*Signer, error) {
	if issuer == "" {
		return nil, errors.New("new svcjwt signer: issuer must not be empty")
	}
	seed, err := decodeKey(seedB64, ed25519.SeedSize)
	if err != nil {
		return nil, fmt.Errorf("new svcjwt signer: %w", err)
	}
	return &Signer{key: ed25519.NewKeyFromSeed(seed), issuer: issuer}, nil
}

// Sign mints a token for subject/audience valid for ttl, returning it with its
// expiry as unix seconds.
func (s *Signer) Sign(subject, audience string, ttl time.Duration) (string, int64, error) {
	if subject == "" || audience == "" {
		return "", 0, errors.New("sign svcjwt: subject and audience must not be empty")
	}
	if ttl <= 0 {
		return "", 0, fmt.Errorf("sign svcjwt: ttl must be positive, got %s", ttl)
	}
	now := time.Now().UTC()
	exp := now.Add(ttl).Unix()
	hb, err := json.Marshal(header{Alg: alg, Typ: "JWT"})
	if err != nil {
		return "", 0, fmt.Errorf("marshal svcjwt header: %w", err)
	}
	cb, err := json.Marshal(Claims{
		Issuer:   s.issuer,
		Subject:  subject,
		Audience: audience,
		IssuedAt: now.Unix(),
		ExpiresAt: exp,
		JTI:      idgen.GenerateUUIDv7(),
	})
	if err != nil {
		return "", 0, fmt.Errorf("marshal svcjwt claims: %w", err)
	}
	signing := b64(hb) + "." + b64(cb)
	return signing + "." + b64(ed25519.Sign(s.key, []byte(signing))), exp, nil
}

// Verifier checks tokens against one issuer and one audience.
type Verifier struct {
	key      ed25519.PublicKey
	issuer   string
	audience string
	// now is injected so tests can drive expiry without sleeping.
	now func() time.Time
}

// NewVerifier decodes pubKeyB64 — base64 of the raw 32-byte Ed25519 public key.
func NewVerifier(pubKeyB64, issuer, audience string) (*Verifier, error) {
	if issuer == "" || audience == "" {
		return nil, errors.New("new svcjwt verifier: issuer and audience must not be empty")
	}
	pub, err := decodeKey(pubKeyB64, ed25519.PublicKeySize)
	if err != nil {
		return nil, fmt.Errorf("new svcjwt verifier: %w", err)
	}
	return &Verifier{key: ed25519.PublicKey(pub), issuer: issuer, audience: audience, now: time.Now}, nil
}

// Verify checks structure, then algorithm, then signature, then claims — in
// that order. Claims are unmarshalled only after the signature has proven the
// bytes authentic, so no attacker-controlled JSON is parsed on an unverified
// token. Error messages deliberately exclude the token and any segment of it.
func (v *Verifier) Verify(token string) (*Claims, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("svcjwt: want 3 segments, got %d: %w", len(parts), ErrInvalidToken)
	}
	hb, err := unb64(parts[0])
	if err != nil {
		return nil, fmt.Errorf("svcjwt: header is not base64url: %w", ErrInvalidToken)
	}
	var h header
	if err := json.Unmarshal(hb, &h); err != nil {
		return nil, fmt.Errorf("svcjwt: header is not json: %w", ErrInvalidToken)
	}
	// Compared, never dispatched on — see the alg const.
	if h.Alg != alg {
		return nil, fmt.Errorf("svcjwt: unsupported algorithm: %w", ErrInvalidToken)
	}
	sig, err := unb64(parts[2])
	if err != nil {
		return nil, fmt.Errorf("svcjwt: signature is not base64url: %w", ErrInvalidToken)
	}
	if !ed25519.Verify(v.key, []byte(parts[0]+"."+parts[1]), sig) {
		return nil, fmt.Errorf("svcjwt: signature mismatch: %w", ErrInvalidToken)
	}

	// Authenticated from here on.
	cb, err := unb64(parts[1])
	if err != nil {
		return nil, fmt.Errorf("svcjwt: claims are not base64url: %w", ErrInvalidToken)
	}
	var c Claims
	if err := json.Unmarshal(cb, &c); err != nil {
		return nil, fmt.Errorf("svcjwt: claims are not json: %w", ErrInvalidToken)
	}
	if c.Issuer != v.issuer {
		return nil, fmt.Errorf("svcjwt: issuer mismatch: %w", ErrInvalidToken)
	}
	if c.Audience != v.audience {
		return nil, fmt.Errorf("svcjwt: audience mismatch: %w", ErrInvalidToken)
	}
	if c.Subject == "" {
		return nil, fmt.Errorf("svcjwt: empty subject: %w", ErrInvalidToken)
	}
	now := v.now().UTC()
	if c.ExpiresAt <= 0 || now.After(time.Unix(c.ExpiresAt, 0).Add(leeway)) {
		return nil, fmt.Errorf("svcjwt: token expired: %w", ErrInvalidToken)
	}
	if c.IssuedAt > 0 && time.Unix(c.IssuedAt, 0).After(now.Add(leeway)) {
		return nil, fmt.Errorf("svcjwt: issued in the future: %w", ErrInvalidToken)
	}
	return &c, nil
}

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

func unb64(s string) ([]byte, error) { return base64.RawURLEncoding.DecodeString(s) }

// decodeKey decodes a standard-base64 key and checks its decoded length, so a
// truncated key or one of the wrong kind is rejected at construction. The key
// itself never appears in the returned error.
func decodeKey(s string, want int) ([]byte, error) {
	if s == "" {
		return nil, errors.New("key must not be empty")
	}
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, errors.New("key is not valid base64")
	}
	if len(b) != want {
		return nil, fmt.Errorf("key must decode to %d bytes, got %d", want, len(b))
	}
	return b, nil
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `make test SERVICE=pkg/svcjwt`
Expected: PASS, all subtests.

- [ ] **Step 5: Check coverage meets the 90% target**

Run: `go test -race -coverprofile=/tmp/svcjwt.out ./pkg/svcjwt && go tool cover -func=/tmp/svcjwt.out | tail -1`
Expected: total coverage ≥ 90%. If below, add cases for the uncovered lines (most likely the `json.Marshal` error branches, which are unreachable — leave those and confirm the rest is covered).

- [ ] **Step 6: Lint and commit**

```bash
make fmt
make lint
git add pkg/svcjwt/
git commit -m "feat(svcjwt): Ed25519 service-account token signer and verifier

Adds a stdlib-only JWT implementation for service-to-service auth. The alg
header is compared against a constant rather than dispatched on, so alg-none
and HS256-confusion have no code path. Signatures are checked before claims
are unmarshalled, and no error message carries the token."
```

---

### Task 2: `tools/svcjwtkey` — keypair generator

**Files:**
- Create: `tools/svcjwtkey/main.go`
- Test: `tools/svcjwtkey/main_test.go`

**Interfaces:**
- Consumes: `svcjwt.NewSigner`, `svcjwt.NewVerifier` (Task 1) — the test round-trips the generated pair through them.
- Produces: a `go run ./tools/svcjwtkey` command printing `SVCJWT_PRIVATE_KEY=…` and `SVCJWT_PUBLIC_KEY=…`. Nothing imports it.

- [ ] **Step 1: Write the failing test**

Create `tools/svcjwtkey/main_test.go`:

```go
package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/svcjwt"
)

// parseEnv turns the "K=V" lines run writes into a map.
func parseEnv(t *testing.T, s string) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(s), "\n") {
		k, v, ok := strings.Cut(strings.TrimSpace(line), "=")
		require.True(t, ok, "line %q is not K=V", line)
		out[k] = v
	}
	return out
}

func TestRun_EmitsBothKeys(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, run(&buf))

	env := parseEnv(t, buf.String())
	assert.NotEmpty(t, env["SVCJWT_PRIVATE_KEY"])
	assert.NotEmpty(t, env["SVCJWT_PUBLIC_KEY"])
	assert.NotEqual(t, env["SVCJWT_PRIVATE_KEY"], env["SVCJWT_PUBLIC_KEY"])
}

// TestRun_KeysWorkTogether is the point of the tool: the printed pair must
// actually sign and verify.
func TestRun_KeysWorkTogether(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, run(&buf))
	env := parseEnv(t, buf.String())

	signer, err := svcjwt.NewSigner(env["SVCJWT_PRIVATE_KEY"], "admin-service")
	require.NoError(t, err)
	verifier, err := svcjwt.NewVerifier(env["SVCJWT_PUBLIC_KEY"], "admin-service", "client-update-service")
	require.NoError(t, err)

	token, _, err := signer.Sign("svc-updater", "client-update-service", time.Hour)
	require.NoError(t, err)
	claims, err := verifier.Verify(token)
	require.NoError(t, err)
	assert.Equal(t, "svc-updater", claims.Subject)
}

func TestRun_KeysDifferEachInvocation(t *testing.T) {
	var a, b bytes.Buffer
	require.NoError(t, run(&a))
	require.NoError(t, run(&b))
	assert.NotEqual(t, a.String(), b.String())
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `make test SERVICE=tools/svcjwtkey`
Expected: FAIL — `undefined: run`.

- [ ] **Step 3: Write the minimal implementation**

Create `tools/svcjwtkey/main.go`:

```go
// Command svcjwtkey prints a fresh Ed25519 keypair in the base64 form
// SVCJWT_PRIVATE_KEY and SVCJWT_PUBLIC_KEY expect, so provisioning does not
// depend on an ad-hoc script.
//
// Usage: go run ./tools/svcjwtkey
package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"os"
)

func main() {
	if err := run(os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run writes the keypair to w. Split from main so it is testable.
func run(w io.Writer) error {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return fmt.Errorf("generate ed25519 key: %w", err)
	}
	enc := base64.StdEncoding
	// The private line is the secret half: give it to the minting service only.
	if _, err := fmt.Fprintf(w, "SVCJWT_PRIVATE_KEY=%s\n", enc.EncodeToString(priv.Seed())); err != nil {
		return fmt.Errorf("write private key: %w", err)
	}
	if _, err := fmt.Fprintf(w, "SVCJWT_PUBLIC_KEY=%s\n", enc.EncodeToString(pub)); err != nil {
		return fmt.Errorf("write public key: %w", err)
	}
	return nil
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `make test SERVICE=tools/svcjwtkey`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
make fmt && make lint
git add tools/svcjwtkey/
git commit -m "feat(tools): add svcjwtkey Ed25519 keypair generator

Prints the SVCJWT_PRIVATE_KEY / SVCJWT_PUBLIC_KEY pair pkg/svcjwt expects, so
key provisioning does not depend on an ad-hoc script."
```

---

### Task 3: client-update-service auth middleware

**Files:**
- Create: `pkg/errcode/codes_clientupdate.go`
- Create: `client-update-service/auth.go`
- Test: `client-update-service/auth_test.go`

**Interfaces:**
- Consumes: `svcjwt.Claims`, `svcjwt.NewSigner`, `svcjwt.NewVerifier` (Task 1).
- Produces:
  - `errcode.ClientUpdateInvalidToken`, `errcode.ClientUpdateNotAuthorized` (Reason constants)
  - `tokenVerifier` interface — `Verify(token string) (*svcjwt.Claims, error)`
  - `requireServiceAccount(v tokenVerifier, allowed []string) gin.HandlerFunc`
  - `ctxServiceAccount` const — the Gin context key holding the verified subject
  - `bearerToken(c *gin.Context) string`

- [ ] **Step 1: Write the failing test**

Create `client-update-service/auth_test.go`:

```go
package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/svcjwt"
)

const (
	authTestIssuer   = "admin-service"
	authTestAudience = "client-update-service"
	authTestAccount  = "svc-updater"
)

// authTestPair returns a signer plus the verifier that trusts it.
func authTestPair(t *testing.T) (*svcjwt.Signer, *svcjwt.Verifier) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	enc := base64.StdEncoding
	s, err := svcjwt.NewSigner(enc.EncodeToString(priv.Seed()), authTestIssuer)
	require.NoError(t, err)
	v, err := svcjwt.NewVerifier(enc.EncodeToString(pub), authTestIssuer, authTestAudience)
	require.NoError(t, err)
	return s, v
}

// stubVerifier lets a test force a verification failure without constructing a
// token that breaks a specific rule — those rules are pkg/svcjwt's own tests.
type stubVerifier struct {
	claims *svcjwt.Claims
	err    error
}

func (s stubVerifier) Verify(string) (*svcjwt.Claims, error) { return s.claims, s.err }

// authRouter mounts the middleware on a probe route and reports whether the
// handler ran and what service account it saw.
func authRouter(v tokenVerifier, allowed []string, reached *bool, seen *string) *gin.Engine {
	r := gin.New()
	r.POST("/api/v1/version", requireServiceAccount(v, allowed), func(c *gin.Context) {
		*reached = true
		*seen = c.GetString(ctxServiceAccount)
		c.JSON(http.StatusOK, gin.H{"result": "success"})
	})
	return r
}

// envelope decodes the errcode error envelope: {"code":…,"reason":…,"error":…}.
func envelope(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var out map[string]any
	require.NoError(t, json.Unmarshal(body, &out))
	return out
}

func TestRequireServiceAccount_AllowsAllowlistedSubject(t *testing.T) {
	signer, verifier := authTestPair(t)
	token, _, err := signer.Sign(authTestAccount, authTestAudience, time.Hour)
	require.NoError(t, err)

	var reached bool
	var seen string
	r := authRouter(verifier, []string{authTestAccount}, &reached, &seen)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/version", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.True(t, reached, "an allowlisted service account must reach the handler")
	assert.Equal(t, authTestAccount, seen, "the verified subject must be on the context for the access log")
}

func TestRequireServiceAccount_RejectsUnallowlistedSubject(t *testing.T) {
	signer, verifier := authTestPair(t)
	// A perfectly valid token — signed by the trusted key, right issuer and
	// audience — for an account that is simply not permitted.
	token, _, err := signer.Sign("svc-someone-else", authTestAudience, time.Hour)
	require.NoError(t, err)

	var reached bool
	var seen string
	r := authRouter(verifier, []string{authTestAccount}, &reached, &seen)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/version", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusForbidden, w.Code)
	assert.False(t, reached)
	assert.Equal(t, string(errcodeReasonNotAuthorized), envelope(t, w.Body.Bytes())["reason"])
}

func TestRequireServiceAccount_RejectsBadCredentials(t *testing.T) {
	tests := []struct {
		name     string
		header   string // "" means send no Authorization header
		verifier tokenVerifier
	}{
		{"no header", "", stubVerifier{claims: &svcjwt.Claims{Subject: authTestAccount}}},
		{"empty bearer", "Bearer ", stubVerifier{claims: &svcjwt.Claims{Subject: authTestAccount}}},
		{"wrong scheme", "Basic abc123", stubVerifier{claims: &svcjwt.Claims{Subject: authTestAccount}}},
		{"raw token, no scheme", "sometoken", stubVerifier{claims: &svcjwt.Claims{Subject: authTestAccount}}},
		{"verifier rejects", "Bearer whatever", stubVerifier{err: svcjwt.ErrInvalidToken}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var reached bool
			var seen string
			r := authRouter(tc.verifier, []string{authTestAccount}, &reached, &seen)

			req := httptest.NewRequest(http.MethodPost, "/api/v1/version", nil)
			if tc.header != "" {
				req.Header.Set("Authorization", tc.header)
			}
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)

			assert.Equal(t, http.StatusUnauthorized, w.Code)
			assert.False(t, reached, "the handler must not run for a bad credential")
			assert.Equal(t, string(errcodeReasonInvalidToken), envelope(t, w.Body.Bytes())["reason"])
		})
	}
}

// TestRequireServiceAccount_ResponseHidesTheCause guards the rule that a
// verification cause is logged server-side but never serialized.
func TestRequireServiceAccount_ResponseHidesTheCause(t *testing.T) {
	v := stubVerifier{err: errors.New("signature mismatch on segment two")}
	var reached bool
	var seen string
	r := authRouter(v, []string{authTestAccount}, &reached, &seen)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/version", nil)
	req.Header.Set("Authorization", "Bearer sometoken")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
	assert.NotContains(t, w.Body.String(), "segment two", "the cause must never reach the client")
	assert.NotContains(t, w.Body.String(), "sometoken", "the token must never be echoed")
}

func TestRequireServiceAccount_IgnoresBlankAllowlistEntries(t *testing.T) {
	signer, verifier := authTestPair(t)
	token, _, err := signer.Sign(authTestAccount, authTestAudience, time.Hour)
	require.NoError(t, err)

	var reached bool
	var seen string
	// Whitespace and empty entries are what a sloppy env var looks like:
	// "svc-updater, , other". They must be trimmed, and blanks must never
	// become a permitted empty subject.
	r := authRouter(verifier, []string{" " + authTestAccount + " ", "", "  "}, &reached, &seen)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/version", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.True(t, reached)
}

func TestBearerToken(t *testing.T) {
	tests := []struct {
		name, header, want string
	}{
		{"bearer token", "Bearer abc", "abc"},
		{"bearer with padding", "Bearer   abc  ", "abc"},
		{"no header", "", ""},
		{"lowercase scheme is not accepted", "bearer abc", ""},
		{"other scheme", "Basic abc", ""},
		{"scheme only", "Bearer", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request = httptest.NewRequest(http.MethodGet, "/", nil)
			if tc.header != "" {
				c.Request.Header.Set("Authorization", tc.header)
			}
			assert.Equal(t, tc.want, bearerToken(c))
		})
	}
}
```

Add these two aliases at the top of `auth_test.go` (right after the imports) so the assertions read cleanly and a renamed constant breaks compilation rather than silently passing:

```go
// Aliases so a rename of either reason is caught at compile time.
var (
	errcodeReasonInvalidToken  = errcode.ClientUpdateInvalidToken
	errcodeReasonNotAuthorized = errcode.ClientUpdateNotAuthorized
)
```

and add `"github.com/hmchangw/chat/pkg/errcode"` to the test's imports.

- [ ] **Step 2: Run the test to verify it fails**

Run: `make test SERVICE=client-update-service`
Expected: FAIL — `undefined: requireServiceAccount`, `undefined: ctxServiceAccount`, `undefined: bearerToken`, `undefined: tokenVerifier`, `undefined: errcode.ClientUpdateInvalidToken`.

- [ ] **Step 3: Add the error reasons**

Create `pkg/errcode/codes_clientupdate.go`:

```go
package errcode

// client-update-service reasons. Emitted by its service-account auth middleware.
const (
	ClientUpdateInvalidToken  Reason = "invalid_token"   // 401: missing, malformed, unsigned, or expired service token
	ClientUpdateNotAuthorized Reason = "not_authorized"  // 403: valid token whose subject is not in ALLOWED_SERVICE_ACCOUNTS
)
```

- [ ] **Step 4: Write the middleware**

Create `client-update-service/auth.go`:

```go
package main

import (
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
	"github.com/hmchangw/chat/pkg/svcjwt"
)

// ctxServiceAccount is where requireServiceAccount parks the verified subject
// for the access log.
const ctxServiceAccount = "service_account"

// tokenVerifier is the slice of *svcjwt.Verifier this middleware needs, declared
// here — at the consumer — so tests can substitute a stub.
type tokenVerifier interface {
	Verify(token string) (*svcjwt.Claims, error)
}

// requireServiceAccount admits a request only when it carries a service token
// this site trusts AND that token names an allowlisted subject.
//
// The two failures are answered differently on purpose. A JWT cannot be
// guessed — forging one needs the private key — so distinguishing "your token
// is bad" (401) from "your account is not permitted" (403) leaks nothing
// exploitable, and turns a missing allowlist entry into an immediately
// diagnosable 403 rather than a mystery 401.
func requireServiceAccount(v tokenVerifier, allowed []string) gin.HandlerFunc {
	permitted := make(map[string]struct{}, len(allowed))
	for _, a := range allowed {
		// Trim so "a, b" from an env var works, and skip blanks so an empty
		// entry can never permit an empty subject.
		if a = strings.TrimSpace(a); a != "" {
			permitted[a] = struct{}{}
		}
	}
	return func(c *gin.Context) {
		ctx := c.Request.Context()

		tok := bearerToken(c)
		if tok == "" {
			errhttp.Write(ctx, c, errcode.Unauthenticated("missing service token",
				errcode.WithReason(errcode.ClientUpdateInvalidToken)))
			c.Abort()
			return
		}

		claims, err := v.Verify(tok)
		if err != nil {
			// The cause names the broken rule for the server log only; Classify
			// logs it once and never serializes it.
			errhttp.Write(ctx, c, errcode.Unauthenticated("invalid service token",
				errcode.WithReason(errcode.ClientUpdateInvalidToken),
				errcode.WithCause(err)))
			c.Abort()
			return
		}

		if _, ok := permitted[claims.Subject]; !ok {
			errhttp.Write(ctx, c, errcode.Forbidden("service account is not authorized to upload",
				errcode.WithReason(errcode.ClientUpdateNotAuthorized)))
			c.Abort()
			return
		}

		c.Set(ctxServiceAccount, claims.Subject)
		c.Next()
	}
}

// bearerToken extracts the token from "Authorization: Bearer <token>", or ""
// when the header is absent or uses another scheme.
func bearerToken(c *gin.Context) string {
	if after, ok := strings.CutPrefix(c.GetHeader("Authorization"), "Bearer "); ok {
		return strings.TrimSpace(after)
	}
	return ""
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `make test SERVICE=client-update-service`
Expected: PASS. Also run `make test SERVICE=pkg/errcode` — the new reasons must not collide with existing ones.

- [ ] **Step 6: Lint and commit**

```bash
make fmt && make lint
git add pkg/errcode/codes_clientupdate.go client-update-service/auth.go client-update-service/auth_test.go
git commit -m "feat(client-update-service): service-account auth middleware

Verifies an Ed25519 service token and checks its subject against an allowlist.
A bad token answers 401 invalid_token; a valid token for an unlisted account
answers 403 not_authorized, so a missing allowlist entry is diagnosable rather
than indistinguishable from a forged token."
```

---

### Task 4: Wire the middleware into client-update-service

**Files:**
- Modify: `client-update-service/config.go`
- Modify: `client-update-service/routes.go`
- Modify: `client-update-service/main.go`
- Modify: `client-update-service/middleware.go` (access log gains `service_account`)
- Modify: `client-update-service/handler_test.go:95` and `client-update-service/integration_test.go:85` — both call the old 2-argument `registerRoutes` and will not compile
- Test: `client-update-service/config_test.go` (extend), `client-update-service/routes_test.go` (create)

**Interfaces:**
- Consumes: `requireServiceAccount`, `ctxServiceAccount` (Task 3); `svcjwt.NewVerifier` (Task 1).
- Produces: `registerRoutes(r *gin.Engine, h *Handler, auth gin.HandlerFunc)` — **note the changed signature**; `config` gains `SvcJWTPublicKey`, `SvcJWTIssuer`, `SvcJWTAudience`, `AllowedServiceAccounts`, `HTTPReadTimeout`.

This task also fixes the pre-existing `ReadTimeout` bug (spec §5.5): `main.go` hardcodes `ReadTimeout: 30 * time.Second` while `WriteTimeout` is a configurable 10m. Since `ReadTimeout` covers reading the request body, uploads longer than 30s already fail today.

- [ ] **Step 1: Write the failing tests**

Create `client-update-service/routes_test.go`:

```go
package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

// passthroughAuth stands in for the real middleware: it records that it ran.
func passthroughAuth(ran *bool) gin.HandlerFunc {
	return func(c *gin.Context) { *ran = true; c.Next() }
}

// TestRegisterRoutes_UploadIsGuarded pins the security boundary: the upload
// route must run the auth middleware.
func TestRegisterRoutes_UploadIsGuarded(t *testing.T) {
	var ran bool
	r := gin.New()
	registerRoutes(r, NewHandler(nil, testCache(1024)), passthroughAuth(&ran))

	// No body: the handler will reject it, but the middleware must have run first.
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/api/v1/version", nil))

	assert.True(t, ran, "POST /api/v1/version must be behind the auth middleware")
}

// TestRegisterRoutes_DownloadStaysOpen is a regression guard for spec §2: the
// download must never require a credential, because deployed desktop update
// clients hold none and cannot obtain one.
func TestRegisterRoutes_DownloadStaysOpen(t *testing.T) {
	var ran bool
	r := gin.New()
	registerRoutes(r, NewHandler(nil, testCache(1024)), passthroughAuth(&ran))

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/version/app.yaml", nil))

	assert.False(t, ran, "GET /api/v1/version/:fileName must NOT be behind auth")
}

func TestRegisterRoutes_HealthStaysOpen(t *testing.T) {
	var ran bool
	r := gin.New()
	registerRoutes(r, NewHandler(nil, testCache(1024)), passthroughAuth(&ran))

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	assert.Equal(t, http.StatusOK, w.Code)
	assert.False(t, ran, "the health probe must never require a credential")
}
```

Append to `client-update-service/config_test.go` (keep the existing tests; match their established style for setting env vars):

```go
func TestConfig_AuthDefaults(t *testing.T) {
	t.Setenv("SITE_ID", "site-local")
	t.Setenv("MINIO_ENDPOINT", "minio:9000")
	t.Setenv("MINIO_ACCESS_KEY", "key")
	t.Setenv("MINIO_SECRET_KEY", "secret")
	t.Setenv("MINIO_BUCKET", "chat-updates")
	t.Setenv("SVCJWT_PUBLIC_KEY", "Zm9vYmFyZm9vYmFyZm9vYmFyZm9vYmFyZm9vYmE=")
	t.Setenv("ALLOWED_SERVICE_ACCOUNTS", "svc-updater, svc-other")

	cfg, err := env.ParseAs[config]()
	require.NoError(t, err)

	assert.Equal(t, "admin-service", cfg.SvcJWTIssuer)
	assert.Equal(t, "client-update-service", cfg.SvcJWTAudience)
	assert.Equal(t, []string{"svc-updater", " svc-other"}, cfg.AllowedServiceAccounts,
		"env parsing splits on comma only; the middleware trims the entries")
	assert.Equal(t, 10*time.Minute, cfg.HTTPReadTimeout,
		"the read timeout must cover a full upload body, matching the write timeout default")
}

// NOTE: do NOT test a missing required var with t.Setenv(k, "") — env/v11 treats
// an empty string as "defined", so such a test passes vacuously. The existing
// TestConfig_RequiresEachRequiredVar in this file documents the correct idiom:
// seed every required var, then os.Unsetenv the one under test. That existing
// test is extended below rather than duplicated here.
```

Then extend the EXISTING `TestConfig_RequiresEachRequiredVar` in the same file: add the two
new variables to its `required` slice, so the missing-var loop covers them using the
`os.Unsetenv` idiom it already implements:

```go
	required := []string{
		"SITE_ID", "MINIO_ENDPOINT", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY", "MINIO_BUCKET",
		"SVCJWT_PUBLIC_KEY", "ALLOWED_SERVICE_ACCOUNTS",
	}
```

And extend the EXISTING `TestConfig_Defaults` in the same file — it calls
`env.ParseAs[config]()` with only the five MinIO/site vars and will now fail, because the two
new variables are required. Add them to its setup:

```go
	t.Setenv("SVCJWT_PUBLIC_KEY", "Zm9vYmFyZm9vYmFyZm9vYmFyZm9vYmFyZm9vYmE=")
	t.Setenv("ALLOWED_SERVICE_ACCOUNTS", "svc-updater")
```

`config_test.go` already imports `os`, `time`, `env/v11`, `assert` and `require` — no import
changes are needed.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test SERVICE=client-update-service`
Expected: FAIL — `registerRoutes` takes 2 arguments, not 3; `cfg.SvcJWTIssuer` undefined.

- [ ] **Step 3: Extend the config**

In `client-update-service/config.go`, add to the `config` struct:

```go
	// HTTPReadTimeout must cover reading the whole upload body: net/http's
	// ReadTimeout spans the body, so a value below the write timeout silently
	// caps upload size no matter what HTTP_WRITE_TIMEOUT says.
	HTTPReadTimeout time.Duration `env:"HTTP_READ_TIMEOUT" envDefault:"10m"`

	// Service-account auth on the upload route. The public key only: this
	// service verifies tokens and can never mint them.
	SvcJWTPublicKey        string   `env:"SVCJWT_PUBLIC_KEY,required"`
	SvcJWTIssuer           string   `env:"SVCJWT_ISSUER" envDefault:"admin-service"`
	SvcJWTAudience         string   `env:"SVCJWT_AUDIENCE" envDefault:"client-update-service"`
	// AllowedServiceAccounts is required, not defaulted: an empty allowlist
	// would refuse every upload, and a permissive default would silently
	// reopen the hole this gate closes.
	AllowedServiceAccounts []string `env:"ALLOWED_SERVICE_ACCOUNTS,required" envSeparator:","`
```

- [ ] **Step 4: Update the routes**

Replace `client-update-service/routes.go` entirely:

```go
package main

import "github.com/gin-gonic/gin"

// registerRoutes wires the health probe plus the /api/v1 version endpoints.
// auth guards the upload only: the download stays open because deployed desktop
// update clients hold no credential and cannot obtain one.
func registerRoutes(r *gin.Engine, h *Handler, auth gin.HandlerFunc) {
	r.GET("/healthz", h.HandleHealth)

	api := r.Group("/api/v1")
	api.POST("/version", auth, h.HandleUpload)
	api.GET("/version/:fileName", h.HandleDownload)
}
```

- [ ] **Step 5: Wire it in main.go**

In `client-update-service/main.go`, after the `handler := NewHandler(store, cache)` line, add:

```go
	verifier, err := svcjwt.NewVerifier(cfg.SvcJWTPublicKey, cfg.SvcJWTIssuer, cfg.SvcJWTAudience)
	if err != nil {
		return fmt.Errorf("build service-token verifier: %w", err)
	}
```

Change the `registerRoutes` call to:

```go
	registerRoutes(r, handler, requireServiceAccount(verifier, cfg.AllowedServiceAccounts))
```

Change the server's `ReadTimeout` from the hardcoded `30 * time.Second` to:

```go
		ReadTimeout:  cfg.HTTPReadTimeout,
```

Add `"github.com/hmchangw/chat/pkg/svcjwt"` to the imports.

Also add this line to the startup log so an operator can see the gate is on, without logging the key:

```go
		slog.Info("client-update-service starting", "addr", addr, "site", cfg.SiteID,
			"allowed_service_accounts", len(cfg.AllowedServiceAccounts))
```

- [ ] **Step 6: Add the service account to the access log**

In `client-update-service/middleware.go`, inside `accessLogMiddleware`'s `slog.Info` call, add one field:

```go
			"service_account", c.GetString(ctxServiceAccount),
```

It is empty on unauthenticated routes, which is correct — only the upload has one. The token itself is never logged.

- [ ] **Step 7: Fix the two existing `registerRoutes` call sites**

Changing the signature breaks two existing callers. Neither is testing auth, so
both pass a middleware that does nothing:

In `client-update-service/handler_test.go` (~line 95, inside `TestRoutesRegistered`):

```go
	registerRoutes(r, h, func(c *gin.Context) { c.Next() })
```

In `client-update-service/integration_test.go` (~line 85, inside the cache-reuse
test):

```go
	registerRoutes(r, h, func(c *gin.Context) { c.Next() })
```

Both exercise the download path only, so an open-gate stand-in is exactly right —
the real middleware is covered by `auth_test.go` and by Task 5's integration tests.

- [ ] **Step 8: Run the tests to verify they pass**

Run: `make test SERVICE=client-update-service`
Expected: PASS, including the pre-existing handler, cache, version and middleware tests.

- [ ] **Step 9: Verify it builds and commit**

```bash
make build SERVICE=client-update-service
make fmt && make lint
git add client-update-service/
git commit -m "feat(client-update-service): require a service account to upload

Gates POST /api/v1/version behind the service-token middleware and leaves the
download open for deployed desktop clients.

Also fixes a pre-existing bug: ReadTimeout was hardcoded to 30s while
WriteTimeout was a configurable 10m. ReadTimeout covers the request body, so
uploads over 30s already failed regardless of the write timeout. It is now
HTTP_READ_TIMEOUT, defaulting to 10m to match."
```

---

### Task 5: client-update-service integration test

**Files:**
- Modify: `client-update-service/integration_test.go`

**Interfaces:**
- Consumes: everything from Tasks 1, 3, 4.
- Produces: nothing consumed elsewhere.

- [ ] **Step 1: Write the failing test**

Append to `client-update-service/integration_test.go`. The existing file already has `func TestMain(m *testing.M) { testutil.RunTests(m) }` — do not add a second one.

```go
// authedRouter builds the real router with real auth against a real MinIO
// container, and returns it with a signer that mints tokens it will accept.
func authedRouter(t *testing.T) (*gin.Engine, *svcjwt.Signer) {
	t.Helper()
	client, bucket := testutil.MinIO(t, "clientupdateauth")
	store := newMinioVersionStore(client, bucket, 30*time.Second)
	h := NewHandler(store, newBlobCache(4, time.Hour, 1<<20))

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	enc := base64.StdEncoding
	signer, err := svcjwt.NewSigner(enc.EncodeToString(priv.Seed()), "admin-service")
	require.NoError(t, err)
	verifier, err := svcjwt.NewVerifier(enc.EncodeToString(pub), "admin-service", "client-update-service")
	require.NoError(t, err)

	gin.SetMode(gin.TestMode)
	r := gin.New()
	registerRoutes(r, h, requireServiceAccount(verifier, []string{"svc-updater"}))
	return r, signer
}

// uploadForm builds the configFile + executeFile multipart body the upload expects.
func uploadForm(t *testing.T, cfgName, cfgBody, exeName, exeBody string) (*bytes.Buffer, string) {
	t.Helper()
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	for _, f := range []struct{ field, name, body string }{
		{"configFile", cfgName, cfgBody},
		{"executeFile", exeName, exeBody},
	} {
		part, err := w.CreateFormFile(f.field, f.name)
		require.NoError(t, err)
		_, err = part.Write([]byte(f.body))
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())
	return &buf, w.FormDataContentType()
}

func TestIntegration_AuthedUploadThenOpenDownload(t *testing.T) {
	r, signer := authedRouter(t)
	token, _, err := signer.Sign("svc-updater", "client-update-service", time.Hour)
	require.NoError(t, err)

	body, contentType := uploadForm(t, "app.yaml", "version: 2", "app.exe", "MZbinary")
	req := httptest.NewRequest(http.MethodPost, "/api/v1/version", body)
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())

	// The download must still work with NO credential — the whole point of
	// gating only the upload.
	w = httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/version/app.exe", nil))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "MZbinary", w.Body.String())
}

func TestIntegration_UnauthenticatedUploadIsRefused(t *testing.T) {
	r, _ := authedRouter(t)

	body, contentType := uploadForm(t, "app.yaml", "version: 2", "app.exe", "MZbinary")
	req := httptest.NewRequest(http.MethodPost, "/api/v1/version", body)
	req.Header.Set("Content-Type", contentType)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusUnauthorized, w.Code)

	// And nothing was written: the artifact must not exist.
	w = httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/version/app.exe", nil))
	assert.Equal(t, http.StatusNotFound, w.Code,
		"a refused upload must not have reached MinIO")
}

func TestIntegration_UnallowlistedAccountIsRefused(t *testing.T) {
	r, signer := authedRouter(t)
	token, _, err := signer.Sign("svc-intruder", "client-update-service", time.Hour)
	require.NoError(t, err)

	body, contentType := uploadForm(t, "app.yaml", "version: 2", "app.exe", "MZbinary")
	req := httptest.NewRequest(http.MethodPost, "/api/v1/version", body)
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusForbidden, w.Code)
}
```

Add to the file's imports: `bytes`, `crypto/ed25519`, `crypto/rand`, `encoding/base64`, `mime/multipart`, and `github.com/hmchangw/chat/pkg/svcjwt`.

- [ ] **Step 2: Run the tests**

Run: `make test-integration SERVICE=client-update-service`
Expected: PASS (requires Docker; `pkg/testutil` starts the shared MinIO container).

- [ ] **Step 3: Commit**

```bash
make fmt && make lint
git add client-update-service/integration_test.go
git commit -m "test(client-update-service): integration coverage for service-account auth

Proves an allowlisted token uploads to real MinIO, an unlisted one gets 403, an
unauthenticated upload gets 401 and writes nothing, and the download still
works with no credential at all."
```

---

### Task 6: admin-service config and signer

**Files:**
- Modify: `admin-service/config.go`
- Test: `admin-service/config_test.go` (extend)

**Interfaces:**
- Consumes: `svcjwt.NewSigner` (Task 1).
- Produces: `Config` gains `SvcJWTPrivateKey`, `SvcJWTIssuer`, `SvcJWTTTL`, `ClientUpdateBaseURL`, `ClientUpdateAudience`, `ClientUpdateServiceAccount`, `ClientUpdateUploadTimeout`.

- [ ] **Step 1: Write the failing test**

Append to `admin-service/config_test.go`. Match the existing file's helper for setting the base required env vars — if it has one (e.g. `setRequiredEnv(t)`), reuse it; otherwise set `SITE_ID`, `MONGO_URI` and `NATS_URL` as the existing tests do.

```go
// TestLoadConfig_ClientUpdateDefaults covers a site that HAS opted in.
func TestLoadConfig_ClientUpdateDefaults(t *testing.T) {
	t.Setenv("SITE_ID", "site-local")
	t.Setenv("MONGO_URI", "mongodb://mongo:27017")
	t.Setenv("NATS_URL", "nats://nats:4222")
	t.Setenv("SVCJWT_PRIVATE_KEY", "Zm9vYmFyZm9vYmFyZm9vYmFyZm9vYmFyZm9vYmE=")
	t.Setenv("CLIENT_UPDATE_BASE_URL", "http://client-update-service:8080")
	t.Setenv("CLIENT_UPDATE_SERVICE_ACCOUNT", "svc-updater")

	cfg, err := loadConfig()
	require.NoError(t, err)

	assert.Equal(t, "admin-service", cfg.SvcJWTIssuer)
	assert.Equal(t, "client-update-service", cfg.ClientUpdateAudience)
	assert.Equal(t, 5*time.Minute, cfg.SvcJWTTTL)
	assert.Equal(t, 10*time.Minute, cfg.ClientUpdateUploadTimeout)
}

// TestLoadConfig_ClientUpdateOptional pins that publishing is opt-in per site.
// admin-service runs everywhere; requiring the signing key would put a copy of
// the Ed25519 PRIVATE key on every site merely to boot.
func TestLoadConfig_ClientUpdateOptional(t *testing.T) {
	t.Setenv("SITE_ID", "site-local")
	t.Setenv("MONGO_URI", "mongodb://mongo:27017")
	t.Setenv("NATS_URL", "nats://nats:4222")

	cfg, err := loadConfig()
	require.NoError(t, err, "a site that does not publish client updates must still start")
	assert.Empty(t, cfg.ClientUpdateBaseURL)
	assert.Empty(t, cfg.SvcJWTPrivateKey)
}

// TestLoadConfig_ClientUpdateHalfConfigured proves opt-in is all-or-nothing: a
// site that sets the base URL but omits the key or the account fails at startup
// rather than at the first upload.
func TestLoadConfig_ClientUpdateHalfConfigured(t *testing.T) {
	tests := []struct {
		name, key, account string
	}{
		{"base URL without signing key", "", "svc-updater"},
		{"base URL without service account", "Zm9vYmFyZm9vYmFyZm9vYmFyZm9vYmFyZm9vYmE=", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SITE_ID", "site-local")
			t.Setenv("MONGO_URI", "mongodb://mongo:27017")
			t.Setenv("NATS_URL", "nats://nats:4222")
			t.Setenv("CLIENT_UPDATE_BASE_URL", "http://client-update-service:8080")
			t.Setenv("SVCJWT_PRIVATE_KEY", tc.key)
			t.Setenv("CLIENT_UPDATE_SERVICE_ACCOUNT", tc.account)

			_, err := loadConfig()
			assert.Error(t, err)
		})
	}
}

// TestLoadConfig_UploadTimeoutEscapesHandlerBudget documents that the upload
// route deliberately exceeds httpWriteTimeout: it sets its own per-request
// deadlines (see extendDeadlines), so checkHandlerTimeout must NOT be applied
// to it. This guards against someone "fixing" the apparent inconsistency.
func TestLoadConfig_UploadTimeoutEscapesHandlerBudget(t *testing.T) {
	t.Setenv("SITE_ID", "site-local")
	t.Setenv("MONGO_URI", "mongodb://mongo:27017")
	t.Setenv("NATS_URL", "nats://nats:4222")
	t.Setenv("SVCJWT_PRIVATE_KEY", "Zm9vYmFyZm9vYmFyZm9vYmFyZm9vYmFyZm9vYmE=")
	t.Setenv("CLIENT_UPDATE_BASE_URL", "http://client-update-service:8080")
	t.Setenv("CLIENT_UPDATE_SERVICE_ACCOUNT", "svc-updater")
	t.Setenv("CLIENT_UPDATE_UPLOAD_TIMEOUT", "10m")

	cfg, err := loadConfig()
	require.NoError(t, err)
	assert.Greater(t, cfg.ClientUpdateUploadTimeout, httpWriteTimeout,
		"the upload route sets its own deadlines and is expected to outlive the server write timeout")
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `make test SERVICE=admin-service`
Expected: FAIL — `cfg.SvcJWTIssuer` undefined.

- [ ] **Step 3: Extend the config**

Add to the `Config` struct in `admin-service/config.go`:

```go
	// SvcJWTPrivateKey signs the service-account tokens this service presents to
	// client-update-service. Private half only lives here; client-update-service
	// holds the public key and can verify but never mint.
	//
	// Optional, NOT required: admin-service runs at every site, but client updates
	// are published from one. Requiring it would force every site to hold a copy of
	// the signing key merely to boot, multiplying the private key across sites for
	// no benefit. Half-configured is still rejected — see checkClientUpdateConfig.
	SvcJWTPrivateKey string        `env:"SVCJWT_PRIVATE_KEY" envDefault:""`
	SvcJWTIssuer     string        `env:"SVCJWT_ISSUER" envDefault:"admin-service"`
	// SvcJWTTTL only has to cover mint -> the downstream's middleware reading the
	// request headers, which is milliseconds. The body may then stream for as
	// long as ClientUpdateUploadTimeout allows: the token is verified once, before
	// the body is read, and exp is never consulted again. Do NOT widen this to
	// match the upload timeout — it would enlarge the forgery window for nothing.
	SvcJWTTTL        time.Duration `env:"SVCJWT_TTL" envDefault:"5m"`

	// ClientUpdateBaseURL empty means client-update publishing is disabled at this
	// site: no forwarder is built and the upload route answers 503.
	ClientUpdateBaseURL        string `env:"CLIENT_UPDATE_BASE_URL" envDefault:""`
	ClientUpdateAudience       string `env:"CLIENT_UPDATE_AUDIENCE" envDefault:"client-update-service"`
	ClientUpdateServiceAccount string `env:"CLIENT_UPDATE_SERVICE_ACCOUNT" envDefault:""`
	// ClientUpdateUploadTimeout is the per-request deadline the upload route
	// installs for itself via extendDeadlines. It is deliberately NOT passed
	// through checkHandlerTimeout: that guard keeps handler budgets under the 40s
	// server write timeout, and this route escapes that timeout by design.
	ClientUpdateUploadTimeout time.Duration `env:"CLIENT_UPDATE_UPLOAD_TIMEOUT" envDefault:"10m"`
```

Do **not** add `checkHandlerTimeout` calls for `ClientUpdateUploadTimeout` in `loadConfig` — the test above pins that.

Add the paired validation instead, and call it from `loadConfig` after the existing checks:

```go
// checkClientUpdateConfig rejects a half-configured forwarder. The feature is
// opt-in per site (an empty base URL disables it), but a site that opts in and
// omits the signing key or the service account would fail at the first upload
// rather than at startup — so that combination is a startup error.
func checkClientUpdateConfig(c Config) error { //nolint:gocritic // hugeParam: startup value, called once
	if c.ClientUpdateBaseURL == "" {
		return nil
	}
	if c.SvcJWTPrivateKey == "" {
		return errors.New("CLIENT_UPDATE_BASE_URL is set but SVCJWT_PRIVATE_KEY is empty: client update publishing cannot sign its requests")
	}
	if c.ClientUpdateServiceAccount == "" {
		return errors.New("CLIENT_UPDATE_BASE_URL is set but CLIENT_UPDATE_SERVICE_ACCOUNT is empty: client update publishing has no identity to present")
	}
	return nil
}
```

In `loadConfig`, after `checkHandlerTimeout("FANOUT_TIMEOUT", …)`:

```go
	if err := checkClientUpdateConfig(c); err != nil {
		return Config{}, err
	}
```

Add `errors` to the file's imports.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `make test SERVICE=admin-service`
Expected: PASS — including the four pre-existing `loadConfig` tests, which set only
`SITE_ID`/`MONGO_URI`/`NATS_URL`. They keep passing precisely because the new variables are
optional; if you made them `required` instead, those four would break. That is the signal the
optional decision is load-bearing, not cosmetic.

- [ ] **Step 5: Commit**

```bash
make fmt && make lint
git add admin-service/config.go admin-service/config_test.go
git commit -m "feat(admin-service): config for client-update forwarding

Adds the Ed25519 signing key, target base URL, service account and upload
timeout. The upload timeout intentionally exceeds httpWriteTimeout because the
upload route installs its own per-request deadlines."
```

---

### Task 7: admin-service streaming forwarder

**Files:**
- Create: `admin-service/forwarder.go`
- Test: `admin-service/forwarder_test.go`

**Interfaces:**
- Consumes: `Config` fields (Task 6); `pkg/restyutil`; `pkg/errcode`.
- Produces:
  - `uploadedNames{ConfigFile, ExecuteFile string}`
  - `configFileField = "configFile"`, `executeFileField = "executeFile"` consts
  - `newClientUpdateForwarder(baseURL string, timeout time.Duration, mintToken func() (string, error)) *clientUpdateForwarder`
  - `(*clientUpdateForwarder).Forward(ctx context.Context, src *multipart.Reader) (uploadedNames, error)`

- [ ] **Step 1: Write the failing test**

Create `admin-service/forwarder_test.go`:

```go
package main

import (
	"context"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/errcode"
)

// fwdTestForm builds a multipart body and returns a reader over it plus its
// boundary, mimicking what Gin hands the handler.
func fwdTestForm(t *testing.T, parts []struct{ field, name, body, contentType string }) (*multipart.Reader, string) {
	t.Helper()
	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)
	go func() {
		for _, p := range parts {
			var w io.Writer
			var err error
			if p.contentType != "" {
				h := textproto.MIMEHeader{}
				h.Set("Content-Disposition", `form-data; name="`+p.field+`"; filename="`+p.name+`"`)
				h.Set("Content-Type", p.contentType)
				w, err = mw.CreatePart(h)
			} else {
				w, err = mw.CreateFormFile(p.field, p.name)
			}
			if err != nil {
				_ = pw.CloseWithError(err)
				return
			}
			if _, err := io.WriteString(w, p.body); err != nil {
				_ = pw.CloseWithError(err)
				return
			}
		}
		_ = pw.CloseWithError(mw.Close())
	}()
	return multipart.NewReader(pr, mw.Boundary()), mw.Boundary()
}

// twoGoodParts is the ordinary, valid submission.
func twoGoodParts() []struct{ field, name, body, contentType string } {
	return []struct{ field, name, body, contentType string }{
		{configFileField, "app.yaml", "version: 3", "application/x-yaml"},
		{executeFileField, "app.exe", "MZbinarybytes", ""},
	}
}

func staticToken(tok string) func() (string, error) {
	return func() (string, error) { return tok, nil }
}

func TestForward_StreamsBothPartsAndAuthenticates(t *testing.T) {
	var (
		gotAuth        string
		gotConfig      string
		gotExecute     string
		gotConfigType  string
	)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		mr, err := r.MultipartReader()
		require.NoError(t, err)
		for {
			part, err := mr.NextPart()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			body, err := io.ReadAll(part)
			require.NoError(t, err)
			switch part.FormName() {
			case configFileField:
				gotConfig = string(body)
				gotConfigType = part.Header.Get("Content-Type")
			case executeFileField:
				gotExecute = string(body)
			}
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"result":"success"}`))
	}))
	defer upstream.Close()

	f := newClientUpdateForwarder(upstream.URL, 30*time.Second, staticToken("minted-token"))
	src, _ := fwdTestForm(t, twoGoodParts())

	names, err := f.Forward(context.Background(), src)
	require.NoError(t, err)

	assert.Equal(t, "Bearer minted-token", gotAuth, "the forward must carry the minted service token")
	assert.Equal(t, "version: 3", gotConfig)
	assert.Equal(t, "MZbinarybytes", gotExecute)
	assert.Equal(t, "application/x-yaml", gotConfigType,
		"the part's declared content type must survive the re-encode, or the downstream stores the yaml as octet-stream")
	assert.Equal(t, "app.yaml", names.ConfigFile)
	assert.Equal(t, "app.exe", names.ExecuteFile)
}

// TestForward_MapsUpstreamStatuses pins spec §6.1's table. A downstream 401/403
// means OUR credential was refused — a configuration fault — so it must never
// be relayed as the admin's own 401.
func TestForward_MapsUpstreamStatuses(t *testing.T) {
	tests := []struct {
		name         string
		status       int
		body         string
		wantHTTP     int
		wantReason   errcode.Reason
		wantContains string
	}{
		{"200 succeeds", http.StatusOK, `{"result":"success"}`, 0, "", ""},
		{"400 relays the message", http.StatusBadRequest,
			`{"code":"bad_request","error":"configFile must be a .yaml or .yml file"}`,
			http.StatusBadRequest, "", "configFile must be a .yaml"},
		{"401 becomes 503 upstream_unauthorized", http.StatusUnauthorized,
			`{"code":"unauthenticated","error":"invalid service token"}`,
			http.StatusServiceUnavailable, errcode.AdminUpstreamUnauthorized, ""},
		{"403 becomes 503 upstream_unauthorized", http.StatusForbidden,
			`{"code":"forbidden","error":"not authorized"}`,
			http.StatusServiceUnavailable, errcode.AdminUpstreamUnauthorized, ""},
		{"500 becomes 503 upstream_unavailable", http.StatusInternalServerError,
			`{"code":"internal","error":"minio down"}`,
			http.StatusServiceUnavailable, errcode.AdminUpstreamUnavailable, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// Drain so the pipe writer always completes.
				_, _ = io.Copy(io.Discard, r.Body)
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer upstream.Close()

			f := newClientUpdateForwarder(upstream.URL, 30*time.Second, staticToken("tok"))
			src, _ := fwdTestForm(t, twoGoodParts())

			_, err := f.Forward(context.Background(), src)
			if tc.wantHTTP == 0 {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			var ec *errcode.Error
			require.ErrorAs(t, err, &ec)
			assert.Equal(t, tc.wantHTTP, ec.HTTPStatus())
			if tc.wantReason != "" {
				assert.Equal(t, tc.wantReason, ec.Reason)
			}
			if tc.wantContains != "" {
				assert.Contains(t, ec.Error(), tc.wantContains)
			}
		})
	}
}

// TestForward_UpstreamErrorNeverLeaksTheToken guards the logging rule.
func TestForward_UpstreamErrorNeverLeaksTheToken(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"code":"unauthenticated","error":"nope"}`))
	}))
	defer upstream.Close()

	f := newClientUpdateForwarder(upstream.URL, 30*time.Second, staticToken("super-secret-token"))
	src, _ := fwdTestForm(t, twoGoodParts())

	_, err := f.Forward(context.Background(), src)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "super-secret-token")
}

func TestForward_TransportFailureIsUnavailable(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := upstream.URL
	upstream.Close() // nothing is listening any more

	f := newClientUpdateForwarder(url, 2*time.Second, staticToken("tok"))
	src, _ := fwdTestForm(t, twoGoodParts())

	_, err := f.Forward(context.Background(), src)
	require.Error(t, err)
	var ec *errcode.Error
	require.ErrorAs(t, err, &ec)
	assert.Equal(t, http.StatusServiceUnavailable, ec.HTTPStatus())
	assert.Equal(t, errcode.AdminUpstreamUnavailable, ec.Reason)
}

func TestForward_MintFailureIsReported(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("upstream must not be called when minting fails")
	}))
	defer upstream.Close()

	f := newClientUpdateForwarder(upstream.URL, 30*time.Second, func() (string, error) {
		return "", assert.AnError
	})
	src, _ := fwdTestForm(t, twoGoodParts())

	_, err := f.Forward(context.Background(), src)
	assert.Error(t, err)
}

// TestForward_RejectsUnknownField stops a client streaming a large body into a
// field nothing will read. A missing field cannot be caught here — it is only
// knowable at EOF — so the downstream's own 400 covers that case.
func TestForward_RejectsUnknownField(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	f := newClientUpdateForwarder(upstream.URL, 30*time.Second, staticToken("tok"))
	src, _ := fwdTestForm(t, []struct{ field, name, body, contentType string }{
		{"surpriseField", "evil.bin", strings.Repeat("x", 128), ""},
	})

	_, err := f.Forward(context.Background(), src)
	require.Error(t, err)
	var ec *errcode.Error
	require.ErrorAs(t, err, &ec)
	assert.Equal(t, http.StatusBadRequest, ec.HTTPStatus())
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `make test SERVICE=admin-service`
Expected: FAIL — `undefined: newClientUpdateForwarder`, `undefined: configFileField`, `undefined: errcode.AdminUpstreamUnauthorized`.

- [ ] **Step 3: Add the admin error reasons**

Append to `pkg/errcode/codes_admin.go`, inside the existing `const` block:

```go
	AdminUpstreamUnauthorized Reason = "upstream_unauthorized" // 503: client-update-service refused this service's token
	AdminUpstreamUnavailable  Reason = "upstream_unavailable"  // 503: client-update-service unreachable or failing
```

- [ ] **Step 4: Write the forwarder**

Create `admin-service/forwarder.go`:

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"strings"
	"time"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/restyutil"
)

// The two multipart fields client-update-service requires.
const (
	configFileField  = "configFile"
	executeFileField = "executeFile"
)

// upstreamBodyLimit bounds how much of a downstream error body we read, so a
// misbehaving upstream cannot push an unbounded string into our response.
const upstreamBodyLimit = 4 << 10

// quoteEscaper mirrors mime/multipart's own escaping. CR and LF become %0D/%0A
// so a crafted upload filename cannot inject extra headers into the part.
var quoteEscaper = strings.NewReplacer("\\", "\\\\", `"`, "\\\"", "\r", "%0D", "\n", "%0A")

// uploadedNames records which artifacts a forward carried, for the audit entry.
type uploadedNames struct {
	ConfigFile  string
	ExecuteFile string
}

// clientUpdateForwarder streams an artifact pair to client-update-service under
// a freshly minted service-account token.
type clientUpdateForwarder struct {
	http      *http.Client
	baseURL   string
	mintToken func() (string, error)
}

// newClientUpdateForwarder builds the forwarder. mintToken yields one bearer
// token per forward.
func newClientUpdateForwarder(baseURL string, timeout time.Duration, mintToken func() (string, error)) *clientUpdateForwarder {
	return &clientUpdateForwarder{
		// A raw *http.Client rather than the resty client itself: resty v2
		// materializes any io.Reader body it cannot natively replay
		// (createHTTPRequest -> getBodyCopy -> io.ReadAll), which is precisely
		// the OOM this streaming path exists to avoid. Built through restyutil
		// so the shared transport, OTel instrumentation and timeout still apply.
		// Same documented exception as pkg/drive/uploader.go.
		http:      restyutil.New(baseURL, restyutil.WithTimeout(timeout)).GetClient(),
		baseURL:   strings.TrimSuffix(baseURL, "/"),
		mintToken: mintToken,
	}
}

// Forward re-encodes src part-by-part into a request to client-update-service.
// Nothing is buffered whole and nothing touches disk, so peak memory is one copy
// buffer regardless of artifact size.
//
// Because the body is piped it carries no Content-Length and is sent chunked;
// the downstream reads it with c.FormFile, which handles that normally.
func (f *clientUpdateForwarder) Forward(ctx context.Context, src *multipart.Reader) (uploadedNames, error) {
	token, err := f.mintToken()
	if err != nil {
		return uploadedNames{}, fmt.Errorf("mint service token: %w", err)
	}

	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)
	// Buffered so the copier never blocks handing back what it saw.
	done := make(chan struct {
		names uploadedNames
		err   error
	}, 1)

	go func() {
		names, err := copyParts(src, mw)
		if err == nil {
			err = mw.Close()
		}
		done <- struct {
			names uploadedNames
			err   error
		}{names, err}
		// A non-nil error surfaces on the reader side, so the in-flight request
		// fails rather than sending a silently truncated body.
		_ = pw.CloseWithError(err)
	}()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.baseURL+"/api/v1/version", pr)
	if err != nil {
		return uploadedNames{}, fmt.Errorf("build client-update request: %w", err)
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+token)

	resp, doErr := f.http.Do(req)
	// net/http closes req.Body on the way out, which unblocks the copier if it
	// is mid-write, so this receive cannot deadlock.
	copied := <-done

	// A copier error is the truer cause: the request failed because we stopped
	// feeding it. Report that rather than the transport symptom.
	if copied.err != nil {
		if resp != nil {
			_ = resp.Body.Close()
		}
		return copied.names, copied.err
	}
	if doErr != nil {
		return copied.names, errcode.Unavailable("client-update-service is unreachable",
			errcode.WithReason(errcode.AdminUpstreamUnavailable), errcode.WithCause(doErr))
	}
	defer resp.Body.Close()
	return copied.names, classifyUpstream(resp)
}

// copyParts streams each part of src into dst, preserving field name, filename
// and declared content type, and recording the two artifact names as it goes.
//
// An unknown field is refused before its bytes are streamed, so a client cannot
// push a large body into a field nothing reads. A MISSING field cannot be
// detected here — that is only knowable at EOF, by which point the upload is
// spent — so client-update-service's own 400 covers it.
func copyParts(src *multipart.Reader, dst *multipart.Writer) (uploadedNames, error) {
	var names uploadedNames
	for {
		part, err := src.NextPart()
		if errors.Is(err, io.EOF) {
			return names, nil
		}
		if err != nil {
			return names, errcode.BadRequest("malformed multipart upload",
				errcode.WithCause(fmt.Errorf("read upload part: %w", err)))
		}

		field, filename := part.FormName(), part.FileName()
		switch field {
		case configFileField:
			names.ConfigFile = filename
		case executeFileField:
			names.ExecuteFile = filename
		default:
			_ = part.Close()
			return names, errcode.BadRequest(
				fmt.Sprintf("unexpected form field %q: only %s and %s are accepted",
					field, configFileField, executeFileField))
		}

		hdr := textproto.MIMEHeader{}
		hdr.Set("Content-Disposition", fmt.Sprintf(`form-data; name="%s"; filename="%s"`,
			quoteEscaper.Replace(field), quoteEscaper.Replace(filename)))
		// Preserve the declared type: the downstream only falls back to
		// application/x-yaml when a part declares none, so dropping this would
		// store the descriptor as octet-stream.
		if ct := part.Header.Get("Content-Type"); ct != "" {
			hdr.Set("Content-Type", ct)
		}
		w, err := dst.CreatePart(hdr)
		if err != nil {
			_ = part.Close()
			return names, fmt.Errorf("create forwarded part %q: %w", field, err)
		}
		if _, err := io.Copy(w, part); err != nil {
			_ = part.Close()
			return names, fmt.Errorf("stream part %q: %w", field, err)
		}
		if err := part.Close(); err != nil {
			return names, fmt.Errorf("close part %q: %w", field, err)
		}
	}
}

// classifyUpstream turns client-update-service's answer into nil or the error
// admin-service should return.
//
// A 401/403 means OUR credential was refused — a key, issuer, audience or
// allowlist misconfiguration — not the admin's session. Relaying it as a 401
// would tell an authenticated admin their own login failed and send them to
// debug the wrong thing entirely, so it becomes a 503 with a distinct reason.
func classifyUpstream(resp *http.Response) error {
	switch {
	case resp.StatusCode < http.StatusMultipleChoices:
		return nil
	case resp.StatusCode == http.StatusBadRequest:
		return errcode.BadRequest(upstreamMessage(resp))
	case resp.StatusCode == http.StatusUnauthorized, resp.StatusCode == http.StatusForbidden:
		return errcode.Unavailable("client-update-service rejected this service's credential",
			errcode.WithReason(errcode.AdminUpstreamUnauthorized))
	default:
		return errcode.Unavailable("client-update-service could not store the upload",
			errcode.WithReason(errcode.AdminUpstreamUnavailable))
	}
}

// upstreamMessage reads the downstream errcode envelope's user-safe message.
// The envelope marshals its message under "error" (see pkg/errcode.Error).
func upstreamMessage(resp *http.Response) string {
	const fallback = "client-update-service rejected the upload"
	body, err := io.ReadAll(io.LimitReader(resp.Body, upstreamBodyLimit))
	if err != nil || len(body) == 0 {
		return fallback
	}
	var env struct {
		Message string `json:"error"`
	}
	if err := json.Unmarshal(body, &env); err != nil || env.Message == "" {
		return fallback
	}
	return env.Message
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `make test SERVICE=admin-service`
Expected: PASS. Run with `-race` (the Makefile does) — the pipe goroutine is the thing being checked.

- [ ] **Step 6: Commit**

```bash
make fmt && make lint
git add admin-service/forwarder.go admin-service/forwarder_test.go pkg/errcode/codes_admin.go
git commit -m "feat(admin-service): streaming forwarder to client-update-service

Re-encodes the inbound multipart stream into the outbound request through an
io.Pipe, so peak memory is independent of artifact size and nothing touches
disk. A downstream 401/403 maps to 503 upstream_unauthorized rather than being
relayed, since it means this service's own credential was refused."
```

---

### Task 8: admin-service upload endpoint

**Files:**
- Create: `admin-service/clientupdate.go`
- Test: `admin-service/clientupdate_test.go`
- Modify: `admin-service/handler.go` (add the `clientUpdate` field + variadic options)
- Modify: `admin-service/middleware.go` (add `extendDeadlines`)
- Modify: `admin-service/routes.go` (register the route)
- Modify: `admin-service/main.go` (build the signer and forwarder, pass the option)
- Modify: `admin-service/integration_test.go:1290`, `admin-service/room_onduty_integration_test.go:102` and `:225` — all three call the old 4-argument `registerRoutes` and will not compile

**Interfaces:**
- Consumes: `uploadedNames`, `newClientUpdateForwarder`, `configFileField`, `executeFileField` (Task 7); `Config` (Task 6); `svcjwt.NewSigner` (Task 1).
- Produces:
  - `clientUpdateUploader` interface — `Forward(ctx context.Context, src *multipart.Reader) (uploadedNames, error)`
  - `handlerOption func(*Handler)`, `withClientUpdate(f clientUpdateUploader) handlerOption`
  - `newHandler(store AdminStore, sessions session.Store, cfg Config, rpc roomRequester, publishInbox func(context.Context, string, []byte) error, opts ...handlerOption) *Handler` — **variadic, so every existing call site is unchanged**
  - `(*Handler).uploadClientVersion(c *gin.Context)`
  - `extendDeadlines(d time.Duration) gin.HandlerFunc`
  - `auditActionClientUpdateUpload = "client_update.upload"`

- [ ] **Step 1: Write the failing test**

Create `admin-service/clientupdate_test.go`:

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/session"
)

// fakeUploader stands in for the real forwarder: it records the parts it was
// handed and returns a scripted result.
type fakeUploader struct {
	called bool
	names  uploadedNames
	err    error
	// drained is what the fake actually read from the stream, proving the
	// handler passes a live reader rather than a spent one.
	drained map[string]string
}

func (f *fakeUploader) Forward(_ context.Context, src *multipart.Reader) (uploadedNames, error) {
	f.called = true
	f.drained = map[string]string{}
	for {
		part, err := src.NextPart()
		if err != nil {
			break
		}
		var buf bytes.Buffer
		_, _ = buf.ReadFrom(part)
		f.drained[part.FormName()] = buf.String()
		_ = part.Close()
	}
	return f.names, f.err
}

// uploadRequest builds a POST carrying the two artifact parts.
func uploadRequest(t *testing.T) *http.Request {
	t.Helper()
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	for _, p := range []struct{ field, name, content string }{
		{configFileField, "app.yaml", "version: 4"},
		{executeFileField, "app.exe", "MZbinary"},
	} {
		w, err := mw.CreateFormFile(p.field, p.name)
		require.NoError(t, err)
		_, err = w.Write([]byte(p.content))
		require.NoError(t, err)
	}
	require.NoError(t, mw.Close())

	req := httptest.NewRequest(http.MethodPost, "/v1/admin/client-update/version", &body)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	return req
}

// uploadRouter mounts just the upload handler with an already-authenticated
// principal, so these tests exercise the handler rather than requireAdmin
// (which middleware_test.go already covers).
func uploadRouter(h *Handler) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/v1/admin/client-update/version", func(c *gin.Context) {
		c.Set(ctxPrincipal, session.Session{
			UserID: "u-1", Account: "admin1", SiteID: "site-local",
			Roles: []string{"admin"},
		})
		c.Next()
	}, h.uploadClientVersion)
	return r
}

func TestUploadClientVersion_ForwardsAndAudits(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockAdminStore(ctrl)

	var audited *AuditEntry
	store.EXPECT().AppendAudit(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, e *AuditEntry) error { audited = e; return nil })

	up := &fakeUploader{names: uploadedNames{ConfigFile: "app.yaml", ExecuteFile: "app.exe"}}
	h := newHandler(store, nil, Config{SiteID: "site-local"}, nil, nil, withClientUpdate(up))

	w := httptest.NewRecorder()
	uploadRouter(h).ServeHTTP(w, uploadRequest(t))

	require.Equal(t, http.StatusOK, w.Code, "body=%s", w.Body.String())
	var got map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Equal(t, "success", got["result"])

	assert.True(t, up.called)
	assert.Equal(t, "version: 4", up.drained[configFileField],
		"the handler must hand the forwarder a live stream, not a consumed one")
	assert.Equal(t, "MZbinary", up.drained[executeFileField])

	require.NotNil(t, audited, "a successful upload must be audited")
	assert.Equal(t, auditActionClientUpdateUpload, audited.Action)
	assert.Equal(t, "admin1", audited.ActorAccount)
	assert.Equal(t, "app.yaml", audited.Details["configFile"])
	assert.Equal(t, "app.exe", audited.Details["executeFile"])
}

func TestUploadClientVersion_ForwardErrorIsNotAudited(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockAdminStore(ctrl)
	// No AppendAudit expectation: a failed upload must not be recorded as one.

	up := &fakeUploader{err: errcode.Unavailable("client-update-service is unreachable",
		errcode.WithReason(errcode.AdminUpstreamUnavailable))}
	h := newHandler(store, nil, Config{SiteID: "site-local"}, nil, nil, withClientUpdate(up))

	w := httptest.NewRecorder()
	uploadRouter(h).ServeHTTP(w, uploadRequest(t))

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	var env map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &env))
	assert.Equal(t, string(errcode.AdminUpstreamUnavailable), env["reason"])
}

func TestUploadClientVersion_RejectsNonMultipart(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockAdminStore(ctrl)

	up := &fakeUploader{}
	h := newHandler(store, nil, Config{SiteID: "site-local"}, nil, nil, withClientUpdate(up))

	req := httptest.NewRequest(http.MethodPost, "/v1/admin/client-update/version",
		bytes.NewBufferString(`{"not":"multipart"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	uploadRouter(h).ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.False(t, up.called, "a non-multipart body must be refused before forwarding")
}

// TestUploadClientVersion_UnconfiguredIsUnavailable covers the nil-forwarder
// path, which every existing newHandler call site in this package produces.
func TestUploadClientVersion_UnconfiguredIsUnavailable(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockAdminStore(ctrl)

	h := newHandler(store, nil, Config{SiteID: "site-local"}, nil, nil)

	w := httptest.NewRecorder()
	uploadRouter(h).ServeHTTP(w, uploadRequest(t))

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
}

// TestExtendDeadlines_ToleratesUnsupportedWriter proves the middleware is a
// no-op rather than a failure on a ResponseWriter that cannot take deadlines
// (httptest's recorder), so unit tests of every other route stay unaffected.
func TestExtendDeadlines_ToleratesUnsupportedWriter(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	var reached bool
	r.GET("/probe", extendDeadlines(time.Minute), func(c *gin.Context) {
		reached = true
		c.Status(http.StatusOK)
	})

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/probe", nil))

	assert.Equal(t, http.StatusOK, w.Code)
	assert.True(t, reached)
}
```

Add `time` and `go.uber.org/mock/gomock` to the imports. The mock construction
above matches the pattern already used throughout `admin-service/handler_test.go`
(`gomock.NewController(t)` then `NewMockAdminStore(ctrl)`); there is no shared
helper to reuse. `gomock.NewController` registers its own cleanup with `t`, so
no explicit `Finish` is needed.

- [ ] **Step 2: Run the test to verify it fails**

Run: `make test SERVICE=admin-service`
Expected: FAIL — `undefined: withClientUpdate`, `undefined: uploadClientVersion`, `undefined: extendDeadlines`, `undefined: auditActionClientUpdateUpload`.

- [ ] **Step 3: Add the handler option and field**

In `admin-service/handler.go`, add the `clientUpdate` field to the `Handler` struct:

```go
	// clientUpdate forwards artifact uploads to client-update-service. Nil when
	// the service is not configured for it; the upload route then answers 503
	// rather than dereferencing it.
	clientUpdate clientUpdateUploader
```

Change `newHandler`'s signature to accept variadic options — this keeps every existing call site in `handler_test.go`, `permissions_test.go`, `login_test.go` and `room_onduty_test.go` compiling unchanged:

```go
// handlerOption customizes a Handler after construction. Variadic so adding a
// dependency never churns the existing call sites.
type handlerOption func(*Handler)

// withClientUpdate installs the client-update forwarder.
func withClientUpdate(f clientUpdateUploader) handlerOption {
	return func(h *Handler) { h.clientUpdate = f }
}

func newHandler(store AdminStore, sessions session.Store, cfg Config, rpc roomRequester, publishInbox func(ctx context.Context, subj string, data []byte) error, opts ...handlerOption) *Handler { //nolint:gocritic // hugeParam: Config is a startup value copied once at construction
	h := &Handler{
		store: store, sessions: sessions, cfg: cfg, roomRPC: rpc, publishInbox: publishInbox,
		remoteDests: remoteSites(cfg.AllSiteIDs, cfg.SiteID),
	}
	for _, opt := range opts {
		opt(h)
	}
	return h
}
```

- [ ] **Step 4: Write the handler**

Create `admin-service/clientupdate.go`:

```go
package main

import (
	"context"
	"mime/multipart"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
)

// auditActionClientUpdateUpload names the ledger entry for a published update.
// Because admin-service is the only route to client-update-service, this entry
// is a complete record: no artifact reaches storage without one.
const auditActionClientUpdateUpload = "client_update.upload"

// clientUpdateUploader is the forwarding surface this handler needs, declared
// here — at the consumer — so tests can substitute a fake.
type clientUpdateUploader interface {
	Forward(ctx context.Context, src *multipart.Reader) (uploadedNames, error)
}

// uploadClientVersion streams an update-artifact pair through to
// client-update-service under this service's own service-account token.
//
// The body is read with MultipartReader rather than c.FormFile: Gin's form
// parsing buffers to memory and spills to local disk, which a large executable
// must not do in a pod. Validation of the pair itself happens downstream —
// a missing part is only knowable at EOF, by which point the upload is spent.
func (h *Handler) uploadClientVersion(c *gin.Context) {
	ctx := c.Request.Context()

	if h.clientUpdate == nil {
		errhttp.Write(ctx, c, errcode.Unavailable("client update publishing is not configured on this site",
			errcode.WithReason(errcode.AdminUpstreamUnavailable)))
		return
	}

	src, err := c.Request.MultipartReader()
	if err != nil {
		errhttp.Write(ctx, c, errcode.BadRequest("request body must be multipart/form-data",
			errcode.WithCause(err)))
		return
	}

	names, err := h.clientUpdate.Forward(ctx, src)
	if err != nil {
		errhttp.Write(ctx, c, err)
		return
	}

	h.audit(ctx, c, auditActionClientUpdateUpload, "", "", map[string]string{
		"configFile":  names.ConfigFile,
		"executeFile": names.ExecuteFile,
	})
	c.JSON(http.StatusOK, gin.H{"result": "success"})
}
```

- [ ] **Step 5: Add the deadline middleware**

Append to `admin-service/middleware.go`:

```go
// extendDeadlines pushes this request's read and write deadlines out to d.
//
// admin-service's server-wide ReadTimeout (15s) and WriteTimeout
// (httpWriteTimeout, 40s) are sized for the cross-site permission fanout — see
// config.go and applyBaseMiddleware — and must not be widened for every route
// just because one route needs minutes. An artifact upload extends its own
// instead, leaving every other route's behavior untouched.
//
// A ResponseWriter that cannot take deadlines (httptest's recorder in unit
// tests) reports http.ErrNotSupported; the handler still runs, it simply keeps
// the server-wide timeouts.
func extendDeadlines(d time.Duration) gin.HandlerFunc {
	return func(c *gin.Context) {
		rc := http.NewResponseController(c.Writer)
		until := time.Now().Add(d)
		if err := rc.SetReadDeadline(until); err != nil && !errors.Is(err, http.ErrNotSupported) {
			slog.WarnContext(c.Request.Context(), "extend read deadline failed", "error", err)
		}
		if err := rc.SetWriteDeadline(until); err != nil && !errors.Is(err, http.ErrNotSupported) {
			slog.WarnContext(c.Request.Context(), "extend write deadline failed", "error", err)
		}
		c.Next()
	}
}
```

Add `errors`, `log/slog`, `net/http` and `time` to that file's imports.

- [ ] **Step 6: Register the route**

In `admin-service/routes.go`, change the signature to take the config value and add the route at the end of the `admin` group:

```go
func registerRoutes(r *gin.Engine, h *Handler, sessions session.Store, siteID string, uploadTimeout time.Duration) {
```

```go
	admin.POST("/client-update/version", extendDeadlines(uploadTimeout), h.uploadClientVersion)
```

Add `time` to the imports.

- [ ] **Step 7: Wire main.go**

In `admin-service/main.go`, after the `nc`/`js`/`publishInbox` setup and before `h := newHandler(...)`:

```go
	// Client-update publishing is opt-in per site: an empty base URL leaves the
	// forwarder nil and the upload route answers 503. loadConfig has already
	// rejected a half-configured opt-in, so reaching here with a base URL means
	// the key and account are present too.
	var handlerOpts []handlerOption
	if cfg.ClientUpdateBaseURL == "" {
		slog.Warn("client update publishing is disabled: CLIENT_UPDATE_BASE_URL is unset",
			"site", cfg.SiteID)
	} else {
		signer, err := svcjwt.NewSigner(cfg.SvcJWTPrivateKey, cfg.SvcJWTIssuer)
		if err != nil {
			return fmt.Errorf("build service-token signer: %w", err)
		}
		// Minted per forward and never returned to a caller, so no bearer
		// credential for client-update-service ever leaves this process.
		mintClientUpdateToken := func() (string, error) {
			token, _, err := signer.Sign(cfg.ClientUpdateServiceAccount, cfg.ClientUpdateAudience, cfg.SvcJWTTTL)
			if err != nil {
				return "", fmt.Errorf("sign client-update token: %w", err)
			}
			return token, nil
		}
		handlerOpts = append(handlerOpts,
			withClientUpdate(newClientUpdateForwarder(cfg.ClientUpdateBaseURL, cfg.ClientUpdateUploadTimeout, mintClientUpdateToken)))
	}
```

Change the handler construction and route registration:

```go
	h := newHandler(st, sessStore, cfg, nc, publishInbox, handlerOpts...)
```

```go
	registerRoutes(r, h, sessStore, cfg.SiteID, cfg.ClientUpdateUploadTimeout)
```

Add `"github.com/hmchangw/chat/pkg/svcjwt"` to the imports.

- [ ] **Step 8: Fix the three existing `registerRoutes` call sites**

Adding the parameter breaks three integration-test callers. None exercises the
upload, so any positive timeout works:

- `admin-service/integration_test.go` (~line 1290)
- `admin-service/room_onduty_integration_test.go` (~line 102)
- `admin-service/room_onduty_integration_test.go` (~line 225)

Change each to:

```go
	registerRoutes(r, h, sessions, cfg.SiteID, 10*time.Minute)
```

Ensure each file imports `time` (both already do — confirm rather than assume).

- [ ] **Step 9: Run the tests to verify they pass**

Run: `make test SERVICE=admin-service`
Expected: PASS, including the pre-existing `handler_test.go`, `permissions_test.go`, `login_test.go`, `room_onduty_test.go` and `router_test.go`.

Then confirm the integration files compile, which the unit run does not cover:

Run: `go vet -tags integration ./admin-service`
Expected: no output.

- [ ] **Step 10: Verify build, check coverage, and commit**

```bash
make build SERVICE=admin-service
go test -race -coverprofile=/tmp/admin.out ./admin-service && go tool cover -func=/tmp/admin.out | tail -1
```
Expected: build succeeds; total coverage ≥ 80%.

```bash
make fmt && make lint
git add admin-service/
git commit -m "feat(admin-service): admin endpoint to publish a client update

POST /v1/admin/client-update/version streams the artifact pair to
client-update-service under a per-request service-account token the caller
never sees, and records the publication in the audit ledger.

The route installs its own read/write deadlines via extendDeadlines rather than
widening the server-wide timeouts, which the cross-site permission fanout is
sized against."
```

---

### Task 9: Documentation and local compose

**Files:**
- Modify: `docs/client-api.md` (§9 Admin Service, §12 Client Update Service)
- Modify: `client-update-service/deploy/docker-compose.yml`
- Modify: `admin-service/deploy/docker-compose.yml`

**Interfaces:**
- Consumes: the finished behavior of Tasks 3–8.
- Produces: nothing consumed by code.

CLAUDE.md requires that any change to a client-facing HTTP handler updates `docs/client-api.md` **in the same PR**. Neither `docs/client-api/request-reply.md` nor `docs/client-api/events.md` changes — both are derived views of the NATS `chat.user.` surface, and this change adds no NATS subject and touches no `pkg/model` wire struct.

- [ ] **Step 1: Generate a dev keypair**

Run: `go run ./tools/svcjwtkey`

Copy the two printed values — they go into both compose files below. This pair is **dev-only** and committed deliberately so `docker compose up` works from a clean checkout.

- [ ] **Step 2: Update client-update-service compose**

In `client-update-service/deploy/docker-compose.yml`, add to `environment:` (substituting the generated public key):

```yaml
      - SVCJWT_PUBLIC_KEY=${SVCJWT_PUBLIC_KEY:-<generated public key>}
      - SVCJWT_ISSUER=${SVCJWT_ISSUER:-admin-service}
      - SVCJWT_AUDIENCE=${SVCJWT_AUDIENCE:-client-update-service}
      - ALLOWED_SERVICE_ACCOUNTS=${ALLOWED_SERVICE_ACCOUNTS:-svc-updater}
      - HTTP_READ_TIMEOUT=${HTTP_READ_TIMEOUT:-10m}
      - HTTP_WRITE_TIMEOUT=${HTTP_WRITE_TIMEOUT:-10m}
```

Add a comment directly above them:

```yaml
      # Dev-only keypair, committed so `docker compose up` works from a clean
      # checkout. Generate a real one with `go run ./tools/svcjwtkey`; production
      # keys come from ops/IaC and must never be committed.
```

- [ ] **Step 3: Update admin-service compose**

In `admin-service/deploy/docker-compose.yml`, add to `environment:` (substituting the generated **private** key — the matching half of the public key above):

```yaml
      # Dev-only keypair, committed so `docker compose up` works from a clean
      # checkout. The public half is in client-update-service's compose file.
      # Generate a real one with `go run ./tools/svcjwtkey`; production keys come
      # from ops/IaC and must never be committed.
      - SVCJWT_PRIVATE_KEY=${SVCJWT_PRIVATE_KEY:-<generated private key>}
      - SVCJWT_ISSUER=${SVCJWT_ISSUER:-admin-service}
      - SVCJWT_TTL=${SVCJWT_TTL:-5m}
      - CLIENT_UPDATE_BASE_URL=${CLIENT_UPDATE_BASE_URL:-http://client-update-service:8080}
      - CLIENT_UPDATE_AUDIENCE=${CLIENT_UPDATE_AUDIENCE:-client-update-service}
      - CLIENT_UPDATE_SERVICE_ACCOUNT=${CLIENT_UPDATE_SERVICE_ACCOUNT:-svc-updater}
      - CLIENT_UPDATE_UPLOAD_TIMEOUT=${CLIENT_UPDATE_UPLOAD_TIMEOUT:-10m}
```

- [ ] **Step 4: Update `docs/client-api.md` §12**

Replace the blanket warning block under `## 12. Client Update Service` with one scoped to the download:

```markdown
> [!WARNING]
> **`GET /api/v1/version/:fileName` is UNAUTHENTICATED.** Anyone who can reach
> the service can download update artifacts. It **MUST** be network-restricted
> before any production exposure. The upload endpoint is gated — see below.
```

Change the upload's `**Auth:** none (v1)` line to:

```markdown
**Auth:** `Authorization: Bearer <serviceToken>` — an Ed25519 JWT minted by
`admin-service` whose `sub` is an allowlisted service account. Admins publish
updates through `POST /v1/admin/client-update/version` (§9), which mints and
attaches this token itself; there is no endpoint that issues one to a caller.
```

Add these two rows to the upload's response table:

```markdown
| `401 Unauthorized` | Missing, malformed, unsigned, or expired service token. Reason `invalid_token`. |
| `403 Forbidden` | Valid token whose service account is not allowlisted. Reason `not_authorized`. |
```

Leave the `GET /api/v1/version/:fileName` section's `**Auth:** none` unchanged.

- [ ] **Step 5: Update `docs/client-api.md` §9**

Add a subsection to §9 (Admin Service), matching the house style of the sections already there — field tables with explicit types, and a JSON example for the success response:

````markdown
### POST /v1/admin/client-update/version

**Auth:** `Authorization: Bearer <authToken>` (admin role required)

Publishes a client update: uploads a `.yaml` descriptor and its executable as
`multipart/form-data`. admin-service streams both parts through to
`client-update-service` under its own service-account credential, which the
caller never sees or supplies. Both parts are required. An upload reusing an
existing file name overwrites it.

#### Request

| Part | Type | Required | Notes |
|---|---|---|---|
| `configFile` | file (`.yaml`/`.yml`) | yes | Update descriptor. Its declared `Content-Type` is preserved on the way through. |
| `executeFile` | file (binary) | yes | The executable. No size cap; the body is streamed, never buffered. |

#### Response

| Status | Condition |
|---|---|
| `200 OK` | Both files stored. |
| `400 Bad Request` | Body is not `multipart/form-data`; an unexpected form field; or `client-update-service` rejected the pair (its message is relayed). |
| `401 Unauthorized` | Missing or invalid admin session token. |
| `403 Forbidden` | Valid session without the admin role. |
| `503 Service Unavailable` | `client-update-service` is unreachable or failing (reason `upstream_unavailable`), or it refused this service's credential (reason `upstream_unauthorized`, a server-side misconfiguration). |

##### Success response (`200`)

| Field | Type | Notes |
|---|---|---|
| `result` | string | Always `"success"`. |

```json
{ "result": "success" }
```
````

Also add the row to §9's endpoint index/table of contents if one exists.

- [ ] **Step 6: Verify the whole repo is green**

```bash
make fmt
make lint
make test
make sast
```
Expected: all pass. `make sast` is a blocking CI gate and fails on medium+; if `gosec` flags anything in the new code, fix it rather than suppressing — suppress only a genuine false positive, with `// #nosec <RULE> -- reason` on the line directly above the statement.

- [ ] **Step 7: Run the integration suites for both services**

```bash
make test-integration SERVICE=client-update-service
make test-integration SERVICE=admin-service
```
Expected: PASS (requires Docker).

- [ ] **Step 8: Commit**

```bash
git add docs/client-api.md client-update-service/deploy/docker-compose.yml admin-service/deploy/docker-compose.yml
git commit -m "docs: document service-account auth and the admin upload endpoint

Narrows client-update-service's UNAUTHENTICATED warning to the download only,
documents the upload's bearer requirement and its 401/403 cases, and adds
POST /v1/admin/client-update/version to the admin section. Local compose files
gain a dev-only keypair so the pair works from a clean checkout."
```

- [ ] **Step 9: Push the branch**

```bash
git push -u origin claude/service-auth-admin-upload-njomeg
```

Retry up to 4 times with exponential backoff (2s, 4s, 8s, 16s) on network failure. Do **not** open a pull request unless the user asks.

---

## Notes for the executor

- **Do not add anything to `go.mod`.** If you find yourself reaching for a JWT library, stop — Task 1 is deliberately stdlib-only, and a new dependency is a global-constraint violation, not a judgement call.
- **`docs/reviews/` must be empty before any PR** (CLAUDE.md §5). Nothing in this plan writes there, but check if you ran the `branch_review` skill.
- The `admin-frontend` upload page is **out of scope** (spec §10.1). If it turns out to be wanted, that is a new plan, not an extra task here.
- If a task reveals that the spec is wrong rather than incomplete, stop and say so rather than quietly diverging — the spec travels with this plan and reviewers read both.
