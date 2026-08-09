# translation-service: J1 token from a Kubernetes ServiceAccount token file

**Date:** 2026-08-04
**Status:** Approved (design)
**Service:** `translation-service`
**Ships with:** PR #180 (`claude/fix-translation-token-body`) — extends the J1→body fix in the same PR.

## Problem

`translation-service`'s `stream` backend authenticates to the third-party
translate API by first exchanging a **J1** token for a **J2** token
(`POST {"key": <J1>}` → accessToken API → J2). Today J1 is supplied as a
plaintext environment variable, `TRANSLATION_J1_TOKEN`, read once at startup and
held as a fixed string for the process lifetime.

We want production to obtain J1 from the pod's **Kubernetes ServiceAccount
token**, mounted at `/var/run/secrets/kubernetes.io/serviceaccount/token`,
instead of a plaintext env var. Two consequences drive the design:

1. A plaintext secret in the environment is visible in `kubectl describe`, crash
   dumps, and child processes. Sourcing it from a file keeps it out of the
   environment.
2. The projected ServiceAccount token is **short-lived and rotated in place by
   kubelet** (default ~1h TTL). A value read once at startup goes stale; the J1
   must be re-read from the file on each use.

## Goal

- Production reads J1 from the ServiceAccount token file, picking up kubelet
  rotation automatically (no restart).
- Local/dev and tests keep a way to supply J1 without a Kubernetes mount.
- No change to the J1→J2→translate flow, the NATS RPC contract, or any
  client-facing schema.

## Non-goals

- No Vault involvement (this is a direct file read, not `pkg/atrest`'s Vault K8s
  auth). J1 *is* the file contents; we do not exchange the SA token for anything
  before sending it as `key`.
- No change to how J2 is sent to the translate API — it still travels as the
  `Authorization` header on the translate call (unchanged from PR #180).
- No Kubernetes manifest changes in this repo (deploy manifests live in
  ops/IaC).

## Design (Approach A: injected `j1Source` func)

### Config surface (`main.go`)

Keep the existing literal, add a file path:

```go
J1Token     string `env:"TRANSLATION_J1_TOKEN"      envDefault:""`
J1TokenFile string `env:"TRANSLATION_J1_TOKEN_FILE" envDefault:"/var/run/secrets/kubernetes.io/serviceaccount/token"`
```

**Precedence:** `TRANSLATION_J1_TOKEN` (literal) wins when set; otherwise read
`TRANSLATION_J1_TOKEN_FILE`.

**`TRANSLATION_J1_TOKEN` is a local/dev + test escape hatch only.** Production
leaves it unset and relies on the file. This mirrors `pkg/atrest`'s `VAULT_TOKEN`
("Suitable for local docker-compose only; production should use Kubernetes …
auth").

### New file `j1source.go`

```go
// j1Source yields the current J1 token. Read fresh on each J1→J2 exchange so a
// rotated Kubernetes ServiceAccount token is picked up without a restart.
type j1Source func() (string, error)

func staticJ1(tok string) j1Source // always yields tok (literal / tests)

func fileJ1(path string) j1Source  // ReadFile(path) + TrimSpace on every call;
                                   // empty file or read error → error (no token in msg)

// newJ1Source: literal wins, else file, else error (neither configured).
func newJ1Source(literal, file string) (j1Source, error)
```

- **Re-read per call** is the core behavior — `fileJ1` does `os.ReadFile` every
  time, so rotation is transparent.
- `os.ReadFile` on a variable path trips gosec **G304**. The path is
  operator-configured (env), not user input → justified suppression with
  `// #nosec G304 -- path is an operator-configured token mount, not user input`
  directly above the read. If golangci-lint's gosec also flags it, add the
  companion `//nolint:gosec` (the two mechanisms are independent per CLAUDE.md).

### Wiring changes

- **`token.go`**: replace field `j1Token string` with `readJ1 j1Source`.
  `newTokenProvider(accessTokenURL string, j1 j1Source, timeout, skew time.Duration)`.
  In `fetchLocked`, before building the body:
  ```go
  key, err := p.readJ1()
  if err != nil {
      return "", fmt.Errorf("read j1 token: %w", err)
  }
  // ... SetBody(accessTokenRequest{Key: key})
  ```
  The J2 caching, expiry, skew, and refresh logic are unchanged — J1 is only
  read at the moment of an exchange, so re-reads happen exactly when J2 is
  (re)fetched.
- **`translator_stream.go`**:
  `newStreamTranslator(endpoint, accessTokenURL string, j1 j1Source, timeout, skew time.Duration)`,
  passing `j1` through to `newTokenProvider`.
- **`main.go` `newTranslator`** (stream branch): keep the Endpoint /
  AccessTokenURL / HTTPTimeout fail-fast checks; replace the
  `if cfg.J1Token == ""` check with:
  ```go
  src, err := newJ1Source(cfg.J1Token, cfg.J1TokenFile)
  if err != nil {
      return nil, fmt.Errorf("%w when TRANSLATION_BACKEND=stream", err)
  }
  if _, err := src(); err != nil { // startup probe — fail fast on a missing/empty mount
      return nil, fmt.Errorf("validate j1 token source: %w", err)
  }
  return newStreamTranslator(cfg.Endpoint, cfg.AccessTokenURL, src, cfg.HTTPTimeout, cfg.TokenSkew), nil
  ```

### Fail-fast & error handling

- **Startup probe:** `newTranslator` calls the source once. Static source →
  non-empty literal; file source → actually reads the file. A missing mount or
  empty file dies at startup, preserving today's "misconfig fails at boot, not
  per-request" property.
- **Runtime:** if a re-read fails mid-flight, that one translate returns
  `translate backend: get access token: read j1 token: …`, which collapses to
  `internal` at the NATS boundary. The process does not crash.
- **Secret hygiene:** error messages carry the file **path** only, never the
  token contents. J1/J2 are never logged (CLAUDE.md).

## Testing (TDD)

- **New `j1source_test.go`**: `staticJ1` returns the literal; `fileJ1` reads +
  trims, empty file → error, missing file → error; `newJ1Source` covers all
  three precedence branches (literal / file / neither→error).
- **`token_test.go`**: the 5 existing `newTokenProvider(url, "J1", …)` calls
  pass `staticJ1("J1")`. `TestTokenProvider_SendsJ1InBody` uses a static source.
  **Add** `TestTokenProvider_RereadsJ1EachFetch` (source returns changing values
  across two exchanges → outgoing `key` differs, proving the re-read) and
  `TestTokenProvider_J1SourceError` (source error surfaces as `read j1 token`).
- **`translator_stream_test.go`**: the 5 existing `newStreamTranslator(…, "J1…", …)`
  calls wrap the literal in `staticJ1(…)`. Existing J2-header assertions are
  unaffected.
- **`main_test.go`**: `TestNewTranslator_StreamRequiresJ1Token` (struct literal,
  both empty) still asserts the error mentions `TRANSLATION_J1_TOKEN`. **Add**
  a file-source success case (temp file with a token, `J1TokenFile` set, no
  literal) and a file-missing fail-fast case.
- All under `-race` via `make test SERVICE=translation-service`; `make lint` and
  `make sast` (G304 suppression must survive `gosec`) clean before push.

## Docs impact

- **No `docs/client-api.md` change** — this is backend-to-third-party auth, not a
  client-facing handler; the NATS request/response schema is unchanged.
- Update the older spec `docs/superpowers/specs/2026-07-23-translation-api-design.md`
  where it describes J1's source (the env-var lines) to say "env literal **or**
  the ServiceAccount token file".
- `translation-service/deploy/docker-compose.yml` is unchanged — local keeps
  using `TRANSLATION_J1_TOKEN`.

## Ops prerequisites (outside this repo)

- The pod must actually mount the token: `automountServiceAccountToken` must not
  be `false`.
- The pod's ServiceAccount token must be a value the accessToken API accepts as
  J1 (the backend contract that makes `{"key": <SA-token>}` valid is arranged
  ops-side).
- These belong in the ops/IaC Kubernetes manifests, not in
  `translation-service/deploy/`.

## Out of scope

- Reversing precedence (file-wins) or removing the env literal entirely
  (options (b)/(c) considered and declined; (a) chosen for parity with atrest
  and minimal local-dev friction).
- Any Vault / `pkg/atrest` integration.
