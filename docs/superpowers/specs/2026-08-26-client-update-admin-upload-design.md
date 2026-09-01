# Admin-driven client update uploads — service-account auth + streaming relay

**Date:** 2026-08-26
**Services:** `client-update-service`, `admin-service`, `admin-frontend`
**Branch:** `claude/client-update-service-auth-i3hvpf`

## Goal

Close the authentication gap the original `client-update-service` design left open
("The legacy service is unauthenticated; … Auth deferred to a follow-up" —
`2026-08-03-client-update-service-design.md`), and give admins a supported way to
publish client update artifacts.

Two halves, one trust chain:

1. `POST /api/v1/version` on `client-update-service` requires an authorized
   **service account** bearer token. Exactly one account is provisioned:
   `admin-service`.
2. `admin-service` gains `POST /v1/admin/client-updates`, an **admin-authenticated,
   audited, streaming relay** to that endpoint, driven by a new Updates console in
   `admin-frontend`.

```
admin browser ──Bearer admin session──▶ admin-service ──Bearer service-account token──▶ client-update-service ──▶ MinIO
```

Two independent credentials, neither substituting for the other. The browser never
holds the service-account token and never reaches `client-update-service`.

## Non-goals

- **No virus scanning or signature verification** of the uploaded executable.
- **No versioning or rollback.** Upload still overwrites by filename, as today.
- **No auth on `GET /api/v1/version/:fileName`.** The client fleet pulling updates
  holds no credential; gating downloads is a separate problem with a separate
  answer.
- **Not fixing** `client-update-service`'s use of `c.FormFile`, which spills large
  parts to a temp file. Its "streams end-to-end" claim is really "bounded memory,
  via disk". Pre-existing; out of scope here.

## Resolved decisions

1. **Credential = static shared secret**, not a botplatform session or OIDC token.
   The caller is a backend service, not a human or a registered bot.
2. **Named accounts, not one anonymous token.** `UPLOAD_TOKENS` is an
   account→token map. Today it holds one entry, but the shape gives every upload a
   caller name in the audit log and makes zero-downtime rotation possible (add the
   new entry, cut over, drop the old) instead of a redeploy-with-downtime swap.
3. **File-extension validation lives only in `client-update-service`.**
   `admin-service` relays and validates nothing about artifact content. One
   authority on what a valid artifact is; the two services can never disagree.
   Accepted cost: a mistyped `.yaml` is rejected only after the whole body has
   streamed upstream.
4. **Per-route deadline extension**, not raised global timeouts. See §3.2.
5. **Parse-and-re-encode the multipart stream**, not a verbatim body relay, so
   `admin-service` learns the filenames it must write into the audit entry.

---

## 1. `client-update-service` — service-account auth

### 1.1 Config (`config.go`)

```go
UploadTokens map[string]string `env:"UPLOAD_TOKENS" envSeparator:"," envKeyValSeparator:":"`
// UPLOAD_TOKENS=admin-service:<token>
```

`caarlos0/env` v11 parses this shape already (`pkg/obs` uses it for
`OTEL_EXPORTER_OTLP_HEADERS`). No `envDefault` — CLAUDE.md §6: never default a
secret.

**Not** `required`: an unset or empty table is a valid configuration meaning
"uploads disabled". It authorizes nobody — every upload answers `401`, because
an empty map matches no token — so a site that does not publish client updates
deploys without provisioning one. Downloads are unaffected. `run()` logs a
warning at startup when the table is empty, so the state is visible rather than
mysterious.

A `validateUploadTokens` runs from `run()` immediately after `env.ParseAs`, and
rejects:

- an empty account name,
- a token shorter than 16 characters (which also covers the empty token in
  `UPLOAD_TOKENS=admin-service:`, that would otherwise authorize the empty string),
- the same token shared by two accounts, naming the accounts but never the token.

A token may not contain `,` — that is the entry separator, and a value
containing one is mangled into entries that trip the checks above. A `:` is fine:
`env` v11 splits each entry on the FIRST colon only (`env.go:781`, `SplitN(…, 2)`),
so everything after it is the token. Documented in the compose file and in
`docs/client-api.md` §12.

### 1.2 Middleware (`middleware.go`)

Joins the two middlewares already in that file.

```go
const ctxServiceAccount = "service_account"

func bearer(c *gin.Context) string           // strict "Bearer " prefix + TrimSpace
func requireServiceAccount(tokens map[string]string) gin.HandlerFunc
func lookupAccount(tokens map[string]string, tok string) (string, bool)
```

`bearer` mirrors `admin-service/middleware.go`'s existing helper exactly, including
its case-sensitive `"Bearer "` prefix, so the two services agree on what a bearer
header looks like.

`lookupAccount` scans **every** entry with `subtle.ConstantTimeCompare` and no
early break, so response timing cannot reveal which account a guessed token
belongs to. (`ConstantTimeCompare` returns 0 immediately on a length mismatch;
that length leak is inherent and accepted.)

Missing header, wrong scheme, empty or unknown token all produce one uniform
response — `errcode.Unauthenticated("invalid or missing service account token")`,
which `errhttp.Write` renders as `401` (`pkg/errcode/category.go:34`). No `reason`
tag: the caller is a machine, nothing branches on it, and no new `codes_*.go` is
warranted. The token is never logged, in any branch.

On success: `c.Set(ctxServiceAccount, account)`.

### 1.3 Routes (`routes.go`)

```go
api := r.Group("/api/v1")
api.POST("/version", requireServiceAccount(cfg.UploadTokens), h.HandleUpload)
api.GET("/version/:fileName", h.HandleDownload)   // unchanged — deliberately open
```

`registerRoutes` takes the token map as a new parameter.

### 1.4 Access log

`accessLogMiddleware` gains `"service_account", c.GetString(ctxServiceAccount)` —
empty for unauthenticated and for `GET`. This is the audit trail on the
`client-update-service` side; `admin-service` holds the richer one.

### 1.5 Unchanged

`version.go` in full: `validFileName`, `hasYAMLExt`, the empty-file and
duplicate-name checks, and the streaming `Put`. Extension policy stays here.

---

## 2. `admin-service` — the relay endpoint

New files `client_update.go` and `client_update_test.go`, matching how `login.go`,
`permissions.go` and `room_onduty.go` each own one handler area.

### 2.1 Route

```go
admin.POST("/client-updates", h.uploadClientVersion)
```

Inside the existing `/v1/admin` group, so `requireAdmin(sessions, siteID)` already
applies: valid session, `admin` role, matching site. No new auth code.

### 2.2 Escaping the server timeouts

`admin-service` runs `ReadTimeout: 15s` and `WriteTimeout: httpWriteTimeout` (40s).
Both are too short for a large artifact, and **the constant must not simply be
raised**: `requestBudget = httpWriteTimeout - 2s`, and `checkHandlerTimeout`
rejects any `ROOM_RPC_TIMEOUT` or `FANOUT_TIMEOUT` at or above it. Raising
`httpWriteTimeout` to 10m would let a misconfigured `FANOUT_TIMEOUT=5m` pass a
guard whose whole purpose is to catch that.

Instead the handler extends **its own** deadlines:

```go
rc := http.NewResponseController(c.Writer)
deadline := time.Now().Add(h.cfg.ClientUpdateTimeout)
if err := rc.SetReadDeadline(deadline); err != nil {
    return fmt.Errorf("extend upload read deadline: %w", err)   // collapses to 500
}
if err := rc.SetWriteDeadline(deadline); err != nil {
    return fmt.Errorf("extend upload write deadline: %w", err)
}
```

A raw wrapped error, not an `errcode` — this is an infra failure, and CLAUDE.md §3
says those collapse to `internal` at the boundary rather than being dressed up.

Verified viable: gin v1.12.0's `responseWriter` implements
`Unwrap() http.ResponseWriter` (`response_writer.go:57`), which is what
`http.NewResponseController` needs to reach the underlying connection.

**Risk, pinned by a test:** if `o11ygin`'s middleware wraps `c.Writer` in a type
lacking `Unwrap`, the controller returns `ErrNotSupported` and the extension
becomes a silent no-op — uploads would then die at 15s with a confusing error. The
handler treats that error as fatal rather than ignoring it, and a test asserts the
call succeeds through the real middleware chain.

`applyBaseMiddleware` and the `httpWriteTimeout` constant are **untouched**.

### 2.3 Streaming relay

```
c.Request.MultipartReader()  →  io.Pipe  →  multipart.Writer  →  resty SetBody(pr)
```

Parts are read one at a time and re-encoded into a fresh multipart body written
into the pipe by a goroutine; resty reads the pipe's read end as the request body.
The goroutine's termination path is the pipe: it always closes the writer (with
`CloseWithError` on failure), which unblocks the reader — no leak.

**Correction — memory does NOT stay flat.** This section originally claimed it
did. resty v2.17.2 calls `getBodyCopy` after building the request, and for an
`io.Reader` body (`GetBody == nil`, always so here) that path runs `io.ReadAll`
before the connection is dialled. Measured: a 64 MiB body allocated 375 MiB and
was fully drained before the dial. The pipe still bounds what the *relay*
goroutine holds, but the process holds the whole artifact regardless, so
`admin-service` must be sized for it. Fixing it needs a decision this design does
not make — drive the one call with `net/http` (against the project's "never
`net/http` client directly" rule), move to resty v3, or accept the buffering —
and two coupled changes if it is fixed: the `errors.Is(res.err, uploadErr)` blame
attribution currently works *because* resty drains the pipe first, and
`client-update-service`'s read timeout is presently masked by the same buffering.

Each part is copied through under its own `part.FormName()` and `part.FileName()`,
so field names reach `client-update-service` unchanged and the filenames are
available for the audit entry (§2.5) before the body finishes streaming. Every
file part is relayed, including ones the handler does not recognise —
`client-update-service` alone decides which fields it requires (decision 3). A
malformed multipart body, or a request that is not multipart at all, fails at
`MultipartReader()` and returns `400` before anything is sent upstream.

Part order is irrelevant: `client-update-service` uses `c.FormFile`, which parses
the whole body before looking up either field.

**Two hard constraints on the resty client, both load-bearing:**

- **Never enable `SetContentLength`.** resty buffers an entire `io.Reader` body
  into memory when it is set (`middleware.go:519-527`), which would defeat
  streaming completely. `restyutil.New` does not set it; it must stay that way.
- **No retries.** A retry after the pipe has been drained would send an empty or
  partial body. `restyutil.New` configures none; this client must not add any.

Both get a comment at the construction site and a test.

### 2.4 Upstream error mapping

| `client-update-service` | admin sees | rationale |
|---|---|---|
| `400` | `400`, upstream message relayed | the admin's files are genuinely wrong |
| `401` / `403` | `503`, cause logged server-side | `admin-service`'s own token is bad — a deployment fault. Relaying `401` would read to the admin as an expired session and send them to a pointless re-login |
| other `5xx` | `503` | |
| transport error / timeout | `503` | |

`503` is `errcode.Unavailable` (`pkg/errcode/category.go:44`). The upstream token
never appears in a message, a cause, or a log field.

The upstream envelope is **not** forwarded verbatim. On a `400`, `admin-service`
builds its own `errcode.BadRequest` carrying the upstream `error` text, and does
not copy the upstream `reason` — reasons are a contract between one service and
its own clients, and re-emitting another service's would put codes into
`admin-service`'s surface that `docs/client-api.md` §9 does not document.

### 2.5 Audit

```go
h.audit(ctx, c, "client_update.upload", "", "", map[string]string{
    "configFile":  cfgName,
    "executeFile": exeName,
})
```

Reuses the existing best-effort hook — a failed audit write is logged, never fails
the request, consistent with every other mutating admin action. Filenames only;
`AuditEntry.Details` is documented as non-secret context.

Written only on a successful upstream response, so the log records what was
actually published.

### 2.6 Config (`config.go`)

```go
ClientUpdateURL     string        `env:"CLIENT_UPDATE_URL,required"`
ClientUpdateToken   string        `env:"CLIENT_UPDATE_TOKEN,required"`
ClientUpdateTimeout time.Duration `env:"CLIENT_UPDATE_UPLOAD_TIMEOUT" envDefault:"10m"`
```

`loadConfig` validates the URL parses and the token is non-empty, and
**deliberately does not** run `ClientUpdateTimeout` through `checkHandlerTimeout`
— it is intentionally far above `httpWriteTimeout`, which is the entire reason
§2.2 exists. This carries a comment, or the next reader will "fix" it.

Client construction, in `run()`:

```go
restyutil.New(cfg.ClientUpdateURL,
    restyutil.WithBearerToken(cfg.ClientUpdateToken),
    restyutil.WithTimeout(cfg.ClientUpdateTimeout))
```

Overriding the timeout is required, not optional: `restyutil`'s 30s default would
cut a large upload off mid-body.

The relay is injected into `Handler` as an interface defined in `client_update.go`
(consumer-side, per CLAUDE.md §3), so tests can substitute a fake without an HTTP
server:

```go
type versionUploader interface {
    Upload(ctx context.Context, contentType string, body io.Reader) error
}
```

---

## 3. `admin-frontend` — Updates console

### 3.1 API client

`src/api/admin/index.ts` gains
`uploadClientVersion(authToken, configFile, executeFile, onProgress?, signal?)`,
building a `FormData` with the `configFile` and `executeFile` fields.
`authToken` is the admin session token and is sent as `Authorization: Bearer
<authToken>`; `signal` is an optional `AbortSignal` that cancels an upload in
flight.
`Content-Type` is never set by hand — the browser must write its own multipart
boundary.

**Deliberate deviation:** this one method uses `XMLHttpRequest` rather than
`fetch`, because `fetch` cannot report upload progress and a multi-hundred-MB
upload with a dead UI for minutes is unacceptable. The deviation is confined to
this method; non-2xx responses are still parsed into `AsyncJobError` through the
same envelope shape `parseHttpEnvelopeError` produces, so callers see no
difference.

### 3.2 Console

New `src/components/UpdatesConsole/` — `UpdatesPage.jsx`, `index.jsx`,
`style.css`, `UpdatesPage.test.jsx` — matching the existing component-folder
convention used by `UsersConsole`, `AuditView` and `PermissionsView`.

`AppShell.jsx`'s nav gains `{ key: 'updates', label: 'Updates', Component: lazy(() => import('@/components/UpdatesConsole')) }`,
lazy-loaded like its siblings.

**Deploy-gated** (added 2026-09-01): the section carries `gate: updatesEnabled`, so
the tab renders only where the runtime config flag `UPDATES_ENABLED` is the literal
string `"true"` (nginx envsubst renders it from the container env; default `false`,
dev compose included). This mirrors `PERMISSIONS_ENABLED` and gates the UI section
only — `admin-service`'s `POST /v1/admin/client-updates` is unchanged and stays
reachable with an admin token. To actually disable uploads for a site, leave
`client-update-service`'s `UPLOAD_TOKENS` empty (§1.1) — an empty table is a valid
config meaning "uploads disabled". Clearing `admin-service`'s `CLIENT_UPDATE_TOKEN`
is not an alternative: it is `required` and rejected when empty, so admin-service
fails to start.

States: idle → both files chosen → uploading (percentage) → success / error.
Upload is disabled until both files are chosen. Error copy goes through
`formatAsyncJobError`.

---

## 4. Testing

TDD throughout, per CLAUDE.md §4: tests first, confirmed failing, then implementation.

### 4.1 `client-update-service`

`middleware_test.go`, table-driven: no header; `Basic` scheme; lowercase `bearer`;
empty token after the prefix; unknown token; valid token; a token that is a proper
prefix of a valid one; a second account's token. `config_test.go`: the map parses,
and validation rejects empty-account, empty-token and short-token.
Route level: `POST` without a credential returns `401` **and the mock store's
`Put` is never called**; `GET` without one still returns `200`, pinning the
deliberate asymmetry.

### 4.2 `admin-service`

Handler behaviour is tested against the fake `versionUploader` — no HTTP server
needed for the routing, auth, audit and error-mapping cases. The status-mapping
table in §2.4 is additionally covered against a real `httptest` upstream through
the real resty client, since that is where the mapping actually happens.

- non-admin session → `403`, upstream never called
- upstream `400` → `400` with the message relayed
- upstream `401` → `503`, **not** `401`
- transport failure → `503`
- success → `200`, audit entry written carrying both filenames
- the deadline extension succeeds through the real middleware chain (§2.2 risk)
- a body larger than any internal buffer arrives byte-identical upstream, proving
  the relay streams rather than truncates

### 4.3 `admin-frontend`

vitest on the client method — `FormData` field names, no hand-set `Content-Type`,
error-envelope parsing, progress callback — and on the console's state machine.

### 4.4 Integration

`client-update-service/integration_test.go` gains an authenticated round-trip and
an unauthenticated rejection.

### 4.5 Coverage

80% floor, 90%+ on the new handler and middleware, per CLAUDE.md §4.

---

## 5. Docs (same PR)

- `docs/client-api.md`: new §9.17 for `POST /v1/admin/client-updates`; rows in the
  reason-code table; §12 rewritten — the blanket "UNAUTHENTICATED in v1" banner
  becomes an auth line on `POST` plus a `401` row, with the warning narrowed to
  the still-open `GET`. TOC updated.
- `docs/client-api/request-reply.md`: the matching blocks under
  "HTTP — Admin Service" and "HTTP — Client Update Service", so the derived views
  do not drift.
- `client-update-service/deploy/docker-compose.yml`: a dev `UPLOAD_TOKENS`.
- `admin-service/deploy/docker-compose.yml`: `CLIENT_UPDATE_URL` /
  `CLIENT_UPDATE_TOKEN` / `CLIENT_UPDATE_UPLOAD_TIMEOUT`, with the dev token
  matching the one above.
- The 2026-08-03 spec's "auth deferred to a follow-up" bullet is left as-is; it is
  a historical record of that decision, and this document is the follow-up.

## 6. Verification

`make test SERVICE=client-update-service`, `make test SERVICE=admin-service`,
`make lint`, `make sast`, `make test-integration SERVICE=client-update-service`,
and `npm test` in `admin-frontend`. No `make generate` — no store interface changes.
