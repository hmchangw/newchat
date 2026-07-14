# Unified `/api/v1` Gateway Migration — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use `- [ ]` checkboxes.

**Goal:** Finish the unified backend-endpoint architecture: a Traefik gateway (`:7777` = `baseUrl`) fronting auth/upload/media/botplatform under `/api/v1/*`; migrate the remaining `/v1/*` routes to `/api/v1/*`; fix portal's directory so bot/admin accounts resolve; make the admin-frontend console-only (remove chat) and route its password-change through admin-service.

**Architecture:** Portal stays the pre-`baseUrl` discovery entry (portal-direct, NOT behind the gateway). Portal returns `baseUrl = gateway`; main clients then hit `{baseUrl}/api/v1/{auth,password/change,file,avatar}` via Traefik. The admin-frontend talks only to admin-service (admin ops + password-change proxy) + portal (login). Dev mode skips all validate.

## Global Constraints / locked decisions
- Portal is **portal-direct** (`PORTAL_URL=:8085`); gateway does **not** route `/api/v1/login`.
- Password-change: **admin-frontend → admin-service (proxy) → bp `/api/v1/password/change`**; main clients → `{baseUrl}/api/v1/password/change` → Traefik → bp.
- **Dev mode: skip all validate** (dev login is the tokenless `{account}` path).
- **Remove the entire chat stack** from admin-frontend (console-only; drop `nats.ws`/`nkeys.js`).
- `natsUrl` stays in the portal bundle; `authServiceUrl` is already gone; `botplatformUrl` stays in `PORTAL_SITE_URLS` (portal's forward target).
- Go: `make` targets, TDD, ≥80% cov, `pkg/errcode`, explicit Mongo projections, no `$lookup` unless justified. Frontend: chat-frontend/CLAUDE.md conventions. Commit messages: **no AI-provenance trailers** (repo convention).

---

### Task 1: botplatform-service — `/api/v1` migration + CORS

**Files:** `botplatform-service/routes.go`, `main.go` (middleware), `handler_test.go`.
- Routes → keep `POST /api/v1/login`; migrate `POST /v1/auth/validate` → `POST /api/v1/auth/validate`; `POST /v1/password/change` → `POST /api/v1/password/change`; **drop** `POST /v1/login`. Keep `GET /healthz`.
- Add **CORS middleware** mirroring auth-service's `pkg/ginutil.CORS()` (password-change + login are now browser-facing via the gateway/portal). Confirm `pkg/ginutil` exists and how auth-service wires it; match it.

- [ ] Update tests that hit the old paths to the new `/api/v1/*` paths (don't weaken assertions).
- [ ] `make test SERVICE=botplatform-service` PASS; `make lint`.
- [ ] Commit `feat(botplatform-service): migrate routes to /api/v1 + CORS`.

### Task 2: auth-service — dev-mode dispatch + validator URL

**Files:** `auth-service/handler.go`, `bpvalidator.go`, `handler_test.go`.
- `bpvalidator.go`: `POST {baseURL}/v1/auth/validate` → `/api/v1/auth/validate`.
- `handler.go` `HandleAuth`: remove the top-level `if h.devMode { handleDevAuth; return }`; instead dispatch:
  ```
  switch {
  case req.SSOToken != "": handleSSO(c)
  case req.AuthToken != "": handleSession(c)
  default: if h.devMode { handleDevAuth(c) } else { <400 missing_token> }
  }
  ```
  (Match the existing `handleSSO`/`handleSession`/error names — read the file.)

- [ ] Tests: dev mode with `{account}` (no token) → dev mint (no validate); dev mode with `{authToken}` → session path still validates (documented edge, not the dev flow); prod unchanged. Update any test asserting the old dev short-circuit.
- [ ] `make test SERVICE=auth-service` PASS; `make lint`. Update `docs/client-api.md` §2.2 if the request dispatch table changed.
- [ ] Commit `feat(auth-service): dev-auth only on tokenless requests; validator /api/v1`.

### Task 3: portal-service — users-primary directory + route + port

**Files:** `portal-service/store_mongo.go` (the `ListEmployees` aggregation), `store.go` (comments), `routes.go`, `handler.go` (comments), `main.go`/config (port default), `store` integration test, `deploy/docker-compose.yml`.
- **Directory fix (the core):** change the load-time aggregation from an INNER `hr_employee ∩ users` intersection to **`users`-primary with `hr_employee` LEFT-joined** — so every `users` account (incl. bot/admin) is in the directory with `account`, `siteId`, `roles`, `_id`; humans additionally get `employeeId` + rich fields from `hr_employee`. Add an inline `// $lookup justification:` (a left-join lookup is the documented reason). Keep explicit projections.
- Route: `POST /v1/login` → `POST /api/v1/login`.
- Port default → `8085` (whatever env var sets portal's listen port); update `deploy/docker-compose.yml`.

- [ ] Integration test (`testutil.MongoDB`): a bot/admin account present in `users` but NOT `hr_employee` IS now in the directory (was excluded before); a human still gets rich fields. Unit/handler tests updated for the `/api/v1/login` path.
- [ ] `make test SERVICE=portal-service` + `make test-integration SERVICE=portal-service` (Docker permitting) PASS; `make lint`.
- [ ] Commit `fix(portal-service): users-primary directory (include bots/admins); /api/v1/login; port 8085`.

### Task 4: admin-service — password-change proxy + requireAuth + config

**Files:** `admin-service/config.go` (`BOTPLATFORM_URL`), `middleware.go` (`requireAuth`), `handler.go` + `routes.go` (proxy handler), `store`/http client, tests, `deploy/docker-compose.yml`.
- Config: add `BOTPLATFORM_URL` (env `BOTPLATFORM_URL`, sensible local default e.g. `http://botplatform-service:8080`).
- `requireAuth(store, siteID)` middleware: like `requireAdmin` but WITHOUT the admin-role check — validates the session token (present + resolves + same site), stamps the principal. (Reuse the sessiontoken hash + `FindSessionByHash` path.)
- New route `POST /v1/password/change` under `requireAuth` → handler **proxies** the request (`Authorization: Bearer <token>` + `{oldPassword,newPassword}` body) to `${BOTPLATFORM_URL}/api/v1/password/change` (Resty), returning bp's response/errcode envelope through `errhttp`. On bp network failure → 502.
- `deploy/docker-compose.yml`: add `BOTPLATFORM_URL`.

- [ ] Tests (mocked bp HTTP): valid session proxies + returns bp success; bp 401/400 propagated; non-session → 401; bp unreachable → 502. `requireAuth` allows a non-admin session (unlike requireAdmin).
- [ ] `make test SERVICE=admin-service` PASS; `make lint`.
- [ ] Commit `feat(admin-service): password-change proxy to botplatform + requireAuth`.

### Task 5: Traefik gateway + docker-local

**Files:** `docker-local/compose.*.yaml` (add traefik), any gateway config, `docker-local/setup.sh` (ports).
- Add `traefik:3.6.4` service, entrypoint `:7777`, dashboard optional. Route (via labels or a dynamic config file):
  - `PathPrefix(/api/v1/auth)` → auth-service
  - `PathPrefix(/api/v1/file)` → upload-service
  - `PathPrefix(/api/v1/avatar)` → media-service
  - `PathPrefix(/api/v1/password/change)` → botplatform-service
  (Do NOT route `/api/v1/login`.) Optional Traefik CORS middleware (bp already has its own from Task 1).
- Fix ports: portal `8085` (avoid mongo-express `8081`); ensure `7777` free. Update any seed/compose env that references portal's port.

- [ ] Validate compose config (`docker compose config` if Docker available; else structural review vs existing services). Confirm each `PathPrefix` maps to the right service + port.
- [ ] Commit `feat(docker-local): traefik unified api gateway :7777`.

### Task 6: admin-frontend — remove chat + password-change via admin-service

**Files:** delete the copied chat stack; edit `App.jsx`, `AppShell`, `AdminLoginPage`, `botAuth.ts`, `api/admin`, `runtimeConfig`, `package.json`.
- **Remove the entire chat stack** added in Phase 2: `context/{NatsContext,RoomKeysContext,RoomEventsContext,ThreadEventsContext,DebugContext,ThemeContext(if only chat used it)}`, `components/MainApp/**`, the copied chat `api/<op>/**` + chat `_transport` (keep `httpEnvelope`), `lib/{roomcrypto,idgen,jwtExpiry,messageBuffer,roomFormat,constants}` if only chat used them, `hooks/{useTypeaheadSearch,useHoverWithDelay}`, `api/auth/oidcClient.js`, the chat `shared/*` (MessageList, etc.), and the `Chat` section + NATS wiring in `AppShell`/`App.jsx`/`AdminLoginPage`. Remove `nats.ws`/`nkeys.js` from `package.json`. **Method:** delete, then let `npm run build`/`typecheck`/`vitest` name anything still referenced; keep the console (Users/Audit/login) fully working.
- Restore `App.jsx` to the console-only gate (`ErrorBoundary > AuthProvider > (session? AppShell : AdminLoginPage)`), no `NatsProvider`.
- `botAuth.ts`: trim the `Bundle` to what the console needs (`{userId?, authToken, account, siteId, requirePasswordChange}`); `changePassword` no longer takes `baseUrl` — it calls **admin-service** (`POST ${ADMIN_SERVICE_URL}/v1/password/change`, `Authorization: Bearer`, `{oldPassword,newPassword}`). Move `changePassword` into `api/admin` (it's an admin-service call now) or keep in `botAuth` but pointed at admin-service.
- `AdminLoginPage`: drop the `nats.connect` call; the forced-change path calls the new `changePassword` (no `baseUrl`).
- `runtimeConfig`: `PORTAL_URL` default → `http://localhost:8085`.

- [ ] `npm run build` + `typecheck` + `vitest` all green (console tests intact; chat tests deleted with the code).
- [ ] Commit `refactor(admin-frontend): console-only (remove chat); password-change via admin-service; portal :8085`.

### Task 7: chat-frontend port + docs

**Files:** `chat-frontend/src/lib/runtimeConfig.js`, `chat-frontend/deploy/*` / `docker-local/setup.sh`, `docs/client-api.md`.
- `chat-frontend` `PORTAL_URL` default → `http://localhost:8085`.
- `docs/client-api.md`: update the moved endpoints — portal `/api/v1/login`, botplatform `/api/v1/{login,auth/validate,password/change}`, the admin-service `/v1/password/change` proxy, and the baseUrl/gateway note. Update the derived views if touched.

- [ ] `chat-frontend`: `npm run typecheck` + `vitest` green.
- [ ] Commit `chore(chat-frontend): portal :8085; docs: unified /api/v1 endpoints`.

---

## Self-Review
- Directory flaw (core) → Task 3. Route migrations → 1/2/3. Dev-mode → 2. Gateway → 5. admin-service proxy → 4. admin-frontend console-only → 6. Ports/docs → 6/7.
- Ordering: bp paths (1) before auth-validator (2) and portal-forward (3) and admin proxy (4) that depend on them; frontends (6/7) last.

## After
Verify the full local stack conceptually; then decide whether to squash this migration into the existing frontend/backend commits or keep as a separate "unified-gateway" commit — ask the user.
