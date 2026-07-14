# admin-frontend (console) Implementation Plan — Phase 1

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A new separate internal `admin-frontend/` Vite/React app: `/admin-login` (password login, reusing the bot/admin flow) + a Settings→Users admin console backed by the existing `admin-service` REST API. **Pure HTTP — no NATS in Phase 1.**

**Architecture:** A sibling app to `chat-frontend/` (no shared package exists yet, so the small login + infra pieces are **copied** from chat-frontend and adapted). Admin logs in via `POST ${PORTAL_URL}/v1/login` → bundle containing `authToken`; the console sends `Authorization: Bearer ${authToken}` to `admin-service` `/v1/admin/*`. The same `pkg/errcode` envelope flows over HTTP, so the copied `formatAsyncJobError` handles admin errors.

**Tech Stack:** Vite 6, React 19, vitest 2 + @testing-library/react + jsdom, plain CSS design tokens. (No `nats.ws`/`nkeys.js`/`oidc-client-ts` in Phase 1 — chat/SSO are Phase 2.)

**Out of scope (Phase 2, separate plan):** the NATS chat client (search/DM/rooms/messages/members), `NatsContext`/`useJwtRefresh`, the `MainApp` chat tree.

## Global Constraints

- Follow `chat-frontend/CLAUDE.md` conventions verbatim: folder-per-component (`Name/{Name.jsx,index.jsx,style.css,Name.test.jsx}`); `api/` is TypeScript, components/pages/lib are `.jsx`/`.js`; `@/` alias → `src/` (mirror in vite/vitest/tsconfig); design tokens only (no hardcoded hex/px); tests colocated `Foo.test.jsx`; branch on `reason ?? code`, never message text; components never deep-import `api/_transport`.
- Error envelope: `{ error, code, reason?, metadata? }`; `AsyncJobError` carries `code`/`reason`; `formatAsyncJobError` is reason-keyed. Admin reasons: `not_admin` (403), `invalid_token` (401), `user_not_found` (404), `account_exists` (409), `missing_fields` (400).
- Runtime config: `window.__APP_CONFIG__` (prod) → `import.meta.env.VITE_*` (dev) → default. New vars: `VITE_PORTAL_URL` (default `http://localhost:8081`), `VITE_ADMIN_SERVICE_URL` (default `http://localhost:8082`).
- admin-service endpoints (already built, under `/v1/admin`, Bearer `authToken`): `GET /users?q=&page=&limit=`, `POST /users`, `GET /users/:id`, `PATCH /users/:id`, `POST /users/:id/password`, `GET /users/:id/sessions`, `DELETE /users/:id/sessions`, `DELETE /users/:id/sessions/:sessionId`, `GET /audit?targetUserId=&actor=&action=&page=&limit=`.
- User projection returned by admin-service: `{ id, account, siteId, engName, chineseName, roles, deactivated, requirePasswordChange }` (never a password/hash).
- `npm run build`, `npm run typecheck`, `npm test` must pass on every task. Commit trailers per repo git rules; never mention model identity.

---

## File Structure (new, under `admin-frontend/`)

```text
admin-frontend/
  package.json  vite.config.js  vitest.config.js  tsconfig.json  index.html
  src/
    main.jsx  App.jsx
    styles/          tokens.css  index.css            (copied verbatim from chat-frontend)
    lib/             runtimeConfig.js
    api/
      _transport/    httpEnvelope.ts (AsyncJobError + formatAsyncJobError + parse)
      auth/          botAuth.ts (botLogin, changePassword)
      admin/         index.ts (listUsers/getUser/createUser/updateUser/setPassword/listSessions/revokeSessions/revokeSession/listAudit)
      index.ts       (barrel)
    context/         AuthContext/ (AuthProvider, useAuth)
    pages/
      AdminLoginPage/    ChangePasswordForm/
    components/
      UsersConsole/   (UsersPage, UserTable, CreateUserForm, EditUserDialog, SetPasswordDialog, SessionsDialog)
      AuditView/
      AppShell/       (nav + logout)
      shared/         (Modal, ErrorBoundary, LazyFallback — copied/trimmed)
  deploy/            Dockerfile  nginx.conf  config.js.template  30-render-config.sh  azure-pipelines.yml
```

---

### Task 1: Scaffold the app

**Files:** `admin-frontend/package.json`, `vite.config.js`, `vitest.config.js`, `tsconfig.json`, `index.html`, `src/main.jsx`, `src/App.jsx`, `src/styles/tokens.css`, `src/styles/index.css`, `src/App.test.jsx`.

**Interfaces:** Produces a bootable app rendering a placeholder; the `@/` alias.

- [ ] **Step 1: Copy styles + config skeleton.** Copy `chat-frontend/src/styles/tokens.css` and `index.css` verbatim. Create `package.json` (`"type":"module"`, scripts `dev`/`build`/`preview`/`test`/`typecheck` mirroring chat-frontend; deps: `react ^19.1.0`, `react-dom ^19.1.0`, `uuid ^11.1.0`; dev: `vite ^6.3.2`, `@vitejs/plugin-react ^4.4.1`, `vitest ^2.1.0`, `jsdom`, `@testing-library/react ^16`, `@testing-library/jest-dom`, `typescript ^6.0.3`). `vite.config.js` (`react()`, alias `@`→`src`, `server.port: 3001`). `vitest.config.js` + `tsconfig.json` with the SAME `@` alias (`allowJs:true`,`checkJs:false`,strict). `index.html` with `<script src="/config.js"></script>` before the module + a root div.

- [ ] **Step 2: Write a failing App test.** `src/App.test.jsx`: renders `<App/>`, expects a placeholder heading (e.g. "Admin"). Run `npm install` then `npx vitest run src/App.test.jsx` → FAIL (no App).

- [ ] **Step 3: Implement `main.jsx` + minimal `App.jsx`.** `main.jsx`: `createRoot(...).render(<StrictMode><App/></StrictMode>)`, imports `./styles/tokens.css` then `./styles/index.css`. `App.jsx`: render a placeholder `<h1>Admin</h1>` for now.

- [ ] **Step 4: Verify.** `npm run build` OK; `npx vitest run` PASS; `npm run typecheck` clean.

- [ ] **Step 5: Commit.**
```bash
git add admin-frontend
git commit -m "feat(admin-frontend): scaffold Vite/React app"
```

---

### Task 2: Runtime config + HTTP error envelope

**Files:** `admin-frontend/src/lib/runtimeConfig.js`, `src/api/_transport/httpEnvelope.ts`, `src/api/index.ts`, tests for each.

**Interfaces:**
- Produces `PORTAL_URL`, `ADMIN_SERVICE_URL` (runtimeConfig).
- Produces `class AsyncJobError extends Error { code?; reason?; metadata? }`, `formatAsyncJobError(err): string`, `async parseHttpEnvelopeError(resp, fallback): Promise<never>` (throws `AsyncJobError` from a non-2xx `{error,code,reason,metadata}` body).

- [ ] **Step 1: Failing tests.**
  - `runtimeConfig.test.js`: with no `window.__APP_CONFIG__`, `PORTAL_URL`/`ADMIN_SERVICE_URL` fall back to defaults; with `window.__APP_CONFIG__` set, they read from it.
  - `httpEnvelope.test.ts`: `parseHttpEnvelopeError` on a `403 {code:'forbidden',reason:'not_admin',error:'admin role required'}` throws `AsyncJobError` with `.code==='forbidden'`, `.reason==='not_admin'`; `formatAsyncJobError` returns friendly copy for `not_admin`/`account_exists`/`invalid_token` and falls back to `err.message` otherwise.

- [ ] **Step 2: Run → FAIL.**

- [ ] **Step 3: Implement.** `runtimeConfig.js` mirrors chat-frontend's read chain (`window.__APP_CONFIG__?.X ?? import.meta.env.VITE_X ?? default`). `httpEnvelope.ts`: port a slim version of chat-frontend `api/_transport/asyncJob.ts`'s `AsyncJobError` + `formatAsyncJobError` (keep a small `REASON_COPY` with the admin reasons + a couple of shared ones) + `parseHttpEnvelopeError`. Re-export all three from `api/index.ts`.

- [ ] **Step 4: Run → PASS; typecheck clean.**

- [ ] **Step 5: Commit** `feat(admin-frontend): runtime config + http error envelope`.

---

### Task 3: Auth API + AuthContext

**Files:** `src/api/auth/botAuth.ts`, `src/context/AuthContext/{AuthContext.jsx,index.jsx}`, tests.

**Interfaces:**
- Produces `botLogin({username,password}): Promise<Bundle>` (`Bundle = {userId, authToken, account, siteId, authServiceUrl, baseUrl, natsUrl, requirePasswordChange}`), `changePassword({baseUrl, authToken, oldPassword, newPassword}): Promise<void>`.
- Produces `AuthProvider`, `useAuth()` → `{ session, login, completePasswordChange, logout }` where `session = {authToken, account, siteId} | null`.

- [ ] **Step 1: Failing tests** (mock `fetch`).
  - `botAuth.test.ts`: `botLogin` POSTs `${PORTAL_URL}/v1/login` `{username,password}`, returns the parsed bundle; a `401 {reason:'invalid_credentials'}` throws `AsyncJobError`. `changePassword` sends `Authorization: Bearer` + body; non-2xx throws.
  - `AuthContext.test.jsx`: `login` stores the bundle in `sessionStorage` (key `admin.session`) and exposes `session`; on mount an existing stored session is restored; `logout` clears it.

- [ ] **Step 2: Run → FAIL.**

- [ ] **Step 3: Implement.** `botAuth.ts`: copy chat-frontend `api/auth/botAuth.js` behavior (using this app's `parseHttpEnvelopeError`). `AuthContext`: hold `session`, persist bundle to `sessionStorage` (`admin.session`, tab-scoped) on `login(bundle)`; restore on mount; `logout()` clears storage + state. `completePasswordChange` is a passthrough that finalizes login after a forced change (stores the bundle). Keep only `{authToken, account, siteId}` in the exposed `session` (don't leak the rest).

- [ ] **Step 4: Run → PASS; typecheck clean.**

- [ ] **Step 5: Commit** `feat(admin-frontend): botAuth + AuthContext (sessionStorage)`.

---

### Task 4: `/admin-login` page + change-password gate + routing

**Files:** `src/pages/AdminLoginPage/{AdminLoginPage.jsx,index.jsx,style.css,AdminLoginPage.test.jsx}`, `src/pages/ChangePasswordForm/{ChangePasswordForm.jsx,index.jsx,style.css,ChangePasswordForm.test.jsx}`, modify `src/App.jsx` (+ `App.test.jsx`).

**Interfaces:** Consumes `botLogin`/`changePassword`, `useAuth`. Produces the authenticated-vs-login gate in `App.jsx`.

- [ ] **Step 1: Failing tests.**
  - `ChangePasswordForm.test.jsx`: copy chat-frontend's — required fields, `new===confirm`, `new!==old`; calls `onSubmit({oldPassword,newPassword})`.
  - `AdminLoginPage.test.jsx`: happy login (no forced change) → `useAuth().login` called with the bundle; `requirePasswordChange:true` → renders the change step, submit → `changePassword` then `login`; `invalid_credentials` → uniform error copy.
  - `App.test.jsx`: when `useAuth().session` is null → renders `AdminLoginPage`; when set → renders the app shell (placeholder for now).

- [ ] **Step 2: Run → FAIL.**

- [ ] **Step 3: Implement.** Copy `ChangePasswordForm` verbatim (rename folder). Adapt `BotLoginPage`→`AdminLoginPage`: same two-step flow but on success call `useAuth().login(bundle)` instead of `connect({mode:'session'})`; admin branding/title ("Admin sign in"); reuse `.login-page`/`.login-form` classes (copy `style.css`). `App.jsx`: wrap in `<AuthProvider>`; if `!session` render `<AdminLoginPage/>`, else render `<AppShell/>` (placeholder until Task 7). No pathname routing needed (login is the only unauthenticated screen); optionally guard `window.location.pathname` to `/admin-login` cosmetically.

- [ ] **Step 4: Run → PASS; build + typecheck clean.**

- [ ] **Step 5: Commit** `feat(admin-frontend): /admin-login page + change-password gate`.

---

### Task 5: Admin REST API client

**Files:** `src/api/admin/index.ts` (+ types), `src/api/admin/admin.test.ts`, re-export from `src/api/index.ts`.

**Interfaces:** Produces, each taking `(authToken, args)` and returning parsed JSON or throwing `AsyncJobError`:
- `listUsers(authToken, {q?, page?, limit?}) → {users: AdminUser[], total: number}`
- `getUser(authToken, id) → AdminUser`
- `createUser(authToken, {account, engName?, chineseName?, roles, password, requirePasswordChange?}) → AdminUser`
- `updateUser(authToken, id, {engName?, chineseName?, roles?, deactivated?}) → AdminUser`
- `setPassword(authToken, id, {newPassword, requirePasswordChange?}) → void`
- `listSessions(authToken, id) → AdminSession[]`
- `revokeAllSessions(authToken, id) → void`; `revokeSession(authToken, id, sessionId) → void`
- `listAudit(authToken, {targetUserId?, actor?, action?, page?, limit?}) → {entries: AuditEntry[], total: number}`
- Types `AdminUser = {id,account,siteId,engName,chineseName,roles:string[],deactivated:boolean,requirePasswordChange:boolean}`, `AdminSession = {id,userId,siteId,issuedAt:number}`, `AuditEntry = {id,actorAccount,action,targetUserId,targetAccount?,details?,timestamp:number}`.

- [ ] **Step 1: Failing tests** (mock `fetch`): each function hits the right `${ADMIN_SERVICE_URL}/v1/admin/...` path + method, sends `Authorization: Bearer <token>`, JSON body where applicable; a `403 {reason:'not_admin'}` throws `AsyncJobError` with `.reason`; a `409 {reason:'account_exists'}` on create throws.

- [ ] **Step 2: Run → FAIL.**

- [ ] **Step 3: Implement** a small typed `fetch` wrapper (`adminFetch(authToken, method, path, body?)`) that sets the header, JSON-encodes, and calls `parseHttpEnvelopeError` on non-2xx; build each function on it. Re-export from `api/index.ts`.

- [ ] **Step 4: Run → PASS; typecheck clean.**

- [ ] **Step 5: Commit** `feat(admin-frontend): admin-service REST client`.

---

### Task 6: Users console UI

**Files:** `src/components/UsersConsole/{UsersPage,UserTable,CreateUserForm,EditUserDialog,SetPasswordDialog,SessionsDialog}/…`, `src/components/shared/{Modal,LazyFallback}/…` (copy/trim from chat-frontend), tests for each stateful piece.

**Interfaces:** Consumes the Task 5 client + `useAuth().session.authToken`. Produces `<UsersPage/>`.

- [ ] **Step 1: Failing tests** (mock `@/api`):
  - `UsersPage`: on mount calls `listUsers(token, {})`, renders rows; typing in search (debounced) re-queries with `{q}`; a `403` renders an "not authorized" state.
  - `CreateUserForm`: validates required `account`+`password`+≥1 role; submit calls `createUser` with the form; `account_exists` shows the friendly error; success closes + refreshes.
  - `EditUserDialog`: toggling roles / deactivated calls `updateUser` with only changed fields; deactivate confirms.
  - `SetPasswordDialog`: new+confirm required and equal; calls `setPassword`; a "force change on next login" checkbox maps to `requirePasswordChange`.
  - `SessionsDialog`: lists sessions (`issuedAt` formatted); "revoke" calls `revokeSession`; "revoke all" calls `revokeAllSessions`; list refreshes.

- [ ] **Step 2: Run → FAIL.**

- [ ] **Step 3: Implement.** `UsersPage`: search box (reuse a small debounce hook — copy `useDebouncedSearch`) + `UserTable` (account, name, roles, status badge, actions) + a "New user" button opening `CreateUserForm`. Dialogs use a copied `shared/Modal` + the `.dialog*` token classes; lazy-load dialogs via `React.lazy` + `<Suspense fallback={<LazyFallback variant="dialog"/>}>`. All errors via `formatAsyncJobError`. Every mutation refreshes the affected data.

- [ ] **Step 4: Run → PASS; build + typecheck clean.**

- [ ] **Step 5: Commit** `feat(admin-frontend): Users management console`.

---

### Task 7: Audit view + app shell + logout

**Files:** `src/components/AuditView/…`, `src/components/AppShell/…`, modify `src/App.jsx`, tests.

**Interfaces:** Produces `<AppShell/>` (nav between Users / Audit, shows signed-in admin account, Logout) rendered when authenticated.

- [ ] **Step 1: Failing tests.**
  - `AuditView`: calls `listAudit(token, {})`, renders entries newest-first (actor, action, target, time); filter inputs (action/targetUserId) re-query.
  - `AppShell`: renders the admin account + a Logout button that calls `useAuth().logout`; nav switches between Users and Audit.
  - `App.test.jsx`: authenticated → `AppShell` with Users default.

- [ ] **Step 2: Run → FAIL.**

- [ ] **Step 3: Implement** `AppShell` (simple sidebar/topbar using tokens; sections Users + Audit; footer with account + Logout) and `AuditView` (filter row + table). Wire `App.jsx` to render `<AppShell/>` when `session` is set.

- [ ] **Step 4: Run → PASS; build + typecheck clean.**

- [ ] **Step 5: Commit** `feat(admin-frontend): audit view + app shell`.

---

### Task 8: Deploy + docs

**Files:** `admin-frontend/deploy/{Dockerfile,nginx.conf,config.js.template,30-render-config.sh,azure-pipelines.yml}`, `admin-frontend/README.md`.

- [ ] **Step 1: Deploy files.** Copy chat-frontend's `deploy/*` pattern: node:22-alpine build (`npm ci && npm run build`) → nginx:alpine serving `dist/`; `config.js.template` sets `window.__APP_CONFIG__ = { PORTAL_URL:"${PORTAL_URL}", ADMIN_SERVICE_URL:"${ADMIN_SERVICE_URL}" }`; `30-render-config.sh` envsubst at container start (PORTAL_URL + ADMIN_SERVICE_URL required, fail-fast); `index.html` already loads `/config.js`. `azure-pipelines.yml` cloned from chat-frontend.

- [ ] **Step 2: README** — brief: what it is (internal admin console), env vars, `npm run dev` (`VITE_PORTAL_URL`/`VITE_ADMIN_SERVICE_URL`), and that chat is Phase 2.

- [ ] **Step 3: Verify** `npm run build` clean; `npm run typecheck` clean; `npx vitest run` all green.

- [ ] **Step 4: Commit** `feat(admin-frontend): deploy config + README`.

---

## Self-Review

- **Spec coverage (design §5):** separate app → Task 1; `/admin-login` reuse → Tasks 3–4; Settings→Users (search/create/roles/password/activate/sessions) → Tasks 5–6; audit view → Task 7; runtime config (PORTAL_URL + ADMIN_SERVICE_URL) → Task 2; deploy → Task 8. Chat client (§5.2 item 1) is **explicitly Phase 2**, not this plan.
- **Placeholder scan:** copy-from-chat-frontend steps name the exact source file; no vague TODOs.
- **Type consistency:** `Bundle`, `AdminUser`, `AdminSession`, `AuditEntry` defined in Tasks 3/5 and used consistently; `authToken` threaded from `useAuth().session` into the Task 5 client throughout.

## Phase 2 (separate plan, not built here)

Copy `NatsContext` + `useJwtRefresh` + `DebugContext`/`ThemeContext` + the `api/_transport` NATS transport + the chat `api/<op>/` set + the `components/MainApp` tree + `RoomKeys`/`RoomEvents`/`ThreadEvents` contexts, wire a `session`-mode connect after login, and add a "Chat" section to `AppShell`. Add a real `searchUsers` op (needs a backend user-search subject) if free-text DM targeting is insufficient.
