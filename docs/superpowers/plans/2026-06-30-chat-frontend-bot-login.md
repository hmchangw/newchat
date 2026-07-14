# chat-frontend bot/admin password login — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `/dev-login` username/password page for bot/admin accounts (forced first-login change-password), exchanging the session `authToken` for a NATS JWT and connecting, with sessionStorage reload-persistence and background JWT refresh.

**Architecture:** A new `api/auth/botAuth.js` wraps `portal /v1/login` and `botplatform /v1/password/change`. `NatsContext.connectToNats` is parameterized so the bot path reuses the existing user-account connect tail — it accepts a pre-resolved site `bundle` (from `/v1/login`, skipping `/api/userInfo`) and puts `authToken` in the `/auth` body. The bundle is persisted to `sessionStorage` for reload auto-reconnect; `useJwtRefresh` gains a session re-mint branch.

**Tech Stack:** React 19, Vite, Vitest + @testing-library/react, plain `fetch`, `nats.ws`.

## Global Constraints

- Frontend lives in `chat-frontend/`; run all commands from there. Tests: `npm test` (vitest run); single file: `npm test -- src/path/File.test.jsx`.
- `node_modules` must be installed first: `npm install` (one-time).
- Components are `.jsx`, co-located `index.jsx` re-exports `export { default } from './X'`, styles in `style.css` using existing CSS vars (`var(--accent)`, `var(--space-*)`, etc.).
- Imports use the `@/` alias for `src/`.
- Error display uses `formatAsyncJobError` from `@/api`; API errors are thrown as `AsyncJobError` carrying `{code, reason, metadata}`.
- Backend contract is owned by PR #428 — do NOT edit any Go service or `docs/client-api.md`.
- Route name is `/dev-login` (bot/admin password login; independent of `DEV_MODE`).
- Change-password endpoint: `POST ${baseUrl}/v1/password/change` with `Authorization: Bearer <authToken>`.
- Commit after each task with `noreply@anthropic.com` identity. Co-trailer:
  `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.
- Keep existing `dev`/`sso` connect behavior byte-for-byte unchanged: only the **session** path passes `mode`/`authToken` to `setCredentials`, so existing exact-match assertions stay green.

---

## File Structure

**New**
- `src/api/auth/botAuth.js` — `botLogin`, `changePassword`, envelope-error helper.
- `src/api/auth/botAuth.test.js`
- `src/pages/ChangePasswordPage/ChangePasswordForm.jsx` — controlled change-password form (presentational; calls back on submit).
- `src/pages/ChangePasswordPage/{index.jsx, style.css, ChangePasswordForm.test.jsx}`
- `src/pages/BotLoginPage/BotLoginPage.jsx` — login step + change-pwd gate + connect.
- `src/pages/BotLoginPage/{index.jsx, style.css, BotLoginPage.test.jsx}`

**Modified**
- `src/context/NatsContext/useJwtRefresh.js` — `mode`/`authToken` in `setCredentials`; session re-mint branch; optional `onSessionLost`.
- `src/context/NatsContext/NatsContext.jsx` — session `bundle` path in `connectToNats`; sessionStorage persist/clear; mount auto-reconnect; wire `onSessionLost`.
- `src/App.jsx` — `/dev-login` route branch.

---

## Task 1: `botAuth.js` API layer

**Files:**
- Create: `src/api/auth/botAuth.js`
- Test: `src/api/auth/botAuth.test.js`

**Interfaces:**
- Consumes: `PORTAL_URL` from `@/lib/runtimeConfig`; `AsyncJobError`, `ASYNC_JOB_ERROR_KINDS` from `@/api`.
- Produces:
  - `botLogin({ username, password }) → Promise<bundle>` where `bundle = { userId, authToken, account, siteId, authServiceUrl, baseUrl, natsUrl, requirePasswordChange }`.
  - `changePassword({ baseUrl, authToken, oldPassword, newPassword }) → Promise<void>`.

- [ ] **Step 1: Write the failing test**

Create `src/api/auth/botAuth.test.js`:

```jsx
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'

vi.mock('@/lib/runtimeConfig', () => ({ PORTAL_URL: 'http://portal.test' }))

import { botLogin, changePassword } from './botAuth'
import { AsyncJobError } from '@/api'

const BUNDLE = {
  userId: 'u17', authToken: 'tok43', account: 'p_admin', siteId: 'site-a',
  authServiceUrl: 'http://auth.site-a', baseUrl: 'http://site-a', natsUrl: 'ws://nats.site-a',
  requirePasswordChange: true,
}

beforeEach(() => { global.fetch = vi.fn() })
afterEach(() => { vi.restoreAllMocks() })

describe('botLogin', () => {
  it('POSTs username/password to portal /v1/login and returns the bundle', async () => {
    global.fetch.mockResolvedValue({ ok: true, json: async () => BUNDLE })
    const out = await botLogin({ username: 'p_admin', password: 'pw' })
    expect(out).toEqual(BUNDLE)
    expect(global.fetch).toHaveBeenCalledWith('http://portal.test/v1/login', expect.objectContaining({
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username: 'p_admin', password: 'pw' }),
    }))
  })

  it('throws AsyncJobError carrying code/reason on a 401', async () => {
    global.fetch.mockResolvedValue({
      ok: false, status: 401,
      json: async () => ({ code: 'unauthenticated', reason: 'invalid_credentials', error: 'nope' }),
    })
    const err = await botLogin({ username: 'x', password: 'y' }).catch((e) => e)
    expect(err).toBeInstanceOf(AsyncJobError)
    expect(err.reason).toBe('invalid_credentials')
    expect(err.message).toBe('nope')
  })

  it('falls back to a status message when the error body is not JSON', async () => {
    global.fetch.mockResolvedValue({ ok: false, status: 503, json: async () => { throw new Error('not json') } })
    const err = await botLogin({ username: 'x', password: 'y' }).catch((e) => e)
    expect(err).toBeInstanceOf(AsyncJobError)
    expect(err.message).toMatch(/503/)
  })
})

describe('changePassword', () => {
  it('POSTs to ${baseUrl}/v1/password/change with Bearer auth and the body', async () => {
    global.fetch.mockResolvedValue({ ok: true, json: async () => ({ status: 'success' }) })
    await changePassword({ baseUrl: 'http://site-a', authToken: 'tok43', oldPassword: 'o', newPassword: 'n' })
    expect(global.fetch).toHaveBeenCalledWith('http://site-a/v1/password/change', expect.objectContaining({
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: 'Bearer tok43' },
      body: JSON.stringify({ oldPassword: 'o', newPassword: 'n' }),
    }))
  })

  it('throws AsyncJobError on invalid_credentials (wrong old password)', async () => {
    global.fetch.mockResolvedValue({
      ok: false, status: 401,
      json: async () => ({ code: 'unauthenticated', reason: 'invalid_credentials', error: 'bad old pw' }),
    })
    const err = await changePassword({ baseUrl: 'http://site-a', authToken: 't', oldPassword: 'o', newPassword: 'n' }).catch((e) => e)
    expect(err.reason).toBe('invalid_credentials')
  })
})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- src/api/auth/botAuth.test.js`
Expected: FAIL — `botAuth` module not found / `botLogin is not a function`.

- [ ] **Step 3: Write minimal implementation**

Create `src/api/auth/botAuth.js`:

```js
import { PORTAL_URL } from '@/lib/runtimeConfig'
import { AsyncJobError, ASYNC_JOB_ERROR_KINDS } from '@/api'

// Both portal and botplatform emit the errcode envelope {code, reason?, error, metadata?}.
// Parse it into an AsyncJobError so callers branch on reason and display .message.
async function throwHttpEnvelopeError(resp, fallbackMsg) {
  const body = await resp.json().catch(() => ({}))
  throw new AsyncJobError(
    body.error || `${fallbackMsg}: ${resp.status}`,
    ASYNC_JOB_ERROR_KINDS.SyncError,
    { code: body.code, reason: body.reason, metadata: body.metadata },
  )
}

/**
 * Bot/admin password login via portal-service. Returns the merged 8-field
 * bundle (session token + home-site URL bundle) so the caller needs no
 * separate /api/userInfo discovery call.
 *
 * @param {{username: string, password: string}} args
 * @returns {Promise<{userId, authToken, account, siteId, authServiceUrl, baseUrl, natsUrl, requirePasswordChange}>}
 * @throws {AsyncJobError} on a non-2xx response.
 */
export async function botLogin({ username, password }) {
  const resp = await fetch(`${PORTAL_URL}/v1/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username, password }),
  })
  if (!resp.ok) await throwHttpEnvelopeError(resp, 'Login failed')
  return resp.json()
}

/**
 * Authenticated password rotation against the home-site botplatform. The
 * caller's session stays valid; the server revokes all OTHER sessions.
 *
 * @param {{baseUrl: string, authToken: string, oldPassword: string, newPassword: string}} args
 * @returns {Promise<void>}
 * @throws {AsyncJobError} on a non-2xx response.
 */
export async function changePassword({ baseUrl, authToken, oldPassword, newPassword }) {
  const resp = await fetch(`${baseUrl}/v1/password/change`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${authToken}` },
    body: JSON.stringify({ oldPassword, newPassword }),
  })
  if (!resp.ok) await throwHttpEnvelopeError(resp, 'Password change failed')
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- src/api/auth/botAuth.test.js`
Expected: PASS (all 5 tests).

- [ ] **Step 5: Commit**

```bash
git add src/api/auth/botAuth.js src/api/auth/botAuth.test.js
git commit -m "feat(chat-frontend): add botAuth API (botLogin, changePassword)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: `useJwtRefresh` — session re-mint branch

**Files:**
- Modify: `src/context/NatsContext/useJwtRefresh.js`
- Test: `src/context/NatsContext/useJwtRefresh.test.js`

**Interfaces:**
- Consumes: existing `getAuthUrl`, `ncRef`.
- Produces (new/changed):
  - `useJwtRefresh({ getAuthUrl, ncRef, onSessionLost })` — `onSessionLost` optional callback (default noop), invoked instead of the IdP redirect when a **session**-mode token fails terminally.
  - `setCredentials({ jwt, seed, natsPublicKey, refreshable, mode, authToken })` — `mode` defaults to `'sso'`; `authToken` stored for session re-mint.

- [ ] **Step 1: Write the failing test**

Append to `src/context/NatsContext/useJwtRefresh.test.js` (inside the top-level `describe('useJwtRefresh', ...)`). Reuse the file's existing `setup`, `makeJwt`, and timer helpers:

```jsx
  it('session mode re-mints with {authToken} (no SSO renew) and reconnects', async () => {
    const reconnect = vi.fn().mockResolvedValue(undefined)
    global.fetch.mockResolvedValue({ ok: true, json: async () => ({ natsJwt: makeJwt(100) }) })
    const { result } = setup({ ncRef: { current: { reconnect } }, getAuthUrl: () => 'http://auth.site-a' })
    act(() => {
      result.current.setCredentials({
        jwt: makeJwt(100), seed: new Uint8Array([3]), natsPublicKey: 'UPUB',
        refreshable: true, mode: 'session', authToken: 'tok43',
      })
    })
    await act(async () => { await vi.advanceTimersByTimeAsync(100 * 1000) })

    expect(renewSsoToken).not.toHaveBeenCalled()
    const [url, opts] = global.fetch.mock.calls.at(-1)
    expect(url).toBe('http://auth.site-a/auth')
    expect(JSON.parse(opts.body)).toEqual({ authToken: 'tok43', natsPublicKey: 'UPUB' })
  })

  it('session mode calls onSessionLost (not the IdP redirect) on a terminal 4xx', async () => {
    const onSessionLost = vi.fn()
    global.fetch.mockResolvedValue({ ok: false, status: 401, json: async () => ({}) })
    const { result } = setup({ ncRef: { current: null }, getAuthUrl: () => 'http://auth.site-a', onSessionLost })
    act(() => {
      result.current.setCredentials({
        jwt: makeJwt(100), seed: new Uint8Array(), natsPublicKey: 'UPUB',
        refreshable: true, mode: 'session', authToken: 'tok43',
      })
    })
    await act(async () => { await vi.advanceTimersByTimeAsync(100 * 1000) })

    expect(onSessionLost).toHaveBeenCalledTimes(1)
    expect(redirectToReloginOnTokenInvalid).not.toHaveBeenCalled()
  })
```

If the file's `setup` helper does not forward `onSessionLost`/`getAuthUrl`, update it. The current helper (top of the test file) looks like `function setup(overrides) { return renderHook(() => useJwtRefresh({ getAuthUrl: () => null, ncRef: { current: null }, ...overrides })) }` — ensure `...overrides` is spread so `onSessionLost` and `getAuthUrl` pass through. Read the helper and adjust only if needed.

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- src/context/NatsContext/useJwtRefresh.test.js`
Expected: FAIL — session branch hits SSO renew or the wrong body; `onSessionLost` undefined.

- [ ] **Step 3: Write minimal implementation**

In `src/context/NatsContext/useJwtRefresh.js`:

1. Add refs near the other refs:
```js
  const modeRef = useRef('sso')
  const authTokenRef = useRef(null)
```

2. Accept `onSessionLost` in the hook signature and keep it in a ref so the timer closure reads the latest:
```js
export function useJwtRefresh({ getAuthUrl, ncRef, onSessionLost }) {
```
add with the other refs:
```js
  const onSessionLostRef = useRef(onSessionLost)
  useEffect(() => { onSessionLostRef.current = onSessionLost }, [onSessionLost])
```

3. In `redirect()`, branch by mode so session tokens don't bounce to Keycloak:
```js
  const redirect = useCallback(async (reason, detail) => {
    console.warn(`NATS JWT refresh: ${reason}; redirecting to login`, detail)
    clearTimer()
    if (modeRef.current === 'session') {
      onSessionLostRef.current?.()
      return
    }
    await redirectToReloginOnTokenInvalid()
  }, [clearTimer])
```

4. In `refresh()`, skip SSO renewal for session mode and pick the body field. Replace the SSO-renew block + body construction:
```js
  const refresh = useCallback(async (attempt) => {
    const myGen = genRef.current
    const stale = () => genRef.current !== myGen

    // 1) For SSO, renew the access token first (terminal on failure). Session
    //    tokens are permanent — re-mint directly with the stored authToken.
    let authBody
    if (modeRef.current === 'session') {
      authBody = { authToken: authTokenRef.current, natsPublicKey: pubKeyRef.current }
    } else {
      let ssoToken
      try {
        ssoToken = await renewSsoToken()
      } catch (err) {
        if (!stale()) await redirect('silent renew failed', { error: err?.message })
        return
      }
      if (stale()) return
      authBody = { ssoToken, natsPublicKey: pubKeyRef.current }
    }

    // 2) Re-mint the NATS JWT. (unchanged below, but use authBody)
    try {
      const resp = await fetch(`${getAuthUrl()}/auth`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(authBody),
      })
      // ... rest of the existing try/catch body unchanged ...
```
Keep the remainder of the `try { … } catch` exactly as it is today (status handling, `jwtRef.current = natsJwt`, reconnect, `scheduleRefresh`, retry/backoff).

5. In `setCredentials`, store mode/authToken:
```js
  const setCredentials = useCallback(({ jwt, seed, natsPublicKey, refreshable, mode = 'sso', authToken = null }) => {
    genRef.current += 1
    jwtRef.current = jwt
    seedRef.current = seed
    pubKeyRef.current = natsPublicKey
    modeRef.current = mode
    authTokenRef.current = authToken
    if (refreshable) scheduleRefresh(jwt)
    else clearTimer()
  }, [scheduleRefresh, clearTimer])
```

6. In `stop()`, also clear them:
```js
    modeRef.current = 'sso'
    authTokenRef.current = null
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- src/context/NatsContext/useJwtRefresh.test.js`
Expected: PASS — both new tests plus all existing SSO tests still green (mode defaults to `'sso'`).

- [ ] **Step 5: Commit**

```bash
git add src/context/NatsContext/useJwtRefresh.js src/context/NatsContext/useJwtRefresh.test.js
git commit -m "feat(chat-frontend): session-token re-mint branch in useJwtRefresh

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: `NatsContext` — session connect, persistence, auto-reconnect

**Files:**
- Modify: `src/context/NatsContext/NatsContext.jsx`
- Test: `src/context/NatsContext/NatsContext.test.jsx`

**Interfaces:**
- Consumes: `setCredentials` (now accepts `mode`/`authToken`), `botLogin` bundle shape.
- Produces:
  - `connect({ mode: 'session', bundle })` — `bundle = { account, siteId, authServiceUrl, baseUrl, natsUrl, authToken }`. Skips `/api/userInfo`; mints with `{authToken, natsPublicKey}`; persists bundle to `sessionStorage['chat.botSession']`.
  - On `NatsProvider` mount, auto-reconnects from a stored bundle.
  - `disconnect()` clears `sessionStorage['chat.botSession']`.

- [ ] **Step 1: Write the failing test**

Append a new `describe` block to `src/context/NatsContext/NatsContext.test.jsx` (reuse the file's `wrapper`, `natsConnect`, `setCredentials` mocks):

```jsx
describe('NatsProvider session (bot/admin) connect', () => {
  const BUNDLE = {
    account: 'p_admin', siteId: 'site-a', authServiceUrl: 'http://auth.site-a',
    baseUrl: 'http://site-a', natsUrl: 'ws://nats.site-a', authToken: 'tok43',
  }
  beforeEach(() => {
    setCredentials.mockReset()
    stop.mockReset()
    natsConnect.mockReset().mockResolvedValue({ closed: () => new Promise(() => {}) })
    window.sessionStorage.clear()
    global.fetch = vi.fn(async () => ({ ok: true, json: async () => ({ natsJwt: 'JWT9', user: { account: 'p_admin' } }) }))
  })
  afterEach(() => { window.sessionStorage.clear(); vi.restoreAllMocks() })

  it('skips /api/userInfo, mints with authToken, persists the bundle', async () => {
    const { result } = renderHook(() => useNats(), { wrapper })
    await act(async () => { await result.current.connect({ mode: 'session', bundle: BUNDLE }) })

    const urls = global.fetch.mock.calls.map((c) => String(c[0]))
    expect(urls.some((u) => u.includes('/api/userInfo'))).toBe(false)
    expect(global.fetch).toHaveBeenCalledWith('http://auth.site-a/auth', expect.anything())
    const authBody = JSON.parse(global.fetch.mock.calls.at(-1)[1].body)
    expect(authBody).toEqual({ authToken: 'tok43', natsPublicKey: 'UPUBKEY' })
    expect(setCredentials).toHaveBeenCalledWith(expect.objectContaining({ mode: 'session', authToken: 'tok43', refreshable: true }))
    await waitFor(() => expect(result.current.connected).toBe(true))
    expect(result.current.user.siteId).toBe('site-a')
    expect(JSON.parse(window.sessionStorage.getItem('chat.botSession'))).toEqual(BUNDLE)
  })

  it('disconnect() clears the stored bot session', async () => {
    const { result } = renderHook(() => useNats(), { wrapper })
    await act(async () => { await result.current.connect({ mode: 'session', bundle: BUNDLE }) })
    expect(window.sessionStorage.getItem('chat.botSession')).not.toBeNull()
    await act(async () => { await result.current.disconnect() })
    expect(window.sessionStorage.getItem('chat.botSession')).toBeNull()
  })

  it('auto-reconnects on mount from a stored bot session', async () => {
    window.sessionStorage.setItem('chat.botSession', JSON.stringify(BUNDLE))
    const { result } = renderHook(() => useNats(), { wrapper })
    await waitFor(() => expect(result.current.connected).toBe(true))
    expect(global.fetch).toHaveBeenCalledWith('http://auth.site-a/auth', expect.anything())
  })

  it('clears a stored bot session and stays logged out when auto-reconnect fails', async () => {
    window.sessionStorage.setItem('chat.botSession', JSON.stringify(BUNDLE))
    natsConnect.mockRejectedValue(new Error('dial fail'))
    const { result } = renderHook(() => useNats(), { wrapper })
    await waitFor(() => expect(window.sessionStorage.getItem('chat.botSession')).toBeNull())
    expect(result.current.connected).toBe(false)
  })
})
```

Note: the existing `nkeys.js` mock returns `getPublicKey: () => 'UPUBKEY'`.

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- src/context/NatsContext/NatsContext.test.jsx`
Expected: FAIL — `mode: 'session'` is unhandled; no persistence; no auto-reconnect.

- [ ] **Step 3: Write minimal implementation**

In `src/context/NatsContext/NatsContext.jsx`:

1. Add a storage key + helpers near the top (after `const sc = StringCodec()`):
```js
const BOT_SESSION_KEY = 'chat.botSession'

function readStoredBotSession() {
  try {
    const raw = window.sessionStorage.getItem(BOT_SESSION_KEY)
    return raw ? JSON.parse(raw) : null
  } catch {
    return null
  }
}
function clearStoredBotSession() {
  try { window.sessionStorage.removeItem(BOT_SESSION_KEY) } catch { /* storage unavailable */ }
}
```

2. Define `onSessionLost` and pass it to `useJwtRefresh`. Replace the `useJwtRefresh({ getAuthUrl, ncRef })` call:
```js
  const onSessionLost = useCallback(() => {
    clearStoredBotSession()
    connectGenRef.current += 1
    if (ncRef.current) { ncRef.current.drain().catch(() => {}); ncRef.current = null }
    setConnected(false)
    setUser(null)
  }, [])
  const { authenticator, setCredentials, stop } = useJwtRefresh({ getAuthUrl, ncRef, onSessionLost })
```

3. In `connectToNats`, branch discovery and the auth body on the session mode. Replace the destructure + discovery + body section:
```js
    const { mode, account, ssoToken, bundle } = opts || {}

    // 1) Site discovery. The session (bot/admin) path already holds the home-site
    // bundle from portal /v1/login, so it skips the /api/userInfo lookup.
    let portal
    if (mode === 'session') {
      portal = bundle
    } else {
      const lookupResp = await fetch(`${PORTAL_URL}/api/userInfo?account=${encodeURIComponent(account ?? '')}`)
      if (!lookupResp.ok) {
        await throwEnvelopeError(lookupResp, 'Portal lookup failed')
      }
      portal = await lookupResp.json()
    }
    const nextAuthUrl = portal.authServiceUrl

    // 2) Mint the NATS JWT at the resolved site's auth-service.
    const nkey = createUser()
    const natsPublicKey = nkey.getPublicKey()

    const body =
      mode === 'sso'
        ? { ssoToken, natsPublicKey }
        : mode === 'session'
          ? { authToken: bundle.authToken, natsPublicKey }
          : { account, natsPublicKey }
```

4. In the success branch (after `setUser(...)`/`setConnected(true)`), set `refreshable` and pass mode/authToken to `setCredentials`. Replace the existing `setCredentials({...})` call:
```js
      setCredentials({
        jwt: natsJwt,
        seed: nkey.getSeed(),
        natsPublicKey,
        refreshable: mode === 'sso' || mode === 'session',
        ...(mode === 'session' ? { mode: 'session', authToken: bundle.authToken } : {}),
      })
```
(For `dev`/`sso` the object is unchanged — existing exact-match assertions stay valid.)

5. Persist the bundle after a successful session connect. Right after `setConnected(true)` (inside the success path, before wiring `nc.closed()`):
```js
      if (mode === 'session') {
        try { window.sessionStorage.setItem(BOT_SESSION_KEY, JSON.stringify(bundle)) } catch { /* storage unavailable */ }
      }
```

6. In `disconnect`, clear the stored session. Add `clearStoredBotSession()` as the first line of the `disconnect` callback body.

7. Auto-reconnect on mount. Add an effect after `connectToNats` is defined:
```js
  // On mount, resume a persisted bot/admin session (sessionStorage survives a
  // tab reload). Best-effort: a failed resume clears the stash and falls back
  // to the login form. Runs once; the ref survives a StrictMode double-invoke.
  const didResumeRef = useRef(false)
  useEffect(() => {
    if (didResumeRef.current) return
    didResumeRef.current = true
    const stored = readStoredBotSession()
    if (!stored) return
    connectToNats({ mode: 'session', bundle: stored }).catch(() => {
      clearStoredBotSession()
    })
  }, [connectToNats])
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- src/context/NatsContext/NatsContext.test.jsx`
Expected: PASS — new session block plus all existing connect/dev/sso tests still green.

- [ ] **Step 5: Commit**

```bash
git add src/context/NatsContext/NatsContext.jsx src/context/NatsContext/NatsContext.test.jsx
git commit -m "feat(chat-frontend): session connect + sessionStorage resume in NatsContext

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 4: `ChangePasswordForm` component

**Files:**
- Create: `src/pages/ChangePasswordPage/ChangePasswordForm.jsx`
- Create: `src/pages/ChangePasswordPage/index.jsx`
- Create: `src/pages/ChangePasswordPage/style.css`
- Test: `src/pages/ChangePasswordPage/ChangePasswordForm.test.jsx`

**Interfaces:**
- Produces: `ChangePasswordForm({ onSubmit, error, loading })` — controlled form with `oldPassword`, `newPassword`, `confirmPassword`. Calls `onSubmit({ oldPassword, newPassword })` only when client validation passes (all non-empty, `newPassword === confirmPassword`, `newPassword !== oldPassword`). Surfaces a local validation message otherwise, and renders the `error` prop (server error) when present.

- [ ] **Step 1: Write the failing test**

Create `src/pages/ChangePasswordPage/ChangePasswordForm.test.jsx`:

```jsx
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import ChangePasswordForm from './ChangePasswordForm'

function fill(label, value) {
  fireEvent.change(screen.getByLabelText(label), { target: { value } })
}

beforeEach(() => { vi.clearAllMocks() })

describe('ChangePasswordForm', () => {
  it('submits {oldPassword, newPassword} when valid', () => {
    const onSubmit = vi.fn()
    render(<ChangePasswordForm onSubmit={onSubmit} />)
    fill(/current password/i, 'old1')
    fill(/^new password/i, 'new2')
    fill(/confirm/i, 'new2')
    fireEvent.click(screen.getByRole('button', { name: /change password/i }))
    expect(onSubmit).toHaveBeenCalledWith({ oldPassword: 'old1', newPassword: 'new2' })
  })

  it('blocks submit and shows a message when new != confirm', () => {
    const onSubmit = vi.fn()
    render(<ChangePasswordForm onSubmit={onSubmit} />)
    fill(/current password/i, 'old1')
    fill(/^new password/i, 'new2')
    fill(/confirm/i, 'nope')
    fireEvent.click(screen.getByRole('button', { name: /change password/i }))
    expect(onSubmit).not.toHaveBeenCalled()
    expect(screen.getByText(/do not match/i)).toBeInTheDocument()
  })

  it('blocks submit when the new password equals the old', () => {
    const onSubmit = vi.fn()
    render(<ChangePasswordForm onSubmit={onSubmit} />)
    fill(/current password/i, 'same')
    fill(/^new password/i, 'same')
    fill(/confirm/i, 'same')
    fireEvent.click(screen.getByRole('button', { name: /change password/i }))
    expect(onSubmit).not.toHaveBeenCalled()
    expect(screen.getByText(/must differ/i)).toBeInTheDocument()
  })

  it('renders a server error passed via the error prop', () => {
    render(<ChangePasswordForm onSubmit={vi.fn()} error="bad old pw" />)
    expect(screen.getByText('bad old pw')).toBeInTheDocument()
  })

  it('disables the button while loading', () => {
    render(<ChangePasswordForm onSubmit={vi.fn()} loading />)
    expect(screen.getByRole('button')).toBeDisabled()
  })
})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- src/pages/ChangePasswordPage/ChangePasswordForm.test.jsx`
Expected: FAIL — module not found.

- [ ] **Step 3: Write minimal implementation**

Create `src/pages/ChangePasswordPage/ChangePasswordForm.jsx`:

```jsx
import { useState } from 'react'
import './style.css'

export default function ChangePasswordForm({ onSubmit, error, loading }) {
  const [oldPassword, setOld] = useState('')
  const [newPassword, setNew] = useState('')
  const [confirmPassword, setConfirm] = useState('')
  const [localError, setLocalError] = useState(null)

  const handleSubmit = (e) => {
    e.preventDefault()
    if (!oldPassword || !newPassword || !confirmPassword) {
      setLocalError('All fields are required')
      return
    }
    if (newPassword !== confirmPassword) {
      setLocalError('New passwords do not match')
      return
    }
    if (newPassword === oldPassword) {
      setLocalError('New password must differ from the current password')
      return
    }
    setLocalError(null)
    onSubmit({ oldPassword, newPassword })
  }

  return (
    <div className="login-page">
      <form className="login-form" onSubmit={handleSubmit}>
        <h1>Change Password</h1>
        <p className="login-subtitle">Set a new password to continue</p>

        <label htmlFor="old-password">Current password</label>
        <input id="old-password" type="password" value={oldPassword}
          onChange={(e) => setOld(e.target.value)} autoFocus disabled={loading} />

        <label htmlFor="new-password">New password</label>
        <input id="new-password" type="password" value={newPassword}
          onChange={(e) => setNew(e.target.value)} disabled={loading} />

        <label htmlFor="confirm-password">Confirm new password</label>
        <input id="confirm-password" type="password" value={confirmPassword}
          onChange={(e) => setConfirm(e.target.value)} disabled={loading} />

        <button type="submit" disabled={loading}>
          {loading ? 'Saving…' : 'Change password'}
        </button>

        {(localError || error) && <div className="login-error">{localError || error}</div>}
      </form>
    </div>
  )
}
```

Create `src/pages/ChangePasswordPage/index.jsx`:
```jsx
export { default } from './ChangePasswordForm'
```

Create `src/pages/ChangePasswordPage/style.css`:
```css
/* Change-password reuses the shared .login-page / .login-form styles. */
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- src/pages/ChangePasswordPage/ChangePasswordForm.test.jsx`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add src/pages/ChangePasswordPage
git commit -m "feat(chat-frontend): ChangePasswordForm component

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 5: `BotLoginPage` — login + change-pwd gate + connect

**Files:**
- Create: `src/pages/BotLoginPage/BotLoginPage.jsx`
- Create: `src/pages/BotLoginPage/index.jsx`
- Create: `src/pages/BotLoginPage/style.css`
- Test: `src/pages/BotLoginPage/BotLoginPage.test.jsx`

**Interfaces:**
- Consumes: `useNats().connect`, `botLogin`, `changePassword`, `formatAsyncJobError`, `ChangePasswordForm`.
- Produces: default-exported `BotLoginPage` rendering the username/password step, then (if `requirePasswordChange`) the change-password step, then `connect({ mode: 'session', bundle })`.

- [ ] **Step 1: Write the failing test**

Create `src/pages/BotLoginPage/BotLoginPage.test.jsx`:

```jsx
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'

vi.mock('@/context/NatsContext', () => ({ useNats: vi.fn() }))
vi.mock('@/api/auth/botAuth', () => ({ botLogin: vi.fn(), changePassword: vi.fn() }))

import BotLoginPage from './BotLoginPage'
import { useNats } from '@/context/NatsContext'
import { botLogin, changePassword } from '@/api/auth/botAuth'

const BUNDLE = {
  userId: 'u17', authToken: 'tok43', account: 'p_admin', siteId: 'site-a',
  authServiceUrl: 'http://auth.site-a', baseUrl: 'http://site-a', natsUrl: 'ws://nats.site-a',
  requirePasswordChange: false,
}

beforeEach(() => {
  vi.clearAllMocks()
  useNats.mockReturnValue({ connect: vi.fn().mockResolvedValue(undefined) })
})

function login(user = 'p_admin', pw = 'pw') {
  fireEvent.change(screen.getByLabelText(/username/i), { target: { value: user } })
  fireEvent.change(screen.getByLabelText(/password/i), { target: { value: pw } })
  fireEvent.click(screen.getByRole('button', { name: /sign in/i }))
}

describe('BotLoginPage', () => {
  it('logs in and connects with the session bundle when no password change is required', async () => {
    botLogin.mockResolvedValue(BUNDLE)
    const connect = vi.fn().mockResolvedValue(undefined)
    useNats.mockReturnValue({ connect })
    render(<BotLoginPage />)
    login()
    await waitFor(() => expect(botLogin).toHaveBeenCalledWith({ username: 'p_admin', password: 'pw' }))
    await waitFor(() => expect(connect).toHaveBeenCalledWith({ mode: 'session', bundle: BUNDLE }))
  })

  it('shows the uniform error on invalid credentials and does not connect', async () => {
    const err = Object.assign(new Error('invalid username or password'), { kind: 'sync-error', reason: 'invalid_credentials' })
    botLogin.mockRejectedValue(err)
    const connect = vi.fn()
    useNats.mockReturnValue({ connect })
    render(<BotLoginPage />)
    login('x', 'y')
    await waitFor(() => expect(screen.getByText(/invalid username or password/i)).toBeInTheDocument())
    expect(connect).not.toHaveBeenCalled()
  })

  it('routes to the change-password step when requirePasswordChange is true', async () => {
    botLogin.mockResolvedValue({ ...BUNDLE, requirePasswordChange: true })
    render(<BotLoginPage />)
    login()
    await waitFor(() => expect(screen.getByRole('button', { name: /change password/i })).toBeInTheDocument())
  })

  it('changes the password then connects, carrying the same authToken', async () => {
    botLogin.mockResolvedValue({ ...BUNDLE, requirePasswordChange: true })
    changePassword.mockResolvedValue(undefined)
    const connect = vi.fn().mockResolvedValue(undefined)
    useNats.mockReturnValue({ connect })
    render(<BotLoginPage />)
    login()
    await waitFor(() => screen.getByLabelText(/current password/i))

    fireEvent.change(screen.getByLabelText(/current password/i), { target: { value: 'pw' } })
    fireEvent.change(screen.getByLabelText(/^new password/i), { target: { value: 'new9' } })
    fireEvent.change(screen.getByLabelText(/confirm/i), { target: { value: 'new9' } })
    fireEvent.click(screen.getByRole('button', { name: /change password/i }))

    await waitFor(() => expect(changePassword).toHaveBeenCalledWith({
      baseUrl: 'http://site-a', authToken: 'tok43', oldPassword: 'pw', newPassword: 'new9',
    }))
    await waitFor(() => expect(connect).toHaveBeenCalledWith({
      mode: 'session', bundle: { ...BUNDLE, requirePasswordChange: true },
    }))
  })
})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- src/pages/BotLoginPage/BotLoginPage.test.jsx`
Expected: FAIL — module not found.

- [ ] **Step 3: Write minimal implementation**

Create `src/pages/BotLoginPage/BotLoginPage.jsx`:

```jsx
import { useState } from 'react'
import { useNats } from '@/context/NatsContext'
import { botLogin, changePassword } from '@/api/auth/botAuth'
import { formatAsyncJobError } from '@/api'
import ChangePasswordForm from '@/pages/ChangePasswordPage'
import './style.css'

export default function BotLoginPage() {
  const { connect } = useNats()
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [bundle, setBundle] = useState(null)   // set once login succeeds
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  const connectSession = async (resolved) => {
    await connect({ mode: 'session', bundle: resolved })
  }

  const handleLogin = async (e) => {
    e.preventDefault()
    if (!username.trim() || !password) return
    setLoading(true)
    setError(null)
    try {
      const resolved = await botLogin({ username: username.trim(), password })
      if (resolved.requirePasswordChange) {
        setBundle(resolved)         // show the change-password step
        return
      }
      await connectSession(resolved)
    } catch (err) {
      setError(formatAsyncJobError(err))
    } finally {
      setLoading(false)
    }
  }

  const handleChangePassword = async ({ oldPassword, newPassword }) => {
    setLoading(true)
    setError(null)
    try {
      await changePassword({ baseUrl: bundle.baseUrl, authToken: bundle.authToken, oldPassword, newPassword })
      await connectSession(bundle)
    } catch (err) {
      setError(formatAsyncJobError(err))
    } finally {
      setLoading(false)
    }
  }

  if (bundle?.requirePasswordChange) {
    return <ChangePasswordForm onSubmit={handleChangePassword} error={error} loading={loading} />
  }

  return (
    <div className="login-page">
      <form className="login-form" onSubmit={handleLogin}>
        <h1>Chat</h1>
        <p className="login-subtitle">Bot / Admin sign in</p>

        <label htmlFor="bot-username">Username</label>
        <input id="bot-username" type="text" value={username}
          onChange={(e) => setUsername(e.target.value)} autoFocus disabled={loading} />

        <label htmlFor="bot-password">Password</label>
        <input id="bot-password" type="password" value={password}
          onChange={(e) => setPassword(e.target.value)} disabled={loading} />

        <button type="submit" disabled={loading || !username.trim() || !password}>
          {loading ? 'Signing in…' : 'Sign in'}
        </button>

        {error && <div className="login-error">{error}</div>}
      </form>
    </div>
  )
}
```

Create `src/pages/BotLoginPage/index.jsx`:
```jsx
export { default } from './BotLoginPage'
```

Create `src/pages/BotLoginPage/style.css`:
```css
/* Bot login reuses the shared .login-page / .login-form styles. */
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- src/pages/BotLoginPage/BotLoginPage.test.jsx`
Expected: PASS (4 tests). Note: `formatAsyncJobError` is not mocked, so the thrown error's `.message` is used directly — the test error message "invalid username or password" is what renders.

- [ ] **Step 5: Commit**

```bash
git add src/pages/BotLoginPage
git commit -m "feat(chat-frontend): BotLoginPage with first-login change-password gate

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 6: `/dev-login` route in `App.jsx`

**Files:**
- Modify: `src/App.jsx`
- Test: `src/App.test.jsx` (create if absent)

**Interfaces:**
- Consumes: `BotLoginPage`, existing `useNats().connected`, `pathname` state.
- Produces: `/dev-login` renders `BotLoginPage` when not connected, regardless of `DEV_MODE`.

- [ ] **Step 1: Write the failing test**

Create `src/App.test.jsx`:

```jsx
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen } from '@testing-library/react'

vi.mock('@/context/NatsContext', async () => {
  const actual = await vi.importActual('@/context/NatsContext')
  return { ...actual, useNats: vi.fn() }
})
vi.mock('@/pages/BotLoginPage', () => ({ default: () => <div>BOT LOGIN PAGE</div> }))
vi.mock('@/pages/LoginPage', () => ({ default: () => <div>SSO LOGIN PAGE</div> }))
vi.mock('@/pages/OidcCallback', () => ({ default: () => <div>OIDC CALLBACK</div> }))
vi.mock('@/components/MainApp/MainApp', () => ({ default: () => <div>MAIN APP</div> }))

import App from './App'
import { useNats } from '@/context/NatsContext'

function setPath(p) {
  window.history.pushState({}, '', p)
}

beforeEach(() => { vi.clearAllMocks(); useNats.mockReturnValue({ connected: false }) })
afterEach(() => { setPath('/') })

describe('App routing', () => {
  it('renders BotLoginPage at /dev-login when not connected', () => {
    setPath('/dev-login')
    render(<App />)
    expect(screen.getByText('BOT LOGIN PAGE')).toBeInTheDocument()
  })

  it('renders the SSO LoginPage at / when not connected', () => {
    setPath('/')
    render(<App />)
    expect(screen.getByText('SSO LOGIN PAGE')).toBeInTheDocument()
  })
})
```

Note: `App` wraps content in `NatsProvider`; the partial mock keeps the real provider but overrides `useNats` so `AppContent` reads `{ connected: false }`. If the real `NatsProvider` import path differs, mock it to a passthrough `({ children }) => children` in the same factory.

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- src/App.test.jsx`
Expected: FAIL — `/dev-login` currently falls through to the SSO LoginPage.

- [ ] **Step 3: Write minimal implementation**

In `src/App.jsx`, add the import and the route branch. After the existing `OidcCallback` import:
```jsx
import BotLoginPage from '@/pages/BotLoginPage'
```
In `AppContent`, immediately after the `/oidc-callback` branch:
```jsx
  if (pathname === '/dev-login') {
    return <BotLoginPage />
  }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- src/App.test.jsx`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add src/App.jsx src/App.test.jsx
git commit -m "feat(chat-frontend): /dev-login route renders BotLoginPage

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 7: Full verification + push

**Files:** none (verification only).

- [ ] **Step 1: Run the full test suite**

Run: `npm test`
Expected: PASS — all suites green, including the pre-existing ones (no regressions in `LoginPage`, `NatsContext`, `useJwtRefresh`).

- [ ] **Step 2: Typecheck and build**

Run: `npm run typecheck && npm run build`
Expected: no type errors; production build succeeds. (If `typecheck` flags pre-existing unrelated issues, note them but do not fix out-of-scope code.)

- [ ] **Step 3: Push the branch**

```bash
git push -u origin claude/chat-frontend-bot-login-rp2-msq8db
```
Retry on network error with backoff (2s, 4s, 8s, 16s).

- [ ] **Step 4: Report**

Summarize: files added/changed, test counts, and the #428 dependency. Do NOT open a PR unless the user asks.

---

## Self-Review

**Spec coverage:**
- §1 routing → Task 6. §2 botAuth → Task 1. §3 session connect → Task 3. §4 persistence/auto-reconnect → Task 3. §5 JWT refresh → Task 2. §6 change-pwd gate + components → Tasks 4 & 5. Testing → every task + Task 7. ✓
- Out-of-scope items (voluntary change-pwd, admin app, backend) correctly excluded. ✓

**Type/name consistency:**
- `bundle` shape `{userId, authToken, account, siteId, authServiceUrl, baseUrl, natsUrl, requirePasswordChange}` consistent across Tasks 1/3/5. ✓
- `connect({ mode: 'session', bundle })` consistent (Tasks 3/5). ✓
- `setCredentials({..., mode, authToken})` defined Task 2, produced Task 3. ✓
- `changePassword({ baseUrl, authToken, oldPassword, newPassword })` consistent (Tasks 1/5). ✓
- `ChangePasswordForm({ onSubmit, error, loading })` defined Task 4, consumed Task 5. ✓
- `BOT_SESSION_KEY = 'chat.botSession'` consistent within Task 3. ✓

**Placeholder scan:** none — every code/test step is complete. ✓
