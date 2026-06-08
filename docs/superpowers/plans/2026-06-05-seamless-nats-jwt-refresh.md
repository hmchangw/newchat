# Seamless NATS-JWT Refresh Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep an authenticated NATS WebSocket session alive across JWT expiry with no visible logout, by re-minting the NATS user JWT in the background (OIDC refresh token → `/auth`) and applying it through a dynamic, ref-backed authenticator with a proactive reconnect; fall back to a graceful login redirect only when the SSO session itself ends.

**Architecture:** Frontend-led. A new `useJwtRefresh` hook owns `jwtRef`/`seedRef`/`pubKeyRef`, builds one dynamic `jwtAuthenticator(() => jwtRef.current, () => seedRef.current)`, schedules a jittered timer at ~80% of the JWT's real `exp`, and on fire runs `signinSilent()` → POST `/auth` (reusing the same nkey) → swaps `jwtRef` → forces a quick reconnect. `NatsContext` wires the hook in. Backend adds lifetime jitter to `signNATSJWT` so a fleet doesn't expire in lockstep. `/auth` request/response schema is unchanged.

**Tech Stack:** Go 1.25 (`nats-io/jwt/v2`, `nats-io/nkeys`, `gin`, `caarlos0/env`, `crypto/rand`), React + `nats.ws@^1.30`, `oidc-client-ts@^3.5`, `nkeys.js@^1.1`, vitest@^2.1 (`@testing-library/react`, fake timers).

**Design source:** `docs/superpowers/specs/2026-06-05-seamless-nats-jwt-refresh-design.md`

**Refinement vs spec:** The spec mentioned `automaticSilentRenew`. We do NOT enable it — the refresh schedule is driven by the *NATS JWT* expiry, so we call `signinSilent()` on demand from our own timer rather than running oidc-client-ts's independent access-token timer. Net effect (refresh-token-based silent renew) is identical; one fewer uncontrolled timer.

---

## File Structure

| File | Create/Modify | Responsibility |
|---|---|---|
| `auth-service/handler.go` | Modify | Add jitter fields + `Option`s to `AuthHandler`; apply jitter in JWT expiry |
| `auth-service/handler_test.go` | Modify | Add deterministic jitter test |
| `auth-service/main.go` | Modify | Add `NATS_JWT_EXPIRY_JITTER` config; pass `WithJitter` |
| `chat-frontend/src/lib/jwtExpiry.js` | Create | Pure `parseNatsJwtExp(jwt)` |
| `chat-frontend/src/lib/jwtExpiry.test.js` | Create | Unit tests for the parser |
| `chat-frontend/src/api/auth/oidcClient.js` | Modify | Add `renewSsoToken()` |
| `chat-frontend/src/api/auth/oidcClient.test.js` | Create | Unit tests for `renewSsoToken` |
| `chat-frontend/src/context/NatsContext/useJwtRefresh.js` | Create | Dynamic authenticator + refresh loop |
| `chat-frontend/src/context/NatsContext/useJwtRefresh.test.js` | Create | Hook tests (fake timers) |
| `chat-frontend/src/context/NatsContext/NatsContext.jsx` | Modify | Consume hook; set creds before connect; stop on disconnect |
| `chat-frontend/src/context/NatsContext/NatsContext.test.jsx` | Create | Provider wiring tests |
| `docs/client-api.md` | Modify | Note that `/auth` is also the renewal endpoint |

---

## Task 1: Backend JWT-lifetime jitter

**Files:**
- Modify: `auth-service/handler.go`
- Modify: `auth-service/main.go`
- Test: `auth-service/handler_test.go`

- [ ] **Step 1: Write the failing test**

Add to `auth-service/handler_test.go`:

```go
func TestSignNATSJWT_LifetimeJitter(t *testing.T) {
	signingKP := mustAccountKP(t)
	validator := &fakeValidator{account: "alice", subject: "uuid-alice"}
	base := 100 * time.Minute

	tests := []struct {
		name      string
		rnd       float64
		wantRatio float64 // expected multiple of base
	}{
		{"low end", 0.0, 0.9},
		{"midpoint", 0.5, 1.0},
		{"high end", 1.0, 1.1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := NewAuthHandler(validator, signingKP, base, false,
				WithJitter(0.1), WithRandFloat(func() float64 { return tt.rnd }))
			router := setupRouter(t, handler)

			userPub := mustUserNKey(t)
			body := `{"ssoToken":"valid","natsPublicKey":"` + userPub + `"}`
			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/auth", strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")

			before := time.Now()
			router.ServeHTTP(w, req)
			require.Equal(t, http.StatusOK, w.Code)

			var resp authResponse
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
			claims, err := jwt.DecodeUserClaims(resp.NATSJWT)
			require.NoError(t, err)

			wantLifeSec := (time.Duration(float64(base) * tt.wantRatio)).Seconds()
			gotLifeSec := time.Unix(claims.Expires, 0).Sub(before).Seconds()
			assert.InDelta(t, wantLifeSec, gotLifeSec, 5) // 5s slack for exec time
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test SERVICE=auth-service`
Expected: compile failure — `undefined: WithJitter`, `undefined: WithRandFloat`.

- [ ] **Step 3: Implement jitter in `handler.go`**

Add imports `crypto/rand` and `math/big` to the import block. Replace the `AuthHandler` struct, `NewAuthHandler`, and the `uc.Expires` line, and add the helpers:

```go
// AuthHandler processes auth requests, validates SSO tokens via OIDC,
// and returns signed NATS user JWTs with scoped permissions.
type AuthHandler struct {
	validator  TokenValidator
	signingKey nkeys.KeyPair
	jwtExpiry  time.Duration
	jwtJitter  float64       // fraction of jwtExpiry; 0 = fixed lifetime
	randFloat  func() float64 // injectable [0,1) source; defaults to crypto rand
	devMode    bool
}

// Option configures optional AuthHandler behavior.
type Option func(*AuthHandler)

// WithJitter sets the JWT-lifetime jitter fraction (clamped to [0, 0.9]) so a
// fleet of sessions minted together does not expire in lockstep.
func WithJitter(frac float64) Option {
	return func(h *AuthHandler) {
		if frac < 0 {
			frac = 0
		}
		if frac > 0.9 {
			frac = 0.9
		}
		h.jwtJitter = frac
	}
}

// WithRandFloat overrides the randomness source (test seam).
func WithRandFloat(fn func() float64) Option {
	return func(h *AuthHandler) { h.randFloat = fn }
}

// NewAuthHandler creates an AuthHandler with the given token validator,
// NATS account signing key, and JWT expiry duration.
func NewAuthHandler(validator TokenValidator, signingKey nkeys.KeyPair, jwtExpiry time.Duration, devMode bool, opts ...Option) *AuthHandler {
	h := &AuthHandler{
		validator:  validator,
		signingKey: signingKey,
		jwtExpiry:  jwtExpiry,
		randFloat:  cryptoRandFloat,
		devMode:    devMode,
	}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

// cryptoRandFloat returns a uniform float in [0,1) from crypto/rand. On the
// (practically impossible) read error it returns 0.5 — the no-skew midpoint.
func cryptoRandFloat() float64 {
	const denom = 1 << 53
	n, err := rand.Int(rand.Reader, big.NewInt(denom))
	if err != nil {
		return 0.5
	}
	return float64(n.Int64()) / float64(denom)
}
```

Then change `signNATSJWT` to use a jittered expiry. Replace its `uc.Expires` line:

```go
	uc.Expires = h.jwtExpiryAt().Unix()
```

and add the helper right below `signNATSJWT`:

```go
// jwtExpiryAt returns the absolute expiry, applying ±jwtJitter around the base
// lifetime: factor = 1 + jitter*(2r-1), r in [0,1).
func (h *AuthHandler) jwtExpiryAt() time.Time {
	factor := 1 + h.jwtJitter*(2*h.randFloat()-1)
	return time.Now().Add(time.Duration(float64(h.jwtExpiry) * factor))
}
```

- [ ] **Step 4: Wire config in `main.go`**

Add to the `config` struct (after `NATSJWTExpiry`):

```go
	NATSJWTExpiryJitter float64       `env:"NATS_JWT_EXPIRY_JITTER" envDefault:"0.1"`
```

Pass `WithJitter` at both `NewAuthHandler` call sites:

```go
		handler = NewAuthHandler(nil, signingKP, cfg.NATSJWTExpiry, true, WithJitter(cfg.NATSJWTExpiryJitter))
```

```go
		handler = NewAuthHandler(oidcValidator, signingKP, cfg.NATSJWTExpiry, false, WithJitter(cfg.NATSJWTExpiryJitter))
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `make test SERVICE=auth-service`
Expected: PASS, including the existing `TestHandleAuth_ValidToken` (default jitter is 0 at those call sites, so `exp ≈ now+2h`, still `≤ 2h+1min`) and the new `TestSignNATSJWT_LifetimeJitter`.

- [ ] **Step 6: Lint + SAST**

Run: `make lint && make sast-gosec`
Expected: clean. (We use `crypto/rand`, so no G404 weak-random finding.)

- [ ] **Step 7: Commit**

```bash
git add auth-service/handler.go auth-service/handler_test.go auth-service/main.go
git commit -m "feat(auth-service): jitter NATS JWT lifetime to de-sync fleet expiry

Apply +/- NATS_JWT_EXPIRY_JITTER (default 10%) around the base lifetime so
sessions minted together don't expire in lockstep and reconnect-storm the
service. Randomness source is injectable for deterministic tests; defaults
to crypto/rand."
```

---

## Task 2: `parseNatsJwtExp` pure helper

**Files:**
- Create: `chat-frontend/src/lib/jwtExpiry.js`
- Test: `chat-frontend/src/lib/jwtExpiry.test.js`

- [ ] **Step 1: Write the failing test**

Create `chat-frontend/src/lib/jwtExpiry.test.js`:

```js
import { describe, it, expect } from 'vitest'
import { parseNatsJwtExp } from './jwtExpiry'

function makeJwt(payload) {
  const b64 = (obj) =>
    btoa(JSON.stringify(obj)).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
  return `${b64({ alg: 'ed25519' })}.${b64(payload)}.sig`
}

describe('parseNatsJwtExp', () => {
  it('returns the exp claim in unix seconds', () => {
    expect(parseNatsJwtExp(makeJwt({ exp: 1700000000 }))).toBe(1700000000)
  })
  it('returns null when exp is missing', () => {
    expect(parseNatsJwtExp(makeJwt({ sub: 'x' }))).toBeNull()
  })
  it('returns null when exp is not a number', () => {
    expect(parseNatsJwtExp(makeJwt({ exp: 'soon' }))).toBeNull()
  })
  it('returns null for a malformed token', () => {
    expect(parseNatsJwtExp('not-a-jwt')).toBeNull()
  })
  it('returns null for non-string input', () => {
    expect(parseNatsJwtExp(null)).toBeNull()
  })
})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd chat-frontend && npx vitest run src/lib/jwtExpiry.test.js`
Expected: FAIL — cannot resolve `./jwtExpiry`.

- [ ] **Step 3: Implement the parser**

Create `chat-frontend/src/lib/jwtExpiry.js`:

```js
// Decode the `exp` (unix seconds) claim from a NATS user JWT WITHOUT verifying
// the signature. Used only to schedule client-side token refresh; never for
// any trust/authorization decision. Returns null on any malformed input.
export function parseNatsJwtExp(jwt) {
  const parts = String(jwt).split('.')
  if (parts.length < 2) return null
  try {
    const b64 = parts[1].replace(/-/g, '+').replace(/_/g, '/')
    const payload = JSON.parse(atob(b64))
    const exp = payload?.exp
    return typeof exp === 'number' && Number.isFinite(exp) ? exp : null
  } catch {
    return null
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd chat-frontend && npx vitest run src/lib/jwtExpiry.test.js`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add chat-frontend/src/lib/jwtExpiry.js chat-frontend/src/lib/jwtExpiry.test.js
git commit -m "feat(frontend): parseNatsJwtExp to read a NATS JWT exp for scheduling

Pure, signature-free decode of the exp claim so the refresh loop schedules
off the token's real lifetime (robust to backend lifetime jitter) instead of
a hardcoded duration."
```

---

## Task 3: `renewSsoToken` via silent renew

**Files:**
- Modify: `chat-frontend/src/api/auth/oidcClient.js`
- Test: `chat-frontend/src/api/auth/oidcClient.test.js`

- [ ] **Step 1: Write the failing test**

Create `chat-frontend/src/api/auth/oidcClient.test.js`:

```js
import { describe, it, expect, vi, beforeEach } from 'vitest'

const signinSilent = vi.fn()
vi.mock('oidc-client-ts', () => ({
  UserManager: vi.fn(() => ({ signinSilent, removeUser: vi.fn(), signinRedirect: vi.fn() })),
  WebStorageStateStore: vi.fn(),
}))

import { renewSsoToken, _resetOidcManagerForTests } from './oidcClient'

describe('renewSsoToken', () => {
  beforeEach(() => {
    _resetOidcManagerForTests()
    signinSilent.mockReset()
  })

  it('returns the fresh access token from signinSilent', async () => {
    signinSilent.mockResolvedValue({ access_token: 'fresh-token' })
    await expect(renewSsoToken()).resolves.toBe('fresh-token')
  })

  it('throws when silent renew yields no token', async () => {
    signinSilent.mockResolvedValue(null)
    await expect(renewSsoToken()).rejects.toThrow()
  })

  it('propagates a silent-renew failure (SSO session ended)', async () => {
    signinSilent.mockRejectedValue(new Error('login_required'))
    await expect(renewSsoToken()).rejects.toThrow('login_required')
  })
})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd chat-frontend && npx vitest run src/api/auth/oidcClient.test.js`
Expected: FAIL — `renewSsoToken` is not exported.

- [ ] **Step 3: Implement `renewSsoToken`**

Append to `chat-frontend/src/api/auth/oidcClient.js` (after `redirectToReloginOnTokenInvalid`):

```js
/**
 * Obtain a fresh SSO access token in the background using the OIDC refresh
 * token (refresh_token grant via oidc-client-ts signinSilent — no redirect,
 * no iframe). Returns the same token shape login uses (`user.access_token`,
 * see OidcCallback.jsx). Throws when silent renewal is not possible (e.g. the
 * SSO session has ended); callers fall back to redirectToReloginOnTokenInvalid().
 */
export async function renewSsoToken() {
  const mgr = getOidcManager()
  const user = await mgr.signinSilent()
  if (!user || !user.access_token) {
    throw new Error('silent renew returned no access token')
  }
  return user.access_token
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd chat-frontend && npx vitest run src/api/auth/oidcClient.test.js`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add chat-frontend/src/api/auth/oidcClient.js chat-frontend/src/api/auth/oidcClient.test.js
git commit -m "feat(frontend): renewSsoToken for background silent renewal

Wraps userManager.signinSilent (refresh_token grant) to mint a fresh SSO
access token without a redirect, returning user.access_token to match the
login path. Throws when the SSO session has ended so callers can redirect."
```

---

## Task 4: `useJwtRefresh` hook (dynamic authenticator + refresh loop)

**Files:**
- Create: `chat-frontend/src/context/NatsContext/useJwtRefresh.js`
- Test: `chat-frontend/src/context/NatsContext/useJwtRefresh.test.js`

- [ ] **Step 1: Write the failing test**

Create `chat-frontend/src/context/NatsContext/useJwtRefresh.test.js`:

```js
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { renderHook, act } from '@testing-library/react'

vi.mock('nats.ws', () => ({
  jwtAuthenticator: vi.fn((jwtFn, seedFn) => ({ jwtFn, seedFn })),
}))
vi.mock('@/api/auth/oidcClient', () => ({
  renewSsoToken: vi.fn(),
  redirectToReloginOnTokenInvalid: vi.fn(() => Promise.resolve()),
}))

import { jwtAuthenticator } from 'nats.ws'
import { renewSsoToken, redirectToReloginOnTokenInvalid } from '@/api/auth/oidcClient'
import { useJwtRefresh } from './useJwtRefresh'

function makeJwt(expSecFromNow) {
  const b64 = (obj) =>
    btoa(JSON.stringify(obj)).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
  const exp = Math.floor(Date.now() / 1000) + expSecFromNow
  return `${b64({ alg: 'ed25519' })}.${b64({ exp })}.sig`
}

describe('useJwtRefresh', () => {
  beforeEach(() => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
    renewSsoToken.mockReset()
    redirectToReloginOnTokenInvalid.mockReset().mockResolvedValue()
    global.fetch = vi.fn()
  })
  afterEach(() => {
    vi.useRealTimers()
    vi.restoreAllMocks()
  })

  it('builds a dynamic authenticator whose getters read current creds', () => {
    const { result } = renderHook(() =>
      useJwtRefresh({ authUrl: 'http://auth', ncRef: { current: null } }))
    act(() => {
      result.current.setCredentials({
        jwt: 'jwt-A', seed: new Uint8Array([1]), natsPublicKey: 'UPUB', refreshable: false,
      })
    })
    expect(jwtAuthenticator).toHaveBeenCalledTimes(1)
    expect(result.current.authenticator.jwtFn()).toBe('jwt-A')
    expect(result.current.authenticator.seedFn()).toEqual(new Uint8Array([1]))
  })

  it('refreshes at ~80% of life: renews SSO, re-mints with same nkey, reconnects', async () => {
    const reconnect = vi.fn()
    const ncRef = { current: { reconnect } }
    renewSsoToken.mockResolvedValue('fresh-sso')
    global.fetch.mockResolvedValue({ ok: true, json: async () => ({ natsJwt: makeJwt(3600) }) })

    const { result } = renderHook(() => useJwtRefresh({ authUrl: 'http://auth', ncRef }))
    act(() => {
      result.current.setCredentials({
        jwt: makeJwt(100), seed: new Uint8Array([9]), natsPublicKey: 'UPUB', refreshable: true,
      })
    })
    // life=100s, refresh fires in [76s, 84s]; advance past the window.
    await act(async () => { await vi.advanceTimersByTimeAsync(85_000) })

    expect(renewSsoToken).toHaveBeenCalledTimes(1)
    expect(global.fetch).toHaveBeenCalledWith('http://auth/auth', expect.objectContaining({ method: 'POST' }))
    const body = JSON.parse(global.fetch.mock.calls[0][1].body)
    expect(body).toEqual({ ssoToken: 'fresh-sso', natsPublicKey: 'UPUB' }) // same nkey reused
    expect(reconnect).toHaveBeenCalledTimes(1)
  })

  it('redirects to relogin when silent renew fails', async () => {
    renewSsoToken.mockRejectedValue(new Error('login_required'))
    const { result } = renderHook(() =>
      useJwtRefresh({ authUrl: 'http://auth', ncRef: { current: null } }))
    act(() => {
      result.current.setCredentials({
        jwt: makeJwt(100), seed: new Uint8Array(), natsPublicKey: 'UPUB', refreshable: true,
      })
    })
    await act(async () => { await vi.advanceTimersByTimeAsync(85_000) })
    expect(redirectToReloginOnTokenInvalid).toHaveBeenCalledTimes(1)
  })

  it('does not schedule a refresh when refreshable is false (dev mode)', async () => {
    const { result } = renderHook(() =>
      useJwtRefresh({ authUrl: 'http://auth', ncRef: { current: null } }))
    act(() => {
      result.current.setCredentials({
        jwt: makeJwt(100), seed: new Uint8Array(), natsPublicKey: 'UPUB', refreshable: false,
      })
    })
    await act(async () => { await vi.advanceTimersByTimeAsync(200_000) })
    expect(renewSsoToken).not.toHaveBeenCalled()
  })

  it('clears the pending timer on unmount', async () => {
    const { result, unmount } = renderHook(() =>
      useJwtRefresh({ authUrl: 'http://auth', ncRef: { current: null } }))
    act(() => {
      result.current.setCredentials({
        jwt: makeJwt(100), seed: new Uint8Array(), natsPublicKey: 'UPUB', refreshable: true,
      })
    })
    unmount()
    await act(async () => { await vi.advanceTimersByTimeAsync(200_000) })
    expect(renewSsoToken).not.toHaveBeenCalled()
  })
})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd chat-frontend && npx vitest run src/context/NatsContext/useJwtRefresh.test.js`
Expected: FAIL — cannot resolve `./useJwtRefresh`.

- [ ] **Step 3: Implement the hook**

Create `chat-frontend/src/context/NatsContext/useJwtRefresh.js`:

```js
import { useRef, useMemo, useCallback, useEffect } from 'react'
import { jwtAuthenticator } from 'nats.ws'
import { renewSsoToken, redirectToReloginOnTokenInvalid } from '@/api/auth/oidcClient'
import { parseNatsJwtExp } from '@/lib/jwtExpiry'

// Refresh at ~80% of the JWT's remaining life, ±5% jitter so a fleet of
// sessions does not re-mint in lockstep.
const REFRESH_FRACTION = 0.8
const REFRESH_JITTER = 0.05

/**
 * Owns the live NATS credentials and the background-renewal loop.
 *
 * Returns:
 *  - authenticator: one stable nats.ws authenticator; pass it to connect().
 *    nats.ws re-invokes its getters on every (re)connect, so the connection
 *    always presents the current JWT/seed — every reconnect self-heals.
 *  - setCredentials({ jwt, seed, natsPublicKey, refreshable }): call after
 *    each /auth, BEFORE connect(), so the getters are populated for the
 *    handshake. Schedules the next refresh when refreshable.
 *  - stop(): clear the timer and creds (call on disconnect).
 */
export function useJwtRefresh({ authUrl, ncRef }) {
  const jwtRef = useRef(null)
  const seedRef = useRef(null)
  const pubKeyRef = useRef(null)
  const timerRef = useRef(null)
  const refreshRef = useRef(() => {})

  const authenticator = useMemo(
    () => jwtAuthenticator(() => jwtRef.current, () => seedRef.current),
    [],
  )

  const clearTimer = useCallback(() => {
    if (timerRef.current) {
      clearTimeout(timerRef.current)
      timerRef.current = null
    }
  }, [])

  const scheduleRefresh = useCallback((jwt) => {
    clearTimer()
    const expSec = parseNatsJwtExp(jwt)
    if (!expSec) return
    const lifeMs = expSec * 1000 - Date.now()
    if (lifeMs <= 0) return
    const delay = lifeMs * REFRESH_FRACTION * (1 + REFRESH_JITTER * (2 * Math.random() - 1))
    timerRef.current = setTimeout(() => { refreshRef.current() }, delay)
  }, [clearTimer])

  const refresh = useCallback(async () => {
    try {
      const ssoToken = await renewSsoToken()
      const resp = await fetch(`${authUrl}/auth`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ssoToken, natsPublicKey: pubKeyRef.current }),
      })
      if (!resp.ok) throw new Error(`auth refresh failed: ${resp.status}`)
      const { natsJwt } = await resp.json()
      jwtRef.current = natsJwt
      // Apply: force a quick reconnect so nats.ws re-reads the ref and presents
      // the fresh JWT. Degrades to the expiry-driven reconnect (the ref is
      // already fresh) if this build doesn't expose reconnect().
      const nc = ncRef.current
      if (nc && typeof nc.reconnect === 'function') {
        nc.reconnect()
      }
      scheduleRefresh(natsJwt)
    } catch {
      clearTimer()
      await redirectToReloginOnTokenInvalid()
    }
  }, [authUrl, ncRef, scheduleRefresh, clearTimer])

  // Keep refreshRef current so the timer always calls the latest closure —
  // this breaks the scheduleRefresh <-> refresh dependency cycle.
  useEffect(() => { refreshRef.current = refresh }, [refresh])

  const setCredentials = useCallback(({ jwt, seed, natsPublicKey, refreshable }) => {
    jwtRef.current = jwt
    seedRef.current = seed
    pubKeyRef.current = natsPublicKey
    if (refreshable) scheduleRefresh(jwt)
    else clearTimer()
  }, [scheduleRefresh, clearTimer])

  const stop = useCallback(() => {
    clearTimer()
    jwtRef.current = null
    seedRef.current = null
    pubKeyRef.current = null
  }, [clearTimer])

  useEffect(() => () => clearTimer(), [clearTimer])

  return { authenticator, setCredentials, stop }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd chat-frontend && npx vitest run src/context/NatsContext/useJwtRefresh.test.js`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add chat-frontend/src/context/NatsContext/useJwtRefresh.js chat-frontend/src/context/NatsContext/useJwtRefresh.test.js
git commit -m "feat(frontend): useJwtRefresh — dynamic authenticator + proactive renewal

Holds jwt/seed/pubkey refs behind one stable jwtAuthenticator (getter form),
so every reconnect re-reads current creds. A jittered timer at ~80% of the
JWT's real life runs signinSilent -> /auth (reusing the same nkey) -> swaps
the ref -> forces a quick reconnect. Silent-renew failure routes to the
graceful login redirect."
```

---

## Task 5: Wire `useJwtRefresh` into `NatsContext`

**Files:**
- Modify: `chat-frontend/src/context/NatsContext/NatsContext.jsx`
- Test: `chat-frontend/src/context/NatsContext/NatsContext.test.jsx`

- [ ] **Step 1: Write the failing test**

Create `chat-frontend/src/context/NatsContext/NatsContext.test.jsx`:

```js
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { renderHook, act, waitFor } from '@testing-library/react'

const setCredentials = vi.fn()
const stop = vi.fn()
const fakeAuthenticator = { tag: 'dynamic-auth' }
vi.mock('./useJwtRefresh', () => ({
  useJwtRefresh: vi.fn(() => ({ authenticator: fakeAuthenticator, setCredentials, stop })),
}))

const natsConnect = vi.fn()
vi.mock('nats.ws', () => ({
  connect: (...a) => natsConnect(...a),
  StringCodec: () => ({ encode: (s) => s, decode: (s) => s }),
  jwtAuthenticator: vi.fn(),
}))

vi.mock('nkeys.js', () => ({
  createUser: () => ({ getPublicKey: () => 'UPUBKEY', getSeed: () => new Uint8Array([7]) }),
}))

import { NatsProvider, useNats } from './NatsContext'

function wrapper({ children }) {
  return <NatsProvider>{children}</NatsProvider>
}

describe('NatsProvider connect wiring', () => {
  beforeEach(() => {
    setCredentials.mockReset()
    stop.mockReset()
    natsConnect.mockReset().mockResolvedValue({ closed: () => new Promise(() => {}) })
    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ natsJwt: 'JWT123', user: { account: 'alice' } }),
    })
  })
  afterEach(() => { vi.restoreAllMocks() })

  it('sets credentials before connecting and passes the dynamic authenticator', async () => {
    const { result } = renderHook(() => useNats(), { wrapper })
    await act(async () => {
      await result.current.connect({ mode: 'sso', ssoToken: 'tok', siteId: 'site-1' })
    })
    expect(setCredentials).toHaveBeenCalledWith({
      jwt: 'JWT123',
      seed: new Uint8Array([7]),
      natsPublicKey: 'UPUBKEY',
      refreshable: true,
    })
    expect(natsConnect).toHaveBeenCalledWith(
      expect.objectContaining({ authenticator: fakeAuthenticator }))
    await waitFor(() => expect(result.current.connected).toBe(true))
  })

  it('marks dev-mode connections non-refreshable', async () => {
    const { result } = renderHook(() => useNats(), { wrapper })
    await act(async () => {
      await result.current.connect({ mode: 'dev', account: 'alice', siteId: 'site-1' })
    })
    expect(setCredentials).toHaveBeenCalledWith(expect.objectContaining({ refreshable: false }))
  })

  it('stops the refresh loop on disconnect', async () => {
    natsConnect.mockResolvedValue({
      closed: () => new Promise(() => {}),
      drain: vi.fn().mockResolvedValue(),
    })
    const { result } = renderHook(() => useNats(), { wrapper })
    await act(async () => {
      await result.current.connect({ mode: 'sso', ssoToken: 'tok', siteId: 'site-1' })
    })
    await act(async () => { await result.current.disconnect() })
    expect(stop).toHaveBeenCalledTimes(1)
  })
})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd chat-frontend && npx vitest run src/context/NatsContext/NatsContext.test.jsx`
Expected: FAIL — `setCredentials`/`stop` never called (provider still builds a static authenticator inline).

- [ ] **Step 3: Update imports in `NatsContext.jsx`**

Change line 2 to drop `jwtAuthenticator` (it moves into the hook):

```js
import { connect as natsConnect, StringCodec } from 'nats.ws'
```

Add after the existing imports:

```js
import { useJwtRefresh } from './useJwtRefresh'
```

- [ ] **Step 4: Consume the hook and rewrite the connect/disconnect bodies**

Inside `NatsProvider`, just after `const natsUrl = NATS_URL`, add:

```js
  const { authenticator, setCredentials, stop } = useJwtRefresh({ authUrl, ncRef })
```

Replace the body of `connectToNats` from the `const { natsJwt, user: userInfo } = await authResp.json()` line through the `nc.closed()...` block with:

```js
    const { natsJwt, user: userInfo } = await authResp.json()

    // Populate the credential refs BEFORE connecting so the dynamic
    // authenticator's getters return the right values during the handshake.
    setCredentials({
      jwt: natsJwt,
      seed: nkey.getSeed(),
      natsPublicKey,
      refreshable: mode === 'sso',
    })

    const nc = await natsConnect({
      servers: natsUrl,
      authenticator,
    })

    ncRef.current = nc
    setUser({ ...userInfo, siteId })
    setConnected(true)

    nc.closed().then((err) => {
      if (err) {
        setError(`Disconnected: ${err.message}`)
      }
      setConnected(false)
    })
```

Update the `connectToNats` dependency array to:

```js
  }, [authUrl, natsUrl, authenticator, setCredentials])
```

Replace `disconnect` with:

```js
  const disconnect = useCallback(async () => {
    stop()
    if (ncRef.current) {
      await ncRef.current.drain()
      ncRef.current = null
    }
    setConnected(false)
    setUser(null)
  }, [stop])
```

(`nkey`, `natsPublicKey`, and the `body`/`fetch` lines above stay exactly as they are.)

- [ ] **Step 5: Run test to verify it passes**

Run: `cd chat-frontend && npx vitest run src/context/NatsContext/NatsContext.test.jsx`
Expected: PASS (3 tests).

- [ ] **Step 6: Full frontend gates**

Run: `cd chat-frontend && npm test && npm run typecheck && npm run build`
Expected: all tests pass, typecheck clean, build clean.

- [ ] **Step 7: Commit**

```bash
git add chat-frontend/src/context/NatsContext/NatsContext.jsx chat-frontend/src/context/NatsContext/NatsContext.test.jsx
git commit -m "feat(frontend): drive NATS connection through useJwtRefresh

Replace the frozen inline jwtAuthenticator with the hook's dynamic
authenticator. connectToNats now sets credentials before connecting and
marks sso-mode sessions refreshable; disconnect stops the refresh loop. The
session now renews across JWT expiry instead of evicting to the LoginPage."
```

---

## Task 6: Docs + final verification

**Files:**
- Modify: `docs/client-api.md`

- [ ] **Step 1: Locate the `/auth` section**

Run: `grep -n "POST /auth\|/auth\b" docs/client-api.md | head`
Identify the heading that documents the auth-service `POST /auth` endpoint.

- [ ] **Step 2: Add a renewal note**

Under that section, add this paragraph (schema is unchanged — this is informational only):

```markdown
> **Background renewal.** The web client also calls `POST /auth` periodically to
> renew the NATS user JWT before it expires (at ~80% of the token's lifetime,
> jittered). It obtains a fresh SSO access token in the background via the OIDC
> refresh token (silent renew) and re-mints with the **same** `natsPublicKey`,
> so the request/response schema is identical to the initial login call. When
> silent renewal fails (the SSO session has ended), the client performs a
> graceful re-login redirect instead.
```

- [ ] **Step 3: Full repo verification**

Run (backend): `make test SERVICE=auth-service && make lint && make sast`
Expected: tests pass; lint clean; SAST reports no medium+ findings.

Run (frontend): `cd chat-frontend && npm test && npm run typecheck && npm run build`
Expected: tests pass; typecheck clean; build clean.

- [ ] **Step 4: Commit**

```bash
git add docs/client-api.md
git commit -m "docs(client-api): note /auth is also the background-renewal endpoint"
```

- [ ] **Step 5: Push**

```bash
git push -u origin claude/reconnect-storm-triggers-SeK92
```

---

## Self-Review

**Spec coverage:**
- Refresh-token silent renew → Task 3 (`renewSsoToken`) + Task 4 (called in `refresh`). ✓
- Dynamic authenticator (getter form) → Task 4 (`useMemo` + getters), Task 5 (passed to `connect`). ✓
- nkey stable across refreshes → Task 4 (`pubKeyRef` reused in the `/auth` body; asserted in test). ✓
- Proactive trigger, jittered ~80% of real exp → Task 2 (`parseNatsJwtExp`) + Task 4 (`scheduleRefresh`). ✓
- Degradation if no `reconnect()` → Task 4 (`typeof nc.reconnect === 'function'` guard + comment). ✓
- Terminal fallback → graceful redirect → Task 4 (`catch` → `redirectToReloginOnTokenInvalid`). ✓
- Backend lifetime jitter → Task 1. ✓
- `/auth` schema unchanged + doc note → Task 6. ✓
- Per-unit tests, ≥80% coverage of new code → each task's tests cover happy path, failure, and edge (dev/non-refreshable, malformed token, unmount cleanup). ✓
- No new dependencies → confirmed (`signinSilent`, getter-form `jwtAuthenticator` already provided). ✓

**Type/name consistency:** `setCredentials({ jwt, seed, natsPublicKey, refreshable })` shape is identical in Task 4 (definition + tests) and Task 5 (call site + test). `authenticator`/`stop` names match across hook and provider. `parseNatsJwtExp` returns seconds; `useJwtRefresh` multiplies by 1000 — consistent.

**Risks carried from the spec (verify during execution, do not block a task):**
1. `nats.ws@1.30` reconnect entry point — if `nc.reconnect()` is absent, the `typeof` guard already degrades to passive; no code change needed, but note it in the PR.
2. Keycloak must actually issue a refresh token for the `nats-chat` client — if not, `signinSilent` rejects and the user hits the graceful redirect sooner (still no abrupt drop). Out of scope to configure here.
