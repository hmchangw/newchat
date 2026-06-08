# Seamless NATS-JWT Refresh — Design

**Date:** 2026-06-05
**Status:** Approved (pending spec review)
**Branch:** `claude/reconnect-storm-triggers-SeK92`

## Problem

The browser authenticates once at login: it POSTs an OIDC SSO access token to
auth-service `POST /auth`, receives a signed NATS user JWT, and opens the NATS
WebSocket with `jwtAuthenticator(natsJwt, seed)` (`NatsContext.jsx:71`). That
JWT is **frozen at connect time** and carries its own `exp` —
`NATS_JWT_EXPIRY`, default **2h** (`auth-service/main.go:23`,
`handler.go:188`).

NATS uses the **decentralized JWT model**: the server trusts the account
signing key and validates the user JWT's signature and `exp` itself — there is
no `auth_callout` on the connect path (the shipped auth-service is HTTP-only;
the original callout plan was not adopted). Consequently, when the JWT expires
the **server closes the connection**, and the only way to extend a session is
to present a freshly-signed JWT, which requires a (re)connect. Because the
current authenticator is static, every reconnect replays the *expired* JWT, so
the user is evicted to the LoginPage roughly every 2 hours.

Two failure shapes:
1. **Per-user eviction** at the 2h cliff (the user experience problem).
2. **Lockstep expiry** across a fleet — many sessions minted around the same
   time expire together and reconnect-storm auth-service (the operational
   problem).

## Goal

Keep an authenticated NATS session alive across JWT expiry **without any
visible logout**, for as long as the upstream OIDC SSO session permits. When
the SSO session itself ends (e.g. overnight), fall back to a **graceful login
redirect**, never an abrupt drop.

## Approach

Renew the NATS JWT *before* it expires by obtaining a fresh SSO access token in
the background and re-minting through the existing `/auth` endpoint, then apply
the new JWT to the live connection via a **dynamic authenticator** and a
controlled reconnect.

### Renewal mechanism — refresh token, not `offline_access`

The OIDC client already uses Authorization Code + PKCE
(`oidcClient.js:18`), so Keycloak already issues a **session-scoped refresh
token**, stashed by oidc-client-ts on the `User` object. We use it via
`userManager.signinSilent()`, which runs the `refresh_token` grant in the
background (no redirect, no iframe) and yields a fresh access token.

- We do **not** add `offline_access`. That issues an *offline* token that
  outlives the SSO session, which contradicts the desired "graceful redirect
  when the session ends" behavior. The regular refresh token's lifetime is
  bounded by the SSO session — exactly the cliff where renewal should stop and
  the redirect should take over.
- The refresh token **never leaves the browser**. auth-service stays stateless:
  it keeps minting NATS JWTs from whatever valid access token it is handed. Clean
  security boundary; no backend involvement in refresh-token handling.

### Dynamic authenticator

Replace the static authenticator with the callback form `nats.ws` supports:

```js
jwtAuthenticator(() => jwtRef.current, () => seedRef.current)
```

`nats.ws` re-invokes these getters on **every connect and every reconnect**, so
the connection always presents current credentials. This is the load-bearing
primitive: it makes *every* reconnect self-healing (planned, network blip, or
laptop-wake), not just the refresh path.

- The **nkey is stable** across refreshes. A NATS JWT is minted for a specific
  nkey public key (the `natsPublicKey` POSTed to `/auth`), so on renewal we
  reuse the same nkey and rotate only `jwtRef`; `seedRef` is untouched and
  identity does not churn.
- A **single** authenticator instance (`useMemo(..., [])`) reads the refs for
  the provider's whole lifetime.

### Trigger — proactive

A jittered timer fires at **~80% of the JWT's actual remaining life** (read from
the token, not assumed — see scheduling below). On fire:

1. `signinSilent()` → fresh access token.
2. POST `/auth` with the fresh access token + the **same** `natsPublicKey`.
3. Write the new JWT into `jwtRef`.
4. Force **one quick reconnect** so `nats.ws` re-reads the ref and applies the
   new JWT at a controlled, jittered moment.

The reconnect's first auth attempt already holds a valid fresh JWT, so it
succeeds immediately — no race against the expiry edge, no background-tab timer
throttling gap, no consecutive-auth-failure abort. The gap is sub-second and the
app already tolerates reconnects (generation counters, refetch-on-reconnect).

**Degradation:** if `nats.ws` 1.30 does not expose a clean reconnect entry
point, this degrades naturally to passive — the ref is already fresh, so the
eventual expiry-driven reconnect picks it up. The exact reconnect API
(`nc.reconnect()` vs a controlled drain+reconnect) is verified during
implementation.

### Scheduling — read the real `exp`

The timer schedules off the JWT's actual `exp`, decoded client-side
(`parseNatsJwtExp`), not a hardcoded 2h. This keeps the client correct even
though the backend now **jitters** the lifetime (below), and keeps the `/auth`
response contract unchanged (no new field).

### Terminal fallback — graceful redirect

When `signinSilent()` fails (SSO session genuinely ended, refresh token
rejected), route through the **existing** `redirectToReloginOnTokenInvalid()`
(`oidcClient.js:46`) — clears stashed OIDC state and kicks a clean sign-in
redirect, returning the user to where they were. This reuses the path already
used for the `sso_token_expired` RPC case.

### Backend — lifetime jitter (de-sync)

In `signNATSJWT` (`auth-service/handler.go:186`), set
`exp = now + base ± jitter` instead of a fixed `base`, so a fleet that *does*
drop (failed renewals, deploy bounce) does not expire in lockstep. `/auth`
itself is otherwise unchanged and is reused as the re-mint endpoint. Jitter
band and config knob (env var with `envDefault`) are finalized in the plan;
target ≈ ±10% of `NATS_JWT_EXPIRY`.

## Components

Each unit has one purpose, a defined interface, and is testable in isolation.

| Unit | Location | Responsibility | Tests |
|---|---|---|---|
| Lifetime jitter | `auth-service/handler.go` `signNATSJWT` + config | `exp = now + base ± jitter` | Go unit: bounds + spread, deterministic via injected randomness |
| Silent renew config + helper | `chat-frontend/src/api/auth/oidcClient.js` | `renewSsoToken()` wrapping `signinSilent` (on-demand, driven by the refresh hook's timer — `automaticSilentRenew` intentionally NOT enabled, to avoid a second uncoordinated renewer) | vitest: mocked `UserManager`, success + failure |
| JWT exp parser | `chat-frontend/src/lib/jwtExpiry.js` (new) | `parseNatsJwtExp(jwt): number` (pure base64/JSON) | vitest: valid, malformed, missing `exp` |
| Refresh hook | `chat-frontend/src/context/NatsContext/useJwtRefresh.js` (new) | own `jwtRef`/`seedRef`; build the one dynamic authenticator; jittered timer; renew→swap→reconnect; failure→redirect | vitest: fake timers + mocked `nats.ws` + mocked oidc |
| Provider wiring | `chat-frontend/src/context/NatsContext/NatsContext.jsx` | consume `useJwtRefresh`; `setCredentials` after `/auth`; pass authenticator to `natsConnect` | existing NatsContext tests, extended |

### Data flow

```
login / reconnect
  └─ connectToNats: POST /auth (access token) → natsJwt
       └─ setCredentials(natsJwt, seed)  → jwtRef, seedRef
       └─ natsConnect({ authenticator })  → reads refs via getters
  └─ useJwtRefresh schedules timer at ~80% of parseNatsJwtExp(natsJwt)
       └─ timer fires:
            signinSilent() ─success→ POST /auth → jwtRef = fresh → reconnect
                           ─fail───→ redirectToReloginOnTokenInvalid()
```

## Scope and contracts

- Primarily a frontend feature plus one small backend addition.
- `/auth` request/response schema is **unchanged**; it is simply also called for
  renewal. `docs/client-api.md` needs only a note that `/auth` is now also the
  renewal endpoint — no schema change.
- No new third-party dependencies. `signinSilent` and the callback-form
  authenticator are already provided by `oidc-client-ts` and `nats.ws`.

## Testing strategy (TDD)

Red-Green-Refactor per unit, per CLAUDE.md.

- **Backend jitter:** table-driven Go test asserting `exp` falls within
  `[base-jitter, base+jitter]` and that distinct mints spread across the band
  (inject the randomness source so the test is deterministic). ≥80% coverage on
  the changed function.
- **`parseNatsJwtExp`:** pure unit tests — valid token, malformed segments,
  missing `exp`, non-numeric `exp`.
- **`renewSsoToken`:** mocked `UserManager`; assert it calls `signinSilent` and
  surfaces success token / throws on failure.
- **`useJwtRefresh`:** vitest with `vi.useFakeTimers` and mocked `nats.ws` +
  oidc. Cases: timer fires at the jittered point; successful renew swaps the ref
  and triggers reconnect; renew failure calls the redirect; nkey stays stable;
  cleanup clears the timer on unmount/disconnect (no leak).
- **Provider:** extend existing NatsContext tests to assert `setCredentials` is
  called after `/auth` and the dynamic authenticator (not a frozen string) is
  passed to `natsConnect`.

## Risks / to verify during implementation

1. **Reconnect entry point** in `nats.ws` 1.30 — confirm `nc.reconnect()` exists;
   else controlled drain+reconnect; else passive degradation.
2. **Keycloak refresh token issuance** — confirmed by flow type, but verify a
   refresh token is actually present on the `User` object in the target
   deployment; absence forces the passive iframe path (out of scope, would be a
   separate follow-up).
3. **Background-tab timer throttling** — proactive trigger plus reading the real
   `exp` mitigates; the forced reconnect on a fresh ref is the safety net.

## Out of scope

- `prompt=none` iframe silent renew (refresh-token-free path).
- Any `auth_callout` server wiring.
- Fixing the stale CLAUDE.md "NATS callout service" label (separate doc cleanup).
