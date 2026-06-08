import { useRef, useMemo, useCallback, useEffect } from 'react'
import { jwtAuthenticator } from 'nats.ws'
import { renewSsoToken, redirectToReloginOnTokenInvalid } from '@/api/auth/oidcClient'
import { parseNatsJwtExp } from '@/lib/jwtExpiry'

// Refresh at ~80% of the JWT's remaining life, ±5% jitter so a fleet of
// sessions does not re-mint in lockstep.
const REFRESH_FRACTION = 0.8
const REFRESH_JITTER = 0.05
// Transient re-mint failures (network / 5xx / 429) are retried with backoff
// before giving up — there is ~20% of token life in hand, so a momentary
// backend hiccup should not evict the user. A genuine renewal failure (SSO
// session gone, or a 4xx rejection) redirects immediately.
const RETRY_BACKOFF_MS = [2000, 4000, 8000]
const MAX_REFRESH_ATTEMPTS = RETRY_BACKOFF_MS.length + 1

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
  // Bumped by setCredentials/stop to invalidate an in-flight refresh, so a
  // relogin landing mid-refresh cannot be clobbered by a stale result
  // (codebase "stale-cycle protection" convention).
  const genRef = useRef(0)

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

  // Replace any pending timer with the next refresh attempt.
  const armTimer = useCallback((delayMs, attempt) => {
    clearTimer()
    timerRef.current = setTimeout(() => refreshRef.current(attempt), delayMs)
  }, [clearTimer])

  const scheduleRefresh = useCallback((jwt) => {
    clearTimer()
    const expSec = parseNatsJwtExp(jwt)
    if (!expSec) return
    const lifeMs = expSec * 1000 - Date.now()
    if (lifeMs <= 0) return
    const jitter = 1 + REFRESH_JITTER * (2 * Math.random() - 1)
    armTimer(lifeMs * REFRESH_FRACTION * jitter, 1)
  }, [clearTimer, armTimer])

  const redirect = useCallback(async (reason, detail) => {
    // No token/body in the log — just the reason and a coarse detail.
    console.warn(`NATS JWT refresh: ${reason}; redirecting to login`, detail)
    clearTimer()
    await redirectToReloginOnTokenInvalid()
  }, [clearTimer])

  const refresh = useCallback(async (attempt) => {
    const myGen = genRef.current
    const stale = () => genRef.current !== myGen

    // 1) Renew the SSO token. A failure here means the session is gone — terminal.
    let ssoToken
    try {
      ssoToken = await renewSsoToken()
    } catch (err) {
      if (!stale()) await redirect('silent renew failed', { error: err?.message })
      return
    }
    if (stale()) return

    // 2) Re-mint the NATS JWT. Transport failures are transient (retry with
    //    backoff); a 4xx rejection is terminal.
    try {
      const resp = await fetch(`${authUrl}/auth`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ssoToken, natsPublicKey: pubKeyRef.current }),
      })
      if (stale()) return
      if (!resp.ok) {
        if (resp.status >= 500 || resp.status === 429) {
          throw new Error(`transient auth status ${resp.status}`)
        }
        await redirect(`auth rejected (${resp.status})`, { status: resp.status })
        return
      }
      const { natsJwt } = await resp.json()
      if (stale()) return
      jwtRef.current = natsJwt
      // Force a quick reconnect so nats.ws re-reads the ref and presents the
      // fresh JWT. Fire-and-forget; swallow a rejected reconnect so it isn't an
      // unhandled rejection. An absent reconnect() degrades to the
      // expiry-driven reconnect — the ref is already fresh either way.
      if (typeof ncRef.current?.reconnect === 'function') {
        Promise.resolve(ncRef.current.reconnect()).catch(() => {})
      }
      scheduleRefresh(natsJwt)
    } catch (err) {
      if (stale()) return
      if (attempt >= MAX_REFRESH_ATTEMPTS) {
        await redirect('re-mint exhausted retries', { error: err?.message })
        return
      }
      console.warn('NATS JWT re-mint failed; retrying', { attempt, error: err?.message })
      armTimer(RETRY_BACKOFF_MS[attempt - 1], attempt + 1)
    }
  }, [authUrl, ncRef, scheduleRefresh, redirect, armTimer])

  // Keep refreshRef current so the timer always calls the latest closure —
  // this breaks the scheduleRefresh <-> refresh dependency cycle.
  useEffect(() => { refreshRef.current = refresh }, [refresh])

  const setCredentials = useCallback(({ jwt, seed, natsPublicKey, refreshable }) => {
    genRef.current += 1
    jwtRef.current = jwt
    seedRef.current = seed
    pubKeyRef.current = natsPublicKey
    if (refreshable) scheduleRefresh(jwt)
    else clearTimer()
  }, [scheduleRefresh, clearTimer])

  const stop = useCallback(() => {
    genRef.current += 1
    clearTimer()
    jwtRef.current = null
    seedRef.current = null
    pubKeyRef.current = null
  }, [clearTimer])

  useEffect(() => () => clearTimer(), [clearTimer])

  return { authenticator, setCredentials, stop }
}
