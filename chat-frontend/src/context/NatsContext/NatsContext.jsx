import { createContext, useContext, useRef, useState, useCallback, useEffect, useMemo } from 'react'
import { SpanKind } from '@opentelemetry/api'
import { connect as natsConnect, StringCodec, headers as natsHeaders } from 'nats.ws'
import { createUser } from 'nkeys.js'
import { PORTAL_URL } from '@/lib/runtimeConfig'
import { injectTraceHeaders, natsSpanName, withLinkedSpan, withSpan } from '@/lib/telemetry'
import { useDebug } from '@/context/DebugContext'
import { useJwtRefresh } from './useJwtRefresh'
import { setFailoverMode } from '@/api/_transport/failover'
import { shufflePeers } from '@/api/_transport/peers'
import {
  requestWithAsyncResult as asyncJobRequest,
  requestSync as asyncJobRequestSync,
  AsyncJobError,
  ASYNC_JOB_ERROR_KINDS,
} from '@/api/_transport/asyncJob'

export const NatsContext = createContext(null)

const sc = StringCodec()

// Persisted bot/admin login bundle. sessionStorage (not localStorage) scopes it
// to the tab lifetime so a reload auto-reconnects but closing the tab logs out.
const BOT_SESSION_KEY = 'chat.botSession'

// Home re-probe backoff while connected to a peer.
//
// HOME_PROBE_MAX_MS is COUPLED to the server's FAILOVER_REVERT_GRACE (default
// 30m): while any client may still be on a peer, publishers keep emitting room
// events to BOTH subject roots. Raising this cap without raising that window
// reopens the silent recovery gap, where servers have reverted to local routing
// and the stragglers hear nothing.
export const HOME_PROBE_BASE_MS = 5_000
const HOME_PROBE_MAX_MS = 300_000

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

// Both services emit the errcode envelope {code, reason?, error, metadata?}.
// Legacy deployments may return {error} only — callers fall back to message.
async function throwEnvelopeError(resp, fallbackMsg) {
  const errBody = await resp.json().catch(() => ({}))
  throw new AsyncJobError(
    errBody.error || `${fallbackMsg}: ${resp.status}`,
    ASYNC_JOB_ERROR_KINDS.SyncError,
    { code: errBody.code, reason: errBody.reason, metadata: errBody.metadata },
  )
}

// fetchPeerSites reads the failover peer list from the portal. Called only
// after home has actually refused, so the common path pays nothing for it — and
// the portal is reachable during the outage this exists for, since only NATS is
// down. A portal that cannot serve the list yields no candidates rather than
// masking the original connect failure.
async function fetchPeerSites() {
  try {
    const resp = await tracedFetch('GET', `${PORTAL_URL}/api/settings`)
    if (!resp.ok) return []
    const settings = await resp.json()
    return Array.isArray(settings.sites) ? settings.sites : []
  } catch {
    return []
  }
}

function tracedFetch(method, url, init = {}) {
  const parsed = new URL(url)
  return withSpan(
    'http.client',
    {
      'http.request.method': method,
      'server.address': parsed.hostname,
      'server.port': parsed.port ? Number(parsed.port) : undefined,
      'url.scheme': parsed.protocol.replace(':', ''),
      'url.path': parsed.pathname,
    },
    async () => {
      const headers = new Headers(init.headers ?? {})
      injectTraceHeaders(headers)
      return fetch(url, { ...init, method, headers })
    },
  )
}

export function NatsProvider({ children }) {
  const ncRef = useRef(null)
  // Bumped on every connect and disconnect so a superseded connection's
  // long-lived nc.closed() callback can detect it is stale and drop its write
  // (codebase "stale-cycle protection" convention).
  const connectGenRef = useRef(0)
  // Pending home re-probe while connected to a peer. Cleared on every teardown:
  // a probe that outlives its session leaks a timer, and a late success would
  // clobber a newer connection.
  const homeProbeRef = useRef(null)
  const [connected, setConnected] = useState(false)
  const [user, setUser] = useState(null)
  const [error, setError] = useState(null)

  // Resolved per user by the portal lookup at connect time; the JWT-refresh
  // loop reads it through the getter so re-mints follow the resolved site.
  const authUrlRef = useRef(null)
  const getAuthUrl = useCallback(() => authUrlRef.current, [])

  // Keep the live debug settings in refs so the transport callbacks can read
  // them at send time without being recreated (and re-rendering consumers) on
  // every change. When the level is not 'off' every request/publish carries an
  // `X-Debug: <level>` header; when payload capture is on it also carries
  // `X-Debug-Payload: 1` (independent of the level).
  const { level: debugLevel, payload: debugPayload } = useDebug()
  const debugLevelRef = useRef(debugLevel)
  const debugPayloadRef = useRef(debugPayload)
  useEffect(() => { debugLevelRef.current = debugLevel }, [debugLevel])
  useEffect(() => { debugPayloadRef.current = debugPayload }, [debugPayload])

  const buildHeaders = useCallback(() => {
    const lvl = debugLevelRef.current
    const payload = debugPayloadRef.current
    const wantsDebug = lvl && lvl !== 'off'
    if (!wantsDebug && !payload) return undefined
    const h = natsHeaders()
    if (wantsDebug) h.set('X-Debug', lvl)
    if (payload) h.set('X-Debug-Payload', '1')
    return h
  }, [])

  const clearHomeProbe = useCallback(() => {
    if (homeProbeRef.current) {
      clearTimeout(homeProbeRef.current)
      homeProbeRef.current = null
    }
  }, [])

  // Terminal session-token failure: no IdP to bounce to, so tear the link down
  // and clear the stash. App then renders the login form.
  const onSessionLost = useCallback(() => {
    clearStoredBotSession()
    setFailoverMode(false)
    clearHomeProbe()
    connectGenRef.current += 1
    if (ncRef.current) { ncRef.current.drain().catch(() => {}); ncRef.current = null }
    setConnected(false)
    setUser(null)
  }, [clearHomeProbe])

  const { authenticator, setCredentials, stop } = useJwtRefresh({ getAuthUrl, ncRef, onSessionLost })

  // watchClosed reports a server-initiated close, ignoring links superseded by a
  // newer connect (or by the home revert below).
  const watchClosed = useCallback((nc, myGen) => {
    nc.closed().then((err) => {
      if (myGen !== connectGenRef.current || ncRef.current !== nc) return
      if (err) setError(`Disconnected: ${err.message}`)
      setConnected(false)
    })
  }, [])

  /**
   * While connected to a peer, retry home on exponential backoff and swap back
   * the moment it accepts. Servers revert as soon as their own lane delivers
   * again, so a client only finds out by asking — without this a displaced user
   * stays on the peer until they reload the tab.
   *
   * The JWT is account-scoped and every site's NATS shares that account, so the
   * credentials already staged for the peer dial work for home unchanged.
   */
  const startHomeProbe = useCallback((portal, myGen) => {
    let delay = HOME_PROBE_BASE_MS
    const attempt = async () => {
      homeProbeRef.current = null
      if (myGen !== connectGenRef.current) return
      let home
      try {
        home = await natsConnect({ servers: portal.natsUrl, authenticator })
      } catch {
        delay = Math.min(delay * 2, HOME_PROBE_MAX_MS)
        homeProbeRef.current = setTimeout(attempt, delay)
        return
      }
      // The session moved on while we were dialling — drop the probe link
      // rather than install it over a newer connection.
      if (myGen !== connectGenRef.current) {
        home.drain().catch(() => {})
        return
      }
      const peer = ncRef.current
      ncRef.current = home
      setFailoverMode(false)
      watchClosed(home, myGen)
      if (peer) peer.drain().catch(() => {})
    }
    homeProbeRef.current = setTimeout(attempt, delay)
  }, [authenticator, watchClosed])

  /**
   * Resolve the user's home site via the portal lookup, authenticate
   * against that site's auth-service, and open the NATS WebSocket
   * connection to that site. On success, `user`/`connected` flip true and
   * any subsequent server-initiated close updates `error`.
   *
   * @param {Object} opts
   * @param {'dev'|'sso'|'session'} opts.mode
   * @param {string} [opts.account]   Dev mode: account name to log in as.
   * @param {string} [opts.ssoToken]  Production mode: OIDC access token.
   * @param {Object} [opts.bundle]    Session mode: bot/admin login bundle from
   *   `botLogin` (skips the portal lookup — the bundle already carries the
   *   home-site info).
   * @throws if the portal lookup or auth-service rejects, or the NATS
   *   handshake fails.
   */
  const connectToNats = useCallback(async (opts) => {
    const myGen = ++connectGenRef.current
    setError(null)

    const { mode, account, ssoToken, bundle } = opts || {}

    // 1) Site discovery: which auth-service, which NATS, which siteId. The
    // session (bot/admin) path already holds the home-site bundle from portal
    // /api/v1/login, so it skips the /api/userInfo lookup. For sso/dev this is
    // discovery only — the portal validates no token; auth-service is the real
    // gate that re-validates the token before minting the JWT.
    let portal
    if (mode === 'session') {
      portal = bundle
    } else {
      const lookupResp = await tracedFetch(
        'GET',
        `${PORTAL_URL}/api/userInfo?account=${encodeURIComponent(account ?? '')}`,
      )
      if (!lookupResp.ok) {
        await throwEnvelopeError(lookupResp, 'Portal lookup failed')
      }
      portal = await lookupResp.json()
    }
    // baseUrl is the site's unified API gateway; auth lives at /api/v1/auth
    // behind it. This backend consolidated the former separate authServiceUrl
    // into that single baseUrl endpoint.
    const nextAuthUrl = portal.baseUrl

    // 2) Mint the NATS JWT at the resolved site's auth-service.
    const nkey = createUser()
    const natsPublicKey = nkey.getPublicKey()

    const body =
      mode === 'sso'
        ? { ssoToken, natsPublicKey }
        : mode === 'session'
          ? { authToken: bundle.authToken, natsPublicKey }
          : { account, natsPublicKey }

    const authResp = await tracedFetch('POST', `${nextAuthUrl}/api/v1/auth`, {
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    })

    if (!authResp.ok) {
      await throwEnvelopeError(authResp, 'Auth failed')
    }

    const { natsJwt, user: userInfo } = await authResp.json()

    // A failed dial must roll back: stop() disarms the refresh loop the
    // staged credentials armed, and the auth URL is committed only on success.
    try {
      // Populate the credential refs BEFORE connecting so the dynamic
      // authenticator's getters return the right values during the handshake.
      setCredentials({
        jwt: natsJwt,
        seed: nkey.getSeed(),
        natsPublicKey,
        refreshable: mode === 'sso' || mode === 'session',
        ...(mode === 'session' ? { mode: 'session', authToken: bundle.authToken } : {}),
      })

      // 3) Dial the resolved site's NATS, falling back to a peer if home
      // refuses. Home is tried first and alone in the common case; the peer
      // list is only fetched once home has actually failed.
      let nc = null
      let landedOn = null
      let homeErr = null
      try {
        nc = await natsConnect({ servers: portal.natsUrl, authenticator })
        landedOn = portal.siteId
      } catch (err) {
        homeErr = err
        // A peer that is also down is just the next failed attempt — there is
        // no liveness tracking anywhere, by design.
        for (const peer of shufflePeers(await fetchPeerSites(), portal.siteId)) {
          try {
            nc = await natsConnect({ servers: peer.natsUrl, authenticator })
            landedOn = peer.siteId
            break
          } catch { /* next candidate */ }
        }
      }
      // Nothing accepted: surface HOME's error, not a generic one. Home is the
      // site the user belongs to, so its failure is the actual diagnosis; the
      // peers were only ever a fallback.
      if (!nc) throw homeErr

      // Failover mode is on exactly when we are not on our home site. It makes
      // every room subscription use the global subject root, matching the
      // server, which publishes there while this site's own NATS is down.
      const onFailover = landedOn !== portal.siteId
      setFailoverMode(onFailover)

      authUrlRef.current = nextAuthUrl
      ncRef.current = nc
      // baseUrl is the media/API gateway origin — components build absolute
      // attachment (image/file) URLs from it + the attachment's relative
      // titleLink. Exposed here so MessageAttachments doesn't reach into the ref.
      setUser({ ...userInfo, siteId: portal.siteId, baseUrl: nextAuthUrl })
      setConnected(true)

      // Persist the bot/admin bundle so a tab reload resumes the session.
      if (mode === 'session') {
        try { window.sessionStorage.setItem(BOT_SESSION_KEY, JSON.stringify(bundle)) } catch { /* storage unavailable */ }
      }

      // A newer connect, a disconnect, or a home revert supersedes this link;
      // its close must not clobber the live session's state.
      watchClosed(nc, myGen)

      // On a peer: keep asking whether home is back, and swap over when it is.
      if (onFailover) startHomeProbe(portal, myGen)
    } catch (err) {
      stop()
      throw err
    }
  }, [authenticator, setCredentials, stop, watchClosed, startHomeProbe, clearHomeProbe])

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

  /**
   * Send a synchronous NATS request/reply. Use this for handlers that
   * return their full result inline (e.g. `member.list`, `search.rooms`).
   * For deferred-result operations use `requestWithAsyncResult` instead.
   *
   * @param {string} subject
   * @param {unknown} [data={}]  JSON-serialisable payload.
   * @returns {Promise<unknown>} Parsed JSON reply.
   * @throws {AsyncJobError} On error replies the thrown error carries
   *   `.code` (always) and `.reason`/`.metadata` (when the backend emits
   *   them). Branch on `reason ?? code`; `.message` is the user-safe text
   *   for display only. Wire-level failures (not connected, request
   *   timeout) still throw a plain Error.
   */
  const request = useCallback(async (subject, data = {}) => {
    if (!ncRef.current) throw new Error('Not connected')
    return asyncJobRequestSync(ncRef.current, subject, data, {
      debugLevel: debugLevelRef.current,
      debugPayload: debugPayloadRef.current,
    })
  }, [])

  /**
   * Two-phase request/reply for operations whose sync reply is just
   * "accepted" — the real outcome arrives later on the per-request
   * response subject as an AsyncJobResult. Components await this and
   * get the final ok/error from the worker, not the optimistic accept.
   *
   * Injects the current `user.account` and the live `nc`; for the full
   * contract see {@link asyncJobRequest} in `api/_transport/asyncJob.js`.
   *
   * @param {string} subject
   * @param {unknown} [data={}]
   * @param {Object} [opts]  Forwarded to the helper (`treatAsSuccess`,
   *   `requestId`, `syncTimeout`, `asyncTimeout`).
   * @returns {Promise<{requestId: string, sync: unknown, async: unknown}>}
   * @throws Tagged Error with `.kind` from ASYNC_JOB_ERROR_KINDS on every
   *   failure path; use `formatAsyncJobError` for user-facing text.
   */
  const requestWithAsyncResult = useCallback(async (subject, data = {}, opts = {}) => {
    if (!ncRef.current) throw new Error('Not connected')
    const account = user?.account
    if (!account) throw new Error('Not authenticated')
    return asyncJobRequest(ncRef.current, account, subject, data, {
      debugLevel: debugLevelRef.current,
      debugPayload: debugPayloadRef.current,
      ...opts,
    })
  }, [user])

  /**
   * Fire-and-forget JSON publish. Use for events the server consumes
   * via QueueSubscribe (no reply expected); for request/reply use
   * `request` or `requestWithAsyncResult`.
   *
   * @param {string} subject
   * @param {unknown} [data={}]
   * @throws if not connected.
   */
  const publish = useCallback((subject, data = {}) => {
    if (!ncRef.current) throw new Error('Not connected')
    return withSpan(
      natsSpanName('publish', subject),
      {
        'messaging.system': 'nats',
        'messaging.operation.name': 'publish',
        'messaging.destination.name': subject,
      },
      () => {
        const payload = sc.encode(JSON.stringify(data))
        const h = buildHeaders() ?? natsHeaders()
        injectTraceHeaders(h)
        ncRef.current.publish(subject, payload, { headers: h })
      },
    )
  }, [buildHeaders])

  /**
   * Subscribe to a subject pattern and dispatch parsed JSON messages
   * to `callback`. Malformed JSON is silently skipped (server
   * canonical events are always JSON).
   *
   * @param {string} subject
   * @param {(data: unknown) => void} callback
   * @returns {{unsubscribe: () => void}} The underlying NATS
   *   subscription. Callers MUST call `.unsubscribe()` on unmount /
   *   cleanup to avoid leaking the iterator and the server-side sid.
   * @throws if not connected.
   */
  const subscribe = useCallback((subject, callback) => {
    if (!ncRef.current) throw new Error('Not connected')
    const sub = ncRef.current.subscribe(subject)
    ;(async () => {
      for await (const msg of sub) {
        const msgSubject = msg.subject || subject
        withLinkedSpan(
          natsSpanName('receive', msgSubject),
          {
            'messaging.system': 'nats',
            'messaging.operation.name': 'receive',
            'messaging.destination.name': msgSubject,
            'messaging.subscription.name': subject,
          },
          msg.headers,
          () => {
            try {
              const data = JSON.parse(sc.decode(msg.data))
              callback(data)
            } catch {
              // skip malformed messages
            }
          },
          SpanKind.CONSUMER,
        )
      }
    })()
    return sub
  }, [])

  /**
   * Drain the NATS connection (flushes pending publishes, then closes)
   * and reset `user`/`connected`. Idempotent: calling on a disconnected
   * provider is a no-op.
   */
  const disconnect = useCallback(async () => {
    // Invalidate any in-flight connect and the live link's closed() callback so
    // a late close can't resurrect error/connected state after we tore down.
    clearStoredBotSession()
    setFailoverMode(false)
    clearHomeProbe()
    connectGenRef.current += 1
    stop()
    if (ncRef.current) {
      await ncRef.current.drain()
      ncRef.current = null
    }
    setConnected(false)
    setUser(null)
  }, [stop, clearHomeProbe])

  // Memoise so consumers that only read stable callbacks don't re-render
  // on every provider render. The value identity flips only when one of
  // the listed primitives/refs flips.
  const value = useMemo(
    () => ({
      connected, user, error,
      connect: connectToNats, request, requestWithAsyncResult, publish, subscribe, disconnect,
    }),
    [connected, user, error, connectToNats, request, requestWithAsyncResult, publish, subscribe, disconnect]
  )

  return <NatsContext.Provider value={value}>{children}</NatsContext.Provider>
}

export function useNats() {
  const ctx = useContext(NatsContext)
  if (!ctx) throw new Error('useNats must be used within NatsProvider')
  return ctx
}
