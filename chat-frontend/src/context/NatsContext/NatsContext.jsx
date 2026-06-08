import { createContext, useContext, useRef, useState, useCallback, useMemo } from 'react'
import { connect as natsConnect, StringCodec } from 'nats.ws'
import { createUser } from 'nkeys.js'
import { AUTH_URL, NATS_URL } from '@/lib/runtimeConfig'
import { useJwtRefresh } from './useJwtRefresh'
import {
  requestWithAsyncResult as asyncJobRequest,
  AsyncJobError,
  ASYNC_JOB_ERROR_KINDS,
} from '@/api/_transport/asyncJob'

export const NatsContext = createContext(null)

const sc = StringCodec()

export function NatsProvider({ children }) {
  const ncRef = useRef(null)
  const [connected, setConnected] = useState(false)
  const [user, setUser] = useState(null)
  const [error, setError] = useState(null)

  const authUrl = AUTH_URL
  const natsUrl = NATS_URL

  const { authenticator, setCredentials, stop } = useJwtRefresh({ authUrl, ncRef })

  /**
   * Authenticate against auth-service and open the NATS WebSocket
   * connection. On success, `user`/`connected` flip true and any
   * subsequent server-initiated close updates `error`.
   *
   * @param {Object} opts
   * @param {'dev'|'sso'} opts.mode
   * @param {string} [opts.account]   Dev mode: account name to log in as.
   * @param {string} [opts.ssoToken]  Production mode: OIDC access token.
   * @param {string}  opts.siteId
   * @throws if auth-service rejects or the NATS handshake fails.
   */
  const connectToNats = useCallback(async (opts) => {
    setError(null)

    const { mode, account, ssoToken, siteId } = opts || {}

    const nkey = createUser()
    const natsPublicKey = nkey.getPublicKey()

    const body =
      mode === 'sso'
        ? { ssoToken, natsPublicKey }
        : { account, natsPublicKey }

    const authResp = await fetch(`${authUrl}/auth`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    })

    if (!authResp.ok) {
      // auth-service emits the errcode envelope {code, reason?, error, metadata?}
      // via errhttp.Write. Older auth deployments may return {error} only —
      // err.code is then undefined and consumers fall back to err.message text.
      const errBody = await authResp.json().catch(() => ({}))
      throw new AsyncJobError(
        errBody.error || `Auth failed: ${authResp.status}`,
        ASYNC_JOB_ERROR_KINDS.SyncError,
        { code: errBody.code, reason: errBody.reason, metadata: errBody.metadata },
      )
    }

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
  }, [authUrl, natsUrl, authenticator, setCredentials])

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
    const payload = sc.encode(JSON.stringify(data))
    const resp = await ncRef.current.request(subject, payload, { timeout: 5000 })
    const parsed = JSON.parse(sc.decode(resp.data))
    if (parsed.error) {
      // errcode envelope {code, reason?, error, metadata?}. Legacy replies
      // (pre-migration backend during rollout) lack code/reason — consumers
      // fall back to err.message.
      throw new AsyncJobError(parsed.error, ASYNC_JOB_ERROR_KINDS.SyncError, {
        code: parsed.code,
        reason: parsed.reason,
        metadata: parsed.metadata,
      })
    }
    return parsed
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
    return asyncJobRequest(ncRef.current, account, subject, data, opts)
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
    const payload = sc.encode(JSON.stringify(data))
    ncRef.current.publish(subject, payload)
  }, [])

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
        try {
          const data = JSON.parse(sc.decode(msg.data))
          callback(data)
        } catch {
          // skip malformed messages
        }
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
    stop()
    if (ncRef.current) {
      await ncRef.current.drain()
      ncRef.current = null
    }
    setConnected(false)
    setUser(null)
  }, [stop])

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
