import { createContext, useCallback, useContext, useEffect, useReducer, useRef } from 'react'
import { requestRoomKey, subscribeToRoomKeyEvents } from '@/api'
import type { Nats, RoomKeyEvent } from '@/api'
import { useNats } from '@/context/NatsContext'
import { b64decode, importAesKey, decryptRoomMessage } from '@/lib/roomcrypto'
import { bytesEqual, initialRoomKeysState, roomKeysReducer } from './reducer'

const KEY_RETRY_BACKOFF_MS = 60_000

type DecryptInput = {
  roomId: string
  version: number
  nonceB64: string
  ciphertextB64: string
}

type RoomKeysContextValue = {
  hasKey(roomId: string, version: number): boolean
  /** Returns null if the key is not (yet) known for that (roomId, version),
   *  or if decryption fails. */
  decrypt(input: DecryptInput): Promise<string | null>
  /** Fetch the (roomId, version) key from room-service when it isn't
   *  cached. Resolves true on success (KEY_RECEIVED dispatched), false on
   *  any error. Concurrent callers for the same (roomId, version) share
   *  one in-flight RPC. After a failure, subsequent calls within
   *  KEY_RETRY_BACKOFF_MS resolve false without re-issuing the RPC. */
  ensureKey(roomId: string, version: number, siteId: string): Promise<boolean>
}

const RoomKeysContext = createContext<RoomKeysContextValue | null>(null)

export function useRoomKeys(): RoomKeysContextValue {
  const ctx = useContext(RoomKeysContext)
  if (!ctx) throw new Error('useRoomKeys called outside RoomKeysProvider')
  return ctx
}

export function RoomKeysProvider({ children }: { children: React.ReactNode }) {
  const [state, dispatch] = useReducer(roomKeysReducer, initialRoomKeysState)
  // `useNats()` returns `never` to TS because NatsContext.jsx does
  // `createContext(null)` without annotations. Cast here so downstream
  // callbacks see the proper Nats interface — safe because the
  // provider only renders inside the `connected` gate at App.jsx,
  // where the NATS handshake has populated user/request/etc.
  const nats = useNats() as unknown as Nats

  // CryptoKey cache lives in a ref — imported lazily, not React state.
  // Keyed by `${roomId}|${version}`.
  const aesKeyCacheRef = useRef<Map<string, Promise<CryptoKey>>>(new Map())
  const stateRef = useRef(state)
  stateRef.current = state

  // Keep a live ref to `nats` so long-lived subscription callbacks see
  // the latest connection without forcing the effect to re-run. The
  // effect depends only on user.account (a stable primitive) so it
  // rebuilds subs only when login actually changes — not on every nats
  // context value re-memoisation (see useRoomSubscriptions for prior art).
  const natsRef = useRef(nats)
  natsRef.current = nats

  // In-flight ensureKey promises keyed by `${roomId}|${version}` — concurrent
  // callers for the same key share the one RPC promise.
  const pendingRequestsRef = useRef<Map<string, Promise<boolean>>>(new Map())
  // Timestamp (ms) of the last failed fetch per `${roomId}|${version}` key —
  // prevents stampedes within KEY_RETRY_BACKOFF_MS of a failure.
  const failedAtRef = useRef<Map<string, number>>(new Map())
  // Synchronous set of keys known to be present — updated before dispatch so
  // subsequent ensureKey calls short-circuit without waiting for a React
  // re-render to flush the state update into stateRef.
  const knownKeysRef = useRef<Set<string>>(new Set())

  const userAccount = nats.user?.account ?? null

  useEffect(() => {
    if (!userAccount) return

    const liveNats = natsRef.current

    // TODO: seed initial keys from sub.room.privateKey + sub.room.keyVersion
    // delivered by subscription.list. The backend now populates these fields
    // (SubscriptionRoom.PrivateKey / KeyVersion in pkg/model). Wiring them
    // here requires exposing a seedKey() method on RoomKeysContextValue and
    // calling it from useRoomSubscriptions.js after the BUCKETS_LOADED
    // dispatch. Until that follow-up lands, RoomKeysContext populates from
    // live RoomKeyEvent subscriptions only; reconnecting users re-acquire
    // keys when a rotation or membership change next fires for each room.
    const sub = subscribeToRoomKeyEvents(liveNats, (raw) => {
      const evt = raw as RoomKeyEvent
      if (!evt || typeof evt.roomId !== 'string' || typeof evt.version !== 'number' || typeof evt.privateKey !== 'string') return
      let privateKey: Uint8Array
      try {
        privateKey = b64decode(evt.privateKey)
      } catch (err) {
        // eslint-disable-next-line no-console
        console.warn('roomKeyEvent: invalid base64 privateKey, dropping event', err)
        return
      }
      // Skip evicting the cached AES key when the rebroadcast bytes match
      // the stored bytes — the reducer no-ops on that path, so dropping
      // the derived CryptoKey would force a redundant deriveKey call.
      const existing = stateRef.current.byRoom[evt.roomId]?.[evt.version]
      if (!existing || !bytesEqual(existing.privateKey, privateKey)) {
        aesKeyCacheRef.current.delete(`${evt.roomId}|${evt.version}`)
      }
      knownKeysRef.current.add(`${evt.roomId}|${evt.version}`)
      dispatch({
        type: 'KEY_RECEIVED',
        roomId: evt.roomId,
        version: evt.version,
        privateKey,
      })
    })

    return () => {
      sub.unsubscribe()
      aesKeyCacheRef.current.clear()
      pendingRequestsRef.current.clear()
      failedAtRef.current.clear()
      knownKeysRef.current.clear()
      dispatch({ type: 'CLEAR_KEYS' })
    }
    // userAccount is a stable primitive (set once on login).
    // natsRef is always current — no need to list it.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [userAccount])

  const hasKey = useCallback((roomId: string, version: number) => {
    // knownKeysRef may retain entries evicted by trimVersions; hasKey must
    // reflect what decrypt can actually use.
    return !!stateRef.current.byRoom[roomId]?.[version]
  }, [])

  const decrypt = useCallback(async ({ roomId, version, nonceB64, ciphertextB64 }: DecryptInput): Promise<string | null> => {
    const entry = stateRef.current.byRoom[roomId]?.[version]
    if (!entry) return null

    const cacheKey = `${roomId}|${version}`
    let pending = aesKeyCacheRef.current.get(cacheKey)
    if (!pending) {
      pending = importAesKey(entry.privateKey)
      aesKeyCacheRef.current.set(cacheKey, pending)
    }
    try {
      const aesKey = await pending
      return await decryptRoomMessage(b64decode(ciphertextB64), b64decode(nonceB64), aesKey)
    } catch (err) {
      // Drop the cached promise so a subsequent decrypt retries derivation
      // instead of awaiting the same rejected promise forever. If the cache
      // entry was already replaced by a newer event between read and catch,
      // only delete our own — peek before evicting.
      if (aesKeyCacheRef.current.get(cacheKey) === pending) {
        aesKeyCacheRef.current.delete(cacheKey)
      }
      // eslint-disable-next-line no-console
      console.warn('roomKeysContext.decrypt failed:', err)
      return null
    }
  }, [])

  const ensureKey = useCallback(
    async (roomId: string, version: number, siteId: string): Promise<boolean> => {
      const cacheKey = `${roomId}|${version}`
      // Check both the synchronous ref (updated before dispatch) and the
      // reducer state (updated after re-render) so the short-circuit fires
      // even before the React state flush completes.
      if (knownKeysRef.current.has(cacheKey) || stateRef.current.byRoom[roomId]?.[version]) return true

      const existing = pendingRequestsRef.current.get(cacheKey)
      if (existing) return existing

      const failedAt = failedAtRef.current.get(cacheKey)
      if (failedAt !== undefined && Date.now() - failedAt < KEY_RETRY_BACKOFF_MS) {
        return false
      }

      const liveNats = natsRef.current
      if (!liveNats?.user?.account) return false

      const fetchPromise = (async () => {
        try {
          const resp = await requestRoomKey(liveNats, { roomId, siteId, version })
          let privateKey: Uint8Array
          try {
            privateKey = b64decode(resp.privateKey)
          } catch (err) {
            // eslint-disable-next-line no-console
            console.warn('ensureKey: invalid base64 privateKey', err)
            failedAtRef.current.set(cacheKey, Date.now())
            return false
          }
          // Mark the key as present synchronously before dispatch so
          // concurrent ensureKey callers short-circuit on the next tick
          // without waiting for the React state flush.
          knownKeysRef.current.add(cacheKey)
          dispatch({
            type: 'KEY_RECEIVED',
            roomId,
            version: resp.version,
            privateKey,
          })
          failedAtRef.current.delete(cacheKey)
          return true
        } catch (err) {
          // eslint-disable-next-line no-console
          console.warn('ensureKey: requestRoomKey failed', err)
          failedAtRef.current.set(cacheKey, Date.now())
          return false
        } finally {
          pendingRequestsRef.current.delete(cacheKey)
        }
      })()

      pendingRequestsRef.current.set(cacheKey, fetchPromise)
      return fetchPromise
    },
    [],
  )

  const value: RoomKeysContextValue = { hasKey, decrypt, ensureKey }

  return <RoomKeysContext.Provider value={value}>{children}</RoomKeysContext.Provider>
}
