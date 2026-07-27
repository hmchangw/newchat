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

/** One room key delivered at bootstrap by `subscription.list`. privateKey is
 *  base64 (same wire shape as `RoomKeyGetResponse`). */
type SeedKeyEntry = {
  roomId: string
  version: number
  privateKey: string
}

type RoomKeysContextValue = {
  hasKey(roomId: string, version: number): boolean
  /** Seed room keys known at bootstrap (from `subscription.list`) so the first
   *  message in a room decrypts without waiting for a live event or an
   *  on-demand fetch. Malformed / undecodable entries are skipped; identical
   *  already-held keys no-op (via the reducer's bytesEqual guard). */
  seedKeys(entries: SeedKeyEntry[]): void
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
  // Synchronous cache of raw key bytes keyed by `${roomId}|${version}`, written
  // in lockstep with knownKeysRef. decrypt reads it FIRST so a key fetched via
  // ensureKey can decrypt the very message that triggered the fetch: the
  // caller (useRoomSubscriptions.decryptAndDispatch) retries decrypt in the
  // same async tick, before React flushes KEY_RECEIVED into reducer state —
  // reading only stateRef would still miss the just-fetched key and fall back
  // to the "[encrypted message]" placeholder.
  const keyBytesRef = useRef<Map<string, Uint8Array>>(new Map())

  const userAccount = nats.user?.account ?? null

  useEffect(() => {
    if (!userAccount) return

    const liveNats = natsRef.current

    // Initial keys are seeded from sub.room.privateKey + sub.room.keyVersion
    // (delivered inline on subscription.list) via the `seedKeys` method, called
    // from useRoomSubscriptions after BUCKETS_LOADED. This subscription handles
    // the live delta: keys that rotate or are granted mid-session via
    // RoomKeyEvent. On-demand fetches (ensureKey) cover any remaining gap.
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
      keyBytesRef.current.set(`${evt.roomId}|${evt.version}`, privateKey)
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
      keyBytesRef.current.clear()
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
    const cacheKey = `${roomId}|${version}`
    // Prefer the synchronous byte cache so a key just fetched by ensureKey is
    // usable on the immediate retry, before the KEY_RECEIVED dispatch flushes
    // into reducer state. Fall back to reducer state for keys seeded by paths
    // that don't touch the ref (defensive; today both writers keep them in
    // sync).
    const privateKey = keyBytesRef.current.get(cacheKey) ?? stateRef.current.byRoom[roomId]?.[version]?.privateKey
    if (!privateKey) return null

    let pending = aesKeyCacheRef.current.get(cacheKey)
    if (!pending) {
      pending = importAesKey(privateKey)
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
          // without waiting for the React state flush. keyBytesRef lets the
          // caller's immediate decrypt retry read the key in this same tick.
          knownKeysRef.current.add(cacheKey)
          keyBytesRef.current.set(cacheKey, privateKey)
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

  const seedKeys = useCallback((entries: SeedKeyEntry[]) => {
    if (!Array.isArray(entries)) return
    for (const entry of entries) {
      const { roomId, version, privateKey } = entry ?? {}
      if (!roomId || typeof version !== 'number' || typeof privateKey !== 'string' || !privateKey) {
        continue
      }
      let bytes: Uint8Array
      try {
        bytes = b64decode(privateKey)
      } catch (err) {
        // eslint-disable-next-line no-console
        console.warn('seedKeys: invalid base64 privateKey, skipping', { roomId, version }, err)
        continue
      }
      const cacheKey = `${roomId}|${version}`
      // Already hold identical bytes → nothing to do (mirrors the live-event
      // handler's no-op path; avoids evicting a derived AES key).
      const existing = stateRef.current.byRoom[roomId]?.[version]
      if (existing && bytesEqual(existing.privateKey, bytes)) continue
      // Different bytes for a version we already cached → drop the stale
      // derived AES key so decrypt re-imports from the new bytes.
      if (existing) aesKeyCacheRef.current.delete(cacheKey)
      knownKeysRef.current.add(cacheKey)
      keyBytesRef.current.set(cacheKey, bytes)
      dispatch({ type: 'KEY_RECEIVED', roomId, version, privateKey: bytes })
    }
  }, [])

  const value: RoomKeysContextValue = { hasKey, decrypt, ensureKey, seedKeys }

  return <RoomKeysContext.Provider value={value}>{children}</RoomKeysContext.Provider>
}
