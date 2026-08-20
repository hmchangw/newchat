/**
 * Browser-local cache of the user's subscription list, so the sidebar's
 * first paint after a reload already has rooms instead of waiting for the
 * three `subscription.list` bootstrap RPCs to drain.
 *
 * localStorage (not IndexedDB) on purpose: the read has to be synchronous
 * so `RoomEventsProvider` can hydrate inside `useReducer`'s lazy
 * initializer, during the first render. An async store would force
 * hydration into an effect, reintroducing the empty first frame this
 * cache exists to remove.
 *
 * Only ever an accelerator: every entry point swallows storage failures
 * and degrades to "no cache". Nothing here is a source of truth — the
 * bootstrap fetch reconciles whatever we paint.
 */

/** @typedef {import('../api/types').DMSubscription} DMSubscription */
/** @typedef {import('../api/types').ChatlistState} ChatlistState */

/**
 * @typedef {object} SubscriptionCachePayload
 * @property {Record<string, DMSubscription>} subscriptions
 * @property {string[]} favoriteIds
 * @property {string[]} appIds
 * @property {string[]} channelDmIds
 * @property {ChatlistState} [chatlist]
 */

export const CACHE_KEY = 'chat.subscriptionCache.v1'

/** Bumped whenever the stored shape changes. A record written by an older
 *  (or newer) build is ignored rather than migrated — the bootstrap fetch
 *  refills it within a second. */
export const CACHE_VERSION = 1

/** How many subscriptions a quota-failed write retries with, keeping the
 *  most recently active rooms. A truncated cache is fine: it only shortens
 *  the warm paint, and the fetch restores the full list. */
export const QUOTA_RETRY_LIMIT = 300

/** Drop the room's E2E private key before it reaches disk. Room keys stay
 *  memory-only in RoomKeysContext, and this cache must not be the thing
 *  that changes that. Everything else is copied as-is so the cache doesn't
 *  silently lose fields as the wire type grows. */
function withoutPrivateKey(sub) {
  if (!sub?.room || !('privateKey' in sub.room)) return sub
  const { privateKey, ...room } = sub.room
  return { ...sub, room }
}

/** The bucket ids live in state as Sets, and `JSON.stringify(new Set(…))`
 *  is `"{}"` — persisting one verbatim would hand the next hydration a
 *  non-iterable. Normalize any iterable to a plain array. */
function toIdArray(ids) {
  if (!ids) return []
  return Array.isArray(ids) ? ids : [...ids]
}

function lastActivity(sub) {
  const ts = Date.parse(sub?.room?.lastMsgAt ?? '')
  return Number.isNaN(ts) ? 0 : ts
}

/** Keep the `limit` most recently active rooms. */
function trimToRecent(subscriptions, limit) {
  const entries = Object.entries(subscriptions)
  if (entries.length <= limit) return subscriptions
  entries.sort((a, b) => lastActivity(b[1]) - lastActivity(a[1]))
  return Object.fromEntries(entries.slice(0, limit))
}

function write(record) {
  window.localStorage.setItem(CACHE_KEY, JSON.stringify(record))
}

/**
 * Persist the sidebar-shaped slices for `user`. No-op without an
 * identified user. A failed write (quota, disabled storage) is retried
 * once with a trimmed subscription set, then the record is dropped
 * entirely rather than left half-written.
 *
 * @param {{ account?: string, siteId?: string } | null | undefined} user
 * @param {{
 *   subscriptions?: Record<string, DMSubscription>,
 *   favoriteIds?: Iterable<string>,
 *   appIds?: Iterable<string>,
 *   channelDmIds?: Iterable<string>,
 *   chatlist?: ChatlistState,
 * }} slices
 */
export function saveSubscriptionCache(user, { subscriptions, favoriteIds, appIds, channelDmIds, chatlist }) {
  if (!user?.account || !user?.siteId) return
  const record = {
    v: CACHE_VERSION,
    account: user.account,
    siteId: user.siteId,
    savedAt: Date.now(),
    subscriptions: Object.fromEntries(
      Object.entries(subscriptions ?? {}).map(([roomId, sub]) => [roomId, withoutPrivateKey(sub)]),
    ),
    favoriteIds: toIdArray(favoriteIds),
    appIds: toIdArray(appIds),
    channelDmIds: toIdArray(channelDmIds),
    chatlist,
  }
  try {
    write(record)
    return
  } catch {
    // Fall through to the trimmed retry.
  }
  try {
    write({ ...record, subscriptions: trimToRecent(record.subscriptions, QUOTA_RETRY_LIMIT) })
  } catch {
    clearSubscriptionCache()
  }
}

/**
 * Read the cached slices for `user`, or null when there is nothing
 * usable: no record, a record from another schema version, a record
 * belonging to a different account or site, or malformed content.
 *
 * A different account overwrites rather than accumulating one record per
 * user, which bounds what the cache can occupy. The cost is that the
 * previous account's next login is a cold start.
 *
 * @param {{ account?: string, siteId?: string } | null | undefined} user
 * @returns {SubscriptionCachePayload | null}
 */
export function loadSubscriptionCache(user) {
  if (!user?.account || !user?.siteId) return null
  let record
  try {
    const raw = window.localStorage.getItem(CACHE_KEY)
    if (!raw) return null
    record = JSON.parse(raw)
  } catch {
    return null
  }
  if (record?.v !== CACHE_VERSION) return null
  if (record.account !== user.account || record.siteId !== user.siteId) return null
  if (!record.subscriptions || typeof record.subscriptions !== 'object') return null
  // localStorage is user-editable, so a bucket that isn't an array is
  // dropped rather than passed on to `new Set(...)` at hydration.
  const ids = (value) => (Array.isArray(value) ? value : [])
  return {
    subscriptions: record.subscriptions,
    favoriteIds: ids(record.favoriteIds),
    appIds: ids(record.appIds),
    channelDmIds: ids(record.channelDmIds),
    chatlist: record.chatlist,
  }
}

export function clearSubscriptionCache() {
  try {
    window.localStorage.removeItem(CACHE_KEY)
  } catch {
    // Storage unavailable; nothing to clear.
  }
}
