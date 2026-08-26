import { userSubscriptionList } from '../_transport/subjects'
import type { Nats, DMSubscription, Room } from '../types'

/** Page size requested per `subscription.list` call. The server caps a
 *  single page at MAX_SUBSCRIPTION_LIMIT (1000) and defaults to 40 when
 *  no limit is sent; we request an explicit, larger page so a typical
 *  user's whole bucket arrives in one round-trip while each reply's
 *  per-site room/last-message enrichment stays bounded. Larger buckets
 *  page via the hasMore loop below. */
export const PAGE_LIMIT = 200

/** Hard ceiling on pages fetched per bucket. A correct backend clears
 *  hasMore, so this only guards against a buggy/misconfigured server
 *  claiming "another page follows" forever — it bails instead of
 *  spinning. PAGE_LIMIT * MAX_PAGES = 40k rooms per bucket, far beyond
 *  any real user; hitting it is logged, never silently truncated. */
export const MAX_PAGES = 200

/** Wire shape of one `subscription.list` reply page served by user-service.
 *  Mirrors Go's `PagedSubscriptionListResponse`: `subscriptions` is always
 *  a slice (possibly empty) and `hasMore` is true when a further page
 *  exists (the server over-fetches by one to compute it).
 *
 *  Each entry is typed `DMSubscription` (= Subscription ∪ { hrInfo? }) to
 *  match Go's flattened JSON for both subscription kinds: channels/groups
 *  ship plain Subscription (hrInfo absent ⇒ typed `undefined`), DM rooms
 *  ship DMSubscription (hrInfo present). One type covers both since
 *  DMSubscription extends Subscription. user-service embeds room-level
 *  metadata under `sub.room` so the frontend doesn't need a separate
 *  `rooms.list` call. */
interface SidebarBucketReply {
  subscriptions: DMSubscription[]
  hasMore: boolean
}

/** Which of the three `subscription.list` buckets a result came from. */
export type SidebarBucket = 'favorites' | 'apps' | 'rooms'

export interface SidebarBuckets {
  favoriteIds: string[]
  appIds: string[]
  channelDmIds: string[]
  /** Buckets that did NOT drain cleanly — an RPC rejected, a later page
   *  failed, or the server never cleared `hasMore`. Their contents are
   *  whatever we managed to fetch, so the caller must treat them as
   *  incomplete and refuse to delete rooms on their behalf. Empty means
   *  the whole bootstrap is authoritative. */
  failures: SidebarBucket[]
  /** Per-roomId map of the full subscription record (DM variant typing
   *  covers both kinds — see SidebarBucketReply above). The reducer
   *  stores this directly under `state.subscriptions` so components
   *  consume the live per-room state via `useSubscription(roomId)`. */
  subscriptions: Record<string, DMSubscription>
  /** Room records derived from the union of the three subscription
   *  responses, deduped by roomId. The reducer's BUCKETS_LOADED case
   *  consumes this to build `state.summaries` — no separate rooms.list
   *  RPC is needed because the real user-service embeds room metadata
   *  inline on each subscription reply. */
  rooms: Room[]
}

/**
 * Bootstrap the sidebar by fetching three lists from user-service in
 * parallel via `subscription.list`:
 *   1. `{ type: "current", favorite: true }` — favorited subscriptions,
 *      drives the Favorite section.
 *   2. `{ type: "apps" }` — app subscriptions, drives the Apps section.
 *   3. `{ type: "rooms" }` — non-app room subscriptions (channels / DMs /
 *      discussions), drives the Channels and DMs section.
 *
 * `subscription.list` is paginated (server default 40, cap 1000 per page).
 * Each bucket is drained to completion via `fetchAllPages` — it follows the
 * reply's `hasMore` flag, advancing `offset` by PAGE_LIMIT, so the sidebar
 * lists EVERY subscription rather than just the first page.
 *
 * Each subscription record carries its room metadata inline, so we derive
 * `rooms` from the union of all three replies (deduped by roomId). The
 * reducer's `BUCKETS_LOADED` action consumes this shape directly. Partition
 * exclusivity (favorite > apps > channelDm) is enforced at render time by
 * `useSidebarSections`, so a room ID can appear in more than one bucket
 * without double-render.
 *
 * Uses `Promise.allSettled` so a single bucket's RPC failure degrades that
 * one bucket to whatever it fetched before failing (empty if page one
 * failed) rather than black-holing the whole bootstrap.
 */
export async function fetchSidebarBuckets({ user, request }: Nats): Promise<SidebarBuckets> {
  const subject = userSubscriptionList(user.account, user.siteId)
  const results = await Promise.allSettled([
    fetchAllPages(request, subject, { type: 'current', favorite: true }),
    fetchAllPages(request, subject, { type: 'apps' }),
    fetchAllPages(request, subject, { type: 'rooms' }),
  ])
  const failures: SidebarBucket[] = []
  const unwrap = (
    result: PromiseSettledResult<BucketPages>,
    bucket: SidebarBucket,
    label: string,
  ): DMSubscription[] => {
    if (result.status === 'fulfilled') {
      if (result.value.degraded) failures.push(bucket)
      return result.value.subs
    }
    const err = result.reason
    console.warn(
      '[sidebar-bootstrap]',
      label,
      'FAILED:',
      err?.message ?? err,
    )
    failures.push(bucket)
    return []
  }
  const favSubs = unwrap(results[0], 'favorites', `${subject} {type:current,favorite:true}`)
  const appSubs = unwrap(results[1], 'apps', `${subject} {type:apps}`)
  const roomSubs = unwrap(results[2], 'rooms', `${subject} {type:rooms}`)

  const subscriptions: Record<string, DMSubscription> = {}
  const rooms: Room[] = []
  const collect = (subs: DMSubscription[], markFavorite = false) => {
    for (const s of subs) {
      if (!s?.roomId) continue
      // Later sources overwrite earlier ones, but the three responses
      // describe the same Subscription record so collisions are benign.
      // `favorite` is sticky: the favorite bucket means "favorited", so stamp
      // it here; a later bucket's copy (which omits the flag on the current
      // backend) must not clear it. The chatlist v2 read model derives the
      // Favorites section from this field. Idempotent once #134 sets it natively.
      const prev = subscriptions[s.roomId]
      const favorite = markFavorite || s.favorite || prev?.favorite
      const merged = favorite ? { ...s, favorite: true } : s
      const first = prev === undefined
      subscriptions[s.roomId] = merged
      if (first) rooms.push(subToRoom(merged, user.siteId))
    }
  }
  collect(favSubs, true)
  collect(appSubs)
  collect(roomSubs)
  return {
    favoriteIds: favSubs.map((s) => s.roomId),
    appIds: appSubs.map((s) => s.roomId),
    channelDmIds: roomSubs.map((s) => s.roomId),
    failures,
    subscriptions,
    rooms,
  }
}

/** One bucket's drained pages plus whether the drain completed. `degraded`
 *  means `subs` is a partial view of that bucket. */
interface BucketPages {
  subs: DMSubscription[]
  degraded: boolean
}

/** Drain one subscription bucket to completion. Requests successive pages
 *  (offset advances by PAGE_LIMIT — the requested window, NOT the returned
 *  row count, since the server may drop cross-site soft-deleted rows AFTER
 *  slicing while `hasMore` still reflects the query-level over-fetch) until
 *  the server clears `hasMore` or MAX_PAGES is hit.
 *
 *  Degrades toward "show as much as we could": a page-N failure keeps the
 *  pages already fetched instead of discarding the whole bucket — the goal
 *  is to list every subscription we can reach, so a partial list beats an
 *  empty one. A page-one failure naturally yields an empty bucket.
 *
 *  `filter` carries the bucket discriminator ({type} plus optional
 *  {favorite}). Never rejects; the caller's allSettled is a defensive net. */
async function fetchAllPages(
  request: Nats['request'],
  subject: string,
  filter: Record<string, unknown>,
): Promise<BucketPages> {
  const all: DMSubscription[] = []
  for (let page = 0; page < MAX_PAGES; page++) {
    let reply: SidebarBucketReply
    try {
      reply = await request<SidebarBucketReply>(subject, {
        ...filter,
        offset: page * PAGE_LIMIT,
        limit: PAGE_LIMIT,
      })
    } catch (err) {
      console.warn(
        '[sidebar-bootstrap]',
        `${subject} ${JSON.stringify(filter)} page ${page}`,
        'FAILED:',
        (err as Error)?.message ?? err,
      )
      return { subs: all, degraded: true }
    }
    if (reply?.subscriptions?.length) all.push(...reply.subscriptions)
    if (!reply?.hasMore) return { subs: all, degraded: false }
  }
  console.warn(
    '[sidebar-bootstrap]',
    `${subject} ${JSON.stringify(filter)}`,
    `stopped after MAX_PAGES=${MAX_PAGES}; server kept signalling hasMore — bucket may be truncated`,
  )
  return { subs: all, degraded: true }
}

/** Derive a `Room` from a subscription record — the ONE wire→Room mapper
 *  for the `Subscription`+`room` shape, shared by the sidebar bootstrap
 *  and the live `added` subscription.update path (both carry the same
 *  shape by design). The real user-service embeds the fields we actually
 *  need under `sub.room` (userCount, lastMsgAt, lastUserMsgAt, lastMsgId,
 *  appCount); fields the reducer's `toSummary` doesn't read default to
 *  neutral zero/empty values so the type contract is satisfied.
 *
 *  `crossSite` is tri-state on the wire: an explicit `true`/`false` is
 *  authoritative (global/local); ABSENT means the room's locality is
 *  unknown/unclassified (server hasn't backfilled it yet) and defaults to
 *  `true` (global) here — a missing flag must never be read as "safe to
 *  route local". */
export function subToRoom(sub: DMSubscription, fallbackSiteId: string): Room {
  return {
    id: sub.roomId,
    name: sub.name ?? '',
    type: sub.roomType,
    siteId: sub.siteId ?? fallbackSiteId,
    userCount: sub.room?.userCount ?? 0,
    appCount: sub.room?.appCount ?? 0,
    lastMsgId: sub.room?.lastMsgId ?? '',
    // lastMsgAt here means "user activity" client-side: seed from
    // lastUserMsgAt (last NON-system message) and fall back to lastMsgAt
    // for rooms that predate the field.
    lastMsgAt: sub.room?.lastUserMsgAt ?? sub.room?.lastMsgAt ?? undefined,
    createdAt: '',
    updatedAt: '',
    crossSite: sub.room?.crossSite ?? true,
  }
}

/** Extract the RoomKeysContext seed entry from a subscription's embedded
 *  room key, or null when the room carries none (plaintext DM / key not
 *  provisioned). The ONE place that knows the wire key contract — shared
 *  by the sidebar bootstrap and the live `added` subscription.update path. */
export function keyEntryFor(
  sub: Pick<DMSubscription, 'roomId' | 'room'> | undefined,
): { roomId: string; version: number; privateKey: string } | null {
  if (!sub) return null
  const room = sub.room
  if (!room?.privateKey || typeof room.keyVersion !== 'number') return null
  return { roomId: sub.roomId, version: room.keyVersion, privateKey: room.privateKey }
}
