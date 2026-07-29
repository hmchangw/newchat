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

export interface SidebarBuckets {
  favoriteIds: string[]
  appIds: string[]
  channelDmIds: string[]
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
  const unwrap = (
    result: PromiseSettledResult<DMSubscription[]>,
    label: string,
  ): DMSubscription[] => {
    if (result.status === 'fulfilled') {
      return result.value
    }
    const err = result.reason
    console.warn(
      '[sidebar-bootstrap]',
      label,
      'FAILED:',
      err?.message ?? err,
    )
    return []
  }
  const favSubs = unwrap(results[0], `${subject} {type:current,favorite:true}`)
  const appSubs = unwrap(results[1], `${subject} {type:apps}`)
  const roomSubs = unwrap(results[2], `${subject} {type:rooms}`)

  const subscriptions: Record<string, DMSubscription> = {}
  const rooms: Room[] = []
  const collect = (subs: DMSubscription[]) => {
    for (const s of subs) {
      if (!s?.roomId) continue
      // Later sources overwrite earlier ones, but the three responses
      // describe the same Subscription record so collisions are benign.
      const first = subscriptions[s.roomId] === undefined
      subscriptions[s.roomId] = s
      if (first) rooms.push(subToRoom(s, user.siteId))
    }
  }
  collect(favSubs)
  collect(appSubs)
  collect(roomSubs)
  return {
    favoriteIds: favSubs.map((s) => s.roomId),
    appIds: appSubs.map((s) => s.roomId),
    channelDmIds: roomSubs.map((s) => s.roomId),
    subscriptions,
    rooms,
  }
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
): Promise<DMSubscription[]> {
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
      return all
    }
    if (reply?.subscriptions?.length) all.push(...reply.subscriptions)
    if (!reply?.hasMore) return all
  }
  console.warn(
    '[sidebar-bootstrap]',
    `${subject} ${JSON.stringify(filter)}`,
    `stopped after MAX_PAGES=${MAX_PAGES}; server kept signalling hasMore — bucket may be truncated`,
  )
  return all
}

/** Derive a `Room` from a subscription record. The real user-service
 *  embeds the fields we actually need under `sub.room` (userCount,
 *  lastMsgAt, lastMsgId, appCount); fields the reducer's `toSummary`
 *  doesn't read default to neutral zero/empty values so the type
 *  contract is satisfied. */
function subToRoom(sub: DMSubscription, fallbackSiteId: string): Room {
  return {
    id: sub.roomId,
    name: sub.name ?? '',
    type: sub.roomType,
    siteId: sub.siteId ?? fallbackSiteId,
    userCount: sub.room?.userCount ?? 0,
    appCount: sub.room?.appCount ?? 0,
    lastMsgId: sub.room?.lastMsgId ?? '',
    lastMsgAt: sub.room?.lastMsgAt ?? undefined,
    createdAt: '',
    updatedAt: '',
  }
}
