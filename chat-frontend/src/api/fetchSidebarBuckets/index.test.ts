import { describe, it, expect, vi, afterEach } from 'vitest'
import { fetchSidebarBuckets, PAGE_LIMIT, MAX_PAGES } from './index'
import type { Nats, DMSubscription } from '../types'

const SUBJECT = 'chat.user.alice.request.user.site-A.subscription.list'

function fakeNats(request: unknown) {
  return {
    user: { account: 'alice', siteId: 'site-A' },
    request,
  } as unknown as Nats
}

/** Minimal Subscription-shaped row — only the fields fetchSidebarBuckets reads. */
function sub(roomId: string, over: Partial<DMSubscription> = {}): DMSubscription {
  return {
    roomId,
    roomType: 'channel',
    siteId: 'site-A',
    name: roomId,
    ...over,
  } as DMSubscription
}

/** Build a request stub that dispatches by the {type, favorite} discriminator
 *  and pages by the {offset, limit} in the body. `pagesByKey` maps a bucket
 *  key to the ordered list of page results that bucket should return. */
function pagingRequest(
  pagesByKey: Record<string, Array<{ subscriptions: DMSubscription[]; hasMore: boolean }>>,
) {
  return vi.fn(async (_subject: string, body: Record<string, unknown>) => {
    const key = body.favorite ? 'current' : (body.type as string)
    const pages = pagesByKey[key] ?? [{ subscriptions: [], hasMore: false }]
    const offset = (body.offset as number) ?? 0
    const idx = Math.floor(offset / PAGE_LIMIT)
    return pages[idx] ?? { subscriptions: [], hasMore: false }
  })
}

afterEach(() => {
  vi.restoreAllMocks()
})

describe('fetchSidebarBuckets', () => {
  it('requests each bucket with an explicit first-page offset/limit', async () => {
    const request = pagingRequest({
      current: [{ subscriptions: [sub('r1')], hasMore: false }],
      apps: [{ subscriptions: [sub('a1', { roomType: 'botDM' })], hasMore: false }],
      rooms: [{ subscriptions: [sub('r1')], hasMore: false }],
    })

    await fetchSidebarBuckets(fakeNats(request))

    expect(request).toHaveBeenCalledWith(SUBJECT, {
      type: 'current',
      favorite: true,
      offset: 0,
      limit: PAGE_LIMIT,
    })
    expect(request).toHaveBeenCalledWith(SUBJECT, { type: 'apps', offset: 0, limit: PAGE_LIMIT })
    expect(request).toHaveBeenCalledWith(SUBJECT, { type: 'rooms', offset: 0, limit: PAGE_LIMIT })
  })

  it('follows hasMore across pages and returns EVERY subscription', async () => {
    // rooms bucket spans three pages; the last has hasMore:false.
    const page0 = { subscriptions: [sub('r1'), sub('r2')], hasMore: true }
    const page1 = { subscriptions: [sub('r3'), sub('r4')], hasMore: true }
    const page2 = { subscriptions: [sub('r5')], hasMore: false }
    const request = pagingRequest({ rooms: [page0, page1, page2] })

    const buckets = await fetchSidebarBuckets(fakeNats(request))

    expect(buckets.channelDmIds).toEqual(['r1', 'r2', 'r3', 'r4', 'r5'])
    expect(Object.keys(buckets.subscriptions).sort()).toEqual(['r1', 'r2', 'r3', 'r4', 'r5'])
    expect(buckets.rooms.map((r) => r.id).sort()).toEqual(['r1', 'r2', 'r3', 'r4', 'r5'])
  })

  it('advances offset by the requested limit, not by returned row count', async () => {
    // Page 0 returns fewer rows than PAGE_LIMIT (server dropped cross-site
    // soft-deleted rows post-slice) yet still signals hasMore. Offset must
    // advance by PAGE_LIMIT so the next page aligns with the query window.
    const request = pagingRequest({
      rooms: [
        { subscriptions: [sub('r1')], hasMore: true },
        { subscriptions: [sub('r2')], hasMore: false },
      ],
    })

    const buckets = await fetchSidebarBuckets(fakeNats(request))

    expect(buckets.channelDmIds).toEqual(['r1', 'r2'])
    expect(request).toHaveBeenCalledWith(SUBJECT, { type: 'rooms', offset: 0, limit: PAGE_LIMIT })
    expect(request).toHaveBeenCalledWith(SUBJECT, {
      type: 'rooms',
      offset: PAGE_LIMIT,
      limit: PAGE_LIMIT,
    })
  })

  it('degrades a failing bucket to empty without dropping the others', async () => {
    const request = vi.fn(async (_subject: string, body: Record<string, unknown>) => {
      if (body.type === 'apps') throw new Error('apps bucket boom')
      const key = body.favorite ? 'current' : (body.type as string)
      if (key === 'current') return { subscriptions: [sub('fav1')], hasMore: false }
      return { subscriptions: [sub('r1')], hasMore: false }
    })
    vi.spyOn(console, 'warn').mockImplementation(() => {})

    const buckets = await fetchSidebarBuckets(fakeNats(request))

    expect(buckets.favoriteIds).toEqual(['fav1'])
    expect(buckets.appIds).toEqual([])
    expect(buckets.channelDmIds).toEqual(['r1'])
  })

  it('degrades a bucket whose LATER page fails, keeping the pages already fetched', async () => {
    let roomsCall = 0
    const request = vi.fn(async (_subject: string, body: Record<string, unknown>) => {
      const key = body.favorite ? 'current' : (body.type as string)
      if (key !== 'rooms') return { subscriptions: [], hasMore: false }
      roomsCall += 1
      if (roomsCall === 1) return { subscriptions: [sub('r1')], hasMore: true }
      throw new Error('page 2 boom')
    })
    vi.spyOn(console, 'warn').mockImplementation(() => {})

    const buckets = await fetchSidebarBuckets(fakeNats(request))

    // The first page's rows survive even though the second page failed.
    expect(buckets.channelDmIds).toEqual(['r1'])
  })

  it('dedupes a roomId that appears in more than one bucket (favorite wins first-seen)', async () => {
    const request = pagingRequest({
      current: [{ subscriptions: [sub('shared', { name: 'fav-name' })], hasMore: false }],
      rooms: [{ subscriptions: [sub('shared', { name: 'room-name' })], hasMore: false }],
    })

    const buckets = await fetchSidebarBuckets(fakeNats(request))

    expect(buckets.favoriteIds).toEqual(['shared'])
    expect(buckets.channelDmIds).toEqual(['shared'])
    // Only one Room record is derived (first-seen), so the sidebar renders once.
    expect(buckets.rooms).toHaveLength(1)
  })

  it('stops after MAX_PAGES and warns when hasMore never clears (no infinite loop)', async () => {
    // A bucket that always claims another page follows.
    const request = vi.fn(async (_subject: string, body: Record<string, unknown>) => {
      const key = body.favorite ? 'current' : (body.type as string)
      if (key !== 'rooms') return { subscriptions: [], hasMore: false }
      const offset = (body.offset as number) ?? 0
      return { subscriptions: [sub(`r${offset}`)], hasMore: true }
    })
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})

    const buckets = await fetchSidebarBuckets(fakeNats(request))

    // rooms fetched exactly MAX_PAGES times, then bailed.
    const roomsCalls = request.mock.calls.filter((c) => (c[1] as { type?: string }).type === 'rooms')
    expect(roomsCalls).toHaveLength(MAX_PAGES)
    expect(buckets.channelDmIds).toHaveLength(MAX_PAGES)
    expect(warn).toHaveBeenCalled()
  })
})
