import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import {
  CACHE_KEY,
  QUOTA_RETRY_LIMIT,
  clearSubscriptionCache,
  loadSubscriptionCache,
  saveSubscriptionCache,
} from './subscriptionCache'

const USER = { account: 'alice', siteId: 'site-a' }

function sub(roomId, overrides = {}) {
  return {
    id: `s-${roomId}`,
    u: { id: 'u1', account: 'alice' },
    roomId,
    siteId: 'site-a',
    roles: ['member'],
    name: `room ${roomId}`,
    roomType: 'channel',
    joinedAt: '2026-08-01T00:00:00Z',
    hasMention: false,
    alert: true,
    ...overrides,
  }
}

function payload(overrides = {}) {
  return {
    subscriptions: { r1: sub('r1') },
    favoriteIds: ['r1'],
    appIds: [],
    channelDmIds: ['r1'],
    chatlist: { sectionOrder: ['favorites'], sections: {}, sortMode: 'recent' },
    ...overrides,
  }
}

describe('subscriptionCache', () => {
  beforeEach(() => {
    window.localStorage.clear()
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('round-trips the persisted slices', () => {
    saveSubscriptionCache(USER, payload())
    expect(loadSubscriptionCache(USER)).toEqual(payload())
  })

  it('stores the bucket ids as arrays when the caller passes Sets', () => {
    // state.favoriteIds & friends are Sets, and JSON.stringify(new Set([...]))
    // is "{}" — persisting one verbatim would hand hydration a non-iterable.
    saveSubscriptionCache(USER, {
      ...payload(),
      favoriteIds: new Set(['r1']),
      appIds: new Set(),
      channelDmIds: new Set(['r1', 'r2']),
    })
    const loaded = loadSubscriptionCache(USER)
    expect(loaded.favoriteIds).toEqual(['r1'])
    expect(loaded.appIds).toEqual([])
    expect(loaded.channelDmIds.sort()).toEqual(['r1', 'r2'])
  })

  it('strips room.privateKey while keeping the rest of room', () => {
    saveSubscriptionCache(
      USER,
      payload({
        subscriptions: {
          r1: sub('r1', {
            room: { userCount: 4, keyVersion: 2, crossSite: false, privateKey: 'SECRET' },
          }),
        },
      }),
    )
    const room = loadSubscriptionCache(USER).subscriptions.r1.room
    expect(room).toEqual({ userCount: 4, keyVersion: 2, crossSite: false })
    expect(window.localStorage.getItem(CACHE_KEY)).not.toContain('SECRET')
  })

  it('returns null when nothing is stored', () => {
    expect(loadSubscriptionCache(USER)).toBeNull()
  })

  it('returns null when the schema version differs', () => {
    saveSubscriptionCache(USER, payload())
    const stored = JSON.parse(window.localStorage.getItem(CACHE_KEY))
    window.localStorage.setItem(CACHE_KEY, JSON.stringify({ ...stored, v: 999 }))
    expect(loadSubscriptionCache(USER)).toBeNull()
  })

  it('returns null for a different account', () => {
    saveSubscriptionCache(USER, payload())
    expect(loadSubscriptionCache({ account: 'bob', siteId: 'site-a' })).toBeNull()
  })

  it('returns null for the same account on a different site', () => {
    saveSubscriptionCache(USER, payload())
    expect(loadSubscriptionCache({ account: 'alice', siteId: 'site-b' })).toBeNull()
  })

  it('returns null for malformed JSON', () => {
    window.localStorage.setItem(CACHE_KEY, '{not json')
    expect(loadSubscriptionCache(USER)).toBeNull()
  })

  it('returns null when the record has no subscriptions map', () => {
    window.localStorage.setItem(
      CACHE_KEY,
      JSON.stringify({ v: 1, account: 'alice', siteId: 'site-a', subscriptions: null }),
    )
    expect(loadSubscriptionCache(USER)).toBeNull()
  })

  it('falls back to empty buckets when a stored record holds non-array ids', () => {
    // localStorage is user-editable; a bucket that isn't an array would
    // otherwise reach `new Set(...)` and throw during the first render.
    saveSubscriptionCache(USER, payload())
    const stored = JSON.parse(window.localStorage.getItem(CACHE_KEY))
    window.localStorage.setItem(CACHE_KEY, JSON.stringify({ ...stored, favoriteIds: { r1: true } }))
    expect(loadSubscriptionCache(USER).favoriteIds).toEqual([])
  })

  it('clearSubscriptionCache removes the record', () => {
    saveSubscriptionCache(USER, payload())
    clearSubscriptionCache()
    expect(window.localStorage.getItem(CACHE_KEY)).toBeNull()
  })

  it('retries a quota failure with only the most recently active rooms', () => {
    const subscriptions = {}
    for (let i = 0; i < QUOTA_RETRY_LIMIT + 5; i++) {
      subscriptions[`r${i}`] = sub(`r${i}`, {
        room: { lastMsgAt: new Date(1_700_000_000_000 + i * 1000).toISOString() },
      })
    }
    const quotaErr = new Error('quota')
    quotaErr.name = 'QuotaExceededError'
    const setItem = vi
      .spyOn(Storage.prototype, 'setItem')
      .mockImplementationOnce(() => {
        throw quotaErr
      })
    saveSubscriptionCache(USER, payload({ subscriptions }))
    setItem.mockRestore()

    const loaded = loadSubscriptionCache(USER)
    expect(Object.keys(loaded.subscriptions)).toHaveLength(QUOTA_RETRY_LIMIT)
    // The five oldest rooms are the ones dropped.
    expect(loaded.subscriptions.r0).toBeUndefined()
    expect(loaded.subscriptions[`r${QUOTA_RETRY_LIMIT + 4}`]).toBeDefined()
  })

  it('clears the record when even the trimmed retry exceeds quota', () => {
    saveSubscriptionCache(USER, payload())
    const quotaErr = new Error('quota')
    quotaErr.name = 'QuotaExceededError'
    vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => {
      throw quotaErr
    })
    expect(() => saveSubscriptionCache(USER, payload())).not.toThrow()
    vi.restoreAllMocks()
    expect(window.localStorage.getItem(CACHE_KEY)).toBeNull()
  })

  it('survives storage that throws on read', () => {
    vi.spyOn(Storage.prototype, 'getItem').mockImplementation(() => {
      throw new Error('storage disabled')
    })
    expect(loadSubscriptionCache(USER)).toBeNull()
  })

  it('survives storage that throws on clear', () => {
    vi.spyOn(Storage.prototype, 'removeItem').mockImplementation(() => {
      throw new Error('storage disabled')
    })
    expect(() => clearSubscriptionCache()).not.toThrow()
  })

  it('does not write without an identified user', () => {
    saveSubscriptionCache(null, payload())
    expect(window.localStorage.getItem(CACHE_KEY)).toBeNull()
  })
})
