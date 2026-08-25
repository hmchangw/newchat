import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import { NatsContext } from '../NatsContext/NatsContext'
import { RoomEventsProvider, useRoomSummaries, useSidebarSections } from './RoomEventsContext'
import { CACHE_KEY, loadSubscriptionCache, saveSubscriptionCache } from '@/lib/subscriptionCache'

vi.mock('@/context/RoomKeysContext', () => ({
  useRoomKeys: () => ({ decrypt: async () => null, hasKey: () => false, ensureKey: async () => false, seedKeys: () => {} }),
}))

const USER = { account: 'alice', siteId: 'site-A' }

function sub(roomId, over = {}) {
  return {
    id: `sub-${roomId}`,
    u: { id: 'u1', account: 'alice' },
    roomId,
    siteId: 'site-A',
    roles: ['member'],
    name: `name-${roomId}`,
    roomType: 'channel',
    joinedAt: '2026-01-01T00:00:00Z',
    hasMention: false,
    alert: false,
    room: { userCount: 3, lastMsgAt: '2026-08-01T00:00:00Z', crossSite: false },
    ...over,
  }
}

/** Seed the cache the way a previous session would have left it. */
function seedCache(subscriptions, over = {}) {
  const ids = Object.keys(subscriptions)
  saveSubscriptionCache(USER, {
    subscriptions,
    favoriteIds: [],
    appIds: [],
    channelDmIds: ids,
    chatlist: undefined,
    ...over,
  })
}

function mockNats({ request, subscribe } = {}) {
  return {
    connected: true,
    user: USER,
    error: null,
    connect: vi.fn(),
    request: request ?? vi.fn().mockResolvedValue({ subscriptions: [], hasMore: false }),
    publish: vi.fn(),
    subscribe: subscribe ?? vi.fn().mockReturnValue({ unsubscribe: vi.fn() }),
    disconnect: vi.fn(),
  }
}

function Probe() {
  const { summaries, error } = useRoomSummaries()
  const sections = useSidebarSections()
  const previews = sections
    .flatMap((s) => s.rooms)
    .map((r) => `${r.id}:${r.preview?.text ?? ''}`)
    .sort()
    .join(',')
  return (
    <div>
      <div data-testid="ids">{summaries.map((r) => r.id).sort().join(',')}</div>
      <div data-testid="previews">{previews}</div>
      <div data-testid="error">{error ?? ''}</div>
    </div>
  )
}

function wrap(nats) {
  return (
    <NatsContext.Provider value={nats}>
      <RoomEventsProvider>
        <Probe />
      </RoomEventsProvider>
    </NatsContext.Provider>
  )
}

/** A request stub whose subscription.list buckets each behave per `byBucket`
 *  — either a subscription array or an Error to reject with. */
function bucketRequest(byBucket) {
  return vi.fn(async (subject, body) => {
    if (!subject.endsWith('.subscription.list')) return { messages: [] }
    const key = body.favorite ? 'favorites' : body.type === 'apps' ? 'apps' : 'rooms'
    const outcome = byBucket[key] ?? []
    if (outcome instanceof Error) throw outcome
    return { subscriptions: outcome, hasMore: false }
  })
}

describe('RoomEventsProvider: subscription cache', () => {
  beforeEach(() => {
    window.localStorage.clear()
    vi.clearAllMocks()
    vi.spyOn(console, 'warn').mockImplementation(() => {})
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('paints cached rooms on the very first render', () => {
    seedCache({ r1: sub('r1'), r2: sub('r2') })
    render(wrap(mockNats()))
    // Synchronous assertion — no await. A hydration that only lands in an
    // effect would leave this empty for a frame, which is the flash we're
    // removing.
    expect(screen.getByTestId('ids').textContent).toBe('r1,r2')
  })

  it('starts empty when the cache holds another account', () => {
    saveSubscriptionCache({ account: 'bob', siteId: 'site-A' }, {
      subscriptions: { r1: sub('r1') },
      favoriteIds: [],
      appIds: [],
      channelDmIds: ['r1'],
    })
    render(wrap(mockNats()))
    expect(screen.getByTestId('ids').textContent).toBe('')
  })

  it('opens live channel subscriptions for cached rooms', async () => {
    seedCache({ r1: sub('r1') })
    const subscribe = vi.fn().mockReturnValue({ unsubscribe: vi.fn() })
    render(wrap(mockNats({ subscribe })))
    await waitFor(() => {
      const subjects = subscribe.mock.calls.map((c) => c[0])
      expect(subjects).toContain('chat.local.room.r1.event')
    })
  })

  it('keeps cached rooms when every bootstrap bucket fails', async () => {
    seedCache({ r1: sub('r1'), r2: sub('r2') })
    const err = new Error('disconnected')
    const request = bucketRequest({ favorites: err, apps: err, rooms: err })
    render(wrap(mockNats({ request })))
    await waitFor(() => expect(request).toHaveBeenCalledTimes(3))
    await waitFor(() => expect(screen.getByTestId('error').textContent).not.toBe(''))
    expect(screen.getByTestId('ids').textContent).toBe('r1,r2')
  })

  it('keeps the cached rooms of a bucket that failed while applying the ones that succeeded', async () => {
    seedCache({ r1: sub('r1'), app1: sub('app1', { roomType: 'botDM' }) })
    const request = bucketRequest({
      favorites: [],
      apps: new Error('apps bucket died'),
      rooms: [sub('r1'), sub('r3')],
    })
    render(wrap(mockNats({ request })))
    await waitFor(() => expect(screen.getByTestId('ids').textContent).toBe('app1,r1,r3'))
  })

  it('drops rooms the user left when every bucket succeeds', async () => {
    seedCache({ r1: sub('r1'), gone: sub('gone') })
    const request = bucketRequest({ favorites: [], apps: [], rooms: [sub('r1')] })
    render(wrap(mockNats({ request })))
    await waitFor(() => expect(screen.getByTestId('ids').textContent).toBe('r1'))
  })

  it('persists the fetched subscriptions for the next session', async () => {
    const request = bucketRequest({ favorites: [], apps: [], rooms: [sub('r1')] })
    render(wrap(mockNats({ request })))
    await waitFor(
      () => {
        expect(Object.keys(loadSubscriptionCache(USER)?.subscriptions ?? {})).toEqual(['r1'])
      },
      { timeout: 3000 },
    )
  })

  it('does not blank the cache when the provider tears down', async () => {
    const request = bucketRequest({ favorites: [], apps: [], rooms: [sub('r1')] })
    const { unmount } = render(wrap(mockNats({ request })))
    await waitFor(() => expect(loadSubscriptionCache(USER)).not.toBeNull(), { timeout: 3000 })
    unmount()
    await new Promise((r) => setTimeout(r, 1100))
    expect(Object.keys(loadSubscriptionCache(USER)?.subscriptions ?? {})).toEqual(['r1'])
  })

  it('replaces a cached preview with the fresher one the bootstrap fetched', async () => {
    // Re-login regression: hydration seeds previews from the PREVIOUS session's
    // cached previewMessage, so the sidebar must still take the newer snippet
    // `subscription.list` returns rather than keeping the stale one.
    const previewMessage = (messageId, content, createdAt) => ({
      messageId,
      sender: { account: 'bob', displayName: 'Bob Lin' },
      content,
      createdAt,
    })
    seedCache(
      {
        r1: sub('r1', {
          room: {
            userCount: 3,
            lastMsgAt: '2026-08-01T00:00:00Z',
            crossSite: false,
            previewMessage: previewMessage('m-old', 'last session', '2026-08-01T00:00:00Z'),
          },
        }),
      },
      // What last session's write-through left behind for that same row.
      {
        previews: {
          r1: {
            messageId: 'm-old', senderName: 'Bob Lin', text: 'last session',
            createdAt: '2026-08-01T00:00:00Z',
          },
        },
      },
    )
    render(wrap(mockNats()))
    expect(screen.getByTestId('previews').textContent).toBe('r1:last session')

    const fresh = sub('r1', {
      room: {
        userCount: 3,
        lastMsgAt: '2026-08-02T00:00:00Z',
        crossSite: false,
        previewMessage: previewMessage('m-new', 'newest message', '2026-08-02T00:00:00Z'),
      },
    })
    const request = bucketRequest({ favorites: [], apps: [], rooms: [fresh] })
    render(wrap(mockNats({ request })))
    await waitFor(() =>
      expect(screen.getAllByTestId('previews').at(-1).textContent).toBe('r1:newest message'),
    )
  })

  it('paints last session\'s newest snippet on the very first render', async () => {
    // The cached subscription's previewMessage is the bootstrap snapshot from
    // the previous session; the persisted preview is where its live messages
    // landed. The warm paint must show the latter.
    seedCache(
      {
        r1: sub('r1', {
          room: {
            userCount: 3,
            lastMsgAt: '2026-08-01T00:00:00Z',
            crossSite: false,
            previewMessage: {
              messageId: 'm-old',
              sender: { account: 'bob', displayName: 'Bob Lin' },
              content: 'bootstrap snapshot',
              createdAt: '2026-08-01T00:00:00Z',
            },
          },
        }),
      },
      {
        previews: {
          r1: {
            messageId: 'm-live',
            senderName: 'Bob Lin',
            text: 'arrived while I was logged in',
            createdAt: '2026-08-02T00:00:00Z',
          },
        },
      },
    )
    render(wrap(mockNats()))
    expect(screen.getByTestId('previews').textContent).toBe('r1:arrived while I was logged in')
  })

  it('does not resurrect a preview whose message was deleted last session', () => {
    // The delete broadcast cleared state.previews, but the cached subscription
    // row still names the deleted message as its previewMessage.
    seedCache(
      {
        r1: sub('r1', {
          room: {
            userCount: 3,
            lastMsgAt: '2026-08-01T00:00:00Z',
            crossSite: false,
            previewMessage: {
              messageId: 'm-deleted',
              sender: { account: 'bob', displayName: 'Bob Lin' },
              content: 'this was deleted',
              createdAt: '2026-08-01T00:00:00Z',
            },
          },
        }),
      },
      { previews: {} },
    )
    render(wrap(mockNats()))
    expect(screen.getByTestId('previews').textContent).toBe('r1:')
  })

  it('persists the previews it holds for the next session', async () => {
    const request = bucketRequest({
      favorites: [],
      apps: [],
      rooms: [sub('r1', {
        room: {
          userCount: 3,
          lastMsgAt: '2026-08-02T00:00:00Z',
          crossSite: false,
          previewMessage: {
            messageId: 'm-new',
            sender: { account: 'bob', displayName: 'Bob Lin' },
            content: 'newest message',
            createdAt: '2026-08-02T00:00:00Z',
          },
        },
      })],
    })
    render(wrap(mockNats({ request })))
    await waitFor(
      () => expect(loadSubscriptionCache(USER)?.previews?.r1?.text).toBe('newest message'),
      { timeout: 3000 },
    )
  })

  it('writes nothing when storage is unavailable', async () => {
    const setItem = vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => {
      throw new Error('storage disabled')
    })
    const request = bucketRequest({ favorites: [], apps: [], rooms: [sub('r1')] })
    render(wrap(mockNats({ request })))
    await waitFor(() => expect(screen.getByTestId('ids').textContent).toBe('r1'))
    // Let the write debounce elapse so the failing write actually happens.
    await waitFor(() => expect(setItem).toHaveBeenCalled(), { timeout: 3000 })
    expect(window.localStorage.getItem(CACHE_KEY)).toBeNull()
  })
})
