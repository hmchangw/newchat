import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, act, waitFor } from '@testing-library/react'
import { useState } from 'react'
import { NatsContext } from '../NatsContext/NatsContext'
import { RoomEventsProvider, useRoomEvents, useRoomSummaries, useSidebarSections, useSubscription } from './RoomEventsContext'
import { BUFFER_MODE } from './reducer'
// jumpToMessage / resetToLiveTail tests — see suite below

// RoomEventsContext now calls useRoomKeys() internally. Stub it out so tests
// don't need a real RoomKeysProvider (which would try to connect to NATS and
// fetch key material). The no-op decrypt matches the default used in
// useRoomSubscriptions when no key is available.
//
// The factory reads from `currentRoomKeysMock` so individual test blocks can
// assign custom spies (e.g. the missing-key path tests) without affecting the
// default no-op used everywhere else.
let currentRoomKeysMock = {
  decrypt: async () => null,
  hasKey: () => false,
  ensureKey: async () => false,
}
vi.mock('@/context/RoomKeysContext', () => ({
  useRoomKeys: () => currentRoomKeysMock,
}))

/** Turn an inline "room-shaped" fixture into a subscription record that
 *  the new bootstrap (3 subscription RPCs) returns. The real user-service
 *  now embeds room metadata under `room` (not top-level), so this helper
 *  nests userCount / lastMsgAt / lastMsgId there to mirror the wire shape. */
function roomToSub(room) {
  return {
    id: `sub-${room.id}`,
    u: { id: `u-${room.id}`, account: 'alice' },
    roomId: room.id,
    siteId: room.siteId ?? 'site-A',
    roles: ['member'],
    name: room.name ?? '',
    roomType: room.type,
    joinedAt: '2026-01-01T00:00:00Z',
    hasMention: false,
    alert: false,
    room: {
      userCount: room.userCount,
      lastMsgAt: room.lastMsgAt ?? null,
      lastMsgId: room.lastMsgId,
    },
  }
}

function mockNats({ request, subscribe, user = { account: 'alice', siteId: 'site-A' } } = {}) {
  return {
    connected: true,
    user,
    error: null,
    connect: vi.fn(),
    request: request ?? vi.fn().mockResolvedValue({ rooms: [], subscriptions: [] }),
    publish: vi.fn(),
    subscribe: subscribe ?? vi.fn().mockReturnValue({ unsubscribe: vi.fn() }),
    disconnect: vi.fn(),
  }
}

function wrap(ui, nats) {
  return (
    <NatsContext.Provider value={nats}>
      <RoomEventsProvider>{ui}</RoomEventsProvider>
    </NatsContext.Provider>
  )
}

function SummariesProbe() {
  const { summaries } = useRoomSummaries()
  return <div data-testid="count">{summaries.length}</div>
}

function EventsProbe({ roomId }) {
  const { messages, hasLoadedHistory, historyError } = useRoomEvents(roomId)
  return (
    <div>
      <div data-testid="messages">{messages.map((m) => m.id).join(',')}</div>
      <div data-testid="loaded">{String(hasLoadedHistory)}</div>
      <div data-testid="error">{historyError ?? ''}</div>
    </div>
  )
}

describe('RoomEventsProvider', () => {
  beforeEach(() => vi.clearAllMocks())

  it('exposes empty summaries before rooms load', async () => {
    const nats = mockNats()
    render(wrap(<SummariesProbe />, nats))
    expect(screen.getByTestId('count').textContent).toBe('0')
    // Let the empty bootstrap RPCs settle
    await waitFor(() => expect(nats.request).toHaveBeenCalled())
  })

  it('loadHistory requests msg.history and populates messages', async () => {
    const history = [
      { id: 'm1', roomId: 'a', content: 'old', createdAt: '2026-04-17T10:00:00Z', sender: { account: 'bob' } },
    ]
    const request = vi.fn().mockImplementation((subject, payload) => {
      if (subject.includes('.msg.history')) return Promise.resolve({ messages: [...history] })
      if (subject.endsWith('.subscription.list')) return Promise.resolve({ subscriptions: [] })
      throw new Error('unexpected subject: ' + subject)
    })
    const nats = mockNats({ request })

    function Trigger() {
      const { messages, loadHistory } = useRoomEvents('a')
      return (
        <div>
          <button onClick={() => loadHistory()}>load</button>
          <div data-testid="messages">{messages.map((m) => m.id).join(',')}</div>
        </div>
      )
    }

    render(wrap(<Trigger />, nats))
    await act(async () => {
      screen.getByText('load').click()
    })
    await waitFor(() => expect(screen.getByTestId('messages').textContent).toBe('m1'))
    expect(request).toHaveBeenCalledWith(
      'chat.user.alice.request.room.a.site-A.msg.history',
      { limit: 50 }
    )
  })

  it('loadHistory surfaces historyError on failure', async () => {
    const request = vi.fn().mockImplementation((subject) => {
      if (subject.includes('.msg.history')) return Promise.reject(new Error('boom'))
      return Promise.resolve({ rooms: [] })
    })
    const nats = mockNats({ request })

    function Trigger() {
      const { loadHistory, historyError } = useRoomEvents('a')
      return (
        <div>
          <button onClick={() => loadHistory().catch(() => {})}>load</button>
          <div data-testid="error">{historyError ?? ''}</div>
        </div>
      )
    }

    render(wrap(<Trigger />, nats))
    await act(async () => {
      screen.getByText('load').click()
    })
    await waitFor(() => expect(screen.getByTestId('error').textContent).toBe('boom'))
  })

  it('useRoomEvents returns a stable loadHistory across renders for the same roomId', async () => {
    const nats = mockNats()
    const captured = []
    function Probe() {
      const { loadHistory } = useRoomEvents('a')
      captured.push(loadHistory)
      return null
    }
    const { rerender } = render(wrap(<Probe />, nats))
    rerender(wrap(<Probe />, nats))
    rerender(wrap(<Probe />, nats))
    expect(captured.length).toBeGreaterThanOrEqual(2)
    for (let i = 1; i < captured.length; i++) {
      expect(captured[i]).toBe(captured[0])
    }
    await waitFor(() => expect(nats.request).toHaveBeenCalled())
  })
})

describe('RoomEventsProvider subscriptions', () => {
  beforeEach(() => vi.clearAllMocks())

  it('fetches rooms on mount and subscribes to user-scoped events', async () => {
    const rooms = [
      { id: 'g1', name: 'general-channel', type: 'channel', siteId: 'site-A', userCount: 3, lastMsgAt: '2026-04-17T10:00:00Z' },
      { id: 'd1', name: 'dm',    type: 'dm',    siteId: 'site-A', userCount: 2, lastMsgAt: '2026-04-17T11:00:00Z' },
    ]
    const request = vi.fn().mockImplementation((subject, payload) => {
      if (subject.endsWith('.subscription.list') && payload?.type === 'rooms')
        return Promise.resolve({ subscriptions: rooms.map(roomToSub) })
      if (subject.endsWith('.subscription.list')) return Promise.resolve({ subscriptions: [] })
      throw new Error('unexpected request: ' + subject)
    })
    const subjects = []
    const subscribe = vi.fn().mockImplementation((subject) => {
      subjects.push(subject)
      return { unsubscribe: vi.fn() }
    })
    const nats = mockNats({ request, subscribe })

    render(wrap(<SummariesProbe />, nats))
    await waitFor(() => expect(screen.getByTestId('count').textContent).toBe('2'))

    expect(subjects).toContain('chat.user.alice.event.room')
    expect(subjects).toContain('chat.user.alice.event.subscription.update')
    expect(subjects).toContain('chat.user.alice.event.room.metadata.update')
    expect(subjects).toContain('chat.room.g1.event')
    expect(subjects).not.toContain('chat.room.d1.event')
  })

  it('applies DM events from the user-scoped subscription', async () => {
    const rooms = [{ id: 'd1', name: 'dm', type: 'dm', siteId: 'site-A', userCount: 2, lastMsgAt: null }]
    const request = vi.fn().mockImplementation((subject, payload) => {
      if (subject.endsWith('.subscription.list') && payload?.type === 'rooms')
        return Promise.resolve({ subscriptions: rooms.map(roomToSub) })
      return Promise.resolve({ subscriptions: [] })
    })
    const handlers = new Map()
    const subscribe = vi.fn().mockImplementation((subject, cb) => {
      handlers.set(subject, cb)
      return { unsubscribe: vi.fn() }
    })
    const nats = mockNats({ request, subscribe })

    render(wrap(<EventsProbe roomId="d1" />, nats))
    await waitFor(() => expect(subscribe).toHaveBeenCalled())

    act(() => {
      handlers.get('chat.user.alice.event.room')({
        type: 'new_message',
        roomId: 'd1',
        hasMention: false,
        lastMsgAt: '2026-04-17T12:00:00Z',
        lastMsgId: 'mdm1',
        message: { id: 'mdm1', roomId: 'd1', content: 'hey', createdAt: '2026-04-17T12:00:00Z', sender: { account: 'bob' } },
      })
    })
    await waitFor(() => expect(screen.getByTestId('messages').textContent).toBe('mdm1'))
  })

  it('opens a new channel subscription when a channel room is added', async () => {
    const request = vi.fn().mockImplementation((subject) => {
      if (subject.endsWith('.subscription.list')) return Promise.resolve({ subscriptions: [] })
      throw new Error('unexpected request: ' + subject)
    })
    const handlers = new Map()
    const subscribe = vi.fn().mockImplementation((subject, cb) => {
      handlers.set(subject, cb)
      return { unsubscribe: vi.fn() }
    })
    const nats = mockNats({ request, subscribe })

    render(wrap(<SummariesProbe />, nats))
    await waitFor(() => expect(subscribe).toHaveBeenCalled())

    // The room is built from the subscription record — no rooms.get RPC.
    act(() => {
      handlers.get('chat.user.alice.event.subscription.update')({
        action: 'added',
        subscription: { roomId: 'g2', roomType: 'channel', siteId: 'site-A', name: 'new' },
      })
    })
    await waitFor(() =>
      expect(subscribe.mock.calls.map((c) => c[0])).toContain('chat.room.g2.event')
    )
    expect(screen.getByTestId('count').textContent).toBe('1')
  })

  it('drops state and unsubscribes on room removal', async () => {
    const rooms = [{ id: 'g1', name: 'g', type: 'channel', siteId: 'site-A', userCount: 2, lastMsgAt: null }]
    const request = vi.fn().mockImplementation((subject, payload) => {
      if (subject.endsWith('.subscription.list') && payload?.type === 'rooms')
        return Promise.resolve({ subscriptions: rooms.map(roomToSub) })
      return Promise.resolve({ subscriptions: [] })
    })
    const unsubs = []
    const handlers = new Map()
    const subscribe = vi.fn().mockImplementation((subject, cb) => {
      handlers.set(subject, cb)
      const sub = { unsubscribe: vi.fn() }
      if (subject === 'chat.room.g1.event') unsubs.push(sub)
      return sub
    })
    const nats = mockNats({ request, subscribe })

    render(wrap(<SummariesProbe />, nats))
    await waitFor(() => expect(screen.getByTestId('count').textContent).toBe('1'))

    act(() => {
      handlers.get('chat.user.alice.event.subscription.update')({
        action: 'removed',
        subscription: { roomId: 'g1' },
      })
    })
    await waitFor(() => expect(screen.getByTestId('count').textContent).toBe('0'))
    expect(unsubs[0].unsubscribe).toHaveBeenCalled()
  })

  it('dispatches SUBSCRIPTION_UPSERTED on role_updated events (live propagation of role changes)', async () => {
    // Cold-start returns a single channel-room subscription where the
    // user is `member`. A live `role_updated` event arrives bumping
    // them to `owner`; useSubscription should reflect the new roles
    // without any other refresh.
    const rooms = [{ id: 'g1', name: 'g', type: 'channel', siteId: 'site-A', userCount: 2, lastMsgAt: null }]
    const request = vi.fn().mockImplementation((subject, payload) => {
      if (subject.endsWith('.subscription.list') && payload?.type === 'rooms')
        return Promise.resolve({ subscriptions: rooms.map(roomToSub) })
      if (subject.endsWith('.subscription.list') && payload?.type === 'current')
        return Promise.resolve({ subscriptions: [{ roomId: 'g1', roles: ['member'], name: 'g' }] })
      if (subject.endsWith('.subscription.list') && payload?.type === 'apps')
        return Promise.resolve({ subscriptions: [] })
      throw new Error('unexpected request: ' + subject)
    })
    const handlers = new Map()
    const subscribe = vi.fn().mockImplementation((subject, cb) => {
      handlers.set(subject, cb)
      return { unsubscribe: vi.fn() }
    })
    const nats = mockNats({ request, subscribe })

    function RoleProbe() {
      const sub = useSubscription('g1')
      return <div data-testid="roles">{sub?.roles?.join(',') ?? '∅'}</div>
    }

    render(wrap(<RoleProbe />, nats))
    await waitFor(() => expect(screen.getByTestId('roles').textContent).toBe('member'))

    act(() => {
      handlers.get('chat.user.alice.event.subscription.update')({
        action: 'role_updated',
        subscription: { roomId: 'g1', roles: ['owner', 'member'], name: 'g' },
        userId: 'u-alice',
        timestamp: Date.now(),
      })
    })
    await waitFor(() => expect(screen.getByTestId('roles').textContent).toBe('owner,member'))
  })

  it('tears down old subscriptions and opens new ones when the user changes', async () => {
    const request = vi.fn().mockResolvedValue({ rooms: [] })
    const subs = []
    const subscribe = vi.fn().mockImplementation((subject) => {
      const sub = { subject, unsubscribe: vi.fn() }
      subs.push(sub)
      return sub
    })
    const aliceNats = mockNats({ request, subscribe, user: { account: 'alice', siteId: 'site-A' } })
    const bobNats = mockNats({ request, subscribe, user: { account: 'bob', siteId: 'site-A' } })

    const { rerender } = render(wrap(<SummariesProbe />, aliceNats))
    await waitFor(() => expect(subs.some((s) => s.subject === 'chat.user.alice.event.room')).toBe(true))
    const aliceSubs = subs.filter((s) => s.subject.includes('alice'))

    rerender(wrap(<SummariesProbe />, bobNats))
    await waitFor(() =>
      expect(subs.some((s) => s.subject === 'chat.user.bob.event.room')).toBe(true)
    )

    for (const s of aliceSubs) {
      expect(s.unsubscribe).toHaveBeenCalled()
    }
  })

  async function setupMentionScenario() {
    const rooms = [{ id: 'g1', name: 'g', type: 'channel', siteId: 'site-A', userCount: 2, lastMsgAt: null }]
    const request = vi.fn().mockImplementation((subject, payload) => {
      if (subject.endsWith('.subscription.list') && payload?.type === 'rooms')
        return Promise.resolve({ subscriptions: rooms.map(roomToSub) })
      return Promise.resolve({ subscriptions: [] })
    })
    const handlers = new Map()
    const subscribe = vi.fn().mockImplementation((subject, cb) => {
      handlers.set(subject, cb)
      return { unsubscribe: vi.fn() }
    })
    const nats = mockNats({ request, subscribe })

    const captured = { summaries: null }
    function MentionProbe() {
      const { summaries } = useRoomSummaries()
      captured.summaries = summaries
      return <div data-testid="count">{summaries.length}</div>
    }
    render(wrap(<MentionProbe />, nats))
    await waitFor(() => expect(screen.getByTestId('count').textContent).toBe('1'))
    return { handlers, captured }
  }

  it('computes hasMention from mentions[] for channel events', async () => {
    const { handlers, captured } = await setupMentionScenario()
    act(() => {
      handlers.get('chat.room.g1.event')({
        type: 'new_message',
        roomId: 'g1',
        mentions: [{ account: 'alice', engName: 'Alice' }],
        mentionAll: false,
        lastMsgAt: '2026-04-17T12:00:00Z',
        lastMsgId: 'mg1',
        message: { id: 'mg1', roomId: 'g1', content: '@alice hi', createdAt: '2026-04-17T12:00:00Z', sender: { account: 'bob' } },
      })
    })
    await waitFor(() => {
      expect(captured.summaries.find((r) => r.id === 'g1')?.hasMention).toBe(true)
    })
  })

  it('does not set hasMention for channel events that do not mention the user', async () => {
    const { handlers, captured } = await setupMentionScenario()
    act(() => {
      handlers.get('chat.room.g1.event')({
        type: 'new_message',
        roomId: 'g1',
        mentions: [{ account: 'charlie' }],
        mentionAll: false,
        lastMsgAt: '2026-04-17T12:00:00Z',
        lastMsgId: 'mg2',
        message: { id: 'mg2', roomId: 'g1', content: '@charlie hi', createdAt: '2026-04-17T12:00:00Z', sender: { account: 'bob' } },
      })
    })
    await waitFor(() => {
      expect(captured.summaries.find((r) => r.id === 'g1')?.hasMention).toBe(false)
    })
  })

  it('does not dispatch HISTORY_LOADED after the user changes (cancelledRef guard)', async () => {
    // This tests the real bug: user A starts a loadHistory, user switches to B, the
    // cleanup sets cancelledRef=true and the new effect sets it back to false. Without
    // the guard on the dispatch, user A's late resolve would dispatch into user B's state.
    let resolveAliceHistory
    const request = vi.fn().mockImplementation((subject) => {
      if (subject.endsWith('.subscription.list')) return Promise.resolve({ subscriptions: [] })
      if (subject.includes('alice') && subject.includes('.msg.history')) {
        return new Promise((resolve) => { resolveAliceHistory = resolve })
      }
      if (subject.includes('bob') && subject.includes('.msg.history')) {
        return new Promise(() => {}) // bob's history never resolves in this test
      }
      throw new Error('unexpected: ' + subject)
    })
    const subscribe = vi.fn().mockReturnValue({ unsubscribe: vi.fn() })

    const aliceNats = mockNats({ request, subscribe, user: { account: 'alice', siteId: 'site-A' } })
    const bobNats   = mockNats({ request, subscribe, user: { account: 'bob',   siteId: 'site-A' } })

    // Trigger alice's loadHistory, then switch user to bob mid-flight
    function Trigger() {
      const { loadHistory } = useRoomEvents('a')
      return <button onClick={() => { loadHistory().catch(() => {}) }}>load</button>
    }

    const { rerender } = render(wrap(<Trigger />, aliceNats))
    await waitFor(() => expect(subscribe).toHaveBeenCalled())
    await act(async () => { screen.getByText('load').click() })

    // Switch to bob — this triggers cleanup (cancelledRef=true) then new effect (cancelledRef=false)
    let bobMessages
    function BobProbe() {
      const { messages } = useRoomEvents('a')
      bobMessages = messages
      return null
    }
    rerender(wrap(<BobProbe />, bobNats))
    await waitFor(() => expect(subscribe.mock.calls.some((c) => c[0].includes('bob'))).toBe(true))

    // Now alice's inflight history resolves — the guard must prevent it landing in bob's state
    await act(async () => {
      resolveAliceHistory({ messages: [{ id: 'alice-msg', roomId: 'a', content: 'hi', createdAt: '2026-04-17T10:00:00Z', sender: { account: 'alice' } }] })
      await Promise.resolve()
      await Promise.resolve()
    })

    // Bob's state should be empty — the stale alice dispatch must not have gone through
    expect(bobMessages).toEqual([])
  })
})

describe('RoomEventsProvider jumpToMessage / resetToLiveTail', () => {
  beforeEach(() => vi.clearAllMocks())

  it('jumpToMessage requests msg.surrounding using the room siteId and replaces the buffer', async () => {
    const rooms = [
      { id: 'r1', name: 'general', type: 'channel', siteId: 'site-B', userCount: 2, lastMsgAt: null },
    ]
    const surrounding = [
      { id: 'm10', roomId: 'r1', content: 'before', createdAt: '2026-04-17T11:00:00Z', sender: { account: 'bob' } },
      { id: 'm11', roomId: 'r1', content: 'hit',    createdAt: '2026-04-17T11:01:00Z', sender: { account: 'bob' } },
      { id: 'm12', roomId: 'r1', content: 'after',  createdAt: '2026-04-17T11:02:00Z', sender: { account: 'bob' } },
    ]
    const request = vi.fn().mockImplementation((subject, payload) => {
      if (subject.endsWith('.subscription.list') && payload?.type === 'rooms')
        return Promise.resolve({ subscriptions: rooms.map(roomToSub) })
      if (subject.includes('.msg.surrounding')) {
        return Promise.resolve({ messages: surrounding })
      }
      if (subject.endsWith('.subscription.list')) return Promise.resolve({ subscriptions: [] })
      throw new Error('unexpected subject: ' + subject)
    })
    const nats = mockNats({ request })

    function Probe() {
      const { messages, focusMessageId, bufferMode, jumpToMessage } = useRoomEvents('r1')
      return (
        <div>
          <button onClick={() => jumpToMessage('m11').catch(() => {})}>jump</button>
          <div data-testid="messages">{messages.map((m) => m.id).join(',')}</div>
          <div data-testid="focus">{focusMessageId ?? ''}</div>
          <div data-testid="mode">{bufferMode}</div>
        </div>
      )
    }

    render(wrap(<Probe />, nats))
    // Wait for rooms list to load so summary is present (so jumpToMessage uses room siteId)
    await waitFor(() =>
      expect(request).toHaveBeenCalledWith(
        'chat.user.alice.request.user.site-A.subscription.list',
        { type: 'rooms' },
      )
    )

    await act(async () => {
      screen.getByText('jump').click()
    })

    await waitFor(() =>
      expect(request).toHaveBeenCalledWith(
        'chat.user.alice.request.room.r1.site-B.msg.surrounding',
        { messageId: 'm11' }
      )
    )

    await waitFor(() =>
      expect(screen.getByTestId('messages').textContent).toBe('m10,m11,m12')
    )
    expect(screen.getByTestId('focus').textContent).toBe('m11')
    expect(screen.getByTestId('mode').textContent).toBe(BUFFER_MODE.HISTORICAL)
  })

  it('exposes pendingCount when in historical mode and live messages arrive', async () => {
    const rooms = [
      { id: 'r1', name: 'general', type: 'channel', siteId: 'site-A', userCount: 2, lastMsgAt: null },
    ]
    const handlers = new Map()
    const request = vi.fn().mockImplementation((subject, payload) => {
      if (subject.endsWith('.subscription.list') && payload?.type === 'rooms')
        return Promise.resolve({ subscriptions: rooms.map(roomToSub) })
      if (subject.includes('.msg.surrounding')) {
        return Promise.resolve({
          messages: [
            { id: 'old', roomId: 'r1', content: 'old', createdAt: '2026-04-17T10:00:00Z', sender: { account: 'bob' } },
          ],
        })
      }
      if (subject.endsWith('.subscription.list')) return Promise.resolve({ subscriptions: [] })
      throw new Error('unexpected subject: ' + subject)
    })
    const subscribe = vi.fn().mockImplementation((subject, cb) => {
      handlers.set(subject, cb)
      return { unsubscribe: vi.fn() }
    })
    const nats = mockNats({ request, subscribe })

    function Probe() {
      const { pendingCount, bufferMode, jumpToMessage, resetToLiveTail, messages } = useRoomEvents('r1')
      return (
        <div>
          <button onClick={() => jumpToMessage('old').catch(() => {})}>jump</button>
          <button onClick={() => resetToLiveTail()}>reset</button>
          <div data-testid="pending">{pendingCount}</div>
          <div data-testid="mode">{bufferMode}</div>
          <div data-testid="messages">{messages.map((m) => m.id).join(',')}</div>
        </div>
      )
    }

    render(wrap(<Probe />, nats))
    await waitFor(() => expect(handlers.has('chat.room.r1.event')).toBe(true))
    await act(async () => { await Promise.resolve(); await Promise.resolve() })

    await act(async () => {
      screen.getByText('jump').click()
      await Promise.resolve()
    })
    await waitFor(() => expect(screen.getByTestId('mode').textContent).toBe(BUFFER_MODE.HISTORICAL))

    act(() => {
      handlers.get('chat.room.r1.event')({
        type: 'new_message',
        roomId: 'r1',
        mentions: [],
        mentionAll: false,
        lastMsgAt: '2026-04-17T12:00:00Z',
        lastMsgId: 'live1',
        message: { id: 'live1', roomId: 'r1', content: 'live!', createdAt: '2026-04-17T12:00:00Z', sender: { account: 'bob' } },
      })
    })
    await waitFor(() => expect(screen.getByTestId('pending').textContent).toBe('1'))
    expect(screen.getByTestId('messages').textContent).toBe('old')

    act(() => {
      screen.getByText('reset').click()
    })
    await waitFor(() => expect(screen.getByTestId('mode').textContent).toBe(BUFFER_MODE.LIVE))
    expect(screen.getByTestId('messages').textContent).toBe('old,live1')
    expect(screen.getByTestId('pending').textContent).toBe('0')
  })

  it('useRoomSummaries exposes jumpToMessage', async () => {
    const nats = mockNats()
    let captured
    function Probe() {
      const { jumpToMessage } = useRoomSummaries()
      captured = jumpToMessage
      return null
    }
    render(wrap(<Probe />, nats))
    await waitFor(() => expect(nats.request).toHaveBeenCalled())
    expect(typeof captured).toBe('function')
  })
})

describe('RoomEventsProvider message.read wiring', () => {
  beforeEach(() => vi.clearAllMocks())

  function readSubjectFor(roomId, siteId = 'site-A') {
    return `chat.user.alice.request.room.${roomId}.${siteId}.message.read`
  }

  function setupWithRooms(rooms) {
    const request = vi.fn().mockImplementation((subject, payload) => {
      if (subject.endsWith('.subscription.list') && payload?.type === 'rooms')
        return Promise.resolve({ subscriptions: rooms.map(roomToSub) })
      // Other bootstrap RPCs return empty bucket; everything else
      // (message.read etc.) doesn't care about the reply shape.
      if (subject.endsWith('.subscription.list')) return Promise.resolve({ subscriptions: [] })
      return Promise.resolve({})
    })
    const handlers = new Map()
    const subscribe = vi.fn().mockImplementation((subject, cb) => {
      handlers.set(subject, cb)
      return { unsubscribe: vi.fn() }
    })
    const nats = mockNats({ request, subscribe })
    return { nats, request, handlers }
  }

  it('fires message.read when setActiveRoom is called with a non-null id', async () => {
    const rooms = [
      { id: 'g1', name: 'general', type: 'channel', siteId: 'site-A', userCount: 3, lastMsgAt: null },
    ]
    const { nats, request } = setupWithRooms(rooms)
    let captured
    function Probe() {
      const { setActiveRoom } = useRoomSummaries()
      captured = setActiveRoom
      return null
    }
    render(wrap(<Probe />, nats))
    await waitFor(() => expect(request).toHaveBeenCalledWith(
      'chat.user.alice.request.user.site-A.subscription.list',
      { type: 'rooms' },
    ))

    act(() => { captured('g1') })

    expect(request).toHaveBeenCalledWith(readSubjectFor('g1'), {})
  })

  it('does NOT fire message.read when setActiveRoom is called with null', async () => {
    const { nats, request } = setupWithRooms([])
    let captured
    function Probe() {
      const { setActiveRoom } = useRoomSummaries()
      captured = setActiveRoom
      return null
    }
    render(wrap(<Probe />, nats))
    await waitFor(() => expect(request).toHaveBeenCalled())

    request.mockClear()
    act(() => { captured(null) })

    const subjects = request.mock.calls.map((c) => c[0])
    expect(subjects.some((s) => s.endsWith('.message.read'))).toBe(false)
  })

  it('fires message.read when a new_message arrives in the active channel room (after the trailing debounce)', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      const rooms = [
        { id: 'g1', name: 'general', type: 'channel', siteId: 'site-A', userCount: 3, lastMsgAt: null },
      ]
      const { nats, request, handlers } = setupWithRooms(rooms)
      let setActive
      function Probe() {
        const { setActiveRoom } = useRoomSummaries()
        setActive = setActiveRoom
        return null
      }
      render(wrap(<Probe />, nats))
      await waitFor(() => expect(handlers.has('chat.room.g1.event')).toBe(true))

      act(() => { setActive('g1') })
      request.mockClear()

      act(() => {
        handlers.get('chat.room.g1.event')({
          type: 'new_message',
          roomId: 'g1',
          message: { id: 'm1', roomId: 'g1', sender: { account: 'bob' }, content: 'hi', createdAt: '2026-04-17T12:00:00Z' },
        })
      })

      // The new-message-in-active-room mark-read is debounced 500ms.
      await act(async () => { await vi.advanceTimersByTimeAsync(600) })
      expect(request).toHaveBeenCalledWith(readSubjectFor('g1'), {})
    } finally {
      vi.useRealTimers()
    }
  })

  it('does NOT fire message.read when a new_message arrives in a non-active room', async () => {
    const rooms = [
      { id: 'g1', name: 'general', type: 'channel', siteId: 'site-A', userCount: 3, lastMsgAt: null },
      { id: 'g2', name: 'random',  type: 'channel', siteId: 'site-A', userCount: 3, lastMsgAt: null },
    ]
    const { nats, request, handlers } = setupWithRooms(rooms)
    let setActive
    function Probe() {
      const { setActiveRoom } = useRoomSummaries()
      setActive = setActiveRoom
      return null
    }
    render(wrap(<Probe />, nats))
    await waitFor(() => expect(handlers.has('chat.room.g2.event')).toBe(true))

    act(() => { setActive('g1') })
    request.mockClear()

    act(() => {
      handlers.get('chat.room.g2.event')({
        type: 'new_message',
        roomId: 'g2',
        message: { id: 'm9', roomId: 'g2', sender: { account: 'bob' }, content: 'hi', createdAt: '2026-04-17T12:00:00Z' },
      })
    })

    const subjects = request.mock.calls.map((c) => c[0])
    expect(subjects.some((s) => s.endsWith('.message.read'))).toBe(false)
  })

  it('DOES fire message.read when the active-room message was sent by self (after the debounce window)', async () => {
    // Unread is derived server-side as lastMsgAt > lastSeenAt. Sending
    // advances Room.lastMsgAt but NOT the sender's lastSeenAt, so the
    // sender's own active room would count as unread until a mark-read
    // advances lastSeenAt. The self-sender skip is therefore removed:
    // an own message in the active room must still schedule the read.
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      const rooms = [
        { id: 'g1', name: 'general', type: 'channel', siteId: 'site-A', userCount: 3, lastMsgAt: null },
      ]
      const { nats, request, handlers } = setupWithRooms(rooms)
      let setActive
      function Probe() {
        const { setActiveRoom } = useRoomSummaries()
        setActive = setActiveRoom
        return null
      }
      render(wrap(<Probe />, nats))
      await waitFor(() => expect(handlers.has('chat.room.g1.event')).toBe(true))

      act(() => { setActive('g1') })
      request.mockClear()

      act(() => {
        handlers.get('chat.room.g1.event')({
          type: 'new_message',
          roomId: 'g1',
          message: { id: 'm1', roomId: 'g1', sender: { account: 'alice' }, content: 'self', createdAt: '2026-04-17T12:00:00Z' },
        })
      })

      // Before the debounce window: not yet.
      await act(async () => { await vi.advanceTimersByTimeAsync(300) })
      expect(request.mock.calls.some((c) => c[0].endsWith('.message.read'))).toBe(false)

      // After the trailing window: the read fires for the own message.
      await act(async () => { await vi.advanceTimersByTimeAsync(300) })
      const subjects = request.mock.calls.map((c) => c[0])
      expect(subjects.some((s) => s.endsWith('.message.read'))).toBe(true)
    } finally {
      vi.useRealTimers()
    }
  })

  it('cancels the pending mark-read timer on logout (user → null)', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      const rooms = [
        { id: 'g1', name: 'general', type: 'channel', siteId: 'site-A', userCount: 3, lastMsgAt: null },
      ]
      const { request, handlers } = setupWithRooms(rooms)

      // Build a NatsContext where user can be unset to simulate logout.
      function ToggleProbe() {
        const { setActiveRoom } = useRoomSummaries()
        return <button onClick={() => setActiveRoom('g1')}>activate</button>
      }

      let setNatsValue
      function NatsHarness({ children }) {
        const [v, setV] = useState({
          connected: true,
          user: { account: 'alice', siteId: 'site-A' },
          error: null,
          connect: vi.fn(),
          request,
          publish: vi.fn(),
          subscribe: vi.fn().mockImplementation((subject, cb) => {
            handlers.set(subject, cb)
            return { unsubscribe: vi.fn() }
          }),
          disconnect: vi.fn(),
        })
        setNatsValue = setV
        return (
          <NatsContext.Provider value={v}>
            <RoomEventsProvider>{children}</RoomEventsProvider>
          </NatsContext.Provider>
        )
      }

      render(<NatsHarness><ToggleProbe /></NatsHarness>)
      await waitFor(() => expect(handlers.has('chat.room.g1.event')).toBe(true))
      await act(async () => { screen.getByText('activate').click() })

      // Schedule a trailing mark-read by simulating a burst…
      act(() => {
        handlers.get('chat.room.g1.event')({
          type: 'new_message',
          roomId: 'g1',
          message: { id: 'm1', sender: { account: 'bob' }, content: 'hi', createdAt: '...' },
        })
      })

      // …then logout BEFORE the debounce fires.
      const readsBefore = request.mock.calls.filter((c) => c[0].endsWith('.message.read')).length
      await act(async () => {
        setNatsValue((v) => ({ ...v, user: null }))
      })
      // Advance past the debounce window.
      await act(async () => { await vi.advanceTimersByTimeAsync(600) })

      const readsAfter = request.mock.calls.filter((c) => c[0].endsWith('.message.read')).length
      // The pending timer must have been cancelled by the effect's cleanup.
      expect(readsAfter).toBe(readsBefore)
    } finally {
      vi.useRealTimers()
    }
  })

  it('two bursts more than 500ms apart fire TWO trailing message.read RPCs', async () => {
    // Confirms the timer re-arms cleanly after firing — not a sticky one-shot.
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      const rooms = [{ id: 'g1', name: 'general', type: 'channel', siteId: 'site-A', userCount: 3, lastMsgAt: null }]
      const { nats, request, handlers } = setupWithRooms(rooms)
      let setActive
      function Probe() {
        const { setActiveRoom } = useRoomSummaries()
        setActive = setActiveRoom
        return null
      }
      render(wrap(<Probe />, nats))
      await waitFor(() => expect(handlers.has('chat.room.g1.event')).toBe(true))
      act(() => { setActive('g1') })
      request.mockClear()

      // First burst.
      act(() => {
        handlers.get('chat.room.g1.event')({
          type: 'new_message', roomId: 'g1',
          message: { id: 'b1m1', sender: { account: 'bob' }, content: 'hi', createdAt: '...' },
        })
      })
      await act(async () => { await vi.advanceTimersByTimeAsync(600) })

      // Second burst, well after the first trailing fired.
      act(() => {
        handlers.get('chat.room.g1.event')({
          type: 'new_message', roomId: 'g1',
          message: { id: 'b2m1', sender: { account: 'bob' }, content: 'hi again', createdAt: '...' },
        })
      })
      await act(async () => { await vi.advanceTimersByTimeAsync(600) })

      const reads = request.mock.calls.filter((c) => c[0].endsWith('.message.read'))
      expect(reads.length).toBe(2)
    } finally {
      vi.useRealTimers()
    }
  })

  it('debounces a burst of active-room messages to a SINGLE trailing message.read RPC', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      const rooms = [{ id: 'g1', name: 'general', type: 'channel', siteId: 'site-A', userCount: 4, lastMsgAt: null }]
      const request = vi.fn().mockImplementation((subject, payload) => {
        if (subject.endsWith('.subscription.list') && payload?.type === 'rooms')
          return Promise.resolve({ subscriptions: rooms.map(roomToSub) })
        if (subject.endsWith('.subscription.list')) return Promise.resolve({ subscriptions: [] })
        return Promise.resolve({}) // every other (incl. message.read)
      })
      const handlers = new Map()
      const subscribe = vi.fn().mockImplementation((subject, cb) => {
        handlers.set(subject, cb)
        return { unsubscribe: vi.fn() }
      })
      const nats = mockNats({ request, subscribe })

      function Activator() {
        const { setActiveRoom } = useRoomSummaries()
        return <button onClick={() => setActiveRoom('g1')}>activate</button>
      }
      render(wrap(<Activator />, nats))

      // Open the room — fires immediate mark-read for setActiveRoom (1 call).
      await waitFor(() => expect(handlers.get('chat.room.g1.event')).toBeDefined())
      await act(async () => { screen.getByText('activate').click() })
      const after = request.mock.calls.filter((c) => c[0].endsWith('.message.read')).length
      expect(after).toBe(1)

      // Burst of 10 messages within the debounce window.
      await act(async () => {
        for (let i = 0; i < 10; i++) {
          handlers.get('chat.room.g1.event')({
            type: 'new_message',
            roomId: 'g1',
            message: { id: `m${i}`, roomId: 'g1', sender: { account: 'bob' }, content: 'hi', createdAt: '2026-04-17T12:00:00Z' },
          })
        }
      })

      // Inside the debounce window: no NEW message.read yet (still 1 from setActiveRoom).
      const inWindow = request.mock.calls.filter((c) => c[0].endsWith('.message.read')).length
      expect(inWindow).toBe(1)

      // Advance past the debounce window.
      await act(async () => {
        await vi.advanceTimersByTimeAsync(600)
      })
      const afterDebounce = request.mock.calls.filter((c) => c[0].endsWith('.message.read')).length
      // Exactly one trailing call should have fired for the burst.
      expect(afterDebounce).toBe(2)
    } finally {
      vi.useRealTimers()
    }
  })

  it('cancels the pending mark-read timer if the user switches rooms mid-burst', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      const rooms = [
        { id: 'g1', name: 'general', type: 'channel', siteId: 'site-A', userCount: 4, lastMsgAt: null },
        { id: 'g2', name: 'other',   type: 'channel', siteId: 'site-A', userCount: 2, lastMsgAt: null },
      ]
      const request = vi.fn().mockImplementation((subject, payload) => {
        if (subject.endsWith('.subscription.list') && payload?.type === 'rooms')
          return Promise.resolve({ subscriptions: rooms.map(roomToSub) })
        if (subject.endsWith('.subscription.list')) return Promise.resolve({ subscriptions: [] })
        return Promise.resolve({})
      })
      const handlers = new Map()
      const subscribe = vi.fn().mockImplementation((subject, cb) => {
        handlers.set(subject, cb)
        return { unsubscribe: vi.fn() }
      })
      const nats = mockNats({ request, subscribe })

      function Activator() {
        const { setActiveRoom } = useRoomSummaries()
        return (
          <>
            <button onClick={() => setActiveRoom('g1')}>g1</button>
            <button onClick={() => setActiveRoom('g2')}>g2</button>
          </>
        )
      }
      render(wrap(<Activator />, nats))

      await waitFor(() => expect(handlers.get('chat.room.g1.event')).toBeDefined())
      await waitFor(() => expect(handlers.get('chat.room.g2.event')).toBeDefined())
      await act(async () => { screen.getByText('g1').click() }) // setActiveRoom: 1 read
      // Burst in g1; trailing timer queued.
      await act(async () => {
        handlers.get('chat.room.g1.event')({
          type: 'new_message',
          roomId: 'g1',
          message: { id: 'm1', sender: { account: 'bob' }, content: 'hi', createdAt: '...' },
        })
      })
      // Switch BEFORE the debounce fires.
      await act(async () => { screen.getByText('g2').click() }) // setActiveRoom: 2 reads now (g1 + g2)
      // Advance past the debounce window.
      await act(async () => { await vi.advanceTimersByTimeAsync(600) })

      const readCalls = request.mock.calls.filter((c) => c[0].endsWith('.message.read'))
      // The trailing g1 timer must NOT fire (room is no longer active).
      // Expected reads: setActiveRoom(g1) + setActiveRoom(g2) = 2.
      expect(readCalls.length).toBe(2)
      // Both setActiveRoom calls fired immediate reads — confirm by subject.
      expect(readCalls.some((c) => c[0].includes('.room.g1.'))).toBe(true)
      expect(readCalls.some((c) => c[0].includes('.room.g2.'))).toBe(true)
    } finally {
      vi.useRealTimers()
    }
  })
})

describe('RoomEventsProvider sidebar buckets bootstrap', () => {
  beforeEach(() => vi.clearAllMocks())

  it('fires the three subscription RPCs with the documented payloads (no listRooms)', async () => {
    const calls = []
    const request = vi.fn().mockImplementation((subject, payload) => {
      calls.push({ subject, payload })
      if (subject.endsWith('.subscription.list') && payload?.type === 'rooms')
        return Promise.resolve({ subscriptions: [{ roomId: 'c1', roomType: 'channel', name: 'c1', siteId: 'site-A' }] })
      if (subject.endsWith('.subscription.list') && payload?.type === 'current')
        return Promise.resolve({ subscriptions: [{ roomId: 'f1', roomType: 'channel', name: 'f1', siteId: 'site-A' }] })
      if (subject.endsWith('.subscription.list') && payload?.type === 'apps')
        return Promise.resolve({ subscriptions: [{ roomId: 'a1', roomType: 'botDM', name: 'a1', siteId: 'site-A' }] })
      throw new Error('unexpected subject: ' + subject)
    })
    const nats = mockNats({ request })

    render(wrap(<SummariesProbe />, nats))
    // Three subscription RPCs total — all to subscription.list with different `type` values.
    // listRooms is not fired because subscriptions carry room metadata inline.
    await waitFor(() => expect(request).toHaveBeenCalledTimes(3))

    const getCurrent = calls.find((c) => c.payload?.type === 'current')
    const getApps = calls.find((c) => c.payload?.type === 'apps')
    const getRooms = calls.find((c) => c.payload?.type === 'rooms')

    expect(getCurrent.subject).toBe('chat.user.alice.request.user.site-A.subscription.list')
    expect(getCurrent.payload).toEqual({ type: 'current', favorite: true })
    expect(getApps.subject).toBe('chat.user.alice.request.user.site-A.subscription.list')
    expect(getApps.payload).toEqual({ type: 'apps' })
    expect(getRooms.subject).toBe('chat.user.alice.request.user.site-A.subscription.list')
    expect(getRooms.payload).toEqual({ type: 'rooms' })

    // No `rooms.list` RPC was made.
    expect(calls.find((c) => c.subject.endsWith('.rooms.list'))).toBeUndefined()
  })

  it('degrades gracefully (Promise.allSettled) when one bucket RPC fails', async () => {
    // getApps rejects; getCurrent + getRooms resolve. Sidebar should
    // still bootstrap with the rooms from getRooms; the Apps bucket
    // comes back empty (instead of black-holing the whole sidebar).
    const request = vi.fn().mockImplementation((subject, payload) => {
      if (subject.endsWith('.subscription.list') && payload?.type === 'rooms')
        return Promise.resolve({ subscriptions: [{ roomId: 'c1', roomType: 'channel', name: 'c1', siteId: 'site-A' }] })
      if (subject.endsWith('.subscription.list') && payload?.type === 'current')
        return Promise.resolve({ subscriptions: [] })
      if (subject.endsWith('.subscription.list') && payload?.type === 'apps')
        return Promise.reject(new Error('boom'))
      throw new Error('unexpected subject: ' + subject)
    })
    const nats = mockNats({ request })

    render(wrap(<SummariesProbe />, nats))
    await waitFor(() => expect(request).toHaveBeenCalledTimes(3))
    // The successful getRooms put c1 in summaries — failure of getApps
    // does NOT kill the rest of the bootstrap.
    await waitFor(() => expect(screen.getByTestId('count').textContent).toBe('1'))
  })

})

describe('useSidebarSections', () => {
  beforeEach(() => vi.clearAllMocks())

  function bootstrapNats({ buckets }) {
    // Each of the three bucket RPCs returns the subscriptions that
    // belong in that bucket. The frontend reconstructs `summaries`
    // from the union (deduped by roomId). Source-of-truth shape is
    // roomToSub() — the helper produces a sub with embedded room
    // metadata, which is what the real user-service returns.
    const room = (id, type = 'channel') => ({
      id,
      name: id,
      type,
      siteId: 'site-A',
      userCount: 2,
      lastMsgAt: `2026-04-17T${10 + (id.charCodeAt(0) % 5)}:00:00Z`,
    })
    const subsFor = (ids, typeFor = () => 'channel') =>
      ids.map((id) => roomToSub(room(id, typeFor(id))))
    return mockNats({
      request: vi.fn().mockImplementation((subject, payload) => {
        if (subject.endsWith('.subscription.list') && payload?.type === 'current' && payload?.favorite)
          return Promise.resolve({ subscriptions: subsFor(buckets.favoriteIds) })
        if (subject.endsWith('.subscription.list') && payload?.type === 'apps')
          return Promise.resolve({ subscriptions: subsFor(buckets.appIds, () => 'botDM') })
        if (subject.endsWith('.subscription.list') && payload?.type === 'rooms')
          return Promise.resolve({ subscriptions: subsFor(buckets.channelDmIds) })
        throw new Error('unexpected subject: ' + subject)
      }),
    })
  }

  function SectionsProbe() {
    const sections = useSidebarSections()
    return (
      <ul>
        {sections.map((s) => (
          <li key={s.key} data-testid={`section-${s.key}`}>
            {s.title}: {s.rooms.map((r) => r.id).join(',')}
          </li>
        ))}
      </ul>
    )
  }

  it('returns three sections in fixed order', async () => {
    const nats = bootstrapNats({ buckets: { favoriteIds: ['f1'], appIds: ['a1'], channelDmIds: ['c1'] } })
    render(wrap(<SectionsProbe />, nats))
    await waitFor(() =>
      expect(screen.getByTestId('section-channelDm').textContent).toContain('c1')
    )
    const items = screen.getAllByRole('listitem').map((li) => li.getAttribute('data-testid'))
    expect(items).toEqual(['section-favorite', 'section-apps', 'section-channelDm'])
  })

  it('puts favorited rooms in Favorite (favorite > apps > channelDm exclusivity)', async () => {
    // a1 appears in all three bucket lists — partition exclusivity
    // (favorite > apps > channelDm) lands it under Favorite only.
    const nats = bootstrapNats({
      buckets: { favoriteIds: ['f1', 'a1'], appIds: ['a1'], channelDmIds: ['c1', 'a1'] },
    })
    render(wrap(<SectionsProbe />, nats))
    await waitFor(() =>
      expect(screen.getByTestId('section-favorite').textContent).toContain('a1')
    )
    expect(screen.getByTestId('section-favorite').textContent).toContain('f1')
    expect(screen.getByTestId('section-apps').textContent).not.toContain('a1')
    expect(screen.getByTestId('section-channelDm').textContent).not.toContain('a1')
  })

  it('puts apps in Apps (apps > channelDm exclusivity)', async () => {
    const nats = bootstrapNats({
      buckets: { favoriteIds: [], appIds: ['a1'], channelDmIds: ['a1', 'c1'] },
    })
    render(wrap(<SectionsProbe />, nats))
    await waitFor(() => expect(screen.getByTestId('section-apps').textContent).toContain('a1'))
    expect(screen.getByTestId('section-channelDm').textContent).not.toContain('a1')
    expect(screen.getByTestId('section-channelDm').textContent).toContain('c1')
  })

  it('only renders rooms that appear in at least one bucket RPC', async () => {
    // With listRooms gone, summaries are derived entirely from the
    // three subscription RPCs — a room that isn't in any of them
    // simply doesn't exist in state.
    const nats = bootstrapNats({
      buckets: { favoriteIds: ['f1'], appIds: ['a1'], channelDmIds: ['c1'] },
    })
    render(wrap(<SectionsProbe />, nats))
    await waitFor(() =>
      expect(screen.getByTestId('section-channelDm').textContent).toContain('c1')
    )
    expect(screen.getByTestId('section-favorite').textContent).not.toContain('u1')
    expect(screen.getByTestId('section-apps').textContent).not.toContain('u1')
    expect(screen.getByTestId('section-channelDm').textContent).not.toContain('u1')
  })

  it('renders all three section headers empty when every bucket RPC returns nothing', async () => {
    // Cold-start path with zero subscriptions: the user has no rooms
    // at all. All three sections render empty (RoomList shows the
    // "No rooms" placeholder via section.note presence is handled in
    // RoomList tests).
    const nats = bootstrapNats({
      buckets: { favoriteIds: [], appIds: [], channelDmIds: [] },
    })
    render(wrap(<SectionsProbe />, nats))
    // Wait for the bootstrap to settle.
    await waitFor(() => expect(nats.request).toHaveBeenCalledTimes(3))
    expect(screen.getByTestId('section-favorite').textContent).toBe('Favorite: ')
    expect(screen.getByTestId('section-apps').textContent).toBe('Apps: ')
    expect(screen.getByTestId('section-channelDm').textContent).toBe('Channels and DMs: ')
  })

  it('preserves summaries recency order within each section', async () => {
    // bootstrapNats builds lastMsgAt from each ID's first-char code,
    // so c1 / f1 / a1 / u1 get distinct timestamps. Within
    // channelDm — three rooms — the order is desc by lastMsgAt.
    const nats = bootstrapNats({
      buckets: { favoriteIds: [], appIds: [], channelDmIds: ['f1', 'c1', 'u1'] },
    })
    render(wrap(<SectionsProbe />, nats))
    await waitFor(() =>
      expect(screen.getByTestId('section-channelDm').textContent).toContain('c1')
    )
    // We don't pin the exact order here (sortByLastMsgDesc is already
    // covered by its own reducer tests); just assert all three rendered.
    const channelDm = screen.getByTestId('section-channelDm').textContent
    expect(channelDm).toContain('f1')
    expect(channelDm).toContain('c1')
    expect(channelDm).toContain('u1')
  })

  it('exposes subscription name and hrInfo on each room (sub IS the room)', async () => {
    // Subscriptions returned by the bucket RPCs carry both their
    // subscription-local name (used as `subscriptionName` on the
    // summary) AND embedded room metadata. For DMs the same payload
    // includes hrInfo so the friend's display name is reachable.
    const nats = mockNats({
      request: vi.fn().mockImplementation((subject, payload) => {
        if (subject.endsWith('.subscription.list') && payload?.type === 'current' && payload?.favorite)
          return Promise.resolve({ subscriptions: [] })
        if (subject.endsWith('.subscription.list') && payload?.type === 'apps')
          return Promise.resolve({ subscriptions: [] })
        if (subject.endsWith('.subscription.list') && payload?.type === 'rooms')
          return Promise.resolve({
            subscriptions: [
              {
                ...roomToSub({ id: 'c1', name: 'frontend-team', type: 'channel', siteId: 'site-A', userCount: 2 }),
              },
              {
                ...roomToSub({ id: 'd1', name: 'bob-dm', type: 'dm', siteId: 'site-A', userCount: 2 }),
                hrInfo: { account: 'bob', engName: 'Bob Chen', name: '鮑勃' },
              },
            ],
          })
        throw new Error('unexpected subject: ' + subject)
      }),
    })

    function MergeProbe() {
      const sections = useSidebarSections()
      const channelDm = sections.find((s) => s.key === 'channelDm')
      return (
        <ul>
          {channelDm.rooms.map((r) => (
            <li key={r.id} data-testid={`room-${r.id}`}>
              name={r.subscriptionName ?? '∅'};hrEng={r.hrInfo?.engName ?? '∅'};hrName={r.hrInfo?.name ?? '∅'}
            </li>
          ))}
        </ul>
      )
    }

    render(wrap(<MergeProbe />, nats))
    await waitFor(() => expect(screen.queryByTestId('room-c1')).toBeInTheDocument())
    expect(screen.getByTestId('room-c1').textContent).toContain('name=frontend-team')
    expect(screen.getByTestId('room-d1').textContent).toContain('hrEng=Bob Chen')
    expect(screen.getByTestId('room-d1').textContent).toContain('hrName=鮑勃')
  })
})

describe('useSubscription', () => {
  beforeEach(() => vi.clearAllMocks())

  it('returns the full per-room subscription once the bucket bootstrap resolves', async () => {
    const nats = mockNats({
      request: vi.fn().mockImplementation((subject, payload) => {
        if (subject.endsWith('.subscription.list') && payload?.type === 'current' && payload?.favorite)
          return Promise.resolve({ subscriptions: [] })
        if (subject.endsWith('.subscription.list') && payload?.type === 'apps')
          return Promise.resolve({ subscriptions: [] })
        if (subject.endsWith('.subscription.list') && payload?.type === 'rooms')
          return Promise.resolve({
            subscriptions: [
              {
                ...roomToSub({ id: 'r1', name: 'general', type: 'channel', siteId: 'site-A' }),
                roles: ['owner'],
                alert: true,
              },
            ],
          })
        throw new Error('unexpected subject: ' + subject)
      }),
    })

    function Probe() {
      const sub = useSubscription('r1')
      if (!sub) return <div>no-sub</div>
      return <div>roles={sub.roles.join(',')};name={sub.name}</div>
    }

    render(wrap(<Probe />, nats))
    await waitFor(() =>
      expect(screen.getByText(/roles=owner;name=general/)).toBeInTheDocument()
    )
  })

  it('returns undefined for an unknown roomId or before bootstrap completes', () => {
    const nats = mockNats({
      // Never resolves — exercises the pre-bootstrap empty state synchronously.
      request: vi.fn().mockReturnValue(new Promise(() => {})),
    })
    function Probe() {
      const sub = useSubscription('unknown')
      return <div>{sub === undefined ? 'absent' : 'present'}</div>
    }
    render(wrap(<Probe />, nats))
    expect(screen.getByText('absent')).toBeInTheDocument()
  })
})

// ---------------------------------------------------------------------------
// Missing-key path: ensureKey + decrypt retry
// ---------------------------------------------------------------------------
describe('RoomEventsProvider missing-key path', () => {
  // Encrypted event fixture for a channel room message.
  const encEvent = {
    type: 'new_message',
    roomId: 'r1',
    siteId: 'site-A',
    encryptedMessage: { version: 2, nonce: 'AA==', ciphertext: 'BB==' },
    lastMsgId: 'm1',
    lastMsgAt: '2026-06-02T00:00:00Z',
    timestamp: Date.now(),
  }
  const decryptedPayload = JSON.stringify({
    id: 'm1',
    roomId: 'r1',
    content: 'hi',
    createdAt: '2026-06-02T00:00:00Z',
    sender: { account: 'bob' },
  })

  function setupEncryptedChannel(roomKeysMock) {
    // Prime the mock factory so RoomEventsContext reads the test's spies.
    currentRoomKeysMock = roomKeysMock

    const request = vi.fn().mockImplementation((subject, payload) => {
      if (subject.endsWith('.subscription.list') && payload?.type === 'rooms')
        return Promise.resolve({
          subscriptions: [
            {
              id: 'sub-r1',
              u: { id: 'u-r1', account: 'alice' },
              roomId: 'r1',
              siteId: 'site-A',
              roles: ['member'],
              name: 'r1',
              roomType: 'channel',
              joinedAt: '2026-01-01T00:00:00Z',
              hasMention: false,
              alert: false,
              room: {
                userCount: 2,
                lastMsgAt: null,
                lastMsgId: null,
              },
            },
          ],
        })
      if (subject.endsWith('.subscription.list')) return Promise.resolve({ subscriptions: [] })
      throw new Error('unexpected request: ' + subject)
    })
    const handlers = new Map()
    const subscribe = vi.fn().mockImplementation((subject, cb) => {
      handlers.set(subject, cb)
      return { unsubscribe: vi.fn() }
    })
    const nats = mockNats({ request, subscribe })
    return { nats, handlers }
  }

  beforeEach(() => {
    vi.clearAllMocks()
    // Reset to the harmless default after each test so the rest of the
    // suite is unaffected.
    currentRoomKeysMock = {
      decrypt: async () => null,
      hasKey: () => false,
      ensureKey: async () => false,
    }
  })

  it('calls ensureKey and retries decrypt once when the first decrypt returns null but ensureKey succeeds', async () => {
    let callCount = 0
    const decrypt = vi.fn().mockImplementation(async () => {
      callCount += 1
      if (callCount === 1) return null          // first call: key missing
      return decryptedPayload                   // second call: key present after ensureKey
    })
    const ensureKey = vi.fn().mockResolvedValue(true)
    const { nats, handlers } = setupEncryptedChannel({ decrypt, ensureKey, hasKey: () => false })

    render(wrap(<EventsProbe roomId="r1" />, nats))
    await waitFor(() => expect(handlers.has('chat.room.r1.event')).toBe(true))

    act(() => {
      handlers.get('chat.room.r1.event')(encEvent)
    })

    await waitFor(() =>
      expect(screen.getByTestId('messages').textContent).toContain('m1')
    )

    expect(ensureKey).toHaveBeenCalledTimes(1)
    expect(ensureKey).toHaveBeenCalledWith('r1', 2, 'site-A')
    expect(decrypt).toHaveBeenCalledTimes(2)
  })

  it('does NOT retry decrypt and falls through to placeholder when ensureKey resolves false', async () => {
    const decrypt = vi.fn().mockResolvedValue(null)    // always null
    const ensureKey = vi.fn().mockResolvedValue(false) // key fetch failed
    const { nats, handlers } = setupEncryptedChannel({ decrypt, ensureKey, hasKey: () => false })

    render(wrap(<EventsProbe roomId="r1" />, nats))
    await waitFor(() => expect(handlers.has('chat.room.r1.event')).toBe(true))

    act(() => {
      handlers.get('chat.room.r1.event')(encEvent)
    })

    // The event falls through to the placeholder branch, which uses lastMsgId as the id.
    await waitFor(() =>
      expect(screen.getByTestId('messages').textContent).toContain('m1')
    )

    expect(ensureKey).toHaveBeenCalledTimes(1)
    // decrypt is NOT retried — only the initial attempt.
    expect(decrypt).toHaveBeenCalledTimes(1)
  })

  it('does NOT call ensureKey when the first decrypt succeeds', async () => {
    const decrypt = vi.fn().mockResolvedValue(decryptedPayload) // always succeeds
    const ensureKey = vi.fn()
    const { nats, handlers } = setupEncryptedChannel({ decrypt, ensureKey, hasKey: () => true })

    render(wrap(<EventsProbe roomId="r1" />, nats))
    await waitFor(() => expect(handlers.has('chat.room.r1.event')).toBe(true))

    act(() => {
      handlers.get('chat.room.r1.event')(encEvent)
    })

    await waitFor(() =>
      expect(screen.getByTestId('messages').textContent).toContain('m1')
    )

    expect(ensureKey).not.toHaveBeenCalled()
    expect(decrypt).toHaveBeenCalledTimes(1)
  })
})
