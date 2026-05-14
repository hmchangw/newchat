import { describe, it, expect } from 'vitest'
import { BUFFER_MODE, initialState, roomEventsReducer } from './roomEventsReducer'

function room(id, overrides = {}) {
  return {
    id,
    name: `room-${id}`,
    type: 'channel',
    siteId: 'site-A',
    userCount: 2,
    lastMsgAt: '2026-04-17T10:00:00Z',
    ...overrides,
  }
}

describe('roomEventsReducer: rooms actions', () => {
  it('ROOMS_LOADED populates summaries sorted by lastMsgAt desc', () => {
    const a = room('a', { lastMsgAt: '2026-04-17T10:00:00Z' })
    const b = room('b', { lastMsgAt: '2026-04-17T12:00:00Z' })
    const next = roomEventsReducer(initialState, {
      type: 'ROOMS_LOADED',
      rooms: [a, b],
    })
    expect(next.summaries.map((r) => r.id)).toEqual(['b', 'a'])
    expect(next.summaries[0]).toMatchObject({
      id: 'b',
      name: 'room-b',
      type: 'channel',
      unreadCount: 0,
      hasMention: false,
      mentionAll: false,
    })
  })

  it('ROOM_ADDED appends a room and keeps sort order', () => {
    const a = room('a', { lastMsgAt: '2026-04-17T09:00:00Z' })
    const state = roomEventsReducer(initialState, { type: 'ROOMS_LOADED', rooms: [a] })
    const b = room('b', { lastMsgAt: '2026-04-17T10:00:00Z' })
    const next = roomEventsReducer(state, { type: 'ROOM_ADDED', room: b })
    expect(next.summaries.map((r) => r.id)).toEqual(['b', 'a'])
  })

  it('ROOM_ADDED ignores duplicates', () => {
    const a = room('a')
    const state = roomEventsReducer(initialState, { type: 'ROOMS_LOADED', rooms: [a] })
    const next = roomEventsReducer(state, { type: 'ROOM_ADDED', room: a })
    expect(next.summaries).toHaveLength(1)
  })

  it('ROOM_REMOVED drops the room from summaries and clears roomState', () => {
    const a = room('a')
    const b = room('b')
    const state = roomEventsReducer(initialState, { type: 'ROOMS_LOADED', rooms: [a, b] })
    const withCache = {
      ...state,
      roomState: {
        a: { messages: [], hasLoadedHistory: false, historyError: null, unreadCount: 1, hasMention: false, mentionAll: false, lastMsgAt: null, lastMsgId: null },
      },
    }
    const next = roomEventsReducer(withCache, { type: 'ROOM_REMOVED', roomId: 'a' })
    expect(next.summaries.map((r) => r.id)).toEqual(['b'])
    expect(next.roomState.a).toBeUndefined()
  })

  it('ROOM_METADATA_UPDATED patches name/userCount/lastMsgAt and re-sorts', () => {
    const a = room('a', { lastMsgAt: '2026-04-17T09:00:00Z' })
    const b = room('b', { lastMsgAt: '2026-04-17T10:00:00Z' })
    const state = roomEventsReducer(initialState, { type: 'ROOMS_LOADED', rooms: [a, b] })
    const next = roomEventsReducer(state, {
      type: 'ROOM_METADATA_UPDATED',
      roomId: 'a',
      name: 'a-renamed',
      userCount: 5,
      lastMsgAt: '2026-04-17T11:00:00Z',
    })
    expect(next.summaries[0]).toMatchObject({ id: 'a', name: 'a-renamed', userCount: 5 })
  })

  it('ROOM_METADATA_UPDATED for unknown room is a no-op', () => {
    const next = roomEventsReducer(initialState, {
      type: 'ROOM_METADATA_UPDATED',
      roomId: 'missing',
      name: 'x',
      userCount: 1,
      lastMsgAt: '2026-04-17T11:00:00Z',
    })
    expect(next).toBe(initialState)
  })
})

function newMessageEvent(overrides = {}) {
  return {
    type: 'new_message',
    roomId: 'a',
    roomName: 'room-a',
    roomType: 'channel',
    siteId: 'site-A',
    userCount: 3,
    lastMsgAt: '2026-04-17T12:00:00Z',
    lastMsgId: 'm1',
    mentionAll: false,
    hasMention: false,
    message: {
      id: 'm1',
      roomId: 'a',
      content: 'hi',
      createdAt: '2026-04-17T12:00:00Z',
      sender: { account: 'bob', engName: 'Bob' },
    },
    timestamp: 1,
    ...overrides,
  }
}

describe('roomEventsReducer: MESSAGE_RECEIVED', () => {
  it('appends a message and seeds roomState for an unknown room', () => {
    const next = roomEventsReducer(initialState, {
      type: 'MESSAGE_RECEIVED',
      event: newMessageEvent(),
    })
    expect(next.roomState.a.messages).toHaveLength(1)
    expect(next.roomState.a.messages[0].id).toBe('m1')
    expect(next.roomState.a.unreadCount).toBe(1)
    expect(next.roomState.a.lastMsgAt).toBe('2026-04-17T12:00:00Z')
    expect(next.roomState.a.lastMsgId).toBe('m1')
  })

  it('deduplicates by message.id', () => {
    const s1 = roomEventsReducer(initialState, {
      type: 'MESSAGE_RECEIVED',
      event: newMessageEvent(),
    })
    const s2 = roomEventsReducer(s1, {
      type: 'MESSAGE_RECEIVED',
      event: newMessageEvent(),
    })
    expect(s2.roomState.a.messages).toHaveLength(1)
    expect(s2.roomState.a.unreadCount).toBe(1)
  })

  it('does not increment unreadCount for the active room', () => {
    const state = { ...initialState, activeRoomId: 'a' }
    const next = roomEventsReducer(state, {
      type: 'MESSAGE_RECEIVED',
      event: newMessageEvent(),
    })
    expect(next.roomState.a.unreadCount).toBe(0)
  })

  it('sets hasMention when event.hasMention is true and room is not active', () => {
    const next = roomEventsReducer(initialState, {
      type: 'MESSAGE_RECEIVED',
      event: newMessageEvent({ hasMention: true }),
    })
    expect(next.roomState.a.hasMention).toBe(true)
    expect(next.roomState.a.mentionAll).toBe(false)
  })

  it('sets mentionAll when event.mentionAll is true and room is not active', () => {
    const next = roomEventsReducer(initialState, {
      type: 'MESSAGE_RECEIVED',
      event: newMessageEvent({ mentionAll: true }),
    })
    expect(next.roomState.a.mentionAll).toBe(true)
  })

  it('does not set mention flags for the active room', () => {
    const state = { ...initialState, activeRoomId: 'a' }
    const next = roomEventsReducer(state, {
      type: 'MESSAGE_RECEIVED',
      event: newMessageEvent({ hasMention: true, mentionAll: true }),
    })
    expect(next.roomState.a.hasMention).toBe(false)
    expect(next.roomState.a.mentionAll).toBe(false)
  })

  it('updates matching summary lastMsgAt and resorts', () => {
    const a = { id: 'a', name: 'a', type: 'channel', siteId: 'site-A', userCount: 2, lastMsgAt: '2026-04-17T08:00:00Z' }
    const b = { id: 'b', name: 'b', type: 'channel', siteId: 'site-A', userCount: 2, lastMsgAt: '2026-04-17T09:00:00Z' }
    const loaded = roomEventsReducer(initialState, { type: 'ROOMS_LOADED', rooms: [a, b] })
    const next = roomEventsReducer(loaded, {
      type: 'MESSAGE_RECEIVED',
      event: newMessageEvent({ roomId: 'a', lastMsgAt: '2026-04-17T10:00:00Z' }),
    })
    expect(next.summaries.map((r) => r.id)).toEqual(['a', 'b'])
    expect(next.summaries[0].lastMsgAt).toBe('2026-04-17T10:00:00Z')
    expect(next.summaries[0].unreadCount).toBe(1)
  })

  it('renders a placeholder when only encryptedMessage is present (no plaintext .message)', () => {
    // broadcast-worker with ENCRYPTION_ENABLED=true emits events where
    // ClientMessage is encrypted into evt.encryptedMessage and evt.message
    // is dropped via json:omitempty. Until client-side crypto lands we
    // can't decrypt — but silently swallowing the event makes the room
    // look frozen. Synthesize a "[encrypted message]" placeholder from
    // the top-level lastMsgId/lastMsgAt so the user at least sees that
    // a message arrived (and can tell their broadcast-worker is encrypting).
    const next = roomEventsReducer(initialState, {
      type: 'MESSAGE_RECEIVED',
      event: {
        type: 'new_message',
        roomId: 'a',
        lastMsgAt: '2026-04-17T12:00:00Z',
        lastMsgId: 'm-enc',
        encryptedMessage: { v: 1, ciphertext: 'AAA' },
        // no .message field — the omitempty drop
        timestamp: 1,
      },
    })
    expect(next.roomState.a.messages).toHaveLength(1)
    expect(next.roomState.a.messages[0]).toMatchObject({
      id: 'm-enc',
      content: '[encrypted message]',
      encrypted: true,
    })
  })

  it('does not drop an event that has both message and encryptedMessage — plaintext wins', () => {
    // Forward-compatible: if a future broadcaster sends both lanes (e.g.
    // during a rollout), the plaintext path is authoritative.
    const next = roomEventsReducer(initialState, {
      type: 'MESSAGE_RECEIVED',
      event: newMessageEvent({ encryptedMessage: { v: 1, ciphertext: 'XX' } }),
    })
    expect(next.roomState.a.messages[0].content).toBe('hi')
    expect(next.roomState.a.messages[0].encrypted).not.toBe(true)
  })

  it('caps the cached messages at MAX_CACHED, dropping oldest', async () => {
    const { MAX_CACHED } = await import('./roomEventsReducer')
    let state = initialState
    for (let i = 0; i < MAX_CACHED + 5; i++) {
      state = roomEventsReducer(state, {
        type: 'MESSAGE_RECEIVED',
        event: newMessageEvent({
          message: {
            id: `m${i}`,
            roomId: 'a',
            content: String(i),
            createdAt: '2026-04-17T12:00:00Z',
            sender: { account: 'bob', engName: 'Bob' },
          },
        }),
      })
    }
    const msgs = state.roomState.a.messages
    expect(msgs).toHaveLength(MAX_CACHED)
    expect(msgs[0].id).toBe('m5')
    expect(msgs[MAX_CACHED - 1].id).toBe(`m${MAX_CACHED + 4}`)
  })
})

describe('roomEventsReducer: history and active room', () => {
  it('HISTORY_LOADED merges ascending messages and preserves live ones', () => {
    const live = roomEventsReducer(initialState, {
      type: 'MESSAGE_RECEIVED',
      event: newMessageEvent({
        message: { id: 'm3', roomId: 'a', content: 'live', createdAt: '2026-04-17T12:00:00Z', sender: { account: 'bob' } },
      }),
    })
    const hist = [
      { id: 'm1', roomId: 'a', content: 'old1', createdAt: '2026-04-17T10:00:00Z', sender: { account: 'bob' } },
      { id: 'm2', roomId: 'a', content: 'old2', createdAt: '2026-04-17T11:00:00Z', sender: { account: 'bob' } },
    ]
    const next = roomEventsReducer(live, {
      type: 'HISTORY_LOADED',
      roomId: 'a',
      messages: hist,
    })
    expect(next.roomState.a.messages.map((m) => m.id)).toEqual(['m1', 'm2', 'm3'])
    expect(next.roomState.a.hasLoadedHistory).toBe(true)
    expect(next.roomState.a.historyError).toBe(null)
  })

  it('HISTORY_LOADED dedupes overlaps', () => {
    const live = roomEventsReducer(initialState, {
      type: 'MESSAGE_RECEIVED',
      event: newMessageEvent({
        message: { id: 'm2', roomId: 'a', content: 'live', createdAt: '2026-04-17T11:00:00Z', sender: { account: 'bob' } },
      }),
    })
    const hist = [
      { id: 'm1', roomId: 'a', content: 'old1', createdAt: '2026-04-17T10:00:00Z', sender: { account: 'bob' } },
      { id: 'm2', roomId: 'a', content: 'old2', createdAt: '2026-04-17T11:00:00Z', sender: { account: 'bob' } },
    ]
    const next = roomEventsReducer(live, { type: 'HISTORY_LOADED', roomId: 'a', messages: hist })
    expect(next.roomState.a.messages.map((m) => m.id)).toEqual(['m1', 'm2'])
  })

  it('HISTORY_FAILED sets historyError and does not flip hasLoadedHistory', () => {
    const next = roomEventsReducer(initialState, {
      type: 'HISTORY_FAILED',
      roomId: 'a',
      error: 'boom',
    })
    expect(next.roomState.a.historyError).toBe('boom')
    expect(next.roomState.a.hasLoadedHistory).toBe(false)
  })

  it('SET_ACTIVE_ROOM updates activeRoomId and clears unread/mention for that room', () => {
    const s1 = roomEventsReducer(initialState, {
      type: 'MESSAGE_RECEIVED',
      event: newMessageEvent({ hasMention: true, mentionAll: true }),
    })
    expect(s1.roomState.a.unreadCount).toBe(1)
    const s2 = roomEventsReducer(s1, { type: 'SET_ACTIVE_ROOM', roomId: 'a' })
    expect(s2.activeRoomId).toBe('a')
    expect(s2.roomState.a.unreadCount).toBe(0)
    expect(s2.roomState.a.hasMention).toBe(false)
    expect(s2.roomState.a.mentionAll).toBe(false)
  })

  it('SET_ACTIVE_ROOM clears the matching summary flags', () => {
    const loaded = roomEventsReducer(initialState, {
      type: 'ROOMS_LOADED',
      rooms: [{ id: 'a', name: 'a', type: 'channel', siteId: 'site-A', userCount: 2, lastMsgAt: '2026-04-17T10:00:00Z' }],
    })
    const withMsg = roomEventsReducer(loaded, {
      type: 'MESSAGE_RECEIVED',
      event: newMessageEvent({ hasMention: true }),
    })
    expect(withMsg.summaries[0].hasMention).toBe(true)
    expect(withMsg.summaries[0].unreadCount).toBe(1)
    const next = roomEventsReducer(withMsg, { type: 'SET_ACTIVE_ROOM', roomId: 'a' })
    expect(next.summaries[0].hasMention).toBe(false)
    expect(next.summaries[0].unreadCount).toBe(0)
  })

  it('SET_ACTIVE_ROOM to null clears the activeRoomId only', () => {
    const s1 = { ...initialState, activeRoomId: 'a' }
    const next = roomEventsReducer(s1, { type: 'SET_ACTIVE_ROOM', roomId: null })
    expect(next.activeRoomId).toBe(null)
  })

  it('RESET returns the initial state', () => {
    const s1 = roomEventsReducer(initialState, {
      type: 'ROOMS_LOADED',
      rooms: [{ id: 'a', name: 'a', type: 'channel', siteId: 'site-A', userCount: 2, lastMsgAt: null }],
    })
    const next = roomEventsReducer(s1, { type: 'RESET' })
    expect(next).toEqual(initialState)
  })

  it('ROOMS_FAILED stores the error message', () => {
    const next = roomEventsReducer(initialState, { type: 'ROOMS_FAILED', error: 'boom' })
    expect(next.roomsError).toBe('boom')
  })
})

describe('roomEventsReducer: buffer mode (jump-to-message)', () => {
  it('emptyRoomState defaults bufferMode=live with no pending messages or focus', () => {
    const next = roomEventsReducer(initialState, {
      type: 'MESSAGE_RECEIVED',
      event: newMessageEvent(),
    })
    expect(next.roomState.a.bufferMode).toBe(BUFFER_MODE.LIVE)
    expect(next.roomState.a.pendingLiveMessages).toEqual([])
    expect(next.roomState.a.focusMessageId).toBe(null)
  })

  it('REPLACE_ROOM_BUFFER swaps messages, sets historical mode + focus', () => {
    const surrounding = [
      { id: 'm10', roomId: 'a', content: 'before', createdAt: '2026-04-17T11:00:00Z', sender: { account: 'bob' } },
      { id: 'm11', roomId: 'a', content: 'hit',    createdAt: '2026-04-17T11:01:00Z', sender: { account: 'bob' } },
      { id: 'm12', roomId: 'a', content: 'after',  createdAt: '2026-04-17T11:02:00Z', sender: { account: 'bob' } },
    ]
    const next = roomEventsReducer(initialState, {
      type: 'REPLACE_ROOM_BUFFER',
      roomId: 'a',
      messages: surrounding,
      focusMessageId: 'm11',
    })
    expect(next.roomState.a.messages.map((m) => m.id)).toEqual(['m10', 'm11', 'm12'])
    expect(next.roomState.a.bufferMode).toBe(BUFFER_MODE.HISTORICAL)
    expect(next.roomState.a.focusMessageId).toBe('m11')
    expect(next.roomState.a.hasLoadedHistory).toBe(true)
    expect(next.roomState.a.pendingLiveMessages).toEqual([])
  })

  it('MESSAGE_RECEIVED in historical mode buffers into pendingLiveMessages and does not touch messages', () => {
    const seeded = roomEventsReducer(initialState, {
      type: 'REPLACE_ROOM_BUFFER',
      roomId: 'a',
      messages: [
        { id: 'm1', roomId: 'a', content: 'old', createdAt: '2026-04-17T10:00:00Z', sender: { account: 'bob' } },
      ],
      focusMessageId: 'm1',
    })
    const next = roomEventsReducer(seeded, {
      type: 'MESSAGE_RECEIVED',
      event: newMessageEvent({
        message: { id: 'mLive', roomId: 'a', content: 'live', createdAt: '2026-04-17T12:00:00Z', sender: { account: 'bob' } },
        lastMsgId: 'mLive',
      }),
    })
    expect(next.roomState.a.messages.map((m) => m.id)).toEqual(['m1'])
    expect(next.roomState.a.pendingLiveMessages.map((m) => m.id)).toEqual(['mLive'])
    expect(next.roomState.a.bufferMode).toBe(BUFFER_MODE.HISTORICAL)
  })

  it('MESSAGE_RECEIVED in historical mode dedupes pendingLiveMessages by id', () => {
    let s = roomEventsReducer(initialState, {
      type: 'REPLACE_ROOM_BUFFER',
      roomId: 'a',
      messages: [],
      focusMessageId: null,
    })
    s = roomEventsReducer(s, {
      type: 'MESSAGE_RECEIVED',
      event: newMessageEvent({
        message: { id: 'mLive', roomId: 'a', content: 'live', createdAt: '2026-04-17T12:00:00Z', sender: { account: 'bob' } },
      }),
    })
    s = roomEventsReducer(s, {
      type: 'MESSAGE_RECEIVED',
      event: newMessageEvent({
        message: { id: 'mLive', roomId: 'a', content: 'live', createdAt: '2026-04-17T12:00:00Z', sender: { account: 'bob' } },
      }),
    })
    expect(s.roomState.a.pendingLiveMessages.map((m) => m.id)).toEqual(['mLive'])
  })

  it('RESET_TO_LIVE_TAIL merges pending into messages, clears pending + focus, flips back to live', () => {
    let s = roomEventsReducer(initialState, {
      type: 'REPLACE_ROOM_BUFFER',
      roomId: 'a',
      messages: [
        { id: 'm1', roomId: 'a', content: 'old', createdAt: '2026-04-17T10:00:00Z', sender: { account: 'bob' } },
      ],
      focusMessageId: 'm1',
    })
    s = roomEventsReducer(s, {
      type: 'MESSAGE_RECEIVED',
      event: newMessageEvent({
        message: { id: 'mLive', roomId: 'a', content: 'live', createdAt: '2026-04-17T12:00:00Z', sender: { account: 'bob' } },
      }),
    })
    const next = roomEventsReducer(s, { type: 'RESET_TO_LIVE_TAIL', roomId: 'a' })
    expect(next.roomState.a.messages.map((m) => m.id)).toEqual(['m1', 'mLive'])
    expect(next.roomState.a.pendingLiveMessages).toEqual([])
    expect(next.roomState.a.focusMessageId).toBe(null)
    expect(next.roomState.a.bufferMode).toBe(BUFFER_MODE.LIVE)
  })

  it('RESET_TO_LIVE_TAIL dedupes pending vs existing messages', () => {
    let s = roomEventsReducer(initialState, {
      type: 'REPLACE_ROOM_BUFFER',
      roomId: 'a',
      messages: [
        { id: 'm1', roomId: 'a', content: 'old', createdAt: '2026-04-17T10:00:00Z', sender: { account: 'bob' } },
      ],
      focusMessageId: 'm1',
    })
    // Inject a pending message that already exists in messages (defensive)
    s = {
      ...s,
      roomState: {
        ...s.roomState,
        a: {
          ...s.roomState.a,
          pendingLiveMessages: [
            { id: 'm1', roomId: 'a', content: 'dup', createdAt: '2026-04-17T10:00:00Z', sender: { account: 'bob' } },
          ],
        },
      },
    }
    const next = roomEventsReducer(s, { type: 'RESET_TO_LIVE_TAIL', roomId: 'a' })
    expect(next.roomState.a.messages.map((m) => m.id)).toEqual(['m1'])
  })

  it('RESET_TO_LIVE_TAIL on an unknown room is a safe no-op-ish', () => {
    const next = roomEventsReducer(initialState, { type: 'RESET_TO_LIVE_TAIL', roomId: 'missing' })
    // Either no change to state or an empty roomState.missing in live mode is acceptable.
    expect(next.roomState.missing?.bufferMode ?? BUFFER_MODE.LIVE).toBe(BUFFER_MODE.LIVE)
  })
})

describe('roomEventsReducer: bucket Sets', () => {
  it('initialState has empty favoriteIds, appIds, channelDmIds Sets', () => {
    expect(initialState.favoriteIds).toBeInstanceOf(Set)
    expect(initialState.appIds).toBeInstanceOf(Set)
    expect(initialState.channelDmIds).toBeInstanceOf(Set)
    expect(initialState.favoriteIds.size).toBe(0)
    expect(initialState.appIds.size).toBe(0)
    expect(initialState.channelDmIds.size).toBe(0)
  })

  it('BUCKETS_LOADED populates all three Sets from input arrays', () => {
    const next = roomEventsReducer(initialState, {
      type: 'BUCKETS_LOADED',
      favoriteIds: ['f1', 'f2'],
      appIds: ['a1'],
      channelDmIds: ['c1', 'c2', 'c3'],
    })
    expect([...next.favoriteIds].sort()).toEqual(['f1', 'f2'])
    expect([...next.appIds].sort()).toEqual(['a1'])
    expect([...next.channelDmIds].sort()).toEqual(['c1', 'c2', 'c3'])
  })

  it('BUCKETS_LOADED replaces previous Set contents', () => {
    const first = roomEventsReducer(initialState, {
      type: 'BUCKETS_LOADED',
      favoriteIds: ['f1'],
      appIds: ['a1'],
      channelDmIds: ['c1'],
    })
    const second = roomEventsReducer(first, {
      type: 'BUCKETS_LOADED',
      favoriteIds: ['f2'],
      appIds: [],
      channelDmIds: ['c2'],
    })
    expect([...second.favoriteIds]).toEqual(['f2'])
    expect(second.appIds.size).toBe(0)
    expect([...second.channelDmIds]).toEqual(['c2'])
  })

  it('ROOM_ADDED with botDM type adds to appIds, leaves channelDmIds unchanged', () => {
    const next = roomEventsReducer(initialState, {
      type: 'ROOM_ADDED',
      room: room('bot1', { type: 'botDM' }),
    })
    expect(next.appIds.has('bot1')).toBe(true)
    expect(next.channelDmIds.has('bot1')).toBe(false)
    expect(next.favoriteIds.has('bot1')).toBe(false)
  })

  it('ROOM_ADDED with channel type adds to channelDmIds, leaves appIds unchanged', () => {
    const next = roomEventsReducer(initialState, {
      type: 'ROOM_ADDED',
      room: room('ch1', { type: 'channel' }),
    })
    expect(next.channelDmIds.has('ch1')).toBe(true)
    expect(next.appIds.has('ch1')).toBe(false)
    expect(next.favoriteIds.has('ch1')).toBe(false)
  })

  it('ROOM_ADDED with dm type adds to channelDmIds', () => {
    const next = roomEventsReducer(initialState, {
      type: 'ROOM_ADDED',
      room: room('dm1', { type: 'dm' }),
    })
    expect(next.channelDmIds.has('dm1')).toBe(true)
    expect(next.appIds.has('dm1')).toBe(false)
  })

  it('ROOM_ADDED with discussion type adds to channelDmIds', () => {
    const next = roomEventsReducer(initialState, {
      type: 'ROOM_ADDED',
      room: room('d1', { type: 'discussion' }),
    })
    expect(next.channelDmIds.has('d1')).toBe(true)
    expect(next.appIds.has('d1')).toBe(false)
  })

  it('ROOM_ADDED never modifies favoriteIds', () => {
    const seeded = roomEventsReducer(initialState, {
      type: 'BUCKETS_LOADED',
      favoriteIds: ['f1'],
      appIds: [],
      channelDmIds: [],
    })
    const next = roomEventsReducer(seeded, {
      type: 'ROOM_ADDED',
      room: room('newRoom', { type: 'botDM' }),
    })
    expect([...next.favoriteIds]).toEqual(['f1'])
  })

  it('ROOM_ADDED for a roomId already in appIds is a no-op for the bucket', () => {
    const seeded = roomEventsReducer(initialState, {
      type: 'BUCKETS_LOADED',
      favoriteIds: [],
      appIds: ['bot1'],
      channelDmIds: [],
    })
    const next = roomEventsReducer(seeded, {
      type: 'ROOM_ADDED',
      room: room('bot1', { type: 'botDM' }),
    })
    expect(next.appIds.size).toBe(1)
    expect(next.appIds.has('bot1')).toBe(true)
  })

  it('ROOM_REMOVED removes the roomId from favoriteIds if present', () => {
    const seeded = roomEventsReducer(initialState, {
      type: 'BUCKETS_LOADED',
      favoriteIds: ['f1', 'f2'],
      appIds: [],
      channelDmIds: [],
    })
    const next = roomEventsReducer(seeded, { type: 'ROOM_REMOVED', roomId: 'f1' })
    expect([...next.favoriteIds]).toEqual(['f2'])
  })

  it('ROOM_REMOVED removes the roomId from appIds if present', () => {
    const seeded = roomEventsReducer(initialState, {
      type: 'BUCKETS_LOADED',
      favoriteIds: [],
      appIds: ['a1', 'a2'],
      channelDmIds: [],
    })
    const next = roomEventsReducer(seeded, { type: 'ROOM_REMOVED', roomId: 'a1' })
    expect([...next.appIds]).toEqual(['a2'])
  })

  it('ROOM_REMOVED removes the roomId from channelDmIds if present', () => {
    const seeded = roomEventsReducer(initialState, {
      type: 'BUCKETS_LOADED',
      favoriteIds: [],
      appIds: [],
      channelDmIds: ['c1', 'c2'],
    })
    const next = roomEventsReducer(seeded, { type: 'ROOM_REMOVED', roomId: 'c1' })
    expect([...next.channelDmIds]).toEqual(['c2'])
  })

  it('ROOM_REMOVED for a roomId in none of the Sets leaves them unchanged', () => {
    const seeded = roomEventsReducer(initialState, {
      type: 'BUCKETS_LOADED',
      favoriteIds: ['f1'],
      appIds: ['a1'],
      channelDmIds: ['c1'],
    })
    const next = roomEventsReducer(seeded, { type: 'ROOM_REMOVED', roomId: 'unknown' })
    expect([...next.favoriteIds]).toEqual(['f1'])
    expect([...next.appIds]).toEqual(['a1'])
    expect([...next.channelDmIds]).toEqual(['c1'])
  })

  it('RESET empties all three bucket Sets', () => {
    const seeded = roomEventsReducer(initialState, {
      type: 'BUCKETS_LOADED',
      favoriteIds: ['f1'],
      appIds: ['a1'],
      channelDmIds: ['c1'],
    })
    const next = roomEventsReducer(seeded, { type: 'RESET' })
    expect(next.favoriteIds.size).toBe(0)
    expect(next.appIds.size).toBe(0)
    expect(next.channelDmIds.size).toBe(0)
  })

  it('initialState has an empty subscriptionData map', () => {
    expect(initialState.subscriptionData).toEqual({})
  })

  it('BUCKETS_LOADED stores subscriptionData when provided', () => {
    const next = roomEventsReducer(initialState, {
      type: 'BUCKETS_LOADED',
      favoriteIds: ['f1'],
      appIds: ['a1'],
      channelDmIds: ['c1'],
      subscriptionData: {
        f1: { name: 'general', hrInfo: undefined },
        c1: { name: 'one-on-one', hrInfo: { engName: 'bob', name: '鮑勃' } },
      },
    })
    expect(next.subscriptionData.f1.name).toBe('general')
    expect(next.subscriptionData.c1.hrInfo.engName).toBe('bob')
  })

  it('BUCKETS_LOADED with no subscriptionData payload keeps the map empty', () => {
    const next = roomEventsReducer(initialState, {
      type: 'BUCKETS_LOADED',
      favoriteIds: ['f1'],
      appIds: [],
      channelDmIds: [],
    })
    expect(next.subscriptionData).toEqual({})
  })

  it('ROOM_REMOVED also drops the room from subscriptionData', () => {
    const seeded = roomEventsReducer(initialState, {
      type: 'BUCKETS_LOADED',
      favoriteIds: [],
      appIds: [],
      channelDmIds: ['c1'],
      subscriptionData: { c1: { name: 'one-on-one' } },
    })
    const next = roomEventsReducer(seeded, { type: 'ROOM_REMOVED', roomId: 'c1' })
    expect(next.subscriptionData.c1).toBeUndefined()
  })

  it('RESET also clears subscriptionData', () => {
    const seeded = roomEventsReducer(initialState, {
      type: 'BUCKETS_LOADED',
      favoriteIds: ['f1'],
      appIds: [],
      channelDmIds: [],
      subscriptionData: { f1: { name: 'general' } },
    })
    const next = roomEventsReducer(seeded, { type: 'RESET' })
    expect(next.subscriptionData).toEqual({})
  })
})
