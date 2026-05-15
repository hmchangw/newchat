import { describe, it, expect } from 'vitest'
import { BUFFER_MODE, initialState, roomEventsReducer, selectUnreadTotal } from './reducer'

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
    const { MAX_CACHED } = await import('./reducer')
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

describe('MESSAGE_SENT_LOCAL', () => {
  it('appends an optimistic message to the room buffer (creating room state if absent)', () => {
    const out = roomEventsReducer(initialState, {
      type: 'MESSAGE_SENT_LOCAL',
      roomId: 'r1',
      message: { id: 'opt-1', content: 'hi', _local: true },
    })
    expect(out.roomState.r1.messages).toEqual([{ id: 'opt-1', content: 'hi', _local: true }])
  })

  it('is a no-op when message.id already exists in the buffer (dedupe)', () => {
    const seed = {
      ...initialState,
      roomState: { r1: { messages: [{ id: 'opt-1', content: 'first' }] } },
    }
    const out = roomEventsReducer(seed, {
      type: 'MESSAGE_SENT_LOCAL',
      roomId: 'r1',
      message: { id: 'opt-1', content: 'second' },
    })
    expect(out.roomState.r1.messages).toEqual([{ id: 'opt-1', content: 'first' }])
  })

  it('is a no-op when action.message has no id', () => {
    const out = roomEventsReducer(initialState, {
      type: 'MESSAGE_SENT_LOCAL', roomId: 'r1', message: { content: 'no id' },
    })
    expect(out).toBe(initialState)
  })
})

describe('MESSAGE_EDITED_LOCAL', () => {
  it('replaces content + editedAt on the matching message in roomState[roomId].messages', () => {
    const seed = {
      ...initialState,
      roomState: {
        r1: {
          messages: [{ id: 'm1', content: 'old' }, { id: 'm2', content: 'other' }],
          hasLoadedHistory: true,
          historyError: null,
          unreadCount: 0,
          hasMention: false,
          mentionAll: false,
          lastMsgAt: null,
          lastMsgId: null,
          bufferMode: 'live',
          pendingLiveMessages: [],
          focusMessageId: null,
        },
      },
    }
    const out = roomEventsReducer(seed, {
      type: 'MESSAGE_EDITED_LOCAL',
      roomId: 'r1',
      messageId: 'm1',
      content: 'new',
      editedAt: '2026-05-13T11:00:00Z',
    })
    expect(out.roomState.r1.messages[0]).toEqual({
      id: 'm1', content: 'new', editedAt: '2026-05-13T11:00:00Z',
    })
    expect(out.roomState.r1.messages[1]).toEqual({ id: 'm2', content: 'other' })
  })

  it('is a no-op when the message id is not buffered', () => {
    const seed = {
      ...initialState,
      roomState: { r1: { messages: [{ id: 'm1', content: 'old' }] } },
    }
    const out = roomEventsReducer(seed, {
      type: 'MESSAGE_EDITED_LOCAL', roomId: 'r1', messageId: 'unknown', content: 'x', editedAt: 't',
    })
    expect(out).toBe(seed)
  })
})

describe('MESSAGE_DELETED_LOCAL', () => {
  it('flags the matching message as deleted', () => {
    const seed = {
      ...initialState,
      roomState: { r1: { messages: [{ id: 'm1', content: 'bye' }] } },
    }
    const out = roomEventsReducer(seed, {
      type: 'MESSAGE_DELETED_LOCAL', roomId: 'r1', messageId: 'm1',
    })
    expect(out.roomState.r1.messages[0]).toEqual({
      id: 'm1', content: 'bye', deleted: true,
    })
  })

  it('is a no-op when the message id is not buffered', () => {
    const seed = { ...initialState, roomState: { r1: { messages: [] } } }
    const out = roomEventsReducer(seed, {
      type: 'MESSAGE_DELETED_LOCAL', roomId: 'r1', messageId: 'm1',
    })
    expect(out).toBe(seed)
  })
})

describe('MESSAGE_RECEIVED — thread-reply filter', () => {
  it('drops events whose message.threadParentMessageId is non-empty', () => {
    const seed = {
      ...initialState,
      summaries: [{ id: 'r1', name: 'general', type: 'channel', siteId: 's', userCount: 1, lastMsgAt: null, unreadCount: 0, hasMention: false, mentionAll: false }],
      activeRoomId: 'r1',
      roomState: {
        r1: {
          messages: [{ id: 'm-existing' }],
          hasLoadedHistory: true, historyError: null,
          unreadCount: 0, hasMention: false, mentionAll: false,
          lastMsgAt: null, lastMsgId: null,
          bufferMode: 'live', pendingLiveMessages: [], focusMessageId: null,
        },
      },
    }
    const out = roomEventsReducer(seed, {
      type: 'MESSAGE_RECEIVED',
      event: {
        roomId: 'r1',
        message: { id: 'reply-1', content: 'thread', threadParentMessageId: 'parent-1' },
      },
    })
    expect(out).toBe(seed)
  })

  it('still appends events whose threadParentMessageId is empty', () => {
    const seed = {
      ...initialState,
      summaries: [{ id: 'r1', name: 'general', type: 'channel', siteId: 's', userCount: 1, lastMsgAt: null, unreadCount: 0, hasMention: false, mentionAll: false }],
      activeRoomId: 'r1',
      roomState: {
        r1: {
          messages: [],
          hasLoadedHistory: true, historyError: null,
          unreadCount: 0, hasMention: false, mentionAll: false,
          lastMsgAt: null, lastMsgId: null,
          bufferMode: 'live', pendingLiveMessages: [], focusMessageId: null,
        },
      },
    }
    const out = roomEventsReducer(seed, {
      type: 'MESSAGE_RECEIVED',
      event: { roomId: 'r1', message: { id: 'm-1', content: 'top-level' } },
    })
    expect(out.roomState.r1.messages.map((m) => m.id)).toEqual(['m-1'])
  })
})

describe('OWN_THREAD_REPLY_SENT', () => {
  it('increments tcount on the parent message in roomState[roomId].messages', () => {
    const seed = {
      ...initialState,
      roomState: {
        r1: {
          messages: [{ id: 'p1', content: 'parent', tcount: 0 }],
          hasLoadedHistory: true, historyError: null,
          unreadCount: 0, hasMention: false, mentionAll: false,
          lastMsgAt: null, lastMsgId: null,
          bufferMode: 'live', pendingLiveMessages: [], focusMessageId: null,
        },
      },
    }
    const out = roomEventsReducer(seed, { type: 'OWN_THREAD_REPLY_SENT', roomId: 'r1', parentId: 'p1' })
    expect(out.roomState.r1.messages[0].tcount).toBe(1)
  })

  it('initialises tcount to 1 if previously undefined', () => {
    const seed = {
      ...initialState,
      roomState: { r1: { messages: [{ id: 'p1' }] } },
    }
    const out = roomEventsReducer(seed, { type: 'OWN_THREAD_REPLY_SENT', roomId: 'r1', parentId: 'p1' })
    expect(out.roomState.r1.messages[0].tcount).toBe(1)
  })

  it('is a no-op when the parent isn\'t in the room buffer', () => {
    const seed = { ...initialState, roomState: { r1: { messages: [] } } }
    const out = roomEventsReducer(seed, { type: 'OWN_THREAD_REPLY_SENT', roomId: 'r1', parentId: 'p1' })
    expect(out).toBe(seed)
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

  it('initialState has an empty subscriptions map', () => {
    expect(initialState.subscriptions).toEqual({})
  })

  it('BUCKETS_LOADED stores the full subscription record per roomId', () => {
    const next = roomEventsReducer(initialState, {
      type: 'BUCKETS_LOADED',
      favoriteIds: ['f1'],
      appIds: ['a1'],
      channelDmIds: ['c1'],
      subscriptions: {
        f1: { roomId: 'f1', name: 'general', roles: ['member'], hasMention: false, alert: true },
        c1: { roomId: 'c1', name: 'one-on-one', roles: ['member'], hasMention: false, alert: true, hrInfo: { account: 'bob', engName: 'bob', name: '鮑勃' } },
      },
    })
    expect(next.subscriptions.f1.name).toBe('general')
    expect(next.subscriptions.c1.hrInfo.engName).toBe('bob')
    expect(next.subscriptions.c1.hrInfo.account).toBe('bob')
    expect(next.subscriptions.c1.roles).toEqual(['member'])
  })

  it('BUCKETS_LOADED with no subscriptions payload keeps the map empty', () => {
    const next = roomEventsReducer(initialState, {
      type: 'BUCKETS_LOADED',
      favoriteIds: ['f1'],
      appIds: [],
      channelDmIds: [],
    })
    expect(next.subscriptions).toEqual({})
  })

  it('BUCKETS_LOADED seeds summary.hasMention from the server-side flag', () => {
    // Pre-populate a summary so the seed has somewhere to write.
    const withSummary = roomEventsReducer(initialState, {
      type: 'ROOM_ADDED',
      room: room('c1', { type: 'channel' }),
    })
    const next = roomEventsReducer(withSummary, {
      type: 'BUCKETS_LOADED',
      favoriteIds: [],
      appIds: [],
      channelDmIds: ['c1'],
      subscriptions: { c1: { roomId: 'c1', hasMention: true, roles: ['member'] } },
    })
    expect(next.summaries.find((s) => s.id === 'c1').hasMention).toBe(true)
  })

  it('ROOM_REMOVED also drops the room from subscriptions', () => {
    const seeded = roomEventsReducer(initialState, {
      type: 'BUCKETS_LOADED',
      favoriteIds: [],
      appIds: [],
      channelDmIds: ['c1'],
      subscriptions: { c1: { roomId: 'c1', name: 'one-on-one', roles: ['member'] } },
    })
    const next = roomEventsReducer(seeded, { type: 'ROOM_REMOVED', roomId: 'c1' })
    expect(next.subscriptions.c1).toBeUndefined()
  })

  it('RESET also clears subscriptions', () => {
    const seeded = roomEventsReducer(initialState, {
      type: 'BUCKETS_LOADED',
      favoriteIds: ['f1'],
      appIds: [],
      channelDmIds: [],
      subscriptions: { f1: { roomId: 'f1', name: 'general', roles: ['member'] } },
    })
    const next = roomEventsReducer(seeded, { type: 'RESET' })
    expect(next.subscriptions).toEqual({})
  })

  it('SUBSCRIPTION_UPSERTED inserts a new record and merges hasMention into the summary', () => {
    const withSummary = roomEventsReducer(initialState, {
      type: 'ROOM_ADDED',
      room: room('c1', { type: 'channel' }),
    })
    const next = roomEventsReducer(withSummary, {
      type: 'SUBSCRIPTION_UPSERTED',
      subscription: { roomId: 'c1', name: 'general', roles: ['owner'], hasMention: true, alert: true },
    })
    expect(next.subscriptions.c1.roles).toEqual(['owner'])
    expect(next.summaries.find((s) => s.id === 'c1').hasMention).toBe(true)
  })

  it('SUBSCRIPTION_UPSERTED replaces an existing record (full-record semantics)', () => {
    const seeded = roomEventsReducer(initialState, {
      type: 'SUBSCRIPTION_UPSERTED',
      subscription: { roomId: 'c1', name: 'old', roles: ['member'] },
    })
    const next = roomEventsReducer(seeded, {
      type: 'SUBSCRIPTION_UPSERTED',
      subscription: { roomId: 'c1', name: 'new', roles: ['owner'] },
    })
    expect(next.subscriptions.c1.name).toBe('new')
    expect(next.subscriptions.c1.roles).toEqual(['owner'])
  })

  it('SUBSCRIPTION_UPSERTED with no roomId is a no-op', () => {
    const next = roomEventsReducer(initialState, {
      type: 'SUBSCRIPTION_UPSERTED',
      subscription: { name: 'orphan' },
    })
    expect(next).toBe(initialState)
  })

  it('SUBSCRIPTION_UPSERTED with hasMention: false CLEARS an already-true summary mention (server-canonical)', () => {
    // Pre-existing summary with a live-detected mention.
    const seed = roomEventsReducer(initialState, {
      type: 'ROOM_ADDED',
      room: room('c1', { type: 'channel' }),
    })
    const flagged = roomEventsReducer(seed, {
      type: 'MESSAGE_RECEIVED',
      event: {
        type: 'new_message',
        roomId: 'c1',
        timestamp: Date.now(),
        message: { id: 'm1', content: 'hi @alice', createdAt: '2026-05-13T10:00:00Z' },
        mentions: [{ account: 'alice' }],
        hasMention: true,
      },
    })
    expect(flagged.summaries.find((s) => s.id === 'c1').hasMention).toBe(true)
    // Server says "user marked-as-read".
    const cleared = roomEventsReducer(flagged, {
      type: 'SUBSCRIPTION_UPSERTED',
      subscription: { roomId: 'c1', hasMention: false, roles: ['member'] },
    })
    expect(cleared.summaries.find((s) => s.id === 'c1').hasMention).toBe(false)
  })

  it('ROOM_ADDED merges a pre-existing subscription record into the new summary', () => {
    // Subscription arrives first (as in the live `subscription.update added`
    // → SUBSCRIPTION_UPSERTED → async getRoom → ROOM_ADDED ordering).
    const withSub = roomEventsReducer(initialState, {
      type: 'SUBSCRIPTION_UPSERTED',
      subscription: {
        roomId: 'r-new',
        name: 'bob-dm',
        roles: ['member'],
        hasMention: true,
        alert: true,
      },
    })
    const next = roomEventsReducer(withSub, {
      type: 'ROOM_ADDED',
      room: room('r-new', { type: 'dm', name: '' }),
    })
    const summary = next.summaries.find((s) => s.id === 'r-new')
    expect(summary.hasMention).toBe(true)
    expect(summary.subscriptionName).toBe('bob-dm')
  })

  it('MESSAGE_RECEIVED with hasMention does NOT clobber a BUCKETS_LOADED-seeded mention', () => {
    // Subscription with mention pending; summary already exists.
    const withSummary = roomEventsReducer(initialState, {
      type: 'ROOM_ADDED',
      room: room('c1', { type: 'channel' }),
    })
    const seeded = roomEventsReducer(withSummary, {
      type: 'BUCKETS_LOADED',
      favoriteIds: [],
      appIds: [],
      channelDmIds: ['c1'],
      subscriptions: { c1: { roomId: 'c1', hasMention: true, roles: ['member'] } },
    })
    // A new message arrives that does NOT mention the user.
    const next = roomEventsReducer(seeded, {
      type: 'MESSAGE_RECEIVED',
      event: {
        type: 'new_message',
        roomId: 'c1',
        timestamp: Date.now(),
        message: { id: 'm2', content: 'hi', createdAt: '2026-05-13T10:01:00Z' },
        mentions: [],
        hasMention: false,
      },
    })
    // The seeded `hasMention=true` must survive — MESSAGE_RECEIVED only OR's.
    expect(next.summaries.find((s) => s.id === 'c1').hasMention).toBe(true)
  })

  it('SET_ACTIVE_ROOM clears state.subscriptions[roomId].hasMention so a cold reload does not resurrect the badge', () => {
    // Seed: a room with a pending mention recorded on the subscription.
    const withSummary = roomEventsReducer(initialState, {
      type: 'ROOM_ADDED',
      room: room('c1', { type: 'channel' }),
    })
    const seeded = roomEventsReducer(withSummary, {
      type: 'BUCKETS_LOADED',
      favoriteIds: [],
      appIds: [],
      channelDmIds: ['c1'],
      subscriptions: { c1: { roomId: 'c1', hasMention: true, roles: ['member'], alert: true } },
    })
    expect(seeded.subscriptions.c1.hasMention).toBe(true)
    expect(seeded.summaries.find((s) => s.id === 'c1').hasMention).toBe(true)

    // Open the room.
    const opened = roomEventsReducer(seeded, { type: 'SET_ACTIVE_ROOM', roomId: 'c1' })
    expect(opened.summaries.find((s) => s.id === 'c1').hasMention).toBe(false)
    // The CRITICAL assertion: the per-room subscription record also clears.
    expect(opened.subscriptions.c1.hasMention).toBe(false)
    // Other subscription fields preserved.
    expect(opened.subscriptions.c1.roles).toEqual(['member'])
    expect(opened.subscriptions.c1.alert).toBe(true)
  })

  it('SET_ACTIVE_ROOM is a no-op on the subscriptions map when the room has no pending mention', () => {
    const seeded = roomEventsReducer(initialState, {
      type: 'BUCKETS_LOADED',
      favoriteIds: [],
      appIds: [],
      channelDmIds: ['c1'],
      subscriptions: { c1: { roomId: 'c1', hasMention: false, roles: ['member'] } },
    })
    const opened = roomEventsReducer(seeded, { type: 'SET_ACTIVE_ROOM', roomId: 'c1' })
    // Reference identity preserved — no needless map rebuild.
    expect(opened.subscriptions).toBe(seeded.subscriptions)
  })

  it('SUBSCRIPTION_UPSERTED merges partial deltas into the prior record', () => {
    // Seed with a full record.
    const seeded = roomEventsReducer(initialState, {
      type: 'SUBSCRIPTION_UPSERTED',
      subscription: {
        roomId: 'c1',
        name: 'general',
        roles: ['member'],
        hasMention: false,
        alert: true,
        lastSeenAt: '2026-05-14T10:00:00Z',
        hrInfo: undefined,
      },
    })
    // Partial event: role-update only carries the new roles.
    const next = roomEventsReducer(seeded, {
      type: 'SUBSCRIPTION_UPSERTED',
      subscription: { roomId: 'c1', roles: ['owner'] },
    })
    expect(next.subscriptions.c1.roles).toEqual(['owner'])
    // Other fields preserved.
    expect(next.subscriptions.c1.name).toBe('general')
    expect(next.subscriptions.c1.alert).toBe(true)
    expect(next.subscriptions.c1.lastSeenAt).toBe('2026-05-14T10:00:00Z')
  })

  it('SUBSCRIPTION_UPSERTED with a partial event does NOT clear an existing hasMention on the summary', () => {
    // Pre-existing summary with live-detected mention.
    const withSummary = roomEventsReducer(initialState, {
      type: 'ROOM_ADDED',
      room: room('c1', { type: 'channel' }),
    })
    const mentioned = roomEventsReducer(withSummary, {
      type: 'MESSAGE_RECEIVED',
      event: {
        type: 'new_message',
        roomId: 'c1',
        timestamp: Date.now(),
        message: { id: 'm1', content: 'hi @alice', createdAt: '2026-05-14T10:00:00Z' },
        mentions: [{ account: 'alice' }],
        hasMention: true,
      },
    })
    expect(mentioned.summaries.find((s) => s.id === 'c1').hasMention).toBe(true)

    // Partial role-update event with NO hasMention field.
    const next = roomEventsReducer(mentioned, {
      type: 'SUBSCRIPTION_UPSERTED',
      subscription: { roomId: 'c1', roles: ['owner'] },
    })
    // hasMention must survive — the event didn't carry the field.
    expect(next.summaries.find((s) => s.id === 'c1').hasMention).toBe(true)
  })
})

describe('selectUnreadTotal', () => {
  it('returns zero/false for empty summaries', () => {
    expect(selectUnreadTotal([])).toEqual({ total: 0, hasMention: false })
  })

  it('sums unreadCount across rooms', () => {
    const summaries = [
      { id: 'a', unreadCount: 3, hasMention: false },
      { id: 'b', unreadCount: 2, hasMention: false },
      { id: 'c', unreadCount: 0, hasMention: false },
    ]
    expect(selectUnreadTotal(summaries)).toEqual({ total: 5, hasMention: false })
  })

  it('ORs hasMention across rooms', () => {
    const summaries = [
      { id: 'a', unreadCount: 1, hasMention: false },
      { id: 'b', unreadCount: 4, hasMention: true },
    ]
    expect(selectUnreadTotal(summaries)).toEqual({ total: 5, hasMention: true })
  })

  it('tolerates missing/undefined unreadCount fields', () => {
    const summaries = [{ id: 'a' }, { id: 'b', unreadCount: 2 }]
    expect(selectUnreadTotal(summaries)).toEqual({ total: 2, hasMention: false })
  })

  it('recomputes after a MESSAGE_RECEIVED in a non-active room (count up)', () => {
    const loaded = roomEventsReducer(initialState, {
      type: 'ROOMS_LOADED',
      rooms: [{ id: 'a', name: 'a', type: 'channel', siteId: 's', userCount: 1 }],
    })
    expect(selectUnreadTotal(loaded.summaries)).toEqual({ total: 0, hasMention: false })
    const next = roomEventsReducer(loaded, {
      type: 'MESSAGE_RECEIVED',
      event: { roomId: 'a', message: { id: 'm1', roomId: 'a', content: 'hi' }, hasMention: true },
    })
    expect(selectUnreadTotal(next.summaries)).toEqual({ total: 1, hasMention: true })
  })

  it('recomputes after SET_ACTIVE_ROOM clears that room (count down)', () => {
    let state = roomEventsReducer(initialState, {
      type: 'ROOMS_LOADED',
      rooms: [{ id: 'a', name: 'a', type: 'channel', siteId: 's', userCount: 1 }],
    })
    state = roomEventsReducer(state, {
      type: 'MESSAGE_RECEIVED',
      event: { roomId: 'a', message: { id: 'm1', roomId: 'a', content: 'hi' } },
    })
    expect(selectUnreadTotal(state.summaries).total).toBe(1)
    state = roomEventsReducer(state, { type: 'SET_ACTIVE_ROOM', roomId: 'a' })
    expect(selectUnreadTotal(state.summaries)).toEqual({ total: 0, hasMention: false })
  })
})
