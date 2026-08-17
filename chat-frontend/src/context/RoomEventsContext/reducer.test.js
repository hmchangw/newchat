import { describe, it, expect } from 'vitest'
import { BUFFER_MODE, initialState, roomEventsReducer } from './reducer'

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

  it('treats a successfully-decrypted MESSAGE_RECEIVED as a normal new-message event', () => {
    // After Task 25, useRoomSubscriptions decrypts evt.encryptedMessage and
    // dispatches with .message populated AND .encryptedMessage cleared.
    // The reducer should see no difference between this and a plaintext event:
    // no placeholder, content is the decoded plaintext, encrypted flag unset.
    const event = newMessageEvent({
      message: {
        id: 'm-decoded',
        roomId: 'a',
        content: 'decrypted body',
        createdAt: '2026-05-20T00:00:00Z',
        sender: { account: 'bob', engName: 'Bob' },
      },
      lastMsgId: 'm-decoded',
      encryptedMessage: undefined,
    })
    const next = roomEventsReducer(initialState, {
      type: 'MESSAGE_RECEIVED',
      event,
    })
    const inserted = next.roomState.a.messages.find((m) => m.id === 'm-decoded')
    expect(inserted).toBeDefined()
    expect(inserted.content).toBe('decrypted body')
    expect(inserted.encrypted).toBeFalsy()
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

describe('MESSAGE_EDITED / MESSAGE_DELETED — live broadcast', () => {
  const seedRoom = (msgs) => ({
    ...initialState,
    roomState: {
      r1: {
        messages: msgs,
        hasLoadedHistory: true, historyError: null,
        unreadCount: 0, hasMention: false, mentionAll: false,
        lastMsgAt: null, lastMsgId: null,
        bufferMode: 'live', pendingLiveMessages: [], focusMessageId: null,
      },
    },
  })

  it('MESSAGE_EDITED updates content + editedAt for the matching message', () => {
    const seed = seedRoom([{ id: 'a', content: 'old' }, { id: 'b', content: 'untouched' }])
    const out = roomEventsReducer(seed, {
      type: 'MESSAGE_EDITED',
      roomId: 'r1',
      messageId: 'a',
      content: 'new',
      editedAt: '2026-05-19T10:00:00.000Z',
    })
    expect(out.roomState.r1.messages[0]).toEqual({
      id: 'a', content: 'new', editedAt: '2026-05-19T10:00:00.000Z',
    })
    expect(out.roomState.r1.messages[1]).toEqual({ id: 'b', content: 'untouched' })
  })

  it('MESSAGE_EDITED is a no-op when room or message is unknown', () => {
    const seed = seedRoom([{ id: 'a', content: 'x' }])
    const out1 = roomEventsReducer(seed, {
      type: 'MESSAGE_EDITED', roomId: 'unknown', messageId: 'a', content: 'n', editedAt: 't',
    })
    expect(out1).toBe(seed)
    const out2 = roomEventsReducer(seed, {
      type: 'MESSAGE_EDITED', roomId: 'r1', messageId: 'unknown', content: 'n', editedAt: 't',
    })
    expect(out2).toBe(seed)
  })

  it('MESSAGE_DELETED flags the matching message as deleted', () => {
    const seed = seedRoom([{ id: 'a', content: 'x' }, { id: 'b', content: 'y' }])
    const out = roomEventsReducer(seed, {
      type: 'MESSAGE_DELETED', roomId: 'r1', messageId: 'a',
    })
    expect(out.roomState.r1.messages[0].deleted).toBe(true)
    expect(out.roomState.r1.messages[1].deleted).toBeUndefined()
  })

  it('MESSAGE_DELETED is a no-op when message is unknown', () => {
    const seed = seedRoom([{ id: 'a' }])
    const out = roomEventsReducer(seed, {
      type: 'MESSAGE_DELETED', roomId: 'r1', messageId: 'unknown',
    })
    expect(out).toBe(seed)
  })

  // In historical mode (jumped to old message), recent live messages sit in
  // pendingLiveMessages until RESET_TO_LIVE_TAIL merges them back. Edits and
  // deletes arriving in that window MUST patch both buffers or the change is
  // lost on merge.
  const seedHistorical = (msgs, pending) => ({
    ...initialState,
    roomState: {
      r1: {
        messages: msgs,
        hasLoadedHistory: true, historyError: null,
        unreadCount: 0, hasMention: false, mentionAll: false,
        lastMsgAt: null, lastMsgId: null,
        bufferMode: 'historical', pendingLiveMessages: pending, focusMessageId: 'a',
      },
    },
  })

  it('MESSAGE_EDITED patches pendingLiveMessages when the target lives there', () => {
    const seed = seedHistorical(
      [{ id: 'a', content: 'old-a' }],
      [{ id: 'p1', content: 'old-p' }],
    )
    const out = roomEventsReducer(seed, {
      type: 'MESSAGE_EDITED', roomId: 'r1', messageId: 'p1',
      content: 'new-p', editedAt: '2026-05-19T10:00:00.000Z',
    })
    expect(out.roomState.r1.pendingLiveMessages[0]).toEqual({
      id: 'p1', content: 'new-p', editedAt: '2026-05-19T10:00:00.000Z',
    })
    expect(out.roomState.r1.messages).toBe(seed.roomState.r1.messages)
  })

  it('MESSAGE_DELETED patches pendingLiveMessages when the target lives there', () => {
    const seed = seedHistorical(
      [{ id: 'a', content: 'x' }],
      [{ id: 'p1', content: 'y' }],
    )
    const out = roomEventsReducer(seed, {
      type: 'MESSAGE_DELETED', roomId: 'r1', messageId: 'p1',
    })
    expect(out.roomState.r1.pendingLiveMessages[0].deleted).toBe(true)
    expect(out.roomState.r1.messages).toBe(seed.roomState.r1.messages)
  })

  it('MESSAGE_EDITED no-ops when id is absent from both buffers', () => {
    const seed = seedHistorical([{ id: 'a' }], [{ id: 'p1' }])
    const out = roomEventsReducer(seed, {
      type: 'MESSAGE_EDITED', roomId: 'r1', messageId: 'ghost',
      content: 'n', editedAt: 't',
    })
    expect(out).toBe(seed)
  })
})

describe('MESSAGE_REACTED — live reaction toggles', () => {
  const seedRoom = (msgs, pending = []) => ({
    ...initialState,
    roomState: {
      r1: {
        messages: msgs,
        hasLoadedHistory: true, historyError: null,
        unreadCount: 0, hasMention: false, mentionAll: false,
        lastMsgAt: null, lastMsgId: null,
        bufferMode: 'live', pendingLiveMessages: pending, focusMessageId: null,
      },
    },
  })
  const react = (over) => ({
    type: 'MESSAGE_REACTED', roomId: 'r1', messageId: 'a',
    shortcode: '👍', action: 'added', account: 'bob', displayName: 'Bob', ...over,
  })

  it('adds a reactor under the shortcode', () => {
    const out = roomEventsReducer(seedRoom([{ id: 'a', content: 'x' }]), react())
    expect(out.roomState.r1.messages[0].reactions).toEqual({
      '👍': [{ account: 'bob', displayName: 'Bob' }],
    })
  })

  it('appends a second distinct reactor to the same shortcode', () => {
    const s1 = roomEventsReducer(seedRoom([{ id: 'a' }]), react())
    const out = roomEventsReducer(s1, react({ account: 'carol', displayName: 'Carol' }))
    expect(out.roomState.r1.messages[0].reactions['👍']).toEqual([
      { account: 'bob', displayName: 'Bob' },
      { account: 'carol', displayName: 'Carol' },
    ])
  })

  it('is idempotent — re-adding the same account does not duplicate', () => {
    const s1 = roomEventsReducer(seedRoom([{ id: 'a' }]), react())
    const out = roomEventsReducer(s1, react())
    expect(out.roomState.r1.messages[0].reactions['👍']).toHaveLength(1)
  })

  it('removes a reactor and drops the shortcode key when it empties', () => {
    const s1 = roomEventsReducer(seedRoom([{ id: 'a' }]), react())
    const out = roomEventsReducer(s1, react({ action: 'removed' }))
    expect(out.roomState.r1.messages[0].reactions).toEqual({})
  })

  it('patches pendingLiveMessages when the target lives there', () => {
    const seed = seedRoom([{ id: 'a' }], [{ id: 'p1' }])
    const out = roomEventsReducer(seed, react({ messageId: 'p1' }))
    expect(out.roomState.r1.pendingLiveMessages[0].reactions['👍']).toHaveLength(1)
    expect(out.roomState.r1.messages).toBe(seed.roomState.r1.messages)
  })

  it('no-ops when the room or message is unknown', () => {
    const seed = seedRoom([{ id: 'a' }])
    expect(roomEventsReducer(seed, react({ roomId: 'nope' }))).toBe(seed)
    expect(roomEventsReducer(seed, react({ messageId: 'ghost' }))).toBe(seed)
  })
})

describe('MESSAGE_RECEIVED — server echo overwrites optimistic createdAt', () => {
  // Regression for: every thread reply silently failing message-worker's
  // IF EXISTS stamp on messages_by_id because the parent message in
  // frontend state kept the OPTIMISTIC `new Date().toISOString()` value
  // (client clock) instead of the SERVER's canonical createdAt. Then
  // `threadParentMessageCreatedAt` derived from the parent's `createdAt`
  // pointed at a time Cassandra never stored, IF EXISTS missed, and the
  // thread_room_id stamp on the parent silently failed — leading to
  // empty thread fetches on close+reopen.
  it('replaces the optimistic message with the server broadcast for the same id (preserving _local)', () => {
    const seed = {
      ...initialState,
      summaries: [{ id: 'r1', name: 'g', type: 'channel', siteId: 's', userCount: 1, lastMsgAt: null, unreadCount: 0, hasMention: false, mentionAll: false }],
      activeRoomId: 'r1',
      roomState: {
        r1: {
          messages: [
            // OPTIMISTIC — client clock, has _local flag
            { id: 'msg-1', content: 'hi', createdAt: '2026-05-18T10:23:45.123Z', _local: true },
          ],
          hasLoadedHistory: true, historyError: null,
          unreadCount: 0, hasMention: false, mentionAll: false,
          lastMsgAt: null, lastMsgId: null,
          bufferMode: 'live', pendingLiveMessages: [], focusMessageId: null,
        },
      },
    }
    // Server broadcast echo with the SAME id but the authoritative server createdAt.
    const out = roomEventsReducer(seed, {
      type: 'MESSAGE_RECEIVED',
      event: {
        roomId: 'r1',
        lastMsgAt: '2026-05-18T10:23:45.789Z',
        lastMsgId: 'msg-1',
        message: {
          id: 'msg-1',
          content: 'hi',
          createdAt: '2026-05-18T10:23:45.789Z', // server's clock
          sender: { account: 'alice' },
        },
      },
    })
    const msg = out.roomState.r1.messages.find((m) => m.id === 'msg-1')
    expect(msg.createdAt).toBe('2026-05-18T10:23:45.789Z')
    // _local flag preserved so any pending-row UI affordance stays.
    expect(msg._local).toBe(true)
    // sender survived from the broadcast.
    expect(msg.sender?.account).toBe('alice')
    // exactly one entry — no duplicate row.
    expect(out.roomState.r1.messages).toHaveLength(1)
  })

  it('still appends as a new row when no existing entry has that id', () => {
    const seed = {
      ...initialState,
      summaries: [{ id: 'r1', name: 'g', type: 'channel', siteId: 's', userCount: 1, lastMsgAt: null, unreadCount: 0, hasMention: false, mentionAll: false }],
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
      event: {
        roomId: 'r1',
        message: { id: 'msg-1', content: 'hi', createdAt: '2026-05-18T10:23:45.789Z' },
      },
    })
    expect(out.roomState.r1.messages.map((m) => m.id)).toEqual(['msg-1'])
  })
})

describe('MESSAGE_RECEIVED — thread-reply tcount bump', () => {
  it('increments parent.tcount when a thread reply arrives and the parent is in the buffer', () => {
    const seed = {
      ...initialState,
      summaries: [{ id: 'r1', name: 'general', type: 'channel', siteId: 's', userCount: 1, lastMsgAt: null, unreadCount: 0, hasMention: false, mentionAll: false }],
      activeRoomId: 'r1',
      roomState: {
        r1: {
          messages: [{ id: 'parent-1', content: 'parent', tcount: 0 }],
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
    const parent = out.roomState.r1.messages.find((m) => m.id === 'parent-1')
    expect(parent.tcount).toBe(1)
    expect(parent.threadReplyIds.has('reply-1')).toBe(true)
    // The reply itself does NOT enter the main feed.
    expect(out.roomState.r1.messages.find((m) => m.id === 'reply-1')).toBeUndefined()
  })

  it('dedupes against the sender-side OWN_THREAD_REPLY_SENT echo by reply ID', () => {
    const seedWithOwnBump = roomEventsReducer({
      ...initialState,
      roomState: {
        r1: {
          messages: [{ id: 'parent-1', tcount: 0 }],
          hasLoadedHistory: true, historyError: null,
          unreadCount: 0, hasMention: false, mentionAll: false,
          lastMsgAt: null, lastMsgId: null,
          bufferMode: 'live', pendingLiveMessages: [], focusMessageId: null,
        },
      },
    }, { type: 'OWN_THREAD_REPLY_SENT', roomId: 'r1', parentId: 'parent-1', replyId: 'reply-1' })
    expect(seedWithOwnBump.roomState.r1.messages[0].tcount).toBe(1)
    // Echo arrives via MESSAGE_RECEIVED with the same reply ID; the
    // reducer must NOT bump again.
    const echoed = roomEventsReducer(seedWithOwnBump, {
      type: 'MESSAGE_RECEIVED',
      event: {
        roomId: 'r1',
        message: { id: 'reply-1', threadParentMessageId: 'parent-1' },
      },
    })
    expect(echoed).toBe(seedWithOwnBump)
  })

  it('is a no-op when the parent message is not in the buffer (e.g. main feed not loaded)', () => {
    const seed = {
      ...initialState,
      summaries: [{ id: 'r1', name: 'general', type: 'channel', siteId: 's', userCount: 1, lastMsgAt: null, unreadCount: 0, hasMention: false, mentionAll: false }],
      activeRoomId: 'r1',
      roomState: {
        r1: {
          messages: [],
          hasLoadedHistory: false, historyError: null,
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
        message: { id: 'reply-1', threadParentMessageId: 'parent-1' },
      },
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

  it('ROOM_ADDED still runs bucket-Set maintenance when summary already exists (ROOMS_LOADED+live-add race)', () => {
    // Race scenario: listRooms returns the room first → ROOMS_LOADED
    // seeds the summary; then `subscription.update added` fires →
    // ROOM_ADDED runs but the summary is already present. The old
    // early-return left channelDmIds empty → useSidebarSections's
    // partition dropped the room silently.
    const a = room('a', { type: 'channel' })
    const seeded = roomEventsReducer(initialState, { type: 'ROOMS_LOADED', rooms: [a] })
    expect(seeded.channelDmIds.size).toBe(0)
    const next = roomEventsReducer(seeded, { type: 'ROOM_ADDED', room: a })
    expect(next.channelDmIds.has('a')).toBe(true)
    expect(next.summaries).toHaveLength(1)
  })

  it('ROOM_ADDED is a true no-op when summary AND correct bucket are already present', () => {
    // Idempotency check: a duplicate ROOM_ADDED (e.g. React StrictMode
    // double-fire) should return the same state object — no spurious
    // re-renders downstream.
    const a = room('a', { type: 'channel' })
    const after1 = roomEventsReducer(initialState, { type: 'ROOM_ADDED', room: a })
    const after2 = roomEventsReducer(after1, { type: 'ROOM_ADDED', room: a })
    expect(after2).toBe(after1)
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

  it('ROOMS_LOADED enriches incoming summaries with already-seeded subscriptions (BUCKETS_LOADED race)', () => {
    // Race scenario: BUCKETS_LOADED lands BEFORE ROOMS_LOADED (the
    // bootstrap fan-out runs both in parallel). The subscription with
    // a server-canonical mention should NOT be wiped out by
    // toSummary's zero-init when ROOMS_LOADED arrives.
    const seeded = roomEventsReducer(initialState, {
      type: 'BUCKETS_LOADED',
      favoriteIds: [],
      appIds: [],
      channelDmIds: ['r1'],
      subscriptions: {
        r1: { roomId: 'r1', name: 'frontend-team', hasMention: true, roles: ['member'] },
      },
    })
    // summaries was empty when BUCKETS_LOADED ran — no enrichment yet.
    expect(seeded.summaries).toHaveLength(0)
    const next = roomEventsReducer(seeded, {
      type: 'ROOMS_LOADED',
      rooms: [room('r1', { name: 'old-canonical' })],
    })
    const s = next.summaries.find((x) => x.id === 'r1')
    expect(s.hasMention).toBe(true)
    expect(s.subscriptionName).toBe('frontend-team')
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

describe('roomEventsReducer: msgRecvSeq (unread-badge refetch trigger)', () => {
  it('initial state starts at 0', () => {
    expect(initialState.msgRecvSeq).toBe(0)
  })

  it('increments on a received message in a non-active room', () => {
    const next = roomEventsReducer(initialState, {
      type: 'MESSAGE_RECEIVED',
      event: newMessageEvent(),
    })
    expect(next.msgRecvSeq).toBe(1)
  })

  it('increments on a received message in the active room too (any message)', () => {
    const state = { ...initialState, activeRoomId: 'a' }
    const next = roomEventsReducer(state, {
      type: 'MESSAGE_RECEIVED',
      event: newMessageEvent(),
    })
    expect(next.msgRecvSeq).toBe(1)
  })

  it('does not increment on a duplicate (no-op) message', () => {
    const s1 = roomEventsReducer(initialState, {
      type: 'MESSAGE_RECEIVED',
      event: newMessageEvent(),
    })
    const s2 = roomEventsReducer(s1, {
      type: 'MESSAGE_RECEIVED',
      event: newMessageEvent(),
    })
    expect(s1.msgRecvSeq).toBe(1)
    expect(s2.msgRecvSeq).toBe(1)
  })

  it('does not increment on a thread-reply message (filtered, no-op)', () => {
    const next = roomEventsReducer(initialState, {
      type: 'MESSAGE_RECEIVED',
      event: newMessageEvent({
        message: {
          id: 'tr1',
          roomId: 'a',
          content: 'reply',
          createdAt: '2026-04-17T12:00:00Z',
          threadParentMessageId: 'p1',
        },
      }),
    })
    expect(next.msgRecvSeq).toBe(0)
  })

  it('is preserved by non-message actions (SET_ACTIVE_ROOM)', () => {
    const afterMsg = roomEventsReducer(initialState, {
      type: 'MESSAGE_RECEIVED',
      event: newMessageEvent(),
    })
    const next = roomEventsReducer(afterMsg, {
      type: 'SET_ACTIVE_ROOM',
      roomId: 'a',
    })
    expect(next.msgRecvSeq).toBe(1)
  })
})

describe('roomEventsReducer: readSeq (post-mark-read refetch trigger)', () => {
  it('initial state starts at 0', () => {
    expect(initialState.readSeq).toBe(0)
  })

  it('ROOM_READ_SYNCED increments readSeq', () => {
    const next = roomEventsReducer(initialState, { type: 'ROOM_READ_SYNCED' })
    expect(next.readSeq).toBe(1)
    const next2 = roomEventsReducer(next, { type: 'ROOM_READ_SYNCED' })
    expect(next2.readSeq).toBe(2)
  })

  it('ROOM_READ_SYNCED does not touch msgRecvSeq', () => {
    const next = roomEventsReducer(initialState, { type: 'ROOM_READ_SYNCED' })
    expect(next.msgRecvSeq).toBe(0)
  })

  it('a received message does not touch readSeq', () => {
    const next = roomEventsReducer(initialState, {
      type: 'MESSAGE_RECEIVED',
      event: newMessageEvent(),
    })
    expect(next.readSeq).toBe(0)
  })

  it('is preserved by other actions (SET_ACTIVE_ROOM)', () => {
    const synced = roomEventsReducer(initialState, { type: 'ROOM_READ_SYNCED' })
    const next = roomEventsReducer(synced, {
      type: 'SET_ACTIVE_ROOM',
      roomId: 'a',
    })
    expect(next.readSeq).toBe(1)
  })
})

// Helper: build an ascending block of history messages m<start>..m<end>.
function histBlock(ids, roomId = 'a') {
  return ids.map((id, i) => ({
    id,
    roomId,
    content: id,
    createdAt: new Date(Date.UTC(2026, 3, 17, 8, i, 0)).toISOString(),
    sender: { account: 'bob' },
  }))
}

describe('roomEventsReducer: older-message pagination', () => {
  it('HISTORY_LOADED records hasMoreOlder from the action', () => {
    const next = roomEventsReducer(initialState, {
      type: 'HISTORY_LOADED',
      roomId: 'a',
      messages: histBlock(['m1', 'm2']),
      hasMoreOlder: true,
    })
    expect(next.roomState.a.hasMoreOlder).toBe(true)
    expect(next.roomState.a.loadingOlder).toBe(false)
  })

  it('a fresh room state defaults hasMoreOlder true / loadingOlder false', () => {
    const next = roomEventsReducer(initialState, {
      type: 'HISTORY_LOADED',
      roomId: 'a',
      messages: histBlock(['m1']),
      hasMoreOlder: false,
    })
    // hasMoreOlder reflects the action; loadingOlder starts false.
    expect(next.roomState.a.hasMoreOlder).toBe(false)
    expect(next.roomState.a.loadingOlder).toBe(false)
  })

  it('HISTORY_OLDER_LOADING flips loadingOlder true and clears historyError', () => {
    const loaded = roomEventsReducer(initialState, {
      type: 'HISTORY_LOADED',
      roomId: 'a',
      messages: histBlock(['m5', 'm6']),
      hasMoreOlder: true,
    })
    const next = roomEventsReducer(loaded, { type: 'HISTORY_OLDER_LOADING', roomId: 'a' })
    expect(next.roomState.a.loadingOlder).toBe(true)
    expect(next.roomState.a.historyError).toBe(null)
  })

  it('HISTORY_OLDER_LOADED prepends the older block ahead of existing messages', () => {
    const loaded = roomEventsReducer(initialState, {
      type: 'HISTORY_LOADED',
      roomId: 'a',
      messages: histBlock(['m5', 'm6']),
      hasMoreOlder: true,
    })
    const loading = roomEventsReducer(loaded, { type: 'HISTORY_OLDER_LOADING', roomId: 'a' })
    const next = roomEventsReducer(loading, {
      type: 'HISTORY_OLDER_LOADED',
      roomId: 'a',
      messages: histBlock(['m3', 'm4']),
      hasMoreOlder: true,
    })
    expect(next.roomState.a.messages.map((m) => m.id)).toEqual(['m3', 'm4', 'm5', 'm6'])
    expect(next.roomState.a.loadingOlder).toBe(false)
    expect(next.roomState.a.hasMoreOlder).toBe(true)
  })

  it('HISTORY_OLDER_LOADED dedupes ids already present', () => {
    const loaded = roomEventsReducer(initialState, {
      type: 'HISTORY_LOADED',
      roomId: 'a',
      messages: histBlock(['m4', 'm5']),
      hasMoreOlder: true,
    })
    const next = roomEventsReducer(loaded, {
      type: 'HISTORY_OLDER_LOADED',
      roomId: 'a',
      messages: histBlock(['m3', 'm4']), // m4 overlaps
      hasMoreOlder: false,
    })
    expect(next.roomState.a.messages.map((m) => m.id)).toEqual(['m3', 'm4', 'm5'])
    expect(next.roomState.a.hasMoreOlder).toBe(false)
  })

  it('HISTORY_OLDER_LOADED does NOT trim the front (keeps older beyond the live cap)', () => {
    // Seed a full-cap buffer of newest messages, then prepend older ones.
    const newest = histBlock(Array.from({ length: 200 }, (_, i) => `n${i}`))
    const loaded = roomEventsReducer(initialState, {
      type: 'HISTORY_LOADED',
      roomId: 'a',
      messages: newest,
      hasMoreOlder: true,
    })
    const older = histBlock(['o1', 'o2', 'o3'])
    const next = roomEventsReducer(loaded, {
      type: 'HISTORY_OLDER_LOADED',
      roomId: 'a',
      messages: older,
      hasMoreOlder: true,
    })
    expect(next.roomState.a.messages.length).toBe(203)
    expect(next.roomState.a.messages.slice(0, 3).map((m) => m.id)).toEqual(['o1', 'o2', 'o3'])
  })

  it('HISTORY_OLDER_FAILED clears loadingOlder and keeps hasMoreOlder for retry', () => {
    const loaded = roomEventsReducer(initialState, {
      type: 'HISTORY_LOADED',
      roomId: 'a',
      messages: histBlock(['m5']),
      hasMoreOlder: true,
    })
    const loading = roomEventsReducer(loaded, { type: 'HISTORY_OLDER_LOADING', roomId: 'a' })
    const next = roomEventsReducer(loading, { type: 'HISTORY_OLDER_FAILED', roomId: 'a' })
    expect(next.roomState.a.loadingOlder).toBe(false)
    expect(next.roomState.a.hasMoreOlder).toBe(true)
  })

  it('REPLACE_ROOM_BUFFER (jump) resets hasMoreOlder true so a jumped window can page up', () => {
    const next = roomEventsReducer(initialState, {
      type: 'REPLACE_ROOM_BUFFER',
      roomId: 'a',
      messages: histBlock(['j1', 'j2']),
      focusMessageId: 'j1',
    })
    expect(next.roomState.a.hasMoreOlder).toBe(true)
    expect(next.roomState.a.loadingOlder).toBe(false)
    expect(next.roomState.a.bufferMode).toBe(BUFFER_MODE.HISTORICAL)
  })
})

describe('roomEventsReducer previews', () => {
  function emptyBuffer() {
    return {
      messages: [], hasLoadedHistory: false, historyError: null, unreadCount: 0,
      hasMention: false, mentionAll: false, lastMsgAt: null, lastMsgId: null,
      bufferMode: 'live', pendingLiveMessages: [], focusMessageId: null,
      hasMoreOlder: true, loadingOlder: false,
    }
  }

  const wirePreview = (over = {}) => ({
    messageId: 'm1',
    sender: { account: 'alice', displayName: 'Alice Chen' },
    content: 'hello **there**',
    createdAt: '2026-08-14T10:00:00Z',
    ...over,
  })

  it('starts empty', () => {
    expect(initialState.previews).toEqual({})
  })

  it('BUCKETS_LOADED seeds a preview from sub.room.previewMessage', () => {
    const next = roomEventsReducer(initialState, {
      type: 'BUCKETS_LOADED',
      favoriteIds: [], appIds: [], channelDmIds: ['r1'],
      rooms: [{ id: 'r1', name: 'General', type: 'channel', siteId: 'site-A', userCount: 2 }],
      subscriptions: { r1: { roomId: 'r1', name: 'General', room: { previewMessage: wirePreview() } } },
    })
    expect(next.previews.r1).toEqual({
      messageId: 'm1', senderName: 'Alice Chen', text: 'hello there',
      createdAt: '2026-08-14T10:00:00Z',
    })
  })

  it('BUCKETS_LOADED leaves a room without a previewMessage absent from the map', () => {
    const next = roomEventsReducer(initialState, {
      type: 'BUCKETS_LOADED',
      favoriteIds: [], appIds: [], channelDmIds: ['r1'],
      rooms: [{ id: 'r1', name: 'General', type: 'channel', siteId: 'site-A', userCount: 2 }],
      subscriptions: { r1: { roomId: 'r1', name: 'General', room: {} } },
    })
    expect(next.previews.r1).toBeUndefined()
  })

  it('BUCKETS_LOADED does not clobber a fresher live preview already in state', () => {
    // Regression: the DM subscription goes live BEFORE fetchSidebarBuckets
    // resolves, so a live message can land and be written to previews via
    // MESSAGE_RECEIVED before this dispatch fires. That live entry is NEWER
    // than the list snapshot's previewMessage and must survive.
    const seeded = {
      ...initialState,
      previews: { r1: { messageId: 'm-live', senderName: 'Live Sender', text: 'just arrived' } },
    }
    const next = roomEventsReducer(seeded, {
      type: 'BUCKETS_LOADED',
      favoriteIds: [], appIds: [], channelDmIds: ['r1'],
      rooms: [{ id: 'r1', name: 'General', type: 'channel', siteId: 'site-A', userCount: 2 }],
      subscriptions: { r1: { roomId: 'r1', name: 'General', room: { previewMessage: wirePreview({ messageId: 'm-stale' }) } } },
    })
    expect(next.previews.r1).toEqual({ messageId: 'm-live', senderName: 'Live Sender', text: 'just arrived' })
  })

  it('BUCKETS_LOADED still seeds a room with no prior preview alongside one that is guarded', () => {
    const seeded = {
      ...initialState,
      previews: { r1: { messageId: 'm-live', senderName: 'Live Sender', text: 'just arrived' } },
    }
    const next = roomEventsReducer(seeded, {
      type: 'BUCKETS_LOADED',
      favoriteIds: [], appIds: [], channelDmIds: ['r1', 'r2'],
      rooms: [
        { id: 'r1', name: 'General', type: 'channel', siteId: 'site-A', userCount: 2 },
        { id: 'r2', name: 'Other', type: 'channel', siteId: 'site-A', userCount: 2 },
      ],
      subscriptions: {
        r1: { roomId: 'r1', name: 'General', room: { previewMessage: wirePreview({ messageId: 'm-stale' }) } },
        r2: { roomId: 'r2', name: 'Other', room: { previewMessage: wirePreview({ messageId: 'm-new-r2' }) } },
      },
    })
    // r1's live entry survives; r2 (no prior entry) is seeded normally.
    expect(next.previews.r1.messageId).toBe('m-live')
    expect(next.previews.r2.messageId).toBe('m-new-r2')
  })

  it('MESSAGE_RECEIVED overwrites the preview for a room with no message buffer', () => {
    const next = roomEventsReducer(initialState, {
      type: 'MESSAGE_RECEIVED',
      event: {
        roomId: 'r1',
        message: { id: 'm2', content: 'newest', userDisplayName: 'Bob Lin', createdAt: '2026-08-14T11:00:00Z' },
      },
    })
    expect(next.previews.r1).toEqual({
      messageId: 'm2', senderName: 'Bob Lin', text: 'newest',
      createdAt: '2026-08-14T11:00:00Z',
    })
  })

  it('MESSAGE_RECEIVED uses the attachment fallback when the body is empty', () => {
    const next = roomEventsReducer(initialState, {
      type: 'MESSAGE_RECEIVED',
      event: {
        roomId: 'r1',
        message: {
          id: 'm3', content: '', userDisplayName: 'Bob Lin',
          createdAt: '2026-08-14T11:00:00Z', attachments: [{ imageUrl: '/a.png' }],
        },
      },
    })
    expect(next.previews.r1.text).toBe('Photo')
  })

  it('MESSAGE_RECEIVED keeps the previews reference stable on a content-identical write', () => {
    // No roomState.r1 is seeded here, so this exercises the NEW-message
    // branch (existingIdx < 0), not the existingIdx >= 0 server-echo branch.
    // previews is computed once and shared across every return point though,
    // so the reference-stability guarantee is exercised either way: the
    // incoming message renders to the exact same stored preview already in
    // state, so the previews map must not be reallocated — a fresh object
    // here would invalidate useSidebarSections' memo for every room in the
    // sidebar.
    const seeded = {
      ...initialState,
      previews: { r1: { messageId: 'm2', senderName: 'Bob Lin', text: 'newest' } },
    }
    const next = roomEventsReducer(seeded, {
      type: 'MESSAGE_RECEIVED',
      event: {
        roomId: 'r1',
        message: { id: 'm2', content: 'newest', userDisplayName: 'Bob Lin', createdAt: '2026-08-14T11:00:00Z' },
      },
    })
    expect(next.previews).toBe(seeded.previews)
  })

  it('MESSAGE_RECEIVED resolves mentions from the event when the message carries none', () => {
    // Message.Mentions is never populated by the gatekeeper — mentions ride
    // the event, not the message. Without the fallback, a live preview would
    // show the raw @account instead of the resolved display name.
    const next = roomEventsReducer(initialState, {
      type: 'MESSAGE_RECEIVED',
      event: {
        roomId: 'r1',
        mentions: [{ account: 'alice', engName: 'Alice Chen' }],
        message: {
          id: 'm10',
          content: 'hey @alice check this out',
          userDisplayName: 'Bob Lin',
          createdAt: '2026-08-14T11:00:00Z',
          // no mentions on the message itself
        },
      },
    })
    expect(next.previews.r1.text).toBe('hey @Alice Chen check this out')
  })

  it('MESSAGE_RECEIVED ignores a thread reply', () => {
    // Seed roomState.r1 with the parent message present so the dispatch
    // actually reaches the thread-reply path (state.roomState[roomId] must
    // exist and contain the parent, or the reducer returns early at the
    // `!tPrev` / `parentIdx < 0` guards before ever reaching the preview
    // computation this test means to exercise).
    const seeded = {
      ...initialState,
      previews: { r1: { messageId: 'm1', senderName: 'A', text: 'first' } },
      roomState: { r1: { ...emptyBuffer(), messages: [{ id: 'm1', content: 'first' }] } },
    }
    const next = roomEventsReducer(seeded, {
      type: 'MESSAGE_RECEIVED',
      event: {
        roomId: 'r1',
        message: { id: 'm9', content: 'reply', threadParentMessageId: 'm1', createdAt: '2026-08-14T12:00:00Z' },
      },
    })
    expect(next.previews.r1.messageId).toBe('m1')
    // Proves the thread-reply path actually ran (not short-circuited by a
    // missing parent): the reducer bumps the parent's tcount by 1 on a
    // successfully-matched, non-duplicate thread reply.
    expect(next.roomState.r1.messages.find((m) => m.id === 'm1').tcount).toBe(1)
  })

  it('MESSAGE_SENT_LOCAL previews the local send optimistically', () => {
    const next = roomEventsReducer(initialState, {
      type: 'MESSAGE_SENT_LOCAL',
      roomId: 'r1',
      message: { id: 'm4', content: 'typed by me', userDisplayName: 'Me', createdAt: '2026-08-14T13:00:00Z', _local: true },
    })
    expect(next.previews.r1).toEqual({
      messageId: 'm4', senderName: 'Me', text: 'typed by me',
      createdAt: '2026-08-14T13:00:00Z',
    })
  })

  it('MESSAGE_EDITED_LOCAL rewrites the preview only when the edited message is the one on display', () => {
    const seeded = {
      ...initialState,
      previews: { r1: { messageId: 'm5', senderName: 'Me', text: 'before' } },
      roomState: { r1: { ...emptyBuffer(), messages: [{ id: 'm5', content: 'before' }] } },
    }
    const hit = roomEventsReducer(seeded, {
      type: 'MESSAGE_EDITED_LOCAL', roomId: 'r1', messageId: 'm5',
      content: 'after', editedAt: '2026-08-14T14:00:00Z',
    })
    expect(hit.previews.r1.text).toBe('after')

    const miss = roomEventsReducer(seeded, {
      type: 'MESSAGE_EDITED_LOCAL', roomId: 'r1', messageId: 'm5-other',
      content: 'irrelevant', editedAt: '2026-08-14T14:00:00Z',
    })
    expect(miss.previews.r1.text).toBe('before')
  })

  it('MESSAGE_EDITED_LOCAL falls back to the attachment label when the edited content is empty', () => {
    // Fix 4 regression: editing a photo's caption to empty must not blank
    // the snippet to a dangling "Alice Chen: " — previewSnippet (not
    // previewText) must run so the attachment fallback kicks in. The action
    // itself carries no attachments; they come from the buffered message
    // being edited.
    const seeded = {
      ...initialState,
      previews: { r1: { messageId: 'm5', senderName: 'Alice Chen', text: 'a caption' } },
      roomState: {
        r1: {
          ...emptyBuffer(),
          messages: [{ id: 'm5', content: 'a caption', attachments: [{ imageUrl: '/a.png' }] }],
        },
      },
    }
    const out = roomEventsReducer(seeded, {
      type: 'MESSAGE_EDITED_LOCAL', roomId: 'r1', messageId: 'm5',
      content: '', editedAt: '2026-08-14T14:00:00Z',
    })
    expect(out.previews.r1.text).toBe('Photo')
  })

  it('MESSAGE_DELETED_LOCAL leaves the preview alone', () => {
    const seeded = {
      ...initialState,
      previews: { r1: { messageId: 'm6', senderName: 'Me', text: 'doomed' } },
      roomState: { r1: { ...emptyBuffer(), messages: [{ id: 'm6', content: 'doomed' }] } },
    }
    const next = roomEventsReducer(seeded, { type: 'MESSAGE_DELETED_LOCAL', roomId: 'r1', messageId: 'm6' })
    expect(next.previews.r1.text).toBe('doomed')
  })

  it('SUBSCRIPTION_UPSERTED never seeds a preview', () => {
    // Its `added` payload embeds a `room` object, which makes it look like a
    // seeding source — but docs/client-api.md specifies previewMessage is
    // always omitted there. Regression guard against wiring it up.
    const next = roomEventsReducer(initialState, {
      type: 'SUBSCRIPTION_UPSERTED',
      subscription: {
        roomId: 'r1', roomType: 'channel', siteId: 'site-A', name: 'General',
        room: { previewMessage: wirePreview() },
      },
    })
    expect(next.previews).toEqual({})
  })

  it('ROOM_REMOVED drops the room preview', () => {
    const seeded = { ...initialState, previews: { r1: { messageId: 'm7', senderName: 'A', text: 'x' } } }
    const next = roomEventsReducer(seeded, { type: 'ROOM_REMOVED', roomId: 'r1' })
    expect(next.previews.r1).toBeUndefined()
  })

  it('RESET clears every preview', () => {
    const seeded = { ...initialState, previews: { r1: { messageId: 'm8', senderName: 'A', text: 'x' } } }
    expect(roomEventsReducer(seeded, { type: 'RESET' }).previews).toEqual({})
  })

  describe('ROOM_PREVIEW_UPDATED', () => {
    const seeded = () => ({
      ...initialState,
      previews: { r1: { messageId: 'm1', senderName: 'Alice Chen', text: 'old body' } },
    })

    it('overwrites from the event previewMessage', () => {
      const next = roomEventsReducer(seeded(), {
        type: 'ROOM_PREVIEW_UPDATED',
        roomId: 'r1',
        previewMessage: {
          messageId: 'm2',
          sender: { account: 'bob', displayName: 'Bob Lin' },
          content: 'edited body',
          createdAt: '2026-08-14T15:00:00Z',
        },
      })
      expect(next.previews.r1).toEqual({
        messageId: 'm2', senderName: 'Bob Lin', text: 'edited body',
        createdAt: '2026-08-14T15:00:00Z',
      })
    })

    it('updates a room that has no message buffer', () => {
      const next = roomEventsReducer(initialState, {
        type: 'ROOM_PREVIEW_UPDATED',
        roomId: 'r-unopened',
        previewMessage: {
          messageId: 'm3',
          sender: { account: 'bob', displayName: 'Bob Lin' },
          content: 'from a room I never opened',
          createdAt: '2026-08-14T15:00:00Z',
        },
      })
      expect(next.previews['r-unopened'].text).toBe('from a room I never opened')
    })

    it('clears when the deleted message is the one on display and no preview follows', () => {
      const next = roomEventsReducer(seeded(), {
        type: 'ROOM_PREVIEW_UPDATED', roomId: 'r1', deletedMessageId: 'm1',
      })
      expect(next.previews.r1).toBeUndefined()
    })

    it('does NOT clear when the deleted message is a different one', () => {
      const next = roomEventsReducer(seeded(), {
        type: 'ROOM_PREVIEW_UPDATED', roomId: 'r1', deletedMessageId: 'm-other',
      })
      expect(next.previews.r1.text).toBe('old body')
    })

    it('leaves the preview alone when neither a previewMessage nor a deletedMessageId is given', () => {
      const next = roomEventsReducer(seeded(), { type: 'ROOM_PREVIEW_UPDATED', roomId: 'r1' })
      expect(next.previews.r1.text).toBe('old body')
    })

    it('ignores an action with no roomId', () => {
      const state = seeded()
      expect(roomEventsReducer(state, { type: 'ROOM_PREVIEW_UPDATED' })).toBe(state)
    })

    it('leaves state untouched when a delete arrives for a room with no stored preview', () => {
      // The `!cur` guard must short-circuit before cur.messageId is read. Nothing
      // else in the suite covers deletedMessageId present + no stored entry, and
      // this branch is the clear rule's core guarantee.
      const state = { ...initialState, previews: {} }
      const next = roomEventsReducer(state, {
        type: 'ROOM_PREVIEW_UPDATED', roomId: 'r-none', deletedMessageId: 'm1',
      })
      expect(next).toBe(state)
    })

    it('leaves other rooms untouched when a delete arrives for a room with no stored preview', () => {
      const state = {
        ...initialState,
        previews: { 'r-other': { messageId: 'm1', senderName: 'Alice Chen', text: 'kept' } },
      }
      const next = roomEventsReducer(state, {
        type: 'ROOM_PREVIEW_UPDATED', roomId: 'r-none', deletedMessageId: 'm1',
      })
      expect(next).toBe(state)
      expect(next.previews['r-other'].text).toBe('kept')
    })

    describe('encrypted-placeholder guard (Fix 3)', () => {
      it('does not overwrite a stored encrypted placeholder with the wire plaintext for the SAME message', () => {
        const state = {
          ...initialState,
          previews: {
            r1: {
              messageId: 'm-enc', senderName: 'Alice Chen', text: '[encrypted message]',
              createdAt: '2026-08-14T15:00:00Z', encrypted: true,
            },
          },
        }
        const next = roomEventsReducer(state, {
          type: 'ROOM_PREVIEW_UPDATED',
          roomId: 'r1',
          previewMessage: {
            messageId: 'm-enc',
            sender: { account: 'bob', displayName: 'Bob Lin' },
            content: 'the actual plaintext body',
            createdAt: '2026-08-14T15:00:00Z',
          },
        })
        expect(next).toBe(state)
      })

      it('DOES overwrite an encrypted placeholder when the wire preview is for a DIFFERENT (newer) message', () => {
        const state = {
          ...initialState,
          previews: {
            r1: {
              messageId: 'm-enc', senderName: 'Alice Chen', text: '[encrypted message]',
              createdAt: '2026-08-14T15:00:00Z', encrypted: true,
            },
          },
        }
        const next = roomEventsReducer(state, {
          type: 'ROOM_PREVIEW_UPDATED',
          roomId: 'r1',
          previewMessage: {
            messageId: 'm-newer',
            sender: { account: 'bob', displayName: 'Bob Lin' },
            content: 'a newer plaintext message',
            createdAt: '2026-08-14T16:00:00Z',
          },
        })
        expect(next.previews.r1).toEqual({
          messageId: 'm-newer', senderName: 'Bob Lin', text: 'a newer plaintext message',
          createdAt: '2026-08-14T16:00:00Z',
        })
      })
    })

    describe('recency guard (Fix 7)', () => {
      const seededWithCreatedAt = () => ({
        ...initialState,
        previews: {
          r1: {
            messageId: 'm1', senderName: 'Alice Chen', text: 'old body',
            createdAt: '2026-08-14T15:00:00Z',
          },
        },
      })

      it('rejects a wire preview strictly older than the stored one', () => {
        const state = seededWithCreatedAt()
        const next = roomEventsReducer(state, {
          type: 'ROOM_PREVIEW_UPDATED',
          roomId: 'r1',
          previewMessage: {
            messageId: 'm-late-arriving',
            sender: { account: 'bob', displayName: 'Bob Lin' },
            content: 'resolved before the newer message but delivered after',
            createdAt: '2026-08-14T14:00:00Z',
          },
        })
        expect(next).toBe(state)
      })

      it('accepts a wire preview newer than the stored one', () => {
        const next = roomEventsReducer(seededWithCreatedAt(), {
          type: 'ROOM_PREVIEW_UPDATED',
          roomId: 'r1',
          previewMessage: {
            messageId: 'm-new',
            sender: { account: 'bob', displayName: 'Bob Lin' },
            content: 'genuinely newer',
            createdAt: '2026-08-14T16:00:00Z',
          },
        })
        expect(next.previews.r1).toEqual({
          messageId: 'm-new', senderName: 'Bob Lin', text: 'genuinely newer',
          createdAt: '2026-08-14T16:00:00Z',
        })
      })

      it('accepts the write when either timestamp is missing or unparseable, rather than dropping it', () => {
        // Missing stored createdAt (e.g. a preview seeded before this field existed).
        const noStoredTimestamp = {
          ...initialState,
          previews: { r1: { messageId: 'm1', senderName: 'Alice Chen', text: 'old body' } },
        }
        const next1 = roomEventsReducer(noStoredTimestamp, {
          type: 'ROOM_PREVIEW_UPDATED',
          roomId: 'r1',
          previewMessage: {
            messageId: 'm2', sender: { account: 'bob', displayName: 'Bob Lin' },
            content: 'accepted', createdAt: '2026-08-14T10:00:00Z',
          },
        })
        expect(next1.previews.r1.messageId).toBe('m2')

        // Unparseable incoming createdAt.
        const next2 = roomEventsReducer(seededWithCreatedAt(), {
          type: 'ROOM_PREVIEW_UPDATED',
          roomId: 'r1',
          previewMessage: {
            messageId: 'm3', sender: { account: 'bob', displayName: 'Bob Lin' },
            content: 'also accepted', createdAt: 'not-a-real-timestamp',
          },
        })
        expect(next2.previews.r1.messageId).toBe('m3')
      })

      it('does not interfere with the clear-on-delete rule', () => {
        const next = roomEventsReducer(seededWithCreatedAt(), {
          type: 'ROOM_PREVIEW_UPDATED', roomId: 'r1', deletedMessageId: 'm1',
        })
        expect(next.previews.r1).toBeUndefined()
      })
    })
  })
})

describe('roomEventsReducer: system messages are never live previews (Fix 1)', () => {
  const SYSTEM_TYPES = [
    'room_created',
    'members_added',
    'member_removed',
    'member_left',
    'room_renamed',
    'room_restricted',
    'teams_meet_started',
  ]

  it.each(SYSTEM_TYPES)('MESSAGE_RECEIVED with type=%s does not seed or overwrite the room preview', (type) => {
    const seeded = {
      ...initialState,
      previews: { r1: { messageId: 'm-old', senderName: 'Alice Chen', text: 'existing preview' } },
    }
    const next = roomEventsReducer(seeded, {
      type: 'MESSAGE_RECEIVED',
      event: {
        roomId: 'r1',
        message: {
          id: 'm-sys', type, content: 'Bob renamed the channel to #general',
          createdAt: '2026-08-14T12:00:00Z', sender: { account: 'bob', engName: 'Bob' },
        },
      },
    })
    // Message still lands in the room timeline (system messages ARE
    // rendered, just not previewed) — only the preview must be untouched.
    expect(next.roomState.r1.messages.some((m) => m.id === 'm-sys')).toBe(true)
    expect(next.previews.r1).toBe(seeded.previews.r1)
  })

  it('does NOT exclude type="important" — it previews like a normal message', () => {
    const next = roomEventsReducer(initialState, {
      type: 'MESSAGE_RECEIVED',
      event: {
        roomId: 'r1',
        message: {
          id: 'm-imp', type: 'important', content: 'read this',
          createdAt: '2026-08-14T12:00:00Z', sender: { account: 'bob', engName: 'Bob' },
        },
      },
    })
    expect(next.previews.r1).toMatchObject({ messageId: 'm-imp', text: 'read this' })
  })

  it('excludes a message carrying sysMsgData even without a recognized type (belt and braces)', () => {
    const seeded = {
      ...initialState,
      previews: { r1: { messageId: 'm-old', senderName: 'Alice Chen', text: 'existing preview' } },
    }
    const next = roomEventsReducer(seeded, {
      type: 'MESSAGE_RECEIVED',
      event: {
        roomId: 'r1',
        message: {
          id: 'm-sys2', sysMsgData: '{"meetingId":"abc"}', content: 'Meeting started',
          createdAt: '2026-08-14T12:00:00Z',
        },
      },
    })
    expect(next.previews.r1).toBe(seeded.previews.r1)
  })
})

describe('roomEventsReducer: MESSAGE_RECEIVED historical-mode duplicate guard applies previews (Fix 2)', () => {
  function historicalRoom(pending = []) {
    return {
      messages: [{ id: 'm1', content: 'old', createdAt: '2026-08-14T09:00:00Z' }],
      hasLoadedHistory: true, historyError: null, unreadCount: 0,
      hasMention: false, mentionAll: false, lastMsgAt: null, lastMsgId: null,
      bufferMode: BUFFER_MODE.HISTORICAL, pendingLiveMessages: pending, focusMessageId: 'm1',
      hasMoreOlder: true, loadingOlder: false,
    }
  }

  it('a MESSAGE_RECEIVED duplicate (already in messages) still applies the computed preview', () => {
    // Regression: the duplicate-message guard used to `return state`
    // unconditionally, discarding the previews map computed just above it —
    // an optimistic preview was never upgraded by the server echo while the
    // room sat in historical buffer mode.
    const seeded = {
      ...initialState,
      previews: { r1: { messageId: 'm1', senderName: 'Me', text: 'optimistic body' } },
      roomState: { r1: historicalRoom() },
    }
    const next = roomEventsReducer(seeded, {
      type: 'MESSAGE_RECEIVED',
      event: {
        roomId: 'r1',
        message: {
          id: 'm1', content: 'server-confirmed body', createdAt: '2026-08-14T09:00:00Z',
          sender: { account: 'me', engName: 'Me' },
        },
      },
    })
    expect(next.previews.r1.text).toBe('server-confirmed body')
    // The duplicate guard's own job (not touching messages/pending) still holds.
    expect(next.roomState.r1.messages).toBe(seeded.roomState.r1.messages)
  })

  it('a duplicate already in pendingLiveMessages still applies the computed preview', () => {
    const seeded = {
      ...initialState,
      previews: { r1: { messageId: 'm2', senderName: 'Me', text: 'optimistic body' } },
      roomState: {
        r1: historicalRoom([{ id: 'm2', content: 'pending', createdAt: '2026-08-14T09:30:00Z' }]),
      },
    }
    const next = roomEventsReducer(seeded, {
      type: 'MESSAGE_RECEIVED',
      event: {
        roomId: 'r1',
        message: {
          id: 'm2', content: 'server-confirmed body', createdAt: '2026-08-14T09:30:00Z',
          sender: { account: 'me', engName: 'Me' },
        },
      },
    })
    expect(next.previews.r1.text).toBe('server-confirmed body')
  })

  it('a true no-op duplicate (identical resulting preview) returns the identical state reference', () => {
    const seeded = {
      ...initialState,
      previews: { r1: { messageId: 'm1', senderName: 'Bob', text: 'old', createdAt: '2026-08-14T09:00:00Z' } },
      roomState: {
        r1: historicalRoom(),
      },
    }
    const next = roomEventsReducer(seeded, {
      type: 'MESSAGE_RECEIVED',
      event: {
        roomId: 'r1',
        message: {
          id: 'm1', content: 'old', createdAt: '2026-08-14T09:00:00Z',
          sender: { account: 'bob', engName: 'Bob' },
        },
      },
    })
    expect(next).toBe(seeded)
  })
})
