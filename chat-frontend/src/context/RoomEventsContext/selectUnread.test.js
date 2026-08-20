import { describe, it, expect } from 'vitest'
import { initialState } from './reducer'
import { selectUnreadRoomIds } from './selectUnread'

/** Build a state whose summaries and subscriptions are joined by roomId.
 *  `rooms` entries: { id, lastMsgAt, lastSeenAt, muted, threadUnread }. */
function stateWith(rooms, { activeRoomId = null } = {}) {
  return {
    ...initialState,
    activeRoomId,
    summaries: rooms.map((r) => ({ id: r.id, name: r.id, type: 'channel', siteId: 'site-A', lastMsgAt: r.lastMsgAt ?? null })),
    subscriptions: Object.fromEntries(rooms.map((r) => [r.id, {
      roomId: r.id,
      siteId: 'site-A',
      lastSeenAt: r.lastSeenAt,
      muted: r.muted ?? false,
      threadUnread: r.threadUnread,
    }])),
  }
}

const T0 = '2026-08-19T10:00:00Z'
const T1 = '2026-08-19T11:00:00Z'

describe('selectUnreadRoomIds', () => {
  const cases = [
    {
      name: 'message newer than lastSeenAt counts',
      rooms: [{ id: 'a', lastMsgAt: T1, lastSeenAt: T0 }],
      expected: ['a'],
    },
    {
      name: 'message older than lastSeenAt does not count',
      rooms: [{ id: 'a', lastMsgAt: T0, lastSeenAt: T1 }],
      expected: [],
    },
    {
      name: 'never-read room with messages counts',
      rooms: [{ id: 'a', lastMsgAt: T1, lastSeenAt: undefined }],
      expected: ['a'],
    },
    {
      name: 'room with no messages never counts',
      rooms: [{ id: 'a', lastMsgAt: null, lastSeenAt: undefined }],
      expected: [],
    },
    {
      name: 'unread followed thread counts even when messages are read',
      rooms: [{ id: 'a', lastMsgAt: T0, lastSeenAt: T1, threadUnread: ['p1'] }],
      expected: ['a'],
    },
    {
      name: 'empty threadUnread does not count',
      rooms: [{ id: 'a', lastMsgAt: T0, lastSeenAt: T1, threadUnread: [] }],
      expected: [],
    },
    {
      name: 'muted room never counts, by message or by thread',
      rooms: [{ id: 'a', lastMsgAt: T1, lastSeenAt: T0, muted: true, threadUnread: ['p1'] }],
      expected: [],
    },
    {
      name: 'a room unread by BOTH message and thread is counted once',
      rooms: [{ id: 'a', lastMsgAt: T1, lastSeenAt: T0, threadUnread: ['p1'] }],
      expected: ['a'],
    },
    {
      name: 'no rooms yields nothing',
      rooms: [],
      expected: [],
    },
  ]

  for (const c of cases) {
    it(c.name, () => {
      expect(selectUnreadRoomIds(stateWith(c.rooms), true)).toEqual(c.expected)
    })
  }

  it('excludes the active room while the window is visible', () => {
    const state = stateWith([{ id: 'a', lastMsgAt: T1, lastSeenAt: T0 }], { activeRoomId: 'a' })
    expect(selectUnreadRoomIds(state, true)).toEqual([])
  })

  it('INCLUDES the active room while the window is hidden', () => {
    // A minimized window has no room the user is actually reading — the
    // taskbar badge must still count it.
    const state = stateWith([{ id: 'a', lastMsgAt: T1, lastSeenAt: T0 }], { activeRoomId: 'a' })
    expect(selectUnreadRoomIds(state, false)).toEqual(['a'])
  })

  it('counts the visibly-active room when it has an unread thread', () => {
    // Sitting in a room's main feed is not reading its threads, and the
    // server counts it either way — suppressing it here would put the fold
    // permanently below the server and spin the reconcile.
    const state = stateWith(
      [{ id: 'a', lastMsgAt: T1, lastSeenAt: T0, threadUnread: ['p1'] }],
      { activeRoomId: 'a' },
    )
    expect(selectUnreadRoomIds(state, true)).toEqual(['a'])
  })

  it('counts a room with no subscription record by message alone', () => {
    // A room can land in summaries before its subscription row arrives.
    const state = { ...initialState, summaries: [{ id: 'a', lastMsgAt: T1 }], subscriptions: {} }
    expect(selectUnreadRoomIds(state, true)).toEqual(['a'])
  })
})
