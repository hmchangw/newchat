import { describe, it, expect, afterEach } from 'vitest'
import { renderHook, act } from '@testing-library/react'
import { initialState } from './reducer'
import { useUnreadCount } from './useUnreadCount'

function defineVisibility(state) {
  Object.defineProperty(document, 'visibilityState', { value: state, configurable: true })
}

function setVisibility(state) {
  defineVisibility(state)
  document.dispatchEvent(new Event('visibilitychange'))
}

afterEach(() => defineVisibility('visible'))

const T0 = '2026-08-19T10:00:00Z'
const T1 = '2026-08-19T11:00:00Z'

/** rooms: { id, lastMsgAt, lastSeenAt, muted, threadUnread } */
function stateWith(rooms, { activeRoomId = null } = {}) {
  return {
    ...initialState,
    activeRoomId,
    summaries: rooms.map((r) => ({ id: r.id, lastMsgAt: r.lastMsgAt ?? null })),
    subscriptions: Object.fromEntries(rooms.map((r) => [r.id, {
      roomId: r.id, lastSeenAt: r.lastSeenAt, muted: r.muted ?? false, threadUnread: r.threadUnread,
    }])),
  }
}

describe('useUnreadCount', () => {
  it('returns the number of unread rooms folded from local state', () => {
    const state = stateWith([
      { id: 'a', lastMsgAt: T1, lastSeenAt: T0 },
      { id: 'b', lastMsgAt: T1, lastSeenAt: T0 },
      { id: 'c', lastMsgAt: T0, lastSeenAt: T1 },
    ])
    const { result } = renderHook(() => useUnreadCount(state))
    expect(result.current).toBe(2)
  })

  it('returns 0 when nothing is unread', () => {
    const state = stateWith([{ id: 'a', lastMsgAt: T0, lastSeenAt: T1 }])
    const { result } = renderHook(() => useUnreadCount(state))
    expect(result.current).toBe(0)
  })

  it('excludes the active room while the window is visible', () => {
    const state = stateWith([{ id: 'a', lastMsgAt: T1, lastSeenAt: T0 }], { activeRoomId: 'a' })
    const { result } = renderHook(() => useUnreadCount(state))
    expect(result.current).toBe(0)
  })

  it('counts the active room once the window is hidden, without a state change', () => {
    const state = stateWith([{ id: 'a', lastMsgAt: T1, lastSeenAt: T0 }], { activeRoomId: 'a' })
    const { result } = renderHook(() => useUnreadCount(state))
    expect(result.current).toBe(0)

    act(() => { setVisibility('hidden') })

    expect(result.current).toBe(1)
  })

  it('recounts when new state arrives', () => {
    const before = stateWith([{ id: 'a', lastMsgAt: T0, lastSeenAt: T1 }])
    const after = stateWith([{ id: 'a', lastMsgAt: T1, lastSeenAt: T0 }])
    const { result, rerender } = renderHook(({ s }) => useUnreadCount(s), {
      initialProps: { s: before },
    })
    expect(result.current).toBe(0)

    rerender({ s: after })

    expect(result.current).toBe(1)
  })
})
