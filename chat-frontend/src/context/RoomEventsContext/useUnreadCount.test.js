import { describe, it, expect, afterEach, beforeEach, vi } from 'vitest'
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

beforeEach(() => vi.useFakeTimers({ shouldAdvanceTime: true }))
afterEach(() => {
  vi.useRealTimers()
  defineVisibility('visible')
})

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

describe('useUnreadCount reconcile', () => {
  const RECONCILE_MS = 5 * 60 * 1000

  /** One unread room, so the fold reports 1. */
  const UNREAD_ONE = stateWith([{ id: 'a', lastMsgAt: T1, lastSeenAt: T0 }])

  function harness({ serverCounts = [1], resync = vi.fn().mockResolvedValue(undefined) } = {}) {
    let call = 0
    const getCount = vi.fn().mockImplementation(() => {
      const n = serverCounts[Math.min(call++, serverCounts.length - 1)]
      return n instanceof Error ? Promise.reject(n) : Promise.resolve({ count: n })
    })
    return { getCount, resync }
  }

  it('does not resync while the server agrees with the fold', async () => {
    const { getCount, resync } = harness({ serverCounts: [1, 1, 1] })
    renderHook(() => useUnreadCount(UNREAD_ONE, { getCount, resync }))
    await act(async () => { await vi.advanceTimersByTimeAsync(RECONCILE_MS * 2 + 10) })

    expect(getCount).toHaveBeenCalled()
    expect(resync).not.toHaveBeenCalled()
  })

  it('does not resync on a single mismatch', async () => {
    // One disagreement is the active-room write window, not drift.
    const { getCount, resync } = harness({ serverCounts: [2, 1, 1] })
    renderHook(() => useUnreadCount(UNREAD_ONE, { getCount, resync }))
    await act(async () => { await vi.advanceTimersByTimeAsync(RECONCILE_MS * 2 + 10) })

    expect(resync).not.toHaveBeenCalled()
  })

  it('resyncs after two consecutive mismatches', async () => {
    const { getCount, resync } = harness({ serverCounts: [2, 2] })
    renderHook(() => useUnreadCount(UNREAD_ONE, { getCount, resync }))
    await act(async () => { await vi.advanceTimersByTimeAsync(RECONCILE_MS * 2 + 10) })

    expect(resync).toHaveBeenCalledTimes(1)
  })

  it('keeps the folded value when the reconcile RPC fails', async () => {
    const { getCount, resync } = harness({ serverCounts: [new Error('no responders')] })
    const { result } = renderHook(() => useUnreadCount(UNREAD_ONE, { getCount, resync }))
    await act(async () => { await vi.advanceTimersByTimeAsync(RECONCILE_MS + 10) })

    expect(result.current).toBe(1)
    expect(resync).not.toHaveBeenCalled()
  })

  it('reconciles while the document is hidden', async () => {
    // The taskbar badge matters most while backgrounded, so the interval
    // must not be gated on visibility.
    defineVisibility('hidden')
    const { getCount, resync } = harness({ serverCounts: [1] })
    renderHook(() => useUnreadCount(UNREAD_ONE, { getCount, resync }))
    await act(async () => { await vi.advanceTimersByTimeAsync(RECONCILE_MS + 10) })

    expect(getCount).toHaveBeenCalled()
  })

  it('reconciles when the window becomes visible', async () => {
    defineVisibility('hidden')
    const { getCount, resync } = harness({ serverCounts: [1] })
    renderHook(() => useUnreadCount(UNREAD_ONE, { getCount, resync }))
    getCount.mockClear()

    await act(async () => { setVisibility('visible') })

    expect(getCount).toHaveBeenCalled()
  })

  it('works with no reconcile wiring at all', () => {
    const { result } = renderHook(() => useUnreadCount(UNREAD_ONE))
    expect(result.current).toBe(1)
  })
})
