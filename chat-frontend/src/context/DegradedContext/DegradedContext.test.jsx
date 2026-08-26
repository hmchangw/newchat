import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { renderHook, act } from '@testing-library/react'
import { DegradedProvider, useDegraded } from './DegradedContext'

const wrapper = ({ children }) => <DegradedProvider>{children}</DegradedProvider>

describe('DegradedContext', () => {
  beforeEach(() => vi.useFakeTimers())
  afterEach(() => vi.useRealTimers())

  it('starts healthy', () => {
    const { result } = renderHook(() => useDegraded(), { wrapper })
    expect(result.current.historyDegraded).toBe(false)
  })

  it('goes degraded on an unavailable history failure', () => {
    const { result } = renderHook(() => useDegraded(), { wrapper })
    act(() => result.current.noteHistoryFailure({ code: 'unavailable' }))
    expect(result.current.historyDegraded).toBe(true)
  })

  it('goes degraded on an internal history failure', () => {
    const { result } = renderHook(() => useDegraded(), { wrapper })
    act(() => result.current.noteHistoryFailure({ code: 'internal' }))
    expect(result.current.historyDegraded).toBe(true)
  })

  it('goes degraded on a thread_start_unavailable refusal', () => {
    const { result } = renderHook(() => useDegraded(), { wrapper })
    act(() => result.current.noteHistoryFailure({ reason: 'thread_start_unavailable' }))
    expect(result.current.historyDegraded).toBe(true)
  })

  it('ignores a terminal error — not_found is not an outage', () => {
    const { result } = renderHook(() => useDegraded(), { wrapper })
    act(() => result.current.noteHistoryFailure({ code: 'not_found' }))
    expect(result.current.historyDegraded).toBe(false)
  })

  it('clears on a successful history load', () => {
    const { result } = renderHook(() => useDegraded(), { wrapper })
    act(() => result.current.noteHistoryFailure({ code: 'unavailable' }))
    act(() => result.current.noteHistorySuccess())
    expect(result.current.historyDegraded).toBe(false)
  })

  it('self-clears after the TTL so a stuck flag cannot outlive the outage', () => {
    const { result } = renderHook(() => useDegraded(), { wrapper })
    act(() => result.current.noteHistoryFailure({ code: 'unavailable' }))
    act(() => vi.advanceTimersByTime(60_000))
    expect(result.current.historyDegraded).toBe(false)
  })

  it('a repeated failure re-arms the TTL so a flapping outage does not prematurely clear', () => {
    const { result } = renderHook(() => useDegraded(), { wrapper })
    act(() => result.current.noteHistoryFailure({ code: 'unavailable' }))
    act(() => vi.advanceTimersByTime(30_000))
    expect(result.current.historyDegraded).toBe(true)
    // A second failure at t=30s must reset the TTL window rather than
    // stacking a second timer alongside the first.
    act(() => result.current.noteHistoryFailure({ code: 'unavailable' }))
    // Advance past the ORIGINAL 60s window (30s + 40s = 70s elapsed since the
    // first failure). If the flag cleared here, the re-arm would have failed
    // to reset the timer and a flapping outage could prematurely re-enable
    // thread starts.
    act(() => vi.advanceTimersByTime(40_000))
    expect(result.current.historyDegraded).toBe(true)
    // The re-armed timer (started at t=30s) expires at t=90s; the remaining
    // 20s clears it.
    act(() => vi.advanceTimersByTime(20_000))
    expect(result.current.historyDegraded).toBe(false)
  })
})
