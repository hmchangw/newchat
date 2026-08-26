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
})
