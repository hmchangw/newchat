import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { renderHook, act } from '@testing-library/react'
import { useDebouncedSearch } from './useDebouncedSearch'

describe('useDebouncedSearch', () => {
  beforeEach(() => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
  })
  afterEach(() => {
    vi.useRealTimers()
  })

  it('calls onSearch with the new query only after the delay elapses', () => {
    const onSearch = vi.fn()
    const { result } = renderHook(() => useDebouncedSearch({ delay: 300, onSearch }))

    act(() => result.current.setQuery('ali'))
    expect(onSearch).not.toHaveBeenCalled()

    act(() => vi.advanceTimersByTime(299))
    expect(onSearch).not.toHaveBeenCalled()

    act(() => vi.advanceTimersByTime(1))
    expect(onSearch).toHaveBeenCalledTimes(1)
    expect(onSearch).toHaveBeenCalledWith('ali')
  })

  it('resets the timer on rapid retyping — only the final value lands after one delay window', () => {
    const onSearch = vi.fn()
    const { result } = renderHook(() => useDebouncedSearch({ delay: 300, onSearch }))

    act(() => result.current.setQuery('a'))
    act(() => vi.advanceTimersByTime(200))
    act(() => result.current.setQuery('al'))
    act(() => vi.advanceTimersByTime(200))
    act(() => result.current.setQuery('ali'))

    // Neither earlier keystroke's timer should have fired yet.
    expect(onSearch).not.toHaveBeenCalled()

    act(() => vi.advanceTimersByTime(300))
    expect(onSearch).toHaveBeenCalledTimes(1)
    expect(onSearch).toHaveBeenCalledWith('ali')
  })

  it('honors a custom delay', () => {
    const onSearch = vi.fn()
    const { result } = renderHook(() => useDebouncedSearch({ delay: 1000, onSearch }))

    act(() => result.current.setQuery('bob'))
    act(() => vi.advanceTimersByTime(999))
    expect(onSearch).not.toHaveBeenCalled()

    act(() => vi.advanceTimersByTime(1))
    expect(onSearch).toHaveBeenCalledWith('bob')
  })

  it('cancels a pending timer on unmount — no callback fires after unmount', () => {
    const onSearch = vi.fn()
    const { result, unmount } = renderHook(() => useDebouncedSearch({ delay: 300, onSearch }))

    act(() => result.current.setQuery('ali'))
    unmount()

    act(() => vi.advanceTimersByTime(500))
    expect(onSearch).not.toHaveBeenCalled()
  })
})
