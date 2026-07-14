import { describe, it, expect, vi, beforeEach } from 'vitest'
import { renderHook } from '@testing-library/react'

vi.mock('@/context/AuthContext', () => ({ useAuth: vi.fn() }))

import { useHandleAdminError } from './useHandleAdminError'
import { useAuth } from '@/context/AuthContext'
import { AsyncJobError } from '@/api'

describe('useHandleAdminError', () => {
  let logout

  beforeEach(() => {
    logout = vi.fn()
    useAuth.mockReturnValue({ logout })
  })

  it('logs the admin out and returns null on invalid_token', () => {
    const { result } = renderHook(() => useHandleAdminError())
    const err = new AsyncJobError('expired', {
      code: 'unauthenticated',
      reason: 'invalid_token',
    })

    const message = result.current(err)

    expect(logout).toHaveBeenCalledTimes(1)
    expect(message).toBeNull()
  })

  it('does not log out and returns the formatted message for other errors', () => {
    const { result } = renderHook(() => useHandleAdminError())
    const err = new AsyncJobError('boom', { code: 'internal' })

    const message = result.current(err)

    expect(logout).not.toHaveBeenCalled()
    expect(message).toBe('boom')
  })

  it('does not log out for a not_admin error — callers own that state themselves', () => {
    const { result } = renderHook(() => useHandleAdminError())
    const err = new AsyncJobError('forbidden', { code: 'forbidden', reason: 'not_admin' })

    const message = result.current(err)

    expect(logout).not.toHaveBeenCalled()
    expect(message).toBe('You need admin access to do that.')
  })

  it('formats a non-AsyncJobError without touching logout', () => {
    const { result } = renderHook(() => useHandleAdminError())

    const message = result.current(new Error('network down'))

    expect(logout).not.toHaveBeenCalled()
    expect(message).toBe('network down')
  })
})
