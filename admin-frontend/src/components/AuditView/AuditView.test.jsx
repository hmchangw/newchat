import { describe, it, expect, vi, beforeEach } from 'vitest'
import { act, render, screen, fireEvent, waitFor } from '@testing-library/react'

vi.mock('@/context/AuthContext', () => ({ useAuth: vi.fn() }))
vi.mock('@/api', async (importOriginal) => {
  const actual = await importOriginal()
  return { ...actual, listAudit: vi.fn() }
})

import AuditView from './AuditView'
import { useAuth } from '@/context/AuthContext'
import { listAudit, AsyncJobError } from '@/api'

const ENTRY_1 = {
  id: 'a-1',
  actorUserId: 'u-1',
  actorAccount: 'root',
  action: 'user.create',
  targetUserId: 'u-2',
  targetAccount: 'alice',
  siteId: 'site-1',
  timestamp: 1700000000000,
}

const ENTRY_2 = {
  id: 'a-2',
  actorUserId: 'u-1',
  actorAccount: 'root',
  action: 'user.deactivate',
  targetUserId: 'u-3',
  siteId: 'site-1',
  timestamp: 1700000100000,
}

let logout

beforeEach(() => {
  vi.clearAllMocks()
  logout = vi.fn()
  useAuth.mockReturnValue({ session: { authToken: 'tok', account: 'root', siteId: 'site-1' }, logout })
  listAudit.mockResolvedValue({ entries: [ENTRY_1, ENTRY_2], total: 2 })
})

describe('AuditView', () => {
  it('calls listAudit(token, {page:1, limit:20}) on mount and renders rows in received order', async () => {
    render(<AuditView />)
    await waitFor(() => expect(listAudit).toHaveBeenCalledWith('tok', { page: 1, limit: 20 }))

    const rows = await screen.findAllByRole('row')
    // header row + 2 data rows, in the order the backend returned them
    expect(rows).toHaveLength(3)
    expect(rows[1]).toHaveTextContent('alice')
    expect(rows[1]).toHaveTextContent('user.create')
    expect(rows[2]).toHaveTextContent('u-3')
    expect(rows[2]).toHaveTextContent('user.deactivate')
  })

  it('re-queries with {action} once the debounce elapses when the action filter changes', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      render(<AuditView />)
      await waitFor(() => expect(listAudit).toHaveBeenCalledWith('tok', { page: 1, limit: 20 }))

      fireEvent.change(screen.getByLabelText(/filter by action/i), {
        target: { value: 'user.create' },
      })
      await act(async () => {
        await vi.advanceTimersByTimeAsync(400)
      })

      await waitFor(() =>
        expect(listAudit).toHaveBeenCalledWith('tok', {
          action: 'user.create',
          page: 1,
          limit: 20,
        }),
      )
    } finally {
      vi.useRealTimers()
    }
  })

  it('re-queries with {targetAccount} once the debounce elapses when the target filter changes', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      render(<AuditView />)
      await waitFor(() => expect(listAudit).toHaveBeenCalledWith('tok', { page: 1, limit: 20 }))

      fireEvent.change(screen.getByLabelText(/filter by target account/i), {
        target: { value: 'grace' },
      })
      await act(async () => {
        await vi.advanceTimersByTimeAsync(400)
      })

      await waitFor(() =>
        expect(listAudit).toHaveBeenCalledWith('tok', { targetAccount: 'grace', page: 1, limit: 20 }),
      )
    } finally {
      vi.useRealTimers()
    }
  })

  it('debounces rapid filter keystrokes into a single query', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      render(<AuditView />)
      await waitFor(() => expect(listAudit).toHaveBeenCalledTimes(1))

      const input = screen.getByLabelText(/filter by action/i)
      fireEvent.change(input, { target: { value: 'u' } })
      fireEvent.change(input, { target: { value: 'us' } })
      fireEvent.change(input, { target: { value: 'user' } })

      await act(async () => {
        await vi.advanceTimersByTimeAsync(400)
      })

      // 1 mount call + exactly 1 debounced call, not one per keystroke
      expect(listAudit).toHaveBeenCalledTimes(2)
      expect(listAudit).toHaveBeenLastCalledWith('tok', { action: 'user', page: 1, limit: 20 })
    } finally {
      vi.useRealTimers()
    }
  })

  it('renders a not-authorized state on a 403 not_admin error', async () => {
    listAudit.mockRejectedValue(
      new AsyncJobError('forbidden', { code: 'forbidden', reason: 'not_admin' }),
    )
    render(<AuditView />)
    expect(await screen.findByText(/not authorized/i)).toBeInTheDocument()
    expect(logout).not.toHaveBeenCalled()
  })

  it('shows a formatted error message on other failures', async () => {
    listAudit.mockRejectedValue(new AsyncJobError('boom', { code: 'internal' }))
    render(<AuditView />)
    expect(await screen.findByText(/boom/i)).toBeInTheDocument()
    expect(logout).not.toHaveBeenCalled()
  })

  it('logs the admin out instead of showing a banner on invalid_token', async () => {
    listAudit.mockRejectedValue(
      new AsyncJobError('expired', { code: 'unauthenticated', reason: 'invalid_token' }),
    )
    render(<AuditView />)
    await waitFor(() => expect(logout).toHaveBeenCalledTimes(1))
    expect(screen.queryByText(/expired/i)).not.toBeInTheDocument()
    expect(screen.queryByText(/not authorized/i)).not.toBeInTheDocument()
  })

  describe('pagination', () => {
    it('disables Prev on page 1 and requests page 2 on Next', async () => {
      listAudit.mockResolvedValue({ entries: [ENTRY_1, ENTRY_2], total: 50 })
      render(<AuditView />)
      await waitFor(() => expect(listAudit).toHaveBeenCalledWith('tok', { page: 1, limit: 20 }))

      expect(screen.getByRole('button', { name: /prev/i })).toBeDisabled()

      fireEvent.click(screen.getByRole('button', { name: /next/i }))
      await waitFor(() => expect(listAudit).toHaveBeenCalledWith('tok', { page: 2, limit: 20 }))
    })

    it('goes back to page 1 with Prev after advancing to page 2', async () => {
      listAudit.mockResolvedValue({ entries: [ENTRY_1, ENTRY_2], total: 50 })
      render(<AuditView />)
      await waitFor(() => expect(listAudit).toHaveBeenCalledWith('tok', { page: 1, limit: 20 }))

      fireEvent.click(screen.getByRole('button', { name: /next/i }))
      await waitFor(() => expect(listAudit).toHaveBeenCalledWith('tok', { page: 2, limit: 20 }))

      fireEvent.click(screen.getByRole('button', { name: /prev/i }))
      await waitFor(() => expect(listAudit).toHaveBeenCalledWith('tok', { page: 1, limit: 20 }))
    })

    it('disables Next once the current page already covers total', async () => {
      listAudit.mockResolvedValue({ entries: [ENTRY_1, ENTRY_2], total: 20 })
      render(<AuditView />)
      await waitFor(() => expect(listAudit).toHaveBeenCalledWith('tok', { page: 1, limit: 20 }))
      expect(await screen.findByRole('button', { name: /next/i })).toBeDisabled()
    })

    it('does not clobber a manual goToPage with a redundant debounced reset to page 1', async () => {
      listAudit.mockResolvedValue({ entries: [ENTRY_1, ENTRY_2], total: 50 })
      vi.useFakeTimers({ shouldAdvanceTime: true })
      try {
        render(<AuditView />)
        await waitFor(() => expect(listAudit).toHaveBeenCalledWith('tok', { page: 1, limit: 20 }))

        // Filter change starts the debounce timer, but the user pages before it fires.
        fireEvent.change(screen.getByLabelText(/filter by action/i), {
          target: { value: 'user.create' },
        })
        fireEvent.click(screen.getByRole('button', { name: /next/i }))
        await waitFor(() =>
          expect(listAudit).toHaveBeenCalledWith('tok', {
            action: 'user.create',
            page: 2,
            limit: 20,
          }),
        )
        expect(await screen.findByText(/page 2/i)).toBeInTheDocument()

        // Now the debounce fires with the same (already-applied) filter — it must
        // not re-fetch or reset the page the user just navigated to.
        await act(async () => {
          await vi.advanceTimersByTimeAsync(400)
        })

        expect(listAudit).toHaveBeenCalledTimes(2)
        expect(screen.getByText(/page 2/i)).toBeInTheDocument()
      } finally {
        vi.useRealTimers()
      }
    })

    it('resets to page 1 once the debounce elapses after a filter change', async () => {
      listAudit.mockResolvedValue({ entries: [ENTRY_1, ENTRY_2], total: 50 })
      vi.useFakeTimers({ shouldAdvanceTime: true })
      try {
        render(<AuditView />)
        await waitFor(() => expect(listAudit).toHaveBeenCalledWith('tok', { page: 1, limit: 20 }))

        fireEvent.click(screen.getByRole('button', { name: /next/i }))
        await waitFor(() => expect(listAudit).toHaveBeenCalledWith('tok', { page: 2, limit: 20 }))

        fireEvent.change(screen.getByLabelText(/filter by action/i), {
          target: { value: 'user.create' },
        })
        await act(async () => {
          await vi.advanceTimersByTimeAsync(400)
        })

        await waitFor(() =>
          expect(listAudit).toHaveBeenCalledWith('tok', {
            action: 'user.create',
            page: 1,
            limit: 20,
          }),
        )
      } finally {
        vi.useRealTimers()
      }
    })
  })

  it('drops a stale out-of-order response so a slow earlier request cannot overwrite newer rows', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      render(<AuditView />)
      await waitFor(() => expect(listAudit).toHaveBeenCalledWith('tok', { page: 1, limit: 20 }))

      let resolveSlow
      const slow = new Promise((resolve) => {
        resolveSlow = resolve
      })
      // First debounced filter change: a slow request that resolves late.
      listAudit.mockReturnValueOnce(slow)
      fireEvent.change(screen.getByLabelText(/filter by action/i), {
        target: { value: 'first' },
      })
      await act(async () => {
        await vi.advanceTimersByTimeAsync(400)
      })
      await waitFor(() =>
        expect(listAudit).toHaveBeenCalledWith('tok', { action: 'first', page: 1, limit: 20 }),
      )

      // Second debounced filter change: a fast request that resolves first.
      listAudit.mockResolvedValueOnce({ entries: [ENTRY_2], total: 2 })
      fireEvent.change(screen.getByLabelText(/filter by action/i), {
        target: { value: 'second' },
      })
      await act(async () => {
        await vi.advanceTimersByTimeAsync(400)
      })
      await waitFor(() =>
        expect(listAudit).toHaveBeenCalledWith('tok', { action: 'second', page: 1, limit: 20 }),
      )

      // The fast (newer) response landed.
      await waitFor(() => expect(screen.getByText('user.deactivate')).toBeInTheDocument())
      expect(screen.queryByText('user.create')).not.toBeInTheDocument()

      // Now the slow (stale) response resolves — it must be ignored, not
      // overwrite the newer rows already on screen.
      await act(async () => {
        resolveSlow({ entries: [ENTRY_1], total: 2 })
        await Promise.resolve()
      })

      expect(screen.queryByText('user.create')).not.toBeInTheDocument()
      expect(screen.getByText('user.deactivate')).toBeInTheDocument()
    } finally {
      vi.useRealTimers()
    }
  })
})
