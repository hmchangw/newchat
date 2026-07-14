import { describe, it, expect, vi, beforeEach } from 'vitest'
import { act, render, screen, fireEvent, waitFor } from '@testing-library/react'

vi.mock('@/context/AuthContext', () => ({ useAuth: vi.fn() }))
vi.mock('@/api', async (importOriginal) => {
  const actual = await importOriginal()
  return { ...actual, listUsers: vi.fn() }
})

vi.mock('../CreateUserForm', () => ({
  default: ({ onCreated }) => (
    <div role="dialog" aria-label="create user">
      <button type="button" onClick={onCreated}>
        Fake create
      </button>
    </div>
  ),
}))

vi.mock('../SetPasswordDialog', () => ({
  default: ({ onUpdated }) => (
    <div role="dialog" aria-label="set password">
      <button type="button" onClick={onUpdated}>
        Fake set password
      </button>
    </div>
  ),
}))

import UsersPage from './UsersPage'
import { useAuth } from '@/context/AuthContext'
import { listUsers, AsyncJobError } from '@/api'

const USER = {
  id: 'u-1',
  account: 'alice',
  siteId: 'site-1',
  engName: 'Alice',
  chineseName: '爱丽丝',
  roles: ['admin'],
  deactivated: false,
  requirePasswordChange: false,
}

let logout

beforeEach(() => {
  vi.clearAllMocks()
  logout = vi.fn()
  useAuth.mockReturnValue({
    session: { authToken: 'tok', account: 'root', siteId: 'site-1' },
    logout,
  })
  listUsers.mockResolvedValue({ users: [USER], total: 1 })
})

describe('UsersPage', () => {
  it('calls listUsers(token, {page:1, limit:20}) on mount and renders rows', async () => {
    render(<UsersPage />)
    await waitFor(() => expect(listUsers).toHaveBeenCalledWith('tok', { page: 1, limit: 20 }))
    expect(await screen.findByText('alice')).toBeInTheDocument()
  })

  it('re-queries with {q} after the search box debounces', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      render(<UsersPage />)
      await waitFor(() => expect(listUsers).toHaveBeenCalledWith('tok', { page: 1, limit: 20 }))
      fireEvent.change(screen.getByLabelText(/search users/i), { target: { value: 'ali' } })
      await act(async () => {
        await vi.advanceTimersByTimeAsync(400)
      })
      await waitFor(() =>
        expect(listUsers).toHaveBeenCalledWith('tok', { q: 'ali', page: 1, limit: 20 }),
      )
    } finally {
      vi.useRealTimers()
    }
  })

  it('renders a not-authorized state on a 403 not_admin error', async () => {
    listUsers.mockRejectedValue(new AsyncJobError('forbidden', { code: 'forbidden', reason: 'not_admin' }))
    render(<UsersPage />)
    expect(await screen.findByText(/not authorized/i)).toBeInTheDocument()
    expect(logout).not.toHaveBeenCalled()
  })

  it('shows a formatted error banner for other failures', async () => {
    listUsers.mockRejectedValue(new AsyncJobError('boom', { code: 'internal' }))
    render(<UsersPage />)
    expect(await screen.findByText(/boom/i)).toBeInTheDocument()
    expect(logout).not.toHaveBeenCalled()
  })

  it('logs the admin out instead of showing a banner on invalid_token', async () => {
    listUsers.mockRejectedValue(
      new AsyncJobError('expired', { code: 'unauthenticated', reason: 'invalid_token' }),
    )
    render(<UsersPage />)
    await waitFor(() => expect(logout).toHaveBeenCalledTimes(1))
    expect(screen.queryByText(/expired/i)).not.toBeInTheDocument()
    expect(screen.queryByText(/not authorized/i)).not.toBeInTheDocument()
  })

  it('opens CreateUserForm when "New user" is clicked', async () => {
    render(<UsersPage />)
    await waitFor(() => expect(listUsers).toHaveBeenCalled())
    fireEvent.click(screen.getByRole('button', { name: /new user/i }))
    expect(await screen.findByRole('dialog')).toBeInTheDocument()
  })

  it('closes the create dialog and re-fetches the list after a successful create', async () => {
    render(<UsersPage />)
    await waitFor(() => expect(listUsers).toHaveBeenCalledTimes(1))

    fireEvent.click(screen.getByRole('button', { name: /new user/i }))
    expect(await screen.findByRole('dialog')).toBeInTheDocument()

    fireEvent.click(screen.getByRole('button', { name: /fake create/i }))

    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument())
    await waitFor(() => expect(listUsers).toHaveBeenCalledTimes(2))
  })

  it('closes the set-password dialog and re-fetches the list after a successful update', async () => {
    render(<UsersPage />)
    await waitFor(() => expect(listUsers).toHaveBeenCalledTimes(1))
    await screen.findByText('alice')

    fireEvent.click(screen.getByRole('button', { name: /set password/i }))
    expect(await screen.findByRole('dialog')).toBeInTheDocument()

    fireEvent.click(screen.getByRole('button', { name: /fake set password/i }))

    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument())
    await waitFor(() => expect(listUsers).toHaveBeenCalledTimes(2))
  })

  describe('pagination', () => {
    it('disables Prev on page 1 and requests page 2 on Next', async () => {
      listUsers.mockResolvedValue({ users: [USER], total: 50 })
      render(<UsersPage />)
      await waitFor(() => expect(listUsers).toHaveBeenCalledWith('tok', { page: 1, limit: 20 }))

      expect(screen.getByRole('button', { name: /prev/i })).toBeDisabled()

      fireEvent.click(screen.getByRole('button', { name: /next/i }))
      await waitFor(() => expect(listUsers).toHaveBeenCalledWith('tok', { page: 2, limit: 20 }))
    })

    it('goes back to page 1 with Prev after advancing to page 2', async () => {
      listUsers.mockResolvedValue({ users: [USER], total: 50 })
      render(<UsersPage />)
      await waitFor(() => expect(listUsers).toHaveBeenCalledWith('tok', { page: 1, limit: 20 }))

      fireEvent.click(screen.getByRole('button', { name: /next/i }))
      await waitFor(() => expect(listUsers).toHaveBeenCalledWith('tok', { page: 2, limit: 20 }))

      fireEvent.click(screen.getByRole('button', { name: /prev/i }))
      await waitFor(() => expect(listUsers).toHaveBeenCalledWith('tok', { page: 1, limit: 20 }))
    })

    it('disables Next once the current page already covers total', async () => {
      // page 1 * limit 20 = 20 >= total 20 — nothing more to page into.
      listUsers.mockResolvedValue({ users: [USER], total: 20 })
      render(<UsersPage />)
      await waitFor(() => expect(listUsers).toHaveBeenCalledWith('tok', { page: 1, limit: 20 }))
      expect(await screen.findByRole('button', { name: /next/i })).toBeDisabled()
    })

    it('resets to page 1 when the search box changes', async () => {
      listUsers.mockResolvedValue({ users: [USER], total: 50 })
      vi.useFakeTimers({ shouldAdvanceTime: true })
      try {
        render(<UsersPage />)
        await waitFor(() => expect(listUsers).toHaveBeenCalledWith('tok', { page: 1, limit: 20 }))

        fireEvent.click(screen.getByRole('button', { name: /next/i }))
        await waitFor(() => expect(listUsers).toHaveBeenCalledWith('tok', { page: 2, limit: 20 }))

        fireEvent.change(screen.getByLabelText(/search users/i), { target: { value: 'ali' } })
        await act(async () => {
          await vi.advanceTimersByTimeAsync(400)
        })
        await waitFor(() =>
          expect(listUsers).toHaveBeenCalledWith('tok', { q: 'ali', page: 1, limit: 20 }),
        )
      } finally {
        vi.useRealTimers()
      }
    })
  })

  it('drops a stale out-of-order response so a slow earlier request cannot overwrite newer rows', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      render(<UsersPage />)
      await waitFor(() => expect(listUsers).toHaveBeenCalledWith('tok', { page: 1, limit: 20 }))

      let resolveSlow
      const slow = new Promise((resolve) => {
        resolveSlow = resolve
      })
      // First debounced search: a slow request that resolves late.
      listUsers.mockReturnValueOnce(slow)
      fireEvent.change(screen.getByLabelText(/search users/i), { target: { value: 'first' } })
      await act(async () => {
        await vi.advanceTimersByTimeAsync(400)
      })
      await waitFor(() =>
        expect(listUsers).toHaveBeenCalledWith('tok', { q: 'first', page: 1, limit: 20 }),
      )

      // Second debounced search: a fast request that resolves first.
      listUsers.mockResolvedValueOnce({ users: [{ ...USER, id: 'u-2', account: 'bob' }], total: 1 })
      fireEvent.change(screen.getByLabelText(/search users/i), { target: { value: 'second' } })
      await act(async () => {
        await vi.advanceTimersByTimeAsync(400)
      })
      await waitFor(() =>
        expect(listUsers).toHaveBeenCalledWith('tok', { q: 'second', page: 1, limit: 20 }),
      )

      // The fast (newer) response landed.
      await waitFor(() => expect(screen.getByText('bob')).toBeInTheDocument())
      expect(screen.queryByText('alice')).not.toBeInTheDocument()

      // Now the slow (stale) response resolves — it must be ignored, not
      // overwrite the newer rows already on screen.
      await act(async () => {
        resolveSlow({ users: [USER], total: 1 })
        await Promise.resolve()
      })

      expect(screen.queryByText('alice')).not.toBeInTheDocument()
      expect(screen.getByText('bob')).toBeInTheDocument()
    } finally {
      vi.useRealTimers()
    }
  })
})
