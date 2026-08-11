import { describe, it, expect, vi, beforeEach } from 'vitest'
import { act, render, screen, fireEvent, waitFor } from '@testing-library/react'

vi.mock('@/context/AuthContext', () => ({ useAuth: vi.fn() }))
vi.mock('@/api', async (importOriginal) => {
  const actual = await importOriginal()
  return { ...actual, listPermissions: vi.fn() }
})

vi.mock('../CreatePermissionsDialog', () => ({
  default: ({ onClose, onCreated }) => (
    <div role="dialog" aria-label="create permissions">
      <button type="button" onClick={onCreated}>
        Fake create
      </button>
      <button type="button" onClick={onClose}>
        Fake close
      </button>
    </div>
  ),
}))

import PermissionsPage from './PermissionsPage'
import { useAuth } from '@/context/AuthContext'
import { listPermissions, AsyncJobError } from '@/api'

const GRANT_ROW = {
  id: 'g-1',
  permission: 'external.image.view',
  subjectAccount: 'alice',
  granted: true,
  effectiveFrom: '2026-09-01',
  expiresAt: '2026-12-31',
  expiresAtUTC: '2026-12-31T16:00:00Z',
  applicantAccount: 'carol',
  approverAccount: 'dave',
  reason: 'Business trip.',
  recordedBy: 'p_admin_wang',
  recordedAt: '2026-08-11T03:00:00Z',
}

const REVOKE_ROW = {
  id: 'g-2',
  permission: 'external.image.view',
  subjectAccount: 'alice',
  granted: false,
  applicantAccount: 'carol',
  approverAccount: 'dave',
  reason: 'Project ended.',
  recordedBy: 'p_admin_wang',
  recordedAt: '2026-08-12T03:00:00Z',
}

let logout

beforeEach(() => {
  vi.clearAllMocks()
  logout = vi.fn()
  useAuth.mockReturnValue({
    session: { authToken: 'tok', account: 'root', siteId: 'site-1' },
    logout,
  })
  listPermissions.mockResolvedValue({ entries: [], total: 0 })
})

describe('PermissionsPage — list', () => {
  it('calls listPermissions(token, {page:1, limit:20}) on mount with no filters and renders rows', async () => {
    listPermissions.mockResolvedValue({ entries: [GRANT_ROW, REVOKE_ROW], total: 2 })
    render(<PermissionsPage />)
    await waitFor(() => expect(listPermissions).toHaveBeenCalledWith('tok', { page: 1, limit: 20 }))

    const rows = await screen.findAllByRole('row')
    expect(rows).toHaveLength(3)
    expect(rows[1]).toHaveTextContent('alice')
    expect(rows[1]).toHaveTextContent('Grant')
    expect(rows[1]).toHaveTextContent('2026-09-01')
    expect(rows[1]).toHaveTextContent('2026-12-31')
    expect(rows[2]).toHaveTextContent('Revoke')
    expect(rows[2]).toHaveTextContent('—')
  })

  it('shows a status message when the list is empty', async () => {
    render(<PermissionsPage />)
    expect(await screen.findByText(/no permission records found/i)).toBeInTheDocument()
  })

  it('re-queries with {subjectAccount} after the filter input debounces', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      render(<PermissionsPage />)
      await waitFor(() => expect(listPermissions).toHaveBeenCalledWith('tok', { page: 1, limit: 20 }))

      fireEvent.change(screen.getByLabelText(/filter by subject account/i), {
        target: { value: 'alice' },
      })
      await act(async () => {
        await vi.advanceTimersByTimeAsync(400)
      })

      await waitFor(() =>
        expect(listPermissions).toHaveBeenCalledWith('tok', {
          subjectAccount: 'alice',
          page: 1,
          limit: 20,
        }),
      )
    } finally {
      vi.useRealTimers()
    }
  })

  it('re-queries with {permission} after the permission select debounces', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      render(<PermissionsPage />)
      await waitFor(() => expect(listPermissions).toHaveBeenCalledWith('tok', { page: 1, limit: 20 }))

      fireEvent.change(screen.getByLabelText(/filter by permission/i), {
        target: { value: 'external.image.view' },
      })
      await act(async () => {
        await vi.advanceTimersByTimeAsync(400)
      })

      await waitFor(() =>
        expect(listPermissions).toHaveBeenCalledWith('tok', {
          permission: 'external.image.view',
          page: 1,
          limit: 20,
        }),
      )
    } finally {
      vi.useRealTimers()
    }
  })

  it('combines both filters into one fetch call once both have been set', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      render(<PermissionsPage />)
      await waitFor(() => expect(listPermissions).toHaveBeenCalledWith('tok', { page: 1, limit: 20 }))

      fireEvent.change(screen.getByLabelText(/filter by subject account/i), {
        target: { value: 'alice' },
      })
      fireEvent.change(screen.getByLabelText(/filter by permission/i), {
        target: { value: 'external.image.view' },
      })
      await act(async () => {
        await vi.advanceTimersByTimeAsync(400)
      })

      await waitFor(() =>
        expect(listPermissions).toHaveBeenCalledWith('tok', {
          subjectAccount: 'alice',
          permission: 'external.image.view',
          page: 1,
          limit: 20,
        }),
      )
    } finally {
      vi.useRealTimers()
    }
  })

  it('shows the "Currently granted" badge only when currentlyGranted is true', async () => {
    listPermissions.mockResolvedValue({ entries: [GRANT_ROW], total: 1, currentlyGranted: true })
    render(<PermissionsPage />)
    expect(await screen.findByText(/currently granted/i)).toBeInTheDocument()
  })

  it('shows the "Not granted" badge when currentlyGranted is false', async () => {
    listPermissions.mockResolvedValue({ entries: [REVOKE_ROW], total: 1, currentlyGranted: false })
    render(<PermissionsPage />)
    expect(await screen.findByText(/^not granted$/i)).toBeInTheDocument()
  })

  it('shows no badge when currentlyGranted is absent from the response', async () => {
    render(<PermissionsPage />)
    await waitFor(() => expect(listPermissions).toHaveBeenCalled())
    expect(screen.queryByText(/currently granted/i)).not.toBeInTheDocument()
    expect(screen.queryByText(/^not granted$/i)).not.toBeInTheDocument()
  })

  it('renders a not-authorized state on a 403 not_admin error', async () => {
    listPermissions.mockRejectedValue(
      new AsyncJobError('forbidden', { code: 'forbidden', reason: 'not_admin' }),
    )
    render(<PermissionsPage />)
    expect(await screen.findByText(/not authorized/i)).toBeInTheDocument()
    expect(logout).not.toHaveBeenCalled()
  })

  it('shows a formatted error banner for other failures', async () => {
    listPermissions.mockRejectedValue(new AsyncJobError('boom', { code: 'internal' }))
    render(<PermissionsPage />)
    expect(await screen.findByText(/boom/i)).toBeInTheDocument()
    expect(logout).not.toHaveBeenCalled()
  })

  it('logs the admin out instead of showing a banner on invalid_token', async () => {
    listPermissions.mockRejectedValue(
      new AsyncJobError('expired', { code: 'unauthenticated', reason: 'invalid_token' }),
    )
    render(<PermissionsPage />)
    await waitFor(() => expect(logout).toHaveBeenCalledTimes(1))
    expect(screen.queryByText(/expired/i)).not.toBeInTheDocument()
  })

  describe('pagination', () => {
    it('disables Prev on page 1 and requests page 2 on Next', async () => {
      listPermissions.mockResolvedValue({ entries: [GRANT_ROW], total: 50 })
      render(<PermissionsPage />)
      await waitFor(() => expect(listPermissions).toHaveBeenCalledWith('tok', { page: 1, limit: 20 }))
      expect(screen.getByRole('button', { name: /prev/i })).toBeDisabled()

      fireEvent.click(screen.getByRole('button', { name: /next/i }))
      await waitFor(() => expect(listPermissions).toHaveBeenCalledWith('tok', { page: 2, limit: 20 }))
    })

    it('resets to page 1 when a filter changes', async () => {
      listPermissions.mockResolvedValue({ entries: [GRANT_ROW], total: 50 })
      vi.useFakeTimers({ shouldAdvanceTime: true })
      try {
        render(<PermissionsPage />)
        await waitFor(() => expect(listPermissions).toHaveBeenCalledWith('tok', { page: 1, limit: 20 }))

        fireEvent.click(screen.getByRole('button', { name: /next/i }))
        await waitFor(() => expect(listPermissions).toHaveBeenCalledWith('tok', { page: 2, limit: 20 }))

        fireEvent.change(screen.getByLabelText(/filter by subject account/i), {
          target: { value: 'alice' },
        })
        await act(async () => {
          await vi.advanceTimersByTimeAsync(400)
        })

        await waitFor(() =>
          expect(listPermissions).toHaveBeenCalledWith('tok', {
            subjectAccount: 'alice',
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
      render(<PermissionsPage />)
      await waitFor(() => expect(listPermissions).toHaveBeenCalledWith('tok', { page: 1, limit: 20 }))

      let resolveSlow
      const slow = new Promise((resolve) => {
        resolveSlow = resolve
      })
      listPermissions.mockReturnValueOnce(slow)
      fireEvent.change(screen.getByLabelText(/filter by subject account/i), {
        target: { value: 'first' },
      })
      await act(async () => {
        await vi.advanceTimersByTimeAsync(400)
      })
      await waitFor(() =>
        expect(listPermissions).toHaveBeenCalledWith('tok', {
          subjectAccount: 'first',
          page: 1,
          limit: 20,
        }),
      )

      listPermissions.mockResolvedValueOnce({
        entries: [{ ...GRANT_ROW, id: 'g-3', subjectAccount: 'second' }],
        total: 1,
      })
      fireEvent.change(screen.getByLabelText(/filter by subject account/i), {
        target: { value: 'second' },
      })
      await act(async () => {
        await vi.advanceTimersByTimeAsync(400)
      })
      await waitFor(() =>
        expect(listPermissions).toHaveBeenCalledWith('tok', {
          subjectAccount: 'second',
          page: 1,
          limit: 20,
        }),
      )

      await waitFor(() => expect(screen.getByText('second')).toBeInTheDocument())

      await act(async () => {
        resolveSlow({ entries: [{ ...GRANT_ROW, id: 'g-1', subjectAccount: 'alice' }], total: 1 })
        await Promise.resolve()
      })

      expect(screen.queryByText('alice')).not.toBeInTheDocument()
      expect(screen.getByText('second')).toBeInTheDocument()
    } finally {
      vi.useRealTimers()
    }
  })
})

describe('PermissionsPage — create dialog', () => {
  it('opens CreatePermissionsDialog when "Create" is clicked', async () => {
    render(<PermissionsPage />)
    await waitFor(() => expect(listPermissions).toHaveBeenCalled())
    fireEvent.click(screen.getByRole('button', { name: /^create$/i }))
    expect(await screen.findByRole('dialog')).toBeInTheDocument()
  })

  it('refetches the list when the dialog reports a successful create, keeping the dialog open', async () => {
    render(<PermissionsPage />)
    await waitFor(() => expect(listPermissions).toHaveBeenCalledTimes(1))

    fireEvent.click(screen.getByRole('button', { name: /^create$/i }))
    expect(await screen.findByRole('dialog')).toBeInTheDocument()

    fireEvent.click(screen.getByRole('button', { name: /fake create/i }))

    await waitFor(() => expect(listPermissions).toHaveBeenCalledTimes(2))
    expect(screen.getByRole('dialog')).toBeInTheDocument()
  })

  it('closes the dialog when it reports close', async () => {
    render(<PermissionsPage />)
    await waitFor(() => expect(listPermissions).toHaveBeenCalled())

    fireEvent.click(screen.getByRole('button', { name: /^create$/i }))
    expect(await screen.findByRole('dialog')).toBeInTheDocument()

    fireEvent.click(screen.getByRole('button', { name: /fake close/i }))

    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument())
  })
})
