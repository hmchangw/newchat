import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'

vi.mock('@/api', async (importOriginal) => {
  const actual = await importOriginal()
  return { ...actual, updateUser: vi.fn() }
})
vi.mock('@/context/AuthContext', () => ({ useAuth: vi.fn() }))

import EditUserDialog from './EditUserDialog'
import { updateUser, AsyncJobError } from '@/api'
import { useAuth } from '@/context/AuthContext'

const USER = {
  id: 'u-1',
  account: 'alice',
  siteId: 'site-1',
  engName: 'Alice',
  chineseName: '',
  roles: ['user'],
  active: true,
  requirePasswordChange: false,
}

let logout

beforeEach(() => {
  vi.clearAllMocks()
  logout = vi.fn()
  useAuth.mockReturnValue({ logout })
})

const CLEAN = { syncFailures: [] }

describe('EditUserDialog', () => {
  it('submits only the changed field (roles) via updateUser', async () => {
    updateUser.mockResolvedValue(CLEAN)
    const onUpdated = vi.fn()
    render(<EditUserDialog authToken="tok" user={USER} onClose={vi.fn()} onUpdated={onUpdated} />)
    fireEvent.click(screen.getByRole('checkbox', { name: /^admin$/i }))
    fireEvent.click(screen.getByRole('button', { name: /^save$/i }))
    await waitFor(() =>
      expect(updateUser).toHaveBeenCalledWith('tok', 'alice', { roles: ['user', 'admin'] }),
    )
    await waitFor(() => expect(onUpdated).toHaveBeenCalled())
  })

  it('requires a second confirming click before deactivating', async () => {
    updateUser.mockResolvedValue(CLEAN)
    render(<EditUserDialog authToken="tok" user={USER} onClose={vi.fn()} onUpdated={vi.fn()} />)
    fireEvent.click(screen.getByRole('checkbox', { name: /^deactivated$/i }))
    fireEvent.click(screen.getByRole('button', { name: /^save$/i }))
    expect(updateUser).not.toHaveBeenCalled()
    expect(screen.getByText(/confirm/i)).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: /^save$/i }))
    await waitFor(() => expect(updateUser).toHaveBeenCalledWith('tok', 'alice', { active: false }))
  })

  it('does not require confirmation when reactivating an inactive user', async () => {
    updateUser.mockResolvedValue(CLEAN)
    render(
      <EditUserDialog
        authToken="tok"
        user={{ ...USER, active: false }}
        onClose={vi.fn()}
        onUpdated={vi.fn()}
      />,
    )
    fireEvent.click(screen.getByRole('checkbox', { name: /^deactivated$/i }))
    fireEvent.click(screen.getByRole('button', { name: /^save$/i }))
    await waitFor(() => expect(updateUser).toHaveBeenCalledWith('tok', 'alice', { active: true }))
  })

  it('logs the admin out instead of showing a banner on invalid_token', async () => {
    updateUser.mockRejectedValue(
      new AsyncJobError('expired', { code: 'unauthenticated', reason: 'invalid_token' }),
    )
    render(<EditUserDialog authToken="tok" user={USER} onClose={vi.fn()} onUpdated={vi.fn()} />)
    fireEvent.click(screen.getByRole('checkbox', { name: /^admin$/i }))
    fireEvent.click(screen.getByRole('button', { name: /^save$/i }))
    await waitFor(() => expect(logout).toHaveBeenCalledTimes(1))
    expect(screen.queryByText(/expired/i)).not.toBeInTheDocument()
  })

  it('closes immediately on a clean save', async () => {
    updateUser.mockResolvedValue(CLEAN)
    const onUpdated = vi.fn()
    render(<EditUserDialog authToken="tok" user={USER} onClose={vi.fn()} onUpdated={onUpdated} />)
    fireEvent.change(screen.getByLabelText(/english name/i), { target: { value: 'New' } })
    fireEvent.click(screen.getByRole('button', { name: /^save$/i }))
    await waitFor(() => expect(onUpdated).toHaveBeenCalled())
    expect(screen.queryByText(/saved on this site/i)).toBeNull()
  })

  it('shows the sync notice and defers onUpdated to Close', async () => {
    updateUser.mockResolvedValue({ syncFailures: ['site-b'] })
    const onUpdated = vi.fn()
    render(<EditUserDialog authToken="tok" user={USER} onClose={vi.fn()} onUpdated={onUpdated} />)
    fireEvent.change(screen.getByLabelText(/english name/i), { target: { value: 'New' } })
    fireEvent.click(screen.getByRole('button', { name: /^save$/i }))
    await waitFor(() => expect(screen.getByText(/saved on this site/i)).toBeInTheDocument())
    expect(onUpdated).not.toHaveBeenCalled()
    expect(screen.getByText(/site-b/)).toBeInTheDocument()
    expect(screen.getByText(/use resync on this user/i)).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: /^close$/i }))
    expect(onUpdated).toHaveBeenCalled()
  })

  it('shows the sync notice on a deactivate that failed to sync', async () => {
    updateUser.mockResolvedValue({ syncFailures: ['site-b'] })
    const onUpdated = vi.fn()
    render(<EditUserDialog authToken="tok" user={USER} onClose={vi.fn()} onUpdated={onUpdated} />)
    fireEvent.click(screen.getByRole('checkbox', { name: /^deactivated$/i }))
    fireEvent.click(screen.getByRole('button', { name: /^save$/i }))
    fireEvent.click(screen.getByRole('button', { name: /^save$/i }))
    await waitFor(() => expect(screen.getByText(/saved on this site/i)).toBeInTheDocument())
    expect(onUpdated).not.toHaveBeenCalled()
  })
})
