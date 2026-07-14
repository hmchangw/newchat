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
  deactivated: false,
  requirePasswordChange: false,
}

let logout

beforeEach(() => {
  vi.clearAllMocks()
  logout = vi.fn()
  useAuth.mockReturnValue({ logout })
})

describe('EditUserDialog', () => {
  it('submits only the changed field (roles) via updateUser', async () => {
    updateUser.mockResolvedValue(undefined)
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
    updateUser.mockResolvedValue(undefined)
    render(<EditUserDialog authToken="tok" user={USER} onClose={vi.fn()} onUpdated={vi.fn()} />)
    fireEvent.click(screen.getByRole('checkbox', { name: /^deactivated$/i }))
    fireEvent.click(screen.getByRole('button', { name: /^save$/i }))
    expect(updateUser).not.toHaveBeenCalled()
    expect(screen.getByText(/confirm/i)).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: /^save$/i }))
    await waitFor(() => expect(updateUser).toHaveBeenCalledWith('tok', 'alice', { deactivated: true }))
  })

  it('does not require confirmation when reactivating an already-deactivated user', async () => {
    updateUser.mockResolvedValue(undefined)
    render(
      <EditUserDialog
        authToken="tok"
        user={{ ...USER, deactivated: true }}
        onClose={vi.fn()}
        onUpdated={vi.fn()}
      />,
    )
    fireEvent.click(screen.getByRole('checkbox', { name: /^deactivated$/i }))
    fireEvent.click(screen.getByRole('button', { name: /^save$/i }))
    await waitFor(() => expect(updateUser).toHaveBeenCalledWith('tok', 'alice', { deactivated: false }))
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
})
