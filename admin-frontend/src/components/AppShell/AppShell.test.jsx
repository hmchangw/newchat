import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'

vi.mock('@/context/AuthContext', () => ({ useAuth: vi.fn() }))
vi.mock('@/api', async (importOriginal) => {
  const actual = await importOriginal()
  return { ...actual, listUsers: vi.fn(), listAudit: vi.fn() }
})

import AppShell from './AppShell'
import { useAuth } from '@/context/AuthContext'
import { listUsers, listAudit } from '@/api'

beforeEach(() => {
  vi.clearAllMocks()
  useAuth.mockReturnValue({
    session: { authToken: 'tok', account: 'root', siteId: 'site-1' },
    logout: vi.fn(),
  })
  listUsers.mockResolvedValue({ users: [], total: 0 })
  listAudit.mockResolvedValue({ entries: [], total: 0 })
})

describe('AppShell', () => {
  it('shows the signed-in account and mounts Users by default', async () => {
    render(<AppShell />)
    expect(screen.getByText(/root/i)).toBeInTheDocument()
    await waitFor(() => expect(listUsers).toHaveBeenCalledWith('tok', { page: 1, limit: 20 }))
  })

  it('calls logout when the Logout button is clicked', async () => {
    const logout = vi.fn()
    useAuth.mockReturnValue({
      session: { authToken: 'tok', account: 'root', siteId: 'site-1' },
      logout,
    })
    render(<AppShell />)
    await waitFor(() => expect(listUsers).toHaveBeenCalled())

    fireEvent.click(screen.getByRole('button', { name: /log out/i }))
    expect(logout).toHaveBeenCalledTimes(1)
  })

  it('switches from Users to Audit via nav and mounts AuditView', async () => {
    render(<AppShell />)
    await waitFor(() => expect(listUsers).toHaveBeenCalled())

    fireEvent.click(screen.getByRole('button', { name: /^audit$/i }))

    await waitFor(() => expect(listAudit).toHaveBeenCalledWith('tok', { page: 1, limit: 20 }))
  })

  it('switches back from Audit to Users via nav', async () => {
    render(<AppShell />)
    await waitFor(() => expect(listUsers).toHaveBeenCalled())

    fireEvent.click(screen.getByRole('button', { name: /^audit$/i }))
    await waitFor(() => expect(listAudit).toHaveBeenCalled())

    fireEvent.click(screen.getByRole('button', { name: /^users$/i }))
    await waitFor(() => expect(listUsers).toHaveBeenCalledTimes(2))
  })
})
