import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'

vi.mock('@/context/AuthContext', () => ({ useAuth: vi.fn() }))
vi.mock('@/api', async (importOriginal) => {
  const actual = await importOriginal()
  return {
    ...actual,
    listUsers: vi.fn(),
    listAudit: vi.fn(),
    listRooms: vi.fn(),
    createPermissions: vi.fn(),
    listPermissions: vi.fn(),
  }
})

import AppShell from './AppShell'
import { useAuth } from '@/context/AuthContext'
import { listUsers, listAudit, listPermissions, listRooms } from '@/api'

beforeEach(() => {
  vi.clearAllMocks()
  // Default both deploy gates on so the pre-existing Permissions/Updates tab
  // tests keep exercising those sections; the gating tests below override them.
  window.__APP_CONFIG__ = { PERMISSIONS_ENABLED: 'true', UPDATES_ENABLED: 'true' }
  useAuth.mockReturnValue({
    session: { authToken: 'tok', account: 'root', siteId: 'site-1' },
    logout: vi.fn(),
  })
  listUsers.mockResolvedValue({ users: [], total: 0 })
  listAudit.mockResolvedValue({ entries: [], total: 0 })
  listPermissions.mockResolvedValue({ entries: [], total: 0 })
  listRooms.mockResolvedValue({ rooms: [], total: 0 })
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

  it('switches from Users to Permissions via nav and mounts PermissionsView', async () => {
    render(<AppShell />)
    await waitFor(() => expect(listUsers).toHaveBeenCalled())

    fireEvent.click(screen.getByRole('button', { name: /^permissions$/i }))

    await waitFor(() =>
      expect(listPermissions).toHaveBeenCalledWith('tok', { page: 1, limit: 20 }),
    )
    expect(screen.getByRole('button', { name: /^create$/i })).toBeInTheDocument()
  })

  it('switches from Users to Updates via nav and mounts UpdatesConsole', async () => {
    render(<AppShell />)
    await waitFor(() => expect(listUsers).toHaveBeenCalled())

    fireEvent.click(screen.getByRole('button', { name: 'Updates' }))

    await waitFor(() =>
      expect(screen.getByRole('heading', { name: /client updates/i })).toBeInTheDocument(),
    )
  })
})

describe('AppShell permissions gating', () => {
  it('hides the Permissions tab when the runtime flag is absent', () => {
    window.__APP_CONFIG__ = {}
    render(<AppShell />)
    expect(screen.queryByRole('button', { name: 'Permissions' })).toBeNull()
    expect(screen.getByRole('button', { name: 'Users' })).toBeInTheDocument()
  })

  it('hides the Permissions tab when the runtime flag is not the string "true"', () => {
    window.__APP_CONFIG__ = { PERMISSIONS_ENABLED: 'false' }
    render(<AppShell />)
    expect(screen.queryByRole('button', { name: 'Permissions' })).toBeNull()
  })

  it('shows the Permissions tab when the runtime flag is "true"', () => {
    window.__APP_CONFIG__ = { PERMISSIONS_ENABLED: 'true' }
    render(<AppShell />)
    expect(screen.getByRole('button', { name: 'Permissions' })).toBeInTheDocument()
  })

  it('switches from Users to Rooms via nav and mounts RoomsPage', async () => {
    render(<AppShell />)
    await waitFor(() => expect(listUsers).toHaveBeenCalled())

    fireEvent.click(screen.getByRole('button', { name: /^rooms$/i }))

    await waitFor(() => expect(listRooms).toHaveBeenCalledWith('tok', { page: 1, limit: 20 }))
  })

  it('shows the Rooms tab regardless of the permissions runtime flag', () => {
    window.__APP_CONFIG__ = { PERMISSIONS_ENABLED: 'false' }
    render(<AppShell />)
    expect(screen.getByRole('button', { name: /^rooms$/i })).toBeInTheDocument()
  })
})

describe('AppShell updates gating', () => {
  it('hides the Updates tab when the runtime flag is absent', () => {
    window.__APP_CONFIG__ = {}
    render(<AppShell />)
    expect(screen.queryByRole('button', { name: 'Updates' })).toBeNull()
    expect(screen.getByRole('button', { name: 'Users' })).toBeInTheDocument()
  })

  it('hides the Updates tab when the runtime flag is not the string "true"', () => {
    window.__APP_CONFIG__ = { UPDATES_ENABLED: 'false' }
    render(<AppShell />)
    expect(screen.queryByRole('button', { name: 'Updates' })).toBeNull()
  })

  it('shows the Updates tab when the runtime flag is "true"', () => {
    window.__APP_CONFIG__ = { UPDATES_ENABLED: 'true' }
    render(<AppShell />)
    expect(screen.getByRole('button', { name: 'Updates' })).toBeInTheDocument()
  })

  it('gates Updates independently of Permissions', () => {
    window.__APP_CONFIG__ = { PERMISSIONS_ENABLED: 'true', UPDATES_ENABLED: 'false' }
    render(<AppShell />)
    expect(screen.getByRole('button', { name: 'Permissions' })).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Updates' })).toBeNull()
  })
})
