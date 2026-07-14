import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'

vi.mock('@/context/AuthContext', async (importOriginal) => {
  const actual = await importOriginal()
  return { ...actual, useAuth: vi.fn() }
})
vi.mock('@/pages/AdminLoginPage', () => ({
  default: vi.fn(() => <div>Admin Login Page</div>),
}))
vi.mock('@/api', async (importOriginal) => {
  const actual = await importOriginal()
  return { ...actual, listUsers: vi.fn() }
})

import App from './App'
import { useAuth } from '@/context/AuthContext'
import AdminLoginPage from '@/pages/AdminLoginPage'
import { listUsers } from '@/api'

beforeEach(() => {
  vi.clearAllMocks()
  listUsers.mockResolvedValue({ users: [], total: 0 })
  AdminLoginPage.mockImplementation(() => <div>Admin Login Page</div>)
})

describe('App', () => {
  it('renders AdminLoginPage when there is no session', () => {
    useAuth.mockReturnValue({ session: null, login: vi.fn(), logout: vi.fn() })
    render(<App />)
    expect(screen.getByText(/admin login page/i)).toBeInTheDocument()
  })

  it('renders AppShell with Users visible by default when a session is present', async () => {
    useAuth.mockReturnValue({
      session: { authToken: 'tok', account: 'p_admin', siteId: 'site-a' },
      login: vi.fn(),
      logout: vi.fn(),
    })
    render(<App />)
    expect(screen.getByText(/p_admin/i)).toBeInTheDocument()
    await waitFor(() => expect(listUsers).toHaveBeenCalledWith('tok', { page: 1, limit: 20 }))
  })

  describe('error boundary', () => {
    let consoleSpy
    beforeEach(() => {
      consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
    })
    afterEach(() => {
      consoleSpy.mockRestore()
    })

    it('shows the recovery UI instead of a white screen when the tree throws', () => {
      useAuth.mockReturnValue({ session: null, login: vi.fn(), logout: vi.fn() })
      AdminLoginPage.mockImplementation(() => {
        throw new Error('render boom')
      })
      render(<App />)
      expect(screen.getByRole('alert')).toBeInTheDocument()
      expect(screen.getByText(/something went wrong/i)).toBeInTheDocument()
      expect(screen.queryByText(/admin login page/i)).not.toBeInTheDocument()
    })
  })
})
