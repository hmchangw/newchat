import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'

vi.mock('@/context/AuthContext', () => ({ useAuth: vi.fn() }))
vi.mock('@/api', async (importOriginal) => {
  const actual = await importOriginal()
  return { ...actual, botLogin: vi.fn(), changePassword: vi.fn() }
})

import AdminLoginPage from './AdminLoginPage'
import { useAuth } from '@/context/AuthContext'
import { botLogin, changePassword } from '@/api'

const BUNDLE = {
  userId: 'u17', authToken: 'tok43', account: 'p_admin', siteId: 'site-a',
  requirePasswordChange: false,
}

beforeEach(() => {
  vi.clearAllMocks()
  useAuth.mockReturnValue({ session: null, login: vi.fn(), logout: vi.fn() })
})

function login(user = 'p_admin', pw = 'pw') {
  fireEvent.change(screen.getByLabelText(/username/i), { target: { value: user } })
  fireEvent.change(screen.getByLabelText(/password/i), { target: { value: pw } })
  fireEvent.click(screen.getByRole('button', { name: /sign in/i }))
}

describe('AdminLoginPage', () => {
  it('logs in and calls useAuth().login with the session bundle when no password change is required', async () => {
    botLogin.mockResolvedValue(BUNDLE)
    const authLogin = vi.fn()
    useAuth.mockReturnValue({ session: null, login: authLogin, logout: vi.fn() })
    render(<AdminLoginPage />)
    login()
    await waitFor(() => expect(botLogin).toHaveBeenCalledWith({ username: 'p_admin', password: 'pw' }))
    await waitFor(() => expect(authLogin).toHaveBeenCalledWith(BUNDLE))
  })

  it('shows the uniform error on invalid credentials and does not log in', async () => {
    const err = Object.assign(new Error('invalid username or password'), { kind: 'sync-error', reason: 'invalid_credentials' })
    botLogin.mockRejectedValue(err)
    const authLogin = vi.fn()
    useAuth.mockReturnValue({ session: null, login: authLogin, logout: vi.fn() })
    render(<AdminLoginPage />)
    login('x', 'y')
    await waitFor(() => expect(screen.getByText(/invalid username or password/i)).toBeInTheDocument())
    expect(authLogin).not.toHaveBeenCalled()
  })

  it('routes to the change-password step when requirePasswordChange is true', async () => {
    botLogin.mockResolvedValue({ ...BUNDLE, requirePasswordChange: true })
    render(<AdminLoginPage />)
    login()
    await waitFor(() => expect(screen.getByRole('button', { name: /change password/i })).toBeInTheDocument())
  })

  it('changes the password then logs in, carrying the same authToken', async () => {
    botLogin.mockResolvedValue({ ...BUNDLE, requirePasswordChange: true })
    changePassword.mockResolvedValue(undefined)
    const authLogin = vi.fn()
    useAuth.mockReturnValue({ session: null, login: authLogin, logout: vi.fn() })
    render(<AdminLoginPage />)
    login()
    await waitFor(() => screen.getByLabelText(/current password/i))

    fireEvent.change(screen.getByLabelText(/current password/i), { target: { value: 'pw' } })
    fireEvent.change(screen.getByLabelText(/^new password/i), { target: { value: 'new9' } })
    fireEvent.change(screen.getByLabelText(/confirm/i), { target: { value: 'new9' } })
    fireEvent.click(screen.getByRole('button', { name: /change password/i }))

    await waitFor(() => expect(changePassword).toHaveBeenCalledWith({
      authToken: 'tok43', oldPassword: 'pw', newPassword: 'new9',
    }))
    await waitFor(() => expect(authLogin).toHaveBeenCalledWith({ ...BUNDLE, requirePasswordChange: true }))
  })

  it('renders admin branding', () => {
    render(<AdminLoginPage />)
    expect(screen.getByText(/admin sign in/i)).toBeInTheDocument()
  })
})
