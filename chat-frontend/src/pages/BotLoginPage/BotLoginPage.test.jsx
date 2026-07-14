import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'

vi.mock('@/context/NatsContext', () => ({ useNats: vi.fn() }))
vi.mock('@/api/auth/botAuth', () => ({ botLogin: vi.fn() }))

import BotLoginPage from './BotLoginPage'
import { useNats } from '@/context/NatsContext'
import { botLogin } from '@/api/auth/botAuth'

const BUNDLE = {
  userId: 'u17', authToken: 'tok43', account: 'p_admin', siteId: 'site-a',
  authServiceUrl: 'http://auth.site-a', baseUrl: 'http://site-a', natsUrl: 'ws://nats.site-a',
  requirePasswordChange: false,
}

beforeEach(() => {
  vi.clearAllMocks()
  useNats.mockReturnValue({ connect: vi.fn().mockResolvedValue(undefined) })
})

function login(user = 'p_admin', pw = 'pw') {
  fireEvent.change(screen.getByLabelText(/username/i), { target: { value: user } })
  fireEvent.change(screen.getByLabelText(/password/i), { target: { value: pw } })
  fireEvent.click(screen.getByRole('button', { name: /sign in/i }))
}

describe('BotLoginPage', () => {
  it('logs in and connects with the session bundle when no password change is required', async () => {
    botLogin.mockResolvedValue(BUNDLE)
    const connect = vi.fn().mockResolvedValue(undefined)
    useNats.mockReturnValue({ connect })
    render(<BotLoginPage />)
    login()
    await waitFor(() => expect(botLogin).toHaveBeenCalledWith({ username: 'p_admin', password: 'pw' }))
    await waitFor(() => expect(connect).toHaveBeenCalledWith({ mode: 'session', bundle: BUNDLE }))
  })

  it('shows the uniform error on invalid credentials and does not connect', async () => {
    const err = Object.assign(new Error('invalid username or password'), { kind: 'sync-error', reason: 'invalid_credentials' })
    botLogin.mockRejectedValue(err)
    const connect = vi.fn()
    useNats.mockReturnValue({ connect })
    render(<BotLoginPage />)
    login('x', 'y')
    await waitFor(() => expect(screen.getByText(/invalid username or password/i)).toBeInTheDocument())
    expect(connect).not.toHaveBeenCalled()
  })

  it('connects the session even when requirePasswordChange is true (rotation moved to the registration web)', async () => {
    const bundle = { ...BUNDLE, requirePasswordChange: true }
    botLogin.mockResolvedValue(bundle)
    const connect = vi.fn().mockResolvedValue(undefined)
    useNats.mockReturnValue({ connect })
    render(<BotLoginPage />)
    login()
    await waitFor(() => expect(connect).toHaveBeenCalledWith({ mode: 'session', bundle }))
    // No change-password step is rendered.
    expect(screen.queryByRole('button', { name: /change password/i })).not.toBeInTheDocument()
  })
})
