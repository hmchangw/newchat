import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'

vi.mock('@/context/NatsContext', () => ({
  useNats: vi.fn(),
}))

vi.mock('@/api/auth/oidcClient', () => ({
  getOidcManager: vi.fn(),
  isSSOTokenInvalidError: vi.fn(() => false),
  redirectToReloginOnTokenInvalid: vi.fn(() => Promise.resolve()),
}))

import OidcCallback from './OidcCallback'
import { useNats } from '@/context/NatsContext'
import { getOidcManager } from '@/api/auth/oidcClient'

beforeEach(() => {
  useNats.mockReset()
  getOidcManager.mockReset()
  window.sessionStorage.clear()
  // Default jsdom location is http://localhost/. Reset it.
  window.history.replaceState({}, '', '/oidc-callback?code=abc&state=xyz')
})

afterEach(() => {
  window.sessionStorage.clear()
  window.history.replaceState({}, '', '/')
})

describe('OidcCallback', () => {
  it('renders a loading message while processing the callback', () => {
    const connect = vi.fn(() => new Promise(() => {})) // never resolves
    useNats.mockReturnValue({ connect })
    getOidcManager.mockReturnValue({
      signinRedirectCallback: vi.fn(() => new Promise(() => {})),
    })

    render(<OidcCallback onDone={vi.fn()} />)
    expect(screen.getByText(/completing sign-in/i)).toBeInTheDocument()
  })

  it('completes signin, calls connect with sso opts, replaces history, and calls onDone', async () => {
    const connect = vi.fn().mockResolvedValue(undefined)
    const onDone = vi.fn()
    useNats.mockReturnValue({ connect })

    const fakeUser = { access_token: 'access-token-123', profile: { preferred_username: 'alice' } }
    getOidcManager.mockReturnValue({
      signinRedirectCallback: vi.fn().mockResolvedValue(fakeUser),
    })

    const replaceStateSpy = vi.spyOn(window.history, 'replaceState')

    render(<OidcCallback onDone={onDone} />)

    await waitFor(() => {
      expect(connect).toHaveBeenCalledWith({
        mode: 'sso',
        ssoToken: 'access-token-123',
        account: 'alice',
      })
    })

    await waitFor(() => {
      expect(onDone).toHaveBeenCalled()
    })

    expect(replaceStateSpy).toHaveBeenCalled()
    // Last replaceState call should set path to "/"
    const lastCall = replaceStateSpy.mock.calls[replaceStateSpy.mock.calls.length - 1]
    expect(lastCall[2]).toBe('/')
  })

  it('shows an error message when signinRedirectCallback fails', async () => {
    const connect = vi.fn()
    useNats.mockReturnValue({ connect })
    getOidcManager.mockReturnValue({
      signinRedirectCallback: vi.fn().mockRejectedValue(new Error('bad code')),
    })

    render(<OidcCallback onDone={vi.fn()} />)

    await waitFor(() => {
      expect(screen.getByText(/bad code/i)).toBeInTheDocument()
    })
    expect(connect).not.toHaveBeenCalled()
  })

  it('shows an error message when connect() fails after a successful OIDC callback', async () => {
    const connect = vi.fn().mockRejectedValue(new Error('nats blew up'))
    useNats.mockReturnValue({ connect })
    getOidcManager.mockReturnValue({
      signinRedirectCallback: vi.fn().mockResolvedValue({ access_token: 'tok' }),
    })

    render(<OidcCallback onDone={vi.fn()} />)

    await waitFor(() => {
      expect(screen.getByText(/nats blew up/i)).toBeInTheDocument()
    })
  })
})
