import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'

vi.mock('@/context/NatsContext', () => ({
  useNats: vi.fn(),
}))

vi.mock('@/lib/runtimeConfig', () => ({
  DEV_MODE: true,
}))

vi.mock('@/api/auth/oidcClient', () => ({
  getOidcManager: vi.fn(),
  isSSOTokenInvalidError: vi.fn(() => false),
  redirectToReloginOnTokenInvalid: vi.fn(() => Promise.resolve()),
}))

import LoginPage from './LoginPage'
import { useNats } from '@/context/NatsContext'
import * as runtimeConfig from '@/lib/runtimeConfig'
import { getOidcManager } from '@/api/auth/oidcClient'

beforeEach(() => {
  useNats.mockReset()
  getOidcManager.mockReset()
  window.sessionStorage.clear()
  // Reset DEV_MODE default before each test.
  runtimeConfig.DEV_MODE = true
})

afterEach(() => {
  window.sessionStorage.clear()
})

describe('LoginPage in DEV_MODE=true', () => {
  beforeEach(() => {
    runtimeConfig.DEV_MODE = true
  })

  it('renders the dev account form without a Site ID field', () => {
    useNats.mockReturnValue({ connect: vi.fn(), error: null })
    render(<LoginPage />)
    expect(screen.getByLabelText(/account/i)).toBeInTheDocument()
    expect(screen.queryByLabelText(/site id/i)).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /keycloak/i })).not.toBeInTheDocument()
  })

  it('does NOT auto-redirect to Keycloak in dev mode', () => {
    useNats.mockReturnValue({ connect: vi.fn(), error: null })
    const signinRedirect = vi.fn().mockResolvedValue(undefined)
    getOidcManager.mockReturnValue({ signinRedirect })

    render(<LoginPage />)

    expect(signinRedirect).not.toHaveBeenCalled()
  })

  it('submits with {mode: "dev", account}', async () => {
    const connect = vi.fn().mockResolvedValue(undefined)
    useNats.mockReturnValue({ connect, error: null })

    render(<LoginPage />)

    fireEvent.change(screen.getByLabelText(/account/i), { target: { value: 'alice' } })
    fireEvent.click(screen.getByRole('button', { name: /connect/i }))

    await waitFor(() => {
      expect(connect).toHaveBeenCalledWith({ mode: 'dev', account: 'alice' })
    })
  })

  it('renders connect error from connect()', async () => {
    const connect = vi.fn().mockRejectedValue(new Error('boom'))
    useNats.mockReturnValue({ connect, error: null })

    render(<LoginPage />)
    fireEvent.change(screen.getByLabelText(/account/i), { target: { value: 'alice' } })
    fireEvent.click(screen.getByRole('button', { name: /connect/i }))

    await waitFor(() => {
      expect(screen.getByText(/boom/i)).toBeInTheDocument()
    })
  })
})

describe('LoginPage in DEV_MODE=false', () => {
  beforeEach(() => {
    runtimeConfig.DEV_MODE = false
  })

  it('renders the Keycloak sign-in UI instead of the account form', () => {
    useNats.mockReturnValue({ connect: vi.fn(), error: null })
    getOidcManager.mockReturnValue({ signinRedirect: vi.fn().mockResolvedValue(undefined) })
    render(<LoginPage />)
    // The subtitle is the Keycloak heading; the button itself flips to
    // "Redirecting…" because the auto-redirect fires on mount.
    expect(screen.getByText('Sign in with Keycloak')).toBeInTheDocument()
    expect(screen.getByRole('button')).toHaveTextContent(/redirecting/i)
    expect(screen.queryByLabelText(/account/i)).not.toBeInTheDocument()
    expect(screen.queryByLabelText(/site id/i)).not.toBeInTheDocument()
  })

  it('auto-redirects to Keycloak on mount when unauthenticated (no click needed)', async () => {
    useNats.mockReturnValue({ connect: vi.fn(), error: null })
    const signinRedirect = vi.fn().mockResolvedValue(undefined)
    getOidcManager.mockReturnValue({ signinRedirect })

    render(<LoginPage />)

    // The visitor never clicks — landing on the login page in production with no
    // session must send them straight to Keycloak.
    await waitFor(() => {
      expect(signinRedirect).toHaveBeenCalledTimes(1)
    })
  })

  it('redirects to Keycloak without any Site ID input or stash', async () => {
    useNats.mockReturnValue({ connect: vi.fn(), error: null })
    const signinRedirect = vi.fn().mockResolvedValue(undefined)
    getOidcManager.mockReturnValue({ signinRedirect })

    render(<LoginPage />)
    expect(screen.queryByLabelText(/site id/i)).not.toBeInTheDocument()
    await waitFor(() => expect(signinRedirect).toHaveBeenCalled())
    expect(window.sessionStorage.getItem('oidc.siteId')).toBeNull()
  })

  it('shows an error if the Keycloak redirect throws', async () => {
    useNats.mockReturnValue({ connect: vi.fn(), error: null })
    const signinRedirect = vi.fn().mockRejectedValue(new Error('idp down'))
    getOidcManager.mockReturnValue({ signinRedirect })

    render(<LoginPage />)

    await waitFor(() => {
      expect(screen.getByText(/idp down/i)).toBeInTheDocument()
    })
  })

  it('lets the visitor retry via the button after a failed redirect', async () => {
    useNats.mockReturnValue({ connect: vi.fn(), error: null })
    const signinRedirect = vi.fn()
      .mockRejectedValueOnce(new Error('idp down'))
      .mockResolvedValueOnce(undefined)
    getOidcManager.mockReturnValue({ signinRedirect })

    render(<LoginPage />)
    // Auto-redirect fails first; the button re-enables for a manual retry.
    await waitFor(() => expect(screen.getByText(/idp down/i)).toBeInTheDocument())

    fireEvent.click(screen.getByRole('button', { name: /keycloak/i }))
    await waitFor(() => expect(signinRedirect).toHaveBeenCalledTimes(2))
  })
})
