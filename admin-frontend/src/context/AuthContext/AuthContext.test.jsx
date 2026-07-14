import { afterEach, describe, expect, it } from 'vitest'
import { render, screen, act } from '@testing-library/react'
import { AuthProvider, useAuth } from './AuthContext'

const STORAGE_KEY = 'admin.session'

const BUNDLE = {
  userId: 'u-1',
  authToken: 'tok-abc',
  account: 'acct-1',
  siteId: 'site-1',
  requirePasswordChange: false,
}

function Probe() {
  const { session, login, logout } = useAuth()
  return (
    <>
      <span data-testid="session">{session ? JSON.stringify(session) : 'null'}</span>
      <button data-testid="login" onClick={() => login(BUNDLE)}>
        login
      </button>
      <button data-testid="logout" onClick={() => logout()}>
        logout
      </button>
    </>
  )
}

afterEach(() => {
  sessionStorage.clear()
})

describe('AuthProvider', () => {
  it('starts logged out with no stored session', () => {
    render(
      <AuthProvider>
        <Probe />
      </AuthProvider>,
    )
    expect(screen.getByTestId('session').textContent).toBe('null')
  })

  it('login persists the full bundle to sessionStorage and exposes only {authToken, account, siteId}', () => {
    render(
      <AuthProvider>
        <Probe />
      </AuthProvider>,
    )

    act(() => {
      screen.getByTestId('login').click()
    })

    const stored = JSON.parse(sessionStorage.getItem(STORAGE_KEY))
    expect(stored).toEqual(BUNDLE)

    const session = JSON.parse(screen.getByTestId('session').textContent)
    expect(session).toEqual({
      authToken: 'tok-abc',
      account: 'acct-1',
      siteId: 'site-1',
    })
    expect(session.userId).toBeUndefined()
    expect(session.requirePasswordChange).toBeUndefined()
  })

  it('restores session from an existing sessionStorage value on mount', () => {
    sessionStorage.setItem(STORAGE_KEY, JSON.stringify(BUNDLE))

    render(
      <AuthProvider>
        <Probe />
      </AuthProvider>,
    )

    const session = JSON.parse(screen.getByTestId('session').textContent)
    expect(session).toEqual({
      authToken: 'tok-abc',
      account: 'acct-1',
      siteId: 'site-1',
    })
  })

  it('logout clears storage and resets session to null', () => {
    sessionStorage.setItem(STORAGE_KEY, JSON.stringify(BUNDLE))

    render(
      <AuthProvider>
        <Probe />
      </AuthProvider>,
    )

    act(() => {
      screen.getByTestId('logout').click()
    })

    expect(screen.getByTestId('session').textContent).toBe('null')
    expect(sessionStorage.getItem(STORAGE_KEY)).toBeNull()
  })

  it('clears storage and stays logged out when the stored value is malformed', () => {
    sessionStorage.setItem(STORAGE_KEY, 'not-json{')

    render(
      <AuthProvider>
        <Probe />
      </AuthProvider>,
    )

    expect(screen.getByTestId('session').textContent).toBe('null')
    expect(sessionStorage.getItem(STORAGE_KEY)).toBeNull()
  })

  it('throws when useAuth is used outside a provider', () => {
    function Bare() {
      useAuth()
      return null
    }
    expect(() => render(<Bare />)).toThrow()
  })
})
