import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen } from '@testing-library/react'

// Passthrough provider so App's routing renders without the real NatsProvider
// (which needs DebugProvider, wired in main.jsx, not App).
vi.mock('@/context/NatsContext', () => ({
  NatsProvider: ({ children }) => children,
  useNats: vi.fn(),
}))
vi.mock('@/pages/BotLoginPage', () => ({ default: () => <div>BOT LOGIN PAGE</div> }))
vi.mock('@/pages/LoginPage', () => ({ default: () => <div>SSO LOGIN PAGE</div> }))
vi.mock('@/pages/OidcCallback', () => ({ default: () => <div>OIDC CALLBACK</div> }))
vi.mock('@/components/MainApp/MainApp', () => ({ default: () => <div>MAIN APP</div> }))

import App from './App'
import { useNats } from '@/context/NatsContext'

function setPath(p) {
  window.history.pushState({}, '', p)
}

beforeEach(() => { vi.clearAllMocks(); useNats.mockReturnValue({ connected: false }) })
afterEach(() => { setPath('/') })

describe('App routing', () => {
  it('renders BotLoginPage at /dev-login when not connected', () => {
    setPath('/dev-login')
    render(<App />)
    expect(screen.getByText('BOT LOGIN PAGE')).toBeInTheDocument()
  })

  it('renders the SSO LoginPage at / when not connected', () => {
    setPath('/')
    render(<App />)
    expect(screen.getByText('SSO LOGIN PAGE')).toBeInTheDocument()
  })
})

// BOT_LOGIN_ENABLED is a module-level const snapshotted at import time, so
// these cases re-import App fresh under vi.doMock per case (per the module's
// own runtimeConfig.test.js pattern of vi.resetModules + dynamic import).
describe('App routing — BOT_LOGIN_ENABLED gate', () => {
  function mockAppDeps() {
    vi.doMock('@/context/NatsContext', () => ({
      NatsProvider: ({ children }) => children,
      useNats: () => ({ connected: false }),
    }))
    vi.doMock('@/pages/BotLoginPage', () => ({ default: () => <div>BOT LOGIN PAGE</div> }))
    vi.doMock('@/pages/LoginPage', () => ({ default: () => <div>SSO LOGIN PAGE</div> }))
    vi.doMock('@/pages/OidcCallback', () => ({ default: () => <div>OIDC CALLBACK</div> }))
    vi.doMock('@/components/MainApp/MainApp', () => ({ default: () => <div>MAIN APP</div> }))
  }

  afterEach(() => {
    setPath('/')
    vi.doUnmock('@/lib/runtimeConfig')
  })

  it('renders BotLoginPage at /dev-login when BOT_LOGIN_ENABLED=true (regression guard)', async () => {
    vi.resetModules()
    mockAppDeps()
    vi.doMock('@/lib/runtimeConfig', async (importOriginal) => {
      const actual = await importOriginal()
      return { ...actual, BOT_LOGIN_ENABLED: true }
    })

    const { default: FreshApp } = await import('./App')
    setPath('/dev-login')
    render(<FreshApp />)
    expect(screen.getByText('BOT LOGIN PAGE')).toBeInTheDocument()
  })

  it('renders LoginPage at /dev-login when BOT_LOGIN_ENABLED=false', async () => {
    vi.resetModules()
    mockAppDeps()
    vi.doMock('@/lib/runtimeConfig', async (importOriginal) => {
      const actual = await importOriginal()
      return { ...actual, BOT_LOGIN_ENABLED: false }
    })

    const { default: FreshApp } = await import('./App')
    setPath('/dev-login')
    render(<FreshApp />)
    expect(screen.getByText('SSO LOGIN PAGE')).toBeInTheDocument()
    expect(screen.queryByText('BOT LOGIN PAGE')).not.toBeInTheDocument()
  })
})
