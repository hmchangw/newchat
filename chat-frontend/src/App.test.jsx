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
