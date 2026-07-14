import { render, screen, fireEvent } from '@testing-library/react'
import { describe, it, expect, vi } from 'vitest'
import AppHeader from './AppHeader'

vi.mock('@/context/NatsContext', () => ({
  useNats: () => ({
    user: { account: 'alice', siteId: 'site-1' },
    disconnect: vi.fn(),
  }),
}))
vi.mock('./SearchBar/SearchBar', () => ({
  default: ({ onEnterSearch }) => (
    <button type="button" onClick={() => onEnterSearch?.('q')}>fake-search</button>
  ),
}))
vi.mock('./ThemeToggle/ThemeToggle', () => ({ default: () => <span>fake-theme</span> }))
vi.mock('./DebugLevelSelect/DebugLevelSelect', () => ({ default: () => <span>fake-debug</span> }))
vi.mock('./DebugPayloadToggle/DebugPayloadToggle', () => ({ default: () => <span>fake-payload</span> }))
vi.mock('./UnreadBadge', () => ({ default: () => <span>fake-unread</span> }))

describe('AppHeader', () => {
  it('renders user chip, theme toggle, logout', () => {
    render(<AppHeader onSelectRoom={() => {}} onEnterSearch={() => {}} />)
    expect(screen.getByText('alice · site-1')).toBeInTheDocument()
    expect(screen.getByText('fake-theme')).toBeInTheDocument()
    expect(screen.getByText('fake-unread')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /logout/i })).toBeInTheDocument()
  })

  it('renders the "Chat" brand title with the unread badge as a superscript', () => {
    const { container } = render(
      <AppHeader onSelectRoom={() => {}} onEnterSearch={() => {}} />,
    )
    const title = container.querySelector('.chat-header-title')
    expect(title).not.toBeNull()
    expect(title.textContent).toContain('Chat')
    const sup = title.querySelector('sup')
    expect(sup).not.toBeNull()
    expect(sup).toHaveTextContent('fake-unread')
  })

  it('clicking Logout invokes disconnect', async () => {
    const disconnect = vi.fn()
    vi.resetModules()
    vi.doMock('@/context/NatsContext', () => ({
      useNats: () => ({ user: { account: 'a', siteId: 's' }, disconnect }),
    }))
    // re-import after re-mock
    const { default: Re } = await import('./AppHeader')
    render(<Re onSelectRoom={() => {}} onEnterSearch={() => {}} />)
    fireEvent.click(screen.getByRole('button', { name: /logout/i }))
    expect(disconnect).toHaveBeenCalled()
  })

  it('forwards onSelectRoom and onEnterSearch to the search bar', () => {
    const onEnterSearch = vi.fn()
    render(<AppHeader onSelectRoom={() => {}} onEnterSearch={onEnterSearch} />)
    fireEvent.click(screen.getByText('fake-search'))
    expect(onEnterSearch).toHaveBeenCalledWith('q')
  })
})
