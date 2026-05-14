import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import MessageInput from './MessageInput'

vi.mock('../context/NatsContext', () => ({
  useNats: vi.fn(),
}))

import { useNats } from '../context/NatsContext'

function setupNats(overrides = {}) {
  const publish = vi.fn()
  useNats.mockReturnValue({
    user: { account: 'alice', siteId: 'site-A' },
    publish,
    ...overrides,
  })
  return { publish }
}

describe('MessageInput', () => {
  beforeEach(() => useNats.mockReset())

  it('falls back to "Select a room..." when no room is selected', () => {
    setupNats()
    render(<MessageInput room={null} />)
    expect(screen.getByPlaceholderText('Select a room...')).toBeInTheDocument()
  })

  it('uses "# {name}" placeholder for channel rooms', () => {
    setupNats()
    render(<MessageInput room={{ id: 'r1', name: 'frontend', type: 'channel' }} />)
    expect(screen.getByPlaceholderText('Message # frontend')).toBeInTheDocument()
  })

  it('uses "@ {name}" placeholder for DM rooms via roomDisplayName', () => {
    // DMs use the @ prefix and route through roomDisplayName, which composes
    // the counterpart's HRInfo (engName + name, collapsed when equal).
    setupNats()
    render(
      <MessageInput
        room={{ id: 'r-dm', type: 'dm', hrInfo: { engName: 'bob', name: 'bob' } }}
      />
    )
    expect(screen.getByPlaceholderText('Message @ bob')).toBeInTheDocument()
  })

  it('falls back to "(DM)" when a DM room has no name', () => {
    setupNats()
    render(<MessageInput room={{ id: 'r-dm', name: '', type: 'dm' }} />)
    expect(screen.getByPlaceholderText('Message @ (DM)')).toBeInTheDocument()
  })

  it('publishes msgSend on submit', () => {
    const { publish } = setupNats()
    render(<MessageInput room={{ id: 'r1', siteId: 'site-A', name: 'frontend', type: 'channel' }} />)
    fireEvent.change(screen.getByPlaceholderText(/Message/i), { target: { value: 'hi' } })
    fireEvent.click(screen.getByRole('button', { name: /Send/i }))
    expect(publish).toHaveBeenCalledTimes(1)
    expect(publish.mock.calls[0][0]).toBe('chat.user.alice.room.r1.site-A.msg.send')
    expect(publish.mock.calls[0][1]).toMatchObject({ content: 'hi' })
  })
})
