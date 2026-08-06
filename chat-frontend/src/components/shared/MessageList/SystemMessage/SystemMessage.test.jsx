import { render, screen } from '@testing-library/react'
import { describe, it, expect } from 'vitest'
import SystemMessage from './SystemMessage'

describe('SystemMessage', () => {
  it.each([
    ['members_added (2+)', '"Alice Anderson 王愛麗" added members to the channel'],
    ['members_added (1)', '"Alice Anderson 王愛麗" added "Frank Fischer 費法蘭" to the channel'],
    ['room_created', 'A new room has been created'],
  ])('renders Message.Content verbatim for %s', (_, content) => {
    render(<SystemMessage message={{ id: 'm', content, createdAt: '2026-05-18T10:00:00Z' }} />)
    expect(screen.getByText(content)).toBeInTheDocument()
  })

  it('renders a timestamp with both date and time alongside the text', () => {
    const { container } = render(
      <SystemMessage
        message={{ id: 'm', content: 'whatever', createdAt: '2026-05-18T10:42:00Z' }}
      />
    )
    // 2026-05-18T10:42:00Z stays within 2026 in any timezone, so the year
    // proves the date renders alongside the HH:MM time.
    const timeEl = container.querySelector('.system-message-time')
    expect(timeEl.textContent).toMatch(/2026/)
    expect(timeEl.textContent).toMatch(/\d{1,2}:\d{2}/)
  })
})
