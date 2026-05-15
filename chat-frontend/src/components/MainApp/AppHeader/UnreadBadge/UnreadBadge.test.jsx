import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import UnreadBadge from './UnreadBadge'

const useUnreadTotal = vi.fn()
vi.mock('@/context/RoomEventsContext', () => ({
  useUnreadTotal: () => useUnreadTotal(),
}))

describe('UnreadBadge', () => {
  beforeEach(() => vi.clearAllMocks())

  it('renders nothing when total is zero', () => {
    useUnreadTotal.mockReturnValue({ total: 0, hasMention: false })
    const { container } = render(<UnreadBadge />)
    expect(container).toBeEmptyDOMElement()
  })

  it('renders the count when there are unread messages', () => {
    useUnreadTotal.mockReturnValue({ total: 5, hasMention: false })
    render(<UnreadBadge />)
    const badge = screen.getByLabelText('5 unread messages')
    expect(badge).toHaveTextContent('5')
  })

  it('caps display at 99+ past 99', () => {
    useUnreadTotal.mockReturnValue({ total: 150, hasMention: false })
    render(<UnreadBadge />)
    expect(screen.getByText('99+')).toBeInTheDocument()
  })

  it('singularizes the aria-label for a single unread', () => {
    useUnreadTotal.mockReturnValue({ total: 1, hasMention: false })
    render(<UnreadBadge />)
    expect(screen.getByLabelText('1 unread message')).toBeInTheDocument()
  })

  it('applies the mention modifier class when hasMention is true', () => {
    useUnreadTotal.mockReturnValue({ total: 2, hasMention: true })
    render(<UnreadBadge />)
    expect(screen.getByLabelText('2 unread messages')).toHaveClass('unread-badge--mention')
  })

  it('omits the mention modifier class when hasMention is false', () => {
    useUnreadTotal.mockReturnValue({ total: 2, hasMention: false })
    render(<UnreadBadge />)
    expect(screen.getByLabelText('2 unread messages')).not.toHaveClass('unread-badge--mention')
  })
})
