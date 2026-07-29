import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import MessageReactions from './MessageReactions'

describe('MessageReactions', () => {
  it('renders nothing when there are no reactions', () => {
    const { container } = render(<MessageReactions reactions={undefined} />)
    expect(container.firstChild).toBeNull()
    const { container: c2 } = render(<MessageReactions reactions={{}} />)
    expect(c2.firstChild).toBeNull()
  })

  it('renders a chip per shortcode with the reactor count', () => {
    render(
      <MessageReactions
        reactions={{
          '👍': [{ account: 'a', displayName: 'A' }, { account: 'b', displayName: 'B' }],
          '🎉': [{ account: 'c', displayName: 'C' }],
        }}
      />,
    )
    expect(screen.getByText('👍')).toBeInTheDocument()
    expect(screen.getByText('🎉')).toBeInTheDocument()
    // counts
    expect(screen.getByText('2')).toBeInTheDocument()
    expect(screen.getByText('1')).toBeInTheDocument()
  })

  it('renders an ASCII/custom shortcode as :name:', () => {
    render(<MessageReactions reactions={{ acme_party: [{ account: 'a', displayName: 'A' }] }} />)
    expect(screen.getByText(':acme_party:')).toBeInTheDocument()
  })

  it('lists reactor display names in the chip title', () => {
    render(
      <MessageReactions
        reactions={{ '👍': [{ account: 'a', displayName: 'Alice' }, { account: 'b', displayName: 'Bob' }] }}
      />,
    )
    expect(screen.getByRole('listitem')).toHaveAttribute('title', 'Alice, Bob')
  })

  it('highlights a chip the current user reacted to', () => {
    render(
      <MessageReactions
        reactions={{
          '👍': [{ account: 'me', displayName: 'Me' }],
          '🎉': [{ account: 'other', displayName: 'Other' }],
        }}
        selfAccount="ME"
      />,
    )
    const chips = screen.getAllByRole('listitem')
    const mine = chips.find((c) => c.textContent.includes('👍'))
    const notMine = chips.find((c) => c.textContent.includes('🎉'))
    expect(mine).toHaveClass('reaction-chip-mine')
    expect(notMine).not.toHaveClass('reaction-chip-mine')
  })

  it('skips shortcodes whose reactor list is empty', () => {
    render(<MessageReactions reactions={{ '👍': [], '🎉': [{ account: 'a', displayName: 'A' }] }} />)
    expect(screen.queryByText('👍')).not.toBeInTheDocument()
    expect(screen.getByText('🎉')).toBeInTheDocument()
  })
})
