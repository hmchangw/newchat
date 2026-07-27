import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import MessageContent from './MessageContent'

describe('MessageContent', () => {
  it('renders plain text', () => {
    render(<MessageContent content="hello world" mentions={[]} />)
    expect(screen.getByText('hello world')).toBeInTheDocument()
  })

  it('renders a resolved mention as a highlighted span with the display name', () => {
    render(<MessageContent content="hi @alice" mentions={[{ account: 'alice', engName: 'Alice Lee' }]} />)
    const el = screen.getByText('@Alice Lee')
    expect(el).toHaveClass('mention')
    expect(el).not.toHaveClass('mention-self')
    expect(el.dataset.account).toBe('alice')
  })

  it('marks a self-mention distinctly', () => {
    render(
      <MessageContent
        content="@alice ping"
        mentions={[{ account: 'alice', engName: 'Alice' }]}
        selfAccount="alice"
      />,
    )
    expect(screen.getByText('@Alice')).toHaveClass('mention-self')
  })

  it('styles @all as a group mention', () => {
    render(<MessageContent content="@all hey" mentions={[]} />)
    const el = screen.getByText('@all')
    expect(el).toHaveClass('mention')
    expect(el).toHaveClass('mention-all')
  })

  it('renders URLs as safe external links', () => {
    render(<MessageContent content="see https://example.com/x" mentions={[]} />)
    const a = screen.getByRole('link', { name: 'https://example.com/x' })
    expect(a).toHaveAttribute('href', 'https://example.com/x')
    expect(a).toHaveAttribute('target', '_blank')
    expect(a).toHaveAttribute('rel', 'noopener noreferrer')
  })

  it('renders nothing for empty content', () => {
    const { container } = render(<MessageContent content="" mentions={[]} />)
    expect(container.textContent).toBe('')
  })
})
