import { useEffect } from 'react'
import { render, screen, fireEvent } from '@testing-library/react'
import { describe, it, expect, vi } from 'vitest'
import MessageActions from './MessageActions'
import { DegradedProvider, useDegraded } from '@/context/DegradedContext'

// MessageActionMenu (the kebab) is rendered as the last button inside
// MessageActions. It depends on NatsContext at runtime, which we don't
// want to pull into this isolated component test. Mock it down to a
// no-op stub.
vi.mock('./MessageActionMenu/MessageActionMenu', () => ({ default: () => null }))

const msg = { id: 'm1', userAccount: 'alice' }

// Primes DegradedContext's flag before rendering `ui`, via a throwaway
// component that calls noteHistoryFailure in an effect (RTL's render
// flushes effects synchronously via act()).
function renderWithDegraded(ui, { degraded }) {
  function Primer() {
    const { noteHistoryFailure } = useDegraded()
    useEffect(() => {
      if (degraded) noteHistoryFailure({ code: 'unavailable' })
    }, [])
    return ui
  }
  return render(
    <DegradedProvider>
      <Primer />
    </DegradedProvider>
  )
}

describe('MessageActions', () => {
  it('renders Thread and Reply buttons in the main feed context', () => {
    render(<MessageActions message={msg} context="main" onThread={() => {}} onReply={() => {}} />, { wrapper: DegradedProvider })
    expect(screen.getByRole('button', { name: /reply in thread/i })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /quote/i })).toBeInTheDocument()
  })

  it('omits Thread on thread-parent (already in a thread) but keeps Quote', () => {
    render(<MessageActions message={msg} context="thread-parent" onThread={() => {}} onReply={() => {}} />, { wrapper: DegradedProvider })
    expect(screen.queryByRole('button', { name: /reply in thread/i })).not.toBeInTheDocument()
    // Quote stays so users can quote the parent inside their thread reply.
    expect(screen.getByRole('button', { name: /quote/i })).toBeInTheDocument()
  })

  it('omits Thread inside the thread reply list — only Quote remains', () => {
    render(<MessageActions message={msg} context="thread" onThread={() => {}} onReply={() => {}} />, { wrapper: DegradedProvider })
    expect(screen.queryByRole('button', { name: /reply in thread/i })).not.toBeInTheDocument()
    expect(screen.getByRole('button', { name: /quote/i })).toBeInTheDocument()
  })

  it('clicking Thread invokes onThread with the message', () => {
    const onThread = vi.fn()
    render(<MessageActions message={msg} context="main" onThread={onThread} onReply={() => {}} />, { wrapper: DegradedProvider })
    fireEvent.click(screen.getByRole('button', { name: /reply in thread/i }))
    expect(onThread).toHaveBeenCalledWith(msg)
  })

  it('clicking Reply invokes onReply with the message', () => {
    const onReply = vi.fn()
    render(<MessageActions message={msg} context="main" onThread={() => {}} onReply={onReply} />, { wrapper: DegradedProvider })
    fireEvent.click(screen.getByRole('button', { name: /quote/i }))
    expect(onReply).toHaveBeenCalledWith(msg)
  })
})

describe('MessageActions — Edit / Delete visibility', () => {
  it('renders Edit and Delete on own messages', () => {
    render(
      <MessageActions
        message={msg}
        context="main"
        isOwn
        onThread={() => {}} onReply={() => {}}
        onEdit={() => {}} onDelete={() => {}}
      />,
      { wrapper: DegradedProvider }
    )
    expect(screen.getByRole('button', { name: /edit message/i })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /delete message/i })).toBeInTheDocument()
  })

  it('omits Edit and Delete on other users\' messages', () => {
    render(
      <MessageActions
        message={msg}
        context="main"
        isOwn={false}
        onThread={() => {}} onReply={() => {}}
        onEdit={() => {}} onDelete={() => {}}
      />,
      { wrapper: DegradedProvider }
    )
    expect(screen.queryByRole('button', { name: /edit message/i })).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /delete message/i })).not.toBeInTheDocument()
  })

  it('clicking Edit / Delete invokes the handlers with the message', () => {
    const onEdit = vi.fn()
    const onDelete = vi.fn()
    render(
      <MessageActions
        message={msg}
        context="main"
        isOwn
        onThread={() => {}} onReply={() => {}}
        onEdit={onEdit} onDelete={onDelete}
      />,
      { wrapper: DegradedProvider }
    )
    fireEvent.click(screen.getByRole('button', { name: /edit message/i }))
    expect(onEdit).toHaveBeenCalledWith(msg)
    fireEvent.click(screen.getByRole('button', { name: /delete message/i }))
    expect(onDelete).toHaveBeenCalledWith(msg)
  })
})

describe('MessageActions — thread start while history is degraded', () => {
  it('disables "reply in thread" on a message with no replies while degraded', () => {
    renderWithDegraded(<MessageActions message={{ id: 'm1', tcount: 0 }} onThread={() => {}} />, { degraded: true })
    expect(screen.getByRole('button', { name: /thread/i })).toBeDisabled()
  })

  it('keeps "reply in thread" enabled on a message that already has replies', () => {
    renderWithDegraded(<MessageActions message={{ id: 'm1', tcount: 3 }} onThread={() => {}} />, { degraded: true })
    expect(screen.getByRole('button', { name: /thread/i })).toBeEnabled()
  })

  it('keeps "reply in thread" enabled when healthy', () => {
    renderWithDegraded(<MessageActions message={{ id: 'm1', tcount: 0 }} onThread={() => {}} />, { degraded: false })
    expect(screen.getByRole('button', { name: /thread/i })).toBeEnabled()
  })
})
