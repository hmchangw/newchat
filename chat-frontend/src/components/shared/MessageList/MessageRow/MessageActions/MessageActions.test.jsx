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

const BLOCKED_COPY =
  "Message history is unavailable — you can't start a new thread right now. Try again shortly."

describe('MessageActions — thread start while history is degraded', () => {
  // room type × tcount matrix. Only a channel message with no existing thread
  // is refusable by the gatekeeper (channel + no thread_rooms document); every
  // other combination resolves from Mongo and must stay enabled.
  const cases = [
    { name: 'channel, no replies', room: 'channel', tcount: 0, blocked: true },
    { name: 'channel, tcount undefined (field absent on the wire)', room: 'channel', tcount: undefined, blocked: true },
    { name: 'channel, thread already exists', room: 'channel', tcount: 3, blocked: false },
    { name: 'dm', room: 'dm', tcount: 0, blocked: false },
    { name: 'botDM', room: 'botDM', tcount: 0, blocked: false },
    { name: 'discussion', room: 'discussion', tcount: 0, blocked: false },
  ]

  cases.forEach(({ name, room, tcount, blocked }) => {
    it(`${blocked ? 'blocks' : 'keeps'} "reply in thread" — ${name}`, () => {
      renderWithDegraded(
        <MessageActions message={{ id: 'm1', tcount }} room={{ type: room }} context="main" onThread={() => {}} />,
        { degraded: true }
      )
      const btn = screen.getByRole('button', { name: /reply in thread/i })
      if (blocked) expect(btn).toHaveAttribute('aria-disabled', 'true')
      else expect(btn).not.toHaveAttribute('aria-disabled')
    })
  })

  it('keeps "reply in thread" enabled when healthy', () => {
    renderWithDegraded(
      <MessageActions message={{ id: 'm1', tcount: 0 }} room={{ type: 'channel' }} context="main" onThread={() => {}} />,
      { degraded: false }
    )
    expect(screen.getByRole('button', { name: /reply in thread/i })).not.toHaveAttribute('aria-disabled')
  })

  // The disable has to be functional, not cosmetic: aria-disabled does not stop
  // a click on its own, so the handler must refuse it explicitly.
  it('a click on the blocked button never reaches onThread', () => {
    const onThread = vi.fn()
    renderWithDegraded(
      <MessageActions message={{ id: 'm1', tcount: 0 }} room={{ type: 'channel' }} context="main" onThread={onThread} />,
      { degraded: true }
    )
    fireEvent.click(screen.getByRole('button', { name: /reply in thread/i }))
    expect(onThread).not.toHaveBeenCalled()
  })

  it('exposes the reason to assistive tech and keeps the button focusable', () => {
    renderWithDegraded(
      <MessageActions message={{ id: 'm1', tcount: 0 }} room={{ type: 'channel' }} context="main" onThread={() => {}} />,
      { degraded: true }
    )
    const btn = screen.getByRole('button', { name: /reply in thread/i })
    // A real `disabled` button is out of the tab order, so neither the
    // description nor the title tooltip is reachable by keyboard.
    expect(btn).not.toBeDisabled()
    btn.focus()
    expect(btn).toHaveFocus()
    expect(btn).toHaveAccessibleDescription(BLOCKED_COPY)
  })

  it('carries no description while healthy — nothing to explain', () => {
    renderWithDegraded(
      <MessageActions message={{ id: 'm1', tcount: 0 }} room={{ type: 'channel' }} context="main" onThread={() => {}} />,
      { degraded: false }
    )
    const btn = screen.getByRole('button', { name: /reply in thread/i })
    expect(btn).not.toHaveAccessibleDescription(BLOCKED_COPY)
    // The healthy hover tooltip predates the degraded work and must survive it.
    // (It is also what the accessible description falls back to, absent
    // aria-describedby.)
    expect(btn).toHaveAttribute('title', 'Reply in thread')
  })
})
