import { render, screen, fireEvent } from '@testing-library/react'
import { describe, it, expect, vi } from 'vitest'
import MessageRow from './MessageRow'

// Mock the existing read-receipt kebab so MessageRow tests don't depend on
// NatsContext (the kebab calls useNats internally). We still want to assert
// it's mounted and receives the right props.
vi.mock('./MessageActions/MessageActionMenu/MessageActionMenu', () => ({
  default: ({ message, room }) => (
    <div data-testid="kebab" data-message-id={message.id} data-room-id={room?.id ?? ''} />
  ),
}))

// Stub RoomEventsContext so MessageRow can read the active subscription's
// historySharedSince without requiring a provider in unit tests. Individual
// tests override via `mockSubscription` below.
let mockSubscription
vi.mock('@/context/RoomEventsContext', () => ({
  useSubscription: () => mockSubscription,
}))

// MessageRow reads useNats for the current user's account + media base URL.
vi.mock('@/context/NatsContext', () => ({
  useNats: () => ({ user: { account: 'me', baseUrl: 'https://media.test' } }),
}))

const msg = {
  id: 'm1',
  content: 'hello world',
  createdAt: '2026-05-13T10:42:00Z',
  sender: { engName: 'Alice', account: 'alice' },
}
const room = { id: 'r1', siteId: 's1', type: 'channel', userCount: 5 }

describe('MessageRow', () => {
  it('renders sender, time, and content', () => {
    render(<MessageRow message={msg} room={room} context="main" onThread={() => {}} onReply={() => {}} onJumpToMessage={() => {}} />)
    expect(screen.getByText('Alice')).toBeInTheDocument()
    expect(screen.getByText('hello world')).toBeInTheDocument()
    expect(screen.getByText(/\d\d:\d\d/)).toBeInTheDocument()
  })

  it('renders the timestamp with both date and time', () => {
    const { container } = render(
      <MessageRow message={msg} room={room} context="main" onThread={() => {}} onReply={() => {}} onJumpToMessage={() => {}} />
    )
    // createdAt is 2026-05-13T10:42:00Z — regardless of the test runner's
    // timezone this stays within 2026, so asserting the year proves the date
    // is rendered alongside the HH:MM time.
    const timeEl = container.querySelector('.message-time')
    expect(timeEl.textContent).toMatch(/2026/)
    expect(timeEl.textContent).toMatch(/\d{1,2}:\d{2}/)
  })

  it('renders the row with tabindex 0 and data-message-id', () => {
    const { container } = render(
      <MessageRow message={msg} room={room} context="main" onThread={() => {}} onReply={() => {}} onJumpToMessage={() => {}} />
    )
    const row = container.querySelector('.message-row')
    expect(row).not.toBeNull()
    expect(row.getAttribute('tabindex')).toBe('0')
    expect(row.getAttribute('data-message-id')).toBe('m1')
  })

  it('mounts the read-receipt kebab (MessageActionMenu) on hover and forwards message + room', () => {
    const { container } = render(<MessageRow message={msg} room={room} context="main" onThread={() => {}} onReply={() => {}} onJumpToMessage={() => {}} />)
    // Menu is JS-driven; only in the DOM while bubble is hovered. Simulate
    // mouseenter so the toolbar (and the kebab inside it) mounts.
    fireEvent.mouseEnter(container.querySelector('.message-bubble-wrap'))
    const kebab = screen.getByTestId('kebab')
    expect(kebab.getAttribute('data-message-id')).toBe('m1')
    expect(kebab.getAttribute('data-room-id')).toBe('r1')
  })

  it('renders an in-bubble QuotedBlock when message.quotedParentMessage is set', () => {
    const quoted = {
      ...msg,
      quotedParentMessage: { id: 'orig', senderName: 'bob', content: 'the original' },
    }
    render(<MessageRow message={quoted} room={room} context="main" onThread={() => {}} onReply={() => {}} onJumpToMessage={() => {}} />)
    expect(screen.getByText('bob')).toBeInTheDocument()
    expect(screen.getByText('the original')).toBeInTheDocument()
  })

  it('clicking the in-bubble quote fires onJumpToMessage with the original id', () => {
    const onJumpToMessage = vi.fn()
    const quoted = {
      ...msg,
      quotedParentMessage: { id: 'orig', senderName: 'bob', content: 'the original' },
    }
    const { container } = render(
      <MessageRow message={quoted} room={room} context="main" onThread={() => {}} onReply={() => {}} onJumpToMessage={onJumpToMessage} />
    )
    fireEvent.click(container.querySelector('.quoted-block-bubble'))
    expect(onJumpToMessage).toHaveBeenCalledWith('orig')
  })

  it('forwards Thread/Reply clicks via MessageActions (after hover-reveal)', () => {
    const onThread = vi.fn()
    const onReply = vi.fn()
    const { container } = render(<MessageRow message={msg} room={room} context="main" onThread={onThread} onReply={onReply} onJumpToMessage={() => {}} />)
    // Hover to mount the action toolbar; clicking Thread / Reply otherwise
    // doesn't find the buttons (they aren't in the DOM until hover).
    fireEvent.mouseEnter(container.querySelector('.message-bubble-wrap'))
    fireEvent.click(screen.getByRole('button', { name: /reply in thread/i }))
    expect(onThread).toHaveBeenCalledWith(msg)
    fireEvent.click(screen.getByRole('button', { name: /quote/i }))
    expect(onReply).toHaveBeenCalledWith(msg)
  })

  it('renders the nested sender displayName when userDisplayName is absent', () => {
    const botMsg = {
      id: 'm-bot',
      content: 'deploy finished',
      createdAt: '2026-05-13T10:42:00Z',
      sender: { account: 'bot', displayName: 'Deploy Bot' },
    }
    render(<MessageRow message={botMsg} room={room} context="main" onThread={() => {}} onReply={() => {}} onJumpToMessage={() => {}} />)
    expect(screen.getByText('Deploy Bot')).toBeInTheDocument()
  })
})

describe('MessageRow — historySharedSince quote gate', () => {
  it('renders the placeholder when quote.createdAt is before the subscription historySharedSince', () => {
    mockSubscription = { historySharedSince: '2026-05-13T10:00:00Z' }
    const quoted = {
      ...msg,
      quotedParentMessage: {
        messageId: 'orig',
        sender: { engName: 'bob', account: 'bob' },
        msg: 'the original',
        createdAt: '2026-05-13T09:00:00Z',
      },
    }
    render(<MessageRow message={quoted} room={room} context="main" onThread={() => {}} onReply={() => {}} onJumpToMessage={() => {}} />)
    expect(screen.getByText('This message is unavailable')).toBeInTheDocument()
    expect(screen.queryByText('the original')).not.toBeInTheDocument()
    expect(screen.queryByText('bob')).not.toBeInTheDocument()
    mockSubscription = undefined
  })

  it('renders the original quote content when quote.createdAt is after historySharedSince', () => {
    mockSubscription = { historySharedSince: '2026-05-13T10:00:00Z' }
    const quoted = {
      ...msg,
      quotedParentMessage: {
        messageId: 'orig',
        sender: { engName: 'bob', account: 'bob' },
        msg: 'the original',
        createdAt: '2026-05-13T11:00:00Z',
      },
    }
    render(<MessageRow message={quoted} room={room} context="main" onThread={() => {}} onReply={() => {}} onJumpToMessage={() => {}} />)
    expect(screen.getByText('the original')).toBeInTheDocument()
    expect(screen.queryByText('This message is unavailable')).not.toBeInTheDocument()
    mockSubscription = undefined
  })

  it('renders the original quote content when no historySharedSince is set (unrestricted)', () => {
    mockSubscription = { historySharedSince: undefined }
    const quoted = {
      ...msg,
      quotedParentMessage: {
        messageId: 'orig',
        sender: { engName: 'bob', account: 'bob' },
        msg: 'the original',
        createdAt: '2020-01-01T00:00:00Z',
      },
    }
    render(<MessageRow message={quoted} room={room} context="main" onThread={() => {}} onReply={() => {}} onJumpToMessage={() => {}} />)
    expect(screen.getByText('the original')).toBeInTheDocument()
    mockSubscription = undefined
  })
})

describe('MessageRow — display markers', () => {
  it('renders "(edited)" marker when message.editedAt is set', () => {
    render(
      <MessageRow
        message={{ ...msg, editedAt: '2026-05-13T11:00:00Z' }}
        room={room}
        context="main"
        onThread={() => {}} onReply={() => {}} onJumpToMessage={() => {}}
      />
    )
    expect(screen.getByText(/\(edited\)/i)).toBeInTheDocument()
  })

  it('omits "(edited)" marker when editedAt is unset', () => {
    render(
      <MessageRow
        message={msg}
        room={room}
        context="main"
        onThread={() => {}} onReply={() => {}} onJumpToMessage={() => {}}
      />
    )
    expect(screen.queryByText(/\(edited\)/i)).not.toBeInTheDocument()
  })

  it('renders nothing when message.deleted is true (row removed entirely)', () => {
    const { container } = render(
      <MessageRow
        message={{ ...msg, deleted: true }}
        room={room}
        context="main"
        onThread={() => {}} onReply={() => {}} onJumpToMessage={() => {}}
      />
    )
    expect(container.firstChild).toBeNull()
    expect(screen.queryByText(/message deleted/i)).not.toBeInTheDocument()
  })
})

describe('MessageRow — failed-row UI', () => {
  it('renders Retry and Dismiss when _status is failed', () => {
    render(
      <MessageRow
        message={{ ...msg, _status: 'failed', _local: true }}
        room={room}
        context="thread"
        onRetry={() => {}}
        onDismiss={() => {}}
        onThread={() => {}} onReply={() => {}} onJumpToMessage={() => {}}
      />
    )
    expect(screen.getByRole('button', { name: /retry/i })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /dismiss/i })).toBeInTheDocument()
  })

  it('clicking Retry calls onRetry with message id; Dismiss calls onDismiss', () => {
    const onRetry = vi.fn()
    const onDismiss = vi.fn()
    render(
      <MessageRow
        message={{ ...msg, _status: 'failed', _local: true }}
        room={room}
        context="thread"
        onRetry={onRetry}
        onDismiss={onDismiss}
        onThread={() => {}} onReply={() => {}} onJumpToMessage={() => {}}
      />
    )
    fireEvent.click(screen.getByRole('button', { name: /retry/i }))
    expect(onRetry).toHaveBeenCalledWith('m1')
    fireEvent.click(screen.getByRole('button', { name: /dismiss/i }))
    expect(onDismiss).toHaveBeenCalledWith('m1')
  })

  it('does not render Retry/Dismiss when _status is not failed', () => {
    render(
      <MessageRow message={msg} room={room} context="thread" onThread={() => {}} onReply={() => {}} onJumpToMessage={() => {}} />
    )
    expect(screen.queryByRole('button', { name: /retry/i })).not.toBeInTheDocument()
  })
})

describe('MessageRow — reply-count badge', () => {
  it('renders the badge when tcount > 0; clicking calls onThread', () => {
    const onThread = vi.fn()
    render(
      <MessageRow
        message={{ ...msg, tcount: 3 }}
        room={room}
        context="main"
        onThread={onThread}
        onReply={() => {}} onJumpToMessage={() => {}}
      />
    )
    const btn = screen.getByRole('button', { name: /3 replies/i })
    fireEvent.click(btn)
    expect(onThread).toHaveBeenCalledWith(expect.objectContaining({ id: 'm1' }))
  })

  it('singular form for tcount === 1', () => {
    render(
      <MessageRow
        message={{ ...msg, tcount: 1 }}
        room={room}
        context="main"
        onThread={() => {}} onReply={() => {}} onJumpToMessage={() => {}}
      />
    )
    expect(screen.getByRole('button', { name: /1 reply/i })).toBeInTheDocument()
  })

  it('no badge when tcount is 0 or missing', () => {
    render(
      <MessageRow message={msg} room={room} context="main" onThread={() => {}} onReply={() => {}} onJumpToMessage={() => {}} />
    )
    expect(screen.queryByRole('button', { name: /replies?/i })).not.toBeInTheDocument()
  })

  it('prefers userDisplayName over sender.engName for the sender label', () => {
    render(
      <MessageRow
        message={{ ...msg, userDisplayName: 'Alice Lee (愛麗絲)' }}
        room={room}
        context="main"
        onThread={() => {}}
        onReply={() => {}}
        onJumpToMessage={() => {}}
      />,
    )
    expect(screen.getByText('Alice Lee (愛麗絲)')).toBeInTheDocument()
    expect(screen.queryByText('Alice')).not.toBeInTheDocument()
  })

  it('shows a pinned indicator when pinnedAt is set', () => {
    render(
      <MessageRow
        message={{ ...msg, pinnedAt: '2026-05-13T11:00:00Z', pinnedBy: { account: 'bob', engName: 'Bob' } }}
        room={room}
        context="main"
        onThread={() => {}}
        onReply={() => {}}
        onJumpToMessage={() => {}}
      />,
    )
    expect(screen.getByLabelText(/pinned/i)).toBeInTheDocument()
  })

  it('renders no pinned indicator on an unpinned message', () => {
    render(
      <MessageRow message={msg} room={room} context="main" onThread={() => {}} onReply={() => {}} onJumpToMessage={() => {}} />,
    )
    expect(screen.queryByLabelText(/pinned/i)).not.toBeInTheDocument()
  })

  it('renders attachments with URLs built from the media base', () => {
    const message = {
      ...msg,
      attachments: [
        { id: 'a1', title: 'doc.pdf', type: 'file', titleLink: 'api/v1/file/rooms/r1/file/a1', fileType: 'application/pdf' },
      ],
    }
    render(
      <MessageRow message={message} room={room} context="main" onThread={() => {}} onReply={() => {}} onJumpToMessage={() => {}} />,
    )
    expect(screen.getByRole('link', { name: /doc\.pdf/ })).toHaveAttribute(
      'href',
      'https://media.test/api/v1/file/rooms/r1/file/a1',
    )
  })

  it('renders reaction chips carried by the message', () => {
    const message = { ...msg, reactions: { '👍': [{ account: 'bob', displayName: 'Bob' }] } }
    render(
      <MessageRow message={message} room={room} context="main" onThread={() => {}} onReply={() => {}} onJumpToMessage={() => {}} />,
    )
    expect(screen.getByText('👍')).toBeInTheDocument()
    expect(screen.getByText('1')).toBeInTheDocument()
  })

  it('no badge inside the thread panel even when tcount > 0', () => {
    render(
      <MessageRow message={{ ...msg, tcount: 5 }} room={room} context="thread-parent" onThread={() => {}} onReply={() => {}} onJumpToMessage={() => {}} />
    )
    expect(screen.queryByRole('button', { name: /replies/i })).not.toBeInTheDocument()
  })
})
