import { render, screen, fireEvent } from '@testing-library/react'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import ChatPage from './ChatPage'
import { vi as viE2E } from 'vitest'

vi.mock('./RoomMessageArea/RoomMessageArea', () => ({
  default: ({ onReply, onThread }) => (
    <div>
      area
      <button type="button" onClick={() => onReply?.({ id: 'm-orig', sender: { account: 'alice' }, content: 'hello there' })}>
        fire-reply
      </button>
      <button
        type="button"
        onClick={() => onThread?.({ id: 'm-thread', createdAt: '2026-05-13T10:00:00.000Z' })}
      >
        fire-thread
      </button>
    </div>
  ),
}))
vi.mock('./RoomMessageInput/RoomMessageInput', () => ({
  default: ({ room, quotedTarget, onClearQuote }) => (
    <div>
      input:{room?.id ?? 'none'}
      {quotedTarget && (
        <>
          <span data-testid="staged">staged:{quotedTarget.id}</span>
          <button type="button" onClick={onClearQuote}>clear-staged</button>
        </>
      )}
    </div>
  ),
}))
vi.mock('./InRoomSearch/InRoomSearch', () => ({
  default: ({ onClose }) => (
    <aside role="complementary">
      in-room-search
      <button type="button" onClick={onClose}>close-inroom</button>
    </aside>
  ),
}))
vi.mock('./ManageMembersDialog/ManageMembersDialog', () => ({
  default: ({ onClose }) => (
    <div role="dialog">
      members-dialog
      <button type="button" onClick={onClose}>close-members</button>
    </div>
  ),
}))
// RoomMembersBadge depends on NatsContext + the member.list RPC; mock it
// down to a deterministic "Members ({count})" button so we can assert on
// the open affordance and verify ChatPage forwards the room/refreshKey props.
vi.mock('./RoomMembersBadge/RoomMembersBadge', () => ({
  default: ({ room, onOpen, refreshKey }) => (
    <button
      type="button"
      onClick={onOpen}
      data-room-id={room?.id ?? ''}
      data-refresh-key={refreshKey}
    >
      Members ({room?.userCount ?? 0})
    </button>
  ),
}))
vi.mock('@/context/RoomEventsContext', () => ({
  useRoomSummaries: () => ({ jumpToMessage: vi.fn() }),
  useRoomDispatch: () => vi.fn(),
}))
const openThread = vi.fn()
const closeThread = vi.fn()
let mockActiveParent = null
vi.mock('@/context/ThreadEventsContext', () => ({
  useThreadEvents: () => ({ openThread, closeThread, activeParent: mockActiveParent }),
}))

const channel = { id: 'r1', name: 'general', type: 'channel', userCount: 7, siteId: 's1' }
const dm = { id: 'r2', name: 'alice & bob', type: 'dm', userCount: 2 }

describe('ChatPage (middle column)', () => {
  it('renders MessageArea and MessageInput with the selected room', () => {
    render(<ChatPage selectedRoom={channel} onSelectRoom={() => {}} />)
    expect(screen.getByText('area')).toBeInTheDocument()
    expect(screen.getByText('input:r1')).toBeInTheDocument()
  })

  it('renders room-header with room name and RoomMembersBadge (no Leave button)', () => {
    render(<ChatPage selectedRoom={channel} onSelectRoom={() => {}} />)
    expect(screen.getByText(/# general/)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /^members \(7\)$/i })).toBeInTheDocument()
    // Leave is intentionally NOT rendered in the header — Members is the only
    // affordance on the right edge.
    expect(screen.queryByRole('button', { name: /leave/i })).not.toBeInTheDocument()
  })

  it('renders RoomMembersBadge for DMs and never shows Leave', () => {
    render(<ChatPage selectedRoom={dm} onSelectRoom={() => {}} />)
    expect(screen.queryByRole('button', { name: /leave/i })).not.toBeInTheDocument()
  })

  // ManageMembersDialog + InRoomSearch are lazy-loaded — assertions
  // after the opening event use findBy* to await the chunk resolving.
  it('clicking the badge opens the members dialog', async () => {
    render(<ChatPage selectedRoom={channel} onSelectRoom={() => {}} />)
    fireEvent.click(screen.getByRole('button', { name: /^members \(7\)$/i }))
    expect(await screen.findByRole('dialog')).toBeInTheDocument()
  })

  it('Ctrl-F opens InRoomSearch; Esc closes it', async () => {
    render(<ChatPage selectedRoom={channel} onSelectRoom={() => {}} />)
    fireEvent.keyDown(window, { key: 'f', ctrlKey: true })
    expect(await screen.findByText('in-room-search')).toBeInTheDocument()
    fireEvent.keyDown(window, { key: 'Escape' })
    expect(screen.queryByText('in-room-search')).not.toBeInTheDocument()
  })

  it('renders no room-header when no room is selected', () => {
    render(<ChatPage selectedRoom={null} onSelectRoom={() => {}} />)
    expect(screen.queryByRole('button', { name: /^members/i })).not.toBeInTheDocument()
    expect(screen.getByText('area')).toBeInTheDocument()
  })
})

describe('ChatPage — quote-reply staging', () => {
  it('clicking Reply on a message stages the quotedTarget in the input', () => {
    render(<ChatPage selectedRoom={channel} onSelectRoom={() => {}} />)
    expect(screen.queryByTestId('staged')).not.toBeInTheDocument()
    fireEvent.click(screen.getByText('fire-reply'))
    expect(screen.getByText('staged:m-orig')).toBeInTheDocument()
  })

  it("clicking the chip's clear button clears quotedTarget", () => {
    render(<ChatPage selectedRoom={channel} onSelectRoom={() => {}} />)
    fireEvent.click(screen.getByText('fire-reply'))
    fireEvent.click(screen.getByText('clear-staged'))
    expect(screen.queryByTestId('staged')).not.toBeInTheDocument()
  })

  it('switching rooms clears quotedTarget', () => {
    const { rerender } = render(<ChatPage selectedRoom={channel} onSelectRoom={() => {}} />)
    fireEvent.click(screen.getByText('fire-reply'))
    expect(screen.getByText('staged:m-orig')).toBeInTheDocument()
    rerender(<ChatPage selectedRoom={dm} onSelectRoom={() => {}} />)
    expect(screen.queryByTestId('staged')).not.toBeInTheDocument()
  })
})

describe('ChatPage — quote-reply E2E (real RoomMessageInput)', () => {
  it('publish call carries quotedParentMessageId after Reply staging', async () => {
    viE2E.resetModules()
    const publish = viE2E.fn()
    const subscribe = viE2E.fn(() => ({ unsubscribe: viE2E.fn() }))

    viE2E.doMock('@/context/NatsContext', () => ({
      useNats: () => ({ user: { account: 'alice', siteId: 's1' }, publish, subscribe }),
    }))
    viE2E.doMock('@/lib/idgen', () => ({ generateMessageID: () => '12345678901234567890' }))
    viE2E.doMock('uuid', () => ({ v4: () => 'req-1' }))
    viE2E.doMock('./RoomMessageArea/RoomMessageArea', () => ({
      default: ({ onReply }) => (
        <button
          type="button"
          onClick={() => onReply?.({ id: 'orig', sender: { account: 'alice' }, content: 'hello' })}
        >
          stage
        </button>
      ),
    }))
    viE2E.doMock('./InRoomSearch/InRoomSearch', () => ({ default: () => null }))
    viE2E.doMock('./ManageMembersDialog/ManageMembersDialog', () => ({ default: () => null }))
    viE2E.doMock('./LeaveRoomButton/LeaveRoomButton', () => ({ default: () => null }))
    viE2E.doMock('@/context/RoomEventsContext', () => ({
      useRoomSummaries: () => ({ jumpToMessage: viE2E.fn() }),
      useRoomDispatch: () => viE2E.fn(),
    }))
    viE2E.doMock('./RoomMembersBadge/RoomMembersBadge', () => ({ default: () => null }))
    // Use the real RoomMessageInput so we can assert on the publish payload.
    // vi.mock (hoisted) would otherwise override it; doMock + importActual wins
    // for modules loaded via the subsequent dynamic import.
    viE2E.doMock('./RoomMessageInput/RoomMessageInput', async () => await viE2E.importActual('./RoomMessageInput/RoomMessageInput'))

    const { default: FreshChatPage } = await import('./ChatPage')
    render(<FreshChatPage selectedRoom={channel} onSelectRoom={() => {}} />)

    fireEvent.click(screen.getByText('stage'))
    const input = screen.getByPlaceholderText(/general/i)
    fireEvent.change(input, { target: { value: 'a reply' } })
    fireEvent.keyDown(input, { key: 'Enter' })

    expect(publish).toHaveBeenCalledWith(
      'chat.user.alice.room.r1.s1.msg.send',
      {
        id: '12345678901234567890',
        content: 'a reply',
        requestId: 'req-1',
        quotedParentMessageId: 'orig',
      }
    )
  })
})

describe('ChatPage — opening a thread', () => {
  beforeEach(() => openThread.mockClear())

  it('clicking Thread on a message calls openThread with full parent identity', () => {
    render(<ChatPage selectedRoom={channel} onSelectRoom={() => {}} />)
    fireEvent.click(screen.getByText('fire-thread'))
    expect(openThread).toHaveBeenCalledWith({
      roomId: 'r1',
      siteId: channel.siteId,
      messageId: 'm-thread',
      createdAtMs: Date.parse('2026-05-13T10:00:00.000Z'),
    })
  })
})

describe('ChatPage — mutual exclusion', () => {
  beforeEach(() => { closeThread.mockClear() })
  afterEach(() => { mockActiveParent = null })

  it('opening InRoomSearch (Ctrl-F) closes any open thread', () => {
    mockActiveParent = { roomId: channel.id, messageId: 'p1' }
    render(<ChatPage selectedRoom={channel} onSelectRoom={() => {}} />)
    fireEvent.keyDown(window, { key: 'f', ctrlKey: true })
    expect(closeThread).toHaveBeenCalled()
  })

  it('opening a thread closes InRoomSearch', async () => {
    render(<ChatPage selectedRoom={channel} onSelectRoom={() => {}} />)
    fireEvent.keyDown(window, { key: 'f', ctrlKey: true })
    expect(await screen.findByText('in-room-search')).toBeInTheDocument()
    fireEvent.click(screen.getByText('fire-thread'))
    expect(screen.queryByText('in-room-search')).not.toBeInTheDocument()
  })
})

describe('ChatPage — close thread on room switch', () => {
  beforeEach(() => {
    closeThread.mockClear()
    mockActiveParent = { roomId: channel.id, messageId: 'p1' }
  })
  afterEach(() => { mockActiveParent = null })

  it('changing selectedRoom calls closeThread', () => {
    const { rerender } = render(<ChatPage selectedRoom={channel} onSelectRoom={() => {}} />)
    rerender(<ChatPage selectedRoom={dm} onSelectRoom={() => {}} />)
    expect(closeThread).toHaveBeenCalled()
  })

  it('does NOT call closeThread when re-rendering with the same room', () => {
    const { rerender } = render(<ChatPage selectedRoom={channel} onSelectRoom={() => {}} />)
    closeThread.mockClear()
    rerender(<ChatPage selectedRoom={channel} onSelectRoom={() => {}} />)
    expect(closeThread).not.toHaveBeenCalled()
  })
})
