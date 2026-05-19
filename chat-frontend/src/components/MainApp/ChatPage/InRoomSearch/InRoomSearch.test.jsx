import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import InRoomSearch from './InRoomSearch'

vi.mock('@/context/NatsContext', () => ({
  useNats: vi.fn(),
}))

import { useNats } from '@/context/NatsContext'

describe('InRoomSearch', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('does not fetch on typing; fetches on Enter scoped to roomIds: [roomId]', async () => {
    const request = vi.fn().mockResolvedValue({ messages: [], total: 0 })
    useNats.mockReturnValue({
      user: { account: 'alice' },
      request,
    })

    render(
      <InRoomSearch
        roomId="r1"
        onClose={vi.fn()}
        onJumpToMessage={vi.fn()}
      />
    )

    const input = screen.getByRole('textbox')
    fireEvent.change(input, { target: { value: 'hi' } })

    expect(request).not.toHaveBeenCalled()

    fireEvent.keyDown(input, { key: 'Enter' })

    await waitFor(() => {
      expect(request).toHaveBeenCalledWith(
        'chat.user.alice.request.search.messages',
        { query: 'hi', roomIds: ['r1'], size: 50 }
      )
    })
  })

  it('clicking a result calls onJumpToMessage with the message id', async () => {
    const onJumpToMessage = vi.fn()
    const onClose = vi.fn()
    const request = vi.fn().mockResolvedValue({
      messages: [
        { messageId: 'msg-1', roomId: 'r1', content: 'hello world' },
      ],
      total: 1,
    })
    useNats.mockReturnValue({
      user: { account: 'alice' },
      request,
    })

    render(
      <InRoomSearch
        roomId="r1"
        onClose={onClose}
        onJumpToMessage={onJumpToMessage}
      />
    )

    const input = screen.getByRole('textbox')
    fireEvent.change(input, { target: { value: 'hello' } })
    fireEvent.keyDown(input, { key: 'Enter' })

    await waitFor(() => {
      expect(screen.getByText('hello world')).toBeInTheDocument()
    })

    fireEvent.click(screen.getByText('hello world'))

    expect(onJumpToMessage).toHaveBeenCalledWith('msg-1')
    expect(onClose).toHaveBeenCalled()
  })

  it('X button calls onClose', () => {
    const onClose = vi.fn()
    useNats.mockReturnValue({
      user: { account: 'alice' },
      request: vi.fn().mockResolvedValue({ messages: [], total: 0 }),
    })

    render(
      <InRoomSearch
        roomId="r1"
        onClose={onClose}
        onJumpToMessage={vi.fn()}
      />
    )

    fireEvent.click(screen.getByLabelText(/close/i))
    expect(onClose).toHaveBeenCalled()
  })
})
