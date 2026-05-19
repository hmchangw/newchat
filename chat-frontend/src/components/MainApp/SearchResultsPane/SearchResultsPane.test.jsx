import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import SearchResultsPane from './SearchResultsPane'

vi.mock('@/context/NatsContext', () => ({
  useNats: vi.fn(),
}))

import { useNats } from '@/context/NatsContext'

describe('SearchResultsPane', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('fetches and displays room results immediately', async () => {
    const request = vi.fn().mockResolvedValue({
      rooms: [
        { roomId: 'r1', name: 'general', roomType: 'c', siteId: 'site-A' },
      ],
    })
    useNats.mockReturnValue({
      user: { account: 'alice' },
      request,
    })

    render(
      <SearchResultsPane
        query="gen"
        onClose={vi.fn()}
        onSelectRoom={vi.fn()}
        onJumpToMessage={vi.fn()}
      />
    )

    await waitFor(() => {
      expect(screen.getByText('general')).toBeInTheDocument()
    })

    expect(request).toHaveBeenCalledWith(
      'chat.user.alice.request.search.rooms',
      { query: 'gen', roomType: 'all', size: 50 }
    )
  })

  it('Rooms tab shows results, Messages tab fetches on click', async () => {
    const request = vi.fn().mockImplementation((subject) => {
      if (subject.includes('.search.rooms')) {
        return Promise.resolve({
          rooms: [{ roomId: 'r1', name: 'general', roomType: 'c', siteId: 'site-A' }],
        })
      }
      if (subject.includes('.search.messages')) {
        return Promise.resolve({
          messages: [
            { messageId: 'm1', roomId: 'r1', content: 'hello', createdAt: '2026-04-17T10:00:00Z', userAccount: 'bob' },
          ],
          total: 1,
        })
      }
    })
    useNats.mockReturnValue({
      user: { account: 'alice' },
      request,
    })

    render(
      <SearchResultsPane
        query="test"
        onClose={vi.fn()}
        onSelectRoom={vi.fn()}
        onJumpToMessage={vi.fn()}
      />
    )

    // Room results shown on Rooms tab
    await waitFor(() => {
      expect(screen.getByText('general')).toBeInTheDocument()
    })

    // Click Messages tab
    fireEvent.click(screen.getByRole('tab', { name: /Messages/ }))

    // Messages results show
    await waitFor(() => {
      expect(screen.getByText('hello')).toBeInTheDocument()
    })
  })

  it('clicking room result calls onSelectRoom and onClose', async () => {
    const onSelectRoom = vi.fn()
    const onClose = vi.fn()
    const request = vi.fn().mockResolvedValue({
      rooms: [
        { roomId: 'r1', name: 'general', roomType: 'c', siteId: 'site-A' },
      ],
    })
    useNats.mockReturnValue({
      user: { account: 'alice' },
      request,
    })

    render(
      <SearchResultsPane
        query="gen"
        onClose={onClose}
        onSelectRoom={onSelectRoom}
        onJumpToMessage={vi.fn()}
      />
    )

    await waitFor(() => {
      fireEvent.click(screen.getByText('general'))
    })

    expect(onSelectRoom).toHaveBeenCalledWith({
      id: 'r1',
      name: 'general',
      type: 'c',
      siteId: 'site-A',
    })
    expect(onClose).toHaveBeenCalled()
  })

  it('clicking message result calls onJumpToMessage and onClose', async () => {
    const onJumpToMessage = vi.fn()
    const onClose = vi.fn()
    const request = vi.fn().mockImplementation((subject) => {
      if (subject.includes('.search.rooms')) {
        return Promise.resolve({ rooms: [] })
      }
      if (subject.includes('.search.messages')) {
        return Promise.resolve({
          messages: [
            { messageId: 'm1', roomId: 'r1', content: 'hello world', createdAt: '2026-04-17T10:00:00Z', userAccount: 'bob' },
          ],
          total: 1,
        })
      }
    })
    useNats.mockReturnValue({
      user: { account: 'alice' },
      request,
    })

    render(
      <SearchResultsPane
        query="hello"
        onClose={onClose}
        onSelectRoom={vi.fn()}
        onJumpToMessage={onJumpToMessage}
      />
    )

    fireEvent.click(screen.getByRole('tab', { name: /Messages/ }))

    await waitFor(() => {
      expect(screen.getByText('hello world')).toBeInTheDocument()
    })

    fireEvent.click(screen.getByText('hello world'))

    expect(onJumpToMessage).toHaveBeenCalledWith('r1', 'm1')
    expect(onClose).toHaveBeenCalled()
  })
})
