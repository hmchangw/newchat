import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import SearchBar from './SearchBar'

vi.mock('@/context/NatsContext', () => ({
  useNats: vi.fn(),
}))

import { useNats } from '@/context/NatsContext'

describe('SearchBar', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    vi.useFakeTimers({ shouldAdvanceTime: true })
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('does not search when query < 2 chars', async () => {
    const request = vi.fn()
    useNats.mockReturnValue({
      user: { account: 'alice' },
      request,
    })

    render(<SearchBar onSelectRoom={vi.fn()} onEnterSearch={vi.fn()} />)
    fireEvent.change(screen.getByRole('textbox'), { target: { value: 'a' } })
    vi.runAllTimers()

    expect(request).not.toHaveBeenCalled()
  })

  it('fetches rooms after 250ms debounce when query >= 2 chars', async () => {
    const request = vi.fn().mockResolvedValue({
      rooms: [
        { roomId: 'r1', name: 'general', roomType: 'c', siteId: 'site-A' },
      ],
      total: 1,
    })
    useNats.mockReturnValue({
      user: { account: 'alice' },
      request,
    })

    render(<SearchBar onSelectRoom={vi.fn()} onEnterSearch={vi.fn()} />)
    fireEvent.change(screen.getByRole('textbox'), { target: { value: 'fro' } })

    expect(request).not.toHaveBeenCalled()

    vi.advanceTimersByTime(250)
    await waitFor(() => {
      expect(request).toHaveBeenCalledWith(
        'chat.user.alice.request.search.rooms',
        { query: 'fro', roomType: 'all', size: 8 }
      )
    })
  })

  it('shows results in dropdown', async () => {
    const request = vi.fn().mockResolvedValue({
      rooms: [
        { roomId: 'r1', name: 'frontend-team', roomType: 'c', siteId: 'site-A' },
        { roomId: 'r2', name: 'frontend-perf', roomType: 'c', siteId: 'site-A' },
      ],
      total: 2,
    })
    useNats.mockReturnValue({
      user: { account: 'alice' },
      request,
    })

    render(<SearchBar onSelectRoom={vi.fn()} onEnterSearch={vi.fn()} />)
    fireEvent.change(screen.getByRole('textbox'), { target: { value: 'fro' } })

    vi.advanceTimersByTime(250)
    await waitFor(() => {
      expect(screen.getByText('frontend-team')).toBeInTheDocument()
      expect(screen.getByText('frontend-perf')).toBeInTheDocument()
    })
  })

  it('clicking result calls onSelectRoom and clears input', async () => {
    const onSelectRoom = vi.fn()
    const request = vi.fn().mockResolvedValue({
      rooms: [
        { roomId: 'r1', name: 'general', roomType: 'c', siteId: 'site-A' },
      ],
      total: 1,
    })
    useNats.mockReturnValue({
      user: { account: 'alice' },
      request,
    })

    render(<SearchBar onSelectRoom={onSelectRoom} onEnterSearch={vi.fn()} />)
    fireEvent.change(screen.getByRole('textbox'), { target: { value: 'gen' } })

    vi.advanceTimersByTime(250)
    await waitFor(() => {
      fireEvent.click(screen.getByText('general'))
    })

    expect(onSelectRoom).toHaveBeenCalledWith({
      id: 'r1',
      name: 'general',
      type: 'c',
      siteId: 'site-A',
    })
    expect(screen.getByRole('textbox').value).toBe('')
  })

  it('Enter key calls onEnterSearch', async () => {
    const onEnterSearch = vi.fn()
    useNats.mockReturnValue({
      user: { account: 'alice' },
      request: vi.fn().mockResolvedValue({ rooms: [], total: 0 }),
    })

    render(<SearchBar onSelectRoom={vi.fn()} onEnterSearch={onEnterSearch} />)
    fireEvent.change(screen.getByRole('textbox'), { target: { value: 'test' } })

    vi.advanceTimersByTime(250)
    fireEvent.keyDown(screen.getByRole('textbox'), { key: 'Enter' })

    expect(onEnterSearch).toHaveBeenCalledWith('test')
  })

  it('Escape key closes dropdown and clears input', async () => {
    useNats.mockReturnValue({
      user: { account: 'alice' },
      request: vi.fn().mockResolvedValue({
        rooms: [{ roomId: 'r1', name: 'general', roomType: 'c', siteId: 'site-A' }],
        total: 1,
      }),
    })

    render(<SearchBar onSelectRoom={vi.fn()} onEnterSearch={vi.fn()} />)
    fireEvent.change(screen.getByRole('textbox'), { target: { value: 'gen' } })

    vi.advanceTimersByTime(250)
    await waitFor(() => {
      expect(screen.getByText('general')).toBeInTheDocument()
    })

    fireEvent.keyDown(screen.getByRole('textbox'), { key: 'Escape' })

    expect(screen.queryByText('general')).not.toBeInTheDocument()
    expect(screen.getByRole('textbox').value).toBe('')
  })
})
