import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import RoomList from './RoomList'

vi.mock('../context/RoomEventsContext', () => ({
  useRoomSummaries: vi.fn(),
  useSidebarSections: vi.fn(),
}))

import { useRoomSummaries, useSidebarSections } from '../context/RoomEventsContext'

function summary(id, overrides = {}) {
  return {
    id,
    name: id,
    type: 'channel',
    siteId: 'site-A',
    userCount: 2,
    lastMsgAt: '2026-04-17T10:00:00Z',
    unreadCount: 0,
    hasMention: false,
    mentionAll: false,
    ...overrides,
  }
}

function setupSections({ favorite = [], apps = [], channelDm = [], error = null } = {}) {
  useRoomSummaries.mockReturnValue({
    summaries: [...favorite, ...apps, ...channelDm],
    setActiveRoom: vi.fn(),
    error,
  })
  useSidebarSections.mockReturnValue([
    { key: 'favorite',  title: 'Favorite',         rooms: favorite },
    { key: 'apps',      title: 'Apps',             rooms: apps },
    { key: 'channelDm', title: 'Channels and DMs', rooms: channelDm },
  ])
}

describe('RoomList: three-section render', () => {
  it('renders all three section headers when each section has rooms', () => {
    setupSections({
      favorite:  [summary('f1', { name: 'fav' })],
      apps:      [summary('a1', { name: 'app',  type: 'botDM' })],
      channelDm: [summary('c1', { name: 'gen' })],
    })
    render(<RoomList selectedRoomId={null} onSelectRoom={vi.fn()} />)
    expect(screen.getByText('Favorite')).toBeInTheDocument()
    expect(screen.getByText('Apps')).toBeInTheDocument()
    expect(screen.getByText('Channels and DMs')).toBeInTheDocument()
  })

  it('renders sections in fixed order: Favorite, Apps, Channels and DMs', () => {
    setupSections({
      favorite:  [summary('f1')],
      apps:      [summary('a1', { type: 'botDM' })],
      channelDm: [summary('c1')],
    })
    const { container } = render(<RoomList selectedRoomId={null} onSelectRoom={vi.fn()} />)
    const headers = Array.from(container.querySelectorAll('.room-list-section-header')).map(
      (el) => el.textContent
    )
    expect(headers).toEqual(['Favorite', 'Apps', 'Channels and DMs'])
  })

  it('hides empty sections (no header rendered)', () => {
    setupSections({ favorite: [], apps: [], channelDm: [summary('c1')] })
    render(<RoomList selectedRoomId={null} onSelectRoom={vi.fn()} />)
    expect(screen.queryByText('Favorite')).not.toBeInTheDocument()
    expect(screen.queryByText('Apps')).not.toBeInTheDocument()
    expect(screen.getByText('Channels and DMs')).toBeInTheDocument()
  })

  it('toggles section collapse on header click', () => {
    setupSections({
      favorite: [],
      apps: [],
      channelDm: [summary('c1', { name: 'general' })],
    })
    render(<RoomList selectedRoomId={null} onSelectRoom={vi.fn()} />)
    expect(screen.getByText(/# general/)).toBeInTheDocument()
    fireEvent.click(screen.getByText('Channels and DMs'))
    expect(screen.queryByText(/# general/)).not.toBeInTheDocument()
    fireEvent.click(screen.getByText('Channels and DMs'))
    expect(screen.getByText(/# general/)).toBeInTheDocument()
  })

  it('preserves room item rendering: prefix, mention badge, unread badge, userCount', () => {
    setupSections({
      favorite: [],
      apps: [],
      channelDm: [
        summary('c1', { name: 'general', unreadCount: 3, hasMention: true }),
      ],
    })
    const { container } = render(<RoomList selectedRoomId={null} onSelectRoom={vi.fn()} />)
    expect(screen.getByText(/# general/)).toBeInTheDocument()
    expect(container.querySelector('.room-badge-mention')).toBeInTheDocument()
    expect(container.querySelector('.room-badge-unread').textContent).toBe('3')
    expect(container.querySelector('.room-meta').textContent).toBe('2')
  })

  it('calls onSelectRoom when a room item is clicked', () => {
    const onSelectRoom = vi.fn()
    setupSections({ favorite: [summary('f1', { name: 'fav' })], apps: [], channelDm: [] })
    render(<RoomList selectedRoomId={null} onSelectRoom={onSelectRoom} />)
    fireEvent.click(screen.getByText(/# fav/))
    expect(onSelectRoom).toHaveBeenCalledWith(expect.objectContaining({ id: 'f1' }))
  })

  it('still surfaces the rooms-load error', () => {
    setupSections({ error: 'oh no' })
    render(<RoomList selectedRoomId={null} onSelectRoom={vi.fn()} />)
    expect(screen.getByText('oh no')).toBeInTheDocument()
  })
})
