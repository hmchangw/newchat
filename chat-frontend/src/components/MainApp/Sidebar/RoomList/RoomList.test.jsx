import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import RoomList from './RoomList'

vi.mock('@/context/RoomEventsContext', () => ({
  useRoomSummaries: vi.fn(),
  useSidebarSections: vi.fn(),
  useChatlistActions: vi.fn(),
}))

import { useRoomSummaries, useSidebarSections, useChatlistActions } from '@/context/RoomEventsContext'

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

function section(key, rooms, over = {}) {
  return { key, title: over.title ?? key, builtIn: over.builtIn ?? true, sortMode: over.sortMode ?? 'mostRecent', rooms }
}

const actions = {
  createSection: vi.fn(),
  renameSection: vi.fn(),
  deleteSection: vi.fn(),
  setSortMode: vi.fn(),
  moveChatTo: vi.fn(),
}

function setup(sections, { error = null } = {}) {
  const all = sections.flatMap((s) => s.rooms)
  useRoomSummaries.mockReturnValue({ summaries: all, setActiveRoom: vi.fn(), error })
  useSidebarSections.mockReturnValue(sections)
  useChatlistActions.mockReturnValue(actions)
}

beforeEach(() => vi.clearAllMocks())

describe('RoomList: v2 grouped render', () => {
  it('renders each provided section header + rooms', () => {
    setup([
      section('favorites', [summary('f1', { name: 'fav' })], { title: 'Favorites' }),
      section('work', [summary('w1', { name: 'proj' })], { title: 'Work', builtIn: false, sortMode: 'custom' }),
      section('chats', [summary('c1', { name: 'gen' })], { title: 'Chats' }),
    ])
    render(<RoomList selectedRoomId={null} onSelectRoom={vi.fn()} />)
    expect(screen.getByText('Favorites')).toBeInTheDocument()
    expect(screen.getByText('Work')).toBeInTheDocument()
    expect(screen.getByText('Chats')).toBeInTheDocument()
    expect(screen.getByText(/# gen/)).toBeInTheDocument()
  })

  it('shows a "No rooms" placeholder under an empty section', () => {
    setup([section('work', [], { title: 'Work', builtIn: false, sortMode: 'custom' }), section('chats', [summary('c1')])])
    const { container } = render(<RoomList selectedRoomId={null} onSelectRoom={vi.fn()} />)
    expect(container.querySelectorAll('.room-list-section-empty').length).toBe(1)
  })

  it('toggles collapse on the section title click', () => {
    setup([section('chats', [summary('c1', { name: 'general' })])])
    render(<RoomList selectedRoomId={null} onSelectRoom={vi.fn()} />)
    expect(screen.getByText(/# general/)).toBeInTheDocument()
    fireEvent.click(screen.getByText('chats'))
    expect(screen.queryByText(/# general/)).not.toBeInTheDocument()
  })

  it('renders room item chrome: prefix, mention badge, unread badge, userCount', () => {
    setup([section('chats', [summary('c1', { name: 'general', unreadCount: 3, hasMention: true })])])
    const { container } = render(<RoomList selectedRoomId={null} onSelectRoom={vi.fn()} />)
    expect(screen.getByText(/# general/)).toBeInTheDocument()
    expect(container.querySelector('.room-badge-mention')).toBeInTheDocument()
    expect(container.querySelector('.room-badge-unread').textContent).toBe('3')
    expect(container.querySelector('.room-meta').textContent).toBe('2')
  })

  it('calls onSelectRoom when a room is clicked', () => {
    const onSelectRoom = vi.fn()
    setup([section('chats', [summary('c1', { name: 'gen' })])])
    render(<RoomList selectedRoomId={null} onSelectRoom={onSelectRoom} />)
    fireEvent.click(screen.getByText(/# gen/))
    expect(onSelectRoom).toHaveBeenCalledWith(expect.objectContaining({ id: 'c1' }))
  })

  it('surfaces the rooms-load error', () => {
    setup([], { error: 'oh no' })
    render(<RoomList selectedRoomId={null} onSelectRoom={vi.fn()} />)
    expect(screen.getByText('oh no')).toBeInTheDocument()
  })
})

describe('RoomList: section controls', () => {
  it('shows rename/delete controls only on custom sections', () => {
    setup([
      section('chats', [summary('c1')], { title: 'Chats' }),
      section('work', [summary('w1')], { title: 'Work', builtIn: false, sortMode: 'custom' }),
    ])
    render(<RoomList selectedRoomId={null} onSelectRoom={vi.fn()} />)
    expect(screen.getByLabelText('Rename Work')).toBeInTheDocument()
    expect(screen.getByLabelText('Delete Work')).toBeInTheDocument()
    expect(screen.queryByLabelText('Rename Chats')).toBeNull()
  })

  it('delete calls deleteSection with the section id', () => {
    setup([section('work', [summary('w1')], { title: 'Work', builtIn: false, sortMode: 'custom' })])
    render(<RoomList selectedRoomId={null} onSelectRoom={vi.fn()} />)
    fireEvent.click(screen.getByLabelText('Delete Work'))
    expect(actions.deleteSection).toHaveBeenCalledWith('work')
  })

  it('sort toggle flips custom↔mostRecent', () => {
    setup([section('work', [summary('w1')], { title: 'Work', builtIn: false, sortMode: 'custom' })])
    render(<RoomList selectedRoomId={null} onSelectRoom={vi.fn()} />)
    fireEvent.click(screen.getByLabelText('Sort Work'))
    expect(actions.setSortMode).toHaveBeenCalledWith('work', 'mostRecent')
  })

  it('opens the create-section dialog from the header + button', async () => {
    setup([section('chats', [summary('c1')])])
    render(<RoomList selectedRoomId={null} onSelectRoom={vi.fn()} />)
    fireEvent.click(screen.getByLabelText('New section'))
    expect(await screen.findByRole('dialog', { name: 'New section' })).toBeInTheDocument()
  })
})

describe('RoomList: drag to move', () => {
  it('dragging a chat and dropping on a custom section moves it there', () => {
    setup([
      section('work', [], { title: 'Work', builtIn: false, sortMode: 'custom' }),
      section('chats', [summary('c1', { name: 'gen', siteId: 'site-A' })]),
    ])
    const { container } = render(<RoomList selectedRoomId={null} onSelectRoom={vi.fn()} />)
    const dataTransfer = { setData: vi.fn(), getData: vi.fn(), effectAllowed: '' }
    fireEvent.dragStart(screen.getByText(/# gen/).closest('.room-item'), { dataTransfer })
    // The Work section is the first .room-list-section
    const workSection = container.querySelectorAll('.room-list-section')[0]
    fireEvent.drop(workSection)
    expect(actions.moveChatTo).toHaveBeenCalledWith('c1', 'site-A', 'work', undefined)
  })

  it('dropping a chat on the Chats section removes it (sectionId null)', () => {
    setup([
      section('work', [summary('w1', { name: 'proj', siteId: 'site-A' })], { title: 'Work', builtIn: false, sortMode: 'custom' }),
      section('chats', [summary('c1')]),
    ])
    const { container } = render(<RoomList selectedRoomId={null} onSelectRoom={vi.fn()} />)
    const dataTransfer = { setData: vi.fn(), getData: vi.fn(), effectAllowed: '' }
    fireEvent.dragStart(screen.getByText(/# proj/).closest('.room-item'), { dataTransfer })
    const chatsSection = container.querySelectorAll('.room-list-section')[1]
    fireEvent.drop(chatsSection)
    expect(actions.moveChatTo).toHaveBeenCalledWith('w1', 'site-A', null, undefined)
  })

  it('dropping onto a specific room passes it as afterRoomId', () => {
    setup([
      section('work', [summary('w1', { name: 'proj', siteId: 'site-A' })], { title: 'Work', builtIn: false, sortMode: 'custom' }),
      section('chats', [summary('c1', { name: 'gen', siteId: 'site-A' })]),
    ])
    render(<RoomList selectedRoomId={null} onSelectRoom={vi.fn()} />)
    const dataTransfer = { setData: vi.fn(), getData: vi.fn(), effectAllowed: '' }
    fireEvent.dragStart(screen.getByText(/# gen/).closest('.room-item'), { dataTransfer })
    fireEvent.drop(screen.getByText(/# proj/).closest('.room-item'))
    expect(actions.moveChatTo).toHaveBeenCalledWith('c1', 'site-A', 'work', 'w1')
  })
})
