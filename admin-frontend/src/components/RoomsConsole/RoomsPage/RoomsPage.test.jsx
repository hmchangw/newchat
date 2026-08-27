import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'

vi.mock('@/context/AuthContext', () => ({ useAuth: vi.fn() }))
vi.mock('@/api', async (importOriginal) => {
  const actual = await importOriginal()
  return { ...actual, listRooms: vi.fn() }
})

vi.mock('../SetOnDutyDialog', () => ({
  default: ({ room, onDone, onClose }) => (
    <div role="dialog" aria-label="set onduty">
      set {room.id}
      <button type="button" onClick={onDone}>
        Fake set done
      </button>
      <button type="button" onClick={onClose}>
        Fake set close
      </button>
    </div>
  ),
}))

vi.mock('../UnsetOnDutyDialog', () => ({
  default: ({ room, onDone }) => (
    <div role="dialog" aria-label="unset onduty">
      unset {room.id}
      <button type="button" onClick={onDone}>
        Fake unset done
      </button>
    </div>
  ),
}))

import RoomsPage from './RoomsPage'
import { useAuth } from '@/context/AuthContext'
import { listRooms, AsyncJobError } from '@/api'

const OPEN = { id: 'r-off', name: 'general', type: 'channel', userCount: 7, restricted: false }
const ONDUTY = { id: 'r-on', name: 'ops', type: 'channel', userCount: 9, restricted: true }

let logout

beforeEach(() => {
  vi.clearAllMocks()
  logout = vi.fn()
  useAuth.mockReturnValue({ session: { authToken: 'tok', account: 'root', siteId: 'site-1' }, logout })
  listRooms.mockResolvedValue({ rooms: [OPEN, ONDUTY], total: 2 })
})

describe('RoomsPage', () => {
  it('lists the rooms on mount with the default paging', async () => {
    render(<RoomsPage />)
    await waitFor(() => expect(listRooms).toHaveBeenCalledWith('tok', { page: 1, limit: 20 }))
    expect(await screen.findByText('r-off')).toBeInTheDocument()
    expect(screen.getByText('r-on')).toBeInTheDocument()
  })

  it('shows the total room count', async () => {
    render(<RoomsPage />)
    expect(await screen.findByText(/2 rooms/i)).toBeInTheDocument()
  })

  it('opens the set dialog for the clicked room', async () => {
    render(<RoomsPage />)
    fireEvent.click(await screen.findByRole('button', { name: /^set onduty$/i }))
    const dialog = await screen.findByRole('dialog', { name: 'set onduty' })
    expect(dialog).toHaveTextContent('r-off')
  })

  it('opens the unset dialog for the clicked room', async () => {
    render(<RoomsPage />)
    fireEvent.click(await screen.findByRole('button', { name: /unset onduty/i }))
    const dialog = await screen.findByRole('dialog', { name: 'unset onduty' })
    expect(dialog).toHaveTextContent('r-on')
  })

  it('closes the dialog and refetches the list once a toggle lands', async () => {
    render(<RoomsPage />)
    fireEvent.click(await screen.findByRole('button', { name: /^set onduty$/i }))
    fireEvent.click(await screen.findByRole('button', { name: /fake set done/i }))
    await waitFor(() =>
      expect(screen.queryByRole('dialog', { name: 'set onduty' })).not.toBeInTheDocument(),
    )
    await waitFor(() => expect(listRooms).toHaveBeenCalledTimes(2))
  })

  it('closes the dialog without refetching when it is cancelled', async () => {
    render(<RoomsPage />)
    fireEvent.click(await screen.findByRole('button', { name: /^set onduty$/i }))
    fireEvent.click(await screen.findByRole('button', { name: /fake set close/i }))
    await waitFor(() =>
      expect(screen.queryByRole('dialog', { name: 'set onduty' })).not.toBeInTheDocument(),
    )
    expect(listRooms).toHaveBeenCalledTimes(1)
  })

  it('pages forward, keeping the requested page in the query', async () => {
    listRooms.mockResolvedValue({ rooms: [OPEN], total: 40 })
    render(<RoomsPage />)
    await waitFor(() => expect(listRooms).toHaveBeenCalledTimes(1))
    fireEvent.click(screen.getByRole('button', { name: /next/i }))
    await waitFor(() => expect(listRooms).toHaveBeenCalledWith('tok', { page: 2, limit: 20 }))
  })

  it('shows an error banner when the listing fails', async () => {
    listRooms.mockRejectedValue(new AsyncJobError('db offline', { code: 'internal' }))
    render(<RoomsPage />)
    expect(await screen.findByText(/db offline/i)).toBeInTheDocument()
  })

  it('shows a not-authorized notice instead of the table on not_admin', async () => {
    listRooms.mockRejectedValue(new AsyncJobError('nope', { code: 'forbidden', reason: 'not_admin' }))
    render(<RoomsPage />)
    expect(await screen.findByText(/not authorized/i)).toBeInTheDocument()
    expect(screen.queryByRole('table')).not.toBeInTheDocument()
  })

  it('logs the admin out when the session token is rejected', async () => {
    listRooms.mockRejectedValue(new AsyncJobError('bad token', { code: 'unauthenticated', reason: 'invalid_token' }))
    render(<RoomsPage />)
    await waitFor(() => expect(logout).toHaveBeenCalled())
  })
})
