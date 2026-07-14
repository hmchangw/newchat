import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'

vi.mock('@/api', async (importOriginal) => {
  const actual = await importOriginal()
  return { ...actual, listSessions: vi.fn(), revokeSession: vi.fn(), revokeAllSessions: vi.fn() }
})
vi.mock('@/context/AuthContext', () => ({ useAuth: vi.fn() }))

import SessionsDialog from './SessionsDialog'
import { listSessions, revokeSession, revokeAllSessions, AsyncJobError } from '@/api'
import { useAuth } from '@/context/AuthContext'

const USER = {
  id: 'u-1',
  account: 'alice',
  siteId: 'site-1',
  engName: '',
  chineseName: '',
  roles: [],
  deactivated: false,
  requirePasswordChange: false,
}

const SESSIONS = [
  { id: 's-1', userId: 'u-1', siteId: 'site-1', issuedAt: 1700000000000 },
  { id: 's-2', userId: 'u-1', siteId: 'site-1', issuedAt: 1700000100000 },
]

let logout

beforeEach(() => {
  vi.clearAllMocks()
  listSessions.mockResolvedValue(SESSIONS)
  revokeSession.mockResolvedValue(undefined)
  revokeAllSessions.mockResolvedValue(undefined)
  logout = vi.fn()
  useAuth.mockReturnValue({ logout })
})

describe('SessionsDialog', () => {
  it('lists sessions with a formatted issuedAt date', async () => {
    render(<SessionsDialog authToken="tok" user={USER} onClose={vi.fn()} />)
    await waitFor(() => expect(listSessions).toHaveBeenCalledWith('tok', 'alice'))
    expect(await screen.findAllByRole('listitem')).toHaveLength(2)
    expect(screen.getByText(new Date(SESSIONS[0].issuedAt).toLocaleString())).toBeInTheDocument()
  })

  it('revokes a single session and refreshes the list', async () => {
    render(<SessionsDialog authToken="tok" user={USER} onClose={vi.fn()} />)
    await screen.findAllByRole('listitem')
    listSessions.mockResolvedValue([SESSIONS[1]])
    fireEvent.click(screen.getAllByRole('button', { name: /^revoke$/i })[0])
    await waitFor(() => expect(revokeSession).toHaveBeenCalledWith('tok', 'alice', 's-1'))
    await waitFor(() => expect(listSessions).toHaveBeenCalledTimes(2))
  })

  it('revokes all sessions and refreshes the list', async () => {
    render(<SessionsDialog authToken="tok" user={USER} onClose={vi.fn()} />)
    await screen.findAllByRole('listitem')
    listSessions.mockResolvedValue([])
    fireEvent.click(screen.getByRole('button', { name: /revoke all/i }))
    await waitFor(() => expect(revokeAllSessions).toHaveBeenCalledWith('tok', 'alice'))
    await waitFor(() => expect(listSessions).toHaveBeenCalledTimes(2))
  })

  it('logs the admin out instead of showing a banner when the initial fetch gets invalid_token', async () => {
    listSessions.mockRejectedValue(
      new AsyncJobError('expired', { code: 'unauthenticated', reason: 'invalid_token' }),
    )
    render(<SessionsDialog authToken="tok" user={USER} onClose={vi.fn()} />)
    await waitFor(() => expect(logout).toHaveBeenCalledTimes(1))
    expect(screen.queryByText(/expired/i)).not.toBeInTheDocument()
  })
})
