import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'

vi.mock('@/api', async (importOriginal) => {
  const actual = await importOriginal()
  return { ...actual, resyncUser: vi.fn() }
})
vi.mock('@/context/AuthContext', () => ({ useAuth: vi.fn() }))

import ResyncUserDialog from './ResyncUserDialog'
import { resyncUser } from '@/api'
import { useAuth } from '@/context/AuthContext'

const USER = { id: 'u-1', account: 'alice', siteId: 'site-1' }

beforeEach(() => {
  vi.clearAllMocks()
  useAuth.mockReturnValue({ logout: vi.fn() })
  resyncUser.mockResolvedValue({ syncFailures: [], hrSyncFailed: false })
})

describe('ResyncUserDialog', () => {
  it('asks for confirmation first — nothing fires until Resync is confirmed', () => {
    render(<ResyncUserDialog authToken="tok" user={USER} onClose={vi.fn()} />)
    expect(screen.getByRole('heading', { name: /resync alice/i })).toBeInTheDocument()
    expect(resyncUser).not.toHaveBeenCalled()
  })

  it('Cancel closes without calling the API', () => {
    const onClose = vi.fn()
    render(<ResyncUserDialog authToken="tok" user={USER} onClose={onClose} />)
    fireEvent.click(screen.getByRole('button', { name: /cancel/i }))
    expect(onClose).toHaveBeenCalled()
    expect(resyncUser).not.toHaveBeenCalled()
  })

  it('confirm calls resyncUser and closes silently on full success', async () => {
    const onClose = vi.fn()
    render(<ResyncUserDialog authToken="tok" user={USER} onClose={onClose} />)
    fireEvent.click(screen.getByRole('button', { name: /^resync$/i }))
    await waitFor(() => expect(onClose).toHaveBeenCalled())
    expect(resyncUser).toHaveBeenCalledWith('tok', 'alice')
    expect(screen.queryByRole('alert')).toBeNull()
  })

  // Either lane alone fully covers a site (spec Q6/R3) — same rule as the
  // create notice (R5): only both-lanes-missed is worth an alarm.
  it('closes silently when only the HR lane failed', async () => {
    resyncUser.mockResolvedValue({ syncFailures: [], hrSyncFailed: true })
    const onClose = vi.fn()
    render(<ResyncUserDialog authToken="tok" user={USER} onClose={onClose} />)
    fireEvent.click(screen.getByRole('button', { name: /^resync$/i }))
    await waitFor(() => expect(onClose).toHaveBeenCalled())
  })

  it('closes silently when only the direct sync missed sites', async () => {
    resyncUser.mockResolvedValue({ syncFailures: ['site-b'], hrSyncFailed: false })
    const onClose = vi.fn()
    render(<ResyncUserDialog authToken="tok" user={USER} onClose={onClose} />)
    fireEvent.click(screen.getByRole('button', { name: /^resync$/i }))
    await waitFor(() => expect(onClose).toHaveBeenCalled())
  })

  it('shows the both-lanes-failed alert and defers close to Done', async () => {
    resyncUser.mockResolvedValue({ syncFailures: ['site-b', 'site-c'], hrSyncFailed: true })
    const onClose = vi.fn()
    render(<ResyncUserDialog authToken="tok" user={USER} onClose={onClose} />)
    fireEvent.click(screen.getByRole('button', { name: /^resync$/i }))

    const alert = await screen.findByRole('alert')
    expect(alert).toHaveTextContent('site-b, site-c')
    expect(onClose).not.toHaveBeenCalled()

    fireEvent.click(screen.getByRole('button', { name: /done/i }))
    expect(onClose).toHaveBeenCalled()
  })

  it('surfaces an API error inline and stays open', async () => {
    resyncUser.mockRejectedValue(new Error('boom'))
    const onClose = vi.fn()
    render(<ResyncUserDialog authToken="tok" user={USER} onClose={onClose} />)
    fireEvent.click(screen.getByRole('button', { name: /^resync$/i }))
    await screen.findByText(/boom|failed/i)
    expect(onClose).not.toHaveBeenCalled()
  })
})
