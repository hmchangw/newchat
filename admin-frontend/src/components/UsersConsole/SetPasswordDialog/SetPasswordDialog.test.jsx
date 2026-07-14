import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'

vi.mock('@/api', async (importOriginal) => {
  const actual = await importOriginal()
  return { ...actual, setPassword: vi.fn() }
})
vi.mock('@/context/AuthContext', () => ({ useAuth: vi.fn() }))

import SetPasswordDialog from './SetPasswordDialog'
import { setPassword, AsyncJobError } from '@/api'
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

let logout

beforeEach(() => {
  vi.clearAllMocks()
  logout = vi.fn()
  useAuth.mockReturnValue({ logout })
})

describe('SetPasswordDialog', () => {
  it('blocks submit when fields are empty', () => {
    render(<SetPasswordDialog authToken="tok" user={USER} onClose={vi.fn()} onUpdated={vi.fn()} />)
    fireEvent.click(screen.getByRole('button', { name: /set password/i }))
    expect(setPassword).not.toHaveBeenCalled()
  })

  it('blocks submit when the passwords do not match', () => {
    render(<SetPasswordDialog authToken="tok" user={USER} onClose={vi.fn()} onUpdated={vi.fn()} />)
    fireEvent.change(screen.getByLabelText(/^new password/i), { target: { value: 'new1' } })
    fireEvent.change(screen.getByLabelText(/confirm/i), { target: { value: 'new2' } })
    fireEvent.click(screen.getByRole('button', { name: /set password/i }))
    expect(setPassword).not.toHaveBeenCalled()
    expect(screen.getByText(/do not match/i)).toBeInTheDocument()
  })

  it('calls setPassword mapping the "force change" checkbox to requirePasswordChange, then reports onUpdated', async () => {
    setPassword.mockResolvedValue(undefined)
    const onUpdated = vi.fn()
    render(
      <SetPasswordDialog authToken="tok" user={USER} onClose={vi.fn()} onUpdated={onUpdated} />,
    )
    fireEvent.change(screen.getByLabelText(/^new password/i), { target: { value: 'new1' } })
    fireEvent.change(screen.getByLabelText(/confirm/i), { target: { value: 'new1' } })
    fireEvent.click(screen.getByLabelText(/force change/i))
    fireEvent.click(screen.getByRole('button', { name: /set password/i }))
    await waitFor(() =>
      expect(setPassword).toHaveBeenCalledWith('tok', 'alice', {
        newPassword: 'new1',
        requirePasswordChange: true,
      }),
    )
    await waitFor(() => expect(onUpdated).toHaveBeenCalled())
  })

  it('logs the admin out instead of showing a banner on invalid_token', async () => {
    setPassword.mockRejectedValue(
      new AsyncJobError('expired', { code: 'unauthenticated', reason: 'invalid_token' }),
    )
    render(<SetPasswordDialog authToken="tok" user={USER} onClose={vi.fn()} onUpdated={vi.fn()} />)
    fireEvent.change(screen.getByLabelText(/^new password/i), { target: { value: 'new1' } })
    fireEvent.change(screen.getByLabelText(/confirm/i), { target: { value: 'new1' } })
    fireEvent.click(screen.getByRole('button', { name: /set password/i }))
    await waitFor(() => expect(logout).toHaveBeenCalledTimes(1))
    expect(screen.queryByText(/expired/i)).not.toBeInTheDocument()
  })
})
