import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'

vi.mock('@/api', async (importOriginal) => {
  const actual = await importOriginal()
  return { ...actual, createUser: vi.fn() }
})
vi.mock('@/context/AuthContext', () => ({ useAuth: vi.fn() }))

import CreateUserForm from './CreateUserForm'
import { createUser, AsyncJobError } from '@/api'
import { useAuth } from '@/context/AuthContext'

let logout

beforeEach(() => {
  vi.clearAllMocks()
  logout = vi.fn()
  useAuth.mockReturnValue({ logout })
})

const cleanResult = { user: { account: 'alice' }, syncFailures: [], hrSyncFailed: false }

function fillValidForm() {
  fireEvent.change(screen.getByLabelText(/^account/i), { target: { value: 'alice' } })
  fireEvent.change(screen.getByLabelText(/^password/i), { target: { value: 'hunter2' } })
  fireEvent.click(screen.getByRole('checkbox', { name: /^admin$/i }))
}

describe('CreateUserForm', () => {
  it('blocks submit when account is missing', () => {
    render(<CreateUserForm authToken="tok" onClose={vi.fn()} onCreated={vi.fn()} />)
    fireEvent.change(screen.getByLabelText(/^password/i), { target: { value: 'hunter2' } })
    fireEvent.click(screen.getByRole('checkbox', { name: /^admin$/i }))
    fireEvent.click(screen.getByRole('button', { name: /create user/i }))
    expect(createUser).not.toHaveBeenCalled()
    expect(screen.getByText(/required/i)).toBeInTheDocument()
  })

  it('blocks submit when password is missing', () => {
    render(<CreateUserForm authToken="tok" onClose={vi.fn()} onCreated={vi.fn()} />)
    fireEvent.change(screen.getByLabelText(/^account/i), { target: { value: 'alice' } })
    fireEvent.click(screen.getByRole('checkbox', { name: /^admin$/i }))
    fireEvent.click(screen.getByRole('button', { name: /create user/i }))
    expect(createUser).not.toHaveBeenCalled()
  })

  it('blocks submit when no role is selected', () => {
    render(<CreateUserForm authToken="tok" onClose={vi.fn()} onCreated={vi.fn()} />)
    fireEvent.change(screen.getByLabelText(/^account/i), { target: { value: 'alice' } })
    fireEvent.change(screen.getByLabelText(/^password/i), { target: { value: 'hunter2' } })
    fireEvent.click(screen.getByRole('button', { name: /create user/i }))
    expect(createUser).not.toHaveBeenCalled()
  })

  it('submits createUser with the form values on valid input', async () => {
    createUser.mockResolvedValue(cleanResult)
    const onCreated = vi.fn()
    render(<CreateUserForm authToken="tok" onClose={vi.fn()} onCreated={onCreated} />)
    fillValidForm()
    fireEvent.click(screen.getByRole('button', { name: /create user/i }))
    await waitFor(() =>
      expect(createUser).toHaveBeenCalledWith(
        'tok',
        expect.objectContaining({ account: 'alice', password: 'hunter2', roles: ['admin'] }),
      ),
    )
    await waitFor(() => expect(onCreated).toHaveBeenCalled())
  })

  it('shows a friendly inline error on account_exists', async () => {
    createUser.mockRejectedValue(new AsyncJobError('conflict', { code: 'conflict', reason: 'account_exists' }))
    render(<CreateUserForm authToken="tok" onClose={vi.fn()} onCreated={vi.fn()} />)
    fillValidForm()
    fireEvent.click(screen.getByRole('button', { name: /create user/i }))
    expect(await screen.findByText(/already exists/i)).toBeInTheDocument()
  })

  it('logs the admin out instead of showing a banner on invalid_token', async () => {
    createUser.mockRejectedValue(
      new AsyncJobError('expired', { code: 'unauthenticated', reason: 'invalid_token' }),
    )
    render(<CreateUserForm authToken="tok" onClose={vi.fn()} onCreated={vi.fn()} />)
    fillValidForm()
    fireEvent.click(screen.getByRole('button', { name: /create user/i }))
    await waitFor(() => expect(logout).toHaveBeenCalledTimes(1))
    expect(screen.queryByText(/expired/i)).not.toBeInTheDocument()
  })

  it('closes immediately on a clean result', async () => {
    createUser.mockResolvedValue(cleanResult)
    const onCreated = vi.fn()
    render(<CreateUserForm authToken="tok" onClose={vi.fn()} onCreated={onCreated} />)
    fillValidForm()
    fireEvent.click(screen.getByRole('button', { name: /create/i }))
    await waitFor(() => expect(onCreated).toHaveBeenCalled())
    expect(screen.queryByText(/sync/i)).toBeNull()
  })

  // Notice rule (spec R9): the INBOX snapshot is the only complete lane — the
  // durable HR feed carries identity fields only (never roles/status). So the
  // notice fires whenever the direct sync missed a site; hrSyncFailed merely
  // picks the severity (full failure vs identity-only partial sync).

  it('closes immediately when only the HR lane failed (direct sync reached every site)', async () => {
    createUser.mockResolvedValue({ user: { account: 'alice' }, syncFailures: [], hrSyncFailed: true })
    const onCreated = vi.fn()
    render(<CreateUserForm authToken="tok" onClose={vi.fn()} onCreated={onCreated} />)
    fillValidForm()
    fireEvent.click(screen.getByRole('button', { name: /create/i }))
    await waitFor(() => expect(onCreated).toHaveBeenCalled())
    expect(screen.queryByRole('alert')).toBeNull()
  })

  it('shows the identity-only partial notice when the direct sync missed sites', async () => {
    createUser.mockResolvedValue({
      user: { account: 'alice' },
      syncFailures: ['site-c'],
      hrSyncFailed: false,
    })
    const onCreated = vi.fn()
    render(<CreateUserForm authToken="tok" onClose={vi.fn()} onCreated={onCreated} />)
    fillValidForm()
    fireEvent.click(screen.getByRole('button', { name: /create/i }))
    const alert = await screen.findByRole('alert')
    expect(onCreated).not.toHaveBeenCalled()
    expect(alert).toHaveTextContent('site-c')
    expect(alert).toHaveTextContent(/only .* identity/i)
    expect(alert).toHaveTextContent(/roles and status/i)
    expect(alert).toHaveTextContent(/resync/i)
    fireEvent.click(screen.getByRole('button', { name: /done/i }))
    expect(onCreated).toHaveBeenCalled()
  })

  it('shows the both-lanes-failed notice and defers onCreated to Done', async () => {
    createUser.mockResolvedValue({
      user: { account: 'alice' },
      syncFailures: ['site-c'],
      hrSyncFailed: true,
    })
    const onCreated = vi.fn()
    render(<CreateUserForm authToken="tok" onClose={vi.fn()} onCreated={onCreated} />)
    fillValidForm()
    fireEvent.click(screen.getByRole('button', { name: /create/i }))
    await waitFor(() => expect(screen.getByText(/created on this site/i)).toBeInTheDocument())
    expect(onCreated).not.toHaveBeenCalled()
    const alert = screen.getByRole('alert')
    expect(alert).toHaveTextContent('site-c')
    expect(alert).toHaveTextContent(/did not sync/i)
    expect(alert).toHaveTextContent(/resync/i)
    fireEvent.click(screen.getByRole('button', { name: /done/i }))
    expect(onCreated).toHaveBeenCalled()
  })
})
