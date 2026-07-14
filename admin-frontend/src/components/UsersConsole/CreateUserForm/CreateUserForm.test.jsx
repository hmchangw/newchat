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
    createUser.mockResolvedValue({ id: 'u-1' })
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
})
