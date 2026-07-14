import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import ChangePasswordForm from './ChangePasswordForm'

function fill(label, value) {
  fireEvent.change(screen.getByLabelText(label), { target: { value } })
}

beforeEach(() => { vi.clearAllMocks() })

describe('ChangePasswordForm', () => {
  it('submits {oldPassword, newPassword} when valid', () => {
    const onSubmit = vi.fn()
    render(<ChangePasswordForm onSubmit={onSubmit} />)
    fill(/current password/i, 'old1')
    fill(/^new password/i, 'new2')
    fill(/confirm/i, 'new2')
    fireEvent.click(screen.getByRole('button', { name: /change password/i }))
    expect(onSubmit).toHaveBeenCalledWith({ oldPassword: 'old1', newPassword: 'new2' })
  })

  it('blocks submit and shows a message when new != confirm', () => {
    const onSubmit = vi.fn()
    render(<ChangePasswordForm onSubmit={onSubmit} />)
    fill(/current password/i, 'old1')
    fill(/^new password/i, 'new2')
    fill(/confirm/i, 'nope')
    fireEvent.click(screen.getByRole('button', { name: /change password/i }))
    expect(onSubmit).not.toHaveBeenCalled()
    expect(screen.getByText(/do not match/i)).toBeInTheDocument()
  })

  it('blocks submit when the new password equals the old', () => {
    const onSubmit = vi.fn()
    render(<ChangePasswordForm onSubmit={onSubmit} />)
    fill(/current password/i, 'same')
    fill(/^new password/i, 'same')
    fill(/confirm/i, 'same')
    fireEvent.click(screen.getByRole('button', { name: /change password/i }))
    expect(onSubmit).not.toHaveBeenCalled()
    expect(screen.getByText(/must differ/i)).toBeInTheDocument()
  })

  it('renders a server error passed via the error prop', () => {
    render(<ChangePasswordForm onSubmit={vi.fn()} error="bad old pw" />)
    expect(screen.getByText('bad old pw')).toBeInTheDocument()
  })

  it('disables the button while loading', () => {
    render(<ChangePasswordForm onSubmit={vi.fn()} loading />)
    expect(screen.getByRole('button')).toBeDisabled()
  })
})
