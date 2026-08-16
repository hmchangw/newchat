import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import ChatlistSectionDialog from './ChatlistSectionDialog'

describe('ChatlistSectionDialog', () => {
  it('submits a valid trimmed name and closes', async () => {
    const onSubmit = vi.fn().mockResolvedValue()
    const onClose = vi.fn()
    render(<ChatlistSectionDialog mode="create" onSubmit={onSubmit} onClose={onClose} />)
    fireEvent.change(screen.getByLabelText('Section name'), { target: { value: '  Work  ' } })
    fireEvent.click(screen.getByRole('button', { name: 'Create' }))
    await waitFor(() => expect(onSubmit).toHaveBeenCalledWith('Work'))
    expect(onClose).toHaveBeenCalled()
  })

  it('blocks an invalid name locally (no RPC)', () => {
    const onSubmit = vi.fn()
    render(<ChatlistSectionDialog mode="create" onSubmit={onSubmit} onClose={vi.fn()} />)
    fireEvent.change(screen.getByLabelText('Section name'), { target: { value: 'bad  name' } })
    fireEvent.click(screen.getByRole('button', { name: 'Create' }))
    expect(onSubmit).not.toHaveBeenCalled()
    expect(screen.getByText(/1–50/)).toBeInTheDocument()
  })

  it('surfaces a duplicate-name error from the RPC', async () => {
    const onSubmit = vi.fn().mockRejectedValue({ reason: 'chatlist_duplicate_name' })
    render(<ChatlistSectionDialog mode="rename" initialName="Work" onSubmit={onSubmit} onClose={vi.fn()} />)
    fireEvent.click(screen.getByRole('button', { name: 'Rename' }))
    await waitFor(() => expect(screen.getByText(/already exists/)).toBeInTheDocument())
  })
})
