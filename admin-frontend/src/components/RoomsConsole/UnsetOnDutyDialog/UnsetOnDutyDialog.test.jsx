import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'

vi.mock('@/context/AuthContext', () => ({ useAuth: vi.fn() }))
vi.mock('@/api', async (importOriginal) => {
  const actual = await importOriginal()
  return { ...actual, setRoomOnDuty: vi.fn() }
})

import UnsetOnDutyDialog from './UnsetOnDutyDialog'
import { useAuth } from '@/context/AuthContext'
import { setRoomOnDuty, AsyncJobError } from '@/api'

const ROOM = {
  id: 'r-1',
  name: 'general',
  type: 'channel',
  userCount: 7,
  restricted: true,
  externalAccess: true,
  onDuty: true,
}

beforeEach(() => {
  vi.clearAllMocks()
  useAuth.mockReturnValue({ logout: vi.fn() })
  setRoomOnDuty.mockResolvedValue(undefined)
})

describe('UnsetOnDutyDialog', () => {
  it('names the room and warns that external access is revoked', () => {
    render(<UnsetOnDutyDialog authToken="tok" room={ROOM} onClose={vi.fn()} onDone={vi.fn()} />)
    expect(screen.getByRole('dialog')).toHaveTextContent('general')
    expect(screen.getByRole('dialog')).toHaveTextContent(/external access/i)
  })

  it('sends onDuty:false with no owner account, then reports done', async () => {
    const onDone = vi.fn()
    render(<UnsetOnDutyDialog authToken="tok" room={ROOM} onClose={vi.fn()} onDone={onDone} />)
    fireEvent.click(screen.getByRole('button', { name: /unset onduty/i }))
    await waitFor(() =>
      expect(setRoomOnDuty).toHaveBeenCalledWith('tok', 'r-1', { onDuty: false }),
    )
    await waitFor(() => expect(onDone).toHaveBeenCalled())
  })

  it('shows the server error and does not report done when the toggle is rejected', async () => {
    setRoomOnDuty.mockRejectedValue(new AsyncJobError('room service unavailable', { code: 'unavailable' }))
    const onDone = vi.fn()
    render(<UnsetOnDutyDialog authToken="tok" room={ROOM} onClose={vi.fn()} onDone={onDone} />)
    fireEvent.click(screen.getByRole('button', { name: /unset onduty/i }))
    expect(await screen.findByText(/room service unavailable/i)).toBeInTheDocument()
    expect(onDone).not.toHaveBeenCalled()
  })

  it('closes without calling the API when cancelled', () => {
    const onClose = vi.fn()
    render(<UnsetOnDutyDialog authToken="tok" room={ROOM} onClose={onClose} onDone={vi.fn()} />)
    fireEvent.click(screen.getByRole('button', { name: /cancel/i }))
    expect(onClose).toHaveBeenCalled()
    expect(setRoomOnDuty).not.toHaveBeenCalled()
  })
})
