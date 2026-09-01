import { describe, it, expect, vi, beforeEach } from 'vitest'
import { act, render, screen, fireEvent, waitFor } from '@testing-library/react'

vi.mock('@/context/AuthContext', () => ({ useAuth: vi.fn() }))
vi.mock('@/api', async (importOriginal) => {
  const actual = await importOriginal()
  return { ...actual, listRoomMembers: vi.fn(), setRoomOnDuty: vi.fn() }
})

import SetOnDutyDialog from './SetOnDutyDialog'
import { useAuth } from '@/context/AuthContext'
import { listRoomMembers, setRoomOnDuty, AsyncJobError } from '@/api'

const ROOM = {
  id: 'r-1',
  name: 'general',
  type: 'channel',
  userCount: 7,
  restricted: false,
  externalAccess: false,
  onDuty: false,
}

beforeEach(() => {
  vi.clearAllMocks()
  useAuth.mockReturnValue({ logout: vi.fn() })
  listRoomMembers.mockResolvedValue([
    { account: 'alice', isBot: false },
    { account: 'bob', isBot: false },
  ])
  setRoomOnDuty.mockResolvedValue(undefined)
})

async function typeOwner(text) {
  fireEvent.change(screen.getByLabelText(/owner/i), { target: { value: text } })
  await act(async () => {
    await vi.advanceTimersByTimeAsync(300)
  })
}

/** Types a query and clicks the matching option, owning the fake-timer swap the
 * debounce needs — four tests repeated this dance verbatim. */
async function pickOwner(query, name) {
  vi.useFakeTimers({ shouldAdvanceTime: true })
  try {
    await typeOwner(query)
    fireEvent.click(await screen.findByRole('option', { name }))
  } finally {
    vi.useRealTimers()
  }
}

describe('SetOnDutyDialog', () => {
  it('loads the room members on open', async () => {
    render(<SetOnDutyDialog authToken="tok" room={ROOM} onClose={vi.fn()} onDone={vi.fn()} />)
    await waitFor(() => expect(listRoomMembers).toHaveBeenCalledWith('tok', 'r-1'))
  })

  it('offers only the room members as owner candidates', async () => {
    render(<SetOnDutyDialog authToken="tok" room={ROOM} onClose={vi.fn()} onDone={vi.fn()} />)
    await waitFor(() => expect(listRoomMembers).toHaveBeenCalled())
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      await typeOwner('b')
      expect(await screen.findByRole('option', { name: /bob/i })).toBeInTheDocument()
      expect(screen.queryByRole('option', { name: /alice/i })).not.toBeInTheDocument()
    } finally {
      vi.useRealTimers()
    }
  })

  it('keeps confirm disabled until an owner is chosen', async () => {
    render(<SetOnDutyDialog authToken="tok" room={ROOM} onClose={vi.fn()} onDone={vi.fn()} />)
    await waitFor(() => expect(listRoomMembers).toHaveBeenCalled())
    expect(screen.getByRole('button', { name: /set onduty/i })).toBeDisabled()
  })

  it('sets duty on with the chosen owner, then reports done', async () => {
    const onDone = vi.fn()
    render(<SetOnDutyDialog authToken="tok" room={ROOM} onClose={vi.fn()} onDone={onDone} />)
    await waitFor(() => expect(listRoomMembers).toHaveBeenCalled())
    await pickOwner('ali', /alice/i)
    fireEvent.click(screen.getByRole('button', { name: /set onduty/i }))
    await waitFor(() =>
      expect(setRoomOnDuty).toHaveBeenCalledWith('tok', 'r-1', {
        onDuty: true,
        ownerAccount: 'alice',
      }),
    )
    await waitFor(() => expect(onDone).toHaveBeenCalled())
  })

  it('shows the server error and does not report done when the toggle is rejected', async () => {
    setRoomOnDuty.mockRejectedValue(
      new AsyncJobError('room has fewer members than required', { code: 'conflict' }),
    )
    const onDone = vi.fn()
    render(<SetOnDutyDialog authToken="tok" room={ROOM} onClose={vi.fn()} onDone={onDone} />)
    await waitFor(() => expect(listRoomMembers).toHaveBeenCalled())
    await pickOwner('ali', /alice/i)
    fireEvent.click(screen.getByRole('button', { name: /set onduty/i }))
    expect(await screen.findByText(/fewer members/i)).toBeInTheDocument()
    expect(onDone).not.toHaveBeenCalled()
  })

  it('surfaces a failure to load the members instead of offering a broken picker', async () => {
    listRoomMembers.mockRejectedValue(new AsyncJobError('db offline', { code: 'internal' }))
    render(<SetOnDutyDialog authToken="tok" room={ROOM} onClose={vi.fn()} onDone={vi.fn()} />)
    expect(await screen.findByText(/db offline/i)).toBeInTheDocument()
  })

  it('accepts exactly one owner — picking one removes the field that could add a second', async () => {
    render(<SetOnDutyDialog authToken="tok" room={ROOM} onClose={vi.fn()} onDone={vi.fn()} />)
    await waitFor(() => expect(listRoomMembers).toHaveBeenCalled())
    await pickOwner('ali', /alice/i)
    // No field remains through which a second account could be added.
    expect(screen.queryByLabelText(/owner/i)).not.toBeInTheDocument()
    expect(screen.getByText('alice')).toBeInTheDocument()
  })

  it('closes without calling the API when cancelled', async () => {
    const onClose = vi.fn()
    render(<SetOnDutyDialog authToken="tok" room={ROOM} onClose={onClose} onDone={vi.fn()} />)
    await waitFor(() => expect(listRoomMembers).toHaveBeenCalled())
    fireEvent.click(screen.getByRole('button', { name: /cancel/i }))
    expect(onClose).toHaveBeenCalled()
    expect(setRoomOnDuty).not.toHaveBeenCalled()
  })
})
