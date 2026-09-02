import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor, within } from '@testing-library/react'

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
    { account: 'helperbot', isBot: true },
  ])
  setRoomOnDuty.mockResolvedValue(undefined)
})

function renderDialog({ onClose = vi.fn(), onDone = vi.fn() } = {}) {
  render(<SetOnDutyDialog authToken="tok" room={ROOM} onClose={onClose} onDone={onDone} />)
}

/** Waits for the roster to land, then returns the owner radio for `account`. */
async function ownerRadio(account) {
  return screen.findByRole('radio', { name: new RegExp(account) })
}

describe('SetOnDutyDialog', () => {
  it('loads the room members on open', async () => {
    renderDialog()
    await waitFor(() => expect(listRoomMembers).toHaveBeenCalledWith('tok', 'r-1'))
  })

  it('lists every member as a radio option, with no typing required', async () => {
    renderDialog()
    const group = await screen.findByRole('radiogroup', { name: /owner/i })
    const radios = within(group).getAllByRole('radio')
    expect(radios).toHaveLength(3)
    expect(radios.map((r) => r.value)).toEqual(['alice', 'bob', 'helperbot'])
  })

  it('marks a bot member so an admin does not hand ownership to one unknowingly', async () => {
    renderDialog()
    expect(await screen.findByRole('radio', { name: /helperbot.*bot/i })).toBeInTheDocument()
  })

  it('starts with no owner selected', async () => {
    renderDialog()
    const group = await screen.findByRole('radiogroup', { name: /owner/i })
    for (const radio of within(group).getAllByRole('radio')) {
      expect(radio).not.toBeChecked()
    }
  })

  it('keeps confirm disabled until an owner is chosen', async () => {
    renderDialog()
    await waitFor(() => expect(listRoomMembers).toHaveBeenCalled())
    expect(screen.getByRole('button', { name: /set onduty/i })).toBeDisabled()

    fireEvent.click(await ownerRadio('alice'))
    expect(screen.getByRole('button', { name: /set onduty/i })).toBeEnabled()
  })

  it('selecting a second member replaces the first — never two owners', async () => {
    renderDialog()
    fireEvent.click(await ownerRadio('alice'))
    fireEvent.click(await ownerRadio('bob'))

    expect(await ownerRadio('alice')).not.toBeChecked()
    expect(await ownerRadio('bob')).toBeChecked()

    fireEvent.click(screen.getByRole('button', { name: /set onduty/i }))
    // ownerAccount is a single string on the wire — there is no shape carrying two.
    await waitFor(() =>
      expect(setRoomOnDuty).toHaveBeenCalledWith('tok', 'r-1', {
        onDuty: true,
        ownerAccount: 'bob',
      }),
    )
  })

  it('sets duty on with the chosen owner, then reports done', async () => {
    const onDone = vi.fn()
    renderDialog({ onDone })
    fireEvent.click(await ownerRadio('alice'))
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
    renderDialog({ onDone })
    fireEvent.click(await ownerRadio('alice'))
    fireEvent.click(screen.getByRole('button', { name: /set onduty/i }))

    expect(await screen.findByText(/fewer members/i)).toBeInTheDocument()
    expect(onDone).not.toHaveBeenCalled()
  })

  it('surfaces a failure to load the members instead of offering an empty list', async () => {
    listRoomMembers.mockRejectedValue(new AsyncJobError('db offline', { code: 'internal' }))
    renderDialog()
    expect(await screen.findByText(/db offline/i)).toBeInTheDocument()
    expect(screen.queryByRole('radio')).not.toBeInTheDocument()
  })

  it('says so when the room has no members to promote', async () => {
    listRoomMembers.mockResolvedValue([])
    renderDialog()
    expect(await screen.findByText(/no members/i)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /set onduty/i })).toBeDisabled()
  })

  it('closes without calling the API when cancelled', async () => {
    const onClose = vi.fn()
    renderDialog({ onClose })
    await waitFor(() => expect(listRoomMembers).toHaveBeenCalled())
    fireEvent.click(screen.getByRole('button', { name: /cancel/i }))
    expect(onClose).toHaveBeenCalled()
    expect(setRoomOnDuty).not.toHaveBeenCalled()
  })
})
