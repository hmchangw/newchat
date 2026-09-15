import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, within } from '@testing-library/react'

import RoomTable from './RoomTable'

const room = (over) => ({
  id: 'r-1',
  name: 'general',
  type: 'channel',
  userCount: 7,
  restricted: false,
  externalAccess: false,
  onDuty: false,
  ...over,
})

/** A room the duty toggle turned on — the server derives onDuty from both flags. */
const ondutyRoom = (over) => room({ restricted: true, externalAccess: true, onDuty: true, ...over })

/** Returns the Action cell of the row whose _id cell holds `id`. */
function actionCell(id) {
  const row = screen.getByText(id).closest('tr')
  return within(row).getAllByRole('cell').at(-1)
}

/** Returns the Status cell of the row whose _id cell holds `id`. */
function statusCell(id) {
  const row = screen.getByText(id).closest('tr')
  return within(row).getAllByRole('cell').at(-2)
}

let onSetOnDuty
let onUnsetOnDuty

beforeEach(() => {
  vi.clearAllMocks()
  onSetOnDuty = vi.fn()
  onUnsetOnDuty = vi.fn()
})

/** Renders the table with the default member floor; pass `minMembers` to vary it. */
function renderTable(rooms, { loading = false, minMembers = 5 } = {}) {
  return render(
    <RoomTable
      rooms={rooms}
      loading={loading}
      minMembers={minMembers}
      onSetOnDuty={onSetOnDuty}
      onUnsetOnDuty={onUnsetOnDuty}
    />,
  )
}

describe('RoomTable', () => {
  it('renders _id, name, type, members, status and action columns', () => {
    renderTable([room()])
    for (const header of ['_id', 'Name', 'Type', 'Members', 'Status', 'Action']) {
      expect(screen.getByRole('columnheader', { name: header })).toBeInTheDocument()
    }
    const cells = within(screen.getByText('r-1').closest('tr')).getAllByRole('cell')
    expect(cells[0]).toHaveTextContent('r-1')
    expect(cells[1]).toHaveTextContent('general')
    expect(cells[2]).toHaveTextContent('channel')
    expect(cells[3]).toHaveTextContent('7')
  })

  it('shows "onduty" as the status of an on-duty room and nothing otherwise', () => {
    renderTable([ondutyRoom({ id: 'r-on' }), room({ id: 'r-off' })])
    expect(statusCell('r-on')).toHaveTextContent('onduty')
    // toHaveTextContent('') matches anything, so assert genuine emptiness.
    expect(statusCell('r-off')).toBeEmptyDOMElement()
  })

  it('offers "set onduty" for an unrestricted channel at or above the member floor', () => {
    renderTable([room({ userCount: 5 })])
    expect(within(actionCell('r-1')).getByRole('button', { name: /set onduty/i })).toBeInTheDocument()
  })

  it('offers "unset onduty" for an on-duty channel regardless of the member floor', () => {
    renderTable([ondutyRoom({ userCount: 2 })])
    const cell = actionCell('r-1')
    expect(within(cell).getByRole('button', { name: /unset onduty/i })).toBeInTheDocument()
    expect(within(cell).queryByRole('button', { name: /^set onduty/i })).not.toBeInTheDocument()
  })

  it('offers no action for an unrestricted channel below the member floor', () => {
    renderTable([room({ userCount: 4 })])
    expect(within(actionCell('r-1')).queryByRole('button')).not.toBeInTheDocument()
  })

  it('offers no action for a DM, on duty or not — room-service refuses to restrict one', () => {
    renderTable([
          room({ id: 'r-dm', type: 'dm', userCount: 9 }),
          ondutyRoom({ id: 'r-dm-on', type: 'dm', userCount: 9 }),
        ])
    expect(within(actionCell('r-dm')).queryByRole('button')).not.toBeInTheDocument()
    expect(within(actionCell('r-dm-on')).queryByRole('button')).not.toBeInTheDocument()
  })

  it('honours a deployment that raised the member floor', () => {
    renderTable([room({ userCount: 7 })], { minMembers: 10 })
    expect(within(actionCell('r-1')).queryByRole('button')).not.toBeInTheDocument()
  })

  it('passes the room up when an action is clicked', () => {
    const rooms = [room({ id: 'r-off' }), ondutyRoom({ id: 'r-on' })]
    renderTable(rooms)
    within(actionCell('r-off')).getByRole('button', { name: /set onduty/i }).click()
    expect(onSetOnDuty).toHaveBeenCalledWith(rooms[0])
    within(actionCell('r-on')).getByRole('button', { name: /unset onduty/i }).click()
    expect(onUnsetOnDuty).toHaveBeenCalledWith(rooms[1])
  })

  it('shows a loading placeholder instead of the table while loading', () => {
    renderTable([], { loading: true })
    expect(screen.getByText(/loading/i)).toBeInTheDocument()
    expect(screen.queryByRole('table')).not.toBeInTheDocument()
  })

  it('shows an empty-state message when the site has no rooms', () => {
    renderTable([])
    expect(screen.getByText(/no rooms found/i)).toBeInTheDocument()
  })
})
