import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, within } from '@testing-library/react'

vi.mock('@/lib/runtimeConfig', async (importOriginal) => {
  const actual = await importOriginal()
  return { ...actual, ondutyMinMembers: vi.fn() }
})

import RoomTable from './RoomTable'
import { ondutyMinMembers } from '@/lib/runtimeConfig'

const room = (over) => ({
  id: 'r-1',
  name: 'general',
  type: 'channel',
  userCount: 7,
  restricted: false,
  externalAccess: false,
  ...over,
})

/** A room the duty toggle turned on — it writes both flags together. */
const ondutyRoom = (over) => room({ restricted: true, externalAccess: true, ...over })

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

beforeEach(() => {
  vi.clearAllMocks()
  ondutyMinMembers.mockReturnValue(5)
})

describe('RoomTable', () => {
  it('renders _id, name, type, members, status and action columns', () => {
    render(<RoomTable rooms={[room()]} loading={false} onSetOnDuty={vi.fn()} onUnsetOnDuty={vi.fn()} />)
    for (const header of ['_id', 'Name', 'Type', 'Members', 'Status', 'Action']) {
      expect(screen.getByRole('columnheader', { name: header })).toBeInTheDocument()
    }
    const cells = within(screen.getByText('r-1').closest('tr')).getAllByRole('cell')
    expect(cells[0]).toHaveTextContent('r-1')
    expect(cells[1]).toHaveTextContent('general')
    expect(cells[2]).toHaveTextContent('channel')
    expect(cells[3]).toHaveTextContent('7')
  })

  it('shows "onduty" only when both duty flags are set', () => {
    render(
      <RoomTable
        rooms={[
          ondutyRoom({ id: 'r-on' }),
          room({ id: 'r-off' }),
          room({ id: 'r-restricted-only', restricted: true }),
          room({ id: 'r-external-only', externalAccess: true }),
        ]}
        loading={false}
        onSetOnDuty={vi.fn()}
        onUnsetOnDuty={vi.fn()}
      />,
    )
    expect(statusCell('r-on')).toHaveTextContent('onduty')
    expect(statusCell('r-off')).toHaveTextContent('')
    // Half-set flags are not on duty — the toggle only ever writes both together.
    expect(statusCell('r-restricted-only')).toHaveTextContent('')
    expect(statusCell('r-external-only')).toHaveTextContent('')
  })

  it('offers "set onduty" for an unrestricted channel at or above the member floor', () => {
    render(
      <RoomTable
        rooms={[room({ userCount: 5 })]}
        loading={false}
        onSetOnDuty={vi.fn()}
        onUnsetOnDuty={vi.fn()}
      />,
    )
    expect(within(actionCell('r-1')).getByRole('button', { name: /set onduty/i })).toBeInTheDocument()
  })

  it('offers "unset onduty" for an on-duty channel regardless of the member floor', () => {
    render(
      <RoomTable
        rooms={[ondutyRoom({ userCount: 2 })]}
        loading={false}
        onSetOnDuty={vi.fn()}
        onUnsetOnDuty={vi.fn()}
      />,
    )
    const cell = actionCell('r-1')
    expect(within(cell).getByRole('button', { name: /unset onduty/i })).toBeInTheDocument()
    expect(within(cell).queryByRole('button', { name: /^set onduty/i })).not.toBeInTheDocument()
  })

  it('offers "set onduty" for a half-set channel, so it can be brought fully on duty', () => {
    render(
      <RoomTable
        rooms={[room({ restricted: true })]}
        loading={false}
        onSetOnDuty={vi.fn()}
        onUnsetOnDuty={vi.fn()}
      />,
    )
    const cell = actionCell('r-1')
    expect(within(cell).getByRole('button', { name: /^set onduty/i })).toBeInTheDocument()
    expect(within(cell).queryByRole('button', { name: /unset onduty/i })).not.toBeInTheDocument()
  })

  it('offers no action for an on-duty DM', () => {
    render(
      <RoomTable
        rooms={[ondutyRoom({ type: 'dm', userCount: 9 })]}
        loading={false}
        onSetOnDuty={vi.fn()}
        onUnsetOnDuty={vi.fn()}
      />,
    )
    expect(within(actionCell('r-1')).queryByRole('button')).not.toBeInTheDocument()
  })

  it('offers no action for an unrestricted channel below the member floor', () => {
    render(
      <RoomTable
        rooms={[room({ userCount: 4 })]}
        loading={false}
        onSetOnDuty={vi.fn()}
        onUnsetOnDuty={vi.fn()}
      />,
    )
    expect(within(actionCell('r-1')).queryByRole('button')).not.toBeInTheDocument()
  })

  it('offers no action for a DM, which room-service refuses to restrict', () => {
    render(
      <RoomTable
        rooms={[room({ type: 'dm', userCount: 9 })]}
        loading={false}
        onSetOnDuty={vi.fn()}
        onUnsetOnDuty={vi.fn()}
      />,
    )
    expect(within(actionCell('r-1')).queryByRole('button')).not.toBeInTheDocument()
  })

  it('honours a deployment that raised the member floor', () => {
    ondutyMinMembers.mockReturnValue(10)
    render(
      <RoomTable
        rooms={[room({ userCount: 7 })]}
        loading={false}
        onSetOnDuty={vi.fn()}
        onUnsetOnDuty={vi.fn()}
      />,
    )
    expect(within(actionCell('r-1')).queryByRole('button')).not.toBeInTheDocument()
  })

  it('passes the room up when an action is clicked', () => {
    const onSetOnDuty = vi.fn()
    const onUnsetOnDuty = vi.fn()
    const rooms = [room({ id: 'r-off' }), ondutyRoom({ id: 'r-on' })]
    render(
      <RoomTable rooms={rooms} loading={false} onSetOnDuty={onSetOnDuty} onUnsetOnDuty={onUnsetOnDuty} />,
    )
    within(actionCell('r-off')).getByRole('button', { name: /set onduty/i }).click()
    expect(onSetOnDuty).toHaveBeenCalledWith(rooms[0])
    within(actionCell('r-on')).getByRole('button', { name: /unset onduty/i }).click()
    expect(onUnsetOnDuty).toHaveBeenCalledWith(rooms[1])
  })

  it('shows a loading placeholder instead of the table while loading', () => {
    render(<RoomTable rooms={[]} loading onSetOnDuty={vi.fn()} onUnsetOnDuty={vi.fn()} />)
    expect(screen.getByText(/loading/i)).toBeInTheDocument()
    expect(screen.queryByRole('table')).not.toBeInTheDocument()
  })

  it('shows an empty-state message when the site has no rooms', () => {
    render(<RoomTable rooms={[]} loading={false} onSetOnDuty={vi.fn()} onUnsetOnDuty={vi.fn()} />)
    expect(screen.getByText(/no rooms found/i)).toBeInTheDocument()
  })
})
