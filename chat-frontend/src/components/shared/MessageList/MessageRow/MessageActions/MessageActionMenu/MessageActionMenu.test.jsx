import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor, act } from '@testing-library/react'
import MessageActionMenu from './MessageActionMenu'
// The subject builder lives in api/_transport (internal); the test
// asserts on the wire subject by hardcoding the expected string. If
// the format ever changes both the production code and this string
// need to flip together.
const READ_RECEIPT_SUBJECT = 'chat.user.alice.request.room.r1.siteA.message.read-receipt'
const MEMBER_LIST_SUBJECT = 'chat.user.alice.request.room.r1.siteA.member.list'

vi.mock('@/context/NatsContext', () => ({
  useNats: vi.fn(),
}))

import { useNats } from '@/context/NatsContext'

const room = { id: 'r1', siteId: 'siteA', userCount: 4 }

beforeEach(() => {
  useNats.mockReset()
  useNats.mockReturnValue({
    user: { account: 'alice', siteId: 'siteA' },
    request: vi.fn(),
  })
})

describe('MessageActionMenu render-gating', () => {
  it('renders the kebab button for the current user\'s own message', () => {
    const msg = { id: 'm1', sender: { account: 'alice' } }
    render(<MessageActionMenu message={msg} room={room} />)
    expect(screen.getByRole('button', { name: /Message actions/i })).toBeInTheDocument()
  })

  it('renders nothing for messages sent by other users', () => {
    const msg = { id: 'm1', sender: { account: 'bob' } }
    const { container } = render(<MessageActionMenu message={msg} room={room} />)
    expect(container).toBeEmptyDOMElement()
  })

  it('renders nothing when there is no signed-in user', () => {
    useNats.mockReturnValue({ user: null, request: vi.fn() })
    const msg = { id: 'm1', sender: { account: 'alice' } }
    const { container } = render(<MessageActionMenu message={msg} room={room} />)
    expect(container).toBeEmptyDOMElement()
  })

  it('renders nothing when room is missing or has no id', () => {
    const msg = { id: 'm1', sender: { account: 'alice' } }
    const nullRoom = render(<MessageActionMenu message={msg} room={null} />)
    expect(nullRoom.container).toBeEmptyDOMElement()
    nullRoom.unmount()
    const idless = render(<MessageActionMenu message={msg} room={{ userCount: 3 }} />)
    expect(idless.container).toBeEmptyDOMElement()
  })
})

describe('MessageActionMenu open/close', () => {
  const msg = { id: 'm1', sender: { account: 'alice' } }

  function setup({ request = vi.fn().mockReturnValue(new Promise(() => {})) } = {}) {
    useNats.mockReturnValue({
      user: { account: 'alice', siteId: 'siteA' },
      request,
    })
    return render(<MessageActionMenu message={msg} room={room} />)
  }

  it('opens the popover when the kebab is clicked', () => {
    setup()
    fireEvent.click(screen.getByRole('button', { name: /Message actions/i }))
    expect(screen.getByRole('menu')).toBeInTheDocument()
  })

  it('closes the popover when the kebab is clicked again (toggle)', () => {
    setup()
    const kebab = screen.getByRole('button', { name: /Message actions/i })
    fireEvent.click(kebab)
    fireEvent.click(kebab)
    expect(screen.queryByRole('menu')).not.toBeInTheDocument()
  })

  it('closes the popover when the user clicks outside', () => {
    setup()
    fireEvent.click(screen.getByRole('button', { name: /Message actions/i }))
    expect(screen.getByRole('menu')).toBeInTheDocument()
    fireEvent.mouseDown(document.body)
    expect(screen.queryByRole('menu')).not.toBeInTheDocument()
  })

  it('closes the popover when Escape is pressed', () => {
    setup()
    fireEvent.click(screen.getByRole('button', { name: /Message actions/i }))
    fireEvent.keyDown(document, { key: 'Escape' })
    expect(screen.queryByRole('menu')).not.toBeInTheDocument()
  })

  it('does not close when clicking inside the popover', () => {
    setup()
    fireEvent.click(screen.getByRole('button', { name: /Message actions/i }))
    fireEvent.mouseDown(screen.getByRole('menu'))
    expect(screen.getByRole('menu')).toBeInTheDocument()
  })
})

describe('MessageActionMenu read-receipt RPC', () => {
  const msg = { id: 'm1', sender: { account: 'alice' } }

  function deferred() {
    let resolve, reject
    const promise = new Promise((res, rej) => { resolve = res; reject = rej })
    return { promise, resolve, reject }
  }

  it('shows Loading… immediately after opening the menu', () => {
    const d = deferred()
    const request = vi.fn().mockReturnValue(d.promise)
    useNats.mockReturnValue({
      user: { account: 'alice', siteId: 'siteA' },
      request,
    })
    render(<MessageActionMenu message={msg} room={room} />)
    fireEvent.click(screen.getByRole('button', { name: /Message actions/i }))
    expect(screen.getByText(/Loading…/i)).toBeInTheDocument()
  })

  it('calls the RPC with the correct subject and payload', () => {
    const d = deferred()
    const request = vi.fn().mockReturnValue(d.promise)
    useNats.mockReturnValue({
      user: { account: 'alice', siteId: 'siteA' },
      request,
    })
    render(<MessageActionMenu message={msg} room={room} />)
    fireEvent.click(screen.getByRole('button', { name: /Message actions/i }))
    expect(request).toHaveBeenCalledWith(
      READ_RECEIPT_SUBJECT,
      { messageId: 'm1' }
    )
  })

  it('renders "Read by X of Y" once the RPC resolves', async () => {
    const d = deferred()
    const request = vi.fn().mockReturnValue(d.promise)
    useNats.mockReturnValue({
      user: { account: 'alice', siteId: 'siteA' },
      request,
    })
    render(<MessageActionMenu message={msg} room={room} />)
    fireEvent.click(screen.getByRole('button', { name: /Message actions/i }))
    await act(async () => {
      d.resolve({ readers: [
        { userId: 'u1', account: 'bob', engName: 'Bob', chineseName: '' },
        { userId: 'u2', account: 'carol', engName: 'Carol', chineseName: '' },
      ] })
    })
    expect(await screen.findByText('Read by 2 of 3')).toBeInTheDocument()
  })

  it('clamps Y at 0 for a single-member room', async () => {
    const d = deferred()
    const request = vi.fn().mockReturnValue(d.promise)
    useNats.mockReturnValue({
      user: { account: 'alice', siteId: 'siteA' },
      request,
    })
    render(
      <MessageActionMenu
        message={msg}
        room={{ id: 'r1', siteId: 'siteA', userCount: 1 }}
      />
    )
    fireEvent.click(screen.getByRole('button', { name: /Message actions/i }))
    await act(async () => { d.resolve({ readers: [] }) })
    expect(await screen.findByText('Read by 0 of 0')).toBeInTheDocument()
  })

  it('renders the RPC error message inline', async () => {
    const d = deferred()
    const request = vi.fn().mockReturnValue(d.promise)
    useNats.mockReturnValue({
      user: { account: 'alice', siteId: 'siteA' },
      request,
    })
    render(<MessageActionMenu message={msg} room={room} />)
    fireEvent.click(screen.getByRole('button', { name: /Message actions/i }))
    await act(async () => { d.reject(new Error('only the message sender can view read receipts')) })
    expect(
      await screen.findByText(/only the message sender can view read receipts/i)
    ).toBeInTheDocument()
  })

  it('refetches the RPC every time the menu is reopened', async () => {
    // The kebab fires two RPCs per open (read-receipt + member.list); route
    // by subject so a sequence-of-once mock isn't fragile to call order.
    let receiptCalls = 0
    const request = vi.fn((subj) => {
      if (subj.includes('read-receipt')) {
        receiptCalls++
        return receiptCalls === 1
          ? Promise.resolve({ readers: [] })
          : Promise.resolve({ readers: [{ userId: 'u1', account: 'bob', engName: 'Bob', chineseName: '' }] })
      }
      if (subj.includes('member.list')) {
        return Promise.resolve({ members: [
          { id: 'a', rid: 'r1', ts: '', member: { type: 'individual', id: 'u0', account: 'alice' } },
          { id: 'b', rid: 'r1', ts: '', member: { type: 'individual', id: 'u1', account: 'bob' } },
          { id: 'c', rid: 'r1', ts: '', member: { type: 'individual', id: 'u2', account: 'carol' } },
          { id: 'd', rid: 'r1', ts: '', member: { type: 'individual', id: 'u3', account: 'dave' } },
        ] })
      }
      return Promise.reject(new Error('unexpected subject: ' + subj))
    })
    useNats.mockReturnValue({
      user: { account: 'alice', siteId: 'siteA' },
      request,
    })
    render(<MessageActionMenu message={msg} room={room} />)
    const kebab = screen.getByRole('button', { name: /Message actions/i })

    fireEvent.click(kebab)
    await waitFor(() => expect(screen.getByText('Read by 0 of 3')).toBeInTheDocument())
    fireEvent.click(kebab) // close
    fireEvent.click(kebab) // reopen
    await waitFor(() => expect(screen.getByText('Read by 1 of 3')).toBeInTheDocument())
    expect(receiptCalls).toBe(2)
  })

  it('uses message.messageId when message.id is absent (history-loaded shape)', () => {
    const d = deferred()
    const request = vi.fn().mockReturnValue(d.promise)
    useNats.mockReturnValue({
      user: { account: 'alice', siteId: 'siteA' },
      request,
    })
    const historyMsg = { messageId: 'h1', sender: { account: 'alice' } }
    render(<MessageActionMenu message={historyMsg} room={room} />)
    fireEvent.click(screen.getByRole('button', { name: /Message actions/i }))
    expect(request).toHaveBeenCalledWith(
      READ_RECEIPT_SUBJECT,
      { messageId: 'h1' }
    )
  })

  it('falls back to user.siteId when room.siteId is missing', () => {
    const d = deferred()
    const request = vi.fn().mockReturnValue(d.promise)
    useNats.mockReturnValue({
      user: { account: 'alice', siteId: 'siteA' },
      request,
    })
    render(
      <MessageActionMenu
        message={msg}
        room={{ id: 'r1', userCount: 3 }}
      />
    )
    fireEvent.click(screen.getByRole('button', { name: /Message actions/i }))
    expect(request).toHaveBeenCalledWith(
      READ_RECEIPT_SUBJECT,
      { messageId: 'm1' }
    )
  })
})

describe('MessageActionMenu recipient count (Y) sourcing', () => {
  const msg = { id: 'm1', sender: { account: 'alice' } }

  function mkRequest({ readers, members, memberListError }) {
    return vi.fn((subj) => {
      if (subj.includes('read-receipt')) return Promise.resolve({ readers })
      if (subj.includes('member.list')) {
        if (memberListError) return Promise.reject(memberListError)
        return Promise.resolve({ members })
      }
      return Promise.reject(new Error('unexpected subject: ' + subj))
    })
  }

  it('derives Y from member.list (members - 1) even when room.userCount is stale', async () => {
    // Regression: after Alice logs out and back in, the room summary's
    // userCount is hydrated from a Subscription record that doesn't carry
    // the field, so it collapses to 0. The kebab must still show the right
    // denominator by fetching member.list itself.
    const members = ['alice', 'bob', 'carol', 'dave', 'eve'].map((account, i) => ({
      id: `m${i}`, rid: 'r1', ts: '', member: { type: 'individual', id: `u${i}`, account },
    }))
    const request = mkRequest({
      readers: [
        { userId: 'u1', account: 'bob', engName: 'Bob' },
        { userId: 'u2', account: 'carol', engName: 'Carol' },
      ],
      members,
    })
    useNats.mockReturnValue({ user: { account: 'alice', siteId: 'siteA' }, request })
    render(
      <MessageActionMenu
        message={msg}
        room={{ id: 'r1', siteId: 'siteA', userCount: 0 }}
      />,
    )
    fireEvent.click(screen.getByRole('button', { name: /Message actions/i }))
    expect(await screen.findByText('Read by 2 of 4')).toBeInTheDocument()
  })

  it('calls member.list on the same room/site as the read-receipt RPC', async () => {
    const request = mkRequest({ readers: [], members: [] })
    useNats.mockReturnValue({ user: { account: 'alice', siteId: 'siteA' }, request })
    render(
      <MessageActionMenu
        message={msg}
        room={{ id: 'r1', siteId: 'siteA', userCount: 4 }}
      />,
    )
    fireEvent.click(screen.getByRole('button', { name: /Message actions/i }))
    expect(request).toHaveBeenCalledWith(MEMBER_LIST_SUBJECT, {})
  })

  it('falls back to room.userCount - 1 when member.list rejects', async () => {
    // listRoomMembers failure is non-blocking: Y degrades to the prior
    // behavior so users with a valid in-session userCount still see a
    // sensible denominator, and the X side of the menu still renders.
    const request = mkRequest({
      readers: [{ userId: 'u1', account: 'bob', engName: 'Bob' }],
      memberListError: new Error('member.list down'),
    })
    useNats.mockReturnValue({ user: { account: 'alice', siteId: 'siteA' }, request })
    render(
      <MessageActionMenu
        message={msg}
        room={{ id: 'r1', siteId: 'siteA', userCount: 4 }}
      />,
    )
    fireEvent.click(screen.getByRole('button', { name: /Message actions/i }))
    expect(await screen.findByText('Read by 1 of 3')).toBeInTheDocument()
  })
})

describe('MessageActionMenu reader sub-tooltip', () => {
  const msg = { id: 'm1', sender: { account: 'alice' } }

  async function openMenuWith(readers) {
    const request = vi.fn().mockResolvedValue({ readers })
    useNats.mockReturnValue({
      user: { account: 'alice', siteId: 'siteA' },
      request,
    })
    render(<MessageActionMenu message={msg} room={room} />)
    fireEvent.click(screen.getByRole('button', { name: /Message actions/i }))
    await screen.findByText(/Read by /)
  }

  it('does not render the tooltip when X = 0', async () => {
    await openMenuWith([])
    expect(screen.queryByRole('tooltip')).not.toBeInTheDocument()
    fireEvent.mouseEnter(screen.getByText('Read by 0 of 3'))
    expect(screen.queryByRole('tooltip')).not.toBeInTheDocument()
  })

  it('opens the tooltip on hover when X > 0 and closes on mouse-leave', async () => {
    await openMenuWith([
      { userId: 'u1', account: 'bob', engName: 'Bob', chineseName: '鮑勃' },
    ])
    const row = screen.getByRole('menuitem', { name: /Read by 1 of 3/i })
    fireEvent.mouseEnter(row)
    expect(screen.getByRole('tooltip')).toBeInTheDocument()
    fireEvent.mouseLeave(row)
    expect(screen.queryByRole('tooltip')).not.toBeInTheDocument()
  })

  it('opens the tooltip on keyboard focus and closes on blur', async () => {
    await openMenuWith([
      { userId: 'u1', account: 'bob', engName: 'Bob', chineseName: '' },
    ])
    const row = screen.getByRole('menuitem', { name: /Read by 1 of 3/i })
    fireEvent.focus(row)
    expect(screen.getByRole('tooltip')).toBeInTheDocument()
    fireEvent.blur(row)
    expect(screen.queryByRole('tooltip')).not.toBeInTheDocument()
  })

  it('formats reader names as "engName chineseName" when both are present', async () => {
    await openMenuWith([
      { userId: 'u1', account: 'bob', engName: 'Bob', chineseName: '鮑勃' },
    ])
    fireEvent.mouseEnter(screen.getByRole('menuitem', { name: /Read by 1 of 3/i }))
    expect(screen.getByRole('tooltip')).toHaveTextContent('Bob 鮑勃')
  })

  it('formats reader names as "engName" when chineseName is empty', async () => {
    await openMenuWith([
      { userId: 'u1', account: 'bob', engName: 'Bob', chineseName: '' },
    ])
    fireEvent.mouseEnter(screen.getByRole('menuitem', { name: /Read by 1 of 3/i }))
    expect(screen.getByRole('tooltip')).toHaveTextContent('Bob')
    expect(screen.getByRole('tooltip')).not.toHaveTextContent('Bob ')
  })

  it('falls back to account when engName is empty', async () => {
    await openMenuWith([
      { userId: 'u1', account: 'bob', engName: '', chineseName: '鮑勃' },
    ])
    fireEvent.mouseEnter(screen.getByRole('menuitem', { name: /Read by 1 of 3/i }))
    expect(screen.getByRole('tooltip')).toHaveTextContent('bob 鮑勃')
  })

  it('lists all readers in the tooltip', async () => {
    await openMenuWith([
      { userId: 'u1', account: 'bob', engName: 'Bob', chineseName: '' },
      { userId: 'u2', account: 'carol', engName: 'Carol', chineseName: '凱蘿' },
    ])
    fireEvent.mouseEnter(screen.getByRole('menuitem', { name: /Read by 2 of 3/i }))
    const items = screen.getAllByRole('listitem')
    expect(items.map((li) => li.textContent)).toEqual(['Bob', 'Carol 凱蘿'])
  })
})
