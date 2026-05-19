import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { createRef } from 'react'
import { render, screen, fireEvent, waitFor, act } from '@testing-library/react'
import MemberPicker from './MemberPicker'

vi.mock('@/context/NatsContext', () => ({
  useNats: vi.fn(),
}))

import { useNats } from '@/context/NatsContext'

function setup(overrides = {}) {
  const request = vi.fn().mockResolvedValue({ rooms: [] })
  useNats.mockReturnValue({
    user: { account: 'alice', siteId: 'site-A' },
    request,
    ...overrides,
  })
  const onUsersChange = vi.fn()
  const onOrgsChange = vi.fn()
  const onChannelsChange = vi.fn()
  const utils = render(
    <MemberPicker
      users={[]}
      orgs={[]}
      channels={[]}
      onUsersChange={onUsersChange}
      onOrgsChange={onOrgsChange}
      onChannelsChange={onChannelsChange}
    />
  )
  return { request, onUsersChange, onOrgsChange, onChannelsChange, ...utils }
}

describe('MemberPicker', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    vi.useFakeTimers({ shouldAdvanceTime: true })
  })
  afterEach(() => {
    vi.useRealTimers()
  })

  it('renders three labeled inputs', () => {
    setup()
    expect(screen.getByLabelText(/Users/i)).toBeInTheDocument()
    expect(screen.getByLabelText(/Orgs/i)).toBeInTheDocument()
    expect(screen.getByLabelText(/Channels/i)).toBeInTheDocument()
  })

  it('renders existing chips from props', () => {
    useNats.mockReturnValue({ user: { account: 'alice', siteId: 'site-A' }, request: vi.fn() })
    render(
      <MemberPicker
        users={['bob', 'charlie']}
        orgs={['eng']}
        channels={[{ roomId: 'r-x', siteId: 'site-A' }]}
        onUsersChange={vi.fn()}
        onOrgsChange={vi.fn()}
        onChannelsChange={vi.fn()}
      />
    )
    expect(screen.getByText('bob')).toBeInTheDocument()
    expect(screen.getByText('charlie')).toBeInTheDocument()
    expect(screen.getByText('eng')).toBeInTheDocument()
    expect(screen.getByText(/r-x/)).toBeInTheDocument()
  })

  it('Enter on Users input commits typed value as a chip and clears the input', () => {
    const { onUsersChange } = setup()
    const input = screen.getByLabelText(/Users/i)
    fireEvent.change(input, { target: { value: 'bob' } })
    fireEvent.keyDown(input, { key: 'Enter' })
    expect(onUsersChange).toHaveBeenCalledWith(['bob'])
    expect(input.value).toBe('')
  })

  it('Enter on Users input with comma-separated text commits each segment as its own chip', () => {
    const { onUsersChange } = setup()
    const input = screen.getByLabelText(/Users/i)
    fireEvent.change(input, { target: { value: 'alice, bob , charlie' } })
    fireEvent.keyDown(input, { key: 'Enter' })
    expect(onUsersChange).toHaveBeenCalledWith(['alice', 'bob', 'charlie'])
    expect(input.value).toBe('')
  })

  it('comma-separated parsing drops empty segments and trims whitespace', () => {
    const { onOrgsChange } = setup()
    const input = screen.getByLabelText(/Orgs/i)
    fireEvent.change(input, { target: { value: ' eng,, ops ,   ' } })
    fireEvent.keyDown(input, { key: 'Enter' })
    expect(onOrgsChange).toHaveBeenCalledWith(['eng', 'ops'])
  })

  it('comma-separated parsing for Channels turns each id into a local-site ChannelRef', () => {
    const { onChannelsChange } = setup()
    const input = screen.getByLabelText(/Channels/i)
    fireEvent.change(input, { target: { value: 'r1, r2, r3' } })
    fireEvent.keyDown(input, { key: 'Enter' })
    expect(onChannelsChange).toHaveBeenCalledWith([
      { roomId: 'r1', siteId: 'site-A' },
      { roomId: 'r2', siteId: 'site-A' },
      { roomId: 'r3', siteId: 'site-A' },
    ])
  })

  it('partial dedup on multi-add: existing chips are skipped, new ones land', () => {
    const onUsersChange = vi.fn()
    useNats.mockReturnValue({ user: { account: 'alice', siteId: 'site-A' }, request: vi.fn() })
    render(
      <MemberPicker
        users={['bob']}
        orgs={[]}
        channels={[]}
        onUsersChange={onUsersChange}
        onOrgsChange={vi.fn()}
        onChannelsChange={vi.fn()}
      />
    )
    fireEvent.change(screen.getByLabelText(/Users/i), { target: { value: 'alice, bob, charlie' } })
    fireEvent.keyDown(screen.getByLabelText(/Users/i), { key: 'Enter' })
    expect(onUsersChange).toHaveBeenCalledWith(['bob', 'alice', 'charlie'])
  })

  it('flushAndGetEntries also splits comma-separated pending text', () => {
    const onUsersChange = vi.fn()
    useNats.mockReturnValue({ user: { account: 'alice', siteId: 'site-A' }, request: vi.fn() })
    const ref = createRef()
    render(
      <MemberPicker
        ref={ref}
        users={[]}
        orgs={[]}
        channels={[]}
        onUsersChange={onUsersChange}
        onOrgsChange={vi.fn()}
        onChannelsChange={vi.fn()}
      />
    )
    fireEvent.change(screen.getByLabelText(/Users/i), { target: { value: 'alice, bob, charlie' } })
    let merged
    act(() => {
      merged = ref.current.flushAndGetEntries()
    })
    expect(merged.users).toEqual(['alice', 'bob', 'charlie'])
    expect(onUsersChange).toHaveBeenCalledWith(['alice', 'bob', 'charlie'])
  })

  it('does not call search.users — endpoint does not exist server-side', () => {
    const { request } = setup()
    fireEvent.change(screen.getByLabelText(/Users/i), { target: { value: 'bo' } })
    vi.advanceTimersByTime(500)
    expect(request).not.toHaveBeenCalled()
  })

  it('Enter on Channels input commits a ChannelRef using the current user siteId', () => {
    const { onChannelsChange } = setup()
    const input = screen.getByLabelText(/Channels/i)
    fireEvent.change(input, { target: { value: 'r-x' } })
    fireEvent.keyDown(input, { key: 'Enter' })
    expect(onChannelsChange).toHaveBeenCalledWith([{ roomId: 'r-x', siteId: 'site-A' }])
  })

  it('Enter on an empty input does nothing', () => {
    const { onUsersChange } = setup()
    const input = screen.getByLabelText(/Users/i)
    fireEvent.keyDown(input, { key: 'Enter' })
    expect(onUsersChange).not.toHaveBeenCalled()
  })

  it('does not duplicate an already-selected value on Enter', () => {
    const onUsersChange = vi.fn()
    useNats.mockReturnValue({ user: { account: 'alice', siteId: 'site-A' }, request: vi.fn() })
    render(
      <MemberPicker
        users={['bob']}
        orgs={[]}
        channels={[]}
        onUsersChange={onUsersChange}
        onOrgsChange={vi.fn()}
        onChannelsChange={vi.fn()}
      />
    )
    const input = screen.getByLabelText(/Users/i)
    fireEvent.change(input, { target: { value: 'bob' } })
    fireEvent.keyDown(input, { key: 'Enter' })
    expect(onUsersChange).not.toHaveBeenCalled()
  })

  it('clicking the × on a chip removes that entry', () => {
    const onUsersChange = vi.fn()
    useNats.mockReturnValue({ user: { account: 'alice', siteId: 'site-A' }, request: vi.fn() })
    render(
      <MemberPicker
        users={['bob', 'charlie']}
        orgs={[]}
        channels={[]}
        onUsersChange={onUsersChange}
        onOrgsChange={vi.fn()}
        onChannelsChange={vi.fn()}
      />
    )
    fireEvent.click(screen.getByRole('button', { name: /Remove bob/i }))
    expect(onUsersChange).toHaveBeenCalledWith(['charlie'])
  })

  it('debounces search.rooms (channels) and adds a ChannelRef when a result is clicked', async () => {
    const request = vi.fn().mockResolvedValue({
      rooms: [{ roomId: 'r-x', name: 'project-x', siteId: 'site-B', roomType: 'c' }],
    })
    const onChannelsChange = vi.fn()
    useNats.mockReturnValue({ user: { account: 'alice', siteId: 'site-A' }, request })
    render(
      <MemberPicker
        users={[]}
        orgs={[]}
        channels={[]}
        onUsersChange={vi.fn()}
        onOrgsChange={vi.fn()}
        onChannelsChange={onChannelsChange}
      />
    )
    fireEvent.change(screen.getByLabelText(/Channels/i), { target: { value: 'pro' } })
    vi.advanceTimersByTime(250)
    await waitFor(() => {
      expect(request).toHaveBeenCalledWith(
        'chat.user.alice.request.search.rooms',
        expect.objectContaining({ query: 'pro' })
      )
    })
    await waitFor(() => expect(screen.getByText('project-x')).toBeInTheDocument())
    fireEvent.click(screen.getByText('project-x'))
    expect(onChannelsChange).toHaveBeenCalledWith([{ roomId: 'r-x', siteId: 'site-B' }])
  })

  it('survives a failing search.rooms request without breaking the input', async () => {
    const request = vi.fn().mockRejectedValue(new Error('no responders available'))
    useNats.mockReturnValue({ user: { account: 'alice', siteId: 'site-A' }, request })
    render(
      <MemberPicker
        users={[]}
        orgs={[]}
        channels={[]}
        onUsersChange={vi.fn()}
        onOrgsChange={vi.fn()}
        onChannelsChange={vi.fn()}
      />
    )
    fireEvent.change(screen.getByLabelText(/Channels/i), { target: { value: 'pro' } })
    vi.advanceTimersByTime(250)
    await waitFor(() => expect(request).toHaveBeenCalled())
    expect(screen.getByLabelText(/Channels/i)).not.toBeDisabled()
  })

  it('forwarded ref exposes flushAndGetEntries that captures typed-but-uncommitted text', () => {
    const onUsersChange = vi.fn()
    const onOrgsChange = vi.fn()
    const onChannelsChange = vi.fn()
    useNats.mockReturnValue({ user: { account: 'alice', siteId: 'site-A' }, request: vi.fn() })
    const ref = createRef()
    render(
      <MemberPicker
        ref={ref}
        users={['existing']}
        orgs={[]}
        channels={[]}
        onUsersChange={onUsersChange}
        onOrgsChange={onOrgsChange}
        onChannelsChange={onChannelsChange}
      />
    )
    // User types into each field without ever pressing Enter:
    fireEvent.change(screen.getByLabelText(/Users/i), { target: { value: 'bob' } })
    fireEvent.change(screen.getByLabelText(/Orgs/i), { target: { value: 'eng-org' } })
    fireEvent.change(screen.getByLabelText(/Channels/i), { target: { value: 'r-typed' } })

    // Parent calls flush at submit time. Wrap in act so the reset() state
    // updates inside the imperative call flush before the next render-state
    // assertion. Without act, the input value still shows the typed text.
    let merged
    act(() => {
      merged = ref.current.flushAndGetEntries()
    })

    expect(merged).toEqual({
      users: ['existing', 'bob'],
      orgs: ['eng-org'],
      channels: [{ roomId: 'r-typed', siteId: 'site-A' }],
    })
    // Each field's pending text is also propagated as a chip via onChange.
    expect(onUsersChange).toHaveBeenCalledWith(['existing', 'bob'])
    expect(onOrgsChange).toHaveBeenCalledWith(['eng-org'])
    expect(onChannelsChange).toHaveBeenCalledWith([{ roomId: 'r-typed', siteId: 'site-A' }])
    // Inputs are cleared after flush.
    expect(screen.getByLabelText(/Users/i).value).toBe('')
    expect(screen.getByLabelText(/Orgs/i).value).toBe('')
    expect(screen.getByLabelText(/Channels/i).value).toBe('')
  })

  it('flushAndGetEntries returns current entries unchanged when no pending text', () => {
    useNats.mockReturnValue({ user: { account: 'alice', siteId: 'site-A' }, request: vi.fn() })
    const ref = createRef()
    render(
      <MemberPicker
        ref={ref}
        users={['bob']}
        orgs={['eng']}
        channels={[{ roomId: 'r-x', siteId: 'site-A' }]}
        onUsersChange={vi.fn()}
        onOrgsChange={vi.fn()}
        onChannelsChange={vi.fn()}
      />
    )
    const merged = ref.current.flushAndGetEntries()
    expect(merged).toEqual({
      users: ['bob'],
      orgs: ['eng'],
      channels: [{ roomId: 'r-x', siteId: 'site-A' }],
    })
  })

  it('flushAndGetEntries skips pending text that duplicates an existing chip', () => {
    const onUsersChange = vi.fn()
    useNats.mockReturnValue({ user: { account: 'alice', siteId: 'site-A' }, request: vi.fn() })
    const ref = createRef()
    render(
      <MemberPicker
        ref={ref}
        users={['bob']}
        orgs={[]}
        channels={[]}
        onUsersChange={onUsersChange}
        onOrgsChange={vi.fn()}
        onChannelsChange={vi.fn()}
      />
    )
    fireEvent.change(screen.getByLabelText(/Users/i), { target: { value: 'bob' } })
    let merged
    act(() => {
      merged = ref.current.flushAndGetEntries()
    })
    expect(merged.users).toEqual(['bob'])
    expect(onUsersChange).not.toHaveBeenCalled()
    // Input still clears even on dedup so the next submit isn't sticky.
    expect(screen.getByLabelText(/Users/i).value).toBe('')
  })

  it('disables all inputs when disabled prop is set', () => {
    useNats.mockReturnValue({ user: { account: 'alice', siteId: 'site-A' }, request: vi.fn() })
    render(
      <MemberPicker
        users={[]}
        orgs={[]}
        channels={[]}
        onUsersChange={vi.fn()}
        onOrgsChange={vi.fn()}
        onChannelsChange={vi.fn()}
        disabled
      />
    )
    expect(screen.getByLabelText(/Users/i)).toBeDisabled()
    expect(screen.getByLabelText(/Orgs/i)).toBeDisabled()
    expect(screen.getByLabelText(/Channels/i)).toBeDisabled()
  })
})
