import { describe, it, expect, vi, beforeEach } from 'vitest'
import { act, render, screen, fireEvent, waitFor } from '@testing-library/react'

vi.mock('@/context/AuthContext', () => ({ useAuth: vi.fn() }))
vi.mock('@/api', async (importOriginal) => {
  const actual = await importOriginal()
  return { ...actual, listUsers: vi.fn() }
})

import AccountPicker from './AccountPicker'
import { useAuth } from '@/context/AuthContext'
import { listUsers, AsyncJobError } from '@/api'

const ALICE = {
  id: 'u-1',
  account: 'alice',
  siteId: 'site-1',
  engName: 'Alice',
  chineseName: '',
  roles: [],
  active: true,
  requirePasswordChange: false,
}
const BOB = {
  id: 'u-2',
  account: 'bob',
  siteId: 'site-1',
  engName: 'Bob',
  chineseName: '',
  roles: [],
  active: true,
  requirePasswordChange: false,
}

let logout

beforeEach(() => {
  vi.clearAllMocks()
  logout = vi.fn()
  useAuth.mockReturnValue({ logout })
  listUsers.mockResolvedValue({ users: [], total: 0 })
})

async function typeAndWait(input, text) {
  fireEvent.change(input, { target: { value: text } })
  await act(async () => {
    await vi.advanceTimersByTimeAsync(300)
  })
}

describe('AccountPicker — single select', () => {
  it('does not query listUsers on mount', () => {
    render(<AccountPicker id="p" label="Applicant" authToken="tok" value="" onChange={vi.fn()} />)
    expect(listUsers).not.toHaveBeenCalled()
  })

  it('queries listUsers debounced as the admin types and lists matches', async () => {
    listUsers.mockResolvedValue({ users: [ALICE], total: 1 })
    render(<AccountPicker id="p" label="Applicant" authToken="tok" value="" onChange={vi.fn()} />)
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      await typeAndWait(screen.getByLabelText(/applicant/i), 'ali')
      expect(listUsers).toHaveBeenCalledWith('tok', { q: 'ali', page: 1, limit: 10 })
      expect(await screen.findByRole('option', { name: /alice/i })).toBeInTheDocument()
    } finally {
      vi.useRealTimers()
    }
  })

  it('selects a matched account on click, committing it via onChange', async () => {
    listUsers.mockResolvedValue({ users: [ALICE], total: 1 })
    const onChange = vi.fn()
    render(<AccountPicker id="p" label="Applicant" authToken="tok" value="" onChange={onChange} />)
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      await typeAndWait(screen.getByLabelText(/applicant/i), 'ali')
      fireEvent.click(await screen.findByRole('option', { name: /alice/i }))
    } finally {
      vi.useRealTimers()
    }
    expect(onChange).toHaveBeenCalledWith('alice')
  })

  it('never commits free-typed text that was not selected from the results', async () => {
    listUsers.mockResolvedValue({ users: [], total: 0 })
    const onChange = vi.fn()
    render(<AccountPicker id="p" label="Applicant" authToken="tok" value="" onChange={onChange} />)
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      await typeAndWait(screen.getByLabelText(/applicant/i), 'zzz-not-a-match')
    } finally {
      vi.useRealTimers()
    }
    expect(onChange).not.toHaveBeenCalled()
    expect(screen.queryByRole('option')).not.toBeInTheDocument()
  })

  it('renders the committed value as a removable chip and hides the search input', () => {
    render(<AccountPicker id="p" label="Applicant" authToken="tok" value="alice" onChange={vi.fn()} />)
    expect(screen.getByText('alice')).toBeInTheDocument()
    expect(screen.queryByLabelText(/applicant/i)).not.toBeInTheDocument()
  })

  it('clears the selection and re-shows the input when the chip is removed', () => {
    const onChange = vi.fn()
    render(<AccountPicker id="p" label="Applicant" authToken="tok" value="alice" onChange={onChange} />)
    fireEvent.click(screen.getByRole('button', { name: /remove alice/i }))
    expect(onChange).toHaveBeenCalledWith('')
  })

  it('closes the dropdown on Escape without committing a selection', async () => {
    listUsers.mockResolvedValue({ users: [ALICE], total: 1 })
    const onChange = vi.fn()
    render(<AccountPicker id="p" label="Applicant" authToken="tok" value="" onChange={onChange} />)
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      const input = screen.getByLabelText(/applicant/i)
      await typeAndWait(input, 'ali')
      expect(await screen.findByRole('option', { name: /alice/i })).toBeInTheDocument()
      fireEvent.keyDown(input, { key: 'Escape' })
    } finally {
      vi.useRealTimers()
    }
    expect(screen.queryByRole('option')).not.toBeInTheDocument()
    expect(onChange).not.toHaveBeenCalled()
  })
})

describe('AccountPicker — keyboard', () => {
  it('moves the highlight with ArrowDown and commits it with Enter', async () => {
    listUsers.mockResolvedValue({ users: [ALICE, BOB], total: 2 })
    const onChange = vi.fn()
    render(<AccountPicker id="p" label="Applicant" authToken="tok" value="" onChange={onChange} />)
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      const input = screen.getByLabelText(/applicant/i)
      await typeAndWait(input, 'a')
      expect(await screen.findByRole('option', { name: /alice/i })).toBeInTheDocument()

      // The highlight starts on the first match; one ArrowDown moves it to the second.
      expect(input).toHaveAttribute('aria-activedescendant', 'p-option-0')
      fireEvent.keyDown(input, { key: 'ArrowDown' })
      expect(input).toHaveAttribute('aria-activedescendant', 'p-option-1')
      expect(screen.getByRole('option', { name: /bob/i })).toHaveAttribute('aria-selected', 'true')
      expect(screen.getByRole('option', { name: /alice/i })).toHaveAttribute('aria-selected', 'false')

      // fireEvent returns false when the handler called preventDefault — Enter must not
      // fall through and submit the form this picker sits in.
      expect(fireEvent.keyDown(input, { key: 'Enter' })).toBe(false)
    } finally {
      vi.useRealTimers()
    }
    expect(onChange).toHaveBeenCalledWith('bob')
  })

  it('wraps the highlight from the first option to the last on ArrowUp', async () => {
    listUsers.mockResolvedValue({ users: [ALICE, BOB], total: 2 })
    const onChange = vi.fn()
    render(<AccountPicker id="p" label="Applicant" authToken="tok" value="" onChange={onChange} />)
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      const input = screen.getByLabelText(/applicant/i)
      await typeAndWait(input, 'a')
      expect(await screen.findByRole('option', { name: /alice/i })).toBeInTheDocument()
      fireEvent.keyDown(input, { key: 'ArrowUp' })
      expect(input).toHaveAttribute('aria-activedescendant', 'p-option-1')
      fireEvent.keyDown(input, { key: 'Enter' })
    } finally {
      vi.useRealTimers()
    }
    expect(onChange).toHaveBeenCalledWith('bob')
  })

  it('commits the first match on Enter with no arrow keys pressed', async () => {
    listUsers.mockResolvedValue({ users: [ALICE, BOB], total: 2 })
    const onChange = vi.fn()
    render(<AccountPicker id="p" label="Subjects" authToken="tok" multiple value={[]} onChange={onChange} />)
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      const input = screen.getByLabelText(/subjects/i)
      await typeAndWait(input, 'a')
      expect(await screen.findByRole('option', { name: /alice/i })).toBeInTheDocument()
      fireEvent.keyDown(input, { key: 'Enter' })
    } finally {
      vi.useRealTimers()
    }
    expect(onChange).toHaveBeenCalledWith(['alice'])
  })

  it('drops aria-activedescendant when Escape closes the list, and ArrowDown re-opens it', async () => {
    listUsers.mockResolvedValue({ users: [ALICE, BOB], total: 2 })
    const onChange = vi.fn()
    render(<AccountPicker id="p" label="Applicant" authToken="tok" value="" onChange={onChange} />)
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      const input = screen.getByLabelText(/applicant/i)
      await typeAndWait(input, 'a')
      expect(await screen.findByRole('option', { name: /alice/i })).toBeInTheDocument()

      fireEvent.keyDown(input, { key: 'Escape' })
      expect(input).not.toHaveAttribute('aria-activedescendant')
      // Enter on a closed list must not commit anything.
      fireEvent.keyDown(input, { key: 'Enter' })
      expect(onChange).not.toHaveBeenCalled()

      fireEvent.keyDown(input, { key: 'ArrowDown' })
      expect(input).toHaveAttribute('aria-activedescendant', 'p-option-0')
    } finally {
      vi.useRealTimers()
    }
  })
})

describe('AccountPicker — multi select', () => {
  it('appends a selected account as a chip via onChange', async () => {
    listUsers.mockResolvedValue({ users: [ALICE, BOB], total: 2 })
    const onChange = vi.fn()
    render(
      <AccountPicker id="p" label="Subjects" authToken="tok" multiple value={[]} onChange={onChange} />,
    )
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      await typeAndWait(screen.getByLabelText(/subjects/i), 'a')
      fireEvent.click(await screen.findByRole('option', { name: /alice/i }))
    } finally {
      vi.useRealTimers()
    }
    expect(onChange).toHaveBeenCalledWith(['alice'])
  })

  it('keeps the dropdown open after a selection so more matches can be picked', async () => {
    listUsers.mockResolvedValue({ users: [ALICE, BOB], total: 2 })
    render(<AccountPicker id="p" label="Subjects" authToken="tok" multiple value={[]} onChange={vi.fn()} />)
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      await typeAndWait(screen.getByLabelText(/subjects/i), 'a')
      fireEvent.click(await screen.findByRole('option', { name: /alice/i }))
      expect(await screen.findByRole('option', { name: /bob/i })).toBeInTheDocument()
    } finally {
      vi.useRealTimers()
    }
  })

  it('shows one chip per selected account and removes only the clicked one', () => {
    const onChange = vi.fn()
    render(
      <AccountPicker
        id="p"
        label="Subjects"
        authToken="tok"
        multiple
        value={['alice', 'bob']}
        onChange={onChange}
      />,
    )
    expect(screen.getByText('alice')).toBeInTheDocument()
    expect(screen.getByText('bob')).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: /remove alice/i }))
    expect(onChange).toHaveBeenCalledWith(['bob'])
  })

  it('excludes already-selected accounts from the results dropdown', async () => {
    listUsers.mockResolvedValue({ users: [ALICE, BOB], total: 2 })
    render(
      <AccountPicker id="p" label="Subjects" authToken="tok" multiple value={['alice']} onChange={vi.fn()} />,
    )
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      await typeAndWait(screen.getByLabelText(/subjects/i), 'a')
      expect(await screen.findByRole('option', { name: /bob/i })).toBeInTheDocument()
    } finally {
      vi.useRealTimers()
    }
    expect(screen.getAllByRole('option')).toHaveLength(1)
    expect(screen.queryByRole('option', { name: /alice/i })).not.toBeInTheDocument()
  })

  it('keeps the search input visible even after selections exist', () => {
    render(
      <AccountPicker id="p" label="Subjects" authToken="tok" multiple value={['alice']} onChange={vi.fn()} />,
    )
    expect(screen.getByLabelText(/subjects/i)).toBeInTheDocument()
  })

  it('never commits free-typed text into the selected array', async () => {
    listUsers.mockResolvedValue({ users: [], total: 0 })
    const onChange = vi.fn()
    render(<AccountPicker id="p" label="Subjects" authToken="tok" multiple value={[]} onChange={onChange} />)
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      await typeAndWait(screen.getByLabelText(/subjects/i), 'not-a-real-match')
    } finally {
      vi.useRealTimers()
    }
    expect(onChange).not.toHaveBeenCalled()
  })
})

describe('AccountPicker — errors', () => {
  it('logs the admin out instead of showing results on invalid_token', async () => {
    listUsers.mockRejectedValue(
      new AsyncJobError('expired', { code: 'unauthenticated', reason: 'invalid_token' }),
    )
    render(<AccountPicker id="p" label="Applicant" authToken="tok" value="" onChange={vi.fn()} />)
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      await typeAndWait(screen.getByLabelText(/applicant/i), 'ali')
    } finally {
      vi.useRealTimers()
    }
    await waitFor(() => expect(logout).toHaveBeenCalledTimes(1))
  })

  it('shows no options and does not throw on a non-auth error', async () => {
    listUsers.mockRejectedValue(new AsyncJobError('boom', { code: 'internal' }))
    render(<AccountPicker id="p" label="Applicant" authToken="tok" value="" onChange={vi.fn()} />)
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      await typeAndWait(screen.getByLabelText(/applicant/i), 'ali')
    } finally {
      vi.useRealTimers()
    }
    expect(screen.queryByRole('option')).not.toBeInTheDocument()
    expect(logout).not.toHaveBeenCalled()
  })
})

describe('AccountPicker — stale response guard', () => {
  it('drops a stale out-of-order response so a slow earlier query cannot overwrite newer results', async () => {
    render(<AccountPicker id="p" label="Applicant" authToken="tok" value="" onChange={vi.fn()} />)
    const input = screen.getByLabelText(/applicant/i)

    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      // First keystroke ("al") fires a query that will resolve LATE.
      let resolveSlow
      const slow = new Promise((resolve) => {
        resolveSlow = resolve
      })
      listUsers.mockReturnValueOnce(slow)
      await typeAndWait(input, 'al')
      await waitFor(() =>
        expect(listUsers).toHaveBeenCalledWith('tok', { q: 'al', page: 1, limit: 10 }),
      )

      // Second keystroke ("ali") fires a newer query that resolves immediately, before the
      // first one settles — both requests are now in flight, out of order.
      listUsers.mockResolvedValueOnce({
        users: [
          {
            id: 'u-fresh',
            account: 'fresh-account',
            siteId: 'site-1',
            engName: '',
            chineseName: '',
            roles: [],
            active: true,
            requirePasswordChange: false,
          },
        ],
        total: 1,
      })
      await typeAndWait(input, 'ali')
      await waitFor(() =>
        expect(listUsers).toHaveBeenCalledWith('tok', { q: 'ali', page: 1, limit: 10 }),
      )
      expect(await screen.findByRole('option', { name: /fresh-account/i })).toBeInTheDocument()

      // The stale "al" response now resolves — it must be dropped, not overwrite the
      // already-current "ali" results.
      await act(async () => {
        resolveSlow({
          users: [
            {
              id: 'u-stale',
              account: 'stale-account',
              siteId: 'site-1',
              engName: '',
              chineseName: '',
              roles: [],
              active: true,
              requirePasswordChange: false,
            },
          ],
          total: 1,
        })
        await Promise.resolve()
      })

      expect(screen.queryByRole('option', { name: /stale-account/i })).not.toBeInTheDocument()
      expect(screen.getByRole('option', { name: /fresh-account/i })).toBeInTheDocument()
    } finally {
      vi.useRealTimers()
    }
  })
})

describe('AccountPicker — injected searchAccounts', () => {
  it('queries the injected source instead of listUsers', async () => {
    const searchAccounts = vi.fn().mockResolvedValue([{ account: 'alice' }])
    render(
      <AccountPicker
        id="p"
        label="Owner"
        authToken="tok"
        value=""
        onChange={vi.fn()}
        searchAccounts={searchAccounts}
      />,
    )
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      await typeAndWait(screen.getByLabelText(/owner/i), 'ali')
      expect(searchAccounts).toHaveBeenCalledWith('ali')
      expect(listUsers).not.toHaveBeenCalled()
      expect(await screen.findByRole('option', { name: /alice/i })).toBeInTheDocument()
    } finally {
      vi.useRealTimers()
    }
  })

  it('commits an account selected from the injected source', async () => {
    const onChange = vi.fn()
    render(
      <AccountPicker
        id="p"
        label="Owner"
        authToken="tok"
        value=""
        onChange={onChange}
        searchAccounts={vi.fn().mockResolvedValue([{ account: 'alice' }])}
      />,
    )
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      await typeAndWait(screen.getByLabelText(/owner/i), 'ali')
      fireEvent.click(await screen.findByRole('option', { name: /alice/i }))
    } finally {
      vi.useRealTimers()
    }
    expect(onChange).toHaveBeenCalledWith('alice')
  })

  it('leaves the dropdown empty when the injected source rejects', async () => {
    render(
      <AccountPicker
        id="p"
        label="Owner"
        authToken="tok"
        value=""
        onChange={vi.fn()}
        searchAccounts={vi.fn().mockRejectedValue(new Error('boom'))}
      />,
    )
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      await typeAndWait(screen.getByLabelText(/owner/i), 'ali')
    } finally {
      vi.useRealTimers()
    }
    expect(screen.queryByRole('option')).not.toBeInTheDocument()
  })
})
