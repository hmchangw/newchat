import { describe, it, expect, vi, beforeEach } from 'vitest'
import { act, render, screen, fireEvent, waitFor } from '@testing-library/react'

vi.mock('@/context/AuthContext', () => ({ useAuth: vi.fn() }))
vi.mock('@/api', async (importOriginal) => {
  const actual = await importOriginal()
  return { ...actual, createPermissions: vi.fn(), listUsers: vi.fn() }
})

import CreatePermissionsDialog from './CreatePermissionsDialog'
import { useAuth } from '@/context/AuthContext'
import { createPermissions, listUsers, AsyncJobError } from '@/api'

const ALICE = { id: 'u-1', account: 'alice', siteId: 'site-1', engName: 'Alice', chineseName: '', roles: [], active: true, requirePasswordChange: false }
const BOB = { id: 'u-2', account: 'bob', siteId: 'site-1', engName: 'Bob', chineseName: '', roles: [], active: true, requirePasswordChange: false }
const CAROL = { id: 'u-3', account: 'carol', siteId: 'site-1', engName: 'Carol', chineseName: '', roles: [], active: true, requirePasswordChange: false }
const DAVE = { id: 'u-4', account: 'dave', siteId: 'site-1', engName: 'Dave', chineseName: '', roles: [], active: true, requirePasswordChange: false }

let logout

beforeEach(() => {
  vi.clearAllMocks()
  logout = vi.fn()
  useAuth.mockReturnValue({ logout })
  listUsers.mockResolvedValue({ users: [], total: 0 })
})

// Drives one AccountPicker through a real search → select cycle. Requires fake timers
// to already be active (the picker's search is debounced 300ms).
async function pick(labelRegex, user) {
  listUsers.mockResolvedValueOnce({ users: [user], total: 1 })
  fireEvent.change(screen.getByLabelText(labelRegex), { target: { value: user.account } })
  await act(async () => {
    await vi.advanceTimersByTimeAsync(300)
  })
  fireEvent.click(await screen.findByRole('option', { name: new RegExp(`^${user.account}`, 'i') }))
}

async function fillRequiredPickers({ expiresAt = '2026-12-31' } = {}) {
  vi.useFakeTimers({ shouldAdvanceTime: true })
  try {
    await pick(/^subject accounts$/i, ALICE)
    await pick(/^applicant account$/i, CAROL)
    await pick(/^approver account$/i, DAVE)
  } finally {
    vi.useRealTimers()
  }
  if (expiresAt) {
    fireEvent.change(screen.getByLabelText(/expires at/i), { target: { value: expiresAt } })
  }
}

describe('CreatePermissionsDialog — form', () => {
  it('renders the form fields with grant mode selected by default', () => {
    render(<CreatePermissionsDialog authToken="tok" onClose={vi.fn()} onCreated={vi.fn()} />)
    expect(screen.getByRole('radio', { name: /^grant$/i })).toBeChecked()
    expect(screen.getByRole('radio', { name: /^revoke$/i })).not.toBeChecked()
    expect(screen.getByLabelText(/^subject accounts$/i)).toBeInTheDocument()
    expect(screen.getByLabelText(/effective from/i)).toBeInTheDocument()
    expect(screen.getByLabelText(/expires at/i)).toBeInTheDocument()
    expect(screen.getByLabelText(/^applicant account$/i)).toBeInTheDocument()
    expect(screen.getByLabelText(/^approver account$/i)).toBeInTheDocument()
    expect(screen.getByLabelText(/^reason/i)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /grant permission/i })).toBeInTheDocument()
  })

  it('hides the effectiveFrom/expiresAt date inputs in revoke mode', () => {
    render(<CreatePermissionsDialog authToken="tok" onClose={vi.fn()} onCreated={vi.fn()} />)
    fireEvent.click(screen.getByRole('radio', { name: /^revoke$/i }))
    expect(screen.queryByLabelText(/effective from/i)).not.toBeInTheDocument()
    expect(screen.queryByLabelText(/expires at/i)).not.toBeInTheDocument()
    expect(screen.getByRole('button', { name: /revoke permission/i })).toBeInTheDocument()
  })

  it('disables submit until subjects, applicant, approver, and expires-at are all set', async () => {
    render(<CreatePermissionsDialog authToken="tok" onClose={vi.fn()} onCreated={vi.fn()} />)
    expect(screen.getByRole('button', { name: /grant permission/i })).toBeDisabled()

    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      await pick(/^subject accounts$/i, ALICE)
    } finally {
      vi.useRealTimers()
    }
    expect(screen.getByRole('button', { name: /grant permission/i })).toBeDisabled()

    fireEvent.change(screen.getByLabelText(/expires at/i), { target: { value: '2026-12-31' } })
    expect(screen.getByRole('button', { name: /grant permission/i })).toBeDisabled()

    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      await pick(/^applicant account$/i, CAROL)
      await pick(/^approver account$/i, DAVE)
    } finally {
      vi.useRealTimers()
    }
    expect(screen.getByRole('button', { name: /grant permission/i })).toBeEnabled()
  })

  it('shows a client-side error and disables submit when more than 200 subjects are selected', async () => {
    const manyUsers = Array.from({ length: 201 }, (_, i) => ({
      id: `u-${i}`,
      account: `user${i}`,
      siteId: 'site-1',
      engName: '',
      chineseName: '',
      roles: [],
      active: true,
      requirePasswordChange: false,
    }))
    listUsers.mockResolvedValue({ users: manyUsers, total: 201 })
    render(<CreatePermissionsDialog authToken="tok" onClose={vi.fn()} onCreated={vi.fn()} />)

    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      fireEvent.change(screen.getByLabelText(/^subject accounts$/i), { target: { value: 'user' } })
      await act(async () => {
        await vi.advanceTimersByTimeAsync(300)
      })
      const options = await screen.findAllByRole('option')
      expect(options).toHaveLength(201)
      // One fireEvent.click per option, NOT batched inside a single outer act(): each
      // fireEvent.click already act()-wraps and flushes its own render, so `value` stays
      // fresh for the next click. Batching all 201 into one act() would have every click
      // read the same stale `value` closure and only the last selection would stick.
      for (const option of options) {
        fireEvent.click(option)
      }
    } finally {
      vi.useRealTimers()
    }

    expect(screen.getByText(/enter at most 200 accounts/i)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /grant permission/i })).toBeDisabled()
  })

  it('never includes free-typed, unselected text in the submitted subjectAccounts', async () => {
    createPermissions.mockResolvedValue({ created: 1, duplicatesIgnored: [], grants: [] })
    render(<CreatePermissionsDialog authToken="tok" onClose={vi.fn()} onCreated={vi.fn()} />)

    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      await pick(/^subject accounts$/i, ALICE)
      listUsers.mockResolvedValueOnce({ users: [], total: 0 })
      fireEvent.change(screen.getByLabelText(/^subject accounts$/i), {
        target: { value: 'not-a-real-match' },
      })
      await act(async () => {
        await vi.advanceTimersByTimeAsync(300)
      })
      await pick(/^applicant account$/i, CAROL)
      await pick(/^approver account$/i, DAVE)
    } finally {
      vi.useRealTimers()
    }
    fireEvent.change(screen.getByLabelText(/expires at/i), { target: { value: '2026-12-31' } })

    fireEvent.click(screen.getByRole('button', { name: /grant permission/i }))

    await waitFor(() => expect(createPermissions).toHaveBeenCalledTimes(1))
    const [, payload] = createPermissions.mock.calls[0]
    expect(payload.subjectAccounts).toEqual(['alice'])
  })

  it('blocks submit and shows an error when reason exceeds 1000 runes', async () => {
    render(<CreatePermissionsDialog authToken="tok" onClose={vi.fn()} onCreated={vi.fn()} />)
    await fillRequiredPickers()

    const tooLong = 'a'.repeat(1001)
    fireEvent.change(screen.getByLabelText(/^reason/i), { target: { value: tooLong } })

    expect(screen.getByRole('button', { name: /grant permission/i })).toBeDisabled()
    expect(screen.getByText(/1001 \/ 1000/)).toBeInTheDocument()
  })
})

describe('CreatePermissionsDialog — submit', () => {
  it('submits a grant with the exact payload, omitting effectiveFrom and reason when left blank', async () => {
    createPermissions.mockResolvedValue({ created: 2, duplicatesIgnored: [], grants: [] })
    render(<CreatePermissionsDialog authToken="tok" onClose={vi.fn()} onCreated={vi.fn()} />)

    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      await pick(/^subject accounts$/i, ALICE)
      await pick(/^subject accounts$/i, BOB)
      await pick(/^applicant account$/i, CAROL)
      await pick(/^approver account$/i, DAVE)
    } finally {
      vi.useRealTimers()
    }
    fireEvent.change(screen.getByLabelText(/expires at/i), { target: { value: '2026-12-31' } })

    fireEvent.click(screen.getByRole('button', { name: /grant permission/i }))

    await waitFor(() => expect(createPermissions).toHaveBeenCalledTimes(1))
    const [token, payload] = createPermissions.mock.calls[0]
    expect(token).toBe('tok')
    expect(payload).toEqual({
      permission: 'external.image.view',
      subjectAccounts: ['alice', 'bob'],
      granted: true,
      expiresAt: '2026-12-31',
      applicantAccount: 'carol',
      approverAccount: 'dave',
    })
    expect(payload).not.toHaveProperty('effectiveFrom')
    expect(payload).not.toHaveProperty('reason')
  })

  it('includes effectiveFrom and reason in the grant payload when filled in', async () => {
    createPermissions.mockResolvedValue({ created: 1, duplicatesIgnored: [], grants: [] })
    render(<CreatePermissionsDialog authToken="tok" onClose={vi.fn()} onCreated={vi.fn()} />)
    await fillRequiredPickers()
    fireEvent.change(screen.getByLabelText(/effective from/i), { target: { value: '2026-09-01' } })
    fireEvent.change(screen.getByLabelText(/^reason/i), {
      target: { value: 'On-call staff must review photos.' },
    })

    fireEvent.click(screen.getByRole('button', { name: /grant permission/i }))

    await waitFor(() => expect(createPermissions).toHaveBeenCalledTimes(1))
    const [, payload] = createPermissions.mock.calls[0]
    expect(payload.effectiveFrom).toBe('2026-09-01')
    expect(payload.reason).toBe('On-call staff must review photos.')
  })

  it('submits a revoke with the window fields omitted entirely', async () => {
    createPermissions.mockResolvedValue({ created: 1, duplicatesIgnored: [], grants: [] })
    render(<CreatePermissionsDialog authToken="tok" onClose={vi.fn()} onCreated={vi.fn()} />)
    fireEvent.click(screen.getByRole('radio', { name: /^revoke$/i }))
    await fillRequiredPickers({ expiresAt: null })

    fireEvent.click(screen.getByRole('button', { name: /revoke permission/i }))

    await waitFor(() => expect(createPermissions).toHaveBeenCalledTimes(1))
    const [, payload] = createPermissions.mock.calls[0]
    expect(payload).toEqual({
      permission: 'external.image.view',
      subjectAccounts: ['alice'],
      granted: false,
      applicantAccount: 'carol',
      approverAccount: 'dave',
    })
    expect(payload).not.toHaveProperty('effectiveFrom')
    expect(payload).not.toHaveProperty('expiresAt')
  })

  it('disables the submit button while the create request is in flight', async () => {
    let resolveCreate
    createPermissions.mockReturnValue(
      new Promise((resolve) => {
        resolveCreate = resolve
      }),
    )
    render(<CreatePermissionsDialog authToken="tok" onClose={vi.fn()} onCreated={vi.fn()} />)
    await fillRequiredPickers()

    fireEvent.click(screen.getByRole('button', { name: /grant permission/i }))
    expect(screen.getByRole('button', { name: /submitting/i })).toBeDisabled()

    await act(async () => {
      resolveCreate({ created: 1, duplicatesIgnored: [], grants: [] })
      await Promise.resolve()
    })
    expect(await screen.findByText(/created 1 grant/i)).toBeInTheDocument()
  })
})

describe('CreatePermissionsDialog — result', () => {
  it('shows the result content and calls onCreated immediately on success, without closing the dialog', async () => {
    createPermissions.mockResolvedValue({
      created: 2,
      duplicatesIgnored: ['eve'],
      grants: [
        { id: 'g-1', subjectAccount: 'alice' },
        { id: 'g-2', subjectAccount: 'bob' },
      ],
    })
    const onCreated = vi.fn()
    const onClose = vi.fn()
    render(<CreatePermissionsDialog authToken="tok" onClose={onClose} onCreated={onCreated} />)
    await fillRequiredPickers()

    fireEvent.click(screen.getByRole('button', { name: /grant permission/i }))

    expect(await screen.findByText(/created 2/i)).toBeInTheDocument()
    expect(screen.getByText(/eve/)).toBeInTheDocument()
    expect(onCreated).toHaveBeenCalledTimes(1)
    expect(onClose).not.toHaveBeenCalled()
    expect(screen.getByRole('dialog')).toBeInTheDocument()
  })

  it('renders revoke-specific success copy in revoke mode', async () => {
    createPermissions.mockResolvedValue({ created: 1, duplicatesIgnored: [], grants: [] })
    render(<CreatePermissionsDialog authToken="tok" onClose={vi.fn()} onCreated={vi.fn()} />)
    fireEvent.click(screen.getByRole('radio', { name: /^revoke$/i }))
    await fillRequiredPickers({ expiresAt: null })

    fireEvent.click(screen.getByRole('button', { name: /revoke permission/i }))

    expect(await screen.findByText(/recorded 1 revocation/i)).toBeInTheDocument()
    expect(screen.queryByText(/created/i)).not.toBeInTheDocument()
  })

  it('the Close button in the result view calls onClose', async () => {
    createPermissions.mockResolvedValue({ created: 1, duplicatesIgnored: [], grants: [] })
    const onClose = vi.fn()
    render(<CreatePermissionsDialog authToken="tok" onClose={onClose} onCreated={vi.fn()} />)
    await fillRequiredPickers()

    fireEvent.click(screen.getByRole('button', { name: /grant permission/i }))

    expect(await screen.findByRole('button', { name: /^close$/i })).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: /^close$/i }))
    expect(onClose).toHaveBeenCalledTimes(1)
  })
})

describe('CreatePermissionsDialog — errors', () => {
  it('renders the mapped copy and offending accounts when createPermissions rejects with a reason', async () => {
    createPermissions.mockRejectedValue(
      new AsyncJobError('unknown accounts: zzz', {
        code: 'not_found',
        reason: 'unknown_accounts',
        metadata: { accounts: 'zzz' },
      }),
    )
    render(<CreatePermissionsDialog authToken="tok" onClose={vi.fn()} onCreated={vi.fn()} />)
    await fillRequiredPickers()

    fireEvent.click(screen.getByRole('button', { name: /grant permission/i }))

    expect(await screen.findByText(/do not exist on this site/i)).toBeInTheDocument()
    expect(screen.getByText(/zzz/)).toBeInTheDocument()
  })

  it('logs the admin out instead of showing a banner on invalid_token', async () => {
    createPermissions.mockRejectedValue(
      new AsyncJobError('expired', { code: 'unauthenticated', reason: 'invalid_token' }),
    )
    render(<CreatePermissionsDialog authToken="tok" onClose={vi.fn()} onCreated={vi.fn()} />)
    await fillRequiredPickers()

    fireEvent.click(screen.getByRole('button', { name: /grant permission/i }))

    await waitFor(() => expect(logout).toHaveBeenCalledTimes(1))
    expect(screen.queryByText(/expired/i)).not.toBeInTheDocument()
  })
})
