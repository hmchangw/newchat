import { describe, it, expect, vi, beforeEach } from 'vitest'
import { act, render, screen, fireEvent, waitFor } from '@testing-library/react'

vi.mock('@/context/AuthContext', () => ({ useAuth: vi.fn() }))
vi.mock('@/api', async (importOriginal) => {
  const actual = await importOriginal()
  return { ...actual, createPermissions: vi.fn(), resyncPermissions: vi.fn(), listUsers: vi.fn() }
})

import CreatePermissionsDialog from './CreatePermissionsDialog'
import { useAuth } from '@/context/AuthContext'
import { createPermissions, resyncPermissions, listUsers, AsyncJobError } from '@/api'

const ALICE = { id: 'u-1', account: 'alice', siteId: 'site-1', engName: 'Alice', chineseName: '', roles: [], active: true, requirePasswordChange: false }
const BOB = { id: 'u-2', account: 'bob', siteId: 'site-1', engName: 'Bob', chineseName: '', roles: [], active: true, requirePasswordChange: false }
const CAROL = { id: 'u-3', account: 'carol', siteId: 'site-1', engName: 'Carol', chineseName: '', roles: [], active: true, requirePasswordChange: false }
const DAVE = { id: 'u-4', account: 'dave', siteId: 'site-1', engName: 'Dave', chineseName: '', roles: [], active: true, requirePasswordChange: false }
const GHOST = { id: 'u-5', account: 'ghost1', siteId: 'site-1', engName: 'Ghost', chineseName: '', roles: [], active: true, requirePasswordChange: false }

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

// Everything the form needs except the subject accounts — lets the paste-mode tests supply
// the subjects through the textarea instead of the picker.
async function fillNonSubjectFields({ expiresAt = '2026-12-31' } = {}) {
  vi.useFakeTimers({ shouldAdvanceTime: true })
  try {
    await pick(/^applicant account$/i, CAROL)
    await pick(/^approver account$/i, DAVE)
  } finally {
    vi.useRealTimers()
  }
  if (expiresAt) {
    fireEvent.change(screen.getByLabelText(/expires at/i), { target: { value: expiresAt } })
  }
}

async function fillRequiredPickers({ expiresAt = '2026-12-31' } = {}) {
  vi.useFakeTimers({ shouldAdvanceTime: true })
  try {
    await pick(/^subject accounts$/i, ALICE)
  } finally {
    vi.useRealTimers()
  }
  await fillNonSubjectFields({ expiresAt })
}

function pasteSubjects(text) {
  fireEvent.click(screen.getByRole('radio', { name: /^paste list/i }))
  fireEvent.change(screen.getByLabelText(/^subject accounts \(paste list\)$/i), {
    target: { value: text },
  })
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

  it('never includes free-typed, unselected text in the submitted subjectAccounts', async () => {
    createPermissions.mockResolvedValue({ created: 1, duplicatesIgnored: [] })
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

  it('labels the submit button with the effective account count', async () => {
    render(<CreatePermissionsDialog authToken="tok" onClose={vi.fn()} onCreated={vi.fn()} />)
    expect(
      screen.getByRole('button', { name: /^grant permission to 0 accounts$/i }),
    ).toBeInTheDocument()

    await fillRequiredPickers()
    expect(
      screen.getByRole('button', { name: /^grant permission to 1 account$/i }),
    ).toBeInTheDocument()

    fireEvent.click(screen.getByRole('radio', { name: /^revoke$/i }))
    expect(
      screen.getByRole('button', { name: /^revoke permission from 1 account$/i }),
    ).toBeInTheDocument()
  })
})

describe('CreatePermissionsDialog — subject input modes', () => {
  it('starts in pick mode and swaps the picker for a textarea when paste mode is selected', () => {
    render(<CreatePermissionsDialog authToken="tok" onClose={vi.fn()} onCreated={vi.fn()} />)
    expect(screen.getByRole('radio', { name: /^pick \(0\)$/i })).toBeChecked()
    expect(screen.getByLabelText(/^subject accounts$/i)).toBeInTheDocument()
    expect(screen.queryByLabelText(/^subject accounts \(paste list\)$/i)).not.toBeInTheDocument()

    fireEvent.click(screen.getByRole('radio', { name: /^paste list \(0\)$/i }))

    expect(screen.getByLabelText(/^subject accounts \(paste list\)$/i)).toBeInTheDocument()
    expect(screen.queryByLabelText(/^subject accounts$/i)).not.toBeInTheDocument()
  })

  it('shows the deduped count and the duplicates dropped from a pasted list', () => {
    render(<CreatePermissionsDialog authToken="tok" onClose={vi.fn()} onCreated={vi.fn()} />)
    pasteSubjects('alice bob,alice\ncarol')

    expect(screen.getByText(/3 accounts \(1 duplicate removed\)/)).toBeInTheDocument()
  })

  it('shows each mode its own count in the toggle labels', () => {
    render(<CreatePermissionsDialog authToken="tok" onClose={vi.fn()} onCreated={vi.fn()} />)
    pasteSubjects('alice bob,alice\ncarol')

    expect(screen.getByRole('radio', { name: /^pick \(0\)$/i })).toBeInTheDocument()
    expect(screen.getByRole('radio', { name: /^paste list \(3\)$/i })).toBeChecked()
  })

  it('disables submit while the pasted list parses to zero accounts', async () => {
    render(<CreatePermissionsDialog authToken="tok" onClose={vi.fn()} onCreated={vi.fn()} />)
    await fillNonSubjectFields()
    pasteSubjects('  \n , ; ')

    expect(screen.getByRole('button', { name: /grant permission/i })).toBeDisabled()

    fireEvent.change(screen.getByLabelText(/^subject accounts \(paste list\)$/i), {
      target: { value: 'alice' },
    })
    expect(screen.getByRole('button', { name: /grant permission/i })).toBeEnabled()
  })

  it('submits the parsed paste list, never merged with the picked accounts', async () => {
    createPermissions.mockResolvedValue({ created: 2, duplicatesIgnored: [] })
    render(<CreatePermissionsDialog authToken="tok" onClose={vi.fn()} onCreated={vi.fn()} />)
    await fillRequiredPickers() // picks ALICE in pick mode
    pasteSubjects('bob carol,bob')

    fireEvent.click(screen.getByRole('button', { name: /^grant permission to 2 accounts$/i }))

    await waitFor(() => expect(createPermissions).toHaveBeenCalledTimes(1))
    const [, payload] = createPermissions.mock.calls[0]
    expect(payload.subjectAccounts).toEqual(['bob', 'carol'])
  })

  it('accepts a 250-account paste — the old max-200 client cap is gone', async () => {
    createPermissions.mockResolvedValue({ created: 250, duplicatesIgnored: [] })
    render(<CreatePermissionsDialog authToken="tok" onClose={vi.fn()} onCreated={vi.fn()} />)
    await fillNonSubjectFields()
    const accounts = Array.from({ length: 250 }, (_, i) => `user${i}`)
    pasteSubjects(accounts.join('\n'))

    expect(screen.queryByText(/at most 200 accounts/i)).not.toBeInTheDocument()
    const submit = screen.getByRole('button', { name: /^grant permission to 250 accounts$/i })
    expect(submit).toBeEnabled()

    fireEvent.click(submit)

    await waitFor(() => expect(createPermissions).toHaveBeenCalledTimes(1))
    const [, payload] = createPermissions.mock.calls[0]
    expect(payload.subjectAccounts).toEqual(accounts)
  })
})

describe('CreatePermissionsDialog — submit', () => {
  it('submits a grant with the exact payload, omitting effectiveFrom and reason when left blank', async () => {
    createPermissions.mockResolvedValue({ created: 2, duplicatesIgnored: [] })
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
    createPermissions.mockResolvedValue({ created: 1, duplicatesIgnored: [] })
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
    createPermissions.mockResolvedValue({ created: 1, duplicatesIgnored: [] })
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
      resolveCreate({ created: 1, duplicatesIgnored: [] })
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
    createPermissions.mockResolvedValue({ created: 1, duplicatesIgnored: [] })
    render(<CreatePermissionsDialog authToken="tok" onClose={vi.fn()} onCreated={vi.fn()} />)
    fireEvent.click(screen.getByRole('radio', { name: /^revoke$/i }))
    await fillRequiredPickers({ expiresAt: null })

    fireEvent.click(screen.getByRole('button', { name: /revoke permission/i }))

    expect(await screen.findByText(/recorded 1 revocation/i)).toBeInTheDocument()
    expect(screen.queryByText(/created/i)).not.toBeInTheDocument()
  })

  it('collapses a long duplicatesIgnored list behind a count', async () => {
    const dupes = Array.from({ length: 30 }, (_, i) => `dupe${i}`)
    createPermissions.mockResolvedValue({ created: 1, duplicatesIgnored: dupes })
    render(<CreatePermissionsDialog authToken="tok" onClose={vi.fn()} onCreated={vi.fn()} />)
    await fillRequiredPickers()

    fireEvent.click(screen.getByRole('button', { name: /grant permission/i }))

    expect(await screen.findByText(/^30 duplicates ignored$/i)).toBeVisible()
    expect(screen.getByText(dupes.join(', '))).not.toBeVisible()
  })

  it('keeps a short duplicatesIgnored list inline', async () => {
    createPermissions.mockResolvedValue({
      created: 1,
      duplicatesIgnored: ['eve', 'mallory'],
    })
    render(<CreatePermissionsDialog authToken="tok" onClose={vi.fn()} onCreated={vi.fn()} />)
    await fillRequiredPickers()

    fireEvent.click(screen.getByRole('button', { name: /grant permission/i }))

    expect(await screen.findByText(/duplicates ignored: eve, mallory/i)).toBeVisible()
  })

  it('the Close button in the result view calls onClose', async () => {
    createPermissions.mockResolvedValue({ created: 1, duplicatesIgnored: [] })
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

describe('CreatePermissionsDialog — offender strip', () => {
  const unknownAccounts = (accounts) =>
    new AsyncJobError(`unknown accounts: ${accounts}`, {
      code: 'not_found',
      reason: 'unknown_accounts',
      metadata: { accounts },
    })

  it('removes the offending accounts from the pasted list', async () => {
    createPermissions.mockRejectedValue(unknownAccounts('ghost1, ghost2'))
    render(<CreatePermissionsDialog authToken="tok" onClose={vi.fn()} onCreated={vi.fn()} />)
    await fillNonSubjectFields()
    pasteSubjects('alice ghost1 bob ghost2')

    fireEvent.click(screen.getByRole('button', { name: /^grant permission to 4 accounts$/i }))

    fireEvent.click(await screen.findByRole('button', { name: /^remove these accounts$/i }))

    expect(screen.getByLabelText(/^subject accounts \(paste list\)$/i)).toHaveValue('alice\nbob')
    expect(screen.getByRole('radio', { name: /^paste list \(2\)$/i })).toBeChecked()
    expect(screen.queryByText(/do not exist on this site/i)).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /^remove these accounts$/i })).not.toBeInTheDocument()
  })

  it('removes the offending accounts from the picked selection', async () => {
    createPermissions.mockRejectedValue(unknownAccounts('ghost1'))
    render(<CreatePermissionsDialog authToken="tok" onClose={vi.fn()} onCreated={vi.fn()} />)
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      await pick(/^subject accounts$/i, ALICE)
      await pick(/^subject accounts$/i, GHOST)
    } finally {
      vi.useRealTimers()
    }
    await fillNonSubjectFields()

    fireEvent.click(screen.getByRole('button', { name: /^grant permission to 2 accounts$/i }))

    fireEvent.click(await screen.findByRole('button', { name: /^remove these accounts$/i }))

    expect(screen.queryByRole('button', { name: /^remove ghost1$/i })).not.toBeInTheDocument()
    expect(screen.getByRole('button', { name: /^remove alice$/i })).toBeInTheDocument()
    expect(
      screen.getByRole('button', { name: /^grant permission to 1 account$/i }),
    ).toBeInTheDocument()
  })

  it('offers no strip button when the error carries no offending accounts', async () => {
    createPermissions.mockRejectedValue(
      new AsyncJobError('subject deactivated', { code: 'conflict', reason: 'inactive_subject' }),
    )
    render(<CreatePermissionsDialog authToken="tok" onClose={vi.fn()} onCreated={vi.fn()} />)
    await fillRequiredPickers()

    fireEvent.click(screen.getByRole('button', { name: /grant permission/i }))

    expect(await screen.findByText(/deactivated/i)).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /^remove these accounts$/i })).not.toBeInTheDocument()
  })
})

describe('CreatePermissionsDialog — sync failures', () => {
  const withFailures = (syncFailures) => ({
    created: 2,
    duplicatesIgnored: [],
    syncFailures,
  })

  it('warns which sites did not acknowledge the sync and offers a resend', async () => {
    createPermissions.mockResolvedValue(withFailures(['site-b']))
    render(<CreatePermissionsDialog authToken="tok" onClose={vi.fn()} onCreated={vi.fn()} />)
    await fillRequiredPickers()

    fireEvent.click(screen.getByRole('button', { name: /grant permission/i }))

    expect(await screen.findByText(/sync failed for: site-b/i)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /^resend sync$/i })).toBeEnabled()
  })

  it('shows no sync banner when the response carries no syncFailures', async () => {
    createPermissions.mockResolvedValue({ created: 1, duplicatesIgnored: [] })
    render(<CreatePermissionsDialog authToken="tok" onClose={vi.fn()} onCreated={vi.fn()} />)
    await fillRequiredPickers()

    fireEvent.click(screen.getByRole('button', { name: /grant permission/i }))

    expect(await screen.findByText(/created 1 grant/i)).toBeInTheDocument()
    expect(screen.queryByText(/sync failed/i)).not.toBeInTheDocument()
    expect(screen.queryByText(/sync complete/i)).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /^resend sync$/i })).not.toBeInTheDocument()
  })

  it('resends the just-submitted request, not the picker selection left behind', async () => {
    createPermissions.mockResolvedValue(withFailures(['site-b']))
    resyncPermissions.mockResolvedValue({})
    render(<CreatePermissionsDialog authToken="tok" onClose={vi.fn()} onCreated={vi.fn()} />)
    await fillRequiredPickers() // picks ALICE in pick mode
    pasteSubjects('bob carol')

    fireEvent.click(screen.getByRole('button', { name: /^grant permission to 2 accounts$/i }))

    fireEvent.click(await screen.findByRole('button', { name: /^resend sync$/i }))

    await waitFor(() => expect(resyncPermissions).toHaveBeenCalledTimes(1))
    expect(resyncPermissions.mock.calls[0]).toEqual([
      'tok',
      { permission: 'external.image.view', accounts: ['bob', 'carol'] },
    ])
  })

  it('locks the resend button in flight and reports sync complete when it succeeds', async () => {
    createPermissions.mockResolvedValue(withFailures(['site-b']))
    let resolveResync
    resyncPermissions.mockReturnValue(
      new Promise((resolve) => {
        resolveResync = resolve
      }),
    )
    render(<CreatePermissionsDialog authToken="tok" onClose={vi.fn()} onCreated={vi.fn()} />)
    await fillRequiredPickers()

    fireEvent.click(screen.getByRole('button', { name: /grant permission/i }))
    fireEvent.click(await screen.findByRole('button', { name: /^resend sync$/i }))

    expect(screen.getByRole('button', { name: /resending/i })).toBeDisabled()

    await act(async () => {
      resolveResync({})
      await Promise.resolve()
    })

    expect(await screen.findByText(/^sync complete\.$/i)).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /^resend sync$/i })).not.toBeInTheDocument()
  })

  it('keeps the resend button when the resync still reports failures', async () => {
    createPermissions.mockResolvedValue(withFailures(['site-b']))
    resyncPermissions.mockResolvedValue({ syncFailures: ['site-c'] })
    render(<CreatePermissionsDialog authToken="tok" onClose={vi.fn()} onCreated={vi.fn()} />)
    await fillRequiredPickers()

    fireEvent.click(screen.getByRole('button', { name: /grant permission/i }))
    fireEvent.click(await screen.findByRole('button', { name: /^resend sync$/i }))

    await waitFor(() => expect(resyncPermissions).toHaveBeenCalledTimes(1))
    expect(await screen.findByText(/sync failed for: site-c/i)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /^resend sync$/i })).toBeEnabled()
    expect(screen.queryByText(/sync complete/i)).not.toBeInTheDocument()
  })

  it('surfaces a failed resend without losing the banner', async () => {
    createPermissions.mockResolvedValue(withFailures(['site-b']))
    resyncPermissions.mockRejectedValue(
      new AsyncJobError('boom', { code: 'internal', reason: 'internal' }),
    )
    render(<CreatePermissionsDialog authToken="tok" onClose={vi.fn()} onCreated={vi.fn()} />)
    await fillRequiredPickers()

    fireEvent.click(screen.getByRole('button', { name: /grant permission/i }))
    fireEvent.click(await screen.findByRole('button', { name: /^resend sync$/i }))

    await waitFor(() => expect(resyncPermissions).toHaveBeenCalledTimes(1))
    expect(await screen.findByText(/^boom$/i)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /^resend sync$/i })).toBeEnabled()
  })
})
