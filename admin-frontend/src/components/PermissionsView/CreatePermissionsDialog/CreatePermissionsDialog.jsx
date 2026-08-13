import { useMemo, useState } from 'react'
import { AsyncJobError, createPermissions, resyncPermissions } from '@/api'
import { useHandleAdminError } from '@/hooks/useHandleAdminError'
import Modal from '@/components/shared/Modal'
import AccountPicker from '@/components/shared/AccountPicker'
import { PERMISSION_KEY } from '../permissionKey'
import { parsePastedAccounts } from './parseAccounts'
import './style.css'

const MAX_REASON_RUNES = 1000 // mirrors pkg/model.MaxReasonRunes
const DUPLICATES_INLINE_MAX = 20 // longer lists collapse behind a count

// Grant/revoke form, opened as a dialog from PermissionsPage's "Create" button. On a successful
// submit it reports up via `onCreated` immediately (PermissionsPage refreshes the list right
// away) but — unlike UsersConsole's CreateUserForm/onCreated, which the parent uses to also
// close the dialog — this dialog stays open and swaps to a result view so the admin can read the
// created/duplicatesIgnored summary; `onClose` (via the result view's Close button, Cancel, Esc,
// or the backdrop) is what actually dismisses it.
export default function CreatePermissionsDialog({ authToken, onClose, onCreated }) {
  const handleAdminError = useHandleAdminError()

  const [mode, setMode] = useState('grant')
  const [inputMode, setInputMode] = useState('pick')
  const [subjectAccounts, setSubjectAccounts] = useState([])
  const [pasteText, setPasteText] = useState('')
  const [effectiveFrom, setEffectiveFrom] = useState('')
  const [expiresAt, setExpiresAt] = useState('')
  const [applicantAccount, setApplicantAccount] = useState('')
  const [approverAccount, setApproverAccount] = useState('')
  const [reason, setReason] = useState('')
  const [submitting, setSubmitting] = useState(false)
  const [formError, setFormError] = useState(null)
  const [formErrorMetadata, setFormErrorMetadata] = useState(null)
  const [result, setResult] = useState(null)
  // The request as submitted — resync replays that, not the form fields as they stand now.
  const [lastSubmitted, setLastSubmitted] = useState(null)
  const [resyncing, setResyncing] = useState(false)

  // The two subject inputs never merge: only the active mode's list is submitted.
  const parsed = useMemo(() => parsePastedAccounts(pasteText), [pasteText])
  const effectiveAccounts = inputMode === 'paste' ? parsed.accounts : subjectAccounts
  const reasonRuneCount = [...reason].length
  const clientInvalid =
    effectiveAccounts.length === 0 ||
    reasonRuneCount > MAX_REASON_RUNES ||
    !applicantAccount ||
    !approverAccount ||
    (mode === 'grant' && !expiresAt)

  // Builds the wire payload field-by-field so an omitted window field or a blank reason is
  // truly absent from the object (and therefore from the JSON body) — never sent as "".
  const buildPayload = () => {
    const payload = {
      permission: PERMISSION_KEY,
      subjectAccounts: effectiveAccounts,
      granted: mode === 'grant',
    }
    if (mode === 'grant') {
      if (effectiveFrom) payload.effectiveFrom = effectiveFrom
      payload.expiresAt = expiresAt
    }
    payload.applicantAccount = applicantAccount
    payload.approverAccount = approverAccount
    if (reason) payload.reason = reason
    return payload
  }

  const handleSubmit = async (e) => {
    e.preventDefault()
    if (clientInvalid || submitting) return
    setSubmitting(true)
    setFormError(null)
    setFormErrorMetadata(null)
    try {
      const response = await createPermissions(authToken, buildPayload())
      setResult(response)
      setLastSubmitted({ permission: PERMISSION_KEY, accounts: effectiveAccounts })
      onCreated()
    } catch (err) {
      const message = handleAdminError(err)
      if (message !== null) {
        setFormError(message)
        if (err instanceof AsyncJobError && err.metadata) setFormErrorMetadata(err.metadata)
      }
    } finally {
      setSubmitting(false)
    }
  }

  // Re-delivers the recorded state to every site; writes nothing, so it is safe to press again.
  const handleResync = async () => {
    if (!lastSubmitted || resyncing) return
    setResyncing(true)
    setFormError(null)
    try {
      const r = await resyncPermissions(authToken, lastSubmitted)
      // Empty (not absent) syncFailures is what renders "Sync complete." — the server omits the
      // field when it has nothing to report, so only this path can produce [].
      setResult({ ...result, syncFailures: r.syncFailures ?? [] })
    } catch (err) {
      const message = handleAdminError(err)
      if (message !== null) setFormError(message)
    } finally {
      setResyncing(false)
    }
  }

  // The accounts the server rejected, echoed back joined with ", " (null when it reported none).
  const offenders =
    typeof formErrorMetadata?.accounts === 'string' ? formErrorMetadata.accounts : null

  // Drops those accounts from whichever input is active, so the admin can resubmit without
  // hand-editing.
  const stripOffenders = () => {
    const drop = new Set(parsePastedAccounts(offenders).accounts)
    if (inputMode === 'paste') {
      setPasteText(parsed.accounts.filter((a) => !drop.has(a)).join('\n'))
    } else {
      setSubjectAccounts(subjectAccounts.filter((a) => !drop.has(a)))
    }
    setFormError(null)
    setFormErrorMetadata(null)
  }

  return (
    <Modal onClose={onClose} labelledBy="create-permissions-title">
      <h2 id="create-permissions-title">Grant or revoke permission</h2>

      {result ? (
        <div>
          <div className="permissions-result dialog-success">
            <p>
              {mode === 'grant'
                ? `Created ${result.created} grant${result.created === 1 ? '' : 's'}.`
                : `Recorded ${result.created} revocation${result.created === 1 ? '' : 's'}.`}
            </p>
            {result.duplicatesIgnored.length > DUPLICATES_INLINE_MAX ? (
              <details className="permissions-duplicates">
                <summary>{result.duplicatesIgnored.length} duplicates ignored</summary>
                <p>{result.duplicatesIgnored.join(', ')}</p>
              </details>
            ) : (
              result.duplicatesIgnored.length > 0 && (
                <p className="permissions-duplicates">
                  Duplicates ignored: {result.duplicatesIgnored.join(', ')}
                </p>
              )
            )}
          </div>

          {result.syncFailures?.length > 0 ? (
            <div className="permissions-sync-warning">
              <p>
                Recorded and effective at this site, but sync failed for:{' '}
                {result.syncFailures.join(', ')}. Resend delivers the recorded state again —
                safe to repeat.
              </p>
              <button type="button" className="btn" onClick={handleResync} disabled={resyncing}>
                {resyncing ? 'Resending…' : 'Resend sync'}
              </button>
            </div>
          ) : result.syncFailures?.length === 0 ? (
            <p className="permissions-sync-ok">Sync complete.</p>
          ) : null}

          {formError && <p className="dialog-error">{formError}</p>}

          <div className="dialog-actions">
            <button type="button" className="dialog-cancel" onClick={onClose}>
              Close
            </button>
          </div>
        </div>
      ) : (
        <form onSubmit={handleSubmit}>
          <p className="permissions-form-subtitle">Permission: {PERMISSION_KEY}</p>

          <div className="permissions-mode-group" role="radiogroup" aria-label="Grant or revoke">
            <label htmlFor="permissions-mode-grant" className="permissions-mode-option">
              <input
                type="radio"
                id="permissions-mode-grant"
                name="permissions-mode"
                value="grant"
                checked={mode === 'grant'}
                onChange={() => setMode('grant')}
                disabled={submitting}
              />
              Grant
            </label>
            <label htmlFor="permissions-mode-revoke" className="permissions-mode-option">
              <input
                type="radio"
                id="permissions-mode-revoke"
                name="permissions-mode"
                value="revoke"
                checked={mode === 'revoke'}
                onChange={() => setMode('revoke')}
                disabled={submitting}
              />
              Revoke
            </label>
          </div>

          <div
            className="permissions-mode-group"
            role="radiogroup"
            aria-label="Subject input mode"
          >
            <label htmlFor="permissions-input-mode-pick" className="permissions-mode-option">
              <input
                type="radio"
                id="permissions-input-mode-pick"
                name="permissions-input-mode"
                value="pick"
                checked={inputMode === 'pick'}
                onChange={() => setInputMode('pick')}
                disabled={submitting}
              />
              Pick ({subjectAccounts.length})
            </label>
            <label htmlFor="permissions-input-mode-paste" className="permissions-mode-option">
              <input
                type="radio"
                id="permissions-input-mode-paste"
                name="permissions-input-mode"
                value="paste"
                checked={inputMode === 'paste'}
                onChange={() => setInputMode('paste')}
                disabled={submitting}
              />
              Paste list ({parsed.accounts.length})
            </label>
          </div>

          {inputMode === 'pick' ? (
            <AccountPicker
              id="permissions-subjects"
              label="Subject accounts"
              authToken={authToken}
              multiple
              value={subjectAccounts}
              onChange={setSubjectAccounts}
              disabled={submitting}
            />
          ) : (
            <>
              <label htmlFor="permissions-paste">Subject accounts (paste list)</label>
              <textarea
                id="permissions-paste"
                className="permissions-paste-textarea"
                value={pasteText}
                onChange={(e) => setPasteText(e.target.value)}
                disabled={submitting}
                placeholder="Accounts separated by spaces, commas, or newlines"
              />
              <span className="permissions-subjects-count">
                {parsed.accounts.length} account{parsed.accounts.length === 1 ? '' : 's'}
                {parsed.duplicates > 0
                  ? ` (${parsed.duplicates} duplicate${parsed.duplicates === 1 ? '' : 's'} removed)`
                  : ''}
              </span>
            </>
          )}

          {mode === 'grant' && (
            <div className="permissions-date-row">
              <div className="permissions-date-field">
                <label htmlFor="permissions-effective-from">Effective from (optional)</label>
                <input
                  type="date"
                  id="permissions-effective-from"
                  value={effectiveFrom}
                  onChange={(e) => setEffectiveFrom(e.target.value)}
                  disabled={submitting}
                />
              </div>
              <div className="permissions-date-field">
                <label htmlFor="permissions-expires-at">Expires at</label>
                <input
                  type="date"
                  id="permissions-expires-at"
                  value={expiresAt}
                  onChange={(e) => setExpiresAt(e.target.value)}
                  disabled={submitting}
                />
              </div>
            </div>
          )}

          <AccountPicker
            id="permissions-applicant"
            label="Applicant account"
            authToken={authToken}
            value={applicantAccount}
            onChange={setApplicantAccount}
            disabled={submitting}
          />

          <AccountPicker
            id="permissions-approver"
            label="Approver account"
            authToken={authToken}
            value={approverAccount}
            onChange={setApproverAccount}
            disabled={submitting}
          />

          <label htmlFor="permissions-reason">Reason (optional)</label>
          <textarea
            id="permissions-reason"
            className="permissions-reason-textarea"
            value={reason}
            onChange={(e) => setReason(e.target.value)}
            disabled={submitting}
          />
          <span className="permissions-reason-count">
            {reasonRuneCount} / {MAX_REASON_RUNES}
          </span>

          {formError && (
            <div className="dialog-error">
              <p>{formError}</p>
              {offenders && (
                <>
                  <p className="permissions-metadata-accounts">Accounts: {offenders}</p>
                  <button
                    type="button"
                    className="btn permissions-strip-offenders"
                    onClick={stripOffenders}
                  >
                    Remove these accounts
                  </button>
                </>
              )}
            </div>
          )}

          <div className="dialog-actions">
            <button type="button" className="dialog-cancel" onClick={onClose} disabled={submitting}>
              Cancel
            </button>
            <button type="submit" disabled={submitting || clientInvalid}>
              {submitting
                ? 'Submitting…'
                : `${mode === 'grant' ? 'Grant permission to' : 'Revoke permission from'} ${
                    effectiveAccounts.length
                  } account${effectiveAccounts.length === 1 ? '' : 's'}`}
            </button>
          </div>
        </form>
      )}
    </Modal>
  )
}
