import { useState } from 'react'
import { AsyncJobError, createPermissions } from '@/api'
import { useHandleAdminError } from '@/hooks/useHandleAdminError'
import Modal from '@/components/shared/Modal'
import AccountPicker from '../AccountPicker'
import { PERMISSION_KEY } from '../permissionKey'
import './style.css'

const MAX_SUBJECTS = 200 // mirrors pkg/model.MaxSubjects
const MAX_REASON_RUNES = 1000 // mirrors pkg/model.MaxReasonRunes

// Grant/revoke form, opened as a dialog from PermissionsPage's "Create" button. On a successful
// submit it reports up via `onCreated` immediately (PermissionsPage refreshes the list right
// away) but — unlike UsersConsole's CreateUserForm/onCreated, which the parent uses to also
// close the dialog — this dialog stays open and swaps to a result view so the admin can read the
// created/duplicatesIgnored summary; `onClose` (via the result view's Close button, Cancel, Esc,
// or the backdrop) is what actually dismisses it.
export default function CreatePermissionsDialog({ authToken, onClose, onCreated }) {
  const handleAdminError = useHandleAdminError()

  const [mode, setMode] = useState('grant')
  const [subjectAccounts, setSubjectAccounts] = useState([])
  const [effectiveFrom, setEffectiveFrom] = useState('')
  const [expiresAt, setExpiresAt] = useState('')
  const [applicantAccount, setApplicantAccount] = useState('')
  const [approverAccount, setApproverAccount] = useState('')
  const [reason, setReason] = useState('')
  const [submitting, setSubmitting] = useState(false)
  const [formError, setFormError] = useState(null)
  const [formErrorMetadata, setFormErrorMetadata] = useState(null)
  const [result, setResult] = useState(null)

  const subjectCount = subjectAccounts.length
  const tooManySubjects = subjectCount > MAX_SUBJECTS
  const reasonRuneCount = [...reason].length
  const clientInvalid =
    subjectCount === 0 ||
    tooManySubjects ||
    reasonRuneCount > MAX_REASON_RUNES ||
    !applicantAccount ||
    !approverAccount ||
    (mode === 'grant' && !expiresAt)

  // Builds the wire payload field-by-field so an omitted window field or a blank reason is
  // truly absent from the object (and therefore from the JSON body) — never sent as "".
  const buildPayload = () => {
    const payload = {
      permission: PERMISSION_KEY,
      subjectAccounts,
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
            {result.duplicatesIgnored.length > 0 && (
              <p className="permissions-duplicates">
                Duplicates ignored: {result.duplicatesIgnored.join(', ')}
              </p>
            )}
          </div>
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

          <AccountPicker
            id="permissions-subjects"
            label="Subject accounts"
            authToken={authToken}
            multiple
            value={subjectAccounts}
            onChange={setSubjectAccounts}
            disabled={submitting}
          />
          <span
            className={`permissions-subjects-count ${tooManySubjects ? 'is-over-limit' : ''}`}
          >
            {subjectCount} account{subjectCount === 1 ? '' : 's'} (max {MAX_SUBJECTS})
          </span>
          {tooManySubjects && (
            <div className="permissions-field-error">
              Enter at most {MAX_SUBJECTS} accounts — you have {subjectCount}.
            </div>
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
              {formErrorMetadata && Object.keys(formErrorMetadata).length > 0 && (
                <ul className="permissions-metadata-list">
                  {Object.entries(formErrorMetadata).map(([key, value]) => (
                    <li key={key}>
                      {key}: {value}
                    </li>
                  ))}
                </ul>
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
                : mode === 'grant'
                  ? 'Grant permission'
                  : 'Revoke permission'}
            </button>
          </div>
        </form>
      )}
    </Modal>
  )
}
