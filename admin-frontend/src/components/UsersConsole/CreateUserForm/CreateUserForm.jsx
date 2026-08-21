import { useState } from 'react'
import Modal from '@/components/shared/Modal'
import { createUser } from '@/api'
import { useHandleAdminError } from '@/hooks/useHandleAdminError'
import { ROLE_OPTIONS } from '../roleOptions'

// Server-side rejections (e.g. account_exists) render via formatAsyncJobError's friendly message.
export default function CreateUserForm({ authToken, onClose, onCreated }) {
  const [account, setAccount] = useState('')
  const [engName, setEngName] = useState('')
  const [chineseName, setChineseName] = useState('')
  const [roles, setRoles] = useState([])
  const [password, setPassword] = useState('')
  const [requirePasswordChange, setRequirePasswordChange] = useState(false)
  const [error, setError] = useState(null)
  const [submitting, setSubmitting] = useState(false)
  // Set whenever the direct sync missed a site (spec R9). The INBOX snapshot is
  // the only complete lane — the durable HR feed carries identity fields only —
  // so hrSyncFailed just picks the severity: full failure vs identity-only partial.
  const [syncResult, setSyncResult] = useState(null)
  const handleAdminError = useHandleAdminError()

  const toggleRole = (role) => {
    setRoles((prev) => (prev.includes(role) ? prev.filter((r) => r !== role) : [...prev, role]))
  }

  const handleSubmit = async (e) => {
    e.preventDefault()
    if (!account.trim() || !password || roles.length === 0) {
      setError('Account, password, and at least one role are required.')
      return
    }
    setError(null)
    setSubmitting(true)
    try {
      const res = await createUser(authToken, {
        account: account.trim(),
        engName: engName.trim() || undefined,
        chineseName: chineseName.trim() || undefined,
        roles,
        password,
        requirePasswordChange,
      })
      if (res.syncFailures.length > 0) {
        setSyncResult(res)
      } else {
        onCreated()
      }
    } catch (err) {
      const message = handleAdminError(err)
      if (message !== null) setError(message)
    } finally {
      setSubmitting(false)
    }
  }

  // The user exists locally, so every exit from the notice (Done, Esc, backdrop) is onCreated —
  // the parent has to refresh its list, not just drop the dialog.
  if (syncResult) {
    return (
      <Modal onClose={onCreated} labelledBy="create-user-title">
        <h2 id="create-user-title">User created on this site</h2>
        <p className="dialog-error" role="alert">
          {syncResult.hrSyncFailed ? (
            <>
              This account did not sync to: {syncResult.syncFailures.join(', ')}. Use Resync on
              this user once those sites are reachable.
            </>
          ) : (
            <>
              Only this account&apos;s identity reached: {syncResult.syncFailures.join(', ')} —
              roles and status did not sync. Use Resync on this user to deliver them.
            </>
          )}
        </p>
        <div className="dialog-actions">
          <button type="button" onClick={onCreated}>
            Done
          </button>
        </div>
      </Modal>
    )
  }

  return (
    <Modal onClose={onClose} labelledBy="create-user-title">
      <h2 id="create-user-title">New user</h2>
      <form onSubmit={handleSubmit}>
        <label htmlFor="create-user-account">Account</label>
        <input
          id="create-user-account"
          value={account}
          onChange={(e) => setAccount(e.target.value)}
          disabled={submitting}
          autoFocus
        />

        <label htmlFor="create-user-engname">English name</label>
        <input
          id="create-user-engname"
          value={engName}
          onChange={(e) => setEngName(e.target.value)}
          disabled={submitting}
        />

        <label htmlFor="create-user-chinesename">Chinese name</label>
        <input
          id="create-user-chinesename"
          value={chineseName}
          onChange={(e) => setChineseName(e.target.value)}
          disabled={submitting}
        />

        <fieldset>
          <legend>Roles</legend>
          {ROLE_OPTIONS.map((role) => (
            <label key={role} className="dialog-checkbox">
              <input
                type="checkbox"
                checked={roles.includes(role)}
                onChange={() => toggleRole(role)}
                disabled={submitting}
              />
              {role}
            </label>
          ))}
        </fieldset>

        <label htmlFor="create-user-password">Password</label>
        <input
          id="create-user-password"
          type="password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          disabled={submitting}
        />

        <label className="dialog-checkbox">
          <input
            type="checkbox"
            checked={requirePasswordChange}
            onChange={(e) => setRequirePasswordChange(e.target.checked)}
            disabled={submitting}
          />
          Require password change on first login
        </label>

        {error && <div className="dialog-error">{error}</div>}

        <div className="dialog-actions">
          <button type="button" className="dialog-cancel" onClick={onClose} disabled={submitting}>
            Cancel
          </button>
          <button type="submit" disabled={submitting}>
            {submitting ? 'Creating…' : 'Create user'}
          </button>
        </div>
      </form>
    </Modal>
  )
}
