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
      await createUser(authToken, {
        account: account.trim(),
        engName: engName.trim() || undefined,
        chineseName: chineseName.trim() || undefined,
        roles,
        password,
        requirePasswordChange,
      })
      onCreated()
    } catch (err) {
      const message = handleAdminError(err)
      if (message !== null) setError(message)
    } finally {
      setSubmitting(false)
    }
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
