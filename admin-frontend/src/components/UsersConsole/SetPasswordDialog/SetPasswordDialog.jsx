import { useState } from 'react'
import Modal from '@/components/shared/Modal'
import { setPassword } from '@/api'
import { useHandleAdminError } from '@/hooks/useHandleAdminError'

// On success reports up via `onUpdated` — UsersPage owns closing the dialog and re-fetching.
export default function SetPasswordDialog({ authToken, user, onClose, onUpdated }) {
  const [newPassword, setNewPassword] = useState('')
  const [confirmPassword, setConfirmPassword] = useState('')
  const [requirePasswordChange, setRequirePasswordChange] = useState(false)
  const [error, setError] = useState(null)
  const [submitting, setSubmitting] = useState(false)
  const handleAdminError = useHandleAdminError()

  const handleSubmit = async (e) => {
    e.preventDefault()
    if (!newPassword || !confirmPassword) {
      setError('Enter and confirm the new password.')
      return
    }
    if (newPassword !== confirmPassword) {
      setError('Passwords do not match.')
      return
    }
    setError(null)
    setSubmitting(true)
    try {
      await setPassword(authToken, user.account, { newPassword, requirePasswordChange })
      onUpdated()
    } catch (err) {
      const message = handleAdminError(err)
      if (message !== null) setError(message)
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <Modal onClose={onClose} labelledBy="set-password-title">
      <h2 id="set-password-title">Set password for {user.account}</h2>
      <form onSubmit={handleSubmit}>
        <label htmlFor="set-password-new">New password</label>
        <input
          id="set-password-new"
          type="password"
          value={newPassword}
          autoComplete="new-password"
          onChange={(e) => setNewPassword(e.target.value)}
          disabled={submitting}
          autoFocus
        />

        <label htmlFor="set-password-confirm">Confirm new password</label>
        <input
          id="set-password-confirm"
          type="password"
          value={confirmPassword}
          autoComplete="new-password"
          onChange={(e) => setConfirmPassword(e.target.value)}
          disabled={submitting}
        />

        <label className="dialog-checkbox">
          <input
            type="checkbox"
            checked={requirePasswordChange}
            onChange={(e) => setRequirePasswordChange(e.target.checked)}
            disabled={submitting}
          />
          Force change on next login
        </label>

        {error && <div className="dialog-error">{error}</div>}

        <div className="dialog-actions">
          <button type="button" className="dialog-cancel" onClick={onClose} disabled={submitting}>
            Cancel
          </button>
          <button type="submit" disabled={submitting}>
            {submitting ? 'Saving…' : 'Set password'}
          </button>
        </div>
      </form>
    </Modal>
  )
}
