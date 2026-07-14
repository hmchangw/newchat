import { useState } from 'react'
import './style.css'

// Forced first-login password change. Presentational: validates client-side (non-empty /
// match / differ-from-old) then hands the result to onSubmit — the backend has no strength checks.
export default function ChangePasswordForm({ onSubmit, error, loading }) {
  const [oldPassword, setOld] = useState('')
  const [newPassword, setNew] = useState('')
  const [confirmPassword, setConfirm] = useState('')
  const [localError, setLocalError] = useState(null)

  const handleSubmit = (e) => {
    e.preventDefault()
    if (!oldPassword || !newPassword || !confirmPassword) {
      setLocalError('All fields are required')
      return
    }
    if (newPassword !== confirmPassword) {
      setLocalError('New passwords do not match')
      return
    }
    if (newPassword === oldPassword) {
      setLocalError('New password must differ from the current password')
      return
    }
    setLocalError(null)
    onSubmit({ oldPassword, newPassword })
  }

  return (
    <div className="login-page">
      <form className="login-form" onSubmit={handleSubmit}>
        <h1>Change Password</h1>
        <p className="login-subtitle">Set a new password to continue</p>

        <label htmlFor="old-password">Current password</label>
        <input id="old-password" type="password" value={oldPassword} autoComplete="current-password"
          onChange={(e) => setOld(e.target.value)} autoFocus disabled={loading} />

        <label htmlFor="new-password">New password</label>
        <input id="new-password" type="password" value={newPassword} autoComplete="new-password"
          onChange={(e) => setNew(e.target.value)} disabled={loading} />

        <label htmlFor="confirm-password">Confirm new password</label>
        <input id="confirm-password" type="password" value={confirmPassword} autoComplete="new-password"
          onChange={(e) => setConfirm(e.target.value)} disabled={loading} />

        <button type="submit" disabled={loading}>
          {loading ? 'Saving…' : 'Change password'}
        </button>

        {(localError || error) && <div className="login-error" role="alert">{localError || error}</div>}
      </form>
    </div>
  )
}
