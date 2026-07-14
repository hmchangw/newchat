import { useState } from 'react'
import { useAuth } from '@/context/AuthContext'
import { botLogin, changePassword, formatAsyncJobError } from '@/api'
import ChangePasswordForm from '@/pages/ChangePasswordForm'
import './style.css'

// Login, then a forced change-password step if required; authToken stays valid across
// the change so the admin lands directly in the app with no re-login.
export default function AdminLoginPage() {
  const { login } = useAuth()
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [bundle, setBundle] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  const handleLogin = async (e) => {
    e.preventDefault()
    if (!username.trim() || !password) return
    setLoading(true)
    setError(null)
    try {
      const resolved = await botLogin({ username: username.trim(), password })
      if (resolved.requirePasswordChange) {
        setBundle(resolved)
        return
      }
      login(resolved)
    } catch (err) {
      setError(formatAsyncJobError(err))
    } finally {
      setLoading(false)
    }
  }

  const handleChangePassword = async ({ oldPassword, newPassword }) => {
    setLoading(true)
    setError(null)
    try {
      await changePassword({ authToken: bundle.authToken, oldPassword, newPassword })
      login(bundle)
    } catch (err) {
      setError(formatAsyncJobError(err))
    } finally {
      setLoading(false)
    }
  }

  if (bundle?.requirePasswordChange) {
    return <ChangePasswordForm onSubmit={handleChangePassword} error={error} loading={loading} />
  }

  return (
    <div className="login-page">
      <form className="login-form" onSubmit={handleLogin}>
        <h1>Admin</h1>
        <p className="login-subtitle">Admin sign in</p>

        <label htmlFor="admin-username">Username</label>
        <input id="admin-username" type="text" value={username} autoComplete="username"
          onChange={(e) => setUsername(e.target.value)} autoFocus disabled={loading} />

        <label htmlFor="admin-password">Password</label>
        <input id="admin-password" type="password" value={password} autoComplete="current-password"
          onChange={(e) => setPassword(e.target.value)} disabled={loading} />

        <button type="submit" disabled={loading || !username.trim() || !password}>
          {loading ? 'Signing in…' : 'Sign in'}
        </button>

        {error && <div className="login-error" role="alert">{error}</div>}
      </form>
    </div>
  )
}
