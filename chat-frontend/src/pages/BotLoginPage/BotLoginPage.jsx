import { useState } from 'react'
import { useNats } from '@/context/NatsContext'
import { botLogin } from '@/api/auth/botAuth'
import { formatAsyncJobError } from '@/api'
import './style.css'

// Bot/admin password login. On success the session bundle connects directly to
// NATS. Password rotation is handled by the bot registration web, not here.
export default function BotLoginPage() {
  const { connect } = useNats()
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  const handleLogin = async (e) => {
    e.preventDefault()
    if (!username.trim() || !password) return
    setLoading(true)
    setError(null)
    try {
      const resolved = await botLogin({ username: username.trim(), password })
      await connect({ mode: 'session', bundle: resolved })
    } catch (err) {
      setError(formatAsyncJobError(err))
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="login-page">
      <form className="login-form" onSubmit={handleLogin}>
        <h1>Chat</h1>
        <p className="login-subtitle">Bot / Admin sign in</p>

        <label htmlFor="bot-username">Username</label>
        <input id="bot-username" type="text" value={username}
          onChange={(e) => setUsername(e.target.value)} autoFocus disabled={loading} />

        <label htmlFor="bot-password">Password</label>
        <input id="bot-password" type="password" value={password}
          onChange={(e) => setPassword(e.target.value)} disabled={loading} />

        <button type="submit" disabled={loading || !username.trim() || !password}>
          {loading ? 'Signing in…' : 'Sign in'}
        </button>

        {error && <div className="login-error">{error}</div>}
      </form>
    </div>
  )
}
