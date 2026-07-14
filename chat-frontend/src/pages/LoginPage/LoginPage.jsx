import { useEffect, useRef, useState } from 'react'
import { useNats } from '@/context/NatsContext'
import { DEV_MODE } from '@/lib/runtimeConfig'
import { getOidcManager, isSSOTokenInvalidError, redirectToReloginOnTokenInvalid } from '@/api/auth/oidcClient'
import { formatAsyncJobError } from '@/api'
import './style.css'

export default function LoginPage() {
  const { connect, error: natsError } = useNats()

  const [account, setAccount] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const didAutoRedirect = useRef(false)

  const handleDevSubmit = async (e) => {
    e.preventDefault()
    if (!account.trim()) return

    setLoading(true)
    setError(null)
    try {
      await connect({
        mode: 'dev',
        account: account.trim(),
      })
    } catch (err) {
      if (isSSOTokenInvalidError(err)) {
        await redirectToReloginOnTokenInvalid()
        return
      }
      setError(formatAsyncJobError(err))
    } finally {
      setLoading(false)
    }
  }

  const handleKeycloakLogin = async () => {
    setLoading(true)
    setError(null)
    try {
      const manager = getOidcManager()
      await manager.signinRedirect()
      // Browser navigates away — code below this point is unreachable in prod.
    } catch (err) {
      if (isSSOTokenInvalidError(err)) {
        await redirectToReloginOnTokenInvalid()
        return
      }
      setError(formatAsyncJobError(err))
      setLoading(false)
    }
  }

  // Production: a visitor who lands here has no live session, so send them
  // straight to Keycloak instead of making them click. After login the browser
  // returns via /oidc-callback and connects through the portal. Fire once per
  // mount; the ref survives a StrictMode double-invoke. Dev mode keeps the
  // account form (no IdP), so it never auto-redirects.
  useEffect(() => {
    if (DEV_MODE) return
    if (didAutoRedirect.current) return
    didAutoRedirect.current = true
    handleKeycloakLogin()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  if (DEV_MODE) {
    return (
      <div className="login-page">
        <form className="login-form" onSubmit={handleDevSubmit}>
          <h1>Chat</h1>
          <p className="login-subtitle">Dev Mode Login</p>

          <label htmlFor="account">Account</label>
          <input
            id="account"
            type="text"
            value={account}
            onChange={(e) => setAccount(e.target.value)}
            placeholder="e.g. alice"
            autoFocus
            disabled={loading}
          />

          <button type="submit" disabled={loading || !account.trim()}>
            {loading ? 'Connecting...' : 'Connect'}
          </button>

          {(error || natsError) && (
            <div className="login-error">{error || natsError}</div>
          )}
        </form>
      </div>
    )
  }

  return (
    <div className="login-page">
      <div className="login-form">
        <h1>Chat</h1>
        <p className="login-subtitle">Sign in with Keycloak</p>

        <button type="button" onClick={handleKeycloakLogin} disabled={loading}>
          {loading ? 'Redirecting...' : 'Sign in with Keycloak'}
        </button>

        {(error || natsError) && (
          <div className="login-error">{error || natsError}</div>
        )}
      </div>
    </div>
  )
}
