import { useEffect, useState } from 'react'
import { useNats } from '@/context/NatsContext'
import { getOidcManager, isSSOTokenInvalidError, redirectToReloginOnTokenInvalid } from '@/api/auth/oidcClient'
import { formatAsyncJobError } from '@/api'

// OidcCallback handles the post-redirect leg of the OIDC authorization-code
// flow. It pulls the access token from oidc-client-ts, then hands it off to
// NatsContext.connect() with mode "sso". After a successful connect, it
// rewrites the browser URL back to "/" and notifies its parent via onDone()
// so the app can re-render the main shell.
export default function OidcCallback({ onDone }) {
  const { connect } = useNats()
  const [error, setError] = useState(null)

  useEffect(() => {
    let cancelled = false

    async function run() {
      try {
        const manager = getOidcManager()
        const user = await manager.signinRedirectCallback()
        if (cancelled) return

        await connect({
          mode: 'sso',
          ssoToken: user.access_token,
          account: user.profile?.preferred_username,
        })
        if (cancelled) return

        // Replace the noisy /oidc-callback?code=... URL with a clean "/"
        // so a refresh doesn't try to re-process the (now-stale) auth code.
        window.history.replaceState({}, '', '/')

        if (typeof onDone === 'function') {
          onDone()
        }
      } catch (err) {
        if (cancelled) return
        if (isSSOTokenInvalidError(err)) {
          try {
            await redirectToReloginOnTokenInvalid()
            return
          } catch (redirectErr) {
            // Redirect failed (e.g. signinRedirect rejected) — surface the
            // error so the user isn't stuck on "Completing sign-in...".
            setError(formatAsyncJobError(redirectErr) || String(redirectErr))
            return
          }
        }
        setError(formatAsyncJobError(err) || String(err))
      }
    }

    run()

    return () => {
      cancelled = true
    }
  }, [connect, onDone])

  if (error) {
    return (
      <div className="login-page">
        <div className="login-form">
          <h1>Sign-in failed</h1>
          <div className="login-error">{error}</div>
        </div>
      </div>
    )
  }

  return (
    <div className="login-page">
      <div className="login-form">
        <p>Completing sign-in...</p>
      </div>
    </div>
  )
}
