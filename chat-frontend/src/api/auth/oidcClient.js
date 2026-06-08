// OIDC client singleton for Keycloak (or any OIDC-compliant) login.
// Uses authorization code flow with PKCE.
import { UserManager, WebStorageStateStore } from 'oidc-client-ts'
import { OIDC_CLIENT_ID, OIDC_ISSUER_URL } from '@/lib/runtimeConfig'

let manager = null

export function getOidcManager() {
  if (manager) return manager

  const redirectUri = `${window.location.origin}/oidc-callback`

  manager = new UserManager({
    authority: OIDC_ISSUER_URL,
    client_id: OIDC_CLIENT_ID,
    redirect_uri: redirectUri,
    post_logout_redirect_uri: window.location.origin,
    response_type: 'code',
    scope: 'openid profile email',
    userStore: new WebStorageStateStore({ store: window.sessionStorage }),
  })

  return manager
}

// Test-only helper to reset the singleton between tests.
export function _resetOidcManagerForTests() {
  manager = null
}

/**
 * Reason-aware re-login redirect. Call from catch blocks when an AsyncJobError
 * carries an SSO token-expired/invalid reason. Clears any stashed OIDC session
 * state and kicks the OIDC sign-in redirect; the browser navigates away, so
 * the caller's catch returns and any subsequent setError(...) never paints.
 *
 * For dev-mode (no OIDC manager configured), no-ops — dev sessions don't carry
 * SSO tokens, so this branch shouldn't fire there anyway.
 */
export function isSSOTokenInvalidError(err) {
  if (!err || typeof err !== 'object') return false
  const reason = err.reason
  return reason === 'sso_token_expired' || reason === 'invalid_sso_token'
}

export async function redirectToReloginOnTokenInvalid() {
  try {
    // Clear oidc-client-ts's stashed user + the LoginPage's siteId stash.
    if (manager) {
      try { await manager.removeUser() } catch { /* best-effort */ }
    }
    window.sessionStorage.removeItem('oidc.siteId')
    const mgr = getOidcManager()
    await mgr.signinRedirect()
  } catch {
    // If the redirect itself fails (e.g. test envs without window.location
    // navigation), fall through; the caller's outer flow will then surface a
    // generic error.
  }
}

/**
 * Obtain a fresh SSO access token in the background using the OIDC refresh
 * token (refresh_token grant via oidc-client-ts signinSilent — no redirect,
 * no iframe). Returns the same token shape login uses (`user.access_token`,
 * see OidcCallback.jsx). Throws when silent renewal is not possible (e.g. the
 * SSO session has ended); callers fall back to redirectToReloginOnTokenInvalid().
 */
export async function renewSsoToken() {
  const mgr = getOidcManager()
  const user = await mgr.signinSilent()
  if (!user || !user.access_token) {
    throw new Error('silent renew returned no access token')
  }
  return user.access_token
}
