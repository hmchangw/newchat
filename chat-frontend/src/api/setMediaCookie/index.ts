export interface SetMediaCookieArgs {
  /** Media gateway origin (NatsContext user.baseUrl). */
  baseUrl: string
  /** SSO access token (from getAccessToken). */
  ssoToken: string
}

/**
 * Establish the media download session cookie so header-less `<img>` / `<a>`
 * requests to the protected file endpoint authenticate. The backend's
 * setCookie route reads the ssoToken header and issues it back as an ssoToken
 * cookie. Best-effort: resolves false (never throws) on missing inputs or any
 * failure — the caller treats it as advisory.
 */
export async function setMediaCookie({ baseUrl, ssoToken }: SetMediaCookieArgs): Promise<boolean> {
  if (!baseUrl || !ssoToken) return false
  try {
    const resp = await fetch(`${baseUrl.replace(/\/+$/, '')}/api/v1/file/setCookie`, {
      method: 'POST',
      headers: { ssoToken },
      credentials: 'include',
    })
    return resp.ok
  } catch {
    return false
  }
}
