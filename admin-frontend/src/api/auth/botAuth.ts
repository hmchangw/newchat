import { PORTAL_URL, ADMIN_SERVICE_URL } from '@/lib/runtimeConfig'
import { parseHttpEnvelopeError } from '../_transport/httpEnvelope'

/** Session bundle from `botLogin`; only `{authToken, account, siteId}` is ever exposed outside api/auth (see `AuthContext`). */
export interface Bundle {
  userId?: string
  authToken: string
  account: string
  siteId: string
  requirePasswordChange: boolean
}

/** Bot/admin password login via portal-service. @throws {AsyncJobError} on a non-2xx response. */
export async function botLogin({
  username,
  password,
}: {
  username: string
  password: string
}): Promise<Bundle> {
  const resp = await fetch(`${PORTAL_URL}/api/v1/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username, password }),
  })
  if (!resp.ok) await parseHttpEnvelopeError(resp, 'Login failed')
  return (await resp.json()) as Bundle
}

/** Password rotation against admin-service; caller's session stays valid, server revokes all others. */
export async function changePassword({
  authToken,
  oldPassword,
  newPassword,
}: {
  authToken: string
  oldPassword: string
  newPassword: string
}): Promise<void> {
  const resp = await fetch(`${ADMIN_SERVICE_URL}/v1/password/change`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${authToken}` },
    body: JSON.stringify({ oldPassword, newPassword }),
  })
  if (!resp.ok) await parseHttpEnvelopeError(resp, 'Password change failed')
}
