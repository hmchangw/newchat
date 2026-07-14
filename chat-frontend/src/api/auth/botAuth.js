import { PORTAL_URL } from '@/lib/runtimeConfig'
import { AsyncJobError, ASYNC_JOB_ERROR_KINDS } from '@/api'

// Parses the {code, reason?, error, metadata?} errcode envelope into an AsyncJobError.
async function throwHttpEnvelopeError(resp, fallbackMsg) {
  const body = await resp.json().catch(() => ({}))
  throw new AsyncJobError(
    body.error || `${fallbackMsg}: ${resp.status}`,
    ASYNC_JOB_ERROR_KINDS.SyncError,
    { code: body.code, reason: body.reason, metadata: body.metadata },
  )
}

/**
 * Bot/admin password login via portal-service; returns the merged session+home-site bundle
 * so the caller needs no separate /api/userInfo discovery call.
 * @param {{username: string, password: string}} args
 * @returns {Promise<{userId: string, authToken: string, account: string, siteId: string, authServiceUrl: string, baseUrl: string, natsUrl: string, requirePasswordChange: boolean}>}
 */
export async function botLogin({ username, password }) {
  const resp = await fetch(`${PORTAL_URL}/api/v1/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username, password }),
  })
  if (!resp.ok) await throwHttpEnvelopeError(resp, 'Login failed')
  return resp.json()
}
