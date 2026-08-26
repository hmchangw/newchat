// Typed REST client for admin-service. Every call is Bearer-authed; non-2xx
// responses throw `AsyncJobError` via `parseHttpEnvelopeError`.

import { ADMIN_SERVICE_URL } from '@/lib/runtimeConfig'
import { AsyncJobError, parseHttpEnvelopeError } from '@/api'

/** Admin-facing user projection (mirrors admin-service's `userView` — never the bcrypt hash);
 * `normalizeUser` fills defaults for the server's `omitempty` fields. */
export interface AdminUser {
  id: string
  account: string
  siteId: string
  engName: string
  chineseName: string
  roles: string[]
  active: boolean
  requirePasswordChange: boolean
}

/** Safe projection of a session (mirrors admin-service's `sessionView`). */
export interface AdminSession {
  id: string
  userId: string
  account: string
  siteId: string
  issuedAt: number
}

/** One mutating admin action (mirrors admin-service's `AuditEntry`). */
export interface AuditEntry {
  id: string
  actorUserId: string
  actorAccount: string
  action: string
  targetUserId?: string
  targetAccount?: string
  details?: Record<string, string>
  siteId: string
  timestamp: number
}

export interface ListUsersParams {
  q?: string
  page?: number
  limit?: number
}

export interface CreateUserInput {
  account: string
  engName?: string
  chineseName?: string
  roles: string[]
  password: string
  requirePasswordChange?: boolean
}

export interface UpdateUserPatch {
  engName?: string
  chineseName?: string
  roles?: string[]
  active?: boolean
}

export interface SetPasswordInput {
  newPassword: string
  requirePasswordChange?: boolean
}

export interface AuditFilter {
  targetAccount?: string
  actor?: string
  action?: string
  page?: number
  limit?: number
}

/** One row of the permission ledger (mirrors admin-service's `permissionGrantView`). */
export interface PermissionGrantView {
  id: string
  permission: string
  subjectAccount: string
  granted: boolean
  effectiveFrom?: string // "2026-09-01"
  expiresAt?: string // "2026-12-31"
  applicantAccount: string
  approverAccount: string
  reason: string
  recordedBy: string
  recordedAt: string
}

export interface CreatePermissionsRequest {
  permission: string
  subjectAccounts: string[]
  granted: boolean
  effectiveFrom?: string
  expiresAt?: string
  applicantAccount: string
  approverAccount: string
  reason?: string
}

/** `syncFailures` lists the sites whose whitelist sync failed — the ledger write still
 * succeeded (201), so those sites need a `resyncPermissions` follow-up. */
export interface CreatePermissionsResponse {
  created: number
  duplicatesIgnored: string[]
  syncFailures?: string[]
}

export interface ResyncPermissionsRequest {
  permission: string
  accounts: string[]
}

export interface ResyncPermissionsResponse {
  syncFailures?: string[]
}

export interface ListPermissionsResponse {
  currentlyGranted?: boolean
  entries: PermissionGrantView[]
  total: number
}

/** `subjectAccount` and `permission` are independently optional and combinable; the server
 * includes `currentlyGranted` in the response only when both are given. */
export interface ListPermissionsParams {
  subjectAccount?: string
  permission?: string
  page?: number
  limit?: number
}

/** Raw shape of admin-service's `userView` as it appears on the wire — the
 * `omitempty` fields may be absent; `normalizeUser` fills the defaults. */
interface UserViewWire {
  id: string
  account: string
  siteId: string
  engName?: string
  chineseName?: string
  roles?: string[]
  active?: boolean
  requirePasswordChange?: boolean
}

function normalizeUser(raw: UserViewWire): AdminUser {
  return {
    id: raw.id,
    account: raw.account,
    siteId: raw.siteId,
    engName: raw.engName ?? '',
    chineseName: raw.chineseName ?? '',
    roles: raw.roles ?? [],
    active: raw.active ?? true,
    requirePasswordChange: raw.requirePasswordChange ?? false,
  }
}

/** Builds a `?a=b&c=d` query string, omitting `undefined`/empty params; returns `''` when none remain. */
function buildQuery(params: Record<string, string | number | undefined>): string {
  const usp = new URLSearchParams()
  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === '') continue
    usp.set(key, String(value))
  }
  const qs = usp.toString()
  return qs ? `?${qs}` : ''
}

/** Shared fetch wrapper: Bearer + JSON headers, throws `AsyncJobError` on a non-2xx response. */
async function adminFetch<T>(
  authToken: string,
  method: string,
  path: string,
  body?: unknown,
): Promise<T> {
  const resp = await fetch(`${ADMIN_SERVICE_URL}/v1/admin${path}`, {
    method,
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${authToken}`,
    },
    body: body !== undefined ? JSON.stringify(body) : undefined,
  })
  if (!resp.ok) await parseHttpEnvelopeError(resp, `admin request failed: ${method} ${path}`)
  return (await resp.json()) as T
}

/** @throws {AsyncJobError} on a non-2xx response. */
export async function listUsers(
  authToken: string,
  params: ListUsersParams = {},
): Promise<{ users: AdminUser[]; total: number }> {
  const qs = buildQuery({ q: params.q, page: params.page, limit: params.limit })
  const raw = await adminFetch<{ users: UserViewWire[]; total: number }>(
    authToken,
    'GET',
    `/users${qs}`,
  )
  return { users: raw.users.map(normalizeUser), total: raw.total }
}

/** @throws {AsyncJobError} on a non-2xx response (e.g. `user_not_found`). */
export async function getUser(authToken: string, account: string): Promise<AdminUser> {
  const raw = await adminFetch<UserViewWire>(authToken, 'GET', `/users/${encodeURIComponent(account)}`)
  return normalizeUser(raw)
}

export interface CreateUserResult {
  user: AdminUser
  syncFailures: string[]
  hrSyncFailed: boolean
}

/** @throws {AsyncJobError} on a non-2xx response (e.g. `account_exists`). A 2xx
 * with `syncFailures`/`hrSyncFailed` means the account committed locally but
 * some cross-site replication did not land — the caller must show it. */
export async function createUser(
  authToken: string,
  input: CreateUserInput,
): Promise<CreateUserResult> {
  const raw = await adminFetch<UserViewWire & { syncFailures?: string[]; hrSyncFailed?: boolean }>(
    authToken,
    'POST',
    '/users',
    input,
  )
  return {
    user: normalizeUser(raw),
    syncFailures: raw.syncFailures ?? [],
    hrSyncFailed: raw.hrSyncFailed ?? false,
  }
}

export interface UpdateUserResult {
  syncFailures: string[]
}

/** Applies a partial update. A 2xx with `syncFailures` committed locally but
 * did not reach those sites — the caller must show it. The server replies
 * `{status:"ok"}`, not the user — follow up with `getUser` for the fresh record. */
export async function updateUser(
  authToken: string,
  account: string,
  patch: UpdateUserPatch,
): Promise<UpdateUserResult> {
  const raw = await adminFetch<{ status: string; syncFailures?: string[] }>(
    authToken,
    'PATCH',
    `/users/${encodeURIComponent(account)}`,
    patch,
  )
  return { syncFailures: raw.syncFailures ?? [] }
}

export interface ResyncUserResult {
  syncFailures: string[]
  hrSyncFailed: boolean
}

/** Re-delivers the current account state on both sync lanes (durable HR
 * bootstrap + direct snapshot to every remote site). Re-delivery only — the
 * server writes nothing. Home-site accounts only; foreign replicas 404. */
export async function resyncUser(authToken: string, account: string): Promise<ResyncUserResult> {
  const raw = await adminFetch<{
    status: string
    syncFailures?: string[]
    hrSyncFailed?: boolean
  }>(authToken, 'POST', `/users/${encodeURIComponent(account)}/resync`)
  return { syncFailures: raw.syncFailures ?? [], hrSyncFailed: raw.hrSyncFailed ?? false }
}

/** Sets a new password; sent over the wire as `password` (admin-service's json tag). */
export async function setPassword(
  authToken: string,
  account: string,
  input: SetPasswordInput,
): Promise<void> {
  await adminFetch<{ status: string }>(
    authToken,
    'POST',
    `/users/${encodeURIComponent(account)}/password`,
    {
      password: input.newPassword,
      requirePasswordChange: input.requirePasswordChange,
    },
  )
}

/** @throws {AsyncJobError} on a non-2xx response. */
export async function listSessions(authToken: string, account: string): Promise<AdminSession[]> {
  const raw = await adminFetch<{ sessions: AdminSession[] }>(
    authToken,
    'GET',
    `/sessions${buildQuery({ account })}`,
  )
  return raw.sessions
}

/** @throws {AsyncJobError} on a non-2xx response. */
export async function revokeAllSessions(authToken: string, account: string): Promise<void> {
  await adminFetch<{ status: string }>(authToken, 'DELETE', `/sessions${buildQuery({ account })}`)
}

/** @throws {AsyncJobError} on a non-2xx response. */
export async function revokeSession(
  authToken: string,
  account: string,
  sessionId: string,
): Promise<void> {
  await adminFetch<{ status: string }>(
    authToken,
    'DELETE',
    `/sessions/${encodeURIComponent(sessionId)}${buildQuery({ account })}`,
  )
}

/** @throws {AsyncJobError} on a non-2xx response. */
export async function listAudit(
  authToken: string,
  filter: AuditFilter = {},
): Promise<{ entries: AuditEntry[]; total: number }> {
  const qs = buildQuery({
    targetAccount: filter.targetAccount,
    actor: filter.actor,
    action: filter.action,
    page: filter.page,
    limit: filter.limit,
  })
  return adminFetch<{ entries: AuditEntry[]; total: number }>(authToken, 'GET', `/audit${qs}`)
}

/** @throws {AsyncJobError} on a non-2xx response (e.g. `unknown_accounts`, `inactive_subject`). */
export async function createPermissions(
  authToken: string,
  body: CreatePermissionsRequest,
): Promise<CreatePermissionsResponse> {
  return adminFetch<CreatePermissionsResponse>(authToken, 'POST', '/permissions', body)
}

/** Replays the current ledger state for `accounts` to every site's whitelist.
 * @throws {AsyncJobError} on a non-2xx response (e.g. `unknown_accounts`). */
export async function resyncPermissions(
  authToken: string,
  body: ResyncPermissionsRequest,
): Promise<ResyncPermissionsResponse> {
  return adminFetch<ResyncPermissionsResponse>(authToken, 'POST', '/permissions/resync', body)
}

/** @throws {AsyncJobError} on a non-2xx response. */
export async function listPermissions(
  authToken: string,
  params: ListPermissionsParams = {},
): Promise<ListPermissionsResponse> {
  const qs = buildQuery({
    subjectAccount: params.subjectAccount,
    permission: params.permission,
    page: params.page,
    limit: params.limit,
  })
  return adminFetch<ListPermissionsResponse>(authToken, 'GET', `/permissions${qs}`)
}

/**
 * Uploads a client update artifact pair. Unlike every other call here this uses
 * `XMLHttpRequest`, because `fetch` cannot report upload progress and these
 * artifacts are large enough that a silent UI would look hung.
 *
 * @throws {AsyncJobError} on a non-2xx response or a transport failure.
 */
export function uploadClientVersion(
  authToken: string,
  configFile: File,
  executeFile: File,
  onProgress?: (percent: number) => void,
): Promise<void> {
  const form = new FormData()
  form.append('configFile', configFile)
  form.append('executeFile', executeFile)

  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest()
    xhr.open('POST', `${ADMIN_SERVICE_URL}/v1/admin/client-updates`)
    xhr.setRequestHeader('Authorization', `Bearer ${authToken}`)
    // Content-Type is deliberately unset: the browser writes the multipart
    // boundary, and overriding it produces a body the server cannot parse.

    if (onProgress) {
      xhr.upload.onprogress = (e: ProgressEvent) => {
        if (!e.lengthComputable || !e.total) return
        onProgress(Math.round((e.loaded / e.total) * 100))
      }
    }

    xhr.onload = () => {
      if (xhr.status >= 200 && xhr.status < 300) {
        resolve()
        return
      }
      reject(uploadEnvelopeError(xhr.status, xhr.responseText))
    }
    xhr.onerror = () => reject(new AsyncJobError('upload failed: could not reach the server'))

    xhr.send(form)
  })
}

/** Builds an `AsyncJobError` from an XHR error body, mirroring `parseHttpEnvelopeError`. */
function uploadEnvelopeError(status: number, responseText: string): AsyncJobError {
  const fallback = `upload failed with status ${status}`
  try {
    const body = JSON.parse(responseText) as {
      error?: string
      code?: string
      reason?: string
      metadata?: Record<string, string>
    }
    return new AsyncJobError(body.error || fallback, {
      code: body.code,
      reason: body.reason,
      metadata: body.metadata,
    })
  } catch {
    return new AsyncJobError(fallback)
  }
}
