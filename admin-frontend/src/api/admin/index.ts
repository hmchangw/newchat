// Typed REST client for admin-service. Every call is Bearer-authed; non-2xx
// responses throw `AsyncJobError` via `parseHttpEnvelopeError`.

import { ADMIN_SERVICE_URL } from '@/lib/runtimeConfig'
import { parseHttpEnvelopeError } from '@/api'

/** Admin-facing user projection (mirrors admin-service's `userView` — never the bcrypt hash);
 * `normalizeUser` fills defaults for the server's `omitempty` fields. */
export interface AdminUser {
  id: string
  account: string
  siteId: string
  engName: string
  chineseName: string
  roles: string[]
  deactivated: boolean
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
  deactivated?: boolean
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

/** Raw shape of admin-service's `userView` as it appears on the wire — the
 * `omitempty` fields may be absent; `normalizeUser` fills the defaults. */
interface UserViewWire {
  id: string
  account: string
  siteId: string
  engName?: string
  chineseName?: string
  roles?: string[]
  deactivated?: boolean
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
    deactivated: raw.deactivated ?? false,
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

/** @throws {AsyncJobError} on a non-2xx response (e.g. `account_exists`). */
export async function createUser(authToken: string, input: CreateUserInput): Promise<AdminUser> {
  const raw = await adminFetch<UserViewWire>(authToken, 'POST', '/users', input)
  return normalizeUser(raw)
}

/** Applies a partial update; resolves to `void` (server replies `{status:"ok"}`, not the user) —
 * follow up with `getUser` if you need the fresh record. */
export async function updateUser(
  authToken: string,
  account: string,
  patch: UpdateUserPatch,
): Promise<void> {
  await adminFetch<{ status: string }>(authToken, 'PATCH', `/users/${encodeURIComponent(account)}`, patch)
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
