// requestWithAsyncResult bridges the room-service two-phase reply contract:
//   1. sync NATS reply  → typically {status:"accepted", …} (or {error})
//   2. async result     → AsyncJobResult on chat.user.{account}.response.{requestID}
//
// The response subscription is opened BEFORE the request is published so a
// fast worker can't beat us to the punch. The X-Request-ID header on the
// request is what tells room-worker which response subject to publish on —
// without it the async result is never emitted.

import { StringCodec, headers as natsHeaders } from 'nats.ws'
import type { NatsConnection, Subscription as NatsSubscription } from 'nats.ws'
import { v7 as uuidv7 } from 'uuid'
import { userResponse } from './subjects'
import type { AsyncJobOptions, AsyncJobResult } from '../types'

const sc = StringCodec()

const DEFAULT_SYNC_TIMEOUT = 5000
const DEFAULT_ASYNC_TIMEOUT = 30000

/** Discriminated string kinds attached to every error thrown from here. */
export const ASYNC_JOB_ERROR_KINDS = {
  SyncError: 'sync-error',
  AsyncError: 'async-error',
  AsyncTimeout: 'async-timeout',
  SubscriptionClosed: 'subscription-closed',
} as const

export type AsyncJobErrorKind =
  (typeof ASYNC_JOB_ERROR_KINDS)[keyof typeof ASYNC_JOB_ERROR_KINDS]

/**
 * The 7+1 generic error categories the backend's `pkg/errcode` package emits
 * on every error envelope (NATS reply, JetStream result, HTTP). Mirrors the
 * closed set in `pkg/errcode/category.go`. `string & {}` is a JSDoc-style
 * escape hatch so a not-yet-mirrored future category still typechecks.
 */
export type ErrorCode =
  | 'bad_request'
  | 'unauthenticated'
  | 'forbidden'
  | 'not_found'
  | 'conflict'
  | 'too_many_requests'
  | 'unavailable'
  | 'internal'
  | (string & {})

/**
 * Error class thrown by `requestWithAsyncResult` AND `NatsContext.request`.
 * Use `instanceof` to narrow without string-matching the message.
 *
 * Why a class (not just an interface): callers can do
 *   `if (err instanceof AsyncJobError) …`
 * which is the idiomatic way to discriminate caught `unknown` in TS.
 *
 * `code` / `reason` / `metadata` are populated from the backend errcode
 * envelope when the failure carries one (`SyncError`, `AsyncError`); they are
 * undefined for wire-level failures (`AsyncTimeout`, `SubscriptionClosed`).
 * Branch on `reason ?? code` — never on `message`.
 */
export class AsyncJobError extends Error {
  readonly kind: AsyncJobErrorKind
  readonly code?: ErrorCode
  readonly reason?: string
  readonly metadata?: Record<string, string>
  constructor(
    message: string,
    kind: AsyncJobErrorKind,
    opts?: {
      cause?: unknown
      code?: ErrorCode
      reason?: string
      metadata?: Record<string, string>
    },
  ) {
    super(message)
    this.name = 'AsyncJobError'
    this.kind = kind
    if (opts?.cause !== undefined) this.cause = opts.cause
    if (opts?.code !== undefined) this.code = opts.code
    if (opts?.reason !== undefined) this.reason = opts.reason
    if (opts?.metadata !== undefined) this.metadata = opts.metadata
  }
}

/**
 * Reason-keyed humanized copy for the errcode reasons emitted today
 * (catalog: docs/client-api.md §6 + chat-frontend/CLAUDE.md). Used by
 * formatAsyncJobError so consumers don't have to maintain their own per-call
 * map of reason→copy. sso_token_expired / invalid_sso_token are intentionally
 * absent — they drive a redirect (Task 20.7), not a user-facing message.
 */
const REASON_COPY: Record<string, string> = {
  max_room_size_reached: 'This room is at capacity.',
  not_room_member: "You're not a member of this room.",
  not_room_owner: 'Only owners can do that.',
  last_owner_cannot_leave: "You're the last owner — promote someone else first.",
  bot_in_channel: "Bots can't join channels.",
  bot_not_available: "This bot isn't available right now.",
  user_not_found: "We couldn't find that user.",
  invalid_org: "We couldn't find that group.",
  self_dm: "You can't DM yourself.",
  last_member_cannot_remove: "Can't remove the last member — delete the room instead.",
  target_not_member: "That user isn't in this room.",
  already_owner: 'That user is already an owner.',
  cannot_demote_last_owner: "Can't demote the last owner — promote someone else first.",
  promote_requires_individual: 'Only individual members can be promoted to owner.',
  large_room_post_restricted: 'Only owners and admins can post here.',
  not_subscribed: 'You need to join this room first.',
  outside_access_window: 'This message is older than your access to this room.',
  pin_disabled: 'Pinning is turned off for this site.',
  pin_limit_reached: 'This room has reached its pin limit — unpin a message first.',
  pin_room_too_large: 'This room is too large for non-admins to pin.',
  account_not_ready: "Your account isn't ready for chat yet — contact your administrator.",
}

/**
 * User-facing message for an error thrown by `requestWithAsyncResult`.
 *
 * Server-side errors (`SyncError`, `AsyncError`) carry the errcode envelope's
 * `reason` when applicable — preferred over `message` because reasons are
 * stable machine codes (the english text can change without notice). Falls
 * back to `err.message` when the reason isn't in the catalog (or absent, e.g.
 * a bare `Error` from a non-backend caller). Wire-level failures
 * (`AsyncTimeout`, `SubscriptionClosed`) get friendlier actionable hints.
 */
export function formatAsyncJobError(err: unknown): string {
  if (!err) return ''
  // Prefer `instanceof AsyncJobError`, but also duck-type on `.kind` so
  // any caller that hand-rolls an Error with a `kind` field still gets
  // the friendly hints. Some test helpers do exactly that.
  const kind =
    err instanceof AsyncJobError
      ? err.kind
      : (err as { kind?: AsyncJobErrorKind })?.kind
  switch (kind) {
    case ASYNC_JOB_ERROR_KINDS.AsyncTimeout:
      return "The server didn't respond in time. The action may still complete — refresh to check."
    case ASYNC_JOB_ERROR_KINDS.SubscriptionClosed:
      return 'Connection interrupted before the server confirmed. Refresh to check the result.'
    default: {
      const reason =
        err instanceof AsyncJobError
          ? err.reason
          : (err as { reason?: string })?.reason
      if (reason && REASON_COPY[reason]) {
        return REASON_COPY[reason]
      }
      return err instanceof Error ? err.message : String(err)
    }
  }
}

// Internal envelope passed from the inbox loop to the awaiter. Discriminated
// so the awaiter can pattern-match on `kind` without optional chaining.
type Envelope =
  | { kind: 'data'; data: unknown }
  | { kind: 'closed' }
  | { kind: 'error'; error: unknown }
  | { kind: 'timeout' }

/** Common shape of any sync reply we treat specially — `error` triggers
 *  the failure branch, `status` is the typical 'accepted'/'error'/'exists'
 *  marker. `code`/`reason`/`metadata` are the new errcode envelope fields
 *  (present on errors from any post-migration backend; absent on a legacy
 *  reply during the rollout window). */
interface SyncReplyEnvelope {
  error?: string
  status?: string
  code?: ErrorCode
  reason?: string
  metadata?: Record<string, string>
}

/** Common shape of any async-job result envelope we receive on the
 *  response subject. `status === 'error'` takes the failure path.
 *  `code`/`reason` mirror the backend `AsyncJobResult.Code`/`Reason` fields. */
interface AsyncReplyEnvelope {
  status?: string
  error?: string
  code?: ErrorCode
  reason?: string
  metadata?: Record<string, string>
}

/**
 * Issues a plain sync NATS request/reply and decodes the errcode envelope.
 *
 * Always stamps a fresh hyphenated-UUID `X-Request-ID` header (overridable
 * via `opts.requestId`). Backend handlers that derive JetStream dedup IDs or
 * deterministic document IDs from the request ID (`RequireRequestID`, see
 * docs/error-handling.md §3a) reject requests without a valid one — so every
 * sync request must carry the header, exactly as the two-phase path does.
 *
 * @throws {AsyncJobError} On an errcode reply, with `.kind = SyncError` and
 *   `.code`/`.reason`/`.metadata` populated. Wire-level failures (timeout,
 *   not connected) propagate as their native Error.
 */
export async function requestSync<T = unknown>(
  nc: NatsConnection,
  subject: string,
  data: unknown = {},
  {
    requestId = uuidv7(),
    timeout = 5000,
    debugLevel = 'off',
    debugPayload = false,
  }: { requestId?: string; timeout?: number; debugLevel?: string; debugPayload?: boolean } = {},
): Promise<T> {
  const h = natsHeaders()
  h.set('X-Request-ID', requestId)
  if (debugLevel && debugLevel !== 'off') h.set('X-Debug', debugLevel)
  if (debugPayload) h.set('X-Debug-Payload', '1')
  const resp = await nc.request(subject, sc.encode(JSON.stringify(data)), { timeout, headers: h })
  const parsed = JSON.parse(sc.decode(resp.data))
  if (parsed.error) {
    // errcode envelope {code, reason?, error, metadata?}. Legacy replies
    // (pre-migration backend during rollout) lack code/reason — consumers
    // fall back to err.message.
    throw new AsyncJobError(parsed.error, ASYNC_JOB_ERROR_KINDS.SyncError, {
      code: parsed.code,
      reason: parsed.reason,
      metadata: parsed.metadata,
    })
  }
  return parsed as T
}

/**
 * Issues a NATS request whose handler responds in two phases: a sync reply
 * (typically `{status:"accepted"}` or `{error}`) followed by an
 * `AsyncJobResult` published to `chat.user.{account}.response.{requestID}`
 * once the underlying worker finishes.
 *
 * Subscribes to the response subject BEFORE publishing so a fast worker
 * can't beat the client to the punch. Sets the `X-Request-ID` NATS header
 * so the worker knows where to publish the async result.
 *
 * @throws {AsyncJobError} On any failure, with `.kind` set to one of
 *   the `AsyncJobErrorKind` values. Pass to `formatAsyncJobError(err)`
 *   for user-facing text.
 */
export async function requestWithAsyncResult<S = unknown, A = unknown>(
  nc: NatsConnection,
  account: string,
  subject: string,
  payload: unknown,
  opts: AsyncJobOptions = {},
): Promise<AsyncJobResult<S, A>> {
  const {
    requestId = uuidv7(),
    syncTimeout = DEFAULT_SYNC_TIMEOUT,
    asyncTimeout = DEFAULT_ASYNC_TIMEOUT,
    treatAsSuccess,
    debugLevel = 'off',
    debugPayload = false,
  } = opts

  const sub: NatsSubscription = nc.subscribe(userResponse(account, requestId), { max: 1 })

  // Register before request resolves so a result that arrives during the
  // sync window is buffered, not dropped. Tagged-envelope resolves never
  // reject so late cleanup signals can't surface as unhandled rejections.
  let resolveAsync!: (env: Envelope) => void
  const asyncPromise = new Promise<Envelope>((res) => { resolveAsync = res })
  ;(async () => {
    try {
      for await (const msg of sub) {
        resolveAsync({ kind: 'data', data: JSON.parse(sc.decode(msg.data)) })
        return
      }
      resolveAsync({ kind: 'closed' })
    } catch (err) {
      resolveAsync({ kind: 'error', error: err })
    }
  })()

  const cleanupSub = () => {
    try { sub.unsubscribe() } catch { /* already closed */ }
  }

  let sync: S
  try {
    const h = natsHeaders()
    h.set('X-Request-ID', requestId)
    if (debugLevel && debugLevel !== 'off') h.set('X-Debug', debugLevel)
    if (debugPayload) h.set('X-Debug-Payload', '1')
    const resp = await nc.request(subject, sc.encode(JSON.stringify(payload)), {
      timeout: syncTimeout,
      headers: h,
    })
    sync = JSON.parse(sc.decode(resp.data)) as S
  } catch (err) {
    cleanupSub()
    const msg = err instanceof Error ? err.message : String(err)
    throw new AsyncJobError(msg, ASYNC_JOB_ERROR_KINDS.SyncError, { cause: err })
  }

  // `sync` is generic, but we always inspect the same envelope fields.
  const syncEnv = sync as unknown as SyncReplyEnvelope
  if (syncEnv?.error) {
    // DM-exists and similar replies the caller wants to treat as success
    // (legacy `{error:"dm already exists", roomId}` shape, or any other
    // 200-with-error contract). The new `{status:"exists", roomId}` shape
    // never has `.error`, so it skips this branch entirely.
    if (treatAsSuccess && treatAsSuccess(sync)) {
      cleanupSub()
      return { requestId, sync, async: null }
    }
    cleanupSub()
    throw new AsyncJobError(syncEnv.error, ASYNC_JOB_ERROR_KINDS.SyncError, {
      code: syncEnv.code,
      reason: syncEnv.reason,
      metadata: syncEnv.metadata,
    })
  }

  let timer: ReturnType<typeof setTimeout> | undefined
  try {
    const timeoutPromise = new Promise<Envelope>((resolve) => {
      timer = setTimeout(() => resolve({ kind: 'timeout' }), asyncTimeout)
    })
    const envelope = await Promise.race([asyncPromise, timeoutPromise])
    if (timer) clearTimeout(timer)
    if (envelope.kind === 'timeout') {
      throw new AsyncJobError('async result timeout', ASYNC_JOB_ERROR_KINDS.AsyncTimeout)
    }
    if (envelope.kind === 'error') {
      const cause = envelope.error
      const msg = cause instanceof Error ? cause.message : 'subscription error'
      throw new AsyncJobError(msg, ASYNC_JOB_ERROR_KINDS.SubscriptionClosed, { cause })
    }
    if (envelope.kind === 'closed') {
      throw new AsyncJobError(
        'subscription closed before result arrived',
        ASYNC_JOB_ERROR_KINDS.SubscriptionClosed,
      )
    }
    const asyncEnv = envelope.data as AsyncReplyEnvelope
    if (asyncEnv.status === 'error') {
      throw new AsyncJobError(
        asyncEnv.error || 'operation failed',
        ASYNC_JOB_ERROR_KINDS.AsyncError,
        {
          code: asyncEnv.code,
          reason: asyncEnv.reason,
          metadata: asyncEnv.metadata,
        },
      )
    }
    cleanupSub()
    return { requestId, sync, async: envelope.data as A }
  } catch (err) {
    if (timer) clearTimeout(timer)
    cleanupSub()
    throw err
  }
}
