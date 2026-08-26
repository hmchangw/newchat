import { msgSend, userResponse } from '../_transport/subjects'
import { AsyncJobError, ASYNC_JOB_ERROR_KINDS } from '../_transport/asyncJob'
import type { Nats, NatsSubscription } from '../types'

/** Matches asyncJob's sync-reply window; the gatekeeper answers off a JetStream
 *  consumer, so this covers consumer lag as well as the request itself. */
const DEFAULT_SEND_TIMEOUT = 10_000

export interface SendMessagePayload {
  id: string
  content: string
  requestId: string
  quotedParentMessageId?: string
  threadParentMessageId?: string
  threadParentMessageCreatedAt?: number
  /** Base64-encoded Attachment JSON blobs (see lib/attachment.encodeAttachment).
   *  Max 1 today; content may be empty when attachments are present. */
  attachments?: string[]
}

export interface SendMessageArgs {
  roomId: string
  siteId: string
  payload: SendMessagePayload
  timeoutMs?: number
}

interface ErrorEnvelope {
  error?: string
  code?: string
  reason?: string
  metadata?: Record<string, string>
}

/**
 * Submit a new message into a room and settle on the gatekeeper's reply.
 *
 * message-gatekeeper acks every validation and authorization failure on
 * `chat.user.{account}.response.{requestId}` (docs/client-api.md §msg.send).
 * This used to be fire-and-forget, so those replies were discarded and a
 * refused send looked identical to a successful one.
 *
 * @throws {AsyncJobError} `.kind` is 'async-error' for a typed refusal (with `.code`
 *   and `.reason` from the envelope), 'async-timeout' when no reply arrives, or
 *   'sync-error' when `subscribe` or `publish` itself throws (e.g. not connected) — a
 *   local transport failure, never a remote one, so it carries no `.code`/`.reason`.
 */
export function sendMessage(
  { user, publish, subscribe }: Nats,
  { roomId, siteId, payload, timeoutMs = DEFAULT_SEND_TIMEOUT }: SendMessageArgs,
): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    let done = false
    let sub: NatsSubscription | undefined
    let timer: ReturnType<typeof setTimeout> | undefined

    function settle(fn: () => void) {
      if (done) return
      done = true
      if (timer !== undefined) clearTimeout(timer)
      sub?.unsubscribe()
      fn()
    }

    try {
      // Subscribe before publishing so a fast gatekeeper cannot beat us to the reply.
      // subscribe() is inside the try because it throws first when the client is
      // disconnected — publish() is never reached in that case.
      sub = subscribe(userResponse(user.account, payload.requestId), (data: unknown) => {
        settle(() => {
          const env = (data ?? {}) as ErrorEnvelope
          if (env.error) {
            reject(new AsyncJobError(env.error, ASYNC_JOB_ERROR_KINDS.AsyncError, { code: env.code, reason: env.reason, metadata: env.metadata }))
            return
          }
          resolve()
        })
      })

      timer = setTimeout(() => {
        settle(() => reject(new AsyncJobError('send timed out', ASYNC_JOB_ERROR_KINDS.AsyncTimeout)))
      }, timeoutMs)

      publish(msgSend(user.account, roomId, siteId), payload)
    } catch (err) {
      // A local transport failure (e.g. not connected) — not a remote
      // refusal, so it gets the same kind requestWithAsyncResult uses for
      // its synchronous request phase, not AsyncError/AsyncTimeout.
      settle(() => {
        const msg = err instanceof Error ? err.message : String(err)
        reject(new AsyncJobError(msg, ASYNC_JOB_ERROR_KINDS.SyncError, { cause: err }))
      })
    }
  })
}
