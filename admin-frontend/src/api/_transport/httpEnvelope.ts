// HTTP-only errcode envelope shape: {error, code, reason?, metadata?}.
// `formatAsyncJobError` is the canonical formatter for the `@/api` barrel.

/** Thrown by `parseHttpEnvelopeError`; branch on `reason ?? code`, never `message`. */
export class AsyncJobError extends Error {
  readonly code?: string
  readonly reason?: string
  readonly metadata?: Record<string, string>
  constructor(
    message: string,
    opts?: { code?: string; reason?: string; metadata?: Record<string, string> },
  ) {
    super(message)
    this.name = 'AsyncJobError'
    if (opts?.code !== undefined) this.code = opts.code
    if (opts?.reason !== undefined) this.reason = opts.reason
    if (opts?.metadata !== undefined) this.metadata = opts.metadata
  }
}

/** Reason-keyed friendly copy for admin-service reasons — extend rather than substring-match messages. */
const REASON_COPY: Record<string, string> = {
  not_admin: 'You need admin access to do that.',
  invalid_token: 'That link has expired or is invalid — request a new one.',
  account_exists: 'An account with that email already exists.',
}

/** User-facing message for an `AsyncJobError`; prefers reason-keyed copy (stable machine codes) over `err.message`. */
export function formatAsyncJobError(err: unknown): string {
  if (!err) return ''
  const reason = err instanceof AsyncJobError ? err.reason : (err as { reason?: string })?.reason
  if (reason && REASON_COPY[reason]) {
    return REASON_COPY[reason]
  }
  return err instanceof Error ? err.message : String(err)
}

/** Shape of the errcode envelope body on a non-2xx HTTP response. */
interface HttpErrorEnvelope {
  error?: string
  code?: string
  reason?: string
  metadata?: Record<string, string>
}

/** Throws an `AsyncJobError` parsed from the non-2xx envelope body; uses `fallback` when the body isn't that shape. */
export async function parseHttpEnvelopeError(resp: Response, fallback: string): Promise<never> {
  let body: HttpErrorEnvelope | undefined
  try {
    body = (await resp.json()) as HttpErrorEnvelope
  } catch {
    body = undefined
  }

  const isEnvelope =
    body !== null &&
    typeof body === 'object' &&
    (typeof body.error === 'string' || typeof body.code === 'string')

  if (isEnvelope && body) {
    throw new AsyncJobError(body.error || fallback, {
      code: body.code,
      reason: body.reason,
      metadata: body.metadata,
    })
  }

  throw new AsyncJobError(fallback)
}
