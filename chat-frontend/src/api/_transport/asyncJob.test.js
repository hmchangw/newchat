import { describe, it, expect, vi } from 'vitest'
import { StringCodec } from 'nats.ws'
import { requestWithAsyncResult, requestSync, ASYNC_JOB_ERROR_KINDS, formatAsyncJobError, AsyncJobError } from './asyncJob'

const HYPHENATED_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i

const sc = StringCodec()

// makeFakeSub builds a minimal duck-typed nats subscription whose
// async-iterator yields once we call push(). unsubscribe() ends iteration.
function makeFakeSub() {
  const buf = []
  let resolveWait
  let closed = false
  return {
    push(msg) {
      buf.push(msg)
      if (resolveWait) {
        const r = resolveWait
        resolveWait = null
        r()
      }
    },
    unsubscribe() {
      closed = true
      if (resolveWait) {
        const r = resolveWait
        resolveWait = null
        r()
      }
    },
    [Symbol.asyncIterator]() {
      return {
        async next() {
          while (buf.length === 0 && !closed) {
            await new Promise((r) => { resolveWait = r })
          }
          if (buf.length === 0 && closed) return { value: undefined, done: true }
          return { value: buf.shift(), done: false }
        },
      }
    },
  }
}

function encode(obj) {
  return { data: sc.encode(JSON.stringify(obj)) }
}

function makeNc({ syncReply, subFactory } = {}) {
  const sub = subFactory ? subFactory() : makeFakeSub()
  return {
    sub,
    request: vi.fn(async (subject, _data, opts) => {
      if (typeof syncReply === 'function') return encode(await syncReply(subject, opts))
      return encode(syncReply ?? { status: 'accepted' })
    }),
    subscribe: vi.fn(() => sub),
  }
}

describe('requestWithAsyncResult', () => {
  it('subscribes to the per-request response subject before publishing', async () => {
    const nc = makeNc()
    const p = requestWithAsyncResult(nc, 'alice', 'chat.user.alice.request.room.s1.create', { name: 'x' }, {
      requestId: 'req-1',
    })
    expect(nc.subscribe).toHaveBeenCalledWith('chat.user.alice.response.req-1', { max: 1 })
    // subscribe must have been called BEFORE request was awaited
    expect(nc.subscribe.mock.invocationCallOrder[0])
      .toBeLessThan(nc.request.mock.invocationCallOrder[0])
    nc.sub.push(encode({ requestId: 'req-1', operation: 'room.create', status: 'ok', roomId: 'r1', timestamp: 1 }))
    await p
  })

  it('sends X-Request-ID header on the underlying request', async () => {
    const nc = makeNc()
    const p = requestWithAsyncResult(nc, 'alice', 'subj', {}, { requestId: 'req-1' })
    nc.sub.push(encode({ requestId: 'req-1', operation: 'room.create', status: 'ok', timestamp: 1 }))
    await p
    const opts = nc.request.mock.calls[0][2]
    expect(opts.headers).toBeDefined()
    expect(opts.headers.get('X-Request-ID')).toBe('req-1')
  })

  it('sends the X-Debug header with the level when opts.debugLevel is set', async () => {
    const nc = makeNc()
    const p = requestWithAsyncResult(nc, 'alice', 'subj', {}, { requestId: 'req-1', debugLevel: 'trace' })
    nc.sub.push(encode({ requestId: 'req-1', operation: 'room.create', status: 'ok', timestamp: 1 }))
    await p
    const opts = nc.request.mock.calls[0][2]
    expect(opts.headers.get('X-Debug')).toBe('trace')
  })

  it('omits the X-Debug header by default', async () => {
    const nc = makeNc()
    const p = requestWithAsyncResult(nc, 'alice', 'subj', {}, { requestId: 'req-1' })
    nc.sub.push(encode({ requestId: 'req-1', operation: 'room.create', status: 'ok', timestamp: 1 }))
    await p
    const opts = nc.request.mock.calls[0][2]
    expect(opts.headers.get('X-Debug')).toBe('')
  })

  it('omits the X-Debug header when debugLevel is "off"', async () => {
    const nc = makeNc()
    const p = requestWithAsyncResult(nc, 'alice', 'subj', {}, { requestId: 'req-1', debugLevel: 'off' })
    nc.sub.push(encode({ requestId: 'req-1', operation: 'room.create', status: 'ok', timestamp: 1 }))
    await p
    const opts = nc.request.mock.calls[0][2]
    expect(opts.headers.get('X-Debug')).toBe('')
  })

  it('sends X-Debug-Payload=1 when opts.debugPayload is true', async () => {
    const nc = makeNc()
    const p = requestWithAsyncResult(nc, 'alice', 'subj', {}, { requestId: 'req-1', debugPayload: true })
    nc.sub.push(encode({ requestId: 'req-1', operation: 'room.create', status: 'ok', timestamp: 1 }))
    await p
    const opts = nc.request.mock.calls[0][2]
    expect(opts.headers.get('X-Debug-Payload')).toBe('1')
  })

  it('captures payloads independently of the debug level', async () => {
    const nc = makeNc()
    const p = requestWithAsyncResult(nc, 'alice', 'subj', {}, { requestId: 'req-1', debugPayload: true })
    nc.sub.push(encode({ requestId: 'req-1', operation: 'room.create', status: 'ok', timestamp: 1 }))
    await p
    const opts = nc.request.mock.calls[0][2]
    expect(opts.headers.get('X-Debug')).toBe('')
    expect(opts.headers.get('X-Debug-Payload')).toBe('1')
  })

  it('omits X-Debug-Payload by default', async () => {
    const nc = makeNc()
    const p = requestWithAsyncResult(nc, 'alice', 'subj', {}, { requestId: 'req-1' })
    nc.sub.push(encode({ requestId: 'req-1', operation: 'room.create', status: 'ok', timestamp: 1 }))
    await p
    const opts = nc.request.mock.calls[0][2]
    expect(opts.headers.get('X-Debug-Payload')).toBe('')
  })

  it('resolves with sync + async results when async status is ok', async () => {
    const nc = makeNc({ syncReply: { status: 'accepted', roomId: 'r1', roomType: 'channel' } })
    const p = requestWithAsyncResult(nc, 'alice', 'subj', {}, { requestId: 'req-1' })
    nc.sub.push(encode({ requestId: 'req-1', operation: 'room.create', status: 'ok', roomId: 'r1', timestamp: 1 }))
    const result = await p
    expect(result.sync).toEqual({ status: 'accepted', roomId: 'r1', roomType: 'channel' })
    expect(result.async).toMatchObject({ status: 'ok', roomId: 'r1' })
  })

  it('throws with the sync error message and kind=sync-error when the sync reply carries an error', async () => {
    const nc = makeNc({ syncReply: { error: 'only owners can add members' } })
    const err = await requestWithAsyncResult(nc, 'alice', 'subj', {}, { requestId: 'req-1' })
      .catch((e) => e)
    expect(err.message).toBe('only owners can add members')
    expect(err.kind).toBe(ASYNC_JOB_ERROR_KINDS.SyncError)
  })

  it('returns the DM-exists payload (error + roomId) without throwing', async () => {
    // Server replies with {error, roomId} for the dedup case — caller must be
    // able to distinguish this from a true failure and navigate to roomId.
    const nc = makeNc({ syncReply: { error: 'dm already exists', roomId: 'r-existing' } })
    const result = await requestWithAsyncResult(nc, 'alice', 'subj', {}, {
      requestId: 'req-1',
      treatAsSuccess: (reply) => reply.error === 'dm already exists' && !!reply.roomId,
    })
    expect(result.sync).toEqual({ error: 'dm already exists', roomId: 'r-existing' })
    expect(result.async).toBeNull()
  })

  it('throws with kind=async-error when async status is error', async () => {
    const nc = makeNc()
    const p = requestWithAsyncResult(nc, 'alice', 'subj', {}, { requestId: 'req-1' })
    nc.sub.push(encode({ requestId: 'req-1', operation: 'room.member.add', status: 'error', error: 'remote site unreachable', timestamp: 1 }))
    const err = await p.catch((e) => e)
    expect(err.message).toBe('remote site unreachable')
    expect(err.kind).toBe(ASYNC_JOB_ERROR_KINDS.AsyncError)
  })

  it('rejects with kind=async-timeout if no async result arrives in time', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      const nc = makeNc()
      const p = requestWithAsyncResult(nc, 'alice', 'subj', {}, {
        requestId: 'req-1',
        asyncTimeout: 500,
      })
      // Attach the rejection handler before advancing: the async timer API
      // flushes the rejection during the advance, so a later .catch would miss it.
      const settled = p.catch((e) => e)
      await Promise.resolve()
      await Promise.resolve()
      await vi.advanceTimersByTimeAsync(600)
      const err = await settled
      expect(err.kind).toBe(ASYNC_JOB_ERROR_KINDS.AsyncTimeout)
      expect(err.message).toMatch(/timeout/i)
    } finally {
      vi.useRealTimers()
    }
  })

  it('unsubscribes the response subscription on async timeout', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      const nc = makeNc()
      const unsubSpy = vi.spyOn(nc.sub, 'unsubscribe')
      const p = requestWithAsyncResult(nc, 'alice', 'subj', {}, {
        requestId: 'req-1',
        asyncTimeout: 100,
      })
      // Attach the rejection assertion before advancing so the async timer
      // flush doesn't surface the rejection as unhandled.
      const rejection = expect(p).rejects.toThrow()
      await Promise.resolve(); await Promise.resolve()
      await vi.advanceTimersByTimeAsync(200)
      await rejection
      expect(unsubSpy).toHaveBeenCalled()
    } finally {
      vi.useRealTimers()
    }
  })

  it('unsubscribes when the sync reply itself is an error', async () => {
    const nc = makeNc({ syncReply: { error: 'invalid request' } })
    const unsubSpy = vi.spyOn(nc.sub, 'unsubscribe')
    await expect(
      requestWithAsyncResult(nc, 'alice', 'subj', {}, { requestId: 'req-1' })
    ).rejects.toThrow('invalid request')
    expect(unsubSpy).toHaveBeenCalled()
  })

  it('rejects with kind=subscription-closed when the sub closes before a result arrives', async () => {
    const nc = makeNc()
    const p = requestWithAsyncResult(nc, 'alice', 'subj', {}, {
      requestId: 'req-1',
      asyncTimeout: 10000,
    })
    // Let the sync request resolve, then close the subscription without pushing.
    await Promise.resolve(); await Promise.resolve()
    nc.sub.unsubscribe()
    const err = await p.catch((e) => e)
    expect(err.kind).toBe(ASYNC_JOB_ERROR_KINDS.SubscriptionClosed)
    expect(err.message).toMatch(/subscription closed/i)
  })

  it('unsubscribes on the success path too (defensive — does not rely on max:1 alone)', async () => {
    const nc = makeNc()
    const unsubSpy = vi.spyOn(nc.sub, 'unsubscribe')
    const p = requestWithAsyncResult(nc, 'alice', 'subj', {}, { requestId: 'req-1' })
    nc.sub.push(encode({ requestId: 'req-1', operation: 'room.create', status: 'ok', timestamp: 1 }))
    await p
    expect(unsubSpy).toHaveBeenCalled()
  })

  it('auto-generates a request id when caller does not supply one', async () => {
    const nc = makeNc()
    const p = requestWithAsyncResult(nc, 'alice', 'subj', {})
    const subCall = nc.subscribe.mock.calls[0][0]
    // UUIDv7: 8-4-4-4-12 hex with the version nibble '7' at the start of the
    // third group, and IETF variant nibble (8/9/a/b) at the start of the fourth.
    const m = subCall.match(/^chat\.user\.alice\.response\.([0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})$/i)
    expect(m).not.toBeNull()
    const generatedId = m[1]
    nc.sub.push(encode({ requestId: generatedId, operation: 'room.create', status: 'ok', timestamp: 1 }))
    const result = await p
    expect(result.requestId).toBe(generatedId)
  })
})

describe('formatAsyncJobError', () => {
  it('returns a friendly retry hint for AsyncTimeout', () => {
    const err = new Error('async result timeout')
    err.kind = ASYNC_JOB_ERROR_KINDS.AsyncTimeout
    const msg = formatAsyncJobError(err)
    expect(msg).toMatch(/server didn.t respond/i)
    expect(msg).toMatch(/may still complete|refresh/i)
  })

  it('returns a friendly hint for SubscriptionClosed', () => {
    const err = new Error('subscription closed before result arrived')
    err.kind = ASYNC_JOB_ERROR_KINDS.SubscriptionClosed
    expect(formatAsyncJobError(err)).toMatch(/connection|disconnected|interrupted/i)
  })

  it('falls back to message for SyncError when no reason is attached (server-supplied user-safe text)', () => {
    const err = new Error('only owners can add members')
    err.kind = ASYNC_JOB_ERROR_KINDS.SyncError
    expect(formatAsyncJobError(err)).toBe('only owners can add members')
  })

  it('falls back to message for AsyncError when no reason is attached', () => {
    const err = new Error('exceeds maximum capacity')
    err.kind = ASYNC_JOB_ERROR_KINDS.AsyncError
    expect(formatAsyncJobError(err)).toBe('exceeds maximum capacity')
  })

  it('returns the humanized REASON_COPY copy when a SyncError carries a known reason', () => {
    const err = new AsyncJobError('exceeds maximum capacity (50)', ASYNC_JOB_ERROR_KINDS.SyncError, {
      reason: 'max_room_size_reached',
    })
    expect(formatAsyncJobError(err)).toBe('This room is at capacity.')
  })

  it('maps account_not_ready to the contact-administrator copy', () => {
    const err = new AsyncJobError('account not ready for chat', ASYNC_JOB_ERROR_KINDS.SyncError, {
      code: 'forbidden',
      reason: 'account_not_ready',
    })
    expect(formatAsyncJobError(err)).toBe("Your account isn't ready for chat yet — contact your administrator.")
  })

  it('returns the humanized copy for not_room_member', () => {
    const err = new AsyncJobError('only room members can do that', ASYNC_JOB_ERROR_KINDS.AsyncError, {
      reason: 'not_room_member',
    })
    expect(formatAsyncJobError(err)).toBe("You're not a member of this room.")
  })

  it('returns the humanized copy for not_subscribed', () => {
    const err = new AsyncJobError('not subscribed', ASYNC_JOB_ERROR_KINDS.SyncError, {
      reason: 'not_subscribed',
    })
    expect(formatAsyncJobError(err)).toBe('You need to join this room first.')
  })

  it('returns the humanized copy for large_room_post_restricted', () => {
    const err = new AsyncJobError('only owners/admins may post', ASYNC_JOB_ERROR_KINDS.SyncError, {
      reason: 'large_room_post_restricted',
    })
    expect(formatAsyncJobError(err)).toBe('Only owners and admins can post here.')
  })

  it('falls back to err.message when the reason is unrecognized', () => {
    const err = new AsyncJobError('some unmapped error', ASYNC_JOB_ERROR_KINDS.SyncError, {
      reason: 'not_in_catalog',
    })
    expect(formatAsyncJobError(err)).toBe('some unmapped error')
  })

  it('falls back to message for unknown / untagged errors', () => {
    expect(formatAsyncJobError(new Error('random'))).toBe('random')
    expect(formatAsyncJobError(null)).toBe('')
    expect(formatAsyncJobError(undefined)).toBe('')
  })

  it('handles every ASYNC_JOB_ERROR_KINDS value (no silent drift when a new kind is added)', () => {
    // Kinds that intentionally pass the raw server-supplied message through
    // (already user-safe via room-service's sanitizeError). All other kinds
    // must produce a friendlier client-side hint.
    const RAW_MESSAGE_KINDS = new Set([
      ASYNC_JOB_ERROR_KINDS.SyncError,
      ASYNC_JOB_ERROR_KINDS.AsyncError,
    ])
    for (const kind of Object.values(ASYNC_JOB_ERROR_KINDS)) {
      const err = new Error('original-message')
      err.kind = kind
      const formatted = formatAsyncJobError(err)
      if (RAW_MESSAGE_KINDS.has(kind)) {
        expect(formatted, `kind=${kind} should pass server message through`).toBe('original-message')
      } else {
        expect(formatted, `kind=${kind} should produce a branded client-side hint`).not.toBe('original-message')
        expect(formatted, `kind=${kind} should produce non-empty hint`).toBeTruthy()
      }
    }
  })
})

describe('requestSync', () => {
  it('stamps a valid hyphenated-UUID X-Request-ID header when none is given', async () => {
    const nc = makeNc({ syncReply: { count: 3 } })
    await requestSync(nc, 'chat.user.alice.request.user.s1.subscription.count', { unread: true })
    const opts = nc.request.mock.calls[0][2]
    expect(opts.headers).toBeDefined()
    expect(opts.headers.get('X-Request-ID')).toMatch(HYPHENATED_UUID)
  })

  it('uses an explicit requestId when provided', async () => {
    const nc = makeNc({ syncReply: { count: 0 } })
    await requestSync(nc, 'subj', {}, { requestId: '01970a4f-8c2d-7c9a-abcd-e0123456789f' })
    const opts = nc.request.mock.calls[0][2]
    expect(opts.headers.get('X-Request-ID')).toBe('01970a4f-8c2d-7c9a-abcd-e0123456789f')
  })

  it('returns the decoded reply on success', async () => {
    const nc = makeNc({ syncReply: { count: 7 } })
    const result = await requestSync(nc, 'subj', {})
    expect(result).toEqual({ count: 7 })
  })

  it('throws an AsyncJobError carrying code/reason on an errcode envelope', async () => {
    const nc = makeNc({ syncReply: { error: 'we could not find that user', code: 'not_found', reason: 'user_not_found' } })
    const err = await requestSync(nc, 'subj', {}).catch((e) => e)
    expect(err).toBeInstanceOf(AsyncJobError)
    expect(err.kind).toBe(ASYNC_JOB_ERROR_KINDS.SyncError)
    expect(err.code).toBe('not_found')
    expect(err.reason).toBe('user_not_found')
  })
})
