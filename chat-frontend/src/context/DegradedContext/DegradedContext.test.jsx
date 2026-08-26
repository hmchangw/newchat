import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { renderHook, act } from '@testing-library/react'
import { ErrorCode, NatsError } from 'nats.ws'
import { sendMessage } from '@/api'
import { DegradedProvider, useDegraded } from './DegradedContext'

const wrapper = ({ children }) => <DegradedProvider>{children}</DegradedProvider>

describe('DegradedContext', () => {
  beforeEach(() => vi.useFakeTimers())
  afterEach(() => vi.useRealTimers())

  it('starts healthy', () => {
    const { result } = renderHook(() => useDegraded(), { wrapper })
    expect(result.current.historyDegraded).toBe(false)
  })

  it('goes degraded on an unavailable history failure', () => {
    const { result } = renderHook(() => useDegraded(), { wrapper })
    act(() => result.current.noteHistoryFailure({ code: 'unavailable' }))
    expect(result.current.historyDegraded).toBe(true)
  })

  it('goes degraded on an internal history failure', () => {
    const { result } = renderHook(() => useDegraded(), { wrapper })
    act(() => result.current.noteHistoryFailure({ code: 'internal' }))
    expect(result.current.historyDegraded).toBe(true)
  })

  it('goes degraded on a thread_start_unavailable refusal', () => {
    const { result } = renderHook(() => useDegraded(), { wrapper })
    act(() => result.current.noteHistoryFailure({ reason: 'thread_start_unavailable' }))
    expect(result.current.historyDegraded).toBe(true)
  })

  it('ignores a terminal error — not_found is not an outage', () => {
    const { result } = renderHook(() => useDegraded(), { wrapper })
    act(() => result.current.noteHistoryFailure({ code: 'not_found' }))
    expect(result.current.historyDegraded).toBe(false)
  })

  it('clears on a successful history load', () => {
    const { result } = renderHook(() => useDegraded(), { wrapper })
    act(() => result.current.noteHistoryFailure({ code: 'unavailable' }))
    act(() => result.current.noteHistorySuccess())
    expect(result.current.historyDegraded).toBe(false)
  })

  it('self-clears after the TTL so a stuck flag cannot outlive the outage', () => {
    const { result } = renderHook(() => useDegraded(), { wrapper })
    act(() => result.current.noteHistoryFailure({ code: 'unavailable' }))
    act(() => vi.advanceTimersByTime(60_000))
    expect(result.current.historyDegraded).toBe(false)
  })

  it('a repeated failure re-arms the TTL so a flapping outage does not prematurely clear', () => {
    const { result } = renderHook(() => useDegraded(), { wrapper })
    act(() => result.current.noteHistoryFailure({ code: 'unavailable' }))
    act(() => vi.advanceTimersByTime(30_000))
    expect(result.current.historyDegraded).toBe(true)
    // A second failure at t=30s must reset the TTL window rather than
    // stacking a second timer alongside the first.
    act(() => result.current.noteHistoryFailure({ code: 'unavailable' }))
    // Advance past the ORIGINAL 60s window (30s + 40s = 70s elapsed since the
    // first failure). If the flag cleared here, the re-arm would have failed
    // to reset the timer and a flapping outage could prematurely re-enable
    // thread starts.
    act(() => vi.advanceTimersByTime(40_000))
    expect(result.current.historyDegraded).toBe(true)
    // The re-armed timer (started at t=30s) expires at t=90s; the remaining
    // 20s clears it.
    act(() => vi.advanceTimersByTime(20_000))
    expect(result.current.historyDegraded).toBe(false)
  })
})

// The two flavours of outage that never reach an errcode envelope. requestSync
// has a 5s timeout and only builds an AsyncJobError when the reply carries one,
// so Cassandra hanging (its own query timeout is longer than ours) and
// history-service being down surface as bare nats.ws NatsErrors instead.
describe('DegradedContext — wire-level failures', () => {
  beforeEach(() => vi.useFakeTimers())
  afterEach(() => vi.useRealTimers())

  it('goes degraded on a request timeout (nats.ws TIMEOUT)', () => {
    const { result } = renderHook(() => useDegraded(), { wrapper })
    act(() => result.current.noteHistoryFailure(NatsError.errorForCode(ErrorCode.Timeout)))
    expect(result.current.historyDegraded).toBe(true)
  })

  it('goes degraded on no-responders (nats.ws 503 — history-service down)', () => {
    const { result } = renderHook(() => useDegraded(), { wrapper })
    act(() => result.current.noteHistoryFailure(NatsError.errorForCode(ErrorCode.NoResponders)))
    expect(result.current.historyDegraded).toBe(true)
  })

  it('goes degraded on an unclassified error — unclassified means infra, as on the server', () => {
    const { result } = renderHook(() => useDegraded(), { wrapper })
    act(() => result.current.noteHistoryFailure(new Error('Unexpected end of JSON input')))
    expect(result.current.historyDegraded).toBe(true)
  })

  it('stays healthy on no error at all', () => {
    const { result } = renderHook(() => useDegraded(), { wrapper })
    act(() => result.current.noteHistoryFailure(undefined))
    expect(result.current.historyDegraded).toBe(false)
  })

  const terminal = ['bad_request', 'unauthenticated', 'forbidden', 'not_found', 'conflict', 'too_many_requests']
  terminal.forEach((code) => {
    it(`ignores the terminal category ${code} — a settled answer is not an outage`, () => {
      const { result } = renderHook(() => useDegraded(), { wrapper })
      act(() => result.current.noteHistoryFailure({ code }))
      expect(result.current.historyDegraded).toBe(false)
    })
  })
})

// The arming tests above hand the provider a hand-written error. This one joins
// the halves: a REAL error object built by the api layer, fed to the REAL
// provider. Nothing else verifies that what `@/api` throws is a shape
// isOutageSignal accepts.
describe('DegradedContext — api-layer seam', () => {
  const payload = { id: 'M1', content: 'hi', requestId: 'req-1', threadParentMessageId: 'p1' }

  /** Real sendMessage over a fake transport. `onPublish` decides what the
   *  gatekeeper does with the send. Returns the rejection. */
  async function sendAndCatch(onPublish) {
    const handlers = new Map()
    const nats = {
      user: { account: 'alice' },
      subscribe: (subj, cb) => {
        handlers.set(subj, cb)
        return { unsubscribe: () => handlers.delete(subj) }
      },
      publish: () => onPublish(handlers.get('chat.user.alice.response.req-1')),
    }
    return sendMessage(nats, { roomId: 'r1', siteId: 's1', payload, timeoutMs: 10 }).then(
      () => { throw new Error('expected the send to reject') },
      (err) => err,
    )
  }

  it("arms on the real error sendMessage throws for the gatekeeper's thread-start refusal", async () => {
    const err = await sendAndCatch((reply) =>
      reply({ error: 'thread start unavailable', code: 'unavailable', reason: 'thread_start_unavailable' }))
    const { result } = renderHook(() => useDegraded(), { wrapper })
    act(() => result.current.noteHistoryFailure(err))
    expect(result.current.historyDegraded).toBe(true)
  })

  it('arms on the real error sendMessage throws when no reply ever arrives', async () => {
    const err = await sendAndCatch(() => {})
    const { result } = renderHook(() => useDegraded(), { wrapper })
    act(() => result.current.noteHistoryFailure(err))
    expect(result.current.historyDegraded).toBe(true)
  })
})
