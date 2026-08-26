import { describe, it, expect, vi } from 'vitest'
import { sendMessage } from './index'
import { AsyncJobError } from '../_transport/asyncJob'

function natsDouble(reply?: unknown, opts: { delayMs?: number } = {}) {
  const handlers: Record<string, (data: unknown) => void> = {}
  return {
    user: { account: 'alice' },
    subscribe: vi.fn((subject: string, cb: (data: unknown) => void) => {
      handlers[subject] = cb
      return { unsubscribe: vi.fn() }
    }),
    publish: vi.fn((subject: string) => {
      if (reply === undefined) return
      const respSubject = Object.keys(handlers)[0]
      setTimeout(() => handlers[respSubject]?.(reply), opts.delayMs ?? 0)
    }),
    handlers,
  }
}

const args = {
  roomId: 'r1',
  siteId: 'site-a',
  payload: { id: 'm1', content: 'hi', requestId: '01970a4f-8c2d-7c9a-abcd-e0123456789f' },
}

describe('sendMessage', () => {
  it('resolves when the gatekeeper replies with the stored message', async () => {
    const nats = natsDouble({ id: 'm1', roomId: 'r1' })
    await expect(sendMessage(nats as never, args)).resolves.toBeUndefined()
  })

  it('subscribes to the response subject before publishing', async () => {
    const nats = natsDouble({ id: 'm1' })
    await sendMessage(nats as never, args)
    const subscribeOrder = nats.subscribe.mock.invocationCallOrder[0]
    const publishOrder = nats.publish.mock.invocationCallOrder[0]
    expect(subscribeOrder).toBeLessThan(publishOrder)
  })

  it('rejects with the typed reason when the gatekeeper refuses a thread start', async () => {
    const nats = natsDouble({
      error: 'cannot start a new thread while message history is unavailable',
      code: 'unavailable',
      reason: 'thread_start_unavailable',
    })
    await expect(sendMessage(nats as never, args)).rejects.toMatchObject({
      reason: 'thread_start_unavailable',
      code: 'unavailable',
    })
  })

  it('rejects on timeout when no reply arrives', async () => {
    vi.useFakeTimers()
    const nats = natsDouble()
    const p = sendMessage(nats as never, { ...args, timeoutMs: 1000 })
    const assertion = expect(p).rejects.toMatchObject({ kind: 'async-timeout' })
    await vi.advanceTimersByTimeAsync(1001)
    await assertion
    vi.useRealTimers()
  })

  it('unsubscribes once settled', async () => {
    const nats = natsDouble({ id: 'm1' })
    await sendMessage(nats as never, args)
    const sub = nats.subscribe.mock.results[0].value
    expect(sub.unsubscribe).toHaveBeenCalled()
  })

  it('rejects with an AsyncJobError and unsubscribes when publish throws synchronously', async () => {
    const unsubscribe = vi.fn()
    const subscribe = vi.fn(() => ({ unsubscribe }))
    const publish = vi.fn(() => {
      throw new Error('Not connected')
    })
    const nats = { user: { account: 'alice' }, subscribe, publish }

    await expect(sendMessage(nats as never, args)).rejects.toBeInstanceOf(AsyncJobError)
    expect(unsubscribe).toHaveBeenCalled()
  })
})
