import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { renderHook, act, waitFor } from '@testing-library/react'

const setCredentials = vi.fn()
const stop = vi.fn()
const fakeAuthenticator = { tag: 'dynamic-auth' }
vi.mock('./useJwtRefresh', () => ({
  useJwtRefresh: vi.fn(() => ({ authenticator: fakeAuthenticator, setCredentials, stop })),
}))

const natsConnect = vi.fn()
vi.mock('nats.ws', () => ({
  connect: (...a) => natsConnect(...a),
  StringCodec: () => ({ encode: (s) => s, decode: (s) => s }),
  headers: () => {
    const values = new Map()
    return {
      set: (k, v) => values.set(k.toLowerCase(), v),
      get: (k) => values.get(k.toLowerCase()) ?? '',
    }
  },
  jwtAuthenticator: vi.fn(),
}))

vi.mock('@/lib/telemetry', () => ({
  initTelemetry: vi.fn(),
  injectTraceHeaders: vi.fn((headers) => headers.set('traceparent', '00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01')),
  withSpan: vi.fn((_name, _attrs, fn) => fn({ setStatus: vi.fn(), recordException: vi.fn() })),
  withLinkedSpan: vi.fn((_name, _attrs, _headers, fn) => fn({ setStatus: vi.fn(), recordException: vi.fn() })),
  natsSpanName: vi.fn((operation, subject) => `nats ${operation} ${subject}`),
}))

vi.mock('nkeys.js', () => ({
  createUser: () => ({ getPublicKey: () => 'UPUBKEY', getSeed: () => new Uint8Array([7]) }),
}))

import { NatsProvider, useNats } from './NatsContext'
import { DebugProvider } from '@/context/DebugContext'
import { useJwtRefresh } from './useJwtRefresh'
import * as telemetry from '@/lib/telemetry'

function wrapper({ children }) {
  return (
    <DebugProvider>
      <NatsProvider>{children}</NatsProvider>
    </DebugProvider>
  )
}

// The provider hands its authUrlRef getter to useJwtRefresh; reading it back
// through the mock observes when the resolved auth URL is committed.
function lastGetAuthUrl() {
  return useJwtRefresh.mock.calls.at(-1)[0].getAuthUrl
}

const PORTAL_RESP = {
  account: 'alice',
  employeeId: 'E001',
  baseUrl: 'http://site-a',
  natsUrl: 'ws://nats.site-a',
  siteId: 'site-a',
}

function makeAsyncSubscription() {
  const pending = []
  let waiter = null
  let closed = false
  return {
    push(msg) {
      if (waiter) {
        const resolve = waiter
        waiter = null
        resolve({ value: msg, done: false })
        return
      }
      pending.push(msg)
    },
    unsubscribe() {
      closed = true
      if (waiter) {
        const resolve = waiter
        waiter = null
        resolve({ value: undefined, done: true })
      }
    },
    [Symbol.asyncIterator]() {
      return this
    },
    next() {
      if (pending.length > 0) return Promise.resolve({ value: pending.shift(), done: false })
      if (closed) return Promise.resolve({ value: undefined, done: true })
      return new Promise((resolve) => { waiter = resolve })
    },
  }
}

describe('NatsProvider connect wiring', () => {
  beforeEach(() => {
    setCredentials.mockReset()
    stop.mockReset()
    natsConnect.mockReset().mockResolvedValue({ closed: () => new Promise(() => {}) })
    telemetry.injectTraceHeaders.mockClear()
    telemetry.withSpan.mockClear()
    telemetry.withLinkedSpan.mockClear()
    telemetry.natsSpanName.mockClear()
    global.fetch = vi.fn(async (url) => {
      if (String(url).includes('/api/userInfo')) {
        return { ok: true, json: async () => PORTAL_RESP }
      }
      return { ok: true, json: async () => ({ natsJwt: 'JWT123', user: { account: 'alice' } }) }
    })
  })
  afterEach(() => { vi.restoreAllMocks() })

  it('resolves the site via portal userInfo, then auths and connects with the resolved URLs', async () => {
    const { result } = renderHook(() => useNats(), { wrapper })
    await act(async () => {
      await result.current.connect({ mode: 'sso', ssoToken: 'tok', account: 'alice' })
    })

    expect(global.fetch).toHaveBeenNthCalledWith(
      1,
      'http://localhost:8085/api/userInfo?account=alice',
      expect.objectContaining({ method: 'GET', headers: expect.any(Headers) }),
    )
    expect(global.fetch).toHaveBeenNthCalledWith(2, 'http://site-a/api/v1/auth', expect.anything())
    expect(setCredentials).toHaveBeenCalledWith({
      jwt: 'JWT123',
      seed: new Uint8Array([7]),
      natsPublicKey: 'UPUBKEY',
      refreshable: true,
    })
    expect(natsConnect).toHaveBeenCalledWith(
      expect.objectContaining({ servers: 'ws://nats.site-a', authenticator: fakeAuthenticator }))
    await waitFor(() => expect(result.current.connected).toBe(true))
    expect(result.current.user.siteId).toBe('site-a')
    expect(lastGetAuthUrl()()).toBe('http://site-a/api/v1')
  })

  it('drops a stale nc.closed() callback from a superseded connection (generation guard)', async () => {
    let closeFirst
    const firstClosed = new Promise((res) => { closeFirst = res })
    natsConnect
      .mockResolvedValueOnce({ closed: () => firstClosed })
      .mockResolvedValueOnce({ closed: () => new Promise(() => {}) })

    const { result } = renderHook(() => useNats(), { wrapper })

    await act(async () => {
      await result.current.connect({ mode: 'sso', ssoToken: 'tok', account: 'alice' })
    })
    expect(result.current.connected).toBe(true)

    // A newer connect supersedes the first and bumps the connect generation.
    await act(async () => {
      await result.current.connect({ mode: 'sso', ssoToken: 'tok2', account: 'alice' })
    })
    expect(result.current.connected).toBe(true)

    // The first (now stale) link closes with an error. Its long-lived callback
    // must not clobber the live second session's connected/error state.
    await act(async () => {
      closeFirst(new Error('old link dropped'))
      await firstClosed.catch(() => {})
    })

    expect(result.current.connected).toBe(true)
    expect(result.current.error).toBeNull()
  })

  it('rolls back staged credentials when the NATS dial fails', async () => {
    natsConnect.mockRejectedValue(new Error('handshake refused'))
    const { result } = renderHook(() => useNats(), { wrapper })
    let thrown
    await act(async () => {
      try { await result.current.connect({ mode: 'sso', ssoToken: 'tok', account: 'alice' }) } catch (err) { thrown = err }
    })

    expect(thrown.message).toBe('handshake refused')
    expect(setCredentials).toHaveBeenCalledTimes(1)
    expect(stop).toHaveBeenCalledTimes(1)
    expect(result.current.connected).toBe(false)
    // The refresh loop must not be pointed at the new site by a failed connect.
    expect(lastGetAuthUrl()()).toBeNull()
  })

  it('dev mode looks up the account via userInfo and is non-refreshable', async () => {
    const { result } = renderHook(() => useNats(), { wrapper })
    await act(async () => {
      await result.current.connect({ mode: 'dev', account: 'alice' })
    })
    expect(global.fetch).toHaveBeenNthCalledWith(
      1,
      'http://localhost:8085/api/userInfo?account=alice',
      expect.objectContaining({ method: 'GET', headers: expect.any(Headers) }),
    )
    expect(setCredentials).toHaveBeenCalledWith(expect.objectContaining({ refreshable: false }))
  })

  it('propagates the portal error envelope and never dials auth or NATS', async () => {
    global.fetch = vi.fn(async () => ({
      ok: false,
      json: async () => ({ code: 'forbidden', reason: 'account_not_ready', error: 'account not ready for chat' }),
    }))
    const { result } = renderHook(() => useNats(), { wrapper })
    let thrown
    await act(async () => {
      try { await result.current.connect({ mode: 'sso', ssoToken: 'tok', account: 'alice' }) } catch (err) { thrown = err }
    })
    expect(thrown.reason).toBe('account_not_ready')
    expect(thrown.code).toBe('forbidden')
    expect(global.fetch).toHaveBeenCalledTimes(1)
    expect(natsConnect).not.toHaveBeenCalled()
  })

  it('propagates the auth-step error envelope after a successful lookup', async () => {
    global.fetch = vi.fn(async (url) => {
      if (String(url).includes('/api/userInfo')) {
        return { ok: true, json: async () => PORTAL_RESP }
      }
      return {
        ok: false,
        json: async () => ({ code: 'unauthenticated', reason: 'sso_token_expired', error: 'SSO token has expired, please re-login' }),
      }
    })
    const { result } = renderHook(() => useNats(), { wrapper })
    let thrown
    await act(async () => {
      try { await result.current.connect({ mode: 'sso', ssoToken: 'tok', account: 'alice' }) } catch (err) { thrown = err }
    })
    expect(thrown.reason).toBe('sso_token_expired')
    expect(thrown.message).toBe('SSO token has expired, please re-login')
    expect(global.fetch).toHaveBeenCalledTimes(2)
    expect(natsConnect).not.toHaveBeenCalled()
  })

  it('falls back to a status message when the error body is not JSON', async () => {
    global.fetch = vi.fn(async () => ({
      ok: false,
      status: 502,
      json: async () => { throw new Error('not json') },
    }))
    const { result } = renderHook(() => useNats(), { wrapper })
    let thrown
    await act(async () => {
      try { await result.current.connect({ mode: 'sso', ssoToken: 'tok', account: 'alice' }) } catch (err) { thrown = err }
    })
    expect(thrown.message).toBe('Portal lookup failed: 502')
    expect(thrown.code).toBeUndefined()
    expect(natsConnect).not.toHaveBeenCalled()
  })

  it('stops the refresh loop on disconnect', async () => {
    natsConnect.mockResolvedValue({
      closed: () => new Promise(() => {}),
      drain: vi.fn().mockResolvedValue(),
    })
    const { result } = renderHook(() => useNats(), { wrapper })
    await act(async () => {
      await result.current.connect({ mode: 'sso', ssoToken: 'tok', account: 'alice' })
    })
    await act(async () => { await result.current.disconnect() })
    expect(stop).toHaveBeenCalledTimes(1)
  })

  it('injects traceparent on fire-and-forget publish', async () => {
    const publish = vi.fn()
    natsConnect.mockResolvedValue({
      closed: () => new Promise(() => {}),
      publish,
    })
    const { result } = renderHook(() => useNats(), { wrapper })
    await act(async () => {
      await result.current.connect({ mode: 'dev', account: 'alice' })
    })
    act(() => {
      result.current.publish('chat.user.alice.room.r1.site-a.msg.send', { id: 'm1' })
    })
    expect(publish).toHaveBeenCalledWith(
      'chat.user.alice.room.r1.site-a.msg.send',
      JSON.stringify({ id: 'm1' }),
      expect.objectContaining({
        headers: expect.objectContaining({
          get: expect.any(Function),
        }),
      }),
    )
    expect(publish.mock.calls[0][2].headers.get('traceparent')).toBe('00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01')
    expect(telemetry.withSpan).toHaveBeenCalledWith(
      'nats publish chat.user.alice.room.r1.site-a.msg.send',
      expect.objectContaining({
        'messaging.operation.name': 'publish',
        'messaging.destination.name': 'chat.user.alice.room.r1.site-a.msg.send',
      }),
      expect.any(Function),
    )
  })

  it('wraps received subscription messages in a linked consumer span', async () => {
    const sub = makeAsyncSubscription()
    const subscribe = vi.fn(() => sub)
    natsConnect.mockResolvedValue({
      closed: () => new Promise(() => {}),
      subscribe,
    })
    const { result } = renderHook(() => useNats(), { wrapper })
    await act(async () => {
      await result.current.connect({ mode: 'dev', account: 'alice' })
    })

    const callback = vi.fn()
    act(() => {
      result.current.subscribe('chat.room.r1.event.>', callback)
    })
    const headers = { get: vi.fn((key) => (key.toLowerCase() === 'traceparent' ? '00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01' : '')) }
    sub.push({
      subject: 'chat.room.r1.event.created',
      data: JSON.stringify({ type: 'created', roomId: 'r1' }),
      headers,
    })

    await waitFor(() => expect(callback).toHaveBeenCalledWith({ type: 'created', roomId: 'r1' }))
    expect(telemetry.withLinkedSpan).toHaveBeenCalledWith(
      'nats receive chat.room.r1.event.created',
      expect.objectContaining({
        'messaging.system': 'nats',
        'messaging.operation.name': 'receive',
        'messaging.destination.name': 'chat.room.r1.event.created',
        'messaging.subscription.name': 'chat.room.r1.event.>',
      }),
      headers,
      expect.any(Function),
      expect.any(Number),
    )
  })
})

describe('NatsProvider session (bot/admin) connect', () => {
  const BUNDLE = {
    account: 'p_admin', siteId: 'site-a',
    baseUrl: 'http://site-a', natsUrl: 'ws://nats.site-a', authToken: 'tok43',
  }
  beforeEach(() => {
    setCredentials.mockReset()
    stop.mockReset()
    natsConnect.mockReset().mockResolvedValue({ closed: () => new Promise(() => {}), drain: async () => {} })
    window.sessionStorage.clear()
    global.fetch = vi.fn(async () => ({ ok: true, json: async () => ({ natsJwt: 'JWT9', user: { account: 'p_admin' } }) }))
  })
  afterEach(() => { window.sessionStorage.clear(); vi.restoreAllMocks() })

  it('skips /api/userInfo, mints with authToken, persists the bundle', async () => {
    const { result } = renderHook(() => useNats(), { wrapper })
    await act(async () => { await result.current.connect({ mode: 'session', bundle: BUNDLE }) })

    const urls = global.fetch.mock.calls.map((c) => String(c[0]))
    expect(urls.some((u) => u.includes('/api/userInfo'))).toBe(false)
    expect(global.fetch).toHaveBeenCalledWith('http://site-a/api/v1/auth', expect.anything())
    const authBody = JSON.parse(global.fetch.mock.calls.at(-1)[1].body)
    expect(authBody).toEqual({ authToken: 'tok43', natsPublicKey: 'UPUBKEY' })
    expect(setCredentials).toHaveBeenCalledWith(
      expect.objectContaining({ mode: 'session', authToken: 'tok43', refreshable: true }))
    await waitFor(() => expect(result.current.connected).toBe(true))
    expect(result.current.user.siteId).toBe('site-a')
    expect(JSON.parse(window.sessionStorage.getItem('chat.botSession'))).toEqual(BUNDLE)
  })

  it('disconnect() clears the stored bot session', async () => {
    const { result } = renderHook(() => useNats(), { wrapper })
    await act(async () => { await result.current.connect({ mode: 'session', bundle: BUNDLE }) })
    expect(window.sessionStorage.getItem('chat.botSession')).not.toBeNull()
    await act(async () => { await result.current.disconnect() })
    expect(window.sessionStorage.getItem('chat.botSession')).toBeNull()
  })

  it('auto-reconnects on mount from a stored bot session', async () => {
    window.sessionStorage.setItem('chat.botSession', JSON.stringify(BUNDLE))
    const { result } = renderHook(() => useNats(), { wrapper })
    await waitFor(() => expect(result.current.connected).toBe(true))
    expect(global.fetch).toHaveBeenCalledWith('http://site-a/api/v1/auth', expect.anything())
  })

  it('clears a stored bot session and stays logged out when auto-reconnect fails', async () => {
    window.sessionStorage.setItem('chat.botSession', JSON.stringify(BUNDLE))
    natsConnect.mockRejectedValue(new Error('dial fail'))
    const { result } = renderHook(() => useNats(), { wrapper })
    await waitFor(() => expect(window.sessionStorage.getItem('chat.botSession')).toBeNull())
    expect(result.current.connected).toBe(false)
  })
})
