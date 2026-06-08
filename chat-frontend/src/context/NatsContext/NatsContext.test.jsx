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
  jwtAuthenticator: vi.fn(),
}))

vi.mock('nkeys.js', () => ({
  createUser: () => ({ getPublicKey: () => 'UPUBKEY', getSeed: () => new Uint8Array([7]) }),
}))

import { NatsProvider, useNats } from './NatsContext'

function wrapper({ children }) {
  return <NatsProvider>{children}</NatsProvider>
}

describe('NatsProvider connect wiring', () => {
  beforeEach(() => {
    setCredentials.mockReset()
    stop.mockReset()
    natsConnect.mockReset().mockResolvedValue({ closed: () => new Promise(() => {}) })
    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ natsJwt: 'JWT123', user: { account: 'alice' } }),
    })
  })
  afterEach(() => { vi.restoreAllMocks() })

  it('sets credentials before connecting and passes the dynamic authenticator', async () => {
    const { result } = renderHook(() => useNats(), { wrapper })
    await act(async () => {
      await result.current.connect({ mode: 'sso', ssoToken: 'tok', siteId: 'site-1' })
    })
    expect(setCredentials).toHaveBeenCalledWith({
      jwt: 'JWT123',
      seed: new Uint8Array([7]),
      natsPublicKey: 'UPUBKEY',
      refreshable: true,
    })
    expect(natsConnect).toHaveBeenCalledWith(
      expect.objectContaining({ authenticator: fakeAuthenticator }))
    await waitFor(() => expect(result.current.connected).toBe(true))
  })

  it('marks dev-mode connections non-refreshable', async () => {
    const { result } = renderHook(() => useNats(), { wrapper })
    await act(async () => {
      await result.current.connect({ mode: 'dev', account: 'alice', siteId: 'site-1' })
    })
    expect(setCredentials).toHaveBeenCalledWith(expect.objectContaining({ refreshable: false }))
  })

  it('stops the refresh loop on disconnect', async () => {
    natsConnect.mockResolvedValue({
      closed: () => new Promise(() => {}),
      drain: vi.fn().mockResolvedValue(),
    })
    const { result } = renderHook(() => useNats(), { wrapper })
    await act(async () => {
      await result.current.connect({ mode: 'sso', ssoToken: 'tok', siteId: 'site-1' })
    })
    await act(async () => { await result.current.disconnect() })
    expect(stop).toHaveBeenCalledTimes(1)
  })
})
