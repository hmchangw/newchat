import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { renderHook, act } from '@testing-library/react'

vi.mock('nats.ws', () => ({
  jwtAuthenticator: vi.fn((jwtFn, seedFn) => ({ jwtFn, seedFn })),
}))
vi.mock('@/api/auth/oidcClient', () => ({
  renewSsoToken: vi.fn(),
  redirectToReloginOnTokenInvalid: vi.fn(() => Promise.resolve()),
}))

import { jwtAuthenticator } from 'nats.ws'
import { renewSsoToken, redirectToReloginOnTokenInvalid } from '@/api/auth/oidcClient'
import { useJwtRefresh } from './useJwtRefresh'

function makeJwt(expSecFromNow) {
  const b64 = (obj) =>
    btoa(JSON.stringify(obj)).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
  const exp = Math.floor(Date.now() / 1000) + expSecFromNow
  return `${b64({ alg: 'ed25519' })}.${b64({ exp })}.sig`
}
const okResp = (jwt) => ({ ok: true, json: async () => ({ natsJwt: jwt }) })
const errResp = (status) => ({ ok: false, status, json: async () => ({}) })

function setup({ ncRef = { current: { reconnect: vi.fn() } } } = {}) {
  const view = renderHook(() => useJwtRefresh({ authUrl: 'http://auth', ncRef }))
  return { ...view, ncRef }
}

describe('useJwtRefresh', () => {
  beforeEach(() => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
    renewSsoToken.mockReset()
    redirectToReloginOnTokenInvalid.mockReset().mockResolvedValue()
    vi.spyOn(console, 'warn').mockImplementation(() => {})
    global.fetch = vi.fn()
  })
  afterEach(() => {
    vi.useRealTimers()
    vi.restoreAllMocks()
  })

  it('builds a dynamic authenticator whose getters read current creds', () => {
    const { result } = setup({ ncRef: { current: null } })
    act(() => {
      result.current.setCredentials({ jwt: 'jwt-A', seed: new Uint8Array([1]), natsPublicKey: 'UPUB', refreshable: false })
    })
    expect(jwtAuthenticator).toHaveBeenCalledTimes(1)
    expect(result.current.authenticator.jwtFn()).toBe('jwt-A')
    expect(result.current.authenticator.seedFn()).toEqual(new Uint8Array([1]))
  })

  it('refreshes at ~80% of life: renews SSO, re-mints with same nkey, reconnects', async () => {
    const reconnect = vi.fn()
    renewSsoToken.mockResolvedValue('fresh-sso')
    global.fetch.mockResolvedValue(okResp(makeJwt(3600)))
    const { result } = setup({ ncRef: { current: { reconnect } } })
    act(() => {
      result.current.setCredentials({ jwt: makeJwt(100), seed: new Uint8Array([9]), natsPublicKey: 'UPUB', refreshable: true })
    })
    await act(async () => { await vi.advanceTimersByTimeAsync(85_000) })
    expect(renewSsoToken).toHaveBeenCalledTimes(1)
    const body = JSON.parse(global.fetch.mock.calls[0][1].body)
    expect(body).toEqual({ ssoToken: 'fresh-sso', natsPublicKey: 'UPUB' })
    expect(reconnect).toHaveBeenCalledTimes(1)
    expect(redirectToReloginOnTokenInvalid).not.toHaveBeenCalled()
  })

  it('redirects immediately when silent renew fails (SSO session ended)', async () => {
    renewSsoToken.mockRejectedValue(new Error('login_required'))
    const { result } = setup({ ncRef: { current: null } })
    act(() => {
      result.current.setCredentials({ jwt: makeJwt(100), seed: new Uint8Array(), natsPublicKey: 'UPUB', refreshable: true })
    })
    await act(async () => { await vi.advanceTimersByTimeAsync(85_000) })
    expect(redirectToReloginOnTokenInvalid).toHaveBeenCalledTimes(1)
    expect(global.fetch).not.toHaveBeenCalled()
  })

  it('retries a transient 5xx re-mint failure, then succeeds without redirect', async () => {
    renewSsoToken.mockResolvedValue('sso')
    global.fetch
      .mockResolvedValueOnce(errResp(503))
      .mockResolvedValueOnce(okResp(makeJwt(3600)))
    const reconnect = vi.fn()
    const { result } = setup({ ncRef: { current: { reconnect } } })
    act(() => {
      result.current.setCredentials({ jwt: makeJwt(100), seed: new Uint8Array(), natsPublicKey: 'UPUB', refreshable: true })
    })
    // Fire the initial refresh timer (regardless of jitter delay) and let
    // promises settle. Using advanceTimersToNextTimerAsync avoids a fixed
    // 85_000 ms window that overlaps the 2 s retry under worst-case jitter.
    await act(async () => { await vi.advanceTimersToNextTimerAsync() })
    expect(global.fetch).toHaveBeenCalledTimes(1)
    expect(redirectToReloginOnTokenInvalid).not.toHaveBeenCalled()
    await act(async () => { await vi.advanceTimersByTimeAsync(2_000) })
    expect(global.fetch).toHaveBeenCalledTimes(2)
    expect(reconnect).toHaveBeenCalledTimes(1)
    expect(redirectToReloginOnTokenInvalid).not.toHaveBeenCalled()
  })

  it('redirects after transient retries are exhausted', async () => {
    renewSsoToken.mockResolvedValue('sso')
    global.fetch.mockResolvedValue(errResp(503))
    const { result } = setup()
    act(() => {
      result.current.setCredentials({ jwt: makeJwt(100), seed: new Uint8Array(), natsPublicKey: 'UPUB', refreshable: true })
    })
    await act(async () => { await vi.advanceTimersByTimeAsync(85_000) })
    await act(async () => { await vi.advanceTimersByTimeAsync(2_000) })
    await act(async () => { await vi.advanceTimersByTimeAsync(4_000) })
    await act(async () => { await vi.advanceTimersByTimeAsync(8_000) })
    expect(global.fetch).toHaveBeenCalledTimes(4)
    expect(redirectToReloginOnTokenInvalid).toHaveBeenCalledTimes(1)
  })

  it('redirects immediately on a 4xx re-mint rejection (no retry)', async () => {
    renewSsoToken.mockResolvedValue('sso')
    global.fetch.mockResolvedValue(errResp(401))
    const { result } = setup()
    act(() => {
      result.current.setCredentials({ jwt: makeJwt(100), seed: new Uint8Array(), natsPublicKey: 'UPUB', refreshable: true })
    })
    await act(async () => { await vi.advanceTimersByTimeAsync(85_000) })
    expect(global.fetch).toHaveBeenCalledTimes(1)
    expect(redirectToReloginOnTokenInvalid).toHaveBeenCalledTimes(1)
  })

  it('completes the refresh when the connection lacks reconnect() (degrades gracefully)', async () => {
    renewSsoToken.mockResolvedValue('sso')
    global.fetch.mockResolvedValue(okResp(makeJwt(3600)))
    const { result } = setup({ ncRef: { current: {} } })
    act(() => {
      result.current.setCredentials({ jwt: makeJwt(100), seed: new Uint8Array(), natsPublicKey: 'UPUB', refreshable: true })
    })
    await act(async () => { await vi.advanceTimersByTimeAsync(85_000) })
    expect(global.fetch).toHaveBeenCalledTimes(1)
    expect(redirectToReloginOnTokenInvalid).not.toHaveBeenCalled()
  })

  it('does not schedule when the JWT has no parseable exp', async () => {
    const { result } = setup()
    act(() => {
      result.current.setCredentials({ jwt: 'not-a-jwt', seed: new Uint8Array(), natsPublicKey: 'UPUB', refreshable: true })
    })
    await act(async () => { await vi.advanceTimersByTimeAsync(500_000) })
    expect(renewSsoToken).not.toHaveBeenCalled()
  })

  it('does not schedule a refresh when refreshable is false (dev mode)', async () => {
    const { result } = setup()
    act(() => {
      result.current.setCredentials({ jwt: makeJwt(100), seed: new Uint8Array(), natsPublicKey: 'UPUB', refreshable: false })
    })
    await act(async () => { await vi.advanceTimersByTimeAsync(200_000) })
    expect(renewSsoToken).not.toHaveBeenCalled()
  })

  it('stop() clears the pending timer and creds', async () => {
    const { result } = setup()
    act(() => {
      result.current.setCredentials({ jwt: makeJwt(100), seed: new Uint8Array([5]), natsPublicKey: 'UPUB', refreshable: true })
    })
    act(() => { result.current.stop() })
    await act(async () => { await vi.advanceTimersByTimeAsync(200_000) })
    expect(renewSsoToken).not.toHaveBeenCalled()
    expect(result.current.authenticator.jwtFn()).toBeNull()
  })

  it('clears the pending timer on unmount', async () => {
    const { result, unmount } = setup()
    act(() => {
      result.current.setCredentials({ jwt: makeJwt(100), seed: new Uint8Array(), natsPublicKey: 'UPUB', refreshable: true })
    })
    unmount()
    await act(async () => { await vi.advanceTimersByTimeAsync(200_000) })
    expect(renewSsoToken).not.toHaveBeenCalled()
  })

  it('discards an in-flight refresh when credentials change mid-flight (generation guard)', async () => {
    let resolveRenew
    renewSsoToken.mockImplementation(() => new Promise((res) => { resolveRenew = res }))
    const { result } = setup()
    act(() => {
      result.current.setCredentials({ jwt: makeJwt(100), seed: new Uint8Array(), natsPublicKey: 'OLD', refreshable: true })
    })
    await act(async () => { await vi.advanceTimersByTimeAsync(85_000) })
    expect(renewSsoToken).toHaveBeenCalledTimes(1)
    act(() => {
      result.current.setCredentials({ jwt: makeJwt(100), seed: new Uint8Array(), natsPublicKey: 'NEW', refreshable: false })
    })
    await act(async () => { resolveRenew('sso'); await Promise.resolve() })
    expect(global.fetch).not.toHaveBeenCalled()
  })
})
