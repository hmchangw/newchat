import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'

vi.mock('@/lib/runtimeConfig', async (importOriginal) => ({
  ...(await importOriginal()),
  PORTAL_URL: 'http://portal.test',
}))

import { botLogin } from './botAuth'
import { AsyncJobError } from '@/api'

const BUNDLE = {
  userId: 'u17', authToken: 'tok43', account: 'p_admin', siteId: 'site-a',
  authServiceUrl: 'http://auth.site-a', baseUrl: 'http://site-a', natsUrl: 'ws://nats.site-a',
  requirePasswordChange: true,
}

beforeEach(() => { global.fetch = vi.fn() })
afterEach(() => { vi.restoreAllMocks() })

describe('botLogin', () => {
  it('POSTs username/password to portal /api/v1/login and returns the bundle', async () => {
    global.fetch.mockResolvedValue({ ok: true, json: async () => BUNDLE })
    const out = await botLogin({ username: 'p_admin', password: 'pw' })
    expect(out).toEqual(BUNDLE)
    expect(global.fetch).toHaveBeenCalledWith('http://portal.test/api/v1/login', expect.objectContaining({
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username: 'p_admin', password: 'pw' }),
    }))
  })

  it('throws AsyncJobError carrying code/reason on a 401', async () => {
    global.fetch.mockResolvedValue({
      ok: false, status: 401,
      json: async () => ({ code: 'unauthenticated', reason: 'invalid_credentials', error: 'nope' }),
    })
    const err = await botLogin({ username: 'x', password: 'y' }).catch((e) => e)
    expect(err).toBeInstanceOf(AsyncJobError)
    expect(err.reason).toBe('invalid_credentials')
    expect(err.message).toBe('nope')
  })

  it('falls back to a status message when the error body is not JSON', async () => {
    global.fetch.mockResolvedValue({ ok: false, status: 503, json: async () => { throw new Error('not json') } })
    const err = await botLogin({ username: 'x', password: 'y' }).catch((e) => e)
    expect(err).toBeInstanceOf(AsyncJobError)
    expect(err.message).toMatch(/503/)
  })
})
