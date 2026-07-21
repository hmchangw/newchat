import { afterEach, describe, expect, it, vi } from 'vitest'
import { AsyncJobError } from '@/api'
import { botLogin, changePassword } from './botAuth'

function mockResponse(status: number, body: unknown): Response {
  return {
    ok: status >= 200 && status < 300,
    status,
    json: async () => body,
  } as unknown as Response
}

const BUNDLE = {
  authToken: 'tok-abc',
  account: 'acct-1',
  siteId: 'site-1',
  requirePasswordChange: false,
}

describe('botLogin', () => {
  afterEach(() => {
    vi.unstubAllGlobals()
  })

  it('POSTs to ADMIN_SERVICE_URL/v1/login with the username/password body and returns the bundle', async () => {
    const fetchMock = vi.fn().mockResolvedValue(mockResponse(200, BUNDLE))
    vi.stubGlobal('fetch', fetchMock)

    const result = await botLogin({ username: 'alice', password: 'hunter2' })

    expect(fetchMock).toHaveBeenCalledTimes(1)
    const [url, init] = fetchMock.mock.calls[0]
    expect(url).toBe('http://localhost:8082/v1/login')
    expect(init.method).toBe('POST')
    expect(init.headers['Content-Type']).toBe('application/json')
    expect(JSON.parse(init.body)).toEqual({ username: 'alice', password: 'hunter2' })
    expect(result).toEqual(BUNDLE)
  })

  it('throws AsyncJobError with .reason on a 401 invalid_credentials response', async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      mockResponse(401, {
        error: 'invalid credentials',
        code: 'unauthenticated',
        reason: 'invalid_credentials',
      }),
    )
    vi.stubGlobal('fetch', fetchMock)

    await expect(botLogin({ username: 'alice', password: 'wrong' })).rejects.toBeInstanceOf(
      AsyncJobError,
    )
    await expect(botLogin({ username: 'alice', password: 'wrong' })).rejects.toMatchObject({
      reason: 'invalid_credentials',
    })
  })
})

describe('changePassword', () => {
  afterEach(() => {
    vi.unstubAllGlobals()
  })

  it('POSTs to ADMIN_SERVICE_URL/v1/password/change with the Bearer header and body', async () => {
    const fetchMock = vi.fn().mockResolvedValue(mockResponse(200, {}))
    vi.stubGlobal('fetch', fetchMock)

    await changePassword({
      authToken: 'tok-abc',
      oldPassword: 'old',
      newPassword: 'new',
    })

    expect(fetchMock).toHaveBeenCalledTimes(1)
    const [url, init] = fetchMock.mock.calls[0]
    expect(url).toBe('http://localhost:8082/v1/password/change')
    expect(init.method).toBe('POST')
    expect(init.headers['Content-Type']).toBe('application/json')
    expect(init.headers.Authorization).toBe('Bearer tok-abc')
    expect(JSON.parse(init.body)).toEqual({ oldPassword: 'old', newPassword: 'new' })
  })

  it('throws on a non-2xx response', async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      mockResponse(400, { error: 'old password incorrect', code: 'bad_request' }),
    )
    vi.stubGlobal('fetch', fetchMock)

    await expect(
      changePassword({
        authToken: 'tok-abc',
        oldPassword: 'wrong',
        newPassword: 'new',
      }),
    ).rejects.toBeInstanceOf(AsyncJobError)
  })
})
