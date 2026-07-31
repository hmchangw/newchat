import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { setMediaCookie } from './index'

describe('setMediaCookie', () => {
  beforeEach(() => vi.stubGlobal('fetch', vi.fn()))
  afterEach(() => {
    vi.unstubAllGlobals()
    vi.clearAllMocks()
  })

  it('POSTs the ssoToken to the setCookie endpoint with credentials and resolves true', async () => {
    vi.mocked(fetch).mockResolvedValue({ ok: true } as Response)
    const ok = await setMediaCookie({ baseUrl: 'https://media.test/', ssoToken: 'tok' })
    expect(ok).toBe(true)
    const [url, init] = vi.mocked(fetch).mock.calls[0]
    expect(url).toBe('https://media.test/api/v1/file/setCookie')
    expect(init?.method).toBe('POST')
    expect((init?.headers as Record<string, string>).ssoToken).toBe('tok')
    expect(init?.credentials).toBe('include')
  })

  it('resolves false (no throw) when inputs are missing or the request fails', async () => {
    expect(await setMediaCookie({ baseUrl: '', ssoToken: 'tok' })).toBe(false)
    expect(await setMediaCookie({ baseUrl: 'https://x', ssoToken: '' })).toBe(false)
    expect(fetch).not.toHaveBeenCalled()

    vi.mocked(fetch).mockRejectedValue(new Error('network'))
    expect(await setMediaCookie({ baseUrl: 'https://x', ssoToken: 'tok' })).toBe(false)
  })
})
