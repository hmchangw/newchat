import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { uploadImage } from './index'

vi.mock('../auth/oidcClient', () => ({ getAccessToken: vi.fn() }))
import { getAccessToken } from '../auth/oidcClient'

const nats = { user: { account: 'alice', baseUrl: 'https://media.test' } } as never

describe('uploadImage', () => {
  beforeEach(() => {
    vi.mocked(getAccessToken).mockResolvedValue('tok-123')
    vi.stubGlobal('fetch', vi.fn())
  })
  afterEach(() => {
    vi.unstubAllGlobals()
    vi.clearAllMocks()
  })

  it('POSTs multipart to the upload/file endpoint with the ssoToken header and returns the attachment', async () => {
    const att = { id: 'f1', title: 'pic.png', type: 'file', titleLink: 'api/v1/file/rooms/r1/file/f1', fileType: 'image/png' }
    vi.mocked(fetch).mockResolvedValue({ ok: true, json: async () => ({ success: true, attachments: [att] }) } as Response)

    const file = new File([new Uint8Array([1, 2, 3])], 'pic.png', { type: 'image/png' })
    const out = await uploadImage(nats, { roomId: 'r1', file })

    expect(out).toEqual(att)
    const [url, init] = vi.mocked(fetch).mock.calls[0]
    expect(url).toBe('https://media.test/api/v1/file/rooms/r1/upload/file')
    expect(init?.method).toBe('POST')
    expect((init?.headers as Record<string, string>).ssoToken).toBe('tok-123')
    expect(init?.body).toBeInstanceOf(FormData)
    expect((init?.body as FormData).get('file')).toBe(file)
  })

  it('throws when there is no access token', async () => {
    vi.mocked(getAccessToken).mockResolvedValue(null)
    const file = new File(['x'], 'p.png', { type: 'image/png' })
    await expect(uploadImage(nats, { roomId: 'r1', file })).rejects.toThrow(/authenticat/i)
    expect(fetch).not.toHaveBeenCalled()
  })

  it('throws when the server rejects the upload', async () => {
    vi.mocked(fetch).mockResolvedValue({ ok: false, status: 413 } as Response)
    const file = new File(['x'], 'p.png', { type: 'image/png' })
    await expect(uploadImage(nats, { roomId: 'r1', file })).rejects.toThrow(/413/)
  })

  it('throws when the reply carries no attachment', async () => {
    vi.mocked(fetch).mockResolvedValue({ ok: true, json: async () => ({ success: true, attachments: [] }) } as Response)
    const file = new File(['x'], 'p.png', { type: 'image/png' })
    await expect(uploadImage(nats, { roomId: 'r1', file })).rejects.toThrow(/no attachment/i)
  })
})
