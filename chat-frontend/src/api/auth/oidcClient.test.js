import { describe, it, expect, vi, beforeEach } from 'vitest'

const signinSilent = vi.fn()
vi.mock('oidc-client-ts', () => ({
  UserManager: vi.fn(() => ({ signinSilent, removeUser: vi.fn(), signinRedirect: vi.fn() })),
  WebStorageStateStore: vi.fn(),
}))

import { renewSsoToken, _resetOidcManagerForTests } from './oidcClient'

describe('renewSsoToken', () => {
  beforeEach(() => {
    _resetOidcManagerForTests()
    signinSilent.mockReset()
  })

  it('returns the fresh access token from signinSilent', async () => {
    signinSilent.mockResolvedValue({ access_token: 'fresh-token' })
    await expect(renewSsoToken()).resolves.toBe('fresh-token')
  })

  it('throws when silent renew yields no token', async () => {
    signinSilent.mockResolvedValue(null)
    await expect(renewSsoToken()).rejects.toThrow()
  })

  it('propagates a silent-renew failure (SSO session ended)', async () => {
    signinSilent.mockRejectedValue(new Error('login_required'))
    await expect(renewSsoToken()).rejects.toThrow('login_required')
  })
})
