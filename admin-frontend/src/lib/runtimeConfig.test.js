import { describe, it, expect, beforeEach, vi } from 'vitest'

describe('runtimeConfig', () => {
  beforeEach(() => {
    vi.resetModules()
    delete window.__APP_CONFIG__
  })

  it('ADMIN_SERVICE_URL defaults to localhost:8082 when unset', async () => {
    const { ADMIN_SERVICE_URL } = await import('./runtimeConfig.js')
    expect(ADMIN_SERVICE_URL).toBe('http://localhost:8082')
  })

  it('ADMIN_SERVICE_URL reads from window.__APP_CONFIG__', async () => {
    window.__APP_CONFIG__ = { ADMIN_SERVICE_URL: 'https://admin.example.com' }
    const { ADMIN_SERVICE_URL } = await import('./runtimeConfig.js')
    expect(ADMIN_SERVICE_URL).toBe('https://admin.example.com')
  })
})
