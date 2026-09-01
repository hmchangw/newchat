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

  it('permissionsEnabled is false when the runtime flag is absent', async () => {
    window.__APP_CONFIG__ = {}
    const { permissionsEnabled } = await import('./runtimeConfig.js')
    expect(permissionsEnabled()).toBe(false)
  })

  it('permissionsEnabled is true only for the literal string "true"', async () => {
    window.__APP_CONFIG__ = { PERMISSIONS_ENABLED: 'true' }
    const { permissionsEnabled } = await import('./runtimeConfig.js')
    expect(permissionsEnabled()).toBe(true)
    window.__APP_CONFIG__ = { PERMISSIONS_ENABLED: 'True' }
    expect(permissionsEnabled()).toBe(false)
  })

  it('updatesEnabled is false when the runtime flag is absent', async () => {
    window.__APP_CONFIG__ = {}
    const { updatesEnabled } = await import('./runtimeConfig.js')
    expect(updatesEnabled()).toBe(false)
  })

  it('updatesEnabled is true only for the literal string "true"', async () => {
    window.__APP_CONFIG__ = { UPDATES_ENABLED: 'true' }
    const { updatesEnabled } = await import('./runtimeConfig.js')
    expect(updatesEnabled()).toBe(true)
    window.__APP_CONFIG__ = { UPDATES_ENABLED: 'True' }
    expect(updatesEnabled()).toBe(false)
  })

  it('updatesEnabled reads at call time, independent of permissionsEnabled', async () => {
    window.__APP_CONFIG__ = { PERMISSIONS_ENABLED: 'true', UPDATES_ENABLED: 'false' }
    const { permissionsEnabled, updatesEnabled } = await import('./runtimeConfig.js')
    expect(permissionsEnabled()).toBe(true)
    expect(updatesEnabled()).toBe(false)
  })
})
