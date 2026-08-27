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

  // The nginx-rendered config.js lands before the bundle, but tests flip the flag
  // after import — so the read must happen per call, not once at module load.
  it('the gates read window.__APP_CONFIG__ at call time, not at module load', async () => {
    const { permissionsEnabled, updatesEnabled } = await import('./runtimeConfig.js')
    expect(updatesEnabled()).toBe(false)

    window.__APP_CONFIG__ = { PERMISSIONS_ENABLED: 'true', UPDATES_ENABLED: 'true' }
    expect(updatesEnabled()).toBe(true)
    expect(permissionsEnabled()).toBe(true)

    window.__APP_CONFIG__ = { PERMISSIONS_ENABLED: 'true', UPDATES_ENABLED: 'false' }
    expect(updatesEnabled()).toBe(false)
    expect(permissionsEnabled()).toBe(true)
  })

  it('ondutyMinMembers defaults to 5 when unset', async () => {
    const { ondutyMinMembers } = await import('./runtimeConfig.js')
    expect(ondutyMinMembers()).toBe(5)
  })

  it('ondutyMinMembers reads the envsubst-rendered string from window.__APP_CONFIG__', async () => {
    window.__APP_CONFIG__ = { ROOM_ONDUTY_MIN_MEMBERS: '8' }
    const { ondutyMinMembers } = await import('./runtimeConfig.js')
    expect(ondutyMinMembers()).toBe(8)
  })

  it('ondutyMinMembers falls back to 5 when the rendered value is not a positive number', async () => {
    const { ondutyMinMembers } = await import('./runtimeConfig.js')
    for (const bad of ['', 'abc', '0', '-3', '${ROOM_ONDUTY_MIN_MEMBERS}']) {
      window.__APP_CONFIG__ = { ROOM_ONDUTY_MIN_MEMBERS: bad }
      expect(ondutyMinMembers(), `value ${bad}`).toBe(5)
    }
  })
})
