import { describe, it, expect, beforeEach, vi } from 'vitest'

describe('runtimeConfig', () => {
  beforeEach(() => {
    vi.resetModules()
    delete window.__APP_CONFIG__
  })

  it('DEV_MODE defaults to true when not overridden', async () => {
    const { DEV_MODE } = await import('./runtimeConfig.js')
    expect(DEV_MODE).toBe(true)
  })

  it('DEV_MODE is false when window.__APP_CONFIG__.DEV_MODE = "false"', async () => {
    window.__APP_CONFIG__ = { DEV_MODE: 'false' }
    const { DEV_MODE } = await import('./runtimeConfig.js')
    expect(DEV_MODE).toBe(false)
  })

  it('OIDC_ISSUER_URL defaults to Keycloak chatapp realm', async () => {
    const { OIDC_ISSUER_URL } = await import('./runtimeConfig.js')
    expect(OIDC_ISSUER_URL).toBe('http://localhost:8180/realms/chatapp')
  })

  it('OIDC_CLIENT_ID defaults to nats-chat', async () => {
    const { OIDC_CLIENT_ID } = await import('./runtimeConfig.js')
    expect(OIDC_CLIENT_ID).toBe('nats-chat')
  })

  it('OIDC_ISSUER_URL reads from window.__APP_CONFIG__', async () => {
    window.__APP_CONFIG__ = { OIDC_ISSUER_URL: 'https://custom-keycloak/realms/myrealm' }
    const { OIDC_ISSUER_URL } = await import('./runtimeConfig.js')
    expect(OIDC_ISSUER_URL).toBe('https://custom-keycloak/realms/myrealm')
  })

  it('PORTAL_URL defaults to localhost:8085', async () => {
    const { PORTAL_URL } = await import('./runtimeConfig.js')
    expect(PORTAL_URL).toBe('http://localhost:8085')
  })

  it('PORTAL_URL reads from window.__APP_CONFIG__', async () => {
    window.__APP_CONFIG__ = { PORTAL_URL: 'https://portal.example.com' }
    const { PORTAL_URL } = await import('./runtimeConfig.js')
    expect(PORTAL_URL).toBe('https://portal.example.com')
  })

  it('no longer exports the retired static connection vars', async () => {
    const mod = await import('./runtimeConfig.js')
    expect(mod.AUTH_URL).toBeUndefined()
    expect(mod.NATS_URL).toBeUndefined()
    expect(mod.DEFAULT_SITE_ID).toBeUndefined()
  })

  it('frontend telemetry is enabled by default', async () => {
    const { OTEL_ENABLED } = await import('./runtimeConfig.js')
    expect(OTEL_ENABLED).toBe(true)
  })

  it('OTEL_EXPORTER_OTLP_TRACES_URL defaults to the local collector traces endpoint', async () => {
    const { OTEL_EXPORTER_OTLP_TRACES_URL } = await import('./runtimeConfig.js')
    expect(OTEL_EXPORTER_OTLP_TRACES_URL).toBe('http://localhost:4318/v1/traces')
  })

  it('OTEL_SERVICE_NAME defaults to chat-frontend', async () => {
    const { OTEL_SERVICE_NAME } = await import('./runtimeConfig.js')
    expect(OTEL_SERVICE_NAME).toBe('chat-frontend')
  })

  it('telemetry settings read from window.__APP_CONFIG__', async () => {
    window.__APP_CONFIG__ = {
      OTEL_ENABLED: 'false',
      OTEL_EXPORTER_OTLP_TRACES_URL: 'https://collector.example.com/v1/traces',
      OTEL_SERVICE_NAME: 'custom-frontend',
    }
    const mod = await import('./runtimeConfig.js')
    expect(mod.OTEL_ENABLED).toBe(false)
    expect(mod.OTEL_EXPORTER_OTLP_TRACES_URL).toBe('https://collector.example.com/v1/traces')
    expect(mod.OTEL_SERVICE_NAME).toBe('custom-frontend')
  })
})
