// Runtime config: window.__APP_CONFIG__ in prod (rendered by nginx envsubst),
// import.meta.env.VITE_* in `vite dev`, literal defaults as last resort.

const runtime = (typeof window !== 'undefined' && window.__APP_CONFIG__) || {}

function boolConfig(value, fallback) {
  return String(value ?? fallback).toLowerCase() === 'true'
}

export const PORTAL_URL =
  runtime.PORTAL_URL || import.meta.env.VITE_PORTAL_URL || 'http://localhost:8085'

export const DEV_MODE = boolConfig(runtime.DEV_MODE ?? import.meta.env.VITE_DEV_MODE, true)

export const OIDC_ISSUER_URL =
  runtime.OIDC_ISSUER_URL ||
  import.meta.env.VITE_OIDC_ISSUER_URL ||
  'http://localhost:8180/realms/chatapp'

export const OIDC_CLIENT_ID =
  runtime.OIDC_CLIENT_ID || import.meta.env.VITE_OIDC_CLIENT_ID || 'nats-chat'

export const OTEL_ENABLED = boolConfig(
  runtime.OTEL_ENABLED ?? import.meta.env.VITE_OTEL_ENABLED,
  true,
)

export const OTEL_EXPORTER_OTLP_TRACES_URL =
  runtime.OTEL_EXPORTER_OTLP_TRACES_URL ||
  import.meta.env.VITE_OTEL_EXPORTER_OTLP_TRACES_URL ||
  'http://localhost:4318/v1/traces'

export const OTEL_SERVICE_NAME =
  runtime.OTEL_SERVICE_NAME || import.meta.env.VITE_OTEL_SERVICE_NAME || 'chat-frontend'
